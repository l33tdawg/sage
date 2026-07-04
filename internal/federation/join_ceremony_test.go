package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
)

// ceremonyNode is a fully-provisioned in-process node for the join e2e test:
// its own CA + node cert (under a temp certs dir), an ed25519 operator key, a
// temp Badger store, and a stubbed tx broadcast (no live chain).
type ceremonyNode struct {
	mgr        *Manager
	certsDir   string
	broadcasts int
	mu         sync.Mutex
}

func newCeremonyNode(t *testing.T, chainID string) *ceremonyNode {
	t.Helper()
	dir := t.TempDir()
	caCert, caKey, err := tlsca.LoadOrGenerateCA(dir, chainID)
	if err != nil {
		t.Fatalf("gen CA: %v", err)
	}
	nodeCert, nodeKey, err := tlsca.GenerateNodeCert(caCert, caKey, "node", []string{"127.0.0.1", "localhost"})
	if err != nil {
		t.Fatalf("gen node cert: %v", err)
	}
	if writeErr := tlsca.WriteCert(filepath.Join(dir, tlsca.NodeCertFile), nodeCert); writeErr != nil {
		t.Fatalf("write node cert: %v", writeErr)
	}
	if writeErr := tlsca.WriteKey(filepath.Join(dir, tlsca.NodeKeyFile), nodeKey); writeErr != nil {
		t.Fatalf("write node key: %v", writeErr)
	}
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	badger, err := store.NewBadgerStore(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatalf("open badger: %v", err)
	}
	t.Cleanup(func() { _ = badger.CloseBadger() })

	node := &ceremonyNode{certsDir: dir}
	node.mgr = NewManager(Config{
		LocalChainID: chainID,
		CertsDir:     dir,
		AgentKey:     priv,
		Badger:       badger,
		Logger:       zerolog.Nop(),
	})
	node.mgr.broadcastFn = func(_ []byte) (string, int64, error) {
		node.mu.Lock()
		node.broadcasts++
		node.mu.Unlock()
		return "stub-tx-hash", 1, nil
	}
	return node
}

func (n *ceremonyNode) count() int {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.broadcasts
}

// TestJoinCeremonyHappyPath drives the full real-TOTP JOIN end to end over the
// real mTLS federation listener: both sides agree on the codes and the frozen
// attestation E, both operators broadcast tx-33, both persist the peer CA + the
// shared seed, and the host session goes ACTIVE.
func TestJoinCeremonyHappyPath(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")

	// Stand up the host's federation listener with its real mTLS config.
	hostTLS, err := host.mgr.ServerTLSConfig()
	if err != nil {
		t.Fatalf("host TLS: %v", err)
	}
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	defer srv.Close()
	hostEndpoint := srv.URL
	guestEndpoint := "https://127.0.0.1:8444"
	ctx := context.Background()

	// H1: host opens a session + emits the enrollment QR.
	create, err := host.mgr.HostCreate(hostEndpoint)
	if err != nil {
		t.Fatalf("HostCreate: %v", err)
	}

	// Guest scans the host QR (fetches + pins the host CA), gets its return QR.
	scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
	if err != nil {
		t.Fatalf("GuestScan: %v", err)
	}
	if scan.SessionID != create.SessionID {
		t.Fatalf("session id mismatch: %s vs %s", scan.SessionID, create.SessionID)
	}

	// Host scans the guest's return QR (records the anchor pin).
	if scanErr := host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI); scanErr != nil {
		t.Fatalf("HostScanReturn: %v", scanErr)
	}

	// Guest fires /join/request; both sides compute the codes.
	scopeG := ScopeWire{MaxClearance: 1, AllowedDomains: []string{"*"}, Mode: "exchange", Direction: "both"}
	greq, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, scopeG)
	if err != nil {
		t.Fatalf("GuestRequest: %v", err)
	}

	// Host view computes CODE_G identically.
	view, err := host.mgr.HostSessionStatus(create.SessionID)
	if err != nil {
		t.Fatalf("HostSessionStatus: %v", err)
	}
	if view.CodeG == "" || view.CodeG != greq.CodeG {
		t.Fatalf("CODE_G disagreement: host=%q guest=%q", view.CodeG, greq.CodeG)
	}
	if view.CodeH != "" {
		t.Fatalf("CODE_H leaked before approval")
	}

	// Approval #1: host types the code it heard, sets its grant, freezes E.
	hostGrant := ScopeWire{MaxClearance: 2, AllowedDomains: []string{"*"}, Mode: "exchange", Direction: "both"}
	if err := host.mgr.HostApprove(create.SessionID, greq.CodeG, hostGrant); err != nil {
		t.Fatalf("HostApprove: %v", err)
	}

	// After approval the host reveals CODE_H; the guest already computed it.
	view2, _ := host.mgr.HostSessionStatus(create.SessionID)
	if view2.CodeH == "" || view2.CodeH != greq.CodeH {
		t.Fatalf("CODE_H disagreement: host=%q guest=%q", view2.CodeH, greq.CodeH)
	}

	// Approval #2: guest confirms - broadcasts its tx-33, then the host confirms
	// against the frozen E and broadcasts its tx-33.
	if _, err := guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, hostGrant); err != nil {
		t.Fatalf("GuestConfirm: %v", err)
	}

	// Host session is ACTIVE.
	final, _ := host.mgr.HostSessionStatus(create.SessionID)
	if !final.Active {
		t.Fatalf("host session not active: %s", final.State)
	}

	// Both operators broadcast exactly one tx-33.
	if host.count() != 1 {
		t.Fatalf("host broadcasts = %d, want 1", host.count())
	}
	if guest.count() != 1 {
		t.Fatalf("guest broadcasts = %d, want 1", guest.count())
	}

	// Both persisted the peer CA + the shared seed, and flipped seed_established.
	assertFile(t, filepath.Join(guest.certsDir, "federation", "host-aaaaaa", tlsca.CACertFile))
	assertFile(t, filepath.Join(guest.certsDir, "federation", "host-aaaaaa", "totp.seed.json"))
	assertFile(t, filepath.Join(host.certsDir, "federation", "guest-bbbbbb", tlsca.CACertFile))
	assertFile(t, filepath.Join(host.certsDir, "federation", "guest-bbbbbb", "totp.seed.json"))
	if !guest.mgr.seedEstablished("host-aaaaaa") {
		t.Fatal("guest seed_established not set")
	}
	if !host.mgr.seedEstablished("guest-bbbbbb") {
		t.Fatal("host seed_established not set")
	}
}

// TestJoinApproveWrongCodeRejected: a host that types a code that does not match
// what the guest read cannot approve (approval #1 is the anchor).
func TestJoinApproveWrongCodeRejected(t *testing.T) {
	host := newCeremonyNode(t, "host-cccccc")
	guest := newCeremonyNode(t, "guest-dddddd")

	hostTLS, _ := host.mgr.ServerTLSConfig()
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	defer srv.Close()
	ctx := context.Background()
	guestEndpoint := "https://127.0.0.1:8444"

	create, err := host.mgr.HostCreate(srv.URL)
	if err != nil {
		t.Fatalf("HostCreate: %v", err)
	}
	scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
	if err != nil {
		t.Fatalf("GuestScan: %v", err)
	}
	if scanErr := host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI); scanErr != nil {
		t.Fatalf("HostScanReturn: %v", scanErr)
	}
	if _, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, ScopeWire{AllowedDomains: []string{"*"}}); err != nil {
		t.Fatalf("GuestRequest: %v", err)
	}
	grant := ScopeWire{MaxClearance: 0, AllowedDomains: []string{"*"}}
	if err := host.mgr.HostApprove(create.SessionID, "000000", grant); err == nil {
		t.Fatal("HostApprove accepted a wrong code")
	}
	if host.count() != 0 {
		t.Fatalf("a rejected approval broadcast a tx (%d)", host.count())
	}
}

// TestJoinCeremonyConcurrentPolls exercises the snapshot fix under -race: many
// goroutines poll the host session view (reading Seed/State/GuestPin/etc.) while
// the ceremony mutates those exact fields under the store lock.
func TestJoinCeremonyConcurrentPolls(t *testing.T) {
	host := newCeremonyNode(t, "host-eeeeee")
	guest := newCeremonyNode(t, "guest-ffffff")

	hostTLS, _ := host.mgr.ServerTLSConfig()
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	defer srv.Close()
	ctx := context.Background()
	guestEndpoint := "https://127.0.0.1:8444"

	create, err := host.mgr.HostCreate(srv.URL)
	if err != nil {
		t.Fatalf("HostCreate: %v", err)
	}
	scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
	if err != nil {
		t.Fatalf("GuestScan: %v", err)
	}
	if scanErr := host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI); scanErr != nil {
		t.Fatalf("HostScanReturn: %v", scanErr)
	}

	// Hammer the host view + guest request/approve/confirm concurrently.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _ = host.mgr.HostSessionStatus(create.SessionID)
				}
			}
		}()
	}

	grant := ScopeWire{MaxClearance: 1, AllowedDomains: []string{"*"}, Mode: "exchange", Direction: "both"}
	greq, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, grant)
	if err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("GuestRequest: %v", err)
	}
	if err := host.mgr.HostApprove(create.SessionID, greq.CodeG, grant); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("HostApprove: %v", err)
	}
	if _, err := guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, grant); err != nil {
		close(stop)
		wg.Wait()
		t.Fatalf("GuestConfirm: %v", err)
	}
	close(stop)
	wg.Wait()

	final, _ := host.mgr.HostSessionStatus(create.SessionID)
	if !final.Active {
		t.Fatalf("host session not active: %s", final.State)
	}
}

func assertFile(t *testing.T, path string) {
	t.Helper()
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("expected file %s: %v", path, err)
	}
}
