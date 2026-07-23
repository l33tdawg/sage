package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/totp"
	"github.com/l33tdawg/sage/internal/tx"
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
	if err = badger.SaveValidators(map[string]int64{"local-validator": 1}); err != nil {
		t.Fatalf("seed validator set: %v", err)
	}
	t.Cleanup(func() { _ = badger.CloseBadger() })
	sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(dir, "sage.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = sqlite.Close() })

	node := &ceremonyNode{certsDir: dir}
	node.mgr = NewManager(Config{
		LocalChainID: chainID,
		CertsDir:     dir,
		AgentKey:     priv,
		Badger:       badger,
		MemStore:     sqlite,
		Logger:       zerolog.Nop(),
	})
	node.mgr.broadcastFn = func(txBytes []byte) (string, int64, error) {
		// The production broadcast seam returns only after broadcast_tx_commit;
		// mirror that contract so post-commit JOIN initialization can resolve the
		// newly active trust-only agreement from consensus state.
		parsed, decodeErr := tx.DecodeTx(txBytes)
		if decodeErr != nil {
			return "", 0, decodeErr
		}
		if parsed.Type == tx.TxTypeCrossFedSet && parsed.CrossFedTerms != nil {
			terms := parsed.CrossFedTerms
			if setErr := badger.SetCrossFed(terms.RemoteChainID, terms.Endpoint, terms.PeerPubKey,
				uint8(terms.MaxClearance), terms.ExpiresAt, terms.AllowedDomains, terms.AllowedDepts, terms.Status); setErr != nil {
				return "", 0, setErr
			}
		}
		if parsed.Type == tx.TxTypeCrossFedRevoke && parsed.CrossFedRevoke != nil {
			if revokeErr := badger.UpdateCrossFedStatus(parsed.CrossFedRevoke.RemoteChainID, "revoked"); revokeErr != nil {
				return "", 0, revokeErr
			}
		}
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

func TestJoinHTTPTransportRejectsNonLocalLANDial(t *testing.T) {
	tr := joinHTTPTransport(&tls.Config{MinVersion: tls.VersionTLS13})
	_, err := tr.DialContext(context.Background(), "tcp", "8.8.8.8:443")
	if err == nil || !strings.Contains(err.Error(), "refusing non-local/LAN host") {
		t.Fatalf("public IP dial err=%v, want local/LAN refusal", err)
	}
	_, err = tr.DialContext(context.Background(), "tcp", "example.com:443")
	if err == nil || !strings.Contains(err.Error(), "refusing non-local/LAN host") {
		t.Fatalf("DNS host dial err=%v, want local/LAN refusal", err)
	}
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
	scopeG := trustOnlyJoinScope
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
	if view.GuestScope == nil || len(view.GuestScope.AllowedDomains) != 0 ||
		view.GuestScope.MaxClearance != trustOnlyJoinScope.MaxClearance {
		t.Fatalf("host status omitted the fixed trust-only scope: %+v", view.GuestScope)
	}

	// Approval #1: host types the code it heard, sets its grant, freezes E.
	hostGrant := trustOnlyJoinScope
	if approveErr := host.mgr.HostApprove(create.SessionID, greq.CodeG, hostGrant); approveErr != nil {
		t.Fatalf("HostApprove: %v", approveErr)
	}

	// After approval the host reveals CODE_H; the guest already computed it.
	view2, _ := host.mgr.HostSessionStatus(create.SessionID)
	if view2.CodeH == "" || view2.CodeH != greq.CodeH {
		t.Fatalf("CODE_H disagreement: host=%q guest=%q", view2.CodeH, greq.CodeH)
	}

	// The guest wizard learns of the approval by polling /fed/v1/join/status
	// over the REAL router — this leg carries a query string and 404'd in
	// v11.4.8/9 (netguard.JoinPath escaped the '?'), stalling every ceremony
	// at "1 of 2 confirmed". Keep it exercised end-to-end.
	polled, pollErr := guest.mgr.GuestPollStatus(ctx, create.SessionID)
	if pollErr != nil {
		t.Fatalf("GuestPollStatus: %v", pollErr)
	}
	if !polled.HostApproved {
		t.Fatal("GuestPollStatus: host approval not visible to the guest")
	}
	if polled.HostScope == nil || polled.HostScope.MaxClearance != int(hostGrant.MaxClearance) {
		t.Fatalf("GuestPollStatus: host scope missing or wrong: %+v", polled.HostScope)
	}

	// Approval #2: guest confirms - broadcasts its tx-33, then the host confirms
	// against the frozen E and broadcasts its tx-33.
	firstTx, confirmErr := guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, hostGrant)
	if confirmErr != nil {
		t.Fatalf("GuestConfirm: %v", confirmErr)
	}
	replayedTx, replayErr := guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, hostGrant)
	if replayErr != nil || replayedTx != firstTx {
		t.Fatalf("GuestConfirm idempotent replay tx=%q err=%v, want %q", replayedTx, replayErr, firstTx)
	}
	completedDraft, ok := guest.mgr.getGuestDraft(create.SessionID)
	if !ok || completedDraft.state != guestDraftActive || len(completedDraft.seed) != 0 {
		t.Fatalf("guest completion receipt retained sensitive ceremony state: %+v", completedDraft)
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
	hostGroups, err := host.mgr.syncStore().ListSyncGroups(ctx)
	if err != nil || len(hostGroups) != 0 {
		t.Fatalf("a direct pairing must not create a host group: groups=%v err=%v", hostGroups, err)
	}
	guestGroups, err := guest.mgr.syncStore().ListSyncGroups(ctx)
	if err != nil || len(guestGroups) != 0 {
		t.Fatalf("a direct pairing must not create a guest group: groups=%v err=%v", guestGroups, err)
	}
	hostControl, err := host.mgr.syncStore().GetSyncControl(ctx, "guest-bbbbbb")
	if err != nil || hostControl == nil || hostControl.Role != "host" || hostControl.BindingState != "active" || hostControl.Revision != 0 {
		t.Fatalf("host sync control not default-off/active: %+v err=%v", hostControl, err)
	}
	guestControl, err := guest.mgr.syncStore().GetSyncControl(ctx, "host-aaaaaa")
	if err != nil || guestControl == nil || guestControl.Role != "guest" || guestControl.BindingState != "active" || guestControl.PolicyEpoch != hostControl.PolicyEpoch {
		t.Fatalf("guest sync control mismatch: %+v err=%v", guestControl, err)
	}
}

func TestLANHostCreateRemainsLegacyWithP2PHooks(t *testing.T) {
	host := newCeremonyNode(t, "host-legacy1")
	host.mgr.SetJoinP2PHooks(JoinP2PHooks{LocalBundle: func() (JoinP2PBundle, error) {
		return JoinP2PBundle{PeerID: "new-only", Protocol: "/sage/fed/1.0.0", Addrs: []string{"new-only"}}, nil
	}})
	create, err := host.mgr.HostCreate("https://127.0.0.1:8444")
	if err != nil {
		t.Fatal(err)
	}
	if create.Transport != "" || strings.Contains(create.OTPAuthURI, "x_sage_transport") {
		t.Fatalf("normal LAN QR is not backward-compatible: transport=%q uri=%s", create.Transport, create.OTPAuthURI)
	}
}

func TestHostCreateAutoUsesPreparedRelayBundle(t *testing.T) {
	host := newCeremonyNode(t, "host-auto1")
	bundle := testRouteBundle(t, "203.0.113.80")
	host.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return bundle, nil },
	})
	create, err := host.mgr.HostCreateAuto("https://192.168.1.20:8444")
	require.NoError(t, err)
	assert.Equal(t, "p2p", create.Transport)
	assert.Contains(t, create.OTPAuthURI, "x_sage_transport=p2p")
}

func TestHostCreateAutoFallsBackToLegacyDirectForOldPeers(t *testing.T) {
	host := newCeremonyNode(t, "host-auto2")
	host.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) {
			return JoinP2PBundle{}, errors.New("relay still preparing")
		},
	})
	create, err := host.mgr.HostCreateAuto("https://192.168.1.20:8444")
	require.NoError(t, err)
	assert.Empty(t, create.Transport)
	assert.NotContains(t, create.OTPAuthURI, "x_sage_transport")
}

func TestAutoHostDowngradesTranscriptForLegacyGuestReturn(t *testing.T) {
	host := newCeremonyNode(t, "host-auto3")
	bundle := testRouteBundle(t, "203.0.113.81")
	host.mgr.SetJoinP2PHooks(JoinP2PHooks{
		LocalBundle: func() (JoinP2PBundle, error) { return bundle, nil },
	})
	create, err := host.mgr.HostCreateAuto("https://192.168.1.20:8444")
	require.NoError(t, err)
	require.Equal(t, "p2p", create.Transport)
	legacyReturn := totp.ProvisioningURI(nil, "legacy-guest", "SAGE", bytes.Repeat([]byte{0x42}, 32),
		"https://192.168.1.21:8444", sidForQR(create.SessionID), "guest")
	require.NoError(t, host.mgr.HostScanReturn(create.SessionID, legacyReturn))
	session, ok := host.mgr.JoinStore().Get(create.SessionID, time.Now())
	require.True(t, ok)
	assert.Empty(t, session.HostPeerID)
	assert.Empty(t, session.HostP2PAddrs)
	assert.Empty(t, session.ExpectedGuestPeer)
	assert.Empty(t, session.ExpectedGuestP2P)
}

func TestValidateJoinEndpointRequiresExplicitCompletePort(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		endpoint string
		valid    bool
	}{
		{"https://127.0.0.1:18444", true},
		{" https://127.0.0.1:18444 ", false},
		{"https://127.0.0.1:443", true},
		{"https://[::1]:443", true},
		{"https://[fd00::20]:18444", true},
		{"https://127.0.0.1:", false},
		{"https://[::1]:", false},
		{"https://127.0.0.1", false},
		{"https://[::1]", false},
		{"https:127.0.0.1:18444", false},
		{"https://fd00::20:18444", false},
		{"https://127.0.0.1:0", false},
		{"https://127.0.0.1:65536", false},
		{"https://127.0.0.1:nope", false},
		{"https://user@127.0.0.1:18444", false},
		{"https://127.0.0.1:18444/path", false},
		{"https://127.0.0.1:18444?query=1", false},
	} {
		test := test
		t.Run(test.endpoint, func(t *testing.T) {
			err := validateJoinEndpoint(test.endpoint)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
			}
		})
	}
}

func TestGuestDraftClaimsRejectConcurrentRequestAndConfirm(t *testing.T) {
	m := &Manager{guestDrafts: map[string]*guestDraft{
		"session": {sessionID: "session", seed: []byte("secret"), expiresAt: time.Now().Add(time.Minute), state: guestDraftScanned},
	}}
	request, _, ok := m.claimGuestDraft("session", []guestDraftState{guestDraftScanned}, guestDraftRequesting)
	require.True(t, ok)
	_, state, ok := m.claimGuestDraft("session", []guestDraftState{guestDraftScanned}, guestDraftRequesting)
	assert.False(t, ok)
	assert.Equal(t, guestDraftRequesting, state)
	require.True(t, m.finishGuestRequest(request))

	_, _, ok = m.claimGuestDraft("session", []guestDraftState{guestDraftRequested}, guestDraftConfirming)
	require.True(t, ok)
	_, state, ok = m.claimGuestDraft("session", []guestDraftState{guestDraftRequested}, guestDraftConfirming)
	assert.False(t, ok)
	assert.Equal(t, guestDraftConfirming, state)
	require.True(t, m.transitionGuestDraft("session", 0, guestDraftConfirming, guestDraftLocalActive))
	_, previous, ok := m.claimGuestDraft("session", []guestDraftState{guestDraftLocalActive}, guestDraftConfirming)
	require.True(t, ok, "one-sided activation must allow host-confirm retry without rebroadcast")
	assert.Equal(t, guestDraftLocalActive, previous)

	// ABA ownership: an old claim can never mutate or delete a newer draft
	// that happens to reuse the same session id and state.
	m2 := &Manager{guestDrafts: map[string]*guestDraft{
		"session": {sessionID: "session", generation: 1, seed: []byte("old"), expiresAt: time.Now().Add(time.Minute), state: guestDraftConfirming},
	}}
	m2.guestMu.Lock()
	m2.guestDrafts["session"] = &guestDraft{sessionID: "session", generation: 2, seed: []byte("new"), expiresAt: time.Now().Add(time.Minute), state: guestDraftScanned}
	m2.guestMu.Unlock()
	assert.False(t, m2.transitionGuestDraft("session", 1, guestDraftConfirming, guestDraftLocalActive))
	m2.dropGuestDraft("session", 1)
	newer, ok := m2.getGuestDraft("session")
	require.True(t, ok)
	assert.Equal(t, uint64(2), newer.generation)
	assert.Equal(t, guestDraftScanned, newer.state)
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
	if _, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope); err != nil {
		t.Fatalf("GuestRequest: %v", err)
	}
	grant := trustOnlyJoinScope
	if err := host.mgr.HostApprove(create.SessionID, "000000", grant); err == nil {
		t.Fatal("HostApprove accepted a wrong code")
	}
	if host.count() != 0 {
		t.Fatalf("a rejected approval broadcast a tx (%d)", host.count())
	}
}

func TestJoinRejectsRewrittenHostQREndpointBeforeActivation(t *testing.T) {
	host := newCeremonyNode(t, "host-endpoint")
	guest := newCeremonyNode(t, "guest-endpoint")
	hostTLS, _ := host.mgr.ServerTLSConfig()
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	defer srv.Close()

	create, err := host.mgr.HostCreate(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	// Preserve the live listener port but rewrite the endpoint inside the QR.
	// Both names are on the test certificate, so CA fetching still succeeds and
	// the protocol—not routing—must detect the changed signed transcript.
	endpointURL, err := url.Parse(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	endpointURL.Host = "localhost:" + endpointURL.Port()
	qrURL, err := url.Parse(create.OTPAuthURI)
	if err != nil {
		t.Fatal(err)
	}
	query := qrURL.Query()
	query.Set("x_sage_ep", endpointURL.String())
	qrURL.RawQuery = query.Encode()

	scan, err := guest.mgr.GuestScan(context.Background(), qrURL.String(), "https://127.0.0.1:19444")
	if err != nil {
		t.Fatalf("GuestScan rewritten reachable endpoint: %v", err)
	}
	if scanErr := host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI); scanErr != nil {
		t.Fatal(scanErr)
	}
	_, err = guest.mgr.GuestRequest(context.Background(), create.SessionID, "https://127.0.0.1:19444", trustOnlyJoinScope)
	if err == nil || !strings.Contains(err.Error(), "host endpoint does not match") {
		t.Fatalf("rewritten host endpoint request err=%v", err)
	}
	if host.count() != 0 || guest.count() != 0 {
		t.Fatalf("endpoint mismatch broadcast txs host=%d guest=%d", host.count(), guest.count())
	}
	view, err := host.mgr.HostSessionStatus(create.SessionID)
	if err != nil || view.GuestChain != "" || view.State != JoinCreated {
		t.Fatalf("host bound a mismatched endpoint: view=%+v err=%v", view, err)
	}
}

func TestGuestConfirmRejectsEndpointChangedAfterReturnQRThenRetriesFrozenEndpoint(t *testing.T) {
	host := newCeremonyNode(t, "host-frozen")
	guest := newCeremonyNode(t, "guest-frozen")
	hostTLS, _ := host.mgr.ServerTLSConfig()
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	defer srv.Close()
	ctx := context.Background()
	guestEndpoint := "https://127.0.0.1:19444"

	create, err := host.mgr.HostCreate(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
	if err != nil {
		t.Fatal(err)
	}
	if scanErr := host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI); scanErr != nil {
		t.Fatal(scanErr)
	}
	codes, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
	if err != nil {
		t.Fatal(err)
	}
	if err := host.mgr.HostApprove(create.SessionID, codes.CodeG, trustOnlyJoinScope); err != nil {
		t.Fatal(err)
	}
	if _, err := guest.mgr.GuestConfirm(ctx, create.SessionID, "https://127.0.0.1:8444", trustOnlyJoinScope); err == nil || !strings.Contains(err.Error(), "endpoint changed") {
		t.Fatalf("changed confirm endpoint err=%v", err)
	}
	if host.count() != 0 || guest.count() != 0 {
		t.Fatalf("changed confirm endpoint broadcast txs host=%d guest=%d", host.count(), guest.count())
	}
	if _, err := guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope); err != nil {
		t.Fatalf("retry with frozen endpoint: %v", err)
	}
	if host.count() != 1 || guest.count() != 1 {
		t.Fatalf("successful frozen retry txs host=%d guest=%d", host.count(), guest.count())
	}
}

func TestJoinConfirmActiveReplayIsAuthenticatedAndDoesNotRebroadcast(t *testing.T) {
	host := newCeremonyNode(t, "host-xxxxx")
	joins, sessionID, _, attestation, guestKey, _ := approvedSession(t)
	host.mgr.joins = joins
	goodSig := SignEnroll(guestKey, attestation, false)
	goodAck := SignEnroll(guestKey, attestation, true)
	boundCert := &x509.Certificate{RawSubjectPublicKeyInfo: randN(48)}
	wrongCert := &x509.Certificate{RawSubjectPublicKeyInfo: randN(48)}
	joins.mu.Lock()
	joins.sessions[sessionID].BoundCertSPKI = SPKIFingerprint(boundCert)
	joins.mu.Unlock()
	const connIP = "10.0.0.77"
	for i := 0; i < connMaxAttempts-connConfirmReserve; i++ {
		require.True(t, joins.AllowConn(connIP, time.Now()))
	}
	require.False(t, joins.AllowConn(connIP, time.Now()), "ordinary retries must stop before the confirm reserve")

	post := func(cert *x509.Certificate, sig, ack []byte, forwardedFor string) (int, JoinConfirmResp) {
		t.Helper()
		body, err := json.Marshal(JoinConfirmWire{
			SessionID: sessionID, GuestSig: hex.EncodeToString(sig), GuestAckSig: hex.EncodeToString(ack),
		})
		if err != nil {
			t.Fatal(err)
		}
		req := httptest.NewRequest(http.MethodPost, "/fed/v1/join/confirm", bytes.NewReader(body))
		req.RemoteAddr = connIP + ":4242"
		req.Header.Set("X-Forwarded-For", forwardedFor)
		req.TLS = &tls.ConnectionState{PeerCertificates: []*x509.Certificate{cert}}
		rr := httptest.NewRecorder()
		host.mgr.joinAuth(joinRateConfirm)(http.HandlerFunc(host.mgr.handleJoinConfirm)).ServeHTTP(rr, req)
		var resp JoinConfirmResp
		if rr.Code == http.StatusOK {
			if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
		}
		return rr.Code, resp
	}

	status1, first := post(boundCert, goodSig, goodAck, "198.51.100.1")
	if status1 != http.StatusOK {
		t.Fatalf("first confirm status=%d", status1)
	}
	if host.count() != 1 {
		t.Fatalf("first confirm broadcasts=%d", host.count())
	}
	if status, _ := post(wrongCert, goodSig, goodAck, "198.51.100.2"); status != http.StatusUnprocessableEntity {
		t.Fatalf("ACTIVE replay with changed certificate status=%d", status)
	}
	if status, _ := post(boundCert, randN(64), goodAck, "198.51.100.3"); status != http.StatusUnprocessableEntity {
		t.Fatalf("ACTIVE replay with changed signature status=%d", status)
	}
	status2, replay := post(boundCert, goodSig, goodAck, "198.51.100.4")
	if status2 != http.StatusOK {
		t.Fatalf("replayed confirm status=%d", status2)
	}
	if host.count() != 1 {
		t.Fatalf("replayed confirm rebroadcast: count=%d", host.count())
	}
	if replay.TxHash != first.TxHash || replay.SyncGroupID != first.SyncGroupID || replay.HostChain != first.HostChain {
		t.Fatalf("replay result changed first=%+v replay=%+v", first, replay)
	}
	if status, _ := post(boundCert, goodSig, goodAck, "203.0.113.99"); status != http.StatusTooManyRequests {
		t.Fatalf("confirm reserve exceeded aggregate mutation cap: status=%d", status)
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

	grant := trustOnlyJoinScope
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
