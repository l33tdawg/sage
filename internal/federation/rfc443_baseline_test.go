package federation

// RFC #443 §7 characterization tests: record how main behaves TODAY, before any
// introduced-link code. Every assertion here describes current behaviour, not
// the target contract.

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
)

// rfc443Join drives the real QR JOIN (TOTP codes, handclasp v2) between two
// in-process ceremony nodes over the host's real mTLS listener.
func rfc443Join(t *testing.T, host, guest *ceremonyNode) (string, error) {
	t.Helper()
	hostTLS, err := host.mgr.ServerTLSConfig()
	require.NoError(t, err)
	srv := httptest.NewUnstartedServer(host.mgr.Router())
	srv.TLS = hostTLS
	srv.StartTLS()
	t.Cleanup(srv.Close)
	guestEndpoint := "https://127.0.0.1:8444"
	ctx := context.Background()

	create, err := host.mgr.HostCreate(srv.URL)
	require.NoError(t, err)
	scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
	greq, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostApprove(create.SessionID, greq.CodeG, trustOnlyJoinScope))
	_, err = guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
	return create.SessionID, err
}

// §7.1 flipped in Phase A2: on a fleet-managed node QR JOIN enforces the v2
// PeerAllowList (by agent key); a node with no enrollment is unchanged.
func TestRFC443Baseline_QRJoinEnforcesPeerAllowListOnFleetNode(t *testing.T) {
	// Fleet host whose current signed v2 policy lists some OTHER agent.
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	fleetNode(t, host, hex.EncodeToString(otherPub))
	err := tryJoin(t, host, guest)
	require.Error(t, err, "fleet host joined a peer outside the allow-list")
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.Error(t, err, "no host agreement")

	// Non-fleet pair: a gate is wired but the org store holds no enrollment.
	host2 := newCeremonyNode(t, "host-cccccc")
	guest2 := newCeremonyNode(t, "guest-dddddd")
	sq, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "org.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	ps := orgpolicy.NewStore(sqliteOrgKV{sq})
	host2.mgr.SetJoinPeerGate(func(c JoinGateCheck) error {
		d, aErr := ps.AuthorizeJoinPeer(orgpolicy.JoinPeerRequest{PeerAgentPub: c.PeerAgentPub, PeerChainID: c.PeerChainID, Now: c.Now.Unix()})
		if aErr != nil {
			return aErr
		}
		if !d.Allowed {
			return errors.New(d.Reason)
		}
		return nil
	})
	_, err = rfc443Join(t, host2, guest2)
	require.NoError(t, err, "non-fleet QR JOIN unchanged")
	_, err = host2.mgr.ActiveAgreement("guest-dddddd")
	require.NoError(t, err)
}

// §7.2a JOIN writes tx-33 with ExpiresAt = 0 on both sides.
func TestRFC443Baseline_JoinTx33ExpiresAtZero(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	_, err := rfc443Join(t, host, guest)
	require.NoError(t, err)

	_, _, _, hostExp, _, _, hostSt, err := host.mgr.badger.GetCrossFed("guest-bbbbbb")
	require.NoError(t, err)
	_, _, _, guestExp, _, _, guestSt, err := guest.mgr.badger.GetCrossFed("host-aaaaaa")
	require.NoError(t, err)
	require.Equal(t, "active", hostSt)
	require.Equal(t, "active", guestSt)
	require.Equal(t, int64(0), hostExp, "host tx-33 ExpiresAt")
	require.Equal(t, int64(0), guestExp, "guest tx-33 ExpiresAt")
}

// §7.2b ActiveAgreement fails closed once a set ExpiresAt passes.
func TestRFC443Baseline_ActiveAgreementFailsClosedAfterExpiry(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	_, err := rfc443Join(t, host, guest)
	require.NoError(t, err)

	ep, pk, mc, _, doms, depts, st, err := host.mgr.badger.GetCrossFed("guest-bbbbbb")
	require.NoError(t, err)
	set := func(exp int64) {
		require.NoError(t, host.mgr.badger.SetCrossFed("guest-bbbbbb", ep, pk, mc, exp, doms, depts, st))
	}
	inList := func() bool {
		for _, r := range host.mgr.ActiveAgreements() {
			if r.RemoteChainID == "guest-bbbbbb" {
				return true
			}
		}
		return false
	}

	set(time.Now().Unix() + 3600)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.NoError(t, err, "future ExpiresAt is active")
	require.True(t, inList())

	set(time.Now().Unix() - 1)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "expired")
	require.False(t, inList(), "expired agreement dropped from ActiveAgreements")
}

// §7.3 The host's staged guest CA is committed only after its tx-33 broadcast
// succeeds; a rejected broadcast leaves no guest CA and no host agreement.
func TestRFC443Baseline_HostStagedCACommitsOnlyAfterBroadcast(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	okBroadcast := host.mgr.broadcastFn
	host.mgr.broadcastFn = func([]byte) (string, int64, error) {
		return "", 0, errors.New("rfc443 baseline: broadcast rejected")
	}
	_, err := rfc443Join(t, host, guest)
	require.Error(t, err, "JOIN must fail when the host broadcast is rejected")

	caPath := filepath.Join(host.certsDir, "federation", "guest-bbbbbb", tlsca.CACertFile)
	_, statErr := os.Stat(caPath)
	require.True(t, os.IsNotExist(statErr), "guest CA persisted despite rejected broadcast: %v", statErr)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.Error(t, err, "no host agreement after rejected broadcast")

	// Record (not assert) the guest-first side effect for the contract text.
	_, _, _, _, _, _, gst, gErr := guest.mgr.badger.GetCrossFed("host-aaaaaa")
	t.Logf("guest-side tx-33 after host broadcast failure: status=%q err=%v guest_broadcasts=%d", gst, gErr, guest.count())

	// Positive control: with a working broadcast the same nodes complete and the
	// CA lands on disk.
	host.mgr.broadcastFn = okBroadcast
	host2 := newCeremonyNode(t, "host-cccccc")
	guest2 := newCeremonyNode(t, "guest-dddddd")
	_, err = rfc443Join(t, host2, guest2)
	require.NoError(t, err)
	_, err = os.Stat(filepath.Join(host2.certsDir, "federation", "guest-dddddd", tlsca.CACertFile))
	require.NoError(t, err)
}

// §7.4 tx-34 revoke kills a live JOIN link at the ActiveAgreement gate.
func TestRFC443Baseline_Tx34RevokeKillsLiveJoinLink(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	_, err := rfc443Join(t, host, guest)
	require.NoError(t, err)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.NoError(t, err)

	_, err = host.mgr.RevokeAgreement("guest-bbbbbb")
	require.NoError(t, err)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.Error(t, err)
	require.Contains(t, err.Error(), "revoked")
	for _, r := range host.mgr.ActiveAgreements() {
		require.NotEqual(t, "guest-bbbbbb", r.RemoteChainID)
	}
}
