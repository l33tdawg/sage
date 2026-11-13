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
	"go/build"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/orgpolicy"
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

// §7.1 PeerAllowList is not enforced on QR JOIN.
func TestRFC443Baseline_QRJoinIgnoresPeerAllowList(t *testing.T) {
	// The federation package (which owns JOIN and ActiveAgreement) does not
	// depend on orgpolicy at all, so no allow-list can reach the ceremony.
	pkg, err := build.ImportDir(".", 0)
	require.NoError(t, err)
	for _, imp := range pkg.Imports {
		require.NotEqual(t, "github.com/l33tdawg/sage/internal/orgpolicy", imp,
			"federation (non-test) now imports orgpolicy: baseline changed")
	}

	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")

	// A current, signed org policy whose allow-list names some OTHER agent,
	// never the guest (by agent key) nor its chain id.
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	_, orgKey, _ := ed25519.GenerateKey(rand.Reader)
	now := time.Now().Unix()
	pol := &orgpolicy.Policy{
		OrgID: "org-acme", Scope: "fleet", Revision: 1, KeyEpoch: "epoch-1",
		IssuedAt: now, NotBefore: now - 60, Expiry: now + 3600,
		Payload: orgpolicy.Payload{
			Judge: orgpolicy.Judge{
				ModelTag:   "sage-memory-judge:v16",
				GGUFSHA256: strings.Repeat("ab", 32),
				BlobSHA256: strings.Repeat("cd", 32),
			},
			ChecksVersion: "sage-lasting/1+supported/1",
			PeerAllowList: []string{hex.EncodeToString(otherPub)},
		},
	}
	require.NoError(t, pol.Sign(orgKey))
	require.NoError(t, pol.Verify(orgKey.Public().(ed25519.PublicKey)))
	require.True(t, pol.CurrentAt(now))
	guestKey := hex.EncodeToString(guest.mgr.agentPub)
	require.NotContains(t, pol.Payload.PeerAllowList, guestKey)
	require.NotContains(t, pol.Payload.PeerAllowList, "guest-bbbbbb")

	_, err = rfc443Join(t, host, guest)
	require.NoError(t, err, "QR JOIN with a peer outside the allow-list")
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.NoError(t, err, "host agreement active")
	_, err = guest.mgr.ActiveAgreement("host-aaaaaa")
	require.NoError(t, err, "guest agreement active")
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
