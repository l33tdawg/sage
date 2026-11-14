package federation

// RFC #443 Phase A2: the fleet-managed manual QR JOIN gate, driven through the
// real two-node ceremony (TOTP codes, handclasp v2, tx-33 on both sides).

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

type gateCall struct {
	side, stage, peer, chain string
}

// recordingGate records every check and refuses the ones refuse matches.
type recordingGate struct {
	mu     sync.Mutex
	calls  []gateCall
	refuse func(JoinGateCheck) bool
}

func (g *recordingGate) gate(c JoinGateCheck) error {
	g.mu.Lock()
	g.calls = append(g.calls, gateCall{c.Side, c.Stage, c.PeerAgentPub, c.PeerChainID})
	g.mu.Unlock()
	if g.refuse != nil && g.refuse(c) {
		return errors.New("test policy refuses " + c.Side + "/" + c.Stage)
	}
	return nil
}

func (g *recordingGate) stages() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, 0, len(g.calls))
	for _, c := range g.calls {
		out = append(out, c.side+"/"+c.stage)
	}
	return out
}

// tryJoin drives the full QR JOIN and returns the first step's error.
func tryJoin(t *testing.T, host, guest *ceremonyNode) error {
	t.Helper()
	srv := startCeremonyTLSServer(t, host).URL
	guestEndpoint := "https://127.0.0.1:8444"
	ctx := context.Background()
	create, err := host.mgr.HostCreate(srv)
	require.NoError(t, err)
	scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
	require.NoError(t, err)
	require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))
	greq, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
	if err != nil {
		return err
	}
	if err = host.mgr.HostApprove(create.SessionID, greq.CodeG, trustOnlyJoinScope); err != nil {
		return err
	}
	_, err = guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
	return err
}

func requireNoCrossFed(t *testing.T, n *ceremonyNode, remote string) {
	t.Helper()
	_, err := n.mgr.ActiveAgreement(remote)
	require.Error(t, err, "agreement for %s must not be active", remote)
	_, _, _, _, _, _, status, _ := n.mgr.badger.GetCrossFed(remote)
	require.NotEqual(t, "active", status, "no active tx-33 for %s", remote)
}

func TestFleetJoinGate_ChecksBothSidesAtRequestAndUnderLease(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	hg, gg := &recordingGate{}, &recordingGate{}
	host.mgr.SetJoinPeerGate(hg.gate)
	guest.mgr.SetJoinPeerGate(gg.gate)

	_, err := rfc443Join(t, host, guest)
	require.NoError(t, err)
	require.Equal(t, []string{"host/request", "host/approve", "host/confirm"}, hg.stages())
	require.Equal(t, []string{"guest/request", "guest/confirm"}, gg.stages())
	for _, c := range hg.calls {
		require.Equal(t, hex.EncodeToString(guest.mgr.agentPub), c.peer)
		require.Equal(t, "guest-bbbbbb", c.chain)
	}
	for _, c := range gg.calls {
		require.Equal(t, hex.EncodeToString(host.mgr.agentPub), c.peer)
		require.Equal(t, "host-aaaaaa", c.chain)
	}
}

func TestFleetJoinGate_Refusals(t *testing.T) {
	cases := []struct {
		name      string
		hostStage string // host stage to refuse ("" none)
		gstStage  string // guest stage to refuse ("" none)
		failAt    string // which step returns the refusal
	}{
		{"host refuses at request", JoinGateStageReq, "", "request"},
		{"guest refuses at request", "", JoinGateStageReq, "request"},
		{"host policy rotated before approve", JoinGateStageApprov, "", "approve"},
		{"guest refuses under lease before its tx-33", "", JoinGateStageConfrm, "confirm"},
		{"host refuses under lease before its tx-33", JoinGateStageConfrm, "", "confirm"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			host := newCeremonyNode(t, "host-aaaaaa")
			guest := newCeremonyNode(t, "guest-bbbbbb")
			host.mgr.SetJoinPeerGate((&recordingGate{refuse: func(c JoinGateCheck) bool { return c.Stage == tc.hostStage }}).gate)
			guest.mgr.SetJoinPeerGate((&recordingGate{refuse: func(c JoinGateCheck) bool { return c.Stage == tc.gstStage }}).gate)

			srv := startCeremonyTLSServer(t, host).URL
			guestEndpoint := "https://127.0.0.1:8444"
			ctx := context.Background()
			create, err := host.mgr.HostCreate(srv)
			require.NoError(t, err)
			scan, err := guest.mgr.GuestScan(ctx, create.OTPAuthURI, guestEndpoint)
			require.NoError(t, err)
			require.NoError(t, host.mgr.HostScanReturn(create.SessionID, scan.ReturnURI))

			greq, err := guest.mgr.GuestRequest(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
			if tc.failAt == "request" {
				require.Error(t, err)
				require.Contains(t, err.Error(), "refuses")
				requireNoCrossFed(t, host, "guest-bbbbbb")
				requireNoCrossFed(t, guest, "host-aaaaaa")
				return
			}
			require.NoError(t, err)
			err = host.mgr.HostApprove(create.SessionID, greq.CodeG, trustOnlyJoinScope)
			if tc.failAt == "approve" {
				require.ErrorIs(t, err, ErrJoinPeerNotEligible)
				requireNoCrossFed(t, host, "guest-bbbbbb")
				requireNoCrossFed(t, guest, "host-aaaaaa")
				return
			}
			require.NoError(t, err)
			_, err = guest.mgr.GuestConfirm(ctx, create.SessionID, guestEndpoint, trustOnlyJoinScope)
			require.Error(t, err)
			// The host never broadcasts its tx-33 in either confirm case.
			requireNoCrossFed(t, host, "guest-bbbbbb")
			if tc.gstStage == JoinGateStageConfrm {
				// Refused before the guest's own tx-33 too.
				requireNoCrossFed(t, guest, "host-aaaaaa")
			}
		})
	}
}

// sqliteOrgKV adapts *store.SQLiteStore to orgpolicy.KV + KVLister, as the
// REST layer does in production.
type sqliteOrgKV struct{ s *store.SQLiteStore }

func (k sqliteOrgKV) GetState(key string) ([]byte, error) {
	return k.s.GetOrgPolicyState(context.Background(), key)
}

func (k sqliteOrgKV) SetStatesAtomic(w []orgpolicy.KVWrite) error {
	sw := make([]store.StateWrite, len(w))
	for i, e := range w {
		sw[i] = store.StateWrite{Key: e.Key, Value: e.Value}
	}
	return k.s.SetOrgPolicyStatesAtomic(context.Background(), sw)
}

func (k sqliteOrgKV) CompareAndSetStatesAtomic(c []orgpolicy.KVCondition, w []orgpolicy.KVWrite) (bool, error) {
	sc := make([]store.StateCondition, len(c))
	for i, e := range c {
		sc[i] = store.StateCondition{Key: e.Key, Value: e.Value}
	}
	sw := make([]store.StateWrite, len(w))
	for i, e := range w {
		sw[i] = store.StateWrite{Key: e.Key, Value: e.Value}
	}
	return k.s.CompareAndSetOrgPolicyStatesAtomic(context.Background(), sc, sw)
}

func (k sqliteOrgKV) ListStateKeys(prefix string, limit int) ([]string, error) {
	return k.s.ListOrgPolicyStateKeys(context.Background(), prefix, limit)
}

// fleetNode enrolls a node's org store in org-acme under a v2 policy that
// lists peers, and installs a gate backed by it (the same decision the REST
// layer wires in production).
func fleetNode(t *testing.T, n *ceremonyNode, peers ...string) (*orgpolicy.Store, ed25519.PrivateKey) {
	t.Helper()
	sq, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "org.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sq.Close() })
	ps := orgpolicy.NewStore(sqliteOrgKV{sq})
	_, orgKey, _ := ed25519.GenerateKey(rand.Reader)
	pol := fleetPolicy(t, orgKey, 1, peers...)
	out := orgpolicy.NewEnrollmentOutcome("tok-1", hex.EncodeToString(n.mgr.agentPub), "owner/node/member", pol, time.Now().Unix())
	require.NoError(t, ps.CommitFirstEnrollment(out))
	n.mgr.SetJoinPeerGate(func(c JoinGateCheck) error {
		d, aErr := ps.AuthorizeJoinPeer(orgpolicy.JoinPeerRequest{
			Side: c.Side, Stage: c.Stage, SessionID: c.SessionID,
			PeerAgentPub: c.PeerAgentPub, PeerChainID: c.PeerChainID,
			RootCredentialID: c.RootCredentialID, RootPub: c.RootPub, Now: c.Now.Unix(),
		})
		if aErr != nil {
			return aErr
		}
		if !d.Allowed {
			return errors.New(d.Reason)
		}
		return nil
	})
	return ps, orgKey
}

func fleetPolicy(t *testing.T, orgKey ed25519.PrivateKey, rev uint64, peers ...string) *orgpolicy.Policy {
	t.Helper()
	now := time.Now().Unix()
	pol := &orgpolicy.Policy{
		Schema: orgpolicy.SchemaIDV2,
		OrgID:  "org-acme", Scope: "fleet", Revision: rev, KeyEpoch: "epoch-1",
		IssuedAt: now, NotBefore: now - 60, Expiry: now + 3600,
		Payload: orgpolicy.Payload{
			Judge: orgpolicy.Judge{
				ModelTag:   "sage-memory-judge:v16",
				GGUFSHA256: strings.Repeat("ab", 32),
				BlobSHA256: strings.Repeat("cd", 32),
			},
			ChecksVersion: "sage-lasting/1+supported/1",
			PeerAllowList: peers,
		},
	}
	require.NoError(t, pol.Sign(orgKey))
	return pol
}

func TestFleetJoinGate_OrgPolicyEndToEnd(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	otherPub, _, _ := ed25519.GenerateKey(rand.Reader)
	ps, orgKey := fleetNode(t, host, hex.EncodeToString(otherPub))

	err := tryJoin(t, host, guest)
	require.Error(t, err, "fleet host joined a peer outside its allow-list")
	require.Contains(t, err.Error(), "allow-list")
	requireNoCrossFed(t, host, "guest-bbbbbb")
	requireNoCrossFed(t, guest, "host-aaaaaa")

	// The org adds the guest's agent key: the same ceremony now completes.
	_, d, _, err := ps.ApplyPolicy(fleetPolicy(t, orgKey, 2, hex.EncodeToString(guest.mgr.agentPub)), orgKey.Public().(ed25519.PublicKey))
	require.NoError(t, err)
	require.Equal(t, orgpolicy.DecisionApplied, d)
	_, err = rfc443Join(t, host, guest)
	require.NoError(t, err)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.NoError(t, err)
	_, err = guest.mgr.ActiveAgreement("host-aaaaaa")
	require.NoError(t, err)
}

func TestFleetJoinGate_RootOverrideEndToEnd(t *testing.T) {
	host := newCeremonyNode(t, "host-aaaaaa")
	guest := newCeremonyNode(t, "guest-bbbbbb")
	ps, _ := fleetNode(t, host) // empty v2 allow-list: nobody is eligible
	guestKey := hex.EncodeToString(guest.mgr.agentPub)

	rootID, rootPub, err := host.mgr.CurrentRootCredential()
	require.NoError(t, err)
	now := time.Now().Unix()

	// An override signed by a key that is not the current Root is refused
	// when stored, and an expired one cannot be stored either.
	_, notRoot, _ := ed25519.GenerateKey(rand.Reader)
	bad := &orgpolicy.JoinOverride{PeerAgentPub: guestKey, PeerChainID: "guest-bbbbbb", Reason: "lab", Revision: 1, IssuedAt: now - 10, ExpiresAt: now + 3600}
	require.NoError(t, bad.Sign(notRoot))
	_, err = ps.ApplyJoinOverride(bad, rootID, rootPub, now)
	require.ErrorIs(t, err, orgpolicy.ErrJoinOverrideInvalid)

	o := &orgpolicy.JoinOverride{PeerAgentPub: guestKey, PeerChainID: "guest-bbbbbb", Reason: "lab peer pending allow-list", Revision: 1, IssuedAt: now - 10, ExpiresAt: now + 3600}
	require.NoError(t, o.Sign(host.mgr.agentKey)) // pre-app-v23 Root = node operator key
	_, err = ps.ApplyJoinOverride(o, rootID, rootPub, now)
	require.NoError(t, err)

	_, err = rfc443Join(t, host, guest)
	require.NoError(t, err)
	_, err = host.mgr.ActiveAgreement("guest-bbbbbb")
	require.NoError(t, err)
	rec, ok, err := ps.LoadJoinOverride(guestKey)
	require.NoError(t, err)
	require.True(t, ok)
	require.EqualValues(t, 3, rec.TotalUses, "host request, approve and confirm each record a use")
	require.Equal(t, "confirm", rec.Uses[2].Stage)
}

func TestMintOrgLinkCard_FromLocalRuntimeState(t *testing.T) {
	n := newCeremonyNode(t, "node-aaaaaa")
	now := time.Now().Unix()
	c, err := n.mgr.MintOrgLinkCard(OrgLinkCardRequest{
		OrgID: "org-acme", CardRev: 1, IssuedAt: now, ExpiresAt: now + 86400, Endpoint: "https://10.0.0.1:8444",
	})
	require.NoError(t, err)
	require.NoError(t, c.Verify())
	pin, err := n.mgr.ownPin()
	require.NoError(t, err)
	require.Equal(t, hex.EncodeToString(n.mgr.agentPub), c.AgentPub)
	require.Equal(t, n.mgr.TransportAgentPub(), c.AgentPub)
	require.Equal(t, "node-aaaaaa", c.ChainID)
	require.Equal(t, hex.EncodeToString(pin), c.CASPKIPin)
	require.Empty(t, c.P2PRoutes, "no p2p transport wired")

	// Deterministic: the same request yields the same digest (retry = no-op).
	again, err := n.mgr.MintOrgLinkCard(OrgLinkCardRequest{
		OrgID: "org-acme", CardRev: 1, IssuedAt: now, ExpiresAt: now + 86400, Endpoint: "https://10.0.0.1:8444",
	})
	require.NoError(t, err)
	require.Equal(t, c.Digest(), again.Digest())

	for name, ep := range map[string]string{
		"no endpoint and no p2p route": "",
		"http scheme":                  "http://10.0.0.1:8444",
		"no port":                      "https://10.0.0.1",
	} {
		_, err := n.mgr.MintOrgLinkCard(OrgLinkCardRequest{OrgID: "org-acme", CardRev: 1, IssuedAt: now, ExpiresAt: now + 86400, Endpoint: ep})
		require.Error(t, err, name)
	}

	n.mgr.transportDisabled.Store(true)
	_, err = n.mgr.MintOrgLinkCard(OrgLinkCardRequest{
		OrgID: "org-acme", CardRev: 1, IssuedAt: now, ExpiresAt: now + 86400, Endpoint: "https://10.0.0.1:8444",
	})
	require.ErrorContains(t, err, "transport is disabled")
}
