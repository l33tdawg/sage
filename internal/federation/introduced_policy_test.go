package federation

// RFC #443 Phase B2c: applied pauses and auto-resume, the card rotation
// split, rekey (engine level, with crash points, and end to end over the
// intro wire), and the stale refusal record.

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
)

// rotateCA gives n a fresh CA (and node cert) and restarts it, as an
// operator CA rotation would.
func rotateCA(t *testing.T, n *introNode) {
	t.Helper()
	for _, f := range []string{tlsca.CACertFile, tlsca.CAKeyFile, tlsca.NodeCertFile, tlsca.NodeKeyFile} {
		require.NoError(t, os.Remove(filepath.Join(n.dir, f)))
	}
	caCert, caKey, err := tlsca.LoadOrGenerateCA(n.dir, n.chain)
	require.NoError(t, err)
	nodeCert, nodeKey, err := tlsca.GenerateNodeCert(caCert, caKey, "node", []string{"127.0.0.1", "localhost"})
	require.NoError(t, err)
	require.NoError(t, tlsca.WriteCert(filepath.Join(n.dir, tlsca.NodeCertFile), nodeCert))
	require.NoError(t, tlsca.WriteKey(filepath.Join(n.dir, tlsca.NodeKeyFile), nodeKey))
	n.restart()
}

// cardOf is n's roster card as the activation fixtures describe it (E_intro
// carries card_rev 1 and digest "c"×64).
func cardOf(n *introNode, rev uint64, digest string) IntroCard {
	return IntroCard{AgentPub: n.agentHex(), ChainID: n.chain, Pin: n.pinHex(), Endpoint: n.endpoint(), CardRev: rev, CardDigest: digest}
}

// activeIntroHost binds a host link and installs a fake org source.
func activeIntroHost(t *testing.T) (*introPair, string, *fakeIntroSource) {
	t.Helper()
	p := newIntroPair(t)
	res, err := p.host.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkActive, res.State)
	src := &fakeIntroSource{pins: map[string]bool{}}
	p.host.onOpen = func(m *Manager) { m.SetIntroOrgSource(src) }
	p.host.mgr.SetIntroOrgSource(src)
	return p, res.LinkID, src
}

func lastHistory(t *testing.T, n *introNode, linkID string) store.IntroducedLinkHistory {
	t.Helper()
	h, err := n.sqlite.ListIntroducedLinkHistory(context.Background(), linkID)
	require.NoError(t, err)
	require.NotEmpty(t, h)
	return h[len(h)-1]
}

func TestIntroPolicy_PauseTriggersAndResume(t *testing.T) {
	ctx := context.Background()
	p, linkID, src := activeIntroHost(t)
	h := p.host
	card := cardOf(p.guest, 1, strings.Repeat("c", 64))
	ready := func(states ...IntroPeerState) *IntroOrgPeers { return &IntroOrgPeers{Ready: true, Peers: states} }
	resume := func(name string) {
		src.setPeers(ready(IntroPeerState{State: "paused", Resumable: true, Card: card}))
		require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
		require.Equal(t, store.IntroducedLinkActive, h.row(linkID).State, name+": resume")
		_, err := h.mgr.ActiveAgreement(p.guest.chain)
		require.NoError(t, err, name)
	}
	otherChain := card
	otherChain.ChainID = "intro-elsewhere"
	for _, tc := range []struct {
		name  string
		peers *IntroOrgPeers
		want  string
	}{
		{"roster stale", &IntroOrgPeers{Reason: "accepted roster is not current"}, "org not ready: accepted roster is not current"},
		{"envelope expired", &IntroOrgPeers{Reason: "envelope expired"}, "envelope expired"},
		{"no accepted policy", &IntroOrgPeers{Reason: "no accepted org policy"}, "no accepted org policy"},
		{"card removed", ready(), "no longer in the accepted roster"},
		{"chain id changed", ready(IntroPeerState{State: "active", Card: otherChain}), "a chain id change is refused"},
		{"left the allow-list", ready(IntroPeerState{State: "pause_required", Reason: "agent key is not in PeerAllowList", Card: card}), "PeerAllowList"},
		{"card expired", ready(IntroPeerState{State: "pause_required", Reason: "peer card is not current", Card: card}), "not current"},
		{"envelope narrowed", ready(IntroPeerState{State: "pause_required", Reason: "domain hr is outside the envelope", Card: card}), "outside the envelope"},
	} {
		src.setPeers(tc.peers)
		require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx), tc.name)
		row := h.row(linkID)
		require.Equal(t, store.IntroducedLinkPaused, row.State, tc.name)
		require.Contains(t, row.StateReason, tc.want, tc.name)
		requireDenied(t, h, p.guest.chain)
		// A second pass changes nothing.
		require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
		require.Equal(t, store.IntroducedLinkPaused, h.row(linkID).State)
		resume(tc.name)
	}

	// Resume refusals: not resumable, or the card moved while paused.
	src.setPeers(&IntroOrgPeers{Reason: "accepted roster is not current"})
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	src.setPeers(ready(IntroPeerState{State: "paused", Reason: "a CA pin changed while paused; a new ask is required", Card: card}))
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	require.Equal(t, store.IntroducedLinkPaused, h.row(linkID).State, "not resumable")
	moved := cardOf(p.guest, 2, strings.Repeat("d", 64))
	src.setPeers(ready(IntroPeerState{State: "paused", Resumable: true, Card: moved}))
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	require.Equal(t, store.IntroducedLinkPaused, h.row(linkID).State, "the card changed while paused: a new ask")
	resume("same card again")

	// An org whose state cannot be read leaves its rows alone.
	src.mu.Lock()
	src.peerErr = os.ErrPermission
	src.mu.Unlock()
	require.Error(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	require.Equal(t, store.IntroducedLinkActive, h.row(linkID).State)

	sets, revokes := h.counts()
	require.Equal(t, 1, sets, "pausing and resuming need no tx")
	require.Zero(t, revokes)
}

func TestIntroPolicy_RotationSplit(t *testing.T) {
	ctx := context.Background()
	p, linkID, src := activeIntroHost(t)
	h := p.host
	ready := func(s IntroPeerState) { src.setPeers(&IntroOrgPeers{Ready: true, Peers: []IntroPeerState{s}}) }

	// Endpoint-only change: a tx-33 upsert under the lease; the generation
	// leaves the endpoint out, so the same row keeps governing.
	moved := cardOf(p.guest, 2, strings.Repeat("d", 64))
	moved.Endpoint = "https://intro-guest-moved:9443"
	ready(IntroPeerState{State: "active", Card: moved})
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	sets, _ := h.counts()
	require.Equal(t, 2, sets, "one endpoint upsert")
	terms, err := h.mgr.chainTerms(p.guest.chain)
	require.NoError(t, err)
	require.Equal(t, moved.Endpoint, terms.Endpoint)
	require.Equal(t, store.IntroducedLinkActive, h.row(linkID).State)
	ag, err := h.mgr.ActiveAgreement(p.guest.chain)
	require.NoError(t, err, "the upserted agreement is still governed by the active row")
	require.Equal(t, moved.Endpoint, ag.Endpoint)
	require.Contains(t, lastHistory(t, h, linkID).Reason, "card_rotated rev 2: endpoint")
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	sets, _ = h.counts()
	require.Equal(t, 2, sets, "a followed card is not applied again")

	// Route-only change: no tx, a history entry.
	routed := moved
	routed.CardRev, routed.CardDigest, routed.Routes = 3, strings.Repeat("e", 64), []string{"/ip4/192.0.2.7/tcp/4001"}
	ready(IntroPeerState{State: "active", Card: routed})
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	sets, _ = h.counts()
	require.Equal(t, 2, sets)
	require.Contains(t, lastHistory(t, h, linkID).Reason, "card_rotated rev 3: routes")
	// An older card never rolls the link back.
	ready(IntroPeerState{State: "active", Card: moved})
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	require.Contains(t, lastHistory(t, h, linkID).Reason, "rev 3")

	// A CA pin change: rekey_required, and traffic stops.
	ready(IntroPeerState{State: "rekey_required", Kind: "rekey", OfferID: strings.Repeat("ab", 32), Card: routed})
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	require.Equal(t, store.IntroducedLinkRekeyRequired, h.row(linkID).State)
	requireDenied(t, h, p.guest.chain)
	// rekey_required still pauses when coverage goes.
	ready(IntroPeerState{State: "pause_required", Reason: "peer card is not current", Card: routed})
	require.NoError(t, h.mgr.ApplyIntroducedLinkPolicy(ctx))
	require.Equal(t, store.IntroducedLinkPaused, h.row(linkID).State)
}

// rekeyHost binds a host link, rotates the guest CA and returns a rekey
// activation for the host.
func rekeyHost(t *testing.T) (*introPair, string, *IntroActivation) {
	t.Helper()
	p := newIntroPair(t)
	res, err := p.host.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
	require.NoError(t, err)
	rotateCA(t, p.guest)
	p.kind, p.nonce = "rekey", 2
	p.seed = make([]byte, 32)
	_, _ = rand.Read(p.seed)
	return p, res.LinkID, p.activation(t, "host")
}

func requireRekeyed(t *testing.T, n, peer *introNode, oldLink, newLink string, epoch uint32) {
	t.Helper()
	ctx := context.Background()
	require.Equal(t, store.IntroducedLinkActive, n.row(newLink).State)
	old := n.row(oldLink)
	require.Equal(t, store.IntroducedLinkRevoked, old.State)
	require.Equal(t, "superseded_by_rekey: "+newLink, old.StateReason)
	ag, err := n.mgr.ActiveAgreement(peer.chain)
	require.NoError(t, err)
	require.Equal(t, peer.pinHex(), hex.EncodeToString(ag.PeerPubKey))
	c, err := n.sqlite.GetSyncControl(ctx, peer.chain)
	require.NoError(t, err)
	require.Equal(t, peer.pinHex(), c.RemoteCAPin)
	require.Equal(t, "active", c.BindingState)
	require.Equal(t, introEpochOf(old), c.PolicyEpoch, "the binding (and its policy) is kept")
	env, err := n.mgr.readSeedHeader(peer.chain)
	require.NoError(t, err)
	require.Equal(t, epoch, env.Epoch, "the pair seed rotated")
	_, err = os.Stat(n.mgr.seedPrevPath(peer.chain))
	require.NoError(t, err, "the previous epoch is kept")
	ca, err := os.ReadFile(filepath.Join(n.dir, "federation", peer.chain, tlsca.CACertFile))
	require.NoError(t, err)
	require.Equal(t, string(peer.caPEM()), string(ca), "the peer CA was replaced")
}

func TestIntroRekey_HostBindsNewPin(t *testing.T) {
	p, oldLink, act := rekeyHost(t)
	h := p.host
	res, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkActive, res.State)
	requireRekeyed(t, h, p.guest, oldLink, res.LinkID, 2)
	row := h.row(res.LinkID)
	require.Equal(t, store.IntroducedLinkKindRekey, row.Kind)
	sets, revokes := h.counts()
	require.Equal(t, 2, sets, "one upsert of the same remote chain")
	require.Zero(t, revokes)
	all, err := h.mgr.badger.ListCrossFed()
	require.NoError(t, err)
	require.Len(t, all, 1, "no second agreement")
}

func TestIntroRekey_CrashAfterCommitResumes(t *testing.T) {
	p, oldLink, act := rekeyHost(t)
	h := p.host
	h.mgr.introFaultFn = func(pt string) bool { return pt == "after_commit" }
	linkID := linkIDOf(act)
	_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
	require.ErrorIs(t, err, errIntroSimulatedCrash)
	h.restart()
	requireDenied(t, h, p.guest.chain) // committed, not bound: the new generation's row denies
	require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
	requireRekeyed(t, h, p.guest, oldLink, linkID, 2)
}

func TestIntroRekey_CrashBeforeBroadcastKeepsOldLink(t *testing.T) {
	p, oldLink, act := rekeyHost(t)
	h := p.host
	h.mgr.introFaultFn = func(pt string) bool { return pt == "after_intent" }
	linkID := linkIDOf(act)
	_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
	require.ErrorIs(t, err, errIntroSimulatedCrash)
	h.restart()
	require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
	require.Equal(t, store.IntroducedLinkAborted, h.row(linkID).State)
	require.Equal(t, store.IntroducedLinkActive, h.row(oldLink).State, "the old link is untouched")
	_, err = h.mgr.ActiveAgreement(p.guest.chain)
	require.NoError(t, err, "the old link still governs the chain's agreement")
	sets, _ := h.counts()
	require.Equal(t, 1, sets)
}

// On the side whose own CA changed, the rekey commits the same peer pin, so
// the rekey row shares the generation of the link it replaces. An aborted
// attempt must hand governance back; a later attempt then succeeds.
func TestIntroRekey_SameGenerationGuest(t *testing.T) {
	ctx := context.Background()
	p := newIntroPair(t)
	g := p.guest
	res, err := g.mgr.ActivateIntroducedLink(ctx, p.activation(t, "guest"))
	require.NoError(t, err)
	require.NoError(t, g.mgr.GuestIntroConfirmed(ctx, res.LinkID))
	oldLink := res.LinkID
	rotateCA(t, g)
	p.kind, p.nonce = "rekey", 2
	p.seed = make([]byte, 32)
	_, _ = rand.Read(p.seed)

	act := p.activation(t, "guest")
	g.mgr.introFaultFn = func(pt string) bool { return pt == "after_intent" }
	_, err = g.mgr.ActivateIntroducedLink(ctx, act)
	require.ErrorIs(t, err, errIntroSimulatedCrash)
	g.restart()
	require.NoError(t, g.mgr.ReconcileIntroducedLinks(ctx))
	require.Equal(t, store.IntroducedLinkAborted, g.row(linkIDOf(act)).State)
	_, err = g.mgr.ActiveAgreement(p.host.chain)
	require.NoError(t, err, "the aborted rekey does not take over the shared generation")

	p.nonce = 3
	act = p.activation(t, "guest")
	res, err = g.mgr.ActivateIntroducedLink(ctx, act)
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkBoundPendingPeer, res.State)
	requireDenied(t, g, p.host.chain) // one-sided until the host confirms
	require.NoError(t, g.mgr.GuestIntroConfirmed(ctx, res.LinkID))
	requireRekeyed(t, g, p.host, oldLink, res.LinkID, 2)
}

func TestIntroRekey_GateRefusals(t *testing.T) {
	ctx := context.Background()
	t.Run("terms do not carry over", func(t *testing.T) {
		p, _, act := rekeyHost(t)
		act.Terms.MaxClearance = 2
		_, err := p.host.mgr.ActivateIntroducedLink(ctx, act)
		var stale *IntroStaleError
		require.ErrorAs(t, err, &stale)
		require.Contains(t, stale.Which, "carry over")
	})
	t.Run("manual agreement", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		pin, _ := hex.DecodeString(p.guest.pinHex())
		require.NoError(t, h.badger.SetCrossFed(p.guest.chain, p.guest.endpoint(), pin, 4, 0, nil, nil, "active"))
		p.kind = "rekey"
		_, err := h.mgr.ActivateIntroducedLink(ctx, p.activation(t, "host"))
		var stale *IntroStaleError
		require.ErrorAs(t, err, &stale)
		require.Contains(t, stale.Which, "not an introduced link")
	})
	t.Run("paused link", func(t *testing.T) {
		p, oldLink, act := rekeyHost(t)
		h := p.host
		old := h.row(oldLink)
		ok, err := h.sqlite.TransitionIntroducedLink(ctx, store.IntroducedLinkTransition{
			LinkID: oldLink, From: store.IntroducedLinkActive, To: store.IntroducedLinkPaused, Reason: "test", At: time.Now().Unix(), ExpectTxHash: old.TxHash,
		})
		require.NoError(t, err)
		require.True(t, ok)
		_, err = h.mgr.ActivateIntroducedLink(ctx, act)
		var stale *IntroStaleError
		require.ErrorAs(t, err, &stale)
		require.Contains(t, stale.Which, "paused")
	})
}

func TestIntroStale_RefusalIsRecorded(t *testing.T) {
	ctx := context.Background()
	p := newIntroPair(t)
	h := p.host
	act := p.activation(t, "host")
	act.StaleCheck = func(context.Context) error { return os.ErrDeadlineExceeded }
	_, err := h.mgr.ActivateIntroducedLink(ctx, act)
	var stale *IntroStaleError
	require.ErrorAs(t, err, &stale)
	refusals, err := h.sqlite.ListIntroducedLinkRefusals(ctx, "")
	require.NoError(t, err)
	require.Len(t, refusals, 1)
	r := refusals[0]
	require.Equal(t, p.guest.chain, r.RemoteChainID)
	require.Equal(t, "org-acme", r.OrgID)
	require.Equal(t, strings.Repeat("0f", 32), r.OfferID)
	require.Equal(t, "new", r.Kind)
	require.Equal(t, "host", r.Role)
	require.Equal(t, stale.Which, r.Reason)
	rows, err := h.sqlite.ListIntroducedLinks(ctx, "")
	require.NoError(t, err)
	require.Empty(t, rows, "still no row")

	// A plan error that never reached the gate is not a stale refusal.
	act = p.activation(t, "host")
	act.StaleCheck = nil
	_, err = h.mgr.ActivateIntroducedLink(ctx, act)
	require.Error(t, err)
	refusals, err = h.sqlite.ListIntroducedLinkRefusals(ctx, "")
	require.NoError(t, err)
	require.Len(t, refusals, 1)
}

// The rekey end to end over the wire: the guest rotates its CA, both owners
// accept the kind-rekey offer, and both sides end active on the new pin with
// one more tx-33 each and no second agreement.
func TestIntroWire_RekeyEndToEnd(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
	waitStates(t, p, store.IntroducedLinkActive, store.IntroducedLinkActive)
	requireActiveBoth(t, p)
	hostOld, guestOld := p.host.latest(p.guest.chain).LinkID, p.guest.latest(p.host.chain).LinkID

	rotateCA(t, p.guest.introNode)
	const rekeyOffer = "1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e1e"
	newCard := p.guest.card()
	newCard.CardRev, newCard.CardDigest = 2, strings.Repeat("9", 64)
	rekey := func(v *IntroOfferView) {
		v.Kind, v.State, v.OfferID, v.Accepted = "rekey", "rekey_required", rekeyOffer, false
		v.ExpiresAt = time.Now().Add(time.Hour).Unix()
	}
	p.host.src.edit(func(v *IntroOfferView) { rekey(v); v.Peer = newCard })
	p.guest.src.edit(func(v *IntroOfferView) { rekey(v); v.Own = newCard })
	p.host.src.mu.Lock()
	p.host.src.pins[newCard.Pin] = true
	p.host.src.mu.Unlock()
	accept(p.host)
	accept(p.guest)
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, rekeyOffer))
	require.Eventually(t, func() bool {
		h, g := p.host.latest(p.guest.chain), p.guest.latest(p.host.chain)
		return h != nil && g != nil && h.Kind == "rekey" && g.Kind == "rekey" &&
			h.State == store.IntroducedLinkActive && g.State == store.IntroducedLinkActive
	}, 20*time.Second, 20*time.Millisecond, "host=%q guest=%q", p.host.state(p.guest.chain), p.guest.state(p.host.chain))

	hr, gr := p.host.latest(p.guest.chain), p.guest.latest(p.host.chain)
	require.Equal(t, hr.LinkID, gr.LinkID, "one E_intro for both sides")
	requireRekeyed(t, p.host.introNode, p.guest.introNode, hostOld, hr.LinkID, 2)
	requireRekeyed(t, p.guest.introNode, p.host.introNode, guestOld, gr.LinkID, 2)
	hs, hrv := p.host.counts()
	gs, grv := p.guest.counts()
	require.Equal(t, 2, hs)
	require.Equal(t, 2, gs)
	require.Zero(t, hrv+grv, "nothing revoked")
	hSeed, ok := p.host.mgr.loadSeedFromDisk(p.host.mgr.seedPath(p.guest.chain))
	require.True(t, ok)
	gSeed, ok := p.guest.mgr.loadSeedFromDisk(p.guest.mgr.seedPath(p.host.chain))
	require.True(t, ok)
	require.Equal(t, hSeed, gSeed, "both sides rotated to the same new seed")
}
