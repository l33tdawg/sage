package federation

// RFC #443 Phase B2a: the introduced-link write path, reconciler, and the
// design §3.5 crash and stale cases, on real SQLite and Badger with the
// Manager restarted on the same directories.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/tx"
)

// introNode is a restartable node: its stores live in dir and restart()
// closes and reopens them under a fresh Manager, as a process restart would.
type introNode struct {
	t      *testing.T
	chain  string
	dir    string
	priv   ed25519.PrivateKey
	badger *store.BadgerStore
	sqlite *store.SQLiteStore
	mgr    *Manager

	mu       sync.Mutex
	sets     int
	revokes  int
	lastSet  []byte
	setMode  string // "" ok | "reject" | "indeterminate" | "indeterminate-landed"
	resolver tx.TxResolveFunc
	// onOpen runs after every (re)open, for wire tests that install hooks.
	onOpen func(*Manager)
	// mgrMu guards mgr for readers on other goroutines (a test listener).
	mgrMu sync.Mutex
}

// currentMgr reads mgr for goroutines that race a restart.
func (n *introNode) currentMgr() *Manager {
	n.mgrMu.Lock()
	defer n.mgrMu.Unlock()
	return n.mgr
}

func newIntroNode(t *testing.T, chain string, priv ed25519.PrivateKey) *introNode {
	t.Helper()
	dir := t.TempDir()
	caCert, caKey, err := tlsca.LoadOrGenerateCA(dir, chain)
	require.NoError(t, err)
	nodeCert, nodeKey, err := tlsca.GenerateNodeCert(caCert, caKey, "node", []string{"127.0.0.1", "localhost"})
	require.NoError(t, err)
	require.NoError(t, tlsca.WriteCert(filepath.Join(dir, tlsca.NodeCertFile), nodeCert))
	require.NoError(t, tlsca.WriteKey(filepath.Join(dir, tlsca.NodeKeyFile), nodeKey))
	n := &introNode{t: t, chain: chain, dir: dir, priv: priv}
	n.open()
	t.Cleanup(n.close)
	return n
}

func (n *introNode) open() {
	t := n.t
	var err error
	n.badger, err = store.NewBadgerStore(filepath.Join(n.dir, "badger"))
	require.NoError(t, err)
	require.NoError(t, n.badger.SaveValidators(map[string]int64{"local-validator": 1}))
	n.sqlite, err = store.NewSQLiteStore(context.Background(), filepath.Join(n.dir, "sage.db"))
	require.NoError(t, err)
	mgr := NewManager(Config{
		LocalChainID: n.chain, CertsDir: n.dir, AgentKey: n.priv,
		Badger: n.badger, MemStore: n.sqlite, Logger: zerolog.Nop(),
	})
	mgr.SetIntroducedLinkSource(n.sqlite)
	mgr.broadcastFn = n.broadcast
	mgr.introResolveFn = func(ctx context.Context, b []byte) (tx.TxOutcome, error) {
		n.mu.Lock()
		r := n.resolver
		n.mu.Unlock()
		if r == nil {
			return tx.TxOutcome{Verdict: tx.TxVerdictUnresolved}, nil
		}
		return r(ctx, b)
	}
	if n.onOpen != nil {
		n.onOpen(mgr)
	}
	// Publish only a fully configured Manager.
	n.mgrMu.Lock()
	n.mgr = mgr
	n.mgrMu.Unlock()
}

func (n *introNode) close() {
	if n.mgr != nil {
		n.mgr.StopIntroGuests()
		n.mgr.waitIntroGuests()
	}
	if n.badger != nil {
		_ = n.badger.CloseBadger()
		n.badger = nil
	}
	if n.sqlite != nil {
		_ = n.sqlite.Close()
		n.sqlite = nil
	}
}

func (n *introNode) restart() {
	n.close()
	n.open()
}

func (n *introNode) broadcast(b []byte) (string, int64, error) {
	p, err := tx.DecodeTx(b)
	if err != nil {
		return "", 0, err
	}
	n.mu.Lock()
	mode := n.setMode
	n.mu.Unlock()
	apply := func() error {
		switch {
		case p.Type == tx.TxTypeCrossFedSet && p.CrossFedTerms != nil:
			t := p.CrossFedTerms
			return n.badger.SetCrossFed(t.RemoteChainID, t.Endpoint, t.PeerPubKey, uint8(t.MaxClearance),
				t.ExpiresAt, t.AllowedDomains, t.AllowedDepts, t.Status)
		case p.Type == tx.TxTypeCrossFedRevoke && p.CrossFedRevoke != nil:
			return n.badger.UpdateCrossFedStatus(p.CrossFedRevoke.RemoteChainID, "revoked")
		}
		return nil
	}
	if p.Type == tx.TxTypeCrossFedRevoke {
		if err := apply(); err != nil {
			return "", 0, err
		}
		n.mu.Lock()
		n.revokes++
		n.mu.Unlock()
		return "revoke-hash", 1, nil
	}
	n.mu.Lock()
	n.sets++
	n.lastSet = append([]byte(nil), b...)
	n.mu.Unlock()
	switch mode {
	case "reject":
		return "", 0, errors.New("CheckTx rejected: code 7")
	case "indeterminate":
		return "", 0, tx.Indeterminate(errors.New("commit not observed"), b, nil)
	case "indeterminate-landed":
		if err := apply(); err != nil {
			return "", 0, err
		}
		return "", 0, tx.Indeterminate(errors.New("commit not observed"), b, nil)
	}
	if err := apply(); err != nil {
		return "", 0, err
	}
	return "set-hash", 1, nil
}

func (n *introNode) counts() (sets, revokes int) {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.sets, n.revokes
}

func (n *introNode) agentHex() string { return hex.EncodeToString(n.priv.Public().(ed25519.PublicKey)) }

func (n *introNode) pinHex() string {
	pin, err := n.mgr.ownPin()
	require.NoError(n.t, err)
	return hex.EncodeToString(pin)
}

func (n *introNode) caPEM() []byte {
	b, err := os.ReadFile(filepath.Join(n.dir, tlsca.CACertFile))
	require.NoError(n.t, err)
	return b
}

func (n *introNode) endpoint() string { return "https://" + n.chain + ":8443" }

func (n *introNode) liveCA(remote string) bool {
	_, err := os.Stat(filepath.Join(n.dir, "federation", remote, tlsca.CACertFile))
	return err == nil
}

func (n *introNode) stageDir(remote, linkID string) string {
	return filepath.Join(n.dir, "federation", remote, "intro", linkID)
}

// introPair is a host and a guest node (host = the lower agent_pub).
type introPair struct {
	host, guest *introNode
	seed        []byte
	nonce       byte
}

func newIntroPair(t *testing.T) *introPair {
	t.Helper()
	_, a, _ := ed25519.GenerateKey(rand.Reader)
	_, b, _ := ed25519.GenerateKey(rand.Reader)
	if hex.EncodeToString(a.Public().(ed25519.PublicKey)) > hex.EncodeToString(b.Public().(ed25519.PublicKey)) {
		a, b = b, a
	}
	seed := make([]byte, 32)
	_, _ = rand.Read(seed)
	return &introPair{host: newIntroNode(t, "intro-host", a), guest: newIntroNode(t, "intro-guest", b), seed: seed, nonce: 1}
}

func trustOnlyScopeHex() string {
	s := ScopeDigest(4, nil, "exchange", "both")
	return hex.EncodeToString(s[:])
}

func (p *introPair) side(n *introNode, cardRev uint64) IntroSide {
	return IntroSide{
		ChainID: n.chain, AgentPub: n.agentHex(), Pin: n.pinHex(), Endpoint: n.endpoint(),
		Nonce: hex.EncodeToString(bytes.Repeat([]byte{p.nonce}, 16)), Scope: trustOnlyScopeHex(),
		CardRev: cardRev, CardDigest: strings.Repeat("c", 64), EnvelopeRev: 1, EnvelopeDigest: strings.Repeat("e", 64),
	}
}

// activation builds a fully signed session for one side. Each call with a
// fresh nonce is a new session (a new link id).
func (p *introPair) activation(t *testing.T, role string) *IntroActivation {
	t.Helper()
	tr := &IntroTranscript{
		V: IntroTranscriptVersion, Kind: "new", OfferID: strings.Repeat("0f", 32),
		OrgID: "org-acme", OrgRootKeyID: strings.Repeat("ab", 32),
		PolicyRevision: 3, PolicyDigest: strings.Repeat("11", 32), RosterRevision: 2, RosterDigest: strings.Repeat("22", 32),
		Host: p.side(p.host, 1), Guest: p.side(p.guest, 1), SeedCommit: SeedCommitHex(p.seed),
	}
	enc, err := tr.Encode()
	require.NoError(t, err)
	e := IntroAttestation(enc)
	ha, err := SignIntro(p.host.priv, IntroHostAccept, e)
	require.NoError(t, err)
	ga, err := SignIntro(p.guest.priv, IntroGuestAccept, e)
	require.NoError(t, err)
	gk, err := SignIntro(p.guest.priv, IntroGuestAck, e)
	require.NoError(t, err)
	local, remote := p.host, p.guest
	if role == "guest" {
		local, remote = p.guest, p.host
	}
	pin, _ := hex.DecodeString(remote.pinHex())
	prior, err := local.mgr.IntroTermsDigest(remote.chain)
	require.NoError(t, err)
	return &IntroActivation{
		Role: role, OrgID: "org-acme", SessionID: "S-" + hex.EncodeToString(e[:4]),
		Transcript: enc, HostAcceptSig: ha, GuestAcceptSig: ga, GuestAckSig: gk,
		Evidence: []byte(`{"policy_rev":3}`), RemoteCAPEM: remote.caPEM(),
		Seed:             append([]byte(nil), p.seed...),
		Terms:            &tx.CrossFedTerms{RemoteChainID: remote.chain, Endpoint: remote.endpoint(), PeerPubKey: pin, MaxClearance: 4, Status: "active"},
		PriorTermsDigest: prior, Deadline: time.Now().Unix() + 600,
		StaleCheck: func(context.Context) error { return nil },
	}
}

func linkIDOf(act *IntroActivation) string {
	e := IntroAttestation(act.Transcript)
	return hex.EncodeToString(e[:])
}

func (n *introNode) row(linkID string) *store.IntroducedLink {
	r, err := n.sqlite.GetIntroducedLink(context.Background(), linkID)
	require.NoError(n.t, err)
	return r
}

func requireDenied(t *testing.T, n *introNode, remote string) {
	t.Helper()
	_, err := n.mgr.ActiveAgreement(remote)
	require.Error(t, err)
	for _, a := range n.mgr.ActiveAgreements() {
		require.NotEqual(t, remote, a.RemoteChainID, "a denied agreement must not be listed")
	}
}

func TestIntroActivate_HostBindsActive(t *testing.T) {
	p := newIntroPair(t)
	h := p.host
	act := p.activation(t, "host")
	res, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkActive, res.State)
	require.Equal(t, bytes.Repeat([]byte{0}, len(act.Seed)), act.Seed, "the in-memory seed is zeroized")

	rec, err := h.mgr.ActiveAgreement(p.guest.chain)
	require.NoError(t, err)
	require.Equal(t, p.guest.endpoint(), rec.Endpoint)
	require.True(t, h.liveCA(p.guest.chain))
	require.True(t, h.mgr.seedEstablished(p.guest.chain))
	_, statErr := os.Stat(h.stageDir(p.guest.chain, res.LinkID))
	require.True(t, os.IsNotExist(statErr), "staging dir cleared after bind")

	row := h.row(res.LinkID)
	require.Equal(t, store.IntroducedLinkActive, row.State)
	require.Equal(t, hex.EncodeToString(sha256Of(h.lastSet)), row.TxHash, "the row holds the exact broadcast bytes")
	sets, revokes := h.counts()
	require.Equal(t, 1, sets)
	require.Zero(t, revokes)
	ctl, err := h.sqlite.GetSyncControl(context.Background(), p.guest.chain)
	require.NoError(t, err)
	require.Equal(t, "active", ctl.BindingState)
	require.Equal(t, p.guest.agentHex(), ctl.PeerAgentID)
}

func sha256Of(b []byte) []byte {
	s := sha256.Sum256(b)
	return s[:]
}

// The guest is never live until the host confirms (design §3.3, 7.3b).
func TestIntroActivate_GuestWaitsForHost(t *testing.T) {
	p := newIntroPair(t)
	g := p.guest
	res, err := g.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "guest"))
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkBoundPendingPeer, res.State)
	_, err = g.mgr.ActiveAgreement(p.host.chain)
	require.ErrorIs(t, err, ErrIntroducedLinkNotActive)
	requireDenied(t, g, p.host.chain)

	require.NoError(t, g.mgr.GuestIntroConfirmed(context.Background(), res.LinkID))
	_, err = g.mgr.ActiveAgreement(p.host.chain)
	require.NoError(t, err)
	require.Error(t, g.mgr.GuestIntroConfirmed(context.Background(), res.LinkID), "only from bound_pending_peer")
}

// §3.5 case 5: the host refuses, or the deadline passes → tx-34 →
// revoked(peer_not_confirmed); denied throughout.
func TestIntroActivate_GuestWindowRevokes(t *testing.T) {
	t.Run("host refuses", func(t *testing.T) {
		p := newIntroPair(t)
		g := p.guest
		res, err := g.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "guest"))
		require.NoError(t, err)
		g.mgr.introPeerStatusFn = func(context.Context, *store.IntroducedLink) (string, error) {
			return store.IntroducedLinkAborted, nil
		}
		require.NoError(t, g.mgr.ReconcileIntroducedLinks(context.Background()))
		row := g.row(res.LinkID)
		require.Equal(t, store.IntroducedLinkRevoked, row.State)
		require.Contains(t, row.StateReason, "peer_not_confirmed")
		_, revokes := g.counts()
		require.Equal(t, 1, revokes)
		requireDenied(t, g, p.host.chain)
	})
	t.Run("deadline passes", func(t *testing.T) {
		p := newIntroPair(t)
		g := p.guest
		act := p.activation(t, "guest")
		// Enough margin for slow (race-instrumented) runs; the wait below
		// follows the real deadline rather than a fixed sleep.
		act.Deadline = time.Now().Unix() + 4
		res, err := g.mgr.ActivateIntroducedLink(context.Background(), act)
		require.NoError(t, err)
		require.NoError(t, g.mgr.ReconcileIntroducedLinks(context.Background()))
		if time.Now().Unix() < act.Deadline {
			require.Equal(t, store.IntroducedLinkBoundPendingPeer, g.row(res.LinkID).State, "still waiting before the deadline")
		}
		requireDenied(t, g, p.host.chain)
		time.Sleep(time.Until(time.Unix(act.Deadline, 0)) + 100*time.Millisecond)
		require.NoError(t, g.mgr.ReconcileIntroducedLinks(context.Background()))
		require.Equal(t, store.IntroducedLinkRevoked, g.row(res.LinkID).State)
		require.Error(t, g.mgr.GuestIntroConfirmed(context.Background(), res.LinkID))
		requireDenied(t, g, p.host.chain)
	})
	t.Run("host confirms via status", func(t *testing.T) {
		p := newIntroPair(t)
		g := p.guest
		res, err := g.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "guest"))
		require.NoError(t, err)
		g.mgr.introPeerStatusFn = func(context.Context, *store.IntroducedLink) (string, error) {
			return store.IntroducedLinkActive, nil
		}
		require.NoError(t, g.mgr.ReconcileIntroducedLinks(context.Background()))
		require.Equal(t, store.IntroducedLinkActive, g.row(res.LinkID).State)
		_, err = g.mgr.ActiveAgreement(p.host.chain)
		require.NoError(t, err)
	})
}

// §3.5 case 1: crash before broadcast.
func TestIntroCrash_BeforeBroadcast(t *testing.T) {
	t.Run("after intent", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		act := p.activation(t, "host")
		link := linkIDOf(act)
		h.mgr.introFaultFn = func(pt string) bool { return pt == "after_intent" }
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		require.ErrorIs(t, err, errIntroSimulatedCrash)
		_, statErr := os.Stat(h.stageDir(p.guest.chain, link))
		require.NoError(t, statErr, "staged before the crash")

		h.restart()
		require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
		row := h.row(link)
		require.Equal(t, store.IntroducedLinkAborted, row.State)
		require.Equal(t, "crash_before_broadcast", row.StateReason)
		sets, _ := h.counts()
		require.Zero(t, sets, "no tx-33")
		require.False(t, h.liveCA(p.guest.chain), "no CA on disk")
		require.False(t, h.mgr.seedEstablished(p.guest.chain), "no seed on disk")
		_, statErr = os.Stat(h.stageDir(p.guest.chain, link))
		require.True(t, os.IsNotExist(statErr))
		ctl, err := h.sqlite.GetSyncControl(context.Background(), p.guest.chain)
		require.NoError(t, err)
		require.Nil(t, ctl, "pending sync control dropped")
		requireDenied(t, h, p.guest.chain)
	})
	t.Run("after signed, before broadcast", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		act := p.activation(t, "host")
		link := linkIDOf(act)
		h.mgr.introFaultFn = func(pt string) bool { return pt == "after_signed" }
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		require.ErrorIs(t, err, errIntroSimulatedCrash)
		require.Equal(t, store.IntroducedLinkSigned, h.row(link).State)
		sets, _ := h.counts()
		require.Zero(t, sets, "the bytes never left the process")

		h.restart()
		var resubmitted []byte
		h.resolver = func(_ context.Context, b []byte) (tx.TxOutcome, error) {
			resubmitted = append([]byte(nil), b...)
			return tx.TxOutcome{Verdict: tx.TxVerdictRejected}, nil
		}
		require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
		row := h.row(link)
		require.Equal(t, store.IntroducedLinkAborted, row.State)
		require.Equal(t, "rejected", row.StateReason)
		require.Equal(t, row.TxBytes, resubmitted, "only the stored bytes are re-submitted")
		require.False(t, h.liveCA(p.guest.chain))
		requireDenied(t, h, p.guest.chain)
	})
}

// §3.5 case 2: ambiguous commit. Exactly one tx-33 exists in every case.
func TestIntroCrash_AmbiguousCommit(t *testing.T) {
	cases := []struct {
		name      string
		landed    bool
		verdict   tx.TxVerdict
		wantState string
		reason    string
		live      bool
	}{
		{"committed", true, tx.TxVerdictCommitted, store.IntroducedLinkActive, "", true},
		{"rejected", false, tx.TxVerdictRejected, store.IntroducedLinkAborted, "rejected", false},
		{"spent with matching terms", true, tx.TxVerdictSpent, store.IntroducedLinkPaused, "reconcile_spent", false},
		{"spent without terms", false, tx.TxVerdictSpent, store.IntroducedLinkAborted, "spent", false},
		{"superseded", false, tx.TxVerdictSuperseded, store.IntroducedLinkAborted, "superseded", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := newIntroPair(t)
			h := p.host
			h.setMode = "indeterminate"
			if tc.landed {
				h.setMode = "indeterminate-landed"
			}
			act := p.activation(t, "host")
			res, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
			require.ErrorIs(t, err, ErrIntroPending)
			require.Equal(t, store.IntroducedLinkSigned, res.State)
			requireDenied(t, h, p.guest.chain)

			h.restart()
			h.resolver = func(context.Context, []byte) (tx.TxOutcome, error) {
				return tx.TxOutcome{Verdict: tc.verdict}, nil
			}
			require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
			row := h.row(res.LinkID)
			require.Equal(t, tc.wantState, row.State)
			require.Equal(t, tc.reason, row.StateReason)
			sets, _ := h.counts()
			require.Equal(t, 1, sets, "exactly one tx-33")
			require.Equal(t, hex.EncodeToString(sha256Of(h.lastSet)), row.TxHash)
			if tc.live {
				_, err := h.mgr.ActiveAgreement(p.guest.chain)
				require.NoError(t, err)
				require.True(t, h.liveCA(p.guest.chain))
			} else {
				requireDenied(t, h, p.guest.chain)
			}
		})
	}
	t.Run("unresolved stays signed", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		h.setMode = "indeterminate"
		res, err := h.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
		require.ErrorIs(t, err, ErrIntroPending)
		h.restart()
		for i := 0; i < 3; i++ {
			require.Error(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
			require.Equal(t, store.IntroducedLinkSigned, h.row(res.LinkID).State)
			requireDenied(t, h, p.guest.chain)
		}
		sets, _ := h.counts()
		require.Equal(t, 1, sets)
	})
}

// §3.5 case 3: crash after broadcast, before bind.
func TestIntroCrash_AfterBroadcastBeforeBind(t *testing.T) {
	t.Run("bind resumes", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		act := p.activation(t, "host")
		link := linkIDOf(act)
		h.mgr.introFaultFn = func(pt string) bool { return pt == "after_commit" }
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		require.ErrorIs(t, err, errIntroSimulatedCrash)
		require.Equal(t, store.IntroducedLinkCommitted, h.row(link).State)
		requireDenied(t, h, p.guest.chain)

		h.restart()
		require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
		require.Equal(t, store.IntroducedLinkActive, h.row(link).State)
		_, err = h.mgr.ActiveAgreement(p.guest.chain)
		require.NoError(t, err)
		require.True(t, h.liveCA(p.guest.chain))
		require.True(t, h.mgr.seedEstablished(p.guest.chain))
		sets, revokes := h.counts()
		require.Equal(t, 1, sets)
		require.Zero(t, revokes)
	})
	t.Run("staged seed missing", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		act := p.activation(t, "host")
		link := linkIDOf(act)
		h.mgr.introFaultFn = func(pt string) bool { return pt == "after_commit" }
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		require.ErrorIs(t, err, errIntroSimulatedCrash)
		require.NoError(t, os.Remove(filepath.Join(h.stageDir(p.guest.chain, link), "seed.json")))

		h.restart()
		require.Error(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
		row := h.row(link)
		require.Equal(t, store.IntroducedLinkRevoked, row.State)
		require.Contains(t, row.StateReason, "bind_failed")
		_, revokes := h.counts()
		require.Equal(t, 1, revokes, "tx-34")
		requireDenied(t, h, p.guest.chain)
	})
}

// §3.5 case 4: stale-intent rejection, with zero broadcasts.
func TestIntroStale_Rejections(t *testing.T) {
	t.Run("org evidence moved", func(t *testing.T) {
		// Offer expiry, roster/policy revision, card removal, allow-list and
		// envelope narrowing all reach the write path through StaleCheck.
		p := newIntroPair(t)
		h := p.host
		act := p.activation(t, "host")
		act.StaleCheck = func(context.Context) error { return errors.New("roster revision moved") }
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		var stale *IntroStaleError
		require.ErrorAs(t, err, &stale)
		require.Equal(t, "roster revision moved", stale.Which)
		sets, _ := h.counts()
		require.Zero(t, sets)
		rows, lErr := h.sqlite.ListIntroducedLinks(context.Background(), "")
		require.NoError(t, lErr)
		require.Empty(t, rows)
		_, statErr := os.Stat(filepath.Join(h.dir, "federation", p.guest.chain, "intro"))
		require.True(t, os.IsNotExist(statErr), "nothing staged")
	})
	t.Run("chain terms changed", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		act := p.activation(t, "host") // accepted against "absent"
		require.NoError(t, h.badger.SetCrossFed(p.guest.chain, "https://elsewhere:8443", []byte{1}, 1, 0, nil, nil, "active"))
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		var stale *IntroStaleError
		require.ErrorAs(t, err, &stale)
		require.Contains(t, stale.Which, "chain terms")
		sets, _ := h.counts()
		require.Zero(t, sets)
	})
	t.Run("second in-flight intent", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		h.mgr.introFaultFn = func(pt string) bool { return pt == "after_intent" }
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
		require.ErrorIs(t, err, errIntroSimulatedCrash)
		h.mgr.introFaultFn = nil
		p.nonce = 2 // a new session
		_, err = h.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
		var stale *IntroStaleError
		require.ErrorAs(t, err, &stale)
		require.Contains(t, stale.Which, "in flight")
		sets, _ := h.counts()
		require.Zero(t, sets)
	})
	t.Run("restored signed row superseded on chain", func(t *testing.T) {
		p := newIntroPair(t)
		h := p.host
		h.setMode = "indeterminate-landed"
		res, err := h.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
		require.ErrorIs(t, err, ErrIntroPending)
		h.restart()
		// A later upsert replaced the terms on chain.
		require.NoError(t, h.badger.SetCrossFed(p.guest.chain, "https://elsewhere:8443", []byte{1}, 1, 0, nil, nil, "active"))
		h.resolver = func(context.Context, []byte) (tx.TxOutcome, error) {
			return tx.TxOutcome{Verdict: tx.TxVerdictCommitted}, nil
		}
		require.NoError(t, h.mgr.ReconcileIntroducedLinks(context.Background()))
		row := h.row(res.LinkID)
		require.Equal(t, store.IntroducedLinkAborted, row.State)
		require.Equal(t, "superseded_on_chain", row.StateReason)
		_, revokes := h.counts()
		require.Zero(t, revokes, "the chain is left alone")
		require.False(t, h.liveCA(p.guest.chain))
	})
}

func TestIntroActivate_RejectedBroadcastUnwinds(t *testing.T) {
	p := newIntroPair(t)
	h := p.host
	h.setMode = "reject"
	act := p.activation(t, "host")
	_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
	require.Error(t, err)
	row := h.row(linkIDOf(act))
	require.Equal(t, store.IntroducedLinkAborted, row.State)
	require.Contains(t, row.StateReason, "rejected")
	require.False(t, h.liveCA(p.guest.chain))
	_, statErr := os.Stat(h.stageDir(p.guest.chain, row.LinkID))
	require.True(t, os.IsNotExist(statErr))
	ctl, cErr := h.sqlite.GetSyncControl(context.Background(), p.guest.chain)
	require.NoError(t, cErr)
	require.Nil(t, ctl)
	// A new session for the chain can start again.
	h.setMode = ""
	p.nonce = 3
	res, err := h.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkActive, res.State)
}

func TestIntroActivate_PlanRefusals(t *testing.T) {
	p := newIntroPair(t)
	h := p.host
	for name, mut := range map[string]func(*IntroActivation){
		"bad host signature": func(a *IntroActivation) { a.HostAcceptSig = bytes.Repeat([]byte{1}, 64) },
		"guest ack swapped":  func(a *IntroActivation) { a.GuestAckSig = a.GuestAcceptSig },
		"wrong role":         func(a *IntroActivation) { a.Role = "guest" },
		"bad role":           func(a *IntroActivation) { a.Role = "peer" },
		"domains in terms":   func(a *IntroActivation) { a.Terms.AllowedDomains = []string{"eng"} },
		"clearance mismatch": func(a *IntroActivation) { a.Terms.MaxClearance = 2 },
		"other endpoint":     func(a *IntroActivation) { a.Terms.Endpoint = "https://x:1" },
		"other pin":          func(a *IntroActivation) { a.Terms.PeerPubKey = bytes.Repeat([]byte{7}, 32) },
		"seed mismatch":      func(a *IntroActivation) { a.Seed = bytes.Repeat([]byte{5}, 32) },
		"no stale gate":      func(a *IntroActivation) { a.StaleCheck = nil },
		"deadline passed":    func(a *IntroActivation) { a.Deadline = time.Now().Unix() - 1 },
		"tampered transcript": func(a *IntroActivation) {
			a.Transcript = bytes.Replace(a.Transcript, []byte(`"policy_revision":3`), []byte(`"policy_revision":4`), 1)
		},
	} {
		act := p.activation(t, "host")
		mut(act)
		_, err := h.mgr.ActivateIntroducedLink(context.Background(), act)
		require.Error(t, err, name)
	}
	sets, _ := h.counts()
	require.Zero(t, sets)
	rows, err := h.sqlite.ListIntroducedLinks(context.Background(), "")
	require.NoError(t, err)
	require.Empty(t, rows)

	// Rekey is not activated in this phase.
	act := p.activation(t, "host")
	tr, err := DecodeIntroTranscript(act.Transcript)
	require.NoError(t, err)
	tr.Kind = "rekey"
	act.Transcript, err = tr.Encode()
	require.NoError(t, err)
	_, err = h.mgr.ActivateIntroducedLink(context.Background(), act)
	require.ErrorContains(t, err, "only kind new")

	// No introduced-link source: fail closed.
	h.mgr.SetIntroducedLinkSource(nil)
	_, err = h.mgr.ActivateIntroducedLink(context.Background(), p.activation(t, "host"))
	require.ErrorContains(t, err, "no introduced-link source")
}

// The staged reference is never used as a path.
func TestIntroStaging_RefMustMatchRow(t *testing.T) {
	p := newIntroPair(t)
	h := p.host
	link := strings.Repeat("ab", 32)
	ref, _, err := h.mgr.stageIntroArtifacts(p.guest.chain, link, p.guest.caPEM(), p.seed, bytes.Repeat([]byte{1}, 32), 1)
	require.NoError(t, err)
	require.Equal(t, "intro/"+p.guest.chain+"/"+link, ref)
	require.Error(t, h.mgr.rollbackIntroArtifacts(p.guest.chain, link, "../../etc"))
	require.Error(t, h.mgr.commitIntroArtifacts(p.guest.chain, link, "intro/other/"+link, nil, nil))
	require.Error(t, h.mgr.rollbackIntroArtifacts(p.guest.chain, "x", ref))
	require.NoError(t, h.mgr.rollbackIntroArtifacts(p.guest.chain, link, ref))
	_, statErr := os.Stat(h.stageDir(p.guest.chain, link))
	require.True(t, os.IsNotExist(statErr))
	// A CA for another chain is refused at staging.
	_, _, err = h.mgr.stageIntroArtifacts(p.guest.chain, link, h.caPEM(), p.seed, bytes.Repeat([]byte{1}, 32), 1)
	require.Error(t, err)
}
