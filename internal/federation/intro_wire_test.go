package federation

// RFC #443 Phase B2b: the intro wire end to end. Two real nodes (Badger +
// SQLite, restartable) each serve their real federation router over mTLS;
// the org side is a fake IntroOrgSource holding each node's view of one
// offer.

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	wireOrg   = "org-acme"
	wireOffer = "0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f0f"
)

// fakeIntroSource is one node's org view.
type fakeIntroSource struct {
	mu   sync.Mutex
	view *IntroOfferView
	pins map[string]bool
}

func (f *fakeIntroSource) IntroOffer(_ context.Context, orgID, offerID string) (*IntroOfferView, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.view == nil || f.view.OrgID != orgID || f.view.OfferID != offerID {
		return nil, ErrIntroOfferUnknown
	}
	v := *f.view
	return &v, nil
}

func (f *fakeIntroSource) IntroPeerPinKnown(pin string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.pins[pin]
}

func (f *fakeIntroSource) AcceptedIntroOffers(context.Context) ([]IntroOfferRef, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.view == nil || !f.view.Accepted {
		return nil, nil
	}
	return []IntroOfferRef{{OrgID: f.view.OrgID, OfferID: f.view.OfferID}}, nil
}

func (f *fakeIntroSource) edit(fn func(v *IntroOfferView)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	fn(f.view)
}

// wireNode is an introNode serving its router on a stable TLS listener that
// follows the node across restarts.
type wireNode struct {
	*introNode
	src *fakeIntroSource
	srv *httptest.Server

	faultMu sync.Mutex
	faults  map[string]bool
	// hits counts requests that reached the HTTP handler (past TLS).
	hits atomic.Int64
}

func (w *wireNode) setFault(point string, on bool) {
	w.faultMu.Lock()
	defer w.faultMu.Unlock()
	w.faults[point] = on
}

func (w *wireNode) fault(point string) bool {
	w.faultMu.Lock()
	defer w.faultMu.Unlock()
	return w.faults[point]
}

func newWireNode(t *testing.T, n *introNode) *wireNode {
	t.Helper()
	w := &wireNode{introNode: n, src: &fakeIntroSource{pins: map[string]bool{}}, faults: map[string]bool{}}
	install := func(m *Manager) {
		m.SetIntroOrgSource(w.src)
		m.introPoll = 20 * time.Millisecond
		m.introFaultFn = w.fault
	}
	n.onOpen = install
	install(n.mgr)
	var (
		mu     sync.Mutex
		router http.Handler
		owner  *Manager
	)
	w.srv = httptest.NewUnstartedServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		w.hits.Add(1)
		cur := n.currentMgr()
		mu.Lock()
		if owner != cur {
			owner, router = cur, cur.Router()
		}
		h := router
		mu.Unlock()
		h.ServeHTTP(rw, r)
	}))
	w.srv.TLS = &tls.Config{GetConfigForClient: func(*tls.ClientHelloInfo) (*tls.Config, error) {
		return n.currentMgr().ServerTLSConfig()
	}}
	w.srv.StartTLS()
	t.Cleanup(w.srv.Close)
	return w
}

func (w *wireNode) card() IntroCard {
	return IntroCard{
		AgentPub: w.agentHex(), ChainID: w.chain, Pin: w.pinHex(), Endpoint: w.srv.URL,
		CardRev: 1, CardDigest: hex.EncodeToString(sha256Of([]byte(w.chain))),
	}
}

type wirePair struct {
	host, guest *wireNode
}

func newWirePair(t *testing.T) *wirePair {
	t.Helper()
	p := newIntroPair(t)
	wp := &wirePair{host: newWireNode(t, p.host), guest: newWireNode(t, p.guest)}
	base := IntroOfferView{
		OrgID: wireOrg, OrgRootKeyID: strings.Repeat("ab", 32), OfferID: wireOffer, Kind: "new", State: "offered",
		ExpiresAt:      time.Now().Add(time.Hour).Unix(),
		PolicyRevision: 3, PolicyDigest: strings.Repeat("11", 32),
		RosterRevision: 2, RosterDigest: strings.Repeat("22", 32),
	}
	hv, gv := base, base
	hv.Role, hv.Own, hv.Peer = "host", wp.host.card(), wp.guest.card()
	hv.EnvelopeRev, hv.EnvelopeDigest = 1, strings.Repeat("e", 64)
	gv.Role, gv.Own, gv.Peer = "guest", wp.guest.card(), wp.host.card()
	gv.EnvelopeRev, gv.EnvelopeDigest = 4, strings.Repeat("f", 64)
	wp.host.src.view, wp.guest.src.view = &hv, &gv
	wp.host.src.pins[wp.guest.pinHex()] = true
	wp.guest.src.pins[wp.host.pinHex()] = true
	return wp
}

func accept(n *wireNode) {
	n.src.edit(func(v *IntroOfferView) {
		v.Accepted, v.AcceptedEnvelopeRev, v.AcceptedEnvelopeDigest = true, v.EnvelopeRev, v.EnvelopeDigest
	})
}

func (n *wireNode) latest(remote string) *store.IntroducedLink {
	r, err := n.sqlite.LatestIntroducedLinkByChain(context.Background(), remote)
	require.NoError(n.t, err)
	return r
}

func (n *wireNode) state(remote string) string {
	if r := n.latest(remote); r != nil {
		return r.State
	}
	return ""
}

func waitStates(t *testing.T, p *wirePair, host, guest string) {
	t.Helper()
	require.Eventually(t, func() bool {
		return p.host.state(p.guest.chain) == host && p.guest.state(p.host.chain) == guest
	}, 20*time.Second, 20*time.Millisecond, "host=%q guest=%q", p.host.state(p.guest.chain), p.guest.state(p.host.chain))
}

func requireActiveBoth(t *testing.T, p *wirePair) {
	t.Helper()
	_, err := p.host.mgr.ActiveAgreement(p.guest.chain)
	require.NoError(t, err)
	_, err = p.guest.mgr.ActiveAgreement(p.host.chain)
	require.NoError(t, err)
	hs, _ := p.host.counts()
	gs, _ := p.guest.counts()
	require.Equal(t, 1, hs, "exactly one host tx-33")
	require.Equal(t, 1, gs, "exactly one guest tx-33")
	require.True(t, p.host.liveCA(p.guest.chain))
	require.True(t, p.guest.liveCA(p.host.chain))
	// Both sides committed the same pair seed.
	hSeed, ok := p.host.mgr.loadSeedFromDisk(p.host.mgr.seedPath(p.guest.chain))
	require.True(t, ok)
	gSeed, ok := p.guest.mgr.loadSeedFromDisk(p.guest.mgr.seedPath(p.host.chain))
	require.True(t, ok)
	require.Equal(t, hSeed, gSeed)
}

func TestIntroWire_EndToEndActiveOnBoth(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
	waitStates(t, p, store.IntroducedLinkActive, store.IntroducedLinkActive)
	requireActiveBoth(t, p)

	hr, gr := p.host.latest(p.guest.chain), p.guest.latest(p.host.chain)
	require.Equal(t, hr.LinkID, gr.LinkID, "both rows are the same E_intro")
	require.Equal(t, "host", hr.Role)
	require.Equal(t, "guest", gr.Role)
	var ev introEvidence
	require.NoError(t, json.Unmarshal(hr.Evidence, &ev))
	require.Equal(t, uint64(1), ev.HostEnvelopeRev)
	require.Equal(t, uint64(4), ev.GuestEnvelopeRev)

	// The host session's seed is zeroized once its write path ran.
	sess := p.host.mgr.introHostSessionFor(wireOffer)
	require.NotNil(t, sess)
	require.Equal(t, make([]byte, 32), sess.seed)

	// A replayed confirm reports the row and changes nothing.
	cfg, err := p.guest.mgr.introClientTLS(mustCA(t, p.host), nil, nil)
	require.NoError(t, err)
	cr, err := p.guest.mgr.introConfirm(context.Background(), p.host.card(), cfg, wireOffer, gr.SessionID, gr.GuestAccept, gr.GuestAckSig)
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkActive, cr.State)
	hs, _ := p.host.counts()
	require.Equal(t, 1, hs)
}

func mustCA(t *testing.T, n *wireNode) *x509.Certificate {
	t.Helper()
	ca, err := parseCACertPEM(n.caPEM())
	require.NoError(t, err)
	return ca
}

func TestIntroWire_HostOwnerAcceptsLater(t *testing.T) {
	p := newWirePair(t)
	accept(p.guest)
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
	// The guest polls; nothing is written anywhere while the host owner
	// has not accepted.
	require.Eventually(t, func() bool {
		s := p.host.mgr.introHostSessionFor(wireOffer)
		return s != nil
	}, 10*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Nil(t, p.guest.latest(p.host.chain))
	require.Nil(t, p.host.latest(p.guest.chain))

	// The host owner accepts under its current envelope.
	accept(p.host)
	waitStates(t, p, store.IntroducedLinkActive, store.IntroducedLinkActive)
	requireActiveBoth(t, p)
}

func TestIntroWire_AcceptUnderOldEnvelopeDoesNotSign(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	// The host envelope moves on after its owner accepted: the acceptance
	// no longer names the current envelope, so the host never signs.
	p.host.src.edit(func(v *IntroOfferView) { v.EnvelopeRev, v.EnvelopeDigest = 2, strings.Repeat("d", 64) })
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
	require.Eventually(t, func() bool { return p.host.mgr.introHostSessionFor(wireOffer) != nil }, 10*time.Second, 20*time.Millisecond)
	time.Sleep(200 * time.Millisecond)
	require.Nil(t, p.guest.latest(p.host.chain))
	require.Nil(t, p.host.latest(p.guest.chain))
}

func TestIntroWire_GuestNeedsCurrentAcceptance(t *testing.T) {
	p := newWirePair(t)
	require.ErrorIs(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer), errIntroNotAccepted)
	accept(p.guest)
	p.guest.src.edit(func(v *IntroOfferView) { v.EnvelopeRev = 5 })
	require.ErrorIs(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer), errIntroNotAccepted)
	// The host cannot start a guest driver at all.
	accept(p.host)
	require.Error(t, p.host.mgr.StartIntroGuest(wireOrg, wireOffer))
}

// rawIntro sends one raw request from n's intro client identity to peer.
func rawIntro(t *testing.T, n, peer *wireNode, method, path string, body []byte) (int, []byte) {
	t.Helper()
	cfg, err := n.mgr.introClientTLS(mustCA(t, peer), nil, nil)
	require.NoError(t, err)
	req, err := http.NewRequest(method, peer.srv.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	if body == nil {
		req.Body = http.NoBody
	}
	client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, raw
}

func guestRequestBody(t *testing.T, p *wirePair, mutate func(*IntroRequestWire)) []byte {
	t.Helper()
	v := p.guest.src.view
	req := &IntroRequestWire{
		OrgID: wireOrg, OfferID: wireOffer, GuestNonce: strings.Repeat("5", 32),
		Evidence: IntroEvidenceWire{
			PolicyRevision: v.PolicyRevision, PolicyDigest: v.PolicyDigest,
			RosterRevision: v.RosterRevision, RosterDigest: v.RosterDigest,
			EnvelopeRev: v.EnvelopeRev, EnvelopeDigest: v.EnvelopeDigest,
		},
	}
	if mutate != nil {
		mutate(req)
	}
	b, err := json.Marshal(req)
	require.NoError(t, err)
	return b
}

func TestIntroWire_Refusals(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	post := func(body []byte) (int, []byte) {
		return rawIntro(t, p.guest, p.host, http.MethodPost, "/fed/v1/intro/request", body)
	}

	t.Run("policy and roster mismatch are retryable", func(t *testing.T) {
		st, raw := post(guestRequestBody(t, p, func(r *IntroRequestWire) { r.Evidence.PolicyDigest = strings.Repeat("99", 32) }))
		require.Equal(t, http.StatusConflict, st)
		require.Contains(t, string(raw), `"code":"policy_mismatch"`)
		require.Contains(t, string(raw), `"retryable":true`)
		st, raw = post(guestRequestBody(t, p, func(r *IntroRequestWire) { r.Evidence.RosterRevision = 9 }))
		require.Equal(t, http.StatusConflict, st)
		require.Contains(t, string(raw), `"code":"roster_mismatch"`)
	})
	t.Run("oversized bodies are refused before decode", func(t *testing.T) {
		big := []byte(`{"pad":"` + strings.Repeat("a", IntroRequestCap) + `"}`)
		st, _ := post(big)
		require.Equal(t, http.StatusRequestEntityTooLarge, st)
		st, _ = rawIntro(t, p.guest, p.host, http.MethodPost, "/fed/v1/intro/confirm", []byte(`{"pad":"`+strings.Repeat("a", IntroConfirmCap)+`"}`))
		require.Equal(t, http.StatusRequestEntityTooLarge, st)
		st, _ = rawIntro(t, p.guest, p.host, http.MethodGet, "/fed/v1/intro/"+wireOffer+"/status", []byte("x"))
		require.Equal(t, http.StatusRequestEntityTooLarge, st)
	})
	t.Run("unknown fields and trailing data", func(t *testing.T) {
		good := guestRequestBody(t, p, nil)
		st, _ := post(append(append([]byte(nil), good[:len(good)-1]...), []byte(`,"extra":1}`)...))
		require.Equal(t, http.StatusBadRequest, st)
		st, _ = post(append(append([]byte(nil), good...), []byte(` {}`)...))
		require.Equal(t, http.StatusBadRequest, st)
	})
	t.Run("expired or closed offer", func(t *testing.T) {
		p.host.src.edit(func(v *IntroOfferView) { v.ExpiresAt = time.Now().Add(-time.Second).Unix() })
		st, _ := post(guestRequestBody(t, p, nil))
		require.Equal(t, http.StatusConflict, st)
		p.host.src.edit(func(v *IntroOfferView) { v.ExpiresAt = time.Now().Add(time.Hour).Unix(); v.State = "none" })
		st, _ = post(guestRequestBody(t, p, nil))
		require.Equal(t, http.StatusConflict, st)
		p.host.src.edit(func(v *IntroOfferView) { v.State = "offered" })
	})
	t.Run("unknown offer", func(t *testing.T) {
		st, _ := post(guestRequestBody(t, p, func(r *IntroRequestWire) { r.OfferID = strings.Repeat("ee", 32) }))
		require.Equal(t, http.StatusNotFound, st)
	})
	t.Run("caller is not the offer's peer card", func(t *testing.T) {
		// The host's card for the peer pins another CA: the caller's CA is
		// still a roster pin (handshake passes), but not this offer's peer.
		p.host.src.edit(func(v *IntroOfferView) { v.Peer.Pin = strings.Repeat("77", 32) })
		st, _ := post(guestRequestBody(t, p, nil))
		require.Equal(t, http.StatusForbidden, st)
		p.host.src.edit(func(v *IntroOfferView) { v.Peer.Pin = p.guest.pinHex() })
	})
	t.Run("a stranger CA never completes the handshake", func(t *testing.T) {
		p.host.src.mu.Lock()
		delete(p.host.src.pins, p.guest.pinHex())
		p.host.src.mu.Unlock()
		cfg, err := p.guest.mgr.introClientTLS(mustCA(t, p.host), nil, nil)
		require.NoError(t, err)
		client := &http.Client{Transport: &http.Transport{TLSClientConfig: cfg}}
		defer client.CloseIdleConnections()
		resp, err := client.Post(p.host.srv.URL+"/fed/v1/intro/request", "application/json", bytes.NewReader(guestRequestBody(t, p, nil)))
		if resp != nil {
			_ = resp.Body.Close()
		}
		require.Error(t, err, "TLS handshake must fail for a CA that is no roster pin")
		p.host.src.mu.Lock()
		p.host.src.pins[p.guest.pinHex()] = true
		p.host.src.mu.Unlock()
	})
	t.Run("bad guest signatures", func(t *testing.T) {
		st, raw := post(guestRequestBody(t, p, nil))
		require.Equal(t, http.StatusOK, st, string(raw))
		var resp IntroRequestResp
		require.NoError(t, json.Unmarshal(raw, &resp))
		bad, _ := json.Marshal(&IntroConfirmWire{
			OfferID: wireOffer, SessionID: resp.SessionID,
			GuestAcceptSig: strings.Repeat("ab", 64), GuestAckSig: strings.Repeat("cd", 64),
		})
		st, _ = rawIntro(t, p.guest, p.host, http.MethodPost, "/fed/v1/intro/confirm", bad)
		require.Equal(t, http.StatusForbidden, st)
		require.Nil(t, p.host.latest(p.guest.chain), "nothing written for bad signatures")
		// The session is unchanged, seed intact, until a valid confirm.
		sess := p.host.mgr.introHostSessionFor(wireOffer)
		require.NotNil(t, sess)
		require.NotEqual(t, make([]byte, 32), sess.seed)
	})
	hs, _ := p.host.counts()
	require.Equal(t, 0, hs, "no refusal broadcast anything")
}

func TestIntroWire_GuestRejectsWrongHostPin(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	// The guest's roster card for the host pins another CA: the host's TLS
	// chain carries no CA with that pin, so the first request fails at the
	// handshake and never reaches the host's routes.
	p.guest.src.edit(func(v *IntroOfferView) { v.Peer.Pin = strings.Repeat("66", 32) })
	err := p.guest.mgr.introGuestAttempt(context.Background(), wireOrg, wireOffer)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no CA in the peer chain carries the card pin")
	require.Nil(t, p.host.mgr.introHostSessionFor(wireOffer), "the host never opened a session")
	require.Nil(t, p.guest.latest(p.host.chain))
	require.Nil(t, p.host.latest(p.guest.chain))
}

func TestIntroWire_FirstRequestFailsAtHandshakeOnAnotherCA(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	// Another node answers at the host card's endpoint. It presents its
	// own CA, not the card-pinned one, so the guest's first request fails
	// during the TLS handshake: no request body is ever sent to it.
	_, k, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	impostor := newWireNode(t, newIntroNode(t, p.host.chain, k))
	impostor.src.pins[p.guest.pinHex()] = true // it would even admit the guest
	p.guest.src.edit(func(v *IntroOfferView) { v.Peer.Endpoint = impostor.srv.URL })

	var seen []byte
	host := p.guest.src.view.Peer
	cfg, err := p.guest.mgr.introClientTLS(nil, &host, &seen)
	require.NoError(t, err)
	_, _, err = p.guest.mgr.introCall(context.Background(), host, cfg, http.MethodPost, "/fed/v1/intro/request",
		json.RawMessage(guestRequestBody(t, p, nil)), IntroRequestRespCap, 10*time.Second)
	require.Error(t, err)
	require.Contains(t, err.Error(), "no CA in the peer chain carries the card pin")
	require.Empty(t, seen)
	require.Zero(t, impostor.hits.Load(), "the impostor's handler never ran")

	// The full driver stops on it too, and nothing is written anywhere.
	err = p.guest.mgr.introGuestAttempt(context.Background(), wireOrg, wireOffer)
	require.Error(t, err)
	require.Nil(t, p.guest.latest(p.host.chain))
	require.Nil(t, p.host.mgr.introHostSessionFor(wireOffer))
	require.Zero(t, impostor.hits.Load())
}

func TestIntroCardCAFromChain(t *testing.T) {
	p := newWirePair(t)
	host := p.host
	leaf, err := host.mgr.loadNodeKeyPair()
	require.NoError(t, err)
	ca := mustCA(t, host)
	chain := [][]byte{leaf.Certificate[0], ca.Raw}
	got, err := introCardCAFromChain(chain, host.pinHex(), host.chain, x509.ExtKeyUsageServerAuth)
	require.NoError(t, err)
	require.Equal(t, ca.Raw, got.Raw)
	_, err = introCardCAFromChain(chain[:1], host.pinHex(), host.chain, x509.ExtKeyUsageServerAuth)
	require.Error(t, err, "a leaf-only chain carries no CA")
	_, err = introCardCAFromChain(chain, strings.Repeat("66", 32), host.chain, x509.ExtKeyUsageServerAuth)
	require.Error(t, err, "wrong pin")
	_, err = introCardCAFromChain(chain, host.pinHex(), "some-other-chain", x509.ExtKeyUsageServerAuth)
	require.Error(t, err, "the CA must name the card's chain")
	gLeaf, err := p.guest.mgr.loadNodeKeyPair()
	require.NoError(t, err)
	_, err = introCardCAFromChain([][]byte{gLeaf.Certificate[0], ca.Raw}, host.pinHex(), host.chain, x509.ExtKeyUsageServerAuth)
	require.Error(t, err, "a leaf from another CA does not verify")
}

func TestIntroWire_StaleSessionRefusedAtConfirm(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	// The host's roster moves on between request and confirm.
	go func() {
		for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
			if p.host.mgr.introHostSessionFor(wireOffer) != nil {
				p.host.src.edit(func(v *IntroOfferView) { v.RosterDigest = strings.Repeat("33", 32) })
				return
			}
		}
	}()
	// Run the guest by hand: the host refuses the stale session at accept
	// or confirm, and nothing is broadcast on the host.
	_ = p.guest.mgr.introGuestAttempt(context.Background(), wireOrg, wireOffer)
	hs, _ := p.host.counts()
	require.Equal(t, 0, hs)
	require.NotEqual(t, store.IntroducedLinkActive, p.host.state(p.guest.chain))
}

func TestIntroWire_GuestWindowConfirmViaStatus(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	p.host.setFault("drop_confirm", true)
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
	// The guest broadcast and bound; its confirm was lost.
	require.Eventually(t, func() bool { return p.guest.state(p.host.chain) == store.IntroducedLinkBoundPendingPeer }, 20*time.Second, 20*time.Millisecond)
	p.guest.mgr.waitIntroGuests()
	requireDenied(t, p.guest.introNode, p.host.chain)
	require.Nil(t, p.host.latest(p.guest.chain))

	// The window asks the host, which still waits for confirm: the probe
	// re-sends the stored signatures and the host activates.
	p.host.setFault("drop_confirm", false)
	require.NoError(t, p.guest.mgr.ReconcileIntroducedLinks(context.Background()))
	waitStates(t, p, store.IntroducedLinkActive, store.IntroducedLinkActive)
	requireActiveBoth(t, p)
}

func TestIntroWire_GuestWindowRevokesWhenHostLostSession(t *testing.T) {
	p := newWirePair(t)
	accept(p.host)
	accept(p.guest)
	p.host.setFault("drop_confirm", true)
	require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
	require.Eventually(t, func() bool { return p.guest.state(p.host.chain) == store.IntroducedLinkBoundPendingPeer }, 20*time.Second, 20*time.Millisecond)
	p.guest.mgr.waitIntroGuests()

	// The host restarts before it ever saw the confirm: its session (and
	// the seed) are gone, so it can never activate. The guest's window
	// learns that from status and revokes on chain.
	p.host.restart()
	require.NoError(t, p.guest.mgr.ReconcileIntroducedLinks(context.Background()))
	require.Equal(t, store.IntroducedLinkRevoked, p.guest.state(p.host.chain))
	_, revokes := p.guest.counts()
	require.Equal(t, 1, revokes)
	require.Nil(t, p.host.latest(p.guest.chain))
}

func TestIntroWire_RestartMidHandshake(t *testing.T) {
	t.Run("host restarts while the guest waits", func(t *testing.T) {
		p := newWirePair(t)
		accept(p.guest)
		require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
		require.Eventually(t, func() bool { return p.host.mgr.introHostSessionFor(wireOffer) != nil }, 10*time.Second, 20*time.Millisecond)
		p.host.restart()
		accept(p.host)
		// The guest's poll finds the session gone and starts a fresh one.
		waitStates(t, p, store.IntroducedLinkActive, store.IntroducedLinkActive)
		requireActiveBoth(t, p)
	})
	t.Run("guest restarts while waiting for the host owner", func(t *testing.T) {
		p := newWirePair(t)
		accept(p.guest)
		require.NoError(t, p.guest.mgr.StartIntroGuest(wireOrg, wireOffer))
		require.Eventually(t, func() bool { return p.host.mgr.introHostSessionFor(wireOffer) != nil }, 10*time.Second, 20*time.Millisecond)
		first := p.host.mgr.introHostSessionFor(wireOffer).sessionID
		p.guest.restart()
		// The maintenance tick resumes the accepted offer with a new session.
		p.guest.mgr.resumeIntroGuests(context.Background())
		require.Eventually(t, func() bool {
			s := p.host.mgr.introHostSessionFor(wireOffer)
			return s != nil && s.sessionID != first
		}, 10*time.Second, 20*time.Millisecond)
		accept(p.host)
		waitStates(t, p, store.IntroducedLinkActive, store.IntroducedLinkActive)
		requireActiveBoth(t, p)
		rows, err := p.host.sqlite.ListIntroducedLinks(context.Background(), p.guest.chain)
		require.NoError(t, err)
		require.Len(t, rows, 1, "one link, from the second session only")
	})
}

func TestIntroWire_StatusReportsSessionAndRow(t *testing.T) {
	p := newWirePair(t)
	status := func() IntroStatusResp {
		st, raw := rawIntro(t, p.guest, p.host, http.MethodGet, "/fed/v1/intro/"+wireOffer+"/status", nil)
		require.Equal(t, http.StatusOK, st, string(raw))
		var s IntroStatusResp
		require.NoError(t, json.Unmarshal(raw, &s))
		return s
	}
	require.Equal(t, IntroStatusNone, status().State)
	st, raw := rawIntro(t, p.guest, p.host, http.MethodPost, "/fed/v1/intro/request", guestRequestBody(t, p, nil))
	require.Equal(t, http.StatusOK, st, string(raw))
	require.Equal(t, IntroStatusAwaitingAccept, status().State)
	accept(p.host)
	require.Equal(t, IntroStatusAwaitingConfirm, status().State)
	var resp IntroRequestResp
	require.NoError(t, json.Unmarshal(raw, &resp))
	st, raw = rawIntro(t, p.guest, p.host, http.MethodGet, "/fed/v1/intro/"+wireOffer+"/accept?session_id="+resp.SessionID, nil)
	require.Equal(t, http.StatusOK, st)
	var a IntroHostAcceptWire
	require.NoError(t, json.Unmarshal(raw, &a))
	sig, err := hex.DecodeString(a.HostAcceptSig)
	require.NoError(t, err)
	tr, err := DecodeIntroTranscript(resp.Transcript)
	require.NoError(t, err)
	require.True(t, VerifyIntro(tr.Host.AgentPub, IntroHostAccept, IntroAttestation(resp.Transcript), sig))
	// A session id that is not the live one is unknown.
	st, _ = rawIntro(t, p.guest, p.host, http.MethodGet, "/fed/v1/intro/"+wireOffer+"/accept?session_id="+strings.Repeat("0", 32), nil)
	require.Equal(t, http.StatusNotFound, st)
}
