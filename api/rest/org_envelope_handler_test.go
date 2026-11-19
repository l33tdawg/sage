package rest

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// introFed adds the Phase B federation surface to cardFed and captures the
// introduced-link hooks SetFederation installs.
type introFed struct {
	*cardFed
	agreements map[string]store.CrossFedRecord
	src        federation.IntroducedLinkSource
	sharingFn  federation.IntroducedSharingCap
}

func (f *introFed) LocalChainID() string { return f.chain }
func (f *introFed) OrgPeerAgreements() (map[string]store.CrossFedRecord, error) {
	return f.agreements, nil
}
func (f *introFed) SetIntroducedLinkSource(s federation.IntroducedLinkSource) {
	f.src = s
}
func (f *introFed) SetIntroducedSharingCap(c federation.IntroducedSharingCap) { f.sharingFn = c }

func introNode(t *testing.T) (*policyNode, *introFed) {
	t.Helper()
	n := newPolicyNode(t)
	f := &introFed{cardFed: newCardFed(t), agreements: map[string]store.CrossFedRecord{}}
	n.s.SetFederation(f)
	n.s.nodeOperatorID = operatorID
	return n, f
}

func (n *policyNode) introRouter(caller string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), caller)))
		})
	})
	r.Post("/v1/org/{org_id}/envelope", n.s.handleOrgEnvelopeApply)
	r.Get("/v1/org/{org_id}/envelope", n.s.handleOrgEnvelopeStatus)
	r.Get("/v1/org/{org_id}/offers", n.s.handleOrgOffers)
	r.Post("/v1/org/{org_id}/offers/{offer_id}/reoffer", n.s.handleOrgReoffer)
	r.Get("/v1/federation/introduced-links", n.s.handleIntroducedLinkList)
	return r
}

func (n *policyNode) orgRootKeyID(t *testing.T) string {
	t.Helper()
	st, err := n.s.orgPolicyStore(context.Background()).LoadAccepted(policyOrgID)
	require.NoError(t, err)
	require.NotEmpty(t, st.RootKeyID)
	return st.RootKeyID
}

func signedEnvelope(t *testing.T, n *policyNode, root ed25519.PrivateKey, rev uint64, prev string, domains ...string) *orgpolicy.LinkEnvelope {
	t.Helper()
	now := time.Now().Unix()
	e := &orgpolicy.LinkEnvelope{
		OrgID: policyOrgID, OrgRootKeyID: n.orgRootKeyID(t), Scope: "fleet",
		AllowedDomains: domains, MaxClearance: 2, Revision: rev, PrevDigest: prev,
		IssuedAt: now - 10, ExpiresAt: now + 30*24*3600,
	}
	require.NoError(t, e.Sign(root))
	return e
}

func (n *policyNode) postEnvelope(t *testing.T, caller string, e *orgpolicy.LinkEnvelope) (*httptest.ResponseRecorder, OrgEnvelopeResponse) {
	t.Helper()
	b, err := json.Marshal(OrgEnvelopeRequest{Envelope: e})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	n.introRouter(caller).ServeHTTP(rr, localRootRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/envelope", b))
	var resp OrgEnvelopeResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	return rr, resp
}

func (n *policyNode) envelopeStatus(t *testing.T) OrgEnvelopeStatusResponse {
	t.Helper()
	rr := httptest.NewRecorder()
	n.introRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/envelope", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp OrgEnvelopeStatusResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	return resp
}

func TestOrgEnvelope_StoreStatusOrderingAndRoot(t *testing.T) {
	n, f := introNode(t)
	root := n.currentRootID(t)

	// Not enrolled in another org; route/body org mismatch.
	e1 := signedEnvelope(t, n, f.root, 1, "", "eng")
	rr := httptest.NewRecorder()
	b, _ := json.Marshal(OrgEnvelopeRequest{Envelope: e1})
	n.introRouter(root).ServeHTTP(rr, localRootRequest(http.MethodPost, "/v1/org/org-other/envelope", b))
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())

	rr, resp := n.postEnvelope(t, root, e1)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionApplied, resp.Decision)
	require.NotEmpty(t, n.events)
	st := n.envelopeStatus(t)
	assert.Equal(t, "not_covering", st.Status, "no accepted roster yet")
	assert.Contains(t, st.Reason, "roster scope")
	now := time.Now().Unix()
	own, err := f.MintOrgLinkCard(federation.OrgLinkCardRequest{
		OrgID: policyOrgID, CardRev: 1, IssuedAt: now - 60, ExpiresAt: now + 3600, Endpoint: "https://10.0.0.1:8443",
	})
	require.NoError(t, err)
	rr, _ = n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, *own))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	st = n.envelopeStatus(t)
	assert.Equal(t, "covering", st.Status, st.Reason)
	assert.Equal(t, uint64(1), st.Envelope.Envelope.Revision)

	rr, resp = n.postEnvelope(t, root, e1)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, orgpolicy.DecisionNoOpSameDigest, resp.Decision)

	// Same revision, different content: 409, notice, governance event.
	n.events = nil
	rr, resp = n.postEnvelope(t, root, signedEnvelope(t, n, f.root, 1, "", "*"))
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionRejectedEquivocation, resp.Decision)
	require.NotNil(t, resp.Notice)
	require.Len(t, n.events, 1)
	assert.Equal(t, "fleet_envelope_equivocation", n.events[0].data.(map[string]any)["action"])
	assert.NotNil(t, n.envelopeStatus(t).Notice)

	e2 := signedEnvelope(t, n, f.root, 2, e1.Digest(), "eng", "ops")
	rr, _ = n.postEnvelope(t, root, e2)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	rr, _ = n.postEnvelope(t, root, e1)
	assert.Equal(t, http.StatusConflict, rr.Code, "rollback")

	// Not signed by the current Root: 400.
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	rr, _ = n.postEnvelope(t, root, signedEnvelope(t, n, other, 3, e2.Digest(), "eng"))
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())

	// Only the current local Root may store; listing is for the operator.
	rr, _ = n.postEnvelope(t, operatorID, signedEnvelope(t, n, f.root, 3, e2.Digest(), "eng"))
	assert.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	req := localRootRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/envelope", b)
	req.RemoteAddr, req.Host = "192.0.2.10:54321", "sage.example"
	rr = httptest.NewRecorder()
	n.introRouter(root).ServeHTTP(rr, req)
	assert.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	rr = httptest.NewRecorder()
	n.introRouter(strings.Repeat("d", 64)).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/envelope", nil))
	assert.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())

	// Request cap before decode.
	big := []byte(`{"envelope":{"scope":"` + strings.Repeat("x", orgEnvelopeRequestCap) + `"}}`)
	rr = httptest.NewRecorder()
	n.introRouter(root).ServeHTTP(rr, localRootRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/envelope", big))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code, rr.Body.String())

	// Root rotates: the stored envelope stops covering.
	_, f.root, _ = ed25519.GenerateKey(rand.Reader)
	st = n.envelopeStatus(t)
	assert.Equal(t, "not_covering", st.Status)
	assert.Contains(t, st.Reason, "current Root")
}

func (n *policyNode) deliverV2(t *testing.T, rev uint64, allow ...string) {
	t.Helper()
	p := signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", rev)
	p.Schema = orgpolicy.SchemaIDV2
	p.Payload.PeerAllowList = allow
	require.NoError(t, p.Sign(n.orgPriv))
	rr, _ := n.deliver(t, policyOrgID, p, nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

func peerCard(t *testing.T, chain string) (orgpolicy.LinkCard, string) {
	t.Helper()
	_, k, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	now := time.Now().Unix()
	c := orgpolicy.LinkCard{
		OrgID: policyOrgID, ChainID: chain, CASPKIPin: strings.Repeat("12", 32),
		Endpoint: "https://10.0.0.9:8443", CardRev: 1, IssuedAt: now - 60, ExpiresAt: now + 7*24*3600,
	}
	require.NoError(t, c.Sign(k))
	return c, hex.EncodeToString(k.Public().(ed25519.PublicKey))
}

func (n *policyNode) offers(t *testing.T) orgpolicy.OfferList {
	t.Helper()
	rr := httptest.NewRecorder()
	n.introRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/offers", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var l orgpolicy.OfferList
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &l))
	return l
}

func TestOrgOffers_EndToEnd(t *testing.T) {
	n, f := introNode(t)
	now := time.Now().Unix()
	own, err := f.MintOrgLinkCard(federation.OrgLinkCardRequest{
		OrgID: policyOrgID, CardRev: 1, IssuedAt: now - 60, ExpiresAt: now + 7*24*3600, Endpoint: "https://10.0.0.1:8443",
	})
	require.NoError(t, err)
	peer, peerPub := peerCard(t, "node-peer")
	stranger, _ := peerCard(t, "node-stranger")
	n.deliverV2(t, 2, f.TransportAgentPub(), peerPub)
	rr, _ := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, *own, peer, stranger))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// No envelope yet: not ready, with the reason.
	l := n.offers(t)
	assert.False(t, l.Ready)
	assert.Contains(t, l.Reason, "envelope")

	rr, _ = n.postEnvelope(t, n.currentRootID(t), signedEnvelope(t, n, f.root, 1, "", "eng"))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	l = n.offers(t)
	require.True(t, l.Ready, l.Reason)
	require.Len(t, l.Offers, 2)
	byChain := map[string]orgpolicy.PeerOffer{}
	for _, o := range l.Offers {
		byChain[o.PeerChainID] = o
	}
	po := byChain["node-peer"]
	assert.Equal(t, orgpolicy.PeerStateOffered, po.State)
	host, guest, err := orgpolicy.IntroRoles(f.TransportAgentPub(), peerPub)
	require.NoError(t, err)
	assert.Equal(t, orgpolicy.OfferID(policyOrgID, host, guest, 1, 1, orgpolicy.OfferKindNew), po.OfferID)
	assert.Equal(t, orgpolicy.PeerStateNone, byChain["node-stranger"].State, "not in the allow-list")

	// A manual agreement to the peer suppresses the offer.
	f.agreements["node-peer"] = peerAgreementRecord("node-peer", peer.CASPKIPin)
	assert.Equal(t, orgpolicy.PeerStateManual, func() string {
		for _, o := range n.offers(t).Offers {
			if o.PeerChainID == "node-peer" {
				return o.State
			}
		}
		return ""
	}())
	delete(f.agreements, "node-peer")

	// Re-offer: known id 200, unknown 404, malformed 400, non-operator 403.
	post := func(caller, id string) int {
		rr := httptest.NewRecorder()
		n.introRouter(caller).ServeHTTP(rr, httptest.NewRequest(http.MethodPost,
			"/v1/org/"+policyOrgID+"/offers/"+id+"/reoffer", nil))
		return rr.Code
	}
	assert.Equal(t, http.StatusOK, post(operatorID, po.OfferID))
	assert.Equal(t, http.StatusNotFound, post(operatorID, strings.Repeat("f", 64)))
	assert.Equal(t, http.StatusBadRequest, post(operatorID, "nope"))
	assert.Equal(t, http.StatusForbidden, post(strings.Repeat("d", 64), po.OfferID))
	rr = httptest.NewRecorder()
	n.introRouter(strings.Repeat("d", 64)).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/offers", nil))
	assert.Equal(t, http.StatusForbidden, rr.Code)
}

func introRow(chain string) *store.IntroducedLink {
	return introRowFor(peerAgreementRecord(chain, strings.Repeat("12", 32)))
}

// encodedTerms is the exact tx-33 payload for rec.
func encodedTerms(rec store.CrossFedRecord) []byte {
	return tx.EncodeCrossFedTerms(&tx.CrossFedTerms{
		RemoteChainID: rec.RemoteChainID, Endpoint: rec.Endpoint, PeerPubKey: rec.PeerPubKey,
		MaxClearance: tx.ClearanceLevel(rec.MaxClearance), AllowedDomains: rec.AllowedDomains,
		AllowedDepts: rec.AllowedDepts, ExpiresAt: rec.ExpiresAt, Status: rec.Status,
	})
}

// introRowFor is an intent row committing exactly rec.
func introRowFor(rec store.CrossFedRecord) *store.IntroducedLink {
	h := func(b byte) string { return strings.Repeat(fmt.Sprintf("%02x", b), 32) }
	chain := rec.RemoteChainID
	return &store.IntroducedLink{
		LinkID: h(1), OrgID: policyOrgID, RemoteChainID: chain, PeerAgentPub: h(0xaa),
		OfferID: h(0xbb), SessionID: "S1", Kind: store.IntroducedLinkKindNew, Role: "host",
		EIntro: []byte("secret transcript"), HostAcceptSig: []byte("ha"), GuestAccept: []byte("ga"), GuestAckSig: []byte("gk"),
		Evidence: []byte(`{}`), Terms: encodedTerms(rec), PriorTermsDigest: "absent",
		Generation: store.CrossFedGeneration(rec),
		StagedRef:  "staged/1", CreatedAt: 1000, Deadline: 2000,
	}
}

func peerAgreementRecord(chain, pinHex string) store.CrossFedRecord {
	pin, _ := hex.DecodeString(pinHex)
	return store.CrossFedRecord{
		RemoteChainID: chain, Endpoint: "https://10.0.0.9:8443", PeerPubKey: pin,
		MaxClearance: 2, AllowedDomains: []string{"eng"}, Status: "active",
	}
}

// activateIntroRow walks a row from intent to active.
func (n *policyNode) activateIntroRow(t *testing.T, l *store.IntroducedLink) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, n.sq.InsertIntroducedLinkIntent(ctx, l))
	ct, err := tx.DecodeCrossFedTerms(l.Terms)
	require.NoError(t, err)
	_, key, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	p := &tx.ParsedTx{Type: tx.TxTypeCrossFedSet, Nonce: 1, Timestamp: time.Unix(1100, 0), CrossFedTerms: ct}
	require.NoError(t, tx.SignTx(p, key))
	txb, err := tx.EncodeTx(p)
	require.NoError(t, err)
	ok, err := n.sq.TransitionIntroducedLink(ctx, store.IntroducedLinkTransition{LinkID: l.LinkID, From: store.IntroducedLinkIntent, To: store.IntroducedLinkSigned, At: 1100, TxBytes: txb, TxNonce: 1})
	require.NoError(t, err)
	require.True(t, ok)
	got, err := n.sq.GetIntroducedLink(ctx, l.LinkID)
	require.NoError(t, err)
	for _, step := range [][2]string{{store.IntroducedLinkSigned, store.IntroducedLinkCommitted}, {store.IntroducedLinkCommitted, store.IntroducedLinkActive}} {
		ok, err = n.sq.TransitionIntroducedLink(ctx, store.IntroducedLinkTransition{LinkID: l.LinkID, From: step[0], To: step[1], At: 1200, ExpectTxHash: got.TxHash})
		require.NoError(t, err)
		require.True(t, ok)
	}
}

func TestIntroducedLinks_WiredBySetFederation(t *testing.T) {
	n, f := introNode(t)
	require.NotNil(t, f.src, "SetFederation installs the introduced-link source")
	require.NotNil(t, f.sharingFn, "SetFederation installs the envelope sharing cap")
	ctx := context.Background()

	row := introRow("node-peer")
	got, err := f.src.IntroducedLinkForGeneration(ctx, "node-peer", row.Generation)
	require.NoError(t, err)
	require.Nil(t, got)
	require.NoError(t, n.sq.InsertIntroducedLinkIntent(ctx, row))
	got, err = f.src.IntroducedLinkForGeneration(ctx, "node-peer", row.Generation)
	require.NoError(t, err)
	require.Equal(t, store.IntroducedLinkIntent, got.State)
	states, err := f.src.IntroducedLinkGenerationStates(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]map[string]string{"node-peer": {row.Generation: store.IntroducedLinkIntent}}, states)

	// The sharing cap reads the org's envelope under the current Root.
	require.Error(t, f.sharingFn(policyOrgID, "node-peer", []string{"eng"}, 1), "no envelope yet")
	rr, _ := n.postEnvelope(t, n.currentRootID(t), signedEnvelope(t, n, f.root, 1, "", "eng"))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	// The roster scope must match the envelope; deliver one.
	own, err := f.MintOrgLinkCard(federation.OrgLinkCardRequest{
		OrgID: policyOrgID, CardRev: 1, IssuedAt: time.Now().Unix() - 60, ExpiresAt: time.Now().Unix() + 3600, Endpoint: "https://10.0.0.1:8443",
	})
	require.NoError(t, err)
	rr, _ = n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, *own))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.NoError(t, f.sharingFn(policyOrgID, "node-peer", []string{"eng.public"}, 2))
	require.ErrorIs(t, f.sharingFn(policyOrgID, "node-peer", []string{"hr"}, 1), federation.ErrSharingOutsideEnvelope)
	require.ErrorIs(t, f.sharingFn(policyOrgID, "node-peer", []string{"eng"}, 3), federation.ErrSharingOutsideEnvelope)
	_, f.root, _ = ed25519.GenerateKey(rand.Reader)
	require.Error(t, f.sharingFn(policyOrgID, "node-peer", []string{"eng"}, 1), "Root rotation retires the envelope")

	// Diagnostics list the row and its history without the transcript.
	rr = httptest.NewRecorder()
	n.introRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/federation/introduced-links?remote_chain_id=node-peer", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.NotContains(t, rr.Body.String(), "c2VjcmV0", "e_intro must not be echoed")
	var list IntroducedLinkListResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &list))
	require.Len(t, list.Links, 1)
	require.Len(t, list.Links[0].History, 1)
	rr = httptest.NewRecorder()
	n.introRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/federation/introduced-links?remote_chain_id=Bad%20Chain", nil))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
}

// A node with no node-local store has no introduced links: the hook is a no-op.
func TestIntroducedLinks_NoStoreIsUnchanged(t *testing.T) {
	s := &Server{}
	f := &introFed{cardFed: newCardFed(t)}
	s.SetFederation(f)
	got, err := f.src.IntroducedLinkForGeneration(context.Background(), "any", strings.Repeat("0", 64))
	require.NoError(t, err)
	require.Nil(t, got)
	states, err := f.src.IntroducedLinkGenerationStates(context.Background())
	require.NoError(t, err)
	require.Empty(t, states)
}

// The offers path reads only the row bound to the current agreement
// generation (or an in-flight row), and carries the row's org.
func TestOrgOffers_AgreementResolvesGenerationAndOrg(t *testing.T) {
	n, _ := introNode(t)
	ctx := context.Background()
	ils := n.s.introducedLinks()
	require.NotNil(t, ils)
	pin := strings.Repeat("12", 32)
	agreements := map[string]store.CrossFedRecord{"node-peer": peerAgreementRecord("node-peer", pin)}

	ag, err := introducedPeerAgreement(ctx, ils, agreements, "node-peer")
	require.NoError(t, err)
	assert.True(t, ag.Exists)
	assert.Equal(t, pin, ag.PinHex)
	assert.Equal(t, uint8(2), ag.MaxClearance)
	assert.Equal(t, []string{"eng"}, ag.AllowedDomains)
	assert.Empty(t, ag.LinkState, "no row: manual")

	// An active row bound to this generation governs it, with its org.
	row := introRow("node-peer")
	row.OrgID = "org-other"
	n.activateIntroRow(t, row)
	ag, err = introducedPeerAgreement(ctx, ils, agreements, "node-peer")
	require.NoError(t, err)
	assert.Equal(t, store.IntroducedLinkActive, ag.LinkState)
	assert.Equal(t, "org-other", ag.LinkOrgID)

	// A later tx-33 with another generation is not governed by that row.
	later := peerAgreementRecord("node-peer", pin)
	later.ExpiresAt = time.Now().Unix() + 3600
	agreements["node-peer"] = later
	ag, err = introducedPeerAgreement(ctx, ils, agreements, "node-peer")
	require.NoError(t, err)
	assert.Empty(t, ag.LinkState, "the old row must not govern a later generation")

	// An in-flight row governs the chain whatever its current terms.
	inflight := introRowFor(later)
	inflight.LinkID = strings.Repeat("02", 32)
	require.NoError(t, n.sq.InsertIntroducedLinkIntent(ctx, inflight))
	delete(agreements, "node-peer")
	ag, err = introducedPeerAgreement(ctx, ils, agreements, "node-peer")
	require.NoError(t, err)
	assert.False(t, ag.Exists)
	assert.Equal(t, store.IntroducedLinkIntent, ag.LinkState)
	assert.Equal(t, policyOrgID, ag.LinkOrgID)
}
