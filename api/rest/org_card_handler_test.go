package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
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
)

// cardFed is a FederationService stand-in that mints cards from fixed local
// runtime state, resolves a settable Root and captures the JOIN gate.
type cardFed struct {
	FederationService
	key   ed25519.PrivateKey
	root  ed25519.PrivateKey
	gate  federation.JoinPeerGate
	chain string
}

func newCardFed(t *testing.T) *cardFed {
	t.Helper()
	_, k, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	_, r, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return &cardFed{key: k, root: r, chain: "node-self"}
}

func (f *cardFed) MintOrgLinkCard(req federation.OrgLinkCardRequest) (*orgpolicy.LinkCard, error) {
	c := &orgpolicy.LinkCard{
		OrgID: req.OrgID, ChainID: f.chain, CASPKIPin: strings.Repeat("ef", 32),
		Endpoint: req.Endpoint, CardRev: req.CardRev, IssuedAt: req.IssuedAt, ExpiresAt: req.ExpiresAt,
	}
	if err := c.Sign(f.key); err != nil {
		return nil, err
	}
	return c, nil
}

func (f *cardFed) TransportAgentPub() string {
	return hex.EncodeToString(f.key.Public().(ed25519.PublicKey))
}

func (f *cardFed) CurrentRootCredential() (string, ed25519.PublicKey, error) {
	p := f.root.Public().(ed25519.PublicKey)
	return hex.EncodeToString(p), p, nil
}

func (f *cardFed) SetJoinPeerGate(g federation.JoinPeerGate) { f.gate = g }

const operatorID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

func (n *policyNode) cardRouter(caller string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), caller)))
		})
	})
	r.Post("/v1/org/{org_id}/card", n.s.handleOrgCardMint)
	r.Get("/v1/org/{org_id}/card", n.s.handleOrgCardStatus)
	r.Post("/v1/federation/join-overrides", n.s.handleJoinOverrideApply)
	r.Get("/v1/federation/join-overrides", n.s.handleJoinOverrideList)
	return r
}

func cardNode(t *testing.T) (*policyNode, *cardFed) {
	t.Helper()
	n := newPolicyNode(t)
	f := newCardFed(t)
	n.s.SetFederation(f)
	n.s.nodeOperatorID = operatorID
	return n, f
}

func (n *policyNode) mint(t *testing.T, caller, orgID string, body any) (*httptest.ResponseRecorder, OrgCardMintResponse) {
	t.Helper()
	b, err := json.Marshal(body)
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	n.cardRouter(caller).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgID+"/card", bytes.NewReader(b)))
	var resp OrgCardMintResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	return rr, resp
}

func (n *policyNode) cardStatus(t *testing.T) (*httptest.ResponseRecorder, OrgCardStatusResponse) {
	t.Helper()
	rr := httptest.NewRecorder()
	n.cardRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/card", nil))
	var resp OrgCardStatusResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	return rr, resp
}

func mintBody(rev uint64, endpoint string, issued int64) OrgCardMintRequest {
	return OrgCardMintRequest{CardRev: rev, IssuedAt: issued, ExpiresAt: issued + 7*24*3600, Endpoint: endpoint}
}

func TestOrgCard_MintNoOpEquivocationRegression(t *testing.T) {
	n, f := cardNode(t)
	now := time.Now().Unix()

	rr, resp := n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.1:8443", now))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionApplied, resp.Decision)
	require.NotNil(t, resp.Card)
	assert.Equal(t, f.TransportAgentPub(), resp.Card.AgentPub)
	assert.Equal(t, "node-self", resp.Card.ChainID)
	require.NoError(t, resp.Card.Verify())

	// An identical retry (same rev, same window, same endpoint) is a no-op.
	rr, resp = n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.1:8443", now))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionNoOpSameDigest, resp.Decision)

	// Same rev, different content: card equivocation, durable notice + event.
	rr, resp = n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.2:8443", now))
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionRejectedEquivocation, resp.Decision)
	require.NotNil(t, resp.Notice)
	require.Len(t, n.events, 1)
	assert.Equal(t, "governance", n.events[0].eventType)

	sr, st := n.cardStatus(t)
	require.Equal(t, http.StatusOK, sr.Code, sr.Body.String())
	assert.Equal(t, uint64(1), st.CardRev)
	assert.Equal(t, "https://10.0.0.1:8443", st.Card.Endpoint, "the accepted card is kept")
	assert.True(t, st.Current)
	require.NotNil(t, st.Notice)

	// Higher rev replaces; lower rev is refused.
	rr, _ = n.mint(t, operatorID, policyOrgID, mintBody(2, "https://10.0.0.2:8443", now))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	rr, _ = n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.1:8443", now))
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	_, st = n.cardStatus(t)
	assert.Equal(t, uint64(2), st.CardRev)
}

func TestOrgCard_Rejections(t *testing.T) {
	n, _ := cardNode(t)
	now := time.Now().Unix()

	rr, _ := n.mint(t, strings.Repeat("d", 64), policyOrgID, mintBody(1, "https://10.0.0.1:8443", now))
	assert.Equal(t, http.StatusForbidden, rr.Code, "non-operator: %s", rr.Body.String())

	rr, _ = n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.1:8443", now-3600))
	assert.Equal(t, http.StatusBadRequest, rr.Code, "stale issued_at: %s", rr.Body.String())

	rr, _ = n.mint(t, operatorID, "org-other", mintBody(1, "https://10.0.0.1:8443", now))
	assert.Equal(t, http.StatusConflict, rr.Code, "not enrolled: %s", rr.Body.String())

	bad := mintBody(1, "https://10.0.0.1:8443", now)
	bad.ExpiresAt = now + 31*24*3600
	rr, _ = n.mint(t, operatorID, policyOrgID, bad)
	assert.Equal(t, http.StatusBadRequest, rr.Code, "window over 30d: %s", rr.Body.String())

	big := []byte(`{"card_rev":1,"endpoint":"` + strings.Repeat("x", orgCardRequestCap) + `"}`)
	rr = httptest.NewRecorder()
	n.cardRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/card", bytes.NewReader(big)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code, rr.Body.String())

	sr, _ := n.cardStatus(t)
	assert.Equal(t, http.StatusNotFound, sr.Code, "nothing was stored")

	n.s.federation = nil
	rr, _ = n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.1:8443", now))
	assert.Equal(t, http.StatusNotImplemented, rr.Code, rr.Body.String())
}

func TestOrgCard_StorageFailureIs5xx(t *testing.T) {
	n, _ := cardNode(t)
	n.s.orgPolicyState = failingOrgState{n.sq}
	rr, _ := n.mint(t, operatorID, policyOrgID, mintBody(1, "https://10.0.0.1:8443", time.Now().Unix()))
	assert.Equal(t, http.StatusInternalServerError, rr.Code, rr.Body.String())
}

func signedJoinOverride(t *testing.T, root ed25519.PrivateKey, peer, chain string, rev uint64) *orgpolicy.JoinOverride {
	t.Helper()
	now := time.Now().Unix()
	o := &orgpolicy.JoinOverride{
		PeerAgentPub: peer, PeerChainID: chain, Reason: "lab peer pending allow-list",
		Revision: rev, IssuedAt: now - 10, ExpiresAt: now + 3600,
	}
	require.NoError(t, o.Sign(root))
	return o
}

func (n *policyNode) postOverride(t *testing.T, o *orgpolicy.JoinOverride) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(JoinOverrideRequest{Override: o})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	n.cardRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/federation/join-overrides", bytes.NewReader(b)))
	return rr
}

func (n *policyNode) listOverrides(t *testing.T) JoinOverrideListResponse {
	t.Helper()
	rr := httptest.NewRecorder()
	n.cardRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/federation/join-overrides", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp JoinOverrideListResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	return resp
}

func TestJoinOverride_StoreListAndRootRotation(t *testing.T) {
	n, f := cardNode(t)
	peer := strings.Repeat("9", 64)

	rr := n.postOverride(t, signedJoinOverride(t, f.root, peer, "peer-chain", 1))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	list := n.listOverrides(t)
	require.Len(t, list.Overrides, 1)
	assert.Equal(t, "usable", list.Overrides[0].Status)

	// Not signed by the current Root.
	_, other, _ := ed25519.GenerateKey(rand.Reader)
	rr = n.postOverride(t, signedJoinOverride(t, other, strings.Repeat("8", 64), "peer-chain", 1))
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())

	// Same revision, different content.
	o := signedJoinOverride(t, f.root, peer, "peer-chain", 1)
	o.Reason = "changed"
	require.NoError(t, o.Sign(f.root))
	rr = n.postOverride(t, o)
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())

	// Root rotates: the stored override no longer applies.
	_, f.root, _ = ed25519.GenerateKey(rand.Reader)
	list = n.listOverrides(t)
	assert.Equal(t, "not_current_root", list.Overrides[0].Status)

	big := []byte(`{"override":{"reason":"` + strings.Repeat("x", joinOverrideRequestCap) + `"}}`)
	rr = httptest.NewRecorder()
	n.cardRouter(operatorID).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/federation/join-overrides", bytes.NewReader(big)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code, rr.Body.String())
}

func gateCheck(f *cardFed, peer, chain string) federation.JoinGateCheck {
	id, p, _ := f.CurrentRootCredential()
	return federation.JoinGateCheck{
		Side: "host", Stage: "request", SessionID: "S1",
		PeerAgentPub: peer, PeerChainID: chain,
		RootCredentialID: id, RootPub: p, Now: time.Now(),
	}
}

func TestFleetJoinGate_WiredBySetFederation(t *testing.T) {
	n, f := cardNode(t)
	require.NotNil(t, f.gate, "SetFederation installs the fleet JOIN gate")
	peer := strings.Repeat("9", 64)

	// The node is enrolled under a v1 policy: no peer is eligible.
	err := f.gate(gateCheck(f, peer, "peer-chain"))
	require.ErrorIs(t, err, federation.ErrJoinPeerNotEligible)
	assert.Contains(t, err.Error(), "v1 org policy")

	// A v2 policy naming the peer makes it eligible.
	p2 := signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 2)
	p2.Schema = orgpolicy.SchemaIDV2
	p2.Payload.PeerAllowList = []string{peer}
	require.NoError(t, p2.Sign(n.orgPriv))
	rr, _ := n.deliver(t, policyOrgID, p2, nil)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.NoError(t, f.gate(gateCheck(f, peer, "peer-chain")))

	// Another peer is refused, then let through by a Root override; the use
	// raises a governance event and is listed.
	other := strings.Repeat("8", 64)
	require.ErrorIs(t, f.gate(gateCheck(f, other, "other-chain")), federation.ErrJoinPeerNotEligible)
	require.Equal(t, http.StatusOK, n.postOverride(t, signedJoinOverride(t, f.root, other, "other-chain", 1)).Code)
	n.events = nil
	require.NoError(t, f.gate(gateCheck(f, other, "other-chain")))
	require.Len(t, n.events, 1)
	assert.Equal(t, "fleet_join_override_used", n.events[0].data.(map[string]any)["action"])
	list := n.listOverrides(t)
	require.Len(t, list.Overrides, 1)
	assert.EqualValues(t, 1, list.Overrides[0].TotalUses)

	// A storage failure refuses.
	n.s.orgPolicyState = listFailingOrgState{n.sq}
	require.ErrorIs(t, f.gate(gateCheck(f, peer, "peer-chain")), federation.ErrJoinPeerNotEligible)
}

// listFailingOrgState fails key listing, standing in for a storage failure on
// the enrollment lookup.
type listFailingOrgState struct{ orgPolicyStateStore }

func (listFailingOrgState) ListOrgPolicyStateKeys(_ context.Context, _ string, _ int) ([]string, error) {
	return nil, errors.New("disk I/O error")
}
