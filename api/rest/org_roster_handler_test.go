package rest

import (
	"bytes"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/orgpolicy"
)

func (n *policyNode) rosterRouter() http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), strings.Repeat("c", 64))))
		})
	})
	r.Post("/v1/org/{org_id}/roster", n.s.handleOrgRosterUpdate)
	r.Get("/v1/org/{org_id}/roster", n.s.handleOrgRosterStatus)
	r.Post("/v1/org/{org_id}/policy", n.s.handleOrgPolicyUpdate)
	return r
}

func (n *policyNode) deliverRoster(t *testing.T, orgID string, ro *orgpolicy.Roster) (*httptest.ResponseRecorder, OrgRosterUpdateResponse) {
	t.Helper()
	body, err := json.Marshal(OrgRosterUpdateRequest{Roster: ro})
	require.NoError(t, err)
	rr := httptest.NewRecorder()
	n.rosterRouter().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgID+"/roster", bytes.NewReader(body)))
	var resp OrgRosterUpdateResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &resp)
	return rr, resp
}

func testLinkCard(t *testing.T, rev uint64, chain string) orgpolicy.LinkCard {
	t.Helper()
	_, sk, err := auth.GenerateKeypair()
	require.NoError(t, err)
	now := time.Now().Unix()
	c := orgpolicy.LinkCard{
		OrgID: policyOrgID, ChainID: chain, CASPKIPin: strings.Repeat("ab", 32),
		Endpoint: "https://10.0.0.1:8443", CardRev: rev, IssuedAt: now - 60, ExpiresAt: now + 24*3600,
	}
	require.NoError(t, c.Sign(sk))
	return c
}

func testRoster(t *testing.T, key ed25519.PrivateKey, scope string, rev uint64, cards ...orgpolicy.LinkCard) *orgpolicy.Roster {
	t.Helper()
	now := time.Now().Unix()
	r := &orgpolicy.Roster{
		OrgID: policyOrgID, Scope: scope, Revision: rev, KeyEpoch: "epoch-1",
		IssuedAt: now - 60, NotBefore: now - 60, Expiry: now + 3*24*3600, Cards: cards,
	}
	require.NoError(t, r.Sign(key))
	return r
}

func TestOrgRoster_ApplyNoOpRollbackAndStatus(t *testing.T) {
	n := newPolicyNode(t)
	a, b := testLinkCard(t, 1, "node-a"), testLinkCard(t, 1, "node-b")

	rr, resp := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 2, a, b))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Equal(t, orgpolicy.DecisionApplied, resp.Decision)
	assert.Equal(t, 2, resp.Accepted.Cards)

	r2 := testRoster(t, n.orgPriv, "fleet", 3, a)
	rr, resp = n.deliverRoster(t, policyOrgID, r2)
	require.Equal(t, http.StatusOK, rr.Code)
	rr, resp = n.deliverRoster(t, policyOrgID, r2)
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, orgpolicy.DecisionNoOpSameDigest, resp.Decision)

	rr, resp = n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 2, a))
	assert.Equal(t, http.StatusConflict, rr.Code)
	assert.Equal(t, orgpolicy.DecisionRejectedRollback, resp.Decision)

	sr := httptest.NewRecorder()
	n.rosterRouter().ServeHTTP(sr, httptest.NewRequest(http.MethodGet, "/v1/org/"+policyOrgID+"/roster", nil))
	require.Equal(t, http.StatusOK, sr.Code)
	var st OrgRosterStatusResponse
	require.NoError(t, json.Unmarshal(sr.Body.Bytes(), &st))
	assert.Equal(t, uint64(3), st.Accepted.Revision)
	assert.True(t, st.Current)
	require.NotNil(t, st.Roster)
	assert.Len(t, st.Roster.Cards, 1)
}

func TestOrgRoster_EquivocationAlarms(t *testing.T) {
	n := newPolicyNode(t)
	a := testLinkCard(t, 1, "node-a")
	rr, _ := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, a))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	rr, resp := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1))
	require.Equal(t, http.StatusConflict, rr.Code)
	require.NotNil(t, resp.Notice)
	assert.Equal(t, "roster", resp.Notice.Kind)
	require.NotEmpty(t, n.events)
	assert.Equal(t, "governance", n.events[len(n.events)-1].eventType)
}

func TestOrgRoster_Rejections(t *testing.T) {
	n := newPolicyNode(t)
	a := testLinkCard(t, 1, "node-a")

	// Signed by a key that is not authorized under the accepted policy root.
	_, rogue, _ := orgKeypair(t)
	rr, _ := n.deliverRoster(t, policyOrgID, testRoster(t, rogue, "fleet", 1, a))
	assert.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())

	// Scope must equal the enrolled policy scope.
	rr, _ = n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "other-scope", 1, a))
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())

	// Not enrolled in the org.
	other := testRoster(t, n.orgPriv, "fleet", 1, a)
	other.OrgID = "org-other"
	rr, _ = n.deliverRoster(t, "org-other", other)
	assert.Equal(t, http.StatusConflict, rr.Code)

	// Path org and roster org must agree; missing body.
	rr, _ = n.deliverRoster(t, "org-other", testRoster(t, n.orgPriv, "fleet", 2, a))
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	br := httptest.NewRecorder()
	n.rosterRouter().ServeHTTP(br, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/roster", strings.NewReader(`{}`)))
	assert.Equal(t, http.StatusBadRequest, br.Code)
}

func TestOrgRoster_RequestCapEnforcedBeforeDecode(t *testing.T) {
	n := newPolicyNode(t)
	big := bytes.Repeat([]byte(" "), orgpolicy.MaxRosterRequestBytes+1)
	rr := httptest.NewRecorder()
	n.rosterRouter().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/roster", bytes.NewReader(big)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
}

func TestOrgPolicy_RequestCapRaisedTo256KiB(t *testing.T) {
	n := newPolicyNode(t)
	// A valid delivery padded with JSON whitespace past the old 64 KiB cap is
	// now accepted...
	body, err := json.Marshal(OrgPolicyUpdateRequest{Policy: signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 2)})
	require.NoError(t, err)
	padded := append(bytes.Repeat([]byte(" "), 100<<10), body...)
	rr := httptest.NewRecorder()
	n.rosterRouter().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/policy", bytes.NewReader(padded)))
	assert.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// ...and past 256 KiB it is refused before decode.
	over := append(bytes.Repeat([]byte(" "), orgpolicy.MaxPolicyRequestBytes), body...)
	rr = httptest.NewRecorder()
	n.rosterRouter().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+policyOrgID+"/policy", bytes.NewReader(over)))
	assert.Equal(t, http.StatusRequestEntityTooLarge, rr.Code)
}

func TestOrgPolicy_AcceptedBodyStoredForEligibility(t *testing.T) {
	n := newPolicyNode(t)
	ps := n.s.orgPolicyStore(t.Context())
	st, body, err := ps.LoadAcceptedPolicy(policyOrgID)
	require.NoError(t, err)
	require.NotNil(t, body, "enrollment must store the first policy body")
	assert.Equal(t, st.Digest, body.Digest())

	p2 := signedOrgPolicy(t, n.orgPriv, policyOrgID, "epoch-1", 2)
	rr, _ := n.deliver(t, policyOrgID, p2, nil)
	require.Equal(t, http.StatusOK, rr.Code)
	st, body, err = ps.LoadAcceptedPolicy(policyOrgID)
	require.NoError(t, err)
	require.NotNil(t, body)
	assert.Equal(t, uint64(2), body.Revision)
	assert.Equal(t, st.Digest, p2.Digest())
}

func TestOrgRoster_CardRegressionRejectsWholeRoster(t *testing.T) {
	n := newPolicyNode(t)
	_, sk, err := auth.GenerateKeypair()
	require.NoError(t, err)
	now := time.Now().Unix()
	card := func(rev uint64) orgpolicy.LinkCard {
		c := orgpolicy.LinkCard{
			OrgID: policyOrgID, ChainID: "node-a", CASPKIPin: strings.Repeat("ab", 32),
			Endpoint: "https://10.0.0.1:8443", CardRev: rev, IssuedAt: now - 60, ExpiresAt: now + 3600,
		}
		require.NoError(t, c.Sign(sk))
		return c
	}
	rr, _ := n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 1, card(2)))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	other := testLinkCard(t, 1, "node-b")
	rr, _ = n.deliverRoster(t, policyOrgID, testRoster(t, n.orgPriv, "fleet", 2, card(1), other))
	assert.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	ps := n.s.orgPolicyStore(t.Context())
	st, _, err := ps.LoadRoster(policyOrgID)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), st.Revision, "regressing roster must not apply")
	_, ok, err := ps.LoadCard(policyOrgID, other.AgentPub)
	require.NoError(t, err)
	assert.False(t, ok, "no card from a rejected roster may be stored")
}
