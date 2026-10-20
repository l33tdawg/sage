package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/orgpolicy"
	"github.com/l33tdawg/sage/internal/store"
)

// setOrgPolicyClock moves the org-policy clock for the duration of a test.
func setOrgPolicyClock(t *testing.T, now int64) {
	t.Helper()
	prev := orgPolicyNow
	orgPolicyNow = func() int64 { return now }
	t.Cleanup(func() { orgPolicyNow = prev })
}

// The fixture policy's window is NotBefore 1000 .. Expiry 4102444800.
const (
	policyValidAt   = int64(2_000_000_000)
	policyExpiredAt = int64(4_102_444_800)
)

func TestOrgEnroll_ExpiredFirstPolicyRefusedButReplayRecovers(t *testing.T) {
	const orgID = "org-acme"
	_, orgPriv, orgHex := orgKeypair(t)
	ts := newStubEnrollTokenStore()
	ts.seed("bearer", &store.OrgEnrollmentToken{ID: "tok-1", OrgID: orgID, OwnerID: "o", NodeLabel: "n", Role: "member", OrgSignerPub: orgHex})
	s := newEnrollServer(t, ts)
	enrollee := strings.Repeat("a", 64)
	body := enrollBody(t, "bearer", signedOrgPolicy(t, orgPriv, orgID, "epoch-1", 1), nil)

	// Enrollment is authority-granting: an expired first policy is refused and
	// nothing is consumed.
	setOrgPolicyClock(t, policyExpiredAt)
	rr := doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, body)
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	_, consumed, err := s.orgPolicyStore(context.Background()).LoadEnrollment("tok-1")
	require.NoError(t, err)
	assert.False(t, consumed, "a refused enrollment must not consume the token")

	// Enroll while valid, then lose the response: the replay after expiry is
	// recovery of an existing outcome, not new authority, and still succeeds.
	setOrgPolicyClock(t, policyValidAt)
	rr = doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, body)
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	setOrgPolicyClock(t, policyExpiredAt)
	rr = doEnroll(enrollRouterAs(s, enrollee), orgID, enrollee, body)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

func TestOrgPolicyStatus_ReportsPerOperationGate(t *testing.T) {
	n := newPolicyNode(t)
	byOp := func(st OrgPolicyStatusResponse) map[orgpolicy.Operation]orgpolicy.GateResult {
		m := map[orgpolicy.Operation]orgpolicy.GateResult{}
		for _, g := range st.Operations {
			m[g.Operation] = g
		}
		return m
	}

	setOrgPolicyClock(t, policyValidAt)
	_, st := n.status(t)
	assert.True(t, st.Current)
	require.Len(t, st.Operations, len(orgpolicy.Operations))
	for _, g := range st.Operations {
		assert.True(t, g.Allowed, "everything proceeds on a current policy: %+v", g)
	}

	// Management service gone and the policy lapsed: authority-granting and
	// destructive operations hold; access, admission and retention continue.
	setOrgPolicyClock(t, policyExpiredAt)
	_, st = n.status(t)
	assert.False(t, st.Current)
	ops := byOp(st)
	assert.Equal(t, orgpolicy.ModeIndependent, ops[orgpolicy.OpLocalAccess].Mode)
	assert.Equal(t, orgpolicy.ModeLastValid, ops[orgpolicy.OpAdmission].Mode)
	assert.Equal(t, orgpolicy.ModeLastValid, ops[orgpolicy.OpRetention].Mode)
	assert.False(t, ops[orgpolicy.OpNewPeering].Allowed)
	assert.False(t, ops[orgpolicy.OpEnrollment].Allowed)
	assert.False(t, ops[orgpolicy.OpRetentionPurge].Allowed)
}

// federationProposeAs posts a federation proposal from callerID.
func federationProposeAs(s *Server, callerID string, body string) *httptest.ResponseRecorder {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Post("/v1/federation/propose", s.handleFederationPropose)
	rr := httptest.NewRecorder()
	r.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/federation/propose", bytes.NewReader([]byte(body))))
	return rr
}

func TestFederationPropose_HeldOnStaleOrgPolicy(t *testing.T) {
	n := newPolicyNode(t) // holds a fleet policy for policyOrgID
	member := strings.Repeat("d", 64)
	require.NoError(t, n.s.badgerStore.RegisterOrg(policyOrgID, "Acme", "", member, 1))
	require.NoError(t, n.s.badgerStore.AddOrgMember(policyOrgID, member, 1, "member", 1))
	body, err := json.Marshal(FederationProposeReq{ProposerOrgID: policyOrgID, TargetOrgID: "org-target"})
	require.NoError(t, err)

	// Expired policy: a NEW peering is held before anything reaches consensus.
	setOrgPolicyClock(t, policyExpiredAt)
	rr := federationProposeAs(n.s, member, string(body))
	require.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "Held by org policy")

	// Current policy: the gate lets it through to the normal consensus path
	// (which has no chain in this unit test, so it fails later — but not here).
	setOrgPolicyClock(t, policyValidAt)
	rr = federationProposeAs(n.s, member, string(body))
	assert.NotContains(t, rr.Body.String(), "Held by org policy", rr.Body.String())
}

func TestFederationPropose_UnmanagedOrgNeverHeld(t *testing.T) {
	n := newPolicyNode(t)
	member := strings.Repeat("e", 64)
	require.NoError(t, n.s.badgerStore.RegisterOrg("org-legacy", "Legacy", "", member, 1))
	require.NoError(t, n.s.badgerStore.AddOrgMember("org-legacy", member, 1, "member", 1))
	body, err := json.Marshal(FederationProposeReq{ProposerOrgID: "org-legacy", TargetOrgID: "org-target"})
	require.NoError(t, err)

	// No fleet policy for this org: the gate must not change its behavior, even
	// at a time when every fleet policy would be expired.
	setOrgPolicyClock(t, policyExpiredAt)
	rr := federationProposeAs(n.s, member, string(body))
	assert.NotContains(t, rr.Body.String(), "Held by org policy", rr.Body.String())
}
