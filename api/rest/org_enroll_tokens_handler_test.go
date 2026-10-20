package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	orgTokAdminID  = "00000000000000000000000000000000000000000000000000000000000000a1"
	orgTokMemberID = "00000000000000000000000000000000000000000000000000000000000000b2"
	orgTokEnrollee = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	orgTokOrgID    = "org-fleet"
)

// newOrgTokenServer wires a real SQLite store (issuance + lookup) and a real
// BadgerStore holding the on-chain org and a global admin, so authorization and
// org existence run through the production reads.
func newOrgTokenServer(t *testing.T) *Server {
	t.Helper()
	memStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "org-tokens.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = memStore.Close() })

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	require.NoError(t, bs.RegisterAgent(orgTokAdminID, "admin", "admin", "", "", "", 1))
	require.NoError(t, bs.RegisterAgent(orgTokMemberID, "member", "member", "", "", "", 1))
	require.NoError(t, bs.RegisterOrg(orgTokOrgID, "Fleet", "", orgTokAdminID, 1))

	return &Server{
		store:           memStore,
		badgerStore:     bs,
		orgEnrollTokens: memStore,
		orgPolicyState:  memStore,
		logger:          zerolog.Nop(),
	}
}

// orgTokenRouterAs mounts the management routes and the enroll route with
// callerID injected as the authenticated agent.
func orgTokenRouterAs(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Post("/v1/org/{org_id}/enroll", s.handleOrgEnroll)
	r.Post("/v1/org/{org_id}/enroll-tokens", s.handleOrgEnrollTokenIssue)
	r.Get("/v1/org/{org_id}/enroll-tokens", s.handleOrgEnrollTokenList)
	r.Delete("/v1/org/{org_id}/enroll-tokens/{id}", s.handleOrgEnrollTokenRevoke)
	return r
}

func issueOrgToken(t *testing.T, h http.Handler, orgID, body string) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgID+"/enroll-tokens", strings.NewReader(body)))
	return rr
}

func issueBody(signerHex, enrolleeHex string) string {
	return `{"node_label":"edge-7","role":"member","enrollee_pub":"` + enrolleeHex + `","scope":"fleet","org_signer_pub":"` + signerHex + `","key_epoch":"e1"}`
}

func TestOrgEnrollTokenIssue_AdminGetsOneShotToken(t *testing.T) {
	s := newOrgTokenServer(t)
	_, _, signerHex := orgKeypair(t)

	rr := issueOrgToken(t, orgTokenRouterAs(s, orgTokAdminID), orgTokOrgID, issueBody(signerHex, orgTokEnrollee))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())

	var resp OrgEnrollTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.NotEmpty(t, resp.ID)
	assert.NotEmpty(t, resp.Token)
	assert.Equal(t, orgTokOrgID, resp.OrgID)
	assert.Equal(t, orgTokAdminID, resp.OwnerID, "owner is the signed caller, not a body field")
	assert.Equal(t, "edge-7", resp.NodeLabel)
	assert.Equal(t, "member", resp.Role)
	assert.Equal(t, orgTokEnrollee, resp.EnrolleePub)
	assert.Equal(t, "fleet", resp.Scope)
	assert.False(t, resp.ExpiresAt.IsZero())
}

func TestOrgEnrollTokenIssue_NonAdminForbidden(t *testing.T) {
	s := newOrgTokenServer(t)
	_, _, signerHex := orgKeypair(t)

	rr := issueOrgToken(t, orgTokenRouterAs(s, orgTokMemberID), orgTokOrgID, issueBody(signerHex, orgTokEnrollee))
	require.Equal(t, http.StatusForbidden, rr.Code)

	rows, err := s.store.(orgEnrollTokenAdminStore).ListOrgEnrollmentTokens(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, rows, "a refused issue must not persist anything")
}

func TestOrgEnrollTokenIssue_UnknownOrgNotFound(t *testing.T) {
	s := newOrgTokenServer(t)
	_, _, signerHex := orgKeypair(t)

	rr := issueOrgToken(t, orgTokenRouterAs(s, orgTokAdminID), "no-such-org", issueBody(signerHex, orgTokEnrollee))
	require.Equal(t, http.StatusNotFound, rr.Code)
}

func TestOrgEnrollTokenIssue_ValidatesBody(t *testing.T) {
	s := newOrgTokenServer(t)
	h := orgTokenRouterAs(s, orgTokAdminID)
	_, _, signerHex := orgKeypair(t)

	cases := map[string]string{
		"missing label":    `{"role":"member","enrollee_pub":"` + orgTokEnrollee + `","scope":"fleet","org_signer_pub":"` + signerHex + `"}`,
		"missing enrollee": `{"node_label":"n","role":"member","scope":"fleet","org_signer_pub":"` + signerHex + `"}`,
		"missing scope":    `{"node_label":"n","role":"member","enrollee_pub":"` + orgTokEnrollee + `","org_signer_pub":"` + signerHex + `"}`,
		"admin role":       `{"node_label":"n","role":"admin","enrollee_pub":"` + orgTokEnrollee + `","scope":"fleet","org_signer_pub":"` + signerHex + `"}`,
		"unknown role":     `{"node_label":"n","role":"root","enrollee_pub":"` + orgTokEnrollee + `","scope":"fleet","org_signer_pub":"` + signerHex + `"}`,
		"short signer":     `{"node_label":"n","enrollee_pub":"` + orgTokEnrollee + `","scope":"fleet","org_signer_pub":"abcd"}`,
		"non-hex signer":   `{"node_label":"n","enrollee_pub":"` + orgTokEnrollee + `","scope":"fleet","org_signer_pub":"` + strings.Repeat("z", 64) + `"}`,
		"bad json":         `{`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			rr := issueOrgToken(t, h, orgTokOrgID, body)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
		})
	}
}

func TestOrgEnrollTokenIssue_RoleDefaultsToMember(t *testing.T) {
	s := newOrgTokenServer(t)
	_, _, signerHex := orgKeypair(t)

	rr := issueOrgToken(t, orgTokenRouterAs(s, orgTokAdminID), orgTokOrgID,
		`{"node_label":"n","enrollee_pub":"`+orgTokEnrollee+`","scope":"fleet","org_signer_pub":"`+signerHex+`"}`)
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	var resp OrgEnrollTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	assert.Equal(t, "member", resp.Role)
}

func TestOrgEnrollTokenList_NeverReturnsPlaintext(t *testing.T) {
	s := newOrgTokenServer(t)
	h := orgTokenRouterAs(s, orgTokAdminID)
	_, _, signerHex := orgKeypair(t)

	rr := issueOrgToken(t, h, orgTokOrgID, issueBody(signerHex, orgTokEnrollee))
	require.Equal(t, http.StatusCreated, rr.Code)
	var issued OrgEnrollTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&issued))

	lr := httptest.NewRecorder()
	h.ServeHTTP(lr, httptest.NewRequest(http.MethodGet, "/v1/org/"+orgTokOrgID+"/enroll-tokens", nil))
	require.Equal(t, http.StatusOK, lr.Code)
	assert.NotContains(t, lr.Body.String(), issued.Token, "list must never expose the plaintext")

	var list struct {
		Tokens []OrgEnrollTokenSummary `json:"tokens"`
	}
	require.NoError(t, json.Unmarshal(lr.Body.Bytes(), &list))
	require.Len(t, list.Tokens, 1)
	assert.Equal(t, issued.ID, list.Tokens[0].ID)
	assert.Equal(t, signerHex, list.Tokens[0].OrgSignerPub)
	assert.Equal(t, orgTokEnrollee, list.Tokens[0].EnrolleePub)
	assert.Equal(t, "fleet", list.Tokens[0].Scope)

	// Non-admins cannot enumerate the org's tokens.
	fr := httptest.NewRecorder()
	orgTokenRouterAs(s, orgTokMemberID).ServeHTTP(fr,
		httptest.NewRequest(http.MethodGet, "/v1/org/"+orgTokOrgID+"/enroll-tokens", nil))
	assert.Equal(t, http.StatusForbidden, fr.Code)
}

func TestOrgEnrollTokenRevoke_ScopedToPathOrg(t *testing.T) {
	s := newOrgTokenServer(t)
	require.NoError(t, s.badgerStore.RegisterOrg("org-other", "Other", "", orgTokAdminID, 1))
	h := orgTokenRouterAs(s, orgTokAdminID)
	_, _, signerHex := orgKeypair(t)

	rr := issueOrgToken(t, h, orgTokOrgID, issueBody(signerHex, orgTokEnrollee))
	require.Equal(t, http.StatusCreated, rr.Code)
	var issued OrgEnrollTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&issued))

	// Revoking through a different org's path is not-found, and leaves it live.
	xr := httptest.NewRecorder()
	h.ServeHTTP(xr, httptest.NewRequest(http.MethodDelete, "/v1/org/org-other/enroll-tokens/"+issued.ID, nil))
	assert.Equal(t, http.StatusNotFound, xr.Code)

	dr := httptest.NewRecorder()
	h.ServeHTTP(dr, httptest.NewRequest(http.MethodDelete, "/v1/org/"+orgTokOrgID+"/enroll-tokens/"+issued.ID, nil))
	assert.Equal(t, http.StatusNoContent, dr.Code)

	// Idempotent.
	dr2 := httptest.NewRecorder()
	h.ServeHTTP(dr2, httptest.NewRequest(http.MethodDelete, "/v1/org/"+orgTokOrgID+"/enroll-tokens/"+issued.ID, nil))
	assert.Equal(t, http.StatusNoContent, dr2.Code)

	// Non-admin revoke is refused.
	fr := httptest.NewRecorder()
	orgTokenRouterAs(s, orgTokMemberID).ServeHTTP(fr,
		httptest.NewRequest(http.MethodDelete, "/v1/org/"+orgTokOrgID+"/enroll-tokens/"+issued.ID, nil))
	assert.Equal(t, http.StatusForbidden, fr.Code)
}

// End to end against the real stores: an admin issues a token, a fresh node
// redeems it at the live enroll endpoint, and after revocation a second token
// can no longer be redeemed.
func TestOrgEnrollToken_IssueThenRedeemEndToEnd(t *testing.T) {
	s := newOrgTokenServer(t)
	_, orgPriv, orgPubHex := orgKeypair(t)
	_, _, nodeHex := orgKeypair(t)

	rr := issueOrgToken(t, orgTokenRouterAs(s, orgTokAdminID), orgTokOrgID, issueBody(orgPubHex, nodeHex))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	var issued OrgEnrollTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&issued))

	policy := signedOrgPolicy(t, orgPriv, orgTokOrgID, "e1", 1)
	node := orgTokenRouterAs(s, nodeHex)
	er := httptest.NewRecorder()
	node.ServeHTTP(er, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgTokOrgID+"/enroll",
		bytes.NewReader(enrollBody(t, issued.Token, policy, nil))))
	require.Equal(t, http.StatusCreated, er.Code, er.Body.String())
	var out OrgEnrollResponse
	require.NoError(t, json.NewDecoder(er.Body).Decode(&out))
	assert.Equal(t, nodeHex, out.EnrolleePub)
	assert.Equal(t, orgTokAdminID+"/edge-7/member", out.BoundIdentity)

	// Revoking the already-used bearer blocks new redemptions but not the bound
	// node's retry/recovery replay.
	drUsed := httptest.NewRecorder()
	orgTokenRouterAs(s, orgTokAdminID).ServeHTTP(drUsed,
		httptest.NewRequest(http.MethodDelete, "/v1/org/"+orgTokOrgID+"/enroll-tokens/"+issued.ID, nil))
	require.Equal(t, http.StatusNoContent, drUsed.Code)
	replay := httptest.NewRecorder()
	node.ServeHTTP(replay, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgTokOrgID+"/enroll",
		bytes.NewReader(enrollBody(t, issued.Token, policy, nil))))
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	var replayOut OrgEnrollResponse
	require.NoError(t, json.NewDecoder(replay.Body).Decode(&replayOut))
	assert.True(t, replayOut.Replayed)

	// A second token, revoked before use, is refused at redeem.
	_, _, node2Hex := orgKeypair(t)
	rr2 := issueOrgToken(t, orgTokenRouterAs(s, orgTokAdminID), orgTokOrgID, issueBody(orgPubHex, node2Hex))
	require.Equal(t, http.StatusCreated, rr2.Code)
	var issued2 OrgEnrollTokenIssueResponse
	require.NoError(t, json.NewDecoder(rr2.Body).Decode(&issued2))
	dr := httptest.NewRecorder()
	orgTokenRouterAs(s, orgTokAdminID).ServeHTTP(dr,
		httptest.NewRequest(http.MethodDelete, "/v1/org/"+orgTokOrgID+"/enroll-tokens/"+issued2.ID, nil))
	require.Equal(t, http.StatusNoContent, dr.Code)

	er2 := httptest.NewRecorder()
	orgTokenRouterAs(s, node2Hex).ServeHTTP(er2, httptest.NewRequest(http.MethodPost, "/v1/org/"+orgTokOrgID+"/enroll",
		bytes.NewReader(enrollBody(t, issued2.Token, policy, nil))))
	assert.Equal(t, http.StatusForbidden, er2.Code)
}
