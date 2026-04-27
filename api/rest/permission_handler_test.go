package rest

// Regression tests for v6.6.9 — silent-failure on PUT /v1/agent/{id}/permission.
//
// Bug B (Level Up follow-up to v6.6.8): a non-admin caller setting
// `visible_agents="*"` on themselves got 200 + a real tx_hash but
// `network_agents.visible_agents` stayed empty. Two root causes:
//
//  1. REST used broadcast_tx_sync, which only checks CheckTx (signature/
//     nonce) — the FinalizeBlock rejection (code=67 "not an admin") was
//     never propagated to the client.
//  2. The ABCI handler hard-gated permission writes on the on-chain global
//     "admin" Role — meaning only the original deployment-admin identity
//     could ever set permissions, which is wrong for the common case of an
//     agent declaring its own RBAC surface.
//
// Fix:
//   - REST does a fail-fast pre-flight RBAC check using the Badger store,
//     returning 403 instead of broadcasting a doomed tx.
//   - REST switched to broadcast_tx_commit so any FinalizeBlock failure
//     surfaces as an error (defense-in-depth: 403 if access denied,
//     500 otherwise).
//   - ABCI auth model widened to: self-set OR global admin OR org admin
//     of any org the target also belongs to.

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// permTestCometMock returns an httptest server impersonating CometBFT's
// broadcast_tx_commit endpoint with a configurable code/log so tests can
// simulate ABCI rejections without standing up the real chain.
func permTestCometMock(t *testing.T, txCode int, txLog string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{
				"check_tx":  map[string]interface{}{"code": 0, "log": ""},
				"tx_result": map[string]interface{}{"code": txCode, "log": txLog},
				"hash":      "PERMTX0001",
				"height":    "1",
			},
		})
	}))
}

// signedRequestAs builds a signed PUT /v1/agent/<targetID>/permission
// request from a specific keypair — used so tests can assert that the
// caller's *agent identity* (not just any signed request) drives the
// pre-flight RBAC outcome.
func signedRequestAs(t *testing.T, priv ed25519.PrivateKey, callerID, method, path string, body []byte) *http.Request {
	t.Helper()
	ts := time.Now().Unix()
	sig := auth.SignRequest(priv, method, path, body, ts)

	var req *http.Request
	if body != nil {
		req = httptest.NewRequest(method, path, bytes.NewReader(body))
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.Header.Set("X-Agent-ID", callerID)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
	return req
}

// setPermBody is the JSON body used by every permission test.
var setPermBody = []byte(`{"clearance":3,"domain_access":"","visible_agents":"*"}`)

// TestSetPermission_SelfSet_Succeeds — agent X sets permission on itself.
// Auth model rule #1 (self-set): no admin role required, no org membership
// required. Returns 200 + tx_hash, and the chain accepts the tx.
func TestSetPermission_SelfSet_Succeeds(t *testing.T) {
	cometMock := permTestCometMock(t, 0, "agent permissions updated")
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	agentID := auth.PublicKeyToAgentID(pub)
	require.NoError(t, bs.RegisterAgent(agentID, "self-agent", "member", "", "", "", 1))

	req := signedRequestAs(t, priv, agentID, http.MethodPut, "/v1/agent/"+agentID+"/permission", setPermBody)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "self-set must succeed; body: %s", rr.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "permissions_updated", resp["status"])
	assert.NotEmpty(t, resp["tx_hash"], "must return a tx_hash on success")
}

// TestSetPermission_OrgAdmin_Succeeds — an org admin sets permission on a
// member of that org. Auth model rule #3.
func TestSetPermission_OrgAdmin_Succeeds(t *testing.T) {
	cometMock := permTestCometMock(t, 0, "agent permissions updated")
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs

	// Org admin (NOT a global admin — registers as plain "member") and a
	// regular org member; auth comes purely from org-membership role.
	adminPub, adminPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	adminID := auth.PublicKeyToAgentID(adminPub)
	require.NoError(t, bs.RegisterAgent(adminID, "org-admin", "member", "", "", "", 1))

	memberPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	memberID := auth.PublicKeyToAgentID(memberPub)
	require.NoError(t, bs.RegisterAgent(memberID, "org-member", "member", "", "", "", 2))

	const orgID = "org-rest-perm-test"
	require.NoError(t, bs.RegisterOrg(orgID, "Perm Test Org", "", adminID, 1))
	require.NoError(t, bs.AddOrgMember(orgID, adminID, 4, "admin", 1))
	require.NoError(t, bs.AddOrgMember(orgID, memberID, 1, "member", 2))

	req := signedRequestAs(t, adminPriv, adminID, http.MethodPut, "/v1/agent/"+memberID+"/permission", setPermBody)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "org-admin set must succeed; body: %s", rr.Body.String())
}

// TestSetPermission_Unauthorized_Returns403 — a random agent tries to set
// permissions on someone they have no relationship with. The REST layer
// must reject with 403 BEFORE broadcasting (no tx_hash returned).
//
// This is the exact silent-failure scenario from the Level Up bug: prior
// to v6.6.9 this returned 200 + tx_hash with empty SQL.
func TestSetPermission_Unauthorized_Returns403(t *testing.T) {
	// The mock would happily return success — but a correctly-implemented
	// REST handler must short-circuit with 403 before even calling it.
	var cometCalled bool
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		cometCalled = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{
				"check_tx":  map[string]interface{}{"code": 0},
				"tx_result": map[string]interface{}{"code": 0},
				"hash":      "SHOULD_NEVER_BE_RETURNED",
				"height":    "1",
			},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs

	callerPub, callerPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	callerID := auth.PublicKeyToAgentID(callerPub)
	require.NoError(t, bs.RegisterAgent(callerID, "random-caller", "member", "", "", "", 1))

	targetPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	targetID := auth.PublicKeyToAgentID(targetPub)
	require.NoError(t, bs.RegisterAgent(targetID, "victim", "member", "", "", "", 2))

	req := signedRequestAs(t, callerPriv, callerID, http.MethodPut, "/v1/agent/"+targetID+"/permission", setPermBody)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusForbidden, rr.Code, "unauthorized cross-agent set must return 403; body: %s", rr.Body.String())
	assert.False(t, cometCalled, "REST must NOT broadcast when pre-flight RBAC denies the request")

	// The response must be RFC7807-style and must NOT include a tx_hash —
	// that's the exact misleading shape that caused the Level Up bug.
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	_, hasTxHash := resp["tx_hash"]
	assert.False(t, hasTxHash, "403 response must NOT include a tx_hash")
}

// TestSetPermission_GlobalAdmin_Succeeds — backward-compat: an agent
// registered with on-chain Role="admin" can still set permissions on
// anyone. This preserves the legacy deployment-admin path so existing
// single-org installs keep working.
func TestSetPermission_GlobalAdmin_Succeeds(t *testing.T) {
	cometMock := permTestCometMock(t, 0, "ok")
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	defer bs.CloseBadger()
	srv.badgerStore = bs

	adminPub, adminPriv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	adminID := auth.PublicKeyToAgentID(adminPub)
	require.NoError(t, bs.RegisterAgent(adminID, "deployment-admin", "admin", "", "", "", 1))

	targetPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	targetID := auth.PublicKeyToAgentID(targetPub)
	require.NoError(t, bs.RegisterAgent(targetID, "subject", "member", "", "", "", 2))

	req := signedRequestAs(t, adminPriv, adminID, http.MethodPut, "/v1/agent/"+targetID+"/permission", setPermBody)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusOK, rr.Code, "global-admin must keep working; body: %s", rr.Body.String())
}

// TestSetPermission_FinalizeBlockReject_Surfaces403 — defense-in-depth.
// Even if a caller somehow slips past the REST pre-flight (e.g. their
// auth state changed between REST and consensus, or a future REST bug),
// a non-zero ABCI tx_result code must still produce a non-2xx response.
// Pre-v6.6.9 broadcast_tx_sync would have returned 200 + tx_hash here —
// the silent failure we're closing.
func TestSetPermission_FinalizeBlockReject_Surfaces403(t *testing.T) {
	// Simulate the chain rejecting the tx with the "access denied" log
	// the new ABCI handler emits. broadcastErrorStatus maps that to 403.
	cometMock := permTestCometMock(t, 67, "access denied: caller cannot set permissions on target")
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	// Run with badgerStore=nil so the REST pre-flight doesn't fire — we
	// need the request to actually reach the broadcast step to verify the
	// error-mapping path. (In production both gates run; this test
	// isolates the second gate.)
	srv.badgerStore = nil

	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	agentID := auth.PublicKeyToAgentID(pub)

	req := signedRequestAs(t, priv, agentID, http.MethodPut, "/v1/agent/"+agentID+"/permission", setPermBody)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusForbidden, rr.Code, "FinalizeBlock 'access denied' must surface as 403; body: %s", rr.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	_, hasTxHash := resp["tx_hash"]
	assert.False(t, hasTxHash, "rejected tx must NOT return a tx_hash to the client")
}
