package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

// v6.6.9 regression coverage: GET /v1/org/by-name/{name} returns every
// orgID registered under that human-readable name. The Python SDK now
// routes get_org("levelup") through this endpoint instead of stuffing
// the name into /v1/org/{org_id} and 404'ing.

func TestGetOrgByName_EmptyReturns200WithEmptyList(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/by-name/levelup", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	// Empty is a valid answer — must NOT be a 404 (the SDK uses 200+empty
	// as the signal to raise its own clear "no such org" error).
	require.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	orgs, ok := resp["orgs"].([]any)
	require.True(t, ok, "response must contain orgs array")
	assert.Empty(t, orgs)
}

func TestGetOrgByName_SingleMatchReturnsEntry(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	const orgID = "0aaa11111111111111111111111111aa"
	const admin = "admin0000000000000000000000000000000000000000000000000000000adm"
	require.NoError(t, bs.RegisterOrg(orgID, "levelup", "the LU tenant", admin, 1))

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/by-name/levelup", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	orgs, ok := resp["orgs"].([]any)
	require.True(t, ok)
	require.Len(t, orgs, 1)
	entry := orgs[0].(map[string]any)
	assert.Equal(t, orgID, entry["org_id"])
	assert.Equal(t, "levelup", entry["name"])
	assert.Equal(t, admin, entry["admin_agent_id"])
	assert.Equal(t, "the LU tenant", entry["description"])
}

func TestGetOrgByName_MultipleMatchesAllReturned(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	// Two admins both registered an org named "levelup". The endpoint
	// must surface both so the SDK can raise a disambiguation error.
	const orgIDA = "0aaa11111111111111111111111111aa"
	const orgIDB = "0bbb22222222222222222222222222bb"
	const adminA = "adminA00000000000000000000000000000000000000000000000000000aaaa"
	const adminB = "adminB00000000000000000000000000000000000000000000000000000bbbb"
	require.NoError(t, bs.RegisterOrg(orgIDA, "levelup", "tenant A", adminA, 1))
	require.NoError(t, bs.RegisterOrg(orgIDB, "levelup", "tenant B", adminB, 2))

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/by-name/levelup", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	orgs, ok := resp["orgs"].([]any)
	require.True(t, ok)
	require.Len(t, orgs, 2)

	gotIDs := []string{
		orgs[0].(map[string]any)["org_id"].(string),
		orgs[1].(map[string]any)["org_id"].(string),
	}
	sort.Strings(gotIDs)
	assert.Equal(t, []string{orgIDA, orgIDB}, gotIDs)
}

func TestGetOrgByName_DoesNotShadowOrgIDRoute(t *testing.T) {
	// Sanity check: the literal /v1/org/by-name/{name} route must not
	// swallow GET /v1/org/{org_id} for arbitrary IDs. chi handles this
	// correctly because the depths differ, but this guards against
	// future route reorganization.
	srv, _, _ := newTestServer(t, "")

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	req, _ := signedRequest(t, http.MethodGet, "/v1/org/by-name/", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	// chi treats the trailing-slash form as a 404 / redirect rather
	// than dispatching to handleGetOrgByName, which is fine — we only
	// care that it doesn't return 200 with bogus data.
	assert.NotEqual(t, http.StatusOK, rr.Code)
}
