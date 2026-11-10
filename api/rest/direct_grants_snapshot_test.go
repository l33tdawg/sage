package rest

import (
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

func setDirectGrantsRESTState(t *testing.T, f appV23RESTRouteFixture) {
	t.Helper()
	height := make([]byte, 8)
	binary.BigEndian.PutUint64(height, 17)
	require.NoError(t, f.badger.SetStatesAtomic([]store.StateWrite{
		{Key: "height", Value: height},
		{Key: "app_hash", Value: []byte(strings.Repeat("a", 32))},
	}))
}

func serveDirectGrantsREST(
	t *testing.T, f appV23RESTRouteFixture, actor, path string, local bool,
) *httptest.ResponseRecorder {
	t.Helper()
	req := appV23SignedRESTRouteRequest(t, f, actor, http.MethodGet, path, nil, local)
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	ts, err := strconv.ParseInt(req.Header.Get("X-Timestamp"), 10, 64)
	require.NoError(t, err)
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(
		auth.SignRequestWithNonce(f.keys[actor], http.MethodGet, req.URL.RequestURI(), nil, ts, nonce),
	))
	rec := httptest.NewRecorder()
	f.server.Router().ServeHTTP(rec, req)
	return rec
}

func TestDirectGrantsSnapshotCanonicalWithoutMirror(t *testing.T) {
	f := newAppV23RESTRouteFixture(t)
	setDirectGrantsRESTState(t, f)
	member := f.ids["member"]
	require.NoError(t, f.badger.SetAccessGrant("member.home", member, 1, 0, f.ids["current-root"]))

	rec := serveDirectGrantsREST(t, f, "member", "/v1/access/grants-snapshot/"+member, true)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, "private, no-store", rec.Header().Get("Cache-Control"))

	var got store.DirectGrantsSnapshot
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.True(t, got.Complete)
	require.Equal(t, uint64(17), got.Height)
	require.Equal(t, strings.Repeat("61", 32), got.AppHash)
	require.Equal(t, member, got.TargetAgentID)
	require.Equal(t, f.ids["historical-root"], got.RootPrincipalID)
	require.Equal(t, uint64(2), got.RootGeneration)
	require.Len(t, got.Grants, 1)
	require.Equal(t, "member.home", got.Grants[0].Domain)
	require.Equal(t, uint8(1), got.Grants[0].AccessLevel)
}

func TestDirectGrantsSnapshotExistingSelfAdminAndRootBoundary(t *testing.T) {
	f := newAppV23RESTRouteFixture(t)
	setDirectGrantsRESTState(t, f)
	manager := f.ids["manager"]
	require.NoError(t, f.badger.SetAccessGrant("manager.home", manager, 2, 0, f.ids["current-root"]))

	memberCross := serveDirectGrantsREST(t, f, "member", "/v1/access/grants-snapshot/"+manager, true)
	require.Equal(t, http.StatusForbidden, memberCross.Code, memberCross.Body.String())

	adminLocal := serveDirectGrantsREST(t, f, "current-admin", "/v1/access/grants-snapshot/"+manager, true)
	require.Equal(t, http.StatusOK, adminLocal.Code, adminLocal.Body.String())

	adminRemote := serveDirectGrantsREST(t, f, "current-admin", "/v1/access/grants-snapshot/"+manager, false)
	require.Equal(t, http.StatusForbidden, adminRemote.Code, adminRemote.Body.String())

	root := serveDirectGrantsREST(t, f, "current-root", "/v1/access/grants-snapshot/"+manager, true)
	require.Equal(t, http.StatusForbidden, root.Code, root.Body.String())

	staleAdmin := serveDirectGrantsREST(
		t, f, "stale-admin", "/v1/access/grants-snapshot/"+manager, true,
	)
	require.Equal(t, http.StatusForbidden, staleAdmin.Code, staleAdmin.Body.String())

	inactive := serveDirectGrantsREST(
		t, f, "inactive", "/v1/access/grants-snapshot/"+f.ids["inactive"], true,
	)
	require.Equal(t, http.StatusForbidden, inactive.Code, inactive.Body.String())
}

func TestListGrantsUsesCanonicalValuesInsteadOfMirror(t *testing.T) {
	f := newAppV23RESTRouteFixture(t)
	setDirectGrantsRESTState(t, f)
	target := f.ids["manager"]
	chainGranter := f.ids["current-root"]
	mirrorGranter := f.ids["historical-root"]
	require.NoError(t, f.badger.SetAccessGrant("manager.home", target, 1, 0, chainGranter))
	f.server.accessStore = &staleMirrorAccessStore{
		grants: []*store.AccessGrantEntry{{
			Domain: "manager.home", GranteeID: target,
			GranterID: mirrorGranter, Level: 3,
			CreatedHeight: 9, CreatedAt: time.Unix(9, 0).UTC(),
		}},
	}

	rec := serveDirectGrantsREST(t, f, "current-admin", "/v1/access/grants/"+target, true)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got []store.AccessGrantEntry
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 1)
	require.Equal(t, uint8(1), got[0].Level)
	require.Equal(t, chainGranter, got[0].GranterID)
	require.Equal(t, int64(9), got[0].CreatedHeight, "matching mirror metadata remains backward compatible")

	require.NoError(t, f.badger.DeleteAccessGrant("manager.home", target))
	rec = serveDirectGrantsREST(t, f, "current-admin", "/v1/access/grants/"+target, true)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Empty(t, got, "canonical revoke must immediately remove the mirror row")
}

func TestListGrantsCanonicalRowWorksWithoutMirror(t *testing.T) {
	f := newAppV23RESTRouteFixture(t)
	setDirectGrantsRESTState(t, f)
	target := f.ids["manager"]
	require.NoError(t, f.badger.SetAccessGrant("manager.home", target, 2, 0, f.ids["current-root"]))
	f.server.accessStore = nil

	rec := serveDirectGrantsREST(t, f, "current-admin", "/v1/access/grants/"+target, true)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var got []store.AccessGrantEntry
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	require.Len(t, got, 1)
	require.Equal(t, uint8(2), got[0].Level)
	require.Equal(t, target, got[0].GranteeID)
}

func TestDirectGrantsSnapshotRejectsPreV23QueryAndMalformedTarget(t *testing.T) {
	preV23, _, _, _ := newRBACTestServer(t)
	preV23.SetPostV23ForNextTxAccessor(func() bool { return false })
	req, _ := signedRequest(t, http.MethodGet, "/v1/access/grants-snapshot/legacy-target", nil)
	rec := httptest.NewRecorder()
	preV23.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())

	f := newAppV23RESTRouteFixture(t)
	setDirectGrantsRESTState(t, f)
	query := serveDirectGrantsREST(
		t, f, "member", "/v1/access/grants-snapshot/"+f.ids["member"]+"?grants=[]", true,
	)
	require.Equal(t, http.StatusBadRequest, query.Code, query.Body.String())
	malformed := serveDirectGrantsREST(t, f, "member", "/v1/access/grants-snapshot/not-an-agent", true)
	require.Equal(t, http.StatusBadRequest, malformed.Code, malformed.Body.String())
}
