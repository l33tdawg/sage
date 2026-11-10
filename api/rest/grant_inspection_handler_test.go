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

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

func grantInspectionRESTFixture(t *testing.T) appV23RESTRouteFixture {
	t.Helper()
	f := newAppV23RESTRouteFixture(t)
	require.NoError(t, f.badger.MarkUpgradeApplied("app-v30", 30, 5))
	require.NoError(t, f.badger.MarkUpgradeApplied("app-v31", 31, 6))
	h := make([]byte, 8)
	binary.BigEndian.PutUint64(h, 10)
	require.NoError(t, f.badger.SetStatesAtomic([]store.StateWrite{{Key: "height", Value: h}, {Key: "app_hash", Value: []byte(strings.Repeat("h", 32))}, {Key: consensuskeys.GovernanceDelegationDomainV20, Value: []byte(strings.Repeat("d", 32))}}))
	setInspectorPolicy(t, f, true)
	return f
}
func setInspectorPolicy(t *testing.T, f appV23RESTRouteFixture, enabled bool) {
	t.Helper()
	en, err := f.badger.GetAppV23Enrollment(f.ids["member"])
	require.NoError(t, err)
	role, err := f.badger.GetAppV23Role(f.ids["member"])
	require.NoError(t, err)
	next := *en
	root, err := f.badger.GetAppV23Root()
	require.NoError(t, err)
	next.RootGeneration = root.Generation
	next.ApprovedBy = root.CredentialID
	next.UpdatedHeight = 7
	next.Capabilities = 0
	if enabled {
		next.Capabilities = store.AgentCapabilityInspectDirectGrants
	}
	require.NoError(t, f.badger.ApproveAppV31LocalAgent(next, store.AppV23RoleMember, en.Revision, role.Revision))
}
func inspectREST(t *testing.T, f appV23RESTRouteFixture, actor, target string, local bool) *httptest.ResponseRecorder {
	t.Helper()
	req := appV23SignedRESTRouteRequest(t, f, actor, http.MethodGet, "/v1/access/grants-snapshot/"+target, nil, local)
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	ts, err := strconv.ParseInt(req.Header.Get("X-Timestamp"), 10, 64)
	require.NoError(t, err)
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(f.keys[actor], http.MethodGet, req.URL.RequestURI(), nil, ts, nonce)))
	out := httptest.NewRecorder()
	f.server.Router().ServeHTTP(out, req)
	return out
}
func TestDirectGrantSnapshotSignedCapabilityCanonicalRows(t *testing.T) {
	f := grantInspectionRESTFixture(t)
	target := f.ids["manager"]
	require.NoError(t, f.badger.SetAccessGrant("manager.home", target, 1, 0, f.ids["current-root"]))
	out := inspectREST(t, f, "member", target, false)
	require.Equal(t, 200, out.Code, out.Body.String())
	require.Equal(t, "private, no-store", out.Header().Get("Cache-Control"))
	var result store.DirectGrantsSnapshot
	require.NoError(t, json.Unmarshal(out.Body.Bytes(), &result))
	require.True(t, result.Complete)
	require.Equal(t, "direct-grants-for-target", result.Coverage)
	require.Equal(t, f.ids["member"], result.CallerID)
	require.Equal(t, target, result.TargetAgentID)
	require.Equal(t, uint64(10), result.Height)
	require.Equal(t, strings.Repeat("68", 32), result.AppHash)
	require.Len(t, result.Grants, 1)
	require.Equal(t, uint8(1), result.Grants[0].AccessLevel)
	// No grant mirror entry was inserted. This endpoint sees canonical rows anyway.
	setInspectorPolicy(t, f, false)
	denied := inspectREST(t, f, "member", target, false)
	require.Equal(t, 403, denied.Code)
}
func TestDirectGrantSnapshotNoAdminRootOrSelfBypass(t *testing.T) {
	f := grantInspectionRESTFixture(t)
	for _, actor := range []string{"manager", "current-admin", "stale-admin", "current-root", "historical-root", "inactive"} {
		out := inspectREST(t, f, actor, f.ids["manager"], true)
		require.Equal(t, 403, out.Code, actor+out.Body.String())
	}
	for _, target := range []string{"current-root", "historical-root"} {
		out := inspectREST(t, f, "member", f.ids[target], true)
		require.Equal(t, 403, out.Code, out.Body.String())
	}
	setInspectorPolicy(t, f, false)
	require.Equal(t, 403, inspectREST(t, f, "member", f.ids["member"], true).Code)
}
func TestDirectGrantSnapshotRefusesUncommittedActivationAndMalformedInputs(t *testing.T) {
	f := grantInspectionRESTFixture(t)
	require.NoError(t, f.badger.MarkUpgradeApplied("app-v31", 31, 11))
	require.Equal(t, 503, inspectREST(t, f, "member", f.ids["manager"], true).Code)
	require.Equal(t, 400, inspectREST(t, f, "member", "bad-id", true).Code)
	require.Equal(t, 400, inspectREST(t, f, "member", f.ids["manager"]+"?grants=[]", true).Code)
}
func TestDirectGrantSnapshotRootRotationRevokesInspection(t *testing.T) {
	f := grantInspectionRESTFixture(t)
	require.NoError(t, f.badger.RotateAppV23RootCredential(2, strings.Repeat("e", 64), 11))
	require.Equal(t, 403, inspectREST(t, f, "member", f.ids["manager"], true).Code)
	setInspectorPolicy(t, f, true)
	require.Equal(t, 200, inspectREST(t, f, "member", f.ids["manager"], true).Code)
}
func TestDirectGrantSnapshotOldAPIStillPermitsAdminRead(t *testing.T) {
	f := grantInspectionRESTFixture(t)
	f.server.accessStore = &staleMirrorAccessStore{grants: []*store.AccessGrantEntry{}}
	req := appV23SignedRESTRouteRequest(t, f, "current-admin", http.MethodGet, "/v1/access/grants/"+f.ids["manager"], nil, true)
	out := httptest.NewRecorder()
	f.server.Router().ServeHTTP(out, req)
	require.Equal(t, 200, out.Code, out.Body.String())
}

func TestDirectGrantSnapshotPreForkAndAnonymousDenied(t *testing.T) {
	f := newAppV23RESTRouteFixture(t)
	require.Equal(t, 403, inspectREST(t, f, "member", f.ids["member"], true).Code)
	out := httptest.NewRecorder()
	f.server.Router().ServeHTTP(out, httptest.NewRequest(http.MethodGet, "/v1/access/grants-snapshot/"+f.ids["member"], nil))
	require.Equal(t, 401, out.Code)
}
func TestDirectGrantSnapshotDoesNotWidenLegacyGrantRead(t *testing.T) {
	f := grantInspectionRESTFixture(t)
	req := appV23SignedRESTRouteRequest(t, f, "member", http.MethodGet, "/v1/access/grants/"+f.ids["manager"], nil, true)
	out := httptest.NewRecorder()
	f.server.Router().ServeHTTP(out, req)
	require.Equal(t, 403, out.Code, out.Body.String())
}
