package rest

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

const mediaTestPath = "/v1/private-media/" + workflowTestID

func mediaTestJPEG() []byte {
	return append([]byte{255, 216, 255, 192, 0, 11, 8, 2, 208, 5, 0, 1, 1, 17, 0, 255, 218, 0, 8, 1, 1, 0, 0, 63, 0}, []byte("synthetic-media-canary\xff\xd9")...)
}

func newMediaRESTFixture(t *testing.T) workflowTestFixture {
	fixture := newWorkflowTestFixture(t)
	media, err := store.NewPrivateMediaStore(fixture.sqlite, store.PrivateMediaQuota{
		Enabled: true, ActorBytes: 8 << 20, NodeBytes: 16 << 20, ActorObjects: 10, NodeObjects: 20, MinFreeBytes: 512 << 20,
	}, func(context.Context) (int64, error) { return 1 << 30, nil })
	require.NoError(t, err)
	fixture.server.SetPrivateMediaStore(media)
	return fixture
}

func mediaRESTRequest(t *testing.T, fixture workflowTestFixture, actor, method, path string, body []byte, mutate func(*http.Request)) *httptest.ResponseRecorder {
	t.Helper()
	key := fixture.keys[actor]
	request := signedGovernanceRequestAs(t, key, appV23LookupVaultID(key), method, path, body)
	request.Header.Set("Content-Type", "image/jpeg")
	if mutate != nil {
		mutate(request)
	}
	response := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(response, request)
	if strings.HasPrefix(path, "/v1/private-media/") {
		require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
		require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	}
	return response
}

func TestPrivateMediaRESTImmutableOriginalAndActorIsolation(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	image := mediaTestJPEG()
	created := mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, image, nil)
	require.Equal(t, 200, created.Code, created.Body.String())
	var record map[string]any
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &record))
	digest := sha256.Sum256(image)
	require.Equal(t, map[string]any{"schema": "sage.private-media.v1", "agent_id": appV23LookupVaultID(fixture.keys["first"]), "object_id": workflowTestID, "revision": float64(1), "length": float64(len(image)), "digest": hex.EncodeToString(digest[:])}, record)
	replay := mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, image, nil)
	require.Equal(t, 200, replay.Code)
	require.Equal(t, created.Body.String(), replay.Body.String())
	changed := bytes.Replace(image, []byte("canary"), []byte("change"), 1)
	require.Equal(t, 409, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, changed, nil).Code)
	read := mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil)
	require.Equal(t, 200, read.Code)
	require.Equal(t, "image/jpeg", read.Header().Get("Content-Type"))
	require.Equal(t, "sage.private-media.v1", read.Header().Get("X-SAGE-Private-Media-Schema"))
	require.Equal(t, appV23LookupVaultID(fixture.keys["first"]), read.Header().Get("X-SAGE-Agent-ID"))
	require.Equal(t, workflowTestID, read.Header().Get("X-SAGE-Media-ID"))
	require.Equal(t, hex.EncodeToString(digest[:]), read.Header().Get("X-SAGE-Media-SHA256"))
	require.Equal(t, image, read.Body.Bytes())
	require.Equal(t, 404, mediaRESTRequest(t, fixture, "second", "GET", mediaTestPath, nil, nil).Code)
	require.Equal(t, 200, mediaRESTRequest(t, fixture, "second", "PUT", mediaTestPath, changed, nil).Code)
	require.Equal(t, image, mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil).Body.Bytes())
}

func TestPrivateMediaRESTStrictInput(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	for _, path := range []string{"/v1/private-media/not-a-uuid", "/v1/private-media/00000000-0000-0000-0000-000000000000", strings.ToUpper(mediaTestPath[len("/v1/private-media/"):]), mediaTestPath + "?actor=x"} {
		if !strings.HasPrefix(path, "/") {
			path = "/v1/private-media/" + path
		}
		require.Equal(t, 400, mediaRESTRequest(t, fixture, "first", "PUT", path, mediaTestJPEG(), nil).Code)
	}
	for _, contentType := range []string{"", "application/json", "image/jpeg; charset=utf-8", "image/png"} {
		require.Equal(t, 415, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), func(r *http.Request) { r.Header.Set("Content-Type", contentType) }).Code)
	}
	require.Equal(t, 415, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), func(r *http.Request) { r.Header.Add("Content-Type", "image/jpeg") }).Code)
	require.Equal(t, 415, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }).Code)
	for _, body := range [][]byte{nil, []byte("not jpeg"), {255, 216, 255, 217}} {
		require.Equal(t, 400, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, body, nil).Code)
	}
	require.Equal(t, 400, mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, []byte("body"), nil).Code)
}

func TestPrivateMediaRESTExactRawSignatureAndOrdinaryBoundary(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	for _, actor := range []string{"root", "pending"} {
		for _, method := range []string{"GET", "PUT"} {
			require.Contains(t, []int{401, 403}, mediaRESTRequest(t, fixture, actor, method, mediaTestPath, mediaTestJPEG(), nil).Code)
		}
	}
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Header.Del("X-Signature") },
		func(r *http.Request) {
			r.Body = io.NopCloser(bytes.NewReader(bytes.Replace(mediaTestJPEG(), []byte("canary"), []byte("forged"), 1)))
		},
	} {
		require.Equal(t, 401, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), mutate).Code)
	}
	key := fixture.keys["first"]
	request := signedRequestAs(t, key, appV23LookupVaultID(key), "PUT", mediaTestPath, mediaTestJPEG())
	request.Header.Set("Content-Type", "image/jpeg")
	response := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(response, request)
	require.Equal(t, 403, response.Code)
	require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	fixture.server.SetPostV23ForNextTxAccessor(func() bool { return false })
	require.Equal(t, 503, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), nil).Code)
}

func TestPrivateMediaRESTExactRouteBodyLimit(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	image := mediaTestJPEG()
	image = append(image[:len(image)-2], bytes.Repeat([]byte{'x'}, store.MaxPrivateJPEGBytes-len(image))...)
	image = append(image, 255, 217)
	require.Len(t, image, store.MaxPrivateJPEGBytes)
	require.Equal(t, 200, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, image, nil).Code)
	require.Equal(t, image, mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil).Body.Bytes())
	require.Equal(t, 413, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, append(image, 'x'), nil).Code)
	for _, route := range []struct{ method, path string }{
		{"GET", mediaTestPath}, {"POST", mediaTestPath}, {"PUT", workflowTestPath},
		{"PUT", mediaTestPath + "/extra"}, {"PUT", mediaTestPath + "?extra=1"},
		{"PUT", "/v1/private-media/invalid"}, {"PUT", strings.Replace(mediaTestPath, "e837", "%65837", 1)},
	} {
		key := fixture.keys["first"]
		request := signedGovernanceRequestAs(t, key, appV23LookupVaultID(key), route.method, route.path, image)
		response := httptest.NewRecorder()
		middleware.Ed25519AuthMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			t.Error("oversized request reached handler")
		})).ServeHTTP(response, request)
		require.Equal(t, 413, response.Code, route.path)
	}
}

func TestPrivateMediaRESTUnavailableCorruptAndQuota(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	require.Equal(t, 404, mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil).Code)
	require.Equal(t, 200, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), nil).Code)
	database, err := sql.Open("sqlite", fixture.databasePath)
	require.NoError(t, err)
	defer database.Close()
	_, err = database.Exec(`UPDATE private_jpeg_objects SET sealed_record = x'000000'`)
	require.NoError(t, err)
	for _, method := range []string{"GET", "PUT"} {
		var body []byte
		if method == "PUT" {
			body = mediaTestJPEG()
		}
		response := mediaRESTRequest(t, fixture, "first", method, mediaTestPath, body, nil)
		require.Equal(t, 503, response.Code)
		require.NotContains(t, response.Body.String(), "canary")
	}
	quota, err := store.NewPrivateMediaStore(fixture.sqlite, store.PrivateMediaQuota{Enabled: true, ActorBytes: 1, NodeBytes: 1, ActorObjects: 1, NodeObjects: 1}, func(context.Context) (int64, error) { return 1 << 30, nil })
	require.NoError(t, err)
	fixture.server.SetPrivateMediaStore(quota)
	require.Equal(t, 429, mediaRESTRequest(t, fixture, "second", "PUT", mediaTestPath, mediaTestJPEG(), nil).Code)
	fixture.sqlite.SetVault(nil)
	require.Equal(t, 503, mediaRESTRequest(t, fixture, "second", "GET", mediaTestPath, nil, nil).Code)
	require.Equal(t, 503, mediaRESTRequest(t, fixture, "second", "PUT", mediaTestPath, mediaTestJPEG(), nil).Code)
	fixture.server.SetPrivateMediaStore(nil)
	require.Equal(t, 503, mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil).Code)
}

func TestPrivateMediaRESTDefaultDisabled(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	for _, method := range []string{"GET", "PUT"} {
		require.Equal(t, 503, mediaRESTRequest(t, fixture, "first", method, mediaTestPath, mediaTestJPEG(), nil).Code)
	}
	disabled, err := store.NewPrivateMediaStore(fixture.sqlite, store.PrivateMediaQuota{}, nil)
	require.NoError(t, err)
	fixture.server.SetPrivateMediaStore(disabled)
	require.Equal(t, 503, mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil).Code)
	require.Equal(t, 503, mediaRESTRequest(t, fixture, "first", "PUT", mediaTestPath, mediaTestJPEG(), nil).Code)
}

func TestPrivateMediaRESTMissingObjectSchemaMarker(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	missing := mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil)
	require.Equal(t, 404, missing.Code)
	require.Equal(t, "sage.private-media.v1", missing.Header().Get("X-SAGE-Private-Media-Schema"))
	unknown := mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath+"/unknown", nil, nil)
	require.Equal(t, 404, unknown.Code)
	require.Empty(t, unknown.Header().Get("X-SAGE-Private-Media-Schema"))
	fixture.server.SetPrivateMediaStore(nil)
	unavailable := mediaRESTRequest(t, fixture, "first", "GET", mediaTestPath, nil, nil)
	require.Equal(t, 503, unavailable.Code)
	require.Empty(t, unavailable.Header().Get("X-SAGE-Private-Media-Schema"))
}

func TestPrivateMediaRESTNonceReplayDenied(t *testing.T) {
	fixture := newMediaRESTFixture(t)
	key := fixture.keys["first"]
	request := signedGovernanceRequestAs(t, key, appV23LookupVaultID(key), "PUT", mediaTestPath, mediaTestJPEG())
	request.Header.Set("Content-Type", "image/jpeg")
	for _, expected := range []int{200, 401} {
		request.Body = io.NopCloser(bytes.NewReader(mediaTestJPEG()))
		response := httptest.NewRecorder()
		fixture.server.Router().ServeHTTP(response, request)
		require.Equal(t, expected, response.Code)
		require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
		require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
	}
}
