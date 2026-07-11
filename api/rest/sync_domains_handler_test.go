package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

// syncOperatorID is the node operator identity for the sync-consent tests.
const syncOperatorID = "0000000000000000000000000000000000000000000000000000000000000002"

// newSyncServer wires a real SQLite store + BadgerStore + Server for the
// sync-consent CRUD tests (same shape as newTokenServer).
func newSyncServer(t *testing.T) (*Server, *store.SQLiteStore, *store.BadgerStore) {
	t.Helper()
	dir := t.TempDir()
	memStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(dir, "sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = memStore.Close() })

	bs, err := store.NewBadgerStore(filepath.Join(dir, "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })

	s := &Server{
		store:          memStore,
		badgerStore:    bs,
		logger:         zerolog.Nop(),
		nodeOperatorID: syncOperatorID,
	}
	return s, memStore, bs
}

// syncRouterAs mounts the sync routes with callerID injected as the
// authenticated agent identity.
func syncRouterAs(s *Server, callerID string) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			next.ServeHTTP(w, req.WithContext(middleware.WithAgentID(req.Context(), callerID)))
		})
	})
	r.Put("/v1/federation/cross/{chain_id}/sync", s.handleSyncDomainsSet)
	r.Get("/v1/federation/cross/{chain_id}/sync", s.handleSyncDomainsGet)
	return r
}

func putSync(t *testing.T, h http.Handler, chainID string, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPut, "/v1/federation/cross/"+chainID+"/sync", bytes.NewReader([]byte(body)))
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func getSync(t *testing.T, h http.Handler, chainID string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/v1/federation/cross/"+chainID+"/sync", nil)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	return rr
}

func seedAgreement(t *testing.T, bs *store.BadgerStore, chainID string, domains []string, status string) {
	t.Helper()
	require.NoError(t, bs.SetCrossFed(chainID, "https://peer:8444", []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa"), 2, 0, domains, []string{"*"}, status))
}

func TestSyncDomainsSetGetRoundTrip(t *testing.T) {
	s, _, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	seedAgreement(t, bs, "chain-b", []string{"hr", "eng"}, "active")

	// Subtree coverage: "hr.public" is allowed under "hr".
	rr := putSync(t, h, "chain-b", `{"domains":["hr.public","eng"]}`)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp struct {
		RemoteChainID string   `json:"remote_chain_id"`
		SyncDomains   []string `json:"sync_domains"`
	}
	require.NoError(t, json.NewDecoder(getSync(t, h, "chain-b").Body).Decode(&resp))
	assert.Equal(t, []string{"eng", "hr.public"}, resp.SyncDomains)

	// Replace-all: a second PUT overwrites, not merges.
	rr = putSync(t, h, "chain-b", `{"domains":["hr"]}`)
	require.Equal(t, http.StatusOK, rr.Code)
	require.NoError(t, json.NewDecoder(getSync(t, h, "chain-b").Body).Decode(&resp))
	assert.Equal(t, []string{"hr"}, resp.SyncDomains)

	// Unconfigured peer reads back as an empty set, not an error.
	seedAgreement(t, bs, "chain-c", []string{"*"}, "active")
	require.NoError(t, json.NewDecoder(getSync(t, h, "chain-c").Body).Decode(&resp))
	assert.Empty(t, resp.SyncDomains)
}

func TestSyncDomainsValidation(t *testing.T) {
	s, _, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	seedAgreement(t, bs, "chain-b", []string{"hr"}, "active")

	// No agreement -> 404.
	assert.Equal(t, http.StatusNotFound, putSync(t, h, "chain-x", `{"domains":["hr"]}`).Code)

	// Wildcard rejected (concrete consent only).
	assert.Equal(t, http.StatusBadRequest, putSync(t, h, "chain-b", `{"domains":["*"]}`).Code)

	// Empty domain rejected.
	assert.Equal(t, http.StatusBadRequest, putSync(t, h, "chain-b", `{"domains":[""]}`).Code)

	// Outside the agreement's AllowedDomains -> 400 (consent can only narrow).
	assert.Equal(t, http.StatusBadRequest, putSync(t, h, "chain-b", `{"domains":["finance"]}`).Code)

	// Path-hostile chain id rejected before any store access.
	assert.Equal(t, http.StatusBadRequest, putSync(t, h, "..", `{"domains":["hr"]}`).Code)

	// Inactive agreement -> 409.
	seedAgreement(t, bs, "chain-r", []string{"hr"}, "revoked")
	assert.Equal(t, http.StatusConflict, putSync(t, h, "chain-r", `{"domains":["hr"]}`).Code)

	// Non-operator caller -> 403 on both verbs.
	asAgent := syncRouterAs(s, "deadbeef"+syncOperatorID[8:])
	assert.Equal(t, http.StatusForbidden, putSync(t, asAgent, "chain-b", `{"domains":["hr"]}`).Code)
	assert.Equal(t, http.StatusForbidden, getSync(t, asAgent, "chain-b").Code)
}

func TestSyncDomainsWildcardAgreementAllowsConcrete(t *testing.T) {
	s, _, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	// A wildcard AGREEMENT still requires CONCRETE sync consent — the treaty
	// may be broad, but replication consent is explicit per subtree.
	seedAgreement(t, bs, "chain-w", []string{"*"}, "active")

	require.Equal(t, http.StatusOK, putSync(t, h, "chain-w", `{"domains":["ops.runbooks"]}`).Code)
	assert.Equal(t, http.StatusBadRequest, putSync(t, h, "chain-w", `{"domains":["*"]}`).Code)
}

func TestSyncDomainsGuestCannotReplaceHostPolicy(t *testing.T) {
	s, ms, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	seedAgreement(t, bs, "chain-host", []string{"shared"}, "active")
	require.NoError(t, ms.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-host", Role: "guest", ControllerChainID: "chain-host",
		ControllerAgentID: "host-agent", PolicyEpoch: "epoch", RemoteCAPin: "pin",
	}))
	require.NoError(t, ms.ActivateSyncControl(context.Background(), "chain-host", "epoch"))

	rr := putSync(t, h, "chain-host", `{"domains":["shared"]}`)
	assert.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	domains, err := ms.GetSyncDomains(context.Background(), "chain-host")
	require.NoError(t, err)
	assert.Empty(t, domains)
}

func TestSyncDomainsMutationFailsClosedWhenControllerReadFails(t *testing.T) {
	s, ms, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	seedAgreement(t, bs, "chain-b", []string{"shared"}, "active")
	require.NoError(t, ms.Close())

	rr := putSync(t, h, "chain-b", `{"domains":["shared"]}`)
	assert.Equal(t, http.StatusInternalServerError, rr.Code, rr.Body.String())
}

func TestSyncPurgeOnRestRevoke(t *testing.T) {
	ctx := context.Background()
	s, ms, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	seedAgreement(t, bs, "chain-b", []string{"hr"}, "active")

	require.Equal(t, http.StatusOK, putSync(t, h, "chain-b", `{"domains":["hr"]}`).Code)
	_, err := ms.EnqueueSyncOutbox(ctx, "chain-b", "mem-1")
	require.NoError(t, err)

	// Exercise the purge block directly (the full revoke handler needs a live
	// CometBFT broadcast; the purge is the off-consensus half under test).
	require.NoError(t, ms.DeleteSyncDomains(ctx, "chain-b"))
	require.NoError(t, ms.PurgeSyncOutbox(ctx, "chain-b"))

	domains, err := ms.GetSyncDomains(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, domains)
	counts, err := ms.CountSyncOutboxByState(ctx, "chain-b")
	require.NoError(t, err)
	assert.Empty(t, counts)
}
