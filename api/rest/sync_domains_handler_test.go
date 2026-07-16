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
	"github.com/l33tdawg/sage/internal/federation"
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

type syncDirectionalFederation struct {
	*fakeFederation
	store          *store.SQLiteStore
	calls          int
	lastChain      string
	lastPublish    []string
	lastSubscribe  []string
	directionalErr error
}

func (f *syncDirectionalFederation) SetDirectionalSyncPolicy(ctx context.Context, chain string, publish, subscribe []string) (*federation.DirectionalSyncPolicyResult, error) {
	f.calls++
	f.lastChain = chain
	f.lastPublish = append([]string{}, publish...)
	f.lastSubscribe = append([]string{}, subscribe...)
	if f.directionalErr != nil {
		return nil, f.directionalErr
	}
	control, err := f.store.GetSyncControl(ctx, chain)
	if err != nil {
		return nil, err
	}
	revision := control.Revision + 1
	if _, err = f.store.ApplyLocalDirectionalSyncPolicy(ctx, chain, control.PolicyEpoch,
		federation.SyncPolicyVersionPeerRBAC, revision, "rest-directional-test", publish, subscribe); err != nil {
		return nil, err
	}
	return &federation.DirectionalSyncPolicyResult{
		Version:          federation.SyncPolicyVersionPeerRBAC,
		Revision:         revision,
		PublishDomains:   append([]string{}, publish...),
		SubscribeDomains: append([]string{}, subscribe...),
		State:            "pending",
	}, nil
}

func seedActiveV3SyncControl(t *testing.T, ss *store.SQLiteStore, chainID, role string, publish, subscribe, remotePublish, remoteSubscribe []string) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: chainID, Role: role, ControllerChainID: "controller-chain",
		ControllerAgentID: "controller-agent", PeerAgentID: "peer-agent", PolicyEpoch: "epoch-v3",
		RemoteCAPin: "pin", PolicyVersion: federation.SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, ss.ActivateSyncControl(ctx, chainID, "epoch-v3"))
	_, err := ss.ApplyLocalDirectionalSyncPolicy(ctx, chainID, "epoch-v3",
		federation.SyncPolicyVersionPeerRBAC, 1, "local-v3", publish, subscribe)
	require.NoError(t, err)
	_, err = ss.ApplyRemoteDirectionalSyncPolicy(ctx, chainID, "epoch-v3",
		federation.SyncPolicyVersionPeerRBAC, 4, "remote-v3", remotePublish, remoteSubscribe)
	require.NoError(t, err)
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

func TestSyncDomainsV3BothRolesAuthorIndependentLanes(t *testing.T) {
	tests := []struct {
		name          string
		role          string
		body          string
		wantPublish   []string
		wantSubscribe []string
	}{
		{
			name: "host clears only subscribe", role: "host", body: `{"subscribe_domains":[]}`,
			wantPublish: []string{"local.publish"}, wantSubscribe: []string{},
		},
		{
			name: "guest replaces only publish", role: "guest", body: `{"publish_domains":["new.publish"]}`,
			wantPublish: []string{"new.publish"}, wantSubscribe: []string{"local.subscribe"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s, ss, bs := newSyncServer(t)
			seedAgreement(t, bs, "chain-v3", nil, "active")
			seedActiveV3SyncControl(t, ss, "chain-v3", tt.role,
				[]string{"local.publish"}, []string{"local.subscribe"},
				[]string{"remote.publish"}, []string{"remote.subscribe"})
			driver := &syncDirectionalFederation{fakeFederation: &fakeFederation{}, store: ss}
			s.SetFederation(driver)
			h := syncRouterAs(s, syncOperatorID)

			rr := putSync(t, h, "chain-v3", tt.body)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			assert.Equal(t, 1, driver.calls)
			assert.Equal(t, "chain-v3", driver.lastChain)
			assert.Equal(t, tt.wantPublish, driver.lastPublish)
			assert.Equal(t, tt.wantSubscribe, driver.lastSubscribe)

			var got struct {
				Publish             []string `json:"publish_domains"`
				Subscribe           []string `json:"subscribe_domains"`
				RemotePublish       []string `json:"remote_publish_domains"`
				RemoteSubscribe     []string `json:"remote_subscribe_domains"`
				Role                string   `json:"sync_role"`
				PolicyVersion       int      `json:"policy_version"`
				Revision            int64    `json:"revision"`
				RemotePolicyVersion int      `json:"remote_policy_version"`
				RemoteRevision      int64    `json:"remote_revision"`
			}
			require.NoError(t, json.NewDecoder(rr.Body).Decode(&got))
			assert.Equal(t, tt.wantPublish, got.Publish)
			assert.Equal(t, tt.wantSubscribe, got.Subscribe)
			assert.Equal(t, []string{"remote.publish"}, got.RemotePublish)
			assert.Equal(t, []string{"remote.subscribe"}, got.RemoteSubscribe)
			assert.Equal(t, tt.role, got.Role)
			assert.Equal(t, federation.SyncPolicyVersionPeerRBAC, got.PolicyVersion)
			assert.Equal(t, int64(2), got.Revision)
			assert.Equal(t, federation.SyncPolicyVersionPeerRBAC, got.RemotePolicyVersion)
			assert.Equal(t, int64(4), got.RemoteRevision)

			getRR := getSync(t, h, "chain-v3")
			require.Equal(t, http.StatusOK, getRR.Code, getRR.Body.String())
			var raw map[string]json.RawMessage
			require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &raw))
			assert.NotContains(t, raw, "sync_domains", "a bilateral compatibility field would be false when local lanes differ")
		})
	}
}

func TestSyncDomainsV3LegacyCompatibilityOnlyWhenLanesMatch(t *testing.T) {
	s, ss, bs := newSyncServer(t)
	seedAgreement(t, bs, "chain-v3", nil, "active")
	seedActiveV3SyncControl(t, ss, "chain-v3", "guest",
		[]string{"different.publish"}, []string{"different.subscribe"}, nil, nil)
	driver := &syncDirectionalFederation{fakeFederation: &fakeFederation{}, store: ss}
	s.SetFederation(driver)
	h := syncRouterAs(s, syncOperatorID)

	getRR := getSync(t, h, "chain-v3")
	require.Equal(t, http.StatusOK, getRR.Code, getRR.Body.String())
	var raw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &raw))
	assert.NotContains(t, raw, "sync_domains")

	putRR := putSync(t, h, "chain-v3", `{"publish_domains":["same"],"subscribe_domains":["same"]}`)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())
	require.NoError(t, json.Unmarshal(putRR.Body.Bytes(), &raw))
	require.Contains(t, raw, "sync_domains")
	var compatibility []string
	require.NoError(t, json.Unmarshal(raw["sync_domains"], &compatibility))
	assert.Equal(t, []string{"same"}, compatibility)

	getRR = getSync(t, h, "chain-v3")
	require.Equal(t, http.StatusOK, getRR.Code, getRR.Body.String())
	require.NoError(t, json.Unmarshal(getRR.Body.Bytes(), &raw))
	require.Contains(t, raw, "sync_domains")
	require.NoError(t, json.Unmarshal(raw["sync_domains"], &compatibility))
	assert.Equal(t, []string{"same"}, compatibility)
}

func TestSyncDomainsV3CannotDowngradeToLegacySnapshot(t *testing.T) {
	s, ss, bs := newSyncServer(t)
	seedAgreement(t, bs, "chain-v3", []string{"legacy"}, "active")
	seedActiveV3SyncControl(t, ss, "chain-v3", "host",
		[]string{"v3.publish"}, []string{"v3.subscribe"}, nil, nil)
	require.NoError(t, ss.SetSyncDomains(context.Background(), "chain-v3", []string{"stale.legacy"}))
	driver := &syncDirectionalFederation{fakeFederation: &fakeFederation{}, store: ss}
	s.SetFederation(driver)
	h := syncRouterAs(s, syncOperatorID)

	rr := putSync(t, h, "chain-v3", `{"domains":["legacy"]}`)
	assert.Equal(t, http.StatusConflict, rr.Code, rr.Body.String())
	assert.Zero(t, driver.calls)
	legacy, err := ss.GetSyncDomains(context.Background(), "chain-v3")
	require.NoError(t, err)
	assert.Equal(t, []string{"stale.legacy"}, legacy, "rejected v3 mutation must not touch the legacy table")

	rr = putSync(t, h, "chain-v3", `{"domains":[],"subscribe_domains":[]}`)
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
}

func TestSyncDomainsLegacyRejectsDirectionalFields(t *testing.T) {
	s, _, bs := newSyncServer(t)
	seedAgreement(t, bs, "chain-legacy", []string{"shared"}, "active")
	h := syncRouterAs(s, syncOperatorID)

	assert.Equal(t, http.StatusConflict,
		putSync(t, h, "chain-legacy", `{"publish_domains":["shared"]}`).Code)
	assert.Equal(t, http.StatusConflict,
		putSync(t, h, "chain-legacy", `{"subscribe_domains":[]}`).Code)
}

func TestSyncDomainsUnfrozenV3MarkerNeverFallsBackToLegacy(t *testing.T) {
	s, ss, bs := newSyncServer(t)
	seedAgreement(t, bs, "chain-v3", []string{"shared"}, "active")
	require.NoError(t, ss.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-v3", Role: "host", ControllerChainID: "local",
		ControllerAgentID: "local-agent", PolicyEpoch: "epoch-v3", RemoteCAPin: "pin",
		PolicyVersion: federation.SyncPolicyVersionPeerRBAC,
	}))
	require.NoError(t, ss.ActivateSyncControl(context.Background(), "chain-v3", "epoch-v3"))
	require.NoError(t, ss.SetSyncDomains(context.Background(), "chain-v3", []string{"stale.legacy"}))
	h := syncRouterAs(s, syncOperatorID)

	assert.Equal(t, http.StatusConflict,
		putSync(t, h, "chain-v3", `{"domains":["shared"]}`).Code)
	assert.Equal(t, http.StatusConflict, getSync(t, h, "chain-v3").Code)
}

func TestSyncDomainsGuestCannotReplaceHostPolicy(t *testing.T) {
	s, ms, bs := newSyncServer(t)
	h := syncRouterAs(s, syncOperatorID)
	seedAgreement(t, bs, "chain-host", []string{"shared"}, "active")
	require.NoError(t, ms.PrepareSyncControl(context.Background(), store.SyncControl{
		RemoteChainID: "chain-host", Role: "guest", ControllerChainID: "chain-host",
		ControllerAgentID: "host-agent", PolicyEpoch: "epoch", RemoteCAPin: "pin",
		PolicyVersion: federation.SyncPolicyVersionLegacy,
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
