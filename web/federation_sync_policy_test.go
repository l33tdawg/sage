package web

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

type syncContractDriver struct {
	FederationJoinDriver
	publish   []string
	subscribe []string
}

func (d *syncContractDriver) UpdateDirectionalSyncPolicy(_ context.Context, _ string, publish, subscribe *[]string) (*federation.DirectionalSyncPolicyResult, error) {
	if publish != nil {
		d.publish = append([]string(nil), (*publish)...)
	}
	if subscribe != nil {
		d.subscribe = append([]string(nil), (*subscribe)...)
	}
	return &federation.DirectionalSyncPolicyResult{
		Version: federation.SyncPolicyVersionPeerRBAC, Revision: 2,
		PublishDomains: append([]string(nil), d.publish...), SubscribeDomains: append([]string(nil), d.subscribe...),
		State: "delivered",
	}, nil
}

func withFederationChain(req *http.Request, chain string) *http.Request {
	routeCtx := chi.NewRouteContext()
	routeCtx.URLParams.Add("chain_id", chain)
	return req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, routeCtx))
}

func TestFederationSyncV3ContractAndOmittedLanePreservation(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	peerPin := []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, bs.SetCrossFed("chain-b", "https://peer:8444", peerPin, 2, 0,
		[]string{"legacy"}, []string{"*"}, "active"))
	require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-b", Role: "guest", ControllerChainID: "chain-b",
		ControllerAgentID: "peer-agent", PeerAgentID: "peer-agent", PolicyEpoch: "epoch-v3",
		RemoteCAPin: hex.EncodeToString(peerPin),
	}))
	require.NoError(t, ss.ActivateSyncControl(ctx, "chain-b", "epoch-v3"))
	_, err = ss.ApplyLocalDirectionalSyncPolicy(ctx, "chain-b", "epoch-v3",
		federation.SyncPolicyVersionPeerRBAC, 1, "local-1", []string{"local.publish"}, []string{"local.subscribe"})
	require.NoError(t, err)
	_, err = ss.ApplyRemoteDirectionalSyncPolicy(ctx, "chain-b", "epoch-v3",
		federation.SyncPolicyVersionPeerRBAC, 1, "remote-1", []string{"remote.publish"}, []string{"remote.subscribe"})
	require.NoError(t, err)

	driver := &syncContractDriver{publish: []string{"local.publish"}, subscribe: []string{"local.subscribe"}}
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.Federation = driver

	getReq := withFederationChain(httptest.NewRequest(http.MethodGet,
		"http://localhost/v1/dashboard/federation/connections/chain-b/sync", nil), "chain-b")
	getRR := httptest.NewRecorder()
	h.handleFedSyncGet(getRR, getReq)
	require.Equal(t, http.StatusOK, getRR.Code)
	var got struct {
		Publish         []string  `json:"publish_domains"`
		Subscribe       []string  `json:"subscribe_domains"`
		RemotePublish   []string  `json:"remote_publish_domains"`
		RemoteSubscribe []string  `json:"remote_subscribe_domains"`
		SyncDomains     *[]string `json:"sync_domains"`
	}
	require.NoError(t, json.NewDecoder(getRR.Body).Decode(&got))
	assert.Equal(t, []string{"local.publish"}, got.Publish)
	assert.Equal(t, []string{"local.subscribe"}, got.Subscribe)
	assert.Equal(t, []string{"remote.publish"}, got.RemotePublish)
	assert.Equal(t, []string{"remote.subscribe"}, got.RemoteSubscribe)
	assert.Nil(t, got.SyncDomains, "a v3 compatibility field would be false when the local lanes differ")

	putReq := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-b/sync",
		bytes.NewBufferString(`{"subscribe_domains":["new.subscription"]}`)), "chain-b")
	putReq.RemoteAddr = "127.0.0.1:4242"
	putReq.Header.Set("Sec-Fetch-Site", "same-origin")
	putRR := httptest.NewRecorder()
	h.handleFedSyncSet(putRR, putReq)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())
	assert.Equal(t, []string{"local.publish"}, driver.publish, "omitted publish lane must be preserved")
	assert.Equal(t, []string{"new.subscription"}, driver.subscribe)
	var putRaw map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(putRR.Body.Bytes(), &putRaw))
	assert.NotContains(t, putRaw, "sync_domains", "directional lanes must not be collapsed into a false bilateral set")

	legacyReq := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-b/sync",
		bytes.NewBufferString(`{"domains":["legacy"]}`)), "chain-b")
	legacyReq.RemoteAddr = "127.0.0.1:4242"
	legacyReq.Header.Set("Sec-Fetch-Site", "same-origin")
	legacyRR := httptest.NewRecorder()
	h.handleFedSyncSet(legacyRR, legacyReq)
	assert.Equal(t, http.StatusConflict, legacyRR.Code, legacyRR.Body.String())
}

func TestFederationSyncSubscribeOnlyMigrationDoesNotPromoteLegacyCopy(t *testing.T) {
	ctx := context.Background()
	ss, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	peerPin := []byte("pin-bytes-32-aaaaaaaaaaaaaaaaaaaa")
	require.NoError(t, bs.SetCrossFed("chain-b", "https://peer:8444", peerPin, 2, 0,
		[]string{"legacy"}, []string{"*"}, "active"))
	require.NoError(t, ss.PrepareSyncControl(ctx, store.SyncControl{
		RemoteChainID: "chain-b", Role: "guest", ControllerChainID: "chain-b",
		ControllerAgentID: "peer-agent", PeerAgentID: "peer-agent", PolicyEpoch: "epoch-v2",
		RemoteCAPin: hex.EncodeToString(peerPin), PolicyVersion: federation.SyncPolicyVersionDirectional,
	}))
	require.NoError(t, ss.ActivateSyncControl(ctx, "chain-b", "epoch-v2"))
	require.NoError(t, ss.SetSyncDomains(ctx, "chain-b", []string{"legacy"}))

	driver := &syncContractDriver{}
	h := NewDashboardHandler(ss, "test")
	h.BadgerStore = bs
	h.Federation = driver
	putReq := withFederationChain(httptest.NewRequest(http.MethodPut,
		"http://localhost/v1/dashboard/federation/connections/chain-b/sync",
		bytes.NewBufferString(`{"subscribe_domains":["wanted"]}`)), "chain-b")
	putReq.RemoteAddr = "127.0.0.1:4242"
	putReq.Header.Set("Sec-Fetch-Site", "same-origin")
	putRR := httptest.NewRecorder()
	h.handleFedSyncSet(putRR, putReq)
	require.Equal(t, http.StatusOK, putRR.Code, putRR.Body.String())
	assert.Empty(t, driver.publish, "legacy sync_domains must not become v3 Publish/Copy")
	assert.Equal(t, []string{"wanted"}, driver.subscribe)
}
