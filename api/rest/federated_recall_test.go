package rest

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// fakeFederation implements FederationService with canned outcomes so the
// merge path is testable without TLS or a peer chain.
type fakeFederation struct {
	outcomes      []federation.PeerRecallOutcome
	calls         int
	lastReq       *federation.QueryRequest
	peerStatus    *federation.StatusResponse
	peerStatusErr error
	agreementMu   sync.Mutex
}

type parallelStatusFederation struct {
	*fakeFederation
	statuses map[string]*federation.StatusResponse
	delay    time.Duration
	mu       sync.Mutex
	calls    int
	current  int
	max      int
}

type parallelLookupFederation struct {
	*parallelStatusFederation
	lookups          map[string]*federation.PipeContactLookupResponse
	delays           map[string]time.Duration
	statusAwareCalls int
	lookupMu         sync.Mutex
}

type linkedLookupFederation struct {
	*parallelStatusFederation
	linked       map[string]*federation.LinkedMessageDirectoryResult
	linkedErrors map[string]error
	peerHints    map[string]bool
	mu           sync.Mutex
	callerID     string
	name         string
	calls        int
	hintCalls    int
}

func (f *linkedLookupFederation) CallerMayHaveLinkedMessagePeer(
	_ context.Context, chainID, _ string,
) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.hintCalls++
	return f.peerHints[chainID], nil
}

type amplifiedLookupFederation struct {
	*parallelStatusFederation
	totalMu sync.Mutex
	total   int
}

func (f *amplifiedLookupFederation) addCall() {
	f.totalMu.Lock()
	f.total++
	f.totalMu.Unlock()
}

func (f *amplifiedLookupFederation) PeerStatus(ctx context.Context, chainID string) (*federation.StatusResponse, error) {
	f.addCall()
	return f.parallelStatusFederation.PeerStatus(ctx, chainID)
}

func (f *amplifiedLookupFederation) FindRemotePipeContactsWithStatus(
	_ context.Context, _ string, _ *federation.StatusResponse, _ string, _ int,
) (*federation.PipeContactLookupResponse, error) {
	f.addCall()
	return nil, errors.New("no legacy match")
}

func (f *amplifiedLookupFederation) FindRemoteLinkedMessageContacts(
	_ context.Context, _, _, _ string, _ int,
) (*federation.LinkedMessageDirectoryResult, error) {
	f.addCall()
	return nil, errors.New("no linked match")
}

type provenanceMemoryStore struct {
	*mockMemoryStore
	origin *store.SyncOrigin
}

func (s *provenanceMemoryStore) GetSyncOriginByLocalMemoryID(_ context.Context, localID string) (*store.SyncOrigin, error) {
	if s.origin != nil && s.origin.LocalMemoryID == localID {
		return s.origin, nil
	}
	return nil, errors.New("not a synced copy")
}

func (f *parallelStatusFederation) PeerStatus(ctx context.Context, chainID string) (*federation.StatusResponse, error) {
	f.mu.Lock()
	f.calls++
	f.current++
	if f.current > f.max {
		f.max = f.current
	}
	f.mu.Unlock()
	defer func() {
		f.mu.Lock()
		f.current--
		f.mu.Unlock()
	}()
	select {
	case <-time.After(f.delay):
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	status := f.statuses[chainID]
	if status == nil {
		return nil, errors.New("unreachable")
	}
	return status, nil
}

func (f *parallelLookupFederation) FindRemotePipeContacts(ctx context.Context, chainID, _ string, _ int) (*federation.PipeContactLookupResponse, error) {
	if delay := f.delays[chainID]; delay > 0 {
		select {
		case <-time.After(delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	lookup := f.lookups[chainID]
	if lookup == nil {
		return nil, errors.New("lookup unavailable")
	}
	return lookup, nil
}

func (f *parallelLookupFederation) FindRemotePipeContactsWithStatus(ctx context.Context, chainID string, _ *federation.StatusResponse, name string, limit int) (*federation.PipeContactLookupResponse, error) {
	f.lookupMu.Lock()
	f.statusAwareCalls++
	f.lookupMu.Unlock()
	return f.FindRemotePipeContacts(ctx, chainID, name, limit)
}

func (f *linkedLookupFederation) FindRemoteLinkedMessageContacts(
	_ context.Context,
	chainID, callerID, name string,
	_ int,
) (*federation.LinkedMessageDirectoryResult, error) {
	f.mu.Lock()
	f.calls++
	f.callerID = callerID
	f.name = name
	f.mu.Unlock()
	if err := f.linkedErrors[chainID]; err != nil {
		return nil, err
	}
	return f.linked[chainID], nil
}

func (f *linkedLookupFederation) ListRemoteLinkedMessageContacts(
	_ context.Context,
	chainID, callerID string,
) (*federation.LinkedMessageDirectoryResult, error) {
	f.mu.Lock()
	f.calls++
	f.callerID = callerID
	f.name = ""
	f.mu.Unlock()
	if err := f.linkedErrors[chainID]; err != nil {
		return nil, err
	}
	return f.linked[chainID], nil
}

func (f *fakeFederation) LockAgreementMutation() func() {
	f.agreementMu.Lock()
	return f.agreementMu.Unlock
}

func (f *fakeFederation) FanOutRecall(_ context.Context, _ []string, qr *federation.QueryRequest) []federation.PeerRecallOutcome {
	f.calls++
	f.lastReq = qr
	return f.outcomes
}

func (f *fakeFederation) DeliverReceipts(context.Context, string, int64, int64) map[string]federation.DeliveryResult {
	return nil
}
func (f *fakeFederation) StageRemoteCA(string, []byte) ([]byte, func() error, func(), error) {
	return nil, nil, nil, errors.New("na")
}
func (f *fakeFederation) PeerStatus(context.Context, string) (*federation.StatusResponse, error) {
	if f.peerStatus != nil || f.peerStatusErr != nil {
		return f.peerStatus, f.peerStatusErr
	}
	return nil, errors.New("na")
}
func (f *fakeFederation) LocalChainID() string { return "chain-local" }

// v11 JOIN ceremony drivers - unused by the recall tests; stubbed to satisfy
// the FederationService interface.
func (f *fakeFederation) HostCreate(string) (*federation.HostCreateResult, error) {
	return nil, errors.New("na")
}
func (f *fakeFederation) HostScanReturn(string, string) error { return errors.New("na") }
func (f *fakeFederation) HostSessionStatus(string) (*federation.HostSessionView, error) {
	return nil, errors.New("na")
}
func (f *fakeFederation) HostApprove(string, string, federation.ScopeWire) error {
	return errors.New("na")
}
func (f *fakeFederation) HostAbort(string) error { return nil }
func (f *fakeFederation) GuestScan(context.Context, string, string) (*federation.GuestScanResult, error) {
	return nil, errors.New("na")
}
func (f *fakeFederation) GuestRequest(context.Context, string, string, federation.ScopeWire) (*federation.GuestRequestResult, error) {
	return nil, errors.New("na")
}
func (f *fakeFederation) GuestConfirm(context.Context, string, string, federation.ScopeWire) (string, error) {
	return "", errors.New("na")
}
func (f *fakeFederation) SyncReconcileInfo(string) (federation.SyncReconcileStatus, bool) {
	return federation.SyncReconcileStatus{}, false
}

func TestFederatedRecallMergesRemoteResults(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")

	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "local knowledge",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	}

	fed := &fakeFederation{outcomes: []federation.PeerRecallOutcome{
		{
			ChainID: "chain-b",
			Results: []*federation.MemoryResult{{
				MemoryID:        "remote-1",
				SubmittingAgent: "deadbeef@chain-b",
				Content:         "remote knowledge",
				DomainTag:       "shared",
				ConfidenceScore: 0.8,
				Status:          "committed",
				CreatedAt:       time.Now().Add(-2 * time.Hour),
				SourceChainID:   "chain-b",
			}},
		},
		{ChainID: "chain-dead", Err: errors.New("peer unreachable")},
	}}
	srv.SetFederation(fed)

	body, _ := json.Marshal(HybridSearchMemoryRequest{Query: "knowledge", Embedding: []float32{0.1, 0.2}, Federated: true})
	req, agentID := signedRequest(t, http.MethodPost, "/v1/memory/hybrid", body)
	srv.SetNodeOperatorID(agentID) // federated recall is operator-gated
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	require.Equal(t, 1, fed.calls, "federation fan-out should run exactly once")
	assert.Equal(t, federation.ModeHybrid, fed.lastReq.Mode)
	assert.Equal(t, "knowledge", fed.lastReq.Query)

	// Local + remote merged; remote stamped with provenance.
	require.Equal(t, 2, resp.TotalCount)
	var remote *MemoryResult
	for _, r := range resp.Results {
		if r.MemoryID == "remote-1" {
			remote = r
		}
	}
	require.NotNil(t, remote, "remote result missing from merged response")
	assert.Equal(t, "chain-b", remote.SourceChainID)
	assert.Equal(t, "deadbeef@chain-b", remote.SubmittingAgent)

	// Failed peer disclosed, never silently dropped.
	require.NotNil(t, resp.Federation)
	assert.ElementsMatch(t, []string{"chain-b", "chain-dead"}, resp.Federation.Queried)
	assert.Equal(t, 1, resp.Federation.Merged)
	assert.Contains(t, resp.Federation.Errors["chain-dead"], "unreachable")
}

func TestFederatedRecallDeniedForNonOperator(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "local only",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}
	fed := &fakeFederation{outcomes: []federation.PeerRecallOutcome{
		{ChainID: "chain-b", Results: []*federation.MemoryResult{{MemoryID: "remote-1", SourceChainID: "chain-b"}}},
	}}
	srv.SetFederation(fed)
	srv.SetNodeOperatorID("some-other-operator") // caller will NOT match

	body, _ := json.Marshal(HybridSearchMemoryRequest{Query: "local", Embedding: []float32{0.1, 0.2}, Federated: true})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/hybrid", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 0, fed.calls, "non-operator must not trigger a fan-out")
	require.NotNil(t, resp.Federation)
	assert.Contains(t, resp.Federation.Errors["*"], "not authorized")
	// Only the local result survives — no remote leak.
	assert.Equal(t, 1, resp.TotalCount)
}

func TestFederatedRecallOrdinaryAgentUsesSubtreeDelegationAndGlobalTopK(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("local-%d", i)
		memStore.memories[id] = &memory.MemoryRecord{
			MemoryID: id, SubmittingAgent: "local-agent", Content: "benchmark local",
			ContentHash: []byte{byte(i + 1)}, MemoryType: memory.TypeFact,
			DomainTag: "sage-autoresearch.benchmark", ConfidenceScore: 0.9 - float64(i)*0.1,
			Status: memory.StatusCommitted, CreatedAt: time.Now(),
		}
	}
	fed := &fakeFederation{
		peerStatus: &federation.StatusResponse{
			ChainID: "chain-a",
			PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{{
				Domain: "sage-autoresearch", Read: true,
			}}},
		},
		outcomes: []federation.PeerRecallOutcome{{
			ChainID: "chain-a",
			Results: []*federation.MemoryResult{
				{MemoryID: "remote-1", SubmittingAgent: "author-a", Content: "benchmark remote one", DomainTag: "sage-autoresearch.benchmark", ConfidenceScore: .95, Classification: 1, Status: "committed", SourceChainID: "chain-a"},
				{MemoryID: "remote-2", SubmittingAgent: "author-a", Content: "benchmark remote two", DomainTag: "sage-autoresearch.benchmark", ConfidenceScore: .85, Classification: 1, Status: "committed", SourceChainID: "chain-a"},
			},
		}},
	}
	srv.SetFederation(fed)
	require.NoError(t, badger.SetCrossFed("chain-a", "https://redacted.invalid", []byte("pin"), 2, 0, []string{"*"}, nil, "active"))

	body, _ := json.Marshal(HybridSearchMemoryRequest{
		Query: "benchmark", Embedding: []float32{.1, .2}, DomainTag: "sage-autoresearch.benchmark",
		Federated: true, TopK: 2,
	})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/hybrid", body)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1,
		`[{"domain":"sage-autoresearch","read":true}]`, "*", "", ""))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 1, fed.calls, "ordinary authorized caller should delegate")
	require.Len(t, resp.Results, 2, "top_k must cap the globally merged result set")
	require.NotNil(t, resp.Federation)
	assert.GreaterOrEqual(t, resp.Federation.Merged, 1)
	require.Len(t, resp.Federation.Coverage, 1)
	assert.Equal(t, "hybrid", resp.Federation.Coverage[0].SearchMode)
	assert.Contains(t, resp.Federation.Coverage[0].Fallback, "embedding-provider")
	var remote *MemoryResult
	for _, result := range resp.Results {
		if result.SourceKind == "federated_live" {
			remote = result
		}
	}
	require.NotNil(t, remote)
	assert.True(t, remote.Foreign)
	assert.Equal(t, "external_untrusted", remote.Trust)
	assert.Equal(t, "author-a", remote.OriginAgentID)
}

func TestFederatedSemanticRecallCarriesTextFallback(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	fed := &fakeFederation{}
	srv.SetFederation(fed)
	body, _ := json.Marshal(QueryMemoryRequest{
		Query: "benchmark exact words", Embedding: []float32{.1, .2},
		DomainTag: "sage-autoresearch-benchmark", Federated: true,
	})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/query", body)
	srv.SetNodeOperatorID(callerID)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.NotNil(t, fed.lastReq)
	assert.Equal(t, federation.ModeHybrid, fed.lastReq.Mode)
	assert.Equal(t, "benchmark exact words", fed.lastReq.Query)
}

func TestFederatedRecallDeduplicatesLocalAndLiveByContentHash(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-copy-shape"] = &memory.MemoryRecord{
		MemoryID: "local-copy-shape", SubmittingAgent: "local-agent", Content: "same immutable fact",
		ContentHash: []byte{1}, MemoryType: memory.TypeFact, DomainTag: "shared",
		ConfidenceScore: .8, Status: memory.StatusCommitted, CreatedAt: time.Now(),
	}
	fed := &fakeFederation{outcomes: []federation.PeerRecallOutcome{{
		ChainID: "chain-a",
		Results: []*federation.MemoryResult{
			{MemoryID: "origin-1", Content: "same immutable fact", ContentHash: "01", DomainTag: "shared", ConfidenceScore: .9, Status: "committed", SourceChainID: "chain-a"},
			{MemoryID: "origin-2", Content: "different fact", ContentHash: "02", DomainTag: "shared", ConfidenceScore: .7, Status: "committed", SourceChainID: "chain-a"},
		},
	}}}
	srv.SetFederation(fed)
	body, _ := json.Marshal(SearchMemoryRequest{Query: "fact", DomainTag: "shared", Federated: true, TopK: 10})
	req, callerID := signedRequest(t, http.MethodPost, "/v1/memory/search", body)
	srv.SetNodeOperatorID(callerID)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Len(t, resp.Results, 2, "same content hash from local/live sources must be one fused result")
	assert.Equal(t, "local_native", resp.Results[0].SourceKind, "prefer the locally committed representative")
}

func TestRecallSurfacesStoredCopyOriginalProvenance(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-copy"] = &memory.MemoryRecord{
		MemoryID: "local-copy", SubmittingAgent: "local-receiver", Content: "retained foreign fact",
		ContentHash: []byte{9}, MemoryType: memory.TypeObservation, DomainTag: "shared",
		ConfidenceScore: .9, Status: memory.StatusCommitted, CreatedAt: time.Now(),
	}
	srv.store = &provenanceMemoryStore{mockMemoryStore: memStore, origin: &store.SyncOrigin{
		OriginChainID: "chain-a", OriginMemoryID: "origin-9", OriginAgentPubkey: "agent-a",
		LocalMemoryID: "local-copy", ContentHash: "feed09", Classification: 2, MemoryType: "fact",
	}}
	body, _ := json.Marshal(SearchMemoryRequest{Query: "foreign fact", DomainTag: "shared", TopK: 5})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/search", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Len(t, response.Results, 1)
	got := response.Results[0]
	assert.Equal(t, "federated_copy", got.SourceKind)
	assert.Equal(t, "chain-a", got.SourceChainID)
	assert.Equal(t, "origin-9", got.OriginMemoryID)
	assert.Equal(t, "agent-a", got.OriginAgentID)
	assert.Equal(t, "feed09", got.ContentHash)
	assert.Equal(t, 2, got.Classification)
	assert.Equal(t, "fact", got.MemoryType)
	assert.Equal(t, "external_untrusted", got.Trust)
}

func TestCrossFedStatusExposesAuthenticatedRemoteDiscovery(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	fed := &fakeFederation{peerStatus: &federation.StatusResponse{
		ChainID:      "chain-dkan-tii",
		NetworkName:  "DKAN-TII",
		Time:         time.Now().Unix(),
		Capabilities: []string{federation.CapabilitySync, federation.CapabilityFederatedPipeline},
		PeerRBACGrant: &federation.PeerRBACGrant{
			PolicyVersion: 3,
			Domains: []federation.PeerRBACDomainGrant{{
				Domain: "sage-autoresearch-benchmark",
				Read:   true,
			}},
		},
		PipeContacts: &federation.PipeContactGrant{
			Contacts: []federation.PipeContact{{AgentID: "agent-b"}},
		},
	}}
	srv.SetFederation(fed)

	req, agentID := signedRequest(t, http.MethodGet, "/v1/federation/cross/chain-dkan-tii/status", nil)
	srv.SetNodeOperatorID(agentID)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var response map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	assert.Equal(t, true, response["reachable"])
	assert.Equal(t, "DKAN-TII", response["network_name"])
	assert.NotNil(t, response["peer_rbac_grant"])
	assert.NotNil(t, response["pipe_contacts"])
}

func TestFederationAvailableIsCallerFilteredSubtreeAndParallel(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	for _, chain := range []string{"chain-a", "chain-b"} {
		require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"*"}, nil, "active"))
	}
	status := func(name string) *federation.StatusResponse {
		return &federation.StatusResponse{
			ChainID: name, NetworkName: name,
			PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{
				{Domain: "sage-autoresearch", Read: true, Copy: true},
				{Domain: "finance.secret", Read: true},
			}},
		}
	}
	fed := &parallelStatusFederation{
		fakeFederation: &fakeFederation{},
		statuses: map[string]*federation.StatusResponse{
			"chain-a": status("A"),
			"chain-b": status("B"),
		},
		delay: 100 * time.Millisecond,
	}
	srv.SetFederation(fed)

	req, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1,
		`[{"domain":"sage-autoresearch.benchmark","read":true}]`, "*", "", ""))
	start := time.Now()
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	elapsed := time.Since(start)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var response struct {
		Connections []availableFederationConnection `json:"connections"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Len(t, response.Connections, 2)
	for _, connection := range response.Connections {
		assert.Equal(t, []string{"sage-autoresearch.benchmark"}, connection.SharedReadDomains)
		assert.Equal(t, []string{"sage-autoresearch.benchmark"}, connection.CopyOfferedDomains)
	}
	assert.GreaterOrEqual(t, fed.max, 2, "peer status probes should overlap")
	assert.Less(t, elapsed, 190*time.Millisecond, "discovery should be one bounded parallel window")
}

func TestFederationAvailableBoundsNamedDiscoveryWorkersAndPeers(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	statuses := make(map[string]*federation.StatusResponse, maxFederationAvailablePeers+1)
	for i := 0; i < maxFederationAvailablePeers+1; i++ {
		chain := fmt.Sprintf("chain-%03d", i)
		require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"*"}, nil, "active"))
		statuses[chain] = &federation.StatusResponse{
			ChainID: chain,
			PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{{
				Domain: "research", Read: true,
			}}},
		}
	}
	fed := &parallelStatusFederation{
		fakeFederation: &fakeFederation{},
		statuses:       statuses,
		delay:          5 * time.Millisecond,
	}
	srv.SetFederation(fed)
	req, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available?agent_name=innovium", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	fed.mu.Lock()
	calls, maxWorkers := fed.calls, fed.max
	fed.mu.Unlock()
	assert.Equal(t, maxFederatedNameLookupPeers, calls,
		"one friendly-name miss must not turn into a full federation topology crawl")
	assert.LessOrEqual(t, maxWorkers, maxConcurrentFedAvailability, "status and lookup work share one bounded worker pool")
}

func TestFederationAvailableSharesProbeBudgetAcrossConcurrentAgents(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger

	statuses := make(map[string]*federation.StatusResponse, maxFederatedNameLookupPeers)
	for i := range maxFederatedNameLookupPeers {
		chain := fmt.Sprintf("chain-%03d", i)
		require.NoError(t, badger.SetCrossFed(
			chain, "https://redacted.invalid", []byte(chain), 2, 0,
			[]string{"*"}, nil, "active",
		))
		statuses[chain] = &federation.StatusResponse{
			ChainID: chain,
			PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{{
				Domain: "research", Read: true,
			}}},
		}
	}
	fed := &parallelStatusFederation{
		fakeFederation: &fakeFederation{}, statuses: statuses,
		delay: 15 * time.Millisecond,
	}
	srv.SetFederation(fed)

	const callers = 6
	requests := make([]*http.Request, 0, callers)
	for range callers {
		req, callerID := signedRequest(
			t, http.MethodGet,
			"/v1/federation/available?agent_name=missing", nil,
		)
		require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
		require.NoError(t, badger.SetAgentPermission(
			callerID, 1, `[{"domain":"research","read":true}]`, "*", "", "",
		))
		requests = append(requests, req)
	}

	var wg sync.WaitGroup
	for _, req := range requests {
		wg.Add(1)
		go func(request *http.Request) {
			defer wg.Done()
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, request)
			assert.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		}(req)
	}
	wg.Wait()

	fed.mu.Lock()
	peak := fed.max
	fed.mu.Unlock()
	assert.LessOrEqual(t, peak, maxConcurrentFedAvailability,
		"concurrent MCP clients must share one node-wide peer-probe budget")
}

func TestFederationAvailableBoundsTotalDistinctLookupCallsPerCaller(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	srv.federationProbeBudget = newFederationProbeRateBudget()
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	statuses := make(map[string]*federation.StatusResponse, maxFederatedNameLookupPeers)
	for i := range maxFederatedNameLookupPeers {
		chain := fmt.Sprintf("chain-%03d", i)
		require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"*"}, nil, "active"))
		statuses[chain] = &federation.StatusResponse{ChainID: chain}
	}
	fed := &parallelStatusFederation{fakeFederation: &fakeFederation{}, statuses: statuses}
	srv.SetFederation(fed)
	_, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))

	for i := 0; i < 4; i++ {
		req := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/v1/federation/available?agent_name=missing-%d", i), nil)
		req = req.WithContext(middleware.WithAgentID(req.Context(), callerID))
		rr := httptest.NewRecorder()
		srv.handleFederationAvailable(rr, req)
		if i < 3 {
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		} else {
			require.Equal(t, http.StatusTooManyRequests, rr.Code, rr.Body.String())
			require.Equal(t, "10", rr.Header().Get("Retry-After"))
		}
	}
	fed.mu.Lock()
	total := fed.calls
	fed.mu.Unlock()
	require.Equal(t, federationProbeBudgetPerCaller, total,
		"distinct cache misses must consume a fixed total outbound-call budget")
}

func TestFederationAvailableBudgetsEveryAmplifiedUpstreamCall(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	srv.federationProbeBudget = newFederationProbeRateBudget()
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	statuses := make(map[string]*federation.StatusResponse, maxFederatedNameLookupPeers)
	for i := range maxFederatedNameLookupPeers {
		chain := fmt.Sprintf("chain-%03d", i)
		require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"*"}, nil, "active"))
		statuses[chain] = &federation.StatusResponse{ChainID: chain}
	}
	fed := &amplifiedLookupFederation{parallelStatusFederation: &parallelStatusFederation{
		fakeFederation: &fakeFederation{}, statuses: statuses,
	}}
	srv.SetFederation(fed)
	_, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))

	for i, want := range []int{http.StatusOK, http.StatusTooManyRequests} {
		req := httptest.NewRequest(http.MethodGet,
			fmt.Sprintf("/v1/federation/available?agent_name=unique-%d", i), nil)
		req = req.WithContext(middleware.WithAgentID(req.Context(), callerID))
		rr := httptest.NewRecorder()
		srv.handleFederationAvailable(rr, req)
		require.Equal(t, want, rr.Code, rr.Body.String())
	}
	fed.totalMu.Lock()
	total := fed.total
	fed.totalMu.Unlock()
	require.Equal(t, federationProbeBudgetPerCaller, total,
		"one accepted targeted page may spend 8*(status+legacy+linked), while the next distinct miss makes zero upstream calls")
}

func TestFederationAvailableLargeInventoryReturnsBoundedUsefulPage(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	srv.federationProbeBudget = newFederationProbeRateBudget()
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	statuses := make(map[string]*federation.StatusResponse, 40)
	for i := range 40 {
		chain := fmt.Sprintf("chain-%03d", i)
		require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"*"}, nil, "active"))
		statuses[chain] = &federation.StatusResponse{ChainID: chain, NetworkName: chain}
	}
	fed := &parallelStatusFederation{fakeFederation: &fakeFederation{}, statuses: statuses}
	srv.SetFederation(fed)
	_, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))

	req := httptest.NewRequest(http.MethodGet, "/v1/federation/available", nil)
	req = req.WithContext(middleware.WithAgentID(req.Context(), callerID))
	rr := httptest.NewRecorder()
	srv.handleFederationAvailable(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response struct {
		Complete bool   `json:"complete"`
		Next     string `json:"next_peer_cursor"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.False(t, response.Complete)
	require.NotEmpty(t, response.Next)
	fed.mu.Lock()
	calls := fed.calls
	fed.mu.Unlock()
	require.Equal(t, federationProbeBudgetPerCaller, calls,
		"a large status-only inventory returns one useful bounded page instead of rejecting the whole request")
}

func TestFederationAvailableRateCadenceDoesNotRevealHiddenPeerCount(t *testing.T) {
	run := func(hiddenPeerCount int) []int {
		t.Helper()
		srv, _, _ := newTestServer(t, "")
		srv.federationProbeBudget = newFederationProbeRateBudget()
		badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = badger.CloseBadger() })
		srv.badgerStore = badger
		statuses := make(map[string]*federation.StatusResponse, 8+hiddenPeerCount)
		for i := range 8 {
			chain := fmt.Sprintf("chain-%03d", i)
			require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"research"}, nil, "active"))
			statuses[chain] = &federation.StatusResponse{ChainID: chain}
		}
		for i := range hiddenPeerCount {
			chain := fmt.Sprintf("hidden-%03d", i)
			require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"secret"}, nil, "active"))
			statuses[chain] = &federation.StatusResponse{ChainID: chain}
		}
		srv.SetFederation(&parallelStatusFederation{fakeFederation: &fakeFederation{}, statuses: statuses})
		_, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
		require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
		require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))
		codes := make([]int, 4)
		for i := range codes {
			req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/v1/federation/available?agent_name=miss-%d", i), nil)
			req = req.WithContext(middleware.WithAgentID(req.Context(), callerID))
			rr := httptest.NewRecorder()
			srv.handleFederationAvailable(rr, req)
			codes[i] = rr.Code
		}
		return codes
	}
	require.Equal(t, run(0), run(40), "429 cadence must be independent of unrelated hidden active-agreement count")
}

func TestFederationAvailableRunsTargetedLookupsInParallel(t *testing.T) {
	t.Setenv("SAGE_FED_RECALL_TIMEOUT_MS", "120")
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	for _, chain := range []string{"chain-a", "chain-b"} {
		require.NoError(t, badger.SetCrossFed(chain, "https://redacted.invalid", []byte(chain), 2, 0, []string{"*"}, nil, "active"))
	}
	status := func(name string) *federation.StatusResponse {
		return &federation.StatusResponse{
			ChainID: name, NetworkName: name,
			PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{{Domain: "research", Read: true}}},
		}
	}
	agentID := strings.Repeat("b", 64)
	lookup := &federation.PipeContactLookupResponse{Grant: &federation.PipeContactGrant{
		Version: federation.PipeContactVersion, AgreementID: strings.Repeat("a", 64), Revision: strings.Repeat("c", 64),
		Contacts: []federation.PipeContact{{
			AgentID: agentID, ContactID: strings.Repeat("d", 64), Address: agentID + "@chain-b", Handle: "#remote/bbbbbbbb",
			DisplayName: "Innovium", Available: true, Accepting: true,
			Domains: []federation.PipeContactDomain{{Domain: "research"}},
		}},
	}, Total: 1}
	fed := &parallelLookupFederation{
		parallelStatusFederation: &parallelStatusFederation{
			fakeFederation: &fakeFederation{},
			statuses:       map[string]*federation.StatusResponse{"chain-a": status("A"), "chain-b": status("B")},
		},
		lookups: map[string]*federation.PipeContactLookupResponse{"chain-b": lookup},
		delays:  map[string]time.Duration{"chain-a": time.Second},
	}
	srv.SetFederation(fed)

	req, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available?agent_name=innovium", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))
	start := time.Now()
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.Less(t, time.Since(start), 400*time.Millisecond)

	var response struct {
		Connections []availableFederationConnection `json:"connections"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	var chainB *availableFederationConnection
	for i := range response.Connections {
		if response.Connections[i].RemoteChainID == "chain-b" {
			chainB = &response.Connections[i]
			break
		}
	}
	require.NotNil(t, chainB, "the fast peer remains visible while another lookup times out")
	require.Len(t, chainB.RemoteAgents, 1)
	assert.False(t, chainB.RemoteAgentsTruncated, "hidden remote matches must not be inferred from a truncation bit")
	assert.Empty(t, chainB.Capabilities, "named recipient discovery must not retain general peer metadata")
	assert.Empty(t, chainB.RemotePermissions)
	assert.Empty(t, chainB.SharedReadDomains)
	assert.Nil(t, chainB.Sync)
	assert.Equal(t, 2, fed.statusAwareCalls, "named discovery must reuse its authenticated status instead of probing each peer twice")
}

func TestFederationAvailableIncludesOnlyValidatedCallerScopedLinkedContacts(t *testing.T) {
	newServer := func(t *testing.T, contact federation.PipeContact) (*Server, *store.BadgerStore, *linkedLookupFederation) {
		t.Helper()
		srv, _, _ := newTestServer(t, "")
		badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = badger.CloseBadger() })
		srv.badgerStore = badger
		require.NoError(t, badger.SetCrossFed(
			"chain-linked", "https://redacted.invalid", []byte("peer-pin"),
			2, 0, []string{"*"}, nil, "active",
		))
		fed := &linkedLookupFederation{
			parallelStatusFederation: &parallelStatusFederation{
				fakeFederation: &fakeFederation{},
				statuses: map[string]*federation.StatusResponse{
					"chain-linked": {
						ChainID: "chain-linked", NetworkName: "Linked SAGE",
						PeerRBACGrant: &federation.PeerRBACGrant{},
					},
				},
			},
			linked: map[string]*federation.LinkedMessageDirectoryResult{
				"chain-linked": {Contacts: []federation.PipeContact{contact}},
			},
			peerHints:    map[string]bool{"chain-linked": true},
			linkedErrors: make(map[string]error),
		}
		srv.SetFederation(fed)
		return srv, badger, fed
	}

	t.Run("valid contact needs no shared memory domain", func(t *testing.T) {
		agentID := strings.Repeat("a", 64)
		contact := federation.PipeContact{
			AgentID: agentID, DisplayName: "Peer Guest",
			RegisteredName: "mynah/peer-guest", Provider: "mynah",
			Address:           agentID + "@chain-linked",
			AuthorizationMode: federation.LinkedMessageAuthorizationMode,
			Available:         false, Accepting: false,
			Domains: []federation.PipeContactDomain{},
		}
		srv, badger, fed := newServer(t, contact)
		req, callerID := signedRequest(
			t, http.MethodGet,
			"/v1/federation/available?agent_name=peer%20guest", nil,
		)
		require.NoError(t, badger.RegisterAgent(
			callerID, "ordinary", "member", "", "test", "", 1,
		))
		rr := httptest.NewRecorder()
		srv.Router().ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var response struct {
			Connections []availableFederationConnection `json:"connections"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
		require.Len(t, response.Connections, 1)
		connection := response.Connections[0]
		require.Empty(t, connection.RemotePermissions)
		require.Empty(t, connection.SharedReadDomains)
		require.Len(t, connection.RemoteAgents, 1)
		require.Equal(t, contact, connection.RemoteAgents[0])
		fed.mu.Lock()
		defer fed.mu.Unlock()
		require.Equal(t, callerID, fed.callerID)
		require.Equal(t, "peer guest", fed.name)
		require.Equal(t, 1, fed.calls)
	})

	t.Run("malformed contact is non-enumerating", func(t *testing.T) {
		agentID := strings.Repeat("b", 64)
		contact := federation.PipeContact{
			AgentID: agentID, DisplayName: "Peer Guest",
			Address:           "wrong-address",
			AuthorizationMode: federation.LinkedMessageAuthorizationMode,
			Available:         false, Accepting: false,
			Domains: []federation.PipeContactDomain{},
		}
		srv, badger, _ := newServer(t, contact)
		req, callerID := signedRequest(
			t, http.MethodGet,
			"/v1/federation/available?agent_name=peer", nil,
		)
		require.NoError(t, badger.RegisterAgent(
			callerID, "ordinary", "member", "", "test", "", 1,
		))
		rr := httptest.NewRecorder()
		srv.Router().ServeHTTP(rr, req)
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
		var response struct {
			Connections []availableFederationConnection `json:"connections"`
			Total       int                             `json:"total"`
		}
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
		require.Empty(t, response.Connections)
		require.Zero(t, response.Total)
	})
}

func TestFederationAvailableUsesLocalLinkedPeerHintWithoutDirectoryWalk(t *testing.T) {
	newServer := func(t *testing.T, hint bool) (*Server, *store.BadgerStore, *linkedLookupFederation) {
		t.Helper()
		srv, _, _ := newTestServer(t, "")
		badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = badger.CloseBadger() })
		srv.badgerStore = badger
		require.NoError(t, badger.SetCrossFed(
			"chain-linked", "https://redacted.invalid", []byte("peer-pin"),
			2, 0, []string{"secret"}, nil, "active",
		))
		remoteID := strings.Repeat("e", 64)
		fed := &linkedLookupFederation{
			parallelStatusFederation: &parallelStatusFederation{
				fakeFederation: &fakeFederation{},
				statuses: map[string]*federation.StatusResponse{
					"chain-linked": {
						ChainID: "chain-linked", NetworkName: "Linked SAGE",
						PeerRBACGrant: &federation.PeerRBACGrant{},
					},
				},
			},
			peerHints: map[string]bool{"chain-linked": hint},
			linked: map[string]*federation.LinkedMessageDirectoryResult{
				"chain-linked": {Contacts: []federation.PipeContact{{
					AgentID: remoteID, DisplayName: "Linked Only",
					Address:           remoteID + "@chain-linked",
					AuthorizationMode: federation.LinkedMessageAuthorizationMode,
				}}},
			},
			linkedErrors: map[string]error{},
		}
		srv.SetFederation(fed)
		return srv, badger, fed
	}

	for _, tc := range []struct {
		name, query string
		hint        bool
		wantPeers   int
		wantLookups int
	}{
		{name: "linked edge admits one bounded peer", query: "linked", hint: true, wantPeers: 1, wantLookups: 1},
		{name: "absent edge probes no peer", query: "linked", hint: false, wantPeers: 0, wantLookups: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, badger, fed := newServer(t, tc.hint)
			req, callerID := signedRequest(t, http.MethodGet,
				"/v1/federation/available?agent_name="+tc.query, nil)
			require.NoError(t, badger.RegisterAgent(
				callerID, "ordinary", "member", "", "test", "", 1,
			))
			require.NoError(t, badger.SetAgentPermission(
				callerID, 1, `[{"domain":"research","read":true}]`, "*", "", "",
			))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			var response struct {
				Connections []availableFederationConnection `json:"connections"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
			require.Len(t, response.Connections, tc.wantPeers)
			fed.mu.Lock()
			defer fed.mu.Unlock()
			require.Equal(t, 1, fed.hintCalls,
				"one active peer costs one local authority check")
			require.Equal(t, tc.wantLookups, fed.calls,
				"the handler never loops over a remote directory")
		})
	}
}

func TestFederationAvailableEnumeratesOnlyCapabilityNegotiatedLinkedContacts(t *testing.T) {
	newServer := func(t *testing.T, advertise bool) (*Server, *store.BadgerStore, *linkedLookupFederation) {
		t.Helper()
		srv, _, _ := newTestServer(t, "")
		badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = badger.CloseBadger() })
		srv.badgerStore = badger
		require.NoError(t, badger.SetCrossFed(
			"chain-linked", "https://redacted.invalid", []byte("peer-pin"),
			2, 0, []string{"*"}, nil, "active",
		))
		capabilities := []string{}
		if advertise {
			capabilities = append(capabilities,
				federation.CapabilityLinkedMessageDirectoryEnumeration)
		}
		remoteID := strings.Repeat("d", 64)
		fed := &linkedLookupFederation{
			parallelStatusFederation: &parallelStatusFederation{
				fakeFederation: &fakeFederation{},
				statuses: map[string]*federation.StatusResponse{
					"chain-linked": {
						ChainID: "chain-linked", NetworkName: "Linked SAGE",
						Capabilities:  capabilities,
						PeerRBACGrant: &federation.PeerRBACGrant{},
					},
				},
			},
			linked: map[string]*federation.LinkedMessageDirectoryResult{
				"chain-linked": {Contacts: []federation.PipeContact{{
					AgentID: remoteID, DisplayName: "Peer Guest",
					RegisteredName: "mynah/peer-guest", Provider: "mynah",
					Address:           remoteID + "@chain-linked",
					AuthorizationMode: federation.LinkedMessageAuthorizationMode,
				}}},
			},
			peerHints:    map[string]bool{"chain-linked": true},
			linkedErrors: map[string]error{},
		}
		srv.SetFederation(fed)
		return srv, badger, fed
	}

	for _, tc := range []struct {
		name         string
		advertise    bool
		wantContacts int
		wantCalls    int
	}{
		{name: "new peer enumerates", advertise: true, wantContacts: 1, wantCalls: 1},
		{name: "old peer omitted", advertise: false, wantContacts: 0, wantCalls: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, badger, fed := newServer(t, tc.advertise)
			req, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
			require.NoError(t, badger.RegisterAgent(
				callerID, "ordinary", "member", "", "test", "", 1,
			))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			var response struct {
				Connections []availableFederationConnection `json:"connections"`
			}
			require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
			contacts := 0
			for _, connection := range response.Connections {
				contacts += len(connection.RemoteAgents)
			}
			require.Equal(t, tc.wantContacts, contacts)
			fed.mu.Lock()
			defer fed.mu.Unlock()
			require.Equal(t, tc.wantCalls, fed.calls)
		})
	}
}

func TestFederationAvailableDiscoveryHonorsFederatedPipeDeny(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })
	require.NoError(t, badger.SetCrossFed(
		"chain-peer", "https://redacted.invalid", []byte("peer-pin"),
		2, 0, []string{"research"}, nil, "active",
	))
	fed := &parallelStatusFederation{
		fakeFederation: &fakeFederation{},
		statuses: map[string]*federation.StatusResponse{
			"chain-peer": {
				ChainID: "chain-peer", NetworkName: "Peer SAGE",
				PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{{
					Domain: "research", Read: true,
				}}},
				PipeContacts: &federation.PipeContactGrant{
					Version: federation.PipeContactVersion, AgreementID: strings.Repeat("a", 64),
					Revision: strings.Repeat("b", 64), Contacts: []federation.PipeContact{{
						AgentID: strings.Repeat("c", 64), ContactID: strings.Repeat("d", 64),
						Address: strings.Repeat("c", 64) + "@chain-peer", Handle: "#peer/cccccccc",
						DisplayName: "MYNAH (Remote)", Available: true, Accepting: true,
						Domains: []federation.PipeContactDomain{{Domain: "research"}},
					}},
				},
			},
		},
	}
	srv.SetFederation(fed)

	req, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available?agent_name=mynah", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "restricted", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermissionWithCapabilities(
		callerID, 1, `[{"domain":"research","read":true}]`, "*", "", "",
		store.AgentCapabilityDenyFederatedPipe,
	))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var response struct {
		Connections []availableFederationConnection `json:"connections"`
		Total       int                             `json:"total"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	assert.Empty(t, response.Connections)
	assert.Zero(t, response.Total)
	fed.mu.Lock()
	statusCalls := fed.calls
	fed.mu.Unlock()
	assert.Zero(t, statusCalls, "the deny bit must stop recipient discovery before any peer probe")

	// The general federation view remains useful for domain/topology discovery,
	// but the same denied caller must not receive its embedded contact roster.
	generalReq := httptest.NewRequest(http.MethodGet, "/v1/federation/available", nil)
	generalReq = generalReq.WithContext(middleware.WithAgentID(generalReq.Context(), callerID))
	generalRR := httptest.NewRecorder()
	srv.handleFederationAvailable(generalRR, generalReq)
	require.Equal(t, http.StatusOK, generalRR.Code, generalRR.Body.String())
	require.NoError(t, json.Unmarshal(generalRR.Body.Bytes(), &response))
	require.Len(t, response.Connections, 1)
	connection := response.Connections[0]
	assert.Equal(t, "chain-peer", connection.RemoteChainID)
	assert.Equal(t, "Peer SAGE", connection.NetworkName)
	assert.True(t, connection.Reachable)
	assert.Equal(t, []string{"research"}, connection.SharedReadDomains)
	assert.NotEmpty(t, connection.RemotePermissions)
	assert.Empty(t, connection.RemoteAgents,
		"the no-name topology view must not expose recipients while federated pipe is denied")
	assert.False(t, connection.RemoteAgentsTruncated)
}

func TestFederationCallerACLMatchesLocalDomainPolicy(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	_, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 2))

	require.NoError(t, badger.SetAgentPermission(callerID, 2, `[]`, "*", "", ""))
	allowed, _ := srv.federationCallerCanRead(context.Background(), callerID, "sage-autoresearch-benchmark")
	assert.True(t, allowed, "empty JSON policy means unrestricted just like local checkDomainAccess")
	assert.Equal(t, []string{"sage-autoresearch-benchmark"},
		srv.federationVisibleRemoteScopes(context.Background(), callerID, "sage-autoresearch-benchmark"))

	require.NoError(t, badger.SetAgentPermission(callerID, 2,
		`[{"domain":"finance","read":true}]`, "*", "", ""))
	require.NoError(t, badger.SetAccessGrant("sage-autoresearch-benchmark", callerID, 1, 0, "owner"))
	allowed, _ = srv.federationCallerCanRead(context.Background(), callerID, "sage-autoresearch-benchmark")
	assert.False(t, allowed, "a direct grant must not bypass an explicit local DomainAccess deny")
	assert.Error(t, checkDomainAccess(context.Background(), nil, badger, callerID, "sage-autoresearch-benchmark", "read"))

	require.NoError(t, badger.SetAgentPermission(callerID, 2, `{broken`, "*", "", ""))
	allowed, _ = srv.federationCallerCanRead(context.Background(), callerID, "finance")
	assert.False(t, allowed, "malformed registered policy must fail closed")
	assert.Error(t, checkDomainAccess(context.Background(), nil, badger, callerID, "finance", "read"))
}

func TestFederatedContactAuthorizationRechecksCurrentLocalACL(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	srv.SetPostV22ForNextTxAccessor(func() bool { return true })
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	const callerID = "caller-agent"
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 2))
	require.NoError(t, badger.SetCrossFed("chain-innovium", "https://redacted.invalid", []byte("peer-pin"), 4, 0, []string{"*"}, nil, "active"))
	require.NoError(t, badger.SetAgentPermission(callerID, 2,
		`[{"domain":"research","read":true}]`, "*", "", ""))

	request := func() *httptest.ResponseRecorder {
		body := []byte(`{"contacts":[{"remote_chain_id":"chain-innovium","domain":"research"},{"remote_chain_id":"chain-innovium","domain":"finance"}]}`)
		req := httptest.NewRequest(http.MethodPost, "/v1/federation/contacts/authorize", bytes.NewReader(body))
		req = req.WithContext(middleware.WithAgentID(req.Context(), callerID))
		rr := httptest.NewRecorder()
		srv.handleFederatedContactAuthorize(rr, req)
		return rr
	}
	first := request()
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	var response struct {
		AllowedContacts []struct {
			RemoteChainID string `json:"remote_chain_id"`
			Domain        string `json:"domain"`
		} `json:"allowed_contacts"`
	}
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &response))
	assert.Equal(t, []struct {
		RemoteChainID string `json:"remote_chain_id"`
		Domain        string `json:"domain"`
	}{{RemoteChainID: "chain-innovium", Domain: "research"}}, response.AllowedContacts)

	require.NoError(t, badger.SetAgentPermissionWithCapabilities(
		callerID, 2, `[{"domain":"research","read":true}]`, "*", "", "",
		store.AgentCapabilityDenyFederatedPipe,
	))
	denied := request()
	require.Equal(t, http.StatusOK, denied.Code, denied.Body.String())
	require.NoError(t, json.Unmarshal(denied.Body.Bytes(), &response))
	assert.Empty(t, response.AllowedContacts,
		"the federated pipe kill switch must invalidate already-cached contacts")

	require.NoError(t, badger.SetAgentPermission(callerID, 2,
		`[{"domain":"engineering","read":true}]`, "*", "", ""))
	second := request()
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &response))
	assert.Empty(t, response.AllowedContacts, "a local policy revoke must take effect without probing a peer")

	require.NoError(t, badger.SetAgentPermission(callerID, 2,
		`[{"domain":"research","read":true}]`, "*", "", ""))
	require.NoError(t, badger.UpdateCrossFedStatus("chain-innovium", "revoked"))
	third := request()
	require.Equal(t, http.StatusOK, third.Code, third.Body.String())
	require.NoError(t, json.Unmarshal(third.Body.Bytes(), &response))
	assert.Empty(t, response.AllowedContacts, "a revoked chain must not remain visible through an MCP cache hit")
}

func TestCheckDomainAccessTreatsModifyAsMonotonicReadWritePolicy(t *testing.T) {
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	const callerID = "modify-policy-agent"
	require.NoError(t, badger.RegisterAgent(callerID, "modifier", "member", "", "test", "", 2))
	require.NoError(t, badger.SetAgentPermission(
		callerID,
		2,
		`[{"domain":"research","modify":true}]`,
		"*",
		"",
		"",
	))

	assert.NoError(t, checkDomainAccess(context.Background(), nil, badger, callerID, "research", "read"))
	assert.NoError(t, checkDomainAccess(context.Background(), nil, badger, callerID, "research", "write"))
	assert.Error(t, checkDomainAccess(context.Background(), nil, badger, callerID, "finance", "read"))
}

func TestFederationAvailableMetadataIsNarrowedToCallerSubtree(t *testing.T) {
	contacts := []federation.PipeContact{{
		AgentID: "owner", ContactID: "operator-token", Accepting: true,
		Domains: []federation.PipeContactDomain{{
			Domain: "sage-autoresearch", OwningDomain: "sage", OwnerHeight: 42,
		}},
	}}
	got := filterAvailablePipeContacts(contacts, []string{"sage-autoresearch.benchmark"})
	require.Len(t, got, 1)
	assert.Empty(t, got[0].ContactID)
	require.Len(t, got[0].Domains, 1)
	assert.Equal(t, "sage-autoresearch.benchmark", got[0].Domains[0].Domain)
	assert.Empty(t, got[0].Domains[0].OwningDomain)
	assert.Zero(t, got[0].Domains[0].OwnerHeight)
}

func TestFederationAvailableDropsMalformedRemotePipeContacts(t *testing.T) {
	srv, _, _ := newTestServer(t, "")
	badger, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = badger.CloseBadger() })
	srv.badgerStore = badger
	require.NoError(t, badger.SetCrossFed("chain-peer", "https://redacted.invalid", []byte("peer-pin"), 4, 0, []string{"*"}, nil, "active"))
	fed := &fakeFederation{peerStatus: &federation.StatusResponse{
		ChainID:       "chain-peer",
		PeerRBACGrant: &federation.PeerRBACGrant{Domains: []federation.PeerRBACDomainGrant{{Domain: "research", Read: true}}},
		PipeContacts: &federation.PipeContactGrant{
			Version: federation.PipeContactVersion, AgreementID: strings.Repeat("a", 64), Revision: strings.Repeat("b", 64),
			Contacts: []federation.PipeContact{{
				AgentID: "not-a-canonical-agent", Address: "forged@chain-peer", ContactID: strings.Repeat("c", 64),
				Available: true, Accepting: true, Domains: []federation.PipeContactDomain{{Domain: "research"}},
			}},
		},
	}}
	srv.SetFederation(fed)
	req, callerID := signedRequest(t, http.MethodGet, "/v1/federation/available", nil)
	require.NoError(t, badger.RegisterAgent(callerID, "ordinary", "member", "", "test", "", 1))
	require.NoError(t, badger.SetAgentPermission(callerID, 1, `[{"domain":"research","read":true}]`, "*", "", ""))
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response struct {
		Connections []availableFederationConnection `json:"connections"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Len(t, response.Connections, 1)
	assert.Empty(t, response.Connections[0].RemoteAgents, "malformed peer metadata must not become a contactable discovery result")
}

func TestRecallWithoutOptInSkipsFederation(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "purely local",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}
	fed := &fakeFederation{}
	srv.SetFederation(fed)

	body, _ := json.Marshal(SearchMemoryRequest{Query: "purely local"})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/search", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, 0, fed.calls, "federation must not run without opt-in")
	assert.Nil(t, resp.Federation)
	for _, r := range resp.Results {
		assert.Empty(t, r.SourceChainID, "local results must carry no source_chain_id")
	}
}

func TestFederatedRecallWithoutTransportIsNoop(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	memStore.memories["local-1"] = &memory.MemoryRecord{
		MemoryID:        "local-1",
		SubmittingAgent: "agent-x",
		Content:         "no transport wired",
		ContentHash:     []byte{1},
		MemoryType:      memory.TypeFact,
		DomainTag:       "shared",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}
	// No SetFederation: a federated=true request degrades to local-only.
	body, _ := json.Marshal(HybridSearchMemoryRequest{Query: "transport", Embedding: []float32{0.1, 0.2}, Federated: true})
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/hybrid", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Nil(t, resp.Federation)
	assert.Equal(t, 1, resp.TotalCount)
}
