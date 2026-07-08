package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// scriptedComet serves the CometBFT broadcast_tx_commit JSON shape. Each call pops
// the next scripted response; the last one repeats.
type scriptedComet struct {
	calls     atomic.Int32
	responses []string
}

func (f *scriptedComet) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		n := int(f.calls.Add(1)) - 1
		if n >= len(f.responses) {
			n = len(f.responses) - 1
		}
		_, _ = w.Write([]byte(f.responses[n]))
	}
}

const cometOK = `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"AB12","height":"7"}}`

func cometReject(code int, log string) string {
	b, _ := json.Marshal(log)
	return fmt.Sprintf(`{"result":{"check_tx":{"code":0},"tx_result":{"code":%d,"log":%s}}}`, code, string(b))
}

// newSyncTestManager builds a Manager wired to an in-memory SQLite store and
// a scripted CometBFT RPC. Same-package test, so unexported fields are fair game.
func newSyncTestManager(t *testing.T, comet *scriptedComet) (*Manager, *store.SQLiteStore) {
	t.Helper()
	ms, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sync.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ms.Close() })

	rpc := httptest.NewServer(comet.handler())
	t.Cleanup(rpc.Close)

	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	return &Manager{
		localChainID: "chain-local",
		cometRPC:     rpc.URL,
		agentKey:     priv,
		agentPub:     pub,
		memStore:     ms,
		logger:       zerolog.Nop(),
		broadcastSem: make(chan struct{}, 4),
	}, ms
}

// pushAs performs a sync push with an injected authenticated peer identity
// (peerAuth is exercised by the transport tests; these tests target the gates).
func pushAs(t *testing.T, m *Manager, peer *peerIdentity, req SyncPushRequest) (*httptest.ResponseRecorder, *SyncPushResponse) {
	t.Helper()
	body, err := json.Marshal(req)
	require.NoError(t, err)
	httpReq := httptest.NewRequest(http.MethodPost, "/fed/v1/sync/push", bytes.NewReader(body))
	httpReq = httpReq.WithContext(context.WithValue(httpReq.Context(), peerCtxKey{}, peer))
	rr := httptest.NewRecorder()
	m.handleSyncPush(rr, httpReq)
	if rr.Code != http.StatusOK {
		return rr, nil
	}
	var resp SyncPushResponse
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&resp))
	return rr, &resp
}

func testPeer(maxClearance int, allowed ...string) *peerIdentity {
	return &peerIdentity{
		ChainID: "chain-b",
		AgentID: "peer-agent",
		Agreement: &store.CrossFedRecord{
			RemoteChainID:  "chain-b",
			MaxClearance:   uint8(maxClearance), // #nosec G115 -- test values 0-4
			AllowedDomains: allowed,
			Status:         "active",
		},
	}
}

func syncItem(originID, domain, content string) SyncItem {
	sum := sha256.Sum256([]byte(content))
	return SyncItem{
		OriginChainID:  "chain-b",
		OriginMemoryID: originID,
		Domain:         domain,
		Classification: 1,
		Content:        content,
		ContentHash:    hex.EncodeToString(sum[:]),
	}
}

func TestSyncPushGateOrder(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "hr", "eng")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	outsideTreaty := syncItem("m-scope", "finance", "finance fact")
	notConsented := syncItem("m-consent", "eng", "eng fact") // in treaty, not in sync_domains
	tooHigh := syncItem("m-clear", "hr", "secret fact")
	tooHigh.Classification = 4 // above MaxClearance 2
	happy := syncItem("m-ok", "hr.public", "shared hr fact") // consented via subtree

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{outsideTreaty, notConsented, tooHigh, happy}})
	require.NotNil(t, resp)
	require.Len(t, resp.Results, 4)
	assert.Equal(t, SyncOutcomeRejectedScope, resp.Results[0].Outcome)
	assert.Equal(t, SyncOutcomeRejectedConsent, resp.Results[1].Outcome)
	assert.Equal(t, SyncOutcomeRejectedClearance, resp.Results[2].Outcome)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[3].Outcome)
	assert.Equal(t, syncMemoryID("chain-b", "m-ok"), resp.Results[3].LocalMemoryID)

	// Every terminal decision is in the ledger; redelivery replays it without
	// re-running gates or re-broadcasting.
	broadcastsBefore := comet.calls.Load()
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{outsideTreaty, happy}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedScope, resp.Results[0].Outcome)
	assert.Equal(t, SyncOutcomeDuplicate, resp.Results[1].Outcome)
	assert.Equal(t, syncMemoryID("chain-b", "m-ok"), resp.Results[1].LocalMemoryID)
	assert.Equal(t, broadcastsBefore, comet.calls.Load(), "replay must not broadcast")
}

func TestSyncPushStructuralViolations(t *testing.T) {
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "*")
	require.NoError(t, ms.SetSyncDomains(context.Background(), "chain-b", []string{"hr"}))

	// Origin chain != authenticated peer -> whole batch 400 (no laundering).
	forged := syncItem("m-1", "hr", "content")
	forged.OriginChainID = "chain-c"
	rr, _ := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{forged}})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Content-hash mismatch -> 400.
	tampered := syncItem("m-2", "hr", "content")
	tampered.Content = "tampered content"
	rr, _ = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{tampered}})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Batch bigger than SyncPushMaxItems -> 400.
	big := make([]SyncItem, SyncPushMaxItems+1)
	for i := range big {
		big[i] = syncItem(fmt.Sprintf("m-%d", i), "hr", fmt.Sprintf("content %d", i))
	}
	rr, _ = pushAs(t, m, peer, SyncPushRequest{Items: big})
	assert.Equal(t, http.StatusBadRequest, rr.Code)

	// Zero broadcasts happened: structural rejects never reach consensus.
	assert.EqualValues(t, 0, comet.calls.Load())
}

func TestSyncPushCrossDomainDup(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{cometOK}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "*")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr", "eng"}))

	// Seed the same content committed locally in domain "eng".
	content := "identical shared fact"
	sum := sha256.Sum256([]byte(content))
	require.NoError(t, seedCommittedMemory(ctx, ms, "local-eng", "eng", content, sum[:]))

	// Pushed into a DIFFERENT domain -> B-D1 terminal reject.
	item := syncItem("m-dup", "hr", content)
	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{item}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeRejectedXDomainDup, resp.Results[0].Outcome)

	// Pushed into the SAME domain -> duplicate success mapped to existing row.
	same := syncItem("m-same", "eng", content)
	_, resp = pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{same}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeDuplicate, resp.Results[0].Outcome)
	assert.Equal(t, "local-eng", resp.Results[0].LocalMemoryID)

	// Neither path broadcast anything.
	assert.EqualValues(t, 0, comet.calls.Load())
}

func TestSyncPushNonceRaceRetriesOnce(t *testing.T) {
	ctx := context.Background()
	comet := &scriptedComet{responses: []string{
		cometReject(4, "nonce too low: got 5, expected > 6"),
		cometOK,
	}}
	m, ms := newSyncTestManager(t, comet)
	peer := testPeer(2, "*")
	require.NoError(t, ms.SetSyncDomains(ctx, "chain-b", []string{"hr"}))

	_, resp := pushAs(t, m, peer, SyncPushRequest{Items: []SyncItem{syncItem("m-n", "hr", "raced fact")}})
	require.NotNil(t, resp)
	assert.Equal(t, SyncOutcomeAccepted, resp.Results[0].Outcome)
	assert.EqualValues(t, 2, comet.calls.Load(), "one nonce retry with a fresh tx")
}

func TestClassifySyncBroadcast(t *testing.T) {
	// Pins the Log-text soft contract with internal/abci/app.go. If a wording
	// change over there breaks these, update BOTH sides deliberately.
	cases := []struct {
		log  string
		code int
		want syncBcastClass
	}{
		{`memory abc already reached terminal status "committed"; re-submit rejected`, 11, syncBcastDuplicate},
		{"memory abc is a co-committed id and cannot be overwritten by a normal submit", 11, syncBcastDuplicate},
		{"access denied: agent 1234 has no write access to domain hr", 11, syncBcastScopeReject},
		{"nonce too low: got 3, expected > 4", 4, syncBcastNonceRace},
		{"nonce 0 not permitted", 4, syncBcastNonceRace},
		{"memory submit rejected: a non-empty domain_tag is required (app-v16)", 11, syncBcastRetry},
	}
	for _, c := range cases {
		err := fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", c.code, c.log)
		assert.Equal(t, c.want, classifySyncBroadcast(err), c.log)
	}
	assert.Equal(t, syncBcastOK, classifySyncBroadcast(nil))
}

// seedCommittedMemory inserts a committed row via the ordinary store path
// (the B-D1 gate reads the SQLite mirror's committed set).
func seedCommittedMemory(ctx context.Context, ms *store.SQLiteStore, id, domain, content string, hash []byte) error {
	return ms.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: "seed-agent",
		Content:         content,
		ContentHash:     hash,
		MemoryType:      memory.TypeFact,
		DomainTag:       domain,
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	})
}
