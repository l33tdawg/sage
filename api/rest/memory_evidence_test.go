package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// evidenceMockStore is the mock memory store plus node-local evidence.
type evidenceMockStore struct {
	*mockMemoryStore
	mu       sync.Mutex
	uploaded map[string][2]string // evidence id -> {agent, evidence}
	claimed  map[string]string    // memory id -> evidence
	claims   map[string][3]string // memory id -> {evidence id, agent, evidence}
}

func (e *evidenceMockStore) CreateMemoryEvidence(_ context.Context, agentID, evidence string) (string, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	id := "ev-" + strconv.Itoa(len(e.uploaded)+1)
	e.uploaded[id] = [2]string{agentID, evidence}
	return id, nil
}

func (e *evidenceMockStore) ClaimMemoryEvidence(_ context.Context, evidenceID, agentID, memoryID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	up, ok := e.uploaded[evidenceID]
	if !ok || up[0] != agentID {
		return store.ErrEvidenceUnavailable
	}
	delete(e.uploaded, evidenceID)
	e.claimed[memoryID] = up[1]
	e.claims[memoryID] = [3]string{evidenceID, up[0], up[1]}
	return nil
}

func (e *evidenceMockStore) ReleaseMemoryEvidence(_ context.Context, evidenceID, memoryID string) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	c, ok := e.claims[memoryID]
	if !ok || c[0] != evidenceID {
		return nil
	}
	delete(e.claims, memoryID)
	delete(e.claimed, memoryID)
	e.uploaded[evidenceID] = [2]string{c[1], c[2]}
	return nil
}

func (e *evidenceMockStore) unclaimed(evidenceID string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	_, ok := e.uploaded[evidenceID]
	return ok
}

func (e *evidenceMockStore) claimedNow() map[string]string {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make(map[string]string, len(e.claimed))
	for k, v := range e.claimed {
		out[k] = v
	}
	return out
}

func evidenceTestServer(t *testing.T, cometURL string, keepsEvidence bool) (*Server, *evidenceMockStore) {
	t.Helper()
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)
	es := &evidenceMockStore{mockMemoryStore: newMockMemoryStore(), uploaded: map[string][2]string{}, claimed: map[string]string{},
		claims: map[string][3]string{}}
	var ms store.MemoryStore = es.mockMemoryStore
	if keepsEvidence {
		ms = es
	}
	return NewServer(cometURL, ms, newMockScoreStore(), nil, health, zerolog.Nop(), embedding.NewClient("", "")), es
}

// agentCall signs a request with a fixed agent key, so an upload and the
// submission that claims it come from the same agent.
func agentCall(t *testing.T, srv *Server, priv ed25519.PrivateKey, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	ts := time.Now().Unix()
	sig := auth.SignRequest(priv, http.MethodPost, path, []byte(body), ts)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader([]byte(body)))
	req.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(priv.Public().(ed25519.PublicKey)))
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	return rr
}

func uploadEvidence(t *testing.T, srv *Server, priv ed25519.PrivateKey, evidence string) string {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"evidence": evidence})
	rr := agentCall(t, srv, priv, "/v1/memory/evidence", string(body))
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	var resp UploadEvidenceResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.NotEmpty(t, resp.EvidenceID)
	return resp.EvidenceID
}

func TestSubmitMemory_EvidenceIsAttachedBeforeBroadcastAndNeverOnChain(t *testing.T) {
	const ev = "Work order 118: north gate alarm disabled today."
	var es *evidenceMockStore
	var mu sync.Mutex
	var atBroadcast map[string]string
	var sawEvidenceInTx atomic.Bool
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		atBroadcast = es.claimedNow()
		mu.Unlock()
		raw, _ := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		if bytes.Contains(raw, []byte("Work order 118")) {
			sawEvidenceInTx.Store(true)
		}
		writeCometCommitFixture(t, w, r, 0, "", 0, "memory submitted", 1)
	}))
	defer cometMock.Close()
	var srv *Server
	srv, es = evidenceTestServer(t, cometMock.URL, true)
	_, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)

	id := uploadEvidence(t, srv, priv, ev)
	rr := agentCall(t, srv, priv, "/v1/memory/submit", `{"content":"The north gate alarm is disabled.","memory_type":"fact",
		"domain_tag":"site","confidence_score":0.9,"evidence_id":"`+id+`"}`)
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	require.False(t, sawEvidenceInTx.Load(), "the evidence text never enters the transaction")
	mu.Lock()
	defer mu.Unlock()
	require.Len(t, atBroadcast, 1, "evidence is attached before the transaction is broadcast")
	for _, got := range atBroadcast {
		require.Equal(t, ev, got)
	}
}

func TestSubmitMemory_EvidenceIsRefusedNeverDropped(t *testing.T) {
	var broadcasts atomic.Int64
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcasts.Add(1)
		writeCometCommitFixture(t, w, r, 0, "", 0, "memory submitted", 1)
	}))
	defer cometMock.Close()
	_, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	_, other, err := auth.GenerateKeypair()
	require.NoError(t, err)

	plain, _ := evidenceTestServer(t, cometMock.URL, false)
	// Every signed body in this test is distinct: identical bodies signed by
	// the same key within one second are the same signature, which the replay
	// guard correctly refuses.
	rr := agentCall(t, plain, priv, "/v1/memory/evidence", `{"evidence":"Datasheet rev B: rated power 40 kW."}`)
	require.Equal(t, http.StatusNotImplemented, rr.Code, "a store that cannot keep evidence refuses it")
	rr = agentCall(t, plain, priv, "/v1/memory/submit", `{"content":"The pump is rated 40 kW.","memory_type":"fact",
		"domain_tag":"site","confidence_score":0.9,"evidence_id":"ev-1"}`)
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "Evidence not supported")

	keeps, _ := evidenceTestServer(t, cometMock.URL, true)
	rr = agentCall(t, keeps, priv, "/v1/memory/evidence", `{"evidence":"`+strings.Repeat("e", store.MaxEvidenceBytes+1)+`"}`)
	require.Equal(t, http.StatusBadRequest, rr.Code)

	id := uploadEvidence(t, keeps, priv, "Datasheet: rated power 40 kW.")
	rr = agentCall(t, keeps, other, "/v1/memory/submit", `{"content":"The pump is rated 40 kW.","memory_type":"fact",
		"domain_tag":"site","confidence_score":0.9,"evidence_id":"`+id+`"}`)
	require.Equal(t, http.StatusBadRequest, rr.Code, "another agent cannot claim this agent's evidence")
	require.Contains(t, rr.Body.String(), "Invalid evidence_id")

	rr = agentCall(t, keeps, priv, "/v1/memory/submit", `{"content":"Check the pump","memory_type":"task","domain_tag":"site",
		"confidence_score":0.9,"task_status":"planned","evidence_id":"`+id+`"}`)
	require.Equal(t, http.StatusBadRequest, rr.Code)
	require.Contains(t, rr.Body.String(), "task")
	require.Zero(t, broadcasts.Load(), "a refused submission is never broadcast")
}

// fenceSigner holds sk's signing fence the way an indeterminate broadcast
// does, and lifts it through its own API at the end of the test.
func fenceSigner(t *testing.T, sk ed25519.PrivateKey) (lift func()) {
	t.Helper()
	encoded := []byte("indeterminate-bytes-for-evidence-retry-test-" + hex.EncodeToString(sk.Public().(ed25519.PublicKey)[:4]))
	require.ErrorIs(t, tx.WithNonceLease(context.Background(), sk, func(uint64) error {
		return tx.Indeterminate(errors.New("connection reset"), encoded,
			func(context.Context, []byte) (tx.TxOutcome, error) {
				return tx.TxOutcome{Verdict: tx.TxVerdictUnresolved}, nil
			})
	}), tx.ErrSubmitIndeterminate)
	hash := tx.CometTxHash(encoded)
	lifted := false
	lift = func() {
		if lifted {
			return
		}
		lifted = true
		require.NoError(t, tx.LiftFenceWithProof(context.Background(),
			hex.EncodeToString(sk.Public().(ed25519.PublicKey)), tx.FenceLiftProof{
				Kind: "committed", TxHash: strings.ToUpper(hex.EncodeToString(hash[:])), Detail: "test",
			}))
	}
	t.Cleanup(func() {
		if !lifted {
			lifted = true
			_ = tx.LiftFenceWithProof(context.Background(), hex.EncodeToString(sk.Public().(ed25519.PublicKey)),
				tx.FenceLiftProof{Kind: "committed", TxHash: strings.ToUpper(hex.EncodeToString(hash[:])), Detail: "test teardown"})
		}
	})
	return lift
}

func TestSubmitMemory_EvidenceSurvivesARefusalBeforeSigning(t *testing.T) {
	var broadcasts atomic.Int64
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcasts.Add(1)
		writeCometCommitFixture(t, w, r, 0, "", 0, "memory submitted", 1)
	}))
	defer cometMock.Close()
	srv, es := evidenceTestServer(t, cometMock.URL, true)
	_, nodeKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	srv.signingKey = nodeKey
	lift := fenceSigner(t, nodeKey)

	_, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	id := uploadEvidence(t, srv, priv, "Work order 118: north gate alarm disabled today.")
	body := `{"content":"The north gate alarm was disabled under work order 118.","memory_type":"fact",
		"domain_tag":"site","confidence_score":0.9,"evidence_id":"` + id + `"}`

	rr := agentCall(t, srv, priv, "/v1/memory/submit", body)
	require.Equal(t, http.StatusServiceUnavailable, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "Nothing was signed or sent")
	require.Zero(t, broadcasts.Load())
	require.True(t, es.unclaimed(id), "nothing was sent, so the claim is released for a retry")
	require.Empty(t, es.claimedNow())

	lift()
	time.Sleep(1100 * time.Millisecond) // a fresh signature for the same payload
	rr = agentCall(t, srv, priv, "/v1/memory/submit", body)
	require.Equal(t, http.StatusCreated, rr.Code, "the retry claims the same evidence: %s", rr.Body.String())
	var resp SubmitMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, "Work order 118: north gate alarm disabled today.", es.claimedNow()[resp.MemoryID])
}

func TestSubmitMemory_IndeterminateSubmissionKeepsItsEvidence(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "broadcast_tx_commit") {
			writeRPCErrorEnvelope(t, w, "Internal error", "tx not found")
			return
		}
		writeRPCErrorEnvelope(t, w, "Internal error", "timed out waiting for tx to be included in a block")
	}))
	defer cometMock.Close()
	srv, es := evidenceTestServer(t, cometMock.URL, true)
	_, nodeKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	srv.signingKey = nodeKey // this test's indeterminate outcome fences only its own key

	_, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	id := uploadEvidence(t, srv, priv, "Datasheet rev C: rated power 40 kW.")
	rr := agentCall(t, srv, priv, "/v1/memory/submit", `{"content":"The pump is rated 40 kW.","memory_type":"fact",
		"domain_tag":"site","confidence_score":0.9,"evidence_id":"`+id+`"}`)
	require.Equal(t, http.StatusAccepted, rr.Code, rr.Body.String())
	var resp IndeterminateSubmissionResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, "indeterminate", resp.Status)
	require.False(t, es.unclaimed(id), "the transaction may still commit: its memory keeps the evidence")
	require.Len(t, es.claimedNow(), 1)
}

// rbacEvidenceStore adds node-local evidence to the app-v23 route fixture's
// store.
type rbacEvidenceStore struct {
	*rbacMockMemoryStore
	ev *evidenceMockStore
}

func (e *rbacEvidenceStore) CreateMemoryEvidence(ctx context.Context, agentID, evidence string) (string, error) {
	return e.ev.CreateMemoryEvidence(ctx, agentID, evidence)
}
func (e *rbacEvidenceStore) ClaimMemoryEvidence(ctx context.Context, evidenceID, agentID, memoryID string) error {
	return e.ev.ClaimMemoryEvidence(ctx, evidenceID, agentID, memoryID)
}
func (e *rbacEvidenceStore) ReleaseMemoryEvidence(ctx context.Context, evidenceID, memoryID string) error {
	return e.ev.ReleaseMemoryEvidence(ctx, evidenceID, memoryID)
}

func TestUploadEvidence_RequiresAnActiveAgentAboveAppV23(t *testing.T) {
	fixture := newAppV23RESTRouteFixture(t)
	ev := &evidenceMockStore{mockMemoryStore: newMockMemoryStore(), uploaded: map[string][2]string{},
		claimed: map[string]string{}, claims: map[string][3]string{}}
	fixture.server.store = &rbacEvidenceStore{rbacMockMemoryStore: fixture.memories, ev: ev}

	unknownPub, unknownKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	fixture.keys["unknown"], fixture.ids["unknown"] = unknownKey, auth.PublicKeyToAgentID(unknownPub)
	pendingPub, pendingKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	fixture.keys["pending"], fixture.ids["pending"] = pendingKey, auth.PublicKeyToAgentID(pendingPub)
	require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(
		fixture.ids["pending"], "pending", store.AppV23RoleMember, "", "test", "", 2, 0)) // registered, never approved

	for _, tc := range []struct {
		actor string
		local bool
		want  int
	}{
		{"unknown", false, http.StatusForbidden},
		{"pending", false, http.StatusForbidden},
		{"inactive", false, http.StatusForbidden},
		{"current-root", true, http.StatusForbidden},
		{"member", false, http.StatusCreated},
		{"current-admin", true, http.StatusCreated},
	} {
		t.Run(tc.actor, func(t *testing.T) {
			body := []byte(`{"evidence":"Datasheet for ` + tc.actor + `: rated power 40 kW."}`)
			req := appV23SignedRESTRouteRequest(t, fixture, tc.actor, http.MethodPost, "/v1/memory/evidence", body, tc.local)
			rr := httptest.NewRecorder()
			fixture.server.Router().ServeHTTP(rr, req)
			require.Equal(t, tc.want, rr.Code, rr.Body.String())
		})
	}
	ev.mu.Lock()
	defer ev.mu.Unlock()
	require.Len(t, ev.uploaded, 2, "only the active agents stored evidence")
}
