package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// outcomeTestStore is the mock store with a deterministic recall order and
// real SQLite outcome storage.
type outcomeTestStore struct {
	*mockMemoryStore
	outcomes *store.SQLiteStore
	order    []string
	origins  map[string]*store.SyncOrigin
}

func (s *outcomeTestStore) QuerySimilar(_ context.Context, _ []float32, opts store.QueryOptions) ([]*memory.MemoryRecord, error) {
	out := make([]*memory.MemoryRecord, 0, len(s.order))
	for _, id := range s.order {
		out = append(out, s.memories[id])
	}
	if opts.TopK > 0 && len(out) > opts.TopK {
		out = out[:opts.TopK]
	}
	return out, nil
}

func (s *outcomeTestStore) RecordMemoryOutcome(ctx context.Context, memoryID, agentID, outcome string, now time.Time, minInterval time.Duration) error {
	return s.outcomes.RecordMemoryOutcome(ctx, memoryID, agentID, outcome, now, minInterval)
}

func (s *outcomeTestStore) GetMemoryOutcomeStats(ctx context.Context, ids []string, perAgentCap int) (map[string]store.MemoryOutcomeStats, error) {
	return s.outcomes.GetMemoryOutcomeStats(ctx, ids, perAgentCap)
}

func (s *outcomeTestStore) GetSyncOriginByLocalMemoryID(_ context.Context, id string) (*store.SyncOrigin, error) {
	if o, ok := s.origins[id]; ok {
		return o, nil
	}
	return nil, sql.ErrNoRows
}

func newOutcomeTestServer(t *testing.T, n int) (*Server, *outcomeTestStore) {
	t.Helper()
	srv, mock, _ := newTestServer(t, "")
	ss, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "outcomes.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = ss.Close() })
	ts := &outcomeTestStore{mockMemoryStore: mock, outcomes: ss, origins: map[string]*store.SyncOrigin{}}
	for i := 0; i < n; i++ {
		id := "m" + strconv.Itoa(i)
		mock.memories[id] = &memory.MemoryRecord{
			MemoryID: id, SubmittingAgent: "agent-1", Content: "lesson " + id,
			ContentHash: []byte{byte(i)}, MemoryType: memory.TypeObservation,
			DomainTag: "maze", ConfidenceScore: 0.8, Status: memory.StatusCommitted,
			CreatedAt: time.Now().Add(-time.Hour),
		}
		ts.order = append(ts.order, id)
	}
	srv.store = ts
	return srv, ts
}

type outcomeAgent struct {
	priv ed25519.PrivateKey
	id   string
}

func newOutcomeAgent(t *testing.T) outcomeAgent {
	t.Helper()
	pub, priv, err := auth.GenerateKeypair()
	require.NoError(t, err)
	return outcomeAgent{priv: priv, id: auth.PublicKeyToAgentID(pub)}
}

func (a outcomeAgent) do(t *testing.T, srv *Server, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	require.NoError(t, err)
	ts := time.Now().Unix()
	nonce := make([]byte, 16)
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	sig := auth.SignRequestWithNonce(a.priv, method, path, raw, ts, nonce)
	req := httptest.NewRequest(method, path, bytes.NewReader(raw))
	req.Header.Set("X-Agent-ID", a.id)
	req.Header.Set("X-Signature", hex.EncodeToString(sig))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	return rr
}

func queryOrder(t *testing.T, srv *Server, a outcomeAgent) ([]string, []*MemoryResult) {
	t.Helper()
	rr := a.do(t, srv, http.MethodPost, "/v1/memory/query", QueryMemoryRequest{Embedding: []float32{0.1, 0.2}, TopK: 10})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp QueryMemoryResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	ids := make([]string, len(resp.Results))
	for i, r := range resp.Results {
		ids[i] = r.MemoryID
	}
	return ids, resp.Results
}

func seedOutcome(t *testing.T, ts *outcomeTestStore, memoryID, agent, outcome string, at time.Time) {
	t.Helper()
	require.NoError(t, ts.outcomes.RecordMemoryOutcome(context.Background(), memoryID, agent, outcome, at, outcomeMinInterval))
}

func TestMemoryOutcome_EndpointRecordsAndRateLimits(t *testing.T) {
	srv, ts := newOutcomeTestServer(t, 2)
	a := newOutcomeAgent(t)
	srv.SetNodeOperatorID(a.id) // a sees every agent's memories

	rr := a.do(t, srv, http.MethodPost, "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "helped"})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp MemoryOutcomeResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, MemoryOutcomeCounts{Helped: 1, Agents: 1}, resp.Outcomes)

	rr = a.do(t, srv, http.MethodPost, "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "hurt"})
	require.Equal(t, http.StatusTooManyRequests, rr.Code, rr.Body.String())
	require.NotEmpty(t, rr.Header().Get("Retry-After"))

	// The memory's own author is a different agent and is not limited by a.
	b := newOutcomeAgent(t)
	ts.memories["m1"].SubmittingAgent = b.id
	rr = b.do(t, srv, http.MethodPost, "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "hurt"})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// An agent that cannot read the memory cannot report on it.
	stranger := newOutcomeAgent(t)
	rr = stranger.do(t, srv, http.MethodPost, "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "hurt"})
	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())

	stats, err := ts.outcomes.GetMemoryOutcomeStats(context.Background(), []string{"m1"}, outcomePerAgentCap)
	require.NoError(t, err)
	require.Equal(t, store.MemoryOutcomeStats{Helped: 1, Hurt: 1, Agents: 2}, withoutTime(stats["m1"]))
}

func withoutTime(st store.MemoryOutcomeStats) store.MemoryOutcomeStats {
	st.LastAt = time.Time{}
	return st
}

func TestMemoryOutcome_EndpointRefusals(t *testing.T) {
	srv, ts := newOutcomeTestServer(t, 2)
	a := newOutcomeAgent(t)
	srv.SetNodeOperatorID(a.id)
	ts.origins["m0"] = &store.SyncOrigin{OriginChainID: "chain-peer", OriginMemoryID: "remote-1"}

	cases := []struct {
		name string
		path string
		body MemoryOutcomeRequest
		want int
	}{
		{"unknown outcome", "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "great"}, http.StatusBadRequest},
		{"federated result", "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "helped", SourceChainID: "chain-peer"}, http.StatusBadRequest},
		{"not on this node", "/v1/memory/nope/outcome", MemoryOutcomeRequest{Outcome: "helped"}, http.StatusNotFound},
		{"retained federated copy", "/v1/memory/m0/outcome", MemoryOutcomeRequest{Outcome: "helped"}, http.StatusConflict},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rr := a.do(t, srv, http.MethodPost, tc.path, tc.body)
			require.Equal(t, tc.want, rr.Code, rr.Body.String())
		})
	}
	stats, err := ts.outcomes.GetMemoryOutcomeStats(context.Background(), []string{"m0", "m1", "nope"}, outcomePerAgentCap)
	require.NoError(t, err)
	require.Empty(t, stats, "a refused report must not be stored")
}

func TestMemoryOutcome_UnsupportedStoreIs501(t *testing.T) {
	srv, mock, _ := newTestServer(t, "")
	a := newOutcomeAgent(t)
	mock.memories["m1"] = &memory.MemoryRecord{MemoryID: "m1", SubmittingAgent: a.id, Status: memory.StatusCommitted}
	rr := a.do(t, srv, http.MethodPost, "/v1/memory/m1/outcome", MemoryOutcomeRequest{Outcome: "helped"})
	require.Equal(t, http.StatusNotImplemented, rr.Code, rr.Body.String())
}

func TestMemoryOutcome_RecallRankingMovesWithFeedback(t *testing.T) {
	t.Setenv("SAGE_RECALL_OUTCOME_RANKING", "on")
	srv, ts := newOutcomeTestServer(t, 8)
	a := newOutcomeAgent(t)
	t0 := time.Now().Add(-time.Hour)

	ids, results := queryOrder(t, srv, a)
	require.Equal(t, []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"}, ids)
	for _, r := range results {
		require.Nil(t, r.Outcomes, "no feedback, no outcomes field")
	}

	// Strong help for m6 from several agents: it rises by the max shift.
	for i := 0; i < 4; i++ {
		agent := "helper-" + strconv.Itoa(i)
		for j := 0; j < 5; j++ {
			seedOutcome(t, ts, "m6", agent, store.MemoryOutcomeHelped, t0.Add(time.Duration(j)*time.Minute))
		}
	}
	ids, results = queryOrder(t, srv, a)
	require.Equal(t, []string{"m0", "m1", "m2", "m6", "m3", "m4", "m5", "m7"}, ids,
		"helped memory moves up exactly the max shift (3), no further")
	for _, r := range results {
		if r.MemoryID == "m6" {
			require.Equal(t, &MemoryOutcomeCounts{Helped: 20, Agents: 4}, r.Outcomes)
		}
	}

	// Strong hurt for m0: it sinks by the max shift; m6 rising past it also
	// counts, so the combined order is fixed and still bounded.
	for i := 0; i < 4; i++ {
		agent := "critic-" + strconv.Itoa(i)
		for j := 0; j < 5; j++ {
			seedOutcome(t, ts, "m0", agent, store.MemoryOutcomeHurt, t0.Add(time.Duration(j)*time.Minute))
		}
	}
	ids, _ = queryOrder(t, srv, a)
	require.Equal(t, []string{"m1", "m2", "m6", "m3", "m0", "m4", "m5", "m7"}, ids)
}

// TestMemoryOutcome_RankingIsOptIn pins the default: with the switch unset,
// blank, off or unrecognised, recall keeps the store's order exactly, while
// counts are still recorded and returned. Only an explicit on/true/1/yes
// (any case, trimmed) enables the bounded shift.
func TestMemoryOutcome_RankingIsOptIn(t *testing.T) {
	storeOrder := []string{"m0", "m1", "m2", "m3", "m4", "m5", "m6", "m7"}
	shifted := []string{"m0", "m1", "m2", "m6", "m3", "m4", "m5", "m7"}
	cases := []struct {
		name  string
		value *string
		want  []string
	}{
		{"unset", nil, storeOrder},
		{"blank", strPtr(""), storeOrder},
		{"spaces", strPtr("   "), storeOrder},
		{"off", strPtr("off"), storeOrder},
		{"false", strPtr("false"), storeOrder},
		{"0", strPtr("0"), storeOrder},
		{"garbage", strPtr("enabled-ish"), storeOrder},
		{"on", strPtr("on"), shifted},
		{"true", strPtr("true"), shifted},
		{"1", strPtr("1"), shifted},
		{"yes", strPtr("yes"), shifted},
		{"ON trimmed", strPtr("  ON "), shifted},
		{"Yes", strPtr("Yes"), shifted},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("SAGE_RECALL_OUTCOME_RANKING", "")
			if tc.value == nil {
				require.NoError(t, os.Unsetenv("SAGE_RECALL_OUTCOME_RANKING"))
			} else {
				t.Setenv("SAGE_RECALL_OUTCOME_RANKING", *tc.value)
			}
			srv, ts := newOutcomeTestServer(t, 8)
			a := newOutcomeAgent(t)
			srv.SetNodeOperatorID(a.id)
			t0 := time.Now().Add(-time.Hour)
			for i := 0; i < 4; i++ {
				agent := "helper-" + strconv.Itoa(i)
				for j := 0; j < 5; j++ {
					seedOutcome(t, ts, "m6", agent, store.MemoryOutcomeHelped, t0.Add(time.Duration(j)*time.Minute))
				}
			}

			// Recording stays on regardless of the ranking switch. (A single
			// "unused" has zero strength, so it never moves m2 by itself.)
			rr := a.do(t, srv, http.MethodPost, "/v1/memory/m2/outcome", MemoryOutcomeRequest{Outcome: "unused"})
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

			ids, results := queryOrder(t, srv, a)
			require.Equal(t, tc.want, ids)
			// Counts are returned regardless of the ranking switch.
			for _, r := range results {
				switch r.MemoryID {
				case "m6":
					require.Equal(t, &MemoryOutcomeCounts{Helped: 20, Agents: 4}, r.Outcomes)
				case "m2":
					require.Equal(t, &MemoryOutcomeCounts{Unused: 1, Agents: 1}, r.Outcomes)
				default:
					require.Nil(t, r.Outcomes)
				}
			}
		})
	}
}

func strPtr(v string) *string { return &v }

func TestMemoryOutcome_UnusedDecaysAndSingleAgentIsCapped(t *testing.T) {
	t.Setenv("SAGE_RECALL_OUTCOME_RANKING", "on")
	srv, ts := newOutcomeTestServer(t, 6)
	a := newOutcomeAgent(t)
	t0 := time.Now().Add(-24 * time.Hour)

	// One agent spamming "helped" is capped at 5: (5-0)/(5+2) -> shift 2.
	for j := 0; j < 50; j++ {
		seedOutcome(t, ts, "m5", "spammer", store.MemoryOutcomeHelped, t0.Add(time.Duration(j)*time.Minute))
	}
	ids, _ := queryOrder(t, srv, a)
	require.Equal(t, 3, indexOf(ids, "m5"))

	// Many "unused" reports from other agents dilute it back toward its place.
	for i := 0; i < 4; i++ {
		agent := "idle-" + strconv.Itoa(i)
		for j := 0; j < 5; j++ {
			seedOutcome(t, ts, "m5", agent, store.MemoryOutcomeUnused, t0.Add(time.Duration(j)*time.Minute))
		}
	}
	// (5-0)/(5+0.5*20+2) = 0.29 -> shift 1.
	ids, _ = queryOrder(t, srv, a)
	require.Equal(t, 4, indexOf(ids, "m5"))
}

func TestMemoryOutcome_ShiftIsBounded(t *testing.T) {
	for _, st := range []store.MemoryOutcomeStats{
		{Helped: 1_000_000}, {Hurt: 1_000_000}, {Helped: 3, Hurt: 3}, {}, {Unused: 1_000_000},
	} {
		s := outcomeShift(st)
		require.LessOrEqual(t, s, outcomeMaxShift)
		require.GreaterOrEqual(t, s, -outcomeMaxShift)
		str := outcomeStrength(st)
		require.Greater(t, str, -1.0)
		require.Less(t, str, 1.0)
	}
	require.Equal(t, 0, outcomeShift(store.MemoryOutcomeStats{Helped: 3, Hurt: 3}))
	require.Equal(t, 0, outcomeShift(store.MemoryOutcomeStats{}))
}

func indexOf(ids []string, id string) int {
	for i, v := range ids {
		if v == id {
			return i
		}
	}
	return -1
}
