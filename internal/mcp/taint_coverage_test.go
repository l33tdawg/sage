package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTaintCoverageCountListAndUnknownZero(t *testing.T) {
	for _, total := range []int{0, 2} {
		t.Run(strconv.Itoa(total), func(t *testing.T) {
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "/v1/memory/list", r.URL.Path)
				_ = json.NewEncoder(w).Encode(map[string]any{
					"memories": []any{}, "total": total, "total_exact": true, "has_more": false,
					"taint_coverage": map[string]any{"complete": false, "truncated": true, "reason": "ancestor_walk_budget"},
				})
			}))
			defer ts.Close()
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			srv := NewServer(ts.URL, key)
			count, err := srv.callerScopedMemoryCountForDomain(context.Background(), "home")
			require.NoError(t, err)
			require.Equal(t, total, count["total_memories"])
			require.Equal(t, false, count["total_exact"])
			require.Equal(t, total > 0, callerStatusCountsAreReportable(count))
			listed, err := srv.toolList(context.Background(), map[string]any{"domain": "home"})
			require.NoError(t, err)
			list := listed.(map[string]any)
			require.Equal(t, false, list["total_exact"])
			for _, output := range []map[string]any{count, list} {
				wire, marshalErr := json.Marshal(output["taint_coverage"])
				require.NoError(t, marshalErr)
				require.JSONEq(t, `{"complete":false,"truncated":true,"reason":"ancestor_walk_budget"}`, string(wire))
			}
		})
	}
}

func TestTaintCoverageStatsStickyAcrossPages(t *testing.T) {
	for _, coveragePage := range []int{0, 1} {
		t.Run(strconv.Itoa(coveragePage), func(t *testing.T) {
			calls := 0
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
				require.NoError(t, err)
				require.Equal(t, calls*200, offset)
				page := calls
				calls++
				length := 200
				if page == 1 {
					length = 1
				}
				rows := make([]map[string]any, length)
				for index := range rows {
					rows[index] = map[string]any{"domain_tag": "home", "status": "committed"}
				}
				response := map[string]any{"memories": rows, "total": 201, "total_exact": true, "has_more": page == 0}
				if page == coveragePage {
					response["taint_coverage"] = map[string]any{
						"complete": false, "truncated": true, "reason": "ancestor_walk_budget",
						// Extra backend fields cannot be relayed as private metadata.
						"withheld_id": "must-not-leak", "hidden_count": 17,
					}
				}
				_ = json.NewEncoder(w).Encode(response)
			}))
			defer ts.Close()
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			stats, err := NewServer(ts.URL, key).callerScopedMemoryStats(context.Background())
			require.NoError(t, err)
			require.Equal(t, 2, calls)
			require.Equal(t, false, stats["total_exact"])
			require.Equal(t, false, stats["breakdowns_complete"])
			require.Equal(t, 201, stats["total_memories"])
			require.NotNil(t, stats["taint_coverage"])
			wire, err := json.Marshal(stats)
			require.NoError(t, err)
			require.NotContains(t, string(wire), "must-not-leak")
			require.NotContains(t, string(wire), "hidden_count")
		})
	}
}

func TestTaintCoverageRecallAndEmptyTurnRelay(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"semantic": true, "provider": "test", "dimension": 3, "ready": true})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{0.1, 0.2, 0.3}})
	})
	mux.HandleFunc("/v1/memory/query", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []any{}, "total_count": 0, "index_status": "complete",
			"taint_coverage": map[string]any{"complete": false, "truncated": true, "reason": "ancestor_walk_budget"},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	output, err := NewServer(ts.URL, key).toolRecall(context.Background(), map[string]any{"query": "serving"})
	require.NoError(t, err)
	recall := output.(map[string]any)
	require.Equal(t, "unavailable", recall["index_status"])
	require.NotNil(t, recall["taint_coverage"])

	var response recallResp
	require.NoError(t, json.Unmarshal([]byte(`{"results":[],"taint_coverage":{"complete":false,"truncated":true,"reason":"ancestor_walk_budget"}}`), &response))
	turn := map[string]any{}
	appendTurnRecallResult(turn, response, "home")
	require.Equal(t, "unavailable", turn["index_status"])
	require.NotNil(t, turn["taint_coverage"])
}

func TestTaintCoverageRejectsUnknownOrContradictoryMetadata(t *testing.T) {
	for _, raw := range []string{
		`{}`, `{"complete":true,"truncated":true,"reason":"ancestor_walk_budget"}`,
		`{"complete":false,"truncated":false,"reason":"ancestor_walk_budget"}`,
		`{"complete":false,"truncated":true,"reason":"unknown"}`,
	} {
		var coverage taintCoverage
		require.Error(t, json.Unmarshal([]byte(raw), &coverage), raw)
	}
}
