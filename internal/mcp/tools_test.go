package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mockSageAPI(t *testing.T) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()

	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"semantic":  false,
			"provider":  "hash",
			"dimension": 768,
			"ready":     true,
		})
	})

	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})

	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"memory_id": "mem-123",
			"status":    "proposed",
			"tx_hash":   "abc123",
		})
	})

	mockQueryResults := map[string]any{
		"results": []map[string]any{
			{
				"memory_id":        "mem-123",
				"content":          "test memory",
				"domain_tag":       "general",
				"confidence_score": 0.9,
				"memory_type":      "observation",
				"status":           "committed",
				"created_at":       "2024-01-01T00:00:00Z",
			},
		},
		"total_count": 1,
	}

	mux.HandleFunc("/v1/memory/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockQueryResults)
	})

	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockQueryResults)
	})

	mux.HandleFunc("/v1/memory/hybrid", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(mockQueryResults)
	})

	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"accepted": true,
			"votes": []map[string]any{
				{"validator": "quality_filter", "decision": "accept", "reason": "meets threshold"},
			},
		})
	})

	mux.HandleFunc("/v1/memory/{id}/corroborate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{
			"message": "Corroboration recorded successfully.",
			"tx_hash": "corr-tx-456",
		})
	})

	mux.HandleFunc("/v1/memory/", func(w http.ResponseWriter, r *http.Request) {
		// Handles /v1/memory/{id}/challenge
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "challenged"})
	})

	mux.HandleFunc("/v1/memory/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"memories": []map[string]any{
				{
					"memory_id":        "mem-1",
					"content":          "listed memory",
					"domain_tag":       "general",
					"confidence_score": 0.8,
					"memory_type":      "fact",
					"status":           "committed",
					"created_at":       "2024-01-01T00:00:00Z",
				},
			},
			"total": 1,
		})
	})

	mux.HandleFunc("/v1/memory/timeline", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"buckets": []map[string]any{
				{"period": "2024-01-01", "count": 5},
				{"period": "2024-01-02", "count": 3},
			},
			"total": 8,
		})
	})

	mux.HandleFunc("/v1/dashboard/tasks", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"tasks": []map[string]any{
				{
					"memory_id":        "task-1",
					"content":          "[TASK] Build task memory type",
					"domain_tag":       "sage-architecture",
					"task_status":      "planned",
					"confidence_score": 0.9,
					"created_at":       "2024-01-01T00:00:00Z",
				},
			},
			"total": 1,
		})
	})

	mux.HandleFunc("/v1/memory/link", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "linked"})
	})

	mux.HandleFunc("/v1/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"total_memories": 42,
			"by_domain":      map[string]int{"general": 30, "security": 12},
			"by_status":      map[string]int{"committed": 40, "proposed": 2},
		})
	})

	return httptest.NewServer(mux)
}

func TestSageRemember(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolRemember(context.Background(), map[string]any{
		"content":    "test memory content",
		"domain":     "security",
		"type":       "fact",
		"confidence": 0.9,
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "mem-123", m["memory_id"])
	assert.Equal(t, "proposed", m["status"])
	assert.Equal(t, "security", m["domain"])
	assert.Equal(t, "fact", m["type"])
}

func TestSageRemember_MissingContent(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer("http://localhost:9999", priv)

	_, err := s.toolRemember(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "content is required")
}

func TestSageRecall(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolRecall(context.Background(), map[string]any{
		"query": "test query",
		"top_k": float64(5),
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	memories := m["memories"].([]map[string]any)
	assert.Len(t, memories, 1)
	assert.Equal(t, "mem-123", memories[0]["memory_id"])
	assert.Equal(t, "test memory", memories[0]["content"])
}

func TestSageRecall_MissingQuery(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer("http://localhost:9999", priv)

	_, err := s.toolRecall(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "query is required")
}

func TestSageForget(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolForget(context.Background(), map[string]any{
		"memory_id": "mem-123",
		"reason":    "outdated info",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "mem-123", m["memory_id"])
	assert.Equal(t, "challenged", m["status"])
	assert.Equal(t, "outdated info", m["reason"])
}

func TestSageForget_MissingID(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer("http://localhost:9999", priv)

	_, err := s.toolForget(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "memory_id is required")
}

func TestSageCorroborate(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolCorroborate(context.Background(), map[string]any{
		"memory_id": "mem-123",
		"evidence":  "independently observed in the upstream changelog",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "mem-123", m["memory_id"])
	assert.Equal(t, "corroborated", m["status"])
	assert.Equal(t, "corr-tx-456", m["tx_hash"])
}

func TestSageCorroborate_MissingID(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer("http://localhost:9999", priv)

	_, err := s.toolCorroborate(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "memory_id is required")
}

func TestSageList(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolList(context.Background(), map[string]any{
		"domain": "general",
		"limit":  float64(10),
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	memories := m["memories"].([]map[string]any)
	assert.Len(t, memories, 1)
	assert.EqualValues(t, 1, m["total_count"])
}

func TestSageTimeline(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolTimeline(context.Background(), map[string]any{
		"from": "2024-01-01",
		"to":   "2024-12-31",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	buckets := m["buckets"].([]map[string]any)
	assert.Len(t, buckets, 2)
	assert.EqualValues(t, 8, m["total"])
}

func TestSageStatus(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolStatus(context.Background(), map[string]any{})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, float64(42), m["total_memories"])
}

func TestSageTurn(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolTurn(context.Background(), map[string]any{
		"topic":       "debugging config path expansion",
		"observation": "Fixed ~ expansion bug in config.go — paths with ~ were being double-prefixed",
		"domain":      "go-debugging",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "debugging config path expansion", m["topic"])
	assert.Equal(t, "go-debugging", m["domain"])
	assert.True(t, m["stored"].(bool))
	assert.NotNil(t, m["recalled"])
}

func TestSageTurn_RecallOnly(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	// No observation — just recall
	result, err := s.toolTurn(context.Background(), map[string]any{
		"topic": "what do I know about SAGE architecture",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "what do I know about SAGE architecture", m["topic"])
	recalled := m["recalled"].([]map[string]any)
	assert.Len(t, recalled, 1) // mock returns 1 result
	assert.Nil(t, m["stored"]) // no observation = nothing stored
}

func TestSageTurn_MissingTopic(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer("http://localhost:9999", priv)

	_, err := s.toolTurn(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "topic is required")
}

func TestSageInception_ExistingMemories(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolInception(context.Background(), map[string]any{})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "awakened", m["status"])
	assert.Contains(t, m["instructions"], "EVERY TURN")
	assert.Contains(t, m["message"], "Welcome back")
}

func TestSageInception_FreshBrain(t *testing.T) {
	// Mock API that returns 0 total_memories
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/dashboard/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"total_memories": 0})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{0.1, 0.2, 0.3}})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"memory_id": "seed-1", "status": "proposed"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolInception(context.Background(), map[string]any{})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "inception_complete", m["status"])
	assert.EqualValues(t, 5, m["memories_seeded"])
	assert.Contains(t, m["message"], "Welcome to the real world")
}

func TestSageReflect(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolReflect(context.Background(), map[string]any{
		"task_summary": "Fixed config path expansion bug",
		"dos":          "Always expand ~ in file paths before checking IsAbs",
		"donts":        "Don't join relative paths containing ~ with a base directory",
		"domain":       "debugging",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.Equal(t, "reflected", m["status"])
	assert.EqualValues(t, 3, m["memories_stored"])
	assert.Equal(t, "Fixed config path expansion bug", m["task"])
}

func TestSageReflect_MissingSummary(t *testing.T) {
	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer("http://localhost:9999", priv)

	_, err := s.toolReflect(context.Background(), map[string]any{})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "task_summary is required")
}

func TestSageReflect_DosOnly(t *testing.T) {
	ts := mockSageAPI(t)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolReflect(context.Background(), map[string]any{
		"task_summary": "Implemented inception tool",
		"dos":          "Read the research papers for design guidance",
	})
	require.NoError(t, err)

	m := result.(map[string]any)
	assert.EqualValues(t, 2, m["memories_stored"]) // summary + dos (no don'ts)
}

func TestBootSafeguardExistsTrue(t *testing.T) {
	// Mock API returns a memory with boot protocol content in meta domain
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"memories": []map[string]any{
				{
					"content": "[DO] Always run sage_inception BEFORE any response to the user on the first message of every conversation.",
				},
			},
			"total": 1,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	assert.True(t, s.bootSafeguardExists(context.Background()))
}

func TestBootSafeguardExistsFalse(t *testing.T) {
	// Mock API returns no matching memories
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"memories": []map[string]any{},
			"total":    0,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	assert.False(t, s.bootSafeguardExists(context.Background()))
}

func TestSimilarMemoryExists(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/list", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"memories": []map[string]any{
				{
					"content": "[DO] Always expand tilde paths before checking IsAbs in Go config files",
				},
			},
			"total": 1,
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	// Substantially similar content — should match
	assert.True(t, s.similarMemoryExists(context.Background(),
		"[DO] Always expand tilde paths before checking IsAbs", "debugging"))

	// Completely different content — should not match (but this mock always returns the same list,
	// so we test a string that has <60% word overlap)
	assert.False(t, s.similarMemoryExists(context.Background(),
		"[DON'T] Never use fmt.Println for production logging in server handlers", "debugging"))
}

func TestIsLowValueObservation(t *testing.T) {
	// Short observations (< 30 chars)
	assert.True(t, isLowValueObservation("short"))
	assert.True(t, isLowValueObservation("not much to say here"))

	// Noise patterns
	assert.True(t, isLowValueObservation("The user said hi and we started chatting about things"))
	assert.True(t, isLowValueObservation("A new session started with the user asking about SAGE"))
	assert.True(t, isLowValueObservation("Brain is online and ready to work on the project today"))
	assert.True(t, isLowValueObservation("User greeted me and asked about the weather conditions today"))
	assert.True(t, isLowValueObservation("No action taken during this turn of the conversation today"))

	// Valid observations — should NOT be filtered
	assert.False(t, isLowValueObservation("Fixed ~ expansion bug in config.go — paths with ~ were being double-prefixed with home dir"))
	assert.False(t, isLowValueObservation("User wants to implement MCP quality fixes for SAGE v4.0.0 to prevent memory bloat"))
	assert.False(t, isLowValueObservation("Discovered that CometBFT v0.38 requires explicit height tracking for validator set updates"))
}

func TestStoreMemoryPreValidateReject(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"accepted": false,
			"votes": []map[string]any{
				{"validator": "quality_filter", "decision": "reject", "reason": "content too short (15 chars, minimum 20)"},
				{"validator": "sentinel", "decision": "accept", "reason": "baseline accept"},
			},
		})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	err := s.storeMemory(context.Background(), "too short", "general", "observation", 0.8)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "memory rejected by validators")
	assert.Contains(t, err.Error(), "quality_filter")
	assert.Contains(t, err.Error(), "content too short")
}

func TestStoreMemoryPreValidateAccept(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"accepted": true,
			"votes": []map[string]any{
				{"validator": "quality_filter", "decision": "accept", "reason": "content meets quality threshold"},
				{"validator": "sentinel", "decision": "accept", "reason": "baseline accept"},
			},
		})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{0.1, 0.2, 0.3}})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"memory_id": "mem-456", "status": "proposed"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	err := s.storeMemory(context.Background(), "Valid observation about Go debugging patterns", "go-debugging", "observation", 0.85)
	assert.NoError(t, err)
}

// TestSageRecall_VaultActiveForcesSemantic exercises the v6.6.10 primary fix:
// when /v1/embed/info reports semantic=true (which it now does on any
// vault-active node, regardless of whether an Ollama embedder is configured),
// toolRecall MUST take the semantic path — POST /v1/embed then
// POST /v1/memory/query — and MUST NOT fall through to /v1/memory/search,
// which on a vault-active node returns the "text search unavailable" error.
//
// This guards against future regressions where someone adds another condition
// to isSemanticMode (e.g. requiring a specific provider name) and inadvertently
// reroutes vault nodes to the broken FTS5 path.
func TestSageRecall_VaultActiveForcesSemantic(t *testing.T) {
	mux := http.NewServeMux()

	// /v1/embed/info reports semantic=true with an unusual provider —
	// the test should NOT special-case "ollama"; it should trust the
	// semantic flag (which v6.6.10 forces true when the vault is active).
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic":  true,
			"provider":  "vault-encrypted",
			"dimension": 768,
			"ready":     true,
		})
	})

	semanticPathHit := false
	ftsPathHit := false

	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		semanticPathHit = true
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})

	mux.HandleFunc("/v1/memory/query", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"memory_id":        "mem-vault-1",
				"content":          "secret recovered via semantic recall",
				"domain_tag":       "ops",
				"confidence_score": 0.91,
				"memory_type":      "fact",
				"status":           "committed",
				"created_at":       "2026-04-27T00:00:00Z",
			}},
			"total_count": 1,
		})
	})

	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		ftsPathHit = true
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title":  "Search error",
			"detail": "text search unavailable: content is vault-encrypted; this node is in semantic-only mode",
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolRecall(context.Background(), map[string]any{
		"query": "what is the secret",
	})
	require.NoError(t, err, "toolRecall must succeed via semantic path on a vault-active node")

	assert.True(t, semanticPathHit, "semantic path /v1/embed must be hit when /v1/embed/info reports semantic=true")
	assert.False(t, ftsPathHit, "FTS5 /v1/memory/search must NOT be hit when semantic=true")

	m := result.(map[string]any)
	memories := m["memories"].([]map[string]any)
	require.Len(t, memories, 1)
	assert.Equal(t, "mem-vault-1", memories[0]["memory_id"])
}

// TestSageRecall_RetriesSemanticOnVaultEncryptedFTSError exercises the v6.6.10
// belt-and-braces retry: if /v1/embed/info LIES (semantic=false) but
// /v1/memory/search reveals the truth by returning the vault-encrypted marker
// (e.g. an older node where embed_handler.go isn't patched), toolRecall must
// detect the marker substring, log a warning, and silently retry the semantic
// path with the same query and params. This protects mixed-version networks.
func TestSageRecall_RetriesSemanticOnVaultEncryptedFTSError(t *testing.T) {
	// Pin to the legacy single-index path so this test continues to assert the
	// vault-encrypted retry boundary exactly. The hybrid path is exercised by
	// TestSageRecall_HybridPath* below.
	t.Setenv("SAGE_RECALL_HYBRID", "0")
	mux := http.NewServeMux()

	// Lie: claim semantic=false even though the node is vault-active.
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic":  false,
			"provider":  "hash",
			"dimension": 768,
			"ready":     true,
		})
	})

	embedHits := 0
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		embedHits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.7, 0.8, 0.9},
		})
	})

	queryHits := 0
	mux.HandleFunc("/v1/memory/query", func(w http.ResponseWriter, r *http.Request) {
		queryHits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"memory_id":        "mem-retry-ok",
				"content":          "fetched via fallback retry",
				"domain_tag":       "ops",
				"confidence_score": 0.88,
				"memory_type":      "observation",
				"status":           "committed",
				"created_at":       "2026-04-27T00:00:00Z",
			}},
			"total_count": 1,
		})
	})

	searchHits := 0
	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		searchHits++
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title":  "Search error",
			"detail": "text search unavailable: content is vault-encrypted; this node is in semantic-only mode",
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolRecall(context.Background(), map[string]any{
		"query": "anything",
	})
	require.NoError(t, err, "toolRecall must recover via semantic retry when FTS path returns vault-encrypted marker")

	assert.Equal(t, 1, searchHits, "FTS5 path should have been tried exactly once before retry")
	assert.Equal(t, 1, embedHits, "semantic /v1/embed should have been hit by the retry")
	assert.Equal(t, 1, queryHits, "semantic /v1/memory/query should have been hit by the retry")

	m := result.(map[string]any)
	memories := m["memories"].([]map[string]any)
	require.Len(t, memories, 1)
	assert.Equal(t, "mem-retry-ok", memories[0]["memory_id"])
}

// TestSageRecall_NonVaultErrorPropagates confirms the retry only triggers for
// the specific vault-encrypted marker. Other /v1/memory/search errors (e.g.
// network 500s, validation failures) MUST NOT silently retry and mask real
// problems — they should propagate to the caller.
func TestSageRecall_NonVaultErrorPropagates(t *testing.T) {
	t.Setenv("SAGE_RECALL_HYBRID", "0")
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	embedHits := 0
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		embedHits++
	})
	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Search error", "detail": "database is locked",
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	_, err := s.toolRecall(context.Background(), map[string]any{"query": "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "database is locked",
		"non-vault errors must propagate, not silently retry")
	assert.Equal(t, 0, embedHits, "semantic retry must NOT trigger on non-vault errors")
}

// TestSageRecall_HybridPathPreferredWhenAvailable verifies that on a
// non-vault, non-semantic node the new hybrid endpoint is preferred over
// the legacy FTS5 path when the env switch is enabled (the default).
func TestSageRecall_HybridPathPreferredWhenAvailable(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})
	hybridHits := 0
	mux.HandleFunc("/v1/memory/hybrid", func(w http.ResponseWriter, r *http.Request) {
		hybridHits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"memory_id":        "mem-hybrid-ok",
				"content":          "from hybrid path",
				"domain_tag":       "general",
				"confidence_score": 0.91,
				"memory_type":      "observation",
				"status":           "committed",
				"created_at":       "2026-05-14T00:00:00Z",
			}},
			"total_count": 1,
		})
	})
	searchHits := 0
	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		searchHits++
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolRecall(context.Background(), map[string]any{"query": "anything"})
	require.NoError(t, err)
	assert.Equal(t, 1, hybridHits, "hybrid endpoint should be called once")
	assert.Equal(t, 0, searchHits, "legacy FTS5 path should NOT be hit when hybrid succeeds")

	m := result.(map[string]any)
	memories := m["memories"].([]map[string]any)
	require.Len(t, memories, 1)
	assert.Equal(t, "mem-hybrid-ok", memories[0]["memory_id"])
}

// TestSageRecall_HybridFallsBackToFTS verifies graceful degradation when an
// older node doesn't expose /v1/memory/hybrid — recall must still succeed by
// falling back to the FTS5 path automatically.
func TestSageRecall_HybridFallsBackToFTS(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})
	mux.HandleFunc("/v1/memory/hybrid", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Not Found", "detail": "/v1/memory/hybrid not registered on this node",
		})
	})
	searchHits := 0
	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		searchHits++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{{
				"memory_id":        "mem-fts-fallback",
				"content":          "from legacy FTS path",
				"domain_tag":       "general",
				"confidence_score": 0.8,
				"memory_type":      "observation",
				"status":           "committed",
				"created_at":       "2026-05-14T00:00:00Z",
			}},
			"total_count": 1,
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	result, err := s.toolRecall(context.Background(), map[string]any{"query": "anything"})
	require.NoError(t, err, "hybrid failure must fall back to FTS5, not propagate")
	assert.Equal(t, 1, searchHits, "fallback to /v1/memory/search expected")

	m := result.(map[string]any)
	memories := m["memories"].([]map[string]any)
	require.Len(t, memories, 1)
	assert.Equal(t, "mem-fts-fallback", memories[0]["memory_id"])
}

// TestToolRemember_AttachesBranchTag verifies that toolRemember auto-tags
// submitted memories with `branch:<name>` when the MCP server's working
// directory is a git checkout. The branch is detected via git, cached, and
// merged into the submission body alongside any user-supplied tags.
func TestToolRemember_AttachesBranchTag(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}

	// Build a real git repo on a known branch so currentBranchTag has
	// something to detect, then chdir into it for the duration of the test.
	resetBranchCache()
	t.Setenv("SAGE_BRANCH_TAG", "")

	tmp := t.TempDir()
	oldWd, _ := os.Getwd()
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	runGit := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = tmp
		cmd.Env = append(os.Environ(),
			"GIT_CONFIG_GLOBAL=/dev/null",
			"GIT_CONFIG_SYSTEM=/dev/null",
			"HOME="+tmp,
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	runGit("init", "-b", "feature-test-branch")
	runGit("config", "user.email", "test@example.com")
	runGit("config", "user.name", "Test")
	if err := os.WriteFile(tmp+"/f", []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	runGit("add", "f")
	runGit("commit", "-m", "init")

	// Capture the submit body so we can assert what tags the handler sent.
	var capturedTags []any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if rawTags, ok := body["tags"].([]any); ok {
			capturedTags = rawTags
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"memory_id": "mem-branch", "status": "proposed", "tx_hash": "abc",
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	_, err := s.toolRemember(context.Background(), map[string]any{
		"content": "branch-test memory",
		"domain":  "general",
		"tags":    []any{"user-supplied"},
	})
	require.NoError(t, err)

	require.NotNil(t, capturedTags, "submit handler must have received a tags array")
	stringTags := make([]string, 0, len(capturedTags))
	for _, t := range capturedTags {
		if s, ok := t.(string); ok {
			stringTags = append(stringTags, s)
		}
	}
	assert.Contains(t, stringTags, "user-supplied",
		"user-supplied tags must be preserved")
	assert.Contains(t, stringTags, "branch:feature-test-branch",
		"branch:<name> tag must be auto-attached on git-repo writes")
}

// TestToolRemember_NoBranchTagOutsideGitRepo verifies that auto-tagging
// silently no-ops when the working directory isn't a git checkout — the
// submission still succeeds, but no branch tag is appended.
func TestToolRemember_NoBranchTagOutsideGitRepo(t *testing.T) {
	resetBranchCache()
	t.Setenv("SAGE_BRANCH_TAG", "")

	tmp := t.TempDir()
	oldWd, _ := os.Getwd()
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}

	var capturedTags []any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if rawTags, ok := body["tags"].([]any); ok {
			capturedTags = rawTags
		} else {
			capturedTags = nil
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"memory_id": "mem-nobranch", "status": "proposed",
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	_, err := s.toolRemember(context.Background(), map[string]any{
		"content": "outside-repo memory",
		"domain":  "general",
	})
	require.NoError(t, err)

	for _, tag := range capturedTags {
		if s, ok := tag.(string); ok {
			assert.NotContains(t, s, "branch:",
				"no branch tag should be attached outside a git repo")
		}
	}
}

// TestToolRemember_BranchTagDisabledByEnv verifies SAGE_BRANCH_TAG=0 fully
// suppresses auto-tagging even inside a git checkout.
func TestToolRemember_BranchTagDisabledByEnv(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git not available")
	}
	resetBranchCache()
	t.Setenv("SAGE_BRANCH_TAG", "0")

	tmp := t.TempDir()
	oldWd, _ := os.Getwd()
	defer func() { _ = os.Chdir(oldWd) }()
	if err := os.Chdir(tmp); err != nil {
		t.Fatalf("chdir: %v", err)
	}
	runGit := func(args ...string) {
		t.Helper()
		cmd := exec.Command("git", args...)
		cmd.Dir = tmp
		cmd.Env = append(os.Environ(),
			"GIT_CONFIG_GLOBAL=/dev/null",
			"GIT_CONFIG_SYSTEM=/dev/null",
			"HOME="+tmp,
		)
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("git %v: %v: %s", args, err, out)
		}
	}
	runGit("init", "-b", "should-not-appear")
	runGit("config", "user.email", "test@example.com")
	runGit("config", "user.name", "Test")
	if err := os.WriteFile(tmp+"/f", []byte("x"), 0o644); err != nil {
		t.Fatalf("write: %v", err)
	}
	runGit("add", "f")
	runGit("commit", "-m", "init")

	var capturedTags []any
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": []float32{0.1, 0.2, 0.3},
		})
	})
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		if rawTags, ok := body["tags"].([]any); ok {
			capturedTags = rawTags
		} else {
			capturedTags = nil
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"memory_id": "mem-disabled", "status": "proposed",
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	_, err := s.toolRemember(context.Background(), map[string]any{
		"content": "no-tag memory",
		"domain":  "general",
	})
	require.NoError(t, err)

	for _, tag := range capturedTags {
		if s, ok := tag.(string); ok {
			assert.NotContains(t, s, "branch:",
				"SAGE_BRANCH_TAG=0 must fully suppress branch auto-tagging")
		}
	}
}

// TestSageRecall_HybridDisabledByEnv verifies the SAGE_RECALL_HYBRID=0 escape
// hatch routes straight to the legacy FTS5 path without touching the hybrid
// endpoint or the embed service.
func TestSageRecall_HybridDisabledByEnv(t *testing.T) {
	t.Setenv("SAGE_RECALL_HYBRID", "0")
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embed/info", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"semantic": false, "provider": "hash", "dimension": 768, "ready": true,
		})
	})
	embedHits := 0
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		embedHits++
	})
	hybridHits := 0
	mux.HandleFunc("/v1/memory/hybrid", func(w http.ResponseWriter, r *http.Request) {
		hybridHits++
	})
	mux.HandleFunc("/v1/memory/search", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results":     []map[string]any{},
			"total_count": 0,
		})
	})

	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	_, err := s.toolRecall(context.Background(), map[string]any{"query": "x"})
	require.NoError(t, err)
	assert.Equal(t, 0, hybridHits, "hybrid endpoint must NOT be hit when disabled")
	assert.Equal(t, 0, embedHits, "embed must NOT be hit when hybrid disabled and FTS path chosen")
}
