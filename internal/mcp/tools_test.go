package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
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

	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"accepted": true,
			"votes": []map[string]any{
				{"validator": "quality_filter", "decision": "accept", "reason": "meets threshold"},
			},
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
	assert.Nil(t, m["stored"])  // no observation = nothing stored
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
