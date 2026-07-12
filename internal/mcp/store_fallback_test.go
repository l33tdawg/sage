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

// TestStoreMemory_SubmitsWithoutVectorWhenEmbedderDown guards availability: a
// failed compatibility embed still commits, while the current node queues repair.
func TestStoreMemory_SubmitsWithoutVectorWhenEmbedderDown(t *testing.T) {
	withFastBackoffs(t)

	submitCalled := false
	var submittedEmbedding any = "unset"
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"accepted": true})
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_ = json.NewEncoder(w).Encode(map[string]any{"detail": "provider down"})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		submitCalled = true
		var body map[string]any
		_ = json.NewDecoder(r.Body).Decode(&body)
		submittedEmbedding = body["embedding"]
		_ = json.NewEncoder(w).Encode(map[string]any{"memory_id": "m1", "status": "proposed", "embedding_queued": true})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	degraded, err := s.storeMemory(context.Background(), "a durable observation worth keeping", "general", "observation", 0.80)
	require.NoError(t, err, "an embedder outage must NOT drop the observation — store it without a vector")
	assert.True(t, submitCalled, "the memory must still be submitted")
	assert.Nil(t, submittedEmbedding, "embedding must be omitted/null when the embedder is down")
	assert.True(t, degraded, "the node's queued-repair response must surface degraded=true on the turn")
}

func TestStoreMemory_AttachesVectorForOlderNodeCompatibility(t *testing.T) {
	withFastBackoffs(t)

	var submitted []float64
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{0.1, 0.2, 0.3}})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Embedding []float64 `json:"embedding"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		submitted = body.Embedding
		// v11.7.3-style response: no embedding_queued capability field.
		_ = json.NewEncoder(w).Encode(map[string]any{"memory_id": "m1", "status": "proposed"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)
	degraded, err := s.storeMemory(context.Background(), "compatible observation worth preserving", "general", "observation", 0.8)
	require.NoError(t, err)
	assert.False(t, degraded)
	assert.Equal(t, []float64{0.1, 0.2, 0.3}, submitted)
}
