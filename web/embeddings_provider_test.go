package web

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddingProviderEndpointPersistsHashAndRequestsRestart(t *testing.T) {
	h, _ := newTestHandler(t)
	var selected string
	restarted := false
	h.SetEmbeddingProvider = func(provider string) error { selected = provider; return nil }
	h.RequestRestart = func() error { restarted = true; return nil }

	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/embeddings/provider",
		bytes.NewBufferString(`{"provider":"hash"}`))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()
	testRouter(h).ServeHTTP(rr, req)

	require.Equal(t, http.StatusAccepted, rr.Code, rr.Body.String())
	require.Equal(t, "hash", selected)
	require.True(t, restarted)
}

func TestStartEmbeddingRepairMigratesToActiveHashSpace(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestMemory(t, s, "one", "test")
	insertTestMemory(t, s, "two", "test")
	h.SetEmbedder(hashOnlyEmbedder{})
	// Keep the lifecycle hook deterministic for the test.
	h.RunBackground = func(fn func(context.Context)) { fn(context.Background()) }

	started, err := h.StartEmbeddingRepairIfNeeded(context.Background())
	require.NoError(t, err)
	require.True(t, started)
	counts, err := s.CountMemoriesByProvider(context.Background())
	require.NoError(t, err)
	require.Equal(t, 2, counts["hash"])
	require.Zero(t, counts[""])
}

func TestEmbeddingStatusUsesActiveCustomSemanticSpace(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestMemory(t, s, "custom", "test")
	embedder := &fakeEmbedder{
		name: "ollama", model: "bge-m3", dimension: 3,
		ready: true, semantic: true,
	}
	h.SetEmbedder(embedder)
	require.NoError(t, s.UpdateMemoryEmbedding(context.Background(), "custom", []float32{0.1, 0.2, 0.3}, "ollama:bge-m3:3"))

	rr := httptest.NewRecorder()
	testRouter(h).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/embeddings/status", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, true, body["is_semantic"])
	require.Equal(t, "bge-m3", body["model"])
	require.Equal(t, float64(3), body["dimension"])
	require.Equal(t, "ollama:bge-m3:3", body["vector_space"])
	require.Equal(t, float64(0), body["need_reembed"])
	require.Equal(t, float64(1), body["on_ollama"])
}

func TestEmbeddingReembedUsesActiveCustomSpace(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestMemory(t, s, "custom-pending", "test")
	h.SetEmbedder(&fakeEmbedder{
		name: "ollama", model: "bge-m3", dimension: 3,
		ready: true, semantic: true,
	})
	h.RunBackground = func(fn func(context.Context)) { fn(context.Background()) }

	rr := httptest.NewRecorder()
	testRouter(h).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/dashboard/embeddings/reembed", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	counts, err := s.CountMemoriesByProvider(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, counts["ollama:bge-m3:3"])
	require.Zero(t, counts[""])
}

func TestAutomaticRepairUsesActiveCustomSemanticProvider(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestMemory(t, s, "openai-pending", "test")
	h.SetEmbedder(&fakeEmbedder{
		name: "openai-compatible", model: "gte-small", dimension: 3,
		ready: true, semantic: true,
	})
	h.RunBackground = func(fn func(context.Context)) { fn(context.Background()) }

	started, err := h.StartEmbeddingRepairIfNeeded(context.Background())
	require.NoError(t, err)
	require.True(t, started)
	counts, err := s.CountMemoriesByProvider(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, counts["openai-compatible:gte-small:3"])
}

func TestEmbeddingStatusRecognizesOpenAICompatibleAsSemantic(t *testing.T) {
	h, s := newTestHandler(t)
	insertTestMemory(t, s, "openai", "test")
	h.SetEmbedder(&fakeEmbedder{
		name: "openai-compatible", model: "gte-small", dimension: 3,
		ready: true, semantic: true,
	})
	require.NoError(t, s.UpdateMemoryEmbedding(context.Background(), "openai", []float32{0.1, 0.2, 0.3}, "openai-compatible:gte-small:3"))

	rr := httptest.NewRecorder()
	testRouter(h).ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/embeddings/status", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var body map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, true, body["is_semantic"])
	require.Equal(t, "openai-compatible:gte-small:3", body["vector_space"])
	require.Equal(t, float64(0), body["need_reembed"])
}

func TestEmbeddingReembedRejectsPreCutoverHashProvider(t *testing.T) {
	h, _ := newTestHandler(t)
	h.SetEmbedder(hashOnlyEmbedder{})
	rr := httptest.NewRecorder()
	testRouter(h).ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/dashboard/embeddings/reembed", nil))
	require.Equal(t, http.StatusPreconditionFailed, rr.Code, rr.Body.String())
}
