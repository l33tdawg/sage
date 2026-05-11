package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeEmbedder is a test double for an embedding.Provider. It supplies the
// full optional interface surface (Name / Model / Dimension / Ready / Semantic
// / Ping) the dashboard health endpoint type-asserts against, so each variant
// of the matrix (ollama running, openai-compatible offline, hash, missing) can
// be exercised without standing up a real backing service.
type fakeEmbedder struct {
	name      string
	model     string
	dimension int
	ready     bool
	semantic  bool
	pingErr   error
}

func (f *fakeEmbedder) Embed(_ context.Context, _ string) ([]float32, error) {
	return make([]float32, f.dimension), nil
}
func (f *fakeEmbedder) Name() string                 { return f.name }
func (f *fakeEmbedder) Model() string                { return f.model }
func (f *fakeEmbedder) Dimension() int               { return f.dimension }
func (f *fakeEmbedder) Ready() bool                  { return f.ready }
func (f *fakeEmbedder) Semantic() bool               { return f.semantic }
func (f *fakeEmbedder) Ping(_ context.Context) error { return f.pingErr }

// hashOnlyEmbedder reproduces the (pre-Named) HashProvider shape: no Name,
// no Model, no Pinger. The dashboard must still classify it correctly using
// Semantic()=false as the fallback signal — same logic as
// api/rest/embed_handler.go.
type hashOnlyEmbedder struct{}

func (hashOnlyEmbedder) Embed(_ context.Context, _ string) ([]float32, error) {
	return make([]float32, 768), nil
}
func (hashOnlyEmbedder) Dimension() int { return 768 }
func (hashOnlyEmbedder) Ready() bool    { return true }
func (hashOnlyEmbedder) Semantic() bool { return false }

// doHealth dispatches GET /v1/dashboard/health against a router built from h
// and returns the decoded JSON body. It's a thin helper so the table tests
// below stay legible.
func doHealth(t *testing.T, h *DashboardHandler) map[string]any {
	t.Helper()
	r := testRouter(h)
	req := httptest.NewRequest("GET", "/v1/dashboard/health", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	return resp
}

// TestHandleHealth_EmbedderOllamaRunning is the happy-path case for the
// pre-existing default deployment: an Ollama client that pings successfully
// must produce embedder.provider="ollama", embedder.online=true, AND the
// legacy "ollama":"running" field so older dashboards keep working.
func TestHandleHealth_EmbedderOllamaRunning(t *testing.T) {
	h, _ := newTestHandler(t)
	h.SetEmbedder(&fakeEmbedder{
		name:      "ollama",
		model:     "nomic-embed-text",
		dimension: 768,
		ready:     true,
		semantic:  true,
		pingErr:   nil,
	})

	resp := doHealth(t, h)

	emb, ok := resp["embedder"].(map[string]any)
	require.True(t, ok, "embedder block must be present")
	assert.Equal(t, "ollama", emb["provider"])
	assert.Equal(t, "nomic-embed-text", emb["model"])
	assert.Equal(t, float64(768), emb["dimension"])
	assert.Equal(t, true, emb["online"])
	assert.Equal(t, true, emb["semantic"])
	assert.Equal(t, true, emb["ready"])

	// Legacy field — pre-fix dashboards read this; must still say "running"
	// for the Ollama-online case so we don't regress them.
	assert.Equal(t, "running", resp["ollama"])
}

// TestHandleHealth_EmbedderOllamaOffline covers Ollama configured but the
// upstream unreachable: provider stays "ollama", online flips to false, and
// the legacy field reflects it.
func TestHandleHealth_EmbedderOllamaOffline(t *testing.T) {
	h, _ := newTestHandler(t)
	h.SetEmbedder(&fakeEmbedder{
		name:      "ollama",
		model:     "nomic-embed-text",
		dimension: 768,
		ready:     false,
		semantic:  true,
		pingErr:   errors.New("connection refused"),
	})

	resp := doHealth(t, h)

	emb := resp["embedder"].(map[string]any)
	assert.Equal(t, "ollama", emb["provider"])
	assert.Equal(t, false, emb["online"])
	assert.Equal(t, "offline", resp["ollama"])
}

// TestHandleHealth_EmbedderOpenAICompatibleOnline is the bug-fix case: with
// openai-compatible selected, the dashboard MUST NOT paint "Ollama offline".
// embedder.provider="openai-compatible", and the legacy field flips to
// "n/a" so older dashboards don't render a misleading Ollama status.
func TestHandleHealth_EmbedderOpenAICompatibleOnline(t *testing.T) {
	h, _ := newTestHandler(t)
	h.SetEmbedder(&fakeEmbedder{
		name:      "openai-compatible",
		model:     "Alibaba-NLP/gte-Qwen2-1.5B-instruct",
		dimension: 1536,
		ready:     true,
		semantic:  true,
		pingErr:   nil,
	})

	resp := doHealth(t, h)

	emb := resp["embedder"].(map[string]any)
	assert.Equal(t, "openai-compatible", emb["provider"])
	assert.Equal(t, "Alibaba-NLP/gte-Qwen2-1.5B-instruct", emb["model"])
	assert.Equal(t, float64(1536), emb["dimension"])
	assert.Equal(t, true, emb["online"])
	assert.Equal(t, true, emb["semantic"])

	// Legacy field is now "n/a" — older dashboards will paint Ollama as
	// "unknown" rather than the false-positive "Connected" we'd get if we
	// claimed "running" with a non-Ollama provider.
	assert.Equal(t, "n/a", resp["ollama"])
}

// TestHandleHealth_EmbedderOpenAICompatibleOffline confirms the operator pill
// will correctly show the openai-compatible endpoint as offline when its
// Ping fails — a wedge case that, without the fix, the user would see as
// "Ollama offline" and chase the wrong upstream.
func TestHandleHealth_EmbedderOpenAICompatibleOffline(t *testing.T) {
	h, _ := newTestHandler(t)
	h.SetEmbedder(&fakeEmbedder{
		name:      "openai-compatible",
		model:     "gte-qwen2",
		dimension: 1536,
		ready:     false,
		semantic:  true,
		pingErr:   errors.New("dial tcp: connect: connection refused"),
	})

	resp := doHealth(t, h)

	emb := resp["embedder"].(map[string]any)
	assert.Equal(t, "openai-compatible", emb["provider"])
	assert.Equal(t, false, emb["online"])
	assert.Equal(t, "n/a", resp["ollama"])
}

// TestHandleHealth_EmbedderHash exercises the pre-Named hash provider path.
// It has no Name(), no Model(), no Pinger — but Semantic()=false is enough
// to label it correctly. Hash has no upstream, so online=true is the right
// answer (the embed call never leaves the process).
func TestHandleHealth_EmbedderHash(t *testing.T) {
	h, _ := newTestHandler(t)
	h.SetEmbedder(hashOnlyEmbedder{})

	resp := doHealth(t, h)

	emb := resp["embedder"].(map[string]any)
	assert.Equal(t, "hash", emb["provider"])
	assert.Equal(t, float64(768), emb["dimension"])
	assert.Equal(t, true, emb["online"], "hash provider is always online — no upstream to fail")
	assert.Equal(t, false, emb["semantic"])
	// Model is omitted for hash — the helper assert.NotContains makes the
	// "no model suffix in the UI" contract explicit.
	_, hasModel := emb["model"]
	assert.False(t, hasModel, "hash provider must not advertise a model identifier")
	// Legacy field — hash isn't Ollama, so "n/a".
	assert.Equal(t, "n/a", resp["ollama"])
}

// TestHandleHealth_EmbedderNotConfigured covers the boot-window case where
// SetEmbedder hasn't been called yet. The block must still be present (so
// the dashboard can render a sensible "no embedder" state) and online must
// be false.
func TestHandleHealth_EmbedderNotConfigured(t *testing.T) {
	h, _ := newTestHandler(t)
	// Deliberately no SetEmbedder call.

	resp := doHealth(t, h)

	emb, ok := resp["embedder"].(map[string]any)
	require.True(t, ok, "embedder block must always be present, even when nil")
	assert.Equal(t, "none", emb["provider"])
	assert.Equal(t, false, emb["online"])
}
