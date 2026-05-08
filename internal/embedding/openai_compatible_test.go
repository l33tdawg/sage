package embedding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fakeOAIServer constructs an httptest.Server that mimics the OpenAI
// /v1/embeddings shape. It returns the server, a pointer to the most-recent
// inbound Authorization header (so tests can assert auth behavior), and a
// pointer to the most-recent JSON body (so tests can assert the request shape).
func fakeOAIServer(t *testing.T, embedding []float64, status int, lastAuth *string, lastBody *map[string]any) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		if lastAuth != nil {
			*lastAuth = r.Header.Get("Authorization")
		}
		if lastBody != nil {
			*lastBody = map[string]any{}
			_ = json.NewDecoder(r.Body).Decode(lastBody)
		}
		if status != http.StatusOK {
			http.Error(w, `{"error":"forced"}`, status)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"data": []map[string]any{
				{"embedding": embedding, "index": 0, "object": "embedding"},
			},
			"model":  "test-model",
			"object": "list",
		})
	})
	return httptest.NewServer(mux)
}

func TestOpenAICompatible_ImplementsProvider(t *testing.T) {
	var _ Provider = (*OpenAICompatibleClient)(nil)
}

func TestOpenAICompatible_ImplementsNamed(t *testing.T) {
	var _ Named = (*OpenAICompatibleClient)(nil)
}

func TestOpenAICompatible_HappyPath(t *testing.T) {
	emb := make([]float64, 1536)
	for i := range emb {
		emb[i] = float64(i) / 1536.0
	}

	var lastAuth string
	var lastBody map[string]any
	srv := fakeOAIServer(t, emb, http.StatusOK, &lastAuth, &lastBody)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "gte-Qwen2-1.5B-instruct", "", 1536)
	require.False(t, c.Ready(), "should not be Ready before first successful call")

	out, err := c.Embed(context.Background(), "hello world")
	require.NoError(t, err)
	assert.Len(t, out, 1536)
	assert.Equal(t, 1536, c.Dimension())
	assert.True(t, c.Semantic())
	assert.Equal(t, "openai-compatible", c.Name())
	assert.True(t, c.Ready(), "should flip Ready true after a success")

	// Request shape: model + input fields populated correctly.
	assert.Equal(t, "gte-Qwen2-1.5B-instruct", lastBody["model"])
	assert.Equal(t, "hello world", lastBody["input"])
}

func TestOpenAICompatible_AuthHeaderSentWhenAPIKeySet(t *testing.T) {
	var lastAuth string
	srv := fakeOAIServer(t, []float64{0.1, 0.2, 0.3}, http.StatusOK, &lastAuth, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "sk-test-12345", 3)
	_, err := c.Embed(context.Background(), "x")
	require.NoError(t, err)
	assert.Equal(t, "Bearer sk-test-12345", lastAuth)
}

func TestOpenAICompatible_AuthHeaderOmittedWhenAPIKeyEmpty(t *testing.T) {
	var lastAuth string
	srv := fakeOAIServer(t, []float64{0.1, 0.2, 0.3}, http.StatusOK, &lastAuth, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 3)
	_, err := c.Embed(context.Background(), "x")
	require.NoError(t, err)
	assert.Empty(t, lastAuth, "no Authorization header should be sent when api_key is empty")
}

func TestOpenAICompatible_DimensionMismatch(t *testing.T) {
	srv := fakeOAIServer(t, []float64{0.1, 0.2, 0.3}, http.StatusOK, nil, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 1536)
	_, err := c.Embed(context.Background(), "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "dimension mismatch")
	assert.False(t, c.Ready(), "Ready must stay false on mismatch — vector store would have been corrupted")
}

func TestOpenAICompatible_NoData(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/embeddings", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[],"model":"m","object":"list"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 1536)
	_, err := c.Embed(context.Background(), "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no embeddings")
}

func TestOpenAICompatible_ServerError(t *testing.T) {
	srv := fakeOAIServer(t, nil, http.StatusInternalServerError, nil, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 1536)
	_, err := c.Embed(context.Background(), "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 500")
}

func TestOpenAICompatible_BadRequest(t *testing.T) {
	srv := fakeOAIServer(t, nil, http.StatusBadRequest, nil, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 1536)
	_, err := c.Embed(context.Background(), "x")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "status 400")
}

func TestOpenAICompatible_NetworkFailure(t *testing.T) {
	// Bind a server, get its URL, then close it so the next dial fails.
	srv := fakeOAIServer(t, []float64{0.1}, http.StatusOK, nil, nil)
	url := srv.URL
	srv.Close()

	c := NewOpenAICompatibleClient(url, "m", "", 1)
	_, err := c.Embed(context.Background(), "x")
	require.Error(t, err)
	assert.Contains(t, strings.ToLower(err.Error()), "embed request")
}

func TestOpenAICompatible_DimensionGetter(t *testing.T) {
	c := NewOpenAICompatibleClient("http://x", "m", "", 1024)
	assert.Equal(t, 1024, c.Dimension())
	assert.True(t, c.Semantic())
}

func TestOpenAICompatible_DimensionZeroSkipsValidation(t *testing.T) {
	// dimension=0 means "trust whatever the server returns" — useful when the
	// operator hasn't pinned a dimension. Keep behavior permissive.
	srv := fakeOAIServer(t, []float64{0.1, 0.2, 0.3, 0.4}, http.StatusOK, nil, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 0)
	out, err := c.Embed(context.Background(), "x")
	require.NoError(t, err)
	assert.Len(t, out, 4)
}

func TestOpenAICompatible_RespectsContextCancellation(t *testing.T) {
	srv := fakeOAIServer(t, []float64{0.1}, http.StatusOK, nil, nil)
	defer srv.Close()

	c := NewOpenAICompatibleClient(srv.URL, "m", "", 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancelled

	_, err := c.Embed(ctx, "x")
	require.Error(t, err)
}
