package embedding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHTTPReranker_RoundtripPreservesUpstreamOrder(t *testing.T) {
	// TEI returns score-descending; we should preserve whatever it sent.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]tEIRerankResponse{
			{Index: 2, Score: 0.95},
			{Index: 0, Score: 0.42},
			{Index: 1, Score: 0.10},
		})
	}))
	defer srv.Close()

	rk := NewHTTPReranker(srv.URL, "BAAI/bge-reranker-v2-m3", 2*time.Second)
	out, err := rk.Rerank(context.Background(), "test query", []string{"a", "b", "c"})
	require.NoError(t, err)
	require.Len(t, out, 3)
	assert.Equal(t, 2, out[0].Index)
	assert.InDelta(t, 0.95, out[0].Score, 0.001)
	assert.Equal(t, 0, out[1].Index)
	assert.Equal(t, 1, out[2].Index)
}

func TestHTTPReranker_EmptyTexts(t *testing.T) {
	rk := NewHTTPReranker("http://does-not-matter", "m", time.Second)
	out, err := rk.Rerank(context.Background(), "q", nil)
	require.NoError(t, err)
	assert.Nil(t, out)
}

func TestHTTPReranker_UpstreamError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "model not loaded", http.StatusServiceUnavailable)
	}))
	defer srv.Close()

	rk := NewHTTPReranker(srv.URL, "m", time.Second)
	_, err := rk.Rerank(context.Background(), "q", []string{"a"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "503")
}

func TestHTTPReranker_RespectsTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(200 * time.Millisecond)
		_ = json.NewEncoder(w).Encode([]tEIRerankResponse{})
	}))
	defer srv.Close()

	rk := NewHTTPReranker(srv.URL, "m", 50*time.Millisecond)
	_, err := rk.Rerank(context.Background(), "q", []string{"a"})
	require.Error(t, err, "expected timeout error")
}

func TestHTTPReranker_NilReceiverErrorsClean(t *testing.T) {
	var rk *HTTPReranker
	_, err := rk.Rerank(context.Background(), "q", []string{"a"})
	require.Error(t, err)
}

func TestHTTPReranker_BoundsLargeErrorBody(t *testing.T) {
	// A misconfigured upstream might return a multi-MB HTML page; the
	// reranker client should cap how much of that ends up in the SAGE log.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		big := make([]byte, 4096)
		for i := range big {
			big[i] = 'x'
		}
		_, _ = w.Write(big)
	}))
	defer srv.Close()

	rk := NewHTTPReranker(srv.URL, "m", time.Second)
	_, err := rk.Rerank(context.Background(), "q", []string{"a"})
	require.Error(t, err)
	// Error message should NOT contain the full 4KB body.
	assert.Less(t, len(err.Error()), 1024)
}

func TestResolveRerankerConfig_DefaultsOff(t *testing.T) {
	t.Setenv("SAGE_RERANK_ENABLED", "")
	t.Setenv("SAGE_RERANK_URL", "")
	cfg := ResolveRerankerConfig()
	assert.False(t, cfg.Enabled)
	assert.Equal(t, defaultRerankerModel, cfg.Model)
	assert.Equal(t, defaultRerankerTimeoutMS, cfg.TimeoutMS)
	assert.Equal(t, defaultRerankerOversample, cfg.Oversample)
}

func TestResolveRerankerConfig_EnvOverrides(t *testing.T) {
	t.Setenv("SAGE_RERANK_ENABLED", "1")
	t.Setenv("SAGE_RERANK_URL", "http://tei:8080")
	t.Setenv("SAGE_RERANK_MODEL", "custom/model")
	t.Setenv("SAGE_RERANK_TIMEOUT_MS", "5000")
	t.Setenv("SAGE_RERANK_OVERSAMPLE", "4")
	t.Setenv("SAGE_RERANK_MAX_CANDIDATES", "10")

	cfg := ResolveRerankerConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "http://tei:8080", cfg.URL)
	assert.Equal(t, "custom/model", cfg.Model)
	assert.Equal(t, 5000, cfg.TimeoutMS)
	assert.Equal(t, 4, cfg.Oversample)
	assert.Equal(t, 10, cfg.MaxCandidates)
}

func TestResolveRerankerConfig_MaxCandidatesRejectsNonPositive(t *testing.T) {
	for _, v := range []string{"0", "-3", "ten"} {
		t.Setenv("SAGE_RERANK_MAX_CANDIDATES", v)
		assert.Equal(t, 0, ResolveRerankerConfig().MaxCandidates, "value %q must leave the cap off", v)
	}
}

func TestResolveRerankerConfig_EnabledNeedsURL(t *testing.T) {
	// Enabled=1 alone shouldn't produce a working reranker — operator must
	// also point at an endpoint.
	t.Setenv("SAGE_RERANK_ENABLED", "1")
	t.Setenv("SAGE_RERANK_URL", "")
	cfg := ResolveRerankerConfig()
	assert.True(t, cfg.Enabled)
	assert.Empty(t, cfg.URL)
	assert.Nil(t, BuildReranker(cfg), "missing URL must keep BuildReranker nil")
}

func TestBuildReranker_GatedByEnabledAndURL(t *testing.T) {
	cases := []struct {
		name    string
		cfg     RerankerConfig
		wantNil bool
	}{
		{"disabled, no URL", RerankerConfig{Enabled: false}, true},
		{"disabled, with URL", RerankerConfig{Enabled: false, URL: "http://tei"}, true},
		{"enabled, no URL", RerankerConfig{Enabled: true}, true},
		{"enabled, with URL", RerankerConfig{Enabled: true, URL: "http://tei", TimeoutMS: 1000}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := BuildReranker(c.cfg)
			if c.wantNil {
				assert.Nil(t, r)
			} else {
				assert.NotNil(t, r)
			}
		})
	}
}

func TestEnvTrue(t *testing.T) {
	cases := map[string]bool{
		"":      false,
		"0":     false,
		"false": false,
		"no":    false,
		"FALSE": false,
		"1":     true,
		"true":  true,
		"YES":   true,
		"On":    true,
	}
	for v, want := range cases {
		t.Setenv("SAGE_TEST_TRUTHY", v)
		assert.Equal(t, want, envTrue("SAGE_TEST_TRUTHY"), "value=%q", v)
	}
}

func TestHTTPReranker_LlamaCppDialect(t *testing.T) {
	// llama.cpp llama-server: POST /v1/rerank {model, query, documents} ->
	// {results: [{index, relevance_score}]}. Verify both the request shape
	// and the response mapping.
	var gotPath string
	var gotBody llamaCppRerankRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"results": []map[string]any{
				{"index": 1, "relevance_score": 0.87},
				{"index": 0, "relevance_score": 0.12},
			},
		})
	}))
	defer srv.Close()

	rk := NewHTTPRerankerKind(srv.URL, "bge-reranker-v2-m3", RerankKindLlamaCpp, 2*time.Second)
	out, err := rk.Rerank(context.Background(), "q", []string{"a", "b"})
	require.NoError(t, err)
	assert.Equal(t, "/v1/rerank", gotPath)
	assert.Equal(t, "q", gotBody.Query)
	assert.Equal(t, []string{"a", "b"}, gotBody.Documents)
	require.Len(t, out, 2)
	assert.Equal(t, 1, out[0].Index)
	assert.InDelta(t, 0.87, out[0].Score, 0.001)
	assert.Equal(t, 0, out[1].Index)
}

func TestHTTPReranker_UnknownKindFallsBackToTEI(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/rerank", r.URL.Path)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode([]tEIRerankResponse{{Index: 0, Score: 1}})
	}))
	defer srv.Close()
	rk := NewHTTPRerankerKind(srv.URL, "m", "bogus-kind", 2*time.Second)
	out, err := rk.Rerank(context.Background(), "q", []string{"a"})
	require.NoError(t, err)
	require.Len(t, out, 1)
}

func TestHTTPReranker_RejectsPublicEndpoint(t *testing.T) {
	rk := NewHTTPReranker("https://8.8.8.8:443", "m", time.Second)
	_, err := rk.Rerank(context.Background(), "q", []string{"a"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid reranker url")
}
