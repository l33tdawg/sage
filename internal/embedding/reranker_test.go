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

	cfg := ResolveRerankerConfig()
	assert.True(t, cfg.Enabled)
	assert.Equal(t, "http://tei:8080", cfg.URL)
	assert.Equal(t, "custom/model", cfg.Model)
	assert.Equal(t, 5000, cfg.TimeoutMS)
	assert.Equal(t, 4, cfg.Oversample)
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
