package hunch

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

// End-to-end offline-containment: once the local judge is configured, judging a memory — with OR without
// evidence — must reach ONLY the configured local endpoint, and no inherited provider config, proxy, cloud
// alias or redirect may carry the memory or evidence anywhere else. These are the guarantees Dhillon asked to
// see proven with outbound blocked; here "blocked" is a forbidden sink that must receive nothing.
func TestOffline_JudgingNeverLeavesTheLocalEndpoint(t *testing.T) {
	var forbiddenHits int64
	forbidden := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		atomic.AddInt64(&forbiddenHits, 1) // any hit here is an exfiltration
	}))
	defer forbidden.Close()

	// Every inherited knob that could redirect egress points AT the forbidden sink.
	for _, k := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy",
		"OPENAI_BASE_URL", "ANTHROPIC_BASE_URL", "OLLAMA_HOST"} {
		t.Setenv(k, forbidden.URL)
	}

	var localHits int64
	local := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/show" {
			_, _ = w.Write([]byte(`{"details":{"format":"gguf"}}`))
			return
		}
		atomic.AddInt64(&localHits, 1)
		require.Equal(t, "/v1/chat/completions", r.URL.Path)
		_, _ = w.Write(logprobBody("Y", -0.1, map[string]float64{"Y": -0.1, "N": -2.4}))
	}))
	defer local.Close()
	client := &LocalClient{BaseURL: local.URL + "/v1", Model: "judge-local"}

	t.Run("supported check WITH evidence", func(t *testing.T) {
		p, err := SupportJudge{Client: client}.SupportedProbability(context.Background(), "A owns B", "A owns B.")
		require.NoError(t, err)
		require.Greater(t, p, 0.9)
	})
	t.Run("lasting check WITHOUT evidence", func(t *testing.T) {
		p, err := LastingJudge{Client: client}.LastingProbability(context.Background(), "The depot opens at 07:00.")
		require.NoError(t, err)
		require.Greater(t, p, 0.9)
	})

	require.Greater(t, atomic.LoadInt64(&localHits), int64(0), "judging must reach the local endpoint")
	require.Equal(t, int64(0), atomic.LoadInt64(&forbiddenHits),
		"no inherited proxy/base-url/cloud-alias may carry the memory or evidence off the local endpoint")
}

// A redirect from the local endpoint to an external host is not followed: the judge fails closed instead of
// re-sending the memory/evidence to the redirect target.
func TestOffline_RedirectIsNotFollowed(t *testing.T) {
	var sinkHits int64
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt64(&sinkHits, 1)
		_, _ = w.Write(logprobBody("Y", -0.1, map[string]float64{"Y": -0.1, "N": -2.4}))
	}))
	defer sink.Close()
	redir := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, sink.URL+r.URL.Path, http.StatusFound)
	}))
	defer redir.Close()
	client := &LocalClient{BaseURL: redir.URL + "/v1", Model: "judge-local"}
	_, err := SupportJudge{Client: client}.SupportedProbability(context.Background(), "m", "e")
	require.Error(t, err, "a redirect off the local endpoint must not be followed; the judge fails closed")
	require.Equal(t, int64(0), atomic.LoadInt64(&sinkHits), "memory/evidence must not reach the redirect target")
}
