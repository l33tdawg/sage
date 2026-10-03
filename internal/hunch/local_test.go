package hunch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// logprobBody builds the shape Ollama returns for /v1/chat/completions with logprobs.
func logprobBody(first string, firstLP float64, alts map[string]float64) []byte {
	type alt struct {
		Token   string  `json:"token"`
		Logprob float64 `json:"logprob"`
	}
	top := make([]alt, 0, len(alts))
	for tok, lp := range alts {
		top = append(top, alt{tok, lp})
	}
	body := map[string]any{"choices": []any{map[string]any{
		"message": map[string]any{"content": first},
		"logprobs": map[string]any{"content": []any{map[string]any{
			"token": first, "logprob": firstLP, "top_logprobs": top,
		}}}}}}
	out, _ := json.Marshal(body)
	return out
}

func localServer(t *testing.T, body []byte, status int, check func(*http.Request, map[string]any)) *LocalClient {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/api/show" {
			_, _ = w.Write([]byte(`{"details":{"format":"gguf"}}`))
			return
		}
		var req map[string]any
		_ = json.NewDecoder(r.Body).Decode(&req)
		if check != nil {
			check(r, req)
		}
		w.WriteHeader(status)
		_, _ = w.Write(body)
	}))
	t.Cleanup(srv.Close)
	return &LocalClient{BaseURL: srv.URL + "/v1", Model: "judge-local"}
}

func yesNo(t *testing.T, c *LocalClient, ctxMap map[string]string) (float64, error) {
	t.Helper()
	ps, err := c.YesNo(context.Background(), ctxMap, map[string]Check{"c": {Kind: "yesno", Question: "q"}})
	if err != nil {
		return 0, err
	}
	return ps["c"], nil
}

func TestLocalReader_ReadsTheLabelProbability(t *testing.T) {
	// P(Y)=e^-0.1, P(N)=e^-2.4 -> a decisive yes
	c := localServer(t, logprobBody("Y", -0.1, map[string]float64{"Y": -0.1, "N": -2.4, "Yes": -8}), 200,
		func(r *http.Request, req map[string]any) {
			require.Equal(t, "/v1/chat/completions", r.URL.Path)
			require.Equal(t, "judge-local", req["model"])
			require.Equal(t, true, req["logprobs"])
			require.EqualValues(t, 1, req["max_tokens"])
			// a thinking model would spend the single token on reasoning and never answer
			require.Equal(t, "none", req["reasoning_effort"])
			msgs := req["messages"].([]any)
			require.Len(t, msgs, 2)
			require.Contains(t, msgs[1].(map[string]any)["content"], "CONTEXT:")
		})
	p, err := yesNo(t, c, map[string]string{"memory": "m", "evidence": "e"})
	require.NoError(t, err)
	require.Greater(t, p, 0.9, "a decisive yes must read as a decisive yes")
}

func TestLocalReader_FailsClosed(t *testing.T) {
	t.Run("first token is not a label", func(t *testing.T) {
		c := localServer(t, logprobBody("Yes", -0.01, map[string]float64{"Yes": -0.01, "Y": -6, "N": -7}), 200, nil)
		_, err := yesNo(t, c, map[string]string{"memory": "m"})
		require.ErrorIs(t, err, ErrLabelMissing, "must be unavailable, never a renormalised guess")
	})
	t.Run("chosen label carried no top-k mass falls back to its own logprob", func(t *testing.T) {
		// Chosen token is Y but neither bare label appears in top_logprobs: Hunch uses the chosen
		// token's own logprob, so Y gets all the mass -> p=1.0 (accept), not an error.
		c := localServer(t, logprobBody("Y", -0.01, map[string]float64{"Yes": -0.01, "Yea": -1}), 200, nil)
		p, err := yesNo(t, c, map[string]string{"memory": "m"})
		require.NoError(t, err)
		require.Greater(t, p, 0.99)
	})
	t.Run("no token probabilities at all", func(t *testing.T) {
		c := localServer(t, []byte(`{"choices":[{"message":{"content":"Y"}}]}`), 200, nil)
		_, err := yesNo(t, c, map[string]string{"memory": "m"})
		require.Error(t, err)
	})
	t.Run("backend error", func(t *testing.T) {
		c := localServer(t, []byte(`{"error":"model not found"}`), 404, nil)
		_, err := yesNo(t, c, map[string]string{"memory": "m"})
		require.Error(t, err)
	})
	t.Run("not configured", func(t *testing.T) {
		var c *LocalClient
		_, err := c.YesNo(context.Background(), map[string]string{"memory": "m"}, map[string]Check{"c": {}})
		require.Error(t, err)
		_, err = (&LocalClient{BaseURL: "http://x"}).YesNo(context.Background(), map[string]string{"memory": "m"}, map[string]Check{"c": {}})
		require.Error(t, err, "a local judge must be told which model to ask")
	})
}

func TestLocalReader_DebiasAsksBothOrdersAndAverages(t *testing.T) {
	var orders []string
	c := localServer(t, logprobBody("Y", -0.1, map[string]float64{"Y": -0.1, "N": -2.4}), 200,
		func(_ *http.Request, req map[string]any) {
			msgs := req["messages"].([]any)
			content := msgs[1].(map[string]any)["content"].(string)
			if len(content) > 0 {
				if last := content[len(content)-7:]; last == "N or Y." {
					orders = append(orders, "N-first")
					return
				}
			}
			orders = append(orders, "Y-first")
		})
	c.Debias = true
	_, err := yesNo(t, c, map[string]string{"memory": "m", "evidence": "e"})
	require.NoError(t, err)
	require.ElementsMatch(t, []string{"Y-first", "N-first"}, orders)
}

func TestLocalReader_RejectsNonYesNoChecks(t *testing.T) {
	c := localServer(t, logprobBody("Y", -0.1, nil), 200, nil)
	_, err := c.YesNo(context.Background(), map[string]string{"memory": "m"},
		map[string]Check{"c": {Kind: "pick", Question: "q"}})
	require.ErrorContains(t, err, "yesno")
}

// Offline safety: with outbound blocked / the local judge unreachable, the reader must ERROR (which the
// gate treats as unavailable -> hold for review), never return a score that could be read as accept.
// This is the property that makes the no-cloud judge safe when its only dependency is down.
func TestLocalReader_UnreachableJudgeFailsSafe(t *testing.T) {
	// A port nothing listens on — stands in for outbound-blocked / Ollama down.
	c := &LocalClient{BaseURL: "http://127.0.0.1:1/v1", Model: "judge-local", Timeout: 2 * time.Second}
	_, err := c.YesNo(context.Background(), map[string]string{"memory": "m", "evidence": "e"},
		map[string]Check{"c": {Kind: "yesno", Question: "q"}})
	require.Error(t, err, "an unreachable local judge must surface an error, so the gate holds — never a silent score")
}

// The client only ever dials its configured BaseURL: no inherited provider env, no redirect target.
// Constructed URL is BaseURL + /chat/completions and nothing else.
func TestLocalReader_OnlyDialsConfiguredURL(t *testing.T) {
	var hits []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits = append(hits, r.Host+r.URL.Path)
		if r.URL.Path == "/api/show" {
			_, _ = w.Write([]byte(`{"details":{"format":"gguf"}}`))
			return
		}
		w.WriteHeader(200)
		_, _ = w.Write(logprobBody("Y", -0.1, map[string]float64{"Y": -0.1, "N": -2.4}))
	}))
	defer srv.Close()
	c := &LocalClient{BaseURL: srv.URL + "/v1", Model: "judge-local"}
	_, err := c.YesNo(context.Background(), map[string]string{"memory": "m", "evidence": "e"},
		map[string]Check{"c": {Kind: "yesno", Question: "q"}})
	require.NoError(t, err)
	require.Len(t, hits, 2)
	require.Contains(t, hits[0], "/api/show")
	require.Contains(t, hits[1], "/v1/chat/completions")
}
