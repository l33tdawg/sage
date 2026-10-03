package voter

import (
	"context"
	"errors"
	"github.com/l33tdawg/sage/internal/hunch"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestReview_UnavailableJudgeHoldsInsteadOfAccepting(t *testing.T) {
	m := gateRec("unavailable", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	g := &Gate{Judges: []LastingJudge{&fakeJudge{err: errors.New("judge unavailable")}}, Version: "review"}
	g.init()
	g.evaluate(context.Background(), gs, m.MemoryID, zerolog.Nop())
	out, decision := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
	require.Equal(t, gateHold, out)
	require.False(t, decision.Accept)
}

func TestReview_LocalBackendFailuresReachTheReviewQueue(t *testing.T) {
	for _, payload := range []string{
		`{"choices":[{"logprobs":{"content":[{"token":"<think>","logprob":-0.1}]}}]}`,
		`{"choices":[{"logprobs":{"content":[{"token":"Y"}]}}]}`,
		`{"choices":[]}`,
	} {
		t.Run(payload, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/api/show" {
					_, _ = w.Write([]byte(`{"details":{"format":"gguf"}}`))
					return
				}
				_, _ = w.Write([]byte(payload))
			}))
			defer srv.Close()
			client := &hunch.LocalClient{BaseURL: srv.URL + "/v1", Model: "local-judge"}
			m := gateRec("backend", "The depot opens at 07:00 on weekdays.", "notes")
			gs := newFakeGateStore(m)
			gs.evidence[m.MemoryID] = "The depot timetable says weekdays at 07:00."
			g := &Gate{Judges: []LastingJudge{hunch.LastingJudge{Client: client}}, SupportJudges: []SupportJudge{hunch.SupportJudge{Client: client}}, Version: "review"}
			g.init()
			g.evaluate(context.Background(), gs, m.MemoryID, zerolog.Nop())
			out, d := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
			require.Equal(t, gateHold, out)
			require.False(t, d.Accept)
			v, ok, err := gs.SemanticVerdict(context.Background(), m.MemoryID, g.Version)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, memory.VerdictAbstain, v.Verdict)
			require.Contains(t, v.Reason, "unavailable")
		})
	}
}
