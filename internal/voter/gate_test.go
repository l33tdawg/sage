package voter

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// fakeJudge answers with a fixed probability, an error, or blocks until its
// context ends (a hung judge service).
type fakeJudge struct {
	p     float64
	err   error
	hang  bool
	calls atomic.Int64
}

func (f *fakeJudge) LastingProbability(ctx context.Context, _ string) (float64, error) {
	f.calls.Add(1)
	if f.hang {
		<-ctx.Done()
		return 0, ctx.Err()
	}
	return f.p, f.err
}

// fakeGateStore is fakeStore plus the gate's node-local tables.
type fakeGateStore struct {
	fakeStore
	mu       sync.Mutex
	content  map[string]string
	verdicts map[string]memory.SemanticVerdict // memoryID|version
	review   map[string]string
}

func newFakeGateStore(pending ...*memory.MemoryRecord) *fakeGateStore {
	f := &fakeGateStore{fakeStore: fakeStore{pending: pending, dups: map[string]bool{}},
		content: map[string]string{}, verdicts: map[string]memory.SemanticVerdict{}, review: map[string]string{}}
	for _, m := range pending {
		f.content[m.MemoryID] = m.Content
	}
	return f
}

func (f *fakeGateStore) JudgeableContent(_ context.Context, id string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	c, ok := f.content[id]
	if !ok {
		return "", errors.New("content unavailable")
	}
	return c, nil
}
func (f *fakeGateStore) SemanticVerdict(_ context.Context, id, version string) (memory.SemanticVerdict, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.verdicts[id+"|"+version]
	return v, ok, nil
}
func (f *fakeGateStore) RecordSemanticVerdict(_ context.Context, id, version string, v memory.SemanticVerdict) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.verdicts[id+"|"+version] = v
	return nil
}
func (f *fakeGateStore) ReviewDecision(_ context.Context, id string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.review[id]
	return d, ok, nil
}

func gateRec(id, content, domain string) *memory.MemoryRecord {
	return &memory.MemoryRecord{MemoryID: id, Content: content, ContentHash: []byte(id), DomainTag: domain,
		MemoryType: memory.TypeFact, ConfidenceScore: 0.9}
}

var accept = Decision{Accept: true, Reason: "passes all checks"}

func TestGate_FreshBaselineRejectAlwaysWins(t *testing.T) {
	// The reviewed bug: a cached accept must never override a NEW built-in
	// rejection (e.g. the dedup check failing on a vote retry).
	m := gateRec("m1", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	g := &Gate{Judges: []LastingJudge{&fakeJudge{p: 0.99}}, Version: "v1"}
	gs.verdicts["m1|v1"] = memory.SemanticVerdict{Verdict: memory.VerdictPass, P: 0.99}
	gs.review["m1"] = memory.VerdictAccept
	dedupReject := Decision{Accept: false, Reason: "duplicate content (hash: 6d31)"}
	out, d := g.Apply(context.Background(), gs, m, dedupReject, zerolog.Nop())
	require.Equal(t, gateUseBaseline, out)
	require.False(t, d.Accept)
	require.Contains(t, d.Reason, "duplicate content")
}

func TestGate_UnjudgedMemoryIsHeldAndJudgedInTheBackground(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := gateRec("m1", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	j := &fakeJudge{p: 0.97}
	g := &Gate{Judges: []LastingJudge{j}, Version: "v1"}
	g.Start(ctx, gs, zerolog.Nop())

	out, _ := g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Equal(t, gateHold, out, "no verdict yet: hold, never block the loop on the judge")
	require.Eventually(t, func() bool {
		_, ok, _ := gs.SemanticVerdict(ctx, "m1", "v1")
		return ok
	}, 5*time.Second, 10*time.Millisecond)
	out, d := g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Equal(t, gateOverride, out)
	require.True(t, d.Accept)
	require.Contains(t, d.Reason, "passes all checks", "the fresh built-in reason is kept")
	require.Contains(t, d.Reason, "lasting p=0.97")
}

func TestGate_SessionRemarkIsRejected(t *testing.T) {
	m := gateRec("m1", "The attachment was lost; the user must re-send the ten numbers.", "notes")
	gs := newFakeGateStore(m)
	g := &Gate{Judges: []LastingJudge{&fakeJudge{}}, Version: "v1"}
	gs.verdicts["m1|v1"] = g.combineForTest(0.04)
	out, d := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
	require.Equal(t, gateOverride, out)
	require.False(t, d.Accept)
	require.Contains(t, d.Reason, "not lasting memory")
}

func TestGate_HeldForReviewUntilTheOperatorDecides(t *testing.T) {
	m := gateRec("m1", "The quarterly count was around two hundred units.", "notes")
	gs := newFakeGateStore(m)
	g := &Gate{Judges: []LastingJudge{&fakeJudge{}}, Version: "v1"}
	gs.verdicts["m1|v1"] = g.combineForTest(0.7)
	out, _ := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
	require.Equal(t, gateHold, out)

	gs.review["m1"] = memory.VerdictAccept
	out, d := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
	require.Equal(t, gateOverride, out)
	require.True(t, d.Accept)
	require.Contains(t, d.Reason, "resolved by operator review")
}

func TestGate_ReviewDecisionSurvivesAJudgeVersionChange(t *testing.T) {
	m := gateRec("m1", "The quarterly count was around two hundred units.", "notes")
	gs := newFakeGateStore(m)
	gs.review["m1"] = memory.VerdictReject
	g := &Gate{Judges: []LastingJudge{&fakeJudge{p: 0.99}}, Version: "v2"} // no v2 verdict yet
	out, d := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
	require.Equal(t, gateOverride, out, "a human decision is final for that memory")
	require.False(t, d.Accept)
}

func TestGate_VersionChangeRejudges(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := gateRec("m1", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	gs.verdicts["m1|v1"] = memory.SemanticVerdict{Verdict: memory.VerdictReject, P: 0.1}
	j := &fakeJudge{p: 0.97}
	g := &Gate{Judges: []LastingJudge{j}, Version: "v2"}
	g.Start(ctx, gs, zerolog.Nop())
	out, _ := g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Equal(t, gateHold, out, "a verdict from an older judge version is not reused")
	require.Eventually(t, func() bool { return j.calls.Load() == 1 }, 5*time.Second, 10*time.Millisecond)
}

func TestGate_JudgeFailureFallsBackToBuiltInChecks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := gateRec("m1", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	g := &Gate{Judges: []LastingJudge{&fakeJudge{err: errors.New("connection refused")}}, Version: "v1"}
	g.Start(ctx, gs, zerolog.Nop())
	_, _ = g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Eventually(t, func() bool {
		out, d := g.Apply(ctx, gs, m, accept, zerolog.Nop())
		return out == gateUseBaseline && d.Accept
	}, 5*time.Second, 10*time.Millisecond, "after a failure the node votes with the built-in checks")
	_, ok, _ := gs.SemanticVerdict(ctx, "m1", "v1")
	require.False(t, ok, "a failed judgement records nothing")
}

func TestGate_UnreadableContentIsNeverSent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := gateRec("m1", "enc::ciphertext", "notes")
	gs := newFakeGateStore(m)
	delete(gs.content, "m1") // the store cannot produce plaintext
	j := &fakeJudge{p: 0.99}
	g := &Gate{Judges: []LastingJudge{j}, Version: "v1"}
	g.Start(ctx, gs, zerolog.Nop())
	_, _ = g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Eventually(t, func() bool {
		out, _ := g.Apply(ctx, gs, m, accept, zerolog.Nop())
		return out == gateUseBaseline
	}, 5*time.Second, 10*time.Millisecond)
	require.Zero(t, j.calls.Load(), "nothing is sent to a judge when the content cannot be read")
}

func TestGate_ScopeControlsWhatIsSent(t *testing.T) {
	g := &Gate{IncludeDomainPrefixes: []string{"projects."}, ExemptDomainPrefixes: []string{"projects.catalog"}}
	require.True(t, g.InScope("projects.alpha"))
	require.False(t, g.InScope("projects.catalog.tools"))
	require.False(t, g.InScope("personal.notes"))
	require.True(t, (&Gate{}).InScope("anything"))
}

func TestGate_LeadPolicy(t *testing.T) {
	g := &Gate{}
	g.init()
	require.Equal(t, memory.VerdictPass, g.combine([]float64{0.97, 0.62}).Verdict, "a hedging second judge does not block a sure lead")
	require.Equal(t, memory.VerdictAbstain, g.combine([]float64{0.97, 0.2}).Verdict, "a clear objection vetoes the lead")
	require.Equal(t, memory.VerdictReject, g.combine([]float64{0.3, 0.2}).Verdict, "rejection needs every judge")
	all := &Gate{Policy: PolicyAll}
	all.init()
	require.Equal(t, memory.VerdictAbstain, all.combine([]float64{0.97, 0.62}).Verdict)
}

// TestRun_HungJudgeDoesNotBlockOtherVotesOrUpgradeVoting is the isolation
// requirement: with every judge call hanging, the loop keeps voting memories
// the gate does not apply to and keeps voting on the upgrade proposal, and it
// never votes the memory that is waiting on the judge.
func TestRun_HungJudgeDoesNotBlockOtherVotesOrUpgradeVoting(t *testing.T) {
	var captured capturedTxs
	srv := captureServer(t, &captured)
	defer srv.Close()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	judged := gateRec("judged", "The depot opens at 07:00 on weekdays.", "notes")
	exempt := gateRec("exempt", "tool_x lists the files under a directory.", "catalog.tools")
	gs := newFakeGateStore(judged, exempt)
	hung := &fakeJudge{hang: true}
	app := &fakeApp{pid: "prop-1", target: 12, supported: true, ok: true, hasVote: map[string]bool{}}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		Run(ctx, app, gs, Config{Key: priv, CometRPC: srv.URL, PollInterval: 5 * time.Millisecond,
			Gate: &Gate{Judges: []LastingJudge{hung}, Version: "v1", ExemptDomainPrefixes: []string{"catalog."},
				Timeout: time.Hour}}, zerolog.Nop())
		close(done)
	}()

	voted := func(id string) bool {
		for _, p := range captured.all() {
			if p.MemoryVote != nil && p.MemoryVote.MemoryID == id {
				return true
			}
		}
		return false
	}
	upgradeVoted := func() bool {
		for _, p := range captured.all() {
			if p.Type == tx.TxTypeGovVote && p.GovVote != nil && p.GovVote.ProposalID == "prop-1" {
				return true
			}
		}
		return false
	}
	require.Eventually(t, func() bool { return voted("exempt") && upgradeVoted() }, 5*time.Second, 10*time.Millisecond,
		"unrelated voting and upgrade voting continue while the judge hangs")
	require.Eventually(t, func() bool { return hung.calls.Load() >= 1 }, 5*time.Second, 10*time.Millisecond)
	require.False(t, voted("judged"), "the memory waiting on the judge is not voted")

	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return promptly after cancellation while a judge call was hanging")
	}
}

// combineForTest runs the policy on a single judge's probability.
func (g *Gate) combineForTest(p float64) memory.SemanticVerdict {
	g.init()
	return g.combine([]float64{p})
}
