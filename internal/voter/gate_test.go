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

// fakeSupportJudge answers the evidence question with a fixed probability.
type fakeSupportJudge struct {
	p     float64
	calls atomic.Int64
}

func (f *fakeSupportJudge) SupportedProbability(_ context.Context, _, _ string) (float64, error) {
	f.calls.Add(1)
	return f.p, nil
}

// fakeGateStore is fakeStore plus the gate's node-local tables.
type fakeGateStore struct {
	fakeStore
	mu       sync.Mutex
	content  map[string]string
	evidence map[string]string // "" value = evidence exists but cannot be read
	expired  map[string]bool
	verdicts map[string]memory.SemanticVerdict // memoryID|version
	review   map[string]string
}

func newFakeGateStore(pending ...*memory.MemoryRecord) *fakeGateStore {
	f := &fakeGateStore{fakeStore: fakeStore{pending: pending, dups: map[string]bool{}},
		content: map[string]string{}, evidence: map[string]string{}, expired: map[string]bool{}, verdicts: map[string]memory.SemanticVerdict{}, review: map[string]string{}}
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
func (f *fakeGateStore) JudgeableEvidence(_ context.Context, id string) (string, bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.expired[id] {
		return "", true, memory.ErrEvidenceExpired
	}
	e, ok := f.evidence[id]
	if ok && e == "" {
		return "", false, errors.New("evidence unavailable")
	}
	return e, ok, nil
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

func TestGate_JudgeFailureHoldsForReview(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := gateRec("m1", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	g := &Gate{Judges: []LastingJudge{&fakeJudge{err: errors.New("connection refused")}}, Version: "v1"}
	g.Start(ctx, gs, zerolog.Nop())
	_, _ = g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Eventually(t, func() bool {
		out, d := g.Apply(ctx, gs, m, accept, zerolog.Nop())
		return out == gateHold && !d.Accept && g.recentlyFailed("m1")
	}, 5*time.Second, 10*time.Millisecond, "a failed configured judge cannot bypass the gate")
	require.Eventually(t, func() bool {
		v, ok, err := gs.SemanticVerdict(ctx, "m1", "v1")
		return err == nil && ok && v.Verdict == memory.VerdictAbstain
	}, 5*time.Second, 10*time.Millisecond, "the unavailable hold is visible in the review queue")
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
		return out == gateHold && g.recentlyFailed("m1")
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

// judgeOnce runs the background evaluator on one memory and returns its verdict.
func judgeOnce(t *testing.T, g *Gate, gs *fakeGateStore, m *memory.MemoryRecord) (gateOutcome, Decision) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	g.Start(ctx, gs, zerolog.Nop())
	out, _ := g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Equal(t, gateHold, out)
	require.Eventually(t, func() bool {
		_, ok, _ := gs.SemanticVerdict(ctx, m.MemoryID, g.Version)
		return ok
	}, 5*time.Second, 10*time.Millisecond)
	return g.Apply(ctx, gs, m, accept, zerolog.Nop())
}

func TestGate_EvidenceCheck(t *testing.T) {
	const claim = "The north gate alarm is disabled."
	for _, tc := range []struct {
		name     string
		evidence string
		support  []float64
		want     gateOutcome
		accept   bool
		reason   string
	}{
		{"supported by every judge", "Work order 118: north gate alarm disabled today.", []float64{0.98, 0.95}, gateOverride, true, "supported by its evidence p=0.95"},
		{"unsupported is rejected even when lasting", "At the 2023 inspection the north gate alarm was disabled.", []float64{0.04, 0.2}, gateOverride, false, "not supported by the evidence"},
		{"one unsure judge holds it: every judge must agree", "Work order 118: north gate alarm disabled today.", []float64{0.99, 0.86}, gateHold, false, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := gateRec("m1", claim, "notes")
			gs := newFakeGateStore(m)
			gs.evidence["m1"] = tc.evidence
			g := &Gate{Judges: []LastingJudge{&fakeJudge{p: 0.99}}, Version: "v1"}
			for _, p := range tc.support {
				g.SupportJudges = append(g.SupportJudges, &fakeSupportJudge{p: p})
			}
			out, d := judgeOnce(t, g, gs, m)
			require.Equal(t, tc.want, out)
			require.Equal(t, tc.accept, d.Accept)
			require.Contains(t, d.Reason, tc.reason)
		})
	}
}

func TestGate_NoEvidenceMeansTheEvidenceCheckIsNotAsked(t *testing.T) {
	m := gateRec("m1", "The depot opens at 07:00 on weekdays.", "notes")
	gs := newFakeGateStore(m)
	sj := &fakeSupportJudge{p: 0.01}
	g := &Gate{Judges: []LastingJudge{&fakeJudge{p: 0.97}}, SupportJudges: []SupportJudge{sj}, Version: "v1"}
	out, d := judgeOnce(t, g, gs, m)
	require.Equal(t, gateOverride, out)
	require.True(t, d.Accept, "a memory without evidence is judged exactly as before")
	require.Zero(t, sj.calls.Load())
}

func TestGate_ExpiredEvidenceIsHeldForReviewNotJudgedWithout(t *testing.T) {
	m := gateRec("m1", "The alarm is disabled.", "notes")
	gs := newFakeGateStore(m)
	gs.expired["m1"] = true
	j, sj := &fakeJudge{p: 0.99}, &fakeSupportJudge{p: 0.99}
	g := &Gate{Judges: []LastingJudge{j}, SupportJudges: []SupportJudge{sj}, Version: "v1"}
	out, _ := judgeOnce(t, g, gs, m)
	require.Equal(t, gateHold, out, "neither the no-evidence path nor the built-in fallback")
	v, ok, _ := gs.SemanticVerdict(context.Background(), "m1", "v1")
	require.True(t, ok)
	require.Equal(t, memory.VerdictAbstain, v.Verdict)
	require.Contains(t, v.Reason, "expired")
	require.Zero(t, sj.calls.Load())

	gs.review["m1"] = memory.VerdictAccept // the operator decides from the memory alone
	out, d := g.Apply(context.Background(), gs, m, accept, zerolog.Nop())
	require.Equal(t, gateOverride, out)
	require.True(t, d.Accept)
}

func TestGate_UnreadableEvidenceIsNeverSent(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m := gateRec("m1", "The north gate alarm is disabled.", "notes")
	gs := newFakeGateStore(m)
	gs.evidence["m1"] = "" // exists, cannot be decrypted
	j, sj := &fakeJudge{p: 0.99}, &fakeSupportJudge{p: 0.99}
	g := &Gate{Judges: []LastingJudge{j}, SupportJudges: []SupportJudge{sj}, Version: "v1"}
	g.Start(ctx, gs, zerolog.Nop())
	_, _ = g.Apply(ctx, gs, m, accept, zerolog.Nop())
	require.Eventually(t, func() bool {
		out, _ := g.Apply(ctx, gs, m, accept, zerolog.Nop())
		return out == gateHold && g.recentlyFailed("m1")
	}, 5*time.Second, 10*time.Millisecond, "unreadable evidence must remain held")
	require.Zero(t, j.calls.Load()+sj.calls.Load(), "nothing is sent when the evidence cannot be read")
	_, ok, _ := gs.SemanticVerdict(ctx, "m1", "v1")
	require.True(t, ok)
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
	defer cancel()
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
	require.Eventually(t, func() bool {
		_, fenced := tx.FenceForSigner(priv)
		return !fenced
	}, 5*time.Second, 10*time.Millisecond, "any canceled broadcast must reconcile before the mock RPC closes")
}

// combineForTest runs the policy on a single judge's probability.
func (g *Gate) combineForTest(p float64) memory.SemanticVerdict {
	g.init()
	return g.combine([]float64{p})
}
