package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/poe"
)

// v8.4 domain-conditional quorum weight: post-fork, a validator's vote on a
// non-shared-domain memory is weighted by its verdict-correctness IN that
// domain (vstats_domain:<v>:<D>), with the global accuracy/recency/corroboration
// factors recomputed live from vstats:<v>. Shared (general/self) and
// unknown/legacy domains fall back to the v8.2 scalar weight.
//
//   DQ1 expert dissent flips : a proven pwn_heap expert's reject out-weighs two
//                              weak-in-pwn_heap accepts → deprecated, where the
//                              same votes commit under the equal-weight fallback.
//   DQ2 domain-conditional   : the SAME votes commit on a crypto memory and
//                              deprecate on a pwn_heap memory because expertise
//                              is per-domain.
//   DQ3 shared falls back    : a "general" memory ignores domain expertise.
//   DQ4 unknown falls back   : no memdomain: key → scalar fallback.
//   DQ5 pre-fork inert       : v8.4 inactive → no domain weighting, no per-domain
//                              crediting (global v8.3 crediting still fires).
//   DQ6 per-domain crediting : terminal verdict credits vstats_domain:<v>:<D>
//                              for the matchers; shared domains credit only global.

// activateV84 marks the v8.2/v8.3/v8.4 forks active (app-v5 ships only after the
// earlier activations on a real chain, so all three gates are set together).
func activateV84(app *SageApp, height int64) {
	app.v8_2AppliedHeight = height
	app.v8_3AppliedHeight = height
	app.v8_4AppliedHeight = height
}

// activateV85 marks the v8.2..v8.5 forks active at a single height — a coherent
// post-app-v6 gate set (currentAppVersion()==6). app-v6 ships only after the
// earlier activations on a real chain, so all gates are set together. The app-v6
// handler guards key on postV8_5Fork(height) (strict >), so a tx at height>height
// is post-fork. Mirrors activateV84.
func activateV85(app *SageApp, height int64) {
	app.v8_2AppliedHeight = height
	app.v8_3AppliedHeight = height
	app.v8_4AppliedHeight = height
	app.v8_5AppliedHeight = height
}

// seedDomainExpertise drives n verdict credits (all correct or all wrong) into a
// validator's per-domain EWMA via the real store path. n>=10 saturates the
// cold-start blend, so correct→accuracy≈1.0 and wrong→accuracy≈0.0.
func seedDomainExpertise(t *testing.T, app *SageApp, domain, vid string, correct bool, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		require.NoError(t, app.badgerStore.UpdateDomainVerdictStats(domain, map[string]bool{vid: correct}))
	}
}

// domainAccuracyOf reads a validator's live per-domain accuracy.
func domainAccuracyOf(t *testing.T, app *SageApp, vid, domain string) float64 {
	t.Helper()
	s, err := app.badgerStore.GetValidatorDomainStats(vid, domain)
	require.NoError(t, err)
	tr := &poe.EWMATracker{WeightedSum: s.EWMAWeightedSum, WeightDenom: s.EWMAWeightDenom, Count: int64(s.EWMACount)}
	return tr.Accuracy()
}

// vdomStatsOf reads a validator's per-domain stats record.
func vdomStatsOf(t *testing.T, app *SageApp, vid, domain string) (ewmaCount, corrCount uint64) {
	t.Helper()
	s, err := app.badgerStore.GetValidatorDomainStats(vid, domain)
	require.NoError(t, err)
	return s.EWMACount, s.CorrCount
}

// DQ1: a pwn_heap expert (qv0) rejects; two validators weak in pwn_heap (qv1,
// qv2) accept. Post-v8.4 the expert's reject carries enough domain weight to
// keep accept below 2/3 → deprecated. The identical scenario with v8.4 inactive
// uses the equal-weight fallback (2/3 accept) → committed. Same validators, same
// votes, opposite verdicts — the domain factor is doing the work.
func TestQuorumDomain_DQ1_ExpertDissentFlips(t *testing.T) {
	const memID = "mem-dq1"

	buildAndRun := func(app *SageApp) string {
		for _, id := range []string{qv0, qv1, qv2} {
			addQuorumValidator(t, app, id, 0) // PoEWeight 0 → 1/N fallback when domain weighting is off
		}
		seedDomainExpertise(t, app, "pwn_heap", qv0, true, 15)  // proven expert
		seedDomainExpertise(t, app, "pwn_heap", qv1, false, 15) // consistently wrong here
		seedDomainExpertise(t, app, "pwn_heap", qv2, false, 15)
		require.NoError(t, app.badgerStore.SetMemoryDomain(memID, "pwn_heap"))
		seedProposedMemory(t, app, memID)
		recordVote(t, app, memID, qv0, false) // expert rejects
		recordVote(t, app, memID, qv1, true)
		recordVote(t, app, memID, qv2, true)
		app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
		return statusOf(t, app, memID)
	}

	appOn := setupTestApp(t)
	activateV84(appOn, 100)
	require.True(t, appOn.postV8_4Fork(200))
	assert.Equal(t, string(memory.StatusDeprecated), buildAndRun(appOn),
		"post-v8.4: the pwn_heap expert's reject out-weighs two weak accepts → deprecated")
	// Sanity: the seeded expertise saturated as intended (read after the run).
	require.InDelta(t, 1.0, domainAccuracyOf(t, appOn, qv0, "pwn_heap"), 1e-9, "qv0 is a saturated pwn_heap expert")
	require.Less(t, domainAccuracyOf(t, appOn, qv1, "pwn_heap"), 0.1, "qv1 is consistently wrong in pwn_heap")

	appOff := setupTestApp(t)
	appOff.v8_2AppliedHeight = 100
	appOff.v8_3AppliedHeight = 100 // v8.4 deliberately NOT active
	require.False(t, appOff.postV8_4Fork(200))
	assert.Equal(t, string(memory.StatusCommitted), buildAndRun(appOff),
		"v8.4 inactive: equal-weight fallback → 2/3 accept → committed")
}

// DQ2: expertise is per-domain. qv0 is a pwn_heap expert but weak in crypto;
// qv1/qv2 are the reverse. With the SAME votes (qv0 reject, qv1/qv2 accept), a
// pwn_heap memory deprecates (expert reject dominates) while a crypto memory
// commits (the accepting crypto experts dominate).
func TestQuorumDomain_DQ2_DomainConditional(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 100)
	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}
	// qv0: pwn_heap expert, crypto novice (wrong). qv1/qv2: the mirror.
	seedDomainExpertise(t, app, "pwn_heap", qv0, true, 15)
	seedDomainExpertise(t, app, "pwn_heap", qv1, false, 15)
	seedDomainExpertise(t, app, "pwn_heap", qv2, false, 15)
	seedDomainExpertise(t, app, "crypto", qv0, false, 15)
	seedDomainExpertise(t, app, "crypto", qv1, true, 15)
	seedDomainExpertise(t, app, "crypto", qv2, true, 15)

	run := func(memID, domain string) string {
		require.NoError(t, app.badgerStore.SetMemoryDomain(memID, domain))
		seedProposedMemory(t, app, memID)
		recordVote(t, app, memID, qv0, false) // same votes for both memories
		recordVote(t, app, memID, qv1, true)
		recordVote(t, app, memID, qv2, true)
		app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
		return statusOf(t, app, memID)
	}

	assert.Equal(t, string(memory.StatusDeprecated), run("mem-dq2-pwn", "pwn_heap"),
		"pwn_heap: the pwn_heap expert (qv0) rejects → deprecated")
	assert.Equal(t, string(memory.StatusCommitted), run("mem-dq2-crypto", "crypto"),
		"crypto: the crypto experts (qv1,qv2) accept → committed; same votes, opposite outcome")
}

// DQ3: a shared domain ("general") falls back to the scalar weight even with
// v8.4 active — domain expertise in a catch-all is meaningless, so qv0's
// "expertise" is ignored and the 2/3 accept majority commits. No per-domain
// crediting fires for the shared domain.
func TestQuorumDomain_DQ3_SharedDomainFallsBack(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 100)
	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}
	seedDomainExpertise(t, app, "general", qv0, true, 15) // would dominate if it counted
	const memID = "mem-dq3"
	require.NoError(t, app.badgerStore.SetMemoryDomain(memID, "general"))
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, false)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, true)

	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID),
		"shared domain → fallback equal weight → 2/3 accept → committed")

	// Shared domain must not accrue per-domain expertise from this verdict.
	for _, id := range []string{qv1, qv2} {
		ec, _ := vdomStatsOf(t, app, id, "general")
		assert.Equal(t, uint64(0), ec, "%s: shared domain credits nothing per-domain", id)
	}
}

// DQ4: a memory with no memdomain: key (legacy/unknown) falls back to the scalar
// weight even with v8.4 active, regardless of qv0's expertise in other domains.
func TestQuorumDomain_DQ4_UnknownDomainFallsBack(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 100)
	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}
	seedDomainExpertise(t, app, "pwn_heap", qv0, true, 15)
	const memID = "mem-dq4"
	// Deliberately NO SetMemoryDomain → GetMemoryDomain returns "".
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, false)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, true)

	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID),
		"unknown domain (no memdomain:) → fallback equal weight → committed")
}

// DQ5: with v8.4 inactive (only v8.3), domain weighting is off and per-domain
// crediting never fires — but the global v8.3 verdict-correctness crediting
// still works. Proves the v8.4 surface is fully behind its own fork.
func TestQuorumDomain_DQ5_PreForkInert(t *testing.T) {
	app := setupTestApp(t)
	app.v8_2AppliedHeight = 100
	app.v8_3AppliedHeight = 100 // v8.4 NOT active
	require.False(t, app.postV8_4Fork(200))

	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}
	const memID = "mem-dq5"
	require.NoError(t, app.badgerStore.SetMemoryDomain(memID, "pwn_heap")) // present but ignored pre-fork
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, false)

	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID),
		"2/3 accept → committed via the v8.3 path")

	// No per-domain crediting pre-v8.4 …
	for _, id := range []string{qv0, qv1} {
		ec, _ := vdomStatsOf(t, app, id, "pwn_heap")
		assert.Equal(t, uint64(0), ec, "%s: no per-domain credit before v8.4 fork", id)
	}
	// … but the global v8.3 verdict crediting still fired.
	assert.Equal(t, uint64(1), vstatsOf(t, app, qv0).CorrCount, "global v8.3 crediting unaffected")
}

// DQ6: post-v8.4, a terminal verdict credits the per-domain EWMA for the
// matchers (alongside the global accumulator). Cold-start domain stats give
// equal weights, so a 2/3 accept majority commits; then the per-domain records
// accrue. A second, shared-domain memory credits only the global accumulator.
func TestQuorumDomain_DQ6_PerDomainCrediting(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 100)
	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}

	// Non-shared domain: cold start → equal weights → 2/3 accept commits.
	const memID = "mem-dq6"
	require.NoError(t, app.badgerStore.SetMemoryDomain(memID, "pwn_heap"))
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, false)
	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID))

	// Matchers (accept = commit verdict) credited per-domain AND globally.
	for _, id := range []string{qv0, qv1} {
		ec, cc := vdomStatsOf(t, app, id, "pwn_heap")
		assert.Equal(t, uint64(1), ec, "%s: one per-domain verdict participation", id)
		assert.Equal(t, uint64(1), cc, "%s: matched the commit verdict → per-domain corr++", id)
		assert.Equal(t, uint64(1), vstatsOf(t, app, id).CorrCount, "%s: global corr also credited", id)
	}
	// Dissenter participated per-domain but earned no corroboration.
	ec, cc := vdomStatsOf(t, app, qv2, "pwn_heap")
	assert.Equal(t, uint64(1), ec, "qv2 participated in the verdict")
	assert.Equal(t, uint64(0), cc, "qv2 dissented from the commit verdict → no per-domain corr")

	// A shared-domain verdict credits only the global accumulator.
	const memID2 = "mem-dq6-shared"
	require.NoError(t, app.badgerStore.SetMemoryDomain(memID2, "self"))
	seedProposedMemory(t, app, memID2)
	recordVote(t, app, memID2, qv0, true)
	recordVote(t, app, memID2, qv1, true)
	recordVote(t, app, memID2, qv2, true)
	app.checkAndApplyQuorum(memID2, 201, time.Unix(2010, 0))
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID2))
	ecShared, _ := vdomStatsOf(t, app, qv0, "self")
	assert.Equal(t, uint64(0), ecShared, "shared domain 'self' accrues no per-domain expertise")
}

// DQ7: the per-domain credit is idempotent under replayed votes — it fires once,
// on the first proposed→terminal transition, and a re-run of checkAndApplyQuorum
// on the already-committed memory credits nothing more (the priorStatus guard the
// per-domain credit shares with the global v8.3 credit). Without this, a replayed
// vote would inflate per-domain expertise.
func TestQuorumDomain_DQ7_PerDomainCreditIdempotent(t *testing.T) {
	app := setupTestApp(t)
	activateV84(app, 100)
	for _, id := range []string{qv0, qv1, qv2} {
		addQuorumValidator(t, app, id, 0)
	}
	const memID = "mem-dq7"
	require.NoError(t, app.badgerStore.SetMemoryDomain(memID, "pwn_heap"))
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, false)

	// First pass commits and credits per-domain exactly once.
	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))
	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID))
	ec0, cc0 := vdomStatsOf(t, app, qv0, "pwn_heap")
	require.Equal(t, uint64(1), ec0)
	require.Equal(t, uint64(1), cc0)

	// Replay the quorum (and a replayed vote) on the now-terminal memory.
	app.checkAndApplyQuorum(memID, 201, time.Unix(2010, 0))
	recordVote(t, app, memID, qv2, false)
	app.checkAndApplyQuorum(memID, 202, time.Unix(2020, 0))

	// Per-domain stats are untouched by the replays — credited once, period.
	for _, id := range []string{qv0, qv1, qv2} {
		ec, _ := vdomStatsOf(t, app, id, "pwn_heap")
		assert.Equal(t, uint64(1), ec, "%s: per-domain credit must not re-fire on replay", id)
	}
}
