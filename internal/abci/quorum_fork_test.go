package abci

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/validator"
)

// 64-hex-char fake validator IDs — match the test convention (real validator
// IDs are SHA-256 hashes; the consensus logger truncates to v.ID[:16] which
// crashes for shorter strings).
const (
	qv0 = "11111111111111111111111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	qv1 = "22222222222222222222222222222222bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	qv2 = "33333333333333333333333333333333cccccccccccccccccccccccccccccccc"
	qv3 = "44444444444444444444444444444444dddddddddddddddddddddddddddddddd"
	qv4 = "55555555555555555555555555555555eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
)

// v8.2 PoE-weighted quorum: pin the fork-gate semantics in checkAndApplyQuorum.
//
//   F1 pre-fork    : equal-weight branch ignores PoEWeight → 2-2 split deprecates.
//   F2 post-fork   : same setup, PoEWeight tips quorum → committed.
//   F3 cold-boot   : post-fork, all PoEWeight == 0 → 1/N fallback → 2-2 deprecates.
//   F4 mixed       : post-fork, one cold-start + three persisted → fallback counts.
//   F5 gate flip   : same scenario at H_act vs H_act+1 produces opposite decisions.
//   F6 RepCap      : post-fork outcome equals hand-computed normalized weight ratio.
//
// Tests drive checkAndApplyQuorum directly with app.validators pre-populated and
// vote:* keys pre-written. SetMemoryHash seeds the memory in 'proposed' state so
// the transition to committed / deprecated is observable via GetMemoryHash.

// addQuorumValidator installs a validator with a fixed PoEWeight on app.validators.
// Bypasses governance — these tests exercise the quorum branch, not the gov path.
func addQuorumValidator(t *testing.T, app *SageApp, id string, weight float64) {
	t.Helper()
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID:        id,
		Power:     1,
		PoEWeight: weight,
	}))
}

// recordVote pre-populates the vote:<mem>:<v> key the quorum branch reads.
func recordVote(t *testing.T, app *SageApp, memoryID, validatorID string, accept bool) {
	t.Helper()
	decision := "reject"
	if accept {
		decision = "accept"
	}
	require.NoError(t, app.badgerStore.SetState("vote:"+memoryID+":"+validatorID, []byte(decision)))
}

// seedProposedMemory writes the memory entry so GetMemoryHash returns the
// current status. checkAndApplyQuorum's SetMemoryHash call overwrites it on a
// quorum transition.
func seedProposedMemory(t *testing.T, app *SageApp, memoryID string) {
	t.Helper()
	require.NoError(t, app.badgerStore.SetMemoryHash(memoryID, nil, string(memory.StatusProposed)))
}

// statusOf reads the current status without leaking the rest of GetMemoryHash.
func statusOf(t *testing.T, app *SageApp, memoryID string) string {
	t.Helper()
	_, status, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	return status
}

// F1: pre-fork. PoEWeights say accept-side should dominate (0.6+0.1 vs 0.1+0.1)
// but the quorum branch ignores them — equal weights give 2/4 = 50% < 2/3, all
// validators voted, so the memory is deprecated.
func TestQuorumFork_F1_PreForkEqualWeights(t *testing.T) {
	app := setupTestApp(t)
	require.Equal(t, int64(0), app.v8_2AppliedHeight, "precondition: fork inactive")

	addQuorumValidator(t, app, qv0, 0.6)
	addQuorumValidator(t, app, qv1, 0.1)
	addQuorumValidator(t, app, qv2, 0.1)
	addQuorumValidator(t, app, qv3, 0.1)

	const memID = "mem-f1"
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, false)
	recordVote(t, app, memID, qv3, false)

	app.checkAndApplyQuorum(memID, 50, time.Unix(2000, 0))

	assert.Equal(t, string(memory.StatusDeprecated), statusOf(t, app, memID),
		"pre-fork: 2-2 split (equal weights) → deprecated")
}

// F2: post-fork. Identical setup as F1 but the gate is active — PoEWeight
// values drive the decision. Accept side carries 0.6+0.1=0.7 of 0.9 total
// (77.8% >= 2/3), so the memory commits.
func TestQuorumFork_F2_PostForkPoEWeights(t *testing.T) {
	app := setupTestApp(t)
	app.v8_2AppliedHeight = 100
	require.True(t, app.postV8_2Fork(200), "precondition: fork active at H=200")

	addQuorumValidator(t, app, qv0, 0.6)
	addQuorumValidator(t, app, qv1, 0.1)
	addQuorumValidator(t, app, qv2, 0.1)
	addQuorumValidator(t, app, qv3, 0.1)

	const memID = "mem-f2"
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, false)
	recordVote(t, app, memID, qv3, false)

	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))

	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID),
		"post-fork: weighted accept (0.7/0.9 = 77.8%%) >= 2/3 → committed")
}

// F3: post-fork, but every validator has PoEWeight == 0 (pre-first-epoch chain
// or fresh state-restore). poeWeightOrFallback returns 1/N for every validator,
// reproducing the equal-weight pre-fork behavior — closes the cold-boot hazard.
func TestQuorumFork_F3_PostForkColdBootFallback(t *testing.T) {
	app := setupTestApp(t)
	app.v8_2AppliedHeight = 100

	for _, id := range []string{qv0, qv1, qv2, qv3} {
		addQuorumValidator(t, app, id, 0)
	}

	const memID = "mem-f3"
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv1, true)
	recordVote(t, app, memID, qv2, false)
	recordVote(t, app, memID, qv3, false)

	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))

	assert.Equal(t, string(memory.StatusDeprecated), statusOf(t, app, memID),
		"cold-boot post-fork: fallback 1/N → 2-2 split → deprecated (same as F1)")
}

// F4: post-fork, mixed. v3 was added mid-epoch (PoEWeight == 0), the rest
// carry persisted weights. v3 falls back to 1/N=0.25; the accept side wins
// only because v3's vote counts.
//
//	Weights: v0=0.5 v1=0.1 v2=0.1 v3=fallback(0.25)
//	Votes  : v0=accept, v3=accept, v1=reject, v2=reject
//	Accept : 0.5 + 0.25 = 0.75
//	Total  : 0.5 + 0.1 + 0.1 + 0.25 = 0.95
//	Ratio  : 0.789 >= 2/3 → committed
func TestQuorumFork_F4_PostForkMidEpochValidator(t *testing.T) {
	app := setupTestApp(t)
	app.v8_2AppliedHeight = 100

	addQuorumValidator(t, app, qv0, 0.5)
	addQuorumValidator(t, app, qv1, 0.1)
	addQuorumValidator(t, app, qv2, 0.1)
	addQuorumValidator(t, app, qv3, 0) // fresh add — no epoch boundary yet

	const memID = "mem-f4"
	seedProposedMemory(t, app, memID)
	recordVote(t, app, memID, qv0, true)
	recordVote(t, app, memID, qv3, true)
	recordVote(t, app, memID, qv1, false)
	recordVote(t, app, memID, qv2, false)

	app.checkAndApplyQuorum(memID, 200, time.Unix(2000, 0))

	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memID),
		"mid-epoch validator with 1/N fallback counts toward quorum")

	// Without v3's vote (or with v3 voting reject), the same setup should
	// fail quorum — pin the inverse to prove v3's vote is load-bearing.
	const memIDB = "mem-f4b"
	seedProposedMemory(t, app, memIDB)
	recordVote(t, app, memIDB, qv0, true)
	recordVote(t, app, memIDB, qv3, false) // v3 flips → accept loses
	recordVote(t, app, memIDB, qv1, false)
	recordVote(t, app, memIDB, qv2, false)

	app.checkAndApplyQuorum(memIDB, 201, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusDeprecated), statusOf(t, app, memIDB),
		"if v3 flips reject, the weighted accept (0.5/0.95 = 52.6%%) fails 2/3")
}

// F5: same inputs, same block, opposite decisions at H_act vs H_act+1. The
// strict-greater-than gate is the only thing that differs — locks the
// "applied at H+1" semantic for the v8.2 fork.
func TestQuorumFork_F5_GateFlipAtHPlusOne(t *testing.T) {
	const activation = 1000

	// Run the same setup at H=activation (pre-fork — equal weights).
	app1 := setupTestApp(t)
	app1.v8_2AppliedHeight = activation
	for _, v := range []struct {
		id string
		w  float64
	}{{qv0, 0.6}, {qv1, 0.1}, {qv2, 0.1}, {qv3, 0.1}} {
		addQuorumValidator(t, app1, v.id, v.w)
	}
	const memID = "mem-f5"
	seedProposedMemory(t, app1, memID)
	recordVote(t, app1, memID, qv0, true)
	recordVote(t, app1, memID, qv1, true)
	recordVote(t, app1, memID, qv2, false)
	recordVote(t, app1, memID, qv3, false)
	app1.checkAndApplyQuorum(memID, activation, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusDeprecated), statusOf(t, app1, memID),
		"at activation block: still pre-fork → equal weights → deprecated")

	// Same setup, same vote pattern, but at H = activation + 1 (post-fork).
	app2 := setupTestApp(t)
	app2.v8_2AppliedHeight = activation
	for _, v := range []struct {
		id string
		w  float64
	}{{qv0, 0.6}, {qv1, 0.1}, {qv2, 0.1}, {qv3, 0.1}} {
		addQuorumValidator(t, app2, v.id, v.w)
	}
	seedProposedMemory(t, app2, memID)
	recordVote(t, app2, memID, qv0, true)
	recordVote(t, app2, memID, qv1, true)
	recordVote(t, app2, memID, qv2, false)
	recordVote(t, app2, memID, qv3, false)
	app2.checkAndApplyQuorum(memID, activation+1, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app2, memID),
		"first post-activation block: weighted quorum → committed")
}

// F6: RepCap interaction. The plan's contract is that whatever post-NormalizeWeights
// value lands in PoEWeight is exactly what the quorum branch consumes — no
// re-capping, no renormalization. We assert that for a hand-computed normalized
// set, the accept/total ratio drives the same decision the math predicts.
//
//	5 validators with normalized weights summing to 1.0:
//	  v0=0.45 v1=0.05 v2=0.05 v3=0.05 v4=0.40
//	Accept: v0+v4 = 0.85
//	Total : 1.0
//	Ratio : 0.85 >= 2/3 → committed
//
//	Flip: v0=accept, v1..v4 reject
//	Accept: 0.45
//	Total : 1.0
//	Ratio : 0.45 < 2/3 → deprecated (all voted)
func TestQuorumFork_F6_PostForkRepCapWeightedRatio(t *testing.T) {
	app := setupTestApp(t)
	app.v8_2AppliedHeight = 100

	weights := []struct {
		id string
		w  float64
	}{
		{qv0, 0.45},
		{qv1, 0.05},
		{qv2, 0.05},
		{qv3, 0.05},
		{qv4, 0.40},
	}
	for _, v := range weights {
		addQuorumValidator(t, app, v.id, v.w)
	}

	// Case A: v0 + v4 accept (0.85), others reject (0.15) → committed
	const memA = "mem-f6a"
	seedProposedMemory(t, app, memA)
	recordVote(t, app, memA, qv0, true)
	recordVote(t, app, memA, qv4, true)
	recordVote(t, app, memA, qv1, false)
	recordVote(t, app, memA, qv2, false)
	recordVote(t, app, memA, qv3, false)
	app.checkAndApplyQuorum(memA, 200, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusCommitted), statusOf(t, app, memA),
		"0.85 / 1.0 >= 2/3 → committed")

	// Case B: only v0 accepts (0.45), all others reject → deprecated
	const memB = "mem-f6b"
	seedProposedMemory(t, app, memB)
	recordVote(t, app, memB, qv0, true)
	recordVote(t, app, memB, qv1, false)
	recordVote(t, app, memB, qv2, false)
	recordVote(t, app, memB, qv3, false)
	recordVote(t, app, memB, qv4, false)
	app.checkAndApplyQuorum(memB, 201, time.Unix(2000, 0))
	assert.Equal(t, string(memory.StatusDeprecated), statusOf(t, app, memB),
		"0.45 / 1.0 < 2/3 with all voted → deprecated")
}
