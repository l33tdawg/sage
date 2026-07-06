package governance

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCheckGovQuorum_ThreeValidatorsEqualPower_TwoAccept(t *testing.T) {
	votes := map[string]string{
		"v1": "accept",
		"v2": "accept",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	passed, rejected, acceptPower, rejectPower, totalPower := CheckGovQuorum(votes, powers)
	assert.True(t, passed, "2/3 equal-power validators accepting should pass")
	assert.False(t, rejected)
	assert.Equal(t, int64(20), acceptPower)
	assert.Equal(t, int64(0), rejectPower)
	assert.Equal(t, int64(30), totalPower)
}

func TestCheckGovQuorum_ThreeValidatorsEqualPower_OneAccept(t *testing.T) {
	votes := map[string]string{
		"v1": "accept",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	passed, rejected, acceptPower, _, totalPower := CheckGovQuorum(votes, powers)
	assert.False(t, passed, "1/3 equal-power validators accepting should not pass")
	assert.False(t, rejected)
	assert.Equal(t, int64(10), acceptPower)
	assert.Equal(t, int64(30), totalPower)
}

func TestCheckGovQuorum_ThreeValidatorsEqualPower_TwoReject(t *testing.T) {
	votes := map[string]string{
		"v1": "accept",
		"v2": "reject",
		"v3": "reject",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	passed, rejected, _, rejectPower, totalPower := CheckGovQuorum(votes, powers)
	assert.False(t, passed)
	assert.True(t, rejected, "2/3 equal-power validators rejecting should reject")
	assert.Equal(t, int64(20), rejectPower)
	assert.Equal(t, int64(30), totalPower)
}

func TestCheckGovQuorum_FiveValidators_ExactBoundary(t *testing.T) {
	// 5 validators: total power = 30
	// 2/3 threshold: acceptPower*3 >= 30*2 = 60  =>  acceptPower >= 20
	// v1(10) + v2(10) = 20 => 20*3=60 >= 60 => PASSES exactly at boundary
	votes := map[string]string{
		"v1": "accept",
		"v2": "accept",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 4,
		"v4": 3,
		"v5": 3,
	}

	passed, rejected, acceptPower, _, totalPower := CheckGovQuorum(votes, powers)
	assert.True(t, passed, "exact 2/3 boundary should pass (20*3=60 >= 30*2=60)")
	assert.False(t, rejected)
	assert.Equal(t, int64(20), acceptPower)
	assert.Equal(t, int64(30), totalPower)

	// Now one less power: 19*3=57 < 60 => should NOT pass
	powers2 := map[string]int64{
		"v1": 10,
		"v2": 9,
		"v3": 4,
		"v4": 4,
		"v5": 3,
	}
	passed2, _, acceptPower2, _, totalPower2 := CheckGovQuorum(votes, powers2)
	assert.False(t, passed2, "just below 2/3 boundary should not pass (19*3=57 < 30*2=60)")
	assert.Equal(t, int64(19), acceptPower2)
	assert.Equal(t, int64(30), totalPower2)
}

func TestCheckGovQuorum_SingleValidatorAccept(t *testing.T) {
	votes := map[string]string{
		"v1": "accept",
	}
	powers := map[string]int64{
		"v1": 100,
	}

	passed, rejected, acceptPower, _, totalPower := CheckGovQuorum(votes, powers)
	assert.True(t, passed, "single validator accepting should pass")
	assert.False(t, rejected)
	assert.Equal(t, int64(100), acceptPower)
	assert.Equal(t, int64(100), totalPower)
}

func TestCheckGovQuorum_ZeroTotalPower(t *testing.T) {
	votes := map[string]string{}
	powers := map[string]int64{}

	passed, rejected, acceptPower, rejectPower, totalPower := CheckGovQuorum(votes, powers)
	assert.False(t, passed, "zero total power should not pass")
	assert.False(t, rejected, "zero total power should not reject")
	assert.Equal(t, int64(0), acceptPower)
	assert.Equal(t, int64(0), rejectPower)
	assert.Equal(t, int64(0), totalPower)
}

func TestCheckGovQuorum_LargePowerValues(t *testing.T) {
	// Test with values near MaxInt64/3 to check overflow safety.
	// MaxInt64 = 9223372036854775807
	// MaxInt64/3 ~= 3074457345618258602
	// We use values that won't overflow when multiplied by 3.
	bigPower := int64(math.MaxInt64 / 4) // 2305843009213693951
	votes := map[string]string{
		"v1": "accept",
		"v2": "accept",
	}
	powers := map[string]int64{
		"v1": bigPower,
		"v2": bigPower,
		"v3": bigPower,
	}

	passed, rejected, acceptPower, _, totalPower := CheckGovQuorum(votes, powers)
	assert.True(t, passed, "2/3 of large power validators accepting should pass")
	assert.False(t, rejected)
	assert.Equal(t, bigPower*2, acceptPower)
	assert.Equal(t, bigPower*3, totalPower)
}

func TestCheckGovQuorum_AllAbstain(t *testing.T) {
	votes := map[string]string{
		"v1": "abstain",
		"v2": "abstain",
		"v3": "abstain",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	passed, rejected, acceptPower, rejectPower, totalPower := CheckGovQuorum(votes, powers)
	assert.False(t, passed, "all abstain should not pass")
	assert.False(t, rejected, "all abstain should not reject")
	assert.Equal(t, int64(0), acceptPower)
	assert.Equal(t, int64(0), rejectPower)
	assert.Equal(t, int64(30), totalPower)
}

func TestCheckGovQuorum_MixedVotesUnequalPower(t *testing.T) {
	// v1 has dominant power: 60 out of 100.
	// v1 accepts => 60*3=180 >= 100*2=200? NO. Not enough alone.
	votes := map[string]string{
		"v1": "accept",
		"v2": "reject",
		"v3": "abstain",
	}
	powers := map[string]int64{
		"v1": 60,
		"v2": 30,
		"v3": 10,
	}

	passed, rejected, acceptPower, rejectPower, totalPower := CheckGovQuorum(votes, powers)
	assert.False(t, passed, "60/100 acceptance (60*3=180 < 100*2=200) should not pass")
	assert.False(t, rejected, "30/100 rejection (30*3=90 < 100) should not reject")
	assert.Equal(t, int64(60), acceptPower)
	assert.Equal(t, int64(30), rejectPower)
	assert.Equal(t, int64(100), totalPower)

	// Now v1(70) accepts: 70*3=210 >= 100*2=200 => PASSES
	powers["v1"] = 70
	powers["v3"] = 0 // adjust total to stay at 100
	passed2, _, acceptPower2, _, _ := CheckGovQuorum(votes, powers)
	assert.True(t, passed2, "70/100 acceptance should pass (70*3=210 >= 100*2=200)")
	assert.Equal(t, int64(70), acceptPower2)
}

func TestCheckGovQuorum_RejectBoundary(t *testing.T) {
	// totalPower = 30. Reject threshold: rejectPower*3 > 30 => rejectPower > 10
	// rejectPower = 10: 10*3 = 30, NOT > 30 => NOT rejected
	votes := map[string]string{
		"v1": "reject",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	_, rejected, _, rejectPower, _ := CheckGovQuorum(votes, powers)
	assert.False(t, rejected, "exactly 1/3 reject should NOT reject (10*3=30, not > 30)")
	assert.Equal(t, int64(10), rejectPower)

	// rejectPower = 11: 11*3 = 33 > 30 => REJECTED
	powers["v1"] = 11
	powers["v2"] = 10
	powers["v3"] = 9
	_, rejected2, _, rejectPower2, _ := CheckGovQuorum(votes, powers)
	assert.True(t, rejected2, "just over 1/3 reject should reject (11*3=33 > 30)")
	assert.Equal(t, int64(11), rejectPower2)
}

func TestCheckGovQuorum_NonVotersCounted(t *testing.T) {
	// v3 hasn't voted but its power still counts toward total.
	votes := map[string]string{
		"v1": "accept",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	passed, _, acceptPower, _, totalPower := CheckGovQuorum(votes, powers)
	assert.False(t, passed, "10*3=30 < 30*2=60, non-voters inflate total power")
	assert.Equal(t, int64(10), acceptPower)
	assert.Equal(t, int64(30), totalPower)
}

// ---------------------------------------------------------------------------
// v8.0: per-op threshold (ThresholdFor + CheckGovQuorumOp)
// ---------------------------------------------------------------------------

// TestThresholdFor pins the per-op supermajority. OpDomainReassign is 3/4,
// everything else (validator-set changes) keeps the historical 2/3. Changing
// these numbers is a consensus fork — this test guards the wire constants.
func TestThresholdFor(t *testing.T) {
	cases := []struct {
		op      ProposalOp
		wantNum int64
		wantDen int64
	}{
		{OpAddValidator, 2, 3},
		{OpRemoveValidator, 2, 3},
		{OpUpdatePower, 2, 3},
		{OpDomainReassign, 3, 4},
		// OpMemoryDomainRepair (app-v16) uses the DEFAULT 2/3 — ThresholdFor is
		// fork-unaware and must not retroactively change a new op's quorum (replay
		// parity); its safety is admin-propose + supermajority + guarded apply.
		{OpMemoryDomainRepair, 2, 3},
	}
	for _, tc := range cases {
		num, den := ThresholdFor(tc.op)
		assert.Equal(t, tc.wantNum, num, "op=%d num", tc.op)
		assert.Equal(t, tc.wantDen, den, "op=%d den", tc.op)
	}
}

// TestCheckGovQuorumOp_DomainReassign_3of4Passes asserts a 3/4 supermajority
// passes OpDomainReassign on a 4-validator network (3 accept of 4 equal-power
// validators). The same vote count would also pass the 2/3 op default — we
// exercise both paths in the next test to make sure 2/3 is genuinely weaker.
func TestCheckGovQuorumOp_DomainReassign_3of4Passes(t *testing.T) {
	votes := map[string]string{
		"v1": "accept",
		"v2": "accept",
		"v3": "accept",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
		"v4": 10,
	}

	passed, rejected, acceptPower, _, totalPower := CheckGovQuorumOp(votes, powers, OpDomainReassign)
	assert.True(t, passed, "3/4 accept must pass OpDomainReassign (30*4=120 >= 40*3=120)")
	assert.False(t, rejected)
	assert.Equal(t, int64(30), acceptPower)
	assert.Equal(t, int64(40), totalPower)
}

// TestCheckGovQuorumOp_DomainReassign_2of4Fails — exact threshold check.
// 2/4 (=1/2) is enough for 2/3 (20*3=60 >= 40*2=80? no... 60 < 80, so 2/4
// FAILS even the 2/3 threshold). But 3/4 with one missing vote (acceptPower
// = 30 of 40, but assume only 2 accepted with v3 abstaining) should fail.
// The discriminating case is acceptPower=20 on totalPower=30 with a missing
// fourth validator — passes 2/3 (20*3=60 >= 30*2=60) but fails 3/4
// (20*4=80 < 30*3=90).
//
// We exercise the case the spec calls out: at N=4, 2 of 4 accept passes
// neither threshold (1/2 < both 2/3 and 3/4), but 2 of 3 (i.e. acceptPower
// = 20, totalPower = 30) passes 2/3 and fails 3/4.
func TestCheckGovQuorumOp_DomainReassign_2of3PassesDefaultFailsReassign(t *testing.T) {
	// 3 equal-power validators, 2 accept. acceptPower=20, totalPower=30.
	votes := map[string]string{
		"v1": "accept",
		"v2": "accept",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	// 2/3 path (e.g. OpAddValidator): 20*3=60 >= 30*2=60 → passes.
	passedAdd, _, _, _, _ := CheckGovQuorumOp(votes, powers, OpAddValidator)
	assert.True(t, passedAdd, "2 of 3 accept must pass the 2/3 OpAddValidator threshold")

	// 3/4 path (OpDomainReassign): 20*4=80 < 30*3=90 → fails.
	passedReassign, _, acceptPower, _, totalPower := CheckGovQuorumOp(votes, powers, OpDomainReassign)
	assert.False(t, passedReassign, "2 of 3 accept must NOT pass the 3/4 OpDomainReassign threshold (20*4=80 < 30*3=90)")
	assert.Equal(t, int64(20), acceptPower)
	assert.Equal(t, int64(30), totalPower)
}

// TestCheckGovQuorumOp_DomainReassign_RejectThreshold pins the 3/4
// rejection boundary: reject when rejectPower*4 > totalPower*1 (i.e. > 1/4).
// At N=4 with equal power, a single reject is exactly 1/4 (10*4=40, not > 40)
// so NOT rejected; two rejects (20*4=80 > 40) DOES reject.
func TestCheckGovQuorumOp_DomainReassign_RejectThreshold(t *testing.T) {
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
		"v4": 10,
	}

	// One reject — exactly 1/4. NOT rejected (need strictly > 1/4).
	votesOne := map[string]string{"v1": "reject"}
	_, rejectedOne, _, rejectPowerOne, _ := CheckGovQuorumOp(votesOne, powers, OpDomainReassign)
	assert.False(t, rejectedOne, "exactly 1/4 reject must NOT reject (10*4=40, not > 40)")
	assert.Equal(t, int64(10), rejectPowerOne)

	// Two rejects — 1/2 of total. Strictly > 1/4 → REJECTED.
	votesTwo := map[string]string{"v1": "reject", "v2": "reject"}
	_, rejectedTwo, _, rejectPowerTwo, _ := CheckGovQuorumOp(votesTwo, powers, OpDomainReassign)
	assert.True(t, rejectedTwo, "2 of 4 reject must reject (20*4=80 > 40)")
	assert.Equal(t, int64(20), rejectPowerTwo)
}

// TestCheckGovQuorumOp_DefaultMatchesLegacy asserts CheckGovQuorum (legacy
// 2-arg form) and CheckGovQuorumOp with a default-2/3 op return identical
// results. Guards against op-aware refactors silently changing the legacy
// path's quorum math.
func TestCheckGovQuorumOp_DefaultMatchesLegacy(t *testing.T) {
	votes := map[string]string{
		"v1": "accept",
		"v2": "reject",
	}
	powers := map[string]int64{
		"v1": 10,
		"v2": 10,
		"v3": 10,
	}

	legacyPassed, legacyRejected, legacyA, legacyR, legacyT := CheckGovQuorum(votes, powers)
	opPassed, opRejected, opA, opR, opT := CheckGovQuorumOp(votes, powers, OpAddValidator)

	assert.Equal(t, legacyPassed, opPassed)
	assert.Equal(t, legacyRejected, opRejected)
	assert.Equal(t, legacyA, opA)
	assert.Equal(t, legacyR, opR)
	assert.Equal(t, legacyT, opT)
}
