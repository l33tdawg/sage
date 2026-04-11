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
