package governance

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestThresholdForKeepsHistoricalOp12AtTwoThirds(t *testing.T) {
	num, den := ThresholdFor(OpMemoryTaint)
	assert.Equal(t, int64(2), num)
	assert.Equal(t, int64(3), den)
}

func TestThresholdForAtUsesProposalCreationHeight(t *testing.T) {
	const activationHeight int64 = 100
	tests := []struct {
		name          string
		createdHeight int64
		wantNum       int64
		wantDen       int64
	}{
		{"before activation", 99, 2, 3},
		{"activation block", 100, 2, 3},
		{"first post-activation block", 101, 3, 4},
		{"created after activation but executes later", 105, 3, 4},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			num, den := ThresholdForAt(OpMemoryTaint, tc.createdHeight, activationHeight)
			assert.Equal(t, tc.wantNum, num)
			assert.Equal(t, tc.wantDen, den)
		})
	}

	num, den := ThresholdForAt(OpMemoryTaint, 1000, 0)
	assert.Equal(t, int64(2), num, "dormant app-v30 keeps historical op-12 behavior")
	assert.Equal(t, int64(3), den)
}

func TestCheckGovQuorumAtSelectsThresholdFromCreationHeight(t *testing.T) {
	powers := map[string]int64{
		"validator-a": 10,
		"validator-b": 10,
		"validator-c": 10,
	}
	twoVotes := map[string]string{
		"validator-a": "accept",
		"validator-b": "accept",
	}
	passed, _, _, _, _ := CheckGovQuorumAt(twoVotes, powers, OpMemoryTaint, 100, 100)
	assert.True(t, passed, "the activation block retains the historical 2/3 threshold")

	passed, _, _, _, _ = CheckGovQuorumAt(twoVotes, powers, OpMemoryTaint, 101, 100)
	assert.False(t, passed, "two of three do not reach 3/4")

	threeVotes := map[string]string{
		"validator-a": "accept",
		"validator-b": "accept",
		"validator-c": "accept",
	}
	passed, _, _, _, _ = CheckGovQuorumAt(threeVotes, powers, OpMemoryTaint, 101, 100)
	assert.True(t, passed, "three of three reach 3/4")
}

func TestCheckGovQuorumAtPreservesOtherOperationThresholds(t *testing.T) {
	powers := map[string]int64{"a": 10, "b": 10, "c": 10, "d": 10}
	votes := map[string]string{"a": "accept", "b": "accept", "c": "accept"}
	passed, _, _, _, _ := CheckGovQuorumAt(votes, powers, OpAddValidator, 101, 100)
	assert.True(t, passed, "other operations keep the default 2/3 threshold")
}
