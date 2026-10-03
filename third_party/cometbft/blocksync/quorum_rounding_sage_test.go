package blocksync

import (
	"testing"

	"github.com/cometbft/cometbft/crypto/ed25519"
	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
)

func TestBlockSyncQuorumBlockingPowerRoundsUp(t *testing.T) {
	for _, tc := range []struct {
		name   string
		powers []int64
		want   bool
	}{
		{"four unit validators", []int64{1, 1, 1, 1}, false},
		{"five unit validators", []int64{1, 1, 1, 1, 1}, false},
		{"weighted below one third", []int64{2, 1, 1, 1, 1, 1}, false},
		{"exactly one third", []int64{1, 1, 1}, true},
		{"above one third", []int64{2, 1, 1}, true},
		{"single validator", []int64{1}, true},
		{"two validators", []int64{1, 1}, true},
		{"maximum below threshold", []int64{(types.MaxTotalVotingPower - 1) / 3, types.MaxTotalVotingPower - 1 - (types.MaxTotalVotingPower-1)/3}, false},
		{"maximum at threshold", []int64{types.MaxTotalVotingPower / 3, types.MaxTotalVotingPower - types.MaxTotalVotingPower/3}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			validators := make([]*types.Validator, 0, len(tc.powers))
			for _, power := range tc.powers {
				validators = append(validators, types.NewValidator(ed25519.GenPrivKey().PubKey(), power))
			}
			reactor := &Reactor{localAddr: validators[0].Address}
			state := sm.State{Validators: types.NewValidatorSet(validators)}
			if got := reactor.localNodeBlocksTheChain(state); got != tc.want {
				t.Fatalf("local=%d total=%d: quorum-blocking=%t, want %t", tc.powers[0], state.Validators.TotalVotingPower(), got, tc.want)
			}
			reactor.localAddr = ed25519.GenPrivKey().PubKey().Address()
			if reactor.localNodeBlocksTheChain(state) {
				t.Fatal("non-validator was treated as quorum-blocking")
			}
		})
	}
}
