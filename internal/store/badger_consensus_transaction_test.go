package store

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConsensusOwnerCommitWaitsForOwnershipReaders(t *testing.T) {
	base, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.CloseBadger()) })
	require.NoError(t, base.RegisterDomain("research", "owner-a", "", 1))
	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.TransferDomain("research", "owner-b", "", 2))

	unlock := base.LockDomainOwnershipRead()
	committed := make(chan error, 1)
	go func() { committed <- scoped.CommitConsensusTransaction() }()
	select {
	case commitErr := <-committed:
		t.Fatalf("ownership-changing consensus commit bypassed an active reader: %v", commitErr)
	case <-time.After(100 * time.Millisecond):
	}
	unlock()
	require.NoError(t, <-committed)
	owner, err := base.GetDomainOwner("research")
	require.NoError(t, err)
	require.Equal(t, "owner-b", owner)
}

func TestSetSharedDomainWaitsForOwnershipReaders(t *testing.T) {
	base, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.CloseBadger()) })

	unlock := base.LockDomainOwnershipRead()
	shared := make(chan error, 1)
	go func() { shared <- base.SetSharedDomain("open.shared") }()
	select {
	case shareErr := <-shared:
		t.Fatalf("shared-domain publication bypassed an active owner reader: %v", shareErr)
	case <-time.After(100 * time.Millisecond):
	}
	unlock()
	require.NoError(t, <-shared)
	marker, err := base.GetState("shared_domain:open.shared")
	require.NoError(t, err)
	require.Equal(t, []byte{1}, marker)
}

func TestConsensusTransactionPreWriteSentinelDoesNotPoison(t *testing.T) {
	base, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.CloseBadger()) })

	scoped := base.BeginConsensusTransaction(nil)
	require.NoError(t, scoped.RegisterDomain("bounded.example", "owner", "", 1))
	require.ErrorIs(t, scoped.RegisterDomain("bounded.example", "attacker", "", 2), ErrDomainAlreadyRegistered)
	assert.NoError(t, scoped.ConsensusTransactionError(), "a validation sentinel before mutation is an ordinary invalid tx")
	require.NoError(t, scoped.SetState("after-sentinel", []byte("committed")))
	require.NoError(t, scoped.CommitConsensusTransaction())

	owner, err := base.GetDomainOwner("bounded.example")
	require.NoError(t, err)
	assert.Equal(t, "owner", owner)
	value, err := base.GetState("after-sentinel")
	require.NoError(t, err)
	assert.Equal(t, []byte("committed"), value)
}

func TestConsensusTransactionFailedFirstOrMidWriteDiscardsWholeBoundary(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprintf("write-%d", failAt), func(t *testing.T) {
			base, err := NewBadgerStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, base.CloseBadger()) })

			scoped := base.BeginConsensusTransaction(nil)
			scoped.writeFaultHook = func(attempt int) error {
				if attempt == failAt {
					return errors.New("injected staged-write failure")
				}
				return nil
			}
			err = scoped.SetStatesAtomic([]StateWrite{
				{Key: "a", Value: []byte("one")},
				{Key: "b", Value: []byte("two")},
				{Key: "c", Value: []byte("three")},
			})
			require.ErrorContains(t, err, "injected staged-write failure")
			require.Error(t, scoped.ConsensusTransactionError())
			require.Error(t, scoped.CommitConsensusTransaction(), "a poisoned transaction must never publish earlier staged writes")

			for _, key := range []string{"a", "b", "c"} {
				value, getErr := base.GetState(key)
				require.NoError(t, getErr)
				assert.Empty(t, value, "state %q survived a failed atomic boundary", key)
			}
		})
	}
}

func TestConsensusTransactionProofReplayDoesNotPruneExpiredMarkers(t *testing.T) {
	base, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, base.CloseBadger()) })

	now := time.Unix(50_000, 0).UTC()
	duplicate := sha256.Sum256([]byte("duplicate"))
	require.NoError(t, base.ClaimAgentProof(duplicate[:], now, now.Unix()+300))
	expired := make([][sha256.Size]byte, 12)
	for i := range expired {
		expired[i] = sha256.Sum256([]byte(fmt.Sprintf("expired-%d", i)))
		require.NoError(t, base.ClaimAgentProof(expired[i][:], now.Add(-time.Hour), now.Unix()-1))
	}

	scoped := base.BeginConsensusTransaction(nil)
	require.ErrorIs(t, scoped.ClaimAgentProof(duplicate[:], now, now.Unix()+300), ErrAgentProofReplayed)
	assert.NoError(t, scoped.ConsensusTransactionError(), "proof replay is rejected before opportunistic GC mutates")
	require.NoError(t, scoped.SetState("valid-after-replay", []byte("yes")))
	require.NoError(t, scoped.CommitConsensusTransaction())

	for i := range expired {
		exists, hasErr := base.HasAgentProof(expired[i][:], now.Add(-2*time.Second), now.Unix()-1)
		require.NoError(t, hasErr)
		assert.True(t, exists, "rejected replay pruned expired marker %d", i)
	}
}

func TestValidateAppV20ResourceBoundsRejectsOversizedLegacyFullRecord(t *testing.T) {
	bs, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bs.CloseBadger()) })

	const recordLimit = 64 << 10
	require.NoError(t, bs.SetRawForTest([]byte("federation:legacy"), make([]byte, recordLimit+1)))
	err = bs.ValidateAppV20ResourceBounds(512, recordLimit, recordLimit, 100)
	require.ErrorContains(t, err, "legacy consensus record")
}

func TestValidateAppV20ResourceBoundsRejectsStaleValidatorAmplification(t *testing.T) {
	for _, tc := range []struct {
		name string
		seed func(*testing.T, *BadgerStore, map[string]int64)
		want string
	}{
		{
			name: "persisted validator keys",
			seed: func(t *testing.T, bs *BadgerStore, validators map[string]int64) {
				require.NoError(t, bs.SaveValidators(validators))
			},
			want: "persisted validator set",
		},
		{
			name: "persisted PoE weight keys",
			seed: func(t *testing.T, bs *BadgerStore, validators map[string]int64) {
				weights := make(map[string]float64, len(validators))
				for id := range validators {
					weights[id] = 1
				}
				require.NoError(t, bs.SetEpochWeights(1, weights))
			},
			want: "persisted PoE weight set",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bs, err := NewBadgerStore(t.TempDir())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, bs.CloseBadger()) })
			validators := make(map[string]int64, 101)
			for i := 0; i < 101; i++ {
				validators[fmt.Sprintf("%064x", i)] = 1
			}
			tc.seed(t, bs, validators)
			err = bs.ValidateAppV20ResourceBounds(512, 64<<10, 64<<10, 100)
			require.ErrorContains(t, err, tc.want)
		})
	}
}
