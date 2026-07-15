package node

import (
	"errors"
	"testing"

	sm "github.com/cometbft/cometbft/state"
	"github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

type sageBootstrapStateStore struct {
	steps *[]string
	err   error
}

func (s sageBootstrapStateStore) Bootstrap(sm.State) error {
	*s.steps = append(*s.steps, "state")
	return s.err
}

type sageBootstrapCommitStore struct {
	steps     *[]string
	commitErr error
	markerErr error
}

func (s sageBootstrapCommitStore) SaveSeenCommit(int64, *types.Commit) error {
	*s.steps = append(*s.steps, "commit")
	return s.commitErr
}

func (s sageBootstrapCommitStore) SaveStateSyncBootstrapComplete(int64, []byte) error {
	*s.steps = append(*s.steps, "marker")
	return s.markerErr
}

type sageBlockSyncReactor struct {
	steps *[]string
	err   error
}

func (s sageBlockSyncReactor) SwitchToBlockSync(sm.State) error {
	*s.steps = append(*s.steps, "switch")
	return s.err
}

func TestPersistStateSyncBootstrapStoresCommitBeforePublishingState(t *testing.T) {
	steps := []string{}
	err := persistStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps},
		sm.State{LastBlockHeight: 42},
		&types.Commit{Height: 42},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"commit", "state"}, steps)
}

func TestPersistStateSyncBootstrapNeverPublishesStateAfterCommitFailure(t *testing.T) {
	steps := []string{}
	err := persistStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps, commitErr: errors.New("disk full")},
		sm.State{LastBlockHeight: 42},
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "store state-sync seen commit")
	require.Equal(t, []string{"commit"}, steps)
}

func TestCompleteStateSyncBootstrapMarksOnlyAfterBlockSyncHandoff(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps},
		sageBlockSyncReactor{steps: &steps},
		state,
		&types.Commit{Height: 42},
	)
	require.NoError(t, err)
	require.Equal(t, []string{"commit", "state", "switch", "marker"}, steps)
}

func TestCompleteStateSyncBootstrapNeverMarksAfterBlockSyncFailure(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps},
		sageBlockSyncReactor{steps: &steps, err: errors.New("reactor stopped")},
		state,
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "switch to block sync")
	require.Equal(t, []string{"commit", "state", "switch"}, steps)
}

func TestCompleteStateSyncBootstrapNeverSwitchesAfterStateFailure(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps, err: errors.New("disk full")},
		sageBootstrapCommitStore{steps: &steps},
		sageBlockSyncReactor{steps: &steps},
		state,
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "publish state-sync state")
	require.Equal(t, []string{"commit", "state"}, steps)
}

func TestCompleteStateSyncBootstrapReportsMarkerFailureLast(t *testing.T) {
	steps := []string{}
	state := sm.State{LastBlockHeight: 42, AppHash: make([]byte, 32)}
	err := completeStateSyncBootstrap(
		sageBootstrapStateStore{steps: &steps},
		sageBootstrapCommitStore{steps: &steps, markerErr: errors.New("disk full")},
		sageBlockSyncReactor{steps: &steps},
		state,
		&types.Commit{Height: 42},
	)
	require.ErrorContains(t, err, "persist state-sync block-sync handoff")
	require.Equal(t, []string{"commit", "state", "switch", "marker"}, steps)
}
