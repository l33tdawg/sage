package store

import (
	"context"
	"errors"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFederatedAgentExposureDefaultsToAllUntilConfigured(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := federatedReaderTestBinding(t, "chain-exposure", "epoch-exposure", "cd")
	activateFederatedReaderTestBinding(t, s, binding)

	exposure, err := s.GetFederatedAgentExposure(ctx, binding)
	require.NoError(t, err)
	require.NotNil(t, exposure)
	require.False(t, exposure.Configured, "an unconfigured connection must report the default, not a stored row")
	require.Equal(t, FederatedAgentExposureModeAll, exposure.Mode)
	require.Empty(t, exposure.AgentIDs)
	require.Zero(t, exposure.Revision)
}

func TestFederatedAgentExposureSelectedRoundTripAndModeClearing(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := federatedReaderTestBinding(t, "chain-exposure", "epoch-exposure", "ef")
	activateFederatedReaderTestBinding(t, s, binding)
	first, second := testPeerAgentID(t), testPeerAgentID(t)

	selected, err := s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeSelected, []string{second, first, second}, 0)
	require.NoError(t, err)
	require.True(t, selected.Configured)
	require.Equal(t, int64(1), selected.Revision)
	// Compare against a sorted copy of the inputs: the generated agent ids are
	// random, so a fixed [first, second] literal only passes when the two
	// happen to sort that way.
	expected := append([]string(nil), first, second)
	sort.Strings(expected)
	require.Equal(t, expected, selected.AgentIDs,
		"the allow list is stored as a sorted, de-duplicated snapshot")

	reloaded, err := s.GetFederatedAgentExposure(ctx, binding)
	require.NoError(t, err)
	require.Equal(t, selected.AgentIDs, reloaded.AgentIDs)

	// Switching to a mode that carries no list must clear the previous allow
	// list rather than leaving it to be re-applied by a later read.
	deniedAll, err := s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeNone, nil, selected.Revision)
	require.NoError(t, err)
	require.Equal(t, FederatedAgentExposureModeNone, deniedAll.Mode)
	require.Empty(t, deniedAll.AgentIDs)
	require.Equal(t, int64(2), deniedAll.Revision)

	again, err := s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeAll, nil, deniedAll.Revision)
	require.NoError(t, err)
	require.Equal(t, FederatedAgentExposureModeAll, again.Mode)
	require.True(t, again.Configured)
	require.Empty(t, again.AgentIDs)
}

func TestFederatedAgentExposureRejectsInvalidSnapshots(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := federatedReaderTestBinding(t, "chain-exposure", "epoch-exposure", "01")
	activateFederatedReaderTestBinding(t, s, binding)
	agentID := testPeerAgentID(t)

	for name, mode := range map[string]string{"unknown mode": "sometimes"} {
		_, err := s.SetBoundFederatedAgentExposure(ctx, binding, mode, nil, 0)
		require.Error(t, err, name)
	}
	_, err := s.SetBoundFederatedAgentExposure(ctx, binding, FederatedAgentExposureModeSelected, nil, 0)
	require.Error(t, err, "selected mode with no agents has no meaning; none is the deny-all mode")
	_, err = s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeAll, []string{agentID}, 0)
	require.Error(t, err, "all mode must not carry an allow list")
	_, err = s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeSelected, []string{"not-an-agent-id"}, 0)
	require.Error(t, err, "a non-canonical agent id must be rejected")
}

func TestFederatedAgentExposureCASRejectsStaleWriter(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := federatedReaderTestBinding(t, "chain-exposure", "epoch-exposure", "23")
	activateFederatedReaderTestBinding(t, s, binding)
	agentID := testPeerAgentID(t)

	saved, err := s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeSelected, []string{agentID}, 0)
	require.NoError(t, err)

	// A writer that still holds the pre-edit revision must lose, and must not
	// merge its roster into the committed snapshot.
	_, err = s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeSelected, []string{testPeerAgentID(t)}, 0)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFederatedAgentExposureRevisionConflict), "got %v", err)

	after, err := s.GetFederatedAgentExposure(ctx, binding)
	require.NoError(t, err)
	require.Equal(t, saved.AgentIDs, after.AgentIDs)
	require.Equal(t, saved.Revision, after.Revision)
}

func TestFederatedAgentExposureBindingMismatchFailsClosedThenResets(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := federatedReaderTestBinding(t, "chain-exposure", "epoch-exposure", "45")
	activateFederatedReaderTestBinding(t, s, binding)
	agentID := testPeerAgentID(t)
	_, err := s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeNone, nil, 0)
	require.NoError(t, err)

	// A re-pair replaces the epoch on the existing sync control (the same
	// sequence the federation Manager performs), so the stored row now
	// describes a retired generation. Reading it against the live binding must
	// fail closed instead of re-applying an old allow list to the new link.
	retired := binding
	retired.PolicyEpoch = "epoch-exposure-replaced"
	_, err = s.writeExecContext(ctx, `
		UPDATE sync_control SET policy_epoch=? WHERE remote_chain_id=?`,
		retired.PolicyEpoch, retired.RemoteChainID)
	require.NoError(t, err)
	_, err = s.GetFederatedAgentExposure(ctx, retired)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFederatedAgentExposureBindingMismatch), "got %v", err)

	removed, err := s.ResetFederatedAgentExposureForBinding(ctx, retired)
	require.NoError(t, err)
	require.Equal(t, int64(1), removed)
	reset, err := s.GetFederatedAgentExposure(ctx, retired)
	require.NoError(t, err)
	require.False(t, reset.Configured)
	require.Equal(t, FederatedAgentExposureModeAll, reset.Mode)

	// The reset only retired the stale generation: a fresh policy still works.
	_, err = s.SetBoundFederatedAgentExposure(
		ctx, retired, FederatedAgentExposureModeSelected, []string{agentID}, 0)
	require.NoError(t, err)
}

func TestFederatedAgentExposureRequiresActiveSyncControl(t *testing.T) {
	ctx := context.Background()
	s := newSyncTestStore(t)
	binding := federatedReaderTestBinding(t, "chain-exposure", "epoch-exposure", "67")

	// No sync_control row at all: a caller must not be able to install or read
	// a policy for a connection generation that does not exist.
	_, err := s.GetFederatedAgentExposure(ctx, binding)
	require.Error(t, err)
	_, err = s.SetBoundFederatedAgentExposure(
		ctx, binding, FederatedAgentExposureModeNone, nil, 0)
	require.True(t, errors.Is(err, ErrFederatedAgentExposureBindingMismatch), "got %v", err)
}
