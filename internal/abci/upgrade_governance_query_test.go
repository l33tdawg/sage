package abci

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
)

func queryUpgradeGovernanceStatus(t *testing.T, app *SageApp) (*abcitypes.ResponseQuery, *UpgradeGovernanceStatus) {
	t.Helper()
	response, err := app.Query(context.Background(), &abcitypes.RequestQuery{Path: "/upgrade/governance-status"})
	require.NoError(t, err)
	if response.Code != 0 {
		return response, nil
	}
	var status UpgradeGovernanceStatus
	require.NoError(t, json.Unmarshal(response.Value, &status))
	return response, &status
}

func TestUpgradeGovernanceStatusReportsAuthoritativePlanAndBallot(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: "app-v27", TargetAppVersion: 27, ActivationHeight: 900,
		BinarySHA256: strings.Repeat("a", 64), GovernanceDomain: strings.Repeat("b", 64),
		ProposedAt: 123, ProposerID: strings.Repeat("c", 64),
	}))

	payload, err := json.Marshal(UpgradeProposalPayload{
		Name: "app-v28", TargetAppVersion: 28, BinarySHA256: strings.Repeat("d", 64),
	})
	require.NoError(t, err)
	proposal := &governance.ProposalState{
		ProposalID: "upgrade-proposal-28", Operation: governance.OpUpgrade,
		TargetID: "app-v28", Status: governance.StatusVoting,
		CreatedHeight: 700, ExpiryHeight: 800, Payload: payload,
	}
	encoded, err := json.Marshal(proposal)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetGovProposal(proposal.ProposalID, encoded))
	require.NoError(t, app.badgerStore.SetActiveProposal(proposal.ProposalID))

	response, status := queryUpgradeGovernanceStatus(t, app)
	require.Zero(t, response.Code, response.Log)
	require.Equal(t, upgradeGovernanceStatusSchema, status.Schema)
	require.Equal(t, uint64(1), status.CurrentAppVersion)
	require.NotNil(t, status.PendingPlan)
	require.Equal(t, uint64(27), status.PendingPlan.TargetAppVersion)
	require.Equal(t, int64(900), status.PendingPlan.ActivationHeight)
	require.NotNil(t, status.ActiveProposal)
	require.Equal(t, "upgrade", status.ActiveProposal.Operation)
	require.Equal(t, uint8(governance.OpUpgrade), status.ActiveProposal.OperationCode)
	require.Equal(t, "app-v28", status.ActiveProposal.TargetID)
	require.NotNil(t, status.ActiveProposal.TargetAppVersion)
	require.Equal(t, uint64(28), *status.ActiveProposal.TargetAppVersion)
}

func TestUpgradeGovernanceStatusReportsExplicitAbsence(t *testing.T) {
	response, status := queryUpgradeGovernanceStatus(t, setupTestApp(t))
	require.Zero(t, response.Code, response.Log)
	require.Nil(t, status.PendingPlan)
	require.Nil(t, status.ActiveProposal)
}

func TestUpgradeGovernanceStatusValidatesAutomaticBinaryReplacement(t *testing.T) {
	target27 := uint64(27)
	tests := []struct {
		name       string
		status     *UpgradeGovernanceStatus
		maxSupport uint64
		wantError  string
	}{
		{
			name:       "missing status fails closed",
			status:     nil,
			maxSupport: 27,
			wantError:  "status is unavailable",
		},
		{
			name:       "zero binary support fails closed",
			status:     &UpgradeGovernanceStatus{CurrentAppVersion: 27},
			maxSupport: 0,
			wantError:  "zero max supported app version",
		},
		{
			name:       "zero current app version fails closed",
			status:     &UpgradeGovernanceStatus{},
			maxSupport: 27,
			wantError:  "current app version 0",
		},
		{
			name:       "clear current state",
			status:     &UpgradeGovernanceStatus{CurrentAppVersion: 27},
			maxSupport: 27,
		},
		{
			name: "supported pending plan continues automatically",
			status: &UpgradeGovernanceStatus{
				CurrentAppVersion: 26,
				PendingPlan: &UpgradeGovernancePendingPlan{
					Name: "app-v27", TargetAppVersion: 27, ActivationHeight: 900,
				},
			},
			maxSupport: 27,
		},
		{
			name: "supported active upgrade ballot continues automatically",
			status: &UpgradeGovernanceStatus{
				CurrentAppVersion: 26,
				ActiveProposal: &UpgradeGovernanceActiveProposal{
					ProposalID: "proposal-27", Operation: "upgrade",
					OperationCode:    uint8(governance.OpUpgrade),
					TargetAppVersion: &target27,
				},
			},
			maxSupport: 27,
		},
		{
			name: "ordinary ballot is compatible",
			status: &UpgradeGovernanceStatus{
				CurrentAppVersion: 27,
				ActiveProposal: &UpgradeGovernanceActiveProposal{
					ProposalID: "validator-change",
					Operation:  "add_validator", OperationCode: uint8(governance.OpAddValidator),
				},
			},
			maxSupport: 27,
		},
		{
			name:       "current chain exceeds support",
			status:     &UpgradeGovernanceStatus{CurrentAppVersion: 29},
			maxSupport: 28,
			wantError:  "current app version 29",
		},
		{
			name: "pending target exceeds support",
			status: &UpgradeGovernanceStatus{
				CurrentAppVersion: 28,
				PendingPlan: &UpgradeGovernancePendingPlan{
					Name: "app-v29", TargetAppVersion: 29, ActivationHeight: 900,
				},
			},
			maxSupport: 28,
			wantError:  "targets app-v29",
		},
		{
			name: "active upgrade target must be decoded",
			status: &UpgradeGovernanceStatus{
				CurrentAppVersion: 27,
				ActiveProposal: &UpgradeGovernanceActiveProposal{
					ProposalID: "proposal-missing-target", Operation: "upgrade",
					OperationCode: uint8(governance.OpUpgrade),
				},
			},
			maxSupport: 27,
			wantError:  "has no decoded target app version",
		},
		{
			name: "active upgrade target exceeds support",
			status: &UpgradeGovernanceStatus{
				CurrentAppVersion: 28,
				ActiveProposal: &UpgradeGovernanceActiveProposal{
					ProposalID: "proposal-29", Operation: "upgrade",
					OperationCode:    uint8(governance.OpUpgrade),
					TargetAppVersion: func() *uint64 { value := uint64(29); return &value }(),
				},
			},
			maxSupport: 28,
			wantError:  "targets app-v29",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.status.ValidateBinaryReplacement(tt.maxSupport)
			if tt.wantError == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantError)
		})
	}
}

func TestAcquireVerifiedUpgradeSnapshotFenceKeepsProofAndTupleAtomic(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 42
	app.state.AppHash = []byte("committed-app-hash")

	status, height, appHash, release, err := app.AcquireVerifiedUpgradeSnapshotFence(MaxSupportedAppVersion())
	require.NoError(t, err)
	require.NotNil(t, status)
	require.Equal(t, int64(42), height)
	require.Equal(t, []byte("committed-app-hash"), appHash)
	require.NotNil(t, release)
	require.False(t, app.runtimeViewMu.TryLock(), "Commit must remain excluded after governance validation")

	release()
	require.True(t, app.runtimeViewMu.TryLock(), "release must return the runtime write lock")
	app.runtimeViewMu.Unlock()
}

func TestAcquireVerifiedUpgradeSnapshotFenceReleasesOnIncompatibleGovernance(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 42
	app.state.AppHash = []byte("committed-app-hash")
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: "app-v29", TargetAppVersion: 29, ActivationHeight: 900,
	}))

	status, height, appHash, release, err := app.AcquireVerifiedUpgradeSnapshotFence(28)
	require.ErrorContains(t, err, "targets app-v29")
	require.Nil(t, status)
	require.Zero(t, height)
	require.Nil(t, appHash)
	require.Nil(t, release)
	require.True(t, app.runtimeViewMu.TryLock(), "failed validation must not leak the runtime read lock")
	app.runtimeViewMu.Unlock()
}

func TestUpgradeGovernanceStatusReportsNonUpgradeBallotWithoutDecodingPayload(t *testing.T) {
	app := setupTestApp(t)
	proposal := &governance.ProposalState{
		ProposalID: "validator-proposal", Operation: governance.OpAddValidator,
		TargetID: "validator-b", Status: governance.StatusVoting,
		CreatedHeight: 10, ExpiryHeight: 20, Payload: []byte("not-upgrade-json"),
	}
	encoded, err := json.Marshal(proposal)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetGovProposal(proposal.ProposalID, encoded))
	require.NoError(t, app.badgerStore.SetActiveProposal(proposal.ProposalID))

	response, status := queryUpgradeGovernanceStatus(t, app)
	require.Zero(t, response.Code, response.Log)
	require.Equal(t, "add_validator", status.ActiveProposal.Operation)
	require.Nil(t, status.ActiveProposal.TargetAppVersion)
}

func TestUpgradeGovernanceStatusFailsClosedOnMalformedState(t *testing.T) {
	t.Run("pending plan decode", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetRawForTest([]byte("upgrade:plan"), []byte("not-json")))
		response, status := queryUpgradeGovernanceStatus(t, app)
		require.Nil(t, status)
		require.Equal(t, uint32(1), response.Code)
		require.Contains(t, response.Log, "read pending upgrade plan")
	})

	t.Run("non-canonical pending plan", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
			Name: "release-27", TargetAppVersion: 27, ActivationHeight: 900,
		}))
		response, status := queryUpgradeGovernanceStatus(t, app)
		require.Nil(t, status)
		require.Equal(t, uint32(1), response.Code)
		require.Contains(t, response.Log, "non-canonical name")
	})

	t.Run("missing active proposal", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetActiveProposal("missing"))
		response, status := queryUpgradeGovernanceStatus(t, app)
		require.Nil(t, status)
		require.Equal(t, uint32(1), response.Code)
		require.Contains(t, response.Log, "read active governance proposal")
	})

	t.Run("active proposal id mismatch", func(t *testing.T) {
		app := setupTestApp(t)
		proposal := &governance.ProposalState{
			ProposalID: "different", Operation: governance.OpAddValidator,
			TargetID: "validator-b", Status: governance.StatusVoting,
			CreatedHeight: 10, ExpiryHeight: 20,
		}
		encoded, err := json.Marshal(proposal)
		require.NoError(t, err)
		require.NoError(t, app.badgerStore.SetGovProposal("active", encoded))
		require.NoError(t, app.badgerStore.SetActiveProposal("active"))
		response, status := queryUpgradeGovernanceStatus(t, app)
		require.Nil(t, status)
		require.Equal(t, uint32(1), response.Code)
		require.Contains(t, response.Log, "does not match pointer")
	})

	t.Run("oversized active pointer", func(t *testing.T) {
		app := setupTestApp(t)
		require.NoError(t, app.badgerStore.SetActiveProposal(strings.Repeat("x", maxAppV20IdentifierBytes+1)))
		response, status := queryUpgradeGovernanceStatus(t, app)
		require.Nil(t, status)
		require.Equal(t, uint32(1), response.Code)
		require.Contains(t, response.Log, "pointer is not bounded")
	})

	t.Run("malformed upgrade payload", func(t *testing.T) {
		app := setupTestApp(t)
		proposal := &governance.ProposalState{
			ProposalID: "bad-upgrade", Operation: governance.OpUpgrade,
			TargetID: "app-v27", Status: governance.StatusVoting,
			CreatedHeight: 10, ExpiryHeight: 20, Payload: []byte("not-json"),
		}
		encoded, err := json.Marshal(proposal)
		require.NoError(t, err)
		require.NoError(t, app.badgerStore.SetGovProposal(proposal.ProposalID, encoded))
		require.NoError(t, app.badgerStore.SetActiveProposal(proposal.ProposalID))
		response, status := queryUpgradeGovernanceStatus(t, app)
		require.Nil(t, status)
		require.Equal(t, uint32(1), response.Code)
		require.Contains(t, response.Log, "decode active upgrade proposal")
	})
}
