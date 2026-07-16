package abci

import (
	"context"
	"crypto/ed25519"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

type appV20ValidatorGovernanceFixture struct {
	app       *SageApp
	admin     agentKey
	validator agentKey
	third     agentKey
	candidate agentKey
}

func setupAppV20ValidatorGovernanceFixture(t *testing.T) appV20ValidatorGovernanceFixture {
	t.Helper()
	app, admin := setupGovTestApp(t)
	app.appV20AppliedHeight = 1
	second := newAgentKey(t)
	third := newAgentKey(t)
	candidate := newAgentKey(t)
	for _, actor := range []agentKey{second, third} {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: actor.id, PublicKey: actor.pub, Power: 10,
		}))
	}
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{
		admin.id: 10, second.id: 10, third.id: 10,
	}))
	return appV20ValidatorGovernanceFixture{
		app: app, admin: admin, validator: second, third: third, candidate: candidate,
	}
}

func directGovernanceProposal(t *testing.T, signer agentKey, proposal tx.GovPropose) *tx.ParsedTx {
	t.Helper()
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: time.Unix(1_000, 0).UTC(),
		GovPropose: &proposal,
	}
	require.NoError(t, tx.SignTx(parsed, signer.priv))
	return parsed
}

func TestAppV20ValidatorOperationIdentityAndPowerInvariants(t *testing.T) {
	tests := []struct {
		name    string
		build   func(appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64)
		wantErr string
	}{
		{
			name: "valid add",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, f.candidate.pub, 10
			},
		},
		{
			name: "uppercase ID",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, strings.ToUpper(f.candidate.id), f.candidate.pub, 10
			},
			wantErr: "canonical lowercase",
		},
		{
			name: "short ID",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id[:62], f.candidate.pub, 10
			},
			wantErr: "32-byte lowercase-hex",
		},
		{
			name: "add missing pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, nil, 10
			},
			wantErr: "requires target_pubkey",
		},
		{
			name: "add short pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, []byte{1}, 10
			},
			wantErr: "length 1",
		},
		{
			name: "add mismatched pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, f.third.pub, 10
			},
			wantErr: "does not match",
		},
		{
			name: "add existing validator",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.validator.id, f.validator.pub, 10
			},
			wantErr: "already exists",
		},
		{
			name: "add zero power",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, f.candidate.pub, 0
			},
			wantErr: "must be positive",
		},
		{
			name: "add negative power",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, f.candidate.pub, -1
			},
			wantErr: "must be positive",
		},
		{
			name: "add power over one third",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpAddValidator, f.candidate.id, f.candidate.pub, 11
			},
			wantErr: "exceeds 1/3",
		},
		{
			name: "valid update without redundant pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpUpdatePower, f.validator.id, nil, 20
			},
		},
		{
			name: "valid update with matching pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpUpdatePower, f.validator.id, f.validator.pub, 20
			},
		},
		{
			name: "valid update after legacy restore omitted manager pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				existing, _ := f.app.validators.GetValidator(f.validator.id)
				existing.PublicKey = nil // SaveValidators historically persists ID + power only.
				return governance.OpUpdatePower, f.validator.id, nil, 20
			},
		},
		{
			name: "update existing manager key mismatch",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				existing, _ := f.app.validators.GetValidator(f.validator.id)
				existing.PublicKey = f.third.pub
				return governance.OpUpdatePower, f.validator.id, nil, 11
			},
			wantErr: "existing validator public key does not match",
		},
		{
			name: "update mismatched pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpUpdatePower, f.validator.id, f.third.pub, 11
			},
			wantErr: "does not match",
		},
		{
			name: "update missing validator",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpUpdatePower, f.candidate.id, nil, 10
			},
			wantErr: "does not exist",
		},
		{
			name: "update zero power",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpUpdatePower, f.validator.id, nil, 0
			},
			wantErr: "must be positive",
		},
		{
			name: "update power change over one third",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpUpdatePower, f.validator.id, nil, 21
			},
			wantErr: "exceeds max allowed",
		},
		{
			name: "valid remove without redundant pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpRemoveValidator, f.third.id, nil, 0
			},
		},
		{
			name: "valid remove with matching pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpRemoveValidator, f.third.id, f.third.pub, 0
			},
		},
		{
			name: "remove existing manager key mismatch",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				existing, _ := f.app.validators.GetValidator(f.third.id)
				existing.PublicKey = f.validator.pub
				return governance.OpRemoveValidator, f.third.id, nil, 0
			},
			wantErr: "existing validator public key does not match",
		},
		{
			name: "remove mismatched pubkey",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpRemoveValidator, f.third.id, f.validator.pub, 0
			},
			wantErr: "does not match",
		},
		{
			name: "remove missing validator",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpRemoveValidator, f.candidate.id, nil, 0
			},
			wantErr: "does not exist",
		},
		{
			name: "remove nonzero power",
			build: func(f appV20ValidatorGovernanceFixture) (governance.ProposalOp, string, []byte, int64) {
				return governance.OpRemoveValidator, f.third.id, nil, 1
			},
			wantErr: "must be 0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := setupAppV20ValidatorGovernanceFixture(t)
			op, targetID, targetPubKey, targetPower := tt.build(fixture)
			err := fixture.app.validateAppV20ValidatorOperation(op, targetID, targetPubKey, targetPower)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
		})
	}
}

func TestAppV20GovernanceAdmissionRejectsValidatorKeySplit(t *testing.T) {
	fixture := setupAppV20ValidatorGovernanceFixture(t)
	parsed := directGovernanceProposal(t, fixture.admin, tx.GovPropose{
		Operation: tx.GovOpUpdatePower, TargetID: fixture.validator.id,
		TargetPubKey: fixture.third.pub, TargetPower: 11, Reason: "split app and Comet identity",
	})

	result := fixture.app.processTx(parsed, 2, parsed.Timestamp)
	assert.Equal(t, uint32(72), result.Code)
	assert.Contains(t, result.Log, "target_pubkey does not match")
	active, err := fixture.app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestAppV20ValidatorRemovalReplacesPersistedRosterAndCannotResurrect(t *testing.T) {
	fixture := setupAppV20ValidatorGovernanceFixture(t)
	proposal := &governance.ProposalState{
		ProposalID:   "remove-third-persistence-proof",
		Operation:    governance.OpRemoveValidator,
		TargetID:     fixture.third.id,
		TargetPubKey: nil,
		TargetPower:  0,
	}

	update, err := fixture.app.applyGovernanceProposal(proposal, 2)
	require.NoError(t, err)
	require.NotNil(t, update)
	assert.Zero(t, update.Power)

	persisted, err := fixture.app.badgerStore.LoadValidators()
	require.NoError(t, err)
	assert.NotContains(t, persisted, fixture.third.id)
	assert.Equal(t, map[string]int64{
		fixture.admin.id:     10,
		fixture.validator.id: 10,
	}, persisted)

	// Reconstruct the application graph from the same durable Badger handle.
	// NewSageAppWithStores reloads validator:* rather than sharing the previous
	// in-memory manager, so this is the boot path that caught stale upserts.
	restarted, err := NewSageAppWithStores(
		fixture.app.badgerStore,
		fixture.app.offchainStore,
		fixture.app.logger,
	)
	require.NoError(t, err)
	restartedValidators := restarted.validators.GetAll()
	require.Len(t, restartedValidators, 2)
	for _, validator := range restartedValidators {
		assert.NotEqual(t, fixture.third.id, validator.ID)
	}
}

func TestAppV20GovernanceAdmissionRejectsUnknownOperation(t *testing.T) {
	fixture := setupAppV20ValidatorGovernanceFixture(t)
	parsed := directGovernanceProposal(t, fixture.admin, tx.GovPropose{
		Operation: tx.GovProposalOp(255), TargetID: fixture.candidate.id,
		Reason: "unknown operation",
	})
	result := fixture.app.processTx(parsed, 2, parsed.Timestamp)
	assert.Equal(t, uint32(72), result.Code)
	assert.Contains(t, result.Log, "unknown operation 255")
	active, err := fixture.app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestPreAppV20GovernanceAdmissionRetainsHistoricalValidatorValidation(t *testing.T) {
	app, admin := setupGovTestApp(t)
	candidate := newAgentKey(t)
	parsed := directGovernanceProposal(t, admin, tx.GovPropose{
		Operation: tx.GovOpAddValidator, TargetID: candidate.id,
		TargetPubKey: []byte{1}, TargetPower: 3, Reason: "historical malformed key",
	})

	result := app.processTx(parsed, 1, parsed.Timestamp)
	require.Zero(t, result.Code, result.Log)
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, []byte{1}, active.TargetPubKey)
}

func TestAppV20ExecutionPreflightDoesNotCommitFalseSuccess(t *testing.T) {
	fixture := setupAppV20ValidatorGovernanceFixture(t)
	proposalID, err := fixture.app.govEngine.Propose(
		fixture.admin.id,
		governance.OpUpdatePower,
		fixture.validator.id,
		fixture.third.pub, // Deliberately points CometBFT at a different validator.
		11,
		0,
		"malformed approved update",
		2,
		nil,
	)
	require.NoError(t, err)
	require.NoError(t, fixture.app.govEngine.Vote(
		proposalID, fixture.validator.id, "accept", 12,
	))

	resp, err := fixture.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 12, Time: time.Unix(1_012, 0).UTC(),
	})
	require.ErrorContains(t, err, "atomic governance post-processing failed")
	require.ErrorContains(t, err, "target_pubkey does not match target validator ID")
	assert.Nil(t, resp)
	assert.Nil(t, fixture.app.pendingAppV20Finalize)

	proposal, err := fixture.app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	assert.Equal(t, governance.StatusVoting, proposal.Status)
	active, err := fixture.app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, proposalID, active.ProposalID)

	second, exists := fixture.app.validators.GetValidator(fixture.validator.id)
	require.True(t, exists)
	assert.Equal(t, int64(10), second.Power)
	third, exists := fixture.app.validators.GetValidator(fixture.third.id)
	require.True(t, exists)
	assert.Equal(t, int64(10), third.Power)
}

func TestAppV20ApplyRejectsValidatorKeySplitBeforeMutation(t *testing.T) {
	fixture := setupAppV20ValidatorGovernanceFixture(t)
	update, err := fixture.app.applyGovernanceProposal(&governance.ProposalState{
		ProposalID:   "malformed",
		Operation:    governance.OpUpdatePower,
		TargetID:     fixture.validator.id,
		TargetPubKey: append(ed25519.PublicKey(nil), fixture.third.pub...),
		TargetPower:  11,
	}, 2)
	require.Error(t, err)
	assert.Nil(t, update)
	assert.Contains(t, err.Error(), "target_pubkey does not match")
	second, exists := fixture.app.validators.GetValidator(fixture.validator.id)
	require.True(t, exists)
	assert.Equal(t, int64(10), second.Power)
}
