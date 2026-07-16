package abci

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/validator"
)

func appV20ValidatorVote(key agentKey, power int64) abcitypes.VoteInfo {
	return abcitypes.VoteInfo{Validator: abcitypes.Validator{
		Address: cmted25519.PubKey(key.pub).Address(),
		Power:   power,
	}}
}

func TestAppV20PersistCloseReopenHydratesValidatorKeysAndActivates(t *testing.T) {
	root := t.TempDir()
	badgerPath := filepath.Join(root, "badger")
	projectionPath := filepath.Join(root, "projection.db")
	bs, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	require.NoError(t, err)
	app, err := NewSageAppWithStores(bs, projection, zerolog.Nop())
	require.NoError(t, err)

	key := newAgentKey(t)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: key.id, PublicKey: key.pub, Power: 10,
	}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{key.id: 10}))
	require.NoError(t, app.badgerStore.SetState(appV20LegacyResourceAuditStateKey, appV20LegacyResourceAuditValue))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV19UpgradeName, 19, 1))
	require.NoError(t, app.Close())

	reopenedBadger, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	reopenedProjection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	require.NoError(t, err)
	reopened, err := NewSageAppWithStores(reopenedBadger, reopenedProjection, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	require.Equal(t, uint64(19), reopened.currentAppVersion())
	current := reopened.validators.GetAll()
	require.Len(t, current, 1)
	assert.Equal(t, key.id, current[0].ID)
	assert.Equal(t, key.pub, current[0].PublicKey, "legacy power-only persistence must hydrate the key canonically from validator ID")
	require.NoError(t, reopened.appV20ConsensusReadiness())

	domain, err := governance.DelegationDomainForChainID("sage-validator-reopen")
	require.NoError(t, err)
	require.NoError(t, reopened.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV20UpgradeName, TargetAppVersion: 20, ActivationHeight: 2,
		GovernanceDomain: domain, ProposedAt: 1, ProposerID: key.id,
	}))
	wrongCometKey := newAgentKey(t)
	assert.Panics(t, func() {
		_, _ = reopened.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
			Height: 2, Time: time.Unix(17_002, 0).UTC(),
			DecidedLastCommit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
				appV20ValidatorVote(wrongCometKey, 10),
			}},
		})
	}, "a persisted app validator A must not activate against Comet validator B")
	assert.Equal(t, uint64(19), reopened.currentAppVersion())
	stillPending, err := reopened.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, stillPending, "rejected speculative activation must preserve the durable plan")

	finalized, err := reopened.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(17_002, 0).UTC(),
		DecidedLastCommit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
			appV20ValidatorVote(key, 10),
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, finalized.ConsensusParamUpdates)
	require.NotNil(t, finalized.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(20), finalized.ConsensusParamUpdates.Version.App)
	_, err = reopened.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, uint64(20), reopened.currentAppVersion())
}

func TestAppV20ActivationValidatorCommitRequiresExactSetAndPower(t *testing.T) {
	first := newAgentKey(t)
	second := newAgentKey(t)
	extra := newAgentKey(t)
	app := setupTestApp(t)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: first.id, PublicKey: first.pub, Power: 11,
	}))
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: second.id, PublicKey: second.pub, Power: 7,
	}))

	tests := []struct {
		name    string
		commit  abcitypes.CommitInfo
		wantErr string
	}{
		{
			name: "exact set in arbitrary order",
			commit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
				appV20ValidatorVote(second, 7),
				appV20ValidatorVote(first, 11),
			}},
		},
		{name: "empty", commit: abcitypes.CommitInfo{}, wantErr: "empty"},
		{
			name: "duplicate",
			commit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
				appV20ValidatorVote(first, 11),
				appV20ValidatorVote(first, 11),
			}},
			wantErr: "duplicate",
		},
		{
			name: "missing",
			commit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
				appV20ValidatorVote(first, 11),
			}},
			wantErr: "missing",
		},
		{
			name: "extra",
			commit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
				appV20ValidatorVote(first, 11),
				appV20ValidatorVote(second, 7),
				appV20ValidatorVote(extra, 5),
			}},
			wantErr: "unexpected",
		},
		{
			name: "power mismatch",
			commit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
				appV20ValidatorVote(first, 10),
				appV20ValidatorVote(second, 7),
			}},
			wantErr: "power 10",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := app.validateAppV20ActivationValidatorCommit(tt.commit)
			if tt.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tt.wantErr)
		})
	}
}

func TestInitChainRetainsGenesisValidatorPublicKey(t *testing.T) {
	app := setupTestApp(t)
	genesis := i52Validator(t)
	_, err := app.InitChain(context.Background(), &abcitypes.RequestInitChain{Validators: []abcitypes.ValidatorUpdate{genesis}})
	require.NoError(t, err)
	current := app.validators.GetAll()
	require.Len(t, current, 1)
	assert.Equal(t, genesis.PubKey.GetEd25519(), []byte(current[0].PublicKey))
}
