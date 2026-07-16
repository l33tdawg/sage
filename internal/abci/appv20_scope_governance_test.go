package abci

import (
	"context"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func scopeTemplate(scopeID, controller, domain string, revision uint64, state scope.State, members ...scope.Member) scope.Record {
	return scope.Record{
		ScopeID:               scopeID,
		Revision:              revision,
		State:                 state,
		ControllerValidatorID: controller,
		Domains:               []scope.Domain{{Name: domain}},
		Members:               members,
	}
}

func makeScopeGovProposeTx(t *testing.T, proposer agentKey, record scope.Record, targetID string, nonce uint64) []byte {
	t.Helper()
	payload, err := scope.Encode(record)
	require.NoError(t, err)
	reason := "govern canonical replication scope"
	// This lifecycle exercises historical direct-validator governance. Keep the
	// envelope truly proofless; proof-bearing app-v20 requests are tested through
	// the exact REST/dashboard request binders in appv20_governance_agent_proof_test.
	parsed := &tx.ParsedTx{
		Type:  tx.TxTypeGovPropose,
		Nonce: nonce,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpScopeAction,
			TargetID:  targetID,
			Reason:    reason,
			Payload:   payload,
		},
	}
	require.NoError(t, tx.SignTx(parsed, proposer.priv))
	encoded, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return encoded
}

func finalizeScopeBlock(t *testing.T, app *SageApp, height int64, blockTime time.Time, rawTx []byte) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: height,
		Time:   blockTime,
		Txs:    [][]byte{rawTx},
	})
	require.NoError(t, err)
	require.Len(t, resp.TxResults, 1)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	return resp
}

func TestAppV20ScopeGovernanceLifecycle(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV20AppliedHeight = 1
	require.NoError(t, app.badgerStore.RegisterDomain("research", admin.id, "", 1))

	members := []scope.Member{{
		ValidatorID: admin.id, AssignedWeight: 10, JoinedRevision: 1, Active: true,
	}}
	create := scopeTemplate("scope-research", admin.id, "research", 1, scope.StateActive, members...)
	resp := finalizeScopeBlock(t, app, 2, time.Unix(2, 0), makeScopeGovProposeTx(t, admin, create, create.ScopeID, 1))
	require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	assert.Empty(t, resp.ValidatorUpdates, "scope governance never mutates the CometBFT validator set")

	created, err := app.badgerStore.GetScopeRecord(create.ScopeID)
	require.NoError(t, err)
	require.NotNil(t, created)
	assert.Equal(t, int64(2), created.CreatedHeight)
	assert.Equal(t, int64(2), created.UpdatedHeight)
	assert.Equal(t, scope.StateActive, created.State)
	mapped, err := app.badgerStore.GetScopeForDomain("research")
	require.NoError(t, err)
	assert.Equal(t, created, mapped)

	update := create
	update.Revision = 2
	update.State = scope.StatePaused
	resp = finalizeScopeBlock(t, app, 52, time.Unix(52, 0), makeScopeGovProposeTx(t, admin, update, update.ScopeID, 2))
	require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	paused, err := app.badgerStore.GetScopeRecord(update.ScopeID)
	require.NoError(t, err)
	require.NotNil(t, paused)
	assert.Equal(t, uint64(2), paused.Revision)
	assert.Equal(t, scope.StatePaused, paused.State)
	assert.Equal(t, int64(2), paused.CreatedHeight, "created height is consensus-owned and immutable")
	assert.Equal(t, int64(52), paused.UpdatedHeight)

	retire := update
	retire.Revision = 3
	retire.State = scope.StateRetired
	resp = finalizeScopeBlock(t, app, 102, time.Unix(102, 0), makeScopeGovProposeTx(t, admin, retire, retire.ScopeID, 3))
	require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	retired, err := app.badgerStore.GetScopeRecord(retire.ScopeID)
	require.NoError(t, err)
	require.NotNil(t, retired)
	assert.Equal(t, scope.StateRetired, retired.State)
	mapped, err = app.badgerStore.GetScopeForDomain("research")
	require.NoError(t, err)
	assert.Nil(t, mapped, "retirement atomically releases the exact-domain mapping")
}

func TestAppV20ScopeProposalRejectsNonCanonicalAuthority(t *testing.T) {
	member := newAgentKey(t)
	baseRecord := scopeTemplate("scope-a", member.id, "audit", 1, scope.StateActive, scope.Member{
		ValidatorID: member.id, AssignedWeight: 5, JoinedRevision: 1, Active: true,
	})

	tests := []struct {
		name   string
		mutate func(app *SageApp, admin agentKey, record *scope.Record, proposal *tx.GovPropose)
		want   string
	}{
		{
			name: "target mismatch",
			mutate: func(_ *SageApp, _ agentKey, _ *scope.Record, proposal *tx.GovPropose) {
				proposal.TargetID = "scope-b"
			},
			want: "does not match scope_id",
		},
		{
			name: "proposer chooses heights",
			mutate: func(_ *SageApp, _ agentKey, record *scope.Record, _ *tx.GovPropose) {
				record.CreatedHeight = 7
				record.UpdatedHeight = 7
			},
			want: "must be zero",
		},
		{
			name: "unregistered domain",
			mutate: func(_ *SageApp, _ agentKey, record *scope.Record, _ *tx.GovPropose) {
				record.Domains[0].Name = "missing"
			},
			want: "not registered on-chain",
		},
		{
			name: "non-validator member",
			mutate: func(_ *SageApp, _ agentKey, record *scope.Record, _ *tx.GovPropose) {
				record.ControllerValidatorID = member.id
				record.Members[0].ValidatorID = member.id
			},
			want: "not an active on-chain validator",
		},
		{
			name: "nonzero target power",
			mutate: func(_ *SageApp, _ agentKey, _ *scope.Record, proposal *tx.GovPropose) {
				proposal.TargetPower = 1
			},
			want: "target_power must be zero",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app, admin := setupGovTestApp(t)
			app.appV20AppliedHeight = 1
			require.NoError(t, app.badgerStore.RegisterDomain("audit", admin.id, "", 1))
			record := baseRecord
			record.ControllerValidatorID = admin.id
			record.Members = []scope.Member{{
				ValidatorID: admin.id, AssignedWeight: 5, JoinedRevision: 1, Active: true,
			}}
			proposal := &tx.GovPropose{Operation: tx.GovOpScopeAction, TargetID: record.ScopeID}
			tc.mutate(app, admin, &record, proposal)
			payload, err := scope.Encode(record)
			require.NoError(t, err)
			proposal.Payload = payload
			result := app.processGovPropose(&tx.ParsedTx{PublicKey: admin.pub, GovPropose: proposal}, 2, time.Unix(2, 0))
			assert.Equal(t, uint32(72), result.Code)
			assert.Contains(t, result.Log, tc.want)
		})
	}
}

func TestAppV20ScopeUpdatePinsJoinRevisionAndRejectsCollision(t *testing.T) {
	app, admin := setupGovTestApp(t)
	app.appV20AppliedHeight = 1
	require.NoError(t, app.badgerStore.RegisterDomain("audit", admin.id, "", 1))
	require.NoError(t, app.badgerStore.RegisterDomain("security", admin.id, "", 1))

	existing := scopeTemplate("scope-a", admin.id, "audit", 1, scope.StateActive, scope.Member{
		ValidatorID: admin.id, AssignedWeight: 10, JoinedRevision: 1, Active: true,
	})
	existing.CreatedHeight = 2
	existing.UpdatedHeight = 2
	require.NoError(t, app.badgerStore.SetScopeRecord(existing))

	t.Run("existing member cannot rewrite join revision", func(t *testing.T) {
		next := existing
		next.Revision = 2
		next.CreatedHeight = 0
		next.UpdatedHeight = 0
		next.Members[0].JoinedRevision = 2
		payload, err := scope.Encode(next)
		require.NoError(t, err)
		_, err = app.prepareScopeProposal(next.ScopeID, nil, 0, payload, admin.id, 52, false)
		require.ErrorContains(t, err, "joined_revision is immutable")
	})

	t.Run("another scope cannot capture an existing mapping", func(t *testing.T) {
		other := scopeTemplate("scope-b", admin.id, "audit", 1, scope.StateActive, scope.Member{
			ValidatorID: admin.id, AssignedWeight: 10, JoinedRevision: 1, Active: true,
		})
		payload, err := scope.Encode(other)
		require.NoError(t, err)
		_, err = app.prepareScopeProposal(other.ScopeID, nil, 0, payload, admin.id, 52, false)
		require.ErrorContains(t, err, "already bound to scope")
	})
}

func TestPreAppV20ScopeOperationRemainsInertUnknownGovernance(t *testing.T) {
	app, admin := setupGovTestApp(t)
	require.Zero(t, app.appV20AppliedHeight)
	require.NoError(t, app.badgerStore.RegisterDomain("research", admin.id, "", 1))
	record := scopeTemplate("future-scope", admin.id, "research", 1, scope.StateActive, scope.Member{
		ValidatorID: admin.id, AssignedWeight: 10, JoinedRevision: 1, Active: true,
	})

	resp := finalizeScopeBlock(t, app, 2, time.Unix(2, 0), makeScopeGovProposeTx(t, admin, record, record.ScopeID, 1))
	require.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
	got, err := app.badgerStore.GetScopeRecord(record.ScopeID)
	require.NoError(t, err)
	assert.Nil(t, got, "pre-v20 op==8 follows the historical unknown-op apply path")
	proposalID := governance.ComputeProposalID(admin.id, 2, governance.OpScopeAction, record.ScopeID)
	proposal, err := app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	assert.Equal(t, governance.StatusExecuted, proposal.Status, "legacy governance still records and votes the unknown op")
}

func TestAppV20ScopeProposalRequiresValidatorProposer(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 1
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin", "admin")
	require.NoError(t, app.badgerStore.RegisterDomain("audit", admin.id, "", 1))
	member := newAgentKey(t)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: member.id, PublicKey: member.pub, Power: 10}))
	record := scopeTemplate("scope-a", member.id, "audit", 1, scope.StateActive, scope.Member{
		ValidatorID: member.id, AssignedWeight: 10, JoinedRevision: 1, Active: true,
	})
	payload, err := scope.Encode(record)
	require.NoError(t, err)
	result := app.processGovPropose(&tx.ParsedTx{
		PublicKey: admin.pub,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpScopeAction,
			TargetID:  record.ScopeID,
			Payload:   payload,
		},
	}, 2, time.Unix(2, 0))
	assert.Equal(t, uint32(72), result.Code)
	assert.Contains(t, result.Log, "proposer")
	assert.Contains(t, result.Log, "not an active on-chain validator")
}
