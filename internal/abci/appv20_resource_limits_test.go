package abci

import (
	"context"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func TestAppV20ResourceLimitsRejectBeforeNonceOrHandlerMutation(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	key := newAgentKey(t)
	parsed := makeAgentRegisterTx(t, key, strings.Repeat("n", maxAppV20IdentifierBytes+1), "member", "", "test", "")
	parsed.Nonce = 1
	require.NoError(t, tx.SignTx(parsed, key.priv))
	raw := encodeGovernanceReplayTx(t, parsed)

	checked, err := h.app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
	require.NoError(t, err)
	assert.Equal(t, appV20ResourceLimitCode, checked.Code)
	assert.Contains(t, checked.Log, "agent name")

	resp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(20_002, 0).UTC(), Txs: [][]byte{raw},
	})
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, appV20ResourceLimitCode, resp.TxResults[0].Code)
	commitGovernanceReplayBlock(t, h.app)
	assert.False(t, h.app.badgerStore.IsAgentRegistered(key.id))
	nonce, err := h.app.badgerStore.GetNonce(key.id)
	require.NoError(t, err)
	assert.Zero(t, nonce)
}

func TestAppV20ResourceLimitsRejectLargeHashInsideSmallEnoughBlock(t *testing.T) {
	h := newGovernanceReplayHarness(t)
	key := newAgentKey(t)
	parsed := makeMemorySubmitTx(t, key, "general", "hash amplification payload long enough to be otherwise valid")
	parsed.MemorySubmit.ContentHash = make([]byte, 512<<10)
	parsed.Nonce = 1
	require.NoError(t, tx.SignTx(parsed, key.priv))
	raw := encodeGovernanceReplayTx(t, parsed)
	require.Less(t, len(raw), maxAppV20AtomicFinalizeTxBytes, "the field limit, not the aggregate block limit, must reject")

	resp := finalizeGovernanceReplayBlock(t, h.app, &abcitypes.RequestFinalizeBlock{
		Height: 2, Time: time.Unix(20_102, 0).UTC(), Txs: [][]byte{raw},
	})
	require.Len(t, resp.TxResults, 1)
	assert.Equal(t, appV20ResourceLimitCode, resp.TxResults[0].Code)
	assert.Contains(t, resp.TxResults[0].Log, "content hash")
	commitGovernanceReplayBlock(t, h.app)
}

func TestAppV20UpgradeReadinessRejectsOversizedLegacyConsensusState(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)
	app.appV19AppliedHeight = 6
	wantDomain, err := governance.DelegationDomainForChainID("sage-resource-readiness")
	require.NoError(t, err)
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain("sage-resource-readiness"))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		"legacy-memory", make([]byte, maxAppV20IdentifierBytes+1), string(memory.StatusProposed),
	))

	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = wantDomain
	proposal.Nonce = 1
	proposal.Timestamp = time.Unix(20_202, 0).UTC()
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	result := app.processUpgradePropose(proposal, 10, proposal.Timestamp)
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "readiness failed")

	auditMarker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, auditMarker, "a failed exhaustive audit must not persist the completion marker")
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active, "unsafe legacy state must be rejected before opening the governance vote")
}

func TestAppV20LegacyAuditRunsOnlyAfterAuthenticatedAdminChecks(t *testing.T) {
	app := setupTestApp(t)
	app.appV19AppliedHeight = 1
	forged := newAgentKey(t)
	proposal := makeUpgradeProposeTx(t, forged, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	require.NoError(t, tx.SignTx(proposal, forged.priv))

	result := app.processUpgradePropose(proposal, 10, proposal.Timestamp)
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "proposer not registered")
	marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Nil(t, marker, "an unauthenticated/unregistered target-20 proposal must not run or mark the exhaustive audit")
}

func TestAppV20AutoVoterUsesPersistedAuditMarkerWithoutRescan(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)
	app.appV19AppliedHeight = 6
	domain, err := governance.DelegationDomainForChainID("sage-audit-marker")
	require.NoError(t, err)
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain("sage-audit-marker"))
	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = domain
	proposal.Nonce = 1
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	result := app.processUpgradePropose(proposal, 10, proposal.Timestamp)
	require.Zero(t, result.Code, result.Log)

	marker, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	require.NoError(t, err)
	assert.Equal(t, appV20LegacyResourceAuditValue, marker)
	// Test-only raw mutation models why the auto-voter must trust the persisted
	// marker instead of repeating a keyspace scan. In production the marker also
	// keeps every subsequent block on the atomic resource-limited path, so this
	// oversized post-audit write cannot be delivered by a transaction.
	require.NoError(t, app.badgerStore.SetRawForTest([]byte("federation:test-only-after-audit"), make([]byte, maxAppV20EncodedRecordBytes+1)))
	_, _, supported, ok := app.ActiveUpgradeVote()
	require.True(t, ok)
	assert.True(t, supported, "auto-voter readiness must remain bounded and must not repeat the exhaustive audit")
}

func TestAppV20ValidatorReadinessRequiresCanonicalIDKeyBinding(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)
	app.appV19AppliedHeight = 6
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain("sage-validator-readiness"))
	wantDomain, err := governance.DelegationDomainForChainID("sage-validator-readiness")
	require.NoError(t, err)

	rogue := newAgentKey(t)
	mismatchedID := strings.Repeat("ab", 32)
	require.NotEqual(t, rogue.id, mismatchedID)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: mismatchedID, PublicKey: rogue.pub, Power: 1,
	}))

	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = wantDomain
	proposal.Nonce = 1
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	result := app.processUpgradePropose(proposal, 10, proposal.Timestamp)
	assert.Equal(t, uint32(47), result.Code)
	assert.Contains(t, result.Log, "readiness failed")
	assert.Contains(t, result.Log, "does not canonically bind")

	proposalID := proposeActiveAppV20Upgrade(t, app, admin, wantDomain)
	gotID, target, supported, ok := app.ActiveUpgradeVote()
	require.True(t, ok)
	assert.Equal(t, proposalID, gotID)
	assert.Equal(t, uint64(20), target)
	assert.False(t, supported, "a validator must not auto-vote for a set whose IDs do not bind to their public keys")

	activation := setupTestApp(t)
	activation.appV19AppliedHeight = 1
	require.NoError(t, activation.validators.AddValidator(&validator.ValidatorInfo{
		ID: mismatchedID, PublicKey: rogue.pub, Power: 1,
	}))
	require.NoError(t, activation.badgerStore.SetState(appV20LegacyResourceAuditStateKey, appV20LegacyResourceAuditValue))
	require.NoError(t, activation.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV20UpgradeName, TargetAppVersion: 20, ActivationHeight: 10,
		GovernanceDomain: wantDomain,
	}))
	assert.Panics(t, func() { finalizeBlock(t, activation, 10) }, "activation must fail-stop on the same malformed validator binding")
}
