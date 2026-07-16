package abci

import (
	"context"
	"encoding/json"
	"strings"
	"sync"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func proposeActiveAppV20Upgrade(t *testing.T, app *SageApp, proposer agentKey, domain string) string {
	t.Helper()
	require.NoError(t, app.badgerStore.SetState(appV20LegacyResourceAuditStateKey, appV20LegacyResourceAuditValue))
	payload, err := json.Marshal(UpgradeProposalPayload{
		Name:             appV20UpgradeName,
		TargetAppVersion: 20,
		GovernanceDomain: domain,
	})
	require.NoError(t, err)
	proposalID, err := app.govEngine.Propose(
		proposer.id,
		governance.OpUpgrade,
		appV20UpgradeName,
		nil,
		0,
		0,
		"activate validator-bound governance",
		10,
		payload,
	)
	require.NoError(t, err)
	return proposalID
}

func TestAppV20ActiveUpgradeVoteRequiresExactChainDomain(t *testing.T) {
	wantDomain, err := governance.DelegationDomainForChainID("sage-domain-a")
	require.NoError(t, err)
	otherDomain, err := governance.DelegationDomainForChainID("sage-domain-b")
	require.NoError(t, err)

	tests := []struct {
		name           string
		configureChain bool
		proposedDomain string
		wantSupported  bool
	}{
		{name: "exact domain", configureChain: true, proposedDomain: wantDomain, wantSupported: true},
		{name: "missing local chain id", proposedDomain: wantDomain},
		{name: "missing proposed domain", configureChain: true},
		{name: "different chain", configureChain: true, proposedDomain: otherDomain},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app, admin, _, _ := setupAppV8Chain(t, 5)
			app.appV19AppliedHeight = 6
			if tc.configureChain {
				require.NoError(t, app.SetExpectedGovernanceDelegationDomain("sage-domain-a"))
			}
			wantProposalID := proposeActiveAppV20Upgrade(t, app, admin, tc.proposedDomain)

			proposalID, target, supported, ok := app.ActiveUpgradeVote()
			require.True(t, ok)
			assert.Equal(t, wantProposalID, proposalID)
			assert.Equal(t, uint64(20), target)
			assert.Equal(t, tc.wantSupported, supported)
		})
	}
}

func TestExpectedGovernanceDomainBindingIsConcurrentIdempotentAndImmutable(t *testing.T) {
	app := &SageApp{}
	const chainID = "sage-domain-a"
	wantDomain, err := governance.DelegationDomainForChainID(chainID)
	require.NoError(t, err)

	var wg sync.WaitGroup
	errs := make(chan error, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- app.SetExpectedGovernanceDelegationDomain(chainID)
			_ = app.expectedGovernanceDelegationDomain()
		}()
	}
	wg.Wait()
	close(errs)
	for bindErr := range errs {
		require.NoError(t, bindErr)
	}
	require.Equal(t, wantDomain, app.expectedGovernanceDelegationDomain())

	rebindErr := app.SetExpectedGovernanceDelegationDomain("sage-domain-b")
	require.ErrorContains(t, rebindErr, "refusing rebind")
	require.Equal(t, wantDomain, app.expectedGovernanceDelegationDomain())
}

func TestAppV20ActiveUpgradeVoteRecoversAfterLateDomainBinding(t *testing.T) {
	app, admin, _, _ := setupAppV8Chain(t, 5)
	app.appV19AppliedHeight = 6
	wantDomain, err := governance.DelegationDomainForChainID("sage-late-comet")
	require.NoError(t, err)
	wantProposalID := proposeActiveAppV20Upgrade(t, app, admin, wantDomain)

	proposalID, target, supported, ok := app.ActiveUpgradeVote()
	require.True(t, ok)
	require.Equal(t, wantProposalID, proposalID)
	require.Equal(t, uint64(20), target)
	require.False(t, supported, "voter must remain fail-closed before Comet chain-id binding")

	require.NoError(t, app.SetExpectedGovernanceDelegationDomain("sage-late-comet"))
	proposalID, target, supported, ok = app.ActiveUpgradeVote()
	require.True(t, ok)
	require.Equal(t, wantProposalID, proposalID)
	require.Equal(t, uint64(20), target)
	require.True(t, supported, "the same live voter must recover after late Comet binding")
}

func TestAppV20UpgradeCannotSkipItsAppV19Prerequisite(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*SageApp)
		version uint64
	}{
		{name: "fresh app-v1 chain", prepare: func(*SageApp) {}, version: 1},
		{name: "app-v7 chain", prepare: func(app *SageApp) { app.appV7AppliedHeight = 1 }, version: 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app, admin := setupGovTestApp(t)
			tc.prepare(app)
			require.Equal(t, tc.version, app.currentAppVersion())

			proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
			proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
			result := app.processUpgradePropose(proposal, 10, proposal.Timestamp)
			assert.Equal(t, uint32(47), result.Code)
			assert.Contains(t, result.Log, "requires current committed app version 19")

			_, err := app.badgerStore.GetUpgradePlan()
			assert.ErrorIs(t, err, store.ErrNoUpgradePlan)
			active, err := app.govEngine.GetActiveProposal()
			require.NoError(t, err)
			assert.Nil(t, active)
		})
	}
}

func TestAppV20UpgradeAlwaysRoutesThroughGovernanceQuorum(t *testing.T) {
	app, admin := setupGovTestApp(t)
	// Model a chain whose highest committed gate is app-v19. Target 20 must never
	// fall through to the legacy single-signer UpgradePlan path.
	app.appV19AppliedHeight = 1
	require.Equal(t, uint64(19), app.currentAppVersion())

	proposal := makeUpgradeProposeTx(t, admin, appV20UpgradeName, 20, "", defaultUpgradeDelayBlocks)
	proposal.UpgradePropose.GovernanceDomain = governanceReplayTestDomain
	require.NoError(t, tx.SignTx(proposal, admin.priv))
	result := app.processUpgradePropose(proposal, 10, proposal.Timestamp)
	require.Equal(t, uint32(0), result.Code, result.Log)
	assert.Contains(t, result.Log, "awaiting 2/3 quorum")

	_, err := app.badgerStore.GetUpgradePlan()
	assert.ErrorIs(t, err, store.ErrNoUpgradePlan)
	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, active)
	assert.Equal(t, governance.OpUpgrade, active.Operation)
	assert.Equal(t, appV20UpgradeName, active.TargetID)
}

func TestAppV20ActivationRejectsLegacySkipAheadPlan(t *testing.T) {
	app := setupTestApp(t)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV20UpgradeName, TargetAppVersion: 20, ActivationHeight: 10,
		GovernanceDomain: governanceReplayTestDomain,
	}))

	assert.PanicsWithValue(t,
		"sage: refuse app-v20 activation at height 10: current committed app version is 1, want 19",
		func() { finalizeBlock(t, app, 10) },
	)
	_, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err, "the rejected speculative activation must leave its durable plan untouched")
}

func TestAppV20ActivationPersistsDomainBeforePlanConsumption(t *testing.T) {
	app := setupTestApp(t)
	app.appV19AppliedHeight = 1
	activationValidator := newAgentKey(t)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
		ID: activationValidator.id, PublicKey: activationValidator.pub, Power: 10,
	}))
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{activationValidator.id: 10}))
	require.NoError(t, app.badgerStore.SetState(appV20LegacyResourceAuditStateKey, appV20LegacyResourceAuditValue))
	domain, err := governance.DelegationDomainForChainID("sage-activation")
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name:             appV20UpgradeName,
		TargetAppVersion: 20,
		ActivationHeight: 100,
		ProposedAt:       1,
		GovernanceDomain: domain,
	}))

	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 100,
		Time:   time.Unix(17_100, 0).UTC(),
		DecidedLastCommit: abcitypes.CommitInfo{Votes: []abcitypes.VoteInfo{
			appV20ValidatorVote(activationValidator, 10),
		}},
	})
	require.NoError(t, err)
	require.NotNil(t, resp.ConsensusParamUpdates)
	require.NotNil(t, resp.ConsensusParamUpdates.Version)
	assert.Equal(t, uint64(20), resp.ConsensusParamUpdates.Version.App)
	assert.Zero(t, app.appV20AppliedHeight, "FinalizeBlock remains speculative until Commit")
	assert.Empty(t, app.GovernanceDelegationDomain())
	pending, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.NotNil(t, pending)

	commitGovernanceReplayBlock(t, app)
	assert.Equal(t, int64(100), app.appV20AppliedHeight)
	assert.Equal(t, domain, app.GovernanceDelegationDomain())

	_, err = app.badgerStore.GetUpgradePlan()
	require.ErrorIs(t, err, store.ErrNoUpgradePlan, "activation consumes the plan only after materializing the domain")
	applied, err := app.badgerStore.GetAppliedUpgrade(appV20UpgradeName)
	require.NoError(t, err)
	require.NotNil(t, applied)
	assert.Equal(t, int64(100), applied.AppliedHeight)

	// Once the app-side Commit succeeds, CometBFT's persisted
	// FinalizeBlockResponse owns the crash handshake; the application must not
	// re-execute height 100 and cannot invent or change authorization state.
}

func TestEnsureGovernanceDelegationDomainIsCanonicalAndImmutable(t *testing.T) {
	app := setupTestApp(t)
	domain := strings.Repeat("ab", 32)
	require.NoError(t, app.ensureGovernanceDelegationDomain(domain))
	require.NoError(t, app.ensureGovernanceDelegationDomain(domain), "exact activation replay is idempotent")
	assert.Equal(t, domain, app.GovernanceDelegationDomain())

	assert.ErrorContains(t, app.ensureGovernanceDelegationDomain(strings.ToUpper(domain)), "canonical")
	assert.ErrorContains(t, app.ensureGovernanceDelegationDomain(strings.Repeat("cd", 32)), "differs")
	assert.Equal(t, domain, app.GovernanceDelegationDomain(), "rejected values must not replace committed state")
}
