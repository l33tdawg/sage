package abci

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func permissionTxWithCapabilities(
	t *testing.T,
	sender agentKey,
	targetID string,
	clearance uint8,
	capabilities store.AgentCapabilities,
) *tx.ParsedTx {
	t.Helper()
	parsed := makeAgentSetPermissionTx(t, sender, targetID, clearance, "", "*", "", "")
	parsed.AgentSetPermission.Capabilities = uint32(capabilities)
	parsed.AgentSetPermission.CapabilitiesPresent = true
	return parsed
}

func seedAppV22PredecessorLadder(
	t *testing.T,
	bs *store.BadgerStore,
	firstHeight int64,
	skipVersion uint64,
	heightOverride map[uint64]int64,
) {
	t.Helper()
	for version := uint64(6); version <= 21; version++ {
		if version == skipVersion {
			continue
		}
		height := firstHeight + int64(version-6)
		if override, ok := heightOverride[version]; ok {
			height = override
		}
		require.NoError(t, bs.MarkUpgradeApplied(tx.CanonicalUpgradeName(version), version, height))
	}
}

func TestAppV22ForkGateAndVersionLockstep(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV22Fork(100))
	assert.False(t, app.IsAppV22ActiveForNextTx())
	assert.Equal(t, tx.CanonicalUpgradeName(22), appV22UpgradeName)
	assert.Equal(t, uint64(26), MaxSupportedAppVersion())

	app.appV21AppliedHeight = 99
	app.appV22AppliedHeight = 100
	app.state.Height = 100
	assert.False(t, app.postAppV22Fork(100), "activation block remains app-v21")
	assert.True(t, app.postAppV22Fork(101), "capability enforcement starts at H+1")
	assert.True(t, app.IsAppV22ActiveForNextTx())
	assert.True(t, app.postAppV8Rules(101), "app-v22 subsumes additive lower rules")
	assert.True(t, app.postAppV17Rules(101), "app-v22 subsumes delegated-proof rules")
	assert.True(t, app.postAppV19Rules(101), "app-v22 subsumes readiness rules")
	assert.False(t, app.postAppV18Rules(101), "app-v22 must not enable the independent admin override")
	assert.Equal(t, uint64(22), app.currentAppVersion())
}

func TestReplayAppV22BootRefreshRequiresPersistedPredecessorLadder(t *testing.T) {
	t.Run("canonical app-v6 plus every ordered independent predecessor passes", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv22.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		seedAppV22PredecessorLadder(t, bs, 4100, 0, nil)
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 22, 4400))
		require.NoError(t, SaveState(bs, &AppState{Height: 4400}))

		app, err := NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.NoError(t, err)
		assert.Equal(t, int64(4400), app.appV22AppliedHeight)
		assert.Equal(t, uint64(22), app.currentAppVersion())
		assert.False(t, app.postAppV22Fork(4400))
		assert.True(t, app.postAppV22Fork(4401))
	})

	t.Run("missing canonical cumulative app-v6 evidence fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv22-missing-v6.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		seedAppV22PredecessorLadder(t, bs, 4100, 6, nil)
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 22, 4400))
		require.NoError(t, SaveState(bs, &AppState{Height: 4400}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "missing canonical applied app-v6 predecessor")
	})

	t.Run("missing app-v18 fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv22-missing-v18.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		seedAppV22PredecessorLadder(t, bs, 4100, 18, nil)
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 22, 4400))
		require.NoError(t, SaveState(bs, &AppState{Height: 4400}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "missing canonical applied app-v18 predecessor")
	})

	t.Run("missing early independent app-v7 fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv22-missing-v7.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		seedAppV22PredecessorLadder(t, bs, 4100, 7, nil)
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 22, 4400))
		require.NoError(t, SaveState(bs, &AppState{Height: 4400}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "missing canonical applied app-v7 predecessor")
	})

	t.Run("non-monotonic independent predecessor height fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv22-bad-order.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		seedAppV22PredecessorLadder(t, bs, 4100, 0, map[uint64]int64{18: 4110})
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 22, 4400))
		require.NoError(t, SaveState(bs, &AppState{Height: 4400}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "app-v18 predecessor height 4110 must be after app-v17 height 4111")
	})

	t.Run("malformed applied target fails closed", func(t *testing.T) {
		bs := setupTestBadger(t)
		sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "appv22-bad-target.db"))
		require.NoError(t, err)
		t.Cleanup(func() { _ = sqlite.Close() })

		seedAppV22PredecessorLadder(t, bs, 4100, 0, nil)
		seedTestGovernanceDelegationDomain(t, bs)
		require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 23, 4400))
		require.NoError(t, SaveState(bs, &AppState{Height: 4400}))

		_, err = NewSageAppWithStores(bs, sqlite, zerolog.Nop())
		require.ErrorContains(t, err, "target app version 23, want 22")
	})
}

func TestReplayAppV22ActivationAndCrashReplayReemitVersion(t *testing.T) {
	app := setupTestApp(t)
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 0, nil)
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.state.Height = 29
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV22UpgradeName, TargetAppVersion: 22, ActivationHeight: 30, ProposedAt: 29,
	}))

	first, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 30, Time: time.Unix(100, 0),
	})
	require.NoError(t, err)
	require.NotNil(t, first.ConsensusParamUpdates)
	assert.Equal(t, uint64(22), first.ConsensusParamUpdates.Version.App)
	assert.Equal(t, int64(30), app.pendingAppV20Finalize.app.appV22AppliedHeight)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	assert.Equal(t, int64(30), app.appV22AppliedHeight)
	assert.Equal(t, uint64(22), app.currentAppVersion())

	replay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 30, Time: time.Unix(100, 0),
	})
	require.NoError(t, err)
	require.NotNil(t, replay.ConsensusParamUpdates)
	assert.Equal(t, uint64(22), replay.ConsensusParamUpdates.Version.App)
}

func TestReplayAppV22ActivationRejectsMalformedTargetBeforeMutation(t *testing.T) {
	app := setupTestApp(t)
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 0, nil)
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.state.Height = 29
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV22UpgradeName, TargetAppVersion: 23, ActivationHeight: 30, ProposedAt: 29,
	}))

	_, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 30, Time: time.Unix(100, 0),
	})
	require.ErrorContains(t, err, "malformed app-v22 activation")
	applied, getErr := app.badgerStore.GetAppliedUpgrade(appV22UpgradeName)
	require.NoError(t, getErr)
	assert.Nil(t, applied)
	assert.Zero(t, app.appV22AppliedHeight)
}

func TestAppV22ProposalRejectsIncompletePersistedPredecessorLadder(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "global-admin", "admin")
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 18, nil)
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.state.Height = 30

	proposal := makeUpgradeProposeTx(t, admin, appV22UpgradeName, 22, "", defaultUpgradeDelayBlocks)
	result := app.processUpgradePropose(proposal, 31, proposal.Timestamp)
	require.Equal(t, uint32(47), result.Code, result.Log)
	require.Contains(t, result.Log, "missing canonical applied app-v18 predecessor")

	active, err := app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.Nil(t, active, "an invalid app-v22 ladder must not reach governance")
}

func TestAppV22ApprovedProposalExecutionRejectsIncompletePersistedPredecessorLadder(t *testing.T) {
	app := setupTestApp(t)
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 18, nil)
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.state.Height = 30

	payload, err := json.Marshal(UpgradeProposalPayload{
		Name:             appV22UpgradeName,
		TargetAppVersion: 22,
	})
	require.NoError(t, err)
	err = app.applyUpgradeProposal(&governance.ProposalState{
		ProposalID: "approved-app-v22",
		Operation:  governance.OpUpgrade,
		TargetID:   appV22UpgradeName,
		Status:     governance.StatusExecuted,
		Payload:    payload,
	}, 31)
	require.ErrorContains(t, err, "missing canonical applied app-v18 predecessor")

	plan, getErr := app.badgerStore.GetUpgradePlan()
	require.ErrorIs(t, getErr, store.ErrNoUpgradePlan)
	require.Nil(t, plan)
}

func TestAppV22FinalizationRejectsIncompletePersistedPredecessorLadder(t *testing.T) {
	app := setupTestApp(t)
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 18, nil)
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.state.Height = 29
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV22UpgradeName, TargetAppVersion: 22, ActivationHeight: 30, ProposedAt: 29,
	}))

	_, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 30,
		Time:   time.Unix(100, 0),
	})
	require.ErrorContains(t, err, "missing canonical applied app-v18 predecessor")

	applied, getErr := app.badgerStore.GetAppliedUpgrade(appV22UpgradeName)
	require.NoError(t, getErr)
	require.Nil(t, applied)
	require.Zero(t, app.appV22AppliedHeight)
}

func TestAppV22CapabilityChangesRequireGlobalAdmin(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	companion := newAgentKey(t)
	registerAgent(t, app, admin, "global-admin", "admin")
	registerAgent(t, app, companion, "voice-companion", "member")

	companionMask := store.AgentCapabilityReadAllDomains |
		store.AgentCapabilityDenySharedDomainWrite |
		store.AgentCapabilityDenyDomainClaim |
		store.AgentCapabilityDenyForeignDomainWrite

	preFork := app.processAgentSetPermission(
		permissionTxWithCapabilities(t, admin, companion.id, 1, companionMask),
		100,
		time.Now(),
	)
	require.Equal(t, uint32(67), preFork.Code)
	require.Contains(t, preFork.Log, "require app-v22")

	app.appV22AppliedHeight = 100
	applied := app.processAgentSetPermission(
		permissionTxWithCapabilities(t, admin, companion.id, 1, companionMask),
		101,
		time.Now(),
	)
	require.Equal(t, uint32(0), applied.Code, applied.Log)

	onChain, err := app.badgerStore.GetRegisteredAgent(companion.id)
	require.NoError(t, err)
	require.Equal(t, companionMask, onChain.Capabilities)

	legacyUpdate := makeAgentSetPermissionTx(t, admin, companion.id, 1, "", "*", "", "")
	legacyResult := app.processAgentSetPermission(legacyUpdate, 102, time.Now())
	require.Equal(t, uint32(0), legacyResult.Code, legacyResult.Log)
	onChain, err = app.badgerStore.GetRegisteredAgent(companion.id)
	require.NoError(t, err)
	assert.Equal(t, companionMask, onChain.Capabilities, "legacy permission tx must preserve an assigned mask")

	selfClear := app.processAgentSetPermission(
		permissionTxWithCapabilities(t, companion, companion.id, 1, 0),
		103,
		time.Now(),
	)
	require.Equal(t, uint32(67), selfClear.Code)
	require.Contains(t, selfClear.Log, "global administrator")

	onChain, err = app.badgerStore.GetRegisteredAgent(companion.id)
	require.NoError(t, err)
	assert.Equal(t, companionMask, onChain.Capabilities, "restricted agent must not clear its own mask")

	unknown := app.processAgentSetPermission(
		permissionTxWithCapabilities(t, admin, companion.id, 1, store.AgentCapabilities(1<<31)),
		103,
		time.Now(),
	)
	require.Equal(t, uint32(67), unknown.Code)
	require.Contains(t, unknown.Log, "unknown agent capability bits")

	adminSelfSet := app.processAgentSetPermission(
		permissionTxWithCapabilities(t, admin, admin.id, 1, store.AgentCapabilityReadAllDomains),
		104,
		time.Now(),
	)
	require.Equal(t, uint32(0), adminSelfSet.Code, adminSelfSet.Log)
	onChain, err = app.badgerStore.GetRegisteredAgent(admin.id)
	require.NoError(t, err)
	assert.Equal(t, store.AgentCapabilityReadAllDomains, onChain.Capabilities,
		"a global administrator must retain capability authority when it is also the target")
}

func TestAppV22CapabilityPermissionProjectsToOffchainAgentStore(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	companion := newAgentKey(t)
	registerAgent(t, app, admin, "global-admin", "admin")
	registerAgent(t, app, companion, "voice-companion", "member")
	app.appV22AppliedHeight = 10

	mask := store.AgentCapabilityReadAllDomains |
		store.AgentCapabilityDenySharedDomainWrite |
		store.AgentCapabilityDenyDomainClaim |
		store.AgentCapabilityDenyForeignDomainWrite
	result := app.processAgentSetPermission(
		permissionTxWithCapabilities(t, admin, companion.id, 1, mask),
		11,
		time.Now(),
	)
	require.Zero(t, result.Code, result.Log)

	ctx := context.Background()
	require.NoError(t, app.offchainStore.RunInAgentContactTx(ctx, func(s store.OffchainStore) error {
		return app.flushPendingWrites(ctx, s, app.pendingWrites)
	}))
	projected, err := app.offchainStore.GetAgent(ctx, companion.id)
	require.NoError(t, err)
	assert.Equal(t, mask, projected.Capabilities)
}

func TestAppV22AnyPermissionFieldChangeRequiresGlobalAdmin(t *testing.T) {
	app := setupTestApp(t)
	app.appV22AppliedHeight = 10
	globalAdmin := newAgentKey(t)
	orgAdmin := newAgentKey(t)
	target := newAgentKey(t)
	registerAgent(t, app, globalAdmin, "global-admin", "admin")
	registerAgent(t, app, orgAdmin, "org-admin", "member")
	registerAgent(t, app, target, "read-all-target", "member")
	require.NoError(t, app.badgerStore.RegisterOrg("shared-org", "Shared Org", "", orgAdmin.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember("shared-org", orgAdmin.id, 4, "admin", 1))
	require.NoError(t, app.badgerStore.AddOrgMember("shared-org", target.id, 1, "member", 1))
	require.NoError(t, app.badgerStore.SetAgentPermissionWithCapabilities(
		target.id, 1, `[]`, `[]`, "", "", store.AgentCapabilityReadAllDomains,
	))

	noOp := makeAgentSetPermissionTx(t, orgAdmin, target.id, 1, `[]`, `[]`, "", "")
	noOpResult := app.processAgentSetPermission(noOp, 11, time.Now())
	require.Equal(t, uint32(0), noOpResult.Code, noOpResult.Log)

	fieldChanges := map[string]*tx.AgentSetPermission{
		"clearance": {
			AgentID: target.id, Clearance: 4, DomainAccess: `[]`, VisibleAgents: `[]`,
		},
		"domain access": {
			AgentID: target.id, Clearance: 1, DomainAccess: `[{"domain":"*","read":true}]`, VisibleAgents: `[]`,
		},
		"visible agents": {
			AgentID: target.id, Clearance: 1, DomainAccess: `[]`, VisibleAgents: `*`,
		},
		"organization": {
			AgentID: target.id, Clearance: 1, DomainAccess: `[]`, VisibleAgents: `[]`, OrgID: "other-org",
		},
		"department": {
			AgentID: target.id, Clearance: 1, DomainAccess: `[]`, VisibleAgents: `[]`, DeptID: "other-dept",
		},
	}
	for name, permission := range fieldChanges {
		t.Run(name, func(t *testing.T) {
			body := []byte("permission-change:" + name)
			pubKey, sig, bodyHash, ts := signAgentProof(t, orgAdmin, body)
			result := app.processAgentSetPermission(&tx.ParsedTx{
				Type: tx.TxTypeAgentSetPermission, AgentSetPermission: permission,
				AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
			}, 12, time.Now())
			require.Equal(t, uint32(67), result.Code)
			require.Contains(t, result.Log, "global administrator")

			current, err := app.badgerStore.GetRegisteredAgent(target.id)
			require.NoError(t, err)
			require.Equal(t, uint8(1), current.Clearance)
			require.Equal(t, `[]`, current.DomainAccess)
			require.Equal(t, `[]`, current.VisibleAgents)
			require.Empty(t, current.OrgID)
			require.Empty(t, current.DeptID)
			require.Equal(t, store.AgentCapabilityReadAllDomains, current.Capabilities)
		})
	}

	globalChange := makeAgentSetPermissionTx(t, globalAdmin, target.id, 4, `[]`, `[]`, "", "")
	globalResult := app.processAgentSetPermission(globalChange, 13, time.Now())
	require.Equal(t, uint32(0), globalResult.Code, globalResult.Log)
	current, err := app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	require.Equal(t, uint8(4), current.Clearance)
	require.Equal(t, store.AgentCapabilityReadAllDomains, current.Capabilities)
}

func TestAppV22FreshIdentityMustRegisterAndReceivesRestrictiveDefaults(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 1
	app.appV22AppliedHeight = 10
	activationAgent := newAgentKey(t)
	activationWrite := app.processMemorySubmit(
		makeMemorySubmitTx(t, activationAgent, "general", "activation-block legacy write"),
		10,
		time.Now(),
	)
	require.Equal(t, uint32(0), activationWrite.Code, activationWrite.Log)

	activationRegistrant := newAgentKey(t)
	activationRegistration := app.processAgentRegister(
		makeAgentRegisterTx(t, activationRegistrant, "activation-agent", "member", "", "codex", ""),
		10,
		time.Now(),
	)
	require.Equal(t, uint32(0), activationRegistration.Code, activationRegistration.Log)
	activationOnChain, err := app.badgerStore.GetRegisteredAgent(activationRegistrant.id)
	require.NoError(t, err)
	assert.Zero(t, activationOnChain.Capabilities, "the activation block must preserve legacy registration bytes")

	fresh := newAgentKey(t)

	unregistered := app.processMemorySubmit(
		makeMemorySubmitTx(t, fresh, "general", "fresh-key bypass"),
		11,
		time.Now(),
	)
	require.Equal(t, uint32(11), unregistered.Code)
	require.Contains(t, unregistered.Log, "capability lookup")

	freshEnvelope, _ := buildCoCommitEnvelope(t, fresh, "general", []byte("fresh-key-cocommit"), "sage-b")
	unregisteredCoCommit := app.processCoCommitSubmit(
		coCommitSubmitTx(t, fresh, freshEnvelope),
		11,
		time.Now(),
	)
	require.Equal(t, uint32(97), unregisteredCoCommit.Code)
	require.Contains(t, unregisteredCoCommit.Log, "capability lookup")

	unregisteredGrant := app.processAccessGrant(
		makeAccessGrantTx(t, fresh, activationRegistrant.id, "fresh.unowned", 1),
		11,
		time.Now(),
	)
	require.Equal(t, uint32(34), unregisteredGrant.Code)
	require.Contains(t, unregisteredGrant.Log, "capability lookup")

	registerBody := []byte("fresh.explicit")
	pubKey, sig, bodyHash, ts := signAgentProof(t, fresh, registerBody)
	unregisteredDomain := app.processDomainRegister(&tx.ParsedTx{
		Type: tx.TxTypeDomainRegister,
		DomainRegister: &tx.DomainRegister{
			DomainName:   "fresh.explicit",
			OwnerAgentID: fresh.id,
		},
		AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}, 11, time.Now())
	require.Equal(t, uint32(45), unregisteredDomain.Code)
	require.Contains(t, unregisteredDomain.Log, "capability lookup")

	registered := app.processAgentRegister(
		makeAgentRegisterTx(t, fresh, "fresh-agent", "member", "", "codex", ""),
		12,
		time.Now(),
	)
	require.Equal(t, uint32(0), registered.Code, registered.Log)

	onChain, err := app.badgerStore.GetRegisteredAgent(fresh.id)
	require.NoError(t, err)
	assert.Equal(t, store.DefaultSelfRegisteredAgentCapabilities, onChain.Capabilities)

	shared := app.processMemorySubmit(
		makeMemorySubmitTx(t, fresh, "general", "fresh-key shared write"),
		13,
		time.Now(),
	)
	require.Equal(t, uint32(11), shared.Code)
	require.Contains(t, shared.Log, "cannot write shared domain")

	claim := app.processMemorySubmit(
		makeMemorySubmitTx(t, fresh, "fresh.private", "fresh-key claim"),
		14,
		time.Now(),
	)
	require.Equal(t, uint32(11), claim.Code)
	require.Contains(t, claim.Log, "cannot claim unowned domain")
}

func TestAppV22CapabilityLookupRejectsUnknownStoredBits(t *testing.T) {
	app := setupTestApp(t)
	app.appV22AppliedHeight = 10
	agent := newAgentKey(t)
	registerAgent(t, app, agent, "bad-mask", "member")
	onChain, err := app.badgerStore.GetRegisteredAgent(agent.id)
	require.NoError(t, err)
	onChain.Capabilities = store.AgentCapabilities(1 << 31)
	rawAgent, err := json.Marshal(onChain)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetRawForTest([]byte("agent:"+agent.id), rawAgent))

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, agent, "general", "unknown mask"),
		11,
		time.Now(),
	)
	require.Equal(t, uint32(11), result.Code)
	require.Contains(t, result.Log, "unknown bits")

	require.NoError(t, app.badgerStore.SetRawForTest([]byte("agent:"+agent.id), []byte("{not-json")))
	corrupt := app.processMemorySubmit(
		makeMemorySubmitTx(t, agent, "general", "corrupt capability record"),
		12,
		time.Now(),
	)
	require.Equal(t, uint32(11), corrupt.Code)
	require.Contains(t, corrupt.Log, "capability lookup")
}

func TestAppV22CompanionWriteRestrictionsAreConsensusEnforced(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 1
	app.appV22AppliedHeight = 10
	admin := newAgentKey(t)
	owner := newAgentKey(t)
	companion := newAgentKey(t)
	registerAgent(t, app, admin, "global-admin", "admin")
	registerAgent(t, app, owner, "domain-owner", "member")
	registerAgent(t, app, companion, "voice-companion", "member")

	mask := store.AgentCapabilityReadAllDomains |
		store.AgentCapabilityDenySharedDomainWrite |
		store.AgentCapabilityDenyDomainClaim |
		store.AgentCapabilityDenyForeignDomainWrite
	require.NoError(t, app.badgerStore.SetAgentPermissionWithCapabilities(companion.id, 1, "", "*", "", "", mask))

	shared := app.processMemorySubmit(makeMemorySubmitTx(t, companion, "general", "shared write"), 11, time.Now())
	require.Equal(t, uint32(11), shared.Code)
	require.Contains(t, shared.Log, "cannot write shared domain")
	sharedEnvelope, _ := buildCoCommitEnvelope(t, companion, "general", []byte("appv22-shared"), "sage-b")
	sharedCoCommit := app.processCoCommitSubmit(coCommitSubmitTx(t, companion, sharedEnvelope), 11, time.Now())
	require.Equal(t, uint32(97), sharedCoCommit.Code)
	require.Contains(t, sharedCoCommit.Log, "cannot write shared domain")

	unowned := app.processMemorySubmit(makeMemorySubmitTx(t, companion, "voice.private", "claim attempt"), 12, time.Now())
	require.Equal(t, uint32(11), unowned.Code)
	require.Contains(t, unowned.Log, "cannot claim unowned domain")
	_, err := app.badgerStore.GetDomainOwner("voice.private")
	require.Error(t, err)

	require.NoError(t, app.badgerStore.RegisterDomain("research", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("research", companion.id, 2, 0, owner.id))
	foreign := app.processMemorySubmit(makeMemorySubmitTx(t, companion, "research", "foreign write"), 13, time.Now())
	require.Equal(t, uint32(11), foreign.Code)
	require.Contains(t, foreign.Log, "does not own")
	foreignEnvelope, _ := buildCoCommitEnvelope(t, companion, "research", []byte("appv22-foreign"), "sage-b")
	foreignCoCommit := app.processCoCommitSubmit(coCommitSubmitTx(t, companion, foreignEnvelope), 13, time.Now())
	require.Equal(t, uint32(97), foreignCoCommit.Code)
	require.Contains(t, foreignCoCommit.Log, "does not own")

	require.NoError(t, app.badgerStore.RegisterDomain("companion-owned", companion.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("companion-owned", companion.id, 2, 0, companion.id))
	owned := app.processMemorySubmit(makeMemorySubmitTx(t, companion, "companion-owned", "owned write"), 14, time.Now())
	require.Equal(t, uint32(0), owned.Code, owned.Log)
}

func TestAppV22RequiresExplicitWriteWithoutAppV18(t *testing.T) {
	app := setupTestApp(t)
	app.appV15AppliedHeight = 1
	app.appV22AppliedHeight = 10
	require.False(t, app.postAppV18Rules(20), "the test must cover a valid app-v22 chain that skipped independent app-v18")
	require.True(t, app.postAppV22Rules(20))

	runBothSubmitPaths := func(t *testing.T, writer agentKey, domain, label string, wantNormal, wantCoCommit uint32) {
		t.Helper()
		normal := app.processMemorySubmit(
			makeMemorySubmitTx(t, writer, domain, label+" normal"),
			20,
			time.Unix(200, 0),
		)
		require.Equal(t, wantNormal, normal.Code, "normal submit: %s", normal.Log)

		envelope, _ := buildCoCommitEnvelope(
			t,
			writer,
			domain,
			[]byte(label+" co-commit"),
			"sage-b",
		)
		coCommit := app.processCoCommitSubmit(
			coCommitSubmitTx(t, writer, envelope),
			20,
			time.Unix(200, 0),
		)
		require.Equal(t, wantCoCommit, coCommit.Code, "co-commit: %s", coCommit.Log)
	}

	ownerWriter := newAgentKey(t)
	foreignOwner := newAgentKey(t)
	registerAgent(t, app, ownerWriter, "owner-writer", "member")
	registerAgent(t, app, foreignOwner, "foreign-owner", "member")

	require.NoError(t, app.badgerStore.RegisterDomain("v22.owner", ownerWriter.id, "", 1))
	t.Run("effective owner remains writable", func(t *testing.T) {
		runBothSubmitPaths(t, ownerWriter, "v22.owner", "owner", 0, 0)
	})

	require.NoError(t, app.badgerStore.RegisterDomain("v22.level2", foreignOwner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("v22.level2", ownerWriter.id, 2, 0, foreignOwner.id))
	t.Run("level two grant permits write", func(t *testing.T) {
		runBothSubmitPaths(t, ownerWriter, "v22.level2", "level2", 0, 0)
	})

	require.NoError(t, app.badgerStore.RegisterDomain("v22.level1", foreignOwner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("v22.level1", ownerWriter.id, 1, 0, foreignOwner.id))
	t.Run("level one grant remains read only", func(t *testing.T) {
		runBothSubmitPaths(t, ownerWriter, "v22.level1", "level1", 11, 97)
	})

	sameOrgWriter := newAgentKey(t)
	sameOrgOwner := newAgentKey(t)
	registerAgent(t, app, sameOrgWriter, "same-org-writer", "member")
	registerAgent(t, app, sameOrgOwner, "same-org-owner", "member")
	require.NoError(t, app.badgerStore.RegisterOrg("v22-same-org", "Same Org", "", sameOrgOwner.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember("v22-same-org", sameOrgWriter.id, 4, "member", 1))
	require.NoError(t, app.badgerStore.AddOrgMember("v22-same-org", sameOrgOwner.id, 4, "member", 1))
	require.NoError(t, app.badgerStore.RegisterDomain("v22.same-org-read", sameOrgOwner.id, "", 1))
	t.Run("same organization clearance remains read only", func(t *testing.T) {
		runBothSubmitPaths(t, sameOrgWriter, "v22.same-org-read", "same-org", 11, 97)
	})

	federatedWriter := newAgentKey(t)
	federatedOwner := newAgentKey(t)
	registerAgent(t, app, federatedWriter, "federated-writer", "member")
	registerAgent(t, app, federatedOwner, "federated-owner", "member")
	require.NoError(t, app.badgerStore.RegisterOrg("v22-writer-org", "Writer Org", "", federatedWriter.id, 1))
	require.NoError(t, app.badgerStore.RegisterOrg("v22-owner-org", "Owner Org", "", federatedOwner.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember("v22-writer-org", federatedWriter.id, 4, "member", 1))
	require.NoError(t, app.badgerStore.AddOrgMember("v22-owner-org", federatedOwner.id, 4, "member", 1))
	require.NoError(t, app.badgerStore.RegisterDomain("v22.federated-read", federatedOwner.id, "", 1))
	require.NoError(t, app.badgerStore.SetFederation(
		"v22-read-only-federation",
		"v22-writer-org",
		"v22-owner-org",
		[]string{"v22.federated-read"},
		4,
		0,
		true,
		"active",
	))
	t.Run("organization federation remains read only", func(t *testing.T) {
		runBothSubmitPaths(t, federatedWriter, "v22.federated-read", "federation", 11, 97)
	})
}

func TestAppV22DomainClaimRestrictionCoversGrantAndExplicitRegister(t *testing.T) {
	app := setupTestApp(t)
	app.appV22AppliedHeight = 10
	companion := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, companion, "voice-companion", "member")
	registerAgent(t, app, grantee, "recipient", "member")
	require.NoError(t, app.badgerStore.SetAgentPermissionWithCapabilities(
		companion.id, 1, "", "*", "", "", store.AgentCapabilityDenyDomainClaim,
	))

	grant := app.processAccessGrant(makeAccessGrantTx(t, companion, grantee.id, "unowned.grant", 1), 11, time.Now())
	require.Equal(t, uint32(34), grant.Code)
	require.Contains(t, grant.Log, "cannot claim unowned domain")

	body := []byte("explicit.domain")
	pubKey, sig, bodyHash, ts := signAgentProof(t, companion, body)
	register := app.processDomainRegister(&tx.ParsedTx{
		Type: tx.TxTypeDomainRegister,
		DomainRegister: &tx.DomainRegister{
			DomainName:   "explicit.domain",
			OwnerAgentID: companion.id,
		},
		AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}, 12, time.Now())
	require.Equal(t, uint32(45), register.Code)
	require.Contains(t, register.Log, "cannot register domains")
}

func TestAppV22OrganizationEscalationRequiresGlobalAdmin(t *testing.T) {
	app := setupTestApp(t)
	app.appV22AppliedHeight = 10
	const (
		localOrgID        = "local-org-000000000000"
		adminCreatedOrgID = "admin-created-000000"
		selfElevationID   = "self-elevation-000000"
	)
	globalAdmin := newAgentKey(t)
	outsideGlobalAdmin := newAgentKey(t)
	orgAdmin := newAgentKey(t)
	member := newAgentKey(t)
	registerAgent(t, app, globalAdmin, "global-admin", "admin")
	registerAgent(t, app, outsideGlobalAdmin, "outside-global-admin", "admin")
	registerAgent(t, app, orgAdmin, "local-org-admin", "member")
	registerAgent(t, app, member, "member", "member")

	require.NoError(t, app.badgerStore.RegisterOrg(localOrgID, "Local Org", "", orgAdmin.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember(localOrgID, orgAdmin.id, uint8(tx.ClearanceTopSecret), "admin", 1))
	require.NoError(t, app.badgerStore.AddOrgMember(localOrgID, globalAdmin.id, uint8(tx.ClearanceTopSecret), "member", 1))

	orgRegister := func(signer agentKey, orgID string) *tx.ParsedTx {
		body := []byte("org-register:" + orgID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeOrgRegister,
			OrgRegister: &tx.OrgRegister{
				OrgID: orgID, Name: orgID, AdminAgent: signer.id,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	orgAdd := func(signer agentKey, orgID string) *tx.ParsedTx {
		body := []byte("org-add:" + orgID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeOrgAddMember,
			OrgAddMember: &tx.OrgAddMember{
				OrgID: orgID, AgentID: member.id, Clearance: tx.ClearanceInternal, Role: "member",
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	orgSetClearance := func(signer agentKey, orgID, targetID string, clearance tx.ClearanceLevel) *tx.ParsedTx {
		body := []byte("org-clearance:" + orgID + ":" + targetID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeOrgSetClearance,
			OrgSetClearance: &tx.OrgSetClearance{
				OrgID: orgID, AgentID: targetID, Clearance: clearance,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	deptRegister := func(signer agentKey, orgID, deptID string) *tx.ParsedTx {
		body := []byte("dept-register:" + orgID + ":" + deptID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeDeptRegister,
			DeptRegister: &tx.DeptRegister{
				OrgID: orgID, DeptID: deptID, DeptName: deptID,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	deptAdd := func(signer agentKey, orgID, deptID string) *tx.ParsedTx {
		body := []byte("dept-add:" + orgID + ":" + deptID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeDeptAddMember,
			DeptAddMember: &tx.DeptAddMember{
				OrgID: orgID, DeptID: deptID, AgentID: member.id,
				Clearance: tx.ClearanceTopSecret, Role: "member",
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	orgRemove := func(signer agentKey, orgID string) *tx.ParsedTx {
		body := []byte("org-remove:" + orgID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeOrgRemoveMember,
			OrgRemoveMember: &tx.OrgRemoveMember{
				OrgID: orgID, AgentID: member.id,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	deptRemove := func(signer agentKey, orgID, deptID string) *tx.ParsedTx {
		body := []byte("dept-remove:" + orgID + ":" + deptID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeDeptRemoveMember,
			DeptRemoveMember: &tx.DeptRemoveMember{
				OrgID: orgID, DeptID: deptID, AgentID: member.id,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	federationPropose := func(signer agentKey, maxClearance tx.ClearanceLevel, expiresAt int64) *tx.ParsedTx {
		body := []byte("federation-propose")
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeFederationPropose,
			FederationPropose: &tx.FederationPropose{
				ProposerOrgID: localOrgID, TargetOrgID: adminCreatedOrgID,
				AllowedDomains: []string{"voice"}, MaxClearance: maxClearance, ExpiresAt: expiresAt,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	federationApprove := func(signer agentKey, federationID string) *tx.ParsedTx {
		body := []byte("federation-approve:" + federationID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeFederationApprove,
			FederationApprove: &tx.FederationApprove{
				FederationID: federationID, ApproverOrgID: adminCreatedOrgID,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}
	federationRevoke := func(signer agentKey, federationID string) *tx.ParsedTx {
		body := []byte("federation-revoke:" + federationID)
		pubKey, sig, bodyHash, ts := signAgentProof(t, signer, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeFederationRevoke,
			FederationRevoke: &tx.FederationRevoke{
				FederationID: federationID, RevokerOrgID: localOrgID,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}

	deniedRegister := app.processOrgRegister(orgRegister(orgAdmin, selfElevationID), 11, time.Now())
	require.Equal(t, uint32(50), deniedRegister.Code)
	require.Contains(t, deniedRegister.Log, "global administrator")

	allowedRegister := app.processOrgRegister(orgRegister(globalAdmin, adminCreatedOrgID), 12, time.Now())
	require.Equal(t, uint32(0), allowedRegister.Code, allowedRegister.Log)

	deniedAdd := app.processOrgAddMember(orgAdd(orgAdmin, localOrgID), 13, time.Now())
	require.Equal(t, uint32(54), deniedAdd.Code)
	require.Contains(t, deniedAdd.Log, "global administrator")

	globalOverride := app.processOrgAddMember(orgAdd(globalAdmin, localOrgID), 14, time.Now())
	require.Equal(t, uint32(0), globalOverride.Code, globalOverride.Log)

	allowedAdd := app.processOrgAddMember(orgAdd(globalAdmin, adminCreatedOrgID), 14, time.Now())
	require.Equal(t, uint32(0), allowedAdd.Code, allowedAdd.Log)

	deniedClearance := app.processOrgSetClearance(
		orgSetClearance(orgAdmin, localOrgID, member.id, tx.ClearanceTopSecret),
		15,
		time.Now(),
	)
	require.Equal(t, uint32(59), deniedClearance.Code)
	require.Contains(t, deniedClearance.Log, "global administrator")

	allowedClearance := app.processOrgSetClearance(
		orgSetClearance(globalAdmin, localOrgID, member.id, tx.ClearanceTopSecret),
		16,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedClearance.Code, allowedClearance.Log)
	clearance, _, err := app.badgerStore.GetMemberClearance(localOrgID, member.id)
	require.NoError(t, err)
	require.Equal(t, uint8(tx.ClearanceTopSecret), clearance)

	deniedDeptRegister := app.processDeptRegister(
		deptRegister(orgAdmin, localOrgID, "voice-ops"),
		17,
		time.Now(),
	)
	require.Equal(t, uint32(71), deniedDeptRegister.Code)
	require.Contains(t, deniedDeptRegister.Log, "global administrator")

	allowedDeptRegister := app.processDeptRegister(
		deptRegister(globalAdmin, localOrgID, "voice-ops"),
		18,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedDeptRegister.Code, allowedDeptRegister.Log)

	deniedDeptAdd := app.processDeptAddMember(
		deptAdd(orgAdmin, localOrgID, "voice-ops"),
		19,
		time.Now(),
	)
	require.Equal(t, uint32(75), deniedDeptAdd.Code)
	require.Contains(t, deniedDeptAdd.Log, "global administrator")

	allowedDeptAdd := app.processDeptAddMember(
		deptAdd(globalAdmin, localOrgID, "voice-ops"),
		20,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedDeptAdd.Code, allowedDeptAdd.Log)

	invalidMax := app.processFederationPropose(
		federationPropose(globalAdmin, tx.ClearanceLevel(5), 0),
		21,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(61), invalidMax.Code)
	require.Contains(t, invalidMax.Log, "0-4")

	invalidExpiry := app.processFederationPropose(
		federationPropose(globalAdmin, tx.ClearanceInternal, -1),
		21,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(61), invalidExpiry.Code)
	require.Contains(t, invalidExpiry.Log, "expiry")

	deniedPropose := app.processFederationPropose(
		federationPropose(orgAdmin, tx.ClearanceInternal, 0),
		21,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(61), deniedPropose.Code)
	require.Contains(t, deniedPropose.Log, "global administrator")
	outsidePropose := app.processFederationPropose(
		federationPropose(outsideGlobalAdmin, tx.ClearanceInternal, 0),
		21,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(61), outsidePropose.Code)
	require.Contains(t, outsidePropose.Log, "not a member of proposer org")

	allowedPropose := app.processFederationPropose(
		federationPropose(globalAdmin, tx.ClearanceInternal, 0),
		22,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(0), allowedPropose.Code, allowedPropose.Log)
	federationID := string(allowedPropose.Data)
	require.NotEmpty(t, federationID)

	deniedApprove := app.processFederationApprove(
		federationApprove(orgAdmin, federationID),
		23,
		time.Now(),
	)
	require.Equal(t, uint32(64), deniedApprove.Code)
	require.Contains(t, deniedApprove.Log, "global administrator")
	outsideApprove := app.processFederationApprove(
		federationApprove(outsideGlobalAdmin, federationID),
		23,
		time.Now(),
	)
	require.Equal(t, uint32(64), outsideApprove.Code)
	require.Contains(t, outsideApprove.Log, "not a member of target org")

	allowedApprove := app.processFederationApprove(
		federationApprove(globalAdmin, federationID),
		24,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedApprove.Code, allowedApprove.Log)

	deniedRevoke := app.processFederationRevoke(
		federationRevoke(orgAdmin, federationID),
		25,
		time.Now(),
	)
	require.Equal(t, uint32(66), deniedRevoke.Code)
	require.Contains(t, deniedRevoke.Log, "global administrator")
	outsideRevoke := app.processFederationRevoke(
		federationRevoke(outsideGlobalAdmin, federationID),
		25,
		time.Now(),
	)
	require.Equal(t, uint32(66), outsideRevoke.Code)
	require.Contains(t, outsideRevoke.Log, "authorized member")
	_, _, _, _, status, err := app.badgerStore.GetFederation(federationID)
	require.NoError(t, err)
	require.Equal(t, "active", status)

	allowedRevoke := app.processFederationRevoke(
		federationRevoke(globalAdmin, federationID),
		26,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedRevoke.Code, allowedRevoke.Log)

	deniedDeptRemove := app.processDeptRemoveMember(
		deptRemove(orgAdmin, localOrgID, "voice-ops"),
		27,
		time.Now(),
	)
	require.Equal(t, uint32(79), deniedDeptRemove.Code)
	require.Contains(t, deniedDeptRemove.Log, "global administrator")
	_, _, err = app.badgerStore.GetDeptMemberClearance(localOrgID, "voice-ops", member.id)
	require.NoError(t, err)

	allowedDeptRemove := app.processDeptRemoveMember(
		deptRemove(globalAdmin, localOrgID, "voice-ops"),
		28,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedDeptRemove.Code, allowedDeptRemove.Log)

	deniedOrgRemove := app.processOrgRemoveMember(
		orgRemove(orgAdmin, localOrgID),
		29,
		time.Now(),
	)
	require.Equal(t, uint32(57), deniedOrgRemove.Code)
	require.Contains(t, deniedOrgRemove.Log, "global administrator")
	memberPresent, err := app.badgerStore.IsAgentInOrg(member.id, localOrgID)
	require.NoError(t, err)
	require.True(t, memberPresent)

	allowedOrgRemove := app.processOrgRemoveMember(
		orgRemove(globalAdmin, localOrgID),
		30,
		time.Now(),
	)
	require.Equal(t, uint32(0), allowedOrgRemove.Code, allowedOrgRemove.Log)
}

func TestAppV22FederationTermsValidationPreservesLegacyReplay(t *testing.T) {
	app := setupTestApp(t)
	app.appV22AppliedHeight = 15
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "global-org-admin", "admin")

	const proposerOrg = "proposer-org-000000000000"
	require.NoError(t, app.badgerStore.RegisterOrg(proposerOrg, "Proposer", "", admin.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember(proposerOrg, admin.id, 4, "admin", 1))

	proposal := func(target string, max tx.ClearanceLevel, expiry int64) *tx.ParsedTx {
		body := []byte("federation-terms:" + target)
		pubKey, sig, bodyHash, ts := signAgentProof(t, admin, body)
		return &tx.ParsedTx{
			Type: tx.TxTypeFederationPropose,
			FederationPropose: &tx.FederationPropose{
				ProposerOrgID: proposerOrg,
				TargetOrgID:   target,
				AllowedDomains: []string{
					"*",
				},
				MaxClearance: max,
				ExpiresAt:    expiry,
			},
			AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}

	legacyMax := app.processFederationPropose(
		proposal("legacy-max-target-00000", tx.ClearanceLevel(255), 0),
		10,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(0), legacyMax.Code, legacyMax.Log)
	legacyExpiry := app.processFederationPropose(
		proposal("legacy-expiry-target-00", tx.ClearanceInternal, -1),
		11,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(0), legacyExpiry.Code, legacyExpiry.Log)

	postMax := app.processFederationPropose(
		proposal("post-max-target-0000000", tx.ClearanceLevel(255), 0),
		16,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(61), postMax.Code, postMax.Log)
	require.Contains(t, postMax.Log, "0-4")
	postExpiry := app.processFederationPropose(
		proposal("post-expiry-target-0000", tx.ClearanceInternal, -1),
		17,
		time.Unix(100, 0),
	)
	require.Equal(t, uint32(61), postExpiry.Code, postExpiry.Log)
	require.Contains(t, postExpiry.Log, "expiry")
}

func TestAppV22FederationScopePolicyDoesNotRewriteHistoricalMemorySubmit(t *testing.T) {
	app := setupTestApp(t)
	// Simulate replaying a historical block after both later upgrades are
	// already recorded in state. At height 50, the legacy evaluator must still
	// ignore the empty federation scope exactly as the original binary did.
	app.appV18AppliedHeight = 100
	app.appV22AppliedHeight = 200
	writer := newAgentKey(t)
	owner := newAgentKey(t)
	registerAgent(t, app, writer, "historical-writer", "member")
	registerAgent(t, app, owner, "historical-owner", "member")

	const (
		writerOrg = "writer-org-00000000000000"
		ownerOrg  = "owner-org-000000000000000"
	)
	require.NoError(t, app.badgerStore.RegisterOrg(writerOrg, "Writer Org", "", writer.id, 1))
	require.NoError(t, app.badgerStore.RegisterOrg(ownerOrg, "Owner Org", "", owner.id, 1))
	require.NoError(t, app.badgerStore.AddOrgMember(writerOrg, writer.id, 4, "member", 1))
	require.NoError(t, app.badgerStore.AddOrgMember(ownerOrg, owner.id, 4, "member", 1))
	require.NoError(t, app.badgerStore.RegisterDomain("historical.research", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetFederation(
		"historical-federation", writerOrg, ownerOrg,
		nil, 4, 0, false, "active",
	))

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, writer, "historical.research", "legacy federation write"),
		50,
		time.Unix(50, 0),
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
}
