package abci

import (
	"context"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestAppV31StrictBoundaryAndPredecessor(t *testing.T) {
	app := setupTestApp(t)
	app.state.Height = 101
	require.Equal(t, uint64(31), MaxCompiledAppVersion())
	require.Equal(t, tx.CanonicalUpgradeName(31), appV31UpgradeName)
	require.False(t, app.postAppV31Fork(101))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV31UpgradeName, 31, 100))
	require.NoError(t, app.refreshAppV31Fork())
	require.Error(t, app.validateAppV31Prerequisite())
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 99))
	app.appV30AppliedHeight = 99
	require.NoError(t, app.validateAppV31Prerequisite())
	require.False(t, app.postAppV31Fork(100))
	require.True(t, app.postAppV31Fork(101))
	require.Equal(t, uint64(31), app.currentAppVersion())
}

func TestAppV31AssignmentUsesExistingSignedAtomicPolicyPath(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "dormant", true: "active"}[active], func(t *testing.T) {
			app := setupTestApp(t)
			root, reader := newAgentKey(t), newAgentKey(t)
			registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
			registerAppV23Agent(t, app, reader, "observer", 2, 0)
			require.NoError(t, app.badgerStore.EnsureAppV23Root("inspection-scope", 10))
			app.appV23AppliedHeight = 10
			if active {
				require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 19))
				require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV31UpgradeName, 31, 20))
				app.appV30AppliedHeight = 19
				app.appV31AppliedHeight = 20
			}
			enrollment, err := app.badgerStore.GetAppV23Enrollment(reader.id)
			require.NoError(t, err)
			role, err := app.badgerStore.GetAppV23Role(reader.id)
			require.NoError(t, err)
			change := &tx.AgentRoleChange{AgentID: reader.id, ExpectedRevision: role.Revision, EnrollmentRevision: enrollment.Revision, Role: role.Role, ExpectedProfile: enrollment.Profile, Profile: enrollment.Profile, Clearance: enrollment.Clearance, Capabilities: uint32(enrollment.Capabilities | store.AgentCapabilityInspectDirectGrants)}
			pub, sig, bodyHash, ts := signAgentProof(t, root, []byte("explicit grant inspection"))
			parsed := &tx.ParsedTx{Type: tx.TxTypeAgentRoleChange, AgentRoleChange: change, AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts}
			signAppV23Outer(t, parsed, root, 1)
			result := app.processTx(parsed, 21, appV23BlockTime())
			if active {
				require.Zero(t, result.Code, result.Log)
			} else {
				require.NotZero(t, result.Code)
			}
			after, err := app.badgerStore.GetAppV23Enrollment(reader.id)
			require.NoError(t, err)
			require.Equal(t, active, after.Capabilities.Has(store.AgentCapabilityInspectDirectGrants))
			require.NoError(t, app.badgerStore.ValidateAppV23State())
		})
	}
}

func TestAppV31FreshGenesisInspectionRequiresExplicitVersionedConsent(t *testing.T) {
	for _, version := range []uint64{30, 31} {
		app := setupTestApp(t)
		manifest, root, agent := freshGenesisFixture(t, version, 1, true)
		manifest.Agent.Capabilities |= uint32(store.AgentCapabilityInspectDirectGrants)
		request := freshGenesisRequest(t, "fresh-inspection", manifest, root, agent)
		_, err := app.InitChain(context.Background(), request)
		if version == 30 {
			require.Error(t, err)
			continue
		}
		require.NoError(t, err)
		require.True(t, app.IsAppV31ActiveForNextTx())
		enrollment, err := app.badgerStore.GetAppV23Enrollment(agent.id)
		require.NoError(t, err)
		require.Equal(t, store.AgentCapabilities(15)|store.AgentCapabilityInspectDirectGrants, enrollment.Capabilities)
		require.NoError(t, app.badgerStore.ValidateAppV23State())
		require.NoError(t, app.badgerStore.ValidateFreshGenesisInitialState())
		require.NoError(t, app.stopMemoryTaintMirrorWorker())
		reopened, err := NewSageAppWithStores(app.badgerStore, app.offchainStore, zerolog.Nop())
		require.NoError(t, err)
		app.memoryTaintMirrorWorker = reopened.memoryTaintMirrorWorker
		require.True(t, reopened.IsAppV31ActiveForNextTx())
		restored, err := reopened.badgerStore.GetAppV23Enrollment(agent.id)
		require.NoError(t, err)
		require.Equal(t, enrollment.Capabilities, restored.Capabilities)
		_, err = store.ReconcileAppV23AgentProjections(context.Background(), reopened.offchainStore, reopened.badgerStore)
		require.NoError(t, err)
		projected, err := reopened.offchainStore.GetAgent(context.Background(), agent.id)
		require.NoError(t, err)
		require.Equal(t, enrollment.Capabilities, projected.Capabilities)
	}
}

func TestAppV31ActivationBlockKeepsHistoricalPolicyRefusal(t *testing.T) {
	app := setupTestApp(t)
	app.appV23AppliedHeight = 10
	app.appV31AppliedHeight = 20
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV30UpgradeName, 30, 19))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV31UpgradeName, 31, 20))
	parsed := &tx.ParsedTx{Type: tx.TxTypeAgentRoleChange, AgentRoleChange: &tx.AgentRoleChange{
		Role: store.AppV23RoleMember, Profile: store.AppV23ProfileStandard, Clearance: 1, Capabilities: uint32(store.AgentCapabilityInspectDirectGrants),
	}}
	result := app.processAgentRoleChangeV23(parsed, 20, appV23BlockTime())
	require.Equal(t, uint32(111), result.Code)
	require.Equal(t, "invalid app-v23 role change payload", result.Log)
	result = app.processAgentRoleChangeV23(parsed, 21, appV23BlockTime())
	require.Equal(t, "agent identity verification failed", result.Log)
}
