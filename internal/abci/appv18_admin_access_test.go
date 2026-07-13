package abci

import (
	"context"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func makeAccessRevokeTx(t *testing.T, revoker agentKey, granteeID, domain string) *tx.ParsedTx {
	t.Helper()
	pubKey, sig, bodyHash, ts := signAgentProof(t, revoker, []byte(domain+granteeID))
	return &tx.ParsedTx{
		Type: tx.TxTypeAccessRevoke,
		AccessRevoke: &tx.AccessRevoke{
			RevokerID: revoker.id,
			GranteeID: granteeID,
			Domain:    domain,
			Reason:    "admin override test",
		},
		AgentPubKey: pubKey, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
}

func TestAppV18ForkGateAndVersionLockstep(t *testing.T) {
	app := setupTestApp(t)
	assert.False(t, app.postAppV18Fork(10))
	app.appV18AppliedHeight = 10
	assert.False(t, app.postAppV18Fork(10), "activation block remains pre-v18")
	assert.True(t, app.postAppV18Fork(11))
	assert.True(t, app.postAppV17Rules(11), "v18 subsumes lower additive rules")
	assert.Equal(t, uint64(18), app.currentAppVersion())
	assert.Equal(t, tx.CanonicalUpgradeName(18), appV18UpgradeName)
	assert.Equal(t, uint64(19), MaxSupportedAppVersion())
	app.state.Height = 10
	assert.True(t, app.IsAppV17ActiveForNextTx(), "v18 skip-ahead must enable v17 REST/CheckTx construction")

	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV18UpgradeName, 18, 42))
	app.appV18AppliedHeight = 0
	app.refreshAppV18Fork()
	assert.Equal(t, int64(42), app.appV18AppliedHeight, "restart refresh must restore the v18 gate")
}

func TestAppV18AdminAccessOverride_BoundaryAndOwnership(t *testing.T) {
	app := setupTestApp(t)
	app.appV18AppliedHeight = 100
	admin := newAgentKey(t)
	owner := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, admin, "genesis-admin", "admin")
	registerAgent(t, app, owner, "domain-owner", "member")
	registerAgent(t, app, grantee, "local-agent", "member")
	require.NoError(t, app.badgerStore.RegisterDomain("research.eurorack", owner.id, "", 1))

	// Activation block itself retains the old owner-only rule.
	atBoundary := app.processAccessGrant(makeAccessGrantTx(t, admin, grantee.id, "research.eurorack", 2), 100, time.Now())
	assert.Equal(t, uint32(34), atBoundary.Code, atBoundary.Log)

	// H+1: global admin may grant, but domain ownership stays untouched.
	afterTx := makeAccessGrantTx(t, admin, grantee.id, "research.eurorack", 2)
	afterTx.AccessGrant.ExpectedOwnerID = owner.id
	afterTx.AccessGrant.ExpectedOwnedDomain = "research.eurorack"
	after := app.processAccessGrant(afterTx, 101, time.Now())
	require.Equal(t, uint32(0), after.Code, after.Log)
	level, _, granter, err := app.badgerStore.GetAccessGrant("research.eurorack", grantee.id)
	require.NoError(t, err)
	assert.Equal(t, uint8(2), level)
	assert.Equal(t, admin.id, granter)
	domainOwner, err := app.badgerStore.GetDomainOwner("research.eurorack")
	require.NoError(t, err)
	assert.Equal(t, owner.id, domainOwner, "admin access override must not transfer ownership")
}

func TestAppV18AdminAccessOverride_NonAdminStillDenied(t *testing.T) {
	app := setupTestApp(t)
	app.appV18AppliedHeight = 100
	owner := newAgentKey(t)
	member := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, owner, "domain-owner", "member")
	registerAgent(t, app, member, "ordinary-member", "member")
	registerAgent(t, app, grantee, "local-agent", "member")
	require.NoError(t, app.badgerStore.RegisterDomain("research.dmt", owner.id, "", 1))

	result := app.processAccessGrant(makeAccessGrantTx(t, member, grantee.id, "research.dmt", 1), 101, time.Now())
	assert.Equal(t, uint32(34), result.Code, result.Log)
}

func TestAppV18AdminAccessOverride_Revoke(t *testing.T) {
	app := setupTestApp(t)
	app.appV18AppliedHeight = 100
	admin := newAgentKey(t)
	owner := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, admin, "genesis-admin", "admin")
	registerAgent(t, app, owner, "domain-owner", "member")
	registerAgent(t, app, grantee, "local-agent", "member")
	require.NoError(t, app.badgerStore.RegisterDomain("research.modular", owner.id, "", 1))
	require.NoError(t, app.badgerStore.SetAccessGrant("research.modular", grantee.id, 2, 0, owner.id))

	revokeTx := makeAccessRevokeTx(t, admin, grantee.id, "research.modular")
	revokeTx.AccessRevoke.ExpectedOwnerID = owner.id
	revokeTx.AccessRevoke.ExpectedOwnedDomain = "research.modular"
	result := app.processAccessRevoke(revokeTx, 101, time.Now())
	require.Equal(t, uint32(0), result.Code, result.Log)
	_, _, _, err := app.badgerStore.GetAccessGrant("research.modular", grantee.id)
	assert.Error(t, err)
	ownerAfter, err := app.badgerStore.GetDomainOwner("research.modular")
	require.NoError(t, err)
	assert.Equal(t, owner.id, ownerAfter)
}

func TestAppV18AdminGrant_UnownedDomainStillClaimsForAdmin(t *testing.T) {
	app := setupTestApp(t)
	app.appV18AppliedHeight = 100
	admin := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, admin, "genesis-admin", "admin")
	registerAgent(t, app, grantee, "local-agent", "member")

	result := app.processAccessGrant(makeAccessGrantTx(t, admin, grantee.id, "research.new", 1), 101, time.Now())
	require.Equal(t, uint32(0), result.Code, result.Log)
	owner, err := app.badgerStore.GetDomainOwner("research.new")
	require.NoError(t, err)
	assert.Equal(t, admin.id, owner)
	ownerLevel, _, ownerGranter, err := app.badgerStore.GetAccessGrant("research.new", admin.id)
	require.NoError(t, err)
	assert.Equal(t, uint8(2), ownerLevel)
	assert.Equal(t, admin.id, ownerGranter)
}

func TestAppV18WriteGrantBoundary_MemorySubmit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		height   int64
		grant    uint8
		wantCode uint32
	}{
		{name: "activation block keeps legacy read grant behavior", height: 100, grant: 1, wantCode: 0},
		{name: "h plus one rejects read-only grant", height: 101, grant: 1, wantCode: 11},
		{name: "h plus one accepts write grant", height: 101, grant: 2, wantCode: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV18AppliedHeight = 100
			owner := newAgentKey(t)
			writer := newAgentKey(t)
			registerAgent(t, app, owner, "owner", "member")
			registerAgent(t, app, writer, "writer", "member")
			require.NoError(t, app.badgerStore.RegisterDomain("research.write-boundary", owner.id, "", 1))
			require.NoError(t, app.badgerStore.SetAccessGrant("research.write-boundary", writer.id, tc.grant, 0, owner.id))

			result := app.processMemorySubmit(makeMemorySubmitTx(t, writer, "research.write-boundary", tc.name), tc.height, time.Now())
			assert.Equal(t, tc.wantCode, result.Code, result.Log)
		})
	}
}

func TestAppV18WriteGrantBoundary_CoCommit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		height   int64
		grant    uint8
		wantCode uint32
	}{
		{name: "activation block keeps legacy read grant behavior", height: 100, grant: 1, wantCode: 0},
		{name: "h plus one rejects read-only grant", height: 101, grant: 1, wantCode: 97},
		{name: "h plus one accepts write grant", height: 101, grant: 2, wantCode: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV15AppliedHeight = 1
			app.appV18AppliedHeight = 100
			owner := newAgentKey(t)
			writer := newAgentKey(t)
			registerAgent(t, app, owner, "owner", "member")
			registerAgent(t, app, writer, "writer", "member")
			require.NoError(t, app.badgerStore.RegisterDomain("research.cocommit-boundary", owner.id, "", 1))
			require.NoError(t, app.badgerStore.SetAccessGrant("research.cocommit-boundary", writer.id, tc.grant, 0, owner.id))
			env, _ := buildCoCommitEnvelope(t, writer, "research.cocommit-boundary", []byte(tc.name), "sage-b")

			result := app.processCoCommitSubmit(coCommitSubmitTx(t, writer, env), tc.height, time.Now())
			assert.Equal(t, tc.wantCode, result.Code, result.Log)
		})
	}
}

func TestAppV18RealActivationAndBoundary(t *testing.T) {
	app := setupTestApp(t)
	proposer := deterministicAgentKey(t)
	ptx := makeUpgradeProposeTx(t, proposer, appV18UpgradeName, 18, "feed", 0)
	require.Equal(t, uint32(0), app.processUpgradePropose(ptx, 11, time.Unix(0, 0)).Code)
	plan, err := app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)

	admin := newAgentKey(t)
	owner := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, admin, "genesis-admin", "admin")
	registerAgent(t, app, owner, "owner", "member")
	registerAgent(t, app, grantee, "grantee", "member")
	require.NoError(t, app.badgerStore.RegisterDomain("research.activation", owner.id, "", 1))

	before := app.processAccessGrant(makeAccessGrantTx(t, admin, grantee.id, "research.activation", 1), plan.ActivationHeight, time.Now())
	assert.Equal(t, uint32(34), before.Code, before.Log)
	resp, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: plan.ActivationHeight, Time: time.Now()})
	require.NoError(t, err)
	require.NotNil(t, resp.ConsensusParamUpdates)
	assert.Equal(t, uint64(18), resp.ConsensusParamUpdates.Version.App)
	assert.Equal(t, plan.ActivationHeight, app.appV18AppliedHeight)

	afterTx := makeAccessGrantTx(t, admin, grantee.id, "research.activation", 1)
	afterTx.AccessGrant.ExpectedOwnerID = owner.id
	afterTx.AccessGrant.ExpectedOwnedDomain = "research.activation"
	after := app.processAccessGrant(afterTx, plan.ActivationHeight+1, time.Now())
	assert.Equal(t, uint32(0), after.Code, after.Log)
}

func TestAppV18AdminOverrideConsensusCASRejectsStaleOwner(t *testing.T) {
	app := setupTestApp(t)
	app.appV18AppliedHeight = 100
	admin := newAgentKey(t)
	owner := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAgent(t, app, admin, "admin", "admin")
	registerAgent(t, app, owner, "owner", "member")
	registerAgent(t, app, grantee, "grantee", "member")
	require.NoError(t, app.badgerStore.RegisterDomain("research.cas", owner.id, "", 1))

	grantTx := makeAccessGrantTx(t, admin, grantee.id, "research.cas", 2)
	grantTx.AccessGrant.ExpectedOwnerID = owner.id
	grantTx.AccessGrant.ExpectedOwnedDomain = "research.cas"
	// The race can transfer ownership to the admin signer itself. Bindings must
	// still be checked even though the signer has become the ordinary owner.
	require.NoError(t, app.badgerStore.TransferDomain("research.cas", admin.id, "", 2))
	grantResult := app.processAccessGrant(grantTx, 101, time.Now())
	assert.Equal(t, uint32(34), grantResult.Code, grantResult.Log)
	_, _, _, err := app.badgerStore.GetAccessGrant("research.cas", grantee.id)
	assert.Error(t, err)

	require.NoError(t, app.badgerStore.SetAccessGrant("research.cas", grantee.id, 2, 0, admin.id))
	revokeTx := makeAccessRevokeTx(t, admin, grantee.id, "research.cas")
	revokeTx.AccessRevoke.ExpectedOwnerID = owner.id
	revokeTx.AccessRevoke.ExpectedOwnedDomain = "research.cas"
	revokeResult := app.processAccessRevoke(revokeTx, 101, time.Now())
	assert.Equal(t, uint32(38), revokeResult.Code, revokeResult.Log)
	_, _, _, err = app.badgerStore.GetAccessGrant("research.cas", grantee.id)
	assert.NoError(t, err, "stale revoke binding must not delete the grant")
}

func TestAppV18DescendantWriteUsesOwningAncestor_MemorySubmit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		grant    uint8
		wantCode uint32
	}{
		{name: "read-only denied", grant: 1, wantCode: 11},
		{name: "write grant accepted", grant: 2, wantCode: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV18AppliedHeight = 100
			owner := newAgentKey(t)
			writer := newAgentKey(t)
			registerAgent(t, app, owner, "owner", "member")
			registerAgent(t, app, writer, "writer", "member")
			require.NoError(t, app.badgerStore.RegisterDomain("research.parent", owner.id, "", 1))
			require.NoError(t, app.badgerStore.SetAccessGrant("research.parent", writer.id, tc.grant, 0, owner.id))

			result := app.processMemorySubmit(makeMemorySubmitTx(t, writer, "research.parent.child", tc.name), 101, time.Now())
			assert.Equal(t, tc.wantCode, result.Code, result.Log)
			_, err := app.badgerStore.GetDomainOwner("research.parent.child")
			assert.Error(t, err, "descendant must not be claimed around its ancestor owner")
		})
	}
}

func TestAppV18DescendantWriteUsesOwningAncestor_CoCommit(t *testing.T) {
	for _, tc := range []struct {
		name     string
		grant    uint8
		wantCode uint32
	}{
		{name: "read-only denied", grant: 1, wantCode: 97},
		{name: "write grant accepted", grant: 2, wantCode: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV15AppliedHeight = 1
			app.appV18AppliedHeight = 100
			owner := newAgentKey(t)
			writer := newAgentKey(t)
			registerAgent(t, app, owner, "owner", "member")
			registerAgent(t, app, writer, "writer", "member")
			require.NoError(t, app.badgerStore.RegisterDomain("research.parent", owner.id, "", 1))
			require.NoError(t, app.badgerStore.SetAccessGrant("research.parent", writer.id, tc.grant, 0, owner.id))
			env, _ := buildCoCommitEnvelope(t, writer, "research.parent.child", []byte(tc.name), "sage-b")

			result := app.processCoCommitSubmit(coCommitSubmitTx(t, writer, env), 101, time.Now())
			assert.Equal(t, tc.wantCode, result.Code, result.Log)
			_, err := app.badgerStore.GetDomainOwner("research.parent.child")
			assert.Error(t, err, "descendant must not be claimed around its ancestor owner")
		})
	}
}

func TestAppV18EffectiveOwnerCanWriteWithoutSelfGrant(t *testing.T) {
	t.Run("memory submit", func(t *testing.T) {
		app := setupTestApp(t)
		app.appV18AppliedHeight = 100
		owner := newAgentKey(t)
		registerAgent(t, app, owner, "owner", "member")
		require.NoError(t, app.badgerStore.RegisterDomain("research.owner", owner.id, "", 1))

		result := app.processMemorySubmit(makeMemorySubmitTx(t, owner, "research.owner.child", "owner write"), 101, time.Now())
		assert.Equal(t, uint32(0), result.Code, result.Log)
	})

	t.Run("co-commit", func(t *testing.T) {
		app := setupTestApp(t)
		app.appV15AppliedHeight = 1
		app.appV18AppliedHeight = 100
		owner := newAgentKey(t)
		registerAgent(t, app, owner, "owner", "member")
		require.NoError(t, app.badgerStore.RegisterDomain("research.owner", owner.id, "", 1))
		env, _ := buildCoCommitEnvelope(t, owner, "research.owner.child", []byte("owner write"), "sage-b")

		result := app.processCoCommitSubmit(coCommitSubmitTx(t, owner, env), 101, time.Now())
		assert.Equal(t, uint32(0), result.Code, result.Log)
	})
}

func TestAppV18WriteRequiresExplicitGrantEvenWithinOwnerOrg(t *testing.T) {
	for _, tc := range []struct {
		name     string
		coCommit bool
		wantCode uint32
	}{
		{name: "memory submit", wantCode: 11},
		{name: "co-commit", coCommit: true, wantCode: 97},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV15AppliedHeight = 1
			app.appV18AppliedHeight = 100
			owner := newAgentKey(t)
			reader := newAgentKey(t)
			registerAgent(t, app, owner, "owner", "member")
			registerAgent(t, app, reader, "reader", "member")
			require.NoError(t, app.badgerStore.RegisterOrg("org-research", "Research", "", owner.id, 1))
			require.NoError(t, app.badgerStore.AddOrgMember("org-research", owner.id, 4, "admin", 1))
			require.NoError(t, app.badgerStore.AddOrgMember("org-research", reader.id, 4, "member", 1))
			require.NoError(t, app.badgerStore.RegisterDomain("research.org", owner.id, "", 1))
			require.NoError(t, app.badgerStore.SetAccessGrant("research.org", reader.id, 1, 0, owner.id))

			if tc.coCommit {
				env, _ := buildCoCommitEnvelope(t, reader, "research.org", []byte(tc.name), "sage-b")
				result := app.processCoCommitSubmit(coCommitSubmitTx(t, reader, env), 101, time.Now())
				assert.Equal(t, tc.wantCode, result.Code, result.Log)
				return
			}
			result := app.processMemorySubmit(makeMemorySubmitTx(t, reader, "research.org", tc.name), 101, time.Now())
			assert.Equal(t, tc.wantCode, result.Code, result.Log)
		})
	}
}

func TestAppV18OverDepthDomainFailsClosed(t *testing.T) {
	deepDomain := "root." + strings.TrimSuffix(strings.Repeat("child.", 16), ".")
	require.Len(t, strings.Split(deepDomain, "."), 17)
	for _, tc := range []struct {
		name     string
		coCommit bool
		wantCode uint32
	}{
		{name: "memory submit", wantCode: 11},
		{name: "co-commit", coCommit: true, wantCode: 97},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			app.appV15AppliedHeight = 1
			app.appV18AppliedHeight = 100
			owner := newAgentKey(t)
			attacker := newAgentKey(t)
			registerAgent(t, app, owner, "owner", "member")
			registerAgent(t, app, attacker, "attacker", "member")
			require.NoError(t, app.badgerStore.RegisterDomain("root", owner.id, "", 1))

			if tc.coCommit {
				env, _ := buildCoCommitEnvelope(t, attacker, deepDomain, []byte(tc.name), "sage-b")
				result := app.processCoCommitSubmit(coCommitSubmitTx(t, attacker, env), 101, time.Now())
				assert.Equal(t, tc.wantCode, result.Code, result.Log)
			} else {
				result := app.processMemorySubmit(makeMemorySubmitTx(t, attacker, deepDomain, tc.name), 101, time.Now())
				assert.Equal(t, tc.wantCode, result.Code, result.Log)
			}
			_, err := app.badgerStore.GetDomainOwner(deepDomain)
			assert.Error(t, err, "invalid path must never be auto-claimed")
		})
	}
}
