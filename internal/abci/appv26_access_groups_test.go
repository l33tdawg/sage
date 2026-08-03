package abci

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/authzdenial"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV26ConstantsAndStrictForkBoundary(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(26), appV26UpgradeName)
	require.Equal(t, uint64(26), MaxSupportedAppVersion())
	app := setupTestApp(t)
	app.appV26AppliedHeight = 50
	require.False(t, app.postAppV26Fork(50))
	require.True(t, app.postAppV26Fork(51))
}

func TestAppV26GovernedAgentDisplayRenamePreservesImmutableIdentity(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	admin := newAgentKey(t)
	member := newAgentKey(t)
	target := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, admin, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
	require.NoError(t, app.badgerStore.RegisterAgentWithCapabilities(
		target.id, "registered-target", store.AppV23RoleMember,
		"immutable boot purpose", "codex", "", 4, 0,
	))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-agent-rename", 5))
	promoteAppV23TestAdmin(t, app, root, admin, 6)
	app.appV23AppliedHeight = 5
	app.appV26AppliedHeight = 10

	// The activation block retains historical self-only behavior.
	atActivation := app.processAgentUpdate(
		makeAgentUpdateTx(t, root, target.id, "too-early", "immutable boot purpose"),
		10, appV23BlockTime(),
	)
	require.NotZero(t, atActivation.Code)

	// At H+1 current Root may rename the governed target, but only while
	// carrying the target's exact current boot bio.
	renamed := app.processAgentUpdate(
		makeAgentUpdateTx(t, root, target.id, "operator-label", "immutable boot purpose"),
		11, appV23BlockTime(),
	)
	require.Zero(t, renamed.Code, renamed.Log)
	stored, err := app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	require.Equal(t, target.id, stored.AgentID)
	require.Equal(t, "registered-target", stored.RegisteredName)
	require.Equal(t, "immutable boot purpose", stored.BootBio)
	require.Equal(t, "operator-label", stored.Name)

	// A Member cannot use the same wire transaction to rename another agent.
	memberAttempt := app.processAgentUpdate(
		makeAgentUpdateTx(t, member, target.id, "member-forgery", "immutable boot purpose"),
		11, appV23BlockTime(),
	)
	require.NotZero(t, memberAttempt.Code)

	// Even Root cannot smuggle a purpose/bio rewrite through the display-name
	// control.
	bioRewrite := app.processAgentUpdate(
		makeAgentUpdateTx(t, root, target.id, "forged-purpose", "different bio"),
		11, appV23BlockTime(),
	)
	require.NotZero(t, bioRewrite.Code)
	stored, err = app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	require.Equal(t, "operator-label", stored.Name)
	require.Equal(t, "immutable boot purpose", stored.BootBio)
	for _, invalidName := range []string{"", " padded ", strings.Repeat("x", 129), string([]byte{0xff, 0xfe})} {
		invalidRename := app.processAgentUpdate(
			makeAgentUpdateTx(t, root, target.id, invalidName, "immutable boot purpose"),
			11, appV23BlockTime(),
		)
		require.NotZero(t, invalidRename.Code, "operator rename must reject %q", invalidName)
	}

	// Delegated Admin uses the central current-generation Root elevation gate;
	// this exercises processTx rather than bypassing that gate via the handler.
	adminRename := makeAgentUpdateTx(
		t, admin, target.id, "admin-label", "immutable boot purpose",
	)
	attachAppV23Elevation(
		t, adminRename, root, admin, "v26-agent-rename",
		"v26_agent_rename_admin_000001", 12,
	)
	signAppV23Outer(t, adminRename, admin, 1)
	adminResult := app.processTx(adminRename, 12, appV23BlockTime())
	require.Zero(t, adminResult.Code, adminResult.Log)
	stored, err = app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	require.Equal(t, "admin-label", stored.Name)
	require.Equal(t, "registered-target", stored.RegisteredName)
	require.Equal(t, "immutable boot purpose", stored.BootBio)

	// A post-v23 self-registration may still be pending review with no
	// enrollment record. It is nevertheless a real local on-chain agent and
	// Root may label it so the human can identify it before approval.
	pending := newAgentKey(t)
	require.NoError(t, app.badgerStore.RegisterAgentWithCapabilities(
		pending.id, "pending-registration", store.AppV23RoleMember,
		"pending boot purpose", "mcp", "", 7,
		store.DefaultSelfRegisteredAgentCapabilities,
	))
	pendingEnrollment, err := app.badgerStore.GetAppV23Enrollment(pending.id)
	require.NoError(t, err)
	require.Nil(t, pendingEnrollment)
	pendingRename := app.processAgentUpdate(
		makeAgentUpdateTx(t, root, pending.id, "Pending voice app", "pending boot purpose"),
		13, appV23BlockTime(),
	)
	require.Zero(t, pendingRename.Code, pendingRename.Log)
	pendingStored, err := app.badgerStore.GetRegisteredAgent(pending.id)
	require.NoError(t, err)
	require.Equal(t, "Pending voice app", pendingStored.Name)
	require.Equal(t, "pending-registration", pendingStored.RegisteredName)
	require.Equal(t, "pending boot purpose", pendingStored.BootBio)
}

func TestAppV26AgentDeactivationMovesCurrentDomainAuthorityToRootAtHPlusOne(t *testing.T) {
	for _, test := range []struct {
		name     string
		height   int64
		wantRoot bool
	}{
		{name: "activation height keeps historical semantics", height: 10},
		{name: "H plus one retires current ownership", height: 11, wantRoot: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			app := setupTestApp(t)
			rootKey := newAgentKey(t)
			ownerKey := newAgentKey(t)
			writerKey := newAgentKey(t)
			registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
			registerAppV23Agent(t, app, ownerKey, store.AppV23RoleMember, 2, 0)
			registerAppV23Agent(t, app, writerKey, store.AppV23RoleMember, 3, 0)
			require.NoError(t, app.badgerStore.RegisterDomain("retired-home", ownerKey.id, "", 4))
			require.NoError(t, app.badgerStore.RegisterDomain("retired-shared", ownerKey.id, "", 5))
			require.NoError(t, app.badgerStore.SetSharedDomain("retired-shared"))
			require.NoError(t, app.badgerStore.SetAccessGrant(
				"retired-shared", writerKey.id, 2, 0, ownerKey.id,
			))
			require.NoError(t, app.badgerStore.SetMemoryAuthor("retired-memory", ownerKey.id))
			require.NoError(t, app.badgerStore.SetMemoryDomain("retired-memory", "retired-shared"))
			require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-retirement-scope", 8))
			app.appV23AppliedHeight = 5
			app.appV26AppliedHeight = 10

			root, err := app.badgerStore.GetAppV23Root()
			require.NoError(t, err)
			enrollment, err := app.badgerStore.GetAppV23Enrollment(ownerKey.id)
			require.NoError(t, err)
			role, err := app.badgerStore.GetAppV23Role(ownerKey.id)
			require.NoError(t, err)
			approval := &tx.LocalAgentApprove{
				AgentID: ownerKey.id, Active: false, Role: store.AppV23RoleMember,
				Profile: enrollment.Profile, HomeDomain: enrollment.HomeDomain,
				Clearance: enrollment.Clearance, Capabilities: uint32(enrollment.Capabilities),
				Scope: root.Scope, ExpectedRevision: enrollment.Revision,
				ExpectedRoleRevision: role.Revision,
			}
			pub, sig, bodyHash, timestamp := signAgentProof(
				t, rootKey, []byte("app-v26-domain-owner-retirement-"+test.name),
			)
			result := app.processLocalAgentApprove(&tx.ParsedTx{
				Type: tx.TxTypeLocalAgentApprove, LocalAgentApprove: approval,
				AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash,
				AgentTimestamp: timestamp,
			}, test.height, appV23BlockTime())
			require.Zero(t, result.Code, result.Log)

			for _, domain := range []string{"retired-home", "retired-shared"} {
				owner, ownerErr := app.badgerStore.GetDomainOwner(domain)
				require.NoError(t, ownerErr)
				if test.wantRoot {
					require.Equal(t, root.PrincipalID, owner)
				} else {
					require.Equal(t, ownerKey.id, owner)
				}
			}
			level, _, granter, err := app.badgerStore.GetAccessGrant("retired-shared", writerKey.id)
			require.NoError(t, err)
			require.Equal(t, uint8(2), level)
			require.Equal(t, ownerKey.id, granter)
			author, err := app.badgerStore.GetMemoryAuthor("retired-memory")
			require.NoError(t, err)
			require.Equal(t, ownerKey.id, author)
			history, err := app.badgerStore.ListAppV26DomainOwnershipHistory("retired-shared")
			require.NoError(t, err)
			if test.wantRoot {
				require.Len(t, history, 1)
				require.Equal(t, ownerKey.id, history[0].PreviousOwner)
			} else {
				require.Empty(t, history)
			}
		})
	}
}

func TestAppV26ActivationMigratesGroupsAndCrashReplayIsDeterministic(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	member := newAgentKey(t)
	retiring := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, owner, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
	registerAppV23Agent(t, app, retiring, store.AppV23RoleMember, 4, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("activation-height-retiring", retiring.id, "", 5))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-activation-scope", 10))
	members := []string{owner.id, member.id}
	sort.Strings(members)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		root.id, "legacy-team", "Legacy Team", members, 0, false, 12,
	))
	legacy, err := app.badgerStore.GetAppV23AccessGroup("legacy-team")
	require.NoError(t, err)
	require.Empty(t, legacy.MemberAuthority)
	ownerEnrollmentBeforeRepair, err := app.badgerStore.GetAppV23Enrollment(owner.id)
	require.NoError(t, err)
	// Persist the exact invalid home shape emitted by the historical app-v25
	// batch bug so activation crash/replay covers both group authority and the
	// local-RBAC repair in one consensus transaction.
	require.NoError(t, app.badgerStore.SetState(
		"shared_domain:"+ownerEnrollmentBeforeRepair.HomeDomain, []byte{1},
	))
	require.Error(t, app.badgerStore.ValidateAppV23State())
	require.NoError(t, app.badgerStore.ValidateAppV23StateForPreV26Recovery())

	app.appV20AppliedHeight = 10
	app.appV21AppliedHeight = 20
	app.appV22AppliedHeight = 30
	app.appV23AppliedHeight = 35
	app.appV24AppliedHeight = 40
	app.appV25AppliedHeight = 50
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV25UpgradeName, 25, 50))
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV26UpgradeName, TargetAppVersion: 26,
		ActivationHeight: 60, ProposedAt: 59,
	}))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	retiringEnrollment, err := app.badgerStore.GetAppV23Enrollment(retiring.id)
	require.NoError(t, err)
	retiringRole, err := app.badgerStore.GetAppV23Role(retiring.id)
	require.NoError(t, err)
	approval := &tx.LocalAgentApprove{
		AgentID: retiring.id, Active: false, Role: store.AppV23RoleMember,
		Profile: retiringEnrollment.Profile, HomeDomain: retiringEnrollment.HomeDomain,
		Clearance:    retiringEnrollment.Clearance,
		Capabilities: uint32(retiringEnrollment.Capabilities), Scope: rootState.Scope,
		ExpectedRevision:     retiringEnrollment.Revision,
		ExpectedRoleRevision: retiringRole.Revision,
	}
	pub, sig, bodyHash, timestamp := signAgentProof(
		t, root, []byte("app-v26-activation-height-deactivation"),
	)
	deactivation := &tx.ParsedTx{
		Type: tx.TxTypeLocalAgentApprove, LocalAgentApprove: approval,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash,
		AgentTimestamp: timestamp,
	}
	signAppV23Outer(t, deactivation, root, 1)
	rawDeactivation, err := tx.EncodeTx(deactivation)
	require.NoError(t, err)
	request := &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(), Txs: [][]byte{rawDeactivation},
	}
	first, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, first.TxResults, 1)
	require.Zero(t, first.TxResults[0].Code, first.TxResults[0].Log)
	require.NotNil(t, first.ConsensusParamUpdates)
	require.Equal(t, uint64(26), first.ConsensusParamUpdates.Version.App)
	firstHash := append([]byte(nil), first.AppHash...)

	app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
	app.pendingAppV20Finalize = nil
	stillLegacy, err := app.badgerStore.GetAppV23AccessGroup("legacy-team")
	require.NoError(t, err)
	require.Empty(t, stillLegacy.MemberAuthority)
	require.Error(t, app.badgerStore.ValidateAppV23State(),
		"discarding the activation overlay must also discard the home repair")
	replayed, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, firstHash, replayed.AppHash)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, uint64(26), app.currentAppVersion())
	migrated, err := app.badgerStore.GetAppV23AccessGroup("legacy-team")
	require.NoError(t, err)
	require.Equal(t, store.AppV26GroupAuthorityRead, migrated.MemberAuthority)
	require.Equal(t, legacy.Revision, migrated.Revision)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
	ownerEnrollmentAfterRepair, err := app.badgerStore.GetAppV23Enrollment(owner.id)
	require.NoError(t, err)
	require.NotEqual(t, ownerEnrollmentBeforeRepair.HomeDomain, ownerEnrollmentAfterRepair.HomeDomain)
	retiredOwner, err := app.badgerStore.GetDomainOwner("activation-height-retiring")
	require.NoError(t, err)
	require.Equal(t, rootState.PrincipalID, retiredOwner)
	ownerHistory, err := app.badgerStore.ListAppV26DomainOwnershipHistory("activation-height-retiring")
	require.NoError(t, err)
	require.Len(t, ownerHistory, 1)
	require.Equal(t, "inactive_agent_reconciled", ownerHistory[0].Reason)
}

func TestAppV26PrerequisiteRejectsMissingPredecessorAndUnmigratedGroups(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, owner, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-prerequisite-scope", 10))
	members := []string{owner.id, member.id}
	sort.Strings(members)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		root.id, "legacy-team", "Legacy Team", members, 0, false, 12,
	))

	app.appV26AppliedHeight = 60
	err := app.validateAppV26Prerequisite()
	require.ErrorContains(t, err, "missing active")

	// An in-memory fork height alone is not a predecessor. The exact persisted
	// app-v25 record is part of the consensus prerequisite.
	app.appV25AppliedHeight = 50
	err = app.validateAppV26Prerequisite()
	require.ErrorContains(t, err, "invalid active")
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV25UpgradeName, 25, 50))

	// Once app-v26 is active, a historical empty authority cannot silently fall
	// back to role-derived semantics at restart or state sync.
	err = app.validateAppV26Prerequisite()
	require.ErrorContains(t, err, "invalid Access Group state")
	require.NoError(t, app.badgerStore.MigrateAppV26AccessGroupAuthorities(60))
	require.NoError(t, app.validateAppV26Prerequisite())
}

func TestAppV26ProcessAccessGroupRequiresAndPersistsAuthority(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, owner, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-process-scope", 10))
	app.appV23AppliedHeight = 5
	app.appV26AppliedHeight = 10
	members := []string{owner.id, member.id}
	sort.Strings(members)

	process := func(authority string, nonce byte) *abcitypes.ExecTxResult {
		mutation := &tx.AccessGroupMutate{
			GroupID: "team", Name: "Team", Members: members,
			MemberAuthority: authority,
		}
		pub, sig, bodyHash, ts := signAgentProof(t, root, []byte{nonce})
		return app.processAccessGroupMutateV23(&tx.ParsedTx{
			Type: tx.TxTypeAccessGroupMutate, AccessGroupMutate: mutation,
			AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}, 11, appV23BlockTime())
	}
	require.NotZero(t, process("", 1).Code, "post-v26 groups cannot omit authority")
	require.NotZero(t, process("owner", 2).Code, "unknown authority must fail closed")
	result := process(store.AppV26GroupAuthorityReadWrite, 3)
	require.Zero(t, result.Code, result.Log)
	group, err := app.badgerStore.GetAppV23AccessGroup("team")
	require.NoError(t, err)
	require.Equal(t, store.AppV26GroupAuthorityReadWrite, group.MemberAuthority)
}

func TestAppV25ReplayRejectsAppV26AuthorityField(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("v25-replay-scope", 10))
	app.appV23AppliedHeight = 5
	mutation := &tx.AccessGroupMutate{
		GroupID: "future", Name: "Future",
		MemberAuthority: store.AppV26GroupAuthorityRead,
	}
	pub, sig, bodyHash, ts := signAgentProof(t, root, []byte("future-field"))
	result := app.processAccessGroupMutateV23(&tx.ParsedTx{
		Type: tx.TxTypeAccessGroupMutate, AccessGroupMutate: mutation,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}, 6, appV23BlockTime())
	require.NotZero(t, result.Code)
}

func TestAppV26CheckTxBlocksExtendedWirePayloadUntilHPlusOne(t *testing.T) {
	app := setupTestApp(t)
	signer := newAgentKey(t)
	app.appV23AppliedHeight = 5
	app.state.Height = 10

	check := func(nonce uint64, mutation *tx.AccessGroupMutate) *abcitypes.ResponseCheckTx {
		parsed := &tx.ParsedTx{
			Type: tx.TxTypeAccessGroupMutate, Nonce: nonce,
			Timestamp:         time.Unix(26_000+int64(nonce), 0).UTC(),
			AgentPubKey:       append([]byte(nil), signer.pub...),
			AccessGroupMutate: mutation,
		}
		require.NoError(t, tx.SignTx(parsed, signer.priv))
		raw, err := tx.EncodeTx(parsed)
		require.NoError(t, err)
		response, err := app.CheckTx(
			context.Background(), &abcitypes.RequestCheckTx{Tx: raw},
		)
		require.NoError(t, err)
		return response
	}

	// While app-v26 is dormant, the historical wire form remains admissible
	// but the appended field must not enter a mixed v25/v26 mempool.
	legacy := check(1, &tx.AccessGroupMutate{GroupID: "legacy", Name: "Legacy"})
	require.Zero(t, legacy.Code, legacy.Log)
	extended := check(2, &tx.AccessGroupMutate{
		GroupID: "future", Name: "Future",
		MemberAuthority: store.AppV26GroupAuthorityRead,
	})
	require.Equal(t, uint32(10), extended.Code)
	require.Contains(t, extended.Log, "app-v26")

	// Once activation height H is committed, CheckTx evaluates H+1 and mirrors
	// the consensus handler: non-delete mutations require an exact tier, while
	// deletes must omit it.
	app.appV26AppliedHeight = 10
	missing := check(3, &tx.AccessGroupMutate{GroupID: "missing", Name: "Missing"})
	require.Equal(t, uint32(110), missing.Code)
	valid := check(4, &tx.AccessGroupMutate{
		GroupID: "valid", Name: "Valid",
		MemberAuthority: store.AppV26GroupAuthorityReadWrite,
	})
	require.Zero(t, valid.Code, valid.Log)
	deleteWithTier := check(5, &tx.AccessGroupMutate{
		GroupID: "valid", Delete: true,
		MemberAuthority: store.AppV26GroupAuthorityRead,
	})
	require.Equal(t, uint32(110), deleteWithTier.Code)
	deleteWithoutTier := check(6, &tx.AccessGroupMutate{
		GroupID: "valid", Delete: true,
	})
	require.Zero(t, deleteWithoutTier.Code, deleteWithoutTier.Log)
}

func TestAppV26ProcessProposalRejectsExtendedWirePayloadUntilHPlusOne(t *testing.T) {
	app := setupTestApp(t)
	signer := newAgentKey(t)
	app.appV23AppliedHeight = 5
	app.state.Height = 9

	encode := func(nonce uint64, mutation *tx.AccessGroupMutate) []byte {
		parsed := &tx.ParsedTx{
			Type: tx.TxTypeAccessGroupMutate, Nonce: nonce,
			Timestamp:         time.Unix(26_100+int64(nonce), 0).UTC(),
			AgentPubKey:       append([]byte(nil), signer.pub...),
			AccessGroupMutate: mutation,
		}
		require.NoError(t, tx.SignTx(parsed, signer.priv))
		raw, err := tx.EncodeTx(parsed)
		require.NoError(t, err)
		return raw
	}
	process := func(height int64, raw ...[]byte) abcitypes.ResponseProcessProposal_ProposalStatus {
		response, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
			Height: height, Txs: raw,
		})
		require.NoError(t, err)
		return response.Status
	}

	legacy := encode(1, &tx.AccessGroupMutate{GroupID: "legacy", Name: "Legacy"})
	extended := encode(2, &tx.AccessGroupMutate{
		GroupID: "future", Name: "Future",
		MemberAuthority: store.AppV26GroupAuthorityRead,
	})
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, process(10, legacy))
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, process(10, extended))
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, process(10, legacy, extended))

	// Activation height H still executes the historical app-v25 interpretation.
	// The extended wire form becomes proposal-admissible only at H+1.
	app.appV26AppliedHeight = 10
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, process(10, extended))
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, process(11, extended))
}

func TestAppV26DomainReassignOwnerBindingStartsStrictlyAtHPlusOne(t *testing.T) {
	app := setupTestApp(t)
	signer := newAgentKey(t)
	app.v8AppliedHeight = 1
	app.state.Height = 10

	encode := func(nonce uint64, expectedOwner string) []byte {
		parsed := &tx.ParsedTx{
			Type: tx.TxTypeDomainReassign, Nonce: nonce,
			Timestamp:   time.Unix(26_200+int64(nonce), 0).UTC(),
			AgentPubKey: append([]byte(nil), signer.pub...),
			DomainReassign: &tx.DomainReassign{
				Domain: "owner-binding", NewOwnerID: fmt.Sprintf("%064x", 2),
				ProposalID: fmt.Sprintf("%064x", 3), ExpectedOwnerID: expectedOwner,
			},
		}
		require.NoError(t, tx.SignTx(parsed, signer.priv))
		raw, err := tx.EncodeTx(parsed)
		require.NoError(t, err)
		return raw
	}
	check := func(raw []byte) *abcitypes.ResponseCheckTx {
		response, err := app.CheckTx(
			context.Background(), &abcitypes.RequestCheckTx{Tx: raw},
		)
		require.NoError(t, err)
		return response
	}
	process := func(height int64, raw []byte) abcitypes.ResponseProcessProposal_ProposalStatus {
		response, err := app.ProcessProposal(context.Background(), &abcitypes.RequestProcessProposal{
			Height: height, Txs: [][]byte{raw},
		})
		require.NoError(t, err)
		return response.Status
	}

	legacy := encode(1, "")
	extended := encode(2, fmt.Sprintf("%064x", 1))
	require.Zero(t, check(legacy).Code)
	require.Equal(t, uint32(10), check(extended).Code)
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, process(10, legacy))
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, process(10, extended))

	// The activation block H remains app-v25. Once H commits, CheckTx targets
	// H+1 and proposal validation accepts the signed CAS extension there.
	app.appV26AppliedHeight = 10
	require.Zero(t, check(extended).Code)
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, process(10, extended))
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, process(11, extended))
}

// TestAppV26SignedDataPlaneAuthorityTiers proves that the persisted app-v26
// field is not merely accepted by the group mutation handler: each tier reaches
// the concrete signed read, write, and modify transaction paths at H+1. This is
// deliberately separate from the store-level policy matrix so a future drift
// in processTx/appV23DomainDecision cannot turn the UI selector into a lie.
func TestAppV26SignedDataPlaneAuthorityTiers(t *testing.T) {
	tiers := []struct {
		name      string
		authority string
		write     bool
		modify    bool
	}{
		{name: "read", authority: store.AppV26GroupAuthorityRead},
		{name: "read-write", authority: store.AppV26GroupAuthorityReadWrite, write: true},
		{name: "read-write-modify", authority: store.AppV26GroupAuthorityReadWriteModify, write: true, modify: true},
	}
	for tierIndex, tier := range tiers {
		t.Run(tier.name, func(t *testing.T) {
			app := setupTestApp(t)
			root := newAgentKey(t)
			owner := newAgentKey(t)
			member := newAgentKey(t)
			registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
			registerAppV23Agent(t, app, owner, store.AppV23RoleMember, 2, 0)
			registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
			require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-signed-tier-"+tier.name, 4))

			ownerEnrollment, err := app.badgerStore.GetAppV23Enrollment(owner.id)
			require.NoError(t, err)
			members := []string{owner.id, member.id}
			sort.Strings(members)
			require.NoError(t, app.badgerStore.MutateAppV26AccessGroup(
				root.id, "signed-tier", "Signed tier", members,
				tier.authority, 0, false, 9,
			))
			app.appV21AppliedHeight = 5
			app.appV23AppliedHeight = 6
			app.appV26AppliedHeight = 10

			seed := func(memoryID string, status memory.MemoryStatus) {
				hash := sha256.Sum256([]byte(memoryID))
				require.NoError(t, app.badgerStore.SetMemoryHash(memoryID, hash[:], string(status)))
				require.NoError(t, app.badgerStore.SetMemoryDomain(memoryID, ownerEnrollment.HomeDomain))
				require.NoError(t, app.badgerStore.SetMemoryClassification(memoryID, 0))
				require.NoError(t, app.badgerStore.SetMemoryAuthor(memoryID, owner.id))
				require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(memoryID, owner.id))
			}

			readID := fmt.Sprintf("v26-signed-read-%d", tierIndex)
			seed(readID, memory.StatusProposed)
			readResult := appV23MatrixProcessRaw(
				t, app,
				makeMemoryCorroborateTx(t, member, readID, "app-v26 group read"),
				member, 1, 11,
			)
			require.Zero(t, readResult.Code, readResult.Log)

			writeTx := makeMemorySubmitTx(
				t, member, ownerEnrollment.HomeDomain, "app-v26 group write",
			)
			writeTx.MemorySubmit.MemoryID = fmt.Sprintf("v26-signed-write-%d", tierIndex)
			writeTx.MemorySubmit.Classification = tx.ClearancePublic
			writeResult := appV23MatrixProcessRaw(t, app, writeTx, member, 2, 11)
			if tier.write {
				require.Zero(t, writeResult.Code, writeResult.Log)
			} else {
				require.Equal(t, appV23Denial(authzdenial.CodeMissingWriteGrant), writeResult)
			}

			modifyID := fmt.Sprintf("v26-signed-modify-%d", tierIndex)
			seed(modifyID, memory.StatusCommitted)
			modifyResult := appV23MatrixProcessRaw(
				t, app,
				makeMemoryChallengeTx(t, member, modifyID, "app-v26 group modify"),
				member, 3, 11,
			)
			if tier.modify {
				require.Zero(t, modifyResult.Code, modifyResult.Log)
			} else {
				require.Equal(t, appV23Denial(authzdenial.CodeMissingWriteGrant), modifyResult)
			}

			// Group authority never narrows the actual owner, even at the Read
			// tier. Prove all three owner verbs at the consensus policy boundary.
			for _, verb := range []store.AppV23DomainVerb{
				store.AppV23VerbRead, store.AppV23VerbWrite, store.AppV23VerbModify,
			} {
				allowed, denial, decisionErr := app.appV23DomainDecision(
					&tx.ParsedTx{}, owner.id, ownerEnrollment.HomeDomain,
					verb, 11, appV23BlockTime(),
				)
				require.NoError(t, decisionErr)
				require.True(t, allowed, "owner verb %d", verb)
				require.Empty(t, denial)
			}
		})
	}
}
