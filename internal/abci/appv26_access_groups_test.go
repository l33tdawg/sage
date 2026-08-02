package abci

import (
	"context"
	"sort"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

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

func TestAppV26ActivationMigratesGroupsAndCrashReplayIsDeterministic(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, owner, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("v26-activation-scope", 10))
	members := []string{owner.id, member.id}
	sort.Strings(members)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		root.id, "legacy-team", "Legacy Team", members, 0, false, 12,
	))
	legacy, err := app.badgerStore.GetAppV23AccessGroup("legacy-team")
	require.NoError(t, err)
	require.Empty(t, legacy.MemberAuthority)

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
	request := &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: time.Unix(26_060, 0).UTC(),
	}
	first, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, first.ConsensusParamUpdates)
	require.Equal(t, uint64(26), first.ConsensusParamUpdates.Version.App)
	firstHash := append([]byte(nil), first.AppHash...)

	app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
	app.pendingAppV20Finalize = nil
	stillLegacy, err := app.badgerStore.GetAppV23AccessGroup("legacy-team")
	require.NoError(t, err)
	require.Empty(t, stillLegacy.MemberAuthority)
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
	require.NoError(t, app.badgerStore.MigrateAppV26AccessGroupAuthorities())
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
