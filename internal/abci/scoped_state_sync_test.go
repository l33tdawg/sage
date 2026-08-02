package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func TestPrepareAppV20StateSyncBackupValidatesWithoutActivating(t *testing.T) {
	root := t.TempDir()
	sourcePath := filepath.Join(root, "source-badger")
	source, err := store.NewBadgerStore(sourcePath)
	require.NoError(t, err)
	require.NoError(t, source.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	seedTestGovernanceDelegationDomain(t, source)
	projection, err := store.NewSQLiteStore(context.Background(), filepath.Join(root, "source.db"))
	require.NoError(t, err)
	app, err := NewSageAppWithStores(source, projection, zerolog.Nop())
	require.NoError(t, err)
	closed := false
	t.Cleanup(func() {
		if !closed {
			_ = app.Close()
		}
	})

	validators := []agentKey{deterministicScopedAgent(1), deterministicScopedAgent(33), deterministicScopedAgent(65)}
	sort.Slice(validators, func(i, j int) bool { return validators[i].id < validators[j].id })
	for _, key := range validators {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: key.id, PublicKey: key.pub, Power: 10}))
	}
	require.NoError(t, source.RegisterDomain("research", validators[0].id, "", 1))
	require.NoError(t, source.SetAccessGrant("research", validators[0].id, 2, 0, validators[0].id))
	installScopeForValidators(t, app, "scope-state-sync", "research", 1, scope.StateActive, validators)
	submit := makeMemorySubmitTx(t, validators[0], "research", "network-safe staged recovery")
	result := app.processMemorySubmit(submit, 2, time.Unix(4_002, 0).UTC())
	require.Zero(t, result.Code, result.Log)
	memoryID := string(result.Data)
	for i := range validators {
		require.Zero(t, scopedVote(t, app, validators[i], memoryID, tx.VoteDecisionAccept, int64(3+i)))
	}

	app.state.Height = 5
	app.state.EpochNum = poe.EpochNumber(app.state.Height)
	appHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	app.state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, app.state))
	appHash, err = source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	app.state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, app.state))

	backupPath := filepath.Join(root, "badger.backup")
	backup, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), source.DB(), backup))
	require.NoError(t, backup.Close())
	require.NoError(t, app.Close())
	closed = true

	target := filepath.Join(root, "prepared-badger")
	require.NoError(t, PrepareAppV20StateSyncBackup(context.Background(), backupPath, target, 5, appHash))
	prepared, err := store.OpenBadgerStoreReadOnly(target)
	require.NoError(t, err)
	preparedContent, err := prepared.GetScopedContent(memoryID)
	require.NoError(t, err)
	require.NotNil(t, preparedContent)
	assert.Equal(t, submit.MemorySubmit.Content, preparedContent.Content)
	preparedHash, err := prepared.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	assert.Equal(t, appHash, preparedHash)
	require.NoError(t, prepared.CloseBadger())
	inspectedHeight, inspectedHash, err := InspectAppV20StateSyncDirectory(context.Background(), target)
	require.NoError(t, err)
	assert.Equal(t, uint64(5), inspectedHeight)
	assert.Equal(t, appHash, inspectedHash)
	require.NoError(t, VerifyActivatedAppV20StateSyncDirectory(context.Background(), target, 5, appHash))
	wrongActivatedHash := bytes.Repeat([]byte{0xee}, len(appHash))
	require.ErrorContains(t, VerifyActivatedAppV20StateSyncDirectory(context.Background(), target, 5, wrongActivatedHash), "trusted state")

	verifierHash, err := AppV20StateSyncBackupVerifier(5)(context.Background(), backupPath)
	require.NoError(t, err)
	assert.Equal(t, appHash, verifierHash)
	wrongHash := bytes.Repeat([]byte{0xff}, len(appHash))
	wrongTarget := filepath.Join(root, "wrong-target")
	require.ErrorContains(t, PrepareAppV20StateSyncBackup(context.Background(), backupPath, wrongTarget, 5, wrongHash), "trusted AppHash")
	_, err = os.Stat(wrongTarget)
	assert.ErrorIs(t, err, os.ErrNotExist, "failed preparation removes all staged state")
	require.ErrorContains(t, PrepareAppV20StateSyncBackup(context.Background(), backupPath, filepath.Join(root, "wrong-height"), 6, appHash), "height")
	require.ErrorContains(t, PrepareAppV20StateSyncBackup(context.Background(), backupPath, target, 5, appHash), "already exists")
}

func TestPrepareAppV20StateSyncBackupRejectsMalformedCanonicalScope(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "badger")
	bs, err := store.NewBadgerStore(path)
	require.NoError(t, err)
	require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	seedTestGovernanceDelegationDomain(t, bs)
	require.NoError(t, bs.DB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("state:scope-content:malformed"), []byte("not-a-canonical-envelope"))
	}))
	state := &AppState{Height: 2, EpochNum: poe.EpochNumber(2)}
	hash, err := bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = hash
	require.NoError(t, SaveState(bs, state))
	hash, err = bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = hash
	require.NoError(t, SaveState(bs, state))
	backupPath := filepath.Join(root, "badger.backup")
	backup, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), bs.DB(), backup))
	require.NoError(t, backup.Close())
	require.NoError(t, bs.CloseBadger())

	err = PrepareAppV20StateSyncBackup(context.Background(), backupPath, filepath.Join(root, "prepared"), 2, hash)
	require.ErrorContains(t, err, "verify staged scoped state")
}

func TestPrepareAppV20StateSyncBackupRejectsMalformedAppV23State(t *testing.T) {
	root := t.TempDir()
	path := filepath.Join(root, "badger")
	bs, err := store.NewBadgerStore(path)
	require.NoError(t, err)
	require.NoError(t, bs.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	require.NoError(t, bs.MarkUpgradeApplied(appV21UpgradeName, 21, 2))
	require.NoError(t, bs.MarkUpgradeApplied(appV22UpgradeName, 22, 3))
	require.NoError(t, bs.MarkUpgradeApplied(appV23UpgradeName, 23, 4))
	seedTestGovernanceDelegationDomain(t, bs)
	state := &AppState{Height: 5, EpochNum: poe.EpochNumber(5)}
	hash, err := bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = hash
	require.NoError(t, SaveState(bs, state))
	hash, err = bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = hash
	require.NoError(t, SaveState(bs, state))

	backupPath := filepath.Join(root, "badger.backup")
	backup, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), bs.DB(), backup))
	require.NoError(t, backup.Close())
	require.NoError(t, bs.CloseBadger())

	err = PrepareAppV20StateSyncBackup(
		context.Background(), backupPath, filepath.Join(root, "prepared"), 5, hash,
	)
	require.ErrorContains(t, err, "verify staged app-v23 state")
}

func TestPrepareAppV20StateSyncBackupRejectsMissingHistoricalRootCredential(t *testing.T) {
	rootDir := t.TempDir()
	source, err := store.NewBadgerStore(filepath.Join(rootDir, "badger"))
	require.NoError(t, err)

	root := deterministicScopedAgent(10)
	generationTwo := deterministicScopedAgent(42)
	generationThree := deterministicScopedAgent(74)
	require.NoError(t, source.RegisterAgentWithCapabilities(
		root.id, "CEREBRUM", store.AppV23RoleAdmin, "", "", "", 1, 0,
	))
	seedTestGovernanceDelegationDomain(t, source)
	require.NoError(t, source.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	require.NoError(t, source.MarkUpgradeApplied(appV21UpgradeName, 21, 2))
	require.NoError(t, source.MarkUpgradeApplied(appV22UpgradeName, 22, 3))
	require.NoError(t, source.EnsureAppV23Root("missing-root-history", 4))
	require.NoError(t, source.MarkUpgradeApplied(appV23UpgradeName, 23, 4))
	require.NoError(t, source.RotateAppV23RootCredential(1, generationTwo.id, 5))
	require.NoError(t, source.RotateAppV23RootCredential(2, generationThree.id, 6))
	require.NoError(t, source.ValidateAppV23State())

	// Model a hash-consistent but semantically incomplete trusted snapshot. The
	// current generation still says three, so dropping only the middle marker
	// must not be accepted merely because canonical backup/AppHash agree.
	require.NoError(t, source.DB().Update(func(txn *badger.Txn) error {
		return txn.Delete([]byte("appv23:root_credential:" + generationTwo.id))
	}))

	state := &AppState{Height: 6, EpochNum: poe.EpochNumber(6)}
	appHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, state))
	appHash, err = source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, state))

	backupPath := filepath.Join(rootDir, "app-v23-missing-history.backup")
	backup, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), source.DB(), backup))
	require.NoError(t, backup.Close())
	require.NoError(t, source.CloseBadger())

	err = PrepareAppV20StateSyncBackup(
		context.Background(), backupPath, filepath.Join(rootDir, "prepared"), 6, appHash,
	)
	require.ErrorContains(t, err, "verify staged app-v23 state")
	require.ErrorContains(t, err, "root credential history count")
}

func TestPrepareAppV20StateSyncBackupPreservesValidAppV26GroupAuthority(t *testing.T) {
	rootDir := t.TempDir()
	sourcePath := filepath.Join(rootDir, "source-badger")
	source, err := store.NewBadgerStore(sourcePath)
	require.NoError(t, err)

	root := deterministicScopedAgent(9)
	member := deterministicScopedAgent(41)
	manager := deterministicScopedAgent(73)
	retiring := deterministicScopedAgent(89)
	require.NoError(t, source.RegisterAgentWithCapabilities(
		root.id, "CEREBRUM", store.AppV23RoleAdmin, "", "", "", 1, 0,
	))
	require.NoError(t, source.RegisterAgentWithCapabilities(
		member.id, "member", store.AppV23RoleMember, "", "", "", 2, 0,
	))
	require.NoError(t, source.RegisterAgentWithCapabilities(
		manager.id, "manager", store.AppV23RoleMember, "", "", "", 3, 0,
	))
	require.NoError(t, source.RegisterAgentWithCapabilities(
		retiring.id, "retiring", store.AppV23RoleMember, "", "", "", 3, 0,
	))
	require.NoError(t, source.RegisterDomain("retiring-owned", retiring.id, "", 3))
	seedTestGovernanceDelegationDomain(t, source)
	require.NoError(t, source.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	require.NoError(t, source.MarkUpgradeApplied(appV21UpgradeName, 21, 2))
	require.NoError(t, source.MarkUpgradeApplied(appV22UpgradeName, 22, 3))
	require.NoError(t, source.EnsureAppV23Root("scope-state-sync-v23", 4))
	require.NoError(t, source.MarkUpgradeApplied(appV23UpgradeName, 23, 4))

	managerEnrollment, err := source.GetAppV23Enrollment(manager.id)
	require.NoError(t, err)
	managerRole, err := source.GetAppV23Role(manager.id)
	require.NoError(t, err)
	require.NoError(t, source.SetAppV23Policy(
		root.id, manager.id, store.AppV23RoleManager,
		managerEnrollment.Profile, store.AppV23ProfileStandard,
		3, 0, managerRole.Revision, managerEnrollment.Revision, 5,
	))
	groupMembers := []string{member.id, manager.id}
	sort.Strings(groupMembers)
	require.NoError(t, source.MutateAppV23AccessGroup(
		root.id, "state-sync-team", "State Sync Team", groupMembers, 0, false, 5,
	))
	rotatedRoot := deterministicScopedAgent(105)
	require.NoError(t, source.RotateAppV23RootCredential(1, rotatedRoot.id, 5))
	currentRoot := deterministicScopedAgent(137)
	require.NoError(t, source.RotateAppV23RootCredential(2, currentRoot.id, 6))
	require.NoError(t, source.ValidateAppV23State())
	require.NoError(t, source.MarkUpgradeApplied(appV24UpgradeName, 24, 5))
	require.NoError(t, source.MarkUpgradeApplied(appV25UpgradeName, 25, 6))
	require.NoError(t, source.MigrateAppV26AccessGroupAuthorities(7))
	require.NoError(t, source.MarkUpgradeApplied(appV26UpgradeName, 26, 7))
	require.NoError(t, source.MutateAppV26AccessGroup(
		currentRoot.id, "state-sync-team", "State Sync Team", groupMembers,
		store.AppV26GroupAuthorityReadWriteModify, 1, false, 8,
	))
	retiringEnrollment, err := source.GetAppV23Enrollment(retiring.id)
	require.NoError(t, err)
	retiringRole, err := source.GetAppV23Role(retiring.id)
	require.NoError(t, err)
	require.NoError(t, source.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
		AgentID: retiring.id, ApprovedBy: currentRoot.id,
		RootGeneration: 3, Profile: retiringEnrollment.Profile,
		HomeDomain: retiringEnrollment.HomeDomain, Clearance: retiringEnrollment.Clearance,
		Capabilities: retiringEnrollment.Capabilities, Active: false, UpdatedHeight: 8,
		RetireOwnedDomainsToRoot: true,
	}, store.AppV23RoleMember, retiringEnrollment.Revision, retiringRole.Revision))
	require.NoError(t, source.ValidateAppV26AccessGroupAuthorities())

	state := &AppState{Height: 8, EpochNum: poe.EpochNumber(8)}
	appHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, state))
	appHash, err = source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, state))

	backupPath := filepath.Join(rootDir, "app-v26.backup")
	backup, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), source.DB(), backup))
	require.NoError(t, backup.Close())
	require.NoError(t, source.CloseBadger())

	target := filepath.Join(rootDir, "prepared")
	require.NoError(t, PrepareAppV20StateSyncBackup(
		context.Background(), backupPath, target, 8, appHash,
	))
	prepared, err := store.OpenBadgerStoreReadOnly(target)
	require.NoError(t, err)
	require.NoError(t, prepared.ValidateAppV23State())
	rootState, err := prepared.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, root.id, rootState.PrincipalID)
	require.Equal(t, currentRoot.id, rootState.CredentialID)
	require.Equal(t, uint64(3), rootState.Generation)
	for _, id := range []string{root.id, rotatedRoot.id, currentRoot.id} {
		wasRoot, markerErr := prepared.IsAppV23RootCredential(id)
		require.NoError(t, markerErr)
		require.True(t, wasRoot, "state sync must preserve every Root credential generation")
	}
	preparedRole, err := prepared.GetAppV23Role(manager.id)
	require.NoError(t, err)
	require.Equal(t, store.AppV23RoleManager, preparedRole.Role)
	groups, err := prepared.ListAppV23AccessGroups()
	require.NoError(t, err)
	require.Len(t, groups, 1)
	require.Equal(t, "state-sync-team", groups[0].GroupID)
	require.Equal(t, groupMembers, groups[0].Members)
	require.Equal(t, store.AppV26GroupAuthorityReadWriteModify, groups[0].MemberAuthority)
	require.Equal(t, uint64(2), groups[0].Revision)
	domainOwner, err := prepared.GetDomainOwner("retiring-owned")
	require.NoError(t, err)
	require.Equal(t, root.id, domainOwner,
		"state sync must preserve Root's current authority over a retired agent's domains")
	ownerHistory, err := prepared.ListAppV26DomainOwnershipHistory("retiring-owned")
	require.NoError(t, err)
	require.Len(t, ownerHistory, 1)
	require.Equal(t, retiring.id, ownerHistory[0].PreviousOwner)
	require.Equal(t, root.id, ownerHistory[0].NewOwner)
	require.NoError(t, prepared.ValidateAppV26AccessGroupAuthorities())
	require.NoError(t, prepared.CloseBadger())

	height, inspectedHash, err := InspectAppV20StateSyncDirectory(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, uint64(8), height)
	require.Equal(t, appHash, inspectedHash)
}

func TestPrepareAppV20StateSyncBackupAcceptsExactAppV26ActivationHeight(t *testing.T) {
	rootDir := t.TempDir()
	sourcePath := filepath.Join(rootDir, "source-badger")
	source, err := store.NewBadgerStore(sourcePath)
	require.NoError(t, err)

	root := deterministicScopedAgent(211)
	require.NoError(t, source.RegisterAgentWithCapabilities(
		root.id, "CEREBRUM", store.AppV23RoleAdmin, "", "", "", 1, 0,
	))
	seedTestGovernanceDelegationDomain(t, source)
	require.NoError(t, source.MarkUpgradeApplied(appV20UpgradeName, 20, 1))
	require.NoError(t, source.MarkUpgradeApplied(appV21UpgradeName, 21, 2))
	require.NoError(t, source.MarkUpgradeApplied(appV22UpgradeName, 22, 3))
	require.NoError(t, source.EnsureAppV23Root("scope-state-sync-v26-h", 4))
	require.NoError(t, source.MarkUpgradeApplied(appV23UpgradeName, 23, 4))
	require.NoError(t, source.MarkUpgradeApplied(appV24UpgradeName, 24, 5))
	require.NoError(t, source.MarkUpgradeApplied(appV25UpgradeName, 25, 6))
	require.NoError(t, source.MigrateAppV26AccessGroupAuthorities(7))
	require.NoError(t, source.MarkUpgradeApplied(appV26UpgradeName, 26, 7))

	// A snapshot emitted by Commit(H) is already a complete app-v26 image. The
	// first transaction it will execute after activation is H+1, so rejecting
	// state.Height == AppliedHeight strands a valid release-boundary snapshot.
	state := &AppState{Height: 7, EpochNum: poe.EpochNumber(7)}
	appHash, err := source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, state))
	appHash, err = source.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	state.AppHash = append([]byte(nil), appHash...)
	require.NoError(t, SaveState(source, state))

	backupPath := filepath.Join(rootDir, "app-v26-at-h.backup")
	backup, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	require.NoError(t, err)
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), source.DB(), backup))
	require.NoError(t, backup.Close())
	require.NoError(t, source.CloseBadger())

	target := filepath.Join(rootDir, "prepared")
	require.NoError(t, PrepareAppV20StateSyncBackup(
		context.Background(), backupPath, target, 7, appHash,
	))
	height, inspectedHash, err := InspectAppV20StateSyncDirectory(context.Background(), target)
	require.NoError(t, err)
	require.Equal(t, uint64(7), height)
	require.Equal(t, appHash, inspectedHash)
}

func seedTestGovernanceDelegationDomain(t *testing.T, badgerStore *store.BadgerStore) {
	t.Helper()
	require.NoError(t, badgerStore.SetState(governanceDelegationDomainStateKey, bytes.Repeat([]byte{0x5a}, sha256.Size)))
	require.NoError(t, badgerStore.SetState(appV20LegacyResourceAuditStateKey, appV20LegacyResourceAuditValue))
}

func TestInspectStateSyncRecoveryDirectoryAcceptsCanonicalFreshStore(t *testing.T) {
	root := t.TempDir()
	freshPath := filepath.Join(root, "fresh")
	fresh, err := store.NewBadgerStore(freshPath)
	require.NoError(t, err)
	require.NoError(t, fresh.CloseBadger())
	height, appHash, err := InspectStateSyncRecoveryDirectory(context.Background(), freshPath)
	require.NoError(t, err)
	assert.Zero(t, height)
	assert.Empty(t, appHash)

	tamperedPath := filepath.Join(root, "tampered")
	tampered, err := store.NewBadgerStore(tamperedPath)
	require.NoError(t, err)
	tamperedHash := bytes.Repeat([]byte{0xaa}, sha256.Size)
	require.NoError(t, SaveState(tampered, &AppState{Height: 0, AppHash: tamperedHash}))
	require.NoError(t, tampered.CloseBadger())
	_, _, err = InspectStateSyncRecoveryDirectory(context.Background(), tamperedPath)
	require.ErrorContains(t, err, "non-empty AppHash")

	hiddenPath := filepath.Join(root, "hidden-fresh-state")
	hidden, err := store.NewBadgerStore(hiddenPath)
	require.NoError(t, err)
	require.NoError(t, hidden.DB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("memory:hidden"), []byte("unexpected consensus bytes"))
	}))
	require.NoError(t, hidden.CloseBadger())
	_, _, err = InspectStateSyncRecoveryDirectory(context.Background(), hiddenPath)
	require.ErrorContains(t, err, "contains consensus state")

	corruptHeightPath := filepath.Join(root, "corrupt-fresh-height")
	corruptHeight, err := store.NewBadgerStore(corruptHeightPath)
	require.NoError(t, err)
	require.NoError(t, corruptHeight.SetState(stateHeightKey, []byte{1}))
	require.NoError(t, corruptHeight.CloseBadger())
	_, _, err = InspectStateSyncRecoveryDirectory(context.Background(), corruptHeightPath)
	require.ErrorContains(t, err, "invalid height bookkeeping")

	legacyPath := filepath.Join(root, "pre-app-v20")
	legacy, err := store.NewBadgerStore(legacyPath)
	require.NoError(t, err)
	legacyHash := bytes.Repeat([]byte{0xbb}, sha256.Size)
	require.NoError(t, SaveState(legacy, &AppState{Height: 9, AppHash: legacyHash}))
	require.NoError(t, legacy.CloseBadger())
	height, appHash, err = InspectStateSyncRecoveryDirectory(context.Background(), legacyPath)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), height)
	assert.Equal(t, legacyHash, appHash, "pre-app-v20 quarantine is anchored by exact persisted Comet state")
}
