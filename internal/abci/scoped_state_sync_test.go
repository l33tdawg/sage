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
