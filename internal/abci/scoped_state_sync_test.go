package abci

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/scope"
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
	_, err = source.DB().Backup(backup, 0)
	require.NoError(t, err)
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
	require.NoError(t, bs.DB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("state:scope-content:malformed"), []byte("not-a-canonical-envelope"))
	}))
	state := &AppState{Height: 2}
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
	_, err = bs.DB().Backup(backup, 0)
	require.NoError(t, err)
	require.NoError(t, backup.Close())
	require.NoError(t, bs.CloseBadger())

	err = PrepareAppV20StateSyncBackup(context.Background(), backupPath, filepath.Join(root, "prepared"), 2, hash)
	require.ErrorContains(t, err, "verify staged scoped state")
}
