package main

import (
	"encoding/binary"
	"errors"
	"path/filepath"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRollbackPendingUpdateInvalidatesBeforeExecutableSwap(t *testing.T) {
	const execPath = "/opt/sage/sage-gui"
	t.Run("invalidation failure leaves rollback untouched", func(t *testing.T) {
		errInvalidate := errors.New("sidecar fsync failed")
		var calls []string
		rollbackCalled := false
		rolledBack, err := rollbackPendingUpdateAfterIndexInvalidationWith(
			execPath,
			func(path string) string {
				assert.Equal(t, execPath, path)
				calls = append(calls, "pending")
				return "v11.9.0"
			},
			func() error {
				calls = append(calls, "invalidate")
				return errInvalidate
			},
			func(string) (bool, error) {
				rollbackCalled = true
				calls = append(calls, "rollback")
				return true, nil
			},
		)
		assert.False(t, rolledBack)
		require.ErrorIs(t, err, errInvalidate)
		assert.False(t, rollbackCalled, "executable swap and pending-marker removal must not run")
		assert.Equal(t, []string{"pending", "invalidate"}, calls)
	})

	t.Run("success invalidates before rollback", func(t *testing.T) {
		errDurabilityWarning := errors.New("executable parent sync warning")
		var calls []string
		rolledBack, err := rollbackPendingUpdateAfterIndexInvalidationWith(
			execPath,
			func(string) string {
				calls = append(calls, "pending")
				return "v11.9.0"
			},
			func() error {
				calls = append(calls, "invalidate")
				return nil
			},
			func(path string) (bool, error) {
				assert.Equal(t, execPath, path)
				calls = append(calls, "rollback")
				return true, errDurabilityWarning
			},
		)
		assert.True(t, rolledBack)
		require.ErrorIs(t, err, errDurabilityWarning)
		assert.Equal(t, []string{"pending", "invalidate", "rollback"}, calls)
	})
}

func TestAutomaticBinaryRollbackInvalidatesConfiguredBadgerIndexProgress(t *testing.T) {
	home := t.TempDir()
	dataDir := filepath.Join(t.TempDir(), "custom data 状態")
	t.Setenv("SAGE_HOME", home)
	cfg := DefaultConfig(home)
	cfg.DataDir = dataDir
	require.NoError(t, SaveConfig(cfg))

	badgerDir := filepath.Join(dataDir, "badger")
	bs, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	// Model a pre-index rollback binary creating an authoritative membership
	// after the current binary had already recorded migration completion.
	role := "member"
	value := make([]byte, 1+4+len(role)+8)
	value[0] = 4
	binary.BigEndian.PutUint32(value[1:5], uint32(len(role))) // #nosec G115 -- fixed test string
	copy(value[5:], role)
	binary.BigEndian.PutUint64(value[5+len(role):], 1)
	require.NoError(t, bs.DB().Update(func(txn *badger.Txn) error {
		return txn.Set([]byte("org_member:rollback-org:rollback-agent"), value)
	}))
	require.NoError(t, bs.CloseBadger())

	require.NoError(t, invalidateIndexBackfillProgressForAutomaticRollback())
	reopened, err := store.NewBadgerStore(badgerDir)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	inOrg, err := reopened.IsAgentInOrg("rollback-agent", "rollback-org")
	require.NoError(t, err)
	assert.True(t, inOrg, "restored binary must not preserve a stale completion claim")
}
