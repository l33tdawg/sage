package statesync

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errSimulatedActivationCrash = errors.New("simulated process crash")

func writeActivationTestState(t *testing.T, path string, height uint64, appHash []byte) {
	t.Helper()
	require.NoError(t, os.Mkdir(path, 0o700))
	encoded := make([]byte, 8+len(appHash))
	binary.BigEndian.PutUint64(encoded, height)
	copy(encoded[8:], appHash)
	require.NoError(t, os.WriteFile(filepath.Join(path, "state"), encoded, 0o600))
}

func inspectActivationTestState(path string) (uint64, []byte, error) {
	encoded, err := os.ReadFile(filepath.Join(path, "state")) //nolint:gosec // test-owned path
	if err != nil {
		return 0, nil, err
	}
	if len(encoded) < 8 {
		return 0, nil, errors.New("test state is truncated")
	}
	return binary.BigEndian.Uint64(encoded), append([]byte(nil), encoded[8:]...), nil
}

func setupActivationDirectories(t *testing.T) (string, string, ActivationJournal, []byte, []byte) {
	t.Helper()
	root := t.TempDir()
	journal := activationTestJournal()
	journal.Phase = ActivationPrepared
	oldHash := sha256.Sum256([]byte("old-live"))
	writeActivationTestState(t, filepath.Join(root, journal.LiveName), 40, oldHash[:])
	writeActivationTestState(t, filepath.Join(root, journal.PreparedName), journal.Height, journal.AppHash)
	return root, filepath.Join(root, "activation.journal"), journal, oldHash[:], append([]byte(nil), journal.AppHash...)
}

func verifyActivationTestState(path string, height uint64, appHash []byte) error {
	actualHeight, actualHash, err := inspectActivationTestState(path)
	if err != nil {
		return err
	}
	if actualHeight != height || !bytes.Equal(actualHash, appHash) {
		return errors.New("unexpected activated state")
	}
	return nil
}

func TestActivatePreparedDirectoryWritesPendingJournal(t *testing.T) {
	root, journalPath, journal, _, newHash := setupActivationDirectories(t)
	require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))

	height, appHash, err := inspectActivationTestState(filepath.Join(root, journal.LiveName))
	require.NoError(t, err)
	assert.Equal(t, journal.Height, height)
	assert.Equal(t, newHash, appHash)
	assert.NoDirExists(t, filepath.Join(root, journal.PreparedName))
	assert.DirExists(t, filepath.Join(root, journal.QuarantineName))
	loaded, err := LoadActivationJournal(journalPath)
	require.NoError(t, err)
	assert.Equal(t, ActivationPendingComet, loaded.Phase)
}

func TestActivationDirectoryCrashRecoveryRestoresPersistedCometState(t *testing.T) {
	steps := []activationDirectoryStep{
		activationStepJournalPrepared,
		activationStepLiveQuarantined,
		activationStepPreparedLive,
		activationStepVerified,
		activationStepPendingComet,
	}
	for _, step := range steps {
		t.Run(string(step), func(t *testing.T) {
			root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
			err := activatePreparedDirectory(root, journalPath, journal, verifyActivationTestState, func(current activationDirectoryStep) error {
				if current == step {
					return errSimulatedActivationCrash
				}
				return nil
			})
			require.ErrorIs(t, err, errSimulatedActivationCrash)

			action, err := RecoverActivationDirectories(root, journalPath, 40, oldHash, inspectActivationTestState)
			require.NoError(t, err)
			if step == activationStepJournalPrepared {
				assert.Equal(t, RecoveryDiscardPrepared, action)
			} else {
				assert.Equal(t, RecoveryRestoreQuarantine, action)
			}
			height, appHash, err := inspectActivationTestState(filepath.Join(root, journal.LiveName))
			require.NoError(t, err)
			assert.Equal(t, uint64(40), height)
			assert.Equal(t, oldHash, appHash)
			assert.NoDirExists(t, filepath.Join(root, journal.PreparedName))
			assert.NoDirExists(t, filepath.Join(root, journal.QuarantineName))
			assert.NoFileExists(t, journalPath)
		})
	}
}

func TestRecoverActivationDirectoriesSealsCommittedState(t *testing.T) {
	root, journalPath, journal, _, newHash := setupActivationDirectories(t)
	require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))

	action, err := RecoverActivationDirectories(root, journalPath, journal.Height, newHash, inspectActivationTestState)
	require.NoError(t, err)
	assert.Equal(t, RecoveryKeepActivated, action)
	height, appHash, err := inspectActivationTestState(filepath.Join(root, journal.LiveName))
	require.NoError(t, err)
	assert.Equal(t, journal.Height, height)
	assert.Equal(t, newHash, appHash)
	assert.NoDirExists(t, filepath.Join(root, journal.QuarantineName))
	assert.NoFileExists(t, journalPath)
}

func TestRecoverActivationDirectoriesFinishesInterruptedRecovery(t *testing.T) {
	t.Run("quarantine restored before prepared cleanup", func(t *testing.T) {
		root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
		require.NoError(t, WriteActivationJournal(journalPath, journal))
		require.NoError(t, os.Rename(filepath.Join(root, journal.LiveName), filepath.Join(root, journal.QuarantineName)))
		require.NoError(t, os.Rename(filepath.Join(root, journal.QuarantineName), filepath.Join(root, journal.LiveName)))

		action, err := RecoverActivationDirectories(root, journalPath, 40, oldHash, inspectActivationTestState)
		require.NoError(t, err)
		assert.Equal(t, RecoveryDiscardPrepared, action)
		assert.NoDirExists(t, filepath.Join(root, journal.PreparedName))
		assert.NoFileExists(t, journalPath)
	})

	t.Run("sealed journal after quarantine cleanup", func(t *testing.T) {
		root, journalPath, journal, _, newHash := setupActivationDirectories(t)
		require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))
		journal.Phase = ActivationSealed
		require.NoError(t, WriteActivationJournal(journalPath, journal))
		require.NoError(t, os.RemoveAll(filepath.Join(root, journal.QuarantineName)))

		action, err := RecoverActivationDirectories(root, journalPath, journal.Height, newHash, inspectActivationTestState)
		require.NoError(t, err)
		assert.Equal(t, RecoveryKeepActivated, action)
		assert.NoFileExists(t, journalPath)
	})
}

func TestActivatePreparedDirectoryVerificationFailureRollsBack(t *testing.T) {
	root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
	err := ActivatePreparedDirectory(root, journalPath, journal, func(string, uint64, []byte) error {
		return errors.New("writable verification failed")
	})
	require.ErrorContains(t, err, "writable verification failed")
	height, appHash, inspectErr := inspectActivationTestState(filepath.Join(root, journal.LiveName))
	require.NoError(t, inspectErr)
	assert.Equal(t, uint64(40), height)
	assert.Equal(t, oldHash, appHash)
	assert.NoDirExists(t, filepath.Join(root, journal.PreparedName))
	assert.NoDirExists(t, filepath.Join(root, journal.QuarantineName))
	assert.NoFileExists(t, journalPath)
}

func TestActivationDirectoriesRejectUnsafeOrAmbiguousLayouts(t *testing.T) {
	t.Run("symlink prepared", func(t *testing.T) {
		root, journalPath, journal, _, _ := setupActivationDirectories(t)
		require.NoError(t, os.RemoveAll(filepath.Join(root, journal.PreparedName)))
		target := filepath.Join(root, "target")
		writeActivationTestState(t, target, journal.Height, journal.AppHash)
		require.NoError(t, os.Symlink(target, filepath.Join(root, journal.PreparedName)))

		err := ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState)
		assert.ErrorContains(t, err, "real directory")
		assert.NoFileExists(t, journalPath)
		assert.DirExists(t, filepath.Join(root, journal.LiveName))
	})

	t.Run("all three directories", func(t *testing.T) {
		root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
		require.NoError(t, WriteActivationJournal(journalPath, journal))
		writeActivationTestState(t, filepath.Join(root, journal.QuarantineName), 40, oldHash)

		_, err := RecoverActivationDirectories(root, journalPath, 40, oldHash, inspectActivationTestState)
		assert.ErrorContains(t, err, "ambiguous")
		assert.DirExists(t, filepath.Join(root, journal.LiveName))
		assert.DirExists(t, filepath.Join(root, journal.PreparedName))
		assert.DirExists(t, filepath.Join(root, journal.QuarantineName))
		assert.FileExists(t, journalPath)
	})

	t.Run("quarantine does not match Comet", func(t *testing.T) {
		root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
		require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))
		wrongHash := sha256.Sum256([]byte("wrong persisted state"))

		_, err := RecoverActivationDirectories(root, journalPath, 40, wrongHash[:], inspectActivationTestState)
		assert.ErrorContains(t, err, "does not match")
		assert.DirExists(t, filepath.Join(root, journal.LiveName))
		assert.DirExists(t, filepath.Join(root, journal.QuarantineName))
		assert.FileExists(t, journalPath)
		assert.NotEqual(t, oldHash, wrongHash[:])
	})
}
