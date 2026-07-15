package statesync

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
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

func TestActivationDirectoryCrashRecoveryRestoresFreshCometState(t *testing.T) {
	steps := []activationDirectoryStep{
		activationStepJournalPrepared,
		activationStepLiveQuarantined,
		activationStepPreparedLive,
		activationStepVerified,
		activationStepPendingComet,
	}
	for _, step := range steps {
		t.Run(string(step), func(t *testing.T) {
			root := t.TempDir()
			journal := activationTestJournal()
			journal.Phase = ActivationPrepared
			journalPath := filepath.Join(root, "activation.journal")
			writeActivationTestState(t, filepath.Join(root, journal.LiveName), 0, nil)
			writeActivationTestState(t, filepath.Join(root, journal.PreparedName), journal.Height, journal.AppHash)

			err := activatePreparedDirectory(root, journalPath, journal, verifyActivationTestState, func(current activationDirectoryStep) error {
				if current == step {
					return errSimulatedActivationCrash
				}
				return nil
			})
			require.ErrorIs(t, err, errSimulatedActivationCrash)
			action, err := RecoverActivationDirectories(root, journalPath, 0, nil, inspectActivationTestState)
			require.NoError(t, err)
			if step == activationStepJournalPrepared {
				assert.Equal(t, RecoveryDiscardPrepared, action)
			} else {
				assert.Equal(t, RecoveryRestoreQuarantine, action)
			}
			height, appHash, err := inspectActivationTestState(filepath.Join(root, journal.LiveName))
			require.NoError(t, err)
			assert.Zero(t, height)
			assert.Empty(t, appHash)
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

func TestSealActivatedDirectoryUsesAlreadyVerifiedLiveState(t *testing.T) {
	root, journalPath, journal, _, newHash := setupActivationDirectories(t)
	reopenCalls := 0
	require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, func(path string, height uint64, appHash []byte) error {
		reopenCalls++
		return verifyActivationTestState(path, height, appHash)
	}))
	require.Equal(t, 1, reopenCalls)

	require.NoError(t, SealActivatedDirectory(root, journalPath, journal.Height, newHash, journal.Height, newHash))
	assert.Equal(t, 1, reopenCalls, "runtime seal must not reopen the live database")
	assert.DirExists(t, filepath.Join(root, journal.LiveName))
	assert.NoDirExists(t, filepath.Join(root, journal.QuarantineName))
	assert.NoFileExists(t, journalPath)
}

func TestSealActivatedDirectoryRejectsUnverifiedOrAmbiguousState(t *testing.T) {
	t.Run("active and Comet differ", func(t *testing.T) {
		root, journalPath, journal, _, newHash := setupActivationDirectories(t)
		require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))
		wrongHash := sha256.Sum256([]byte("active differs"))
		err := SealActivatedDirectory(root, journalPath, journal.Height, newHash, journal.Height, wrongHash[:])
		assert.ErrorContains(t, err, "does not match")
		assert.DirExists(t, filepath.Join(root, journal.QuarantineName))
		assert.FileExists(t, journalPath)
	})

	t.Run("Comet below pending activation", func(t *testing.T) {
		root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
		require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))
		err := SealActivatedDirectory(root, journalPath, 40, oldHash, 40, oldHash)
		assert.ErrorContains(t, err, "below pending activation")
		assert.DirExists(t, filepath.Join(root, journal.QuarantineName))
		assert.FileExists(t, journalPath)
	})

	t.Run("layout lost quarantine", func(t *testing.T) {
		root, journalPath, journal, _, newHash := setupActivationDirectories(t)
		require.NoError(t, ActivatePreparedDirectory(root, journalPath, journal, verifyActivationTestState))
		require.NoError(t, os.RemoveAll(filepath.Join(root, journal.QuarantineName)))
		err := SealActivatedDirectory(root, journalPath, journal.Height, newHash, journal.Height, newHash)
		assert.ErrorContains(t, err, "layout is ambiguous")
		assert.FileExists(t, journalPath)
	})
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

func TestActivatePreparedDirectoryAuthorizationExpiryRestoresOldLiveState(t *testing.T) {
	for failAt := 1; failAt <= 5; failAt++ {
		t.Run(fmt.Sprintf("boundary-%d", failAt), func(t *testing.T) {
			root, journalPath, journal, oldHash, _ := setupActivationDirectories(t)
			calls := 0
			err := ActivatePreparedDirectoryAuthorized(root, journalPath, journal, verifyActivationTestState, func() error {
				calls++
				if calls == failAt {
					return errors.New("authorization expired")
				}
				return nil
			})
			require.ErrorContains(t, err, "authorization expired")
			height, appHash, inspectErr := inspectActivationTestState(filepath.Join(root, journal.LiveName))
			require.NoError(t, inspectErr)
			assert.Equal(t, uint64(40), height)
			assert.Equal(t, oldHash, appHash)
			assert.NoDirExists(t, filepath.Join(root, journal.QuarantineName))
			assert.NoFileExists(t, journalPath)
		})
	}
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
