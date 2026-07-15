package statesync

import (
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func activationTestJournal() ActivationJournal {
	appHash := sha256.Sum256([]byte("app"))
	metadataHash := sha256.Sum256([]byte("metadata"))
	return ActivationJournal{
		Phase: ActivationPendingComet, Height: 42,
		AppHash: appHash[:], MetadataHash: metadataHash[:],
		PreparedName: "state-sync-prepared-42", QuarantineName: "badger-old-42", LiveName: "badger",
	}
}

func TestActivationJournalCanonicalRoundTripAndAtomicWrite(t *testing.T) {
	journal := activationTestJournal()
	encoded, err := EncodeActivationJournal(journal)
	require.NoError(t, err)
	decoded, err := DecodeActivationJournal(encoded)
	require.NoError(t, err)
	assert.Equal(t, journal, decoded)

	path := filepath.Join(t.TempDir(), "activation.journal")
	require.NoError(t, WriteActivationJournal(path, journal))
	loaded, err := LoadActivationJournal(path)
	require.NoError(t, err)
	assert.Equal(t, journal, loaded)
	info, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, os.FileMode(0o600), info.Mode().Perm())

	journal.Phase = ActivationSealed
	require.NoError(t, WriteActivationJournal(path, journal))
	loaded, err = LoadActivationJournal(path)
	require.NoError(t, err)
	assert.Equal(t, ActivationSealed, loaded.Phase)
}

func TestActivationJournalRejectsMalformedAndUnsafeInput(t *testing.T) {
	journal := activationTestJournal()
	bad := journal
	bad.PreparedName = "../prepared"
	_, err := EncodeActivationJournal(bad)
	assert.ErrorContains(t, err, "unsafe")

	encoded, err := EncodeActivationJournal(journal)
	require.NoError(t, err)
	_, err = DecodeActivationJournal(append(encoded, 0))
	assert.ErrorContains(t, err, "trailing")
	unknown := append([]byte(nil), encoded...)
	unknown[len(activationJournalMagic)+1] = 99
	_, err = DecodeActivationJournal(unknown)
	assert.ErrorContains(t, err, "phase")

	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	require.NoError(t, os.WriteFile(target, encoded, 0o600))
	link := filepath.Join(dir, "journal-link")
	require.NoError(t, os.Symlink(target, link))
	_, err = LoadActivationJournal(link)
	assert.ErrorContains(t, err, "regular file")
}

func TestDecideActivationRecovery(t *testing.T) {
	journal := activationTestJournal()
	oldHash := sha256.Sum256([]byte("old"))
	newHash := append([]byte(nil), journal.AppHash...)
	nextHash := sha256.Sum256([]byte("next"))

	journal.Phase = ActivationPrepared
	action, err := DecideActivationRecovery(journal, 40, oldHash[:], 40, oldHash[:])
	require.NoError(t, err)
	assert.Equal(t, RecoveryDiscardPrepared, action)

	journal.Phase = ActivationPendingComet
	action, err = DecideActivationRecovery(journal, 40, oldHash[:], 42, newHash)
	require.NoError(t, err)
	assert.Equal(t, RecoveryRestoreQuarantine, action)

	action, err = DecideActivationRecovery(journal, 42, newHash, 42, newHash)
	require.NoError(t, err)
	assert.Equal(t, RecoveryKeepActivated, action)

	action, err = DecideActivationRecovery(journal, 43, nextHash[:], 43, nextHash[:])
	require.NoError(t, err)
	assert.Equal(t, RecoveryKeepActivated, action, "later block catch-up may seal the already activated snapshot")

	wrong := sha256.Sum256([]byte("wrong"))
	_, err = DecideActivationRecovery(journal, 42, wrong[:], 42, wrong[:])
	assert.ErrorContains(t, err, "journal")
	_, err = DecideActivationRecovery(journal, 42, newHash, 41, oldHash[:])
	assert.ErrorContains(t, err, "does not match")
}

func FuzzDecodeActivationJournal(f *testing.F) {
	encoded, err := EncodeActivationJournal(activationTestJournal())
	if err != nil {
		f.Fatal(err)
	}
	f.Add(encoded)
	f.Add([]byte("local snapshot manifest"))
	f.Fuzz(func(t *testing.T, input []byte) {
		journal, decodeErr := DecodeActivationJournal(input)
		if decodeErr != nil {
			return
		}
		canonical, encodeErr := EncodeActivationJournal(journal)
		require.NoError(t, encodeErr)
		assert.Equal(t, input, canonical)
	})
}
