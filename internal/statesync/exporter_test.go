package statesync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
)

func narrowAppHashVerifier(t *testing.T) BackupVerifier {
	t.Helper()
	return func(ctx context.Context, backupPath string) ([]byte, error) {
		restoredPath := filepath.Join(t.TempDir(), "verify-badger")
		db, err := badger.Open(badger.DefaultOptions(restoredPath).WithLogger(nil))
		if err != nil {
			return nil, err
		}
		file, err := os.Open(backupPath) //nolint:gosec // exporter-owned staging path
		if err != nil {
			_ = db.Close()
			return nil, err
		}
		if loadErr := RestoreCanonicalState(ctx, file, db); loadErr != nil {
			_ = file.Close()
			_ = db.Close()
			return nil, loadErr
		}
		if closeFileErr := file.Close(); closeFileErr != nil {
			_ = db.Close()
			return nil, closeFileErr
		}
		if closeDBErr := db.Close(); closeDBErr != nil {
			return nil, closeDBErr
		}
		restored, err := store.NewBadgerStore(restoredPath)
		if err != nil {
			return nil, err
		}
		hash, hashErr := restored.ComputeAppHashExcludingBookkeeping()
		closeErr := restored.CloseBadger()
		if hashErr != nil {
			return nil, hashErr
		}
		return hash, closeErr
	}
}

func TestExportCatalogAndLoadChunkContainOnlyConsensusState(t *testing.T) {
	badgerPath := filepath.Join(t.TempDir(), "live-badger")
	live, err := store.NewBadgerStore(badgerPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.CloseBadger() })
	require.NoError(t, live.SetState("scope:export", bytes.Repeat([]byte("roster"), 20_000)))
	require.NoError(t, live.SetState("scope-content:export", bytes.Repeat([]byte("selected"), 20_000)))
	appHash, err := live.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)

	root := filepath.Join(t.TempDir(), "network-snapshots")
	exported, err := Export(context.Background(), live.DB(), root, 88, appHash, MinChunkSize, narrowAppHashVerifier(t))
	require.NoError(t, err)
	assert.Equal(t, uint64(88), exported.Metadata.Height)
	assert.Equal(t, appHash, exported.Metadata.AppHash)
	entries, err := os.ReadDir(exported.Dir)
	require.NoError(t, err)
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	assert.ElementsMatch(t, []string{metadataFilename, chunksDirname, snapshotOwnerFilename}, names,
		"network export must contain no SQLite, CometBFT db/config, keys, vault, or binary")
	require.NoError(t, requireSnapshotOwnerMarker(filepath.Join(exported.Dir, snapshotOwnerFilename)))

	opened, err := OpenSnapshot(exported.Dir)
	require.NoError(t, err)
	assert.Equal(t, exported.Hash, opened.Hash)
	var backup bytes.Buffer
	for index := range opened.Metadata.ChunkHashes {
		chunk, loadChunkErr := opened.LoadChunk(uint32(index))
		require.NoError(t, loadChunkErr)
		_, _ = backup.Write(chunk)
	}
	assert.Equal(t, opened.Metadata.BackupSize, uint64(backup.Len()))
	backupHash := sha256.Sum256(backup.Bytes())
	assert.Equal(t, opened.Metadata.BackupHash, backupHash[:])

	catalog, err := ListSnapshots(root)
	require.NoError(t, err)
	require.Len(t, catalog, 1)
	assert.Equal(t, opened.Hash, catalog[0].Hash)
	require.NoError(t, os.WriteFile(filepath.Join(exported.Dir, "node_key.json"), []byte("must never travel"), 0o600))
	_, err = OpenSnapshot(exported.Dir)
	require.ErrorContains(t, err, "unexpected entries", "network catalogs reject private-file smuggling")
}

func TestExportRejectsWrongAppHashAndLoadDetectsTampering(t *testing.T) {
	live, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "live-badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.CloseBadger() })
	require.NoError(t, live.SetState("scope:export", []byte("state")))
	appHash, err := live.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	wrong := sha256.Sum256([]byte("wrong"))
	root := filepath.Join(t.TempDir(), "network-snapshots")
	_, err = Export(context.Background(), live.DB(), root, 1, wrong[:], MinChunkSize, narrowAppHashVerifier(t))
	require.ErrorContains(t, err, "does not match")
	catalog, err := ListSnapshots(root)
	require.NoError(t, err)
	assert.Empty(t, catalog, "failed export leaves no published snapshot")

	exported, err := Export(context.Background(), live.DB(), root, 2, appHash, MinChunkSize, narrowAppHashVerifier(t))
	require.NoError(t, err)
	chunkPath := filepath.Join(exported.Dir, chunksDirname, chunkFilename(0))
	chunk, err := os.ReadFile(chunkPath) //nolint:gosec // test-owned path
	require.NoError(t, err)
	chunk[0] ^= 0xff
	require.NoError(t, os.WriteFile(chunkPath, chunk, 0o600))
	_, err = exported.LoadChunk(0)
	require.ErrorContains(t, err, "hash changed")
	_, err = ListSnapshots(root)
	require.NoError(t, err, "catalog checks type/size; LoadChunk performs just-in-time content verification")
}

func TestOpenSnapshotRejectsSymlinkChunk(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	metadata, encoded, _, chunks, _ := stateSyncFixture(t)
	dir := filepath.Join(t.TempDir(), "snapshot")
	require.NoError(t, os.MkdirAll(filepath.Join(dir, chunksDirname), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(dir, metadataFilename), encoded, 0o600))
	target := filepath.Join(t.TempDir(), "external")
	require.NoError(t, os.WriteFile(target, chunks[0], 0o600))
	require.NoError(t, os.Symlink(target, filepath.Join(dir, chunksDirname, chunkFilename(0))))
	for index := 1; index < len(metadata.ChunkHashes); index++ {
		require.NoError(t, os.WriteFile(filepath.Join(dir, chunksDirname, chunkFilename(uint32(index))), chunks[index], 0o600))
	}
	_, err := OpenSnapshot(dir)
	require.ErrorContains(t, err, "not a regular file")
}

func TestProviderSnapshotMaintenanceSweepsOnlyOwnedStagingDirectories(t *testing.T) {
	live, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "live-badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.CloseBadger() })
	require.NoError(t, live.SetState("scope:export", []byte("bounded provider storage")))
	appHash, err := live.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)

	root := filepath.Join(t.TempDir(), "network-snapshots")
	require.NoError(t, os.Mkdir(root, 0o700))
	owned := filepath.Join(root, ".staging-7-12345")
	require.NoError(t, os.Mkdir(owned, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(owned, snapshotOwnerFilename), []byte(snapshotOwnerContents), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(owned, canonicalStateFilename), []byte("partial canonical image"), 0o600))

	legacyOwned := filepath.Join(root, ".staging-8-23456")
	require.NoError(t, os.Mkdir(legacyOwned, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(legacyOwned, canonicalStateFilename), []byte("legacy partial canonical image"), 0o600))
	unmarked := filepath.Join(root, ".staging-14-89012")
	require.NoError(t, os.Mkdir(unmarked, 0o700))
	markedWithUnexpectedEntry := filepath.Join(root, ".staging-9-34567")
	require.NoError(t, os.Mkdir(markedWithUnexpectedEntry, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(markedWithUnexpectedEntry, snapshotOwnerFilename), []byte(snapshotOwnerContents), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(markedWithUnexpectedEntry, "operator-note"), []byte("preserve"), 0o600))
	misnamed := filepath.Join(root, ".staging-010-45678")
	require.NoError(t, os.Mkdir(misnamed, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(misnamed, snapshotOwnerFilename), []byte(snapshotOwnerContents), 0o600))
	unrelatedFile := filepath.Join(root, ".staging-11-56789")
	require.NoError(t, os.WriteFile(unrelatedFile, []byte("not a directory"), 0o600))

	var externalSentinel string
	var stagingSymlink string
	var markerSymlinkDir string
	if runtime.GOOS != "windows" {
		external := t.TempDir()
		externalSentinel = filepath.Join(external, "sentinel")
		require.NoError(t, os.WriteFile(externalSentinel, []byte("outside"), 0o600))
		stagingSymlink = filepath.Join(root, ".staging-12-67890")
		require.NoError(t, os.Symlink(external, stagingSymlink))
		markerSymlinkDir = filepath.Join(root, ".staging-13-78901")
		require.NoError(t, os.Mkdir(markerSymlinkDir, 0o700))
		require.NoError(t, os.Symlink(externalSentinel, filepath.Join(markerSymlinkDir, snapshotOwnerFilename)))
	}

	require.NoError(t, MaintainProviderSnapshotRoot(root))
	_, err = os.Lstat(owned)
	assert.ErrorIs(t, err, os.ErrNotExist, "the strictly named, marked, safe staging directory is swept")
	_, err = os.Lstat(legacyOwned)
	assert.ErrorIs(t, err, os.ErrNotExist, "structurally exact pre-marker staging is swept during upgrade")
	for _, path := range []string{unmarked, markedWithUnexpectedEntry, misnamed, unrelatedFile} {
		_, statErr := os.Lstat(path)
		require.NoError(t, statErr, "unrelated entry %q must be preserved", filepath.Base(path))
	}
	if runtime.GOOS != "windows" {
		for _, path := range []string{stagingSymlink, markerSymlinkDir} {
			_, statErr := os.Lstat(path)
			require.NoError(t, statErr, "adversarial symlink entry must be preserved")
		}
		contents, readErr := os.ReadFile(externalSentinel) //nolint:gosec // test-owned path
		require.NoError(t, readErr)
		assert.Equal(t, "outside", string(contents), "cleanup must never follow a symlink outside the root")
	}
	_, err = Export(context.Background(), live.DB(), root, 50, appHash, MinChunkSize, narrowAppHashVerifier(t))
	require.NoError(t, err, "export repeats maintenance without touching preserved entries")
}

func TestExportRetainsEightNewestOwnedSnapshotsAndPreservesAdversarialEntries(t *testing.T) {
	live, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "live-badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.CloseBadger() })
	require.NoError(t, live.SetState("scope:export", []byte("deterministic retention")))
	appHash, err := live.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)

	root := filepath.Join(t.TempDir(), "network-snapshots")
	require.NoError(t, os.Mkdir(root, 0o700))
	unrelatedDir := filepath.Join(root, "operator-data")
	require.NoError(t, os.Mkdir(unrelatedDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(unrelatedDir, "keep"), []byte("operator"), 0o600))
	malformedOwnedName := filepath.Join(root, "00000000000000000999-aaaaaaaaaaaaaaaa")
	require.NoError(t, os.Mkdir(malformedOwnedName, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(malformedOwnedName, "keep"), []byte("malformed"), 0o600))

	var externalSentinel string
	var publishedSymlink string
	if runtime.GOOS != "windows" {
		external := t.TempDir()
		externalSentinel = filepath.Join(external, "sentinel")
		require.NoError(t, os.WriteFile(externalSentinel, []byte("outside"), 0o600))
		publishedSymlink = filepath.Join(root, "00000000000000001000-bbbbbbbbbbbbbbbb")
		require.NoError(t, os.Symlink(external, publishedSymlink))
	}

	total := providerSnapshotRetention + 3
	for height := 1; height <= total; height++ {
		_, exportErr := Export(context.Background(), live.DB(), root, uint64(height), appHash, MinChunkSize, narrowAppHashVerifier(t))
		require.NoError(t, exportErr)
	}
	owned, err := listOwnedPublishedSnapshots(root)
	require.NoError(t, err)
	require.Len(t, owned, providerSnapshotRetention)
	for index, snapshot := range owned {
		assert.Equal(t, uint64(total-index), snapshot.Metadata.Height)
		require.NoError(t, requireSnapshotOwnerMarker(filepath.Join(snapshot.Dir, snapshotOwnerFilename)))
	}
	assert.Equal(t, uint64(total-2), owned[2].Metadata.Height,
		"after the newest two H+2-ineligible candidates, an eligible fallback remains")
	assert.Equal(t, uint64(total-providerSnapshotRetention+1), owned[len(owned)-1].Metadata.Height)

	for _, path := range []string{unrelatedDir, malformedOwnedName} {
		_, statErr := os.Lstat(path)
		require.NoError(t, statErr, "retention must preserve unrelated or malformed directories")
	}
	if runtime.GOOS != "windows" {
		_, statErr := os.Lstat(publishedSymlink)
		require.NoError(t, statErr, "retention must preserve snapshot-shaped symlinks")
		contents, readErr := os.ReadFile(externalSentinel) //nolint:gosec // test-owned path
		require.NoError(t, readErr)
		assert.Equal(t, "outside", string(contents))
	}
}

func TestExportRejectsSymlinkProviderRoot(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("symlink semantics differ on Windows")
	}
	live, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "live-badger"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = live.CloseBadger() })
	require.NoError(t, live.SetState("scope:export", []byte("root symlink")))
	appHash, err := live.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)

	external := t.TempDir()
	sentinel := filepath.Join(external, "sentinel")
	require.NoError(t, os.WriteFile(sentinel, []byte("outside"), 0o600))
	root := filepath.Join(t.TempDir(), "snapshot-root-link")
	require.NoError(t, os.Symlink(external, root))
	_, err = Export(context.Background(), live.DB(), root, 1, appHash, MinChunkSize, narrowAppHashVerifier(t))
	require.ErrorContains(t, err, "real directory")
	contents, err := os.ReadFile(sentinel) //nolint:gosec // test-owned path
	require.NoError(t, err)
	assert.Equal(t, "outside", string(contents))
	entries, err := os.ReadDir(external)
	require.NoError(t, err)
	require.Len(t, entries, 1, "a rejected symlink root must not receive staging or snapshots")
}
