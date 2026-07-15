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
	return func(_ context.Context, backupPath string) ([]byte, error) {
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
		if err := db.Load(file, 16); err != nil {
			_ = file.Close()
			_ = db.Close()
			return nil, err
		}
		if err := file.Close(); err != nil {
			_ = db.Close()
			return nil, err
		}
		if err := db.Close(); err != nil {
			return nil, err
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
	assert.ElementsMatch(t, []string{metadataFilename, chunksDirname}, names,
		"network export must contain no SQLite, CometBFT db/config, keys, vault, or binary")

	opened, err := OpenSnapshot(exported.Dir)
	require.NoError(t, err)
	assert.Equal(t, exported.Hash, opened.Hash)
	var backup bytes.Buffer
	for index := range opened.Metadata.ChunkHashes {
		chunk, err := opened.LoadChunk(uint32(index))
		require.NoError(t, err)
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
