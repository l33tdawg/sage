package statesync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	metadataFilename = "metadata.bin"
	chunksDirname    = "chunks"
)

// BackupVerifier loads a staged Badger backup with the historical AppHash rule
// appropriate to its height and returns the computed hash. Export requires this
// callback so a racing/inconsistent backup is never advertised.
type BackupVerifier func(context.Context, string) ([]byte, error)

// Snapshot is a verified, network-safe export descriptor. Dir contains only
// canonical public metadata and Badger backup chunks.
type Snapshot struct {
	Dir      string
	Metadata Metadata
	Encoded  []byte
	Hash     []byte
}

// Export writes a bounded live Badger backup, verifies its AppHash, splits it
// into hashed chunks, removes the monolithic staging file, and atomically
// publishes a network-safe snapshot directory.
func Export(ctx context.Context, db *badger.DB, root string, height uint64, appHash []byte, chunkSize uint32, verify BackupVerifier) (*Snapshot, error) {
	if db == nil || root == "" || verify == nil {
		return nil, errors.New("state sync export requires db, root, and verifier")
	}
	if height == 0 || len(appHash) != sha256.Size {
		return nil, errors.New("state sync export requires positive height and SHA-256 AppHash")
	}
	if chunkSize < MinChunkSize || chunkSize > MaxChunkSize {
		return nil, fmt.Errorf("state sync chunk size must be %d..%d bytes", MinChunkSize, MaxChunkSize)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	staging, err := os.MkdirTemp(root, fmt.Sprintf(".staging-%d-", height))
	if err != nil {
		return nil, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()

	backupPath := filepath.Join(staging, "badger.backup")
	backupFile, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	bounded := &boundedContextWriter{ctx: ctx, writer: backupFile, remaining: MaxSnapshotBytes}
	_, backupErr := db.Backup(bounded, 0)
	syncErr := backupFile.Sync()
	closeErr := backupFile.Close()
	if backupErr != nil {
		return nil, fmt.Errorf("state sync badger backup: %w", backupErr)
	}
	if syncErr != nil {
		return nil, syncErr
	}
	if closeErr != nil {
		return nil, closeErr
	}
	stat, err := os.Stat(backupPath)
	if err != nil {
		return nil, err
	}
	if stat.Size() <= 0 || uint64(stat.Size()) > MaxSnapshotBytes { // #nosec G115 -- positive size checked first
		return nil, fmt.Errorf("state sync backup size %d outside 1..%d", stat.Size(), MaxSnapshotBytes)
	}
	computedAppHash, err := verify(ctx, backupPath)
	if err != nil {
		return nil, fmt.Errorf("verify state sync backup AppHash: %w", err)
	}
	if !bytes.Equal(computedAppHash, appHash) {
		return nil, errors.New("state sync backup AppHash does not match committed AppHash")
	}

	chunksDir := filepath.Join(staging, chunksDirname)
	if mkdirChunksErr := os.Mkdir(chunksDir, 0o700); mkdirChunksErr != nil {
		return nil, mkdirChunksErr
	}
	input, err := os.Open(backupPath) //nolint:gosec // staging-owned path
	if err != nil {
		return nil, err
	}
	whole := sha256.New()
	chunkHashes := make([][]byte, 0, (uint64(stat.Size())+uint64(chunkSize)-1)/uint64(chunkSize)) // #nosec G115 -- positive bounded size
	buffer := make([]byte, int(chunkSize))
	for index := uint32(0); ; index++ {
		if contextErr := ctx.Err(); contextErr != nil {
			_ = input.Close()
			return nil, contextErr
		}
		if index >= MaxChunks {
			_ = input.Close()
			return nil, fmt.Errorf("state sync export exceeds %d chunks", MaxChunks)
		}
		read, readErr := io.ReadFull(input, buffer)
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			_ = input.Close()
			return nil, readErr
		}
		chunk := buffer[:read]
		_, _ = whole.Write(chunk)
		hash := sha256.Sum256(chunk)
		chunkHashes = append(chunkHashes, append([]byte(nil), hash[:]...))
		if writeChunkErr := writeSyncedFile(filepath.Join(chunksDir, chunkFilename(index)), chunk); writeChunkErr != nil {
			_ = input.Close()
			return nil, writeChunkErr
		}
		if errors.Is(readErr, io.ErrUnexpectedEOF) {
			break
		}
	}
	if closeInputErr := input.Close(); closeInputErr != nil {
		return nil, closeInputErr
	}
	if len(chunkHashes) == 0 {
		return nil, errors.New("state sync backup produced no chunks")
	}
	if removeBackupErr := os.Remove(backupPath); removeBackupErr != nil {
		return nil, removeBackupErr
	}
	metadata, encoded, snapshotHash, err := buildMetadataFromDigests(
		height, appHash, uint64(stat.Size()), chunkSize, whole.Sum(nil), chunkHashes, // #nosec G115 -- positive bounded size
	)
	if err != nil {
		return nil, err
	}
	if err := writeSyncedFile(filepath.Join(staging, metadataFilename), encoded); err != nil {
		return nil, err
	}
	if err := syncDir(chunksDir); err != nil {
		return nil, err
	}
	if err := syncDir(staging); err != nil {
		return nil, err
	}
	finalDir := filepath.Join(root, fmt.Sprintf("%020d-%s", height, hex.EncodeToString(snapshotHash[:8])))
	if _, err := os.Stat(finalDir); err == nil {
		return nil, errors.New("state sync snapshot already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err := os.Rename(staging, finalDir); err != nil {
		return nil, err
	}
	if err := syncDir(root); err != nil {
		_ = os.RemoveAll(finalDir)
		_ = syncDir(root)
		return nil, err
	}
	published = true
	return &Snapshot{Dir: finalDir, Metadata: metadata, Encoded: encoded, Hash: snapshotHash}, nil
}

// OpenSnapshot validates a published descriptor and the size/type of every
// chunk. LoadChunk repeats the content hash immediately before bytes are served.
func OpenSnapshot(dir string) (*Snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(entries) != 2 {
		return nil, errors.New("state sync snapshot directory contains unexpected entries")
	}
	entryTypes := make(map[string]os.FileMode, len(entries))
	for _, entry := range entries {
		entryTypes[entry.Name()] = entry.Type()
	}
	if _, ok := entryTypes[metadataFilename]; !ok {
		return nil, errors.New("state sync snapshot metadata is missing")
	}
	if _, ok := entryTypes[chunksDirname]; !ok {
		return nil, errors.New("state sync snapshot chunks directory is missing")
	}
	chunksInfo, err := os.Lstat(filepath.Join(dir, chunksDirname))
	if err != nil || !chunksInfo.IsDir() || chunksInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state sync chunks path is not a real directory")
	}
	metadataPath := filepath.Join(dir, metadataFilename)
	if regularMetadataErr := requireRegularFile(metadataPath); regularMetadataErr != nil {
		return nil, regularMetadataErr
	}
	encoded, err := os.ReadFile(metadataPath) //nolint:gosec // caller-selected snapshot root
	if err != nil {
		return nil, err
	}
	metadata, err := DecodeMetadata(encoded)
	if err != nil {
		return nil, err
	}
	chunkEntries, err := os.ReadDir(filepath.Join(dir, chunksDirname))
	if err != nil {
		return nil, err
	}
	if len(chunkEntries) != len(metadata.ChunkHashes) {
		return nil, errors.New("state sync chunks directory has unexpected entry count")
	}
	expectedNames := make(map[string]struct{}, len(metadata.ChunkHashes))
	for index := range metadata.ChunkHashes {
		expectedNames[chunkFilename(uint32(index))] = struct{}{}
	}
	for _, entry := range chunkEntries {
		if _, ok := expectedNames[entry.Name()]; !ok {
			return nil, fmt.Errorf("unexpected state sync chunk entry %q", entry.Name())
		}
	}
	for index := range metadata.ChunkHashes {
		path := filepath.Join(dir, chunksDirname, chunkFilename(uint32(index)))
		if err := requireRegularFile(path); err != nil {
			return nil, err
		}
		stat, err := os.Stat(path)
		if err != nil {
			return nil, err
		}
		expected, err := expectedChunkSize(metadata, uint32(index))
		if err != nil || stat.Size() != int64(expected) {
			return nil, fmt.Errorf("state sync chunk %d has invalid published size", index)
		}
	}
	hash := sha256.Sum256(encoded)
	return &Snapshot{Dir: dir, Metadata: metadata, Encoded: encoded, Hash: append([]byte(nil), hash[:]...)}, nil
}

// LoadChunk returns one verified chunk and refuses symlinks or post-publication
// tampering.
func (snapshot *Snapshot) LoadChunk(index uint32) ([]byte, error) {
	if snapshot == nil {
		return nil, errors.New("nil state sync snapshot")
	}
	expected, err := expectedChunkSize(snapshot.Metadata, index)
	if err != nil {
		return nil, err
	}
	path := filepath.Join(snapshot.Dir, chunksDirname, chunkFilename(index))
	if regularChunkErr := requireRegularFile(path); regularChunkErr != nil {
		return nil, regularChunkErr
	}
	chunk, err := os.ReadFile(path) //nolint:gosec // index is bounded metadata
	if err != nil {
		return nil, err
	}
	if len(chunk) != expected {
		return nil, fmt.Errorf("state sync chunk %d size changed after publication", index)
	}
	hash := sha256.Sum256(chunk)
	if !bytes.Equal(hash[:], snapshot.Metadata.ChunkHashes[index]) {
		return nil, fmt.Errorf("state sync chunk %d hash changed after publication", index)
	}
	return chunk, nil
}

// ListSnapshots returns valid published snapshots newest-first. A malformed
// directory fails the catalog closed instead of disappearing silently.
func ListSnapshots(root string) ([]*Snapshot, error) {
	entries, err := os.ReadDir(root)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	snapshots := make([]*Snapshot, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || len(entry.Name()) > 0 && entry.Name()[0] == '.' {
			continue
		}
		snapshot, err := OpenSnapshot(filepath.Join(root, entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("open state sync snapshot %q: %w", entry.Name(), err)
		}
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		if snapshots[i].Metadata.Height == snapshots[j].Metadata.Height {
			return bytes.Compare(snapshots[i].Hash, snapshots[j].Hash) > 0
		}
		return snapshots[i].Metadata.Height > snapshots[j].Metadata.Height
	})
	return snapshots, nil
}

type boundedContextWriter struct {
	ctx       context.Context
	writer    io.Writer
	remaining uint64
}

func (writer *boundedContextWriter) Write(data []byte) (int, error) {
	if err := writer.ctx.Err(); err != nil {
		return 0, err
	}
	if uint64(len(data)) > writer.remaining {
		return 0, fmt.Errorf("state sync backup exceeds %d bytes", MaxSnapshotBytes)
	}
	written, err := writer.writer.Write(data)
	writer.remaining -= uint64(written) // #nosec G115 -- written never exceeds len(data)
	return written, err
}

func writeSyncedFile(path string, data []byte) error {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	ok := false
	defer func() {
		_ = file.Close()
		if !ok {
			_ = os.Remove(path)
		}
	}()
	if _, err := file.Write(data); err != nil {
		return err
	}
	if err := file.Sync(); err != nil {
		return err
	}
	if err := file.Close(); err != nil {
		return err
	}
	ok = true
	return nil
}

func requireRegularFile(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("state sync path is not a regular file: %s", path)
	}
	return nil
}
