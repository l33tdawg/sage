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
	"strconv"
	"strings"
	"sync"

	badger "github.com/dgraph-io/badger/v4"
)

const (
	metadataFilename       = "metadata.bin"
	chunksDirname          = "chunks"
	canonicalStateFilename = "canonical.state"
	snapshotOwnerFilename  = ".sage-public-snapshot-v1"
	snapshotOwnerContents  = "SAGE public state-sync snapshot v1\n"
	stagingPrefix          = ".staging-"

	// providerSnapshotRetention keeps the newest two snapshots that may still
	// lack H+1/H+2 light blocks plus six older deterministic fallback points.
	// Completed public payload storage is bounded to eight snapshots. Before an
	// export, retention drops to seven; its canonical source image and chunk set
	// may then each grow to MaxSnapshotBytes before publication restores eight.
	providerSnapshotRetention = 8
)

var providerSnapshotStorageMu sync.Mutex

// BackupVerifier restores a staged canonical state image with the historical
// AppHash rule appropriate to its height and returns the computed hash. Export
// requires this callback so a racing/inconsistent image is never advertised.
type BackupVerifier func(context.Context, string) ([]byte, error)

// Snapshot is a verified, network-safe export descriptor. Dir contains only
// canonical public metadata, canonical latest-visible state chunks, and the
// optional fixed local ownership marker that is never transported as a chunk.
type Snapshot struct {
	Dir      string
	Metadata Metadata
	Encoded  []byte
	Hash     []byte
}

// MaintainProviderSnapshotRoot performs startup maintenance before a provider
// reuses an existing catalog: it sweeps only provably owned staging leftovers
// and retains the newest bounded set of completed public snapshots. Export also
// invokes the same maintenance, reserving one retention slot for its new image.
func MaintainProviderSnapshotRoot(root string) error {
	providerSnapshotStorageMu.Lock()
	defer providerSnapshotStorageMu.Unlock()
	if err := requireProviderSnapshotRoot(root); err != nil {
		return err
	}
	return maintainProviderSnapshotRoot(root, providerSnapshotRetention)
}

// Export writes a bounded canonical latest-visible state image, verifies its
// AppHash, splits it into hashed chunks, removes the monolithic staging file,
// and atomically publishes a network-safe snapshot directory.
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
	providerSnapshotStorageMu.Lock()
	defer providerSnapshotStorageMu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, err
	}
	if err := requireProviderSnapshotRoot(root); err != nil {
		return nil, err
	}
	// Prune before publishing so a successful export can never exceed the
	// completed-snapshot bound and a post-rename cleanup error cannot make the
	// caller retry an already published snapshot.
	if err := maintainProviderSnapshotRoot(root, providerSnapshotRetention-1); err != nil {
		return nil, err
	}
	staging, err := os.MkdirTemp(root, fmt.Sprintf("%s%d-", stagingPrefix, height))
	if err != nil {
		return nil, err
	}
	published := false
	defer func() {
		if !published {
			_ = os.RemoveAll(staging)
		}
	}()
	if ownerWriteErr := writeSyncedFile(filepath.Join(staging, snapshotOwnerFilename), []byte(snapshotOwnerContents)); ownerWriteErr != nil {
		return nil, ownerWriteErr
	}
	if stagingSyncErr := syncDir(staging); stagingSyncErr != nil {
		return nil, stagingSyncErr
	}
	if rootSyncErr := syncDir(root); rootSyncErr != nil {
		return nil, rootSyncErr
	}

	backupPath := filepath.Join(staging, canonicalStateFilename)
	backupFile, err := os.OpenFile(backupPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return nil, err
	}
	bounded := &boundedContextWriter{ctx: ctx, writer: backupFile, remaining: MaxSnapshotBytes}
	backupErr := WriteCanonicalState(ctx, db, bounded)
	syncErr := backupFile.Sync()
	closeErr := backupFile.Close()
	if backupErr != nil {
		return nil, fmt.Errorf("state sync canonical state export: %w", backupErr)
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
	finalDir := filepath.Join(root, publishedSnapshotDirectoryName(height, snapshotHash))
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
// chunk. New exports also carry one fixed public ownership marker used only for
// safe local cleanup; legacy two-entry exports remain readable. LoadChunk
// repeats the content hash immediately before bytes are served.
func OpenSnapshot(dir string) (*Snapshot, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	if len(entries) < 2 || len(entries) > 3 {
		return nil, errors.New("state sync snapshot directory contains unexpected entries")
	}
	entryTypes := make(map[string]os.FileMode, len(entries))
	for _, entry := range entries {
		switch entry.Name() {
		case metadataFilename, chunksDirname, snapshotOwnerFilename:
		default:
			return nil, errors.New("state sync snapshot directory contains unexpected entries")
		}
		entryTypes[entry.Name()] = entry.Type()
	}
	if _, ok := entryTypes[metadataFilename]; !ok {
		return nil, errors.New("state sync snapshot metadata is missing")
	}
	if _, ok := entryTypes[chunksDirname]; !ok {
		return nil, errors.New("state sync snapshot chunks directory is missing")
	}
	if _, ok := entryTypes[snapshotOwnerFilename]; ok {
		if ownerErr := requireSnapshotOwnerMarker(filepath.Join(dir, snapshotOwnerFilename)); ownerErr != nil {
			return nil, ownerErr
		}
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

func requireProviderSnapshotRoot(root string) error {
	info, err := os.Lstat(root)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state sync provider snapshot root must be a real directory")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("state sync provider snapshot root must not be group/world writable")
	}
	return nil
}

func maintainProviderSnapshotRoot(root string, keepCompleted int) error {
	if keepCompleted < 0 {
		return errors.New("state sync completed snapshot retention cannot be negative")
	}
	changed, err := sweepOwnedProviderStaging(root)
	if err != nil {
		return err
	}
	completed, err := listOwnedPublishedSnapshots(root)
	if err != nil {
		return err
	}
	for _, snapshot := range completed[min(keepCompleted, len(completed)):] {
		if err := removeOwnedPublishedSnapshot(root, snapshot); err != nil {
			return err
		}
		changed = true
	}
	if changed {
		return syncDir(root)
	}
	return nil
}

func sweepOwnedProviderStaging(root string) (bool, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return false, err
	}
	changed := false
	for _, entry := range entries {
		if !entry.IsDir() || !isProviderStagingDirectoryName(entry.Name()) {
			continue
		}
		path := filepath.Join(root, entry.Name())
		if !isOwnedProviderStagingDirectory(path) {
			continue
		}
		if err := os.RemoveAll(path); err != nil {
			return changed, fmt.Errorf("remove owned state sync staging directory %q: %w", entry.Name(), err)
		}
		changed = true
	}
	return changed, nil
}

func isProviderStagingDirectoryName(name string) bool {
	if !strings.HasPrefix(name, stagingPrefix) {
		return false
	}
	parts := strings.Split(strings.TrimPrefix(name, stagingPrefix), "-")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return false
	}
	height, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil || height == 0 || strconv.FormatUint(height, 10) != parts[0] {
		return false
	}
	nonce, err := strconv.ParseUint(parts[1], 10, 32)
	return err == nil && strconv.FormatUint(nonce, 10) == parts[1]
}

func isOwnedProviderStagingDirectory(path string) bool {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return false
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	marked := false
	legacyArtifact := false
	for _, entry := range entries {
		entryPath := filepath.Join(path, entry.Name())
		switch entry.Name() {
		case snapshotOwnerFilename:
			if requireSnapshotOwnerMarker(entryPath) != nil {
				return false
			}
			marked = true
		case canonicalStateFilename, metadataFilename:
			if requireRegularFile(entryPath) != nil {
				return false
			}
			legacyArtifact = true
		case chunksDirname:
			if !isSafePartialChunksDirectory(entryPath) {
				return false
			}
			legacyArtifact = true
		default:
			return false
		}
	}
	// Pre-marker releases used the same strict random directory name and only
	// these three staging artifacts. Preserve empty ambiguous directories, but
	// reclaim structurally exact legacy exports so upgrades clean large crashes.
	return marked || legacyArtifact
}

func isSafePartialChunksDirectory(path string) bool {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return false
	}
	for _, entry := range entries {
		name := entry.Name()
		if len(name) != len(chunkFilename(0)) || !strings.HasPrefix(name, "chunk-") {
			return false
		}
		index, parseErr := strconv.ParseUint(strings.TrimPrefix(name, "chunk-"), 10, 32)
		if parseErr != nil || index >= uint64(MaxChunks) || chunkFilename(uint32(index)) != name { // #nosec G115 -- bounded above
			return false
		}
		if requireRegularFile(filepath.Join(path, name)) != nil {
			return false
		}
	}
	return true
}

func requireSnapshotOwnerMarker(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || info.Size() != int64(len(snapshotOwnerContents)) {
		return errors.New("state sync snapshot ownership marker is invalid")
	}
	contents, err := os.ReadFile(path) //nolint:gosec // fixed-size marker under the provider-owned snapshot root
	if err != nil {
		return err
	}
	if string(contents) != snapshotOwnerContents {
		return errors.New("state sync snapshot ownership marker is invalid")
	}
	return nil
}

func listOwnedPublishedSnapshots(root string) ([]*Snapshot, error) {
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, err
	}
	snapshots := make([]*Snapshot, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !looksLikePublishedSnapshotDirectoryName(entry.Name()) {
			continue
		}
		snapshot, openErr := OpenSnapshot(filepath.Join(root, entry.Name()))
		if openErr != nil || publishedSnapshotDirectoryName(snapshot.Metadata.Height, snapshot.Hash) != entry.Name() {
			continue
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

func looksLikePublishedSnapshotDirectoryName(name string) bool {
	if len(name) != 20+1+16 || name[20] != '-' {
		return false
	}
	height, err := strconv.ParseUint(name[:20], 10, 64)
	if err != nil || height == 0 || fmt.Sprintf("%020d", height) != name[:20] {
		return false
	}
	suffix := name[21:]
	if suffix != strings.ToLower(suffix) {
		return false
	}
	_, err = hex.DecodeString(suffix)
	return err == nil
}

func publishedSnapshotDirectoryName(height uint64, snapshotHash []byte) string {
	if height == 0 || len(snapshotHash) < 8 {
		return ""
	}
	return fmt.Sprintf("%020d-%s", height, hex.EncodeToString(snapshotHash[:8]))
}

func removeOwnedPublishedSnapshot(root string, snapshot *Snapshot) error {
	if snapshot == nil {
		return errors.New("cannot prune nil state sync snapshot")
	}
	name := publishedSnapshotDirectoryName(snapshot.Metadata.Height, snapshot.Hash)
	path := filepath.Join(root, name)
	if name == "" || filepath.Clean(snapshot.Dir) != path || filepath.Dir(path) != filepath.Clean(root) {
		return errors.New("state sync snapshot prune path is outside the provider root")
	}
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state sync snapshot changed before retention pruning")
	}
	current, err := OpenSnapshot(path)
	if err != nil || current.Metadata.Height != snapshot.Metadata.Height || !bytes.Equal(current.Hash, snapshot.Hash) || publishedSnapshotDirectoryName(current.Metadata.Height, current.Hash) != name {
		return errors.New("state sync snapshot changed before retention pruning")
	}
	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("remove retained state sync snapshot %q: %w", name, err)
	}
	return nil
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
	if _, writeErr := file.Write(data); writeErr != nil {
		return writeErr
	}
	if syncErr := file.Sync(); syncErr != nil {
		return syncErr
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
