package statesync

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"sync"
)

// Assembler persists verified chunks into a fresh staging directory. It never
// opens or mutates the live Badger database; atomic database activation belongs
// to the later ABCI integration gate.
type Assembler struct {
	mu       sync.Mutex
	dir      string
	metadata Metadata
	received map[uint32]struct{}
}

// NewAssembler creates a fresh staging directory. Refusing an existing path
// prevents an interrupted or attacker-controlled assembly from being silently
// mixed with a newly offered snapshot.
func NewAssembler(dir string, metadata Metadata) (*Assembler, error) {
	if dir == "" {
		return nil, errors.New("state sync staging directory is empty")
	}
	if err := validateMetadata(metadata); err != nil {
		return nil, err
	}
	if err := os.Mkdir(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create state sync staging directory: %w", err)
	}
	return &Assembler{dir: dir, metadata: metadata, received: make(map[uint32]struct{})}, nil
}

// AddChunk verifies index, canonical length, and the manifest hash before an
// atomic write. Re-delivery of the exact chunk is idempotent.
func (a *Assembler) AddChunk(index uint32, chunk []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	expectedSize, err := expectedChunkSize(a.metadata, index)
	if err != nil {
		return err
	}
	if len(chunk) != expectedSize {
		return fmt.Errorf("state sync chunk %d size %d, expected %d", index, len(chunk), expectedSize)
	}
	hash := sha256.Sum256(chunk)
	if !bytes.Equal(hash[:], a.metadata.ChunkHashes[index]) {
		return fmt.Errorf("state sync chunk %d hash mismatch", index)
	}
	if _, ok := a.received[index]; ok {
		return nil
	}
	finalPath := filepath.Join(a.dir, chunkFilename(index))
	if _, statErr := os.Stat(finalPath); statErr == nil {
		return fmt.Errorf("state sync chunk %d exists outside assembler state", index)
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	tmp, err := os.CreateTemp(a.dir, ".chunk-part-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return err
	}
	if _, err := tmp.Write(chunk); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, finalPath); err != nil {
		return err
	}
	if err := syncDir(a.dir); err != nil {
		return err
	}
	ok = true
	a.received[index] = struct{}{}
	return nil
}

// Missing returns missing chunk indexes in canonical ascending order.
func (a *Assembler) Missing() []uint32 {
	a.mu.Lock()
	defer a.mu.Unlock()
	missing := make([]uint32, 0, len(a.metadata.ChunkHashes)-len(a.received))
	for i := range a.metadata.ChunkHashes {
		index := uint32(i)
		if _, ok := a.received[index]; !ok {
			missing = append(missing, index)
		}
	}
	return missing
}

// Assemble streams all verified chunks into a new file, verifies the complete
// Badger backup hash and byte count, fsyncs, then atomically renames it. It
// refuses to replace an existing output.
func (a *Assembler) Assemble(outputPath string) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.received) != len(a.metadata.ChunkHashes) {
		return errors.New("state sync snapshot is incomplete")
	}
	if outputPath == "" {
		return errors.New("state sync output path is empty")
	}
	if _, err := os.Stat(outputPath); err == nil {
		return errors.New("state sync output already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	outputDir := filepath.Dir(outputPath)
	if err := os.MkdirAll(outputDir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(outputDir, ".state-sync-backup-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	ok := false
	defer func() {
		_ = tmp.Close()
		if !ok {
			_ = os.Remove(tmpPath)
		}
	}()
	whole := sha256.New()
	written := uint64(0)
	indexes := make([]int, len(a.metadata.ChunkHashes))
	for i := range indexes {
		indexes[i] = i
	}
	sort.Ints(indexes)
	for _, index := range indexes {
		chunkFile, err := os.Open(filepath.Join(a.dir, chunkFilename(uint32(index)))) //nolint:gosec // index is bounded metadata
		if err != nil {
			return err
		}
		count, copyErr := io.Copy(io.MultiWriter(tmp, whole), chunkFile)
		closeErr := chunkFile.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeErr != nil {
			return closeErr
		}
		written += uint64(count) // #nosec G115 -- each chunk is bounded and positive
	}
	if written != a.metadata.BackupSize || !bytes.Equal(whole.Sum(nil), a.metadata.BackupHash) {
		return errors.New("assembled state sync backup verification failed")
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, outputPath); err != nil {
		return err
	}
	if err := syncDir(outputDir); err != nil {
		return err
	}
	ok = true
	return nil
}

func chunkFilename(index uint32) string { return fmt.Sprintf("chunk-%06d", index) }

func syncDir(path string) error {
	dir, err := os.Open(path) //nolint:gosec // caller-supplied staging/output directory
	if err != nil {
		return err
	}
	defer func() { _ = dir.Close() }()
	return dir.Sync()
}
