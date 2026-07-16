package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const (
	indexBackfillSidecarMagic = "SAGEIDX1"

	agentOrgsIndexBackfillSidecar = ".sage-index-backfill-agent-orgs-v1"
	orgNameIndexBackfillSidecar   = ".sage-index-backfill-org-name-v1"

	indexBackfillSidecarHeaderBytes   = len(indexBackfillSidecarMagic) + 1 + 4
	indexBackfillSidecarChecksumBytes = sha256.Size
	maxIndexBackfillSidecarBytes      = indexBackfillSidecarHeaderBytes + defaultIndexBackfillMaxBytes + indexBackfillSidecarChecksumBytes
	maxIndexBackfillSidecarFileBytes  = int64(maxIndexBackfillSidecarBytes)
)

type indexBackfillProgress struct {
	complete bool
	cursor   []byte
}

// encodeIndexBackfillProgress produces a checksummed, self-delimiting local
// progress record. The checksum is not an authentication boundary (the Badger
// directory is operator-owned), but it turns torn writes and storage damage
// into a fail-closed startup error instead of an accidental cursor advance.
func encodeIndexBackfillProgress(progress indexBackfillProgress) ([]byte, error) {
	if progress.complete && len(progress.cursor) != 0 {
		return nil, errors.New("completed index backfill progress cannot contain a cursor")
	}
	if len(progress.cursor) > defaultIndexBackfillMaxBytes {
		return nil, fmt.Errorf("index backfill cursor exceeds %d bytes", defaultIndexBackfillMaxBytes)
	}

	encoded := make([]byte, indexBackfillSidecarHeaderBytes+len(progress.cursor)+indexBackfillSidecarChecksumBytes)
	copy(encoded, indexBackfillSidecarMagic)
	if progress.complete {
		encoded[len(indexBackfillSidecarMagic)] = indexBackfillComplete
	} else {
		encoded[len(indexBackfillSidecarMagic)] = indexBackfillInProgress
	}
	binary.BigEndian.PutUint32(encoded[len(indexBackfillSidecarMagic)+1:indexBackfillSidecarHeaderBytes], uint32(len(progress.cursor))) // #nosec G115 -- bounded to 1 MiB above
	copy(encoded[indexBackfillSidecarHeaderBytes:], progress.cursor)
	checksumOffset := len(encoded) - indexBackfillSidecarChecksumBytes
	checksum := sha256.Sum256(encoded[:checksumOffset])
	copy(encoded[checksumOffset:], checksum[:])
	return encoded, nil
}

func decodeIndexBackfillProgress(encoded, sourcePrefix []byte) (indexBackfillProgress, error) {
	minimum := indexBackfillSidecarHeaderBytes + indexBackfillSidecarChecksumBytes
	if len(encoded) < minimum || len(encoded) > maxIndexBackfillSidecarBytes {
		return indexBackfillProgress{}, fmt.Errorf("index backfill sidecar size %d is outside %d..%d", len(encoded), minimum, maxIndexBackfillSidecarBytes)
	}
	if !bytes.Equal(encoded[:len(indexBackfillSidecarMagic)], []byte(indexBackfillSidecarMagic)) {
		return indexBackfillProgress{}, errors.New("index backfill sidecar magic mismatch")
	}

	cursorBytes := int(binary.BigEndian.Uint32(encoded[len(indexBackfillSidecarMagic)+1 : indexBackfillSidecarHeaderBytes]))
	wantBytes := indexBackfillSidecarHeaderBytes + cursorBytes + indexBackfillSidecarChecksumBytes
	if cursorBytes < 0 || wantBytes != len(encoded) {
		return indexBackfillProgress{}, errors.New("index backfill sidecar cursor length mismatch")
	}
	checksumOffset := len(encoded) - indexBackfillSidecarChecksumBytes
	wantChecksum := sha256.Sum256(encoded[:checksumOffset])
	if !bytes.Equal(encoded[checksumOffset:], wantChecksum[:]) {
		return indexBackfillProgress{}, errors.New("index backfill sidecar checksum mismatch")
	}

	state := encoded[len(indexBackfillSidecarMagic)]
	cursor := append([]byte(nil), encoded[indexBackfillSidecarHeaderBytes:checksumOffset]...)
	switch state {
	case indexBackfillComplete:
		if len(cursor) != 0 {
			return indexBackfillProgress{}, errors.New("completed index backfill sidecar contains a cursor")
		}
		return indexBackfillProgress{complete: true}, nil
	case indexBackfillInProgress:
		if len(cursor) > 0 && !bytes.HasPrefix(cursor, sourcePrefix) {
			return indexBackfillProgress{}, errors.New("index backfill sidecar cursor is outside source namespace")
		}
		return indexBackfillProgress{cursor: cursor}, nil
	default:
		return indexBackfillProgress{}, errors.New("index backfill sidecar has invalid progress state")
	}
}

func validateIndexBackfillDirectory(dir string) error {
	if dir == "" || filepath.Clean(dir) != dir {
		return errors.New("index backfill Badger directory path must be non-empty and clean")
	}
	info, err := os.Lstat(dir)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("index backfill Badger path must be a real directory")
	}
	return nil
}

func readIndexBackfillProgress(dir, name string, sourcePrefix []byte) (indexBackfillProgress, error) {
	if err := validateIndexBackfillDirectory(dir); err != nil {
		return indexBackfillProgress{}, err
	}
	path := filepath.Join(dir, name)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		// A genuinely missing sidecar is the only restart-from-zero case. Every
		// derived row is an idempotent Set, so replaying from the beginning is safe.
		return indexBackfillProgress{}, nil
	}
	if err != nil {
		return indexBackfillProgress{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return indexBackfillProgress{}, errors.New("index backfill sidecar must be a regular non-symlink file")
	}
	if info.Size() <= 0 || info.Size() > maxIndexBackfillSidecarFileBytes {
		return indexBackfillProgress{}, fmt.Errorf("index backfill sidecar size %d is invalid", info.Size())
	}

	file, err := os.Open(path) //nolint:gosec // exact file inside the validated operator-owned Badger directory
	if err != nil {
		return indexBackfillProgress{}, err
	}
	defer func() { _ = file.Close() }()
	openedInfo, err := file.Stat()
	if err != nil {
		return indexBackfillProgress{}, err
	}
	if !openedInfo.Mode().IsRegular() || !os.SameFile(info, openedInfo) || openedInfo.Size() != info.Size() {
		return indexBackfillProgress{}, errors.New("index backfill sidecar changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxIndexBackfillSidecarFileBytes+1))
	if err != nil {
		return indexBackfillProgress{}, err
	}
	if len(encoded) > maxIndexBackfillSidecarBytes {
		return indexBackfillProgress{}, errors.New("index backfill sidecar exceeds maximum size")
	}
	progress, err := decodeIndexBackfillProgress(encoded, sourcePrefix)
	if err != nil {
		return indexBackfillProgress{}, err
	}
	return progress, nil
}

func writeIndexBackfillProgress(dir, name string, progress indexBackfillProgress) error {
	if validationErr := validateIndexBackfillDirectory(dir); validationErr != nil {
		return validationErr
	}
	encoded, encodeErr := encodeIndexBackfillProgress(progress)
	if encodeErr != nil {
		return encodeErr
	}
	targetPath := filepath.Join(dir, name)
	if info, statErr := os.Lstat(targetPath); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
			return errors.New("index backfill sidecar target must be a regular non-symlink file")
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	// A fixed same-directory temp name makes a crash leave at most one orphan.
	// We never follow or overwrite an unexpected temp object.
	tempPath := targetPath + ".tmp"
	if info, statErr := os.Lstat(tempPath); statErr == nil {
		if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() > maxIndexBackfillSidecarFileBytes {
			return errors.New("index backfill sidecar temp path is unsafe")
		}
		if removeErr := os.Remove(tempPath); removeErr != nil {
			return fmt.Errorf("remove stale index backfill sidecar temp: %w", removeErr)
		}
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}

	temp, openErr := os.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) //nolint:gosec // fixed file inside validated private DB directory
	if openErr != nil {
		return openErr
	}
	tempOpen := true
	committed := false
	defer func() {
		if tempOpen {
			_ = temp.Close()
		}
		if !committed {
			_ = os.Remove(tempPath)
		}
	}()
	if chmodErr := temp.Chmod(0o600); chmodErr != nil {
		return chmodErr
	}
	written, copyErr := io.Copy(temp, bytes.NewReader(encoded))
	if copyErr != nil {
		return copyErr
	}
	if written != int64(len(encoded)) {
		return io.ErrShortWrite
	}
	if syncErr := temp.Sync(); syncErr != nil {
		return syncErr
	}
	if closeErr := temp.Close(); closeErr != nil {
		return closeErr
	}
	tempOpen = false
	if replaceErr := replaceIndexBackfillSidecarDurably(tempPath, targetPath); replaceErr != nil {
		return replaceErr
	}
	committed = true
	return nil
}

// InvalidateIndexBackfillProgress durably resets both local migration cursors
// to the beginning. The caller must hold exclusive ownership of the Badger
// directory (automatic binary rollback calls this after the node has closed its
// database). It is exported so rollback plumbing cannot accidentally preserve
// a completion claim across a downgrade to a binary predating either index.
func InvalidateIndexBackfillProgress(dir string) error {
	if _, err := os.Lstat(dir); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	type sidecarSpec struct {
		name   string
		prefix []byte
	}
	specs := []sidecarSpec{
		{name: agentOrgsIndexBackfillSidecar, prefix: []byte("org_member:")},
		{name: orgNameIndexBackfillSidecar, prefix: []byte("org:")},
	}
	// Validate every existing record before mutating either. Corruption or a
	// symlink is a rollback blocker, never a reason to silently skip a migration.
	for _, spec := range specs {
		if _, err := readIndexBackfillProgress(dir, spec.name, spec.prefix); err != nil {
			return fmt.Errorf("validate %s progress before invalidation: %w", spec.name, err)
		}
	}
	for _, spec := range specs {
		if err := writeIndexBackfillProgress(dir, spec.name, indexBackfillProgress{}); err != nil {
			return fmt.Errorf("invalidate %s progress: %w", spec.name, err)
		}
	}
	return nil
}
