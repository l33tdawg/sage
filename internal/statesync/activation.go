package statesync

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

const (
	activationJournalVersion  byte = 1
	maxActivationNameBytes         = 255
	activationJournalHeader        = 12 + 1 + 1 + 8 + sha256.Size + sha256.Size
	maxActivationJournalBytes      = activationJournalHeader + 3*(2+maxActivationNameBytes)
)

var activationJournalMagic = [12]byte{'S', 'A', 'G', 'E', '-', 'A', 'C', 'T', 'I', 'V', 'E', 0}

// ActivationPhase is the durable portion of the boot-only activation state
// machine. Discovery and assembly are disposable staging work and need no
// journal; these phases straddle live-directory and CometBFT persistence.
type ActivationPhase byte

const (
	ActivationPrepared ActivationPhase = iota + 1
	ActivationPendingComet
	ActivationSealed
)

// ActivationJournal is the key-free recovery record for one verified state
// sync activation. Directory fields are basenames beneath a caller-owned root,
// never arbitrary paths. AppHash and MetadataHash are immutable SHA-256 values.
type ActivationJournal struct {
	Phase          ActivationPhase
	Height         uint64
	AppHash        []byte
	MetadataHash   []byte
	PreparedName   string
	QuarantineName string
	LiveName       string
}

// RecoveryAction tells startup which directory layout can be considered. The
// caller must still verify the selected Badger database against CometBFT before
// any rename or normal service startup.
type RecoveryAction byte

const (
	RecoveryDiscardPrepared RecoveryAction = iota + 1
	RecoveryRestoreQuarantine
	RecoveryKeepActivated
)

func EncodeActivationJournal(journal ActivationJournal) ([]byte, error) {
	if err := validateActivationJournal(journal); err != nil {
		return nil, err
	}
	encoded := make([]byte, activationJournalHeader)
	offset := 0
	copy(encoded[offset:], activationJournalMagic[:])
	offset += len(activationJournalMagic)
	encoded[offset] = activationJournalVersion
	offset++
	encoded[offset] = byte(journal.Phase)
	offset++
	binary.BigEndian.PutUint64(encoded[offset:], journal.Height)
	offset += 8
	copy(encoded[offset:], journal.AppHash)
	offset += sha256.Size
	copy(encoded[offset:], journal.MetadataHash)
	for _, name := range []string{journal.PreparedName, journal.QuarantineName, journal.LiveName} {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(name))) // #nosec G115 -- validateActivationJournal bounds to 255
		encoded = append(encoded, length[:]...)
		encoded = append(encoded, name...)
	}
	return encoded, nil
}

func DecodeActivationJournal(encoded []byte) (ActivationJournal, error) {
	if len(encoded) < activationJournalHeader+6 || len(encoded) > maxActivationJournalBytes {
		return ActivationJournal{}, errors.New("activation journal size is invalid")
	}
	offset := 0
	if !bytes.Equal(encoded[:len(activationJournalMagic)], activationJournalMagic[:]) {
		return ActivationJournal{}, errors.New("activation journal magic mismatch")
	}
	offset += len(activationJournalMagic)
	if encoded[offset] != activationJournalVersion {
		return ActivationJournal{}, fmt.Errorf("unsupported activation journal version %d", encoded[offset])
	}
	offset++
	journal := ActivationJournal{Phase: ActivationPhase(encoded[offset])}
	offset++
	journal.Height = binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	journal.AppHash = append([]byte(nil), encoded[offset:offset+sha256.Size]...)
	offset += sha256.Size
	journal.MetadataHash = append([]byte(nil), encoded[offset:offset+sha256.Size]...)
	offset += sha256.Size
	names := []*string{&journal.PreparedName, &journal.QuarantineName, &journal.LiveName}
	for _, target := range names {
		if len(encoded)-offset < 2 {
			return ActivationJournal{}, errors.New("activation journal name length is truncated")
		}
		length := int(binary.BigEndian.Uint16(encoded[offset:]))
		offset += 2
		if length == 0 || length > maxActivationNameBytes || len(encoded)-offset < length {
			return ActivationJournal{}, errors.New("activation journal name is invalid")
		}
		*target = string(encoded[offset : offset+length])
		offset += length
	}
	if offset != len(encoded) {
		return ActivationJournal{}, errors.New("activation journal has trailing bytes")
	}
	if err := validateActivationJournal(journal); err != nil {
		return ActivationJournal{}, err
	}
	return journal, nil
}

// WriteActivationJournal atomically replaces path with a canonical, fsynced
// record. Temporary files live in the same directory so rename is atomic.
func WriteActivationJournal(path string, journal ActivationJournal) error {
	if path == "" {
		return errors.New("activation journal path is empty")
	}
	encoded, err := EncodeActivationJournal(journal)
	if err != nil {
		return err
	}
	dir := filepath.Dir(path)
	info, err := os.Lstat(dir)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("activation journal parent must be an existing real directory")
	}
	tmp, err := os.CreateTemp(dir, ".activation-journal-")
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
	if _, err := tmp.Write(encoded); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	ok = true
	return nil
}

func LoadActivationJournal(path string) (ActivationJournal, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return ActivationJournal{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > maxActivationJournalBytes {
		return ActivationJournal{}, errors.New("activation journal must be a bounded regular file")
	}
	file, err := os.Open(path) //nolint:gosec // lstat-checked operator-owned path
	if err != nil {
		return ActivationJournal{}, err
	}
	defer func() { _ = file.Close() }()
	encoded, err := io.ReadAll(io.LimitReader(file, maxActivationJournalBytes+1))
	if err != nil {
		return ActivationJournal{}, err
	}
	return DecodeActivationJournal(encoded)
}

// DecideActivationRecovery closes the app-ahead-of-Comet crash window without
// touching disk. liveHeight/liveAppHash describe the currently named live
// Badger directory. A restore decision still requires the caller to verify the
// quarantined store against CometBFT before renaming it.
func DecideActivationRecovery(journal ActivationJournal, cometHeight uint64, cometAppHash []byte, liveHeight uint64, liveAppHash []byte) (RecoveryAction, error) {
	if err := validateActivationJournal(journal); err != nil {
		return 0, err
	}
	if !validPersistedState(cometHeight, cometAppHash) || !validPersistedState(liveHeight, liveAppHash) {
		return 0, errors.New("activation recovery state hash is invalid")
	}

	switch journal.Phase {
	case ActivationPrepared:
		if cometHeight != liveHeight || !bytes.Equal(cometAppHash, liveAppHash) {
			return 0, errors.New("prepared activation live state does not match CometBFT")
		}
		return RecoveryDiscardPrepared, nil

	case ActivationPendingComet:
		if cometHeight < journal.Height {
			return RecoveryRestoreQuarantine, nil
		}
		fallthrough

	case ActivationSealed:
		if cometHeight < journal.Height {
			return 0, errors.New("sealed activation is ahead of CometBFT")
		}
		if cometHeight == journal.Height && !bytes.Equal(cometAppHash, journal.AppHash) {
			return 0, errors.New("CometBFT snapshot AppHash does not match activation journal")
		}
		if liveHeight != cometHeight || !bytes.Equal(liveAppHash, cometAppHash) {
			return 0, errors.New("activated Badger state does not match CometBFT")
		}
		return RecoveryKeepActivated, nil
	default:
		return 0, errors.New("activation journal phase is invalid")
	}
}

func validateActivationJournal(journal ActivationJournal) error {
	if journal.Phase != ActivationPrepared && journal.Phase != ActivationPendingComet && journal.Phase != ActivationSealed {
		return errors.New("activation journal phase is invalid")
	}
	if journal.Height == 0 {
		return errors.New("activation journal height must be positive")
	}
	if len(journal.AppHash) != sha256.Size || len(journal.MetadataHash) != sha256.Size {
		return errors.New("activation journal hashes must be SHA-256 sized")
	}
	for _, name := range []string{journal.PreparedName, journal.QuarantineName, journal.LiveName} {
		if len(name) == 0 || len(name) > maxActivationNameBytes || name == "." || name == ".." ||
			filepath.Base(name) != name || strings.IndexByte(name, 0) >= 0 {
			return errors.New("activation journal directory name is unsafe")
		}
	}
	if journal.PreparedName == journal.QuarantineName || journal.PreparedName == journal.LiveName || journal.QuarantineName == journal.LiveName {
		return errors.New("activation journal directory names must be distinct")
	}
	return nil
}

func validPersistedState(height uint64, appHash []byte) bool {
	if height == 0 {
		return len(appHash) == 0
	}
	return len(appHash) == sha256.Size
}
