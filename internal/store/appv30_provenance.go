package store

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/tx"
)

const (
	maxAppV30ProvenanceSigners = 64
	appV30ProvenanceSignerSize = canonicalAgentIDHexBytes + canonicalAgentIDHexBytes + 8 + 1
)

var ErrAppV30ProvenanceConflict = errors.New("app-v30 memory provenance conflicts with existing state")

type AppV30ProvenanceSigner struct {
	CredentialID   string
	PrincipalID    string
	RootGeneration uint64
	Exact          bool
}

func AppV30ProvenanceKey(memoryID string) []byte {
	return []byte("memprov:v30:" + memoryID)
}

func appV30MigrationMarkerKey(memoryID string) []byte {
	return []byte("memprov:migrated:" + memoryID)
}

// SetAppV30Provenance installs the immutable creation-time signer set for one
// app-v30 memory. Exact replay is idempotent; any different existing set is
// rejected.
func (s *BadgerStore) SetAppV30Provenance(
	memoryID string,
	signers []AppV30ProvenanceSigner,
) error {
	encoded, err := encodeAppV30Provenance(signers)
	if err != nil {
		return err
	}
	if len(memoryID) == 0 || len(memoryID) > tx.MaxDerivedFromMemoryIDBytes {
		return fmt.Errorf("app-v30 provenance memory ID length %d is invalid", len(memoryID))
	}
	key := AppV30ProvenanceKey(memoryID)
	return s.update(func(txn *badger.Txn) error {
		existing, found, err := readTaintValueTxn(txn, key)
		if err != nil {
			return err
		}
		if found {
			if !bytes.Equal(existing, encoded) {
				return ErrAppV30ProvenanceConflict
			}
			return nil
		}
		return s.txnSet(txn, key, encoded)
	})
}

func (s *BadgerStore) GetAppV30Provenance(
	memoryID string,
) ([]AppV30ProvenanceSigner, bool, error) {
	if len(memoryID) == 0 || len(memoryID) > tx.MaxDerivedFromMemoryIDBytes {
		return nil, false, fmt.Errorf("app-v30 provenance memory ID length %d is invalid", len(memoryID))
	}
	var signers []AppV30ProvenanceSigner
	found := false
	err := s.view(func(txn *badger.Txn) error {
		raw, exists, err := readTaintValueTxn(txn, AppV30ProvenanceKey(memoryID))
		if err != nil || !exists {
			return err
		}
		signers, err = decodeAppV30Provenance(raw)
		found = err == nil
		return err
	})
	return signers, found, err
}

func (s *BadgerStore) ValidateAppV30Provenance(
	memoryID string,
	signers []AppV30ProvenanceSigner,
) error {
	encoded, err := encodeAppV30Provenance(signers)
	if err != nil {
		return err
	}
	if len(memoryID) == 0 || len(memoryID) > tx.MaxDerivedFromMemoryIDBytes {
		return fmt.Errorf("app-v30 provenance memory ID length %d is invalid", len(memoryID))
	}
	return s.view(func(txn *badger.Txn) error {
		existing, found, err := readTaintValueTxn(txn, AppV30ProvenanceKey(memoryID))
		if err != nil {
			return err
		}
		if found && !bytes.Equal(existing, encoded) {
			return ErrAppV30ProvenanceConflict
		}
		return nil
	})
}

// HasAppV30MigrationMarker distinguishes records copied by the app-v30
// public-memory migration from records submitted natively on this chain.
func (s *BadgerStore) HasAppV30MigrationMarker(memoryID string) (bool, error) {
	if len(memoryID) == 0 || len(memoryID) > tx.MaxDerivedFromMemoryIDBytes {
		return false, fmt.Errorf("app-v30 migration marker memory ID length %d is invalid", len(memoryID))
	}
	found := false
	err := s.view(func(txn *badger.Txn) error {
		raw, exists, err := readTaintValueTxn(txn, appV30MigrationMarkerKey(memoryID))
		if err != nil || !exists {
			return err
		}
		if !bytes.Equal(raw, []byte{1}) {
			return fmt.Errorf("app-v30 migration marker is malformed for %s", memoryID)
		}
		found = true
		return nil
	})
	return found, err
}

func (s *BadgerStore) copyAppV30MigrationMetadata(
	memoryID string,
	provenance, adoptionReceipt []byte,
) error {
	if len(memoryID) == 0 || len(memoryID) > tx.MaxDerivedFromMemoryIDBytes {
		return fmt.Errorf("app-v30 migration memory ID length %d is invalid", len(memoryID))
	}
	if len(provenance) > 0 {
		if _, err := decodeAppV30Provenance(provenance); err != nil {
			return fmt.Errorf("copy app-v30 provenance for %s: %w", memoryID, err)
		}
	}
	if len(adoptionReceipt) > 0 && len(adoptionReceipt) != sha256.Size {
		return fmt.Errorf("copy adoption receipt for %s: invalid receipt length %d", memoryID, len(adoptionReceipt))
	}
	return s.update(func(txn *badger.Txn) error {
		if len(provenance) > 0 {
			if err := s.txnSet(txn, AppV30ProvenanceKey(memoryID), provenance); err != nil {
				return err
			}
		}
		if len(adoptionReceipt) > 0 {
			if err := s.txnSet(txn, memoryLegacyAdoptionReceiptKey(memoryID), adoptionReceipt); err != nil {
				return err
			}
		}
		return s.txnSet(txn, appV30MigrationMarkerKey(memoryID), []byte{1})
	})
}

func encodeAppV30Provenance(signers []AppV30ProvenanceSigner) ([]byte, error) {
	if len(signers) == 0 || len(signers) > maxAppV30ProvenanceSigners {
		return nil, fmt.Errorf(
			"app-v30 provenance signer count %d is outside 1..%d",
			len(signers),
			maxAppV30ProvenanceSigners,
		)
	}
	canonical := append([]AppV30ProvenanceSigner(nil), signers...)
	sort.Slice(canonical, func(i, j int) bool {
		return compareAppV30ProvenanceSigners(canonical[i], canonical[j]) < 0
	})
	encoded := make([]byte, 0, 1+len(canonical)*appV30ProvenanceSignerSize)
	encoded = append(encoded, byte(len(canonical)))
	for i, signer := range canonical {
		if !isCanonicalAgentID(signer.CredentialID) ||
			!isCanonicalAgentID(signer.PrincipalID) {
			return nil, fmt.Errorf("app-v30 provenance signer %d has a non-canonical identity", i)
		}
		if !signer.Exact {
			return nil, fmt.Errorf("app-v30 provenance signer %d is not exact", i)
		}
		if i > 0 && compareAppV30ProvenanceSigners(canonical[i-1], signer) == 0 {
			return nil, fmt.Errorf("app-v30 provenance signer %d is duplicated", i)
		}
		encoded = append(encoded, signer.CredentialID...)
		encoded = append(encoded, signer.PrincipalID...)
		encoded = binary.BigEndian.AppendUint64(encoded, signer.RootGeneration)
		encoded = append(encoded, 1)
	}
	return encoded, nil
}

func decodeAppV30Provenance(encoded []byte) ([]AppV30ProvenanceSigner, error) {
	if len(encoded) < 1 {
		return nil, fmt.Errorf("app-v30 provenance is empty")
	}
	count := int(encoded[0])
	if count == 0 || count > maxAppV30ProvenanceSigners {
		return nil, fmt.Errorf("app-v30 provenance signer count %d is invalid", count)
	}
	if len(encoded) != 1+count*appV30ProvenanceSignerSize {
		return nil, fmt.Errorf("app-v30 provenance length %d is invalid", len(encoded))
	}
	signers := make([]AppV30ProvenanceSigner, 0, count)
	offset := 1
	for i := 0; i < count; i++ {
		signer := AppV30ProvenanceSigner{
			CredentialID: string(encoded[offset : offset+canonicalAgentIDHexBytes]),
			PrincipalID:  string(encoded[offset+canonicalAgentIDHexBytes : offset+2*canonicalAgentIDHexBytes]),
		}
		offset += 2 * canonicalAgentIDHexBytes
		signer.RootGeneration = binary.BigEndian.Uint64(encoded[offset:])
		offset += 8
		if encoded[offset] != 1 {
			return nil, fmt.Errorf("app-v30 provenance signer %d is not exact", i)
		}
		signer.Exact = true
		offset++
		if !isCanonicalAgentID(signer.CredentialID) ||
			!isCanonicalAgentID(signer.PrincipalID) {
			return nil, fmt.Errorf("app-v30 provenance signer %d has a non-canonical identity", i)
		}
		if i > 0 && compareAppV30ProvenanceSigners(signers[i-1], signer) >= 0 {
			return nil, fmt.Errorf("app-v30 provenance signers are not canonically sorted")
		}
		signers = append(signers, signer)
	}
	return signers, nil
}

func compareAppV30ProvenanceSigners(a, b AppV30ProvenanceSigner) int {
	if comparison := bytes.Compare([]byte(a.CredentialID), []byte(b.CredentialID)); comparison != 0 {
		return comparison
	}
	if comparison := bytes.Compare([]byte(a.PrincipalID), []byte(b.PrincipalID)); comparison != 0 {
		return comparison
	}
	switch {
	case a.RootGeneration < b.RootGeneration:
		return -1
	case a.RootGeneration > b.RootGeneration:
		return 1
	case !a.Exact && b.Exact:
		return -1
	case a.Exact && !b.Exact:
		return 1
	default:
		return 0
	}
}
