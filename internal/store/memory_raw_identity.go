package store

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/tx"
)

// GetMemoryDisclosureStateAppV30 reads a raw 1–512-byte memory identity without
// trimming or decoding it. Consensus callers must select this method only at
// the strict app-v30 H+1 boundary. It does not establish activation or authorize
// a serving request. All envelope decoding and integrity checks remain shared
// with the historical reader.
func (s *BadgerStore) GetMemoryDisclosureStateAppV30(memoryID string) (*MemoryDisclosureState, error) {
	if err := validateAppV30RawMemoryID(memoryID); err != nil {
		return nil, err
	}
	return s.getMemoryDisclosureStateExact(memoryID)
}

// SetMemoryStatusPreservingHashAppV30 accepts the same exact identity bytes as
// the app-v30 reader. It retains the existing status validation, transaction,
// missing/malformed/hash checks, and hash-preserving write. Consensus callers
// must enforce the app-v30 H+1 gate; legacy status updates keep their old API.
func (s *BadgerStore) SetMemoryStatusPreservingHashAppV30(memoryID, status string) error {
	if err := validateAppV30RawMemoryID(memoryID); err != nil {
		return err
	}
	return s.setMemoryStatusPreservingHashExact(memoryID, status)
}

func validateAppV30RawMemoryID(memoryID string) error {
	if len(memoryID) == 0 || len(memoryID) > tx.MaxDerivedFromMemoryIDBytes {
		return fmt.Errorf("app-v30 memory identity must contain 1–512 raw bytes")
	}
	return nil
}
