package store

import (
	"encoding/binary"
	"errors"
	"fmt"
	"strings"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/l33tdawg/sage/internal/lineage"
	"github.com/l33tdawg/sage/internal/taint"
)

// The zero policy is the existing1–512-byte contract. A legacyRootID may be
// created only after the canonical legacy envelope and live read policy have
// succeeded in this held read-only view. It never widens write APIs or parent
// reference decoding and is not exported to callers.
const maxHistoricalTaintReadIDBytes = 1<<16 - 1

type taintReadIdentityPolicy struct{ legacyRootID string }

func (s *BadgerStore) taintIdentityForAuthorizedEnvelope(memoryID string, state *MemoryDisclosureState) (taintReadIdentityPolicy, error) {
	var identity taintReadIdentityPolicy
	if len(memoryID) <= taint.MaxMemoryIDBytes {
		return identity, identity.validate(memoryID)
	}
	if s == nil || s.txn == nil || !s.taintServingReadOnly || state == nil {
		return identity, errors.New("historical identity requires an authorized canonical read snapshot")
	}
	if len(memoryID) > maxHistoricalTaintReadIDBytes || strings.TrimSpace(memoryID) == "" {
		return identity, fmt.Errorf("%w: invalid historical memory identity", ErrTaintServingState)
	}
	identity.legacyRootID = memoryID
	return identity, nil
}

func (identity taintReadIdentityPolicy) validate(memoryID string) error {
	if len(memoryID) > 0 && len(memoryID) <= taint.MaxMemoryIDBytes {
		return nil
	}
	if memoryID == identity.legacyRootID && len(memoryID) > taint.MaxMemoryIDBytes && len(memoryID) <= maxHistoricalTaintReadIDBytes && strings.TrimSpace(memoryID) != "" {
		return nil
	}
	return errors.New("memory identity is outside this read policy")
}

// The authorized envelope already proved this exact physical memory key.
// Recheck its existence in the same view under the mandatory request meter;
// an absent arbitrary ID can never create a wide-ID read capability. Badger's
// persisted-key bound is consequently stricter than the uint16 framing cap.
func (identity taintReadIdentityPolicy) validatePhysicalRootTxn(txn *badger.Txn, meter *taintUnitMeter) error {
	if identity.legacyRootID == "" {
		return nil
	}
	if err := identity.validate(identity.legacyRootID); err != nil {
		return err
	}
	if err := chargeTaintUnits(meter, 1); err != nil {
		return err
	}
	item, err := txn.Get(memoryKey(identity.legacyRootID))
	if err != nil {
		return fmt.Errorf("%w: historical canonical identity unavailable: %w", ErrTaintServingState, err)
	}
	if len(item.Key()) != len(memoryKey(identity.legacyRootID)) {
		return fmt.Errorf("%w: historical canonical key mismatch", ErrTaintServingState)
	}
	return nil
}

func historicalIdentityPrefix(namespace, memoryID string) []byte {
	prefix := make([]byte, 0, len(namespace)+2+len(memoryID)+1)
	prefix = append(prefix, namespace...)
	prefix = binary.BigEndian.AppendUint16(prefix, uint16(len(memoryID))) // #nosec G115 -- caller policy validates at most MaxUint16
	prefix = append(prefix, memoryID...)
	return append(prefix, ':')
}

func (identity taintReadIdentityPolicy) membershipPrefix(memoryID string) ([]byte, error) {
	if err := identity.validate(memoryID); err != nil {
		return nil, err
	}
	if len(memoryID) <= taint.MaxMemoryIDBytes {
		return taint.MembershipPrefix(memoryID), nil
	}
	return historicalIdentityPrefix("taintm:", memoryID), nil
}

func (identity taintReadIdentityPolicy) forwardPrefix(memoryID string) ([]byte, error) {
	if err := identity.validate(memoryID); err != nil {
		return nil, err
	}
	if len(memoryID) <= taint.MaxMemoryIDBytes {
		return lineage.ForwardPrefix(memoryID), nil
	}
	return historicalIdentityPrefix("lineage:", memoryID), nil
}

func (identity taintReadIdentityPolicy) validateLineageIDs(memoryID, parentID string) error {
	if err := identity.validate(memoryID); err != nil {
		return err
	}
	// The child may be the one verified legacy root; the authenticated ParentRef
	// still goes through its unchanged512-byte codec and exact hash checks.
	if len(memoryID) > taint.MaxMemoryIDBytes {
		return lineage.ValidateParentID(parentID)
	}
	return lineage.ValidateIDs(memoryID, parentID)
}
