package lineage

import (
	"encoding/hex"
	"fmt"

	"github.com/l33tdawg/sage/internal/tx"
)

const ParentIDBytes = 32

func ForwardPrefix(childID string) []byte {
	return []byte("lineage:" + childID + ":")
}

func ForwardKey(childID, parentID string) []byte {
	return []byte("lineage:" + childID + ":" + parentID)
}

func ReversePrefix(parentID string) []byte {
	return []byte("lineage-ref:" + parentID + ":")
}

func ReverseKey(parentID, childID string) []byte {
	return []byte("lineage-ref:" + parentID + ":" + childID)
}

// ValidateIDs validates the two fixed/variable key components used by the
// lineage indexes.
func ValidateIDs(childID, parentID string) error {
	if err := ValidateChildID(childID); err != nil {
		return err
	}
	return ValidateParentID(parentID)
}

func ValidateChildID(childID string) error {
	if len(childID) == 0 || len(childID) > tx.MaxDerivedFromMemoryIDBytes {
		return fmt.Errorf("lineage child ID length %d is outside 1..%d",
			len(childID), tx.MaxDerivedFromMemoryIDBytes)
	}
	return nil
}

func ValidateParentID(parentID string) error {
	decoded, err := hex.DecodeString(parentID)
	if err != nil || len(decoded) != ParentIDBytes || hex.EncodeToString(decoded) != parentID {
		return fmt.Errorf("lineage parent ID must be lowercase %d-byte hex", ParentIDBytes)
	}
	return nil
}
