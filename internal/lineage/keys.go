package lineage

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"

	"github.com/l33tdawg/sage/internal/tx"
)

const ParentIDBytes = 32

func ForwardPrefix(childID string) []byte {
	// Invalid callers cannot wrap a length into another valid child's prefix.
	// Store entry points reject this input; recovery rejects the zero length.
	if len(childID) > tx.MaxDerivedFromMemoryIDBytes {
		return []byte("lineage:\x00\x00:")
	}
	// Length-delimit the arbitrary child ID so another child's edges cannot
	// enter this bounded scan. This layout is only used by dormant app-v30.
	prefix := make([]byte, 0, len("lineage:")+2+len(childID)+1)
	prefix = append(prefix, "lineage:"...)
	prefix = binary.BigEndian.AppendUint16(prefix, uint16(len(childID))) // #nosec G115 -- validated IDs are at most MaxDerivedFromMemoryIDBytes
	prefix = append(prefix, childID...)
	return append(prefix, ':')
}

func ForwardKey(childID, parentID string) []byte {
	return append(ForwardPrefix(childID), parentID...)
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
