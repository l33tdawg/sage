package tx

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
)

const (
	DerivedFromExtensionVersion  uint8 = 1
	MaxDerivedFromParents              = 16
	MaxDerivedFromChainIDBytes         = 64
	MaxDerivedFromMemoryIDBytes        = 512
	MaxDerivedFromExtensionBytes       = 10 << 10
)

const derivedFromParentRefDomain = "sage/lineage/parent-ref/v1\x00"

var (
	errDerivedFromExtension       = errors.New("invalid derived_from extension")
	memorySubmitDerivedFromMarker = []byte("SAGE-MEM-DERIVED-FROM-V1\x00")
)

// ParentRef is one immutable parent assertion in a MemorySubmit. The content
// hash commits to the exact parent content; the memory ID alone is not enough.
type ParentRef struct {
	SourceChainID string
	MemoryID      string
	ContentHash   [sha256.Size]byte
}

// SortParentRefs canonicalizes refs by the raw tuple
// (source_chain_id, memory_id, content_hash).
func SortParentRefs(refs []ParentRef) {
	sort.Slice(refs, func(i, j int) bool {
		return compareParentRefs(refs[i], refs[j]) < 0
	})
}

// EncodeDerivedFromExtension returns the complete trailing MemorySubmit
// extension, including its marker.
func EncodeDerivedFromExtension(refs []ParentRef) ([]byte, error) {
	if err := validateParentRefs(refs); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, len(memorySubmitDerivedFromMarker)+2+len(refs)*(2+64+2+512+sha256.Size))
	encoded = append(encoded, memorySubmitDerivedFromMarker...)
	encoded = append(encoded, DerivedFromExtensionVersion, uint8(len(refs)))
	for _, ref := range refs {
		encoded = appendDerivedFromBytes(encoded, []byte(ref.SourceChainID))
		encoded = appendDerivedFromBytes(encoded, []byte(ref.MemoryID))
		encoded = append(encoded, ref.ContentHash[:]...)
	}
	if len(encoded) > MaxDerivedFromExtensionBytes {
		return nil, fmt.Errorf(
			"%w: encoded size %d exceeds %d bytes",
			errDerivedFromExtension, len(encoded), MaxDerivedFromExtensionBytes,
		)
	}
	return encoded, nil
}

// DecodeDerivedFromExtension accepts only the canonical v1 extension.
func DecodeDerivedFromExtension(encoded []byte) ([]ParentRef, error) {
	if len(encoded) == 0 || len(encoded) > MaxDerivedFromExtensionBytes {
		return nil, fmt.Errorf("%w: invalid encoded size %d", errDerivedFromExtension, len(encoded))
	}
	if !bytes.HasPrefix(encoded, memorySubmitDerivedFromMarker) {
		return nil, fmt.Errorf("%w: missing marker", errDerivedFromExtension)
	}
	offset := len(memorySubmitDerivedFromMarker)
	if len(encoded)-offset < 2 {
		return nil, fmt.Errorf("%w: truncated version or count", errDerivedFromExtension)
	}
	version := encoded[offset]
	offset++
	if version != DerivedFromExtensionVersion {
		return nil, fmt.Errorf("%w: unsupported version %d", errDerivedFromExtension, version)
	}
	count := int(encoded[offset])
	offset++
	if count == 0 || count > MaxDerivedFromParents {
		return nil, fmt.Errorf("%w: parent count %d is outside 1..%d",
			errDerivedFromExtension, count, MaxDerivedFromParents)
	}
	refs := make([]ParentRef, 0, count)
	for i := 0; i < count; i++ {
		sourceChainID, next, err := readDerivedFromBytes(
			encoded, offset, MaxDerivedFromChainIDBytes, fmt.Sprintf("parent %d chain ID", i),
		)
		if err != nil {
			return nil, err
		}
		offset = next
		memoryID, next, err := readDerivedFromBytes(
			encoded, offset, MaxDerivedFromMemoryIDBytes, fmt.Sprintf("parent %d memory ID", i),
		)
		if err != nil {
			return nil, err
		}
		offset = next
		if len(encoded)-offset < sha256.Size {
			return nil, fmt.Errorf("%w: truncated parent %d content hash", errDerivedFromExtension, i)
		}
		ref := ParentRef{SourceChainID: string(sourceChainID), MemoryID: string(memoryID)}
		copy(ref.ContentHash[:], encoded[offset:offset+sha256.Size])
		offset += sha256.Size
		refs = append(refs, ref)
	}
	if offset != len(encoded) {
		return nil, fmt.Errorf("%w: %d trailing bytes", errDerivedFromExtension, len(encoded)-offset)
	}
	if err := validateParentRefs(refs); err != nil {
		return nil, err
	}
	canonical, err := EncodeDerivedFromExtension(refs)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, encoded) {
		return nil, fmt.Errorf("%w: non-canonical encoding", errDerivedFromExtension)
	}
	return refs, nil
}

// ParentRefID returns the domain-separated 32-byte identity used by both
// lineage index directions.
func ParentRefID(ref ParentRef) ([sha256.Size]byte, error) {
	if err := validateParentRef(ref); err != nil {
		return [sha256.Size]byte{}, err
	}
	digest := sha256.New()
	_, _ = digest.Write([]byte(derivedFromParentRefDomain))
	_, _ = digest.Write(canonicalParentRef(ref))
	var id [sha256.Size]byte
	copy(id[:], digest.Sum(nil))
	return id, nil
}

// ParentRefIDHex returns the lowercase key-safe form of ParentRefID.
func ParentRefIDHex(ref ParentRef) (string, error) {
	id, err := ParentRefID(ref)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(id[:]), nil
}

// EncodeParentRefValue returns the canonical value stored on a forward lineage
// edge. It is the same length-prefixed tuple used by ParentRefID.
func EncodeParentRefValue(ref ParentRef) ([]byte, error) {
	if err := validateParentRef(ref); err != nil {
		return nil, err
	}
	return canonicalParentRef(ref), nil
}

// DecodeParentRefValue accepts only the canonical forward-edge value.
func DecodeParentRefValue(encoded []byte) (ParentRef, error) {
	sourceChainID, offset, err := readDerivedFromBytes(
		encoded, 0, MaxDerivedFromChainIDBytes, "parent source chain ID",
	)
	if err != nil {
		return ParentRef{}, err
	}
	memoryID, offset, err := readDerivedFromBytes(
		encoded, offset, MaxDerivedFromMemoryIDBytes, "parent memory ID",
	)
	if err != nil {
		return ParentRef{}, err
	}
	if len(encoded)-offset != sha256.Size {
		return ParentRef{}, fmt.Errorf("%w: parent value has %d trailing bytes",
			errDerivedFromExtension, len(encoded)-offset)
	}
	ref := ParentRef{SourceChainID: string(sourceChainID), MemoryID: string(memoryID)}
	copy(ref.ContentHash[:], encoded[offset:])
	return ref, nil
}

func validateParentRefs(refs []ParentRef) error {
	if len(refs) == 0 || len(refs) > MaxDerivedFromParents {
		return fmt.Errorf("%w: parent count %d is outside 1..%d",
			errDerivedFromExtension, len(refs), MaxDerivedFromParents)
	}
	for i, ref := range refs {
		if err := validateParentRef(ref); err != nil {
			return fmt.Errorf("parent %d: %w", i, err)
		}
		if i > 0 && compareParentRefs(refs[i-1], ref) >= 0 {
			return fmt.Errorf("%w: parents are not strictly canonical-sorted and unique", errDerivedFromExtension)
		}
	}
	return nil
}

func validateParentRef(ref ParentRef) error {
	if len(ref.SourceChainID) == 0 || len(ref.SourceChainID) > MaxDerivedFromChainIDBytes {
		return fmt.Errorf("%w: source chain ID length %d is outside 1..%d",
			errDerivedFromExtension, len(ref.SourceChainID), MaxDerivedFromChainIDBytes)
	}
	if len(ref.MemoryID) == 0 || len(ref.MemoryID) > MaxDerivedFromMemoryIDBytes {
		return fmt.Errorf("%w: memory ID length %d is outside 1..%d",
			errDerivedFromExtension, len(ref.MemoryID), MaxDerivedFromMemoryIDBytes)
	}
	return nil
}

func compareParentRefs(a, b ParentRef) int {
	if comparison := bytes.Compare([]byte(a.SourceChainID), []byte(b.SourceChainID)); comparison != 0 {
		return comparison
	}
	if comparison := bytes.Compare([]byte(a.MemoryID), []byte(b.MemoryID)); comparison != 0 {
		return comparison
	}
	return bytes.Compare(a.ContentHash[:], b.ContentHash[:])
}

func canonicalParentRef(ref ParentRef) []byte {
	encoded := make([]byte, 0, 2+len(ref.SourceChainID)+2+len(ref.MemoryID)+sha256.Size)
	encoded = appendDerivedFromBytes(encoded, []byte(ref.SourceChainID))
	encoded = appendDerivedFromBytes(encoded, []byte(ref.MemoryID))
	return append(encoded, ref.ContentHash[:]...)
}

func appendDerivedFromBytes(dst, value []byte) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(len(value))) // #nosec G115 -- validated caller bounds
	dst = append(dst, buf...)
	return append(dst, value...)
}

func readDerivedFromBytes(data []byte, offset, max int, field string) ([]byte, int, error) {
	if len(data)-offset < 2 {
		return nil, 0, fmt.Errorf("%w: truncated %s length", errDerivedFromExtension, field)
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if length == 0 || length > max {
		return nil, 0, fmt.Errorf("%w: %s length %d is outside 1..%d",
			errDerivedFromExtension, field, length, max)
	}
	if len(data)-offset < length {
		return nil, 0, fmt.Errorf("%w: truncated %s", errDerivedFromExtension, field)
	}
	return append([]byte(nil), data[offset:offset+length]...), offset + length, nil
}
