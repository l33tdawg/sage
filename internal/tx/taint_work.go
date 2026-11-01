package tx

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	MemoryTaintWorkVersion    uint8 = 1
	maxMemoryTaintCursorBytes       = 512
)

var errMemoryTaintWork = errors.New("invalid memory taint work")

func encodeMemoryTaintWork(work *MemoryTaintWork) ([]byte, error) {
	if err := validateMemoryTaintWork(work); err != nil {
		return nil, err
	}
	encoded := []byte{work.Version, byte(work.Kind)}
	encoded = appendBytes(encoded, []byte(work.DecisionID))
	encoded = binary.BigEndian.AppendUint64(encoded, work.Generation)
	encoded = binary.BigEndian.AppendUint64(encoded, work.FrontierHead)
	encoded = append(encoded, work.CleanupPhase)
	encoded = appendBytes(encoded, []byte(work.ParentRefID))
	encoded = appendBytes(encoded, []byte(work.Cursor))
	return encoded, nil
}

func decodeMemoryTaintWork(data []byte) (*MemoryTaintWork, error) {
	if len(data) < 2 {
		return nil, fmt.Errorf("%w: truncated header", errMemoryTaintWork)
	}
	work := &MemoryTaintWork{
		Version: data[0],
		Kind:    MemoryTaintWorkKind(data[1]),
	}
	offset := 2
	decisionID, next, err := readBytes(data, offset)
	if err != nil {
		return nil, fmt.Errorf("%w: decision ID", errMemoryTaintWork)
	}
	work.DecisionID = string(decisionID)
	offset = next
	if len(data)-offset < 17 {
		return nil, fmt.Errorf("%w: truncated cursor state", errMemoryTaintWork)
	}
	work.Generation = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	work.FrontierHead = binary.BigEndian.Uint64(data[offset:])
	offset += 8
	work.CleanupPhase = data[offset]
	offset++
	parentRefID, next, err := readBytes(data, offset)
	if err != nil {
		return nil, fmt.Errorf("%w: parent ref ID", errMemoryTaintWork)
	}
	work.ParentRefID = string(parentRefID)
	offset = next
	cursor, next, err := readBytes(data, offset)
	if err != nil {
		return nil, fmt.Errorf("%w: cursor", errMemoryTaintWork)
	}
	work.Cursor = string(cursor)
	offset = next
	if offset != len(data) {
		return nil, fmt.Errorf("%w: %d trailing bytes", errMemoryTaintWork, len(data)-offset)
	}
	if err := validateMemoryTaintWork(work); err != nil {
		return nil, err
	}
	canonical, err := encodeMemoryTaintWork(work)
	if err != nil {
		return nil, err
	}
	if !bytes.Equal(canonical, data) {
		return nil, fmt.Errorf("%w: non-canonical encoding", errMemoryTaintWork)
	}
	return work, nil
}

func validateMemoryTaintWork(work *MemoryTaintWork) error {
	if work == nil {
		return fmt.Errorf("%w: missing payload", errMemoryTaintWork)
	}
	if work.Version != MemoryTaintWorkVersion {
		return fmt.Errorf("%w: unsupported version %d", errMemoryTaintWork, work.Version)
	}
	if err := validateLowerHexID(work.DecisionID, 32); err != nil {
		return fmt.Errorf("%w: decision ID: %v", errMemoryTaintWork, err)
	}
	if work.Generation == 0 {
		return fmt.Errorf("%w: generation must be positive", errMemoryTaintWork)
	}
	if len(work.Cursor) > maxMemoryTaintCursorBytes {
		return fmt.Errorf("%w: cursor length %d exceeds %d", errMemoryTaintWork, len(work.Cursor), maxMemoryTaintCursorBytes)
	}
	switch work.Kind {
	case MemoryTaintWorkExpand:
		if work.CleanupPhase != 0 {
			return fmt.Errorf("%w: expansion cannot carry a cleanup phase", errMemoryTaintWork)
		}
		if work.ParentRefID != "" {
			if err := validateLowerHexID(work.ParentRefID, 32); err != nil {
				return fmt.Errorf("%w: parent ref ID: %v", errMemoryTaintWork, err)
			}
		}
	case MemoryTaintWorkCleanup:
		if work.FrontierHead != 0 || work.ParentRefID != "" {
			return fmt.Errorf("%w: cleanup cannot carry an expansion cursor", errMemoryTaintWork)
		}
		if work.CleanupPhase != 1 && work.CleanupPhase != 2 {
			return fmt.Errorf("%w: invalid cleanup phase %d", errMemoryTaintWork, work.CleanupPhase)
		}
	default:
		return fmt.Errorf("%w: unsupported kind %d", errMemoryTaintWork, work.Kind)
	}
	return nil
}

func validateLowerHexID(value string, length int) error {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != length || hex.EncodeToString(decoded) != value {
		return fmt.Errorf("must be lowercase %d-byte hex", length)
	}
	return nil
}
