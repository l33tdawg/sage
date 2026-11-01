package taint

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	MaxActiveDecisions       = 32
	MaxParents               = 16
	MaxSigners               = 64
	MaxServingAncestorWalk   = 256
	MaxDepth                 = 64
	PerExecutionUnitBudget   = 1024
	MaxAdmissionTaintUnits   = 625
	MaxNewMembershipsPerExec = 204
	MaxExpansionMemberships  = 16384
	MaxPerBlockBytes         = 4 << 20
	MaxServingRequestUnits   = 65536
	MaxMemoryIDBytes         = 512
	MaxCursorBytes           = 512
	DecisionIDBytes          = 64
)

const minimumHeaderBytes = 78

type Status uint8

const (
	StatusExpanding Status = iota + 1
	StatusComplete
	StatusDepthExhausted
	StatusWorkExhausted
	StatusCleaning
	StatusClean
)

func (s Status) Active() bool {
	return s == StatusExpanding || s == StatusComplete ||
		s == StatusDepthExhausted || s == StatusWorkExhausted
}

func (s Status) Incomplete() bool {
	return s == StatusExpanding || s == StatusDepthExhausted ||
		s == StatusWorkExhausted
}

func (s Status) Valid() bool {
	return s >= StatusExpanding && s <= StatusClean
}

type Action uint8

const (
	ActionApply Action = 1
	ActionClear Action = 2
)

type Origin uint8

const (
	OriginTarget Origin = iota + 1
	OriginExpand
	OriginAdmit
)

type Header struct {
	Action         Action
	Status         Status
	Generation     uint64
	FrontierHead   uint64
	FrontierTail   uint64
	Scanned        uint32
	Materialized   uint32
	MaxDepth       uint32
	ParentRefID    string
	ParentMemoryID string
	ParentDepth    uint16
	LastEdge       string
	PayloadHash    [sha256.Size]byte
}

type Membership struct {
	Generation uint64
	Depth      uint16
	Origin     Origin
}

type QueueEntry struct {
	ParentRefID string
	MemoryID    string
	Depth       uint16
}

var errState = errors.New("invalid taint state")

func DecisionKey(decisionID string) []byte {
	return []byte("taintdec:" + decisionID)
}

func MembershipPrefix(memoryID string) []byte {
	return []byte("taintm:" + memoryID + ":")
}

func MembershipKey(memoryID, decisionID string) []byte {
	return []byte("taintm:" + memoryID + ":" + decisionID)
}

func DecisionIndexPrefix(decisionID string) []byte {
	return []byte("taintd:" + decisionID + ":")
}

func DecisionIndexKey(decisionID, memoryID string) []byte {
	return []byte("taintd:" + decisionID + ":" + memoryID)
}

func FrontierPrefix(decisionID string) []byte {
	return []byte("taintq:" + decisionID + ":")
}

func FrontierKey(decisionID string, generation, seq uint64) []byte {
	key := make([]byte, 0, len(FrontierPrefix(decisionID))+17)
	key = append(key, FrontierPrefix(decisionID)...)
	key = binary.BigEndian.AppendUint64(key, generation)
	key = append(key, ':')
	return binary.BigEndian.AppendUint64(key, seq)
}

func RevisionKey() []byte {
	return []byte("taintrev")
}

func ActiveCountKey() []byte {
	return []byte("taintactive")
}

func ValidateDecisionID(decisionID string) error {
	decoded, err := hex.DecodeString(decisionID)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != decisionID {
		return fmt.Errorf("%w: decision ID must be lowercase %d-byte hex", errState, sha256.Size)
	}
	return nil
}

func EncodeHeader(header Header) ([]byte, error) {
	if err := validateHeader(header); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 48+len(header.ParentRefID)+len(header.ParentMemoryID)+len(header.LastEdge))
	encoded = append(encoded, byte(header.Action), byte(header.Status))
	encoded = binary.BigEndian.AppendUint64(encoded, header.Generation)
	encoded = binary.BigEndian.AppendUint64(encoded, header.FrontierHead)
	encoded = binary.BigEndian.AppendUint64(encoded, header.FrontierTail)
	encoded = binary.BigEndian.AppendUint32(encoded, header.Scanned)
	encoded = binary.BigEndian.AppendUint32(encoded, header.Materialized)
	encoded = binary.BigEndian.AppendUint32(encoded, header.MaxDepth)
	encoded = binary.BigEndian.AppendUint16(encoded, header.ParentDepth)
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(header.ParentRefID)))
	encoded = append(encoded, header.ParentRefID...)
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(header.ParentMemoryID)))
	encoded = append(encoded, header.ParentMemoryID...)
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(header.LastEdge)))
	encoded = append(encoded, header.LastEdge...)
	return append(encoded, header.PayloadHash[:]...), nil
}

func DecodeHeader(encoded []byte) (Header, error) {
	var header Header
	if len(encoded) < minimumHeaderBytes {
		return header, fmt.Errorf("%w: header is %d bytes", errState, len(encoded))
	}
	offset := 0
	header.Action = Action(encoded[offset])
	header.Status = Status(encoded[offset+1])
	offset += 2
	header.Generation = binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	header.FrontierHead = binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	header.FrontierTail = binary.BigEndian.Uint64(encoded[offset:])
	offset += 8
	header.Scanned = binary.BigEndian.Uint32(encoded[offset:])
	offset += 4
	header.Materialized = binary.BigEndian.Uint32(encoded[offset:])
	offset += 4
	header.MaxDepth = binary.BigEndian.Uint32(encoded[offset:])
	offset += 4
	header.ParentDepth = binary.BigEndian.Uint16(encoded[offset:])
	offset += 2
	parentRefLen := int(binary.BigEndian.Uint16(encoded[offset:]))
	offset += 2
	if parentRefLen > DecisionIDBytes || len(encoded)-offset < parentRefLen+2+2+sha256.Size {
		return Header{}, fmt.Errorf("%w: invalid parent ref length %d", errState, parentRefLen)
	}
	header.ParentRefID = string(encoded[offset : offset+parentRefLen])
	offset += parentRefLen
	parentMemoryLen := int(binary.BigEndian.Uint16(encoded[offset:]))
	offset += 2
	if parentMemoryLen > MaxMemoryIDBytes || len(encoded)-offset < parentMemoryLen+2+sha256.Size {
		return Header{}, fmt.Errorf("%w: invalid parent memory length %d", errState, parentMemoryLen)
	}
	header.ParentMemoryID = string(encoded[offset : offset+parentMemoryLen])
	offset += parentMemoryLen
	edgeLen := int(binary.BigEndian.Uint16(encoded[offset:]))
	offset += 2
	if edgeLen > MaxCursorBytes || len(encoded)-offset < edgeLen+sha256.Size {
		return Header{}, fmt.Errorf("%w: invalid cursor length %d", errState, edgeLen)
	}
	header.LastEdge = string(encoded[offset : offset+edgeLen])
	offset += edgeLen
	copy(header.PayloadHash[:], encoded[offset:])
	offset += sha256.Size
	if offset != len(encoded) {
		return Header{}, fmt.Errorf("%w: %d trailing bytes", errState, len(encoded)-offset)
	}
	if err := validateHeader(header); err != nil {
		return Header{}, err
	}
	return header, nil
}

func EncodeMembership(membership Membership) ([]byte, error) {
	if membership.Generation == 0 {
		return nil, fmt.Errorf("%w: membership generation must be positive", errState)
	}
	if !membership.Origin.Valid() {
		return nil, fmt.Errorf("%w: invalid membership origin %d", errState, membership.Origin)
	}
	if membership.Depth > MaxDepth {
		return nil, fmt.Errorf("%w: membership depth %d exceeds %d", errState, membership.Depth, MaxDepth)
	}
	encoded := make([]byte, 0, 11)
	encoded = binary.BigEndian.AppendUint64(encoded, membership.Generation)
	encoded = binary.BigEndian.AppendUint16(encoded, membership.Depth)
	return append(encoded, byte(membership.Origin)), nil
}

func DecodeMembership(encoded []byte) (Membership, error) {
	if len(encoded) != 11 {
		return Membership{}, fmt.Errorf("%w: membership is %d bytes", errState, len(encoded))
	}
	membership := Membership{
		Generation: binary.BigEndian.Uint64(encoded),
		Depth:      binary.BigEndian.Uint16(encoded[8:]),
		Origin:     Origin(encoded[10]),
	}
	if membership.Generation == 0 {
		return Membership{}, fmt.Errorf("%w: membership generation must be positive", errState)
	}
	if !membership.Origin.Valid() {
		return Membership{}, fmt.Errorf("%w: invalid membership origin %d", errState, membership.Origin)
	}
	if membership.Depth > MaxDepth {
		return Membership{}, fmt.Errorf("%w: membership depth %d exceeds %d", errState, membership.Depth, MaxDepth)
	}
	return membership, nil
}

func EncodeQueueEntry(entry QueueEntry) ([]byte, error) {
	if err := validateMemoryID(entry.MemoryID); err != nil {
		return nil, err
	}
	if err := validateParentRefID(entry.ParentRefID); err != nil {
		return nil, err
	}
	if entry.Depth > MaxDepth {
		return nil, fmt.Errorf("%w: queue depth %d exceeds %d", errState, entry.Depth, MaxDepth)
	}
	encoded := make([]byte, 0, 6+len(entry.ParentRefID)+len(entry.MemoryID))
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(entry.ParentRefID))) // #nosec G115 -- bounded
	encoded = append(encoded, entry.ParentRefID...)
	encoded = binary.BigEndian.AppendUint16(encoded, uint16(len(entry.MemoryID))) // #nosec G115 -- bounded
	encoded = append(encoded, entry.MemoryID...)
	return binary.BigEndian.AppendUint16(encoded, entry.Depth), nil
}

func DecodeQueueEntry(encoded []byte) (QueueEntry, error) {
	if len(encoded) < 6 {
		return QueueEntry{}, fmt.Errorf("%w: short queue entry", errState)
	}
	parentRefLen := int(binary.BigEndian.Uint16(encoded))
	offset := 2
	if parentRefLen != DecisionIDBytes || len(encoded)-offset < parentRefLen+4 {
		return QueueEntry{}, fmt.Errorf("%w: invalid queue parent ref length %d", errState, parentRefLen)
	}
	parentRefID := string(encoded[offset : offset+parentRefLen])
	offset += parentRefLen
	memoryLen := int(binary.BigEndian.Uint16(encoded[offset:]))
	offset += 2
	if memoryLen > MaxMemoryIDBytes || len(encoded) != offset+memoryLen+2 {
		return QueueEntry{}, fmt.Errorf("%w: invalid queue entry length %d", errState, len(encoded))
	}
	entry := QueueEntry{
		ParentRefID: parentRefID,
		MemoryID:    string(encoded[offset : offset+memoryLen]),
		Depth:       binary.BigEndian.Uint16(encoded[offset+memoryLen:]),
	}
	if err := validateMemoryID(entry.MemoryID); err != nil {
		return QueueEntry{}, err
	}
	if err := validateParentRefID(entry.ParentRefID); err != nil {
		return QueueEntry{}, err
	}
	if entry.Depth > MaxDepth {
		return QueueEntry{}, fmt.Errorf("%w: queue depth %d exceeds %d", errState, entry.Depth, MaxDepth)
	}
	return entry, nil
}

func (h Header) MembershipEffective(membership Membership) bool {
	return h.Status.Active() && membership.Generation == h.Generation
}

func (o Origin) Valid() bool {
	return o == OriginTarget || o == OriginExpand || o == OriginAdmit
}

func validateHeader(header Header) error {
	if header.Action != ActionApply && header.Action != ActionClear {
		return fmt.Errorf("%w: invalid header action %d", errState, header.Action)
	}
	if !header.Status.Valid() {
		return fmt.Errorf("%w: invalid header status %d", errState, header.Status)
	}
	if header.Generation == 0 {
		return fmt.Errorf("%w: generation must be positive", errState)
	}
	if header.FrontierHead > header.FrontierTail {
		return fmt.Errorf("%w: frontier head %d exceeds tail %d",
			errState, header.FrontierHead, header.FrontierTail)
	}
	if header.ParentDepth > MaxDepth || header.MaxDepth > MaxDepth {
		return fmt.Errorf("%w: depth exceeds %d", errState, MaxDepth)
	}
	if (header.ParentRefID == "") != (header.ParentMemoryID == "") {
		return fmt.Errorf("%w: parent ref ID and parent memory ID must be set together", errState)
	}
	if header.ParentRefID != "" {
		if err := validateParentRefID(header.ParentRefID); err != nil {
			return err
		}
		if err := validateMemoryID(header.ParentMemoryID); err != nil {
			return err
		}
	}
	if len(header.LastEdge) > MaxCursorBytes {
		return fmt.Errorf("%w: cursor length %d exceeds %d",
			errState, len(header.LastEdge), MaxCursorBytes)
	}
	return nil
}

func validateMemoryID(memoryID string) error {
	if len(memoryID) == 0 || len(memoryID) > MaxMemoryIDBytes {
		return fmt.Errorf("%w: memory ID length %d is outside 1..%d",
			errState, len(memoryID), MaxMemoryIDBytes)
	}
	return nil
}

func validateParentRefID(parentRefID string) error {
	decoded, err := hex.DecodeString(parentRefID)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != parentRefID {
		return fmt.Errorf("%w: parent ref ID must be lowercase %d-byte hex", errState, sha256.Size)
	}
	return nil
}
