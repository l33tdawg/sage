// Package scope defines the deterministic, versioned on-chain record used by
// v11.9 domain-scoped quorum. It deliberately has no store or ABCI dependency:
// the same bytes can be validated by transaction handling, Badger persistence,
// snapshot recovery, and tests before any caller is allowed to mutate state.
package scope

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"unicode/utf8"
)

const (
	// SchemaV1 is the leading byte on every v11.9 scope record. Future versions
	// must add a new decoder rather than reinterpret these bytes.
	SchemaV1 byte = 1

	maxIDBytes     = 512
	maxDomains     = 256
	maxMembers     = 256
	maxDomainBytes = 512
)

// State is the lifecycle state of a quorum scope.
type State byte

const (
	StateActive State = iota + 1
	StatePaused
	StateRetired
)

// Domain is an explicit selected domain. V1 records only exact domains;
// Subtree is retained in the wire shape for a later versioned extension but is
// rejected by Validate until its semantics and collision rules are implemented.
type Domain struct {
	Name    string
	Subtree bool
}

// Member is one pinned validator allocation in a scope roster. AssignedWeight
// is deliberately an integer: a scope denominator must never depend on
// architecture-sensitive floating-point arithmetic.
type Member struct {
	ValidatorID    string
	AssignedWeight uint64
	JoinedRevision uint64
	Active         bool
}

// Record is the canonical v11.9 scope roster head. Domains and Members must be
// strictly bytewise sorted by Name and ValidatorID respectively. Encode rejects
// noncanonical caller input instead of sorting it silently.
type Record struct {
	ScopeID               string
	Revision              uint64
	State                 State
	ControllerValidatorID string
	CreatedHeight         int64
	UpdatedHeight         int64
	Domains               []Domain
	Members               []Member
}

// Encode returns the one canonical V1 byte representation of r.
func Encode(r Record) ([]byte, error) {
	if err := Validate(r); err != nil {
		return nil, err
	}

	buf := make([]byte, 0, 64+len(r.ScopeID)+len(r.ControllerValidatorID))
	buf = append(buf, SchemaV1)
	buf = appendString(buf, r.ScopeID)
	buf = appendUint64(buf, r.Revision)
	buf = append(buf, byte(r.State))
	buf = appendString(buf, r.ControllerValidatorID)
	buf = appendInt64(buf, r.CreatedHeight)
	buf = appendInt64(buf, r.UpdatedHeight)
	buf = appendUint32(buf, uint32(len(r.Domains)))
	for _, d := range r.Domains {
		buf = appendString(buf, d.Name)
		if d.Subtree {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	buf = appendUint32(buf, uint32(len(r.Members)))
	for _, m := range r.Members {
		buf = appendString(buf, m.ValidatorID)
		buf = appendUint64(buf, m.AssignedWeight)
		buf = appendUint64(buf, m.JoinedRevision)
		if m.Active {
			buf = append(buf, 1)
		} else {
			buf = append(buf, 0)
		}
	}
	return buf, nil
}

// Decode validates and decodes one canonical V1 scope record. It rejects
// unknown versions, oversized length prefixes, noncanonical ordering, and
// trailing data; callers therefore cannot accept a second encoding for the
// same logical roster.
func Decode(data []byte) (Record, error) {
	if len(data) == 0 {
		return Record{}, errors.New("scope record is empty")
	}
	if data[0] != SchemaV1 {
		return Record{}, fmt.Errorf("unsupported scope schema version %d", data[0])
	}
	off := 1
	var err error
	var r Record
	if r.ScopeID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Record{}, fmt.Errorf("scope id: %w", err)
	}
	if r.Revision, off, err = readUint64(data, off); err != nil {
		return Record{}, fmt.Errorf("revision: %w", err)
	}
	if off >= len(data) {
		return Record{}, errors.New("state: truncated")
	}
	r.State = State(data[off])
	off++
	if r.ControllerValidatorID, off, err = readString(data, off, maxIDBytes); err != nil {
		return Record{}, fmt.Errorf("controller validator: %w", err)
	}
	if r.CreatedHeight, off, err = readInt64(data, off); err != nil {
		return Record{}, fmt.Errorf("created height: %w", err)
	}
	if r.UpdatedHeight, off, err = readInt64(data, off); err != nil {
		return Record{}, fmt.Errorf("updated height: %w", err)
	}

	var count uint32
	if count, off, err = readUint32(data, off); err != nil {
		return Record{}, fmt.Errorf("domain count: %w", err)
	}
	if count > maxDomains {
		return Record{}, fmt.Errorf("domain count %d exceeds %d", count, maxDomains)
	}
	r.Domains = make([]Domain, 0, int(count))
	for i := uint32(0); i < count; i++ {
		var d Domain
		if d.Name, off, err = readString(data, off, maxDomainBytes); err != nil {
			return Record{}, fmt.Errorf("domain %d: %w", i, err)
		}
		if off >= len(data) {
			return Record{}, fmt.Errorf("domain %d subtree: truncated", i)
		}
		switch data[off] {
		case 0:
			d.Subtree = false
		case 1:
			d.Subtree = true
		default:
			return Record{}, fmt.Errorf("domain %d subtree flag is invalid", i)
		}
		off++
		r.Domains = append(r.Domains, d)
	}

	if count, off, err = readUint32(data, off); err != nil {
		return Record{}, fmt.Errorf("member count: %w", err)
	}
	if count > maxMembers {
		return Record{}, fmt.Errorf("member count %d exceeds %d", count, maxMembers)
	}
	r.Members = make([]Member, 0, int(count))
	for i := uint32(0); i < count; i++ {
		var m Member
		if m.ValidatorID, off, err = readString(data, off, maxIDBytes); err != nil {
			return Record{}, fmt.Errorf("member %d id: %w", i, err)
		}
		if m.AssignedWeight, off, err = readUint64(data, off); err != nil {
			return Record{}, fmt.Errorf("member %d weight: %w", i, err)
		}
		if m.JoinedRevision, off, err = readUint64(data, off); err != nil {
			return Record{}, fmt.Errorf("member %d joined revision: %w", i, err)
		}
		if off >= len(data) {
			return Record{}, fmt.Errorf("member %d active flag: truncated", i)
		}
		switch data[off] {
		case 0:
			m.Active = false
		case 1:
			m.Active = true
		default:
			return Record{}, fmt.Errorf("member %d active flag is invalid", i)
		}
		off++
		r.Members = append(r.Members, m)
	}
	if off != len(data) {
		return Record{}, errors.New("scope record has trailing bytes")
	}
	if err := Validate(r); err != nil {
		return Record{}, err
	}
	return r, nil
}

// Validate checks the V1 safety and canonicalization invariants.
func Validate(r Record) error {
	if err := validateID("scope id", r.ScopeID); err != nil {
		return err
	}
	// Scope IDs are exposed as one REST path segment. Rejecting '/' at the
	// consensus codec boundary keeps the canonical identifier addressable on
	// every operator surface instead of creating a valid on-chain scope that can
	// only be discovered through the list endpoint.
	if strings.ContainsRune(r.ScopeID, '/') {
		return errors.New("scope id must not contain '/'")
	}
	if r.Revision == 0 {
		return errors.New("revision must be non-zero")
	}
	if r.State != StateActive && r.State != StatePaused && r.State != StateRetired {
		return fmt.Errorf("invalid scope state %d", r.State)
	}
	if err := validateID("controller validator", r.ControllerValidatorID); err != nil {
		return err
	}
	if r.CreatedHeight < 0 || r.UpdatedHeight < r.CreatedHeight {
		return errors.New("scope heights are invalid")
	}
	if len(r.Domains) == 0 || len(r.Domains) > maxDomains {
		return fmt.Errorf("domain count must be 1..%d", maxDomains)
	}
	previous := ""
	for i, d := range r.Domains {
		if d.Subtree {
			return fmt.Errorf("domain %d uses unsupported subtree scope", i)
		}
		if err := validateString("domain", d.Name, maxDomainBytes); err != nil {
			return fmt.Errorf("domain %d: %w", i, err)
		}
		if i > 0 && d.Name <= previous {
			return errors.New("domains must be strictly bytewise sorted")
		}
		previous = d.Name
	}
	if len(r.Members) == 0 || len(r.Members) > maxMembers {
		return fmt.Errorf("member count must be 1..%d", maxMembers)
	}
	previous = ""
	var total uint64
	controllerActive := false
	for i, m := range r.Members {
		if err := validateID("member validator", m.ValidatorID); err != nil {
			return fmt.Errorf("member %d: %w", i, err)
		}
		if i > 0 && m.ValidatorID <= previous {
			return errors.New("members must be strictly bytewise sorted")
		}
		if m.AssignedWeight == 0 {
			return fmt.Errorf("member %d has zero assigned weight", i)
		}
		if m.JoinedRevision == 0 || m.JoinedRevision > r.Revision {
			return fmt.Errorf("member %d has invalid joined revision", i)
		}
		if math.MaxUint64-total < m.AssignedWeight {
			return errors.New("assigned weight total overflows uint64")
		}
		total += m.AssignedWeight
		if m.ValidatorID == r.ControllerValidatorID && m.Active {
			controllerActive = true
		}
		previous = m.ValidatorID
	}
	if !controllerActive {
		return errors.New("controller must be an active roster member")
	}
	return nil
}

func appendUint32(dst []byte, v uint32) []byte {
	var b [4]byte
	binary.BigEndian.PutUint32(b[:], v)
	return append(dst, b[:]...)
}

func appendUint64(dst []byte, v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return append(dst, b[:]...)
}

func appendInt64(dst []byte, v int64) []byte { return appendUint64(dst, uint64(v)) }

func appendString(dst []byte, s string) []byte {
	dst = appendUint32(dst, uint32(len(s)))
	return append(dst, s...)
}

func readUint32(data []byte, off int) (uint32, int, error) {
	if off < 0 || len(data)-off < 4 {
		return 0, off, errors.New("truncated uint32")
	}
	return binary.BigEndian.Uint32(data[off : off+4]), off + 4, nil
}

func readUint64(data []byte, off int) (uint64, int, error) {
	if off < 0 || len(data)-off < 8 {
		return 0, off, errors.New("truncated uint64")
	}
	return binary.BigEndian.Uint64(data[off : off+8]), off + 8, nil
}

func readInt64(data []byte, off int) (int64, int, error) {
	v, next, err := readUint64(data, off)
	return int64(v), next, err
}

func readString(data []byte, off, max int) (string, int, error) {
	n, next, err := readUint32(data, off)
	if err != nil {
		return "", off, err
	}
	if n > uint32(max) || int(n) > len(data)-next {
		return "", off, errors.New("invalid length-prefixed string")
	}
	s := string(data[next : next+int(n)])
	if !utf8.ValidString(s) {
		return "", off, errors.New("string is not valid UTF-8")
	}
	return s, next + int(n), nil
}

func validateID(label, value string) error { return validateString(label, value, maxIDBytes) }

func validateString(label, value string, max int) error {
	if value == "" || len(value) > max || !utf8.ValidString(value) {
		return fmt.Errorf("%s is invalid", label)
	}
	return nil
}
