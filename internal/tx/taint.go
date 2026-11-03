package tx

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	// TaintApplyPayloadVersion is the only apply payload version understood by
	// app-v30.
	TaintApplyPayloadVersion uint8 = 1
	// TaintClearPayloadVersion is the only clear payload version understood by
	// app-v30.
	TaintClearPayloadVersion uint8 = 1

	TaintActionApply uint8 = 1
	TaintActionClear uint8 = 2

	TaintScopeDirect             uint8 = 0
	TaintScopeRootGeneration     uint8 = 1
	TaintScopeCredentialSet      uint8 = 2
	MaxTaintPayloadBytes               = 4 << 10
	MaxTaintChainIDBytes               = 64
	MaxTaintMemoryIDBytes              = 512
	MaxTaintPrincipalIDBytes           = 128
	MaxTaintCredentialBytes            = 128
	MaxTaintCredentialSetEntries       = 16
)

const (
	taintApplyDecisionDomain = "sage/taint/apply/v1\x00"
	taintClearTargetDomain   = "sage/taint/clear/v1\x00"
)

var errTaintPayload = errors.New("invalid taint payload")

// TaintApplyPayload is the canonical app-v30 apply body. Exactly one scope is
// represented, selected by ScopeKind. Fields for the other scope kinds must be
// empty.
//
// ReasonCategory is an opaque byte chosen by the proposal author. All values
// 0..255 are valid; app-v30 assigns no enum meanings to them. The byte is part
// of the canonical payload and therefore the decision identity.
type TaintApplyPayload struct {
	Version           uint8
	Action            uint8
	TargetChainID     string
	TargetMemoryID    string
	TargetContentHash []byte
	ScopeKind         uint8
	PrincipalID       string
	GenerationFrom    uint64
	GenerationTo      uint64
	Credentials       []string
	ReasonCategory    uint8
	EvidenceDigest    []byte
}

// TaintClearPayload is the canonical app-v30 clear body. DecisionID is the
// SHA-256 identity derived from the exact apply payload.
// ReasonCategory has the same opaque-byte semantics as in TaintApplyPayload
// and contributes to the clear target identity.
type TaintClearPayload struct {
	Version        uint8
	Action         uint8
	DecisionID     []byte
	ReasonCategory uint8
	EvidenceDigest []byte
}

// EncodeTaintApplyPayload returns the canonical TaintApplyV1 encoding.
func EncodeTaintApplyPayload(payload TaintApplyPayload) ([]byte, error) {
	if err := validateTaintApplyPayload(payload); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 128+len(payload.TargetChainID)+len(payload.TargetMemoryID))
	encoded = append(encoded, payload.Version, payload.Action)
	encoded = appendTaintBytes(encoded, []byte(payload.TargetChainID))
	encoded = appendTaintBytes(encoded, []byte(payload.TargetMemoryID))
	encoded = append(encoded, payload.TargetContentHash...)
	encoded = append(encoded, payload.ScopeKind)
	switch payload.ScopeKind {
	case TaintScopeDirect:
	case TaintScopeRootGeneration:
		encoded = appendTaintBytes(encoded, []byte(payload.PrincipalID))
		encoded = appendTaintUint64(encoded, payload.GenerationFrom)
		encoded = appendTaintUint64(encoded, payload.GenerationTo)
	case TaintScopeCredentialSet:
		encoded = append(encoded, uint8(len(payload.Credentials)))
		for _, credential := range payload.Credentials {
			encoded = appendTaintBytes(encoded, []byte(credential))
		}
	}
	encoded = append(encoded, payload.ReasonCategory)
	encoded = append(encoded, payload.EvidenceDigest...)
	if len(encoded) > MaxTaintPayloadBytes {
		return nil, fmt.Errorf(
			"%w: encoded size %d exceeds %d bytes",
			errTaintPayload, len(encoded), MaxTaintPayloadBytes,
		)
	}
	return encoded, nil
}

// DecodeTaintApplyPayload accepts only the canonical TaintApplyV1 encoding.
func DecodeTaintApplyPayload(encoded []byte) (*TaintApplyPayload, error) {
	if len(encoded) == 0 || len(encoded) > MaxTaintPayloadBytes {
		return nil, fmt.Errorf("%w: invalid encoded size %d", errTaintPayload, len(encoded))
	}
	offset := 0
	version, next, err := readTaintByte(encoded, offset, "version")
	if err != nil {
		return nil, err
	}
	offset = next
	action, next, err := readTaintByte(encoded, offset, "action")
	if err != nil {
		return nil, err
	}
	offset = next
	chainID, next, err := readTaintBytes(encoded, offset, MaxTaintChainIDBytes, "target chain ID")
	if err != nil {
		return nil, err
	}
	offset = next
	memoryID, next, err := readTaintBytes(encoded, offset, MaxTaintMemoryIDBytes, "target memory ID")
	if err != nil {
		return nil, err
	}
	offset = next
	if len(encoded)-offset < sha256.Size {
		return nil, fmt.Errorf("%w: truncated target content hash", errTaintPayload)
	}
	targetHash := append([]byte(nil), encoded[offset:offset+sha256.Size]...)
	offset += sha256.Size
	scopeKind, next, err := readTaintByte(encoded, offset, "scope kind")
	if err != nil {
		return nil, err
	}
	offset = next
	payload := &TaintApplyPayload{
		Version:           version,
		Action:            action,
		TargetChainID:     string(chainID),
		TargetMemoryID:    string(memoryID),
		TargetContentHash: targetHash,
		ScopeKind:         scopeKind,
	}
	switch scopeKind {
	case TaintScopeDirect:
	case TaintScopeRootGeneration:
		principal, afterPrincipal, readErr := readTaintBytes(
			encoded, offset, MaxTaintPrincipalIDBytes, "Root principal ID",
		)
		if readErr != nil {
			return nil, readErr
		}
		offset = afterPrincipal
		generationFrom, afterFrom, readErr := readTaintUint64(
			encoded, offset, "Root generation lower bound",
		)
		if readErr != nil {
			return nil, readErr
		}
		offset = afterFrom
		generationTo, afterTo, readErr := readTaintUint64(
			encoded, offset, "Root generation upper bound",
		)
		if readErr != nil {
			return nil, readErr
		}
		offset = afterTo
		payload.PrincipalID = string(principal)
		payload.GenerationFrom = generationFrom
		payload.GenerationTo = generationTo
	case TaintScopeCredentialSet:
		count, afterCount, readErr := readTaintByte(encoded, offset, "credential count")
		if readErr != nil {
			return nil, readErr
		}
		offset = afterCount
		payload.Credentials = make([]string, 0, int(count))
		for i := 0; i < int(count); i++ {
			credential, afterCredential, credentialErr := readTaintBytes(
				encoded, offset, MaxTaintCredentialBytes, fmt.Sprintf("credential %d", i),
			)
			if credentialErr != nil {
				return nil, credentialErr
			}
			offset = afterCredential
			payload.Credentials = append(payload.Credentials, string(credential))
		}
	default:
		return nil, fmt.Errorf("%w: unsupported scope kind %d", errTaintPayload, scopeKind)
	}
	reason, next, err := readTaintByte(encoded, offset, "reason category")
	if err != nil {
		return nil, err
	}
	offset = next
	if len(encoded)-offset < sha256.Size {
		return nil, fmt.Errorf("%w: truncated evidence digest", errTaintPayload)
	}
	payload.ReasonCategory = reason
	payload.EvidenceDigest = append([]byte(nil), encoded[offset:offset+sha256.Size]...)
	offset += sha256.Size
	if offset != len(encoded) {
		return nil, fmt.Errorf("%w: %d trailing bytes", errTaintPayload, len(encoded)-offset)
	}
	if err := validateTaintApplyPayload(*payload); err != nil {
		return nil, err
	}
	canonical, encodeErr := EncodeTaintApplyPayload(*payload)
	if encodeErr != nil {
		return nil, encodeErr
	}
	if !bytes.Equal(canonical, encoded) {
		return nil, fmt.Errorf("%w: non-canonical encoding", errTaintPayload)
	}
	return payload, nil
}

// EncodeTaintClearPayload returns the canonical TaintClearV1 encoding.
func EncodeTaintClearPayload(payload TaintClearPayload) ([]byte, error) {
	if err := validateTaintClearPayload(payload); err != nil {
		return nil, err
	}
	encoded := make([]byte, 0, 67)
	encoded = append(encoded, payload.Version, payload.Action)
	encoded = append(encoded, payload.DecisionID...)
	encoded = append(encoded, payload.ReasonCategory)
	encoded = append(encoded, payload.EvidenceDigest...)
	return encoded, nil
}

// DecodeTaintClearPayload accepts only the canonical TaintClearV1 encoding.
func DecodeTaintClearPayload(encoded []byte) (*TaintClearPayload, error) {
	if len(encoded) != 67 {
		return nil, fmt.Errorf("%w: invalid encoded size %d", errTaintPayload, len(encoded))
	}
	payload := &TaintClearPayload{
		Version:        encoded[0],
		Action:         encoded[1],
		DecisionID:     append([]byte(nil), encoded[2:34]...),
		ReasonCategory: encoded[34],
		EvidenceDigest: append([]byte(nil), encoded[35:67]...),
	}
	if err := validateTaintClearPayload(*payload); err != nil {
		return nil, err
	}
	return payload, nil
}

// TaintApplyDecisionID returns the lowercase domain-separated SHA-256 identity
// used as both the decision ID and GovPropose.TargetID.
func TaintApplyDecisionID(encoded []byte) (string, error) {
	if _, err := DecodeTaintApplyPayload(encoded); err != nil {
		return "", err
	}
	return taintDomainDigest(taintApplyDecisionDomain, encoded), nil
}

// TaintClearTargetID returns the lowercase domain-separated SHA-256 identity
// used as GovPropose.TargetID for a clear.
func TaintClearTargetID(encoded []byte) (string, error) {
	if _, err := DecodeTaintClearPayload(encoded); err != nil {
		return "", err
	}
	return taintDomainDigest(taintClearTargetDomain, encoded), nil
}

func validateTaintApplyPayload(payload TaintApplyPayload) error {
	if payload.Version != TaintApplyPayloadVersion {
		return fmt.Errorf("%w: unsupported apply version %d", errTaintPayload, payload.Version)
	}
	if payload.Action != TaintActionApply {
		return fmt.Errorf("%w: apply action is %d", errTaintPayload, payload.Action)
	}
	if len(payload.TargetChainID) == 0 || len(payload.TargetChainID) > MaxTaintChainIDBytes {
		return fmt.Errorf("%w: target chain ID length %d is outside 1..%d",
			errTaintPayload, len(payload.TargetChainID), MaxTaintChainIDBytes)
	}
	if len(payload.TargetMemoryID) == 0 || len(payload.TargetMemoryID) > MaxTaintMemoryIDBytes {
		return fmt.Errorf("%w: target memory ID length %d is outside 1..%d",
			errTaintPayload, len(payload.TargetMemoryID), MaxTaintMemoryIDBytes)
	}
	if len(payload.TargetContentHash) != sha256.Size {
		return fmt.Errorf("%w: target content hash length %d, want %d",
			errTaintPayload, len(payload.TargetContentHash), sha256.Size)
	}
	switch payload.ScopeKind {
	case TaintScopeDirect:
		if payload.PrincipalID != "" || payload.GenerationFrom != 0 ||
			payload.GenerationTo != 0 || len(payload.Credentials) != 0 {
			return fmt.Errorf("%w: direct-only scope contains scope data", errTaintPayload)
		}
	case TaintScopeRootGeneration:
		if len(payload.PrincipalID) == 0 || len(payload.PrincipalID) > MaxTaintPrincipalIDBytes {
			return fmt.Errorf("%w: Root principal ID length %d is outside 1..%d",
				errTaintPayload, len(payload.PrincipalID), MaxTaintPrincipalIDBytes)
		}
		if payload.GenerationFrom == 0 || payload.GenerationTo < payload.GenerationFrom {
			return fmt.Errorf("%w: invalid Root generation range %d..%d",
				errTaintPayload, payload.GenerationFrom, payload.GenerationTo)
		}
		if len(payload.Credentials) != 0 {
			return fmt.Errorf("%w: Root scope contains credentials", errTaintPayload)
		}
	case TaintScopeCredentialSet:
		if payload.PrincipalID != "" || payload.GenerationFrom != 0 || payload.GenerationTo != 0 {
			return fmt.Errorf("%w: credential scope contains Root data", errTaintPayload)
		}
		if len(payload.Credentials) == 0 || len(payload.Credentials) > MaxTaintCredentialSetEntries {
			return fmt.Errorf("%w: credential count %d is outside 1..%d",
				errTaintPayload, len(payload.Credentials), MaxTaintCredentialSetEntries)
		}
		var previous []byte
		for i, credential := range payload.Credentials {
			value := []byte(credential)
			if len(value) == 0 || len(value) > MaxTaintCredentialBytes {
				return fmt.Errorf("%w: credential %d length %d is outside 1..%d",
					errTaintPayload, i, len(value), MaxTaintCredentialBytes)
			}
			if i > 0 && bytes.Compare(previous, value) >= 0 {
				return fmt.Errorf("%w: credentials are not strictly raw-byte sorted and unique", errTaintPayload)
			}
			previous = value
		}
	default:
		return fmt.Errorf("%w: unsupported scope kind %d", errTaintPayload, payload.ScopeKind)
	}
	if len(payload.EvidenceDigest) != sha256.Size {
		return fmt.Errorf("%w: evidence digest length %d, want %d",
			errTaintPayload, len(payload.EvidenceDigest), sha256.Size)
	}
	return nil
}

func validateTaintClearPayload(payload TaintClearPayload) error {
	if payload.Version != TaintClearPayloadVersion {
		return fmt.Errorf("%w: unsupported clear version %d", errTaintPayload, payload.Version)
	}
	if payload.Action != TaintActionClear {
		return fmt.Errorf("%w: clear action is %d", errTaintPayload, payload.Action)
	}
	if len(payload.DecisionID) != sha256.Size {
		return fmt.Errorf("%w: decision ID length %d, want %d",
			errTaintPayload, len(payload.DecisionID), sha256.Size)
	}
	if len(payload.EvidenceDigest) != sha256.Size {
		return fmt.Errorf("%w: evidence digest length %d, want %d",
			errTaintPayload, len(payload.EvidenceDigest), sha256.Size)
	}
	return nil
}

func taintDomainDigest(domain string, payload []byte) string {
	digest := sha256.New()
	_, _ = digest.Write([]byte(domain))
	_, _ = digest.Write(payload)
	return hex.EncodeToString(digest.Sum(nil))
}

func appendTaintUint64(dst []byte, value uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, value)
	return append(dst, buf...)
}

func appendTaintBytes(dst, value []byte) []byte {
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(len(value))) // #nosec G115 -- validated caller bounds
	dst = append(dst, buf...)
	return append(dst, value...)
}

func readTaintByte(data []byte, offset int, field string) (uint8, int, error) {
	if offset >= len(data) {
		return 0, 0, fmt.Errorf("%w: truncated %s", errTaintPayload, field)
	}
	return data[offset], offset + 1, nil
}

func readTaintUint64(data []byte, offset int, field string) (uint64, int, error) {
	if len(data)-offset < 8 {
		return 0, 0, fmt.Errorf("%w: truncated %s", errTaintPayload, field)
	}
	return binary.BigEndian.Uint64(data[offset : offset+8]), offset + 8, nil
}

func readTaintBytes(data []byte, offset, max int, field string) ([]byte, int, error) {
	if len(data)-offset < 2 {
		return nil, 0, fmt.Errorf("%w: truncated %s length", errTaintPayload, field)
	}
	length := int(binary.BigEndian.Uint16(data[offset : offset+2]))
	offset += 2
	if length == 0 || length > max {
		return nil, 0, fmt.Errorf("%w: %s length %d is outside 1..%d",
			errTaintPayload, field, length, max)
	}
	if len(data)-offset < length {
		return nil, 0, fmt.Errorf("%w: truncated %s", errTaintPayload, field)
	}
	return append([]byte(nil), data[offset:offset+length]...), offset + length, nil
}
