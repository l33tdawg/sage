package taskidempotency

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"math"

	"github.com/l33tdawg/sage/internal/tx"
)

const MaxKeyBytes = 128

var ErrInvalidKey = errors.New("invalid task idempotency key")

// ValidateKey accepts one canonical, signature-bound request token. Keeping the
// alphabet to visible ASCII makes the same token unambiguous in JSON, logs and
// the consensus key derivation; raw tokens are never stored in Badger.
func ValidateKey(key string) error {
	if len(key) == 0 || len(key) > MaxKeyBytes {
		return fmt.Errorf("%w: must contain 1-%d bytes", ErrInvalidKey, MaxKeyBytes)
	}
	for i := range key {
		if key[i] < 0x21 || key[i] > 0x7e {
			return fmt.Errorf("%w: must contain only visible ASCII without spaces", ErrInvalidKey)
		}
	}
	return nil
}

func validPrincipal(principalID string) bool {
	if len(principalID) != 64 {
		return false
	}
	for i := range principalID {
		c := principalID[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// SemanticKey returns the stable task key used when an ordinary agent omits an
// explicit idempotency key. Keep the historical "mcp-" wire prefix and digest
// label: app-v23 makes this identity durable, and the REST and MCP entry points
// must resolve the same caller/domain/content task to the same binding.
func SemanticKey(agentID, domain, content string) (string, error) {
	if !validPrincipal(agentID) {
		return "", errors.New("task idempotency agent must be a lowercase 64-hex identity")
	}
	if domain == "" {
		return "", errors.New("task idempotency domain is required")
	}
	if content == "" {
		return "", errors.New("task idempotency content is required")
	}
	sum := sha256.Sum256([]byte(
		"sage/mcp/task/idempotency/v1\x00" +
			agentID + "\x00" +
			domain + "\x00" +
			content,
	))
	key := "mcp-" + hex.EncodeToString(sum[:])
	if err := ValidateKey(key); err != nil {
		return "", err
	}
	return key, nil
}

func digestParts(label string, parts ...[]byte) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte(label))
	var size [4]byte
	for _, part := range parts {
		binary.BigEndian.PutUint32(size[:], uint32(len(part))) // #nosec G115 -- all callers protocol-bound inputs
		_, _ = h.Write(size[:])
		_, _ = h.Write(part)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

// BindingKeyDigest is the opaque Badger-key suffix. It binds one caller-owned
// token to the effective policy principal without persisting the raw token.
func BindingKeyDigest(principalID, key string) ([sha256.Size]byte, error) {
	if !validPrincipal(principalID) {
		return [sha256.Size]byte{}, errors.New("task idempotency principal must be a lowercase 64-hex identity")
	}
	if err := ValidateKey(key); err != nil {
		return [sha256.Size]byte{}, err
	}
	return digestParts(
		"sage/app-v23/task-idempotency/key/v1\x00",
		[]byte(principalID),
		[]byte(key),
	), nil
}

// MemoryID returns a stable UUID-shaped identifier for one principal/key pair.
// The RFC-4122 version/variant bits are cosmetic; collision resistance comes
// from the principal-bound SHA-256 preimage.
func MemoryID(principalID, key string) (string, error) {
	sum, err := BindingKeyDigest(principalID, key)
	if err != nil {
		return "", err
	}
	raw := append([]byte(nil), sum[:16]...)
	raw[6] = (raw[6] & 0x0f) | 0x40
	raw[8] = (raw[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}

// PayloadDigest covers every consensus task field plus the exact local
// assignee. EmbeddingHash is deliberately excluded: the receiving node owns the
// active vector space and may regenerate it between retries. The task's signed
// semantic and policy payload remains exact. Empty lineage keeps the original
// v1 digest byte-for-byte. App-v30 nonempty lineage uses a v2 domain and the
// canonical transaction extension, shared by REST replay and consensus receipts.
func PayloadDigest(
	principalID, assigneeID string,
	submit *tx.MemorySubmit,
) ([sha256.Size]byte, error) {
	if !validPrincipal(principalID) || !validPrincipal(assigneeID) {
		return [sha256.Size]byte{}, errors.New("task idempotency identities must be lowercase 64-hex values")
	}
	if submit == nil || submit.MemoryType != tx.MemoryTypeTask {
		return [sha256.Size]byte{}, errors.New("task idempotency payload must be a task")
	}
	var memoryType [1]byte
	memoryType[0] = byte(submit.MemoryType)
	var classification [1]byte
	classification[0] = byte(submit.Classification)
	var confidence [8]byte
	binary.BigEndian.PutUint64(confidence[:], math.Float64bits(submit.ConfidenceScore))
	tagParts := make([][]byte, 0, len(submit.Tags)+13)
	tagParts = append(tagParts,
		[]byte(principalID),
		[]byte(assigneeID),
		[]byte(submit.MemoryID),
		submit.ContentHash,
		memoryType[:],
		[]byte(submit.DomainTag),
		confidence[:],
		[]byte(submit.Content),
		[]byte(submit.ParentHash),
		classification[:],
		[]byte(submit.TaskStatus),
	)
	for _, tag := range submit.Tags {
		tagParts = append(tagParts, []byte(tag))
	}
	if len(submit.DerivedFrom) == 0 {
		return digestParts("sage/app-v23/task-idempotency/payload/v1\x00", tagParts...), nil
	}
	parents := append([]tx.ParentRef(nil), submit.DerivedFrom...)
	tx.SortParentRefs(parents)
	encodedLineage, lineageErr := tx.EncodeDerivedFromExtension(parents)
	if lineageErr != nil {
		return [sha256.Size]byte{}, fmt.Errorf("task idempotency lineage: %w", lineageErr)
	}
	tagParts = append(tagParts, encodedLineage)
	return digestParts("sage/app-v30/task-idempotency/payload/v2\x00", tagParts...), nil
}

func Hex(sum [sha256.Size]byte) string {
	return hex.EncodeToString(sum[:])
}
