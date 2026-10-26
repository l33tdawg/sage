package outcomereceipt

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
)

const signatureDomain = VersionV1

// CanonicalJSON returns the deterministic v1 body encoding. V1 deliberately
// uses only structs, strings, booleans, integers, times, and arrays: it contains
// no float or map fields. Set-like arrays are sorted before encoding.
func CanonicalJSON(receipt Receipt) ([]byte, error) {
	if err := Validate(receipt); err != nil {
		return nil, err
	}
	normalized := normalizeReceipt(receipt)
	return encodeCanonical(normalized)
}

// Digest returns SHA-256(CanonicalJSON(receipt)).
func Digest(receipt Receipt) ([32]byte, error) {
	body, err := CanonicalJSON(receipt)
	if err != nil {
		return [32]byte{}, err
	}
	return sha256.Sum256(body), nil
}

// DigestHex returns the canonical body digest as lowercase hex.
func DigestHex(receipt Receipt) (string, error) {
	digest, err := Digest(receipt)
	if err != nil {
		return "", err
	}
	return hex.EncodeToString(digest[:]), nil
}

// TaskSetDigest returns the versioned digest that must populate
// TaskSet.Digest. Tasks are sorted by chain ID and memory ID before hashing.
func TaskSetDigest(tasks []TaskRef) (string, error) {
	sorted := append([]TaskRef(nil), tasks...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].ChainID != sorted[j].ChainID {
			return sorted[i].ChainID < sorted[j].ChainID
		}
		return sorted[i].MemoryID < sorted[j].MemoryID
	})
	body, err := encodeCanonical(struct {
		Version string    `json:"version"`
		Tasks   []TaskRef `json:"tasks"`
	}{
		Version: VersionV1,
		Tasks:   sorted,
	})
	if err != nil {
		return "", err
	}
	digest := sha256.Sum256(body)
	return hex.EncodeToString(digest[:]), nil
}

// Sign creates one detached signature over the receipt body. SAGE agent
// signatures should use the agent's normal Ed25519 identity.
func Sign(receipt Receipt, role, signerID string, privateKey ed25519.PrivateKey) (Signature, error) {
	if len(privateKey) != ed25519.PrivateKeySize {
		return Signature{}, fmt.Errorf("ed25519 private key has length %d, want %d", len(privateKey), ed25519.PrivateKeySize)
	}
	if err := validateSignatureMetadata(role, signerID); err != nil {
		return Signature{}, err
	}

	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	if !ok || len(publicKey) != ed25519.PublicKeySize {
		return Signature{}, fmt.Errorf("ed25519 private key did not expose a valid public key")
	}
	publicHex := hex.EncodeToString(publicKey)
	if role == RoleSAGEAgent && signerID != publicHex {
		return Signature{}, fmt.Errorf("SAGE signer_id %q does not match the signing public key", signerID)
	}

	message, err := signingMessage(receipt)
	if err != nil {
		return Signature{}, err
	}
	signature := ed25519.Sign(privateKey, message)
	return Signature{
		Scheme:    SignatureSchemeEd25519,
		Role:      role,
		SignerID:  signerID,
		PublicKey: publicHex,
		Signature: hex.EncodeToString(signature),
	}, nil
}

// VerifySignature verifies one detached signature against the receipt body.
func VerifySignature(receipt Receipt, signature Signature) error {
	if signature.Scheme != SignatureSchemeEd25519 {
		return fmt.Errorf("unsupported signature scheme %q", signature.Scheme)
	}
	if err := validateSignatureMetadata(signature.Role, signature.SignerID); err != nil {
		return err
	}
	publicKey, err := decodeHex("public_key", signature.PublicKey, ed25519.PublicKeySize)
	if err != nil {
		return err
	}
	sig, err := decodeHex("signature", signature.Signature, ed25519.SignatureSize)
	if err != nil {
		return err
	}
	if signature.Role == RoleSAGEAgent && signature.SignerID != signature.PublicKey {
		return fmt.Errorf("SAGE signer_id does not match public_key")
	}

	message, err := signingMessage(receipt)
	if err != nil {
		return err
	}
	if !ed25519.Verify(ed25519.PublicKey(publicKey), message, sig) {
		return fmt.Errorf("signature verification failed")
	}
	return nil
}

// VerifyEnvelope validates the receipt and verifies every signature. At least
// one evaluator signature is required; additional SAGE or Noesis signatures are
// optional in v1.
func VerifyEnvelope(envelope SignedReceipt) error {
	if err := Validate(envelope.Receipt); err != nil {
		return err
	}
	if len(envelope.Signatures) == 0 {
		return fmt.Errorf("receipt has no signatures")
	}

	seen := make(map[string]struct{}, len(envelope.Signatures))
	hasEvaluator := false
	for i, signature := range envelope.Signatures {
		if err := VerifySignature(envelope.Receipt, signature); err != nil {
			return fmt.Errorf("signature %d: %w", i, err)
		}
		key := signature.Role + "\x00" + signature.SignerID + "\x00" + signature.PublicKey
		if _, exists := seen[key]; exists {
			return fmt.Errorf("duplicate signature for role %q signer %q", signature.Role, signature.SignerID)
		}
		seen[key] = struct{}{}
		if signature.Role == RoleEvaluator {
			hasEvaluator = true
		}
	}
	if !hasEvaluator {
		return fmt.Errorf("receipt has no evaluator signature")
	}
	return nil
}

func signingMessage(receipt Receipt) ([]byte, error) {
	digest, err := Digest(receipt)
	if err != nil {
		return nil, err
	}
	message := make([]byte, 0, len(signatureDomain)+1+len(digest))
	message = append(message, signatureDomain...)
	message = append(message, 0)
	message = append(message, digest[:]...)
	return message, nil
}

func normalizeReceipt(receipt Receipt) Receipt {
	normalized := receipt
	normalized.TaskSet.Tasks = append([]TaskRef(nil), receipt.TaskSet.Tasks...)
	sort.Slice(normalized.TaskSet.Tasks, func(i, j int) bool {
		if normalized.TaskSet.Tasks[i].ChainID != normalized.TaskSet.Tasks[j].ChainID {
			return normalized.TaskSet.Tasks[i].ChainID < normalized.TaskSet.Tasks[j].ChainID
		}
		return normalized.TaskSet.Tasks[i].MemoryID < normalized.TaskSet.Tasks[j].MemoryID
	})

	normalized.Runs = append([]Run(nil), receipt.Runs...)
	for i := range normalized.Runs {
		normalized.Runs[i].ContextMemoryIDs = append([]string(nil), receipt.Runs[i].ContextMemoryIDs...)
		normalized.Runs[i].MaskedMemoryIDs = append([]string(nil), receipt.Runs[i].MaskedMemoryIDs...)
		normalized.Runs[i].ArtifactHashes = append([]string(nil), receipt.Runs[i].ArtifactHashes...)
		sort.Strings(normalized.Runs[i].MaskedMemoryIDs)
		sort.Strings(normalized.Runs[i].ArtifactHashes)
	}
	sort.Slice(normalized.Runs, func(i, j int) bool {
		a, b := normalized.Runs[i], normalized.Runs[j]
		if a.TaskMemoryID != b.TaskMemoryID {
			return a.TaskMemoryID < b.TaskMemoryID
		}
		if a.Trial != b.Trial {
			return a.Trial < b.Trial
		}
		return a.Arm < b.Arm
	})

	normalized.Evidence = append([]EvidenceRef(nil), receipt.Evidence...)
	sort.Slice(normalized.Evidence, func(i, j int) bool {
		if normalized.Evidence[i].Digest != normalized.Evidence[j].Digest {
			return normalized.Evidence[i].Digest < normalized.Evidence[j].Digest
		}
		if normalized.Evidence[i].Kind != normalized.Evidence[j].Kind {
			return normalized.Evidence[i].Kind < normalized.Evidence[j].Kind
		}
		return normalized.Evidence[i].MediaType < normalized.Evidence[j].MediaType
	})
	return normalized
}

func encodeCanonical(value any) ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(value); err != nil {
		return nil, fmt.Errorf("encode canonical JSON: %w", err)
	}
	body := bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
	return append([]byte(nil), body...), nil
}

func decodeHex(field, value string, wantBytes int) ([]byte, error) {
	if value == "" {
		return nil, fmt.Errorf("%s is required", field)
	}
	if value != strings.ToLower(value) {
		return nil, fmt.Errorf("%s must be lowercase hex", field)
	}
	decoded, err := hex.DecodeString(value)
	if err != nil {
		return nil, fmt.Errorf("decode %s: %w", field, err)
	}
	if len(decoded) != wantBytes {
		return nil, fmt.Errorf("%s has %d bytes, want %d", field, len(decoded), wantBytes)
	}
	return decoded, nil
}
