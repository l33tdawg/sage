package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
)

// GenerateKeypair creates a new Ed25519 keypair.
func GenerateKeypair() (ed25519.PublicKey, ed25519.PrivateKey, error) {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate ed25519 keypair: %w", err)
	}
	return pub, priv, nil
}

// Sign signs a message with an Ed25519 private key.
func Sign(privateKey ed25519.PrivateKey, message []byte) []byte {
	return ed25519.Sign(privateKey, message)
}

// Verify checks an Ed25519 signature.
func Verify(publicKey ed25519.PublicKey, message []byte, signature []byte) bool {
	return ed25519.Verify(publicKey, message, signature)
}

// PublicKeyToAgentID converts an Ed25519 public key to a hex-encoded agent ID.
func PublicKeyToAgentID(pub ed25519.PublicKey) string {
	return hex.EncodeToString(pub)
}

// AgentIDToPublicKey converts a hex-encoded agent ID to an Ed25519 public key.
func AgentIDToPublicKey(agentID string) (ed25519.PublicKey, error) {
	b, err := hex.DecodeString(agentID)
	if err != nil {
		return nil, fmt.Errorf("decode agent ID: %w", err)
	}
	if len(b) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("invalid public key length: got %d, want %d", len(b), ed25519.PublicKeySize)
	}
	return ed25519.PublicKey(b), nil
}

// SignRequest creates a signature for an API request.
// The signed message is SHA-256(method + " " + path + "\n" + body) + timestamp (big-endian int64).
// This binds the signature to the specific HTTP method and path, preventing
// cross-endpoint replay attacks (e.g., a POST /submit sig replayed against POST /query).
func SignRequest(privateKey ed25519.PrivateKey, method, path string, body []byte, timestamp int64) []byte {
	message := buildRequestMessage(method, path, body, timestamp)
	return Sign(privateKey, message)
}

// VerifyRequest verifies an API request signature.
func VerifyRequest(publicKey ed25519.PublicKey, method, path string, body []byte, timestamp int64, signature []byte) bool {
	message := buildRequestMessage(method, path, body, timestamp)
	return Verify(publicKey, message, signature)
}

// buildRequestMessage constructs the message to sign for API requests.
// Format: SHA-256(method + " " + path + "\n" + body) || BigEndian(timestamp)
func buildRequestMessage(method, path string, body []byte, timestamp int64) []byte {
	// Build canonical request: "POST /v1/memory/submit\n<body>"
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	bodyHash := sha256.Sum256(canonical)

	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(timestamp)) // #nosec G115 -- timestamp from trusted int64
	message := make([]byte, 0, len(bodyHash)+8)
	message = append(message, bodyHash[:]...)
	message = append(message, tsBytes...)
	return message
}

// VerifyAgentProof re-verifies an agent's Ed25519 signature on-chain using the
// embedded proof fields from the transaction. Returns the verified agent ID
// (hex-encoded public key) or an error if verification fails.
//
// This is the critical on-chain identity verification — the ABCI handler uses
// this to independently establish agent identity without trusting the REST layer.
func VerifyAgentProof(agentPubKey, agentSig, bodyHash []byte, agentTimestamp int64) (string, error) {
	if len(agentPubKey) != ed25519.PublicKeySize {
		return "", fmt.Errorf("invalid agent public key length: %d", len(agentPubKey))
	}
	if len(agentSig) != ed25519.SignatureSize {
		return "", fmt.Errorf("invalid agent signature length: %d", len(agentSig))
	}
	if len(bodyHash) != 32 {
		return "", fmt.Errorf("invalid body hash length: %d", len(bodyHash))
	}

	// Check for zero-filled fields (no agent proof embedded)
	allZero := true
	for _, b := range agentPubKey {
		if b != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return "", fmt.Errorf("no agent identity proof in transaction")
	}

	// Reconstruct the signed message: bodyHash + bigEndian(timestamp)
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(agentTimestamp)) // #nosec G115 -- timestamp from trusted int64
	message := make([]byte, 0, 40)
	message = append(message, bodyHash...)
	message = append(message, tsBytes...)

	// Verify the agent's Ed25519 signature
	if !ed25519.Verify(ed25519.PublicKey(agentPubKey), message, agentSig) {
		return "", fmt.Errorf("agent signature verification failed")
	}

	return hex.EncodeToString(agentPubKey), nil
}
