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
	message := buildRequestMessage(method, path, body, timestamp, nil)
	return Sign(privateKey, message)
}

// SignRequestWithNonce creates a signature that includes a random nonce,
// preventing replay collisions when multiple requests share the same
// method+path+body+timestamp (i.e., within the same second).
func SignRequestWithNonce(privateKey ed25519.PrivateKey, method, path string, body []byte, timestamp int64, nonce []byte) []byte {
	message := buildRequestMessage(method, path, body, timestamp, nonce)
	return Sign(privateKey, message)
}

// VerifyRequest verifies an API request signature (without nonce — backward compatible).
func VerifyRequest(publicKey ed25519.PublicKey, method, path string, body []byte, timestamp int64, signature []byte) bool {
	message := buildRequestMessage(method, path, body, timestamp, nil)
	return Verify(publicKey, message, signature)
}

// VerifyRequestWithNonce verifies an API request signature that includes a nonce.
func VerifyRequestWithNonce(publicKey ed25519.PublicKey, method, path string, body []byte, timestamp int64, nonce []byte, signature []byte) bool {
	message := buildRequestMessage(method, path, body, timestamp, nonce)
	return Verify(publicKey, message, signature)
}

// buildRequestMessage constructs the message to sign for API requests.
// Format: SHA-256(method + " " + path + "\n" + body) || BigEndian(timestamp) [|| nonce]
// The nonce is appended only when non-nil, maintaining backward compatibility.
func buildRequestMessage(method, path string, body []byte, timestamp int64, nonce []byte) []byte {
	// Build canonical request: "POST /v1/memory/submit\n<body>"
	canonical := []byte(method + " " + path + "\n")
	canonical = append(canonical, body...)
	bodyHash := sha256.Sum256(canonical)

	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(timestamp)) // #nosec G115 -- timestamp from trusted int64
	message := make([]byte, 0, len(bodyHash)+8+len(nonce))
	message = append(message, bodyHash[:]...)
	message = append(message, tsBytes...)
	if len(nonce) > 0 {
		message = append(message, nonce...)
	}
	return message
}

// --- X-Sig-Version=2 — chain-qualified request signing (v11 federation) ---
//
// V1 signatures bind method+path+body+timestamp[+nonce] but carry no notion of
// WHICH network the request is for: the same agent key used on two chains
// produces byte-identical signatures, so a request captured on chain A replays
// verbatim against chain B. V2 folds a domain-separation hash of
// (senderChainID, receiverChainID) into the signed message as a fixed-width
// PREFIX (before the variable-length nonce), making the canonical bytes
// unambiguous and un-spliceable. A v2 message can never collide with a v1
// message: interpreting a v2 message as v1 would require the chain hash to
// equal SHA-256 of a valid canonical request line, a second-preimage.

// sigV2DomainTag domain-separates the chain-binding hash from every other
// SHA-256 use in the protocol.
const sigV2DomainTag = "sage-fed-sig-v2"

// chainBindingHash = SHA-256(tag \x00 sender \x00 receiver). NUL separators make
// the encoding injective (chain ids never contain NUL — genesis ids are
// [a-z0-9-] — so no two (sender, receiver) pairs share a preimage).
func chainBindingHash(senderChainID, receiverChainID string) [32]byte {
	return sha256.Sum256([]byte(sigV2DomainTag + "\x00" + senderChainID + "\x00" + receiverChainID))
}

// buildRequestMessageV2 constructs the chain-qualified message to sign.
// Format: chainBindingHash(32) || SHA-256(method + " " + path + "\n" + body)(32)
// || BigEndian(timestamp)(8) || nonce.
func buildRequestMessageV2(senderChainID, receiverChainID, method, path string, body []byte, timestamp int64, nonce []byte) []byte {
	ch := chainBindingHash(senderChainID, receiverChainID)
	return append(ch[:], buildRequestMessage(method, path, body, timestamp, nonce)...)
}

// SignRequestV2 signs a cross-chain federation request. The signature binds the
// sending chain AND the intended receiving chain, so it cannot be replayed
// against a third chain or reflected back at the sender.
func SignRequestV2(privateKey ed25519.PrivateKey, senderChainID, receiverChainID, method, path string, body []byte, timestamp int64, nonce []byte) []byte {
	return Sign(privateKey, buildRequestMessageV2(senderChainID, receiverChainID, method, path, body, timestamp, nonce))
}

// VerifyRequestV2 verifies a chain-qualified federation request signature. The
// verifier passes its OWN chain id as receiverChainID and the claimed sender
// chain id — a signature minted for any other (sender, receiver) pair fails.
func VerifyRequestV2(publicKey ed25519.PublicKey, senderChainID, receiverChainID, method, path string, body []byte, timestamp int64, nonce []byte, signature []byte) bool {
	return Verify(publicKey, buildRequestMessageV2(senderChainID, receiverChainID, method, path, body, timestamp, nonce), signature)
}

// VerifyAgentProof re-verifies an agent's Ed25519 signature on-chain using the
// embedded proof fields from the transaction. Returns the verified agent ID
// (hex-encoded public key) or an error if verification fails.
//
// This is the critical on-chain identity verification — the ABCI handler uses
// this to independently establish agent identity without trusting the REST layer.
func VerifyAgentProof(agentPubKey, agentSig, bodyHash []byte, agentTimestamp int64, agentNonce []byte) (string, error) {
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

	// Reconstruct the signed message: bodyHash + bigEndian(timestamp) [+ nonce]
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(agentTimestamp)) // #nosec G115 -- timestamp from trusted int64
	message := make([]byte, 0, 40+len(agentNonce))
	message = append(message, bodyHash...)
	message = append(message, tsBytes...)
	if len(agentNonce) > 0 {
		message = append(message, agentNonce...)
	}

	// Verify the agent's Ed25519 signature
	if !ed25519.Verify(ed25519.PublicKey(agentPubKey), message, agentSig) {
		return "", fmt.Errorf("agent signature verification failed")
	}

	return hex.EncodeToString(agentPubKey), nil
}
