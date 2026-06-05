package voter

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
)

// LoadPrivValidatorKey extracts the Ed25519 private key from a CometBFT
// priv_validator_key.json file. This is the node's consensus signing key — the
// SAME key it signs blocks with — which the voter reuses to sign MemoryVote txs.
// Because the vote's signer ID is hex(pubkey) (auth.PublicKeyToAgentID) and the
// genesis validator ID is the same hex(pubkey), a vote signed with this key counts
// toward the node's slot in the quorum with no validator-set replacement.
//
// Used by cmd/sage-gui (via loadNodeSigningKey) and cmd/amid (both deployment
// modes), so all load sites share one parser.
func LoadPrivValidatorKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path) //nolint:gosec // operator-supplied key path
	if err != nil {
		return nil, fmt.Errorf("read priv_validator_key: %w", err)
	}

	var keyDoc struct {
		PrivKey struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"priv_key"`
	}
	if err = json.Unmarshal(data, &keyDoc); err != nil {
		return nil, fmt.Errorf("parse priv_validator_key json: %w", err)
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyDoc.PrivKey.Value)
	if err != nil {
		return nil, fmt.Errorf("decode priv_validator_key value: %w", err)
	}
	if len(keyBytes) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("invalid priv_validator_key length: got %d, want %d", len(keyBytes), ed25519.PrivateKeySize)
	}

	return ed25519.PrivateKey(keyBytes), nil
}
