package main

import "os"

// QuorumManifest describes historical mutable-roster ceremony artifacts.
// New chains use a finalized signed genesis from init-genesis.
//
// Security: the CA private key is shipped as CAKeyEncrypted — an Argon2id +
// AES-256-GCM envelope keyed by a passphrase the user must communicate
// out-of-band (separate channel from the manifest itself). A plaintext CAKey
// field is no longer accepted; legacy manifests must be regenerated.
type QuorumManifest struct {
	ChainID        string              `json:"chain_id"`
	GenesisTime    string              `json:"genesis_time,omitempty"`     // RFC3339 — set by initiator
	CACert         string              `json:"ca_cert,omitempty"`          // PEM-encoded CA certificate for TLS
	CAKeyEncrypted string              `json:"ca_key_encrypted,omitempty"` // base64(EncryptCAKey envelope)
	Validators     []ManifestValidator `json:"validators"`
	Peers          []QuorumPeer        `json:"peers"`

	// LegacyCAKey is read for the sole purpose of detecting and rejecting
	// pre-encryption manifests. Receiving this field triggers a hard error
	// and a regeneration prompt — we never use it.
	LegacyCAKey string `json:"ca_key,omitempty"`
}

// ManifestValidator is a JSON-portable validator (avoids CometBFT's amino interface).
type ManifestValidator struct {
	Address string `json:"address"`
	PubKey  string `json:"pub_key"` // base64-encoded Ed25519 public key
	Power   int64  `json:"power"`
	Name    string `json:"name"`
}

// QuorumPeer identifies a node in the quorum.
type QuorumPeer struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"` // host:port for P2P
	Name    string `json:"name"`
}

// The old quorum ceremony incrementally assembled and reminted unsigned
// genesis files. New chains require one Root-signed complete validator roster.
func runQuorumInit() error { return refuseLegacyQuorumGenesisCreation() }

func runQuorumJoin() error { return refuseLegacyQuorumGenesisCreation() }

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0600) //nolint:gosec // dst is server-controlled path
}
