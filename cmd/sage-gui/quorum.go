package main

import (
	"bufio"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// QuorumManifest is the portable file shared between nodes to form a quorum.
//
// Security: the CA private key is shipped as CAKeyEncrypted — an Argon2id +
// AES-256-GCM envelope keyed by a passphrase the user must communicate
// out-of-band (separate channel from the manifest itself). A plaintext CAKey
// field is no longer accepted; legacy manifests must be regenerated.
type QuorumManifest struct {
	ChainID         string              `json:"chain_id"`
	GenesisTime     string              `json:"genesis_time,omitempty"`      // RFC3339 — set by initiator
	CACert          string              `json:"ca_cert,omitempty"`           // PEM-encoded CA certificate for TLS
	CAKeyEncrypted  string              `json:"ca_key_encrypted,omitempty"`  // base64(EncryptCAKey envelope)
	Validators      []ManifestValidator `json:"validators"`
	Peers           []QuorumPeer        `json:"peers"`

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

func (v ManifestValidator) toGenesis() (cmttypes.GenesisValidator, error) {
	pubBytes, err := base64.StdEncoding.DecodeString(v.PubKey)
	if err != nil {
		return cmttypes.GenesisValidator{}, fmt.Errorf("decode pub_key: %w", err)
	}
	pubKey := ed25519.PubKey(pubBytes)
	return cmttypes.GenesisValidator{
		Address: pubKey.Address(),
		PubKey:  pubKey,
		Power:   v.Power,
		Name:    v.Name,
	}, nil
}

// QuorumPeer identifies a node in the quorum.
type QuorumPeer struct {
	NodeID  string `json:"node_id"`
	Address string `json:"address"` // host:port for P2P
	Name    string `json:"name"`
}

// runQuorumInit initializes a quorum network.
// It exports this node's validator info into a manifest file that peers import.
//
// Usage: sage-gui quorum-init [--name NAME] [--address HOST:PORT]
//
// The CA private key embedded in the manifest is encrypted with a passphrase.
// Provide it via the SAGE_QUORUM_PASSPHRASE env var or interactive prompt;
// share the passphrase with peers OUT-OF-BAND (Signal, voice, anything that
// isn't the same channel that carried the manifest).
func runQuorumInit() error {
	home := SageHome()
	cometHome := filepath.Join(home, "data", "cometbft")
	configDir := filepath.Join(cometHome, "config")

	// Parse args
	name := "node0"
	address := ""
	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--name":
			if i+1 < len(os.Args) {
				name = os.Args[i+1]
				i++
			}
		case "--address":
			if i+1 < len(os.Args) {
				address = os.Args[i+1]
				i++
			}
		}
	}

	if address == "" {
		return fmt.Errorf("--address HOST:PORT is required (your LAN address for P2P)")
	}

	passphrase, err := readQuorumPassphrase("Set a passphrase to encrypt the CA private key in the manifest.\n" +
		"Share this passphrase with peers OUT-OF-BAND (different channel from the manifest file).")
	if err != nil {
		return fmt.Errorf("read passphrase: %w", err)
	}

	// Ensure CometBFT is initialized
	if err = initCometBFTConfig(cometHome); err != nil {
		return fmt.Errorf("init CometBFT: %w", err)
	}

	// Load this node's validator key
	pv := privval.LoadFilePV(
		filepath.Join(configDir, "priv_validator_key.json"),
		filepath.Join(cometHome, "data", "priv_validator_state.json"),
	)

	// Load node key for P2P identity
	nodeKey, err := p2p.LoadNodeKey(filepath.Join(configDir, "node_key.json"))
	if err != nil {
		return fmt.Errorf("load node key: %w", err)
	}

	pubKeyB64 := base64.StdEncoding.EncodeToString(pv.Key.PubKey.Bytes())
	genesisTime := cmttime.Now()

	// Generate TLS CA and node certificate for encrypted quorum communication.
	certsDir := filepath.Join(home, "certs")
	caCert, caKey, err := tlsca.LoadOrGenerateCA(certsDir, "sage-quorum")
	if err != nil {
		return fmt.Errorf("generate TLS CA: %w", err)
	}

	host := tlsca.ParseHostPort(address)
	nodeCert, nodeKey2, err := tlsca.GenerateNodeCert(caCert, caKey, string(nodeKey.ID()), []string{host})
	if err != nil {
		return fmt.Errorf("generate node TLS cert: %w", err)
	}
	if writeErr := tlsca.WriteCert(filepath.Join(certsDir, tlsca.NodeCertFile), nodeCert); writeErr != nil {
		return fmt.Errorf("write node cert: %w", writeErr)
	}
	if writeErr := tlsca.WriteKey(filepath.Join(certsDir, tlsca.NodeKeyFile), nodeKey2); writeErr != nil {
		return fmt.Errorf("write node key: %w", writeErr)
	}

	// Encode CA cert (PEM) and encrypt CA key with the operator passphrase.
	// The plaintext key never lands on disk inside the manifest — only the
	// authenticated-encryption envelope does.
	caCertPEM := tlsca.EncodeCertPEM(caCert)
	caKeyPEM, err := tlsca.EncodeKeyPEM(caKey)
	if err != nil {
		return fmt.Errorf("encode CA key: %w", err)
	}
	caKeyEncrypted, err := tlsca.EncryptCAKey(caKeyPEM, passphrase)
	if err != nil {
		return fmt.Errorf("encrypt CA key: %w", err)
	}

	manifest := QuorumManifest{
		ChainID:        "sage-quorum",
		GenesisTime:    genesisTime.Format(time.RFC3339Nano),
		CACert:         caCertPEM,
		CAKeyEncrypted: caKeyEncrypted,
		Validators: []ManifestValidator{
			{
				Address: fmt.Sprintf("%X", pv.Key.PubKey.Address()),
				PubKey:  pubKeyB64,
				Power:   10,
				Name:    name,
			},
		},
		Peers: []QuorumPeer{
			{
				NodeID:  string(nodeKey.ID()),
				Address: address,
				Name:    name,
			},
		},
	}

	outPath := filepath.Join(home, "quorum-manifest.json")
	data, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal manifest: %w", err)
	}

	if err := os.WriteFile(outPath, data, 0600); err != nil {
		return fmt.Errorf("write manifest: %w", err)
	}

	fmt.Println("Quorum manifest generated!")
	fmt.Printf("  File:    %s\n", outPath)
	fmt.Printf("  Node:    %s (%s)\n", name, nodeKey.ID())
	fmt.Printf("  Address: %s\n", address)
	fmt.Printf("  TLS CA:  %s\n", filepath.Join(certsDir, tlsca.CACertFile))
	fmt.Printf("  TLS Cert: %s\n", filepath.Join(certsDir, tlsca.NodeCertFile))
	fmt.Println()
	fmt.Println("CA private key in the manifest is ENCRYPTED with your passphrase.")
	fmt.Println("Send the manifest file AND share the passphrase OUT-OF-BAND.")
	fmt.Println("Peers run:")
	fmt.Println("  sage-gui quorum-join --manifest <peer-manifest.json> --name <this-node> --address <this-host:port>")

	return nil
}

// runQuorumJoin joins a quorum by merging a peer's manifest with this node's validator.
// It generates a shared genesis and updates config.yaml with quorum settings.
//
// Usage: sage-gui quorum-join --manifest <path> --name NAME --address HOST:PORT
func runQuorumJoin() error {
	home := SageHome()
	cometHome := filepath.Join(home, "data", "cometbft")
	configDir := filepath.Join(cometHome, "config")

	// Parse args
	manifestPath := ""
	name := "node1"
	address := ""
	for i := 2; i < len(os.Args); i++ {
		switch os.Args[i] {
		case "--manifest":
			if i+1 < len(os.Args) {
				manifestPath = os.Args[i+1]
				i++
			}
		case "--name":
			if i+1 < len(os.Args) {
				name = os.Args[i+1]
				i++
			}
		case "--address":
			if i+1 < len(os.Args) {
				address = os.Args[i+1]
				i++
			}
		}
	}

	if manifestPath == "" {
		return fmt.Errorf("--manifest PATH is required")
	}
	if address == "" {
		return fmt.Errorf("--address HOST:PORT is required")
	}

	// Load peer manifest
	data, err := os.ReadFile(manifestPath) //nolint:gosec // manifestPath is from CLI args, not HTTP input
	if err != nil {
		return fmt.Errorf("read manifest: %w", err)
	}
	var peerManifest QuorumManifest
	if unmarshalErr := json.Unmarshal(data, &peerManifest); unmarshalErr != nil {
		return fmt.Errorf("parse manifest: %w", unmarshalErr)
	}

	// Refuse pre-encryption manifests outright. Plaintext CA keys in transit
	// were the credential-leak surface this flow was rebuilt to close.
	if peerManifest.LegacyCAKey != "" {
		return fmt.Errorf("manifest contains a plaintext ca_key field — this format is no longer accepted. " +
			"Re-run sage-gui quorum-init on the initiator to produce an encrypted manifest, then retry quorum-join")
	}

	// Decrypt the CA private key with the operator passphrase, but only if
	// the manifest actually carries one (the initiator's own follow-up call
	// re-uses an already-decrypted CA on disk).
	var decryptedCAKeyPEM string
	if peerManifest.CAKeyEncrypted != "" {
		passphrase, ppErr := readQuorumPassphrase("Enter the quorum passphrase that was shared out-of-band by the initiator.")
		if ppErr != nil {
			return fmt.Errorf("read passphrase: %w", ppErr)
		}
		decryptedCAKeyPEM, ppErr = tlsca.DecryptCAKey(peerManifest.CAKeyEncrypted, passphrase)
		if ppErr != nil {
			return fmt.Errorf("decrypt CA key: %w", ppErr)
		}
	}

	// Ensure CometBFT is initialized
	if initErr := initCometBFTConfig(cometHome); initErr != nil {
		return fmt.Errorf("init CometBFT: %w", initErr)
	}

	// Load this node's validator key
	pv := privval.LoadFilePV(
		filepath.Join(configDir, "priv_validator_key.json"),
		filepath.Join(cometHome, "data", "priv_validator_state.json"),
	)

	nodeKey, err := p2p.LoadNodeKey(filepath.Join(configDir, "node_key.json"))
	if err != nil {
		return fmt.Errorf("load node key: %w", err)
	}

	// Convert peer manifest validators to CometBFT genesis validators
	validators := make([]cmttypes.GenesisValidator, 0, len(peerManifest.Validators)+1)
	for _, mv := range peerManifest.Validators {
		gv, convErr := mv.toGenesis()
		if convErr != nil {
			return fmt.Errorf("convert peer validator %s: %w", mv.Name, convErr)
		}
		validators = append(validators, gv)
	}
	// Add ourselves
	validators = append(validators, cmttypes.GenesisValidator{
		Address: pv.Key.PubKey.Address(),
		PubKey:  pv.Key.PubKey,
		Power:   10,
		Name:    name,
	})

	// Build peer list (just the peer nodes, not ourselves)
	peers := make([]string, 0, len(peerManifest.Peers))
	for _, p := range peerManifest.Peers {
		peers = append(peers, fmt.Sprintf("%s@%s", p.NodeID, p.Address))
	}

	// Use the genesis time from the manifest (set by initiator) for determinism
	genesisTime := cmttime.Now()
	if peerManifest.GenesisTime != "" {
		if parsed, parseErr := time.Parse(time.RFC3339Nano, peerManifest.GenesisTime); parseErr == nil {
			genesisTime = parsed
		}
	}

	// Generate shared genesis with all validators
	genDoc := cmttypes.GenesisDoc{
		ChainID:         peerManifest.ChainID,
		GenesisTime:     genesisTime,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators:      validators,
	}
	if valErr := genDoc.ValidateAndComplete(); valErr != nil {
		return fmt.Errorf("validate genesis: %w", valErr)
	}

	// Back up existing genesis
	genesisPath := filepath.Join(configDir, "genesis.json")
	if _, statErr := os.Stat(genesisPath); statErr == nil {
		backupPath := genesisPath + ".bak"
		if copyErr := copyFile(genesisPath, backupPath); copyErr != nil {
			return fmt.Errorf("backup genesis: %w", copyErr)
		}
		fmt.Printf("  Backed up old genesis to %s\n", backupPath)
	}

	// Write new shared genesis
	if saveErr := genDoc.SaveAs(genesisPath); saveErr != nil {
		return fmt.Errorf("save genesis: %w", saveErr)
	}

	// Also need to wipe CometBFT state since genesis changed
	statePath := filepath.Join(cometHome, "data", "priv_validator_state.json")
	resetState := `{
  "height": "0",
  "round": 0,
  "step": 0
}`
	if writeErr := os.WriteFile(statePath, []byte(resetState), 0600); writeErr != nil {
		return fmt.Errorf("reset validator state: %w", writeErr)
	}

	// Remove old block data AND badger on-chain state (genesis changed, incompatible)
	blockDB := filepath.Join(cometHome, "data", "blockstore.db")
	stateDB := filepath.Join(cometHome, "data", "state.db")
	txDB := filepath.Join(cometHome, "data", "tx_index.db")
	evidenceDB := filepath.Join(cometHome, "data", "evidence.db")
	for _, db := range []string{blockDB, stateDB, txDB, evidenceDB} {
		os.RemoveAll(db) //nolint:errcheck
	}

	// Wipe BadgerDB on-chain state (AppHash would mismatch)
	badgerPath := filepath.Join(filepath.Dir(cometHome), "badger")
	if _, statErr := os.Stat(badgerPath); statErr == nil {
		fmt.Printf("  Resetting on-chain state (BadgerDB)...\n")
		os.RemoveAll(badgerPath) //nolint:errcheck
		os.MkdirAll(badgerPath, 0700) //nolint:errcheck
	}

	// Set up TLS certificates from the peer manifest's CA.
	certsDir := filepath.Join(home, "certs")
	if peerManifest.CACert != "" && decryptedCAKeyPEM != "" {
		if mkErr := os.MkdirAll(certsDir, 0700); mkErr != nil {
			return fmt.Errorf("create certs directory: %w", mkErr)
		}

		// Write the shared CA cert and key from the manifest.
		caCert, caErr := tlsca.DecodeCertPEM(peerManifest.CACert)
		if caErr != nil {
			return fmt.Errorf("decode CA cert from manifest: %w", caErr)
		}
		caKey, keyErr := tlsca.DecodeKeyPEM(decryptedCAKeyPEM)
		if keyErr != nil {
			return fmt.Errorf("decode CA key from manifest: %w", keyErr)
		}

		if writeErr := tlsca.WriteCert(filepath.Join(certsDir, tlsca.CACertFile), caCert); writeErr != nil {
			return fmt.Errorf("write CA cert: %w", writeErr)
		}
		if writeErr := tlsca.WriteKey(filepath.Join(certsDir, tlsca.CAKeyFile), caKey); writeErr != nil {
			return fmt.Errorf("write CA key: %w", writeErr)
		}

		// Generate this node's TLS certificate signed by the quorum CA.
		host := tlsca.ParseHostPort(address)
		nodeCert, nodeKeyTLS, certErr := tlsca.GenerateNodeCert(caCert, caKey, string(nodeKey.ID()), []string{host})
		if certErr != nil {
			return fmt.Errorf("generate node TLS cert: %w", certErr)
		}
		if writeErr := tlsca.WriteCert(filepath.Join(certsDir, tlsca.NodeCertFile), nodeCert); writeErr != nil {
			return fmt.Errorf("write node cert: %w", writeErr)
		}
		if writeErr := tlsca.WriteKey(filepath.Join(certsDir, tlsca.NodeKeyFile), nodeKeyTLS); writeErr != nil {
			return fmt.Errorf("write node key: %w", writeErr)
		}

		fmt.Printf("  TLS:        certificates generated from quorum CA\n")
	}

	// Update config.yaml with quorum settings
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	cfg.Quorum.Enabled = true
	cfg.Quorum.Peers = peers
	if err := SaveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}

	// Export our manifest so the peer can do the same. Re-share the
	// initiator's encrypted CA-key envelope verbatim — only someone with
	// the out-of-band passphrase can use it. Never re-emit a plaintext key.
	ourPubKeyB64 := base64.StdEncoding.EncodeToString(pv.Key.PubKey.Bytes())
	ourManifest := QuorumManifest{
		ChainID:        peerManifest.ChainID,
		CACert:         peerManifest.CACert,
		CAKeyEncrypted: peerManifest.CAKeyEncrypted,
		Validators: []ManifestValidator{
			{
				Address: fmt.Sprintf("%X", pv.Key.PubKey.Address()),
				PubKey:  ourPubKeyB64,
				Power:   10,
				Name:    name,
			},
		},
		Peers: []QuorumPeer{
			{
				NodeID:  string(nodeKey.ID()),
				Address: address,
				Name:    name,
			},
		},
	}

	ourManifestPath := filepath.Join(home, "quorum-manifest.json")
	ourData, _ := json.MarshalIndent(ourManifest, "", "  ")
	os.WriteFile(ourManifestPath, ourData, 0600) //nolint:errcheck

	fmt.Println("Quorum joined!")
	fmt.Printf("  Chain:      %s\n", peerManifest.ChainID)
	fmt.Printf("  Validators: %d\n", len(validators))
	for _, v := range validators {
		fmt.Printf("    - %s (power=%d)\n", v.Name, v.Power)
	}
	fmt.Printf("  Peers:      %s\n", peers)
	fmt.Println()
	fmt.Println("The peer node must also run quorum-join with YOUR manifest:")
	fmt.Printf("  sage-gui quorum-join --manifest %s --name <peer-name> --address <peer-host:port>\n", ourManifestPath)
	fmt.Println()
	fmt.Println("Then start both nodes: sage-gui serve")

	return nil
}

// copyFile copies a file from src to dst.
func copyFile(src, dst string) error {
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	return os.WriteFile(dst, data, 0600) //nolint:gosec // dst is server-controlled path
}

// readQuorumPassphrase returns the operator's passphrase used to wrap the CA
// key in a quorum manifest. SAGE_QUORUM_PASSPHRASE wins over the prompt so
// scripted installs and DMG launchers (no terminal) keep working.
//
// The minimum length below is a UX guardrail, not a strength claim — Argon2id
// does the heavy lifting. A short passphrase still buys you authenticated
// encryption against a passive observer; a strong one buys you survival
// against an offline attacker who got the manifest.
func readQuorumPassphrase(prompt string) (string, error) {
	const minLen = 8

	if env := strings.TrimSpace(os.Getenv("SAGE_QUORUM_PASSPHRASE")); env != "" {
		if len(env) < minLen {
			return "", fmt.Errorf("SAGE_QUORUM_PASSPHRASE must be at least %d characters", minLen)
		}
		return env, nil
	}

	if prompt != "" {
		fmt.Println(prompt)
	}
	fmt.Print("Passphrase: ")

	scanner := bufio.NewScanner(os.Stdin)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", err
		}
		return "", fmt.Errorf("no passphrase provided")
	}
	pass := strings.TrimSpace(scanner.Text())
	if len(pass) < minLen {
		return "", fmt.Errorf("passphrase must be at least %d characters", minLen)
	}
	return pass, nil
}
