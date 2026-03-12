package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

// QuorumManifest is the portable file shared between nodes to form a quorum.
type QuorumManifest struct {
	ChainID     string              `json:"chain_id"`
	GenesisTime string              `json:"genesis_time,omitempty"` // RFC3339 — set by initiator
	Validators  []ManifestValidator `json:"validators"`
	Peers       []QuorumPeer        `json:"peers"`
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

	// Ensure CometBFT is initialized
	if err := initCometBFTConfig(cometHome); err != nil {
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

	manifest := QuorumManifest{
		ChainID:     "sage-quorum",
		GenesisTime: genesisTime.Format(time.RFC3339Nano),
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
	fmt.Println()
	fmt.Println("Send this file to peer nodes, then run:")
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

	// Export our manifest so the peer can do the same
	ourPubKeyB64 := base64.StdEncoding.EncodeToString(pv.Key.PubKey.Bytes())
	ourManifest := QuorumManifest{
		ChainID: peerManifest.ChainID,
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
