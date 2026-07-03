package main

// Node-join (Phase 5b-3) — the machinery behind "add another computer to my
// SAGE network" (Flow 3). A joining machine runs its OWN full SAGE node that
// joins the host's network as a FULL NON-VALIDATOR PEER: it adopts the host's
// genesis verbatim (host stays the sole validator), p2p-syncs the shared
// consensus ledger, and holds the shared memory locally.
//
// Why non-validator by default: CometBFT liveness needs >2/3 of validators
// online, so promoting a joiner to validator would make a 1-node chain need
// BOTH nodes up — the host would halt the moment the guest sleeps. A
// non-validator peer syncs + submits writes (the host validator commits them)
// without ever coupling liveness. Validator promotion is a separate, deliberate
// governance action, never this flow.
//
// The bundle carries NO secrets: {chain_id, genesis.json (verbatim), host_peer}.
// Same-chain quorum authenticates over CometBFT's p2p (the guest pins the host's
// node ID from host_peer); the guest's own REST/TLS stays self-signed and its
// local tools connect over stdio (Flow 1). No CA key, no passphrase.

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/cometbft/cometbft/p2p"
	cmttypes "github.com/cometbft/cometbft/types"
)

// defaultP2PPort is CometBFT's default P2P port; used when the host's
// Quorum.P2PAddr doesn't pin an explicit one.
const defaultP2PPort = "26656"

// NodeJoinBundle is the secret-free payload the host hands a joining guest so it
// can re-home onto the host's chain as a non-validator peer.
type NodeJoinBundle struct {
	ChainID string `json:"chain_id"`
	// GenesisB64 is base64(the host's genesis.json, byte-for-byte) — the guest
	// must run the IDENTICAL genesis to land on the same chain / AppHash.
	GenesisB64 string `json:"genesis_b64"`
	// HostPeer is the CometBFT persistent-peer string nodeID@host-lan-ip:port the
	// guest dials. The nodeID pins the host's p2p identity (MITM-resistant).
	HostPeer string `json:"host_peer"`
	// AppVersion is the host's sage-gui version. The guest's ABCI app must be the
	// same build or its replayed AppHash can diverge from the host's, stalling
	// consensus — so applyNodeJoinBundle refuses a mismatch unless forced.
	AppVersion string `json:"app_version,omitempty"`
}

// nodeIDHostRe matches a CometBFT persistent-peer string: 40-hex node id, '@',
// then host:port. Host is validated separately via net.SplitHostPort.
var nodeIDRe = regexp.MustCompile(`^[0-9a-f]{40}$`)

// buildNodeJoinBundle assembles the host's join bundle for a guest that will
// reach this node at lanIP. It reads the host's genesis + p2p node id and never
// includes any private key material.
func buildNodeJoinBundle(lanIP string) (NodeJoinBundle, error) {
	home := SageHome()
	cometHome := filepath.Join(home, "data", "cometbft")
	configDir := filepath.Join(cometHome, "config")

	lanIP = strings.TrimSpace(lanIP)
	if net.ParseIP(lanIP) == nil {
		return NodeJoinBundle{}, fmt.Errorf("invalid lan ip %q", lanIP)
	}

	genesisPath := filepath.Join(configDir, "genesis.json")
	genBytes, err := os.ReadFile(genesisPath) //nolint:gosec // path under SAGE home
	if err != nil {
		return NodeJoinBundle{}, fmt.Errorf("read genesis: %w", err)
	}
	gen, err := cmttypes.GenesisDocFromJSON(genBytes)
	if err != nil {
		return NodeJoinBundle{}, fmt.Errorf("parse genesis: %w", err)
	}
	if gen.ChainID == "" {
		return NodeJoinBundle{}, fmt.Errorf("genesis has no chain_id")
	}

	nodeKey, err := p2p.LoadNodeKey(filepath.Join(configDir, "node_key.json"))
	if err != nil {
		return NodeJoinBundle{}, fmt.Errorf("load node key: %w", err)
	}

	port := hostP2PPort()
	hostPeer := fmt.Sprintf("%s@%s", nodeKey.ID(), net.JoinHostPort(lanIP, port))

	return NodeJoinBundle{
		ChainID:    gen.ChainID,
		GenesisB64: base64.StdEncoding.EncodeToString(genBytes),
		HostPeer:   hostPeer,
		AppVersion: version,
	}, nil
}

// hostP2PPort returns the port the host's CometBFT P2P listener uses, derived
// from Quorum.P2PAddr when set, else the CometBFT default.
func hostP2PPort() string {
	cfg, err := LoadConfig()
	if err != nil || cfg.Quorum.P2PAddr == "" {
		return defaultP2PPort
	}
	addr := strings.TrimPrefix(cfg.Quorum.P2PAddr, "tcp://")
	if _, port, splitErr := net.SplitHostPort(addr); splitErr == nil && port != "" {
		return port
	}
	return defaultP2PPort
}

// validateNodeJoinBundle checks a bundle is well-formed and internally
// consistent BEFORE any destructive local action. Returns the decoded genesis
// bytes so the caller doesn't decode twice.
func validateNodeJoinBundle(b NodeJoinBundle) (genesisBytes []byte, err error) {
	if strings.TrimSpace(b.ChainID) == "" {
		return nil, fmt.Errorf("bundle has no chain_id")
	}
	// host_peer: nodeID@host:port
	at := strings.IndexByte(b.HostPeer, '@')
	if at <= 0 {
		return nil, fmt.Errorf("host_peer must be nodeID@host:port")
	}
	if !nodeIDRe.MatchString(b.HostPeer[:at]) {
		return nil, fmt.Errorf("host_peer node id must be 40 hex chars")
	}
	host, port, splitErr := net.SplitHostPort(b.HostPeer[at+1:])
	if splitErr != nil || host == "" || port == "" {
		return nil, fmt.Errorf("host_peer address must be host:port")
	}
	if net.ParseIP(host) == nil {
		return nil, fmt.Errorf("host_peer host must be an IP address")
	}
	genesisBytes, err = base64.StdEncoding.DecodeString(b.GenesisB64)
	if err != nil {
		return nil, fmt.Errorf("decode genesis: %w", err)
	}
	gen, err := cmttypes.GenesisDocFromJSON(genesisBytes)
	if err != nil {
		return nil, fmt.Errorf("parse bundled genesis: %w", err)
	}
	if gen.ChainID != b.ChainID {
		return nil, fmt.Errorf("bundle chain_id %q does not match genesis chain_id %q", b.ChainID, gen.ChainID)
	}
	return genesisBytes, nil
}

// applyNodeJoinBundle re-homes THIS node onto the host's chain as a
// non-validator peer. It is destructive: it replaces the local genesis and
// WIPES local chain state (this node becomes a fresh syncer of the host's
// ledger). Memories in SQLite are NOT touched — only on-chain/CometBFT state is
// reset. The caller must restart `sage-gui serve` afterward. force skips the
// app-version compatibility guard (for power users on mixed dev builds).
func applyNodeJoinBundle(b NodeJoinBundle, force bool) error {
	genesisBytes, err := validateNodeJoinBundle(b)
	if err != nil {
		return err
	}

	// App-version guard BEFORE any destructive step: a guest running a
	// different ABCI build can replay the host's blocks to a divergent AppHash
	// and stall consensus. Refuse a mismatch unless explicitly forced.
	if !force && b.AppVersion != "" && b.AppVersion != version {
		return fmt.Errorf("host runs SAGE %s but this machine runs %s — install the same version before joining (or re-run with --force if you are sure)", b.AppVersion, version)
	}

	// Refuse to wipe chain state out from under a running node — that risks
	// BadgerDB/CometBFT corruption. Check BEFORE any destructive step.
	if serveIsRunning() {
		return fmt.Errorf("a SAGE node appears to be running (CometBFT RPC on :26657 is up) — stop it before joining a network")
	}

	home := SageHome()
	cometHome := filepath.Join(home, "data", "cometbft")
	configDir := filepath.Join(cometHome, "config")

	// Ensure this node has its own CometBFT identity (node key + validator key).
	// A brand-new joiner has none; initCometBFTConfig creates them and seeds a
	// throwaway local genesis we immediately overwrite below.
	if initErr := initCometBFTConfig(cometHome); initErr != nil {
		return fmt.Errorf("init CometBFT: %w", initErr)
	}

	// Back up and replace the local genesis with the host's, VERBATIM. Writing
	// the exact bytes (not a re-marshalled doc) guarantees an identical genesis
	// hash / height-1 AppHash so this node lands on the host's chain.
	genesisPath := filepath.Join(configDir, "genesis.json")
	if _, statErr := os.Stat(genesisPath); statErr == nil {
		if copyErr := copyFile(genesisPath, genesisPath+".bak"); copyErr != nil {
			return fmt.Errorf("backup genesis: %w", copyErr)
		}
	}
	if writeErr := os.WriteFile(genesisPath, genesisBytes, 0600); writeErr != nil {
		return fmt.Errorf("write genesis: %w", writeErr)
	}

	// Wipe local chain state — the previous personal chain is incompatible with
	// the adopted genesis. Mirrors runQuorumJoin: reset priv_validator_state to
	// height 0, drop CometBFT DBs, and reset BadgerDB (AppHash would mismatch).
	if wipeErr := wipeChainStateForJoin(cometHome, home); wipeErr != nil {
		return wipeErr
	}

	// Point config at the host: quorum mode (p2p peers + 0.0.0.0 binds), the
	// host as our sole persistent peer, and the adopted chain_id.
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}
	cfg.Quorum.Enabled = true
	cfg.Quorum.Peers = []string{b.HostPeer}
	cfg.ChainID = b.ChainID
	if err := SaveConfig(cfg); err != nil {
		return fmt.Errorf("save config: %w", err)
	}
	return nil
}

// wipeChainStateForJoin resets CometBFT + BadgerDB state so this node re-syncs
// the adopted chain from genesis. SQLite memories are untouched.
func wipeChainStateForJoin(cometHome, home string) error {
	dataDir := filepath.Join(cometHome, "data")

	// Reset validator signing state to height 0 (CometBFT refuses to sign below
	// its last-signed height; the adopted chain starts at 1).
	pvStatePath := filepath.Join(dataDir, "priv_validator_state.json")
	if writeErr := os.WriteFile(pvStatePath, []byte(`{"height":"0","round":0,"step":0}`), 0600); writeErr != nil {
		return fmt.Errorf("reset validator state: %w", writeErr)
	}

	for _, db := range []string{"blockstore.db", "state.db", "tx_index.db", "evidence.db"} {
		if rmErr := os.RemoveAll(filepath.Join(dataDir, db)); rmErr != nil {
			return fmt.Errorf("remove %s: %w", db, rmErr)
		}
	}

	badgerPath := filepath.Join(home, "data", "badger")
	if _, statErr := os.Stat(badgerPath); statErr == nil {
		if rmErr := os.RemoveAll(badgerPath); rmErr != nil {
			return fmt.Errorf("reset badger: %w", rmErr)
		}
		if mkErr := os.MkdirAll(badgerPath, 0700); mkErr != nil {
			return fmt.Errorf("recreate badger: %w", mkErr)
		}
	}
	return nil
}

// serveIsRunning reports whether a local SAGE node appears to be running, by
// probing the CometBFT RPC port. Used to refuse a destructive join-apply that
// would corrupt a live node's on-disk state. It resolves the ACTUAL RPC bind
// (honouring the SAGE_CMT_RPC_ADDR override) rather than a hardcoded port, so a
// node running on a non-default RPC port is still detected — a wildcard listen
// host is dialed as loopback.
func serveIsRunning() bool {
	addr := strings.TrimPrefix(cmtRPCAddr(), "tcp://")
	addr = strings.Replace(addr, "0.0.0.0", "127.0.0.1", 1)
	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// parseNodeJoinBundleJSON decodes a bundle from JSON (used by the CLI and the
// ceremony client).
func parseNodeJoinBundleJSON(data []byte) (NodeJoinBundle, error) {
	var b NodeJoinBundle
	if err := json.Unmarshal(data, &b); err != nil {
		return NodeJoinBundle{}, fmt.Errorf("parse bundle: %w", err)
	}
	return b, nil
}
