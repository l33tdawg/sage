package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// legacySharedChainID is the literal chain_id every pre-v11 personal node was
// born with. Globally-unique per-node minting only landed in v11.0.0 (see
// mintChainID) and only applies to genesis CREATED on v11+, so every node
// upgraded from an older release still carries this shared id (the chain_id
// reconcile deliberately grandfathers it — no destructive re-genesis on upgrade).
//
// Because the id is IDENTICAL across independently-created nodes, federation's
// self-federation guard — which refuses when a scanned peer's chain_id equals our
// own (internal/federation/join_routes.go, manager.go, and the ABCI guard) — treats
// any two such nodes as the same network and blocks the connection. Two
// "sage-personal" nodes are also genuinely indistinguishable at the protocol level
// (cross_fed records + co-commit anchors + chain-qualified signatures are all keyed
// by chain_id), so the guard is correct: the id must be made unique to fix federation.
const legacySharedChainID = "sage-personal"

// instanceIsServing reports whether another SAGE node is already live on this home
// (Guard 4). Indirected through a var so tests can exercise the guard without a real
// listener and so the destructive path is decoupled from whatever is on the RPC port
// in the test environment.
var instanceIsServing = serveIsRunning

// remintLegacyChainID migrates a personal node off the shared legacy
// "sage-personal" chain_id onto a globally-unique minted id, so cross-node
// federation stops being blocked by the self-federation guard. Returns
// migrated=true only when the re-mint actually ran.
//
// MEMORY-SAFE: it reuses resetChainState — the same hardened fork-transition path
// (vault-key backup, VACUUM-INTO SQLite backup with a content-verified
// abort-before-destruct gate — structural quick_check + memories row-count parity)
// that every past upgrade already ran. SQLite, where
// ALL memories live, is backed up and never wiped; only the derived Badger +
// CometBFT chain state is rebuilt. The memories table has no chain_id column and
// recall reads purely by status, so changing the chain_id cannot orphan a memory.
//
// AVAILABILITY-SAFE: the destructive phase is NON-FATAL. If the chain reset or the
// genesis rewrite cannot complete (e.g. the backup integrity check aborts on a
// damaged DB, or a state store survives the wipe), the re-mint is skipped with a
// loud log and the node boots normally — federation stays broken (the pre-fix
// status quo) but memories and availability are untouched. It never propagates an
// error that would abort boot.
//
// GUARDED — only a genuine single-validator, non-networked personal node on the
// EXACT legacy literal, whose genesis validator is THIS node's own signing key, is
// migrated. Quorum/LAN-networked nodes and guests that adopted a host's genesis
// intentionally run a shared chain_id they don't solely own; re-minting them would
// fork them away from their network. And we never wipe chain state out from under
// another live instance.
//
// IDEMPOTENT — after the re-mint the id is no longer "sage-personal", so every
// subsequent boot is a no-op.
func remintLegacyChainID(dataDir string, cfg *Config, logger zerolog.Logger) (migrated bool, err error) {
	// Dev builds never touch persisted state (mirrors migrateOnUpgrade).
	if version == "dev" {
		return false, nil
	}

	cometHome := filepath.Join(dataDir, "cometbft")

	// Guard 1: quorum / LAN-networked nodes share a chain_id across validators by
	// design. Both the mode flag and any configured peers count — a node that ever
	// joined a shared network must never be re-minted.
	if cfg.Quorum.Enabled || len(cfg.Quorum.Peers) > 0 {
		return false, nil
	}

	// Guard 1b (defense-in-depth): the CometBFT address book records every peer this
	// node has DIALED. A standalone personal node dials nobody (0 addrs), so any
	// recorded peer means the node joined/participated in a P2P network — skip it,
	// even if its current config flags have been cleared.
	//
	// KNOWN RESIDUAL — this does NOT catch a Flow-3 *host*: SAGE runs quorum P2P with
	// PEX disabled (node.go), and a host only ever ACCEPTS dials (guests dial in), so
	// a host's address book stays empty. A pre-v11 host that hosted a LAN network and
	// then toggled Network Mode off is therefore indistinguishable on disk from a
	// standalone node and WILL be re-minted. This is accepted: (a) the window is tiny
	// (LAN hosting shipped in v11.0.0, days before this release, and requires an
	// explicit toggle-off before upgrading); (b) only the host re-mints — its guests
	// carry the host's validator key in genesis, so Guard 3b skips them and their
	// memories (own SQLite) are untouched; recovery is a one-time re-join. Flagged in
	// the release notes. There is no earlier-version disk trace to detect it by.
	if hasNetworkedPeers(cometHome) {
		logger.Warn().Msg("legacy chain_id re-mint skipped — address book shows dialed peers (this node participated in a P2P network)")
		return false, nil
	}

	// Read the authoritative chain_id from genesis. No genesis => brand-new install
	// (initCometBFTConfig mints a unique id downstream) — nothing to migrate.
	curID, readErr := readChainIDFromGenesis(cometHome)
	if readErr != nil || curID == "" {
		return false, nil
	}

	// Guard 2: only the exact legacy literal. Freshly-minted unique ids and legacy
	// "sage-quorum" are left untouched.
	if curID != legacySharedChainID {
		return false, nil
	}

	// Guard 3: single-validator (personal) genesis only. A multi-validator
	// "sage-personal" genesis would be an old shared network; re-minting would fork
	// it away from its peers.
	genesisPath := filepath.Join(cometHome, "config", "genesis.json")
	genDoc, gErr := cmttypes.GenesisDocFromFile(genesisPath)
	if gErr != nil {
		logger.Warn().Err(gErr).Msg("legacy chain_id re-mint skipped — genesis.json unreadable")
		return false, nil
	}
	if len(genDoc.Validators) != 1 {
		logger.Warn().Int("validators", len(genDoc.Validators)).
			Msg("legacy chain_id re-mint skipped — multi-validator genesis looks like a shared network")
		return false, nil
	}
	validator := genDoc.Validators[0]
	fallbackAppState := genDoc.AppState // preserve the existing chain-admin seed if we can't re-derive one

	// Guard 3b: the sole genesis validator must be OUR OWN signing key. A Flow-3
	// guest adopts the host's genesis verbatim, so its single validator is the HOST's
	// pubkey — re-minting would fork the guest onto a chain it can never sign
	// (priv_validator_key.json wouldn't match) and sever it from its network. The
	// quorum guard normally catches guests, but a MISSING config.yaml makes
	// LoadConfig silently return Quorum.Enabled=false, so verify key ownership
	// directly rather than trust the flag.
	localPub, lpErr := localValidatorPubKey(cometHome)
	if lpErr != nil {
		logger.Warn().Err(lpErr).Msg("legacy chain_id re-mint skipped — cannot read local validator key to confirm ownership")
		return false, nil
	}
	if !bytes.Equal(localPub, validator.PubKey.Bytes()) {
		logger.Warn().Msg("legacy chain_id re-mint skipped — genesis validator is not this node's key (adopted/guest genesis); re-minting would fork it off its network")
		return false, nil
	}

	// Guard 4: never wipe chain state out from under a live instance. If another
	// sage-gui is already serving on this SAGE_HOME (a common upgrade footgun:
	// starting the new binary before stopping the old), skip and let the normal
	// Badger directory lock surface the "already running" error cleanly, rather than
	// deleting the running node's databases mid-flight. serveIsRunning() probes this
	// node's own CometBFT RPC port, which is not yet up in this process.
	if instanceIsServing() {
		logger.Warn().Msg("legacy chain_id re-mint skipped — another SAGE instance appears to be running on this home")
		return false, nil
	}

	logger.Info().Str("old_chain_id", curID).
		Msg("legacy shared chain_id detected — re-minting a unique network identity so federation works (memories are backed up and preserved)")
	fmt.Fprintf(os.Stderr, "\n  SAGE: your node used the shared legacy network id %q, which blocks connecting to other people's networks.\n  Re-minting a unique id now — your memories are backed up first and never touched.\n  (If you had already connected to another network, you'll need to reconnect once.)\n\n", curID)

	badgerPath := filepath.Join(dataDir, "badger")
	sqlitePath := filepath.Join(dataDir, "sage.db")

	// Back up (vault + SQLite, verified) and wipe ONLY derived chain state.
	// NON-FATAL: the backup integrity check aborts BEFORE any wipe, so a failure here
	// leaves everything intact — skip the re-mint and let the node boot rather than
	// abort into a boot loop.
	if resetErr := resetChainState(dataDir, badgerPath, cometHome, sqlitePath, version); resetErr != nil {
		logger.Error().Err(resetErr).
			Msg("legacy chain_id re-mint skipped — chain backup/reset did not complete; node boots normally, federation stays disabled until resolved")
		fmt.Fprintf(os.Stderr, "  SAGE: could not safely re-mint the network id (%v). Your memories are untouched; the node is starting normally.\n\n", resetErr)
		return false, nil
	}

	// The genesis rewrite is only honored if CometBFT has NO cached genesis doc in
	// state.db (it prefers that cache over genesis.json). resetChainState removes the
	// block/state stores but is non-fatal on individual removal errors, so confirm
	// they are actually gone before rewriting — otherwise we'd advertise a new
	// chain_id while CometBFT silently keeps running the old chain (split-brain).
	// Skip and self-heal next boot if either survived.
	cometDataDir := filepath.Join(cometHome, "data")
	for _, dbName := range []string{"state.db", "blockstore.db"} {
		if _, statErr := os.Stat(filepath.Join(cometDataDir, dbName)); statErr == nil {
			logger.Error().Str("db", dbName).
				Msg("legacy chain_id re-mint skipped — chain state store survived the reset; retrying next boot to avoid a chain_id split-brain")
			return false, nil
		}
	}

	// Rewrite genesis.json with a fresh unique chain_id bound to the existing
	// validator key. NON-FATAL: on failure the node boots on the rebuilt legacy
	// chain and retries the re-mint next boot.
	newID, regenErr := regenGenesisUniqueID(cometHome, validator, fallbackAppState)
	if regenErr != nil {
		logger.Error().Err(regenErr).
			Msg("legacy chain_id re-mint: genesis rewrite failed after reset; node boots on the rebuilt legacy chain and will retry next boot")
		return false, nil
	}

	// TLS certs: the boot sequence's reconcileCACommonName (called from runServe's
	// cert block, after the chain_id reconcile and before cert auto-gen) detects that
	// the on-disk CA's CommonName "sage-ca-sage-personal" no longer matches the new
	// chain_id and regenerates all four cert files with the correct CN. It runs every
	// boot and handles partial-rotation states, so the re-mint itself needs to do
	// nothing here — a stale CN can never permanently block federation.

	logger.Info().Str("new_chain_id", newID).
		Msg("re-minted unique chain_id — federation self-federation guard will no longer misfire; chain rebuilds on first run, memories intact")
	fmt.Fprintf(os.Stderr, "  New network id: %s · memories intact · chain rebuilds on first run\n\n", newID)
	return true, nil
}

// regenGenesisUniqueID rewrites genesis.json with a freshly-minted globally-unique
// chain_id, reusing the passed-in validator (its pubkey, verified by the caller to
// be this node's own signing key, seeds both the genesis validator entry and the
// chain_id digest) so the node's consensus identity stays stable and continues to
// match priv_validator_key.json. fallbackAppState carries the existing genesis
// app_state forward when a fresh admin seed can't be derived (a corrupt agent.key),
// so the re-genesised chain never strands at the fork-ladder admin gate. The caller
// MUST have wiped the block/state store first (resetChainState) and confirmed
// state.db is gone. Returns the new chain_id.
func regenGenesisUniqueID(cometHome string, validator cmttypes.GenesisValidator, fallbackAppState json.RawMessage) (string, error) {
	configDir := filepath.Join(cometHome, "config")
	genesisPath := filepath.Join(configDir, "genesis.json")

	genesisTime := cmttime.Now()
	chainID, mintErr := mintChainID(legacySharedChainID, [][]byte{validator.PubKey.Bytes()}, genesisTime)
	if mintErr != nil {
		return "", fmt.Errorf("mint chain_id: %w", mintErr)
	}

	genDoc := cmttypes.GenesisDoc{
		ChainID:         chainID,
		GenesisTime:     genesisTime,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators: []cmttypes.GenesisValidator{
			{
				Address: validator.PubKey.Address(),
				PubKey:  validator.PubKey,
				Power:   10,
				Name:    "personal",
			},
		},
	}
	// Re-seed the operator's agent key as genesis chain-admin (issue #52) so the
	// re-genesised chain can climb the fork ladder without stranding at the propose
	// admin-gate. If the key can't be re-derived, carry the OLD genesis app_state
	// forward rather than emit an admin-less genesis.
	appState := genesisInitialAdminAppState()
	if len(appState) == 0 {
		appState = fallbackAppState
	}
	if len(appState) > 0 {
		genDoc.AppState = appState
	}
	if vErr := genDoc.ValidateAndComplete(); vErr != nil {
		return "", fmt.Errorf("validate genesis: %w", vErr)
	}

	// Back up the old genesis (best-effort; we are intentionally discarding it), then
	// write atomically (temp + rename) so a crash mid-write can never leave a
	// half-written genesis.json.
	if bkErr := copyFile(genesisPath, genesisPath+".pre-remint.bak"); bkErr != nil {
		// Non-fatal: the legacy genesis is being replaced on purpose. Log only.
		fmt.Fprintf(os.Stderr, "  Note: could not back up the pre-remint genesis: %v\n", bkErr)
	}
	tmpPath := genesisPath + ".tmp"
	if saveErr := genDoc.SaveAs(tmpPath); saveErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("stage genesis: %w", saveErr)
	}
	// fsync the temp file before the rename so a power loss can't leave a torn or
	// zero-length genesis.json (chain state is already wiped by this point, so a
	// corrupt genesis would fail every boot until manual restore of .pre-remint.bak).
	if syncErr := fsyncPath(tmpPath); syncErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("sync genesis: %w", syncErr)
	}
	if renErr := os.Rename(tmpPath, genesisPath); renErr != nil {
		_ = os.Remove(tmpPath)
		return "", fmt.Errorf("commit genesis: %w", renErr)
	}
	// fsync the directory so the rename itself is durable across power loss.
	_ = fsyncPath(configDir)
	return chainID, nil
}

// localValidatorPubKey returns the raw ed25519 public key bytes from this node's
// priv_validator_key.json — the identity CometBFT actually signs blocks with. Used
// to confirm a to-be-re-minted genesis validator is really this node's own key (and
// not a host's key inherited via an adopted/guest genesis) before any destructive
// action. Parses the JSON directly (rather than privval.LoadFilePV, which exits the
// process on a malformed file) so an unreadable key is a safe skip, not a crash.
func localValidatorPubKey(cometHome string) ([]byte, error) {
	path := filepath.Join(cometHome, "config", "priv_validator_key.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var pv struct {
		PubKey struct {
			Value string `json:"value"`
		} `json:"pub_key"`
	}
	if uErr := json.Unmarshal(data, &pv); uErr != nil {
		return nil, uErr
	}
	if pv.PubKey.Value == "" {
		return nil, fmt.Errorf("priv_validator_key.json has no pub_key value")
	}
	raw, decErr := base64.StdEncoding.DecodeString(pv.PubKey.Value)
	if decErr != nil {
		return nil, fmt.Errorf("decode validator pubkey: %w", decErr)
	}
	return raw, nil
}

// reconcileCACommonName self-heals a stale TLS CA CommonName on a personal node.
// The CA CN is "sage-ca-<chain_id>" and the federation handshake enforces it
// (requireChainCN); if the chain_id changed — e.g. a legacy "sage-personal" node was
// re-minted — this reconcile is the ONE mechanism that keeps the CA CommonName in
// step with the chain_id. Because it runs on every boot (not gated on the re-mint,
// which is idempotent-by-chain-id), a stale CN can never silently block federation
// forever, even across crashes or partial cert writes. Called BEFORE the cert
// auto-gen: when the CN no longer matches, it removes all four cert files so they
// regenerate with the correct CN. A CN mismatch implies no live agreement (a
// matching CA is a precondition for ever having federated, and re-mint wipes
// agreements anyway), so this strands no peer. Quorum CAs come from the shared
// genesis flow and are left alone. Returns true if certs were removed.
func reconcileCACommonName(certsDir, chainID string, quorum bool, logger zerolog.Logger) bool {
	if quorum || chainID == "" || !tlsca.CertsExist(certsDir) {
		return false
	}
	caCert, err := tlsca.ReadCert(filepath.Join(certsDir, tlsca.CACertFile))
	if err != nil {
		// Inconsistent state: node cert/key exist (CertsExist) but the CA is missing
		// or unreadable — e.g. a partial rotation that removed the CA but not the node
		// cert. Leaving it means the auto-gen (gated on CertsExist) is skipped and the
		// node presents a node cert with no CA. Clear all four to force a clean regen.
		logger.Warn().Err(err).Msg("TLS node cert present but CA missing/unreadable — clearing certs to regenerate a consistent set")
		removeOwnCerts(certsDir)
		return true
	}
	want := "sage-ca-" + chainID
	if caCert.Subject.CommonName == want {
		return false
	}
	logger.Warn().Str("have", caCert.Subject.CommonName).Str("want", want).
		Msg("TLS CA CommonName does not match chain_id — regenerating certs so federation presents the correct identity")
	removeOwnCerts(certsDir)
	return true
}

// removeOwnCerts deletes this node's own CA + node TLS material (best-effort).
func removeOwnCerts(certsDir string) {
	for _, f := range []string{tlsca.CACertFile, tlsca.CAKeyFile, tlsca.NodeCertFile, tlsca.NodeKeyFile} {
		_ = os.Remove(filepath.Join(certsDir, f))
	}
}

// fsyncPath flushes a file (or directory) to stable storage. Used to make the
// genesis temp-write + rename durable against power loss during the fleet-wide
// re-mint boot. A directory fsync makes the rename entry itself durable.
func fsyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()
	return f.Sync()
}

// hasNetworkedPeers reports whether the CometBFT address book records any DIALED
// peer — a durable signal that this node joined/participated in a P2P network. A
// standalone personal node dials nobody, so its address book stays empty (0 addrs).
// NOTE: this only sees dialed peers; a Flow-3 host (which only accepts inbound dials,
// PEX disabled) leaves an empty book — see the Guard 1b residual note. Absent or
// unreadable address book => treat as not networked (fresh / never-networked node).
func hasNetworkedPeers(cometHome string) bool {
	data, err := os.ReadFile(filepath.Join(cometHome, "config", "addrbook.json"))
	if err != nil {
		return false
	}
	var ab struct {
		Addrs []json.RawMessage `json:"addrs"`
	}
	if json.Unmarshal(data, &ab) != nil {
		return false
	}
	return len(ab.Addrs) > 0
}

