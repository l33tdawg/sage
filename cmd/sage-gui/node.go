package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/api/rest"
	"github.com/l33tdawg/sage/api/rest/middleware"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/mcp"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/ollamad"
	"github.com/l33tdawg/sage/internal/orchestrator"
	"github.com/l33tdawg/sage/internal/rerankd"
	"github.com/l33tdawg/sage/internal/snapshot"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/l33tdawg/sage/internal/voter"
	"github.com/l33tdawg/sage/web"
)

// resolveRetainBlocks maps the retain_blocks config knob to the effective
// retention window: 0 = mode default (personal 100k, quorum disabled),
// negative = explicitly keep everything, positive = operator's window.
// Factored out of runServe so the mode-default policy is unit-testable.
func resolveRetainBlocks(configured int64, quorumEnabled bool) int64 {
	switch {
	case configured < 0:
		return 0
	case configured == 0:
		if quorumEnabled {
			return 0
		}
		return 100_000
	default:
		return configured
	}
}

func runServe() (rerr error) {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// v7.5: catch panics, write HALT sentinel so the launcher's
	// --supervise mode can roll back. Re-panics so the original
	// stack still reaches stderr and the process exits non-zero.
	defer haltOnPanic(cfg.DataDir)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Str("service", "sage-gui").Logger()

	logger.Info().
		Str("data_dir", cfg.DataDir).
		Str("rest_addr", cfg.RESTAddr).
		Str("embedding", cfg.Embedding.Provider).
		Msg("starting SAGE Personal node")

	// Ensure directories exist
	cometHome := filepath.Join(cfg.DataDir, "cometbft")
	badgerPath := filepath.Join(cfg.DataDir, "badger")
	sqlitePath := filepath.Join(cfg.DataDir, "sage.db")

	for _, dir := range []string{cfg.DataDir, cometHome, badgerPath} {
		if mkErr := os.MkdirAll(dir, 0700); mkErr != nil {
			return fmt.Errorf("create dir %s: %w", dir, mkErr)
		}
	}

	// Pending LAN join (Flow 3, guest side): the "Join a network" dashboard flow
	// decrypts the host bundle, stages it, and restarts. Apply it HERE — before
	// any store opens or genesis is seeded — so the destructive wipe+adopt runs
	// on a closed chain, then normal startup proceeds already joined.
	if applied, joinErr := applyPendingJoinAtStartup(logger); joinErr != nil {
		return fmt.Errorf("apply pending network join: %w", joinErr)
	} else if applied {
		logger.Info().Msg("applied pending network join — this node is now a peer on the host's network")
		// doWipeAndAdopt wrote quorum mode + the host peer to config.yaml. Reload
		// so THIS same-process CometBFT startup binds LAN P2P and dials the host
		// (a stale in-memory cfg would start the node isolated on loopback), and
		// so the later encryption-reconcile SaveConfig can't clobber the quorum flip.
		reloaded, reloadErr := LoadConfig()
		if reloadErr != nil {
			return fmt.Errorf("reload config after network join: %w", reloadErr)
		}
		cfg = reloaded
	}

	// Auto-migrate on version upgrade: backup SQLite, reset chain state
	if migrated, migrateErr := migrateOnUpgrade(cfg.DataDir); migrateErr != nil {
		return fmt.Errorf("upgrade migration: %w", migrateErr)
	} else if migrated {
		logger.Info().
			Str("version", version).
			Msg("upgrade migration completed — chain state reset, memories preserved")
	}

	// Federation fix: pre-v11 personal nodes were all born with the identical
	// "sage-personal" chain_id, which the federation self-federation guard treats
	// as the same network — so two distinct users could never connect. Re-mint a
	// globally-unique id (memories backed up + preserved; quorum/joined nodes and
	// already-unique ids are skipped). Runs AFTER migrateOnUpgrade (whose reset
	// keeps the legacy genesis) and BEFORE ensureGenesisSeed + the chain_id
	// reconcile below, so the new id flows into cfg.ChainID for this same boot.
	if remolded, remintErr := remintLegacyChainID(cfg.DataDir, cfg, logger); remintErr != nil {
		return fmt.Errorf("re-mint legacy chain_id: %w", remintErr)
	} else if remolded {
		logger.Info().Msg("legacy shared chain_id re-minted — cross-node federation is now unblocked")
	}

	// Initialize CometBFT config (seeds a brand-new chain's genesis with the operator
	// admin) and, if a prior admin-less genesis survived a reset, re-inject the seed.
	// One helper so the heal step can't be dropped from serve unnoticed (issue #52).
	if initErr := ensureGenesisSeed(cometHome, logger); initErr != nil {
		return initErr
	}

	// Reconcile the persisted chain_id from the authoritative genesis into
	// config.yaml so it is surfaced read-only and available before CometBFT is
	// up (the federation identity + same-id collision guard depend on it). This
	// grandfathers existing chains: their genesis id — including legacy
	// "sage-personal"/"sage-quorum" — flows into config on next boot with no
	// destructive re-genesis. New chains minted a unique id in ensureGenesisSeed
	// above. One-shot: after the first reconcile cfg.ChainID matches and no
	// further write occurs.
	if genChainID, cidErr := readChainIDFromGenesis(cometHome); cidErr == nil && genChainID != "" && genChainID != cfg.ChainID {
		cfg.ChainID = genChainID // in-memory, for this running process
		// persistChainID writes ONLY the chain_id delta, preserving the raw
		// (un-expanded) paths in config.yaml — SaveConfig(cfg) here would bake the
		// LoadConfig-expanded absolute DataDir/AgentKey into the file.
		if saveErr := persistChainID(genChainID); saveErr != nil {
			logger.Warn().Err(saveErr).Msg("failed to persist reconciled chain_id to config.yaml")
		} else {
			logger.Info().Str("chain_id", genChainID).Msg("reconciled chain_id from genesis")
		}
	}

	// Create SQLite store. ctx is cancelled on shutdown (below) so the long-lived
	// goroutines that take it — the memory auto-voter, pipeline TTL cleanup — stop
	// cleanly instead of leaking until process exit.
	ctx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	sqliteStore, err := store.NewSQLiteStore(ctx, sqlitePath)
	if err != nil {
		return fmt.Errorf("open SQLite: %w", err)
	}
	defer func() { _ = sqliteStore.Close() }()

	// Optional cross-encoder reranker on the hybrid recall path. Off by
	// default; operator turns it on with SAGE_RERANK_ENABLED=1 and
	// SAGE_RERANK_URL=<tei-endpoint>. v7.1 ships this as additive polish
	// over the v7.0 hybrid baseline.
	if rerankCfg := embedding.ResolveRerankerConfig(); rerankCfg.Enabled && rerankCfg.URL != "" {
		if rr := embedding.BuildReranker(rerankCfg); rr != nil {
			sqliteStore.SetReranker(rr, rerankCfg.Oversample)
			logger.Info().
				Str("url", rerankCfg.URL).
				Str("model", rerankCfg.Model).
				Int("oversample", rerankCfg.Oversample).
				Int("timeout_ms", rerankCfg.TimeoutMS).
				Msg("hybrid recall reranker enabled")
		}
	}
	// Manager for the optional llama.cpp reranker sidecar (guided setup from
	// the dashboard; adopt-or-spawn on boot when the operator enabled it).
	rerankdMgr := rerankd.New(SageHome())
	// Manager for the local Ollama runtime used by smart memory setup. It
	// follows the same adopt-or-spawn model as rerankd.
	ollamaMgr := ollamad.New(SageHome())

	// Persisted reranker intent (Settings > Engine toggle) overrides the env
	// config so an operator's dashboard on/off choice survives restart without
	// needing SAGE_RERANK_* env vars. A stored "0" explicitly turns it off.
	if prefs, perr := sqliteStore.GetAllPreferences(context.Background()); perr == nil {
		if v, ok := prefs["reranker_enabled"]; ok {
			cfg := embedding.ResolveRerankerConfig()
			cfg.Enabled = v == "1" || v == "true"
			if u := prefs["reranker_url"]; u != "" {
				cfg.URL = u
			}
			if m := prefs["reranker_model"]; m != "" {
				cfg.Model = m
			}
			if k := prefs["reranker_kind"]; k != "" {
				cfg.Kind = k
			}
			sqliteStore.SetReranker(embedding.BuildReranker(cfg), cfg.Oversample)
			logger.Info().Bool("enabled", cfg.Enabled).Str("url", cfg.URL).Msg("reranker applied from saved preferences")
		}
		// Managed sidecar: re-establish the llama.cpp reranker this operator
		// enabled through the guided setup. Adopt-or-spawn runs in the
		// background - recalls degrade gracefully (RRF ordering) until the
		// sidecar answers, so boot is never blocked on it.
		if prefs["reranker_managed"] == "1" {
			go func() {
				startCtx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
				defer cancel()
				rerankURL, startErr := rerankdMgr.Start(startCtx)
				if startErr != nil {
					logger.Warn().Err(startErr).Msg("managed reranker sidecar did not start; recall continues without reranking")
					return
				}
				logger.Info().Str("url", rerankURL).Msg("managed reranker sidecar ready")
			}()
		}
		if prefs["ollama_managed"] == "1" {
			go func() {
				startCtx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
				defer cancel()
				ollamaURL, startErr := ollamaMgr.Start(startCtx)
				if startErr != nil {
					logger.Warn().Err(startErr).Msg("managed Ollama runtime did not start; semantic recall may be unavailable until setup runs")
					return
				}
				logger.Info().Str("url", ollamaURL).Msg("managed Ollama runtime ready")
			}()
		}
	}

	// Unlock encryption vault if enabled
	vaultKeyPath := filepath.Join(SageHome(), "vault.key")

	// Safeguard: if vault.key exists but encryption is disabled in config,
	// auto-re-enable to prevent silent encryption downgrade.
	if !cfg.Encryption.Enabled && vault.Exists(vaultKeyPath) {
		logger.Warn().Msg("vault.key exists but encryption disabled in config — auto-re-enabling encryption")
		cfg.Encryption.Enabled = true
		if saveErr := SaveConfig(cfg); saveErr != nil {
			logger.Error().Err(saveErr).Msg("failed to save re-enabled encryption config")
		}
	}

	vaultUnlocked := false
	// vaultPassphrase is kept in scope past the unlock block so the
	// v7.5 snapshot scheduler can use it as the at-rest encryption
	// passphrase. Empty when encryption is off OR the vault wasn't
	// unlocked at boot (e.g. launched from app icon with no terminal).
	var vaultPassphrase string
	if cfg.Encryption.Enabled {
		if !vault.Exists(vaultKeyPath) {
			return fmt.Errorf("encryption enabled but vault.key not found at %s — run 'sage-gui setup' first", vaultKeyPath)
		}

		// Mark that encryption is expected — writes will be rejected if vault stays locked.
		sqliteStore.SetVaultExpected(true)

		passphrase := os.Getenv("SAGE_PASSPHRASE")
		if passphrase == "" {
			fmt.Print("  Enter vault passphrase: ")
			passphrase, err = readPassphrase()
			if err != nil {
				// No terminal available (e.g. launched from app icon) —
				// server starts with encryption flag on but vault locked.
				// The web UI will show a login screen for the user to unlock.
				// Writes are blocked until then (vaultExpected = true, vault = nil).
				logger.Warn().Msg("no passphrase available — Synaptic Ledger locked (writes blocked until unlock via CEREBRUM)")
				passphrase = ""
			}
		}

		if passphrase != "" {
			v, vaultErr := vault.Open(vaultKeyPath, passphrase)
			if vaultErr != nil {
				return fmt.Errorf("unlock vault: %w", vaultErr)
			}
			sqliteStore.SetVault(v)
			vaultUnlocked = true
			vaultPassphrase = passphrase
			logger.Info().Msg("Synaptic Ledger unlocked — memories are encrypted at rest (AES-256-GCM)")
		}
	}

	// Create BadgerDB store
	badgerStore, err := store.NewBadgerStore(badgerPath)
	if err != nil {
		return fmt.Errorf("open BadgerDB: %w", err)
	}
	defer badgerStore.CloseBadger() //nolint:errcheck

	// Seed the replay-nonce allocator from the chain's committed nonces. The
	// app-v9 consensus gate rejects any tx whose nonce <= the signer's highest
	// committed nonce, so on a fresh process / post-restart every in-process
	// producer (REST, web, watchdog, validator txs — all on the shared node key)
	// must resume ABOVE the chain instead of trusting the wall clock to exceed it.
	// Local badger read, keyed exactly like the consensus path
	// (auth.PublicKeyToAgentID), consulted at most once per key. Liveness-only,
	// never in the AppHash. GetNonce returns 0 for an unseen key -> no-op seed.
	tx.SetNonceFloorFunc(func(pub ed25519.PublicKey) (uint64, bool) {
		n, gerr := badgerStore.GetNonce(auth.PublicKeyToAgentID(pub))
		if gerr != nil || n == 0 {
			return 0, false
		}
		return n, true
	})

	// Create SAGE ABCI app with SQLite backend
	app, err := sageabci.NewSageAppWithStores(badgerStore, sqliteStore, logger)
	if err != nil {
		return fmt.Errorf("create SAGE app: %w", err)
	}
	app.Version = version
	defer func() { _ = app.Close() }()

	// Block-retention window (issue #40): bound blockstore growth by telling
	// CometBFT it may prune blocks older than the window. Local node policy —
	// never consensus state — so modes can default differently: personal nodes
	// (single validator, no peers ever sync from them) prune by default;
	// quorum nodes keep everything unless the operator opts in, because a
	// fresh peer block-syncs history from the existing validators.
	if retainBlocks := resolveRetainBlocks(cfg.RetainBlocks, cfg.Quorum.Enabled); retainBlocks > 0 {
		app.SetRetainBlocks(retainBlocks)
		logger.Info().Int64("retain_blocks", retainBlocks).Msg("block retention armed — CometBFT will prune blocks older than the window")
	}

	// Content-validation enforcement advisory (non-fatal): warn when the app-v7
	// fork is active on this chain but this binary has no validator registry
	// compiled in, so this node won't enforce the Layer-2 gate. Bootable by
	// design — a generic-only fleet is valid — but a MIXED fleet (some nodes
	// wired, some not) would diverge the AppHash, so surface it loudly.
	if warn := app.ContentValidationEnforcementWarning(); warn != "" {
		logger.Warn().Msg(warn)
	}

	// v7.5 snapshot scheduler: anchor every 10k blocks and every 6h
	// of wall time. Snapshots include the live binary so a rollback
	// can re-exec without operator intervention. Encryption posture
	// inherits the vault state. nil when intervals are zero (e.g.
	// in tests/single-shot CLIs) — SageApp.Tick is nil-safe.
	// Snapshot encryption inherits the vault posture: encrypt at rest
	// when the vault is unlocked AND we know the passphrase. Without
	// the passphrase (no-terminal boot path) we ship plaintext snapshots
	// — better than skipping snapshots entirely, since rollback is
	// only possible if anchors exist on disk.
	snapEncrypted := vaultUnlocked && vaultPassphrase != ""
	// Snapshot retention: keep the N newest snapshots (plus one anchor per
	// binary version, which is never pruned). Override with SAGE_SNAPSHOT_KEEP
	// (must be >=1); default 5. Before v9.2.2 retention was never wired, so
	// long-lived nodes accumulated snapshots unbounded.
	snapKeep := 5
	if v := os.Getenv("SAGE_SNAPSHOT_KEEP"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			snapKeep = n
		} else {
			logger.Warn().Str("value", v).Int("using", snapKeep).Msg("ignoring invalid SAGE_SNAPSHOT_KEEP (want integer >= 1)")
		}
	}
	if sched := sageabci.NewSnapshotScheduler(sageabci.SnapshotSchedulerConfig{
		DataDir:         cfg.DataDir,
		BinaryVersion:   version,
		VaultKeyPath:    vaultKeyPath,
		VaultEncrypted:  snapEncrypted,
		VaultPassphrase: vaultPassphrase,
		HeightInterval:  10_000,
		TimeInterval:    6 * time.Hour,
		KeepLast:        snapKeep,
		LiveBadger:      badgerStore.DB(),
	}, logger); sched != nil {
		app.SetSnapshotScheduler(sched)
		logger.Info().Msg("v7.5 snapshot scheduler armed")
	}

	// Snapshot housekeeping (off the consensus path — touches only
	// DataDir/snapshots/). Runs ASYNC so a large backlog doesn't delay boot:
	// reap .staging-* dirs left by crashed Takes, then prune all but the
	// newest snapshots (plus per-version anchors). This is what clears the
	// unbounded accumulation that existed before v9.2.2 wired retention; the
	// scheduler keeps it bounded thereafter by pruning after each Take.
	go func() {
		if swept, sErr := snapshot.SweepStaging(cfg.DataDir); sErr != nil {
			logger.Warn().Err(sErr).Msg("snapshot staging sweep hit an error")
		} else if swept > 0 {
			logger.Info().Int("removed", swept).Msg("reaped crashed snapshot staging dirs")
		}
		if pruned, pErr := snapshot.KeepLast(cfg.DataDir, snapKeep); pErr != nil {
			logger.Warn().Err(pErr).Int("keep_last", snapKeep).Msg("snapshot retention (KeepLast) hit an error at boot")
		} else if pruned > 0 {
			logger.Info().Int("removed", pruned).Int("keep_last", snapKeep).Msg("snapshot retention pruned old snapshots at boot")
		}
	}()

	// Backfill the FTS5 text-search index for any pre-existing memories that
	// predate incremental indexing. Runs ASYNC: on a large chain the initial build
	// is a CPU-bound SQLite sort, and running it synchronously here wedged startup
	// before the first block was ever produced (health dead, no consensus).
	// BackfillFTS self-gates with a cheap count check, so this is a fast no-op once
	// the index is current; a genuinely-needed build proceeds in the background
	// (off the consensus path — it only touches the off-chain SQLite mirror) while
	// the node boots and produces blocks. No-op when the vault is active.
	go func() {
		start := time.Now()
		if ftsErr := sqliteStore.BackfillFTS(ctx); ftsErr != nil {
			logger.Warn().Err(ftsErr).Msg("FTS5 backfill failed — text search may be incomplete")
			return
		}
		logger.Info().Dur("elapsed", time.Since(start)).Msg("FTS5 backfill complete (or already current)")
	}()

	// Create embedding provider
	embedProvider := createEmbeddingProvider(cfg, logger)

	// Health checker
	health := metrics.NewHealthChecker()
	health.Version = version
	health.SetPostgresHealth(true) // SQLite is always "healthy"

	// Embedder health watchdog: keeps /ready's semantic-recall signal current so a
	// down embedding provider surfaces as "degraded" instead of a silently
	// keyword-only recall. Non-blocking (probes in its own goroutine).
	startEmbedderWatchdog(ctx, embedProvider, health, logger)

	// Start CometBFT in-process
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(cometHome)
	cometCfg.Consensus.CreateEmptyBlocks = false
	cometCfg.Consensus.CreateEmptyBlocksInterval = 0
	cometCfg.RPC.ListenAddress = cmtRPCAddr()
	cometCfg.Instrumentation.Prometheus = false

	if cfg.Quorum.Enabled {
		// Quorum mode: enable P2P for multi-validator consensus
		cometCfg.Consensus.TimeoutCommit = 3 * time.Second
		p2pAddr := cfg.Quorum.P2PAddr
		if p2pAddr == "" {
			p2pAddr = cmtP2PAddr("tcp://0.0.0.0:26656")
		}
		cometCfg.P2P.ListenAddress = p2pAddr
		cometCfg.P2P.PersistentPeers = joinPeers(cfg.Quorum.Peers)
		cometCfg.P2P.AddrBookStrict = false  // Allow LAN addresses
		cometCfg.P2P.AllowDuplicateIP = true // Multiple nodes on same network
		cometCfg.P2P.PexReactor = false      // Use persistent peers only
		logger.Info().
			Str("p2p_addr", p2pAddr).
			Int("peers", len(cfg.Quorum.Peers)).
			Msg("quorum mode enabled — multi-validator consensus")
	} else {
		// Personal mode: single validator, fast blocks, no P2P
		cometCfg.Consensus.TimeoutCommit = 1 * time.Second
		cometCfg.P2P.ListenAddress = cmtP2PAddr("tcp://127.0.0.1:26656")
	}

	pv := privval.LoadFilePV(
		cometCfg.PrivValidatorKeyFile(),
		cometCfg.PrivValidatorStateFile(),
	)

	// Detect and fix height regression: if the validator signing state is ahead
	// of the block store (e.g. after chain reset, upgrade, or crash), CometBFT
	// refuses to sign at the lower height.  Auto-reset to prevent a stuck chain.
	pvStatePath := cometCfg.PrivValidatorStateFile()
	blockStoreDBPath := filepath.Join(cometCfg.RootDir, "data", "blockstore.db")
	if _, statErr := os.Stat(blockStoreDBPath); os.IsNotExist(statErr) {
		// Block store is gone (upgrade or reset) — clean up stale state that
		// would otherwise hang or confuse CometBFT on fresh start.
		if signedHeight := pv.LastSignState.Height; signedHeight > 0 {
			logger.Warn().
				Int64("signed_height", signedHeight).
				Msg("validator signed ahead of missing block store — resetting signing state to prevent height regression")
			resetState := []byte(`{"height":"0","round":0,"step":0}`)
			if wErr := os.WriteFile(pvStatePath, resetState, 0600); wErr != nil {
				return fmt.Errorf("reset validator state: %w", wErr)
			}
			pv = privval.LoadFilePV(cometCfg.PrivValidatorKeyFile(), pvStatePath)
		}
		// Remove stale consensus WAL — replaying old WAL against empty databases
		// causes CometBFT to hang during startup (60s timeout then failure).
		csWalDir := filepath.Join(cometCfg.RootDir, "data", "cs.wal")
		if _, walErr := os.Stat(csWalDir); walErr == nil {
			logger.Warn().Msg("removing stale consensus WAL (block store missing)")
			_ = os.RemoveAll(csWalDir)
		}
	}

	nodeKey, err := p2p.LoadNodeKey(cometCfg.NodeKeyFile())
	if err != nil {
		return fmt.Errorf("load node key: %w", err)
	}

	cmtLogger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	cmtLogger = cmtlog.NewFilter(cmtLogger, cmtlog.AllowError()) // Quiet CometBFT logs

	// Runs-or-exits guarantee (voter.required / SAGE_VOTER_REQUIRED): a node that
	// cannot vote must exit BEFORE CometBFT or the HTTP server comes up, not warn
	// and serve voter-less. voter.Run itself refuses an invalid key and returns
	// immediately from its goroutine, so the key has to be validated HERE — after
	// this gate a nil key only warns (legacy behavior). required+disabled is
	// already rejected at LoadConfig, so no voter-enabled check is needed.
	if cfg.Voter.Required {
		if loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger) == nil {
			return fmt.Errorf("voter.required=true but the consensus key (%s) is missing or invalid — memory auto-voter cannot start; fix the key or unset voter.required", cometCfg.PrivValidatorKeyFile())
		}
	}

	// Create the node controller — manages CometBFT lifecycle for redeployment
	nodeCtrl := NewSageNodeController(cometCfg, app, pv, nodeKey, cmtLogger, logger, cfg.DataDir)

	if err := nodeCtrl.StartChain(); err != nil {
		return fmt.Errorf("start CometBFT: %w", err)
	}
	defer func() {
		if stopErr := nodeCtrl.StopChain(); stopErr != nil {
			logger.Error().Err(stopErr).Msg("error stopping CometBFT")
		}
	}()

	health.SetCometBFTHealth(true)
	cometNode := nodeCtrl.GetCometNode()
	logger.Info().
		Str("node_id", string(cometNode.NodeInfo().ID())).
		Msg("CometBFT node started (single-validator personal mode)")

	// Resolve any stale "challenged" memories → deprecated (upgrade from < v4.5.0
	// where challenges stayed in limbo instead of being auto-deprecated).
	if n, resolveErr := sqliteStore.ResolveChallengedMemories(ctx); resolveErr != nil {
		logger.Warn().Err(resolveErr).Msg("failed to resolve challenged memories")
	} else if n > 0 {
		logger.Info().Int("resolved", n).Msg("upgraded challenged memories to deprecated")
	}

	// Auto-seed network_agents from existing chain state (v3 upgrade path)
	seedNetworkAgents(ctx, sqliteStore, cometHome, cometNode, logger)

	// Ensure an operator-specified admin agent is provisioned in SQL on
	// every boot. The on-chain admin role is materialized on first admin
	// op via bootstrapAdminFromSQL (internal/abci/app.go) — this just
	// guarantees the SQL trust row exists so post-reset deployments don't
	// have to re-bootstrap admin manually.
	ensureInitialAdmin(ctx, sqliteStore, logger)

	// CometBFT RPC URL for tx broadcast — derived from the RPC listen address.
	cometRPC := cmtRPCClientURL()

	// Backfill on_chain_height and first_seen for agents already registered on-chain
	// but missing these fields in SQLite (upgrade path from v3.5 → v3.7.6+)
	signingKeyForMigrate := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger)
	sageabci.MigrateAgentsOnChain(ctx, sqliteStore, badgerStore, cometRPC, signingKeyForMigrate, logger)

	// v7.5 upgrade watchdog: auto-propose an UpgradePlan when the
	// running binary's embedded TargetAppVersion exceeds the chain's
	// current app version. No-op when target == current (the steady
	// state for releases that don't change consensus rules).
	startUpgradeWatchdog(ctx, upgradeWatchdogConfig{
		BinaryVersion: version,
		AgentKey:      loadOperatorAgentKey(logger),
		CometRPC:      cometRPC,
		Logger:        logger,
		// v10.5.1 auto-advance: personal nodes walk the fork ladder to the
		// binary ceiling automatically (issue #40 follow-up — updating the
		// binary now brings the chain up to date too). Quorum clusters keep
		// the legacy target-6 watchdog; disable_auto_upgrade opts out.
		PersonalMode: !cfg.Quorum.Enabled,
		AutoAdvance:  !cfg.DisableAutoUpgrade,
		// v10.5.2 (issue #41): in-process pending-plan accessor for the
		// always-on pump and the auto-advance pre-check. GetUpgradePlan's
		// ErrNoUpgradePlan is flattened to nil by readPendingPlan.
		PendingPlan: badgerStore.GetUpgradePlan,
	})

	// Create REST server
	restServer := rest.NewServer(cometRPC, sqliteStore, sqliteStore, badgerStore, health, logger, embedProvider)
	restServer.SetSuppCache(app.SuppCache)
	// Backpressure signals: hand the REST layer the REAL runtime mempool cap.
	// cometCfg comes from config.DefaultConfig() above with Mempool.Size never
	// overridden (CometBFT default 5000) — plumbing it instead of hardcoding
	// means any future code-side override propagates automatically. The
	// on-disk config.toml [mempool] size is reference-only and never read.
	restServer.SetMempoolCap(cometCfg.Mempool.Size)
	// v8.0: wire the off-consensus fork-gate accessor so REST handlers
	// flip to ancestor-walk access checks once the chain reports a post-fork
	// height. Advisory only — the consensus path uses app.postV8Fork(height).
	restServer.SetPostV8ForkAccessor(app.IsPostV8Fork)

	// v7.1: tell the REST layer which ed25519 public key identifies the local
	// node operator. Requests signed with this key bypass the cross-agent
	// visibility filter so the v7.0 SessionStart-hook prefetch returns
	// useful context on nodes where the LLM agent is registered separately.
	// Skip if agent.key is unreadable; the bypass simply stays off.
	if opKey, opErr := readNodeOperatorKey(); opErr == nil && opKey != "" {
		restServer.SetNodeOperatorID(opKey)
		logger.Info().Str("operator_id", opKey[:16]+"...").Msg("node operator key registered for hook read-scope bypass")
	}

	// Create dashboard handler
	dashboard := web.NewDashboardHandler(sqliteStore, version)
	dashboard.Rerankd = rerankdMgr            // managed reranker sidecar (guided setup)
	dashboard.Ollamad = ollamaMgr             // managed Ollama runtime for smart memory setup
	dashboard.BadgerStore = badgerStore       // Wire on-chain RBAC for agent isolation
	dashboard.PostV8ForkFn = app.IsPostV8Fork // v8.0: ancestor-walk grants on post-fork dashboards

	// Bridge REST API events to dashboard SSE for the chain activity log
	restServer.OnEvent = func(eventType, memoryID, domain, content string, data any) {
		dashboard.SSE.Broadcast(web.SSEEvent{
			Type:     web.EventType(eventType),
			MemoryID: memoryID,
			Domain:   domain,
			Content:  content,
			Data:     data,
		})
	}
	dashboard.SetEmbedder(embedProvider)
	if ep, epErr := os.Executable(); epErr == nil {
		dashboard.ExecPath = ep
	}
	dashboard.Encrypted.Store(cfg.Encryption.Enabled)
	dashboard.VaultLocked.Store(cfg.Encryption.Enabled && !vaultUnlocked)
	dashboard.VaultKeyPath = filepath.Join(SageHome(), "vault.key")
	dashboard.SaveEncryptionConfig = func(enabled bool) error {
		cfg.Encryption.Enabled = enabled
		return SaveConfig(cfg)
	}

	// Wire CometBFT consensus for dashboard agent operations (Step 7).
	// Agent create/update will be broadcast on-chain in addition to direct SQLite writes.
	dashboard.CometBFTRPC = cometRPC
	dashboard.RESTAddr = cfg.RESTAddr // surfaced read-only in Settings > Connection
	// Same-machine one-click connect: the dashboard endpoint delegates provider
	// config writing to the package-main writers via this dispatcher.
	dashboard.ConnectFunc = connectProvider
	// LAN node-join ceremony (Flow 3): the dashboard drives it but the temp LAN
	// listener + the (secret-free) bundle assembly live here in package main.
	dashboard.PairingListenerFn = startPairingListener
	dashboard.BuildJoinBundleFn = func(lanIP string) ([]byte, error) {
		bundle, err := buildNodeJoinBundle(lanIP)
		if err != nil {
			return nil, err
		}
		return json.Marshal(bundle)
	}
	dashboard.QuorumEnabled = cfg.Quorum.Enabled
	dashboard.ValidatorCountFn = app.ValidatorCount // authoritative single-validator check for agent ops
	// Embeddings setup: flip the config to the bundled Ollama + nomic-embed-text
	// provider (the node re-reads it on restart). The embedder is locked to this.
	dashboard.SetEmbeddingOllama = func() error {
		cfg.Embedding.Provider = "ollama"
		cfg.Embedding.BaseURL = "http://localhost:11434"
		cfg.Embedding.Model = "nomic-embed-text"
		cfg.Embedding.Dimension = 768
		return SaveConfig(cfg)
	}
	dashboard.SetNetworkMode = func(enabled bool) error {
		cfg.Quorum.Enabled = enabled
		return SaveConfig(cfg)
	}
	// Federation on/off, surfaced in Settings (the toggle re-execs to start/stop
	// the inbound mTLS listener). Outbound recall/receipt delivery is unaffected.
	dashboard.FederationEnabled = cfg.Federation.Enabled
	dashboard.SetFederationEnabledFn = func(enabled bool) error {
		cfg.Federation.Enabled = enabled
		return SaveConfig(cfg)
	}
	// Guest side of Flow 3: the joining node's dashboard drives the ceremony and
	// stages the bundle; it's applied at the next startup (before stores open).
	dashboard.GuestNodeIDFn = func() (string, error) {
		nk, nkErr := p2p.LoadNodeKey(filepath.Join(cometHome, "config", "node_key.json"))
		if nkErr != nil {
			return "", nkErr
		}
		return string(nk.ID()), nil
	}
	dashboard.WritePendingJoinFn = WritePendingJoin
	dashboard.RemovePendingJoinFn = RemovePendingJoin
	if sk := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger); sk != nil {
		dashboard.SigningKey = sk
	}
	// v11.3 RBAC reassign + access-control: the validator key above is not a
	// registered admin, so admin-gated txs (GovPropose, DomainReassign) are
	// signed with the operator/admin key (~/.sage/agent.key), and owner-scoped
	// AccessGrant/AccessRevoke are signed as the resolved domain owner. Neither
	// touches consensus; memory submits still sign with the validator key so
	// authorship (submitting_agent) stays immutable.
	if adminKey := adminSigningKey(); adminKey != nil {
		dashboard.AdminSigningKey = adminKey
	}
	dashboard.ResolveAgentKeyFn = localAgentKeyResolver()

	// Create redeployment orchestrator and wire it to the dashboard
	redeployer := orchestrator.NewRedeployer(sqliteStore, nodeCtrl, logger)
	dashboard.Redeployer = redeployer

	// Bridge redeployer status updates to SSE so the dashboard gets live updates
	go func() {
		for status := range redeployer.StatusChan() {
			dashboard.SSE.Broadcast(web.SSEEvent{
				Type: "redeploy",
				Data: map[string]any{
					"active":    status.Active,
					"operation": status.Operation,
					"agent_id":  status.AgentID,
					"phases":    status.Phases,
				},
			})
		}
	}()

	// Wire pre-validate function into both dashboard and REST API. This is the
	// advisory pre-vote display; it delegates to voter.DecideVerbose so it shows the
	// EXACT named checks (dedup/quality/consistency) the node's real vote applies —
	// one rule set, no second drifting copy.
	preValidate := func(content, contentHash, domain, memType string, confidence float64) []web.PreValidateVote {
		_, checks := voter.DecideVerbose(ctx, sqliteStore, voter.MemoryInput{
			Content: content, ContentHash: contentHash, Domain: domain, MemType: memType, Confidence: confidence,
		})
		votes := make([]web.PreValidateVote, len(checks))
		for i, c := range checks {
			decision := "accept"
			if !c.Pass {
				decision = "reject"
			}
			votes[i] = web.PreValidateVote{Validator: c.Name, Decision: decision, Reason: c.Reason}
		}
		return votes
	}
	dashboard.PreValidateFunc = preValidate
	restServer.PreValidateFunc = func(content, contentHash, domain, memType string, confidence float64) []rest.PreValidateResult {
		webVotes := preValidate(content, contentHash, domain, memType, confidence)
		results := make([]rest.PreValidateResult, len(webVotes))
		for i, v := range webVotes {
			results[i] = rest.PreValidateResult{Validator: v.Validator, Decision: v.Decision, Reason: v.Reason}
		}
		return results
	}

	// Build combined router
	r := chi.NewRouter()
	r.Use(cors.Handler(cors.Options{
		// Dashboard router only serves localhost-bound clients (the SPA,
		// CLIs, and locally-running MCP agents). The HTTP MCP transport at
		// /v1/mcp/* runs its own CORS layer in internal/mcp/http_transport.go
		// for actual remote MCP clients — so the OAuth flow and MCP traffic
		// from ChatGPT / Cursor / Cline stay reachable without giving those
		// origins a path to the rest of the dashboard surface.
		AllowedOrigins:   []string{"http://localhost:*", "http://127.0.0.1:*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type", "X-Agent-ID", "X-Signature", "X-Timestamp", "X-Nonce", "Mcp-Session-Id"},
		AllowCredentials: false,
	}))

	// Mount REST API routes
	r.Mount("/", restServer.Router())
	// Mount dashboard routes (these don't collide — dashboard uses /v1/dashboard/ prefix)
	dashboard.RegisterRoutes(r)

	// HTTP MCP transport — enables non-Claude-Code agents (ChatGPT, Cursor,
	// Cline, custom HTTP MCP clients) to talk to SAGE without spawning a
	// stdio subprocess. Two transports under /v1/mcp:
	//   /v1/mcp/sse        — SSE (older spec, ChatGPT-compatible today)
	//   /v1/mcp/messages   — paired POST endpoint for SSE clients
	//   /v1/mcp/streamable — newer Streamable-HTTP spec
	//
	// Auth: bearer token in Authorization header, validated against the
	// mcp_tokens table. Tokens are SHA-256-hashed before storage.
	mountMCPHTTPTransport(r, sqliteStore, cfg, logger)

	// OAuth 2.0 + PKCE wrapper around bearer auth (v6.7.2). ChatGPT's MCP
	// connector requires Auth URL + Token URL form fields; static-bearer
	// auth doesn't fit that flow. We expose:
	//   GET  /.well-known/oauth-authorization-server  RFC 8414 metadata
	//   GET  /oauth/authorize  consent screen (gated by dashboard session)
	//   POST /oauth/authorize  consent submission → mints bearer + auth code
	//   POST /oauth/token      auth code → bearer (PKCE-verified)
	// The bearer the OAuth flow yields is a normal mcp_tokens row — same
	// /v1/mcp/sse endpoint, same revocation surface. No changes to the
	// bearer-auth middleware itself.
	oauthHandler := rest.NewOAuthHandler(sqliteStore, dashboard.IsRequestAuthenticated, nil)
	// HasDashboardCookie gates the consent-screen identity panel — without a
	// real session cookie we render an 8-char prefix instead of the full hex
	// pubkey, so an unauthenticated tunnel visitor never sees the operator's
	// full identity.
	oauthHandler.HasDashboardCookie = dashboard.HasValidSessionCookie
	// NodeOperatorAgentID is the identity that OAuth-issued bearers will run
	// as. The HTTP MCP transport signs every outgoing REST call with the
	// node's signing key, so this label always matches reality.
	if keyData, kerr := os.ReadFile(cfg.AgentKey); kerr == nil {
		var k ed25519.PrivateKey
		switch len(keyData) {
		case ed25519.SeedSize:
			k = ed25519.NewKeyFromSeed(keyData)
		case ed25519.PrivateKeySize:
			k = ed25519.PrivateKey(keyData)
		}
		if k != nil {
			if pub, ok := k.Public().(ed25519.PublicKey); ok {
				oauthHandler.NodeOperatorAgentID = hex.EncodeToString(pub)
			}
		}
	}
	rest.MountOAuthRoutes(r, oauthHandler)
	logger.Info().Msg("OAuth 2.0 + PKCE wrapper enabled (/.well-known/oauth-authorization-server, /oauth/authorize, /oauth/token)")

	// Start background memory cleanup loop
	memory.StartCleanupLoop(ctx, sqliteStore)

	// Embedding endpoint override — use configured provider, not just Ollama
	r.Post("/v1/embed/personal", handleEmbedPersonal(embedProvider))

	// Prometheus scrape endpoint. amid serves this via internal/metrics's
	// dedicated metrics server; sage-gui has no such listener, so the default
	// registry (sage_voter_running, sage_proposed_oldest_age_seconds, …) is
	// exposed on the same loopback-bound dashboard mux as everything else here.
	r.Method(http.MethodGet, "/metrics", promhttp.Handler())

	httpServer := &http.Server{
		Addr:         cfg.RESTAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// Build display URL: extract just the port for localhost display
	displayAddr := cfg.RESTAddr
	if host, port, err := net.SplitHostPort(cfg.RESTAddr); err == nil {
		if host == "" || host == "0.0.0.0" || host == "127.0.0.1" {
			displayAddr = "localhost:" + port
		} else {
			displayAddr = host + ":" + port
		}
	}

	certsDir := filepath.Join(SageHome(), "certs")

	// v11 federation transport: wire the manager BEFORE any listener serves
	// requests (the TLS REST listener below mounts the same router). The
	// OUTBOUND side (federated recall, receipt delivery) needs no inbound
	// listener — only active cross_fed agreements; the dedicated inbound mTLS
	// listener starts after cert auto-generation, and only when
	// federation.enabled is set.
	var fedMgr *federation.Manager
	if fedAgentKey, akErr := loadOrGenerateKey(cfg.AgentKey); akErr != nil {
		logger.Warn().Err(akErr).Msg("agent key unavailable — federation transport disabled")
	} else if cfg.ChainID == "" {
		logger.Warn().Msg("no chain_id in genesis/config — federation transport disabled")
	} else {
		fedMgr = federation.NewManager(federation.Config{
			LocalChainID: cfg.ChainID,
			CertsDir:     certsDir,
			CometRPC:     cometRPC,
			AgentKey:     fedAgentKey,
			Badger:       badgerStore,
			MemStore:     sqliteStore,
			Logger:       logger,
		})
		restServer.SetFederation(fedMgr)
		// The dashboard drives the guided JOIN wizards (cookie-authed) by calling
		// the same Manager directly - the browser has a session, not the operator
		// signing key, so it cannot reach the agent-signed REST endpoints.
		dashboard.SetFederation(fedMgr)
	}

	// TLS listener: encrypted REST on a separate port.
	// Auto-generates self-signed certs on first run if none exist.
	// Personal mode: TLS on localhost:8443. Quorum mode: TLS on 0.0.0.0:8443.
	// Plain HTTP stays on localhost for dashboard backward compatibility.
	var tlsServer *http.Server
	// Self-heal a stale TLS CA CommonName before the auto-gen below (see
	// reconcileCACommonName): on a re-minted personal node whose cert rotation didn't
	// fully complete, this removes the mismatched certs so they regenerate with a CN
	// tracking the new chain_id — otherwise federation's requireChainCN silently fails.
	reconcileCACommonName(certsDir, cfg.ChainID, cfg.Quorum.Enabled, logger)
	if !tlsca.CertsExist(certsDir) {
		// Auto-generate self-signed certs for HTTPS. The CA CommonName tracks the
		// unique chain_id (reconciled above); fall back to the legacy label only
		// for a chain whose genesis somehow predates chain_id reconciliation.
		chainID := cfg.ChainID
		if chainID == "" {
			chainID = "sage-personal"
			if cfg.Quorum.Enabled {
				chainID = "sage-quorum"
			}
		}
		caCert, caKey, caErr := tlsca.LoadOrGenerateCA(certsDir, chainID)
		if caErr != nil {
			logger.Warn().Err(caErr).Msg("failed to generate TLS CA — running without TLS")
		} else {
			nodeCert, nodeKey2, certErr := tlsca.GenerateNodeCert(caCert, caKey, "personal", []string{"127.0.0.1", "localhost"})
			if certErr != nil {
				logger.Warn().Err(certErr).Msg("failed to generate TLS node cert")
			} else {
				_ = tlsca.WriteCert(filepath.Join(certsDir, tlsca.NodeCertFile), nodeCert)
				_ = tlsca.WriteKey(filepath.Join(certsDir, tlsca.NodeKeyFile), nodeKey2)
				logger.Info().Str("certs_dir", certsDir).Msg("auto-generated TLS certificates")
			}
		}
	}
	if tlsca.CertsExist(certsDir) {
		tlsCfg, tlsErr := tlsca.ServerTLSConfig(certsDir)
		if tlsErr != nil {
			logger.Warn().Err(tlsErr).Msg("TLS certs found but failed to load — running without TLS")
		} else {
			tlsAddr := cfg.Quorum.TLSAddr
			if tlsAddr == "" {
				if cfg.Quorum.Enabled {
					tlsAddr = "0.0.0.0:8443" // Quorum: listen on all interfaces.
				} else {
					tlsAddr = "127.0.0.1:8443" // Personal: localhost only.
				}
			}
			// Surface the effective MCP TLS bind so the dashboard's remote-connect
			// (Flow 2) discovery can tell whether a tool on another computer can
			// reach this node directly over the LAN (non-loopback bind) or only via
			// a tunnel (loopback bind).
			dashboard.MCPTLSAddr = tlsAddr
			tlsServer = &http.Server{
				Addr:         tlsAddr,
				Handler:      r,
				TLSConfig:    tlsCfg,
				ReadTimeout:  15 * time.Second,
				WriteTimeout: 15 * time.Second,
				IdleTimeout:  60 * time.Second,
			}
			go func() {
				logger.Info().Str("addr", tlsAddr).Msg("TLS REST API server starting")
				if tlsServeErr := tlsServer.ListenAndServeTLS("", ""); tlsServeErr != nil && tlsServeErr != http.ErrServerClosed {
					logger.Error().Err(tlsServeErr).Msg("TLS server error")
				}
			}()
		}
	}

	// Dedicated v11 federation mTLS listener — a SEPARATE port and router from
	// the local API: RequireAnyClientCert + per-agreement pinned-CA
	// verification at handshake, chain-qualified signed requests per call. The
	// local REST/TLS listeners above keep NoClientCert semantics untouched.
	var fedServer *http.Server
	if cfg.Federation.Enabled && fedMgr != nil {
		if !tlsca.CertsExist(certsDir) {
			logger.Warn().Msg("federation listener disabled: no TLS certificates")
		} else if fedTLS, fedTLSErr := fedMgr.ServerTLSConfig(); fedTLSErr != nil {
			logger.Warn().Err(fedTLSErr).Msg("federation listener disabled: TLS config failed")
		} else {
			fedAddr := cfg.Federation.ListenAddr
			if fedAddr == "" {
				fedAddr = "0.0.0.0:8444"
			}
			fedServer = &http.Server{
				Addr:         fedAddr,
				Handler:      fedMgr.Router(),
				TLSConfig:    fedTLS,
				ReadTimeout:  15 * time.Second,
				WriteTimeout: 15 * time.Second,
				IdleTimeout:  60 * time.Second,
			}
			// Periodic reaper for expired join sessions / guest drafts / rate-limit
			// map (seed hygiene + staged-CA rollback + unbounded-growth guard).
			fedMgr.StartMaintenance(ctx)
			// v11.5 domain-sync outbox drainer (scan + deliver). Internally a
			// no-op with a log line on non-SQLite backends.
			fedMgr.StartSyncDrainer(ctx)
			// Commit-tail watcher: low-latency enqueue of just-committed
			// memories (the drainer's poll scan stays the correctness
			// backstop). MUST come after StartSyncDrainer — the watcher
			// nudges the channel the drainer creates.
			app.SetSyncNotifier(fedMgr.SyncWatcher())
			go func() {
				logger.Info().Str("addr", fedAddr).Str("chain_id", cfg.ChainID).Msg("federation mTLS listener starting")
				if fedServeErr := fedServer.ListenAndServeTLS("", ""); fedServeErr != nil && fedServeErr != http.ErrServerClosed {
					logger.Error().Err(fedServeErr).Msg("federation listener error")
				}
			}()
		}
	} else if cfg.Federation.Enabled {
		logger.Warn().Msg("federation.enabled set but transport unavailable (missing agent key or chain_id)")
	}

	go func() {
		logger.Info().
			Str("addr", cfg.RESTAddr).
			Str("dashboard", fmt.Sprintf("http://%s/ui/", displayAddr)).
			Msg("SAGE Personal ready")

		fmt.Fprintf(os.Stderr, "\n  SAGE Personal is running!\n")
		fmt.Fprintf(os.Stderr, "  CEREBRUM:  http://%s/ui/\n", displayAddr)
		fmt.Fprintf(os.Stderr, "  REST API:  http://%s/v1/\n", displayAddr)
		if tlsServer != nil {
			fmt.Fprintf(os.Stderr, "  TLS API:   https://%s/v1/\n", tlsServer.Addr)
		}
		fmt.Fprintf(os.Stderr, "\n")

		// Auto-open dashboard in browser (unless suppressed by tray app)
		if os.Getenv("SAGE_NO_BROWSER") == "" {
			go openBrowser(fmt.Sprintf("http://%s/ui/", displayAddr))
		}

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server error")
		}
	}()

	// Start the per-node memory auto-voter: one node, one vote, signed with the
	// node's OWN consensus validator key (priv_validator_key.json). No validator-set
	// replacement — the genesis/quorum validator set already keys each node by this
	// identity, so the node's votes count toward the same 2/3 quorum the chain tallies.
	selfKey := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger)

	// Legacy single-node chain repairs run whenever the consensus key is available —
	// INDEPENDENT of voter.enabled. They fix validator-set / dedup damage from retired
	// code paths, not voting cadence, so disabling the voter must not disable them.
	if selfKey != nil {
		selfID := hex.EncodeToString(selfKey.Public().(ed25519.PublicKey))
		// Repair a legacy single-node chain that previously ran the retired
		// 4-archetype RegisterAppValidators path — whether the persisted set lacks this
		// node's consensus key (votes rejected) or carries it alongside the 4 phantom
		// archetypes (governance quorum unreachable, issue #37). Guarded off on quorum.
		if changed, rErr := app.ReconcileSelfValidator(selfID, deriveArchetypeIDs(selfKey), !cfg.Quorum.Enabled); rErr != nil {
			logger.Warn().Err(rErr).Msg("legacy validator reconcile skipped")
		} else if changed {
			logger.Warn().Str("self", selfID[:16]).Msg("legacy app-validators replaced by node consensus key (single-node repair)")
		}
		// Resurrect memories the pre-v10.4.2 voter wrongly deprecated as "duplicates"
		// of their own proposed row (dedup self-match). Runs AFTER ReconcileSelfValidator
		// so a just-collapsed legacy set passes the repair's set-is-exactly-{selfID}
		// guard; the voter (if enabled) re-votes the resurrected memories into committed.
		if repaired, rErr := app.RepairSelfDupRejectedMemories(ctx, selfID, !cfg.Quorum.Enabled); rErr != nil {
			logger.Warn().Err(rErr).Msg("self-dup-reject memory repair incomplete — will retry next startup")
		} else if repaired > 0 {
			logger.Warn().Int("memories", repaired).Msg("memories wrongly deprecated by the dedup self-match bug restored to proposed (single-node repair)")
		}
	}

	switch {
	case !cfg.Voter.Enabled:
		// Explicit operator choice (voter.enabled=false / SAGE_VOTER_ENABLED=false),
		// not a degraded state — so Info, not Warn. Submitted memories stay proposed
		// until some other validator votes them through.
		logger.Info().Msg("memory auto-voter disabled by config (voter.enabled=false)")
	case selfKey != nil:
		// Poll interval from config (voter.poll_interval / SAGE_VOTER_POLL_INTERVAL);
		// unset/unparsable falls back to the historical 2s. Liveness-only knob, so
		// a bad value warns rather than refusing to boot.
		pollInterval := 2 * time.Second
		if raw := cfg.Voter.PollInterval; raw != "" {
			if d, perr := time.ParseDuration(raw); perr == nil && d > 0 {
				pollInterval = d
			} else {
				logger.Warn().Str("poll_interval", raw).Msg("invalid voter.poll_interval — using default 2s")
			}
		}
		// Health wired in so /ready's "voter" block tracks liveness + the
		// proposed backlog (nil-safe: amid starts the voter without one).
		go voter.Run(ctx, app, sqliteStore, voter.Config{Key: selfKey, CometRPC: cometRPC, PollInterval: pollInterval, Health: health}, logger)
	case cfg.Voter.Required:
		// Normally unreachable — the pre-serve gate before StartChain already refused
		// to boot — but a key that rots between the gate and here must still honor the
		// runs-or-exits contract.
		return fmt.Errorf("voter.required=true but no usable consensus key — memory auto-voter cannot start")
	default:
		logger.Warn().Msg("no consensus key — memory auto-voter disabled")
	}
	if cfg.Quorum.Enabled {
		logger.Info().Msg("quorum mode — P2P consensus active, blocks validated by both nodes")
	}

	// Auto-import pending chat history (from setup wizard)
	go autoImport(cfg, embedProvider, logger)

	// Pipeline TTL cleanup — expire and purge stale pipeline messages every 5 minutes
	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				expired, _ := sqliteStore.ExpirePipelines(ctx)
				purged, _ := sqliteStore.PurgePipelines(ctx, time.Now().Add(-24*time.Hour))
				if expired > 0 || purged > 0 {
					logger.Debug().Int("expired", expired).Int("purged", purged).Msg("pipeline cleanup")
				}
				// Sweep stale OAuth auth-codes (5-min TTL, single-use) — the
				// store retains rows past use for audit visibility, but the
				// bearer plaintext is wiped at redemption time. Anything older
				// than 1h is genuinely abandoned and can drop. Older DCR
				// registrations also age out (90d window) so a forgotten
				// connector setup doesn't accumulate state forever.
				if removed, _ := sqliteStore.PurgeExpiredAuthCodes(ctx); removed > 0 {
					logger.Debug().Int64("removed", removed).Msg("oauth auth-codes purged")
				}
				if removed, _ := sqliteStore.PurgeOldOAuthClients(ctx, 90*24*time.Hour); removed > 0 {
					logger.Debug().Int64("removed", removed).Msg("oauth clients purged")
				}
			}
		}
	}()

	// Wait for shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info().Str("signal", sig.String()).Msg("shutting down")
	cancelRun() // stop the voter + background loops promptly, before draining HTTP

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if tlsServer != nil {
		_ = tlsServer.Shutdown(shutdownCtx)
	}
	if fedServer != nil {
		_ = fedServer.Shutdown(shutdownCtx)
	}
	return httpServer.Shutdown(shutdownCtx)
}

// joinPeers joins a list of peers into a comma-separated string.
func joinPeers(peers []string) string {
	result := ""
	for i, p := range peers {
		if i > 0 {
			result += ","
		}
		result += p
	}
	return result
}

// restBaseURL builds an HTTP base URL from a REST address.
// If addr starts with ":" (just a port), it prepends "localhost".
// Otherwise it uses the address as-is (e.g. "127.0.0.1:18080").
func restBaseURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}
	return "http://" + addr
}

// cmtRPCAddr returns the CometBFT RPC listen address. Overridable via
// SAGE_CMT_RPC_ADDR so two personal nodes can coexist on one host; the default
// preserves the historical tcp://127.0.0.1:26657. This is transport only — the
// RPC port is not consensus state, so changing it is ABCI-determinism-neutral.
func cmtRPCAddr() string {
	if v := os.Getenv("SAGE_CMT_RPC_ADDR"); v != "" {
		return v
	}
	return "tcp://127.0.0.1:26657"
}

// cmtRPCClientURL converts the RPC listen address into a URL the in-process
// tx-broadcast client can dial: tcp:// → http://, and a 0.0.0.0 wildcard listen
// host is dialed as 127.0.0.1 (you connect to loopback, not the wildcard).
func cmtRPCClientURL() string {
	u := strings.Replace(cmtRPCAddr(), "tcp://", "http://", 1)
	return strings.Replace(u, "0.0.0.0", "127.0.0.1", 1)
}

// cmtP2PAddr returns the CometBFT P2P listen address, overridable via
// SAGE_CMT_P2P_ADDR. def is the historical fallback used when the env is unset
// (loopback for personal mode, 0.0.0.0 for quorum) so defaults stay unchanged.
func cmtP2PAddr(def string) string {
	if v := os.Getenv("SAGE_CMT_P2P_ADDR"); v != "" {
		return v
	}
	return def
}

// genesisInitialAdminAppState returns the genesis app_state JSON that seeds the node
// operator's agent key as the chain-admin, or nil if the operator key is unavailable.
func genesisInitialAdminAppState() json.RawMessage {
	admin := ensureOperatorAdminID()
	if admin == "" {
		return nil
	}
	// admin is canonical lowercase 64-hex, safe to embed verbatim.
	return json.RawMessage(`{"sage":{"initial_admin":"` + admin + `"}}`)
}

// ensureOperatorAdminID returns the lowercase-hex ed25519 public key of the node
// operator's ~/.sage/agent.key, GENERATING the key (32-byte seed form, via the
// canonical loadOrGenerateKey) if it does not yet exist — `serve` does not
// otherwise create it. Empty string on any failure.
func ensureOperatorAdminID() string {
	priv, err := loadOrGenerateKey(filepath.Join(SageHome(), "agent.key"))
	if err != nil {
		return ""
	}
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return ""
	}
	return hex.EncodeToString(pub)
}

// ensureGenesisSeed performs the two issue-#52 genesis steps the serve path needs
// after migrateOnUpgrade: initCometBFTConfig (which creates a freshly-seeded genesis
// for a brand-new chain) and healGenesisAdminIfReset (which re-injects the seed if a
// prior admin-less genesis survived a reset — initCometBFTConfig short-circuits on an
// existing genesis, so without the heal an upgraded+reset chain re-deadlocks).
//
// The two steps live in ONE named helper, called from runServe and exercised directly
// by TestIssue52_HealThenInitChain_EndToEnd, so the heal step cannot be silently
// dropped from the serve path without a test going red.
func ensureGenesisSeed(cometHome string, logger zerolog.Logger) error {
	if err := initCometBFTConfig(cometHome); err != nil {
		return fmt.Errorf("init CometBFT: %w", err)
	}
	// Strictly gated on height-0 (block store wiped) so a live chain's genesis hash is
	// never disturbed. Runs AFTER migrateOnUpgrade's reset.
	healGenesisAdminIfReset(cometHome, logger)
	return nil
}

// healGenesisAdminIfReset injects the genesis chain-admin seed into a PRE-EXISTING
// genesis.json that lacks one — but ONLY when the chain is at height 0 (the block
// store has been wiped, e.g. by a fork-transition reset that keeps config/genesis).
// This is what lets an already-deadlocked personal chain recover on its next reset:
// initCometBFTConfig short-circuits when genesis.json exists, so without this an
// upgraded+reset chain re-uses its admin-less genesis and re-deadlocks (issue #52).
//
// Strictly gated: if the block store still exists (a LIVE chain) it does nothing —
// rewriting genesis.json on a live chain would change the genesis hash and break
// CometBFT's handshake. Single-validator genesis only.
func healGenesisAdminIfReset(home string, logger zerolog.Logger) {
	configDir := filepath.Join(home, "config")
	dataDir := filepath.Join(home, "data")
	genesisPath := filepath.Join(configDir, "genesis.json")

	if _, err := os.Stat(genesisPath); err != nil {
		return // no genesis yet — initCometBFTConfig creates it WITH the seed
	}
	// Height-0 gate: rewrite genesis ONLY when BOTH the block store and the state
	// store are absent. blockstore.db absence means no committed blocks; state.db
	// absence matters because CometBFT loads the genesis doc from state.db's cached
	// copy FIRST and only falls back to genesis.json when state.db has none — so if
	// state.db survived a partial reset, a rewritten genesis.json would be silently
	// ignored (the chain would re-read the stale admin-less doc and re-deadlock while
	// we logged success). Either store present => treat as a LIVE chain, never touch
	// its genesis (rewriting it would change the genesis hash and break the handshake).
	if _, err := os.Stat(filepath.Join(dataDir, "blockstore.db")); err == nil {
		return // committed blocks exist: live chain
	}
	if _, err := os.Stat(filepath.Join(dataDir, "state.db")); err == nil {
		return // cached genesis doc would win over a rewrite: leave untouched
	}
	genDoc, err := cmttypes.GenesisDocFromFile(genesisPath)
	if err != nil {
		return
	}
	if len(genDoc.Validators) != 1 {
		return // single-validator (personal) chains only
	}
	if genesisAppStateHasInitialAdmin(genDoc.AppState) {
		return // already seeded
	}
	admin := ensureOperatorAdminID()
	if admin == "" {
		return
	}
	genDoc.AppState = json.RawMessage(`{"sage":{"initial_admin":"` + admin + `"}}`)
	if err := genDoc.ValidateAndComplete(); err != nil {
		logger.Warn().Err(err).Msg("issue#52 heal: genesis re-validate failed — leaving genesis unchanged")
		return
	}
	// Back up the existing genesis, then write atomically (temp file + rename) so a
	// crash mid-rewrite can never leave a half-written genesis.json. Mirrors the
	// quorum-join backup at quorum.go. genesisPath is known to exist (checked above).
	if err := copyFile(genesisPath, genesisPath+".bak"); err != nil {
		logger.Warn().Err(err).Msg("issue#52 heal: could not back up genesis.json — leaving unchanged")
		return
	}
	tmpPath := genesisPath + ".tmp"
	if err := genDoc.SaveAs(tmpPath); err != nil {
		_ = os.Remove(tmpPath)
		logger.Warn().Err(err).Msg("issue#52 heal: could not stage genesis rewrite — leaving unchanged")
		return
	}
	if err := os.Rename(tmpPath, genesisPath); err != nil {
		_ = os.Remove(tmpPath)
		logger.Warn().Err(err).Msg("issue#52 heal: could not commit genesis rewrite — leaving unchanged (backup at .bak)")
		return
	}
	logger.Info().Str("admin_id", admin[:16]).Msg("issue#52: injected genesis chain-admin into reset chain's genesis.json")
}

// genesisAppStateHasInitialAdmin reports whether the genesis app_state already
// carries a non-empty sage.initial_admin (so heal-on-reset is a no-op).
func genesisAppStateHasInitialAdmin(appState json.RawMessage) bool {
	if len(appState) == 0 {
		return false
	}
	var as struct {
		Sage struct {
			InitialAdmin string `json:"initial_admin"`
		} `json:"sage"`
	}
	if err := json.Unmarshal(appState, &as); err != nil {
		return false
	}
	return strings.TrimSpace(as.Sage.InitialAdmin) != ""
}

// initCometBFTConfig generates CometBFT config files for a single-validator node.
func initCometBFTConfig(home string) error {
	configDir := filepath.Join(home, "config")
	dataDir := filepath.Join(home, "data")

	// Check if already initialized
	if _, err := os.Stat(filepath.Join(configDir, "genesis.json")); err == nil {
		return nil // Already initialized
	}

	if err := os.MkdirAll(configDir, 0700); err != nil {
		return err
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return err
	}

	// Generate validator key
	pv := privval.GenFilePV(
		filepath.Join(configDir, "priv_validator_key.json"),
		filepath.Join(dataDir, "priv_validator_state.json"),
	)
	pv.Save()

	// Generate node key
	if _, err := p2p.LoadOrGenNodeKey(filepath.Join(configDir, "node_key.json")); err != nil {
		return fmt.Errorf("generate node key: %w", err)
	}

	// Create genesis with single validator. Mint a globally-unique chain_id so no
	// two independently-created SAGE networks collide — the federation identity,
	// co-commit cross-anchors, and cross-chain replay defence all assume chain_id
	// is unique (every personal node was previously born as the identical
	// "sage-personal"). Bound to this node's validator key + genesis time + entropy.
	genesisTime := cmttime.Now()
	chainID, mintErr := mintChainID("sage-personal", [][]byte{pv.Key.PubKey.Bytes()}, genesisTime)
	if mintErr != nil {
		return fmt.Errorf("mint chain_id: %w", mintErr)
	}
	genDoc := cmttypes.GenesisDoc{
		ChainID:         chainID,
		GenesisTime:     genesisTime,
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators: []cmttypes.GenesisValidator{
			{
				Address: pv.Key.PubKey.Address(),
				PubKey:  pv.Key.PubKey,
				Power:   10,
				Name:    "personal",
			},
		},
	}
	// issue #52: seed the operator's agent key as the genesis chain-admin so the
	// personal fork-ladder climb to app-v13 never strands at the propose admin-gate.
	// This is a single-validator genesis (quorum join overwrites it with a shared
	// genesis that carries no app_state, so quorum is unaffected). InitChain only
	// honours the seed for single-validator chains.
	if admin := genesisInitialAdminAppState(); admin != nil {
		genDoc.AppState = admin
	}
	if err := genDoc.ValidateAndComplete(); err != nil {
		return fmt.Errorf("validate genesis: %w", err)
	}
	if err := genDoc.SaveAs(filepath.Join(configDir, "genesis.json")); err != nil {
		return fmt.Errorf("save genesis: %w", err)
	}

	// Write minimal config.toml.
	//
	// NOTE: SAGE builds the RUNNING CometBFT configuration in code at startup (see
	// runServe → config.DefaultConfig() + explicit overrides), NOT from this file. The
	// [consensus] and [mempool] values below are written for reference/tooling only
	// and are NOT read at runtime — editing them has no effect. They are kept here in
	// step with the actual code-set runtime values so a reader isn't misled:
	//   consensus.create_empty_blocks = false  — an IDLE chain mints no blocks; a
	//     mempool tx mints the next block (plus one trailing empty proof block). See
	//     docs/reference/concepts/block-production-and-idle.md.
	//   consensus.timeout_commit = 1s (personal) / 3s (quorum)
	//   mempool.size = 5000 (CometBFT default)
	configToml := fmt.Sprintf(`# SAGE Personal — CometBFT config
#
# The [consensus]/[mempool] values below are NOT read at runtime — SAGE sets the
# running config in code (see node.go); editing them has no effect. They reflect
# personal-mode runtime for reference (quorum mode uses timeout_commit = 3s).
proxy_app = "kvstore"
moniker = "sage-personal"

[rpc]
laddr = %q

[p2p]
laddr = ""

[consensus]
timeout_commit = "1s"
create_empty_blocks = false

[mempool]
size = 5000
`, cmtRPCAddr())
	return os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(configToml), 0600)
}

// startEmbedderWatchdog probes the embedding provider every 30s so /ready reflects
// real semantic-recall availability (a down provider degrades hybrid recall to
// keyword-only). It prefers the optional Pinger for a live check — an Ollama Ping hits
// /api/tags, an OpenAI-compatible Ping is a real embed request, so the 30s cadence
// keeps it cheap — and falls back to the sticky Ready() flag otherwise. Non-blocking:
// the initial probe runs inside the goroutine so boot never waits on the embedder.
func startEmbedderWatchdog(ctx context.Context, p embedding.Provider, health *metrics.HealthChecker, logger zerolog.Logger) {
	if p == nil || health == nil {
		return
	}
	probe := func() metrics.EmbedderStatus {
		s := metrics.EmbedderStatus{Semantic: p.Semantic()}
		if n, ok := p.(embedding.Named); ok {
			s.Provider = n.Name()
		}
		if m, ok := p.(embedding.Modeler); ok {
			s.Model = m.Model()
		}
		if pinger, ok := p.(embedding.Pinger); ok {
			pctx, cancel := context.WithTimeout(ctx, 3*time.Second)
			err := pinger.Ping(pctx)
			cancel()
			s.OK = err == nil
			if err != nil {
				// Bound the detail: an OpenAI-compatible upstream can embed a large
				// error body, which would otherwise bloat every /ready response.
				s.Detail = truncateString(err.Error(), 200)
			}
		} else {
			s.OK = p.Ready()
		}
		health.SetEmbedderHealth(s)
		return s
	}
	go func() {
		// Log only on transition (and once at startup) so a persistently-down provider
		// doesn't spam the log every 30s.
		var prevOK, probed bool
		record := func(s metrics.EmbedderStatus) {
			if s.Semantic && (!probed || prevOK != s.OK) {
				if s.OK {
					logger.Info().Str("provider", s.Provider).Str("model", s.Model).
						Msg("embedding provider healthy — semantic recall available")
				} else {
					logger.Warn().Str("provider", s.Provider).Str("detail", s.Detail).
						Msg("embedding provider unreachable — recall is keyword-only until it recovers")
				}
			}
			prevOK, probed = s.OK, true
		}
		record(probe()) // seed immediately (in-goroutine, so it doesn't block boot)
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				record(probe())
			}
		}
	}()
}

// truncateString caps s at max runes, appending an ellipsis when it overflows.
func truncateString(s string, max int) string {
	r := []rune(s)
	if len(r) <= max {
		return s
	}
	return string(r[:max]) + "…"
}

// createEmbeddingProvider creates the configured embedding provider.
func createEmbeddingProvider(cfg *Config, logger zerolog.Logger) embedding.Provider {
	switch cfg.Embedding.Provider {
	case "ollama":
		logger.Info().Str("url", cfg.Embedding.BaseURL).Msg("using Ollama embeddings")
		return embedding.NewClient(cfg.Embedding.BaseURL, cfg.Embedding.Model)
	case "openai-compatible":
		dim := cfg.Embedding.Dimension
		if dim <= 0 {
			dim = 1536 // OpenAI text-embedding-3-small default
		}
		logger.Info().
			Str("url", cfg.Embedding.BaseURL).
			Str("model", cfg.Embedding.Model).
			Int("dimension", dim).
			Bool("authenticated", cfg.Embedding.APIKey != "").
			Msg("using OpenAI-compatible embeddings")
		return embedding.NewOpenAICompatibleClient(
			cfg.Embedding.BaseURL,
			cfg.Embedding.Model,
			cfg.Embedding.APIKey,
			dim,
		)
	default:
		dim := cfg.Embedding.Dimension
		if dim <= 0 {
			dim = 768
		}
		logger.Info().Int("dimension", dim).Msg("using hash-based pseudo-embeddings")
		return embedding.NewHashProvider(dim)
	}
}

// handleEmbedPersonal returns an HTTP handler for the personal embedding endpoint.
func handleEmbedPersonal(provider embedding.Provider) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
		var req struct {
			Text string `json:"text"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
			return
		}
		if req.Text == "" {
			http.Error(w, `{"error":"text is required"}`, http.StatusBadRequest)
			return
		}

		emb, err := provider.Embed(r.Context(), req.Text)
		if err != nil {
			http.Error(w, `{"error":"embedding failed"}`, http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"embedding": emb,
			"dimension": provider.Dimension(),
		})
	}
}

// deriveArchetypeIDs reproduces the 4 seed-derived validator IDs that the retired
// startAppValidators path persisted, so ReconcileSelfValidator can fingerprint a
// legacy single-node chain and repair it. The derivation MUST match the old one
// exactly: sha256(node-seed) -> sha256(seed + "sage-validator-"+name) -> ed25519 key.
func deriveArchetypeIDs(selfKey ed25519.PrivateKey) []string {
	var seed [32]byte
	h := sha256.Sum256(selfKey.Seed())
	copy(seed[:], h[:])
	names := []string{"sentinel", "dedup", "quality", "consistency"}
	ids := make([]string, 0, len(names))
	for _, name := range names {
		keySeed := sha256.Sum256(append(seed[:], []byte("sage-validator-"+name)...))
		key := ed25519.NewKeyFromSeed(keySeed[:])
		ids = append(ids, hex.EncodeToString(key.Public().(ed25519.PublicKey)))
	}
	return ids
}

// autoImport checks for pending-import.json from the setup wizard and seeds memories.
func autoImport(cfg *Config, embedProvider embedding.Provider, logger zerolog.Logger) {
	home := SageHome()
	importPath := filepath.Join(home, "pending-import.json")

	data, err := os.ReadFile(importPath)
	if err != nil {
		return // No pending import
	}

	var memories []seedMemory
	if unmarshalErr := json.Unmarshal(data, &memories); unmarshalErr != nil {
		logger.Error().Err(unmarshalErr).Msg("failed to parse pending import")
		return
	}

	if len(memories) == 0 {
		os.Remove(importPath)
		return
	}

	// Wait for the REST API to be ready
	time.Sleep(5 * time.Second)

	logger.Info().Int("count", len(memories)).Msg("auto-importing chat history from setup wizard")

	baseURL := restBaseURL(cfg.RESTAddr)

	// Load agent key for signing
	keyData, err := os.ReadFile(cfg.AgentKey)
	if err != nil {
		logger.Error().Err(err).Msg("failed to read agent key for auto-import")
		return
	}
	priv := ed25519.NewKeyFromSeed(keyData)
	pub, _ := priv.Public().(ed25519.PublicKey) //nolint:errcheck
	agentID := hex.EncodeToString(pub)

	success := 0
	for _, mem := range memories {
		if mem.Domain == "" {
			mem.Domain = "imported"
		}
		if mem.Type == "" {
			mem.Type = "observation"
		}
		if mem.Confidence == 0 {
			mem.Confidence = 0.75
		}

		emb, err := getEmbedding(baseURL, mem.Content, agentID, priv)
		if err != nil {
			logger.Debug().Err(err).Msg("auto-import embed failed")
			continue
		}

		body, _ := json.Marshal(map[string]any{
			"content":          mem.Content,
			"memory_type":      mem.Type,
			"domain_tag":       mem.Domain,
			"confidence_score": mem.Confidence,
			"embedding":        emb,
		})

		if err := submitSigned(baseURL+"/v1/memory/submit", body, agentID, priv); err != nil {
			logger.Debug().Err(err).Msg("auto-import submit failed")
			continue
		}
		success++
	}

	logger.Info().Int("imported", success).Int("total", len(memories)).Msg("auto-import complete")

	// Remove the pending file
	os.Remove(importPath)

	// Write a marker so we don't re-import
	doneMsg := fmt.Sprintf("Imported %d/%d memories on %s", success, len(memories), time.Now().Format(time.RFC3339))
	_ = os.WriteFile(filepath.Join(home, "import-done.txt"), []byte(doneMsg), 0600)
}

// readPassphrase reads a line from stdin. For the DMG launcher (no terminal),
// the passphrase must be set via SAGE_PASSPHRASE env var.
func readPassphrase() (string, error) {
	scanner := bufio.NewScanner(os.Stdin)
	if scanner.Scan() {
		return scanner.Text(), nil
	}
	if err := scanner.Err(); err != nil {
		return "", err
	}
	return "", fmt.Errorf("no input")
}

func runStatus() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}

	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = restBaseURL(cfg.RESTAddr)
	}

	ctx := context.Background()
	healthReq, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/health", nil) //nolint:gosec // baseURL is from config, not user input
	resp, err := http.DefaultClient.Do(healthReq)                                  //nolint:gosec // internal health check
	if err != nil {
		return fmt.Errorf("SAGE is not running: %w", err)
	}
	defer resp.Body.Close()

	var health map[string]any
	json.NewDecoder(resp.Body).Decode(&health) //nolint:errcheck

	fmt.Println("SAGE Personal Status")
	fmt.Println("====================")
	fmt.Printf("  Endpoint: %s\n", baseURL)
	fmt.Printf("  Health:   %s\n", resp.Status)
	fmt.Printf("  Dashboard: %s/ui/\n", baseURL)

	// Try to get stats
	statsReq, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/v1/dashboard/stats", nil) //nolint:gosec // baseURL from config
	statsResp, err := http.DefaultClient.Do(statsReq)                                         //nolint:gosec // internal API call
	if err == nil {
		defer func() { _ = statsResp.Body.Close() }()
		var stats map[string]any
		if json.NewDecoder(statsResp.Body).Decode(&stats) == nil {
			if total, ok := stats["total_memories"]; ok {
				fmt.Printf("  Memories: %.0f\n", total)
			}
		}
	}

	return nil
}

// seedNetworkAgents auto-populates the network_agents table from existing chain
// state on first v3 boot. This ensures existing validators appear in the Network
// page without requiring manual re-registration.
func seedNetworkAgents(ctx context.Context, s *store.SQLiteStore, cometHome string, cometNode *node.Node, logger zerolog.Logger) {
	// Check if agents already exist
	agents, err := s.ListAgents(ctx)
	if err != nil {
		logger.Warn().Err(err).Msg("seed agents: failed to list existing agents")
		return
	}
	if len(agents) > 0 {
		return // Already seeded
	}

	// Read genesis to find validators
	genesisPath := filepath.Join(cometHome, "config", "genesis.json")
	genDoc, err := cmttypes.GenesisDocFromFile(genesisPath)
	if err != nil {
		logger.Warn().Err(err).Msg("seed agents: failed to read genesis")
		return
	}

	// Get local node info
	localNodeID := string(cometNode.NodeInfo().ID())
	localMoniker := "sage-node"
	if dni, ok := cometNode.NodeInfo().(p2p.DefaultNodeInfo); ok {
		localMoniker = dni.Moniker
	}

	// Get the local validator pubkey
	pvKeyPath := filepath.Join(cometHome, "config", "priv_validator_key.json")
	localPV := privval.LoadFilePV(pvKeyPath, filepath.Join(cometHome, "data", "priv_validator_state.json"))
	localValPubkey := hex.EncodeToString(localPV.Key.PubKey.Bytes())

	// Get agent signing key (Ed25519 pubkey from agent.key)
	localAgentID := ""
	agentKeyPath := filepath.Join(SageHome(), "agent.key")
	if seed, readErr := os.ReadFile(agentKeyPath); readErr == nil && len(seed) == ed25519.SeedSize {
		pk, ok := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)
		if ok {
			localAgentID = hex.EncodeToString(pk)
		}
	}

	seeded := 0
	for _, v := range genDoc.Validators {
		valPubkeyHex := hex.EncodeToString(v.PubKey.Bytes())
		isLocal := valPubkeyHex == localValPubkey

		agentID := valPubkeyHex // Default: use validator pubkey as agent_id
		nodeName := v.Name
		nodeID := ""
		p2pAddr := ""
		status := "active"
		role := "member"
		avatar := ""

		if isLocal {
			// Local node — use the actual agent signing key if available
			if localAgentID != "" {
				agentID = localAgentID
			}
			nodeName = localMoniker
			nodeID = localNodeID
			role = "admin"
			avatar = "💻"
		} else {
			avatar = "🖥️"
		}

		agent := &store.AgentEntry{
			AgentID:         agentID,
			Name:            nodeName,
			Role:            role,
			Avatar:          avatar,
			ValidatorPubkey: valPubkeyHex,
			NodeID:          nodeID,
			P2PAddress:      p2pAddr,
			Status:          status,
			Clearance:       2,
		}
		if createErr := s.CreateAgent(ctx, agent); createErr != nil {
			logger.Warn().Err(createErr).Str("name", nodeName).Msg("seed agents: failed to create")
			continue
		}
		seeded++
		logger.Info().Str("name", nodeName).Str("role", role).Bool("local", isLocal).Msg("seeded network agent from genesis")
	}

	if seeded > 0 {
		logger.Info().Int("count", seeded).Msg("auto-seeded network agents from existing chain state")
	}
}

// mountMCPHTTPTransport wires the HTTP/HTTPS MCP transport endpoints onto
// the given chi router. Requires SQLite for the bearer-token store.
//
// The MCP server is created with a per-request agent identity resolved from
// the bearer token — each token belongs to exactly one agent, so we mint a
// fresh ed25519 signing pair for the transport and let the underlying tool
// handlers run as that token's agent. (Long-term, an enhancement would let
// each token also carry its own agent.key — for now they share a transport
// key and the agent_id is propagated via context.)
//
// CORS is liberal — MCP clients are first-class.
func mountMCPHTTPTransport(r chi.Router, sqliteStore *store.SQLiteStore, cfg *Config, logger zerolog.Logger) {
	// Use the node's own agent identity as the underlying signing key for
	// transport-originated REST calls. The actual *acting* agent is supplied
	// by the bearer-token middleware via request context — downstream tool
	// handlers can inspect it for audit / on-chain RBAC.
	keyData, readErr := os.ReadFile(cfg.AgentKey) //nolint:gosec // path from trusted config
	if readErr != nil {
		logger.Warn().Err(readErr).Msg("HTTP MCP: cannot load agent key — transport disabled")
		return
	}
	var transportKey ed25519.PrivateKey
	switch len(keyData) {
	case ed25519.SeedSize:
		transportKey = ed25519.NewKeyFromSeed(keyData)
	case ed25519.PrivateKeySize:
		transportKey = ed25519.PrivateKey(keyData)
	default:
		logger.Warn().Int("len", len(keyData)).Msg("HTTP MCP: invalid agent key size — transport disabled")
		return
	}

	// Build the MCP Server reusing the existing stdio Server logic.
	// The base URL points at the local REST API so tool handlers funnel
	// through the same signed-REST pipeline as stdio.
	baseURL := restBaseURL(cfg.RESTAddr)
	mcpServer := mcp.NewServer(baseURL, transportKey)
	mcpServer.SetVersion(version)

	transport := mcp.NewHTTPTransport(mcpServer)

	// Bearer-auth lookup: take the SHA-256 digest the middleware computed,
	// hand it to SQLite, return the agent_id. Translate the store's
	// ErrTokenRevoked into the middleware-side sentinel so 401 vs 500 are
	// distinguishable.
	bearerLookup := func(ctx context.Context, tokenSHA256 string) (string, error) {
		tok, err := sqliteStore.LookupMCPToken(ctx, tokenSHA256)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", err
			}
			if errors.Is(err, store.ErrTokenRevoked) {
				return "", middleware.ErrMCPTokenRevoked
			}
			return "", err
		}
		if tok == nil {
			return "", sql.ErrNoRows
		}
		return tok.AgentID, nil
	}

	// IMPORTANT: register the transport endpoints as FLAT paths, not via
	// r.Route("/v1/mcp", ...). Using a sub-route here mounts a subrouter
	// that shadows /v1/mcp/tokens (registered on the main api/rest router
	// with ed25519 auth) — chi resolves the mount as a catchall for
	// /v1/mcp/* and the explicit /v1/mcp/tokens registration becomes
	// unreachable, returning 404 to every token-management call. v6.7.0
	// shipped that bug; this is the v6.7.1 fix.
	//
	// /tokens stays admin-managed by the ed25519-auth group on the main
	// router. The bearer middleware applies only to the SSE/streamable
	// transport routes wired below.
	mcpTransportRouter := r.With(transport.CORSMiddleware, middleware.MCPBearerAuthMiddleware(bearerLookup))
	mcpTransportRouter.Get("/v1/mcp/sse", transport.HandleSSE)
	mcpTransportRouter.Post("/v1/mcp/messages", transport.HandleSSEMessages)
	mcpTransportRouter.Post("/v1/mcp/streamable", transport.HandleStreamable)

	logger.Info().Msg("HTTP MCP transport enabled (/v1/mcp/sse, /v1/mcp/streamable)")
}

// readNodeOperatorKey returns the hex-encoded ed25519 public key derived from
// ~/.sage/agent.key, accepting either the 32-byte seed or the 64-byte expanded
// private-key form (matches mountMCPHTTPTransport's existing parse). Empty
// string + nil error means the file isn't present — the caller treats that as
// "no operator key, hook bypass stays off."
func readNodeOperatorKey() (string, error) {
	path := filepath.Join(SageHome(), "agent.key")
	data, err := os.ReadFile(path) //nolint:gosec // path under operator's own home dir
	if err != nil {
		if os.IsNotExist(err) {
			return "", nil
		}
		return "", err
	}
	var pk ed25519.PublicKey
	switch len(data) {
	case ed25519.SeedSize:
		pk, _ = ed25519.NewKeyFromSeed(data).Public().(ed25519.PublicKey)
	case ed25519.PrivateKeySize:
		pk, _ = ed25519.PrivateKey(data).Public().(ed25519.PublicKey)
	default:
		return "", fmt.Errorf("agent.key has unexpected length %d (want 32 or 64)", len(data))
	}
	if pk == nil {
		return "", fmt.Errorf("agent.key did not yield a usable ed25519 public key")
	}
	return hex.EncodeToString(pk), nil
}

// loadNodeSigningKey extracts the Ed25519 private key from CometBFT's
// priv_validator_key.json, delegating to the shared voter.LoadPrivValidatorKey
// parser. Returns nil if the key cannot be loaded (broadcasts/voting are skipped).
func loadNodeSigningKey(keyFilePath string, logger zerolog.Logger) ed25519.PrivateKey {
	key, err := voter.LoadPrivValidatorKey(keyFilePath)
	if err != nil {
		logger.Warn().Err(err).Msg("cannot load validator key — on-chain agent broadcasts / memory voting disabled")
		return nil
	}
	return key
}
