package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmtcryptoed "github.com/cometbft/cometbft/crypto/ed25519"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/libs/protoio"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
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
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/ollamad"
	"github.com/l33tdawg/sage/internal/orchestrator"
	sagep2p "github.com/l33tdawg/sage/internal/p2p"
	"github.com/l33tdawg/sage/internal/rerankd"
	"github.com/l33tdawg/sage/internal/shellcontrol"
	"github.com/l33tdawg/sage/internal/snapshot"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/tunnelclientd"
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

func scopedProjectionReadinessStatus(records int, vaultLocked bool) metrics.ScopedProjectionStatus {
	status := metrics.ScopedProjectionStatus{
		Required: records > 0 || vaultLocked,
		Records:  records,
	}
	if vaultLocked {
		status.Detail = "vault locked; scoped serving projection rebuild deferred"
		return status
	}
	if records == 0 {
		status.OK = true
	}
	return status
}

var errCoordinatedRestart = errors.New("coordinated restart requested")

type preparedRestartRequest struct {
	release      func()
	commit       func()
	abort        func()
	fallbackExec string
}

type snapshotDrainPreparer interface {
	PrepareQuiesce(context.Context) (commit func(), abort func(), err error)
}

type runningBinaryPreserver interface {
	PreserveRunningBinary() (string, error)
}

func prepareAndQueueRestart(ctx context.Context, scheduler snapshotDrainPreparer, requests chan<- preparedRestartRequest, release func()) error {
	if scheduler == nil {
		return fmt.Errorf("snapshot scheduler is unavailable")
	}
	prepareCtx, cancelPrepare := context.WithTimeout(ctx, 15*time.Second)
	commit, abort, prepareErr := scheduler.PrepareQuiesce(prepareCtx)
	cancelPrepare()
	if prepareErr != nil {
		return prepareErr
	}
	fallbackExec := ""
	if preserver, ok := scheduler.(runningBinaryPreserver); ok {
		fallbackExec, prepareErr = preserver.PreserveRunningBinary()
		if prepareErr != nil {
			abort()
			return fmt.Errorf("preserve exact running executable before drain: %w", prepareErr)
		}
	}
	select {
	case requests <- preparedRestartRequest{release: release, commit: commit, abort: abort, fallbackExec: fallbackExec}:
		return nil
	default:
		abort()
		return fmt.Errorf("restart already in progress")
	}
}

func verifyPreparedRestartFallback(preserver runningBinaryPreserver, request preparedRestartRequest) (string, error) {
	if preserver == nil || request.fallbackExec == "" {
		return "", errors.New("pre-drain executable recovery is unavailable")
	}
	verified, err := preserver.PreserveRunningBinary()
	if err != nil {
		return "", fmt.Errorf("verify exact pre-drain executable: %w", err)
	}
	if verified != request.fallbackExec {
		return "", errors.New("pre-drain executable recovery identity changed")
	}
	return verified, nil
}

func releaseRestartFence(release *func()) {
	if release == nil || *release == nil {
		return
	}
	(*release)()
	*release = nil
}

func releaseQueuedRestartFences(requests <-chan preparedRestartRequest) {
	for {
		select {
		case request := <-requests:
			if request.abort != nil {
				request.abort()
			}
			if request.release != nil {
				request.release()
			}
		default:
			return
		}
	}
}

func adoptQueuedRestartRequests(requests <-chan preparedRestartRequest, adopted *preparedRestartRequest, restarting *bool) {
	for {
		select {
		case request := <-requests:
			if adopted.commit == nil && adopted.abort == nil && adopted.release == nil {
				*adopted = request
			} else {
				if request.abort != nil {
					request.abort()
				}
				if request.release != nil {
					request.release()
				}
			}
			*restarting = true
		default:
			return
		}
	}
}

// Authenticated federation operations may include bounded consensus waits.
// Keep the peer listener above one ordinary 60-second broadcast ceiling. The
// browser-facing final confirmation spans two sequential commits (guest then
// host), so the REST response budget must cover both.
// Peer Write itself is deliberately fail-closed in v11.9 until a scoped
// consensus ingress capability exists.
var (
	federationListenerWriteTimeout = federation.JoinConfirmationPeerTimeout() + 5*time.Second
	restListenerWriteTimeout       = federation.JoinConfirmationOperationTimeout() + 5*time.Second
)

const defaultFederationListenAddr = "0.0.0.0:8444"

func effectiveFederationListenAddr(configured string) string {
	if addr := strings.TrimSpace(configured); addr != "" {
		return addr
	}
	return defaultFederationListenAddr
}

func validatedFederationListenAddr(configured string) (string, error) {
	addr := effectiveFederationListenAddr(configured)
	_, portText, err := net.SplitHostPort(addr)
	if err != nil {
		return "", fmt.Errorf("must be host:port: %w", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil || port < 1 || port > 65535 {
		return "", fmt.Errorf("port must be between 1 and 65535")
	}
	return addr, nil
}

func runServe(startupProof string) (rerr error) {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	// Preserve the original panic and stack while deliberately declining
	// to authorize automatic state/binary rollback (v11.16.1 safety rule).
	defer haltOnPanic(cfg.DataDir)

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Str("service", "sage-gui").Logger()

	logger.Info().
		Str("data_dir", cfg.DataDir).
		Str("rest_addr", cfg.RESTAddr).
		Str("embedding", cfg.Embedding.Provider).
		Msg("starting SAGE Personal node")

	// Resolve paths and prove the vendored origin before starting even the local
	// native-shell endpoint. A failed direct-v23 preflight must be observational:
	// no socket, PID, directory, key, recovery, migration, or config mutation.
	cometHome := filepath.Join(cfg.DataDir, "cometbft")
	badgerPath := filepath.Join(cfg.DataDir, "badger")
	sqlitePath := filepath.Join(cfg.DataDir, "sage.db")
	vendoredChainIDRecovered := false
	if cfg.VendoredAgentBootstrap != nil {
		configuredChainIDWasEmpty := cfg.ChainID == ""
		if preflightErr := preflightVendoredStartup(cometHome, cfg); preflightErr != nil {
			return fmt.Errorf("preflight first-party app-v23 genesis: %w", preflightErr)
		}
		vendoredChainIDRecovered = configuredChainIDWasEmpty && cfg.ChainID != ""
	}

	freshNodeOrigin, originErr := genuinelyFreshNodeOrigin(cfg.DataDir)
	if originErr != nil {
		return fmt.Errorf("classify node origin before startup: %w", originErr)
	}
	if genesisErr := preflightGenesisOrigin(cometHome, freshNodeOrigin); genesisErr != nil {
		return genesisErr
	}
	// Validate persisted lineage before startup creates sockets, directories,
	// keys, repairs activation state, applies a pending network join, or opens
	// any store.
	if _, migrateErr := migrateOnUpgrade(cfg.DataDir); migrateErr != nil {
		return fmt.Errorf("upgrade migration: %w", migrateErr)
	}

	// The native shell is additive: failure to create its per-user control
	// endpoint must not take the daemon, browser CEREBRUM, CLI, or MCP down.
	// The endpoint starts only after main owns the existing instance lock.
	var nativeControl *shellcontrol.Server
	nativeControl, controlErr := shellcontrol.Start(SageHome(), version, restBaseURL(cfg.RESTAddr), startupProof)
	if controlErr != nil {
		logger.Warn().Err(controlErr).Msg("native shell control endpoint unavailable — browser CEREBRUM remains available")
	} else {
		defer func() {
			if rerr != nil {
				_ = nativeControl.SetState(shellcontrol.StateFailed)
			}
			if closeErr := nativeControl.Close(); closeErr != nil {
				// The shell endpoint is additive. In particular, refusing to remove a
				// path another process replaced is a safety success, not permission to
				// turn an otherwise clean daemon shutdown into an update rollback.
				logger.Warn().Err(closeErr).Msg("native shell control endpoint cleanup incomplete")
			}
		}()
		logger.Info().Str("endpoint", nativeControl.Endpoint()).Msg("native shell control endpoint ready for negotiation")
	}

	// Do not create the canonical Badger directory until activation recovery has
	// inspected the crash layout. A missing live directory can be intentional.
	for _, dir := range []string{cfg.DataDir, cometHome} {
		if mkErr := os.MkdirAll(dir, 0700); mkErr != nil {
			return fmt.Errorf("create dir %s: %w", dir, mkErr)
		}
	}
	recoveredReceiverCompleted := false
	recoveryAction, recoveryFound, recoveryErr := recoverPendingStateSyncActivation(
		context.Background(), cfg.DataDir, cometHome, badgerPath,
		func(height uint64, appHash []byte) error {
			wasReceiving := cfg.Quorum.StateSync.Receiving
			projectionNodeKey, validatorPublicKey, identityErr := stateSyncProjectionLocalIdentity(cometHome)
			if identityErr != nil {
				return identityErr
			}
			if completeErr := validateStateSyncProjectionBaselinePendingAndComplete(
				cfg,
				projectionNodeKey,
				validatorPublicKey,
				height,
				appHash,
			); completeErr != nil {
				return completeErr
			}
			recoveredReceiverCompleted = recoveredReceiverCompleted || wasReceiving
			return nil
		},
	)
	if recoveryErr != nil {
		return fmt.Errorf("recover state sync activation before ABCI handshake: %w", recoveryErr)
	} else if recoveryFound {
		logger.Warn().Str("action", stateSyncRecoveryActionName(recoveryAction)).Msg("recovered interrupted state sync activation before opening Badger")
	}
	if recoveredReceiverCompleted {
		logger.Info().Msg("completed recovered one-shot state sync as an ordinary synchronized node")
	}
	stateSyncRecoveryComplete := true
	if mkErr := os.MkdirAll(badgerPath, 0700); mkErr != nil {
		return fmt.Errorf("create dir %s: %w", badgerPath, mkErr)
	}
	// cfg.AgentKey is the stable node federation transport credential.
	// It may also be the genesis Root credential on an older/fresh node, but Root
	// handover does not rotate this transport identity: peers pin its exact public
	// key during JOIN. Generate it only before any node identity exists. Once
	// genesis, agreement, or crash-surviving store state exists, missing/corrupt
	// material is a recovery event and startup must never mint a replacement.
	federationTransportKey, transportKeyErr := loadStableFederationTransportKey(
		cfg.AgentKey, SageHome(), cfg.DataDir, cometHome,
	)
	if transportKeyErr != nil {
		return fmt.Errorf("load stable node federation transport identity: %w", transportKeyErr)
	}
	if transportID := federationTransportKeyLabel(federationTransportKey); transportID != "" {
		logger.Info().
			Str("transport_operator_id", transportID[:16]+"...").
			Msg("loaded stable node federation transport identity")
	}
	pidPath := filepath.Join(SageHome(), "sage.pid")
	pidValue := strconv.Itoa(os.Getpid())
	preservePIDForExec := false
	if pidErr := os.WriteFile(pidPath, []byte(pidValue), 0600); pidErr != nil {
		return fmt.Errorf("write daemon pid: %w", pidErr)
	}
	defer func() {
		// A newer owner may have replaced the file during an unusual handoff;
		// remove it only when it still names this process.
		if !preservePIDForExec {
			if data, readErr := os.ReadFile(pidPath); readErr == nil && strings.TrimSpace(string(data)) == pidValue {
				_ = os.Remove(pidPath)
			}
		}
	}()

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
		if verifyErr := verifyStableFederationTransportKey(cfg.AgentKey, federationTransportKey); verifyErr != nil {
			return fmt.Errorf("preserve local transport identity across network join: %w", verifyErr)
		}
	}

	// Pre-v11 personal nodes were all born with the identical
	// "sage-personal" chain_id, which the federation self-federation guard treats
	// as the same network. Detect it, but never re-mint automatically: changing the
	// id by resetting Badger/CometBFT would discard canonical history. A governed,
	// history-preserving migration is required before federation can be enabled.
	if _, remintErr := remintLegacyChainID(cfg.DataDir, cfg, logger); remintErr != nil {
		return fmt.Errorf("re-mint legacy chain_id: %w", remintErr)
	}

	// Initialize CometBFT config (seeds a brand-new chain's genesis with the operator
	// admin) and, if a prior admin-less genesis survived a reset, re-inject the seed.
	// One helper so the heal step can't be dropped from serve unnoticed (issue #52).
	if initErr := ensureGenesisSeedWithConfig(cometHome, cfg, logger); initErr != nil {
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
	if genChainID, cidErr := readChainIDFromGenesis(cometHome); cidErr == nil &&
		genChainID != "" &&
		(genChainID != cfg.ChainID || vendoredChainIDRecovered) {
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
	var backgroundWG sync.WaitGroup
	var workerAdmissionMu sync.Mutex
	workersClosing := false
	startWorker := func(fn func()) {
		workerAdmissionMu.Lock()
		if workersClosing {
			workerAdmissionMu.Unlock()
			return
		}
		backgroundWG.Add(1)
		workerAdmissionMu.Unlock()
		go func() {
			defer backgroundWG.Done()
			fn()
		}()
	}
	var listenerWG sync.WaitGroup
	startListener := func(fn func()) {
		listenerWG.Add(1)
		go func() {
			defer listenerWG.Done()
			fn()
		}()
	}
	var stopWorkersOnce sync.Once
	stopWorkers := func() {
		stopWorkersOnce.Do(func() {
			// Close admission under the same lock used by Add. This makes the
			// transition to Wait race-free even if a force-closed HTTP handler is
			// still unwinding and tries to queue one last dashboard job.
			workerAdmissionMu.Lock()
			workersClosing = true
			cancelRun()
			workerAdmissionMu.Unlock()
			backgroundWG.Wait()
		})
	}
	trackDone := func(done <-chan struct{}) {
		if done == nil {
			return
		}
		startWorker(func() { <-done })
	}
	sqliteStore, err := store.NewSQLiteStore(ctx, sqlitePath)
	if err != nil {
		return fmt.Errorf("open SQLite: %w", err)
	}
	defer func() {
		stopWorkers()
		_ = sqliteStore.Close()
	}()

	// Created before the ABCI app so canonical scoped-projection recovery can
	// publish a fail-closed readiness state during construction.
	health := metrics.NewHealthChecker()
	health.Version = version
	health.SetPostgresHealth(true) // SQLite is always "healthy"
	if cfg.VendoredAgentBootstrap != nil {
		health.SetVendoredAgentEnrollmentStatus(metrics.VendoredAgentEnrollmentStatus{
			Required: true,
			OK:       false,
			State:    "checking",
		})
	}

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
	// Manager for OpenAI's tunnel-client sidecar used by the ChatGPT setup
	// flow. The runtime key remains process-env only; profiles contain no key.
	tunnelClientMgr := tunnelclientd.New(SageHome())

	// Persisted reranker intent (Settings > Engine toggle) overrides the env
	// config so an operator's dashboard on/off choice survives restart without
	// needing SAGE_RERANK_* env vars. A stored "0" explicitly turns it off.
	managedReranker := false
	managedOllama := false
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
		managedReranker = prefs["reranker_managed"] == "1"
		managedOllama = prefs["ollama_managed"] == "1"
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
	badgerOwnedByRuntime := false
	defer func() {
		stopWorkers()
		if !badgerOwnedByRuntime {
			_ = badgerStore.CloseBadger()
		}
	}()

	// Create SAGE ABCI app with SQLite backend
	app, err := sageabci.NewSageAppWithStores(badgerStore, sqliteStore, logger)
	if err != nil {
		return fmt.Errorf("create SAGE app: %w", err)
	}
	app.Version = version
	if domainErr := app.SetExpectedGovernanceDelegationDomain(cfg.ChainID); domainErr != nil {
		return fmt.Errorf("configure app-v20 governance domain from genesis chain_id: %w", domainErr)
	}
	vaultLockedAtStartup := sqliteStore.VaultLocked()
	if vaultLockedAtStartup {
		if isolationErr := validateLockedVaultConsensusStartup(
			ctx,
			cfg,
			filepath.Join(cometHome, "config", "genesis.json"),
			app,
		); isolationErr != nil {
			return isolationErr
		}
	}
	retainBlocks := resolveRetainBlocks(cfg.RetainBlocks, cfg.Quorum.Enabled)
	if retainBlocks > 0 {
		app.SetRetainBlocks(retainBlocks)
		logger.Info().Int64("retain_blocks", retainBlocks).Msg("block retention armed — CometBFT will prune blocks older than the window")
	}

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
		if vaultLockedAtStartup {
			isolateLockedPersonalP2P(cometCfg.P2P)
		}
	}
	if vaultLockedAtStartup {
		if replayErr := validateLockedVaultCometReplaySafety(ctx, cometCfg, app); replayErr != nil {
			return replayErr
		}
	}

	pv := privval.LoadFilePV(
		cometCfg.PrivValidatorKeyFile(),
		cometCfg.PrivValidatorStateFile(),
	)

	// Legacy missing-blockstore repair is unsafe for a state-sync receiver: its
	// signing state and WAL are freshness/double-sign evidence. Preserve them
	// until the receiver gate rejects the boot. A just-recovered activation is
	// preserved for the same reason even though its one-shot role is disabled in
	// memory above.
	preserveStateSyncEvidence := cfg.Quorum.StateSync.Receiving ||
		(recoveryFound && recoveryAction == statesync.RecoveryKeepActivated)
	pv, err = repairMissingBlockStoreArtifacts(cometCfg, pv, preserveStateSyncEvidence, logger)
	if err != nil {
		return err
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

	// CometBFT receives only the boot runtime, never a raw SageApp pointer. The
	// runtime is dormant for now: normal ABCI calls delegate to this complete
	// bundle while network state-sync endpoints remain empty/reject/abort.
	consensusBundle, err := sageabci.NewConsensusBundleWithCleanup(ctx, app, app.CloseConsensusState)
	if err != nil {
		return fmt.Errorf("create consensus bundle: %w", err)
	}
	bootRuntime, err := sageabci.NewBootStateSyncRuntime(consensusBundle)
	if err != nil {
		return fmt.Errorf("create boot state-sync runtime: %w", err)
	}
	if err := bootRuntime.ConfigureApplicationBundles(func(application abcitypes.Application) error {
		return configureStateSyncApplicationBundle(application, version, retainBlocks, cfg.ChainID)
	}); err != nil {
		return fmt.Errorf("configure state-sync application bundles: %w", err)
	}
	if vaultLockedAtStartup {
		bootRuntime.SetLocalTxAdmissionBlocked(true)
	}
	badgerOwnedByRuntime = true
	defer func() {
		stopWorkers()
		if closeErr := bootRuntime.Close(); closeErr != nil {
			logger.Error().Err(closeErr).Msg("close active consensus bundle")
		}
	}()
	// State-sync is opt-in and must be completely authorized, configured, and
	// armed before node.NewNode can expose the P2P reactor. Providers publish a
	// verified app-v20 snapshot here; receivers install their one-shot finalizer
	// and Comet light-client trust before the asynchronous bootstrap begins.
	if armErr := armConfiguredStateSync(ctx, cfg, cometCfg, pv, nodeKey, app, sqliteStore, bootRuntime, logger); armErr != nil {
		return fmt.Errorf("arm configured validator state sync: %w", armErr)
	}

	// Create the node controller — manages CometBFT lifecycle for redeployment.
	nodeCtrl := NewSageNodeController(cometCfg, bootRuntime, pv, nodeKey, cmtLogger, logger, cfg.DataDir)

	if err := nodeCtrl.StartChain(); err != nil {
		return fmt.Errorf("start CometBFT: %w", err)
	}
	var restartFenceRelease func()
	var preparedRestart preparedRestartRequest
	var restartRequested chan preparedRestartRequest
	defer func() {
		// A fenced request transfers ownership when it is queued. Reclaim any
		// adopted or still-buffered callback before StopChain: holding the app
		// read fence while Comet waits for an already-entered Commit deadlocks.
		releaseRestartFence(&restartFenceRelease)
		if preparedRestart.abort != nil {
			preparedRestart.abort()
		}
		releaseQueuedRestartFences(restartRequested)
		stopWorkers()
		if stopErr := nodeCtrl.StopChain(); stopErr != nil {
			logger.Error().Err(stopErr).Msg("error stopping CometBFT")
		}
	}()
	cometNode := nodeCtrl.GetCometNode()
	if cometNode == nil {
		return errors.New("CometBFT reported successful startup without a live node")
	}
	stateSyncStartupTimeout, timeoutErr := cfg.Quorum.StateSync.effectiveStartupTimeout()
	if timeoutErr != nil {
		return fmt.Errorf("resolve state-sync startup timeout: %w", timeoutErr)
	}
	var receiverValidatorPublicKey []byte
	if cfg.Quorum.StateSync.Receiving {
		receiverPublicKey, publicKeyErr := pv.GetPubKey()
		if publicKeyErr != nil || receiverPublicKey == nil {
			return errors.New("read state-sync receiver validator public key before Comet seal")
		}
		receiverValidatorPublicKey = append([]byte(nil), receiverPublicKey.Bytes()...)
	}
	cometStateReader, readerErr := newRunningCometStateReader(cometNode, receiverValidatorPublicKey)
	if readerErr != nil {
		return fmt.Errorf("prepare running CometBFT state reader: %w", readerErr)
	}
	durableSeal := func(height int64, appHash []byte, _ uint64) error {
		if height <= 0 {
			return errors.New("state-sync activation seal has a non-positive height")
		}
		sealedHeight := uint64(height) // #nosec G115 -- positive height checked above
		journal, journalErr := statesync.LoadActivationJournal(
			filepath.Join(cfg.DataDir, stateSyncActivationJournalName),
		)
		if journalErr != nil {
			return fmt.Errorf("load state-sync activation journal before projection completion: %w", journalErr)
		}
		return statesync.SealActivatedDirectory(
			cfg.DataDir,
			filepath.Join(cfg.DataDir, stateSyncActivationJournalName),
			sealedHeight,
			appHash,
			sealedHeight,
			appHash,
			func() error {
				return validateStateSyncProjectionBaselinePendingAndComplete(
					cfg,
					nodeKey,
					receiverValidatorPublicKey,
					journal.Height,
					journal.AppHash,
				)
			},
		)
	}
	sealCtx, cancelSeal := context.WithTimeout(ctx, stateSyncStartupTimeout)
	sealResult, sealErr := waitForBootStateSyncSeal(sealCtx, bootRuntime, cometStateReader, durableSeal)
	cancelSeal()
	if sealErr != nil {
		// A post-switch block-sync call may be waiting behind the PendingComet
		// barrier. Close marks the runtime failed and releases that call before
		// the deferred Comet StopChain waits for its reactor goroutines.
		if closeErr := bootRuntime.Close(); closeErr != nil {
			return errors.Join(
				fmt.Errorf("seal boot state sync before normal serving: %w", sealErr),
				fmt.Errorf("close failed boot state-sync runtime: %w", closeErr),
			)
		}
		return fmt.Errorf("seal boot state sync before normal serving: %w", sealErr)
	}
	if sealResult.activated {
		logger.Info().
			Int64("height", sealResult.height).
			Str("app_hash", hex.EncodeToString(sealResult.appHash)).
			Msg("authorized validator state-sync activation sealed before service admission")
	}
	servingApplication, gateErr := acquireNormalServingAfterStateSyncRecovery(stateSyncRecoveryComplete, bootRuntime)
	if gateErr != nil {
		return fmt.Errorf("normal serving blocked by state sync startup gate: %w", gateErr)
	}
	servingApp, ok := servingApplication.(*sageabci.SageApp)
	if !ok {
		return fmt.Errorf("normal serving requires *abci.SageApp, got %T", servingApplication)
	}
	app = servingApp
	badgerStore = app.GetBadgerStore()
	if badgerStore == nil {
		return errors.New("normal-serving SAGE app has no Badger store")
	}
	// One authoritative inventory for every current-Root operation. In
	// particular, the exact app-owned vendored key path may live outside the
	// conventional ~/.sage key directories; readiness and the upgrade watchdog
	// must agree on whether that credential is locally held.
	resolveLocalAgentKey := localAgentKeyResolverForConfig(cfg)
	// Fresh direct-v23 Init has now completed, and a state-sync receiver has been
	// sealed. Verify immutable vendored provenance before sidecars, migrations,
	// watchdogs, voters, or serving workers start. The companion remains
	// fail-closed in /ready until governed app-v24 activation reaches its strict
	// H+1 boundary: app-v23's terminal lifecycle bug can otherwise make the
	// first-party app write memories that it cannot subsequently read.
	if cfg.VendoredAgentBootstrap != nil {
		verifyVendoredEnrollment := func() error {
			return verifyAppV23VendoredAgentReadiness(
				cfg.VendoredAgentBootstrap,
				resolveLocalAgentKey,
				badgerStore,
			)
		}
		if readinessErr := verifyVendoredEnrollment(); readinessErr != nil {
			health.SetVendoredAgentEnrollmentStatus(metrics.VendoredAgentEnrollmentStatus{
				Required: true,
				State:    "blocked",
			})
			return fmt.Errorf("first-party app-v23 companion is not ready: %w", readinessErr)
		}
		if app.IsAppV24ActiveForNextTx() {
			health.SetVendoredAgentEnrollmentStatus(vendoredAgentProtocolStatus(true))
			logger.Info().
				Str("home_domain", cfg.VendoredAgentBootstrap.HomeDomain).
				Msg("first-party companion enrollment and app-v24 lifecycle are ready")
		} else {
			health.SetVendoredAgentEnrollmentStatus(vendoredAgentProtocolStatus(false))
			logger.Info().
				Str("home_domain", cfg.VendoredAgentBootstrap.HomeDomain).
				Msg("first-party companion is waiting for governed app-v24 activation")
			startWorker(func() {
				ticker := time.NewTicker(time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if !app.IsAppV24ActiveForNextTx() {
							continue
						}
						if readinessErr := verifyVendoredEnrollment(); readinessErr != nil {
							health.SetVendoredAgentEnrollmentStatus(metrics.VendoredAgentEnrollmentStatus{
								Required: true,
								State:    "blocked",
							})
							logger.Error().Err(readinessErr).
								Msg("first-party companion enrollment changed before app-v24 readiness")
							return
						}
						health.SetVendoredAgentEnrollmentStatus(vendoredAgentProtocolStatus(true))
						logger.Info().
							Str("home_domain", cfg.VendoredAgentBootstrap.HomeDomain).
							Msg("first-party companion admitted after app-v24 activation")
						return
					}
				}
			})
		}
	}

	// Everything below may capture concrete app/store references because the
	// acquire above permanently freezes boot bundle replacement.
	if managedReranker {
		startWorker(func() {
			startCtx, cancel := context.WithTimeout(ctx, 3*time.Minute)
			defer cancel()
			rerankURL, startErr := rerankdMgr.Start(startCtx)
			if startErr != nil {
				logger.Warn().Err(startErr).Msg("managed reranker sidecar did not start; recall continues without reranking")
				return
			}
			logger.Info().Str("url", rerankURL).Msg("managed reranker sidecar ready")
		})
	}
	if managedOllama {
		startWorker(func() {
			superviseManagedOllama(ctx, ollamaMgr, 30*time.Second, logger)
		})
	}

	// Seed the replay-nonce allocator from the final chain store. This callback
	// must never capture the closed pre-state-sync Badger instance.
	tx.SetNonceFloorFunc(func(pub ed25519.PublicKey) (uint64, bool) {
		n, gerr := badgerStore.GetNonce(auth.PublicKeyToAgentID(pub))
		if gerr != nil || n == 0 {
			return 0, false
		}
		return n, true
	})

	// Badger is canonical scoped content; SQLite is only the serving projection.
	// Verify/rebuild only after the final app/store graph is frozen.
	rebuildScopedProjection := func(rebuildCtx context.Context) (int, error) {
		contents, listErr := badgerStore.ListScopedContents()
		if listErr != nil {
			health.SetScopedProjectionStatus(metrics.ScopedProjectionStatus{Required: true, Detail: listErr.Error()})
			return 0, listErr
		}
		status := scopedProjectionReadinessStatus(len(contents), sqliteStore.VaultLocked())
		if sqliteStore.VaultLocked() {
			health.SetScopedProjectionStatus(status)
			return 0, fmt.Errorf("%s", status.Detail)
		}
		if len(contents) == 0 {
			health.SetScopedProjectionStatus(status)
			return 0, nil
		}
		rebuilt, rebuildErr := app.RebuildScopedProjection(rebuildCtx)
		status.Rebuilt = rebuilt
		status.OK = rebuildErr == nil && rebuilt == len(contents)
		if rebuildErr != nil {
			status.Detail = rebuildErr.Error()
		} else if !status.OK {
			status.Detail = fmt.Sprintf("rebuilt %d of %d scoped records", rebuilt, len(contents))
			rebuildErr = fmt.Errorf("%s", status.Detail)
		}
		health.SetScopedProjectionStatus(status)
		return rebuilt, rebuildErr
	}
	if rebuilt, rebuildErr := rebuildScopedProjection(ctx); rebuildErr != nil {
		logger.Warn().Err(rebuildErr).Msg("scoped serving projection is not ready")
	} else if rebuilt > 0 {
		logger.Info().Int("records", rebuilt).Msg("scoped serving projection verified from canonical state")
	}

	var projectionBaselineMu sync.RWMutex
	var projectionBaselineAllowed map[string]struct{}
	projectionBaselineRequired := cfg.Quorum.StateSync.Received
	ensureProjectionBaseline := func(baselineCtx context.Context) error {
		if !projectionBaselineRequired {
			return nil
		}
		info, baselineInfoErr := app.Info(baselineCtx, nil)
		if baselineInfoErr != nil || info == nil {
			if baselineInfoErr == nil {
				baselineInfoErr = errors.New("nil application info")
			}
			return fmt.Errorf("read canonical state for projection baseline: %w", baselineInfoErr)
		}
		projectionNodeKey, validatorPublicKey, identityErr := stateSyncProjectionLocalIdentity(cometHome)
		if identityErr != nil {
			return identityErr
		}
		baseline, baselineErr := ensureStateSyncProjectionBaseline(
			cfg.DataDir,
			cfg.ChainID,
			projectionNodeKey,
			validatorPublicKey,
			info.LastBlockHeight,
			info.LastBlockAppHash,
			badgerStore,
		)
		if baselineErr != nil {
			return baselineErr
		}
		allowed := baselineAllowedMissingIDs(baseline)
		projectionBaselineMu.Lock()
		projectionBaselineAllowed = allowed
		projectionBaselineMu.Unlock()
		return nil
	}
	if baselineErr := ensureProjectionBaseline(ctx); baselineErr != nil {
		logger.Error().Err(baselineErr).
			Msg("state-sync ordinary-memory projection baseline unavailable; using strict readiness")
	}

	if warn := app.ContentValidationEnforcementWarning(); warn != "" {
		logger.Warn().Msg(warn)
	}

	snapEncrypted := vaultUnlocked && vaultPassphrase != ""
	snapKeep := 5
	if v := os.Getenv("SAGE_SNAPSHOT_KEEP"); v != "" {
		if n, perr := strconv.Atoi(v); perr == nil && n > 0 {
			snapKeep = n
		} else {
			logger.Warn().Str("value", v).Int("using", snapKeep).Msg("ignoring invalid SAGE_SNAPSHOT_KEEP (want integer >= 1)")
		}
	}
	var snapshotScheduler *sageabci.SnapshotScheduler
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
		CometDBBackend:  cometCfg.DBBackend,
	}, logger); sched != nil {
		snapshotScheduler = sched
		app.SetSnapshotScheduler(sched)
		logger.Info().Msg("v7.5 snapshot scheduler armed")
	}

	// Local snapshot cleanup and all serving/search workers start only after the
	// state-sync seal. They can never observe quarantine or the pre-sync store.
	go func() { //nolint:gosec
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
	startWorker(func() {
		start := time.Now()
		if ftsErr := sqliteStore.BackfillFTS(ctx); ftsErr != nil {
			logger.Warn().Err(ftsErr).Msg("FTS5 backfill failed — text search may be incomplete")
			return
		}
		logger.Info().Dur("elapsed", time.Since(start)).Msg("FTS5 backfill complete (or already current)")
	})

	embedProvider := createEmbeddingProvider(cfg, logger)
	embedderReady := make(chan struct{}, 1)
	trackDone(startEmbedderWatchdog(ctx, embedProvider, health, logger, func() {
		select {
		case embedderReady <- struct{}{}:
		default:
		}
	}))

	health.SetCometBFTHealth(true)
	logger.Info().
		Str("node_id", string(cometNode.NodeInfo().ID())).
		Msg("CometBFT node started (single-validator personal mode)")

	// Legacy challenged-memory cleanup is intentionally not run at startup.
	// Its historical implementation rewrote only the SQLite serving projection,
	// which can diverge from canonical Badger status. Any future repair must be a
	// governed/canonical transition rather than an automatic boot sweep.

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
	if migrationErr := migrateAgentsOnChainAtStartup(
		ctx,
		sqliteStore,
		badgerStore,
		cometRPC,
		signingKeyForMigrate,
		cfg.Quorum.Enabled,
		logger,
	); migrationErr != nil {
		return fmt.Errorf(
			"reconcile consensus agent serving projection: %w",
			migrationErr,
		)
	}

	// v7.5 upgrade watchdog: auto-propose an UpgradePlan when the
	// running binary's embedded TargetAppVersion exceeds the chain's
	// current app version. No-op when target == current (the steady
	// state for releases that don't change consensus rules).
	startUpgradeWatchdog(ctx, upgradeWatchdogConfig{
		BinaryVersion: version,
		ChainID:       cfg.ChainID,
		CometRPC:      cometRPC,
		Logger:        logger,
		ResolveSigningKey: func() (ed25519.PrivateKey, error) {
			return currentUpgradeSigningKey(
				badgerStore,
				app.IsAppV23ActiveForNextTx,
				resolveLocalAgentKey,
				cfg.AgentKey,
				logger,
			)
		},
		// v10.5.1 auto-advance: personal nodes walk the fork ladder to the
		// binary ceiling automatically (issue #40 follow-up — updating the
		// binary now brings the chain up to date too). Quorum clusters keep
		// governed multi-validator activation. v11.17.1 requires app-v26 and
		// never lets disable_auto_upgrade suppress personal-node consensus
		// migrations: binary/chain-rule divergence is not a supported state.
		PersonalMode:       !cfg.Quorum.Enabled,
		AutoAdvance:        true,
		RequiredAppVersion: 26,
		// v10.5.2 (issue #41): in-process pending-plan accessor for the
		// always-on pump and the auto-advance pre-check. GetUpgradePlan's
		// ErrNoUpgradePlan is flattened to nil by readPendingPlan.
		PendingPlan: badgerStore.GetUpgradePlan,
		StartWorker: startWorker,
	})

	// Create REST server
	restServer := rest.NewServer(cometRPC, sqliteStore, sqliteStore, badgerStore, health, logger, embedProvider)
	// This runtime's Comet home is authoritative. Close the legacy env-loaded
	// gateway before attempting that explicit key so a stale VALIDATOR_KEY_FILE
	// can never survive a failed home-key load.
	restServer.DisableValidatorSigningKey()
	validatorSigningKey := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger)
	if validatorSigningKey == nil {
		logger.Error().Str("key_file", cometCfg.PrivValidatorKeyFile()).Msg("REST governance disabled: live validator signing key unavailable")
	} else if keyErr := restServer.SetValidatorSigningKey(validatorSigningKey); keyErr != nil {
		logger.Error().Err(keyErr).Msg("REST governance disabled: invalid validator signing key")
	}
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
	restServer.SetPostV17ForNextTxAccessor(app.IsAppV17ActiveForNextTx)
	restServer.SetPostV20ForNextTxAccessor(app.IsAppV20ActiveForNextTx)
	restServer.SetPostV22ForNextTxAccessor(app.IsAppV22ActiveForNextTx)
	restServer.SetPostV23ForNextTxAccessor(app.IsAppV23ActiveForNextTx)
	restServer.SetGovernanceDomainAccessor(app.GovernanceDelegationDomain)
	// App-v23 Root is a non-delegable local credential. Before v23, legacy
	// keyless MCP bearers intentionally fell back to cfg.AgentKey; at activation
	// that identity is the initial CEREBRUM Root credential. A later Root handover
	// retires its Root authority but deliberately leaves its separate JOIN-frozen
	// federation transport role unchanged. Gate issuance and lookup synchronously
	// on the live app version, then durably revoke those rows at boot/activation.
	sqliteStore.SetMCPTokenKeyedIdentityRequirement(app.IsAppV23ActiveForNextTx)
	revokeLegacyMCPBearers := func() bool {
		revoked, err := sqliteStore.RevokeAppV23LegacyMCPBearers(ctx)
		if err != nil {
			logger.Warn().Err(err).Msg("app-v23 legacy MCP bearer revocation incomplete")
			return false
		}
		if revoked > 0 {
			logger.Warn().Int64("revoked", revoked).Msg("app-v23 revoked legacy MCP bearers that could inherit Root")
		}
		return true
	}
	legacyMCPRevocationComplete := false
	if app.IsAppV23ActiveForNextTx() {
		legacyMCPRevocationComplete = revokeLegacyMCPBearers()
	}
	if !legacyMCPRevocationComplete {
		startWorker(func() {
			ticker := time.NewTicker(time.Second)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					if app.IsAppV23ActiveForNextTx() && revokeLegacyMCPBearers() {
						return
					}
				}
			}
		})
	}

	// Tell the REST layer which ed25519 public key identifies the local node
	// operator. This preserves the explicit operator scope for legacy direct
	// REST administration; project lifecycle hooks use their ordinary-agent
	// workspace key and never inherit this identity. Skip if agent.key is
	// unreadable; the separate operator compatibility scope simply stays off.
	operatorAgentID, operatorIDErr := readNodeOperatorKey(cfg.AgentKey)
	if operatorIDErr == nil && operatorAgentID != "" {
		restServer.SetNodeOperatorID(operatorAgentID)
		if governanceErr := restServer.SetGovernanceOperatorID(operatorAgentID); governanceErr != nil {
			logger.Error().Err(governanceErr).Msg("REST governance disabled: invalid operator identity")
		}
		// One registrar backs direct REST, OAuth, and wizard token issuance.
		// Vault-backed bearers remain pending (and cannot authenticate) until
		// their standard AgentRegister transaction is confirmed on-chain.
		restServer.ConfigureMCPTokenIdentityRegistrar(sqliteStore)
		// A canceled /broadcast_tx_commit response is ambiguous: the chain may
		// already have accepted a token identity even though issuance retained its
		// bearer fail-closed as pending. Re-run idempotent AgentRegister at startup
		// to finalize those rows without ever treating an unknown result as absent.
		startWorker(func() {
			confirmed, reconcileErr := sqliteStore.ReconcilePendingMCPTokenIdentities(ctx)
			if reconcileErr != nil {
				logger.Warn().Err(reconcileErr).Msg("pending MCP token identity reconciliation incomplete")
			} else if confirmed > 0 {
				logger.Info().Int("count", confirmed).Msg("reconciled pending MCP token identities")
			}
		})
		logger.Info().Str("operator_id", operatorAgentID[:16]+"...").Msg("node operator key registered for direct REST compatibility scope")
	} else if operatorIDErr != nil {
		logger.Warn().Err(operatorIDErr).Msg("node operator key unavailable")
	}

	// Create dashboard handler. Restart requests are routed back to this main
	// lifecycle so every listener/store/consensus component drains before exec.
	restartRequested = make(chan preparedRestartRequest, 1)
	dashboard := web.NewDashboardHandler(sqliteStore, version)
	dashboard.NodeOperatorAgentID = operatorAgentID
	dashboard.RunBackground = func(fn func(context.Context)) {
		startWorker(func() { fn(ctx) })
	}
	projectionAuditRequests := make(chan struct{}, 1)
	requestProjectionAudit := func() {
		select {
		case projectionAuditRequests <- struct{}{}:
		default:
			// One pending request already represents every source mutation
			// observed before the scheduler's trailing-edge audit.
		}
	}
	dashboard.RequestRestart = func() error {
		return prepareAndQueueRestart(context.Background(), snapshotScheduler, restartRequested, nil)
	}
	dashboard.RequestRestartWithFence = func(release func()) error {
		if release == nil {
			return fmt.Errorf("restart state fence is unavailable")
		}
		return prepareAndQueueRestart(context.Background(), snapshotScheduler, restartRequested, release)
	}
	dashboard.PrepareRestartDrain = func(prepareCtx context.Context) (func(), func(), error) {
		if snapshotScheduler == nil {
			return nil, nil, fmt.Errorf("snapshot scheduler is unavailable")
		}
		return snapshotScheduler.PrepareQuiesce(prepareCtx)
	}
	dashboard.RequestRestartPrepared = func(release, commit, abort func()) error {
		if commit == nil || abort == nil {
			return fmt.Errorf("prepared restart ownership is incomplete")
		}
		fallbackExec, preserveErr := snapshotScheduler.PreserveRunningBinary()
		if preserveErr != nil {
			abort()
			return fmt.Errorf("preserve exact running executable before drain: %w", preserveErr)
		}
		select {
		case restartRequested <- preparedRestartRequest{
			release: release, commit: commit, abort: abort, fallbackExec: fallbackExec,
		}:
			return nil
		default:
			return fmt.Errorf("restart already in progress")
		}
	}
	dashboard.PrepareVersionTransition = func(snapshotCtx context.Context, targetVersion string) (_ func(), retErr error) {
		if snapshotScheduler == nil {
			return nil, fmt.Errorf("snapshot scheduler is unavailable")
		}
		height, appHash, release := app.AcquireSnapshotStateFence()
		handedOff := false
		defer func() {
			if !handedOff {
				release()
			}
		}()
		if height <= 0 || len(appHash) == 0 {
			return nil, fmt.Errorf("read committed state for pre-update snapshot: no committed application state")
		}
		_, snapshotErr := snapshotScheduler.TakeVerified(
			snapshotCtx,
			height,
			appHash,
			"pre-upgrade-"+strings.TrimPrefix(targetVersion, "v"),
			func(manifest *snapshot.Manifest) error {
				if manifest == nil || manifest.Height != height || !bytes.Equal(manifest.AppHash, appHash) {
					return fmt.Errorf("committed state changed while proving the pre-update snapshot")
				}
				return nil
			},
		)
		if snapshotErr != nil {
			return nil, snapshotErr
		}
		handedOff = true
		return release, nil
	}
	dashboard.Rerankd = rerankdMgr            // managed reranker sidecar (guided setup)
	dashboard.Ollamad = ollamaMgr             // managed Ollama runtime for smart memory setup
	dashboard.TunnelClient = tunnelClientMgr  // managed OpenAI tunnel-client for ChatGPT/Codex
	dashboard.BadgerStore = badgerStore       // Wire on-chain RBAC for agent isolation
	dashboard.PostV8ForkFn = app.IsPostV8Fork // v8.0: ancestor-walk grants on post-fork dashboards
	dashboard.PostV22ForNextTxFn = app.IsAppV22ActiveForNextTx
	dashboard.ScopedProjectionRebuildFn = func(rebuildCtx context.Context) (int, error) {
		rebuilt, rebuildErr := rebuildScopedProjection(rebuildCtx)
		if rebuildErr != nil {
			logger.Warn().Err(rebuildErr).Msg("scoped serving projection rebuild incomplete")
		} else if rebuilt > 0 {
			logger.Info().Int("records", rebuilt).Msg("scoped serving projection ready after vault unlock")
		}
		if rebuildErr == nil {
			if baselineErr := ensureProjectionBaseline(rebuildCtx); baselineErr != nil {
				logger.Error().Err(baselineErr).
					Msg("state-sync ordinary-memory projection baseline unavailable after vault unlock")
				rebuildErr = baselineErr
			}
		}
		return rebuilt, rebuildErr
	}

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
	dashboard.OnVaultUnlocked = func(_ string) {
		// publishUnlockedVault attaches the vault to SQLite before invoking this
		// callback, so no CheckTx can observe admission open with a locked
		// projection.
		bootRuntime.SetLocalTxAdmissionBlocked(false)
		requestProjectionAudit()
	}
	// The health watchdog signals every healthy probe, not only startup. This
	// makes vector repair eventual after a late Ollama start or transient provider
	// outage without a full-store UI poll or a manual "Finish setup" click.
	startWorker(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-embedderReady:
				if _, repairErr := dashboard.StartEmbeddingRepairIfNeeded(ctx); repairErr != nil {
					logger.Warn().Err(repairErr).Msg("automatic embedding repair could not start")
				}
			}
		}
	})
	dashboard.VaultKeyPath = filepath.Join(SageHome(), "vault.key")
	dashboard.SaveEncryptionConfig = func(enabled bool) error {
		cfg.Encryption.Enabled = enabled
		return SaveConfig(cfg)
	}

	// Wire CometBFT consensus for dashboard agent operations (Step 7).
	// Agent create/update will be broadcast on-chain in addition to direct SQLite writes.
	dashboard.CometBFTRPC = cometRPC
	dashboard.RESTAddr = cfg.RESTAddr // surfaced read-only in Settings > Connection
	// JOIN codes are signed over the exact advertised endpoints. Surface the
	// same effective address used by the listener so a custom port cannot be
	// silently replaced with the historical 8444 default in the QR payload.
	dashboard.FederationAddr = effectiveFederationListenAddr(cfg.Federation.ListenAddr)
	if cfg.Federation.Enabled {
		fedAddr, fedAddrErr := validatedFederationListenAddr(cfg.Federation.ListenAddr)
		if fedAddrErr != nil {
			return fmt.Errorf("invalid federation listen_addr %q: %w", cfg.Federation.ListenAddr, fedAddrErr)
		}
		dashboard.FederationAddr = fedAddr
	}
	mcpConfigAPIURL = restBaseURL(cfg.RESTAddr)
	if currentExec, execErr := os.Executable(); execErr == nil {
		if resolvedExec, resolveErr := filepath.EvalSymlinks(currentExec); resolveErr == nil {
			currentExec = resolvedExec
		}
		for _, healErr := range selfHealKnownMCPConfigs(SageHome(), currentExec) {
			logger.Warn().Err(healErr).Msg("could not refresh an existing global MCP config")
		}
	}
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
	dashboard.AppV18ActiveFn = app.IsAppV18ActiveForNextTx
	dashboard.AppV19ActiveFn = app.IsAppV19ActiveForNextTx // app-v19: local-agents-default-READ flip (off-consensus)
	dashboard.AppV20ActiveFn = app.IsAppV20ActiveForNextTx
	dashboard.AppV22ActiveFn = app.IsAppV22ActiveForNextTx
	dashboard.AppV23ActiveFn = app.IsAppV23ActiveForNextTx
	dashboard.AppV24ActiveFn = app.IsAppV24ActiveForNextTx
	dashboard.AppV26ActiveFn = app.IsAppV26ActiveForNextTx
	if projectionBaselineRequired {
		dashboard.CanonicalProjectionMissingAllowedFn = func(memoryID string) bool {
			projectionBaselineMu.RLock()
			allowed := projectionBaselineAllowed
			projectionBaselineMu.RUnlock()
			return stateSyncProjectionMissingAllowed(allowed, badgerStore, memoryID)
		}
	}
	app.SetCanonicalProjectionAuditNotifier(func() {
		requestProjectionAudit()
	})
	health.SetCanonicalMemoryProjectionProvider(func() metrics.CanonicalMemoryProjectionStatus {
		status := badgerStore.CanonicalMemoryProjectionHealth()
		return metrics.CanonicalMemoryProjectionStatus{
			Checked:          status.Checked,
			Required:         app.IsAppV23ActiveForNextTx(),
			OK:               status.OK,
			State:            status.State,
			LegacyCompatible: status.LegacyCompatible,
			Quarantined:      status.Quarantined,
		}
	})
	dashboard.ConfigureAppV25Maintenance(app.IsAppV25ActiveForNextTx())
	health.SetAppV25MaintenanceProvider(func() metrics.AppV25MaintenanceStatus {
		status := dashboard.AppV25MaintenanceStatus()
		requiredNow := app.IsAppV25ActiveForNextTx()
		if requiredNow && !status.Required {
			// Close the activation-height window before the worker's first
			// timer tick. The chain can cross into app-v25 after startup.
			status.Required = true
			status.Checked = false
			status.OK = false
			status.State = "checking"
		}
		return metrics.AppV25MaintenanceStatus{
			Checked:  status.Checked,
			Required: status.Required,
			OK:       status.OK,
			Recovery: status.Recovery,
			State:    status.State,
		}
	})
	startWorker(func() {
		runCanonicalProjectionAuditScheduler(
			ctx,
			projectionAuditRequests,
			canonicalProjectionAuditDebounce,
			func(auditCtx context.Context) {
				if projectionErr := dashboard.AuditAppV23CanonicalMemoryProjection(auditCtx); projectionErr != nil {
					logger.Warn().Err(projectionErr).
						Msg("canonical ordinary-memory projection audit incomplete")
				}
			},
		)
	})
	dashboard.GovernanceDomainFn = app.GovernanceDelegationDomain
	startWorker(func() {
		dashboard.RunAppV25LegacyAdoptionWorker(
			ctx,
			app.IsAppV25ActiveForNextTx,
			logger,
		)
	})
	startWorker(func() {
		dashboard.RunAppV25DomainContinuityWorker(
			ctx,
			app.IsAppV25ActiveForNextTx,
			logger,
		)
	})
	dashboard.StrictRBAC = cfg.RBAC.Strict // opt-out of the app-v19 default-read flip
	// Embeddings setup: flip the config to the bundled Ollama + nomic-embed-text
	// provider (the node re-reads it on restart). The embedder is locked to this.
	dashboard.SetEmbeddingProvider = func(provider string) error {
		cfg.Embedding.Provider = provider
		cfg.Embedding.Dimension = 768
		if provider == "ollama" {
			cfg.Embedding.BaseURL = "http://localhost:11434"
			cfg.Embedding.Model = "nomic-embed-text"
		} else {
			cfg.Embedding.BaseURL = ""
			cfg.Embedding.Model = ""
		}
		return SaveConfig(cfg)
	}
	dashboard.SetNetworkMode = func(enabled bool) error {
		return setNetworkMode(cfg, enabled)
	}
	// Federation on/off, surfaced in Settings (the toggle re-execs to start/stop
	// the inbound mTLS listener). Outbound recall/receipt delivery is unaffected.
	dashboard.FederationEnabled = cfg.Federation.Enabled
	dashboard.SetFederationEnabledFn = func(enabled bool) error {
		cfg.Federation.Enabled = enabled
		// Raw round-trip (NOT SaveConfig(cfg)) so the operator's tilde/relative
		// paths aren't rewritten to runtime-expanded absolutes by the toggle.
		return persistFederationEnabled(enabled)
	}
	// Friendly network name (rename): persist to config.yaml via the same raw
	// round-trip. The dashboard also pushes it to the live Manager so the next
	// join ceremony carries it without a restart.
	dashboard.SetNetworkNameFn = func(name string) error {
		clean := sanitizeNetworkName(name)
		cfg.NetworkName = clean
		return persistNetworkName(clean)
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
	if validatorSigningKey != nil {
		dashboard.SigningKey = validatorSigningKey
	}
	// The stable operator/transport key (~/.sage/agent.key) authorizes legacy
	// pre-v23 admin-gated actions. After app-v20 governance keeps this key as an
	// embedded exact-action proof while the live validator key above remains the
	// outer proposer/voter; pre-v20 GovPropose and non-governance DomainReassign
	// retain their direct historical signing path. App-v23 Root authority is
	// resolved separately after any handover. Owner-scoped AccessGrant/
	// AccessRevoke resolve the domain-owner key. Memory submits still use the
	// validator outer key so authorship (submitting_agent) stays immutable.
	if adminKey := adminSigningKeyAt(cfg.AgentKey); adminKey != nil {
		dashboard.AdminSigningKey = adminKey
	}
	dashboard.ResolveAgentKeyFn = resolveLocalAgentKey
	restServer.SetAppV23RootKeyResolver(dashboard.ResolveAgentKeyFn)
	// Create redeployment orchestrator and wire it to the dashboard
	redeployer := orchestrator.NewRedeployer(sqliteStore, nodeCtrl, logger)
	dashboard.Redeployer = redeployer

	// Bridge redeployer status updates to SSE so the dashboard gets live updates
	startWorker(func() {
		for {
			select {
			case <-ctx.Done():
				return
			case status := <-redeployer.StatusChan():
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
		}
	})

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
	mcpHTTPTransport := mountMCPHTTPTransport(r, sqliteStore, cfg, logger)
	if mcpHTTPTransport != nil {
		restServer.SetMessageNotifier(func(notification rest.AgentMessageNotification) {
			mcpHTTPTransport.NotifyAgent(notification.RecipientAgentID, mcp.AgentMessageNotification{
				MessageID: notification.MessageID,
				FromAgent: notification.FromAgent,
				SentAt:    notification.SentAt,
			})
		})
	}

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
	// Public /oauth/authorize never renders CEREBRUM through the tunnel. It
	// hands an opaque short-lived handle to this loopback-only approval origin,
	// where live committed Root/Admin authority is resolved on the exact GET
	// and POST. NodeOperatorAgentID remains only a pre-v23 compatibility value;
	// it is never consulted after Root handover.
	oauthHandler.IsLocalApproval = dashboard.IsLocalCEREBRUMRequest
	oauthHandler.ResolveControlActor = dashboard.ResolveOAuthControlActor
	oauthHandler.LocalApprovalBaseURL = oauthLocalApprovalBaseURL(cfg.RESTAddr)
	oauthHandler.NodeOperatorAgentID = operatorAgentID
	rest.MountOAuthRoutes(r, oauthHandler)
	logger.Info().Msg("OAuth 2.0 + PKCE wrapper enabled (/.well-known/oauth-authorization-server, /oauth/authorize, /oauth/token)")

	// App-v23+ lifecycle state is consensus-authoritative. The historical
	// cleanup loop changed only SQLite, so its next projection audit hid each
	// affected memory as divergent. Cleanup remains available as a dry run in
	// CEREBRUM; an actual forget must travel through the canonical challenge
	// transaction path.

	// Prometheus scrape endpoint. amid serves this via internal/metrics's
	// dedicated metrics server; sage-gui has no such listener, so the default
	// registry (sage_voter_running, sage_proposed_oldest_age_seconds, …) is
	// exposed on the combined mux behind an explicit loopback boundary. The
	// REST listener may intentionally bind non-loopback for signed agent and
	// federation data-plane traffic.
	r.Method(http.MethodGet, "/metrics", loopbackOnlyMetrics(promhttp.Handler()))

	// Preview builds represented peer Write with an ordinary AccessGrant. That
	// credential is reusable through public REST or raw Comet even when the
	// federation Manager cannot start, so retirement cannot be conditional on
	// transport availability. Revoke every recorded preview grant before even
	// binding an API listener and refuse to serve if consensus cannot confirm it.
	if cleanupErr := dashboard.ReconcileFederationManagedGrants(ctx); cleanupErr != nil {
		return fmt.Errorf("retire unsafe federation Write grants before serving: %w", cleanupErr)
	}
	dashboard.StartFederationManagedGrantReconciler()

	httpServer := &http.Server{
		Addr:         cfg.RESTAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: restListenerWriteTimeout,
		IdleTimeout:  60 * time.Second,
	}
	var startupErr error
	appendStartupErr := func(name string, err error) {
		if err != nil {
			startupErr = errors.Join(startupErr, fmt.Errorf("%s listener bind: %w", name, err))
		}
	}
	restListener, restListenErr := (&net.ListenConfig{}).Listen(ctx, "tcp", cfg.RESTAddr)
	appendStartupErr("REST", restListenErr)
	serveErrors := make(chan error, 4)
	reportServeError := func(name string, err error) {
		wrapped := fmt.Errorf("%s listener failed: %w", name, err)
		logger.Error().Err(wrapped).Msg("listener stopped unexpectedly")
		select {
		case serveErrors <- wrapped:
		default:
		}
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

	// v11 federation transport: wire the manager BEFORE any listener serves.
	// federationTransportKey is the stable JOIN-frozen node transport identity,
	// not a lookup of the current rotatable app-v23 CEREBRUM Root credential. Wire
	// it before requests can reach either listener (the TLS REST listener below
	// mounts the same router). The OUTBOUND side (federated recall, receipt
	// delivery) needs no inbound listener — only active cross_fed agreements; the
	// dedicated inbound mTLS listener starts after cert auto-generation, and only
	// when federation.enabled is set.
	var fedMgr *federation.Manager
	if keyErr := verifyStableFederationTransportKey(cfg.AgentKey, federationTransportKey); keyErr != nil {
		return fmt.Errorf("start federation transport: %w", keyErr)
	} else if cfg.ChainID == "" {
		logger.Warn().Msg("no chain_id in genesis/config — federation transport disabled")
	} else {
		fedMgr = federation.NewManager(federation.Config{
			Disabled:            !cfg.Federation.Enabled,
			LocalChainID:        cfg.ChainID,
			NetworkName:         sanitizeNetworkName(cfg.NetworkName),
			CertsDir:            certsDir,
			CometRPC:            cometRPC,
			AgentKey:            federationTransportKey,
			Badger:              badgerStore,
			MemStore:            sqliteStore,
			FederatedGuestStore: sqliteStore,
			LocalGroupResolver:  badgerStore,
			QueryChallengeStore: sqliteStore,
			RootKeyResolver:     dashboard.ResolveAgentKeyFn,
			PostV20ForNextTx:    app.IsAppV20ActiveForNextTx,
			PostV22ForNextTx:    app.IsAppV22ActiveForNextTx,
			PostV23ForNextTx:    app.IsAppV23ActiveForNextTx,
			PostV26ForNextTx:    app.IsAppV26ActiveForNextTx,
			PostV8ForAccess:     app.IsPostV8Fork,
			MessageNotifier: func(targetAgentID string, notification federation.AgentMessageNotification) {
				if mcpHTTPTransport == nil {
					return
				}
				mcpHTTPTransport.NotifyAgent(targetAgentID, mcp.AgentMessageNotification{
					MessageID: notification.MessageID,
					FromAgent: notification.FromAgent,
					SentAt:    notification.CreatedAt,
				})
			},
			Logger: logger,
		})
		restServer.SetFederation(fedMgr)
		// The dashboard drives the guided JOIN wizards (cookie-authed) by calling
		// the same Manager directly - the browser has a session, not the operator
		// signing key, so it cannot reach the agent-signed REST endpoints.
		dashboard.SetFederation(fedMgr)
		// Restore the per-agreement TOTP seed cache from disk so an established
		// federation SURVIVES a restart. The cache is otherwise only populated
		// during the JOIN ceremony, so without this the fail-closed X-Sig-Version=3
		// gate returns "federation locked" for every peer after the first restart
		// (and outbound falls back to v2, which the peer then rejects) — federation
		// silently breaks on reboot. When the vault was unlocked at boot, reuse
		// that passphrase so federation seeds are wrapped at rest in the same
		// protection domain; plaintext legacy seed envelopes still load for
		// backward compatibility.
		var restoredSeeds int
		if vaultPassphrase != "" {
			restoredSeeds = fedMgr.SetVaultPassphrase(vaultPassphrase)
		} else {
			restoredSeeds = fedMgr.LoadSeedsIntoCache()
		}
		if restoredSeeds > 0 {
			logger.Info().Int("agreements", restoredSeeds).Msg("federation: restored peer seeds into cache after boot")
		}
		dashboard.OnVaultUnlocked = chainFederationVaultUnlock(
			dashboard.OnVaultUnlocked,
			fedMgr,
			func(restored int) {
				logger.Info().
					Int("agreements", restored).
					Msg("federation: refreshed encrypted peer seeds after vault unlock")
			},
		)
		// Federation Off is a complete network kill switch: no listener, recall,
		// receipts, sync drainer, route refresh, or relay reservations.
		// SetSyncNotifier MUST follow StartSyncDrainer (it nudges the channel the
		// drainer creates).
		if cfg.Federation.Enabled {
			fedMgr.StartSyncDrainer(ctx)
			defer fedMgr.StopSyncDrainer()
			app.SetSyncNotifier(fedMgr.SyncWatcher())
		}
	}

	// Optional v11.6 connectivity substrate. It starts only while federation is
	// enabled; Off must not leave outbound relay reservations or peer dials live.
	var fedP2P *sagep2p.Transport
	var fedP2PRoutesMu sync.RWMutex
	if cfg.Federation.Enabled && cfg.Federation.P2PEnabled && fedMgr != nil {
		for _, remoteChainID := range pruneExpiredFederationRoutes(&cfg.Federation, time.Now()) {
			if err := persistFederationPeer(remoteChainID, nil); err != nil {
				logger.Warn().Err(err).Str("chain_id", remoteChainID).Msg("could not prune expired federation route from config")
			} else {
				logger.Info().Str("chain_id", remoteChainID).Msg("pruned expired federation route")
			}
		}
		allowedPeerAddrs := make([]string, 0)
		for remoteChainID := range cfg.Federation.P2PPeers {
			targets, routeErr := configuredFederationRouteTargets(cfg.Federation, remoteChainID, time.Now())
			if routeErr != nil {
				logger.Warn().Err(routeErr).Str("chain_id", remoteChainID).Msg("ignoring invalid federation route")
				continue
			}
			allowedPeerAddrs = append(allowedPeerAddrs, targets...)
		}
		p2pTransport, p2pErr := sagep2p.New(ctx, sagep2p.Config{
			IdentityKeyPath:      filepath.Join(SageHome(), "p2p.key"),
			ListenAddrs:          cfg.Federation.P2PListenAddrs,
			RelayAddrs:           cfg.Federation.P2PRelayAddrs,
			AcceptInbound:        cfg.Federation.Enabled,
			AllowedPeerAddrs:     allowedPeerAddrs,
			EnforcePeerAllowlist: cfg.Federation.Enabled,
			ForcePrivate:         cfg.Federation.P2PForcePrivate,
			UserAgent:            "sage/" + version,
		})
		if p2pErr != nil {
			logger.Warn().Err(p2pErr).Msg("federation p2p transport unavailable; direct HTTPS remains active")
		} else {
			fedP2P = p2pTransport
			fedMgr.SetPeerRouteDialFunc(func(dialCtx context.Context, remoteChainID string, authenticate federation.PeerRouteAuthenticator) (federation.PeerRouteDialResult, bool, error) {
				fedP2PRoutesMu.RLock()
				targets, routeErr := configuredFederationRouteTargets(cfg.Federation, remoteChainID, time.Now())
				fedP2PRoutesMu.RUnlock()
				if routeErr != nil {
					return federation.PeerRouteDialResult{}, true, routeErr
				}
				return dialFederationP2PRoutes(dialCtx, p2pTransport, targets, authenticate)
			})
			fedMgr.SetJoinP2PHooks(federation.JoinP2PHooks{
				LocalBundle: func() (federation.JoinP2PBundle, error) {
					if !cfg.Federation.Enabled {
						return federation.JoinP2PBundle{}, errors.New("enable federation before creating an internet connection")
					}
					return localFederationRouteBundle(p2pTransport)
				},
				DialTarget: p2pTransport.DialContext,
				Begin:      p2pTransport.BeginJoin,
				BindPeer:   p2pTransport.BindJoinPeer,
				End:        p2pTransport.EndJoin,
				Persist: func(remoteChainID string, targets []string) error {
					fedP2PRoutesMu.Lock()
					defer fedP2PRoutesMu.Unlock()
					previous := append([]string(nil), cfg.Federation.P2PPeers[remoteChainID]...)
					if err := persistFederationPeer(remoteChainID, targets); err != nil {
						return err
					}
					p2pTransport.RemoveAllowedPeer(previous)
					if err := p2pTransport.AddAllowedPeer(targets); err != nil {
						_ = persistFederationPeer(remoteChainID, previous)
						if len(previous) > 0 {
							_ = p2pTransport.AddAllowedPeer(previous)
						}
						return err
					}
					if cfg.Federation.P2PPeers == nil {
						cfg.Federation.P2PPeers = make(map[string][]string)
					}
					cfg.Federation.P2PPeers[remoteChainID] = append([]string(nil), targets...)
					return nil
				},
				PersistSnapshot: func(remoteChainID string, snapshot federation.RouteSnapshot) error {
					fedP2PRoutesMu.Lock()
					defer fedP2PRoutesMu.Unlock()
					previous := append([]string(nil), cfg.Federation.P2PPeers[remoteChainID]...)
					if err := persistFederationRouteSnapshot(remoteChainID, snapshot); err != nil {
						return err
					}
					p2pTransport.RemoveAllowedPeer(previous)
					if err := p2pTransport.AddAllowedPeer(snapshot.Addrs); err != nil {
						if prior, ok := cfg.Federation.P2PRoutes[remoteChainID]; ok {
							_ = persistFederationRouteSnapshot(remoteChainID, configuredRouteSnapshot(prior))
						} else {
							_ = persistFederationPeer(remoteChainID, previous)
						}
						if len(previous) > 0 {
							_ = p2pTransport.AddAllowedPeer(previous)
						}
						return err
					}
					if cfg.Federation.P2PPeers == nil {
						cfg.Federation.P2PPeers = make(map[string][]string)
					}
					if cfg.Federation.P2PRoutes == nil {
						cfg.Federation.P2PRoutes = make(map[string]FederationRouteSnapshot)
					}
					cfg.Federation.P2PPeers[remoteChainID] = append([]string(nil), snapshot.Addrs...)
					cfg.Federation.P2PRoutes[remoteChainID] = FederationRouteSnapshot{
						PeerID: snapshot.PeerID, Protocol: snapshot.Protocol,
						Addrs:    append([]string(nil), snapshot.Addrs...),
						Revision: snapshot.Revision, IssuedAt: snapshot.IssuedAt,
						ExpiresAt: snapshot.ExpiresAt, Generation: snapshot.Generation,
					}
					return nil
				},
				LoadSnapshot: func(remoteChainID string) (federation.RouteSnapshot, bool) {
					fedP2PRoutesMu.RLock()
					defer fedP2PRoutesMu.RUnlock()
					raw, ok := cfg.Federation.P2PRoutes[remoteChainID]
					return configuredRouteSnapshot(raw), ok
				},
				Remove: func(remoteChainID string) error {
					fedP2PRoutesMu.Lock()
					defer fedP2PRoutesMu.Unlock()
					targets := append([]string(nil), cfg.Federation.P2PPeers[remoteChainID]...)
					if err := persistFederationPeer(remoteChainID, nil); err != nil {
						return err
					}
					p2pTransport.RemoveAllowedPeer(targets)
					delete(cfg.Federation.P2PPeers, remoteChainID)
					delete(cfg.Federation.P2PRoutes, remoteChainID)
					return nil
				},
			})
			fedMgr.StartRouteRefresher(ctx)
			defer fedMgr.StopRouteRefresher()
			logger.Info().
				Str("peer_id", fedP2P.Host().ID().String()).
				Int("addresses", len(fedP2P.Addrs())).
				Int("relays", len(cfg.Federation.P2PRelayAddrs)).
				Bool("inbound", cfg.Federation.Enabled).
				Msg("federation p2p transport started")
		}
	}

	// TLS listener: encrypted REST on a separate port.
	// Auto-generates self-signed certs on first run if none exist.
	// Personal mode: TLS on localhost:8443. Quorum mode: TLS on 0.0.0.0:8443.
	// Plain HTTP stays on localhost for dashboard backward compatibility.
	var tlsServer *http.Server
	var tlsListener net.Listener
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
				WriteTimeout: restListenerWriteTimeout,
				IdleTimeout:  60 * time.Second,
			}
			plainTLSListener, listenErr := (&net.ListenConfig{}).Listen(ctx, "tcp", tlsAddr)
			appendStartupErr("MCP TLS", listenErr)
			if listenErr == nil {
				tlsListener = tls.NewListener(plainTLSListener, tlsCfg)
			}
			if tlsListener != nil {
				startListener(func() {
					logger.Info().Str("addr", tlsAddr).Msg("TLS REST API server starting")
					if tlsServeErr := tlsServer.Serve(tlsListener); tlsServeErr != nil && tlsServeErr != http.ErrServerClosed {
						reportServeError("MCP TLS", tlsServeErr)
					}
				})
			}
		}
	}

	// Dedicated v11 federation mTLS listener — a SEPARATE port and router from
	// the local API: RequireAnyClientCert + per-agreement pinned-CA
	// verification at handshake, chain-qualified signed requests per call. The
	// local REST/TLS listeners above keep NoClientCert semantics untouched.
	var fedServer *http.Server
	var fedP2PServer *http.Server
	if cfg.Federation.Enabled && fedMgr != nil {
		if !tlsca.CertsExist(certsDir) {
			logger.Warn().Msg("federation listener disabled: no TLS certificates")
		} else if fedTLS, fedTLSErr := fedMgr.ServerTLSConfig(); fedTLSErr != nil {
			logger.Warn().Err(fedTLSErr).Msg("federation listener disabled: TLS config failed")
		} else {
			fedAddr := dashboard.FederationAddr
			fedServer = &http.Server{
				Addr:         fedAddr,
				Handler:      fedMgr.Router(),
				TLSConfig:    fedTLS,
				ReadTimeout:  15 * time.Second,
				WriteTimeout: federationListenerWriteTimeout,
				IdleTimeout:  60 * time.Second,
			}
			plainFedListener, listenErr := (&net.ListenConfig{}).Listen(ctx, "tcp", fedAddr)
			appendStartupErr("federation TLS", listenErr)
			var fedListener net.Listener
			if listenErr == nil {
				fedListener = tls.NewListener(plainFedListener, fedTLS)
			}
			// Periodic reaper for expired join sessions / guest drafts / rate-limit
			// map (seed hygiene + staged-CA rollback + unbounded-growth guard).
			trackDone(fedMgr.StartMaintenance(ctx))
			// Domain-sync drainer + commit-tail watcher are wired above and share
			// the same complete federation.enabled lifecycle as this listener.
			if fedListener != nil {
				startListener(func() {
					logger.Info().Str("addr", fedAddr).Str("chain_id", cfg.ChainID).Msg("federation mTLS listener starting")
					if fedServeErr := fedServer.Serve(fedListener); fedServeErr != nil && fedServeErr != http.ErrServerClosed {
						reportServeError("federation TLS", fedServeErr)
					}
				})
			}
			if fedP2P != nil {
				fedP2PServer = &http.Server{
					Handler:      fedMgr.Router(),
					TLSConfig:    fedTLS.Clone(),
					ReadTimeout:  15 * time.Second,
					WriteTimeout: federationListenerWriteTimeout,
					IdleTimeout:  60 * time.Second,
				}
				startListener(func() {
					logger.Info().
						Str("peer_id", fedP2P.Host().ID().String()).
						Str("protocol", string(sagep2p.FederationProtocol)).
						Msg("federation mTLS listener starting over libp2p")
					if serveErr := fedP2PServer.ServeTLS(fedP2P.Listener(), "", ""); serveErr != nil && serveErr != http.ErrServerClosed && !errors.Is(serveErr, net.ErrClosed) {
						reportServeError("federation P2P", serveErr)
					}
				})
			}
		}
	} else if cfg.Federation.Enabled {
		logger.Warn().Msg("federation.enabled set but transport unavailable (missing agent key or chain_id)")
	}

	if restListener != nil {
		startListener(func() {
			if nativeControl != nil {
				_ = nativeControl.SetState(shellcontrol.StateReady)
			}
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

			if err := httpServer.Serve(restListener); err != nil && err != http.ErrServerClosed {
				reportServeError("REST", err)
			}
		})
	}

	// Start the per-node memory auto-voter: one node, one vote, signed with the
	// node's OWN consensus validator key (priv_validator_key.json). No validator-set
	// replacement — the genesis/quorum validator set already keys each node by this
	// identity, so the node's votes count toward the same 2/3 quorum the chain tallies.
	selfKey := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger)

	// Retired direct-state startup repairs are intentionally disabled. They changed
	// AppHash-covered validator/memory/vote keys outside a committed transaction and
	// could also rewrite SQLite status. Historical anomalies now remain unchanged
	// until a governed, replay-safe repair is available.

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
		startWorker(func() {
			voter.Run(ctx, app, sqliteStore, voter.Config{Key: selfKey, CometRPC: cometRPC, PollInterval: pollInterval, Health: health}, logger)
		})
	case cfg.Voter.Required:
		// Normally unreachable — the pre-serve gate before StartChain already refused
		// to boot — but a key that rots between the gate and here must still honor the
		// runs-or-exits contract.
		// Listeners and workers are already live here. Record the late failure
		// and fall through to the one coordinated cleanup path below; returning
		// directly would close stores underneath those goroutines.
		startupErr = fmt.Errorf("voter.required=true but no usable consensus key — memory auto-voter cannot start")
	default:
		logger.Warn().Msg("no consensus key — memory auto-voter disabled")
	}
	if cfg.Quorum.Enabled {
		logger.Info().Msg("quorum mode — P2P consensus active, blocks validated by both nodes")
	}

	if startupErr == nil {
		// Auto-import pending chat history (from setup wizard)
		startWorker(func() { autoImport(ctx, cfg, embedProvider, logger) })

		// Pipeline retention windows (E8c). The sweep runs on the 5-minute ticker
		// below; a one-shot boot sweep runs first so a node that was offline reclaims
		// stale rows immediately instead of waiting a full interval.
		const (
			pipeSweepInterval   = 5 * time.Minute
			pipeRetentionWindow = 24 * time.Hour // terminal metadata retained this long after terminal/read transition
			pipeStalenessWindow = 48 * time.Hour // non-terminal rows force-expired past this age
		)
		// Boot one-shot pipeline reconciliation — mirror the ResolveChallengedMemories
		// boot sweep so nothing accumulates while the node was down.
		{
			_, _ = sqliteStore.ExpirePipelines(ctx)
			_, _ = sqliteStore.ExpireStalePipelines(ctx, time.Now().Add(-pipeStalenessWindow))
			if purged, _ := sqliteStore.PurgePipelines(ctx, time.Now().Add(-pipeRetentionWindow)); purged > 0 {
				logger.Debug().Int("purged", purged).Msg("pipeline boot sweep")
			}
		}
		// OAuth bearer/code lifecycle recovery is also a boot-time invariant:
		// pending cleanup rows must never wait for the first five-minute ticker.
		_, _ = sqliteStore.PurgeExpiredAuthCodes(ctx)
		if revoked, _ := sqliteStore.PurgeMCPTokenCleanup(ctx); revoked > 0 {
			logger.Warn().Int64("revoked", revoked).Msg("boot reconciled fail-closed OAuth token cleanup")
		}

		// Pipeline TTL cleanup — expire and purge stale pipeline messages every 5 minutes
		startWorker(func() {
			ticker := time.NewTicker(pipeSweepInterval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					expired, _ := sqliteStore.ExpirePipelines(ctx)
					stale, _ := sqliteStore.ExpireStalePipelines(ctx, time.Now().Add(-pipeStalenessWindow))
					purged, _ := sqliteStore.PurgePipelines(ctx, time.Now().Add(-pipeRetentionWindow))
					if expired > 0 || stale > 0 || purged > 0 {
						logger.Debug().Int("expired", expired).Int("stale", stale).Int("purged", purged).Msg("pipeline cleanup")
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
					if revoked, _ := sqliteStore.PurgeMCPTokenCleanup(ctx); revoked > 0 {
						logger.Warn().Int64("revoked", revoked).Msg("fail-closed OAuth token cleanup reconciled")
					}
					if removed, _ := sqliteStore.PurgeOldOAuthClients(ctx, 90*24*time.Hour); removed > 0 {
						logger.Debug().Int64("removed", removed).Msg("oauth clients purged")
					}
				}
			}
		})
	}

	// A replacement binary keeps its rollback copy until this exact process has
	// answered health with its own boot ID and expected version. A failed proof
	// exits without downgrading the executable: an older binary may be unable to
	// read chain state that the replacement has already advanced.
	if startupErr == nil {
		if execPath, pathErr := os.Executable(); pathErr == nil {
			if resolved, resolveErr := filepath.EvalSymlinks(execPath); resolveErr == nil {
				execPath = resolved
			}
			pendingVersion := web.PendingUpdateVersion(execPath)
			if pendingVersion == "" {
				execPath = ""
			} else if !pendingUpdateMatchesRunningVersion(pendingVersion, version) {
				// A legacy one-line marker can survive a crash before executable
				// activation, but it has no captured rollback version with which to
				// prove that state. Preserve the evidence without turning a runnable
				// old release into a readiness-failure restart loop.
				logger.Warn().
					Str("pending_version", pendingVersion).
					Str("running_version", version).
					Msg("pending update does not match running binary — preserving recovery state without confirmation")
				execPath = ""
			}
			if execPath != "" {
				readyCtx, cancelReady := context.WithTimeout(ctx, 15*time.Second)
				startupErr = confirmPendingUpdateAfterReady(readyCtx, restBaseURL(cfg.RESTAddr), dashboard.BootID, execPath)
				cancelReady()
				if startupErr == nil {
					select {
					case listenerErr := <-serveErrors:
						startupErr = listenerErr
					default:
					}
				}
				if startupErr == nil {
					if confirmErr := web.ConfirmPendingUpdate(execPath); confirmErr != nil {
						// The replacement already proved healthy; a failed marker
						// cleanup (transient fsync/unlink error) must not roll a
						// good update back. Leave the marker — the next boot
						// re-proves and re-confirms it.
						logger.Warn().Err(confirmErr).Msg("update confirmed but rollback-marker cleanup failed — will reconfirm on next boot")
					}
				}
				if startupErr != nil {
					logger.Error().Err(startupErr).Msg("updated process failed readiness proof — pending update remains for operator recovery")
				}
			}
		}
	}

	// Wait for shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	restarting := false
	if startupErr == nil {
		select {
		case sig := <-quit:
			logger.Info().Str("signal", sig.String()).Msg("shutting down")
		case preparedRestart = <-restartRequested:
			restartFenceRelease = preparedRestart.release
			preparedRestart.commit()
			restarting = true
			logger.Info().Msg("coordinated restart requested — draining node")
		case serveErr := <-serveErrors:
			startupErr = serveErr
		}
	}
	signal.Stop(quit)
	if nativeControl != nil {
		_ = nativeControl.SetState(shellcontrol.StateDraining)
	}
	// Stop MCP admission and cancel/join active tool calls first. A slow quorum
	// call must not outlive the stores it is using or consume the entire listener
	// shutdown budget.
	if mcpHTTPTransport != nil {
		// Never close stores under a live MCP tool call. Close has already canceled
		// every admitted handler; joining without a short timeout favors a clean
		// restart over an unsafe partial teardown.
		_ = mcpHTTPTransport.Close(context.Background())
	}
	// Disconnect dashboard event streams before Shutdown. The dashboard keeps
	// an EventSource open for the whole tab lifetime, so without an explicit
	// drain httpServer.Shutdown blocks its entire budget on that stream and a
	// coordinated restart aborts whenever a tab is open.
	dashboard.SSE.CloseAll()
	if fedMgr != nil {
		fedMgr.StopSyncDrainer()
	}

	// All listeners get the same full wall-clock budget by draining in parallel;
	// every result is checked before a restart is allowed to exec.
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	servers := []struct {
		name   string
		server *http.Server
	}{
		{name: "REST", server: httpServer},
		{name: "MCP TLS", server: tlsServer},
		{name: "federation TLS", server: fedServer},
		{name: "federation P2P", server: fedP2PServer},
	}
	var shutdownWG sync.WaitGroup
	var shutdownMu sync.Mutex
	var shutdownErrs []error
	for _, item := range servers {
		if item.server == nil {
			continue
		}
		shutdownWG.Add(1)
		go func(name string, server *http.Server) {
			defer shutdownWG.Done()
			if err := server.Shutdown(shutdownCtx); err != nil && !errors.Is(err, http.ErrServerClosed) {
				// Shutdown timed out waiting for an active handler (e.g. a
				// long-lived stream that ignored the drain). Force-close so no
				// request outlives the stores. A drained-then-force-closed
				// listener is still a clean stop; only a failed force close may
				// veto the restart, else an open browser tab aborts the exec and
				// strands the node (or rolls back a just-installed update).
				logger.Warn().Err(err).Str("listener", name).Msg("graceful drain timed out — force closing")
				if closeErr := server.Close(); closeErr != nil && !errors.Is(closeErr, http.ErrServerClosed) {
					shutdownMu.Lock()
					shutdownErrs = append(shutdownErrs, fmt.Errorf("%s force close: %w", name, closeErr))
					shutdownMu.Unlock()
				}
			}
		}(item.name, item.server)
	}
	shutdownWG.Wait()
	if fedP2P != nil {
		if err := fedP2P.Close(); err != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("federation P2P transport: %w", err))
		}
	}
	listenerWG.Wait()
	// The first select can race a signal/serve error with a queued update
	// restart. All HTTP handlers have now returned, so the channel is stable:
	// adopt that request before teardown. Release duplicate fenced requests.
	adoptQueuedRestartRequests(restartRequested, &preparedRestart, &restarting)
	if restartFenceRelease == nil && preparedRestart.release != nil {
		restartFenceRelease = preparedRestart.release
	}
	if restarting && preparedRestart.commit != nil {
		preparedRestart.commit()
	}
	// With every HTTP admission point closed and active handler gone, no new
	// dashboard job can Add to the worker group while it is being joined.
	stopWorkers()
	// Snapshot workers hold the live Badger handle. Cancel cadence work and
	// give its context-aware backup stream a short bounded drain before any
	// shutdown path closes the store underneath it.
	var quiesceErr error
	if snapshotScheduler != nil {
		quiesceCtx, cancelQuiesce := context.WithTimeout(context.Background(), 15*time.Second)
		quiesceErr = snapshotScheduler.Quiesce(quiesceCtx)
		cancelQuiesce()
		if quiesceErr != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("quiesce snapshot scheduler: %w", quiesceErr))
			// A timed-out reversible preparation restores scheduler admission by
			// design. Ordinary shutdown is no longer reversible, so close cadence
			// permanently before joining the existing worker. Stores must never be
			// closed beneath a slow Badger backup; preserving data is more important
			// than forcing a fast process exit.
			snapshotScheduler.Close()
			snapshotScheduler.WaitIdle()
		}
	}
	versionTransitionRestart := restarting && restartFenceRelease != nil
	if versionTransitionRestart {
		// Every external transaction admission point and background producer is
		// now drained. Release the preflight fence so any ABCI Commit that already
		// entered can finish, then stop Comet and wait for that final durable
		// commit. The second snapshot below is the authoritative zero-loss gate.
		restartFenceRelease()
		restartFenceRelease = nil
		stopErr := nodeCtrl.StopChain()
		if stopErr != nil {
			shutdownErrs = append(shutdownErrs, fmt.Errorf("stop CometBFT before restart snapshot: %w", stopErr))
		} else if quiesceErr == nil {
			height, appHash, release := app.AcquireSnapshotStateFence()
			if height <= 0 || len(appHash) == 0 {
				shutdownErrs = append(shutdownErrs, errors.New("final restart snapshot has no committed application state"))
			} else if _, snapshotErr := snapshotScheduler.TakeVerified(
				context.Background(),
				height,
				appHash,
				"final-pre-restart",
				func(manifest *snapshot.Manifest) error {
					if manifest == nil || manifest.Height != height || !bytes.Equal(manifest.AppHash, appHash) {
						return errors.New("final restart snapshot does not match stopped application state")
					}
					return nil
				},
			); snapshotErr != nil {
				shutdownErrs = append(shutdownErrs, fmt.Errorf("final restart snapshot: %w", snapshotErr))
			}
			release()
		}
	}
	if err := errors.Join(shutdownErrs...); err != nil {
		if versionTransitionRestart {
			managedRollbackSucceeded := false
			if execPath, pathErr := os.Executable(); pathErr == nil {
				if resolved, resolveErr := filepath.EvalSymlinks(execPath); resolveErr == nil {
					execPath = resolved
				}
				rolledBack, rollbackErr := web.RollbackPendingUpdate(execPath)
				if rolledBack && rollbackErr == nil {
					logger.Error().Err(err).Msg("final restart safety gate failed — restored previous binary and restarting it")
					managedRollbackSucceeded = true
				}
				if rollbackErr != nil {
					err = errors.Join(err, fmt.Errorf("restore previous binary after final gate failure: %w", rollbackErr))
				}
			}
			if managedRollbackSucceeded {
				preservePIDForExec = true
				return errCoordinatedRestart
			}
			// Finder replacement has no managed update marker or old app bundle.
			// Re-verify the exact executable pinned before drain and hand it to the
			// outer exec loop. The replacement app remains untouched for operator
			// recovery, while the known running binary resumes against unchanged
			// chain state instead of leaving the node offline.
			if preparedRestart.fallbackExec != "" && snapshotScheduler != nil {
				verifiedFallback, verifyErr := verifyPreparedRestartFallback(snapshotScheduler, preparedRestart)
				if verifyErr == nil {
					restartExecOverride = verifiedFallback
					preservePIDForExec = true
					logger.Error().Err(err).Str("recovery_binary", verifiedFallback).
						Msg("final restart safety gate failed — restarting exact pre-drain executable")
					return errCoordinatedRestart
				}
				err = errors.Join(err, verifyErr)
			}
		}
		return fmt.Errorf("clean shutdown failed: %w", err)
	}
	if startupErr != nil {
		return startupErr
	}
	if restarting {
		preservePIDForExec = true
		return errCoordinatedRestart
	}
	return nil
}

func pendingUpdateMatchesRunningVersion(pending, running string) bool {
	pending = strings.TrimPrefix(strings.TrimSpace(pending), "v")
	running = strings.TrimPrefix(strings.TrimSpace(running), "v")
	return pending != "" && running != "" && pending != "dev" && running != "dev" && pending == running
}

func stateSyncRecoveryActionName(action statesync.RecoveryAction) string {
	switch action {
	case statesync.RecoveryDiscardPrepared:
		return "discard_prepared"
	case statesync.RecoveryRestoreQuarantine:
		return "restore_quarantine"
	case statesync.RecoveryKeepActivated:
		return "keep_activated"
	default:
		return "unknown"
	}
}

func confirmPendingUpdateAfterReady(ctx context.Context, baseURL, bootID, execPath string) error {
	expectedVersion := web.PendingUpdateVersion(execPath)
	if expectedVersion == "" {
		return nil
	}
	client := &http.Client{Timeout: time.Second}
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(baseURL, "/")+"/v1/dashboard/health", nil)
		if err == nil {
			if resp, doErr := client.Do(req); doErr == nil {
				var health struct {
					SAGE    string `json:"sage"`
					Version string `json:"version"`
					BootID  string `json:"boot_id"`
				}
				decodeErr := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&health)
				_ = resp.Body.Close()
				if resp.StatusCode == http.StatusOK && decodeErr == nil && health.SAGE == "running" &&
					health.BootID == bootID && strings.TrimPrefix(health.Version, "v") == strings.TrimPrefix(expectedVersion, "v") {
					return nil
				}
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("updated node did not prove boot %s at version %s: %w", bootID, expectedVersion, ctx.Err())
		case <-ticker.C:
		}
	}
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

// restBaseURL builds a dialable HTTP base URL from a REST listen address.
// Wildcard hosts are valid bind targets but invalid client destinations, so
// local hooks and MCP configs must use loopback for those cases.
func restBaseURL(addr string) string {
	if host, port, err := net.SplitHostPort(addr); err == nil {
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "localhost"
		}
		return "http://" + net.JoinHostPort(host, port)
	}
	return "http://" + addr
}

// oauthLocalApprovalBaseURL is deliberately loopback even when the REST API
// listener is configured on a wildcard/LAN address. Cloudflare may expose
// discovery, token exchange, registration, and /oauth/authorize, but browser
// approval always crosses onto this same-machine origin.
func oauthLocalApprovalBaseURL(addr string) string {
	if _, port, err := net.SplitHostPort(addr); err == nil {
		return "http://" + net.JoinHostPort("127.0.0.1", port)
	}
	return "http://127.0.0.1:8080"
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

func genesisInitialAdminAppStateForKey(keyPath string) json.RawMessage {
	admin := ensureOperatorAdminIDForKey(keyPath)
	if admin == "" {
		return nil
	}
	// admin is canonical lowercase 64-hex, safe to embed verbatim.
	return json.RawMessage(`{"sage":{"initial_admin":"` + admin + `"}}`)
}

// genesisAppStateForVendoredAgent builds the app-v23 first-party bootstrap
// manifest for a brand-new personal chain. Both locally-held principals sign
// the exact chain-bound manifest. The companion key may be generated at the
// configured path on first launch, allowing a bundled application to consume
// the same stable identity without any manual CEREBRUM ceremony.
func genesisAppStateForVendoredAgent(
	chainID, rootKeyPath string,
	bootstrap *VendoredAgentBootstrapConfig,
	validatorID string,
	validatorPower int64,
) (json.RawMessage, error) {
	if bootstrap == nil {
		// Preserve the legacy issue-#52 behavior exactly: an unavailable
		// operator key omits app_state. A configured first-party bootstrap is
		// stricter and fails startup below.
		return genesisInitialAdminAppStateForKey(rootKeyPath), nil
	}

	rootPath := filepath.Clean(expandTilde(rootKeyPath))
	agentPath := filepath.Clean(expandTilde(bootstrap.AgentKeyFile))
	if rootPath == agentPath {
		return nil, errors.New("vendored agent key must be distinct from the CEREBRUM root key")
	}
	if err := os.MkdirAll(filepath.Dir(rootPath), 0700); err != nil {
		return nil, fmt.Errorf("create CEREBRUM root key directory: %w", err)
	}
	rootKey, err := loadOrGenerateKey(rootPath)
	if err != nil {
		return nil, fmt.Errorf("load CEREBRUM root key: %w", err)
	}
	if mkdirErr := os.MkdirAll(filepath.Dir(agentPath), 0700); mkdirErr != nil {
		return nil, fmt.Errorf("create vendored agent key directory: %w", mkdirErr)
	}
	agentKey, err := loadOrGenerateKey(agentPath)
	if err != nil {
		return nil, fmt.Errorf("load vendored agent key: %w", err)
	}
	rootPublic, rootOK := rootKey.Public().(ed25519.PublicKey)
	agentPublic, agentOK := agentKey.Public().(ed25519.PublicKey)
	if !rootOK || !agentOK {
		return nil, errors.New("vendored bootstrap requires Ed25519 keys")
	}

	manifest := sageabci.AppV23GenesisManifest{
		Version:   sageabci.AppV23GenesisManifestVersion,
		RootID:    hex.EncodeToString(rootPublic),
		AgentID:   hex.EncodeToString(agentPublic),
		Profile:   store.AppV23ProfileCompanion,
		Clearance: bootstrap.Clearance,
		Capabilities: uint32(store.AgentCapabilityReadAllDomains |
			store.AgentCapabilityDenySharedDomainWrite |
			store.AgentCapabilityDenyDomainClaim |
			store.AgentCapabilityDenyForeignDomainWrite),
		HomeDomain:  strings.TrimSpace(bootstrap.HomeDomain),
		ValidatorID: validatorID, ValidatorPower: validatorPower,
	}
	signBytes := sageabci.AppV23GenesisManifestSignBytes(chainID, manifest)
	manifest.RootSignature = hex.EncodeToString(ed25519.Sign(rootKey, signBytes))
	manifest.AgentSignature = hex.EncodeToString(ed25519.Sign(agentKey, signBytes))
	if _, verifyErr := sageabci.VerifyAppV23GenesisManifest(chainID, manifest); verifyErr != nil {
		return nil, fmt.Errorf("verify vendored bootstrap manifest: %w", verifyErr)
	}
	appState, err := json.Marshal(struct {
		Sage struct {
			InitialAdmin    string                         `json:"initial_admin"`
			AppV23Bootstrap sageabci.AppV23GenesisManifest `json:"app_v23_bootstrap"`
		} `json:"sage"`
	}{
		Sage: struct {
			InitialAdmin    string                         `json:"initial_admin"`
			AppV23Bootstrap sageabci.AppV23GenesisManifest `json:"app_v23_bootstrap"`
		}{
			InitialAdmin:    manifest.RootID,
			AppV23Bootstrap: manifest,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("encode vendored bootstrap app state: %w", err)
	}
	return appState, nil
}

// ensureOperatorAdminID returns the lowercase-hex ed25519 public key of the node
// operator's ~/.sage/agent.key, GENERATING the key (32-byte seed form, via the
// canonical loadOrGenerateKey) if it does not yet exist — `serve` does not
// otherwise create it. Empty string on any failure.
func ensureOperatorAdminID() string {
	return ensureOperatorAdminIDForKey(filepath.Join(SageHome(), "agent.key"))
}

func ensureOperatorAdminIDForKey(keyPath string) string {
	priv, err := loadOrGenerateKey(filepath.Clean(expandTilde(keyPath)))
	if err != nil {
		return ""
	}
	pub, ok := priv.Public().(ed25519.PublicKey)
	if !ok {
		return ""
	}
	return hex.EncodeToString(pub)
}

// ensureGenesisSeed creates a seeded genesis only for a caller-proven fresh
// origin. Existing genesis is never rewritten here; legacy automatic healing
// could change authority outside a committed governance transition.
func ensureGenesisSeed(cometHome string, logger zerolog.Logger) error {
	return ensureGenesisSeedWithKey(cometHome, filepath.Join(SageHome(), "agent.key"), logger)
}

func ensureGenesisSeedWithKey(cometHome, keyPath string, logger zerolog.Logger) error {
	return ensureGenesisSeedWithBootstrap(cometHome, keyPath, nil, logger)
}

func ensureGenesisSeedWithConfig(cometHome string, cfg *Config, logger zerolog.Logger) error {
	if cfg == nil {
		return errors.New("initialize genesis: config is required")
	}
	return ensureGenesisSeedWithBootstrap(cometHome, cfg.AgentKey, cfg.VendoredAgentBootstrap, logger)
}

func ensureGenesisSeedWithBootstrap(
	cometHome, keyPath string,
	bootstrap *VendoredAgentBootstrapConfig,
	logger zerolog.Logger,
) error {
	if err := initCometBFTConfigWithBootstrap(cometHome, keyPath, bootstrap); err != nil {
		return fmt.Errorf("init CometBFT: %w", err)
	}
	if bootstrap != nil {
		// Direct-v23 genesis is immutable and was either freshly created above or
		// read-only preflighted by runServe. The legacy height-0 healer must never
		// rewrite it into an issue-#52 lookalike.
		return nil
	}
	_ = logger
	return nil
}

func preflightGenesisOrigin(cometHome string, freshNodeOrigin bool) error {
	if freshNodeOrigin {
		return nil
	}
	genesisPath := filepath.Join(cometHome, "config", "genesis.json")
	if _, err := cmttypes.GenesisDocFromFile(genesisPath); err != nil {
		return fmt.Errorf(
			"initialized node genesis is missing or invalid; refusing to mint or rewrite "+
				"a replacement identity at %s: %w",
			genesisPath,
			err,
		)
	}
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
	healGenesisAdminIfResetWithKey(home, filepath.Join(SageHome(), "agent.key"), logger)
}

func healGenesisAdminIfResetWithKey(home, keyPath string, logger zerolog.Logger) {
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
	admin := ensureOperatorAdminIDForKey(keyPath)
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
	return initCometBFTConfigWithKey(home, filepath.Join(SageHome(), "agent.key"))
}

func initCometBFTConfigWithKey(home, keyPath string) error {
	return initCometBFTConfigWithBootstrap(home, keyPath, nil)
}

func preflightExistingVendoredGenesis(cometHome string, cfg *Config) error {
	if cfg == nil || cfg.VendoredAgentBootstrap == nil {
		return nil
	}
	genesisPath := filepath.Join(cometHome, "config", "genesis.json")
	if _, statErr := os.Stat(genesisPath); errors.Is(statErr, os.ErrNotExist) {
		dataDir := strings.TrimSpace(cfg.DataDir)
		if dataDir == "" {
			dataDir = filepath.Dir(cometHome)
		}
		evidence, evidenceErr := persistedNodeIdentityEvidence(
			SageHome(),
			dataDir,
			cometHome,
		)
		if evidenceErr != nil {
			return fmt.Errorf("prove fresh direct app-v23 origin: %w", evidenceErr)
		}
		if evidence != "" {
			return fmt.Errorf(
				"direct app-v23 genesis is missing but %s survives; refusing to mint a replacement identity before recovery",
				evidence,
			)
		}
		// The generic transport helper permits an empty Badger directory because
		// runServe creates it immediately before first-launch key generation. This
		// earlier vendored preflight runs before that mutation, so even an empty
		// pre-existing Badger path is an ambiguous partial launch and must fail.
		if _, lstatErr := os.Lstat(filepath.Join(dataDir, "badger")); lstatErr == nil {
			return errors.New(
				"direct app-v23 genesis is missing but a Badger chain state path survives; refusing to mint a replacement identity before recovery",
			)
		} else if !errors.Is(lstatErr, os.ErrNotExist) {
			return fmt.Errorf("inspect fresh direct app-v23 Badger path: %w", lstatErr)
		}
		if preflightErr := preflightFreshVendoredKeys(cfg); preflightErr != nil {
			return preflightErr
		}
		return nil
	} else if statErr != nil {
		return fmt.Errorf("inspect existing genesis: %w", statErr)
	}
	genesis, genesisErr := cmttypes.GenesisDocFromFile(genesisPath)
	if genesisErr != nil {
		return fmt.Errorf("read existing genesis: %w", genesisErr)
	}
	if genesis.ConsensusParams == nil ||
		genesis.ConsensusParams.Version.App != sageabci.AppV23GenesisAppVersion {
		return errors.New("existing genesis is not a direct app-v23 origin")
	}
	if cfg.ChainID != "" && genesis.ChainID != cfg.ChainID {
		return fmt.Errorf(
			"existing direct app-v23 genesis chain_id %q does not match configured chain_id %q",
			genesis.ChainID,
			cfg.ChainID,
		)
	}
	if len(genesis.Validators) != 1 {
		return errors.New("direct app-v23 genesis requires exactly one validator")
	}
	validator := genesis.Validators[0]
	if _, ok := validator.PubKey.(cmtcryptoed.PubKey); !ok ||
		len(validator.PubKey.Bytes()) != ed25519.PublicKeySize ||
		validator.Power <= 0 {
		return errors.New("direct app-v23 genesis validator is not a canonical positive-power Ed25519 validator")
	}
	var appState struct {
		Sage struct {
			InitialAdmin    string                          `json:"initial_admin"`
			AppV23Bootstrap *sageabci.AppV23GenesisManifest `json:"app_v23_bootstrap,omitempty"`
		} `json:"sage"`
	}
	if decodeErr := json.Unmarshal(genesis.AppState, &appState); decodeErr != nil ||
		appState.Sage.AppV23Bootstrap == nil {
		return errors.New("existing genesis lacks the direct app-v23 bootstrap manifest")
	}
	manifest := appState.Sage.AppV23Bootstrap
	if _, manifestErr := sageabci.VerifyAppV23GenesisManifest(genesis.ChainID, *manifest); manifestErr != nil {
		return fmt.Errorf("verify existing direct app-v23 manifest: %w", manifestErr)
	}
	validatorID := hex.EncodeToString(validator.PubKey.Bytes())
	if manifest.ValidatorID != validatorID ||
		manifest.ValidatorPower != validator.Power {
		return errors.New("existing direct app-v23 validator does not match its signed manifest")
	}
	localValidatorID, available, validatorErr := localVendoredValidatorID(cometHome)
	if validatorErr != nil {
		return fmt.Errorf("verify local direct app-v23 validator key: %w", validatorErr)
	}
	if !available {
		return errors.New("local validator key is missing for existing direct app-v23 genesis")
	}
	if localValidatorID != manifest.ValidatorID {
		return errors.New("local validator key does not match existing direct app-v23 genesis")
	}
	if stateErr := validateVendoredValidatorState(
		filepath.Join(cometHome, "data", "priv_validator_state.json"),
		validator.PubKey.(cmtcryptoed.PubKey),
		genesis.ChainID,
	); stateErr != nil {
		return fmt.Errorf("verify local direct app-v23 validator signing state: %w", stateErr)
	}
	if nodeKeyErr := validateVendoredNodeKey(
		filepath.Join(cometHome, "config", "node_key.json"),
	); nodeKeyErr != nil {
		return fmt.Errorf("verify local direct app-v23 peer identity: %w", nodeKeyErr)
	}
	if appState.Sage.InitialAdmin != manifest.RootID {
		return errors.New("existing direct app-v23 initial Root does not match its manifest")
	}
	transportKey, transportErr := readFederationTransportKey(cfg.AgentKey)
	if transportErr != nil {
		return fmt.Errorf("read immutable direct app-v23 Root/transport key: %w", transportErr)
	}
	if appV23AgentIDForKey(transportKey) != manifest.RootID {
		return errors.New("configured stable transport key does not match immutable direct app-v23 genesis Root")
	}
	agentKey, agentOK := parseKeyFile(cfg.VendoredAgentBootstrap.AgentKeyFile)
	if !agentOK || appV23AgentIDForKey(agentKey) != manifest.AgentID {
		return errors.New("configured first-party agent key does not match existing direct app-v23 genesis")
	}
	if manifest.Profile != store.AppV23ProfileCompanion ||
		manifest.Capabilities != 15 ||
		manifest.HomeDomain != cfg.VendoredAgentBootstrap.HomeDomain ||
		manifest.Clearance != cfg.VendoredAgentBootstrap.Clearance {
		return errors.New("configured first-party policy does not match existing direct app-v23 genesis")
	}
	if cfg.ChainID == "" {
		// genesis.json is authoritative once every signed identity, validator,
		// local key, and policy binding above has passed. Keep this recovery
		// in-memory; the normal later reconcile flow owns config.yaml persistence.
		cfg.ChainID = genesis.ChainID
	}
	return nil
}

func preflightFreshVendoredKeys(cfg *Config) error {
	if cfg == nil || cfg.VendoredAgentBootstrap == nil {
		return nil
	}
	readOptional := func(path, label string) (ed25519.PrivateKey, bool, error) {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return nil, false, nil
		} else if err != nil {
			return nil, false, fmt.Errorf("inspect pre-provisioned %s: %w", label, err)
		}
		data, err := os.ReadFile(path) //nolint:gosec // configured local identity path
		if err != nil {
			return nil, false, fmt.Errorf("read pre-provisioned %s: %w", label, err)
		}
		key, err := decodeEd25519PrivateKey(data)
		if err != nil {
			return nil, false, fmt.Errorf("validate pre-provisioned %s: %w", label, err)
		}
		return key, true, nil
	}
	rootKey, rootPresent, err := readOptional(cfg.AgentKey, "CEREBRUM Root/transport key")
	if err != nil {
		return err
	}
	agentKey, agentPresent, err := readOptional(
		cfg.VendoredAgentBootstrap.AgentKeyFile,
		"first-party agent key",
	)
	if err != nil {
		return err
	}
	if rootPresent && agentPresent &&
		appV23AgentIDForKey(rootKey) == appV23AgentIDForKey(agentKey) {
		return errors.New("pre-provisioned first-party agent key must be distinct from the CEREBRUM Root/transport key")
	}
	return nil
}

// localVendoredValidatorID reads the configured CometBFT private-validator
// identity without invoking CometBFT's panic/exit-on-malformed loader and
// without creating or rewriting any key material. The private key is the
// authority: the redundant public key and address fields must agree with it.
func localVendoredValidatorID(cometHome string) (string, bool, error) {
	keyPath := filepath.Join(cometHome, "config", "priv_validator_key.json")
	keyJSON, err := os.ReadFile(keyPath) //nolint:gosec // fixed path under the configured Comet home
	if errors.Is(err, os.ErrNotExist) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	var key privval.FilePVKey
	if err := cmtjson.Unmarshal(keyJSON, &key); err != nil {
		return "", false, fmt.Errorf("decode private-validator key: %w", err)
	}
	if key.PrivKey == nil {
		return "", false, errors.New("private-validator key has no private key")
	}
	privateKey, ok := key.PrivKey.(cmtcryptoed.PrivKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return "", false, errors.New("private-validator key is not canonical Ed25519")
	}
	derivedPrivate := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if !bytes.Equal(derivedPrivate, privateKey) {
		return "", false, errors.New("private-validator private key public component does not match its seed")
	}
	derived := cmtcryptoed.PubKey(append([]byte(nil), derivedPrivate[ed25519.SeedSize:]...))
	if key.PubKey == nil ||
		!bytes.Equal(key.PubKey.Bytes(), derived.Bytes()) ||
		!bytes.Equal(key.Address, derived.Address()) {
		return "", false, errors.New("private-validator public identity does not match its private key")
	}
	return hex.EncodeToString(derived.Bytes()), true, nil
}

func validateVendoredValidatorState(
	path string,
	validatorKey cmtcryptoed.PubKey,
	chainID string,
) error {
	stateJSON, err := os.ReadFile(path) //nolint:gosec // fixed path under configured Comet home
	if err != nil {
		return err
	}
	if err := validateStrictJSONObjectKeys(
		stateJSON,
		[]string{"height", "round", "step"},
		[]string{"signature", "signbytes"},
	); err != nil {
		return fmt.Errorf("decode private-validator signing state: %w", err)
	}
	var state privval.FilePVLastSignState
	if err := cmtjson.Unmarshal(stateJSON, &state); err != nil {
		return fmt.Errorf("decode private-validator signing state: %w", err)
	}
	if state.Height < 0 || state.Round < 0 || state.Step < 0 || state.Step > 3 {
		return errors.New("private-validator signing state has invalid height, round, or step")
	}
	hasSignature := len(state.Signature) > 0
	hasSignBytes := len(state.SignBytes) > 0
	if hasSignature != hasSignBytes {
		return errors.New("private-validator signing state must contain both signature and signbytes or neither")
	}
	if state.Height == 0 {
		if state.Round != 0 || state.Step != 0 || hasSignature {
			return errors.New("height-zero private-validator signing state must be pristine")
		}
		return nil
	}
	if state.Step == 0 || !hasSignature {
		return errors.New("positive-height private-validator signing state lacks a signed consensus step")
	}
	if len(state.Signature) != ed25519.SignatureSize {
		return errors.New("private-validator signing state has an invalid Ed25519 signature length")
	}
	if len(validatorKey) != ed25519.PublicKeySize ||
		!validatorKey.VerifySignature(state.SignBytes, state.Signature) {
		return errors.New("private-validator signing state signature does not match the genesis validator")
	}
	if err := validateVendoredValidatorSignBytes(state, chainID); err != nil {
		return fmt.Errorf("private-validator signing state signbytes are not canonical: %w", err)
	}
	return nil
}

func validateVendoredValidatorSignBytes(
	state privval.FilePVLastSignState,
	chainID string,
) error {
	if chainID == "" {
		return errors.New("chain_id is empty")
	}
	switch state.Step {
	case 1:
		var proposal cmtproto.CanonicalProposal
		if err := protoio.UnmarshalDelimited(state.SignBytes, &proposal); err != nil {
			return fmt.Errorf("decode canonical proposal: %w", err)
		}
		if proposal.Type != cmtproto.ProposalType {
			return fmt.Errorf("proposal has signed-message type %d", proposal.Type)
		}
		if proposal.ChainID != chainID {
			return errors.New("proposal chain_id does not match genesis")
		}
		if proposal.Height != state.Height ||
			proposal.Round != int64(state.Round) {
			return errors.New("proposal height/round do not match signing state")
		}
		canonical, err := protoio.MarshalDelimited(&proposal)
		if err != nil {
			return fmt.Errorf("re-encode canonical proposal: %w", err)
		}
		if !bytes.Equal(canonical, state.SignBytes) {
			return errors.New("proposal encoding is non-canonical or contains unknown fields")
		}
	case 2, 3:
		var vote cmtproto.CanonicalVote
		if err := protoio.UnmarshalDelimited(state.SignBytes, &vote); err != nil {
			return fmt.Errorf("decode canonical vote: %w", err)
		}
		expectedType := cmtproto.PrevoteType
		if state.Step == 3 {
			expectedType = cmtproto.PrecommitType
		}
		if vote.Type != expectedType {
			return fmt.Errorf(
				"vote type %d does not match signing step %d",
				vote.Type,
				state.Step,
			)
		}
		if vote.ChainID != chainID {
			return errors.New("vote chain_id does not match genesis")
		}
		if vote.Height != state.Height ||
			vote.Round != int64(state.Round) {
			return errors.New("vote height/round do not match signing state")
		}
		canonical, err := protoio.MarshalDelimited(&vote)
		if err != nil {
			return fmt.Errorf("re-encode canonical vote: %w", err)
		}
		if !bytes.Equal(canonical, state.SignBytes) {
			return errors.New("vote encoding is non-canonical or contains unknown fields")
		}
	default:
		return fmt.Errorf("unsupported signing step %d", state.Step)
	}
	return nil
}

func validateVendoredNodeKey(path string) error {
	nodeKey, err := p2p.LoadNodeKey(path)
	if err != nil {
		return err
	}
	if nodeKey == nil || nodeKey.PrivKey == nil {
		return errors.New("CometBFT peer identity has no private key")
	}
	privateKey, ok := nodeKey.PrivKey.(cmtcryptoed.PrivKey)
	if !ok || len(privateKey) != ed25519.PrivateKeySize {
		return errors.New("CometBFT peer identity is not canonical Ed25519")
	}
	derived := ed25519.NewKeyFromSeed(privateKey[:ed25519.SeedSize])
	if !bytes.Equal(derived, privateKey) {
		return errors.New("CometBFT peer identity public component does not match its seed")
	}
	return nil
}

func validateStrictJSONObjectKeys(data []byte, required, optional []string) error {
	allowed := make(map[string]bool, len(required)+len(optional))
	for _, key := range required {
		allowed[key] = true
	}
	for _, key := range optional {
		allowed[key] = false
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	start, startErr := decoder.Token()
	if startErr != nil {
		return startErr
	}
	if delimiter, ok := start.(json.Delim); !ok || delimiter != '{' {
		return errors.New("expected a JSON object")
	}
	seen := make(map[string]struct{}, len(allowed))
	for decoder.More() {
		token, tokenErr := decoder.Token()
		if tokenErr != nil {
			return tokenErr
		}
		key, ok := token.(string)
		if !ok {
			return errors.New("JSON object key is not a string")
		}
		if _, duplicate := seen[key]; duplicate {
			return fmt.Errorf("duplicate field %q", key)
		}
		if _, known := allowed[key]; !known {
			return fmt.Errorf("unknown field %q", key)
		}
		seen[key] = struct{}{}
		var value json.RawMessage
		if decodeErr := decoder.Decode(&value); decodeErr != nil {
			return decodeErr
		}
	}
	end, endErr := decoder.Token()
	if endErr != nil {
		return endErr
	}
	if delimiter, ok := end.(json.Delim); !ok || delimiter != '}' {
		return errors.New("unterminated JSON object")
	}
	if _, trailingErr := decoder.Token(); !errors.Is(trailingErr, io.EOF) {
		if trailingErr == nil {
			return errors.New("JSON object has trailing data")
		}
		return trailingErr
	}
	for _, key := range required {
		if _, ok := seen[key]; !ok {
			return fmt.Errorf("missing required field %q", key)
		}
	}
	return nil
}

func preflightVendoredStartup(cometHome string, cfg *Config) error {
	if err := preflightExistingVendoredGenesis(cometHome, cfg); err != nil {
		return err
	}
	if _, err := os.Stat(pendingJoinPath()); err == nil {
		return errors.New("configured first-party app-v23 node cannot apply a pending quorum join")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect pending quorum join: %w", err)
	}
	return nil
}

func initCometBFTConfigWithBootstrap(
	home, keyPath string,
	bootstrap *VendoredAgentBootstrapConfig,
) error {
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
	validatorID := hex.EncodeToString(pv.Key.PubKey.Bytes())
	appState, appStateErr := genesisAppStateForVendoredAgent(
		chainID, keyPath, bootstrap, validatorID, 10,
	)
	if appStateErr != nil {
		return fmt.Errorf("build genesis app_state: %w", appStateErr)
	}
	genDoc.AppState = appState
	if bootstrap != nil {
		// Declare the same protocol origin that the dual-signed InitChain
		// manifest activates. InitChain returns this value again so CometBFT's
		// pre-InitChain Info handshake and persisted consensus state converge
		// before block 1.
		genDoc.ConsensusParams.Version.App = sageabci.AppV23GenesisAppVersion
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

var embedderWatchdogInterval = 30 * time.Second

// startEmbedderWatchdog probes the embedding provider every 30s so /ready reflects
// real semantic-recall availability (a down provider degrades hybrid recall to
// keyword-only). It prefers the optional Pinger for a live check — an Ollama Ping hits
// /api/tags, an OpenAI-compatible Ping is a real embed request, so the 30s cadence
// keeps it cheap — and falls back to the sticky Ready() flag otherwise. Non-blocking:
// the initial probe runs inside the goroutine so boot never waits on the embedder.
func startEmbedderWatchdog(ctx context.Context, p embedding.Provider, health *metrics.HealthChecker, logger zerolog.Logger, onReady func()) <-chan struct{} {
	done := make(chan struct{})
	if p == nil || health == nil {
		close(done)
		return done
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
		if s.OK && onReady != nil {
			onReady()
		}
		return s
	}
	go func() {
		defer close(done)
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
		ticker := time.NewTicker(embedderWatchdogInterval)
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
	return done
}

type managedOllamaRuntime interface {
	Start(context.Context) (string, error)
	Probe(context.Context) bool
}

// superviseManagedOllama keeps the operator's persisted "managed" choice true
// in practice, not just in preferences. Previously boot made one Start attempt
// and the health watchdog only reported later crashes; semantic memory then
// remained offline until the user repaired it or restarted SAGE manually.
func superviseManagedOllama(ctx context.Context, runtime managedOllamaRuntime, retryInterval time.Duration, logger zerolog.Logger) {
	start := func() {
		startCtx, cancel := context.WithTimeout(ctx, 90*time.Second)
		defer cancel()
		ollamaURL, err := runtime.Start(startCtx)
		if err != nil {
			if ctx.Err() == nil {
				logger.Warn().Err(err).Msg("managed Ollama runtime unavailable; SAGE will retry automatically")
			}
			return
		}
		logger.Info().Str("url", ollamaURL).Msg("managed Ollama runtime ready")
	}

	start() // Start adopts an already-running sidecar, including across upgrades.
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !runtime.Probe(ctx) {
				start()
			}
		}
	}
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

// autoImport checks for pending-import.json from the setup wizard and seeds memories.
func autoImport(ctx context.Context, cfg *Config, embedProvider embedding.Provider, logger zerolog.Logger) {
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

	// Wait for the REST API to be ready, but never hold a coordinated restart.
	select {
	case <-time.After(5 * time.Second):
	case <-ctx.Done():
		return
	}

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
		if ctx.Err() != nil {
			return
		}
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
// Keyed bearer tokens sign underlying REST calls with their own encrypted
// Ed25519 identity. Legacy keyless tokens retain the local node operator key
// for backward compatibility; the lookup must never silently downgrade a
// keyed token when its signer cannot be loaded.
//
// CORS is liberal — MCP clients are first-class.
func mountMCPHTTPTransport(r chi.Router, sqliteStore *store.SQLiteStore, cfg *Config, logger zerolog.Logger) *mcp.HTTPTransport {
	// Use the node's own agent identity as the underlying signing key for
	// transport-originated REST calls. The actual *acting* agent is supplied
	// by the bearer-token middleware via request context — downstream tool
	// handlers can inspect it for audit / on-chain RBAC.
	keyData, readErr := os.ReadFile(cfg.AgentKey) //nolint:gosec // path from trusted config
	if readErr != nil {
		logger.Warn().Err(readErr).Msg("HTTP MCP: cannot load agent key — transport disabled")
		return nil
	}
	var transportKey ed25519.PrivateKey
	switch len(keyData) {
	case ed25519.SeedSize:
		transportKey = ed25519.NewKeyFromSeed(keyData)
	case ed25519.PrivateKeySize:
		transportKey = ed25519.PrivateKey(keyData)
	default:
		logger.Warn().Int("len", len(keyData)).Msg("HTTP MCP: invalid agent key size — transport disabled")
		return nil
	}
	transportAgentID := hex.EncodeToString(transportKey.Public().(ed25519.PublicKey))

	// Build the MCP Server reusing the existing stdio Server logic.
	// The base URL points at the local REST API so tool handlers funnel
	// through the same signed-REST pipeline as stdio.
	baseURL := restBaseURL(cfg.RESTAddr)
	mcpServer := mcp.NewServer(baseURL, transportKey)
	mcpServer.SetVersion(version)

	transport := mcp.NewHTTPTransport(mcpServer)

	bearerLookup := mcpBearerSignerLookup(sqliteStore, transportAgentID)

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
	return transport
}

// mcpBearerSignerLookup resolves the bearer to the only key permitted to sign
// its downstream REST calls. Legacy keyless tokens retain their pre-v23
// operator fallback only while the store's live app-version policy allows it.
// App-v23's explicit legacy-disabled sentinel is presented as revoked rather
// than an internal error so clients fail closed with a stable 401.
func mcpBearerSignerLookup(
	sqliteStore *store.SQLiteStore,
	transportAgentID string,
) middleware.MCPTokenLookupFn {
	return func(
		ctx context.Context,
		bearerPlaintext, tokenSHA256 string,
	) (string, ed25519.PrivateKey, error) {
		agentID, priv, err := sqliteStore.LookupMCPTokenSignerWithBearer(
			ctx, bearerPlaintext, tokenSHA256,
		)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return "", nil, err
			}
			if errors.Is(err, store.ErrTokenRevoked) ||
				errors.Is(err, store.ErrTokenLegacyDisabled) {
				return "", nil, middleware.ErrMCPTokenRevoked
			}
			// Fail closed on anything else (DB error, or a KEYED token whose vault
			// is locked — LookupMCPTokenSigner never degrades a keyed token to the
			// operator identity).
			return "", nil, err
		}
		if priv == nil {
			// Legacy keyless token: no per-token signer, so run as the node
			// operator signed with the transport key (pre-v11.8 behavior).
			return transportAgentID, nil, nil
		}
		// KEYED token: act as the token's own on-chain identity.
		return agentID, priv, nil
	}
}

// readNodeOperatorKey returns the hex-encoded ed25519 public key derived from
// the configured agent_key_file, accepting either the 32-byte seed or the 64-byte expanded
// private-key form (matches mountMCPHTTPTransport's existing parse). Empty
// string + nil error means the file isn't present — the caller treats that as
// "no operator compatibility identity; ordinary agent hooks are unaffected."
func readNodeOperatorKey(path string) (string, error) {
	path = filepath.Clean(expandTilde(path))
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
