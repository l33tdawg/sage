package main

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	abciserver "github.com/cometbft/cometbft/abci/server"
	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/api/rest"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/voter"
)

// Set via ldflags at build time.
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	// Parse flags
	cometHome := flag.String("home", os.Getenv("COMETBFT_HOME"), "CometBFT home directory")
	postgresURL := flag.String("postgres-url", os.Getenv("POSTGRES_URL"), "PostgreSQL connection URL")
	restAddr := flag.String("rest-addr", envOrDefault("REST_ADDR", ":8080"), "REST API listen address")
	metricsAddr := flag.String("metrics-addr", envOrDefault("METRICS_ADDR", ":2112"), "Prometheus metrics listen address")
	badgerPath := flag.String("badger-path", envOrDefault("BADGER_PATH", "data/sage.db"), "BadgerDB data path")
	abciAddr := flag.String("abci-addr", envOrDefault("ABCI_ADDR", ""), "ABCI server listen address (e.g. tcp://0.0.0.0:26658). If set, runs as standalone ABCI server; otherwise embeds CometBFT in-process")
	cometRPC := flag.String("comet-rpc", envOrDefault("COMET_RPC", "http://127.0.0.1:26657"), "CometBFT RPC endpoint for REST API tx broadcast")
	validatorKeyFile := flag.String("validator-key-file", os.Getenv("VALIDATOR_KEY_FILE"), "priv_validator_key.json for the memory auto-voter and REST governance gateway in socket mode (in-process mode uses the key under --home). If unset in socket mode, no voter runs and governance mutations return 503")
	governanceOperatorID := flag.String("governance-operator-id", os.Getenv("SAGE_GOVERNANCE_OPERATOR_ID"), "hex Ed25519 identity allowed to authorize this validator's REST governance mutations (empty disables them)")
	requireVoter := flag.Bool("require-voter", envBoolOrDefault("VOTER_REQUIRED", false), "Exit non-zero at startup if the memory auto-voter cannot start (missing/unreadable/invalid validator key) instead of serving without a voter")
	tlsCert := flag.String("tls-cert", os.Getenv("TLS_CERT"), "TLS certificate file for REST API (PEM)")
	tlsKey := flag.String("tls-key", os.Getenv("TLS_KEY"), "TLS private key file for REST API (PEM)")
	tlsCA := flag.String("tls-ca", os.Getenv("TLS_CA"), "CA certificate for TLS verification (PEM)")
	showVersion := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("amid %s (commit %s, built %s)\n", version, commit, date)
		os.Exit(0)
	}

	// Setup logger
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Str("service", "amid").Logger()

	if *postgresURL == "" {
		logger.Fatal().Msg("PostgreSQL URL is required (--postgres-url or POSTGRES_URL)")
	}

	logger.Info().
		Str("rest_addr", *restAddr).
		Str("metrics_addr", *metricsAddr).
		Str("mode", modeStr(*abciAddr)).
		Msg("starting SAGE ABCI daemon")

	// Create health checker
	health := metrics.NewHealthChecker()

	// Create SAGE ABCI application
	app, err := sageabci.NewSageApp(*badgerPath, *postgresURL, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create SAGE app")
	}
	defer func() { _ = app.Close() }()

	// v11.9 recovery: canonical scoped envelopes live in Badger and must be
	// verified into PostgreSQL before this replica advertises serving readiness.
	// A failure does not stop consensus from catching up, but /ready remains 503
	// so traffic cannot be routed to an incomplete selected-domain projection.
	if contents, listErr := app.GetBadgerStore().ListScopedContents(); listErr != nil {
		health.SetScopedProjectionStatus(metrics.ScopedProjectionStatus{Required: true, Detail: listErr.Error()})
		logger.Warn().Err(listErr).Msg("scoped serving projection state is unreadable")
	} else if len(contents) == 0 {
		health.SetScopedProjectionStatus(metrics.ScopedProjectionStatus{OK: true})
	} else {
		rebuilt, rebuildErr := app.RebuildScopedProjection(context.Background())
		projectionStatus := metrics.ScopedProjectionStatus{
			Required: true,
			OK:       rebuildErr == nil && rebuilt == len(contents),
			Records:  len(contents),
			Rebuilt:  rebuilt,
		}
		if rebuildErr != nil {
			projectionStatus.Detail = rebuildErr.Error()
		} else if !projectionStatus.OK {
			projectionStatus.Detail = fmt.Sprintf("rebuilt %d of %d scoped records", rebuilt, len(contents))
		}
		health.SetScopedProjectionStatus(projectionStatus)
		if !projectionStatus.OK {
			logger.Warn().Str("detail", projectionStatus.Detail).Msg("scoped serving projection is not ready")
		} else {
			logger.Info().Int("records", rebuilt).Msg("scoped serving projection verified from canonical state")
		}
	}

	// Content-validation enforcement advisory (non-fatal): warn when the app-v7
	// fork is active but this binary has no validator registry compiled in, so
	// this node won't enforce the gate. A mixed fleet (some nodes wired, some
	// not) would diverge — surface it so operators run a uniform build.
	if warn := app.ContentValidationEnforcementWarning(); warn != "" {
		logger.Warn().Msg(warn)
	}

	// Seed the replay-nonce allocator from the chain's committed nonces (same
	// wiring as cmd/sage-gui/node.go): the app-v9 consensus gate rejects any tx
	// whose nonce <= the signer's highest committed nonce, so a restarted amid
	// voter must resume ABOVE the chain instead of trusting the wall clock to
	// exceed it. Local badger read, keyed exactly like the consensus path
	// (auth.PublicKeyToAgentID), consulted at most once per key. Liveness-only,
	// never in the AppHash. GetNonce returns 0 for an unseen key -> no-op seed.
	badgerStore := app.GetBadgerStore()
	tx.SetNonceFloorFunc(func(pub ed25519.PublicKey) (uint64, bool) {
		n, gerr := badgerStore.GetNonce(auth.PublicKeyToAgentID(pub))
		if gerr != nil || n == 0 {
			return 0, false
		}
		return n, true
	})

	health.SetPostgresHealth(true)
	logger.Info().Msg("SAGE ABCI application created")

	if *abciAddr != "" {
		// ── Standalone ABCI server mode (Docker: separate CometBFT container) ──
		runABCIServer(app, *abciAddr, *restAddr, *metricsAddr, *cometRPC, *validatorKeyFile, *governanceOperatorID, *tlsCert, *tlsKey, *tlsCA, *requireVoter, health, logger)
	} else {
		// ── In-process mode (single binary: ABCI + CometBFT embedded) ──
		if *cometHome == "" {
			logger.Fatal().Msg("CometBFT home directory is required in in-process mode (--home or COMETBFT_HOME)")
		}
		runInProcess(app, *cometHome, *restAddr, *metricsAddr, *governanceOperatorID, *tlsCert, *tlsKey, *tlsCA, *requireVoter, health, logger)
	}
}

// startMemoryVoter launches the per-node memory auto-voter. The voter signs
// MemoryVote/GovVote txs with the node's own validator key (no validator-set
// replacement) so submitted memories reach the 2/3 quorum on a real multi-node
// chain. Returns an error — rather than only warning — when no voter can run
// (keyFile empty, unreadable, or invalid); the caller decides whether that is
// fatal (--require-voter / VOTER_REQUIRED) or logged-and-tolerated (default).
func startMemoryVoter(ctx context.Context, app *sageabci.SageApp, cometRPC, keyFile string, health *metrics.HealthChecker, logger zerolog.Logger) error {
	if keyFile == "" {
		return fmt.Errorf("validator key not set (set --validator-key-file / VALIDATOR_KEY_FILE)")
	}
	key, err := voter.LoadPrivValidatorKey(keyFile)
	if err != nil {
		return fmt.Errorf("load validator key %s: %w", keyFile, err)
	}
	// Wire the health checker so amid's /ready voter block reflects the live voter
	// (amid serves /ready via NewMetricsServer), keeping it consistent with the
	// sage_voter_running gauge on the same listener.
	go voter.Run(ctx, app, app.GetOffchainStore(), voter.Config{Key: key, CometRPC: cometRPC, PollInterval: 2 * time.Second, Health: health}, logger)
	return nil
}

// requireVoterKeyOrExit is the --require-voter / VOTER_REQUIRED fail-fast gate:
// called BEFORE any listener starts, it exits non-zero when the memory
// auto-voter's consensus key is missing/unreadable/invalid, so the daemon can
// never silently serve without a voter (every submitted memory would strand at
// proposed forever). Without the flag, boot proceeds and startMemoryVoter's
// error is merely logged (legacy warn-and-continue behavior).
func requireVoterKeyOrExit(keyFile string, logger zerolog.Logger) {
	if keyFile == "" {
		logger.Fatal().Msg("--require-voter / VOTER_REQUIRED set but no validator key configured (set --validator-key-file / VALIDATOR_KEY_FILE) — refusing to serve without the memory auto-voter")
	}
	if _, err := voter.LoadPrivValidatorKey(keyFile); err != nil {
		logger.Fatal().Err(err).Str("key_file", keyFile).Msg("--require-voter / VOTER_REQUIRED set but the validator key is unusable — refusing to serve without the memory auto-voter")
	}
}

type governanceDomainBinder interface {
	SetExpectedGovernanceDelegationDomain(chainID string) error
}

const (
	governanceDomainRequestTimeout = 3 * time.Second
	governanceDomainResponseLimit  = 1 << 20
	governanceDomainRetryInitial   = 250 * time.Millisecond
	governanceDomainRetryMax       = 5 * time.Second
)

func newGovernanceDomainHTTPClient() *http.Client {
	return &http.Client{
		Timeout: governanceDomainRequestTimeout,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			// The validator's chain identity must come from the exact operator-
			// configured Comet endpoint, never a redirected authority.
			return http.ErrUseLastResponse
		},
	}
}

// configureExpectedGovernanceDomainFromRPC performs one bounded lookup of the
// authoritative CometBFT chain_id and binds it into the local app-v20 voter
// guard. Socket mode deliberately starts ABCI before the external Comet process,
// so transient connection failures are returned to the caller for retry.
func configureExpectedGovernanceDomainFromRPC(ctx context.Context, client *http.Client, app governanceDomainBinder, cometRPC string) (string, error) {
	if client == nil {
		return "", errors.New("CometBFT chain-id HTTP client is nil")
	}
	requestCtx, cancel := context.WithTimeout(ctx, governanceDomainRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(requestCtx, http.MethodGet, strings.TrimRight(cometRPC, "/")+"/status", nil)
	if err != nil {
		return "", fmt.Errorf("build CometBFT chain-id request: %w", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch CometBFT chain-id: %w", err)
	}
	defer resp.Body.Close()
	var status struct {
		Result struct {
			NodeInfo struct {
				Network string `json:"network"`
			} `json:"node_info"`
		} `json:"result"`
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fetch CometBFT chain-id: HTTP status %d", resp.StatusCode)
	}
	if decodeErr := json.NewDecoder(io.LimitReader(resp.Body, governanceDomainResponseLimit)).Decode(&status); decodeErr != nil {
		return "", fmt.Errorf("decode CometBFT chain-id response: %w", decodeErr)
	}
	if status.Result.NodeInfo.Network == "" {
		return "", errors.New("CometBFT status omitted chain-id")
	}
	if err = app.SetExpectedGovernanceDelegationDomain(status.Result.NodeInfo.Network); err != nil {
		return "", fmt.Errorf("bind CometBFT chain-id: %w", err)
	}
	return status.Result.NodeInfo.Network, nil
}

// bindExpectedGovernanceDomainFromRPCUntilReady keeps the app-v20 voter
// fail-closed until the external Comet process is reachable. A successful bind
// is permanent for this process; SageApp rejects any later attempt to swap the
// chain identity underneath a live validator.
func bindExpectedGovernanceDomainFromRPCUntilReady(
	ctx context.Context,
	client *http.Client,
	app governanceDomainBinder,
	cometRPC string,
	initialRetry time.Duration,
	maxRetry time.Duration,
	logger zerolog.Logger,
) error {
	if initialRetry <= 0 || maxRetry < initialRetry {
		return fmt.Errorf("invalid CometBFT chain-id retry window: initial=%s max=%s", initialRetry, maxRetry)
	}
	retryDelay := initialRetry
	for attempt := 1; ; attempt++ {
		chainID, err := configureExpectedGovernanceDomainFromRPC(ctx, client, app, cometRPC)
		if err == nil {
			logger.Info().Str("chain_id", chainID).Int("attempt", attempt).
				Msg("app-v20 upgrade voter bound to authoritative CometBFT chain-id")
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if attempt == 1 || attempt%12 == 0 {
			logger.Warn().Err(err).Int("attempt", attempt).Dur("retry_in", retryDelay).
				Msg("waiting for CometBFT chain-id; app-v20 upgrade voter remains fail-closed")
		}

		timer := time.NewTimer(retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-timer.C:
		}
		if retryDelay < maxRetry {
			retryDelay *= 2
			if retryDelay > maxRetry {
				retryDelay = maxRetry
			}
		}
	}
}

// runABCIServer starts the ABCI app as a TCP server for an external CometBFT node.
func runABCIServer(app *sageabci.SageApp, abciAddr, restAddr, metricsAddr, cometRPC, validatorKeyFile, governanceOperatorID, tlsCert, tlsKey, tlsCA string, requireVoter bool, health *metrics.HealthChecker, logger zerolog.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cmtLogger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stdout))

	// Runs-or-exits guarantee: validate the voter's key before any listener is up.
	if requireVoter {
		requireVoterKeyOrExit(validatorKeyFile, logger)
	}

	srv, err := abciserver.NewServer(abciAddr, "socket", app)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create ABCI server")
	}
	srv.SetLogger(cmtLogger)

	if err := srv.Start(); err != nil {
		logger.Fatal().Err(err).Msg("failed to start ABCI server")
	}
	defer func() {
		if err := srv.Stop(); err != nil {
			logger.Error().Err(err).Msg("error stopping ABCI server")
		}
	}()

	logger.Info().Str("addr", abciAddr).Msg("ABCI server listening")

	// Start metrics + REST + health in background
	startServices(ctx, app, restAddr, metricsAddr, cometRPC, validatorKeyFile, governanceOperatorID, tlsCert, tlsKey, tlsCA, health, logger)

	// Socket mode: the consensus key lives with the separate CometBFT process, so
	// the voter needs it supplied explicitly (operator mounts priv_validator_key.json
	// readable to amid via --validator-key-file). Absent → no voter (or no boot at
	// all under --require-voter, enforced by the pre-serve gate above).
	go func() {
		client := newGovernanceDomainHTTPClient()
		if bindErr := bindExpectedGovernanceDomainFromRPCUntilReady(
			ctx,
			client,
			app,
			cometRPC,
			governanceDomainRetryInitial,
			governanceDomainRetryMax,
			logger,
		); bindErr == nil {
			health.SetCometBFTHealth(true)
		} else if !errors.Is(bindErr, context.Canceled) {
			logger.Warn().Err(bindErr).Msg("CometBFT chain-id binding stopped; app-v20 upgrade voter remains fail-closed")
		}
	}()
	if err := startMemoryVoter(ctx, app, cometRPC, validatorKeyFile, health, logger); err != nil {
		if requireVoter {
			// Normally unreachable — requireVoterKeyOrExit already refused to serve —
			// but a key that rots between the gate and here must not slip through.
			logger.Fatal().Err(err).Msg("memory auto-voter cannot start (--require-voter / VOTER_REQUIRED)")
		}
		logger.Warn().Err(err).Msg("memory auto-voter disabled")
	}

	// Wait for shutdown
	waitForShutdown(nil, nil, health, logger)
}

// runInProcess embeds CometBFT in the same process as the ABCI app.
func runInProcess(app *sageabci.SageApp, cometHome, restAddr, metricsAddr, governanceOperatorID, tlsCert, tlsKey, tlsCA string, requireVoter bool, health *metrics.HealthChecker, logger zerolog.Logger) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cometCfg, err := loadCometConfig(cometHome)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to parse CometBFT config, using defaults")
		cometCfg = config.DefaultConfig()
		cometCfg.SetRoot(cometHome)
	}

	// Runs-or-exits guarantee: validate the voter's key before CometBFT or any
	// listener starts. In-process the key is the one under --home.
	if requireVoter {
		requireVoterKeyOrExit(cometCfg.PrivValidatorKeyFile(), logger)
	}

	pv := privval.LoadFilePV(
		cometCfg.PrivValidatorKeyFile(),
		cometCfg.PrivValidatorStateFile(),
	)
	nodeKey, err := p2p.LoadNodeKey(cometCfg.NodeKeyFile())
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to load node key")
	}

	cmtLogger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stdout))

	cometNode, err := node.NewNode(
		cometCfg,
		pv,
		nodeKey,
		proxy.NewLocalClientCreator(app),
		node.DefaultGenesisDocProviderFunc(cometCfg),
		config.DefaultDBProvider,
		node.DefaultMetricsProvider(cometCfg.Instrumentation),
		cmtLogger,
	)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create CometBFT node")
	}

	if err := cometNode.Start(); err != nil {
		logger.Fatal().Err(err).Msg("failed to start CometBFT node")
	}
	defer func() {
		if err := cometNode.Stop(); err != nil {
			logger.Error().Err(err).Msg("error stopping CometBFT node")
		}
		cometNode.Wait()
	}()

	logger.Info().
		Str("node_id", string(cometNode.NodeInfo().ID())).
		Msg("CometBFT node started (in-process)")
	defaultNodeInfo, ok := cometNode.NodeInfo().(p2p.DefaultNodeInfo)
	if !ok {
		logger.Fatal().Msg("cannot read embedded CometBFT chain-id from node info")
	}
	if err := app.SetExpectedGovernanceDelegationDomain(defaultNodeInfo.Network); err != nil {
		logger.Fatal().Err(err).Msg("cannot bind app-v20 upgrade voter to embedded CometBFT chain-id")
	}
	health.SetCometBFTHealth(true)

	// In-process: CometBFT RPC is localhost
	cometRPC := fmt.Sprintf("http://127.0.0.1%s", cometCfg.RPC.ListenAddress[len("tcp://0.0.0.0"):])
	startServices(ctx, app, restAddr, metricsAddr, cometRPC, cometCfg.PrivValidatorKeyFile(), governanceOperatorID, tlsCert, tlsKey, tlsCA, health, logger)

	// In-process: the consensus key is right here under --home; the voter signs
	// memory votes with it (same key CometBFT validates blocks with).
	if err := startMemoryVoter(ctx, app, cometRPC, cometCfg.PrivValidatorKeyFile(), health, logger); err != nil {
		if requireVoter {
			// Normally unreachable — requireVoterKeyOrExit already refused to serve —
			// but a key that rots between the gate and here must not slip through.
			logger.Fatal().Err(err).Msg("memory auto-voter cannot start (--require-voter / VOTER_REQUIRED)")
		}
		logger.Warn().Err(err).Msg("memory auto-voter disabled")
	}

	// Health checks with node status
	go healthLoop(app, cometNode, health)

	waitForShutdown(nil, nil, health, logger)
}

// startServices launches the metrics server and REST API.
func startServices(ctx context.Context, app *sageabci.SageApp, restAddr, metricsAddr, cometRPC, validatorKeyFile, governanceOperatorID, tlsCert, tlsKey, tlsCA string, health *metrics.HealthChecker, logger zerolog.Logger) {
	// Prometheus metrics server
	metricsServer := metrics.NewMetricsServer(metricsAddr, health)
	go func() {
		logger.Info().Str("addr", metricsAddr).Msg("starting metrics server")
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("metrics server error")
		}
	}()

	// REST API server
	pgStore := app.GetOffchainStore()
	badgerStore := app.GetBadgerStore()
	restServer := rest.NewServer(cometRPC, pgStore, pgStore, badgerStore, health, logger, embedding.NewClient("", ""))
	// --home/--validator-key-file is authoritative for amid. Neutralize any key
	// NewServer inherited from the compatibility env path before explicit load.
	restServer.DisableValidatorSigningKey()
	if validatorKeyFile == "" {
		logger.Warn().Msg("REST governance disabled: validator key file is not configured")
	} else if validatorKey, keyErr := voter.LoadPrivValidatorKey(validatorKeyFile); keyErr != nil {
		logger.Error().Err(keyErr).Str("key_file", validatorKeyFile).Msg("REST governance disabled: validator key is unusable")
	} else if keyErr = restServer.SetValidatorSigningKey(validatorKey); keyErr != nil {
		logger.Error().Err(keyErr).Msg("REST governance disabled: validator key injection failed")
	}
	if governanceOperatorID == "" {
		logger.Warn().Msg("REST governance disabled: set --governance-operator-id / SAGE_GOVERNANCE_OPERATOR_ID")
	} else if operatorErr := restServer.SetGovernanceOperatorID(governanceOperatorID); operatorErr != nil {
		logger.Error().Err(operatorErr).Msg("REST governance disabled: governance operator identity is invalid")
	}
	restServer.StartEmbeddingRepair(ctx)
	restServer.SetSuppCache(app.SuppCache)
	// v8.0: wire the off-consensus fork-gate accessor so REST handlers
	// flip to ancestor-walk access checks once the chain reports a post-fork
	// height. Advisory only — the consensus path uses app.postV8Fork(height).
	restServer.SetPostV8ForkAccessor(app.IsPostV8Fork)
	restServer.SetPostV17ForNextTxAccessor(app.IsAppV17ActiveForNextTx)
	restServer.SetPostV20ForNextTxAccessor(app.IsAppV20ActiveForNextTx)
	restServer.SetGovernanceDomainAccessor(app.GovernanceDelegationDomain)

	if tlsCert != "" && tlsKey != "" {
		// TLS mode: load certs and start HTTPS.
		tlsCfg, err := buildTLSConfig(tlsCert, tlsKey, tlsCA, logger)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to build TLS config")
		}
		go func() {
			logger.Info().Str("addr", restAddr).Str("comet_rpc", cometRPC).Msg("starting REST server (TLS)")
			if err := restServer.StartTLS(restAddr, tlsCfg); err != nil && err != http.ErrServerClosed {
				logger.Error().Err(err).Msg("REST TLS server error")
			}
		}()
	} else {
		go func() {
			logger.Info().Str("addr", restAddr).Str("comet_rpc", cometRPC).Msg("starting REST server")
			if err := restServer.Start(restAddr); err != nil && err != http.ErrServerClosed {
				logger.Error().Err(err).Msg("REST server error")
			}
		}()
	}
}

// buildTLSConfig creates a tls.Config from individual cert/key/CA file paths.
func buildTLSConfig(certFile, keyFile, caFile string, logger zerolog.Logger) (*tls.Config, error) {
	cfg, err := tlsca.ServerTLSConfigFromFiles(certFile, keyFile, caFile)
	if err != nil {
		return nil, err
	}
	logger.Info().
		Str("cert", certFile).
		Str("ca", caFile).
		Msg("TLS configured for REST API")
	return cfg, nil
}

func healthLoop(app *sageabci.SageApp, cometNode *node.Node, health *metrics.HealthChecker) {
	pgStore := app.GetOffchainStore()
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		if err := pgStore.Ping(ctx); err != nil {
			health.SetPostgresHealth(false)
		} else {
			health.SetPostgresHealth(true)
		}
		cancel()
		if cometNode != nil {
			health.SetCometBFTHealth(cometNode.IsRunning())
		}
	}
}

func waitForShutdown(restServer, metricsServer *http.Server, health *metrics.HealthChecker, logger zerolog.Logger) {
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit

	logger.Info().Str("signal", sig.String()).Msg("shutting down")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if restServer != nil {
		if err := restServer.Shutdown(ctx); err != nil {
			logger.Error().Err(err).Msg("REST server shutdown error")
		}
	}
	if metricsServer != nil {
		if err := metricsServer.Shutdown(ctx); err != nil {
			logger.Error().Err(err).Msg("metrics server shutdown error")
		}
	}
}

func modeStr(abciAddr string) string {
	if abciAddr != "" {
		return "abci-server"
	}
	return "in-process"
}

func envOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

// envBoolOrDefault parses key as a boolean ("true", "1", "false", ...); unset
// or unparsable values yield defaultVal.
func envBoolOrDefault(key string, defaultVal bool) bool {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	// Accept the strconv.ParseBool set plus common yes/no/on/off. An unrecognized
	// value on a fail-fast safety flag (VOTER_REQUIRED) must not silently disarm the
	// gate — warn loudly and keep the default.
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "t", "true", "yes", "y", "on":
		return true
	case "0", "f", "false", "no", "n", "off":
		return false
	default:
		fmt.Fprintf(os.Stderr, "amid: %s=%q is not a recognized boolean (use true/false); using default %v\n", key, v, defaultVal)
		return defaultVal
	}
}

func loadCometConfig(home string) (*config.Config, error) {
	cfg := config.DefaultConfig()
	cfg.SetRoot(home)

	configFile := cfg.RootDir + "/config/config.toml"
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return cfg, fmt.Errorf("config file not found: %s", configFile)
	}

	return cfg, nil
}
