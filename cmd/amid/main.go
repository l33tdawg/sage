package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/tlsca"
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

	health.SetPostgresHealth(true)
	logger.Info().Msg("SAGE ABCI application created")

	if *abciAddr != "" {
		// ── Standalone ABCI server mode (Docker: separate CometBFT container) ──
		runABCIServer(app, *abciAddr, *restAddr, *metricsAddr, *cometRPC, *tlsCert, *tlsKey, *tlsCA, health, logger)
	} else {
		// ── In-process mode (single binary: ABCI + CometBFT embedded) ──
		if *cometHome == "" {
			logger.Fatal().Msg("CometBFT home directory is required in in-process mode (--home or COMETBFT_HOME)")
		}
		runInProcess(app, *cometHome, *restAddr, *metricsAddr, *tlsCert, *tlsKey, *tlsCA, health, logger)
	}
}

// runABCIServer starts the ABCI app as a TCP server for an external CometBFT node.
func runABCIServer(app *sageabci.SageApp, abciAddr, restAddr, metricsAddr, cometRPC, tlsCert, tlsKey, tlsCA string, health *metrics.HealthChecker, logger zerolog.Logger) {
	cmtLogger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stdout))

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

	health.SetCometBFTHealth(true)
	logger.Info().Str("addr", abciAddr).Msg("ABCI server listening")

	// Start metrics + REST + health in background
	startServices(app, restAddr, metricsAddr, cometRPC, tlsCert, tlsKey, tlsCA, health, logger)

	// Wait for shutdown
	waitForShutdown(nil, nil, health, logger)
}

// runInProcess embeds CometBFT in the same process as the ABCI app.
func runInProcess(app *sageabci.SageApp, cometHome, restAddr, metricsAddr, tlsCert, tlsKey, tlsCA string, health *metrics.HealthChecker, logger zerolog.Logger) {
	cometCfg, err := loadCometConfig(cometHome)
	if err != nil {
		logger.Warn().Err(err).Msg("failed to parse CometBFT config, using defaults")
		cometCfg = config.DefaultConfig()
		cometCfg.SetRoot(cometHome)
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

	health.SetCometBFTHealth(true)
	logger.Info().
		Str("node_id", string(cometNode.NodeInfo().ID())).
		Msg("CometBFT node started (in-process)")

	// In-process: CometBFT RPC is localhost
	cometRPC := fmt.Sprintf("http://127.0.0.1%s", cometCfg.RPC.ListenAddress[len("tcp://0.0.0.0"):])
	startServices(app, restAddr, metricsAddr, cometRPC, tlsCert, tlsKey, tlsCA, health, logger)

	// Health checks with node status
	go healthLoop(app, cometNode, health)

	waitForShutdown(nil, nil, health, logger)
}

// startServices launches the metrics server and REST API.
func startServices(app *sageabci.SageApp, restAddr, metricsAddr, cometRPC, tlsCert, tlsKey, tlsCA string, health *metrics.HealthChecker, logger zerolog.Logger) {
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
	restServer.SetSuppCache(app.SuppCache)

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

func loadCometConfig(home string) (*config.Config, error) {
	cfg := config.DefaultConfig()
	cfg.SetRoot(home)

	configFile := cfg.RootDir + "/config/config.toml"
	if _, err := os.Stat(configFile); os.IsNotExist(err) {
		return cfg, fmt.Errorf("config file not found: %s", configFile)
	}

	return cfg, nil
}

