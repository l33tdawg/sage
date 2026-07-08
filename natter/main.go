package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	ma "github.com/multiformats/go-multiaddr"
	"github.com/rs/zerolog"
)

// version is stamped via -ldflags "-X main.version=..." by the Makefile.
var version = "0.1.0-dev"

const defaultConfigPath = "natter.yaml"

func main() {
	configPath := flag.String("config", defaultConfigPath, "path to natter YAML config")
	showVersion := flag.Bool("version", false, "print version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Printf("natter %s\n", version)
		return
	}

	log := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Str("svc", "natter").Logger()

	if err := run(*configPath, log); err != nil {
		log.Fatal().Err(err).Msg("natter exited with error")
	}
}

func run(configPath string, log zerolog.Logger) error {
	// Only insist the config file exists when the operator pointed at one
	// explicitly; the default path missing just means "run with defaults".
	mustExist := configPath != defaultConfigPath
	cfg, err := LoadConfig(configPath, mustExist)
	if err != nil {
		return err
	}
	if !mustExist {
		if _, statErr := os.Stat(configPath); os.IsNotExist(statErr) {
			log.Warn().Str("path", configPath).Msg("no config file found, running with built-in defaults")
		}
	}

	priv, err := loadOrCreateIdentity(cfg.IdentityKeyPath, log)
	if err != nil {
		return err
	}

	h, err := buildHost(cfg, priv)
	if err != nil {
		return err
	}
	defer h.Close()

	var rly *relay.Relay
	if cfg.Relay.RelayEnabled() {
		rly, err = startRelay(h, cfg.Relay)
		if err != nil {
			return err
		}
		defer rly.Close()
		log.Info().
			Str("version", version).
			Int("relay_max_reservations", cfg.Relay.MaxReservations).
			Int("relay_max_circuits_per_peer", cfg.Relay.MaxCircuits).
			Int64("relay_circuit_data_bytes", cfg.Relay.CircuitDataBytes).
			Dur("relay_circuit_duration", cfg.Relay.CircuitDuration.Std()).
			Msg("circuit relay v2 + AutoNAT service online")
	} else {
		// Coordinator-only: AutoNAT reachability + identify, no relaying. NOTE
		// this also disables DCUtR brokering (the relay is DCUtR's bootstrap
		// channel), so two NAT'd peers cannot connect THROUGH natter — only
		// already-reachable peers benefit. See RelayConfig.Enabled.
		log.Warn().
			Str("version", version).
			Msg("relay DISABLED (relay.enabled=false): AutoNAT-only coordinator — natter will NOT broker hole-punches for NAT'd peers")
	}

	printBootstrapBanner(h)

	// Rendezvous note: SAGE nodes bootstrap directly against the static
	// multiaddrs printed above (published in SAGE bootstrap config); peers
	// discover each other via identify + the relay's peerstore once
	// connected. A dedicated rendezvous protocol server is deliberately NOT
	// enabled here — it is Sprint-3 client-side work (see README).

	healthSrv := startHealthServer(cfg.HealthAddr, h, log)

	// Graceful shutdown on SIGTERM (systemd stop) / SIGINT (ctrl-c).
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	log.Info().Str("signal", sig.String()).Msg("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if healthSrv != nil {
		if err := healthSrv.Shutdown(shutdownCtx); err != nil {
			log.Warn().Err(err).Msg("health server shutdown")
		}
	}
	if rly != nil {
		if err := rly.Close(); err != nil {
			log.Warn().Err(err).Msg("relay close")
		}
	}
	if err := h.Close(); err != nil {
		return fmt.Errorf("close libp2p host: %w", err)
	}
	log.Info().Msg("bye")
	return nil
}

// printBootstrapBanner writes the peer ID + full multiaddrs to stdout in a
// copy-paste-ready block. This exact block is what gets published as SAGE
// bootstrap config, so keep it clean of log decoration.
func printBootstrapBanner(h host.Host) {
	p2pPart, err := ma.NewMultiaddr("/p2p/" + h.ID().String())
	if err != nil {
		// Cannot happen for a valid host ID; guard anyway.
		fmt.Fprintf(os.Stderr, "encode /p2p multiaddr: %v\n", err)
		return
	}
	fmt.Println("==================== NATTER BOOTSTRAP INFO ====================")
	fmt.Printf("peer id: %s\n", h.ID())
	fmt.Println("multiaddrs (publish these in SAGE bootstrap config):")
	for _, addr := range h.Addrs() {
		fmt.Printf("  %s\n", addr.Encapsulate(p2pPart))
	}
	fmt.Println("===============================================================")
}

// startHealthServer exposes GET /healthz on a local port for systemd
// watchdogs and uptime checks. It reports liveness only — no relay metrics,
// no peer info beyond our own ID — and should stay bound to localhost.
func startHealthServer(addr string, h host.Host, log zerolog.Logger) *http.Server {
	if addr == "" {
		log.Warn().Msg("health_addr empty, /healthz disabled")
		return nil
	}
	started := time.Now()
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":         "ok",
			"peer_id":        h.ID().String(),
			"uptime_seconds": int64(time.Since(started).Seconds()),
			"version":        version,
		})
	})
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error().Err(err).Str("addr", addr).Msg("health server failed")
		}
	}()
	log.Info().Str("addr", addr).Msg("health endpoint listening on /healthz")
	return srv
}
