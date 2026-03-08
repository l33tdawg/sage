package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"bufio"

	"github.com/cometbft/cometbft/config"
	cmtlog "github.com/cometbft/cometbft/libs/log"
	"github.com/cometbft/cometbft/node"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	"github.com/cometbft/cometbft/proxy"
	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/api/rest"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/l33tdawg/sage/web"
)

func runServe() error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr, TimeFormat: time.RFC3339}).
		With().Timestamp().Str("service", "sage-lite").Logger()

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

	// Initialize CometBFT config if needed
	if initErr := initCometBFTConfig(cometHome); initErr != nil {
		return fmt.Errorf("init CometBFT: %w", initErr)
	}

	// Create SQLite store
	ctx := context.Background()
	sqliteStore, err := store.NewSQLiteStore(ctx, sqlitePath)
	if err != nil {
		return fmt.Errorf("open SQLite: %w", err)
	}
	defer sqliteStore.Close()

	// Unlock encryption vault if enabled
	if cfg.Encryption.Enabled {
		vaultKeyPath := filepath.Join(SageHome(), "vault.key")
		if !vault.Exists(vaultKeyPath) {
			return fmt.Errorf("encryption enabled but vault.key not found at %s — run 'sage-lite setup' first", vaultKeyPath)
		}

		passphrase := os.Getenv("SAGE_PASSPHRASE")
		if passphrase == "" {
			fmt.Print("  Enter vault passphrase: ")
			passphrase, err = readPassphrase()
			if err != nil {
				return fmt.Errorf("read passphrase: %w", err)
			}
		}

		v, vaultErr := vault.Open(vaultKeyPath, passphrase)
		if vaultErr != nil {
			return fmt.Errorf("unlock vault: %w", vaultErr)
		}
		sqliteStore.SetVault(v)
		logger.Info().Msg("encryption vault unlocked — memories are encrypted at rest (AES-256-GCM)")
	}

	// Create BadgerDB store
	badgerStore, err := store.NewBadgerStore(badgerPath)
	if err != nil {
		return fmt.Errorf("open BadgerDB: %w", err)
	}
	defer badgerStore.CloseBadger() //nolint:errcheck

	// Create SAGE ABCI app with SQLite backend
	app, err := sageabci.NewSageAppWithStores(badgerStore, sqliteStore, logger)
	if err != nil {
		return fmt.Errorf("create SAGE app: %w", err)
	}
	defer app.Close()

	// Create embedding provider
	embedProvider := createEmbeddingProvider(cfg, logger)

	// Health checker
	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true) // SQLite is always "healthy"

	// Start CometBFT in-process
	cometCfg := config.DefaultConfig()
	cometCfg.SetRoot(cometHome)
	cometCfg.Consensus.CreateEmptyBlocks = false
	cometCfg.Consensus.CreateEmptyBlocksInterval = 0
	cometCfg.RPC.ListenAddress = "tcp://127.0.0.1:26657"
	cometCfg.Instrumentation.Prometheus = false

	if cfg.Quorum.Enabled {
		// Quorum mode: enable P2P for multi-validator consensus
		cometCfg.Consensus.TimeoutCommit = 3 * time.Second
		p2pAddr := cfg.Quorum.P2PAddr
		if p2pAddr == "" {
			p2pAddr = "tcp://0.0.0.0:26656"
		}
		cometCfg.P2P.ListenAddress = p2pAddr
		cometCfg.P2P.PersistentPeers = joinPeers(cfg.Quorum.Peers)
		cometCfg.P2P.AddrBookStrict = false   // Allow LAN addresses
		cometCfg.P2P.AllowDuplicateIP = true   // Multiple nodes on same network
		cometCfg.P2P.PexReactor = false        // Use persistent peers only
		logger.Info().
			Str("p2p_addr", p2pAddr).
			Int("peers", len(cfg.Quorum.Peers)).
			Msg("quorum mode enabled — multi-validator consensus")
	} else {
		// Personal mode: single validator, fast blocks, no P2P
		cometCfg.Consensus.TimeoutCommit = 1 * time.Second
		cometCfg.P2P.ListenAddress = "tcp://127.0.0.1:26656"
	}

	pv := privval.LoadFilePV(
		cometCfg.PrivValidatorKeyFile(),
		cometCfg.PrivValidatorStateFile(),
	)
	nodeKey, err := p2p.LoadNodeKey(cometCfg.NodeKeyFile())
	if err != nil {
		return fmt.Errorf("load node key: %w", err)
	}

	cmtLogger := cmtlog.NewTMLogger(cmtlog.NewSyncWriter(os.Stderr))
	cmtLogger = cmtlog.NewFilter(cmtLogger, cmtlog.AllowError()) // Quiet CometBFT logs

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
		return fmt.Errorf("create CometBFT node: %w", err)
	}

	if err := cometNode.Start(); err != nil {
		return fmt.Errorf("start CometBFT: %w", err)
	}
	defer func() {
		if stopErr := cometNode.Stop(); stopErr != nil {
			logger.Error().Err(stopErr).Msg("error stopping CometBFT")
		}
		cometNode.Wait()
	}()

	health.SetCometBFTHealth(true)
	logger.Info().
		Str("node_id", string(cometNode.NodeInfo().ID())).
		Msg("CometBFT node started (single-validator personal mode)")

	// CometBFT RPC URL for tx broadcast
	cometRPC := "http://127.0.0.1:26657"

	// Create REST server
	restServer := rest.NewServer(cometRPC, sqliteStore, sqliteStore, badgerStore, health, logger)

	// Create dashboard handler
	dashboard := web.NewDashboardHandler(sqliteStore, version)
	dashboard.Encrypted = cfg.Encryption.Enabled
	if cfg.Encryption.Enabled {
		dashboard.VaultKeyPath = filepath.Join(SageHome(), "vault.key")
	}

	// Build combined router
	r := chi.NewRouter()
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"*"},
		AllowCredentials: false,
	}))

	// Mount REST API routes
	r.Mount("/", restServer.Router())
	// Mount dashboard routes (these don't collide — dashboard uses /v1/dashboard/ prefix)
	dashboard.RegisterRoutes(r)

	// Embedding endpoint override — use configured provider, not just Ollama
	r.Post("/v1/embed/personal", handleEmbedPersonal(embedProvider))

	httpServer := &http.Server{
		Addr:         cfg.RESTAddr,
		Handler:      r,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		logger.Info().
			Str("addr", cfg.RESTAddr).
			Str("dashboard", fmt.Sprintf("http://localhost%s/ui/", cfg.RESTAddr)).
			Msg("SAGE Personal ready")

		fmt.Fprintf(os.Stderr, "\n  SAGE Personal is running!\n")
		fmt.Fprintf(os.Stderr, "  Dashboard: http://localhost%s/ui/\n", cfg.RESTAddr)
		fmt.Fprintf(os.Stderr, "  REST API:  http://localhost%s/v1/\n\n", cfg.RESTAddr)

		// Auto-open dashboard in browser
		go openBrowser(fmt.Sprintf("http://localhost%s/ui/", cfg.RESTAddr))

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server error")
		}
	}()

	// Start auto-validator goroutine
	// In quorum mode, auto-validator still runs locally (memories auto-commit on each node).
	// Full cross-node vote exchange is a Phase 2 feature.
	go autoValidator(ctx, sqliteStore, logger)
	if cfg.Quorum.Enabled {
		logger.Info().Msg("quorum mode — P2P consensus active, blocks validated by both nodes")
	}

	// Auto-import pending chat history (from setup wizard)
	go autoImport(cfg, embedProvider, logger)

	// Wait for shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	sig := <-quit
	logger.Info().Str("signal", sig.String()).Msg("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
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

	// Create genesis with single validator
	genDoc := cmttypes.GenesisDoc{
		ChainID:         "sage-personal",
		GenesisTime:     cmttime.Now(),
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
	if err := genDoc.ValidateAndComplete(); err != nil {
		return fmt.Errorf("validate genesis: %w", err)
	}
	if err := genDoc.SaveAs(filepath.Join(configDir, "genesis.json")); err != nil {
		return fmt.Errorf("save genesis: %w", err)
	}

	// Write minimal config.toml
	configToml := `# SAGE Personal — CometBFT config
proxy_app = "kvstore"
moniker = "sage-personal"

[rpc]
laddr = "tcp://127.0.0.1:26657"

[p2p]
laddr = ""

[consensus]
timeout_commit = "1s"
create_empty_blocks = false

[mempool]
size = 1000
`
	return os.WriteFile(filepath.Join(configDir, "config.toml"), []byte(configToml), 0600)
}

// createEmbeddingProvider creates the configured embedding provider.
func createEmbeddingProvider(cfg *Config, logger zerolog.Logger) embedding.Provider {
	switch cfg.Embedding.Provider {
	case "ollama":
		logger.Info().Str("url", cfg.Embedding.BaseURL).Msg("using Ollama embeddings")
		return embedding.NewClient(cfg.Embedding.BaseURL, cfg.Embedding.Model)
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

// autoValidator auto-votes "accept" on all proposed memories.
// This simulates permissive governance for the personal single-validator mode.
// It directly updates memory status in the store (skips the vote tx pipeline).
func autoValidator(ctx context.Context, memStore store.MemoryStore, logger zerolog.Logger) {
	// Wait for the node to start producing blocks
	time.Sleep(3 * time.Second)
	logger.Info().Msg("auto-validator started — will auto-commit proposed memories")

	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, err := memStore.GetPendingByDomain(ctx, "%", 20)
			if err != nil {
				continue
			}
			for _, mem := range pending {
				now := time.Now()
				if updateErr := memStore.UpdateStatus(ctx, mem.MemoryID, "committed", now); updateErr != nil {
					logger.Debug().Err(updateErr).Str("memory_id", mem.MemoryID).Msg("auto-commit failed")
				} else {
					logger.Info().Str("memory_id", mem.MemoryID).Str("domain", mem.DomainTag).Msg("auto-committed memory")
				}
			}
		}
	}
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

	baseURL := fmt.Sprintf("http://localhost%s", cfg.RESTAddr)

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
		baseURL = fmt.Sprintf("http://localhost%s", cfg.RESTAddr)
	}

	ctx := context.Background()
	healthReq, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/health", nil)
	resp, err := http.DefaultClient.Do(healthReq)
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
	statsReq, _ := http.NewRequestWithContext(ctx, "GET", baseURL+"/v1/dashboard/stats", nil)
	statsResp, err := http.DefaultClient.Do(statsReq)
	if err == nil {
		defer statsResp.Body.Close()
		var stats map[string]any
		if json.NewDecoder(statsResp.Body).Decode(&stats) == nil {
			if total, ok := stats["total_memories"]; ok {
				fmt.Printf("  Memories: %.0f\n", total)
			}
		}
	}

	return nil
}
