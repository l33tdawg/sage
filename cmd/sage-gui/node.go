package main

import (
	"bufio"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
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
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/api/rest"
	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/orchestrator"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/l33tdawg/sage/web"
)

func runServe() error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

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

	// Auto-migrate on version upgrade: backup SQLite, reset chain state
	if migrated, migrateErr := migrateOnUpgrade(cfg.DataDir); migrateErr != nil {
		return fmt.Errorf("upgrade migration: %w", migrateErr)
	} else if migrated {
		logger.Info().
			Str("version", version).
			Msg("upgrade migration completed — chain state reset, memories preserved")
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
	defer func() { _ = sqliteStore.Close() }()

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
			logger.Info().Msg("Synaptic Ledger unlocked — memories are encrypted at rest (AES-256-GCM)")
		}
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
	app.Version = version
	defer func() { _ = app.Close() }()

	// Create embedding provider
	embedProvider := createEmbeddingProvider(cfg, logger)

	// Health checker
	health := metrics.NewHealthChecker()
	health.Version = version
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

	// CometBFT RPC URL for tx broadcast
	cometRPC := "http://127.0.0.1:26657"

	// Backfill on_chain_height and first_seen for agents already registered on-chain
	// but missing these fields in SQLite (upgrade path from v3.5 → v3.7.6+)
	signingKeyForMigrate := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger)
	sageabci.MigrateAgentsOnChain(ctx, sqliteStore, badgerStore, cometRPC, signingKeyForMigrate, logger)

	// Create REST server
	restServer := rest.NewServer(cometRPC, sqliteStore, sqliteStore, badgerStore, health, logger, embedProvider)

	// Create dashboard handler
	dashboard := web.NewDashboardHandler(sqliteStore, version)
	dashboard.BadgerStore = badgerStore // Wire on-chain RBAC for agent isolation

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
	if sk := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger); sk != nil {
		dashboard.SigningKey = sk
	}

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

	// Wire pre-validate function into both dashboard and REST API.
	// This shares the same 4 validator checks used by startAppValidators.
	preValidate := func(content, contentHash, domain, memType string, confidence float64) []web.PreValidateVote {
		type checker struct {
			name     string
			validate func() (string, string)
		}
		checks := []checker{
			{"sentinel", func() (string, string) { return "accept", "baseline accept" }},
			{"dedup", func() (string, string) {
				exists, err := sqliteStore.FindByContentHash(ctx, contentHash)
				if err == nil && exists {
					return "reject", fmt.Sprintf("duplicate content (hash: %s)", contentHash[:8])
				}
				return "accept", "content is unique"
			}},
			{"quality", func() (string, string) {
				if len(content) < 20 {
					return "reject", fmt.Sprintf("content too short (%d chars, minimum 20)", len(content))
				}
				lower := strings.ToLower(content)
				for _, p := range []string{"user said hi", "user greeted", "session started", "brain online", "brain is awake", "no action taken", "user said morning", "new session started"} {
					if strings.Contains(lower, p) {
						return "reject", "low-value observation: matches noise pattern"
					}
				}
				if strings.HasPrefix(content, "[Task Reflection]") && len(content) < 60 {
					return "reject", "empty reflection header without substance"
				}
				return "accept", "content passes quality check"
			}},
			{"consistency", func() (string, string) {
				if confidence < 0.3 {
					return "reject", fmt.Sprintf("confidence too low (%.2f)", confidence)
				}
				if memType == "fact" && confidence < 0.7 {
					return "reject", fmt.Sprintf("facts require confidence >= 0.7 (got %.2f)", confidence)
				}
				if domain == "" {
					return "reject", "domain required"
				}
				return "accept", "passes consistency check"
			}},
		}

		votes := make([]web.PreValidateVote, len(checks))
		for i, c := range checks {
			decision, reason := c.validate()
			votes[i] = web.PreValidateVote{Validator: c.name, Decision: decision, Reason: reason}
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
		AllowedOrigins:   []string{"http://localhost:*", "http://127.0.0.1:*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Content-Type", "X-Agent-ID", "X-Signature", "X-Timestamp", "X-Nonce"},
		AllowCredentials: false,
	}))

	// Mount REST API routes
	r.Mount("/", restServer.Router())
	// Mount dashboard routes (these don't collide — dashboard uses /v1/dashboard/ prefix)
	dashboard.RegisterRoutes(r)

	// Start background memory cleanup loop
	memory.StartCleanupLoop(ctx, sqliteStore)

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
		fmt.Fprintf(os.Stderr, "  CEREBRUM:  http://localhost%s/ui/\n", cfg.RESTAddr)
		fmt.Fprintf(os.Stderr, "  REST API:  http://localhost%s/v1/\n\n", cfg.RESTAddr)

		// Auto-open dashboard in browser (unless suppressed by tray app)
		if os.Getenv("SAGE_NO_BROWSER") == "" {
			go openBrowser(fmt.Sprintf("http://localhost%s/ui/", cfg.RESTAddr))
		}

		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error().Err(err).Msg("HTTP server error")
		}
	}()

	// Start app-level validators (4 in-process validators with BFT consensus)
	// Derive deterministic seed from the node's signing key for reproducible validator keys.
	var validatorSeed [32]byte
	if sk := loadNodeSigningKey(cometCfg.PrivValidatorKeyFile(), logger); sk != nil {
		h := sha256.Sum256(sk.Seed())
		copy(validatorSeed[:], h[:])
	}
	go startAppValidators(ctx, app, sqliteStore, cometRPC, validatorSeed, logger)
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

// restBaseURL builds an HTTP base URL from a REST address.
// If addr starts with ":" (just a port), it prepends "localhost".
// Otherwise it uses the address as-is (e.g. "127.0.0.1:18080").
func restBaseURL(addr string) string {
	if strings.HasPrefix(addr, ":") {
		return "http://localhost" + addr
	}
	return "http://" + addr
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

// voteDecisionFromString converts a decision string to a tx.VoteDecision uint8.
func voteDecisionFromString(s string) tx.VoteDecision {
	switch s {
	case "accept":
		return tx.VoteDecisionAccept
	case "reject":
		return tx.VoteDecisionReject
	case "abstain":
		return tx.VoteDecisionAbstain
	default:
		return tx.VoteDecisionAccept
	}
}

// broadcastVoteTx sends an encoded vote transaction to CometBFT via broadcast_tx_sync.
func broadcastVoteTx(cometRPC string, encoded []byte, logger zerolog.Logger) {
	txHex := hex.EncodeToString(encoded)
	url := fmt.Sprintf("%s/broadcast_tx_sync?tx=0x%s", cometRPC, txHex)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil) //nolint:gosec
	if err != nil {
		logger.Debug().Err(err).Msg("failed to create broadcast request")
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Debug().Err(err).Msg("failed to broadcast vote tx")
		return
	}
	resp.Body.Close()
}

// startAppValidators runs 4 in-process application validators (sentinel, dedup,
// quality, consistency) that vote on proposed memories through CometBFT consensus.
// Each validator has a deterministic Ed25519 key derived from the node's seed.
func startAppValidators(ctx context.Context, app *sageabci.SageApp, memStore store.MemoryStore,
	cometRPC string, seed [32]byte, logger zerolog.Logger) {

	// Generate 4 validator keys deterministically from the node seed
	type valConfig struct {
		name     string
		key      ed25519.PrivateKey
		validate func(content, hash, domain, memType string, conf float64) (string, string) // decision, reason
	}

	validators := make([]valConfig, 4)
	names := []string{"sentinel", "dedup", "quality", "consistency"}

	for i, name := range names {
		keySeed := sha256.Sum256(append(seed[:], []byte("sage-validator-"+name)...))
		key := ed25519.NewKeyFromSeed(keySeed[:])
		validators[i] = valConfig{name: name, key: key}
	}

	// Sentinel — baseline accept (permissive, ensures liveness)
	validators[0].validate = func(_, _, _, _ string, _ float64) (string, string) {
		return "accept", "baseline accept"
	}

	// Dedup — reject duplicate content
	validators[1].validate = func(_, hash, _, _ string, _ float64) (string, string) {
		exists, err := memStore.FindByContentHash(ctx, hash)
		if err == nil && exists {
			return "reject", fmt.Sprintf("duplicate content (hash: %s)", hash[:8])
		}
		return "accept", "content is unique"
	}

	// Quality — reject low-quality or noise memories
	validators[2].validate = func(content, _, _, _ string, _ float64) (string, string) {
		if len(content) < 20 {
			return "reject", fmt.Sprintf("content too short (%d chars, minimum 20)", len(content))
		}
		lower := strings.ToLower(content)
		noisePatterns := []string{
			"user said hi", "user greeted", "session started",
			"brain online", "brain is awake", "no action taken",
			"user said morning", "new session started",
		}
		for _, p := range noisePatterns {
			if strings.Contains(lower, p) {
				return "reject", "low-value observation: matches noise pattern"
			}
		}
		if strings.HasPrefix(content, "[Task Reflection]") && len(content) < 60 {
			return "reject", "empty reflection header without substance"
		}
		return "accept", "content passes quality check"
	}

	// Consistency — reject inconsistent metadata
	validators[3].validate = func(_, _, domain, memType string, conf float64) (string, string) {
		if conf < 0.3 {
			return "reject", fmt.Sprintf("confidence too low (%.2f)", conf)
		}
		if memType == "fact" && conf < 0.7 {
			return "reject", fmt.Sprintf("facts require confidence >= 0.7 (got %.2f)", conf)
		}
		if domain == "" {
			return "reject", "domain required"
		}
		return "accept", "passes consistency check"
	}

	// Register all 4 in ABCI validator set
	valMap := make(map[string]int64)
	for _, v := range validators {
		pubKey := v.key.Public().(ed25519.PublicKey)
		agentID := hex.EncodeToString(pubKey)
		valMap[agentID] = 10
	}
	if err := app.RegisterAppValidators(valMap); err != nil {
		logger.Error().Err(err).Msg("failed to register app validators")
		return
	}

	// Wait for node startup
	time.Sleep(3 * time.Second)
	logger.Info().Int("count", 4).Msg("app validators started — BFT memory consensus active")

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
				contentHash := hex.EncodeToString(mem.ContentHash)
				for _, v := range validators {
					decision, rationale := v.validate(mem.Content, contentHash, mem.DomainTag, string(mem.MemoryType), mem.ConfidenceScore)

					// Create and sign vote transaction
					voteTx := &tx.ParsedTx{
						Type:      tx.TxTypeMemoryVote,
						Nonce:     uint64(time.Now().UnixNano()), //nolint:gosec
						Timestamp: time.Now(),
						MemoryVote: &tx.MemoryVote{
							MemoryID:  mem.MemoryID,
							Decision:  voteDecisionFromString(decision),
							Rationale: rationale,
						},
					}

					if signErr := tx.SignTx(voteTx, v.key); signErr != nil {
						logger.Debug().Err(signErr).Msg("failed to sign vote tx")
						continue
					}

					encoded, encErr := tx.EncodeTx(voteTx)
					if encErr != nil {
						logger.Debug().Err(encErr).Msg("failed to encode vote tx")
						continue
					}

					// Broadcast to CometBFT
					broadcastVoteTx(cometRPC, encoded, logger)
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
	resp, err := http.DefaultClient.Do(healthReq) //nolint:gosec // internal health check
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
	statsResp, err := http.DefaultClient.Do(statsReq)                                       //nolint:gosec // internal API call
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

// loadNodeSigningKey extracts the Ed25519 private key from CometBFT's priv_validator_key.json.
// Returns nil if the key cannot be loaded (dashboard will skip on-chain broadcasts).
func loadNodeSigningKey(keyFilePath string, logger zerolog.Logger) ed25519.PrivateKey {
	data, err := os.ReadFile(keyFilePath)
	if err != nil {
		logger.Warn().Err(err).Msg("cannot load validator key for dashboard consensus — on-chain agent broadcasts disabled")
		return nil
	}

	var keyDoc struct {
		PrivKey struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"priv_key"`
	}
	if err = json.Unmarshal(data, &keyDoc); err != nil {
		logger.Warn().Err(err).Msg("cannot parse validator key JSON for dashboard consensus")
		return nil
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyDoc.PrivKey.Value)
	if err != nil || len(keyBytes) != ed25519.PrivateKeySize {
		logger.Warn().Err(err).Int("key_len", len(keyBytes)).Msg("invalid validator key for dashboard consensus")
		return nil
	}

	return ed25519.PrivateKey(keyBytes)
}
