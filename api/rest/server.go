package rest

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/cors"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// Server is the SAGE REST API server.
// EventCallback is a function that receives event notifications (type, memoryID, domain, content, data).
// Used to bridge REST API events to the dashboard SSE broadcaster.
// The data parameter carries rich detail (e.g., retrieved memory list for recall events).
type EventCallback func(eventType, memoryID, domain, content string, data any)

type Server struct {
	router      chi.Router
	cometbftRPC string
	store       store.MemoryStore
	scoreStore  store.ValidatorScoreStore
	badgerStore *store.BadgerStore       // On-chain state for access control
	accessStore store.AccessStore        // PostgreSQL access control queries
	orgStore    store.OrgStore           // Organization and federation queries
	agentStore  store.AgentStore        // Network agent registry (domain access enforcement)
	health      *metrics.HealthChecker
	logger      zerolog.Logger
	httpServer  *http.Server
	signingKey  ed25519.PrivateKey      // Node-level key for signing on-chain txs
	embedder    embedding.Provider       // Embedding provider (Ollama or hash)
	OnEvent     EventCallback           // Optional: called when notable events occur
	suppCache   SuppCacheWriter         // Bridges off-chain data (embeddings) to ABCI for consensus-first writes

	// PreValidateFunc runs the 4 app validators without on-chain submission.
	// Set during node startup. Returns per-validator results.
	PreValidateFunc func(content, contentHash, domain, memType string, confidence float64) []PreValidateResult
}

// SuppCacheWriter is the interface the REST server uses to store supplementary data
// for the ABCI app to pick up during FinalizeBlock. Implemented by abci.SupplementaryCache.
type SuppCacheWriter interface {
	Put(memoryID string, data *memory.SupplementaryData)
}

// PreValidateResult holds one validator's pre-validation result.
type PreValidateResult struct {
	Validator string `json:"validator"`
	Decision  string `json:"decision"`
	Reason    string `json:"reason"`
}

// NewServer creates a new REST API server.
// It loads the node's signing key from VALIDATOR_KEY_FILE env var (CometBFT priv_validator_key.json format)
// so that vote transactions are signed by the same identity as the CometBFT validator.
// Falls back to a random key if the env var is not set.
func NewServer(cometbftRPC string, memStore store.MemoryStore, scoreStore store.ValidatorScoreStore, badgerStore *store.BadgerStore, health *metrics.HealthChecker, logger zerolog.Logger, embedProvider embedding.Provider) *Server {
	signingKey := loadValidatorSigningKey(logger)

	// Type-assert memStore to AccessStore if possible (PostgresStore implements both)
	var accessStore store.AccessStore
	if as, ok := memStore.(store.AccessStore); ok {
		accessStore = as
	}

	// Type-assert memStore to OrgStore if possible (PostgresStore implements all three)
	var orgStore store.OrgStore
	if os, ok := memStore.(store.OrgStore); ok {
		orgStore = os
	}

	// Type-assert memStore to AgentStore for domain access enforcement
	var agentStore store.AgentStore
	if as, ok := memStore.(store.AgentStore); ok {
		agentStore = as
	}

	s := &Server{
		cometbftRPC: cometbftRPC,
		store:       memStore,
		scoreStore:  scoreStore,
		badgerStore: badgerStore,
		accessStore: accessStore,
		orgStore:    orgStore,
		agentStore:  agentStore,
		health:      health,
		logger:      logger,
		signingKey:  signingKey,
		embedder:    embedProvider,
	}
	s.router = s.setupRouter()
	return s
}

// SetSuppCache wires the supplementary data cache used for consensus-first writes.
// Must be called before the server starts accepting requests.
func (s *Server) SetSuppCache(cache SuppCacheWriter) {
	s.suppCache = cache
}

// loadValidatorSigningKey loads the CometBFT validator private key so that
// vote transactions are signed by the same identity in the validator set.
// This is critical for quorum: checkAndApplyQuorum matches votes by validator ID.
func loadValidatorSigningKey(logger zerolog.Logger) ed25519.PrivateKey {
	keyFile := os.Getenv("VALIDATOR_KEY_FILE")
	if keyFile == "" {
		logger.Warn().Msg("VALIDATOR_KEY_FILE not set — generating random signing key (quorum will not work)")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk
	}

	data, err := os.ReadFile(keyFile) //nolint:gosec // keyFile is from trusted config
	if err != nil {
		logger.Error().Err(err).Str("file", keyFile).Msg("failed to read validator key file — using random key")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk
	}

	// CometBFT priv_validator_key.json format:
	// { "priv_key": { "type": "tendermint/PrivKeyEd25519", "value": "<base64 of 64-byte ed25519 key>" } }
	var keyDoc struct {
		PrivKey struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"priv_key"`
	}
	if err = json.Unmarshal(data, &keyDoc); err != nil {
		logger.Error().Err(err).Msg("failed to parse validator key JSON — using random key")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyDoc.PrivKey.Value)
	if err != nil || len(keyBytes) != ed25519.PrivateKeySize {
		logger.Error().Err(err).Int("key_len", len(keyBytes)).Msg("invalid validator key — using random key")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk
	}

	sk := ed25519.PrivateKey(keyBytes)
	pub, _ := sk.Public().(ed25519.PublicKey)
	pubHex := fmt.Sprintf("%x", pub)
	logger.Info().Str("validator_id", pubHex[:16]+"...").Msg("loaded CometBFT validator signing key")
	return sk
}

// setupRouter configures the chi router with middleware and routes.
func (s *Server) setupRouter() chi.Router {
	r := chi.NewRouter()

	// Global middleware
	r.Use(middleware.RequestLogger)
	r.Use(middleware.RateLimitMiddleware())
	corsOrigins := []string{"*"}
	if origins := os.Getenv("CORS_ALLOWED_ORIGINS"); origins != "" {
		corsOrigins = strings.Split(origins, ",")
	}
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   corsOrigins,
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS"},
		AllowedHeaders:   []string{"Accept", "Content-Type", "X-Agent-ID", "X-Signature", "X-Timestamp"},
		ExposedHeaders:   []string{"X-Request-ID"},
		AllowCredentials: false,
		MaxAge:           300,
	}))

	// Public endpoints (no auth)
	r.Get("/health", s.health.HealthHandler)
	r.Get("/ready", s.health.ReadinessHandler)

	// Public read-only agent identity endpoints (no auth required)
	r.Get("/v1/agents", s.handleListRegisteredAgents)

	// Authenticated API routes
	r.Group(func(r chi.Router) {
		r.Use(middleware.Ed25519AuthMiddleware)

		// Memory endpoints
		r.Post("/v1/memory/submit", s.handleSubmitMemory)
		r.Post("/v1/memory/query", s.handleQueryMemory)
			r.Post("/v1/memory/search", s.handleSearchMemory)
		r.Get("/v1/memory/{memory_id}", s.handleGetMemory)
		r.Post("/v1/memory/{memory_id}/vote", s.handleVoteMemory)
		r.Post("/v1/memory/{memory_id}/challenge", s.handleChallengeMemory)
		r.Post("/v1/memory/{memory_id}/corroborate", s.handleCorroborateMemory)
		r.Put("/v1/memory/{memory_id}/task-status", s.handleUpdateTaskStatus)
		r.Post("/v1/memory/link", s.handleLinkMemories)
		r.Get("/v1/memory/tasks", s.handleGetOpenTasks)
		r.Get("/v1/memory/list", s.handleListMemoriesAuth)
		r.Get("/v1/memory/timeline", s.handleTimelineAuth)
		r.Post("/v1/memory/pre-validate", s.handlePreValidate)

		// Agent endpoints
		r.Get("/v1/agent/me", s.handleGetAgent)

		// On-chain agent identity endpoints
		r.Post("/v1/agent/register", s.handleAgentRegister)
		r.Put("/v1/agent/update", s.handleAgentUpdate)
		r.Get("/v1/agent/{id}", s.handleGetRegisteredAgent)
		r.Put("/v1/agent/{id}/permission", s.handleAgentSetPermission)

		// Validator endpoints
		r.Get("/v1/validator/pending", s.handleGetPending)
		r.Get("/v1/validator/epoch", s.handleGetEpoch)

		// Embedding endpoints (local Ollama, no cloud)
		r.Post("/v1/embed", s.handleEmbed)
			r.Get("/v1/embed/info", s.handleEmbedInfo)

		// Access control endpoints
		r.Post("/v1/access/request", s.handleAccessRequest)
		r.Post("/v1/access/grant", s.handleAccessGrant)
		r.Post("/v1/access/revoke", s.handleAccessRevoke)
		r.Get("/v1/access/grants/{agent_id}", s.handleListGrants)
		r.Post("/v1/domain/register", s.handleDomainRegister)
		r.Get("/v1/domain/{name}", s.handleGetDomain)

		// Organization endpoints
		r.Post("/v1/org/register", s.handleOrgRegister)
		r.Get("/v1/org/{org_id}", s.handleGetOrg)
		r.Get("/v1/org/{org_id}/members", s.handleListOrgMembers)
		r.Post("/v1/org/{org_id}/member", s.handleOrgAddMember)
		r.Delete("/v1/org/{org_id}/member/{agent_id}", s.handleOrgRemoveMember)
		r.Post("/v1/org/{org_id}/clearance", s.handleOrgSetClearance)

		// Federation endpoints
		r.Post("/v1/federation/propose", s.handleFederationPropose)
		r.Post("/v1/federation/{fed_id}/approve", s.handleFederationApprove)
		r.Post("/v1/federation/{fed_id}/revoke", s.handleFederationRevoke)
		r.Get("/v1/federation/{fed_id}", s.handleGetFederation)
		r.Get("/v1/federation/active/{org_id}", s.handleListFederations)

		// Department endpoints
		r.Post("/v1/org/{org_id}/dept", s.handleDeptRegister)
		r.Get("/v1/org/{org_id}/dept/{dept_id}", s.handleGetDept)
		r.Get("/v1/org/{org_id}/depts", s.handleListOrgDepts)
		r.Post("/v1/org/{org_id}/dept/{dept_id}/member", s.handleDeptAddMember)
		r.Delete("/v1/org/{org_id}/dept/{dept_id}/member/{agent_id}", s.handleDeptRemoveMember)
		r.Get("/v1/org/{org_id}/dept/{dept_id}/members", s.handleListDeptMembers)

		// Pipeline endpoints
		r.Post("/v1/pipe/send", s.handlePipeSend)
		r.Get("/v1/pipe/inbox", s.handlePipeInbox)
		r.Put("/v1/pipe/{pipe_id}/claim", s.handlePipeClaim)
		r.Put("/v1/pipe/{pipe_id}/result", s.handlePipeResult)
		r.Get("/v1/pipe/{pipe_id}", s.handlePipeStatus)
		r.Get("/v1/pipe/results", s.handlePipeResults)

		// Governance endpoints
		r.Post("/v1/governance/propose", s.handleGovPropose)
		r.Post("/v1/governance/vote", s.handleGovVote)
		r.Post("/v1/governance/cancel", s.handleGovCancel)
	})

	return r
}

// embedAgentAuth copies the authenticated agent's cryptographic proof from the
// request context into the ParsedTx. This allows ABCI to independently verify
// the agent's identity on-chain — no trust in REST payload fields needed.
func embedAgentAuth(ctx context.Context, ptx *tx.ParsedTx) {
	proof := middleware.ContextAgentAuth(ctx)
	if proof == nil {
		return
	}
	ptx.AgentPubKey = proof.PubKey
	ptx.AgentSig = proof.Signature
	ptx.AgentTimestamp = proof.Timestamp
	ptx.AgentBodyHash = proof.BodyHash
	ptx.AgentNonce = proof.Nonce
}

// Router returns the underlying chi router for testing.
func (s *Server) Router() chi.Router {
	return s.router
}

// Start begins listening on the given address.
func (s *Server) Start(addr string) error {
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	s.logger.Info().Str("addr", addr).Msg("starting REST API server")
	return s.httpServer.ListenAndServe()
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
