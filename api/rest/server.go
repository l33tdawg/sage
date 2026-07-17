package rest

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
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
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/federation"
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
	badgerStore *store.BadgerStore // On-chain state for access control
	accessStore store.AccessStore  // PostgreSQL access control queries
	orgStore    store.OrgStore     // Organization and federation queries
	agentStore  store.AgentStore   // Network agent registry (domain access enforcement)
	health      *metrics.HealthChecker
	logger      zerolog.Logger
	httpServer  *http.Server
	signingKey  ed25519.PrivateKey // Node-level key for signing on-chain txs
	// validatorSigningKeyConfigured is true only when signingKey came from the
	// live CometBFT priv-validator key (environment or explicit runtime wiring).
	// The legacy random fallback keeps non-governance embedded/test callers
	// source-compatible, but governance always fails closed unless this is true.
	validatorSigningKeyConfigured bool
	embedder                      embedding.Provider // Embedding provider (Ollama or hash)
	OnEvent                       EventCallback      // Optional: called when notable events occur
	suppCache                     SuppCacheWriter    // Bridges off-chain data (embeddings) to ABCI for consensus-first writes
	mempool                       *mempoolSampler    // TTL-cached CometBFT mempool depth for backpressure signals

	// nodeOperatorID is the hex-encoded ed25519 public key of the local
	// node operator (~/.sage/agent.key). When a request's X-Agent-ID
	// matches this value, the caller is presumed to be the node operator
	// and bypasses agent-isolation RBAC: the operator owns the node, so
	// the cross-agent visibility filter doesn't apply to them. Other
	// access gates (per-domain, classification) still run.
	// Empty string disables the bypass entirely (preserves pre-v7.1 behaviour).
	nodeOperatorID string

	// governanceOperatorID is deliberately separate from nodeOperatorID: read
	// visibility bypass and authority to make this validator mutate governance
	// are distinct capabilities. Empty disables every REST governance mutation.
	governanceOperatorID string
	// governanceDomainFn returns the consensus-derived app-v20 chain domain.
	// It is read for every governance mutation so a restored/state-synced node
	// never serves an authorization against stale process-local context.
	governanceDomainFn func() string

	// PreValidateFunc runs the per-node validation checks without on-chain submission.
	// Set during node startup. Returns per-check results.
	PreValidateFunc func(content, contentHash, domain, memType string, confidence float64) []PreValidateResult

	// postV8ForkFn is the off-consensus advisory accessor for the v8.0
	// access-control fork gate. REST handlers can't carry a deterministic
	// block height, so they consult this dynamic predicate (which the
	// SageApp populates from its cached chain height) every time they
	// invoke a fork-aware access check. nil means "pre-fork everywhere",
	// preserving v7.1.1-equivalent behaviour for callers that don't wire
	// the fork gate (e.g. tests, older deployments).
	postV8ForkFn func() bool

	// postV17ForNextTxFn reports whether a transaction broadcast now will
	// execute under app-v17 rules. Unlike the v8 read-side accessor, this flips
	// immediately after the activation block commits so delegated REST
	// transactions carry the proof envelope before their first eligible block.
	// nil preserves the legacy wire encoding used by tests and pre-v17 nodes.
	postV17ForNextTxFn func() bool
	// postV20ForNextTxFn prevents operator surfaces from constructing a scope
	// proposal before app-v20 consensus can apply it. nil is fail-closed.
	postV20ForNextTxFn func() bool

	// federation is the v11 OFF-consensus transport (recall proxy, co-commit
	// receipt exchange, cross-fed agreement setup). nil disables every
	// federation surface — handlers 501 and the recall merge is skipped, so
	// unwired deployments/tests behave exactly pre-v11.
	federation FederationService
}

// FederationService is the slice of the federation.Manager the REST layer
// consumes. An interface so handler tests can fake peers without TLS.
type FederationService interface {
	FanOutRecall(ctx context.Context, targets []string, qr *federation.QueryRequest) []federation.PeerRecallOutcome
	DeliverReceipts(ctx context.Context, sharedID string, height, commitTime int64) map[string]federation.DeliveryResult
	// StageRemoteCA writes the remote CA to a pending sidecar and returns its
	// pin plus commit/rollback closures; the JOIN handler commits only after
	// the terms tx is authorized on-chain (so an unauthorized set can't
	// overwrite a live agreement's CA).
	StageRemoteCA(remoteChainID string, caPEM []byte) (pin []byte, commit func() error, rollback func(), err error)
	PeerStatus(ctx context.Context, remoteChainID string) (*federation.StatusResponse, error)
	LocalChainID() string

	// v11 JOIN ceremony drivers (off-consensus; the operator half). Host side:
	HostCreate(hostEndpoint string) (*federation.HostCreateResult, error)
	HostScanReturn(sessionID, returnURI string) error
	HostSessionStatus(sessionID string) (*federation.HostSessionView, error)
	HostApprove(sessionID, typedCode string, grant federation.ScopeWire) error
	HostAbort(sessionID string) error
	// Guest side:
	GuestScan(ctx context.Context, uri, guestEndpoint string) (*federation.GuestScanResult, error)
	GuestRequest(ctx context.Context, sessionID, guestEndpoint string, scope federation.ScopeWire) (*federation.GuestRequestResult, error)
	GuestConfirm(ctx context.Context, sessionID, guestEndpoint string, hostScope federation.ScopeWire) (string, error)

	// v11.5 domain sync: per-peer anti-entropy bookkeeping for the status surface.
	SyncReconcileInfo(remoteChainID string) (federation.SyncReconcileStatus, bool)
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
// It loads the node's signing key from VALIDATOR_KEY_FILE env var (CometBFT
// priv_validator_key.json format). A random compatibility key is retained for
// older embedded callers when the file is absent or invalid, but governance
// mutations remain disabled until a real validator key is explicitly wired.
func NewServer(cometbftRPC string, memStore store.MemoryStore, scoreStore store.ValidatorScoreStore, badgerStore *store.BadgerStore, health *metrics.HealthChecker, logger zerolog.Logger, embedProvider embedding.Provider) *Server {
	signingKey, signingKeyConfigured := loadValidatorSigningKey(logger)

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
		cometbftRPC:                   cometbftRPC,
		store:                         memStore,
		scoreStore:                    scoreStore,
		badgerStore:                   badgerStore,
		accessStore:                   accessStore,
		orgStore:                      orgStore,
		agentStore:                    agentStore,
		health:                        health,
		logger:                        logger,
		signingKey:                    signingKey,
		validatorSigningKeyConfigured: signingKeyConfigured,
		embedder:                      embedProvider,
		mempool:                       newMempoolSampler(cometbftRPC, DefaultMempoolMaxTxs),
	}
	s.router = s.setupRouter()
	return s
}

// SetSuppCache wires the supplementary data cache used for consensus-first writes.
// Must be called before the server starts accepting requests.
func (s *Server) SetSuppCache(cache SuppCacheWriter) {
	s.suppCache = cache
}

// SetMempoolCap wires the node's REAL runtime CometBFT mempool size
// (cometCfg.Mempool.Size in cmd/sage-gui/node.go) so mempool_pct in the
// backpressure signals is computed against the actual cap rather than the
// DefaultMempoolMaxTxs fallback. Call before the server starts accepting
// requests. Non-positive values are ignored.
func (s *Server) SetMempoolCap(n int) {
	s.mempool.setMaxTxs(n)
}

// SetPostV8ForkAccessor wires a dynamic predicate that reports whether the
// v8.0 access-control fork has activated. The function is invoked on each
// access check that fork-gates its semantics. Pass app.IsPostV8Fork from the
// owning SageApp at server-wire-up time. nil disables the gate (everything
// stays pre-fork — v7.1.1-equivalent).
func (s *Server) SetPostV8ForkAccessor(fn func() bool) {
	s.postV8ForkFn = fn
}

// isPostV8Fork is the internal accessor REST handlers use when calling
// HasAccessMultiOrg. Returns false (pre-fork) when no accessor is wired.
func (s *Server) isPostV8Fork() bool {
	if s.postV8ForkFn == nil {
		return false
	}
	return s.postV8ForkFn()
}

// SetPostV17ForNextTxAccessor wires the dynamic app-v17 transaction-envelope
// gate. Callers should pass app.IsAppV17ActiveForNextTx.
func (s *Server) SetPostV17ForNextTxAccessor(fn func() bool) {
	s.postV17ForNextTxFn = fn
}

func (s *Server) isPostV17ForNextTx() bool {
	return s.postV17ForNextTxFn != nil && s.postV17ForNextTxFn()
}

// SetPostV20ForNextTxAccessor wires the dynamic app-v20 scope-construction and
// validator/chain-bound governance gate. Callers should pass
// app.IsAppV20ActiveForNextTx.
func (s *Server) SetPostV20ForNextTxAccessor(fn func() bool) {
	s.postV20ForNextTxFn = fn
}

func (s *Server) isPostV20ForNextTx() bool {
	return s.postV20ForNextTxFn != nil && s.postV20ForNextTxFn()
}

// loadValidatorSigningKey loads the CometBFT validator private key so that
// vote transactions are signed by the same identity in the validator set.
// This is critical for quorum: checkAndApplyQuorum matches votes by validator ID.
func loadValidatorSigningKey(logger zerolog.Logger) (ed25519.PrivateKey, bool) {
	keyFile := os.Getenv("VALIDATOR_KEY_FILE")
	if keyFile == "" {
		logger.Warn().Msg("VALIDATOR_KEY_FILE not set — generating compatibility signing key (governance disabled)")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk, false
	}

	data, err := os.ReadFile(keyFile) //nolint:gosec // keyFile is from trusted config
	if err != nil {
		logger.Error().Err(err).Str("file", keyFile).Msg("failed to read validator key file — using compatibility key (governance disabled)")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk, false
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
		logger.Error().Err(err).Msg("failed to parse validator key JSON — using compatibility key (governance disabled)")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk, false
	}

	keyBytes, err := base64.StdEncoding.DecodeString(keyDoc.PrivKey.Value)
	if err != nil || len(keyBytes) != ed25519.PrivateKeySize {
		logger.Error().Err(err).Int("key_len", len(keyBytes)).Msg("invalid validator key — using compatibility key (governance disabled)")
		_, sk, _ := ed25519.GenerateKey(nil)
		return sk, false
	}

	sk := ed25519.PrivateKey(keyBytes)
	pub, _ := sk.Public().(ed25519.PublicKey)
	pubHex := fmt.Sprintf("%x", pub)
	logger.Info().Str("validator_id", pubHex[:16]+"...").Msg("loaded CometBFT validator signing key")
	return sk, true
}

// SetValidatorSigningKey injects the concrete private-validator key owned by
// the embedding runtime. Governance never trusts the random compatibility key
// created by NewServer, so sage-gui/amid must call this before serving.
func (s *Server) SetValidatorSigningKey(key ed25519.PrivateKey) error {
	if len(key) != ed25519.PrivateKeySize {
		return fmt.Errorf("validator signing key has length %d, want %d", len(key), ed25519.PrivateKeySize)
	}
	s.signingKey = append(ed25519.PrivateKey(nil), key...)
	s.validatorSigningKeyConfigured = true
	return nil
}

// DisableValidatorSigningKey explicitly closes the governance gateway. The
// embedding runtimes call this before attempting authoritative key injection,
// preventing NewServer's legacy VALIDATOR_KEY_FILE fallback from remaining
// enabled when an explicit --home/--validator-key-file load fails.
func (s *Server) DisableValidatorSigningKey() {
	s.validatorSigningKeyConfigured = false
}

// SetFederation wires the v11 federation transport. Must be called before the
// server starts accepting requests (same contract as SetSuppCache). nil keeps
// every federation surface disabled.
func (s *Server) SetFederation(f FederationService) {
	s.federation = f
}

// SetNodeOperatorID records the hex-encoded ed25519 public key that
// identifies the local node operator (~/.sage/agent.key). Requests signed
// with this key bypass agent-isolation RBAC on read paths since the
// operator owns the node and shouldn't be subject to cross-agent visibility
// filtering. Domain access and classification gates still apply.
//
// Empty string disables the bypass (default — pre-v7.1 behaviour).
func (s *Server) SetNodeOperatorID(id string) {
	s.nodeOperatorID = id
}

// NodeOperatorID returns the configured operator ID for diagnostic use.
func (s *Server) NodeOperatorID() string {
	return s.nodeOperatorID
}

// SetGovernanceOperatorID configures the sole HTTP signer allowed to ask this
// validator to propose, vote, or cancel. The canonical lowercase encoding
// avoids rejecting valid uppercase hex identities after middleware verification.
func (s *Server) SetGovernanceOperatorID(id string) error {
	id = strings.TrimSpace(id)
	if id == "" {
		s.governanceOperatorID = ""
		return nil
	}
	pub, err := auth.AgentIDToPublicKey(id)
	if err != nil {
		return fmt.Errorf("governance operator id: %w", err)
	}
	s.governanceOperatorID = auth.PublicKeyToAgentID(pub)
	return nil
}

// GovernanceOperatorID returns the canonical configured governance operator.
func (s *Server) GovernanceOperatorID() string {
	return s.governanceOperatorID
}

// SetGovernanceDomainAccessor wires the committed app-v20 chain domain used to
// bind delegated operator proofs to this chain. nil/empty fails closed once
// app-v20 is active.
func (s *Server) SetGovernanceDomainAccessor(fn func() string) {
	s.governanceDomainFn = fn
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
		AllowedHeaders:   []string{"Accept", "Content-Type", "X-Agent-ID", "X-Signature", "X-Timestamp", "X-Nonce"},
		ExposedHeaders:   []string{"X-Request-ID", "X-Sage-Mempool-Pct", "Retry-After"},
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
		r.Post("/v1/memory/hybrid", s.handleHybridSearchMemory)
		r.Get("/v1/memory/{memory_id}", s.handleGetMemory)
		r.Post("/v1/memory/{memory_id}/vote", s.handleVoteMemory)
		r.Post("/v1/memory/{memory_id}/challenge", s.handleChallengeMemory)
		r.Post("/v1/memory/{memory_id}/forget", s.handleForgetMemory)
		r.Post("/v1/memory/{memory_id}/reinstate", s.handleReinstateMemory)
		r.Post("/v1/memory/{memory_id}/corroborate", s.handleCorroborateMemory)
		r.Put("/v1/memory/{memory_id}/task-status", s.handleUpdateTaskStatus)
		r.Post("/v1/memory/link", s.handleLinkMemories)
		r.Get("/v1/memory/tasks", s.handleGetOpenTasks)
		r.Get("/v1/memory/list", s.handleListMemoriesAuth)
		r.Get("/v1/memory/timeline", s.handleTimelineAuth)
		r.Post("/v1/memory/pre-validate", s.handlePreValidate)

		// v11 federation transport (all OFF-consensus; consensus effects go
		// through ordinary signed txs broadcast to CometBFT)
		r.Post("/v1/federation/cross", s.handleCrossFedSet)
		r.Get("/v1/federation/cross", s.handleCrossFedList)
		r.Post("/v1/federation/cross/{chain_id}/revoke", s.handleCrossFedRevoke)
		r.Get("/v1/federation/cross/{chain_id}/status", s.handleCrossFedPeerStatus)
		r.Post("/v1/federation/cross/{chain_id}/write", s.handleCrossFedWrite)
		// v11.6 host-controlled domain sync (off-consensus; operator-only)
		r.Put("/v1/federation/cross/{chain_id}/sync", s.handleSyncDomainsSet)
		r.Get("/v1/federation/cross/{chain_id}/sync", s.handleSyncDomainsGet)
		r.Get("/v1/federation/cross/{chain_id}/sync/status", s.handleSyncStatus)

		// v11 real-TOTP JOIN ceremony - the operator's localhost control surface
		// (the guided guest/host wizards). Node-operator-only; off-consensus.
		r.Post("/v1/federation/join/host/create", s.handleJoinHostCreate)
		r.Post("/v1/federation/join/host/scan-return", s.handleJoinHostScanReturn)
		r.Get("/v1/federation/join/host/{session_id}", s.handleJoinHostStatus)
		r.Post("/v1/federation/join/host/{session_id}/approve", s.handleJoinHostApprove)
		r.Post("/v1/federation/join/host/{session_id}/abort", s.handleJoinHostAbort)
		r.Post("/v1/federation/join/guest/scan", s.handleJoinGuestScan)
		r.Post("/v1/federation/join/guest/request", s.handleJoinGuestRequest)
		r.Post("/v1/federation/join/guest/confirm", s.handleJoinGuestConfirm)
		r.Post("/v1/cocommit/submit", s.handleCoCommitSubmit)
		r.Post("/v1/cocommit/{shared_id}/receipt/send", s.handleCoCommitReceiptSend)
		r.Get("/v1/cocommit/{shared_id}/status", s.handleCoCommitStatus)

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

		// Chain backpressure signal — TTL-cached mempool depth vs the runtime
		// cap so writers can self-throttle without polling raw CometBFT RPC.
		r.Get("/v1/chain/backpressure", s.handleChainBackpressure)

		// Embedding endpoints (local Ollama, no cloud)
		r.Post("/v1/embed", s.handleEmbed)
		r.Get("/v1/embed/info", s.handleEmbedInfo)

		// Access control endpoints
		r.Post("/v1/access/request", s.handleAccessRequest)
		r.Post("/v1/access/grant", s.handleAccessGrant)
		r.Post("/v1/access/revoke", s.handleAccessRevoke)
		r.Get("/v1/access/grants/{agent_id}", s.handleListGrants)
		r.Post("/v1/domain/register", s.handleDomainRegister)
		r.Post("/v1/domain/reassign", s.handleDomainReassign)
		r.Get("/v1/domain/{name}", s.handleGetDomain)

		// Organization endpoints
		r.Post("/v1/org/register", s.handleOrgRegister)
		r.Get("/v1/org/by-name/{name}", s.handleGetOrgByName)
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
		r.Get("/v1/governance/context", s.handleGovernanceContext)
		r.Post("/v1/governance/propose", s.handleGovPropose)
		r.Post("/v1/governance/vote", s.handleGovVote)
		r.Post("/v1/governance/cancel", s.handleGovCancel)

		// v11.9 canonical scope topology (operator/admin read-only).
		r.Get("/v1/scopes", s.handleListScopes)
		r.Get("/v1/scopes/{scope_id}", s.handleGetScope)

		// HTTP MCP token management — admin issues tokens for external MCP
		// clients (ChatGPT, Cursor, etc.) that can't sign every request with
		// ed25519. The tokens themselves are validated by the bearer-auth
		// middleware on /v1/mcp/sse + /v1/mcp/streamable, NOT here.
		r.Post("/v1/mcp/tokens", s.handleMCPTokenIssue)
		r.Get("/v1/mcp/tokens", s.handleMCPTokenList)
		r.Delete("/v1/mcp/tokens/{id}", s.handleMCPTokenRevoke)
	})

	return r
}

// embedAgentAuth copies the authenticated agent's cryptographic proof from the
// request context into the ParsedTx. This allows ABCI to independently verify
// the agent's identity on-chain — no trust in REST payload fields needed.
func (s *Server) embedAgentAuth(ctx context.Context, ptx *tx.ParsedTx) {
	proof := middleware.ContextAgentAuth(ctx)
	if proof == nil {
		return
	}
	ptx.AgentPubKey = proof.PubKey
	ptx.AgentSig = proof.Signature
	ptx.AgentTimestamp = proof.Timestamp
	ptx.AgentBodyHash = proof.BodyHash
	ptx.AgentNonce = proof.Nonce
	// Appending this field before activation would make a new binary produce
	// bytes an old binary cannot reproduce when it verifies the outer node
	// signature. Keep it absent until the activation block has committed.
	if s.isPostV17ForNextTx() {
		ptx.AgentRequest = append([]byte(nil), proof.CanonicalRequest...)
	}
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

// StartTLS begins listening with TLS on the given address.
// Certificates are loaded from the provided tls.Config (not from file paths).
func (s *Server) StartTLS(addr string, tlsConfig *tls.Config) error {
	s.httpServer = &http.Server{
		Addr:         addr,
		Handler:      s.router,
		TLSConfig:    tlsConfig,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
		IdleTimeout:  60 * time.Second,
	}
	s.logger.Info().Str("addr", addr).Msg("starting REST API server (TLS)")
	return s.httpServer.ListenAndServeTLS("", "") // Certs from TLSConfig.
}

// Shutdown gracefully shuts down the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
