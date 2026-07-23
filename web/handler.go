package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/vault"
)

// errDeprecatedNoFTS forces handleListMemories onto the keyword pool-scan path
// for status=deprecated: deprecated rows are deliberately absent from the FTS
// index, so an FTS MATCH can never serve that filter.
var errDeprecatedNoFTS = errors.New("deprecated status uses keyword scan, not FTS")

// assetVerRe matches the ?v=NNN cache-busting token on served HTML/JS so the
// content-hash rewrite survives manual version bumps (a fixed literal rots the
// moment index.html's token is changed).
var assetVerRe = regexp.MustCompile(`\?v=\d+`)

// PreferencesStore defines methods for preferences and cleanup operations.
type PreferencesStore interface {
	GetPreference(ctx context.Context, key string) (string, error)
	SetPreference(ctx context.Context, key, value string) error
	GetAllPreferences(ctx context.Context) (map[string]string, error)
	GetCleanupCandidates(ctx context.Context, observationTTLDays int, sessionTTLDays int, staleThreshold float64) ([]*memory.MemoryRecord, error)
	DeprecateMemories(ctx context.Context, memoryIDs []string) (int, error)
}

// Embedder generates vector embeddings for text content. The dashboard
// imports + task-planning paths only need Embed; the optional methods below
// let /v1/dashboard/health report which provider is configured and whether
// it is reachable. Any value satisfying embedding.Provider (Ollama, OpenAI-
// compatible, Hash) plugs in unchanged via SetEmbedder.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// embedderProvider is the dashboard-side view of the optional methods an
// embedding.Provider exposes. handleHealth type-asserts h.embedder to this
// interface so the status pill can dispatch on the actual configured
// provider (Ollama vs openai-compatible vs hash) instead of hard-coding an
// Ollama probe at localhost:11434 — the bug this exists to fix.
type embedderProvider interface {
	Dimension() int
	Ready() bool
	Semantic() bool
}

// rerankerInfoProvider is implemented by SQLiteStore to expose optional
// reranker config to /health. handleHealth type-asserts h.store to this
// interface so stores without a reranker simply omit the key.
type rerankerInfoProvider interface {
	RerankerInfo() (enabled bool, model string, url string)
}

// DashboardHandler serves the CEREBRUM dashboard UI and its API endpoints.
type DashboardHandler struct {
	store     store.MemoryStore
	prefStore PreferencesStore
	embedder  Embedder
	SSE       *SSEBroadcaster
	Version   string
	BootID    string // unique per serve process; restart verification must observe a change
	// NodeOperatorAgentID is the identity actually held by HTTP MCP's signing
	// key. OAuth/wizard bearer metadata must use this exact ID.
	NodeOperatorAgentID string

	// RequestRestart asks the main serve lifecycle to drain listeners, stop
	// consensus and close stores before the process image is replaced. nil means
	// this embedding cannot restart safely in-process.
	RequestRestart func() error
	// RunBackground binds operator-triggered jobs to the node lifecycle. The
	// production node cancels and joins these before closing stores; nil keeps
	// the lightweight test/embed behavior.
	RunBackground func(func(context.Context))
	// ScopedProjectionRebuildFn replays AppHash-covered v11.9 content into the
	// local serving projection after a locked vault becomes writable. nil keeps
	// pre-v11.9 embeddings/tests unchanged.
	ScopedProjectionRebuildFn func(context.Context) (int, error)

	// graphCache memoises the expensive /memory/graph response with a
	// stale-while-revalidate policy: the first load computes synchronously, every
	// repeat load is served instantly from cache while a background refresh keeps
	// it warm. Keyed by query params + RBAC scope so it never leaks across agents.
	graphCacheMu     sync.Mutex
	graphCache       map[string]*graphCacheEntry
	ExecPath         string // path to sage-gui binary, used by /v1/mcp-config
	RESTAddr         string // configured REST listen address (cfg.RESTAddr), surfaced read-only in Settings > Connection
	MCPTLSAddr       string // configured MCP TLS (bearer) listen address, used by /v1/dashboard/connect/remote-url to report whether the node is reachable from another computer (loopback bind = local-only)
	FederationAddr   string // effective federation mTLS listen address, used to generate JOIN codes with the port the node actually bound
	TunnelClient     TunnelClientManager
	Encrypted        atomic.Bool
	VaultLocked      atomic.Bool // true when encryption is enabled but vault hasn't been unlocked yet
	UpdateInProgress atomic.Bool
	updateStateMu    sync.RWMutex
	updateState      map[string]string
	agentReplayMu    sync.Mutex
	agentReplaySeen  map[string]time.Time

	// Auth — only active when Encrypted is true.
	VaultKeyPath  string
	sessions      sync.Map // token -> expiry time
	loginAttempts sync.Map // IP -> []time.Time

	// SaveEncryptionConfig persists encryption enabled/disabled state to config.yaml.
	SaveEncryptionConfig func(enabled bool) error

	// Redeployer — when set, write endpoints return 503 during active redeployment
	// and the /redeploy endpoint can trigger chain redeployment.
	// Must implement RedeployChecker (for the guard middleware).
	// Also provides Deploy/GetStatus for the network redeploy endpoint.
	Redeployer RedeployOrchestrator

	// Pairing — ephemeral pairing code store for LAN agent setup.
	Pairing *PairingStore

	// CometBFT consensus — when set, agent create/update operations are also
	// broadcast as on-chain transactions through CometBFT consensus.
	CometBFTRPC string
	SigningKey  ed25519.PrivateKey

	// AdminSigningKey is the operator/admin key (~/.sage/agent.key), the
	// on-chain genesis admin. SigningKey above is the CometBFT validator key,
	// which is NOT a registered admin, so the admin-gated RBAC txs (GovPropose,
	// DomainReassign) are signed with this key instead. nil disables the
	// domain-reassign endpoint. Memory submits keep using SigningKey so
	// authorship (submitting_agent) stays immutable.
	AdminSigningKey ed25519.PrivateKey
	// ResolveAgentKeyFn maps an on-chain agent id to the local Ed25519 key that
	// produces it (over the keys this node holds), or (nil,false) for a remote
	// agent. Used to sign owner-scoped AccessGrant/AccessRevoke AS the domain
	// owner. nil => owner-scoped grants are always deferred.
	ResolveAgentKeyFn func(agentID string) (ed25519.PrivateKey, bool)

	// BadgerStore — when set, on-chain RBAC is enforced on dashboard endpoints
	// when requests include X-Agent-ID headers (i.e. MCP agent requests).
	BadgerStore *store.BadgerStore

	// Federation — the v11 JOIN ceremony driver (off-consensus). When set, the
	// cookie-authed /v1/dashboard/federation/* proxy drives the guided guest/host
	// wizards by calling the federation Manager directly (the browser has a
	// dashboard session, not the operator's signing key, so it cannot hit the
	// agent-signed REST endpoints; this proxy IS the operator half).
	Federation FederationJoinDriver
	// federationPolicyMu serializes the multi-step deny-overlay -> consensus
	// grant/revoke -> final-policy transition. SQLite snapshots are individually
	// atomic, but two overlapping replacements must not interleave those phases.
	federationPolicyMu sync.Mutex

	// pendingImports holds parsed records from preview, keyed by import ID.
	pendingImports sync.Map // string -> *pendingImport

	// PreValidateFunc runs the per-node validation checks against proposed content
	// without submitting it on-chain. Set during node startup to share the vote logic.
	PreValidateFunc func(content, contentHash, domain, memType string, confidence float64) []PreValidateVote

	// PostV8ForkFn is the off-consensus advisory accessor for the v8.0
	// access-control fork gate. When set and returning true, dashboard
	// grant lookups walk the dotted-domain path (HasAccessOrAncestor)
	// instead of doing exact-match (HasAccess). nil keeps pre-fork
	// (v7.1.1-equivalent) semantics.
	PostV8ForkFn func() bool

	// ConnectFunc performs a same-machine one-click connect for a provider
	// (claude-code, codex, cursor, windsurf, claude-desktop): it writes the
	// provider's MCP config and returns the files touched. Wired in
	// cmd/sage-gui to connectProvider so the config-writer funcs (package main)
	// run without the web package importing them. nil disables the endpoint.
	ConnectFunc func(provider, path, token string) ([]ConnectFile, error)

	// NetworkJoin drives the LAN node-join ceremony (Phase 5b-3, Flow 3). The
	// two callbacks below let the package-web handler start a temporary LAN
	// listener and build the (secret-free) join bundle without importing the
	// cmd/sage-gui config/CometBFT writers. All three nil => the join endpoints
	// return 503.
	NetworkJoin *NetworkJoinStore
	// PairingListenerFn binds a temporary 0.0.0.0 listener serving the guest
	// pairing handler and returns a stop func + the bound port.
	PairingListenerFn func(handler http.Handler) (stop func(), port int, err error)
	// BuildJoinBundleFn assembles this host's join bundle (JSON) for a guest
	// that will reach the node at lanIP.
	BuildJoinBundleFn func(lanIP string) ([]byte, error)
	// QuorumEnabled reports whether this node is in network/quorum mode (P2P on
	// the LAN). A personal node binds P2P to loopback, so it must switch to
	// network mode before it can accept a joining peer.
	QuorumEnabled bool
	// reembed holds the state of the background re-embed job (server-side, decoupled
	// from any request so it survives client disconnects). See embeddings_handler.go.
	reembed reembedJob
	// SetEmbeddingProvider persists the active vector space ("ollama" or
	// "hash") in config.yaml; the node re-reads it after the controlled restart.
	SetEmbeddingProvider func(provider string) error
	// Rerankd manages the optional llama.cpp reranker sidecar (guided setup,
	// pinned model download, spawn/adopt/stop). Wired in cmd/sage-gui; nil
	// disables the /v1/dashboard/reranker/setup/* endpoints.
	Rerankd RerankdManager
	// Ollamad manages the local Ollama runtime used by smart memory. Wired in
	// cmd/sage-gui; nil leaves the older "bring your own Ollama" endpoints.
	Ollamad OllamadManager
	// ValidatorCountFn returns the live consensus validator count. When set it is
	// the authoritative signal for whether an agent op needs a full chain
	// redeploy (count > 1) or can be applied instantly (count <= 1) — more
	// accurate than QuorumEnabled, since a network-mode host with only
	// non-validator peers is still a single-validator chain.
	ValidatorCountFn func() int
	// AppV18ActiveFn reports whether the next committed tx executes under the
	// app-v18 global-admin domain-access override rules.
	AppV18ActiveFn func() bool
	// AppV19ActiveFn reports whether the app-v19 fork has activated (>= the
	// activation height). app-v19 is behavior-empty on the consensus path; its
	// sole effect is the OFF-consensus local-agents-default-READ flip below: once
	// active (and StrictRBAC is off), a non-admin agent may READ the operator's
	// UNCLASSIFIED domains it is not explicitly granted, instead of the fail-closed
	// deny. Nil (unwired) or false leaves the pre-fork explicit-allowlist behavior,
	// so a chain that has not activated app-v19 reads exactly as before.
	AppV19ActiveFn func() bool
	// AppV20ActiveFn reports whether a newly broadcast scope_action can execute
	// under app-v20. nil/false keeps the dashboard construction path disabled.
	AppV20ActiveFn func() bool
	// GovernanceDomainFn returns the committed app-v20 chain authorization
	// domain. Post-v20 dashboard governance fails closed when it is unavailable.
	GovernanceDomainFn func() string
	// StrictRBAC, when true, opts the operator out of the app-v19 default-read
	// flip: even with app-v19 active, a non-admin agent is confined to its explicit
	// DomainAccess allowlist. Sourced from cfg.RBAC.Strict; default false = local
	// agents default-READ the operator's unclassified domains once app-v19 activates.
	StrictRBAC bool
	// SetNetworkMode persists the network-mode flag to config.yaml (the node
	// then re-binds P2P to the LAN on the next restart). Wired in cmd/sage-gui.
	SetNetworkMode func(enabled bool) error

	// FederationEnabled is the current federation.enabled config value, surfaced
	// read-only in Settings. SetFederationEnabledFn persists a change to
	// config.yaml; the node starts/stops the inbound mTLS listener on the next
	// restart (the toggle re-execs, mirroring network mode). Both wired in
	// cmd/sage-gui; a nil setter disables the Settings toggle.
	FederationEnabled      bool
	SetFederationEnabledFn func(enabled bool) error

	// SetNetworkNameFn persists the friendly network label to config.yaml. Wired
	// in cmd/sage-gui; a nil setter disables renaming in Settings. The dashboard
	// also pushes the new name to the live federation Manager (SetNetworkName) so
	// a rename takes effect for the next join ceremony without a restart.
	SetNetworkNameFn func(name string) error

	// GuestJoin drives the JOINING node's "Join a network" ceremony (Flow 3,
	// guest side). GuestNodeIDFn returns this node's CometBFT p2p node id (for
	// the hello proof) and WritePendingJoinFn stages the decrypted bundle for
	// apply-on-next-restart. All nil => the guest join endpoints return 503.
	GuestJoin          *GuestJoinStore
	GuestNodeIDFn      func() (string, error)
	WritePendingJoinFn func(bundleJSON []byte) error
	// RemovePendingJoinFn un-stages a pending join (on cancel/backout).
	RemovePendingJoinFn func() error
}

// isPostV8Fork is the internal accessor — returns false when no fork gate is
// wired, keeping dashboard behaviour byte-identical to v7.1.1 by default.
func (h *DashboardHandler) isPostV8Fork() bool {
	if h.PostV8ForkFn == nil {
		return false
	}
	return h.PostV8ForkFn()
}

// RedeployOrchestrator extends RedeployChecker with deploy/status methods
// for the network redeploy endpoint. Implemented by *orchestrator.Redeployer.
type RedeployOrchestrator interface {
	RedeployChecker
	DeployOp(ctx context.Context, op, agentID string) error
	GetRedeployStatus(ctx context.Context) (active bool, operation, agentID string, err error)
	// GetLiveStatus reports the coarse run status (running|completed|failed|idle),
	// the current-or-last phase, and an error message on failure - derived from the
	// redeploy log so a poll can tell a completed redeploy from a failed one.
	GetLiveStatus(ctx context.Context) (status, currentPhase, operation, agentID, errMsg string, err error)
	// ClearStale clears a stuck/abandoned redeployment (refuses if one is genuinely
	// live). Returns the number of stale log rows cleared.
	ClearStale(ctx context.Context) (int, error)
	// QuickAgentOp applies an agent operation WITHOUT a chain redeployment, for a
	// single-validator (personal) node where the validator set never changes.
	QuickAgentOp(ctx context.Context, op, agentID string) error
}

// PreValidateVote represents a single validator's vote result from pre-validation.
type PreValidateVote struct {
	Validator string `json:"validator"`
	Decision  string `json:"decision"`
	Reason    string `json:"reason"`
}

// resolveAgentRBAC checks whether the request comes from an authenticated MCP agent
// (a signature-bound agent identity in request context) and resolves on-chain RBAC visibility.
// Returns (allowedAgents, seeAll). If no agent header or no BadgerStore, returns (nil, true)
// meaning no filtering (human dashboard user).
func (h *DashboardHandler) resolveAgentRBAC(r *http.Request) ([]string, bool) {
	agentID := verifiedDashboardAgentID(r.Context())
	if agentID == "" || h.BadgerStore == nil {
		return nil, true // Human dashboard — no filtering
	}

	agent, err := h.BadgerStore.GetRegisteredAgent(agentID)
	if err == nil && agent != nil {
		if agent.Role == "admin" {
			return nil, true
		}
		if agent.VisibleAgents == "*" {
			return nil, true
		}
		allowed := []string{agentID} // Always see own
		if agent.VisibleAgents != "" {
			var list []string
			if json.Unmarshal([]byte(agent.VisibleAgents), &list) == nil {
				allowed = append(allowed, list...)
			}
		}
		return allowed, false
	}

	// Not registered on-chain — isolated by default (own memories only)
	return []string{agentID}, false
}

// NewDashboardHandler creates a new dashboard handler.
func NewDashboardHandler(memStore store.MemoryStore, version string) *DashboardHandler {
	h := &DashboardHandler{
		store:           memStore,
		SSE:             NewSSEBroadcaster(),
		Version:         version,
		BootID:          uuid.NewString(),
		Pairing:         NewPairingStore(),
		NetworkJoin:     NewNetworkJoinStore(),
		GuestJoin:       NewGuestJoinStore(),
		updateState:     map[string]string{"step": "idle", "status": "idle", "message": ""},
		agentReplaySeen: make(map[string]time.Time),
	}
	// If the store implements PreferencesStore, wire it up.
	if ps, ok := memStore.(PreferencesStore); ok {
		h.prefStore = ps
	}
	return h
}

// SetEmbedder configures the embedding provider for import operations.
func (h *DashboardHandler) SetEmbedder(e Embedder) {
	h.embedder = e
}

func (h *DashboardHandler) runBackground(fn func(context.Context)) {
	if h.RunBackground != nil {
		h.RunBackground(fn)
		return
	}
	go fn(context.Background()) //nolint:gosec // embedded/test handler has no lifecycle owner
}

func (h *DashboardHandler) startPostUnlockRepairs() {
	h.runBackground(func(ctx context.Context) {
		if h.ScopedProjectionRebuildFn != nil {
			_, _ = h.ScopedProjectionRebuildFn(ctx)
		}
		_, _ = h.StartEmbeddingRepairIfNeeded(ctx)
	})
}

// handlePreValidate runs the per-node validation checks (dedup, quality,
// consistency) against proposed content without actually submitting it on-chain.
// Returns per-check results and the would-be accept outcome (the node votes accept
// iff all checks pass).
func (h *DashboardHandler) handlePreValidate(w http.ResponseWriter, r *http.Request) {
	if h.PreValidateFunc == nil {
		http.Error(w, `{"error":"pre-validation not configured"}`, http.StatusServiceUnavailable)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	var req struct {
		Content    string  `json:"content"`
		Domain     string  `json:"domain"`
		Type       string  `json:"type"`
		Confidence float64 `json:"confidence"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid request"}`, http.StatusBadRequest)
		return
	}

	// Compute content hash (same as memory submission)
	hash := sha256.Sum256([]byte(req.Content))
	contentHash := hex.EncodeToString(hash[:])

	votes := h.PreValidateFunc(req.Content, contentHash, req.Domain, req.Type, req.Confidence)

	acceptCount := 0
	for _, v := range votes {
		if v.Decision == "accept" {
			acceptCount++
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"accepted": acceptCount == len(votes), // the node votes accept iff every check passes
		"votes":    votes,
		"quorum":   fmt.Sprintf("%d/%d", acceptCount, len(votes)),
	})
}

const sessionCookieName = "sage_session"
const sessionTTL = 24 * time.Hour

// taskContentPrefix marks a memory as a task in its stored content. Kept in sync
// with internal/mcp (deliberately duplicated rather than importing internal/mcp
// into web just for this).
const taskContentPrefix = "[TASK] "

// applyTaskPrefix marks content as a task, idempotently — a task typed as
// "[TASK] ..." in the UI must not become "[TASK] [TASK] ...".
func applyTaskPrefix(content string) string {
	if strings.HasPrefix(content, taskContentPrefix) {
		return content
	}
	return taskContentPrefix + content
}

type verifiedDashboardAgentKey struct{}

func verifiedDashboardAgentID(ctx context.Context) string {
	agentID, _ := ctx.Value(verifiedDashboardAgentKey{}).(string)
	return agentID
}

// isCEREBRUMOperatorRequest distinguishes the local dashboard operator from a
// signed agent or an unauthenticated LAN process. Browser headers provide CSRF
// protection, not identity: non-loopback browsers must also carry a real
// encrypted-dashboard session. Older WebViews can omit Fetch Metadata and
// Origin on same-origin requests; those requests are admitted only when they
// come from loopback, target a safe local/IP Host, and carry a valid encrypted
// dashboard session. On encryption-off nodes operator mutations and full-board
// reads are therefore loopback-only.
func (h *DashboardHandler) isCEREBRUMOperatorRequest(r *http.Request) bool {
	if verifiedDashboardAgentID(r.Context()) != "" || !isLocalRequest(r) {
		return false
	}
	secFetch := strings.TrimSpace(r.Header.Get("Sec-Fetch-Site"))
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if secFetch == "" && origin == "" {
		if !isLoopbackRemote(r.RemoteAddr) || !hostIsLoopbackOrIP(r.Host) || !h.Encrypted.Load() {
			return false
		}
		cookie, err := r.Cookie(sessionCookieName)
		return err == nil && h.validSession(cookie.Value)
	}
	if isLoopbackRemote(r.RemoteAddr) {
		return true
	}
	if !h.Encrypted.Load() {
		return false
	}
	cookie, err := r.Cookie(sessionCookieName)
	return err == nil && h.validSession(cookie.Value)
}

// securityHeaders adds standard security headers to all responses.
func securityHeaders(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Security-Policy", "frame-ancestors 'none'")
		w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
		next.ServeHTTP(w, r)
	})
}

// RegisterRoutes mounts dashboard routes on the given router.
func (h *DashboardHandler) RegisterRoutes(r chi.Router) {
	// Use a group so securityHeaders doesn't conflict with already-registered routes on the parent router.
	r.Group(func(r chi.Router) {
		r.Use(securityHeaders)

		// Auth endpoints — always available (login page needs to load without auth).
		r.Post("/v1/dashboard/auth/login", h.handleLogin)
		r.Post("/v1/dashboard/auth/lock", h.handleLock)
		r.Get("/v1/dashboard/auth/check", h.handleAuthCheck)

		// Health is public (needed by CLI status command).
		r.Get("/v1/dashboard/health", h.handleHealth)
		// Validator set is public chain data — same publicness as /health.
		r.Get("/v1/dashboard/chain/validators", h.handleChainValidators)

		// MCP config — public so AI agents can self-configure.
		r.Get("/v1/mcp-config", h.handleMCPConfig)

		// Pairing redemption — unauthenticated (the code IS the auth).
		h.RegisterPairingRoutes(r)

		// Recovery — unauthenticated (the recovery key IS the auth).
		r.Post("/v1/dashboard/settings/ledger/recover", h.handleRecoverLedger)

		// Protected routes — auth middleware checks dynamically whether encryption is active.
		r.Group(func(r chi.Router) {
			r.Use(h.authMiddleware)
			// Redeploy guard — returns 503 for write endpoints during active redeployment.
			r.Use(redeployGuard(h.Redeployer))

			r.Get("/v1/dashboard/memory/list", h.handleListMemories)
			r.Get("/v1/dashboard/export", h.handleExport)
			r.Get("/v1/dashboard/memory/timeline", h.handleTimeline)
			r.Get("/v1/dashboard/memory/graph", h.handleGraph)
			r.Get("/v1/dashboard/stats", h.handleStats)

			// Embeddings setup — turn on the bundled semantic embedder + re-embed.
			h.RegisterEmbeddingsRoutes(r)

			// Reranker setup downloads+extracts+chmods a binary and spawns
			// llama-server as a subprocess, so it gets the same same-origin gate
			// as the other subprocess-spawning wizards (ChatGPT/federation/
			// network-join) on top of authMiddleware - a cross-origin tab must
			// never be able to drive subprocess execution.
			r.Group(func(r chi.Router) {
				r.Use(h.wizardSecurityGate)
				h.RegisterRerankerSetupRoutes(r)
			})

			r.Delete("/v1/dashboard/memory/{id}", h.handleDeleteMemory)
			r.Patch("/v1/dashboard/memory/{id}", h.handleUpdateMemory)
			r.Post("/v1/dashboard/memory/bulk", h.handleBulkUpdateMemories)
			r.Get("/v1/dashboard/events", h.SSE.ServeHTTP)
			r.Post("/v1/dashboard/import", h.handleImportUpload)
			r.Post("/v1/dashboard/import/preview", h.handleImportPreview)
			r.Post("/v1/dashboard/import/confirm", h.handleImportConfirm)

			// Recall settings (k-value, confidence threshold)
			r.Get("/v1/dashboard/settings/recall", h.handleGetRecallSettings)
			r.Post("/v1/dashboard/settings/recall", h.handleSaveRecallSettings)
			r.Get("/v1/dashboard/settings/reranker", h.handleGetReranker)
			r.Post("/v1/dashboard/settings/reranker", h.handleSaveReranker)
			r.Post("/v1/dashboard/settings/reranker/test", h.handleTestReranker)
			r.Get("/v1/dashboard/settings/reranker/detect", h.handleDetectReranker)
			r.Get("/v1/dashboard/settings/onboarding", h.handleGetOnboarding)
			r.Post("/v1/dashboard/settings/onboarding", h.handleSaveOnboarding)
			r.Get("/v1/dashboard/settings/federation", h.handleGetFederationSetting)
			r.With(h.federationOperatorGate).Post("/v1/dashboard/settings/federation", h.handleSetFederationSetting)
			r.Get("/v1/dashboard/settings/cleanup", h.handleGetCleanupSettings)
			r.Post("/v1/dashboard/settings/cleanup", h.handleSaveCleanupSettings)
			r.Post("/v1/dashboard/cleanup/run", h.handleRunCleanup)
			r.Get("/v1/dashboard/settings/boot-instructions", h.handleGetBootInstructions)
			r.Post("/v1/dashboard/settings/boot-instructions", h.handleSaveBootInstructions)
			r.Get("/v1/dashboard/settings/memory-mode", h.handleGetMemoryMode)
			r.Post("/v1/dashboard/settings/memory-mode", h.handleSaveMemoryMode)

			// Same-machine one-click connect — writes a provider's MCP config.
			r.Post("/v1/dashboard/connect/{provider}", h.handleConnectProvider)

			// Remote-connect (Flow 2) — reports how a tool on ANOTHER computer can
			// reach this node (public tunnel and/or LAN bind), so the frontend can
			// generate the right per-tool paste block or gate honestly when there is
			// no remote path yet.
			r.Get("/v1/dashboard/connect/remote-url", h.handleConnectRemoteURL)

			// LAN node-join ceremony (Flow 3) — operator side. Same-origin gated
			// on top of authMiddleware (like the wizard/federation routes) since
			// these routes open a LAN listener and reconfigure the node, so a
			// cross-origin tab must never drive them. The guest-facing half runs
			// on a temporary listener (see guestPairingHandler).
			r.Group(func(r chi.Router) {
				r.Use(h.wizardSecurityGate)
				h.RegisterNetworkJoinHostRoutes(r)
				h.RegisterNetworkJoinGuestRoutes(r)
			})

			// Task backlog
			r.Get("/v1/dashboard/tasks", h.handleGetTasks)
			r.Post("/v1/dashboard/tasks", h.handleCreateTaskDashboard)
			r.Put("/v1/dashboard/tasks/order", h.handleReorderTasksDashboard)
			r.Put("/v1/dashboard/tasks/{id}/status", h.handleUpdateTaskStatusDashboard)
			r.Put("/v1/dashboard/tasks/{id}/assign", h.handleAssignTask)
			r.Get("/v1/dashboard/task-notifications", h.handleTaskNotifications)

			// Tags
			r.Get("/v1/dashboard/tags", h.handleListTags)
			r.Get("/v1/dashboard/memory/{id}/tags", h.handleGetMemoryTags)
			r.Put("/v1/dashboard/memory/{id}/tags", h.handleSetMemoryTags)
			// Memory "train of thought" - powers the MRI click-to-explore.
			r.Get("/v1/dashboard/memory/{id}/related", h.handleMemoryRelated)

			// Auto-start (open at login)
			r.Get("/v1/dashboard/settings/autostart", h.handleGetAutostart)
			r.Post("/v1/dashboard/settings/autostart", h.handleSetAutostart)

			// Software Update
			r.Get("/v1/dashboard/settings/update/check", h.handleCheckUpdate)
			r.Get("/v1/dashboard/settings/update/status", h.handleGetUpdateStatus)
			r.Post("/v1/dashboard/settings/update/apply", h.handleApplyUpdate)
			r.Post("/v1/dashboard/settings/update/restart", h.handleRestart)

			// Synaptic Ledger (encryption vault) management
			r.Get("/v1/dashboard/settings/ledger", h.handleGetLedgerStatus)
			r.Post("/v1/dashboard/settings/ledger/enable", h.handleEnableLedger)
			r.Post("/v1/dashboard/settings/ledger/change-passphrase", h.handleChangePassphrase)
			r.Post("/v1/dashboard/settings/ledger/recovery-key", h.handleGetRecoveryKey)
			r.Post("/v1/dashboard/settings/ledger/recovery-key/confirm", h.handleConfirmRecoveryKeyBackup)
			r.Post("/v1/dashboard/settings/ledger/disable", h.handleDisableLedger)

			// Pre-validate endpoint — dry-run the per-node validation checks
			r.Post("/v1/memory/pre-validate", h.handlePreValidate)

			// Pipeline — agent-to-agent message bus
			r.Get("/v1/dashboard/pipeline", h.handlePipelineList)
			r.Get("/v1/dashboard/pipeline/stats", h.handlePipelineStats)
			r.Post("/v1/dashboard/pipeline/send", h.handlePipelineSend)

			// Network agent management routes
			h.RegisterNetworkRoutes(r)

			// Governance routes
			h.RegisterGovernanceRoutes(r)

			// ChatGPT setup wizard — orchestrates the OpenAI tunnel-client setup
			// from CEREBRUM so non-power-users can wire SAGE up to ChatGPT's
			// MCP connector without leaving the browser or opening a terminal.
			// Local-first orchestration only: SAGE installs/starts the local
			// client, while the user owns the OpenAI tunnel end-to-end.
			// wizardSecurityGate adds a strict same-origin check on top of
			// the parent authMiddleware so cross-origin browser tabs cannot
			// drive subprocess execution.
			r.Group(func(r chi.Router) {
				r.Use(h.wizardSecurityGate)
				h.RegisterChatGPTTunnelRoutes(r)
				h.RegisterChatGPTWizardRoutes(r)
			})

			// v11 real-TOTP federation JOIN ceremony proxy. Same-origin gated on
			// top of authMiddleware (like the ChatGPT wizard) because these routes
			// broadcast tx-33/tx-34 and dial remote peers on the operator's behalf,
			// so a cross-origin tab must never be able to drive them.
			r.Group(func(r chi.Router) {
				r.Use(h.wizardSecurityGate)
				h.registerFederationRoutes(r)
			})
		})

		// Launch endpoint — redirects to CEREBRUM dashboard.
		// The dock/tray app opens this URL; simple redirect avoids popup-blocker issues on macOS Tahoe+.
		r.Get("/ui/launch", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusFound)
		})
		// Browser-independent dashboard presence for the native macOS launcher.
		// Firefox has no useful AppleScript tab API, so the tray checks whether an
		// authenticated CEREBRUM tab already owns an SSE stream before deciding to
		// open another URL. This endpoint exposes only an aggregate local UI count.
		r.Get("/ui/presence", func(w http.ResponseWriter, r *http.Request) {
			if !isLoopbackRemote(r.RemoteAddr) {
				http.NotFound(w, r)
				return
			}
			clients := 0
			if h.SSE != nil {
				clients = h.SSE.ClientCount()
			}
			w.Header().Set("Cache-Control", "no-store")
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"active":  clients > 0,
				"clients": clients,
			})
		})

		// SPA - serve static files, fallback to index.html. SAGE_UI_DIR (dev only)
		// serves from disk instead of the embed, so UI edits show on a browser
		// reload with no rebuild; unset (production) uses the embedded assets.
		var staticFS fs.FS
		if dir := os.Getenv("SAGE_UI_DIR"); dir != "" {
			staticFS = os.DirFS(dir)
		} else {
			sub, _ := fs.Sub(StaticFS, "static")
			staticFS = sub
		}
		fileServer := http.FileServer(http.FS(staticFS))

		// Content-hash asset version for cache-busting. index.html's ?v token was
		// hardcoded (never bumped across releases) and app.js imports mri-brain.js
		// with NO version — so browsers/CDNs served stale JS after a deploy (the
		// fix only showed in incognito). This hash changes whenever any embedded
		// asset changes, so each build serves fresh asset URLs.
		assetVer := func() string {
			h := sha256.New()
			var files []string
			_ = fs.WalkDir(staticFS, ".", func(p string, d fs.DirEntry, err error) error {
				if err == nil && !d.IsDir() {
					files = append(files, p)
				}
				return nil
			})
			sort.Strings(files)
			for _, p := range files {
				if b, e := fs.ReadFile(staticFS, p); e == nil {
					h.Write([]byte(p))
					h.Write(b)
				}
			}
			return hex.EncodeToString(h.Sum(nil))[:12]
		}()

		r.Get("/ui/*", func(w http.ResponseWriter, r *http.Request) {
			// Strip /ui prefix
			path := strings.TrimPrefix(r.URL.Path, "/ui")
			if path == "" || path == "/" {
				path = "/index.html"
			}

			// Try to serve the file directly
			f, err := staticFS.(fs.ReadFileFS).ReadFile(strings.TrimPrefix(path, "/")) //nolint:errcheck
			if err != nil {
				// Fallback to index.html for SPA routing
				r.URL.Path = "/index.html"
				fileServer.ServeHTTP(w, r)
				return
			}

			// Set content type
			switch {
			case strings.HasSuffix(path, ".html"):
				w.Header().Set("Content-Type", "text/html; charset=utf-8")
			case strings.HasSuffix(path, ".css"):
				w.Header().Set("Content-Type", "text/css; charset=utf-8")
			case strings.HasSuffix(path, ".js"):
				w.Header().Set("Content-Type", "application/javascript; charset=utf-8")
			case strings.HasSuffix(path, ".json"):
				w.Header().Set("Content-Type", "application/json")
			case strings.HasSuffix(path, ".svg"):
				w.Header().Set("Content-Type", "image/svg+xml")
			case strings.HasSuffix(path, ".png"):
				w.Header().Set("Content-Type", "image/png")
			case strings.HasSuffix(path, ".ico"):
				w.Header().Set("Content-Type", "image/x-icon")
			}

			// Don't let Cloudflare or browsers cache the SPA assets — they ship
			// with each release and any CDN cache (default max-age=14400) makes
			// users miss bug fixes for hours after a deploy. The files are tiny;
			// always-fresh is the right tradeoff.
			w.Header().Set("Cache-Control", "no-cache, no-store, must-revalidate")
			w.Header().Set("Pragma", "no-cache")
			w.Header().Set("Expires", "0")

			// Cache-busting: rewrite any ?v=NNN token to the content hash
			// (pattern-based so a manual version bump can't outrun it), and inject
			// the hash into app.js's/mri-brain.js's version-less imports - so a new
			// build always serves fresh JS through any cache/CDN. The regex runs
			// FIRST so it only ever touches source tokens, never an injected hash.
			if strings.HasSuffix(path, ".html") {
				f = assetVerRe.ReplaceAll(f, []byte("?v="+assetVer))
			} else if strings.HasSuffix(path, ".js") {
				f = assetVerRe.ReplaceAll(f, []byte("?v="+assetVer))
				f = bytes.ReplaceAll(f, []byte("from './mri-brain.js'"), []byte("from './mri-brain.js?v="+assetVer+"'"))
				f = bytes.ReplaceAll(f, []byte("from './api.js'"), []byte("from './api.js?v="+assetVer+"'"))
				f = bytes.ReplaceAll(f, []byte("import('./api.js')"), []byte("import('./api.js?v="+assetVer+"')"))
				f = bytes.ReplaceAll(f, []byte("from './sse.js'"), []byte("from './sse.js?v="+assetVer+"'"))
				f = bytes.ReplaceAll(f, []byte("from '/ui/js/vendor/sage-graph.bundle.js'"), []byte("from '/ui/js/vendor/sage-graph.bundle.js?v="+assetVer+"'"))
			}

			w.Write(f) //nolint:errcheck,gosec // static embedded file, not user input
		})

		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/ui/", http.StatusFound)
		})
	}) // end securityHeaders group
}

// authMiddleware gates dashboard routes based on encryption state.
//
// Encryption ON — require either a valid session cookie or a fresh Ed25519
// signature.
//
// Encryption OFF — fail-open is unsafe because the dashboard CORS allowlist
// includes localhost-bound origins for the SPA, and any non-browser client
// can still trigger state-changing endpoints. Instead we require either:
//   - a same-origin / no-Origin request (the SPA itself, or a CLI), OR
//   - a valid Ed25519 signature.
//
// This keeps the SPA and CLI workflows exactly as before while denying any
// browser tab whose Origin is not localhost / 127.0.0.1.
func (h *DashboardHandler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// An agent identity is usable only when this exact request verifies. Check
		// it before the local/browser path so a caller cannot forge X-Agent-ID (or
		// omit a bad signature) and inherit same-origin/session authorization.
		if strings.TrimSpace(r.Header.Get("X-Agent-ID")) != "" {
			if !h.validAgentSignature(r) {
				writeUnauthorized(w)
				return
			}
			agentID := strings.TrimSpace(r.Header.Get("X-Agent-ID"))
			ctx := context.WithValue(r.Context(), verifiedDashboardAgentKey{}, agentID)
			next.ServeHTTP(w, r.WithContext(ctx))
			return
		}
		if !h.Encrypted.Load() {
			if isLocalRequest(r) {
				next.ServeHTTP(w, r)
				return
			}
			writeUnauthorized(w)
			return
		}
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil || !h.validSession(cookie.Value) {
			writeUnauthorized(w)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// writeUnauthorized writes the canonical 401 envelope.
func writeUnauthorized(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{"error": "unauthorized", "login_required": true})
}

// isLocalRequest returns true for requests that originate from the same
// machine as the SAGE node — the dashboard SPA, a CLI invocation, or any
// non-browser caller. Browser tabs running on a different origin (e.g. a
// chatgpt.com tab attempting to fetch localhost:8080) are rejected.
//
// Sec-Fetch-Site is the canonical signal; we fall back to an Origin
// allowlist for older browsers that don't emit it.
//
// Anti-DNS-rebinding: the Host header MUST be localhost or a raw IP first.
// A rebinding attack serves its page from an attacker DOMAIN that re-resolves
// to 127.0.0.1; the victim's browser then sends Host: attacker.com with a
// same-origin Sec-Fetch-Site, which would otherwise pass every check below.
// A rebinding target must be a resolvable domain name, so requiring a
// loopback/IP-literal Host defeats it without us needing to know our own
// bound address.
func isLocalRequest(r *http.Request) bool {
	secFetch := r.Header.Get("Sec-Fetch-Site")
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	// The anti-rebinding Host gate applies to BROWSER requests only: a rebinding
	// page reaches us with a same-origin Sec-Fetch but an attacker-DOMAIN Host.
	// Non-browser callers (CLI, curl, MCP agents) connect directly, are not a
	// rebinding vector, and legitimately send arbitrary Host values, so they are
	// not constrained here. Sec-Fetch-Site is a browser-set forbidden header that
	// page JS cannot suppress when the browser supplies Fetch Metadata; older
	// WebViews may omit it and are handled by the stricter operator gate above.
	if (secFetch != "" || origin != "") && !hostIsLoopbackOrIP(r.Host) {
		return false
	}
	switch secFetch {
	case "cross-site":
		return false
	case "same-origin", "same-site", "none":
		return true
	}
	if origin == "" {
		// No Origin header: same-origin GET, or non-browser caller.
		return true
	}
	return originIsLocal(origin)
}

// hostIsLoopbackOrIP reports whether the request Host header is "localhost" or
// a bare IP literal (loopback or LAN) - i.e. not a domain name that could be a
// DNS-rebinding vector. Any explicit port is stripped first.
func hostIsLoopbackOrIP(host string) bool {
	host = strings.TrimSpace(host)
	if host == "" {
		return false
	}
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimPrefix(strings.TrimSuffix(host, "]"), "[") // unwrap [::1]
	if strings.EqualFold(host, "localhost") {
		return true
	}
	return net.ParseIP(host) != nil
}

// originIsLocal parses an Origin header and checks its hostname is EXACTLY a
// loopback name/address - not a prefix match, so http://localhost.evil.com and
// http://127.0.0.1.evil.com are correctly rejected.
func originIsLocal(origin string) bool {
	u, err := url.Parse(origin)
	if err != nil || u.Host == "" {
		return false
	}
	host := u.Hostname() // strips port and [] brackets
	return strings.EqualFold(host, "localhost") || host == "127.0.0.1" || host == "::1"
}

// wizardSecurityGate is a defence-in-depth layer specifically for the
// /v1/wizard/chatgpt/* endpoints. The wizard orchestrates `cloudflared` and
// platform autostart subprocesses, so even with authMiddleware in front of
// it we add a strict same-origin check that rejects any browser request
// whose Origin / Sec-Fetch-Site is not local — independent of cookie or
// session state.
func (h *DashboardHandler) wizardSecurityGate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !isLocalRequest(r) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"error": "forbidden"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

// validAgentSignature checks if the request carries valid Ed25519 agent auth headers.
// Reads and re-buffers the body so downstream handlers can still access it.
func (h *DashboardHandler) validAgentSignature(r *http.Request) bool {
	agentID := strings.TrimSpace(r.Header.Get("X-Agent-ID"))
	sigHex := strings.TrimSpace(r.Header.Get("X-Signature"))
	tsStr := strings.TrimSpace(r.Header.Get("X-Timestamp"))
	if agentID == "" || sigHex == "" || tsStr == "" {
		return false
	}
	tsUnix, err := strconv.ParseInt(tsStr, 10, 64)
	if err != nil {
		return false
	}
	diff := time.Now().Unix() - tsUnix
	if diff < 0 {
		diff = -diff
	}
	if time.Duration(diff)*time.Second > 5*time.Minute {
		return false
	}
	pubKey, err := auth.AgentIDToPublicKey(agentID)
	if err != nil {
		return false
	}
	sig, err := hex.DecodeString(sigHex)
	if err != nil {
		return false
	}
	// Read body for signature verification, then put it back.
	var body []byte
	if r.Body != nil {
		body, err = io.ReadAll(io.LimitReader(r.Body, 1<<20))
		if err != nil {
			return false
		}
		r.Body = io.NopCloser(bytes.NewReader(body))
	}
	reqPath := r.URL.Path
	if r.URL.RawQuery != "" {
		reqPath = r.URL.Path + "?" + r.URL.RawQuery
	}
	valid := false
	if nonceHex := strings.TrimSpace(r.Header.Get("X-Nonce")); nonceHex != "" {
		nonce, decodeErr := hex.DecodeString(nonceHex)
		if decodeErr != nil || len(nonce) == 0 {
			return false
		}
		valid = auth.VerifyRequestWithNonce(pubKey, r.Method, reqPath, body, tsUnix, nonce, sig)
	} else {
		valid = auth.VerifyRequest(pubKey, r.Method, reqPath, body, tsUnix, sig)
	}
	if !valid {
		return false
	}
	return !h.agentSignatureReplayed(agentID + ":" + sigHex)
}

// agentSignatureReplayed provides dashboard routes the same bounded replay
// protection as the REST auth middleware. Cryptographic verification alone
// proves who signed a request, not that a captured mutation was not resent.
func (h *DashboardHandler) agentSignatureReplayed(key string) bool {
	h.agentReplayMu.Lock()
	defer h.agentReplayMu.Unlock()
	if h.agentReplaySeen == nil {
		h.agentReplaySeen = make(map[string]time.Time)
	}
	now := time.Now()
	if len(h.agentReplaySeen) >= 10000 {
		for seenKey, seenAt := range h.agentReplaySeen {
			if now.Sub(seenAt) > 5*time.Minute {
				delete(h.agentReplaySeen, seenKey)
			}
		}
	}
	if _, exists := h.agentReplaySeen[key]; exists {
		return true
	}
	if len(h.agentReplaySeen) >= 10000 {
		return true // fail closed instead of allowing the bounded cache to grow
	}
	h.agentReplaySeen[key] = now
	return false
}

// handleLogin verifies the vault passphrase and sets a session cookie.
func (h *DashboardHandler) handleLogin(w http.ResponseWriter, r *http.Request) {
	if !h.Encrypted.Load() {
		writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "message": "no auth required"})
		return
	}

	// Rate limit: max 5 attempts per IP per minute.
	ip, _, _ := net.SplitHostPort(r.RemoteAddr)
	if ip == "" {
		ip = r.RemoteAddr
	}
	now := time.Now()
	cutoff := now.Add(-1 * time.Minute)
	val, _ := h.loginAttempts.LoadOrStore(ip, &[]time.Time{})
	attempts := val.(*[]time.Time)
	// Filter to only recent attempts.
	recent := (*attempts)[:0]
	for _, t := range *attempts {
		if t.After(cutoff) {
			recent = append(recent, t)
		}
	}
	if len(recent) >= 5 {
		*attempts = recent
		writeJSONResp(w, http.StatusTooManyRequests, map[string]any{"ok": false, "error": "too many login attempts, try again later"})
		return
	}
	*attempts = append(recent, now)

	var req struct {
		Passphrase string `json:"passphrase"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Passphrase == "" {
		writeJSONResp(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "passphrase required"})
		return
	}

	// Verify passphrase against vault
	v, err := vault.Open(h.VaultKeyPath, req.Passphrase)
	if err != nil {
		writeJSONResp(w, http.StatusUnauthorized, map[string]any{"ok": false, "error": "wrong passphrase"})
		return
	}

	// Unlock the vault store so new writes are encrypted.
	// This handles the case where the server started without a passphrase
	// (e.g. launched from app icon) and the user unlocks via the web UI.
	if vs, ok := h.store.(VaultStore); ok {
		vs.SetVault(v)
	}
	h.VaultLocked.Store(false)
	// A transient MCP embed failure may have preserved observations without
	// vectors while the vault was locked. Repair them now that plaintext is
	// available instead of leaving a manual setup banner behind.
	h.startPostUnlockRepairs()

	h.issueDashboardSession(w, r)

	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
}

// issueDashboardSession keeps cookie policy identical for ordinary login and
// the successful enable-encryption transition. The latter turns auth on during
// the request, so issuing the session in the same response prevents the browser
// from being stranded between setup and recovery-key acknowledgement.
func (h *DashboardHandler) issueDashboardSession(w http.ResponseWriter, r *http.Request) {
	token := generateToken()
	h.sessions.Store(token, time.Now().Add(sessionTTL))

	// gosec G124 wants a literal `Secure: true`; we set it based on
	// r.TLS != nil because SAGE-Personal legitimately serves over plain
	// HTTP on localhost when launched from the app icon (forcing Secure
	// there would silently break login). Production deployments terminate
	// TLS in front of sage-gui and r.TLS is populated automatically.
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // G124 — Secure derived from r.TLS, see comment above
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(sessionTTL.Seconds()),
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteStrictMode,
	})
}

// handleLock invalidates the current session — like Cmd+L in 1Password.
func (h *DashboardHandler) handleLock(w http.ResponseWriter, r *http.Request) {
	if !h.Encrypted.Load() {
		writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "message": "encryption not enabled"})
		return
	}

	// Invalidate the session token.
	if cookie, err := r.Cookie(sessionCookieName); err == nil {
		h.sessions.Delete(cookie.Value)
	}

	// Clear the cookie. Mirror the Secure attribute logic from
	// handleUnlock — only mark Secure when the request is over TLS so
	// localhost HTTP isn't broken. Same gosec G124 suppression rationale.
	http.SetCookie(w, &http.Cookie{ //nolint:gosec // G124 — Secure derived from r.TLS, see handleUnlock
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
		Secure:   r.TLS != nil,
		SameSite: http.SameSiteStrictMode,
	})

	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "locked": true})
}

// handleAuthCheck returns whether auth is required and if current session is valid.
func (h *DashboardHandler) handleAuthCheck(w http.ResponseWriter, r *http.Request) {
	if !h.Encrypted.Load() {
		writeJSONResp(w, http.StatusOK, map[string]any{"auth_required": false, "authenticated": true})
		return
	}

	cookie, err := r.Cookie(sessionCookieName)
	authenticated := err == nil && h.validSession(cookie.Value)

	writeJSONResp(w, http.StatusOK, map[string]any{"auth_required": true, "authenticated": authenticated})
}

// IsRequestAuthenticated reports whether `r` carries a valid dashboard
// session, considering encryption state. Used by the OAuth /authorize
// endpoint (api/rest) to gate the consent screen with the existing
// dashboard session — no separate auth implementation.
//
// Encryption-off nodes require an exact node-operator Ed25519 signature.
// Local/browser routing headers are not authority, and a tunnel/remote request
// is never authorized merely because no vault password was configured.
// Returns (false, redirectURL) otherwise; the
// caller should 302 the user to redirectURL so the dashboard SPA can run
// the login dance and round-trip back via `?next=...`.
func (h *DashboardHandler) IsRequestAuthenticated(r *http.Request) (bool, string) {
	if !h.Encrypted.Load() {
		// No vault means there is no meaningful browser session. Locality,
		// Origin, Sec-Fetch-Site, Host, and forwarded headers are routing hints,
		// not operator proof: a loopback process can forge all of them. OAuth
		// consent therefore requires the exact node key to sign this request.
		operatorID := strings.TrimSpace(h.NodeOperatorAgentID)
		if operatorID != "" && strings.TrimSpace(r.Header.Get("X-Agent-ID")) == operatorID && h.validAgentSignature(r) {
			return true, ""
		}
		next := r.URL.RequestURI()
		return false, "/ui/?next=" + url.QueryEscape(next)
	}
	if cookie, err := r.Cookie(sessionCookieName); err == nil && h.validSession(cookie.Value) {
		return true, ""
	}
	// Build a redirect to the SPA carrying `next=<original URL>` so the
	// frontend can redirect back here once the operator unlocks the vault.
	next := r.URL.RequestURI()
	return false, "/ui/?next=" + url.QueryEscape(next)
}

// HasValidSessionCookie reports whether the request carries a real dashboard
// session cookie, independent of encryption state. Used by the OAuth handler
// to decide whether to render the privileged agent-roster dropdown — when
// no cookie is present the consent screen falls back to the free-text input
// so unauthenticated tunnel visitors never see the agent list.
func (h *DashboardHandler) HasValidSessionCookie(r *http.Request) bool {
	cookie, err := r.Cookie(sessionCookieName)
	if err != nil {
		return false
	}
	return h.validSession(cookie.Value)
}

func (h *DashboardHandler) validSession(token string) bool {
	val, ok := h.sessions.Load(token)
	if !ok {
		return false
	}
	expiry, ok := val.(time.Time)
	if !ok {
		return false
	}
	if time.Now().After(expiry) {
		h.sessions.Delete(token)
		return false
	}
	return true
}

func generateToken() string {
	b := make([]byte, 32)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// handleListMemories returns paginated, filterable memory list.
func (h *DashboardHandler) handleListMemories(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset, _ := strconv.Atoi(q.Get("offset"))

	opts := cerebrumListOptions(store.ListOptions{
		DomainTag:       q.Get("domain"),
		Tag:             q.Get("tag"),
		Provider:        q.Get("provider"),
		Status:          q.Get("status"),
		SubmittingAgent: q.Get("agent"),
		CreatedFrom:     q.Get("from"),
		CreatedTo:       q.Get("to"),
		Limit:           limit,
		Offset:          offset,
		Sort:            q.Get("sort"),
	})

	// On-chain RBAC: if request comes from an MCP agent, enforce agent isolation.
	if allowedAgents, seeAll := h.resolveAgentRBAC(r); !seeAll {
		// Grant-aware override: skip agent isolation when agent has a grant OR domain is unregistered
		listAgentID := verifiedDashboardAgentID(r.Context())
		if opts.DomainTag != "" && h.BadgerStore != nil && listAgentID != "" {
			var hasGrant bool
			if h.isPostV8Fork() {
				hasGrant, _ = h.BadgerStore.HasAccessOrAncestor(opts.DomainTag, listAgentID, 1, time.Now())
			} else {
				hasGrant, _ = h.BadgerStore.HasAccess(opts.DomainTag, listAgentID, 1, time.Now())
			}
			if !hasGrant {
				// Unregistered domains have no access policy — don't enforce agent isolation
				_, ownerErr := h.BadgerStore.GetDomainOwner(opts.DomainTag)
				if ownerErr != nil {
					// Domain not registered — open visibility
				} else {
					opts.SubmittingAgents = allowedAgents
				}
			}
		} else {
			opts.SubmittingAgents = allowedAgents
		}
	}

	// Real search: when q is present, use FTS (SearchByText) for ranked results.
	// On an encrypted vault FTS is disabled, so fall back to a server-side keyword
	// scan over a broad recent pool - still far better than the old client-side
	// substring filter over only the newest 100. RBAC (opts.SubmittingAgents) is
	// applied on both paths.
	var records []*memory.MemoryRecord
	var total int
	if qStr := strings.TrimSpace(q.Get("q")); qStr != "" {
		qopts := cerebrumQueryOptions(store.QueryOptions{
			DomainTag:        opts.DomainTag,
			Provider:         opts.Provider,
			StatusFilter:     opts.Status,
			TopK:             limit,
			SubmittingAgents: opts.SubmittingAgents,
			CreatedFrom:      opts.CreatedFrom,
			CreatedTo:        opts.CreatedTo,
		})
		if opts.Tag != "" {
			qopts.Tags = []string{opts.Tag} // honor the tag filter on the FTS path too (the fallback already did)
		}
		var ftsRecs []*memory.MemoryRecord
		var ferr error
		if opts.Status == "deprecated" {
			// Deprecated rows are deliberately absent from the FTS index
			// (DeleteMemory drops them; BackfillFTS excludes them), so a MATCH
			// can never serve status=deprecated - it would always return zero.
			// Force the keyword pool-scan fallback below, which searches
			// deprecated rows correctly on both encrypted and unencrypted nodes.
			ferr = errDeprecatedNoFTS
		} else {
			ftsRecs, ferr = h.store.SearchByText(r.Context(), qStr, qopts)
		}
		if ferr == nil {
			records, total = ftsRecs, len(ftsRecs)
		} else {
			pool, _, perr := h.store.ListMemories(r.Context(), cerebrumListOptions(store.ListOptions{
				DomainTag: opts.DomainTag, Tag: opts.Tag, Provider: opts.Provider,
				Status: opts.Status, SubmittingAgent: opts.SubmittingAgent,
				SubmittingAgents: opts.SubmittingAgents,
				CreatedFrom:      opts.CreatedFrom, CreatedTo: opts.CreatedTo,
				Limit: 1000, Sort: "newest",
			}))
			if perr != nil {
				writeError(w, http.StatusInternalServerError, perr.Error())
				return
			}
			needle := strings.ToLower(qStr)
			for _, m := range pool {
				if strings.Contains(strings.ToLower(m.Content), needle) || strings.Contains(strings.ToLower(m.DomainTag), needle) {
					records = append(records, m)
					if len(records) >= limit {
						break
					}
				}
			}
			total = len(records)
		}
	} else {
		var lerr error
		records, total, lerr = h.store.ListMemories(r.Context(), opts)
		if lerr != nil {
			writeError(w, http.StatusInternalServerError, lerr.Error())
			return
		}
	}

	// Augment with corroboration counts so the UI can show how many agents have
	// backed each memory (display-only; same value the graph/recall paths use).
	if len(records) > 0 {
		ids := make([]string, len(records))
		for i, rec := range records {
			ids[i] = rec.MemoryID
		}
		if counts, cErr := h.store.GetCorroborationCounts(r.Context(), ids); cErr == nil {
			for _, rec := range records {
				rec.CorroborationCount = counts[rec.MemoryID]
			}
		}
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"memories": records,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

// handleExport streams active (non-deprecated) memories as JSONL (one JSON object per line).
// This format can be re-imported via the import system for backup/restore.
func (h *DashboardHandler) handleExport(w http.ResponseWriter, r *http.Request) {
	// Export format: one MemoryRecord JSON per line (JSONL).
	// Each line contains: memory_id, content, memory_type, domain_tag, confidence_score,
	// status, provider, submitting_agent, created_at, committed_at, etc.
	// Embeddings are excluded to keep export portable (re-generated on import).

	// Page through all records to avoid loading everything in memory at once.
	const pageSize = 500
	offset := 0

	filename := fmt.Sprintf("sage-backup-%s.jsonl", time.Now().Format("2006-01-02"))
	w.Header().Set("Content-Type", "application/x-ndjson")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))

	enc := json.NewEncoder(w)
	exported := 0

	for {
		records, _, err := h.store.ListMemories(r.Context(), cerebrumListOptions(store.ListOptions{
			Limit:  pageSize,
			Offset: offset,
			Sort:   "oldest",
			Status: "active",
		}))
		if err != nil {
			if exported == 0 {
				writeError(w, http.StatusInternalServerError, err.Error())
			}
			return
		}
		if len(records) == 0 {
			break
		}

		for _, rec := range records {
			// Export record — strip embeddings (regenerated on import).
			export := memory.MemoryRecord{
				MemoryID:        rec.MemoryID,
				SubmittingAgent: rec.SubmittingAgent,
				Content:         rec.Content,
				MemoryType:      rec.MemoryType,
				DomainTag:       rec.DomainTag,
				Provider:        rec.Provider,
				ConfidenceScore: rec.ConfidenceScore,
				Status:          rec.Status,
				ParentHash:      rec.ParentHash,
				TaskStatus:      rec.TaskStatus,
				CreatedAt:       rec.CreatedAt,
				CommittedAt:     rec.CommittedAt,
				DeprecatedAt:    rec.DeprecatedAt,
			}
			if err := enc.Encode(export); err != nil {
				return // client disconnected
			}
			exported++
		}

		offset += len(records)
	}

	// Keep an empty backup self-identifying. Without a manifest line, Import
	// cannot distinguish an empty SAGE JSONL file from another empty format.
	if exported == 0 {
		_ = enc.Encode(map[string]any{
			"record_type":         "sage_backup_manifest",
			"sage_backup_version": 1,
		})
	}
}

// handleTimeline returns aggregated counts for the timeline bar.
func (h *DashboardHandler) handleTimeline(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	domain := q.Get("domain")
	bucket := q.Get("bucket")
	if bucket == "" {
		bucket = "hour"
	}

	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()
	if v := q.Get("from"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			from = t
		}
	}
	if v := q.Get("to"); v != "" {
		if t, err := time.Parse(time.RFC3339, v); err == nil {
			to = t
		}
	}

	if isCerebrumInternalMemoryDomain(domain) {
		writeJSONResp(w, http.StatusOK, map[string]any{"buckets": []store.TimelineBucket{}})
		return
	}
	var buckets []store.TimelineBucket
	var err error
	if provider, ok := h.store.(interface {
		GetTimelineExcludingDomainPrefixes(context.Context, time.Time, time.Time, string, string, []string) ([]store.TimelineBucket, error)
	}); ok {
		buckets, err = provider.GetTimelineExcludingDomainPrefixes(
			r.Context(), from, to, domain, bucket, cerebrumInternalDomainPrefixes,
		)
	} else {
		buckets, err = h.store.GetTimeline(r.Context(), from, to, domain, bucket)
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{"buckets": buckets})
}

// graphNode is a memory node for the force-directed graph.
type graphNode struct {
	ID         string   `json:"id"`
	Content    string   `json:"content"`
	Domain     string   `json:"domain"`
	Confidence float64  `json:"confidence"`
	Status     string   `json:"status"`
	MemoryType string   `json:"memory_type"`
	CreatedAt  string   `json:"created_at"`
	Agent      string   `json:"agent"`
	Tags       []string `json:"tags,omitempty"`
	// CorroborationCount drives consolidation visuals in the brain view (a
	// well-corroborated memory glows/enlarges). Same value the recall paths
	// surface as corroboration_count.
	CorroborationCount int `json:"corroboration_count"`
}

// graphEdge connects two memories.
type graphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"` // "domain", "parent", "triple"
}

type graphCacheEntry struct {
	body       []byte
	at         time.Time
	refreshing bool
}

const (
	defaultGraphMaxNodes = 2500             // dense enough to fill the instanced MRI without overwhelming weaker GPUs
	graphCacheFresh      = 25 * time.Second // serve from cache without refreshing if younger than this
	graphCacheTTL        = 10 * time.Minute // hard cap — older than this, recompute synchronously
)

func graphMaxNodes() int {
	if v, _ := strconv.Atoi(os.Getenv("SAGE_GRAPH_MAX_NODES")); v > 0 {
		return v
	}
	return defaultGraphMaxNodes
}

// handleGraph returns all memories with edges for force-directed layout. The
// graph is expensive to compute on large brains (per-domain sampling + stats),
// so it is memoised stale-while-revalidate: the first load computes
// synchronously, repeat loads are served instantly while a background refresh
// keeps the entry warm. The actual build lives in computeGraphJSON.
func (h *DashboardHandler) handleGraph(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	// Node cap: the MRI renderer uses one instanced draw call and comfortably
	// handles a few thousand memories. Operators can still tune the bound for
	// unusually weak or strong hardware via SAGE_GRAPH_MAX_NODES.
	maxNodes := graphMaxNodes()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > maxNodes {
		limit = maxNodes
	}
	statusParam := q.Get("status")
	drillDomain := q.Get("domain")
	// On-chain RBAC: if the request comes from an MCP agent, enforce agent isolation.
	allowedAgents, seeAll := h.resolveAgentRBAC(r)

	// Cache key: every input that changes the output, incl. the RBAC scope so a
	// restricted agent is never served an operator (or another agent's) graph.
	scope := append([]string(nil), allowedAgents...)
	sort.Strings(scope)
	cacheKey := fmt.Sprintf("%v|%s|%s|%d|%s", seeAll, statusParam, drillDomain, limit, strings.Join(scope, ","))

	if body := h.serveGraphFromCache(cacheKey, statusParam, drillDomain, limit, seeAll, allowedAgents); body != nil {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(body) //nolint:gosec // G705: body is server-built JSON sent as application/json, not HTML — no XSS sink
		return
	}
	body, err := h.computeGraphJSON(r.Context(), statusParam, drillDomain, limit, seeAll, allowedAgents)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.putGraphCache(cacheKey, body)
	w.Header().Set("Content-Type", "application/json")
	_, _ = w.Write(body) //nolint:gosec // G705: body is server-built JSON sent as application/json, not HTML — no XSS sink
}

// serveGraphFromCache returns a cached graph body when one is fresh enough to
// use, triggering a single background refresh once an entry is past its fresh
// window. Returns nil on a miss (the caller then computes synchronously).
func (h *DashboardHandler) serveGraphFromCache(key, statusParam, drillDomain string, limit int, seeAll bool, allowedAgents []string) []byte {
	now := time.Now()
	h.graphCacheMu.Lock()
	defer h.graphCacheMu.Unlock()
	ent := h.graphCache[key]
	if ent == nil || now.Sub(ent.at) >= graphCacheTTL {
		return nil
	}
	if now.Sub(ent.at) >= graphCacheFresh && !ent.refreshing {
		ent.refreshing = true
		h.runBackground(func(ctx context.Context) {
			h.refreshGraphCache(ctx, key, statusParam, drillDomain, limit, seeAll, allowedAgents)
		})
	}
	return ent.body
}

// putGraphCache stores a freshly-computed body, resetting the map if it has
// grown unbounded (e.g. from many drill-down domains).
func (h *DashboardHandler) putGraphCache(key string, body []byte) {
	h.graphCacheMu.Lock()
	defer h.graphCacheMu.Unlock()
	if h.graphCache == nil || len(h.graphCache) > 128 {
		h.graphCache = make(map[string]*graphCacheEntry)
	}
	h.graphCache[key] = &graphCacheEntry{body: body, at: time.Now()}
}

// refreshGraphCache recomputes a cache entry off the request path (the
// "revalidate" half of stale-while-revalidate).
func (h *DashboardHandler) refreshGraphCache(parent context.Context, key, statusParam, drillDomain string, limit int, seeAll bool, allowedAgents []string) {
	ctx, cancel := context.WithTimeout(parent, 60*time.Second)
	defer cancel()
	body, err := h.computeGraphJSON(ctx, statusParam, drillDomain, limit, seeAll, allowedAgents)
	h.graphCacheMu.Lock()
	defer h.graphCacheMu.Unlock()
	ent := h.graphCache[key]
	if ent == nil {
		ent = &graphCacheEntry{}
		h.graphCache[key] = ent
	}
	ent.refreshing = false
	if err == nil {
		ent.body = body
		ent.at = time.Now()
	}
}

// computeGraphJSON performs the (expensive) graph build and returns the
// serialized JSON response. It is a pure function of its inputs, so it is safe
// to call both on the request path and from the background refresh goroutine.
func (h *DashboardHandler) computeGraphJSON(ctx context.Context, statusParam, drillDomain string, limit int, seeAll bool, allowedAgents []string) ([]byte, error) {
	opts := cerebrumListOptions(store.ListOptions{
		Limit:  limit,
		Sort:   "newest",
		Status: statusParam,
	})
	// Default: exclude deprecated memories from graph view
	if opts.Status == "" {
		opts.Status = "committed"
	}
	// status=all opts into the full brain (incl. challenged/deprecated) — the
	// MRI view uses it to render synaptic pruning.
	if opts.Status == "all" {
		opts.Status = ""
	}
	if !seeAll {
		opts.SubmittingAgents = allowedAgents
	}
	// Drill-down: ?domain=X loads just that lobe's memories.
	if drillDomain != "" {
		opts.DomainTag = drillDomain
	}

	// Scale aggregates — operator view only (no RBAC aggregate leak).
	var grandTotal int
	var domainCounts map[string]int
	if seeAll {
		if stats, sErr := h.cerebrumVisibleStats(ctx); sErr == nil && stats != nil {
			grandTotal = stats.TotalMemories
			domainCounts = stats.ByDomain
		}
	}

	// Node selection:
	//   - drill-down: that domain, most-significant (highest-confidence) first;
	//   - operator overview beyond the cap: a stratified importance sample — each
	//     lobe gets a quota proportional to its true size, filled with its
	//     highest-confidence memories, so lobe density stays faithful and the dots
	//     shown are the meaningful ones;
	//   - otherwise: newest within the cap.
	var records []*memory.MemoryRecord
	var err error
	switch {
	case drillDomain != "":
		opts.Sort = "confidence"
		records, _, err = h.store.ListMemories(ctx, opts)
	case seeAll && grandTotal > limit && len(domainCounts) > 1:
		records = h.stratifiedSample(ctx, opts, domainCounts, grandTotal, limit)
	default:
		records, _, err = h.store.ListMemories(ctx, opts)
	}
	if err != nil {
		return nil, err
	}

	nodes := make([]graphNode, 0, len(records))
	edges := make([]graphEdge, 0)

	// Batch-fetch tags for all memories
	memIDs := make([]string, len(records))
	for i, rec := range records {
		memIDs[i] = rec.MemoryID
	}
	tagMap, _ := h.store.GetTagsBatch(ctx, memIDs)
	// Batched corroboration counts for all rendered nodes — one query instead of
	// one GetCorroborations per node (the per-node form was an N+1 that made the
	// graph endpoint slow on large brains).
	corrCounts, _ := h.store.GetCorroborationCounts(ctx, memIDs)

	// Build domain groups for edge generation
	domainMemories := make(map[string][]string)
	for _, rec := range records {
		nodes = append(nodes, graphNode{
			ID:                 rec.MemoryID,
			Content:            truncate(rec.Content, 200),
			Domain:             rec.DomainTag,
			Confidence:         rec.ConfidenceScore,
			Status:             string(rec.Status),
			MemoryType:         string(rec.MemoryType),
			CreatedAt:          rec.CreatedAt.Format(time.RFC3339),
			Agent:              rec.SubmittingAgent,
			Tags:               tagMap[rec.MemoryID],
			CorroborationCount: corrCounts[rec.MemoryID],
		})
		domainMemories[rec.DomainTag] = append(domainMemories[rec.DomainTag], rec.MemoryID)

		// Parent edge
		if rec.ParentHash != "" {
			for _, other := range records {
				if other.MemoryID == rec.ParentHash {
					edges = append(edges, graphEdge{Source: rec.MemoryID, Target: other.MemoryID, Type: "parent"})
					break
				}
			}
		}
	}

	// Domain edges: connect sequential memories within the same domain (chain, not full mesh)
	for domain, ids := range domainMemories {
		_ = domain
		for i := 1; i < len(ids); i++ {
			edges = append(edges, graphEdge{Source: ids[i-1], Target: ids[i], Type: "domain"})
		}
	}

	// Real typed links (sage_link): supports / contradicts / causes / precedes /
	// refines / related. GetLinksAmong returns only links whose BOTH endpoints are
	// in memIDs (the RBAC-visible set), so the connectome never surfaces a link to
	// a memory the caller cannot see — and it is a single query, not one per node.
	seenLink := make(map[string]bool)
	if typed, lErr := h.store.GetLinksAmong(ctx, memIDs); lErr == nil {
		for _, l := range typed {
			key := l.SourceID + "\x00" + l.TargetID + "\x00" + l.LinkType
			if seenLink[key] {
				continue
			}
			seenLink[key] = true
			lt := l.LinkType
			if lt == "" {
				lt = "related"
			}
			edges = append(edges, graphEdge{Source: l.SourceID, Target: l.TargetID, Type: lt})
		}
	}

	resp := map[string]any{
		"nodes": nodes,
		"edges": edges,
	}
	// Scale signal for the MRI view: the true memory total + per-domain counts so
	// the brain can convey "showing N of M" and weight each lobe by its real size
	// even when only a bounded sample of nodes is rendered. Operator view only —
	// an RBAC-restricted agent must not see global aggregate counts.
	if seeAll && domainCounts != nil {
		resp["total"] = grandTotal
		resp["domain_counts"] = domainCounts
		// Per-domain recency so the lobe list can order by last activity
		// (newest first) instead of alphabetically. Optional-interface so
		// non-SQLite stores simply omit it.
		if dap, ok := h.store.(domainActivityProvider); ok {
			if dl, dErr := dap.GetDomainLastActivity(ctx); dErr == nil {
				for domain := range dl {
					if isCerebrumInternalMemoryDomain(domain) {
						delete(dl, domain)
					}
				}
				resp["domain_last"] = dl
			}
		}
	}
	return json.Marshal(resp)
}

// domainActivityProvider is implemented by SQLiteStore: per-domain timestamp
// of the most recent non-deprecated memory, feeding the MRI lobe ordering.
type domainActivityProvider interface {
	GetDomainLastActivity(ctx context.Context) (map[string]string, error)
}

// stratifiedSample draws a representative, importance-ranked sample for the
// overview brain: each domain gets a quota proportional to its share of the
// total, filled with that domain's highest-confidence memories. Keeps lobe
// density faithful to reality and surfaces the most significant memories, while
// never exceeding the cap. One bounded ListMemories call per domain.
func (h *DashboardHandler) stratifiedSample(ctx context.Context, base store.ListOptions, domainCounts map[string]int, total, cap int) []*memory.MemoryRecord {
	out := make([]*memory.MemoryRecord, 0, cap)
	for domain, cnt := range domainCounts {
		if cnt <= 0 {
			continue
		}
		quota := (cap*cnt + total/2) / total // round(cap * cnt/total)
		if quota < 1 {
			quota = 1
		}
		o := base
		o.DomainTag = domain
		o.Sort = "confidence"
		o.Limit = quota
		recs, _, err := h.store.ListMemories(ctx, o)
		if err != nil {
			continue
		}
		out = append(out, recs...)
	}
	if len(out) > cap {
		out = out[:cap]
	}
	return out
}

// handleStats returns aggregate statistics.
func (h *DashboardHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.cerebrumVisibleStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, stats)
}

// handleDeleteMemory deprecates a memory.
func (h *DashboardHandler) handleDeleteMemory(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "memories can only be removed from an authenticated CEREBRUM session")
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing memory id")
		return
	}
	if h.rejectInternalCerebrumMemory(w, r.Context(), id) {
		return
	}
	if err := h.store.DeleteMemory(r.Context(), id); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h.SSE.Broadcast(SSEEvent{Type: EventForget, MemoryID: id})
	writeJSONResp(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// handleUpdateMemory updates a memory's domain tag and/or tags.
func (h *DashboardHandler) handleUpdateMemory(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing memory id")
		return
	}
	if h.rejectInternalCerebrumMemory(w, r.Context(), id) {
		return
	}

	var body struct {
		Domain string   `json:"domain"`
		Tags   []string `json:"tags,omitempty"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Domain == "" && body.Tags == nil {
		writeError(w, http.StatusBadRequest, "domain or tags is required")
		return
	}
	if isCerebrumInternalMemoryDomain(body.Domain) {
		writeError(w, http.StatusBadRequest, "domain is reserved for internal federation state")
		return
	}
	if body.Domain != "" {
		if err := h.store.UpdateDomainTag(r.Context(), id, body.Domain); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	if body.Tags != nil {
		if err := h.store.SetTags(r.Context(), id, body.Tags); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}
	writeJSONResp(w, http.StatusOK, map[string]string{"status": "updated"})
}

// handleBulkUpdateMemories applies domain and/or tag changes to multiple memories at once.
func (h *DashboardHandler) handleBulkUpdateMemories(w http.ResponseWriter, r *http.Request) {
	var body struct {
		IDs     []string `json:"ids"`
		Domain  string   `json:"domain,omitempty"`
		AddTags []string `json:"add_tags,omitempty"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(body.IDs) == 0 {
		writeError(w, http.StatusBadRequest, "ids is required")
		return
	}
	if len(body.IDs) > 500 {
		writeError(w, http.StatusBadRequest, "max 500 memories per bulk operation")
		return
	}
	if body.Domain == "" && len(body.AddTags) == 0 {
		writeError(w, http.StatusBadRequest, "domain or add_tags is required")
		return
	}
	if isCerebrumInternalMemoryDomain(body.Domain) {
		writeError(w, http.StatusBadRequest, "domain is reserved for internal federation state")
		return
	}

	ctx := r.Context()
	updated := 0
	var firstErr error
	firstErrStatus := http.StatusInternalServerError

	for _, id := range body.IDs {
		if record, err := h.store.GetMemory(ctx, id); err == nil && record != nil && isCerebrumInternalMemoryDomain(record.DomainTag) {
			if firstErr == nil {
				firstErr = fmt.Errorf("memory not found")
				firstErrStatus = http.StatusNotFound
			}
			continue
		}
		if body.Domain != "" {
			if err := h.store.UpdateDomainTag(ctx, id, body.Domain); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		if len(body.AddTags) > 0 {
			existing, err := h.store.GetTags(ctx, id)
			if err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
			tagSet := make(map[string]bool, len(existing)+len(body.AddTags))
			for _, t := range existing {
				tagSet[t] = true
			}
			for _, t := range body.AddTags {
				tagSet[t] = true
			}
			merged := make([]string, 0, len(tagSet))
			for t := range tagSet {
				merged = append(merged, t)
			}
			if err := h.store.SetTags(ctx, id, merged); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		updated++
	}

	if firstErr != nil && updated == 0 {
		writeError(w, firstErrStatus, firstErr.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"status":  "updated",
		"updated": updated,
		"total":   len(body.IDs),
	})
}

// handleListTags returns all unique tags with counts.
func (h *DashboardHandler) handleListTags(w http.ResponseWriter, r *http.Request) {
	tags, err := h.store.ListAllTags(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tags == nil {
		tags = []store.TagCount{}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"tags": tags})
}

// handleGetMemoryTags returns tags for a specific memory.
func (h *DashboardHandler) handleGetMemoryTags(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if h.rejectInternalCerebrumMemory(w, r.Context(), id) {
		return
	}
	tags, err := h.store.GetTags(r.Context(), id)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if tags == nil {
		tags = []string{}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"memory_id": id, "tags": tags})
}

// handleSetMemoryTags replaces all tags on a memory.
func (h *DashboardHandler) handleSetMemoryTags(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if h.rejectInternalCerebrumMemory(w, r.Context(), id) {
		return
	}
	var body struct {
		Tags []string `json:"tags"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := h.store.SetTags(r.Context(), id, body.Tags); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"memory_id": id, "tags": body.Tags, "status": "updated"})
}

// handleGetTasks returns tasks from the backlog. Use ?all=true for all statuses (Kanban board).
func (h *DashboardHandler) handleGetTasks(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")

	var tasks []*memory.MemoryRecord
	var err error

	if r.URL.Query().Get("all") == "true" {
		// The cross-status feed is the local CEREBRUM board, not an agent API.
		// Agents use the scoped backlog below, where authorization happens before
		// work is returned or claimed. Keeping signed callers out also prevents a
		// global board limit from starving their permitted tasks behind denied rows.
		if !h.isCEREBRUMOperatorRequest(r) {
			writeError(w, http.StatusForbidden, "the full task board is available only in local CEREBRUM; agents should use their task backlog")
			return
		}
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 100
		} else if limit > 500 {
			limit = 500
		}
		tasks, err = h.store.GetAllTasks(r.Context(), domain, limit)
	} else {
		provider := r.URL.Query().Get("provider")
		// X-Agent-ID (set by the MCP server on every request) drives ownership: an
		// agent's backlog excludes tasks claimed by another agent.
		agentID := verifiedDashboardAgentID(r.Context())
		if agentID == "" && !h.isCEREBRUMOperatorRequest(r) {
			writeError(w, http.StatusForbidden, "the task backlog is available only to signed agents or an authenticated CEREBRUM session")
			return
		}
		tasks, err = h.store.GetOpenTasks(r.Context(), domain, provider, agentID)
		if err == nil && agentID != "" {
			filtered := tasks[:0]
			for _, task := range tasks {
				if h.agentCanReadTask(r.Context(), agentID, task.MemoryID, task.DomainTag) {
					filtered = append(filtered, task)
				}
			}
			tasks = filtered
		}
	}

	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	type taskResult struct {
		MemoryID        string  `json:"memory_id"`
		Content         string  `json:"content"`
		DomainTag       string  `json:"domain_tag"`
		TaskStatus      string  `json:"task_status"`
		ConfidenceScore float64 `json:"confidence_score"`
		CreatedAt       string  `json:"created_at"`
		// Authorship so the board can badge agent-authored vs human-created tasks
		// and filter by author. Human-created tasks carry an empty provider.
		Provider        string `json:"provider"`
		SubmittingAgent string `json:"submitting_agent"`
		// Assignee: the agent_id a task is assigned to / claimed by (board only).
		Assignee            string `json:"assignee"`
		TaskPickedUpBy      string `json:"task_picked_up_by"`
		TaskPickedUpAt      string `json:"task_picked_up_at,omitempty"`
		TaskStatusUpdatedAt string `json:"task_status_updated_at,omitempty"`
	}
	results := make([]taskResult, 0, len(tasks))
	for _, t := range tasks {
		taskPickedUpAt := ""
		if t.TaskPickedUpAt != nil {
			taskPickedUpAt = t.TaskPickedUpAt.Format("2006-01-02T15:04:05Z")
		}
		taskStatusUpdatedAt := ""
		if t.TaskStatusUpdatedAt != nil {
			taskStatusUpdatedAt = t.TaskStatusUpdatedAt.Format("2006-01-02T15:04:05Z")
		}
		results = append(results, taskResult{
			MemoryID:            t.MemoryID,
			Content:             t.Content,
			DomainTag:           t.DomainTag,
			TaskStatus:          string(t.TaskStatus),
			ConfidenceScore:     t.ConfidenceScore,
			CreatedAt:           t.CreatedAt.Format("2006-01-02T15:04:05Z"),
			Provider:            t.Provider,
			SubmittingAgent:     t.SubmittingAgent,
			Assignee:            t.Assignee,
			TaskPickedUpBy:      t.TaskPickedUpBy,
			TaskPickedUpAt:      taskPickedUpAt,
			TaskStatusUpdatedAt: taskStatusUpdatedAt,
		})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"tasks": results, "total": len(results)})
}

// handleReorderTasksDashboard persists the human operator's within-column
// ordering. Agent identities cannot use this local CEREBRUM preference surface.
func (h *DashboardHandler) handleReorderTasksDashboard(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "task order can only be changed from an authenticated CEREBRUM session")
		return
	}
	var body struct {
		TaskStatus string   `json:"task_status"`
		TaskIDs    []string `json:"task_ids"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ts := memory.TaskStatus(body.TaskStatus)
	if !memory.IsValidTaskStatus(ts) {
		writeError(w, http.StatusBadRequest, "task_status must be one of: planned, in_progress, done, dropped")
		return
	}
	if len(body.TaskIDs) == 0 || len(body.TaskIDs) > 500 {
		writeError(w, http.StatusBadRequest, "task_ids must contain between 1 and 500 cards")
		return
	}
	orderStore, ok := h.store.(store.TaskBoardOrderStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "task ordering is not available on this datastore")
		return
	}
	if err := orderStore.ReorderTasks(r.Context(), ts, body.TaskIDs); err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{Type: EventTask})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"task_status": body.TaskStatus, "task_ids": body.TaskIDs})
}

// handleUpdateTaskStatusDashboard updates a task's status from the dashboard.
func (h *DashboardHandler) handleUpdateTaskStatusDashboard(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}
	var body struct {
		TaskStatus string `json:"task_status"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	ts := memory.TaskStatus(body.TaskStatus)
	if !memory.IsValidTaskStatus(ts) {
		writeError(w, http.StatusBadRequest, "task_status must be one of: planned, in_progress, done, dropped")
		return
	}
	// Agent status changes require an active signature-bound identity and current
	// task read permission. Starting and terminal completion are store-level CAS
	// operations; reopening/planning is reserved for the operator task board.
	if agentID := verifiedDashboardAgentID(r.Context()); agentID != "" {
		agentStore, ok := h.store.(store.AgentStore)
		if !ok {
			writeError(w, http.StatusNotImplemented, "agent registry is not available on this datastore")
			return
		}
		agent, agentErr := agentStore.GetAgent(r.Context(), agentID)
		if agentErr != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil {
			writeError(w, http.StatusForbidden, "active registered agent identity required")
			return
		}
		task, taskErr := h.store.GetMemory(r.Context(), id)
		if taskErr != nil {
			writeError(w, http.StatusNotFound, taskErr.Error())
			return
		}
		if !h.agentCanReadTask(r.Context(), agentID, id, task.DomainTag) {
			writeError(w, http.StatusForbidden, "you do not have permission to update this task")
			return
		}

		var changed bool
		var changeErr error
		switch ts {
		case memory.TaskStatusInProgress:
			changed, changeErr = h.store.ClaimTask(r.Context(), id, agentID)
		case memory.TaskStatusDone, memory.TaskStatusDropped:
			assignmentStore, supported := h.store.(store.TaskAssignmentStore)
			if !supported {
				writeError(w, http.StatusNotImplemented, "agent task completion is not available on this datastore")
				return
			}
			changed, changeErr = assignmentStore.CompleteTaskAsAgent(r.Context(), id, agentID, ts)
		case memory.TaskStatusPlanned:
			writeError(w, http.StatusForbidden, "only the local CEREBRUM task board can reopen or re-plan work")
			return
		}
		if changeErr != nil {
			writeError(w, http.StatusInternalServerError, changeErr.Error())
			return
		}
		if !changed {
			writeError(w, http.StatusConflict, "task is terminal or not currently assigned to this agent")
			return
		}
		if h.SSE != nil {
			h.SSE.Broadcast(SSEEvent{Type: EventTask, MemoryID: id})
		}
		writeJSONResp(w, http.StatusOK, map[string]string{"memory_id": id, "task_status": body.TaskStatus})
		return
	}
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "task status can only be changed by the assigned agent or from an authenticated CEREBRUM session")
		return
	}
	if err := h.store.UpdateTaskStatus(r.Context(), id, ts); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	// Real-time: agents and humans update status through this same handler, so one
	// broadcast keeps every open board in sync without a manual refresh.
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{Type: EventTask, MemoryID: id})
	}
	writeJSONResp(w, http.StatusOK, map[string]string{"memory_id": id, "task_status": body.TaskStatus})
}

// handleAssignTask assigns (or, with an empty assignee, unassigns) a task to a
// specific agent from the dashboard. Assigning is treated as handing the job to
// that agent now, so the card moves to In Progress immediately.
func (h *DashboardHandler) handleAssignTask(w http.ResponseWriter, r *http.Request) {
	// Assignment is a dashboard operator action. A caller presenting an agent
	// identity cannot assign, reassign, or unassign work. On an unencrypted
	// personal node, every local process is already inside the documented trusted
	// operator boundary; Origin/Sec-Fetch only defend against cross-site browser
	// requests and are not claimed as authentication against local software.
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "task assignments can only be changed from the local CEREBRUM task board")
		return
	}
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing task id")
		return
	}
	var body struct {
		Assignee string `json:"assignee"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	assignee := strings.TrimSpace(body.Assignee)
	if assignee != "" {
		agentStore, ok := h.store.(store.AgentStore)
		if !ok {
			writeError(w, http.StatusNotImplemented, "task assignment is not available on this datastore")
			return
		}
		agent, err := agentStore.GetAgent(r.Context(), assignee)
		if err != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil {
			writeError(w, http.StatusBadRequest, "choose an active registered agent")
			return
		}
		task, err := h.store.GetMemory(r.Context(), id)
		if err != nil {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		if !h.agentCanReadTask(r.Context(), assignee, id, task.DomainTag) {
			writeError(w, http.StatusForbidden, "that agent does not have permission to read this task")
			return
		}
	}
	assignmentStore, ok := h.store.(store.TaskAssignmentStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "task assignment notifications are not available on this datastore")
		return
	}
	result, err := assignmentStore.AssignTaskAndNotify(r.Context(), id, assignee)
	if err != nil {
		writeError(w, http.StatusConflict, err.Error())
		return
	}
	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{Type: EventTask, MemoryID: id})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{
		"memory_id": id, "assignee": assignee, "task_status": result.TaskStatus,
		"assignment_version": result.AssignmentVersion, "changed": result.Changed,
		"notification_created": result.NotificationCreated,
	})
}

type taskClassificationReader interface {
	GetMemoryClassificationLocal(ctx context.Context, memoryID string) (int, error)
}

// agentCanReadTask prevents assignment/backlog from becoming an RBAC side
// channel. Public tasks are readable; non-public tasks must pass the same
// multi-org/domain access check as memory reads.
func (h *DashboardHandler) agentCanReadTask(ctx context.Context, agentID, memoryID, domain string) bool {
	allowed, _ := h.agentTaskReadDecision(ctx, agentID, memoryID, domain)
	return allowed
}

// agentTaskReadDecision distinguishes a definitive access denial from a
// transient/unavailable authorization lookup. Notification delivery keeps a
// notice unread on transient failures and supersedes only definitive denials.
func (h *DashboardHandler) agentTaskReadDecision(ctx context.Context, agentID, memoryID, domain string) (allowed, definitive bool) {
	if domainAllowed, domainDefinitive := h.agentDomainReadDecision(ctx, agentID, domain); !domainAllowed {
		return false, domainDefinitive
	}
	reader, ok := h.store.(taskClassificationReader)
	if !ok {
		return false, false
	}
	classification, err := reader.GetMemoryClassificationLocal(ctx, memoryID)
	if err != nil {
		return false, false
	}
	if classification == 0 {
		return true, true
	}
	if h.BadgerStore == nil {
		return false, false
	}
	postFork := h.PostV8ForkFn != nil && h.PostV8ForkFn()
	allowed, err = h.BadgerStore.HasAccessMultiOrg(domain, agentID, uint8(classification), time.Now(), postFork)
	if err != nil {
		return false, false
	}
	return allowed, true
}

// agentDomainReadDecision applies the same explicit domain allowlist used by
// the REST memory APIs. Assignment is workflow ownership, not an implicit RBAC
// grant, even for a PUBLIC task.
func (h *DashboardHandler) agentDomainReadDecision(ctx context.Context, agentID, domain string) (allowed, definitive bool) {
	check := func(role, raw string) (bool, bool) {
		if role == "admin" || strings.TrimSpace(raw) == "" {
			return true, true
		}
		var entries []struct {
			Domain string `json:"domain"`
			Read   bool   `json:"read"`
		}
		if err := json.Unmarshal([]byte(raw), &entries); err != nil || len(entries) == 0 {
			return true, true // backward-compatible unrestricted legacy value
		}
		for _, entry := range entries {
			if entry.Domain == domain {
				return entry.Read, true
			}
		}
		// app-v19 local-agents-default-READ: once the fork has activated and the
		// operator has not opted into strict RBAC, a non-admin agent whose explicit
		// allowlist simply OMITS this domain reads it by default instead of the
		// fail-closed deny. An explicit `read:false` entry above is still honored
		// (deliberate deny). This is the domain-level gate only; per-memory
		// classification stays enforced one layer up in agentTaskReadDecision via
		// HasAccessMultiOrg, so a classified memory in a defaulted-readable domain
		// is still gated by clearance. Pre-fork / StrictRBAC keeps the deny below,
		// so a chain that has not activated app-v19 reads exactly as before.
		if h.AppV19ActiveFn != nil && h.AppV19ActiveFn() && !h.StrictRBAC {
			return true, true
		}
		return false, true
	}

	if h.BadgerStore != nil {
		if agent, err := h.BadgerStore.GetRegisteredAgent(agentID); err == nil && agent != nil {
			return check(agent.Role, agent.DomainAccess)
		}
	}
	agentStore, ok := h.store.(store.AgentStore)
	if !ok {
		return false, false
	}
	agent, err := agentStore.GetAgent(ctx, agentID)
	if err != nil || agent == nil {
		return false, false
	}
	return check(agent.Role, agent.DomainAccess)
}

// handleTaskNotifications returns one-way assignment notices for the signed
// agent. Reading atomically acknowledges them; unlike pipeline work, these
// notices require no claim or result.
func (h *DashboardHandler) handleTaskNotifications(w http.ResponseWriter, r *http.Request) {
	agentID := verifiedDashboardAgentID(r.Context())
	if agentID == "" {
		writeError(w, http.StatusUnauthorized, "agent identity required")
		return
	}
	agentStore, ok := h.store.(store.AgentStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "agent registry is not available on this datastore")
		return
	}
	agent, err := agentStore.GetAgent(r.Context(), agentID)
	if err != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil {
		writeError(w, http.StatusForbidden, "active registered agent identity required")
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	assignmentStore, ok := h.store.(store.TaskAssignmentStore)
	if !ok {
		writeJSONResp(w, http.StatusOK, map[string]any{"items": []any{}, "count": 0})
		return
	}
	items, err := assignmentStore.PeekAgentNotifications(r.Context(), agentID, limit)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	filtered := items[:0]
	ackIDs := make([]string, 0, len(items))
	deniedIDs := make([]string, 0)
	for _, item := range items {
		allowed, definitive := h.agentTaskReadDecision(r.Context(), agentID, item.TaskID, item.Domain)
		if allowed {
			filtered = append(filtered, item)
			ackIDs = append(ackIDs, item.NotificationID)
		} else if definitive {
			deniedIDs = append(deniedIDs, item.NotificationID)
		}
	}
	// Close the small status-change window between the first active check and
	// response serialization. RBAC is rechecked per task immediately above.
	agent, err = agentStore.GetAgent(r.Context(), agentID)
	if err != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil {
		writeError(w, http.StatusForbidden, "active registered agent identity required")
		return
	}
	if err = assignmentStore.SupersedeAgentNotifications(r.Context(), agentID, deniedIDs); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	acknowledged, err := assignmentStore.AcknowledgeAgentNotifications(r.Context(), agentID, ackIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	winners := make(map[string]bool, len(acknowledged))
	for _, id := range acknowledged {
		winners[id] = true
	}
	returned := filtered[:0]
	for _, item := range filtered {
		if winners[item.NotificationID] {
			item.State = "read"
			returned = append(returned, item)
		}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"items": returned, "count": len(returned)})
}

// handleCreateTaskDashboard creates a new task from the CEREBRUM dashboard.
func (h *DashboardHandler) handleCreateTaskDashboard(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "tasks can only be added from an authenticated CEREBRUM session; agents should use sage_task")
		return
	}
	var body struct {
		Content string `json:"content"`
		Domain  string `json:"domain"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Content == "" {
		writeError(w, http.StatusBadRequest, "content is required")
		return
	}
	if body.Domain == "" {
		body.Domain = "general"
	}

	taskContent := applyTaskPrefix(body.Content)
	memoryID := uuid.New().String()

	// Generate embedding
	var embedding []float32
	var embeddingHash []byte
	if h.embedder != nil {
		if emb, err := h.embedder.Embed(r.Context(), taskContent); err == nil {
			embedding = emb
			eh := sha256.New()
			for _, v := range emb {
				fmt.Fprintf(eh, "%f", v)
			}
			embeddingHash = eh.Sum(nil)
		}
	}

	contentHash := sha256.Sum256([]byte(taskContent))

	rec := &memory.MemoryRecord{
		MemoryID:          memoryID,
		SubmittingAgent:   agentIDForKey(h.SigningKey),
		Content:           taskContent,
		ContentHash:       contentHash[:],
		MemoryType:        memory.TypeTask,
		DomainTag:         body.Domain,
		ConfidenceScore:   0.90,
		TaskStatus:        memory.TaskStatusPlanned,
		Status:            memory.StatusProposed,
		CreatedAt:         time.Now(),
		Embedding:         embedding,
		EmbeddingProvider: embeddingProviderStamp(h.embedder, embedding),
	}

	// Broadcast on-chain through CometBFT consensus
	if h.CometBFTRPC != "" {
		if len(h.SigningKey) != ed25519.PrivateKeySize {
			writeError(w, http.StatusServiceUnavailable, "this node cannot sign a task submission; restart it and try again")
			return
		}
		submitTx := &tx.ParsedTx{
			Type:      tx.TxTypeMemorySubmit,
			Timestamp: time.Now(),
			MemorySubmit: &tx.MemorySubmit{
				MemoryID:        memoryID,
				ContentHash:     contentHash[:],
				EmbeddingHash:   embeddingHash,
				MemoryType:      tx.MemoryTypeTask,
				DomainTag:       body.Domain,
				ConfidenceScore: 0.90,
				Content:         taskContent,
				TaskStatus:      "planned",
				Classification:  tx.ClearanceLevel(1), // INTERNAL
			},
		}
		if _, _, _, err := h.signAndBroadcastCommit(submitTx, h.SigningKey); err != nil {
			writeError(w, http.StatusBadGateway, "the network did not confirm this task; nothing was added: "+err.Error())
			return
		}
		// The dashboard broadcasts directly instead of going through api/rest's
		// supplementary-data cache. Consensus has inserted the task by the time
		// broadcast_tx_commit returns; enrich that row with the local semantic
		// vector and its provenance so Settings does not flag it for repair.
		if rec.EmbeddingProvider != "" {
			_ = h.store.UpdateMemoryEmbedding(r.Context(), rec.MemoryID, rec.Embedding, rec.EmbeddingProvider)
		}
	} else {
		if rec.SubmittingAgent == "" {
			rec.SubmittingAgent = "cerebrum"
		}
		if err := h.store.InsertMemory(r.Context(), rec); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	if h.SSE != nil {
		h.SSE.Broadcast(SSEEvent{Type: EventTask, MemoryID: memoryID})
	}

	writeJSONResp(w, http.StatusCreated, map[string]string{
		"memory_id":   memoryID,
		"task_status": "planned",
		"domain":      body.Domain,
	})
}

// handleHealth returns system health including Ollama status.
// handleMCPConfig returns the .mcp.json content for AI agents to self-configure.
// The server knows its own binary path, so agents don't need to find it.
func (h *DashboardHandler) handleMCPConfig(w http.ResponseWriter, _ *http.Request) {
	execPath := h.ExecPath
	if execPath == "" {
		execPath = "sage-gui" // fallback
	}

	sageHome := h.sageHome()

	config := map[string]any{
		"mcpServers": map[string]any{
			"sage": map[string]any{
				"command": execPath,
				"args":    []string{"mcp"},
				"env": map[string]string{
					"SAGE_HOME":     sageHome,
					"SAGE_PROVIDER": "claude-code",
				},
			},
		},
	}

	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(config)
}

// sageHome returns the SAGE data directory path.
func (h *DashboardHandler) sageHome() string {
	if v := os.Getenv("SAGE_HOME"); v != "" {
		return v
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "~/.sage"
	}
	return home + "/.sage"
}

func (h *DashboardHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]any{
		"sage":         "running",
		"version":      h.Version,
		"boot_id":      h.BootID,
		"encrypted":    h.Encrypted.Load(),
		"vault_locked": h.VaultLocked.Load(),
		"uptime":       time.Since(startTime).String(),
	}
	if h.RESTAddr != "" {
		health["rest_addr"] = h.RESTAddr
	}

	// Embedder status. Before v6.8.8 the dashboard hard-coded an Ollama probe
	// to localhost:11434, which painted "Ollama offline" any time the operator
	// had selected the openai-compatible or hash provider — cosmetic only, but
	// confusing because SAGE was actually embedding fine through the configured
	// path. We now dispatch on the configured provider: type-assert h.embedder
	// to the optional interfaces in internal/embedding (Named / Modeler /
	// Pinger / the Dimension+Ready+Semantic trio from Provider) and report a
	// structured embedder block.
	//
	// The legacy "ollama" string field is still populated so older builds of
	// the dashboard JS keep showing something sensible during version skew:
	//   - provider == "ollama": "running" iff the configured client pings ok
	//   - provider != "ollama": "n/a" — the old key is no longer meaningful,
	//     but we don't reuse it to signal openai-compatible state because old
	//     clients would mis-render that as "Ollama running".
	provider, model := "", ""
	dimension := 0
	ready := false
	semantic := false
	probeAttempted := false
	probeOK := false

	if h.embedder != nil {
		if ep, ok := h.embedder.(embedderProvider); ok {
			dimension = ep.Dimension()
			ready = ep.Ready()
			semantic = ep.Semantic()
		}
		if named, ok := h.embedder.(embedding.Named); ok {
			provider = named.Name()
		} else if semantic {
			// Pre-Named providers in the wild are Ollama; hash exposes
			// Semantic()=false. Mirror api/rest/embed_handler.go's fallback.
			provider = "ollama"
		} else {
			provider = "hash"
		}
		if modeler, ok := h.embedder.(embedding.Modeler); ok {
			model = modeler.Model()
		}
		if pinger, ok := h.embedder.(embedding.Pinger); ok {
			probeAttempted = true
			pingCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			if err := pinger.Ping(pingCtx); err == nil {
				probeOK = true
			}
			cancel()
		}
	}

	// Legacy field, kept for backward compatibility with pre-v6.8.8 dashboards
	// that read health.ollama directly.
	switch provider {
	case "ollama":
		if probeAttempted {
			if probeOK {
				health["ollama"] = "running"
			} else {
				health["ollama"] = "offline"
			}
		} else {
			// No Pinger on this Ollama client (shouldn't happen in current
			// builds, but keep the legacy probe as a fallback so the field
			// stays accurate).
			client := &http.Client{Timeout: 2 * time.Second}
			ollamaReq, _ := http.NewRequestWithContext(r.Context(), "GET", "http://localhost:11434/api/tags", nil)
			if resp, err := client.Do(ollamaReq); err != nil {
				health["ollama"] = "offline"
			} else {
				resp.Body.Close()
				health["ollama"] = "running"
			}
		}
	default:
		// "n/a" signals to legacy clients that this field doesn't describe
		// the active embedder. New clients ignore it and read health.embedder.
		health["ollama"] = "n/a"
	}

	embedderInfo := map[string]any{
		"provider":  provider,
		"dimension": dimension,
		"ready":     ready,
		"semantic":  semantic,
	}
	if model != "" {
		embedderInfo["model"] = model
	}
	// online: best-effort liveness for the operator pill. Prefer Pinger
	// (real-time), fall back to Ready() (sticky has-ever-succeeded). Hash
	// is always online — it has no upstream.
	switch {
	case probeAttempted:
		embedderInfo["online"] = probeOK
	case provider == "hash":
		embedderInfo["online"] = true
	default:
		embedderInfo["online"] = ready
	}
	if h.embedder == nil {
		embedderInfo["provider"] = "none"
		embedderInfo["online"] = false
	}
	// Optional reranker config — present only when the store implements
	// rerankerInfoProvider (SQLiteStore). A live online-ping is deferred; the
	// operator reads "enabled" as the status.
	if rp, ok := h.store.(rerankerInfoProvider); ok {
		en, m, _ := rp.RerankerInfo()
		// url intentionally omitted: /health is public (CLI status), so we do
		// not expose the internal reranker upstream endpoint here. An authed
		// settings surface can fetch the URL separately when it needs it.
		embedderInfo["reranker"] = map[string]any{"enabled": en, "model": m}
	}
	health["embedder"] = embedderInfo

	// Get memory stats
	stats, err := h.cerebrumVisibleStats(r.Context())
	if err == nil {
		health["memories"] = stats
	}

	// CometBFT chain stats — dial the configured RPC endpoint (honors
	// SAGE_CMT_RPC_ADDR via h.CometBFTRPC) so a node moved off 26657 reports its
	// own chain state instead of querying the historical hardcoded port.
	chain := map[string]any{}
	cometRPC := h.CometBFTRPC
	if cometRPC == "" {
		cometRPC = "http://127.0.0.1:26657"
	}
	cometClient := &http.Client{Timeout: 2 * time.Second}
	statusReq, _ := http.NewRequestWithContext(r.Context(), "GET", cometRPC+"/status", nil)
	if statusResp, statusErr := cometClient.Do(statusReq); statusErr == nil {
		defer statusResp.Body.Close()
		var cometStatus struct {
			Result struct {
				NodeInfo struct {
					Network         string `json:"network"`
					Moniker         string `json:"moniker"`
					ProtocolVersion struct {
						App string `json:"app"`
					} `json:"protocol_version"`
				} `json:"node_info"`
				SyncInfo struct {
					LatestBlockHeight string `json:"latest_block_height"`
					LatestBlockTime   string `json:"latest_block_time"`
					LatestAppHash     string `json:"latest_app_hash"`
					CatchingUp        bool   `json:"catching_up"`
				} `json:"sync_info"`
				ValidatorInfo struct {
					VotingPower string `json:"voting_power"`
				} `json:"validator_info"`
			} `json:"result"`
		}
		if decErr := json.NewDecoder(statusResp.Body).Decode(&cometStatus); decErr == nil {
			chain["block_height"] = cometStatus.Result.SyncInfo.LatestBlockHeight
			chain["block_time"] = cometStatus.Result.SyncInfo.LatestBlockTime
			chain["catching_up"] = cometStatus.Result.SyncInfo.CatchingUp
			chain["chain_id"] = cometStatus.Result.NodeInfo.Network
			chain["moniker"] = cometStatus.Result.NodeInfo.Moniker
			chain["voting_power"] = cometStatus.Result.ValidatorInfo.VotingPower
			chain["app_version"] = cometStatus.Result.NodeInfo.ProtocolVersion.App
			chain["app_hash"] = cometStatus.Result.SyncInfo.LatestAppHash
		}
	}
	// Mempool stats — unconfirmed tx count + total bytes. Same dial/timeout
	// style as /status and /net_info: guard each step, close body on success.
	mempoolReq, _ := http.NewRequestWithContext(r.Context(), "GET", cometRPC+"/num_unconfirmed_txs", nil)
	if mempoolResp, mempoolErr := cometClient.Do(mempoolReq); mempoolErr == nil {
		defer mempoolResp.Body.Close()
		var mempool struct {
			Result struct {
				NTxs       string `json:"n_txs"`
				TotalBytes string `json:"total_bytes"`
			} `json:"result"`
		}
		if decErr := json.NewDecoder(mempoolResp.Body).Decode(&mempool); decErr == nil {
			chain["mempool_txs"] = mempool.Result.NTxs
			chain["mempool_bytes"] = mempool.Result.TotalBytes
		}
	}
	// Derived liveness signals. SAGE runs create_empty_blocks=false, so an IDLE chain
	// (no pending txs, no recent block) is HEALTHY by design — its height simply stops
	// advancing; a chain with PENDING txs but no recent block is STUCK. Surfacing this
	// server-side lets curl/monitoring distinguish the two without re-deriving the rule
	// (see docs/reference/concepts/block-production-and-idle.md). idleAfterSeconds
	// mirrors the dashboard's 30s threshold.
	const idleAfterSeconds = 30
	if bt, ok := chain["block_time"].(string); ok && bt != "" {
		// Skip a height-0 chain that has never produced a block (CometBFT reports the
		// zero time "0001-01-01…"), whose age would be absurd and whose idle state is
		// not yet meaningful.
		if t, perr := time.Parse(time.RFC3339Nano, bt); perr == nil && t.Year() > 1 {
			ageSec := int64(time.Since(t).Seconds())
			chain["last_block_age_seconds"] = ageSec
			// Only derive idle/stuck when the mempool depth is actually KNOWN. If the
			// num_unconfirmed_txs RPC failed, mempool_txs is absent — a missing count
			// would read as 0 and could mislabel a stuck chain (pending txs, no block)
			// as idle. Require the count before flagging either state.
			if s, ok := chain["mempool_txs"].(string); ok {
				// Only derive idle/stuck when the count actually PARSES. A CometBFT
				// RPC error payload decodes to an empty Result, so mempool_txs can be
				// present but "" — treating that as 0 would mislabel an unknown-depth
				// chain as idle. Skip both flags when the depth is not a real number.
				if nTxs, err := strconv.Atoi(s); err == nil {
					chain["idle"] = nTxs == 0 && ageSec > idleAfterSeconds
					chain["stuck"] = nTxs > 0 && ageSec > idleAfterSeconds
				}
			}
		}
	}
	// Peer details
	netReq, _ := http.NewRequestWithContext(r.Context(), "GET", cometRPC+"/net_info", nil)
	if netResp, netErr := cometClient.Do(netReq); netErr == nil {
		defer netResp.Body.Close()
		var netInfo struct {
			Result struct {
				NPeers string `json:"n_peers"`
				Peers  []struct {
					NodeInfo struct {
						Moniker string `json:"moniker"`
						Network string `json:"network"`
						ID      string `json:"id"`
					} `json:"node_info"`
					IsOutbound       bool   `json:"is_outbound"`
					RemoteIP         string `json:"remote_ip"`
					ConnectionStatus struct {
						Duration    string `json:"Duration"`
						SendMonitor struct {
							Bytes string `json:"Bytes"`
						} `json:"SendMonitor"`
						RecvMonitor struct {
							Bytes string `json:"Bytes"`
						} `json:"RecvMonitor"`
					} `json:"connection_status"`
				} `json:"peers"`
			} `json:"result"`
		}
		if decErr := json.NewDecoder(netResp.Body).Decode(&netInfo); decErr == nil {
			chain["peers"] = netInfo.Result.NPeers
			peerList := make([]map[string]any, 0, len(netInfo.Result.Peers))
			for _, p := range netInfo.Result.Peers {
				peerList = append(peerList, map[string]any{
					"moniker":    p.NodeInfo.Moniker,
					"id":         p.NodeInfo.ID[:12],
					"remote_ip":  p.RemoteIP,
					"outbound":   p.IsOutbound,
					"duration":   p.ConnectionStatus.Duration,
					"bytes_sent": p.ConnectionStatus.SendMonitor.Bytes,
					"bytes_recv": p.ConnectionStatus.RecvMonitor.Bytes,
				})
			}
			chain["peer_list"] = peerList
		}
	}
	if len(chain) > 0 {
		health["chain"] = chain
	}

	writeJSONResp(w, http.StatusOK, health)
}

// handleChainValidators proxies the CometBFT validator set for the dashboard.
// The validator set is public chain data (same publicness as /health). On any
// RPC or decode failure it still returns 200 with an empty set + an error
// string so the UI degrades to "--" instead of hard-failing.
func (h *DashboardHandler) handleChainValidators(w http.ResponseWriter, r *http.Request) {
	cometRPC := h.CometBFTRPC
	if cometRPC == "" {
		cometRPC = "http://127.0.0.1:26657"
	}
	fail := func(msg string) {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"count":              0,
			"total_voting_power": "0",
			"validators":         []map[string]any{},
			"error":              msg,
		})
	}

	cometClient := &http.Client{Timeout: 2 * time.Second}
	type cometVal struct {
		Address string `json:"address"`
		PubKey  struct {
			Type  string `json:"type"`
			Value string `json:"value"`
		} `json:"pub_key"`
		VotingPower      string `json:"voting_power"`
		ProposerPriority string `json:"proposer_priority"`
	}
	// Page through /validators (CometBFT caps per_page at 100). Without the
	// loop, count and total_voting_power silently truncate past 100 validators.
	// Bounded to 50 pages (5000 validators) so a misbehaving RPC can't loop us.
	var collected []cometVal
	total := -1
	for page := 1; page <= 50; page++ {
		url := cometRPC + "/validators?per_page=100&page=" + strconv.Itoa(page)
		req, _ := http.NewRequestWithContext(r.Context(), "GET", url, nil)
		resp, err := cometClient.Do(req)
		if err != nil {
			if page == 1 {
				fail(err.Error())
				return
			}
			break // earlier pages already gave us partial data; better than none
		}
		var payload struct {
			Result struct {
				Validators []cometVal `json:"validators"`
				Total      string     `json:"total"`
			} `json:"result"`
		}
		decErr := json.NewDecoder(resp.Body).Decode(&payload)
		resp.Body.Close()
		if decErr != nil {
			if page == 1 {
				fail(decErr.Error())
				return
			}
			break
		}
		if total < 0 {
			if t, perr := strconv.Atoi(payload.Result.Total); perr == nil {
				total = t
			}
		}
		if len(payload.Result.Validators) == 0 {
			break
		}
		collected = append(collected, payload.Result.Validators...)
		if total >= 0 && len(collected) >= total {
			break
		}
	}

	var totalVP int64
	validators := make([]map[string]any, 0, len(collected))
	for _, v := range collected {
		pubKeyBytes, decodeErr := base64.StdEncoding.DecodeString(v.PubKey.Value)
		if decodeErr != nil || len(pubKeyBytes) != ed25519.PublicKeySize {
			fail("validator set contains an invalid Ed25519 public key")
			return
		}
		if vp, perr := strconv.ParseInt(v.VotingPower, 10, 64); perr == nil {
			totalVP += vp
		}
		validators = append(validators, map[string]any{
			"address": v.Address,
			// Scope governance and vote transactions identify a validator by
			// lowercase hex(Ed25519 public key), not by CometBFT's 20-byte
			// address and not necessarily by the local dashboard agent ID.
			"agent_id":          auth.PublicKeyToAgentID(ed25519.PublicKey(pubKeyBytes)),
			"pub_key":           v.PubKey,
			"voting_power":      v.VotingPower,
			"proposer_priority": v.ProposerPriority,
		})
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"count":              len(validators),
		"total_voting_power": strconv.FormatInt(totalVP, 10),
		"validators":         validators,
	})
}

var startTime = time.Now()

func writeJSONResp(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v) //nolint:errcheck
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSONResp(w, status, map[string]string{"error": msg})
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// handleGetCleanupSettings returns the current cleanup configuration.
func (h *DashboardHandler) handleGetCleanupSettings(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}

	prefs, err := h.prefStore.GetAllPreferences(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	cfg := memory.CleanupConfigFromPrefs(prefs)

	// Also include last run info
	resp := map[string]any{
		"config":      cfg,
		"last_run":    prefs["cleanup_last_run"],
		"last_result": prefs["cleanup_last_result"],
	}

	writeJSONResp(w, http.StatusOK, resp)
}

// handleSaveCleanupSettings saves the cleanup configuration.
func (h *DashboardHandler) handleSaveCleanupSettings(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}

	var cfg memory.CleanupConfig
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Validate bounds
	if cfg.ObservationTTLDays < 1 {
		cfg.ObservationTTLDays = 1
	}
	if cfg.SessionTTLDays < 1 {
		cfg.SessionTTLDays = 1
	}
	if cfg.StaleThreshold < 0.01 {
		cfg.StaleThreshold = 0.01
	}
	if cfg.StaleThreshold > 0.5 {
		cfg.StaleThreshold = 0.5
	}
	if cfg.CleanupIntervalHours < 1 {
		cfg.CleanupIntervalHours = 1
	}

	prefs := memory.CleanupConfigToPrefs(cfg)
	for k, v := range prefs {
		if err := h.prefStore.SetPreference(r.Context(), k, v); err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	}

	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "config": cfg})
}

// handleRunCleanup triggers an on-demand cleanup (supports dry_run).
func (h *DashboardHandler) handleRunCleanup(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}

	var body struct {
		DryRun bool `json:"dry_run"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		// Default to dry run for safety
		body.DryRun = true
	}

	prefs, err := h.prefStore.GetAllPreferences(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	cfg := memory.CleanupConfigFromPrefs(prefs)
	// For manual runs, force enabled so it actually runs
	cfg.Enabled = true

	result, err := memory.RunCleanup(r.Context(), h.prefStore, cfg, body.DryRun)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, result)
}

// handleGetBootInstructions returns the custom boot instructions for MCP inception.
func (h *DashboardHandler) handleGetBootInstructions(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}
	instructions, err := h.prefStore.GetPreference(r.Context(), "boot_instructions")
	if err != nil {
		instructions = ""
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"instructions": instructions})
}

// handleSaveBootInstructions saves custom boot instructions for MCP inception.
func (h *DashboardHandler) handleSaveBootInstructions(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}
	var body struct {
		Instructions string `json:"instructions"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := h.prefStore.SetPreference(r.Context(), "boot_instructions", body.Instructions); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "instructions": body.Instructions})
}

// handleGetRecallSettings returns the current recall tuning parameters.
func (h *DashboardHandler) handleGetRecallSettings(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}

	prefs, err := h.prefStore.GetAllPreferences(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	topK := 5
	if v, ok := prefs["recall_top_k"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			topK = n
		}
	}

	confidence := 70 // Default 70% — catches observations (0.80+) and inferences (0.60+), not just facts
	if v, ok := prefs["recall_min_confidence"]; ok {
		if n, err := strconv.Atoi(v); err == nil {
			confidence = n
		}
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"top_k":          topK,
		"min_confidence": confidence,
	})
}

// handleSaveRecallSettings saves recall tuning parameters.
func (h *DashboardHandler) handleSaveRecallSettings(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}

	var body struct {
		TopK          int `json:"top_k"`
		MinConfidence int `json:"min_confidence"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	// Clamp to valid ranges. k tops out at 20: with the reranker on, high k
	// stays sharp (over-sample + re-score); the floor of 3 keeps recall from
	// being starved into uselessness.
	if body.TopK < 3 {
		body.TopK = 3
	}
	if body.TopK > 20 {
		body.TopK = 20
	}
	// Floor 50: low enough to include inferences (0.60+) and the documented
	// 70% default - the old floor of 85 made the default unrepresentable the
	// moment anyone pressed Save.
	if body.MinConfidence < 50 {
		body.MinConfidence = 50
	}
	if body.MinConfidence > 100 {
		body.MinConfidence = 100
	}

	if err := h.prefStore.SetPreference(r.Context(), "recall_top_k", strconv.Itoa(body.TopK)); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if err := h.prefStore.SetPreference(r.Context(), "recall_min_confidence", strconv.Itoa(body.MinConfidence)); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":             true,
		"top_k":          body.TopK,
		"min_confidence": body.MinConfidence,
	})
}

// handleGetMemoryMode returns the current memory mode setting.
func (h *DashboardHandler) handleGetMemoryMode(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}
	mode, err := h.prefStore.GetPreference(r.Context(), "memory_mode")
	if err != nil || mode == "" {
		mode = "full"
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"mode": mode})
}

// handleSaveMemoryMode saves the memory mode setting.
// Valid modes: "full" (sage_turn every turn), "bookend" (inception + reflect only),
// or "on-demand" (no automatic calls — user manually triggers recall/reflect).
// Also writes ~/.sage/memory_mode flag file so hook scripts can read it without an API call.
func (h *DashboardHandler) handleSaveMemoryMode(w http.ResponseWriter, r *http.Request) {
	if h.prefStore == nil {
		writeError(w, http.StatusNotImplemented, "preferences not available")
		return
	}
	var body struct {
		Mode string `json:"mode"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Mode != "full" && body.Mode != "bookend" && body.Mode != "on-demand" {
		writeError(w, http.StatusBadRequest, "mode must be 'full', 'bookend', or 'on-demand'")
		return
	}
	if err := h.prefStore.SetPreference(r.Context(), "memory_mode", body.Mode); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	// Write flag file so hook scripts can check the mode without an API call.
	// This is the bridge between server-side preference and client-side hooks.
	flagSynced := false
	if sageHome := os.Getenv("SAGE_HOME"); sageHome != "" {
		if err := os.WriteFile(filepath.Join(sageHome, "memory_mode"), []byte(body.Mode), 0600); err == nil { //nolint:gosec // trusted local path
			flagSynced = true
		}
	} else if home, err := os.UserHomeDir(); err == nil {
		sageHome := filepath.Join(home, ".sage")
		if err := os.WriteFile(filepath.Join(sageHome, "memory_mode"), []byte(body.Mode), 0600); err == nil { //nolint:gosec // trusted local path
			flagSynced = true
		}
	}

	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "mode": body.Mode, "flag_synced": flagSynced})
}
