package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"io/fs"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/vault"
)

// PreferencesStore defines methods for preferences and cleanup operations.
type PreferencesStore interface {
	GetPreference(ctx context.Context, key string) (string, error)
	SetPreference(ctx context.Context, key, value string) error
	GetAllPreferences(ctx context.Context) (map[string]string, error)
	GetCleanupCandidates(ctx context.Context, observationTTLDays int, sessionTTLDays int, staleThreshold float64) ([]*memory.MemoryRecord, error)
	DeprecateMemories(ctx context.Context, memoryIDs []string) (int, error)
}

// Embedder generates vector embeddings for text content.
type Embedder interface {
	Embed(ctx context.Context, text string) ([]float32, error)
}

// DashboardHandler serves the CEREBRUM dashboard UI and its API endpoints.
type DashboardHandler struct {
	store     store.MemoryStore
	prefStore PreferencesStore
	embedder  Embedder
	SSE       *SSEBroadcaster
	Version   string
	ExecPath  string // path to sage-gui binary, used by /v1/mcp-config
	Encrypted   atomic.Bool
	VaultLocked atomic.Bool // true when encryption is enabled but vault hasn't been unlocked yet

	// Auth — only active when Encrypted is true.
	VaultKeyPath   string
	sessions       sync.Map // token -> expiry time
	loginAttempts  sync.Map // IP -> []time.Time

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
	SigningKey   ed25519.PrivateKey

	// BadgerStore — when set, on-chain RBAC is enforced on dashboard endpoints
	// when requests include X-Agent-ID headers (i.e. MCP agent requests).
	BadgerStore *store.BadgerStore

	// pendingImports holds parsed records from preview, keyed by import ID.
	pendingImports sync.Map // string -> *pendingImport

	// PreValidateFunc runs the 4 app validators against proposed content without
	// submitting it on-chain. Set during node startup to share validator logic.
	PreValidateFunc func(content, contentHash, domain, memType string, confidence float64) []PreValidateVote
}

// RedeployOrchestrator extends RedeployChecker with deploy/status methods
// for the network redeploy endpoint. Implemented by *orchestrator.Redeployer.
type RedeployOrchestrator interface {
	RedeployChecker
	DeployOp(ctx context.Context, op, agentID string) error
	GetRedeployStatus(ctx context.Context) (active bool, operation, agentID string, err error)
}

// PreValidateVote represents a single validator's vote result from pre-validation.
type PreValidateVote struct {
	Validator string `json:"validator"`
	Decision  string `json:"decision"`
	Reason    string `json:"reason"`
}

// resolveAgentRBAC checks whether the request comes from an authenticated MCP agent
// (X-Agent-ID header present) and resolves on-chain RBAC visibility.
// Returns (allowedAgents, seeAll). If no agent header or no BadgerStore, returns (nil, true)
// meaning no filtering (human dashboard user).
func (h *DashboardHandler) resolveAgentRBAC(r *http.Request) ([]string, bool) {
	agentID := strings.TrimSpace(r.Header.Get("X-Agent-ID"))
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
		store:   memStore,
		SSE:     NewSSEBroadcaster(),
		Version: version,
		Pairing: NewPairingStore(),
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

// handlePreValidate runs the 4 app validators against proposed content
// without actually submitting it on-chain. Returns per-validator results
// and quorum outcome.
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
		"accepted": acceptCount >= 3, // BFT quorum: 3 of 4
		"votes":    votes,
		"quorum":   fmt.Sprintf("%d/4", acceptCount),
	})
}

const sessionCookieName = "sage_session"
const sessionTTL = 24 * time.Hour

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
		r.Get("/v1/dashboard/settings/cleanup", h.handleGetCleanupSettings)
		r.Post("/v1/dashboard/settings/cleanup", h.handleSaveCleanupSettings)
		r.Post("/v1/dashboard/cleanup/run", h.handleRunCleanup)
		r.Get("/v1/dashboard/settings/boot-instructions", h.handleGetBootInstructions)
		r.Post("/v1/dashboard/settings/boot-instructions", h.handleSaveBootInstructions)
		r.Get("/v1/dashboard/settings/memory-mode", h.handleGetMemoryMode)
		r.Post("/v1/dashboard/settings/memory-mode", h.handleSaveMemoryMode)

		// Task backlog
		r.Get("/v1/dashboard/tasks", h.handleGetTasks)
		r.Post("/v1/dashboard/tasks", h.handleCreateTaskDashboard)
		r.Put("/v1/dashboard/tasks/{id}/status", h.handleUpdateTaskStatusDashboard)

		// Tags
		r.Get("/v1/dashboard/tags", h.handleListTags)
		r.Get("/v1/dashboard/memory/{id}/tags", h.handleGetMemoryTags)
		r.Put("/v1/dashboard/memory/{id}/tags", h.handleSetMemoryTags)

		// Auto-start (open at login)
		r.Get("/v1/dashboard/settings/autostart", h.handleGetAutostart)
		r.Post("/v1/dashboard/settings/autostart", h.handleSetAutostart)

		// Software Update
		r.Get("/v1/dashboard/settings/update/check", h.handleCheckUpdate)
		r.Post("/v1/dashboard/settings/update/apply", h.handleApplyUpdate)
		r.Post("/v1/dashboard/settings/update/restart", h.handleRestart)

		// Synaptic Ledger (encryption vault) management
		r.Get("/v1/dashboard/settings/ledger", h.handleGetLedgerStatus)
		r.Post("/v1/dashboard/settings/ledger/enable", h.handleEnableLedger)
		r.Post("/v1/dashboard/settings/ledger/change-passphrase", h.handleChangePassphrase)
		r.Post("/v1/dashboard/settings/ledger/disable", h.handleDisableLedger)

		// Pre-validate endpoint — dry-run the 4 app validators
		r.Post("/v1/memory/pre-validate", h.handlePreValidate)

		// Pipeline — agent-to-agent message bus
		r.Get("/v1/dashboard/pipeline", h.handlePipelineList)
		r.Get("/v1/dashboard/pipeline/stats", h.handlePipelineStats)

		// Network agent management routes
		h.RegisterNetworkRoutes(r)

		// Governance routes
		h.RegisterGovernanceRoutes(r)
	})

	// Launch endpoint — redirects to CEREBRUM dashboard.
	// The dock/tray app opens this URL; simple redirect avoids popup-blocker issues on macOS Tahoe+.
	r.Get("/ui/launch", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})

	// SPA — serve static files, fallback to index.html
	staticFS, _ := fs.Sub(StaticFS, "static")
	fileServer := http.FileServer(http.FS(staticFS))

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

		w.Write(f) //nolint:errcheck,gosec // static embedded file, not user input
	})

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	}) // end securityHeaders group
}

// authMiddleware checks for a valid session cookie when encryption is active.
// Always wired in the middleware chain — skips auth dynamically when encryption is off.
// MCP agents authenticate via Ed25519 signatures (X-Agent-ID + X-Signature + X-Timestamp)
// and bypass the session cookie requirement.
func (h *DashboardHandler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.Encrypted.Load() {
			next.ServeHTTP(w, r)
			return
		}
		// Allow MCP agents with valid Ed25519 signatures to bypass cookie auth.
		if h.validAgentSignature(r) {
			next.ServeHTTP(w, r)
			return
		}
		cookie, err := r.Cookie(sessionCookieName)
		if err != nil || !h.validSession(cookie.Value) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusUnauthorized)
			json.NewEncoder(w).Encode(map[string]any{"error": "unauthorized", "login_required": true}) //nolint:errcheck
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
	return auth.VerifyRequest(pubKey, r.Method, reqPath, body, tsUnix, sig)
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

	// Create session
	token := generateToken()
	h.sessions.Store(token, time.Now().Add(sessionTTL))

	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    token,
		Path:     "/",
		MaxAge:   int(sessionTTL.Seconds()),
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
	})

	writeJSONResp(w, http.StatusOK, map[string]any{"ok": true})
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

	// Clear the cookie.
	http.SetCookie(w, &http.Cookie{
		Name:     sessionCookieName,
		Value:    "",
		Path:     "/",
		MaxAge:   -1,
		HttpOnly: true,
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

	opts := store.ListOptions{
		DomainTag:       q.Get("domain"),
		Tag:             q.Get("tag"),
		Provider:        q.Get("provider"),
		Status:          q.Get("status"),
		SubmittingAgent: q.Get("agent"),
		Limit:           limit,
		Offset:          offset,
		Sort:            q.Get("sort"),
	}

	// On-chain RBAC: if request comes from an MCP agent, enforce agent isolation.
	if allowedAgents, seeAll := h.resolveAgentRBAC(r); !seeAll {
		// Grant-aware override: skip agent isolation when agent has a grant OR domain is unregistered
		listAgentID := strings.TrimSpace(r.Header.Get("X-Agent-ID"))
		if opts.DomainTag != "" && h.BadgerStore != nil && listAgentID != "" {
			hasGrant, _ := h.BadgerStore.HasAccess(opts.DomainTag, listAgentID, 1, time.Now())
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

	records, total, err := h.store.ListMemories(r.Context(), opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"memories": records,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

// handleExport streams ALL memories as JSONL (one JSON object per line).
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
		records, _, err := h.store.ListMemories(r.Context(), store.ListOptions{
			Limit:  pageSize,
			Offset: offset,
			Sort:   "oldest",
		})
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

	buckets, err := h.store.GetTimeline(r.Context(), from, to, domain, bucket)
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
}

// graphEdge connects two memories.
type graphEdge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"` // "domain", "parent", "triple"
}

// handleGraph returns all memories with edges for force-directed layout.
func (h *DashboardHandler) handleGraph(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 500 {
		limit = 500
	}

	opts := store.ListOptions{
		Limit:  limit,
		Sort:   "newest",
		Status: q.Get("status"), // Allow frontend to filter by status
	}
	// Default: exclude deprecated memories from graph view
	if opts.Status == "" {
		opts.Status = "committed"
	}
	// On-chain RBAC: if request comes from an MCP agent, enforce agent isolation.
	if allowedAgents, seeAll := h.resolveAgentRBAC(r); !seeAll {
		opts.SubmittingAgents = allowedAgents
	}

	records, _, err := h.store.ListMemories(r.Context(), opts)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	nodes := make([]graphNode, 0, len(records))
	edges := make([]graphEdge, 0)

	// Batch-fetch tags for all memories
	memIDs := make([]string, len(records))
	for i, rec := range records {
		memIDs[i] = rec.MemoryID
	}
	tagMap, _ := h.store.GetTagsBatch(r.Context(), memIDs)

	// Build domain groups for edge generation
	domainMemories := make(map[string][]string)
	for _, rec := range records {
		nodes = append(nodes, graphNode{
			ID:         rec.MemoryID,
			Content:    truncate(rec.Content, 200),
			Domain:     rec.DomainTag,
			Confidence: rec.ConfidenceScore,
			Status:     string(rec.Status),
			MemoryType: string(rec.MemoryType),
			CreatedAt:  rec.CreatedAt.Format(time.RFC3339),
			Agent:      rec.SubmittingAgent,
			Tags:       tagMap[rec.MemoryID],
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

	writeJSONResp(w, http.StatusOK, map[string]any{
		"nodes": nodes,
		"edges": edges,
	})
}

// handleStats returns aggregate statistics.
func (h *DashboardHandler) handleStats(w http.ResponseWriter, r *http.Request) {
	stats, err := h.store.GetStats(r.Context())
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, stats)
}

// handleDeleteMemory deprecates a memory.
func (h *DashboardHandler) handleDeleteMemory(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing memory id")
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
		Agent   string   `json:"agent,omitempty"`
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
	if body.Domain == "" && len(body.AddTags) == 0 && body.Agent == "" {
		writeError(w, http.StatusBadRequest, "domain, add_tags, or agent is required")
		return
	}

	ctx := r.Context()
	updated := 0
	var firstErr error

	for _, id := range body.IDs {
		if body.Domain != "" {
			if err := h.store.UpdateDomainTag(ctx, id, body.Domain); err != nil {
				if firstErr == nil {
					firstErr = err
				}
				continue
			}
		}
		if body.Agent != "" {
			if err := h.store.UpdateMemoryAgent(ctx, id, body.Agent); err != nil {
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
		writeError(w, http.StatusInternalServerError, firstErr.Error())
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
		limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
		if limit <= 0 {
			limit = 100
		}
		tasks, err = h.store.GetAllTasks(r.Context(), domain, limit)
	} else {
		provider := r.URL.Query().Get("provider")
		tasks, err = h.store.GetOpenTasks(r.Context(), domain, provider)
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
	}
	results := make([]taskResult, 0, len(tasks))
	for _, t := range tasks {
		results = append(results, taskResult{
			MemoryID:        t.MemoryID,
			Content:         t.Content,
			DomainTag:       t.DomainTag,
			TaskStatus:      string(t.TaskStatus),
			ConfidenceScore: t.ConfidenceScore,
			CreatedAt:       t.CreatedAt.Format("2006-01-02T15:04:05Z"),
		})
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"tasks": results, "total": len(results)})
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
	if err := h.store.UpdateTaskStatus(r.Context(), id, ts); err != nil {
		writeError(w, http.StatusNotFound, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]string{"memory_id": id, "task_status": body.TaskStatus})
}

// handleCreateTaskDashboard creates a new task from the CEREBRUM dashboard.
func (h *DashboardHandler) handleCreateTaskDashboard(w http.ResponseWriter, r *http.Request) {
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

	taskContent := "[TASK] " + body.Content
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
		MemoryID:        memoryID,
		Content:         taskContent,
		ContentHash:     contentHash[:],
		MemoryType:      memory.TypeTask,
		DomainTag:       body.Domain,
		ConfidenceScore: 0.90,
		TaskStatus:      memory.TaskStatusPlanned,
		CreatedAt:       time.Now(),
		Embedding:       embedding,
	}

	// Broadcast on-chain through CometBFT consensus
	if h.CometBFTRPC != "" && h.SigningKey != nil {
		submitTx := &tx.ParsedTx{
			Type:      tx.TxTypeMemorySubmit,
			Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
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
		embedDashboardAgentProof(submitTx, h.SigningKey)
		if signErr := tx.SignTx(submitTx, h.SigningKey); signErr == nil {
			if encoded, encErr := tx.EncodeTx(submitTx); encErr == nil {
				_ = broadcastTxSync(h.CometBFTRPC, encoded)
			}
		}
	}

	if err := h.store.InsertMemory(r.Context(), rec); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
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
		"encrypted":    h.Encrypted.Load(),
		"vault_locked": h.VaultLocked.Load(),
		"uptime":       time.Since(startTime).String(),
	}

	// Check Ollama
	client := &http.Client{Timeout: 2 * time.Second}
	ollamaReq, _ := http.NewRequestWithContext(r.Context(), "GET", "http://localhost:11434/api/tags", nil)
	resp, err := client.Do(ollamaReq)
	if err != nil {
		health["ollama"] = "offline"
	} else {
		resp.Body.Close()
		health["ollama"] = "running"
	}

	// Get memory stats
	stats, err := h.store.GetStats(r.Context())
	if err == nil {
		health["memories"] = stats
	}

	// CometBFT chain stats
	chain := map[string]any{}
	cometClient := &http.Client{Timeout: 2 * time.Second}
	statusReq, _ := http.NewRequestWithContext(r.Context(), "GET", "http://127.0.0.1:26657/status", nil)
	if statusResp, statusErr := cometClient.Do(statusReq); statusErr == nil {
		defer statusResp.Body.Close()
		var cometStatus struct {
			Result struct {
				NodeInfo struct {
					Network string `json:"network"`
					Moniker string `json:"moniker"`
				} `json:"node_info"`
				SyncInfo struct {
					LatestBlockHeight string `json:"latest_block_height"`
					LatestBlockTime   string `json:"latest_block_time"`
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
		}
	}
	// Peer details
	netReq, _ := http.NewRequestWithContext(r.Context(), "GET", "http://127.0.0.1:26657/net_info", nil)
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
		"config":     cfg,
		"last_run":   prefs["cleanup_last_run"],
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

	// Clamp to valid ranges
	if body.TopK < 4 {
		body.TopK = 4
	}
	if body.TopK > 10 {
		body.TopK = 10
	}
	if body.MinConfidence < 85 {
		body.MinConfidence = 85
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
