package web

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io/fs"
	"net"
	"net/http"
	"strconv"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
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
	Encrypted atomic.Bool

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

	// pendingImports holds parsed records from preview, keyed by import ID.
	pendingImports sync.Map // string -> *pendingImport
}

// RedeployOrchestrator extends RedeployChecker with deploy/status methods
// for the network redeploy endpoint. Implemented by *orchestrator.Redeployer.
type RedeployOrchestrator interface {
	RedeployChecker
	DeployOp(ctx context.Context, op, agentID string) error
	GetRedeployStatus(ctx context.Context) (active bool, operation, agentID string, err error)
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

		// Pairing redemption — unauthenticated (the code IS the auth).
		h.RegisterPairingRoutes(r)

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

		// Task backlog
		r.Get("/v1/dashboard/tasks", h.handleGetTasks)
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

		// Network agent management routes
		h.RegisterNetworkRoutes(r)
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

		w.Write(f) //nolint:errcheck
	})

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
	}) // end securityHeaders group
}

// authMiddleware checks for a valid session cookie when encryption is active.
// Always wired in the middleware chain — skips auth dynamically when encryption is off.
func (h *DashboardHandler) authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !h.Encrypted.Load() {
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
	_, err := vault.Open(h.VaultKeyPath, req.Passphrase)
	if err != nil {
		writeJSONResp(w, http.StatusUnauthorized, map[string]any{"ok": false, "error": "wrong passphrase"})
		return
	}

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
	ID         string  `json:"id"`
	Content    string  `json:"content"`
	Domain     string  `json:"domain"`
	Confidence float64 `json:"confidence"`
	Status     string  `json:"status"`
	MemoryType string  `json:"memory_type"`
	CreatedAt  string  `json:"created_at"`
	Agent      string  `json:"agent"`
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

	records, _, err := h.store.ListMemories(r.Context(), store.ListOptions{
		Limit: limit,
		Sort:  "newest",
	})
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}

	nodes := make([]graphNode, 0, len(records))
	edges := make([]graphEdge, 0)

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

// handleHealth returns system health including Ollama status.
func (h *DashboardHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]any{
		"sage":      "running",
		"version":   h.Version,
		"encrypted": h.Encrypted.Load(),
		"uptime":    time.Since(startTime).String(),
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

	confidence := 95
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
