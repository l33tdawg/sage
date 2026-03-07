package web

import (
	"encoding/json"
	"io/fs"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/store"
)

// DashboardHandler serves the Brain Dashboard UI and its API endpoints.
type DashboardHandler struct {
	store     store.MemoryStore
	SSE       *SSEBroadcaster
	Version   string
	Encrypted bool
}

// NewDashboardHandler creates a new dashboard handler.
func NewDashboardHandler(memStore store.MemoryStore, version string) *DashboardHandler {
	return &DashboardHandler{
		store:   memStore,
		SSE:     NewSSEBroadcaster(),
		Version: version,
	}
}

// RegisterRoutes mounts dashboard routes on the given router.
func (h *DashboardHandler) RegisterRoutes(r chi.Router) {
	// Dashboard API endpoints (no auth required — local dashboard)
	r.Get("/v1/dashboard/memory/list", h.handleListMemories)
	r.Get("/v1/dashboard/memory/timeline", h.handleTimeline)
	r.Get("/v1/dashboard/memory/graph", h.handleGraph)
	r.Get("/v1/dashboard/stats", h.handleStats)
	r.Delete("/v1/dashboard/memory/{id}", h.handleDeleteMemory)
	r.Patch("/v1/dashboard/memory/{id}", h.handleUpdateMemory)
	r.Get("/v1/dashboard/events", h.SSE.ServeHTTP)
	r.Get("/v1/dashboard/health", h.handleHealth)

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
		}

		w.Write(f) //nolint:errcheck
	})

	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "/ui/", http.StatusFound)
	})
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
		DomainTag: q.Get("domain"),
		Provider:  q.Get("provider"),
		Status:    q.Get("status"),
		Limit:     limit,
		Offset:    offset,
		Sort:      q.Get("sort"),
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

// handleUpdateMemory updates a memory's domain tag.
func (h *DashboardHandler) handleUpdateMemory(w http.ResponseWriter, r *http.Request) {
	id := chi.URLParam(r, "id")
	if id == "" {
		writeError(w, http.StatusBadRequest, "missing memory id")
		return
	}

	var body struct {
		Domain string `json:"domain"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.Domain == "" {
		writeError(w, http.StatusBadRequest, "domain is required")
		return
	}
	if err := h.store.UpdateDomainTag(r.Context(), id, body.Domain); err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]string{"status": "updated"})
}

// handleHealth returns system health including Ollama status.
func (h *DashboardHandler) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]any{
		"sage":      "running",
		"version":   h.Version,
		"encrypted": h.Encrypted,
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
