package web

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/l33tdawg/sage/internal/store"
)

// pipelineItemView is the enriched view of a pipeline message for the dashboard.
type pipelineItemView struct {
	store.PipelineMessage
	FromName string `json:"from_name,omitempty"`
	ToName   string `json:"to_name,omitempty"`
}

// handlePipelineList returns recent pipeline messages for the dashboard.
func (h *DashboardHandler) handlePipelineList(w http.ResponseWriter, r *http.Request) {
	pipeStore, ok := h.store.(store.PipelineStore)
	if !ok {
		writeJSONDash(w, http.StatusOK, map[string]any{
			"items": []any{},
			"count": 0,
		})
		return
	}

	status := r.URL.Query().Get("status")
	limit := 50
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 200 {
			limit = n
		}
	}

	items, err := pipeStore.ListPipelines(r.Context(), status, limit)
	if err != nil {
		writeJSONDash(w, http.StatusInternalServerError, map[string]any{
			"error": err.Error(),
		})
		return
	}
	if items == nil {
		items = []*store.PipelineMessage{}
	}

	// Enrich with agent names
	agentNames := h.buildAgentNameMap(r.Context(), items)
	views := make([]pipelineItemView, len(items))
	for i, item := range items {
		views[i] = pipelineItemView{PipelineMessage: *item}
		if name, ok := agentNames[item.FromAgent]; ok {
			views[i].FromName = name
		}
		if name, ok := agentNames[item.ToAgent]; ok {
			views[i].ToName = name
		}
	}

	writeJSONDash(w, http.StatusOK, map[string]any{
		"items": views,
		"count": len(views),
	})
}

// buildAgentNameMap collects unique agent IDs from pipeline items and resolves them to names.
func (h *DashboardHandler) buildAgentNameMap(ctx context.Context, items []*store.PipelineMessage) map[string]string {
	names := make(map[string]string)
	agentStore, ok := h.store.(store.AgentStore)
	if !ok {
		return names
	}

	// Collect unique agent IDs
	seen := make(map[string]bool)
	for _, item := range items {
		if item.FromAgent != "" && !seen[item.FromAgent] {
			seen[item.FromAgent] = true
		}
		if item.ToAgent != "" && !seen[item.ToAgent] {
			seen[item.ToAgent] = true
		}
	}

	// Resolve each
	for id := range seen {
		if agent, err := agentStore.GetAgent(ctx, id); err == nil && agent.Name != "" {
			names[id] = agent.Name
		}
	}
	return names
}

// handlePipelineStats returns pipeline status counts for the dashboard.
func (h *DashboardHandler) handlePipelineStats(w http.ResponseWriter, r *http.Request) {
	pipeStore, ok := h.store.(store.PipelineStore)
	if !ok {
		writeJSONDash(w, http.StatusOK, map[string]any{
			"stats": map[string]int{},
			"total": 0,
		})
		return
	}

	stats, err := pipeStore.PipelineStats(r.Context())
	if err != nil {
		writeJSONDash(w, http.StatusInternalServerError, map[string]any{
			"error": err.Error(),
		})
		return
	}

	total := 0
	for _, v := range stats {
		total += v
	}

	writeJSONDash(w, http.StatusOK, map[string]any{
		"stats": stats,
		"total": total,
	})
}

// writeJSONDash writes a JSON response (dashboard-specific to avoid name collision).
func writeJSONDash(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
