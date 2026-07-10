package web

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/l33tdawg/sage/internal/store"
)

// handlePipelineSend lets a HUMAN operator send a note/instruction to a specific
// agent through the pipe bus. The agent-side POST /v1/pipe/send stamps the sender
// from a bearer token, so there was no human path at all. Session-authed; the note
// is stamped from a friendly "operator" origin so it renders cleanly in the
// agent's inbox (sage_inbox / sage_turn) and never trips the FromAgent[:16] slice.
func (h *DashboardHandler) handlePipelineSend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ToAgent string `json:"to_agent"`
		Intent  string `json:"intent"`
		Payload string `json:"payload"`
		TTLMin  int    `json:"ttl_minutes"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if req.ToAgent == "" {
		writeError(w, http.StatusBadRequest, "to_agent is required")
		return
	}
	if req.Payload == "" {
		writeError(w, http.StatusBadRequest, "payload (the note) is required")
		return
	}
	// Size cap (E8c) — fast-fail before the store; the store re-checks.
	if len(req.Payload) > store.MaxPipeContentBytes {
		writeError(w, http.StatusRequestEntityTooLarge, "payload exceeds the maximum size")
		return
	}
	if req.Intent == "" {
		req.Intent = "note"
	}
	ttl := req.TTLMin
	if ttl <= 0 {
		ttl = 1440 // 24h default
	}
	if ttl > 1440 {
		ttl = 1440 // match the agent send path; bound pipe lifetime so a huge TTL can't outlive GC
	}
	pipeStore, ok := h.store.(store.PipelineStore)
	if !ok {
		writeError(w, http.StatusInternalServerError, "pipeline not available on this store")
		return
	}
	now := time.Now().UTC()
	msg := &store.PipelineMessage{
		PipeID:       "pipe-" + uuid.New().String(),
		FromAgent:    "operator",
		FromProvider: "operator",
		ToAgent:      req.ToAgent,
		Intent:       req.Intent,
		Payload:      req.Payload,
		Status:       "pending",
		CreatedAt:    now,
		ExpiresAt:    now.Add(time.Duration(ttl) * time.Minute),
	}
	if err := pipeStore.InsertPipeline(r.Context(), msg); err != nil {
		switch {
		case errors.Is(err, store.ErrPipePayloadTooLarge), errors.Is(err, store.ErrPipeIntentTooLarge):
			writeError(w, http.StatusRequestEntityTooLarge, err.Error())
		case errors.Is(err, store.ErrPipeQuotaPerAgent), errors.Is(err, store.ErrPipeQuotaGlobal):
			writeError(w, http.StatusTooManyRequests, err.Error())
		default:
			writeError(w, http.StatusInternalServerError, err.Error())
		}
		return
	}
	writeJSONResp(w, http.StatusCreated, map[string]any{"pipe_id": msg.PipeID, "to_agent": req.ToAgent, "status": "pending"})
}

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
