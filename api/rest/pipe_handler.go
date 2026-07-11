package rest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	// pipeQuotaProblemType marks the 429 returned when a requester (or the node
	// as a whole) has too many open pipes — kept distinct from the rate
	// limiter's status-derived 429 so clients can tell a storage quota breach
	// ("you have too many pipes in flight") from request-rate shaping.
	pipeQuotaProblemType = "https://sage.dev/errors/pipe-quota"
	// pipeTooLargeProblemType marks the 413 returned when a pipe payload,
	// intent, or result exceeds its size cap.
	pipeTooLargeProblemType = "https://sage.dev/errors/pipe-too-large"
)

// handlePipeSend creates a pipeline message addressed to another agent/provider.
func (s *Server) handlePipeSend(w http.ResponseWriter, r *http.Request) {
	var req struct {
		ToAgent    string `json:"to_agent"`
		ToProvider string `json:"to_provider"`
		Intent     string `json:"intent"`
		Payload    string `json:"payload"`
		TTLMinutes int    `json:"ttl_minutes"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Payload == "" {
		writeProblem(w, http.StatusBadRequest, "Missing payload", "payload is required")
		return
	}
	if req.ToAgent == "" && req.ToProvider == "" {
		writeProblem(w, http.StatusBadRequest, "Missing target", "to_agent or to_provider is required")
		return
	}
	// Size caps (E8c) — fast-fail before the store; the store re-checks so the
	// MCP and dashboard paths are covered too.
	if len(req.Payload) > store.MaxPipeContentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Payload too large", fmt.Sprintf("payload exceeds the %d byte limit.", store.MaxPipeContentBytes))
		return
	}
	if len(req.Intent) > store.MaxPipeIntentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Intent too large", fmt.Sprintf("intent exceeds the %d byte limit.", store.MaxPipeIntentBytes))
		return
	}

	// Validate the target agent/provider actually exists
	if s.agentStore != nil {
		if req.ToAgent != "" {
			// Direct agent_id — must exist
			if _, err := s.agentStore.GetAgent(r.Context(), req.ToAgent); err != nil {
				writeProblem(w, http.StatusNotFound, "Unknown target agent",
					fmt.Sprintf("agent_id %s is not registered", req.ToAgent))
				return
			}
		} else if req.ToProvider != "" {
			// Provider — check if any active agent has this provider
			agents, err := s.agentStore.ListAgents(r.Context())
			if err == nil {
				found := false
				for _, a := range agents {
					if a.Provider == req.ToProvider {
						found = true
						break
					}
				}
				if !found {
					// Also try as agent name
					if agent, _ := s.agentStore.GetAgentByName(r.Context(), req.ToProvider); agent != nil {
						// Resolve name → agent_id for direct delivery
						req.ToAgent = agent.AgentID
						req.ToProvider = ""
					} else {
						writeProblem(w, http.StatusNotFound, "Unknown target",
							fmt.Sprintf("no registered agent with provider or name %q", req.ToProvider))
						return
					}
				}
			}
		}
	}

	ttl := req.TTLMinutes
	if ttl <= 0 {
		ttl = 60
	}
	if ttl > 1440 {
		ttl = 1440
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up sender's provider from agent registry
	fromProvider := ""
	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), agentID); err == nil {
			fromProvider = agent.Provider
		}
	}

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	now := time.Now().UTC()
	msg := &store.PipelineMessage{
		PipeID:       generatePipeID(),
		FromAgent:    agentID,
		FromProvider: fromProvider,
		ToAgent:      req.ToAgent,
		ToProvider:   req.ToProvider,
		Intent:       req.Intent,
		Payload:      req.Payload,
		Status:       "pending",
		CreatedAt:    now,
		ExpiresAt:    now.Add(time.Duration(ttl) * time.Minute),
	}

	if err := pipeStore.InsertPipeline(r.Context(), msg); err != nil {
		switch {
		case errors.Is(err, store.ErrPipePayloadTooLarge), errors.Is(err, store.ErrPipeIntentTooLarge):
			writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType, "Pipeline content too large", err.Error())
		case errors.Is(err, store.ErrPipeQuotaPerAgent), errors.Is(err, store.ErrPipeQuotaGlobal):
			// Storage quota, not chain backpressure: tell the caller to drain
			// their in-flight pipes before sending more.
			w.Header().Set("Retry-After", "5")
			writeProblemTyped(w, http.StatusTooManyRequests, pipeQuotaProblemType, "Too many open pipelines", err.Error())
		default:
			writeProblem(w, http.StatusInternalServerError, "Pipeline insert failed", err.Error())
		}
		return
	}

	if s.OnEvent != nil {
		target := req.ToProvider
		if target == "" && len(req.ToAgent) >= 16 {
			target = req.ToAgent[:16] + "..."
		}
		s.OnEvent("pipeline_send", msg.PipeID, "agent-pipeline",
			fmt.Sprintf("%s piped work to %s (intent: %s)", fromProvider, target, req.Intent), nil)
	}

	writeJSON(w, http.StatusCreated, map[string]any{
		"pipe_id":    msg.PipeID,
		"status":     msg.Status,
		"expires_at": msg.ExpiresAt.Format(time.RFC3339),
	})
}

// handlePipeInbox returns pending pipeline items for the authenticated agent.
func (s *Server) handlePipeInbox(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Look up agent's provider
	provider := ""
	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), agentID); err == nil {
			provider = agent.Provider
		}
	}

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	items, err := pipeStore.GetInbox(r.Context(), agentID, provider, limit)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Inbox query failed", err.Error())
		return
	}

	// Auto-claim returned items, but return ONLY CAS winners. Two concurrent
	// inbox reads may select the same pending row; ignoring ClaimPipeline errors
	// let both agents believe they owned it. The loser now omits that item.
	claimedItems := items[:0]
	for _, item := range items {
		if err := pipeStore.ClaimPipeline(r.Context(), item.PipeID, agentID); err != nil {
			continue
		}
		item.Status = "claimed"
		item.ClaimedBy = agentID
		claimedItems = append(claimedItems, item)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": claimedItems,
		"count": len(claimedItems),
	})
}

// handlePipeClaim atomically claims a pipeline item.
func (s *Server) handlePipeClaim(w http.ResponseWriter, r *http.Request) {
	pipeID := chi.URLParam(r, "pipe_id")
	agentID := middleware.ContextAgentID(r.Context())

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	pipeNotFoundDetail := fmt.Sprintf("No pipeline message with id %s.", pipeID)
	msg, err := pipeStore.GetPipeline(r.Context(), pipeID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}
	if !s.callerCanClaimPipe(r.Context(), agentID, msg) {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}

	if err := pipeStore.ClaimPipeline(r.Context(), pipeID, agentID); err != nil {
		writeProblem(w, http.StatusConflict, "Claim failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"pipe_id": pipeID,
		"status":  "claimed",
	})
}

// handlePipeResult submits a result for a claimed pipeline item and triggers auto-journal.
func (s *Server) handlePipeResult(w http.ResponseWriter, r *http.Request) {
	pipeID := chi.URLParam(r, "pipe_id")
	agentID := middleware.ContextAgentID(r.Context())

	var req struct {
		Result string `json:"result"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Result == "" {
		writeProblem(w, http.StatusBadRequest, "Missing result", "result is required")
		return
	}
	// Size cap (E8c) — fast-fail before the store re-checks.
	if len(req.Result) > store.MaxPipeContentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Result too large", fmt.Sprintf("result exceeds the %d byte limit.", store.MaxPipeContentBytes))
		return
	}

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	// Get the pipe message for auto-journal context
	msg, err := pipeStore.GetPipeline(r.Context(), pipeID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", err.Error())
		return
	}
	if !s.callerCanViewPipe(r.Context(), agentID, msg) {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", fmt.Sprintf("No pipeline message with id %s.", pipeID))
		return
	}
	if msg.ClaimedBy != agentID && (msg.ClaimedBy != "" || !s.callerCanClaimPipe(r.Context(), agentID, msg)) {
		writeProblem(w, http.StatusConflict, "Completion failed", fmt.Sprintf("pipeline message %s not available for completion by this agent", pipeID))
		return
	}

	// Build auto-journal summary
	resultPreview := req.Result
	if len(resultPreview) > 200 {
		resultPreview = resultPreview[:200] + "..."
	}

	fromName := msg.FromProvider
	if fromName == "" && len(msg.FromAgent) >= 16 {
		fromName = msg.FromAgent[:16] + "..."
	}
	toName := msg.ToProvider
	if toName == "" && len(msg.ToAgent) >= 16 {
		toName = msg.ToAgent[:16] + "..."
	}

	elapsed := ""
	if msg.ClaimedAt != nil {
		elapsed = fmt.Sprintf(" in %s", time.Since(*msg.ClaimedAt).Truncate(time.Second))
	}

	summary := fmt.Sprintf("[Pipeline] %s asked %s to %s. Result received (%d chars)%s. Preview: %s",
		fromName, toName, msg.Intent, len(req.Result), elapsed, resultPreview)

	// Submit journal entry as observation memory
	journalID := s.autoJournalPipeline(r.Context(), summary)

	// Complete the pipeline
	if err := pipeStore.CompletePipeline(r.Context(), pipeID, agentID, req.Result, journalID); err != nil {
		if errors.Is(err, store.ErrPipeResultTooLarge) {
			writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType, "Result too large", err.Error())
			return
		}
		writeProblem(w, http.StatusConflict, "Completion failed", err.Error())
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("pipeline_complete", pipeID, "agent-pipeline", summary, nil)
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"status":     "completed",
		"journal_id": journalID,
	})
}

// callerCanViewPipe reports whether callerID is a party to msg (sender,
// addressed recipient, or matching destination provider) or an operator/admin.
func (s *Server) callerCanViewPipe(ctx context.Context, callerID string, msg *store.PipelineMessage) bool {
	if callerID == "" || msg == nil {
		return false
	}
	if callerID == msg.FromAgent || callerID == msg.ToAgent {
		return true
	}
	if msg.ToProvider != "" && s.agentStore != nil {
		if a, err := s.agentStore.GetAgent(ctx, callerID); err == nil && a != nil && a.Provider == msg.ToProvider {
			return true
		}
	}
	return s.callerIsOperatorOrAdmin(ctx, callerID)
}

// callerCanClaimPipe reports whether callerID is the addressed recipient for
// this pipe (direct agent or provider match) or an operator/admin. Senders can
// read their own pipe status/results, but they do not claim the work item.
func (s *Server) callerCanClaimPipe(ctx context.Context, callerID string, msg *store.PipelineMessage) bool {
	if callerID == "" || msg == nil {
		return false
	}
	if callerID == msg.ToAgent {
		return true
	}
	if msg.ToProvider != "" && msg.ToAgent == "" && s.agentStore != nil {
		if a, err := s.agentStore.GetAgent(ctx, callerID); err == nil && a != nil && a.Provider == msg.ToProvider {
			return true
		}
	}
	return s.callerIsOperatorOrAdmin(ctx, callerID)
}

// handlePipeStatus returns the current status of a pipeline message.
func (s *Server) handlePipeStatus(w http.ResponseWriter, r *http.Request) {
	pipeID := chi.URLParam(r, "pipe_id")

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	// pipeNotFoundDetail is shared by the genuine-not-found and the
	// exists-but-unauthorized branches so the two 404s are byte-for-byte
	// identical — an unrelated caller cannot distinguish "pipe exists but I'm not
	// a party" from "pipe does not exist" (the anti-enumeration goal below).
	pipeNotFoundDetail := fmt.Sprintf("No pipeline message with id %s.", pipeID)

	msg, err := pipeStore.GetPipeline(r.Context(), pipeID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}

	// Authorization: a pipe carries a private payload between two parties. Only
	// the sender, the addressed recipient (agent or provider), or an
	// operator/admin may read it. Return 404 (not 403) so the endpoint does not
	// confirm the existence of a pipe_id to an unrelated caller.
	callerID := middleware.ContextAgentID(r.Context())
	if !s.callerCanViewPipe(r.Context(), callerID, msg) {
		writeProblem(w, http.StatusNotFound, "Pipeline message not found", pipeNotFoundDetail)
		return
	}

	writeJSON(w, http.StatusOK, msg)
}

// handlePipeResults returns completed pipeline items sent by this agent.
func (s *Server) handlePipeResults(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 && n <= 20 {
			limit = n
		}
	}

	agentID := middleware.ContextAgentID(r.Context())

	pipeStore, ok := s.store.(store.PipelineStore)
	if !ok {
		writeProblem(w, http.StatusInternalServerError, "Pipeline not available", "store does not support pipeline operations")
		return
	}

	items, err := pipeStore.GetCompletedForSender(r.Context(), agentID, limit)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Results query failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"count": len(items),
	})
}

// autoJournalPipeline inserts a journal entry as an observation memory directly
// into the off-chain store, bypassing CometBFT. The auto-validator goroutine
// will pick it up and commit it within seconds.
// Returns the memory_id of the journal entry (empty string on failure).
func (s *Server) autoJournalPipeline(ctx context.Context, summary string) string {
	offchain, ok := s.store.(store.OffchainStore)
	if !ok {
		s.logger.Warn().Msg("pipeline auto-journal: store does not support off-chain insert")
		return ""
	}

	memoryID := generateUUID()
	contentHash := sha256.Sum256([]byte(summary))

	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: "sage-system",
		Content:         summary,
		ContentHash:     contentHash[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       "agent-pipeline",
		Provider:        "sage-system",
		ConfidenceScore: 0.90,
		Status:          memory.StatusProposed,
		CreatedAt:       time.Now().UTC(),
	}

	if err := offchain.InsertMemory(ctx, record); err != nil {
		s.logger.Warn().Err(err).Msg("pipeline auto-journal: failed to insert memory")
		return ""
	}

	return memoryID
}

// generatePipeID creates a random pipe ID with a "pipe-" prefix.
func generatePipeID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("pipe-%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
