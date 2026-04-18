package rest

import (
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request / Response types ------------------------------------------------

// VoteRequest is the JSON body for POST /v1/memory/{memory_id}/vote.
type VoteRequest struct {
	Decision  string `json:"decision"`
	Rationale string `json:"rationale,omitempty"`
}

// VoteResponse is the JSON body for a successful vote.
type VoteResponse struct {
	Message string `json:"message"`
	TxHash  string `json:"tx_hash"`
}

// ChallengeRequest is the JSON body for POST /v1/memory/{memory_id}/challenge.
type ChallengeRequest struct {
	Reason   string `json:"reason"`
	Evidence string `json:"evidence,omitempty"`
}

// ChallengeResponse is the JSON body for a successful challenge.
type ChallengeResponse struct {
	Message string `json:"message"`
	TxHash  string `json:"tx_hash"`
}

// ForgetRequest is the JSON body for POST /v1/memory/{memory_id}/forget.
// Thin semantic alias for challenge — "forget" is the user-facing verb used
// across MCP (sage_forget), dashboard events, and now REST.
type ForgetRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ForgetResponse is the JSON body for a successful forget.
type ForgetResponse struct {
	Message string `json:"message"`
	TxHash  string `json:"tx_hash"`
}

// CorroborateRequest is the JSON body for POST /v1/memory/{memory_id}/corroborate.
type CorroborateRequest struct {
	Evidence string `json:"evidence,omitempty"`
}

// CorroborateResponse is the JSON body for a successful corroboration.
type CorroborateResponse struct {
	Message string `json:"message"`
	TxHash  string `json:"tx_hash"`
}

// AgentProfileResponse is the JSON body for GET /v1/agent/me.
type AgentProfileResponse struct {
	AgentID   string  `json:"agent_id"`
	PoEWeight float64 `json:"poe_weight"`
	VoteCount int64   `json:"vote_count"`
}

// PendingMemoriesResponse is the JSON body for GET /v1/validator/pending.
type PendingMemoriesResponse struct {
	Memories []*MemoryResult `json:"memories"`
}

// EpochResponse is the JSON body for GET /v1/validator/epoch.
type EpochResponse struct {
	EpochNum    int64                  `json:"epoch_num"`
	BlockHeight int64                  `json:"block_height"`
	Scores      []*store.ValidatorScore `json:"scores"`
}

// --- Handlers ----------------------------------------------------------------

// handleVoteMemory handles POST /v1/memory/{memory_id}/vote.
func (s *Server) handleVoteMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	var req VoteRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	var decision tx.VoteDecision
	decision, err = parseVoteDecision(req.Decision)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid decision", err.Error())
		return
	}

	// Verify memory exists.
	if _, err = s.store.GetMemory(r.Context(), memoryID); err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	// Build vote transaction.
	voteTx := &tx.ParsedTx{
		Type:      tx.TxTypeMemoryVote,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		MemoryVote: &tx.MemoryVote{
			MemoryID:  memoryID,
			Decision:  decision,
			Rationale: req.Rationale,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	embedAgentAuth(r.Context(), voteTx)

	// Sign the transaction with the node's signing key.
	if err = tx.SignTx(voteTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign vote tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(voteTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode vote tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast vote tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", "Failed to broadcast transaction to CometBFT.")
		return
	}

	metrics.VotesTotal.WithLabelValues(req.Decision).Inc()

	// Emit vote event for SSE chain activity log
	if s.OnEvent != nil {
		s.OnEvent("vote", memoryID, "", req.Decision+": "+req.Rationale, nil)
	}

	writeJSON(w, http.StatusOK, VoteResponse{
		Message: "Vote recorded successfully.",
		TxHash:  txHash,
	})
}

// handleChallengeMemory handles POST /v1/memory/{memory_id}/challenge.
func (s *Server) handleChallengeMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	var req ChallengeRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if req.Reason == "" {
		writeProblem(w, http.StatusBadRequest, "Missing reason", "reason is required.")
		return
	}

	// Verify memory exists.
	if _, err = s.store.GetMemory(r.Context(), memoryID); err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	challengeTx := &tx.ParsedTx{
		Type:      tx.TxTypeMemoryChallenge,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		MemoryChallenge: &tx.MemoryChallenge{
			MemoryID: memoryID,
			Reason:   req.Reason,
			Evidence: req.Evidence,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	embedAgentAuth(r.Context(), challengeTx)

	// Sign the transaction with the node's signing key.
	if err = tx.SignTx(challengeTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign challenge tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(challengeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode challenge tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast challenge tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", "Failed to broadcast transaction to CometBFT.")
		return
	}

	metrics.ChallengesTotal.Inc()

	if s.OnEvent != nil {
		s.OnEvent("forget", memoryID, "", req.Reason, map[string]any{
			"tx_hash": txHash,
		})
	}

	writeJSON(w, http.StatusOK, ChallengeResponse{
		Message: "Challenge submitted successfully.",
		TxHash:  txHash,
	})
}

// handleForgetMemory handles POST /v1/memory/{memory_id}/forget.
// Semantic alias for challenge — delegates to the same MemoryChallenge tx path
// with a default reason when the caller doesn't supply one. Lets SDK/REST
// consumers use the same "forget" verb already exposed via MCP (sage_forget)
// and dashboard events.
func (s *Server) handleForgetMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	var req ForgetRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	reason := req.Reason
	if reason == "" {
		reason = "deprecated by user"
	}

	if _, err = s.store.GetMemory(r.Context(), memoryID); err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	challengeTx := &tx.ParsedTx{
		Type:      tx.TxTypeMemoryChallenge,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		MemoryChallenge: &tx.MemoryChallenge{
			MemoryID: memoryID,
			Reason:   reason,
		},
	}

	embedAgentAuth(r.Context(), challengeTx)

	if err = tx.SignTx(challengeTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign forget tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(challengeTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode forget tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast forget tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", "Failed to broadcast transaction to CometBFT.")
		return
	}

	metrics.ChallengesTotal.Inc()

	if s.OnEvent != nil {
		s.OnEvent("forget", memoryID, "", reason, map[string]any{
			"tx_hash": txHash,
		})
	}

	writeJSON(w, http.StatusOK, ForgetResponse{
		Message: "Memory forgotten.",
		TxHash:  txHash,
	})
}

// handleCorroborateMemory handles POST /v1/memory/{memory_id}/corroborate.
func (s *Server) handleCorroborateMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	var req CorroborateRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Verify memory exists.
	if _, err = s.store.GetMemory(r.Context(), memoryID); err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	corrTx := &tx.ParsedTx{
		Type:      tx.TxTypeMemoryCorroborate,
		Nonce:     uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		MemoryCorroborate: &tx.MemoryCorroborate{
			MemoryID: memoryID,
			Evidence: req.Evidence,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	embedAgentAuth(r.Context(), corrTx)

	// Sign the transaction with the node's signing key.
	if err = tx.SignTx(corrTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign corroborate tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(corrTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode corroborate tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast corroborate tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", "Failed to broadcast transaction to CometBFT.")
		return
	}

	metrics.CorroborationsTotal.Inc()

	if s.OnEvent != nil {
		s.OnEvent("consensus", memoryID, "", "Memory corroborated", map[string]any{
			"tx_hash": txHash,
		})
	}

	writeJSON(w, http.StatusOK, CorroborateResponse{
		Message: "Corroboration recorded successfully.",
		TxHash:  txHash,
	})
}

// handleGetAgent handles GET /v1/agent/me.
func (s *Server) handleGetAgent(w http.ResponseWriter, r *http.Request) {
	agentID := middleware.ContextAgentID(r.Context())
	if agentID == "" {
		writeProblem(w, http.StatusUnauthorized, "Unauthorized", "No agent ID in context.")
		return
	}

	score, err := s.scoreStore.GetScore(r.Context(), agentID)
	if err != nil {
		// Agent exists (authenticated) but may not have a score yet.
		writeJSON(w, http.StatusOK, AgentProfileResponse{
			AgentID:   agentID,
			PoEWeight: 0,
			VoteCount: 0,
		})
		return
	}

	writeJSON(w, http.StatusOK, AgentProfileResponse{
		AgentID:   agentID,
		PoEWeight: score.CurrentWeight,
		VoteCount: score.VoteCount,
	})
}

// handleGetPending handles GET /v1/validator/pending.
func (s *Server) handleGetPending(w http.ResponseWriter, r *http.Request) {
	domainTag := r.URL.Query().Get("domain_tag")
	limitStr := r.URL.Query().Get("limit")

	limit := 20
	if limitStr != "" {
		if l, parseErr := strconv.Atoi(limitStr); parseErr == nil && l > 0 && l <= 100 {
			limit = l
		}
	}

	if domainTag == "" {
		domainTag = "%" // match all domains
	}

	records, err := s.store.GetPendingByDomain(r.Context(), domainTag, limit)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to get pending memories")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query pending memories.")
		return
	}

	results := make([]*MemoryResult, 0, len(records))
	for _, rec := range records {
		results = append(results, &MemoryResult{
			MemoryID:        rec.MemoryID,
			SubmittingAgent: rec.SubmittingAgent,
			Content:         rec.Content,
			ContentHash:     hex.EncodeToString(rec.ContentHash),
			MemoryType:      string(rec.MemoryType),
			DomainTag:       rec.DomainTag,
			ConfidenceScore: rec.ConfidenceScore,
			Status:          string(rec.Status),
			ParentHash:      rec.ParentHash,
			CreatedAt:       rec.CreatedAt,
		})
	}

	writeJSON(w, http.StatusOK, PendingMemoriesResponse{
		Memories: results,
	})
}

// handleGetEpoch handles GET /v1/validator/epoch.
func (s *Server) handleGetEpoch(w http.ResponseWriter, r *http.Request) {
	scores, err := s.scoreStore.GetAllScores(r.Context())
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to get validator scores")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query validator scores.")
		return
	}

	writeJSON(w, http.StatusOK, EpochResponse{
		Scores: scores,
	})
}

// --- Helpers -----------------------------------------------------------------

func parseVoteDecision(s string) (tx.VoteDecision, error) {
	switch s {
	case "accept":
		return tx.VoteDecisionAccept, nil
	case "reject":
		return tx.VoteDecisionReject, nil
	case "abstain":
		return tx.VoteDecisionAbstain, nil
	default:
		return 0, fmt.Errorf("decision must be one of: accept, reject, abstain")
	}
}
