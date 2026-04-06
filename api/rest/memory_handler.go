package rest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request / Response types ------------------------------------------------

// SubmitMemoryRequest is the JSON body for POST /v1/memory/submit.
type SubmitMemoryRequest struct {
	Content          string                   `json:"content"`
	MemoryType       string                   `json:"memory_type"`
	DomainTag        string                   `json:"domain_tag"`
	Provider         string                   `json:"provider,omitempty"`
	ConfidenceScore  float64                  `json:"confidence_score"`
	Classification   int                      `json:"classification,omitempty"`
	Embedding        []float32                `json:"embedding,omitempty"`
	KnowledgeTriples []memory.KnowledgeTriple `json:"knowledge_triples,omitempty"`
	ParentHash       string                   `json:"parent_hash,omitempty"`
	TaskStatus       string                   `json:"task_status,omitempty"`
	LinkedMemories   []string                 `json:"linked_memories,omitempty"`
}

// SubmitMemoryResponse is the JSON body for a successful submission.
type SubmitMemoryResponse struct {
	MemoryID string `json:"memory_id"`
	TxHash   string `json:"tx_hash"`
	Status   string `json:"status"`
}

// QueryMemoryRequest is the JSON body for POST /v1/memory/query.
type QueryMemoryRequest struct {
	Embedding     []float32 `json:"embedding"`
	DomainTag     string    `json:"domain_tag,omitempty"`
	Provider      string    `json:"provider,omitempty"`
	MinConfidence float64   `json:"min_confidence,omitempty"`
	StatusFilter  string    `json:"status_filter,omitempty"`
	TopK          int       `json:"top_k,omitempty"`
	Cursor        string    `json:"cursor,omitempty"`
}

// QueryMemoryResponse is the JSON body for a successful query.
type QueryMemoryResponse struct {
	Results    []*MemoryResult `json:"results"`
	NextCursor string          `json:"next_cursor,omitempty"`
	TotalCount int             `json:"total_count"`
}

// MemoryResult is a memory record with computed confidence.
type MemoryResult struct {
	MemoryID        string       `json:"memory_id"`
	SubmittingAgent string       `json:"submitting_agent"`
	Content         string       `json:"content"`
	ContentHash     string       `json:"content_hash"`
	MemoryType      string       `json:"memory_type"`
	DomainTag       string       `json:"domain_tag"`
	ConfidenceScore float64      `json:"confidence_score"`
	Classification  int          `json:"classification"`
	Status          string       `json:"status"`
	ParentHash      string       `json:"parent_hash,omitempty"`
	TaskStatus      string       `json:"task_status,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	CommittedAt     *time.Time   `json:"committed_at,omitempty"`
}

// MemoryDetailResponse is a memory record with votes and corroborations.
type MemoryDetailResponse struct {
	MemoryID        string                `json:"memory_id"`
	SubmittingAgent string                `json:"submitting_agent"`
	Content         string                `json:"content"`
	ContentHash     string                `json:"content_hash"`
	MemoryType      string                `json:"memory_type"`
	DomainTag       string                `json:"domain_tag"`
	ConfidenceScore float64               `json:"confidence_score"`
	Classification  int                   `json:"classification"`
	Status          string                `json:"status"`
	ParentHash      string                `json:"parent_hash,omitempty"`
	TaskStatus      string                `json:"task_status,omitempty"`
	CreatedAt       time.Time             `json:"created_at"`
	CommittedAt     *time.Time            `json:"committed_at,omitempty"`
	Votes           []*store.ValidationVote `json:"votes,omitempty"`
	Corroborations  []*store.Corroboration  `json:"corroborations,omitempty"`
	LinkedMemories  []memory.MemoryLink     `json:"linked_memories,omitempty"`
}

// CometBFT broadcast_tx_sync response structure.
type cometBroadcastResponse struct {
	Result struct {
		Code int    `json:"code"`
		Hash string `json:"hash"`
		Log  string `json:"log"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// CometBFT broadcast_tx_commit response structure.
// Unlike broadcast_tx_sync, this waits for the block to be finalized,
// ensuring ABCI Commit has flushed writes before we return.
type cometCommitResponse struct {
	Result struct {
		CheckTx struct {
			Code int    `json:"code"`
			Log  string `json:"log"`
		} `json:"check_tx"`
		TxResult struct {
			Code int    `json:"code"`
			Data string `json:"data"`
			Log  string `json:"log"`
		} `json:"tx_result"`
		Hash   string `json:"hash"`
		Height int64  `json:"height"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// --- Domain Access Enforcement -----------------------------------------------

// checkDomainAccess verifies an agent has the required access level for a domain.
// Returns nil if allowed, descriptive error if denied.
// Unregistered agents (not in network_agents) are always allowed for backwards compatibility.
// Admins bypass all checks. Observers cannot write.
func checkDomainAccess(ctx context.Context, agentStore store.AgentStore, badgerStore *store.BadgerStore, agentID, domain, action string) error {
	if agentID == "" {
		return nil // No agent identity — allow
	}

	// Check on-chain state first (if BadgerDB available)
	if badgerStore != nil {
		onChainAgent, err := badgerStore.GetRegisteredAgent(agentID)
		if err == nil && onChainAgent != nil {
			// Use on-chain clearance and domain access
			if onChainAgent.Role == "admin" {
				return nil
			}
			if action == "write" && onChainAgent.Role == "observer" {
				return fmt.Errorf("observer agents cannot submit memories")
			}
			if onChainAgent.DomainAccess != "" {
				var access []struct {
					Domain string `json:"domain"`
					Read   bool   `json:"read"`
					Write  bool   `json:"write"`
				}
				if err := json.Unmarshal([]byte(onChainAgent.DomainAccess), &access); err == nil && len(access) > 0 {
					for _, a := range access {
						if a.Domain == domain {
							if action == "read" && a.Read {
								return nil
							}
							if action == "write" && a.Write {
								return nil
							}
							return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
						}
					}
					return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
				}
			}
			// On-chain agent with no domain access restrictions — allow
			return nil
		}
	}

	// Fallback to SQLite agent store
	if agentStore == nil {
		return nil // No agent store — allow
	}

	agent, err := agentStore.GetAgent(ctx, agentID)
	if err != nil {
		return nil // Agent not registered in network_agents — backwards compat
	}

	if agent.Role == "admin" {
		return nil // Admins have full access
	}

	if action == "write" && agent.Role == "observer" {
		return fmt.Errorf("observer agents cannot submit memories")
	}

	// Parse domain_access JSON: [{"domain":"x","read":true,"write":false}, ...]
	if agent.DomainAccess == "" {
		return nil // No restrictions configured — allow all
	}

	var access []struct {
		Domain string `json:"domain"`
		Read   bool   `json:"read"`
		Write  bool   `json:"write"`
	}
	if err := json.Unmarshal([]byte(agent.DomainAccess), &access); err != nil {
		return nil // Malformed JSON — allow (safe default for existing data)
	}

	if len(access) == 0 {
		return nil // Empty list means no restrictions
	}

	for _, a := range access {
		if a.Domain == domain {
			if action == "read" && a.Read {
				return nil
			}
			if action == "write" && a.Write {
				return nil
			}
			return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
		}
	}

	// Domain not in the access list — deny (explicit allowlist model)
	return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
}

// --- Agent Isolation (RBAC) --------------------------------------------------

// resolveVisibleAgents determines which agents' memories the given agent can see.
// Returns (allowedAgentIDs, seeAll). If seeAll is true, no filtering needed.
// RBAC is on-chain: checks BadgerDB for role and visible_agents.
func (s *Server) resolveVisibleAgents(agentID string) ([]string, bool) {
	if agentID == "" {
		return nil, true // No identity = legacy/internal, allow all
	}

	// Resolve visible_agents from the best available source.
	// On-chain (BadgerDB) is checked first, then SQLite as fallback
	// since dashboard writes may not have been broadcast to chain yet.
	var role, visibleAgents string

	if s.badgerStore != nil {
		agent, err := s.badgerStore.GetRegisteredAgent(agentID)
		if err == nil && agent != nil {
			role = agent.Role
			visibleAgents = agent.VisibleAgents
		}
	}

	// Fallback to SQLite if on-chain state has no visible_agents
	if visibleAgents == "" && s.agentStore != nil {
		ctx := context.Background()
		if sqlAgent, err := s.agentStore.GetAgent(ctx, agentID); err == nil && sqlAgent != nil {
			if role == "" {
				role = sqlAgent.Role
			}
			visibleAgents = sqlAgent.VisibleAgents
		}
	}

	if role == "admin" {
		return nil, true
	}
	if visibleAgents == "*" {
		return nil, true
	}
	allowed := []string{agentID} // Always see own
	if visibleAgents != "" {
		var list []string
		if json.Unmarshal([]byte(visibleAgents), &list) == nil {
			allowed = append(allowed, list...)
		}
	}
	return allowed, false
}

// --- Handlers ----------------------------------------------------------------

// handleSubmitMemory handles POST /v1/memory/submit.
func (s *Server) handleSubmitMemory(w http.ResponseWriter, r *http.Request) {
	var req SubmitMemoryRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate required fields.
	if req.Content == "" {
		writeProblem(w, http.StatusBadRequest, "Missing content", "content is required.")
		return
	}
	if !memory.IsValidMemoryType(memory.MemoryType(req.MemoryType)) {
		writeProblem(w, http.StatusBadRequest, "Invalid memory type",
			"memory_type must be one of: fact, observation, inference, task.")
		return
	}
	if req.DomainTag == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain tag", "domain_tag is required.")
		return
	}
	if req.ConfidenceScore < 0 || req.ConfidenceScore > 1 {
		writeProblem(w, http.StatusBadRequest, "Invalid confidence score",
			"confidence_score must be between 0 and 1.")
		return
	}

	agentID := middleware.ContextAgentID(r.Context())

	// Enforce domain access policy from network_agents registry
	if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, req.DomainTag, "write"); accessErr != nil {
		writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
		return
	}

	memoryID := generateUUID()

	// Compute content hash.
	contentHash := sha256.Sum256([]byte(req.Content))

	// Compute embedding hash if provided.
	var embeddingHash []byte
	if len(req.Embedding) > 0 {
		h := sha256.New()
		for _, v := range req.Embedding {
			fmt.Fprintf(h, "%f", v)
		}
		embeddingHash = h.Sum(nil)
	}

	// Build the on-chain transaction.
	classification := req.Classification
	if classification == 0 {
		classification = 1 // Default to INTERNAL
	}

	submitTx := &tx.ParsedTx{
		Type:  tx.TxTypeMemorySubmit,
		Nonce: uint64(time.Now().UnixNano()), // #nosec G115 -- nonce from timestamp
		Timestamp: time.Now(),
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        memoryID,
			ContentHash:     contentHash[:],
			EmbeddingHash:   embeddingHash,
			MemoryType:      memoryTypeToTx(req.MemoryType),
			DomainTag:       req.DomainTag,
			ConfidenceScore: req.ConfidenceScore,
			Content:         req.Content,
			ParentHash:      req.ParentHash,
			Classification:  tx.ClearanceLevel(classification), // #nosec G115 -- validated small int
			TaskStatus:      req.TaskStatus,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	embedAgentAuth(r.Context(), submitTx)

	// Sign the transaction with the node's signing key.
	if err = tx.SignTx(submitTx, s.signingKey); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign submit tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(submitTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode submit tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	// Stage supplementary off-chain data (embedding vector, provider, triples)
	// in the process-local cache. The ABCI app reads this during FinalizeBlock
	// and includes it in the pending write that Commit flushes to the store.
	// This ensures memories only appear in the query layer AFTER consensus.
	if s.suppCache != nil {
		s.suppCache.Put(memoryID, &memory.SupplementaryData{
			Embedding:        req.Embedding,
			EmbeddingHash:    embeddingHash,
			Provider:         req.Provider,
			KnowledgeTriples: req.KnowledgeTriples,
		})
	}

	// Broadcast via CometBFT RPC and wait for block finalization.
	// broadcast_tx_commit blocks until the block containing this tx is committed,
	// meaning ABCI Commit has already flushed the memory to the offchain store.
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast submit tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", "Failed to broadcast transaction to CometBFT.")
		return
	}

	metrics.MemoriesTotal.WithLabelValues(req.MemoryType, req.DomainTag, string(memory.StatusProposed)).Inc()

	// Update agent's last activity timestamp
	if agentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), agentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", agentID).Msg("failed to update agent last_seen")
		}
	}

	// Emit event for SSE chain activity log
	if s.OnEvent != nil {
		s.OnEvent("remember", memoryID, req.DomainTag, truncateContent(req.Content, 80), map[string]any{
			"full_content": req.Content,
			"memory_type":  req.MemoryType,
			"confidence":   req.ConfidenceScore,
		})
	}

	writeJSON(w, http.StatusCreated, SubmitMemoryResponse{
		MemoryID: memoryID,
		TxHash:   txHash,
		Status:   string(memory.StatusProposed),
	})
}

// handleQueryMemory handles POST /v1/memory/query.
func (s *Server) handleQueryMemory(w http.ResponseWriter, r *http.Request) {
	var req QueryMemoryRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if len(req.Embedding) == 0 {
		writeProblem(w, http.StatusBadRequest, "Missing embedding", "embedding is required for similarity search.")
		return
	}

	// Network agent domain access enforcement (read side)
	// checkDomainAccess verifies the agent's DomainAccess policy (explicit allowlist).
	// If the agent passes this check (no restrictions or explicit read permission),
	// we skip the multi-org gate — the two systems are alternatives, not stacked AND.
	domainAccessApproved := false
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, req.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}

	// Multi-org access control gate — only enforce when domain has a registered owner
	// AND the agent wasn't already approved by the DomainAccess policy above.
	if req.DomainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(req.DomainTag)
		if domainErr == nil && domainOwner != "" {
			agentID := middleware.ContextAgentID(r.Context())
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(req.DomainTag, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", req.DomainTag))
				return
			}
		}
	}

	// Resolve agent isolation RBAC — determines which agents' memories are visible
	queryAgentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(queryAgentID)

	// If checkDomainAccess already approved read access for this domain,
	// skip agent isolation — the agent is authorized to see everything in the domain.
	if !seeAll && domainAccessApproved {
		seeAll = true
	}

	// Grant-aware override: if querying a specific domain, skip agent isolation when:
	// (a) the agent has a direct grant on the domain, or
	// (b) the agent has org-level access (clearance >= classification), or
	// (c) the domain has no registered owner (no access policy = open visibility)
	if !seeAll && req.DomainTag != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(req.DomainTag, queryAgentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.badgerStore.HasAccessMultiOrg(req.DomainTag, queryAgentID, 0, time.Now())
			if hasOrgAccess {
				seeAll = true
			} else {
				// Unregistered domains have no access policy — don't enforce agent isolation
				_, ownerErr := s.badgerStore.GetDomainOwner(req.DomainTag)
				if ownerErr != nil {
					seeAll = true
				}
			}
		}
	}

	start := time.Now()

	opts := store.QueryOptions{
		DomainTag:     req.DomainTag,
		Provider:      req.Provider,
		MinConfidence: req.MinConfidence,
		StatusFilter:  req.StatusFilter,
		TopK:          req.TopK,
		Cursor:        req.Cursor,
	}
	if !seeAll {
		opts.SubmittingAgents = allowedAgents
	}

	var records []*memory.MemoryRecord
	records, err = s.store.QuerySimilar(r.Context(), req.Embedding, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to query memories")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query memories.")
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Apply confidence decay and classification filtering.
	now := time.Now()
	results := make([]*MemoryResult, 0, len(records))
	for _, rec := range records {
		// Classification gate: check agent clearance >= memory classification
		// Only enforce when domain has a registered owner (backward compat for pre-RBAC setups)
		var memClass uint8
		if s.badgerStore != nil {
			memClass, _ = s.badgerStore.GetMemoryClassification(rec.MemoryID)
			if memClass > 0 {
				domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
				if domErr == nil && domainOwner != "" {
					hasAccess, _ := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, queryAgentID, memClass, now)
					if !hasAccess && rec.SubmittingAgent != queryAgentID {
						continue // Skip memories the agent can't access
					}
				}
			}
		}

		// Get corroboration count for confidence decay.
		corrs, _ := s.store.GetCorroborations(r.Context(), rec.MemoryID)
		currentConf := memory.ComputeConfidence(rec.ConfidenceScore, rec.CreatedAt, now, len(corrs), rec.DomainTag)

		results = append(results, &MemoryResult{
			MemoryID:        rec.MemoryID,
			SubmittingAgent: rec.SubmittingAgent,
			Content:         rec.Content,
			ContentHash:     hex.EncodeToString(rec.ContentHash),
			MemoryType:      string(rec.MemoryType),
			DomainTag:       rec.DomainTag,
			ConfidenceScore: currentConf,
			Classification:  int(memClass),
			Status:          string(rec.Status),
			ParentHash:      rec.ParentHash,
			CreatedAt:       rec.CreatedAt,
			CommittedAt:     rec.CommittedAt,
		})
	}

	// Update agent's last activity timestamp on recall
	if queryAgentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), queryAgentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", queryAgentID).Msg("failed to update agent last_seen on recall")
		}
	}

	// Emit recall event for SSE chain activity log with full retrieved memory details
	if s.OnEvent != nil && len(results) > 0 {
		domain := req.DomainTag
		if domain == "" && len(results) > 0 {
			domain = results[0].DomainTag
		}
		// Build rich detail for expandable chain activity rows
		retrieved := make([]map[string]any, 0, len(results))
		for _, r := range results {
			retrieved = append(retrieved, map[string]any{
				"memory_id":  r.MemoryID,
				"content":    r.Content,
				"domain":     r.DomainTag,
				"confidence": r.ConfidenceScore,
				"type":       r.MemoryType,
			})
		}
		s.OnEvent("recall", "", domain, fmt.Sprintf("%d memories retrieved", len(results)), map[string]any{
			"retrieved": retrieved,
		})
	}

	writeJSON(w, http.StatusOK, QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	})
}

// handleGetMemory handles GET /v1/memory/{memory_id}.
func (s *Server) handleGetMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	rec, err := s.store.GetMemory(r.Context(), memoryID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	// Agent isolation RBAC check: can this agent see this memory's author?
	agentID := middleware.ContextAgentID(r.Context())
	if agentID != rec.SubmittingAgent {
		allowedAgents, seeAll := s.resolveVisibleAgents(agentID)
		if !seeAll {
			visible := false
			for _, a := range allowedAgents {
				if a == rec.SubmittingAgent {
					visible = true
					break
				}
			}
			if !visible {
				writeProblem(w, http.StatusForbidden, "Access denied",
					"You do not have visibility into this agent's memories.")
				return
			}
		}
	}

	// Access control gate: multi-org + classification enforcement.
	// Submitting agent always has access to their own memory.
	// Domain-level access is only enforced when the domain has a registered owner.
	if rec.DomainTag != "" && s.badgerStore != nil {
		if agentID != rec.SubmittingAgent {
			// Only enforce access if the domain has a registered owner (org structure exists)
			domainOwner, domainErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
			if domainErr == nil && domainOwner != "" {
				classification, _ := s.badgerStore.GetMemoryClassification(memoryID)
				hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, agentID, classification, time.Now())
				if accessErr != nil || !hasAccess {
					writeProblem(w, http.StatusForbidden, "Access denied",
						fmt.Sprintf("No read access to domain %s", rec.DomainTag))
					return
				}
			}
		}
	}

	votes, _ := s.store.GetVotes(r.Context(), memoryID)
	corrs, _ := s.store.GetCorroborations(r.Context(), memoryID)

	// Apply confidence decay.
	currentConf := memory.ComputeConfidence(rec.ConfidenceScore, rec.CreatedAt, time.Now(), len(corrs), rec.DomainTag)

	writeJSON(w, http.StatusOK, MemoryDetailResponse{
		MemoryID:        rec.MemoryID,
		SubmittingAgent: rec.SubmittingAgent,
		Content:         rec.Content,
		ContentHash:     hex.EncodeToString(rec.ContentHash),
		MemoryType:      string(rec.MemoryType),
		DomainTag:       rec.DomainTag,
		ConfidenceScore: currentConf,
		Status:          string(rec.Status),
		ParentHash:      rec.ParentHash,
		CreatedAt:       rec.CreatedAt,
		CommittedAt:     rec.CommittedAt,
		Votes:           votes,
		Corroborations:  corrs,
	})
}

// handlePreValidate handles POST /v1/memory/pre-validate.
// Runs the 4 app validators against proposed content without submitting on-chain.
func (s *Server) handlePreValidate(w http.ResponseWriter, r *http.Request) {
	if s.PreValidateFunc == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Not configured", "Pre-validation not configured on this node.")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	var req struct {
		Content    string  `json:"content"`
		Domain     string  `json:"domain"`
		Type       string  `json:"type"`
		Confidence float64 `json:"confidence"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Compute content hash (same as memory submission)
	hash := sha256.Sum256([]byte(req.Content))
	contentHash := hex.EncodeToString(hash[:])

	votes := s.PreValidateFunc(req.Content, contentHash, req.Domain, req.Type, req.Confidence)

	acceptCount := 0
	for _, v := range votes {
		if v.Decision == "accept" {
			acceptCount++
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": acceptCount >= 3, // BFT quorum: 3 of 4
		"votes":    votes,
		"quorum":   fmt.Sprintf("%d/4", acceptCount),
	})
}

// --- Helpers -----------------------------------------------------------------

// broadcastTx sends a transaction to CometBFT via broadcast_tx_sync RPC.
func (s *Server) broadcastTx(txBytes []byte) (string, error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_sync?tx=0x%s", s.cometbftRPC, txHex)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return "", fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("broadcast tx: %w", err)
	}
	defer resp.Body.Close()

	var result cometBroadcastResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode broadcast response: %w", err)
	}

	if result.Error != nil {
		return "", fmt.Errorf("broadcast error: %s", result.Error.Message)
	}

	if result.Result.Code != 0 {
		return "", fmt.Errorf("tx rejected (code %d): %s", result.Result.Code, result.Result.Log)
	}

	return result.Result.Hash, nil
}

// broadcastTxCommit sends a transaction to CometBFT and waits for block finalization.
// Unlike broadcastTx (sync), this blocks until the block is committed, ensuring
// ABCI Commit has flushed all pending writes to the offchain store before returning.
func (s *Server) broadcastTxCommit(txBytes []byte) (string, error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", s.cometbftRPC, txHex)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return "", fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("broadcast tx commit: %w", err)
	}
	defer resp.Body.Close()

	var result cometCommitResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", fmt.Errorf("decode broadcast commit response: %w", err)
	}

	if result.Error != nil {
		return "", fmt.Errorf("broadcast error: %s", result.Error.Message)
	}

	if result.Result.CheckTx.Code != 0 {
		return "", fmt.Errorf("tx rejected in CheckTx (code %d): %s", result.Result.CheckTx.Code, result.Result.CheckTx.Log)
	}

	if result.Result.TxResult.Code != 0 {
		return "", fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", result.Result.TxResult.Code, result.Result.TxResult.Log)
	}

	return result.Result.Hash, nil
}

func memoryTypeToTx(mt string) tx.MemoryType {
	switch mt {
	case "fact":
		return tx.MemoryTypeFact
	case "observation":
		return tx.MemoryTypeObservation
	case "inference":
		return tx.MemoryTypeInference
	case "task":
		return tx.MemoryTypeTask
	default:
		return tx.MemoryTypeFact
	}
}


// handleUpdateTaskStatus handles PUT /v1/memory/{memory_id}/task-status.
func (s *Server) handleUpdateTaskStatus(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory_id", "memory_id is required.")
		return
	}

	var req struct {
		TaskStatus string `json:"task_status"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	ts := memory.TaskStatus(req.TaskStatus)
	if !memory.IsValidTaskStatus(ts) {
		writeProblem(w, http.StatusBadRequest, "Invalid task status",
			"task_status must be one of: planned, in_progress, done, dropped.")
		return
	}

	if err := s.store.UpdateTaskStatus(r.Context(), memoryID, ts); err != nil {
		writeProblem(w, http.StatusNotFound, "Task not found", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"memory_id":   memoryID,
		"task_status": req.TaskStatus,
	})
}

// handleLinkMemories handles POST /v1/memory/link.
func (s *Server) handleLinkMemories(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SourceID string `json:"source_id"`
		TargetID string `json:"target_id"`
		LinkType string `json:"link_type"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.SourceID == "" || req.TargetID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing IDs", "source_id and target_id are required.")
		return
	}
	if req.LinkType == "" {
		req.LinkType = "related"
	}

	if err := s.store.LinkMemories(r.Context(), req.SourceID, req.TargetID, req.LinkType); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Link failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"source_id": req.SourceID,
		"target_id": req.TargetID,
		"link_type": req.LinkType,
	})
}

// handleGetOpenTasks handles GET /v1/memory/tasks.
func (s *Server) handleGetOpenTasks(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	provider := r.URL.Query().Get("provider")

	tasks, err := s.store.GetOpenTasks(r.Context(), domain, provider)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Failed to get tasks", err.Error())
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

	writeJSON(w, http.StatusOK, map[string]any{
		"tasks": results,
		"total": len(results),
	})
}

// generateUUID creates a random UUID v4 string without an external dependency.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Set version 4 and variant bits per RFC 4122.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func truncateContent(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

// handleListMemoriesAuth handles GET /v1/memory/list (authenticated, agent-isolated).
// Mirrors the dashboard list endpoint but applies RBAC agent isolation.
func (s *Server) handleListMemoriesAuth(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset, _ := strconv.Atoi(q.Get("offset"))

	agentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(agentID)

	domainFilter := q.Get("domain")

	// Grant-aware override: if listing a specific domain, skip agent isolation when:
	// (a) the agent has a direct grant on the domain, or
	// (b) the agent has org-level access (clearance >= classification), or
	// (c) the domain has no registered owner (no access policy = open visibility)
	if !seeAll && domainFilter != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(domainFilter, agentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.badgerStore.HasAccessMultiOrg(domainFilter, agentID, 0, time.Now())
			if hasOrgAccess {
				seeAll = true
			} else {
				_, ownerErr := s.badgerStore.GetDomainOwner(domainFilter)
				if ownerErr != nil {
					seeAll = true
				}
			}
		}
	}

	opts := store.ListOptions{
		DomainTag: domainFilter,
		Tag:       q.Get("tag"),
		Provider:  q.Get("provider"),
		Status:    q.Get("status"),
		Limit:     limit,
		Offset:    offset,
		Sort:      q.Get("sort"),
	}
	// Apply single-agent filter from query param (only if allowed)
	if agent := q.Get("agent"); agent != "" {
		opts.SubmittingAgent = agent
	}
	if !seeAll {
		opts.SubmittingAgents = allowedAgents
	}

	records, total, err := s.store.ListMemories(r.Context(), opts)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Query error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"memories": records,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	})
}

// handleTimelineAuth handles GET /v1/memory/timeline (authenticated, agent-isolated).
// Mirrors the dashboard timeline endpoint. Timeline aggregation is domain-level
// so agent isolation is not applied to counts, but only authenticated agents can call it.
func (s *Server) handleTimelineAuth(w http.ResponseWriter, r *http.Request) {
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

	buckets, err := s.store.GetTimeline(r.Context(), from, to, domain, bucket)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Query error", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{"buckets": buckets})
}
