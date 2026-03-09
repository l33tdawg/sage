package rest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
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
	CreatedAt       time.Time             `json:"created_at"`
	CommittedAt     *time.Time            `json:"committed_at,omitempty"`
	Votes           []*store.ValidationVote `json:"votes,omitempty"`
	Corroborations  []*store.Corroboration  `json:"corroborations,omitempty"`
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

// --- Domain Access Enforcement -----------------------------------------------

// checkDomainAccess verifies an agent has the required access level for a domain.
// Returns nil if allowed, descriptive error if denied.
// Unregistered agents (not in network_agents) are always allowed for backwards compatibility.
// Admins bypass all checks. Observers cannot write.
func checkDomainAccess(ctx context.Context, agentStore store.AgentStore, agentID, domain, action string) error {
	if agentStore == nil || agentID == "" {
		return nil // No agent store or no agent identity — allow
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
			"memory_type must be one of: fact, observation, inference.")
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
	if accessErr := checkDomainAccess(r.Context(), s.agentStore, agentID, req.DomainTag, "write"); accessErr != nil {
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

	// Broadcast via CometBFT RPC.
	txHash, err := s.broadcastTx(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast submit tx")
		writeProblem(w, http.StatusInternalServerError, "Broadcast error", "Failed to broadcast transaction to CometBFT.")
		return
	}

	// Store the full memory object off-chain.
	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: agentID,
		Content:         req.Content,
		ContentHash:     contentHash[:],
		Embedding:       req.Embedding,
		EmbeddingHash:   embeddingHash,
		MemoryType:      memory.MemoryType(req.MemoryType),
		DomainTag:       req.DomainTag,
		Provider:        req.Provider,
		ConfidenceScore: req.ConfidenceScore,
		Status:          memory.StatusProposed,
		ParentHash:      req.ParentHash,
		CreatedAt:       time.Now(),
	}

	if err = s.store.InsertMemory(r.Context(), record); err != nil {
		s.logger.Error().Err(err).Str("memory_id", memoryID).Msg("failed to insert memory")
		writeProblem(w, http.StatusInternalServerError, "Storage error", "Failed to store memory.")
		return
	}

	// Store knowledge triples if provided.
	if len(req.KnowledgeTriples) > 0 {
		if err = s.store.InsertTriples(r.Context(), memoryID, req.KnowledgeTriples); err != nil {
			s.logger.Error().Err(err).Str("memory_id", memoryID).Msg("failed to insert triples")
			// Non-fatal: memory was stored, triples can be retried.
		}
	}

	metrics.MemoriesTotal.WithLabelValues(req.MemoryType, req.DomainTag, string(memory.StatusProposed)).Inc()

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
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, agentID, req.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
	}

	// Multi-org access control gate — only enforce when domain has a registered owner
	if req.DomainTag != "" && s.badgerStore != nil {
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

	start := time.Now()

	opts := store.QueryOptions{
		DomainTag:     req.DomainTag,
		Provider:      req.Provider,
		MinConfidence: req.MinConfidence,
		StatusFilter:  req.StatusFilter,
		TopK:          req.TopK,
		Cursor:        req.Cursor,
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
	queryAgentID := middleware.ContextAgentID(r.Context())
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

	// Access control gate: multi-org + classification enforcement.
	// Submitting agent always has access to their own memory.
	// Domain-level access is only enforced when the domain has a registered owner.
	if rec.DomainTag != "" && s.badgerStore != nil {
		agentID := middleware.ContextAgentID(r.Context())
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

func memoryTypeToTx(mt string) tx.MemoryType {
	switch mt {
	case "fact":
		return tx.MemoryTypeFact
	case "observation":
		return tx.MemoryTypeObservation
	case "inference":
		return tx.MemoryTypeInference
	default:
		return tx.MemoryTypeFact
	}
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
