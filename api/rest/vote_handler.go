package rest

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/poe"
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
	Status  string `json:"status,omitempty"`
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
	Status  string `json:"status,omitempty"`
}

// ReinstateRequest is the JSON body for
// POST /v1/memory/{memory_id}/reinstate. Reason is an optional audit note
// carried in the app-v17 transaction.
type ReinstateRequest struct {
	Reason string `json:"reason,omitempty"`
}

// ReinstateResponse is the JSON body for a successful reinstatement.
type ReinstateResponse struct {
	Message string `json:"message"`
	TxHash  string `json:"tx_hash"`
	Status  string `json:"status"`
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
	AgentID           string   `json:"agent_id"`
	DisplayName       string   `json:"display_name"`
	Domains           []string `json:"domains"`
	Role              string   `json:"role,omitempty"`
	Profile           string   `json:"profile,omitempty"`
	HomeDomain        string   `json:"home_domain,omitempty"`
	EnrollmentState   string   `json:"enrollment_status,omitempty"`
	RegistrationState string   `json:"registration_status,omitempty"`
	ApprovalRequired  bool     `json:"approval_required"`
	Clearance         uint8    `json:"clearance"`
	Capabilities      uint32   `json:"capabilities"`
	CanRead           *bool    `json:"can_read,omitempty"`
	CanWrite          *bool    `json:"can_write,omitempty"`
	AccessScope       string   `json:"access_scope,omitempty"`
	PoEWeight         float64  `json:"poe_weight"`
	VoteCount         int64    `json:"vote_count"`
	Accuracy          float64  `json:"accuracy"`
	// CorrCount is the validator's lifetime corroboration count (votes that
	// matched a terminal verdict) — the δ factor of the PoE quorum weight,
	// read from the authoritative on-chain vstats:<id> record. Not mirrored
	// off-chain.
	CorrCount int64 `json:"corr_count"`
	// DomainExpertise maps each domain the agent has voted in to its
	// per-domain verdict-correctness EWMA (the β factor, read from
	// vstats_domain:<id>:<D>). Omitted for domains with no voting history so
	// generalists don't emit a wall of 0.5 cold-start values.
	DomainExpertise map[string]float64 `json:"domain_expertise,omitempty"`
	OnChainHeight   int64              `json:"on_chain_height"`
}

// PendingMemoriesResponse is the JSON body for GET /v1/validator/pending.
type PendingMemoriesResponse struct {
	Memories []*MemoryResult `json:"memories"`
}

// EpochResponse is the JSON body for GET /v1/validator/epoch.
type EpochResponse struct {
	EpochNum    int64                   `json:"epoch_num"`
	BlockHeight int64                   `json:"block_height"`
	Scores      []*store.ValidatorScore `json:"scores"`
}

// --- Handlers ----------------------------------------------------------------

// handleVoteMemory handles POST /v1/memory/{memory_id}/vote.
func (s *Server) handleVoteMemory(w http.ResponseWriter, r *http.Request) {
	// This endpoint is a deliberate operator override for this node's single
	// validator vote. The HTTP caller does not gain an independent vote: the
	// resulting transaction is signed solely by the configured validator key.
	// Gate before parsing the target or body so ordinary agents cannot use the
	// endpoint as a memory-existence oracle.
	if !s.requireGovernanceOperator(w, r.Context()) {
		return
	}

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
		Type: tx.TxTypeMemoryVote,
		MemoryVote: &tx.MemoryVote{
			MemoryID:  memoryID,
			Decision:  decision,
			Rationale: req.Rationale,
		},
	}

	// Keep memory votes on the validator identity plane. The local operator was
	// authorized above, but embedding that agent proof would both misrepresent
	// the on-chain voter and make a promoted app-v23 Admin inject a local
	// elevation proof into a transaction consensus intentionally requires to
	// remain validator-only.
	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), voteTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "vote", err)
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
		Type: tx.TxTypeMemoryChallenge,
		MemoryChallenge: &tx.MemoryChallenge{
			MemoryID: memoryID,
			Reason:   req.Reason,
			Evidence: req.Evidence,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	s.embedAgentAuth(r.Context(), challengeTx)

	// Sign the transaction with the node's signing key.
	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), challengeTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "challenge", err)
		return
	}

	metrics.ChallengesTotal.Inc()
	resultStatus := s.challengeResultStatus(r.Context(), memoryID)

	if s.OnEvent != nil {
		s.OnEvent("forget", memoryID, "", req.Reason, map[string]any{
			"tx_hash": txHash,
			"status":  resultStatus,
		})
	}

	writeJSON(w, http.StatusOK, ChallengeResponse{
		Message: "Challenge submitted successfully.",
		TxHash:  txHash,
		Status:  resultStatus,
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
		Type: tx.TxTypeMemoryChallenge,
		MemoryChallenge: &tx.MemoryChallenge{
			MemoryID: memoryID,
			Reason:   reason,
		},
	}

	s.embedAgentAuth(r.Context(), challengeTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), challengeTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "forget", err)
		return
	}

	metrics.ChallengesTotal.Inc()
	resultStatus := s.challengeResultStatus(r.Context(), memoryID)

	if s.OnEvent != nil {
		s.OnEvent("forget", memoryID, "", reason, map[string]any{
			"tx_hash": txHash,
			"status":  resultStatus,
		})
	}

	writeJSON(w, http.StatusOK, ForgetResponse{
		Message: "Memory forgotten.",
		TxHash:  txHash,
		Status:  resultStatus,
	})
}

// challengeResultStatus reads authoritative consensus state after
// broadcastTxCommit. Tests and legacy embedded callers that do not wire Badger
// fall back to the SQL projection; a wired production node never lets projection
// lag redefine the committed result.
func (s *Server) challengeResultStatus(ctx context.Context, memoryID string) string {
	if s.badgerStore != nil {
		_, status, err := s.badgerStore.GetMemoryHash(memoryID)
		if err != nil {
			s.logger.Warn().Err(err).Str("memory_id", memoryID).Msg("post-commit challenge status unavailable from canonical state")
			return ""
		}
		switch memory.MemoryStatus(status) {
		case memory.StatusChallenged, memory.StatusDeprecated:
			return status
		default:
			return ""
		}
	}

	rec, err := s.store.GetMemory(ctx, memoryID)
	if err != nil {
		return ""
	}
	switch rec.Status {
	case memory.StatusChallenged, memory.StatusDeprecated:
		return string(rec.Status)
	default:
		return ""
	}
}

// handleReinstateMemory handles POST /v1/memory/{memory_id}/reinstate.
// app-v17 is dual-gated in CheckTx and FinalizeBlock, so a node whose chain has
// not activated the fork returns a sanitized 400 and cannot accidentally admit
// type 35. broadcast_tx_commit waits through Commit, making a successful 200 a
// durable challenged -> committed transition rather than a mempool receipt.
func (s *Server) handleReinstateMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	var req ReinstateRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Fast, user-friendly not-found check. The consensus handler remains the
	// authority for status and authorization because the off-chain mirror can be
	// briefly stale during recovery.
	if _, err := s.store.GetMemory(r.Context(), memoryID); err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	reinstateTx := &tx.ParsedTx{
		Type: tx.TxTypeMemoryReinstate,
		MemoryReinstate: &tx.MemoryReinstate{
			MemoryID: memoryID,
			Reason:   req.Reason,
		},
	}
	s.embedAgentAuth(r.Context(), reinstateTx)

	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), reinstateTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "reinstate", err)
		return
	}

	if s.OnEvent != nil {
		s.OnEvent("reinstate", memoryID, "", req.Reason, map[string]any{
			"tx_hash": txHash,
		})
	}

	writeJSON(w, http.StatusOK, ReinstateResponse{
		Message: "Memory reinstated.",
		TxHash:  txHash,
		Status:  "committed",
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
		Type: tx.TxTypeMemoryCorroborate,
		MemoryCorroborate: &tx.MemoryCorroborate{
			MemoryID: memoryID,
			Evidence: req.Evidence,
		},
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	s.embedAgentAuth(r.Context(), corrTx)

	// Sign the transaction with the node's signing key.
	var txHash string
	stage, err := s.submitConsensusTx(r.Context(), corrTx, func(encoded []byte) error {
		var submitErr error
		txHash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "corroborate", err)
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
	isRoot, rootErr := s.appV23IsRootIdentity(agentID)
	if rootErr != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
			"Current CEREBRUM Root state is unavailable.")
		return
	}
	if isRoot {
		writeProblem(w, http.StatusNotFound, "Agent not found",
			"CEREBRUM Root does not have an ordinary agent profile.")
		return
	}
	resp := AgentProfileResponse{
		AgentID: agentID,
		Domains: []string{},
	}
	if s.isPostV23ForNextTx() {
		if s.badgerStore == nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Consensus access-control state is unavailable.")
			return
		}
		registered, registeredErr := s.badgerStore.GetRegisteredAgent(agentID)
		if registeredErr != nil || registered == nil || registered.AgentID != agentID {
			writeProblem(w, http.StatusNotFound, "Agent not found",
				"This authenticated identity is not registered as an ordinary agent on this SAGE.")
			return
		}
		resp.DisplayName = registered.Name
		resp.OnChainHeight = registered.RegisteredAt
		resp.Role = registered.Role
		resp.Clearance = registered.Clearance
		resp.Capabilities = uint32(registered.Capabilities)

		enrollment, enrollmentErr := s.badgerStore.GetAppV23Enrollment(agentID)
		roleState, roleErr := s.badgerStore.GetAppV23Role(agentID)
		if enrollmentErr != nil || roleErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current local enrollment state is unavailable.")
			return
		}
		active, activeErr := s.appV23ActiveOrdinaryAgent(agentID)
		if activeErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
				"Current local enrollment state is unavailable.")
			return
		}
		canRead, canWrite := false, false
		resp.CanRead = &canRead
		resp.CanWrite = &canWrite
		resp.AccessScope = "home_domain"
		switch {
		case enrollment == nil:
			resp.EnrollmentState = "pending_review"
			resp.RegistrationState = "pending_review"
			resp.ApprovalRequired = true
		case !active:
			resp.Profile = enrollment.Profile
			resp.HomeDomain = enrollment.HomeDomain
			resp.Clearance = enrollment.Clearance
			resp.Capabilities = uint32(enrollment.Capabilities)
			if roleState != nil {
				resp.Role = roleState.Role
			}
			resp.EnrollmentState = "inactive"
			resp.RegistrationState = "inactive"
			resp.ApprovalRequired = true
		default:
			resp.Role = roleState.Role
			resp.Profile = enrollment.Profile
			resp.HomeDomain = enrollment.HomeDomain
			resp.Clearance = enrollment.Clearance
			resp.Capabilities = uint32(enrollment.Capabilities)
			resp.EnrollmentState = "active"
			resp.RegistrationState = "active"
			if enrollment.HomeDomain != "" {
				shared, sharedErr := s.badgerStore.IsAppV23SharedDomain(enrollment.HomeDomain)
				if sharedErr != nil {
					writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
						"Current home-domain state is unavailable.")
					return
				}
				readAuth, readErr := s.badgerStore.AuthorizeAppV23LocalDomain(
					agentID, enrollment.HomeDomain, store.AppV23VerbRead, shared,
				)
				writeAuth, writeErr := s.badgerStore.AuthorizeAppV23LocalDomain(
					agentID, enrollment.HomeDomain, store.AppV23VerbWrite, shared,
				)
				if readErr != nil || writeErr != nil {
					writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
						"Current home-domain access state is unavailable.")
					return
				}
				canRead = readAuth.Allowed
				canWrite = writeAuth.Allowed
			}
			resp.CanRead = &canRead
			resp.CanWrite = &canWrite
		}
	}
	// MCP policy checks and sage_status need only the authenticated consensus
	// standing above. Do not make them wait for SQL domain history or PoE
	// projections; those remain available on the ordinary full profile.
	if r.URL.Query().Get("view") == "standing" {
		writeJSON(w, http.StatusOK, resp)
		return
	}

	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), agentID); err == nil && agent != nil {
			resp.DisplayName = agent.Name
			resp.OnChainHeight = agent.OnChainHeight
		}
		if domains, err := s.agentStore.ListAgentDomains(r.Context(), agentID); err == nil && len(domains) > 0 {
			resp.Domains = domains
		}
	}

	// EWMA accuracy: cold-start prior (0.5) when no score record exists, else
	// reconstruct the tracker from the persisted WeightedSum/WeightDenom/Count.
	resp.Accuracy = (&poe.EWMATracker{}).Accuracy()
	if score, err := s.scoreStore.GetScore(r.Context(), agentID); err == nil && score != nil {
		resp.PoEWeight = score.CurrentWeight
		resp.VoteCount = score.VoteCount
		tracker := &poe.EWMATracker{
			WeightedSum: score.WeightedSum,
			WeightDenom: score.WeightDenom,
			Count:       score.VoteCount,
		}
		resp.Accuracy = tracker.Accuracy()
	}

	// Authoritative on-chain PoE signals from BadgerDB (the consensus source of
	// truth — the off-chain mirror above can lag a block). corr_count and the
	// per-domain expertise EWMA are not mirrored off-chain at all, so they can
	// only come from here. Nil-guarded: the handler test suite constructs a
	// Server without a badgerStore.
	if s.badgerStore != nil {
		if vs, err := s.badgerStore.GetValidatorStats(agentID); err == nil && vs != nil {
			resp.CorrCount = int64(vs.CorrCount) // #nosec G115 -- corroboration count is small/non-negative
			if vs.EWMACount > 0 {
				resp.Accuracy = (&poe.EWMATracker{
					WeightedSum: vs.EWMAWeightedSum,
					WeightDenom: vs.EWMAWeightDenom,
					Count:       int64(vs.EWMACount), // #nosec G115 -- non-negative
				}).Accuracy()
			}
		}
		// Per-domain verdict-correctness, only for domains the agent has actually
		// voted in (EWMACount > 0) so the payload stays focused.
		for _, d := range resp.Domains {
			ds, err := s.badgerStore.GetValidatorDomainStats(agentID, d)
			if err != nil || ds == nil || ds.EWMACount == 0 {
				continue
			}
			if resp.DomainExpertise == nil {
				resp.DomainExpertise = make(map[string]float64)
			}
			resp.DomainExpertise[d] = (&poe.EWMATracker{
				WeightedSum: ds.EWMAWeightedSum,
				WeightDenom: ds.EWMAWeightDenom,
				Count:       int64(ds.EWMACount), // #nosec G115 -- non-negative
			}).Accuracy()
		}
	}

	writeJSON(w, http.StatusOK, resp)
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

	agentID := middleware.ContextAgentID(r.Context())

	// Per-domain read ACL — parity with /v1/memory/list and the query/hybrid
	// recall path. Pending records carry full, unredacted, pre-commit CONTENT, so
	// without this gate any registered agent could enumerate every in-flight
	// submission across every domain (the empty-domain default below fans out to
	// ALL domains via a LIKE '%'). When a concrete domain is requested we 403 up
	// front; the per-record filter covers the all-domains fan-out. checkDomainAccess
	// is the operative per-domain gate; the multi-org block below mirrors query/list
	// for shape-parity (a no-op once checkDomainAccess approves a concrete domain).
	domainAccessApproved := false
	if domainTag != "" {
		if accessErr := s.checkDomainAccess(r.Context(), agentID, domainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	if domainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domErr := s.badgerStore.GetDomainOwner(domainTag)
		if domErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.hasMemoryReadAccess(domainTag, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domainTag))
				return
			}
		}
	}

	if domainTag == "" {
		domainTag = "%" // match all domains; the per-record filter below compartments
	}

	var records []*memory.MemoryRecord
	var err error
	if s.isPostV23ForNextTx() {
		if pager, ok := s.store.(store.PendingMemoryPageStore); ok {
			records, err = s.collectAppV23VisibleRecords(
				r.Context(), agentID, limit,
				func(ctx context.Context, pageLimit, offset int) ([]*memory.MemoryRecord, error) {
					return pager.GetPendingByDomainPage(
						ctx, domainTag, pageLimit, offset,
					)
				},
			)
		} else {
			// Compatibility fallback for external MemoryStore implementations.
			// Built-in SQLite/Postgres stores implement the paging extension.
			records, err = s.store.GetPendingByDomain(r.Context(), domainTag, limit)
		}
	} else {
		records, err = s.store.GetPendingByDomain(r.Context(), domainTag, limit)
	}
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to get pending memories")
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Pending-memory authorization state is unavailable; retry later.")
			return
		}
		if errors.Is(err, errAppV23DisclosureScanBudget) {
			writeProblem(w, http.StatusUnprocessableEntity, "Pending query too broad",
				"Too many pending candidates require authorization; choose a domain filter.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query pending memories.")
		return
	}

	// Per-record gate — drop pending records whose domain the caller cannot read
	// (covers the all-domains fan-out) and records classified above the caller's
	// clearance. The submitter always sees its own pending submissions.
	if s.badgerStore != nil {
		now := time.Now()
		kept := records[:0]
		for _, rec := range records {
			if s.isPostV23ForNextTx() {
				disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
					agentID, rec, now,
				)
				if disclosureErr != nil {
					if isUnsafeAppV23Projection(disclosureErr) {
						continue
					}
					writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
						"Pending-memory authorization state is unavailable; retry later.")
					return
				}
				if !disclosure.Allowed {
					continue
				}
			} else if rec.SubmittingAgent != agentID {
				// Domain-read filter — only needed when no single domain was pre-gated.
				if !domainAccessApproved && rec.DomainTag != "" {
					if accessErr := s.checkDomainAccess(r.Context(), agentID, rec.DomainTag, "read"); accessErr != nil {
						continue
					}
				}
				// Classification filter.
				if rec.DomainTag != "" {
					memClass, _ := s.badgerStore.GetMemoryClassification(rec.MemoryID)
					if memClass > 0 {
						domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
						if domErr == nil && domainOwner != "" {
							hasAccess, _ := s.hasMemoryReadAccess(rec.DomainTag, agentID, memClass, now)
							if !hasAccess {
								continue
							}
						}
					}
				}
			}
			kept = append(kept, rec)
		}
		records = kept
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
