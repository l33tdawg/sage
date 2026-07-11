package rest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
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
	// Tags are user-defined labels attached after consensus commit. They're
	// node-local metadata (not part of the on-chain tx), queryable via the
	// `tags` filter on /v1/memory/query and /v1/memory/search.
	Tags []string `json:"tags,omitempty"`
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
	// Tags, when non-empty, restricts results to memories tagged with ANY
	// of the listed values (OR semantics). SQLite-only.
	Tags []string `json:"tags,omitempty"`
	// Federated opts this recall into the v11 cross-network proxy: results
	// from every active cross_fed peer are merged in (read-only, stamped with
	// source_chain_id). FederateChains narrows the fan-out to named chains
	// ("*" = all active). Both default off — local behaviour is unchanged.
	Federated      bool     `json:"federated,omitempty"`
	FederateChains []string `json:"federate_chains,omitempty"`
}

// QueryMemoryResponse is the JSON body for a successful query.
type QueryMemoryResponse struct {
	Results    []*MemoryResult `json:"results"`
	NextCursor string          `json:"next_cursor,omitempty"`
	TotalCount int             `json:"total_count"`
	Filtered   *FilterInfo     `json:"filtered,omitempty"`
	// Federation discloses the cross-network fan-out when a federated recall
	// ran: which peers were queried and which failed (fail-closed peers are
	// reported, never silently dropped).
	Federation *FederationInfo `json:"federation,omitempty"`
}

// FederationInfo discloses the outcome of a federated recall fan-out.
type FederationInfo struct {
	Queried []string          `json:"queried"`
	Merged  int               `json:"merged"`
	Errors  map[string]string `json:"errors,omitempty"`
}

// FilterInfo surfaces server-side filters that hid data from the caller.
// Populated when any silent-hide filter ran so clients can distinguish
// "empty result" from "access-limited result" without guessing.
// See X-SAGE-Filter-Applied response header for the same info in header form.
type FilterInfo struct {
	By                []string `json:"by"`
	TotalBeforeFilter *int     `json:"total_before_filter,omitempty"`
	Visible           *int     `json:"visible,omitempty"`
	HiddenCount       *int     `json:"hidden_count,omitempty"`
}

const (
	filterHeader           = "X-SAGE-Filter-Applied"
	filterBySubmittingAgts = "rbac_submitting_agents"
	filterByClassification = "classification"
)

// MemoryResult is a memory record with computed confidence.
type MemoryResult struct {
	MemoryID        string `json:"memory_id"`
	SubmittingAgent string `json:"submitting_agent"`
	Content         string `json:"content"`
	ContentHash     string `json:"content_hash"`
	MemoryType      string `json:"memory_type"`
	DomainTag       string `json:"domain_tag"`
	// ConfidenceScore is the DECAYED confidence: the stored value after time decay
	// and the corroboration boost, computed at read time. This is the number the
	// min_confidence floor is enforced against (rest-api.md).
	ConfidenceScore float64 `json:"confidence_score"`
	// InitialConfidence is the STORED (undecayed) confidence — the on-chain value
	// set at submission (corroboration never rewrites it), before any time decay. Exposing it
	// alongside the decayed ConfidenceScore lets a reader see the authoritative
	// floor without re-deriving it (the two diverge as a memory ages). A pointer so
	// a legitimate stored 0.0 still serializes (a plain omitempty float would drop
	// it); non-nil for local memories, nil for federated results, where only the
	// serving peer's already-decayed value is available. (v11.2.0)
	InitialConfidence *float64 `json:"initial_confidence,omitempty"`
	// CorroborationCount is the number of distinct corroborations backing this
	// memory — the multiplier behind the corroboration boost in ConfidenceScore.
	// Exposing it lets readers distinguish a low score caused by no corroboration
	// (a fresh, untested belief) from one caused by time decay (a once-solid fact).
	CorroborationCount int    `json:"corroboration_count"`
	Classification     int    `json:"classification"`
	Status             string `json:"status"`
	// Disputed is true for an app-v17 two-phase-CHALLENGED memory: still live and
	// recallable, but under dispute pending confirm/reinstate. Off-chain surface
	// signal only — the on-chain status is carried in Status ("challenged"). When
	// set, ConfidenceScore already reflects the disputed haircut. Personal nodes
	// never produce this (a challenge there is one-strike deprecate).
	Disputed    bool       `json:"disputed,omitempty"`
	ParentHash  string     `json:"parent_hash,omitempty"`
	TaskStatus  string     `json:"task_status,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	CommittedAt *time.Time `json:"committed_at,omitempty"`
	// SourceChainID is v11 federation provenance: empty for local memories,
	// the origin chain id for results merged in from a cross_fed peer (whose
	// SubmittingAgent is then chain-qualified as pubkey@chain_id). Remote
	// results live only in this response — they are never stored locally.
	SourceChainID string `json:"source_chain_id,omitempty"`
}

// MemoryDetailResponse is a memory record with votes and corroborations.
type MemoryDetailResponse struct {
	MemoryID        string                  `json:"memory_id"`
	SubmittingAgent string                  `json:"submitting_agent"`
	Content         string                  `json:"content"`
	ContentHash     string                  `json:"content_hash"`
	MemoryType      string                  `json:"memory_type"`
	DomainTag       string                  `json:"domain_tag"`
	ConfidenceScore float64                 `json:"confidence_score"`
	Classification  int                     `json:"classification"`
	Status          string                  `json:"status"`
	ParentHash      string                  `json:"parent_hash,omitempty"`
	TaskStatus      string                  `json:"task_status,omitempty"`
	CreatedAt       time.Time               `json:"created_at"`
	CommittedAt     *time.Time              `json:"committed_at,omitempty"`
	Votes           []*store.ValidationVote `json:"votes,omitempty"`
	Corroborations  []*store.Corroboration  `json:"corroborations,omitempty"`
	LinkedMemories  []memory.MemoryLink     `json:"linked_memories,omitempty"`
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
		Height int64  `json:"height,string"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		// Data carries the actionable detail for mempool rejections —
		// CometBFT returns message="Internal error" with the real cause
		// ("mempool is full: number of txs N (max: M)") in error.data.
		Data string `json:"data"`
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

	// v7.1: the node operator (whoever signs with ~/.sage/agent.key) bypasses
	// the cross-agent visibility filter. The operator owns the node, so the
	// agent-isolation gate doesn't apply to them — only multi-agent peer
	// visibility does. Lifts the v7.0 SessionStart-hook prefetch limit on
	// nodes where the LLM identity is registered separately from the
	// operator. Per-domain access and classification gates still run.
	if s.nodeOperatorID != "" && agentID == s.nodeOperatorID {
		return nil, true
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

	// Org-clearance-as-seeAll: a TopSecret member of any org is trusted to
	// see across agents within their org's visibility envelope. Per-domain
	// access control (HasAccessMultiOrg, checkDomainAccess) and per-record
	// classification gates still apply - this only lifts the submitting_agents
	// filter, not the domain-access filters. In single-org deployments this
	// closes the "visible_agents=\"*\" on every agent" boilerplate.
	if s.badgerStore != nil && s.agentHasTopSecretClearance(agentID) {
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

// agentHasTopSecretClearance reports whether the agent is a TopSecret-cleared
// member of any org they belong to. Used to lift the submitting_agents filter
// for trusted agents without forcing admins to configure visible_agents="*"
// per member. Iterates every org membership — TS in one org is enough.
func (s *Server) agentHasTopSecretClearance(agentID string) bool {
	if s.badgerStore == nil {
		return false
	}
	orgIDs, err := s.badgerStore.ListAgentOrgs(agentID)
	if err != nil || len(orgIDs) == 0 {
		return false
	}
	for _, orgID := range orgIDs {
		clearance, _, gerr := s.badgerStore.GetMemberClearance(orgID, agentID)
		if gerr != nil {
			continue
		}
		if clearance >= uint8(tx.ClearanceTopSecret) {
			return true
		}
	}
	return false
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

	// Build the on-chain transaction. v6.8.6: REST passes the caller's
	// classification through verbatim — 0 means PUBLIC. The prior
	// 0→INTERNAL bump here turned an absent or explicitly-public submission
	// into INTERNAL on the wire, which then triggered the per-record
	// classification gate in handleQueryMemory for every cross-agent read.
	classification := req.Classification

	submitTx := &tx.ParsedTx{
		Type:      tx.TxTypeMemorySubmit,
		Nonce:     tx.MonotonicNonce(s.signingKey),
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
	s.embedAgentAuth(r.Context(), submitTx)

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
			Embedding:         req.Embedding,
			EmbeddingHash:     embeddingHash,
			Provider:          req.Provider,
			EmbeddingProvider: s.embedderStampFor(req.Embedding),
			KnowledgeTriples:  req.KnowledgeTriples,
		})
	}

	// Broadcast via CometBFT RPC and wait for block finalization.
	// broadcast_tx_commit blocks until the block containing this tx is committed,
	// meaning ABCI Commit has already flushed the memory to the offchain store.
	txHash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast submit tx")
		status, publicMsg := broadcastErrorPublic(err)
		if isMempoolFullErr(err) {
			// Chain backpressure, not a client fault: tell the writer when
			// to come back (one block interval drains the mempool) and
			// stamp the distinct problem type so this 429 is
			// distinguishable from the rate limiter's quota 429. Header
			// must be set before writeProblemTyped calls WriteHeader —
			// mirrors middleware/ratelimit.go.
			w.Header().Set("Retry-After", "1")
			writeProblemTyped(w, status, mempoolFullProblemType, "Mempool full", publicMsg)
			return
		}
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	metrics.MemoriesTotal.WithLabelValues(req.MemoryType, req.DomainTag, string(memory.StatusProposed)).Inc()

	// Attach user-defined tags after the block is committed. Tags are
	// non-consensus metadata (node-local), so we set them via the store
	// rather than folding them into the tx. broadcastTxCommit has already
	// returned after Commit flushed the memory, so SetTags will find it.
	//
	// Use a fresh short-timeout context rather than r.Context() — the
	// client may have disconnected (SIGKILL, network drop) between
	// broadcastTxCommit completing and here, and with r.Context() every
	// interrupted submit leaves an untagged orphan row that the next
	// idempotency run re-proposes as a duplicate. The commit has already
	// landed on-chain; tag attachment is a node-local finalisation step
	// and must not depend on the HTTP request staying alive.
	tagCtx, tagCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer tagCancel()

	if len(req.Tags) > 0 {
		if setErr := s.store.SetTags(tagCtx, memoryID, req.Tags); setErr != nil {
			s.logger.Warn().Err(setErr).Str("memory_id", memoryID).Msg("failed to set tags on committed memory")
		}
	}

	// Update agent's last activity timestamp. Same disconnect rationale as
	// SetTags above — the memory is on-chain, the last-seen update is
	// post-commit bookkeeping and should not fail just because the client
	// went away.
	if agentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(tagCtx, agentID, time.Now()); updateErr != nil {
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

	// Backpressure hint for streaming writers: current mempool fill fraction
	// (0..1) from the ~1s-TTL cached sampler, so clients can self-throttle
	// with zero extra round-trips. Best-effort — omitted when the probe
	// fails, and must be set before writeJSON calls WriteHeader.
	if smp, ok := s.mempool.sample(); ok {
		w.Header().Set("X-Sage-Mempool-Pct", formatMempoolPct(smp.Pct))
	}

	writeJSON(w, http.StatusCreated, SubmitMemoryResponse{
		MemoryID: memoryID,
		TxHash:   txHash,
		Status:   string(memory.StatusProposed),
	})
}

// setDecayFloor moves a min_confidence request onto the store's DECAYED-confidence
// floor (store.QueryOptions.DecayFloor): the store then filters on the decayed value
// over the full candidate set before the top-K trim — so top_k is filled with
// qualifying records and corroboration-boosted memories are not starved — pinned to
// `now` so the value the store filters on is exactly the one this handler serializes
// below. The legacy stored-column MinConfidence filter is disabled so it cannot also
// run (it is decay-blind in both directions). No-op when no floor is requested, so
// the fast path is unchanged. `now` must be the same instant used for serialization.
func setDecayFloor(opts *store.QueryOptions, now time.Time) {
	if opts.MinConfidence > 0 {
		opts.DecayFloor = opts.MinConfidence
		opts.DecayNow = now
		opts.MinConfidence = 0
	}
}

// initialConfidencePtr returns a pointer to a stored (undecayed) confidence for the
// InitialConfidence response field — a pointer, not an omitempty float, so a
// legitimate stored 0.0 still serializes; nil is reserved for federated results.
func initialConfidencePtr(v float64) *float64 { return &v }

// disputedConfidenceHaircut is shared with the store's decayed-floor admission
// logic. Keeping one value ensures a min_confidence=X query never admits a
// challenged row and then serializes confidence_score below X after the haircut.
// Off-chain presentation only; consensus/AppHash and stored confidence are
// untouched.
const disputedConfidenceHaircut = store.DisputedConfidenceHaircut

// markDisputed reports whether rec is a live-but-disputed (challenged) memory and,
// if so, applies the confidence haircut to conf in place. Off-chain recall surface
// only — the on-chain status is unchanged and still carried in rec.Status.
func markDisputed(rec *memory.MemoryRecord, conf *float64) bool {
	if rec.Status != memory.StatusChallenged {
		return false
	}
	*conf *= disputedConfidenceHaircut
	return true
}

// corroborationCounts batch-fetches corroboration counts for a set of records,
// keyed by memory ID — one query in place of the per-record GetCorroborations N+1
// the recall loops used to run. These feed the SERIALIZED confidence_score. On a
// store error it returns (nil, err): the caller may degrade to no boost (a slightly
// low displayed score) on the no-floor path, but MUST fail closed under a
// min_confidence floor — otherwise a boosted record the store correctly kept could
// serialize a confidence_score below the floor the caller queried with.
func (s *Server) corroborationCounts(ctx context.Context, records []*memory.MemoryRecord) (map[string]int, error) {
	if len(records) == 0 {
		return nil, nil
	}
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.MemoryID
	}
	counts, err := s.store.GetCorroborationCounts(ctx, ids)
	if err != nil {
		s.logger.Warn().Err(err).Msg("batch corroboration count failed")
		return nil, err
	}
	return counts, nil
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
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(req.DomainTag, agentID, 0, time.Now(), s.isPostV8Fork())
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
			hasOrgAccess, _ := s.badgerStore.HasAccessMultiOrg(req.DomainTag, queryAgentID, 0, time.Now(), s.isPostV8Fork())
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
		Tags:          req.Tags,
		// app-v17: keep disputed-but-live memories recallable (flagged + hair-cut
		// at serialize). A no-op unless the caller's filter is "committed".
		IncludeDisputed: true,
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}
	// min_confidence is a DECAYED-confidence floor (rest-api.md): hand it to the
	// store as DecayFloor, which filters the decayed value over the full candidate
	// set before the top-K trim, pinned to `start` so it matches what we serialize.
	setDecayFloor(&opts, start)

	var records []*memory.MemoryRecord
	records, err = s.store.QuerySimilar(r.Context(), req.Embedding, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to query memories")
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query memories.")
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Serialize with task-aware decay (open tasks don't decay), pinned to the same
	// instant the store filtered on; classification filtering as before.
	now := start
	corrCounts, ccErr := s.corroborationCounts(r.Context(), records)
	if opts.DecayFloor > 0 && ccErr != nil {
		// Fail closed under a floor: without accurate corroboration counts a boosted
		// record the store kept could serialize below the floor. (No floor: degrade.)
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to compute confidence scores.")
		return
	}
	results := make([]*MemoryResult, 0, len(records))
	hiddenByClassification := 0
	for _, rec := range records {
		// Per-record domain-read filter — parity with list/tasks/pending. On the
		// no-domain path the store returns candidates across ALL domains; a seeAll
		// caller (visible_agents "*", TopSecret, operator) must not receive content
		// from a domain it has no read grant on. Skipped for own records and when a
		// concrete domain was already gated up front.
		if rec.SubmittingAgent != queryAgentID && req.DomainTag == "" && rec.DomainTag != "" {
			if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, queryAgentID, rec.DomainTag, "read"); accessErr != nil {
				continue
			}
		}
		// Classification gate: check agent clearance >= memory classification
		// Only enforce when domain has a registered owner (backward compat for pre-RBAC setups)
		var memClass uint8
		if s.badgerStore != nil {
			memClass, _ = s.badgerStore.GetMemoryClassification(rec.MemoryID)
			if memClass > 0 {
				domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
				if domErr == nil && domainOwner != "" {
					hasAccess, _ := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, queryAgentID, memClass, now, s.isPostV8Fork())
					if !hasAccess && rec.SubmittingAgent != queryAgentID {
						// v6.8.6 observability: log every hide so operators can
						// detect missing org-bootstrap (the failure mode is silent
						// otherwise — the response carries `filtered.by` but
						// server logs gave no per-record reason). Info-level so
						// it survives default log config.
						s.logger.Info().
							Str("memory_id", rec.MemoryID).
							Str("domain", rec.DomainTag).
							Str("submitter", rec.SubmittingAgent[:16]).
							Str("querier", queryAgentID[:16]).
							Str("domain_owner", domainOwner[:16]).
							Uint8("classification", memClass).
							Msg("classification gate hid memory: querier has no shared-org path to writer at required clearance")
						hiddenByClassification++
						continue // Skip memories the agent can't access
					}
				}
			}
		}

		// Corroboration count (batch-fetched) feeds the boost; task-aware compute so
		// open tasks serialize their non-decaying stored confidence. The store already
		// applied the decayed floor over the full candidate set, so no drop here.
		corrCount := corrCounts[rec.MemoryID]
		currentConf := memory.ComputeConfidenceForRecord(rec, now, corrCount)
		// app-v17: flag disputed-but-live (challenged) memories and hair-cut their
		// surfaced confidence. Off-chain only — the on-chain status stays in
		// rec.Status and committed rows are untouched.
		disputed := markDisputed(rec, &currentConf)

		results = append(results, &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    currentConf,
			InitialConfidence:  initialConfidencePtr(rec.ConfidenceScore),
			CorroborationCount: corrCount,
			Classification:     int(memClass),
			Status:             string(rec.Status),
			Disputed:           disputed,
			ParentHash:         rec.ParentHash,
			CreatedAt:          rec.CreatedAt,
			CommittedAt:        rec.CommittedAt,
		})
	}
	// The store enforced the decayed floor over the full candidate set and filled
	// top_k, so there is nothing to drop or cap here.

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

	resp := QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	}
	setFilterInfo(w, &resp, filterApplied, hiddenByClassification)

	s.mergeFederatedRecall(r, &resp, req.Federated, req.FederateChains, &federation.QueryRequest{
		Mode:          federation.ModeSemantic,
		Embedding:     req.Embedding,
		DomainTag:     req.DomainTag,
		MinConfidence: req.MinConfidence,
		TopK:          req.TopK,
		Tags:          req.Tags,
	})

	writeJSON(w, http.StatusOK, resp)
}

// setFilterInfo attaches the X-SAGE-Filter-Applied header and the `filtered`
// envelope field to a query/search response when either silent-hide filter ran.
// submittingAgentsApplied indicates the store-level agent-isolation filter was
// used; hiddenByClassification is the count of records dropped by the
// in-handler classification+multi-org gate.
func setFilterInfo(w http.ResponseWriter, resp *QueryMemoryResponse, submittingAgentsApplied bool, hiddenByClassification int) {
	if !submittingAgentsApplied && hiddenByClassification == 0 {
		return
	}
	var applied []string
	if submittingAgentsApplied {
		applied = append(applied, filterBySubmittingAgts)
	}
	if hiddenByClassification > 0 {
		applied = append(applied, filterByClassification)
	}
	w.Header().Set(filterHeader, strings.Join(applied, ","))
	info := &FilterInfo{By: applied}
	if hiddenByClassification > 0 {
		hc := hiddenByClassification
		info.HiddenCount = &hc
	}
	resp.Filtered = info
}

// defaultFedRecallTimeout bounds the cross-network fan-out so a slow or dead
// peer can't stall a local recall. Overridable via SAGE_FED_RECALL_TIMEOUT_MS
// (mirrors broadcastTxCommitTimeout's pattern).
const defaultFedRecallTimeout = 4 * time.Second

func fedRecallTimeout() time.Duration {
	if v := os.Getenv("SAGE_FED_RECALL_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultFedRecallTimeout
}

// mergeFederatedRecall appends read-only results from cross_fed peers to a
// recall response (v11 Mode-1 exchange). Opt-in per request; no-op when the
// federation transport isn't wired or the request didn't ask. Remote results
// arrive already filtered by the SERVING peer's treaty enforcement (its
// AllowedDomains + MaxClearance + committed-only) and are merged into the
// response ONLY — nothing touches local stores, embeddings, or AppHash state.
// Peer failures are disclosed in resp.Federation.Errors, never silently
// dropped (fail-closed plus disclosure, same philosophy as FilterInfo).
//
// AUTHORIZATION: federated recall is an OPERATOR capability — the opt-in is
// gated to the node operator (the identity that establishes agreements). The
// per-agreement MaxClearance is a chain↔chain ceiling, NOT the requesting
// local agent's personal clearance, so letting an arbitrary local agent invoke
// it on a multi-tenant node would be a clearance side-channel (a low-clearance
// agent reading a peer's higher-clearance corpus). Per-local-agent clearance
// mapping of remote results is the richer future model (plan §9.8 clearance_map);
// until then the operator gate is the conservative, escalation-free default.
func (s *Server) mergeFederatedRecall(r *http.Request, resp *QueryMemoryResponse, federated bool, chains []string, fedReq *federation.QueryRequest) {
	if s.federation == nil || (!federated && len(chains) == 0) {
		return
	}
	callerID := middleware.ContextAgentID(r.Context())
	if s.nodeOperatorID == "" || callerID != s.nodeOperatorID {
		// Not permitted for this caller — disclose rather than silently return
		// local-only, so the client can tell federation was declined vs unwired.
		resp.Federation = &FederationInfo{
			Queried: []string{},
			Errors:  map[string]string{"*": "federated recall is restricted to the node operator"},
		}
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedRecallTimeout())
	defer cancel()

	outcomes := s.federation.FanOutRecall(ctx, chains, fedReq)
	if len(outcomes) == 0 {
		resp.Federation = &FederationInfo{Queried: []string{}}
		return
	}
	info := &FederationInfo{}
	for _, outcome := range outcomes {
		info.Queried = append(info.Queried, outcome.ChainID)
		if outcome.Err != nil {
			if info.Errors == nil {
				info.Errors = make(map[string]string)
			}
			info.Errors[outcome.ChainID] = outcome.Err.Error()
			s.logger.Warn().Err(outcome.Err).Str("peer", outcome.ChainID).Msg("federated recall peer failed")
			continue
		}
		for _, fr := range outcome.Results {
			// Enforce the caller's decayed floor on peer results too: fr.ConfidenceScore
			// is already the serving peer's decayed value, so this keeps the
			// min_confidence contract ("every returned result satisfies confidence_score
			// >= X") intact across the federation bridge — including against peers on an
			// older build whose serving side still filtered on the stored column.
			if fedReq.MinConfidence > 0 && fr.ConfidenceScore < fedReq.MinConfidence {
				continue
			}
			info.Merged++
			resp.Results = append(resp.Results, &MemoryResult{
				MemoryID:           fr.MemoryID,
				SubmittingAgent:    fr.SubmittingAgent,
				Content:            fr.Content,
				ContentHash:        fr.ContentHash,
				MemoryType:         fr.MemoryType,
				DomainTag:          fr.DomainTag,
				ConfidenceScore:    fr.ConfidenceScore,
				CorroborationCount: fr.CorroborationCount,
				Classification:     fr.Classification,
				Status:             fr.Status,
				CreatedAt:          fr.CreatedAt,
				CommittedAt:        fr.CommittedAt,
				SourceChainID:      fr.SourceChainID,
			})
		}
	}
	resp.TotalCount = len(resp.Results)
	resp.Federation = info
}

// SearchMemoryRequest is the JSON body for POST /v1/memory/search.
type SearchMemoryRequest struct {
	Query         string   `json:"query"`
	DomainTag     string   `json:"domain_tag,omitempty"`
	Provider      string   `json:"provider,omitempty"`
	MinConfidence float64  `json:"min_confidence,omitempty"`
	StatusFilter  string   `json:"status_filter,omitempty"`
	TopK          int      `json:"top_k,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	// v11 federated recall opt-in — see QueryMemoryRequest.
	Federated      bool     `json:"federated,omitempty"`
	FederateChains []string `json:"federate_chains,omitempty"`
}

// handleSearchMemory handles POST /v1/memory/search — FTS5 full-text search.
// Same access control as handleQueryMemory but uses text matching instead of embeddings.
func (s *Server) handleSearchMemory(w http.ResponseWriter, r *http.Request) {
	var req SearchMemoryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Query == "" {
		writeProblem(w, http.StatusBadRequest, "Missing query", "query field is required for text search.")
		return
	}

	// Domain access control (same as handleQueryMemory)
	domainAccessApproved := false
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, req.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}

	// Multi-org access control gate
	if req.DomainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(req.DomainTag)
		if domainErr == nil && domainOwner != "" {
			agentID := middleware.ContextAgentID(r.Context())
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(req.DomainTag, agentID, 0, time.Now(), s.isPostV8Fork())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", req.DomainTag))
				return
			}
		}
	}

	// Agent isolation RBAC
	queryAgentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(queryAgentID)

	if !seeAll && domainAccessApproved {
		seeAll = true
	}

	if !seeAll && req.DomainTag != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(req.DomainTag, queryAgentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.badgerStore.HasAccessMultiOrg(req.DomainTag, queryAgentID, 0, time.Now(), s.isPostV8Fork())
			if hasOrgAccess {
				seeAll = true
			} else {
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
		Tags:          req.Tags,
		// app-v17: keep disputed-but-live memories recallable (flagged + hair-cut
		// at serialize). A no-op unless the caller's filter is "committed".
		IncludeDisputed: true,
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}
	// min_confidence is a DECAYED-confidence floor (rest-api.md): hand it to the
	// store as DecayFloor, which filters the decayed value over the full candidate
	// set before the top-K trim, pinned to `start` so it matches what we serialize.
	setDecayFloor(&opts, start)

	records, err := s.store.SearchByText(r.Context(), req.Query, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to search memories")
		writeProblem(w, http.StatusInternalServerError, "Search error", err.Error())
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Serialize with task-aware decay (same as handleQueryMemory), pinned to the
	// store's filter instant.
	now := start
	corrCounts, ccErr := s.corroborationCounts(r.Context(), records)
	if opts.DecayFloor > 0 && ccErr != nil {
		// Fail closed under a floor: without accurate corroboration counts a boosted
		// record the store kept could serialize below the floor. (No floor: degrade.)
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to compute confidence scores.")
		return
	}
	results := make([]*MemoryResult, 0, len(records))
	hiddenByClassification := 0
	for _, rec := range records {
		// Per-record domain-read filter — parity with list/tasks/pending (see
		// handleQueryMemory). Drops cross-domain content the caller has no read
		// grant on, on the no-domain path; skips own records and concrete-domain
		// requests already gated up front.
		if rec.SubmittingAgent != queryAgentID && req.DomainTag == "" && rec.DomainTag != "" {
			if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, queryAgentID, rec.DomainTag, "read"); accessErr != nil {
				continue
			}
		}
		var memClass uint8
		if s.badgerStore != nil {
			memClass, _ = s.badgerStore.GetMemoryClassification(rec.MemoryID)
			if memClass > 0 {
				domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
				if domErr == nil && domainOwner != "" {
					hasAccess, _ := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, queryAgentID, memClass, now, s.isPostV8Fork())
					if !hasAccess && rec.SubmittingAgent != queryAgentID {
						s.logger.Info().
							Str("memory_id", rec.MemoryID).
							Str("domain", rec.DomainTag).
							Str("submitter", rec.SubmittingAgent[:16]).
							Str("querier", queryAgentID[:16]).
							Str("domain_owner", domainOwner[:16]).
							Uint8("classification", memClass).
							Msg("classification gate hid memory: querier has no shared-org path to writer at required clearance")
						hiddenByClassification++
						continue
					}
				}
			}
		}

		corrCount := corrCounts[rec.MemoryID]
		currentConf := memory.ComputeConfidenceForRecord(rec, now, corrCount)
		// app-v17: flag disputed-but-live (challenged) memories and hair-cut their
		// surfaced confidence. Off-chain only — the on-chain status stays in
		// rec.Status and committed rows are untouched.
		disputed := markDisputed(rec, &currentConf)

		results = append(results, &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    currentConf,
			InitialConfidence:  initialConfidencePtr(rec.ConfidenceScore),
			CorroborationCount: corrCount,
			Classification:     int(memClass),
			Status:             string(rec.Status),
			Disputed:           disputed,
			ParentHash:         rec.ParentHash,
			CreatedAt:          rec.CreatedAt,
			CommittedAt:        rec.CommittedAt,
		})
	}
	// The store enforced the decayed floor and filled top_k; nothing to cap here.

	// Update agent last activity
	if queryAgentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), queryAgentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", queryAgentID).Msg("failed to update agent last_seen on search")
		}
	}

	// Emit search event for SSE chain activity log
	if s.OnEvent != nil && len(results) > 0 {
		domain := req.DomainTag
		if domain == "" && len(results) > 0 {
			domain = results[0].DomainTag
		}
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
		s.OnEvent("search", "", domain, fmt.Sprintf("%d memories found via text search", len(results)), map[string]any{
			"retrieved": retrieved,
		})
	}

	resp := QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	}
	setFilterInfo(w, &resp, filterApplied, hiddenByClassification)

	s.mergeFederatedRecall(r, &resp, req.Federated, req.FederateChains, &federation.QueryRequest{
		Mode:          federation.ModeText,
		Query:         req.Query,
		DomainTag:     req.DomainTag,
		MinConfidence: req.MinConfidence,
		TopK:          req.TopK,
		Tags:          req.Tags,
	})

	writeJSON(w, http.StatusOK, resp)
}

// HybridSearchMemoryRequest is the JSON body for POST /v1/memory/hybrid —
// the unified path that fuses FTS5/BM25 and vector cosine results via
// weighted Reciprocal Rank Fusion. Callers send both the text query and the
// precomputed embedding so the server can run both indexes in one round trip.
//
// v7.1 adds optional Expansions: a list of paraphrase/entity/temporal variants
// of the primary query. When non-empty the server runs SearchHybrid once per
// variant (in addition to the primary), then RRFs the variant rankings into
// one final list. Callers must include both the text and embedding for each
// expansion so SAGE doesn't need to know which embedder generated the primary.
type HybridSearchMemoryRequest struct {
	Query         string            `json:"query"`
	Embedding     []float32         `json:"embedding"`
	Expansions    []HybridExpansion `json:"expansions,omitempty"`
	DomainTag     string            `json:"domain_tag,omitempty"`
	Provider      string            `json:"provider,omitempty"`
	MinConfidence float64           `json:"min_confidence,omitempty"`
	StatusFilter  string            `json:"status_filter,omitempty"`
	TopK          int               `json:"top_k,omitempty"`
	Tags          []string          `json:"tags,omitempty"`
	// v11 federated recall opt-in — see QueryMemoryRequest.
	Federated      bool     `json:"federated,omitempty"`
	FederateChains []string `json:"federate_chains,omitempty"`
}

// HybridExpansion carries a single paraphrase/entity/temporal variant of the
// primary query plus the precomputed embedding for that variant. The caller
// is responsible for using the same embedder that produced the primary
// embedding so the vectors live in the same space as the stored memories.
type HybridExpansion struct {
	Query     string    `json:"query"`
	Embedding []float32 `json:"embedding"`
}

// handleHybridSearchMemory handles POST /v1/memory/hybrid.
// Runs SearchByText and QuerySimilar in parallel and fuses them via RRF.
// Access control mirrors handleQueryMemory exactly — the only differences are
// the underlying store call and that the request carries both a query string
// and an embedding.
func (s *Server) handleHybridSearchMemory(w http.ResponseWriter, r *http.Request) {
	var req HybridSearchMemoryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Query == "" && len(req.Embedding) == 0 {
		writeProblem(w, http.StatusBadRequest, "Missing inputs",
			"hybrid search requires at least one of `query` or `embedding`")
		return
	}

	domainAccessApproved := false
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, req.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}

	if req.DomainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(req.DomainTag)
		if domainErr == nil && domainOwner != "" {
			agentID := middleware.ContextAgentID(r.Context())
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(req.DomainTag, agentID, 0, time.Now(), s.isPostV8Fork())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", req.DomainTag))
				return
			}
		}
	}

	queryAgentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(queryAgentID)

	if !seeAll && domainAccessApproved {
		seeAll = true
	}

	if !seeAll && req.DomainTag != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(req.DomainTag, queryAgentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.badgerStore.HasAccessMultiOrg(req.DomainTag, queryAgentID, 0, time.Now(), s.isPostV8Fork())
			if hasOrgAccess {
				seeAll = true
			} else {
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
		Tags:          req.Tags,
		// app-v17: keep disputed-but-live memories recallable (flagged + hair-cut
		// at serialize). A no-op unless the caller's filter is "committed".
		IncludeDisputed: true,
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}
	// min_confidence is a DECAYED-confidence floor (rest-api.md): hand it to the
	// store as DecayFloor. Carried on the fused opts, both hybrid sub-queries filter
	// the decayed value before their trim, pinned to `start` for serialize-parity.
	setDecayFloor(&opts, start)

	records, err := s.runHybridWithExpansions(r.Context(), req, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to hybrid-search memories")
		writeProblem(w, http.StatusInternalServerError, "Hybrid search error", err.Error())
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Serialize with task-aware decay, pinned to the store's filter instant.
	now := start
	corrCounts, ccErr := s.corroborationCounts(r.Context(), records)
	if opts.DecayFloor > 0 && ccErr != nil {
		// Fail closed under a floor: without accurate corroboration counts a boosted
		// record the store kept could serialize below the floor. (No floor: degrade.)
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to compute confidence scores.")
		return
	}
	results := make([]*MemoryResult, 0, len(records))
	hiddenByClassification := 0
	for _, rec := range records {
		// Per-record domain-read filter — parity with list/tasks/pending (see
		// handleQueryMemory). Drops cross-domain content the caller has no read
		// grant on, on the no-domain path; skips own records and concrete-domain
		// requests already gated up front.
		if rec.SubmittingAgent != queryAgentID && req.DomainTag == "" && rec.DomainTag != "" {
			if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, queryAgentID, rec.DomainTag, "read"); accessErr != nil {
				continue
			}
		}
		var memClass uint8
		if s.badgerStore != nil {
			memClass, _ = s.badgerStore.GetMemoryClassification(rec.MemoryID)
			if memClass > 0 {
				domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
				if domErr == nil && domainOwner != "" {
					hasAccess, _ := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, queryAgentID, memClass, now, s.isPostV8Fork())
					if !hasAccess && rec.SubmittingAgent != queryAgentID {
						s.logger.Info().
							Str("memory_id", rec.MemoryID).
							Str("domain", rec.DomainTag).
							Str("submitter", rec.SubmittingAgent[:16]).
							Str("querier", queryAgentID[:16]).
							Str("domain_owner", domainOwner[:16]).
							Uint8("classification", memClass).
							Msg("classification gate hid memory (hybrid path)")
						hiddenByClassification++
						continue
					}
				}
			}
		}

		corrCount := corrCounts[rec.MemoryID]
		currentConf := memory.ComputeConfidenceForRecord(rec, now, corrCount)
		// app-v17: flag disputed-but-live (challenged) memories and hair-cut their
		// surfaced confidence. Off-chain only — the on-chain status stays in
		// rec.Status and committed rows are untouched.
		disputed := markDisputed(rec, &currentConf)

		results = append(results, &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    currentConf,
			InitialConfidence:  initialConfidencePtr(rec.ConfidenceScore),
			CorroborationCount: corrCount,
			Classification:     int(memClass),
			Status:             string(rec.Status),
			Disputed:           disputed,
			ParentHash:         rec.ParentHash,
			CreatedAt:          rec.CreatedAt,
			CommittedAt:        rec.CommittedAt,
		})
	}
	// The store enforced the decayed floor and filled top_k; nothing to cap here.

	if queryAgentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), queryAgentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", queryAgentID).Msg("failed to update agent last_seen on hybrid search")
		}
	}

	if s.OnEvent != nil && len(results) > 0 {
		domain := req.DomainTag
		if domain == "" {
			domain = results[0].DomainTag
		}
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
		s.OnEvent("hybrid", "", domain, fmt.Sprintf("%d memories retrieved via hybrid search", len(results)), map[string]any{
			"retrieved": retrieved,
		})
	}

	resp := QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	}
	setFilterInfo(w, &resp, filterApplied, hiddenByClassification)

	s.mergeFederatedRecall(r, &resp, req.Federated, req.FederateChains, &federation.QueryRequest{
		Mode:          federation.ModeHybrid,
		Query:         req.Query,
		Embedding:     req.Embedding,
		DomainTag:     req.DomainTag,
		MinConfidence: req.MinConfidence,
		TopK:          req.TopK,
		Tags:          req.Tags,
	})

	writeJSON(w, http.StatusOK, resp)
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

	// Access control gate: DomainAccess allowlist + multi-org classification.
	// Submitting agent always has access to their own memory.
	if rec.DomainTag != "" && agentID != rec.SubmittingAgent {
		// Per-agent DomainAccess allowlist gate — parity with query/search/hybrid/
		// list. Without it, an agent explicitly denied read on this domain could
		// still fetch the record one-by-one by id. Runs regardless of badgerStore
		// (checkDomainAccess falls back to the SQLite agentStore), matching the
		// sibling handlers. The submitter always retains access to its own memory.
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, rec.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		// Per-record classification — only when the domain has a registered owner.
		if s.badgerStore != nil {
			domainOwner, domainErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
			if domainErr == nil && domainOwner != "" {
				classification, _ := s.badgerStore.GetMemoryClassification(memoryID)
				hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, agentID, classification, time.Now(), s.isPostV8Fork())
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
	currentConf := memory.ComputeConfidenceForRecord(rec, time.Now(), len(corrs))

	// Surface the on-chain classification so GET /v1/memory/{id} matches the
	// `classification` field the query/search/hybrid responses already return
	// (handleQueryMemory et al.). Read-display only — enforcement still happens
	// via the HasAccessMultiOrg gate above; this just stops the detail view from
	// silently reporting every record as PUBLIC (0). Guarded on badgerStore like
	// that gate, since some deployment modes run without it.
	var memClass uint8
	if s.badgerStore != nil {
		memClass, _ = s.badgerStore.GetMemoryClassification(memoryID)
	}

	writeJSON(w, http.StatusOK, MemoryDetailResponse{
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
		Votes:           votes,
		Corroborations:  corrs,
	})
}

// handlePreValidate handles POST /v1/memory/pre-validate.
// Runs the per-node validation checks (dedup, quality, consistency) against
// proposed content without submitting on-chain. The node votes accept iff all pass.
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
		"accepted": acceptCount == len(votes), // the node votes accept iff every check passes
		"votes":    votes,
		"quorum":   fmt.Sprintf("%d/%d", acceptCount, len(votes)),
	})
}

// --- Helpers -----------------------------------------------------------------

// broadcastTxCommit sends a transaction to CometBFT and waits for block finalization
// (CheckTx + FinalizeBlock). All REST handlers use this so that consensus-side
// rejections surface as real HTTP errors rather than 201 + ghost tx_hash.
func (s *Server) broadcastTxCommit(txBytes []byte) (string, error) {
	hash, _, err := s.broadcastTxCommitWithHeight(txBytes)
	return hash, err
}

// defaultBroadcastTxCommitTimeout bounds how long broadcastTxCommit
// waits for /broadcast_tx_commit to return. CometBFT's own server-
// side BroadcastTxCommitMaxWaitMs defaults to 10s; setting the client
// equal to that has no headroom — under any consensus slowness or
// mempool backlog the client times out before the server gets a
// chance to reply with the genuine FinalizeBlock result.
//
// 60s matches the typical Cosmos-ecosystem client-side default and
// gives ~10× the quorum-mode TimeoutCommit (3s) of room. Operators
// who run unusually slow consensus (single-validator with heavy
// validators, network-partitioned multi-node) can override via the
// SAGE_TX_COMMIT_TIMEOUT_MS environment variable.
const defaultBroadcastTxCommitTimeout = 60 * time.Second

func broadcastTxCommitTimeout() time.Duration {
	if v := os.Getenv("SAGE_TX_COMMIT_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultBroadcastTxCommitTimeout
}

// broadcastTxCommitWithHeight is broadcastTxCommit plus the committed block
// height. Callers that need the on-chain height in their response (e.g.
// agent registration telemetry) use this variant; everything else stays
// on the simpler (hash, error) signature via broadcastTxCommit.
func (s *Server) broadcastTxCommitWithHeight(txBytes []byte) (string, int64, error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", s.cometbftRPC, txHex)

	ctx, cancel := context.WithTimeout(context.Background(), broadcastTxCommitTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return "", 0, fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("broadcast tx commit: %w", err)
	}
	defer resp.Body.Close()

	var result cometCommitResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, fmt.Errorf("decode broadcast commit response: %w", err)
	}

	if result.Error != nil {
		if result.Error.Data != "" {
			// error.data carries the real cause for mempool rejections
			// (message is just "Internal error") — fold it in so
			// broadcastErrorPublic can classify the failure.
			return "", 0, fmt.Errorf("broadcast error: %s: %s", result.Error.Message, result.Error.Data)
		}
		return "", 0, fmt.Errorf("broadcast error: %s", result.Error.Message)
	}

	if result.Result.CheckTx.Code != 0 {
		return "", 0, fmt.Errorf("tx rejected in CheckTx (code %d): %s", result.Result.CheckTx.Code, result.Result.CheckTx.Log)
	}

	if result.Result.TxResult.Code != 0 {
		return "", 0, fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", result.Result.TxResult.Code, result.Result.TxResult.Log)
	}

	return result.Result.Hash, result.Result.Height, nil
}

// broadcastErrorPublic returns the HTTP status and a sanitized public
// message for an error returned by broadcastTxCommit. The raw error string
// from CometBFT carries the full FinalizeBlock log, which previously leaked
// agent-id prefixes, internal codes, and "agent X not registered" oracles
// to REST callers. This helper maps known failure classes to canonical
// terse strings so the public response stays opaque while the server log
// retains the full diagnostic.
func broadcastErrorPublic(err error) (int, string) {
	if err == nil {
		return http.StatusInternalServerError, "internal error"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "access denied"):
		return http.StatusForbidden, "access denied"
	case strings.Contains(msg, "not in the validator set"):
		return http.StatusForbidden, "access denied"
	case strings.Contains(msg, "agent identity verification failed"):
		return http.StatusUnauthorized, "agent identity verification failed"
	case strings.Contains(msg, "not registered"):
		return http.StatusNotFound, "not found"
	case strings.Contains(msg, "mempool is full"):
		// CometBFT backpressure, not a fault. handleSubmitMemory upgrades
		// this to Retry-After + the distinct mempool-full problem type;
		// other broadcast callers at least surface the right status
		// instead of an opaque 500.
		return http.StatusTooManyRequests, "mempool full, retry later"
	// app-v16 deprecation-gate outcomes. These keys are matched BEFORE the generic
	// FinalizeBlock catch (the same error string would otherwise collapse to a
	// 400 "request rejected") and are ORDERED: the app-v16 "unknown memory" reject
	// also ends with "not authorized to deprecate", so the "no memory record" 404
	// must win over the 403. The 409 keys on the post-fork-only phrase "legacy
	// memory predating app-v8.4", so a PRE-app-v16 chain's domainless reject (which
	// lacks that phrase) falls through to the 403 authz case, not a 409 that would
	// advise an unavailable remediation.
	case strings.Contains(msg, "legacy memory predating app-v8.4"):
		return http.StatusConflict, "memory has no recorded domain (legacy pre-app-v8.4 record); deprecation is blocked until its domain is repaired via an OpMemoryDomainRepair governance proposal (app-v16)"
	case strings.Contains(msg, "no memory record"):
		return http.StatusNotFound, "unknown memory: no on-chain record for that memory id"
	case strings.Contains(msg, "not authorized to deprecate"):
		return http.StatusForbidden, "not authorized to deprecate this memory (need domain ownership or a level-3 modify grant)"
	case strings.Contains(msg, "reinstate:") && strings.Contains(msg, "not authorized"):
		return http.StatusForbidden, "not authorized to reinstate this memory (need domain ownership or a level-3 modify grant)"
	case strings.Contains(msg, "reinstate: memory") && strings.Contains(msg, "not found"):
		return http.StatusNotFound, "memory not found"
	case strings.Contains(msg, "is not challenged"):
		return http.StatusConflict, "memory is not currently challenged"
	case strings.Contains(msg, "tx rejected in CheckTx"):
		return http.StatusBadRequest, "request rejected"
	case strings.Contains(msg, "tx rejected in FinalizeBlock"):
		return http.StatusBadRequest, "request rejected"
	}
	return http.StatusInternalServerError, "internal error"
}

// mempoolFullProblemType is the RFC 7807 problem type stamped on the 429
// returned when CometBFT rejects a broadcast because its mempool is full.
// Distinct from the rate limiter's status-derived type
// (https://sage.dev/errors/429) so clients can tell chain backpressure
// ("everyone slow down") from a per-agent quota breach ("you slow down").
const mempoolFullProblemType = "https://sage.dev/errors/mempool-full"

// isMempoolFullErr reports whether a broadcast error is CometBFT's
// mempool-is-full rejection. The detail arrives in the JSON-RPC error.data
// field ("mempool is full: number of txs N (max: M)"), which
// broadcastTxCommitWithHeight folds into the wrapped error string.
func isMempoolFullErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "mempool is full")
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

	agentID := middleware.ContextAgentID(r.Context())
	if agentID == "" || s.agentStore == nil {
		writeProblem(w, http.StatusForbidden, "Active agent required", "A registered active agent identity is required.")
		return
	}
	agent, err := s.agentStore.GetAgent(r.Context(), agentID)
	if err != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil {
		writeProblem(w, http.StatusForbidden, "Active agent required", "A registered active agent identity is required.")
		return
	}
	rec, err := s.store.GetMemory(r.Context(), memoryID)
	if err != nil || rec == nil || rec.MemoryType != memory.TypeTask {
		writeProblem(w, http.StatusNotFound, "Task not found", "No task was found with that ID.")
		return
	}
	if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, rec.DomainTag, "read"); accessErr != nil {
		writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
		return
	}
	if reader, ok := s.store.(interface {
		GetMemoryClassificationLocal(context.Context, string) (int, error)
	}); ok {
		classification, classErr := reader.GetMemoryClassificationLocal(r.Context(), memoryID)
		if classErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "Task authorization could not be verified; retry later.")
			return
		}
		if classification > 0 {
			if s.badgerStore == nil {
				writeProblem(w, http.StatusForbidden, "Access denied", "No verified read access to this classified task.")
				return
			}
			allowed, accessErr := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, agentID, uint8(classification), time.Now(), s.isPostV8Fork())
			if accessErr != nil || !allowed {
				writeProblem(w, http.StatusForbidden, "Access denied", "No verified read access to this classified task.")
				return
			}
		}
	} else {
		writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "This datastore cannot verify task classification for an agent status change.")
		return
	}

	var changed bool
	switch ts {
	case memory.TaskStatusInProgress:
		changed, err = s.store.ClaimTask(r.Context(), memoryID, agentID)
	case memory.TaskStatusDone, memory.TaskStatusDropped:
		assignmentStore, ok := s.store.(store.TaskAssignmentStore)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Task completion unavailable", "This datastore does not support agent-owned task completion.")
			return
		}
		changed, err = assignmentStore.CompleteTaskAsAgent(r.Context(), memoryID, agentID, ts)
	case memory.TaskStatusPlanned:
		writeProblem(w, http.StatusForbidden, "Operator action required", "Only the local CEREBRUM task board can reopen or re-plan work.")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Task update failed", err.Error())
		return
	}
	if !changed {
		writeProblem(w, http.StatusConflict, "Task update conflict", "The task is terminal or owned by another agent.")
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

	agentID := middleware.ContextAgentID(r.Context())

	// Per-domain read ACL — parity with /v1/memory/list and the query/hybrid
	// recall path. This handler returns full task CONTENT, so without the gate an
	// agent could enumerate the content of every open task in a domain it has no
	// read grant on — and, because `domain` is optional, across ALL domains at
	// once. When a specific domain is requested we 403 up front; the per-record
	// filter below covers the cross-domain (no-domain) case. checkDomainAccess is
	// the operative per-domain gate; the multi-org block below mirrors query/list
	// for shape-parity (a no-op once checkDomainAccess approves a concrete domain).
	domainAccessApproved := false
	if domain != "" {
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, domain, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	if domain != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domErr := s.badgerStore.GetDomainOwner(domain)
		if domErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(domain, agentID, 0, time.Now(), s.isPostV8Fork())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domain))
				return
			}
		}
	}

	tasks, err := s.store.GetOpenTasks(r.Context(), domain, provider, agentID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Failed to get tasks", err.Error())
		return
	}

	// Per-record gate — parity with handleListMemoriesAuth. Drops tasks whose
	// domain the caller cannot read (covers the no-domain cross-domain board) and
	// tasks classified above the caller's clearance. The submitter always sees its
	// own tasks. Agent-isolation is intentionally NOT applied here: within a
	// domain the caller can read, an open-task board is meant to be cross-agent.
	if s.badgerStore != nil {
		now := time.Now()
		kept := tasks[:0]
		for _, rec := range tasks {
			if rec.SubmittingAgent != agentID {
				// Domain-read filter — only needed when no single domain was pre-gated.
				if domain == "" && rec.DomainTag != "" {
					if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, rec.DomainTag, "read"); accessErr != nil {
						continue
					}
				}
				// Classification filter.
				if rec.DomainTag != "" {
					memClass, _ := s.badgerStore.GetMemoryClassification(rec.MemoryID)
					if memClass > 0 {
						domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
						if domErr == nil && domainOwner != "" {
							hasAccess, _ := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, agentID, memClass, now, s.isPostV8Fork())
							if !hasAccess {
								continue
							}
						}
					}
				}
			}
			kept = append(kept, rec)
		}
		tasks = kept
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
	domainFilter := q.Get("domain")

	// Per-domain read ACL — parity with /v1/memory/query, /search and /hybrid.
	// Until this gate was added, list skipped checkDomainAccess entirely, so an
	// agent with no read grant on a domain could enumerate that domain's record
	// CONTENT via list even though hybrid/query correctly 403 — a §5
	// compartmentation hole reported against a multi-node deployment on v10.1.0.
	// Gating here, before the store query, closes it for EVERY status (not just
	// committed). Mirrors handleQueryMemory's two-step domain gate.
	domainAccessApproved := false
	if domainFilter != "" {
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, domainFilter, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	// Multi-org access control gate — only enforce when the domain has a
	// registered owner AND the agent wasn't already approved above.
	if domainFilter != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(domainFilter)
		if domainErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(domainFilter, agentID, 0, time.Now(), s.isPostV8Fork())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domainFilter))
				return
			}
		}
	}

	allowedAgents, seeAll := s.resolveVisibleAgents(agentID)

	// Grant-aware override: if listing a specific domain, skip agent isolation when:
	// (a) the agent has a direct grant on the domain, or
	// (b) the agent has org-level access (clearance >= classification), or
	// (c) the domain has no registered owner (no access policy = open visibility)
	if !seeAll && domainFilter != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(domainFilter, agentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.badgerStore.HasAccessMultiOrg(domainFilter, agentID, 0, time.Now(), s.isPostV8Fork())
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
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}

	records, total, err := s.store.ListMemories(r.Context(), opts)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Query error", err.Error())
		return
	}

	// Per-record gate — parity with the query/hybrid recall path. The up-front
	// domain gate only runs when a domain filter is supplied; on the no-domain
	// path the store returns records across ALL domains, so we additionally drop
	// records whose domain the caller cannot read (a seeAll caller — visible_agents
	// "*", TopSecret, operator — otherwise gets PUBLIC content from domains it has
	// no grant on). The classification gate then drops records classified above the
	// caller's clearance even within a readable domain. The submitter always sees
	// its own records.
	hiddenByClassification := 0
	if s.badgerStore != nil {
		now := time.Now()
		kept := records[:0]
		for _, rec := range records {
			if rec.SubmittingAgent != agentID {
				// Domain-read filter — only needed when no single domain was pre-gated.
				if domainFilter == "" && rec.DomainTag != "" {
					if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, rec.DomainTag, "read"); accessErr != nil {
						continue
					}
				}
				// Classification filter.
				memClass, _ := s.badgerStore.GetMemoryClassification(rec.MemoryID)
				if memClass > 0 {
					domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
					if domErr == nil && domainOwner != "" {
						hasAccess, _ := s.badgerStore.HasAccessMultiOrg(rec.DomainTag, agentID, memClass, now, s.isPostV8Fork())
						if !hasAccess {
							hiddenByClassification++
							continue
						}
					}
				}
			}
			kept = append(kept, rec)
		}
		records = kept
	}

	body := map[string]any{
		"memories": records,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	}

	if filterApplied || hiddenByClassification > 0 {
		applied := make([]string, 0, 2)
		info := &FilterInfo{}
		if filterApplied {
			// Second count-only query without the SubmittingAgents filter so the
			// caller can distinguish empty-domain from rbac-hidden. Limit:1 keeps
			// row materialization bounded; store.ListMemories computes total anyway.
			unfilteredOpts := opts
			unfilteredOpts.SubmittingAgents = nil
			unfilteredOpts.Limit = 1
			unfilteredOpts.Offset = 0
			_, totalBefore, countErr := s.store.ListMemories(r.Context(), unfilteredOpts)
			applied = append(applied, filterBySubmittingAgts)
			visible := total
			info.Visible = &visible
			if countErr == nil {
				info.TotalBeforeFilter = &totalBefore
			}
		}
		if hiddenByClassification > 0 {
			applied = append(applied, filterByClassification)
			hc := hiddenByClassification
			info.HiddenCount = &hc
		}
		info.By = applied
		w.Header().Set(filterHeader, strings.Join(applied, ","))
		body["filtered"] = info
	}

	writeJSON(w, http.StatusOK, body)
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

	// Per-domain read ACL — parity with the other domain-keyed reads. The buckets
	// are aggregate counts (no content), but submission volume over time for a
	// domain is still a metadata signal the caller should hold a read grant for.
	// Global (no-domain) counts stay ungated. checkDomainAccess is the operative
	// per-domain gate; the multi-org block below mirrors query/list for shape-parity
	// (a no-op once checkDomainAccess approves a concrete domain).
	agentID := middleware.ContextAgentID(r.Context())
	domainAccessApproved := false
	if domain != "" {
		if accessErr := checkDomainAccess(r.Context(), s.agentStore, s.badgerStore, agentID, domain, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	if domain != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domErr := s.badgerStore.GetDomainOwner(domain)
		if domErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.badgerStore.HasAccessMultiOrg(domain, agentID, 0, time.Now(), s.isPostV8Fork())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domain))
				return
			}
		}
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

// runHybridWithExpansions runs SearchHybrid once for the primary query plus
// once per expansion variant, then fuses the variant rankings into a single
// list via RRF across variants. With no expansions the call collapses to a
// single SearchHybrid invocation (zero overhead vs the v7.0 path).
func (s *Server) runHybridWithExpansions(ctx context.Context, req HybridSearchMemoryRequest, opts store.QueryOptions) ([]*memory.MemoryRecord, error) {
	// Primary query always runs first; expansions follow. Each variant gets
	// the same QueryOptions (domain, RBAC, filters) so the only thing that
	// varies between calls is the query text + its embedding.
	type variant struct {
		query     string
		embedding []float32
	}
	variants := make([]variant, 0, 1+len(req.Expansions))
	variants = append(variants, variant{query: req.Query, embedding: req.Embedding})
	for _, e := range req.Expansions {
		// Skip empties so callers can pass best-effort expansion lists
		// without us amplifying the no-op into wasted SearchHybrid work.
		if e.Query == "" && len(e.Embedding) == 0 {
			continue
		}
		variants = append(variants, variant{query: e.Query, embedding: e.Embedding})
	}

	// Fast path: one variant means the legacy single-call behaviour applies.
	if len(variants) == 1 {
		return s.store.SearchHybrid(ctx, variants[0].query, variants[0].embedding, opts)
	}

	// Multi-variant path: each call gets its own ranked list. We RRF-merge
	// across variants so a memory that ranks high under several paraphrases
	// outscores one that only matched a single variant. Keeps the constant
	// in lockstep with internal/store/hybrid.go's RRF_K so the rank-fusion
	// shape is consistent end-to-end.
	const rrfKAcrossVariants = 60

	scores := make(map[string]float64)
	records := make(map[string]*memory.MemoryRecord)

	for _, v := range variants {
		got, err := s.store.SearchHybrid(ctx, v.query, v.embedding, opts)
		if err != nil {
			// Under a decayed floor, fail closed: a swallowed leaf error would
			// silently return a partial, floor-unenforced recall.
			if opts.DecayFloor > 0 {
				return nil, err
			}
			// Otherwise a single variant failing shouldn't drop the whole recall —
			// the user paid for an expansion, not a fragility tax. Log and continue
			// with the variants that did succeed.
			s.logger.Warn().Err(err).Str("variant_query", v.query).Msg("expansion variant SearchHybrid failed; skipping")
			continue
		}
		for rank, rec := range got {
			scores[rec.MemoryID] += 1.0 / float64(rrfKAcrossVariants+rank+1)
			if _, ok := records[rec.MemoryID]; !ok {
				records[rec.MemoryID] = rec
			}
		}
	}

	if len(records) == 0 {
		return nil, nil
	}

	type ranked struct {
		mem   *memory.MemoryRecord
		score float64
	}
	out := make([]ranked, 0, len(records))
	for id, rec := range records {
		out = append(out, ranked{mem: rec, score: scores[id]})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].score > out[j].score })

	topK := opts.TopK
	if topK <= 0 {
		topK = 10
	}
	// Cap topK to prevent uncontrolled allocation on attacker-supplied
	// JSON like {"top_k": 2147483647}. Anything above 1k is unreasonable
	// for a single search response.
	const maxTopK = 1000
	if topK > maxTopK {
		topK = maxTopK
	}
	if topK > len(out) {
		topK = len(out)
	}
	final := make([]*memory.MemoryRecord, topK)
	for i := 0; i < topK; i++ {
		final[i] = out[i].mem
	}
	return final, nil
}
