package rest

// Recall outcome feedback: POST /v1/memory/{memory_id}/outcome.
//
// Corroboration counts agents that agree with a memory. It says nothing about
// whether recalling the memory actually helped anyone. An agent that uses
// SAGE as experiential memory reports that here: helped, hurt, or unused.
//
// The counts are node-local (internal/store/memory_outcome.go): never in a
// transaction, the AppHash, or a content hash. Recall always reports them; only
// when an operator opts in (SAGE_RECALL_OUTCOME_RANKING=on) does it also move a
// memory a few places within the page it already returns, so feedback can
// neither bury a memory nor pull one in from outside the page.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	// outcomeMinInterval is the shortest gap between two reports from the same
	// agent for the same memory.
	outcomeMinInterval = 10 * time.Second
	// outcomePerAgentCap caps each agent's contribution per outcome kind when
	// counts are aggregated, so one agent cannot outweigh several.
	outcomePerAgentCap = 5
	// outcomeMaxShift is the most places feedback alone moves a memory.
	outcomeMaxShift = 3
	// outcomePrior shrinks small samples toward zero strength.
	outcomePrior = 2.0
	// outcomeUnusedWeight is how much one "unused" report dilutes strength.
	outcomeUnusedWeight = 0.5
)

// MemoryOutcomeRequest is the JSON body for POST /v1/memory/{memory_id}/outcome.
type MemoryOutcomeRequest struct {
	Outcome string `json:"outcome"`
	// SourceChainID, when set, marks a federated result. Outcomes are recorded
	// only for memories stored on this node, so any non-empty value is refused.
	SourceChainID string `json:"source_chain_id,omitempty"`
}

// MemoryOutcomeCounts is the read-only aggregate feedback on a recall result.
// Each count sums the reporting agents' counts, each capped per agent.
type MemoryOutcomeCounts struct {
	Helped int `json:"helped"`
	Hurt   int `json:"hurt"`
	Unused int `json:"unused"`
	Agents int `json:"agents"`
}

// MemoryOutcomeResponse is returned after an outcome is recorded.
type MemoryOutcomeResponse struct {
	MemoryID string              `json:"memory_id"`
	Outcome  string              `json:"outcome"`
	Outcomes MemoryOutcomeCounts `json:"outcomes"`
}

// outcomeRankingEnabled reports whether feedback reorders recall results.
// Reordering is opt-in: only SAGE_RECALL_OUTCOME_RANKING=on (or true/1/yes,
// case-insensitive) enables it. Outcomes are always recorded and reported on
// results either way.
func outcomeRankingEnabled() bool {
	switch strings.ToLower(strings.TrimSpace(os.Getenv("SAGE_RECALL_OUTCOME_RANKING"))) {
	case "on", "true", "1", "yes":
		return true
	}
	return false
}

// outcomeStrength maps aggregate feedback to a strength in (-1, 1). Hurt
// cancels helped, unused reports and a small prior pull it toward zero.
func outcomeStrength(st store.MemoryOutcomeStats) float64 {
	h, x, u := float64(st.Helped), float64(st.Hurt), float64(st.Unused)
	return (h - x) / (h + x + outcomeUnusedWeight*u + outcomePrior)
}

// outcomeShift is how many places a memory moves up (positive) or down.
func outcomeShift(st store.MemoryOutcomeStats) int {
	shift := int(math.Round(outcomeStrength(st) * outcomeMaxShift))
	if shift > outcomeMaxShift {
		return outcomeMaxShift
	}
	if shift < -outcomeMaxShift {
		return -outcomeMaxShift
	}
	return shift
}

func outcomeCounts(st store.MemoryOutcomeStats) *MemoryOutcomeCounts {
	return &MemoryOutcomeCounts{Helped: st.Helped, Hurt: st.Hurt, Unused: st.Unused, Agents: st.Agents}
}

// applyRecallOutcomes attaches outcome counts to local recall results and,
// when ranking is opted in, reorders them by a bounded per-memory shift. Results must
// be in rank order. A store without outcome support, or a read error, leaves
// the results unchanged.
func (s *Server) applyRecallOutcomes(ctx context.Context, results []*MemoryResult) []*MemoryResult {
	outcomeStore, ok := s.store.(store.MemoryOutcomeStore)
	if !ok || len(results) == 0 {
		return results
	}
	ids := make([]string, 0, len(results))
	for _, res := range results {
		if res.SourceChainID == "" {
			ids = append(ids, res.MemoryID)
		}
	}
	stats, err := outcomeStore.GetMemoryOutcomeStats(ctx, ids, outcomePerAgentCap)
	if err != nil {
		s.logger.Debug().Err(err).Msg("recall outcome stats unavailable; ranking unchanged")
		return results
	}
	if len(stats) == 0 {
		return results
	}
	// Each result keeps its position as its key; a shifted result's key moves
	// by its shift plus half a place in the same direction, so a lone shifted
	// memory moves exactly |shift| places and ties never depend on sort order.
	type ranked struct {
		res *MemoryResult
		key float64
		pos int
	}
	order := make([]ranked, len(results))
	for i, res := range results {
		key := float64(i)
		if res.SourceChainID == "" {
			if st, has := stats[res.MemoryID]; has {
				res.Outcomes = outcomeCounts(st)
				if shift := outcomeShift(st); shift > 0 {
					key -= float64(shift) + 0.5
				} else if shift < 0 {
					key -= float64(shift) - 0.5
				}
			}
		}
		order[i] = ranked{res: res, key: key, pos: i}
	}
	if !outcomeRankingEnabled() {
		return results
	}
	sort.SliceStable(order, func(a, b int) bool {
		if order[a].key != order[b].key {
			return order[a].key < order[b].key
		}
		return order[a].pos < order[b].pos
	})
	out := make([]*MemoryResult, len(order))
	for i := range order {
		out[i] = order[i].res
	}
	return out
}

// handleMemoryOutcome handles POST /v1/memory/{memory_id}/outcome.
func (s *Server) handleMemoryOutcome(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}
	var req MemoryOutcomeRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	switch req.Outcome {
	case store.MemoryOutcomeHelped, store.MemoryOutcomeHurt, store.MemoryOutcomeUnused:
	default:
		writeProblem(w, http.StatusBadRequest, "Invalid outcome",
			`outcome must be one of "helped", "hurt" or "unused".`)
		return
	}
	if req.SourceChainID != "" {
		writeProblem(w, http.StatusBadRequest, "Federated memory",
			"Outcomes are recorded only for memories stored on this node; a federated result cannot receive one here.")
		return
	}
	agentID := middleware.ContextAgentID(r.Context())
	if agentID == "" {
		writeProblem(w, http.StatusUnauthorized, "Agent required", "An authenticated agent is required to record an outcome.")
		return
	}
	outcomeStore, ok := s.store.(store.MemoryOutcomeStore)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Outcome feedback unavailable",
			"The active store does not support recall outcome feedback.")
		return
	}
	rec, err := s.store.GetMemory(r.Context(), memoryID)
	if err != nil || rec == nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s on this node. Federated results cannot receive outcomes here.", memoryID))
		return
	}
	if lookup, has := s.store.(syncOriginLookup); has {
		origin, originErr := lookup.GetSyncOriginByLocalMemoryID(r.Context(), memoryID)
		switch {
		case originErr == nil && origin != nil:
			writeProblem(w, http.StatusConflict, "Federated memory",
				"This memory is a retained copy from another chain; outcomes are recorded only for memories that originate on this node.")
			return
		case originErr != nil && !errors.Is(originErr, sql.ErrNoRows):
			writeProblem(w, http.StatusServiceUnavailable, "Provenance unavailable",
				"Memory provenance is unavailable; retry later.")
			return
		}
	}
	if _, readable := s.authorizeMemoryRead(w, r, agentID, rec); !readable {
		return
	}
	if err := outcomeStore.RecordMemoryOutcome(r.Context(), memoryID, agentID, req.Outcome, time.Now(), outcomeMinInterval); err != nil {
		if errors.Is(err, store.ErrMemoryOutcomeRateLimited) {
			w.Header().Set("Retry-After", strconv.Itoa(int(outcomeMinInterval/time.Second)))
			writeProblem(w, http.StatusTooManyRequests, "Outcome reported too recently",
				fmt.Sprintf("One outcome per memory per agent every %s.", outcomeMinInterval))
			return
		}
		s.logger.Error().Err(err).Msg("failed to record memory outcome")
		writeProblem(w, http.StatusInternalServerError, "Outcome error", "Failed to record the outcome.")
		return
	}
	resp := MemoryOutcomeResponse{MemoryID: memoryID, Outcome: req.Outcome}
	if stats, statsErr := outcomeStore.GetMemoryOutcomeStats(r.Context(), []string{memoryID}, outcomePerAgentCap); statsErr == nil {
		if st, has := stats[memoryID]; has {
			resp.Outcomes = *outcomeCounts(st)
		}
	}
	writeJSON(w, http.StatusOK, resp)
}
