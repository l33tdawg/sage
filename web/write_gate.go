package web

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/voter"
)

// Operator review queue for the optional memory gate (internal/voter.Gate).
// When the judges are uncertain about a proposed memory the node does not vote
// on it; the memory waits here until the operator accepts or rejects it, and
// the voter then applies that decision — together with fresh built-in checks —
// on its next tick. Operator-only: the queue shows memory content.
//
// Reads go through the same projection-integrity path as the dashboard's
// other broad memory reads: the broad-read gate on the route, the sealed
// source/retry loop, internal-domain hiding, and the batch classifier that
// omits quarantined rows. Content that cannot be produced in plaintext is
// reported as unavailable and never passed to the classifier.

type memoryGateReviewStore interface {
	ReviewQueue(ctx context.Context, version string, after store.ReviewQueueCursor, limit int) ([]store.HeldForReview, error)
	ReviewMemory(ctx context.Context, memoryID string) (*memory.MemoryRecord, bool, error)
	GateVerdicts(ctx context.Context, memoryID string) ([]store.GateVerdict, error)
	SetReviewDecision(ctx context.Context, memoryID, version, decision, decidedBy, note string) error
}

// SetMemoryGate tells the dashboard which gate the node runs (nil = off).
func (h *DashboardHandler) SetMemoryGate(g *voter.Gate) { h.memoryGate.Store(g) }

// memoryGateStatus is what the dashboard reports about the gate.
func (h *DashboardHandler) memoryGateStatus() map[string]any {
	g := h.memoryGate.Load()
	if g == nil {
		return map[string]any{"enabled": false}
	}
	return map[string]any{
		"enabled":         true,
		"judge_version":   g.Version,
		"judges":          len(g.Judges),
		"include_domains": nonNilStrings(g.IncludeDomainPrefixes),
		"exempt_domains":  nonNilStrings(g.ExemptDomainPrefixes),
	}
}

func nonNilStrings(s []string) []string {
	if s == nil {
		return []string{}
	}
	return s
}

type reviewQueueItem struct {
	MemoryID           string    `json:"memory_id"`
	DomainTag          string    `json:"domain_tag,omitempty"`
	MemoryType         string    `json:"memory_type,omitempty"`
	Content            string    `json:"content,omitempty"`
	ContentUnavailable bool      `json:"content_unavailable,omitempty"`
	Reason             string    `json:"reason"`
	P                  float64   `json:"p_yes"`
	HeldAt             time.Time `json:"held_at"`
}

// handleReviewQueue: GET /v1/dashboard/memory/review-queue?limit=N
func (h *DashboardHandler) handleReviewQueue(w http.ResponseWriter, r *http.Request) {
	g := h.memoryGate.Load()
	if g == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"gate": h.memoryGateStatus(), "items": []reviewQueueItem{}, "count": 0,
		})
		return
	}
	rs, ok := h.store.(memoryGateReviewStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this node's store does not support the memory gate")
		return
	}
	if !h.appV23IsActive() {
		h.handleReviewQueueUnsealed(w, r, g, rs)
		return
	}
	for attempt := 0; attempt < 3; attempt++ {
		source, cacheable, err := h.appV23GraphSource(r.Context())
		if err != nil {
			if writeAppV23DashboardProjectionFailure(w, err) {
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		if !cacheable {
			writeAppV23DashboardProjectionFailure(w, appV23DashboardProjectionError(
				errors.New("review queue source revisions are unavailable")))
			return
		}
		sealed := newSealedDashboardResponse()
		h.handleReviewQueueUnsealed(sealed, r, g, rs)
		current, sealErr := h.sealAppV23DashboardSource(r.Context(), source)
		if sealErr != nil {
			if writeAppV23DashboardProjectionFailure(w, sealErr) {
				return
			}
			writeError(w, http.StatusInternalServerError, sealErr.Error())
			return
		}
		if !current {
			continue
		}
		writeSealedDashboardResponse(w, sealed)
		return
	}
	writeError(w, http.StatusServiceUnavailable, "review queue changed while it was being prepared; retry")
}

// reviewQueueRawPage and reviewQueueScanBudget bound one request's walk over
// raw held rows, following the dashboard's interactive scan budget.
const (
	reviewQueueRawPage     = 256
	reviewQueueScanBudget  = appV23CerebrumInteractiveScanBudget
	reviewQueueMaxVisible  = 200
	reviewQueueDefaultSize = 50
)

// handleReviewQueueUnsealed walks raw held rows until the requested number of
// VISIBLE items is collected, the rows run out, or the scan budget is spent.
// Internal-domain and quarantined rows are skipped without consuming the
// visible page; a continuation cursor (the last scanned row) is returned when rows
// remain, so a long hidden prefix can never make visible held memories
// unreachable.
func (h *DashboardHandler) handleReviewQueueUnsealed(w http.ResponseWriter, r *http.Request, g *voter.Gate, rs memoryGateReviewStore) {
	ctx := r.Context()
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > reviewQueueMaxVisible {
		limit = reviewQueueDefaultSize
	}
	var after store.ReviewQueueCursor
	if raw := r.URL.Query().Get("cursor"); raw != "" {
		if len(raw) > 4096 {
			writeError(w, http.StatusBadRequest, "invalid review queue cursor")
			return
		}
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil || json.Unmarshal(decoded, &after) != nil || after.CreatedAt == "" || after.MemoryID == "" {
			writeError(w, http.StatusBadRequest, "invalid review queue cursor")
			return
		}
		if after.JudgeVersion != g.Version {
			writeError(w, http.StatusConflict, "the memory gate changed; refresh the review queue")
			return
		}
	}
	scanned := 0
	// Capacity from a constant, never from the request (limit is clamped above,
	// but allocation size must not depend on user input).
	items := make([]reviewQueueItem, 0, reviewQueueDefaultSize)
	exhausted := false
	observedHidden := 0
	for len(items) < limit && scanned < reviewQueueScanBudget {
		batch := min(reviewQueueRawPage, reviewQueueScanBudget-scanned)
		held, err := rs.ReviewQueue(ctx, g.Version, after, batch)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		byID := make(map[string]store.HeldForReview, len(held))
		readable := make([]*memory.MemoryRecord, 0, len(held))
		unavailable := map[string]bool{}
		for _, it := range held {
			rec, available, rerr := rs.ReviewMemory(ctx, it.MemoryID)
			if errors.Is(rerr, store.ErrMemoryNotFound) {
				observedHidden++
				continue
			}
			if rerr != nil {
				writeError(w, http.StatusInternalServerError, rerr.Error())
				return
			}
			if isCerebrumInternalMemoryDomain(rec.DomainTag) {
				observedHidden++
				continue
			}
			byID[it.MemoryID] = it
			if !available {
				// Never classified: undecryptable content would be misread as a
				// projection defect. Reported without content; it cannot be
				// decided until the content can be read.
				unavailable[it.MemoryID] = true
				continue
			}
			readable = append(readable, rec)
		}
		kept, err := h.filterAppV23BroadDashboardRecords(readable)
		if err != nil {
			if writeAppV23DashboardProjectionFailure(w, err) {
				return
			}
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		observedHidden += len(readable) - len(kept)
		keptByID := make(map[string]*memory.MemoryRecord, len(kept))
		for _, rec := range kept {
			keptByID[rec.MemoryID] = rec
		}
		consumed := 0
		for i, it := range held {
			consumed = i + 1
			after = it.Cursor
			id := it.MemoryID
			meta := byID[id]
			switch {
			case unavailable[id]:
				items = append(items, reviewQueueItem{MemoryID: id, ContentUnavailable: true,
					Reason: meta.Reason, P: meta.P, HeldAt: meta.HeldAt})
			case keptByID[id] != nil:
				rec := keptByID[id]
				items = append(items, reviewQueueItem{MemoryID: id, DomainTag: rec.DomainTag,
					MemoryType: string(rec.MemoryType), Content: rec.Content,
					Reason: meta.Reason, P: meta.P, HeldAt: meta.HeldAt})
			}
			if len(items) == limit {
				break
			}
		}
		scanned += consumed
		if len(held) < batch {
			exhausted = consumed == len(held)
			break
		}
	}
	response := map[string]any{"gate": h.memoryGateStatus(), "items": items, "count": len(items)}
	if !exhausted {
		cursorJSON, _ := json.Marshal(after)
		response["next_cursor"] = base64.RawURLEncoding.EncodeToString(cursorJSON)
	}
	if observedHidden > 0 {
		response["hidden"] = observedHidden
	}
	if projection := h.appV23ProjectionResponseForContext(ctx); projection != nil {
		response["projection"] = h.projectionResponseForRequest(r, projection)
	}
	writeJSONResp(w, http.StatusOK, response)
}

// handleMemoryJudgements: GET /v1/dashboard/memory/{id}/judgements — the
// gate's stored verdicts for one memory (no content).
func (h *DashboardHandler) handleMemoryJudgements(w http.ResponseWriter, r *http.Request) {
	rs, ok := h.store.(memoryGateReviewStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this node's store does not support the memory gate")
		return
	}
	vs, err := rs.GateVerdicts(r.Context(), chi.URLParam(r, "id"))
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if vs == nil {
		vs = []store.GateVerdict{}
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"gate": h.memoryGateStatus(), "verdicts": vs})
}

// handleReviewDecision: POST /v1/dashboard/memory/{id}/review
// body {"decision": "accept"|"reject", "note": "..."}
func (h *DashboardHandler) handleReviewDecision(w http.ResponseWriter, r *http.Request) {
	g := h.memoryGate.Load()
	if g == nil {
		writeError(w, http.StatusConflict, "the memory gate is disabled on this node; nothing is held for review")
		return
	}
	rs, ok := h.store.(memoryGateReviewStore)
	if !ok {
		writeError(w, http.StatusNotImplemented, "this node's store does not support the memory gate")
		return
	}
	var body struct {
		Decision string `json:"decision"`
		Note     string `json:"note"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10)).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	id := chi.URLParam(r, "id")
	// Revalidate the exact target immediately before the decision becomes
	// final: a stale page or an API client must not be able to decide a memory
	// whose content can no longer be read, that is internal, or that fails the
	// projection-integrity check. Disabled buttons are not a control.
	rec, available, rerr := rs.ReviewMemory(r.Context(), id)
	switch {
	case errors.Is(rerr, store.ErrMemoryNotFound):
		writeError(w, http.StatusNotFound, "memory not found")
		return
	case rerr != nil:
		writeError(w, http.StatusInternalServerError, rerr.Error())
		return
	case !available:
		writeError(w, http.StatusConflict,
			"this memory's content cannot be read on this node right now (locked or undecryptable); it cannot be reviewed until it can")
		return
	case isCerebrumInternalMemoryDomain(rec.DomainTag):
		writeError(w, http.StatusConflict, store.ErrNotAwaitingReview.Error())
		return
	}
	if verr := h.validateAppV23DashboardRecord(rec); verr != nil {
		if writeAppV23DashboardProjectionFailure(w, verr) {
			return
		}
		writeError(w, http.StatusConflict, verr.Error())
		return
	}
	err := rs.SetReviewDecision(r.Context(), id, g.Version, body.Decision, "operator", body.Note)
	switch {
	case errors.Is(err, store.ErrNotAwaitingReview):
		writeError(w, http.StatusConflict, err.Error())
	case err != nil:
		writeError(w, http.StatusBadRequest, err.Error())
	default:
		writeJSONResp(w, http.StatusOK, map[string]any{"memory_id": id, "decision": body.Decision,
			"note": "the node applies this decision, with fresh built-in checks, on its next voter tick"})
	}
}
