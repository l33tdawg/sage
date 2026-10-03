package rest

import (
	"context"
	"errors"
	"net/http"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

// Evidence for the optional memory gate travels separately from the memory:
// a signed submission body is copied into the transaction (it is the agent's
// on-chain proof), so evidence placed there would reach the chain. The agent
// uploads evidence to its node first, gets a random id, and submits the memory
// with that id. The chain only ever sees the id.

// memoryEvidenceStore is implemented by stores that keep submission evidence
// for the memory gate (the SQLite store).
type memoryEvidenceStore interface {
	CreateMemoryEvidence(ctx context.Context, agentID, evidence string) (string, error)
	ClaimMemoryEvidence(ctx context.Context, evidenceID, agentID, memoryID string) error
	ReleaseMemoryEvidence(ctx context.Context, evidenceID, memoryID string) error
}

// UploadEvidenceRequest is the JSON body for POST /v1/memory/evidence.
type UploadEvidenceRequest struct {
	Evidence string `json:"evidence"`
}

// UploadEvidenceResponse carries the id a memory submission claims the
// evidence with (evidence_id on POST /v1/memory/submit).
type UploadEvidenceResponse struct {
	EvidenceID       string `json:"evidence_id"`
	ExpiresInSeconds int64  `json:"expires_in_seconds"`
}

// handleUploadEvidence handles POST /v1/memory/evidence. The evidence is kept
// node-local; nothing is broadcast.
func (s *Server) handleUploadEvidence(w http.ResponseWriter, r *http.Request) {
	agentID := middleware.ContextAgentID(r.Context())
	// Key possession is not enrollment: above app-v23 only an active ordinary
	// agent may store evidence, so fresh keys cannot bypass the per-agent cap.
	if !s.requireAppV23ActiveOrdinaryAgent(w, agentID, "memory evidence upload") {
		return
	}
	es, ok := s.store.(memoryEvidenceStore)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Evidence not supported",
			"This node's store does not keep submission evidence.")
		return
	}
	var req UploadEvidenceRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Evidence == "" || len(req.Evidence) > store.MaxEvidenceBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid evidence", "evidence is required and at most 32 KiB.")
		return
	}
	id, err := es.CreateMemoryEvidence(r.Context(), agentID, req.Evidence)
	if errors.Is(err, store.ErrTooMuchUnclaimedEvidence) {
		writeProblem(w, http.StatusTooManyRequests, "Too much unclaimed evidence",
			"Submit memories for the evidence already uploaded, or wait for it to expire.")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Evidence not stored", err.Error())
		return
	}
	writeJSON(w, http.StatusCreated, UploadEvidenceResponse{
		EvidenceID: id, ExpiresInSeconds: int64(store.UnclaimedEvidenceTTL.Seconds()),
	})
}

// claimSubmittedEvidence attaches a submission's evidence to its memory
// immediately before the transaction is signed. It writes the problem response
// and returns false when an evidence id was given but cannot be claimed:
// evidence is refused, never silently dropped.
func (s *Server) claimSubmittedEvidence(w http.ResponseWriter, r *http.Request, evidenceID, agentID, memoryID string) bool {
	if evidenceID == "" {
		return true
	}
	es, ok := s.store.(memoryEvidenceStore)
	if !ok {
		writeProblem(w, http.StatusBadRequest, "Evidence not supported",
			"This node's store does not keep submission evidence; submit without evidence_id.")
		return false
	}
	err := es.ClaimMemoryEvidence(r.Context(), evidenceID, agentID, memoryID)
	if errors.Is(err, store.ErrEvidenceUnavailable) {
		writeProblem(w, http.StatusBadRequest, "Invalid evidence_id", err.Error())
		return false
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Evidence not attached", err.Error())
		return false
	}
	return true
}

// releaseUnsentEvidence returns a claim to its uploader when the submission
// provably never sent a transaction — it failed before the submit stage (a
// fenced or paused signer, signing, encoding) — so the caller can retry with
// the same evidence_id. Any failure AT submit keeps the claim, even one that
// looks definitive: an indeterminate transaction may still commit and its
// memory must keep its evidence. A claim whose memory never appears is
// removed by retention (store.OrphanedEvidenceGrace).
func (s *Server) releaseUnsentEvidence(evidenceID, memoryID string, stage consensusTxStage, err error) {
	if evidenceID == "" || err == nil || stage == consensusTxSubmit {
		return
	}
	es, ok := s.store.(memoryEvidenceStore)
	if !ok {
		return
	}
	if rerr := es.ReleaseMemoryEvidence(context.Background(), evidenceID, memoryID); rerr != nil {
		s.logger.Warn().Err(rerr).Str("memory_id", memoryID).Msg("could not release evidence of an unsent submission")
	}
}
