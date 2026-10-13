package rest

import (
	"errors"
	"net/http"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

func (s *Server) handleMessageClose(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	var req struct {
		Reason            string `json:"reason"`
		ClaimantSessionID string `json:"claimant_session_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if strings.TrimSpace(req.Reason) == "" || len(req.Reason) > store.MaxMessageCloseReasonBytes ||
		req.ClaimantSessionID == "" || len(req.ClaimantSessionID) > store.MaxMessageClaimantSessionBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid close fields", "A reason (up to 1024 bytes) and claimant_session_id (up to 128 bytes) are required.")
		return
	}
	closer, ok := s.store.(store.MessageCloseStore)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Message closing unavailable", "The active store does not support closing without a reply.")
		return
	}
	messageID := chi.URLParam(r, "message_id")
	replayed, err := closer.CloseMessage(r.Context(), middleware.ContextAgentID(r.Context()), messageID, req.ClaimantSessionID, req.Reason)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrMessageClaimedByOtherSession):
			writeProblemTyped(w, http.StatusConflict, messageClaimSessionProblemType, "Claimed by another session", "Close from the claiming session or explicitly hand off its claim first.")
		case errors.Is(err, store.ErrMessageCloseConflict):
			writeProblem(w, http.StatusConflict, "Close conflict", "This message already has a different terminal outcome.")
		case errors.Is(err, store.ErrMessageNotFound):
			writeCanonicalMessageNotFound(w, messageID)
		default:
			writeProblem(w, http.StatusInternalServerError, "Message close failed", "The message could not be closed.")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message_id": messageID, "status": "completed", "completion_kind": "closed_without_reply",
		"reply_sent": false, "idempotent_replay": replayed,
	})
}
