package rest

import (
	"net/http"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const inboxActivityVersion = 1

// inboxActivityPayload is intentionally payload-free novelty metadata. It is
// separate from messageWakePayload because task notices and passive replies are
// coordination to inspect, not unfinished work that may block a host Stop.
type inboxActivityPayload struct {
	Version int    `json:"version"`
	Epoch   string `json:"epoch"`
	Seq     uint64 `json:"seq"`
}

func (s *Server) handleInboxActivityState(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	activityStore, ok := s.store.(store.InboxActivityStore)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Inbox activity unavailable", "The active store does not support inbox activity state.")
		return
	}
	seq, err := activityStore.GetInboxActivitySequence(r.Context(), middleware.ContextAgentID(r.Context()))
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Inbox activity unavailable", "Inbox activity state is temporarily unavailable.")
		return
	}
	epoch, err := activityStore.GetInboxActivityEpoch(r.Context())
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Inbox activity unavailable", "Inbox activity incarnation is temporarily unavailable.")
		return
	}
	writeJSON(w, http.StatusOK, inboxActivityPayload{Version: inboxActivityVersion, Epoch: epoch, Seq: seq})
}
