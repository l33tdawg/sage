package rest

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/go-chi/chi/v5"
	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
)

func (s *Server) inboxProvider(r *http.Request) string {
	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), middleware.ContextAgentID(r.Context())); err == nil && agent != nil {
			return agent.Provider
		}
	}
	return ""
}

func (s *Server) inboxReceiptVersion(r *http.Request, m *store.PipelineMessage) (int, error) {
	if m.SourceChainID == "" {
		return 0, nil
	}
	receipts, ok := s.store.(federatedPipeReceiptInboundStore)
	if !ok {
		return 0, errors.New("receipt inspection unavailable")
	}
	_, err := receipts.GetFederatedReceiptInbound(r.Context(), m.PipeID)
	if errors.Is(err, store.ErrFederatedReceiptNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return federation.PipeReceiptVersion, nil
}

func (s *Server) inboxMessageResponse(r *http.Request, m *store.PipelineMessage) (pipelineMessageRESTResponse, error) {
	version, err := s.inboxReceiptVersion(r, m)
	if err != nil {
		return pipelineMessageRESTResponse{}, err
	}
	presentations := s.resolvePipelineAgentPresentations(r.Context(), pipelineMessageAgentIDs([]*store.PipelineMessage{m})...)
	response := enrichPipelineMessageREST(pipelineMessageREST(m, "inbox"), presentations)
	response.ReceiptProtocolVersion = version
	return response, nil
}

// handleMessageInbox reads pending exact, provider-routed and imported requests.
// It never claims, binds a session, acknowledges a read, or emits a receipt.
func (s *Server) handleMessageInbox(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	s.messageInbox(w, r)
}

func (s *Server) messageInbox(w http.ResponseWriter, r *http.Request) {
	limit := 5
	if raw := r.URL.Query().Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 1 || value > 20 {
			writeProblem(w, 400, "Invalid limit", "limit must be between 1 and 20")
			return
		}
		limit = value
	}
	cursor := r.URL.Query().Get("cursor")
	if _, _, err := store.DecodeInboxCursor(cursor); err != nil {
		writeProblem(w, 400, "Invalid cursor", "Use the exact next_cursor from the previous inbox page.")
		return
	}
	inbox, ok := s.store.(store.MessageInboxStore)
	if !ok {
		writeProblem(w, 501, "Inbox unavailable", "The store does not support passive message inspection.")
		return
	}
	items, next, err := inbox.GetPendingInboxPage(r.Context(), middleware.ContextAgentID(r.Context()), s.inboxProvider(r), limit, cursor)
	if err != nil {
		writeProblem(w, 503, "Inbox unavailable", "Message inspection failed; this is not an empty inbox.")
		return
	}
	responses := make([]pipelineMessageRESTResponse, 0, len(items))
	for _, m := range items {
		var snapshot pipelineMessageRESTResponse
		var snapshotErr error
		read := func() error {
			snapshot, snapshotErr = s.inboxMessageResponse(r, m)
			return snapshotErr
		}
		if m.SourceChainID != "" {
			authorizer, ok := s.federation.(federatedPipeAdmissionAuthorizer)
			if !ok {
				writeProblem(w, 503, "Foreign inbox unavailable", "Import authorization could not be checked.")
				return
			}
			if err := authorizer.WithAuthorizedImportedPipe(r.Context(), m, read); err != nil {
				if snapshotErr != nil {
					writeProblem(w, 503, "Receipt state unavailable", "Foreign inspection could not determine its receipt protocol.")
					return
				}
				continue
			}
		} else if err := read(); err != nil {
			writeProblem(w, 503, "Inbox unavailable", "Message inspection failed.")
			return
		}
		responses = append(responses, snapshot)
	}
	response := map[string]any{"items": responses, "count": len(responses), "passive": true, "has_more": next != "", "limit": limit}
	if next != "" {
		response["next_cursor"] = next
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, response)
}

func (s *Server) handleMessageInspect(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	inbox, ok := s.store.(store.MessageInboxStore)
	if !ok {
		writeProblem(w, 501, "Inbox unavailable", "The store does not support message inspection.")
		return
	}
	sessionID := r.URL.Query().Get("claimant_session_id")
	if sessionID == "" || len(sessionID) > store.MaxMessageClaimantSessionBytes {
		writeProblem(w, 400, "Invalid claimant session", "A bounded claimant_session_id is required.")
		return
	}
	id := chi.URLParam(r, "message_id")
	m, err := inbox.InspectClaimableMessage(r.Context(), middleware.ContextAgentID(r.Context()), s.inboxProvider(r), id, sessionID)
	if err != nil {
		s.writeMessageClaimError(w, id, err)
		return
	}
	if m.SourceChainID != "" {
		authorizer, ok := s.federation.(federatedPipeAdmissionAuthorizer)
		if !ok || authorizer.WithAuthorizedImportedPipe(r.Context(), m, func() error { return nil }) != nil {
			writeCanonicalMessageNotFound(w, id)
			return
		}
	}
	snapshot, snapshotErr := s.inboxMessageResponse(r, m)
	if snapshotErr != nil {
		writeProblem(w, 503, "Receipt state unavailable", "Message inspection could not determine its receipt protocol.")
		return
	}
	response := map[string]any{"item": snapshot, "passive": true}
	if snapshot.ReceiptProtocolVersion == 2 {
		receipt, ok := s.federation.(federatedPipeReceiptController)
		if !ok {
			writeProblem(w, 501, "Claim receipts unavailable", "The node cannot prepare a receipt-v2 claim.")
			return
		}
		challenge, err := receipt.ImportedPipeReceiptChallenge(r.Context(), id, middleware.ContextAgentID(r.Context()), "claimed")
		if err != nil {
			writeCanonicalMessageNotFound(w, id)
			return
		}
		response["claim_receipt_challenge"] = challenge
	}
	w.Header().Set("Cache-Control", "no-store")
	writeJSON(w, 200, response)
}

// handleMessageClaim is an explicit exact mutation. Receipt-v2 claims commit
// ownership and the peer-verifiable receipt outbox in the same transaction.
func (s *Server) handleMessageClaim(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	var req struct {
		ClaimantSessionID string                    `json:"claimant_session_id"`
		ClaimProof        *store.PipelineAgentProof `json:"claim_proof,omitempty"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, 400, "Invalid request body", err.Error())
		return
	}
	req.ClaimantSessionID = strings.TrimSpace(req.ClaimantSessionID)
	if req.ClaimantSessionID == "" || len(req.ClaimantSessionID) > store.MaxMessageClaimantSessionBytes {
		writeProblem(w, 400, "Invalid claimant session", "A bounded claimant_session_id is required.")
		return
	}
	inbox, ok := s.store.(store.MessageInboxStore)
	if !ok {
		writeProblem(w, 501, "Claim unavailable", "The store does not support exact message claims.")
		return
	}
	id, agentID := chi.URLParam(r, "message_id"), middleware.ContextAgentID(r.Context())
	provider := s.inboxProvider(r)
	m, err := inbox.InspectClaimableMessage(r.Context(), agentID, provider, id, req.ClaimantSessionID)
	if err != nil {
		s.writeMessageClaimError(w, id, err)
		return
	}
	replayed := false
	claim := func() error {
		var claimErr error
		m, replayed, claimErr = inbox.ClaimInboxMessage(r.Context(), agentID, provider, id, req.ClaimantSessionID)
		return claimErr
	}
	if m.SourceChainID != "" {
		version, versionErr := s.inboxReceiptVersion(r, m)
		if versionErr != nil {
			writeProblem(w, 503, "Receipt state unavailable", "The request was not claimed because its receipt protocol could not be checked.")
			return
		}
		negotiated := version == 2
		if negotiated {
			controller, ok := s.federation.(federatedPipeReceiptController)
			if !ok || req.ClaimProof == nil {
				writeProblem(w, 400, "Claim proof required", "Sign the claim_receipt_challenge returned by message inspection.")
				return
			}
			replayed, err = controller.RecordImportedPipeReceipt(r.Context(), id, agentID, "claimed", *req.ClaimProof, req.ClaimantSessionID)
			if err == nil {
				m, err = inbox.InspectClaimableMessage(r.Context(), agentID, provider, id, req.ClaimantSessionID)
			}
		} else if authorizer, ok := s.federation.(federatedPipeAdmissionAuthorizer); ok {
			err = authorizer.WithAuthorizedImportedPipe(r.Context(), m, claim)
		} else {
			err = store.ErrMessageNotFound
		}
	} else {
		err = claim()
	}
	if err != nil {
		s.writeMessageClaimError(w, id, err)
		return
	}
	snapshot, snapshotErr := s.inboxMessageResponse(r, m)
	if snapshotErr != nil {
		writeProblem(w, 503, "Claim response unavailable", "Reconcile this exact claim with passive history before retrying.")
		return
	}
	writeJSON(w, 200, map[string]any{"item": snapshot, "message_id": id, "status": "claimed",
		"claimant_session_id": req.ClaimantSessionID, "claim_revision": m.ClaimRevision, "idempotent_replay": replayed})
}

func (s *Server) writeMessageClaimError(w http.ResponseWriter, id string, err error) {
	if errors.Is(err, store.ErrMessageClaimedByOtherSession) || errors.Is(err, store.ErrFederatedReceiptConflict) {
		writeProblemTyped(w, 409, messageClaimSessionProblemType, "Claimed by another session", "Inspect claimed-elsewhere state; ownership requires an explicit revision-fenced handoff.")
		return
	}
	writeCanonicalMessageNotFound(w, id)
}
