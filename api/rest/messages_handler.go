package rest

import (
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const messageNotFoundTitle = "Message not found"

func canonicalMessageStore(s *Server) (store.MessageStore, bool) {
	messageStore, ok := s.store.(store.MessageStore)
	return messageStore, ok
}

func writeCanonicalMessageNotFound(w http.ResponseWriter, messageID string) {
	writeProblem(w, http.StatusNotFound, messageNotFoundTitle,
		"No message is available to this caller.")
}

func requireExactSignedMessageAction(w http.ResponseWriter, r *http.Request) bool {
	proof := middleware.ContextAgentAuth(r.Context())
	if proof == nil || len(proof.Signature) == 0 || len(proof.CanonicalRequest) == 0 || len(proof.Nonce) < 8 {
		writeProblem(w, http.StatusForbidden, "Exact agent signature required",
			"This message action requires a fresh nonce-bound request signed by the exact agent credential.")
		return false
	}
	return true
}

// handleMessageSend is the canonical idempotent same-node send operation.
// Federation keeps its existing transport boundary until the receipts
// capability is separately negotiated; this route therefore accepts only an
// exact active local recipient.
func (s *Server) handleMessageSend(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	var req struct {
		ToAgent        string `json:"to_agent"`
		Intent         string `json:"intent"`
		Payload        string `json:"payload"`
		TTLMinutes     *int   `json:"ttl_minutes"`
		IdempotencyKey string `json:"idempotency_key"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	req.ToAgent = strings.TrimSpace(req.ToAgent)
	if req.ToAgent == "" || req.Payload == "" || strings.TrimSpace(req.IdempotencyKey) == "" {
		writeProblem(w, http.StatusBadRequest, "Missing message fields",
			"to_agent, payload, and idempotency_key are required")
		return
	}
	if len(req.IdempotencyKey) > store.MaxMessageTokenBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid idempotency key", "idempotency_key is too long")
		return
	}
	if len(req.Payload) > store.MaxPipeContentBytes || len(req.Intent) > store.MaxPipeIntentBytes {
		writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType,
			"Message content too large", "The message exceeds the configured pipeline content limit.")
		return
	}
	if s.agentStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent directory unavailable", "The local agent directory is unavailable.")
		return
	}
	if _, err := s.agentStore.GetAgent(r.Context(), req.ToAgent); err != nil {
		writeCanonicalMessageNotFound(w, req.ToAgent)
		return
	}
	active, err := s.appV23ActiveOrdinaryAgent(req.ToAgent)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Agent directory unavailable", "Current local enrollment state is unavailable.")
		return
	}
	if !active {
		writeCanonicalMessageNotFound(w, req.ToAgent)
		return
	}
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	senderID := middleware.ContextAgentID(r.Context())
	fromProvider := ""
	if sender, senderErr := s.agentStore.GetAgent(r.Context(), senderID); senderErr == nil && sender != nil {
		fromProvider = sender.Provider
	}
	// Canonical Messages are email-like by default: an offline recipient does
	// not lose unread work. A caller may still opt into a bounded TTL.
	ttl := 0
	if req.TTLMinutes != nil {
		ttl = *req.TTLMinutes
	}
	if ttl < 0 || ttl > 1440 {
		writeProblem(w, http.StatusBadRequest, "Invalid message TTL", "ttl_minutes must be 0 (durable) or between 1 and 1440")
		return
	}
	now := time.Now().UTC()
	lifetime := store.CanonicalMessageLifetime
	if ttl > 0 {
		lifetime = time.Duration(ttl) * time.Minute
	}
	msg, replayed, err := messageStore.SendLocalMessage(r.Context(), req.IdempotencyKey, &store.PipelineMessage{
		PipeID: generatePipeID(), FromAgent: senderID, FromProvider: fromProvider,
		ToAgent: req.ToAgent, Intent: req.Intent, Payload: req.Payload,
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(lifetime),
	})
	if err != nil {
		switch {
		case errors.Is(err, store.ErrMessageIdempotencyConflict):
			writeProblem(w, http.StatusConflict, "Idempotency key conflict",
				"That idempotency key was already used for a different message.")
		case errors.Is(err, store.ErrPipeQuotaPerAgent), errors.Is(err, store.ErrPipeQuotaGlobal):
			w.Header().Set("Retry-After", "5")
			writeProblemTyped(w, http.StatusTooManyRequests, pipeQuotaProblemType, "Too many open messages", err.Error())
		default:
			writeProblem(w, http.StatusInternalServerError, "Message send failed", err.Error())
		}
		return
	}
	code := http.StatusCreated
	if replayed {
		code = http.StatusOK
	}
	if !replayed {
		// Wake metadata is published only after SendLocalMessage has committed the
		// inbox row, idempotency binding, and exact-recipient sequence together.
		// It is best-effort process-local acceleration; durable catch-up remains
		// authoritative across a crash between this return and publication.
		s.publishMessageWake(msg.ToAgent, msg.WakeSeq)
	}
	if !replayed && s.OnEvent != nil {
		// Live connectome firing. Emitted ONLY here: after the send has
		// durably succeeded and only when it is NOT an idempotent replay, so a
		// retried send cannot pulse an edge twice for one message.
		//
		// Deliberately carries nothing — not the endpoints, not the sender's
		// provider, not the intent, not even a count. See web.EventConnectome
		// for why an identity-free fan-out must not name an edge that the
		// RBAC-filtered snapshot would withhold. Clients treat this as
		// "re-fetch the authorized snapshot", never as data.
		s.OnEvent(string(connectomeActivityEvent), "", "", "", nil)
	}
	if !replayed && s.messageNotifier != nil {
		// Best effort only: a missing, closed, or backpressured SSE session
		// cannot change durable send/delivery state and must never fail the send.
		func() {
			defer func() { _ = recover() }()
			s.messageNotifier(AgentMessageNotification{
				RecipientAgentID: msg.ToAgent, MessageID: msg.PipeID,
				FromAgent: msg.FromAgent, SentAt: msg.CreatedAt,
			})
		}()
	}
	response := map[string]any{
		"message_id": msg.PipeID, "status": msg.Status,
		"retention": "durable_until_handled", "idempotent_replay": replayed,
	}
	if ttl > 0 {
		response["expires_at"] = msg.ExpiresAt.Format(time.RFC3339)
		delete(response, "retention")
	}
	writeJSON(w, code, response)
}

func (s *Server) handleMessagesReceive(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	var req struct {
		ReceiveToken      string `json:"receive_token"`
		ClaimantSessionID string `json:"claimant_session_id"`
		Limit             *int   `json:"limit"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if strings.TrimSpace(req.ReceiveToken) == "" {
		writeProblem(w, http.StatusBadRequest, "Missing receive token", "receive_token is required")
		return
	}
	if len(req.ReceiveToken) > store.MaxMessageTokenBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid receive token", "receive_token is too long")
		return
	}
	if len(req.ClaimantSessionID) > store.MaxMessageClaimantSessionBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid claimant session", "claimant_session_id is too long")
		return
	}
	limit := 5
	if req.Limit != nil {
		limit = *req.Limit
	}
	if limit < 1 || limit > 20 {
		writeProblem(w, http.StatusBadRequest, "Invalid receive limit", "limit must be between 1 and 20")
		return
	}
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	agentID := middleware.ContextAgentID(r.Context())
	provider := ""
	if s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(r.Context(), agentID); err == nil && agent != nil {
			provider = agent.Provider
		}
	}
	items, replayed, err := messageStore.ReceiveLocalMessages(r.Context(), agentID, provider, req.ReceiveToken, limit, req.ClaimantSessionID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrMessageReceiveConflict):
			writeProblem(w, http.StatusConflict, "Receive token conflict",
				"That receive token was already used with different parameters.")
		case errors.Is(err, store.ErrMessageReceiveExpired):
			writeProblem(w, http.StatusGone, "Receive replay expired",
				"That exact receive batch is no longer retained; use a new receive token for later work.")
		case errors.Is(err, store.ErrMessageReceiveQuota):
			w.Header().Set("Retry-After", "60")
			writeProblem(w, http.StatusTooManyRequests, "Too many receive attempts",
				"This agent has too many receive tokens inside the replay window; retry later.")
		default:
			writeProblem(w, http.StatusInternalServerError, "Message receive failed", err.Error())
		}
		return
	}
	type receivedMessage struct {
		MessageID          string    `json:"message_id"`
		FromAgent          string    `json:"from_agent"`
		FromProvider       string    `json:"from_provider,omitempty"`
		FromDisplayName    string    `json:"from_display_name,omitempty"`
		FromRegisteredName string    `json:"from_registered_name,omitempty"`
		Intent             string    `json:"intent,omitempty"`
		Payload            string    `json:"payload"`
		Status             string    `json:"status"`
		CreatedAt          time.Time `json:"created_at"`
		ExpiresAt          time.Time `json:"expires_at"`
		Authority          string    `json:"authority"`
		Trust              string    `json:"trust"`
		SecurityNotice     string    `json:"security_notice"`
		ClaimantSessionID  string    `json:"claimant_session_id,omitempty"`
	}
	agentIDs := make([]string, 0, len(items))
	for _, item := range items {
		if item != nil {
			agentIDs = append(agentIDs, item.FromAgent)
		}
	}
	presentations := s.resolvePipelineAgentPresentations(r.Context(), agentIDs...)
	response := make([]receivedMessage, 0, len(items))
	for _, item := range items {
		presentation := presentations[item.FromAgent]
		response = append(response, receivedMessage{
			MessageID: item.PipeID, FromAgent: item.FromAgent, FromProvider: item.FromProvider,
			FromDisplayName: presentation.DisplayName, FromRegisteredName: presentation.RegisteredName,
			Intent: item.Intent, Payload: item.Payload, Status: item.Status,
			CreatedAt: item.CreatedAt, ExpiresAt: item.ExpiresAt,
			Authority: pipeRequestAuthority, Trust: pipeLocalTrust,
			SecurityNotice:    pipeRESTRequestSecurityNotice,
			ClaimantSessionID: item.ClaimedSessionID,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"items": response, "count": len(response), "idempotent_replay": replayed,
	})
}

func (s *Server) handleMessageHandoff(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	var req struct {
		FromSessionID string `json:"from_session_id"`
		ToSessionID   string `json:"to_session_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.FromSessionID == "" || req.ToSessionID == "" ||
		len(req.FromSessionID) > store.MaxMessageClaimantSessionBytes || len(req.ToSessionID) > store.MaxMessageClaimantSessionBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid claimant session", "from_session_id and to_session_id are required and bounded")
		return
	}
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	replayed, err := messageStore.HandoffLocalMessageClaim(r.Context(), middleware.ContextAgentID(r.Context()),
		chi.URLParam(r, "message_id"), req.FromSessionID, req.ToSessionID)
	if err != nil {
		if errors.Is(err, store.ErrMessageReceiveConflict) {
			writeProblem(w, http.StatusConflict, "Claimant session changed", "The message is no longer assigned to from_session_id; refresh passive history before retrying.")
			return
		}
		writeProblem(w, http.StatusNotFound, "Message not found", "The message is not available for handoff.")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message_id": chi.URLParam(r, "message_id"),
		"claimant_session_id": req.ToSessionID, "idempotent_replay": replayed})
}

func (s *Server) handleMessageReply(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	messageID := chi.URLParam(r, "message_id")
	var req struct {
		Result            string `json:"result"`
		ClaimantSessionID string `json:"claimant_session_id"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Result == "" {
		writeProblem(w, http.StatusBadRequest, "Missing reply", "result is required")
		return
	}
	if len(req.ClaimantSessionID) > store.MaxMessageClaimantSessionBytes {
		writeProblem(w, http.StatusBadRequest, "Invalid claimant session", "claimant_session_id is too long")
		return
	}
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	replayed, err := messageStore.ReplyLocalMessage(r.Context(), middleware.ContextAgentID(r.Context()), messageID, req.Result, req.ClaimantSessionID)
	if err != nil {
		switch {
		case errors.Is(err, store.ErrMessageReplyConflict):
			writeProblem(w, http.StatusConflict, "Reply conflict", "This message already has a different reply.")
		case errors.Is(err, store.ErrPipeResultTooLarge):
			writeProblemTyped(w, http.StatusRequestEntityTooLarge, pipeTooLargeProblemType, "Reply too large", err.Error())
		default:
			writeCanonicalMessageNotFound(w, messageID)
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message_id": messageID, "status": "completed", "idempotent_replay": replayed,
	})
}

func (s *Server) handleMessageRead(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	messageID := chi.URLParam(r, "message_id")
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	replayed, err := messageStore.AcknowledgeLocalMessageRead(r.Context(), middleware.ContextAgentID(r.Context()), messageID)
	if err != nil {
		writeCanonicalMessageNotFound(w, messageID)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"message_id": messageID, "read_status": "confirmed", "idempotent_replay": replayed,
	})
}

const maxMessageReadBatch = 20

type messageReadBatchRequest struct {
	MessageIDs []string `json:"message_ids"`
}

// handleMessageReadBatch acknowledges an already-returned exact-recipient
// batch in one authenticated request. Per-item outcomes preserve partial
// failure without turning a 20-message inbox into 20 additional HTTP calls.
func (s *Server) handleMessageReadBatch(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	var req messageReadBatchRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if len(req.MessageIDs) == 0 || len(req.MessageIDs) > maxMessageReadBatch {
		writeProblem(w, http.StatusBadRequest, "Invalid message batch", "message_ids must contain between 1 and 20 exact message IDs")
		return
	}
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	receiverID := middleware.ContextAgentID(r.Context())
	items := make([]map[string]any, 0, len(req.MessageIDs))
	seen := make(map[string]struct{}, len(req.MessageIDs))
	for _, messageID := range req.MessageIDs {
		messageID = strings.TrimSpace(messageID)
		if messageID == "" {
			items = append(items, map[string]any{"message_id": "", "read_status": "not_confirmed", "error": "invalid_message_id"})
			continue
		}
		if _, duplicate := seen[messageID]; duplicate {
			items = append(items, map[string]any{"message_id": messageID, "read_status": "not_confirmed", "error": "duplicate_message_id"})
			continue
		}
		seen[messageID] = struct{}{}
		replayed, err := messageStore.AcknowledgeLocalMessageRead(r.Context(), receiverID, messageID)
		if err != nil {
			items = append(items, map[string]any{"message_id": messageID, "read_status": "not_confirmed", "error": "not_found"})
			continue
		}
		items = append(items, map[string]any{"message_id": messageID, "read_status": "confirmed", "idempotent_replay": replayed})
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "count": len(items)})
}

func (s *Server) handleMessageStatus(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	messageID := chi.URLParam(r, "message_id")
	messageStore, ok := canonicalMessageStore(s)
	if !ok {
		writeProblem(w, http.StatusNotImplemented, "Messages unavailable", "The active store does not support canonical messages.")
		return
	}
	status, err := messageStore.GetMessageStatusForSender(r.Context(), middleware.ContextAgentID(r.Context()), messageID)
	if err != nil {
		writeCanonicalMessageNotFound(w, messageID)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

// handleMessageReplyStatus exposes only the replying agent's own immutable
// federated result event. It is deliberately separate from sender-owned message
// status: no request/result content or original-message workflow state crosses
// this boundary, and the reply is never represented as another inbox request.
func (s *Server) handleMessageReplyStatus(w http.ResponseWriter, r *http.Request) {
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	replyEventID := chi.URLParam(r, "reply_event_id")
	transportStore, ok := s.store.(store.FederatedPipelineStore)
	if !ok {
		writeCanonicalMessageNotFound(w, replyEventID)
		return
	}
	event, err := transportStore.GetPipelineTransport(r.Context(), replyEventID)
	if err != nil || event == nil || event.EventKind != "result" ||
		event.SourceAgentID != middleware.ContextAgentID(r.Context()) {
		writeCanonicalMessageNotFound(w, replyEventID)
		return
	}
	transportStatus := event.State
	if transportStatus == "pending" {
		transportStatus = "queued"
	}
	response := map[string]any{
		"reply_event_id":   replyEventID,
		"scope":            "federated",
		"reply_status":     transportStatus,
		"transport_status": transportStatus,
		"created_at":       event.CreatedAt,
	}
	if event.DeliveredAt != nil {
		response["delivered_at"] = event.DeliveredAt
	}
	writeJSON(w, http.StatusOK, response)
}
