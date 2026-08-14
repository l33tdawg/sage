package rest

import (
	"bytes"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

func messageRouterAs(s *Server, callerID string, exactProof bool) http.Handler {
	r := chi.NewRouter()
	r.Use(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			ctx := middleware.WithAgentID(req.Context(), callerID)
			if exactProof {
				ctx = middleware.WithAgentAuth(ctx, &middleware.AgentAuthProof{
					Signature: make([]byte, 64), Nonce: make([]byte, 16), CanonicalRequest: []byte(req.Method + " " + req.URL.Path),
				})
			}
			next.ServeHTTP(w, req.WithContext(ctx))
		})
	})
	r.Post("/v1/messages", s.handleMessageSend)
	r.Get("/v1/messages/wake", s.handleMessageWake)
	r.Post("/v1/messages/receive", s.handleMessagesReceive)
	r.Post("/v1/messages/{message_id}/reply", s.handleMessageReply)
	r.Put("/v1/messages/{message_id}/handoff", s.handleMessageHandoff)
	r.Put("/v1/messages/{message_id}/read", s.handleMessageRead)
	r.Put("/v1/messages/read-batch", s.handleMessageReadBatch)
	r.Get("/v1/messages/{message_id}/status", s.handleMessageStatus)
	r.Get("/v1/messages/replies/{reply_event_id}/status", s.handleMessageReplyStatus)
	return r
}

func addMessageAgent(t *testing.T, s *store.SQLiteStore, id string) {
	t.Helper()
	require.NoError(t, s.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: id, Name: id, Provider: "test", Status: "active",
	}))
}

func callMessageJSON(t *testing.T, handler http.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	var raw []byte
	if body != nil {
		var err error
		raw, err = json.Marshal(body)
		require.NoError(t, err)
	}
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(method, path, bytes.NewReader(raw)))
	return rr
}

func TestCanonicalLocalMessagesEndToEndAndAntiEnumeration(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")
	var notifications []AgentMessageNotification
	s.SetMessageNotifier(func(notification AgentMessageNotification) {
		notifications = append(notifications, notification)
	})

	sendBody := map[string]any{
		"to_agent": "bob", "intent": "review", "payload": "private request",
		"idempotency_key": "one-send",
	}
	sent := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", sendBody)
	require.Equal(t, http.StatusCreated, sent.Code, sent.Body.String())
	var sendResponse map[string]any
	require.NoError(t, json.Unmarshal(sent.Body.Bytes(), &sendResponse))
	messageID := sendResponse["message_id"].(string)
	require.NotEmpty(t, messageID)
	require.True(t, strings.HasPrefix(messageID, "msg-"), messageID)
	require.Equal(t, "durable_until_handled", sendResponse["retention"])
	require.NotContains(t, sendResponse, "expires_at",
		"omitted message TTL must keep inbox work until the recipient handles it")
	require.Len(t, notifications, 1)
	require.Equal(t, "bob", notifications[0].RecipientAgentID)
	require.Equal(t, "alice", notifications[0].FromAgent)
	require.Equal(t, messageID, notifications[0].MessageID)

	replayedSend := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", sendBody)
	require.Equal(t, http.StatusOK, replayedSend.Code, replayedSend.Body.String())
	require.Contains(t, replayedSend.Body.String(), messageID)
	require.Contains(t, replayedSend.Body.String(), `"idempotent_replay":true`)
	require.Len(t, notifications, 1, "an idempotent send replay must not emit a second wake-up")

	receiveBody := map[string]any{"receive_token": "one-receive", "limit": 5, "claimant_session_id": "mcp-helper"}
	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive", receiveBody)
	require.Equal(t, http.StatusOK, received.Code, received.Body.String())
	require.Contains(t, received.Body.String(), `"message_id":"`+messageID+`"`)
	require.Contains(t, received.Body.String(), "private request")
	receivedAgain := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive", receiveBody)
	require.Equal(t, http.StatusOK, receivedAgain.Code)
	require.Contains(t, receivedAgain.Body.String(), `"idempotent_replay":true`)
	require.Contains(t, receivedAgain.Body.String(), messageID)
	require.Contains(t, receivedAgain.Body.String(), `"claimant_session_id":"mcp-helper"`)

	handoff := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut,
		"/v1/messages/"+messageID+"/handoff", map[string]any{
			"from_session_id": "mcp-helper", "to_session_id": "mcp-supervisor",
		})
	require.Equal(t, http.StatusOK, handoff.Code, handoff.Body.String())
	require.Contains(t, handoff.Body.String(), `"claimant_session_id":"mcp-supervisor"`)
	staleHandoff := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut,
		"/v1/messages/"+messageID+"/handoff", map[string]any{
			"from_session_id": "mcp-helper", "to_session_id": "mcp-third",
		})
	require.Equal(t, http.StatusConflict, staleHandoff.Code, staleHandoff.Body.String())

	read := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut, "/v1/messages/"+messageID+"/read", map[string]any{})
	require.Equal(t, http.StatusOK, read.Code, read.Body.String())
	status := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodGet, "/v1/messages/"+messageID+"/status", nil)
	require.Equal(t, http.StatusOK, status.Code, status.Body.String())
	require.Contains(t, status.Body.String(), `"read_status":"confirmed"`)
	require.NotContains(t, status.Body.String(), "private request")

	unauthorized := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/"+messageID+"/status", nil)
	missing := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/"+messageID+"-missing/status", nil)
	require.Equal(t, http.StatusNotFound, unauthorized.Code)
	require.Equal(t, http.StatusNotFound, missing.Code)
	// IDs differ in the detail, but the endpoint exposes no payload, sender,
	// recipient, status, or existence discriminator.
	require.NotContains(t, unauthorized.Body.String(), "private request")
	require.NotContains(t, unauthorized.Body.String(), "alice")
	require.NotContains(t, unauthorized.Body.String(), "bob")
}

func TestCanonicalFederatedMessageStatusIsSenderOnly(t *testing.T) {
	s, sqlite := newPipeServer(t)
	pub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sender := hex.EncodeToString(pub)
	recipient := strings.Repeat("b", 64)
	now := time.Now().UTC().Truncate(time.Millisecond)
	msg := &store.PipelineMessage{
		PipeID: "fed-status-rest", FromAgent: sender, ToAgent: recipient,
		Intent: "request", Payload: "private", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour), DestinationChainID: "chain-b",
		FederationPolicyEpoch:     strings.Repeat("c", 64),
		FederationAgreementID:     strings.Repeat("d", 64),
		FederationContactID:       strings.Repeat("e", 64),
		FederationContactRevision: strings.Repeat("f", 64),
	}
	event := &store.PipelineTransportOutbox{
		EventID: "fed-status-rest-event", PipeID: msg.PipeID, RemoteChainID: "chain-b",
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sender,
		TargetAgentID: recipient, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Proof: store.PipelineAgentProof{AgentID: sender, Signature: make([]byte, ed25519.SignatureSize),
			Timestamp: now.Unix(), Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}")},
	}
	_, _, err = sqlite.SendFederatedMessage(t.Context(), "fed-status-rest-key", msg, event)
	require.NoError(t, err)

	owned := callMessageJSON(t, messageRouterAs(s, sender, true), http.MethodGet,
		"/v1/messages/"+msg.PipeID+"/status", nil)
	require.Equal(t, http.StatusOK, owned.Code, owned.Body.String())
	var status store.MessageStatus
	require.NoError(t, json.Unmarshal(owned.Body.Bytes(), &status))
	require.Equal(t, "federated", status.Scope)
	require.Equal(t, "queued", status.TransportStatus)
	require.Equal(t, "unsupported", status.ReadStatus)
	require.Equal(t, "pending", status.WorkflowStatus)

	receiver := callMessageJSON(t, messageRouterAs(s, recipient, true), http.MethodGet,
		"/v1/messages/"+msg.PipeID+"/status", nil)
	absent := callMessageJSON(t, messageRouterAs(s, recipient, true), http.MethodGet,
		"/v1/messages/not-present/status", nil)
	require.Equal(t, http.StatusNotFound, receiver.Code)
	require.Equal(t, http.StatusNotFound, absent.Code)
	require.JSONEq(t, absent.Body.String(), receiver.Body.String())
}

func TestCanonicalMessageReadAndReplyRequireExactSignedFetchedRecipient(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")
	_, _, err := sqlite.SendLocalMessage(t.Context(), "send", &store.PipelineMessage{
		PipeID: "msg-exact", FromAgent: "alice", ToAgent: "bob", Payload: "secret", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)

	unsigned := callMessageJSON(t, messageRouterAs(s, "bob", false), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "receive", "limit": 1})
	require.Equal(t, http.StatusForbidden, unsigned.Code)
	unsignedSend := callMessageJSON(t, messageRouterAs(s, "alice", false), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "unsigned", "idempotency_key": "unsigned",
	})
	require.Equal(t, http.StatusForbidden, unsignedSend.Code)
	unsignedStatus := callMessageJSON(t, messageRouterAs(s, "alice", false), http.MethodGet, "/v1/messages/msg-exact/status", nil)
	require.Equal(t, http.StatusForbidden, unsignedStatus.Code)

	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "receive", "limit": 1})
	require.Equal(t, http.StatusOK, received.Code)

	wrong := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodPut, "/v1/messages/msg-exact/read", map[string]any{})
	require.Equal(t, http.StatusNotFound, wrong.Code)
	reply := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-exact/reply",
		map[string]any{"result": "done"})
	require.Equal(t, http.StatusOK, reply.Code, reply.Body.String())
	replyAgain := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-exact/reply",
		map[string]any{"result": "done"})
	require.Equal(t, http.StatusOK, replyAgain.Code)
	require.Contains(t, replyAgain.Body.String(), `"idempotent_replay":true`)
	conflict := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/msg-exact/reply",
		map[string]any{"result": "different"})
	require.Equal(t, http.StatusConflict, conflict.Code)
}

func TestCanonicalMessageReadBatchPreservesPerItemFailure(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	for _, id := range []string{"msg-a", "msg-b"} {
		_, _, err := sqlite.SendLocalMessage(t.Context(), "send-"+id, &store.PipelineMessage{
			PipeID: id, FromAgent: "alice", ToAgent: "bob", Payload: "secret", Status: "pending",
			CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
		})
		require.NoError(t, err)
	}
	received := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPost, "/v1/messages/receive",
		map[string]any{"receive_token": "receive-batch", "limit": 2})
	require.Equal(t, http.StatusOK, received.Code)

	read := callMessageJSON(t, messageRouterAs(s, "bob", true), http.MethodPut, "/v1/messages/read-batch",
		map[string]any{"message_ids": []string{"msg-a", "missing", "msg-b"}})
	require.Equal(t, http.StatusOK, read.Code, read.Body.String())
	var response struct {
		Items []struct {
			MessageID  string `json:"message_id"`
			ReadStatus string `json:"read_status"`
			Error      string `json:"error"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(read.Body.Bytes(), &response))
	require.Equal(t, "confirmed", response.Items[0].ReadStatus)
	require.Equal(t, "not_found", response.Items[1].Error)
	require.Equal(t, "confirmed", response.Items[2].ReadStatus)
}

func TestCanonicalMessageInputBoundsAndNonEnumeratingStatus(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	addMessageAgent(t, sqlite, "mallory")

	tooLong := strings.Repeat("x", store.MaxMessageTokenBytes+1)
	for _, tc := range []struct {
		name, method, path string
		body               any
	}{
		{name: "send token", method: http.MethodPost, path: "/v1/messages", body: map[string]any{
			"to_agent": "bob", "payload": "work", "idempotency_key": tooLong,
		}},
		{name: "receive token", method: http.MethodPost, path: "/v1/messages/receive", body: map[string]any{
			"receive_token": tooLong, "limit": 1,
		}},
		{name: "negative ttl", method: http.MethodPost, path: "/v1/messages", body: map[string]any{
			"to_agent": "bob", "payload": "work", "idempotency_key": "ttl-neg", "ttl_minutes": -1,
		}},
		{name: "oversize ttl", method: http.MethodPost, path: "/v1/messages", body: map[string]any{
			"to_agent": "bob", "payload": "work", "idempotency_key": "ttl-large", "ttl_minutes": 1441,
		}},
		{name: "zero receive limit", method: http.MethodPost, path: "/v1/messages/receive", body: map[string]any{
			"receive_token": "limit-zero", "limit": 0,
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rr := callMessageJSON(t, messageRouterAs(s, "alice", true), tc.method, tc.path, tc.body)
			require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
		})
	}

	_, _, err := sqlite.SendLocalMessage(t.Context(), "hidden", &store.PipelineMessage{
		PipeID: "msg-hidden", FromAgent: "alice", ToAgent: "bob", Payload: "secret", Status: "pending",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	})
	require.NoError(t, err)
	unrelated := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/msg-hidden/status", nil)
	absent := callMessageJSON(t, messageRouterAs(s, "mallory", true), http.MethodGet, "/v1/messages/not-present/status", nil)
	require.Equal(t, http.StatusNotFound, unrelated.Code)
	require.Equal(t, http.StatusNotFound, absent.Code)
	require.JSONEq(t, unrelated.Body.String(), absent.Body.String(), "absent and unrelated IDs must have one indistinguishable problem body")
}

func TestCanonicalMessageSendDoesNotFailWhenOptionalNotifierDrops(t *testing.T) {
	s, sqlite := newPipeServer(t)
	addMessageAgent(t, sqlite, "alice")
	addMessageAgent(t, sqlite, "bob")
	s.SetMessageNotifier(func(AgentMessageNotification) {
		panic("simulated closed/full notification transport")
	})
	rr := callMessageJSON(t, messageRouterAs(s, "alice", true), http.MethodPost, "/v1/messages", map[string]any{
		"to_agent": "bob", "payload": "durable despite wake-up drop", "idempotency_key": "drop-safe",
	})
	require.Equal(t, http.StatusCreated, rr.Code, rr.Body.String())
	require.Contains(t, rr.Body.String(), "message_id")
	pipes, err := sqlite.GetOutbox(t.Context(), "alice", 10)
	require.NoError(t, err)
	require.Len(t, pipes, 1)
}
