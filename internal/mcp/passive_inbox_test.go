package mcp

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
)

func TestPassiveInboxDoesNotAcquireWorkAndClaimSelectsExactID(t *testing.T) {
	var mu sync.Mutex
	var mutations []string
	item := map[string]any{"pipe_id": "msg-selected", "from_agent": "alice", "to_agent": "bob", "payload": "request"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mu.Lock()
			mutations = append(mutations, r.Method+" "+r.URL.Path)
			mu.Unlock()
		}
		switch r.URL.Path {
		case "/v1/messages/inbox":
			json.NewEncoder(w).Encode(map[string]any{"items": []any{item, map[string]any{"pipe_id": "msg-other", "payload": "other"}}, "count": 2, "passive": true})
		case "/v1/dashboard/task-notifications/inbox":
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"notification_id": "notice-1", "state": "unread"}}, "count": 1, "passive": true})
		case "/v1/messages/msg-selected/inspect":
			json.NewEncoder(w).Encode(map[string]any{"item": item, "passive": true})
		case "/v1/messages/msg-selected/claim":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			json.NewEncoder(w).Encode(map[string]any{"item": item, "status": "claimed", "claimant_session_id": body["claimant_session_id"], "claim_revision": 0})
		case "/v1/messages/read-batch":
			json.NewEncoder(w).Encode(map[string]any{"items": []any{map[string]any{"message_id": "msg-selected", "read_status": "confirmed"}}})
		default:
			json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0, "claimed_elsewhere_count": 0})
		}
	}))
	defer server.Close()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(server.URL, priv)
	for i := 0; i < 2; i++ {
		result, err := s.toolInbox(context.Background(), map[string]any{})
		require.NoError(t, err)
		response := result.(map[string]any)
		require.Equal(t, "sage.inbox.v3", response["coordination_schema"])
		require.Equal(t, true, response["passive"])
		require.Equal(t, 3, response["count"], "task notices cannot be starved by a full message page")
		items := response["items"].([]map[string]any)
		require.Equal(t, false, items[0]["requires_reply"])
		require.NotContains(t, items[0], "claimant_session_id")
		require.Equal(t, true, items[2]["requires_acknowledgement"])
	}
	mu.Lock()
	require.Empty(t, mutations)
	mu.Unlock()
	require.NotContains(t, s.tools, "sage_messages_receive")
	result, err := s.toolMessageClaim(context.Background(), map[string]any{"message_id": "msg-selected"})
	require.NoError(t, err)
	owned := result.(map[string]any)["item"].(map[string]any)
	require.Equal(t, true, owned["requires_reply"])
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"PUT /v1/messages/msg-selected/claim", "PUT /v1/messages/read-batch"}, mutations)
}

func TestPassiveInboxRejectsAnUnconfirmedContractWithoutClaimFallback(t *testing.T) {
	var mutated bool
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mutated = true
		}
		json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	}))
	defer ts.Close()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, err = NewServer(ts.URL, priv).toolInbox(context.Background(), map[string]any{})
	require.ErrorContains(t, err, "did not confirm passive")
	require.False(t, mutated)
}

func TestExplicitFederatedClaimSignsOnlyTheSelectedReceipt(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	challenge := json.RawMessage(`{"version":2,"message_id":"remote-origin","event_kind":"claimed"}`)
	candidate := map[string]any{"pipe_id": "local-import", "from_agent": "foreign-agent", "source_chain_id": "peer-chain", "receipt_protocol_version": 2, "payload": "untrusted request"}
	var mutations []string
	var mu sync.Mutex
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			mu.Lock()
			mutations = append(mutations, r.URL.Path)
			mu.Unlock()
		}
		switch r.URL.Path {
		case "/v1/messages/inbox":
			json.NewEncoder(w).Encode(map[string]any{"items": []any{candidate}, "count": 1, "passive": true})
		case "/v1/messages/local-import/inspect":
			json.NewEncoder(w).Encode(map[string]any{"item": candidate, "passive": true, "claim_receipt_challenge": challenge})
		case "/v1/messages/local-import/claim":
			var body struct {
				Session string                   `json:"claimant_session_id"`
				Proof   store.PipelineAgentProof `json:"claim_proof"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.NotEmpty(t, body.Session)
			require.NotEmpty(t, body.Proof.Signature)
			require.NotEmpty(t, body.Proof.Nonce)
			require.True(t, bytes.Equal(append([]byte("PUT /v1/pipe/local-import/receipt/claimed\n"), challenge...), body.Proof.CanonicalRequest))
			json.NewEncoder(w).Encode(map[string]any{"item": candidate, "status": "claimed", "claimant_session_id": body.Session, "claim_revision": 0})
		case "/v1/pipe/local-import/receipt/challenge/read":
			json.NewEncoder(w).Encode(map[string]any{"challenge": map[string]any{"version": 2, "event_kind": "read"}})
		case "/v1/pipe/local-import/receipt/read":
			json.NewEncoder(w).Encode(map[string]any{"receipt_status": "queued"})
		default:
			json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0, "claimed_elsewhere_count": 0, "passive": true})
		}
	}))
	defer ts.Close()
	server := NewServer(ts.URL, priv)
	_, err = server.toolInbox(context.Background(), map[string]any{})
	require.NoError(t, err)
	mu.Lock()
	require.Empty(t, mutations)
	mu.Unlock()
	result, err := server.toolMessageClaim(context.Background(), map[string]any{"message_id": "local-import"})
	require.NoError(t, err)
	item := result.(map[string]any)["item"].(map[string]any)
	require.Equal(t, "external_untrusted", item["trust"])
	require.Equal(t, true, item["requires_reply"])
	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, []string{"/v1/messages/local-import/claim", "/v1/pipe/local-import/receipt/read"}, mutations)
}
