package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireSignedToolRequest(t *testing.T, r *http.Request) {
	t.Helper()
	require.NotEmpty(t, r.Header.Get("X-Agent-ID"))
	require.NotEmpty(t, r.Header.Get("X-Signature"))
	require.NotEmpty(t, r.Header.Get("X-Timestamp"))
	require.Len(t, r.Header.Get("X-Nonce"), 16)
}

func newAdvertisedToolTestServer(t *testing.T, handler http.HandlerFunc) (*Server, string) {
	t.Helper()
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)
	_, privateKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return NewServer(ts.URL, privateKey), ts.URL
}

func TestAdvertisedRegisterUsesExactSignedContract(t *testing.T) {
	s, _ := newAdvertisedToolTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/agent/register", r.URL.Path)
		requireSignedToolRequest(t, r)
		var body map[string]any
		require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
		require.Equal(t, "worker", body["name"])
		require.Equal(t, "safe boot", body["boot_bio"])
		_ = json.NewEncoder(w).Encode(map[string]any{
			"agent_id": "agent-1", "name": "worker", "registered_name": "worker",
			"status": "active", "on_chain_height": 42,
		})
	})
	result, err := s.toolRegister(context.Background(), map[string]any{"name": "worker", "boot_bio": "safe boot"})
	require.NoError(t, err)
	require.Equal(t, "agent-1", result.(map[string]any)["agent_id"])
	_, err = s.toolRegister(context.Background(), map[string]any{})
	require.ErrorContains(t, err, "name is required")
}

func TestAdvertisedRenamePreservesBioAndUsesSelfOnlyUpdate(t *testing.T) {
	requestCount := 0
	s, _ := newAdvertisedToolTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requestCount++
		requireSignedToolRequest(t, r)
		switch {
		case r.Method == http.MethodGet && len(r.URL.Path) > len("/v1/agent/") && r.URL.Path[:len("/v1/agent/")] == "/v1/agent/":
			_ = json.NewEncoder(w).Encode(map[string]string{"boot_bio": "preserve me"})
		case r.Method == http.MethodPut && r.URL.Path == "/v1/agent/update":
			var body map[string]any
			require.NoError(t, json.NewDecoder(r.Body).Decode(&body))
			require.Equal(t, "New Name", body["name"])
			require.Equal(t, "preserve me", body["boot_bio"])
			_ = json.NewEncoder(w).Encode(map[string]string{"agent_id": "agent-1", "name": "New Name", "status": "committed", "tx_hash": "tx-1"})
		default:
			t.Fatalf("unexpected rename request %s %s", r.Method, r.URL.Path)
		}
	})
	result, err := s.toolRename(context.Background(), map[string]any{"name": "New Name"})
	require.NoError(t, err)
	require.Equal(t, 2, requestCount)
	require.Equal(t, "tx-1", result.(map[string]any)["tx_hash"])
	_, err = s.toolRename(context.Background(), map[string]any{})
	require.ErrorContains(t, err, "name is required")
}

func TestAdvertisedScopeToolsUseSignedEscapedRoutes(t *testing.T) {
	requests := 0
	s, _ := newAdvertisedToolTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		requireSignedToolRequest(t, r)
		switch requests {
		case 1:
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "/v1/scopes", r.URL.EscapedPath())
		case 2:
			require.Equal(t, http.MethodGet, r.Method)
			require.Equal(t, "/v1/scopes/research%2Fprivate", r.URL.EscapedPath())
		default:
			t.Fatalf("unexpected scope request")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true})
	})
	_, err := s.toolScopeList(context.Background(), nil)
	require.NoError(t, err)
	_, err = s.toolScopeGet(context.Background(), map[string]any{"scope_id": "research/private"})
	require.NoError(t, err)
	_, err = s.toolScopeGet(context.Background(), nil)
	require.ErrorContains(t, err, "scope_id is required")
}

func TestAdvertisedGovernanceStatusUsesSignedListAndDetailContracts(t *testing.T) {
	requests := 0
	s, _ := newAdvertisedToolTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		requireSignedToolRequest(t, r)
		require.Equal(t, http.MethodGet, r.Method)
		if requests == 1 {
			require.Equal(t, "/v1/dashboard/governance/proposals", r.URL.Path)
			require.Equal(t, "voting", r.URL.Query().Get("status"))
			_ = json.NewEncoder(w).Encode(map[string]any{"proposals": []map[string]any{{"proposal_id": "p-1"}}})
			return
		}
		require.Equal(t, "/v1/dashboard/governance/proposals/p%2F1", r.URL.EscapedPath())
		_ = json.NewEncoder(w).Encode(map[string]any{"proposal_id": "p/1"})
	})
	list, err := s.toolGovStatus(context.Background(), nil)
	require.NoError(t, err)
	require.Equal(t, "active", list.(map[string]any)["status"])
	detail, err := s.toolGovStatus(context.Background(), map[string]any{"proposal_id": "p/1"})
	require.NoError(t, err)
	require.Equal(t, "p/1", detail.(map[string]any)["proposal_id"])
}

// TestAdvertisedMessageRepliesUsesExactSignedPassiveContract pins the v11.18.2
// sender-side reply read to the same standard as every other advertised tool:
// it must reach exactly one route, signed, with a passive GET, and it must
// advertise itself as the reply counterpart of sage_message_reply rather than
// leaving an agent to discover a deprecated alias.
func TestAdvertisedMessageRepliesUsesExactSignedPassiveContract(t *testing.T) {
	requests := 0
	s, _ := newAdvertisedToolTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests++
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/pipe/results", r.URL.Path)
		require.Equal(t, "7", r.URL.Query().Get("limit"))
		requireSignedToolRequest(t, r)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{{
				"pipe_id": "msg-advertised", "to_provider": "claude-code", "intent": "review",
				"result": "answered", "status": "completed", "completed_at": "2026-08-08T00:05:00Z",
			}},
			"count": 1,
		})
	})

	result, err := s.toolMessageReplies(context.Background(), map[string]any{"limit": 7})
	require.NoError(t, err)
	require.Equal(t, 1, requests, "the advertised reply read spends exactly one signed request")
	items := result.(map[string]any)["items"].([]map[string]any)
	require.Len(t, items, 1)
	require.Equal(t, "msg-advertised", items[0]["message_id"])

	tool, ok := s.tools["sage_message_replies"]
	require.True(t, ok)
	require.False(t, hiddenCompatibilityTools[tool.Name],
		"the reply read must be discoverable, not hidden behind the deprecated compatibility window")
	require.Contains(t, tool.Description, "sage_message_reply")
	require.Contains(t, tool.Description, "untrusted")
	require.Contains(t, tool.Description, "Passive")
}

// TestHiddenCompatibilityToolsAreUnchangedByReplyVisibility guards the other
// direction: adding the advertised reply read must not un-hide or re-home any
// deprecated pipe alias, and must not teach a client to use one.
func TestHiddenCompatibilityToolsAreUnchangedByReplyVisibility(t *testing.T) {
	s, _ := newAdvertisedToolTestServer(t, func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"items": []any{}, "count": 0})
	})
	require.Len(t, hiddenCompatibilityTools, 4)
	for _, legacy := range []string{"sage_pipe", "sage_pipe_history", "sage_pipe_receipt_status", "sage_pipe_result"} {
		require.True(t, hiddenCompatibilityTools[legacy], "%s must stay hidden", legacy)
		require.Contains(t, s.tools, legacy, "%s must stay dispatchable for existing callers", legacy)
	}
	require.False(t, hiddenCompatibilityTools["sage_message_replies"])
	for name, tool := range s.tools {
		if hiddenCompatibilityTools[name] {
			continue
		}
		require.NotContains(t, tool.Description, "sage_pipe_result",
			"%s must not point agents at a hidden deprecated alias for reading replies", name)
	}
}
