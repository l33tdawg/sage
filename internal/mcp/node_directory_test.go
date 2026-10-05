package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSageFindAgentRetainsDomainFreeContactsAndRevalidatesThem(t *testing.T) {
	requests := 0
	agentID := strings.Repeat("a", 64)
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agents/lookup", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"agents": []any{}})
	})
	mux.HandleFunc("/v1/federation/available", func(w http.ResponseWriter, r *http.Request) {
		require.NotEmpty(t, r.Header.Get("X-Agent-ID"))
		requests++
		_ = json.NewEncoder(w).Encode(map[string]any{"connections": []any{map[string]any{
			"remote_chain_id": "chain-tii", "network_name": "Research laptop",
			"remote_agents": []any{map[string]any{
				"agent_id": agentID, "display_name": "codex/tii", "registered_name": "codex/tii", "provider": "codex",
				"address": agentID + "@chain-tii", "authorization_mode": "node-messaging-v1", "domains": []any{}, "available": true, "accepting": true,
			}},
		}}})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, key)
	for range 2 {
		result, err := s.toolFindAgent(context.Background(), map[string]any{"name": "codex/tii"})
		require.NoError(t, err)
		out := result.(map[string]any)
		matches := out["matches"].([]map[string]any)
		require.Len(t, matches, 1)
		require.Equal(t, agentID+"@chain-tii", matches[0]["to"])
		require.Equal(t, "live", out["federated_cache"])
	}
	require.Equal(t, 2, requests, "a cached domain-free result must not bypass current registration/deny checks")
}
