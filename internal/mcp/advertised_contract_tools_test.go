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
