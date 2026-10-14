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

func TestSageFindAgentRanksUnifiedMatchesBeforeApplyingLimit(t *testing.T) {
	for _, tc := range []struct {
		name          string
		limit         int
		remoteExact   bool
		wantTotal     int
		wantAddresses []string
	}{
		{"substrings from both sources", 10, false, 2, []string{"local-helper", "remote-helper@remote-chain"}},
		{"exact first under limit", 1, true, 3, []string{"remote-exact@remote-chain"}},
		{"exact and substrings from both sources", 10, true, 3, []string{"remote-exact@remote-chain", "local-helper", "remote-helper@remote-chain"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mux := http.NewServeMux()
			mux.HandleFunc("/v1/agents/lookup", func(w http.ResponseWriter, r *http.Request) {
				require.NotEmpty(t, r.Header.Get("X-Agent-ID"))
				require.Equal(t, "voice", r.URL.Query().Get("name"))
				_ = json.NewEncoder(w).Encode(map[string]any{"agents": []map[string]any{{
					"agent_id": "local-helper", "name": "Alpha voice helper", "match_kind": "substring",
				}}})
			})
			mux.HandleFunc("/v1/federation/available", func(w http.ResponseWriter, r *http.Request) {
				require.NotEmpty(t, r.Header.Get("X-Agent-ID"))
				require.Equal(t, "voice", r.URL.Query().Get("agent_name"))
				require.Equal(t, "20", r.URL.Query().Get("agent_limit"))
				contacts := []map[string]any{{
					"agent_id": "remote-helper", "display_name": "Zulu voice helper", "address": "remote-helper@remote-chain", "authorization_mode": "linked-v23",
				}}
				if tc.remoteExact {
					contacts = append(contacts, map[string]any{
						"agent_id": "remote-exact", "display_name": "Voice", "address": "remote-exact@remote-chain", "authorization_mode": "linked-v23",
					})
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"connections": []map[string]any{{
					"remote_chain_id": "remote-chain", "network_name": "Remote SAGE",
					"remote_agents": contacts,
				}}, "complete": true})
			})
			ts := httptest.NewServer(mux)
			defer ts.Close()
			_, key, err := ed25519.GenerateKey(nil)
			require.NoError(t, err)
			result, err := NewServer(ts.URL, key).toolFindAgent(context.Background(), map[string]any{
				"name": "voice", "scope": "all", "limit": tc.limit,
			})
			require.NoError(t, err)
			out := result.(map[string]any)
			require.Equal(t, tc.wantTotal, out["total"])
			require.Equal(t, tc.limit == 1, out["truncated"])
			matches := out["matches"].([]map[string]any)
			require.Len(t, matches, len(tc.wantAddresses))
			for i, address := range tc.wantAddresses {
				require.Equal(t, address, matches[i]["to"])
			}
		})
	}
}

func TestSageFindAgentLocalScopeSkipsFederation(t *testing.T) {
	localRequests, federatedRequests := 0, 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agents/lookup", func(w http.ResponseWriter, _ *http.Request) {
		localRequests++
		_ = json.NewEncoder(w).Encode(map[string]any{"agents": []map[string]any{
			{"agent_id": "local-partial", "name": "Alpha voice helper", "match_kind": "substring"},
			{"agent_id": "local-exact", "name": "Voice", "match_kind": "exact"},
		}})
	})
	mux.HandleFunc("/v1/federation/available", func(w http.ResponseWriter, _ *http.Request) {
		federatedRequests++
		http.Error(w, "peer unavailable", http.StatusServiceUnavailable)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	result, err := NewServer(ts.URL, key).toolFindAgent(context.Background(), map[string]any{
		"name": "voice", "scope": "local",
	})
	require.NoError(t, err)
	require.Equal(t, 1, localRequests)
	require.Zero(t, federatedRequests)
	out := result.(map[string]any)
	require.Equal(t, []string{"local"}, out["searched"])
	require.Equal(t, true, out["complete"])
	matches := out["matches"].([]map[string]any)
	require.Len(t, matches, 2)
	require.Equal(t, "local-exact", matches[0]["to"])
	require.Equal(t, "local-partial", matches[1]["to"])
}

func TestSageFindAgentRejectsInvalidScopeBeforeAnyRequest(t *testing.T) {
	for _, params := range []map[string]any{
		{"name": "voice", "scope": "unknown"},
		{"name": "voice", "scope": "local", "peer_chain": "remote-chain"},
		{"name": "voice", "scope": "local", "peer_cursor": "next"},
	} {
		_, key, err := ed25519.GenerateKey(nil)
		require.NoError(t, err)
		_, err = NewServer("http://127.0.0.1:1", key).toolFindAgent(context.Background(), params)
		require.ErrorContains(t, err, "scope")
	}
}

func TestSageFindAgentRetainsLocalExactWhenFederationFails(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agents/lookup", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"agents": []map[string]any{{
			"agent_id": "local-voice", "name": "Voice", "match_kind": "exact",
		}}})
	})
	mux.HandleFunc("/v1/federation/available", func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "discovery budget exhausted", http.StatusTooManyRequests)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	result, err := NewServer(ts.URL, key).toolFindAgent(context.Background(), map[string]any{"name": "voice"})
	require.NoError(t, err)
	out := result.(map[string]any)
	require.Equal(t, false, out["complete"])
	require.Equal(t, []string{"local", "federated"}, out["searched"])
	require.NotEmpty(t, out["federated_lookup_error"])
	matches := out["matches"].([]map[string]any)
	require.Len(t, matches, 1)
	require.Equal(t, "local-voice", matches[0]["to"])
}

func TestSageFindAgentLocalExactDoesNotBypassCachedContactReauthorization(t *testing.T) {
	federatedRequests, authorizationRequests := 0, 0
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agents/lookup", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"agents": []map[string]any{{
			"agent_id": "local-voice", "name": "Voice", "match_kind": "exact",
		}}})
	})
	mux.HandleFunc("/v1/federation/available", func(w http.ResponseWriter, _ *http.Request) {
		federatedRequests++
		_ = json.NewEncoder(w).Encode(map[string]any{"connections": []map[string]any{{
			"remote_chain_id": "remote-chain",
			"remote_agents": []map[string]any{{
				"agent_id": "remote-voice", "display_name": "Voice", "address": "remote-voice@remote-chain",
				"available": true, "accepting": true, "domains": []map[string]any{{"domain": "research"}},
			}},
		}}})
	})
	mux.HandleFunc("/v1/federation/contacts/authorize", func(w http.ResponseWriter, _ *http.Request) {
		authorizationRequests++
		http.Error(w, "authorization unavailable", http.StatusServiceUnavailable)
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()
	_, key, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	s := NewServer(ts.URL, key)
	first, err := s.toolFindAgent(context.Background(), map[string]any{"name": "voice"})
	require.NoError(t, err)
	require.Equal(t, 2, first.(map[string]any)["total"])
	second, err := s.toolFindAgent(context.Background(), map[string]any{"name": "voice"})
	require.NoError(t, err)
	out := second.(map[string]any)
	require.Equal(t, false, out["complete"])
	require.NotEmpty(t, out["federated_lookup_error"])
	matches := out["matches"].([]map[string]any)
	require.Len(t, matches, 1)
	require.Equal(t, "local-voice", matches[0]["to"], "stale cached contacts must not be exposed")
	require.Equal(t, 1, federatedRequests)
	require.Positive(t, authorizationRequests)
}
