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

// healthServer answers /v1/dashboard/health with a body the caller supplies,
// which is all this tool reads. It is deliberately not the whole dashboard
// fixture: the point of the tool is what it does with the fence block, not how
// much of /health it can parse.
func healthServer(t *testing.T, status int, body map[string]any) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/dashboard/health" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		if body != nil {
			_ = json.NewEncoder(w).Encode(body)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func nodeHealthFor(t *testing.T, server *httptest.Server) map[string]any {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	result, err := NewServer(server.URL, priv).toolNodeHealth(context.Background(), map[string]any{})
	require.NoError(t, err)
	out, ok := result.(map[string]any)
	require.True(t, ok, "the tool must answer with an object: %T", result)
	return out
}

// TestNodeHealthReportsNoFence is the healthy case an agent should see when a
// write succeeded or was refused for some other reason: the fence is not what
// is stopping it, and the guidance must not imply one is held.
func TestNodeHealthReportsNoFence(t *testing.T) {
	server := healthServer(t, http.StatusOK, map[string]any{
		"version": "11.23.10", "boot_id": "boot-1", "uptime": "1m0s",
		"signer_fences": map[string]any{"active": 0, "oldest_age_seconds": 0},
	})
	out := nodeHealthFor(t, server)

	require.Equal(t, "11.23.10", out["version"])
	fences, ok := out["signer_fences"].(map[string]any)
	require.True(t, ok, "the fence block must be forwarded: %v", out["signer_fences"])
	require.Equal(t, float64(0), fences["active"])
	guidance, _ := out["signer_fence_guidance"].(string)
	require.Contains(t, guidance, "no signing key is fenced")
}

// TestNodeHealthExplainsARestoredFence is the field case: a restored fence
// cannot clear itself, and an agent that treats it as pending waits forever.
func TestNodeHealthExplainsARestoredFence(t *testing.T) {
	server := healthServer(t, http.StatusOK, map[string]any{
		"version": "11.23.10",
		"signer_fences": map[string]any{
			"active": 1, "oldest_age_seconds": 212,
			"explanation": "one or more signing keys are waiting for proof of an earlier submission's fate",
			"signers": []map[string]any{{
				"signer": "3d73cdbdffaacac7", "tx_hash": "AB", "nonce": 41,
				"held_seconds": 212, "cause": "restored_from_durable_intent",
				"resolution": "proof_or_operator", "last_cause": "no_proof",
				"last_detail": "neither proof holds yet",
			}},
		},
	})
	out := nodeHealthFor(t, server)

	guidance, _ := out["signer_fence_guidance"].(string)
	require.Contains(t, guidance, "proof_or_operator")
	require.Contains(t, guidance, "will NOT clear on its own",
		"an agent must be told that waiting is not a recovery for this class")
	require.Contains(t, guidance, "operator abandon")
}

// TestNodeHealthExplainsALiveFence is the other half of the same distinction:
// this fence IS being worked on by the node, and the honest advice is to wait
// rather than to escalate or to retry the write.
func TestNodeHealthExplainsALiveFence(t *testing.T) {
	server := healthServer(t, http.StatusOK, map[string]any{
		"signer_fences": map[string]any{
			"active": 1,
			"signers": []map[string]any{{
				"signer": "3d73cdbdffaacac7", "cause": "rpc", "resolution": "reconciling",
			}},
		},
	})
	out := nodeHealthFor(t, server)

	guidance, _ := out["signer_fence_guidance"].(string)
	require.Contains(t, guidance, "reconciling")
	require.Contains(t, guidance, "clears itself")
	require.NotContains(t, guidance, "proof_or_operator")
}

// TestNodeHealthSaysWhenTheNodeCannotAnswer pins the honesty rule: a node too
// old to report a resolution class, and a node that hides per-fence rows from
// this caller, must both produce "not knowable from here" rather than a
// confident guess. A wrong diagnosis here is worse than no diagnosis, because
// it tells an agent to wait for a fence that will never lift.
func TestNodeHealthSaysWhenTheNodeCannotAnswer(t *testing.T) {
	t.Run("no resolution class (older node)", func(t *testing.T) {
		server := healthServer(t, http.StatusOK, map[string]any{
			"signer_fences": map[string]any{
				"active": 1,
				"signers": []map[string]any{{
					"signer": "3d73cdbdffaacac7", "cause": "rpc",
				}},
			},
		})
		out := nodeHealthFor(t, server)
		guidance, _ := out["signer_fence_guidance"].(string)
		require.Contains(t, guidance, "did not report a resolution class")
		require.NotContains(t, guidance, "clears itself")
	})

	t.Run("per-fence detail withheld", func(t *testing.T) {
		server := healthServer(t, http.StatusOK, map[string]any{
			"signer_fences": map[string]any{"active": 2, "oldest_age_seconds": 90},
		})
		out := nodeHealthFor(t, server)
		guidance, _ := out["signer_fence_guidance"].(string)
		require.Contains(t, guidance, "did not disclose per-fence detail")
	})
}

// TestNodeHealthNamesAnOlderNodeWithoutTheRoute is the capability gap: a build
// with no health surface must produce an error that says so, because "404" on
// its own reads like a bug in the caller.
func TestNodeHealthNamesAnOlderNodeWithoutTheRoute(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(http.StatusNotFound)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"title": "Not found", "status": http.StatusNotFound, "detail": "no such route",
		})
	}))
	t.Cleanup(server.Close)

	_, err = NewServer(server.URL, priv).toolNodeHealth(context.Background(), map[string]any{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not expose the health surface")
}

func TestNodeHealthRejectsAnOutOfRangeTimeout(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer("http://127.0.0.1:1", priv)

	for _, value := range []any{0, 31} {
		_, err := server.toolNodeHealth(context.Background(), map[string]any{"timeout_seconds": value})
		require.Error(t, err)
		require.Contains(t, err.Error(), "timeout_seconds must be between 1 and 30")
	}
}
