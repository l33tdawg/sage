package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// captureOrgTokenStdout runs fn with os.Stdout redirected and returns what it printed.
func captureOrgTokenStdout(t *testing.T, fn func() error) (string, error) {
	t.Helper()
	old := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w
	runErr := fn()
	os.Stdout = old
	require.NoError(t, w.Close())
	out, err := io.ReadAll(r)
	require.NoError(t, err)
	return string(out), runErr
}

// orgTokenNode points the CLI at a fake node and a throwaway signing key, and
// returns the key's agent id. Every request the handler sees must be signed.
func orgTokenNode(t *testing.T, handler http.HandlerFunc) string {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	seed := make([]byte, ed25519.SeedSize)
	seed[0] = 7
	keyPath := filepath.Join(t.TempDir(), "agent.key")
	require.NoError(t, os.WriteFile(keyPath, seed, 0o600))

	t.Setenv("SAGE_HOME", filepath.Join(t.TempDir(), "home"))
	t.Setenv("SAGE_AGENT_KEY", keyPath)
	t.Setenv("SAGE_API_URL", srv.URL)
	return hex.EncodeToString(ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey))
}

func TestOrgTokenCreateHelpDoesNotMint(t *testing.T) {
	orgTokenNode(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("help must not call the node, got %s %s", r.Method, r.URL.Path)
	})
	out, err := captureOrgTokenStdout(t, func() error { return runOrgTokenCreate([]string{"--org", "o", "--help"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "sage-gui org-token create")
}

func TestOrgTokenCreateValidatesBeforeCalling(t *testing.T) {
	orgTokenNode(t, func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("invalid input must not call the node, got %s %s", r.Method, r.URL.Path)
	})
	for name, args := range map[string][]string{
		"unknown flag":   {"--org", "o", "--node", "n", "--signer", "s", "--bogus", "x"},
		"missing org":    {"--node", "n", "--signer", "s"},
		"missing node":   {"--org", "o", "--signer", "s"},
		"missing signer": {"--org", "o", "--node", "n"},
		"dangling flag":  {"--org"},
		"stray arg":      {"--org", "o", "--node", "n", "--signer", "s", "extra"},
	} {
		t.Run(name, func(t *testing.T) {
			require.Error(t, runOrgTokenCreate(args))
		})
	}
}

func TestOrgTokenCreateSignedRequest(t *testing.T) {
	var gotPath, gotAgent string
	var gotBody map[string]string
	agentID := orgTokenNode(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.Method + " " + r.URL.Path
		gotAgent = r.Header.Get("X-Agent-ID")
		_ = json.NewDecoder(r.Body).Decode(&gotBody)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"id":"tok-1","org_id":"org-x","node_label":"edge-7","role":"member","token":"PLAINTEXT-ONCE","issued_at":"2026-10-18T00:00:00Z","use_hint":"hint"}`))
	})

	out, err := captureOrgTokenStdout(t, func() error {
		return runOrgTokenCreate([]string{"--org", "org-x", "--node=edge-7", "--signer", "ab12", "--epoch", "e1"})
	})
	require.NoError(t, err)
	assert.Equal(t, "POST /v1/org/org-x/enroll-tokens", gotPath)
	assert.Equal(t, agentID, gotAgent, "request must be signed by the CLI agent key")
	assert.Equal(t, map[string]string{"node_label": "edge-7", "role": "", "org_signer_pub": "ab12", "key_epoch": "e1"}, gotBody)
	assert.Contains(t, out, "PLAINTEXT-ONCE")
	assert.Contains(t, out, "tok-1")
}

func TestOrgTokenRevokeAndList(t *testing.T) {
	var calls []string
	orgTokenNode(t, func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.Path)
		switch r.Method {
		case http.MethodDelete:
			if r.URL.Path == "/v1/org/org-x/enroll-tokens/missing" {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)
		case http.MethodGet:
			_, _ = w.Write([]byte(`{"tokens":[{"id":"tok-1","node_label":"edge-7","role":"member","issued_at":"2026-10-18T00:00:00Z","revoked_at":"2026-10-18T01:00:00Z"}]}`))
		}
	})

	_, err := captureOrgTokenStdout(t, func() error { return runOrgTokenRevoke([]string{"--org", "org-x", "tok-1"}) })
	require.NoError(t, err)
	_, err = captureOrgTokenStdout(t, func() error { return runOrgTokenRevoke([]string{"--org", "org-x", "missing"}) })
	require.ErrorContains(t, err, "not found")
	require.Error(t, runOrgTokenRevoke([]string{"tok-1"}), "revoke requires --org")

	out, err := captureOrgTokenStdout(t, func() error { return runOrgTokenList([]string{"--org", "org-x"}) })
	require.NoError(t, err)
	assert.Contains(t, out, "edge-7")
	assert.Contains(t, out, "revoked")

	assert.Equal(t, []string{
		"DELETE /v1/org/org-x/enroll-tokens/tok-1",
		"DELETE /v1/org/org-x/enroll-tokens/missing",
		"GET /v1/org/org-x/enroll-tokens",
	}, calls)
}
