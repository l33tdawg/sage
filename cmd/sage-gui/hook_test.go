package main

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// withTestSageEnv plants a temporary $HOME, $SAGE_HOME, $SAGE_AGENT_KEY, and
// $SAGE_API_URL so the hook subcommand resolves to the test fixture rather
// than the developer's real machine. Returns the key file path so callers
// can verify signing.
func withTestSageEnv(t *testing.T, apiURL string) string {
	t.Helper()
	tmp := t.TempDir()
	keyPath := filepath.Join(tmp, "agent.key")
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(keyPath, priv.Seed(), 0600))

	t.Setenv("HOME", tmp)
	t.Setenv("SAGE_HOME", tmp)
	t.Setenv("SAGE_AGENT_KEY", keyPath)
	t.Setenv("SAGE_API_URL", apiURL)
	t.Setenv("SAGE_IDENTITY_PATH", "")
	return keyPath
}

func TestHookSessionStart_PrintsMemoriesContextBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/memory/list", r.URL.Path)
		assert.Equal(t, "10", r.URL.Query().Get("limit"))
		assert.Equal(t, "newest", r.URL.Query().Get("sort"))
		assert.Equal(t, "committed", r.URL.Query().Get("status"))
		assert.NotEmpty(t, r.Header.Get("X-Signature"))
		assert.NotEmpty(t, r.Header.Get("X-Agent-ID"))

		_ = json.NewEncoder(w).Encode(map[string]any{
			"memories": []map[string]any{
				{"domain_tag": "sage-development", "memory_type": "fact", "content": "hooks ship via mcp install"},
				{"domain": "session-continuity", "type": "observation", "content": "multi\nline\nrecord"},
			},
		})
	}))
	defer srv.Close()

	withTestSageEnv(t, srv.URL)

	stdout := captureStdout(t, func() {
		require.NoError(t, runHookSessionStart())
	})

	assert.Contains(t, stdout, "SAGE: recent committed memories")
	assert.Contains(t, stdout, "[sage-development/fact] hooks ship via mcp install")
	assert.Contains(t, stdout, "[session-continuity/observation] multi line record",
		"newlines must be flattened to spaces so each memory is one line")
	assert.Contains(t, stdout, "Use sage_recall for targeted retrieval")
}

func TestHookSessionStart_EmptyResultsEmitAck(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"memories": []map[string]any{}})
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)

	stdout := captureStdout(t, func() {
		require.NoError(t, runHookSessionStart())
	})
	assert.Equal(t, "SAGE: connected, no recent memories to surface.\n", stdout)
}

func TestHookSessionStart_NodeUnreachableReturnsError(t *testing.T) {
	withTestSageEnv(t, "http://127.0.0.1:1") // port 1 → connection refused
	err := runHookSessionStart()
	require.Error(t, err, "shell wrapper relies on non-zero exit to fall back to nudge")
}

func TestHookSessionStart_MissingKeyReturnsError(t *testing.T) {
	tmp := t.TempDir()
	t.Setenv("HOME", tmp)
	t.Setenv("SAGE_HOME", tmp)
	t.Setenv("SAGE_AGENT_KEY", "")
	t.Setenv("SAGE_IDENTITY_PATH", "")

	err := runHookSessionStart()
	require.Error(t, err, "no key file → soft-fail back to nudge")
}

func TestHookSessionEnd_PostsLifecycleObservation(t *testing.T) {
	var received struct {
		Content    string   `json:"content"`
		MemoryType string   `json:"memory_type"`
		DomainTag  string   `json:"domain_tag"`
		Confidence float64  `json:"confidence_score"`
		Tags       []string `json:"tags"`
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/v1/memory/submit", r.URL.Path)
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &received))
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)

	// Feed payload via stdin
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	_, _ = w.Write([]byte(`{"session_id":"abc-123","reason":"prompt_input_exit"}`))
	_ = w.Close()
	defer func() { os.Stdin = oldStdin }()

	require.NoError(t, runHookSessionEnd())

	assert.Equal(t, "observation", received.MemoryType)
	assert.Equal(t, "session-lifecycle", received.DomainTag)
	assert.InDelta(t, 0.85, received.Confidence, 0.0001)
	assert.Contains(t, received.Tags, "claude-code")
	assert.Contains(t, received.Tags, "session-end")
	assert.Contains(t, received.Content, "abc-123")
	assert.Contains(t, received.Content, "prompt_input_exit")
}

func TestHookSessionEnd_NoStdinUsesDefaults(t *testing.T) {
	var received map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(body, &received))
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)

	// Empty stdin (closed pipe)
	oldStdin := os.Stdin
	r, w, _ := os.Pipe()
	os.Stdin = r
	_ = w.Close()
	defer func() { os.Stdin = oldStdin }()

	require.NoError(t, runHookSessionEnd())
	content, _ := received["content"].(string)
	assert.Contains(t, content, "unknown")
	assert.Contains(t, content, "ended")
}

func TestHookSignedRequest_HeadersAreVerifiable(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		agentID := r.Header.Get("X-Agent-ID")
		sigHex := r.Header.Get("X-Signature")
		require.NotEmpty(t, agentID)
		require.NotEmpty(t, sigHex)

		pub, err := hex.DecodeString(agentID)
		require.NoError(t, err)
		require.Len(t, pub, ed25519.PublicKeySize)

		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)

	_, err := hookSignedRequest(http.MethodGet, "/v1/memory/list", nil)
	require.NoError(t, err)
}

// captureStdout runs fn while collecting writes to os.Stdout.
func captureStdout(t *testing.T, fn func()) string {
	t.Helper()
	orig := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	done := make(chan struct{})
	var buf bytes.Buffer
	go func() {
		_, _ = io.Copy(&buf, r)
		close(done)
	}()

	fn()
	_ = w.Close()
	os.Stdout = orig
	<-done
	return buf.String()
}

func TestFirstNonEmpty(t *testing.T) {
	assert.Equal(t, "first", firstNonEmpty("first", "second"))
	assert.Equal(t, "second", firstNonEmpty("", "second"))
	assert.Equal(t, "third", firstNonEmpty("", "", "third"))
	assert.Equal(t, "", firstNonEmpty("", "", ""))
}

func TestFlattenLine(t *testing.T) {
	assert.Equal(t, "a b c", flattenLine("a\nb\nc"))
	// \r\n becomes two spaces (each maps to one space) — intentional; we
	// don't collapse runs because callers truncate downstream anyway.
	assert.Equal(t, "a  b c", flattenLine("a\r\nb\rc"))
	assert.Equal(t, "trim me", flattenLine("   trim me   "))
	// Non-ASCII should round-trip cleanly
	assert.Equal(t, "café résumé", flattenLine("café\nrésumé"))
	assert.True(t, strings.Contains(flattenLine("hello"), "hello"))
}
