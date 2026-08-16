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
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
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

func TestHookBaseURLHonorsCustomTLSListener(t *testing.T) {
	t.Setenv("SAGE_API_URL", "")
	t.Setenv("SAGE_HOME", t.TempDir())
	t.Setenv("SAGE_TLS_ADDR", "127.0.0.1:18443")
	assert.Equal(t, "https://127.0.0.1:18443", hookBaseURL())
}

func TestHookSessionStart_PrintsMemoriesContextBlock(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/agent/me" {
			_ = json.NewEncoder(w).Encode(map[string]any{"home_domain": "sage-development", "access_scope": "home_domain"})
			return
		}
		assert.Equal(t, http.MethodGet, r.Method)
		assert.Equal(t, "/v1/memory/list", r.URL.Path)
		assert.Equal(t, "10", r.URL.Query().Get("limit"))
		assert.Equal(t, "newest", r.URL.Query().Get("sort"))
		assert.Equal(t, "committed", r.URL.Query().Get("status"))
		assert.Equal(t, "sage-development", r.URL.Query().Get("domain"))
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
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/agent/me" {
			_ = json.NewEncoder(w).Encode(map[string]any{"home_domain": "caller-home"})
			return
		}
		require.Equal(t, "caller-home", r.URL.Query().Get("domain"))
		_ = json.NewEncoder(w).Encode(map[string]any{"memories": []map[string]any{}})
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)

	stdout := captureStdout(t, func() {
		require.NoError(t, runHookSessionStart())
	})
	assert.Equal(t, "SAGE: connected, no recent memories to surface.\n", stdout)
}

func TestHookSessionStart_DoesNotFallBackToUnscopedList(t *testing.T) {
	var listCalls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/agent/me" {
			_ = json.NewEncoder(w).Encode(map[string]any{"access_scope": "home_domain"})
			return
		}
		listCalls++
		http.Error(w, "must not read another agent's memories", http.StatusInternalServerError)
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)

	stdout := captureStdout(t, func() {
		require.NoError(t, runHookSessionStart())
	})
	require.Zero(t, listCalls)
	assert.Contains(t, stdout, "no caller-scoped home domain")
}

func TestRunHookHelpDoesNotRunSessionStart(t *testing.T) {
	original := os.Args
	t.Cleanup(func() { os.Args = original })
	os.Args = []string{"sage-gui", "hook", "session-start", "--help"}
	stdout := captureStdout(t, func() {
		require.NoError(t, runHook())
	})
	assert.Contains(t, stdout, "Usage: sage-gui hook session-start")
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

func TestHookInboxStatusIsPayloadFreeAndIdentityScoped(t *testing.T) {
	var requestedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.RequestURI()
		assert.NotEmpty(t, r.Header.Get("X-Agent-ID"))
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 2, "unread": true})
	}))
	defer srv.Close()
	keyPath := withTestSageEnv(t, srv.URL)
	seed, err := os.ReadFile(keyPath)
	require.NoError(t, err)
	pub := ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey)

	stdout := captureStdout(t, func() { require.NoError(t, runHookInboxStatus()) })
	assert.Equal(t, "/v1/pipe/history/inbox?count_only=1", requestedPath)
	assert.Contains(t, stdout, "2 unread item(s)")
	assert.Contains(t, stdout, hex.EncodeToString(pub))
	assert.Contains(t, stdout, "Call sage_inbox with a fresh poll")
	assert.NotContains(t, stdout, "sentinel-payload")
	assert.NotContains(t, stdout, "message_id")
}

func TestHookInboxStatusZeroIsSilentAndFailureIsNotZero(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"count": 0, "unread": false})
	}))
	withTestSageEnv(t, srv.URL)
	assert.Empty(t, captureStdout(t, func() { require.NoError(t, runHookInboxStatus()) }))
	srv.Close()
	err := runHookInboxStatus()
	require.Error(t, err)
	assert.NotContains(t, err.Error(), "count 0")
}

func TestHookInboxStatusRejectsIncompleteOrInconsistentProbe(t *testing.T) {
	for _, body := range []string{`{"count":2}`, `{"unread":true}`, `{"count":2,"unread":false}`, `{"count":0,"unread":true}`, `{"count":-1,"unread":false}`} {
		t.Run(body, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = io.WriteString(w, body) }))
			defer srv.Close()
			withTestSageEnv(t, srv.URL)
			err := runHookInboxStatus()
			require.Error(t, err)
			assert.Contains(t, err.Error(), "invalid count probe response")
		})
	}
}

func writeHookTestSeed(t *testing.T, path string) []byte {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Dir(path), 0700))
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	seed := priv.Seed()
	require.NoError(t, os.WriteFile(path, seed, 0600))
	return seed
}

func withHookWorkingDirectory(t *testing.T, path string) {
	t.Helper()
	original, err := os.Getwd()
	require.NoError(t, err)
	require.NoError(t, os.Chdir(path))
	t.Cleanup(func() { require.NoError(t, os.Chdir(original)) })
}

func TestLoadHookSeedNeverFallsBackToOperatorForMissingPinnedIdentity(t *testing.T) {
	home := t.TempDir()
	t.Setenv("HOME", home)
	t.Setenv("SAGE_HOME", home)
	t.Setenv("SAGE_IDENTITY_MODE", "pinned")
	t.Setenv("SAGE_IDENTITY_PATH", filepath.Join(home, "agents", "missing", "agent.key"))
	t.Setenv("SAGE_AGENT_KEY", "")
	writeHookTestSeed(t, filepath.Join(home, "agent.key"))

	_, err := loadHookSeed()
	require.Error(t, err)
	require.Contains(t, err.Error(), "missing")
}

func TestLoadHookSeedNeverFallsBackToOperatorForMalformedPinnedIdentity(t *testing.T) {
	home := t.TempDir()
	pinned := filepath.Join(home, "agents", "malformed", "agent.key")
	require.NoError(t, os.MkdirAll(filepath.Dir(pinned), 0700))
	require.NoError(t, os.WriteFile(pinned, []byte("not-an-ed25519-key"), 0600))
	writeHookTestSeed(t, filepath.Join(home, "agent.key"))
	t.Setenv("HOME", home)
	t.Setenv("SAGE_HOME", home)
	t.Setenv("SAGE_IDENTITY_MODE", "pinned")
	t.Setenv("SAGE_IDENTITY_PATH", pinned)
	t.Setenv("SAGE_AGENT_KEY", "")

	_, err := loadHookSeed()
	require.ErrorContains(t, err, "invalid Ed25519 key length")
}

func TestLoadHookSeedWorkspaceModeMatchesMCPAndNeverBorrowsOperator(t *testing.T) {
	home := t.TempDir()
	project := t.TempDir()
	withHookWorkingDirectory(t, project)
	project, err := os.Getwd()
	require.NoError(t, err)
	t.Setenv("HOME", home)
	t.Setenv("SAGE_HOME", home)
	t.Setenv("SAGE_PROVIDER", "codex")
	t.Setenv("SAGE_PROJECT", "")
	t.Setenv("SAGE_IDENTITY_MODE", "workspace")
	t.Setenv("SAGE_IDENTITY_PATH", filepath.Join(home, "foreign.key"))
	t.Setenv("SAGE_AGENT_KEY", filepath.Join(home, "agent.key"))
	writeHookTestSeed(t, filepath.Join(home, "agent.key"))

	expectedPath := implicitMCPIdentityPath(home, project, "codex", "")
	expectedSeed := writeHookTestSeed(t, expectedPath)
	seed, err := loadHookSeed()
	require.NoError(t, err)
	require.Equal(t, expectedSeed, seed)

	require.NoError(t, os.Remove(expectedPath))
	_, err = loadHookSeed()
	require.Error(t, err, "missing workspace key must soft-fail instead of borrowing Root")
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
		nonceHex := r.Header.Get("X-Nonce")
		require.NotEmpty(t, agentID)
		require.NotEmpty(t, sigHex)
		require.NotEmpty(t, nonceHex)

		pub, err := hex.DecodeString(agentID)
		require.NoError(t, err)
		require.Len(t, pub, ed25519.PublicKeySize)
		nonce, err := hex.DecodeString(nonceHex)
		require.NoError(t, err)
		require.Len(t, nonce, 8)
		sig, err := hex.DecodeString(sigHex)
		require.NoError(t, err)
		ts, err := strconv.ParseInt(r.Header.Get("X-Timestamp"), 10, 64)
		require.NoError(t, err)
		require.WithinDuration(t, time.Now(), time.Unix(ts, 0), 5*time.Second)
		require.True(t, auth.VerifyRequestWithNonce(ed25519.PublicKey(pub), r.Method, r.URL.RequestURI(), nil, ts, nonce, sig))

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

// withStopHookStdin feeds a Stop hook payload on stdin for one call.
func withStopHookStdin(t *testing.T, payload string) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stop-stdin")
	require.NoError(t, err)
	_, err = f.WriteString(payload)
	require.NoError(t, err)
	_, err = f.Seek(0, io.SeekStart)
	require.NoError(t, err)
	original := os.Stdin
	os.Stdin = f
	t.Cleanup(func() { os.Stdin = original; _ = f.Close() })
}

// stopHookNode serves the durable wake snapshot the stop check now reads.
// seq is the monotonic exact-agent sequence; pending means unfinished work
// exists, whether still claimable or held by a claimant session.
func stopHookNode(t *testing.T, seq uint64, pending bool) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "/v1/messages/wake-state", r.URL.Path,
			"the stop check must read the lease-free snapshot, never the SSE route")
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "seq": seq, "pending": pending})
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// The protocol's own loop guard. stop_hook_active means this turn was already
// held open once, so a second block would start a loop the agent cannot exit.
func TestHookStopCheckNeverBlocksTwiceInARow(t *testing.T) {
	withTestSageEnv(t, stopHookNode(t, 3, true))
	t.Setenv("SAGE_STOP_NUDGE", "1")
	withStopHookStdin(t, `{"session_id":"s1","stop_hook_active":true}`)

	stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	assert.Empty(t, stdout, "an already-blocked stop must be allowed to end")
}

// It changes how every session ends, so it stays off until asked for.
func TestHookStopCheckIsOptIn(t *testing.T) {
	withTestSageEnv(t, stopHookNode(t, 3, true))
	withStopHookStdin(t, `{"session_id":"s1"}`)

	stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	assert.Empty(t, stdout, "unset SAGE_STOP_NUDGE must not block anything")

	t.Setenv("SAGE_STOP_NUDGE", "nonsense")
	withStopHookStdin(t, `{"session_id":"s1"}`)
	stdout = captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	assert.Empty(t, stdout, "an unparseable value must not silently enable blocking")
}

func TestHookStopCheckDeniesTheStopWhenWorkIsPending(t *testing.T) {
	withTestSageEnv(t, stopHookNode(t, 2, true))
	t.Setenv("SAGE_STOP_NUDGE", "true")
	withStopHookStdin(t, `{"session_id":"s1"}`)

	stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	require.NotEmpty(t, stdout)

	// Stop uses the TOP-LEVEL decision model. hookSpecificOutput is the shape
	// for PreToolUse and friends; Claude Code ignores it for Stop, so emitting
	// it would block nothing while this test still passed. Assert the exact
	// documented shape, and assert the wrong one is absent.
	var decision struct {
		Decision           string          `json:"decision"`
		Reason             string          `json:"reason"`
		HookSpecificOutput json.RawMessage `json:"hookSpecificOutput"`
	}
	require.NoError(t, json.Unmarshal([]byte(stdout), &decision), "must emit the documented block document")
	assert.Equal(t, "block", decision.Decision, "Stop blocks with decision=block, not deny")
	assert.Empty(t, decision.HookSpecificOutput, "Stop must not use the hookSpecificOutput shape")
	assert.Contains(t, decision.Reason, "unfinished durable work")
	assert.Contains(t, decision.Reason, "untrusted content",
		"the nudge must carry the inbox security boundary with it")
	// Payload-free: the nudge is derived from the wake snapshot alone, so no message
	// identity or body can reach it. (The word "payload" legitimately appears
	// in the security-boundary sentence, so assert on leak SHAPES instead.)
	assert.NotContains(t, stdout, "message_id")
	assert.NotContains(t, stdout, "msg-")
	assert.NotContains(t, stdout, "from_agent")
	assert.NotContains(t, stdout, "intent")
}

// Work the agent has already been told about, and may have deliberately
// declined, must not hold the session open forever. Only NEW work re-blocks.
func TestHookStopCheckDoesNotReTrapOnDeclinedWork(t *testing.T) {
	withTestSageEnv(t, stopHookNode(t, 2, true))
	t.Setenv("SAGE_STOP_NUDGE", "1")

	withStopHookStdin(t, `{"session_id":"s1"}`)
	first := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	require.Contains(t, first, `"decision":"block"`, "the first pending item should nudge once")

	withStopHookStdin(t, `{"session_id":"s1"}`)
	second := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	assert.Empty(t, second, "the same pending work must not nudge the same session twice")
}

func TestHookStopCheckNudgesAgainOnlyWhenNewWorkArrives(t *testing.T) {
	seq := uint64(2)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "seq": seq, "pending": true})
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)
	t.Setenv("SAGE_STOP_NUDGE", "1")

	withStopHookStdin(t, `{"session_id":"s1"}`)
	require.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`)
	withStopHookStdin(t, `{"session_id":"s1"}`)
	require.Empty(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }))

	seq = 5 // a genuinely new message lands
	withStopHookStdin(t, `{"session_id":"s1"}`)
	again := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
	assert.Contains(t, again, `"decision":"block"`, "new work must be able to nudge again")
	assert.Contains(t, again, `"decision":"block"`)

	// A different session starts fresh regardless of the marker.
	withStopHookStdin(t, `{"session_id":"s2"}`)
	assert.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`)
}

// A hook fault must never be able to wedge a session, so every failure allows
// the stop and none of them returns an error to the dispatcher.
func TestHookStopCheckFailsOpen(t *testing.T) {
	t.Setenv("SAGE_STOP_NUDGE", "1")

	t.Run("unreachable node", func(t *testing.T) {
		withTestSageEnv(t, "http://127.0.0.1:1")
		withStopHookStdin(t, `{"session_id":"s1"}`)
		stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
		assert.Empty(t, stdout)
	})

	t.Run("malformed stdin", func(t *testing.T) {
		withTestSageEnv(t, stopHookNode(t, 4, true))
		withStopHookStdin(t, `{not json`)
		stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
		assert.Empty(t, stdout)
	})

	t.Run("inconsistent probe", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			_ = json.NewEncoder(w).Encode(map[string]any{"count": 3, "unread": false})
		}))
		defer srv.Close()
		withTestSageEnv(t, srv.URL)
		withStopHookStdin(t, `{"session_id":"s1"}`)
		stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
		assert.Empty(t, stdout)
	})

	t.Run("no pending work", func(t *testing.T) {
		withTestSageEnv(t, stopHookNode(t, 7, false))
		withStopHookStdin(t, `{"session_id":"s1"}`)
		stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
		assert.Empty(t, stdout)
	})
}

// A subagent finishing is not evidence that the owning host session is idle.
// Nudging it toward sage_inbox could create a SECOND claimant for the same
// agent — the one-handler violation the claimant-session fence exists to stop.
// Guarded in Go, not only in the generated settings, so a hand-wired
// SubagentStop is still silent.
func TestHookStopCheckRefusesAnyEventOtherThanStop(t *testing.T) {
	t.Setenv("SAGE_STOP_NUDGE", "1")
	for _, event := range []string{"SubagentStop", "PreCompact", "Notification"} {
		t.Run(event, func(t *testing.T) {
			withTestSageEnv(t, stopHookNode(t, 4, true))
			withStopHookStdin(t, `{"session_id":"s1","hook_event_name":"`+event+`"}`)
			stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
			assert.Empty(t, stdout, event+" must never decline a stop")
		})
	}

	// The main event still nudges, so the guard is not simply refusing everything.
	withTestSageEnv(t, stopHookNode(t, 4, true))
	withStopHookStdin(t, `{"session_id":"s1","hook_event_name":"Stop"}`)
	assert.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`)
}

// Without a session identity the per-session marker cannot be attributed.
// Writing it anyway stores malformed state that can re-nudge forever, so the
// check must fail open and leave no marker behind.
func TestHookStopCheckFailsOpenWhenSessionIDIsMissing(t *testing.T) {
	t.Setenv("SAGE_STOP_NUDGE", "1")
	for name, payload := range map[string]string{
		"absent": `{"hook_event_name":"Stop"}`,
		"empty":  `{"session_id":"","hook_event_name":"Stop"}`,
		"blank":  `{"session_id":"   ","hook_event_name":"Stop"}`,
	} {
		t.Run(name, func(t *testing.T) {
			home := t.TempDir()
			t.Setenv("SAGE_HOME", home)
			withTestSageEnv(t, stopHookNode(t, 3, true))
			t.Setenv("SAGE_HOME", home)
			withStopHookStdin(t, payload)

			stdout := captureStdout(t, func() { require.NoError(t, runHookStopCheck()) })
			assert.Empty(t, stdout, "a payload with no usable session id must allow the stop")
			_, statErr := os.Stat(filepath.Join(home, stopNudgeStateFile))
			assert.True(t, os.IsNotExist(statErr), "no marker may be written without a session identity")
		})
	}
}

// BLOCKER 1 REGRESSION. The old check compared an unread COUNT against a stored
// high-water mark, which is a level rather than a generation: nudge at 5, let
// those five be handled, then one genuinely new message arrives and 1 <= 5, so
// the new work never nudges at all. A monotonic durable sequence has no such
// hole. This drives exactly that sequence and requires the nudge to fire.
func TestHookStopCheckNudgesWhenWorkShrinksButSequenceAdvances(t *testing.T) {
	seq := uint64(5)
	pending := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "seq": seq, "pending": pending})
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)
	t.Setenv("SAGE_STOP_NUDGE", "1")

	// Five items pending; nudge once.
	withStopHookStdin(t, `{"session_id":"s1","hook_event_name":"Stop"}`)
	require.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`)

	// All five handled: nothing unfinished, so nothing to say.
	pending = false
	withStopHookStdin(t, `{"session_id":"s1","hook_event_name":"Stop"}`)
	require.Empty(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }))

	// ONE new message arrives. Under a count high-water mark this is 1 <= 5 and
	// would be silently swallowed; under a monotonic sequence it is 6 > 5.
	seq, pending = 6, true
	withStopHookStdin(t, `{"session_id":"s1","hook_event_name":"Stop"}`)
	assert.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`,
		"newer durable work must nudge even though less of it is outstanding")
}

// A row stranded by a dead claimant session keeps pending true at an unchanged
// sequence. A FRESH session has never seen that sequence, so it must be told;
// the session that already saw it must not be re-trapped.
func TestHookStopCheckSurfacesStrandedWorkToAFreshSessionOnly(t *testing.T) {
	withTestSageEnv(t, stopHookNode(t, 9, true))
	t.Setenv("SAGE_STOP_NUDGE", "1")

	withStopHookStdin(t, `{"session_id":"dead-session","hook_event_name":"Stop"}`)
	require.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`)
	withStopHookStdin(t, `{"session_id":"dead-session","hook_event_name":"Stop"}`)
	require.Empty(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }),
		"the session already told must not be re-trapped at the same sequence")

	// A restart: new session id, same durable sequence, work still unfinished.
	withStopHookStdin(t, `{"session_id":"fresh-session","hook_event_name":"Stop"}`)
	assert.Contains(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }), `"decision":"block"`,
		"a fresh runtime must learn about work stranded at an unchanged sequence")
}

// An unrecognised wake contract version must not be guessed at.
func TestHookStopCheckIgnoresAnUnknownWakePayloadVersion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 99, "seq": 4, "pending": true})
	}))
	defer srv.Close()
	withTestSageEnv(t, srv.URL)
	t.Setenv("SAGE_STOP_NUDGE", "1")
	withStopHookStdin(t, `{"session_id":"s1","hook_event_name":"Stop"}`)
	assert.Empty(t, captureStdout(t, func() { require.NoError(t, runHookStopCheck()) }))
}
