package mcp

// Tests for the stale-session auto-heal on memory submit (see
// submitMemoryResilient / isStaleSessionErr in server.go). These reproduce the
// failure mode where a SAGE node restarted (e.g. an in-place v8.x upgrade) out
// from under a live MCP session and every sage_turn store returned a bare
// "Broadcast error: access denied" until the human ran /mcp by hand.

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsStaleSessionErr(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"broadcast access denied", errString("Broadcast error: access denied"), true},
		{"identity verification", errString("agent identity verification failed"), true},
		{"connection refused", errString("API error (HTTP 502): dial tcp: connection refused"), false},
		{"connection reset", errString("Post: read: connection reset by peer"), false},
		{"unexpected eof", errString("read response: unexpected EOF"), false},
		// The REST layer stamps the title "Broadcast error" on EVERY consensus
		// rejection, so a permanent application reject (e.g. a content-schema
		// reject) reaches the client as "Broadcast error: request rejected". That
		// must NOT trigger a re-handshake — even though "Broadcast error: access
		// denied" (a genuine restart symptom) must, via the "access denied" signature.
		{"wrapped content reject", errString("Broadcast error: request rejected"), false},
		// Genuine application rejections must NOT trigger a re-handshake — they
		// would just fail again and waste a register + two retries.
		{"dedup", errString("quality validator: duplicate content hash"), false},
		{"too short", errString("content below quality threshold"), false},
		{"bad domain", errString("domain tag is required"), false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, isStaleSessionErr(c.err))
		})
	}
}

func TestSubmitMemoryResilient_DoesNotRepeatAmbiguousRetry(t *testing.T) {
	withFastBackoffs(t)

	var submits, registers atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/register", func(w http.ResponseWriter, _ *http.Request) {
		registers.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "registered"})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, _ *http.Request) {
		switch submits.Add(1) {
		case 1:
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(map[string]any{"title": "Broadcast error", "detail": "access denied"})
		case 2:
			// Model a commit followed by a lost HTTP response. The client sees EOF,
			// so delivery is ambiguous and a third POST would duplicate the memory.
			hj, ok := w.(http.Hijacker)
			require.True(t, ok)
			conn, _, err := hj.Hijack()
			require.NoError(t, err)
			_ = conn.Close()
		default:
			t.Errorf("ambiguous submit was repeated")
		}
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)
	err := s.submitMemoryResilient(context.Background(), []byte(`{"content":"x"}`), &map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "may have reached SAGE")
	assert.EqualValues(t, 2, submits.Load())
	assert.EqualValues(t, 1, registers.Load())
}

// errString is a tiny error wrapper so the table above reads cleanly.
type errString string

func (e errString) Error() string { return string(e) }

// withFastBackoffs shrinks the retry schedule so tests don't sleep, restoring
// the production schedule afterwards.
func withFastBackoffs(t *testing.T) {
	t.Helper()
	orig := submitRehealBackoffs
	submitRehealBackoffs = []time.Duration{0, 0}
	t.Cleanup(func() { submitRehealBackoffs = orig })
}

func TestSubmitMemoryResilient_HealsAfterRestart(t *testing.T) {
	withFastBackoffs(t)

	var submits, registers atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/register", func(w http.ResponseWriter, r *http.Request) {
		registers.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"status": "registered"})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		// First attempt mimics the node mid-restart: the signed write reaches
		// the node but is rejected at the access layer. Subsequent attempts (after
		// the agent re-registers) succeed.
		if submits.Add(1) == 1 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]any{"title": "Broadcast error", "detail": "access denied"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"memory_id": "m1", "status": "proposed"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	var out struct {
		MemoryID string `json:"memory_id"`
		Status   string `json:"status"`
	}
	err := s.submitMemoryResilient(context.Background(), []byte(`{"content":"x"}`), &out)
	require.NoError(t, err)
	assert.Equal(t, "m1", out.MemoryID)
	assert.EqualValues(t, 2, submits.Load(), "should retry the submit once after re-handshake")
	assert.EqualValues(t, 1, registers.Load(), "should re-register exactly once before retrying")
}

func TestSubmitMemoryResilient_FirstAttemptSuccessNoReheal(t *testing.T) {
	withFastBackoffs(t)

	var submits, registers atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/register", func(w http.ResponseWriter, r *http.Request) {
		registers.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"status": "registered"})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		submits.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"memory_id": "m1", "status": "proposed"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	err := s.submitMemoryResilient(context.Background(), []byte(`{"content":"x"}`), &map[string]any{})
	require.NoError(t, err)
	assert.EqualValues(t, 1, submits.Load(), "happy path must not retry")
	assert.EqualValues(t, 0, registers.Load(), "happy path must not re-register")
}

func TestSubmitMemoryResilient_PermanentDenialBoundedWithHint(t *testing.T) {
	withFastBackoffs(t)

	var submits, registers atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/register", func(w http.ResponseWriter, r *http.Request) {
		registers.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"status": "registered"})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		submits.Add(1)
		w.WriteHeader(http.StatusForbidden)
		json.NewEncoder(w).Encode(map[string]any{"title": "Broadcast error", "detail": "access denied"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	err := s.submitMemoryResilient(context.Background(), []byte(`{"content":"x"}`), &map[string]any{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "access denied", "underlying error preserved")
	assert.Contains(t, err.Error(), "/mcp", "actionable reconnect hint surfaced")
	// 1 initial attempt + len(submitRehealBackoffs) retries — bounded, no loop.
	assert.EqualValues(t, 3, submits.Load())
	assert.EqualValues(t, 1, registers.Load(), "re-register attempted exactly once")
}

// TestStoreMemory_HealsAfterRestart drives the SHARED storeMemory helper — the
// path that sage_turn / sage_reflect / inception actually use — through a node
// restart, proving the re-heal reaches them and not just sage_remember's own
// inline submit. This is the regression guard for the call-site the first cut of
// the fix missed (the swap was made in toolRemember, not storeMemory).
func TestStoreMemory_HealsAfterRestart(t *testing.T) {
	withFastBackoffs(t)

	var submits, registers atomic.Int32
	mux := http.NewServeMux()
	// Pre-validate is optional; a 404 makes storeMemory fall through to submit.
	mux.HandleFunc("/v1/memory/pre-validate", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	mux.HandleFunc("/v1/embed", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"embedding": []float32{0.1, 0.2, 0.3}})
	})
	mux.HandleFunc("/v1/agent/register", func(w http.ResponseWriter, r *http.Request) {
		registers.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"status": "registered"})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		// Mimic the node mid-restart on the first store, healthy thereafter.
		if submits.Add(1) == 1 {
			w.WriteHeader(http.StatusForbidden)
			json.NewEncoder(w).Encode(map[string]any{"title": "Broadcast error", "detail": "access denied"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"memory_id": "m1", "status": "proposed"})
	})
	ts := httptest.NewServer(mux)
	defer ts.Close()

	_, priv, _ := ed25519.GenerateKey(nil)
	s := NewServer(ts.URL, priv)

	// storeMemory backs sage_turn's per-turn observation store — the every-turn
	// path that originally surfaced the bare "Broadcast error: access denied".
	_, err := s.storeMemory(context.Background(), "an observation worth keeping", "sage-debugging", "observation", 0.80)
	require.NoError(t, err)
	assert.EqualValues(t, 2, submits.Load(), "storeMemory should retry the submit after re-handshake")
	assert.EqualValues(t, 1, registers.Load(), "storeMemory should re-register once on the stale-session error")
}
