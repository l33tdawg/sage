package main

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type submittedMemory struct {
	MemoryID       string
	Content        string
	Classification int
	Tags           []string
	Status         string
	submitIdx      int // 1-based submit ordinal, for rejection control in tests
}

// mockNode is a minimal SAGE REST node for the never-compact E2E tests.
type mockNode struct {
	mu       sync.Mutex
	stored   []submittedMemory
	submits  int
	forgets  int
	nextID   int
	homeDom  string
	forceCmt bool
	// rejectIf, when set, makes the memory-detail GET report a memory as terminal
	// rejected instead of committed, keyed by its 1-based submit ordinal and content.
	// Used to exercise the rejected-span lifecycle (e.g. reject content but commit a
	// benign gap marker).
	rejectIf func(submitIdx int, content string) bool
}

func newMockNode() *mockNode { return &mockNode{homeDom: "home.domain"} }

func (m *mockNode) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/agent/me", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{"home_domain": m.homeDom})
	})
	mux.HandleFunc("/v1/memory/submit", func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		var req struct {
			Content        string   `json:"content"`
			Classification int      `json:"classification"`
			Tags           []string `json:"tags"`
		}
		_ = json.Unmarshal(raw, &req)
		m.mu.Lock()
		m.submits++
		m.nextID++
		id := fmt.Sprintf("mem-%d", m.nextID)
		status := "proposed"
		if m.forceCmt {
			status = "committed"
		}
		m.stored = append(m.stored, submittedMemory{id, req.Content, req.Classification, req.Tags, status, m.submits})
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"memory_id": id, "status": status, "committed": true})
	})
	mux.HandleFunc("/v1/memory/list", func(w http.ResponseWriter, r *http.Request) {
		tag := r.URL.Query().Get("tag")
		statusFilter := r.URL.Query().Get("status")
		m.mu.Lock()
		var out []map[string]any
		for _, s := range m.stored {
			if !hasTag(s.Tags, tag) {
				continue
			}
			if statusFilter == "committed" && s.Status != "committed" {
				continue
			}
			out = append(out, map[string]any{"memory_id": s.MemoryID, "content": s.Content, "status": s.Status})
		}
		m.mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"memories": out})
	})
	mux.HandleFunc("/v1/memory/", func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/v1/memory/")
		if strings.HasSuffix(id, "/forget") {
			id = strings.TrimSuffix(id, "/forget")
			m.mu.Lock()
			m.forgets++
			for i := range m.stored {
				if m.stored[i].MemoryID == id {
					m.stored[i].Status = "deprecated"
				}
			}
			m.mu.Unlock()
			_ = json.NewEncoder(w).Encode(map[string]any{"status": "deprecated"})
			return
		}
		m.mu.Lock()
		var found bool
		outStatus := "committed"
		for i := range m.stored {
			if m.stored[i].MemoryID == id {
				if m.rejectIf != nil && m.rejectIf(m.stored[i].submitIdx, m.stored[i].Content) {
					m.stored[i].Status = "rejected"
					outStatus = "rejected"
				} else {
					m.stored[i].Status = "committed"
				}
				found = true
			}
		}
		m.mu.Unlock()
		if !found {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": outStatus})
	})
	return mux
}

// committedUnits decodes every committed chunk into units, ordered/deduped.
func (m *mockNode) committedUnits() []captureUnit {
	m.mu.Lock()
	defer m.mu.Unlock()
	seen := map[[2]int]bool{}
	var units []captureUnit
	for _, s := range m.stored {
		if s.Status != "committed" {
			continue
		}
		for _, u := range decodeChunkUnits(s.Content) {
			k := [2]int{u.Seq, u.Part}
			if seen[k] {
				continue
			}
			seen[k] = true
			units = append(units, u)
		}
	}
	sort.SliceStable(units, func(i, j int) bool {
		if units[i].Seq != units[j].Seq {
			return units[i].Seq < units[j].Seq
		}
		return units[i].Part < units[j].Part
	})
	return units
}

func hasTag(tags []string, want string) bool {
	if want == "" {
		return true
	}
	for _, t := range tags {
		if t == want {
			return true
		}
	}
	return false
}

func setupNeverCompactEnv(t *testing.T, node *mockNode) (root string) {
	t.Helper()
	home := t.TempDir()
	root = filepath.Join(home, "projects")
	require.NoError(t, os.MkdirAll(root, 0o700))
	seed := make([]byte, ed25519.SeedSize)
	_, _ = rand.Read(seed)
	keyPath := filepath.Join(home, "agent.key")
	require.NoError(t, os.WriteFile(keyPath, seed, 0o600))
	srv := httptest.NewServer(node.handler())
	t.Cleanup(srv.Close)
	t.Setenv("SAGE_HOME", home)
	t.Setenv("SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT", root)
	t.Setenv("SAGE_IDENTITY_PATH", keyPath)
	t.Setenv("SAGE_API_URL", srv.URL)
	t.Setenv("SAGE_NEVERCOMPACT", "1")
	return root
}

// writeTranscriptFile writes/overwrites a transcript with the given user turns.
func writeTranscriptFile(t *testing.T, root, session string, turns []string) string {
	t.Helper()
	lines := make([]string, 0, len(turns))
	for _, txt := range turns {
		lines = append(lines, rowJSON(session, "user", txt))
	}
	path := filepath.Join(root, session+".jsonl")
	require.NoError(t, os.WriteFile(path, []byte(strings.Join(lines, "\n")), 0o600))
	return path
}

func feedStdin(t *testing.T, payload string, fn func()) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "stdin")
	require.NoError(t, err)
	_, _ = f.WriteString(payload)
	_, _ = f.Seek(0, 0)
	old := os.Stdin
	os.Stdin = f
	defer func() { os.Stdin = old; f.Close() }()
	fn()
}

func payloadFor(session, path string) string {
	return fmt.Sprintf(`{"session_id":%q,"transcript_path":%q}`, session, path)
}

// ── happy path: capture + classification + tags + verbatim ──────────────────

func TestPreCompactCaptureVerbatimAndTags(t *testing.T) {
	node := newMockNode()
	node.forceCmt = true
	root := setupNeverCompactEnv(t, node)
	path := writeTranscriptFile(t, root, "sess-1", []string{"hello there", "second message"})

	feedStdin(t, payloadFor("sess-1", path), func() { _ = runHookPreCompact() })

	node.mu.Lock()
	require.NotZero(t, node.submits, "capture must submit at least one chunk")
	for _, s := range node.stored {
		require.Equal(t, neverCompactDefaultClassification, s.Classification)
		require.True(t, hasTag(s.Tags, neverCompactTag) && hasTag(s.Tags, neverCompactThreadTagPrefix+"sess-1"))
	}
	node.mu.Unlock()

	turns := reconstructTurns(node.committedUnits())
	require.Len(t, turns, 2)
	require.Equal(t, "hello there", turns[0].text)
	require.Equal(t, "second message", turns[1].text)
}

func TestPreCompactNoCaptureWithoutConsent(t *testing.T) {
	node := newMockNode()
	root := setupNeverCompactEnv(t, node)
	t.Setenv("SAGE_NEVERCOMPACT", "")
	path := writeTranscriptFile(t, root, "sess-2", []string{"private"})
	feedStdin(t, payloadFor("sess-2", path), func() { _ = runHookPreCompact() })
	node.mu.Lock()
	defer node.mu.Unlock()
	require.Zero(t, node.submits, "no capture without consent")
}

// ── blocker 1: long transcripts — the whole tail is eventually committed ─────

func TestTailEventuallyCommitted(t *testing.T) {
	node := newMockNode()
	node.forceCmt = true
	root := setupNeverCompactEnv(t, node)
	t.Setenv("SAGE_NEVERCOMPACT_MAX_UNITS", "2") // tiny per-invocation cap

	const n = 11
	turns := make([]string, n)
	for i := range turns {
		turns[i] = fmt.Sprintf("turn-%02d", i+1)
	}
	path := writeTranscriptFile(t, root, "sess-tail", turns)
	payload := payloadFor("sess-tail", path)

	// Repeated PreCompact invocations must eventually commit every row, not loop on
	// the first 2 forever.
	for i := 0; i < n+3; i++ {
		feedStdin(t, payload, func() { _ = runHookPreCompact() })
	}
	got := reconstructTurns(node.committedUnits())
	require.Len(t, got, n, "every eligible row must eventually be committed")
	for i := range turns {
		require.Equal(t, turns[i], got[i].text)
	}
}

// ── rejected-span lifecycle: forward progress + bounded abandonment ──────────

// TestRejectedSpanResubmitsAndProgresses proves a span whose governed memory ends
// terminal-rejected is RESUBMITTED on the next invocation (not re-adopted forever by
// its deterministic tag) and then reaches a committed outcome. Reproduces the round-4
// blocker: cursor rollback alone left the submit count at 1.
func TestRejectedSpanResubmitsAndProgresses(t *testing.T) {
	node := newMockNode()
	node.rejectIf = func(submitIdx int, _ string) bool { return submitIdx == 1 } // only the first submit is rejected
	root := setupNeverCompactEnv(t, node)
	path := writeTranscriptFile(t, root, "sess-rej", []string{"only turn"})
	payload := payloadFor("sess-rej", path)

	// Invocation 1: submit #1 is transitioned to rejected.
	feedStdin(t, payload, func() { _ = runHookPreCompact() })
	node.mu.Lock()
	require.Equal(t, 1, node.submits, "first invocation submits once")
	require.Equal(t, "rejected", node.stored[0].Status, "the chunk's memory ends terminal-rejected")
	node.mu.Unlock()

	// Invocation 2: the rejected span must resubmit (forward progress) rather than
	// re-adopt the terminal-rejected tag forever. Submit #2 is not rejected → it commits.
	feedStdin(t, payload, func() { _ = runHookPreCompact() })
	node.mu.Lock()
	require.Greater(t, node.submits, 1, "the rejected span is resubmitted, not stuck re-adopting the rejected tag")
	node.mu.Unlock()

	turns := reconstructTurns(node.committedUnits())
	require.Len(t, turns, 1, "the resubmitted span reaches a committed outcome")
	require.Equal(t, "only turn", turns[0].text)
}

// TestAbandonedSpanRetriesGapUntilCommittedAndVisible proves the bounded-abandonment
// fallback never silently loses the span, addressing the fire-and-forget hole:
//   - while governance rejects even the benign gap marker, NOTHING is committed and the
//     cursor does not advance past the span (the bytes are retained, not skipped);
//   - once governance accepts the gap marker, the retained span surfaces as exactly one
//     committed, recall-visible gap.
func TestAbandonedSpanRetriesGapUntilCommittedAndVisible(t *testing.T) {
	node := newMockNode()
	// Phase 1: reject everything, including the gap marker.
	node.rejectIf = func(int, string) bool { return true }
	root := setupNeverCompactEnv(t, node)
	path := writeTranscriptFile(t, root, "sess-gap", []string{"cursed turn"})
	payload := payloadFor("sess-gap", path)

	// Drive past the content retry cap and several more invocations: the span switches to
	// gap mode and keeps retrying the (rejected) gap marker.
	for i := 0; i < preCompactMaxChunkRetries+4; i++ {
		feedStdin(t, payload, func() { _ = runHookPreCompact() })
	}
	require.Empty(t, node.committedUnits(),
		"while the gap marker is rejected, nothing is committed and the span is not silently advanced")
	node.mu.Lock()
	var attemptedGap bool
	for _, s := range node.stored {
		if strings.Contains(s.Content, "capture gap") {
			attemptedGap = true
		}
	}
	node.mu.Unlock()
	require.True(t, attemptedGap, "a gap marker is submitted for the abandoned span")

	// Phase 2: governance now accepts the benign gap marker (still rejects real content).
	node.rejectIf = func(_ int, content string) bool { return !strings.Contains(content, "capture gap") }
	for i := 0; i < 3; i++ {
		feedStdin(t, payload, func() { _ = runHookPreCompact() })
	}
	var gapUnits int
	for _, u := range node.committedUnits() {
		if u.Role == unitRoleGap && strings.Contains(u.Text, "capture gap") {
			gapUnits++
		}
	}
	require.Equal(t, 1, gapUnits,
		"once governance accepts the gap marker, the retained span surfaces as one committed, recall-visible gap")
}

// ── blocker 2: appending never creates overlapping/duplicate chunks ─────────

func TestAppendNoOverlap(t *testing.T) {
	node := newMockNode()
	node.forceCmt = true
	root := setupNeverCompactEnv(t, node)

	// Run 1: two turns.
	path := writeTranscriptFile(t, root, "sess-app", []string{"alpha", "beta"})
	payload := payloadFor("sess-app", path)
	feedStdin(t, payload, func() { _ = runHookPreCompact() })

	// Append a third turn (transcript grows) and run again.
	writeTranscriptFile(t, root, "sess-app", []string{"alpha", "beta", "gamma"})
	feedStdin(t, payload, func() { _ = runHookPreCompact() })

	// Every (seq,part) must appear exactly once across all committed chunks.
	node.mu.Lock()
	counts := map[[2]int]int{}
	for _, s := range node.stored {
		if s.Status != "committed" {
			continue
		}
		for _, u := range decodeChunkUnits(s.Content) {
			counts[[2]int{u.Seq, u.Part}]++
		}
	}
	node.mu.Unlock()
	for k, c := range counts {
		require.Equalf(t, 1, c, "unit seq=%d part=%d captured %d times (overlap)", k[0], k[1], c)
	}
	turns := reconstructTurns(node.committedUnits())
	require.Len(t, turns, 3)
	require.Equal(t, []string{"alpha", "beta", "gamma"}, []string{turns[0].text, turns[1].text, turns[2].text})
}

// ── blocker 2: idempotent replay after sidecar loss (no duplicate) ──────────

func TestIdempotentReplayAfterSidecarLoss(t *testing.T) {
	node := newMockNode()
	node.forceCmt = true
	root := setupNeverCompactEnv(t, node)
	path := writeTranscriptFile(t, root, "sess-idem", []string{"one", "two"})
	payload := payloadFor("sess-idem", path)
	feedStdin(t, payload, func() { _ = runHookPreCompact() })
	node.mu.Lock()
	first := node.submits
	node.mu.Unlock()

	// Lose the sidecar (simulate a crash before the returned id persisted).
	entries, _ := os.ReadDir(neverCompactHomeDir())
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".json") {
			_ = os.Remove(filepath.Join(neverCompactHomeDir(), e.Name()))
		}
	}
	feedStdin(t, payload, func() { _ = runHookPreCompact() })
	node.mu.Lock()
	require.Equal(t, first, node.submits, "the deterministic chunk tag must prevent a duplicate submit")
	node.mu.Unlock()
}

// ── recall: thread-scoped, byte-exact, ordered ──────────────────────────────

func TestRecallByteExact(t *testing.T) {
	node := newMockNode()
	node.forceCmt = true
	root := setupNeverCompactEnv(t, node)
	turns := []string{"turn one alpha", "turn two beta", "turn three gamma"}
	path := writeTranscriptFile(t, root, "sess-3", turns)
	payload := payloadFor("sess-3", path)
	feedStdin(t, payload, func() { _ = runHookPreCompact() })

	out := captureStdout(t, func() {
		feedStdin(t, payload, func() { emitThreadScopedRecall([]byte(payload)) })
	})
	for _, tx := range turns {
		require.Contains(t, out, tx)
	}
	require.Less(t, strings.Index(out, "turn one alpha"), strings.Index(out, "turn three gamma"), "recall must be in order")
}

// ── retention: purge deprecates captured records ────────────────────────────

func TestPurgeDeprecatesCaptured(t *testing.T) {
	node := newMockNode()
	node.forceCmt = true
	root := setupNeverCompactEnv(t, node)
	path := writeTranscriptFile(t, root, "sess-4", []string{"to purge"})
	payload := payloadFor("sess-4", path)
	feedStdin(t, payload, func() { _ = runHookPreCompact() })
	node.mu.Lock()
	captured := node.submits
	node.mu.Unlock()
	require.NotZero(t, captured)
	_ = captureStdout(t, func() { _ = neverCompactPurge([]string{"--thread", "sess-4"}) })
	node.mu.Lock()
	defer node.mu.Unlock()
	require.GreaterOrEqual(t, node.forgets, captured)
	for _, s := range node.stored {
		require.Equal(t, "deprecated", s.Status)
	}
}

// ── blocker 5: deadlines are global ─────────────────────────────────────────

func TestLockRespectsDeadline(t *testing.T) {
	t.Setenv("SAGE_HOME", t.TempDir())
	// Hold the lock, then try to acquire under an already-expired context.
	rel, err := acquireCaptureLock(context.Background(), "thr")
	require.NoError(t, err)
	defer rel()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	time.Sleep(3 * time.Millisecond)
	start := time.Now()
	_, err = acquireCaptureLock(ctx, "thr")
	require.Error(t, err, "lock acquisition must give up on the command deadline")
	require.Less(t, time.Since(start), 500*time.Millisecond, "must not spin past the deadline")
}

func TestRecallStopsOnCancelledContext(t *testing.T) {
	// A cancelled context must stop recall before any page request.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := recallThreadUnits(ctx, "thr", "home")
	require.Error(t, err)
}
