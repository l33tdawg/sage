package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// rowJSON builds one Claude-family transcript JSONL row.
func rowJSON(session, role, text string) string {
	b, _ := json.Marshal(map[string]any{
		"sessionId": session,
		"message":   map[string]any{"role": role, "content": text},
	})
	return string(b)
}

// ── decode: excluded vs malformed vs text (blocker 6) ───────────────────────

func TestDecodeConversationalText(t *testing.T) {
	cases := []struct {
		name, role, content string
		wantText            string
		wantDecoded         bool
	}{
		{"string", "user", `"hello"`, "hello", true},
		{"text-blocks", "assistant", `[{"type":"text","text":"a"},{"type":"text","text":"b"}]`, "a\nb", true},
		{"tool-only decodes empty", "assistant", `[{"type":"tool_use","name":"Bash"}]`, "", true},
		{"non-conversational role excluded", "system", `"x"`, "", false},
		{"malformed object content", "user", `{"weird":true}`, "", false},
		{"malformed number content", "assistant", `42`, "", false},
		{"null content is malformed", "user", `null`, "", false},
		{"absent content is malformed", "assistant", ``, "", false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			role, text, decoded := decodeConversationalText(c.role, json.RawMessage(c.content))
			require.Equal(t, c.role, role)
			require.Equal(t, c.wantDecoded, decoded)
			require.Equal(t, c.wantText, text)
		})
	}
}

// TestUnitsForRowGapSeparation proves blocker 6: malformed user/assistant content
// becomes a VISIBLE gap, while a tool-only or non-conversational row is excluded
// (not a gap), and an unparseable row is a gap.
func TestUnitsForRowGapSeparation(t *testing.T) {
	// malformed user content → one gap unit
	u := unitsForRow(1, []byte(`{"message":{"role":"user","content":{"oops":1}}}`), 6000)
	require.Len(t, u, 1)
	require.Equal(t, unitRoleGap, u[0].Role)

	// tool-only assistant row → excluded (no units, no gap)
	require.Empty(t, unitsForRow(2, []byte(`{"message":{"role":"assistant","content":[{"type":"tool_use","name":"B"}]}}`), 6000))

	// non-conversational (no message) → excluded
	require.Empty(t, unitsForRow(3, []byte(`{"type":"summary","summary":"x"}`), 6000))

	// unparseable row → gap
	g := unitsForRow(4, []byte(`not json`), 6000)
	require.Len(t, g, 1)
	require.Equal(t, unitRoleGap, g[0].Role)

	// null conversational content → gap. json.Unmarshal(`null`) succeeds into both a
	// string and a block slice, so without the explicit guard this silently decoded to
	// empty and was excluded; it must instead emit one visible gap.
	n := unitsForRow(6, []byte(`{"message":{"role":"user","content":null}}`), 6000)
	require.Len(t, n, 1)
	require.Equal(t, unitRoleGap, n[0].Role)

	// a normal text turn → one text unit
	x := unitsForRow(5, []byte(rowJSON("s", "user", "hi")), 6000)
	require.Len(t, x, 1)
	require.Equal(t, unitRoleUser, x[0].Role)
	require.Equal(t, "hi", x[0].Text)
}

// ── byte-exact split + reconstruction, incl. repeated fragments (blocker 3) ──

func TestSplitRawExactByteExact(t *testing.T) {
	for _, s := range []string{
		strings.Repeat("x", 100),
		"héllo wörld " + strings.Repeat("π", 50), // multibyte
		"abc",
		"",
	} {
		for _, mb := range []int{1, 7, 30, 1000} {
			parts := splitRawExact(s, mb)
			require.Equal(t, s, strings.Join(parts, ""), "split(%d) must reconstruct exactly", mb)
		}
	}
}

func TestByteExactReconstructionWithRepeatedFragments(t *testing.T) {
	// An oversize turn that splits into repeated identical fragments (old code would
	// collapse them by identical chunk id and drop bytes), plus a normal turn.
	big := strings.Repeat("x", 100) // under maxUnitBytes=30 → parts x30,x30,x30,x10 (3 identical)
	units := make([]captureUnit, 0, 8)
	units = append(units, unitsForRow(1, []byte(rowJSON("s", "user", big)), 30)...)
	units = append(units, unitsForRow(2, []byte(rowJSON("s", "assistant", "short reply")), 30)...)

	// more than one fragment for the oversize turn, distinct part ordinals
	parts1 := 0
	for _, u := range units {
		if u.Seq == 1 {
			parts1++
		}
	}
	require.Greater(t, parts1, 1, "oversize turn must split into multiple parts")

	// chunk → encode → decode → reconstruct, and require EXACT bytes back
	chunks := chunkUnits("t", units, 10_000)
	decoded := make([]captureUnit, 0, len(units))
	for _, ch := range chunks {
		decoded = append(decoded, decodeChunkUnits(ch.body)...)
	}
	turns := reconstructTurns(decoded)
	require.Len(t, turns, 2)
	require.Equal(t, big, turns[0].text, "repeated identical fragments must all be preserved (byte-exact)")
	require.Equal(t, "short reply", turns[1].text)
}

func TestEncodeDecodeUnitRoundTripArbitraryBytes(t *testing.T) {
	// text containing the header prefix, tabs, and newlines must round-trip exactly
	// (the length prefix means no escaping is needed).
	tricky := "NCU\t1\t0\tuser\t99\nnot a real header\ttab\nnewline"
	u := captureUnit{Seq: 7, Part: 2, Role: unitRoleAssistant, Text: tricky}
	decoded := decodeChunkUnits(encodeUnit(u))
	require.Len(t, decoded, 1)
	require.Equal(t, u, decoded[0])
}

// ── immutable cursor / append stability (blocker 2) ─────────────────────────

func TestCaptureProgressCursor(t *testing.T) {
	p := &captureProgress{}
	s, part := p.cursor()
	require.Equal(t, 0, s)
	require.Equal(t, -1, part)

	upsertChunk(p, chunkRecord{ChunkID: "a", StartSeq: 1, StartPart: 0, EndSeq: 2, EndPart: 0, Status: "committed"})
	upsertChunk(p, chunkRecord{ChunkID: "b", StartSeq: 3, StartPart: 0, EndSeq: 3, EndPart: 2, Status: "proposed"})
	s, part = p.cursor()
	require.Equal(t, 3, s)
	require.Equal(t, 2, part, "proposed chunks advance the cursor so appends never re-group them")

	// a rejected chunk must NOT advance the cursor (its units get recaptured)
	upsertChunk(p, chunkRecord{ChunkID: "c", StartSeq: 4, StartPart: 0, EndSeq: 9, EndPart: 0, Status: "rejected"})
	s, part = p.cursor()
	require.Equal(t, 3, s)
	require.Equal(t, 2, part)
}

// TestCaptureProgressCursorRejectedMiddleSpan proves the cursor is the CONTIGUOUS
// non-rejected prefix, not the maximum non-rejected end: a rejected MIDDLE span (11-20)
// followed by a later accepted chunk (21-30) must roll the cursor back to 10 so 11-20 is
// recaptured, not permanently skipped. Chunks are inserted out of start order to also
// prove the ordering is by span, not insertion.
func TestCaptureProgressCursorRejectedMiddleSpan(t *testing.T) {
	p := &captureProgress{}
	upsertChunk(p, chunkRecord{ChunkID: "m3", StartSeq: 21, StartPart: 0, EndSeq: 30, EndPart: 0, Status: "committed"})
	upsertChunk(p, chunkRecord{ChunkID: "m1", StartSeq: 1, StartPart: 0, EndSeq: 10, EndPart: 0, Status: "committed"})
	upsertChunk(p, chunkRecord{ChunkID: "m2", StartSeq: 11, StartPart: 0, EndSeq: 20, EndPart: 0, Status: "rejected"})
	s, part := p.cursor()
	require.Equal(t, 10, s, "cursor stops at the contiguous prefix; the rejected 11-20 span is recaptured, not skipped by 21-30")
	require.Equal(t, 0, part)
}

func TestChunkUnitsResumeAfterCursorViaParse(t *testing.T) {
	// Build a transcript file and parse with a cursor: only units strictly after the
	// cursor are materialized (blocker 1/2 — no re-grouping of captured units).
	lines := []string{
		rowJSON("s", "user", "one"),
		rowJSON("s", "assistant", "two"),
		rowJSON("s", "user", "three"),
	}
	f := writeTempTranscriptFile(t, strings.Join(lines, "\n"))
	defer f.Close()
	units, err := parseTranscriptUnits(context.Background(), f, 2, 0, 6000)
	require.NoError(t, err)
	// cursor (2,0) → skip seq1 entirely and seq2 part0; keep seq2 (none left) + seq3
	for _, u := range units {
		require.True(t, u.after(2, 0), "unit %v must be strictly after the cursor", u)
	}
	require.Len(t, units, 1)
	require.Equal(t, 3, units[0].Seq)
}

// ── path authority incl. ancestor no-follow (blocker 4) ─────────────────────

func TestOpenValidatedTranscript(t *testing.T) {
	root := t.TempDir()
	t.Setenv("SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT", root)
	sub := filepath.Join(root, "proj")
	require.NoError(t, os.MkdirAll(sub, 0o700))
	good := filepath.Join(sub, "session.jsonl")
	require.NoError(t, os.WriteFile(good, []byte(`{"sessionId":"s"}`), 0o600))

	f, err := openValidatedTranscript(good)
	require.NoError(t, err, "a valid transcript under the root must be accepted")
	f.Close()

	// outside the root
	outside := filepath.Join(t.TempDir(), "x.jsonl")
	_ = os.WriteFile(outside, []byte("{}"), 0o600)
	_, err = openValidatedTranscript(outside)
	require.Error(t, err, "outside the trusted root must be rejected")

	// non-.jsonl
	_, err = openValidatedTranscript(filepath.Join(sub, "notes.txt"))
	require.Error(t, err)

	// directory
	_, err = openValidatedTranscript(sub)
	require.Error(t, err)

	// empty
	_, err = openValidatedTranscript("")
	require.Error(t, err)
}

// TestOpenValidatedTranscriptRejectsAncestorSymlink is the blocker-4 regression:
// a symlinked ANCESTOR directory in the path must be rejected by the no-follow
// component walk, not silently followed out of the trusted root.
func TestOpenValidatedTranscriptRejectsAncestorSymlink(t *testing.T) {
	root := t.TempDir()
	t.Setenv("SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT", root)
	realDir := filepath.Join(root, "real")
	require.NoError(t, os.MkdirAll(realDir, 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(realDir, "session.jsonl"), []byte(`{"sessionId":"s"}`), 0o600))

	// an ANCESTOR symlink under the root pointing at the real dir
	link := filepath.Join(root, "link")
	if err := os.Symlink(realDir, link); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	// request the transcript THROUGH the ancestor symlink
	_, err := openValidatedTranscript(filepath.Join(link, "session.jsonl"))
	require.Error(t, err, "a symlinked ancestor component must be rejected by the no-follow walk")
}

// TestOpenValidatedTranscriptCanonicalizedRoot reproduces the macOS canonicalized-root
// failure on any platform: when the TRUSTED ROOT itself is reached through a symlink
// (as /var → /private/var is on macOS), the root canonicalizes but the payload path
// does not, so a namespace-mismatched comparison wrongly rejects a valid transcript.
// The fix compares in the raw namespace while still walking from the canonical root, so
// a valid transcript under the symlinked root is accepted — WITHOUT resolving any
// below-root symlink (that rejection is covered by the test above).
func TestOpenValidatedTranscriptCanonicalizedRoot(t *testing.T) {
	base := t.TempDir()
	realRoot := filepath.Join(base, "realroot")
	require.NoError(t, os.MkdirAll(filepath.Join(realRoot, "proj"), 0o700))
	require.NoError(t, os.WriteFile(filepath.Join(realRoot, "proj", "session.jsonl"), []byte(`{"sessionId":"s"}`), 0o600))

	// the configured root is a SYMLINK to the real root (models a symlinked ancestor
	// like macOS /var). EvalSymlinks(root) != root, exactly the mismatch that bit.
	linkRoot := filepath.Join(base, "linkroot")
	if err := os.Symlink(realRoot, linkRoot); err != nil {
		t.Skipf("symlink unsupported: %v", err)
	}
	t.Setenv("SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT", linkRoot)

	f, err := openValidatedTranscript(filepath.Join(linkRoot, "proj", "session.jsonl"))
	require.NoError(t, err, "a valid transcript under a symlinked (canonicalized) root must be accepted")
	f.Close()
}

// ── consent (default-off, versioned) ────────────────────────────────────────

func TestConsentDefaultOffAndEnvOptIn(t *testing.T) {
	t.Setenv("SAGE_HOME", t.TempDir())
	t.Setenv("SAGE_NEVERCOMPACT", "")
	ok, _ := neverCompactCapturePermitted()
	require.False(t, ok, "default must be OFF")
	for _, on := range []string{"1", "true", "YES", "on"} {
		t.Setenv("SAGE_NEVERCOMPACT", on)
		enabled, mode := neverCompactCapturePermitted()
		require.True(t, enabled)
		require.Equal(t, "headless", mode)
	}
	t.Setenv("SAGE_NEVERCOMPACT", "0")
	ok, _ = neverCompactCapturePermitted()
	require.False(t, ok)
}

func TestConsentVersionedInteractive(t *testing.T) {
	t.Setenv("SAGE_HOME", t.TempDir())
	t.Setenv("SAGE_NEVERCOMPACT", "")
	writeConsent(t, neverCompactConsentVersion)
	ok, mode := neverCompactCapturePermitted()
	require.True(t, ok)
	require.Equal(t, "interactive", mode)
	writeConsent(t, neverCompactConsentVersion-1)
	ok, _ = neverCompactCapturePermitted()
	require.False(t, ok, "a stale consent version must NOT permit capture")
}

func TestConsentDisclosureStatesRequiredFacts(t *testing.T) {
	d := strings.ToLower(neverCompactDisclosure())
	for _, must := range []string{"classification", "retention", "delet", "revocable", "version", "beyond"} {
		require.Contains(t, d, must)
	}
}

// ── clamps ──────────────────────────────────────────────────────────────────

func TestClassificationNeverPublic(t *testing.T) {
	t.Setenv("SAGE_NEVERCOMPACT_CLASSIFICATION", "")
	require.Equal(t, neverCompactDefaultClassification, neverCompactClassification())
	for _, v := range []string{"0", "-5"} {
		t.Setenv("SAGE_NEVERCOMPACT_CLASSIFICATION", v)
		require.Equal(t, 1, neverCompactClassification(), "%q must clamp to 1 (never Public)", v)
	}
	t.Setenv("SAGE_NEVERCOMPACT_CLASSIFICATION", "9")
	require.Equal(t, 4, neverCompactClassification())
}

func TestKnobsClamp(t *testing.T) {
	t.Setenv("SAGE_NEVERCOMPACT_MAX_UNITS", "9999999")
	require.Equal(t, preCompactMaxUnitsCeiling, preCompactMaxUnits())
	t.Setenv("SAGE_NEVERCOMPACT_CHUNK_BYTES", "9999999")
	require.Equal(t, preCompactMaxChunkBytes, preCompactChunkBytes())
	t.Setenv("SAGE_NEVERCOMPACT_BUDGET_MS", "9999999")
	require.Equal(t, int64(preCompactMaxBudgetMS), preCompactBudget().Milliseconds())
}

// ── helpers ─────────────────────────────────────────────────────────────────

func writeTempTranscriptFile(t *testing.T, content string) *os.File {
	t.Helper()
	path := filepath.Join(t.TempDir(), "t.jsonl")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	f, err := os.Open(path) //nolint:gosec // test file
	require.NoError(t, err)
	return f
}

func writeConsent(t *testing.T, version int) {
	t.Helper()
	c := neverCompactConsent{Version: version, AcceptedAt: "2026-01-01T00:00:00Z", Mode: "interactive"}
	raw, _ := json.MarshalIndent(c, "", "  ")
	require.NoError(t, neverCompactAtomicWrite(neverCompactConsentPath(), raw))
}
