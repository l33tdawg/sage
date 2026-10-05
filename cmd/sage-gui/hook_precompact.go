package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

// hook_precompact.go — CAPTURE half of recall-backed compaction.
//
// On a full-compaction PreCompact event, the harness is about to evict
// conversation turns. This captures those turns verbatim as governed SAGE
// memories so hook.go's thread-scoped recall can restore them later. It is
// default-off (see nevercompact.go for the consent gate) and always soft-fails
// (returns nil) so it can never block compaction.
//
// The load-bearing invariant is that SAGE never durably captures transcript data
// unless it can prove which file it read (a descriptor-relative, no-follow,
// root-bound, owner-checked open), exactly which governed records represent each
// captured byte (a deterministic, append-stable chunk identity carrying the
// source seq+part), and how that thread deterministically recovers or deletes it
// (durable thread id + tag). It is byte-lossless: every eligible byte is
// eventually committed exactly once and reconstructs exactly, or is represented
// as a visible capture gap — it never advances past bytes it dropped.

const (
	neverCompactDefaultClassification = 2 // Confidential; never Public.

	preCompactDefaultChunkBytes = 6_000  // ~1.5k tokens/chunk
	preCompactMaxChunkBytes     = 60_000 // ceiling for the env-tunable chunk size
	preCompactDefaultMaxUnits   = 400    // capture units submitted per invocation
	preCompactMaxUnitsCeiling   = 4_000
	preCompactDefaultBudgetMS   = 3_500 // under the 5s installed-hook budget
	preCompactMaxBudgetMS       = 4_500
	preCompactMaxTotalBytes     = 1 << 20   // total verbatim bytes submitted per invocation
	preCompactMaxTranscript     = 256 << 20 // reject transcripts larger than this
	preCompactMaxLineBytes      = 8 << 20   // per-JSONL-line cap for the streaming scanner
	preCompactStdinCap          = 256 << 10
	preCompactSidecarVersion    = 2

	// preCompactMaxChunkRetries bounds how many times a span whose governed memory
	// ended terminal-rejected is resubmitted before it is abandoned as a visible gap.
	// A transient/ambiguous rejection clears on the first retry; a persistently
	// rejected span becomes a gap instead of resubmitting forever.
	preCompactMaxChunkRetries = 3

	// unitHeaderPrefix marks a byte-exact unit block inside a chunk body:
	//   NCU\t<seq>\t<part>\t<role>\t<byteLen>\n<exactly byteLen raw bytes>
	// The length prefix makes the raw bytes recoverable verbatim with no escaping.
	unitHeaderPrefix = "NCU\t"
)

// preCompactInput is the PreCompact hook payload (stdin).
type preCompactInput struct {
	SessionID      string `json:"session_id"`
	TranscriptPath string `json:"transcript_path"`
	Trigger        string `json:"trigger"`
	CWD            string `json:"cwd"`
}

// transcriptRow is one JSONL line of a Claude-family transcript (only the fields
// this capture needs).
type transcriptRow struct {
	SessionID string `json:"sessionId"`
	Message   *struct {
		Role    string          `json:"role"`
		Content json.RawMessage `json:"content"`
	} `json:"message"`
}

// unitRole classifies a capture unit.
const (
	unitRoleUser      = "user"
	unitRoleAssistant = "assistant"
	unitRoleGap       = "gap"
)

// captureUnit is the atomic unit of capture: a byte-exact fragment of one source
// row. A normal turn is one unit (Part 0). An oversize turn is split into ordered
// parts (0..n-1) whose raw bytes concatenate back to the exact original. A row
// whose conversational content cannot be decoded becomes a single visible gap
// unit. Units are globally ordered by (Seq, Part).
type captureUnit struct {
	Seq  int    // source row ordinal (1-based)
	Part int    // fragment ordinal within the row (0-based)
	Role string // user | assistant | gap
	Text string // raw fragment bytes (for a gap, the human-readable gap text)
}

func (u captureUnit) after(seq, part int) bool {
	return u.Seq > seq || (u.Seq == seq && u.Part > part)
}

// chunkRecord is the durable per-chunk progress entry. StartSeq/StartPart and
// EndSeq/EndPart bound the units it carries; boundaries are immutable once
// submitted, so appending to the transcript never re-groups captured units.
type chunkRecord struct {
	ChunkID   string `json:"chunk_id"`
	StartSeq  int    `json:"start_seq"`
	StartPart int    `json:"start_part"`
	EndSeq    int    `json:"end_seq"`
	EndPart   int    `json:"end_part"`
	MemoryID  string `json:"memory_id"`
	Status    string `json:"status"` // proposed | committed | rejected
	Bytes     int    `json:"bytes"`
	// RejectCount is how many times this span's CONTENT memory has ended
	// terminal-rejected; at preCompactMaxChunkRetries the span switches to gap mode.
	RejectCount int `json:"reject_count,omitempty"`
	// Gap marks a span whose content was rejected past the cap: this record now tracks
	// a GAP-marker memory (MemoryID/Status follow the gap's lifecycle) instead of the
	// content, and the content is never resubmitted again. The cursor advances past the
	// span only once the gap marker is committed, so an errored or rejected gap submit
	// retries safely instead of silently dropping the span.
	Gap bool `json:"gap,omitempty"`
}

// captureProgress is the per-thread sidecar. Progress is the set of governed chunk
// identities and their (seq,part) spans; the capture cursor is the end of the
// contiguous run of non-rejected chunks from the start (see cursor), so chunking
// resumes after the already-captured prefix and a rejected middle span is recaptured
// rather than skipped.
type captureProgress struct {
	Version        int           `json:"version"`
	ThreadID       string        `json:"thread_id"`
	TranscriptPath string        `json:"transcript_path"`
	Chunks         []chunkRecord `json:"chunks"`
}

func (p *captureProgress) byChunkID(id string) *chunkRecord {
	for i := range p.Chunks {
		if p.Chunks[i].ChunkID == id {
			return &p.Chunks[i]
		}
	}
	return nil
}

// cursor returns the end of the CONTIGUOUS run of non-rejected chunks from the start,
// or (0, -1) if none. It is deliberately not the maximum end of all non-rejected
// chunks: walking chunks in start order and stopping at the first rejected one means a
// rejected MIDDLE span is recaptured on the next run instead of being permanently
// skipped by a later accepted chunk's end (which reintroduces the tail-loss class).
// Chunk starts jump over excluded non-conversational rows, so a rejected status — not
// seq adjacency — is the only reliable contiguity boundary.
func (p *captureProgress) cursor() (seq, part int) {
	seq, part = 0, -1
	ordered := append([]chunkRecord(nil), p.Chunks...)
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].StartSeq != ordered[j].StartSeq {
			return ordered[i].StartSeq < ordered[j].StartSeq
		}
		return ordered[i].StartPart < ordered[j].StartPart
	})
	for i := range ordered {
		c := ordered[i]
		if c.Status == "rejected" {
			break
		}
		// A gap-mode span (content abandoned to a gap marker) advances the cursor only
		// once the gap marker is committed — never on a merely-submitted or errored gap,
		// so a rejected/failed gap submit holds the cursor and retries instead of
		// silently dropping the span.
		if c.Gap && c.Status != memoryStatusCommitted {
			break
		}
		if c.EndSeq > seq || (c.EndSeq == seq && c.EndPart > part) {
			seq, part = c.EndSeq, c.EndPart
		}
	}
	return seq, part
}

// runHookPreCompact is the entry point. It always returns nil: capture must never
// block compaction. Diagnostics go to stderr (not surfaced to the agent); nothing
// is written to stdout.
func runHookPreCompact() error {
	// Blocker 5: ONE deadline established at command entry, before any file read,
	// and propagated through lock acquisition and every network operation.
	ctx, cancel := context.WithTimeout(context.Background(), preCompactBudget())
	defer cancel()

	// Blocker 6 (consent): default-off consent gate.
	if permitted, _ := neverCompactCapturePermitted(); !permitted {
		return nil
	}

	raw, _ := io.ReadAll(io.LimitReader(os.Stdin, preCompactStdinCap))
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	var in preCompactInput
	if err := json.Unmarshal(raw, &in); err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: unparseable hook payload: %v\n", err)
		return nil
	}
	if strings.TrimSpace(in.TranscriptPath) == "" || strings.TrimSpace(in.SessionID) == "" {
		return nil
	}

	// Blocker 4: open the transcript by descriptor-relative, no-follow component
	// walk bound to the trusted root, and read from that exact descriptor.
	f, err := openValidatedTranscript(in.TranscriptPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: transcript rejected: %v\n", err)
		return nil
	}
	defer f.Close()

	// A first, cheap streaming pass over the SAME descriptor: prove the durable
	// thread identity and the payload-session binding by scanning ALL rows (blocker
	// 1: never cut this off at a turn cap), without materializing unit text.
	firstSession, sawPayloadSession, lastSeq, err := scanTranscriptIdentity(ctx, f, in.SessionID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: transcript scan aborted: %v\n", err)
		return nil
	}
	if firstSession == "" {
		fmt.Fprintln(os.Stderr, "nevercompact: no durable thread identity in transcript; refusing capture")
		return nil
	}
	if !sawPayloadSession {
		fmt.Fprintln(os.Stderr, "nevercompact: payload session id absent from transcript; refusing capture")
		return nil
	}
	threadID := firstSession
	if lastSeq == 0 {
		return nil
	}

	// Serialize concurrent PreCompact invocations for this thread (blocker 5: the
	// lock waits under the command deadline, not its own).
	release, err := acquireCaptureLock(ctx, threadID)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: could not lock thread progress: %v\n", err)
		return nil
	}
	defer release()

	domain, err := preCompactHomeDomain(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: resolve home domain: %v\n", err)
		return nil
	}

	prog := loadCaptureProgress(threadID)
	prog.Version = preCompactSidecarVersion
	prog.ThreadID = threadID
	prog.TranscriptPath = baseName(in.TranscriptPath)

	// Reconcile prior chunks through the committed lifecycle before deciding new
	// work: a proposed chunk that has now committed advances the cursor; one that
	// was rejected rolls the cursor back so its units are recaptured (blocker 2).
	reconcileProgress(ctx, prog)
	saveCaptureProgress(prog)

	cursorSeq, cursorPart := prog.cursor()

	// Second pass over the SAME descriptor (seek to start): materialize only the
	// units strictly after the cursor (blocker 1: the uncaptured tail, however far
	// past any per-invocation cap earlier runs stopped at).
	if _, seekErr := f.Seek(0, io.SeekStart); seekErr != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: rewind transcript: %v\n", seekErr)
		return nil
	}
	units, err := parseTranscriptUnits(ctx, f, cursorSeq, cursorPart, preCompactChunkBytes())
	if err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: transcript parse aborted: %v\n", err)
		return nil
	}
	if len(units) == 0 {
		return nil
	}

	// Cap this invocation's work (units + total bytes) BEFORE chunking, so every
	// chunk formed is submittable and at least one unit always progresses — the
	// remaining tail defers to the next PreCompact (the cursor persists across
	// runs, so nothing is lost; blocker 1). Chunk boundaries then depend only on
	// the unit stream from the cursor, so they are immutable under append
	// (blocker 2).
	maxUnits := preCompactMaxUnits()
	budgetBytes := 0
	capped := units
	for i, u := range units {
		enc := len(encodeUnit(u))
		if i >= maxUnits || (i > 0 && budgetBytes+enc > preCompactMaxTotalBytes) {
			capped = units[:i]
			break
		}
		budgetBytes += enc
	}
	chunks := chunkUnits(threadID, capped, preCompactChunkBytes())

	class := neverCompactClassification()
	for _, ch := range chunks {
		if ctx.Err() != nil {
			break // budget exhausted; the rest is picked up next PreCompact
		}
		existing := prog.byChunkID(ch.id)
		// A committed span is done.
		if existing != nil && existing.Status == "committed" {
			continue
		}
		// A gap-mode span (content abandoned past the cap) never resubmits its content;
		// it drives its GAP marker toward a committed, recall-visible outcome. The cursor
		// only advances past it once that gap commits, so this retries safely.
		if existing != nil && existing.Gap {
			driveGapMarker(ctx, prog, domain, threadID, class, existing)
			saveCaptureProgress(prog)
			continue
		}
		// Blocker 2 (idempotent replay): deterministic identity → look up before
		// submit → adopt an already-governed NON-REJECTED copy, so an ambiguous timeout
		// on a prior run never creates a duplicate.
		if adopted := reconcileChunkByTag(ctx, ch.id); adopted != nil {
			rec := ch.record(adopted.MemoryID, adopted.Status)
			if existing != nil {
				rec.RejectCount = existing.RejectCount
			}
			upsertChunk(prog, rec)
			saveCaptureProgress(prog)
			continue
		}
		// A span whose only governed copy is terminal-rejected makes forward progress by
		// resubmitting, bounded so the CONTENT cannot resubmit forever: past the cap the
		// span switches to gap mode (a tracked gap marker), driven this same invocation.
		rejectCount := 0
		if existing != nil && existing.Status == "rejected" {
			rejectCount = existing.RejectCount + 1
			if rejectCount > preCompactMaxChunkRetries {
				existing.Gap = true
				existing.RejectCount = rejectCount
				existing.MemoryID = ""
				existing.Status = "proposed"
				driveGapMarker(ctx, prog, domain, threadID, class, existing)
				saveCaptureProgress(prog)
				continue
			}
		}

		memID, status, err := submitCapturedChunk(ctx, domain, threadID, class, ch)
		if err != nil {
			fmt.Fprintf(os.Stderr, "nevercompact: submit chunk %s: %v\n", ch.id[:12], err)
			break // stop on first failure; deferred chunks retry next PreCompact
		}
		rec := ch.record(memID, status)
		rec.RejectCount = rejectCount
		upsertChunk(prog, rec)
		saveCaptureProgress(prog) // persist after every chunk: a crash loses no identity
	}

	reconcileProgress(ctx, prog)
	saveCaptureProgress(prog)
	return nil
}

// ── transcript scanning & parsing ──────────────────────────────────────────

// scanTranscriptIdentity streams every row of the transcript (bounded by the
// command deadline and the line cap) to establish the durable thread identity
// (first row's session), whether the payload session appears anywhere (content
// binding), and the last source-row ordinal. It materializes no unit text.
func scanTranscriptIdentity(ctx context.Context, f *os.File, payloadSession string) (firstSession string, sawPayloadSession bool, lastSeq int, err error) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), preCompactMaxLineBytes)
	seq := 0
	for sc.Scan() {
		if ctx.Err() != nil {
			return "", false, 0, fmt.Errorf("budget exceeded while scanning transcript")
		}
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		seq++
		lastSeq = seq
		var row transcriptRow
		if json.Unmarshal(line, &row) != nil {
			continue // an unparseable row still becomes a gap unit in the parse pass
		}
		if firstSession == "" && strings.TrimSpace(row.SessionID) != "" {
			firstSession = strings.TrimSpace(row.SessionID)
		}
		if row.SessionID == payloadSession {
			sawPayloadSession = true
		}
	}
	if scanErr := sc.Err(); scanErr != nil {
		return "", false, 0, fmt.Errorf("scan transcript: %w", scanErr)
	}
	return firstSession, sawPayloadSession, lastSeq, nil
}

// parseTranscriptUnits streams the transcript and materializes capture units for
// rows strictly after the (cursorSeq, cursorPart) cursor. Rows at or before the
// cursor are counted (to preserve seq) but not materialized. Oversize turns are
// split into byte-exact parts; undecodable conversational rows and unparseable
// rows become visible gaps.
func parseTranscriptUnits(ctx context.Context, f *os.File, cursorSeq, cursorPart, maxUnitBytes int) ([]captureUnit, error) {
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 0, 64<<10), preCompactMaxLineBytes)
	var out []captureUnit
	seq := 0
	for sc.Scan() {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("budget exceeded while reading transcript")
		}
		line := bytes.TrimSpace(sc.Bytes())
		if len(line) == 0 {
			continue
		}
		seq++
		if seq < cursorSeq {
			continue // already captured in a prior invocation
		}
		rowUnits := unitsForRow(seq, line, maxUnitBytes)
		for _, u := range rowUnits {
			if !u.after(cursorSeq, cursorPart) {
				continue // this row was partially captured; skip its captured parts
			}
			out = append(out, u)
		}
	}
	if scanErr := sc.Err(); scanErr != nil {
		return nil, fmt.Errorf("scan transcript: %w", scanErr)
	}
	return out, nil
}

// unitsForRow decodes one JSONL row into ordered capture units. An unparseable row
// or undecodable conversational content yields a single gap unit; a tool-only or
// non-conversational row yields nothing (excluded, not lost). A text turn yields
// one unit, or several byte-exact parts if it exceeds maxUnitBytes.
func unitsForRow(seq int, line []byte, maxUnitBytes int) []captureUnit {
	var row transcriptRow
	if json.Unmarshal(line, &row) != nil {
		return []captureUnit{gapUnit(seq, "unparseable transcript row")}
	}
	if row.Message == nil {
		return nil // non-conversational (summary/meta) row: excluded, not a gap
	}
	role, text, decoded := decodeConversationalText(row.Message.Role, row.Message.Content)
	if !decoded {
		// Blocker 6: a user/assistant row whose content cannot be decoded is a
		// visible gap; a genuinely non-conversational/tool-only row is excluded.
		if role == unitRoleUser || role == unitRoleAssistant {
			return []captureUnit{gapUnit(seq, "undecodable "+role+" content")}
		}
		return nil
	}
	if text == "" {
		return nil // empty conversational content: nothing to capture, not a gap
	}
	parts := splitRawExact(text, maxUnitBytes)
	units := make([]captureUnit, 0, len(parts))
	for i, p := range parts {
		units = append(units, captureUnit{Seq: seq, Part: i, Role: role, Text: p})
	}
	return units
}

func gapUnit(seq int, reason string) captureUnit {
	return captureUnit{Seq: seq, Part: 0, Role: unitRoleGap, Text: "[capture gap at row " + strconv.Itoa(seq) + ": " + reason + "]"}
}

// decodeConversationalText returns (role, text, decoded). decoded is false when a
// user/assistant row's content is malformed (→ caller emits a gap) and also for
// non-conversational roles (→ caller excludes). Only text blocks are kept;
// tool_use/tool_result are excluded (re-derivable, may carry secrets) — a turn
// consisting solely of them decodes to empty text, which is excluded, not a gap.
func decodeConversationalText(role string, content json.RawMessage) (string, string, bool) {
	if role != unitRoleUser && role != unitRoleAssistant {
		return role, "", false
	}
	// A conversational row with null or absent content is malformed, not legitimately
	// empty: json.Unmarshal(`null`, ...) succeeds into BOTH a string ("") and a typed
	// block slice (nil), so without this guard it would report decoded-but-empty and be
	// silently excluded instead of emitting the required gap. A tool-only turn, by
	// contrast, is a non-null array that decodes to empty text and is correctly excluded.
	if t := bytes.TrimSpace(content); len(t) == 0 || bytes.Equal(t, []byte("null")) {
		return role, "", false
	}
	var asString string
	if json.Unmarshal(content, &asString) == nil {
		return role, asString, true
	}
	var blocks []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	}
	if json.Unmarshal(content, &blocks) != nil {
		// Content is present but neither a string nor a typed-block array: malformed.
		return role, "", false
	}
	var b strings.Builder
	for _, blk := range blocks {
		if blk.Type == "text" && blk.Text != "" {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(blk.Text)
		}
	}
	return role, b.String(), true
}

// splitRawExact splits s into byte-bounded pieces on rune boundaries, exactly
// once, such that concatenating the pieces reproduces s byte-for-byte.
func splitRawExact(s string, maxBytes int) []string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return []string{s}
	}
	var pieces []string
	for len(s) > maxBytes {
		cut := maxBytes
		for cut > 0 && !utf8.RuneStart(s[cut]) {
			cut--
		}
		if cut == 0 { // a single rune longer than maxBytes: emit it whole
			_, size := utf8.DecodeRuneInString(s)
			cut = size
		}
		pieces = append(pieces, s[:cut])
		s = s[cut:]
	}
	if len(s) > 0 {
		pieces = append(pieces, s)
	}
	return pieces
}

// ── chunking (blockers 2 & 3: immutable boundaries, byte-exact bodies) ──────

type preCompactChunk struct {
	id        string
	startSeq  int
	startPart int
	endSeq    int
	endPart   int
	body      string
	bytes     int
	unitCount int
}

func (c preCompactChunk) record(memID, status string) chunkRecord {
	if status != memoryStatusCommitted {
		status = "proposed"
	}
	return chunkRecord{
		ChunkID: c.id, StartSeq: c.startSeq, StartPart: c.startPart,
		EndSeq: c.endSeq, EndPart: c.endPart, MemoryID: memID, Status: status, Bytes: c.bytes,
	}
}

// chunkUnits groups the (already cursor-filtered, ordered) units into chunks whose
// encoded body stays under maxBytes. Boundaries depend only on the unit stream, so
// they are identical across invocations; a unit whose own encoded size exceeds
// maxBytes becomes its own chunk (it was already split into parts upstream).
func chunkUnits(threadID string, units []captureUnit, maxBytes int) []preCompactChunk {
	out := make([]preCompactChunk, 0, 1)
	group := make([]captureUnit, 0, len(units))
	groupBytes := 0
	flush := func() {
		if len(group) == 0 {
			return
		}
		out = append(out, buildChunk(threadID, group))
		group = group[:0]
		groupBytes = 0
	}
	for _, u := range units {
		enc := encodeUnit(u)
		if len(group) > 0 && groupBytes+len(enc) > maxBytes {
			flush()
		}
		group = append(group, u)
		groupBytes += len(enc)
	}
	flush()
	return out
}

// encodeUnit serializes one unit as a byte-exact, self-delimiting block.
func encodeUnit(u captureUnit) string {
	return fmt.Sprintf("%s%d\t%d\t%s\t%d\n%s", unitHeaderPrefix, u.Seq, u.Part, u.Role, len(u.Text), u.Text)
}

func buildChunk(threadID string, units []captureUnit) preCompactChunk {
	var b strings.Builder
	for _, u := range units {
		b.WriteString(encodeUnit(u))
	}
	body := b.String()
	first, last := units[0], units[len(units)-1]
	// Deterministic identity carries thread + the exact (seq,part) span + a hash of
	// the encoded body, so identical content at different positions (repeated
	// fragments) gets distinct ids and is never collapsed by dedup (blocker 3).
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00%d\x00%d\x00%d\x00%d\x00", threadID, first.Seq, first.Part, last.Seq, last.Part)
	h.Write([]byte(body))
	return preCompactChunk{
		id:        hex.EncodeToString(h.Sum(nil)),
		startSeq:  first.Seq,
		startPart: first.Part,
		endSeq:    last.Seq,
		endPart:   last.Part,
		body:      body,
		bytes:     len(body),
		unitCount: len(units),
	}
}

// decodeChunkUnits parses a chunk body back into its exact units. Used by recall
// (and asserted by tests) for byte-exact reconstruction.
func decodeChunkUnits(body string) []captureUnit {
	var units []captureUnit
	s := body
	for strings.HasPrefix(s, unitHeaderPrefix) {
		nl := strings.IndexByte(s, '\n')
		if nl < 0 {
			break
		}
		header := s[len(unitHeaderPrefix):nl]
		fields := strings.Split(header, "\t")
		if len(fields) != 4 {
			break
		}
		seq, e1 := strconv.Atoi(fields[0])
		part, e2 := strconv.Atoi(fields[1])
		role := fields[2]
		blen, e3 := strconv.Atoi(fields[3])
		if e1 != nil || e2 != nil || e3 != nil || blen < 0 {
			break
		}
		rest := s[nl+1:]
		if len(rest) < blen {
			break
		}
		units = append(units, captureUnit{Seq: seq, Part: part, Role: role, Text: rest[:blen]})
		s = rest[blen:]
	}
	return units
}

// ── submit + reconcile (committed-gated, idempotent) ────────────────────────

const memoryStatusCommitted = "committed"

func submitCapturedChunk(ctx context.Context, domain, threadID string, class int, ch preCompactChunk) (memoryID, status string, err error) {
	body, _ := json.Marshal(map[string]any{
		"content":          ch.body,
		"memory_type":      "observation",
		"domain_tag":       domain,
		"confidence_score": 0.8,
		"classification":   class,
		"tags": []string{
			neverCompactTag,
			neverCompactThreadTagPrefix + threadID,
			neverCompactChunkTagPrefix + ch.id,
		},
	})
	var resp struct {
		MemoryID  string `json:"memory_id"`
		Status    string `json:"status"`
		Committed bool   `json:"committed"`
	}
	if err := hookSignedJSONCtx(ctx, http.MethodPost, "/v1/memory/submit", body, &resp); err != nil {
		return "", "", err
	}
	return resp.MemoryID, resp.Status, nil
}

// reconcileChunkByTag adopts an already-governed, NON-REJECTED copy of a chunk by its
// deterministic tag (idempotent-replay backstop for the ambiguous-timeout case). It
// deliberately skips terminal rejected/deprecated copies: adopting one would re-mark the
// span rejected on every invocation and it could never make forward progress. Scanning a
// few newest (rather than only the single newest) lets a fresh committed resubmission be
// adopted even when an older rejected copy shares the tag, so the ambiguous-timeout dedup
// still holds for the live copy.
func reconcileChunkByTag(ctx context.Context, chunkID string) *chunkRecord {
	q := url.Values{}
	q.Set("tag", neverCompactChunkTagPrefix+chunkID)
	q.Set("limit", "8")
	q.Set("sort", "newest")
	var payload struct {
		Memories []struct {
			MemoryID string `json:"memory_id"`
			Status   string `json:"status"`
		} `json:"memories"`
	}
	if err := hookSignedJSONCtx(ctx, http.MethodGet, "/v1/memory/list?"+q.Encode(), nil, &payload); err != nil {
		return nil
	}
	for _, mem := range payload.Memories {
		if mem.Status == "rejected" || mem.Status == "deprecated" {
			continue // terminal-rejected: the span must retry, not adopt this copy
		}
		return &chunkRecord{MemoryID: mem.MemoryID, Status: mem.Status}
	}
	return nil
}

// reconcileProgress advances proposed chunks toward their final lifecycle status:
// a committed chunk lets the cursor advance; a deprecated/rejected chunk is marked
// rejected so its units are recaptured. The cursor is thus the set of committed
// (or in-flight proposed) spans, never an HTTP accept.
func reconcileProgress(ctx context.Context, prog *captureProgress) {
	for i := range prog.Chunks {
		if ctx.Err() != nil {
			return
		}
		c := &prog.Chunks[i]
		if c.Status == memoryStatusCommitted || c.MemoryID == "" {
			continue
		}
		var detail struct {
			Status string `json:"status"`
		}
		if err := hookSignedJSONCtx(ctx, http.MethodGet, "/v1/memory/"+url.PathEscape(c.MemoryID), nil, &detail); err != nil {
			continue
		}
		switch detail.Status {
		case memoryStatusCommitted:
			c.Status = memoryStatusCommitted
		case "deprecated", "rejected":
			c.Status = "rejected"
		}
	}
}

func upsertChunk(prog *captureProgress, rec chunkRecord) {
	if existing := prog.byChunkID(rec.ChunkID); existing != nil {
		*existing = rec
		return
	}
	prog.Chunks = append(prog.Chunks, rec)
}

// buildGapChunk builds the deterministic gap-marker chunk for an abandoned content span:
// one gap unit covering the span, under a distinct id (the "gap" separator keeps it clear
// of the content chunk's id) so a repeated abandonment dedups by tag rather than
// duplicating.
func buildGapChunk(threadID string, span *chunkRecord) preCompactChunk {
	gapText := fmt.Sprintf("[capture gap: rows %d-%d were rejected by governance after %d attempts]",
		span.StartSeq, span.EndSeq, preCompactMaxChunkRetries)
	body := encodeUnit(captureUnit{Seq: span.StartSeq, Part: 0, Role: unitRoleGap, Text: gapText})
	h := sha256.New()
	fmt.Fprintf(h, "%s\x00gap\x00%d\x00%d\x00%d\x00%d\x00", threadID, span.StartSeq, span.StartPart, span.EndSeq, span.EndPart)
	h.Write([]byte(body))
	return preCompactChunk{
		id:        hex.EncodeToString(h.Sum(nil)),
		startSeq:  span.StartSeq,
		startPart: span.StartPart,
		endSeq:    span.EndSeq,
		endPart:   span.EndPart,
		body:      body,
		bytes:     len(body),
		unitCount: 1,
	}
}

// driveGapMarker submits/adopts and TRACKS the gap marker for a gap-mode span, updating
// the sidecar record (a pointer into prog.Chunks) so its MemoryID/Status follow the gap
// marker's lifecycle. The record keeps Gap set and never resubmits the content. Because
// the cursor advances past a gap span only once its marker is committed (see cursor), a
// submit error or a rejected gap leaves the record non-committed and the span is retried
// next invocation — the abandoned bytes are represented by a committed, recall-visible
// gap or not skipped at all, never silently dropped.
func driveGapMarker(ctx context.Context, prog *captureProgress, domain, threadID string, class int, span *chunkRecord) {
	_ = prog // the span pointer already aliases prog.Chunks; kept for call-site symmetry
	gap := buildGapChunk(threadID, span)
	// Adopt an already-governed, non-rejected gap copy (ambiguous-timeout dedup for the
	// gap submit itself), else submit a fresh gap marker.
	if adopted := reconcileChunkByTag(ctx, gap.id); adopted != nil {
		span.MemoryID = adopted.MemoryID
		span.Status = adopted.Status
		return
	}
	memID, status, err := submitCapturedChunk(ctx, domain, threadID, class, gap)
	if err != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: gap marker for rows %d-%d not submitted: %v\n", span.StartSeq, span.EndSeq, err)
		return // leave the record non-committed; retried next invocation (no silent drop)
	}
	span.MemoryID = memID
	span.Status = status
}

// ── knobs & helpers ─────────────────────────────────────────────────────────

func preCompactBudget() time.Duration {
	ms := envIntClamped("SAGE_NEVERCOMPACT_BUDGET_MS", preCompactDefaultBudgetMS, 500, preCompactMaxBudgetMS)
	return time.Duration(ms) * time.Millisecond
}

func preCompactChunkBytes() int {
	return envIntClamped("SAGE_NEVERCOMPACT_CHUNK_BYTES", preCompactDefaultChunkBytes, 1_000, preCompactMaxChunkBytes)
}

func preCompactMaxUnits() int {
	return envIntClamped("SAGE_NEVERCOMPACT_MAX_UNITS", preCompactDefaultMaxUnits, 1, preCompactMaxUnitsCeiling)
}

func neverCompactClassification() int {
	return envIntClamped("SAGE_NEVERCOMPACT_CLASSIFICATION", neverCompactDefaultClassification, 1, 4)
}

func envIntClamped(name string, def, lo, hi int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	if n < lo {
		return lo
	}
	if n > hi {
		return hi
	}
	return n
}

// preCompactHomeDomain resolves the caller's home domain via the signed self
// profile, under the command deadline.
func preCompactHomeDomain(ctx context.Context) (string, error) {
	var self struct {
		HomeDomain string `json:"home_domain"`
	}
	if err := hookSignedJSONCtx(ctx, http.MethodGet, "/v1/agent/me", nil, &self); err != nil {
		return "", err
	}
	d := strings.TrimSpace(self.HomeDomain)
	if d == "" {
		return "", fmt.Errorf("no home domain for caller")
	}
	return d, nil
}

func baseName(p string) string { return filepath.Base(p) }

// preCompactTranscriptRoot is the host-specific trusted root a transcript path
// must resolve within. Defaults to the Claude projects directory; overridable for
// headless/centrally-managed hosts.
func preCompactTranscriptRoot() (string, error) {
	if v := strings.TrimSpace(os.Getenv("SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT")); v != "" {
		return filepath.Clean(expandTilde(v)), nil
	}
	home, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("resolve home dir: %w", err)
	}
	return filepath.Join(home, ".claude", "projects"), nil
}

// transcriptRelComponents resolves payloadPath to a lexical path under the
// (symlink-resolved) trusted root and returns the ordered path components from the
// root to the target, plus the canonical root. It rejects any path that escapes
// the root or a non-.jsonl target. The components are walked with no-follow
// semantics by the platform opener, so ancestor symlinks cannot redirect them.
func transcriptRelComponents(payloadPath string) (root string, components []string, err error) {
	if strings.TrimSpace(payloadPath) == "" {
		return "", nil, fmt.Errorf("empty transcript path")
	}
	abs, err := filepath.Abs(expandTilde(payloadPath))
	if err != nil {
		return "", nil, fmt.Errorf("resolve path: %w", err)
	}
	abs = filepath.Clean(abs)
	if !strings.EqualFold(filepath.Ext(abs), ".jsonl") {
		return "", nil, fmt.Errorf("not a .jsonl transcript")
	}
	rawRoot, err := preCompactTranscriptRoot()
	if err != nil {
		return "", nil, err
	}
	rootRaw := filepath.Clean(rawRoot)
	// The root itself is trusted; resolve it once so the no-follow walk starts from a
	// real directory. Only the components BELOW the root are walked no-follow.
	rootReal, err := filepath.EvalSymlinks(rawRoot)
	if err != nil {
		rootReal = rootRaw
	}
	// Compare in a SINGLE namespace: abs is uncanonicalised (filepath.Abs, no symlink
	// resolution), so the relative path must be taken against the equally uncanonicalised
	// rootRaw — otherwise a symlinked ancestor of the trusted root (e.g. macOS
	// /var → /private/var) makes a valid transcript look like it is outside the root.
	// rootReal (canonical) is still the directory the no-follow walk starts from, and the
	// below-root components are walked no-follow unchanged, so this fixes the comparison
	// WITHOUT resolving any user-controlled path symlink.
	rel, err := filepath.Rel(rootRaw, abs)
	if err != nil {
		return "", nil, fmt.Errorf("path not under trusted root")
	}
	if rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return "", nil, fmt.Errorf("transcript is outside the trusted root")
	}
	for _, c := range strings.Split(rel, string(os.PathSeparator)) {
		if c == "" || c == "." || c == ".." {
			return "", nil, fmt.Errorf("illegal path component")
		}
		components = append(components, c)
	}
	if len(components) == 0 {
		return "", nil, fmt.Errorf("empty relative path")
	}
	return rootReal, components, nil
}

// ── sidecar persistence (atomic) + ctx-aware per-thread lock ────────────────

func captureProgressPath(threadID string) string {
	sum := sha256.Sum256([]byte(threadID))
	return filepath.Join(neverCompactHomeDir(), hex.EncodeToString(sum[:])+".json")
}

func loadCaptureProgress(threadID string) *captureProgress {
	prog := &captureProgress{Version: preCompactSidecarVersion, ThreadID: threadID}
	rawb, err := os.ReadFile(captureProgressPath(threadID))
	if err != nil {
		return prog
	}
	var loaded captureProgress
	if json.Unmarshal(rawb, &loaded) != nil {
		return prog // corrupt sidecar: start fresh (recall-time dedup is the backstop)
	}
	if loaded.ThreadID == threadID {
		return &loaded
	}
	return prog
}

func saveCaptureProgress(prog *captureProgress) {
	rawb, err := json.MarshalIndent(prog, "", "  ")
	if err != nil {
		return
	}
	if werr := neverCompactAtomicWrite(captureProgressPath(prog.ThreadID), rawb); werr != nil {
		fmt.Fprintf(os.Stderr, "nevercompact: persist progress: %v\n", werr)
	}
}

// acquireCaptureLock serializes concurrent PreCompact invocations for one thread
// with an atomic mkdir lock (portable; no flock dependency). It waits under the
// caller's command deadline (blocker 5), reclaims a stale lock from a crashed
// hook, and fails when the deadline is reached.
func acquireCaptureLock(ctx context.Context, threadID string) (func(), error) {
	sum := sha256.Sum256([]byte(threadID))
	lockPath := filepath.Join(neverCompactHomeDir(), hex.EncodeToString(sum[:])+".lock")
	if err := os.MkdirAll(neverCompactHomeDir(), 0o700); err != nil {
		return nil, err
	}
	for {
		if err := os.Mkdir(lockPath, 0o700); err == nil {
			return func() { _ = os.Remove(lockPath) }, nil
		} else if !os.IsExist(err) {
			return nil, err
		}
		if info, statErr := os.Stat(lockPath); statErr == nil && time.Since(info.ModTime()) > 10*time.Second {
			_ = os.Remove(lockPath)
			continue
		}
		if ctx.Err() != nil {
			return nil, fmt.Errorf("deadline reached acquiring thread lock")
		}
		select {
		case <-ctx.Done():
			return nil, fmt.Errorf("deadline reached acquiring thread lock")
		case <-time.After(5 * time.Millisecond):
		}
	}
}
