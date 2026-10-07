package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"
)

// nevercompact_recall.go — RECALL half of recall-backed compaction.
//
// On SessionStart (resume), this restores the WHOLE captured thread verbatim, in
// order — not the newest-ten home-domain prefetch. It derives the durable thread
// identity from the resumed transcript's first row (the same identity the capture
// path tagged records with), pulls every committed chunk for that thread under one
// bounded deadline, decodes the byte-exact unit encoding, deduplicates by (seq,
// part), and reconstructs the turns in exact order for the harness to inject.
//
// Recall reads already-governed data, so it is not consent-gated; it simply finds
// nothing when nothing was captured.

const (
	recallDefaultBudgetMS = 8_000
	recallMaxBudgetMS     = 20_000
	recallPageSize        = 200
)

func recallBudget() time.Duration {
	ms := envIntClamped("SAGE_NEVERCOMPACT_RECALL_BUDGET_MS", recallDefaultBudgetMS, 1_000, recallMaxBudgetMS)
	return time.Duration(ms) * time.Millisecond
}

// emitThreadScopedRecall reads the SessionStart hook payload, derives the thread
// identity from its transcript, and prints the complete captured thread. It is
// best-effort and silent on any error or when nothing was captured. All network
// work runs under one command-entry deadline (blocker 5).
func emitThreadScopedRecall(payload []byte) {
	if len(bytes.TrimSpace(payload)) == 0 {
		return
	}
	var in preCompactInput // shares session_id / transcript_path fields
	if json.Unmarshal(payload, &in) != nil || strings.TrimSpace(in.TranscriptPath) == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), recallBudget())
	defer cancel()

	f, err := openValidatedTranscript(in.TranscriptPath)
	if err != nil {
		return
	}
	defer f.Close()
	threadID := firstRowSessionID(f)
	if threadID == "" {
		return
	}

	domain, err := preCompactHomeDomain(ctx)
	if err != nil {
		return
	}
	units, err := recallThreadUnits(ctx, threadID, domain)
	if err != nil || len(units) == 0 {
		return
	}
	turns := reconstructTurns(units)
	if len(turns) == 0 {
		return
	}

	fmt.Printf("SAGE recall-backed compaction: restored %d captured turn(s) for this thread (verbatim, in order):\n\n", len(turns))
	for _, t := range turns {
		if t.role == unitRoleGap {
			fmt.Printf("%s\n", t.text)
			continue
		}
		fmt.Printf("--- %s ---\n%s\n\n", t.role, t.text)
	}
	fmt.Println("(The above is restored conversation context recovered from before the last compaction.)")
}

// recallThreadUnits pulls every committed chunk for the thread under the deadline,
// paginating to completeness, decodes each byte-exact chunk body into units, and
// deduplicates by (seq, part) — so a rare duplicate submit collapses to one.
func recallThreadUnits(ctx context.Context, threadID, domain string) ([]captureUnit, error) {
	seen := map[[2]int]bool{}
	var units []captureUnit
	for offset := 0; ; offset += recallPageSize {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		q := url.Values{}
		q.Set("tag", neverCompactThreadTagPrefix+threadID)
		q.Set("status", "committed")
		q.Set("domain", domain)
		q.Set("sort", "oldest")
		q.Set("limit", fmt.Sprintf("%d", recallPageSize))
		q.Set("offset", fmt.Sprintf("%d", offset))
		var payload struct {
			Memories []struct {
				Content string `json:"content"`
			} `json:"memories"`
		}
		if err := hookSignedJSONCtx(ctx, http.MethodGet, "/v1/memory/list?"+q.Encode(), nil, &payload); err != nil {
			return nil, err
		}
		if len(payload.Memories) == 0 {
			break
		}
		for _, m := range payload.Memories {
			for _, u := range decodeChunkUnits(m.Content) {
				key := [2]int{u.Seq, u.Part}
				if seen[key] {
					continue
				}
				seen[key] = true
				units = append(units, u)
			}
		}
		if len(payload.Memories) < recallPageSize {
			break
		}
	}
	return units, nil
}

// recalledTurn is one reconstructed source turn (its parts concatenated).
type recalledTurn struct {
	seq  int
	role string
	text string
}

// reconstructTurns orders units by (seq, part) and concatenates the parts of each
// source row back into its exact original text — the byte-exact inverse of the
// oversize split.
func reconstructTurns(units []captureUnit) []recalledTurn {
	sort.SliceStable(units, func(i, j int) bool {
		if units[i].Seq != units[j].Seq {
			return units[i].Seq < units[j].Seq
		}
		return units[i].Part < units[j].Part
	})
	var turns []recalledTurn
	var cur *recalledTurn
	for _, u := range units {
		if cur == nil || cur.seq != u.Seq {
			turns = append(turns, recalledTurn{seq: u.Seq, role: u.Role, text: u.Text})
			cur = &turns[len(turns)-1]
			continue
		}
		cur.text += u.Text
	}
	return turns
}

// firstRowSessionID reads the transcript's first row carrying a sessionId — the
// durable thread identity. Bounded read; returns "" if none is found early.
func firstRowSessionID(f *os.File) string {
	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 0, 64<<10), preCompactMaxLineBytes)
	scanned := 0
	for scanner.Scan() && scanned < 4096 {
		scanned++
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var row transcriptRow
		if json.Unmarshal(line, &row) != nil {
			continue
		}
		if s := strings.TrimSpace(row.SessionID); s != "" {
			return s
		}
	}
	return ""
}
