// Package voter implements SAGE's per-node memory auto-voter: the component that
// moves a proposed memory toward "committed" by signing a MemoryVote tx with the
// node's OWN single consensus validator key and broadcasting it through CometBFT
// consensus. There is one validator per node — the honest BFT unit — not a bag of
// simulated sub-keys. The node's vote counts toward the same 2/3 quorum the chain
// already tallies in checkAndApplyQuorum.
//
// This package is the single shared implementation used by both the production
// daemon (cmd/amid) and the personal node (cmd/sage-gui). It deliberately depends
// only on the tx/memory packages and an App interface — never on internal/abci —
// so there is no import cycle and the decision logic is unit-testable in isolation.
package voter

import (
	"context"
	"fmt"
	"strings"
)

// Decision is one node's verdict on a proposed memory: accept iff every check
// passes, otherwise reject carrying the first failing check's reason.
type Decision struct {
	Accept bool
	Reason string
}

// CheckResult is the outcome of a single named check, surfaced by DecideVerbose so
// the dashboard / REST advisory display and the real on-chain vote share one rule
// set (no second drifting copy).
type CheckResult struct {
	Name   string
	Pass   bool
	Reason string
}

// MemoryInput is the minimal projection of a memory the decider needs.
type MemoryInput struct {
	Content     string
	ContentHash string // hex-encoded
	Domain      string
	MemType     string
	Confidence  float64
}

// DupChecker abstracts the local content-hash lookup (store.FindByContentHash).
// It is a node-local read producing this node's opinion — never consensus state —
// so different nodes disagreeing is fine; the quorum tally resolves it
// deterministically.
type DupChecker interface {
	FindByContentHash(ctx context.Context, contentHash string) (bool, error)
}

// noisePatterns are low-value observation fingerprints rejected by the quality
// check. Kept verbatim from the legacy 4-archetype "quality" validator so behavior
// is unchanged — only the packaging (4 keys → 1 policy) changes.
var noisePatterns = []string{
	"user said hi", "user greeted", "session started",
	"brain online", "brain is awake", "no action taken",
	"user said morning", "new session started",
}

// Decide collapses the three meaningful archetype validators (dedup, quality,
// consistency) into ONE per-node verdict: accept iff all three pass, else reject
// with the first failing reason. The legacy "sentinel" (baseline-accept) archetype
// is intentionally dropped — a vote that never rejects was a liveness hack for the
// old 4-vote quota and is meaningless for a single per-node vote.
func Decide(ctx context.Context, dup DupChecker, m MemoryInput) Decision {
	decision, _ := DecideVerbose(ctx, dup, m)
	return decision
}

// DecideVerbose returns the aggregate Decision plus each named check's result, for
// the advisory pre-vote display. Checks run in order dedup → quality → consistency;
// the aggregate rejects with the first failing reason.
func DecideVerbose(ctx context.Context, dup DupChecker, m MemoryInput) (Decision, []CheckResult) {
	checks := []CheckResult{
		dedupCheck(ctx, dup, m),
		qualityCheck(m),
		consistencyCheck(m),
	}
	for _, c := range checks {
		if !c.Pass {
			return Decision{Accept: false, Reason: c.Reason}, checks
		}
	}
	return Decision{Accept: true, Reason: "passes all checks"}, checks
}

func dedupCheck(ctx context.Context, dup DupChecker, m MemoryInput) CheckResult {
	// On lookup error, treat as non-duplicate (accept) — matches the legacy
	// `err == nil && exists` reject condition. A node never blocks a memory on its
	// own store hiccup; the quorum still decides.
	if dup != nil {
		if exists, err := dup.FindByContentHash(ctx, m.ContentHash); err == nil && exists {
			short := m.ContentHash
			if len(short) > 8 {
				short = short[:8]
			}
			return CheckResult{Name: "dedup", Pass: false, Reason: fmt.Sprintf("duplicate content (hash: %s)", short)}
		}
	}
	return CheckResult{Name: "dedup", Pass: true, Reason: "content is unique"}
}

func qualityCheck(m MemoryInput) CheckResult {
	if len(m.Content) < 20 {
		return CheckResult{Name: "quality", Pass: false, Reason: fmt.Sprintf("content too short (%d chars, minimum 20)", len(m.Content))}
	}
	lower := strings.ToLower(m.Content)
	for _, p := range noisePatterns {
		if strings.Contains(lower, p) {
			return CheckResult{Name: "quality", Pass: false, Reason: "low-value observation: matches noise pattern"}
		}
	}
	if strings.HasPrefix(m.Content, "[Task Reflection]") && len(m.Content) < 60 {
		return CheckResult{Name: "quality", Pass: false, Reason: "empty reflection header without substance"}
	}
	return CheckResult{Name: "quality", Pass: true, Reason: "content passes quality check"}
}

func consistencyCheck(m MemoryInput) CheckResult {
	if m.Confidence < 0.3 {
		return CheckResult{Name: "consistency", Pass: false, Reason: fmt.Sprintf("confidence too low (%.2f)", m.Confidence)}
	}
	if m.MemType == "fact" && m.Confidence < 0.7 {
		return CheckResult{Name: "consistency", Pass: false, Reason: fmt.Sprintf("facts require confidence >= 0.7 (got %.2f)", m.Confidence)}
	}
	if m.Domain == "" {
		return CheckResult{Name: "consistency", Pass: false, Reason: "domain required"}
	}
	return CheckResult{Name: "consistency", Pass: true, Reason: "passes consistency check"}
}
