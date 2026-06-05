package voter

import (
	"context"
	"testing"
)

type fakeDup struct{ dups map[string]bool }

func (f fakeDup) FindByContentHash(_ context.Context, h string) (bool, error) {
	return f.dups[h], nil
}

// TestDecide pins behavior parity with the legacy 4-archetype validators
// (cmd/sage-gui/node.go before the per-node voter): the same dedup/quality/
// consistency rules, now collapsed into one verdict.
func TestDecide(t *testing.T) {
	ctx := context.Background()
	good := "this is a sufficiently long and substantive memory body"

	cases := []struct {
		name   string
		in     MemoryInput
		dups   map[string]bool
		accept bool
	}{
		{"clean accept", MemoryInput{Content: good, ContentHash: "abc12345", Domain: "go-debugging", MemType: "observation", Confidence: 0.8}, nil, true},
		{"too short", MemoryInput{Content: "short", Domain: "d", MemType: "observation", Confidence: 0.8}, nil, false},
		{"noise pattern", MemoryInput{Content: "the user said hi to the assistant today", Domain: "d", MemType: "observation", Confidence: 0.8}, nil, false},
		{"empty reflection", MemoryInput{Content: "[Task Reflection] tiny note here", Domain: "d", MemType: "observation", Confidence: 0.8}, nil, false},
		{"fact low confidence", MemoryInput{Content: good, Domain: "d", MemType: "fact", Confidence: 0.6}, nil, false},
		{"confidence too low", MemoryInput{Content: good, Domain: "d", MemType: "observation", Confidence: 0.2}, nil, false},
		{"empty domain", MemoryInput{Content: good, Domain: "", MemType: "observation", Confidence: 0.8}, nil, false},
		{"duplicate content", MemoryInput{Content: good, ContentHash: "duphash00", Domain: "d", MemType: "observation", Confidence: 0.8}, map[string]bool{"duphash00": true}, false},
		{"fact high confidence accepts", MemoryInput{Content: good, Domain: "d", MemType: "fact", Confidence: 0.8}, nil, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			d := Decide(ctx, fakeDup{dups: c.dups}, c.in)
			if d.Accept != c.accept {
				t.Fatalf("Decide(%s) accept=%v reason=%q, want accept=%v", c.name, d.Accept, d.Reason, c.accept)
			}
			if !d.Accept && d.Reason == "" {
				t.Fatalf("Decide(%s) rejected without a reason", c.name)
			}
		})
	}
}

// TestDecideVerbose_ReportsNamedChecks confirms the advisory display gets all three
// named checks (so the dashboard renders the same rule set the vote applies).
func TestDecideVerbose_ReportsNamedChecks(t *testing.T) {
	ctx := context.Background()
	decision, checks := DecideVerbose(ctx, fakeDup{}, MemoryInput{
		Content: "this is long enough to pass the quality gate", Domain: "d", MemType: "observation", Confidence: 0.8,
	})
	if !decision.Accept {
		t.Fatalf("clean memory should accept, got reject: %s", decision.Reason)
	}
	if len(checks) != 3 {
		t.Fatalf("want 3 named checks, got %d", len(checks))
	}
	names := map[string]bool{}
	for _, c := range checks {
		names[c.Name] = true
		if !c.Pass {
			t.Fatalf("check %q unexpectedly failed: %s", c.Name, c.Reason)
		}
	}
	for _, want := range []string{"dedup", "quality", "consistency"} {
		if !names[want] {
			t.Fatalf("DecideVerbose missing check %q", want)
		}
	}
}

// TestDecideVerbose_RejectReason confirms the aggregate reason is the first failing
// check's reason while all checks are still reported.
func TestDecideVerbose_RejectReason(t *testing.T) {
	ctx := context.Background()
	decision, checks := DecideVerbose(ctx, fakeDup{}, MemoryInput{
		Content: "x", Domain: "d", MemType: "observation", Confidence: 0.8, // too short → quality fails
	})
	if decision.Accept {
		t.Fatal("short content should be rejected")
	}
	if len(checks) != 3 {
		t.Fatalf("want 3 checks reported even on reject, got %d", len(checks))
	}
}
