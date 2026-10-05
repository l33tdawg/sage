package hunch

import (
	"encoding/json"
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// Conformance: the Go port's label reader must return the SAME probability as the reader that produced
// the shipped model's qualification numbers (eval_rev.py / Hunch's engine._parse rule — chosen token is a
// label, renormalise over present label mass). The fixtures are real Ollama /v1 responses (reasoning off,
// top_logprobs=20) for frozen reversed-relation items, each with the qualification reader's expected p.
// If this drifts, the shipped judge is no longer the thing that was measured.
func TestConformance_MatchesQualificationReader(t *testing.T) {
	raw, err := os.ReadFile("testdata_conformance.json")
	require.NoError(t, err)
	var items []struct {
		Flavour   string          `json:"flavour"`
		Label     string          `json:"label"`
		Response  json.RawMessage `json:"response"`
		ExpectedP float64         `json:"expected_p"`
	}
	require.NoError(t, json.Unmarshal(raw, &items))
	require.GreaterOrEqual(t, len(items), 20, "expected a representative fixture set")

	var maxDelta float64
	for _, it := range items {
		got, err := parseLabelProbability(it.Response)
		require.NoError(t, err, "flavour=%s label=%s", it.Flavour, it.Label)
		d := math.Abs(got - it.ExpectedP)
		if d > maxDelta {
			maxDelta = d
		}
		require.LessOrEqualf(t, d, 1e-6,
			"per-item divergence from the qualification reader: flavour=%s label=%s got=%.6f want=%.6f",
			it.Flavour, it.Label, got, it.ExpectedP)
	}
	t.Logf("conformance max|Δp| over %d items = %.2e", len(items), maxDelta)
}
