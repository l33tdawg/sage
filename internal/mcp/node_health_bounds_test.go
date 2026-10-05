package mcp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests try to FALSIFY the node-health guidance contract: every shape the
// node can answer with must produce advice that is either correct or explicitly
// says it cannot tell. The failure this guards against is not a crash — it is a
// confident sentence that sends an agent to wait for a fence that will never
// lift, which is exactly the field report this tool was written for.

func TestNodeHealthGuidanceNeverGuesses(t *testing.T) {
	cases := []struct {
		name             string
		fences           map[string]any
		want             []string
		notWant          []string
		expectEmptyReply bool
	}{
		{
			name:             "no fence block at all",
			fences:           nil,
			expectEmptyReply: true,
		},
		{
			name:    "active count is not a number",
			fences:  map[string]any{"active": "one"},
			want:    []string{"did not report a usable"},
			notWant: []string{"clears itself", "no signing key is fenced"},
		},
		{
			name:    "unknown resolution class",
			fences:  map[string]any{"active": float64(1), "signers": []any{map[string]any{"resolution": "future_class"}}},
			want:    []string{"did not report a resolution class"},
			notWant: []string{"clears itself", "will NOT clear"},
		},
		{
			name: "mixed classes",
			fences: map[string]any{"active": float64(2), "signers": []any{
				map[string]any{"resolution": "reconciling"},
				map[string]any{"resolution": "proof_or_operator"},
			}},
			want: []string{"Both classes", "reconciling", "proof_or_operator"},
		},
		{
			name:    "active count exceeds disclosed rows",
			fences:  map[string]any{"active": float64(3), "signers": []any{map[string]any{"resolution": "reconciling"}}},
			want:    []string{"reconciling"},
			notWant: []string{"Both classes"},
		},
		{
			name:    "signers is not an array",
			fences:  map[string]any{"active": float64(1), "signers": "yes"},
			want:    []string{"cannot read"},
			notWant: []string{"clears itself", "did not disclose"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			guidance := signerFenceGuidance(tc.fences)
			if tc.expectEmptyReply {
				assert.Empty(t, guidance, "a missing block must not produce a claim")
				return
			}
			require.NotEmpty(t, guidance, "every parseable block must produce a sentence")
			for _, want := range tc.want {
				assert.Contains(t, guidance, want)
			}
			for _, notWant := range tc.notWant {
				assert.NotContains(t, guidance, notWant)
			}
		})
	}
}

// TestNodeHealthGuidanceForARestoredFenceNeverSaysWait is the safety property
// stated on its own: the one answer that is dangerous for a fence whose bytes
// are gone is "wait for it to clear".
func TestNodeHealthGuidanceForARestoredFenceNeverSaysWait(t *testing.T) {
	guidance := signerFenceGuidance(map[string]any{
		"active": float64(1),
		"signers": []any{map[string]any{
			"signer": "abc", "resolution": "proof_or_operator",
		}},
	})
	require.Contains(t, guidance, "will NOT clear on its own")
	require.Contains(t, guidance, "operator abandon")
	assert.NotContains(t, guidance, "clears itself",
		"a restored fence must never be described as one that clears itself")
}
