package mcp

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The confidence floor is a hard filter applied before ranking, so a caller
// cannot recover from it: it has to be TOLD. The failure this pins is a memory
// that was committed and visible to a tag lookup while being unreachable by the
// recall path a fresh session uses at boot, because the node's floor sat above
// the tier it was written with.
func TestConfidenceFloorDisclosure(t *testing.T) {
	floorAt := func(v float64) *float64 { return &v }
	count := func(v int) *int { return &v }

	t.Run("no floor reported adds nothing", func(t *testing.T) {
		_, _, _, ok := confidenceFloorDisclosure(nil)
		assert.False(t, ok, "a node that reported no floor must not have one invented for it")
		_, _, _, ok = confidenceFloorDisclosure(&recallFilterInfo{})
		assert.False(t, ok, "an envelope without a floor value proves nothing about filtering")
	})

	t.Run("a floor that hid nothing is still disclosed", func(t *testing.T) {
		floor, hidden, note, ok := confidenceFloorDisclosure(&recallFilterInfo{
			ConfidenceFloor: floorAt(0.85), HiddenByConfidenceFloor: count(0),
		})
		require.True(t, ok)
		assert.Equal(t, 0.85, floor)
		assert.Zero(t, hidden)
		assert.Empty(t, note, "nothing was hidden, so there is nothing to explain")
	})

	t.Run("hidden candidates are explained", func(t *testing.T) {
		floor, hidden, note, ok := confidenceFloorDisclosure(&recallFilterInfo{
			ConfidenceFloor: floorAt(0.85), HiddenByConfidenceFloor: count(2),
		})
		require.True(t, ok)
		assert.Equal(t, 0.85, floor)
		assert.Equal(t, 2, hidden)
		// The note has to name the count, the floor, the remedy, and the tiers a
		// floor above them hides — those are the four facts that turn a silent
		// empty result into a diagnosable one.
		assert.Contains(t, note, "2 candidate(s)")
		assert.Contains(t, note, "0.85")
		assert.Contains(t, note, "min_confidence")
		assert.Contains(t, note, "0.80")
		assert.Contains(t, note, "0.60")
		assert.NotContains(t, note, "%!", "the note is formatted, not a raw format string")
	})

	t.Run("a hidden count without a floor value is ignored", func(t *testing.T) {
		// Defensive: a count with no threshold cannot be explained to a caller,
		// and inventing a floor would be worse than saying nothing.
		_, _, _, ok := confidenceFloorDisclosure(&recallFilterInfo{HiddenByConfidenceFloor: count(3)})
		assert.False(t, ok)
	})
}

func TestRecallFloorNoteIsSingleLine(t *testing.T) {
	// The note lands in tool output that agents read as prose; a stray newline
	// would truncate it in any line-oriented rendering.
	floor := 0.9
	hidden := 5
	_, _, note, ok := confidenceFloorDisclosure(&recallFilterInfo{
		ConfidenceFloor: &floor, HiddenByConfidenceFloor: &hidden,
	})
	require.True(t, ok)
	assert.NotContains(t, note, "\n")
	assert.True(t, strings.HasPrefix(note, "5 candidate(s)"))
}
