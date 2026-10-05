package web

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

// The recall floor is set through CEREBRUM, and the value a node stores can be
// above a tier the write path produces without anyone choosing it deliberately:
// the save handler's own comment records that the input floor used to be 85, so a
// node that was saved once under that build carries a floor above the observation
// tier forever. The warning is what makes that visible where it is read and set.
func TestRecallFloorTierWarning(t *testing.T) {
	for _, tc := range []struct {
		percent  int
		mentions []string
		empty    bool
	}{
		{percent: 50, empty: true},
		{percent: 60, empty: true}, // exactly at the inference tier: the filter keeps records AT the floor
		{percent: 70, mentions: []string{"inferences"}},
		{percent: 80, mentions: []string{"inferences"}}, // at the observation tier: observations still pass
		{percent: 85, mentions: []string{"observations", "inferences"}},
		{percent: 96, mentions: []string{"facts", "observations", "inferences"}},
	} {
		warning := recallFloorTierWarning(tc.percent)
		if tc.empty {
			assert.Empty(t, warning, "floor %d hides no tier and must not warn", tc.percent)
			continue
		}
		for _, want := range tc.mentions {
			assert.True(t, strings.Contains(warning, want),
				"floor %d must name the %s tier it hides: %q", tc.percent, want, warning)
		}
		// The remedy is part of the message: an operator who sees this needs to
		// know the value is the cause and what the default is.
		assert.Contains(t, warning, "default is 70")
		assert.Contains(t, warning, "tag or list lookup")
	}
}
