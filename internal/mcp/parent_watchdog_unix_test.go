//go:build !windows

package mcp

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMCPParentPIDChangeRequiresObservableReparenting(t *testing.T) {
	for _, tc := range []struct {
		name    string
		initial int
		current int
		gone    bool
	}{
		{"same live parent", 123, 123, false},
		{"reparented to init", 123, 1, true},
		{"reparented to subreaper", 123, 456, true},
		{"container client is init", 1, 1, false},
		{"client outside PID namespace", 0, 0, false},
		{"initial parent unavailable", 0, 123, false},
		{"current parent unavailable", 123, 0, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.gone, mcpParentPIDChanged(tc.initial, tc.current))
		})
	}
}

func TestMCPParentObserverAllowsContainerPIDZero(t *testing.T) {
	parent, err := newMCPParentProcess(0)
	require.NoError(t, err)
	defer parent.close()
	require.False(t, parent.exited(), "an unavailable creator PID cannot prove a dead client")
}
