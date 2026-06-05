package abci

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/validator"
)

// A single-node chain commits on its own validator's accept (1.0 >= 2/3) — the
// per-node voter model needs no 4-archetype simulation for liveness.
func TestMemoryVoter_SingleNodeCommits(t *testing.T) {
	app := setupTestApp(t)
	v := newAgentKey(t)
	require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: v.id, Power: 10}))

	submitMemory(t, app, v, "m1", 100)
	castVote(t, app, v, "m1", true, 100)

	require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, "m1"),
		"single validator's accept must commit")
}

// The core multi-node regression guard. Three independent nodes share the SAME
// genesis validator set (as quorum-init/join produces) and each receives the
// IDENTICAL ordered block stream of memory submits + per-node votes. Two
// properties must hold:
//
//  1. votes signed by independent per-node consensus keys drive the 2/3 quorum
//     correctly (supermajority commits; a lone vote stays proposed), and
//  2. every node computes a BYTE-IDENTICAL AppHash — i.e. no node ever did the old
//     local validator:* clobber that forked the chain. This is the exact property
//     whose absence halted a real multi-node cluster.
func TestMemoryVoter_MultiNodeAppHashDeterminism(t *testing.T) {
	// Stable validator identities, reused across every node instance.
	keys := []agentKey{newAgentKey(t), newAgentKey(t), newAgentKey(t)}

	newNode := func() *SageApp {
		app := setupTestApp(t)
		for _, k := range keys {
			require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{ID: k.id, Power: 10}))
		}
		return app
	}
	nodes := []*SageApp{newNode(), newNode(), newNode()}

	for _, app := range nodes {
		// Supermajority accept → commits.
		submitMemory(t, app, keys[0], "commit-me", 100)
		castVote(t, app, keys[0], "commit-me", true, 100)
		castVote(t, app, keys[1], "commit-me", true, 100)
		castVote(t, app, keys[2], "commit-me", true, 100)

		// A single vote (1 of 3) → never reaches 2/3 → stays proposed.
		submitMemory(t, app, keys[0], "stay-proposed", 100)
		castVote(t, app, keys[0], "stay-proposed", true, 100)
	}

	for i, app := range nodes {
		require.Equal(t, string(memory.StatusCommitted), statusOf(t, app, "commit-me"),
			"node%d: supermajority accept must commit", i)
		require.Equal(t, string(memory.StatusProposed), statusOf(t, app, "stay-proposed"),
			"node%d: a lone vote must NOT reach the 2/3 quorum", i)
	}

	want, err := nodes[0].badgerStore.ComputeAppHash()
	require.NoError(t, err)
	for i := 1; i < len(nodes); i++ {
		got, err := nodes[i].badgerStore.ComputeAppHash()
		require.NoError(t, err)
		require.Equal(t, want, got, "node%d AppHash diverged from node0 — consensus fork", i)
	}
}
