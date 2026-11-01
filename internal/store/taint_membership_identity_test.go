package store

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestTaintMembershipReadsIsolateExactMemoryID(t *testing.T) {
	s := openTaintStore(t)
	decisionID := testTaintDecisionID("colon-neighbour")
	created, err := s.ApplyTaintDecision("sage", decisionID,
		testTaintParent("victim:x"), testTaintPayloadHash("colon-neighbour"))
	require.NoError(t, err)
	require.True(t, created)
	decisions, err := s.GetEffectiveTaintDecisions("victim:x")
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)
	decisions, err = s.GetEffectiveTaintDecisions("other")
	require.NoError(t, err)
	require.Empty(t, decisions)
	decisions, err = s.GetEffectiveTaintDecisions("victim")
	require.NoError(t, err, "another memory's membership must not corrupt this identity")
	require.Empty(t, decisions)
}

func TestTaintInheritanceBudgetExcludesPrefixNeighbours(t *testing.T) {
	s := openTaintStore(t)
	parent := testTaintParent("root")
	decisionID := testTaintDecisionID("bounded-neighbours")
	created, err := s.ApplyTaintDecision("sage", decisionID, parent,
		testTaintPayloadHash("bounded-neighbours"))
	require.NoError(t, err)
	require.True(t, created)
	_, err = s.InheritTaintMemberships("sage", "victim", []tx.ParentRef{parent})
	require.NoError(t, err)
	before, err := s.ValidateTaintInheritance("sage", "child", []tx.ParentRef{testTaintParent("victim")})
	require.NoError(t, err)
	for i := 0; i <= taint.MaxActiveDecisions; i++ {
		_, err = s.InheritTaintMemberships("sage", fmt.Sprintf("victim:%02d", i), []tx.ParentRef{parent})
		require.NoError(t, err)
	}
	after, err := s.ValidateTaintInheritance("sage", "child", []tx.ParentRef{testTaintParent("victim")})
	require.NoError(t, err, "valid neighbouring memberships must not consume this parent's cap or units")
	require.Equal(t, before, after)
}

func TestLineageReadsIsolateExactChildID(t *testing.T) {
	s := openTaintStore(t)
	require.NoError(t, s.SetMemoryLineage("victim:x", []tx.ParentRef{testTaintParent("parent")}))
	refs, err := s.GetMemoryLineage("victim:x")
	require.NoError(t, err)
	require.Len(t, refs, 1)
	refs, err = s.GetMemoryLineage("victim")
	require.NoError(t, err, "another child's forward edge must not corrupt this identity")
	require.Empty(t, refs)
}
