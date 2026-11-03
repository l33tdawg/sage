package rest

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func (m *mockMemoryStore) DomainSpaceCompletenessAtTaintRevision(
	ctx context.Context, domain, activeSpace string, cap int, expected uint64,
) (hasOutside, established, mirrorMatched bool, err error) {
	m.taintCompletenessCalled = true
	m.taintCompletenessExpected = expected
	// Capture the query snapshot match BEFORE the interleaving callback. A
	// later revision value must never retroactively certify this statement.
	matched := m.taintCompletenessMirrorRevision == expected
	outside, complete, err := m.DomainSpaceCompleteness(ctx, domain, activeSpace, cap)
	return outside, complete, matched, err
}

func seedRecallCompletenessDecision(t *testing.T, badger *store.BadgerStore) string {
	t.Helper()
	id := strings.Repeat("a", taint.DecisionIDBytes)
	target := "recall-completeness-target"
	hash := sha256.Sum256([]byte(target))
	require.NoError(t, badger.SetMemoryHash(target, hash[:], "committed"))
	created, err := badger.ApplyTaintDecision("sage", id,
		tx.ParentRef{SourceChainID: "sage", MemoryID: target, ContentHash: hash},
		sha256.Sum256([]byte("recall completeness payload")))
	require.NoError(t, err)
	require.True(t, created)
	return id
}

func TestRecallTaintCompletenessRequiresExpansionAndMatchingSnapshot(t *testing.T) {
	srv, badger, _, _, _ := setupAppV23RESTAccess(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	id := seedRecallCompletenessDecision(t, badger)
	caller := fullVisibilityCaller(t, srv, badger, "fv.home")
	mock := mockStoreOf(srv)
	mock.outside = outsideSpaceProbe{hasOutside: false, established: true}
	_, pending := queryEmptyRecall(t, srv, caller, "fv.home", "committed")
	require.Equal(t, "unavailable", pending.IndexStatus)
	require.False(t, mock.taintCompletenessCalled)
	require.Nil(t, pending.TaintCoverage, "header proof uncertainty is not an ancestor-walk event")

	expanded, err := badger.ExpandTaintDecision("sage", id, taint.PerExecutionUnitBudget)
	require.NoError(t, err)
	require.Equal(t, taint.StatusComplete, expanded.Status)
	// Re-establish the independently required canonical projection source after
	// real taint work; the probe cannot waive the existing disclosure fence.
	projection, err := mock.MemoryProjectionRevision(context.Background())
	require.NoError(t, err)
	badger.PublishCanonicalMemoryProjectionAuditAt(true, false, false,
		projection, badger.CanonicalMemoryProjectionRevision(), mock.VaultGeneration())
	revision, err := badger.TaintRevision()
	require.NoError(t, err)
	_, stale := queryEmptyRecall(t, srv, caller, "fv.home", "committed")
	require.True(t, mock.taintCompletenessCalled)
	require.Equal(t, revision, mock.taintCompletenessExpected)
	require.Equal(t, "unavailable", stale.IndexStatus)
	require.Nil(t, stale.TaintCoverage)
	mock.taintCompletenessMirrorRevision = revision
	_, exact := queryEmptyRecall(t, srv, caller, "fv.home", "committed")
	require.Equal(t, "complete", exact.IndexStatus)
	require.Nil(t, exact.TaintCoverage)
	mock.outside.after = func() {
		changed, err := badger.ClearTaintDecision(id)
		require.NoError(t, err)
		require.True(t, changed)
	}
	_, changed := queryEmptyRecall(t, srv, caller, "fv.home", "committed")
	require.Equal(t, "unavailable", changed.IndexStatus)
}

func TestRecallTaintCompletenessRejectsFirstTaintDuringProbe(t *testing.T) {
	srv, badger, _, _, _ := setupAppV23RESTAccess(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	caller := fullVisibilityCaller(t, srv, badger, "fv.home")
	mock := mockStoreOf(srv)
	mock.outside = outsideSpaceProbe{hasOutside: false, established: true}
	mock.outside.after = func() { seedRecallCompletenessDecision(t, badger) }
	_, response := queryEmptyRecall(t, srv, caller, "fv.home", "committed")
	require.Equal(t, "unavailable", response.IndexStatus)
	require.Nil(t, response.TaintCoverage)
}

func TestRecallTaintCompletenessSnapshotMismatchCannotBeRecertified(t *testing.T) {
	srv, badger, _, _, _ := setupAppV23RESTAccess(t)
	srv.embedder = authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	caller := fullVisibilityCaller(t, srv, badger, "fv.home")
	mock := mockStoreOf(srv)
	mock.taintCompletenessMirrorRevision = 1 // expected canonical revision is zero
	mock.outside = outsideSpaceProbe{hasOutside: false, established: true}
	mock.outside.after = func() { mock.taintCompletenessMirrorRevision = 0 }
	_, response := queryEmptyRecall(t, srv, caller, "fv.home", "committed")
	require.Equal(t, "unavailable", response.IndexStatus,
		"a later matching revision cannot replace the SQL statement's mismatch")
	require.Nil(t, response.TaintCoverage)
}

func TestRecallTaintCompletenessFinalAuthorizedCoverageOverride(t *testing.T) {
	srv, _, _, reader, deep := appV23TaintCoverageFixture(t, false, false)
	scope := newAppV23ReadScope()
	allowed, err := srv.appV23RecallCandidateFilterScoped(reader, time.Now(), scope)(deep)
	require.NoError(t, err)
	require.False(t, allowed)
	response := QueryMemoryResponse{Results: []*MemoryResult{}, IndexStatus: "complete"}
	scope.applyRecallCoverage(&response)
	require.Equal(t, "unavailable", response.IndexStatus)
	require.NotNil(t, response.TaintCoverage)
	require.Equal(t, "ancestor_walk_budget", response.TaintCoverage.Reason)
}
