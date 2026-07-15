package store

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/scope"
)

func testScopeRecord(revision uint64, domains ...string) scope.Record {
	selected := make([]scope.Domain, 0, len(domains))
	for _, domain := range domains {
		selected = append(selected, scope.Domain{Name: domain})
	}
	return scope.Record{
		ScopeID:               "scope-a",
		Revision:              revision,
		State:                 scope.StateActive,
		ControllerValidatorID: "validator-a",
		CreatedHeight:         10,
		UpdatedHeight:         int64(10 + revision),
		Domains:               selected,
		Members: []scope.Member{
			{ValidatorID: "validator-a", AssignedWeight: 4, JoinedRevision: 1, Active: true},
			{ValidatorID: "validator-b", AssignedWeight: 3, JoinedRevision: 1, Active: true},
		},
	}
}

func TestScopeRecordStoreRoundTripReplayAndDomainReplacement(t *testing.T) {
	bs := newTestBadger(t)
	initial := testScopeRecord(1, "audit", "research")
	require.NoError(t, bs.SetScopeRecord(initial))
	require.NoError(t, bs.SetScopeRecord(initial), "exact crash replay is idempotent")

	got, err := bs.GetScopeForDomain("audit")
	require.NoError(t, err)
	assert.Equal(t, initial, *got)
	revision1, err := bs.GetScopeRevisionHash(initial.ScopeID, 1)
	require.NoError(t, err)
	require.Len(t, revision1, 32)

	updated := testScopeRecord(2, "audit", "security")
	require.NoError(t, bs.SetScopeRecord(updated))
	old, err := bs.GetScopeForDomain("research")
	require.NoError(t, err)
	assert.Nil(t, old, "removed domain mapping must not survive a roster update")
	newScope, err := bs.GetScopeForDomain("security")
	require.NoError(t, err)
	assert.Equal(t, updated, *newScope)
	revision1After, err := bs.GetScopeRevisionHash(initial.ScopeID, 1)
	require.NoError(t, err)
	revision2, err := bs.GetScopeRevisionHash(initial.ScopeID, 2)
	require.NoError(t, err)
	assert.Equal(t, revision1, revision1After, "advancing the head cannot rewrite an older audit anchor")
	require.Len(t, revision2, 32)
	assert.NotEqual(t, revision1, revision2)
}

func TestScopeRecordStoreRejectsRevisionRewriteAndDomainCollisionAtomically(t *testing.T) {
	bs := newTestBadger(t)
	initial := testScopeRecord(1, "audit")
	require.NoError(t, bs.SetScopeRecord(initial))

	sameRevisionDifferent := initial
	sameRevisionDifferent.UpdatedHeight++
	err := bs.SetScopeRecord(sameRevisionDifferent)
	require.ErrorIs(t, err, ErrScopeRevision)

	lower := testScopeRecord(0, "audit")
	err = bs.SetScopeRecord(lower)
	require.Error(t, err)

	other := testScopeRecord(1, "audit")
	other.ScopeID = "scope-b"
	err = bs.SetScopeRecord(other)
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrScopeRevision), "collision is not confused with a revision replay")
	missing, err := bs.GetScopeRecord("scope-b")
	require.NoError(t, err)
	assert.Nil(t, missing, "failed collision write must not leave a partial scope record")
}

func TestScopeRecordStoreRetirementRemovesDomainMapping(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, bs.SetScopeRecord(testScopeRecord(1, "audit")))
	retired := testScopeRecord(2, "audit")
	retired.State = scope.StateRetired
	require.NoError(t, bs.SetScopeRecord(retired))

	mapped, err := bs.GetScopeForDomain("audit")
	require.NoError(t, err)
	assert.Nil(t, mapped)
	record, err := bs.GetScopeRecord("scope-a")
	require.NoError(t, err)
	assert.Equal(t, scope.StateRetired, record.State)

	reactivate := testScopeRecord(3, "audit")
	err = bs.SetScopeRecord(reactivate)
	assert.ErrorIs(t, err, ErrScopeRetired)
}

func TestListScopeRecordsIsCanonicalAndSorted(t *testing.T) {
	bs := newTestBadger(t)
	second := testScopeRecord(1, "security")
	second.ScopeID = "scope-z"
	first := testScopeRecord(1, "audit")
	first.ScopeID = "scope-a"
	require.NoError(t, bs.SetScopeRecord(second))
	require.NoError(t, bs.SetScopeRecord(first))

	records, err := bs.ListScopeRecords()
	require.NoError(t, err)
	require.Len(t, records, 2)
	assert.Equal(t, []string{"scope-a", "scope-z"}, []string{records[0].ScopeID, records[1].ScopeID})
}
