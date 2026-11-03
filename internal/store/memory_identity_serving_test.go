package store

import (
	"crypto/sha256"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/taint"
)

func identityProjectionFixture(t *testing.T, s *BadgerStore, id string) *memory.MemoryRecord {
	t.Helper()
	content := "raw canonical content"
	hash := sha256.Sum256([]byte(content))
	author := strings.Repeat("aa", 32)
	require.NoError(t, s.SetMemoryHash(id, hash[:], "committed"))
	require.NoError(t, s.SetMemoryDomain(id, "raw.domain"))
	require.NoError(t, s.SetMemoryAuthor(id, author))
	require.NoError(t, s.SetMemoryAuthorPrincipal(id, author))
	require.NoError(t, s.SetMemoryClassification(id, uint8(ClearanceInternal)))
	return &memory.MemoryRecord{MemoryID: id, Content: content, ContentHash: hash[:], Status: memory.StatusCommitted, DomainTag: "raw.domain", SubmittingAgent: author}
}

func TestMemoryIdentityServingRequiresQualifiedRawSnapshot(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 10)
	record := identityProjectionFixture(t, s, " ")
	calls := 0
	policy := func(*BadgerStore, *MemoryDisclosureState) (bool, error) { calls++; return true, nil }
	_, _, _, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), policy)
	require.Error(t, err)
	require.Zero(t, calls)
	identityTestHeight(t, s, 11)
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), policy)
	require.NoError(t, err)
	require.True(t, allowed)
	require.NotNil(t, state)
	require.Equal(t, MemoryProjectionExact, disposition)
	require.Equal(t, 1, calls)
	records := []*memory.MemoryRecord{record, identityProjectionFixture(t, s, "\x00"), identityProjectionFixture(t, s, string([]byte{0xff}))}
	batch, err := s.ClassifyMemoryProjections(records)
	require.NoError(t, err)
	require.Len(t, batch, len(records))
	for _, result := range batch {
		require.NoError(t, result.Err)
		require.Equal(t, MemoryProjectionExact, result.Disposition)
	}
}

func TestMemoryIdentityServingDenialPrecedesTaintDecode(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 11)
	record := identityProjectionFixture(t, s, " ")
	id := strings.Repeat("a", taint.DecisionIDBytes)
	key := append(append([]byte(nil), taint.DecisionPrefix()...), id...)
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error { return txn.Set(key, []byte{0}) }))
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), func(reader *BadgerStore, state *MemoryDisclosureState) (bool, error) {
		require.NotNil(t, reader.txn)
		require.True(t, reader.taintServingReadOnly)
		require.Equal(t, "raw.domain", state.Domain)
		return false, nil
	})
	require.NoError(t, err)
	require.False(t, allowed)
	require.Nil(t, state)
	require.Equal(t, MemoryProjectionExact, disposition)
	// Positive control: the same malformed canonical taint state is fatal once
	// an authorized reader actually reaches classification.
	_, _, _, err = s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), allowMemoryProjectionRead)
	require.Error(t, err)
}

func TestMemoryIdentityServingAuthorizationSharesEnvelopeSnapshot(t *testing.T) {
	s := identityTestStore(t)
	require.NoError(t, s.MarkUpgradeApplied("app-v30", 30, 10))
	identityTestHeight(t, s, 11)
	record := identityProjectionFixture(t, s, string([]byte{0xff}))
	state, disposition, allowed, err := s.ClassifyMemoryProjectionForRead(record, NewTaintServingReadScope(), func(reader *BadgerStore, state *MemoryDisclosureState) (bool, error) {
		require.NoError(t, s.SetMemoryDomain(record.MemoryID, "changed.domain"))
		held, err := reader.GetMemoryDisclosureStateAppV30(record.MemoryID)
		require.NoError(t, err)
		require.Equal(t, state.Domain, held.Domain)
		require.Equal(t, "raw.domain", held.Domain)
		return true, nil
	})
	require.NoError(t, err)
	require.True(t, allowed)
	require.NotNil(t, state)
	require.Equal(t, MemoryProjectionExact, disposition)
	_, _, err = s.ClassifyMemoryProjection(record)
	require.Error(t, err)
}
