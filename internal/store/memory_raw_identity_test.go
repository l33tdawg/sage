package store

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"
)

// These tests exercise store primitives, not transaction admission or transport.
func TestAppV30RawMemoryIdentityPreservesBytesAndHash(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	ids := []string{" ", "\t\n", "\u00a0", "\x00", "\xff", "a:b/c", strings.Repeat("x", 512)}
	for _, id := range ids {
		t.Run(fmt.Sprintf("id_%x", []byte(id)), func(t *testing.T) {
			hash := sha256.Sum256(append([]byte("raw-id-fixture:"), []byte(id)...))
			require.NoError(t, s.SetMemoryHash(id, hash[:], "proposed"))
			state, err := s.GetMemoryDisclosureStateAppV30(id)
			require.NoError(t, err)
			require.Equal(t, hash[:], state.ContentHash)
			require.Equal(t, "proposed", state.Status)
			require.NoError(t, s.SetMemoryStatusPreservingHashAppV30(id, "committed"))
			state, err = s.GetMemoryDisclosureStateAppV30(id)
			require.NoError(t, err)
			require.Equal(t, hash[:], state.ContentHash)
			require.Equal(t, "committed", state.Status)
			if strings.TrimSpace(id) == "" {
				_, err = s.GetMemoryDisclosureState(id)
				require.Error(t, err, "legacy read remains strict")
				require.Error(t, s.SetMemoryStatusPreservingHash(id, "deprecated"))
			} else {
				legacy, err := s.GetMemoryDisclosureState(id)
				require.NoError(t, err)
				require.Equal(t, state, legacy)
			}
		})
	}
	// Trimming would alias these keys and make these different hashes collide.
	space, err := s.GetMemoryDisclosureStateAppV30(" ")
	require.NoError(t, err)
	tab, err := s.GetMemoryDisclosureStateAppV30("\t\n")
	require.NoError(t, err)
	require.NotEqual(t, space.ContentHash, tab.ContentHash)
}

func TestAppV30RawMemoryIdentityRetainsIntegrityErrors(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	for _, id := range []string{"", strings.Repeat("x", 513)} {
		_, err := s.GetMemoryDisclosureStateAppV30(id)
		require.Error(t, err)
		require.Error(t, s.SetMemoryStatusPreservingHashAppV30(id, "committed"))
	}
	_, err = s.GetMemoryDisclosureStateAppV30("\t")
	require.ErrorIs(t, err, ErrMemoryDisclosureNotFound)
	require.ErrorIs(t, s.SetMemoryStatusPreservingHashAppV30("\t", "committed"), ErrMemoryNotFound)

	require.NoError(t, s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, memoryKey(" "), []byte{1})
	}))
	_, err = s.GetMemoryDisclosureStateAppV30(" ")
	require.ErrorIs(t, err, ErrMemoryDisclosureCorrupt)
	require.ErrorIs(t, s.SetMemoryStatusPreservingHashAppV30(" ", "committed"), ErrMemoryHashMalformed)
	require.NoError(t, s.SetMemoryHash("\t", []byte{1}, "proposed"))
	require.ErrorIs(t, s.SetMemoryStatusPreservingHashAppV30("\t", "committed"), ErrMemoryHashMalformed)
	originalHash, originalStatus, err := s.GetMemoryHash("\t")
	require.NoError(t, err)
	require.Equal(t, []byte{1}, originalHash)
	require.Equal(t, "proposed", originalStatus)
	hash := sha256.Sum256([]byte("valid"))
	require.NoError(t, s.SetMemoryHash("\n", hash[:], "proposed"))
	require.Error(t, s.SetMemoryStatusPreservingHashAppV30("\n", " "))
	state, err := s.GetMemoryDisclosureStateAppV30("\n")
	require.NoError(t, err)
	require.Equal(t, "proposed", state.Status)
	require.Equal(t, hash[:], state.ContentHash)
}

func TestAppV30RawMemoryIdentityDoesNotNarrowHistoricalReader(t *testing.T) {
	s, err := NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.CloseBadger()) })
	// The historical envelope reader had no byte-length/UTF-8 policy. Its
	// behavior is distinct from the strict legacy PUBLIC index selector.
	id := strings.Repeat("z", 513)
	hash := sha256.Sum256([]byte("legacy-reader-control"))
	require.NoError(t, s.SetMemoryHash(id, hash[:], "proposed"))
	legacy, err := s.GetMemoryDisclosureState(id)
	require.NoError(t, err)
	require.Equal(t, hash[:], legacy.ContentHash)
	_, err = s.GetMemoryDisclosureStateAppV30(id)
	require.Error(t, err)
}
