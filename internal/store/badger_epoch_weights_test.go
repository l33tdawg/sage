package store

import (
	"bytes"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// PoE epoch-weight persistence (v8.2 Task #3). These tests pin the on-chain
// behavior of SetEpochWeights / GetEpochWeights / GetEpochNumber. Cases:
//
//   W1: Round-trip a 4-validator weight set → identical map back, epoch matches.
//   W2: Fresh store → GetEpochWeights returns (nil, false, nil).
//   W3: Overwrite with a smaller set → stale validators are pruned.
//   W4: Encoding regression — 0.25 must land as the documented 8 big-endian
//       bytes so the on-chain format is locked.
//   W5: Atomicity — a malformed input (empty-string validator ID) must be
//       rejected BEFORE any poew:* key lands in BadgerDB.
//
// All cases use newTestBadger from badger_multiorg_test.go (t.TempDir() +
// deferred CloseBadger via t.Cleanup) to keep the store hermetic per test.

// W1: round-trip four weights — every value comes back identical, epoch
// matches what was written.
func TestSetEpochWeights_RoundTrip(t *testing.T) {
	bs := newTestBadger(t)

	const epoch uint64 = 42
	in := map[string]float64{
		"validator-a": 0.25,
		"validator-b": 0.125,
		"validator-c": 0.375,
		"validator-d": 0.25,
	}
	require.NoError(t, bs.SetEpochWeights(epoch, in))

	got, ok, err := bs.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok, "GetEpochWeights should report ok=true after a write")
	assert.Equal(t, in, got, "weight map round-trip must be byte-identical")

	gotEpoch, ok, err := bs.GetEpochNumber()
	require.NoError(t, err)
	require.True(t, ok, "GetEpochNumber should report ok=true after a write")
	assert.Equal(t, epoch, gotEpoch)
}

// W2: a fresh store with no SetEpochWeights call returns (nil, false, nil)
// from GetEpochWeights and (0, false, nil) from GetEpochNumber. The cold-boot
// check in refreshPoEWeights uses this distinction to decide whether to
// hydrate or fall back to the equal-weight bootstrap branch.
func TestGetEpochWeights_FreshStore(t *testing.T) {
	bs := newTestBadger(t)

	weights, ok, err := bs.GetEpochWeights()
	require.NoError(t, err)
	assert.False(t, ok, "fresh store must report ok=false")
	assert.Nil(t, weights, "fresh store must return a nil map")

	epoch, ok, err := bs.GetEpochNumber()
	require.NoError(t, err)
	assert.False(t, ok, "fresh store must report ok=false for the epoch marker")
	assert.Equal(t, uint64(0), epoch)
}

// W3: SetEpochWeights({A,B,C,D}) then SetEpochWeights({A,B}) — C and D must
// be pruned. A validator removed via governance cannot be allowed to leave a
// stale poew:<id> behind, because the boot loader would silently apply that
// dead weight to a validator that no longer exists in the set.
func TestSetEpochWeights_StaleValidatorPruning(t *testing.T) {
	bs := newTestBadger(t)

	first := map[string]float64{
		"validator-a": 0.25,
		"validator-b": 0.25,
		"validator-c": 0.25,
		"validator-d": 0.25,
	}
	require.NoError(t, bs.SetEpochWeights(5, first))

	second := map[string]float64{
		"validator-a": 0.5,
		"validator-b": 0.5,
	}
	require.NoError(t, bs.SetEpochWeights(6, second))

	// Functional check: GetEpochWeights must return exactly the new set.
	got, ok, err := bs.GetEpochWeights()
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, second, got, "stale poew:<id> entries for C and D must be pruned")

	// Belt-and-braces: confirm directly via db.View that the raw keys
	// poew:validator-c and poew:validator-d are no longer present.
	require.NoError(t, bs.db.View(func(txn *badger.Txn) error {
		for _, id := range []string{"validator-c", "validator-d"} {
			_, err := txn.Get(poeWeightKey(id))
			assert.ErrorIs(t, err, badger.ErrKeyNotFound,
				"poew:%s should be deleted after overwrite", id)
		}
		// Also assert epoch marker advanced to 6.
		gotEpoch, ok, err := bs.GetEpochNumber()
		require.NoError(t, err)
		require.True(t, ok)
		assert.Equal(t, uint64(6), gotEpoch)
		return nil
	}))
}

// W4: encoding regression — pin the on-chain bytes for a known input. The
// IEEE-754 double 0.25 in big-endian is 0x3FD0000000000000. If a future
// refactor accidentally swaps to little-endian or to a Gob/JSON encoding,
// this test catches it before the change makes it to consensus.
func TestSetEpochWeights_EncodingPin(t *testing.T) {
	bs := newTestBadger(t)

	require.NoError(t, bs.SetEpochWeights(1, map[string]float64{
		"validator-a": 0.25,
	}))

	want := []byte{0x3F, 0xD0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}
	var got []byte
	require.NoError(t, bs.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(poeWeightKey("validator-a"))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			got = append([]byte(nil), val...)
			return nil
		})
	}))
	assert.True(t, bytes.Equal(got, want),
		"on-chain encoding of 0.25 must be big-endian IEEE-754 (got=%x want=%x)", got, want)
}

// W5: atomicity — a malformed input must be rejected before ANY poew:* key
// is written. Failure mode chosen: an empty-string validator ID. The
// implementation validates inputs up front (outside the db.Update closure),
// so the error returns before the txn even opens. Verify against a fresh
// store that no poew:* keys exist after the failed call. If a future
// refactor moves validation inside the closure but keeps txn.Discard
// semantics, this test still passes — that is the point: ALL keys land or
// NONE do.
func TestSetEpochWeights_AtomicityOnInvalidInput(t *testing.T) {
	bs := newTestBadger(t)

	bad := map[string]float64{
		"":            0.5, // invalid — empty id is rejected before the txn opens
		"validator-a": 0.5,
	}
	err := bs.SetEpochWeights(7, bad)
	require.Error(t, err, "empty validator id must fail SetEpochWeights")

	// No poew:* key — including the poew:current marker — should be visible.
	weights, ok, gErr := bs.GetEpochWeights()
	require.NoError(t, gErr)
	assert.False(t, ok, "GetEpochWeights must report ok=false after a failed write")
	assert.Nil(t, weights)

	epoch, ok, gErr := bs.GetEpochNumber()
	require.NoError(t, gErr)
	assert.False(t, ok, "GetEpochNumber must report ok=false after a failed write")
	assert.Equal(t, uint64(0), epoch)

	// Also assert via raw iteration that there is no poew:* key at all.
	require.NoError(t, bs.db.View(func(txn *badger.Txn) error {
		prefix := []byte("poew:")
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		var leftover []string
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			leftover = append(leftover, string(it.Item().KeyCopy(nil)))
		}
		assert.Empty(t, leftover, "no poew:* key may exist after a rejected SetEpochWeights")
		return nil
	}))
}
