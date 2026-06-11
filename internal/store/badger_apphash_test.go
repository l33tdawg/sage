package store

import (
	"bytes"
	"crypto/sha256"
	"fmt"
	"sort"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// referenceAppHash reproduces the pre-#26 algorithm: collect every key/value
// into a slice, sort by key, then hash key||value in sorted order. The
// streaming ComputeAppHash must be byte-identical to this — that equality is
// what guarantees issue #26's perf fix did NOT change the app hash (i.e. it is
// a consensus no-op, not a fork). If this ever diverges, the fix changed the
// committed app hash and would fork existing chains.
func referenceAppHash(t *testing.T, s *BadgerStore) []byte {
	t.Helper()
	type kv struct{ k, v []byte }
	var entries []kv
	require.NoError(t, s.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchValues = true
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := append([]byte(nil), item.Key()...)
			var v []byte
			require.NoError(t, item.Value(func(val []byte) error {
				v = append([]byte(nil), val...)
				return nil
			}))
			entries = append(entries, kv{k, v})
		}
		return nil
	}))
	sort.Slice(entries, func(i, j int) bool { return string(entries[i].k) < string(entries[j].k) })
	h := sha256.New()
	for _, e := range entries {
		h.Write(e.k)
		h.Write(e.v)
	}
	return h.Sum(nil)
}

// TestComputeAppHash_ByteIdenticalToSortedConcat is the consensus-safety pin
// for issue #26. Keys are written in deliberately non-lexicographic order; the
// streaming ComputeAppHash must equal the sort-then-hash reference regardless,
// because BadgerDB's forward iterator already yields keys in sorted byte order.
func TestComputeAppHash_ByteIdenticalToSortedConcat(t *testing.T) {
	bs := newTestBadger(t)

	// Insert in scrambled order (not sorted, not insertion-stable).
	pairs := map[string]string{
		"zeta":      "26",
		"alpha":     "1",
		"mike":      "13",
		"":          "empty-key-value",
		"alpha.sub": "child",
		"\x00\x01":  "binary-key",
		"yankee":    "",
	}
	order := []string{"mike", "zeta", "alpha.sub", "", "alpha", "yankee", "\x00\x01"}
	for _, k := range order {
		require.NoError(t, bs.SetState(k, []byte(pairs[k])))
	}

	got, err := bs.ComputeAppHash()
	require.NoError(t, err)
	want := referenceAppHash(t, bs)
	assert.True(t, bytes.Equal(got, want),
		"streaming ComputeAppHash must be byte-identical to the sort-then-hash reference — "+
			"a mismatch means the #26 fix changed the app hash (consensus fork)")
}

// TestComputeAppHash_OrderIndependent asserts the hash depends only on the set
// of key/value pairs, not the order they were written — two stores populated
// with the same data in different orders must agree.
func TestComputeAppHash_OrderIndependent(t *testing.T) {
	data := map[string]string{
		"org:a": "alpha", "org:b": "bravo", "dom:x": "xray",
		"dom:y": "yankee", "agent:1": "one", "agent:2": "two",
	}

	bs1 := newTestBadger(t)
	for _, k := range []string{"org:a", "org:b", "dom:x", "dom:y", "agent:1", "agent:2"} {
		require.NoError(t, bs1.SetState(k, []byte(data[k])))
	}
	bs2 := newTestBadger(t)
	for _, k := range []string{"agent:2", "dom:y", "org:b", "agent:1", "dom:x", "org:a"} {
		require.NoError(t, bs2.SetState(k, []byte(data[k])))
	}

	h1, err := bs1.ComputeAppHash()
	require.NoError(t, err)
	h2, err := bs2.ComputeAppHash()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(h1, h2), "app hash must be independent of write order")
}

// TestComputeAppHash_EmptyDeterministic pins the empty-store hash as stable.
func TestComputeAppHash_EmptyDeterministic(t *testing.T) {
	a, err := newTestBadger(t).ComputeAppHash()
	require.NoError(t, err)
	b, err := newTestBadger(t).ComputeAppHash()
	require.NoError(t, err)
	assert.True(t, bytes.Equal(a, b), "empty-store app hash must be deterministic")
}

// BenchmarkComputeAppHash tracks the per-call cost as state grows (issue #26).
// Run with -benchmem; allocations should stay roughly flat per key, not carry
// the old O(state) slice-copy tax. Vary N via sub-benchmarks.
func BenchmarkComputeAppHash(b *testing.B) {
	for _, n := range []int{1000, 10000, 50000} {
		b.Run(fmt.Sprintf("N=%d", n), func(b *testing.B) {
			bs, err := NewBadgerStore(b.TempDir())
			require.NoError(b, err)
			b.Cleanup(func() { _ = bs.CloseBadger() })
			for i := 0; i < n; i++ {
				require.NoError(b, bs.SetState(fmt.Sprintf("key:%08d", i), []byte(fmt.Sprintf("value-%d", i))))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := bs.ComputeAppHash(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// TestComputeAppHashExcludingState_EqualsFullHashWithoutStateKeys is the
// semantic pin for the app-v12 hash rule (issue #40): hashing a keyspace that
// CONTAINS state: keys with ComputeAppHashExcludingState must be
// byte-identical to hashing the SAME keyspace minus those keys with the
// legacy ComputeAppHash. That equivalence is the whole rule — the new hash is
// the old hash over the real consensus state, with only the volatile
// SaveState bookkeeping removed.
func TestComputeAppHashExcludingState_EqualsFullHashWithoutStateKeys(t *testing.T) {
	withState := newTestBadger(t)
	withoutState := newTestBadger(t)

	// Identical real consensus keys in both stores.
	for _, bs := range []*BadgerStore{withState, withoutState} {
		require.NoError(t, bs.SetNonce("agent-a", 7))
		require.NoError(t, bs.SetNonce("agent-b", 42))
		require.NoError(t, bs.SaveValidators(map[string]int64{"val-1": 10}))
	}

	// The volatile bookkeeping keys exist only in the first store.
	require.NoError(t, withState.SetState("height", []byte{0, 0, 0, 0, 0, 0, 0, 9}))
	require.NoError(t, withState.SetState("app_hash", []byte("prevhash")))
	require.NoError(t, withState.SetState("epoch", []byte{0, 0, 0, 0, 0, 0, 0, 1}))

	got, err := withState.ComputeAppHashExcludingState()
	require.NoError(t, err)
	want, err := withoutState.ComputeAppHash()
	require.NoError(t, err)
	assert.Equal(t, want, got,
		"excluding-state hash over {real keys + state:*} must equal the legacy hash over {real keys}")
}

// TestComputeAppHashExcludingState_StateWritesAreInert asserts the fixed-point
// property the app-v12 fork relies on: rewriting state: keys (what Commit's
// SaveState does every block) does NOT move the excluding-state hash, while a
// real consensus write does — and the legacy ComputeAppHash keeps moving on
// state: writes (the pre-fork replay behavior).
func TestComputeAppHashExcludingState_StateWritesAreInert(t *testing.T) {
	bs := newTestBadger(t)
	require.NoError(t, bs.SetNonce("agent-a", 7))

	h1, err := bs.ComputeAppHashExcludingState()
	require.NoError(t, err)
	legacy1, err := bs.ComputeAppHash()
	require.NoError(t, err)

	// Simulate SaveState after an empty block.
	require.NoError(t, bs.SetState("height", []byte{0, 0, 0, 0, 0, 0, 0, 12}))
	require.NoError(t, bs.SetState("app_hash", h1))

	h2, err := bs.ComputeAppHashExcludingState()
	require.NoError(t, err)
	legacy2, err := bs.ComputeAppHash()
	require.NoError(t, err)
	assert.Equal(t, h1, h2, "state: rewrites must be inert under the app-v12 rule")
	assert.NotEqual(t, legacy1, legacy2, "state: rewrites must keep moving the legacy hash (pre-fork replay)")

	// A real consensus write still moves the new hash.
	require.NoError(t, bs.SetNonce("agent-b", 1))
	h3, err := bs.ComputeAppHashExcludingState()
	require.NoError(t, err)
	assert.NotEqual(t, h2, h3, "non-state writes must still enter the app-v12 hash")
}
