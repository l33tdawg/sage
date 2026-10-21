package store

import (
	"encoding/binary"
	"math"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/poe"
)

// V1 — 56-byte round-trip: write a full v8.3 record, read all seven fields back.
func TestValidatorStats_V83RoundTrip(t *testing.T) {
	in := &ValidatorStats{
		TotalVotes:      7,
		AcceptVotes:     5,
		LastBlockHeight: 1234,
		EWMAWeightedSum: 3.5,
		EWMAWeightDenom: 4.0,
		EWMACount:       4,
		CorrCount:       3,
	}
	encoded := encodeValidatorStats(in, true)
	require.Len(t, encoded, validatorStatsLenV83)

	out, err := decodeValidatorStats(encoded)
	require.NoError(t, err)
	assert.Equal(t, in, out)
}

// V2 — legacy decode: a 24-byte record decodes with the four v8.3 fields zero.
func TestValidatorStats_LegacyDecode(t *testing.T) {
	legacy := make([]byte, validatorStatsLenLegacy)
	binary.BigEndian.PutUint64(legacy[0:8], 9)
	binary.BigEndian.PutUint64(legacy[8:16], 6)
	binary.BigEndian.PutUint64(legacy[16:24], 4242)

	out, err := decodeValidatorStats(legacy)
	require.NoError(t, err)
	assert.Equal(t, uint64(9), out.TotalVotes)
	assert.Equal(t, uint64(6), out.AcceptVotes)
	assert.Equal(t, uint64(4242), out.LastBlockHeight)
	assert.Zero(t, out.EWMAWeightedSum)
	assert.Zero(t, out.EWMAWeightDenom)
	assert.Zero(t, out.EWMACount)
	assert.Zero(t, out.CorrCount)

	// And a legacy record reads as Phase-1 values through the PoE math.
	tracker := &poe.EWMATracker{WeightedSum: out.EWMAWeightedSum, WeightDenom: out.EWMAWeightDenom, Count: int64(out.EWMACount)}
	assert.Equal(t, 0.5, tracker.Accuracy(), "legacy record → EWMA cold-start prior")
	assert.Equal(t, 0.0, poe.CorroborationScore(int(out.CorrCount), poe.CorrMax), "legacy record → corroboration 0")

	// A bad length is still rejected.
	_, err = decodeValidatorStats(make([]byte, 40))
	require.Error(t, err)
}

// V3 — golden bytes: lock the on-chain 56-byte layout, incl. IEEE-754 fields.
func TestValidatorStats_GoldenBytes(t *testing.T) {
	s := &ValidatorStats{
		TotalVotes:      3,
		AcceptVotes:     2,
		LastBlockHeight: 150,
		EWMAWeightedSum: 0.25, // 0x3FD0000000000000
		EWMAWeightDenom: 1.0,  // 0x3FF0000000000000
		EWMACount:       1,
		CorrCount:       1,
	}
	got := encodeValidatorStats(s, true)

	want := []byte{
		0, 0, 0, 0, 0, 0, 0, 3, // TotalVotes
		0, 0, 0, 0, 0, 0, 0, 2, // AcceptVotes
		0, 0, 0, 0, 0, 0, 0, 150, // LastBlockHeight
		0x3F, 0xD0, 0, 0, 0, 0, 0, 0, // EWMAWeightedSum = 0.25
		0x3F, 0xF0, 0, 0, 0, 0, 0, 0, // EWMAWeightDenom = 1.0
		0, 0, 0, 0, 0, 0, 0, 1, // EWMACount
		0, 0, 0, 0, 0, 0, 0, 1, // CorrCount
	}
	assert.Equal(t, want, got)

	// Legacy encode of the same record is exactly the first 24 bytes.
	assert.Equal(t, want[:validatorStatsLenLegacy], encodeValidatorStats(s, false))
}

// V4 — UpdateVerdictStats math: two matches + one miss reproduce three EWMA
// updates and CorrCount == 2.
func TestUpdateVerdictStats_Math(t *testing.T) {
	s := newTestBadger(t)
	const vid = "v-math"

	// Seed an existing 56-byte record so we also exercise read-modify-write.
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(validatorStatsKey(vid), encodeValidatorStats(&ValidatorStats{TotalVotes: 3, AcceptVotes: 2, LastBlockHeight: 10}, true))
	}))

	// Three sequential single-validator updates: true, true, false.
	require.NoError(t, s.UpdateVerdictStats(map[string]bool{vid: true}))
	require.NoError(t, s.UpdateVerdictStats(map[string]bool{vid: true}))
	require.NoError(t, s.UpdateVerdictStats(map[string]bool{vid: false}))

	got, err := s.GetValidatorStats(vid)
	require.NoError(t, err)

	// Reference: drive a fresh EWMATracker with the same outcomes.
	ref := poe.NewEWMATracker()
	ref.Update(1.0)
	ref.Update(1.0)
	ref.Update(0.0)
	assert.InDelta(t, ref.WeightedSum, got.EWMAWeightedSum, 1e-12)
	assert.InDelta(t, ref.WeightDenom, got.EWMAWeightDenom, 1e-12)
	assert.Equal(t, uint64(3), got.EWMACount)
	assert.Equal(t, uint64(2), got.CorrCount)
	// Pre-existing counters untouched.
	assert.Equal(t, uint64(3), got.TotalVotes)
	assert.Equal(t, uint64(2), got.AcceptVotes)
}

func TestUpdateVerdictStatsDeterministic_GoldenBits(t *testing.T) {
	s := newTestBadger(t)
	const vid = "v-deterministic"
	const domain = "cryptography"

	require.NoError(t, s.UpdateVerdictStatsDeterministic(map[string]bool{vid: true}))
	require.NoError(t, s.UpdateVerdictStatsDeterministic(map[string]bool{vid: false}))
	require.NoError(t, s.UpdateVerdictStatsDeterministic(map[string]bool{vid: true}))
	require.NoError(t, s.UpdateVerdictStatsDeterministic(map[string]bool{vid: true}))
	require.NoError(t, s.UpdateDomainVerdictStatsDeterministic(domain, map[string]bool{vid: true}))
	require.NoError(t, s.UpdateDomainVerdictStatsDeterministic(domain, map[string]bool{vid: false}))
	require.NoError(t, s.UpdateDomainVerdictStatsDeterministic(domain, map[string]bool{vid: true}))
	require.NoError(t, s.UpdateDomainVerdictStatsDeterministic(domain, map[string]bool{vid: true}))

	global, err := s.GetValidatorStats(vid)
	require.NoError(t, err)
	scoped, err := s.GetValidatorDomainStats(vid, domain)
	require.NoError(t, err)

	for name, stats := range map[string]*ValidatorStats{
		"global": global,
		"domain": scoped,
	} {
		assert.Equal(t, uint64(0x4005083126e978d5), math.Float64bits(stats.EWMAWeightedSum), name+" weighted sum")
		assert.Equal(t, uint64(0x400b83126e978d50), math.Float64bits(stats.EWMAWeightDenom), name+" weight denominator")
		assert.Equal(t, uint64(4), stats.EWMACount, name+" count")
		assert.Equal(t, uint64(3), stats.CorrCount, name+" corroboration count")
		tracker := &poe.EWMATracker{
			WeightedSum: stats.EWMAWeightedSum,
			WeightDenom: stats.EWMAWeightDenom,
			Count:       int64(stats.EWMACount),
		}
		assert.Equal(t, uint64(0x3fe3629a82177e4c), math.Float64bits(tracker.AccuracyDeterministic()), name+" accuracy")
	}
}

// V5 — atomicity: a batch error leaves no record changed. We trigger a decode
// error mid-batch by planting a corrupt record for one validator and confirm
// the other validator's record is unchanged (the whole db.Update rolls back).
func TestUpdateVerdictStats_Atomic(t *testing.T) {
	s := newTestBadger(t)

	// "v-a" has a clean record; "v-b" has a corrupt (40-byte) record that will
	// fail decodeValidatorStats inside the batch.
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		if err := txn.Set(validatorStatsKey("v-a"), encodeValidatorStats(&ValidatorStats{TotalVotes: 1}, true)); err != nil {
			return err
		}
		return txn.Set(validatorStatsKey("v-b"), make([]byte, 40))
	}))

	before, err := s.GetValidatorStats("v-a")
	require.NoError(t, err)

	// Sorted iteration hits v-a (ok) then v-b (decode error) → batch aborts.
	err = s.UpdateVerdictStats(map[string]bool{"v-a": true, "v-b": true})
	require.Error(t, err)

	after, err := s.GetValidatorStats("v-a")
	require.NoError(t, err)
	assert.Equal(t, before, after, "v-a must be unchanged when the batch rolls back")
}

// V6 — lazy migration: a 24-byte legacy record grows to 56 bytes on the first
// post-fork IncrementVoteStats, counters incremented, EWMA/corr still zero.
func TestIncrementVoteStats_LazyMigration(t *testing.T) {
	s := newTestBadger(t)
	const vid = "v-mig"

	// Plant a legacy 24-byte record.
	require.NoError(t, s.db.Update(func(txn *badger.Txn) error {
		return txn.Set(validatorStatsKey(vid), encodeValidatorStats(&ValidatorStats{TotalVotes: 5, AcceptVotes: 4, LastBlockHeight: 80}, false))
	}))

	// Post-fork vote → 56-byte record.
	require.NoError(t, s.IncrementVoteStats(vid, true, 81, true))

	var rawLen int
	require.NoError(t, s.db.View(func(txn *badger.Txn) error {
		item, gErr := txn.Get(validatorStatsKey(vid))
		if gErr != nil {
			return gErr
		}
		return item.Value(func(v []byte) error { rawLen = len(v); return nil })
	}))
	assert.Equal(t, validatorStatsLenV83, rawLen, "record migrated 24 → 56 bytes")

	got, err := s.GetValidatorStats(vid)
	require.NoError(t, err)
	assert.Equal(t, uint64(6), got.TotalVotes)
	assert.Equal(t, uint64(5), got.AcceptVotes)
	assert.Equal(t, uint64(81), got.LastBlockHeight)
	assert.Zero(t, got.EWMACount, "vote increment must not touch EWMA")
	assert.Zero(t, got.CorrCount)

	// Pre-fork increment keeps it 24-byte (verify the other direction).
	const vid2 = "v-legacy"
	require.NoError(t, s.IncrementVoteStats(vid2, true, 1, false))
	require.NoError(t, s.db.View(func(txn *badger.Txn) error {
		item, gErr := txn.Get(validatorStatsKey(vid2))
		if gErr != nil {
			return gErr
		}
		return item.Value(func(v []byte) error { rawLen = len(v); return nil })
	}))
	assert.Equal(t, validatorStatsLenLegacy, rawLen, "pre-fork write stays 24 bytes")
}
