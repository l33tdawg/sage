package main

import (
	"fmt"
	"io"
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func openTest(t *testing.T) *badger.DB {
	t.Helper()
	db, err := openBadger(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func seed(t *testing.T, db *badger.DB, build func(seeder)) {
	t.Helper()
	batch := db.NewWriteBatch()
	defer batch.Cancel()
	build(seeder{batch: batch})
	require.NoError(t, batch.Flush())
}

var testProfile = idProfile{Name: "uuid", Len: uuidIDLen}

func blockOpts(active int64) blockOptions {
	return blockOptions{ceiling: defaultBlockCeiling, tombstone: 20, activeAtHeight: active, contentBytes: 64}
}

func TestHeaderRoundTrip(t *testing.T) {
	header := decisionHeader{Status: statusExpanding, Generation: 2, Head: 3, Tail: 4, Scanned: 5, Materialized: 6, MaxDepth: 7, Parent: "parent", ParentDepth: 8, LastEdge: "edge"}
	decoded, err := decodeHeader(header.encode())
	require.NoError(t, err)
	assert.Equal(t, header, decoded)
}

func TestUnitCeilingIsExactAndFailsClosed(t *testing.T) {
	db := openTest(t)
	var fixture readFixture
	seed(t, db, func(s seeder) {
		var err error
		fixture, err = seedRead(s, testProfile, 2, 32, 1, 4)
		require.NoError(t, err)
	})
	for _, c := range []readCase{
		{"deep", len(fixture.deep), walkCandidates(fixture.deep)},
		{"flat", len(fixture.flat), walkCandidates(fixture.flat)},
	} {
		demand, failed, _, err := execRead(db, c, 0)
		require.NoError(t, err)
		require.False(t, failed)
		_, failed, _, err = execRead(db, c, demand)
		require.NoError(t, err)
		assert.False(t, failed, "ceiling equal to demand must succeed")
		units, failed, _, err := execRead(db, c, demand-1)
		require.NoError(t, err)
		assert.True(t, failed, "ceiling below demand must fail closed")
		assert.LessOrEqual(t, units, demand-1)
	}
}

func TestDeepWalkChargesProvenanceAtEveryNode(t *testing.T) {
	db := openTest(t)
	var fixture readFixture
	seed(t, db, func(s seeder) {
		var err error
		fixture, err = seedRead(s, testProfile, 1, 1, 1, 1)
		require.NoError(t, err)
	})
	demand, _, _, err := execRead(db, readCase{"deep", 1, walkCandidates(fixture.deep)}, 0)
	require.NoError(t, err)
	// Every visited node costs at least its marker, 64 signers and 31 cleared
	// memberships, and the walk stops at 256 nodes.
	assert.GreaterOrEqual(t, demand, int64(maxServingWalk*(1+64+1+31)))
	assert.LessOrEqual(t, demand, int64(maxServingWalk*(1+64+1+31+1+16)+1+32))
}

func TestExpandStopsAtBudgetAndResumesWithoutMissingChildren(t *testing.T) {
	db := openTest(t)
	var decision string
	seed(t, db, func(s seeder) {
		var err error
		decision, err = seedExpander(s, "resume", testProfile, 0)
		require.NoError(t, err)
	})
	var expandedPerTx []int64
	for range 40 {
		outcome, err := runBlock(db, []txSpec{{Kind: txExpand, Decision: decision}}, blockOpts(1))
		require.NoError(t, err)
		require.True(t, outcome.ActualWithinBudget)
		require.LessOrEqual(t, outcome.MaxUnitsByKind[txExpand], int64(txUnitBudget))
		expandedPerTx = append(expandedPerTx, outcome.NewMemberships)
		var status byte
		require.NoError(t, db.View(func(txn *badger.Txn) error {
			header, err := newStage(txn, 20).loadHeader(decision)
			status = header.Status
			return err
		}))
		if status == statusComplete {
			break
		}
	}
	assert.Equal(t, int64(203), expandedPerTx[0], "units bind before the 256 membership ceiling")
	assert.Equal(t, int64(204), expandedPerTx[1], "a resumed parent pays no queue pop")
	total := int64(0)
	for _, count := range expandedPerTx {
		total += count
	}
	assert.Equal(t, int64(fanout), total)
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		for n := range fanout {
			child := makeID(testProfile.Len, "resume/expander-child/0", n)
			if _, err := txn.Get(memberKey(child, decision)); err != nil {
				return fmt.Errorf("child %d: %w", n, err)
			}
			if _, err := txn.Get(indexKey(decision, child)); err != nil {
				return fmt.Errorf("child %d index: %w", n, err)
			}
		}
		return nil
	}))
}

func TestFullAdmissionStaysInsideTheUnitBound(t *testing.T) {
	db := openTest(t)
	var fixture blockFixture
	seed(t, db, func(s seeder) {
		var err error
		fixture, err = seedFullAdmission(s, "admission", testProfile)
		require.NoError(t, err)
	})
	outcome, err := runBlock(db, fixture.txs, blockOpts(fixture.active))
	require.NoError(t, err)
	assert.True(t, outcome.ActualWithinBudget)
	assert.Equal(t, int64(submitUnitBound), outcome.MaxUnitsByKind[txSubmit], "the worst submit reaches the RFC bound exactly")
	assert.Equal(t, "accept", outcome.ProcessProposal)
}

func TestActualNeverExceedsEstimate(t *testing.T) {
	builders := map[string]func(seeder, string, idProfile) (blockFixture, error){
		"full":          seedFullAdmission,
		"mixed_expand":  func(s seeder, ns string, p idProfile) (blockFixture, error) { return seedMixed(s, ns, p, false) },
		"mixed_cleanup": func(s seeder, ns string, p idProfile) (blockFixture, error) { return seedMixed(s, ns, p, true) },
		"cleanup":       seedCleanupBlock,
	}
	for _, p := range idProfiles {
		for name, build := range builders {
			t.Run(p.Name+"/"+name, func(t *testing.T) {
				db := openTest(t)
				var fixture blockFixture
				seed(t, db, func(s seeder) {
					var err error
					fixture, err = build(s, name, p)
					require.NoError(t, err)
				})
				outcome, err := runBlock(db, fixture.txs, blockOpts(fixture.active))
				require.NoError(t, err)
				assert.True(t, outcome.ActualWithinBudget)
				assert.Zero(t, outcome.OverflowedTxs)
				assert.LessOrEqual(t, outcome.GeneratedTaint, outcome.Estimate)
			})
		}
	}
}

// A parent gains a membership from an apply in the same block, so committed
// parent memberships alone would underestimate the submit behind it.
func TestSubmitInheritsMembershipAppliedInTheSameBlock(t *testing.T) {
	db := openTest(t)
	var target string
	seed(t, db, func(s seeder) {
		target = makeID(testProfile.Len, "same-block/target", 0)
		require.NoError(t, seedChildren(s, "same-block/child", testProfile, 0, target))
	})
	decision := makeID(decisionIDLen, "same-block/decision", 0)
	child := makeID(testProfile.Len, "same-block/submit", 0)
	outcome, err := runBlock(db, []txSpec{
		{Kind: txApply, Decision: decision, Target: target},
		{Kind: txSubmit, Memory: child, Parents: []string{target}},
	}, blockOpts(0))
	require.NoError(t, err)
	assert.True(t, outcome.ActualWithinBudget)
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		_, err := txn.Get(memberKey(child, decision))
		return err
	}))
}

func TestEstimatorBugFailsOnlyThatTxAndKeepsTheCursor(t *testing.T) {
	db := openTest(t)
	var first, second string
	seed(t, db, func(s seeder) {
		var err error
		first, err = seedExpander(s, "overflow", testProfile, 0)
		require.NoError(t, err)
		second, err = seedExpander(s, "overflow", testProfile, 1)
		require.NoError(t, err)
	})
	options := blockOpts(2)
	options.estimateCap = map[int]int64{0: 1}
	outcome, err := runBlock(db, []txSpec{{Kind: txExpand, Decision: first}, {Kind: txExpand, Decision: second}}, options)
	require.NoError(t, err)
	assert.Equal(t, 1, outcome.OverflowedTxs)
	assert.False(t, outcome.ActualWithinBudget)
	require.NoError(t, db.View(func(txn *badger.Txn) error {
		s := newStage(txn, 20)
		failed, err := s.loadHeader(first)
		require.NoError(t, err)
		assert.Equal(t, uint64(0), failed.Head, "cursor must not move")
		assert.Empty(t, failed.Parent)
		passed, err := s.loadHeader(second)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), passed.Head, "the rest of the block commits")
		return nil
	}))
	retry, err := runBlock(db, []txSpec{{Kind: txExpand, Decision: first}}, blockOpts(2))
	require.NoError(t, err)
	assert.True(t, retry.ActualWithinBudget)
	assert.Equal(t, int64(203), retry.NewMemberships, "the next block retries from the same cursor")
}

func TestOverCeilingBlockIsRejectedBeforeExecution(t *testing.T) {
	db := openTest(t)
	var fixture blockFixture
	seed(t, db, func(s seeder) {
		var err error
		fixture, err = seedFullAdmission(s, "ceiling", testProfile)
		require.NoError(t, err)
	})
	options := blockOpts(fixture.active)
	options.ceiling = 1000
	outcome, err := runBlock(db, fixture.txs, options)
	require.NoError(t, err)
	assert.Equal(t, "reject", outcome.ProcessProposal)
	assert.Less(t, outcome.SelectedByPrepare, len(fixture.txs))
}

func TestV30WritesCanTipABlockOverTheCeiling(t *testing.T) {
	db := openTest(t)
	var fixture blockFixture
	seed(t, db, func(s seeder) {
		var err error
		fixture, err = seedFullAdmission(s, "v30", testProfile)
		require.NoError(t, err)
	})
	options := blockOpts(fixture.active)
	probe := estimator{sizes: maximumSizes(), tombstone: options.tombstone, activeDecisions: fixture.active}
	options.ceiling = 0
	for _, tx := range fixture.txs {
		options.ceiling += probe.estimate(tx)
	}
	outcome, err := runBlock(db, fixture.txs, options)
	require.NoError(t, err)
	assert.Equal(t, "accept", outcome.ProcessProposal)
	assert.Equal(t, "reject", outcome.ProposalWithV30)
	assert.Positive(t, outcome.V30Writes)
	assert.Less(t, outcome.V30Writes, outcome.OtherWrites)
}

func TestTombstoneOverheadComesFromBadger(t *testing.T) {
	report, err := measureTombstone(t.TempDir())
	require.NoError(t, err)
	for _, sample := range report.Samples {
		assert.Equal(t, int64(badgerEntryOverhead), sample.OverheadBytes, "key bytes %d", sample.KeyBytes)
	}
	assert.Equal(t, int64(badgerEntryOverhead+badgerVersionSuffix), report.Constant)
}

func TestAssertArch(t *testing.T) {
	amd := machineContext{GOARCH: "amd64", UnameM: "x86_64"}
	arm := machineContext{GOARCH: "arm64", UnameM: "aarch64"}
	emulated := machineContext{GOARCH: "amd64", UnameM: "aarch64"}
	assert.NoError(t, assertArch(amd, "amd64"))
	assert.NoError(t, assertArch(arm, "arm64"))
	assert.NoError(t, assertArch(arm, ""))
	assert.ErrorContains(t, assertArch(arm, "amd64"), "architecture mismatch")
	assert.ErrorContains(t, assertArch(emulated, "amd64"), "architecture mismatch")
	assert.ErrorContains(t, assertArch(amd, "riscv64"), "unsupported")
}

func TestRealNodeCountsReadACopyAndNeverGuessMigrated(t *testing.T) {
	path := t.TempDir()
	db, err := openBadger(path)
	require.NoError(t, err)
	require.NoError(t, db.Update(func(txn *badger.Txn) error {
		require.NoError(t, txn.Set(memoryKey("short"), []byte("x")))
		require.NoError(t, txn.Set(memoryKey(makeID(memoryIDCap+1, "long", 0)), []byte("x")))
		return txn.Set(adoptionKey("short"), []byte("r"))
	}))
	require.NoError(t, db.Close())

	counts, err := countRealNode(path)
	require.NoError(t, err)
	assert.True(t, counts.Measured)
	assert.Equal(t, int64(2), *counts.Records)
	assert.Equal(t, int64(1), *counts.OverCapIDs)
	assert.Equal(t, int64(1), *counts.AdoptedRecords)
	assert.Nil(t, counts.Migrated)
	assert.NotEmpty(t, counts.MigratedReason)

	missing := notMeasured()
	assert.False(t, missing.Measured)
	assert.Nil(t, missing.Records)
}

func TestSmokeRunIsLabelledAndRepeatable(t *testing.T) {
	cfg, err := profileDefaults("smoke")
	require.NoError(t, err)
	cfg.Repeats, cfg.BackgroundRecords, cfg.Chains, cfg.FrontierEntries, cfg.FlatRecords = 1, 16, 1, 16, 4
	cfg.DataDir = t.TempDir()
	first, err := run(cfg, io.Discard)
	require.NoError(t, err)
	second, err := run(cfg, io.Discard)
	require.NoError(t, err)
	assert.True(t, first.StandIn)
	assert.Contains(t, first.Notice, "STAND-IN")
	assert.True(t, first.Invariants.Passed, "%v", first.Invariants.Failures)
	assert.False(t, first.RealNode.Measured)
	assert.Equal(t, first.DeterministicDigest, second.DeterministicDigest)
}
