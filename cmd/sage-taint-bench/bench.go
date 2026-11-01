package main

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"time"

	badger "github.com/dgraph-io/badger/v4"
)

const standInNotice = "STAND-IN MEASUREMENT: the taint protocol is not implemented yet. These numbers come from a stand-in over real Badger using the key layouts and costs planned in RFC #460, not from SAGE consensus code. There is no AppHash, CometBFT, SQL mirror or real node here. All constants stay provisional."

type config struct {
	Profile           string
	DataDir           string `json:"-"`
	Repeats           int
	BackgroundRecords int
	Chains            int
	FrontierEntries   int
	FrontierDecisions int
	FlatRecords       int
	ContentBytes      int
	UnitCeiling       int64
	BlockCeiling      int64
	NodeCopy          string `json:"-"`
}

type timing struct {
	Samples int     `json:"samples"`
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	MaxMS   float64 `json:"max_ms"`
}

func summarize(samples []time.Duration) timing {
	if len(samples) == 0 {
		return timing{}
	}
	sorted := slices.Sorted(slices.Values(samples))
	at := func(q float64) float64 {
		return float64(sorted[int(q*float64(len(sorted)-1))]) / float64(time.Millisecond)
	}
	return timing{Samples: len(sorted), P50MS: at(0.5), P95MS: at(0.95), MaxMS: at(1)}
}

type readDeterministic struct {
	Name                string `json:"name"`
	IDProfile           string `json:"id_profile"`
	Candidates          int    `json:"candidates"`
	UnitsCharged        int64  `json:"units_charged"`
	FailedClosed        bool   `json:"failed_closed_at_ceiling"`
	DemandUnits         int64  `json:"demand_units_uncapped"`
	ExtrapolatedDemand  int64  `json:"extrapolated_demand_at_8192_candidates,omitempty"`
	BoundaryExactPassed bool   `json:"ceiling_equal_to_demand_succeeds"`
	BoundaryLowFailed   bool   `json:"ceiling_one_below_demand_fails_closed"`
}

type readResult struct {
	Deterministic readDeterministic `json:"deterministic"`
	Timing        timing            `json:"timing_at_ceiling"`
	UncappedMS    float64           `json:"uncapped_ms"`
}

type blockResult struct {
	Name          string             `json:"name"`
	IDProfile     string             `json:"id_profile"`
	Deterministic blockDeterministic `json:"deterministic"`
	Finalize      timing             `json:"finalize_stage_flush"`
	Commit        timing             `json:"commit"`
	Sync          timing             `json:"sync"`
	Total         timing             `json:"total"`
}

type unitCeilingReport struct {
	Provisional         int64    `json:"provisional"`
	WorstSeededDemand   int64    `json:"worst_seeded_demand"`
	WorstExtrapolated   int64    `json:"worst_extrapolated_demand_at_8192_candidates"`
	FailedClosed        []string `json:"scenarios_failed_closed"`
	WorstP95MSAtCeiling float64  `json:"worst_p95_ms_at_ceiling"`
}

type blockCeilingReport struct {
	Provisional   int64    `json:"provisional_bytes"`
	WorstEstimate int64    `json:"worst_valid_block_estimate_bytes"`
	WorstActual   int64    `json:"worst_valid_block_actual_bytes"`
	WorstSubmitE  int64    `json:"estimate_bytes_per_worst_submit"`
	WorstSubmits  int64    `json:"worst_submits_that_fit_block_ceiling"`
	Rejected      []string `json:"scenarios_rejected_by_process_proposal"`
}

type expansionReport struct {
	PerTxCeiling        int   `json:"new_memberships_per_execution_ceiling"`
	PerTxReachable      int64 `json:"new_memberships_per_execution_reachable"`
	EstimatePerExpandTx int64 `json:"estimate_bytes_per_expand_tx"`
	CeilingExpandTxs    int64 `json:"expand_txs_that_fit_block_ceiling"`
	PlannedExpandTxs    int   `json:"planned_expand_cleanup_txs_per_block"`
	PerBlockAtPlanned   int64 `json:"new_memberships_per_block_at_planned_limit"`
	PerBlockAtCeiling   int64 `json:"new_memberships_per_block_at_byte_ceiling"`
	DecisionCap         int   `json:"expansion_cap_per_decision_provisional"`
}

type sizeReport struct {
	MembershipTripleActual int64 `json:"membership_triple_bytes_actual"`
	MembershipTripleRFC    int64 `json:"membership_triple_bytes_rfc_derivation"`
	HeaderActual           int64 `json:"header_entry_bytes_actual"`
	HeaderRFC              int64 `json:"header_entry_bytes_rfc_derivation"`
}

type constantsReport struct {
	Units     unitCeilingReport  `json:"unit_ceiling"`
	Block     blockCeilingReport `json:"block_write_ceiling"`
	Tombstone int64              `json:"tombstone_constant_bytes"`
	Expansion expansionReport    `json:"expansion"`
	Sizes     sizeReport         `json:"encoded_sizes"`
}

type invariants struct {
	Passed   bool     `json:"passed"`
	Failures []string `json:"failures"`
}

type benchResult struct {
	Schema              string          `json:"schema"`
	StandIn             bool            `json:"stand_in"`
	Notice              string          `json:"notice"`
	Machine             machineContext  `json:"machine"`
	Commit              commitContext   `json:"commit"`
	Config              config          `json:"config"`
	Tombstone           tombstoneReport `json:"tombstone"`
	Constants           constantsReport `json:"constants"`
	Reads               []readResult    `json:"reads"`
	Blocks              []blockResult   `json:"blocks"`
	RealNode            realNodeCounts  `json:"real_node_counts"`
	DeterministicDigest string          `json:"deterministic_digest"`
	Invariants          invariants      `json:"invariants"`
}

func openBadger(path string) (*badger.DB, error) {
	// Same options as store.NewBadgerStore.
	options := badger.DefaultOptions(path)
	options.Logger = nil
	return badger.Open(options)
}

type blockPlan struct {
	name    string
	profile idProfile
	repeats []blockFixture
}

type readPlan struct {
	profile idProfile
	fixture readFixture
}

func sweepCounts(ceiling, tombstone int64) []int {
	est := estimator{sizes: maximumSizes(), tombstone: tombstone}
	fit := int(ceiling / est.estimate(txSpec{Kind: txExpand}))
	counts := []int{}
	for _, count := range []int{1, 2, 4, 8} {
		if count < fit {
			counts = append(counts, count)
		}
	}
	return append(counts, fit, fit+1)
}

func seedAll(db *badger.DB, cfg config, tombstone int64) ([]readPlan, []blockPlan, error) {
	if err := seedBackground(db, cfg.BackgroundRecords, 2048); err != nil {
		return nil, nil, fmt.Errorf("seed background: %w", err)
	}
	batch := db.NewWriteBatch()
	defer batch.Cancel()
	s := seeder{batch: batch}
	var reads []readPlan
	var blocks []blockPlan
	type builder struct {
		name  string
		build func(ns string, p idProfile) (blockFixture, error)
	}
	builders := []builder{
		{"full_admission", func(ns string, p idProfile) (blockFixture, error) { return seedFullAdmission(s, ns, p) }},
		{"mixed_expand", func(ns string, p idProfile) (blockFixture, error) { return seedMixed(s, ns, p, false) }},
		{"mixed_cleanup", func(ns string, p idProfile) (blockFixture, error) { return seedMixed(s, ns, p, true) }},
		{"cleanup_only", func(ns string, p idProfile) (blockFixture, error) { return seedCleanupBlock(s, ns, p) }},
	}
	for _, count := range sweepCounts(cfg.BlockCeiling, tombstone) {
		builders = append(builders, builder{fmt.Sprintf("expand_sweep_%d", count), func(ns string, p idProfile) (blockFixture, error) {
			return seedExpandSweep(s, ns, p, count)
		}})
	}
	for _, p := range idProfiles {
		fixture, err := seedRead(s, p, cfg.Chains, cfg.FrontierEntries, cfg.FrontierDecisions, cfg.FlatRecords)
		if err != nil {
			return nil, nil, fmt.Errorf("seed reads: %w", err)
		}
		reads = append(reads, readPlan{profile: p, fixture: fixture})
		for _, b := range builders {
			plan := blockPlan{name: b.name, profile: p}
			for repeat := range cfg.Repeats {
				built, err := b.build(fmt.Sprintf("%s/%s/%d", p.Name, b.name, repeat), p)
				if err != nil {
					return nil, nil, fmt.Errorf("seed %s: %w", b.name, err)
				}
				plan.repeats = append(plan.repeats, built)
			}
			blocks = append(blocks, plan)
		}
	}
	return reads, blocks, batch.Flush()
}

type readCase struct {
	name       string
	candidates int
	exec       func(*request) error
}

func walkCandidates(ids []string) func(*request) error {
	return func(r *request) error {
		for _, id := range ids {
			held, err := r.verdict(id)
			if err != nil {
				return err
			}
			if held {
				return fmt.Errorf("candidate %s was held; the fixture has no effective membership", id)
			}
		}
		return nil
	}
}

func execRead(db *badger.DB, c readCase, ceiling int64) (int64, bool, time.Duration, error) {
	var units int64
	failedClosed := false
	var elapsed time.Duration
	err := db.View(func(txn *badger.Txn) error {
		started := time.Now()
		r, err := newRequest(txn, ceiling)
		if err != nil {
			return err
		}
		err = c.exec(r)
		elapsed = time.Since(started)
		units = r.meter.units
		if errors.Is(err, errUnitCeiling) {
			failedClosed = true
			return nil
		}
		return err
	})
	return units, failedClosed, elapsed, err
}

func measureRead(db *badger.DB, c readCase, p idProfile, cfg config) (readResult, error) {
	result := readResult{Deterministic: readDeterministic{Name: c.name, IDProfile: p.Name, Candidates: c.candidates}}
	det := &result.Deterministic
	samples := make([]time.Duration, 0, cfg.Repeats)
	for repeat := range cfg.Repeats {
		units, failed, elapsed, err := execRead(db, c, cfg.UnitCeiling)
		if err != nil {
			return result, err
		}
		if repeat > 0 && (units != det.UnitsCharged || failed != det.FailedClosed) {
			return result, fmt.Errorf("%s: repeat %d charged %d units, first charged %d", c.name, repeat, units, det.UnitsCharged)
		}
		det.UnitsCharged, det.FailedClosed = units, failed
		samples = append(samples, elapsed)
	}
	result.Timing = summarize(samples)
	demand, _, elapsed, err := execRead(db, c, 0)
	if err != nil {
		return result, err
	}
	det.DemandUnits = demand
	result.UncappedMS = float64(elapsed) / float64(time.Millisecond)
	if c.candidates > 0 {
		det.ExtrapolatedDemand = demand * candidateBudget / int64(c.candidates)
	}
	_, exactFailed, _, err := execRead(db, c, demand)
	if err != nil {
		return result, err
	}
	_, lowFailed, _, err := execRead(db, c, demand-1)
	if err != nil {
		return result, err
	}
	det.BoundaryExactPassed, det.BoundaryLowFailed = !exactFailed, lowFailed
	return result, nil
}

func measureBlock(db *badger.DB, plan blockPlan, cfg config, tombstone int64) (blockResult, error) {
	result := blockResult{Name: plan.name, IDProfile: plan.profile.Name}
	var finalize, commit, sync, total []time.Duration
	for i, fixture := range plan.repeats {
		outcome, err := runBlock(db, fixture.txs, blockOptions{
			ceiling: cfg.BlockCeiling, tombstone: tombstone, activeAtHeight: fixture.active, contentBytes: cfg.ContentBytes,
		})
		if err != nil {
			return result, fmt.Errorf("%s/%s repeat %d: %w", plan.profile.Name, plan.name, i, err)
		}
		if i == 0 {
			result.Deterministic = outcome.blockDeterministic
		} else if !sameBlock(result.Deterministic, outcome.blockDeterministic) {
			return result, fmt.Errorf("%s/%s repeat %d differs from the first", plan.profile.Name, plan.name, i)
		}
		finalize = append(finalize, outcome.finalize)
		commit = append(commit, outcome.commit)
		sync = append(sync, outcome.sync)
		total = append(total, outcome.finalize+outcome.commit+outcome.sync)
	}
	result.Finalize, result.Commit, result.Sync, result.Total = summarize(finalize), summarize(commit), summarize(sync), summarize(total)
	return result, nil
}

func sameBlock(a, b blockDeterministic) bool {
	first, _ := json.Marshal(a)
	second, _ := json.Marshal(b)
	return string(first) == string(second)
}

func run(cfg config, log io.Writer) (*benchResult, error) {
	result := &benchResult{Schema: "sage-taint-bench/v1", StandIn: true, Notice: standInNotice, Config: cfg}
	fmt.Fprintln(log, standInNotice)
	dir, err := os.MkdirTemp(cfg.DataDir, "sage-taint-bench-")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(dir) }()

	fmt.Fprintln(log, "measuring tombstone overhead")
	result.Tombstone, err = measureTombstone(dir)
	if err != nil {
		return nil, fmt.Errorf("measure tombstone: %w", err)
	}
	tombstone := result.Tombstone.Constant

	path := filepath.Join(dir, "state")
	db, err := openBadger(path)
	if err != nil {
		return nil, err
	}
	fmt.Fprintln(log, "seeding")
	reads, blocks, err := seedAll(db, cfg, tombstone)
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	// Reopen so reads come from SSTs and not the memtable.
	if err = db.Close(); err != nil {
		return nil, err
	}
	if db, err = openBadger(path); err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	for _, plan := range reads {
		wide := plan.fixture.wide
		cases := []readCase{
			{"deep_walk_64_coauthors", len(plan.fixture.deep), walkCandidates(plan.fixture.deep)},
			{"flat_pre_v30_64_coauthors", len(plan.fixture.flat), walkCandidates(plan.fixture.flat)},
			{"wide_frontier", 0, func(r *request) error {
				for _, decision := range wide {
					if frontierErr := r.frontier(decision); frontierErr != nil {
						return frontierErr
					}
				}
				return nil
			}},
		}
		for _, c := range cases {
			fmt.Fprintf(log, "read %s/%s\n", plan.profile.Name, c.name)
			measured, readErr := measureRead(db, c, plan.profile, cfg)
			if readErr != nil {
				return nil, fmt.Errorf("read %s: %w", c.name, readErr)
			}
			result.Reads = append(result.Reads, measured)
		}
	}
	for _, plan := range blocks {
		fmt.Fprintf(log, "block %s/%s\n", plan.profile.Name, plan.name)
		measured, blockErr := measureBlock(db, plan, cfg, tombstone)
		if blockErr != nil {
			return nil, blockErr
		}
		result.Blocks = append(result.Blocks, measured)
	}
	result.Constants = summarizeConstants(result, cfg, tombstone)
	result.Invariants = checkInvariants(result)
	result.DeterministicDigest, err = digest(result)
	if err != nil {
		return nil, err
	}
	result.RealNode = notMeasured()
	if cfg.NodeCopy != "" {
		if result.RealNode, err = countRealNode(cfg.NodeCopy); err != nil {
			return nil, fmt.Errorf("read node copy: %w", err)
		}
	}
	return result, nil
}

func summarizeConstants(result *benchResult, cfg config, tombstone int64) constantsReport {
	est := estimator{sizes: maximumSizes(), tombstone: tombstone}
	constants := constantsReport{Tombstone: tombstone}
	constants.Units.Provisional = cfg.UnitCeiling
	for _, read := range result.Reads {
		det := read.Deterministic
		constants.Units.WorstSeededDemand = max(constants.Units.WorstSeededDemand, det.DemandUnits)
		constants.Units.WorstExtrapolated = max(constants.Units.WorstExtrapolated, det.ExtrapolatedDemand)
		constants.Units.WorstP95MSAtCeiling = max(constants.Units.WorstP95MSAtCeiling, read.Timing.P95MS)
		if det.FailedClosed {
			constants.Units.FailedClosed = append(constants.Units.FailedClosed, det.IDProfile+"/"+det.Name)
		}
	}
	constants.Block.Provisional = cfg.BlockCeiling
	for _, block := range result.Blocks {
		det := block.Deterministic
		if det.ProcessProposal == "reject" {
			constants.Block.Rejected = append(constants.Block.Rejected, block.IDProfile+"/"+block.Name)
		}
		if det.ProcessProposal == "accept" && (block.Name == "full_admission" || block.Name == "mixed_expand" || block.Name == "mixed_cleanup" || block.Name == "cleanup_only") {
			constants.Block.WorstEstimate = max(constants.Block.WorstEstimate, det.Estimate)
			constants.Block.WorstActual = max(constants.Block.WorstActual, det.GeneratedTaint+det.V30Writes)
		}
	}
	worst := est
	worst.activeDecisions = maxActiveDecisions
	constants.Block.WorstSubmitE = worst.estimate(worstSubmit())
	constants.Block.WorstSubmits = cfg.BlockCeiling / constants.Block.WorstSubmitE
	// A batch resuming a parent pays no queue pop, so it is the larger one.
	reachable := int64(min(maxNewPerExecution, (txUnitBudget-3)/5))
	perTx := est.estimate(txSpec{Kind: txExpand})
	constants.Expansion = expansionReport{
		PerTxCeiling: maxNewPerExecution, PerTxReachable: reachable, EstimatePerExpandTx: perTx,
		CeilingExpandTxs: cfg.BlockCeiling / perTx, PlannedExpandTxs: maxExpandCleanupTxs,
		PerBlockAtPlanned: maxExpandCleanupTxs * reachable, PerBlockAtCeiling: cfg.BlockCeiling / perTx * reachable,
		DecisionCap: expansionCap,
	}
	constants.Sizes = sizeReport{
		MembershipTripleActual: est.sizes.triple, MembershipTripleRFC: 1728,
		HeaderActual: est.sizes.header, HeaderRFC: 1280,
	}
	return constants
}

// worstSubmit has the largest shape the protocol limits allow.
func worstSubmit() txSpec {
	parents := make([]string, maxParents)
	for i := range parents {
		parents[i] = makeID(memoryIDCap, "worst/parent", i)
	}
	return txSpec{Kind: txSubmit, Memory: makeID(memoryIDCap, "worst/child", 0), Parents: parents}
}

func checkInvariants(result *benchResult) invariants {
	check := invariants{Passed: true, Failures: []string{}}
	fail := func(format string, args ...any) {
		check.Passed = false
		check.Failures = append(check.Failures, fmt.Sprintf(format, args...))
	}
	for _, read := range result.Reads {
		det := read.Deterministic
		label := det.IDProfile + "/" + det.Name
		if !det.BoundaryExactPassed || !det.BoundaryLowFailed {
			fail("%s: ceiling boundary not exact", label)
		}
		if det.DemandUnits > result.Config.UnitCeiling && !det.FailedClosed {
			fail("%s: demand above the ceiling did not fail closed", label)
		}
		if det.UnitsCharged > result.Config.UnitCeiling {
			fail("%s: charged %d units over the ceiling", label, det.UnitsCharged)
		}
	}
	for _, block := range result.Blocks {
		det := block.Deterministic
		label := block.IDProfile + "/" + block.Name
		if !det.ActualWithinBudget || det.OverflowedTxs > 0 || det.GeneratedTaint+det.V30Writes > det.Estimate {
			fail("%s: actual bytes exceeded the estimate", label)
		}
		if det.MaxUnitsByKind[txSubmit] > submitUnitBound {
			fail("%s: submit charged %d units, bound %d", label, det.MaxUnitsByKind[txSubmit], submitUnitBound)
		}
		for _, kind := range []txKind{txApply, txExpand, txCleanup} {
			if det.MaxUnitsByKind[kind] > txUnitBudget {
				fail("%s: %s charged %d units, budget %d", label, kind, det.MaxUnitsByKind[kind], txUnitBudget)
			}
		}
	}
	return check
}

// digest covers only values that must match across architectures. Compare it
// between the arm64 and amd64 runs.
func digest(result *benchResult) (string, error) {
	type block struct {
		Name          string
		IDProfile     string
		Deterministic blockDeterministic
	}
	view := struct {
		Reads     []readDeterministic
		Blocks    []block
		Tombstone []tombstoneSample
		Constant  int64
	}{Tombstone: result.Tombstone.Samples, Constant: result.Tombstone.Constant}
	for _, read := range result.Reads {
		view.Reads = append(view.Reads, read.Deterministic)
	}
	for _, b := range result.Blocks {
		view.Blocks = append(view.Blocks, block{b.Name, b.IDProfile, b.Deterministic})
	}
	encoded, err := json.Marshal(view)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:]), nil
}
