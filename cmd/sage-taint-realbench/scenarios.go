package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math"
	"os"
	"sort"
	"time"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const benchmarkChainID = "sage-taint-realbench"

type benchmarkStore struct {
	store *store.BadgerStore
	dir   string
}

func runScenarios(config profile, parentDir string) ([]scenarioResult, error) {
	scenarios := make([]scenarioResult, 0, 4)
	scope, err := runScopeScenario(config, parentDir)
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, scope)
	lineage, err := runLineageScenario(config, parentDir)
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, lineage)
	admission, err := runAdmissionScenario(config, parentDir)
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, admission)
	fanout, err := runFanoutScenario(config, parentDir)
	if err != nil {
		return nil, err
	}
	scenarios = append(scenarios, fanout)
	return scenarios, nil
}

func openBenchmarkStore(parentDir string) (*benchmarkStore, error) {
	dir, err := os.MkdirTemp(parentDir, "sage-taint-realbench-*")
	if err != nil {
		return nil, fmt.Errorf("create benchmark data directory: %w", err)
	}
	bs, err := store.NewBadgerStore(dir)
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, fmt.Errorf("open benchmark Badger: %w", err)
	}
	return &benchmarkStore{store: bs, dir: dir}, nil
}

func (b *benchmarkStore) Close() error {
	if b == nil {
		return nil
	}
	var closeErr error
	if b.store != nil {
		closeErr = b.store.CloseBadger()
	}
	removeErr := os.RemoveAll(b.dir)
	if closeErr != nil {
		return closeErr
	}
	return removeErr
}

func runScopeScenario(config profile, parentDir string) (scenarioResult, error) {
	b, err := openBenchmarkStore(parentDir)
	if err != nil {
		return scenarioResult{}, err
	}
	defer b.Close() //nolint:errcheck
	matchingCredential := benchmarkCredential("scope/matching", 0)
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		target, err := seedBenchmarkMemory(b.store, fmt.Sprintf("scope-target-%02d", i), "scope target")
		if err != nil {
			return scenarioResult{}, err
		}
		decisionID := benchmarkDecisionID(fmt.Sprintf("scope-decision-%02d", i))
		scopeCredential := benchmarkCredential("scope/credential", i)
		if i == taint.MaxActiveDecisions-1 {
			scopeCredential = matchingCredential
		}
		created, err := b.store.ApplyTaintDecisionWithScope(
			benchmarkChainID,
			decisionID,
			target,
			sha256.Sum256([]byte("scope payload "+decisionID)),
			store.TaintDecisionScope{
				Kind:        tx.TaintScopeCredentialSet,
				Credentials: []string{scopeCredential},
			},
		)
		if err != nil || !created {
			return scenarioResult{}, fmt.Errorf("apply scope decision %d: created=%v err=%w", i, created, err)
		}
		if _, err := b.store.ExpandTaintDecision(
			benchmarkChainID,
			decisionID,
			taint.PerExecutionUnitBudget,
		); err != nil {
			return scenarioResult{}, err
		}
	}
	signers := make([]store.AppV30ProvenanceSigner, 0, config.ScopeSigners)
	for i := 0; i < config.ScopeSigners; i++ {
		credential := benchmarkCredential("scope/signer", i)
		signers = append(signers, store.AppV30ProvenanceSigner{
			CredentialID: credential,
			PrincipalID:  credential,
			Exact:        true,
		})
	}
	signers[config.ScopeSigners-1].CredentialID = matchingCredential
	signers[config.ScopeSigners-1].PrincipalID = matchingCredential
	if err := b.store.SetAppV30Provenance("scope-record", signers); err != nil {
		return scenarioResult{}, err
	}

	verdict, err := b.store.ClassifyMemoryTaint("scope-record")
	if err != nil {
		return scenarioResult{}, err
	}
	det := deterministicScenario{
		Name:        "scope_match",
		Outcome:     taintOutcomeName(verdict.Outcome),
		DecisionIDs: len(verdict.DecisionIDs),
		Nodes:       verdict.Nodes,
		Units:       verdict.Units,
		Coverage:    verdict.CoverageComplete,
	}
	if err := finalizeDeterministic(b.store, &det); err != nil {
		return scenarioResult{}, err
	}
	timing, err := measureOperation(config.Repeats, func() error {
		got, err := b.store.ClassifyMemoryTaint("scope-record")
		if err != nil {
			return err
		}
		if got.Units != det.Units || len(got.DecisionIDs) != det.DecisionIDs {
			return fmt.Errorf("scope classification changed: units %d/%d decisions %d/%d",
				got.Units, det.Units, len(got.DecisionIDs), det.DecisionIDs)
		}
		return nil
	})
	if err != nil {
		return scenarioResult{}, err
	}
	return scenarioResult{Deterministic: det, Timing: timing}, nil
}

func runLineageScenario(config profile, parentDir string) (scenarioResult, error) {
	b, err := openBenchmarkStore(parentDir)
	if err != nil {
		return scenarioResult{}, err
	}
	defer b.Close() //nolint:errcheck
	nodes := config.ChainNodes
	if nodes > taint.MaxServingAncestorWalk {
		nodes = taint.MaxServingAncestorWalk
	}
	refs := make([]tx.ParentRef, nodes)
	for i := range refs {
		ref, err := seedBenchmarkMemory(b.store, fmt.Sprintf("lineage-%03d", i), "lineage node")
		if err != nil {
			return scenarioResult{}, err
		}
		refs[i] = ref
		if i > 0 {
			if err := b.store.SetMemoryLineage(ref.MemoryID, []tx.ParentRef{refs[i-1]}); err != nil {
				return scenarioResult{}, err
			}
		}
	}
	decisionID := benchmarkDecisionID("lineage-decision")
	created, err := b.store.ApplyTaintDecision(
		benchmarkChainID,
		decisionID,
		refs[0],
		sha256.Sum256([]byte("lineage payload")),
	)
	if err != nil || !created {
		return scenarioResult{}, fmt.Errorf("apply lineage decision: created=%v err=%w", created, err)
	}
	leafID := refs[len(refs)-1].MemoryID
	verdict, err := b.store.ClassifyMemoryTaint(leafID)
	if err != nil {
		return scenarioResult{}, err
	}
	det := deterministicScenario{
		Name:        "lineage_walk",
		Outcome:     taintOutcomeName(verdict.Outcome),
		DecisionIDs: len(verdict.DecisionIDs),
		Nodes:       verdict.Nodes,
		Units:       verdict.Units,
		Coverage:    verdict.CoverageComplete,
	}
	if err := finalizeDeterministic(b.store, &det); err != nil {
		return scenarioResult{}, err
	}
	timing, err := measureOperation(config.Repeats, func() error {
		got, err := b.store.ClassifyMemoryTaint(leafID)
		if err != nil {
			return err
		}
		if got.Nodes != det.Nodes || got.Units != det.Units || got.Outcome != verdict.Outcome {
			return fmt.Errorf("lineage classification changed")
		}
		return nil
	})
	if err != nil {
		return scenarioResult{}, err
	}
	return scenarioResult{Deterministic: det, Timing: timing}, nil
}

func runAdmissionScenario(config profile, parentDir string) (scenarioResult, error) {
	b, err := openBenchmarkStore(parentDir)
	if err != nil {
		return scenarioResult{}, err
	}
	defer b.Close() //nolint:errcheck
	parents := make([]tx.ParentRef, taint.MaxParents)
	for i := range parents {
		ref, err := seedBenchmarkMemory(b.store, fmt.Sprintf("admission-parent-%02d", i), "admission parent")
		if err != nil {
			return scenarioResult{}, err
		}
		parents[i] = ref
	}
	tx.SortParentRefs(parents)
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		decisionID := benchmarkDecisionID(fmt.Sprintf("admission-decision-%02d", i))
		created, err := b.store.ApplyTaintDecision(
			benchmarkChainID,
			decisionID,
			parents[0],
			sha256.Sum256([]byte("admission payload "+decisionID)),
		)
		if err != nil || !created {
			return scenarioResult{}, fmt.Errorf("apply admission decision %d: created=%v err=%w", i, created, err)
		}
	}
	for i := 1; i < len(parents); i++ {
		if _, err := b.store.ApplyTaintAdmission(
			benchmarkChainID,
			parents[i].MemoryID,
			[]tx.ParentRef{parents[0]},
			nil,
		); err != nil {
			return scenarioResult{}, err
		}
	}
	signers := []store.AppV30ProvenanceSigner{{
		CredentialID: benchmarkCredential("admission/signer", 0),
		PrincipalID:  benchmarkCredential("admission/signer", 0),
		Exact:        true,
	}}
	childID := "admission-child"
	planned, err := b.store.ValidateTaintAdmission(
		benchmarkChainID,
		childID,
		parents,
		signers,
	)
	if err != nil {
		return scenarioResult{}, err
	}
	det := deterministicScenario{
		Name:      "admission_plan",
		Decisions: planned.Decisions,
		Units:     planned.Units,
	}
	applied, err := b.store.ApplyTaintAdmission(
		benchmarkChainID,
		childID,
		parents,
		signers,
	)
	if err != nil {
		return scenarioResult{}, err
	}
	if applied.Decisions != planned.Decisions || applied.Units != planned.Units {
		return scenarioResult{}, fmt.Errorf("admission apply disagrees with validation")
	}
	if err := finalizeDeterministic(b.store, &det); err != nil {
		return scenarioResult{}, err
	}
	timing, err := measureOperation(config.Repeats, func() error {
		got, err := b.store.ValidateTaintAdmission(
			benchmarkChainID,
			childID,
			parents,
			signers,
		)
		if err != nil {
			return err
		}
		if got.Decisions != det.Decisions || got.Units != det.Units {
			return fmt.Errorf("admission validation changed")
		}
		return nil
	})
	if err != nil {
		return scenarioResult{}, err
	}
	return scenarioResult{Deterministic: det, Timing: timing}, nil
}

func runFanoutScenario(config profile, parentDir string) (scenarioResult, error) {
	var baseline deterministicScenario
	durations := make([]time.Duration, 0, config.Repeats)
	for repeat := 0; repeat < config.Repeats; repeat++ {
		b, err := openBenchmarkStore(parentDir)
		if err != nil {
			return scenarioResult{}, err
		}
		root, err := seedBenchmarkMemory(b.store, "fanout-root", "fanout root")
		if err != nil {
			_ = b.Close()
			return scenarioResult{}, err
		}
		for i := 0; i < config.Fanout; i++ {
			child, err := seedBenchmarkMemory(
				b.store,
				fmt.Sprintf("fanout-child-%04d", i),
				"fanout child",
			)
			if err != nil {
				_ = b.Close()
				return scenarioResult{}, err
			}
			if err := b.store.SetMemoryLineage(child.MemoryID, []tx.ParentRef{root}); err != nil {
				_ = b.Close()
				return scenarioResult{}, err
			}
		}
		decisionID := benchmarkDecisionID("fanout-decision")
		created, err := b.store.ApplyTaintDecision(
			benchmarkChainID,
			decisionID,
			root,
			sha256.Sum256([]byte("fanout payload")),
		)
		if err != nil || !created {
			_ = b.Close()
			return scenarioResult{}, fmt.Errorf("apply fanout decision: created=%v err=%w", created, err)
		}
		start := time.Now()
		expanded, err := b.store.ExpandTaintDecision(
			benchmarkChainID,
			decisionID,
			taint.PerExecutionUnitBudget,
		)
		durations = append(durations, time.Since(start))
		if err != nil {
			_ = b.Close()
			return scenarioResult{}, err
		}
		det := deterministicScenario{
			Name:         "fanout_expand",
			Status:       fmt.Sprint(expanded.Status),
			Units:        expanded.Units,
			Materialized: expanded.Materialized,
			Scanned:      expanded.Scanned,
			Enqueued:     expanded.Enqueued,
		}
		if err := finalizeDeterministic(b.store, &det); err != nil {
			_ = b.Close()
			return scenarioResult{}, err
		}
		if repeat == 0 {
			baseline = det
		} else if det != baseline {
			_ = b.Close()
			return scenarioResult{}, fmt.Errorf("fanout expansion differs across repeats")
		}
		if err := b.Close(); err != nil {
			return scenarioResult{}, err
		}
	}
	return scenarioResult{Deterministic: baseline, Timing: summarizeDurations(durations)}, nil
}

func seedBenchmarkMemory(
	bs *store.BadgerStore,
	memoryID, content string,
) (tx.ParentRef, error) {
	digest := sha256.Sum256([]byte(memoryID + "\x00" + content))
	if err := bs.SetMemoryHash(memoryID, digest[:], "committed"); err != nil {
		return tx.ParentRef{}, err
	}
	return tx.ParentRef{
		SourceChainID: benchmarkChainID,
		MemoryID:      memoryID,
		ContentHash:   digest,
	}, nil
}

func benchmarkDecisionID(namespace string) string {
	digest := sha256.Sum256([]byte("sage-taint-realbench/decision\x00" + namespace))
	return hex.EncodeToString(digest[:])
}

func benchmarkCredential(namespace string, index int) string {
	digest := sha256.Sum256(fmt.Appendf(nil, "sage-taint-realbench/%s/%d", namespace, index))
	return hex.EncodeToString(digest[:])
}

func taintOutcomeName(outcome store.TaintServingOutcome) string {
	switch outcome {
	case store.TaintServingClear:
		return "clear"
	case store.TaintServingTainted:
		return "tainted"
	case store.TaintServingPending:
		return "pending"
	default:
		return "unknown"
	}
}

func finalizeDeterministic(bs *store.BadgerStore, det *deterministicScenario) error {
	appHash, err := bs.ComputeAppHash()
	if err != nil {
		return err
	}
	keys, keyBytes, valueBytes, err := countBadgerState(bs.DB())
	if err != nil {
		return err
	}
	det.AppHash = hex.EncodeToString(appHash)
	det.Keys = keys
	det.KeyBytes = keyBytes
	det.ValueBytes = valueBytes
	return nil
}

func countBadgerState(db *badger.DB) (keys int, keyBytes, valueBytes int64, err error) {
	err = db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			keys++
			keyBytes += int64(len(item.Key()))
			if err := item.Value(func(value []byte) error {
				valueBytes += int64(len(value))
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	})
	return keys, keyBytes, valueBytes, err
}

func measureOperation(repeats int, operation func() error) (timingSummary, error) {
	if err := operation(); err != nil {
		return timingSummary{}, err
	}
	durations := make([]time.Duration, 0, repeats)
	for i := 0; i < repeats; i++ {
		start := time.Now()
		if err := operation(); err != nil {
			return timingSummary{}, err
		}
		durations = append(durations, time.Since(start))
	}
	return summarizeDurations(durations), nil
}

func summarizeDurations(durations []time.Duration) timingSummary {
	if len(durations) == 0 {
		return timingSummary{}
	}
	sorted := append([]time.Duration(nil), durations...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	var total time.Duration
	for _, duration := range sorted {
		total += duration
	}
	p95Index := int(math.Ceil(float64(len(sorted))*0.95)) - 1
	if p95Index < 0 {
		p95Index = 0
	}
	p50Index := (len(sorted) - 1) / 2
	return timingSummary{
		MinMS:  durationMS(sorted[0]),
		P50MS:  durationMS(sorted[p50Index]),
		P95MS:  durationMS(sorted[p95Index]),
		MaxMS:  durationMS(sorted[len(sorted)-1]),
		MeanMS: durationMS(total / time.Duration(len(sorted))),
	}
}

func durationMS(duration time.Duration) float64 {
	return float64(duration) / float64(time.Millisecond)
}
