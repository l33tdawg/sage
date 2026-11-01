package store

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

var (
	ErrTaintServingWalkBudget = errors.New("taint serving ancestor walk exceeded its budget")
	ErrTaintServingState      = errors.New("taint serving state is malformed")
)

type TaintServingOutcome uint8

const (
	TaintServingClear TaintServingOutcome = iota
	TaintServingTainted
	TaintServingPending
)

type TaintServingVerdict struct {
	Outcome          TaintServingOutcome
	DecisionIDs      []string
	Nodes            int
	Units            int
	CoverageComplete bool
	LineageMissing   bool
	DepthExhausted   bool
	WorkExhausted    bool
}

// ClassifyMemoryTaint checks direct memberships and the authenticated ancestor
// path in one Badger snapshot. It never treats an absent materialized
// membership as proof that an incomplete ancestor decision is independent.
func (s *BadgerStore) ClassifyMemoryTaint(memoryID string) (TaintServingVerdict, error) {
	verdict := TaintServingVerdict{CoverageComplete: true}
	if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
		return verdict, fmt.Errorf("%w: invalid memory ID", ErrTaintServingState)
	}
	err := s.view(func(txn *badger.Txn) error {
		meter := newTaintUnitMeter(taint.MaxServingRequestUnits, ErrTaintServingWalkBudget)
		activeScopes, err := readActiveTaintDecisionScopesTxn(txn, meter)
		if err != nil {
			return err
		}
		queue := []string{memoryID}
		visited := make(map[string]struct{}, taint.MaxServingAncestorWalk)
		for len(queue) > 0 {
			current := queue[0]
			queue = queue[1:]
			if _, seen := visited[current]; seen {
				continue
			}
			if len(visited) >= taint.MaxServingAncestorWalk {
				return ErrTaintServingWalkBudget
			}
			visited[current] = struct{}{}
			verdict.Nodes++

			decisions, decisionHeaders, err := getEffectiveTaintDecisionsTxn(
				txn,
				current,
				meter,
			)
			if err != nil {
				return err
			}
			provenance, err := authorProvenanceTxn(txn, current, meter)
			if err != nil {
				return err
			}

			matched := append([]string(nil), decisions...)
			directTainted := false
			scopeTainted := false
			for _, decisionID := range decisions {
				header := decisionHeaders[decisionID]
				if header.Status.Incomplete() {
					verdict.CoverageComplete = false
				}
				if current == memoryID || !header.Status.Incomplete() {
					directTainted = true
				}
				recordTaintExhaustion(&verdict, header.Status)
			}
			for _, active := range activeScopes {
				if !active.Scope.Matches(provenance) {
					continue
				}
				scopeTainted = true
				matched = appendDecisionIDUnique(matched, active.DecisionID)
				if active.Header.Status.Incomplete() {
					verdict.CoverageComplete = false
				}
				recordTaintExhaustion(&verdict, active.Header.Status)
			}
			if len(matched) > 0 {
				verdict.DecisionIDs = sortedUniqueDecisionIDs(matched)
				if directTainted || scopeTainted {
					verdict.Outcome = TaintServingTainted
				} else {
					verdict.Outcome = TaintServingPending
				}
				verdict.Units = meter.units
				return nil
			}

			parents, err := s.memoryLineageTxn(txn, current)
			if err != nil {
				return err
			}
			if err := meter.charge(1 + len(parents)*2); err != nil {
				return err
			}
			if len(parents) == 0 {
				if !provenance.Exact {
					verdict.CoverageComplete = false
					verdict.LineageMissing = true
				}
			}
			for _, parent := range parents {
				if _, seen := visited[parent.ref.MemoryID]; seen {
					continue
				}
				queue = append(queue, parent.ref.MemoryID)
			}
		}
		verdict.Units = meter.units
		return nil
	})
	return verdict, err
}

func recordTaintExhaustion(verdict *TaintServingVerdict, status taint.Status) {
	switch status {
	case taint.StatusDepthExhausted:
		verdict.DepthExhausted = true
	case taint.StatusWorkExhausted:
		verdict.WorkExhausted = true
	}
}

// TaintServingCoverageComplete reports whether every active decision has
// completed authenticated-graph expansion. A false result means the SQL taint
// mirror cannot be used for aggregate exclusion or completeness claims because
// pending descendants may not have materialized memberships yet.
func (s *BadgerStore) TaintServingCoverageComplete() (bool, error) {
	complete := true
	err := s.view(func(txn *badger.Txn) error {
		activeScopes, err := readActiveTaintDecisionScopesTxn(txn, nil)
		if err != nil {
			return err
		}
		for _, active := range activeScopes {
			if active.Header.Status.Incomplete() || active.Scope.Kind != tx.TaintScopeDirect {
				complete = false
				return nil
			}
		}
		return nil
	})
	return complete, err
}
