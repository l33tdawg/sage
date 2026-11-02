package store

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"

	"github.com/l33tdawg/sage/internal/taint"
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
	Outcome     TaintServingOutcome
	DecisionIDs []string
	Nodes       int
	Units       int
}

// ClassifyMemoryTaint checks direct memberships and the authenticated ancestor
// path in one Badger snapshot. It never treats an absent materialized
// membership as proof that an incomplete ancestor decision is independent.
func (s *BadgerStore) ClassifyMemoryTaint(memoryID string) (TaintServingVerdict, error) {
	var verdict TaintServingVerdict
	if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
		return verdict, fmt.Errorf("%w: invalid memory ID", ErrTaintServingState)
	}
	err := s.view(func(txn *badger.Txn) error {
		reader := &BadgerStore{db: s.db, txn: txn}
		queue := []string{memoryID}
		visited := make(map[string]struct{}, taint.MaxServingAncestorWalk)
		headers := make(map[string]taint.Header)
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
			verdict.Units++

			decisions, err := reader.GetEffectiveTaintDecisions(current)
			if err != nil {
				return err
			}
			verdict.Units += len(decisions) + 1
			if len(decisions) > 0 {
				pendingOnly := current != memoryID
				for _, decisionID := range decisions {
					header, cached := headers[decisionID]
					if !cached {
						var found bool
						header, found, err = readTaintHeaderTxn(txn, decisionID)
						if err != nil {
							return err
						}
						if !found {
							return fmt.Errorf("%w: missing decision header", ErrTaintServingState)
						}
						headers[decisionID] = header
						verdict.Units++
					}
					if !header.Status.Incomplete() {
						pendingOnly = false
					}
				}
				verdict.DecisionIDs = append([]string(nil), decisions...)
				if pendingOnly {
					verdict.Outcome = TaintServingPending
				} else {
					verdict.Outcome = TaintServingTainted
				}
				return nil
			}

			parents, err := s.memoryLineageTxn(txn, current)
			if err != nil {
				return err
			}
			verdict.Units += len(parents)
			for _, parent := range parents {
				if _, seen := visited[parent.ref.MemoryID]; seen {
					continue
				}
				queue = append(queue, parent.ref.MemoryID)
			}
		}
		return nil
	})
	return verdict, err
}
