package store

import (
	"errors"
	"fmt"

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

	// Optional, bounded metadata about the stored, hash-bound ancestor graph.
	// Complete requires every observed branch to close at an exact-provenance
	// root, no effective incomplete decision, and no error/cap/cycle. It says
	// nothing about omitted derivations or global provenance completeness.
	// False Complete + false Missing is unproved, not a positive absence proof.
	CoverageComplete bool
	// Missing is positive evidence: a checked leaf had no provenance record.
	// Optional read/decoder errors and exhausted metadata work do not set it.
	LineageMissing bool
	// These report observed effective decision statuses, never serving caps.
	DepthExhausted bool
	WorkExhausted  bool
}

// ClassifyMemoryTaint checks direct memberships and the authenticated ancestor
// path in one Badger snapshot. It never treats an absent materialized
// membership as proof that an incomplete ancestor decision is independent.
func (s *BadgerStore) ClassifyMemoryTaint(memoryID string) (TaintServingVerdict, error) {
	return s.classifyMemoryTaintForRead(memoryID, NewTaintServingReadScope())
}

func (s *BadgerStore) classifyMemoryTaintForRead(memoryID string, scope *TaintServingReadScope) (TaintServingVerdict, error) {
	if scope == nil {
		scope = NewTaintServingReadScope()
	}
	var verdict TaintServingVerdict
	if len(memoryID) == 0 || len(memoryID) > taint.MaxMemoryIDBytes {
		return verdict, fmt.Errorf("%w: invalid memory ID", ErrTaintServingState)
	}
	err := s.withTaintServingReadSnapshot(func(reader *BadgerStore) (classificationErr error) {
		// Discovery is optional: it shares this exact reader and request scope,
		// but spends only its separate metadata allowance. Its uncertainty must
		// never change the mandatory disposition, errors, Nodes or Units.
		defer func() {
			if classificationErr != nil {
				verdict.CoverageComplete = false
				return
			}
			coverage := reader.discoverTaintCoverage(memoryID, scope)
			verdict.CoverageComplete = coverage.complete
			verdict.LineageMissing = coverage.missing
			verdict.DepthExhausted = verdict.DepthExhausted || coverage.depth
			verdict.WorkExhausted = verdict.WorkExhausted || coverage.work
		}()
		txn := reader.txn
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
					verdict.DepthExhausted = verdict.DepthExhausted || header.Status == taint.StatusDepthExhausted
					verdict.WorkExhausted = verdict.WorkExhausted || header.Status == taint.StatusWorkExhausted
				}
				verdict.DecisionIDs = append([]string(nil), decisions...)
				if pendingOnly {
					verdict.Outcome = TaintServingPending
				} else {
					verdict.Outcome = TaintServingTainted
				}
				return nil
			}

			if current == memoryID {
				status, headerErr := reader.incompleteTaintStatus(scope)
				if headerErr != nil {
					return headerErr
				}
				if status == taintIncompleteNone {
					return nil
				}
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
