package store

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"

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
	Outcome     TaintServingOutcome
	DecisionIDs []string
	Nodes       int
	Units       int

	// Optional, bounded metadata about the stored, hash-bound ancestor graph.
	// Complete requires every observed branch to close at an exact-provenance
	// root, no effective incomplete membership or scope, and no error/cap/cycle. It says
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
	return s.classifyMemoryTaintWithIdentityForRead(memoryID, scope, taintReadIdentityPolicy{})
}

func (s *BadgerStore) classifyMemoryTaintWithIdentityForRead(memoryID string, scope *TaintServingReadScope, identity taintReadIdentityPolicy) (TaintServingVerdict, error) {
	if scope == nil {
		scope = NewTaintServingReadScope()
	}
	var verdict TaintServingVerdict
	if identity.validate(memoryID) != nil {
		return verdict, fmt.Errorf("%w: invalid memory ID", ErrTaintServingState)
	}
	err := s.withTaintServingReadSnapshot(func(reader *BadgerStore) (classificationErr error) {
		var nonDirectScopes []activeTaintDecisionScope
		// Discovery is optional: it shares this exact reader and request scope,
		// but spends only its separate metadata allowance. Its uncertainty must
		// never change the mandatory disposition, errors, Nodes or Units.
		defer func() {
			if classificationErr != nil {
				verdict.CoverageComplete = false
				return
			}
			coverage := reader.discoverTaintCoverageWithIdentity(memoryID, scope, nonDirectScopes, identity)
			verdict.CoverageComplete = coverage.complete
			verdict.LineageMissing = coverage.missing
			verdict.DepthExhausted = verdict.DepthExhausted || coverage.depth
			verdict.WorkExhausted = verdict.WorkExhausted || coverage.work
		}()
		txn := reader.txn
		meter := newTaintUnitMeter(taint.MaxServingRequestUnits, ErrTaintServingWalkBudget)
		meter.request = scope
		defer func() { verdict.Units = meter.units }()
		if err := identity.validatePhysicalRootTxn(txn, meter); err != nil {
			return err
		}
		activeScopes, err := readActiveTaintDecisionScopesTxn(txn, meter)
		if err != nil {
			return err
		}
		nonDirectScopes = make([]activeTaintDecisionScope, 0, len(activeScopes))
		for _, active := range activeScopes {
			if active.Scope.Kind != tx.TaintScopeDirect {
				nonDirectScopes = append(nonDirectScopes, active)
			}
		}
		queue := []taintCoverageNode{{id: memoryID}}
		visited := make(map[string]struct{}, taint.MaxServingAncestorWalk)
		headers := make(map[string]taint.Header)
		for len(queue) > 0 {
			node := queue[0]
			current := node.id
			queue = queue[1:]
			if _, seen := visited[current]; seen {
				continue
			}
			if len(visited) >= taint.MaxServingAncestorWalk {
				return ErrTaintServingWalkBudget
			}
			visited[current] = struct{}{}
			verdict.Nodes++
			if err := meter.charge(1); err != nil {
				return err
			}
			if node.bound {
				raw, found, err := readBoundedMeteredTaintValueTxn(txn, memoryKey(current), 4+sha256.Size+taint.MaxCursorBytes, meter)
				if err != nil {
					return err
				}
				if !found {
					return fmt.Errorf("%w: missing canonical ancestor", ErrMemoryLineageMalformed)
				}
				hash, _, err := decodeMemoryHashEntry(raw)
				if err != nil || !bytes.Equal(hash, node.expected[:]) {
					return fmt.Errorf("%w: canonical ancestor hash mismatch", ErrMemoryLineageMalformed)
				}
			}

			decisions, currentHeaders, err := getEffectiveTaintDecisionsWithIdentityTxn(txn, current, meter, identity)
			if err != nil {
				return err
			}
			for id, header := range currentHeaders {
				headers[id] = header
			}
			scopeMatched := false
			if len(nonDirectScopes) > 0 {
				provenance, provenanceErr := authorProvenanceWithIdentityTxn(txn, current, meter, identity)
				if provenanceErr != nil {
					return provenanceErr
				}
				for _, active := range nonDirectScopes {
					if active.Scope.Matches(provenance) {
						scopeMatched = true
						decisions = appendDecisionIDUnique(decisions, active.DecisionID)
						headers[active.DecisionID] = active.Header
					}
				}
			}
			decisions = sortedUniqueDecisionIDs(decisions)
			if len(decisions) > 0 {
				pendingOnly := current != memoryID && !scopeMatched
				for _, decisionID := range decisions {
					header := headers[decisionID]
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
				if status == taintIncompleteNone && len(nonDirectScopes) == 0 {
					return nil
				}
			}

			parents, err := servingScopeParentsWithIdentityTxn(txn, current, meter, identity)
			if err != nil {
				return err
			}
			for _, parent := range parents {
				if _, seen := visited[parent.MemoryID]; seen {
					continue
				}
				queue = append(queue, taintCoverageNode{id: parent.MemoryID, expected: parent.ContentHash, bound: true})
			}
		}
		return nil
	})
	return verdict, err
}
