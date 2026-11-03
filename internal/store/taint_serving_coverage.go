package store

import (
	"context"

	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// TaintServingCoverageSnapshot binds the revision and bounded header proof to
// one canonical snapshot. None means no INCOMPLETE headers, not no active
// decisions: a completed direct decision is compatible with mirror exclusion.
// Any active non-direct scope makes membership-only mirror exclusion incomplete.
// Unknown/capped/request-exhausted proofs are unavailable, never complete.
func (s *BadgerStore) TaintServingCoverageSnapshot(
	ctx context.Context,
	scope *TaintServingReadScope,
) (revision uint64, complete bool, err error) {
	if scope == nil {
		scope = NewTaintServingReadScope()
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return 0, false, contextErr
	}
	err = s.withTaintServingReadSnapshot(func(reader *BadgerStore) error {
		var readErr error
		revision, readErr = readTaintUint64Txn(reader.txn, taint.RevisionKey())
		if readErr != nil {
			return readErr
		}
		status, readErr := reader.incompleteTaintStatus(scope)
		if readErr != nil {
			return readErr
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		if status != taintIncompleteNone {
			// An already unavailable proof needs no scope inventory. In
			// particular optional SQL acceleration cannot consume an exhausted
			// request allowance merely to return complete=false.
			return nil
		}
		meter := newTaintUnitMeter(taint.MaxServingRequestUnits, ErrTaintServingWalkBudget)
		meter.request = scope
		activeScopes, scopeErr := readActiveTaintDecisionScopesTxn(reader.txn, meter)
		if scopeErr != nil {
			return scopeErr
		}
		for _, active := range activeScopes {
			if active.Scope.Kind != tx.TaintScopeDirect {
				// Expansion completeness does not materialize every future author
				// match. SQL membership exclusion cannot claim a complete index.
				return nil
			}
		}
		complete = status == taintIncompleteNone
		return nil
	})
	if err != nil {
		return revision, false, err
	}
	return revision, complete, nil
}
