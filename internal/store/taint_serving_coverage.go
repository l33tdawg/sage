package store

import (
	"context"

	"github.com/l33tdawg/sage/internal/taint"
)

// TaintServingCoverageSnapshot binds the revision and bounded header proof to
// one canonical snapshot. None means no INCOMPLETE headers, not no active
// decisions: a completed active decision is compatible with mirror exclusion.
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
		complete = status == taintIncompleteNone
		return nil
	})
	if err != nil {
		return revision, false, err
	}
	return revision, complete, nil
}
