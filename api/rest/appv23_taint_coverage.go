package rest

import (
	"sync/atomic"

	"github.com/l33tdawg/sage/internal/store"
)

// appV23ReadScope has no per-record metadata. Uncertainty is monotonic, including
// parallel hybrid filters and the final live-authorization recheck.
type appV23ReadScope struct {
	allowPartial bool
	taint        *store.TaintServingReadScope
	uncertain    atomic.Bool
}

func newAppV23ReadScope() *appV23ReadScope {
	return &appV23ReadScope{taint: store.NewTaintServingReadScope(), allowPartial: true}
}

// Strict batch readers share the same cold-header allowance but retain their
// explicit error contract because their response cannot represent a partial set.
func newAppV23StrictReadScope() *appV23ReadScope {
	return &appV23ReadScope{taint: store.NewTaintServingReadScope()}
}

type taintCoverage struct {
	Complete  bool   `json:"complete"`
	Truncated bool   `json:"truncated"`
	Reason    string `json:"reason"`
}

func (scope *appV23ReadScope) coverage() *taintCoverage {
	if scope == nil || !scope.uncertain.Load() {
		return nil
	}
	return &taintCoverage{Complete: false, Truncated: true, Reason: "ancestor_walk_budget"}
}

func (scope *appV23ReadScope) applyRecallCoverage(response *QueryMemoryResponse) {
	response.TaintCoverage = scope.coverage()
	if response.TaintCoverage != nil && len(response.Results) == 0 {
		// A separate index probe cannot establish absence after a serving
		// candidate was withheld because its taint state could not be decided.
		response.IndexStatus = "unavailable"
	}
}
