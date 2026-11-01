package rest

import (
	"context"
	"errors"
	"net/http"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	memoryTaintedProblemType        = "https://sage.dev/errors/memory-tainted"
	memoryTaintPendingProblemType   = "https://sage.dev/errors/memory-taint-pending"
	memoryTaintedProblemTitle       = "Memory withheld by governance taint"
	memoryTaintPendingProblemTitle  = "Memory taint coverage is pending"
	memoryTaintedProblemDetail      = "An active governance decision withholds this memory."
	memoryTaintPendingProblemDetail = "Governance taint coverage is still being determined for this memory."
)

// memoryTaintFilterEnabled opts SQL into the taint prefilter only while its
// revision exactly matches canonical Badger state. A stale mirror must fall
// back to the authoritative per-record classifier.
func (s *Server) memoryTaintFilterEnabled(ctx context.Context) bool {
	if s == nil || s.badgerStore == nil || s.store == nil {
		return false
	}
	mirror, ok := s.store.(store.MemoryTaintMirrorStore)
	if !ok {
		return false
	}
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, s.badgerStore, mirror)
	return err == nil && fresh
}

func appV30TaintProjectionProblem(err error) (problemType, title, detail string, ok bool) {
	switch {
	case errors.Is(err, store.ErrMemoryProjectionTainted):
		return memoryTaintedProblemType,
			memoryTaintedProblemTitle,
			memoryTaintedProblemDetail,
			true
	case errors.Is(err, store.ErrMemoryProjectionTaintPending):
		return memoryTaintPendingProblemType,
			memoryTaintPendingProblemTitle,
			memoryTaintPendingProblemDetail,
			true
	default:
		return "", "", "", false
	}
}

func writeAppV30TaintProjectionProblem(w http.ResponseWriter, err error) bool {
	problemType, title, detail, ok := appV30TaintProjectionProblem(err)
	if !ok {
		return false
	}
	writeProblemTyped(
		w,
		http.StatusUnavailableForLegalReasons,
		problemType,
		title,
		detail,
	)
	return true
}
