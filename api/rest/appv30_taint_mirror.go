package rest

import (
	"context"

	"github.com/l33tdawg/sage/internal/store"
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
