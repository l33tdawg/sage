package web

import (
	"context"

	"github.com/l33tdawg/sage/internal/store"
)

// appV30TaskTaintFilterEnabled permits the SQL task/notice prefilter only when
// the mirror revision exactly matches canonical Badger state. A stale or broken
// mirror falls back to the authoritative per-record classifier.
func (h *DashboardHandler) appV30TaskTaintFilterEnabled(ctx context.Context) bool {
	if h == nil || h.BadgerStore == nil || h.store == nil {
		return false
	}
	mirror, ok := h.store.(store.MemoryTaintMirrorStore)
	if !ok {
		return false
	}
	fresh, _, err := store.MemoryTaintMirrorFresh(ctx, h.BadgerStore, mirror)
	return err == nil && fresh
}
