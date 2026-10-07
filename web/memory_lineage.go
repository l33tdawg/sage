package web

import (
	"context"

	"github.com/l33tdawg/sage/internal/store"
)

func (h *DashboardHandler) findMemoryParent(ctx context.Context, pointer string) (*store.MemoryParent, error) {
	if lineage, ok := h.store.(store.MemoryLineageStore); ok {
		return lineage.FindMemoryParent(ctx, pointer)
	}
	// Third-party stores without hash lookup retain their existing exact-ID
	// behavior. Never scan their full memory inventory to manufacture lineage.
	return &store.MemoryParent{MemoryID: pointer}, nil
}
