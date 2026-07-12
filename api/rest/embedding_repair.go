package rest

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/embedding"
)

const embeddingRepairInterval = 30 * time.Second

// StartEmbeddingRepair continuously repairs vectorless or stale-space rows for
// REST-only deployments such as amid. CEREBRUM has its own lifecycle-bound job;
// amid otherwise has no dashboard process to heal an observation committed
// while its embedder was temporarily unavailable.
func (s *Server) StartEmbeddingRepair(ctx context.Context) {
	go func() {
		run := func() {
			repaired, err := s.RepairEmbeddingsOnce(ctx)
			if err != nil {
				s.logger.Warn().Err(err).Msg("embedding repair deferred")
			} else if repaired > 0 {
				s.logger.Info().Int("repaired", repaired).Str("vector_space", embedding.SpaceID(s.embedder)).Msg("embedding repair completed")
			}
		}
		run()
		ticker := time.NewTicker(embeddingRepairInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				run()
			}
		}
	}()
}

// RepairEmbeddingsOnce performs one convergent pass into the server's active
// vector space. Failed rows are marked for retry on the next pass; unreadable or
// empty rows are skipped. Embeddings are off-consensus search-index data.
func (s *Server) RepairEmbeddingsOnce(ctx context.Context) (int, error) {
	if s.embedder == nil {
		return 0, nil
	}
	if pinger, ok := s.embedder.(embedding.Pinger); ok {
		if err := pinger.Ping(ctx); err != nil {
			return 0, fmt.Errorf("active embedder unavailable: %w", err)
		}
	} else if !s.embedder.Ready() {
		return 0, fmt.Errorf("active embedder is not ready")
	}
	target := embedding.SpaceID(s.embedder)
	if target == "" {
		return 0, fmt.Errorf("active vector space is unknown")
	}
	if _, err := s.store.ResetErroredEmbeddings(ctx); err != nil {
		return 0, fmt.Errorf("reset errored embeddings: %w", err)
	}
	repaired := 0
	for {
		items, err := s.store.ListMemoriesForReembed(ctx, target, 100)
		if err != nil {
			return repaired, fmt.Errorf("list memories for re-embed: %w", err)
		}
		if len(items) == 0 {
			return repaired, nil
		}
		for _, item := range items {
			if !item.Decryptable || strings.TrimSpace(item.Content) == "" {
				if err := s.store.MarkMemoryEmbeddingSkipped(ctx, item.MemoryID); err != nil {
					return repaired, fmt.Errorf("mark memory %s skipped: %w", item.MemoryID, err)
				}
				continue
			}
			vector, err := s.embedder.Embed(ctx, item.Content)
			if err != nil || len(vector) == 0 {
				if markErr := s.store.MarkMemoryEmbeddingError(ctx, item.MemoryID); markErr != nil {
					return repaired, fmt.Errorf("mark memory %s errored: %w", item.MemoryID, markErr)
				}
				continue
			}
			if err := s.store.UpdateMemoryEmbedding(ctx, item.MemoryID, vector, target); err != nil {
				return repaired, fmt.Errorf("update memory %s embedding: %w", item.MemoryID, err)
			}
			repaired++
		}
	}
}
