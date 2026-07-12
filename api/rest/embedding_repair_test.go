package rest

import (
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
)

func TestRepairEmbeddingsOnceHealsVectorlessRowsIntoActiveSpace(t *testing.T) {
	ctx := context.Background()
	s, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "repair.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })
	hash := sha256.Sum256([]byte("memory committed while Ollama was down"))
	require.NoError(t, s.InsertMemory(ctx, &memory.MemoryRecord{
		MemoryID:        "mem-1",
		SubmittingAgent: "agent-1",
		Content:         "memory committed while Ollama was down",
		ContentHash:     hash[:],
		MemoryType:      memory.TypeObservation,
		DomainTag:       "test",
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now(),
	}))

	embedder := authoritativeTestEmbedder{vector: []float32{0.1, 0.2, 0.3}, name: "ollama"}
	srv := NewServer("", s, s, nil, metrics.NewHealthChecker(), zerolog.Nop(), embedder)
	repaired, err := srv.RepairEmbeddingsOnce(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, repaired)
	counts, err := s.CountMemoriesByProvider(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, counts["ollama:3"])
	require.Zero(t, counts[""])
}
