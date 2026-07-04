//go:build live

// Live end-to-end exercise of the managed reranker: real GitHub release
// download, real llama-server spawn, real cross-encoder scoring. Run by hand:
//
//	go test -tags live -run TestLiveE2E -v ./internal/rerankd/
//
// Not part of CI (network + 636MB model). This file is intentionally
// uncommitted scaffolding.
package rerankd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLiveE2E(t *testing.T) {
	home, err := os.UserHomeDir()
	require.NoError(t, err)
	m := New(filepath.Join(home, ".sage"))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	require.NoError(t, m.InstallEngine(ctx, func(done, total int64) {
		if done == total {
			fmt.Printf("engine: %d/%d bytes\n", done, total)
		}
	}))
	bin, ok := m.BinaryPath()
	require.True(t, ok)
	fmt.Println("engine binary:", bin)

	require.True(t, m.ModelReady(), "model expected in place (prefetched)")

	url, err := m.Start(ctx)
	require.NoError(t, err)
	fmt.Println("sidecar up at:", url)

	rr := embedding.NewHTTPRerankerKind(url, ModelDisplayName, embedding.RerankKindLlamaCpp, 30*time.Second)
	out, err := rr.Rerank(ctx, "what is the capital of France?", []string{
		"Paris is the capital and largest city of France.",
		"Bananas are rich in potassium.",
	})
	require.NoError(t, err)
	require.Len(t, out, 2)
	scores := map[int]float64{}
	for _, r := range out {
		scores[r.Index] = r.Score
		fmt.Printf("doc %d score %.4f\n", r.Index, r.Score)
	}
	assert.Greater(t, scores[0], scores[1], "relevant doc must outscore the banana")

	require.NoError(t, m.Stop())
	fmt.Println("sidecar stopped clean")
}
