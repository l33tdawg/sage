package main

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/metrics"
)

type recoveringEmbedder struct{ probes atomic.Int32 }

func (p *recoveringEmbedder) Embed(context.Context, string) ([]float32, error) {
	return []float32{1, 0, 0}, nil
}
func (p *recoveringEmbedder) Dimension() int { return 3 }
func (p *recoveringEmbedder) Ready() bool    { return false }
func (p *recoveringEmbedder) Semantic() bool { return true }
func (p *recoveringEmbedder) Name() string   { return "recovering" }
func (p *recoveringEmbedder) Ping(context.Context) error {
	if p.probes.Add(1) == 1 {
		return fmt.Errorf("not ready yet")
	}
	return nil
}

func TestEmbedderWatchdogSignalsWhenProviderRecovers(t *testing.T) {
	oldInterval := embedderWatchdogInterval
	embedderWatchdogInterval = 10 * time.Millisecond
	t.Cleanup(func() { embedderWatchdogInterval = oldInterval })

	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan struct{}, 1)
	provider := &recoveringEmbedder{}
	done := startEmbedderWatchdog(ctx, provider, metrics.NewHealthChecker(), zerolog.Nop(), func() {
		select {
		case ready <- struct{}{}:
		default:
		}
	})

	select {
	case <-ready:
	case <-time.After(time.Second):
		t.Fatal("healthy transition did not signal automatic repair")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("watchdog did not stop with node context")
	}
	require.GreaterOrEqual(t, provider.probes.Load(), int32(2))
}
