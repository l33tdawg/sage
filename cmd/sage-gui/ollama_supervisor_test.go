package main

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

type fakeManagedOllama struct {
	mu      sync.Mutex
	healthy bool
	starts  chan struct{}
}

func (f *fakeManagedOllama) Start(context.Context) (string, error) {
	f.mu.Lock()
	f.healthy = true
	f.mu.Unlock()
	f.starts <- struct{}{}
	return "http://127.0.0.1:11434", nil
}

func (f *fakeManagedOllama) Probe(context.Context) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.healthy
}

func (f *fakeManagedOllama) crash() {
	f.mu.Lock()
	f.healthy = false
	f.mu.Unlock()
}

func TestSuperviseManagedOllamaRestartsAfterCrash(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	runtime := &fakeManagedOllama{starts: make(chan struct{}, 2)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		superviseManagedOllama(ctx, runtime, 5*time.Millisecond, zerolog.Nop())
	}()

	select {
	case <-runtime.starts: // initial adopt/start
	case <-time.After(time.Second):
		t.Fatal("managed Ollama was not started")
	}

	runtime.crash()
	select {
	case <-runtime.starts: // supervisor recovery
	case <-time.After(time.Second):
		t.Fatal("managed Ollama was not restarted after becoming unhealthy")
	}

	cancel()
	require.Eventually(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, time.Second, 5*time.Millisecond)
}
