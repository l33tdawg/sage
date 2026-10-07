package web

import (
	"context"
	"testing"
	"time"
)

// The handler spawns unowned goroutines for background work when no lifecycle owner is injected
// (embedded and test use). A caller that owns the stores has to be able to drain them before closing,
// or a background audit touches a closed Badger DB and panics the whole process — which is what this
// seam exists to prevent (observed reddening the suite from another test's teardown, 2026-09-25).
func TestWaitBackgroundDrainsUnownedWork(t *testing.T) {
	h := &DashboardHandler{}
	released := make(chan struct{})
	finished := make(chan struct{})
	h.runBackground(func(context.Context) {
		<-released
		close(finished)
	})

	waited := make(chan struct{})
	go func() {
		h.WaitBackground()
		close(waited)
	}()

	select {
	case <-waited:
		t.Fatal("WaitBackground returned while an unowned background goroutine was still running")
	case <-time.After(50 * time.Millisecond):
	}

	close(released)
	select {
	case <-waited:
	case <-time.After(2 * time.Second):
		t.Fatal("WaitBackground did not return after the background goroutine finished")
	}
	select {
	case <-finished:
	default:
		t.Fatal("the background function never ran to completion")
	}
}

// An injected owner decides how background work runs; nothing is outstanding on the handler, so the
// drain must not block (and must not claim work that is not its own).
func TestAnInjectedOwnerKeepsTheDrainInert(t *testing.T) {
	var ran bool
	h := &DashboardHandler{RunBackground: func(fn func(context.Context)) { ran = true; fn(context.Background()) }}
	h.runBackground(func(context.Context) {})
	h.WaitBackground()
	if !ran {
		t.Fatal("the injected owner was bypassed")
	}
}
