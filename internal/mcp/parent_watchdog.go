package mcp

import (
	"context"
	"sync"
	"time"
)

const (
	mcpParentPollInterval = time.Second
	mcpParentExitGrace    = 2 * time.Second
)

// An observer must report exit only when it is certain that the captured
// parent is gone. A failed liveness query is not evidence of parent exit.
type mcpParentProcess interface {
	exited() bool
	close()
}

// startMCPParentWatch owns the stdio bridge's process lifetime. Inherited pipe
// writers can keep stdin open after the client exits, so EOF is not a reliable
// disconnect signal. Cancel first to stop tools and host subscriptions, then
// give normal cleanup a bounded opportunity to finish.
//
// Closing an inherited blocking os.File does not necessarily interrupt an
// active read or write. The process-only fallback also covers a blocked stdout
// pump or an executable handoff waiting for its stdin copier. It is armed only
// after proven parent exit, never for an idle client or context cancellation.
// The returned stop function must run AFTER every other Run cleanup operation;
// a stuck cleanup must not disable the fallback.
func startMCPParentWatch(
	parent mcpParentProcess,
	cancel context.CancelFunc,
	interrupt func(),
	exit func(int),
	interval time.Duration,
	grace time.Duration,
) func() {
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer parent.close()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			if parent.exited() {
				// Arm the deadline before touching stdio; even an inherited file's
				// Close or a diagnostic write can block during disconnect.
				deadline := time.AfterFunc(grace, func() {
					select {
					case <-stop:
						return
					default:
						exit(0)
					}
				})
				defer deadline.Stop()
				cancel()
				go interrupt()
				<-stop
				return
			}
			select {
			case <-stop:
				return
			case <-ticker.C:
			}
		}
	}()
	var once sync.Once
	return func() {
		once.Do(func() { close(stop) })
		<-done
	}
}
