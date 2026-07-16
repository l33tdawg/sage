//go:build v119testfixture

package abci

import "sync"

var appV20CommitBoundaryTestHook struct {
	sync.RWMutex
	fn func(AppV20CommitBoundaryStage)
}

// SetAppV20CommitBoundaryHookForTest installs the tagged v11.9 crash hook and
// returns an idempotent-style restore closure. The callback runs outside the
// mutex so a child fixture can block until its parent delivers SIGKILL.
func SetAppV20CommitBoundaryHookForTest(hook func(AppV20CommitBoundaryStage)) func() {
	appV20CommitBoundaryTestHook.Lock()
	previous := appV20CommitBoundaryTestHook.fn
	appV20CommitBoundaryTestHook.fn = hook
	appV20CommitBoundaryTestHook.Unlock()
	return func() {
		appV20CommitBoundaryTestHook.Lock()
		appV20CommitBoundaryTestHook.fn = previous
		appV20CommitBoundaryTestHook.Unlock()
	}
}

func runAppV20CommitBoundaryHook(stage AppV20CommitBoundaryStage) {
	appV20CommitBoundaryTestHook.RLock()
	hook := appV20CommitBoundaryTestHook.fn
	appV20CommitBoundaryTestHook.RUnlock()
	if hook != nil {
		hook(stage)
	}
}
