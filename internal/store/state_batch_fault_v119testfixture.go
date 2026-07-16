//go:build v119testfixture

package store

import "sync"

var atomicStateWriteFaultHook struct {
	sync.RWMutex
	fn func(AtomicStateWriteStage) error
}

// SetAtomicStateWriteFaultHookForTest installs the tagged v11.9 crash hook and
// returns a restoration closure. It is absent from production builds.
func SetAtomicStateWriteFaultHookForTest(hook func(AtomicStateWriteStage) error) func() {
	atomicStateWriteFaultHook.Lock()
	previous := atomicStateWriteFaultHook.fn
	atomicStateWriteFaultHook.fn = hook
	atomicStateWriteFaultHook.Unlock()
	return func() {
		atomicStateWriteFaultHook.Lock()
		atomicStateWriteFaultHook.fn = previous
		atomicStateWriteFaultHook.Unlock()
	}
}

func runAtomicStateWriteFaultHook(stage AtomicStateWriteStage) error {
	atomicStateWriteFaultHook.RLock()
	hook := atomicStateWriteFaultHook.fn
	atomicStateWriteFaultHook.RUnlock()
	if hook == nil {
		return nil
	}
	return hook(stage)
}
