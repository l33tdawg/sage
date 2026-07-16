//go:build !v119testfixture

package store

func runAtomicStateWriteFaultHook(AtomicStateWriteStage) error { return nil }
