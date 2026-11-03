package main

import (
	"errors"
	"fmt"
	"sync"
)

// serveStoreCleanup follows runServe's consensus owner from the raw store,
// through the initial app, to the boot runtime's current application bundle.
// Ownership changes only during startup, before deferred cleanup can begin.
type serveStoreCleanup struct {
	closeConsensus func() error
	closeSQLite    func() error
	consensusOnce  sync.Once
	consensusErr   error
	projectionOnce sync.Once
	projectionErr  error
}

func (c *serveStoreCleanup) closeConsensusState() error {
	c.consensusOnce.Do(func() {
		// If cleanup panics, earlier defers must still fail closed instead of
		// treating sync.Once's completed state as a successful worker join.
		c.consensusErr = errors.New("consensus cleanup did not finish; stores remain open")
		if c.closeConsensus != nil {
			c.consensusErr = c.closeConsensus()
		} else {
			c.consensusErr = nil
		}
	})
	return c.consensusErr
}

func (c *serveStoreCleanup) close() error {
	if err := c.closeConsensusState(); err != nil {
		// In particular, a failed worker join leaves both stores live. Never
		// turn another deferred close into an implicit retry or bypass.
		return fmt.Errorf("close consensus storage; projection left open: %w", err)
	}
	c.projectionOnce.Do(func() {
		if c.closeSQLite != nil {
			c.projectionErr = c.closeSQLite()
		}
	})
	if c.projectionErr != nil {
		return fmt.Errorf("close SQLite projection: %w", c.projectionErr)
	}
	return nil
}

func serveStoreCleanupResult(serveErr, closeErr error) error {
	if closeErr == nil {
		return serveErr
	}
	if errors.Is(serveErr, errCoordinatedRestart) {
		// main authorizes exec with errors.Is. Preserve the requested action
		// as text, but do not wrap its sentinel after storage cleanup failed.
		return fmt.Errorf("coordinated restart canceled (%v): %w", serveErr, closeErr)
	}
	return errors.Join(serveErr, closeErr)
}
