//go:build !windows

package store

import (
	"fmt"
	"os"
	"path/filepath"
)

func replaceIndexBackfillSidecarDurably(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("replace index backfill sidecar: %w", err)
	}
	parent, err := os.Open(filepath.Dir(to)) //nolint:gosec // validated Badger directory
	if err != nil {
		return fmt.Errorf("open index backfill sidecar parent: %w", err)
	}
	if err := parent.Sync(); err != nil {
		_ = parent.Close()
		return fmt.Errorf("sync index backfill sidecar parent: %w", err)
	}
	if err := parent.Close(); err != nil {
		return fmt.Errorf("close index backfill sidecar parent: %w", err)
	}
	return nil
}
