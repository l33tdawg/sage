//go:build !windows

package main

import (
	"fmt"
	"os"
	"path/filepath"
)

func replaceConfigFileDurably(from, to string) error {
	if err := os.Rename(from, to); err != nil {
		return fmt.Errorf("replace config file: %w", err)
	}
	dir, err := os.Open(filepath.Dir(to))
	if err != nil {
		return fmt.Errorf("open config parent directory for sync: %w", err)
	}
	if err := dir.Sync(); err != nil {
		_ = dir.Close()
		return fmt.Errorf("sync config parent directory: %w", err)
	}
	if err := dir.Close(); err != nil {
		return fmt.Errorf("close config parent directory after sync: %w", err)
	}
	return nil
}
