//go:build !windows

package mcp

import (
	"fmt"
	"os"
)

func syncClaimantIdentityDirectory(dir string) error {
	handle, err := os.Open(dir) //nolint:gosec // private claimant directory under SAGE_HOME
	if err != nil {
		return fmt.Errorf("open claimant identity directory for sync: %w", err)
	}
	if err := handle.Sync(); err != nil {
		_ = handle.Close()
		return fmt.Errorf("sync claimant identity directory: %w", err)
	}
	if err := handle.Close(); err != nil {
		return fmt.Errorf("close claimant identity directory: %w", err)
	}
	return nil
}
