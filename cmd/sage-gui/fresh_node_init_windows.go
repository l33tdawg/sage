//go:build windows

package main

import (
	"errors"
	"os"
	"path/filepath"
)

// The initializer requires the fd-relative no-follow and flock implementation.
// Do not silently fall back to a weaker publication route on another platform.
func runInitFreshNode(_ []string) error {
	return errors.New("fresh-node initialization is not supported on Windows")
}
func preflightFreshNodeInitialization(home string, _ *Config) error {
	for _, name := range []string{".fresh-node-init-v1.claim", ".fresh-node-init-v1.ready"} {
		if _, err := os.Lstat(filepath.Join(home, name)); !os.IsNotExist(err) {
			return errors.New("fresh-node receipt validation is not supported on Windows")
		}
	}
	return nil
}
