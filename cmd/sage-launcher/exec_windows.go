//go:build windows

package main

import (
	"os"
	"os/exec"
)

// defaultExecer is the production Execer. Windows has no real
// exec(2) — the closest equivalent is to spawn the new process,
// hand off our handles, and exit with the child's exit code.
// This sacrifices the same-PID guarantee Unix gives us, but
// supervised SAGE on Windows is a rarer deployment shape and we
// can revisit if anyone needs strict PID continuity there.
type defaultExecer struct{}

func (defaultExecer) Exec(argv0 string, argv []string, envv []string) error {
	cmd := exec.Command(argv0, argv[1:]...) //nolint:gosec,noctx // launcher-owned argv from manifest
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.Env = envv
	if err := cmd.Run(); err != nil {
		if exitErr, ok := err.(*exec.ExitError); ok {
			os.Exit(exitErr.ExitCode())
		}
		return err
	}
	os.Exit(0)
	return nil // unreachable
}
