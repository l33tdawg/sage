package main

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// Run the actual entry point in a child process so a regression cannot start
// the live node, hang the suite, or terminate the parent test process.
func TestCommandHelpProcess(t *testing.T) {
	if os.Getenv("SAGE_TEST_CLI_ENTRY") != "1" {
		return
	}
	for i, arg := range os.Args {
		if arg == "--" {
			os.Args = append([]string{"sage-gui"}, os.Args[i+1:]...)
			break
		}
	}
	main()
	os.Exit(0)
}

func TestSubcommandHelpHasNoSideEffects(t *testing.T) {
	for _, args := range [][]string{
		{"serve", "--help"}, {"setup", "-h"}, {"mcp", "--help"},
		{"mcp", "install", "--help"}, {"codex", "install", "-h"},
		{"seed", "missing.json", "--help"}, {"export", "--help"},
		{"import", "missing.vault", "--help"}, {"backup", "--full", "--help"},
		{"restore", "--from", "missing.tar.gz", "--help"}, {"snapshot", "prune", "--help"},
		{"recover", "--help"}, {"quorum-init", "--help"}, {"quorum-join", "--help"},
		{"pair", "--help"}, {"fence", "abandon", "--help"},
		{"mcp-token", "create", "--help"}, {"nevercompact", "enable", "--help"},
		{"upgrade", "propose", "--help"}, {"hook", "session-start", "--help"},
		{"init-lantern-private", "--help"}, {"repair-chain", "--help"},
	} {
		t.Run(args[0]+"/"+args[1], func(t *testing.T) {
			dir := t.TempDir()
			home := filepath.Join(dir, "absent-home")
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, os.Args[0], append([]string{"-test.run=^TestCommandHelpProcess$", "--"}, args...)...)
			cmd.Dir = dir
			cmd.Env = append(os.Environ(), "SAGE_TEST_CLI_ENTRY=1", "SAGE_HOME="+home)
			out, err := cmd.CombinedOutput()
			require.NoError(t, err, string(out))
			require.Contains(t, string(out), "sage-gui")
			require.Contains(t, string(out), "SAGE_HOME")
			require.NoDirExists(t, home)
			entries, err := os.ReadDir(dir)
			require.NoError(t, err)
			require.Empty(t, entries, "help must not write project configuration or exports")
		})
	}
}

func TestSimpleCommandsRejectUnknownArguments(t *testing.T) {
	for _, name := range []string{"serve", "setup", "status", "version", "cert-status", "mcp", "codex"} {
		require.Error(t, validateSimpleCommandArgs([]string{name, "--bogus"}))
	}
	require.NoError(t, validateSimpleCommandArgs([]string{"serve"}))
	require.NoError(t, validateSimpleCommandArgs([]string{"mcp", "install"}))
	require.NoError(t, validateSimpleCommandArgs([]string{"mcp", "install", "--token", "claim-token"}))
	require.NoError(t, validateSimpleCommandArgs([]string{"mcp", "install", "--token=claim-token"}))
	require.Error(t, validateSimpleCommandArgs([]string{"mcp", "install", "--token"}))
	require.Error(t, validateSimpleCommandArgs([]string{"mcp", "install", "--token="}))
	require.Error(t, validateSimpleCommandArgs([]string{"mcp", "install", "--bogus"}))
	require.NoError(t, validateSimpleCommandArgs([]string{"backup", "--full"}))
}
