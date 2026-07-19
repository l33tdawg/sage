package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShellStartupProofUsesOnlyInheritedChallengeBytes(t *testing.T) {
	challenge := bytes.Repeat([]byte{0xA5}, shellStartupChallengeBytes)
	proof, err := shellStartupProof(bytes.NewReader(challenge), "1")
	require.NoError(t, err)
	assert.Equal(t, "fc8b64001c5fdd0f2f40fb67dae4a865a2c5bd17836676d6d5b58b7917e33717", proof)
}

func TestShellStartupProofLeavesNormalServeStdinUntouched(t *testing.T) {
	proof, err := shellStartupProof(bytes.NewBufferString("not consumed"), "")
	require.NoError(t, err)
	assert.Empty(t, proof)
}

func TestShellStartupProofRejectsMissingOrInvalidChallenge(t *testing.T) {
	_, err := shellStartupProof(bytes.NewReader([]byte("short")), "1")
	require.ErrorContains(t, err, "read native-shell startup challenge")

	_, err = shellStartupProof(bytes.NewReader(nil), "yes")
	require.ErrorContains(t, err, "invalid native-shell startup challenge mode")
}

func TestShellStartupProofClearsOneShotMarkerBeforeServeCanRestart(t *testing.T) {
	cleared := ""
	proof, err := shellStartupProofAndClear(
		bytes.NewReader(bytes.Repeat([]byte{0xA5}, shellStartupChallengeBytes)),
		"1",
		func(name string) error {
			cleared = name
			return nil
		},
	)
	require.NoError(t, err)
	assert.NotEmpty(t, proof)
	assert.Equal(t, shellStartupChallengeEnv, cleared)

	_, err = shellStartupProofAndClear(
		bytes.NewReader(bytes.Repeat([]byte{0xA5}, shellStartupChallengeBytes)),
		"1",
		func(string) error {
			return errors.New("cannot clear")
		},
	)
	require.ErrorContains(t, err, "clear native-shell startup challenge mode")
}

func TestServeExitCodeSignalsOnlyLockContention(t *testing.T) {
	assert.Equal(t, nativeShellAlreadyRunningExitCode, serveExitCode(errInstanceLockHeld))
	assert.Equal(t, 1, serveExitCode(errors.New("ordinary startup error")))
}

func TestShellStartupProofRuntimeRejectsTruncatedInheritedPipe(t *testing.T) {
	binary := filepath.Join(t.TempDir(), "sage-gui")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	build := exec.Command("go", "build", "-o", binary, ".")
	build.Env = os.Environ()
	output, err := build.CombinedOutput()
	require.NoErrorf(t, err, "build runtime smoke binary: %s", output)

	command := exec.Command(binary, "serve")
	command.Env = append(
		os.Environ(),
		"SAGE_HOME="+t.TempDir(),
		shellStartupChallengeEnv+"=1",
	)
	command.Stdin = bytes.NewReader(bytes.Repeat([]byte{0xA5}, shellStartupChallengeBytes-1))
	output, err = command.CombinedOutput()
	require.Error(t, err, "truncated inherited challenge must abort before daemon startup")
	assert.Contains(t, string(output), "read native-shell startup challenge")
	assert.NotContains(t, string(output), "a5")
}
