package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
)

// shellStartupChallengeEnv is a presence flag only. The 256-bit challenge is
// carried exclusively by the inherited stdin pipe; it must never appear in an
// argument, environment value, URL, log line, or persistent file.
const shellStartupChallengeEnv = "SAGE_SHELL_STARTUP_CHALLENGE"

const shellStartupChallengeBytes = 32

func shellStartupProofFromEnvironment() (string, error) {
	return shellStartupProof(os.Stdin, os.Getenv(shellStartupChallengeEnv))
}

func shellStartupProof(input io.Reader, enabled string) (string, error) {
	if enabled == "" {
		return "", nil
	}
	if enabled != "1" {
		return "", fmt.Errorf("invalid native-shell startup challenge mode")
	}
	var challenge [shellStartupChallengeBytes]byte
	if _, err := io.ReadFull(input, challenge[:]); err != nil {
		return "", fmt.Errorf("read native-shell startup challenge: %w", err)
	}
	proof := sha256.Sum256(challenge[:])
	return hex.EncodeToString(proof[:]), nil
}
