package mcp

import (
	"context"
	"os"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// branchCache holds the auto-detected git branch for the current working
// directory so we don't shell out to `git` on every memory write. The cache
// expires on a short TTL because the user may switch branches mid-session.
var (
	branchMu        sync.RWMutex
	branchValue     string
	branchValueWhen time.Time
	branchTTL       = 30 * time.Second
)

// currentBranchTag returns a tag like "branch:feature/foo" for the agent's
// working directory, or empty if branch tagging is disabled or the working
// directory isn't a git checkout. Auto-tagging is opt-out: set
// SAGE_BRANCH_TAG=0 to disable.
func currentBranchTag(ctx context.Context) string {
	if v := os.Getenv("SAGE_BRANCH_TAG"); v == "0" || v == "false" || v == "no" {
		return ""
	}

	branchMu.RLock()
	if branchValue != "" && time.Since(branchValueWhen) < branchTTL {
		v := branchValue
		branchMu.RUnlock()
		return v
	}
	branchMu.RUnlock()

	branchMu.Lock()
	defer branchMu.Unlock()
	// double-check after acquiring the write lock
	if branchValue != "" && time.Since(branchValueWhen) < branchTTL {
		return branchValue
	}

	cwd, err := os.Getwd()
	if err != nil {
		return ""
	}

	// Soft timeout so a wedged git process can't stall a memory write.
	// Race-enabled full-suite runs can leave a healthy local git process queued
	// behind CPU-heavy packages for longer than 750ms. Two seconds remains a
	// tight user-facing bound while avoiding false "not a repository" results
	// under ordinary machine load.
	cmdCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	cmd := exec.CommandContext(cmdCtx, "git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD")
	out, err := cmd.Output()
	if err != nil {
		// Not a git repo, git not installed, or the command timed out — all
		// equally "no branch tag for this write." Cache the empty result so
		// we don't retry on every memory write.
		branchValue = "-"
		branchValueWhen = time.Now()
		return ""
	}

	name := strings.TrimSpace(string(out))
	if name == "" || name == "HEAD" {
		// Detached HEAD or empty — not a meaningful branch to tag with.
		branchValue = "-"
		branchValueWhen = time.Now()
		return ""
	}

	tag := "branch:" + name
	branchValue = tag
	branchValueWhen = time.Now()
	return tag
}

// resetBranchCache clears the cached branch detection. Intended for tests.
func resetBranchCache() {
	branchMu.Lock()
	defer branchMu.Unlock()
	branchValue = ""
	branchValueWhen = time.Time{}
}
