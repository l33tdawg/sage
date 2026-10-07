//go:build !unix

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// openValidatedTranscript is the non-unix fallback. Without openat/O_NOFOLLOW it
// cannot close the ancestor-swap race the unix walk does; it canonicalizes the
// full path, verifies it stays under the trusted root, opens it, and validates the
// resulting descriptor. (Recall-backed compaction targets unix hosts; this keeps
// the build portable.)
func openValidatedTranscript(payloadPath string) (*os.File, error) {
	root, components, err := transcriptRelComponents(payloadPath)
	if err != nil {
		return nil, err
	}
	full := filepath.Join(append([]string{root}, components...)...)
	canonical, err := filepath.EvalSymlinks(full)
	if err != nil {
		return nil, fmt.Errorf("canonicalize: %w", err)
	}
	rel, err := filepath.Rel(root, canonical)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return nil, fmt.Errorf("transcript resolves outside the trusted root")
	}
	f, err := os.OpenFile(canonical, os.O_RDONLY, 0) //nolint:gosec // canonical is root-bound above
	if err != nil {
		return nil, fmt.Errorf("open: %w", err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat: %w", err)
	}
	if !fi.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("not a regular file")
	}
	if fi.Size() > preCompactMaxTranscript {
		_ = f.Close()
		return nil, fmt.Errorf("transcript too large: %d bytes", fi.Size())
	}
	if err := fileOwnedByCurrentUser(fi); err != nil {
		_ = f.Close()
		return nil, err
	}
	return f, nil
}

// fileOwnedByCurrentUser is a no-op on non-unix platforms; the trusted-root and
// regular-file checks still apply there.
func fileOwnedByCurrentUser(_ os.FileInfo) error { return nil }
