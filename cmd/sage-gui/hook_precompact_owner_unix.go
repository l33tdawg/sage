//go:build unix

package main

import (
	"fmt"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// openValidatedTranscript opens the transcript by a descriptor-relative, no-follow
// component walk anchored at the trusted root, then validates the exact descriptor
// it returns. Every component below the root is opened with O_NOFOLLOW relative to
// its already-verified parent (via openat), so an ancestor directory swapped to a
// symlink between validation and open fails closed (ELOOP) instead of redirecting
// the descriptor out of the trusted root — closing the check-to-open race
// (blocker 4). The descriptor that is validated is the exact one the caller reads.
//
// Uses golang.org/x/sys/unix for openat, which is portable across linux/darwin/bsd
// (the stdlib syscall package does not expose Openat on darwin).
func openValidatedTranscript(payloadPath string) (*os.File, error) {
	root, components, err := transcriptRelComponents(payloadPath)
	if err != nil {
		return nil, err
	}
	// The trusted root is opened directly (its own symlinks are trusted); only the
	// components below it are walked no-follow.
	dirfd, err := unix.Open(root, unix.O_RDONLY|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, fmt.Errorf("open trusted root: %w", err)
	}
	for i, comp := range components {
		flags := unix.O_RDONLY | unix.O_NOFOLLOW | unix.O_CLOEXEC
		if i < len(components)-1 {
			flags |= unix.O_DIRECTORY
		}
		nfd, openErr := unix.Openat(dirfd, comp, flags, 0)
		_ = unix.Close(dirfd)
		if openErr != nil {
			return nil, fmt.Errorf("open %q no-follow: %w", comp, openErr)
		}
		dirfd = nfd
	}

	f := os.NewFile(uintptr(dirfd), payloadPath)
	if f == nil {
		_ = unix.Close(dirfd)
		return nil, fmt.Errorf("wrap transcript descriptor")
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("stat descriptor: %w", err)
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

// fileOwnedByCurrentUser rejects a transcript owned by a different uid, so the
// capture never reads a file planted by another user. Unix only.
func fileOwnedByCurrentUser(fi os.FileInfo) error {
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return nil // ownership indeterminable — best effort, don't block capture
	}
	if int(st.Uid) != os.Getuid() {
		return fmt.Errorf("transcript is owned by uid %d, not the current user (uid %d); refusing", st.Uid, os.Getuid())
	}
	return nil
}
