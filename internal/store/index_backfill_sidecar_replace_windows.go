//go:build windows

package store

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// Windows does not expose a portable parent-directory fsync. MoveFileEx with
// REPLACE_EXISTING and WRITE_THROUGH provides the matching durable replacement
// boundary after the temp file itself has been synced and closed.
func replaceIndexBackfillSidecarDurably(from, to string) error {
	fromUTF16, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return fmt.Errorf("encode temporary index backfill sidecar path: %w", err)
	}
	toUTF16, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return fmt.Errorf("encode index backfill sidecar path: %w", err)
	}
	flags := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if err := windows.MoveFileEx(fromUTF16, toUTF16, flags); err != nil {
		return fmt.Errorf("replace index backfill sidecar durably: %w", err)
	}
	return nil
}
