//go:build windows

package main

import (
	"fmt"

	"golang.org/x/sys/windows"
)

// Windows does not expose a portable directory fsync through os.File. A
// write-through replacement gives the receiver-role transition the equivalent
// durability boundary before its activation journal may be removed.
func replaceConfigFileDurably(from, to string) error {
	fromUTF16, err := windows.UTF16PtrFromString(from)
	if err != nil {
		return fmt.Errorf("encode temporary config path: %w", err)
	}
	toUTF16, err := windows.UTF16PtrFromString(to)
	if err != nil {
		return fmt.Errorf("encode config path: %w", err)
	}
	flags := uint32(windows.MOVEFILE_REPLACE_EXISTING | windows.MOVEFILE_WRITE_THROUGH)
	if err := windows.MoveFileEx(fromUTF16, toUTF16, flags); err != nil {
		return fmt.Errorf("replace config file durably: %w", err)
	}
	return nil
}
