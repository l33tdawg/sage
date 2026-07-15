//go:build windows

package statesync

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/windows"
)

func TestIsLocalDiskCapacityErrorWindows(t *testing.T) {
	assert.True(t, IsLocalDiskCapacityError(fmt.Errorf("Badger restore: %w", windows.ERROR_DISK_FULL)))
	assert.True(t, IsLocalDiskCapacityError(fmt.Errorf("Badger restore: %w", windows.ERROR_HANDLE_DISK_FULL)))
	assert.True(t, IsLocalDiskCapacityError(fmt.Errorf("Badger restore: %w", windows.ERROR_DISK_QUOTA_EXCEEDED)))
	assert.False(t, IsLocalDiskCapacityError(fmt.Errorf("corrupt provider image")))
}
