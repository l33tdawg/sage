//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package statesync

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sys/unix"
)

func TestIsLocalDiskCapacityErrorUnix(t *testing.T) {
	assert.True(t, IsLocalDiskCapacityError(fmt.Errorf("Badger restore: %w", unix.ENOSPC)))
	assert.True(t, IsLocalDiskCapacityError(fmt.Errorf("Badger restore: %w", unix.EDQUOT)))
	assert.True(t, IsLocalDiskCapacityError(fmt.Errorf("preflight: %w", ErrDiskCapacity)))
	assert.False(t, IsLocalDiskCapacityError(fmt.Errorf("corrupt provider image")))
}
