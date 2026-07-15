//go:build darwin || dragonfly || freebsd || linux || netbsd || openbsd

package statesync

import (
	"errors"
	"math"

	"golang.org/x/sys/unix"
)

func availableDiskBytes(path string) (uint64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, err
	}
	blockSize := uint64(stats.Bsize) // #nosec G115 -- successful Statfs reports a non-negative block size
	availableBlocks := uint64(stats.Bavail)
	if blockSize == 0 || availableBlocks > math.MaxUint64/blockSize {
		return 0, errors.New("filesystem returned invalid available-block accounting")
	}
	return availableBlocks * blockSize, nil
}

func isPlatformDiskCapacityError(err error) bool {
	return errors.Is(err, unix.ENOSPC) || errors.Is(err, unix.EDQUOT)
}
