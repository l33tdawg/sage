package statesync

import (
	"errors"
	"fmt"
	"math"
)

const receiveDiskHeadroom uint64 = 256 << 20

// ErrDiskCapacity identifies a local receiver capacity check or exhaustion.
// It is terminal for the boot attempt and must not be blamed on or retried
// against another authorized provider.
var ErrDiskCapacity = errors.New("state sync disk capacity unavailable")

// IsLocalDiskCapacityError recognizes both the preflight sentinel and native
// filesystem exhaustion returned after a successful preflight. Capacity can
// change between the check and the Badger restore, so ENOSPC/EDQUOT-class
// failures remain terminal local errors instead of provider fallback signals.
func IsLocalDiskCapacityError(err error) bool {
	return errors.Is(err, ErrDiskCapacity) || isPlatformDiskCapacityError(err)
}

// ReceiveStagingDiskRequirement covers verified chunks plus the assembled
// canonical image. The fixed headroom keeps filesystem metadata and atomic
// temporary writes from consuming the final free blocks.
func ReceiveStagingDiskRequirement(snapshotBytes uint64) (uint64, error) {
	return amplifiedDiskRequirement(snapshotBytes, 2)
}

// ReceivePreparationDiskRequirement covers the restored Badger candidate plus
// its write/compaction overhead. When staging and live state share a filesystem,
// this check runs after staging already consumed its own reservation.
func ReceivePreparationDiskRequirement(snapshotBytes uint64) (uint64, error) {
	return amplifiedDiskRequirement(snapshotBytes, 4)
}

// RequireAvailableDiskSpace rejects a state-sync phase before it starts if the
// target filesystem cannot hold the bounded copies that phase creates.
func RequireAvailableDiskSpace(path string, required uint64) error {
	if path == "" || required == 0 {
		return errors.New("state sync disk-space check requires a path and positive byte count")
	}
	available, err := availableDiskBytes(path)
	if err != nil {
		return fmt.Errorf("%w: inspect filesystem: %v", ErrDiskCapacity, err)
	}
	if available < required {
		return fmt.Errorf("%w: filesystem has %d available bytes, requires %d", ErrDiskCapacity, available, required)
	}
	return nil
}

func amplifiedDiskRequirement(snapshotBytes, copies uint64) (uint64, error) {
	if snapshotBytes == 0 || snapshotBytes > MaxSnapshotBytes || copies == 0 {
		return 0, errors.New("state sync disk requirement input is invalid")
	}
	if snapshotBytes > (math.MaxUint64-receiveDiskHeadroom)/copies {
		return 0, errors.New("state sync disk requirement overflows")
	}
	return snapshotBytes*copies + receiveDiskHeadroom, nil
}
