package statesync

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReceiveDiskRequirementsAndCapacityGate(t *testing.T) {
	staging, err := ReceiveStagingDiskRequirement(uint64(MinChunkSize))
	require.NoError(t, err)
	assert.Equal(t, uint64(2*MinChunkSize)+receiveDiskHeadroom, staging)
	prepared, err := ReceivePreparationDiskRequirement(uint64(MinChunkSize))
	require.NoError(t, err)
	assert.Equal(t, uint64(4*MinChunkSize)+receiveDiskHeadroom, prepared)
	assert.Greater(t, prepared, staging)
	_, err = ReceiveStagingDiskRequirement(0)
	require.Error(t, err)
	_, err = ReceiveStagingDiskRequirement(MaxSnapshotBytes + 1)
	require.Error(t, err)

	require.NoError(t, RequireAvailableDiskSpace(t.TempDir(), 1))
	capacityErr := RequireAvailableDiskSpace(t.TempDir(), math.MaxUint64)
	require.ErrorIs(t, capacityErr, ErrDiskCapacity)
	require.ErrorContains(t, capacityErr, "requires")
}
