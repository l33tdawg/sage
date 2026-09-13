//go:build !linux && !darwin

package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrivateMediaUnsupportedPlatformFailsClosed(t *testing.T) {
	probe, closeProbe, err := newPrivateMediaSpaceProbe("unused")
	require.Error(t, err)
	require.Nil(t, probe)
	require.Nil(t, closeProbe)
}
