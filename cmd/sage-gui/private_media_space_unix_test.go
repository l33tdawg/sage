//go:build linux || darwin

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPrivateMediaNativeSpaceProbeLifecycle(t *testing.T) {
	path := filepath.Join(t.TempDir(), "synthetic.db")
	require.NoError(t, os.WriteFile(path, nil, 0600))
	probe, closeProbe, err := newPrivateMediaSpaceProbe(path)
	require.NoError(t, err)
	defer closeProbe()
	available, err := probe(t.Context())
	require.NoError(t, err)
	require.Greater(t, available, int64(0))
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = probe(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.NoError(t, os.Rename(path, path+".moved"))
	_, err = probe(t.Context())
	require.NoError(t, err)
	closeProbe()
	_, err = probe(t.Context())
	require.Error(t, err)
}

func TestPrivateMediaNativeProbeRejectsUnsupportedPaths(t *testing.T) {
	for _, path := range []string{"relative.db", t.TempDir(), filepath.Join(t.TempDir(), "missing.db")} {
		_, _, err := newPrivateMediaSpaceProbe(path)
		require.Error(t, err)
	}
}
