//go:build darwin

package web

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func writeFakeAppBundle(t *testing.T, path, version string) string {
	t.Helper()
	binDir := filepath.Join(path, "Contents", "MacOS")
	require.NoError(t, os.MkdirAll(binDir, 0755))
	execPath := filepath.Join(binDir, "sage-gui")
	contents := "#!/bin/sh\nprintf 'sage-gui %s\\n' '" + version + "'\n"
	require.NoError(t, os.WriteFile(execPath, []byte(contents), 0755))
	return execPath
}

func TestMacOSAppBundleForExecutable(t *testing.T) {
	assertPath := "/Applications/SAGE.app/Contents/MacOS/sage-gui"
	require.Equal(t, "/Applications/SAGE.app", macOSAppBundleForExecutable(assertPath))
	require.Empty(t, macOSAppBundleForExecutable("/usr/local/bin/sage-gui"))
	require.Empty(t, macOSAppBundleForExecutable("/Applications/SAGE.app/Contents/MacOS/other"))
}

func TestPendingAppBundleInstallAndRollback(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stageDir := filepath.Join(root, "stage")
	stagedBundle := filepath.Join(stageDir, "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")

	require.NoError(t, installPendingAppBundle(execPath, stagedBundle, "v11.7.2"))
	require.Equal(t, "v11.7.2", PendingUpdateVersion(execPath))
	newData, err := os.ReadFile(execPath)
	require.NoError(t, err)
	require.Contains(t, string(newData), "v11.7.2")
	require.FileExists(t, filepath.Join(destination+".update-old", "Contents", "MacOS", "sage-gui"))

	rolledBack, err := RollbackPendingUpdate(execPath)
	require.NoError(t, err)
	require.True(t, rolledBack)
	oldData, err := os.ReadFile(execPath)
	require.NoError(t, err)
	require.Contains(t, string(oldData), "v11.7.1")
	require.Empty(t, PendingUpdateVersion(execPath))
}

func TestConfirmPendingAppUpdateRemovesExternalMarker(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	stageDir := filepath.Join(root, "stage")
	stagedBundle := filepath.Join(stageDir, "SAGE.app")
	writeFakeAppBundle(t, stagedBundle, "v11.7.2")

	require.NoError(t, installPendingAppBundle(execPath, stagedBundle, "v11.7.2"))
	require.NoError(t, ConfirmPendingUpdate(execPath))
	require.Empty(t, PendingUpdateVersion(execPath))
	require.NoDirExists(t, destination+".update-old")
}

func TestPreparedPendingAppUpdateDoesNotRollForwardDuringRecovery(t *testing.T) {
	root := t.TempDir()
	destination := filepath.Join(root, "SAGE.app")
	execPath := writeFakeAppBundle(t, destination, "v11.7.1")
	backupPath := destination + ".update-old"
	writeFakeAppBundle(t, backupPath, "v11.7.2")
	require.NoError(t, writeFileAtomicDurable(platformPendingUpdateMarker(execPath), []byte("v11.7.2\n"), 0600))

	rolledBack, err := RollbackPendingUpdate(execPath)
	require.NoError(t, err)
	require.False(t, rolledBack)
	require.Empty(t, PendingUpdateVersion(execPath))
	require.NoDirExists(t, backupPath)
	require.Equal(t, "v11.7.1", diskBinaryVersion(context.Background(), execPath))
}
