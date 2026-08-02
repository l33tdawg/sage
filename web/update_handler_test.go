package web

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSemverGreater(t *testing.T) {
	tests := []struct {
		a, b string
		want bool
	}{
		{"3.6.0", "3.5.0", true},
		{"3.5.0", "3.6.0", false},
		{"3.6.0", "3.6.0", false}, // equal, not greater
		{"3.10.0", "3.9.0", true}, // 10 > 9 (not string compare)
		{"4.0.0", "3.99.99", true},
		{"3.6.1", "3.6.0", true},
		{"3.6.0", "3.6.1", false},
		{"1.0.0", "0.99.0", true},
		{"3.7.0", "3.6.0", true},     // the real scenario: latest > current
		{"3.5.0", "3.5.0", false},    // same version
		{"dev", "3.6.0", false},      // dev parses as 0.0.0
		{"3.6.0-rc1", "3.5.0", true}, // pre-release suffix stripped
	}

	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			assert.Equal(t, tt.want, semverGreater(tt.a, tt.b))
		})
	}
}

func TestAssetSHA256Digest(t *testing.T) {
	const sum = "01e4bb2ba530b9269b6391569a7865e3e90bcecc1256ec632dc31c01551b5773"
	assert.Equal(t, sum, assetSHA256Digest("sha256:"+sum))
	assert.Equal(t, sum, assetSHA256Digest("SHA256:"+strings.ToUpper(sum)))

	for _, invalid := range []string{
		"",
		"sha512:" + sum,
		"sha256:short",
		"sha256:" + strings.Repeat("z", 64),
	} {
		assert.Empty(t, assetSHA256Digest(invalid), invalid)
	}
}

func TestInAppUpdateSupportRequiresExternalRecoveryOwner(t *testing.T) {
	assert.True(t, inAppUpdateSupported("linux"))
	assert.False(t, inAppUpdateSupported("darwin"), "macOS must use the signed DMG until an external rollback helper exists")
	assert.False(t, inAppUpdateSupported("windows"))
}

func TestReleaseAssetVersionBindsCanonicalTag(t *testing.T) {
	asset := findUpdateAssetName("11.17.0", runtime.GOOS, runtime.GOARCH)
	rawURL := "https://github.com/l33tdawg/sage/releases/download/v11.17.0/" + asset
	version, err := releaseAssetVersion(rawURL)
	require.NoError(t, err)
	require.Equal(t, "v11.17.0", version)
	require.True(t, releaseAssetMatchesPlatform(rawURL, version))
	require.False(t, releaseAssetMatchesPlatform(
		"https://github.com/l33tdawg/sage/releases/download/v11.17.0/wrong-platform.zip", version,
	))

	for _, rawURL := range []string{
		"https://github.com/l33tdawg/sage/releases/latest/download/SAGE.dmg",
		"https://github.com/l33tdawg/sage/releases/download/v11.17/SAGE.dmg",
		"https://github.com/l33tdawg/sage/releases/download/v11.17.0/extra/SAGE.dmg",
		"https://github.com/l33tdawg/sage/releases/download/v11.17.0%2Fbad/SAGE.dmg",
		"https://github.com.evil.invalid/l33tdawg/sage/releases/download/v11.17.0/SAGE.dmg",
		"https://github.com:8443/l33tdawg/sage/releases/download/v11.17.0/SAGE.dmg",
		"https://github.com/l33tdawg/other/releases/download/v11.17.0/SAGE.dmg",
	} {
		_, err := releaseAssetVersion(rawURL)
		require.Error(t, err, rawURL)
	}
}

func TestHandleRestartQueuesCoordinatedLifecycle(t *testing.T) {
	var calls atomic.Int32
	h := &DashboardHandler{BootID: "boot-a", RequestRestart: func() error {
		calls.Add(1)
		return nil
	}}
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/update/restart", nil)
	markUnencryptedLoopbackCEREBRUM(req)
	w := httptest.NewRecorder()
	h.handleRestart(w, req)
	if runtime.GOOS == "windows" {
		require.Equal(t, http.StatusOK, w.Code)
		require.Zero(t, calls.Load())
		return
	}
	require.Equal(t, http.StatusAccepted, w.Code, w.Body.String())
	require.Equal(t, int32(1), calls.Load())
	assert.Contains(t, w.Body.String(), `"status":"draining"`)
	assert.Contains(t, w.Body.String(), `"boot_id":"boot-a"`)
}

func TestHandleRestartRejectsAgentAndUpdateRace(t *testing.T) {
	h := &DashboardHandler{RequestRestart: func() error { return nil }}
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/update/restart", nil)
	signAgentRequest(t, req, priv, nil)
	w := httptest.NewRecorder()
	h.handleRestart(w, req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, hex.EncodeToString(priv.Public().(ed25519.PublicKey)))))
	require.Equal(t, http.StatusForbidden, w.Code)

	h.UpdateInProgress.Store(true)
	req = httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/update/restart", nil)
	markLocalCEREBRUM(h, req)
	w = httptest.NewRecorder()
	h.handleRestart(w, req)
	require.Equal(t, http.StatusConflict, w.Code)
}

func TestHandleApplyUpdateRequiresTrustedChecksum(t *testing.T) {
	h := &DashboardHandler{}
	body := strings.NewReader(`{"download_url":"https://github.com/l33tdawg/sage/releases/download/v11.7.0/sage.tar.gz"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/update/apply", body)
	markUnencryptedLoopbackCEREBRUM(req)
	w := httptest.NewRecorder()
	h.handleApplyUpdate(w, req)
	require.Equal(t, http.StatusBadRequest, w.Code)
	if runtime.GOOS == "darwin" {
		assert.Contains(t, w.Body.String(), "signed DMG")
	} else if runtime.GOOS == "windows" {
		assert.Contains(t, w.Body.String(), "signed release installer")
	} else {
		assert.Contains(t, w.Body.String(), "checksum")
	}
}

func TestUnsupportedPlatformApplyRejectsBeforeAnyUpdaterMutation(t *testing.T) {
	if inAppUpdateSupported(runtime.GOOS) {
		t.Skip("this platform intentionally supports the in-app updater")
	}
	vaultDir := t.TempDir()
	vaultPath := filepath.Join(vaultDir, "vault.key")
	require.NoError(t, os.WriteFile(vaultPath, []byte("irreplaceable-vault"), 0600))
	h := &DashboardHandler{VaultKeyPath: vaultPath}
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/update/apply", strings.NewReader(`{`))
	markUnencryptedLoopbackCEREBRUM(req)
	w := httptest.NewRecorder()

	h.handleApplyUpdate(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	assert.False(t, h.UpdateInProgress.Load(), "manual-install rejection must not claim the updater lock")
	h.updateStateMu.RLock()
	stateLen := len(h.updateState)
	h.updateStateMu.RUnlock()
	assert.Zero(t, stateLen, "manual-install rejection must not publish a queued updater state")
	require.NoFileExists(t, filepath.Join(vaultDir, "backups", "vault-pre-update.key"))
	require.NoFileExists(t, vaultPath+pendingUpdateSuffix)
	data, err := os.ReadFile(vaultPath)
	require.NoError(t, err)
	assert.Equal(t, "irreplaceable-vault", string(data))
}

func TestUnencryptedLoopbackCEREBRUMReachesUpdaterThroughRouter(t *testing.T) {
	h, _ := newTestHandler(t)
	router := testRouter(h)
	body := strings.NewReader(`{"download_url":"https://github.com/l33tdawg/sage/releases/download/v11.14.2/sage.tar.gz"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/update/apply", body)
	req.Header.Set("Content-Type", "application/json")
	markUnencryptedLoopbackCEREBRUM(req)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusBadRequest, w.Code, w.Body.String())
	if runtime.GOOS == "darwin" {
		assert.Contains(t, w.Body.String(), "signed DMG")
	} else if runtime.GOOS == "windows" {
		assert.Contains(t, w.Body.String(), "signed release installer")
	} else {
		assert.Contains(t, w.Body.String(), "checksum")
	}
}

func markUnencryptedLoopbackCEREBRUM(req *http.Request) {
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
}

func TestUpdateStatusSurvivesDroppedSSEConnection(t *testing.T) {
	h := &DashboardHandler{}
	h.UpdateInProgress.Store(true)
	h.sendUpdateProgress("verify", "active", "Verifying SHA-256 checksum...")

	w := httptest.NewRecorder()
	h.handleGetUpdateStatus(w, httptest.NewRequest(http.MethodGet, "/v1/dashboard/settings/update/status", nil))
	require.Equal(t, http.StatusOK, w.Code)
	assert.JSONEq(t, `{
		"in_progress": true,
		"state": {
			"step": "verify",
			"status": "active",
			"message": "Verifying SHA-256 checksum..."
		}
	}`, w.Body.String())
}

func TestPendingBinaryInstallRollbackAndConfirmation(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("executable replacement semantics differ on Windows")
	}
	makeExecutable := func(path, version string) {
		require.NoError(t, os.WriteFile(path, []byte("#!/bin/sh\necho sage-gui "+version+"\n"), 0755))
	}

	t.Run("rollback restores previous executable", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")

		require.NoError(t, installPendingBinary(execPath, newPath, "v11.7.0"))
		require.Equal(t, "v11.7.0", PendingUpdateVersion(execPath))
		markerData, err := os.ReadFile(execPath + pendingUpdateSuffix)
		require.NoError(t, err)
		require.Equal(t, pendingUpdateRecord{
			Version: "v11.7.0", RollbackVersion: "v11.6.1",
		}, decodePendingUpdateRecord(markerData))
		require.FileExists(t, execPath+".old")
		require.Equal(t, "v11.7.0", diskBinaryVersion(context.Background(), execPath))

		rolledBack, err := RollbackPendingUpdate(execPath)
		require.NoError(t, err)
		require.True(t, rolledBack)
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("prepared crash clears only the exact structured marker", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		backupPath := execPath + ".old"
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(backupPath, "v11.6.1")
		marker, err := json.Marshal(pendingUpdateRecord{
			Version: "v11.7.0", RollbackVersion: "v11.6.1",
		})
		require.NoError(t, err)
		require.NoError(t, writeFileAtomicDurable(execPath+pendingUpdateSuffix, append(marker, '\n'), 0600))

		reconciled, err := ReconcilePreparedPendingBinaryUpdate(execPath)
		require.NoError(t, err)
		require.True(t, reconciled)
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), backupPath))
	})

	t.Run("prepared crash does not guess across a lineage mismatch", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		makeExecutable(execPath, "v11.5.9")
		makeExecutable(execPath+".old", "v11.6.1")
		marker, err := json.Marshal(pendingUpdateRecord{
			Version: "v11.7.0", RollbackVersion: "v11.6.1",
		})
		require.NoError(t, err)
		require.NoError(t, writeFileAtomicDurable(execPath+pendingUpdateSuffix, append(marker, '\n'), 0600))

		reconciled, err := ReconcilePreparedPendingBinaryUpdate(execPath)
		require.ErrorContains(t, err, "expected either v11.7.0 or rollback v11.6.1")
		require.False(t, reconciled)
		require.FileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("activated crash keeps exact marker and rollback binary", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")

		require.NoError(t, installPendingBinary(execPath, newPath, "v11.7.0"))
		reconciled, err := ReconcilePreparedPendingBinaryUpdate(execPath)
		require.NoError(t, err)
		require.False(t, reconciled, "an activated update must remain pending until readiness confirmation")
		require.Equal(t, "v11.7.0", diskBinaryVersion(context.Background(), execPath))
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath+".old"))
		require.Equal(t, "v11.7.0", PendingUpdateVersion(execPath))
	})

	t.Run("legacy marker remains untouched during prepared reconciliation", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(execPath+".old", "v11.6.1")
		require.NoError(t, writeFileAtomicDurable(execPath+pendingUpdateSuffix, []byte("v11.7.0\n"), 0600))

		reconciled, err := ReconcilePreparedPendingBinaryUpdate(execPath)
		require.NoError(t, err)
		require.False(t, reconciled)
		require.Equal(t, "v11.7.0", PendingUpdateVersion(execPath))
		require.FileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("linked pending marker blocks a new install", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")
		realMarker := filepath.Join(dir, "real-marker")
		require.NoError(t, os.WriteFile(realMarker, []byte("v11.7.0\n"), 0600))
		require.NoError(t, os.Symlink(realMarker, execPath+pendingUpdateSuffix))

		err := installPendingBinary(execPath, newPath, "v11.7.0")
		require.ErrorContains(t, err, "must be a real regular file")
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
	})

	t.Run("linked rollback binary is never followed", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		makeExecutable(execPath, "v11.7.0")
		realBackup := filepath.Join(dir, "real-old")
		makeExecutable(realBackup, "v11.6.1")
		require.NoError(t, os.Symlink(realBackup, execPath+".old"))
		marker, err := json.Marshal(pendingUpdateRecord{
			Version: "v11.7.0", RollbackVersion: "v11.6.1",
		})
		require.NoError(t, err)
		require.NoError(t, writeFileAtomicDurable(execPath+pendingUpdateSuffix, append(marker, '\n'), 0600))

		rolledBack, err := RollbackPendingUpdate(execPath)
		require.ErrorContains(t, err, "must be a real regular file")
		require.False(t, rolledBack)
		require.Equal(t, "v11.7.0", diskBinaryVersion(context.Background(), execPath))
		require.FileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("linked installed binary is rejected before mutation", func(t *testing.T) {
		dir := t.TempDir()
		realExec := filepath.Join(dir, "real-sage-gui")
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(realExec, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")
		require.NoError(t, os.Symlink(realExec, execPath))

		err := installPendingBinary(execPath, newPath, "v11.7.0")
		require.ErrorContains(t, err, "installed binary must be a real regular file")
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), realExec))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
		require.NoFileExists(t, execPath+".old")
	})

	t.Run("existing linked rollback artifact blocks install", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		realBackup := filepath.Join(dir, "real-backup")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")
		makeExecutable(realBackup, "v11.5.0")
		require.NoError(t, os.Symlink(realBackup, execPath+".old"))

		err := installPendingBinary(execPath, newPath, "v11.7.0")
		require.ErrorContains(t, err, "existing rollback binary must be a real regular file")
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.Equal(t, "v11.5.0", diskBinaryVersion(context.Background(), realBackup))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("oversized update binary is rejected instead of truncated", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")
		require.NoError(t, os.Truncate(newPath, maxUpdateBinarySize+1))

		err := installPendingBinary(execPath, newPath, "v11.7.0")
		require.ErrorContains(t, err, "verified update binary has an invalid size")
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
		require.NoFileExists(t, execPath+".old")
	})

	t.Run("copied update must report the exact requested version", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.1")

		err := installPendingBinary(execPath, newPath, "v11.7.0")
		require.ErrorContains(t, err, "verified update binary reports v11.7.1, expected v11.7.0")
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("oversized marker is visible as invalid recovery state", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		makeExecutable(execPath, "v11.7.0")
		makeExecutable(execPath+".old", "v11.6.1")
		require.NoError(t, os.WriteFile(execPath+pendingUpdateSuffix, make([]byte, 4097), 0600))

		rolledBack, err := RollbackPendingUpdate(execPath)
		require.ErrorContains(t, err, "pending update marker has an invalid size")
		require.False(t, rolledBack)
		require.FileExists(t, execPath+pendingUpdateSuffix)
		require.FileExists(t, execPath+".old")
	})

	t.Run("unlaunchable activated binary restores a fresh exact rollback copy", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")
		require.NoError(t, installPendingBinary(execPath, newPath, "v11.7.0"))
		backupInfo, err := os.Stat(execPath + ".old")
		require.NoError(t, err)
		require.NoError(t, os.Chmod(execPath, 0644))

		rolledBack, err := RollbackPendingUpdate(execPath)
		require.NoError(t, err)
		require.True(t, rolledBack)
		restoredInfo, err := os.Stat(execPath)
		require.NoError(t, err)
		require.False(t, os.SameFile(backupInfo, restoredInfo), "rollback must validate a fresh final-path inode")
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
		require.NoFileExists(t, execPath+".old")
	})

	t.Run("legacy marker still permits explicit rollback", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		makeExecutable(execPath, "v11.7.0")
		makeExecutable(execPath+".old", "v11.6.1")
		require.NoError(t, writeFileAtomicDurable(execPath+pendingUpdateSuffix, []byte("v11.7.0\n"), 0600))

		rolledBack, err := RollbackPendingUpdate(execPath)
		require.NoError(t, err)
		require.True(t, rolledBack)
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath))
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
	})

	t.Run("healthy replacement removes rollback state", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		newPath := filepath.Join(dir, "downloaded")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(newPath, "v11.7.0")

		require.NoError(t, installPendingBinary(execPath, newPath, "v11.7.0"))
		require.NoError(t, ConfirmPendingUpdate(execPath))
		require.FileExists(t, execPath+".old", "one previous healthy binary is retained for recovery")
		require.NoFileExists(t, execPath+pendingUpdateSuffix)
		require.Equal(t, "v11.7.0", diskBinaryVersion(context.Background(), execPath))
	})

	t.Run("second update cannot replace unconfirmed rollback ancestry", func(t *testing.T) {
		dir := t.TempDir()
		execPath := filepath.Join(dir, "sage-gui")
		firstPath := filepath.Join(dir, "first")
		secondPath := filepath.Join(dir, "second")
		makeExecutable(execPath, "v11.6.1")
		makeExecutable(firstPath, "v11.7.0")
		makeExecutable(secondPath, "v11.8.0")

		require.NoError(t, installPendingBinary(execPath, firstPath, "v11.7.0"))
		require.Error(t, installPendingBinary(execPath, secondPath, "v11.8.0"))
		require.Equal(t, "v11.7.0", diskBinaryVersion(context.Background(), execPath))
		require.Equal(t, "v11.6.1", diskBinaryVersion(context.Background(), execPath+".old"))
		require.Equal(t, "v11.7.0", PendingUpdateVersion(execPath))
	})
}

func TestLinuxPackagedBinarySwapEndToEnd(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("the release workflow supplies a Linux GoReleaser archive on its native runner")
	}
	archivePath := strings.TrimSpace(os.Getenv("SAGE_PACKAGED_UPDATE_ARCHIVE"))
	expectedVersion := strings.TrimSpace(os.Getenv("SAGE_PACKAGED_UPDATE_VERSION"))
	if archivePath == "" || expectedVersion == "" {
		t.Skip("set SAGE_PACKAGED_UPDATE_ARCHIVE and SAGE_PACKAGED_UPDATE_VERSION in the release artifact job")
	}
	archiveInfo, err := os.Lstat(archivePath)
	require.NoError(t, err)
	require.True(t, archiveInfo.Mode().IsRegular())
	require.Zero(t, archiveInfo.Mode()&os.ModeSymlink)

	archive, err := os.Open(archivePath) //nolint:gosec -- release-job supplied exact artifact path
	require.NoError(t, err)
	extractedPath, err := extractBinaryFromTarGz(archive, "sage-gui")
	require.NoError(t, err)
	require.NoError(t, archive.Close())
	t.Cleanup(func() { _ = os.Remove(extractedPath) })
	require.True(t, sameReleaseVersion(diskBinaryVersion(context.Background(), extractedPath), expectedVersion))

	dir := t.TempDir()
	execPath := filepath.Join(dir, "sage-gui")
	require.NoError(t, os.WriteFile(execPath, []byte("#!/bin/sh\nprintf 'sage-gui 0.0.0\\n'\n"), 0755))
	require.NoError(t, installPendingBinary(execPath, extractedPath, expectedVersion))
	require.True(t, sameReleaseVersion(diskBinaryVersion(context.Background(), execPath), expectedVersion))
	require.Equal(t, pendingUpdateRecord{
		Version: expectedVersion, RollbackVersion: "0.0.0",
	}, decodePendingUpdateRecord(mustReadFile(t, execPath+pendingUpdateSuffix)))
	require.NoError(t, ConfirmPendingUpdate(execPath))
	require.NoFileExists(t, execPath+pendingUpdateSuffix)
	require.Equal(t, "0.0.0", diskBinaryVersion(context.Background(), execPath+".old"))
}

func mustReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return data
}

func TestParseSemver(t *testing.T) {
	tests := []struct {
		input string
		want  [3]int
	}{
		{"3.6.0", [3]int{3, 6, 0}},
		{"v3.6.0", [3]int{3, 6, 0}},
		{"3.10.1", [3]int{3, 10, 1}},
		{"3.6.0-rc1", [3]int{3, 6, 0}},
		{"3.6.0+build123", [3]int{3, 6, 0}},
		{"dev", [3]int{0, 0, 0}},
		{"1.0", [3]int{1, 0, 0}},
		{"", [3]int{0, 0, 0}},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.want, parseSemver(tt.input))
		})
	}
}

func TestFindAssetName(t *testing.T) {
	assert.Equal(t, "SAGE-v3.7.0-macOS-arm64.dmg", findUpdateAssetName("3.7.0", "darwin", "arm64"))
	assert.Equal(t, "SAGE-v3.7.0-macOS-x86_64.dmg", findUpdateAssetName("3.7.0", "darwin", "amd64"))
	assert.Equal(t, "sage-gui_3.7.0_linux_amd64.tar.gz", findUpdateAssetName("3.7.0", "linux", "amd64"))
	assert.Equal(t, "sage-gui_3.7.0_windows_arm64.zip", findUpdateAssetName("3.7.0", "windows", "arm64"))
}

func TestRedirectRestriction(t *testing.T) {
	// Set up a malicious server that the redirect would point to
	malicious := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("malicious payload")) //nolint:errcheck
	}))
	defer malicious.Close()

	// Set up a server that redirects to the malicious server
	redirector := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, malicious.URL+"/evil", http.StatusFound)
	}))
	defer redirector.Close()

	// Build the CheckRedirect function (same as in handleApplyUpdate)
	checkRedirect := func(req *http.Request, via []*http.Request) error {
		u := req.URL.String()
		allowed := strings.HasPrefix(u, "https://github.com/") ||
			strings.HasPrefix(u, "https://objects.githubusercontent.com/") ||
			strings.HasPrefix(u, "https://release-assets.githubusercontent.com/")
		if !allowed {
			return fmt.Errorf("redirect to non-GitHub URL blocked")
		}
		return nil
	}

	// Test: non-GitHub redirect is blocked
	t.Run("blocks non-GitHub redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://evil.com/payload", nil)
		err := checkRedirect(req, nil)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "redirect to non-GitHub URL blocked")
	})

	// Test: GitHub redirect is allowed
	t.Run("allows GitHub redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://github.com/l33tdawg/sage/releases/download/v3.7.0/archive.tar.gz", nil)
		err := checkRedirect(req, nil)
		assert.NoError(t, err)
	})

	// Test: objects.githubusercontent.com redirect is allowed
	t.Run("allows githubusercontent redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://objects.githubusercontent.com/some-hash/archive.tar.gz", nil)
		err := checkRedirect(req, nil)
		assert.NoError(t, err)
	})

	// Test: release-assets.githubusercontent.com redirect is allowed
	t.Run("allows release-assets githubusercontent redirect", func(t *testing.T) {
		req, _ := http.NewRequest("GET", "https://release-assets.githubusercontent.com/github-production-release-asset/123456/archive.tar.gz", nil)
		err := checkRedirect(req, nil)
		assert.NoError(t, err)
	})
}

func TestPathTraversalRejection(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want bool // true = should be rejected
	}{
		{"clean URL", "https://github.com/l33tdawg/sage/releases/download/v3.7.0/archive.tar.gz", false},
		{"path traversal", "https://github.com/l33tdawg/sage/releases/download/../../evil.tar.gz", true},
		{"double dot in query", "https://github.com/l33tdawg/sage/releases/download/v3.7.0/a..b.tar.gz", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rejected := strings.Contains(tt.url, "..")
			assert.Equal(t, tt.want, rejected)
		})
	}
}

func TestInstallErrorMessagePermissionDenied(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("read-only dir permission semantics differ on Windows")
	}

	// Simulate the TCC failure mode: renaming a binary inside a directory we
	// cannot write to (like /Applications/SAGE.app/Contents/MacOS when macOS
	// denies App Management).
	locked := filepath.Join(t.TempDir(), "locked")
	require.NoError(t, os.Mkdir(locked, 0755))
	binPath := filepath.Join(locked, "sage-gui")
	require.NoError(t, os.WriteFile(binPath, []byte("old binary"), 0755))
	require.NoError(t, os.Chmod(locked, 0555))
	t.Cleanup(func() { _ = os.Chmod(locked, 0755) })

	err := os.Rename(binPath, binPath+".old")
	require.Error(t, err, "rename inside a read-only dir should fail")
	assert.True(t, isPermissionDenied(err), "rename failure should be detected as permission denied: %v", err)

	downloadURL := "https://github.com/l33tdawg/sage/releases/download/v10.5.0/sage-gui_10.5.0_darwin_arm64.tar.gz"
	msg := installErrorMessage("Failed to backup current binary", err, downloadURL)

	if runtime.GOOS == "darwin" {
		// Actionable guidance, not a dead end
		assert.Contains(t, msg, "App Management")
		assert.Contains(t, msg, "System Settings")
		assert.Contains(t, msg, "quit SAGE")
		assert.Contains(t, msg, "https://github.com/l33tdawg/sage/releases/tag/v10.5.0")
	} else {
		assert.Contains(t, msg, "Failed to backup current binary")
	}
}

func TestInstallErrorMessageGenericError(t *testing.T) {
	// Non-permission errors keep the plain "action: error" shape.
	err := errors.New("no space left on device")
	msg := installErrorMessage("Failed to install", err, "https://github.com/l33tdawg/sage/releases/download/v10.5.0/x.tar.gz")
	assert.Equal(t, "Failed to install: no space left on device", msg)
	assert.NotContains(t, msg, "App Management")
}

func TestIsPermissionDenied(t *testing.T) {
	assert.True(t, isPermissionDenied(os.ErrPermission))
	assert.True(t, isPermissionDenied(fmt.Errorf("rename failed: %w", os.ErrPermission)))
	assert.False(t, isPermissionDenied(errors.New("operation not permitted"))) // plain string, no errno
	assert.False(t, isPermissionDenied(errors.New("disk full")))
	assert.False(t, isPermissionDenied(nil))
}

func TestReleasePageURL(t *testing.T) {
	tests := []struct {
		name string
		url  string
		want string
	}{
		{
			"asset URL",
			"https://github.com/l33tdawg/sage/releases/download/v10.5.0/sage-gui_10.5.0_darwin_arm64.tar.gz",
			"https://github.com/l33tdawg/sage/releases/tag/v10.5.0",
		},
		{
			"no marker falls back to latest",
			"https://github.com/l33tdawg/sage/archive/main.tar.gz",
			"https://github.com/l33tdawg/sage/releases/latest",
		},
		{
			"marker without asset falls back to latest",
			"https://github.com/l33tdawg/sage/releases/download/v10.5.0",
			"https://github.com/l33tdawg/sage/releases/latest",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, releasePageURL(tt.url))
		})
	}
}

func TestParseVersionOutput(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"release build", "sage-gui v10.4.4 (commit e55b431, built 2026-06-10)\n", "v10.4.4"},
		{"dev build", "sage-gui dev (commit none, built unknown)\n", "dev"},
		{"multi-line takes first", "sage-gui v10.5.0 (commit abc, built now)\nextra noise\n", "v10.5.0"},
		{"wrong binary name", "other-tool v1.0.0\n", ""},
		{"garbage", "command not found\n", ""},
		{"empty", "", ""},
		{"name only", "sage-gui\n", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, parseVersionOutput(tt.input))
		})
	}
}

func TestRestartRequired(t *testing.T) {
	tests := []struct {
		name          string
		running, disk string
		want          bool
	}{
		{"disk newer than running", "v10.4.4", "v10.5.0", true},
		{"same version", "v10.4.4", "v10.4.4", false},
		{"same modulo v prefix", "10.4.4", "v10.4.4", false},
		{"disk unknown", "v10.4.4", "", false},
		{"running unknown", "", "v10.5.0", false},
		{"running dev", "dev", "v10.5.0", false},
		{"disk dev", "v10.4.4", "dev", false},
		{"disk older still differs", "v10.5.0", "v10.4.4", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, restartRequired(tt.running, tt.disk))
		})
	}
}

func TestDiskBinaryVersion(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("uses shell scripts to fake the binary")
	}
	dir := t.TempDir()
	writeScript := func(name, body string) string {
		p := filepath.Join(dir, name)
		require.NoError(t, os.WriteFile(p, []byte("#!/bin/sh\n"+body+"\n"), 0755))
		return p
	}
	ctx := context.Background()

	t.Run("parses version from fake binary", func(t *testing.T) {
		p := writeScript("good", `echo "sage-gui v10.5.0 (commit abc1234, built 2026-06-11)"`)
		assert.Equal(t, "v10.5.0", diskBinaryVersion(ctx, p))
	})

	t.Run("unparseable output is graceful", func(t *testing.T) {
		p := writeScript("garbage", `echo "totally unexpected output"`)
		assert.Equal(t, "", diskBinaryVersion(ctx, p))
	})

	t.Run("non-zero exit is graceful", func(t *testing.T) {
		p := writeScript("failing", `exit 1`)
		assert.Equal(t, "", diskBinaryVersion(ctx, p))
	})

	t.Run("missing binary is graceful", func(t *testing.T) {
		assert.Equal(t, "", diskBinaryVersion(ctx, filepath.Join(dir, "does-not-exist")))
	})
}
