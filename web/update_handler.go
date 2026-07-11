package web

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	githubOwner = "l33tdawg"
	githubRepo  = "sage"
	githubAPI   = "https://api.github.com"
)

type githubRelease struct {
	TagName string        `json:"tag_name"`
	Name    string        `json:"name"`
	Body    string        `json:"body"`
	Assets  []githubAsset `json:"assets"`
	HTMLURL string        `json:"html_url"`
}

type githubAsset struct {
	Name               string `json:"name"`
	BrowserDownloadURL string `json:"browser_download_url"`
	Size               int64  `json:"size"`
}

// handleCheckUpdate checks current version vs latest GitHub release.
func (h *DashboardHandler) handleCheckUpdate(w http.ResponseWriter, r *http.Request) {
	current := h.Version

	// Fetch latest release from GitHub
	client := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequestWithContext(r.Context(), "GET",
		fmt.Sprintf("%s/repos/%s/%s/releases/latest", githubAPI, githubOwner, githubRepo), nil)
	if err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           "failed to check for updates",
		})
		return
	}
	req.Header.Set("Accept", "application/vnd.github.v3+json")
	req.Header.Set("User-Agent", "sage-gui/"+current)

	resp, err := client.Do(req)
	if err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           "could not reach GitHub: " + err.Error(),
		})
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           fmt.Sprintf("GitHub API returned %d", resp.StatusCode),
		})
		return
	}

	var release githubRelease
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"current_version": current,
			"error":           "failed to parse release info",
		})
		return
	}

	latest := strings.TrimPrefix(release.TagName, "v")
	currentClean := strings.TrimPrefix(current, "v")
	updateAvailable := current != "dev" && semverGreater(latest, currentClean)

	// Find the right asset for this platform
	assetName := findUpdateAssetName(latest, runtime.GOOS, runtime.GOARCH)
	var downloadURL string
	var assetSize int64
	var checksumsURL string
	var assetChecksumURL string
	for _, a := range release.Assets {
		if a.Name == assetName {
			downloadURL = a.BrowserDownloadURL
			assetSize = a.Size
		}
		if a.Name == "checksums.txt" {
			checksumsURL = a.BrowserDownloadURL
		}
		if a.Name == assetName+".sha256" {
			assetChecksumURL = a.BrowserDownloadURL
		}
	}

	// GoReleaser archives are listed in checksums.txt. Desktop installers are
	// built afterward and publish a sibling .sha256 asset instead; prefer that
	// when present so the signed macOS DMG can use the same verified updater.
	var expectedChecksum string
	if assetChecksumURL != "" && assetName != "" {
		expectedChecksum = fetchChecksumForAsset(r.Context(), client, assetChecksumURL, assetName)
	} else if checksumsURL != "" && assetName != "" {
		expectedChecksum = fetchChecksumForAsset(r.Context(), client, checksumsURL, assetName)
	}

	result := map[string]any{
		"current_version":          current,
		"latest_version":           latest,
		"update_available":         updateAvailable,
		"release_name":             release.Name,
		"release_notes":            release.Body,
		"release_url":              release.HTMLURL,
		"download_url":             downloadURL,
		"download_size":            assetSize,
		"checksum":                 expectedChecksum,
		"platform":                 runtime.GOOS + "/" + runtime.GOARCH,
		"in_app_update_supported":  runtime.GOOS == "linux" || runtime.GOOS == "darwin",
		"in_app_restart_supported": runtime.GOOS != "windows",
		"update_instructions": func() string {
			switch runtime.GOOS {
			case "darwin":
				return "SAGE can download, verify, and install the signed app update in the background."
			case "windows":
				return "Download the signed installer, fully quit SAGE, install it, then open SAGE again."
			default:
				return "SAGE can install this verified update in the app."
			}
		}(),
	}

	// Detect an out-of-band update (e.g. drag-and-drop in Finder): the serve
	// daemon survives the GUI quit, so the binary on disk may already be newer
	// than this running process. When the versions differ, the UI should offer
	// a restart instead of a re-download.
	if diskVer := runningBinaryDiskVersion(r.Context()); restartRequired(current, diskVer) {
		result["restart_required"] = true
		result["disk_version"] = diskVer
	}

	writeJSONResp(w, http.StatusOK, result)
}

// runningBinaryDiskVersion returns the version reported by the binary currently
// on disk at this process's executable path, or "" if it cannot be determined.
func runningBinaryDiskVersion(ctx context.Context) string {
	execPath, err := os.Executable()
	if err != nil {
		return ""
	}
	if resolved, rerr := filepath.EvalSymlinks(execPath); rerr == nil {
		execPath = resolved
	}
	return diskBinaryVersion(ctx, execPath)
}

// diskBinaryVersion runs binPath with the "version" arg and parses the version
// from its output. Returns "" on any failure — callers treat that as "unknown".
func diskBinaryVersion(ctx context.Context, binPath string) string {
	ctx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, binPath, "version").Output() // #nosec G204 -- binPath is this process's own executable
	if err != nil {
		return ""
	}
	return parseVersionOutput(string(out))
}

// parseVersionOutput extracts the version from sage-gui's version line,
// e.g. "sage-gui v10.4.4 (commit abc1234, built 2026-06-11)".
// Returns "" if the output doesn't look like that.
func parseVersionOutput(out string) string {
	line := strings.TrimSpace(out)
	if idx := strings.IndexByte(line, '\n'); idx >= 0 {
		line = strings.TrimSpace(line[:idx])
	}
	fields := strings.Fields(line)
	if len(fields) < 2 || fields[0] != "sage-gui" {
		return ""
	}
	return fields[1]
}

// restartRequired reports whether the on-disk binary differs from the running
// version (i.e. an update landed on disk but the daemon is still the old
// binary). Unknown or dev versions never require a restart.
func restartRequired(running, disk string) bool {
	if running == "" || disk == "" || running == "dev" || disk == "dev" {
		return false
	}
	return strings.TrimPrefix(running, "v") != strings.TrimPrefix(disk, "v")
}

// handleApplyUpdate kicks off an async download-and-replace of the sage-gui binary.
// Progress is streamed to the dashboard via SSE events so the user sees each step.
func (h *DashboardHandler) handleApplyUpdate(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "updates can only be installed from an authenticated CEREBRUM session")
		return
	}
	if runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		writeError(w, http.StatusBadRequest, "This platform uses the signed release installer. Fully quit SAGE, install the release, then reopen it.")
		return
	}
	var body struct {
		DownloadURL string `json:"download_url"`
		Checksum    string `json:"checksum"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DownloadURL == "" {
		writeError(w, http.StatusBadRequest, "download_url required")
		return
	}
	if len(body.Checksum) != sha256.Size*2 {
		writeError(w, http.StatusBadRequest, "this update has no trusted SHA-256 checksum; use the release installer instead")
		return
	}
	if _, err := hex.DecodeString(body.Checksum); err != nil {
		writeError(w, http.StatusBadRequest, "invalid SHA-256 checksum")
		return
	}

	// Reject path traversal in URL
	if strings.Contains(body.DownloadURL, "..") {
		writeError(w, http.StatusBadRequest, "invalid download URL")
		return
	}

	// Validate the URL is from GitHub releases
	if !strings.HasPrefix(body.DownloadURL, "https://github.com/"+githubOwner+"/"+githubRepo+"/releases/") {
		writeError(w, http.StatusBadRequest, "invalid download URL — must be a GitHub release")
		return
	}

	// Get current binary path (validate before going async)
	execPath, err := os.Executable()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot determine binary path: "+err.Error())
		return
	}
	execPath, err = filepath.EvalSymlinks(execPath)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot resolve binary path: "+err.Error())
		return
	}
	if pending := PendingUpdateVersion(execPath); pending != "" {
		writeError(w, http.StatusConflict, "update "+pending+" is already installed and waiting for a restart; restart SAGE before installing another update")
		return
	}
	if !h.UpdateInProgress.CompareAndSwap(false, true) {
		writeError(w, http.StatusConflict, "an update is already in progress")
		return
	}
	// Replace any terminal state from a previous attempt before replying. The
	// durable status poll can run immediately after this response; leaving the
	// old "complete" state visible for even one tick made the UI falsely claim
	// the newly queued update was already installed.
	h.sendUpdateProgress("queued", "active", "Update queued — preparing the verified download...")

	// Return immediately — the heavy work happens in a goroutine with SSE progress
	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"status":  "started",
		"message": "Update started — follow progress in the dashboard.",
	})

	// Run download + install async, broadcasting progress via SSE
	h.runBackground(func(ctx context.Context) {
		defer h.UpdateInProgress.Store(false)
		h.performUpdate(ctx, body.DownloadURL, body.Checksum, execPath)
	})
}

// sendUpdateProgress broadcasts an SSE update event with step/status info.
func (h *DashboardHandler) sendUpdateProgress(step, status, message string) {
	h.updateStateMu.Lock()
	h.updateState = map[string]string{"step": step, "status": status, "message": message}
	h.updateStateMu.Unlock()
	if h.SSE == nil {
		return
	}
	h.SSE.Broadcast(SSEEvent{
		Type: EventUpdate,
		Data: map[string]string{
			"step":    step,
			"status":  status,
			"message": message,
		},
	})
}

func (h *DashboardHandler) handleGetUpdateStatus(w http.ResponseWriter, _ *http.Request) {
	h.updateStateMu.RLock()
	state := make(map[string]string, len(h.updateState))
	for key, value := range h.updateState {
		state[key] = value
	}
	h.updateStateMu.RUnlock()
	writeJSONResp(w, http.StatusOK, map[string]any{
		"in_progress": h.UpdateInProgress.Load(),
		"state":       state,
	})
}

// performUpdate does the actual download, checksum, extraction, and binary replacement.
// It broadcasts progress via SSE at each step.
func (h *DashboardHandler) performUpdate(ctx context.Context, downloadURL, checksum, execPath string) {
	// Step 1: Download
	h.sendUpdateProgress("download", "active", "Downloading update from GitHub...")

	// SSRF defence: re-validate the URL at the download site even though
	// handleApplyUpdate already checks it. CodeQL can't trace the value
	// across the goroutine boundary, and defence-in-depth is cheap.
	// The URL must be HTTPS and the host must be on a tight allowlist of
	// GitHub-owned release-asset hosts.
	parsedURL, err := url.Parse(downloadURL)
	if err != nil || parsedURL.Scheme != "https" {
		h.sendUpdateProgress("download", "error", "Invalid download URL")
		return
	}
	allowedHosts := map[string]bool{
		"github.com":                           true,
		"objects.githubusercontent.com":        true,
		"release-assets.githubusercontent.com": true,
	}
	if !allowedHosts[parsedURL.Host] {
		h.sendUpdateProgress("download", "error", "Download URL host not allowed")
		return
	}

	client := &http.Client{
		Timeout: 5 * time.Minute,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if req.URL.Scheme != "https" || !allowedHosts[req.URL.Host] {
				return fmt.Errorf("redirect to non-GitHub URL blocked")
			}
			return nil
		},
	}
	dlReq, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		h.sendUpdateProgress("download", "error", "Invalid download URL")
		return
	}
	resp, err := client.Do(dlReq)
	if err != nil {
		h.sendUpdateProgress("download", "error", "Download failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		h.sendUpdateProgress("download", "error", fmt.Sprintf("GitHub returned HTTP %d", resp.StatusCode))
		return
	}

	// Save to temp file while computing checksum
	tempPattern := "sage-archive-*"
	if runtime.GOOS == "darwin" {
		tempPattern = "sage-archive-*.dmg"
	}
	archiveTmp, err := os.CreateTemp("", tempPattern)
	if err != nil {
		h.sendUpdateProgress("download", "error", "Failed to create temp file")
		return
	}
	defer os.Remove(archiveTmp.Name())

	hasher := sha256.New()
	written, copyErr := io.Copy(archiveTmp, io.TeeReader(io.LimitReader(resp.Body, 500<<20), hasher))
	if copyErr != nil {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("download", "error", "Download interrupted: "+copyErr.Error())
		return
	}

	h.sendUpdateProgress("download", "done", fmt.Sprintf("Downloaded %s", formatBytes(written)))

	// Step 2: Verify checksum
	actualChecksum := hex.EncodeToString(hasher.Sum(nil))
	h.sendUpdateProgress("verify", "active", "Verifying SHA-256 checksum...")
	if !strings.EqualFold(actualChecksum, checksum) {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("verify", "error", "Checksum mismatch — archive may be corrupted")
		return
	}
	h.sendUpdateProgress("verify", "done", "Checksum verified")

	// Protect the irreplaceable vault key before either the app-bundle or
	// standalone-binary installer touches the current installation.
	if h.VaultKeyPath != "" {
		if vkData, vkErr := os.ReadFile(h.VaultKeyPath); vkErr == nil {
			backupDir := filepath.Join(filepath.Dir(h.VaultKeyPath), "backups")
			_ = os.MkdirAll(backupDir, 0700)
			vaultBackup := filepath.Join(backupDir, "vault-pre-update.key")
			_ = os.WriteFile(vaultBackup, vkData, 0600) //nolint:gosec // trusted local vault backup
		}
	}

	if runtime.GOOS == "darwin" {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("extract", "active", "Opening signed SAGE app update...")
		stagedVersion, installErr := installDarwinAppUpdate(ctx, archiveTmp.Name(), execPath)
		if installErr != nil {
			h.sendUpdateProgress("install", "error", installErrorMessage("Failed to install signed app update", installErr, downloadURL))
			return
		}
		h.sendUpdateProgress("extract", "done", "Signed app verified")
		h.sendUpdateProgress("install", "done", "SAGE "+stagedVersion+" installed — restart SAGE to apply")
		h.sendUpdateProgress("complete", "done", "ready_to_restart")
		return
	}

	// Step 3: Extract
	h.sendUpdateProgress("extract", "active", "Extracting sage-gui binary...")
	if _, seekErr := archiveTmp.Seek(0, io.SeekStart); seekErr != nil {
		_ = archiveTmp.Close()
		h.sendUpdateProgress("extract", "error", "Failed to read archive")
		return
	}

	newBinary, err := extractBinaryFromTarGz(archiveTmp, "sage-gui")
	_ = archiveTmp.Close()
	if err != nil {
		h.sendUpdateProgress("extract", "error", "Extraction failed: "+err.Error())
		return
	}
	defer os.Remove(newBinary)

	h.sendUpdateProgress("extract", "done", "Binary extracted")
	stagedVersion := diskBinaryVersion(context.Background(), newBinary)
	if stagedVersion == "" || stagedVersion == "dev" {
		h.sendUpdateProgress("extract", "error", "The verified archive did not contain a runnable release build. Use the signed release installer instead.")
		return
	}

	// Step 4: Install
	h.sendUpdateProgress("install", "active", "Installing new binary...")

	if err := installPendingBinary(execPath, newBinary, stagedVersion); err != nil {
		h.sendUpdateProgress("install", "error", installErrorMessage("Failed to install", err, downloadURL))
		return
	}

	h.sendUpdateProgress("install", "done", "Update installed — restart SAGE to apply")
	h.sendUpdateProgress("complete", "done", "ready_to_restart")
}

const pendingUpdateSuffix = ".update-pending"

// installPendingBinary stages the verified executable in the destination
// directory, durably snapshots the current executable, then atomically replaces
// it. The backup remains until the replacement process proves its own boot ID
// healthy; early exec/startup failure can therefore roll back automatically.
func installPendingBinary(execPath, extractedPath, version string) error {
	if pending := PendingUpdateVersion(execPath); pending != "" {
		return fmt.Errorf("update %s is still pending boot confirmation", pending)
	}
	dir := filepath.Dir(execPath)
	mode := os.FileMode(0755)
	if info, err := os.Stat(execPath); err == nil {
		mode = info.Mode()
	}
	staged, err := os.CreateTemp(dir, ".sage-gui-staged-*")
	if err != nil {
		return fmt.Errorf("stage beside installed binary: %w", err)
	}
	stagedPath := staged.Name()
	defer func() { _ = os.Remove(stagedPath) }()
	src, err := os.Open(extractedPath) //nolint:gosec // verified updater temp path
	if err != nil {
		_ = staged.Close()
		return fmt.Errorf("open verified binary: %w", err)
	}
	_, copyErr := io.Copy(staged, io.LimitReader(src, 500<<20))
	closeSrcErr := src.Close()
	if copyErr == nil {
		copyErr = closeSrcErr
	}
	if copyErr == nil {
		copyErr = staged.Chmod(mode)
	}
	if copyErr == nil {
		copyErr = staged.Sync()
	}
	closeStageErr := staged.Close()
	if copyErr == nil {
		copyErr = closeStageErr
	}
	if copyErr != nil {
		return fmt.Errorf("stage verified binary: %w", copyErr)
	}

	backupPath := execPath + ".old"
	markerPath := platformPendingUpdateMarker(execPath)
	_ = os.Remove(backupPath)
	if err = copyFileDurable(execPath, backupPath, mode); err != nil {
		return fmt.Errorf("preserve rollback binary: %w", err)
	}
	markerData := []byte(strings.TrimSpace(version) + "\n")
	if err = writeFileAtomicDurable(markerPath, markerData, 0600); err != nil {
		_ = os.Remove(backupPath)
		return fmt.Errorf("record pending update: %w", err)
	}
	if err = os.Rename(stagedPath, execPath); err != nil {
		_ = os.Remove(markerPath)
		_ = os.Remove(backupPath)
		return fmt.Errorf("atomically replace installed binary: %w", err)
	}
	return syncDirectory(dir)
}

func copyFileDurable(srcPath, dstPath string, mode os.FileMode) error {
	src, err := os.Open(srcPath) //nolint:gosec // trusted installed executable
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode) //nolint:gosec // sibling rollback path
	if err != nil {
		return err
	}
	_, err = io.Copy(dst, io.LimitReader(src, 500<<20))
	if err == nil {
		err = dst.Sync()
	}
	closeErr := dst.Close()
	if err == nil {
		err = closeErr
	}
	return err
}

func writeFileAtomicDurable(path string, data []byte, mode os.FileMode) error {
	tmp, err := os.CreateTemp(filepath.Dir(path), ".sage-update-marker-*")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Chmod(mode); err == nil {
		_, err = tmp.Write(data)
	}
	if err == nil {
		err = tmp.Sync()
	}
	closeErr := tmp.Close()
	if err == nil {
		err = closeErr
	}
	if err == nil {
		err = os.Rename(tmpPath, path)
	}
	if err != nil {
		return err
	}
	return syncDirectory(filepath.Dir(path))
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir) //nolint:gosec // trusted executable directory
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

// PendingUpdateVersion reports the release awaiting boot confirmation.
func PendingUpdateVersion(execPath string) string {
	paths := []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix}
	for i, path := range paths {
		if i > 0 && path == paths[0] {
			continue
		}
		data, err := os.ReadFile(path) //nolint:gosec // trusted executable/app sibling
		if err == nil {
			return strings.TrimSpace(string(data))
		}
	}
	return ""
}

// ConfirmPendingUpdate removes rollback state only after the replacement node
// has served a new, matching boot identity and version.
func ConfirmPendingUpdate(execPath string) error {
	if PendingUpdateVersion(execPath) == "" {
		return nil
	}
	if handled, err := confirmPendingAppBundle(execPath); handled {
		return err
	}
	// Make the installed executable + rollback copy durable before committing.
	// Keep .old as a one-generation recovery artifact; the next update replaces
	// it only after confirming no update is currently pending.
	if err := syncDirectory(filepath.Dir(execPath)); err != nil {
		return err
	}
	for _, path := range []string{platformPendingUpdateMarker(execPath), execPath + pendingUpdateSuffix} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	// A post-unlink fsync failure is not a boot failure: the replacement already
	// proved healthy, and a crash may at worst leave the marker to reconfirm.
	_ = syncDirectory(filepath.Dir(execPath))
	return nil
}

// RollbackPendingUpdate atomically restores the previous executable after an
// exec or early-boot failure. It returns false when no update is pending.
func RollbackPendingUpdate(execPath string) (bool, error) {
	if PendingUpdateVersion(execPath) == "" {
		return false, nil
	}
	if handled, rolledBack, err := rollbackPendingAppBundle(execPath); handled {
		return rolledBack, err
	}
	backupPath := execPath + ".old"
	if _, err := os.Stat(backupPath); err != nil {
		return false, fmt.Errorf("pending update has no rollback binary: %w", err)
	}
	if err := os.Rename(backupPath, execPath); err != nil {
		return false, err
	}
	_ = os.Remove(platformPendingUpdateMarker(execPath))
	_ = os.Remove(execPath + pendingUpdateSuffix)
	if err := syncDirectory(filepath.Dir(execPath)); err != nil {
		// The atomic rename already restored the old executable. Tell the caller
		// to relaunch it even if the durability barrier could not be confirmed.
		return true, fmt.Errorf("rollback binary restored but directory sync failed: %w", err)
	}
	return true, nil
}

// handleRestart asks the main serve lifecycle to drain and restart. The handler
// never execs from an HTTP goroutine: that skipped defers and could leave
// consensus, stores, listeners, and sidecars in an indeterminate state.
func (h *DashboardHandler) handleRestart(w http.ResponseWriter, r *http.Request) {
	if !h.isCEREBRUMOperatorRequest(r) {
		writeError(w, http.StatusForbidden, "restart can only be requested from an authenticated CEREBRUM session")
		return
	}
	if h.UpdateInProgress.Load() {
		writeError(w, http.StatusConflict, "wait for the update installation to finish before restarting")
		return
	}
	if !restartInProcessSupported() || h.RequestRestart == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": false, "restart_required": true,
			"message": "The update is installed. Fully quit SAGE and open it again to finish.",
		})
		return
	}
	if err := h.RequestRestart(); err != nil {
		writeError(w, http.StatusServiceUnavailable, "SAGE could not begin a clean restart: "+err.Error())
		return
	}
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"ok": true, "status": "draining", "boot_id": h.BootID,
		"message": "SAGE is closing cleanly, then restarting…",
	})
}

// isPermissionDenied reports whether err is a permission-style failure
// (EPERM/EACCES). On macOS, a TCC "App Management" denial surfaces as
// "operation not permitted" (EPERM) when renaming inside /Applications/SAGE.app.
func isPermissionDenied(err error) bool {
	return errors.Is(err, os.ErrPermission) ||
		errors.Is(err, syscall.EPERM) ||
		errors.Is(err, syscall.EACCES)
}

// installErrorMessage maps an install-step failure to a user-facing SSE message.
// On macOS, permission errors get actionable TCC guidance instead of a dead end.
func installErrorMessage(action string, err error, downloadURL string) string {
	if runtime.GOOS == "darwin" && isPermissionDenied(err) {
		return fmt.Sprintf(
			"macOS blocked SAGE from modifying its app bundle (%s). "+
				"Either: (a) grant SAGE \"App Management\" in System Settings → Privacy & Security → App Management, "+
				"fully quit SAGE from the menu bar, relaunch, and retry the update; "+
				"or (b) download the DMG from %s, drag-replace SAGE in Finder, then restart SAGE.",
			err.Error(), releasePageURL(downloadURL))
	}
	return action + ": " + err.Error()
}

// releasePageURL derives the GitHub release page URL from a release-asset
// download URL (".../releases/download/<tag>/<asset>" → ".../releases/tag/<tag>").
// Falls back to the repo's latest-release page if the URL doesn't match that shape.
func releasePageURL(downloadURL string) string {
	const marker = "/releases/download/"
	if idx := strings.Index(downloadURL, marker); idx >= 0 {
		rest := downloadURL[idx+len(marker):]
		if slash := strings.IndexByte(rest, '/'); slash > 0 {
			return downloadURL[:idx] + "/releases/tag/" + rest[:slash]
		}
	}
	return fmt.Sprintf("https://github.com/%s/%s/releases/latest", githubOwner, githubRepo)
}

// findUpdateAssetName returns the release asset for a target platform.
func findUpdateAssetName(version, goos, goarch string) string {
	if goos == "darwin" {
		archLabel := goarch
		if goarch == "amd64" {
			archLabel = "x86_64"
		}
		return fmt.Sprintf("SAGE-v%s-macOS-%s.dmg", version, archLabel)
	}
	ext := "tar.gz"
	if goos == "windows" {
		ext = "zip"
	}
	return fmt.Sprintf("sage-gui_%s_%s_%s.%s", version, goos, goarch, ext)
}

// extractBinaryFromTarGz extracts a named binary from a .tar.gz stream to a temp file.
func extractBinaryFromTarGz(reader io.Reader, binaryName string) (string, error) {
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	for {
		header, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("tar: %w", err)
		}

		// Match the binary name (could be in a subdirectory)
		base := filepath.Base(header.Name)
		if base == binaryName && header.Typeflag == tar.TypeReg {
			tmpFile, err := os.CreateTemp("", "sage-update-*")
			if err != nil {
				return "", err
			}
			if _, err := io.Copy(tmpFile, io.LimitReader(tr, 500<<20)); err != nil { // 500MB max
				_ = tmpFile.Close()
				_ = os.Remove(tmpFile.Name())
				return "", err
			}
			_ = tmpFile.Close()
			_ = os.Chmod(tmpFile.Name(), 0755)
			return tmpFile.Name(), nil
		}
	}

	return "", fmt.Errorf("binary %q not found in archive", binaryName)
}

// semverGreater returns true if version a is strictly greater than version b.
// Handles versions like "3.6.0", "3.10.0", "3.6.0-rc1" (pre-release ignored).
func semverGreater(a, b string) bool {
	aParts := parseSemver(a)
	bParts := parseSemver(b)
	for i := 0; i < 3; i++ {
		if aParts[i] > bParts[i] {
			return true
		}
		if aParts[i] < bParts[i] {
			return false
		}
	}
	return false // equal
}

// parseSemver extracts [major, minor, patch] from a version string.
// Strips any pre-release suffix (e.g., "3.6.0-rc1" -> [3, 6, 0]).
func parseSemver(v string) [3]int {
	v = strings.TrimPrefix(v, "v")
	// Strip pre-release suffix
	if idx := strings.IndexAny(v, "-+"); idx >= 0 {
		v = v[:idx]
	}
	parts := strings.SplitN(v, ".", 3)
	var result [3]int
	for i := 0; i < 3 && i < len(parts); i++ {
		n, err := strconv.Atoi(parts[i])
		if err == nil {
			result[i] = n
		}
	}
	return result
}

// fetchChecksumForAsset downloads checksums.txt and returns the SHA-256 checksum
// for the given asset name. Returns empty string if not found.
func fetchChecksumForAsset(ctx context.Context, client *http.Client, checksumsURL, assetName string) string {
	req, err := http.NewRequestWithContext(ctx, "GET", checksumsURL, nil)
	if err != nil {
		return ""
	}
	resp, err := client.Do(req)
	if err != nil || resp.StatusCode != 200 {
		if resp != nil {
			resp.Body.Close()
		}
		return ""
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1MB max
	if err != nil {
		return ""
	}

	// checksums.txt format: "<sha256>  <filename>" (two spaces)
	for _, line := range strings.Split(string(body), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		parts := strings.Fields(line)
		if len(parts) >= 2 && parts[1] == assetName {
			return parts[0]
		}
	}
	return ""
}

// formatBytes returns a human-readable byte count (e.g. "15.2 MB").
func formatBytes(b int64) string {
	if b < 1024 {
		return fmt.Sprintf("%d B", b)
	}
	if b < 1048576 {
		return fmt.Sprintf("%.0f KB", float64(b)/1024)
	}
	return fmt.Sprintf("%.1f MB", float64(b)/1048576)
}
