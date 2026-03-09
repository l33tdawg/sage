package web

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
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
	req.Header.Set("User-Agent", "sage-lite/"+current)

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
	updateAvailable := latest != strings.TrimPrefix(current, "v") && current != "dev"

	// Find the right asset for this platform
	assetName := findAssetName(latest)
	var downloadURL string
	var assetSize int64
	for _, a := range release.Assets {
		if a.Name == assetName {
			downloadURL = a.BrowserDownloadURL
			assetSize = a.Size
			break
		}
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"current_version":  current,
		"latest_version":   latest,
		"update_available": updateAvailable,
		"release_name":     release.Name,
		"release_notes":    release.Body,
		"release_url":      release.HTMLURL,
		"download_url":     downloadURL,
		"download_size":    assetSize,
		"platform":         runtime.GOOS + "/" + runtime.GOARCH,
	})
}

// handleApplyUpdate downloads and replaces the sage-lite binary.
func (h *DashboardHandler) handleApplyUpdate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		DownloadURL string `json:"download_url"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.DownloadURL == "" {
		writeError(w, http.StatusBadRequest, "download_url required")
		return
	}

	// Validate the URL is from GitHub releases
	if !strings.HasPrefix(body.DownloadURL, "https://github.com/"+githubOwner+"/"+githubRepo+"/releases/") {
		writeError(w, http.StatusBadRequest, "invalid download URL — must be a GitHub release")
		return
	}

	// Get current binary path
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

	// Download the archive
	client := &http.Client{Timeout: 5 * time.Minute}
	resp, err := client.Get(body.DownloadURL)
	if err != nil {
		writeError(w, http.StatusBadGateway, "download failed: "+err.Error())
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		writeError(w, http.StatusBadGateway, fmt.Sprintf("download returned %d", resp.StatusCode))
		return
	}

	// Extract sage-lite binary from tar.gz
	newBinary, err := extractBinaryFromTarGz(resp.Body, "sage-lite")
	if err != nil {
		writeError(w, http.StatusInternalServerError, "extraction failed: "+err.Error())
		return
	}
	defer os.Remove(newBinary)

	// Replace the binary atomically
	// On Unix, we can unlink the old file and rename the new one in place.
	// The running process keeps the old inode open.
	backupPath := execPath + ".old"
	os.Remove(backupPath) // remove any previous backup

	// Rename current -> backup
	if err := os.Rename(execPath, backupPath); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to backup current binary: "+err.Error())
		return
	}

	// Move new -> current
	if err := os.Rename(newBinary, execPath); err != nil {
		// Rollback
		os.Rename(backupPath, execPath)
		writeError(w, http.StatusInternalServerError, "failed to install new binary: "+err.Error())
		return
	}

	// Copy permissions from backup
	if info, err := os.Stat(backupPath); err == nil {
		os.Chmod(execPath, info.Mode())
	} else {
		os.Chmod(execPath, 0755)
	}

	// Clean up backup
	os.Remove(backupPath)

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":               true,
		"message":          "Update installed. Restart SAGE to apply.",
		"restart_required": true,
	})
}

// handleRestart gracefully restarts sage-lite by re-exec'ing itself.
func (h *DashboardHandler) handleRestart(w http.ResponseWriter, r *http.Request) {
	execPath, err := os.Executable()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "cannot determine binary path")
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "Restarting SAGE...",
	})

	// Flush the response
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// Give the response time to reach the client
	go func() {
		time.Sleep(500 * time.Millisecond)
		// Re-exec the binary with the same args
		syscall.Exec(execPath, os.Args, os.Environ()) //nolint:errcheck
	}()
}

// findAssetName returns the expected GoReleaser archive name for the current platform.
func findAssetName(version string) string {
	goos := runtime.GOOS
	goarch := runtime.GOARCH
	ext := "tar.gz"
	if goos == "windows" {
		ext = "zip"
	}
	return fmt.Sprintf("sage-lite_%s_%s_%s.%s", version, goos, goarch, ext)
}

// extractBinaryFromTarGz extracts a named binary from a .tar.gz stream to a temp file.
func extractBinaryFromTarGz(reader io.Reader, binaryName string) (string, error) {
	gz, err := gzip.NewReader(reader)
	if err != nil {
		return "", fmt.Errorf("gzip: %w", err)
	}
	defer gz.Close()

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
			if _, err := io.Copy(tmpFile, tr); err != nil {
				tmpFile.Close()
				os.Remove(tmpFile.Name())
				return "", err
			}
			tmpFile.Close()
			os.Chmod(tmpFile.Name(), 0755)
			return tmpFile.Name(), nil
		}
	}

	return "", fmt.Errorf("binary %q not found in archive", binaryName)
}
