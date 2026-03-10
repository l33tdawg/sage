package web

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"text/template"
)

const (
	launchdLabel = "com.sage.personal"
	launchdFile  = "com.sage.personal.plist"
)

// launchdPlistTemplate is the macOS launchd agent plist for auto-start.
var launchdPlistTemplate = template.Must(template.New("plist").Parse(`<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>{{.Label}}</string>
    <key>ProgramArguments</key>
    <array>
        <string>{{.BinaryPath}}</string>
        <string>serve</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <false/>
    <key>StandardOutPath</key>
    <string>{{.LogPath}}</string>
    <key>StandardErrorPath</key>
    <string>{{.LogPath}}</string>
    <key>WorkingDirectory</key>
    <string>{{.SageHome}}</string>
</dict>
</plist>
`))

type autostartResponse struct {
	Enabled   bool   `json:"enabled"`
	Supported bool   `json:"supported"`
	Platform  string `json:"platform"`
	Message   string `json:"message,omitempty"`
}

// handleGetAutostart returns whether open-at-login is enabled.
func (h *DashboardHandler) handleGetAutostart(w http.ResponseWriter, r *http.Request) {
	supported := runtime.GOOS == "darwin" || runtime.GOOS == "windows"
	enabled := false

	switch runtime.GOOS {
	case "darwin":
		enabled = launchdPlistExists()
	case "windows":
		// Future: check registry
	}

	writeJSONResp(w, http.StatusOK, autostartResponse{
		Enabled:   enabled,
		Supported: supported,
		Platform:  runtime.GOOS,
	})
}

// handleSetAutostart enables or disables open-at-login.
func (h *DashboardHandler) handleSetAutostart(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Enabled bool `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request: "+err.Error())
		return
	}

	switch runtime.GOOS {
	case "darwin":
		var err error
		if req.Enabled {
			err = installLaunchdPlist()
		} else {
			err = removeLaunchdPlist()
		}
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
	default:
		writeError(w, http.StatusBadRequest, fmt.Sprintf("auto-start not yet supported on %s", runtime.GOOS))
		return
	}

	writeJSONResp(w, http.StatusOK, autostartResponse{
		Enabled:   req.Enabled,
		Supported: true,
		Platform:  runtime.GOOS,
	})
}

// ---- macOS launchd helpers ----

func launchAgentsDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "Library", "LaunchAgents")
}

func launchdPlistPath() string {
	return filepath.Join(launchAgentsDir(), launchdFile)
}

func launchdPlistExists() bool {
	_, err := os.Stat(launchdPlistPath())
	return err == nil
}

func sageBinaryPath() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".sage", "bin", "sage-lite")
}

func installLaunchdPlist() error {
	binPath := sageBinaryPath()
	// Verify the binary exists
	if _, err := os.Stat(binPath); err != nil {
		return fmt.Errorf("sage-lite binary not found at %s — launch SAGE from the app first", binPath)
	}

	dir := launchAgentsDir()
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("create LaunchAgents dir: %w", err)
	}

	home, _ := os.UserHomeDir()
	sageHome := filepath.Join(home, ".sage")
	logPath := filepath.Join(sageHome, "logs", "sage.log")

	// Ensure log dir exists
	os.MkdirAll(filepath.Join(sageHome, "logs"), 0755) //nolint:errcheck

	f, err := os.Create(launchdPlistPath())
	if err != nil {
		return fmt.Errorf("create plist: %w", err)
	}
	defer f.Close()

	data := struct {
		Label      string
		BinaryPath string
		LogPath    string
		SageHome   string
	}{
		Label:      launchdLabel,
		BinaryPath: binPath,
		LogPath:    logPath,
		SageHome:   sageHome,
	}

	if err := launchdPlistTemplate.Execute(f, data); err != nil {
		os.Remove(launchdPlistPath())
		return fmt.Errorf("write plist: %w", err)
	}

	return nil
}

func removeLaunchdPlist() error {
	path := launchdPlistPath()
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil // Already removed
	}
	if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove plist: %w", err)
	}
	return nil
}

