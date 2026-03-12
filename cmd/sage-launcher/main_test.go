package main

import (
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"
)

func TestSageDir(t *testing.T) {
	d := sageDir()
	if d == "" {
		t.Fatal("sageDir returned empty string")
	}
	if !filepath.IsAbs(d) {
		t.Fatalf("sageDir returned relative path: %s", d)
	}
	if filepath.Base(d) != ".sage" {
		t.Fatalf("sageDir should end with .sage, got: %s", d)
	}
}

func TestFindSageGUI(t *testing.T) {
	// findSageGUI should return empty string if sage-gui isn't found
	// (it might find one in PATH on dev machines, so just verify it doesn't panic)
	_ = findSageGUI()
}

func TestHealthOK_Running(t *testing.T) {
	// Create a test server that mimics /health
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"ok"}`)) //nolint:errcheck
	}))
	defer srv.Close()

	// healthOK checks hardcoded localhost:8080, so we can't easily redirect it.
	// Instead, test the HTTP client logic directly.
	client := &http.Client{Timeout: 2 * 1e9} // 2s
	resp, err := client.Get(srv.URL)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}
}

func TestHealthOK_NotRunning(t *testing.T) {
	// When nothing is listening, healthOK should return false
	// (This test assumes nothing is on port 8080 — may need to skip in CI)
	ok := healthOK()
	// Can't assert false because sage might actually be running on dev machine.
	// Just verify it doesn't panic.
	_ = ok
}

func TestProcessAlive(t *testing.T) {
	// Our own PID should be alive
	if !processAlive(os.Getpid()) {
		t.Fatal("processAlive returned false for own PID")
	}

	// PID 0 or very large PID should not be alive
	if processAlive(999999999) {
		t.Fatal("processAlive returned true for non-existent PID")
	}
}

func TestIsRunning_WithPIDFile(t *testing.T) {
	// Write a PID file with a dead PID
	tmpDir := t.TempDir()
	pidFile := filepath.Join(tmpDir, "sage.pid")
	os.WriteFile(pidFile, []byte(strconv.Itoa(999999999)), 0644) //nolint:errcheck

	// isRunning checks the real sageDir, but processAlive for a dead PID returns false
	// Just verify isRunning doesn't panic
	_ = isRunning()
}

func TestOpenBrowser(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping browser test in short mode")
	}
	// Just verify openBrowser doesn't panic with an invalid URL
	// (it fires and forgets, so errors are silently ignored)
	openBrowser("http://127.0.0.1:1/nonexistent")
}
