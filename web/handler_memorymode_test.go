package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleGetMemoryMode_Default(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/settings/memory-mode", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "full", resp["mode"], "default mode should be 'full'")
}

func TestHandleSaveMemoryMode_Full(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"mode": "full"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.Equal(t, "full", resp["mode"])
}

func TestHandleSaveMemoryMode_Bookend(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"mode": "bookend"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.Equal(t, "bookend", resp["mode"])
}

func TestHandleSaveMemoryMode_OnDemand(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"mode": "on-demand"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.Equal(t, "on-demand", resp["mode"])

	// Read back
	req = httptest.NewRequest("GET", "/v1/dashboard/settings/memory-mode", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "on-demand", resp["mode"])
}

func TestHandleSaveMemoryMode_InvalidMode(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"mode": "turbo"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveMemoryMode_Persistence(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	// Save bookend
	body, _ := json.Marshal(map[string]string{"mode": "bookend"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	// Read back
	req = httptest.NewRequest("GET", "/v1/dashboard/settings/memory-mode", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, "bookend", resp["mode"], "mode should persist after save")
}

func TestHandleSaveMemoryMode_FlagFile(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	// Set up a temp SAGE_HOME so the handler writes the flag file there
	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)

	body, _ := json.Marshal(map[string]string{"mode": "bookend"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["flag_synced"], "flag file should be synced")

	// Verify flag file contents
	flagData, err := os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	require.NoError(t, err)
	assert.Equal(t, "bookend", string(flagData))
}

func TestHandleSaveMemoryMode_FlagFileUpdatesOnModeChange(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	sageHome := t.TempDir()
	t.Setenv("SAGE_HOME", sageHome)

	// Set bookend
	body, _ := json.Marshal(map[string]string{"mode": "bookend"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	flagData, _ := os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	assert.Equal(t, "bookend", string(flagData))

	// Switch back to full
	body, _ = json.Marshal(map[string]string{"mode": "full"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusOK, w.Code)

	flagData, _ = os.ReadFile(filepath.Join(sageHome, "memory_mode"))
	assert.Equal(t, "full", string(flagData), "flag should update when mode changes back to full")
}

func TestHandleSaveMemoryMode_InvalidJSON(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("POST", "/v1/dashboard/settings/memory-mode", bytes.NewReader([]byte("not json")))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
