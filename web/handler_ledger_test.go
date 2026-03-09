package web

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandleGetLedgerStatus_Disabled(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("GET", "/v1/dashboard/settings/ledger", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, false, resp["enabled"])
}

func TestHandleEnableLedger(t *testing.T) {
	h, _ := newTestHandler(t)
	// Register routes first (no auth middleware), then set VaultKeyPath for the handler logic.
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	body, _ := json.Marshal(map[string]string{"passphrase": "test-pass-123"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.NotEmpty(t, resp["recovery_key"])
	assert.True(t, h.Encrypted)
}

func TestHandleEnableLedger_MissingPassphrase(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	body, _ := json.Marshal(map[string]string{})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleEnableLedger_AlreadyEnabled(t *testing.T) {
	h, _ := newTestHandler(t)
	h.Encrypted = true
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"passphrase": "test"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusConflict, w.Code)
}

func TestHandleGetLedgerStatus_Enabled(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable first
	body, _ := json.Marshal(map[string]string{"passphrase": "test-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Check status
	req = httptest.NewRequest("GET", "/v1/dashboard/settings/ledger", nil)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["enabled"])
	assert.Equal(t, "AES-256-GCM", resp["algorithm"])
	assert.Equal(t, "Argon2id", resp["kdf"])
}

func TestHandleChangePassphrase(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable
	body, _ := json.Marshal(map[string]string{"passphrase": "old-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Change passphrase
	body, _ = json.Marshal(map[string]string{"old_passphrase": "old-pass", "new_passphrase": "new-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/change-passphrase", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
}

func TestHandleChangePassphrase_WrongOld(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable
	body, _ := json.Marshal(map[string]string{"passphrase": "correct-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Wrong old passphrase
	body, _ = json.Marshal(map[string]string{"old_passphrase": "wrong", "new_passphrase": "new-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/change-passphrase", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleDisableLedger(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable
	body, _ := json.Marshal(map[string]string{"passphrase": "my-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Disable
	body, _ = json.Marshal(map[string]string{"passphrase": "my-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.NotEmpty(t, resp["warning"])
	assert.False(t, h.Encrypted)
}

func TestHandleDisableLedger_WrongPassphrase(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable
	body, _ := json.Marshal(map[string]string{"passphrase": "my-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Disable with wrong passphrase
	body, _ = json.Marshal(map[string]string{"passphrase": "wrong"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.True(t, h.Encrypted) // Should still be enabled
}

func TestHandleDisableLedger_NotEnabled(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"passphrase": "test"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleChangePassphrase_NotEnabled(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"old_passphrase": "old", "new_passphrase": "new"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/change-passphrase", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleEnableLedger_NoVaultPath(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	// VaultKeyPath deliberately left empty

	body, _ := json.Marshal(map[string]string{"passphrase": "test"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}
