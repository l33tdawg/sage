package web

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

type setFailingPreferences struct {
	PreferencesStore
	err error
}

func (s *setFailingPreferences) SetPreference(context.Context, string, string) error {
	return s.err
}

// loginAfterEnable calls the login endpoint and returns the session cookie.
func loginAfterEnable(t *testing.T, r http.Handler, passphrase string) *http.Cookie {
	t.Helper()
	body, _ := json.Marshal(map[string]string{"passphrase": passphrase})
	req := httptest.NewRequest("POST", "/v1/dashboard/auth/login", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	cookies := w.Result().Cookies()
	require.NotEmpty(t, cookies, "expected session cookie after login")
	return cookies[0]
}

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
	h, s := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, s.SetPreference(context.Background(), recoveryBackupConfirmedPreference, "1"))

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
	assert.True(t, h.Encrypted.Load())
	var cookie *http.Cookie
	for _, candidate := range w.Result().Cookies() {
		if candidate.Name == sessionCookieName {
			cookie = candidate
			break
		}
	}
	require.NotNil(t, cookie, "enabling encryption should authenticate the current dashboard session")

	// A newly generated recovery key must start as not-yet-backed-up even if a
	// stale preference survived from an older vault.
	statusReq := httptest.NewRequest("GET", "/v1/dashboard/settings/ledger", nil)
	statusReq.AddCookie(cookie)
	statusW := httptest.NewRecorder()
	r.ServeHTTP(statusW, statusReq)
	require.Equal(t, http.StatusOK, statusW.Code)
	var statusResp map[string]any
	require.NoError(t, json.Unmarshal(statusW.Body.Bytes(), &statusResp))
	assert.Equal(t, false, statusResp["recovery_backup_confirmed"])

	// The authenticated acknowledgement persists across later status reads.
	confirmReq := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/recovery-key/confirm", nil)
	confirmReq.AddCookie(cookie)
	confirmW := httptest.NewRecorder()
	r.ServeHTTP(confirmW, confirmReq)
	require.Equal(t, http.StatusOK, confirmW.Code)

	statusReq = httptest.NewRequest("GET", "/v1/dashboard/settings/ledger", nil)
	statusReq.AddCookie(cookie)
	statusW = httptest.NewRecorder()
	r.ServeHTTP(statusW, statusReq)
	require.Equal(t, http.StatusOK, statusW.Code)
	require.NoError(t, json.Unmarshal(statusW.Body.Bytes(), &statusResp))
	assert.Equal(t, true, statusResp["recovery_backup_confirmed"])
}

func TestHandleEnableLedgerFailsClosedWhenBackupStatusCannotReset(t *testing.T) {
	h, s := newTestHandler(t)
	h.prefStore = &setFailingPreferences{PreferencesStore: s, err: errors.New("preference write failed")}
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")
	r := testRouter(h)

	body, err := json.Marshal(map[string]string{"passphrase": "test-pass-123"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusInternalServerError, w.Code)
	assert.False(t, h.Encrypted.Load())
	assert.NoFileExists(t, h.VaultKeyPath, "a new vault must not be created with a stale backup-confirmed flag")
}

func TestHandleConfirmRecoveryKeyBackupRequiresEncryption(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/recovery-key/confirm", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleConfirmRecoveryKeyBackupRejectsCrossSiteBrowser(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)

	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/recovery-key/confirm", nil)
	req.Header.Set("Origin", "https://attacker.example")
	req.Header.Set("Sec-Fetch-Site", "cross-site")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleConfirmRecoveryKeyBackupRejectsRemoteCallerWithoutSession(t *testing.T) {
	h, _ := newTestHandler(t)
	h.Encrypted.Store(true)
	r := testRouter(h)

	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/recovery-key/confirm", nil)
	req.RemoteAddr = "192.168.1.9:1234"
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestVaultLoginTriggersScopedProjectionRecovery(t *testing.T) {
	h, _ := newTestHandler(t)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")
	h.RunBackground = func(fn func(context.Context)) { fn(context.Background()) }
	called := 0
	h.ScopedProjectionRebuildFn = func(context.Context) (int, error) {
		called++
		return 3, nil
	}
	r := testRouter(h)

	body, _ := json.Marshal(map[string]string{"passphrase": "test-pass-123"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	_ = loginAfterEnable(t, r, "test-pass-123")
	assert.Equal(t, 1, called, "unlock must retry the canonical scoped projection before readiness can recover")
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
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable first
	body, _ := json.Marshal(map[string]string{"passphrase": "test-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Login to get session cookie
	cookie := loginAfterEnable(t, r, "test-pass")

	// Try to enable again — should get conflict
	body, _ = json.Marshal(map[string]string{"passphrase": "test"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
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

	// Login to get session cookie
	cookie := loginAfterEnable(t, r, "test-pass")

	// Check status
	req = httptest.NewRequest("GET", "/v1/dashboard/settings/ledger", nil)
	req.AddCookie(cookie)
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

	// Login
	cookie := loginAfterEnable(t, r, "old-pass")

	// Change passphrase
	body, _ = json.Marshal(map[string]string{"old_passphrase": "old-pass", "new_passphrase": "new-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/change-passphrase", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
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

	// Login
	cookie := loginAfterEnable(t, r, "correct-pass")

	// Wrong old passphrase
	body, _ = json.Marshal(map[string]string{"old_passphrase": "wrong", "new_passphrase": "new-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/change-passphrase", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleDisableLedger(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable
	body, _ := json.Marshal(map[string]string{"passphrase": "my-pass-123"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Login
	cookie := loginAfterEnable(t, r, "my-pass-123")

	// Disable
	body, _ = json.Marshal(map[string]string{"passphrase": "my-pass-123"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.NotEmpty(t, resp["warning"])
	assert.False(t, h.Encrypted.Load())
}

func TestHandleDisableLedger_WrongPassphrase(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable
	body, _ := json.Marshal(map[string]string{"passphrase": "my-pass-123"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Login
	cookie := loginAfterEnable(t, r, "my-pass-123")

	// Disable with wrong passphrase
	body, _ = json.Marshal(map[string]string{"passphrase": "wrong"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.True(t, h.Encrypted.Load()) // Should still be enabled
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

func TestHandleRecoverLedger(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable encryption first.
	body, _ := json.Marshal(map[string]string{"passphrase": "original-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var enableResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &enableResp))
	recoveryKey := enableResp["recovery_key"].(string)
	require.NotEmpty(t, recoveryKey)

	// Simulate lockout: mark vault as locked.
	h.VaultLocked.Store(true)

	// Recover using recovery key (no auth cookie needed — endpoint is unauthenticated).
	body, _ = json.Marshal(map[string]string{
		"recovery_key":   recoveryKey,
		"new_passphrase": "new-pass-456",
	})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/recover", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusOK, w.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &resp))
	assert.Equal(t, true, resp["ok"])
	assert.False(t, h.VaultLocked.Load(), "vault should be unlocked after recovery")
	cookies := w.Result().Cookies()
	require.Len(t, cookies, 1, "successful recovery must establish a dashboard session")
	assert.Equal(t, sessionCookieName, cookies[0].Name)
	assert.NotEmpty(t, cookies[0].Value)
}

func TestHandleRecoverLedger_BadKey(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable encryption.
	body, _ := json.Marshal(map[string]string{"passphrase": "original-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Try recovering with a bad key.
	body, _ = json.Marshal(map[string]string{
		"recovery_key":   "dGhpcyBpcyBub3QgYSB2YWxpZCBrZXk=",
		"new_passphrase": "new-pass-456",
	})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/recover", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestHandleRecoverLedger_WrongSameLengthKeyLeavesVaultUntouched(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	body, err := json.Marshal(map[string]string{"passphrase": "original-pass"})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	original, err := os.ReadFile(h.VaultKeyPath)
	require.NoError(t, err)

	wrongKey := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x42}, 32))
	body, err = json.Marshal(map[string]string{
		"recovery_key":   wrongKey,
		"new_passphrase": "new-pass-456",
	})
	require.NoError(t, err)
	req = httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/ledger/recover", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)

	assert.Equal(t, http.StatusUnauthorized, w.Code)
	after, err := os.ReadFile(h.VaultKeyPath)
	require.NoError(t, err)
	assert.Equal(t, original, after, "a wrong recovery key must never rewrite vault.key")
	assert.NoFileExists(t, h.VaultKeyPath+".bak", "verification must happen before backup or mutation")
	_, err = vault.Open(h.VaultKeyPath, "original-pass")
	assert.NoError(t, err, "the original passphrase must still unlock the untouched vault")
}

func TestHandleEnableLedger_ReusesExistingKey(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable encryption.
	body, _ := json.Marshal(map[string]string{"passphrase": "original-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var enableResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &enableResp))
	recoveryKey1 := enableResp["recovery_key"].(string)

	// Login and disable encryption.
	cookie := loginAfterEnable(t, r, "original-pass")
	body, _ = json.Marshal(map[string]string{"passphrase": "original-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)
	require.False(t, h.Encrypted.Load())

	// Re-enable encryption — should reuse existing vault.key, not create new one.
	body, _ = json.Marshal(map[string]string{"passphrase": "original-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	var reEnableResp map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &reEnableResp))
	assert.Equal(t, true, reEnableResp["ok"])
	assert.Equal(t, true, reEnableResp["reused_existing_key"])

	// Recovery key should be the same (same data key).
	recoveryKey2 := reEnableResp["recovery_key"].(string)
	assert.Equal(t, recoveryKey1, recoveryKey2, "re-enable should reuse the same data key")
}

func TestHandleEnableLedger_ReusesExistingKey_WrongPassphrase(t *testing.T) {
	h, _ := newTestHandler(t)
	r := testRouter(h)
	h.VaultKeyPath = filepath.Join(t.TempDir(), "vault.key")

	// Enable and then disable.
	body, _ := json.Marshal(map[string]string{"passphrase": "original-pass"})
	req := httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	cookie := loginAfterEnable(t, r, "original-pass")
	body, _ = json.Marshal(map[string]string{"passphrase": "original-pass"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/disable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.AddCookie(cookie)
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	require.Equal(t, http.StatusOK, w.Code)

	// Re-enable with WRONG passphrase — should get 401, not create new key.
	body, _ = json.Marshal(map[string]string{"passphrase": "wrong-pass!!"})
	req = httptest.NewRequest("POST", "/v1/dashboard/settings/ledger/enable", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusUnauthorized, w.Code)
	assert.False(t, h.Encrypted.Load(), "encryption should not be enabled after wrong passphrase")
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
