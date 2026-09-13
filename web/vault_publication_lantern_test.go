package web

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/vault"
)

func TestLanternEnablePublishesExpectedBeforeConfigSave(t *testing.T) {
	for _, failSave := range []bool{false, true} {
		name := "saved"
		if failSave {
			name = "save_failed"
		}
		t.Run(name, func(t *testing.T) {
			projection := &store.SQLiteStore{}
			handler := &DashboardHandler{store: projection, VaultKeyPath: filepath.Join(t.TempDir(), "synthetic-vault.key")}
			saved := false
			handler.SaveEncryptionConfig = func(enabled bool) error {
				saved = true
				status := projection.MessageStorageStatus()
				if !enabled || !status.EncryptionExpected || !status.VaultActive || !status.Stable || !handler.Encrypted.Load() {
					t.Fatal("config save preceded complete protected publication")
				}
				if failSave {
					return errors.New("synthetic persistence failure")
				}
				return nil
			}
			request := httptest.NewRequest(http.MethodPost, "/v1/dashboard/settings/ledger/enable", strings.NewReader(`{"passphrase":"synthetic-test-passphrase"}`))
			response := httptest.NewRecorder()
			handler.handleEnableLedger(response, request)
			expectedStatus := http.StatusOK
			if failSave {
				expectedStatus = http.StatusInternalServerError
			}
			if !saved || response.Code != expectedStatus {
				t.Fatal("unexpected enable outcome")
			}
			if failSave && len(response.Result().Cookies()) != 0 {
				t.Fatal("failed persistence issued a session")
			}
			status := projection.MessageStorageStatus()
			if !status.EncryptionExpected || !status.VaultActive || !handler.Encrypted.Load() {
				t.Fatal("enable failure downgraded encryption requirement")
			}
			projection.SetVault(nil)
			if !projection.VaultLocked() {
				t.Fatal("enable followed by detach allowed plaintext")
			}
		})
	}
}

func TestLanternUnlockPublishesExpectedBeforeOpeningAdmission(t *testing.T) {
	projection := &store.SQLiteStore{}
	handler := &DashboardHandler{store: projection}
	handler.VaultLocked.Store(true)
	active, err := vault.FromDataKey("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=")
	if err != nil {
		t.Fatal("synthetic vault construction failed")
	}
	opened := false
	handler.OnVaultUnlocked = func(passphrase string) {
		status := projection.MessageStorageStatus()
		if !status.EncryptionExpected || !status.VaultActive || !handler.VaultLocked.Load() {
			t.Fatal("unlock admission preceded protected publication")
		}
		opened = true
	}
	if err := handler.publishUnlockedVault(active, "synthetic-test-passphrase"); err != nil {
		t.Fatal("unlock failed")
	}
	if !opened || handler.VaultLocked.Load() {
		t.Fatal("unlock admission did not complete")
	}
}
