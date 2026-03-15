package web

import (
	"encoding/json"
	"net/http"
	"os"
	"strings"

	"github.com/l33tdawg/sage/internal/vault"
)

// VaultStore is implemented by stores that support encryption.
type VaultStore interface {
	SetVault(v *vault.Vault)
}

// handleGetLedgerStatus returns the current Synaptic Ledger (encryption vault) status.
func (h *DashboardHandler) handleGetLedgerStatus(w http.ResponseWriter, _ *http.Request) {
	if !h.Encrypted.Load() && !vault.Exists(h.VaultKeyPath) {
		writeJSONResp(w, http.StatusOK, map[string]any{"enabled": false})
		return
	}

	// Tilde-shorten the path for display.
	displayPath := h.VaultKeyPath
	if home, err := os.UserHomeDir(); err == nil {
		displayPath = strings.Replace(displayPath, home, "~", 1)
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"enabled":    h.Encrypted.Load(),
		"algorithm":  "AES-256-GCM",
		"kdf":        "Argon2id",
		"vault_path": displayPath,
	})
}

// handleEnableLedger initialises the encryption vault and attaches it to the store.
func (h *DashboardHandler) handleEnableLedger(w http.ResponseWriter, r *http.Request) {
	if h.Encrypted.Load() {
		writeError(w, http.StatusConflict, "encryption is already enabled")
		return
	}
	if h.VaultKeyPath == "" {
		writeError(w, http.StatusBadRequest, "vault key path not configured")
		return
	}

	var body struct {
		Passphrase string `json:"passphrase"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Passphrase == "" {
		writeError(w, http.StatusBadRequest, "passphrase is required")
		return
	}
	if len(body.Passphrase) < 8 {
		writeError(w, http.StatusBadRequest, "passphrase must be at least 8 characters")
		return
	}

	// Create the vault key file.
	if err := vault.Init(h.VaultKeyPath, body.Passphrase); err != nil {
		writeError(w, http.StatusInternalServerError, "failed to initialise vault: "+err.Error())
		return
	}

	// Open the vault so we can attach it to the store.
	v, err := vault.Open(h.VaultKeyPath, body.Passphrase)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "vault created but failed to open: "+err.Error())
		return
	}

	// Attach vault to the store if the store supports it.
	if vs, ok := h.store.(VaultStore); ok {
		vs.SetVault(v)
	}

	h.Encrypted.Store(true)

	// Persist encryption state to config.yaml so lock screen appears on restart.
	if h.SaveEncryptionConfig != nil {
		if err := h.SaveEncryptionConfig(true); err != nil {
			writeError(w, http.StatusInternalServerError, "encryption enabled but failed to save config: "+err.Error())
			return
		}
	}

	// Recovery key: the raw data key, base64-encoded.
	// This can re-initialize the vault with a new passphrase if the original is lost.
	recoveryKey, _ := v.RecoveryKey()

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":           true,
		"recovery_key": recoveryKey,
	})
}

// handleChangePassphrase changes the vault passphrase without re-encrypting data.
func (h *DashboardHandler) handleChangePassphrase(w http.ResponseWriter, r *http.Request) {
	if !h.Encrypted.Load() {
		writeError(w, http.StatusBadRequest, "encryption is not enabled")
		return
	}

	var body struct {
		OldPassphrase string `json:"old_passphrase"`
		NewPassphrase string `json:"new_passphrase"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.OldPassphrase == "" || body.NewPassphrase == "" {
		writeError(w, http.StatusBadRequest, "old_passphrase and new_passphrase are required")
		return
	}
	if len(body.NewPassphrase) < 8 {
		writeError(w, http.StatusBadRequest, "passphrase must be at least 8 characters")
		return
	}

	if err := vault.ChangePassphrase(h.VaultKeyPath, body.OldPassphrase, body.NewPassphrase); err != nil {
		if err == vault.ErrWrongPassphrase {
			writeError(w, http.StatusUnauthorized, "wrong passphrase")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to change passphrase: "+err.Error())
		return
	}

	// Return a fresh recovery key (same data key, but user should save again
	// since they may have lost the old recovery key alongside the old passphrase).
	var recoveryKey string
	if v, openErr := vault.Open(h.VaultKeyPath, body.NewPassphrase); openErr == nil {
		recoveryKey, _ = v.RecoveryKey()
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":           true,
		"recovery_key": recoveryKey,
	})
}

// handleRecoverLedger resets the vault passphrase using the recovery key.
// This is the escape hatch when a user loses their passphrase but has the recovery key.
func (h *DashboardHandler) handleRecoverLedger(w http.ResponseWriter, r *http.Request) {
	if !h.Encrypted.Load() && !vault.Exists(h.VaultKeyPath) {
		writeError(w, http.StatusBadRequest, "encryption is not enabled")
		return
	}
	if h.VaultKeyPath == "" {
		writeError(w, http.StatusBadRequest, "vault key path not configured")
		return
	}

	var body struct {
		RecoveryKey   string `json:"recovery_key"`
		NewPassphrase string `json:"new_passphrase"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if body.RecoveryKey == "" || body.NewPassphrase == "" {
		writeError(w, http.StatusBadRequest, "recovery_key and new_passphrase are required")
		return
	}
	if len(body.NewPassphrase) < 8 {
		writeError(w, http.StatusBadRequest, "passphrase must be at least 8 characters")
		return
	}

	// Re-initialize the vault key file from the recovery key.
	if err := vault.InitFromRecoveryKey(h.VaultKeyPath, body.RecoveryKey, body.NewPassphrase); err != nil {
		writeError(w, http.StatusUnauthorized, "recovery failed: "+err.Error())
		return
	}

	// Open the vault with the new passphrase to verify it works.
	v, err := vault.Open(h.VaultKeyPath, body.NewPassphrase)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "vault recovered but failed to open: "+err.Error())
		return
	}

	// Attach vault to the store and unlock.
	if vs, ok := h.store.(VaultStore); ok {
		vs.SetVault(v)
	}
	h.VaultLocked.Store(false)

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"message": "vault recovered successfully — you are now unlocked",
	})
}

// handleDisableLedger disables encryption for new memories. Existing encrypted
// memories remain encrypted — they can still be read while the vault is in
// memory, but new writes will be plaintext.
func (h *DashboardHandler) handleDisableLedger(w http.ResponseWriter, r *http.Request) {
	if !h.Encrypted.Load() {
		writeError(w, http.StatusBadRequest, "encryption is not enabled")
		return
	}

	var body struct {
		Passphrase string `json:"passphrase"`
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Passphrase == "" {
		writeError(w, http.StatusBadRequest, "passphrase is required")
		return
	}

	// Verify the passphrase is correct before disabling.
	_, err := vault.Open(h.VaultKeyPath, body.Passphrase)
	if err != nil {
		if err == vault.ErrWrongPassphrase {
			writeError(w, http.StatusUnauthorized, "wrong passphrase")
			return
		}
		writeError(w, http.StatusInternalServerError, "failed to verify passphrase: "+err.Error())
		return
	}

	h.Encrypted.Store(false)

	// Persist disabled state to config.yaml.
	if h.SaveEncryptionConfig != nil {
		if err := h.SaveEncryptionConfig(false); err != nil {
			// Re-enable in memory since config didn't save.
			h.Encrypted.Store(true)
			writeError(w, http.StatusInternalServerError, "failed to save config: "+err.Error())
			return
		}
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"ok":      true,
		"warning": "Existing encrypted memories will remain encrypted but new memories won't be encrypted. Re-enable encryption to protect new data.",
	})
}
