package rest

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strconv"
	"sync"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

type messageStorageBinding struct {
	mutex    sync.Mutex
	once     sync.Once
	store    *store.SQLiteStore
	instance string
	failed   bool
}

type messageStorageResponse struct {
	Schema                   string `json:"schema"`
	InstanceID               string `json:"instance_id"`
	AgentID                  string `json:"agent_id"`
	EncryptionExpected       bool   `json:"encryption_expected"`
	VaultActive              bool   `json:"vault_active"`
	VaultGeneration          string `json:"vault_generation"`
	Stable                   bool   `json:"stable"`
	CanonicalSendIdempotency bool   `json:"canonical_send_idempotency"`
	EncryptedSendAdmission   bool   `json:"encrypted_send_admission"`
}

func messageStorageNoStore(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/messages/storage" {
			w.Header().Set("Cache-Control", "no-store")
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleMessageStorageStatus(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !requireExactSignedMessageAction(w, r) {
		return
	}
	agentID := middleware.ContextAgentID(r.Context())
	if agentID == "" {
		writeProblem(w, http.StatusForbidden, "Exact agent signature required", "Authenticated agent required.")
		return
	}
	if !s.isPostV23ForNextTx() {
		writeProblem(w, http.StatusServiceUnavailable, "Message storage unavailable", "Storage observation unavailable.")
		return
	}
	binding := &s.messageStorage
	binding.mutex.Lock()
	defer binding.mutex.Unlock()
	sqlite, concrete := s.store.(*store.SQLiteStore)
	_, canonical := canonicalMessageStore(s)
	if binding.store != nil && binding.store != sqlite {
		binding.failed = true
	}
	if !concrete || sqlite == nil || !canonical || binding.failed {
		writeProblem(w, http.StatusServiceUnavailable, "Message storage unavailable", "Storage observation unavailable.")
		return
	}
	binding.once.Do(func() {
		var identity [32]byte
		if _, err := rand.Read(identity[:]); err != nil {
			binding.failed = true
			return
		}
		binding.store = sqlite
		binding.instance = hex.EncodeToString(identity[:])
	})
	if binding.failed || binding.instance == "" {
		writeProblem(w, http.StatusServiceUnavailable, "Message storage unavailable", "Storage observation unavailable.")
		return
	}
	status := sqlite.MessageStorageStatus()
	writeJSON(w, http.StatusOK, messageStorageResponse{
		Schema: "sage.message-storage.v1", InstanceID: binding.instance, AgentID: agentID,
		EncryptionExpected: status.EncryptionExpected, VaultActive: status.VaultActive,
		VaultGeneration: strconv.FormatUint(status.VaultGeneration, 10), Stable: status.Stable,
		CanonicalSendIdempotency: true, EncryptedSendAdmission: true,
	})
}
