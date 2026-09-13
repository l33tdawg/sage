package rest

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/vault"
)

const messageStoragePath = "/v1/messages/storage"

func messageStorageFixture(t *testing.T) (*Server, *store.SQLiteStore, ed25519.PrivateKey, string) {
	t.Helper()
	server, _, badger, _ := newRBACTestServer(t)
	sqlite, err := store.NewSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "storage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlite.Close()) })
	server.store = sqlite
	server.agentStore = sqlite
	key := appV23LookupVaultKey("message-storage-caller")
	caller := appV23LookupVaultID(key)
	root := appV23LookupVaultID(appV23LookupVaultKey("message-storage-root"))
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: root, Scope: "storage-test", AgentID: caller, Profile: store.AppV23ProfileStandard,
		HomeDomain: "caller.home", Clearance: 1, Height: 1, BootstrapDigest: "storage-test",
	}))
	for _, entry := range []*store.AgentEntry{
		{AgentID: root, Name: "root", Role: store.AppV23RoleAdmin, Status: "active", Clearance: 4},
		{AgentID: caller, Name: "caller", Role: store.AppV23RoleMember, Status: "active", Clearance: 1},
	} {
		require.NoError(t, sqlite.CreateAgent(t.Context(), entry))
	}
	server.SetPostV23ForNextTxAccessor(func() bool { return true })
	return server, sqlite, key, caller
}

func readMessageStorage(t *testing.T, server *Server, key ed25519.PrivateKey, caller string) messageStorageResponse {
	t.Helper()
	recorder := httptest.NewRecorder()
	server.Router().ServeHTTP(recorder, signedAgentLookupRequest(t, key, caller, messageStoragePath))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &fields))
	require.Len(t, fields, 9)
	for _, name := range []string{"schema", "instance_id", "agent_id", "encryption_expected", "vault_active", "vault_generation", "stable", "canonical_send_idempotency", "encrypted_send_admission"} {
		require.Contains(t, fields, name)
	}
	var result messageStorageResponse
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
	require.Equal(t, "sage.message-storage.v1", result.Schema)
	require.Equal(t, caller, result.AgentID)
	require.Regexp(t, "^[0-9a-f]{64}$", result.InstanceID)
	require.Regexp(t, "^(0|[1-9][0-9]*)$", result.VaultGeneration)
	require.True(t, result.CanonicalSendIdempotency)
	require.True(t, result.EncryptedSendAdmission)
	return result
}

func TestMessageStorageStatusSQLiteLifecycle(t *testing.T) {
	server, sqlite, key, caller := messageStorageFixture(t)
	initial := readMessageStorage(t, server, key, caller)
	require.False(t, initial.EncryptionExpected)
	require.False(t, initial.VaultActive)
	require.True(t, initial.Stable)
	sqlite.SetVaultExpected(true)
	locked := readMessageStorage(t, server, key, caller)
	require.True(t, locked.EncryptionExpected)
	require.False(t, locked.VaultActive)
	keyPath := filepath.Join(t.TempDir(), "synthetic-vault.key")
	require.NoError(t, vault.Init(keyPath, "synthetic-test-passphrase"))
	unlockedVault, err := vault.Open(keyPath, "synthetic-test-passphrase")
	require.NoError(t, err)
	sqlite.SetVault(unlockedVault)
	unlocked := readMessageStorage(t, server, key, caller)
	require.True(t, unlocked.VaultActive)
	require.True(t, unlocked.EncryptionExpected)
	sqlite.SetVault(nil)
	relocked := readMessageStorage(t, server, key, caller)
	require.False(t, relocked.VaultActive)
	previous := uint64(0)
	for index, status := range []messageStorageResponse{initial, locked, unlocked, relocked} {
		require.Equal(t, initial.InstanceID, status.InstanceID)
		generation, parseErr := strconv.ParseUint(status.VaultGeneration, 10, 64)
		require.NoError(t, parseErr)
		if index > 0 {
			require.Greater(t, generation, previous)
		}
		previous = generation
	}
}

func TestMessageStorageStatusRejectsUnsignedRootAndUnknownCaller(t *testing.T) {
	server, sqlite, _, _ := messageStorageFixture(t)
	pendingKey := appV23LookupVaultKey("message-storage-pending")
	require.NoError(t, sqlite.CreateAgent(t.Context(), &store.AgentEntry{
		AgentID: appV23LookupVaultID(pendingKey), Name: "pending", Role: store.AppV23RoleMember, Status: "active", Clearance: 1,
	}))
	requests := []*http.Request{httptest.NewRequest(http.MethodGet, messageStoragePath, nil)}
	for _, label := range []string{"message-storage-root", "message-storage-unknown", "message-storage-pending"} {
		key := appV23LookupVaultKey(label)
		requests = append(requests, signedAgentLookupRequest(t, key, appV23LookupVaultID(key), messageStoragePath))
	}
	for _, request := range requests {
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, request)
		require.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, recorder.Code)
		require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
		require.NotContains(t, recorder.Body.String(), "vault_active")
		require.NotContains(t, recorder.Body.String(), "instance_id")
	}
}

func TestMessageStorageStatusNoLegacyBoundaryBypass(t *testing.T) {
	server, _, key, caller := messageStorageFixture(t)
	server.SetPostV23ForNextTxAccessor(func() bool { return false })
	recorder := httptest.NewRecorder()
	server.Router().ServeHTTP(recorder, signedAgentLookupRequest(t, key, caller, messageStoragePath))
	require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	require.Empty(t, server.messageStorage.instance)
}

func TestMessageStorageStatusRequiresExactProofEvenWithAgentContext(t *testing.T) {
	server, _, _, caller := messageStorageFixture(t)
	request := httptest.NewRequest(http.MethodGet, messageStoragePath, nil)
	request = request.WithContext(middleware.WithAgentID(request.Context(), caller))
	recorder := httptest.NewRecorder()
	server.handleMessageStorageStatus(recorder, request)
	require.Equal(t, http.StatusForbidden, recorder.Code)
	require.Empty(t, server.messageStorage.instance)
}

type wrappedMessageSQLite struct {
	*store.SQLiteStore
}

func TestMessageStorageStatusUnsupportedAndChangedStore(t *testing.T) {
	server, sqlite, key, caller := messageStorageFixture(t)
	for _, unsupported := range []store.MemoryStore{newMockMemoryStore(), &wrappedMessageSQLite{sqlite}, (*store.SQLiteStore)(nil)} {
		server.store = unsupported
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, signedAgentLookupRequest(t, key, caller, messageStoragePath))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		require.NotContains(t, recorder.Body.String(), "instance_id")
	}
	server.store = sqlite
	readMessageStorage(t, server, key, caller)
	replacement, err := store.NewSQLiteStore(t.Context(), filepath.Join(t.TempDir(), "replacement.db"))
	require.NoError(t, err)
	defer replacement.Close()
	for _, changed := range []store.MemoryStore{replacement, sqlite} {
		server.store = changed
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, signedAgentLookupRequest(t, key, caller, messageStoragePath))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code)
	}
}

func TestMessageStorageStatusInstancePerServerAndConcurrentObservation(t *testing.T) {
	server, sqlite, key, caller := messageStorageFixture(t)
	other, _, _, _ := messageStorageFixture(t)
	other.store = sqlite
	first := readMessageStorage(t, server, key, caller)
	second := readMessageStorage(t, other, key, caller)
	require.NotEqual(t, first.InstanceID, second.InstanceID)
	var workers sync.WaitGroup
	responses := make(chan *httptest.ResponseRecorder, 8)
	for index := 0; index < 8; index++ {
		request := signedAgentLookupRequest(t, key, caller, messageStoragePath)
		workers.Add(1)
		go func() {
			defer workers.Done()
			recorder := httptest.NewRecorder()
			server.Router().ServeHTTP(recorder, request)
			responses <- recorder
		}()
	}
	workers.Wait()
	close(responses)
	for recorder := range responses {
		require.Equal(t, http.StatusOK, recorder.Code)
		var value messageStorageResponse
		require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &value))
		require.Equal(t, first.InstanceID, value.InstanceID)
	}
}

func TestMessageStorageEncryptedSendAdmissionLifecycle(t *testing.T) {
	server, sqlite, key, caller := messageStorageFixture(t)
	body, err := json.Marshal(map[string]any{
		"to_agent": caller, "payload": "synthetic protected payload", "idempotency_key": "protected-key",
		"require_encrypted_storage": true,
	})
	require.NoError(t, err)
	send := func() *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, signedGovernanceRequestAs(t, key, caller, http.MethodPost, "/v1/messages", body))
		return recorder
	}
	for _, expected := range []bool{false, true} {
		sqlite.SetVaultExpected(expected)
		recorder := send()
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
		require.NotContains(t, recorder.Body.String(), "synthetic protected payload")
		rows, readErr := sqlite.GetOutbox(t.Context(), caller, 20)
		require.NoError(t, readErr)
		require.Empty(t, rows)
	}
	keyPath := filepath.Join(t.TempDir(), "synthetic-send-vault.key")
	require.NoError(t, vault.Init(keyPath, "synthetic-send-passphrase"))
	active, err := vault.Open(keyPath, "synthetic-send-passphrase")
	require.NoError(t, err)
	sqlite.SetVault(active)
	sqlite.SetVaultExpected(false)
	require.Equal(t, http.StatusServiceUnavailable, send().Code)
	sqlite.SetVaultExpected(true)
	first := send()
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())
	replayed := send()
	require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
	var receipt map[string]any
	require.NoError(t, json.Unmarshal(replayed.Body.Bytes(), &receipt))
	require.Equal(t, true, receipt["idempotent_replay"])
	sqlite.SetVault(nil)
	require.Equal(t, http.StatusServiceUnavailable, send().Code)
	sqlite.SetVault(active)
	rows, err := sqlite.GetOutbox(t.Context(), caller, 20)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "synthetic protected payload", rows[0].Payload)
}

func TestMessageStorageEncryptedSendUnsupportedAndDefaultUnchanged(t *testing.T) {
	server, sqlite, key, caller := messageStorageFixture(t)
	for _, unsupported := range []store.MemoryStore{newMockMemoryStore(), &wrappedMessageSQLite{sqlite}, (*store.SQLiteStore)(nil)} {
		server.store = unsupported
		body, err := json.Marshal(map[string]any{"to_agent": caller, "payload": "fixture",
			"idempotency_key": "unsupported", "require_encrypted_storage": true})
		require.NoError(t, err)
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, signedGovernanceRequestAs(t, key, caller, http.MethodPost, "/v1/messages", body))
		require.Equal(t, http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}
	server.store = sqlite
	for index, includeFalse := range []bool{false, true} {
		fields := map[string]any{"to_agent": caller, "payload": "default plaintext fixture",
			"idempotency_key": "default-" + strconv.Itoa(index)}
		if includeFalse {
			fields["require_encrypted_storage"] = false
		}
		body, err := json.Marshal(fields)
		require.NoError(t, err)
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, signedGovernanceRequestAs(t, key, caller, http.MethodPost, "/v1/messages", body))
		require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
	}
	rows, err := sqlite.GetOutbox(t.Context(), caller, 20)
	require.NoError(t, err)
	require.Len(t, rows, 2)
}

func TestMessageStorageEncryptedSendRequiresStrictSignedBoolean(t *testing.T) {
	server, sqlite, key, caller := messageStorageFixture(t)
	for _, invalid := range []string{"null", "1", "\"true\"", "true,\"require_encrypted_storage\":false"} {
		body := []byte(`{"to_agent":"` + caller + `","payload":"fixture","idempotency_key":"invalid","require_encrypted_storage":` + invalid + `}`)
		recorder := httptest.NewRecorder()
		server.Router().ServeHTTP(recorder, signedGovernanceRequestAs(t, key, caller, http.MethodPost, "/v1/messages", body))
		require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	rows, err := sqlite.GetOutbox(t.Context(), caller, 20)
	require.NoError(t, err)
	require.Empty(t, rows)
}
