package rest

import (
	"crypto/ed25519"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/stretchr/testify/require"
)

const workflowTestID = "e837d957-c686-4e53-894f-c050e84a3ec8"
const workflowTestPath = "/v1/workflows/" + workflowTestID
const workflowTestBody = `{"kind":"mesh_outbound","expected_revision":0,"payload":{"text":"synthetic-private-canary"}}`

type workflowTestFixture struct {
	server       *Server
	sqlite       *store.SQLiteStore
	databasePath string
	keys         map[string]ed25519.PrivateKey
}

func newWorkflowTestFixture(t *testing.T) workflowTestFixture {
	t.Helper()
	server, _, badger, _ := newRBACTestServer(t)
	path := filepath.Join(t.TempDir(), "workflows.db")
	sqlite, err := store.NewSQLiteStore(t.Context(), path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlite.Close()) })
	server.store, server.agentStore = sqlite, sqlite
	keys := map[string]ed25519.PrivateKey{}
	for _, name := range []string{"root", "first", "second", "pending"} {
		keys[name] = appV23LookupVaultKey("workflow-" + name)
	}
	root := appV23LookupVaultID(keys["root"])
	first := appV23LookupVaultID(keys["first"])
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: root, Scope: "workflow-test", AgentID: first, Profile: store.AppV23ProfileStandard,
		HomeDomain: "first.home", Clearance: 1, Height: 1, BootstrapDigest: "workflow-test",
	}))
	for _, name := range []string{"root", "first", "second", "pending"} {
		actor := appV23LookupVaultID(keys[name])
		if name == "second" || name == "pending" {
			require.NoError(t, badger.RegisterAgentWithCapabilities(actor, name, store.AppV23RoleMember, "", "", "", 1, 0))
			if name == "second" {
				require.NoError(t, badger.ApproveAppV23LocalAgent(store.AppV23LocalEnrollment{
					AgentID: actor, ApprovedBy: root, RootGeneration: 1, Profile: store.AppV23ProfileStandard,
					HomeDomain: "second.home", Clearance: 1, Active: true, UpdatedHeight: 2,
				}, store.AppV23RoleMember, 0, 0))
			}
		}
		role := store.AppV23RoleMember
		if name == "root" {
			role = store.AppV23RoleAdmin
		}
		require.NoError(t, sqlite.CreateAgent(t.Context(), &store.AgentEntry{
			AgentID: actor, Name: name, Role: role, Status: "active", Clearance: 1,
		}))
	}
	server.SetPostV23ForNextTxAccessor(func() bool { return true })
	keyPath := filepath.Join(t.TempDir(), "synthetic-vault.key")
	require.NoError(t, vault.Init(keyPath, "synthetic-workflow-passphrase"))
	active, err := vault.Open(keyPath, "synthetic-workflow-passphrase")
	require.NoError(t, err)
	sqlite.SetVaultExpected(true)
	sqlite.SetVault(active)
	return workflowTestFixture{server, sqlite, path, keys}
}

func (fixture workflowTestFixture) request(t *testing.T, actor, method, path, body string) *httptest.ResponseRecorder {
	t.Helper()
	key := fixture.keys[actor]
	recorder := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(recorder, signedGovernanceRequestAs(t, key, appV23LookupVaultID(key), method, path, []byte(body)))
	require.Equal(t, "no-store", recorder.Header().Get("Cache-Control"))
	return recorder
}

func TestWorkflowJournalRESTLifecycleCASAndUntrustedContract(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	created := fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowTestBody)
	require.Equal(t, http.StatusOK, created.Code, created.Body.String())
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &fields))
	require.Len(t, fields, 7)
	var record workflowJournalResponse
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &record))
	require.Equal(t, "sage.workflow-journal.v1", record.Schema)
	require.Equal(t, "untrusted_auxiliary", record.Trust)
	require.Equal(t, appV23LookupVaultID(fixture.keys["first"]), record.AgentID)
	require.Equal(t, int64(1), record.Revision)
	read := fixture.request(t, "first", http.MethodGet, workflowTestPath, "")
	require.Equal(t, created.Body.String(), read.Body.String())
	replay := fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowTestBody)
	require.Equal(t, created.Body.String(), replay.Body.String())
	updated := fixture.request(t, "first", http.MethodPut, workflowTestPath, `{"kind":"mesh_outbound","expected_revision":1,"payload":{"state":"pending"}}`)
	require.Equal(t, http.StatusOK, updated.Code, updated.Body.String())
	conflict := fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowTestBody)
	require.Equal(t, http.StatusConflict, conflict.Code)
	require.NotContains(t, conflict.Body.String(), "synthetic-private-canary")
	read = fixture.request(t, "first", http.MethodGet, workflowTestPath, "")
	require.NoError(t, json.Unmarshal(read.Body.Bytes(), &record))
	require.Equal(t, int64(2), record.Revision)
}

func TestWorkflowJournalRESTCrossActorIsolation(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	require.Equal(t, http.StatusOK, fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowTestBody).Code)
	other := fixture.request(t, "second", http.MethodGet, workflowTestPath, "")
	require.Equal(t, http.StatusNotFound, other.Code)
	require.NotContains(t, other.Body.String(), "synthetic-private-canary")
	require.Equal(t, http.StatusConflict, fixture.request(t, "second", http.MethodPut, workflowTestPath,
		`{"kind":"mesh_outbound","expected_revision":1,"payload":{}}`).Code)
	require.Equal(t, http.StatusOK, fixture.request(t, "second", http.MethodPut, workflowTestPath,
		`{"kind":"public_proposal","expected_revision":0,"payload":{"second":true}}`).Code)
	read := fixture.request(t, "first", http.MethodGet, workflowTestPath, "")
	require.Contains(t, read.Body.String(), "synthetic-private-canary")
	require.NotContains(t, read.Body.String(), `"second":true`)
}

func TestWorkflowJournalRESTRejectsRootPendingUnsignedAndLegacyProof(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		for _, actor := range []string{"root", "pending"} {
			response := fixture.request(t, actor, method, workflowTestPath, workflowTestBody)
			require.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, response.Code)
			require.NotContains(t, response.Body.String(), "synthetic-private-canary")
		}
		key := fixture.keys["first"]
		for _, request := range []*http.Request{
			httptest.NewRequest(method, workflowTestPath, strings.NewReader(workflowTestBody)),
			signedRequestAs(t, key, appV23LookupVaultID(key), method, workflowTestPath, []byte(workflowTestBody)),
		} {
			response := httptest.NewRecorder()
			fixture.server.Router().ServeHTTP(response, request)
			require.Contains(t, []int{http.StatusUnauthorized, http.StatusForbidden}, response.Code)
			require.Equal(t, "no-store", response.Header().Get("Cache-Control"))
		}
	}
}

func TestWorkflowJournalRESTStrictBodyAndPayloadBounds(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	for _, body := range []string{
		`{}`, `null`, `[]`, workflowTestBody + `{}`,
		`{"kind":"mesh_outbound","expected_revision":0,"payload":{},"agent_id":"another-actor"}`,
		`{"kind":"mesh_outbound","expected_revision":0,"expected_revision":0,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":0,"payload":{"duplicate":1,"duplicate":2}}`,
		`{"kind":"mesh_outbound","expected_revision":null,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":true,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":"0","payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":-1,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":0.0,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":9223372036854775807,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":9007199254740991,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":9007199254740992,"payload":{}}`,
		`{"kind":"canonical_memory","expected_revision":0,"payload":{}}`,
		`{"kind":"mesh_outbound","expected_revision":0,"payload":"` + string([]byte{255}) + `"}`,
	} {
		response := fixture.request(t, "first", http.MethodPut, workflowTestPath, body)
		require.Equal(t, http.StatusBadRequest, response.Code, body)
		require.NotContains(t, response.Body.String(), "another-actor")
	}
	oversized := `{"kind":"mesh_outbound","expected_revision":0,"payload":"` + strings.Repeat("x", 16384) + `"}`
	require.Equal(t, http.StatusRequestEntityTooLarge, fixture.request(t, "first", http.MethodPut, workflowTestPath, oversized).Code)
	require.Equal(t, http.StatusNotFound, fixture.request(t, "first", http.MethodGet, workflowTestPath, "").Code)
}

func TestWorkflowJournalRESTVaultUnavailableAndCorruptAreNotNotFound(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	require.Equal(t, http.StatusOK, fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowTestBody).Code)
	database, err := sql.Open("sqlite", fixture.databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	_, err = database.ExecContext(t.Context(), `UPDATE workflow_journal SET sealed_record = ?`, []byte("synthetic-corrupt-canary"))
	require.NoError(t, err)
	for _, method := range []string{http.MethodGet, http.MethodPut} {
		body := ""
		if method == http.MethodPut {
			body = workflowTestBody
		}
		response := fixture.request(t, "first", method, workflowTestPath, body)
		require.Equal(t, http.StatusServiceUnavailable, response.Code)
		require.NotContains(t, response.Body.String(), "synthetic-corrupt-canary")
	}
	fixture.sqlite.SetVault(nil)
	for _, path := range []string{workflowTestPath, "/v1/workflows/9726bde5-5f3d-49f0-b420-33a16875b59c"} {
		require.Equal(t, http.StatusServiceUnavailable, fixture.request(t, "first", http.MethodGet, path, "").Code)
	}
}

func TestWorkflowJournalRESTPostV23AndCanonicalUUIDRequired(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	for _, identifier := range []string{"not-a-uuid", strings.ToUpper(workflowTestID), "00000000-0000-0000-0000-000000000000"} {
		require.Equal(t, http.StatusBadRequest, fixture.request(t, "first", http.MethodGet, "/v1/workflows/"+identifier, "").Code)
	}
	fixture.server.SetPostV23ForNextTxAccessor(func() bool { return false })
	require.Equal(t, http.StatusServiceUnavailable, fixture.request(t, "first", http.MethodGet, workflowTestPath, "").Code)
}

func TestWorkflowJournalRESTHasNoDelete(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	require.Equal(t, http.StatusMethodNotAllowed, fixture.request(t, "first", http.MethodDelete, workflowTestPath, "").Code)
	response := httptest.NewRecorder()
	key := fixture.keys["first"]
	fixture.server.Router().ServeHTTP(response, signedAgentLookupRequest(t, key, appV23LookupVaultID(key), "/v1/workflows"))
	require.Equal(t, http.StatusOK, response.Code)
	require.JSONEq(t, `{"schema":"sage.workflow-journal.v1","items":[],"next_after":null,"has_more":false}`, response.Body.String())
}

func TestWorkflowJournalRESTErrorMappingNeverLeaksStorageContent(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{
		{store.ErrWorkflowJournalInvalid, http.StatusBadRequest},
		{store.ErrWorkflowJournalNotFound, http.StatusNotFound},
		{store.ErrWorkflowJournalConflict, http.StatusConflict},
		{store.ErrWorkflowJournalLimit, http.StatusTooManyRequests},
		{store.ErrWorkflowJournalVaultRequired, http.StatusServiceUnavailable},
		{store.ErrWorkflowJournalCorrupt, http.StatusServiceUnavailable},
		{errors.New("synthetic-secret-path-and-payload"), http.StatusServiceUnavailable},
	} {
		recorder := httptest.NewRecorder()
		workflowJournalError(recorder, test.err)
		require.Equal(t, test.status, recorder.Code)
		require.NotContains(t, recorder.Body.String(), test.err.Error())
	}
}

func TestWorkflowJournalRESTNonceReplayRejected(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	key := fixture.keys["first"]
	request := signedAgentLookupRequest(t, key, appV23LookupVaultID(key), "/v1/workflows/not-a-uuid")
	first := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(first, request)
	require.Equal(t, http.StatusBadRequest, first.Code)
	second := httptest.NewRecorder()
	fixture.server.Router().ServeHTTP(second, request)
	require.Equal(t, http.StatusUnauthorized, second.Code)
	require.Equal(t, "no-store", second.Header().Get("Cache-Control"))
}

func TestWorkflowJournalRESTRevisionJSONSafeBoundary(t *testing.T) {
	kind, revision, payload, guard, valid := decodeWorkflowJournalRequest([]byte(`{"kind":"mesh_inbound","expected_revision":9007199254740990,"payload":{}}`))
	require.True(t, valid)
	require.Equal(t, "mesh_inbound", kind)
	require.Equal(t, maxWorkflowJSONRevision-1, revision)
	require.JSONEq(t, `{}`, string(payload))
	require.Nil(t, guard)
	_, _, _, _, valid = decodeWorkflowJournalRequest([]byte(`{"kind":"mesh_inbound","expected_revision":9007199254740991,"payload":{}}`))
	require.False(t, valid)
}

func TestWorkflowJournalRESTAuxiliaryKindsAndEncryptedPersistence(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	identifiers := []string{workflowTestID, "9726bde5-5f3d-49f0-b420-33a16875b59c", "12dce032-a342-4bcc-912d-3884d1f0df09", uuid.NewString(), uuid.NewString()}
	for index, kind := range []string{"mesh_outbound", "mesh_inbound", "public_proposal", "conversation_control", "conversation_session"} {
		body, err := json.Marshal(map[string]any{
			"kind": kind, "expected_revision": 0,
			"payload": map[string]any{"text": "synthetic-untrusted-instruction-canary", "accepted": true},
		})
		require.NoError(t, err)
		response := fixture.request(t, "first", http.MethodPut, "/v1/workflows/"+identifiers[index], string(body))
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		var record workflowJournalResponse
		require.NoError(t, json.Unmarshal(response.Body.Bytes(), &record))
		require.Equal(t, "untrusted_auxiliary", record.Trust)
		require.Equal(t, kind, record.Kind)
	}
	database, err := sql.Open("sqlite", fixture.databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	var sealed []byte
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT sealed_record FROM workflow_journal WHERE record_id = ?`, workflowTestID).Scan(&sealed))
	require.NotContains(t, string(sealed), "synthetic-untrusted-instruction-canary")
	require.NotContains(t, string(sealed), "mesh_outbound")
	var count int
	require.NoError(t, database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM pipeline_messages`).Scan(&count))
	require.Zero(t, count)
}

func TestWorkflowJournalRESTListPagesAreActorBoundAndStrict(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	firstID := "12dce032-a342-4bcc-912d-3884d1f0df09"
	for _, identifier := range []string{workflowTestID, firstID} {
		require.Equal(t, http.StatusOK, fixture.request(t, "first", http.MethodPut, "/v1/workflows/"+identifier, workflowTestBody).Code)
	}
	first := fixture.request(t, "first", http.MethodGet, "/v1/workflows?limit=1", "")
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	var page workflowJournalPageResponse
	require.NoError(t, json.Unmarshal(first.Body.Bytes(), &page))
	require.Len(t, page.Items, 1)
	require.True(t, page.HasMore)
	require.Equal(t, firstID, *page.NextAfter)
	require.Equal(t, firstID, page.Items[0].RecordID)
	second := fixture.request(t, "first", http.MethodGet, "/v1/workflows?after="+firstID+"&limit=1", "")
	require.Equal(t, http.StatusOK, second.Code)
	require.NoError(t, json.Unmarshal(second.Body.Bytes(), &page))
	require.Len(t, page.Items, 1)
	require.False(t, page.HasMore)
	require.Nil(t, page.NextAfter)
	require.Equal(t, workflowTestID, page.Items[0].RecordID)
	other := fixture.request(t, "second", http.MethodGet, "/v1/workflows?limit=20", "")
	require.JSONEq(t, `{"schema":"sage.workflow-journal.v1","items":[],"next_after":null,"has_more":false}`, other.Body.String())
	for _, query := range []string{"limit=0", "limit=51", "limit=true", "limit=1.0", "limit=01", "limit=", "limit=1&limit=2",
		"agent_id=other", "kind=mesh_outbound", "after=bad", "after=" + firstID + "&after=" + firstID} {
		require.Equal(t, http.StatusBadRequest, fixture.request(t, "first", http.MethodGet, "/v1/workflows?"+query, "").Code, query)
	}
	fixture.sqlite.SetVault(nil)
	require.Equal(t, http.StatusServiceUnavailable, fixture.request(t, "first", http.MethodGet, "/v1/workflows", "").Code)
}

func TestWorkflowJournalRESTListCorruptionIsNotEmptyOrPartial(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	require.Equal(t, http.StatusOK, fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowTestBody).Code)
	database, err := sql.Open("sqlite", fixture.databasePath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	_, err = database.ExecContext(t.Context(), `UPDATE workflow_journal SET sealed_record = ?`, []byte("synthetic-corrupt"))
	require.NoError(t, err)
	response := fixture.request(t, "first", http.MethodGet, "/v1/workflows", "")
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
	require.NotContains(t, response.Body.String(), "synthetic-corrupt")
	require.NotContains(t, response.Body.String(), "items")
}

func workflowGuardBody(controlID string, targetRevision, guardRevision int64) string {
	return fmt.Sprintf(`{"kind":"conversation_session","expected_revision":%d,"payload":{"state":"reserved"},"guard":{"record_id":%q,"expected_revision":%d}}`, targetRevision, controlID, guardRevision)
}

func TestWorkflowJournalRESTGuardLifecycleAndOptOut(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	controlID := uuid.NewString()
	controlPath := "/v1/workflows/" + controlID
	control := fixture.request(t, "first", http.MethodPut, controlPath, `{"kind":"conversation_control","expected_revision":0,"payload":{"enabled":true}}`)
	require.Equal(t, http.StatusOK, control.Code)
	body := workflowGuardBody(controlID, 0, 1)
	created := fixture.request(t, "first", http.MethodPut, workflowTestPath, body)
	require.Equal(t, http.StatusOK, created.Code, created.Body.String())
	replay := fixture.request(t, "first", http.MethodPut, workflowTestPath, body)
	require.Equal(t, http.StatusOK, replay.Code)
	require.JSONEq(t, created.Body.String(), replay.Body.String())
	var fields map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(created.Body.Bytes(), &fields))
	require.Len(t, fields, 7)
	require.NotContains(t, fields, "guard")
	require.JSONEq(t, control.Body.String(), fixture.request(t, "first", http.MethodGet, controlPath, "").Body.String())
	optOut := fixture.request(t, "first", http.MethodPut, controlPath, `{"kind":"conversation_control","expected_revision":1,"payload":{"enabled":false}}`)
	require.Equal(t, http.StatusOK, optOut.Code)
	for _, targetRevision := range []int64{0, 1} {
		denied := fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowGuardBody(controlID, targetRevision, 1))
		require.Equal(t, http.StatusConflict, denied.Code)
	}
	require.JSONEq(t, created.Body.String(), fixture.request(t, "first", http.MethodGet, workflowTestPath, "").Body.String())
	updated := fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowGuardBody(controlID, 1, 2))
	require.Equal(t, http.StatusOK, updated.Code)
	var record workflowJournalResponse
	require.NoError(t, json.Unmarshal(updated.Body.Bytes(), &record))
	require.Equal(t, int64(2), record.Revision)
	require.Equal(t, appV23LookupVaultID(fixture.keys["first"]), record.AgentID)
	require.Equal(t, "untrusted_auxiliary", record.Trust)
}

func TestWorkflowJournalRESTGuardStrictObjectAndIntegers(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	controlID := uuid.NewString()
	for _, rawGuard := range []string{
		`null`, `[]`, `true`, `0`, `"guard"`, `{}`,
		`{"record_id":"` + controlID + `"}`,
		`{"expected_revision":1}`,
		`{"record_id":"` + controlID + `","expected_revision":1,"agent_id":"another-actor"}`,
		`{"record_id":"` + controlID + `","record_id":"` + controlID + `","expected_revision":1}`,
		`{"record_id":"` + controlID + `","\u0072ecord_id":"` + controlID + `","expected_revision":1}`,
		`{"record_id":"` + controlID + `","expected_revision":1,"expected_revision":1}`,
	} {
		body := `{"kind":"conversation_session","expected_revision":0,"payload":{},"guard":` + rawGuard + `}`
		require.Equal(t, http.StatusBadRequest, fixture.request(t, "first", http.MethodPut, workflowTestPath, body).Code, body)
	}
	for _, revision := range []string{`null`, `true`, `"1"`, `1.0`, `1e0`, `-0`, `0`, `-1`, `01`, `+1`, `9007199254740992`, `9223372036854775807`} {
		body := `{"kind":"conversation_session","expected_revision":0,"payload":{},"guard":{"record_id":"` + controlID + `","expected_revision":` + revision + `}}`
		require.Equal(t, http.StatusBadRequest, fixture.request(t, "first", http.MethodPut, workflowTestPath, body).Code, revision)
	}
	for _, identifier := range []string{"bad", "", uuid.Nil.String(), strings.ToUpper(controlID), workflowTestID} {
		require.Equal(t, http.StatusBadRequest, fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowGuardBody(identifier, 0, 1)).Code, identifier)
	}
	for _, body := range []string{
		strings.Replace(workflowGuardBody(controlID, 0, 1), `"conversation_session"`, `"mesh_outbound"`, 1),
		strings.Replace(workflowGuardBody(controlID, 0, 1), `"payload":{"state":"reserved"},`, ``, 1),
		strings.Replace(workflowGuardBody(controlID, 0, 1), `"guard":`, `"guard":null,"guard":`, 1),
	} {
		require.Equal(t, http.StatusBadRequest, fixture.request(t, "first", http.MethodPut, workflowTestPath, body).Code)
	}
	_, _, _, guard, valid := decodeWorkflowJournalRequest([]byte(workflowGuardBody(controlID, 0, maxWorkflowJSONRevision)))
	require.True(t, valid)
	require.Equal(t, maxWorkflowJSONRevision, guard.ExpectedRevision)
	require.Equal(t, http.StatusConflict, fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowGuardBody(controlID, 0, maxWorkflowJSONRevision)).Code)
	require.Equal(t, http.StatusNotFound, fixture.request(t, "first", http.MethodGet, workflowTestPath, "").Code)
}

func TestWorkflowJournalRESTGuardFailureIsolation(t *testing.T) {
	for _, failure := range []string{"missing", "stale", "other_actor", "wrong_kind", "corrupt", "locked", "encryption_disabled"} {
		t.Run(failure, func(t *testing.T) {
			fixture := newWorkflowTestFixture(t)
			controlID := uuid.NewString()
			actor, kind := "first", "conversation_control"
			if failure == "other_actor" {
				actor = "second"
			}
			if failure == "wrong_kind" {
				kind = "conversation_session"
			}
			if failure != "missing" {
				body := fmt.Sprintf(`{"kind":%q,"expected_revision":0,"payload":{"text":"synthetic-control-canary"}}`, kind)
				require.Equal(t, http.StatusOK, fixture.request(t, actor, http.MethodPut, "/v1/workflows/"+controlID, body).Code)
			}
			database, err := sql.Open("sqlite", fixture.databasePath)
			require.NoError(t, err)
			defer database.Close()
			wanted := http.StatusConflict
			guardRevision := int64(1)
			switch failure {
			case "stale":
				guardRevision = 2
			case "corrupt":
				_, err := database.ExecContext(t.Context(), `UPDATE workflow_journal SET sealed_record=? WHERE record_id=?`, []byte("synthetic-corrupt-canary"), controlID)
				require.NoError(t, err)
				wanted = http.StatusServiceUnavailable
			case "locked":
				fixture.sqlite.SetVault(nil)
				wanted = http.StatusServiceUnavailable
			case "encryption_disabled":
				fixture.sqlite.SetVaultExpected(false)
				wanted = http.StatusServiceUnavailable
			}
			response := fixture.request(t, "first", http.MethodPut, workflowTestPath, workflowGuardBody(controlID, 0, guardRevision))
			require.Equal(t, wanted, response.Code, response.Body.String())
			require.NotContains(t, response.Body.String(), "canary")
			var count int
			require.NoError(t, database.QueryRowContext(t.Context(), `SELECT COUNT(*) FROM workflow_journal WHERE record_id=?`, workflowTestID).Scan(&count))
			require.Zero(t, count)
		})
	}
}

func TestWorkflowJournalRESTGuardConcurrentOptOut(t *testing.T) {
	fixture := newWorkflowTestFixture(t)
	router := fixture.server.Router()
	key := fixture.keys["first"]
	for attempt := 0; attempt < 12; attempt++ {
		controlID, sessionID := uuid.NewString(), uuid.NewString()
		controlPath, sessionPath := "/v1/workflows/"+controlID, "/v1/workflows/"+sessionID
		require.Equal(t, http.StatusOK, fixture.request(t, "first", http.MethodPut, controlPath, `{"kind":"conversation_control","expected_revision":0,"payload":{"enabled":true}}`).Code)
		optOutRequest := signedGovernanceRequestAs(t, key, appV23LookupVaultID(key), http.MethodPut, controlPath,
			[]byte(`{"kind":"conversation_control","expected_revision":1,"payload":{"enabled":false}}`))
		sessionRequest := signedGovernanceRequestAs(t, key, appV23LookupVaultID(key), http.MethodPut, sessionPath, []byte(workflowGuardBody(controlID, 0, 1)))
		start := make(chan struct{})
		optOut, session := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
		for _, work := range []struct {
			request *http.Request
			result  chan *httptest.ResponseRecorder
		}{{optOutRequest, optOut}, {sessionRequest, session}} {
			go func() {
				<-start
				response := httptest.NewRecorder()
				router.ServeHTTP(response, work.request)
				work.result <- response
			}()
		}
		close(start)
		require.Equal(t, http.StatusOK, (<-optOut).Code)
		result := <-session
		require.Contains(t, []int{http.StatusOK, http.StatusConflict}, result.Code, result.Body.String())
		require.Equal(t, "no-store", result.Header().Get("Cache-Control"))
		read := fixture.request(t, "first", http.MethodGet, sessionPath, "")
		if result.Code == http.StatusOK {
			require.Equal(t, http.StatusOK, read.Code)
			require.JSONEq(t, result.Body.String(), read.Body.String())
		} else {
			require.Equal(t, http.StatusNotFound, read.Code)
		}
		require.Equal(t, http.StatusConflict, fixture.request(t, "first", http.MethodPut, sessionPath, workflowGuardBody(controlID, 0, 1)).Code)
	}
}
