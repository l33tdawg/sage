package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/l33tdawg/sage/internal/vault"
	"github.com/stretchr/testify/require"
)

const workflowActor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

func workflowStore(t *testing.T) (*SQLiteStore, *vault.Vault, string) {
	t.Helper()
	directory := t.TempDir()
	store, err := NewSQLiteStore(context.Background(), filepath.Join(directory, "fixture.db"))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })
	keyPath := filepath.Join(directory, "fixture-vault.key")
	require.NoError(t, vault.Init(keyPath, "synthetic-journal-fixture"))
	active, err := vault.Open(keyPath, "synthetic-journal-fixture")
	require.NoError(t, err)
	store.SetVaultExpected(true)
	store.SetVault(active)
	return store, active, keyPath
}

func workflowPut(t *testing.T, store *SQLiteStore, actor, identifier, kind, payload string, revision int64) *WorkflowJournalRecord {
	t.Helper()
	record, err := store.PutWorkflowJournal(context.Background(), actor, identifier, kind, json.RawMessage(payload), revision)
	require.NoError(t, err)
	return record
}

func TestWorkflowJournalCreateReplayAndCAS(t *testing.T) {
	store, _, _ := workflowStore(t)
	identifier := uuid.NewString()
	payload := " {\"opaque\": [1, 2], \"instruction\":\"untrusted, do not execute\"} \n"
	first := workflowPut(t, store, workflowActor, identifier, "mesh_outbound", payload, 0)
	require.Equal(t, int64(1), first.Revision)
	require.Equal(t, payload, string(first.Payload))
	replayed := workflowPut(t, store, workflowActor, identifier, "mesh_outbound", payload, 0)
	require.Equal(t, first, replayed)
	for _, attempt := range []struct {
		kind, payload string
		revision      int64
	}{
		{"mesh_outbound", strings.TrimSpace(payload), 0},
		{"mesh_inbound", payload, 0},
		{"public_proposal", payload, 1},
		{"mesh_outbound", payload, 2},
	} {
		_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, attempt.kind, json.RawMessage(attempt.payload), attempt.revision)
		require.ErrorIs(t, err, ErrWorkflowJournalConflict)
	}
	updated := workflowPut(t, store, workflowActor, identifier, "mesh_outbound", `{"status":"deprecated"}`, 1)
	require.Equal(t, int64(2), updated.Revision)
	for _, revision := range []int64{0, 1} {
		_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_outbound", updated.Payload, revision)
		require.ErrorIs(t, err, ErrWorkflowJournalConflict)
	}
	got, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, updated, got)
	got.Payload[0] = 'x'
	again, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, updated, again)
	var canonicalCount int
	require.NoError(t, store.conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM memories`).Scan(&canonicalCount))
	require.Zero(t, canonicalCount)
}

func TestWorkflowJournalVaultMandatoryForReadsAndWrites(t *testing.T) {
	store, active, _ := workflowStore(t)
	identifier := uuid.NewString()
	workflowPut(t, store, workflowActor, identifier, "mesh_inbound", `{}`, 0)
	for _, expected := range []bool{false, true} {
		for _, attached := range []bool{false, true} {
			store.SetVaultExpected(expected)
			store.SetVault(nil)
			if attached {
				store.SetVault(active)
			}
			_, getErr := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
			_, putErr := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_inbound", json.RawMessage(`{}`), 0)
			if expected && attached {
				require.NoError(t, getErr)
				require.NoError(t, putErr)
			} else {
				require.ErrorIs(t, getErr, ErrWorkflowJournalVaultRequired)
				require.ErrorIs(t, putErr, ErrWorkflowJournalVaultRequired)
			}
		}
	}
}

func TestWorkflowJournalStrictArguments(t *testing.T) {
	store, _, _ := workflowStore(t)
	identifier := uuid.NewString()
	for _, payload := range [][]byte{
		nil, []byte(` `), []byte(`{"a":1,"a":2}`), []byte(`{"a":1,"\u0061":2}`),
		[]byte(`[{"a":1,"a":2}]`), []byte(`NaN`), []byte(`Infinity`), []byte(`{} {}`),
		[]byte(`1e99999`), []byte(`{"value":-1e9999}`), []byte(`"\ud800"`), []byte(`"\udc00"`),
		[]byte(`"\ud800\u0041"`), []byte(`"\ud800\ud800"`),
		[]byte(`{"x":}`), []byte(`{"x":1,}`), {0xff},
		[]byte(strings.Repeat("[", 65) + "0" + strings.Repeat("]", 65)),
		[]byte(`"` + strings.Repeat("x", MaxWorkflowJournalPayload) + `"`),
	} {
		_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_inbound", payload, 0)
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	for _, actor := range []string{"", "alice", strings.ToUpper(workflowActor), workflowActor + "a", strings.Repeat("g", 64)} {
		_, err := store.GetWorkflowJournal(context.Background(), actor, identifier)
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	for _, badID := range []string{"", "not-uuid", strings.ToUpper(identifier), "{" + identifier + "}", uuid.Nil.String()} {
		_, err := store.PutWorkflowJournal(context.Background(), workflowActor, badID, "mesh_inbound", json.RawMessage(`{}`), 0)
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	for _, revision := range []int64{-1, MaxWorkflowJournalRevision, MaxWorkflowJournalRevision + 1, math.MaxInt64} {
		_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_inbound", json.RawMessage(`{}`), revision)
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "canonical_memory", json.RawMessage(`{}`), 0)
	require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	for _, payload := range []string{`null`, `true`, `1e99`, `[]`, `"\ud83d\ude00"`, `"\\ud800"`, `"` + strings.Repeat("x", MaxWorkflowJournalPayload-2) + `"`} {
		workflowPut(t, store, workflowActor, uuid.NewString(), "public_proposal", payload, 0)
	}
}

func TestWorkflowJournalActorIsolationAndMissingUpdate(t *testing.T) {
	store, _, _ := workflowStore(t)
	identifier := uuid.NewString()
	workflowPut(t, store, workflowActor, identifier, "mesh_outbound", `{"value":1}`, 0)
	other := strings.Repeat("b", 64)
	_, err := store.GetWorkflowJournal(context.Background(), other, identifier)
	require.ErrorIs(t, err, ErrWorkflowJournalNotFound)
	_, err = store.PutWorkflowJournal(context.Background(), other, identifier, "mesh_outbound", json.RawMessage(`{}`), 1)
	require.ErrorIs(t, err, ErrWorkflowJournalConflict)
	workflowPut(t, store, other, identifier, "public_proposal", `{"value":2}`, 0)
	got, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, `{"value":1}`, string(got.Payload))
}

func TestWorkflowJournalConcurrentCASOneWinner(t *testing.T) {
	store, _, _ := workflowStore(t)
	identifier := uuid.NewString()
	workflowPut(t, store, workflowActor, identifier, "mesh_outbound", `{}`, 0)
	var workers sync.WaitGroup
	results := make(chan error, 16)
	for index := range 16 {
		workers.Add(1)
		go func(value int) {
			defer workers.Done()
			payload, _ := json.Marshal(map[string]int{"value": value})
			_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_outbound", payload, 1)
			results <- err
		}(index)
	}
	workers.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, ErrWorkflowJournalConflict)
		}
	}
	require.Equal(t, 1, winners)
	got, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Revision)
}

func TestWorkflowJournalSealedBindingAndCorruption(t *testing.T) {
	store, _, _ := workflowStore(t)
	for _, attack := range []string{"record", "actor", "revision", "corrupt", "plaintext"} {
		t.Run(attack, func(t *testing.T) {
			identifier := uuid.NewString()
			workflowPut(t, store, workflowActor, identifier, "public_proposal", `{"private":"fixture-secret"}`, 0)
			var sealed []byte
			require.NoError(t, store.conn.QueryRowContext(context.Background(), `SELECT sealed_record FROM workflow_journal WHERE agent_id=? AND record_id=?`, workflowActor, identifier).Scan(&sealed))
			require.False(t, bytes.Contains(sealed, []byte("fixture-secret")))
			require.False(t, bytes.Contains(sealed, []byte("public_proposal")))
			actor := workflowActor
			switch attack {
			case "record":
				target := uuid.NewString()
				_, err := store.writeExecContext(context.Background(), `UPDATE workflow_journal SET record_id=? WHERE agent_id=? AND record_id=?`, target, actor, identifier)
				require.NoError(t, err)
				identifier = target
			case "actor":
				target := strings.Repeat("b", 64)
				_, err := store.writeExecContext(context.Background(), `UPDATE workflow_journal SET agent_id=? WHERE agent_id=? AND record_id=?`, target, actor, identifier)
				require.NoError(t, err)
				actor = target
			case "revision":
				_, err := store.writeExecContext(context.Background(), `UPDATE workflow_journal SET revision=2 WHERE agent_id=? AND record_id=?`, actor, identifier)
				require.NoError(t, err)
			default:
				if attack == "plaintext" {
					sealed = []byte(`{"kind":"public_proposal","payload":{}}`)
				} else {
					sealed[len(sealed)-1] ^= 1
				}
				_, err := store.writeExecContext(context.Background(), `UPDATE workflow_journal SET sealed_record=? WHERE agent_id=? AND record_id=?`, sealed, actor, identifier)
				require.NoError(t, err)
			}
			_, err := store.GetWorkflowJournal(context.Background(), actor, identifier)
			require.ErrorIs(t, err, ErrWorkflowJournalCorrupt)
			_, err = store.PutWorkflowJournal(context.Background(), actor, identifier, "public_proposal", json.RawMessage(`{}`), 0)
			require.ErrorIs(t, err, ErrWorkflowJournalCorrupt)
		})
	}
}

func TestWorkflowJournalRestartAndWrongKey(t *testing.T) {
	store, active, keyPath := workflowStore(t)
	identifier := uuid.NewString()
	workflowPut(t, store, workflowActor, identifier, "mesh_inbound", `{ "opaque": true }`, 0)
	workflowPut(t, store, workflowActor, identifier, "mesh_inbound", `{ "opaque": false }`, 1)
	path := store.dbPath
	require.NoError(t, store.Close())
	reopened, err := NewSQLiteStore(context.Background(), path)
	require.NoError(t, err)
	defer reopened.Close()
	reopened.SetVaultExpected(true)
	_, err = reopened.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.ErrorIs(t, err, ErrWorkflowJournalVaultRequired)
	active, err = vault.Open(keyPath, "synthetic-journal-fixture")
	require.NoError(t, err)
	reopened.SetVault(active)
	got, err := reopened.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, int64(2), got.Revision)
	require.Equal(t, `{ "opaque": false }`, string(got.Payload))
	_, wrongKey, _ := workflowStore(t)
	reopened.SetVault(wrongKey)
	_, err = reopened.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.ErrorIs(t, err, ErrWorkflowJournalCorrupt)
}

func TestWorkflowJournalQuotasAndMetadataOnlySchema(t *testing.T) {
	store, _, _ := workflowStore(t)
	identifier := uuid.NewString()
	workflowPut(t, store, workflowActor, identifier, "mesh_outbound", `{}`, 0)
	_, err := store.writeExecContext(context.Background(), `WITH RECURSIVE counts(number) AS (
		SELECT 1 UNION ALL SELECT number+1 FROM counts WHERE number < 4095)
		INSERT INTO workflow_journal SELECT ?, printf('%08x-0000-4000-8000-000000000000', number), 1,
		(SELECT sealed_record FROM workflow_journal WHERE agent_id=? AND record_id=?) FROM counts`, workflowActor, workflowActor, identifier)
	require.NoError(t, err)
	_, err = store.PutWorkflowJournal(context.Background(), workflowActor, uuid.NewString(), "mesh_outbound", json.RawMessage(`{}`), 0)
	require.ErrorIs(t, err, ErrWorkflowJournalLimit)
	workflowPut(t, store, workflowActor, identifier, "mesh_outbound", `{}`, 0)
	workflowPut(t, store, workflowActor, identifier, "mesh_outbound", `{"next":true}`, 1)
	for _, actor := range []string{strings.Repeat("b", 64), strings.Repeat("c", 64), strings.Repeat("d", 64)} {
		_, err := store.writeExecContext(context.Background(), `INSERT INTO workflow_journal SELECT ?, record_id, revision, sealed_record FROM workflow_journal WHERE agent_id=?`, actor, workflowActor)
		require.NoError(t, err)
	}
	_, err = store.PutWorkflowJournal(context.Background(), strings.Repeat("e", 64), uuid.NewString(), "mesh_inbound", json.RawMessage(`{}`), 0)
	require.ErrorIs(t, err, ErrWorkflowJournalLimit)
	rows, err := store.conn.QueryContext(context.Background(), `PRAGMA table_info(workflow_journal)`)
	require.NoError(t, err)
	defer rows.Close()
	var columns []string
	for rows.Next() {
		var position, notNull, primary int
		var name, kind string
		var defaultValue any
		require.NoError(t, rows.Scan(&position, &name, &kind, &notNull, &defaultValue, &primary))
		columns = append(columns, name)
	}
	require.NoError(t, rows.Err())
	require.Equal(t, []string{"agent_id", "record_id", "revision", "sealed_record"}, columns)
}

type workflowBlockedConnection struct {
	sqlQuerier
	entered chan struct{}
	release chan struct{}
}

func (connection workflowBlockedConnection) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	close(connection.entered)
	<-connection.release
	return connection.sqlQuerier.QueryRowContext(ctx, query, args...)
}

func TestWorkflowJournalReadHoldsVaultPublicationLock(t *testing.T) {
	store, _, _ := workflowStore(t)
	identifier := uuid.NewString()
	workflowPut(t, store, workflowActor, identifier, "mesh_inbound", `{}`, 0)
	connection := workflowBlockedConnection{store.conn, make(chan struct{}), make(chan struct{})}
	store.conn = connection
	readDone, lockDone := make(chan error, 1), make(chan struct{})
	go func() {
		_, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
		readDone <- err
	}()
	<-connection.entered
	go func() { store.SetVault(nil); close(lockDone) }()
	select {
	case <-lockDone:
		t.Fatal("vault changed before read/decrypt completed")
	case <-time.After(20 * time.Millisecond):
	}
	close(connection.release)
	require.NoError(t, <-readDone)
	<-lockDone
	store.conn = connection.sqlQuerier
}

func TestWorkflowJournalWriteHoldsVaultThroughCommit(t *testing.T) {
	store, active, _ := workflowStore(t)
	identifier := uuid.NewString()
	store.db.SetMaxOpenConns(1)
	connection, err := store.db.Conn(context.Background())
	require.NoError(t, err)
	defer connection.Close()
	writeDone, lockDone := make(chan error, 1), make(chan struct{})
	go func() {
		_, err := store.PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_inbound", json.RawMessage(`{}`), 0)
		writeDone <- err
	}()
	deadline := time.Now().Add(time.Second)
	for store.writeMu.TryLock() {
		store.writeMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("writer never acquired vault gate")
		}
		time.Sleep(time.Millisecond)
	}
	go func() { store.SetVault(nil); close(lockDone) }()
	select {
	case <-lockDone:
		t.Fatal("vault changed before write committed")
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, connection.Close())
	require.NoError(t, <-writeDone)
	<-lockDone
	store.SetVault(active)
	got, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, int64(1), got.Revision)
}

func TestWorkflowJournalQueuedWriteCancellationAllowsVaultPublication(t *testing.T) {
	store, _, _ := workflowStore(t)
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started, done := make(chan struct{}), make(chan error, 1)
	go func() {
		close(started)
		_, err := store.PutWorkflowJournal(ctx, workflowActor, uuid.NewString(), "mesh_inbound", json.RawMessage(`{}`), 0)
		done <- err
	}()
	<-started
	publicationDone := make(chan struct{})
	go func() { store.SetVault(nil); close(publicationDone) }()
	select {
	case <-publicationDone:
	case <-time.After(time.Second):
		t.Fatal("queued write blocked vault publication")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued write ignored cancellation")
	}
}

func TestWorkflowJournalSafeRevisionBoundary(t *testing.T) {
	store, active, _ := workflowStore(t)
	identifier := uuid.NewString()
	record := sealedWorkflowJournal{Schema: workflowJournalSchema, AgentID: workflowActor,
		RecordID: identifier, Revision: MaxWorkflowJournalRevision - 1, Kind: "mesh_inbound", Payload: `{}`}
	plaintext, err := json.Marshal(record)
	require.NoError(t, err)
	sealed, err := active.Encrypt(plaintext)
	require.NoError(t, err)
	_, err = store.writeExecContext(context.Background(), `INSERT INTO workflow_journal (agent_id, record_id, revision, sealed_record) VALUES (?, ?, ?, ?)`, workflowActor, identifier, record.Revision, sealed)
	require.NoError(t, err)
	updated := workflowPut(t, store, workflowActor, identifier, record.Kind, `{"final":true}`, record.Revision)
	require.Equal(t, MaxWorkflowJournalRevision, updated.Revision)
	got, err := store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.NoError(t, err)
	require.Equal(t, updated, got)
	_, err = store.PutWorkflowJournal(context.Background(), workflowActor, identifier, record.Kind, json.RawMessage(`{}`), MaxWorkflowJournalRevision)
	require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	_, err = store.writeExecContext(context.Background(), `UPDATE workflow_journal SET revision = ? WHERE agent_id = ? AND record_id = ?`, MaxWorkflowJournalRevision+1, workflowActor, identifier)
	require.Error(t, err)
}

func TestWorkflowJournalCancelledAndTransactionScopedCalls(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	identifier := uuid.NewString()
	_, err := store.PutWorkflowJournal(ctx, workflowActor, identifier, "mesh_inbound", json.RawMessage(`{}`), 0)
	require.True(t, errors.Is(err, context.Canceled))
	_, err = store.GetWorkflowJournal(context.Background(), workflowActor, identifier)
	require.ErrorIs(t, err, ErrWorkflowJournalNotFound)
	require.NoError(t, store.RunInTx(context.Background(), func(transaction OffchainStore) error {
		_, err := transaction.(*SQLiteStore).GetWorkflowJournal(context.Background(), workflowActor, identifier)
		require.ErrorIs(t, err, ErrWorkflowJournalStandaloneRequired)
		_, err = transaction.(*SQLiteStore).PutWorkflowJournal(context.Background(), workflowActor, identifier, "mesh_inbound", json.RawMessage(`{}`), 0)
		require.ErrorIs(t, err, ErrWorkflowJournalStandaloneRequired)
		return nil
	}))
}

func TestWorkflowJournalListBoundedDiscovery(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	empty, err := store.ListWorkflowJournal(ctx, workflowActor, "", 50)
	require.NoError(t, err)
	require.Empty(t, empty.Records)
	require.NotNil(t, empty.Records)
	require.False(t, empty.More)
	require.Empty(t, empty.NextCursor)
	identifiers := make([]string, 51)
	for index := range identifiers {
		identifiers[index] = uuid.NewString()
		workflowPut(t, store, workflowActor, identifiers[index], "mesh_outbound", `{}`, 0)
	}
	sort.Strings(identifiers)
	workflowPut(t, store, strings.Repeat("b", 64), uuid.NewString(), "mesh_inbound", `{}`, 0)
	page, err := store.ListWorkflowJournal(ctx, workflowActor, "", 50)
	require.NoError(t, err)
	require.Len(t, page.Records, 50)
	require.True(t, page.More)
	require.Equal(t, identifiers[49], page.NextCursor)
	for index, record := range page.Records {
		require.Equal(t, identifiers[index], record.RecordID)
		require.Equal(t, workflowActor, record.AgentID)
	}
	last, err := store.ListWorkflowJournal(ctx, workflowActor, page.NextCursor, 50)
	require.NoError(t, err)
	require.Len(t, last.Records, 1)
	require.Equal(t, identifiers[50], last.Records[0].RecordID)
	require.False(t, last.More)
	require.Empty(t, last.NextCursor)
	for _, limit := range []int{0, -1, 51} {
		_, err := store.ListWorkflowJournal(ctx, workflowActor, "", limit)
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	_, err = store.ListWorkflowJournal(ctx, workflowActor, "bad", 1)
	require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	_, err = store.ListWorkflowJournal(ctx, "bad", "", 1)
	require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	_, err = store.writeExecContext(ctx, `UPDATE workflow_journal SET sealed_record = ? WHERE agent_id = ? AND record_id = ?`, []byte(`{}`), workflowActor, identifiers[50])
	require.NoError(t, err)
	_, err = store.ListWorkflowJournal(ctx, workflowActor, "", 50)
	require.ErrorIs(t, err, ErrWorkflowJournalCorrupt)
	store.SetVault(nil)
	_, err = store.ListWorkflowJournal(ctx, workflowActor, "", 50)
	require.ErrorIs(t, err, ErrWorkflowJournalVaultRequired)
}

func TestWorkflowJournalConversationGuardLifecycle(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	controlID, sessionID := uuid.NewString(), uuid.NewString()
	control := workflowPut(t, store, workflowActor, controlID, "conversation_control", `{"enabled":true}`, 0)
	guard := &WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: control.Revision}
	payload := json.RawMessage(`{"interaction":{"state":"queued"},"reflection":{"state":"queued"}}`)
	created, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", payload, 0, guard)
	require.NoError(t, err)
	require.Equal(t, int64(1), created.Revision)
	replayed, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", payload, 0, guard)
	require.NoError(t, err)
	require.Equal(t, created, replayed)
	unchanged, err := store.GetWorkflowJournal(ctx, workflowActor, controlID)
	require.NoError(t, err)
	require.Equal(t, control, unchanged)
	workflowPut(t, store, workflowActor, controlID, "conversation_control", `{"enabled":false}`, 1)
	for _, expected := range []int64{0, 1} {
		_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", payload, expected, guard)
		require.ErrorIs(t, err, ErrWorkflowJournalConflict)
	}
	unchanged, err = store.GetWorkflowJournal(ctx, workflowActor, sessionID)
	require.NoError(t, err)
	require.Equal(t, created, unchanged)
	guard.ExpectedRevision = 2
	updated, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{"suppressed":true}`), 1, guard)
	require.NoError(t, err)
	require.Equal(t, int64(2), updated.Revision)
	_, err = store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", payload, 1, guard)
	require.ErrorIs(t, err, ErrWorkflowJournalConflict)
	page, err := store.ListWorkflowJournal(ctx, workflowActor, "", 50)
	require.NoError(t, err)
	require.Len(t, page.Records, 2)
	workflowPut(t, store, workflowActor, uuid.NewString(), "conversation_session", `{}`, 0)
}

func TestWorkflowJournalGuardRejectsMalformedArguments(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	controlID, sessionID := uuid.NewString(), uuid.NewString()
	workflowPut(t, store, workflowActor, controlID, "conversation_control", `{}`, 0)
	for _, identifier := range []string{"", "bad", uuid.Nil.String(), "{" + controlID + "}", strings.ToUpper(controlID), sessionID} {
		_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{}`), 0,
			&WorkflowJournalGuard{RecordID: identifier, ExpectedRevision: 1})
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	for _, revision := range []int64{-1, 0, MaxWorkflowJournalRevision + 1, math.MaxInt64} {
		_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{}`), 0,
			&WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: revision})
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	for _, kind := range []string{"mesh_outbound", "mesh_inbound", "public_proposal", "conversation_control", "bad"} {
		_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, kind, json.RawMessage(`{}`), 0,
			&WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: 1})
		require.ErrorIs(t, err, ErrWorkflowJournalInvalid)
	}
	_, err := store.GetWorkflowJournal(ctx, workflowActor, sessionID)
	require.ErrorIs(t, err, ErrWorkflowJournalNotFound)
}

func TestWorkflowJournalGuardFailuresNeverMutateTarget(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	for _, failure := range []string{"missing", "stale", "other_actor", "wrong_kind", "corrupt", "cipher_binding"} {
		t.Run(failure, func(t *testing.T) {
			controlID, sessionID := uuid.NewString(), uuid.NewString()
			actor, kind := workflowActor, "conversation_control"
			if failure == "other_actor" {
				actor = strings.Repeat("b", 64)
			}
			if failure == "wrong_kind" {
				kind = "public_proposal"
			}
			if failure != "missing" {
				workflowPut(t, store, actor, controlID, kind, `{}`, 0)
			}
			guard := &WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: 1}
			if failure == "stale" {
				guard.ExpectedRevision = 2
			}
			want := ErrWorkflowJournalConflict
			if failure == "corrupt" {
				_, err := store.writeExecContext(ctx, `UPDATE workflow_journal SET sealed_record=? WHERE agent_id=? AND record_id=?`, []byte(`{}`), actor, controlID)
				require.NoError(t, err)
				want = ErrWorkflowJournalCorrupt
			}
			if failure == "cipher_binding" {
				replacementID := uuid.NewString()
				_, err := store.writeExecContext(ctx, `UPDATE workflow_journal SET record_id=? WHERE agent_id=? AND record_id=?`, replacementID, actor, controlID)
				require.NoError(t, err)
				guard.RecordID = replacementID
				want = ErrWorkflowJournalCorrupt
			}
			_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{}`), 0, guard)
			require.ErrorIs(t, err, want)
			_, err = store.GetWorkflowJournal(ctx, workflowActor, sessionID)
			require.ErrorIs(t, err, ErrWorkflowJournalNotFound)
			original := workflowPut(t, store, workflowActor, sessionID, "conversation_session", `{"state":"queued"}`, 0)
			_, err = store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{"state":"dispatching"}`), 1, guard)
			require.ErrorIs(t, err, want)
			unchanged, err := store.GetWorkflowJournal(ctx, workflowActor, sessionID)
			require.NoError(t, err)
			require.Equal(t, original, unchanged)
		})
	}
}

func TestWorkflowJournalGuardOptOutSerializesWithCAS(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	for attempt := 0; attempt < 20; attempt++ {
		controlID, sessionID := uuid.NewString(), uuid.NewString()
		workflowPut(t, store, workflowActor, controlID, "conversation_control", `{"enabled":true}`, 0)
		workflowPut(t, store, workflowActor, sessionID, "conversation_session", `{"state":"queued"}`, 0)
		start := make(chan struct{})
		optOut, dispatch := make(chan error, 1), make(chan error, 1)
		go func() {
			<-start
			_, err := store.PutWorkflowJournal(ctx, workflowActor, controlID, "conversation_control", json.RawMessage(`{"enabled":false}`), 1)
			optOut <- err
		}()
		go func() {
			<-start
			_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{"state":"dispatching"}`), 1,
				&WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: 1})
			dispatch <- err
		}()
		close(start)
		require.NoError(t, <-optOut)
		dispatchErr := <-dispatch
		if dispatchErr != nil {
			require.ErrorIs(t, dispatchErr, ErrWorkflowJournalConflict)
		}
		result, err := store.GetWorkflowJournal(ctx, workflowActor, sessionID)
		require.NoError(t, err)
		if dispatchErr == nil {
			require.Equal(t, int64(2), result.Revision)
		} else {
			require.Equal(t, int64(1), result.Revision)
		}
		_, err = store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", result.Payload, result.Revision,
			&WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: 1})
		require.ErrorIs(t, err, ErrWorkflowJournalConflict)
	}
}

func TestWorkflowJournalGuardConcurrentTargetCAS(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	controlID, sessionID := uuid.NewString(), uuid.NewString()
	workflowPut(t, store, workflowActor, controlID, "conversation_control", `{}`, 0)
	workflowPut(t, store, workflowActor, sessionID, "conversation_session", `{}`, 0)
	results := make(chan error, 16)
	for attempt := 0; attempt < 16; attempt++ {
		go func() {
			_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{"reserved":true}`), 1,
				&WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: 1})
			results <- err
		}()
	}
	winners := 0
	for attempt := 0; attempt < 16; attempt++ {
		if err := <-results; err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, ErrWorkflowJournalConflict)
		}
	}
	require.Equal(t, 1, winners)
	store.SetVault(nil)
	_, err := store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{}`), 2,
		&WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: 1})
	require.ErrorIs(t, err, ErrWorkflowJournalVaultRequired)
}

func TestWorkflowJournalGuardMaximumReadRevisionAndReopen(t *testing.T) {
	store, active, keyPath := workflowStore(t)
	ctx := context.Background()
	controlID, sessionID := uuid.NewString(), uuid.NewString()
	control := sealedWorkflowJournal{Schema: workflowJournalSchema, AgentID: workflowActor, RecordID: controlID,
		Revision: MaxWorkflowJournalRevision, Kind: "conversation_control", Payload: `{}`}
	plaintext, err := json.Marshal(control)
	require.NoError(t, err)
	sealed, err := active.Encrypt(plaintext)
	require.NoError(t, err)
	_, err = store.writeExecContext(ctx, `INSERT INTO workflow_journal (agent_id,record_id,revision,sealed_record) VALUES (?,?,?,?)`,
		workflowActor, controlID, control.Revision, sealed)
	require.NoError(t, err)
	guard := &WorkflowJournalGuard{RecordID: controlID, ExpectedRevision: MaxWorkflowJournalRevision}
	_, err = store.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{}`), 0, guard)
	require.NoError(t, err)
	path := store.dbPath
	require.NoError(t, store.Close())
	reopened, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	defer reopened.Close()
	reopened.SetVaultExpected(true)
	active, err = vault.Open(keyPath, "synthetic-journal-fixture")
	require.NoError(t, err)
	reopened.SetVault(active)
	updated, err := reopened.PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{"reopened":true}`), 1, guard)
	require.NoError(t, err)
	require.Equal(t, int64(2), updated.Revision)
	require.NoError(t, reopened.RunInTx(ctx, func(transaction OffchainStore) error {
		_, err := transaction.(*SQLiteStore).PutWorkflowJournalGuarded(ctx, workflowActor, sessionID, "conversation_session", json.RawMessage(`{}`), 2, guard)
		require.ErrorIs(t, err, ErrWorkflowJournalStandaloneRequired)
		return nil
	}))
}
