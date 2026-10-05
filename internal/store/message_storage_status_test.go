package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/vault"
	"github.com/stretchr/testify/require"
)

func TestMessageStorageStatusTracksVaultLifecycle(t *testing.T) {
	directory := t.TempDir()
	database, err := NewSQLiteStore(context.Background(), filepath.Join(directory, "store.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, database.Close()) })
	initial := database.MessageStorageStatus()
	message := testLocalMessage("storage-probe", "alice", "bob", "private test")
	_, _, err = database.SendEncryptedLocalMessage(context.Background(), "probe", message)
	require.ErrorIs(t, err, ErrEncryptedMessageStorageRequired)
	require.False(t, initial.EncryptionExpected)
	require.False(t, initial.VaultActive)
	require.True(t, initial.Stable)
	database.SetVaultExpected(true)
	_, _, err = database.SendEncryptedLocalMessage(context.Background(), "probe", message)
	require.ErrorIs(t, err, ErrEncryptedMessageStorageRequired)
	locked := database.MessageStorageStatus()
	require.True(t, locked.EncryptionExpected)
	require.False(t, locked.VaultActive)
	require.Greater(t, locked.VaultGeneration, initial.VaultGeneration)
	keyPath := filepath.Join(directory, "vault.key")
	require.NoError(t, vault.Init(keyPath, "synthetic-passphrase"))
	opened, err := vault.Open(keyPath, "synthetic-passphrase")
	require.NoError(t, err)
	database.SetVault(opened)
	unlocked := database.MessageStorageStatus()
	require.True(t, unlocked.VaultActive)
	require.True(t, unlocked.EncryptionExpected)
	require.True(t, unlocked.Stable)
	require.Greater(t, unlocked.VaultGeneration, locked.VaultGeneration)
	database.SetVault(nil)
	database.SetVault(opened)
	require.Greater(t, database.MessageStorageStatus().VaultGeneration, unlocked.VaultGeneration)
}

func TestEncryptedMessageQueuedCancellationDoesNotBlockVault(t *testing.T) {
	store, _, _ := workflowStore(t)
	store.writeMu.Lock()
	defer store.writeMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, _, err := store.SendEncryptedLocalMessage(ctx, "cancelled", testLocalMessage("cancelled", "alice", "bob", "synthetic"))
		done <- err
	}()
	publication := make(chan struct{})
	go func() { store.SetVault(nil); close(publication) }()
	select {
	case <-publication:
	case <-time.After(time.Second):
		t.Fatal("queued encrypted message blocked publication")
	}
	cancel()
	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("queued encrypted message ignored cancellation")
	}
	var count int
	require.NoError(t, store.conn.QueryRowContext(context.Background(), `SELECT count(*) FROM pipeline_messages`).Scan(&count))
	require.Zero(t, count)
}

func TestEncryptedMessageCommitVaultGateAndReplay(t *testing.T) {
	store, active, _ := workflowStore(t)
	ctx := context.Background()
	store.db.SetMaxOpenConns(1)
	connection, err := store.db.Conn(ctx)
	require.NoError(t, err)
	defer connection.Close()
	message := testLocalMessage("protected", "alice", "bob", "synthetic-secret")
	done := make(chan error, 1)
	go func() {
		_, _, err := store.SendEncryptedLocalMessage(ctx, "protected-key", message)
		done <- err
	}()
	deadline := time.Now().Add(time.Second)
	for store.writeMu.TryLock() {
		store.writeMu.Unlock()
		if time.Now().After(deadline) {
			t.Fatal("encrypted message did not acquire admission gate")
		}
		time.Sleep(time.Millisecond)
	}
	publication := make(chan struct{})
	go func() { store.SetVault(nil); close(publication) }()
	select {
	case <-publication:
		t.Fatal("vault changed before protected transaction finished")
	case <-time.After(20 * time.Millisecond):
	}
	require.NoError(t, connection.Close())
	require.NoError(t, <-done)
	<-publication
	store.SetVault(active)
	var sealed string
	require.NoError(t, store.conn.QueryRowContext(ctx, `SELECT payload FROM pipeline_messages WHERE pipe_id = ?`, message.PipeID).Scan(&sealed))
	require.NotContains(t, sealed, message.Payload)
	got, replayed, err := store.SendEncryptedLocalMessage(ctx, "protected-key", message)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, message.Payload, got.Payload)
	var sequence int
	require.NoError(t, store.conn.QueryRowContext(ctx, `SELECT seq FROM message_wake_state WHERE recipient_agent_id = 'bob'`).Scan(&sequence))
	require.Equal(t, 1, sequence)
}

func TestEncryptedMessageRollbackAndRejectTransactionClone(t *testing.T) {
	store, _, _ := workflowStore(t)
	ctx := context.Background()
	_, err := store.writeExecContext(ctx, `CREATE TRIGGER fail_protected_send BEFORE INSERT ON message_send_idempotency BEGIN SELECT RAISE(ABORT, 'synthetic failure'); END`)
	require.NoError(t, err)
	result, _, err := store.SendEncryptedLocalMessage(ctx, "rollback", testLocalMessage("rollback", "alice", "bob", "synthetic"))
	require.Error(t, err)
	require.Nil(t, result)
	for _, table := range []string{"pipeline_messages", "message_wake_state", "message_send_idempotency"} {
		var count int
		require.NoError(t, store.conn.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&count))
		require.Zero(t, count)
	}
	require.NoError(t, store.RunInTx(ctx, func(transaction OffchainStore) error {
		_, _, err := transaction.(*SQLiteStore).SendEncryptedLocalMessage(ctx, "nested", testLocalMessage("nested", "alice", "bob", "synthetic"))
		require.ErrorIs(t, err, ErrEncryptedMessageStandaloneRequired)
		return nil
	}))
}
