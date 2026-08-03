package store

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func importedReceiptOutboxFixture(t *testing.T, s *SQLiteStore) (*PipelineMessage, *FederatedReceiptOutbox) {
	t.Helper()
	msg, dedup := receiptNegotiationInboundFixture(FederatedPipelineReceiptVersion)
	_, duplicate, err := s.AdmitFederatedPipeline(context.Background(), msg, dedup)
	require.NoError(t, err)
	require.False(t, duplicate)
	binding, err := s.GetFederatedReceiptInbound(context.Background(), msg.PipeID)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Second)
	event := &FederatedReceiptOutbox{
		FederatedReceiptBinding: *binding,
		EventID:                 "receipt-outbox-claim",
		RecipientPipeID:         msg.PipeID,
		Kind:                    "claimed",
		EventAt:                 now,
		Proof: PipelineAgentProof{
			AgentID: binding.RecipientAgentID, Signature: make([]byte, ed25519.SignatureSize),
			Timestamp: now.Unix(), Nonce: []byte("nonce-v2"),
			CanonicalRequest: []byte("PUT /v1/pipe/" + msg.PipeID + "/receipt/claimed\n{}"),
		},
		CreatedAt: now, NextAttemptAt: now, ExpiresAt: now.Add(time.Hour),
	}
	return msg, event
}

func newReceiptV2SecurityStore(t *testing.T) *SQLiteStore {
	t.Helper()
	store, err := NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "receipts.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	return store
}

func receiptV2SecurityFixture(t *testing.T, store *SQLiteStore) FederatedReceiptEvent {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := strings.Repeat("a", 64)
	recipient := strings.Repeat("b", 64)
	message := &PipelineMessage{
		PipeID: "pipe-receipt-security", FromAgent: sender, ToAgent: recipient,
		DestinationChainID: "chain-recipient", Status: "pending", Payload: "payload",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, store.InsertPipeline(context.Background(), message))
	proofHash := sha256.Sum256([]byte("claim-proof"))
	return FederatedReceiptEvent{
		FederatedReceiptBinding: FederatedReceiptBinding{
			MessageID: "message-security", LocalPipeID: message.PipeID,
			SenderChainID: "chain-sender", RecipientChainID: message.DestinationChainID,
			SenderAgentID: sender, RecipientAgentID: recipient,
			ContentDigest: strings.Repeat("1", 64), PolicyEpoch: "epoch-1",
			AgreementID: strings.Repeat("2", 64), ContactID: strings.Repeat("3", 64),
			ContactRevision: strings.Repeat("4", 64),
		},
		EventID: "receipt-claim", Kind: "claimed", EventAt: now, ProofHash: proofHash[:],
	}
}

func TestReceiptV2RejectsCrossKindBindingEquivocationAtomically(t *testing.T) {
	ctx := context.Background()
	store := newReceiptV2SecurityStore(t)
	claimed := receiptV2SecurityFixture(t, store)
	replayed, err := store.ApplyFederatedReceiptEvent(ctx, &claimed)
	require.NoError(t, err)
	require.False(t, replayed)

	// A read event for the same message must carry the exact immutable send
	// binding. It cannot create a second event row while leaving the sender's
	// projection on the old binding.
	read := claimed
	read.EventID = "receipt-read-equivocation"
	read.Kind = "read"
	read.ContactRevision = strings.Repeat("5", 64)
	readHash := sha256.Sum256([]byte("read-proof-equivocation"))
	read.ProofHash = readHash[:]
	_, err = store.ApplyFederatedReceiptEvent(ctx, &read)
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)

	var count int
	require.NoError(t, store.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_receipt_v2_events WHERE message_id=?`, claimed.MessageID).Scan(&count))
	require.Equal(t, 1, count, "a rejected receipt must not leave an orphan evidence row")
}

func TestReceiptV2RejectsUnboundedOrNonCanonicalIdentityFields(t *testing.T) {
	store := newReceiptV2SecurityStore(t)
	base := receiptV2SecurityFixture(t, store)
	tests := []struct {
		name   string
		mutate func(*FederatedReceiptEvent)
	}{
		{"non-hex sender", func(event *FederatedReceiptEvent) { event.SenderAgentID = strings.Repeat("z", 64) }},
		{"short recipient", func(event *FederatedReceiptEvent) { event.RecipientAgentID = "recipient" }},
		{"oversize local pipe id", func(event *FederatedReceiptEvent) { event.LocalPipeID = strings.Repeat("p", 4097) }},
		{"oversize sender chain", func(event *FederatedReceiptEvent) { event.SenderChainID = strings.Repeat("s", 4097) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			event := base
			test.mutate(&event)
			_, err := store.ApplyFederatedReceiptEvent(context.Background(), &event)
			require.ErrorIs(t, err, ErrFederatedReceiptInvalid)
		})
	}
}

func TestReceiptV2EvidenceAndIdempotencySurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart-receipts.db")
	store, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	event := receiptV2SecurityFixture(t, store)
	replayed, err := store.ApplyFederatedReceiptEvent(ctx, &event)
	require.NoError(t, err)
	require.False(t, replayed)
	require.NoError(t, store.Close())

	reopened, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	replayed, err = reopened.ApplyFederatedReceiptEvent(ctx, &event)
	require.NoError(t, err)
	require.True(t, replayed, "an exact retry after restart must not create a second transition")
	projection, err := reopened.GetFederatedReceiptForSender(ctx, event.SenderAgentID, event.LocalPipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.ClaimedAt)
	require.Nil(t, projection.ReadAt)
}

func TestReceiptV2DeliveryCanArriveAfterClaimAndIsWriteOnce(t *testing.T) {
	ctx := context.Background()
	store := newReceiptV2SecurityStore(t)
	event := receiptV2SecurityFixture(t, store)
	_, err := store.ApplyFederatedReceiptEvent(ctx, &event)
	require.NoError(t, err)

	firstDelivery := event.EventAt.Add(time.Second)
	replayed, err := store.RecordFederatedReceiptDelivery(ctx, event.FederatedReceiptBinding, firstDelivery)
	require.NoError(t, err)
	require.False(t, replayed)

	// A transport retry may observe admission later, but it cannot rewrite the
	// sender's first durable delivery time.
	replayed, err = store.RecordFederatedReceiptDelivery(ctx, event.FederatedReceiptBinding, firstDelivery.Add(time.Minute))
	require.NoError(t, err)
	require.True(t, replayed)
	projection, err := store.GetFederatedReceiptForSender(ctx, event.SenderAgentID, event.LocalPipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.DeliveredAt)
	require.True(t, projection.DeliveredAt.Equal(firstDelivery))
	require.NotNil(t, projection.ClaimedAt, "delivery must not erase independent claim evidence")
}

func TestReceiptV2MigrationRejectsLaxPartialSchema(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "partial-receipts.db")
	raw, err := sql.Open("sqlite", path)
	require.NoError(t, err)
	_, err = raw.ExecContext(ctx, `CREATE TABLE pipeline_receipt_v2_events (
		event_id TEXT, message_id TEXT, local_pipe_id TEXT, sender_chain_id TEXT,
		recipient_chain_id TEXT, sender_agent_id TEXT, recipient_agent_id TEXT,
		content_digest TEXT, policy_epoch TEXT, agreement_id TEXT, contact_id TEXT,
		contact_revision TEXT, authorization_mode TEXT, relation_digest TEXT,
		event_kind TEXT, event_at TEXT, proof_hash BLOB, accepted_at TEXT
	)`)
	require.NoError(t, err)
	require.NoError(t, raw.Close())

	opened, err := NewSQLiteStore(ctx, path)
	require.Error(t, err)
	require.Nil(t, opened)
	require.Contains(t, err.Error(), "required constraint")
}

func TestReceiptV2ConcurrentExactEventsRemainMonotonic(t *testing.T) {
	ctx := context.Background()
	store := newReceiptV2SecurityStore(t)
	claimed := receiptV2SecurityFixture(t, store)
	read := claimed
	read.Kind = "read"
	read.EventID = "receipt-read-concurrent"
	read.EventAt = claimed.EventAt.Add(time.Second)
	readHash := sha256.Sum256([]byte("read-proof-concurrent"))
	read.ProofHash = readHash[:]

	const workers = 24
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(index int) {
			defer wg.Done()
			event := &claimed
			if index%2 == 1 {
				event = &read
			}
			_, err := store.ApplyFederatedReceiptEvent(ctx, event)
			errs <- err
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	projection, err := store.GetFederatedReceiptForSender(ctx, claimed.SenderAgentID, claimed.LocalPipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.ClaimedAt)
	require.NotNil(t, projection.ReadAt)
	var eventCount int
	require.NoError(t, store.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_receipt_v2_events WHERE message_id=?`, claimed.MessageID).Scan(&eventCount))
	require.Equal(t, 2, eventCount)
}

func TestReceiptV2RetentionUsesTerminalAgeAndCascadesEvidence(t *testing.T) {
	ctx := context.Background()
	store := newReceiptV2SecurityStore(t)
	event := receiptV2SecurityFixture(t, store)
	_, err := store.ApplyFederatedReceiptEvent(ctx, &event)
	require.NoError(t, err)
	terminalAt := event.EventAt.Add(10 * time.Minute)
	_, err = store.writeExecContext(ctx, `UPDATE pipeline_messages
		SET status='completed',completed_at=?,terminal_at=? WHERE pipe_id=?`,
		formatTime(terminalAt), formatTime(terminalAt), event.LocalPipeID)
	require.NoError(t, err)

	purged, err := store.PurgePipelines(ctx, terminalAt.Add(-time.Second))
	require.NoError(t, err)
	require.Zero(t, purged)
	_, err = store.GetFederatedReceiptForSender(ctx, event.SenderAgentID, event.LocalPipeID)
	require.NoError(t, err)

	purged, err = store.PurgePipelines(ctx, terminalAt.Add(time.Second))
	require.NoError(t, err)
	require.Equal(t, 1, purged)
	_, err = store.GetFederatedReceiptForSender(ctx, event.SenderAgentID, event.LocalPipeID)
	require.ErrorIs(t, err, ErrFederatedReceiptNotFound)
	var evidence int
	require.NoError(t, store.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_receipt_v2_events WHERE message_id=?`, event.MessageID).Scan(&evidence))
	require.Zero(t, evidence, "purging content must cascade the bounded receipt proof metadata")
}

func TestGetFederatedReceiptForSenderDoesNotHideDatabaseFailureAsUnconfirmed(t *testing.T) {
	ctx := context.Background()
	store := newReceiptV2SecurityStore(t)
	require.NoError(t, store.Close())
	_, err := store.GetFederatedReceiptForSender(ctx, strings.Repeat("a", 64), "pipe-closed-db")
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrFederatedReceiptNotFound,
		"only a genuine missing row may become sender-visible unconfirmed status")
}

func TestReceiptV2TerminalCASNeverErasesParticipantEvidence(t *testing.T) {
	ctx := context.Background()
	store := newReceiptV2SecurityStore(t)
	event := receiptV2SecurityFixture(t, store)
	event.Kind = "read"
	event.EventID = "receipt-read-before-terminal"
	readHash := sha256.Sum256([]byte("read-before-terminal"))
	event.ProofHash = readHash[:]
	_, err := store.ApplyFederatedReceiptEvent(ctx, &event)
	require.NoError(t, err)
	terminalAt := event.EventAt.Add(time.Minute)
	replayed, err := store.RecordFederatedReceiptTerminal(ctx, event.FederatedReceiptBinding, "failed", terminalAt)
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = store.RecordFederatedReceiptTerminal(ctx, event.FederatedReceiptBinding, "failed", terminalAt)
	require.NoError(t, err)
	require.True(t, replayed)

	_, err = store.RecordFederatedReceiptTerminal(ctx, event.FederatedReceiptBinding, "failed", terminalAt.Add(time.Second))
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)
	_, err = store.RecordFederatedReceiptTerminal(ctx, event.FederatedReceiptBinding, "revoked", terminalAt)
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)
	projection, err := store.GetFederatedReceiptForSender(ctx, event.SenderAgentID, event.LocalPipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.ReadAt)
	require.Equal(t, "failed", projection.TerminalKind)
	require.NotNil(t, projection.TerminalAt)
	require.True(t, projection.TerminalAt.Equal(terminalAt))
}

func TestReceiptV2ImportedClaimAndOutboxAreAtomicIdempotentAndRestartSafe(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "recipient-outbox.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	msg, event := importedReceiptOutboxFixture(t, s)

	replayed, err := s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, event)
	require.NoError(t, err)
	require.False(t, replayed)
	claimed, err := s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "claimed", claimed.Status)
	require.Equal(t, msg.ToAgent, claimed.ClaimedBy)
	due, err := s.ListPendingFederatedReceiptOutbox(ctx, event.CreatedAt, 10)
	require.NoError(t, err)
	require.Len(t, due, 1)
	require.Equal(t, event.EventID, due[0].EventID)

	replayed, err = s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, event)
	require.NoError(t, err)
	require.True(t, replayed)
	require.NoError(t, s.Close())

	reopened, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	replayed, err = reopened.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, event)
	require.NoError(t, err)
	require.True(t, replayed)
	var count int
	require.NoError(t, reopened.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_receipt_v2_outbox WHERE message_id=?`, event.MessageID).Scan(&count))
	require.Equal(t, 1, count, "retries and restarts must never duplicate signed evidence")
}

func TestReceiptV2ImportedClaimRollsBackWhenOutboxConflicts(t *testing.T) {
	ctx := context.Background()
	s := newReceiptV2SecurityStore(t)
	msg, prior := importedReceiptOutboxFixture(t, s)
	// Seed an existing fact with a different immutable binding. A fresh proof
	// must only be idempotent for the exact same fact; it must never turn a
	// binding conflict into a successful recipient claim.
	prior.ContentDigest = strings.Repeat("f", 64)
	_, err := s.insertFederatedReceiptOutbox(ctx, prior)
	require.NoError(t, err)

	binding, err := s.GetFederatedReceiptInbound(ctx, msg.PipeID)
	require.NoError(t, err)
	conflict := *prior
	conflict.FederatedReceiptBinding = *binding
	conflict.EventID = "different-signed-claim"
	conflict.Proof = prior.Proof
	conflict.Proof.Nonce = []byte("nonce-v3")
	_, err = s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, &conflict)
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)
	unchanged, err := s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "pending", unchanged.Status,
		"a receipt enqueue conflict must roll back the recipient claim")
	require.Empty(t, unchanged.ClaimedBy)
}

func TestReceiptV2ImportedFreshProofIsSemanticReplay(t *testing.T) {
	ctx := context.Background()
	s := newReceiptV2SecurityStore(t)
	msg, first := importedReceiptOutboxFixture(t, s)

	replayed, err := s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, first)
	require.NoError(t, err)
	require.False(t, replayed)

	retry := *first
	retry.EventID = "fresh-proof-same-fact"
	retry.EventAt = first.EventAt.Add(time.Second)
	retry.Proof = first.Proof
	retry.Proof.Nonce = []byte("fresh-nonce")
	retry.Proof.Timestamp++
	replayed, err = s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, &retry)
	require.NoError(t, err)
	require.True(t, replayed)

	var eventID, eventAt string
	var count int
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT COUNT(*),event_id,event_at
		FROM pipeline_receipt_v2_outbox WHERE message_id=? AND event_kind='claimed'`, first.MessageID).
		Scan(&count, &eventID, &eventAt))
	require.Equal(t, 1, count)
	require.Equal(t, first.EventID, eventID, "semantic retry must preserve first admitted evidence")
	require.Equal(t, formatTime(first.EventAt), eventAt)
}

func TestReceiptV2ImportedReadRequiresExactPriorRecipientClaim(t *testing.T) {
	ctx := context.Background()
	s := newReceiptV2SecurityStore(t)
	msg, event := importedReceiptOutboxFixture(t, s)
	event.Kind = "read"
	event.EventID = "receipt-outbox-read"
	event.Proof.CanonicalRequest = []byte("PUT /v1/pipe/" + msg.PipeID + "/receipt/read\n{}")

	_, err := s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, event)
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)
	var count int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_receipt_v2_outbox WHERE message_id=?`, event.MessageID).Scan(&count))
	require.Zero(t, count)

	claim := *event
	claim.Kind = "claimed"
	claim.EventID = "receipt-outbox-claim-first"
	claim.Proof.CanonicalRequest = []byte("PUT /v1/pipe/" + msg.PipeID + "/receipt/claimed\n{}")
	_, err = s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, &claim)
	require.NoError(t, err)
	_, err = s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, event)
	require.NoError(t, err)
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_receipt_v2_outbox WHERE message_id=?`, event.MessageID).Scan(&count))
	require.Equal(t, 2, count)
}

func TestReceiptV2PurgeCannotCascadeLivePendingRecipientEvidence(t *testing.T) {
	ctx := context.Background()
	s := newReceiptV2SecurityStore(t)
	msg, event := importedReceiptOutboxFixture(t, s)
	event.ExpiresAt = time.Now().UTC().Add(time.Hour)
	_, err := s.RecordImportedFederatedReceipt(ctx, msg.PipeID, msg.ToAgent, event)
	require.NoError(t, err)

	terminalAt := time.Now().UTC().Add(-time.Hour)
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages
		SET status='failed',terminal_at=? WHERE pipe_id=?`, formatTime(terminalAt), msg.PipeID)
	require.NoError(t, err)

	purged, err := s.PurgePipelines(ctx, terminalAt.Add(time.Minute))
	require.NoError(t, err)
	require.Zero(t, purged, "a live pending receipt must retain its parent message until delivery or expiry")
	_, err = s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	var pending int
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM pipeline_receipt_v2_outbox
		WHERE message_id=? AND state='pending'`, event.MessageID).Scan(&pending))
	require.Equal(t, 1, pending)
}
