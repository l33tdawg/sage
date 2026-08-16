package store

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

func newMessageTestStore(t *testing.T) *SQLiteStore {
	t.Helper()
	s, err := NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "messages.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	return s
}

func testLocalMessage(id, from, to, payload string) *PipelineMessage {
	now := time.Now().UTC().Truncate(time.Millisecond)
	return &PipelineMessage{
		PipeID: id, FromAgent: from, ToAgent: to, Intent: "request", Payload: payload,
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
}

func TestMessageSendIdempotencyIsCallerBoundAndExact(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	first, replayed, err := s.SendLocalMessage(ctx, "stable-key", testLocalMessage("msg-1", "alice", "bob", "one"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, "msg-1", first.PipeID)

	replay, replayed, err := s.SendLocalMessage(ctx, "stable-key", testLocalMessage("discarded-id", "alice", "bob", "one"))
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, "msg-1", replay.PipeID)

	_, _, err = s.SendLocalMessage(ctx, "stable-key", testLocalMessage("msg-2", "alice", "bob", "different"))
	require.ErrorIs(t, err, ErrMessageIdempotencyConflict)

	other, replayed, err := s.SendLocalMessage(ctx, "stable-key", testLocalMessage("msg-3", "charlie", "bob", "different"))
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, "msg-3", other.PipeID)
}

func TestMessageSendReplayIgnoresMutableProviderMetadata(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	first := testLocalMessage("msg-provider", "alice", "bob", "one")
	first.FromProvider = "old-display"
	_, replayed, err := s.SendLocalMessage(ctx, "stable-provider", first)
	require.NoError(t, err)
	require.False(t, replayed)
	retry := testLocalMessage("discarded", "alice", "bob", "one")
	retry.FromProvider = "new-display"
	got, replayed, err := s.SendLocalMessage(ctx, "stable-provider", retry)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, "msg-provider", got.PipeID)
	require.Equal(t, "old-display", got.FromProvider, "replay returns the immutable original row")
}

func TestFederatedMessageSendIdempotencyCommitsTransportOnce(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	pub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sender := fmt.Sprintf("%x", pub)
	makePair := func(messageID, payload, chain string) (*PipelineMessage, *PipelineTransportOutbox) {
		now := time.Now().UTC().Truncate(time.Millisecond)
		msg := testLocalMessage(messageID, sender, strings.Repeat("b", 64), payload)
		msg.CreatedAt, msg.ExpiresAt, msg.DestinationChainID = now, now.Add(time.Hour), chain
		msg.FederationPolicyEpoch = "epoch-1"
		msg.FederationAgreementID = strings.Repeat("c", 64)
		msg.FederationContactID = strings.Repeat("d", 64)
		msg.FederationContactRevision = strings.Repeat("e", 64)
		event := &PipelineTransportOutbox{
			EventID: "event-" + messageID, PipeID: messageID, RemoteChainID: chain,
			EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision, SourceAgentID: sender,
			TargetAgentID: msg.ToAgent, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
			Proof: PipelineAgentProof{AgentID: sender, Signature: make([]byte, ed25519.SignatureSize),
				Timestamp: now.Unix(), Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}")},
		}
		return msg, event
	}
	firstMsg, firstEvent := makePair("fed-1", "one", "chain-b")
	first, replayed, err := s.SendFederatedMessage(ctx, "fed-key", firstMsg, firstEvent)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, "fed-1", first.PipeID)

	retryMsg, retryEvent := makePair("discarded", "one", "chain-b")
	replay, replayed, err := s.SendFederatedMessage(ctx, "fed-key", retryMsg, retryEvent)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, "fed-1", replay.PipeID)
	_, err = s.GetPipelineTransport(ctx, retryEvent.EventID)
	require.Error(t, err, "an idempotent replay must not create a second transport event")

	conflictMsg, conflictEvent := makePair("fed-2", "one", "chain-c")
	_, _, err = s.SendFederatedMessage(ctx, "fed-key", conflictMsg, conflictEvent)
	require.ErrorIs(t, err, ErrMessageIdempotencyConflict)
}

func TestFederatedMessageStatusMergesWorkflowTransportAndExactReadForSender(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := strings.Repeat("a", 64)
	recipient := strings.Repeat("b", 64)
	msg := testLocalMessage("fed-status", sender, recipient, "work")
	msg.CreatedAt, msg.ExpiresAt, msg.DestinationChainID = now, now.Add(time.Hour), "chain-b"
	msg.FederationPolicyEpoch = strings.Repeat("c", 64)
	msg.FederationAgreementID = strings.Repeat("d", 64)
	msg.FederationContactID = strings.Repeat("e", 64)
	msg.FederationContactRevision = strings.Repeat("f", 64)
	event := &PipelineTransportOutbox{
		EventID: "fed-status-event", PipeID: msg.PipeID, RemoteChainID: "chain-b",
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sender,
		TargetAgentID: recipient, ReceiptProtocolVersion: FederatedPipelineReceiptVersion,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		Proof: PipelineAgentProof{AgentID: sender, Signature: make([]byte, ed25519.SignatureSize),
			Timestamp: now.Unix(), Nonce: []byte("12345678"), CanonicalRequest: []byte("POST /v1/pipe/send\n{}")},
	}
	_, replayed, err := s.SendFederatedMessage(ctx, "fed-status-key", msg, event)
	require.NoError(t, err)
	require.False(t, replayed)
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET status='completed',completed_at=?,terminal_at=? WHERE pipe_id=?`,
		formatTime(now.Add(3*time.Second)), formatTime(now.Add(3*time.Second)), msg.PipeID)
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_transport_outbox SET state='delivered',delivered_at=? WHERE pipe_id=?`,
		formatTime(now.Add(time.Second)), msg.PipeID)
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `INSERT INTO pipeline_receipt_v2_projection
		(message_id,local_pipe_id,sender_chain_id,recipient_chain_id,sender_agent_id,recipient_agent_id,
		content_digest,policy_epoch,agreement_id,contact_id,contact_revision,authorization_mode,
		relation_digest,delivered_at,delivery_evidence,claimed_at,read_at,updated_at)
		VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`,
		"remote-event", msg.PipeID, "chain-a", "chain-b", sender, recipient,
		strings.Repeat("1", 64), msg.FederationPolicyEpoch, msg.FederationAgreementID,
		msg.FederationContactID, msg.FederationContactRevision, "", "",
		formatTime(now.Add(time.Second)), "signed_peer_receipt", formatTime(now.Add(2*time.Second)),
		formatTime(now.Add(2*time.Second)), formatTime(now.Add(3*time.Second)))
	require.NoError(t, err)

	status, err := s.GetMessageStatusForSender(ctx, sender, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "federated", status.Scope)
	require.Equal(t, "delivered", status.TransportStatus)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "federated_receipt_v2", status.ReadEvidence)
	require.Equal(t, "completed", status.WorkflowStatus)
	require.Equal(t, "completed", status.TerminalReason)
	require.NotNil(t, status.DeliveredAt)
	require.NotNil(t, status.ReadAt)

	for _, caller := range []string{recipient, strings.Repeat("9", 64), "root"} {
		_, err = s.GetMessageStatusForSender(ctx, caller, msg.PipeID)
		require.ErrorIs(t, err, ErrMessageNotFound)
	}
}

func TestMessageReceiveReplaysExactClaimedBatchAfterLostResponse(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	base := time.Now().UTC().Add(-time.Minute).Truncate(time.Millisecond)
	for i := 1; i <= 3; i++ {
		msg := testLocalMessage(fmt.Sprintf("msg-%d", i), "alice", "bob", fmt.Sprintf("payload-%d", i))
		msg.CreatedAt = base.Add(time.Duration(i) * time.Second)
		msg.ExpiresAt = msg.CreatedAt.Add(time.Hour)
		_, _, err := s.SendLocalMessage(ctx, fmt.Sprintf("send-%d", i), msg)
		require.NoError(t, err)
	}
	first, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive-a", 2)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, first, 2)
	require.Equal(t, []string{"msg-1", "msg-2"}, []string{first[0].PipeID, first[1].PipeID})

	// A later arrival must not leak into a retry of the lost first response.
	msg4 := testLocalMessage("msg-4", "alice", "bob", "payload-4")
	msg4.CreatedAt = base.Add(4 * time.Second)
	msg4.ExpiresAt = msg4.CreatedAt.Add(time.Hour)
	_, _, err = s.SendLocalMessage(ctx, "send-4", msg4)
	require.NoError(t, err)
	retry, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive-a", 2)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Equal(t, []string{"msg-1", "msg-2"}, []string{retry[0].PipeID, retry[1].PipeID})

	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive-a", 3)
	require.ErrorIs(t, err, ErrMessageReceiveConflict)
	next, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive-b", 2)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Equal(t, []string{"msg-3", "msg-4"}, []string{next[0].PipeID, next[1].PipeID})
}

func TestMessageReceiveReplaySurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "restart.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "send", testLocalMessage("msg-restart", "alice", "bob", "payload"))
	require.NoError(t, err)
	first, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "restart-token", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, first, 1)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	retry, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "restart-token", 1)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Len(t, retry, 1)
	require.Equal(t, "msg-restart", retry[0].PipeID)
}

func TestClaimedMessageHistoryAndReceiptSurviveRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "history-restart.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "send-history", testLocalMessage(
		"msg-history-restart", "alice", "bob", "durable work",
	))
	require.NoError(t, err)
	items, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "claim-history", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.NoError(t, s.Close())

	s, err = NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, s.Close()) })
	history, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, "msg-history-restart", history[0].PipeID)
	require.Equal(t, "claimed", history[0].Status)
	require.Equal(t, "durable work", history[0].Payload)

	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-history-restart")
	require.NoError(t, err)
	require.Equal(t, "not_confirmed", status.ReadStatus)
	_, err = s.AcknowledgeLocalMessageRead(ctx, "bob", "msg-history-restart")
	require.NoError(t, err)
	status, err = s.GetMessageStatusForSender(ctx, "alice", "msg-history-restart")
	require.NoError(t, err)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "local_exact_ack", status.ReadEvidence)
}

func TestMessageReceiveConcurrentTokensNeverReturnSameMessage(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg-race", "alice", "bob", "payload"))
	require.NoError(t, err)
	type result struct {
		items []*PipelineMessage
		err   error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, token := range []string{"race-a", "race-b"} {
		wg.Add(1)
		go func(token string) {
			defer wg.Done()
			items, _, callErr := s.ReceiveLocalMessages(ctx, "bob", "", token, 1)
			results <- result{items: items, err: callErr}
		}(token)
	}
	wg.Wait()
	close(results)
	total := 0
	for got := range results {
		require.NoError(t, got.err)
		total += len(got.items)
	}
	require.Equal(t, 1, total)
}

func TestMessageReceiveAttributesWinningSessionAndDeterministicHandoff(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send-session", testLocalMessage("msg-session", "alice", "bob", "work"))
	require.NoError(t, err)

	type result struct {
		items   []*PipelineMessage
		session string
		err     error
	}
	results := make(chan result, 2)
	var wg sync.WaitGroup
	for _, session := range []string{"mcp-supervisor", "mcp-helper"} {
		wg.Add(1)
		go func(session string) {
			defer wg.Done()
			items, _, callErr := s.ReceiveLocalMessages(ctx, "bob", "", "receive-"+session, 1, session)
			results <- result{items: items, session: session, err: callErr}
		}(session)
	}
	wg.Wait()
	close(results)

	winner := ""
	for got := range results {
		require.NoError(t, got.err)
		if len(got.items) == 1 {
			winner = got.session
			require.Equal(t, winner, got.items[0].ClaimedSessionID)
		} else {
			require.Empty(t, got.items)
		}
	}
	require.NotEmpty(t, winner)

	history, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Len(t, history, 1)
	require.Equal(t, winner, history[0].ClaimedSessionID)

	target := "mcp-supervisor"
	if winner == target {
		target = "mcp-helper"
	}
	replayed, err := s.HandoffLocalMessageClaim(ctx, "bob", "msg-session", winner, target)
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.HandoffLocalMessageClaim(ctx, "bob", "msg-session", winner, target)
	require.NoError(t, err)
	require.True(t, replayed)
	_, err = s.HandoffLocalMessageClaim(ctx, "bob", "msg-session", winner, "mcp-third")
	require.ErrorIs(t, err, ErrMessageReceiveConflict)

	history, err = s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Equal(t, target, history[0].ClaimedSessionID)
	// Was ErrMessageNotFound. That expectation encoded the collapsing this
	// package now avoids: a stale session and an absent message reported the
	// same error, the REST layer turned both into 404, and the MCP client's
	// older-node fallback retried the call without a session id — bypassing
	// this fence. The refusal itself is unchanged; only its precision is.
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-session", "stale", winner)
	require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-session", "done", target)
	require.NoError(t, err)
}

func TestMessageReceiveStorageFailureRollsBackRetryToken(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg-trigger", "alice", "bob", "payload"))
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `CREATE TRIGGER fail_message_claim BEFORE UPDATE OF status ON pipeline_messages
		WHEN OLD.pipe_id='msg-trigger' AND NEW.status='claimed'
		BEGIN SELECT RAISE(ABORT, 'injected claim failure'); END`)
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "retry-after-failure", 1)
	require.ErrorContains(t, err, "injected claim failure")
	_, err = s.writeExecContext(ctx, `DROP TRIGGER fail_message_claim`)
	require.NoError(t, err)
	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "retry-after-failure", 1)
	require.NoError(t, err)
	require.False(t, replayed, "failed transaction must not burn the retry token")
	require.Len(t, items, 1)
}

func TestMessageReceiveDetectsPurgedReplayInsteadOfClaimingLaterWork(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg-purged", "alice", "bob", "first"))
	require.NoError(t, err)
	items, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "purged-token", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = s.writeExecContext(ctx, `DELETE FROM pipeline_messages WHERE pipe_id='msg-purged'`)
	require.NoError(t, err)
	_, _, err = s.SendLocalMessage(ctx, "send-later", testLocalMessage("msg-later", "alice", "bob", "later"))
	require.NoError(t, err)
	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "purged-token", 1)
	require.ErrorIs(t, err, ErrMessageReceiveExpired)
	require.True(t, replayed)
	require.Empty(t, items)
	newItems, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "new-token", 1)
	require.NoError(t, err)
	require.Len(t, newItems, 1)
	require.Equal(t, "msg-later", newItems[0].PipeID)
}

func TestMessageReceiveTokenQuotaAndAgedMetadataCleanup(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	require.NoError(t, s.RunInTx(ctx, func(txStore OffchainStore) error {
		tx := txStore.(*SQLiteStore)
		for i := 0; i < maxMessageReceiveBatchesPerAgent; i++ {
			if _, err := tx.writeExecContext(ctx, `INSERT INTO message_receive_batches
				(receiver_agent_id,token_hash,requested_limit,claimed_count,created_at) VALUES(?,?,?,?,?)`,
				"bob", fmt.Sprintf("token-%d", i), 1, 0, formatTime(time.Now().UTC())); err != nil {
				return err
			}
		}
		return nil
	}))
	_, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "over-quota", 1)
	require.ErrorIs(t, err, ErrMessageReceiveQuota)
	_, err = s.writeExecContext(ctx, `UPDATE message_receive_batches SET created_at=? WHERE receiver_agent_id='bob'`,
		formatTime(time.Now().UTC().Add(-messageReceiveBatchRetention-time.Minute)))
	require.NoError(t, err)
	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "after-cleanup", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Empty(t, items)
}

func TestMessageAgedReplayMetadataDoesNotRevokeRetainedInboxAuthority(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send-retained", testLocalMessage("msg-retained", "alice", "bob", "work"))
	require.NoError(t, err)
	items, _, err := s.ReceiveLocalMessages(ctx, "bob", "", "old-receive", 1)
	require.NoError(t, err)
	require.Len(t, items, 1)
	_, err = s.writeExecContext(ctx, `UPDATE message_receive_batches SET created_at=?
		WHERE receiver_agent_id='bob' AND token_hash=?`,
		formatTime(time.Now().UTC().Add(-messageReceiveBatchRetention-time.Minute)), messageKeyHash("old-receive"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "cleanup-trigger", 1)
	require.NoError(t, err)

	var oldBatchItems int
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT COUNT(*) FROM message_receive_batch_items
		WHERE receiver_agent_id='bob' AND token_hash=?`, messageKeyHash("old-receive")).Scan(&oldBatchItems))
	require.Zero(t, oldBatchItems, "aged replay metadata should be pruned")
	replayed, err := s.AcknowledgeLocalMessageRead(ctx, "bob", "msg-retained")
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.ReplyLocalMessage(ctx, "bob", "msg-retained", "done")
	require.NoError(t, err)
	require.False(t, replayed)
}

func TestMessageReceiveIsNotStarvedByFederatedOrProviderQueueRows(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	foreign := testLocalMessage("foreign", "remote", "bob", "remote payload")
	foreign.SourceChainID = "other-chain"
	foreign.SourcePipeID = "remote-1"
	foreign.CreatedAt = foreign.CreatedAt.Add(-time.Minute)
	require.NoError(t, s.InsertPipeline(ctx, foreign))
	providerOnly := testLocalMessage("provider", "alice", "", "old provider work")
	providerOnly.ToProvider = "claude"
	providerOnly.CreatedAt = providerOnly.CreatedAt.Add(-30 * time.Second)
	require.NoError(t, s.InsertPipeline(ctx, providerOnly))
	ambiguousProvider := testLocalMessage("ambiguous-provider", "alice", "bob", "must stay legacy")
	ambiguousProvider.ToProvider = "claude"
	ambiguousProvider.CreatedAt = providerOnly.CreatedAt.Add(time.Second)
	require.NoError(t, s.InsertPipeline(ctx, ambiguousProvider))
	_, _, err := s.SendLocalMessage(ctx, "local-send", testLocalMessage("local", "alice", "bob", "local payload"))
	require.NoError(t, err)

	items, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "claude", "local-receive", 1)
	require.NoError(t, err)
	require.False(t, replayed)
	require.Len(t, items, 1)
	require.Equal(t, "local", items[0].PipeID)
	_, err = s.GetMessageStatusForSender(ctx, "alice", "ambiguous-provider")
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestMessageReadRequiresExactRecipientFetchAndStatusIsSenderOnlyMetadata(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg", "alice", "bob", "secret"))
	require.NoError(t, err)

	_, err = s.AcknowledgeLocalMessageRead(ctx, "bob", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound, "an inbox request proof without exact returned ID is insufficient")
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)
	_, err = s.AcknowledgeLocalMessageRead(ctx, "mallory", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound)
	replayed, err := s.AcknowledgeLocalMessageRead(ctx, "bob", "msg")
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.AcknowledgeLocalMessageRead(ctx, "bob", "msg")
	require.NoError(t, err)
	require.True(t, replayed)

	// Corrupt encrypted content on purpose: status must not touch/decrypt it.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET payload='enc::not-valid-ciphertext' WHERE pipe_id='msg'`)
	require.NoError(t, err)
	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg")
	require.NoError(t, err)
	require.Equal(t, "delivered", status.TransportStatus)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "local_exact_ack", status.ReadEvidence)
	require.NotNil(t, status.ReadAt)
	_, err = s.GetMessageStatusForSender(ctx, "bob", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound)
	_, err = s.GetMessageStatusForSender(ctx, "root", "msg")
	require.ErrorIs(t, err, ErrMessageNotFound)
}

func TestMessageReplyIsExactRecipientLocalAndIdempotent(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg", "alice", "bob", "work"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)

	_, err = s.ReplyLocalMessage(ctx, "mallory", "msg", "done")
	require.ErrorIs(t, err, ErrMessageNotFound)
	replayed, err := s.ReplyLocalMessage(ctx, "bob", "msg", "done")
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.ReplyLocalMessage(ctx, "bob", "msg", "done")
	require.NoError(t, err)
	require.True(t, replayed)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg", "changed")
	require.True(t, errors.Is(err, ErrMessageReplyConflict))
	receivedAgain, replayed, err := s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)
	require.True(t, replayed)
	require.Len(t, receivedAgain, 1)
	require.Equal(t, "claimed", receivedAgain[0].Status)
	require.Empty(t, receivedAgain[0].Result, "lost receive response must replay its original claimed snapshot")
	require.Nil(t, receivedAgain[0].CompletedAt)

	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg")
	require.NoError(t, err)
	require.Equal(t, "completed", status.WorkflowStatus)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "completed", status.TerminalReason)
	inboxHistory, err := s.GetInboxHistory(ctx, "bob", "", 10)
	require.NoError(t, err)
	require.Len(t, inboxHistory, 1)
	require.Equal(t, "work", inboxHistory[0].Payload)
	require.Equal(t, "done", inboxHistory[0].Result)
	outboxHistory, err := s.GetOutbox(ctx, "alice", 10)
	require.NoError(t, err)
	require.Len(t, outboxHistory, 1)
	require.Equal(t, "msg", outboxHistory[0].PipeID)
}

func TestMessageTerminalAndReadTransitionsOwnTheirRetentionWindows(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	old := now.Add(-72 * time.Hour)
	msg := testLocalMessage("msg-retention", "alice", "bob", "old work")
	msg.CreatedAt = old
	msg.ExpiresAt = now.Add(time.Hour)
	_, _, err := s.SendLocalMessage(ctx, "send-retention", msg)
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive-retention", 1)
	require.NoError(t, err)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-retention", "done")
	require.NoError(t, err)

	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-retention")
	require.NoError(t, err)
	require.Equal(t, "completed", status.WorkflowStatus)
	require.NotNil(t, status.TerminalAt)
	require.WithinDuration(t, now, *status.TerminalAt, time.Minute)

	cutoff := now.Add(-24 * time.Hour)
	purged, err := s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "an old send completed now gets a fresh terminal retention window")

	// A later exact read fact independently keeps sender status queryable even
	// after the terminal transition itself has aged out.
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET terminal_at=? WHERE pipe_id=?`,
		formatTime(old), "msg-retention")
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "a fresh read receipt extends terminal metadata retention")

	_, err = s.writeExecContext(ctx, `UPDATE message_read_receipts SET read_at=? WHERE message_id=?`,
		formatTime(old), "msg-retention")
	require.NoError(t, err)
	purged, err = s.PurgePipelines(ctx, cutoff)
	require.NoError(t, err)
	require.Zero(t, purged, "canonical message history is durable")
	_, err = s.GetMessageStatusForSender(ctx, "alice", "msg-retention")
	require.NoError(t, err)
}

func TestCanonicalMessageUpgradeExtendsUnreadAndSkipsLegacyExpirySweep(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "messages-upgrade.db")
	now := time.Now().UTC().Truncate(time.Millisecond)

	before, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	// A genuine v11.17.8 row: the old default stamped expires_at at exactly
	// created_at + 24h. The fixture previously used created_at-24h with a +1h
	// expiry, which is created_at + 25h — not the old default at all, but a
	// bounded expiry of the shape a caller gets from ttl_minutes. The rescue
	// now targets the old default precisely so a caller's bounded TTL is not
	// re-stamped on every store open, so the fixture must model what it claims.
	msg := testLocalMessage("msg-v11178-unread", "alice", "bob", "survive the upgrade")
	msg.CreatedAt = now.Add(-23 * time.Hour)
	msg.ExpiresAt = msg.CreatedAt.Add(24 * time.Hour)
	_, _, err = before.SendLocalMessage(ctx, "upgrade-send", msg)
	require.NoError(t, err)
	require.NoError(t, before.Close())

	after, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, after.Close()) })
	upgraded, err := after.GetPipeline(ctx, "msg-v11178-unread")
	require.NoError(t, err)
	require.True(t, upgraded.ExpiresAt.After(now.Add(99*365*24*time.Hour)),
		"an unread v11.17.8 canonical message must become durable on upgrade")

	expired, err := after.ExpireStalePipelines(ctx, now.Add(48*time.Hour))
	require.NoError(t, err)
	require.Zero(t, expired, "legacy expiry sweeps must not expire canonical Messages")
	stillPending, err := after.GetPipeline(ctx, "msg-v11178-unread")
	require.NoError(t, err)
	require.Equal(t, "pending", stillPending.Status)
}

func TestMessageReplyAndReadRaceConvergesOnOneReceipt(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send-race", testLocalMessage("msg-read-reply", "alice", "bob", "work"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive-race", 1)
	require.NoError(t, err)
	errs := make(chan error, 2)
	go func() {
		_, callErr := s.AcknowledgeLocalMessageRead(ctx, "bob", "msg-read-reply")
		errs <- callErr
	}()
	go func() {
		_, callErr := s.ReplyLocalMessage(ctx, "bob", "msg-read-reply", "done")
		errs <- callErr
	}()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
	status, err := s.GetMessageStatusForSender(ctx, "alice", "msg-read-reply")
	require.NoError(t, err)
	require.Equal(t, "confirmed", status.ReadStatus)
	require.Equal(t, "completed", status.WorkflowStatus)
	var receiptCount int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM message_read_receipts WHERE message_id='msg-read-reply'`).Scan(&receiptCount))
	require.Equal(t, 1, receiptCount)
}

func TestMessageFingerprintsAreVaultSealedAndStillReplay(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	keyPath := filepath.Join(t.TempDir(), "messages-vault.key")
	require.NoError(t, vault.Init(keyPath, "message-passphrase"))
	v, err := vault.Open(keyPath, "message-passphrase")
	require.NoError(t, err)
	s.SetVault(v)
	_, _, err = s.SendLocalMessage(ctx, "low entropy key", testLocalMessage("msg-vault", "alice", "bob", "yes"))
	require.NoError(t, err)
	var requestHash string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT request_hash FROM message_send_idempotency WHERE message_id='msg-vault'`).Scan(&requestHash))
	require.True(t, strings.HasPrefix(requestHash, encPrefix))
	_, replayed, err := s.SendLocalMessage(ctx, "low entropy key", testLocalMessage("discarded", "alice", "bob", "yes"))
	require.NoError(t, err)
	require.True(t, replayed)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1)
	require.NoError(t, err)
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg-vault", "ok")
	require.NoError(t, err)
	var replyHash string
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT result_hash FROM message_replies WHERE message_id='msg-vault'`).Scan(&replyHash))
	require.True(t, strings.HasPrefix(replyHash, encPrefix))
	replayed, err = s.ReplyLocalMessage(ctx, "bob", "msg-vault", "ok")
	require.NoError(t, err)
	require.True(t, replayed)
}

// A fence rejection and a missing message are different facts and must not
// collapse into the same error. Collapsing them is what let the MCP client
// treat "another session holds this claim" as "this node is too old for this
// route" and retry the same call without a session id, bypassing the fence.
//
// Distinguishing them leaks nothing across an agent boundary: the fence is
// reached only after the caller is proven to be the addressed recipient, so it
// separates sessions of ONE agent.
func TestMessageReplyDistinguishesAnotherSessionsClaimFromAMissingMessage(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg", "alice", "bob", "work"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1, "session-a")
	require.NoError(t, err)

	// Same agent, different session, no handoff: refused, and refused
	// DISTINGUISHABLY.
	_, err = s.ReplyLocalMessage(ctx, "bob", "msg", "from-b", "session-b")
	require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)
	require.NotErrorIs(t, err, ErrMessageNotFound,
		"a fence rejection must not masquerade as an absent message")

	// A genuinely absent message stays a plain not-found, so the older-node
	// compatibility path keeps working.
	_, err = s.ReplyLocalMessage(ctx, "bob", "no-such-message", "from-a", "session-a")
	require.ErrorIs(t, err, ErrMessageNotFound)
	require.NotErrorIs(t, err, ErrMessageClaimedByOtherSession)

	// A different AGENT is still an ordinary not-found: the fence must never be
	// the thing that reveals another agent's message exists.
	_, err = s.ReplyLocalMessage(ctx, "mallory", "msg", "from-mallory", "session-m")
	require.ErrorIs(t, err, ErrMessageNotFound)
	require.NotErrorIs(t, err, ErrMessageClaimedByOtherSession)

	// The claiming session still succeeds, and an unfenced caller still
	// succeeds, so the fix does not tighten anything it should not.
	replayed, err := s.ReplyLocalMessage(ctx, "bob", "msg", "from-a", "session-a")
	require.NoError(t, err)
	require.False(t, replayed)
}

// After a documented handoff the new session owns the claim and the old one
// loses it — still distinguishably.
func TestMessageReplyFenceFollowsAnExplicitHandoff(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	_, _, err := s.SendLocalMessage(ctx, "send", testLocalMessage("msg", "alice", "bob", "work"))
	require.NoError(t, err)
	_, _, err = s.ReceiveLocalMessages(ctx, "bob", "", "receive", 1, "session-a")
	require.NoError(t, err)

	// The bool is "replayed" (an idempotent re-handoff), not "moved": a fresh
	// handoff reports false.
	replayedHandoff, err := s.HandoffLocalMessageClaim(ctx, "bob", "msg", "session-a", "session-b")
	require.NoError(t, err)
	require.False(t, replayedHandoff)

	_, err = s.ReplyLocalMessage(ctx, "bob", "msg", "stale", "session-a")
	require.ErrorIs(t, err, ErrMessageClaimedByOtherSession)

	replayed, err := s.ReplyLocalMessage(ctx, "bob", "msg", "fresh", "session-b")
	require.NoError(t, err)
	require.False(t, replayed)
}

// migrateMessages runs on EVERY store open, not once. It exists to rescue rows
// that v11.17.8 stamped with the old 24-hour pipeline TTL, but it used to match
// every canonical msg-* row in pending/claimed — so a sender's deliberate
// bounded ttl_minutes was re-stamped to +100 years on the next restart and the
// message became permanent. ttl_minutes is a documented parameter (0 durable,
// otherwise 1-1440), so that silently broke the contract.
func TestCanonicalMigrationKeepsSenderChosenTTLAcrossReopen(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "ttl.db")

	s, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)

	now := time.Now().UTC().Truncate(time.Millisecond)
	bounded := testLocalMessage("msg-bounded", "alice", "bob", "short-lived")
	bounded.CreatedAt = now
	bounded.ExpiresAt = now.Add(30 * time.Minute) // a caller's ttl_minutes: 30
	_, _, err = s.SendLocalMessage(ctx, "bounded-send", bounded)
	require.NoError(t, err)

	durable := testLocalMessage("msg-durable", "alice", "bob", "durable")
	durable.CreatedAt = now
	durable.ExpiresAt = now.Add(CanonicalMessageLifetime)
	_, _, err = s.SendLocalMessage(ctx, "durable-send", durable)
	require.NoError(t, err)

	expiryOf := func(store *SQLiteStore, id string) string {
		var got string
		require.NoError(t, store.conn.QueryRowContext(ctx,
			`SELECT expires_at FROM pipeline_messages WHERE pipe_id=?`, id).Scan(&got))
		return got
	}
	boundedBefore := expiryOf(s, "msg-bounded")
	durableBefore := expiryOf(s, "msg-durable")
	require.NoError(t, s.Close())

	// Reopen: this is what re-runs migrateMessages against live rows.
	reopened, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	require.Equal(t, boundedBefore, expiryOf(reopened, "msg-bounded"),
		"a sender's bounded TTL must survive a store reopen, not be re-stamped to +100 years")
	require.Equal(t, durableBefore, expiryOf(reopened, "msg-durable"),
		"an already-durable row must be left alone")
}

// The rescue itself must still work: a row carrying exactly the old 24-hour
// stamp is still extended, so an upgrade does not drop unread work.
func TestCanonicalMigrationStillRescuesTheOldTwentyFourHourStamp(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "rescue.db")

	s, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)
	now := time.Now().UTC().Truncate(time.Millisecond)
	legacy := testLocalMessage("msg-legacy", "alice", "bob", "stamped by v11.17.8")
	legacy.CreatedAt = now
	legacy.ExpiresAt = now.Add(CanonicalMessageLifetime)
	_, _, err = s.SendLocalMessage(ctx, "legacy-send", legacy)
	require.NoError(t, err)

	// Reproduce exactly what v11.17.8 left behind: expires_at == created_at+24h,
	// written through the same SQL expression the migration compares against.
	_, err = s.writeExecContext(ctx,
		`UPDATE pipeline_messages
		 SET expires_at=strftime('%Y-%m-%dT%H:%M:%fZ',created_at,'+24 hours')
		 WHERE pipe_id='msg-legacy'`)
	require.NoError(t, err)
	var stamped string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT expires_at FROM pipeline_messages WHERE pipe_id=?`, "msg-legacy").Scan(&stamped))
	require.NoError(t, s.Close())

	reopened, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	var got string
	require.NoError(t, reopened.conn.QueryRowContext(ctx,
		`SELECT expires_at FROM pipeline_messages WHERE pipe_id=?`, "msg-legacy").Scan(&got))
	require.NotEqual(t, stamped, got, "the old 24-hour stamp must still be rescued on upgrade")
	require.Greater(t, got, stamped, "rescue must extend the expiry, not shorten it")
}

// The rescue must survive PRODUCTION timestamp precision. Production writes
// expires_at through formatTime (RFC3339Nano), so a real row carries up to 9
// fractional digits, while SQLite's strftime %f emits only 3. A textual
// comparison therefore never matches a real v11.17.8 row and would rescue
// nothing at all — silently.
//
// Every other fixture here goes through testLocalMessage, which truncates to
// milliseconds and so cannot see this. This one deliberately does not.
func TestCanonicalMigrationRescuesRowsWrittenWithNanosecondPrecision(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "nano.db")

	s, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)

	// Deliberately NOT millisecond-aligned: 123456789ns.
	created := time.Date(2026, 3, 4, 5, 6, 7, 123456789, time.UTC)
	require.NotEqual(t, created, created.Truncate(time.Millisecond),
		"fixture must carry sub-millisecond precision or it cannot prove anything")

	legacy := &PipelineMessage{
		PipeID: "msg-nano", FromAgent: "alice", ToAgent: "bob",
		Intent: "request", Payload: "v11.17.8 row at production precision",
		Status: "pending", CreatedAt: created, ExpiresAt: created.Add(CanonicalMessageLifetime),
	}
	_, _, err = s.SendLocalMessage(ctx, "nano-send", legacy)
	require.NoError(t, err)

	// Reproduce the old 24-hour stamp at full precision, the way v11.17.8 left it.
	_, err = s.writeExecContext(ctx,
		`UPDATE pipeline_messages SET expires_at=? WHERE pipe_id='msg-nano'`,
		formatTime(created.Add(24*time.Hour)))
	require.NoError(t, err)
	var stamped string
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT expires_at FROM pipeline_messages WHERE pipe_id='msg-nano'`).Scan(&stamped))
	require.Contains(t, stamped, "123456789", "the stamp must retain nanosecond digits")
	require.NoError(t, s.Close())

	reopened, err := NewSQLiteStore(ctx, dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })

	var got string
	require.NoError(t, reopened.conn.QueryRowContext(ctx,
		`SELECT expires_at FROM pipeline_messages WHERE pipe_id='msg-nano'`).Scan(&got))
	require.NotEqual(t, stamped, got,
		"a real v11.17.8 row written at RFC3339Nano precision must still be rescued")
	require.Greater(t, got, stamped, "rescue must extend, not shorten")
}
