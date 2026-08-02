package store

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func receiptV2Fixture(t *testing.T, s *SQLiteStore) *FederatedReceiptEvent {
	t.Helper()
	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := strings.Repeat("a", 64)
	recipient := strings.Repeat("b", 64)
	msg := &PipelineMessage{
		PipeID: "local-outbound", FromAgent: sender, ToAgent: recipient,
		DestinationChainID: "chain-recipient", Intent: "review", Payload: "exact payload",
		Status: "pending", CreatedAt: now.Add(-time.Minute), ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.InsertPipeline(context.Background(), msg))
	proof := sha256.Sum256([]byte("exact receipt proof"))
	return &FederatedReceiptEvent{
		FederatedReceiptBinding: FederatedReceiptBinding{
			MessageID: "pipe-event-origin", LocalPipeID: msg.PipeID,
			SenderChainID: "chain-sender", RecipientChainID: msg.DestinationChainID,
			SenderAgentID: sender, RecipientAgentID: recipient,
			ContentDigest: strings.Repeat("1", 64), PolicyEpoch: "epoch-v26",
			AgreementID: strings.Repeat("2", 64), ContactID: strings.Repeat("3", 64),
			ContactRevision: strings.Repeat("4", 64), AuthorizationMode: "linked-v23",
			RelationDigest: strings.Repeat("5", 64),
		},
		EventID: "receipt-event-claimed", Kind: "claimed", EventAt: now, ProofHash: proof[:],
	}
}

func TestFederatedReceiptV2MigrationInventsNoLegacyEvidence(t *testing.T) {
	s := newMessageTestStore(t)
	var events, projections int
	require.NoError(t, s.conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM pipeline_receipt_v2_events`).Scan(&events))
	require.NoError(t, s.conn.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM pipeline_receipt_v2_projection`).Scan(&projections))
	require.Zero(t, events)
	require.Zero(t, projections)
}

func TestFederatedReceiptV2ExactReplayAndIndependentDimensions(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	claimed := receiptV2Fixture(t, s)
	replayed, err := s.ApplyFederatedReceiptEvent(ctx, claimed)
	require.NoError(t, err)
	require.False(t, replayed)
	replayed, err = s.ApplyFederatedReceiptEvent(ctx, claimed)
	require.NoError(t, err)
	require.True(t, replayed)

	projection, err := s.GetFederatedReceiptForSender(ctx, claimed.SenderAgentID, claimed.LocalPipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.ClaimedAt)
	require.Nil(t, projection.ReadAt)

	read := *claimed
	read.Kind = "read"
	read.EventID = "receipt-event-read"
	read.EventAt = claimed.EventAt.Add(time.Second)
	readProof := sha256.Sum256([]byte("exact read proof"))
	read.ProofHash = readProof[:]
	replayed, err = s.ApplyFederatedReceiptEvent(ctx, &read)
	require.NoError(t, err)
	require.False(t, replayed)
	projection, err = s.GetFederatedReceiptForSender(ctx, claimed.SenderAgentID, claimed.LocalPipeID)
	require.NoError(t, err)
	require.NotNil(t, projection.ClaimedAt)
	require.NotNil(t, projection.ReadAt)

	_, err = s.GetFederatedReceiptForSender(ctx, strings.Repeat("c", 64), claimed.LocalPipeID)
	require.ErrorIs(t, err, ErrFederatedReceiptNotFound)
}

func TestFederatedReceiptV2RejectsConflictsAndCrossMessageProofReplay(t *testing.T) {
	ctx := context.Background()
	s := newMessageTestStore(t)
	event := receiptV2Fixture(t, s)
	_, err := s.ApplyFederatedReceiptEvent(ctx, event)
	require.NoError(t, err)

	conflict := *event
	conflict.EventID = "different-event-id"
	_, err = s.ApplyFederatedReceiptEvent(ctx, &conflict)
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)

	read := *event
	read.Kind = "read"
	read.EventID = "receipt-read-reused-proof"
	_, err = s.ApplyFederatedReceiptEvent(ctx, &read)
	require.ErrorIs(t, err, ErrFederatedReceiptConflict)

	badBinding := *event
	badBinding.EventID = "receipt-other-generation"
	badBinding.Kind = "read"
	badBinding.ContactRevision = strings.Repeat("6", 64)
	otherProof := sha256.Sum256([]byte("other proof"))
	badBinding.ProofHash = otherProof[:]
	_, err = s.ApplyFederatedReceiptEvent(ctx, &badBinding)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFederatedReceiptConflict) || errors.Is(err, ErrFederatedReceiptInvalid))
}
