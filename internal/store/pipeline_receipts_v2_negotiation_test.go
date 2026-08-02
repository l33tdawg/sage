package store

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/stretchr/testify/require"
)

func receiptNegotiationInboundFixture(version int) (*PipelineMessage, *PipelineTransportDedup) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	sender := strings.Repeat("a", 64)
	recipient := strings.Repeat("b", 64)
	msg := &PipelineMessage{
		PipeID: "local-imported-pipe", FromAgent: sender, ToAgent: recipient,
		Intent: "review", Payload: "immutable payload", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		SourceChainID: "chain-sender", SourcePipeID: "remote-send-event",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("1", 64),
		FederationContactID: strings.Repeat("2", 64), FederationContactRevision: strings.Repeat("3", 64),
		FederationReceiptProtocolVersion: version,
	}
	if version == FederatedPipelineReceiptVersion {
		msg.FederationReceiptContentDigest = strings.Repeat("4", 64)
		msg.FederationReceiptRecipientChainID = "chain-recipient"
	}
	dedup := &PipelineTransportDedup{
		RemoteChainID: msg.SourceChainID, PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sender,
		TargetAgentID: recipient, EventKind: "send", RemotePipeID: msg.SourcePipeID,
		ContentHash: make([]byte, 32), ProofHash: append([]byte{1}, make([]byte, 31)...),
		LocalPipeID: msg.PipeID, Outcome: "accepted", ExpiresAt: now.Add(2 * time.Hour),
	}
	return msg, dedup
}

func TestReceiptNegotiationLegacyAdmissionNeverInventsV2Binding(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "legacy-receipt-negotiation.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	msg, dedup := receiptNegotiationInboundFixture(0)
	_, duplicate, err := s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.NoError(t, err)
	require.False(t, duplicate)
	_, err = s.GetFederatedReceiptInbound(ctx, msg.PipeID)
	require.ErrorIs(t, err, ErrFederatedReceiptNotFound)
	require.NoError(t, s.Close())

	reopened, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	_, err = reopened.GetFederatedReceiptInbound(ctx, msg.PipeID)
	require.ErrorIs(t, err, ErrFederatedReceiptNotFound,
		"a legacy/zero send must not acquire receipt-v2 authority during restart migration")
}

func TestReceiptNegotiationInboundBindingSurvivesRestartExactly(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "v2-receipt-negotiation.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	msg, dedup := receiptNegotiationInboundFixture(FederatedPipelineReceiptVersion)
	_, duplicate, err := s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.NoError(t, err)
	require.False(t, duplicate)
	want, err := s.GetFederatedReceiptInbound(ctx, msg.PipeID)
	require.NoError(t, err)
	require.NoError(t, s.Close())

	reopened, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	got, err := reopened.GetFederatedReceiptInbound(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, msg.FederationReceiptContentDigest, got.ContentDigest)
}

func TestReceiptNegotiationRejectsMalformedAndUnknownVersions(t *testing.T) {
	for _, version := range []int{-1, 1, 3} {
		t.Run("version-"+strconv.Itoa(version), func(t *testing.T) {
			s := newMessageTestStore(t)
			msg, dedup := receiptNegotiationInboundFixture(version)
			_, _, err := s.AdmitFederatedPipeline(context.Background(), msg, dedup)
			require.ErrorContains(t, err, "unsupported imported receipt protocol version")
		})
	}
	t.Run("legacy-with-v2-digest", func(t *testing.T) {
		s := newMessageTestStore(t)
		msg, dedup := receiptNegotiationInboundFixture(0)
		msg.FederationReceiptContentDigest = strings.Repeat("4", 64)
		_, _, err := s.AdmitFederatedPipeline(context.Background(), msg, dedup)
		require.ErrorContains(t, err, "legacy imported pipeline cannot carry a receipt digest")
	})
}

func TestReceiptNegotiationOutboundVersionSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "outbound-receipt-negotiation.db")
	s, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	_, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sender := auth.PublicKeyToAgentID(privateKey.Public().(ed25519.PublicKey))
	recipient := strings.Repeat("b", 64)
	now := time.Now().UTC().Truncate(time.Second)
	msg := &PipelineMessage{
		PipeID: "outbound-v2-pipe", FromAgent: sender, ToAgent: recipient,
		DestinationChainID: "chain-recipient", Intent: "review", Payload: "payload",
		FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("1", 64),
		FederationContactID: strings.Repeat("2", 64), FederationContactRevision: strings.Repeat("3", 64),
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &PipelineTransportOutbox{
		EventID: "outbound-v2-event", PipeID: msg.PipeID, RemoteChainID: msg.DestinationChainID,
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: sender,
		TargetAgentID: recipient, ReceiptProtocolVersion: FederatedPipelineReceiptVersion,
		Proof: PipelineAgentProof{AgentID: sender, Signature: make([]byte, ed25519.SignatureSize),
			Timestamp: now.Unix(), CanonicalRequest: []byte("POST /v1/pipe/send\n{}")},
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, msg, outbox))
	require.NoError(t, s.Close())

	reopened, err := NewSQLiteStore(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	got, err := reopened.GetPipelineTransport(ctx, outbox.EventID)
	require.NoError(t, err)
	require.Equal(t, FederatedPipelineReceiptVersion, got.ReceiptProtocolVersion,
		"restart must not downgrade the version negotiated for the original send")
}
