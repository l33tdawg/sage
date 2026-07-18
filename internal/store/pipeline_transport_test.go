package store

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/vault"
)

func testPipelineTransportProof(t *testing.T) PipelineAgentProof {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return PipelineAgentProof{
		AgentID: hex.EncodeToString(pub), Signature: make([]byte, ed25519.SignatureSize),
		Timestamp: time.Now().Unix(), Nonce: []byte("12345678"),
		CanonicalRequest: []byte("POST /v1/pipe/send\n{\"payload\":\"secret\"}"),
	}
}

func TestInsertPipelineWithTransportIsAtomicAndVaultEncryptsProof(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "pipes.db"))
	require.NoError(t, err)
	defer s.Close()
	keyFile := filepath.Join(t.TempDir(), "vault.key")
	require.NoError(t, vault.Init(keyFile, "transport-passphrase"))
	v, err := vault.Open(keyFile, "transport-passphrase")
	require.NoError(t, err)
	s.SetVault(v)

	proof := testPipelineTransportProof(t)
	now := time.Now().UTC()
	msg := &PipelineMessage{
		PipeID: "pipe-local", FromAgent: proof.AgentID, ToAgent: strings.Repeat("b", 64),
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Intent: "review", Payload: "secret work",
		Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	event := &PipelineTransportOutbox{
		EventID: "event-1", PipeID: msg.PipeID, RemoteChainID: msg.DestinationChainID,
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch,
		AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
		ContactRevision: msg.FederationContactRevision, SourceAgentID: msg.FromAgent,
		TargetAgentID: msg.ToAgent, Proof: proof, CreatedAt: now, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, msg, event))
	var rawProof string
	var rawSignature, rawNonce []byte
	var rawTimestamp int64
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT proof_signature, proof_timestamp, proof_nonce, proof_canonical
		 FROM pipeline_transport_outbox WHERE event_id=?`, event.EventID).
		Scan(&rawSignature, &rawTimestamp, &rawNonce, &rawProof))
	require.NotEqual(t, string(proof.CanonicalRequest), rawProof)
	require.True(t, strings.HasPrefix(rawProof, encPrefix))
	require.Empty(t, rawSignature)
	require.Zero(t, rawTimestamp)
	require.Empty(t, rawNonce)

	pending, err := s.ListPendingPipelineTransport(ctx, now.Add(time.Second), 10)
	require.NoError(t, err)
	require.Len(t, pending, 1)
	require.Equal(t, proof.CanonicalRequest, pending[0].Proof.CanonicalRequest)
	require.NoError(t, s.MarkPipelineTransportDelivered(ctx, event.EventID))
	pending, err = s.ListPendingPipelineTransport(ctx, now.Add(time.Second), 10)
	require.NoError(t, err)
	require.Empty(t, pending)

	badMsg := *msg
	badMsg.PipeID = "pipe-rollback"
	badEvent := *event
	badEvent.EventID, badEvent.PipeID, badEvent.ContactID = "event-bad", badMsg.PipeID, strings.Repeat("f", 64)
	require.Error(t, s.InsertPipelineWithTransport(ctx, &badMsg, &badEvent))
	_, err = s.GetPipeline(ctx, badMsg.PipeID)
	require.Error(t, err, "a rejected outbox binding must roll back the pipeline row")
}

func TestTerminalPipelineDeliveryUpdatesReachEachLocalSignerOnce(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	now := time.Now().UTC()

	// Outbound work: the sender owns terminal send feedback.
	sendProof := testPipelineTransportProof(t)
	remoteAgent := strings.Repeat("b", 64)
	sendMsg := &PipelineMessage{
		PipeID: "pipe-failed-send", FromAgent: sendProof.AgentID, ToAgent: remoteAgent,
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "work", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	sendEvent := &PipelineTransportOutbox{
		EventID: "event-failed-send", PipeID: sendMsg.PipeID, RemoteChainID: sendMsg.DestinationChainID,
		EventKind: "send", PolicyEpoch: sendMsg.FederationPolicyEpoch, AgreementID: sendMsg.FederationAgreementID,
		ContactID: sendMsg.FederationContactID, ContactRevision: sendMsg.FederationContactRevision,
		SourceAgentID: sendMsg.FromAgent, TargetAgentID: sendMsg.ToAgent, Proof: sendProof,
		CreatedAt: now, ExpiresAt: sendMsg.ExpiresAt,
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, sendMsg, sendEvent))
	require.NoError(t, s.RecordPipelineTransportFailure(ctx, sendEvent.EventID, "peer rejected send", now, true))

	wrong, err := s.ListPipelineDeliveryUpdates(ctx, remoteAgent, 10)
	require.NoError(t, err)
	require.Empty(t, wrong, "target agent must not see the sender's local delivery notice")
	sendUpdates, err := s.ListPipelineDeliveryUpdates(ctx, sendMsg.FromAgent, 10)
	require.NoError(t, err)
	require.Len(t, sendUpdates, 1)
	require.Equal(t, "send", sendUpdates[0].EventKind)
	require.Equal(t, "peer rejected send", sendUpdates[0].LastError)

	// Imported work: the local completer owns terminal result feedback even
	// though its pipeline row remains locally completed.
	resultProof := testPipelineTransportProof(t)
	imported := &PipelineMessage{
		PipeID: "pipe-failed-result", FromAgent: remoteAgent, ToAgent: resultProof.AgentID,
		SourceChainID: "chain-peer", SourcePipeID: "remote-event", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "request", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, imported))
	require.NoError(t, s.ClaimPipeline(ctx, imported.PipeID, imported.ToAgent))
	resultEvent := &PipelineTransportOutbox{
		EventID: "event-failed-result", PipeID: imported.PipeID, RemoteChainID: imported.SourceChainID,
		EventKind: "result", PolicyEpoch: imported.FederationPolicyEpoch, AgreementID: imported.FederationAgreementID,
		ContactID: imported.FederationContactID, ContactRevision: imported.FederationContactRevision,
		SourceAgentID: imported.ToAgent, TargetAgentID: imported.FromAgent, Proof: resultProof,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.CompleteFederatedPipelineWithTransport(ctx, imported.PipeID, imported.ToAgent, "answer", resultEvent))
	require.NoError(t, s.RecordPipelineTransportFailure(ctx, resultEvent.EventID, "peer rejected result", now, true))
	resultUpdates, err := s.ListPipelineDeliveryUpdates(ctx, imported.ToAgent, 10)
	require.NoError(t, err)
	require.Len(t, resultUpdates, 1)
	require.Equal(t, "result", resultUpdates[0].EventKind)
	require.Equal(t, "peer rejected result", resultUpdates[0].LastError)
	stored, err := s.GetPipeline(ctx, imported.PipeID)
	require.NoError(t, err)
	require.Equal(t, "completed", stored.Status)

	again, err := s.ListPipelineDeliveryUpdates(ctx, sendMsg.FromAgent, 10)
	require.NoError(t, err)
	require.Empty(t, again, "reported terminal notices must not repeat on every turn")
	again, err = s.ListPipelineDeliveryUpdates(ctx, imported.ToAgent, 10)
	require.NoError(t, err)
	require.Empty(t, again)
}

func TestAdmitFederatedPipelineDeduplicatesAndRejectsEquivocation(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	now := time.Now().UTC()
	contentHash := sha256.Sum256([]byte("immutable event"))
	makePair := func(localID string, hash []byte) (*PipelineMessage, *PipelineTransportDedup) {
		msg := &PipelineMessage{
			PipeID: localID, FromAgent: strings.Repeat("a", 64), ToAgent: strings.Repeat("b", 64),
			SourceChainID: "chain-peer", SourcePipeID: "peer-pipe-1",
			FederationPolicyEpoch: "epoch-1", FederationAgreementID: strings.Repeat("c", 64),
			FederationContactID: strings.Repeat("d", 64), FederationContactRevision: strings.Repeat("e", 64),
			Intent: "review", Payload: "work", Status: "pending", CreatedAt: now, ExpiresAt: now.Add(time.Hour),
		}
		dedup := &PipelineTransportDedup{
			RemoteChainID: msg.SourceChainID, PolicyEpoch: msg.FederationPolicyEpoch,
			AgreementID: msg.FederationAgreementID, ContactID: msg.FederationContactID,
			ContactRevision: msg.FederationContactRevision, SourceAgentID: msg.FromAgent,
			TargetAgentID: msg.ToAgent,
			EventKind:     "send", RemotePipeID: msg.SourcePipeID, ContentHash: hash,
			ProofHash:   hash,
			LocalPipeID: msg.PipeID, Outcome: "accepted", ExpiresAt: now.Add(48 * time.Hour),
		}
		return msg, dedup
	}

	msg, dedup := makePair("pipe-import-1", contentHash[:])
	localID, duplicate, err := s.AdmitFederatedPipeline(ctx, msg, dedup)
	require.NoError(t, err)
	require.False(t, duplicate)
	require.Equal(t, msg.PipeID, localID)

	replayMsg, replayDedup := makePair("pipe-import-2", contentHash[:])
	localID, duplicate, err = s.AdmitFederatedPipeline(ctx, replayMsg, replayDedup)
	require.NoError(t, err)
	require.True(t, duplicate)
	require.Equal(t, msg.PipeID, localID, "duplicate replay must return the original local pipe ID")

	rewrappedMsg, rewrappedDedup := makePair("pipe-import-rewrapped", contentHash[:])
	rewrappedMsg.SourcePipeID = "peer-pipe-rewrapped"
	rewrappedDedup.RemotePipeID = rewrappedMsg.SourcePipeID
	rewrappedDedup.LocalPipeID = rewrappedMsg.PipeID
	_, _, err = s.AdmitFederatedPipeline(ctx, rewrappedMsg, rewrappedDedup)
	require.ErrorIs(t, err, ErrPipelineTransportReplay,
		"one agent proof must not be reusable under a fresh node-minted pipe id")

	different := sha256.Sum256([]byte("different event"))
	equivMsg, equivDedup := makePair("pipe-import-3", different[:])
	_, _, err = s.AdmitFederatedPipeline(ctx, equivMsg, equivDedup)
	require.ErrorIs(t, err, ErrPipelineTransportEquivocation)
	pipes, err := s.ListPipelines(ctx, "", 10)
	require.NoError(t, err)
	require.Len(t, pipes, 1)
}

func TestPurgeExpiredPipelineTransportFailsStrandedOutboundPipe(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	proof := testPipelineTransportProof(t)
	now := time.Now().UTC()
	msg := &PipelineMessage{
		PipeID: "pipe-expired-outbound", FromAgent: proof.AgentID, ToAgent: strings.Repeat("b", 64),
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "work", Status: "pending",
		CreatedAt: now.Add(-2 * time.Hour), ExpiresAt: now.Add(-time.Hour),
	}
	event := &PipelineTransportOutbox{
		EventID: "event-expired", PipeID: msg.PipeID, RemoteChainID: msg.DestinationChainID,
		EventKind: "send", PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		SourceAgentID: msg.FromAgent, TargetAgentID: msg.ToAgent, Proof: proof,
		CreatedAt: msg.CreatedAt, ExpiresAt: msg.ExpiresAt,
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, msg, event))
	changed, err := s.PurgeExpiredPipelineTransport(ctx, now)
	require.NoError(t, err)
	require.Equal(t, 1, changed)
	stored, err := s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "failed", stored.Status)
	transport, err := s.GetPipelineTransport(ctx, event.EventID)
	require.NoError(t, err)
	require.Equal(t, "failed", transport.State)
}

func TestPurgeSyncPeerStateFailsBothPipelineDirectionsAndPreservesReplayTombstone(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	proof := testPipelineTransportProof(t)
	now := time.Now().UTC()
	outbound := &PipelineMessage{
		PipeID: "pipe-purge-outbound", FromAgent: proof.AgentID, ToAgent: strings.Repeat("b", 64),
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "outbound", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	outbox := &PipelineTransportOutbox{
		EventID: "event-purge", PipeID: outbound.PipeID, RemoteChainID: "chain-peer", EventKind: "send",
		PolicyEpoch: outbound.FederationPolicyEpoch, AgreementID: outbound.FederationAgreementID,
		ContactID: outbound.FederationContactID, ContactRevision: outbound.FederationContactRevision,
		SourceAgentID: outbound.FromAgent, TargetAgentID: outbound.ToAgent, Proof: proof,
		CreatedAt: now, ExpiresAt: outbound.ExpiresAt,
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, outbound, outbox))

	contentHash := sha256.Sum256([]byte("inbound-event"))
	proofHash := sha256.Sum256([]byte("inbound-proof"))
	inbound := &PipelineMessage{
		PipeID: "pipe-purge-inbound", FromAgent: strings.Repeat("a", 64), ToAgent: strings.Repeat("f", 64),
		SourceChainID: "chain-peer", SourcePipeID: "remote-event", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "inbound", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	dedup := &PipelineTransportDedup{
		RemoteChainID: "chain-peer", PolicyEpoch: inbound.FederationPolicyEpoch,
		AgreementID: inbound.FederationAgreementID, ContactID: inbound.FederationContactID,
		ContactRevision: inbound.FederationContactRevision, SourceAgentID: inbound.FromAgent,
		TargetAgentID: inbound.ToAgent, EventKind: "send", RemotePipeID: inbound.SourcePipeID,
		ContentHash: contentHash[:], ProofHash: proofHash[:], LocalPipeID: inbound.PipeID,
		Outcome: "accepted", ExpiresAt: now.Add(2 * time.Hour),
	}
	_, _, err = s.AdmitFederatedPipeline(ctx, inbound, dedup)
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET status='claimed' WHERE pipe_id=?`, inbound.PipeID)
	require.NoError(t, err)

	require.NoError(t, s.PurgeSyncPeerState(ctx, "chain-peer"))
	for _, pipeID := range []string{outbound.PipeID, inbound.PipeID} {
		stored, getErr := s.GetPipeline(ctx, pipeID)
		require.NoError(t, getErr)
		require.Equal(t, "failed", stored.Status)
	}
	var state, lastError, canonical string
	var signature, nonce []byte
	var timestamp int64
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT state, last_error,
		proof_signature, proof_timestamp, proof_nonce, proof_canonical
		FROM pipeline_transport_outbox WHERE event_id=?`, outbox.EventID).
		Scan(&state, &lastError, &signature, &timestamp, &nonce, &canonical))
	require.Equal(t, "failed", state)
	require.Contains(t, lastError, "revoked before delivery")
	require.Empty(t, signature)
	require.Zero(t, timestamp)
	require.Empty(t, nonce)
	require.Empty(t, canonical, "revocation must retain metadata, not signed payload/proof material")
	updates, err := s.ListPipelineDeliveryUpdates(ctx, outbound.FromAgent, 10)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, outbox.EventID, updates[0].EventID)
	var tombstones int
	require.NoError(t, s.conn.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM pipeline_transport_dedup WHERE remote_chain_id=?`, "chain-peer").Scan(&tombstones))
	require.Equal(t, 1, tombstones, "revocation must not erase replay history")

	fresh := *inbound
	fresh.PipeID = "pipe-after-repair"
	fresh.SourcePipeID = "remote-event-fresh"
	fresh.Status = "pending"
	require.NoError(t, s.InsertPipeline(ctx, &fresh), "failed retired rows must not poison a quick re-pair")
}

func TestPurgeSyncPeerStatePreservesPendingResultNotice(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	now := time.Now().UTC()
	remoteAgent := strings.Repeat("a", 64)
	resultProof := testPipelineTransportProof(t)
	msg := &PipelineMessage{
		PipeID: "pipe-revoked-result", FromAgent: remoteAgent, ToAgent: resultProof.AgentID,
		SourceChainID: "chain-peer", SourcePipeID: "remote-pipe", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "work", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, msg))
	require.NoError(t, s.ClaimPipeline(ctx, msg.PipeID, msg.ToAgent))
	event := &PipelineTransportOutbox{
		EventID: "event-revoked-result", PipeID: msg.PipeID, RemoteChainID: msg.SourceChainID,
		EventKind: "result", PolicyEpoch: msg.FederationPolicyEpoch, AgreementID: msg.FederationAgreementID,
		ContactID: msg.FederationContactID, ContactRevision: msg.FederationContactRevision,
		SourceAgentID: msg.ToAgent, TargetAgentID: msg.FromAgent, Proof: resultProof,
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.CompleteFederatedPipelineWithTransport(ctx, msg.PipeID, msg.ToAgent, "answer", event))
	require.NoError(t, s.PurgeSyncPeerState(ctx, msg.SourceChainID))

	stored, err := s.GetPipeline(ctx, msg.PipeID)
	require.NoError(t, err)
	require.Equal(t, "completed", stored.Status, "local completion must remain truthful")
	updates, err := s.ListPipelineDeliveryUpdates(ctx, msg.ToAgent, 10)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.Equal(t, "result", updates[0].EventKind)
	require.Contains(t, updates[0].LastError, "revoked before delivery")
}

func TestPurgePipelinesRetainsPendingAndUnreportedTransport(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, ":memory:")
	require.NoError(t, err)
	defer s.Close()
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)

	// A completed imported request may have queued its result near the end of
	// the original request's retention window. Its pending result must outlive
	// the pipeline created_at cutoff until transport reaches a terminal state.
	resultProof := testPipelineTransportProof(t)
	resultMsg := &PipelineMessage{
		PipeID: "pipe-old-result", FromAgent: strings.Repeat("a", 64), ToAgent: resultProof.AgentID,
		SourceChainID: "chain-peer", SourcePipeID: "remote-old", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "work", Status: "pending",
		CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.InsertPipeline(ctx, resultMsg))
	require.NoError(t, s.ClaimPipeline(ctx, resultMsg.PipeID, resultMsg.ToAgent))
	resultEvent := &PipelineTransportOutbox{
		EventID: "event-old-result", PipeID: resultMsg.PipeID, RemoteChainID: resultMsg.SourceChainID,
		EventKind: "result", PolicyEpoch: resultMsg.FederationPolicyEpoch,
		AgreementID: resultMsg.FederationAgreementID, ContactID: resultMsg.FederationContactID,
		ContactRevision: resultMsg.FederationContactRevision, SourceAgentID: resultMsg.ToAgent,
		TargetAgentID: resultMsg.FromAgent, Proof: resultProof, CreatedAt: now, ExpiresAt: now.Add(time.Hour),
	}
	require.NoError(t, s.CompleteFederatedPipelineWithTransport(ctx, resultMsg.PipeID, resultMsg.ToAgent, "answer", resultEvent))
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_messages SET created_at=? WHERE pipe_id=?`, formatTime(old), resultMsg.PipeID)
	require.NoError(t, err)

	// An expired/terminal send must retain its unreported one-shot failure notice.
	sendProof := testPipelineTransportProof(t)
	sendMsg := &PipelineMessage{
		PipeID: "pipe-old-send", FromAgent: sendProof.AgentID, ToAgent: strings.Repeat("b", 64),
		DestinationChainID: "chain-peer", FederationPolicyEpoch: "epoch-1",
		FederationAgreementID: strings.Repeat("c", 64), FederationContactID: strings.Repeat("d", 64),
		FederationContactRevision: strings.Repeat("e", 64), Payload: "work", Status: "pending",
		CreatedAt: old, ExpiresAt: now.Add(-time.Hour),
	}
	sendEvent := &PipelineTransportOutbox{
		EventID: "event-old-send", PipeID: sendMsg.PipeID, RemoteChainID: sendMsg.DestinationChainID,
		EventKind: "send", PolicyEpoch: sendMsg.FederationPolicyEpoch, AgreementID: sendMsg.FederationAgreementID,
		ContactID: sendMsg.FederationContactID, ContactRevision: sendMsg.FederationContactRevision,
		SourceAgentID: sendMsg.FromAgent, TargetAgentID: sendMsg.ToAgent, Proof: sendProof,
		CreatedAt: old, ExpiresAt: sendMsg.ExpiresAt,
	}
	require.NoError(t, s.InsertPipelineWithTransport(ctx, sendMsg, sendEvent))
	require.NoError(t, s.RecordPipelineTransportFailure(ctx, sendEvent.EventID, "peer unavailable", now, true))

	purged, err := s.PurgePipelines(ctx, now.Add(-24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, purged)
	_, err = s.GetPipeline(ctx, resultMsg.PipeID)
	require.NoError(t, err)
	_, err = s.GetPipeline(ctx, sendMsg.PipeID)
	require.NoError(t, err)

	// Once the failure is reported and the pending result is delivered, normal
	// retention can reclaim both rows and their transport metadata.
	updates, err := s.ListPipelineDeliveryUpdates(ctx, sendMsg.FromAgent, 10)
	require.NoError(t, err)
	require.Len(t, updates, 1)
	require.NoError(t, s.MarkPipelineTransportDelivered(ctx, resultEvent.EventID))
	purged, err = s.PurgePipelines(ctx, now.Add(-24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, 2, purged)
}
