package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/stretchr/testify/require"
)

type receiptAdmissionFixture struct {
	manager      *Manager
	store        *store.SQLiteStore
	agreement    *store.CrossFedRecord
	peerOperator string
	event        *PipeEvent
}

func newReceiptAdmissionFixture(t *testing.T, postV26 bool, version int) receiptAdmissionFixture {
	t.Helper()
	ctx := context.Background()
	manager, sqlite, badger := newDrainTestManager(t)
	manager.postV26ForNextTx = func() bool { return postV26 }
	peerOperator := newPeerOperatorID(t)
	agreement := configurePeerRBACConnection(t, manager, sqlite, badger,
		"chain-peer", peerOperator, "host", nil, 4)
	owner := newPeerOperatorID(t)
	require.NoError(t, sqlite.CreateAgent(ctx, &store.AgentEntry{
		AgentID: owner, Name: "receipt-target", Status: "active",
	}))
	require.NoError(t, badger.RegisterDomain("receipt-target", owner, "", 10))
	_, err := manager.ReplacePeerRBACPolicy(ctx, "chain-peer", []store.PeerRBACDomainPermission{{
		Domain: "receipt-target.messages", Read: true,
	}})
	require.NoError(t, err)
	grant, err := manager.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	var contact PipeContact
	for _, candidate := range grant.Contacts {
		if candidate.AgentID == owner {
			contact = candidate
			break
		}
	}
	require.Equal(t, owner, contact.AgentID)
	_, err = manager.SetPipeContactAcceptance(ctx, "chain-peer", owner, contact.ContactID, true)
	require.NoError(t, err)
	grant, err = manager.LocalPipeContacts(ctx, "chain-peer")
	require.NoError(t, err)
	for _, candidate := range grant.Contacts {
		if candidate.AgentID == owner {
			contact = candidate
			break
		}
	}
	require.True(t, contact.Accepting)

	_, sourceKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	sourceID := auth.PublicKeyToAgentID(sourceKey.Public().(ed25519.PublicKey))
	created := time.Now().UTC().Truncate(time.Second)
	signedBody, err := json.Marshal(signedPipeSendRequest{
		ToAgent: owner, SourceChainID: "chain-peer", DestinationChainID: manager.localChainID,
		Intent: "review", Payload: "receipt-bound payload", TTLMinutes: 90,
	})
	require.NoError(t, err)
	proof := signedPipeProof(t, sourceKey, sourceID, http.MethodPost,
		"/v1/pipe/send", signedBody, created.Unix())
	event := &PipeEvent{
		Version: PipeEventVersion, Kind: "send", SourceChainID: "chain-peer",
		DestinationChainID: manager.localChainID, SourceAgentID: sourceID,
		TargetAgentID: owner, Intent: "review", Payload: "receipt-bound payload",
		CreatedAt: created, ExpiresAt: created.Add(90 * time.Minute),
		PolicyEpoch: "epoch-chain-peer", AgreementID: grant.AgreementID,
		ContactID:              contact.ContactID,
		ContactRevision:        pipeContactAuthorizationRevision(grant, &contact),
		ReceiptProtocolVersion: version, Proof: proof,
	}
	event.EventID = PipelineProofEventID(event.SourceChainID, event.Kind, proof)
	if version == PipeReceiptVersion {
		event.ReceiptContentDigest = pipeReceiptContentDigest(
			event.EventID, event.SourceChainID, event.DestinationChainID,
			&store.PipelineMessage{
				FromAgent: event.SourceAgentID, ToAgent: event.TargetAgentID,
				Intent: event.Intent, Payload: event.Payload,
				CreatedAt: event.CreatedAt, ExpiresAt: event.ExpiresAt,
			},
		)
	}
	return receiptAdmissionFixture{manager, sqlite, agreement, peerOperator, event}
}

func TestReceiptV2InboundAdmissionRecomputesExactContentDigest(t *testing.T) {
	t.Run("exact digest is persisted", func(t *testing.T) {
		fixture := newReceiptAdmissionFixture(t, true, PipeReceiptVersion)
		response := callPipeEvent(t, fixture.manager, fixture.agreement, fixture.peerOperator, fixture.event)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		messages, err := fixture.store.ListPipelines(context.Background(), "", 10)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		binding, err := fixture.store.GetFederatedReceiptInbound(context.Background(), messages[0].PipeID)
		require.NoError(t, err)
		require.Equal(t, fixture.event.ReceiptContentDigest, binding.ContentDigest)
		require.Equal(t, fixture.event.EventID, binding.MessageID)
	})

	t.Run("one-bit digest change is rejected", func(t *testing.T) {
		fixture := newReceiptAdmissionFixture(t, true, PipeReceiptVersion)
		first := byte('0')
		if fixture.event.ReceiptContentDigest[0] == first {
			first = '1'
		}
		fixture.event.ReceiptContentDigest = string(first) + fixture.event.ReceiptContentDigest[1:]
		response := callPipeEvent(t, fixture.manager, fixture.agreement, fixture.peerOperator, fixture.event)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		messages, err := fixture.store.ListPipelines(context.Background(), "", 10)
		require.NoError(t, err)
		require.Empty(t, messages, "a digest mismatch must not partially admit the message")
	})
}

func TestReceiptV2AdmissionRequiresAppV26(t *testing.T) {
	fixture := newReceiptAdmissionFixture(t, false, PipeReceiptVersion)
	response := callPipeEvent(t, fixture.manager, fixture.agreement, fixture.peerOperator, fixture.event)
	require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	messages, err := fixture.store.ListPipelines(context.Background(), "", 10)
	require.NoError(t, err)
	require.Empty(t, messages)
}

func TestReceiptNegotiationLegacyAndUnknownWireVersions(t *testing.T) {
	t.Run("zero remains legacy without receipt binding", func(t *testing.T) {
		fixture := newReceiptAdmissionFixture(t, true, 0)
		response := callPipeEvent(t, fixture.manager, fixture.agreement, fixture.peerOperator, fixture.event)
		require.Equal(t, http.StatusOK, response.Code, response.Body.String())
		messages, err := fixture.store.ListPipelines(context.Background(), "", 10)
		require.NoError(t, err)
		require.Len(t, messages, 1)
		_, err = fixture.store.GetFederatedReceiptInbound(context.Background(), messages[0].PipeID)
		require.ErrorIs(t, err, store.ErrFederatedReceiptNotFound)
	})

	for _, version := range []int{-1, 1, 3} {
		t.Run("unknown-version-"+strconv.Itoa(version), func(t *testing.T) {
			fixture := newReceiptAdmissionFixture(t, true, version)
			response := callPipeEvent(t, fixture.manager, fixture.agreement, fixture.peerOperator, fixture.event)
			require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
		})
	}

	t.Run("zero with v2 digest is malformed", func(t *testing.T) {
		fixture := newReceiptAdmissionFixture(t, true, 0)
		fixture.event.ReceiptContentDigest = strings.Repeat("4", 64)
		response := callPipeEvent(t, fixture.manager, fixture.agreement, fixture.peerOperator, fixture.event)
		require.Equal(t, http.StatusBadRequest, response.Code, response.Body.String())
	})
}
