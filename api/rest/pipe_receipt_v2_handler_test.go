package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

type receiptV2Controller struct {
	*fakeFederation
	recipient string
	challenge json.RawMessage
	records   []pipeReceiptBatchItem
	replays   map[string]bool
	fail      map[string]error
	claimed   map[string]bool
}

func (c *receiptV2Controller) ImportedPipeReceiptChallenge(
	_ context.Context, pipeID, recipientID, kind string,
) (json.RawMessage, error) {
	if recipientID != c.recipient || pipeID == "" || pipeID == "missing" ||
		(kind != "claimed" && kind != "read") {
		return nil, store.ErrFederatedReceiptNotFound
	}
	return append(json.RawMessage(nil), c.challenge...), nil
}

func (c *receiptV2Controller) RecordImportedPipeReceipt(
	_ context.Context, pipeID, recipientID, kind string, proof store.PipelineAgentProof,
) (bool, error) {
	key := pipeID + "\x00" + kind
	if recipientID != c.recipient || proof.AgentID != recipientID || pipeID == "" || pipeID == "missing" {
		return false, store.ErrFederatedReceiptNotFound
	}
	if kind != "claimed" && kind != "read" {
		return false, store.ErrFederatedReceiptInvalid
	}
	if err := verifyReceiptV2Proof(proof, http.MethodPut, "/v1/pipe/"+pipeID+"/receipt/"+kind, c.challenge); err != nil {
		return false, err
	}
	if err := c.fail[key]; err != nil {
		return false, err
	}
	if kind == "read" && !c.claimed[pipeID] {
		return false, store.ErrFederatedReceiptConflict
	}
	replayed := c.replays[key]
	c.replays[key] = true
	if kind == "claimed" {
		if c.claimed == nil {
			c.claimed = make(map[string]bool)
		}
		c.claimed[pipeID] = true
	}
	c.records = append(c.records, pipeReceiptBatchItem{PipeID: pipeID, Kind: kind, Proof: proof})
	return replayed, nil
}

func verifyReceiptV2Proof(proof store.PipelineAgentProof, method, path string, body []byte) error {
	pub, err := auth.AgentIDToPublicKey(proof.AgentID)
	if err != nil || len(proof.Nonce) < 8 || len(proof.Signature) != ed25519.SignatureSize {
		return store.ErrFederatedReceiptInvalid
	}
	wantCanonical := append([]byte(method+" "+path+"\n"), body...)
	if !bytes.Equal(proof.CanonicalRequest, wantCanonical) ||
		!auth.VerifyRequestWithNonce(pub, method, path, body, proof.Timestamp, proof.Nonce, proof.Signature) {
		return store.ErrFederatedReceiptInvalid
	}
	return nil
}

type receiptV2StatusStore struct {
	*store.SQLiteStore
	transport     *store.PipelineTransportOutbox
	transportErr  error
	projection    *store.FederatedReceiptProjection
	projectionErr error
	projectionN   int
}

func (s *receiptV2StatusStore) GetPipelineTransportForPipe(context.Context, string, string) (*store.PipelineTransportOutbox, error) {
	if s.transportErr != nil {
		return nil, s.transportErr
	}
	if s.transport == nil {
		return nil, errors.New("transport not found")
	}
	copy := *s.transport
	return &copy, nil
}

func (s *receiptV2StatusStore) GetFederatedReceiptForSender(context.Context, string, string) (*store.FederatedReceiptProjection, error) {
	s.projectionN++
	if s.projectionErr != nil {
		return nil, s.projectionErr
	}
	if s.projection == nil {
		return nil, store.ErrFederatedReceiptNotFound
	}
	copy := *s.projection
	return &copy, nil
}

type receiptV2Signer struct {
	id   string
	priv ed25519.PrivateKey
}

func newReceiptV2Signer(t *testing.T) receiptV2Signer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	return receiptV2Signer{id: hex.EncodeToString(pub), priv: priv}
}

func (s receiptV2Signer) proof(t *testing.T, method, path string, body []byte) store.PipelineAgentProof {
	t.Helper()
	nonce := make([]byte, 16)
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	ts := time.Now().Unix()
	return store.PipelineAgentProof{
		AgentID: s.id, Signature: auth.SignRequestWithNonce(s.priv, method, path, body, ts, nonce),
		Timestamp: ts, Nonce: nonce,
		CanonicalRequest: append([]byte(method+" "+path+"\n"), body...),
	}
}

func (s receiptV2Signer) request(t *testing.T, method, path string, body []byte, nonceBound bool) *http.Request {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	ts := time.Now().Unix()
	var signature []byte
	if nonceBound {
		nonce := make([]byte, 16)
		_, err := rand.Read(nonce)
		require.NoError(t, err)
		signature = auth.SignRequestWithNonce(s.priv, method, path, body, ts, nonce)
		req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	} else {
		signature = auth.SignRequest(s.priv, method, path, body, ts)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", s.id)
	req.Header.Set("X-Signature", hex.EncodeToString(signature))
	req.Header.Set("X-Timestamp", strconv.FormatInt(ts, 10))
	return req
}

func receiptV2Router(s *Server) http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Ed25519AuthMiddleware)
	r.Get("/v1/pipe/{pipe_id}/receipt/challenge/{kind}", s.handlePipeReceiptChallenge)
	r.Put("/v1/pipe/{pipe_id}/receipt/{kind}", s.handlePipeReceiptRecord)
	r.Post("/v1/pipe/receipts/challenge-batch", s.handlePipeReceiptChallengeBatch)
	r.Put("/v1/pipe/receipts/batch", s.handlePipeReceiptRecordBatch)
	r.Get("/v1/pipe/{pipe_id}/receipt", s.handlePipeReceiptStatus)
	return r
}

func callReceiptV2(t *testing.T, handler http.Handler, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	return rr
}

func receiptV2Body(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func TestPipeReceiptV2SingularUsesNonceBoundExactRecipientProofAndHidesExistence(t *testing.T) {
	s, _ := newPipeServer(t)
	recipient := newReceiptV2Signer(t)
	challenge := json.RawMessage(`{"version":2,"message_id":"message-1","event_kind":"claimed"}`)
	controller := &receiptV2Controller{
		fakeFederation: &fakeFederation{}, recipient: recipient.id, challenge: challenge,
		replays: make(map[string]bool), fail: make(map[string]error),
	}
	s.SetFederation(controller)
	handler := receiptV2Router(s)

	unsigned := callReceiptV2(t, handler, httptest.NewRequest(http.MethodGet, "/v1/pipe/pipe-1/receipt/challenge/claimed", nil))
	require.Equal(t, http.StatusUnauthorized, unsigned.Code)
	legacySigned := callReceiptV2(t, handler, recipient.request(t, http.MethodGet, "/v1/pipe/pipe-1/receipt/challenge/claimed", nil, false))
	require.Equal(t, http.StatusForbidden, legacySigned.Code)

	ready := callReceiptV2(t, handler, recipient.request(t, http.MethodGet, "/v1/pipe/pipe-1/receipt/challenge/claimed", nil, true))
	require.Equal(t, http.StatusOK, ready.Code, ready.Body.String())
	require.Contains(t, ready.Body.String(), `"challenge":{"version":2`)

	wrongRecipient := newReceiptV2Signer(t)
	forbidden := callReceiptV2(t, handler, wrongRecipient.request(t, http.MethodGet, "/v1/pipe/pipe-1/receipt/challenge/claimed", nil, true))
	missingServer, _ := newPipeServer(t)
	missingServer.SetFederation(&receiptV2Controller{
		fakeFederation: &fakeFederation{}, recipient: "not-the-caller", challenge: challenge,
		replays: make(map[string]bool), fail: make(map[string]error),
	})
	missing := callReceiptV2(t, receiptV2Router(missingServer), wrongRecipient.request(t, http.MethodGet, "/v1/pipe/pipe-1/receipt/challenge/claimed", nil, true))
	require.Equal(t, http.StatusNotFound, forbidden.Code)
	require.Equal(t, http.StatusNotFound, missing.Code)
	require.Equal(t, missing.Body.String(), forbidden.Body.String(),
		"an unauthorized existing receipt and the same absent receipt must be indistinguishable")
	require.NotContains(t, forbidden.Body.String(), recipient.id)
	require.NotContains(t, missing.Body.String(), recipient.id)

	recorded := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, "/v1/pipe/pipe-1/receipt/claimed", challenge, true))
	require.Equal(t, http.StatusOK, recorded.Code, recorded.Body.String())
	require.Contains(t, recorded.Body.String(), `"idempotent_replay":false`)
	legacyRecord := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, "/v1/pipe/pipe-1/receipt/claimed", challenge, false))
	require.Equal(t, http.StatusForbidden, legacyRecord.Code)
	replayed := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, "/v1/pipe/pipe-1/receipt/claimed", challenge, true))
	require.Equal(t, http.StatusOK, replayed.Code, replayed.Body.String())
	require.Contains(t, replayed.Body.String(), `"idempotent_replay":true`)
	require.Len(t, controller.records, 2)

	wrongBody := json.RawMessage(`{"version":2,"message_id":"different","event_kind":"claimed"}`)
	bound := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, "/v1/pipe/pipe-1/receipt/claimed", wrongBody, true))
	require.Equal(t, http.StatusConflict, bound.Code)
	invalidKind := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, "/v1/pipe/pipe-1/receipt/future", challenge, true))
	require.Equal(t, http.StatusConflict, invalidKind.Code)
}

func TestPipeReceiptV2ChallengeBatchBoundsDuplicatesAndPerItemAntiEnumeration(t *testing.T) {
	s, _ := newPipeServer(t)
	recipient := newReceiptV2Signer(t)
	controller := &receiptV2Controller{
		fakeFederation: &fakeFederation{}, recipient: recipient.id,
		challenge: json.RawMessage(`{"version":2}`), replays: make(map[string]bool), fail: make(map[string]error),
	}
	s.SetFederation(controller)
	handler := receiptV2Router(s)
	path := "/v1/pipe/receipts/challenge-batch"

	for _, items := range [][]pipeReceiptBatchItem{{}, make([]pipeReceiptBatchItem, maxPipeReceiptBatchItems+1)} {
		body := receiptV2Body(t, pipeReceiptBatchRequest{Items: items})
		rr := callReceiptV2(t, handler, recipient.request(t, http.MethodPost, path, body, true))
		require.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	}

	body := receiptV2Body(t, pipeReceiptBatchRequest{Items: []pipeReceiptBatchItem{
		{PipeID: "pipe-a", Kind: "claimed"},
		{PipeID: "pipe-a", Kind: "claimed"},
		{PipeID: "", Kind: "read"},
		{PipeID: "pipe-b", Kind: "future"},
		{PipeID: "missing", Kind: "read"},
		{PipeID: "pipe-c", Kind: "read"},
	}})
	rr := callReceiptV2(t, handler, recipient.request(t, http.MethodPost, path, body, true))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response struct {
		Items []struct {
			PipeID string `json:"pipe_id"`
			Kind   string `json:"event_kind"`
			Status string `json:"status"`
			Error  string `json:"error"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Equal(t, []string{"ready", "rejected", "rejected", "rejected", "rejected", "ready"}, []string{
		response.Items[0].Status, response.Items[1].Status, response.Items[2].Status,
		response.Items[3].Status, response.Items[4].Status, response.Items[5].Status,
	})
	require.Equal(t, "duplicate_receipt_event", response.Items[1].Error)
	require.Equal(t, "invalid_receipt_event", response.Items[2].Error)
	require.Equal(t, "not_found", response.Items[4].Error)
}

func TestPipeReceiptV2RecordBatchPreservesOrderingProofBindingAndPartialResults(t *testing.T) {
	s, _ := newPipeServer(t)
	recipient := newReceiptV2Signer(t)
	challenge := json.RawMessage(`{"version":2,"message_id":"batch"}`)
	controller := &receiptV2Controller{
		fakeFederation: &fakeFederation{}, recipient: recipient.id, challenge: challenge,
		replays: make(map[string]bool), fail: map[string]error{"pipe-fail\x00claimed": store.ErrFederatedReceiptConflict},
	}
	s.SetFederation(controller)
	handler := receiptV2Router(s)
	proof := func(pipeID, kind string) store.PipelineAgentProof {
		return recipient.proof(t, http.MethodPut, "/v1/pipe/"+pipeID+"/receipt/"+kind, challenge)
	}
	wrongPathProof := recipient.proof(t, http.MethodPut, "/v1/pipe/wrong/receipt/claimed", challenge)
	wrongAgent := proof("pipe-agent", "claimed")
	wrongAgent.AgentID = newReceiptV2Signer(t).id
	body := receiptV2Body(t, pipeReceiptBatchRequest{Items: []pipeReceiptBatchItem{
		{PipeID: "pipe-fail", Kind: "claimed", Proof: proof("pipe-fail", "claimed")},
		{PipeID: "pipe-fail", Kind: "read", Proof: proof("pipe-fail", "read")},
		{PipeID: "pipe-order", Kind: "read", Proof: proof("pipe-order", "read")},
		{PipeID: "pipe-order", Kind: "claimed", Proof: proof("pipe-order", "claimed")},
		{PipeID: "pipe-good", Kind: "claimed", Proof: proof("pipe-good", "claimed")},
		{PipeID: "pipe-good", Kind: "read", Proof: proof("pipe-good", "read")},
		{PipeID: "pipe-good", Kind: "read", Proof: proof("pipe-good", "read")},
		{PipeID: "pipe-wrong", Kind: "claimed", Proof: wrongPathProof},
		{PipeID: "pipe-agent", Kind: "claimed", Proof: wrongAgent},
	}})
	path := "/v1/pipe/receipts/batch"
	rr := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, path, body, true))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var response struct {
		Items []struct {
			Status string `json:"receipt_status"`
			Error  string `json:"error"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &response))
	require.Equal(t, []string{"unconfirmed", "unconfirmed", "unconfirmed", "queued", "queued", "queued", "unconfirmed", "unconfirmed", "unconfirmed"}, []string{
		response.Items[0].Status, response.Items[1].Status, response.Items[2].Status,
		response.Items[3].Status, response.Items[4].Status, response.Items[5].Status,
		response.Items[6].Status, response.Items[7].Status, response.Items[8].Status,
	})
	require.Equal(t, "claim_not_confirmed", response.Items[1].Error)
	require.Equal(t, "duplicate_receipt_event", response.Items[6].Error)
	require.Equal(t, "invalid_receipt_proof", response.Items[8].Error)
	require.Len(t, controller.records, 3, "failed claim suppresses read, read-before-claim fails, and invalid proofs never reach durable recording")

	replayBody := receiptV2Body(t, pipeReceiptBatchRequest{Items: []pipeReceiptBatchItem{
		{PipeID: "pipe-good", Kind: "claimed", Proof: proof("pipe-good", "claimed")},
		{PipeID: "pipe-good", Kind: "read", Proof: proof("pipe-good", "read")},
	}})
	replay := callReceiptV2(t, handler, recipient.request(t, http.MethodPut, path, replayBody, true))
	require.Equal(t, http.StatusOK, replay.Code, replay.Body.String())
	require.Contains(t, replay.Body.String(), `"idempotent_replay":true`)
}

func TestPipeReceiptV2StatusIsSenderOnlyPayloadFreeAndProtocolAware(t *testing.T) {
	base, sqlite := newPipeServer(t)
	sender := newReceiptV2Signer(t)
	other := newReceiptV2Signer(t)
	require.NoError(t, sqlite.InsertPipeline(t.Context(), &store.PipelineMessage{
		PipeID: "pipe-status", FromAgent: sender.id, ToAgent: other.id,
		Payload: "must-never-leak", Status: "pending", DestinationChainID: "remote-chain",
		CreatedAt: time.Now().UTC(), ExpiresAt: time.Now().UTC().Add(time.Hour),
	}))
	wrapped := &receiptV2StatusStore{SQLiteStore: sqlite, transport: &store.PipelineTransportOutbox{
		PipeID: "pipe-status", SourceAgentID: sender.id, State: "delivered", ReceiptProtocolVersion: 1,
	}}
	base.store = wrapped
	handler := receiptV2Router(base)
	path := "/v1/pipe/pipe-status/receipt"

	unsupported := callReceiptV2(t, handler, sender.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusOK, unsupported.Code, unsupported.Body.String())
	require.Contains(t, unsupported.Body.String(), `"protocol":"unsupported"`)
	require.NotContains(t, unsupported.Body.String(), "must-never-leak")
	require.Zero(t, wrapped.projectionN)
	wrapped.transportErr = fmt.Errorf("transport database unavailable")
	transportUnavailable := callReceiptV2(t, handler, sender.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusServiceUnavailable, transportUnavailable.Code, transportUnavailable.Body.String())
	require.NotContains(t, transportUnavailable.Body.String(), "transport database unavailable")
	wrapped.transportErr = nil

	wrapped.transport.ReceiptProtocolVersion = 2
	unconfirmed := callReceiptV2(t, handler, sender.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusOK, unconfirmed.Code, unconfirmed.Body.String())
	require.Contains(t, unconfirmed.Body.String(), `"claim_status":"unconfirmed"`)

	claimedAt := time.Now().UTC().Add(-time.Minute)
	readAt := time.Now().UTC()
	wrapped.projection = &store.FederatedReceiptProjection{
		ClaimedAt: &claimedAt, ReadAt: &readAt, DeliveryEvidence: "peer_ack",
	}
	confirmed := callReceiptV2(t, handler, sender.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusOK, confirmed.Code, confirmed.Body.String())
	require.Contains(t, confirmed.Body.String(), `"claim_status":"confirmed"`)
	require.Contains(t, confirmed.Body.String(), `"read_status":"confirmed"`)
	require.NotContains(t, confirmed.Body.String(), "must-never-leak")

	forbidden := callReceiptV2(t, handler, other.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusNotFound, forbidden.Code)
	require.NotContains(t, forbidden.Body.String(), sender.id)
	require.NotContains(t, forbidden.Body.String(), "must-never-leak")
	missingBase, _ := newPipeServer(t)
	missingWrapped := &receiptV2StatusStore{SQLiteStore: missingBase.store.(*store.SQLiteStore)}
	missingBase.store = missingWrapped
	missing := callReceiptV2(t, receiptV2Router(missingBase), other.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusNotFound, missing.Code)
	require.Equal(t, missing.Body.String(), forbidden.Body.String(),
		"sender-only status must not reveal whether the same pipe id exists")

	wrapped.projection = nil
	wrapped.projectionErr = fmt.Errorf("database unavailable")
	unavailable := callReceiptV2(t, handler, sender.request(t, http.MethodGet, path, nil, true))
	require.Equal(t, http.StatusServiceUnavailable, unavailable.Code, unavailable.Body.String())
	require.NotContains(t, unavailable.Body.String(), "database unavailable")
}
