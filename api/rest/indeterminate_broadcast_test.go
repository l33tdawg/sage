package rest

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// writeRPCErrorEnvelope answers a broadcast the way a CometBFT node does when
// its own wait for inclusion expires. This is not a verdict about the
// transaction: the bytes are on the wire and may still commit. It is the exact
// shape the sentinel cluster produced on 2026-09-15 (4x), where the mint
// committed ~15s after the caller had already been told 500.
func writeRPCErrorEnvelope(t *testing.T, w http.ResponseWriter, message, data string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      -1,
		"error":   map[string]any{"code": -32603, "message": message, "data": data},
	}))
}

func submitMemoryOnce(t *testing.T, srv *Server) *httptest.ResponseRecorder {
	t.Helper()
	body := []byte(`{
		"content": "Indeterminate broadcast fixture",
		"memory_type": "fact",
		"domain_tag": "crypto",
		"confidence_score": 0.85
	}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	return rr
}

// An ambiguous outcome must be reported as ambiguous, with the handle that
// resolves it. Before this, the caller received the same opaque 500 it receives
// for a genuine internal fault — indistinguishable from "your write never
// happened" — and the observed field behaviour was a caller re-signing a claim
// that had already committed.
func TestSubmitMemoryIndeterminateBroadcastReturnsTxHashNot500(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.URL.Path, "broadcast_tx_commit") {
			// The signer fence reconciles the ambiguous transaction in the
			// background; answer its lookups without asserting anything.
			writeRPCErrorEnvelope(t, w, "Internal error", "tx not found")
			return
		}
		writeRPCErrorEnvelope(t, w, "Internal error", "timed out waiting for tx to be included in a block")
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	rr := submitMemoryOnce(t, srv)

	require.Equal(t, http.StatusAccepted, rr.Code,
		"an indeterminate outcome is not a 500: the transaction reached the network")

	var resp IndeterminateSubmissionResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "indeterminate", resp.Status)
	assert.False(t, resp.Committed, "an unobserved outcome must not be reported as committed")
	assert.False(t, resp.Retryable, "re-signing is not a retry; the response must not invite one")
	assertStrictTxHash(t, resp.TxHash)
	require.NotNil(t, resp.Nonce, "the allocated nonce is what makes a stuck signer actionable")
	assert.Contains(t, resp.Message, "Do not resubmit")
}

// The refusal side, pinned so the new branch cannot swallow definitive
// verdicts: a CheckTx rejection is a verdict about the request and must keep
// the status it always had, with no tx hash to chase.
func TestSubmitMemoryDefinitiveRejectionKeepsItsStatus(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCometCommitFixture(t, w, r, 4, "nonce too low", 0, "", 0)
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	rr := submitMemoryOnce(t, srv)

	require.Equal(t, http.StatusBadRequest, rr.Code,
		"a CheckTx rejection is definitive and must not be reported as indeterminate")

	var problem map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &problem))
	assert.Equal(t, "Broadcast error", problem["title"])
	assert.Nil(t, problem["tx_hash"])
}

// Every handler that shares writeConsensusTxError — vote, governance, org,
// agent, federation — depends on the indeterminate branch declining what it
// does not own. A full mempool reaches that branch TYPED indeterminate (the tx
// layer cannot rule admission out from an RPC text envelope), so a handler that
// returned unconditionally on the type would answer chain backpressure with an
// empty body and a 200. This pins the fall-through, not just the happy path.
func TestWriteConsensusTxErrorKeepsMempoolFullOnItsOwnPath(t *testing.T) {
	srv, _, _ := newTestServer(t, "http://127.0.0.1:1")

	mempoolFull := tx.Indeterminate(
		fmt.Errorf("broadcast error: Internal error: mempool is full: number of txs 5000 (max: 5000)"),
		[]byte("encoded-transaction-bytes"),
		nil,
	)
	require.True(t, errors.Is(mempoolFull, tx.ErrSubmitIndeterminate),
		"fixture must be typed indeterminate to exercise the refusal")

	rr := httptest.NewRecorder()
	srv.writeConsensusTxError(rr, consensusTxSubmit, "vote", mempoolFull)

	require.Equal(t, http.StatusTooManyRequests, rr.Code,
		"a full mempool is chain backpressure, not an unknown outcome")
	require.NotEmpty(t, rr.Body.String(), "the refusal path must still write a body")
	assert.Contains(t, rr.Body.String(), "mempool full, retry later")
	assert.NotContains(t, rr.Body.String(), "indeterminate")
}
