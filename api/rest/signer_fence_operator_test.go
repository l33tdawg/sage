package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/tx"
)

func signerFenceOperatorTestKey(t *testing.T) (ed25519.PrivateKey, string) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return priv, auth.PublicKeyToAgentID(pub)
}

// holdSignerFenceForRESTTest creates a real in-process fence through the public
// lease API. The resolver deliberately stays unresolved so only the route under
// test can retire the fence; cleanup uses the same proof-based lift contract.
func holdSignerFenceForRESTTest(t *testing.T) tx.FencedSigner {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub := priv.Public().(ed25519.PublicKey)
	signer := auth.PublicKeyToAgentID(pub)

	var held tx.FencedSigner
	t.Cleanup(func() {
		for _, candidate := range tx.FencedSigners() {
			if candidate.SignerPubKeyHex != signer {
				continue
			}
			_ = tx.LiftFenceWithProof(context.Background(), candidate.SignerPubKeyHex, tx.FenceLiftProof{
				Kind:     "committed",
				TxHash:   candidate.TxHash,
				Detail:   "REST operator test cleanup",
				Nonce:    candidate.Nonce,
				HasNonce: candidate.HasNonce,
			})
		}
	})

	encoded := []byte("signer-fence-operator-test-" + signer)
	resolver := func(context.Context, []byte) (tx.TxOutcome, error) {
		return tx.TxOutcome{
			Verdict: tx.TxVerdictUnresolved,
			Detail:  "test resolver intentionally leaves the fence unresolved",
		}, nil
	}
	err = tx.WithNonceLease(context.Background(), priv, func(uint64) error {
		return tx.Indeterminate(errors.New("test submit outcome indeterminate"), encoded, resolver)
	})
	require.ErrorIs(t, err, tx.ErrSubmitIndeterminate)
	require.Eventually(t, func() bool {
		for _, candidate := range tx.FencedSigners() {
			if candidate.SignerPubKeyHex == signer {
				held = candidate
				return true
			}
		}
		return false
	}, 3*time.Second, 10*time.Millisecond)
	sum := tx.CometTxHash(encoded)
	require.Equal(t, strings.ToUpper(hex.EncodeToString(sum[:])), held.TxHash)
	return held
}

func signerFenceProofRPC(t *testing.T, committed bool) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/tx" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if !committed {
			_, _ = w.Write([]byte(`{"result":null}`))
			return
		}
		hash := strings.TrimPrefix(r.URL.Query().Get("hash"), "0x")
		_, _ = fmt.Fprintf(w,
			`{"result":{"hash":%q,"height":"1","tx_result":{"code":0,"log":""}}}`,
			hash,
		)
	}))
}

func signerFenceOperatorServer(t *testing.T, cometURL string) (*Server, ed25519.PrivateKey, string) {
	t.Helper()
	server, _, _ := newTestServer(t, cometURL)
	operatorKey, operatorID := signerFenceOperatorTestKey(t)
	require.NoError(t, server.SetGovernanceOperatorID(operatorID))
	return server, operatorKey, operatorID
}

func TestSignerFenceOperatorRequiresConfiguredExactGovernanceOperator(t *testing.T) {
	t.Run("unconfigured", func(t *testing.T) {
		server, _, _ := newTestServer(t, "")
		key, callerID := signerFenceOperatorTestKey(t)
		req := signedRequestAs(t, key, callerID, http.MethodGet,
			"/v1/dashboard/signer-fence/status", nil)
		rec := httptest.NewRecorder()
		server.Router().ServeHTTP(rec, req)
		require.Equal(t, http.StatusServiceUnavailable, rec.Code, rec.Body.String())
	})

	t.Run("missing authentication", func(t *testing.T) {
		server, _, _ := signerFenceOperatorServer(t, "")
		req := httptest.NewRequest(http.MethodGet, "/v1/dashboard/signer-fence/status", nil)
		rec := httptest.NewRecorder()
		server.Router().ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
	})

	t.Run("wrong operator", func(t *testing.T) {
		server, _, _ := signerFenceOperatorServer(t, "")
		wrongKey, wrongID := signerFenceOperatorTestKey(t)
		req := signedRequestAs(t, wrongKey, wrongID, http.MethodGet,
			"/v1/dashboard/signer-fence/status", nil)
		rec := httptest.NewRecorder()
		server.Router().ServeHTTP(rec, req)
		require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	})

	t.Run("uppercase configured identity is canonicalized", func(t *testing.T) {
		server, operatorKey, operatorID := signerFenceOperatorServer(t, "")
		req := signedRequestAs(t, operatorKey, strings.ToUpper(operatorID), http.MethodGet,
			"/v1/dashboard/signer-fence/status", nil)
		rec := httptest.NewRecorder()
		server.Router().ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	})
}

func TestSignerFenceOperatorStatusExposesHeldFence(t *testing.T) {
	server, operatorKey, operatorID := signerFenceOperatorServer(t, "")
	held := holdSignerFenceForRESTTest(t)

	req := signedRequestAs(t, operatorKey, operatorID, http.MethodGet,
		"/v1/dashboard/signer-fence/status", nil)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var response signerFenceStatusResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.GreaterOrEqual(t, response.Active, 1)
	var row *signerFenceStatusRow
	for i := range response.Signers {
		if response.Signers[i].SignerPublicKey == held.SignerPubKeyHex {
			row = &response.Signers[i]
			break
		}
	}
	require.NotNil(t, row, "the held test fence must appear in the operator status")
	require.Equal(t, held.SignerPubKeyHex, row.SignerPublicKey)
	require.Equal(t, held.SignerPubKeyPrefix, row.Signer)
	require.Equal(t, held.TxHash, row.TxHash)
	if held.HasNonce {
		require.Equal(t, fmt.Sprintf("%d", held.Nonce), row.Nonce)
	} else {
		require.Empty(t, row.Nonce)
	}
	require.NotEmpty(t, row.Resolution)
	require.NotEmpty(t, response.Explanation)
}

func TestSignerFenceOperatorLiftRejectsNoFenceAndOversizedBody(t *testing.T) {
	server, operatorKey, operatorID := signerFenceOperatorServer(t, "")

	noFenceBody := []byte(`{"signer":"0000000000000000"}`)
	req := signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", noFenceBody)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())

	invalidReasonBody := []byte(`{"signer":"0000000000000000","reason":"line one\nline two"}`)
	req = signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", invalidReasonBody)
	rec = httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())

	var body bytes.Buffer
	body.WriteString(`{"signer":"0000000000000000","reason":"`)
	body.WriteString(strings.Repeat("x", maxSignerFenceLiftBody))
	body.WriteString(`"}`)
	req = signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", body.Bytes())
	rec = httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusRequestEntityTooLarge, rec.Code, rec.Body.String())
}

func TestSignerFenceOperatorLiftRejectsCallerSuppliedProof(t *testing.T) {
	server, operatorKey, operatorID := signerFenceOperatorServer(t, "")
	body := []byte(`{"signer":"0000000000000000","proof":{"kind":"committed"}}`)
	req := signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", body)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
}

func TestSignerFenceOperatorLiftKeepsFenceWithoutChainProof(t *testing.T) {
	rpc := signerFenceProofRPC(t, false)
	t.Cleanup(rpc.Close)
	server, operatorKey, operatorID := signerFenceOperatorServer(t, rpc.URL)
	held := holdSignerFenceForRESTTest(t)

	body := []byte(fmt.Sprintf(`{"signer":%q,"reason":"operator checked the chain"}`, held.SignerPubKeyHex))
	req := signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", body)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "no proof")

	found := false
	for _, candidate := range tx.FencedSigners() {
		if candidate.SignerPubKeyHex == held.SignerPubKeyHex {
			found = true
			break
		}
	}
	require.True(t, found, "an unproven lift must leave the fence standing")
}

func TestSignerFenceOperatorLiftReadsProofAndRetiresFence(t *testing.T) {
	rpc := signerFenceProofRPC(t, true)
	t.Cleanup(rpc.Close)
	server, operatorKey, operatorID := signerFenceOperatorServer(t, rpc.URL)
	held := holdSignerFenceForRESTTest(t)
	body := []byte(fmt.Sprintf(`{"signer":%q,"reason":"operator verified the committed transaction"}`, held.SignerPubKeyHex))

	req := signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", body)
	rec := httptest.NewRecorder()
	server.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), `"lifted":true`)

	for _, candidate := range tx.FencedSigners() {
		require.NotEqual(t, held.SignerPubKeyHex, candidate.SignerPubKeyHex)
	}

	// The same operator retry is truthful after the state transition: there is
	// now no held fence to lift, so it returns 404 rather than claiming a second
	// mutation or an unreported failure.
	retryBody := []byte(fmt.Sprintf(`{"signer":%q,"reason":"retry after the successful lift"}`, held.SignerPubKeyHex))
	retry := signedRequestAs(t, operatorKey, operatorID, http.MethodPost,
		"/v1/dashboard/signer-fence/lift", retryBody)
	rec = httptest.NewRecorder()
	server.Router().ServeHTTP(rec, retry)
	require.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
}
