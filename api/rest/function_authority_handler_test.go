package rest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func setupFunctionAuthorityREST(t *testing.T, code uint32) (*Server, ed25519.PrivateKey, ed25519.PrivateKey, *atomic.Int32, <-chan *tx.ParsedTx) {
	t.Helper()
	rootKey := appV23ElevationTestKey("function-authority-root")
	memberKey := appV23ElevationTestKey("function-authority-member")
	rootID, memberID := appV23ElevationTestID(rootKey), appV23ElevationTestID(memberKey)
	var calls atomic.Int32
	parsed := make(chan *tx.ParsedTx, 8)
	comet := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		wire, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		valid, err := tx.VerifyTx(wire)
		require.NoError(t, err)
		require.True(t, valid)
		parsed <- wire
		writeCometCommitFixture(t, w, r, 0, "", code, "function authority control rejected", 100)
	}))
	t.Cleanup(comet.Close)
	srv, _, badger, _ := newRBACTestServer(t)
	srv.cometbftRPC = comet.URL
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "function-authority-test-chain", AgentID: memberID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "member.home", Clearance: 2,
		Height: 1, BootstrapDigest: "test-bootstrap",
	}))
	srv.SetPostV17ForNextTxAccessor(func() bool { return true })
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })
	srv.SetPostV31ForNextTxAccessor(func() bool { return true })
	return srv, rootKey, memberKey, &calls, parsed
}

func functionAuthorityRESTBody(subject string) []byte {
	body, _ := json.Marshal(FunctionAuthorityRequest{Action: "assign", Function: "example-record-producer", Domain: "example-records", Scope: "function-authority-test-chain", Subject: subject, ExpectedRevision: 4, ExpiresAt: 2000})
	return body
}

func functionAuthorityRESTRequest(t *testing.T, key ed25519.PrivateKey, body []byte) *http.Request {
	t.Helper()
	req := signedRequestAs(t, key, appV23ElevationTestID(key), http.MethodPost, "/v1/access/function-authority", body)
	nonce := make([]byte, 32)
	_, err := rand.Read(nonce)
	require.NoError(t, err)
	stamp, err := strconv.ParseInt(req.Header.Get("X-Timestamp"), 10, 64)
	require.NoError(t, err)
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(key, req.Method, req.URL.RequestURI(), body, stamp, nonce)))
	req.Host, req.RemoteAddr = "127.0.0.1:8080", "127.0.0.1:12345"
	return req
}

func TestFunctionAuthorityRESTSignedRootUsesNormalConsensusSubmission(t *testing.T) {
	srv, rootKey, memberKey, calls, parsed := setupFunctionAuthorityREST(t, 0)
	body := functionAuthorityRESTBody(appV23ElevationTestID(memberKey))
	recorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, rootKey, body))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, int32(1), calls.Load())
	wire := <-parsed
	require.Equal(t, tx.TxTypeFunctionAuthority, wire.Type)
	require.Equal(t, rootKey.Public().(ed25519.PublicKey), ed25519.PublicKey(wire.AgentPubKey))
	require.Equal(t, "POST /v1/access/function-authority\n"+string(body), string(wire.AgentRequest))
	require.Nil(t, wire.LocalElevation, "Root control must not acquire a promoted Admin proof")
	var wanted FunctionAuthorityRequest
	require.NoError(t, json.Unmarshal(body, &wanted))
	require.Equal(t, wanted, *wire.FunctionAuthority)
	var result map[string]string
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &result))
	require.Equal(t, "committed", result["status"])
	assertStrictTxHash(t, result["tx_hash"])
}

func TestFunctionAuthorityRESTRefusesBeforeBroadcast(t *testing.T) {
	srv, rootKey, memberKey, calls, _ := setupFunctionAuthorityREST(t, 0)
	body := functionAuthorityRESTBody(appV23ElevationTestID(memberKey))
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*http.Request)
	}{
		{"ordinary member", http.StatusForbidden, func(r *http.Request) { *r = *functionAuthorityRESTRequest(t, memberKey, body) }},
		{"remote peer", http.StatusForbidden, func(r *http.Request) { r.RemoteAddr = "192.0.2.1:1234" }},
		{"remote host", http.StatusForbidden, func(r *http.Request) { r.Host = "node.example" }},
		{"forwarded remote", http.StatusForbidden, func(r *http.Request) { r.Header.Set("X-Forwarded-For", "192.0.2.1") }},
		{"unsigned", http.StatusUnauthorized, func(r *http.Request) { r.Header.Del("X-Signature") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := functionAuthorityRESTRequest(t, rootKey, body)
			tc.mutate(req)
			recorder := httptest.NewRecorder()
			srv.Router().ServeHTTP(recorder, req)
			require.Equal(t, tc.status, recorder.Code, recorder.Body.String())
			require.Zero(t, calls.Load())
		})
	}
	srv.SetPostV31ForNextTxAccessor(func() bool { return false })
	recorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, rootKey, body))
	require.Equal(t, http.StatusConflict, recorder.Code, recorder.Body.String())
	require.Zero(t, calls.Load())
}

func TestFunctionAuthorityRESTWrongScopeRefusesBeforeBroadcast(t *testing.T) {
	srv, rootKey, memberKey, calls, _ := setupFunctionAuthorityREST(t, 0)
	body := strings.Replace(string(functionAuthorityRESTBody(appV23ElevationTestID(memberKey))), "function-authority-test-chain", "another-chain", 1)
	recorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, rootKey, []byte(body)))
	require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
	require.Zero(t, calls.Load())
}

func TestFunctionAuthorityRESTCurrentRootNotPrincipalOrAdmin(t *testing.T) {
	srv, oldRoot, memberKey, calls, parsed := setupFunctionAuthorityREST(t, 0)
	newRoot := appV23ElevationTestKey("function-authority-rotated-root")
	require.NoError(t, srv.badgerStore.RotateAppV23RootCredential(1, appV23ElevationTestID(newRoot), 2))
	promoteAppV23ElevationTestAdmin(t, srv.badgerStore, appV23ElevationTestID(oldRoot), appV23ElevationTestID(memberKey), 3)
	body := functionAuthorityRESTBody(appV23ElevationTestID(memberKey))
	for _, key := range []ed25519.PrivateKey{oldRoot, memberKey} {
		recorder := httptest.NewRecorder()
		srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, key, body))
		require.Equal(t, http.StatusForbidden, recorder.Code, recorder.Body.String())
		require.Zero(t, calls.Load())
	}
	recorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, newRoot, body))
	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Equal(t, int32(1), calls.Load())
	require.Equal(t, newRoot.Public().(ed25519.PublicKey), ed25519.PublicKey((<-parsed).AgentPubKey))
}

func TestFunctionAuthorityRESTDoesNotReportRejectedConsensusAsCommitted(t *testing.T) {
	srv, rootKey, memberKey, calls, _ := setupFunctionAuthorityREST(t, 11)
	recorder := httptest.NewRecorder()
	srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, rootKey, functionAuthorityRESTBody(appV23ElevationTestID(memberKey))))
	require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
	require.Equal(t, int32(1), calls.Load())
	require.NotContains(t, recorder.Body.String(), `"status":"committed"`)
}

func TestFunctionAuthorityRESTClosedJSONBeforeBroadcast(t *testing.T) {
	srv, rootKey, memberKey, calls, _ := setupFunctionAuthorityREST(t, 0)
	valid := string(functionAuthorityRESTBody(appV23ElevationTestID(memberKey)))
	for name, body := range map[string]string{
		"unknown":       strings.TrimSuffix(valid, "}") + `,"role":"admin"}`,
		"duplicate":     strings.TrimSuffix(valid, "}") + `,"action":"revoke"}`,
		"case alias":    strings.Replace(valid, `"action"`, `"Action"`, 1),
		"missing":       strings.Replace(valid, `"activation_height":0,`, "", 1),
		"missing scope": strings.Replace(valid, `"scope":"function-authority-test-chain",`, "", 1),
		"null scope":    strings.Replace(valid, `"scope":"function-authority-test-chain"`, `"scope":null`, 1),
		"null":          strings.Replace(valid, `"expires_at":2000`, `"expires_at":null`, 1),
		"wrong type":    strings.Replace(valid, `"expected_revision":4`, `"expected_revision":"4"`, 1),
		"fraction":      strings.Replace(valid, `"expected_revision":4`, `"expected_revision":4.5`, 1),
		"overflow":      strings.Replace(valid, `"expected_revision":4`, `"expected_revision":18446744073709551616`, 1),
		"trailing":      valid + `{}`,
		"array":         `[]`,
		"oversized":     strings.Repeat(" ", 8193),
	} {
		t.Run(name, func(t *testing.T) {
			recorder := httptest.NewRecorder()
			srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, rootKey, []byte(body)))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Zero(t, calls.Load())
		})
	}
}
