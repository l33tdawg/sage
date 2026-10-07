package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// These tests pin the OPERATOR ENTRY POINTS for the fence-abandon route, which
// a field report found to have none. The route was gated on operator authority
// and the gate accepts the configured node-operator identity, an encrypted
// dashboard session, or the same-origin loopback dashboard — but the report's
// author could present none of them: they were an ordinary local agent, and the
// one credential the CLI advertises (an HTTP MCP bearer token) is not consulted
// by this gate at all. The result was a 403 whose text described three entries
// without saying which identity they are about, on a node whose only exit was
// that route.

// TestAbandonGateAdmitsTheLocalDashboardOrigin pins the entry an operator
// actually has on an unencrypted personal node: the same-origin CEREBRUM tab on
// this machine. A fetch from that page — or curl run on the node host WITH the
// matching Origin — is the documented local operator surface, and it must reach
// the route.
func TestAbandonGateAdmitsTheLocalDashboardOrigin(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	handler := appV23AccessTestHandler(fixture, "", nil)

	reached := false
	gate := handler.cerebrumOperatorGate(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		reached = true
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/signer-fence/abandon", nil)
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	recorder := httptest.NewRecorder()
	gate.ServeHTTP(recorder, req)

	require.True(t, reached,
		"a same-origin request from the local CEREBRUM dashboard must reach the operator route; got %d %s",
		recorder.Code, recorder.Body.String())
}

// TestAbandonGateRefusesAnUnsignedCLIRequest is the other half: a plain request
// from the node host that presents neither the browser origin nor an operator
// identity is not the operator, and it must be refused with a reason that says
// WHICH identity would be accepted. Every 403 from this gate that merely
// describes three entries is a support ticket, because the caller cannot tell
// whether the problem is their credential or the route.
func TestAbandonGateRefusesAnUnsignedCLIRequest(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	handler := appV23AccessTestHandler(fixture, "", nil)

	gate := handler.cerebrumOperatorGate(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		t.Fatal("an unsigned CLI-shaped request must not reach the operator route")
	}))

	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/signer-fence/abandon", nil)
	req.Host = "127.0.0.1:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	recorder := httptest.NewRecorder()
	gate.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusForbidden, recorder.Code)
}

// TestAbandonRouteLiftsARestoredFenceForTheLocalDashboard is the end-to-end
// operator action: a restored fence on an unencrypted local node, abandoned by
// the same-origin dashboard request, with the decision recorded and the record
// retired. It is what an operator does in the browser console when the UI has
// no control for it, and it must work without a token, a vault session, or any
// credential beyond being the local dashboard.
func TestAbandonRouteLiftsARestoredFenceForTheLocalDashboard(t *testing.T) {
	handler, store, signerHex, rpcURL := restoredFenceFixtureForWebTest(t)
	handler.Encrypted.Store(false)
	handler.CometBFTRPC = rpcURL

	body, err := json.Marshal(map[string]any{
		"signer": signerHex, "reason": "operator clearing a fence on an idle chain",
		"acknowledge_payload_loss": true,
	})
	require.NoError(t, err)

	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/signer-fence/abandon", bytes.NewReader(body))
	req.Host = "localhost:8080"
	req.RemoteAddr = "127.0.0.1:54321"
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://localhost:8080")
	req.Header.Set("Sec-Fetch-Site", "same-origin")
	recorder := httptest.NewRecorder()

	gate := handler.cerebrumOperatorGate(http.HandlerFunc(handler.handleSignerFenceAbandon))
	gate.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code, recorder.Body.String())
	require.Empty(t, tx.FencedSigners(), "the operator's decision must lift the fence")
	require.False(t, store.held(signerHex), "and retire the durable record")
}

// restoredFenceFixtureForWebTest raises one restored fence and returns a
// handler whose operator gate can be exercised, plus the intent store it uses.
func restoredFenceFixtureForWebTest(t *testing.T) (*DashboardHandler, *abandonIntentStore, string, string) {
	t.Helper()
	tx.SetFenceProverFunc(nil)
	tx.SetFenceAutoResolverFunc(nil)
	store := newAbandonIntentStore()
	tx.SetFenceIntentStore(store)

	_, sk, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pub, ok := sk.Public().(ed25519.PublicKey)
	require.True(t, ok)
	signerHex := hex.EncodeToString(pub)
	require.NoError(t, store.SaveFenceIntent(context.Background(), tx.FenceIntent{
		SignerPubKeyHex: signerHex,
		TxHash:          strings.Repeat("AB", 32),
		Nonce:           41,
		HasNonce:        true,
	}))
	restored, err := tx.RestoreFencesFromIntents(context.Background())
	require.NoError(t, err)
	require.Equal(t, 1, restored)

	peers := &atomic.Int32{}
	rpc := abandonRPC(t, peers)
	t.Cleanup(func() {
		tx.SetFenceAutoResolverFunc(nil)
		tx.SetFenceProverFunc(nil)
		for _, held := range tx.FencedSigners() {
			_ = tx.LiftFenceWithProof(context.Background(), held.SignerPubKeyHex, tx.FenceLiftProof{
				Kind: "committed", TxHash: held.TxHash, Detail: "operator-path test teardown",
			})
		}
		tx.SetFenceIntentStore(nil)
	})
	return &DashboardHandler{}, store, signerHex, rpc.URL
}
