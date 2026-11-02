package abci

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

// These requests have genuine Root signatures but a distinct transport signer,
// as normal REST does. The malformed bodies deliberately have the same typed
// encoding under json.Unmarshal as the authorized action. Consensus must still
// reject them at its own closed request contract, independently of REST.
func TestFunctionAuthorityRootSignedRequestClosedContract(t *testing.T) {
	for _, kind := range []string{"valid", "whitespace", "unknown", "duplicate", "escaped-duplicate", "case-alias", "missing-zero", "null-string", "null-zero"} {
		t.Run(kind, func(t *testing.T) {
			f := newAuthorityTestFixture(t)
			action := tx.FunctionAuthority{Action: "activate", Function: authorityTestFunction, Domain: authorityTestDomain, Scope: "function-authority-fixture", ActivationHeight: 70}
			encoded, err := json.Marshal(action)
			require.NoError(t, err)
			body := string(encoded)
			switch kind {
			case "whitespace":
				body = " \n" + body + "\n"
			case "unknown":
				body = strings.TrimSuffix(body, "}") + `,"role":"admin"}`
			case "duplicate":
				body = strings.TrimSuffix(body, "}") + `,"action":"activate"}`
			case "escaped-duplicate":
				body = strings.TrimSuffix(body, "}") + `,"act\u0069on":"activate"}`
			case "case-alias":
				body = strings.Replace(body, `"action"`, `"Action"`, 1)
			case "missing-zero":
				body = strings.Replace(body, `"expected_revision":0,`, "", 1)
			case "null-string":
				body = strings.Replace(body, `"subject":""`, `"subject":null`, 1)
			case "null-zero":
				body = strings.Replace(body, `"expected_revision":0`, `"expected_revision":null`, 1)
			}
			var legacy tx.FunctionAuthority
			require.NoError(t, json.Unmarshal([]byte(body), &legacy))
			require.Equal(t, action, legacy, "control: permissive decoder cannot distinguish this body")

			now := time.Now().UTC().Truncate(time.Second)
			request := []byte("POST /v1/access/function-authority\n" + body)
			digest := sha256.Sum256(request)
			nonce := []byte("test0001")
			signature := auth.SignRequestWithNonce(f.root.priv, "POST", "/v1/access/function-authority", []byte(body), now.Unix(), nonce)
			require.True(t, auth.VerifyRequestWithNonce(f.root.pub, "POST", "/v1/access/function-authority", []byte(body), now.Unix(), nonce, signature))
			transport := newAgentKey(t)
			parsed := &tx.ParsedTx{Type: tx.TxTypeFunctionAuthority, FunctionAuthority: &action,
				Nonce: 1, Timestamp: now, AgentPubKey: f.root.pub, AgentSig: signature,
				AgentBodyHash: digest[:], AgentTimestamp: now.Unix(), AgentNonce: nonce, AgentRequest: request}
			require.NoError(t, tx.SignTx(parsed, transport.priv))
			raw, err := tx.EncodeTx(parsed)
			require.NoError(t, err)
			check, err := f.app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
			require.NoError(t, err)
			response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 68, Time: now, Txs: [][]byte{raw}})
			require.Len(t, response.TxResults, 1)
			policy, err := f.app.badgerStore.GetFunctionAuthorityPolicy(action.Function, action.Domain)
			require.NoError(t, err)
			if kind == "valid" || kind == "whitespace" {
				require.Zero(t, check.Code, check.Log)
				require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
				require.NotNil(t, policy)
				require.Equal(t, f.root.id, policy.CreatedBy)
			} else {
				require.Equal(t, uint32(109), check.Code, check.Log)
				require.Equal(t, uint32(109), response.TxResults[0].Code, response.TxResults[0].Log)
				require.Contains(t, response.TxResults[0].Log, "delegated agent action mismatch")
				require.Nil(t, policy, "ambiguous Root-signed request must not create policy")
			}
		})
	}
}
