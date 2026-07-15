package web

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestChainValidatorsReturnsCanonicalScopeIdentity(t *testing.T) {
	pubKey := bytes.Repeat([]byte{0x42}, 32)
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "/validators", r.URL.Path)
		assert.Equal(t, "100", r.URL.Query().Get("per_page"))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"total": "1",
				"validators": []map[string]any{{
					"address": "COMET-ADDRESS",
					"pub_key": map[string]string{
						"type":  "tendermint/PubKeyEd25519",
						"value": base64.StdEncoding.EncodeToString(pubKey),
					},
					"voting_power":      "7",
					"proposer_priority": "0",
				}},
			},
		})
	}))
	defer rpc.Close()

	handler := &DashboardHandler{CometBFTRPC: rpc.URL}
	rr := httptest.NewRecorder()
	handler.handleChainValidators(rr, httptest.NewRequest(http.MethodGet, "/v1/dashboard/chain/validators", nil))
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	var body struct {
		Count      int `json:"count"`
		Validators []struct {
			Address string `json:"address"`
			AgentID string `json:"agent_id"`
			PubKey  struct {
				Value string `json:"value"`
			} `json:"pub_key"`
		} `json:"validators"`
	}
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &body))
	require.Equal(t, 1, body.Count)
	require.Len(t, body.Validators, 1)
	assert.Equal(t, "COMET-ADDRESS", body.Validators[0].Address)
	assert.Equal(t, hex.EncodeToString(pubKey), body.Validators[0].AgentID)
	assert.Equal(t, base64.StdEncoding.EncodeToString(pubKey), body.Validators[0].PubKey.Value)
}
