package abci

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/embedding"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

type restABCICommittedTx struct {
	raw    []byte
	parsed *tx.ParsedTx
	height int64
	result *abcitypes.ExecTxResult
}

func signedRESTGovernanceRequest(
	t *testing.T,
	operator agentKey,
	method, path string,
	body []byte,
	nonceByte byte,
) *http.Request {
	t.Helper()
	nonce := []byte{0, 0, 0, 0, 0, 0, 0, nonceByte}
	timestamp := time.Now().Unix()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", operator.id)
	req.Header.Set("X-Timestamp", strconv.FormatInt(timestamp, 10))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(
		auth.SignRequestWithNonce(operator.priv, method, path, body, timestamp, nonce),
	))
	return req
}

func TestAppV20RESTGovernanceCommitsThroughRealABCI(t *testing.T) {
	app := setupTestApp(t)
	operator := newAgentKey(t)
	outerValidator := newAgentKey(t)
	witnessValidator := newAgentKey(t)
	candidateValidator := newAgentKey(t)
	registerAgent(t, app, operator, "governance-operator", "admin")
	registerAgent(t, app, outerValidator, "validator-gateway", "member")
	registerAgent(t, app, witnessValidator, "validator-witness", "member")

	for _, key := range []agentKey{outerValidator, witnessValidator} {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: key.id, PublicKey: key.pub, Power: 10,
		}))
	}
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{
		outerValidator.id:   10,
		witnessValidator.id: 10,
	}))

	const chainID = "sage-rest-abci-v20-integration"
	domain, err := governance.DelegationDomainForChainID(chainID)
	require.NoError(t, err)
	require.NoError(t, app.ensureGovernanceDelegationDomain(domain))
	app.appV20AppliedHeight = 1
	app.state.Height = 1

	var (
		committedMu sync.Mutex
		committed   []restABCICommittedTx
		nextHeight  int64 = 2
	)
	cometShim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/broadcast_tx_commit" {
			http.Error(w, "unexpected CometBFT route", http.StatusNotFound)
			return
		}
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		if decodeErr != nil {
			t.Errorf("decode broadcast tx hex: %v", decodeErr)
			http.Error(w, "bad transaction hex", http.StatusBadRequest)
			return
		}
		parsed, decodeErr := tx.DecodeTx(raw)
		if decodeErr != nil {
			t.Errorf("decode broadcast transaction: %v", decodeErr)
			http.Error(w, "bad transaction", http.StatusBadRequest)
			return
		}

		check, checkErr := app.CheckTx(r.Context(), &abcitypes.RequestCheckTx{Tx: raw})
		if checkErr != nil {
			t.Errorf("real CheckTx: %v", checkErr)
			http.Error(w, "CheckTx failed", http.StatusInternalServerError)
			return
		}

		height := nextHeight
		var execResult *abcitypes.ExecTxResult
		if check.Code == 0 {
			blockTime := time.Unix(parsed.AgentTimestamp, 0).UTC()
			finalized, finalizeErr := app.FinalizeBlock(r.Context(), &abcitypes.RequestFinalizeBlock{
				Height: height,
				Time:   blockTime,
				Txs:    [][]byte{raw},
			})
			if finalizeErr != nil {
				t.Errorf("real FinalizeBlock at height %d: %v", height, finalizeErr)
				http.Error(w, "FinalizeBlock failed", http.StatusInternalServerError)
				return
			}
			if len(finalized.TxResults) != 1 {
				t.Errorf("FinalizeBlock returned %d tx results, want 1", len(finalized.TxResults))
				http.Error(w, "missing transaction result", http.StatusInternalServerError)
				return
			}
			execResult = finalized.TxResults[0]
			if _, commitErr := app.Commit(context.Background(), &abcitypes.RequestCommit{}); commitErr != nil {
				t.Errorf("real Commit at height %d: %v", height, commitErr)
				http.Error(w, "Commit failed", http.StatusInternalServerError)
				return
			}
			nextHeight++
		} else {
			execResult = &abcitypes.ExecTxResult{Code: check.Code, Log: check.Log}
		}

		committedMu.Lock()
		committed = append(committed, restABCICommittedTx{
			raw: append([]byte(nil), raw...), parsed: parsed, height: height, result: execResult,
		})
		committedMu.Unlock()

		txHash := sha256.Sum256(raw)
		w.Header().Set("Content-Type", "application/json")
		if encodeErr := json.NewEncoder(w).Encode(map[string]any{
			"jsonrpc": "2.0",
			"id":      -1,
			"result": map[string]any{
				"check_tx": map[string]any{"code": check.Code, "log": check.Log},
				"tx_result": map[string]any{
					"code": execResult.Code,
					"log":  execResult.Log,
					"data": base64.StdEncoding.EncodeToString(execResult.Data),
				},
				"hash":   strings.ToUpper(hex.EncodeToString(txHash[:])),
				"height": strconv.FormatInt(height, 10),
			},
		}); encodeErr != nil {
			t.Errorf("encode CometBFT response: %v", encodeErr)
		}
	}))
	t.Cleanup(cometShim.Close)

	health := metrics.NewHealthChecker()
	health.SetPostgresHealth(true)
	health.SetCometBFTHealth(true)
	restServer := rest.NewServer(
		cometShim.URL,
		app.GetOffchainStore(),
		app.GetOffchainStore(),
		app.GetBadgerStore(),
		health,
		zerolog.Nop(),
		embedding.NewHashProvider(8),
	)
	require.NoError(t, restServer.SetValidatorSigningKey(outerValidator.priv))
	require.NoError(t, restServer.SetGovernanceOperatorID(operator.id))
	restServer.SetPostV17ForNextTxAccessor(app.IsAppV17ActiveForNextTx)
	restServer.SetPostV20ForNextTxAccessor(app.IsAppV20ActiveForNextTx)
	restServer.SetGovernanceDomainAccessor(app.GovernanceDelegationDomain)

	proposeBody, err := json.Marshal(rest.GovProposeRequest{
		ValidatorID:      outerValidator.id,
		GovernanceDomain: domain,
		Operation:        "add_validator",
		TargetID:         candidateValidator.id,
		TargetPubkey:     candidateValidator.id,
		TargetPower:      5,
		Reason:           "stage a candidate for the integration proof",
	})
	require.NoError(t, err)
	proposeResponse := httptest.NewRecorder()
	restServer.Router().ServeHTTP(proposeResponse, signedRESTGovernanceRequest(
		t, operator, http.MethodPost, "/v1/governance/propose", proposeBody, 1,
	))
	require.Equal(t, http.StatusOK, proposeResponse.Code, proposeResponse.Body.String())

	var proposed rest.GovProposeResponse
	require.NoError(t, json.Unmarshal(proposeResponse.Body.Bytes(), &proposed))
	wantProposalID := governance.ComputeProposalID(
		outerValidator.id,
		2,
		governance.OpAddValidator,
		candidateValidator.id,
	)
	assert.Equal(t, wantProposalID, proposed.ProposalID)
	assert.Equal(t, string(governance.StatusVoting), proposed.Status)
	assert.NotEmpty(t, proposed.TxHash)

	cancelBody, err := json.Marshal(rest.GovCancelRequest{
		ValidatorID:      outerValidator.id,
		GovernanceDomain: domain,
		ProposalID:       proposed.ProposalID,
	})
	require.NoError(t, err)
	cancelResponse := httptest.NewRecorder()
	restServer.Router().ServeHTTP(cancelResponse, signedRESTGovernanceRequest(
		t, operator, http.MethodPost, "/v1/governance/cancel", cancelBody, 2,
	))
	require.Equal(t, http.StatusOK, cancelResponse.Code, cancelResponse.Body.String())

	var cancelled rest.GovCancelResponse
	require.NoError(t, json.Unmarshal(cancelResponse.Body.Bytes(), &cancelled))
	assert.Equal(t, "cancelled", cancelled.Status)
	proposal, err := app.govEngine.LoadProposal(proposed.ProposalID)
	require.NoError(t, err)
	assert.Equal(t, governance.StatusCancelled, proposal.Status)
	assert.Equal(t, outerValidator.id, proposal.ProposerID)

	committedMu.Lock()
	gotCommitted := append([]restABCICommittedTx(nil), committed...)
	committedMu.Unlock()
	require.Len(t, gotCommitted, 2)
	for index, delivered := range gotCommitted {
		require.Zero(t, delivered.result.Code, delivered.result.Log)
		assert.Equal(t, int64(index+2), delivered.height)
		assert.Equal(t, outerValidator.pub, ed25519.PublicKey(delivered.parsed.PublicKey), "outer transaction principal must be the validator")
		assert.Equal(t, operator.pub, ed25519.PublicKey(delivered.parsed.AgentPubKey), "embedded request authorizer must be the separate operator")
		assert.NotEqual(t, delivered.parsed.PublicKey, delivered.parsed.AgentPubKey)
		valid, verifyErr := tx.VerifyTx(delivered.parsed)
		require.NoError(t, verifyErr)
		assert.True(t, valid)
		canonical, encodeErr := tx.EncodeTx(delivered.parsed)
		require.NoError(t, encodeErr)
		assert.Equal(t, delivered.raw, canonical)
	}

	for _, proof := range []struct {
		parsed *tx.ParsedTx
		path   string
		body   []byte
	}{
		{parsed: gotCommitted[0].parsed, path: "/v1/governance/propose", body: proposeBody},
		{parsed: gotCommitted[1].parsed, path: "/v1/governance/cancel", body: cancelBody},
	} {
		assert.Equal(t, append([]byte("POST "+proof.path+"\n"), proof.body...), proof.parsed.AgentRequest)
		require.Len(t, proof.parsed.AgentNonce, 8)
		assert.True(t, auth.VerifyRequestWithNonce(
			operator.pub,
			http.MethodPost,
			proof.path,
			proof.body,
			proof.parsed.AgentTimestamp,
			proof.parsed.AgentNonce,
			proof.parsed.AgentSig,
		))
		var boundContext struct {
			ValidatorID      string `json:"validator_id"`
			GovernanceDomain string `json:"governance_domain"`
		}
		require.NoError(t, json.Unmarshal(proof.body, &boundContext))
		assert.Equal(t, outerValidator.id, boundContext.ValidatorID)
		assert.Equal(t, domain, boundContext.GovernanceDomain)
	}

	assert.Equal(t, fmt.Sprintf("proposal created: %s", proposed.ProposalID), gotCommitted[0].result.Log)
	assert.Equal(t, "proposal cancelled", gotCommitted[1].result.Log)
}
