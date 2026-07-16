package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var dashboardTestGovernanceDomain = strings.Repeat("5a", sha256.Size)

func governanceCaptureServer(t *testing.T) (*httptest.Server, *atomic.Int32, <-chan *tx.ParsedTx) {
	return governanceCaptureServerResult(t, 0, 0)
}

func governanceCaptureServerResult(t *testing.T, checkCode, finalizeCode int) (*httptest.Server, *atomic.Int32, <-chan *tx.ParsedTx) {
	t.Helper()
	var broadcasts atomic.Int32
	captured := make(chan *tx.ParsedTx, 4)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcasts.Add(1)
		raw, err := hex.DecodeString(r.URL.Query().Get("tx")[2:])
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		captured <- parsed
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"check_tx":  map[string]any{"code": checkCode, "log": "check rejected"},
				"tx_result": map[string]any{"code": finalizeCode, "log": "finalize rejected"},
				"hash":      "TXHASH",
				"height":    "9",
			},
		}))
	}))
	return server, &broadcasts, captured
}

func dashboardGovernanceHandler(t *testing.T, rpc string) (*DashboardHandler, ed25519.PrivateKey, ed25519.PrivateKey) {
	t.Helper()
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, operatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	h := &DashboardHandler{
		CometBFTRPC:         rpc,
		SigningKey:          validatorKey,
		AdminSigningKey:     operatorKey,
		AppV20ActiveFn:      func() bool { return true },
		GovernanceDomainFn:  func() string { return dashboardTestGovernanceDomain },
		NodeOperatorAgentID: auth.PublicKeyToAgentID(operatorKey.Public().(ed25519.PublicKey)),
	}
	return h, validatorKey, operatorKey
}

func requireDashboardGovernanceProofBody(t *testing.T, parsed *tx.ParsedTx, method, path string) map[string]any {
	t.Helper()
	prefix := []byte(method + " " + path + "\n")
	require.True(t, bytes.HasPrefix(parsed.AgentRequest, prefix), string(parsed.AgentRequest))
	var body map[string]any
	require.NoError(t, json.Unmarshal(parsed.AgentRequest[len(prefix):], &body))
	return body
}

func markLocalDashboardRequest(req *http.Request) {
	req.RemoteAddr = "127.0.0.1:41000"
	req.Host = "localhost:8080"
	req.Header.Set("Sec-Fetch-Site", "same-origin")
}

func TestDashboardGovernanceRejectsAuthenticatedNonOperator(t *testing.T) {
	comet, broadcasts, _ := governanceCaptureServer(t)
	defer comet.Close()
	h, _, _ := dashboardGovernanceHandler(t, comet.URL)

	body := []byte(`{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/governance/propose", bytes.NewReader(body))
	markLocalDashboardRequest(req)
	req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, "authenticated-agent"))
	resp := httptest.NewRecorder()
	h.handleDashboardGovPropose(resp, req)

	assert.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
	assert.Zero(t, broadcasts.Load())
}

func TestDashboardScopeGovernanceUsesValidatorOuterAndOperatorProof(t *testing.T) {
	comet, broadcasts, captured := governanceCaptureServer(t)
	defer comet.Close()
	h, validatorKey, operatorKey := dashboardGovernanceHandler(t, comet.URL)

	body := []byte(`{"operation":"scope_action","target_id":"scope-a","reason":"form scope","payload":"AQ=="}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/governance/propose", bytes.NewReader(body))
	markLocalDashboardRequest(req)
	resp := httptest.NewRecorder()
	h.handleDashboardGovPropose(resp, req)

	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	require.EqualValues(t, 1, broadcasts.Load())
	parsed := <-captured
	assert.Equal(t, validatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.PublicKey))
	assert.Equal(t, operatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.AgentPubKey))
	proofBody := requireDashboardGovernanceProofBody(t, parsed, http.MethodPost, "/v1/dashboard/governance/propose")
	assert.Equal(t, auth.PublicKeyToAgentID(validatorKey.Public().(ed25519.PublicKey)), proofBody["validator_id"])
	assert.Equal(t, dashboardTestGovernanceDomain, proofBody["governance_domain"])
	assert.Equal(t, "scope_action", proofBody["operation"])
	assert.Len(t, parsed.AgentNonce, 8)
	wantHash := sha256.Sum256(parsed.AgentRequest)
	assert.Equal(t, wantHash[:], parsed.AgentBodyHash)
	verifiedID, err := auth.VerifyAgentProof(parsed.AgentPubKey, parsed.AgentSig, parsed.AgentBodyHash, parsed.AgentTimestamp, parsed.AgentNonce)
	require.NoError(t, err)
	assert.Equal(t, auth.PublicKeyToAgentID(operatorKey.Public().(ed25519.PublicKey)), verifiedID)
}

func TestDashboardNonScopeProposalUsesValidatorOuterAndOperatorProofPostV20(t *testing.T) {
	comet, _, captured := governanceCaptureServer(t)
	defer comet.Close()
	h, validatorKey, operatorKey := dashboardGovernanceHandler(t, comet.URL)

	body := []byte(`{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/governance/propose", bytes.NewReader(body))
	markLocalDashboardRequest(req)
	resp := httptest.NewRecorder()
	h.handleDashboardGovPropose(resp, req)

	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	parsed := <-captured
	assert.Equal(t, validatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.PublicKey))
	assert.Equal(t, operatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.AgentPubKey))
	proofBody := requireDashboardGovernanceProofBody(t, parsed, http.MethodPost, "/v1/dashboard/governance/propose")
	assert.Equal(t, auth.PublicKeyToAgentID(validatorKey.Public().(ed25519.PublicKey)), proofBody["validator_id"])
	assert.Equal(t, dashboardTestGovernanceDomain, proofBody["governance_domain"])
	assert.Equal(t, "remove_validator", proofBody["operation"])

	var response map[string]string
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &response))
	wantID := governance.ComputeProposalID(
		auth.PublicKeyToAgentID(validatorKey.Public().(ed25519.PublicKey)),
		9,
		governance.ProposalOp(tx.GovOpRemoveValidator),
		"validator-a",
	)
	assert.Equal(t, wantID, response["proposal_id"])
	assert.Equal(t, "TXHASH", response["tx_hash"])
	assert.Equal(t, "unknown", response["status"])
}

func TestDashboardNonScopeProposalKeepsLegacyAdminOuterBeforeAppV20(t *testing.T) {
	comet, _, captured := governanceCaptureServer(t)
	defer comet.Close()
	h, _, operatorKey := dashboardGovernanceHandler(t, comet.URL)
	h.AppV20ActiveFn = func() bool { return false }

	body := []byte(`{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/governance/propose", bytes.NewReader(body))
	markLocalDashboardRequest(req)
	resp := httptest.NewRecorder()
	h.handleDashboardGovPropose(resp, req)

	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	parsed := <-captured
	assert.Equal(t, operatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.PublicKey))
	assert.Equal(t, operatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.AgentPubKey))
	assert.Empty(t, parsed.AgentRequest, "pre-app-v20 direct same-key governance remains wire compatible")
}

func TestDashboardVoteUsesValidatorOuterAndOperatorProofPostV20(t *testing.T) {
	comet, _, captured := governanceCaptureServer(t)
	defer comet.Close()
	h, validatorKey, operatorKey := dashboardGovernanceHandler(t, comet.URL)

	body := []byte(`{"proposal_id":"proposal-a","decision":"accept"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/governance/vote", bytes.NewReader(body))
	markLocalDashboardRequest(req)
	resp := httptest.NewRecorder()
	h.handleDashboardGovVote(resp, req)

	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	parsed := <-captured
	assert.Equal(t, validatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.PublicKey))
	assert.Equal(t, operatorKey.Public().(ed25519.PublicKey), ed25519.PublicKey(parsed.AgentPubKey))
	proofBody := requireDashboardGovernanceProofBody(t, parsed, http.MethodPost, "/v1/dashboard/governance/vote")
	assert.Equal(t, auth.PublicKeyToAgentID(validatorKey.Public().(ed25519.PublicKey)), proofBody["validator_id"])
	assert.Equal(t, dashboardTestGovernanceDomain, proofBody["governance_domain"])
	assert.Equal(t, "proposal-a", proofBody["proposal_id"])

	var response map[string]string
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &response))
	assert.Equal(t, "TXHASH", response["tx_hash"])
	assert.Equal(t, "recorded", response["status"])
}

func TestDashboardGovernanceMissingDomainFailsClosedBeforeBroadcast(t *testing.T) {
	comet, broadcasts, _ := governanceCaptureServer(t)
	defer comet.Close()
	h, _, _ := dashboardGovernanceHandler(t, comet.URL)
	h.GovernanceDomainFn = nil

	body := []byte(`{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/governance/propose", bytes.NewReader(body))
	markLocalDashboardRequest(req)
	resp := httptest.NewRecorder()
	h.handleDashboardGovPropose(resp, req)

	assert.Equal(t, http.StatusServiceUnavailable, resp.Code, resp.Body.String())
	assert.Contains(t, resp.Body.String(), "chain domain not configured")
	assert.Zero(t, broadcasts.Load())
}

func TestDashboardGovernanceCommitFailuresReturnBadGatewayWithoutGhostEvent(t *testing.T) {
	tests := []struct {
		name         string
		checkCode    int
		finalizeCode int
		path         string
		body         string
		invoke       func(*DashboardHandler, http.ResponseWriter, *http.Request)
	}{
		{
			name:      "CheckTx proposal rejection",
			checkCode: 7,
			path:      "/v1/dashboard/governance/propose",
			body:      `{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`,
			invoke:    (*DashboardHandler).handleDashboardGovPropose,
		},
		{
			name:         "FinalizeBlock vote rejection",
			finalizeCode: 8,
			path:         "/v1/dashboard/governance/vote",
			body:         `{"proposal_id":"proposal-a","decision":"accept"}`,
			invoke:       (*DashboardHandler).handleDashboardGovVote,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			comet, broadcasts, _ := governanceCaptureServerResult(t, tc.checkCode, tc.finalizeCode)
			defer comet.Close()
			h, _, _ := dashboardGovernanceHandler(t, comet.URL)
			h.SSE = NewSSEBroadcaster()
			events := h.SSE.Subscribe()
			require.NotNil(t, events)
			defer h.SSE.Unsubscribe(events)

			req := httptest.NewRequest(http.MethodPost, tc.path, strings.NewReader(tc.body))
			markLocalDashboardRequest(req)
			resp := httptest.NewRecorder()
			tc.invoke(h, resp, req)

			assert.Equal(t, http.StatusBadGateway, resp.Code, resp.Body.String())
			assert.EqualValues(t, 1, broadcasts.Load())
			select {
			case event := <-events:
				t.Fatalf("rejected governance mutation emitted ghost SSE event: %s", event)
			default:
			}
		})
	}
}
