package rest

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	restmiddleware "github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func configureTestGovernanceGateway(t *testing.T, srv *Server, operatorID string) ed25519.PrivateKey {
	t.Helper()
	_, validatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	require.NoError(t, srv.SetValidatorSigningKey(validatorKey))
	require.NoError(t, srv.SetGovernanceOperatorID(operatorID))
	return validatorKey
}

func signedGovernanceRequestAs(t *testing.T, priv ed25519.PrivateKey, callerID, method, path string, body []byte) *http.Request {
	t.Helper()
	req := signedRequestAs(t, priv, callerID, method, path, body)
	timestamp, err := strconv.ParseInt(req.Header.Get("X-Timestamp"), 10, 64)
	require.NoError(t, err)
	nonce := make([]byte, 8)
	_, err = rand.Read(nonce)
	require.NoError(t, err)
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(priv, method, path, body, timestamp, nonce)))
	return req
}

// TestParseGovOp_MemoryDomainRepair guards that the app-v16 domain-repair op is
// reachable through the REST governance-propose surface. The final fresh-eyes
// review found the op fully implemented in consensus but unmappable end-to-end:
// parseGovOp 400'd "memory_domain_repair", so no client could ever create the
// proposal and the headline v11.2 remediation was unreachable. This pins the
// mapping (and that it numerically matches governance.OpMemoryDomainRepair = 6).
func TestParseGovOp_MemoryDomainRepair(t *testing.T) {
	op, err := parseGovOp("memory_domain_repair")
	require.NoError(t, err)
	assert.Equal(t, tx.GovOpMemoryDomainRepair, op)
	assert.Equal(t, uint8(6), uint8(op), "must match governance.OpMemoryDomainRepair = 6")
}

func TestResolveGovProposalPayloadBuildsCanonicalScope(t *testing.T) {
	template := &scope.ProposalTemplate{
		ScopeID: "scope-a", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a",
		Domains:               []string{"research.private", "research"},
		Members: []scope.ProposalMember{
			{ValidatorID: "validator-b", AssignedWeight: 1},
			{ValidatorID: "validator-a", AssignedWeight: 2},
		},
	}
	targetID, payload, err := resolveGovProposalPayload(tx.GovOpScopeAction, "", "", template)
	require.NoError(t, err)
	assert.Equal(t, "scope-a", targetID)
	record, err := scope.Decode(payload)
	require.NoError(t, err)
	assert.Equal(t, []scope.Domain{{Name: "research"}, {Name: "research.private"}}, record.Domains)
	assert.Zero(t, record.CreatedHeight)
	assert.Zero(t, record.UpdatedHeight)
}

func TestResolveGovProposalPayloadRejectsAmbiguousOrMismatchedScope(t *testing.T) {
	template := &scope.ProposalTemplate{
		ScopeID: "scope-a", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a", Domains: []string{"research"},
		Members: []scope.ProposalMember{{ValidatorID: "validator-a", AssignedWeight: 1}},
	}
	_, _, err := resolveGovProposalPayload(tx.GovOpScopeAction, "scope-a", base64.StdEncoding.EncodeToString([]byte("raw")), template)
	assert.ErrorContains(t, err, "mutually exclusive")
	_, _, err = resolveGovProposalPayload(tx.GovOpScopeAction, "scope-b", "", template)
	assert.ErrorContains(t, err, "does not match")
	_, _, err = resolveGovProposalPayload(tx.GovOpAddValidator, "validator-a", "", template)
	assert.ErrorContains(t, err, "only valid")
	_, _, err = resolveGovProposalPayload(tx.GovOpScopeAction, "scope-a", "", nil)
	assert.ErrorContains(t, err, "requires either")
}

func TestScopeActionConstructionFailsClosedBeforeAppV20(t *testing.T) {
	_, validatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	operatorPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	operatorID := auth.PublicKeyToAgentID(operatorPub)
	server := &Server{}
	require.NoError(t, server.SetValidatorSigningKey(validatorKey))
	require.NoError(t, server.SetGovernanceOperatorID(operatorID))
	req := httptest.NewRequest(http.MethodPost, "/v1/governance/propose", strings.NewReader(`{
		"operation":"scope_action","target_id":"scope-a","reason":"form scope","payload":"AQ=="
	}`))
	req = req.WithContext(restmiddleware.WithAgentID(req.Context(), operatorID))
	resp := httptest.NewRecorder()
	server.handleGovPropose(resp, req)
	assert.Equal(t, http.StatusConflict, resp.Code)
	assert.Contains(t, resp.Body.String(), "app-v20")
}

func TestGovernanceMutationsRejectNonOperatorWithoutBroadcast(t *testing.T) {
	var broadcasts atomic.Int32
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		broadcasts.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"UNEXPECTED","height":"1"}}`))
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	_, validatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	require.NoError(t, srv.SetValidatorSigningKey(validatorKey))
	require.NoError(t, srv.SetGovernanceOperatorID(strings.Repeat("0", 64)))

	tests := []struct {
		name string
		path string
		body string
	}{
		{name: "propose", path: "/v1/governance/propose", body: `{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`},
		{name: "vote", path: "/v1/governance/vote", body: `{"proposal_id":"proposal-a","decision":"accept"}`},
		{name: "cancel", path: "/v1/governance/cancel", body: `{"proposal_id":"proposal-a"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			req, _ := signedRequest(t, http.MethodPost, test.path, []byte(test.body))
			resp := httptest.NewRecorder()
			srv.Router().ServeHTTP(resp, req)
			assert.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
		})
	}
	assert.Zero(t, broadcasts.Load(), "an unauthorized signer must never reach CometBFT")
}

func TestGovernanceMutationFailsClosedWithoutOperatorConfiguration(t *testing.T) {
	var broadcasts atomic.Int32
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		broadcasts.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	_, validatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	require.NoError(t, srv.SetValidatorSigningKey(validatorKey))
	req, _ := signedRequest(t, http.MethodPost, "/v1/governance/vote", []byte(`{"proposal_id":"proposal-a","decision":"accept"}`))
	resp := httptest.NewRecorder()
	srv.Router().ServeHTTP(resp, req)

	assert.Equal(t, http.StatusServiceUnavailable, resp.Code, resp.Body.String())
	assert.Zero(t, broadcasts.Load(), "an unowned governance gateway must never broadcast")
}

func TestGovernanceMutationFailsClosedWithoutLiveValidatorKey(t *testing.T) {
	t.Setenv("VALIDATOR_KEY_FILE", "")
	var broadcasts atomic.Int32
	cometMock := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		broadcasts.Add(1)
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	req, operatorID := signedRequest(t, http.MethodPost, "/v1/governance/vote", []byte(`{"proposal_id":"proposal-a","decision":"accept"}`))
	require.NoError(t, srv.SetGovernanceOperatorID(operatorID))
	resp := httptest.NewRecorder()
	srv.Router().ServeHTTP(resp, req)

	assert.Equal(t, http.StatusServiceUnavailable, resp.Code, resp.Body.String())
	assert.Contains(t, resp.Body.String(), "validator signing key")
	assert.Zero(t, broadcasts.Load())
}

func TestGovernanceOperatorIdentityIsCanonicalAndAcceptsUppercaseHeader(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"TXHASH","height":"9"}}`))
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	operatorPub, operatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	operatorID := auth.PublicKeyToAgentID(operatorPub)
	configureTestGovernanceGateway(t, srv, strings.ToUpper(operatorID))
	assert.Equal(t, operatorID, srv.GovernanceOperatorID())

	body := []byte(`{"proposal_id":"proposal-a","decision":"accept"}`)
	req := signedGovernanceRequestAs(t, operatorKey, strings.ToUpper(operatorID), http.MethodPost, "/v1/governance/vote", body)
	resp := httptest.NewRecorder()
	srv.Router().ServeHTTP(resp, req)
	assert.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
}

func TestGovernanceOperatorReceivesCommittedProposalIDAndReusesIt(t *testing.T) {
	var broadcasts atomic.Int32
	var delivered []*tx.ParsedTx
	var badgerStore *store.BadgerStore
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		delivered = append(delivered, parsed)

		call := broadcasts.Add(1)
		if parsed.GovPropose != nil {
			validatorPub := ed25519.PublicKey(parsed.PublicKey)
			proposalID := governance.ComputeProposalID(
				auth.PublicKeyToAgentID(validatorPub),
				40+int64(call),
				governance.ProposalOp(parsed.GovPropose.Operation),
				parsed.GovPropose.TargetID,
			)
			state, marshalErr := json.Marshal(&governance.ProposalState{
				ProposalID: proposalID, Operation: governance.ProposalOp(parsed.GovPropose.Operation),
				TargetID: parsed.GovPropose.TargetID, ProposerID: auth.PublicKeyToAgentID(validatorPub),
				Status: governance.StatusExecuted, CreatedHeight: 40 + int64(call),
			})
			require.NoError(t, marshalErr)
			require.NoError(t, badgerStore.SetGovProposal(proposalID, state))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"check_tx":{"code":0,"log":""},"tx_result":{"code":0,"log":""},"hash":"TXHASH` + strconv.FormatInt(int64(call), 10) + `","height":"` + strconv.FormatInt(40+int64(call), 10) + `"}}`))
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	var openErr error
	badgerStore, openErr = store.NewBadgerStore(t.TempDir())
	require.NoError(t, openErr)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	srv.badgerStore = badgerStore
	operatorPub, operatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	operatorID := auth.PublicKeyToAgentID(operatorPub)
	_, validatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	require.NoError(t, srv.SetValidatorSigningKey(validatorKey))
	require.NoError(t, srv.SetGovernanceOperatorID(operatorID))
	srv.SetPostV17ForNextTxAccessor(func() bool { return true })
	srv.SetPostV20ForNextTxAccessor(func() bool { return true })
	domain := strings.Repeat("5a", 32)
	srv.SetGovernanceDomainAccessor(func() string { return domain })
	validatorID := auth.PublicKeyToAgentID(validatorKey.Public().(ed25519.PublicKey))

	proposeBody := []byte(`{"validator_id":"` + validatorID + `","governance_domain":"` + domain + `","operation":"remove_validator","target_id":"validator-a","reason":"test"}`)
	proposeReq := signedGovernanceRequestAs(t, operatorKey, operatorID, http.MethodPost, "/v1/governance/propose", proposeBody)
	proposeResp := httptest.NewRecorder()
	srv.Router().ServeHTTP(proposeResp, proposeReq)
	require.Equal(t, http.StatusOK, proposeResp.Code, proposeResp.Body.String())

	var proposed GovProposeResponse
	require.NoError(t, json.Unmarshal(proposeResp.Body.Bytes(), &proposed))
	validatorPub, ok := srv.signingKey.Public().(ed25519.PublicKey)
	require.True(t, ok)
	wantProposalID := governance.ComputeProposalID(
		auth.PublicKeyToAgentID(validatorPub),
		41,
		governance.OpRemoveValidator,
		"validator-a",
	)
	assert.Equal(t, wantProposalID, proposed.ProposalID)
	assert.Equal(t, "TXHASH1", proposed.TxHash)
	assert.NotEqual(t, proposed.TxHash, proposed.ProposalID)
	assert.Equal(t, string(governance.StatusExecuted), proposed.Status, "status must come from committed state, not a hard-coded voting value")

	voteBody, err := json.Marshal(GovVoteRequest{ValidatorID: validatorID, GovernanceDomain: domain, ProposalID: proposed.ProposalID, Decision: "accept"})
	require.NoError(t, err)
	voteReq := signedGovernanceRequestAs(t, operatorKey, operatorID, http.MethodPost, "/v1/governance/vote", voteBody)
	voteResp := httptest.NewRecorder()
	srv.Router().ServeHTTP(voteResp, voteReq)
	require.Equal(t, http.StatusOK, voteResp.Code, voteResp.Body.String())

	cancelBody, err := json.Marshal(GovCancelRequest{ValidatorID: validatorID, GovernanceDomain: domain, ProposalID: proposed.ProposalID})
	require.NoError(t, err)
	cancelReq := signedGovernanceRequestAs(t, operatorKey, operatorID, http.MethodPost, "/v1/governance/cancel", cancelBody)
	cancelResp := httptest.NewRecorder()
	srv.Router().ServeHTTP(cancelResp, cancelReq)
	require.Equal(t, http.StatusOK, cancelResp.Code, cancelResp.Body.String())

	require.Len(t, delivered, 3)
	assert.Equal(t, operatorPub, ed25519.PublicKey(delivered[0].AgentPubKey))
	assert.Equal(t, append([]byte("POST /v1/governance/propose\n"), proposeBody...), delivered[0].AgentRequest)
	assert.Len(t, delivered[0].AgentNonce, 8)
	require.NotNil(t, delivered[1].GovVote)
	assert.Equal(t, proposed.ProposalID, delivered[1].GovVote.ProposalID)
	require.NotNil(t, delivered[2].GovCancel)
	assert.Equal(t, proposed.ProposalID, delivered[2].GovCancel.ProposalID)
}

func TestGovernanceContextAndMismatchFailClosed(t *testing.T) {
	var broadcasts atomic.Int32
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		broadcasts.Add(1)
		_, _ = w.Write([]byte(`{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"TX","height":"1"}}`))
	}))
	defer cometMock.Close()
	srv, _, _ := newTestServer(t, cometMock.URL)
	operatorPub, operatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	operatorID := auth.PublicKeyToAgentID(operatorPub)
	validatorKey := configureTestGovernanceGateway(t, srv, operatorID)
	validatorID := auth.PublicKeyToAgentID(validatorKey.Public().(ed25519.PublicKey))
	badgerStore, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	require.NoError(t, badgerStore.SaveValidators(map[string]int64{validatorID: 10}))
	srv.badgerStore = badgerStore
	domain := strings.Repeat("6b", 32)
	srv.SetPostV20ForNextTxAccessor(func() bool { return true })
	srv.SetGovernanceDomainAccessor(func() string { return domain })

	contextReq := signedGovernanceRequestAs(t, operatorKey, operatorID, http.MethodGet, "/v1/governance/context", nil)
	contextResp := httptest.NewRecorder()
	srv.Router().ServeHTTP(contextResp, contextReq)
	require.Equal(t, http.StatusOK, contextResp.Code, contextResp.Body.String())
	var got GovernanceContextResponse
	require.NoError(t, json.Unmarshal(contextResp.Body.Bytes(), &got))
	assert.Equal(t, GovernanceContextResponse{
		ValidatorID: validatorID, GovernanceDomain: domain, AppV20Active: true,
		ValidatorActive:  true,
		ActiveValidators: []GovernanceActiveValidatorRef{{ValidatorID: validatorID, VotingPower: 10}},
	}, got)

	wrong := []byte(`{"validator_id":"` + validatorID + `","governance_domain":"` + strings.Repeat("7c", 32) + `","proposal_id":"p","decision":"accept"}`)
	wrongReq := signedGovernanceRequestAs(t, operatorKey, operatorID, http.MethodPost, "/v1/governance/vote", wrong)
	wrongResp := httptest.NewRecorder()
	srv.Router().ServeHTTP(wrongResp, wrongReq)
	assert.Equal(t, http.StatusConflict, wrongResp.Code, wrongResp.Body.String())
	assert.Zero(t, broadcasts.Load())
}

func TestPersistedGovernanceValidatorReadinessTracksExactMembership(t *testing.T) {
	srv, _, _ := newTestServer(t, "http://127.0.0.1:1")
	badgerStore, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	srv.badgerStore = badgerStore

	localPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	otherPub, _, err := auth.GenerateKeypair()
	require.NoError(t, err)
	localID := auth.PublicKeyToAgentID(localPub)
	otherID := auth.PublicKeyToAgentID(otherPub)
	require.NoError(t, badgerStore.SaveValidators(map[string]int64{localID: 10, otherID: 12}))

	active, validators, err := srv.persistedGovernanceValidatorReadiness(localID)
	require.NoError(t, err)
	assert.True(t, active)
	assert.ElementsMatch(t, []GovernanceActiveValidatorRef{
		{ValidatorID: localID, VotingPower: 10},
		{ValidatorID: otherID, VotingPower: 12},
	}, validators)

	// Post-app-v20 governance persists the complete roster via
	// ReplaceValidators; model a real removal rather than SaveValidators'
	// replay-compatible upsert behavior.
	require.NoError(t, badgerStore.ReplaceValidators(map[string]int64{otherID: 12}))
	active, validators, err = srv.persistedGovernanceValidatorReadiness(localID)
	require.NoError(t, err)
	assert.False(t, active)
	assert.Equal(t, []GovernanceActiveValidatorRef{{ValidatorID: otherID, VotingPower: 12}}, validators)
}

func TestCommittedGovernanceProposalMissingFromConfiguredStoreReturns500(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"COMMITTEDTX","height":"9"}}`))
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	badgerStore, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, badgerStore.CloseBadger()) })
	srv.badgerStore = badgerStore
	operatorPub, operatorKey, err := auth.GenerateKeypair()
	require.NoError(t, err)
	operatorID := auth.PublicKeyToAgentID(operatorPub)
	configureTestGovernanceGateway(t, srv, operatorID)
	var events atomic.Int32
	srv.OnEvent = func(string, string, string, string, any) { events.Add(1) }

	body := []byte(`{"operation":"remove_validator","target_id":"validator-a","reason":"test"}`)
	req := signedGovernanceRequestAs(t, operatorKey, operatorID, http.MethodPost, "/v1/governance/propose", body)
	resp := httptest.NewRecorder()
	srv.Router().ServeHTTP(resp, req)

	assert.Equal(t, http.StatusInternalServerError, resp.Code, resp.Body.String())
	assert.Contains(t, resp.Body.String(), "transaction committed")
	assert.Zero(t, events.Load(), "unverified committed state must not emit a success event")
}

func TestDisableValidatorSigningKeyNeutralizesLegacyEnvironmentKey(t *testing.T) {
	_, key, err := auth.GenerateKeypair()
	require.NoError(t, err)
	doc, err := json.Marshal(map[string]any{"priv_key": map[string]string{
		"type": "tendermint/PrivKeyEd25519", "value": base64.StdEncoding.EncodeToString(key),
	}})
	require.NoError(t, err)
	path := filepath.Join(t.TempDir(), "priv_validator_key.json")
	require.NoError(t, os.WriteFile(path, doc, 0o600))
	t.Setenv("VALIDATOR_KEY_FILE", path)
	srv, _, _ := newTestServer(t, "")
	require.True(t, srv.validatorSigningKeyConfigured)
	srv.DisableValidatorSigningKey()
	assert.False(t, srv.validatorSigningKeyConfigured)
}

func TestCORSAllowsSignedRequestNonceHeader(t *testing.T) {
	t.Setenv("CORS_ALLOWED_ORIGINS", "https://operator.example")
	srv, _, _ := newTestServer(t, "")
	req := httptest.NewRequest(http.MethodOptions, "/v1/governance/vote", nil)
	req.Header.Set("Origin", "https://operator.example")
	req.Header.Set("Access-Control-Request-Method", http.MethodPost)
	req.Header.Set("Access-Control-Request-Headers", "X-Agent-ID,X-Signature,X-Timestamp,X-Nonce")
	resp := httptest.NewRecorder()
	srv.Router().ServeHTTP(resp, req)

	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Contains(t, strings.ToLower(resp.Header().Get("Access-Control-Allow-Headers")), "x-nonce")
}

// TestParseGovOp_KnownAndUnknown pins the rest of the mapping so the repair addition
// didn't disturb the legacy ops, and an unknown op still errors.
func TestParseGovOp_KnownAndUnknown(t *testing.T) {
	for s, want := range map[string]tx.GovProposalOp{
		"add_validator":     tx.GovOpAddValidator,
		"remove_validator":  tx.GovOpRemoveValidator,
		"update_power":      tx.GovOpUpdatePower,
		"domain_reassign":   tx.GovOpDomainReassign,
		"sync_group_action": tx.GovOpSyncGroupAction,
		"scope_action":      tx.GovOpScopeAction,
	} {
		got, err := parseGovOp(s)
		require.NoError(t, err, s)
		assert.Equal(t, want, got, s)
	}
	_, err := parseGovOp("bogus_op")
	assert.Error(t, err, "unknown op must be rejected")
}
