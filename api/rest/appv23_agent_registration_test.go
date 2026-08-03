package rest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV23AgentRegisterReturnsCommittedPendingPolicy(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	rootID := appV23RESTAgentID("11")
	bundledMemberID := appV23RESTAgentID("22")
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "pending-registration", AgentID: bundledMemberID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "bundled.home",
		Clearance: 1, Capabilities: 0, Height: 1, BootstrapDigest: "bootstrap",
	}))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })
	agentID := appV23RESTAgentID("66")
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		require.NotNil(t, parsed.AgentRegister)
		require.Equal(t, agentID, parsed.AgentRegister.AgentID)
		require.NoError(t, badger.RegisterAgentWithCapabilities(
			agentID, parsed.AgentRegister.Name, store.AppV23RoleMember,
			parsed.AgentRegister.BootBio, parsed.AgentRegister.Provider,
			parsed.AgentRegister.P2PAddress, 7,
			store.DefaultSelfRegisteredAgentCapabilities,
		))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"REGISTER","height":"7"}}`)
	}))
	defer rpc.Close()
	srv.cometbftRPC = rpc.URL

	req := httptest.NewRequest(
		http.MethodPost, "/v1/agent/register",
		bytes.NewBufferString(`{"name":"Requested Admin","role":"admin","provider":"codex"}`),
	)
	req = req.WithContext(middleware.WithAgentID(req.Context(), agentID))
	rec := httptest.NewRecorder()
	srv.handleAgentRegister(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	var response struct {
		Role             string                  `json:"role"`
		Status           string                  `json:"status"`
		Capabilities     store.AgentCapabilities `json:"capabilities"`
		ApprovalRequired bool                    `json:"approval_required"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, store.AppV23RoleMember, response.Role)
	require.Equal(t, "pending_review", response.Status)
	require.Equal(t, store.DefaultSelfRegisteredAgentCapabilities, response.Capabilities)
	require.True(t, response.ApprovalRequired)
}

func TestAppV23RejectedPendingAgentSignedRetryReentersReview(t *testing.T) {
	srv, _, badger, agents := newRBACTestServer(t)
	rootID := appV23RESTAgentID("71")
	bundledID := appV23RESTAgentID("72")
	retryingID := appV23RESTAgentID("73")
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "pending-registration-retry", AgentID: bundledID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "bundled.home",
		Clearance: 1, Capabilities: 0, Height: 1, BootstrapDigest: "bootstrap",
	}))
	require.NoError(t, badger.RegisterAgentWithCapabilities(
		retryingID, "immutable pending name", store.AppV23RoleMember,
		"immutable purpose", "codex", "", 2,
		store.DefaultSelfRegisteredAgentCapabilities,
	))
	removedAt := time.Now().UTC()
	agents.agents[retryingID] = &store.AgentEntry{
		AgentID: retryingID, Name: "immutable pending name",
		RegisteredName: "immutable pending name", Role: store.AppV23RoleMember,
		Status: "removed", RemovedAt: &removedAt,
	}
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })

	var broadcasts int
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		broadcasts++
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		require.NotNil(t, parsed.AgentRegister)
		require.Equal(t, retryingID, parsed.AgentRegister.AgentID)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"RETRY","height":"3"}}`)
	}))
	defer rpc.Close()
	srv.cometbftRPC = rpc.URL

	req := httptest.NewRequest(
		http.MethodPost, "/v1/agent/register",
		bytes.NewBufferString(`{"name":"attempted replacement","role":"admin","provider":"other"}`),
	)
	req = req.WithContext(middleware.WithAgentID(req.Context(), retryingID))
	rec := httptest.NewRecorder()
	srv.handleAgentRegister(rec, req)

	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.Equal(t, 1, broadcasts, "removed pending projection must not short-circuit the signed retry")
	var response struct {
		Name             string `json:"name"`
		RegisteredName   string `json:"registered_name"`
		Status           string `json:"status"`
		ApprovalRequired bool   `json:"approval_required"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "immutable pending name", response.Name)
	require.Equal(t, "immutable pending name", response.RegisteredName)
	require.Equal(t, "pending_review", response.Status)
	require.True(t, response.ApprovalRequired)

	immutable, err := badger.GetRegisteredAgent(retryingID)
	require.NoError(t, err)
	require.Equal(t, int64(2), immutable.RegisteredAt)
	require.Equal(t, "immutable pending name", immutable.RegisteredName)
	require.Equal(t, "immutable purpose", immutable.BootBio)
	enrollment, err := badger.GetAppV23Enrollment(retryingID)
	require.NoError(t, err)
	require.Nil(t, enrollment, "retry must return to review without granting standing")
}

func TestAppV23AgentRegisterRejectsEveryRootCredentialGeneration(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	rootID := appV23RESTAgentID("11")
	memberID := appV23RESTAgentID("22")
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "registration-rotation", AgentID: memberID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "member.home",
		Clearance: 1, Capabilities: 0, Height: 1, BootstrapDigest: "bootstrap",
	}))
	newRootID := appV23RESTAgentID("77")
	require.NoError(t, badger.RotateAppV23RootCredential(1, newRootID, 2))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })

	req := httptest.NewRequest(
		http.MethodPost, "/v1/agent/register",
		bytes.NewBufferString(`{"name":"CEREBRUM","role":"admin"}`),
	)
	req = req.WithContext(middleware.WithAgentID(req.Context(), newRootID))
	rec := httptest.NewRecorder()
	srv.handleAgentRegister(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())

	oldReq := httptest.NewRequest(
		http.MethodPost, "/v1/agent/register",
		bytes.NewBufferString(`{"name":"retired","role":"admin"}`),
	)
	oldReq = oldReq.WithContext(middleware.WithAgentID(oldReq.Context(), rootID))
	oldRec := httptest.NewRecorder()
	srv.handleAgentRegister(oldRec, oldReq)
	require.Equal(t, http.StatusForbidden, oldRec.Code, oldRec.Body.String())
}

func TestAppV23PolicyPrincipalRejectsAllRetiredRootGenerations(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	rootID := appV23RESTAgentID("31")
	memberID := appV23RESTAgentID("32")
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "policy-principal-rotation", AgentID: memberID,
		Profile: store.AppV23ProfileStandard, HomeDomain: "member.home",
		Clearance: 1, Capabilities: 0, Height: 1, BootstrapDigest: "bootstrap",
	}))
	secondRootID := appV23RESTAgentID("33")
	currentRootID := appV23RESTAgentID("34")
	require.NoError(t, badger.RotateAppV23RootCredential(1, secondRootID, 2))
	require.NoError(t, badger.RotateAppV23RootCredential(2, currentRootID, 3))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })

	principal, err := appV23PolicyPrincipal(badger, currentRootID)
	require.NoError(t, err)
	require.Equal(t, rootID, principal)
	for _, retired := range []string{rootID, secondRootID} {
		_, err := appV23PolicyPrincipal(badger, retired)
		require.ErrorContains(t, err, "retired Root credential")
		isRoot, rootErr := srv.appV23IsRootIdentity(retired)
		require.NoError(t, rootErr)
		require.True(t, isRoot)
	}
}
