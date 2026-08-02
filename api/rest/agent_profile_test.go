package rest

import (
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// GET /v1/agent/me must surface the on-chain PoE signals the audit found
// unreadable: the lifetime corroboration count, the authoritative
// verdict-correctness accuracy, and per-domain expertise — all read directly
// from BadgerDB (the consensus source), not the off-chain mirror.
func TestGetAgent_OnChainPoESignals(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	req, agentID := signedRequest(t, http.MethodGet, "/v1/agent/me", nil)

	// Give the agent a domain so domain_expertise can populate.
	mock := newMockAgentStore()
	mock.domains[agentID] = []string{"pwn_heap"}
	srv.agentStore = mock

	// Seed on-chain PoE signals: two globally-matched verdicts (CorrCount=2,
	// EWMACount=2) and one matched verdict in pwn_heap.
	require.NoError(t, bs.UpdateVerdictStats(map[string]bool{agentID: true}))
	require.NoError(t, bs.UpdateVerdictStats(map[string]bool{agentID: true}))
	require.NoError(t, bs.UpdateDomainVerdictStats("pwn_heap", map[string]bool{agentID: true}))

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp AgentProfileResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, int64(2), resp.CorrCount, "two matched verdicts -> corr_count 2")
	assert.Greater(t, resp.Accuracy, 0.5, "two correct verdicts blend accuracy above the 0.5 cold-start")
	require.Contains(t, resp.DomainExpertise, "pwn_heap", "domain the agent voted in must appear")
	assert.Greater(t, resp.DomainExpertise["pwn_heap"], 0.5)
}

// With no on-chain history (empty BadgerStore) the new fields stay at their
// zero/cold-start values and the handler does not panic — the additive change
// is backward-compatible.
func TestGetAgent_OnChainPoESignals_ColdStart(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	bs, err := store.NewBadgerStore(t.TempDir())
	require.NoError(t, err)
	t.Cleanup(func() { _ = bs.CloseBadger() })
	srv.badgerStore = bs

	req, _ := signedRequest(t, http.MethodGet, "/v1/agent/me", nil)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var resp AgentProfileResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))

	assert.Equal(t, int64(0), resp.CorrCount)
	assert.Empty(t, resp.DomainExpertise, "no voting history -> no domain expertise emitted")
}

func TestAppV23GetAgentSelfExposesCallerStandingIncludingPendingReview(t *testing.T) {
	srv, _, badger, agents := newRBACTestServer(t)
	rootPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	companionPub, companionKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rootID := auth.PublicKeyToAgentID(rootPub)
	companionID := auth.PublicKeyToAgentID(companionPub)
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: rootID, Scope: "self-home-policy",
		AgentID: companionID, Profile: store.AppV23ProfileCompanion,
		HomeDomain: "voice-interface", Clearance: 2, Capabilities: 15,
		Height: 1, BootstrapDigest: "self-home-policy",
	}))
	agents.agents[companionID] = &store.AgentEntry{
		AgentID: companionID, Name: "Mynah", Role: store.AppV23RoleMember,
		Status: "active", Clearance: 2, Capabilities: 15,
	}
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })

	req := signedRequestAs(
		t, companionKey, companionID, http.MethodGet, "/v1/agent/me", nil,
	)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	var resp AgentProfileResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	require.Equal(t, companionID, resp.AgentID)
	require.Equal(t, store.AppV23RoleMember, resp.Role)
	require.Equal(t, store.AppV23ProfileCompanion, resp.Profile)
	require.Equal(t, "voice-interface", resp.HomeDomain)
	require.Equal(t, "active", resp.EnrollmentState)
	require.Equal(t, "active", resp.RegistrationState)
	require.Equal(t, uint8(2), resp.Clearance)
	require.Equal(t, uint32(15), resp.Capabilities)
	require.NotNil(t, resp.CanRead)
	require.NotNil(t, resp.CanWrite)
	require.True(t, *resp.CanRead)
	require.True(t, *resp.CanWrite)
	require.Equal(t, "home_domain", resp.AccessScope)
	require.False(t, resp.ApprovalRequired)

	pendingPub, pendingKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	pendingID := auth.PublicKeyToAgentID(pendingPub)
	require.NoError(t, badger.RegisterAgentWithCapabilities(
		pendingID, "pending", store.AppV23RoleMember, "", "test", "", 2, 0,
	))
	agents.agents[pendingID] = &store.AgentEntry{
		AgentID: pendingID, Name: "pending", Role: store.AppV23RoleMember,
		Status: "active", Clearance: 2,
	}
	deniedReq := signedRequestAs(
		t, pendingKey, pendingID, http.MethodGet, "/v1/agent/me", nil,
	)
	pending := httptest.NewRecorder()
	srv.Router().ServeHTTP(pending, deniedReq)
	require.Equal(t, http.StatusOK, pending.Code, pending.Body.String())
	var pendingResp AgentProfileResponse
	require.NoError(t, json.Unmarshal(pending.Body.Bytes(), &pendingResp))
	require.Equal(t, pendingID, pendingResp.AgentID)
	require.Equal(t, store.AppV23RoleMember, pendingResp.Role)
	require.Equal(t, "pending_review", pendingResp.EnrollmentState)
	require.Equal(t, "pending_review", pendingResp.RegistrationState)
	require.Equal(t, uint8(1), pendingResp.Clearance,
		"post-v23 self-registration keeps its consensus pending-review clearance")
	require.Equal(t, uint32(0), pendingResp.Capabilities)
	require.True(t, pendingResp.ApprovalRequired)
	require.NotNil(t, pendingResp.CanRead)
	require.NotNil(t, pendingResp.CanWrite)
	require.False(t, *pendingResp.CanRead)
	require.False(t, *pendingResp.CanWrite)
	require.Empty(t, pendingResp.HomeDomain)
	require.Empty(t, pendingResp.Domains)
}

func TestAppV23GetAgentSelfDoesNotTurnSignedUnknownKeyIntoRosterOracle(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	rootPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	memberPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: auth.PublicKeyToAgentID(rootPub), Scope: "self-standing-unknown",
		AgentID: auth.PublicKeyToAgentID(memberPub), Profile: store.AppV23ProfileStandard,
		HomeDomain: "member-home", Clearance: 1, Capabilities: 0,
		Height: 1, BootstrapDigest: "self-standing-unknown",
	}))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })

	unknownPub, unknownKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	unknownID := auth.PublicKeyToAgentID(unknownPub)
	req := signedRequestAs(t, unknownKey, unknownID, http.MethodGet, "/v1/agent/me", nil)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusNotFound, rr.Code, rr.Body.String())
	require.NotContains(t, rr.Body.String(), "member-home")
}

func TestAppV23GetAgentStandingViewSkipsOptionalServingProjections(t *testing.T) {
	srv, _, badger, _ := newRBACTestServer(t)
	rootPub, _, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	memberPub, memberKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	memberID := auth.PublicKeyToAgentID(memberPub)
	require.NoError(t, badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: auth.PublicKeyToAgentID(rootPub), Scope: "standing-fast-path",
		AgentID: memberID, Profile: store.AppV23ProfileStandard,
		HomeDomain: "member-home", Clearance: 1, Capabilities: 0,
		Height: 1, BootstrapDigest: "standing-fast-path",
	}))
	srv.SetPostV23ForNextTxAccessor(func() bool { return true })
	// The full profile consults these optional projections. Standing must not.
	srv.agentStore = nil
	srv.scoreStore = nil
	req := signedRequestAs(t, memberKey, memberID, http.MethodGet,
		"/v1/agent/me?view=standing", nil)
	rec := httptest.NewRecorder()
	srv.Router().ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response AgentProfileResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, memberID, response.AgentID)
	require.Equal(t, "member-home", response.HomeDomain)
	require.Empty(t, response.Domains)
	require.Zero(t, response.VoteCount)
}
