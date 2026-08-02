package web

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func newGrantTestBadger(t *testing.T) *store.BadgerStore {
	t.Helper()
	bs, err := store.NewBadgerStore(filepath.Join(t.TempDir(), "badger"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, bs.CloseBadger()) })
	return bs
}

func newGrantRPC(t *testing.T, captured **tx.ParsedTx, calls *atomic.Int32) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, err := hex.DecodeString(raw)
		require.NoError(t, err)
		*captured, err = tx.DecodeTx(encoded)
		require.NoError(t, err)
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"ABC123","height":"42"}}`)
	}))
}

func TestGrantAs_UnownedDomainUsesGenesisAdmin(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()

	h := &DashboardHandler{
		BadgerStore:     bs,
		CometBFTRPC:     rpc.URL,
		AdminSigningKey: adminKey,
		SSE:             NewSSEBroadcaster(),
		// Deliberately nil: an unowned domain must not require an owner-key
		// resolver before consensus has atomically established ownership.
		ResolveAgentKeyFn: nil,
	}
	events := h.SSE.Subscribe()
	defer h.SSE.Unsubscribe(events)
	result := h.grantAs("new-research", "agent-b", 1, nil)

	require.True(t, result.OK, result.Error)
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessGrant)
	assert.Equal(t, tx.TxTypeAccessGrant, captured.Type)
	assert.Equal(t, agentIDForKey(adminKey), captured.AccessGrant.GranterID)
	assert.Empty(t, captured.AccessGrant.ExpectedOwnerID)
	assert.Empty(t, captured.AccessGrant.ExpectedOwnedDomain)
	assert.Equal(t, "agent-b", captured.AccessGrant.GranteeID)
	assert.Equal(t, "new-research", captured.AccessGrant.Domain)
	assert.Equal(t, uint8(1), captured.AccessGrant.Level)
	assert.Equal(t, "ABC123", result.TxHash)
	assert.Equal(t, int64(42), result.Height)
	select {
	case event := <-events:
		assert.Contains(t, string(event), "event: access")
		assert.Contains(t, string(event), `"action":"access_granted"`)
		assert.Contains(t, string(event), `"tx_hash":"ABC123"`)
	case <-time.After(time.Second):
		t.Fatal("committed access grant did not emit Chain Activity event")
	}
}

func TestGrantAs_ChildDomainUsesOwningAncestorForModifyLevel(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, ownerKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	ownerID := agentIDForKey(ownerKey)
	require.NoError(t, bs.RegisterDomain("research", ownerID, "", 1))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()

	h := &DashboardHandler{
		BadgerStore: bs,
		CometBFTRPC: rpc.URL,
		ResolveAgentKeyFn: func(id string) (ed25519.PrivateKey, bool) {
			return ownerKey, id == ownerID
		},
	}
	result := h.grantAs("research.eurorack", "agent-b", 3, nil)

	require.True(t, result.OK, result.Error)
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessGrant)
	assert.Equal(t, ownerID, captured.AccessGrant.GranterID)
	assert.Equal(t, "research.eurorack", captured.AccessGrant.Domain)
	assert.Equal(t, uint8(3), captured.AccessGrant.Level)
}

func TestGrantAs_SharedDomainNeedsNoOwnerTransaction(t *testing.T) {
	bs := newGrantTestBadger(t)
	var calls atomic.Int32
	h := &DashboardHandler{BadgerStore: bs, CometBFTRPC: "http://unused.invalid"}

	result := h.grantAs("general", "agent-b", 1, nil)

	assert.True(t, result.OK)
	assert.Equal(t, "shared", result.Action)
	assert.Zero(t, calls.Load())
}

func TestGrantAs_SharedDomainRejectsFakeModifySuccess(t *testing.T) {
	bs := newGrantTestBadger(t)
	h := &DashboardHandler{BadgerStore: bs, CometBFTRPC: "http://unused.invalid"}

	for _, domain := range []string{"general", "sage-project"} {
		result := h.grantAs(domain, "agent-b", 3, nil)
		assert.False(t, result.OK, domain)
		assert.Equal(t, "shared_modify_unsupported", result.Code, domain)
		assert.Equal(t, 3, result.Level, domain)
	}
}

func TestGrantAs_UnownedDomainWithoutAdminKeyIsActionable(t *testing.T) {
	h := &DashboardHandler{BadgerStore: newGrantTestBadger(t), CometBFTRPC: "http://unused.invalid"}

	result := h.grantAs("new-research", "agent-b", 1, nil)

	assert.False(t, result.OK)
	assert.Equal(t, "admin_key_unavailable", result.Code)
	assert.Contains(t, result.Error, "genesis admin")
}

func TestGrantAs_AdminOverrideLocalAgentPreservesOriginalOwner(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetID := agentIDForKey(targetKey)
	const originalOwner = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, bs.RegisterDomain("research.dmt", originalOwner, "", 1))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := &DashboardHandler{
		BadgerStore:     bs,
		CometBFTRPC:     rpc.URL,
		AdminSigningKey: adminKey,
		AppV18ActiveFn:  func() bool { return true },
		ResolveAgentKeyFn: func(id string) (ed25519.PrivateKey, bool) {
			if id == targetID {
				return targetKey, true
			}
			return nil, false
		},
	}

	preflight := h.grantAs("research.dmt", targetID, 2, nil)
	assert.False(t, preflight.OK)
	assert.Equal(t, originalOwner, preflight.OwnerID)
	assert.True(t, preflight.OverrideAvailable)

	override := &adminOverrideExpectation{Domain: "research.dmt", OwnerID: originalOwner, OwnedDomain: "research.dmt", Level: 2}
	result := h.grantAs("research.dmt", targetID, 2, override)
	require.True(t, result.OK, result.Error)
	assert.Equal(t, originalOwner, result.OwnerID)
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessGrant)
	assert.Equal(t, agentIDForKey(adminKey), captured.AccessGrant.GranterID)
	assert.Equal(t, originalOwner, captured.AccessGrant.ExpectedOwnerID)
	assert.Equal(t, "research.dmt", captured.AccessGrant.ExpectedOwnedDomain)
	ownerAfter, err := bs.GetDomainOwner("research.dmt")
	require.NoError(t, err)
	assert.Equal(t, originalOwner, ownerAfter)
}

func TestGrantAs_AdminOverrideRejectsStaleOwnerConfirmation(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetID := agentIDForKey(targetKey)
	const ownerA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const ownerB = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	require.NoError(t, bs.RegisterDomain("research.stale", ownerA, "", 1))
	require.NoError(t, bs.TransferDomain("research.stale", ownerB, "", 2))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := &DashboardHandler{
		BadgerStore:     bs,
		CometBFTRPC:     rpc.URL,
		AdminSigningKey: adminKey,
		AppV18ActiveFn:  func() bool { return true },
		ResolveAgentKeyFn: func(id string) (ed25519.PrivateKey, bool) {
			return targetKey, id == targetID
		},
	}

	expected := &adminOverrideExpectation{Domain: "research.stale", OwnerID: ownerA, OwnedDomain: "research.stale", Level: 2}
	result := h.grantAs("research.stale", targetID, 2, expected)
	assert.False(t, result.OK)
	assert.Equal(t, "owner_changed", result.Code)
	assert.Equal(t, ownerB, result.OwnerID)
	assert.True(t, result.OverrideReady, "fresh owner details must remain directly retryable")
	assert.Zero(t, calls.Load(), "stale confirmation must fail before broadcast")
}

func TestReconcileDomainGrants_OverrideOnlyDomainRetriesFailedRevoke(t *testing.T) {
	bs := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetID := agentIDForKey(targetKey)
	const owner = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	require.NoError(t, bs.RegisterDomain("research.retry", owner, "", 1))
	require.NoError(t, bs.SetAccessGrant("research.retry", targetID, 2, 0, owner))

	var captured *tx.ParsedTx
	var calls atomic.Int32
	rpc := newGrantRPC(t, &captured, &calls)
	defer rpc.Close()
	h := &DashboardHandler{
		BadgerStore:     bs,
		CometBFTRPC:     rpc.URL,
		AdminSigningKey: adminKey,
		AppV18ActiveFn:  func() bool { return true },
		ResolveAgentKeyFn: func(id string) (ed25519.PrivateKey, bool) {
			return targetKey, id == targetID
		},
	}
	overrides := map[string]adminOverrideExpectation{
		"research.retry": {Domain: "research.retry", OwnerID: owner, OwnedDomain: "research.retry", Level: 0},
	}

	results := h.reconcileDomainGrants(targetID, "[]", "[]", overrides)
	require.Len(t, results, 1)
	assert.True(t, results[0].OK, results[0].Error)
	assert.Equal(t, "revoke", results[0].Action)
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessRevoke)
	assert.Equal(t, agentIDForKey(adminKey), captured.AccessRevoke.RevokerID)
	assert.Equal(t, owner, captured.AccessRevoke.ExpectedOwnerID)
	assert.Equal(t, "research.retry", captured.AccessRevoke.ExpectedOwnedDomain)
}

func TestParseDomainAccessLevelsMapsReadWriteAndModifyTiers(t *testing.T) {
	levels := parseDomainAccessLevels(`[{"domain":"research.modify","read":true,"write":true,"modify":true},{"domain":"research.write","read":true,"write":true},{"domain":"research.read","read":true,"write":false}]`)
	assert.Equal(t, 3, levels["research.modify"])
	assert.Equal(t, 2, levels["research.write"])
	assert.Equal(t, 1, levels["research.read"])
}

func TestNormalizeDomainAccessBlobMakesPermissionLadderExplicit(t *testing.T) {
	normalized, err := normalizeDomainAccessBlob(
		`[{"domain":" research.modify ","read":false,"write":false,"modify":true},{"domain":"research.write","read":false,"write":true}]`,
	)
	require.NoError(t, err)

	var entries []domainAccessEntry
	require.NoError(t, json.Unmarshal([]byte(normalized), &entries))
	require.Len(t, entries, 2)
	assert.Equal(t, domainAccessEntry{
		Domain: "research.modify",
		Read:   true,
		Write:  true,
		Modify: true,
	}, entries[0])
	assert.Equal(t, domainAccessEntry{
		Domain: "research.write",
		Read:   true,
		Write:  true,
	}, entries[1])
}

func TestReassignDomainOwnershipRejectsAuthenticatedNonOperator(t *testing.T) {
	h, agentStore := newTestHandler(t)
	var broadcasts atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		broadcasts.Add(1)
		http.Error(w, "must not broadcast", http.StatusInternalServerError)
	}))
	t.Cleanup(rpc.Close)
	h.CometBFTRPC = rpc.URL

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/dashboard/network/reassign-domain-ownership",
		bytes.NewReader([]byte(`{"source_agent_id":"source","target_agent_id":"target","domain":"quiettype-pages"}`)),
	)
	markLocalDashboardRequest(h, req)
	req = req.WithContext(context.WithValue(req.Context(), verifiedDashboardAgentKey{}, "signed-member-agent"))
	resp := httptest.NewRecorder()

	h.handleReassignDomainOwnership(agentStore)(resp, req)

	assert.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
	assert.Zero(t, broadcasts.Load())
}

func TestReassignDomainOwnershipRejectsUnauthenticatedRequest(t *testing.T) {
	h, agentStore := newTestHandler(t)
	var broadcasts atomic.Int32
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		broadcasts.Add(1)
		http.Error(w, "must not broadcast", http.StatusInternalServerError)
	}))
	t.Cleanup(rpc.Close)
	h.CometBFTRPC = rpc.URL

	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/dashboard/network/reassign-domain-ownership",
		bytes.NewReader([]byte(`{"source_agent_id":"source","target_agent_id":"target","domain":"quiettype-pages"}`)),
	)
	resp := httptest.NewRecorder()

	h.handleReassignDomainOwnership(agentStore)(resp, req)

	assert.Equal(t, http.StatusForbidden, resp.Code, resp.Body.String())
	assert.Zero(t, broadcasts.Load())
}

func TestReassignDomainOwnershipPostAppV20UsesChainBoundOperatorProof(t *testing.T) {
	h, agentStore := newTestHandler(t)
	badgerStore := newGrantTestBadger(t)
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceID := agentIDForKey(adminKey)
	validatorID := agentIDForKey(validatorKey)
	targetID := agentIDForKey(targetKey)
	require.NoError(t, badgerStore.RegisterDomain("quiettype-pages", sourceID, "", 1))
	require.NoError(t, agentStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: targetID,
		Name:    "quiettype-agent",
		Role:    "member",
		Status:  "active",
	}))

	var captured []*tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, decodeErr := hex.DecodeString(raw)
		require.NoError(t, decodeErr)
		parsed, decodeErr := tx.DecodeTx(encoded)
		require.NoError(t, decodeErr)
		captured = append(captured, parsed)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0,"log":"purged 1 grants"},"hash":"ABC123","height":"42"}}`)
	}))
	t.Cleanup(rpc.Close)

	h.BadgerStore = badgerStore
	h.CometBFTRPC = rpc.URL
	h.AdminSigningKey = adminKey
	h.SigningKey = validatorKey
	h.AppV20ActiveFn = func() bool { return true }
	h.GovernanceDomainFn = func() string { return dashboardTestGovernanceDomain }
	h.ValidatorCountFn = func() int { return 1 }
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if id == targetID {
			return targetKey, true
		}
		return nil, false
	}

	body := []byte(fmt.Sprintf(
		`{"source_agent_id":%q,"target_agent_id":%q,"domain":"quiettype-pages"}`,
		sourceID,
		targetID,
	))
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/dashboard/network/reassign-domain-ownership",
		bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	markLocalDashboardRequest(h, req)
	resp := httptest.NewRecorder()

	h.handleReassignDomainOwnership(agentStore)(resp, req)

	require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())
	var result map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &result))
	assert.Equal(t, "ok", result["status"])
	require.Len(t, captured, 3, "single-validator post-app-v20 propose auto-votes; only propose, reassign, and grant broadcast")

	propose := captured[0]
	require.Equal(t, tx.TxTypeGovPropose, propose.Type)
	assert.Equal(t, []byte(validatorKey.Public().(ed25519.PublicKey)), []byte(propose.PublicKey))
	assert.Equal(t, []byte(adminKey.Public().(ed25519.PublicKey)), []byte(propose.AgentPubKey))
	assert.Len(t, propose.AgentNonce, 8)
	assert.True(t, bytes.HasPrefix(propose.AgentRequest, []byte("POST /v1/governance/propose\n")))
	var proofBody map[string]any
	require.NoError(t, json.Unmarshal(
		propose.AgentRequest[len("POST /v1/governance/propose\n"):],
		&proofBody,
	))
	assert.Equal(t, validatorID, proofBody["validator_id"])
	assert.Equal(t, dashboardTestGovernanceDomain, proofBody["governance_domain"])
	assert.Equal(t, "domain_reassign", proofBody["operation"])
	assert.Equal(t, "quiettype-pages", proofBody["target_id"])

	reassign := captured[1]
	require.Equal(t, tx.TxTypeDomainReassign, reassign.Type)
	assert.Equal(t, []byte(adminKey.Public().(ed25519.PublicKey)), []byte(reassign.PublicKey))
	assert.Empty(t, reassign.DomainReassign.ExpectedOwnerID,
		"a dormant app-v26 chain must receive the historical wire form")
	assert.Equal(
		t,
		governance.ComputeProposalID(validatorID, 42, governance.OpDomainReassign, "quiettype-pages"),
		reassign.DomainReassign.ProposalID,
	)

	grant := captured[2]
	require.Equal(t, tx.TxTypeAccessGrant, grant.Type)
	assert.Equal(t, []byte(targetKey.Public().(ed25519.PublicKey)), []byte(grant.PublicKey))
	assert.Equal(t, targetID, grant.AccessGrant.GranteeID)
	assert.Equal(t, uint8(3), grant.AccessGrant.Level)
}

func TestCancelActiveProposalPostAppV20UsesChainBoundOperatorProof(t *testing.T) {
	_, adminKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	validatorID := agentIDForKey(validatorKey)

	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw := strings.TrimPrefix(r.URL.Query().Get("tx"), "0x")
		encoded, decodeErr := hex.DecodeString(raw)
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(encoded)
		require.NoError(t, decodeErr)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"ABC123","height":"43"}}`)
	}))
	t.Cleanup(rpc.Close)

	h := &DashboardHandler{
		AdminSigningKey:    adminKey,
		SigningKey:         validatorKey,
		CometBFTRPC:        rpc.URL,
		GovernanceDomainFn: func() string { return dashboardTestGovernanceDomain },
	}
	h.cancelActiveProposal("proposal-123", validatorKey, true)

	require.NotNil(t, captured)
	require.Equal(t, tx.TxTypeGovCancel, captured.Type)
	assert.Equal(t, []byte(validatorKey.Public().(ed25519.PublicKey)), []byte(captured.PublicKey))
	assert.Equal(t, []byte(adminKey.Public().(ed25519.PublicKey)), []byte(captured.AgentPubKey))
	assert.Len(t, captured.AgentNonce, 8)
	assert.True(t, bytes.HasPrefix(captured.AgentRequest, []byte("POST /v1/governance/cancel\n")))

	var proofBody map[string]any
	require.NoError(t, json.Unmarshal(
		captured.AgentRequest[len("POST /v1/governance/cancel\n"):],
		&proofBody,
	))
	assert.Equal(t, validatorID, proofBody["validator_id"])
	assert.Equal(t, dashboardTestGovernanceDomain, proofBody["governance_domain"])
	assert.Equal(t, "proposal-123", proofBody["proposal_id"])
}
