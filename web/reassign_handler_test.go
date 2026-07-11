package web

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

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
		// Deliberately nil: an unowned domain must not require an owner-key
		// resolver before consensus has atomically established ownership.
		ResolveAgentKeyFn: nil,
	}
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
}

func TestGrantAs_ChildDomainUsesOwningAncestor(t *testing.T) {
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
	result := h.grantAs("research.eurorack", "agent-b", 2, nil)

	require.True(t, result.OK, result.Error)
	assert.Equal(t, int32(1), calls.Load())
	require.NotNil(t, captured)
	require.NotNil(t, captured.AccessGrant)
	assert.Equal(t, ownerID, captured.AccessGrant.GranterID)
	assert.Equal(t, "research.eurorack", captured.AccessGrant.Domain)
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

func TestParseDomainAccessLevels_WriteMapsToConsensusLevelTwo(t *testing.T) {
	levels := parseDomainAccessLevels(`[{"domain":"research.write","read":true,"write":true},{"domain":"research.read","read":true,"write":false}]`)
	assert.Equal(t, 2, levels["research.write"])
	assert.Equal(t, 1, levels["research.read"])
}
