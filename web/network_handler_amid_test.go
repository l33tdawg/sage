package web

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// TestAmidOperatorRoutesEmitTheSameTransactionAsCerebrum proves the amid
// mounting is the CEREBRUM pair and not a parallel implementation: the same
// signed Root request driven through both mounts has to produce
// payload-identical transactions. The two mounts differ only in middleware —
// amid has no enclosing dashboard group, so its registration carries the
// request-auth middleware itself.
func TestAmidOperatorRoutesEmitTheSameTransactionAsCerebrum(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	ctx := context.Background()
	agents, err := store.NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "agents.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, agents.Close()) })
	require.NoError(t, agents.CreateAgent(ctx, &store.AgentEntry{
		AgentID: fixture.agentID, Name: "audit-writer", Role: "member",
		Status: "active", Clearance: 1,
	}))
	resolveRootKey := func(id string) (ed25519.PrivateKey, bool) {
		return fixture.rootKey, id == fixture.rootID
	}
	newOperatorHandler := func(version, rpcURL string) *DashboardHandler {
		handler := NewDashboardHandler(agents, version)
		handler.BadgerStore = fixture.badger
		handler.CometBFTRPC = rpcURL
		handler.AppV23ActiveFn = func() bool { return true }
		handler.ResolveAgentKeyFn = resolveRootKey
		return handler
	}

	var amidTx, cerebrumTx *tx.ParsedTx
	var amidCalls, cerebrumCalls atomic.Int32
	amidRPC := newGrantRPC(t, &amidTx, &amidCalls)
	defer amidRPC.Close()
	cerebrumRPC := newGrantRPC(t, &cerebrumTx, &cerebrumCalls)
	defer cerebrumRPC.Close()

	amid := newOperatorHandler("amid-test", amidRPC.URL)
	amidRouter := chi.NewRouter()
	amid.RegisterAmidOperatorRoutes(amidRouter)

	cerebrum := newOperatorHandler("cerebrum-test", cerebrumRPC.URL)
	cerebrumRouter := chi.NewRouter()
	cerebrumRouter.Group(func(r chi.Router) {
		// The protected group RegisterRoutes is mounted inside on a real node.
		r.Use(cerebrum.authMiddleware)
		r.Use(cerebrum.cerebrumBrowserLocalityGate)
		r.Use(cerebrum.dashboardOperatorMutationGate)
		cerebrum.RegisterNetworkRoutes(r)
	})

	body := map[string]any{
		"role": "member", "profile": "companion", "clearance": 2, "capabilities": 15,
	}
	encoded, err := json.Marshal(body)
	require.NoError(t, err)
	policyPath := "/v1/dashboard/network/access/agents/" + fixture.agentID + "/policy"

	send := func(router chi.Router, captured **tx.ParsedTx, calls *atomic.Int32) {
		t.Helper()
		req := appV23AccessRequest(
			t, http.MethodPut, policyPath, "id", fixture.agentID, body,
		)
		signAgentRequest(t, req, fixture.rootKey, encoded)
		rec := httptest.NewRecorder()
		router.ServeHTTP(rec, req)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, int32(1), calls.Load())
		require.NotNil(t, *captured)
	}

	send(amidRouter, &amidTx, &amidCalls)
	send(cerebrumRouter, &cerebrumTx, &cerebrumCalls)

	require.Equal(t, tx.TxTypeAgentRoleChange, amidTx.Type)
	require.Equal(t, cerebrumTx.Type, amidTx.Type)
	require.Equal(t, cerebrumTx.AgentRoleChange, amidTx.AgentRoleChange)
	amidPayload, err := tx.PayloadBytes(amidTx)
	require.NoError(t, err)
	cerebrumPayload, err := tx.PayloadBytes(cerebrumTx)
	require.NoError(t, err)
	require.Equal(t, cerebrumPayload, amidPayload)
}
