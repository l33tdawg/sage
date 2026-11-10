package web

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func TestAppV31PolicyExplicitInspectionRequiresActivation(t *testing.T) {
	for _, active := range []bool{false, true} {
		t.Run(map[bool]string{false: "before", true: "after"}[active], func(t *testing.T) {
			fixture := newAppV23AccessFixture(t)
			if active {
				require.NoError(t, fixture.badger.MarkUpgradeApplied("app-v30", 30, 2))
				require.NoError(t, fixture.badger.MarkUpgradeApplied("app-v31", 31, 3))
			}
			var captured *tx.ParsedTx
			var calls atomic.Int32
			rpc := newGrantRPC(t, &captured, &calls)
			defer rpc.Close()
			h := appV23AccessTestHandler(fixture, rpc.URL, nil)
			requested := uint32(15 | store.AgentCapabilityInspectDirectGrants)
			req := appV23AccessRequest(t, http.MethodPut, "/policy", "id", fixture.agentID, map[string]any{
				"role": "member", "profile": "companion", "clearance": 1, "capabilities": requested, "home_domain": "companion-home",
			})
			req = appV23AccessAs(req, fixture.rootID)
			rec := httptest.NewRecorder()
			h.handleAppV23AgentPolicy().ServeHTTP(rec, req)
			if !active {
				require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
				require.Zero(t, calls.Load())
				return
			}
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, int32(1), calls.Load())
			require.NotNil(t, captured.AgentRoleChange)
			require.Equal(t, requested, captured.AgentRoleChange.Capabilities)
			// A submitted request is not a committed grant; preflight never changes enrollment.
			enrollment, err := fixture.badger.GetAppV23Enrollment(fixture.agentID)
			require.NoError(t, err)
			require.Equal(t, store.AgentCapabilities(15), enrollment.Capabilities)
		})
	}
}
