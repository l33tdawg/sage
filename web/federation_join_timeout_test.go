package web

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
)

type joinConfirmDeadlineDriver struct {
	FederationJoinDriver
	hasDeadline bool
	remaining   time.Duration
}

func (d *joinConfirmDeadlineDriver) GuestConfirm(ctx context.Context, _, _ string, _ federation.ScopeWire) (string, error) {
	deadline, ok := ctx.Deadline()
	d.hasDeadline = ok
	if ok {
		d.remaining = time.Until(deadline)
	}
	return "confirm-tx", nil
}

func TestFedGuestConfirmUsesFullJoinOperationDeadline(t *testing.T) {
	// Keep the expected deadline short and distinct from fedCallTimeout without
	// waiting for it. The production helper derives its value from this override.
	t.Setenv("SAGE_TX_COMMIT_TIMEOUT_MS", "1000")
	want := federation.JoinConfirmationOperationTimeout()
	require.Less(t, want, fedCallTimeout)

	driver := &joinConfirmDeadlineDriver{}
	h := NewDashboardHandler(nil, "test")
	h.Federation = driver
	body := bytes.NewBufferString(`{
		"session_id":"session",
		"endpoint":"https://127.0.0.1:18444",
		"host_scope":{"max_clearance":4,"allowed_domains":[],"mode":"exchange","direction":"both"}
	}`)
	req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/federation/join/guest/confirm", body)
	rr := httptest.NewRecorder()
	h.handleFedGuestConfirm(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	require.True(t, driver.hasDeadline)
	require.LessOrEqual(t, driver.remaining, want)
	require.Greater(t, driver.remaining, want-time.Second,
		"final confirmation inherited the ordinary call deadline instead of the two-commit operation budget")
}
