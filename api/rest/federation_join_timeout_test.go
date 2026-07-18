package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/federation"
)

func TestJoinGuestConfirmUsesFullTwoCommitDeadline(t *testing.T) {
	t.Setenv("SAGE_TX_COMMIT_TIMEOUT_MS", "1000")
	want := federation.JoinConfirmationOperationTimeout()
	require.Greater(t, want, fedRecallTimeout(), "test requires distinct confirmation and ordinary read budgets")

	req := httptest.NewRequest(http.MethodPost, "/v1/federation/join/guest/confirm", nil)
	ctx, cancel := contextWithJoinConfirmTimeout(req)
	defer cancel()

	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	remaining := time.Until(deadline)
	require.LessOrEqual(t, remaining, want)
	require.Greater(t, remaining, want-time.Second)
}
