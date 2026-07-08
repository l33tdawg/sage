package federation

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestClassifySyncStatus pins the load-bearing pre-v11.5-peer detection:
// 404/405/501 must classify as unsupported (distinct from auth churn), because
// a refactor of the shared status switch would otherwise retry old peers on
// the short backoff instead of parking them at the 1h floor. Tests the REAL
// classifier used by both SyncPush and SyncDigest.
func TestClassifySyncStatus(t *testing.T) {
	cases := []struct {
		status          int
		wantOK          bool
		wantUnsupported bool
	}{
		{http.StatusOK, true, false},
		{http.StatusNotFound, false, true},
		{http.StatusMethodNotAllowed, false, true},
		{http.StatusNotImplemented, false, true},
		{http.StatusUnauthorized, false, false},
		{http.StatusForbidden, false, false},
		{http.StatusBadGateway, false, false},
		{http.StatusInternalServerError, false, false},
	}
	for _, c := range cases {
		ok, unsupported := classifySyncStatus(c.status)
		assert.Equal(t, c.wantOK, ok, "ok for status %d", c.status)
		assert.Equal(t, c.wantUnsupported, unsupported, "unsupported for status %d", c.status)
	}
}
