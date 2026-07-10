package rest

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestBroadcastErrorPublic_DeprecationGate pins the fork-aware mapping of the
// app-v16 deprecation-gate rejections that reach broadcastErrorPublic wrapped as
// "tx rejected in FinalizeBlock (code N): <gate log>". The ordering matters: the
// app-v16 "unknown memory" reject also ends with "not authorized to deprecate", so
// the 404 must win over the 403; and a PRE-app-v16 domainless reject (no "legacy
// memory predating app-v8.4" phrase) must fall through to 403, not a 409 that would
// advise a remediation the chain can't run yet.
func TestBroadcastErrorPublic_DeprecationGate(t *testing.T) {
	cases := []struct {
		name   string
		log    string
		status int
	}{
		{
			"app-v16 legacy → 409",
			"challenge: memory abc has no recorded domain (legacy memory predating app-v8.4); deprecation is blocked until its domain is repaired via an OpMemoryDomainRepair governance proposal",
			http.StatusConflict,
		},
		{
			"app-v16 unknown id → 404 (must beat the trailing 'not authorized')",
			"challenge: unknown memory abc (no memory record and no recorded domain); not authorized to deprecate",
			http.StatusNotFound,
		},
		{
			"app-v15 unauthorized (domained) → 403",
			"challenge: agent 0123456789abcdef not authorized to deprecate memory abc (need domain ownership or a level-3 modify grant)",
			http.StatusForbidden,
		},
		{
			"pre-app-v16 domainless reject → 403 (not a 409)",
			"challenge: memory abc has no recorded domain; not authorized to deprecate",
			http.StatusForbidden,
		},
		{
			"app-v17 unauthorized reinstate → 403",
			"reinstate: agent 0123456789abcdef not authorized to reinstate memory abc (need domain ownership or a level-3 modify grant)",
			http.StatusForbidden,
		},
		{
			"app-v17 missing-domain reinstate → 403",
			"reinstate: memory abc has no recorded domain; not authorized",
			http.StatusForbidden,
		},
		{
			"app-v17 wrong lifecycle state → 409",
			"reinstate: memory abc is not challenged (status=committed)",
			http.StatusConflict,
		},
		{
			"other FinalizeBlock reject → generic 400",
			"content schema rejected for (hr,x): bad shape",
			http.StatusBadRequest,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := errors.New("tx rejected in FinalizeBlock (code 91): " + tc.log)
			status, _ := broadcastErrorPublic(err)
			assert.Equal(t, tc.status, status)
		})
	}
}
