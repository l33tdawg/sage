package rest

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/stretchr/testify/require"
)

func TestRemotePipeResolutionProblemsDoNotExposeRouteDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		name   string
		cause  error
		status int
	}{
		{"incomplete", federation.ErrRemotePipeResolutionIncomplete, http.StatusServiceUnavailable},
		{"not visible", federation.ErrRemotePipeTargetNotFound, http.StatusNotFound},
		{"unsupported", federation.ErrRemotePipePeerUnsupported, http.StatusNotImplemented},
	} {
		t.Run(tc.name, func(t *testing.T) {
			failure := errors.Join(tc.cause, fmt.Errorf("federation GET https://private-peer.example/fed/v1/status failed: candidate=relay secret-route; token=secret"))
			response := httptest.NewRecorder()
			(&Server{}).writeRemotePipeTargetError(response, failure)
			require.Equal(t, tc.status, response.Code)
			for _, private := range []string{"private-peer", "fed/v1/status", "candidate=relay", "secret"} {
				require.NotContains(t, response.Body.String(), private)
			}
		})
	}
}
