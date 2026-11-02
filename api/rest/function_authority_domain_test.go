package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestFunctionAuthorityRESTInvalidDomainRefusesBeforeBroadcast(t *testing.T) {
	srv, rootKey, memberKey, calls, _ := setupFunctionAuthorityREST(t, 0)
	for _, domain := range []string{":", "parent:child", ".leading", "trailing.", "empty..segment", "has space", "has\ttab", "has\x00control", "has\u00a0space", strings.Repeat("a", 513)} {
		t.Run(domain, func(t *testing.T) {
			var request FunctionAuthorityRequest
			require.NoError(t, json.Unmarshal(functionAuthorityRESTBody(appV23ElevationTestID(memberKey)), &request))
			request.Domain = domain
			body, err := json.Marshal(request)
			require.NoError(t, err)
			recorder := httptest.NewRecorder()
			srv.Router().ServeHTTP(recorder, functionAuthorityRESTRequest(t, rootKey, body))
			require.Equal(t, http.StatusBadRequest, recorder.Code, recorder.Body.String())
			require.Zero(t, calls.Load(), "invalid lexical domain reached consensus broadcast")
		})
	}
}
