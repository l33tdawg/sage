package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSubmitMemoryDomainWriteDeniedReturnsTypedActionableProblem(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"result": map[string]any{
				"check_tx": map[string]any{"code": 0, "log": ""},
				"tx_result": map[string]any{
					"code": 11,
					"log":  "access denied: agent 0123456789abcdef has no write access to domain fa-pillar",
				},
				"hash": "DENIED", "height": "1",
			},
		})
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)
	body := []byte(`{"content":"domain ACL regression","memory_type":"fact","domain_tag":"fa-pillar","confidence_score":0.9}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusForbidden, rr.Code, rr.Body.String())
	var problem map[string]any
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &problem))
	assert.Equal(t, domainWriteDeniedProblemType, problem["type"])
	assert.Equal(t, "Domain write access denied", problem["title"])
	assert.Contains(t, problem["detail"], "Grant level 2 (read + write)")
	assert.Contains(t, problem["detail"], "CEREBRUM Access Controls")
	assert.NotContains(t, rr.Body.String(), "0123456789abcdef", "the public problem must not leak agent IDs")
	assert.NotContains(t, rr.Body.String(), "fa-pillar", "the public problem must not leak domain names")
}
