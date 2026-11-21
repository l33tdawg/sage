package rest

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecall_NotTagsReachStoreCanonical(t *testing.T) {
	for _, tc := range []struct{ path, body string }{
		{"/v1/memory/query", `{"embedding":[0.1,0.2,0.3],"top_k":5,"tags":["insight"],"not_tags":["gen:2","gen:1","gen:2"]}`},
		{"/v1/memory/search", `{"query":"x","top_k":5,"tags":["insight"],"not_tags":["gen:2","gen:1","gen:2"]}`},
		{"/v1/memory/hybrid", `{"query":"x","embedding":[0.1,0.2,0.3],"top_k":5,"tags":["insight"],"not_tags":["gen:2","gen:1","gen:2"]}`},
	} {
		t.Run(tc.path, func(t *testing.T) {
			srv, memStore, _ := newTestServer(t, "")
			req, _ := signedRequest(t, http.MethodPost, tc.path, []byte(tc.body))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)

			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			assert.Equal(t, []string{"insight"}, memStore.lastQueryOpts.Tags)
			assert.Equal(t, []string{"gen:1", "gen:2"}, memStore.lastQueryOpts.NotTags,
				"not_tags must reach the store sorted and de-duplicated")
		})
	}
}

func TestRecall_NoNotTagsLeavesStoreUnfiltered(t *testing.T) {
	srv, memStore, _ := newTestServer(t, "")
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/query", []byte(`{"embedding":[0.1,0.2,0.3],"top_k":5}`))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Nil(t, memStore.lastQueryOpts.NotTags)
}

func TestRecall_NotTagsRefusedWhenFederatedOrInvalid(t *testing.T) {
	for _, tc := range []struct{ name, path, body string }{
		{"query federated", "/v1/memory/query", `{"embedding":[0.1,0.2,0.3],"domain_tag":"d","federated":true,"not_tags":["x"]}`},
		{"query chains", "/v1/memory/query", `{"embedding":[0.1,0.2,0.3],"domain_tag":"d","federate_chains":["c"],"not_tags":["x"]}`},
		{"search federated", "/v1/memory/search", `{"query":"x","domain_tag":"d","federated":true,"not_tags":["x"]}`},
		{"hybrid federated", "/v1/memory/hybrid", `{"query":"x","embedding":[0.1,0.2,0.3],"domain_tag":"d","federated":true,"not_tags":["x"]}`},
		{"padded tag", "/v1/memory/query", `{"embedding":[0.1,0.2,0.3],"not_tags":[" x"]}`},
		{"empty tag", "/v1/memory/search", `{"query":"x","not_tags":[""]}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			srv, memStore, _ := newTestServer(t, "")
			req, _ := signedRequest(t, http.MethodPost, tc.path, []byte(tc.body))
			rr := httptest.NewRecorder()
			srv.Router().ServeHTTP(rr, req)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), "not_tags")
			assert.Nil(t, memStore.lastQueryOpts.NotTags, "a refused request must not reach the store")
		})
	}
}

func TestRecallIndexStatus_NotTagsIsNotExactUniverse(t *testing.T) {
	req := QueryMemoryRequest{StatusFilter: "committed"}
	require.True(t, recallIndexStatusHasExactQueryUniverse(req))
	req.NotTags = []string{"x"}
	assert.False(t, recallIndexStatusHasExactQueryUniverse(req))
}
