package mcp

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestToolGetLinksValidation covers the input contract of sage_get_links: the
// tool must reject a missing, empty, or all-blank memory_ids list before making
// any network call, so a malformed request never reaches the REST endpoint.
func TestToolGetLinksValidation(t *testing.T) {
	// A backend that fails the test if it is ever called — validation must
	// short-circuit before any request is sent.
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/links", func(w http.ResponseWriter, r *http.Request) {
		t.Fatalf("validation failure must not reach the REST endpoint")
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer(ts.URL, priv)

	cases := []struct {
		name   string
		params map[string]any
	}{
		{"missing", map[string]any{}},
		{"not-an-array", map[string]any{"memory_ids": "mem-a"}},
		{"empty-array", map[string]any{"memory_ids": []any{}}},
		{"all-blank-or-nonstring", map[string]any{"memory_ids": []any{"", 123, nil}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := server.toolGetLinks(context.Background(), tc.params)
			require.Error(t, err, "invalid memory_ids must be rejected")
		})
	}
}

// TestToolGetLinksSignedRequestAndDecode proves the happy path end to end: the
// tool POSTs a signed request to /v1/memory/links carrying exactly the cleaned
// memory_ids, and decodes the returned links into its result — not merely that
// the tool is registered.
func TestToolGetLinksSignedRequestAndDecode(t *testing.T) {
	var gotMethod, gotPath, gotSig, gotAgent string
	var gotBody struct {
		MemoryIDs []string `json:"memory_ids"`
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/memory/links", func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotSig = r.Header.Get("X-Signature")
		gotAgent = r.Header.Get("X-Agent-ID")
		raw, _ := io.ReadAll(r.Body)
		require.NoError(t, json.Unmarshal(raw, &gotBody))
		_ = json.NewEncoder(w).Encode(map[string]any{"links": []map[string]string{
			{"source_id": "mem-a", "target_id": "mem-b", "link_type": "supersedes"},
		}})
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	server := NewServer(ts.URL, priv)

	// Blank / non-string entries are dropped before the request is built.
	result, err := server.toolGetLinks(context.Background(),
		map[string]any{"memory_ids": []any{"mem-a", "", "mem-b", 7}})
	require.NoError(t, err)

	assert.Equal(t, http.MethodPost, gotMethod, "must POST")
	assert.Equal(t, "/v1/memory/links", gotPath, "must target the links read endpoint")
	assert.NotEmpty(t, gotSig, "the request must carry a signature")
	assert.NotEmpty(t, gotAgent, "the request must carry the signing agent identity")
	assert.Equal(t, []string{"mem-a", "mem-b"}, gotBody.MemoryIDs,
		"only the cleaned, non-empty string ids must be sent")

	response, ok := result.(map[string]any)
	require.True(t, ok, "result must be a decodable object")
	links, ok := response["links"].([]map[string]string)
	require.True(t, ok, "result must carry the decoded links")
	require.Len(t, links, 1)
	assert.Equal(t, "mem-a", links[0]["source_id"])
	assert.Equal(t, "mem-b", links[0]["target_id"])
	assert.Equal(t, "supersedes", links[0]["link_type"])
}
