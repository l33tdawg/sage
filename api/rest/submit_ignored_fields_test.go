package rest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIgnoredJSONFields(t *testing.T) {
	known := jsonFieldNames(reflect.TypeOf(SubmitMemoryRequest{}))
	_, hasPrivate := known["ignoredFields"]
	require.False(t, hasPrivate, "the unexported report field must not count as a request field")

	assert.Nil(t, ignoredJSONFields([]byte(`{"content":"x","memory_type":"fact"}`), known))
	// encoding/json matches keys case-insensitively, so these were applied.
	assert.Nil(t, ignoredJSONFields([]byte(`{"Content":"x","DOMAIN_TAG":"general"}`), known))
	assert.Equal(t,
		[]string{"replacement_reason", "replaces_memory_id"},
		ignoredJSONFields([]byte(`{"content":"x","replaces_memory_id":"m1","replacement_reason":"r"}`), known))
	assert.Nil(t, ignoredJSONFields([]byte(`[1,2]`), known), "a non-object body reports nothing")

	var b strings.Builder
	b.WriteString(`{"content":"x"`)
	for i := 0; i < 40; i++ {
		b.WriteString(`,"zz_` + strings.Repeat("k", 100) + string(rune('a'+i%26)) + string(rune('a'+i/26)) + `":1`)
	}
	b.WriteString(`}`)
	got := ignoredJSONFields([]byte(b.String()), known)
	require.Len(t, got, maxIgnoredFieldsReported)
	for _, name := range got {
		assert.LessOrEqual(t, len(name), maxIgnoredFieldNameLen)
	}
}

func TestSubmitMemoryReportsIgnoredFields(t *testing.T) {
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		writeCometCommitFixture(t, w, r, 0, "", 0, "memory submitted", 1)
	}))
	defer cometMock.Close()
	srv, _, _ := newTestServer(t, cometMock.URL)

	submit := func(body string) (int, SubmitMemoryResponse, map[string]json.RawMessage) {
		req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", []byte(body))
		rr := httptest.NewRecorder()
		srv.Router().ServeHTTP(rr, req)
		var resp SubmitMemoryResponse
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
		var raw map[string]json.RawMessage
		require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &raw))
		return rr.Code, resp, raw
	}

	// A correction-shaped body: the memory is stored, and the response says the
	// replacement fields did nothing instead of dropping them silently.
	code, resp, _ := submit(`{
		"content": "The release cadence is monthly.",
		"memory_type": "fact",
		"domain_tag": "crypto",
		"confidence_score": 0.85,
		"replaces_memory_id": "old-memory",
		"replacement_reason": "cadence changed"
	}`)
	assert.Equal(t, http.StatusCreated, code)
	assert.NotEmpty(t, resp.MemoryID)
	assert.Equal(t, []string{"replacement_reason", "replaces_memory_id"}, resp.IgnoredFields)

	// A body with only known fields keeps the response unchanged.
	code, _, raw := submit(`{
		"content": "The release cadence is weekly.",
		"memory_type": "fact",
		"domain_tag": "crypto",
		"confidence_score": 0.85
	}`)
	assert.Equal(t, http.StatusCreated, code)
	_, present := raw["ignored_fields"]
	assert.False(t, present, "ignored_fields is omitted when nothing was ignored")
}
