package rest

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

func TestSubmitDerivedFromCanonicalizesAndValidates(t *testing.T) {
	hashA := strings.Repeat("a", 64)
	hashB := strings.Repeat("b", 64)
	refs, err := parseSubmitDerivedFrom([]SubmitMemoryParent{
		{SourceChainID: "sage", MemoryID: "parent-b", ContentHash: hashB},
		{SourceChainID: "sage", MemoryID: "parent-a", ContentHash: hashA},
	})
	require.NoError(t, err)
	require.Len(t, refs, 2)
	require.Equal(t, "parent-a", refs[0].MemoryID)
	require.Equal(t, "parent-b", refs[1].MemoryID)

	_, err = parseSubmitDerivedFrom([]SubmitMemoryParent{
		{SourceChainID: "sage", MemoryID: "duplicate", ContentHash: hashA},
		{SourceChainID: "sage", MemoryID: "duplicate", ContentHash: hashA},
	})
	require.Error(t, err)

	_, err = parseSubmitDerivedFrom([]SubmitMemoryParent{{
		SourceChainID: "sage",
		MemoryID:      "uppercase",
		ContentHash:   strings.Repeat("A", 64),
	}})
	require.ErrorContains(t, err, "lowercase")
}

func TestSubmitMemoryCarriesDerivedFromOnlyAfterAppV30(t *testing.T) {
	hash := strings.Repeat("a", 64)
	var captured *tx.MemorySubmit
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("tx") == "" {
			http.NotFound(w, r)
			return
		}
		raw, err := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, err)
		parsed, err := tx.DecodeTx(raw)
		require.NoError(t, err)
		require.NoError(t, tx.ActivateMemorySubmitExtensions(parsed, true, true))
		captured = parsed.MemorySubmit
		writeCometCommitFixture(t, w, r, 0, "", 0, "memory submitted", 1)
	}))
	defer cometMock.Close()

	body := []byte(`{
		"content":"lineage child",
		"memory_type":"fact",
		"domain_tag":"general",
		"confidence_score":0.9,
		"derived_from":[{
			"source_chain_id":"sage",
			"memory_id":"parent-a",
			"content_hash":"` + hash + `"
		}]
	}`)

	preV30, _, _ := newTestServer(t, cometMock.URL)
	req, _ := signedRequest(t, http.MethodPost, "/v1/memory/submit", body)
	recorder := httptest.NewRecorder()
	preV30.Router().ServeHTTP(recorder, req)
	require.Equal(t, http.StatusConflict, recorder.Code)
	require.Nil(t, captured)
	var problem map[string]any
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &problem))
	require.Equal(t, "https://sage.dev/errors/app-v30-required", problem["type"])

	postV30, _, _ := newTestServer(t, cometMock.URL)
	postV30.SetPostV30ForNextTxAccessor(func() bool { return true })
	req, _ = signedRequest(
		t,
		http.MethodPost,
		"/v1/memory/submit",
		bytes.Clone(body),
	)
	recorder = httptest.NewRecorder()
	postV30.Router().ServeHTTP(recorder, req)
	require.Equal(t, http.StatusCreated, recorder.Code, recorder.Body.String())
	require.NotNil(t, captured)
	require.Len(t, captured.DerivedFrom, 1)
	require.Equal(t, "parent-a", captured.DerivedFrom[0].MemoryID)
	require.Equal(t, hash, hex.EncodeToString(captured.DerivedFrom[0].ContentHash[:]))
}
