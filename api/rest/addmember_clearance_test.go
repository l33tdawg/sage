package rest

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// These guard the org/dept add-member endpoints against the clearance-0
// escalation: an explicit clearance of 0 (ClearancePublic — a valid level that
// gates reads, internal/tx/types.go) must be carried verbatim into the
// broadcast tx, not silently coerced to INTERNAL (1). Only an OMITTED clearance
// falls back to the safe INTERNAL default. Same class as the agent-permission
// "Bug 2" fix; here a bare int couldn't tell "0" from "absent".

// newAddMemberCometMock returns a CometBFT stub that records the broadcast tx
// hex into *captured and acks Code 0.
func newAddMemberCometMock(t *testing.T, captured *string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*captured = r.URL.Query().Get("tx")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"result": map[string]interface{}{"code": 0, "hash": "ADDMEMBER_TX"},
		})
	}))
}

func decodeAddMemberTx(t *testing.T, capturedTxHex string) *tx.ParsedTx {
	t.Helper()
	require.NotEmpty(t, capturedTxHex, "broadcast should have been invoked")
	txBytes, err := hex.DecodeString(capturedTxHex[2:]) // strip 0x prefix
	require.NoError(t, err)
	parsed, err := tx.DecodeTx(txBytes)
	require.NoError(t, err)
	return parsed
}

const addMemberTarget = "1111111111111111111111111111111111111111111111111111111111111111"

func TestOrgAddMember_ExplicitPublicClearance_NotEscalated(t *testing.T) {
	var capturedTxHex string
	cometMock := newAddMemberCometMock(t, &capturedTxHex)
	defer cometMock.Close()
	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{"agent_id":"` + addMemberTarget + `","clearance":0}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/org/acme/member", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code, "body=%s", rr.Body.String())
	parsed := decodeAddMemberTx(t, capturedTxHex)
	require.NotNil(t, parsed.OrgAddMember)
	assert.Equal(t, tx.ClearancePublic, parsed.OrgAddMember.Clearance,
		"explicit clearance:0 (PUBLIC) must NOT be escalated to INTERNAL")
}

func TestOrgAddMember_OmittedClearance_DefaultsInternal(t *testing.T) {
	var capturedTxHex string
	cometMock := newAddMemberCometMock(t, &capturedTxHex)
	defer cometMock.Close()
	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{"agent_id":"` + addMemberTarget + `"}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/org/acme/member", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code, "body=%s", rr.Body.String())
	parsed := decodeAddMemberTx(t, capturedTxHex)
	require.NotNil(t, parsed.OrgAddMember)
	assert.Equal(t, tx.ClearanceInternal, parsed.OrgAddMember.Clearance,
		"omitted clearance must default to the safe INTERNAL level")
}

func TestDeptAddMember_ExplicitPublicClearance_NotEscalated(t *testing.T) {
	var capturedTxHex string
	cometMock := newAddMemberCometMock(t, &capturedTxHex)
	defer cometMock.Close()
	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{"agent_id":"` + addMemberTarget + `","clearance":0}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/org/acme/dept/eng/member", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code, "body=%s", rr.Body.String())
	parsed := decodeAddMemberTx(t, capturedTxHex)
	require.NotNil(t, parsed.DeptAddMember)
	assert.Equal(t, tx.ClearancePublic, parsed.DeptAddMember.Clearance,
		"explicit clearance:0 (PUBLIC) must NOT be escalated to INTERNAL")
}

func TestDeptAddMember_OmittedClearance_DefaultsInternal(t *testing.T) {
	var capturedTxHex string
	cometMock := newAddMemberCometMock(t, &capturedTxHex)
	defer cometMock.Close()
	srv, _, _ := newTestServer(t, cometMock.URL)

	body := []byte(`{"agent_id":"` + addMemberTarget + `"}`)
	req, _ := signedRequest(t, http.MethodPost, "/v1/org/acme/dept/eng/member", body)
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusCreated, rr.Code, "body=%s", rr.Body.String())
	parsed := decodeAddMemberTx(t, capturedTxHex)
	require.NotNil(t, parsed.DeptAddMember)
	assert.Equal(t, tx.ClearanceInternal, parsed.DeptAddMember.Clearance,
		"omitted clearance must default to the safe INTERNAL level")
}
