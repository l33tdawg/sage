package rest

import (
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/tx"
)

// ---------------------------------------------------------------------------
// Test fixtures
// ---------------------------------------------------------------------------

const (
	// 64-hex characters → 32 bytes, a valid Ed25519 agent ID shape.
	testNewOwnerID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testProposalID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
)

// newCometMock returns an httptest.Server that mimics CometBFT's
// /broadcast_tx_commit shape with the supplied response. Keeps each test
// focused on the REST surface, not chain machinery.
func newCometMock(t *testing.T, body string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(body))
	}))
}

// ---------------------------------------------------------------------------
// Happy path
// ---------------------------------------------------------------------------

// TestDomainReassign_HappyPath: the REST handler turns a well-formed
// request into a TxTypeDomainReassign broadcast and surfaces the
// purged-grants count parsed from the ABCI success Log.
//
// The gov_propose -> vote 3/4 -> execute pre-conditions are exercised
// end-to-end in the ABCI integration tests; here we mock CometBFT
// returning the success Log that processDomainReassign emits when those
// pre-conditions are satisfied. Pinning the Log-parsing contract is the
// point — if the ABCI handler ever changes the format, this test breaks.
func TestDomainReassign_HappyPath(t *testing.T) {
	successLog := `domain reassigned: pipeline.failures.boot2root -> ` + testNewOwnerID + ` (purged 7 grants, open_to_shared=false)`
	cometMock := newCometMock(t, `{
		"result": {
			"check_tx":  {"code": 0, "log": ""},
			"tx_result": {"code": 0, "data": "", "log": "`+successLog+`"},
			"hash": "DEADBEEFCAFEBABE",
			"height": "42"
		}
	}`)
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:       "pipeline.failures.boot2root",
		NewOwnerID:   testNewOwnerID,
		ParentDomain: "pipeline.failures",
		ProposalID:   testProposalID,
		OpenToShared: false,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())

	var resp DomainReassignResponse
	require.NoError(t, json.Unmarshal(rr.Body.Bytes(), &resp))
	assert.Equal(t, "DEADBEEFCAFEBABE", resp.TxHash)
	assert.Equal(t, 7, resp.PurgedGrants, "purged_grants must be parsed from the ABCI success Log")
}

// ---------------------------------------------------------------------------
// Bad request validation — 400s, never reaches CometBFT.
// ---------------------------------------------------------------------------

func TestDomainReassign_BadRequest_MissingDomain(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	body, _ := json.Marshal(DomainReassignRequest{
		NewOwnerID: testNewOwnerID,
		ProposalID: testProposalID,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code, "body=%s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "domain")
}

func TestDomainReassign_BadRequest_MalformedHexOwner(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:     "pipeline.failures.boot2root",
		NewOwnerID: "not-hex-at-all-zzzz",
		ProposalID: testProposalID,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "new_owner_id")
}

func TestDomainReassign_BadRequest_WrongLengthOwner(t *testing.T) {
	// Hex-valid but only 30 bytes — not a 32-byte Ed25519 agent ID.
	srv, _, _ := newTestServer(t, "")

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:     "pipeline.failures.boot2root",
		NewOwnerID: "aabbccddeeff" + strings.Repeat("00", 24), // 30 bytes total
		ProposalID: testProposalID,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "new_owner_id")
}

func TestDomainReassign_BadRequest_MissingProposalID(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:     "pipeline.failures.boot2root",
		NewOwnerID: testNewOwnerID,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "proposal_id")
}

// ---------------------------------------------------------------------------
// FinalizeBlock rejection — upstream Log surfaced as 403 detail.
// ---------------------------------------------------------------------------

// TestDomainReassign_FinalizeBlockRejection_NonexistentProposal: the
// upstream ABCI handler returns Code 81 "proposal not found: <id>" when
// the linked proposal doesn't exist on chain. The REST handler must
// surface that Log verbatim so the caller knows what to fix — abstracting
// it to "request rejected" (the default broadcastErrorPublic behaviour)
// would leak the specific failure into a server-log-only diagnostic and
// force ops to grep through node logs to see why their admin tx failed.
func TestDomainReassign_FinalizeBlockRejection_NonexistentProposal(t *testing.T) {
	cometMock := newCometMock(t, `{
		"result": {
			"check_tx":  {"code": 0, "log": ""},
			"tx_result": {"code": 81, "data": "", "log": "proposal not found: `+testProposalID+`"},
			"hash": "REJECTED",
			"height": "0"
		}
	}`)
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:     "pipeline.failures.boot2root",
		NewOwnerID: testNewOwnerID,
		ProposalID: testProposalID,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusForbidden, rr.Code, "body=%s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "proposal not found",
		"upstream FinalizeBlock Log must be surfaced verbatim — operators need the actual rejection reason")
	assert.Contains(t, rr.Body.String(), testProposalID,
		"proposal ID must be carried through so the caller can correlate")
}

// ---------------------------------------------------------------------------
// Pre-fork rejection — chain hasn't activated v8 yet.
// ---------------------------------------------------------------------------

// TestDomainReassign_PreFork_UnknownTxType: pre-v8 CheckTx returns Code 10
// "unknown tx type" — the wire surface needs to translate that to a 403
// (not 400) because the body is well-formed, the capability just hasn't
// been authorised on this chain yet. Mirrors the ABCI handler's pre-fork
// gate symmetry.
func TestDomainReassign_PreFork_UnknownTxType(t *testing.T) {
	cometMock := newCometMock(t, `{
		"result": {
			"check_tx":  {"code": 10, "log": "unknown tx type"},
			"tx_result": {"code": 0, "data": "", "log": ""},
			"hash": "",
			"height": "0"
		}
	}`)
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:     "pipeline.failures.boot2root",
		NewOwnerID: testNewOwnerID,
		ProposalID: testProposalID,
	})
	req, _ := signedRequest(t, http.MethodPost, "/v1/domain/reassign", body)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusForbidden, rr.Code, "body=%s", rr.Body.String())
	assert.Contains(t, rr.Body.String(), "unknown tx type")
}

// ---------------------------------------------------------------------------
// Unauthorized — no signature headers.
// ---------------------------------------------------------------------------

func TestDomainReassign_Unauthorized_NoSignature(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	body, _ := json.Marshal(DomainReassignRequest{
		Domain:     "pipeline.failures.boot2root",
		NewOwnerID: testNewOwnerID,
		ProposalID: testProposalID,
	})
	// Unsigned request — auth middleware must reject before the handler runs.
	req := httptest.NewRequest(http.MethodPost, "/v1/domain/reassign", strings.NewReader(string(body)))
	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusUnauthorized, rr.Code,
		"unauthenticated requests must never reach the chain layer")
}

// ---------------------------------------------------------------------------
// governance_propose payload round-trip — REST → GovPropose → codec.
// ---------------------------------------------------------------------------

// TestGovPropose_PayloadRoundtripIntoTx: a base64-encoded payload on the
// /v1/governance/propose body must arrive at the chain as the raw bytes —
// no double-encoding, no JSON re-wrap, no truncation. We capture the tx
// CometBFT receives, decode it, and assert the GovPropose.Payload matches
// the original bytes. This is the contract the Python SDK agent relies on
// for OpDomainReassign body-vs-proposal parity at execution time.
func TestGovPropose_PayloadRoundtripIntoTx(t *testing.T) {
	rawPayload, err := json.Marshal(map[string]any{
		"domain":         "pipeline.failures.boot2root",
		"new_owner_id":   testNewOwnerID,
		"parent_domain":  "pipeline.failures",
		"open_to_shared": false,
	})
	require.NoError(t, err)

	// Capture the raw tx bytes broadcast to CometBFT.
	var capturedTxHex string
	cometMock := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedTxHex = r.URL.Query().Get("tx")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{
			"result": {
				"check_tx":  {"code": 0, "log": ""},
				"tx_result": {"code": 0, "log": ""},
				"hash": "PROPOSEHASH",
				"height": "1"
			}
		}`))
	}))
	defer cometMock.Close()

	srv, _, _ := newTestServer(t, cometMock.URL)

	body, _ := json.Marshal(GovProposeRequest{
		Operation: "domain_reassign",
		TargetID:  "pipeline.failures.boot2root", // reused as proposal key per ComputeProposalID
		Reason:    "recovery: previous owner deprecated",
		Payload:   base64.StdEncoding.EncodeToString(rawPayload),
	})
	req, operatorID := signedRequest(t, http.MethodPost, "/v1/governance/propose", body)
	configureTestGovernanceGateway(t, srv, operatorID)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code, "body=%s", rr.Body.String())
	require.NotEmpty(t, capturedTxHex, "CometBFT mock must have received a tx")

	// Strip the "0x" CometBFT URL prefix and decode the wire bytes.
	txHex := strings.TrimPrefix(capturedTxHex, "0x")
	txBytes, err := hex.DecodeString(txHex)
	require.NoError(t, err, "tx hex must decode")

	parsed, err := tx.DecodeTx(txBytes)
	require.NoError(t, err, "broadcast tx must round-trip through the codec")
	require.Equal(t, tx.TxTypeGovPropose, parsed.Type)
	require.NotNil(t, parsed.GovPropose, "decoded tx must carry GovPropose payload")
	assert.Equal(t, tx.GovOpDomainReassign, parsed.GovPropose.Operation,
		"domain_reassign string must map to GovOpDomainReassign")
	assert.Equal(t, rawPayload, parsed.GovPropose.Payload,
		"payload bytes must round-trip verbatim — no double-encoding, no JSON re-wrap")
}

// TestGovPropose_InvalidBase64Payload: malformed base64 must 400 before
// any broadcast — protects callers from latent FinalizeBlock failures that
// would otherwise be hard to diagnose from a generic decode error.
func TestGovPropose_InvalidBase64Payload(t *testing.T) {
	srv, _, _ := newTestServer(t, "")

	body, _ := json.Marshal(GovProposeRequest{
		Operation: "domain_reassign",
		TargetID:  "pipeline.failures.boot2root",
		Reason:    "test",
		Payload:   "@@@not-base64@@@",
	})
	req, operatorID := signedRequest(t, http.MethodPost, "/v1/governance/propose", body)
	configureTestGovernanceGateway(t, srv, operatorID)

	rr := httptest.NewRecorder()
	srv.Router().ServeHTTP(rr, req)

	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "payload")
}
