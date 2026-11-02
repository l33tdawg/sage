package rest

import (
	"bytes"
	"errors"
	"io"
	"net/http"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/tx"
)

// FunctionAuthorityRequest is the complete, proof-bound Tx41 action. Ordinary
// domain ownership, write grants and promoted Admin do not authorize this API.
type FunctionAuthorityRequest = tx.FunctionAuthority

// decodeFunctionAuthorityRequest closes the new control surface to unknown,
// duplicate, case-aliased, missing and null fields. The HTTP signature retains
// its original request bytes; the action codec binds all eight typed fields.
func decodeFunctionAuthorityRequest(r *http.Request) (*FunctionAuthorityRequest, error) {
	if r.Body == nil {
		return nil, errors.New("missing function authority request")
	}
	body, err := io.ReadAll(http.MaxBytesReader(nil, r.Body, tx.MaxFunctionAuthorityRequestSize))
	if err != nil {
		return nil, errors.New("function authority request exceeds its bound or cannot be read")
	}
	_ = r.Body.Close()
	r.Body = io.NopCloser(bytes.NewReader(body))
	return tx.DecodeFunctionAuthorityRequest(body)
}

// handleFunctionAuthority submits a normally signed, consensus-authorized
// action. Local checks are fail-fast only; ABCI rechecks the executing Root,
// app-v31 boundary, canonical policy, target and revision independently.
func (s *Server) handleFunctionAuthority(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		writeProblem(w, http.StatusMethodNotAllowed, "Method not allowed", "Use POST for function authority actions.")
		return
	}
	if !s.isPostV31ForNextTx() {
		writeProblem(w, http.StatusConflict, "Function authority inactive", "Function authority requires app-v31 activation.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable", "Current CEREBRUM Root state is unavailable.")
		return
	}
	root, err := s.badgerStore.GetAppV23Root()
	if err != nil || root == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable", "Current CEREBRUM Root state is unavailable.")
		return
	}
	if middleware.ContextAgentID(r.Context()) != root.CredentialID || !isAppV23LoopbackControlRequest(r) {
		writeProblem(w, http.StatusForbidden, "Local Root required", "Only the current CEREBRUM Root credential on localhost may designate functional authority.")
		return
	}
	request, err := decodeFunctionAuthorityRequest(r)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid function authority request", err.Error())
		return
	}
	if request.Scope != root.Scope {
		writeProblem(w, http.StatusForbidden, "Function authority scope mismatch", "The action must name the executing CEREBRUM Root scope.")
		return
	}
	parsed := &tx.ParsedTx{Type: tx.TxTypeFunctionAuthority, FunctionAuthority: request}
	s.embedAgentAuth(r.Context(), parsed)
	var hash string
	stage, err := s.submitConsensusTx(r.Context(), parsed, func(encoded []byte) error {
		var submitErr error
		hash, submitErr = s.broadcastTxCommit(encoded)
		return submitErr
	})
	if err != nil {
		s.writeConsensusTxError(w, stage, "function authority", err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "committed", "tx_hash": hash})
}
