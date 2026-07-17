package rest

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
)

// v11 real-TOTP JOIN ceremony - the LOCAL operator half. These endpoints drive
// the guided guest/host wizards. They are node-operator-only (requireNodeOperator)
// and off-consensus: the only on-chain writes are the two operators' own tx-33
// CrossFedSet broadcasts, each fired inside the federation Manager only after its
// human confirmation. The peer-facing /fed/v1/join/* routes live on the mTLS
// federation listener; these /v1/federation/join/* routes are the operator's
// localhost control surface.

// federationEnabled guards every join endpoint: a node with no wired transport
// (no chain id / agent key) returns 501, matching handleCrossFedSet.
func (s *Server) federationJoinReady(w http.ResponseWriter, r *http.Request) bool {
	if s.federation == nil {
		writeProblem(w, http.StatusNotImplemented, "Federation disabled", "The federation transport is not wired on this node.")
		return false
	}
	return s.requireNodeOperator(w, r)
}

// --- HOST side --------------------------------------------------------------

// HostCreateBody is POST /v1/federation/join/host/create.
type HostCreateBody struct {
	// Endpoint is this host's externally reachable federation listener URL
	// (https://host:8444) that the guest will connect back to.
	Endpoint string `json:"endpoint"`
}

// handleJoinHostCreate opens a join session + generates the enrollment QR (H1).
func (s *Server) handleJoinHostCreate(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	var body HostCreateBody
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	res, err := s.federation.HostCreate(body.Endpoint)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Could not open connection", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// HostScanReturnBody is POST /v1/federation/join/host/scan-return.
type HostScanReturnBody struct {
	SessionID string `json:"session_id"`
	ReturnURI string `json:"return_uri"` // the guest's scanned pin-only return QR
}

// handleJoinHostScanReturn records the scanned guest pin (the anchor).
func (s *Server) handleJoinHostScanReturn(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	var body HostScanReturnBody
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	if err := s.federation.HostScanReturn(body.SessionID, body.ReturnURI); err != nil {
		writeProblem(w, http.StatusBadRequest, "Could not read their code", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": body.SessionID, "status": "scanned"})
}

// handleJoinHostStatus returns the host wizard poll view.
func (s *Server) handleJoinHostStatus(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	view, err := s.federation.HostSessionStatus(chi.URLParam(r, "session_id"))
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Status error", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, view)
}

// HostApproveBody is POST /v1/federation/join/host/{session_id}/approve.
type HostApproveBody struct {
	TypedCode      string   `json:"typed_code"` // the code the host heard the guest read (CODE_G)
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	Mode           string   `json:"mode"`
	Direction      string   `json:"direction"`
}

// handleJoinHostApprove is approval #1: verify the heard code, set the grant,
// freeze E (H4/H5).
func (s *Server) handleJoinHostApprove(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	var body HostApproveBody
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	err := s.federation.HostApprove(chi.URLParam(r, "session_id"), body.TypedCode, federation.ScopeWire{
		MaxClearance:   body.MaxClearance,
		AllowedDomains: body.AllowedDomains,
		Mode:           body.Mode,
		Direction:      body.Direction,
	})
	if err != nil {
		writeProblem(w, http.StatusUnprocessableEntity, "Not approved", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": chi.URLParam(r, "session_id"), "status": "approved"})
}

// handleJoinHostAbort burns a session (H4 "No" / ignore).
func (s *Server) handleJoinHostAbort(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	if err := s.federation.HostAbort(chi.URLParam(r, "session_id")); err != nil {
		if errors.Is(err, federation.ErrJoinSessionNotFound) {
			writeProblem(w, http.StatusNotFound, "Not found", "This connection setup no longer exists.")
		} else {
			writeProblem(w, http.StatusConflict, "Confirmation in progress", "This connection is already being confirmed. Check Federation before revoking it.")
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": chi.URLParam(r, "session_id"), "status": "aborted"})
}

// --- GUEST side -------------------------------------------------------------

// GuestScanBody is POST /v1/federation/join/guest/scan.
type GuestScanBody struct {
	URI      string `json:"uri"`      // the scanned host enrollment QR (otpauth://…)
	Endpoint string `json:"endpoint"` // this guest's externally reachable federation URL
}

// handleJoinGuestScan validates a scanned host QR + fetches/pins the host CA.
func (s *Server) handleJoinGuestScan(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	var body GuestScanBody
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	ctx, cancel := contextWithFedTimeout(r)
	defer cancel()
	res, err := s.federation.GuestScan(ctx, body.URI, body.Endpoint)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Could not read their code", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// GuestRequestBody is POST /v1/federation/join/guest/request.
type GuestRequestBody struct {
	SessionID      string   `json:"session_id"`
	Endpoint       string   `json:"endpoint"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	Mode           string   `json:"mode"`
	Direction      string   `json:"direction"`
}

// handleJoinGuestRequest fires /fed/v1/join/request and returns the codes.
func (s *Server) handleJoinGuestRequest(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	var body GuestRequestBody
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	ctx, cancel := contextWithFedTimeout(r)
	defer cancel()
	res, err := s.federation.GuestRequest(ctx, body.SessionID, body.Endpoint, federation.ScopeWire{
		MaxClearance:   body.MaxClearance,
		AllowedDomains: body.AllowedDomains,
		Mode:           body.Mode,
		Direction:      body.Direction,
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "Could not reach the host", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, res)
}

// GuestConfirmBody is POST /v1/federation/join/guest/confirm. host_scope is what
// the guest polled from the host's /fed/v1/join/status once it approved.
type GuestConfirmBody struct {
	SessionID string `json:"session_id"`
	Endpoint  string `json:"endpoint"`
	HostScope struct {
		MaxClearance   int      `json:"max_clearance"`
		AllowedDomains []string `json:"allowed_domains"`
		Mode           string   `json:"mode"`
		Direction      string   `json:"direction"`
	} `json:"host_scope"`
}

// handleJoinGuestConfirm is approval #2: broadcast the guest tx-33 + tell the
// host to activate.
func (s *Server) handleJoinGuestConfirm(w http.ResponseWriter, r *http.Request) {
	if !s.federationJoinReady(w, r) {
		return
	}
	var body GuestConfirmBody
	if err := decodeJSON(r, &body); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	ctx, cancel := contextWithFedTimeout(r)
	defer cancel()
	txHash, err := s.federation.GuestConfirm(ctx, body.SessionID, body.Endpoint, federation.ScopeWire{
		MaxClearance:   body.HostScope.MaxClearance,
		AllowedDomains: body.HostScope.AllowedDomains,
		Mode:           body.HostScope.Mode,
		Direction:      body.HostScope.Direction,
	})
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "Connection not finished", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"session_id": body.SessionID, "status": "active", "tx_hash": txHash})
}
