package web

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
)

// v11 real-TOTP federation JOIN ceremony - the dashboard (cookie-authed) proxy
// for the guided guest/host wizards. The browser holds a dashboard session, not
// the node operator's signing key, so it cannot call the agent-signed REST
// endpoints; these routes run behind the dashboard auth middleware and drive the
// federation Manager directly. Everything is OFF-consensus (the only chain
// writes are the two operators' own tx-33/tx-34, fired inside the Manager after
// each human confirmation).

// FederationJoinDriver is the slice of the federation Manager the dashboard
// consumes. An interface so this package need not depend on the concrete type
// beyond its method set.
type FederationJoinDriver interface {
	HostCreate(hostEndpoint string) (*federation.HostCreateResult, error)
	HostScanReturn(sessionID, returnURI string) error
	HostSessionStatus(sessionID string) (*federation.HostSessionView, error)
	HostApprove(sessionID, typedCode string, grant federation.ScopeWire) error
	HostAbort(sessionID string)
	GuestScan(ctx context.Context, uri, guestEndpoint string) (*federation.GuestScanResult, error)
	GuestRequest(ctx context.Context, sessionID, guestEndpoint string, scope federation.ScopeWire) (*federation.GuestRequestResult, error)
	GuestPollStatus(ctx context.Context, sessionID string) (*federation.JoinStatusResp, error)
	GuestConfirm(ctx context.Context, sessionID, guestEndpoint string, hostScope federation.ScopeWire) (string, error)
	RevokeAgreement(remoteChainID string) (string, error)
	PeerStatus(ctx context.Context, remoteChainID string) (*federation.StatusResponse, error)
	LocalChainID() string
}

// SetFederation wires the JOIN ceremony driver (call before RegisterRoutes).
func (h *DashboardHandler) SetFederation(f FederationJoinDriver) { h.Federation = f }

const fedCallTimeout = 25 * time.Second

func fedWriteJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fedWriteErr(w http.ResponseWriter, status int, msg string) {
	fedWriteJSON(w, status, map[string]string{"error": msg})
}

// fedReady guards every route: 501 when the transport is not wired.
func (h *DashboardHandler) fedReady(w http.ResponseWriter) bool {
	if h.Federation == nil {
		fedWriteErr(w, http.StatusNotImplemented, "Federation is not enabled on this node.")
		return false
	}
	return true
}

// registerFederationRoutes mounts the JOIN proxy inside the dashboard's
// authenticated group (called from RegisterRoutes).
func (h *DashboardHandler) registerFederationRoutes(r chi.Router) {
	r.Get("/v1/dashboard/federation/connections", h.handleFedConnections)
	r.Post("/v1/dashboard/federation/connections/{chain_id}/revoke", h.handleFedRevoke)
	r.Get("/v1/dashboard/federation/connections/{chain_id}/status", h.handleFedPeerStatus)

	r.Get("/v1/dashboard/federation/lan-endpoint", h.handleFedLanEndpoint)

	r.Post("/v1/dashboard/federation/join/host/create", h.handleFedHostCreate)
	r.Post("/v1/dashboard/federation/join/host/scan-return", h.handleFedHostScanReturn)
	r.Get("/v1/dashboard/federation/join/host/{session_id}", h.handleFedHostStatus)
	r.Post("/v1/dashboard/federation/join/host/{session_id}/approve", h.handleFedHostApprove)
	r.Post("/v1/dashboard/federation/join/host/{session_id}/abort", h.handleFedHostAbort)

	r.Post("/v1/dashboard/federation/join/guest/scan", h.handleFedGuestScan)
	r.Post("/v1/dashboard/federation/join/guest/request", h.handleFedGuestRequest)
	r.Get("/v1/dashboard/federation/join/guest/{session_id}/status", h.handleFedGuestStatus)
	r.Post("/v1/dashboard/federation/join/guest/confirm", h.handleFedGuestConfirm)
}

// --- LAN endpoint suggestion (fix the localhost-in-join-code footgun) -------

// fedDefaultPort is the standard federation listener port (node.go default
// 0.0.0.0:8444, and the placeholder in every wizard field). An operator who
// runs a custom port edits the suggested endpoint in the wizard.
const fedDefaultPort = 8444

// FedLanCandidate is one address a JOINING peer on another machine could use to
// reach this node's federation listener.
type FedLanCandidate struct {
	Endpoint  string `json:"endpoint"`
	IP        string `json:"ip"`
	Iface     string `json:"iface"`
	IsPrivate bool   `json:"is_private"`
}

// handleFedLanEndpoint suggests the federation endpoint to advertise in a join
// code. The browser only knows location.hostname — usually "localhost" when the
// dashboard is opened on the same machine — which a DIFFERENT laptop can never
// route to (that was the v11.4.0 "dial tcp 127.0.0.1:8444: connection refused"
// on the guest: the host had baked localhost into its code). So we enumerate
// this host's routable LAN addresses server-side (physical-LAN-private first)
// and let the wizard default to the most-likely-reachable one.
func (h *DashboardHandler) handleFedLanEndpoint(w http.ResponseWriter, _ *http.Request) {
	cands := directIPv4Candidates()
	out := make([]FedLanCandidate, 0, len(cands))
	for _, c := range cands {
		out = append(out, FedLanCandidate{
			Endpoint:  fmt.Sprintf("https://%s:%d", c.IP, fedDefaultPort),
			IP:        c.IP,
			Iface:     c.Iface,
			IsPrivate: c.IsPrivate,
		})
	}
	suggested := ""
	if len(out) > 0 {
		suggested = out[0].Endpoint // ranked: physical-LAN-private wins
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{
		"port":               fedDefaultPort,
		"suggested_endpoint": suggested,
		"candidates":         out,
	})
}

// --- Connections list / revoke / status ------------------------------------

// FedConnection is one cross_fed agreement for the Connections view.
type FedConnection struct {
	RemoteChainID  string   `json:"remote_chain_id"`
	Endpoint       string   `json:"endpoint"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	Status         string   `json:"status"`
	Expired        bool     `json:"expired"`
}

func (h *DashboardHandler) handleFedConnections(w http.ResponseWriter, _ *http.Request) {
	if !h.fedReady(w) {
		return
	}
	out := map[string]any{"local_chain_id": h.Federation.LocalChainID(), "connections": []FedConnection{}}
	if h.BadgerStore != nil {
		records, err := h.BadgerStore.ListCrossFed()
		if err != nil {
			fedWriteErr(w, http.StatusInternalServerError, "Failed to list connections.")
			return
		}
		now := time.Now().Unix()
		conns := make([]FedConnection, 0, len(records))
		for _, rec := range records {
			conns = append(conns, FedConnection{
				RemoteChainID:  rec.RemoteChainID,
				Endpoint:       rec.Endpoint,
				MaxClearance:   int(rec.MaxClearance),
				AllowedDomains: rec.AllowedDomains,
				Status:         rec.Status,
				Expired:        rec.ExpiresAt != 0 && now >= rec.ExpiresAt,
			})
		}
		out["connections"] = conns
	}
	fedWriteJSON(w, http.StatusOK, out)
}

func (h *DashboardHandler) handleFedRevoke(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	chain := chi.URLParam(r, "chain_id")
	hash, err := h.Federation.RevokeAgreement(chain)
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"remote_chain_id": chain, "status": "revoked", "tx_hash": hash})
}

func (h *DashboardHandler) handleFedPeerStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	chain := chi.URLParam(r, "chain_id")
	st, err := h.Federation.PeerStatus(ctx, chain)
	if err != nil {
		fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "reachable": false, "error": err.Error()})
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]any{"remote_chain_id": chain, "reachable": true, "peer_time": st.Time})
}

// --- Host wizard ------------------------------------------------------------

func (h *DashboardHandler) handleFedHostCreate(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		Endpoint string `json:"endpoint"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	res, err := h.Federation.HostCreate(body.Endpoint)
	if err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, res)
}

func (h *DashboardHandler) handleFedHostScanReturn(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		SessionID string `json:"session_id"`
		ReturnURI string `json:"return_uri"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	if err := h.Federation.HostScanReturn(body.SessionID, body.ReturnURI); err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"session_id": body.SessionID, "status": "scanned"})
}

func (h *DashboardHandler) handleFedHostStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	view, err := h.Federation.HostSessionStatus(chi.URLParam(r, "session_id"))
	if err != nil {
		fedWriteErr(w, http.StatusInternalServerError, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, view)
}

func (h *DashboardHandler) handleFedHostApprove(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		TypedCode      string   `json:"typed_code"`
		MaxClearance   int      `json:"max_clearance"`
		AllowedDomains []string `json:"allowed_domains"`
		Mode           string   `json:"mode"`
		Direction      string   `json:"direction"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	err := h.Federation.HostApprove(chi.URLParam(r, "session_id"), body.TypedCode, federation.ScopeWire{
		MaxClearance:   body.MaxClearance,
		AllowedDomains: body.AllowedDomains,
		Mode:           body.Mode,
		Direction:      body.Direction,
	})
	if err != nil {
		fedWriteErr(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"status": "approved"})
}

func (h *DashboardHandler) handleFedHostAbort(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	h.Federation.HostAbort(chi.URLParam(r, "session_id"))
	fedWriteJSON(w, http.StatusOK, map[string]string{"status": "aborted"})
}

// --- Guest wizard -----------------------------------------------------------

func (h *DashboardHandler) handleFedGuestScan(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		URI      string `json:"uri"`
		Endpoint string `json:"endpoint"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	res, err := h.Federation.GuestScan(ctx, body.URI, body.Endpoint)
	if err != nil {
		fedWriteErr(w, http.StatusBadRequest, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, res)
}

func (h *DashboardHandler) handleFedGuestRequest(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		SessionID      string   `json:"session_id"`
		Endpoint       string   `json:"endpoint"`
		MaxClearance   int      `json:"max_clearance"`
		AllowedDomains []string `json:"allowed_domains"`
		Mode           string   `json:"mode"`
		Direction      string   `json:"direction"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	res, err := h.Federation.GuestRequest(ctx, body.SessionID, body.Endpoint, federation.ScopeWire{
		MaxClearance:   body.MaxClearance,
		AllowedDomains: body.AllowedDomains,
		Mode:           body.Mode,
		Direction:      body.Direction,
	})
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, res)
}

func (h *DashboardHandler) handleFedGuestStatus(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	resp, err := h.Federation.GuestPollStatus(ctx, chi.URLParam(r, "session_id"))
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, resp)
}

func (h *DashboardHandler) handleFedGuestConfirm(w http.ResponseWriter, r *http.Request) {
	if !h.fedReady(w) {
		return
	}
	var body struct {
		SessionID string `json:"session_id"`
		Endpoint  string `json:"endpoint"`
		HostScope struct {
			MaxClearance   int      `json:"max_clearance"`
			AllowedDomains []string `json:"allowed_domains"`
			Mode           string   `json:"mode"`
			Direction      string   `json:"direction"`
		} `json:"host_scope"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&body); err != nil {
		fedWriteErr(w, http.StatusBadRequest, "Invalid request.")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedCallTimeout)
	defer cancel()
	txHash, err := h.Federation.GuestConfirm(ctx, body.SessionID, body.Endpoint, federation.ScopeWire{
		MaxClearance:   body.HostScope.MaxClearance,
		AllowedDomains: body.HostScope.AllowedDomains,
		Mode:           body.HostScope.Mode,
		Direction:      body.HostScope.Direction,
	})
	if err != nil {
		fedWriteErr(w, http.StatusBadGateway, err.Error())
		return
	}
	fedWriteJSON(w, http.StatusOK, map[string]string{"session_id": body.SessionID, "status": "active", "tx_hash": txHash})
}
