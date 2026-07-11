package web

// Host side of the LAN node-join ceremony (Phase 5b-3). The operator drives it
// from CEREBRUM; a joining guest reaches the host over the LAN through a
// TEMPORARY listener that only exists while a pairing is active.
//
// Flow:
//   1. operator POST /host/start {lan_ip}  -> host builds the (secret-free)
//      bundle, mints a session + secret S, starts the temp LAN listener, and
//      returns a pairing token (shown as a QR + code). S rides ONLY in the token.
//   2. guest POST /pair/hello {proof of S, node id, nonce} -> host verifies the
//      proof, computes the 6-digit SAS, state=connected. Guest shows its SAS.
//   3. operator compares the two SAS codes and POST /host/approve -> approved.
//   4. guest POST /pair/bundle {proof of S} -> host returns the bundle AES-GCM
//      encrypted under S. state=consumed, listener torn down.
//
// The bundle carries the chain genesis (effectively the join credential), so it
// is encrypted under S — a network attacker who never scanned the QR cannot
// read it even though the listener speaks plain HTTP on the LAN.

import (
	"encoding/json"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/pairing"
)

const (
	joinSessionTTL     = 10 * time.Minute
	joinStateWaiting   = "waiting"
	joinStateConnected = "connected"
	joinStateApproved  = "approved"
	joinStateConsumed  = "consumed"
	joinStateAborted   = "aborted"
)

// joinSession is a single in-flight pairing. A node pairs one guest at a time.
type joinSession struct {
	id          string
	secret      []byte
	lanIP       string
	port        int
	bundleJSON  []byte
	state       string
	guestName   string
	guestNodeID string
	guestNonce  string
	sas         string
	expiresAt   time.Time
	stopFn      func()
}

// NetworkJoinStore holds the one active pairing session and a per-IP rate
// limiter for the guest-facing endpoints.
type NetworkJoinStore struct {
	mu      sync.Mutex
	active  *joinSession
	limiter redeemRateLimiter
}

// NewNetworkJoinStore creates an empty store.
func NewNetworkJoinStore() *NetworkJoinStore {
	return &NetworkJoinStore{}
}

// abortLocked tears down the active session (stops its listener). Caller holds
// mu. The listener Shutdown runs in a goroutine so we never block the store
// mutex — critical because this can be reached from inside a handler running on
// that very listener (a synchronous Shutdown would then deadlock/stall).
func (s *NetworkJoinStore) abortLocked() {
	if s.active != nil {
		if s.active.stopFn != nil {
			stop := s.active.stopFn
			go stop()
		}
		s.active = nil
	}
}

// throttleFail applies the brute-force limiter to a FAILED request (bad session
// or bad proof) and writes either 429 or the given error. Successful,
// proof-verified requests never call this, so a legitimate guest's long poll
// can never exhaust the limiter — only unauthorized probes do.
func (h *DashboardHandler) throttleFail(ip string, w http.ResponseWriter, status int, msg string) {
	if !h.NetworkJoin.limiter.allow(ip) {
		writeError(w, http.StatusTooManyRequests, "too many attempts")
		return
	}
	writeError(w, status, msg)
}

// reap tears the active session down if it is still the given session (armed at
// start via time.AfterFunc so an abandoned pairing can never leave the temp LAN
// listener bound past the TTL).
func (s *NetworkJoinStore) reap(sessionID string) {
	s.limiter.sweep() // bound the rate-limiter map while we're here
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active != nil && s.active.id == sessionID {
		s.abortLocked()
	}
}

// getValid returns the active session if it exists and hasn't expired; expired
// sessions are torn down. Caller holds mu.
func (s *NetworkJoinStore) getValidLocked() *joinSession {
	if s.active == nil {
		return nil
	}
	if time.Now().After(s.active.expiresAt) {
		s.abortLocked()
		return nil
	}
	return s.active
}

// ── operator (dashboard, authed) endpoints ─────────────────────────────────

// RegisterNetworkJoinHostRoutes wires the operator-facing ceremony routes. Must
// be called inside the authenticated dashboard route group.
func (h *DashboardHandler) RegisterNetworkJoinHostRoutes(r chi.Router) {
	r.Get("/v1/dashboard/network/join/host/interfaces", h.handleJoinHostInterfaces)
	r.Post("/v1/dashboard/network/mode/enable", h.handleEnableNetworkMode)
	r.Post("/v1/dashboard/network/join/host/start", h.handleJoinHostStart)
	r.Get("/v1/dashboard/network/join/host/status", h.handleJoinHostStatus)
	r.Post("/v1/dashboard/network/join/host/approve", h.handleJoinHostApprove)
	r.Post("/v1/dashboard/network/join/host/abort", h.handleJoinHostAbort)
}

// handleJoinHostInterfaces lists this host's candidate LAN addresses so the
// operator can pick the one the guest shares a network with (reuses the same
// candidate logic as remote-connect Flow 2), plus whether the node is already
// in network mode (LAN P2P) — if not, the frontend switches it first.
func (h *DashboardHandler) handleJoinHostInterfaces(w http.ResponseWriter, r *http.Request) {
	writeJSONResp(w, http.StatusOK, map[string]any{
		"candidates":   directIPv4Candidates(),
		"network_mode": h.QuorumEnabled,
	})
}

// handleEnableNetworkMode flips the node to network/quorum mode (LAN P2P) and
// restarts so the new P2P bind takes effect. Non-destructive: the chain and all
// memories are preserved. The operator re-unlocks the vault after the restart.
func (h *DashboardHandler) handleEnableNetworkMode(w http.ResponseWriter, r *http.Request) {
	if h.SetNetworkMode == nil {
		writeError(w, http.StatusServiceUnavailable, "network mode not available on this node")
		return
	}
	if h.QuorumEnabled {
		writeJSONResp(w, http.StatusOK, map[string]any{"ok": true, "already": true})
		return
	}
	if err := h.SetNetworkMode(true); err != nil {
		writeError(w, http.StatusInternalServerError, "enable network mode: "+err.Error())
		return
	}
	if !restartInProcessSupported() || h.RequestRestart == nil {
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "restart_required": true,
			"message": "Network mode is saved. Fully quit SAGE and open it again to apply it.",
		})
		return
	}
	if err := h.RequestRestart(); err != nil {
		_ = h.SetNetworkMode(false)
		writeError(w, http.StatusServiceUnavailable, "could not begin a clean restart; network mode was not changed: "+err.Error())
		return
	}
	writeJSONResp(w, http.StatusAccepted, map[string]any{"ok": true, "status": "draining", "message": "Switching to network mode and restarting cleanly…"})
}

// handleJoinHostStart begins a pairing: build the bundle, mint a session, start
// the temp LAN listener, return the pairing token.
func (h *DashboardHandler) handleJoinHostStart(w http.ResponseWriter, r *http.Request) {
	if h.NetworkJoin == nil || h.PairingListenerFn == nil || h.BuildJoinBundleFn == nil {
		writeError(w, http.StatusServiceUnavailable, "node join not available on this node")
		return
	}
	// The guest can only sync if this host runs P2P on the LAN, which requires
	// network mode. The frontend enables it (and restarts) first; guard here too.
	if !h.QuorumEnabled {
		writeError(w, http.StatusConflict, "enable network mode first (the node must run on the LAN to accept a peer)")
		return
	}
	var req struct {
		LANIP string `json:"lan_ip"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}
	lanIP := strings.TrimSpace(req.LANIP)
	if net.ParseIP(lanIP) == nil {
		writeError(w, http.StatusBadRequest, "lan_ip must be a valid IP address")
		return
	}
	// lan_ip must be one this host actually holds (don't let the operator paste
	// an arbitrary address into the token).
	if !hostHasIP(lanIP) {
		writeError(w, http.StatusBadRequest, "lan_ip is not an address of this machine")
		return
	}

	bundle, err := h.BuildJoinBundleFn(lanIP)
	if err != nil {
		writeError(w, http.StatusInternalServerError, "build join bundle: "+err.Error())
		return
	}
	secret, err := pairing.NewSecret()
	if err != nil {
		writeError(w, http.StatusInternalServerError, "mint secret: "+err.Error())
		return
	}
	sessionID, err := generatePairingCode() // reuse the crypto-random id generator
	if err != nil {
		writeError(w, http.StatusInternalServerError, "mint session: "+err.Error())
		return
	}

	h.NetworkJoin.mu.Lock()
	// Only one pairing at a time — tear down any previous one.
	h.NetworkJoin.abortLocked()

	stop, port, listenErr := h.PairingListenerFn(h.guestPairingHandler())
	if listenErr != nil {
		h.NetworkJoin.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "start pairing listener: "+listenErr.Error())
		return
	}
	sess := &joinSession{
		id:         sessionID,
		secret:     secret,
		lanIP:      lanIP,
		port:       port,
		bundleJSON: bundle,
		state:      joinStateWaiting,
		expiresAt:  time.Now().Add(joinSessionTTL),
		stopFn:     stop,
	}
	h.NetworkJoin.active = sess
	h.NetworkJoin.mu.Unlock()

	// Self-expiring teardown: even if the operator walks away and the guest
	// never fetches, the temp listener is reaped at the TTL.
	time.AfterFunc(joinSessionTTL, func() { h.NetworkJoin.reap(sessionID) })

	token, err := pairing.EncodeToken(net.JoinHostPort(lanIP, strconv.Itoa(port)), sessionID, secret)
	if err != nil {
		h.NetworkJoin.mu.Lock()
		h.NetworkJoin.abortLocked()
		h.NetworkJoin.mu.Unlock()
		writeError(w, http.StatusInternalServerError, "encode token: "+err.Error())
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"session_id": sessionID,
		"token":      token,
		"lan_ip":     lanIP,
		"port":       port,
		"expires_at": sess.expiresAt.UTC().Format(time.RFC3339),
	})
}

// handleJoinHostStatus reports the active pairing's state for the operator's
// poll loop (including the SAS to compare once the guest has connected).
func (h *DashboardHandler) handleJoinHostStatus(w http.ResponseWriter, r *http.Request) {
	if h.NetworkJoin == nil {
		writeError(w, http.StatusServiceUnavailable, "node join not available")
		return
	}
	h.NetworkJoin.mu.Lock()
	sess := h.NetworkJoin.getValidLocked()
	if sess == nil {
		h.NetworkJoin.mu.Unlock()
		writeJSONResp(w, http.StatusOK, map[string]any{"state": "none"})
		return
	}
	resp := map[string]any{
		"state":      sess.state,
		"lan_ip":     sess.lanIP,
		"expires_at": sess.expiresAt.UTC().Format(time.RFC3339),
	}
	if sess.state == joinStateConnected || sess.state == joinStateApproved {
		resp["sas"] = sess.sas
		resp["guest_name"] = sess.guestName
		resp["guest_node_id"] = sess.guestNodeID
	}
	h.NetworkJoin.mu.Unlock()
	writeJSONResp(w, http.StatusOK, resp)
}

// handleJoinHostApprove releases the bundle after the operator has compared the
// SAS. Only valid once a guest has connected.
func (h *DashboardHandler) handleJoinHostApprove(w http.ResponseWriter, r *http.Request) {
	if h.NetworkJoin == nil {
		writeError(w, http.StatusServiceUnavailable, "node join not available")
		return
	}
	h.NetworkJoin.mu.Lock()
	sess := h.NetworkJoin.getValidLocked()
	if sess == nil {
		h.NetworkJoin.mu.Unlock()
		writeError(w, http.StatusConflict, "no active pairing")
		return
	}
	if sess.state != joinStateConnected {
		state := sess.state
		h.NetworkJoin.mu.Unlock()
		writeError(w, http.StatusConflict, "cannot approve in state "+state+" (wait for the guest to connect)")
		return
	}
	sess.state = joinStateApproved
	h.NetworkJoin.mu.Unlock()
	writeJSONResp(w, http.StatusOK, map[string]any{"state": joinStateApproved})
}

// handleJoinHostAbort cancels the pairing and tears down the listener.
func (h *DashboardHandler) handleJoinHostAbort(w http.ResponseWriter, r *http.Request) {
	if h.NetworkJoin == nil {
		writeError(w, http.StatusServiceUnavailable, "node join not available")
		return
	}
	h.NetworkJoin.mu.Lock()
	h.NetworkJoin.abortLocked()
	h.NetworkJoin.mu.Unlock()
	writeJSONResp(w, http.StatusOK, map[string]any{"state": joinStateAborted})
}

// ── guest-facing (temp LAN listener) endpoints ─────────────────────────────

// guestPairingHandler returns the http.Handler served on the temporary LAN
// listener. It is intentionally minimal and gated entirely by proof-of-S.
func (h *DashboardHandler) guestPairingHandler() http.Handler {
	r := chi.NewRouter()
	r.Post("/pair/hello", h.handlePairHello)
	r.Post("/pair/bundle", h.handlePairBundle)
	return r
}

// handlePairHello: the guest proves possession of S and presents its identity;
// the host computes + stores the SAS and marks the session connected.
func (h *DashboardHandler) handlePairHello(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	var req struct {
		SessionID   string `json:"session_id"`
		GuestNodeID string `json:"guest_node_id"`
		GuestName   string `json:"guest_name"`
		GuestNonce  string `json:"guest_nonce"`
		Proof       string `json:"proof"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	h.NetworkJoin.mu.Lock()
	defer h.NetworkJoin.mu.Unlock()
	sess := h.NetworkJoin.getValidLocked()
	if sess == nil || sess.id != req.SessionID {
		h.throttleFail(ip, w, http.StatusNotFound, "no such pairing session")
		return
	}
	if req.GuestNonce == "" || req.GuestNodeID == "" {
		writeError(w, http.StatusBadRequest, "missing guest fields")
		return
	}
	// Proof-of-S gate. Only FAILED proofs are rate-limited (brute-force
	// defence); a valid proof is never throttled.
	expected := pairing.ProofHello(sess.secret, sess.id, req.GuestNonce, req.GuestNodeID)
	if !pairing.VerifyProof(expected, req.Proof) {
		h.throttleFail(ip, w, http.StatusForbidden, "invalid proof")
		return
	}
	if sess.state != joinStateWaiting && sess.state != joinStateConnected {
		writeError(w, http.StatusConflict, "pairing not accepting connections")
		return
	}
	sess.guestNodeID = req.GuestNodeID
	sess.guestName = strings.TrimSpace(req.GuestName)
	sess.guestNonce = req.GuestNonce
	sess.sas = pairing.SAS(sess.secret, sess.id, req.GuestNonce)
	sess.state = joinStateConnected

	writeJSONResp(w, http.StatusOK, map[string]any{"sas": sess.sas})
}

// handlePairBundle: after operator approval, return the S-encrypted bundle.
// Idempotent: the bundle is re-servable while the session is approved (it's
// encrypted under S, so re-serving harms nothing), which means an on-path
// attacker replaying the proof cannot "consume" a single-use bundle and lock
// the honest guest out. The listener is torn down by abort or the TTL reaper,
// not by a bundle fetch.
func (h *DashboardHandler) handlePairBundle(w http.ResponseWriter, r *http.Request) {
	ip := clientIP(r)
	var req struct {
		SessionID string `json:"session_id"`
		Proof     string `json:"proof"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<16)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid request body")
		return
	}

	h.NetworkJoin.mu.Lock()
	defer h.NetworkJoin.mu.Unlock()
	sess := h.NetworkJoin.getValidLocked()
	if sess == nil || sess.id != req.SessionID {
		h.throttleFail(ip, w, http.StatusNotFound, "no such pairing session")
		return
	}
	// Proof-gate BEFORE leaking the approval state, so an attacker without S
	// can't even probe whether a session was approved. Only FAILED proofs are
	// throttled; a valid-proof poll never consumes limiter budget.
	expected := pairing.ProofBundle(sess.secret, sess.id)
	if !pairing.VerifyProof(expected, req.Proof) {
		h.throttleFail(ip, w, http.StatusForbidden, "invalid proof")
		return
	}
	if sess.state != joinStateApproved {
		writeJSONResp(w, http.StatusAccepted, map[string]any{"state": sess.state}) // not yet approved — keep polling
		return
	}
	nonceB64, ctB64, encErr := pairing.EncryptBundle(sess.secret, sess.bundleJSON)
	if encErr != nil {
		writeError(w, http.StatusInternalServerError, "encrypt bundle")
		return
	}
	writeJSONResp(w, http.StatusOK, map[string]any{"nonce": nonceB64, "ciphertext": ctB64})
}

// hostHasIP reports whether ip is bound to one of this machine's interfaces.
func hostHasIP(ip string) bool {
	target := net.ParseIP(ip)
	if target == nil {
		return false
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return false
	}
	for _, a := range addrs {
		var ipp net.IP
		switch v := a.(type) {
		case *net.IPNet:
			ipp = v.IP
		case *net.IPAddr:
			ipp = v.IP
		}
		if ipp != nil && ipp.Equal(target) {
			return true
		}
	}
	return false
}

// clientIP returns the request's source IP (without the port) for a stable
// per-IP rate-limit key.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
