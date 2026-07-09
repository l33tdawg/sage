package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/netguard"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/totp"
	"github.com/l33tdawg/sage/internal/tx"
)

// v11 real-TOTP JOIN ceremony - the pre-agreement route surface. These routes
// live on the SAME dedicated mTLS federation listener as /fed/v1/query|receipt
// but sit behind a SEPARATE middleware (joinAuth), because during a join there
// is NO active cross_fed agreement yet: peerAuth's ActiveAgreement gate would
// reject every request. Authentication here is by (a) the 64-bit session id
// carried in the QR, (b) the guest CA pin the host scanned off the return QR
// (the anchor), and (c) the per-session TLS-cert binding (RT-2/RT-5). NOTHING
// here is on the consensus path; the only chain writes are the two operators'
// own tx-33 CrossFedSet broadcasts, each fired only after its human confirmation.

const (
	// joinBodyCap bounds a join request body. A CA PEM (~1-2 KB) plus hex fields
	// fits comfortably; anything larger is rejected before parsing.
	joinBodyCap = 64 << 10
	// guestDraftTTL bounds how long a guest keeps the scanned seed in memory
	// between the QR scan and its tx-33.
	guestDraftTTL = 15 * time.Minute
	// joinCAFetchTimeout / joinCallTimeout bound the guest's outbound ceremony
	// calls to the host.
	joinCAFetchTimeout = 15 * time.Second
	joinCallTimeout    = 20 * time.Second
)

type joinCertSPKIKey struct{}

// ---------------------------------------------------------------------------
// Wire types (JSON over the mTLS listener)
// ---------------------------------------------------------------------------

// ScopeWire is a cross_fed scope as exchanged during a join (folded into the
// scope digest each side computes for E - RT-9).
type ScopeWire struct {
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	Mode           string   `json:"mode"`
	Direction      string   `json:"direction"`
}

func (s ScopeWire) digest() [32]byte {
	cl := s.MaxClearance
	if cl < 0 {
		cl = 0
	}
	if cl > 255 {
		cl = 255
	}
	return ScopeDigest(uint8(cl), s.AllowedDomains, s.Mode, s.Direction) // #nosec G115 -- clamped 0..255
}

// JoinRequestWire is POST /fed/v1/join/request (guest -> host). No secret seed
// is carried (the seed rode the QR); the nonce is freshness-only, exchanged in
// the clear (§2.4). The guest is authenticated by mTLS (its cert chains to a CA
// whose SPKI equals the scanned guest pin) - not by a body signature.
type JoinRequestWire struct {
	SessionID     string    `json:"session_id"`
	GuestChain    string    `json:"guest_chain"`
	GuestName     string    `json:"guest_name,omitempty"` // friendly label (cosmetic, unauthenticated)
	GuestAgentID  string    `json:"guest_agent_id"`       // hex ed25519 operator pub
	GuestNonce    string    `json:"guest_nonce"`          // hex 16B
	GuestPin      string    `json:"guest_pin"`            // hex 32B SPKI (must equal the scanned anchor)
	GuestCAPEM    string    `json:"guest_ca_pem"`
	GuestEndpoint string    `json:"guest_endpoint"`
	Scope         ScopeWire `json:"scope"` // guest's scope_G inputs
}

// JoinRequestResp is returned to the guest - enough to compute CODE_G/CODE_H.
type JoinRequestResp struct {
	HostChain    string `json:"host_chain"`
	HostAgentID  string `json:"host_agent_id"` // hex ed25519 operator pub
	HostNonce    string `json:"host_nonce"`    // hex 16B
	ConfirmStep  int64  `json:"confirm_step"`
	HostPin      string `json:"host_pin"` // hex 32B (echo; guest already holds it from the QR)
	HostEndpoint string `json:"host_endpoint"`
}

// JoinStatusResp reports state flags (no secrets). Once the host approves, it
// also carries the host's granted scope so the guest can compute scope_H + E.
type JoinStatusResp struct {
	State        string     `json:"state"`
	HostApproved bool       `json:"host_approved"`
	Aborted      bool       `json:"aborted"`
	Expired      bool       `json:"expired"`
	Active       bool       `json:"active"`
	HostScope    *ScopeWire `json:"host_scope,omitempty"`
	HostEndpoint string     `json:"host_endpoint,omitempty"`
}

// JoinConfirmWire is POST /fed/v1/join/confirm (guest -> host): the guest's
// approval-#2 signatures over the frozen attestation E (§2.4).
type JoinConfirmWire struct {
	SessionID   string `json:"session_id"`
	GuestSig    string `json:"guest_sig"`     // hex, ed25519 over E (tagEnrollSig)
	GuestAckSig string `json:"guest_ack_sig"` // hex, ed25519 over E (tagEnrollAck)
}

// JoinConfirmResp reports the host's activation.
type JoinConfirmResp struct {
	Status    string `json:"status"`
	HostChain string `json:"host_chain"`
	TxHash    string `json:"tx_hash,omitempty"`
}

// JoinCAResp serves the host's own CA PEM to a scanning guest. The guest
// validates it against the scanned x_sage_pin (SPKI) - the transport is not the
// authenticator, the pin is.
type JoinCAResp struct {
	ChainID  string `json:"chain_id"`
	HostName string `json:"host_name,omitempty"` // friendly label (cosmetic, unauthenticated)
	CAPEM    string `json:"ca_pem"`
}

// ---------------------------------------------------------------------------
// Route registration + joinAuth
// ---------------------------------------------------------------------------

// mountJoinRoutes attaches the join ceremony routes under joinAuth. Called from
// Router() so they share the listener but not peerAuth.
func (m *Manager) mountJoinRoutes(r chi.Router) {
	// Code-submitting / state-changing routes: strict per-source rate limit.
	r.Group(func(r chi.Router) {
		r.Use(m.joinAuth(false))
		r.Post("/fed/v1/join/request", m.handleJoinRequest)
		r.Post("/fed/v1/join/confirm", m.handleJoinConfirm)
	})
	// Read-only routes: generous rate limit so a bound guest's ~2s status poll
	// over a multi-minute ceremony is never throttled (the stuck-guest bug).
	r.Group(func(r chi.Router) {
		r.Use(m.joinAuth(true))
		r.Get("/fed/v1/join/ca", m.handleJoinCA)
		r.Get("/fed/v1/join/status", m.handleJoinStatus)
	})
}

// joinAuth is the pre-agreement middleware: it rate-limits on the TLS
// connection (never X-Forwarded-For, RT-3), caps the body, and threads the
// presented client-cert leaf SPKI into the request context for per-session
// binding. It does NOT require an active agreement (that is the whole point).
// readOnly selects the generous read limiter (status polling) vs. the strict
// code-submitting limiter.
func (m *Manager) joinAuth(readOnly bool) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
				httpError(w, http.StatusForbidden, "client certificate required")
				return
			}
			// RT-3: key the limiter on the direct TCP peer (this is a direct-connect
			// listener - no trusted proxy - so r.RemoteAddr is the real connection),
			// NOT any forwarded header.
			connKey := r.RemoteAddr
			if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
				connKey = host
			}
			allowed := m.joins.AllowConn(connKey, time.Now())
			if readOnly {
				allowed = m.joins.AllowConnRead(connKey, time.Now())
			}
			if !allowed {
				httpError(w, http.StatusTooManyRequests, "too many join attempts")
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, joinBodyCap)
			spki := SPKIFingerprint(r.TLS.PeerCertificates[0])
			ctx := context.WithValue(r.Context(), joinCertSPKIKey{}, spki)
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

func joinCertSPKI(ctx context.Context) []byte {
	b, _ := ctx.Value(joinCertSPKIKey{}).([]byte)
	return b
}

// ---------------------------------------------------------------------------
// Host-side route handlers
// ---------------------------------------------------------------------------

// handleJoinCA serves the host's own CA PEM to a guest that presents a live
// session id. The guest authenticates the PEM by the scanned pin, so this only
// needs to gate scanning noise (an open session must exist).
func (m *Manager) handleJoinCA(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session_id")
	if _, ok := m.joins.Get(sid, time.Now()); !ok {
		httpError(w, http.StatusNotFound, "no such join session")
		return
	}
	caPEM, err := m.ownCAPEM()
	if err != nil {
		httpError(w, http.StatusInternalServerError, "own CA unavailable")
		return
	}
	writeJSON(w, http.StatusOK, &JoinCAResp{ChainID: m.localChainID, HostName: sanitizeName(m.NetworkName()), CAPEM: string(caPEM)})
}

// handleJoinRequest binds the guest to the session, asserts the presented guest
// CA against the scanned anchor pin, stages (does not commit) the guest CA, and
// returns the host nonce + confirm step so both sides can compute the codes.
func (m *Manager) handleJoinRequest(w http.ResponseWriter, r *http.Request) {
	var req JoinRequestWire
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	now := time.Now()
	js, ok := m.joins.Get(req.SessionID, now)
	if !ok {
		httpError(w, http.StatusNotFound, "no such join session")
		return
	}
	if err := ValidateChainID(req.GuestChain); err != nil {
		httpError(w, http.StatusBadRequest, "invalid guest chain id")
		return
	}
	if req.GuestChain == m.localChainID {
		httpError(w, http.StatusBadRequest, "refusing self-federation")
		return
	}
	guestNonce, err := hex.DecodeString(req.GuestNonce)
	if err != nil || len(guestNonce) != 16 {
		httpError(w, http.StatusBadRequest, "invalid guest nonce")
		return
	}
	guestPin, err := hex.DecodeString(req.GuestPin)
	if err != nil || len(guestPin) != sha256.Size {
		httpError(w, http.StatusBadRequest, "invalid guest pin")
		return
	}
	guestAgentPub, err := hex.DecodeString(req.GuestAgentID)
	if err != nil || len(guestAgentPub) != ed25519.PublicKeySize {
		httpError(w, http.StatusBadRequest, "invalid guest agent id")
		return
	}
	if valErr := validateJoinEndpoint(req.GuestEndpoint); valErr != nil {
		httpError(w, http.StatusBadRequest, "invalid guest endpoint")
		return
	}
	// Stage the guest CA to a pending sidecar (committed only after the host's
	// tx-33 lands). requireChainCN inside StageRemoteCA binds the CA CN to the
	// claimed guest chain. The SPKI==scanned-pin assertion is the anchor and is
	// enforced under the store lock inside Request (against ExpectedGuestPin).
	pin, commitCA, rollbackCA, err := m.StageRemoteCA(req.GuestChain, []byte(req.GuestCAPEM))
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid guest CA")
		return
	}
	// Defense in depth: the body pin must equal the CA's real SPKI.
	if subtle.ConstantTimeCompare(pin, guestPin) != 1 {
		rollbackCA()
		httpError(w, http.StatusBadRequest, "guest pin does not match guest CA")
		return
	}
	// Verify the presented TLS client cert actually chains to this guest CA
	// (binds the transport identity to the CA the scanned pin authenticates).
	caCert, caErr := parseCACertPEM([]byte(req.GuestCAPEM))
	if caErr != nil {
		rollbackCA()
		httpError(w, http.StatusBadRequest, "invalid guest CA")
		return
	}
	rawCerts := make([][]byte, 0, len(r.TLS.PeerCertificates))
	for _, c := range r.TLS.PeerCertificates {
		rawCerts = append(rawCerts, c.Raw)
	}
	if vErr := verifyChainAgainstCA(rawCerts, caCert, x509.ExtKeyUsageClientAuth); vErr != nil {
		rollbackCA()
		httpError(w, http.StatusForbidden, "client certificate does not chain to the guest CA")
		return
	}

	bound, bErr := m.joins.Request(req.SessionID, now, GuestRequestInput{
		GuestChain:      req.GuestChain,
		GuestName:       sanitizeName(req.GuestName),
		GuestAgentPub:   guestAgentPub,
		GuestNonce:      guestNonce,
		GuestPin:        guestPin,
		GuestEndpoint:   req.GuestEndpoint,
		GuestScope:      req.Scope.digest(),
		CertSPKI:        joinCertSPKI(r.Context()),
		CommitGuestCA:   commitCA,
		RollbackGuestCA: rollbackCA,
	})
	if bErr != nil {
		rollbackCA()
		m.logger.Warn().Err(bErr).Str("session", shortID(req.SessionID)).Msg("join request rejected")
		httpError(w, http.StatusForbidden, bErr.Error())
		return
	}
	m.logger.Info().Str("guest", req.GuestChain).Str("session", shortID(req.SessionID)).Msg("join request bound")
	writeJSON(w, http.StatusOK, &JoinRequestResp{
		HostChain:    m.localChainID,
		HostAgentID:  hex.EncodeToString(m.agentPub),
		HostNonce:    hex.EncodeToString(js.HostNonce),
		ConfirmStep:  bound.ConfirmStep,
		HostPin:      hex.EncodeToString(mustOwnPin(m)),
		HostEndpoint: js.HostEndpoint,
	})
}

// handleJoinStatus returns state flags to the bound guest, plus the host's
// granted scope once approved (so the guest can compute E).
func (m *Manager) handleJoinStatus(w http.ResponseWriter, r *http.Request) {
	sid := r.URL.Query().Get("session_id")
	now := time.Now()
	js, ok := m.joins.Get(sid, now)
	if !ok {
		// Distinguish expired from unknown without leaking other sessions.
		writeJSON(w, http.StatusOK, &JoinStatusResp{State: JoinExpired, Expired: true})
		return
	}
	// RT-5: once a guest cert is bound, only that cert may read status.
	if len(js.BoundCertSPKI) > 0 && subtle.ConstantTimeCompare(js.BoundCertSPKI, joinCertSPKI(r.Context())) != 1 {
		httpError(w, http.StatusForbidden, "status from an unbound client certificate")
		return
	}
	resp := &JoinStatusResp{
		State:        js.State,
		HostApproved: js.Approved,
		Aborted:      js.State == JoinAborted,
		Expired:      js.State == JoinExpired,
		Active:       js.State == JoinActive,
	}
	if js.Approved {
		resp.HostScope = &ScopeWire{
			MaxClearance:   int(js.HostGrantClearance),
			AllowedDomains: js.HostGrantDomains,
			Mode:           js.HostGrantMode,
			Direction:      js.HostGrantDirection,
		}
		resp.HostEndpoint = js.HostEndpoint
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleJoinConfirm verifies the guest's approval-#2 signatures over the FROZEN
// E, then broadcasts the host's tx-33, commits the staged guest CA + seed, and
// marks the session ACTIVE. This is the host's half of the 2-of-2 gate.
func (m *Manager) handleJoinConfirm(w http.ResponseWriter, r *http.Request) {
	var req JoinConfirmWire
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	guestSig, err := hex.DecodeString(req.GuestSig)
	if err != nil || len(guestSig) != ed25519.SignatureSize {
		httpError(w, http.StatusBadRequest, "invalid guest signature")
		return
	}
	guestAckSig, err := hex.DecodeString(req.GuestAckSig)
	if err != nil || len(guestAckSig) != ed25519.SignatureSize {
		httpError(w, http.StatusBadRequest, "invalid guest ack signature")
		return
	}
	txHash, hostChain, err := m.hostConfirm(req.SessionID, joinCertSPKI(r.Context()), guestSig, guestAckSig)
	if err != nil {
		m.logger.Warn().Err(err).Str("session", shortID(req.SessionID)).Msg("join confirm rejected")
		httpError(w, http.StatusUnprocessableEntity, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, &JoinConfirmResp{Status: "active", HostChain: hostChain, TxHash: txHash})
}

// hostConfirm is the server-side confirm driver: verify frozen E, broadcast the
// host tx-33, commit CA + seed, activate.
func (m *Manager) hostConfirm(sessionID string, certSPKI, guestSig, guestAckSig []byte) (string, string, error) {
	// CheckConfirm verifies the guest's approval-#2 signatures over the frozen E
	// under the store lock; on success it hands us exclusive ownership of the
	// staged-CA closures and the session is in CONFIRMING.
	ctx, err := m.joins.CheckConfirm(sessionID, certSPKI, guestSig, guestAckSig, time.Now())
	if err != nil {
		return "", "", err
	}
	// Broadcast the host's own tx-33 CrossFedSet(remote=guest) with the operator
	// key. crossFedAuthorized re-checks authority on-chain.
	txHash, err := m.broadcastCrossFedSet(&tx.CrossFedTerms{
		RemoteChainID:  ctx.GuestChain,
		Endpoint:       ctx.GuestEndpoint,
		PeerPubKey:     ctx.GuestPin,
		MaxClearance:   tx.ClearanceLevel(ctx.HostGrant.Clearance),
		AllowedDomains: ctx.HostGrant.Domains,
		ExpiresAt:      ctx.HostGrant.Expiry,
		Status:         "active",
	})
	if err != nil {
		ctx.RollbackGuestCA() // broadcast rejected: discard the staged CA (no leak)
		return "", "", fmt.Errorf("host agreement broadcast failed: %w", err)
	}
	// On-chain authz succeeded: commit the staged guest CA, then the seed. The
	// seed is the ceremony's OWN seed (frozen in ctx), never re-resolved by chain
	// id - so a second/retried session for the same guest can never persist a
	// different seed than the one E was signed over.
	// A persist failure here leaves the tx-33 ON-CHAIN-ACTIVE while the host has
	// no usable guest CA / seed - an unauthenticatable, half-active agreement.
	// Undo it (revoke on-chain, discard the staged CA, burn the session) instead
	// of marking it active, so local and on-chain state stay consistent.
	// MarkActive is reached ONLY when both commits succeed, which is also what
	// makes its seed-scrub safe.
	if cErr := ctx.CommitGuestCA(); cErr != nil {
		m.logger.Error().Err(cErr).Str("guest", ctx.GuestChain).Msg("guest CA commit failed post-broadcast; revoking agreement")
		return m.undoPartialHostConfirm(sessionID, ctx, "guest CA commit failed", cErr)
	}
	if sErr := m.commitPairSeed(ctx.GuestChain, ctx.GuestPin, ctx.Seed); sErr != nil {
		m.logger.Error().Err(sErr).Str("guest", ctx.GuestChain).Msg("host seed commit failed post-broadcast; revoking agreement")
		return m.undoPartialHostConfirm(sessionID, ctx, "host seed commit failed", sErr)
	}
	m.joins.MarkActive(sessionID)
	m.rememberPeerName(ctx.GuestChain, ctx.GuestName)
	m.logger.Info().Str("guest", ctx.GuestChain).Str("tx", txHash).Msg("federation join activated (host side)")
	return txHash, m.localChainID, nil
}

// rememberPeerName stores a peer's friendly label for local display (connections
// list). Best-effort: a nil/Postgres store or a write error never affects the
// ceremony — the label is cosmetic.
func (m *Manager) rememberPeerName(remoteChainID, name string) {
	if name == "" {
		return
	}
	ss := m.syncStore()
	if ss == nil {
		return
	}
	if err := ss.SetPeerName(context.Background(), remoteChainID, name); err != nil {
		m.logger.Debug().Err(err).Str("peer", remoteChainID).Msg("could not store peer display name (cosmetic)")
	}
}

// undoPartialHostConfirm rolls back a host confirm that broadcast tx-33 but then
// failed to persist the guest CA or seed: it revokes the on-chain agreement
// (tx-34, best-effort), discards the staged CA, and aborts the session - leaving
// no half-active agreement behind.
func (m *Manager) undoPartialHostConfirm(sessionID string, ctx *ConfirmContext, what string, cause error) (string, string, error) {
	if _, rErr := m.RevokeAgreement(ctx.GuestChain); rErr != nil {
		m.logger.Error().Err(rErr).Str("guest", ctx.GuestChain).Msg("revoke after partial host confirm failed - manual cleanup may be needed")
	}
	ctx.RollbackGuestCA()
	m.joins.Abort(sessionID)
	return "", "", fmt.Errorf("%s after broadcast; agreement revoked: %w", what, cause)
}

// ---------------------------------------------------------------------------
// Host-side ceremony drivers (called by the local operator REST endpoints)
// ---------------------------------------------------------------------------

// HostCreateResult is returned to the host wizard (H1).
type HostCreateResult struct {
	SessionID  string `json:"session_id"`
	OTPAuthURI string `json:"otpauth_uri"`
	HostPinHex string `json:"host_pin"`
	Endpoint   string `json:"endpoint"`
	ExpiresAt  int64  `json:"expires_at"`
}

// HostCreate opens a join session, generates the shared seed, and returns the
// enrollment QR string (rendered client-side).
func (m *Manager) HostCreate(hostEndpoint string) (*HostCreateResult, error) {
	if err := validateJoinEndpoint(hostEndpoint); err != nil {
		return nil, fmt.Errorf("invalid host endpoint: %w", err)
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return nil, fmt.Errorf("own pin unavailable: %w", err)
	}
	seed, err := totp.NewSecret()
	if err != nil {
		return nil, err
	}
	now := time.Now()
	js, err := m.joins.Create(m.localChainID, ownPin, hostEndpoint, seed, now)
	if err != nil {
		return nil, err
	}
	uri := totp.ProvisioningURI(seed, m.localChainID, "SAGE", ownPin, hostEndpoint, sidForQR(js.ID), "host")
	return &HostCreateResult{
		SessionID:  js.ID,
		OTPAuthURI: uri,
		HostPinHex: hex.EncodeToString(ownPin),
		Endpoint:   hostEndpoint,
		ExpiresAt:  js.ExpiresAt.Unix(),
	}, nil
}

// HostScanReturn records the guest pin + endpoint the host scanned off the
// guest's return QR (§2.2.5) - the anchor the first /join/request is asserted
// against. The scanned session id must match this session.
func (m *Manager) HostScanReturn(sessionID, returnURI string) error {
	enr, err := totp.ParseEnrollment(returnURI, true) // pin-only reciprocal card allowed
	if err != nil {
		return err
	}
	if enr.Role != "guest" {
		return fmt.Errorf("scanned code is not a guest return card")
	}
	if sidForQR(sessionID) != strings.ToUpper(joinB32.EncodeToString(enr.SessionB)) {
		return fmt.Errorf("scanned code belongs to a different session")
	}
	return m.joins.SetExpectedGuest(sessionID, enr.Pin, enr.Endpoint, time.Now())
}

// HostSessionView is the host wizard's poll payload.
type HostSessionView struct {
	SessionID    string     `json:"session_id"`
	State        string     `json:"state"`
	GuestChain   string     `json:"guest_chain,omitempty"`
	GuestName    string     `json:"guest_name,omitempty"` // friendly label the guest chose (cosmetic)
	GuestScope   *ScopeWire `json:"guest_scope,omitempty"`
	CodeG        string     `json:"code_g,omitempty"` // host compares this to what the guest reads (approval #1)
	CodeH        string     `json:"code_h,omitempty"` // host reads this back (after approval)
	Approved     bool       `json:"approved"`
	Active       bool       `json:"active"`
	ExpectsGuest bool       `json:"expects_guest"`
}

// HostSessionStatus returns the host wizard view, computing CODE_G once a guest
// has requested. CODE_H is exposed only after approval (H6 reveal).
func (m *Manager) HostSessionStatus(sessionID string) (*HostSessionView, error) {
	now := time.Now()
	js, ok := m.joins.Get(sessionID, now)
	if !ok {
		return &HostSessionView{SessionID: sessionID, State: JoinExpired}, nil
	}
	view := &HostSessionView{
		SessionID:    sessionID,
		State:        js.State,
		GuestChain:   js.GuestChain,
		GuestName:    js.GuestName,
		Approved:     js.Approved,
		Active:       js.State == JoinActive,
		ExpectsGuest: len(js.ExpectedGuestPin) == 32,
	}
	if js.GuestChain != "" {
		codeG, codeH := js.confirmCodes()
		view.CodeG = codeG
		if js.Approved {
			view.CodeH = codeH
		}
	}
	return view, nil
}

// HostApprove is approval #1: the host operator types the code they heard; on a
// match it sets the host grant terms, freezes E (RT-6), and moves to
// HOST_APPROVED. The grant's allowed_domains must be non-empty (a cross_fed
// record requires a scope; "*" is a chain-admin treaty).
func (m *Manager) HostApprove(sessionID, typedCode string, grant ScopeWire) error {
	now := time.Now()
	js, ok := m.joins.Get(sessionID, now)
	if !ok {
		return fmt.Errorf("join session not found or expired")
	}
	if js.GuestChain == "" {
		return fmt.Errorf("no guest request to approve yet")
	}
	// Empty AllowedDomains is a legitimate "connect but share nothing" grant: the
	// record is active (so the peer authenticates + reciprocity works) but the
	// guest can read nothing. It is accepted on-chain for a chain-admin operator
	// (the common solo/family case); a non-admin gets a clear on-chain authz
	// error at broadcast, which the wizard surfaces recoverably. So do NOT block
	// it here (blocking it stranded the ceremony at the irreversible compare step
	// for exactly the conservative default the UI encourages).
	if grant.MaxClearance < 0 || grant.MaxClearance > 4 {
		return fmt.Errorf("max_clearance must be 0..4")
	}
	// The code compare AND the E freeze happen atomically inside ApproveWithCode
	// (one locked critical section over the session), so a concurrent re-request
	// can never shift the fields between "compared" and "frozen".
	locked, err := m.joins.ApproveWithCode(sessionID, typedCode, HostGrant{
		Clearance: clampClearance(grant.MaxClearance),
		Domains:   grant.AllowedDomains,
		Expiry:    0,
		Mode:      grant.Mode,
		Direction: grant.Direction,
		Scope:     grant.digest(),
	})
	if locked {
		return fmt.Errorf("code does not match - session locked")
	}
	return err
}

// HostAbort burns a session (H4 "No" / operator ignore).
func (m *Manager) HostAbort(sessionID string) { m.joins.Abort(sessionID) }

// ---------------------------------------------------------------------------
// Guest-side ceremony drivers + draft store
// ---------------------------------------------------------------------------

type guestDraft struct {
	sessionID    string
	hostChain    string
	hostName     string // host's friendly label (cosmetic, from the CA fetch)
	hostEndpoint string
	hostPin      []byte
	hostCAPEM    []byte
	hostAgentPub []byte
	seed         []byte
	guestNonce   []byte
	hostNonce    []byte
	confirmStep  int64
	scope        ScopeWire
	expiresAt    time.Time
}

func (m *Manager) putGuestDraft(d *guestDraft) {
	m.guestMu.Lock()
	defer m.guestMu.Unlock()
	// prune expired
	now := time.Now()
	for k, v := range m.guestDrafts {
		if now.After(v.expiresAt) {
			zeroize(v.seed)
			delete(m.guestDrafts, k)
		}
	}
	m.guestDrafts[d.sessionID] = d
}

func (m *Manager) getGuestDraft(sessionID string) (*guestDraft, bool) {
	m.guestMu.Lock()
	defer m.guestMu.Unlock()
	d, ok := m.guestDrafts[sessionID]
	if !ok {
		return nil, false
	}
	if time.Now().After(d.expiresAt) {
		// Expired: zeroize the secret seed and drop it now (do not wait for the
		// next put/prune), so a stalled ceremony never leaves a seed in RAM.
		zeroize(d.seed)
		delete(m.guestDrafts, sessionID)
		return nil, false
	}
	return d, true
}

// pruneGuestDrafts zeroizes + drops every expired guest draft (periodic reaper).
func (m *Manager) pruneGuestDrafts(now time.Time) {
	m.guestMu.Lock()
	defer m.guestMu.Unlock()
	for k, v := range m.guestDrafts {
		if now.After(v.expiresAt) {
			zeroize(v.seed)
			delete(m.guestDrafts, k)
		}
	}
}

// StartMaintenance runs a periodic reaper until ctx is cancelled: it reaps
// expired/terminal host sessions (rolling back staged CAs, zeroizing seeds),
// drains the rate-limiter map, and prunes expired guest drafts. Wire it once
// from the node when the federation listener starts; tests may skip it.
func (m *Manager) StartMaintenance(ctx context.Context) {
	go func() {
		t := time.NewTicker(time.Minute)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-t.C:
				m.joins.Maintain(now)
				m.pruneGuestDrafts(now)
			}
		}
	}()
}

func (m *Manager) dropGuestDraft(sessionID string) {
	m.guestMu.Lock()
	defer m.guestMu.Unlock()
	if d, ok := m.guestDrafts[sessionID]; ok {
		zeroize(d.seed)
		delete(m.guestDrafts, sessionID)
	}
}

// GuestScanResult is returned to the guest wizard after a successful scan.
type GuestScanResult struct {
	SessionID    string `json:"session_id"`
	HostChain    string `json:"host_chain"`
	HostName     string `json:"host_name,omitempty"` // host's friendly label (cosmetic)
	HostEndpoint string `json:"host_endpoint"`
	HostPinHex   string `json:"host_pin"`
	ReturnURI    string `json:"return_uri"` // the guest's pin-only return QR (host scans it)
}

// GuestScan parses a scanned host enrollment QR (fail-closed), fetches the host
// CA over the join listener, asserts its SPKI equals the scanned pin (refuse on
// mismatch), and caches a draft. It returns the guest's own return QR string.
func (m *Manager) GuestScan(ctx context.Context, uri, guestEndpoint string) (*GuestScanResult, error) {
	if err := validateJoinEndpoint(guestEndpoint); err != nil {
		return nil, fmt.Errorf("invalid guest endpoint: %w", err)
	}
	enr, err := totp.ParseEnrollment(uri, false) // seed REQUIRED for a host enrollment
	if err != nil {
		return nil, err
	}
	if enr.Role != "host" {
		return nil, fmt.Errorf("this is not a host connection code")
	}
	if valErr := ValidateChainID(enr.ChainID); valErr != nil {
		return nil, fmt.Errorf("invalid host chain id in code")
	}
	if enr.ChainID == m.localChainID {
		return nil, fmt.Errorf("refusing self-federation")
	}
	sessionID := strings.ToUpper(joinB32.EncodeToString(enr.SessionB))
	// Fetch the host CA over the join listener (payload is pin-authenticated).
	// hostName is the host's cosmetic label from the same response.
	caPEM, hostName, err := m.fetchHostCA(ctx, enr.Endpoint, sessionID)
	if err != nil {
		return nil, fmt.Errorf("could not fetch host CA: %w", err)
	}
	caCert, err := parseCACertPEM(caPEM)
	if err != nil {
		return nil, fmt.Errorf("host CA: %w", err)
	}
	if subtle.ConstantTimeCompare(SPKIFingerprint(caCert), enr.Pin) != 1 {
		return nil, fmt.Errorf("host CA does not match the scanned code (possible tampering) - stop")
	}
	if cnErr := requireChainCN(caCert, enr.ChainID); cnErr != nil {
		return nil, cnErr
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return nil, err
	}
	m.putGuestDraft(&guestDraft{
		sessionID:    sessionID,
		hostChain:    enr.ChainID,
		hostName:     hostName,
		hostEndpoint: enr.Endpoint,
		hostPin:      append([]byte(nil), enr.Pin...),
		hostCAPEM:    caPEM,
		seed:         append([]byte(nil), enr.Seed...),
		expiresAt:    time.Now().Add(guestDraftTTL),
	})
	returnURI := totp.ProvisioningURI(nil, m.localChainID, "SAGE", ownPin, guestEndpoint, sidForQR(sessionID), "guest")
	return &GuestScanResult{
		SessionID:    sessionID,
		HostChain:    enr.ChainID,
		HostName:     hostName,
		HostEndpoint: enr.Endpoint,
		HostPinHex:   hex.EncodeToString(enr.Pin),
		ReturnURI:    returnURI,
	}, nil
}

// GuestRequestResult carries the codes back to the guest wizard.
type GuestRequestResult struct {
	CodeG       string `json:"code_g"` // guest reads this aloud (approval-#1 material)
	CodeH       string `json:"code_h"` // guest compares this to what the host reads back
	ConfirmStep int64  `json:"confirm_step"`
}

// GuestRequest fires POST /fed/v1/join/request against the host, records the
// exchanged nonces, and computes CODE_G/CODE_H. scope is the guest's scope_G.
func (m *Manager) GuestRequest(ctx context.Context, sessionID, guestEndpoint string, scope ScopeWire) (*GuestRequestResult, error) {
	d, ok := m.getGuestDraft(sessionID)
	if !ok {
		return nil, fmt.Errorf("no scanned connection for this session (re-scan)")
	}
	if err := validateJoinEndpoint(guestEndpoint); err != nil {
		return nil, fmt.Errorf("invalid guest endpoint: %w", err)
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return nil, err
	}
	ownCAPEM, err := m.ownCAPEM()
	if err != nil {
		return nil, err
	}
	guestNonce := make([]byte, 16)
	if _, readErr := randRead(guestNonce); readErr != nil {
		return nil, readErr
	}
	body := &JoinRequestWire{
		SessionID:     sessionID,
		GuestChain:    m.localChainID,
		GuestName:     sanitizeName(m.NetworkName()),
		GuestAgentID:  hex.EncodeToString(m.agentPub),
		GuestNonce:    hex.EncodeToString(guestNonce),
		GuestPin:      hex.EncodeToString(ownPin),
		GuestCAPEM:    string(ownCAPEM),
		GuestEndpoint: guestEndpoint,
		Scope:         scope,
	}
	var resp JoinRequestResp
	if callErr := m.guestCall(ctx, d, http.MethodPost, "/fed/v1/join/request", body, &resp); callErr != nil {
		return nil, callErr
	}
	hostNonce, err := hex.DecodeString(resp.HostNonce)
	if err != nil || len(hostNonce) != 16 {
		return nil, fmt.Errorf("host returned an invalid nonce")
	}
	// The host echoes its pin; it MUST equal the scanned one (anchor).
	if echoed, dErr := hex.DecodeString(resp.HostPin); dErr != nil || subtle.ConstantTimeCompare(echoed, d.hostPin) != 1 {
		return nil, fmt.Errorf("host pin changed since the scan - stop")
	}
	hostAgentPub, err := hex.DecodeString(resp.HostAgentID)
	if err != nil || len(hostAgentPub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("host returned an invalid agent id")
	}
	d.guestNonce = guestNonce
	d.hostNonce = hostNonce
	d.confirmStep = resp.ConfirmStep
	d.hostAgentPub = hostAgentPub
	d.scope = scope
	m.putGuestDraft(d)

	codeG, codeH := m.guestConfirmCodes(d, ownPin)
	return &GuestRequestResult{CodeG: codeG, CodeH: codeH, ConfirmStep: resp.ConfirmStep}, nil
}

// GuestPollStatus dials the host's /fed/v1/join/status over the ceremony mTLS
// config so the guest wizard learns when the host has approved (and receives the
// host's granted scope, needed to compute E at confirm).
func (m *Manager) GuestPollStatus(ctx context.Context, sessionID string) (*JoinStatusResp, error) {
	d, ok := m.getGuestDraft(sessionID)
	if !ok {
		return nil, fmt.Errorf("no scanned connection for this session (re-scan)")
	}
	var resp JoinStatusResp
	path := "/fed/v1/join/status?session_id=" + url.QueryEscape(sessionID)
	if err := m.guestCall(ctx, d, http.MethodGet, path, nil, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

// GuestConfirm is approval #2: it computes E, broadcasts the guest's OWN tx-33
// first (guest-first activation, RT-7), then POSTs /fed/v1/join/confirm so the
// host activates its side. hostScope is what the guest polled from /join/status.
func (m *Manager) GuestConfirm(ctx context.Context, sessionID, guestEndpoint string, hostScope ScopeWire) (string, error) {
	d, ok := m.getGuestDraft(sessionID)
	if !ok {
		return "", fmt.Errorf("no scanned connection for this session (re-scan)")
	}
	if d.hostNonce == nil || d.guestNonce == nil {
		return "", fmt.Errorf("request step not completed")
	}
	if err := validateJoinEndpoint(guestEndpoint); err != nil {
		return "", fmt.Errorf("invalid guest endpoint: %w", err)
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return "", err
	}
	e := EnrollInputs{
		GuestChain:    m.localChainID,
		HostChain:     d.hostChain,
		GuestPin:      ownPin,
		HostPin:       d.hostPin,
		GuestEndpoint: guestEndpoint,
		HostEndpoint:  d.hostEndpoint,
		GuestScope:    d.scope.digest(),
		HostScope:     hostScope.digest(),
		Seed:          d.seed,
		GuestNonce:    d.guestNonce,
		HostNonce:     d.hostNonce,
	}.Attestation()
	guestSig := SignEnroll(m.agentKey, e, false)
	guestAckSig := SignEnroll(m.agentKey, e, true)

	// Guest-first: broadcast our own tx-33 (remote=host) + commit host CA + seed.
	txHash, err := m.broadcastCrossFedSet(&tx.CrossFedTerms{
		RemoteChainID:  d.hostChain,
		Endpoint:       d.hostEndpoint,
		PeerPubKey:     d.hostPin,
		MaxClearance:   tx.ClearanceLevel(clampClearance(d.scope.MaxClearance)),
		AllowedDomains: d.scope.AllowedDomains,
		Status:         "active",
	})
	if err != nil {
		return "", fmt.Errorf("your agreement broadcast failed: %w", err)
	}
	if _, cErr := m.StoreRemoteCA(d.hostChain, d.hostCAPEM); cErr != nil {
		m.logger.Error().Err(cErr).Str("host", d.hostChain).Msg("host CA commit failed post-broadcast")
	}
	if sErr := m.commitPairSeed(d.hostChain, d.hostPin, d.seed); sErr != nil {
		m.logger.Error().Err(sErr).Str("host", d.hostChain).Msg("guest seed commit failed post-broadcast")
	}

	// Tell the host to activate its side.
	confirmBody := &JoinConfirmWire{
		SessionID:   sessionID,
		GuestSig:    hex.EncodeToString(guestSig),
		GuestAckSig: hex.EncodeToString(guestAckSig),
	}
	var confirmResp JoinConfirmResp
	if err := m.guestCall(ctx, d, http.MethodPost, "/fed/v1/join/confirm", confirmBody, &confirmResp); err != nil {
		// Our side is active; the host's is not yet. Safe one-sided window -
		// peerAuth rejects host->guest until the host's tx lands; retry confirm.
		m.logger.Warn().Err(err).Str("host", d.hostChain).Msg("guest active but host confirm failed (one-sided window)")
		return txHash, fmt.Errorf("your side is connected but the host has not confirmed yet: %w", err)
	}
	m.rememberPeerName(d.hostChain, d.hostName)
	m.dropGuestDraft(sessionID)
	m.logger.Info().Str("host", d.hostChain).Str("tx", txHash).Msg("federation join activated (guest side)")
	return txHash, nil
}

// ---------------------------------------------------------------------------
// Shared helpers
// ---------------------------------------------------------------------------

// guestConfirmCodes computes CODE_G/CODE_H from the guest's draft.
func (m *Manager) guestConfirmCodes(d *guestDraft, ownPin []byte) (string, string) {
	return ConfirmCodes(d.seed, m.localChainID, ownPin, d.hostChain, d.hostPin,
		d.guestNonce, d.hostNonce, d.confirmStep)
}

// broadcastCrossFedSet builds, agent-proofs (with the node operator key), and
// broadcasts a tx-33 CrossFedSet. Mirrors the REST tx-builder but node-
// originated - used for both operators' ceremony activations. Determinism/authz
// are re-checked on-chain (crossFedAuthorized).
func (m *Manager) broadcastCrossFedSet(terms *tx.CrossFedTerms) (string, error) {
	body := []byte("cross_fed:" + terms.RemoteChainID)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- ts non-negative
	agentSig := ed25519.Sign(m.agentKey, append(append([]byte{}, bodyHash[:]...), tsBytes...))

	ptx := &tx.ParsedTx{
		Type:           tx.TxTypeCrossFedSet,
		Nonce:          tx.MonotonicNonce(m.agentKey),
		Timestamp:      time.Unix(ts, 0),
		CrossFedTerms:  terms,
		AgentPubKey:    m.agentPub,
		AgentSig:       agentSig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
	}
	if err := tx.SignTx(ptx, m.agentKey); err != nil {
		return "", fmt.Errorf("sign cross_fed set tx: %w", err)
	}
	encoded, err := tx.EncodeTx(ptx)
	if err != nil {
		return "", fmt.Errorf("encode cross_fed set tx: %w", err)
	}
	hash, _, err := m.broadcast(encoded)
	return hash, err
}

// RevokeAgreement broadcasts a tx-34 CrossFedRevoke for a remote chain (the
// "turn off connection" action, H8) and purges the node-local seed + cached CA
// so the peer relaxes back to v2-accepted for a future re-add. tx-34 deletes
// nothing on disk (consensus cannot touch node-local files); the purge is the
// off-consensus half.
func (m *Manager) RevokeAgreement(remoteChainID string) (string, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return "", err
	}
	body := []byte("cross_fed_revoke:" + remoteChainID)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- ts non-negative
	agentSig := ed25519.Sign(m.agentKey, append(append([]byte{}, bodyHash[:]...), tsBytes...))

	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeCrossFedRevoke,
		Nonce:     tx.MonotonicNonce(m.agentKey),
		Timestamp: time.Unix(ts, 0),
		CrossFedRevoke: &tx.CrossFedRevoke{
			RemoteChainID: remoteChainID,
			Reason:        "operator disconnect",
		},
		AgentPubKey:    m.agentPub,
		AgentSig:       agentSig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
	}
	if err := tx.SignTx(ptx, m.agentKey); err != nil {
		return "", fmt.Errorf("sign cross_fed revoke tx: %w", err)
	}
	encoded, err := tx.EncodeTx(ptx)
	if err != nil {
		return "", fmt.Errorf("encode cross_fed revoke tx: %w", err)
	}
	hash, _, err := m.broadcast(encoded)
	if err != nil {
		return "", err
	}
	// Off-consensus purge (zeroize seed cache + delete seed files + drop cached CA).
	m.purgeSeed(remoteChainID)
	m.invalidateCACache(remoteChainID)
	// v11.5 domain-sync purge: consent + queued deliveries die with the
	// agreement. Best-effort — a failed purge must not fail the revoke (the
	// send-time ActiveAgreement gate already stops delivery); the drainer's
	// scan finds no consent rows and goes quiet.
	if ss, ok := m.memStore.(*store.SQLiteStore); ok && ss != nil {
		ctx := context.Background()
		if err := ss.DeleteSyncDomains(ctx, remoteChainID); err != nil {
			m.logger.Warn().Err(err).Str("remote", remoteChainID).Msg("revoke: sync domains purge failed")
		}
		if err := ss.PurgeSyncOutbox(ctx, remoteChainID); err != nil {
			m.logger.Warn().Err(err).Str("remote", remoteChainID).Msg("revoke: sync outbox purge failed")
		}
	}
	return hash, nil
}

// commitPairSeed stages + commits the EXACT ceremony seed for (localChain,
// remoteChain) with the canonical pin-pair binding, and flips seed_established.
// The seed is passed in by the confirm driver (the one E was signed over), never
// re-resolved by chain id, so colliding/retried sessions for the same remote
// chain cannot diverge the pair onto different seeds.
func (m *Manager) commitPairSeed(remoteChain string, remotePin, seed []byte) error {
	if len(seed) == 0 {
		return fmt.Errorf("no seed to commit for %s", remoteChain)
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return err
	}
	pp := pinPair(m.localChainID, ownPin, remoteChain, remotePin)
	commit, _, err := m.stageSeed(remoteChain, seed, pp[:], time.Now().Unix())
	if err != nil {
		return err
	}
	return commit()
}

// fetchHostCA GETs the host CA PEM over the join listener. The transport is not
// the authenticator (InsecureSkipVerify); the caller validates the returned PEM
// against the scanned pin.
// fetchHostCA returns the host's CA PEM (pin-validated by the caller) plus the
// host's friendly label (cosmetic, sanitized, may be empty). The label rides the
// same response but carries no authority — only the CAPEM feeds the pin check.
func (m *Manager) fetchHostCA(ctx context.Context, hostEndpoint, sessionID string) (caPEM []byte, hostName string, err error) {
	tlsCfg, err := m.joinBootstrapTLS()
	if err != nil {
		return nil, "", err
	}
	base, err := netguard.LocalLANHTTPBase(hostEndpoint, "https")
	if err != nil {
		return nil, "", err
	}
	u, err := netguard.JoinPath(base, "/fed/v1/join/ca")
	if err != nil {
		return nil, "", err
	}
	u += "?session_id=" + url.QueryEscape(sessionID)
	reqCtx, cancel := context.WithTimeout(ctx, joinCAFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodGet, u, nil)
	if err != nil {
		return nil, "", err
	}
	client := &http.Client{Transport: joinHTTPTransport(tlsCfg)}
	defer client.CloseIdleConnections()
	// netguard.LocalLANHTTPBase canonicalizes hostEndpoint, and joinHTTPTransport
	// rejects non-local/LAN destinations again at dial time.
	resp, err := client.Do(req) // lgtm[go/request-forgery]
	if err != nil {
		return nil, "", err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, joinBodyCap))
	if err != nil {
		return nil, "", err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, "", fmt.Errorf("host returned %d", resp.StatusCode)
	}
	var out JoinCAResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, "", err
	}
	if out.CAPEM == "" {
		return nil, "", fmt.Errorf("host returned an empty CA")
	}
	return []byte(out.CAPEM), sanitizeName(out.HostName), nil
}

// guestCall performs a signed-by-mTLS ceremony call to the host, verifying the
// host's server cert against the fetched+pinned host CA.
func (m *Manager) guestCall(ctx context.Context, d *guestDraft, method, path string, body, out any) error {
	tlsCfg, err := m.joinClientTLS(d.hostCAPEM, d.hostPin)
	if err != nil {
		return err
	}
	reqCtx, cancel := context.WithTimeout(ctx, joinCallTimeout)
	defer cancel()
	base, err := netguard.LocalLANHTTPBase(d.hostEndpoint, "https")
	if err != nil {
		return err
	}
	u, err := netguard.JoinPath(base, path)
	if err != nil {
		return err
	}
	var reqBody io.Reader = http.NoBody
	if body != nil {
		payload, mErr := json.Marshal(body)
		if mErr != nil {
			return mErr
		}
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, u, reqBody)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	client := &http.Client{Transport: joinHTTPTransport(tlsCfg)}
	defer client.CloseIdleConnections()
	// d.hostEndpoint was captured only after netguard.LocalLANHTTPBase validation,
	// and joinHTTPTransport enforces the same local/LAN constraint at dial time.
	resp, err := client.Do(req) // lgtm[go/request-forgery]
	if err != nil {
		return fmt.Errorf("host unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, joinBodyCap))
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("host returned %d: %s", resp.StatusCode, truncate(raw, 200))
	}
	return json.Unmarshal(raw, out)
}

// joinBootstrapTLS presents our node cert but does NOT verify the server (the
// CA payload is pin-authenticated afterward). Used only for the CA fetch.
func (m *Manager) joinBootstrapTLS() (*tls.Config, error) {
	cert, err := m.loadNodeKeyPair()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		// lgtm[go/disabled-certificate-check] -- this bootstrap call fetches
		// only the CA payload; the caller validates that payload against the
		// scanned SPKI pin before any trusted ceremony call is made.
		InsecureSkipVerify: true, // #nosec G402 -- the fetched CA payload is validated against the scanned SPKI pin
	}, nil
}

// joinClientTLS presents our node cert and pins the host's server cert to the
// (scanned-and-verified) host CA - hostname matching replaced by the pin.
func (m *Manager) joinClientTLS(hostCAPEM, hostPin []byte) (*tls.Config, error) {
	caCert, err := parseCACertPEM(hostCAPEM)
	if err != nil {
		return nil, err
	}
	if subtle.ConstantTimeCompare(SPKIFingerprint(caCert), hostPin) != 1 {
		return nil, fmt.Errorf("host CA pin mismatch")
	}
	cert, err := m.loadNodeKeyPair()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
		InsecureSkipVerify:     true, // #nosec G402 -- verification is the pinned-CA check below
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyChainAgainstCA(rawCerts, caCert, x509.ExtKeyUsageServerAuth)
		},
	}, nil
}

func joinHTTPTransport(tlsCfg *tls.Config) *http.Transport {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return &http.Transport{
		TLSClientConfig: tlsCfg,
		DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
			host, port, err := net.SplitHostPort(address)
			if err != nil {
				return nil, fmt.Errorf("join dial: invalid address: %w", err)
			}
			if !netguard.LocalLANHost(host) {
				return nil, fmt.Errorf("join dial: refusing non-local/LAN host %q", host)
			}
			return dialer.DialContext(ctx, network, net.JoinHostPort(host, port))
		},
	}
}

func (m *Manager) loadNodeKeyPair() (tls.Certificate, error) {
	return tls.LoadX509KeyPair(
		filepath.Join(m.certsDir, tlsca.NodeCertFile),
		filepath.Join(m.certsDir, tlsca.NodeKeyFile),
	)
}

// ownCAPEM reads this node's own CA certificate PEM (served to a scanning guest).
func (m *Manager) ownCAPEM() ([]byte, error) {
	return readFileClamped(filepath.Join(m.certsDir, tlsca.CACertFile))
}

// validateJoinEndpoint enforces the same scheme+host-only rule the cross_fed
// REST builder uses (a path/query would mis-route and mis-pin).
func validateJoinEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("endpoint must be an https://host[:port] URL with no path, query, or fragment")
	}
	if _, err := netguard.LocalLANHTTPBase(endpoint, "https"); err != nil {
		return err
	}
	return nil
}

// maxNetworkNameLen bounds a friendly network label on the wire. Matches the
// dashboard-side cap; enforced again here because inbound peer names are
// untrusted (a hostile peer could send an oversized/control-laden label).
const maxNetworkNameLen = 48

// sanitizeName normalizes a friendly network label from ANY source (the local
// operator OR an untrusted peer over the wire): strips control chars (incl.
// newlines that would forge log lines or split a UI row), collapses internal
// whitespace, trims, and caps the length. The label is purely cosmetic and is
// NEVER an authorization input — the CA pin / SAS is the identity anchor — but
// it is displayed verbatim, so it must be defanged. Returns "" for a blank
// input (callers fall back to the chain id).
func sanitizeName(name string) string {
	var b strings.Builder
	lastSpace := false
	for _, r := range name {
		if r == '\t' || r == '\n' || r == '\r' || r == ' ' {
			if b.Len() > 0 {
				lastSpace = true
			}
			continue
		}
		if r < 0x20 || r == 0x7f {
			continue
		}
		if lastSpace {
			b.WriteByte(' ')
			lastSpace = false
		}
		b.WriteRune(r)
		if len([]rune(b.String())) >= maxNetworkNameLen {
			break
		}
	}
	return b.String()
}

// SanitizeNetworkName is the exported entry point for the dashboard/REST layer
// to defang an operator-supplied friendly label before persisting/displaying it.
// Same rules as the inbound-peer sanitizer.
func SanitizeNetworkName(name string) string { return sanitizeName(name) }

// sidForQR renders a base32 session id string in the exact form the QR carries
// (uppercase, matching ParseEnrollment's decode).
func sidForQR(id string) string { return strings.ToUpper(id) }

func shortID(s string) string {
	if len(s) > 8 {
		return s[:8]
	}
	return s
}

func clampClearance(c int) uint8 {
	if c < 0 {
		return 0
	}
	if c > 4 {
		return 4
	}
	return uint8(c) // #nosec G115 -- clamped 0..4
}

func mustOwnPin(m *Manager) []byte {
	p, _ := m.ownPin()
	return p
}

func zeroize(b []byte) {
	for i := range b {
		b[i] = 0
	}
}

func randRead(b []byte) (int, error) { return rand.Read(b) }

// readFileClamped reads a small on-disk file (a CA PEM), bounding the read.
func readFileClamped(path string) ([]byte, error) {
	f, err := os.Open(path) // #nosec G304 -- fixed path under the node certs dir
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return io.ReadAll(io.LimitReader(f, joinBodyCap))
}
