package federation

// The introduced-link wire (RFC #443 Phase B2b, contract v2 §2).
//
//	POST /fed/v1/intro/request              guest → host: offer_id, guest nonce, guest evidence.
//	                                        The host answers with its nonce and evidence, a
//	                                        fresh 32-byte pair seed and E_intro.
//	GET  /fed/v1/intro/{offer_id}/accept    the guest polls for host-accept; 202 while the host
//	                                        owner has not accepted under the current envelope.
//	POST /fed/v1/intro/confirm              guest → host: guest-accept + guest-ack; the host
//	                                        verifies both and runs its §3.3 write path.
//	GET  /fed/v1/intro/{offer_id}/status    the guest's bound_pending_peer window asks the host.
//
// Authentication is the roster card, never a QR code. The guest presents its
// node certificate chained to its CA; the handshake window admits a chain
// only when that CA's SPKI is the pin of a current roster card, and every
// route then requires that pin to be the card of the offer's peer. The guest
// checks the host the same way: the host's CA must carry the host card's pin
// and the TLS server chain must verify against it. Each route has its own
// request cap, checked before decode, and the client reads every response
// through LimitReader(cap+1).
//
// Host sessions are in memory only, bounded by the offer expiry and an hour.
// The seed lives nowhere else until ActivateIntroducedLink stages it, and it
// is zeroized when the session ends. A restart simply loses the session; the
// guest driver starts a fresh one (new nonces, new E_intro, new seed).

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"regexp"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/netguard"
	"github.com/l33tdawg/sage/internal/store"
)

// Per-route caps (design §2).
const (
	IntroRequestCap      = 32 << 10
	IntroRequestRespCap  = 32 << 10
	IntroAcceptRespCap   = 8 << 10
	IntroConfirmCap      = 8 << 10
	IntroConfirmRespCap  = 8 << 10
	IntroStatusRespCap   = 4 << 10
	introGetBodyCap      = 0
	introHostSessionTTL  = time.Hour
	introMaxChainCerts   = 4
	introDefaultWindow   = 15 * time.Minute
	introConfirmTimeout  = 90 * time.Second
	introMaxHostSessions = 1024
)

// Status values the host reports besides introduced_link states.
const (
	IntroStatusNone            = "none"
	IntroStatusAwaitingAccept  = "awaiting_accept"
	IntroStatusAwaitingConfirm = "awaiting_confirm"
)

// IntroEvidenceWire is one side's org evidence.
type IntroEvidenceWire struct {
	PolicyRevision uint64 `json:"policy_revision"`
	PolicyDigest   string `json:"policy_digest"`
	RosterRevision uint64 `json:"roster_revision"`
	RosterDigest   string `json:"roster_digest"`
	EnvelopeRev    uint64 `json:"envelope_revision"`
	EnvelopeDigest string `json:"envelope_digest"`
}

func (e IntroEvidenceWire) validate() error {
	switch {
	case e.PolicyRevision == 0 || !introHex64.MatchString(e.PolicyDigest):
		return errors.New("policy revision and digest are required")
	case e.RosterRevision == 0 || !introHex64.MatchString(e.RosterDigest):
		return errors.New("roster revision and digest are required")
	case e.EnvelopeRev == 0 || !introHex64.MatchString(e.EnvelopeDigest):
		return errors.New("envelope revision and digest are required")
	}
	return nil
}

// IntroRequestWire is POST /fed/v1/intro/request.
type IntroRequestWire struct {
	OrgID      string            `json:"org_id"`
	OfferID    string            `json:"offer_id"`
	GuestNonce string            `json:"guest_nonce"` // 32 lowercase hex
	Evidence   IntroEvidenceWire `json:"evidence"`
}

// IntroRequestResp is the host's answer. Transcript is the canonical E_intro.
type IntroRequestResp struct {
	OfferID    string            `json:"offer_id"`
	SessionID  string            `json:"session_id"`
	HostNonce  string            `json:"host_nonce"`
	Evidence   IntroEvidenceWire `json:"evidence"`
	Seed       string            `json:"seed"`        // 64 lowercase hex, inside the pinned session
	SeedCommit string            `json:"seed_commit"` // 64 lowercase hex
	HostCAPEM  string            `json:"host_ca_pem"`
	Transcript []byte            `json:"transcript"`
}

// IntroHostAcceptWire is GET /fed/v1/intro/{offer_id}/accept. HostAcceptSig
// is empty while the host owner has not accepted (HTTP 202).
type IntroHostAcceptWire struct {
	OfferID       string `json:"offer_id"`
	SessionID     string `json:"session_id"`
	HostAcceptSig string `json:"host_accept_sig,omitempty"`
}

// IntroConfirmWire is POST /fed/v1/intro/confirm.
type IntroConfirmWire struct {
	OfferID        string `json:"offer_id"`
	SessionID      string `json:"session_id"`
	GuestAcceptSig string `json:"guest_accept_sig"`
	GuestAckSig    string `json:"guest_ack_sig"`
}

// IntroConfirmResp reports where the host's row is.
type IntroConfirmResp struct {
	OfferID   string `json:"offer_id"`
	SessionID string `json:"session_id"`
	LinkID    string `json:"link_id,omitempty"`
	State     string `json:"state"`
	TxHash    string `json:"tx_hash,omitempty"`
}

// IntroStatusResp is GET /fed/v1/intro/{offer_id}/status.
type IntroStatusResp struct {
	OfferID   string `json:"offer_id"`
	SessionID string `json:"session_id,omitempty"`
	LinkID    string `json:"link_id,omitempty"`
	State     string `json:"state"`
}

var introSessionPattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

// introHostSession is one host-side session between request and confirm.
// The fields above mu are fixed at creation. mu guards the lifecycle:
//
//	active ─confirm takes ownership→ confirming ─write path ran→ done
//	                                             └─failed──────→ retired
//	active / done ─replace, expire, drop→ retired
//
// A confirm owns the session while confirming and works on its own copy of
// the seed. Replacement is refused while a confirm owns it (or once it has a
// link); expiry and drop only flag it, and the confirm retires and zeroizes
// it when it finishes. Nothing waits on a session while holding intro.mu.
type introHostSession struct {
	orgID, offerID, sessionID string
	peerChain                 string
	guestPin                  string
	guestCAPEM                []byte
	transcript                []byte
	e                         [32]byte
	priorTerms                string
	expiresAt                 int64

	mu          sync.Mutex
	state       introSessionState
	seed        []byte
	linkID      string
	retireAsked bool
}

type introSessionState int

const (
	introSessActive introSessionState = iota
	introSessConfirming
	introSessDone
	introSessRetired
)

var (
	errIntroConfirmBusy    = errors.New("a confirm for this intro session is already in progress")
	errIntroSessionRetired = errors.New("intro session has ended")
)

// retire ends the session. It never blocks: while a confirm owns the session
// it only flags it, and endConfirm finishes the job.
func (s *introHostSession) retire() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.state == introSessConfirming {
		s.retireAsked = true
		return
	}
	s.state = introSessRetired
	zeroize(s.seed)
}

// busy reports whether a confirm owns the session or it already has a link.
func (s *introHostSession) busy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state == introSessConfirming || s.state == introSessDone
}

// beginConfirm takes ownership for one confirm and returns its own copy of
// the seed. A session that already ran its write path returns its link id.
func (s *introHostSession) beginConfirm() (linkID string, seed []byte, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	switch s.state {
	case introSessDone:
		return s.linkID, nil, nil
	case introSessConfirming:
		return "", nil, errIntroConfirmBusy
	case introSessRetired:
		return "", nil, errIntroSessionRetired
	}
	s.state = introSessConfirming
	return "", append([]byte(nil), s.seed...), nil
}

// endConfirm releases ownership. With a link id the session is done,
// otherwise retired; the seed is zeroized either way, and a retire asked for
// meanwhile takes effect now.
func (s *introHostSession) endConfirm(linkID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	zeroize(s.seed)
	s.state = introSessRetired
	if linkID != "" {
		s.linkID = linkID
		if !s.retireAsked {
			s.state = introSessDone
		}
	}
}

// seedZeroed reports whether the seed is gone (tests).
func (s *introHostSession) seedZeroed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, b := range s.seed {
		if b != 0 {
			return false
		}
	}
	return true
}

func (m *Manager) putIntroHostSession(s *introHostSession) error {
	m.intro.mu.Lock()
	defer m.intro.mu.Unlock()
	if m.intro.host == nil {
		m.intro.host = map[string]*introHostSession{}
	}
	now := time.Now().Unix()
	for id, old := range m.intro.host {
		if old.expiresAt <= now {
			delete(m.intro.host, id)
			old.retire()
		}
	}
	if old, ok := m.intro.host[s.offerID]; ok {
		if old.busy() {
			return errors.New("this offer has a confirm in progress or a link already")
		}
		old.retire()
	} else if len(m.intro.host) >= introMaxHostSessions {
		return errors.New("too many open intro sessions")
	}
	m.intro.host[s.offerID] = s
	return nil
}

func (m *Manager) introHostSessionFor(offerID string) *introHostSession {
	m.intro.mu.Lock()
	defer m.intro.mu.Unlock()
	s := m.intro.host[offerID]
	if s != nil && s.expiresAt <= time.Now().Unix() {
		delete(m.intro.host, offerID)
		s.retire()
		return nil
	}
	return s
}

func (m *Manager) dropIntroHostSession(s *introHostSession) {
	m.intro.mu.Lock()
	if cur := m.intro.host[s.offerID]; cur == s {
		delete(m.intro.host, s.offerID)
	}
	m.intro.mu.Unlock()
	s.retire()
}

// introRequestPublished is a test seam run between publishing a host
// session and answering the request.
func (m *Manager) introRequestPublished() func(*introHostSession) {
	m.intro.mu.Lock()
	defer m.intro.mu.Unlock()
	return m.intro.requestPublished
}

// introConfirmHold is a test seam run while a confirm owns its session.
func (m *Manager) introConfirmHold() func() {
	m.intro.mu.Lock()
	defer m.intro.mu.Unlock()
	return m.intro.confirmHold
}

// ---------------------------------------------------------------------------
// Handshake window and route auth
// ---------------------------------------------------------------------------

// introPeer is the authenticated intro caller: the CA its chain verified
// against, whose SPKI is a roster card pin.
type introPeer struct {
	pin      string
	ca       *x509.Certificate
	leafSPKI []byte
}

type introPeerKey struct{}

// introPeerFromChain finds, in a presented client chain, a CA whose SPKI is a
// current roster card pin and against which the leaf verifies.
func (m *Manager) introPeerFromChain(rawCerts [][]byte) (*introPeer, bool) {
	src := m.introOrgSource()
	if src == nil || len(rawCerts) < 2 {
		return nil, false
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return nil, false
	}
	n := len(rawCerts)
	if n > 1+introMaxChainCerts {
		n = 1 + introMaxChainCerts
	}
	for _, raw := range rawCerts[1:n] {
		ca, pErr := x509.ParseCertificate(raw)
		if pErr != nil || !ca.IsCA {
			continue
		}
		pin := hex.EncodeToString(SPKIFingerprint(ca))
		if !src.IntroPeerPinKnown(pin) {
			continue
		}
		if verifyChainAgainstCA([][]byte{rawCerts[0]}, ca, x509.ExtKeyUsageClientAuth) == nil {
			return &introPeer{pin: pin, ca: ca, leafSPKI: SPKIFingerprint(leaf)}, true
		}
	}
	return nil, false
}

// introWindowAccepts lets an intro peer through the TLS handshake. Like the
// JOIN window it is a coarse gate: every intro route re-checks the pin
// against the offer's peer card.
func (m *Manager) introWindowAccepts(rawCerts [][]byte) bool {
	_, ok := m.introPeerFromChain(rawCerts)
	return ok
}

func (m *Manager) mountIntroRoutes(r chi.Router) {
	r.Group(func(r chi.Router) {
		r.Use(m.introAuth(joinRateMutation, IntroRequestCap))
		r.Post("/fed/v1/intro/request", m.handleIntroRequest)
	})
	r.Group(func(r chi.Router) {
		r.Use(m.introAuth(joinRateConfirm, IntroConfirmCap))
		r.Post("/fed/v1/intro/confirm", m.handleIntroConfirm)
	})
	r.Group(func(r chi.Router) {
		r.Use(m.introAuth(joinRateRead, introGetBodyCap))
		r.Get("/fed/v1/intro/{offer_id}/accept", m.handleIntroAccept)
		r.Get("/fed/v1/intro/{offer_id}/status", m.handleIntroStatus)
	})
}

// introAuth requires a client chain that verifies against a roster card CA,
// rate-limits on the direct TCP peer (shared with JOIN), and caps the body
// before anything decodes it.
func (m *Manager) introAuth(rate joinRateClass, bodyCap int64) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
				httpError(w, http.StatusForbidden, "client certificate required")
				return
			}
			connKey := r.RemoteAddr
			if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
				connKey = host
			}
			now := time.Now()
			var allowed bool
			switch rate {
			case joinRateConfirm:
				allowed = m.joins.AllowConnConfirm(connKey, now)
			case joinRateRead:
				allowed = m.joins.AllowConnRead(connKey, now)
			default:
				allowed = m.joins.AllowConn(connKey, now)
			}
			if !allowed {
				w.Header().Set("Retry-After", "60")
				httpError(w, http.StatusTooManyRequests, "too many intro attempts")
				return
			}
			if r.ContentLength > bodyCap {
				httpError(w, http.StatusRequestEntityTooLarge, "request body exceeds the size limit")
				return
			}
			r.Body = http.MaxBytesReader(w, r.Body, bodyCap)
			raw := make([][]byte, 0, len(r.TLS.PeerCertificates))
			for _, c := range r.TLS.PeerCertificates {
				raw = append(raw, c.Raw)
			}
			peer, ok := m.introPeerFromChain(raw)
			if !ok {
				httpError(w, http.StatusForbidden, "client certificate does not chain to a roster card CA")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), introPeerKey{}, peer)))
		})
	}
}

func introPeerFrom(ctx context.Context) *introPeer {
	p, _ := ctx.Value(introPeerKey{}).(*introPeer)
	return p
}

// decodeIntroJSON reads the whole body under the cap, then decodes it
// strictly: unknown fields and anything after the one JSON value are refused.
func decodeIntroJSON(w http.ResponseWriter, r *http.Request, limit int64, dst any) bool {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, limit))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			httpError(w, http.StatusRequestEntityTooLarge, "request body exceeds the size limit")
			return false
		}
		httpError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		httpError(w, http.StatusBadRequest, "invalid request body")
		return false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid request body: trailing data")
		return false
	}
	return true
}

// introMismatch writes a retryable evidence mismatch.
func introMismatch(w http.ResponseWriter, code, msg string) {
	writeJSON(w, http.StatusConflict, map[string]any{"error": msg, "code": code, "retryable": true})
}

// ---------------------------------------------------------------------------
// Host handlers
// ---------------------------------------------------------------------------

func (m *Manager) handleIntroRequest(w http.ResponseWriter, r *http.Request) {
	peer := introPeerFrom(r.Context())
	var req IntroRequestWire
	if !decodeIntroJSON(w, r, IntroRequestCap, &req) {
		return
	}
	switch {
	case req.OrgID == "" || len(req.OrgID) > maxIntroOrgIDLen:
		httpError(w, http.StatusBadRequest, "invalid org_id")
		return
	case !introHex64.MatchString(req.OfferID):
		httpError(w, http.StatusBadRequest, "offer_id must be 64 lowercase hex")
		return
	case !introHex32.MatchString(req.GuestNonce):
		httpError(w, http.StatusBadRequest, "guest_nonce must be 32 lowercase hex")
		return
	}
	if err := req.Evidence.validate(); err != nil {
		httpError(w, http.StatusBadRequest, "invalid evidence: "+err.Error())
		return
	}
	ctx := r.Context()
	v, err := m.introOpenOffer(ctx, req.OrgID, req.OfferID, "host")
	if errors.Is(err, ErrIntroOfferUnknown) {
		httpError(w, http.StatusNotFound, "no such offer on this node")
		return
	}
	if err != nil {
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	if subtle.ConstantTimeCompare([]byte(v.Peer.Pin), []byte(peer.pin)) != 1 {
		httpError(w, http.StatusForbidden, "client certificate is not the offer peer's card CA")
		return
	}
	if cnErr := requireChainCN(peer.ca, v.Peer.ChainID); cnErr != nil {
		httpError(w, http.StatusForbidden, "peer CA does not name the card's chain")
		return
	}
	if req.Evidence.PolicyRevision != v.PolicyRevision || req.Evidence.PolicyDigest != v.PolicyDigest {
		introMismatch(w, "policy_mismatch", "the two nodes hold different org policies; retry after both refresh")
		return
	}
	if req.Evidence.RosterRevision != v.RosterRevision || req.Evidence.RosterDigest != v.RosterDigest {
		introMismatch(w, "roster_mismatch", "the two nodes hold different rosters; retry after both refresh")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusServiceUnavailable, "introduced links require the SQLite store backend")
		return
	}
	latest, err := ss.LatestIntroducedLinkByChain(ctx, v.Peer.ChainID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "introduced-link state unavailable")
		return
	}
	if latest != nil && store.IntroducedLinkInFlight(latest.State) {
		httpError(w, http.StatusConflict, "a link to this chain is already in flight")
		return
	}
	prior, err := m.IntroTermsDigest(v.Peer.ChainID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "chain terms unavailable")
		return
	}
	hostNonce, sessionID, seed := make([]byte, 16), make([]byte, 16), make([]byte, 32)
	for _, b := range [][]byte{hostNonce, sessionID, seed} {
		if _, rErr := randRead(b); rErr != nil {
			zeroize(seed)
			httpError(w, http.StatusInternalServerError, "randomness unavailable")
			return
		}
	}
	commit := SeedCommitHex(seed)
	transcript, err := introTranscriptFor(v, hex.EncodeToString(hostNonce), req.GuestNonce,
		req.Evidence.EnvelopeRev, req.Evidence.EnvelopeDigest, commit)
	if err != nil {
		zeroize(seed)
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	caPEM, err := m.ownCAPEM()
	if err != nil {
		zeroize(seed)
		httpError(w, http.StatusInternalServerError, "own CA unavailable")
		return
	}
	expires := time.Now().Add(introHostSessionTTL).Unix()
	if v.ExpiresAt < expires {
		expires = v.ExpiresAt
	}
	// The response and the session must not share the seed: once published,
	// a replacement or expiry can retire the session and zeroize its copy
	// while this handler is still answering.
	seedHex := hex.EncodeToString(seed)
	owned := append([]byte(nil), seed...)
	zeroize(seed)
	sess := &introHostSession{
		orgID: req.OrgID, offerID: req.OfferID, sessionID: hex.EncodeToString(sessionID),
		peerChain: v.Peer.ChainID, guestPin: peer.pin,
		guestCAPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: peer.ca.Raw}),
		transcript: transcript, e: IntroAttestation(transcript), seed: owned,
		priorTerms: prior, expiresAt: expires,
	}
	if err := m.putIntroHostSession(sess); err != nil {
		zeroize(owned)
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	if hook := m.introRequestPublished(); hook != nil {
		hook(sess)
	}
	m.logger.Info().Str("peer", v.Peer.ChainID).Str("offer", shortID(req.OfferID)).Msg("intro session opened")
	writeJSON(w, http.StatusOK, &IntroRequestResp{
		OfferID: req.OfferID, SessionID: sess.sessionID, HostNonce: hex.EncodeToString(hostNonce),
		Evidence: IntroEvidenceWire{
			PolicyRevision: v.PolicyRevision, PolicyDigest: v.PolicyDigest,
			RosterRevision: v.RosterRevision, RosterDigest: v.RosterDigest,
			EnvelopeRev: v.EnvelopeRev, EnvelopeDigest: v.EnvelopeDigest,
		},
		Seed: seedHex, SeedCommit: commit, HostCAPEM: string(caPEM), Transcript: transcript,
	})
}

// errIntroNotAccepted is the host's "owner has not accepted yet".
var errIntroNotAccepted = errors.New("no owner acceptance under the current envelope")

// introHostSessionCheck re-checks a host session against the current offer.
func (m *Manager) introHostSessionCheck(ctx context.Context, sess *introHostSession) error {
	tr, err := DecodeIntroTranscript(sess.transcript)
	if err != nil {
		return err
	}
	return m.introStaleCheck(sess.orgID, sess.offerID, "host", tr)(ctx)
}

func (m *Manager) handleIntroAccept(w http.ResponseWriter, r *http.Request) {
	peer := introPeerFrom(r.Context())
	offerID := chi.URLParam(r, "offer_id")
	sid := r.URL.Query().Get("session_id")
	if !introHex64.MatchString(offerID) || !introSessionPattern.MatchString(sid) {
		httpError(w, http.StatusBadRequest, "offer_id and session_id are required")
		return
	}
	sess := m.introHostSessionFor(offerID)
	if sess == nil || sess.sessionID != sid {
		httpError(w, http.StatusNotFound, "no such intro session")
		return
	}
	if subtle.ConstantTimeCompare([]byte(sess.guestPin), []byte(peer.pin)) != 1 {
		httpError(w, http.StatusForbidden, "intro session belongs to another peer")
		return
	}
	switch err := m.introHostSessionCheck(r.Context(), sess); {
	case errors.Is(err, errIntroNotAccepted):
		writeJSON(w, http.StatusAccepted, &IntroHostAcceptWire{OfferID: offerID, SessionID: sid})
		return
	case err != nil:
		m.dropIntroHostSession(sess)
		httpError(w, http.StatusConflict, "intro session is stale: "+err.Error())
		return
	}
	sig, err := SignIntro(m.agentKey, IntroHostAccept, sess.e)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "host signature unavailable")
		return
	}
	writeJSON(w, http.StatusOK, &IntroHostAcceptWire{OfferID: offerID, SessionID: sid, HostAcceptSig: hex.EncodeToString(sig)})
}

func (m *Manager) handleIntroConfirm(w http.ResponseWriter, r *http.Request) {
	peer := introPeerFrom(r.Context())
	var req IntroConfirmWire
	if !decodeIntroJSON(w, r, IntroConfirmCap, &req) {
		return
	}
	if !introHex64.MatchString(req.OfferID) || !introSessionPattern.MatchString(req.SessionID) {
		httpError(w, http.StatusBadRequest, "offer_id and session_id are required")
		return
	}
	guestSig, err1 := hex.DecodeString(req.GuestAcceptSig)
	ackSig, err2 := hex.DecodeString(req.GuestAckSig)
	if err1 != nil || err2 != nil || len(guestSig) != ed25519.SignatureSize || len(ackSig) != ed25519.SignatureSize {
		httpError(w, http.StatusBadRequest, "invalid guest signatures")
		return
	}
	if m.introFault("drop_confirm") {
		httpError(w, http.StatusServiceUnavailable, "simulated lost confirm (test fault)")
		return
	}
	ctx := r.Context()
	sess := m.introHostSessionFor(req.OfferID)
	if sess == nil || sess.sessionID != req.SessionID {
		// A retried confirm after the session ended: report the row, if any.
		if row := m.introHostRowFor(ctx, req.OfferID, req.SessionID, peer.pin); row != nil {
			writeJSON(w, http.StatusOK, &IntroConfirmResp{OfferID: req.OfferID, SessionID: req.SessionID, LinkID: row.LinkID, State: row.State, TxHash: row.TxHash})
			return
		}
		httpError(w, http.StatusNotFound, "no such intro session")
		return
	}
	if subtle.ConstantTimeCompare([]byte(sess.guestPin), []byte(peer.pin)) != 1 {
		httpError(w, http.StatusForbidden, "intro session belongs to another peer")
		return
	}
	tr, err := DecodeIntroTranscript(sess.transcript)
	if err != nil {
		m.dropIntroHostSession(sess)
		httpError(w, http.StatusInternalServerError, "intro session transcript unreadable")
		return
	}
	if !VerifyIntro(tr.Guest.AgentPub, IntroGuestAccept, sess.e, guestSig) ||
		!VerifyIntro(tr.Guest.AgentPub, IntroGuestAck, sess.e, ackSig) {
		httpError(w, http.StatusForbidden, "guest signatures do not verify over E_intro")
		return
	}
	hostSig, err := SignIntro(m.agentKey, IntroHostAccept, sess.e)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "host signature unavailable")
		return
	}
	done, seed, err := sess.beginConfirm()
	switch {
	case errors.Is(err, errIntroConfirmBusy):
		httpError(w, http.StatusConflict, err.Error()+"; retry")
		return
	case errors.Is(err, errIntroSessionRetired):
		if row := m.introHostRowFor(ctx, req.OfferID, req.SessionID, peer.pin); row != nil {
			writeJSON(w, http.StatusOK, &IntroConfirmResp{OfferID: req.OfferID, SessionID: req.SessionID, LinkID: row.LinkID, State: row.State, TxHash: row.TxHash})
			return
		}
		httpError(w, http.StatusNotFound, "no such intro session")
		return
	case done != "":
		m.writeIntroRowState(w, ctx, req, done)
		return
	}
	// This confirm owns the session from here; endConfirm releases it.
	linkID := ""
	defer func() { sess.endConfirm(linkID) }()
	defer zeroize(seed)
	if hold := m.introConfirmHold(); hold != nil {
		hold()
	}
	if staleErr := m.introHostSessionCheck(ctx, sess); staleErr != nil {
		m.dropIntroHostSession(sess)
		httpError(w, http.StatusConflict, "intro session is stale: "+staleErr.Error())
		return
	}
	terms, err := m.introTermsFor(tr.Kind, tr.Guest)
	if err != nil {
		m.dropIntroHostSession(sess)
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	act := &IntroActivation{
		Role: "host", OrgID: sess.orgID, SessionID: sess.sessionID, Transcript: sess.transcript,
		HostAcceptSig: hostSig, GuestAcceptSig: guestSig, GuestAckSig: ackSig,
		Evidence: introEvidenceOf(tr), RemoteCAPEM: sess.guestCAPEM,
		Seed:             append([]byte(nil), seed...),
		Terms:            terms,
		PriorTermsDigest: sess.priorTerms,
		Deadline:         time.Now().Add(m.introWindow()).Unix(),
		StaleCheck:       m.introStaleCheck(sess.orgID, sess.offerID, "host", tr),
	}
	res, err := m.ActivateIntroducedLink(ctx, act)
	switch {
	case err == nil:
		linkID = res.LinkID
		writeJSON(w, http.StatusOK, &IntroConfirmResp{OfferID: req.OfferID, SessionID: req.SessionID, LinkID: res.LinkID, State: res.State, TxHash: res.TxHash})
	case errors.Is(err, ErrIntroPending):
		linkID = hex.EncodeToString(sess.e[:])
		writeJSON(w, http.StatusOK, &IntroConfirmResp{OfferID: req.OfferID, SessionID: req.SessionID, LinkID: linkID, State: store.IntroducedLinkSigned})
	default:
		m.dropIntroHostSession(sess)
		var stale *IntroStaleError
		if errors.As(err, &stale) {
			httpError(w, http.StatusConflict, err.Error())
			return
		}
		m.logger.Warn().Err(err).Str("offer", shortID(req.OfferID)).Msg("intro confirm: host activation failed")
		httpError(w, http.StatusUnprocessableEntity, err.Error())
	}
}

func (m *Manager) writeIntroRowState(w http.ResponseWriter, ctx context.Context, req IntroConfirmWire, linkID string) {
	ss := m.syncStore()
	row, err := ss.GetIntroducedLink(ctx, linkID)
	if err != nil || row == nil {
		httpError(w, http.StatusInternalServerError, "introduced-link state unavailable")
		return
	}
	writeJSON(w, http.StatusOK, &IntroConfirmResp{OfferID: req.OfferID, SessionID: req.SessionID, LinkID: row.LinkID, State: row.State, TxHash: row.TxHash})
}

// introHostRowFor finds this host's newest row for the offer whose guest pin
// is the caller's.
func (m *Manager) introHostRowFor(ctx context.Context, offerID, sessionID, guestPin string) *store.IntroducedLink {
	ss := m.syncStore()
	if ss == nil {
		return nil
	}
	rows, err := ss.ListIntroducedLinks(ctx, "")
	if err != nil {
		return nil
	}
	for i := range rows {
		row := rows[i]
		if row.Role != "host" || row.OfferID != offerID || (sessionID != "" && row.SessionID != sessionID) {
			continue
		}
		tr, dErr := DecodeIntroTranscript(row.EIntro)
		if dErr != nil || subtle.ConstantTimeCompare([]byte(tr.Guest.Pin), []byte(guestPin)) != 1 {
			continue
		}
		return &row
	}
	return nil
}

func (m *Manager) handleIntroStatus(w http.ResponseWriter, r *http.Request) {
	peer := introPeerFrom(r.Context())
	offerID := chi.URLParam(r, "offer_id")
	if !introHex64.MatchString(offerID) {
		httpError(w, http.StatusBadRequest, "offer_id must be 64 lowercase hex")
		return
	}
	ctx := r.Context()
	resp := &IntroStatusResp{OfferID: offerID, State: IntroStatusNone}
	sess := m.introHostSessionFor(offerID)
	if sess != nil && subtle.ConstantTimeCompare([]byte(sess.guestPin), []byte(peer.pin)) != 1 {
		sess = nil
	}
	sid := ""
	if sess != nil {
		sid = sess.sessionID
	}
	if row := m.introHostRowFor(ctx, offerID, sid, peer.pin); row != nil {
		resp.SessionID, resp.LinkID, resp.State = row.SessionID, row.LinkID, row.State
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if sess != nil {
		resp.SessionID = sess.sessionID
		resp.State = IntroStatusAwaitingConfirm
		if errors.Is(m.introHostSessionCheck(ctx, sess), errIntroNotAccepted) {
			resp.State = IntroStatusAwaitingAccept
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

func (m *Manager) introWindow() time.Duration {
	if m.introGuestWindow > 0 {
		return m.introGuestWindow
	}
	return introDefaultWindow
}

// ---------------------------------------------------------------------------
// Client side (guest)
// ---------------------------------------------------------------------------

// introClientTLS presents this node's certificate chained to its own CA (the
// host admits the chain by the CA's card pin) and pins the server at
// handshake time:
//
//   - hostCA set: the server chain must verify against that CA (calls after
//     the first, and the window probe with the committed host CA);
//   - card set (the first request, before the host CA PEM is known): the
//     server chain must carry a CA whose SPKI is the host card's pin and
//     whose CN is the card's chain id, and the leaf must verify against it.
//     Anything else fails the handshake. The verified CA is stored in *seen
//     so the caller can require the response's CA to be the same one.
func (m *Manager) introClientTLS(hostCA *x509.Certificate, card *IntroCard, seen *[]byte) (*tls.Config, error) {
	var cfg *tls.Config
	var err error
	switch {
	case hostCA != nil:
		// The JOIN client config: the server chain must verify against the
		// pinned host CA.
		hostPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: hostCA.Raw})
		cfg, err = m.joinClientTLS(hostPEM, SPKIFingerprint(hostCA))
	case card != nil && seen != nil:
		// The JOIN bootstrap config with the card check as its verifier.
		cfg, err = m.joinBootstrapTLS()
		if err == nil {
			// VerifyConnection runs on every handshake, resumed ones included.
			pin, chain := card.Pin, card.ChainID
			cfg.SessionTicketsDisabled = true
			cfg.VerifyConnection = func(cs tls.ConnectionState) error {
				rawCerts := make([][]byte, 0, len(cs.PeerCertificates))
				for _, c := range cs.PeerCertificates {
					rawCerts = append(rawCerts, c.Raw)
				}
				ca, vErr := introCardCAFromChain(rawCerts, pin, chain, x509.ExtKeyUsageServerAuth)
				if vErr != nil {
					return vErr
				}
				*seen = append([]byte(nil), ca.Raw...)
				return nil
			}
		}
	default:
		return nil, errors.New("intro client TLS needs a host CA or a host card")
	}
	if err != nil {
		return nil, err
	}
	// Present the node certificate chained to this node's own CA.
	caPEM, err := m.ownCAPEM()
	if err != nil {
		return nil, err
	}
	blk, _ := pem.Decode(caPEM)
	if blk == nil || len(cfg.Certificates) != 1 {
		return nil, errors.New("own CA or node certificate unavailable")
	}
	cert := cfg.Certificates[0]
	cert.Certificate = append(append([][]byte(nil), cert.Certificate...), blk.Bytes)
	cfg.Certificates = []tls.Certificate{cert}
	cfg.SessionTicketsDisabled = true
	return cfg, nil
}

// introCardCAFromChain finds, in a presented chain, the CA whose SPKI is pin
// (64 lowercase hex) and whose CN is chainID, and verifies the leaf against
// it.
func introCardCAFromChain(rawCerts [][]byte, pin, chainID string, usage x509.ExtKeyUsage) (*x509.Certificate, error) {
	if len(rawCerts) < 2 {
		return nil, errors.New("intro: the peer chain carries no CA")
	}
	n := len(rawCerts)
	if n > 1+introMaxChainCerts {
		n = 1 + introMaxChainCerts
	}
	for _, raw := range rawCerts[1:n] {
		ca, err := x509.ParseCertificate(raw)
		if err != nil || !ca.IsCA || hex.EncodeToString(SPKIFingerprint(ca)) != pin {
			continue
		}
		if err := requireChainCN(ca, chainID); err != nil {
			return nil, fmt.Errorf("intro: the card-pinned CA does not name the card's chain: %w", err)
		}
		if err := verifyChainAgainstCA([][]byte{rawCerts[0]}, ca, usage); err != nil {
			return nil, fmt.Errorf("intro: the peer leaf does not verify against its card-pinned CA: %w", err)
		}
		return ca, nil
	}
	return nil, errors.New("intro: no CA in the peer chain carries the card pin")
}

// introCall makes one intro call to a peer card's endpoint (or its p2p
// routes), reading at most respCap bytes. It returns the status and body.
func (m *Manager) introCall(ctx context.Context, card IntroCard, cfg *tls.Config, method, path string, body any, respCap int64, timeout time.Duration) (int, []byte, error) {
	reqCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	base, err := introBaseURL(card)
	if err != nil {
		return 0, nil, err
	}
	u, err := netguard.JoinPath(base, path)
	if err != nil {
		return 0, nil, err
	}
	var reqBody io.Reader = http.NoBody
	if body != nil {
		payload, mErr := json.Marshal(body)
		if mErr != nil {
			return 0, nil, mErr
		}
		reqBody = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(reqCtx, method, u, reqBody)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	transport := joinHTTPTransport(cfg)
	if introUsesP2P(card) {
		dial := m.joinP2PHooks().DialTarget
		if dial == nil || len(card.Routes) == 0 {
			return 0, nil, errors.New("internet intro transport is unavailable")
		}
		transport = joinP2PHTTPTransport(cfg, dial, card.Routes)
	}
	client := &http.Client{Transport: transport, CheckRedirect: refuseJoinRedirect}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req) // lgtm[go/request-forgery] -- endpoint from a signed roster card; LAN-only dial or the card's p2p routes
	if err != nil {
		return 0, nil, fmt.Errorf("peer unreachable: %w", err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, respCap+1))
	if err != nil {
		return 0, nil, err
	}
	if int64(len(raw)) > respCap {
		return 0, nil, fmt.Errorf("peer response exceeds %d bytes", respCap)
	}
	return resp.StatusCode, raw, nil
}

func introUsesP2P(card IntroCard) bool {
	if card.Endpoint == joinP2POnlyEndpoint {
		return true
	}
	if _, err := netguard.LocalLANHTTPBase(card.Endpoint, "https"); err != nil {
		return len(card.Routes) > 0
	}
	return false
}

func introBaseURL(card IntroCard) (string, error) {
	if introUsesP2P(card) {
		return netguard.LocalLANHTTPBase(joinP2POnlyEndpoint, "https")
	}
	return netguard.LocalLANHTTPBase(card.Endpoint, "https")
}

// introPeerError turns a non-2xx intro response into an error.
func introPeerError(status int, raw []byte) error {
	var e struct {
		Error     string `json:"error"`
		Code      string `json:"code"`
		Retryable bool   `json:"retryable"`
	}
	_ = json.Unmarshal(raw, &e)
	msg := e.Error
	if msg == "" {
		msg = truncate(raw, 200)
	}
	if e.Code != "" {
		return fmt.Errorf("peer returned %d (%s): %s", status, e.Code, msg)
	}
	return fmt.Errorf("peer returned %d: %s", status, msg)
}

// introVerifyHostCA is defence in depth after the pinned handshake: the
// response's host CA must be the CA the handshake verified, carry the host
// card's pin and name the card's chain.
func introVerifyHostCA(caPEM []byte, host IntroCard, seenCA []byte) (*x509.Certificate, error) {
	ca, err := parseCACertPEM(caPEM)
	if err != nil {
		return nil, err
	}
	if len(seenCA) == 0 || subtle.ConstantTimeCompare(ca.Raw, seenCA) != 1 {
		return nil, errors.New("host CA in the response is not the CA its TLS chain presented")
	}
	if hex.EncodeToString(SPKIFingerprint(ca)) != host.Pin {
		return nil, errors.New("host CA is not the host card's pinned CA")
	}
	if err := requireChainCN(ca, host.ChainID); err != nil {
		return nil, err
	}
	return ca, nil
}

func randHex(n int) (string, error) {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
