package federation

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/subtle"
	"encoding/base32"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

// JoinSession is the HOST-side server state for one bilateral join ceremony
// (the host runs the listener; the guest POSTs /fed/v1/join/*). It carries the
// RT-6 freeze fields locked at approval #1 so the host can never broadcast
// against an attestation no human compared, and binds to exactly one guest
// identity + TLS client cert after the first request (RT-2/RT-5).
type JoinSession struct {
	ID          string
	State       string
	LocalChain  string // host chain
	CreatedAt   time.Time
	ExpiresAt   time.Time
	ConfirmStep int64

	// Host-side material (known at Create).
	HostPin      []byte
	HostEndpoint string
	Seed         []byte
	HostNonce    []byte
	HostPeerID   string
	HostP2PAddrs []string

	// ExpectedGuestPin is the guest CA SPKI the host scanned off the guest's
	// return QR over the authenticated visual channel (§2.2.5). It is the
	// ANCHOR: at the first /join/request the host asserts SPKIFingerprint(the
	// presented guest CA) == ExpectedGuestPin, so a body-supplied pin can never
	// substitute the scanned one. Set by ScanReturn BEFORE the guest requests.
	ExpectedGuestPin  []byte
	ExpectedGuestEnd  string // guest endpoint scanned off the return QR
	ExpectedGuestPeer string
	ExpectedGuestP2P  []string

	// Guest-side material (bound at the first Request - one-guest-identity).
	GuestChain     string
	GuestName      string // friendly label the guest chose (cosmetic, unauthenticated)
	GuestAgentPub  []byte // guest node-operator ed25519 pub (verifies guest_sig/ack over E)
	GuestPin       []byte
	GuestEndpoint  string
	GuestNonce     []byte
	GuestScope     [32]byte
	GuestScopeWire ScopeWire
	HostScope      [32]byte
	BoundCertSPKI  []byte // RT-2/RT-5: the TLS client-cert SPKI bound to this session

	// Host grant terms for cross_fed:<guest> (what the guest may read from the
	// host). Set by Approve from the operator's H3/H5 choices; folded into
	// HostScope (RT-9) and used to build the host's tx-33.
	HostGrantClearance uint8
	HostGrantDomains   []string
	HostGrantExpiry    int64
	HostGrantMode      string
	HostGrantDirection string

	// Staged, un-committed material held between /join/request and /join/confirm.
	// The guest CA is staged (pending sidecar) at request and committed only
	// AFTER the host's tx-33 lands (mirrors the CA stage→broadcast→commit rule).
	commitGuestCA   func() error
	rollbackGuestCA func()

	// RT-6 freeze (locked at HOST_APPROVED). Every guest field folded into E - or
	// used to build/verify the host's tx-33 - is snapshotted here, so a
	// concurrent/late re-request that mutates the live fields can never make the
	// host broadcast against, or verify signatures under, an identity no human
	// compared.
	Approved           bool
	ApprovedE          [32]byte
	ApprovedGuestPin   []byte
	ApprovedGuestNonce []byte
	ApprovedGuestEnd   string
	ApprovedGuestAgent []byte

	// ActivationTxHash is persisted at the same locked transition to ACTIVE.
	// A guest that loses the successful HTTP response may replay the exact signed
	// confirm and receive this result without broadcasting a second tx-33.
	ActivationTxHash string

	failCount int
}

// Join session states (§6.2).
const (
	JoinCreated        = "CREATED"
	JoinRequested      = "REQUESTED"
	JoinHostApproved   = "HOST_APPROVED"
	JoinGuestConfirmed = "GUEST_CONFIRMED"
	// JoinConfirming is the transient window between CheckConfirm handing the
	// commit/rollback closures to the confirm driver and MarkActive. The reaper
	// must not immediately reap it (its staged CA is being committed), and the
	// closures are already nil on the live session so the reaper cannot roll it
	// back out from under the in-flight broadcast.
	JoinConfirming = "CONFIRMING"
	JoinActive     = "ACTIVE"
	JoinAborted    = "ABORTED"
	JoinExpired    = "EXPIRED"
)

const (
	joinSessionTTL    = 15 * time.Minute
	joinSessionIDLen  = 8 // 64 bits ≥ the 40-bit RT-3 floor
	joinMaxFailPerSid = 8 // per-session-id fail cap (RT-3)
)

var joinB32 = base32.StdEncoding.WithPadding(base32.NoPadding)

var (
	// ErrJoinSessionNotFound distinguishes an already-reaped/unknown ceremony
	// from a valid session whose state no longer permits a Stop decision.
	ErrJoinSessionNotFound = errors.New("join session not found")
	// ErrJoinAbortConflict means confirmation has already crossed the atomic
	// commit boundary (CONFIRMING or ACTIVE), so reporting a successful Stop
	// would be false and could surprise the operator with an active pairing.
	ErrJoinAbortConflict = errors.New("join session can no longer be stopped")
	// ErrJoinAbortUnbound prevents one guest certificate from burning another
	// guest's ceremony.
	ErrJoinAbortUnbound = errors.New("join abort certificate is not bound to this session")
)

// JoinStore is the host-side session registry: TTL'd, single-ceremony, with a
// per-session fail cap and a TLS-connection rate limiter (never XFF, RT-3).
//
// The mutation limiter reserves a small tranche for final confirmation. Normal
// request/abort traffic may consume only the non-reserved portion, while signed
// confirms may consume the full (unchanged) aggregate cap. This keeps recovery
// reachable after earlier retries without increasing the accepted mutation
// budget. `rlRead` independently governs /join/ca and /join/status polling.
type JoinStore struct {
	mu       sync.Mutex
	sessions map[string]*JoinSession
	rl       *connRateLimiter
	rlRead   *connRateLimiter
}

func NewJoinStore() *JoinStore {
	return &JoinStore{
		sessions: make(map[string]*JoinSession),
		rl:       newConnRateLimiter(connMaxAttempts),
		rlRead:   newConnRateLimiter(connMaxReadAttempts),
	}
}

// Create opens a host-side session and returns it (state CREATED). The host
// then emits the enrollment QR carrying session_id + host_pin + endpoint + seed.
func (s *JoinStore) Create(localChain string, hostPin []byte, hostEndpoint string, seed []byte, now time.Time) (*JoinSession, error) {
	idb := make([]byte, joinSessionIDLen)
	if _, err := rand.Read(idb); err != nil {
		return nil, fmt.Errorf("join session id: %w", err)
	}
	hn := make([]byte, 16)
	if _, err := rand.Read(hn); err != nil {
		return nil, err
	}
	js := &JoinSession{
		ID: joinB32.EncodeToString(idb), State: JoinCreated,
		LocalChain: localChain, CreatedAt: now, ExpiresAt: now.Add(joinSessionTTL),
		HostPin: append([]byte(nil), hostPin...), HostEndpoint: hostEndpoint,
		Seed: append([]byte(nil), seed...), HostNonce: hn,
	}
	s.mu.Lock()
	s.sessions[js.ID] = js
	s.mu.Unlock()
	return js, nil
}

func (s *JoinStore) SetHostP2P(id, peerID string, addrs []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok || js.State != JoinCreated {
		return fmt.Errorf("join session is not available for p2p setup")
	}
	js.HostPeerID = peerID
	js.HostP2PAddrs = append([]string(nil), addrs...)
	return nil
}

// SessionID returns the base32 id for the QR (x_sage_sid).
func (js *JoinSession) SessionID() string { return js.ID }

// snapshot returns a copy of the session for lock-free reads. It MUST be called
// while holding s.mu. Every OTHER slice field is only ever reassigned (never
// mutated in place) by the mutators, so a shallow header copy is a consistent
// immutable view - EXCEPT Seed, which Cleanup scrubs in place for secret
// hygiene; so Seed is DEEP-copied here, otherwise a lock-free reader
// (confirmCodes) would race the reaper zeroing the shared backing array.
func (js *JoinSession) snapshot() *JoinSession {
	cp := *js
	cp.Seed = append([]byte(nil), js.Seed...)
	cp.HostP2PAddrs = append([]string(nil), js.HostP2PAddrs...)
	cp.ExpectedGuestP2P = append([]string(nil), js.ExpectedGuestP2P...)
	cp.GuestScopeWire.AllowedDomains = append([]string(nil), js.GuestScopeWire.AllowedDomains...)
	// The unexported commit/rollback closures are node-local ceremony plumbing;
	// never expose them on a read snapshot handed to lock-free callers.
	cp.commitGuestCA = nil
	cp.rollbackGuestCA = nil
	return &cp
}

// Get returns a locked snapshot of a live (non-expired) session, expiring it
// lazily. Callers read fields off the snapshot; all mutation goes through the
// id-based JoinStore methods.
func (s *JoinStore) Get(id string, now time.Time) (*JoinSession, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok {
		return nil, false
	}
	if now.After(js.ExpiresAt) && js.State != JoinActive {
		js.State = JoinExpired
		return nil, false
	}
	return js.snapshot(), true
}

// confirmCodes computes CODE_G/CODE_H from a session's own fields (host view).
// Pure over the session, so both the host wizard poll and the atomic approve
// compute an identical value.
func (js *JoinSession) confirmCodes() (codeG, codeH string) {
	return ConfirmCodes(js.Seed, js.GuestChain, js.GuestPin, js.LocalChain, js.HostPin,
		js.GuestNonce, js.HostNonce, js.ConfirmStep)
}

// attestation computes the frozen enrollment attestation E from the host view
// (host grant scope digest supplied).
func (js *JoinSession) attestation(hostScope [32]byte) [32]byte {
	return EnrollInputs{
		GuestChain:    js.GuestChain,
		HostChain:     js.LocalChain,
		GuestPin:      js.GuestPin,
		HostPin:       js.HostPin,
		GuestEndpoint: js.GuestEndpoint,
		HostEndpoint:  js.HostEndpoint,
		GuestScope:    js.GuestScope,
		HostScope:     hostScope,
		Seed:          js.Seed,
		GuestNonce:    js.GuestNonce,
		HostNonce:     js.HostNonce,
		GuestPeerID:   js.ExpectedGuestPeer,
		GuestP2PAddrs: js.ExpectedGuestP2P,
		HostPeerID:    js.HostPeerID,
		HostP2PAddrs:  js.HostP2PAddrs,
	}.Attestation()
}

// AllowConn reports whether a request from connKey (a TLS-connection-derived
// key, NOT XFF) is within the rate limit.
func (s *JoinStore) AllowConn(connKey string, now time.Time) bool {
	return s.rl.allowBelow(connKey, now, connMaxAttempts-connConfirmReserve)
}

// AllowConnConfirm may use the reserved tail of the same aggregate mutation
// budget. Invalid confirms still consume it and total accepted mutations remain
// capped at connMaxAttempts per direct source/window.
func (s *JoinStore) AllowConnConfirm(connKey string, now time.Time) bool {
	return s.rl.allow(connKey, now)
}

// AllowConnRead is the generous limiter for read-only polling routes
// (/join/ca, /join/status). Separate budget from AllowConn so a bound guest's
// legitimate ~2s status poll over a multi-minute ceremony is never throttled.
func (s *JoinStore) AllowConnRead(connKey string, now time.Time) bool {
	return s.rlRead.allow(connKey, now)
}

// SetExpectedGuest records the guest CA SPKI + endpoint the host scanned off the
// guest's return QR (§2.2.5) over the authenticated visual channel. This is the
// anchor the first /join/request asserts the presented guest CA against. Setting
// it does not advance state; it only arms the session to accept a request.
func (s *JoinStore) SetExpectedGuest(id string, pin []byte, endpoint string, now time.Time) error {
	return s.SetExpectedGuestWithP2P(id, pin, endpoint, "", nil, now)
}

func (s *JoinStore) SetExpectedGuestWithP2P(id string, pin []byte, endpoint, peerID string, addrs []string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok || (now.After(js.ExpiresAt) && js.State != JoinActive) {
		return fmt.Errorf("join session not found or expired")
	}
	if js.State != JoinCreated {
		return fmt.Errorf("guest return card is already bound for this session")
	}
	if len(pin) != 32 {
		return fmt.Errorf("scanned guest pin must be 32 bytes")
	}
	js.ExpectedGuestPin = append([]byte(nil), pin...)
	js.ExpectedGuestEnd = endpoint
	js.ExpectedGuestPeer = peerID
	js.ExpectedGuestP2P = append([]string(nil), addrs...)
	return nil
}

// GuestRequestInput carries everything the first /join/request binds.
type GuestRequestInput struct {
	GuestChain     string
	GuestName      string // friendly label (cosmetic, unauthenticated — NOT folded into E)
	GuestAgentPub  []byte
	GuestNonce     []byte
	GuestPin       []byte
	GuestEndpoint  string
	GuestScope     [32]byte
	GuestScopeWire ScopeWire
	CertSPKI       []byte // presented TLS client-cert leaf SPKI (RT-2/RT-5)
	// CommitGuestCA/RollbackGuestCA persist/discard the staged guest CA (held
	// until the host's tx-33 lands). Attached only on the first bind.
	CommitGuestCA   func() error
	RollbackGuestCA func()
}

// Request binds the guest identity + TLS cert to the session on the first call
// (one-guest-identity, RT-2/RT-5), asserts the presented guest pin equals the
// SCANNED anchor (ExpectedGuestPin), picks the confirm step, and moves to
// REQUESTED. A second request with a DIFFERENT guest identity/cert is refused;
// a re-request that mutates guest fields after approval resets the approval
// (enforced in Approve/Confirm via the frozen check). On the first successful
// bind the staged-CA closures are attached; a rejected first bind returns an
// error so the caller rolls back its own staged CA.
func (s *JoinStore) Request(id string, now time.Time, in GuestRequestInput) (*JoinSession, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok || (now.After(js.ExpiresAt) && js.State != JoinActive) {
		return nil, fmt.Errorf("join session not found or expired")
	}
	// Requests may only (re)bind a pre-confirmation session. Once CheckConfirm
	// transfers the frozen transcript and staged-CA closures to the driver, a
	// concurrent re-request must not mutate the live fields that ACTIVE replay
	// retains. In particular, accepting a re-request in CONFIRMING could clear
	// ApprovedE after the real tx uses its safe snapshot, corrupting the returned
	// group id and making recovery from a lost HTTP 200 impossible.
	switch js.State {
	case JoinCreated, JoinRequested, JoinHostApproved:
		// Allowed pre-confirmation states.
	default:
		return nil, fmt.Errorf("join request not allowed while session is %s", js.State)
	}
	if js.failCount >= joinMaxFailPerSid {
		return nil, fmt.Errorf("join session locked (too many attempts)")
	}
	// The host must have scanned the guest's return QR first: the ANCHOR is the
	// visually-scanned pin, never a body-supplied one.
	if len(js.ExpectedGuestPin) != 32 {
		return nil, fmt.Errorf("host has not scanned your connection code yet")
	}
	if subtle.ConstantTimeCompare(js.ExpectedGuestPin, in.GuestPin) != 1 {
		js.failCount++
		return nil, fmt.Errorf("guest key does not match the scanned connection code")
	}
	if js.ExpectedGuestEnd != in.GuestEndpoint {
		js.failCount++
		return nil, fmt.Errorf("guest endpoint does not match the scanned connection code")
	}
	if len(in.GuestAgentPub) == 0 {
		return nil, fmt.Errorf("missing guest agent key")
	}
	// One-guest-identity binding: after the first request, a different guest
	// chain or cert is refused (a distinct guest needs a distinct session).
	if js.GuestChain != "" {
		if js.GuestChain != in.GuestChain || subtle.ConstantTimeCompare(js.BoundCertSPKI, in.CertSPKI) != 1 {
			js.failCount++
			return nil, fmt.Errorf("join session already bound to a different guest")
		}
	}
	// A re-request that changes ANY guest field folded into E (pin, nonce,
	// endpoint, scope, agent key) after an approval resets it (RT-6: a
	// resumed/regenerated session needs a fresh human compare). CheckConfirm
	// verifies against the frozen snapshot regardless, but resetting here also
	// re-advances the state machine so the wizard re-drives the compare.
	if js.Approved && (subtle.ConstantTimeCompare(js.GuestPin, in.GuestPin) != 1 ||
		subtle.ConstantTimeCompare(js.GuestNonce, in.GuestNonce) != 1 ||
		js.GuestEndpoint != in.GuestEndpoint ||
		js.GuestScope != in.GuestScope ||
		subtle.ConstantTimeCompare(js.GuestAgentPub, in.GuestAgentPub) != 1) {
		js.Approved = false
		js.ApprovedE = [32]byte{}
		if js.State == JoinHostApproved {
			js.State = JoinRequested
		}
	}
	firstBind := js.GuestChain == ""
	js.GuestChain = in.GuestChain
	// GuestName is cosmetic + unauthenticated: stored for display only, never
	// folded into E and never a re-approval trigger. Refreshed each request.
	js.GuestName = in.GuestName
	js.GuestAgentPub = append([]byte(nil), in.GuestAgentPub...)
	js.GuestNonce = append([]byte(nil), in.GuestNonce...)
	js.GuestPin = append([]byte(nil), in.GuestPin...)
	js.GuestEndpoint = in.GuestEndpoint
	js.GuestScope = in.GuestScope
	js.GuestScopeWire = in.GuestScopeWire
	js.GuestScopeWire.AllowedDomains = append([]string(nil), in.GuestScopeWire.AllowedDomains...)
	js.BoundCertSPKI = append([]byte(nil), in.CertSPKI...)
	if firstBind {
		js.commitGuestCA = in.CommitGuestCA
		js.rollbackGuestCA = in.RollbackGuestCA
	} else if in.RollbackGuestCA != nil {
		// A re-request re-staged a fresh CA sidecar; drop it (the first bind's
		// staged CA is authoritative for this bound guest).
		in.RollbackGuestCA()
	}
	if js.ConfirmStep == 0 {
		js.ConfirmStep = now.Unix() / 30 // frozen for the whole join window
	}
	if js.State == JoinCreated {
		js.State = JoinRequested
	}
	return js.snapshot(), nil
}

// HostGrant is the host operator's chosen cross_fed:<guest> terms (H3/H5),
// recorded at approval and folded into HostScope (RT-9) + the host tx-33.
type HostGrant struct {
	Clearance uint8
	Domains   []string
	Expiry    int64
	Mode      string
	Direction string
	Scope     [32]byte // ScopeDigest of the above (recomputed identically by the guest)
}

// ApproveWithCode is approval #1, done ATOMICALLY: under a single lock it
// recomputes CODE_G from the CURRENT session, constant-time-compares it to what
// the operator typed (the code they heard the guest read), and - only on a
// match - records the host grant, computes the attestation E over exactly those
// same fields, and FREEZES the identity (RT-6). Doing the compare and the E
// computation in one critical section removes the TOCTOU where a concurrent
// re-request could shift the fields between "compared" and "frozen". A wrong
// code bumps the per-session fail cap. Returns whether the session is now locked.
func (s *JoinStore) ApproveWithCode(id, typedCode string, grant HostGrant) (locked bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok || js.State == JoinExpired || js.State == JoinAborted {
		return false, fmt.Errorf("join session not approvable")
	}
	if js.GuestChain == "" || (js.State != JoinRequested && js.State != JoinHostApproved) {
		return false, fmt.Errorf("no guest request to approve")
	}
	if js.failCount >= joinMaxFailPerSid {
		return true, fmt.Errorf("join session locked (too many attempts)")
	}
	codeG, _ := js.confirmCodes()
	if subtle.ConstantTimeCompare([]byte(strings.TrimSpace(typedCode)), []byte(codeG)) != 1 {
		js.failCount++
		return js.failCount >= joinMaxFailPerSid, fmt.Errorf("code does not match what the guest read")
	}
	js.HostGrantClearance = grant.Clearance
	js.HostGrantDomains = append([]string(nil), grant.Domains...)
	js.HostGrantExpiry = grant.Expiry
	js.HostGrantMode = grant.Mode
	js.HostGrantDirection = grant.Direction
	js.HostScope = grant.Scope
	js.Approved = true
	js.ApprovedE = js.attestation(grant.Scope)
	js.ApprovedGuestPin = append([]byte(nil), js.GuestPin...)
	js.ApprovedGuestNonce = append([]byte(nil), js.GuestNonce...)
	js.ApprovedGuestEnd = js.GuestEndpoint
	js.ApprovedGuestAgent = append([]byte(nil), js.GuestAgentPub...)
	js.State = JoinHostApproved
	return false, nil
}

// CheckConfirm verifies a confirm against the FROZEN attestation (RT-6): the
// approval must stand, the presented TLS cert must match the bound one, and the
// session's current guest fields must equal the frozen ones. It returns a COPY
// of the ceremony fields the confirm driver needs (frozen E, guest agent key,
// host grant, staged-CA committer) so the driver can broadcast + commit without
// holding the store lock. It does NOT advance state; the driver calls MarkActive
// after its tx-33 lands. Concurrent confirms are idempotent at the anchor layer
// (the existing-record short-circuit) and single-flow in practice.
func (s *JoinStore) CheckConfirm(id string, certSPKI, guestSig, guestAckSig []byte, now time.Time) (*ConfirmContext, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok {
		return nil, fmt.Errorf("join session not found")
	}
	// A successful host activation is replayable until the normal session grace
	// cleanup. This is recovery for a lost HTTP 200: authenticate the same bound
	// TLS certificate and re-verify both signatures over the retained frozen E,
	// then return the original tx result without transferring closures or
	// entering CONFIRMING again.
	if js.State == JoinActive {
		if subtle.ConstantTimeCompare(js.BoundCertSPKI, certSPKI) != 1 {
			return nil, fmt.Errorf("confirm from an unbound client certificate")
		}
		if !js.Approved || len(js.ApprovedGuestAgent) != ed25519.PublicKeySize || js.ActivationTxHash == "" {
			return nil, fmt.Errorf("active join session has no replayable confirmation")
		}
		pub := ed25519.PublicKey(js.ApprovedGuestAgent)
		if !VerifyEnroll(pub, js.ApprovedE, false, guestSig) || !VerifyEnroll(pub, js.ApprovedE, true, guestAckSig) {
			return nil, fmt.Errorf("guest approval signatures do not verify against the frozen attestation")
		}
		return &ConfirmContext{
			GuestChain:       js.GuestChain,
			ApprovedE:        js.ApprovedE,
			ActivationTxHash: js.ActivationTxHash,
			AlreadyActive:    true,
		}, nil
	}
	// Expiry gate (parity with Get/Request): never drive a confirm on a lapsed
	// session - combined with the reaper NOT reaping CONFIRMING/ACTIVE, this
	// closes the commit-vs-reap race on the staged CA sidecar.
	if now.After(js.ExpiresAt) {
		return nil, fmt.Errorf("join session expired")
	}
	if !js.Approved || js.State != JoinHostApproved {
		return nil, fmt.Errorf("join session not host-approved")
	}
	// RT-2/RT-5: the confirm must come from the same TLS client cert bound at
	// request time.
	if subtle.ConstantTimeCompare(js.BoundCertSPKI, certSPKI) != 1 {
		return nil, fmt.Errorf("confirm from an unbound client certificate")
	}
	// Reject on ANY mutation of the guest fields folded into the frozen E since
	// approval.
	if subtle.ConstantTimeCompare(js.ApprovedGuestPin, js.GuestPin) != 1 ||
		subtle.ConstantTimeCompare(js.ApprovedGuestNonce, js.GuestNonce) != 1 ||
		js.ApprovedGuestEnd != js.GuestEndpoint ||
		subtle.ConstantTimeCompare(js.ApprovedGuestAgent, js.GuestAgentPub) != 1 {
		js.Approved = false
		js.ApprovedE = [32]byte{}
		js.State = JoinRequested
		return nil, fmt.Errorf("guest identity changed since approval - re-confirm required")
	}
	// Verify the guest's approval-#2 signatures over the FROZEN E, using the
	// FROZEN agent key - INSIDE the lock, BEFORE any state change. A bad
	// signature leaves the session HOST_APPROVED (retryable) and never transfers
	// the staged-CA closures, so a bogus confirm can neither brick the ceremony
	// nor consume the pending sidecar.
	pub := ed25519.PublicKey(js.ApprovedGuestAgent)
	if !VerifyEnroll(pub, js.ApprovedE, false, guestSig) || !VerifyEnroll(pub, js.ApprovedE, true, guestAckSig) {
		return nil, fmt.Errorf("guest approval signatures do not verify against the frozen attestation")
	}
	// Signatures good: move to CONFIRMING and TRANSFER ownership of the staged-CA
	// closures to the driver - nil them on the live session so the reaper can
	// never roll back (or double-commit) the pending CA the driver is about to
	// commit. A second concurrent confirm now finds CONFIRMING (not
	// HOST_APPROVED) and is rejected, so the host broadcasts its tx-33 once.
	ctx := &ConfirmContext{
		GuestChain:    js.GuestChain,
		GuestName:     js.GuestName,
		GuestPin:      append([]byte(nil), js.ApprovedGuestPin...),
		GuestEndpoint: js.ApprovedGuestEnd,
		GuestPeerID:   js.ExpectedGuestPeer,
		GuestP2PAddrs: append([]string(nil), js.ExpectedGuestP2P...),
		GuestAgentID:  append([]byte(nil), js.ApprovedGuestAgent...),
		GuestScope: ScopeWire{
			MaxClearance:   js.GuestScopeWire.MaxClearance,
			AllowedDomains: append([]string(nil), js.GuestScopeWire.AllowedDomains...),
			Mode:           js.GuestScopeWire.Mode,
			Direction:      js.GuestScopeWire.Direction,
		},
		ApprovedE: js.ApprovedE,
		Seed:      append([]byte(nil), js.Seed...),
		HostGrant: HostGrant{
			Clearance: js.HostGrantClearance,
			Domains:   append([]string(nil), js.HostGrantDomains...),
			Expiry:    js.HostGrantExpiry,
			Mode:      js.HostGrantMode,
			Direction: js.HostGrantDirection,
			Scope:     js.HostScope,
		},
		commitGuestCA:   js.commitGuestCA,
		rollbackGuestCA: js.rollbackGuestCA,
	}
	js.commitGuestCA = nil
	js.rollbackGuestCA = nil
	js.State = JoinConfirming
	return ctx, nil
}

// ConfirmContext is the snapshot the confirm driver needs to broadcast the host
// tx-33 and commit the staged guest CA + the exact ceremony seed. The guest
// signatures were already verified against the frozen E inside CheckConfirm
// (under the lock); every field here is a copy - the driver holds no live
// pointer.
type ConfirmContext struct {
	GuestChain       string
	GuestName        string // friendly label the guest chose (cosmetic; for local display)
	GuestPin         []byte
	GuestEndpoint    string
	GuestPeerID      string
	GuestP2PAddrs    []string
	GuestAgentID     []byte
	GuestScope       ScopeWire
	ApprovedE        [32]byte
	Seed             []byte // the ceremony's own seed (NOT re-resolved by chain id)
	HostGrant        HostGrant
	ActivationTxHash string
	AlreadyActive    bool
	commitGuestCA    func() error
	rollbackGuestCA  func()
}

// CommitGuestCA persists the staged guest CA (called by the driver ONLY after
// the host's tx-33 lands on-chain).
func (c *ConfirmContext) CommitGuestCA() error {
	if c.commitGuestCA == nil {
		return nil
	}
	return c.commitGuestCA()
}

// RollbackGuestCA discards the staged guest CA sidecar (called by the driver if
// its tx-33 broadcast fails, so a failed confirm leaks no pending file). The
// driver owns these closures exclusively after CheckConfirm, so the reaper never
// races this.
func (c *ConfirmContext) RollbackGuestCA() {
	if c.rollbackGuestCA != nil {
		c.rollbackGuestCA()
	}
}

// scrubSeed zeroizes the in-memory ceremony seed. Safe once the session is
// terminal (Abort) or its persistent per-agreement seed is committed
// (MarkActive): live TOTP verification reads the seed cache, never
// JoinSession.Seed, so nothing needs the ceremony copy after that point.
func (js *JoinSession) scrubSeed() {
	for i := range js.Seed {
		js.Seed[i] = 0
	}
}

// MarkActive transitions CONFIRMING to ACTIVE after the host broadcasts its
// tx-33 (the staged guest CA is committed by the driver first). The state
// condition is deliberately checked under the same lock as Abort: neither a
// late Stop nor an unrelated terminal transition may be overwritten.
func (s *JoinStore) MarkActive(id, txHash string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok {
		return ErrJoinSessionNotFound
	}
	if js.State != JoinConfirming {
		return fmt.Errorf("activate join from %s: %w", js.State, ErrJoinAbortConflict)
	}
	if strings.TrimSpace(txHash) == "" {
		return fmt.Errorf("activate join without transaction hash")
	}
	js.State = JoinActive
	js.ActivationTxHash = txHash
	js.rollbackGuestCA = nil // committed; do not roll back on later cleanup
	js.commitGuestCA = nil
	js.scrubSeed() // persistent seed is committed; drop the ceremony copy now
	return nil
}

// abortLocked performs the human Stop transition. CONFIRMING is already owned
// by the confirm driver and ACTIVE must use permanent revocation, so both are
// conflicts rather than false-success ABORTED responses.
func (s *JoinStore) abortLocked(js *JoinSession) error {
	switch js.State {
	case JoinAborted:
		return nil // idempotent Stop
	case JoinConfirming, JoinActive:
		return fmt.Errorf("join session is %s: %w", js.State, ErrJoinAbortConflict)
	case JoinExpired:
		return fmt.Errorf("join session is expired: %w", ErrJoinAbortConflict)
	}
	if js.rollbackGuestCA != nil {
		js.rollbackGuestCA()
		js.rollbackGuestCA = nil
		js.commitGuestCA = nil
	}
	js.scrubSeed()
	js.State = JoinAborted
	return nil
}

// Abort atomically burns a locally controlled, pre-confirm session.
func (s *JoinStore) Abort(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok {
		return ErrJoinSessionNotFound
	}
	return s.abortLocked(js)
}

// AbortBound combines guest-certificate authorization and the Stop transition
// in one critical section, closing the Get -> check -> Abort race with confirm.
func (s *JoinStore) AbortBound(id string, certSPKI []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	js, ok := s.sessions[id]
	if !ok {
		return ErrJoinSessionNotFound
	}
	if len(js.BoundCertSPKI) == 0 || subtle.ConstantTimeCompare(js.BoundCertSPKI, certSPKI) != 1 {
		return ErrJoinAbortUnbound
	}
	return s.abortLocked(js)
}

// FailConfirm is the driver's internal rollback path after CheckConfirm has
// transferred ownership of the staged artifacts. It is intentionally not used
// by operator-facing Stop routes.
func (s *JoinStore) FailConfirm(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if js, ok := s.sessions[id]; ok && js.State == JoinConfirming {
		js.scrubSeed()
		js.State = JoinAborted
	}
}

// OpenSessions returns SNAPSHOTS of the sessions still in flight (for the
// verifyFederationClientCert extension: their bound guest certs authenticate).
// Snapshots so the handshake path never dereferences a live session off-lock.
func (s *JoinStore) OpenSessions() []*JoinSession {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*JoinSession, 0, len(s.sessions))
	now := time.Now()
	for _, js := range s.sessions {
		if now.After(js.ExpiresAt) {
			continue
		}
		if js.State == JoinAborted || js.State == JoinExpired || js.State == JoinActive {
			continue
		}
		out = append(out, js.snapshot())
	}
	return out
}

// Cleanup drops expired/terminal sessions, rolling back any staged-but-
// uncommitted guest CA sidecar for a session that expired mid-ceremony and
// zeroizing its shared seed (secret hygiene). A session lingers only until its
// TTL lapses; the reaper (Manager.StartMaintenance) invokes this periodically.
func (s *JoinStore) Cleanup(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, js := range s.sessions {
		// CONFIRMING and ACTIVE are protected from the immediate expiry-reap: a
		// CONFIRMING session's staged CA is being committed by the driver (which
		// owns the closures), and an ACTIVE one is done. Both are only reaped
		// after the TTL grace window, by which time any in-flight confirm
		// (bounded by the broadcast timeout) has long finished.
		protected := js.State == JoinActive || js.State == JoinConfirming
		terminal := js.State == JoinAborted || js.State == JoinExpired ||
			(now.After(js.ExpiresAt) && !protected)
		if terminal || now.After(js.ExpiresAt.Add(joinSessionTTL)) {
			if !protected && js.rollbackGuestCA != nil {
				js.rollbackGuestCA()
			}
			for i := range js.Seed {
				js.Seed[i] = 0
			}
			delete(s.sessions, id)
		}
	}
}

// Maintain reaps expired/terminal sessions and drains the rate-limiter map.
// Invoked periodically by Manager.StartMaintenance.
func (s *JoinStore) Maintain(now time.Time) {
	s.Cleanup(now)
	s.rl.sweep(now)
	s.rlRead.sweep(now)
}

// connRateLimiter is the RT-3 rate limiter - keyed on a TLS-connection-derived
// value the caller supplies (never X-Forwarded-For). Same token-bucket shape as
// web/pairing.go's redeemRateLimiter, re-keyed safely for the direct-connect
// federation listener. The per-window cap is set per instance so a strict
// (code-submitting) and a generous (read-only polling) limiter can coexist.
type connRateLimiter struct {
	mu          sync.Mutex
	maxAttempts int
	attempts    map[string][]time.Time
}

const (
	// connMaxAttempts caps ALL state-changing routes per source per window.
	connMaxAttempts = 20
	// connConfirmReserve keeps enough of that unchanged aggregate cap available
	// for an initial final submit, a same-tick duplicate, and explicit recovery
	// after a lost response. Request/abort traffic cannot consume this tranche.
	connConfirmReserve = 4
	// connMaxReadAttempts caps the read-only routes (/join/ca, /join/status).
	// Generous because a bound guest legitimately polls status every ~2s for the
	// full ceremony; at 30/min a strict cap wrongly throttles the poll and the
	// guest never sees the host's approval. A read carries no code, so it cannot
	// brute-force — the cap only bounds a spray.
	connMaxReadAttempts = 240
	connWindow          = 1 * time.Minute
)

func newConnRateLimiter(maxAttempts int) *connRateLimiter {
	return &connRateLimiter{maxAttempts: maxAttempts, attempts: make(map[string][]time.Time)}
}

func (rl *connRateLimiter) allow(key string, now time.Time) bool {
	return rl.allowBelow(key, now, rl.maxAttempts)
}

func (rl *connRateLimiter) allowBelow(key string, now time.Time, cap int) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := now.Add(-connWindow)
	fresh := rl.attempts[key][:0]
	for _, t := range rl.attempts[key] {
		if t.After(cutoff) {
			fresh = append(fresh, t)
		}
	}
	if len(fresh) >= cap {
		rl.attempts[key] = fresh
		return false
	}
	rl.attempts[key] = append(fresh, now)
	return true
}

// sweep drops keys whose window has fully drained, so the outer map cannot grow
// unboundedly under connections from many/rotating source addresses (a stranger
// spraying the open pre-bind join window). Called by the periodic reaper.
func (rl *connRateLimiter) sweep(now time.Time) {
	rl.mu.Lock()
	defer rl.mu.Unlock()
	cutoff := now.Add(-connWindow)
	for key, ts := range rl.attempts {
		live := ts[:0]
		for _, t := range ts {
			if t.After(cutoff) {
				live = append(live, t)
			}
		}
		if len(live) == 0 {
			delete(rl.attempts, key)
		} else {
			rl.attempts[key] = live
		}
	}
}
