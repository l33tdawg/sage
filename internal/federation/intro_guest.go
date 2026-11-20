package federation

// The guest half of the intro wire (RFC #443 Phase B2b, contract v2 §2).
//
// After the guest owner accepts an offer, a driver runs:
//
//	request (mTLS, pinned by the host card) → rebuild E_intro locally and
//	require the host's bytes to match → poll accept → verify host-accept →
//	sign guest-accept + guest-ack → ActivateIntroducedLink(guest) →
//	confirm → host active ⇒ GuestIntroConfirmed
//
// The driver holds the seed in memory only. A restart before
// ActivateIntroducedLink loses nothing durable: the maintenance tick sees the
// acceptance again and starts a fresh session. After it, the row is the state
// and the reconciler owns it: the bound_pending_peer window asks the host for
// its status (re-sending confirm while the host still waits for it) and
// revokes on the deadline.

import (
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	introDefaultPoll = 3 * time.Second
	introMaxBackoff  = time.Minute
)

func (m *Manager) introPollInterval() time.Duration {
	if m.introPoll > 0 {
		return m.introPoll
	}
	return introDefaultPoll
}

// errIntroGuestStop ends a driver run without retrying.
var errIntroGuestStop = errors.New("intro guest: stop")

// StartIntroGuest starts (or restarts) the guest driver for an accepted
// offer. It returns once the driver is running; the driver itself is bounded
// by the offer expiry.
func (m *Manager) StartIntroGuest(orgID, offerID string) error {
	v, err := m.introOpenOffer(context.Background(), orgID, offerID, "guest")
	if err != nil {
		return err
	}
	if !v.AcceptanceCurrent() {
		return errIntroNotAccepted
	}
	m.spawnIntroGuest(context.Background(), v)
	return nil
}

func (m *Manager) spawnIntroGuest(parent context.Context, v *IntroOfferView) {
	// Bounded by parent (the maintenance loop, or Background for an owner
	// accept), the offer expiry and StopIntroGuests.
	ctx, cancel := context.WithDeadline(parent, time.Unix(v.ExpiresAt, 0))
	m.intro.mu.Lock()
	if m.intro.running == nil {
		m.intro.running = map[string]context.CancelFunc{}
	}
	if m.intro.running[v.OfferID] != nil {
		m.intro.mu.Unlock()
		cancel()
		return
	}
	m.intro.running[v.OfferID] = cancel
	m.intro.wg.Add(1)
	m.intro.mu.Unlock()
	go func() {
		defer m.intro.wg.Done()
		defer func() {
			cancel()
			m.intro.mu.Lock()
			delete(m.intro.running, v.OfferID)
			m.intro.mu.Unlock()
		}()
		m.runIntroGuest(ctx, v.OrgID, v.OfferID)
	}()
}

// StopIntroGuests cancels every running guest driver (shutdown). An accepted
// offer is resumed by the next maintenance tick after a restart.
func (m *Manager) StopIntroGuests() {
	m.intro.mu.Lock()
	defer m.intro.mu.Unlock()
	for _, cancel := range m.intro.running {
		cancel()
	}
}

// waitIntroGuests waits for stopped drivers to return (tests).
func (m *Manager) waitIntroGuests() { m.intro.wg.Wait() }

// resumeIntroGuests restarts the driver for every accepted guest offer that is
// still open. Called on the maintenance tick.
func (m *Manager) resumeIntroGuests(ctx context.Context) {
	src := m.introOrgSource()
	if src == nil {
		return
	}
	refs, err := src.AcceptedIntroOffers(ctx)
	if err != nil {
		m.logger.Debug().Err(err).Msg("intro guest resume: acceptances unavailable")
		return
	}
	for _, ref := range refs {
		v, oErr := m.introOpenOffer(ctx, ref.OrgID, ref.OfferID, "guest")
		if oErr != nil || !v.AcceptanceCurrent() {
			continue
		}
		m.spawnIntroGuest(ctx, v)
	}
}

func (m *Manager) runIntroGuest(ctx context.Context, orgID, offerID string) {
	backoff := m.introPollInterval()
	for {
		err := m.introGuestAttempt(ctx, orgID, offerID)
		if err == nil || errors.Is(err, errIntroGuestStop) {
			if err != nil {
				m.logger.Info().Err(err).Str("offer", shortID(offerID)).Msg("intro guest stopped")
			}
			return
		}
		m.logger.Warn().Err(err).Str("offer", shortID(offerID)).Msg("intro guest attempt failed; retrying")
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > introMaxBackoff {
			backoff = introMaxBackoff
		}
	}
}

func introStop(err error) error { return fmt.Errorf("%w: %v", errIntroGuestStop, err) }

// introGuestAttempt runs one session. nil means the guest row exists (the
// reconciler owns the rest); errIntroGuestStop means do not retry.
func (m *Manager) introGuestAttempt(ctx context.Context, orgID, offerID string) error {
	v, err := m.introOpenOffer(ctx, orgID, offerID, "guest")
	if err != nil {
		return introStop(err)
	}
	if !v.AcceptanceCurrent() {
		return introStop(errIntroNotAccepted)
	}
	host := v.Peer
	prior, err := m.IntroTermsDigest(host.ChainID)
	if err != nil {
		return err
	}
	nonce, err := randHex(16)
	if err != nil {
		return err
	}

	// 1. Request. The host CA is not known yet: record the server chain and
	// verify it against the card-pinned CA before using anything returned.
	var captured [][]byte
	cfg, err := m.introClientTLS(nil, &captured)
	if err != nil {
		return err
	}
	req := &IntroRequestWire{
		OrgID: orgID, OfferID: offerID, GuestNonce: nonce,
		Evidence: IntroEvidenceWire{
			PolicyRevision: v.PolicyRevision, PolicyDigest: v.PolicyDigest,
			RosterRevision: v.RosterRevision, RosterDigest: v.RosterDigest,
			EnvelopeRev: v.EnvelopeRev, EnvelopeDigest: v.EnvelopeDigest,
		},
	}
	status, raw, err := m.introCall(ctx, host, cfg, http.MethodPost, "/fed/v1/intro/request", req, IntroRequestRespCap, joinCallTimeout)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return introPeerError(status, raw)
	}
	var resp IntroRequestResp
	if uErr := json.Unmarshal(raw, &resp); uErr != nil {
		return fmt.Errorf("intro request: bad response: %w", uErr)
	}
	hostCA, err := introVerifyHostCA([]byte(resp.HostCAPEM), host, captured)
	if err != nil {
		// Not an availability problem: the endpoint is not the card's node.
		return introStop(err)
	}
	seed, err := hex.DecodeString(resp.Seed)
	if err != nil || len(seed) != 32 {
		return errors.New("intro request: bad seed")
	}
	defer zeroize(seed)
	if resp.OfferID != offerID || !introSessionPattern.MatchString(resp.SessionID) || !introHex32.MatchString(resp.HostNonce) {
		return errors.New("intro request: malformed response")
	}
	if SeedCommitHex(seed) != resp.SeedCommit {
		return errors.New("intro request: seed does not match its commitment")
	}
	if evErr := resp.Evidence.validate(); evErr != nil {
		return fmt.Errorf("intro request: host evidence: %w", evErr)
	}
	transcript, err := introTranscriptFor(v, resp.HostNonce, nonce, resp.Evidence.EnvelopeRev, resp.Evidence.EnvelopeDigest, resp.SeedCommit)
	if err != nil {
		return err
	}
	if string(transcript) != string(resp.Transcript) {
		return errors.New("intro request: the host's E_intro differs from this node's")
	}
	tr, err := DecodeIntroTranscript(transcript)
	if err != nil {
		return err
	}
	e := IntroAttestation(transcript)
	stale := m.introStaleCheck(orgID, offerID, "guest", tr)

	// 2. Wait for the host owner.
	pinned, err := m.introClientTLS(hostCA, nil)
	if err != nil {
		return err
	}
	acceptPath := "/fed/v1/intro/" + offerID + "/accept?session_id=" + url.QueryEscape(resp.SessionID)
	var hostSig []byte
	for hostSig == nil {
		if sErr := stale(ctx); sErr != nil {
			return introStop(sErr)
		}
		st, body, cErr := m.introCall(ctx, host, pinned, http.MethodGet, acceptPath, nil, IntroAcceptRespCap, joinCallTimeout)
		switch {
		case cErr != nil:
			return cErr
		case st == http.StatusOK:
			var a IntroHostAcceptWire
			if uErr := json.Unmarshal(body, &a); uErr != nil {
				return fmt.Errorf("intro accept: bad response: %w", uErr)
			}
			sig, dErr := hex.DecodeString(a.HostAcceptSig)
			if dErr != nil || a.OfferID != offerID || a.SessionID != resp.SessionID ||
				!VerifyIntro(tr.Host.AgentPub, IntroHostAccept, e, sig) {
				return errors.New("intro accept: host-accept does not verify over E_intro")
			}
			hostSig = sig
		case st == http.StatusAccepted:
			select {
			case <-ctx.Done():
				return introStop(ctx.Err())
			case <-time.After(m.introPollInterval()):
			}
		default:
			return introPeerError(st, body)
		}
	}

	// 3. Both acceptances exist: sign, then activate this side.
	guestSig, err := SignIntro(m.agentKey, IntroGuestAccept, e)
	if err != nil {
		return err
	}
	ackSig, err := SignIntro(m.agentKey, IntroGuestAck, e)
	if err != nil {
		return err
	}
	pin, _ := hex.DecodeString(host.Pin)
	deadline := time.Now().Add(m.introWindow()).Unix()
	act := &IntroActivation{
		Role: "guest", OrgID: orgID, SessionID: resp.SessionID, Transcript: transcript,
		HostAcceptSig: hostSig, GuestAcceptSig: guestSig, GuestAckSig: ackSig,
		Evidence: introEvidenceOf(tr), RemoteCAPEM: []byte(resp.HostCAPEM),
		Seed: append([]byte(nil), seed...),
		Terms: &tx.CrossFedTerms{
			RemoteChainID: host.ChainID, Endpoint: host.Endpoint, PeerPubKey: pin,
			MaxClearance: tx.ClearanceLevel(trustOnlyJoinScope.MaxClearance), Status: "active",
		},
		PriorTermsDigest: prior, Deadline: deadline, StaleCheck: stale,
	}
	res, err := m.ActivateIntroducedLink(ctx, act)
	switch {
	case errors.Is(err, ErrIntroPending):
		// The reconciler resolves the tx and then runs the window, which
		// re-sends confirm while the host waits for it.
		return nil
	case err != nil:
		return introStop(err)
	}

	// 4. Confirm. A lost answer is fine: the window asks again.
	cr, err := m.introConfirm(ctx, host, pinned, offerID, resp.SessionID, guestSig, ackSig)
	if err != nil {
		m.logger.Warn().Err(err).Str("offer", shortID(offerID)).Msg("intro confirm failed; the guest window will retry")
		return nil
	}
	if cr.State == store.IntroducedLinkActive {
		if cErr := m.GuestIntroConfirmed(ctx, res.LinkID); cErr != nil {
			m.logger.Warn().Err(cErr).Str("link", res.LinkID).Msg("intro guest: confirm recorded late")
		}
	}
	return nil
}

func (m *Manager) introConfirm(ctx context.Context, host IntroCard, cfg *tls.Config, offerID, sessionID string, guestSig, ackSig []byte) (*IntroConfirmResp, error) {
	body := &IntroConfirmWire{
		OfferID: offerID, SessionID: sessionID,
		GuestAcceptSig: hex.EncodeToString(guestSig), GuestAckSig: hex.EncodeToString(ackSig),
	}
	st, raw, err := m.introCall(ctx, host, cfg, http.MethodPost, "/fed/v1/intro/confirm", body, IntroConfirmRespCap, introConfirmTimeout)
	if err != nil {
		return nil, err
	}
	if st != http.StatusOK {
		return nil, introPeerError(st, raw)
	}
	var out IntroConfirmResp
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("intro confirm: bad response: %w", err)
	}
	if out.OfferID != offerID || out.SessionID != sessionID {
		return nil, errors.New("intro confirm: response names another session")
	}
	return &out, nil
}

// introPeerStatus is the probe the guest window uses: the test seam if set,
// else the wire probe when the intro wire is configured.
func (m *Manager) introPeerStatus() func(ctx context.Context, row *store.IntroducedLink) (string, error) {
	if m.introPeerStatusFn != nil {
		return m.introPeerStatusFn
	}
	if m.introOrgSource() == nil {
		return nil
	}
	return m.introProbeHost
}

// introProbeHost asks the host for its row's state. While the host still
// waits for confirm it re-sends the stored guest signatures. "" means no
// verdict yet.
func (m *Manager) introProbeHost(ctx context.Context, row *store.IntroducedLink) (string, error) {
	tr, err := DecodeIntroTranscript(row.EIntro)
	if err != nil {
		return "", err
	}
	pin, err := hex.DecodeString(tr.Host.Pin)
	if err != nil {
		return "", err
	}
	hostCA, err := m.loadPinnedRemoteCA(row.RemoteChainID, pin)
	if err != nil {
		return "", fmt.Errorf("host CA: %w", err)
	}
	cfg, err := m.introClientTLS(hostCA, nil)
	if err != nil {
		return "", err
	}
	host := IntroCard{
		AgentPub: tr.Host.AgentPub, ChainID: tr.Host.ChainID, Pin: tr.Host.Pin,
		Endpoint: tr.Host.Endpoint, PeerID: tr.Host.PeerID, Routes: tr.Host.P2PAddrs,
	}
	st, raw, err := m.introCall(ctx, host, cfg, http.MethodGet, "/fed/v1/intro/"+tr.OfferID+"/status", nil, IntroStatusRespCap, joinCallTimeout)
	if err != nil {
		return "", err
	}
	if st != http.StatusOK {
		return "", introPeerError(st, raw)
	}
	var s IntroStatusResp
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", fmt.Errorf("intro status: bad response: %w", err)
	}
	if s.OfferID != tr.OfferID {
		return "", errors.New("intro status: response names another offer")
	}
	state := s.State
	if state == IntroStatusAwaitingConfirm && s.SessionID == row.SessionID {
		cr, cErr := m.introConfirm(ctx, host, cfg, tr.OfferID, row.SessionID, row.GuestAccept, row.GuestAckSig)
		if cErr != nil {
			return "", cErr
		}
		state = cr.State
	}
	if s.SessionID != "" && s.SessionID != row.SessionID && state != IntroStatusNone {
		// The host is on another session for this offer: ours can never
		// complete there.
		return store.IntroducedLinkAborted, nil
	}
	switch state {
	case store.IntroducedLinkActive:
		return store.IntroducedLinkActive, nil
	case store.IntroducedLinkAborted, store.IntroducedLinkRevoked, IntroStatusNone:
		// The host has no live session or row for ours: it can never confirm.
		return store.IntroducedLinkAborted, nil
	}
	return "", nil
}
