package federation

// Introduced-link activation write path (RFC #443 Phase B2a, design §3.3).
//
// The introduced_link row (node-local SQLite) and the tx-33 broadcast are
// separate commit domains, so activation is a durable state machine:
//
//	stale gate → stage CA + seed → intent → sign (CAS intent→signed with the
//	exact bytes) → broadcast those bytes → committed → bind → active (host) /
//	bound_pending_peer (guest)
//
// Everything runs under LockAgreementMutation and one sync-policy generation
// mutation for the remote. A crash at any point leaves a row the reconciler
// (introduced_reconcile.go) can finish or unwind without ever broadcasting
// anything but the stored bytes. ActiveAgreement denies the agreement until
// the row is active (introduced_link_gate.go).
//
// Phase B2a ships the engine only: no production path calls
// ActivateIntroducedLink yet. The /fed/v1/intro/* wire and owner accept
// (B2b) build an IntroActivation and call it.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// IntroActivation is one fully accepted introduced session: E_intro and all
// three acceptance signatures exist before this side signs its tx-33.
type IntroActivation struct {
	// Role is this node's role in the transcript: "host" or "guest".
	Role string
	// ControlActorID is the local Root/Admin actor the tx-33 is signed for
	// ("" = the current Root).
	ControlActorID string
	OrgID          string
	SessionID      string
	// Transcript is the canonical E_intro.
	Transcript                                 []byte
	HostAcceptSig, GuestAcceptSig, GuestAckSig []byte
	// Evidence is the canonical JSON of the policy/roster/card/envelope
	// revisions and digests on both sides.
	Evidence []byte
	// RemoteCAPEM is the peer CA; its SPKI pin must equal the transcript pin.
	RemoteCAPEM []byte
	// Seed is the pair seed whose commitment is in the transcript. It is
	// zeroized before ActivateIntroducedLink returns.
	Seed []byte
	// Terms are the exact tx-33 terms this side commits for the peer.
	Terms *tx.CrossFedTerms
	// PriorTermsDigest is the chain terms digest for the peer when the owner
	// accepted (IntroTermsDigest), or "absent".
	PriorTermsDigest string
	// Deadline bounds the guest's bound_pending_peer window.
	Deadline int64
	// StaleCheck re-checks the org evidence under the agreement lease: offer
	// expiry, policy/roster/card/envelope revisions, PeerEligible and envelope
	// coverage. A nil StaleCheck refuses the activation.
	StaleCheck func(ctx context.Context) error
}

// IntroStaleError is a stale-intent gate refusal. Nothing was staged,
// written, signed or broadcast.
type IntroStaleError struct{ Which string }

func (e *IntroStaleError) Error() string { return "introduced link: stale: " + e.Which }

// ErrIntroPending means the tx-33 outcome is unknown (indeterminate submit).
// The row stays signed, ActiveAgreement keeps denying, and the reconciler
// resolves it.
var ErrIntroPending = errors.New("introduced link: tx-33 outcome pending reconciliation")

// IntroActivationResult reports where a successful call left the row.
type IntroActivationResult struct {
	LinkID string
	TxHash string
	State  string
}

// errIntroSimulatedCrash is returned at a test fault point.
var errIntroSimulatedCrash = errors.New("introduced link: simulated crash (test fault)")

func (m *Manager) introFault(point string) bool {
	return m.introFaultFn != nil && m.introFaultFn(point)
}

// introAbsentTerms is the prior_terms_digest when the chain has no record for
// the remote.
const introAbsentTerms = "absent"

// IntroTermsDigest is the hex sha256 of the canonical tx-33 encoding of the
// chain's current terms for remoteChainID, or "absent".
func (m *Manager) IntroTermsDigest(remoteChainID string) (string, error) {
	rec, err := m.chainTerms(remoteChainID)
	if err != nil {
		return "", err
	}
	if rec == nil {
		return introAbsentTerms, nil
	}
	sum := sha256.Sum256(tx.EncodeCrossFedTerms(rec))
	return hex.EncodeToString(sum[:]), nil
}

// chainTerms reads the raw consensus record for a remote as tx-33 terms,
// with none of ActiveAgreement's gates. It returns nil when there is none.
func (m *Manager) chainTerms(remoteChainID string) (*tx.CrossFedTerms, error) {
	if m.badger == nil {
		return nil, errors.New("consensus state store is unavailable")
	}
	found, err := m.badger.HasCrossFed(remoteChainID)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	endpoint, pin, clearance, expires, domains, depts, status, err := m.badger.GetCrossFed(remoteChainID)
	if err != nil {
		return nil, fmt.Errorf("read chain terms for %s: %w", remoteChainID, err)
	}
	return &tx.CrossFedTerms{
		RemoteChainID: remoteChainID, Endpoint: endpoint, PeerPubKey: pin,
		MaxClearance: tx.ClearanceLevel(clearance), AllowedDomains: domains, AllowedDepts: depts,
		ExpiresAt: expires, Status: status,
	}, nil
}

func termsRecord(t *tx.CrossFedTerms) *store.CrossFedRecord {
	return &store.CrossFedRecord{
		RemoteChainID: t.RemoteChainID, Endpoint: t.Endpoint, PeerPubKey: t.PeerPubKey,
		MaxClearance: uint8(t.MaxClearance), ExpiresAt: t.ExpiresAt,
		AllowedDomains: t.AllowedDomains, AllowedDepts: t.AllowedDepts, Status: t.Status,
	}
}

// introPlan is an IntroActivation checked against its own transcript.
type introPlan struct {
	act      *IntroActivation
	tr       *IntroTranscript
	e        [32]byte
	linkID   string
	local    IntroSide
	remote   IntroSide
	remPin   []byte
	epoch    string
	pinPair  []byte
	encTerms []byte
}

func (m *Manager) planIntroActivation(act *IntroActivation) (*introPlan, error) {
	if act == nil || act.Terms == nil {
		return nil, errors.New("introduced link: activation and terms are required")
	}
	tr, err := DecodeIntroTranscript(act.Transcript)
	if err != nil {
		return nil, err
	}
	if tr.Kind != introKindNew {
		// Rekey replaces an active sync-control binding and lands with the
		// wire in B2b.
		return nil, errors.New("introduced link: only kind new is activated in this phase")
	}
	e := IntroAttestation(act.Transcript)
	if !VerifyIntro(tr.Host.AgentPub, IntroHostAccept, e, act.HostAcceptSig) ||
		!VerifyIntro(tr.Guest.AgentPub, IntroGuestAccept, e, act.GuestAcceptSig) ||
		!VerifyIntro(tr.Guest.AgentPub, IntroGuestAck, e, act.GuestAckSig) {
		return nil, errors.New("introduced link: acceptance signatures do not verify over E_intro")
	}
	p := &introPlan{act: act, tr: tr, e: e, linkID: hex.EncodeToString(e[:])}
	switch act.Role {
	case "host":
		p.local, p.remote = tr.Host, tr.Guest
	case "guest":
		p.local, p.remote = tr.Guest, tr.Host
	default:
		return nil, errors.New("introduced link: role must be host or guest")
	}
	if p.local.ChainID != m.localChainID {
		return nil, errors.New("introduced link: transcript side does not name this chain")
	}
	if p.local.AgentPub != hex.EncodeToString(m.agentPub) {
		return nil, errors.New("introduced link: transcript side does not name this node's agent key")
	}
	ownPin, err := m.ownPin()
	if err != nil {
		return nil, err
	}
	if p.local.Pin != hex.EncodeToString(ownPin) {
		return nil, errors.New("introduced link: transcript pin is not this node's CA")
	}
	if p.remPin, err = hex.DecodeString(p.remote.Pin); err != nil {
		return nil, errors.New("introduced link: bad remote pin")
	}
	t := act.Terms
	scope := ScopeDigest(uint8(t.MaxClearance), t.AllowedDomains, "exchange", "both")
	switch {
	case t.RemoteChainID != p.remote.ChainID:
		return nil, errors.New("introduced link: terms name a different remote chain")
	case t.Endpoint != p.remote.Endpoint:
		return nil, errors.New("introduced link: terms endpoint is not the transcript endpoint")
	case !bytes.Equal(t.PeerPubKey, p.remPin):
		return nil, errors.New("introduced link: terms pin is not the transcript pin")
	case t.Status != "active":
		return nil, errors.New("introduced link: terms must be active")
	case len(t.AllowedDomains) != 0 || len(t.AllowedDepts) != 0:
		// Introduced JOIN stays trust-only; data scope comes only through the
		// envelope-capped UpdateAgreementSharingAs.
		return nil, errors.New("introduced link: terms must be trust-only")
	case hex.EncodeToString(scope[:]) != p.local.Scope:
		return nil, errors.New("introduced link: terms do not match this side's scope in E_intro")
	case SeedCommitHex(act.Seed) != tr.SeedCommit:
		return nil, errors.New("introduced link: seed does not match the transcript commitment")
	case act.StaleCheck == nil:
		return nil, errors.New("introduced link: no stale-intent gate")
	case act.Deadline <= time.Now().Unix():
		return nil, errors.New("introduced link: deadline already passed")
	}
	pp := pinPair(m.localChainID, ownPin, p.remote.ChainID, p.remPin)
	p.pinPair = pp[:]
	p.epoch = syncPolicyEpoch(e)
	p.encTerms = tx.EncodeCrossFedTerms(t)
	return p, nil
}

// ActivateIntroducedLink runs the §3.3 write path for one accepted session.
// On success the row is active (host) or bound_pending_peer (guest). An
// *IntroStaleError means nothing happened; ErrIntroPending means the row is
// signed and the reconciler owns it; any other error after the intent was
// written leaves the row aborted or revoked with the reason recorded.
func (m *Manager) ActivateIntroducedLink(ctx context.Context, act *IntroActivation) (*IntroActivationResult, error) {
	if act != nil {
		defer zeroize(act.Seed)
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("introduced link: requires the SQLite store backend")
	}
	if m.introducedLinkSource() == nil {
		// Without the source ActiveAgreement cannot see the row and would not
		// deny a committed-but-unbound agreement. Fail closed.
		return nil, errors.New("introduced link: no introduced-link source is installed")
	}
	p, err := m.planIntroActivation(act)
	if err != nil {
		return nil, err
	}
	remote := p.remote.ChainID
	if len(p.remote.P2PAddrs) > 0 {
		if h := m.joinP2PHooks(); h.Persist == nil && h.PersistSnapshot == nil {
			return nil, errors.New("introduced link: internet peer route persistence is unavailable")
		}
	}

	unlock := m.LockAgreementMutation()
	defer unlock()
	generation := m.BeginSyncPolicyGenerationMutation(remote)
	defer generation.Restore()

	// 1. Stale-intent gate.
	if staleErr := act.StaleCheck(ctx); staleErr != nil {
		return nil, &IntroStaleError{Which: staleErr.Error()}
	}
	prior, err := m.IntroTermsDigest(remote)
	if err != nil {
		return nil, err
	}
	if prior != act.PriorTermsDigest {
		return nil, &IntroStaleError{Which: "chain terms changed"}
	}
	if latest, lErr := ss.LatestIntroducedLinkByChain(ctx, remote); lErr != nil {
		return nil, fmt.Errorf("introduced link: read in-flight row: %w", lErr)
	} else if latest != nil && store.IntroducedLinkInFlight(latest.State) {
		return nil, &IntroStaleError{Which: "another link for this chain is in flight"}
	}

	// 2. Stage the CA and seed under the link's directory.
	now := time.Now().Unix()
	ref, caPin, err := m.stageIntroArtifacts(remote, p.linkID, act.RemoteCAPEM, act.Seed, p.pinPair, now)
	if err != nil {
		return nil, fmt.Errorf("introduced link: stage artifacts: %w", err)
	}
	if !bytes.Equal(caPin, p.remPin) {
		_ = m.rollbackIntroArtifacts(remote, p.linkID, ref)
		return nil, errors.New("introduced link: peer CA does not match the transcript pin")
	}

	// 3. Durable intent.
	row := &store.IntroducedLink{
		LinkID: p.linkID, OrgID: act.OrgID, RemoteChainID: remote, PeerAgentPub: p.remote.AgentPub,
		OfferID: p.tr.OfferID, SessionID: act.SessionID, Kind: p.tr.Kind, Role: act.Role,
		EIntro: act.Transcript, HostAcceptSig: act.HostAcceptSig, GuestAccept: act.GuestAcceptSig,
		GuestAckSig: act.GuestAckSig, Evidence: act.Evidence, Terms: p.encTerms,
		PriorTermsDigest: act.PriorTermsDigest, StagedRef: ref, CreatedAt: now, Deadline: act.Deadline,
	}
	if insErr := ss.InsertIntroducedLinkIntent(ctx, row); insErr != nil {
		_ = m.rollbackIntroArtifacts(remote, p.linkID, ref)
		if errors.Is(insErr, store.ErrIntroducedLinkInFlight) {
			return nil, &IntroStaleError{Which: "another link for this chain is in flight"}
		}
		return nil, fmt.Errorf("introduced link: write intent: %w", insErr)
	}
	if prepErr := ss.PrepareSyncControl(ctx, m.introSyncControl(p)); prepErr != nil {
		return nil, m.abortIntroLocked(ctx, row, "prepare_failed", fmt.Errorf("prepare sync control: %w", prepErr))
	}
	if m.introFault("after_intent") {
		return nil, errIntroSimulatedCrash
	}

	// 4. Sign inside the nonce lease; CAS intent→signed before the bytes leave.
	signed, crashed := false, false
	var txHash string
	txHash, err = m.broadcastIntroSet(act.ControlActorID, act.Terms, func(encoded []byte, nonce uint64) error {
		ok, casErr := ss.TransitionIntroducedLink(ctx, store.IntroducedLinkTransition{
			LinkID: p.linkID, From: store.IntroducedLinkIntent, To: store.IntroducedLinkSigned,
			At: time.Now().Unix(), TxBytes: encoded, TxNonce: nonce,
		})
		if casErr != nil {
			return fmt.Errorf("record signed tx: %w", casErr)
		}
		if !ok {
			return errors.New("record signed tx: row moved on")
		}
		signed = true
		if m.introFault("after_signed") {
			crashed = true
			return errIntroSimulatedCrash
		}
		return nil
	})
	if crashed {
		return nil, errIntroSimulatedCrash
	}
	if !signed {
		// Nothing left the process.
		return nil, m.abortIntroLocked(ctx, row, "not_signed", err)
	}
	cur, gErr := ss.GetIntroducedLink(ctx, p.linkID)
	if gErr != nil {
		return nil, fmt.Errorf("introduced link: reread signed row: %w", gErr)
	}

	// 5. Broadcast result.
	switch {
	case err == nil:
	case errors.Is(err, tx.ErrSubmitIndeterminate):
		m.logger.Warn().Err(err).Str("remote", remote).Str("link", p.linkID).Msg("introduced link tx-33 indeterminate; reconciler owns it")
		return &IntroActivationResult{LinkID: p.linkID, State: store.IntroducedLinkSigned}, ErrIntroPending
	default:
		return nil, m.abortSignedIntroLocked(ctx, cur, "rejected", err)
	}
	if casErr := m.casIntro(ctx, cur, store.IntroducedLinkCommitted, ""); casErr != nil {
		return nil, casErr
	}
	cur.State = store.IntroducedLinkCommitted
	if m.introFault("after_commit") {
		return nil, errIntroSimulatedCrash
	}

	// 6. Bind.
	state, err := m.bindIntroLocked(ctx, cur, p.remote.PeerID, p.remote.P2PAddrs, act.ControlActorID)
	if err != nil {
		return nil, err
	}
	generation.Activate()
	m.NudgeJournalReconcile()
	m.logger.Info().Str("remote", remote).Str("link", p.linkID).Str("state", state).Str("tx", txHash).Msg("introduced link bound")
	return &IntroActivationResult{LinkID: p.linkID, TxHash: txHash, State: state}, nil
}

// broadcastIntroSet signs and broadcasts tx-33 with the sync-policy write
// lease held around the broadcast only (bind re-takes it internally).
func (m *Manager) broadcastIntroSet(actor string, terms *tx.CrossFedTerms, hook func([]byte, uint64) error) (string, error) {
	if ss := m.syncStore(); ss != nil {
		policyUnlock := ss.LockSyncPolicyWrite()
		defer policyUnlock()
	}
	return m.broadcastCrossFedSetHookedLockedAs(actor, terms, hook)
}

func (m *Manager) introSyncControl(p *introPlan) store.SyncControl {
	c := store.SyncControl{
		RemoteChainID: p.remote.ChainID, Role: p.act.Role,
		PeerAgentID: p.remote.AgentPub, PolicyEpoch: p.epoch, RemoteCAPin: p.remote.Pin,
	}
	if p.act.Role == "host" {
		c.ControllerChainID, c.ControllerAgentID = m.localChainID, p.local.AgentPub
	} else {
		c.ControllerChainID, c.ControllerAgentID = p.remote.ChainID, p.remote.AgentPub
	}
	return c
}

// casIntro moves a row out of a signed-or-later state on its exact tx hash.
func (m *Manager) casIntro(ctx context.Context, row *store.IntroducedLink, to, reason string) error {
	ss := m.syncStore()
	ok, err := ss.TransitionIntroducedLink(ctx, store.IntroducedLinkTransition{
		LinkID: row.LinkID, From: row.State, To: to, Reason: reason, At: time.Now().Unix(), ExpectTxHash: row.TxHash,
	})
	if err != nil {
		return fmt.Errorf("introduced link %s → %s: %w", row.State, to, err)
	}
	if !ok {
		return fmt.Errorf("introduced link %s → %s: row moved on", row.State, to)
	}
	row.State = to
	return nil
}

// dropPendingSyncControl removes a sync_control row this link prepared and
// that never activated.
func (m *Manager) dropPendingSyncControl(ctx context.Context, remote, epoch string) {
	ss := m.syncStore()
	c, err := ss.GetSyncControl(ctx, remote)
	if err != nil || c == nil || c.BindingState != "pending" || c.PolicyEpoch != epoch {
		return
	}
	if dErr := ss.DeleteSyncControl(ctx, remote); dErr != nil {
		m.logger.Warn().Err(dErr).Str("remote", remote).Msg("introduced link: drop pending sync control failed")
	}
}

func introEpochOf(row *store.IntroducedLink) string {
	return syncPolicyEpoch(IntroAttestation(row.EIntro))
}

// abortIntroLocked unwinds a row still in intent: no bytes were signed, so
// nothing can be on chain.
func (m *Manager) abortIntroLocked(ctx context.Context, row *store.IntroducedLink, reason string, cause error) error {
	ss := m.syncStore()
	_ = m.rollbackIntroArtifacts(row.RemoteChainID, row.LinkID, row.StagedRef)
	m.dropPendingSyncControl(ctx, row.RemoteChainID, introEpochOf(row))
	full := reason
	if cause != nil {
		full = reason + ": " + truncateReason(cause.Error())
	}
	if _, err := ss.TransitionIntroducedLink(ctx, store.IntroducedLinkTransition{
		LinkID: row.LinkID, From: store.IntroducedLinkIntent, To: store.IntroducedLinkAborted,
		Reason: full, At: time.Now().Unix(),
	}); err != nil {
		m.logger.Error().Err(err).Str("link", row.LinkID).Msg("introduced link: record abort failed")
	}
	if cause == nil {
		return fmt.Errorf("introduced link aborted: %s", reason)
	}
	return fmt.Errorf("introduced link aborted (%s): %w", reason, cause)
}

// abortSignedIntroLocked unwinds a signed row whose bytes can never commit.
func (m *Manager) abortSignedIntroLocked(ctx context.Context, row *store.IntroducedLink, reason string, cause error) error {
	_ = m.rollbackIntroArtifacts(row.RemoteChainID, row.LinkID, row.StagedRef)
	m.dropPendingSyncControl(ctx, row.RemoteChainID, introEpochOf(row))
	full := reason
	if cause != nil {
		full = reason + ": " + truncateReason(cause.Error())
	}
	if err := m.casIntro(ctx, row, store.IntroducedLinkAborted, full); err != nil {
		m.logger.Error().Err(err).Str("link", row.LinkID).Msg("introduced link: record abort failed")
	}
	if cause == nil {
		return fmt.Errorf("introduced link aborted: %s", reason)
	}
	return fmt.Errorf("introduced link aborted (%s): %w", reason, cause)
}

func truncateReason(s string) string {
	const max = 400
	if len(s) > max {
		return s[:max]
	}
	return s
}

// bindIntroLocked binds a committed row (§3.3 step 6): commit the staged CA
// and seed, activate sync control, persist the peer route, install the
// deny-all peer RBAC snapshot, then host → active, guest →
// bound_pending_peer. Every step is idempotent so the reconciler can resume.
// A failure revokes the agreement (tx-34) and leaves revoked(bind_failed).
func (m *Manager) bindIntroLocked(ctx context.Context, row *store.IntroducedLink, peerID string, routes []string, actor string) (string, error) {
	ss := m.syncStore()
	fail := func(what string, cause error) (string, error) {
		if _, rErr := m.revokeAgreementLockedAs(actor, row.RemoteChainID); rErr != nil {
			m.logger.Error().Err(rErr).Str("remote", row.RemoteChainID).Msg("introduced link: revoke after bind failure failed")
		}
		_ = m.rollbackIntroArtifacts(row.RemoteChainID, row.LinkID, row.StagedRef)
		if err := m.casIntro(ctx, row, store.IntroducedLinkRevoked, "bind_failed: "+what); err != nil {
			m.logger.Error().Err(err).Str("link", row.LinkID).Msg("introduced link: record bind failure failed")
		}
		return "", fmt.Errorf("introduced link bind failed (%s); agreement revoked: %w", what, cause)
	}
	terms, err := m.chainTerms(row.RemoteChainID)
	if err != nil {
		return "", err
	}
	if terms == nil || !bytes.Equal(tx.EncodeCrossFedTerms(terms), row.Terms) {
		return fail("chain terms", errors.New("chain does not hold the committed terms"))
	}
	rec := termsRecord(terms)
	ownPin, err := m.ownPin()
	if err != nil {
		return fail("own pin", err)
	}
	pp := pinPair(m.localChainID, ownPin, row.RemoteChainID, rec.PeerPubKey)
	if err := m.commitIntroArtifacts(row.RemoteChainID, row.LinkID, row.StagedRef, rec.PeerPubKey, pp[:]); err != nil {
		return fail("staged artifacts", err)
	}
	epoch := introEpochOf(row)
	if err := ss.ActivateSyncControl(ctx, row.RemoteChainID, epoch); err != nil {
		return fail("sync control", err)
	}
	if len(routes) > 0 {
		if err := m.persistBootstrapRoute(row.RemoteChainID, peerID, epoch, routes); err != nil {
			return fail("peer route", err)
		}
	}
	if err := m.initializePeerRBACPolicyFor(ctx, rec); err != nil {
		return fail("peer RBAC", err)
	}
	to := store.IntroducedLinkActive
	if row.Role == "guest" {
		to = store.IntroducedLinkBoundPendingPeer
	}
	if err := m.casIntro(ctx, row, to, ""); err != nil {
		return "", err
	}
	return to, nil
}
