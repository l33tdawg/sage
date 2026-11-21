package federation

// Introduced-link policy (RFC #443 Phase B2c, contract v2 §4 and §5).
//
// B1 only reports what the org state means for an active link; this applies
// it, on every maintenance tick and after any org admission (policy, roster,
// envelope, card). For the governing row of each introduced agreement
// (store.IntroducedLinkForGeneration of the chain's current terms) it takes
// LockAgreementMutation and one sync-policy generation mutation, re-reads the
// row, and moves it on:
//
//	org not ready (policy, roster or envelope no longer current or covering),
//	peer card removed, peer card names another chain, or pause_required
//	                                → paused(reason)
//	active, a CA pin moved          → rekey_required (the rekey offer drives it)
//	active, endpoint/route-only card change (same pin, agent and chain)
//	                                → tx-33 upsert for the new endpoint (the
//	                                  generation leaves the endpoint out, so
//	                                  the row keeps governing), a route
//	                                  refresh, and a history entry
//	paused, resumable and the card digest is the one the link last followed
//	                                → active
//
// Anything else needs a new ask. Pausing needs no tx: ActiveAgreement denies
// a generation whose governing row is not active, so traffic stops at once.

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// introPolicyGuard keeps one background policy pass at a time per Manager.
type introPolicyGuard struct{ running atomic.Bool }

// NudgeIntroducedLinkPolicy runs one policy pass in the background, unless
// one is already running. The REST layer calls it after an org admission.
func (m *Manager) NudgeIntroducedLinkPolicy() {
	if !m.introPolicy.running.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.introPolicy.running.Store(false)
		m.applyIntroducedLinkPolicyLogged(context.Background())
	}()
}

func (m *Manager) applyIntroducedLinkPolicyLogged(ctx context.Context) {
	if err := m.ApplyIntroducedLinkPolicy(ctx); err != nil {
		m.logger.Debug().Err(err).Msg("introduced link policy pass left rows unchanged")
	}
}

// ApplyIntroducedLinkPolicy runs one pass over every introduced agreement.
// A row it cannot settle stays as it is; the first error is returned after
// every row was tried. With no org source, no node-local store or no
// introduced-link source it does nothing.
func (m *Manager) ApplyIntroducedLinkPolicy(ctx context.Context) error {
	ss, src := m.syncStore(), m.introOrgSource()
	if ss == nil || src == nil || m.introducedLinkSource() == nil {
		return nil
	}
	rows, err := ss.ListIntroducedLinks(ctx, "")
	if err != nil {
		return fmt.Errorf("list introduced links: %w", err)
	}
	type target struct{ linkID, chain, org string }
	var targets []target
	seen := map[string]bool{}
	for i := range rows {
		chain := rows[i].RemoteChainID
		if seen[chain] {
			continue
		}
		seen[chain] = true
		gov, gErr := m.governingIntroRow(ctx, chain)
		if gErr != nil {
			return gErr
		}
		if gov != nil && introPolicyState(gov.State) {
			targets = append(targets, target{gov.LinkID, chain, gov.OrgID})
		}
	}
	peers := map[string]*IntroOrgPeers{}
	var first error
	keep := func(err error) {
		if err != nil && first == nil {
			first = err
		}
	}
	for _, t := range targets {
		op, ok := peers[t.org]
		if !ok {
			var pErr error
			op, pErr = src.IntroOrgPeers(ctx, t.org)
			if pErr != nil {
				// Org state unreadable: leave the org's rows as they are.
				keep(fmt.Errorf("org %s peers: %w", t.org, pErr))
				op = nil
			}
			peers[t.org] = op
		}
		if op == nil {
			continue
		}
		if aErr := m.applyIntroPolicyRow(ctx, t.linkID, t.chain, op); aErr != nil {
			m.logger.Warn().Err(aErr).Str("link", t.linkID).Str("remote", t.chain).Msg("introduced link policy")
			keep(aErr)
		}
	}
	return first
}

// introPolicyState reports whether the policy pass manages a row in state.
func introPolicyState(state string) bool {
	switch state {
	case store.IntroducedLinkActive, store.IntroducedLinkRekeyRequired, store.IntroducedLinkPaused:
		return true
	}
	return false
}

// governingIntroRow returns the row that governs the chain's current active
// agreement, or nil when there is none (no agreement, or a manual one).
func (m *Manager) governingIntroRow(ctx context.Context, chain string) (*store.IntroducedLink, error) {
	cur, err := m.chainTerms(chain)
	if err != nil {
		return nil, err
	}
	if cur == nil || cur.Status != "active" {
		return nil, nil
	}
	return m.syncStore().IntroducedLinkForGeneration(ctx, chain, store.CrossFedGeneration(*termsRecord(cur)))
}

func (m *Manager) applyIntroPolicyRow(ctx context.Context, linkID, chain string, op *IntroOrgPeers) error {
	unlock := m.LockAgreementMutation()
	defer unlock()
	generation := m.BeginSyncPolicyGenerationMutation(chain)
	defer generation.Restore()

	row, err := m.governingIntroRow(ctx, chain)
	if err != nil {
		return err
	}
	if row == nil || row.LinkID != linkID || !introPolicyState(row.State) {
		return nil // moved on since the pass listed it
	}
	tr, err := DecodeIntroTranscript(row.EIntro)
	if err != nil {
		return fmt.Errorf("decode stored E_intro: %w", err)
	}
	remote := tr.Guest
	if row.Role == "guest" {
		remote = tr.Host
	}
	var ps *IntroPeerState
	for i := range op.Peers {
		if op.Peers[i].Card.AgentPub == row.PeerAgentPub {
			ps = &op.Peers[i]
			break
		}
	}
	pause := func(why string) error {
		if row.State == store.IntroducedLinkPaused {
			return nil
		}
		return m.casIntro(ctx, row, store.IntroducedLinkPaused, truncateReason(why))
	}
	switch {
	case !op.Ready:
		return pause("org not ready: " + op.Reason)
	case ps == nil:
		return pause("the peer card is no longer in the accepted roster")
	case ps.Card.ChainID != row.RemoteChainID:
		return pause("the peer card now names chain " + ps.Card.ChainID + "; a chain id change is refused")
	case ps.State == "pause_required":
		return pause(ps.Reason)
	}
	switch row.State {
	case store.IntroducedLinkPaused:
		if ps.State != "paused" || !ps.Resumable {
			return nil
		}
		_, followed, err := m.introFollowedCard(ctx, row, remote)
		if err != nil {
			return err
		}
		if ps.Card.CardDigest != followed {
			// The card changed while paused: that needs a new ask.
			return nil
		}
		return m.casIntro(ctx, row, store.IntroducedLinkActive, "resumed: covered again with the same card")
	case store.IntroducedLinkActive:
		switch ps.State {
		case "rekey_required":
			return m.casIntro(ctx, row, store.IntroducedLinkRekeyRequired, "a CA pin changed; rekey offer "+shortID(ps.OfferID))
		case "active":
			return m.introFollowCardLocked(ctx, row, remote, ps.Card)
		}
	}
	return nil
}

// introFollowedCard is the peer card revision and digest the link last
// followed: the one recorded by a rotation, else the one in E_intro.
func (m *Manager) introFollowedCard(ctx context.Context, row *store.IntroducedLink, remote IntroSide) (uint64, string, error) {
	c, err := m.syncStore().GetIntroducedLinkCard(ctx, row.LinkID)
	if err != nil {
		return 0, "", err
	}
	if c != nil {
		return c.CardRev, c.CardDigest, nil
	}
	return remote.CardRev, remote.CardDigest, nil
}

// introFollowCardLocked applies an endpoint- or route-only change of the
// peer card to an active link (contract v2 §4 rotation split). The pin, agent
// key and chain id are unchanged here: a pin change was routed to
// rekey_required and a chain id change to paused above.
func (m *Manager) introFollowCardLocked(ctx context.Context, row *store.IntroducedLink, remote IntroSide, card IntroCard) error {
	rev, followed, err := m.introFollowedCard(ctx, row, remote)
	if err != nil {
		return err
	}
	if card.CardDigest == followed || card.CardRev < rev {
		return nil
	}
	if card.Pin != remote.Pin {
		return nil // never reached: offers report rekey_required
	}
	cur, err := m.chainTerms(row.RemoteChainID)
	if err != nil {
		return err
	}
	if cur == nil {
		return errors.New("introduced link: agreement vanished")
	}
	var changed []string
	if card.Endpoint != cur.Endpoint {
		next := *cur
		next.Endpoint = card.Endpoint
		next.AllowedDomains = append([]string(nil), cur.AllowedDomains...)
		next.AllowedDepts = append([]string(nil), cur.AllowedDepts...)
		if _, bErr := m.broadcastIntroSet("", &next, nil); bErr != nil {
			if errors.Is(bErr, tx.ErrSubmitIndeterminate) {
				// The next pass sees whether it landed (endpoint equal) and
				// records the card then.
				return fmt.Errorf("endpoint upsert pending: %w", bErr)
			}
			return fmt.Errorf("endpoint upsert: %w", bErr)
		}
		changed = append(changed, "endpoint")
	}
	if !sameStrings(card.Routes, remote.P2PAddrs) || card.PeerID != remote.PeerID {
		changed = append(changed, "routes")
	}
	if len(changed) == 0 {
		changed = append(changed, "card")
	}
	// Detached: the refresh outlives a request-scoped ctx, and it must not
	// take the sync-policy read lease under this lock.
	go m.triggerRouteRefreshWithContext(context.WithoutCancel(ctx), row.RemoteChainID, nil)
	reason := "card_rotated rev " + strconv.FormatUint(card.CardRev, 10) + ": " + strings.Join(changed, ", ")
	ok, err := m.syncStore().RecordIntroducedLinkCard(ctx, row.LinkID, store.IntroducedLinkActive, card.CardRev, card.CardDigest, reason, time.Now().Unix())
	if err != nil {
		return err
	}
	if !ok {
		return errors.New("introduced link: row moved on while following the card")
	}
	m.logger.Info().Str("link", row.LinkID).Str("remote", row.RemoteChainID).Str("change", reason).Msg("introduced link followed the peer card")
	return nil
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// introRekeyGate is the extra stale gate of a rekey, under the lease: the
// chain must hold an active agreement governed by an active or rekey_required
// row of the same org and peer, and the terms must be exactly that
// agreement's terms with only the pin and endpoint moved.
func (m *Manager) introRekeyGate(ctx context.Context, act *IntroActivation, p *introPlan) error {
	cur, err := m.chainTerms(p.remote.ChainID)
	if err != nil {
		return err
	}
	if cur == nil || cur.Status != "active" {
		return errors.New("no active agreement to rekey")
	}
	want := tx.EncodeCrossFedTerms(rekeyTerms(cur, p.remote.Endpoint, p.remPin))
	if string(want) != string(p.encTerms) {
		return errors.New("rekey terms do not carry over the current agreement")
	}
	gov, err := m.syncStore().IntroducedLinkForGeneration(ctx, p.remote.ChainID, store.CrossFedGeneration(*termsRecord(cur)))
	if err != nil {
		return err
	}
	switch {
	case gov == nil:
		return errors.New("the agreement is not an introduced link")
	case gov.OrgID != act.OrgID:
		return errors.New("the agreement was introduced by another org")
	case gov.PeerAgentPub != p.remote.AgentPub:
		return errors.New("the agreement belongs to another peer key")
	case gov.State != store.IntroducedLinkActive && gov.State != store.IntroducedLinkRekeyRequired:
		return fmt.Errorf("the introduced link is %s, not active or rekey_required", gov.State)
	}
	return nil
}

// closeSupersededIntroRows retires the rows a bound rekey replaced. They no
// longer govern anything (another generation, or an older row of the same
// one); revoking them keeps the record honest. Failures are logged only.
func (m *Manager) closeSupersededIntroRows(ctx context.Context, row *store.IntroducedLink) {
	ss := m.syncStore()
	rows, err := ss.ListIntroducedLinks(ctx, row.RemoteChainID)
	if err != nil {
		m.logger.Warn().Err(err).Str("link", row.LinkID).Msg("introduced link: list superseded rows failed")
		return
	}
	for i := range rows {
		old := rows[i]
		if old.LinkID == row.LinkID || !introPolicyState(old.State) {
			continue
		}
		if cErr := m.casIntro(ctx, &old, store.IntroducedLinkRevoked, "superseded_by_rekey: "+row.LinkID); cErr != nil {
			m.logger.Warn().Err(cErr).Str("link", old.LinkID).Msg("introduced link: retire superseded row failed")
		}
	}
}

// recordIntroRefusal logs a stale-gate refusal. Failures are logged only:
// the refusal itself already happened.
func (m *Manager) recordIntroRefusal(ctx context.Context, act *IntroActivation, p *introPlan, why string) {
	ss := m.syncStore()
	if ss == nil || p == nil {
		return
	}
	if err := ss.InsertIntroducedLinkRefusal(ctx, store.IntroducedLinkRefusal{
		At: time.Now().Unix(), OrgID: act.OrgID, OfferID: p.tr.OfferID, RemoteChainID: p.remote.ChainID,
		Kind: p.tr.Kind, Role: act.Role, Reason: why,
	}); err != nil {
		m.logger.Warn().Err(err).Str("remote", p.remote.ChainID).Msg("introduced link: record stale refusal failed")
	}
}
