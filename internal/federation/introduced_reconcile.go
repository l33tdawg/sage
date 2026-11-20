package federation

// Introduced-link reconciler (RFC #443 Phase B2a, design §3.4).
//
// Runs once at start-up and then on the maintenance tick. For each in-flight
// row it takes LockAgreementMutation and one sync-policy generation mutation,
// re-reads the row, and moves it on. It never broadcasts anything except the
// exact stored tx-33 bytes (through the resolver, which re-submits the
// identical bytes) and, to undo a bind or a one-sided guest, a tx-34.
//
//	intent              → aborted(crash_before_broadcast); nothing was signed
//	signed              → resolve the stored bytes:
//	                        committed + chain holds the row's terms → committed → bind
//	                        committed + chain holds other terms    → aborted(superseded_on_chain)
//	                        rejected / superseded / abandoned      → aborted(<verdict>)
//	                        spent + chain holds the row's terms    → paused(reconcile_spent)
//	                        spent otherwise                        → aborted(spent)
//	                        unresolved / error                     → stays signed
//	committed           → resume bind (idempotent)
//	bound_pending_peer  → active once the host reports active; after the
//	                      deadline → tx-34 → revoked(peer_not_confirmed)

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func (m *Manager) introResolver() tx.TxResolveFunc {
	if m.introResolveFn != nil {
		return m.introResolveFn
	}
	return tx.CometTxResolver(m.cometRPC)
}

// ReconcileIntroducedLinks runs one reconciler pass over every in-flight
// introduced-link row. A row it cannot settle stays where it is (and
// ActiveAgreement keeps denying it); the first error is returned after every
// row was tried.
func (m *Manager) ReconcileIntroducedLinks(ctx context.Context) error {
	ss := m.syncStore()
	if ss == nil {
		return nil
	}
	rows, err := ss.ListIntroducedLinks(ctx, "")
	if err != nil {
		return fmt.Errorf("list introduced links: %w", err)
	}
	var first error
	for i := range rows {
		if !store.IntroducedLinkInFlight(rows[i].State) {
			continue
		}
		if err := m.reconcileIntroRow(ctx, rows[i].LinkID, rows[i].RemoteChainID); err != nil {
			m.logger.Warn().Err(err).Str("link", rows[i].LinkID).Str("remote", rows[i].RemoteChainID).Msg("introduced link reconcile")
			if first == nil {
				first = err
			}
		}
	}
	return first
}

func (m *Manager) reconcileIntroducedLinksLogged(ctx context.Context) {
	if err := m.ReconcileIntroducedLinks(ctx); err != nil {
		m.logger.Debug().Err(err).Msg("introduced link reconcile pass left rows in flight")
	}
}

func (m *Manager) reconcileIntroRow(ctx context.Context, linkID, remote string) error {
	unlock := m.LockAgreementMutation()
	defer unlock()
	generation := m.BeginSyncPolicyGenerationMutation(remote)
	defer generation.Restore()

	ss := m.syncStore()
	row, err := ss.GetIntroducedLink(ctx, linkID)
	if err != nil {
		return fmt.Errorf("reread introduced link: %w", err)
	}
	switch row.State {
	case store.IntroducedLinkIntent:
		// The bytes never existed, so they cannot have been broadcast.
		err := m.abortIntroLocked(ctx, row, "crash_before_broadcast", nil)
		m.logger.Info().Str("link", linkID).Str("remote", remote).Msg(err.Error())
		return nil
	case store.IntroducedLinkSigned:
		return m.reconcileSignedLocked(ctx, row, generation)
	case store.IntroducedLinkCommitted:
		return m.resumeBindLocked(ctx, row, generation)
	case store.IntroducedLinkBoundPendingPeer:
		return m.reconcileGuestWindowLocked(ctx, row)
	}
	return nil
}

func (m *Manager) reconcileSignedLocked(ctx context.Context, row *store.IntroducedLink, generation *SyncPolicyGenerationMutation) error {
	resolve := m.introResolver()
	if resolve == nil {
		return errors.New("no tx resolver: the signed row stays signed")
	}
	out, err := resolve(ctx, row.TxBytes)
	if err != nil {
		return fmt.Errorf("resolve signed tx-33: %w", err)
	}
	switch out.Verdict {
	case tx.TxVerdictCommitted:
		match, mErr := m.chainHoldsTerms(row)
		if mErr != nil {
			return mErr
		}
		if !match {
			// A later upsert replaced these terms; leave the chain alone.
			return m.reconcileAbortSigned(ctx, row, "superseded_on_chain")
		}
		if err := m.casIntro(ctx, row, store.IntroducedLinkCommitted, ""); err != nil {
			return err
		}
		return m.resumeBindLocked(ctx, row, generation)
	case tx.TxVerdictRejected:
		return m.reconcileAbortSigned(ctx, row, "rejected")
	case tx.TxVerdictSuperseded:
		return m.reconcileAbortSigned(ctx, row, "superseded")
	case tx.TxVerdictAbandoned:
		return m.reconcileAbortSigned(ctx, row, "abandoned")
	case tx.TxVerdictSpent:
		match, mErr := m.chainHoldsTerms(row)
		if mErr != nil {
			return mErr
		}
		if !match {
			return m.reconcileAbortSigned(ctx, row, "spent")
		}
		// The nonce floor cannot tell whether these bytes landed. Never
		// auto-activate: the owner decides (revoke or accept).
		return m.casIntro(ctx, row, store.IntroducedLinkPaused, "reconcile_spent")
	}
	return fmt.Errorf("signed tx-33 unresolved: %s", out.Detail)
}

func (m *Manager) reconcileAbortSigned(ctx context.Context, row *store.IntroducedLink, reason string) error {
	err := m.abortSignedIntroLocked(ctx, row, reason, nil)
	m.logger.Info().Str("link", row.LinkID).Str("remote", row.RemoteChainID).Msg(err.Error())
	return nil
}

// chainHoldsTerms reports whether the chain's current record for the remote
// is exactly the row's encoded terms.
func (m *Manager) chainHoldsTerms(row *store.IntroducedLink) (bool, error) {
	terms, err := m.chainTerms(row.RemoteChainID)
	if err != nil {
		return false, err
	}
	return terms != nil && bytes.Equal(tx.EncodeCrossFedTerms(terms), row.Terms), nil
}

func (m *Manager) resumeBindLocked(ctx context.Context, row *store.IntroducedLink, generation *SyncPolicyGenerationMutation) error {
	tr, err := DecodeIntroTranscript(row.EIntro)
	if err != nil {
		return fmt.Errorf("decode stored E_intro: %w", err)
	}
	remote := tr.Guest
	if row.Role == "guest" {
		remote = tr.Host
	}
	state, err := m.bindIntroLocked(ctx, row, remote.PeerID, remote.P2PAddrs, "")
	if err != nil {
		return err
	}
	generation.Activate()
	m.NudgeJournalReconcile()
	m.logger.Info().Str("link", row.LinkID).Str("remote", row.RemoteChainID).Str("state", state).Msg("introduced link bound by reconciler")
	return nil
}

// reconcileGuestWindowLocked closes the guest one-sided window: an introduced
// guest is never live until the host confirms, and is revoked on chain once
// the deadline passes without that confirmation.
func (m *Manager) reconcileGuestWindowLocked(ctx context.Context, row *store.IntroducedLink) error {
	if probe := m.introPeerStatusFn; probe != nil && time.Now().Unix() < row.Deadline {
		status, err := probe(ctx, row)
		if err != nil {
			return fmt.Errorf("host status: %w", err)
		}
		switch status {
		case store.IntroducedLinkActive:
			return m.casIntro(ctx, row, store.IntroducedLinkActive, "")
		case store.IntroducedLinkAborted, store.IntroducedLinkRevoked:
			return m.revokeGuestWindowLocked(ctx, row, "peer_not_confirmed: host "+status)
		}
		return nil
	}
	if time.Now().Unix() < row.Deadline {
		return nil
	}
	return m.revokeGuestWindowLocked(ctx, row, "peer_not_confirmed")
}

func (m *Manager) revokeGuestWindowLocked(ctx context.Context, row *store.IntroducedLink, reason string) error {
	if _, err := m.revokeAgreementLockedAs("", row.RemoteChainID); err != nil {
		// tx-34 failed: the row stays bound_pending_peer (denied) and the
		// next tick retries.
		return fmt.Errorf("revoke one-sided guest link: %w", err)
	}
	return m.casIntro(ctx, row, store.IntroducedLinkRevoked, reason)
}

// GuestIntroConfirmed moves a guest row from bound_pending_peer to active once
// the host reports the link active (the IntroConfirmWire response or the
// status poll, wired in B2b).
func (m *Manager) GuestIntroConfirmed(ctx context.Context, linkID string) error {
	ss := m.syncStore()
	if ss == nil {
		return errors.New("introduced link: requires the SQLite store backend")
	}
	unlock := m.LockAgreementMutation()
	defer unlock()
	row, err := ss.GetIntroducedLink(ctx, linkID)
	if err != nil {
		return err
	}
	if row.Role != "guest" || row.State != store.IntroducedLinkBoundPendingPeer {
		return fmt.Errorf("introduced link: %s row in %s cannot be host-confirmed", row.Role, row.State)
	}
	if time.Now().Unix() >= row.Deadline {
		return errors.New("introduced link: host confirmation arrived after the deadline")
	}
	return m.casIntro(ctx, row, store.IntroducedLinkActive, "")
}
