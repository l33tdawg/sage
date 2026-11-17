package federation

// RFC #443 Phase B: the fail-closed introduced-link hook on ActiveAgreement and
// the owner-envelope cap on outbound sharing.
//
// An org-introduced link has a node-local introduced_link row bound to the
// exact agreement generation it commits (store.CrossFedGeneration). A manual
// (QR JOIN) agreement has no row with its generation. ActiveAgreement /
// ActiveAgreements deny an agreement whose current generation has a row
// unless the NEWEST row for that generation is active, so an introduced link
// is never usable before it is bound, while it is paused, or after it was
// revoked or aborted. Rows for other generations of the same chain (an older
// failed attempt, an earlier introduced link) neither block nor authorize the
// current agreement.
//
// federation stays independent of where the rows live: the REST layer installs
// an IntroducedLinkSource backed by the node-local SQLite store. With no source
// installed (nodes and tests with no node-local store) nothing changes.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

// IntroducedLinkSource reads introduced-link rows. Implementations must return
// (nil, nil) when no row is bound to the generation and an error for any
// storage failure.
type IntroducedLinkSource interface {
	IntroducedLinkForGeneration(ctx context.Context, remoteChainID, generation string) (*store.IntroducedLink, error)
	IntroducedLinkGenerationStates(ctx context.Context) (map[string]map[string]string, error)
}

// IntroducedSharingCap checks an outbound sharing snapshot for an introduced
// agreement against the owner envelope of the link's org. A non-nil error
// refuses the update.
type IntroducedSharingCap func(orgID, remoteChainID string, domains []string, clearance uint8) error

// ErrIntroducedLinkNotActive is returned by ActiveAgreement for an agreement
// whose generation is bound to an introduced-link row that is not active.
var ErrIntroducedLinkNotActive = errors.New("introduced link is not active")

// ErrSharingOutsideEnvelope refuses a sharing update outside the owner
// envelope of an introduced link's org.
var ErrSharingOutsideEnvelope = errors.New("sharing is outside the owner envelope")

// SetIntroducedLinkSource installs the introduced-link row source.
func (m *Manager) SetIntroducedLinkSource(src IntroducedLinkSource) {
	m.introducedMu.Lock()
	m.introducedLinks = src
	m.introducedMu.Unlock()
}

// SetIntroducedSharingCap installs the owner-envelope sharing check.
func (m *Manager) SetIntroducedSharingCap(c IntroducedSharingCap) {
	m.introducedMu.Lock()
	m.introducedSharingCap = c
	m.introducedMu.Unlock()
}

func (m *Manager) introducedLinkSource() IntroducedLinkSource {
	m.introducedMu.RLock()
	defer m.introducedMu.RUnlock()
	return m.introducedLinks
}

func (m *Manager) introducedSharingCapFn() IntroducedSharingCap {
	m.introducedMu.RLock()
	defer m.introducedMu.RUnlock()
	return m.introducedSharingCap
}

// introducedLinkFor returns the row bound to rec's generation, or nil.
func (m *Manager) introducedLinkFor(rec *store.CrossFedRecord) (*store.IntroducedLink, error) {
	src := m.introducedLinkSource()
	if src == nil {
		return nil, nil
	}
	return src.IntroducedLinkForGeneration(context.Background(), rec.RemoteChainID, store.CrossFedGeneration(*rec))
}

// introducedLinkDenial is the extra fail-closed ActiveAgreement check.
func (m *Manager) introducedLinkDenial(rec *store.CrossFedRecord) error {
	row, err := m.introducedLinkFor(rec)
	if err != nil {
		return fmt.Errorf("agreement %s: introduced-link state unavailable: %w", rec.RemoteChainID, err)
	}
	if row == nil || row.State == store.IntroducedLinkActive {
		return nil
	}
	return fmt.Errorf("agreement %s: %w (state %q)", rec.RemoteChainID, ErrIntroducedLinkNotActive, row.State)
}

// introducedLinkStates returns chain → generation → newest-row state for the
// ActiveAgreements filter, or (nil, false) when no source is installed.
func (m *Manager) introducedLinkStates() (map[string]map[string]string, bool, error) {
	src := m.introducedLinkSource()
	if src == nil {
		return nil, false, nil
	}
	states, err := src.IntroducedLinkGenerationStates(context.Background())
	return states, true, err
}

// introducedStateAllows applies the generation-bound filter to one record.
func introducedStateAllows(states map[string]map[string]string, rec *store.CrossFedRecord) bool {
	byGen := states[rec.RemoteChainID]
	if len(byGen) == 0 {
		return true
	}
	state, ok := byGen[store.CrossFedGeneration(*rec)]
	return !ok || state == store.IntroducedLinkActive
}

// checkIntroducedSharing applies the owner envelope to a sharing update of an
// introduced agreement. A manual agreement (no row bound to its generation) is
// unchanged. The update keeps the generation, so the row stays bound to it.
func (m *Manager) checkIntroducedSharing(agreement *store.CrossFedRecord, domains []string) error {
	remoteChainID, clearance := agreement.RemoteChainID, agreement.MaxClearance
	row, err := m.introducedLinkFor(agreement)
	if err != nil {
		return fmt.Errorf("%w: introduced-link state unavailable: %v", ErrSharingOutsideEnvelope, err)
	}
	if row == nil {
		return nil
	}
	capFn := m.introducedSharingCapFn()
	if capFn == nil {
		return fmt.Errorf("%w: no owner envelope check is available", ErrSharingOutsideEnvelope)
	}
	if err := capFn(row.OrgID, remoteChainID, domains, clearance); err != nil {
		if errors.Is(err, ErrSharingOutsideEnvelope) {
			return err
		}
		return fmt.Errorf("%w: %v", ErrSharingOutsideEnvelope, err)
	}
	return nil
}

// OrgPeerAgreements maps every remote chain with an active, unexpired tx-33
// agreement to its record. It reads the consensus records only, ignoring
// introduced-link state, so offers can tell "no agreement" from "agreement
// whose introduced link is paused".
func (m *Manager) OrgPeerAgreements() (map[string]store.CrossFedRecord, error) {
	if m.badger == nil {
		return nil, errors.New("consensus state store is unavailable")
	}
	all, err := m.badger.ListCrossFed()
	if err != nil {
		return nil, fmt.Errorf("list cross_fed agreements: %w", err)
	}
	now := time.Now().Unix()
	out := make(map[string]store.CrossFedRecord, len(all))
	for _, rec := range all {
		if rec.Status != "active" || (rec.ExpiresAt != 0 && now >= rec.ExpiresAt) {
			continue
		}
		out[rec.RemoteChainID] = rec
	}
	return out, nil
}
