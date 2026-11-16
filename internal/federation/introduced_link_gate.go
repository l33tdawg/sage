package federation

// RFC #443 Phase B: the fail-closed introduced-link hook on ActiveAgreement and
// the owner-envelope cap on outbound sharing.
//
// An org-introduced link has a node-local introduced_link row; a manual (QR
// JOIN) agreement has none. ActiveAgreement / ActiveAgreements deny any
// agreement whose remote chain has a row unless the NEWEST row is active, so
// an introduced link is never usable before it is bound, while it is paused,
// or after it was revoked or aborted. Manual agreements are unchanged.
//
// federation stays independent of where the rows live: the REST layer installs
// an IntroducedLinkSource backed by the node-local SQLite store. With no source
// installed (nodes and tests with no node-local store) nothing changes.

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

// IntroducedLinkSource reads introduced-link rows. Implementations must return
// (nil, nil) for a chain with no row and an error for any storage failure.
type IntroducedLinkSource interface {
	LatestIntroducedLinkByChain(ctx context.Context, remoteChainID string) (*store.IntroducedLink, error)
	LatestIntroducedLinkStates(ctx context.Context) (map[string]string, error)
}

// IntroducedSharingCap checks an outbound sharing snapshot for an introduced
// agreement against the owner envelope of the link's org. A non-nil error
// refuses the update.
type IntroducedSharingCap func(orgID, remoteChainID string, domains []string, clearance uint8) error

// ErrIntroducedLinkNotActive is returned by ActiveAgreement for an agreement
// whose newest introduced-link row is not active.
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

// introducedLinkDenial is the extra fail-closed ActiveAgreement check.
func (m *Manager) introducedLinkDenial(remoteChainID string) error {
	src := m.introducedLinkSource()
	if src == nil {
		return nil
	}
	row, err := src.LatestIntroducedLinkByChain(context.Background(), remoteChainID)
	if err != nil {
		return fmt.Errorf("agreement %s: introduced-link state unavailable: %w", remoteChainID, err)
	}
	if row == nil || row.State == store.IntroducedLinkActive {
		return nil
	}
	return fmt.Errorf("agreement %s: %w (state %q)", remoteChainID, ErrIntroducedLinkNotActive, row.State)
}

// introducedLinkStates returns the newest-row state per chain for the
// ActiveAgreements filter, or (nil, false) when no source is installed.
func (m *Manager) introducedLinkStates() (map[string]string, bool, error) {
	src := m.introducedLinkSource()
	if src == nil {
		return nil, false, nil
	}
	states, err := src.LatestIntroducedLinkStates(context.Background())
	return states, true, err
}

// checkIntroducedSharing applies the owner envelope to a sharing update of an
// introduced agreement. A manual agreement (no row) is unchanged.
func (m *Manager) checkIntroducedSharing(remoteChainID string, domains []string, clearance uint8) error {
	src := m.introducedLinkSource()
	if src == nil {
		return nil
	}
	row, err := src.LatestIntroducedLinkByChain(context.Background(), remoteChainID)
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
// agreement to its pinned CA SPKI (hex). It reads the consensus records only,
// ignoring introduced-link state, so offers can tell "no agreement" from
// "agreement whose introduced link is paused".
func (m *Manager) OrgPeerAgreements() (map[string]string, error) {
	if m.badger == nil {
		return nil, errors.New("consensus state store is unavailable")
	}
	all, err := m.badger.ListCrossFed()
	if err != nil {
		return nil, fmt.Errorf("list cross_fed agreements: %w", err)
	}
	now := time.Now().Unix()
	out := make(map[string]string, len(all))
	for _, rec := range all {
		if rec.Status != "active" || (rec.ExpiresAt != 0 && now >= rec.ExpiresAt) {
			continue
		}
		out[rec.RemoteChainID] = hex.EncodeToString(rec.PeerPubKey)
	}
	return out, nil
}
