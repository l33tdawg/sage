package store

import (
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// AgentCapabilityInspectDirectGrants permits only the dedicated local grant
// metadata read. It is never implied by a role or included in enrollment defaults.
// Historical Valid and KnownAgentCapabilities deliberately remain app-v22 masks.
const AgentCapabilityInspectDirectGrants AgentCapabilities = 1 << 5

// ValidateAppV31Policy adds one optional authority to an otherwise unchanged
// named ordinary-agent policy. Root and migration-only profiles cannot hold it.
func ValidateAppV31Policy(role, profile string, capabilities AgentCapabilities, clearance uint8) error {
	if capabilities.Has(AgentCapabilityInspectDirectGrants) && (profile == AppV23ProfileRoot || profile == AppV23ProfileLegacyRestricted) {
		return errors.New("grant inspection requires a named ordinary-agent profile")
	}
	return ValidateAppV23Policy(role, profile, capabilities&^AgentCapabilityInspectDirectGrants, clearance)
}

// appV31ActiveTxn reads the activation authority in the caller's canonical
// snapshot. Startup independently validates the complete predecessor ladder.
func appV31ActiveTxn(txn *badger.Txn) (bool, error) {
	var fresh FreshGenesisActivation
	freshErr := appV23ReadJSON(txn, freshGenesisActivationKey(), &fresh)
	if freshErr != nil && !errors.Is(freshErr, badger.ErrKeyNotFound) {
		return false, freshErr
	}
	if freshErr == nil {
		if err := ValidateFreshGenesisActivation(fresh); err != nil {
			return false, err
		}
	}
	var rec AppliedUpgradeRecord
	err := appV23ReadJSON(txn, upgradeAppliedKey("app-v31"), &rec)
	if errors.Is(err, badger.ErrKeyNotFound) {
		return freshErr == nil && fresh.AppVersion >= 31, nil
	}
	if err != nil {
		return false, err
	}
	if rec.Name != "app-v31" || rec.TargetAppVersion != 31 || rec.AppliedHeight <= 0 {
		return false, errors.New("invalid app-v31 activation")
	}
	if freshErr == nil && fresh.AppVersion >= 31 {
		return false, errors.New("app-v31 origin conflicts with applied activation")
	}
	if freshErr == nil && fresh.AppVersion == 30 {
		return true, nil
	}
	var predecessor AppliedUpgradeRecord
	if err := appV23ReadJSON(txn, upgradeAppliedKey("app-v30"), &predecessor); err != nil {
		return false, err
	}
	if predecessor.Name != "app-v30" || predecessor.TargetAppVersion != 30 || predecessor.AppliedHeight <= 0 || predecessor.AppliedHeight >= rec.AppliedHeight {
		return false, errors.New("invalid app-v31 predecessor")
	}
	return true, nil
}

func (s *BadgerStore) IsAppV31Active() (bool, error) {
	var active bool
	err := s.view(func(txn *badger.Txn) error { var err error; active, err = appV31ActiveTxn(txn); return err })
	return active, err
}

func (s *BadgerStore) ValidateCurrentAgentPolicy(role, profile string, capabilities AgentCapabilities, clearance uint8) error {
	if !capabilities.Has(AgentCapabilityInspectDirectGrants) {
		return ValidateAppV23Policy(role, profile, capabilities, clearance)
	}
	active, err := s.IsAppV31Active()
	if err != nil {
		return err
	}
	if active {
		return ValidateAppV31Policy(role, profile, capabilities, clearance)
	}
	return ValidateAppV23Policy(role, profile, capabilities, clearance)
}

func (s *BadgerStore) validateCurrentEnrollmentPolicy(role, profile string, capabilities AgentCapabilities, clearance uint8, _ bool) error {
	return s.ValidateCurrentAgentPolicy(role, profile, capabilities, clearance)
}

func (s *BadgerStore) validateCurrentCapabilities(capabilities AgentCapabilities) error {
	if capabilities.Valid() {
		return nil
	}
	active, err := s.IsAppV31Active()
	if err != nil {
		return err
	}
	if active {
		capabilities &^= AgentCapabilityInspectDirectGrants
	}
	if !capabilities.Valid() {
		return fmt.Errorf("unknown agent capability bits: 0x%x", uint32(capabilities&^KnownAgentCapabilities))
	}
	return nil
}

// requireInspectionPolicyActorTxn rechecks the existing control-plane authority
// in the same transaction that assigns or renews the inspection capability.
// Inspection itself never supplies this mutation authority.
func (s *BadgerStore) requireInspectionPolicyActorTxn(txn *badger.Txn, root AppV23RootState, actorID string) error {
	if actorID == root.CredentialID {
		return nil
	}
	var enrollment AppV23LocalEnrollment
	var role AppV23RoleState
	if err := s.appV23ReadEffectiveJSONTxn(txn, appV23EnrollmentKey(actorID), &enrollment); err != nil {
		return ErrAppV23NeedsApproval
	}
	if err := s.appV23ReadEffectiveJSONTxn(txn, appV23RoleKey(actorID), &role); err != nil {
		return ErrAppV23NeedsApproval
	}
	if enrollment.AgentID != actorID || role.AgentID != actorID || !enrollment.Active || enrollment.RootGeneration != root.Generation || enrollment.Profile == AppV23ProfileRoot || role.Role != AppV23RoleAdmin || ValidateAppV31Policy(role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance) != nil {
		return ErrAppV23NeedsApproval
	}
	return nil
}
