package store

import (
	"encoding/json"
	"errors"
	"fmt"

	badger "github.com/dgraph-io/badger/v4"
)

// UpgradePlanRecord is the on-chain representation of a pending v7.5
// upgrade. Exactly one plan is active at a time — keyed at
// "upgrade:plan". Cancel removes it; activation (in FinalizeBlock at
// ActivationHeight) marks it applied and moves it to
// "upgrade:applied:<name>".
//
// Wire-format note: this struct is JSON-serialized into BadgerDB and
// MUST be backward-compatible across binary versions. New fields are
// fine; renames break old reads. ProposedAt + ProposerID are tracked
// for audit/replay, not consensus.
type UpgradePlanRecord struct {
	Name             string `json:"name"`
	TargetAppVersion uint64 `json:"target_app_version"`
	ActivationHeight int64  `json:"activation_height"`
	BinarySHA256     string `json:"binary_sha256,omitempty"`
	GovernanceDomain string `json:"governance_domain,omitempty"`
	ProposedAt       int64  `json:"proposed_at"`
	ProposerID       string `json:"proposer_id"`
}

// AppliedUpgradeRecord captures the moment an upgrade landed. Stored
// under "upgrade:applied:<name>" forever (never garbage-collected) so
// the chain has a deterministic audit trail of every activation.
type AppliedUpgradeRecord struct {
	Name             string `json:"name"`
	TargetAppVersion uint64 `json:"target_app_version"`
	AppliedHeight    int64  `json:"applied_height"`
}

// ErrNoUpgradePlan is returned by GetUpgradePlan when no pending plan
// exists. Callers use errors.Is to distinguish from genuine read
// failures.
var ErrNoUpgradePlan = errors.New("no pending upgrade plan")

func upgradePlanKey() []byte            { return []byte("upgrade:plan") }
func upgradeAppliedKey(n string) []byte { return []byte("upgrade:applied:" + n) }

// SetUpgradePlan persists rec as the single pending plan. Overwrites
// any prior record at the same key — callers (processUpgradeCancel,
// processUpgradePropose) are responsible for enforcing "at most one
// pending plan" semantics before calling this.
func (s *BadgerStore) SetUpgradePlan(rec *UpgradePlanRecord) error {
	if rec == nil {
		return errors.New("SetUpgradePlan: nil record")
	}
	if rec.Name == "" {
		return errors.New("SetUpgradePlan: empty name")
	}
	if rec.TargetAppVersion == 0 {
		return errors.New("SetUpgradePlan: zero target_app_version")
	}
	if rec.ActivationHeight <= 0 {
		return errors.New("SetUpgradePlan: non-positive activation_height")
	}
	payload, err := json.Marshal(rec)
	if err != nil {
		return fmt.Errorf("marshal upgrade plan: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		return s.txnSet(txn, upgradePlanKey(), payload)
	})
}

// GetUpgradePlan reads the pending plan. Returns nil, ErrNoUpgradePlan
// when none exists.
func (s *BadgerStore) GetUpgradePlan() (*UpgradePlanRecord, error) {
	var rec *UpgradePlanRecord
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(upgradePlanKey())
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			var r UpgradePlanRecord
			if jErr := json.Unmarshal(val, &r); jErr != nil {
				return jErr
			}
			rec = &r
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, ErrNoUpgradePlan
	}
	if err != nil {
		return nil, fmt.Errorf("read upgrade plan: %w", err)
	}
	return rec, nil
}

// DeleteUpgradePlan removes the pending plan unconditionally. Returns
// nil if no plan exists (idempotent).
func (s *BadgerStore) DeleteUpgradePlan() error {
	return s.update(func(txn *badger.Txn) error {
		if dErr := s.txnDelete(txn, upgradePlanKey()); dErr != nil && !errors.Is(dErr, badger.ErrKeyNotFound) {
			return dErr
		}
		return nil
	})
}

// MarkUpgradeApplied records that an upgrade activated at the given
// height. Also clears the pending plan in the same transaction so the
// state machine atomically transitions from "pending" to "applied".
func (s *BadgerStore) MarkUpgradeApplied(name string, targetAppVersion uint64, appliedHeight int64) error {
	if name == "" {
		return errors.New("MarkUpgradeApplied: empty name")
	}
	rec := AppliedUpgradeRecord{
		Name:             name,
		TargetAppVersion: targetAppVersion,
		AppliedHeight:    appliedHeight,
	}
	payload, err := json.Marshal(&rec)
	if err != nil {
		return fmt.Errorf("marshal applied record: %w", err)
	}
	return s.update(func(txn *badger.Txn) error {
		if sErr := s.txnSet(txn, upgradeAppliedKey(name), payload); sErr != nil {
			return sErr
		}
		if dErr := s.txnDelete(txn, upgradePlanKey()); dErr != nil && !errors.Is(dErr, badger.ErrKeyNotFound) {
			return dErr
		}
		return nil
	})
}

// GetAppliedUpgrade reads an audit record. Returns nil, nil if absent
// (so callers can distinguish "never applied" from "read error").
func (s *BadgerStore) GetAppliedUpgrade(name string) (*AppliedUpgradeRecord, error) {
	var rec *AppliedUpgradeRecord
	err := s.view(func(txn *badger.Txn) error {
		item, getErr := txn.Get(upgradeAppliedKey(name))
		if getErr != nil {
			return getErr
		}
		return item.Value(func(val []byte) error {
			var r AppliedUpgradeRecord
			if jErr := json.Unmarshal(val, &r); jErr != nil {
				return jErr
			}
			rec = &r
			return nil
		})
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read applied record: %w", err)
	}
	return rec, nil
}
