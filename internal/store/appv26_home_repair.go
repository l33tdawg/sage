package store

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/dgraph-io/badger/v4"
)

const appV26HomeRepairPrefix = "appv26:home-repair:"

type AppV26HomeRepair struct {
	AgentID          string `json:"agent_id"`
	PreviousHome     string `json:"previous_home"`
	ReplacementHome  string `json:"replacement_home"`
	Reason           string `json:"reason"`
	PreviousRevision uint64 `json:"previous_revision"`
	NewRevision      uint64 `json:"new_revision"`
	AppliedHeight    int64  `json:"applied_height"`
}

func appV26HomeRepairKey(agentID string) []byte {
	return []byte(appV26HomeRepairPrefix + agentID)
}

// appV23RecoverableHomeDefectTxn classifies only the historical invariant
// defects the app-v25 batch continuity bug could commit. Invalid non-empty
// domain syntax and every unrelated policy invariant remain fatal.
func (s *BadgerStore) appV23RecoverableHomeDefectTxn(
	txn *badger.Txn, enrollment AppV23LocalEnrollment,
) (string, bool, error) {
	if !enrollment.Active || enrollment.Profile == AppV23ProfileRoot ||
		enrollment.Profile == AppV23ProfileReadOnly {
		return "", false, nil
	}
	if enrollment.HomeDomain == "" {
		if AppV23AllowsMigratedDomainless(enrollment.Profile, enrollment.Capabilities) {
			return "", false, nil
		}
		return "empty_home", true, nil
	}
	if err := ValidateAppV23DomainName(enrollment.HomeDomain); err != nil {
		return "", false, err
	}
	shared, err := appV23DomainIsSharedTxn(txn, enrollment.HomeDomain)
	if err != nil {
		return "", false, err
	}
	if shared {
		return "shared_home", true, nil
	}
	value, err := s.appV23ReadEffectiveValueTxn(txn, domainKey(enrollment.HomeDomain))
	if errors.Is(err, badger.ErrKeyNotFound) {
		return "missing_home", true, nil
	}
	if err != nil {
		return "", false, err
	}
	owner, _, err := decodeString(value, 0)
	if err != nil {
		return "", false, err
	}
	if owner != enrollment.AgentID {
		return "foreign_home", true, nil
	}
	return "", false, nil
}

func (s *BadgerStore) repairAppV26EnrollmentHomesTxn(txn *badger.Txn, height int64) error {
	if height <= 0 {
		return errors.New("app-v26 home repair height must be positive")
	}
	enrollments := make([]AppV23LocalEnrollment, 0)
	prefix := []byte("appv23:enroll:")
	if err := s.appV23EffectivePrefixTxn(txn, prefix, func(_ []byte, value []byte) error {
		var enrollment AppV23LocalEnrollment
		if err := json.Unmarshal(value, &enrollment); err != nil {
			return err
		}
		enrollments = append(enrollments, enrollment)
		return nil
	}); err != nil {
		return err
	}
	for _, enrollment := range enrollments {
		reason, defective, defectErr := s.appV23RecoverableHomeDefectTxn(txn, enrollment)
		if defectErr != nil {
			return fmt.Errorf("inspect app-v26 home repair for %s: %w", enrollment.AgentID, defectErr)
		}
		if !defective {
			continue
		}
		if _, getErr := txn.Get(appV26HomeRepairKey(enrollment.AgentID)); getErr == nil {
			return fmt.Errorf("app-v26 home repair already exists for %s", enrollment.AgentID)
		} else if !errors.Is(getErr, badger.ErrKeyNotFound) {
			return getErr
		}
		previousHome := enrollment.HomeDomain
		previousRevision := enrollment.Revision
		replacement, err := appV25AllocateContinuityHomeTxn(
			s, txn, enrollment.AgentID, height, true,
		)
		if err != nil {
			return err
		}
		enrollment.HomeDomain = replacement
		enrollment.Revision++
		enrollment.UpdatedHeight = height
		data, err := appV23Marshal(enrollment)
		if err != nil {
			return err
		}
		if setErr := s.txnSet(txn, appV23EnrollmentKey(enrollment.AgentID), data); setErr != nil {
			return setErr
		}
		audit, err := appV23Marshal(AppV26HomeRepair{
			AgentID: enrollment.AgentID, PreviousHome: previousHome,
			ReplacementHome: replacement, Reason: reason,
			PreviousRevision: previousRevision, NewRevision: enrollment.Revision,
			AppliedHeight: height,
		})
		if err != nil {
			return err
		}
		if err := s.txnSet(txn, appV26HomeRepairKey(enrollment.AgentID), audit); err != nil {
			return err
		}
	}
	return nil
}
