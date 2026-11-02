package store

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"

	badger "github.com/dgraph-io/badger/v4"
)

var (
	ErrFunctionAuthorityPolicyConflict     = errors.New("function authority policy is immutable")
	ErrFunctionAuthorityPolicyNotFound     = errors.New("function authority policy is missing")
	ErrFunctionDesignationRevisionConflict = errors.New("function designation revision conflict")
)

// FunctionAuthorityPolicy is replicated consensus state, not an RBAC role or a
// domain grant. The ABCI caller must authorize its creation and evaluate the
// activation boundary; the store validates structure and create-once semantics.
type FunctionAuthorityPolicy struct {
	Function         string `json:"function"`
	Domain           string `json:"domain"`
	ActivationHeight int64  `json:"activation_height"`
	CreatedHeight    int64  `json:"created_height"`
	CreatedBy        string `json:"created_by"`
	RuleVersion      uint32 `json:"rule_version"`
}

// FunctionDesignation records an explicit function assignment independently of
// role/profile/grant projections. Root authorization, current Root generation,
// enrollment, expiry evaluation and policy activation belong to the ABCI caller.
// Revocation retains the row and advances its revision, preventing ABA reuse.
type FunctionDesignation struct {
	Function       string `json:"function"`
	Domain         string `json:"domain"`
	Subject        string `json:"subject"`
	Revision       uint64 `json:"revision"`
	AssignedBy     string `json:"assigned_by"`
	RootGeneration uint64 `json:"root_generation"`
	UpdatedHeight  int64  `json:"updated_height"`
	ExpiresAt      int64  `json:"expires_at"`
	Revoked        bool   `json:"revoked"`
}

func validateFunctionAuthorityName(function string) error {
	if len(function) == 0 || len(function) > 64 || function[0] < 'a' || function[0] > 'z' {
		return errors.New("function must be a lowercase token of 1 to 64 bytes")
	}
	for i := 1; i < len(function); i++ {
		c := function[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return errors.New("function must be a lowercase token of 1 to 64 bytes")
		}
	}
	return nil
}

func functionAuthorityPolicyKey(function, domain string) ([]byte, error) {
	if err := validateFunctionAuthorityName(function); err != nil {
		return nil, err
	}
	prefix, err := functionAuthorityPolicyDomainPrefix(domain)
	if err != nil {
		return nil, err
	}
	return append(prefix, []byte(hex.EncodeToString([]byte(function)))...), nil
}

func functionAuthorityPolicyDomainPrefix(domain string) ([]byte, error) {
	if err := ValidateAppV23DomainName(domain); err != nil {
		return nil, err
	}
	// Both variable components are encoded, so accepted slash/Unicode domains
	// cannot introduce delimiters or alias another logical tuple. Domain-first
	// ordering allows an exact domain scan without a duplicate consensus index.
	return []byte("funcauth:policy:" + hex.EncodeToString([]byte(domain)) + ":"), nil
}

func functionDesignationKey(function, domain, subject string) ([]byte, error) {
	if _, err := functionAuthorityPolicyKey(function, domain); err != nil {
		return nil, err
	}
	if err := validateCanonicalAgentID("function subject", subject); err != nil {
		return nil, err
	}
	return []byte("funcauth:designation:" + hex.EncodeToString([]byte(function)) + ":" + hex.EncodeToString([]byte(domain)) + ":" + subject), nil
}

func validateFunctionAuthorityPolicy(policy *FunctionAuthorityPolicy) error {
	if policy == nil {
		return errors.New("function authority policy is nil")
	}
	if _, err := functionAuthorityPolicyKey(policy.Function, policy.Domain); err != nil {
		return err
	}
	if policy.RuleVersion != 1 || policy.CreatedHeight <= 0 || policy.ActivationHeight <= policy.CreatedHeight {
		return errors.New("function policy requires rule version 1 and activation after positive creation height")
	}
	return validateCanonicalAgentID("function policy creator", policy.CreatedBy)
}

func validateFunctionDesignation(designation *FunctionDesignation) error {
	if designation == nil {
		return errors.New("function designation is nil")
	}
	if _, err := functionDesignationKey(designation.Function, designation.Domain, designation.Subject); err != nil {
		return err
	}
	if designation.Revision == 0 || designation.RootGeneration == 0 || designation.UpdatedHeight <= 0 || designation.ExpiresAt < 0 {
		return errors.New("function designation requires positive revision, Root generation and height, and nonnegative expiry")
	}
	return validateCanonicalAgentID("function designation assigner", designation.AssignedBy)
}

func readFunctionAuthorityJSON(txn *badger.Txn, key []byte, result any) error {
	item, err := txn.Get(key)
	if err != nil {
		return err
	}
	return item.Value(func(value []byte) error {
		decoder := json.NewDecoder(bytes.NewReader(value))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(result); err != nil {
			return err
		}
		canonical, err := json.Marshal(result)
		if err != nil {
			return err
		}
		// Stored records have exactly one canonical struct encoding. This also
		// refuses duplicate/case-aliased keys, missing fields and trailing JSON.
		if !bytes.Equal(value, canonical) {
			return errors.New("function authority state is not canonical JSON")
		}
		return nil
	})
}

func readFunctionAuthorityPolicy(txn *badger.Txn, function, domain string) (*FunctionAuthorityPolicy, error) {
	key, err := functionAuthorityPolicyKey(function, domain)
	if err != nil {
		return nil, err
	}
	var policy FunctionAuthorityPolicy
	if err := readFunctionAuthorityJSON(txn, key, &policy); err != nil {
		return nil, err
	}
	if err := validateFunctionAuthorityPolicy(&policy); err != nil {
		return nil, err
	}
	if policy.Function != function || policy.Domain != domain {
		return nil, errors.New("function policy key/value mismatch")
	}
	return &policy, nil
}

func readFunctionDesignation(txn *badger.Txn, function, domain, subject string) (*FunctionDesignation, error) {
	key, err := functionDesignationKey(function, domain, subject)
	if err != nil {
		return nil, err
	}
	var designation FunctionDesignation
	if err := readFunctionAuthorityJSON(txn, key, &designation); err != nil {
		return nil, err
	}
	if err := validateFunctionDesignation(&designation); err != nil {
		return nil, err
	}
	if designation.Function != function || designation.Domain != domain || designation.Subject != subject {
		return nil, errors.New("function designation key/value mismatch")
	}
	return &designation, nil
}

// GetFunctionAuthorityPolicy returns nil, nil for a missing tuple and refuses
// malformed stored state. Reads use the executing scoped consensus transaction.
func (s *BadgerStore) GetFunctionAuthorityPolicy(function, domain string) (*FunctionAuthorityPolicy, error) {
	var policy *FunctionAuthorityPolicy
	err := s.view(func(txn *badger.Txn) error {
		var err error
		policy, err = readFunctionAuthorityPolicy(txn, function, domain)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return policy, err
}

// ListFunctionAuthorityPoliciesForDomain returns every canonical policy for
// exactly one domain in deterministic function-key order. It reads the same
// scoped transaction as the current submission, including earlier writes in
// that transaction, and refuses malformed state rather than returning a prefix.
func (s *BadgerStore) ListFunctionAuthorityPoliciesForDomain(domain string) ([]*FunctionAuthorityPolicy, error) {
	prefix, err := functionAuthorityPolicyDomainPrefix(domain)
	if err != nil {
		return nil, err
	}
	policies := make([]*FunctionAuthorityPolicy, 0)
	err = s.view(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.Prefix = prefix
		opts.PrefetchValues = false
		it := txn.NewIterator(opts)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			key := it.Item().KeyCopy(nil)
			var policy FunctionAuthorityPolicy
			if err := readFunctionAuthorityJSON(txn, key, &policy); err != nil {
				return fmt.Errorf("read domain function policy: %w", err)
			}
			if err := validateFunctionAuthorityPolicy(&policy); err != nil {
				return err
			}
			expectedKey, _ := functionAuthorityPolicyKey(policy.Function, policy.Domain)
			if policy.Domain != domain || !bytes.Equal(key, expectedKey) {
				return errors.New("function policy key/value mismatch")
			}
			policies = append(policies, &policy)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return policies, nil
}

// SetFunctionAuthorityPolicy creates an immutable policy. An identical replay
// is a no-op; changing any field conflicts. This method does not authorize Root.
func (s *BadgerStore) SetFunctionAuthorityPolicy(policy *FunctionAuthorityPolicy) error {
	if err := validateFunctionAuthorityPolicy(policy); err != nil {
		return err
	}
	key, _ := functionAuthorityPolicyKey(policy.Function, policy.Domain)
	encoded, err := json.Marshal(policy)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		existing, err := readFunctionAuthorityPolicy(txn, policy.Function, policy.Domain)
		if err == nil {
			if *existing == *policy {
				return nil
			}
			return ErrFunctionAuthorityPolicyConflict
		}
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return fmt.Errorf("read existing function policy: %w", err)
		}
		return s.txnSet(txn, key, encoded)
	})
}

// GetFunctionDesignation returns nil, nil for a missing tuple. It does not
// infer a designation from enrollment, a role, domain ownership or a grant.
func (s *BadgerStore) GetFunctionDesignation(function, domain, subject string) (*FunctionDesignation, error) {
	var designation *FunctionDesignation
	err := s.view(func(txn *badger.Txn) error {
		var err error
		designation, err = readFunctionDesignation(txn, function, domain, subject)
		return err
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return nil, nil
	}
	return designation, err
}

// SetFunctionDesignation atomically compares and advances the stored revision.
// Creation requires expectedRevision=0 and Revision=1. A revoked row remains a
// CAS predecessor; reassigning it must advance the revision. Expiry and current
// Root authority are deliberately evaluated by the consensus caller, not here.
func (s *BadgerStore) SetFunctionDesignation(designation *FunctionDesignation, expectedRevision uint64) error {
	if err := validateFunctionDesignation(designation); err != nil {
		return err
	}
	if expectedRevision == math.MaxUint64 || designation.Revision != expectedRevision+1 {
		return ErrFunctionDesignationRevisionConflict
	}
	key, _ := functionDesignationKey(designation.Function, designation.Domain, designation.Subject)
	encoded, err := json.Marshal(designation)
	if err != nil {
		return err
	}
	return s.update(func(txn *badger.Txn) error {
		policy, err := readFunctionAuthorityPolicy(txn, designation.Function, designation.Domain)
		if errors.Is(err, badger.ErrKeyNotFound) {
			return ErrFunctionAuthorityPolicyNotFound
		}
		if err != nil {
			return err
		}
		if designation.UpdatedHeight < policy.CreatedHeight {
			return errors.New("function designation predates policy creation")
		}
		existing, err := readFunctionDesignation(txn, designation.Function, designation.Domain, designation.Subject)
		if errors.Is(err, badger.ErrKeyNotFound) {
			if expectedRevision != 0 {
				return ErrFunctionDesignationRevisionConflict
			}
		} else if err != nil {
			return err
		} else {
			if existing.Revision != expectedRevision {
				return ErrFunctionDesignationRevisionConflict
			}
			if designation.RootGeneration < existing.RootGeneration || designation.UpdatedHeight < existing.UpdatedHeight {
				return errors.New("function designation metadata regressed")
			}
		}
		return s.txnSet(txn, key, encoded)
	})
}
