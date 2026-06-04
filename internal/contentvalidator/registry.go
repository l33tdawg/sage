// Package contentvalidator provides a generic, deployment-agnostic Layer-2
// content-aware schema gate for memory submissions.
//
// A ContentValidator is a pure function of a single memory record: it inspects
// the record (typically its Content body) and returns nil to accept or a non-nil
// error to REJECT. The error string is surfaced verbatim as the on-chain reject
// Log, so it must be deterministic across all validators and free of any
// node-local or non-reproducible state.
//
// This package ships ZERO knowledge of any particular deployment, domain, or
// outcome schema. Validators are registered at boot by the embedding binary,
// keyed by (domain, outcomeClass). A node that registers nothing — or never
// activates the gate — behaves exactly as a node without this package: every
// submission passes through untouched.
package contentvalidator

import (
	"fmt"

	"github.com/l33tdawg/sage/internal/memory"
)

// ContentValidator is a pure function of a memory record. It returns nil to
// accept the record, or a non-nil error to reject it. The error string becomes
// the on-chain reject Log, so it MUST be deterministic across all validators.
type ContentValidator func(rec *memory.MemoryRecord) error

// validatorKey identifies a registered validator by the (domain, outcomeClass)
// pair it applies to. Both components are matched exactly.
type validatorKey struct {
	domain       string
	outcomeClass string
}

// ContentValidatorRegistry maps (domain, outcomeClass) pairs to the validator
// that gates submissions of that shape. The maps are populated once at boot and
// are treated as read-only (frozen) for the lifetime of the process.
type ContentValidatorRegistry struct {
	m map[validatorKey]ContentValidator
	// domains holds every domain that has at least one registered validator.
	// It scopes closed-domain enforcement (see RegisterClosedDomain): a closed
	// domain with zero validators is a no-op rather than a total lockout.
	domains map[string]struct{}
	// closed is the set of domains marked closed-schema. On a closed domain, a
	// submission whose (domain, outcomeClass) has no registered validator is
	// REJECTED instead of passing through. Open domains (the default) keep the
	// backward-compatible pass-through.
	closed map[string]struct{}
}

// NewContentValidatorRegistry returns an empty registry. An empty registry is a
// pass-through: Validate accepts every key until a validator is registered.
func NewContentValidatorRegistry() *ContentValidatorRegistry {
	return &ContentValidatorRegistry{
		m:       make(map[validatorKey]ContentValidator),
		domains: make(map[string]struct{}),
		closed:  make(map[string]struct{}),
	}
}

// RegisterContentValidator binds a validator to a (domain, outcomeClass) pair.
//
// This is a boot-only operation. The map is frozen during consensus: callers
// must register every validator before the ABCI app begins processing blocks,
// and must never mutate the registry afterwards. Concurrent registration during
// FinalizeBlock would make the gate non-deterministic across validators.
func (r *ContentValidatorRegistry) RegisterContentValidator(domain, outcomeClass string, v ContentValidator) {
	r.m[validatorKey{domain: domain, outcomeClass: outcomeClass}] = v
	r.domains[domain] = struct{}{}
}

// RegisterClosedDomain marks domain as closed-schema: once it has at least one
// registered validator, a submission to it whose outcome_class has NO registered
// validator is REJECTED by Validate instead of passing through. This is the
// opt-in that makes the gate non-bypassable — without it, a body that routes to
// an unregistered class (a malformed/cross-class outcome_class, or a router miss)
// commits unvalidated.
//
// Deployment-agnostic: SAGE ships no closed domains by default, so existing
// consumers are unaffected (every domain stays open / pass-through). A closed
// domain with ZERO registered validators is intentionally a no-op — closing a
// domain you have not yet registered any validator for must not lock it out
// entirely. Boot-only, same frozen-after-boot contract as RegisterContentValidator.
func (r *ContentValidatorRegistry) RegisterClosedDomain(domain string) {
	r.closed[domain] = struct{}{}
}

// Validate runs the validator registered for (domain, outcomeClass), if any,
// against rec. It returns (rejected, reason): when rejected is true, reason is
// the validator's error string for the on-chain reject Log.
//
// Pass-through semantics (all return (false, "")):
//   - a nil receiver, or an empty registry;
//   - a key with NO registered validator on an OPEN domain (backward-compat:
//     unknown shapes are never rejected by this layer);
//   - a registered validator that returns nil.
//
// CRITICAL: on an OPEN domain, pass-through is ONLY for keys with no registered
// validator. A REGISTERED key whose validator returns an error — including the
// case where the validator chokes on a malformed body — is a HARD REJECT, not a
// pass-through. Registering a validator is an explicit opt-in to gate that
// shape, so anything it cannot accept must be turned away.
//
// CLOSED domains (RegisterClosedDomain) tighten the unregistered-key case: once
// a closed domain has any registered validator, a key with NO registered
// validator on it is REJECTED rather than passed through. This closes the
// fail-open hole where a body routing to an unregistered/empty outcome_class
// (malformed, cross-class, or a router miss) would commit unvalidated.
func (r *ContentValidatorRegistry) Validate(domain, outcomeClass string, rec *memory.MemoryRecord) (bool, string) {
	if r == nil || len(r.m) == 0 {
		return false, ""
	}

	v, ok := r.m[validatorKey{domain: domain, outcomeClass: outcomeClass}]
	if !ok {
		// No validator for this exact (domain, class). On a closed domain that
		// has at least one validator, an unregistered class is a hard reject;
		// open domains keep the backward-compatible pass-through.
		if _, isClosed := r.closed[domain]; isClosed {
			if _, hasValidators := r.domains[domain]; hasValidators {
				return true, fmt.Sprintf("unrecognized outcome_class %q for closed domain %q", outcomeClass, domain)
			}
		}
		return false, ""
	}

	if err := v(rec); err != nil {
		return true, err.Error()
	}
	return false, ""
}
