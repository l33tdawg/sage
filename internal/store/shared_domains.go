package store

import (
	"strings"

	"github.com/l33tdawg/sage/internal/domainname"
)

const appV23MaxDomainNameBytes = domainname.MaxBytes

// ValidateAppV23DomainName validates a domain before it can participate in an
// app-v23 consensus mutation. Domain and grant keys historically use ':' as a
// component delimiter, so accepting that byte in a domain would let one
// logical (domain, principal) pair alias another. Empty dotted segments are
// also non-canonical because the ownership walkers deliberately drop them.
//
// Slash-delimited product domains such as "technical/hardware" remain valid;
// app-v23 tightens only ambiguous/control syntax rather than changing the
// established domain vocabulary.
func ValidateAppV23DomainName(name string) error {
	return domainname.Validate(name)
}

// sharedDomainsLocal mirrors the reserved catch-all domain names that the ABCI
// layer (see internal/abci/app.go:isSharedDomain) treats as "no single owner"
// writable by any authenticated agent. They are NEVER inheritable as ancestors
// in the dotted-domain hierarchy — a grant on "general" must not silently
// cascade to "pipeline.general" or any other child whose tail happens to match.
//
// Kept in lock-step with the abci-side map. Both call sites read the same
// truth — duplication is intentional because the store package cannot import
// abci. Fix 3 will fold this into an on-chain registry; until then, edit both
// places together or break the access barrier.
var sharedDomainsLocal = map[string]struct{}{
	"general": {},
	"self":    {},
	"meta":    {},
}

// sharedDomainPrefixesLocal mirrors the cross-cutting prefix families (e.g.
// "sage-*") that follow the same no-single-owner semantics as the entries in
// sharedDomainsLocal. See the abci-side companion for the rationale.
var sharedDomainPrefixesLocal = []string{
	"sage-",
}

// IsSharedDomainName reports whether the given dotted-path candidate is a
// reserved shared domain. Used by HasAccessOrAncestor and
// ResolveOwningAncestor as the cascade barrier — shared domains never grant
// inheritable access to their descendants.
func IsSharedDomainName(name string) bool {
	if _, ok := sharedDomainsLocal[name]; ok {
		return true
	}
	for _, p := range sharedDomainPrefixesLocal {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}
