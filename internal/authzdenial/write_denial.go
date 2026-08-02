// Package authzdenial defines the stable, public taxonomy for effective memory
// write denials. The seven codes in this package are data-plane protocol;
// control-plane authorization failures must use a separate taxonomy.
package authzdenial

import "strings"

const ProblemTypeURI = "https://sage.dev/errors/domain-write-denied"

type Code string

const (
	CodeMissingWriteGrant      Code = "missing_write_grant"
	CodeForeignWriteRestricted Code = "foreign_write_restricted"
	CodeSharedWriteRestricted  Code = "shared_write_restricted"
	CodeDomainClaimRestricted  Code = "domain_claim_restricted"
	CodePrincipalPendingReview Code = "principal_pending_review"
	CodeNoOwnedHomeDomain      Code = "no_owned_home_domain"
	CodeManagerScopeDenied     Code = "manager_scope_denied"
)

// EffectiveWriteDenial is safe to return to an authenticated caller. Remedy
// text deliberately names only actions the caller/operator can take and never
// includes an agent ID, domain name, owner ID, capability mask, or chain log.
type EffectiveWriteDenial struct {
	Code      Code
	Remedy    string
	Retryable bool
}

var definitions = map[Code]EffectiveWriteDenial{
	CodeMissingWriteGrant: {
		Code:      CodeMissingWriteGrant,
		Remedy:    "Submit to a domain this agent owns. If shared management is intended, ask a local Root or Admin to place the principals in an Access Group and explicitly select Read + write or Read + write + modify; CEREBRUM has no direct level-2 grant editor.",
		Retryable: false,
	},
	CodeForeignWriteRestricted: {
		Code:      CodeForeignWriteRestricted,
		Remedy:    "Assign a write-compatible named profile that permits foreign-domain writes, or submit to a domain this agent owns.",
		Retryable: false,
	},
	CodeSharedWriteRestricted: {
		Code:      CodeSharedWriteRestricted,
		Remedy:    "Submit to the agent's owned non-shared home domain, or assign a named profile that permits shared-domain writes.",
		Retryable: false,
	},
	CodeDomainClaimRestricted: {
		Code:      CodeDomainClaimRestricted,
		Remedy:    "Submit to a domain this agent already owns, or ask a local administrator to assign or reassign a non-shared domain; this profile cannot claim an unowned domain.",
		Retryable: false,
	},
	CodePrincipalPendingReview: {
		Code:      CodePrincipalPendingReview,
		Remedy:    "Approve the agent's enrollment and assign its role and named profile before submitting.",
		Retryable: false,
	},
	CodeNoOwnedHomeDomain: {
		Code:      CodeNoOwnedHomeDomain,
		Remedy:    "Complete enrollment by assigning an owned non-shared home domain, then submit there.",
		Retryable: false,
	},
	CodeManagerScopeDenied: {
		Code:      CodeManagerScopeDenied,
		Remedy:    "Submit to a domain this manager owns, or ask a local Root or Admin to place the manager and domain owner in the same Access Group and explicitly select Read + write or Read + write + modify; the Manager role alone does not widen group authority.",
		Retryable: false,
	},
}

func Definition(code Code) (EffectiveWriteDenial, bool) {
	denial, ok := definitions[code]
	return denial, ok
}

func KnownCodes() []Code {
	return []Code{
		CodeMissingWriteGrant,
		CodeForeignWriteRestricted,
		CodeSharedWriteRestricted,
		CodeDomainClaimRestricted,
		CodePrincipalPendingReview,
		CodeNoOwnedHomeDomain,
		CodeManagerScopeDenied,
	}
}

// ValidateProblem accepts only the complete RFC 7807 write-denial contract.
// Callers must not infer authority or remediation from the problem URI alone,
// an unknown reason code, or an omitted/true retryable extension.
func ValidateProblem(problemType string, code Code, retryable *bool) (EffectiveWriteDenial, bool) {
	if problemType != ProblemTypeURI || retryable == nil || *retryable {
		return EffectiveWriteDenial{}, false
	}
	return Definition(code)
}

// Classify maps current legacy consensus/REST diagnostics and the future-safe
// structured marker "denial_code=<code>" onto the public taxonomy. Matching is
// intentionally narrow: a non-zero capability mask alone is not a denial, and
// generic "access denied"/"not registered" remains unclassified because it may
// be a stale-session symptom that MCP can safely re-heal.
func Classify(err error) (EffectiveWriteDenial, bool) {
	if err == nil {
		return EffectiveWriteDenial{}, false
	}
	return ClassifyText(err.Error())
}

func ClassifyText(message string) (EffectiveWriteDenial, bool) {
	log := consensusLog(strings.ToLower(strings.TrimSpace(message)))
	if code, present, known := structuredCode(log); present {
		if !known {
			return EffectiveWriteDenial{}, false
		}
		return Definition(code)
	}

	if code, ok := legacyCode(log); ok {
		return Definition(code)
	}
	return EffectiveWriteDenial{}, false
}

// legacyCode recognizes only exact historical grammars emitted before app-v23.
// The fixed phrase is parsed before the caller-controlled domain tail, so a
// domain name cannot inject another reason. New app-v23 denials must use the
// structured marker and never add a new free-text case here.
func legacyCode(log string) (Code, bool) {
	if strings.HasPrefix(log, "agent does not have write access to domain '") &&
		strings.HasSuffix(log, "'") {
		return CodeMissingWriteGrant, true
	}
	if log == "access denied: agent enrollment is pending" {
		return CodePrincipalPendingReview, true
	}
	if log == "access denied: no owned home domain is assigned" {
		return CodeNoOwnedHomeDomain, true
	}

	const agentPrefix = "access denied: agent "
	if !strings.HasPrefix(log, agentPrefix) {
		return "", false
	}
	rest := log[len(agentPrefix):]
	if len(rest) < 17 || !isLegacyAgentPrefix(rest[:16]) || rest[16] != ' ' {
		return "", false
	}
	rest = rest[17:]
	switch {
	case strings.HasPrefix(rest, "cannot write shared domain "):
		return CodeSharedWriteRestricted, true
	case strings.HasPrefix(rest, "cannot write domain ") && strings.HasSuffix(rest, " it does not own"):
		return CodeForeignWriteRestricted, true
	case strings.HasPrefix(rest, "cannot claim unowned domain "):
		return CodeDomainClaimRestricted, true
	case strings.HasPrefix(rest, "has no write access to domain "):
		return CodeMissingWriteGrant, true
	default:
		return "", false
	}
}

func isLegacyAgentPrefix(value string) bool {
	if len(value) != 16 {
		return false
	}
	for _, char := range value {
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return true
}

func structuredCode(message string) (code Code, present, known bool) {
	for _, prefix := range []string{
		"access denied: denial_code=",
		"effective write denied: reason_code=",
	} {
		if !strings.HasPrefix(message, prefix) {
			continue
		}
		value := message[len(prefix):]
		if end := strings.IndexAny(value, " \t\r\n,;])}"); end >= 0 {
			value = value[:end]
		}
		code := Code(strings.Trim(value, `"'`))
		if _, ok := definitions[code]; ok {
			return code, true, true
		}
		return "", true, false
	}
	return "", false, false
}

// consensusLog removes only the transport prefix we generate ourselves. It
// does not search for a marker anywhere in the string: domain names and other
// echoed input are untrusted and must not be able to forge a public reason code.
func consensusLog(message string) string {
	for _, marker := range []string{
		"tx rejected in checktx (code ",
		"tx rejected in finalizeblock (code ",
	} {
		if strings.HasPrefix(message, marker) {
			if end := strings.Index(message, "): "); end >= 0 {
				return message[end+3:]
			}
		}
	}
	return message
}
