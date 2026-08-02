package rest

import (
	"context"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	callerReadableDomainLimit         = 64
	callerReadableDomainCandidateScan = 256
	callerAuthoredDomainQueryBudget   = 250 * time.Millisecond
)

type callerReadableDomainsResponse struct {
	Domains   []string `json:"domains"`
	Truncated bool     `json:"truncated"`
	Scope     string   `json:"scope"`
}

// handleGetAgentReadableDomains returns a bounded, caller-only set of useful
// scoped-recall targets. Candidates come only from the caller's own home/
// authorship, its direct grants, and its local Access Groups. Every candidate
// is re-authorized against current policy before disclosure. This avoids both
// a global domain-roster leak and an unscoped memory-content scan.
func (s *Server) handleGetAgentReadableDomains(w http.ResponseWriter, r *http.Request) {
	agentID := middleware.ContextAgentID(r.Context())
	if agentID == "" {
		writeProblem(w, http.StatusUnauthorized, "Unauthorized", "No agent ID in context.")
		return
	}
	if !s.isPostV23ForNextTx() || s.badgerStore == nil {
		writeProblem(w, http.StatusNotImplemented, "Unavailable",
			"Bounded caller domain discovery requires app-v23 access control.")
		return
	}
	active, err := s.appV23ActiveOrdinaryAgent(agentID)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
			"Current local enrollment state is unavailable.")
		return
	}
	if !active {
		writeProblem(w, http.StatusForbidden, "Agent inactive",
			"Only an active ordinary local agent can discover readable domains.")
		return
	}
	enrollment, err := s.badgerStore.GetAppV23Enrollment(agentID)
	if err != nil || enrollment == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
			"Current local enrollment state is unavailable.")
		return
	}

	candidates := make(map[string]struct{}, 16)
	truncated := enrollment.Capabilities.Has(store.AgentCapabilityReadAllDomains)
	add := func(domain string) {
		domain = strings.TrimSpace(domain)
		if domain == "" {
			return
		}
		if _, exists := candidates[domain]; exists {
			return
		}
		if len(candidates) == callerReadableDomainCandidateScan {
			truncated = true
			return
		}
		candidates[domain] = struct{}{}
	}
	add(enrollment.HomeDomain)
	now := time.Now()
	queryCtx, cancel := context.WithTimeout(r.Context(), callerAuthoredDomainQueryBudget)
	defer cancel()

	if registered, getErr := s.badgerStore.GetRegisteredAgent(agentID); getErr == nil && registered != nil {
		for _, domain := range strings.Split(registered.DomainAccess, ",") {
			add(domain)
		}
	} else {
		truncated = true
	}
	// Direct grants are indexed by grantee in the SQL serving projection. Do
	// not enumerate the consensus grant keyspace here: its primary key is
	// domain-first, so finding a caller with few grants on a mature node would
	// otherwise require walking every other principal's grants. SQL supplies
	// candidates only; hasMemoryReadAccess below re-authorizes each one against
	// current consensus state, so a stale mirror can at worst make this bounded
	// hint incomplete, never disclose a revoked or foreign domain.
	if accessStore, ok := s.agentStore.(store.AccessStore); ok {
		grantLimit := callerReadableDomainCandidateScan - len(candidates) + 1
		var grants []*store.AccessGrantEntry
		var grantsErr error
		if bounded, boundedOK := s.agentStore.(store.BoundedAccessGrantReader); boundedOK {
			grants, grantsErr = bounded.GetActiveGrantsBounded(queryCtx, agentID, grantLimit)
		} else {
			grants, grantsErr = accessStore.GetActiveGrants(queryCtx, agentID)
		}
		if grantsErr != nil {
			truncated = true
		} else {
			if len(grants) >= grantLimit {
				truncated = true
			}
			for i, grant := range grants {
				if i >= grantLimit-1 {
					break
				}
				if grant == nil || grant.Level < 1 ||
					(grant.ExpiresAt != nil && !grant.ExpiresAt.After(now)) {
					continue
				}
				add(grant.Domain)
				if len(candidates) >= callerReadableDomainCandidateScan {
					truncated = true
					break
				}
			}
		}
	} else {
		truncated = true
	}
	if groups, groupsErr := s.badgerStore.ListAppV23AgentGroups(agentID); groupsErr == nil {
		for _, group := range groups {
			for _, memberID := range group.Members {
				member, memberErr := s.badgerStore.GetAppV23Enrollment(memberID)
				if memberErr != nil {
					truncated = true
					continue
				}
				if member != nil && member.Active {
					add(member.HomeDomain)
				}
			}
		}
	} else {
		truncated = true
	}

	// Authored domains are particularly valuable after an upgrade: app-v25
	// continuity makes previously valid writers readable again. SQL is only a
	// candidate source and has its own short deadline; policy remains Badger.
	if s.agentStore != nil && queryCtx.Err() == nil && len(candidates) < callerReadableDomainCandidateScan {
		domainLimit := callerReadableDomainCandidateScan - len(candidates) + 1
		var domains []string
		var domainsErr error
		if bounded, ok := s.agentStore.(store.BoundedAgentDomainReader); ok {
			domains, domainsErr = bounded.ListAgentDomainsBounded(queryCtx, agentID, domainLimit)
		} else {
			domains, domainsErr = s.agentStore.ListAgentDomains(queryCtx, agentID)
		}
		if domainsErr != nil {
			truncated = true
		} else {
			for i, domain := range domains {
				if i >= domainLimit-1 || len(candidates) >= callerReadableDomainCandidateScan {
					truncated = true
					break
				}
				add(domain)
			}
		}
	}

	ordered := make([]string, 0, len(candidates))
	if _, hasHome := candidates[enrollment.HomeDomain]; hasHome {
		ordered = append(ordered, enrollment.HomeDomain)
	}
	remaining := make([]string, 0, len(candidates))
	for domain := range candidates {
		if domain != enrollment.HomeDomain {
			remaining = append(remaining, domain)
		}
	}
	sort.Strings(remaining)
	ordered = append(ordered, remaining...)
	readable := make([]string, 0, min(len(ordered), callerReadableDomainLimit))
	for _, domain := range ordered {
		allowed, accessErr := s.hasMemoryReadAccess(domain, agentID, 0, now)
		if accessErr != nil {
			truncated = true
			continue
		}
		if !allowed {
			continue
		}
		if len(readable) == callerReadableDomainLimit {
			truncated = true
			break
		}
		readable = append(readable, domain)
	}

	writeJSON(w, http.StatusOK, callerReadableDomainsResponse{
		Domains: readable, Truncated: truncated, Scope: "bounded_policy_and_provenance",
	})
}
