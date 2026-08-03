package rest

import (
	"context"
	"net/http"
	"sort"
	"strconv"
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
	// Domains is the backward-compatible alias for ReadableDomains.
	Domains         []string `json:"domains"`
	OwnedDomains    []string `json:"owned_domains"`
	ReadableDomains []string `json:"readable_domains"`
	WritableDomains []string `json:"writable_domains"`
	Truncated       bool     `json:"truncated"`
	Scope           string   `json:"scope"`
}

type callerOwnedDomainsPageResponse struct {
	Domains    []string `json:"domains"`
	NextCursor string   `json:"next_cursor,omitempty"`
	HasMore    bool     `json:"has_more"`
	Scope      string   `json:"scope"`
}

// handleGetAgentOwnedDomainsPage is the complete, caller-only ownership
// projection. Unlike sage_status's cheap policy sample, this endpoint can be
// paged until has_more=false without scanning memories or a global roster.
func (s *Server) handleGetAgentOwnedDomainsPage(w http.ResponseWriter, r *http.Request) {
	agentID := middleware.ContextAgentID(r.Context())
	if agentID == "" {
		writeProblem(w, http.StatusUnauthorized, "Unauthorized", "No agent ID in context.")
		return
	}
	if !s.requireAppV23ActiveOrdinaryAgent(w, agentID, "owned domain discovery") {
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 1 || parsed > 100 {
			writeProblem(w, http.StatusBadRequest, "Invalid limit", "limit must be between 1 and 100")
			return
		}
		limit = parsed
	}
	cursor := strings.TrimSpace(r.URL.Query().Get("cursor"))
	if cursor != "" {
		if err := store.ValidateAppV23DomainName(cursor); err != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid cursor", "cursor must be an exact domain returned by the previous page")
			return
		}
	}
	domains, hasMore, err := s.badgerStore.ListOwnedDomainsPage(agentID, cursor, limit)
	if err != nil {
		writeProblem(w, http.StatusServiceUnavailable, "Ownership projection unavailable", "The authoritative owned-domain index is not available yet.")
		return
	}
	next := ""
	if hasMore && len(domains) > 0 {
		next = domains[len(domains)-1]
	}
	writeJSON(w, http.StatusOK, callerOwnedDomainsPageResponse{
		Domains: domains, NextCursor: next, HasMore: hasMore,
		Scope: "authoritative_current_owner",
	})
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
				if len(candidates) >= callerReadableDomainCandidateScan {
					truncated = true
					break
				}
				member, memberErr := s.badgerStore.GetAppV23Enrollment(memberID)
				if memberErr != nil {
					truncated = true
					continue
				}
				if member != nil && member.Active {
					add(member.HomeDomain)
				}
			}
			if len(candidates) >= callerReadableDomainCandidateScan {
				break
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
	owned := make([]string, 0, min(len(ordered), callerReadableDomainLimit))
	ownedFromIndex := false
	if indexed, more, indexErr := s.badgerStore.ListOwnedDomainsPage(agentID, "", callerReadableDomainLimit); indexErr == nil {
		owned = indexed
		ownedFromIndex = true
		if more {
			truncated = true
		}
	}
	readable := make([]string, 0, min(len(ordered), callerReadableDomainLimit))
	writable := make([]string, 0, min(len(ordered), callerReadableDomainLimit))
	for _, domain := range ordered {
		owner, _, ownerErr := s.badgerStore.ResolveAppV23OwningAncestor(domain)
		if ownerErr != nil {
			truncated = true
		} else if owner == agentID && !ownedFromIndex {
			if len(owned) < callerReadableDomainLimit {
				owned = append(owned, domain)
			} else {
				truncated = true
			}
		}
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
		} else {
			readable = append(readable, domain)
		}
		if checkAppV23EffectiveWriteAccess(s.badgerStore, agentID, domain, now) == nil {
			if len(writable) < callerReadableDomainLimit {
				writable = append(writable, domain)
			} else {
				truncated = true
			}
		}
	}

	writeJSON(w, http.StatusOK, callerReadableDomainsResponse{
		Domains: readable, OwnedDomains: owned, ReadableDomains: readable,
		WritableDomains: writable, Truncated: truncated,
		Scope: "bounded_policy_and_provenance",
	})
}
