package web

import (
	"context"
	"net/http"
	"strings"

	"github.com/l33tdawg/sage/internal/store"
)

// CEREBRUM is a user knowledge surface, not a raw consensus-table browser.
// Group-removal journal anchors are ordinary committed MemorySubmit records so
// they remain independently auditable and survive snapshots/backups, but their
// namespace is protocol bookkeeping and must not become a user domain.
const cerebrumInternalSyncAuditPrefix = store.SyncAuditDomainPrefix

var cerebrumInternalDomainPrefixes = []string{
	cerebrumInternalSyncAuditPrefix,
}

func isCerebrumInternalMemoryDomain(domain string) bool {
	normalized := strings.ToLower(strings.TrimSpace(domain))
	for _, prefix := range cerebrumInternalDomainPrefixes {
		if strings.HasPrefix(normalized, prefix) {
			return true
		}
	}
	return false
}

func cerebrumListOptions(opts store.ListOptions) store.ListOptions {
	opts.ExcludeDomainPrefixes = append(
		append([]string(nil), opts.ExcludeDomainPrefixes...),
		cerebrumInternalDomainPrefixes...,
	)
	return opts
}

func cerebrumQueryOptions(opts store.QueryOptions) store.QueryOptions {
	opts.ExcludeDomainPrefixes = append(
		append([]string(nil), opts.ExcludeDomainPrefixes...),
		cerebrumInternalDomainPrefixes...,
	)
	return opts
}

// cerebrumVisibleStats returns a presentation copy whose headline totals and
// domain catalogue omit protocol-owned namespaces. The underlying StoreStats is
// never mutated because callers may share/cache it.
func (h *DashboardHandler) cerebrumVisibleStats(ctx context.Context) (*store.StoreStats, error) {
	if provider, ok := h.store.(interface {
		GetStatsExcludingDomainPrefixes(context.Context, []string) (*store.StoreStats, error)
	}); ok {
		return provider.GetStatsExcludingDomainPrefixes(ctx, cerebrumInternalDomainPrefixes)
	}
	stats, err := h.store.GetStats(ctx)
	if err != nil || stats == nil {
		return stats, err
	}
	visible := &store.StoreStats{
		TotalMemories: stats.TotalMemories,
		ByDomain:      make(map[string]int, len(stats.ByDomain)),
		ByStatus:      make(map[string]int, len(stats.ByStatus)),
		ByAgent:       make(map[string]int, len(stats.ByAgent)),
		DBSizeBytes:   stats.DBSizeBytes,
		LastActivity:  stats.LastActivity,
	}
	for status, count := range stats.ByStatus {
		visible.ByStatus[status] = count
	}
	for agent, count := range stats.ByAgent {
		visible.ByAgent[agent] = count
	}
	for domain, count := range stats.ByDomain {
		if isCerebrumInternalMemoryDomain(domain) {
			visible.TotalMemories -= count
			continue
		}
		visible.ByDomain[domain] = count
	}
	if visible.TotalMemories < 0 {
		visible.TotalMemories = 0
	}
	return visible, nil
}

func (h *DashboardHandler) rejectInternalCerebrumMemory(w http.ResponseWriter, ctx context.Context, memoryID string) bool {
	record, err := h.store.GetMemory(ctx, memoryID)
	if err != nil || record == nil || !isCerebrumInternalMemoryDomain(record.DomainTag) {
		return false
	}
	writeError(w, http.StatusNotFound, "memory not found")
	return true
}
