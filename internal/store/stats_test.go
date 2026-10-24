package store

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
)

type statsLifecycleReader interface {
	GetStats(context.Context) (*StoreStats, error)
	GetStatsExcludingDomainPrefixes(context.Context, []string) (*StoreStats, error)
}

func statsLifecycleRecords() []*memory.MemoryRecord {
	fixtures := []struct {
		domain string
		status memory.MemoryStatus
	}{
		{"general", memory.StatusProposed},
		{"general", memory.StatusValidated},
		{"security", memory.StatusCommitted},
		{"security", memory.StatusChallenged},
		{"general", memory.StatusDeprecated},
		{"archive", memory.StatusDeprecated},
		{"SAGE-SYNCAUDIT-GRP-ABC", memory.StatusCommitted},
		{"sage-internal-cache", memory.StatusProposed},
		{"SAGE-SYNCAUDIT-GRP-ABC", memory.StatusDeprecated},
	}
	base := time.Date(2026, time.October, 1, 12, 0, 0, 0, time.UTC)
	records := make([]*memory.MemoryRecord, 0, len(fixtures))
	for i, fixture := range fixtures {
		record := testMemory(fmt.Sprintf("stats-%d", i), "stats-agent", "stats content", fixture.domain)
		record.Status = fixture.status
		record.CreatedAt = base.Add(time.Duration(i) * time.Hour)
		records = append(records, record)
	}
	return records
}

// Both SQL backends must exclude deprecated records from active totals and
// domain counts while retaining their status counts and last activity for audit.
func assertStatsLifecycleCounts(t *testing.T, reader statsLifecycleReader) {
	t.Helper()
	records := statsLifecycleRecords()
	all := &StoreStats{
		TotalMemories: 6,
		ByDomain: map[string]int{
			"general": 2, "security": 2, "SAGE-SYNCAUDIT-GRP-ABC": 1, "sage-internal-cache": 1,
		},
		ByStatus:     map[string]int{"proposed": 2, "validated": 1, "committed": 2, "challenged": 1, "deprecated": 3},
		LastActivity: &records[8].CreatedAt,
	}
	filtered := &StoreStats{
		TotalMemories: 4,
		ByDomain:      map[string]int{"general": 2, "security": 2},
		ByStatus:      map[string]int{"proposed": 1, "validated": 1, "committed": 1, "challenged": 1, "deprecated": 2},
		LastActivity:  &records[5].CreatedAt,
	}
	auditOnly := &StoreStats{
		ByDomain: map[string]int{}, ByStatus: map[string]int{"deprecated": 1},
		LastActivity: &records[5].CreatedAt,
	}
	empty := &StoreStats{ByDomain: map[string]int{}, ByStatus: map[string]int{}}

	for _, tc := range []struct {
		name     string
		prefixes []string
		want     *StoreStats
	}{
		{"all", nil, all},
		{"no exclusions", []string{}, all},
		{"blank exclusions", []string{"", ""}, all},
		{"multiple case-insensitive exclusions", []string{"", "SAGE-SYNCAUDIT-", "sage-INTERNAL-"}, filtered},
		{"deprecated-only domain", []string{"general", "security", "sage-"}, auditOnly},
		{"all domains excluded", []string{"general", "security", "sage-", "archive"}, empty},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var got *StoreStats
			var err error
			if tc.prefixes == nil {
				got, err = reader.GetStats(context.Background())
			} else {
				got, err = reader.GetStatsExcludingDomainPrefixes(context.Background(), tc.prefixes)
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want.TotalMemories, got.TotalMemories)
			assert.Equal(t, tc.want.ByDomain, got.ByDomain)
			assert.Equal(t, tc.want.ByStatus, got.ByStatus)
			if tc.want.LastActivity == nil {
				assert.Nil(t, got.LastActivity)
			} else {
				require.NotNil(t, got.LastActivity)
				assert.True(t, tc.want.LastActivity.Equal(*got.LastActivity),
					"last activity must include deprecated records within the selected domains")
			}
		})
	}
}

func TestSQLiteStatsLifecycleCounts(t *testing.T) {
	s := newTestStore(t)
	for _, record := range statsLifecycleRecords() {
		require.NoError(t, s.InsertMemory(context.Background(), record))
	}
	assertStatsLifecycleCounts(t, s)
}
