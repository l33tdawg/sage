package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/vault"
	"github.com/stretchr/testify/require"
)

func TestSQLiteDurableCommitPragmasAcrossConnectionsAndReopen(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "durability.db")
	for attempt := 0; attempt < 2; attempt++ {
		database, err := NewSQLiteStore(ctx, path)
		require.NoError(t, err)
		connections := make([]*sql.Conn, 0, 3)
		for index := 0; index < 3; index++ {
			connection, err := database.db.Conn(ctx)
			require.NoError(t, err)
			connections = append(connections, connection)
			var synchronous int
			var mode string
			require.NoError(t, connection.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&synchronous))
			require.Equal(t, 2, synchronous)
			require.NoError(t, connection.QueryRowContext(ctx, "PRAGMA journal_mode").Scan(&mode))
			require.Equal(t, "wal", mode)
		}
		for _, connection := range connections {
			require.NoError(t, connection.Close())
		}
		require.NoError(t, database.Close())
	}
}

func BenchmarkWorkflowJournalDurableCommit(benchmark *testing.B) {
	ctx := context.Background()
	directory := benchmark.TempDir()
	database, err := NewSQLiteStore(ctx, filepath.Join(directory, "synthetic.db"))
	require.NoError(benchmark, err)
	benchmark.Cleanup(func() { _ = database.Close() })
	keyPath := filepath.Join(directory, "synthetic.key")
	require.NoError(benchmark, vault.Init(keyPath, "synthetic-benchmark-only"))
	opened, err := vault.Open(keyPath, "synthetic-benchmark-only")
	require.NoError(benchmark, err)
	database.SetVaultExpected(true)
	database.SetVault(opened)
	const identifier = "5159388c-fbc2-4f3c-9ac0-831dd30e1da9"
	const actor = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	samples := make([]time.Duration, 0, 1000)
	benchmark.ResetTimer()
	for index := 0; index < benchmark.N; index++ {
		started := time.Now()
		_, err := database.PutWorkflowJournal(ctx, actor, identifier, "mesh_outbound", json.RawMessage(`{"synthetic":true}`), int64(index))
		require.NoError(benchmark, err)
		if len(samples) < cap(samples) {
			samples = append(samples, time.Since(started))
		}
	}
	benchmark.StopTimer()
	sort.Slice(samples, func(first, second int) bool { return samples[first] < samples[second] })
	if len(samples) > 0 {
		benchmark.ReportMetric(float64(samples[(len(samples)-1)*50/100].Nanoseconds())/1e6, "p50-ms")
		benchmark.ReportMetric(float64(samples[(len(samples)-1)*95/100].Nanoseconds())/1e6, "p95-ms")
	}
}
