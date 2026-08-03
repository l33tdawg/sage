package store

import (
	"context"
	"regexp"
	"testing"

	"github.com/pashagolub/pgxmock/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresFindAgentsByNameUsesBoundedEscapedActiveLookup(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	exact := `mynah-Ü\%\_\\`
	pattern := "%" + exact + "%"
	rows := pgxmock.NewRows([]string{
		"agent_id", "name", "registered_name", "provider", "status", "removed_at",
	}).
		AddRow("exact", "MYNAH-Ü%_\\", "", "mynah-appliance", "active", nil).
		AddRow("partial", "MYNAH (MYNAH-Ü%_\\ bridge)", "registered-bridge", "mynah-app", "active", nil)

	mock.ExpectQuery(regexp.QuoteMeta(postgresFindAgentsByNameSQL)).
		WithArgs(pattern, exact, maxAgentNameLookupResults, 0).
		WillReturnRows(rows)

	agents, err := s.FindAgentsByName(context.Background(), "  MYNAH-Ü%_\\  ", 200)
	require.NoError(t, err)
	require.Len(t, agents, 2)
	assert.Equal(t, "exact", agents[0].AgentID)
	assert.Equal(t, "MYNAH-Ü%_\\", agents[0].RegisteredName, "legacy empty registered name must backfill")
	assert.Equal(t, "partial", agents[1].AgentID)
	assert.Equal(t, "active", agents[1].Status)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresFindAgentsByNamePageUsesBoundedOffset(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	rows := pgxmock.NewRows([]string{
		"agent_id", "name", "registered_name", "provider", "status", "removed_at",
	}).AddRow("canonical", "claude-code/sage", "", "claude-code", "active", nil)
	mock.ExpectQuery(regexp.QuoteMeta(postgresFindAgentsByNameSQL)).
		WithArgs("%claude%", "claude", maxAgentNameLookupResults, 40).
		WillReturnRows(rows)

	agents, err := s.FindAgentsByNamePage(context.Background(), "claude", 200, 40)
	require.NoError(t, err)
	require.Len(t, agents, 1)
	assert.Equal(t, "canonical", agents[0].AgentID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresAgentLookupCandidatesUseOneBoundedQuery(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}

	agentID := "9d83019c5e41d9fcf91eee16c2a3675956a0ada4cc525d7810e24ccb65c64e59"
	rows := pgxmock.NewRows([]string{
		"agent_id", "name", "registered_name", "provider", "status", "removed_at",
	}).AddRow(agentID, "codex/singsearch", "codex/singsearch", "codex", "active", nil)
	mock.ExpectQuery(regexp.QuoteMeta(postgresFindAgentsByNameSQL)).
		WithArgs("%"+agentID+"%", agentID, maxAgentNameLookupCandidates, 0).
		WillReturnRows(rows)

	agents, err := s.FindAgentLookupCandidates(
		context.Background(), agentID, maxAgentNameLookupCandidates+1,
	)
	require.NoError(t, err)
	require.Len(t, agents, 1)
	assert.Equal(t, agentID, agents[0].AgentID)
	require.NoError(t, mock.ExpectationsWereMet(),
		"one logical exact-ID lookup must issue one bounded SQL query")
}

func TestPostgresFindAgentsByNameRejectsEmptyOrNonPositiveLookup(t *testing.T) {
	s := &PostgresStore{}

	for _, tc := range []struct {
		query string
		limit int
	}{
		{query: "", limit: 20},
		{query: "   ", limit: 20},
		{query: "mynah", limit: 0},
		{query: "mynah", limit: -1},
	} {
		agents, err := s.FindAgentsByName(context.Background(), tc.query, tc.limit)
		require.NoError(t, err)
		assert.Empty(t, agents)
	}
}
