package store

import (
	"context"
	"regexp"
	"testing"
	"time"

	"github.com/pashagolub/pgxmock/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPostgresRedeemAgentClaimSuccess(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(postgresRedeemAgentClaimSQL)).
		WithArgs("valid-token", now).
		WillReturnRows(pgxmock.NewRows([]string{"agent_id"}).AddRow("agent-1"))

	agentID, err := s.RedeemAgentClaim(context.Background(), "valid-token", now)
	require.NoError(t, err)
	assert.Equal(t, "agent-1", agentID)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRedeemAgentClaimInvalid(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(postgresRedeemAgentClaimSQL)).
		WithArgs("invalid-token", now).
		WillReturnRows(pgxmock.NewRows([]string{"agent_id"}))
	mock.ExpectExec(regexp.QuoteMeta(postgresExpireAgentClaimSQL)).
		WithArgs("invalid-token", now).
		WillReturnResult(pgxmock.NewResult("UPDATE", 0))

	_, err = s.RedeemAgentClaim(context.Background(), "invalid-token", now)
	require.ErrorIs(t, err, ErrAgentClaimInvalid)
	require.NoError(t, mock.ExpectationsWereMet())
}

func TestPostgresRedeemAgentClaimExpired(t *testing.T) {
	mock, err := pgxmock.NewPool()
	require.NoError(t, err)
	t.Cleanup(mock.Close)
	s := &PostgresStore{db: mock}
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(postgresRedeemAgentClaimSQL)).
		WithArgs("expired-token", now).
		WillReturnRows(pgxmock.NewRows([]string{"agent_id"}))
	mock.ExpectExec(regexp.QuoteMeta(postgresExpireAgentClaimSQL)).
		WithArgs("expired-token", now).
		WillReturnResult(pgxmock.NewResult("UPDATE", 1))

	_, err = s.RedeemAgentClaim(context.Background(), "expired-token", now)
	require.ErrorIs(t, err, ErrAgentClaimExpired)
	require.NoError(t, mock.ExpectationsWereMet())
}
