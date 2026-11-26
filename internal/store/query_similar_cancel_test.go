package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// cancelAfterVectorQuery cancels the caller's context once the vector-scan
// query has returned its rows, and waits for database/sql to close them, so
// the scan loop sees a cancelled iterator.
type cancelAfterVectorQuery struct {
	sqlQuerier
	cancel context.CancelFunc
}

func (c cancelAfterVectorQuery) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	rows, err := c.sqlQuerier.QueryContext(ctx, query, args...)
	if err == nil && strings.Contains(query, "FROM memories WHERE embedding IS NOT NULL") {
		c.cancel()
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond) // let database/sql close the rows
	}
	return rows, err
}

// A scan cut short by cancellation used to be ranked and returned as a
// complete (here: empty) result with a nil error.
func TestQuerySimilar_CancelledScanReturnsError(t *testing.T) {
	s := newTestStore(t)
	bg := context.Background()
	for i := 0; i < 20; i++ {
		rec := testMemory(fmt.Sprintf("c%d", i), "agent1", fmt.Sprintf("row %d", i), "general")
		rec.Embedding = []float32{0.5, 0.5, 0.5}
		require.NoError(t, s.InsertMemory(bg, rec))
	}

	ctx, cancel := context.WithCancel(bg)
	defer cancel()
	orig := s.conn
	s.conn = cancelAfterVectorQuery{sqlQuerier: orig, cancel: cancel}
	defer func() { s.conn = orig }()

	got, err := s.QuerySimilar(ctx, []float32{0.5, 0.5, 0.5}, QueryOptions{TopK: 10})
	require.Error(t, err)
	require.True(t, errors.Is(err, context.Canceled), "got %v", err)
	require.Nil(t, got)
}
