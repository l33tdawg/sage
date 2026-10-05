package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/require"
	"path/filepath"
	"testing"
	"time"
)

func TestFederationActivityContentFreeBoundedRead(t *testing.T) {
	ctx := context.Background()
	s, err := NewSQLiteStore(ctx, filepath.Join(t.TempDir(), "activity.db"))
	require.NoError(t, err)
	defer s.Close()
	now := time.Now().UTC().Format(time.RFC3339)
	for i := 0; i < 105; i++ {
		_, err = s.writeExecContext(ctx, `INSERT INTO pipeline_messages(pipe_id,from_agent,to_agent,payload,intent,result,status,created_at,expires_at,source_chain_id) VALUES(?,?,?,'SECRET payload','SECRET intent','SECRET result','pending',?,?,'peer')`, fmt.Sprintf("in-%03d", i), "source", "target", now, now)
		require.NoError(t, err)
	}
	items, err := s.RecentFederationActivity(ctx)
	require.NoError(t, err)
	require.Len(t, items, 100)
	raw, err := json.Marshal(items)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "SECRET")
	require.NotContains(t, string(raw), "payload")
	require.Equal(t, "received", items[0].State)
	var count int
	require.NoError(t, s.conn.QueryRowContext(ctx, `SELECT count(*) FROM pipeline_messages WHERE status='pending' AND claimed_by=''`).Scan(&count))
	require.Equal(t, 105, count)
	_, err = s.writeExecContext(ctx, `DELETE FROM pipeline_messages`)
	require.NoError(t, err)
	for _, state := range []string{"pending", "delivered", "failed"} {
		_, err = s.writeExecContext(ctx, `INSERT INTO pipeline_transport_outbox(event_id,pipe_id,remote_chain_id,event_kind,policy_epoch,agreement_id,contact_id,contact_revision,source_agent_id,target_agent_id,proof_signature,proof_timestamp,proof_canonical,state,next_attempt_at,created_at,expires_at,last_error) VALUES(?,?,'peer','result','epoch','agreement','contact','revision','source','target',x'00',1,'SECRET proof',?,?,?,?, 'SECRET error')`, state, state, state, now, now, now)
		require.NoError(t, err)
	}
	items, err = s.RecentFederationActivity(ctx)
	require.NoError(t, err)
	require.Len(t, items, 3)
	raw, err = json.Marshal(items)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "SECRET")
	require.Equal(t, "result", items[0].Kind)
	old := time.Now().UTC().Add(-72 * time.Hour).Format(time.RFC3339)
	_, err = s.writeExecContext(ctx, `UPDATE pipeline_transport_outbox SET created_at=?,delivered_at=? WHERE state='delivered'`, old, now)
	require.NoError(t, err)
	_, err = s.writeExecContext(ctx, `INSERT INTO pipeline_messages(pipe_id,from_agent,to_agent,payload,result,status,created_at,completed_at,expires_at,destination_chain_id) VALUES('remote-reply','local-agent','remote-agent','SECRET','SECRET','completed',?,?,?,'peer')`, old, now, now)
	require.NoError(t, err)
	items, err = s.RecentFederationActivity(ctx)
	require.NoError(t, err)
	require.Len(t, items, 4)
	var incoming *FederationActivity
	for i := range items {
		if items[i].ID == "reply:remote-reply" {
			incoming = &items[i]
		}
	}
	require.NotNil(t, incoming)
	require.Equal(t, "remote-agent", incoming.Source)
	require.Equal(t, "local-agent", incoming.Target)
	require.Equal(t, "inbound", incoming.Direction)
	require.Equal(t, "result", incoming.Kind)

}
