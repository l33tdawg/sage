package store

import (
	"context"
	"time"
)

// FederationActivity is a content-free operator projection, never an agent
// inbox or a source of permission. Proofs, errors and message text are omitted.
type FederationActivity struct {
	ID        string `json:"id"`
	ChainID   string `json:"chain_id"`
	Source    string `json:"source"`
	Target    string `json:"target"`
	Direction string `json:"direction"`
	Kind      string `json:"kind"`
	State     string `json:"state"`
	At        string `json:"at"`
}

// RecentFederationActivity returns a bounded snapshot of locally observed
// transport facts. Reconnection is snapshot reconciliation, not event replay.
func (s *SQLiteStore) RecentFederationActivity(ctx context.Context) ([]FederationActivity, error) {
	cutoff := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339)
	rows, err := s.conn.QueryContext(ctx, `SELECT id,chain_id,source,target,direction,kind,state,at FROM (
 SELECT event_id AS id,remote_chain_id AS chain_id,source_agent_id AS source,target_agent_id AS target,
 'outbound' AS direction,event_kind AS kind,state,COALESCE(delivered_at,created_at) AS at
 FROM pipeline_transport_outbox WHERE event_kind IN ('send','result') AND (created_at >= ? OR delivered_at >= ?)
 UNION ALL
 SELECT pipe_id,source_chain_id,from_agent,to_agent,'inbound','send','received',created_at
 FROM pipeline_messages WHERE source_chain_id != '' AND created_at >= ?
 UNION ALL
 SELECT 'reply:' || pipe_id,destination_chain_id,to_agent,from_agent,'inbound','result','received',completed_at
 FROM pipeline_messages WHERE destination_chain_id != '' AND status = 'completed' AND completed_at >= ?
 ) ORDER BY at DESC,id DESC LIMIT 100`, cutoff, cutoff, cutoff, cutoff)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	out := make([]FederationActivity, 0)
	for rows.Next() {
		var item FederationActivity
		if err := rows.Scan(&item.ID, &item.ChainID, &item.Source, &item.Target, &item.Direction, &item.Kind, &item.State, &item.At); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
