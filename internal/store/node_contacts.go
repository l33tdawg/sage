package store

import "context"

// ListNodeContactCandidates is a bounded keyset page of metadata. Authorization
// is evaluated from current canonical state after loading this candidate page.
func (s *SQLiteStore) ListNodeContactCandidates(ctx context.Context, after string, limit int) ([]*AgentEntry, error) {
	if limit < 1 || limit > 129 {
		return nil, nil
	}
	rows, err := s.conn.QueryContext(ctx, `
		SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,''), status, removed_at
		FROM network_agents
		WHERE status='active' AND removed_at IS NULL AND length(agent_id)=64
		AND agent_id NOT GLOB '*[^0-9a-f]*' AND agent_id > ?
		ORDER BY agent_id LIMIT ?`, after, limit)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()
	return scanPipeContactLookupAgents(rows)
}
