package store

import (
	"context"
	"fmt"
	"strings"
)

// AgentPresentation is the metadata-only projection needed to LABEL an agent in
// a response: who to show, never what they own.
//
// It exists because GetAgent is the wrong tool for that job. GetAgent computes
// MemoryCount and LastCommittedMemoryAt through two correlated subqueries over
// the memories table, and a caller that only wants a display name pays both and
// throws the results away — once per agent, on surfaces that render dozens of
// counterparties per page.
type AgentPresentation struct {
	AgentID        string
	DisplayName    string
	RegisteredName string
	Provider       string
}

// GetAgentPresentations resolves labels for many agents in ONE query.
//
// Deliberately NOT filtered by status. The caller is labelling rows it has
// already authorized and returned; dropping a removed agent here would blank
// the attribution on a message the recipient can still legitimately read,
// which trades a presentation nicety for hidden provenance. Whether a removed
// agent should be MARKED as such is a display decision for the caller, not a
// reason to withhold the name.
//
// Missing IDs are simply absent from the map rather than erroring, so a caller
// enriching a page never loses a row because one counterparty is unknown.
func (s *SQLiteStore) GetAgentPresentations(
	ctx context.Context, agentIDs []string,
) (map[string]AgentPresentation, error) {
	out := make(map[string]AgentPresentation, len(agentIDs))
	unique := make([]any, 0, len(agentIDs))
	seen := make(map[string]struct{}, len(agentIDs))
	for _, id := range agentIDs {
		if id == "" {
			continue
		}
		if _, dup := seen[id]; dup {
			continue
		}
		seen[id] = struct{}{}
		unique = append(unique, id)
	}
	if len(unique) == 0 {
		return out, nil
	}

	// Chunked to stay under SQLite's variable limit, the same way
	// GetCorroborationCounts does for the identical reason.
	const chunkSize = 900
	for start := 0; start < len(unique); start += chunkSize {
		end := start + chunkSize
		if end > len(unique) {
			end = len(unique)
		}
		chunk := unique[start:end]
		placeholders := make([]string, len(chunk))
		for i := range chunk {
			placeholders[i] = "?"
		}
		rows, err := s.conn.QueryContext(ctx, `
			SELECT agent_id, name, COALESCE(registered_name,''), COALESCE(provider,'')
			FROM network_agents
			WHERE agent_id IN (`+strings.Join(placeholders, ",")+`)`, chunk...)
		if err != nil {
			return nil, fmt.Errorf("get agent presentations: %w", err)
		}
		for rows.Next() {
			var p AgentPresentation
			if scanErr := rows.Scan(&p.AgentID, &p.DisplayName, &p.RegisteredName, &p.Provider); scanErr != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scan agent presentation: %w", scanErr)
			}
			// Mirror GetAgent's backfill so both paths agree: a row predating
			// the registered_name column carries '' and falls back to the
			// display name rather than presenting an empty registered name.
			if p.RegisteredName == "" {
				p.RegisteredName = p.DisplayName
			}
			out[p.AgentID] = p
		}
		if rowsErr := rows.Err(); rowsErr != nil {
			_ = rows.Close()
			return nil, fmt.Errorf("iterate agent presentations: %w", rowsErr)
		}
		_ = rows.Close()
	}
	return out, nil
}
