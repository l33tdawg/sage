package store

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"
	"time"
)

// Inbox cursors order equal-timestamp rows by their immutable ID. They are
// client-held positions, not claims, snapshots, or authorization credentials.
func EncodeInboxCursor(createdAt time.Time, id string) string {
	return base64.RawURLEncoding.EncodeToString([]byte(formatTime(createdAt) + "|" + id))
}

func DecodeInboxCursor(cursor string) (string, string, error) {
	if cursor == "" {
		return "", "", nil
	}
	if len(cursor) > 1024 {
		return "", "", errors.New("invalid inbox cursor")
	}
	decoded, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return "", "", errors.New("invalid inbox cursor")
	}
	createdAt, id, ok := strings.Cut(string(decoded), "|")
	timestamp, err := time.Parse(time.RFC3339Nano, createdAt)
	if !ok || err != nil || id == "" || len(id) > 256 {
		return "", "", errors.New("invalid inbox cursor")
	}
	return formatTime(timestamp), id, nil
}

// MessageInboxStore separates passive inspection from explicit exact claims.
type MessageInboxStore interface {
	GetPendingInboxPage(context.Context, string, string, int, string) ([]*PipelineMessage, string, error)
	InspectClaimableMessage(context.Context, string, string, string, string) (*PipelineMessage, error)
	ClaimInboxMessage(context.Context, string, string, string, string) (*PipelineMessage, bool, error)
}

func (s *SQLiteStore) GetPendingInboxPage(ctx context.Context, agentID, provider string, limit int, cursor string) ([]*PipelineMessage, string, error) {
	if agentID == "" || limit < 1 || limit > 20 {
		return nil, "", errors.New("invalid inbox request")
	}
	afterTime, afterID, err := DecodeInboxCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT pipe_id,from_agent,from_provider,to_agent,to_provider,intent,payload,status,created_at,expires_at,
        source_chain_id,source_pipe_id,destination_chain_id,federation_policy_epoch,federation_agreement_id,federation_contact_id,federation_contact_revision,
        federation_authorization_mode,federation_linked_relation
        FROM pipeline_messages WHERE status='pending' AND destination_chain_id=''
        AND (to_agent=? OR (?!='' AND to_agent='' AND to_provider=?))
        AND expires_at>strftime('%Y-%m-%dT%H:%M:%fZ','now')
        AND (?='' OR created_at>? OR (created_at=? AND pipe_id>?))
        ORDER BY created_at,pipe_id LIMIT ?`, agentID, provider, provider, afterTime, afterTime, afterTime, afterID, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]*PipelineMessage, 0, limit+1)
	for rows.Next() {
		var m PipelineMessage
		var createdAt, expiresAt string
		if err := rows.Scan(&m.PipeID, &m.FromAgent, &m.FromProvider, &m.ToAgent, &m.ToProvider, &m.Intent, &m.Payload, &m.Status, &createdAt, &expiresAt,
			&m.SourceChainID, &m.SourcePipeID, &m.DestinationChainID, &m.FederationPolicyEpoch, &m.FederationAgreementID, &m.FederationContactID, &m.FederationContactRevision,
			&m.FederationAuthorizationMode, &m.FederationLinkedRelation); err != nil {
			return nil, "", err
		}
		m.CreatedAt, m.ExpiresAt = parseTime(createdAt), parseTime(expiresAt)
		if err := s.decryptPipelineFields(&m); err != nil {
			return nil, "", err
		}
		items = append(items, &m)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		next = EncodeInboxCursor(last.CreatedAt, last.PipeID)
	}
	return items, next, nil
}

// InspectClaimableMessage exposes a pending request or work already owned by
// this exact session. Sibling-session claims never disclose payload here.
func (s *SQLiteStore) InspectClaimableMessage(ctx context.Context, agentID, provider, messageID, sessionID string) (*PipelineMessage, error) {
	m, err := s.GetPipeline(ctx, messageID)
	if err != nil {
		return nil, ErrMessageNotFound
	}
	if agentID == "" || m.DestinationChainID != "" || !m.ExpiresAt.After(time.Now().UTC()) ||
		(m.ToAgent != agentID && !(m.ToAgent == "" && provider != "" && m.ToProvider == provider)) {
		return nil, ErrMessageNotFound
	}
	if m.Status == "pending" {
		return m, nil
	}
	if m.Status != "claimed" {
		return nil, ErrMessageNotFound
	}
	if m.ClaimedBy != agentID {
		return nil, ErrMessageClaimedByOtherSession
	}
	if err := s.conn.QueryRowContext(ctx, `SELECT claimant_session_id,claim_revision FROM message_fetch_receipts
        WHERE message_id=? AND receiver_agent_id=?`, messageID, agentID).Scan(&m.ClaimedSessionID, &m.ClaimRevision); err != nil {
		return nil, ErrMessageNotFound
	}
	if sessionID == "" || m.ClaimedSessionID != sessionID {
		return nil, ErrMessageClaimedByOtherSession
	}
	return m, nil
}

// ClaimInboxMessage claims only the named row, atomically with the session
// fence. Retrying the same claim is safe; another session cannot take it.
func (s *SQLiteStore) ClaimInboxMessage(ctx context.Context, agentID, provider, messageID, sessionID string) (*PipelineMessage, bool, error) {
	if sessionID == "" || len(sessionID) > MaxMessageClaimantSessionBytes {
		return nil, false, ErrMessageNotFound
	}
	var out *PipelineMessage
	var replayed bool
	err := s.RunInTx(ctx, func(offchain OffchainStore) error {
		tx := offchain.(*SQLiteStore)
		var err error
		out, err = tx.InspectClaimableMessage(ctx, agentID, provider, messageID, sessionID)
		if err != nil {
			return err
		}
		if out.Status == "claimed" {
			replayed = true
			return nil
		}
		if err := tx.ClaimPipeline(ctx, messageID, agentID); err != nil {
			return err
		}
		if _, err := tx.writeExecContext(ctx, `INSERT INTO message_fetch_receipts
            (message_id,receiver_agent_id,claimant_session_id,claim_revision) VALUES(?,?,?,0)`, messageID, agentID, sessionID); err != nil {
			return err
		}
		out.Status, out.ClaimedBy, out.ClaimedSessionID = "claimed", agentID, sessionID
		now := time.Now().UTC()
		out.ClaimedAt = &now
		return nil
	})
	if err != nil {
		return nil, false, fmt.Errorf("claim inbox message: %w", err)
	}
	return out, replayed, nil
}
