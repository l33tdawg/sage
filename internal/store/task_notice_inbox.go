package store

import (
	"context"
	"errors"
	"time"
)

var ErrTaskNoticeNotFound = errors.New("task notice not found")

// TaskNoticeInboxStore supplies passive keyset paging and exact-owner lookup.
// Acknowledgement remains an explicit write through TaskAssignmentStore.
type TaskNoticeInboxStore interface {
	PeekAgentNotificationPage(context.Context, string, int, string) ([]*AgentNotification, string, error)
	GetAgentNotification(context.Context, string, string) (*AgentNotification, error)
}

func (s *SQLiteStore) PeekAgentNotificationPage(ctx context.Context, agentID string, limit int, cursor string) ([]*AgentNotification, string, error) {
	if agentID == "" || limit < 1 || limit > 20 {
		return nil, "", errors.New("invalid notice page")
	}
	afterTime, afterID, err := DecodeInboxCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	rows, err := s.conn.QueryContext(ctx, `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.agent_id=? AND n.state='unread' AND m.memory_type='task' AND m.status!='deprecated'
        AND m.task_status IN ('planned','in_progress') AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version
        AND (?='' OR n.created_at>? OR (n.created_at=? AND n.notification_id>?))
        ORDER BY n.created_at,n.notification_id LIMIT ?`, agentID, afterTime, afterTime, afterTime, afterID, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]*AgentNotification, 0, limit+1)
	for rows.Next() {
		var n AgentNotification
		var createdAt string
		if err := rows.Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID, &n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &createdAt); err != nil {
			return nil, "", err
		}
		n.CreatedAt = parseTime(createdAt)
		items = append(items, &n)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return noticePage(items, limit)
}

func (s *PostgresStore) PeekAgentNotificationPage(ctx context.Context, agentID string, limit int, cursor string) ([]*AgentNotification, string, error) {
	if agentID == "" || limit < 1 || limit > 20 {
		return nil, "", errors.New("invalid notice page")
	}
	afterTime, afterID, err := DecodeInboxCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	after := time.Time{}
	if afterTime != "" {
		after, _ = time.Parse(time.RFC3339Nano, afterTime)
	}
	rows, err := s.db.Query(ctx, `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.agent_id=$1 AND n.state='unread' AND m.memory_type='task' AND m.status!='deprecated'
        AND m.task_status IN ('planned','in_progress') AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version
        AND (n.created_at>$2 OR (n.created_at=$2 AND n.notification_id>$3))
        ORDER BY n.created_at,n.notification_id LIMIT $4`, agentID, after, afterID, limit+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	items := make([]*AgentNotification, 0, limit+1)
	for rows.Next() {
		var n AgentNotification
		if err := rows.Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID, &n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &n.CreatedAt); err != nil {
			return nil, "", err
		}
		items = append(items, &n)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	return noticePage(items, limit)
}

func noticePage(items []*AgentNotification, limit int) ([]*AgentNotification, string, error) {
	next := ""
	if len(items) > limit {
		items = items[:limit]
		last := items[len(items)-1]
		next = EncodeInboxCursor(last.CreatedAt, last.NotificationID)
	}
	return items, next, nil
}

func (s *SQLiteStore) GetAgentNotification(ctx context.Context, agentID, id string) (*AgentNotification, error) {
	var n AgentNotification
	var createdAt string
	err := s.conn.QueryRowContext(ctx, `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.notification_id=? AND n.agent_id=? AND n.state IN ('unread','read')
        AND m.memory_type='task' AND m.status!='deprecated' AND m.task_status IN ('planned','in_progress')
        AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version`, id, agentID).
		Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID, &n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &createdAt)
	if err != nil {
		return nil, ErrTaskNoticeNotFound
	}
	n.CreatedAt = parseTime(createdAt)
	return &n, nil
}

func (s *PostgresStore) GetAgentNotification(ctx context.Context, agentID, id string) (*AgentNotification, error) {
	var n AgentNotification
	err := s.db.QueryRow(ctx, `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.notification_id=$1 AND n.agent_id=$2 AND n.state IN ('unread','read')
        AND m.memory_type='task' AND m.status!='deprecated' AND m.task_status IN ('planned','in_progress')
        AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version`, id, agentID).
		Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID, &n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &n.CreatedAt)
	if err != nil {
		return nil, ErrTaskNoticeNotFound
	}
	return &n, nil
}
