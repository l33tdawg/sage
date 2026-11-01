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

// TaintAwareTaskNoticeInboxStore applies the revision-fenced SQL mirror
// prefilter before paging. Callers must enable it only after checking mirror
// freshness against canonical taint revision; the per-record canonical check
// remains authoritative.
type TaintAwareTaskNoticeInboxStore interface {
	PeekAgentNotificationPageFiltered(context.Context, string, int, string, bool) ([]*AgentNotification, string, error)
	GetAgentNotificationFiltered(context.Context, string, string, bool) (*AgentNotification, error)
}

func (s *SQLiteStore) PeekAgentNotificationPage(ctx context.Context, agentID string, limit int, cursor string) ([]*AgentNotification, string, error) {
	return s.PeekAgentNotificationPageFiltered(ctx, agentID, limit, cursor, false)
}

func (s *SQLiteStore) PeekAgentNotificationPageFiltered(
	ctx context.Context,
	agentID string,
	limit int,
	cursor string,
	excludeTainted bool,
) ([]*AgentNotification, string, error) {
	if agentID == "" || limit < 1 || limit > 20 {
		return nil, "", errors.New("invalid notice page")
	}
	afterTime, afterID, err := DecodeInboxCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	query := `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.agent_id=? AND n.state='unread' AND m.memory_type='task' AND m.status!='deprecated'
	        AND m.task_status IN ('planned','in_progress') AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version`
	if excludeTainted {
		query += ` AND NOT EXISTS (
			SELECT 1 FROM memory_taint_mirror tm WHERE tm.memory_id=m.memory_id
		)`
	}
	query += `
        AND (?='' OR n.created_at>? OR (n.created_at=? AND n.notification_id>?))
	        ORDER BY n.created_at,n.notification_id LIMIT ?`
	rows, err := s.conn.QueryContext(
		ctx,
		query,
		agentID,
		afterTime,
		afterTime,
		afterTime,
		afterID,
		limit+1,
	)
	if err != nil {
		return nil, "", err
	}
	defer func() { _ = rows.Close() }()
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
	return s.PeekAgentNotificationPageFiltered(ctx, agentID, limit, cursor, false)
}

func (s *PostgresStore) PeekAgentNotificationPageFiltered(
	ctx context.Context,
	agentID string,
	limit int,
	cursor string,
	excludeTainted bool,
) ([]*AgentNotification, string, error) {
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
	query := `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.agent_id=$1 AND n.state='unread' AND m.memory_type='task' AND m.status!='deprecated'
	        AND m.task_status IN ('planned','in_progress') AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version`
	if excludeTainted {
		query += ` AND NOT EXISTS (
			SELECT 1 FROM memory_taint_mirror tm WHERE tm.memory_id=m.memory_id
		)`
	}
	query += `
        AND (n.created_at>$2 OR (n.created_at=$2 AND n.notification_id>$3))
	        ORDER BY n.created_at,n.notification_id LIMIT $4`
	rows, err := s.db.Query(ctx, query, agentID, after, afterID, limit+1)
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
	return s.GetAgentNotificationFiltered(ctx, agentID, id, false)
}

func (s *SQLiteStore) GetAgentNotificationFiltered(
	ctx context.Context,
	agentID, id string,
	excludeTainted bool,
) (*AgentNotification, error) {
	var n AgentNotification
	var createdAt string
	query := `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.notification_id=? AND n.agent_id=? AND n.state IN ('unread','read')
        AND m.memory_type='task' AND m.status!='deprecated' AND m.task_status IN ('planned','in_progress')
	        AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version`
	if excludeTainted {
		query += ` AND NOT EXISTS (
			SELECT 1 FROM memory_taint_mirror tm WHERE tm.memory_id=m.memory_id
		)`
	}
	err := s.conn.QueryRowContext(ctx, query, id, agentID).
		Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID, &n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &createdAt)
	if err != nil {
		return nil, ErrTaskNoticeNotFound
	}
	n.CreatedAt = parseTime(createdAt)
	return &n, nil
}

func (s *PostgresStore) GetAgentNotification(ctx context.Context, agentID, id string) (*AgentNotification, error) {
	return s.GetAgentNotificationFiltered(ctx, agentID, id, false)
}

func (s *PostgresStore) GetAgentNotificationFiltered(
	ctx context.Context,
	agentID, id string,
	excludeTainted bool,
) (*AgentNotification, error) {
	var n AgentNotification
	query := `SELECT n.notification_id,n.agent_id,n.kind,n.task_id,n.assignment_version,n.domain,n.title,n.state,n.created_at
        FROM agent_notifications n JOIN memories m ON m.memory_id=n.task_id
        WHERE n.notification_id=$1 AND n.agent_id=$2 AND n.state IN ('unread','read')
        AND m.memory_type='task' AND m.status!='deprecated' AND m.task_status IN ('planned','in_progress')
	        AND m.assignee=n.agent_id AND m.task_assignment_version=n.assignment_version`
	if excludeTainted {
		query += ` AND NOT EXISTS (
			SELECT 1 FROM memory_taint_mirror tm WHERE tm.memory_id=m.memory_id
		)`
	}
	err := s.db.QueryRow(ctx, query, id, agentID).
		Scan(&n.NotificationID, &n.AgentID, &n.Kind, &n.TaskID, &n.AssignmentVersion, &n.Domain, &n.Title, &n.State, &n.CreatedAt)
	if err != nil {
		return nil, ErrTaskNoticeNotFound
	}
	return &n, nil
}
