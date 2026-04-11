package store

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"

	"github.com/l33tdawg/sage/internal/memory"
)

// pgxDB is satisfied by both *pgxpool.Pool and pgx.Tx, allowing
// PostgresStore methods to work inside or outside a transaction.
type pgxDB interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

// PostgresStore implements MemoryStore and ValidatorScoreStore using PostgreSQL.
type PostgresStore struct {
	db   pgxDB         // either pool or tx
	pool *pgxpool.Pool // nil for tx-scoped stores
}

// NewPostgresStore creates a new PostgreSQL store.
func NewPostgresStore(ctx context.Context, connString string) (*PostgresStore, error) {
	config, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("parse postgres config: %w", err)
	}

	config.MaxConns = 20
	config.MinConns = 2

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &PostgresStore{db: pool, pool: pool}, nil
}

func (s *PostgresStore) InsertMemory(ctx context.Context, record *memory.MemoryRecord) error {
	var emb *pgvector.Vector
	if len(record.Embedding) > 0 {
		v := pgvector.NewVector(record.Embedding)
		emb = &v
	}

	_, err := s.db.Exec(ctx,
		`INSERT INTO memories (memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
		ON CONFLICT (memory_id) DO UPDATE SET
			submitting_agent = EXCLUDED.submitting_agent,
			status = EXCLUDED.status,
			created_at = EXCLUDED.created_at,
			embedding = COALESCE(EXCLUDED.embedding, memories.embedding),
			embedding_hash = COALESCE(EXCLUDED.embedding_hash, memories.embedding_hash),
			provider = COALESCE(NULLIF(EXCLUDED.provider, ''), memories.provider),
			parent_hash = COALESCE(NULLIF(EXCLUDED.parent_hash, ''), memories.parent_hash)`,
		record.MemoryID, record.SubmittingAgent, record.Content, record.ContentHash,
		emb, record.EmbeddingHash,
		string(record.MemoryType), record.DomainTag, record.Provider, record.ConfidenceScore,
		string(record.Status), record.ParentHash, record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("insert memory: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetMemory(ctx context.Context, memoryID string) (*memory.MemoryRecord, error) {
	row := s.db.QueryRow(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash, embedding, embedding_hash,
			memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at, committed_at, deprecated_at
		FROM memories WHERE memory_id = $1`, memoryID)

	var r memory.MemoryRecord
	var mt, st string
	var emb *pgvector.Vector
	var parentHash *string

	err := row.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
		&emb, &r.EmbeddingHash, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
		&st, &parentHash, &r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("memory not found: %s", memoryID)
		}
		return nil, fmt.Errorf("get memory: %w", err)
	}

	r.MemoryType = memory.MemoryType(mt)
	r.Status = memory.MemoryStatus(st)
	if emb != nil {
		r.Embedding = emb.Slice()
	}
	if parentHash != nil {
		r.ParentHash = *parentHash
	}

	return &r, nil
}

func (s *PostgresStore) UpdateStatus(ctx context.Context, memoryID string, status memory.MemoryStatus, now time.Time) error {
	var query string
	switch status {
	case memory.StatusCommitted:
		query = `UPDATE memories SET status = $2, committed_at = $3 WHERE memory_id = $1`
	case memory.StatusDeprecated:
		query = `UPDATE memories SET status = $2, deprecated_at = $3 WHERE memory_id = $1`
	default:
		query = `UPDATE memories SET status = $2 WHERE memory_id = $1`
	}

	var err error
	if status == memory.StatusCommitted || status == memory.StatusDeprecated {
		_, err = s.db.Exec(ctx, query, memoryID, string(status), now)
	} else {
		_, err = s.db.Exec(ctx, `UPDATE memories SET status = $2 WHERE memory_id = $1`, memoryID, string(status))
	}
	if err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	return nil
}

func (s *PostgresStore) QuerySimilar(ctx context.Context, embedding []float32, opts QueryOptions) ([]*memory.MemoryRecord, error) {
	if opts.TopK <= 0 {
		opts.TopK = 10
	}
	if opts.TopK > 100 {
		opts.TopK = 100
	}

	query := `SELECT memory_id, submitting_agent, content, content_hash, embedding,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at, embedding <=> $1 AS distance
		FROM memories WHERE embedding IS NOT NULL`
	args := []any{pgvector.NewVector(embedding)}
	argIdx := 2

	if opts.DomainTag != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argIdx)
		args = append(args, opts.DomainTag)
		argIdx++
	}
	if opts.Provider != "" {
		query += fmt.Sprintf(" AND (provider = $%d OR provider = '' OR memory_type = 'fact')", argIdx)
		args = append(args, opts.Provider)
		argIdx++
	}
	if opts.MinConfidence > 0 {
		query += fmt.Sprintf(" AND confidence_score >= $%d", argIdx)
		args = append(args, opts.MinConfidence)
		argIdx++
	}
	if opts.StatusFilter != "" {
		query += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, opts.StatusFilter)
		argIdx++
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, a)
			argIdx++
		}
		query += " AND submitting_agent IN (" + strings.Join(placeholders, ",") + ")"
	}

	query += fmt.Sprintf(" ORDER BY embedding <=> $1 LIMIT $%d", argIdx)
	args = append(args, opts.TopK)

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query similar: %w", err)
	}
	defer rows.Close()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st string
		var emb *pgvector.Vector
		var parentHash *string
		var distance float64

		scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&emb, &mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore,
			&st, &parentHash, &r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt, &distance)
		if scanErr != nil {
			return nil, fmt.Errorf("scan row: %w", scanErr)
		}

		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		if emb != nil {
			r.Embedding = emb.Slice()
		}
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		results = append(results, &r)
	}

	return results, nil
}

// SearchByText is not implemented for PostgresStore — Postgres deployments use Ollama for semantic search.
func (s *PostgresStore) SearchByText(_ context.Context, _ string, _ QueryOptions) ([]*memory.MemoryRecord, error) {
	return nil, fmt.Errorf("text search not available on PostgresStore — use semantic search with Ollama")
}

func (s *PostgresStore) InsertTriples(ctx context.Context, memoryID string, triples []memory.KnowledgeTriple) error {
	batch := &pgx.Batch{}
	for _, t := range triples {
		batch.Queue("INSERT INTO knowledge_triples (memory_id, subject, predicate, object) VALUES ($1, $2, $3, $4)",
			memoryID, t.Subject, t.Predicate, t.Object)
	}

	br := s.db.SendBatch(ctx, batch)
	defer func() { _ = br.Close() }()

	for range triples {
		if _, err := br.Exec(); err != nil {
			return fmt.Errorf("insert triple: %w", err)
		}
	}
	return nil
}

func (s *PostgresStore) InsertVote(ctx context.Context, vote *ValidationVote) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO validation_votes (memory_id, validator_id, decision, rationale, weight_at_vote, block_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (memory_id, validator_id) DO UPDATE SET decision = $3, rationale = $4, weight_at_vote = $5`,
		vote.MemoryID, vote.ValidatorID, vote.Decision, vote.Rationale, vote.WeightAtVote, vote.BlockHeight, vote.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert vote: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetVotes(ctx context.Context, memoryID string) ([]*ValidationVote, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, memory_id, validator_id, decision, rationale, weight_at_vote, block_height, created_at
		FROM validation_votes WHERE memory_id = $1 ORDER BY created_at`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get votes: %w", err)
	}
	defer rows.Close()

	var votes []*ValidationVote
	for rows.Next() {
		v := &ValidationVote{}
		if scanErr := rows.Scan(&v.ID, &v.MemoryID, &v.ValidatorID, &v.Decision, &v.Rationale,
			&v.WeightAtVote, &v.BlockHeight, &v.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan vote: %w", scanErr)
		}
		votes = append(votes, v)
	}
	return votes, nil
}

func (s *PostgresStore) InsertChallenge(ctx context.Context, challenge *ChallengeEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO challenges (memory_id, challenger_id, reason, evidence, block_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		challenge.MemoryID, challenge.ChallengerID, challenge.Reason, challenge.Evidence, challenge.BlockHeight, challenge.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert challenge: %w", err)
	}
	return nil
}

func (s *PostgresStore) InsertCorroboration(ctx context.Context, corr *Corroboration) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO corroborations (memory_id, agent_id, evidence, created_at)
		VALUES ($1, $2, $3, $4)`,
		corr.MemoryID, corr.AgentID, corr.Evidence, corr.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert corroboration: %w", err)
	}
	return nil
}

func (s *PostgresStore) GetCorroborations(ctx context.Context, memoryID string) ([]*Corroboration, error) {
	rows, err := s.db.Query(ctx,
		`SELECT id, memory_id, agent_id, evidence, created_at
		FROM corroborations WHERE memory_id = $1 ORDER BY created_at`, memoryID)
	if err != nil {
		return nil, fmt.Errorf("get corroborations: %w", err)
	}
	defer rows.Close()

	var corrs []*Corroboration
	for rows.Next() {
		c := &Corroboration{}
		if scanErr := rows.Scan(&c.ID, &c.MemoryID, &c.AgentID, &c.Evidence, &c.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan corroboration: %w", scanErr)
		}
		corrs = append(corrs, c)
	}
	return corrs, nil
}

func (s *PostgresStore) GetPendingByDomain(ctx context.Context, domainTag string, limit int) ([]*memory.MemoryRecord, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(ctx,
		`SELECT memory_id, submitting_agent, content, content_hash,
			memory_type, domain_tag, confidence_score, status, created_at
		FROM memories WHERE status = 'proposed' AND domain_tag LIKE $1
		ORDER BY created_at LIMIT $2`, domainTag, limit)
	if err != nil {
		return nil, fmt.Errorf("get pending: %w", err)
	}
	defer rows.Close()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.ConfidenceScore, &st, &r.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan pending: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		results = append(results, &r)
	}
	return results, nil
}

// GetScore retrieves a validator's score.
func (s *PostgresStore) GetScore(ctx context.Context, validatorID string) (*ValidatorScore, error) {
	row := s.db.QueryRow(ctx,
		`SELECT validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at
		FROM validator_scores WHERE validator_id = $1`, validatorID)

	vs := &ValidatorScore{}
	err := row.Scan(&vs.ValidatorID, &vs.WeightedSum, &vs.WeightDenom, &vs.VoteCount,
		&vs.ExpertiseVec, &vs.LastActiveTS, &vs.CurrentWeight, &vs.UpdatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("validator score not found: %s", validatorID)
		}
		return nil, fmt.Errorf("get validator score: %w", err)
	}
	return vs, nil
}

// UpdateScore upserts a validator's score.
func (s *PostgresStore) UpdateScore(ctx context.Context, score *ValidatorScore) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO validator_scores (validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		ON CONFLICT (validator_id) DO UPDATE SET
			weighted_sum = $2, weight_denom = $3, vote_count = $4, expertise_vec = $5,
			last_active_ts = $6, current_weight = $7, updated_at = $8`,
		score.ValidatorID, score.WeightedSum, score.WeightDenom, score.VoteCount,
		score.ExpertiseVec, score.LastActiveTS, score.CurrentWeight, score.UpdatedAt)
	if err != nil {
		return fmt.Errorf("update validator score: %w", err)
	}
	return nil
}

// GetAllScores retrieves all validator scores.
func (s *PostgresStore) GetAllScores(ctx context.Context) ([]*ValidatorScore, error) {
	rows, err := s.db.Query(ctx,
		`SELECT validator_id, weighted_sum, weight_denom, vote_count, expertise_vec,
			last_active_ts, current_weight, updated_at
		FROM validator_scores ORDER BY validator_id`)
	if err != nil {
		return nil, fmt.Errorf("get all scores: %w", err)
	}
	defer rows.Close()

	var scores []*ValidatorScore
	for rows.Next() {
		vs := &ValidatorScore{}
		if scanErr := rows.Scan(&vs.ValidatorID, &vs.WeightedSum, &vs.WeightDenom, &vs.VoteCount,
			&vs.ExpertiseVec, &vs.LastActiveTS, &vs.CurrentWeight, &vs.UpdatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan validator score: %w", scanErr)
		}
		scores = append(scores, vs)
	}
	return scores, nil
}

// InsertEpochScore records an epoch score snapshot.
func (s *PostgresStore) InsertEpochScore(ctx context.Context, epoch *EpochScore) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO epoch_scores (epoch_num, block_height, validator_id, accuracy, domain_score,
			recency_score, corr_score, raw_weight, capped_weight, normalized_weight)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
		ON CONFLICT (epoch_num, validator_id) DO NOTHING`,
		epoch.EpochNum, epoch.BlockHeight, epoch.ValidatorID, epoch.Accuracy, epoch.DomainScore,
		epoch.RecencyScore, epoch.CorrScore, epoch.RawWeight, epoch.CappedWeight, epoch.NormalizedWeight)
	if err != nil {
		return fmt.Errorf("insert epoch score: %w", err)
	}
	return nil
}

// --- Federation Access Control ---

// InsertAccessGrant inserts an access grant into PostgreSQL.
func (s *PostgresStore) InsertAccessGrant(ctx context.Context, grant *AccessGrantEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO access_grants (domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (domain, grantee_id, created_height) DO NOTHING`,
		grant.Domain, grant.GranteeID, grant.GranterID, grant.Level, grant.ExpiresAt, grant.CreatedHeight, grant.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert access grant: %w", err)
	}
	return nil
}

// GetActiveGrants retrieves all non-revoked grants for an agent.
func (s *PostgresStore) GetActiveGrants(ctx context.Context, agentID string) ([]*AccessGrantEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT domain, grantee_id, granter_id, access_level, expires_at, created_height, created_at
		FROM access_grants WHERE grantee_id = $1 AND revoked_at IS NULL
		ORDER BY created_at`, agentID)
	if err != nil {
		return nil, fmt.Errorf("get active grants: %w", err)
	}
	defer rows.Close()

	var grants []*AccessGrantEntry
	for rows.Next() {
		g := &AccessGrantEntry{}
		if scanErr := rows.Scan(&g.Domain, &g.GranteeID, &g.GranterID, &g.Level, &g.ExpiresAt, &g.CreatedHeight, &g.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan grant: %w", scanErr)
		}
		grants = append(grants, g)
	}
	return grants, nil
}

// RevokeGrant marks a grant as revoked.
func (s *PostgresStore) RevokeGrant(ctx context.Context, domain, granteeID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE access_grants SET revoked_at = NOW()
		WHERE domain = $1 AND grantee_id = $2 AND revoked_at IS NULL`,
		domain, granteeID)
	if err != nil {
		return fmt.Errorf("revoke grant: %w", err)
	}
	return nil
}

// InsertAccessRequest inserts an access request into PostgreSQL.
func (s *PostgresStore) InsertAccessRequest(ctx context.Context, req *AccessRequestEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO access_requests (request_id, requester_id, target_domain, justification, status, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT (request_id) DO NOTHING`,
		req.RequestID, req.RequesterID, req.TargetDomain, req.Justification, req.Status, req.CreatedHeight, req.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert access request: %w", err)
	}
	return nil
}

// UpdateAccessRequestStatus updates the status of an access request.
func (s *PostgresStore) UpdateAccessRequestStatus(ctx context.Context, requestID, status string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE access_requests SET status = $2, resolved_height = $3 WHERE request_id = $1`,
		requestID, status, height)
	if err != nil {
		return fmt.Errorf("update access request status: %w", err)
	}
	return nil
}

// InsertAccessLog inserts an audit log entry into PostgreSQL.
func (s *PostgresStore) InsertAccessLog(ctx context.Context, log *AccessLogEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO access_logs (agent_id, domain, action, memory_ids, block_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		log.AgentID, log.Domain, log.Action, log.MemoryIDs, log.BlockHeight, log.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert access log: %w", err)
	}
	return nil
}

// InsertDomain inserts a domain registry entry into PostgreSQL.
func (s *PostgresStore) InsertDomain(ctx context.Context, domain *DomainEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO domain_registry (domain_name, owner_agent_id, parent_domain, description, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (domain_name) DO NOTHING`,
		domain.DomainName, domain.OwnerAgentID, domain.ParentDomain, domain.Description, domain.CreatedHeight, domain.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert domain: %w", err)
	}
	return nil
}

// GetDomain retrieves a domain registry entry from PostgreSQL.
func (s *PostgresStore) GetDomain(ctx context.Context, name string) (*DomainEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT domain_name, owner_agent_id, parent_domain, description, created_height, created_at
		FROM domain_registry WHERE domain_name = $1`, name)

	d := &DomainEntry{}
	var parentDomain, description *string
	err := row.Scan(&d.DomainName, &d.OwnerAgentID, &parentDomain, &description, &d.CreatedHeight, &d.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("domain not found: %s", name)
		}
		return nil, fmt.Errorf("get domain: %w", err)
	}
	if parentDomain != nil {
		d.ParentDomain = *parentDomain
	}
	if description != nil {
		d.Description = *description
	}
	return d, nil
}

// --- Organization / Federation / Classification ---

// InsertOrg inserts an organization into PostgreSQL.
func (s *PostgresStore) InsertOrg(ctx context.Context, org *OrgEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO organizations (org_id, name, description, admin_agent_id, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (org_id) DO NOTHING`,
		org.OrgID, org.Name, org.Description, org.AdminAgentID, org.CreatedHeight, org.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert org: %w", err)
	}
	return nil
}

// GetOrg retrieves an organization from PostgreSQL.
func (s *PostgresStore) GetOrg(ctx context.Context, orgID string) (*OrgEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT org_id, name, description, admin_agent_id, created_height, created_at
		FROM organizations WHERE org_id = $1`, orgID)

	o := &OrgEntry{}
	var description *string
	err := row.Scan(&o.OrgID, &o.Name, &description, &o.AdminAgentID, &o.CreatedHeight, &o.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("org not found: %s", orgID)
		}
		return nil, fmt.Errorf("get org: %w", err)
	}
	if description != nil {
		o.Description = *description
	}
	return o, nil
}

// InsertOrgMember inserts an org member into PostgreSQL.
func (s *PostgresStore) InsertOrgMember(ctx context.Context, member *OrgMemberEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO org_members (org_id, agent_id, clearance, role, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (org_id, agent_id) DO NOTHING`,
		member.OrgID, member.AgentID, int16(member.Clearance), member.Role, member.CreatedHeight, member.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert org member: %w", err)
	}
	return nil
}

// RemoveOrgMember marks a member as removed.
func (s *PostgresStore) RemoveOrgMember(ctx context.Context, orgID, agentID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE org_members SET removed_at = NOW()
		WHERE org_id = $1 AND agent_id = $2 AND removed_at IS NULL`,
		orgID, agentID)
	if err != nil {
		return fmt.Errorf("remove org member: %w", err)
	}
	return nil
}

// UpdateMemberClearance updates a member's clearance level.
func (s *PostgresStore) UpdateMemberClearance(ctx context.Context, orgID, agentID string, clearance ClearanceLevel) error {
	_, err := s.db.Exec(ctx,
		`UPDATE org_members SET clearance = $3
		WHERE org_id = $1 AND agent_id = $2 AND removed_at IS NULL`,
		orgID, agentID, int16(clearance))
	if err != nil {
		return fmt.Errorf("update member clearance: %w", err)
	}
	return nil
}

// GetOrgMembers retrieves all active members of an organization.
func (s *PostgresStore) GetOrgMembers(ctx context.Context, orgID string) ([]*OrgMemberEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT org_id, agent_id, clearance, role, created_height, created_at
		FROM org_members WHERE org_id = $1 AND removed_at IS NULL
		ORDER BY created_at`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get org members: %w", err)
	}
	defer rows.Close()

	var members []*OrgMemberEntry
	for rows.Next() {
		m := &OrgMemberEntry{}
		var clearance int16
		if scanErr := rows.Scan(&m.OrgID, &m.AgentID, &clearance, &m.Role, &m.CreatedHeight, &m.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan org member: %w", scanErr)
		}
		m.Clearance = ClearanceLevel(clearance) // #nosec G115 -- clearance is 0-4
		members = append(members, m)
	}
	return members, nil
}

// InsertFederation inserts a federation agreement into PostgreSQL.
func (s *PostgresStore) InsertFederation(ctx context.Context, fed *FederationEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO federations (federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
		ON CONFLICT (federation_id) DO NOTHING`,
		fed.FederationID, fed.ProposerOrgID, fed.TargetOrgID, fed.AllowedDomains, fed.AllowedDepts,
		int16(fed.MaxClearance), fed.ExpiresAt, fed.RequiresApproval, fed.Status,
		fed.CreatedHeight, fed.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert federation: %w", err)
	}
	return nil
}

// GetFederation retrieves a federation agreement from PostgreSQL.
func (s *PostgresStore) GetFederation(ctx context.Context, federationID string) (*FederationEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height,
			approved_height, created_at, revoked_at
		FROM federations WHERE federation_id = $1`, federationID)

	f := &FederationEntry{}
	var maxClearance int16
	err := row.Scan(&f.FederationID, &f.ProposerOrgID, &f.TargetOrgID, &f.AllowedDomains, &f.AllowedDepts,
		&maxClearance, &f.ExpiresAt, &f.RequiresApproval, &f.Status, &f.CreatedHeight,
		&f.ApprovedHeight, &f.CreatedAt, &f.RevokedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("federation not found: %s", federationID)
		}
		return nil, fmt.Errorf("get federation: %w", err)
	}
	f.MaxClearance = ClearanceLevel(maxClearance) // #nosec G115 -- clearance is 0-4
	return f, nil
}

// ApproveFederation marks a federation as active with an approved height.
func (s *PostgresStore) ApproveFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE federations SET status = 'active', approved_height = $2
		WHERE federation_id = $1 AND status = 'proposed'`,
		federationID, height)
	if err != nil {
		return fmt.Errorf("approve federation: %w", err)
	}
	return nil
}

// RevokeFederation marks a federation as revoked.
func (s *PostgresStore) RevokeFederation(ctx context.Context, federationID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE federations SET status = 'revoked', revoked_at = NOW()
		WHERE federation_id = $1 AND status = 'active'`,
		federationID)
	if err != nil {
		return fmt.Errorf("revoke federation: %w", err)
	}
	return nil
}

// GetActiveFederations retrieves all active federations for an org (as proposer or target).
func (s *PostgresStore) GetActiveFederations(ctx context.Context, orgID string) ([]*FederationEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT federation_id, proposer_org_id, target_org_id, allowed_domains, allowed_depts,
			max_clearance, expires_at, requires_approval, status, created_height,
			approved_height, created_at, revoked_at
		FROM federations
		WHERE (proposer_org_id = $1 OR target_org_id = $1) AND status IN ('active', 'proposed')
		ORDER BY created_at`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get active federations: %w", err)
	}
	defer rows.Close()

	var feds []*FederationEntry
	for rows.Next() {
		f := &FederationEntry{}
		var maxClearance int16
		if scanErr := rows.Scan(&f.FederationID, &f.ProposerOrgID, &f.TargetOrgID, &f.AllowedDomains, &f.AllowedDepts,
			&maxClearance, &f.ExpiresAt, &f.RequiresApproval, &f.Status, &f.CreatedHeight,
			&f.ApprovedHeight, &f.CreatedAt, &f.RevokedAt); scanErr != nil {
			return nil, fmt.Errorf("scan federation: %w", scanErr)
		}
		f.MaxClearance = ClearanceLevel(maxClearance) // #nosec G115 -- clearance is 0-4
		feds = append(feds, f)
	}
	return feds, nil
}

// --- Department methods ---

// InsertDept inserts a department into PostgreSQL.
func (s *PostgresStore) InsertDept(ctx context.Context, dept *DeptEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO departments (dept_id, org_id, dept_name, description, parent_dept, created_height)
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT DO NOTHING`,
		dept.DeptID, dept.OrgID, dept.DeptName, dept.Description, dept.ParentDept, dept.CreatedHeight)
	if err != nil {
		return fmt.Errorf("insert dept: %w", err)
	}
	return nil
}

// GetDept retrieves a department by org and dept ID.
func (s *PostgresStore) GetDept(ctx context.Context, orgID, deptID string) (*DeptEntry, error) {
	row := s.db.QueryRow(ctx,
		`SELECT dept_id, org_id, dept_name, description, parent_dept, created_height, created_at
		FROM departments WHERE org_id = $1 AND dept_id = $2`, orgID, deptID)

	d := &DeptEntry{}
	var description, parentDept *string
	err := row.Scan(&d.DeptID, &d.OrgID, &d.DeptName, &description, &parentDept, &d.CreatedHeight, &d.CreatedAt)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, fmt.Errorf("dept not found: %s/%s", orgID, deptID)
		}
		return nil, fmt.Errorf("get dept: %w", err)
	}
	if description != nil {
		d.Description = *description
	}
	if parentDept != nil {
		d.ParentDept = *parentDept
	}
	return d, nil
}

// GetOrgDepts retrieves all departments for an organization.
func (s *PostgresStore) GetOrgDepts(ctx context.Context, orgID string) ([]*DeptEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT dept_id, org_id, dept_name, description, parent_dept, created_height, created_at
		FROM departments WHERE org_id = $1 ORDER BY dept_name`, orgID)
	if err != nil {
		return nil, fmt.Errorf("get org depts: %w", err)
	}
	defer rows.Close()

	var depts []*DeptEntry
	for rows.Next() {
		d := &DeptEntry{}
		var description, parentDept *string
		if scanErr := rows.Scan(&d.DeptID, &d.OrgID, &d.DeptName, &description, &parentDept, &d.CreatedHeight, &d.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan dept: %w", scanErr)
		}
		if description != nil {
			d.Description = *description
		}
		if parentDept != nil {
			d.ParentDept = *parentDept
		}
		depts = append(depts, d)
	}
	return depts, nil
}

// InsertDeptMember inserts a department member into PostgreSQL.
func (s *PostgresStore) InsertDeptMember(ctx context.Context, member *DeptMemberEntry) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO dept_members (org_id, dept_id, agent_id, clearance, role, created_height, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
		ON CONFLICT DO NOTHING`,
		member.OrgID, member.DeptID, member.AgentID, int16(member.Clearance), member.Role, member.CreatedHeight, member.CreatedAt)
	if err != nil {
		return fmt.Errorf("insert dept member: %w", err)
	}
	return nil
}

// RemoveDeptMember soft-deletes a department member.
func (s *PostgresStore) RemoveDeptMember(ctx context.Context, orgID, deptID, agentID string, height int64) error {
	_, err := s.db.Exec(ctx,
		`UPDATE dept_members SET removed_at = NOW()
		WHERE org_id = $1 AND dept_id = $2 AND agent_id = $3 AND removed_at IS NULL`,
		orgID, deptID, agentID)
	if err != nil {
		return fmt.Errorf("remove dept member: %w", err)
	}
	return nil
}

// GetDeptMembers retrieves all active members of a department.
func (s *PostgresStore) GetDeptMembers(ctx context.Context, orgID, deptID string) ([]*DeptMemberEntry, error) {
	rows, err := s.db.Query(ctx,
		`SELECT org_id, dept_id, agent_id, clearance, role, created_height, created_at
		FROM dept_members WHERE org_id = $1 AND dept_id = $2 AND removed_at IS NULL
		ORDER BY created_at`, orgID, deptID)
	if err != nil {
		return nil, fmt.Errorf("get dept members: %w", err)
	}
	defer rows.Close()

	var members []*DeptMemberEntry
	for rows.Next() {
		m := &DeptMemberEntry{}
		var clearance int16
		if scanErr := rows.Scan(&m.OrgID, &m.DeptID, &m.AgentID, &clearance, &m.Role, &m.CreatedHeight, &m.CreatedAt); scanErr != nil {
			return nil, fmt.Errorf("scan dept member: %w", scanErr)
		}
		m.Clearance = ClearanceLevel(clearance) // #nosec G115 -- clearance is 0-4
		members = append(members, m)
	}
	return members, nil
}

// UpdateDeptMemberClearance updates a department member's clearance level.
func (s *PostgresStore) UpdateDeptMemberClearance(ctx context.Context, orgID, deptID, agentID string, clearance ClearanceLevel) error {
	_, err := s.db.Exec(ctx,
		`UPDATE dept_members SET clearance = $4
		WHERE org_id = $1 AND dept_id = $2 AND agent_id = $3 AND removed_at IS NULL`,
		orgID, deptID, agentID, int16(clearance))
	if err != nil {
		return fmt.Errorf("update dept member clearance: %w", err)
	}
	return nil
}

// UpdateMemoryClassification updates a memory's classification level.
func (s *PostgresStore) UpdateMemoryClassification(ctx context.Context, memoryID string, classification ClearanceLevel) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET classification = $2 WHERE memory_id = $1`,
		memoryID, int16(classification))
	if err != nil {
		return fmt.Errorf("update memory classification: %w", err)
	}
	return nil
}

// ListMemories returns memories matching the given filters with pagination.
func (s *PostgresStore) ListMemories(ctx context.Context, opts ListOptions) ([]*memory.MemoryRecord, int, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}

	countQuery := `SELECT COUNT(*) FROM memories WHERE 1=1`
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, provider, confidence_score, status, parent_hash, created_at,
		committed_at, deprecated_at FROM memories WHERE 1=1`
	var args []any
	argIdx := 1

	if opts.DomainTag != "" {
		filter := fmt.Sprintf(" AND domain_tag = $%d", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.DomainTag)
		argIdx++
	}
	if opts.Provider != "" {
		filter := fmt.Sprintf(" AND (provider = $%d OR provider = '' OR memory_type = 'fact')", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.Provider)
		argIdx++
	}
	if opts.Status != "" {
		filter := fmt.Sprintf(" AND status = $%d", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.Status)
		argIdx++
	}
	if opts.SubmittingAgent != "" {
		filter := fmt.Sprintf(" AND submitting_agent = $%d", argIdx)
		query += filter
		countQuery += filter
		args = append(args, opts.SubmittingAgent)
		argIdx++
	}
	if len(opts.SubmittingAgents) > 0 {
		placeholders := make([]string, len(opts.SubmittingAgents))
		for i, a := range opts.SubmittingAgents {
			placeholders[i] = fmt.Sprintf("$%d", argIdx)
			args = append(args, a)
			argIdx++
		}
		filter := " AND submitting_agent IN (" + strings.Join(placeholders, ",") + ")"
		query += filter
		countQuery += filter
	}

	switch opts.Sort {
	case "oldest":
		query += " ORDER BY created_at ASC"
	case "confidence":
		query += " ORDER BY confidence_score DESC"
	default:
		query += " ORDER BY created_at DESC"
	}

	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	queryArgs := append(args, opts.Limit, opts.Offset)

	var total int
	if err := s.db.QueryRow(ctx, countQuery, args...).Scan(&total); err != nil {
		return nil, 0, fmt.Errorf("count memories: %w", err)
	}

	rows, err := s.db.Query(ctx, query, queryArgs...)
	if err != nil {
		return nil, 0, fmt.Errorf("list memories: %w", err)
	}
	defer rows.Close()

	results := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st string
		var parentHash *string
		if scanErr := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash,
			&r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt); scanErr != nil {
			return nil, 0, fmt.Errorf("scan memory: %w", scanErr)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		results = append(results, &r)
	}
	return results, total, nil
}

// GetStats returns aggregate statistics about stored memories.
func (s *PostgresStore) GetStats(ctx context.Context) (*StoreStats, error) {
	stats := &StoreStats{
		ByDomain: make(map[string]int),
		ByStatus: make(map[string]int),
	}

	if err := s.db.QueryRow(ctx, `SELECT COUNT(*) FROM memories`).Scan(&stats.TotalMemories); err != nil {
		return nil, fmt.Errorf("count total: %w", err)
	}

	rows, err := s.db.Query(ctx, `SELECT domain_tag, COUNT(*) FROM memories GROUP BY domain_tag`)
	if err != nil {
		return nil, fmt.Errorf("count by domain: %w", err)
	}
	for rows.Next() {
		var domain string
		var count int
		if scanErr := rows.Scan(&domain, &count); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("scan domain count: %w", scanErr)
		}
		stats.ByDomain[domain] = count
	}
	rows.Close()

	rows, err = s.db.Query(ctx, `SELECT status, COUNT(*) FROM memories GROUP BY status`)
	if err != nil {
		return nil, fmt.Errorf("count by status: %w", err)
	}
	for rows.Next() {
		var status string
		var count int
		if scanErr := rows.Scan(&status, &count); scanErr != nil {
			rows.Close()
			return nil, fmt.Errorf("scan status count: %w", scanErr)
		}
		stats.ByStatus[status] = count
	}
	rows.Close()

	row := s.db.QueryRow(ctx, `SELECT MAX(created_at) FROM memories`)
	var lastActivity *time.Time
	if scanErr := row.Scan(&lastActivity); scanErr == nil {
		stats.LastActivity = lastActivity
	}

	return stats, nil
}

// GetTimeline returns memory counts aggregated by time periods.
func (s *PostgresStore) GetTimeline(ctx context.Context, from, to time.Time, domain string, bucket string) ([]TimelineBucket, error) {
	trunc := "day"
	switch bucket {
	case "hour":
		trunc = "hour"
	case "week":
		trunc = "week"
	case "month":
		trunc = "month"
	}

	query := fmt.Sprintf(`SELECT date_trunc('%s', created_at) AS period, COUNT(*)
		FROM memories WHERE created_at >= $1 AND created_at <= $2`, trunc)
	args := []any{from, to}
	argIdx := 3

	if domain != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argIdx)
		args = append(args, domain)
	}

	query += ` GROUP BY period ORDER BY period`

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get timeline: %w", err)
	}
	defer rows.Close()

	buckets := make([]TimelineBucket, 0)
	for rows.Next() {
		var period time.Time
		var count int
		if scanErr := rows.Scan(&period, &count); scanErr != nil {
			return nil, fmt.Errorf("scan timeline: %w", scanErr)
		}
		buckets = append(buckets, TimelineBucket{
			Period: period.Format(time.RFC3339),
			Count:  count,
			Domain: domain,
		})
	}
	return buckets, nil
}

// DeleteMemory soft-deletes a memory by setting status to deprecated.
func (s *PostgresStore) DeleteMemory(ctx context.Context, memoryID string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET status = 'deprecated', deprecated_at = NOW() WHERE memory_id = $1`,
		memoryID)
	if err != nil {
		return fmt.Errorf("delete memory: %w", err)
	}
	return nil
}

// UpdateDomainTag updates the domain tag of a memory.
func (s *PostgresStore) UpdateDomainTag(ctx context.Context, memoryID string, domain string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET domain_tag = $2 WHERE memory_id = $1`,
		memoryID, domain)
	if err != nil {
		return fmt.Errorf("update domain tag: %w", err)
	}
	return nil
}

// UpdateMemoryAgent updates the submitting agent of a memory.
func (s *PostgresStore) UpdateMemoryAgent(ctx context.Context, memoryID string, agentID string) error {
	_, err := s.db.Exec(ctx,
		`UPDATE memories SET submitting_agent = $2 WHERE memory_id = $1`,
		memoryID, agentID)
	if err != nil {
		return fmt.Errorf("update memory agent: %w", err)
	}
	return nil
}

// UpdateTaskStatus updates the task_status of a task memory.
func (s *PostgresStore) UpdateTaskStatus(ctx context.Context, memoryID string, taskStatus memory.TaskStatus) error {
	result, err := s.db.Exec(ctx,
		`UPDATE memories SET task_status = $2 WHERE memory_id = $1 AND memory_type = 'task'`,
		memoryID, string(taskStatus))
	if err != nil {
		return fmt.Errorf("update task status: %w", err)
	}
	if result.RowsAffected() == 0 {
		return fmt.Errorf("task not found: %s", memoryID)
	}
	return nil
}

// LinkMemories creates a link between two memories.
func (s *PostgresStore) LinkMemories(ctx context.Context, sourceID, targetID, linkType string) error {
	_, err := s.db.Exec(ctx,
		`INSERT INTO memory_links (source_id, target_id, link_type) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING`,
		sourceID, targetID, linkType)
	if err != nil {
		return fmt.Errorf("link memories: %w", err)
	}
	return nil
}

// GetLinkedMemories returns all memories linked to the given memory ID.
func (s *PostgresStore) GetLinkedMemories(ctx context.Context, memoryID string) ([]memory.MemoryLink, error) {
	rows, err := s.db.Query(ctx,
		`SELECT source_id, target_id, link_type, created_at FROM memory_links WHERE source_id = $1 OR target_id = $1`,
		memoryID)
	if err != nil {
		return nil, fmt.Errorf("get linked memories: %w", err)
	}
	defer rows.Close()

	links := make([]memory.MemoryLink, 0)
	for rows.Next() {
		var l memory.MemoryLink
		var createdAt time.Time
		if err := rows.Scan(&l.SourceID, &l.TargetID, &l.LinkType, &createdAt); err != nil {
			return nil, fmt.Errorf("scan link: %w", err)
		}
		l.CreatedAt = createdAt.Format(time.RFC3339)
		links = append(links, l)
	}
	return links, rows.Err()
}

// GetOpenTasks returns all task memories that are planned or in_progress.
func (s *PostgresStore) GetOpenTasks(ctx context.Context, domain string, provider string) ([]*memory.MemoryRecord, error) {
	query := `SELECT memory_id, submitting_agent, content, content_hash,
		memory_type, domain_tag, COALESCE(provider, ''), confidence_score, status, parent_hash, COALESCE(task_status, ''),
		created_at, committed_at, deprecated_at
		FROM memories
		WHERE memory_type = 'task'
		AND task_status IN ('planned', 'in_progress')
		AND status != 'deprecated'`
	args := []any{}
	argN := 1

	if domain != "" {
		query += fmt.Sprintf(" AND domain_tag = $%d", argN)
		args = append(args, domain)
		argN++
	}
	if provider != "" {
		query += fmt.Sprintf(" AND (provider = $%d OR provider = '')", argN)
		args = append(args, provider)
		argN++ //nolint:ineffassign
	}
	query += " ORDER BY created_at DESC"

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("get open tasks: %w", err)
	}
	defer rows.Close()

	records := make([]*memory.MemoryRecord, 0)
	for rows.Next() {
		var r memory.MemoryRecord
		var mt, st, ts string
		var parentHash *string
		if err := rows.Scan(&r.MemoryID, &r.SubmittingAgent, &r.Content, &r.ContentHash,
			&mt, &r.DomainTag, &r.Provider, &r.ConfidenceScore, &st, &parentHash, &ts,
			&r.CreatedAt, &r.CommittedAt, &r.DeprecatedAt); err != nil {
			return nil, fmt.Errorf("scan task: %w", err)
		}
		r.MemoryType = memory.MemoryType(mt)
		r.Status = memory.MemoryStatus(st)
		r.TaskStatus = memory.TaskStatus(ts)
		if parentHash != nil {
			r.ParentHash = *parentHash
		}
		records = append(records, &r)
	}
	return records, rows.Err()
}

// GetAllTasks returns all task memories across all statuses for the Kanban board.
func (s *PostgresStore) GetAllTasks(_ context.Context, _ string, _ int) ([]*memory.MemoryRecord, error) {
	return nil, nil
}

// ---- Tag operations (stubs — Postgres uses enterprise deployment, tags are SQLite/personal) ----

func (s *PostgresStore) SetTags(_ context.Context, _ string, _ []string) error {
	return nil
}

func (s *PostgresStore) GetTags(_ context.Context, _ string) ([]string, error) {
	return nil, nil
}

func (s *PostgresStore) GetTagsBatch(_ context.Context, _ []string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s *PostgresStore) ListAllTags(_ context.Context) ([]TagCount, error) {
	return nil, nil
}

func (s *PostgresStore) ListMemoriesByTag(_ context.Context, _ string, _, _ int) ([]*memory.MemoryRecord, int, error) {
	return nil, 0, nil
}

func (s *PostgresStore) FindByContentHash(_ context.Context, _ string) (bool, error) {
	return false, nil // TODO: implement for postgres
}

func (s *PostgresStore) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

// Ping checks the database connection.
func (s *PostgresStore) Ping(ctx context.Context) error {
	if s.pool != nil {
		return s.pool.Ping(ctx)
	}
	return nil
}

// RunInTx executes fn within a PostgreSQL transaction. All writes through
// the tx-scoped OffchainStore are atomic — either all succeed or all roll back.
func (s *PostgresStore) RunInTx(ctx context.Context, fn func(tx OffchainStore) error) error {
	if s.pool == nil {
		// Already in a transaction — execute directly.
		return fn(s)
	}
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback(ctx) //nolint:errcheck

	txStore := &PostgresStore{db: tx}
	if err := fn(txStore); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// --- AgentStore stubs (PostgreSQL implementation TODO) ---

func (s *PostgresStore) ListAgents(_ context.Context) ([]*AgentEntry, error) {
	return nil, fmt.Errorf("ListAgents not implemented for PostgresStore")
}

func (s *PostgresStore) GetAgent(_ context.Context, _ string) (*AgentEntry, error) {
	return nil, fmt.Errorf("GetAgent not implemented for PostgresStore")
}

func (s *PostgresStore) GetAgentByName(_ context.Context, _ string) (*AgentEntry, error) {
	return nil, fmt.Errorf("GetAgentByName not implemented for PostgresStore")
}

func (s *PostgresStore) CreateAgent(_ context.Context, _ *AgentEntry) error {
	return fmt.Errorf("CreateAgent not implemented for PostgresStore")
}

func (s *PostgresStore) UpdateAgent(_ context.Context, _ *AgentEntry) error {
	return fmt.Errorf("UpdateAgent not implemented for PostgresStore")
}

func (s *PostgresStore) RemoveAgent(_ context.Context, _ string) error {
	return fmt.Errorf("RemoveAgent not implemented for PostgresStore")
}

func (s *PostgresStore) UpdateAgentStatus(_ context.Context, _, _ string) error {
	return fmt.Errorf("UpdateAgentStatus not implemented for PostgresStore")
}

func (s *PostgresStore) UpdateAgentLastSeen(_ context.Context, _ string, _ time.Time) error {
	return fmt.Errorf("UpdateAgentLastSeen not implemented for PostgresStore")
}

func (s *PostgresStore) BackfillFirstSeen(_ context.Context, _ string, _ time.Time) error {
	return fmt.Errorf("BackfillFirstSeen not implemented for PostgresStore")
}

func (s *PostgresStore) RotateAgentKey(_ context.Context, _ string) (string, []byte, error) {
	return "", nil, fmt.Errorf("RotateAgentKey not implemented for PostgresStore")
}

func (s *PostgresStore) ReassignMemories(_ context.Context, _, _ string) (int64, error) {
	return 0, fmt.Errorf("ReassignMemories not implemented for PostgresStore")
}

func (s *PostgresStore) ListAgentTags(_ context.Context, _ string) ([]TagCount, error) {
	return nil, fmt.Errorf("ListAgentTags not implemented for PostgresStore")
}

func (s *PostgresStore) ReassignMemoriesByTag(_ context.Context, _, _, _ string) (int64, error) {
	return 0, fmt.Errorf("ReassignMemoriesByTag not implemented for PostgresStore")
}

func (s *PostgresStore) ReassignMemoriesByDomain(_ context.Context, _, _, _ string) (int64, error) {
	return 0, fmt.Errorf("ReassignMemoriesByDomain not implemented for PostgresStore")
}

func (s *PostgresStore) AcquireRedeployLock(_ context.Context, _, _ string, _ time.Duration) error {
	return fmt.Errorf("AcquireRedeployLock not implemented for PostgresStore")
}

func (s *PostgresStore) ReleaseRedeployLock(_ context.Context) error {
	return fmt.Errorf("ReleaseRedeployLock not implemented for PostgresStore")
}

func (s *PostgresStore) GetRedeployLock(_ context.Context) (*RedeploymentLock, error) {
	return nil, fmt.Errorf("GetRedeployLock not implemented for PostgresStore")
}

func (s *PostgresStore) InsertRedeployLog(_ context.Context, _ *RedeploymentLogEntry) error {
	return fmt.Errorf("InsertRedeployLog not implemented for PostgresStore")
}

func (s *PostgresStore) GetRedeployLog(_ context.Context, _ string) ([]*RedeploymentLogEntry, error) {
	return nil, fmt.Errorf("GetRedeployLog not implemented for PostgresStore")
}

func (s *PostgresStore) UpdateRedeployLog(_ context.Context, _ int64, _, _ string) error {
	return fmt.Errorf("UpdateRedeployLog not implemented for PostgresStore")
}

// --- Pipeline Store stubs (SQLite-only feature for now) ---

func (s *PostgresStore) InsertPipeline(_ context.Context, _ *PipelineMessage) error {
	return fmt.Errorf("InsertPipeline not implemented for PostgresStore")
}

func (s *PostgresStore) GetPipeline(_ context.Context, _ string) (*PipelineMessage, error) {
	return nil, fmt.Errorf("GetPipeline not implemented for PostgresStore")
}

func (s *PostgresStore) GetInbox(_ context.Context, _, _ string, _ int) ([]*PipelineMessage, error) {
	return nil, fmt.Errorf("GetInbox not implemented for PostgresStore")
}

func (s *PostgresStore) ClaimPipeline(_ context.Context, _, _ string) error {
	return fmt.Errorf("ClaimPipeline not implemented for PostgresStore")
}

func (s *PostgresStore) CompletePipeline(_ context.Context, _, _, _ string) error {
	return fmt.Errorf("CompletePipeline not implemented for PostgresStore")
}

func (s *PostgresStore) GetCompletedForSender(_ context.Context, _ string, _ int) ([]*PipelineMessage, error) {
	return nil, fmt.Errorf("GetCompletedForSender not implemented for PostgresStore")
}

func (s *PostgresStore) ListPipelines(_ context.Context, _ string, _ int) ([]*PipelineMessage, error) {
	return nil, fmt.Errorf("ListPipelines not implemented for PostgresStore")
}

func (s *PostgresStore) PipelineStats(_ context.Context) (map[string]int, error) {
	return nil, fmt.Errorf("PipelineStats not implemented for PostgresStore")
}

func (s *PostgresStore) ExpirePipelines(_ context.Context) (int, error) {
	return 0, fmt.Errorf("ExpirePipelines not implemented for PostgresStore")
}

func (s *PostgresStore) PurgePipelines(_ context.Context, _ time.Time) (int, error) {
	return 0, fmt.Errorf("PurgePipelines not implemented for PostgresStore")
}

// GovernanceStore stubs — PostgresStore does not yet implement governance.
func (s *PostgresStore) InsertGovProposal(_ context.Context, _ *GovProposal) error {
	return nil // no-op
}
func (s *PostgresStore) GetGovProposal(_ context.Context, _ string) (*GovProposal, error) {
	return nil, fmt.Errorf("governance not implemented for PostgresStore")
}
func (s *PostgresStore) UpdateGovProposalStatus(_ context.Context, _, _ string, _ *int64) error {
	return nil // no-op
}
func (s *PostgresStore) ListGovProposals(_ context.Context, _ string) ([]*GovProposal, error) {
	return make([]*GovProposal, 0), nil
}
func (s *PostgresStore) InsertGovVote(_ context.Context, _ *GovVote) error {
	return nil // no-op
}
func (s *PostgresStore) GetGovVotes(_ context.Context, _ string) ([]*GovVote, error) {
	return make([]*GovVote, 0), nil
}
