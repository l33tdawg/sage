-- SAGE PostgreSQL Schema
-- Sovereign Agent Governed Experience — off-chain state store

-- Extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS "vector";

-- ============================================================
-- 1. memories
-- ============================================================
CREATE TABLE memories (
    memory_id        UUID             PRIMARY KEY DEFAULT uuid_generate_v4(),
    submitting_agent TEXT             NOT NULL,
    content          TEXT             NOT NULL,
    content_hash     BYTEA            NOT NULL,
    embedding        vector(768),
    embedding_hash   BYTEA,
    memory_type      TEXT             NOT NULL CHECK (memory_type IN ('fact', 'observation', 'inference', 'task')),
    domain_tag       TEXT             NOT NULL,
    provider         TEXT             NOT NULL DEFAULT '',
    confidence_score DOUBLE PRECISION NOT NULL CHECK (confidence_score BETWEEN 0 AND 1),
    status           TEXT             NOT NULL DEFAULT 'proposed',
    parent_hash      TEXT,
    task_status      TEXT             DEFAULT '' CHECK (task_status IN ('', 'planned', 'in_progress', 'done', 'dropped')),
    created_at       TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    committed_at     TIMESTAMPTZ,
    deprecated_at    TIMESTAMPTZ
);

-- ============================================================
-- 1b. memory_links
-- ============================================================
CREATE TABLE memory_links (
    source_id  UUID NOT NULL REFERENCES memories(memory_id),
    target_id  UUID NOT NULL REFERENCES memories(memory_id),
    link_type  TEXT NOT NULL DEFAULT 'related',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (source_id, target_id)
);
CREATE INDEX idx_memory_links_target ON memory_links(target_id);

-- ============================================================
-- 2. knowledge_triples
-- ============================================================
CREATE TABLE knowledge_triples (
    id        BIGSERIAL PRIMARY KEY,
    memory_id UUID      REFERENCES memories(memory_id),
    subject   TEXT      NOT NULL,
    predicate TEXT      NOT NULL,
    object    TEXT      NOT NULL
);

-- ============================================================
-- 3. validation_votes
-- ============================================================
CREATE TABLE validation_votes (
    id             BIGSERIAL        PRIMARY KEY,
    memory_id      UUID             REFERENCES memories(memory_id),
    validator_id   TEXT             NOT NULL,
    decision       TEXT             NOT NULL CHECK (decision IN ('accept', 'reject', 'abstain')),
    rationale      TEXT,
    weight_at_vote DOUBLE PRECISION,
    block_height   BIGINT,
    created_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX idx_validation_votes_memory_validator
    ON validation_votes (memory_id, validator_id);

-- ============================================================
-- 4. corroborations
-- ============================================================
CREATE TABLE corroborations (
    id        BIGSERIAL   PRIMARY KEY,
    memory_id UUID        REFERENCES memories(memory_id),
    agent_id  TEXT        NOT NULL,
    evidence  TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 5. challenges
-- ============================================================
CREATE TABLE IF NOT EXISTS challenges (
    id           BIGSERIAL   PRIMARY KEY,
    memory_id    UUID        NOT NULL REFERENCES memories(memory_id),
    challenger_id TEXT       NOT NULL,
    reason       TEXT        NOT NULL,
    evidence     TEXT,
    block_height BIGINT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_challenges_memory ON challenges(memory_id);

-- ============================================================
-- 6. validator_scores
-- ============================================================
CREATE TABLE validator_scores (
    validator_id   TEXT             PRIMARY KEY,
    weighted_sum   DOUBLE PRECISION NOT NULL DEFAULT 0,
    weight_denom   DOUBLE PRECISION NOT NULL DEFAULT 0,
    vote_count     BIGINT           NOT NULL DEFAULT 0,
    expertise_vec  DOUBLE PRECISION[] NOT NULL DEFAULT '{}',
    last_active_ts TIMESTAMPTZ,
    current_weight DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at     TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 6. epoch_scores
-- ============================================================
CREATE TABLE epoch_scores (
    epoch_num        BIGINT           NOT NULL,
    block_height     BIGINT           NOT NULL,
    validator_id     TEXT             NOT NULL,
    accuracy         DOUBLE PRECISION NOT NULL,
    domain_score     DOUBLE PRECISION NOT NULL,
    recency_score    DOUBLE PRECISION NOT NULL,
    corr_score       DOUBLE PRECISION NOT NULL,
    raw_weight       DOUBLE PRECISION NOT NULL,
    capped_weight    DOUBLE PRECISION NOT NULL,
    normalized_weight DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (epoch_num, validator_id)
);

-- ============================================================
-- 7. domains
-- ============================================================
CREATE TABLE domains (
    domain_tag  TEXT             PRIMARY KEY,
    description TEXT,
    decay_rate  DOUBLE PRECISION NOT NULL DEFAULT 0.005,
    created_at  TIMESTAMPTZ      NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 8. agents
-- ============================================================
CREATE TABLE agents (
    agent_id      TEXT        PRIMARY KEY,
    display_name  TEXT,
    organization  TEXT,
    domains       TEXT[],
    registered_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ============================================================
-- 9. domain_registry (federation ACL)
-- ============================================================
CREATE TABLE IF NOT EXISTS domain_registry (
    domain_name     TEXT PRIMARY KEY,
    owner_agent_id  TEXT NOT NULL,
    parent_domain   TEXT,
    description     TEXT,
    created_height  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 10. access_grants
-- ============================================================
CREATE TABLE IF NOT EXISTS access_grants (
    id              SERIAL PRIMARY KEY,
    domain          TEXT NOT NULL,
    grantee_id      TEXT NOT NULL,
    granter_id      TEXT NOT NULL,
    access_level    SMALLINT NOT NULL DEFAULT 1,
    expires_at      TIMESTAMPTZ,
    revoked_at      TIMESTAMPTZ,
    created_height  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(domain, grantee_id, created_height)
);
CREATE INDEX IF NOT EXISTS idx_access_grants_grantee ON access_grants(grantee_id) WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_access_grants_domain ON access_grants(domain) WHERE revoked_at IS NULL;

-- ============================================================
-- 11. access_requests
-- ============================================================
CREATE TABLE IF NOT EXISTS access_requests (
    request_id      TEXT PRIMARY KEY,
    requester_id    TEXT NOT NULL,
    target_domain   TEXT NOT NULL,
    justification   TEXT,
    status          TEXT NOT NULL DEFAULT 'pending',
    created_height  BIGINT NOT NULL,
    resolved_height BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 12. access_logs
-- ============================================================
CREATE TABLE IF NOT EXISTS access_logs (
    id              SERIAL PRIMARY KEY,
    agent_id        TEXT NOT NULL,
    domain          TEXT NOT NULL,
    action          TEXT NOT NULL,
    memory_ids      TEXT[],
    block_height    BIGINT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_access_logs_agent ON access_logs(agent_id);
CREATE INDEX IF NOT EXISTS idx_access_logs_domain ON access_logs(domain);

-- ============================================================
-- 13. organizations
-- ============================================================
CREATE TABLE IF NOT EXISTS organizations (
    org_id          TEXT PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT,
    admin_agent_id  TEXT NOT NULL,
    created_height  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- ============================================================
-- 14. org_members
-- ============================================================
CREATE TABLE IF NOT EXISTS org_members (
    org_id          TEXT NOT NULL,
    agent_id        TEXT NOT NULL,
    clearance       SMALLINT NOT NULL DEFAULT 1,
    role            TEXT NOT NULL DEFAULT 'member',
    created_height  BIGINT NOT NULL,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    removed_at      TIMESTAMPTZ,
    PRIMARY KEY (org_id, agent_id)
);
CREATE INDEX IF NOT EXISTS idx_org_members_agent ON org_members(agent_id) WHERE removed_at IS NULL;

-- ============================================================
-- 15. federations
-- ============================================================
CREATE TABLE IF NOT EXISTS federations (
    federation_id    TEXT PRIMARY KEY,
    proposer_org_id  TEXT NOT NULL,
    target_org_id    TEXT NOT NULL,
    allowed_domains  TEXT[],
    max_clearance    SMALLINT NOT NULL DEFAULT 2,
    expires_at       TIMESTAMPTZ,
    requires_approval BOOLEAN NOT NULL DEFAULT false,
    status           TEXT NOT NULL DEFAULT 'proposed',
    created_height   BIGINT NOT NULL,
    approved_height  BIGINT,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    revoked_at       TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_federations_proposer ON federations(proposer_org_id) WHERE status = 'active';
CREATE INDEX IF NOT EXISTS idx_federations_target ON federations(target_org_id) WHERE status = 'active';

-- ============================================================
-- 16. departments
-- ============================================================
CREATE TABLE IF NOT EXISTS departments (
    dept_id          TEXT NOT NULL,
    org_id           TEXT NOT NULL,
    dept_name        TEXT NOT NULL,
    description      TEXT,
    parent_dept      TEXT,
    created_height   BIGINT NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (org_id, dept_id)
);
CREATE INDEX IF NOT EXISTS idx_departments_org ON departments(org_id);

-- ============================================================
-- 17. dept_members
-- ============================================================
CREATE TABLE IF NOT EXISTS dept_members (
    org_id           TEXT NOT NULL,
    dept_id          TEXT NOT NULL,
    agent_id         TEXT NOT NULL,
    clearance        SMALLINT NOT NULL DEFAULT 1,
    role             TEXT NOT NULL DEFAULT 'member',
    created_height   BIGINT NOT NULL,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    removed_at       TIMESTAMPTZ,
    PRIMARY KEY (org_id, dept_id, agent_id)
);
CREATE INDEX IF NOT EXISTS idx_dept_members_agent ON dept_members(agent_id) WHERE removed_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_dept_members_dept ON dept_members(org_id, dept_id) WHERE removed_at IS NULL;

-- Add classification column to memories table
ALTER TABLE memories ADD COLUMN IF NOT EXISTS classification SMALLINT NOT NULL DEFAULT 1;

-- Add allowed_depts column to federations table
ALTER TABLE federations ADD COLUMN IF NOT EXISTS allowed_depts TEXT[];

-- ============================================================
-- Indexes
-- ============================================================
CREATE INDEX idx_memories_domain ON memories (domain_tag);
CREATE INDEX idx_memories_status ON memories (status);

-- HNSW index for vector similarity search
CREATE INDEX idx_memories_embedding_hnsw ON memories
    USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 200);

-- ============================================================
-- Seed default domains
-- ============================================================
INSERT INTO domains (domain_tag, decay_rate) VALUES
    ('crypto',               0.001),
    ('vuln_intel',           0.01),
    ('challenge_generation', 0.005),
    ('solver_feedback',      0.005),
    ('calibration',          0.005),
    ('infrastructure',       0.005);
