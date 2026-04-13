# SAGE Python SDK

Python client for the SAGE (Sovereign Agent Governed Experience) protocol -- a governed, verifiable institutional memory layer for multi-agent systems.

**Requires Python 3.10+** | **Compatible with SAGE v5.0.1+** | **TLS support since v6.1.0**

## Installation

```bash
# From PyPI
pip install sage-agent-sdk

# From source (development)
git clone https://github.com/l33tdawg/sage.git
cd sage/sdk/python
pip install -e .

# With dev/test dependencies
pip install -e ".[dev]"
```

## Quickstart

```python
from sage_sdk import SageClient, AgentIdentity

# Generate a new agent identity (Ed25519 keypair)
identity = AgentIdentity.generate()

# Save for reuse across sessions
identity.to_file("my_agent.key")

# Connect to a SAGE node
client = SageClient(base_url="http://localhost:8080", identity=identity)

# Register yourself on-chain
reg = client.register_agent(name="my-agent", role="member", provider="python-sdk")
print(f"Registered: {reg.agent_id}")

# Submit a memory
result = client.propose(
    content="Flask web challenges with SQLi require prepared statements bypass",
    memory_type="fact",
    domain_tag="challenge_generation",
    confidence=0.85,
)
print(f"Memory {result.memory_id} submitted (tx: {result.tx_hash})")

# Query by vector similarity
matches = client.query(
    embedding=[0.1] * 768,  # 768-dim (nomic-embed-text)
    domain_tag="challenge_generation",
    min_confidence=0.7,
    top_k=5,
)
for mem in matches.results:
    print(f"  [{mem.status.value}] {mem.content[:80]}")

# Vote on a proposed memory
client.vote(result.memory_id, decision="accept", rationale="Verified correct")
```

## Authentication

SAGE uses Ed25519 keypairs for agent identity. Every API request is signed with the agent's private key.

```python
from sage_sdk import AgentIdentity

# Generate a new identity
identity = AgentIdentity.generate()

# The agent_id is the hex-encoded public key
print(identity.agent_id)  # e.g. "a1b2c3d4..."

# Persist to disk
identity.to_file("agent.key")

# Load from disk
identity = AgentIdentity.from_file("agent.key")

# Create from a known 32-byte seed (deterministic)
identity = AgentIdentity.from_seed(b"\x00" * 32)
```

Request signing is handled automatically by the client. Each request includes three headers:

| Header | Description |
|--------|-------------|
| `X-Agent-ID` | Hex-encoded public verify key |
| `X-Signature` | Ed25519 signature of `SHA256(method + path + body) \|\| timestamp` |
| `X-Timestamp` | Unix timestamp (seconds) |

## Complete API Reference

### Health & Status

```python
# Check node health (unauthenticated)
client.health()      # GET /health
client.ready()       # GET /ready
```

### Agent Registration & Management

Before an agent can participate in the SAGE network, it must register on-chain. Registration creates an immutable identity record tied to the agent's Ed25519 public key.

```python
# Register on-chain (first time only — idempotent)
reg = client.register_agent(
    name="security-analyst",       # Human-readable name
    role="member",                 # "member", "admin", or "observer"
    boot_bio="Analyzes CVEs",      # Optional: agent description
    provider="claude-code",        # Optional: LLM provider identifier
)
# Returns: AgentRegistration(agent_id, name, role, provider, status, tx_hash)

# Update your profile
client.update_agent(name="security-analyst-v2", boot_bio="Updated bio")

# Get your profile (PoE weight, vote count)
profile = client.get_profile()       # GET /v1/agent/me

# Get any registered agent's info
agent = client.get_agent("a1b2c3...")  # GET /v1/agent/{id}
# Returns: AgentInfo(agent_id, name, role, clearance, org_id, dept_id, ...)

# List all registered agents (public info)
agents = client.list_agents()        # GET /v1/agents

# Set agent permissions (admin only)
client.set_agent_permission(
    agent_id="a1b2c3...",
    clearance=2,                     # 0=Public, 1=Internal, 2=Confidential, 3=Secret, 4=TopSecret
    org_id="org-uuid",
    dept_id="dept-uuid",
)
```

### Memory Operations

```python
# Submit a memory proposal
result = client.propose(
    content="The observation text",
    memory_type="fact",           # "fact", "observation", "inference", or "task"
    domain_tag="security",
    confidence=0.9,               # 0.0 - 1.0
    embedding=[0.1, 0.2, ...],    # Optional: precomputed 768-dim vector
    knowledge_triples=[           # Optional: structured knowledge
        KnowledgeTriple(subject="SQLi", predicate="bypasses", object_="prepared_statements")
    ],
    parent_hash="abc123",         # Optional: link to parent memory
)
# Returns: MemorySubmitResponse(memory_id, tx_hash, status)

# Query by vector similarity
results = client.query(
    embedding=[0.1] * 768,       # Required: 768-dim query vector
    domain_tag="security",       # Optional: filter by domain
    min_confidence=0.7,          # Optional: minimum confidence
    top_k=10,                    # Number of results (default: 10)
    status_filter="committed",   # Optional: filter by status
    cursor="abc123",             # Optional: pagination cursor
)
# Returns: MemoryQueryResponse(results, next_cursor, total_count)

# Get a single memory
memory = client.get_memory("550e8400-e29b-41d4-a716-446655440000")

# List memories with filtering and pagination
memories = client.list_memories(
    limit=50,                    # 1-200 (default: 50)
    offset=0,
    domain="security",           # Optional: filter by domain
    status="committed",          # Optional: filter by status
    sort="newest",               # "newest", "oldest", or "confidence"
    agent="a1b2c3...",           # Optional: filter by agent
)
# Returns: MemoryListResponse(memories, total, limit, offset)

# Get memory timeline (time-bucketed counts)
timeline = client.timeline(
    domain="security",           # Optional
    bucket="day",                # "hour", "day", or "week"
    from_time="2026-03-01T00:00:00Z",
    to_time="2026-03-16T00:00:00Z",
)
# Returns: TimelineResponse(buckets=[{period, count, domain}])

# Link related memories
client.link_memories(
    source_id="mem-1",
    target_id="mem-2",
    link_type="related",         # Default: "related"
)

# Dry-run validation (check without submitting)
result = client.pre_validate(
    content="Test content",
    domain="security",
    memory_type="fact",
    confidence=0.9,
)
# Returns: PreValidateResponse(accepted, votes=[{validator, decision, reason}], quorum)
```

### Task Management

Task memories are a special memory type for tracking actionable work items.

```python
# Submit a task
result = client.propose(
    content="Investigate CVE-2026-1234",
    memory_type="task",
    domain_tag="security",
    confidence=0.9,
)

# List open tasks
tasks = client.list_tasks(
    domain="security",           # Optional
    provider="claude-code",      # Optional: filter by provider
)
# Returns: TaskListResponse(tasks=[{memory_id, content, domain_tag, task_status, ...}], total)

# Update task status
client.update_task_status(result.memory_id, "in_progress")  # planned/in_progress/done/dropped
```

### Voting & Validation

```python
# Vote on a proposed memory
client.vote(
    memory_id="550e8400-...",
    decision="accept",            # "accept", "reject", or "abstain"
    rationale="Verified correct",
)

# Challenge a committed memory
client.challenge(
    memory_id="550e8400-...",
    reason="Outdated information",
    evidence="See CVE-2024-XXXX",
)

# Corroborate (strengthen confidence)
client.corroborate(
    memory_id="550e8400-...",
    evidence="Independently verified via testing",
)

# Get memories pending validation
pending = client.get_pending(domain_tag="security", limit=20)

# Get current epoch info and validator scores
epoch = client.get_epoch()
# Returns: EpochInfo(epoch_num, block_height, scores=[{validator_id, current_weight, ...}])
```

### Pipeline (Agent-to-Agent Messaging)

The pipeline enables direct messaging between agents. Messages are routed by agent ID or provider name, with automatic expiry and journaling.

```python
# Send a message to another agent
msg = client.pipe_send(
    payload="Please analyze this CVE",
    to_agent="target-agent-id",  # Route by agent ID
    # OR: to_provider="chatgpt",  # Route by provider name
    intent="analysis",           # Optional: message intent
    ttl_minutes=60,              # Optional: expiry (default: 60, max: 1440)
)
# Returns: PipeSendResponse(pipe_id, status, expires_at)

# Check your inbox
inbox = client.pipe_inbox(limit=5)
for msg in inbox.items:
    print(f"From {msg.from_agent}: {msg.payload}")

# Claim a message for processing
client.pipe_claim(msg.pipe_id)

# Submit your result
result = client.pipe_result(msg.pipe_id, result="Analysis complete: CVE is critical")
# Returns: PipeResultResponse(status, journal_id) — auto-journaled to memory

# Check message status
status = client.pipe_status(msg.pipe_id)

# List completed results
results = client.pipe_results(limit=5)
```

### Embeddings

```python
# Generate embeddings via SAGE's local Ollama (no cloud API calls)
embedding = client.embed("your text here")  # Returns 768-dim float list
```

## Access Control (RBAC)

SAGE uses a hierarchical access control model. **All operations are on-chain BFT transactions — immutable once committed.**

```
Organization
  +-- Department (access boundary — agents in one dept cannot see another's memories)
        +-- Domain (knowledge category — access-controlled)
              +-- Agent (with clearance level 0-4)
```

### Clearance Levels

| Level | Name | Description |
|-------|------|-------------|
| 0 | Public | No registration needed |
| 1 | Internal | Default for registered domains |
| 2 | Confidential | Restricted access |
| 3 | Secret | High-security data |
| 4 | Top Secret | Maximum restriction |

### Setup Order (Critical)

You MUST set up access controls BEFORE agents submit memories. Memories submitted before access controls exist cannot be retroactively restricted.

```
1. Register organization  -->  2. Create departments  -->  3. Register domains
4. Generate agent keypairs  -->  5. Add agents to org + depts  -->  6. Agents operate
```

### Organization Management

```python
# Register an organization (you become permanent admin)
org = admin_client.register_org("Acme Corp", description="AI security research")
org_id = org["org_id"]

# Get organization info
client.get_org(org_id)

# Add agents to the organization
admin_client.add_org_member(org_id, agent_id="a1b2c3...", clearance=2, role="member")

# List organization members
members = admin_client.list_org_members(org_id)

# Update an agent's clearance level
admin_client.set_org_clearance(org_id, agent_id="a1b2c3...", clearance=3)

# Remove an agent from the organization
admin_client.remove_org_member(org_id, agent_id="a1b2c3...")
```

### Department Management

Departments are access boundaries within an organization. Agents in one department cannot see memories in another department.

```python
# Create departments
eng = admin_client.register_dept(org_id, name="Engineering", description="Core eng team")
eng_dept = eng["dept_id"]

security = admin_client.register_dept(org_id, name="Security", description="Security research")
sec_dept = security["dept_id"]

# Sub-departments
crypto = admin_client.register_dept(
    org_id, name="Cryptography", description="Crypto team", parent_dept=sec_dept
)

# List all departments
depts = admin_client.list_depts(org_id)

# Get department info
dept = admin_client.get_dept(org_id, sec_dept)

# Add agents to departments (determines what domains they can access)
admin_client.add_dept_member(org_id, sec_dept, agent_id="a1b2c3...", clearance=2)

# List department members
members = admin_client.list_dept_members(org_id, sec_dept)

# Remove from department
admin_client.remove_dept_member(org_id, sec_dept, agent_id="a1b2c3...")
```

### Domain Registration & Access Control

**Unregistered domains have NO access control** — any agent can read and write. Register all production domains.

```python
# Register domains (you become the domain owner)
admin_client.register_domain(name="security.crypto", description="Cryptographic security")
admin_client.register_domain(name="security.web", description="Web security", parent="security")

# Get domain info
info = admin_client.get_domain("security.crypto")

# Request access to a domain
client.request_access(domain="security.crypto", justification="Need crypto data", level=2)

# Grant access (domain owner only)
admin_client.grant_access(
    grantee_id="a1b2c3...",
    domain="security.crypto",
    level=2,                    # Clearance level (0-4)
    expires_at=0,               # Unix timestamp, 0 = never
)

# Revoke access
admin_client.revoke_access(grantee_id="a1b2c3...", domain="security.crypto", reason="Decommissioned")

# List grants for an agent
grants = admin_client.list_grants(agent_id="a1b2c3...")
```

### Access Rules

- An agent in Dept A can access Dept A's domains but NOT Dept B's domains
- An agent in Org X cannot access ANY memories in Org Y unless a federation agreement exists
- An agent always has access to memories it submitted, regardless of RBAC
- Read-side RBAC is on-chain (consensus-enforced). Write-side RBAC is your responsibility

### Cross-Organization Federation

Federation enables controlled data sharing between separate organizations.

```python
# Org A proposes federation
fed = admin_a.propose_federation(
    target_org_id=org_b_id,
    allowed_depts=["Engineering"],  # Only Org B's Engineering dept gets access
    max_clearance=2,                # Cap at Confidential
    requires_approval=True,
)

# Org B approves
feds = admin_b.list_federations(org_b_id)
admin_b.approve_federation(feds[0]["federation_id"])

# Now: Org B's Engineering can query Org A's data up to clearance 2
# Org B's Research dept still CANNOT see Org A's data

# Revoke when partnership ends
admin_a.revoke_federation(fed["federation_id"], reason="Partnership ended")

# Get federation details
info = admin_a.get_federation(fed["federation_id"])
```

**Federation rules:**
- Both org admins must agree (propose + approve)
- `allowed_depts` restricts which departments in the TARGET org can access your data
- `max_clearance` caps the clearance level regardless of agent's actual clearance
- Revocation is immediate and on-chain

## Write-Side Domain Enforcement

Read-side access control is enforced on-chain. **Write-side enforcement is your responsibility.** Without it, any agent can submit memories tagged to any domain, polluting retrieval for all consumers.

**Pattern 1: ABCI-level enforcement (strongest)**

```go
// In your processMemorySubmit handler:
hasWriteAccess, err := app.badgerStore.HasAccessMultiOrg(
    submit.DomainTag, agentID, 2, blockTime,  // level 2 = write
)
if err != nil || !hasWriteAccess {
    return &abcitypes.ExecTxResult{Code: 13, Log: "no write access"}
}
```

**Pattern 2: Application-layer gatekeeper**

```python
AGENT_DOMAIN_MAP = {
    "designer": ["design.generation", "design.patterns"],
    "evaluator": ["evaluation.calibration"],
}

def validate_submission(agent_name: str, domain_tag: str) -> bool:
    allowed = AGENT_DOMAIN_MAP.get(agent_name, [])
    return any(domain_tag.startswith(prefix) for prefix in allowed)
```

**Pattern 3: Domain prefix convention**

```python
agent_dept = get_agent_department(agent_id)
domain_prefix = domain_tag.split(".")[0]
if agent_dept != domain_prefix:
    raise ValueError(f"Agent in {agent_dept} cannot write to {domain_tag}")
```

## Async Client

For async/concurrent workloads, use `AsyncSageClient` — it has identical methods, all returning awaitables:

```python
import asyncio
from sage_sdk import AsyncSageClient, AgentIdentity

async def main():
    identity = AgentIdentity.generate()
    async with AsyncSageClient(base_url="http://localhost:8080", identity=identity) as client:
        # Register
        await client.register_agent(name="async-agent", provider="python")

        # Submit a memory
        result = await client.propose(
            content="Async observation",
            memory_type="observation",
            domain_tag="testing",
            confidence=0.75,
        )

        # Concurrent queries
        results = await asyncio.gather(
            client.query(embedding=[0.1] * 768, domain_tag="security"),
            client.query(embedding=[0.2] * 768, domain_tag="testing"),
        )

        # Pipeline messaging
        msg = await client.pipe_send(payload="Hello", to_provider="chatgpt")
        inbox = await client.pipe_inbox()

asyncio.run(main())
```

## Models

### MemoryType

```python
MemoryType.fact          # Verified factual knowledge
MemoryType.observation   # Agent-observed data
MemoryType.inference     # Derived conclusion
MemoryType.task          # Actionable work item
```

### MemoryStatus

```python
MemoryStatus.proposed     # Awaiting validation
MemoryStatus.validated    # Passed quorum vote
MemoryStatus.committed    # Finalized on-chain
MemoryStatus.challenged   # Under dispute
MemoryStatus.deprecated   # Superseded or invalidated
```

### TaskStatus

```python
TaskStatus.planned        # Not yet started
TaskStatus.in_progress    # Currently being worked on
TaskStatus.done           # Completed
TaskStatus.dropped        # Abandoned
```

### PipelineStatus

```python
PipelineStatus.pending    # Awaiting claim
PipelineStatus.claimed    # Being processed
PipelineStatus.completed  # Result submitted
PipelineStatus.expired    # TTL exceeded
PipelineStatus.failed     # Processing failed
```

## Error Handling

```python
from sage_sdk.exceptions import (
    SageError,            # Base exception
    SageAPIError,         # Any API error (has status_code, detail)
    SageAuthError,        # 401/403 authentication failure
    SageNotFoundError,    # 404 resource not found
    SageValidationError,  # 422 validation error
)

try:
    memory = client.get_memory("nonexistent-id")
except SageNotFoundError as e:
    print(f"Not found: {e.detail}")
except SageAuthError as e:
    print(f"Auth failed: {e}")
except SageAPIError as e:
    print(f"API error {e.status_code}: {e.detail}")
```

## Configuration

```python
client = SageClient(
    base_url="http://localhost:8080",  # SAGE node URL
    identity=identity,
    timeout=30.0,                      # Request timeout (default: 30s)
    ca_cert=None,                      # TLS CA cert path, False to disable, None for system default
)

# Use as context manager for automatic cleanup
with SageClient(base_url="http://localhost:8080", identity=identity) as client:
    profile = client.get_profile()
```

## TLS Support (v6.5 Quorum Mode)

When connecting to a SAGE node running in quorum mode with encrypted node-to-node communication (TLS), use the `ca_cert` parameter to specify the CA certificate used by the quorum.

```python
from sage_sdk import SageClient, AgentIdentity

identity = AgentIdentity.from_file("my_agent.key")

# Connect to a TLS-enabled SAGE node with the quorum CA certificate
client = SageClient(
    "https://sage-node:8443",
    identity,
    ca_cert="/path/to/ca.crt",
)

# All requests now use the custom CA for TLS verification
profile = client.get_profile()
```

The CA certificate (`ca.crt`) is included in agent bundles generated by `quorum-init` and `quorum-join`. Look for it in your node's data directory (e.g., `~/.sage/quorum/ca.crt`).

### Options

| `ca_cert` value | Behavior |
|-----------------|----------|
| `None` (default) | Standard TLS verification using system CA bundle |
| `"/path/to/ca.crt"` | Verify server certificate against the specified CA |
| `False` | Disable TLS verification entirely (development only) |

```python
# Disable TLS verification for local development (NOT for production)
dev_client = SageClient("https://localhost:8443", identity, ca_cert=False)
```

The async client supports the same parameter:

```python
async with AsyncSageClient("https://sage-node:8443", identity, ca_cert="/path/to/ca.crt") as client:
    await client.health()
```

## Embeddings

SAGE uses 768-dimensional vectors (Ollama `nomic-embed-text`). Three options:

### 1. Direct Ollama (local agents)

```python
import httpx
resp = httpx.post(
    "http://localhost:11434/api/embed",
    json={"model": "nomic-embed-text", "input": "your text"},
    timeout=30.0,
)
embedding = resp.json()["embeddings"][0]
```

### 2. SAGE Embed Endpoint (remote agents)

```python
embedding = client.embed("your text here")  # Uses SAGE's Ollama
```

### 3. Hash Embedding (testing only)

```python
import hashlib, struct

def hash_embed(text: str, dim: int = 768) -> list[float]:
    rounds = (dim * 4 + 31) // 32
    raw = b""
    current = text.encode("utf-8")
    for i in range(rounds):
        current = hashlib.sha256(current + struct.pack(">I", i)).digest()
        raw += current
    return [(struct.unpack(">I", raw[j*4:j*4+4])[0] / 2147483647.5) - 1.0 for j in range(dim)]
```

## Complete API Reference Table

### Memory

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/memory/submit` | `propose()` |
| `POST` | `/v1/memory/query` | `query()` |
| `GET` | `/v1/memory/{id}` | `get_memory()` |
| `GET` | `/v1/memory/list` | `list_memories()` |
| `GET` | `/v1/memory/timeline` | `timeline()` |
| `POST` | `/v1/memory/link` | `link_memories()` |
| `POST` | `/v1/memory/pre-validate` | `pre_validate()` |
| `POST` | `/v1/memory/{id}/vote` | `vote()` |
| `POST` | `/v1/memory/{id}/challenge` | `challenge()` |
| `POST` | `/v1/memory/{id}/corroborate` | `corroborate()` |
| `PUT` | `/v1/memory/{id}/task-status` | `update_task_status()` |
| `GET` | `/v1/memory/tasks` | `list_tasks()` |

### Agent

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/agent/register` | `register_agent()` |
| `PUT` | `/v1/agent/update` | `update_agent()` |
| `GET` | `/v1/agent/me` | `get_profile()` |
| `GET` | `/v1/agent/{id}` | `get_agent()` |
| `PUT` | `/v1/agent/{id}/permission` | `set_agent_permission()` |
| `GET` | `/v1/agents` | `list_agents()` |

### Pipeline

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/pipe/send` | `pipe_send()` |
| `GET` | `/v1/pipe/inbox` | `pipe_inbox()` |
| `PUT` | `/v1/pipe/{id}/claim` | `pipe_claim()` |
| `PUT` | `/v1/pipe/{id}/result` | `pipe_result()` |
| `GET` | `/v1/pipe/{id}` | `pipe_status()` |
| `GET` | `/v1/pipe/results` | `pipe_results()` |

### Validator

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `GET` | `/v1/validator/pending` | `get_pending()` |
| `GET` | `/v1/validator/epoch` | `get_epoch()` |

### Embedding

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/embed` | `embed()` |

### Organization

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/org/register` | `register_org()` |
| `GET` | `/v1/org/{org_id}` | `get_org()` |
| `POST` | `/v1/org/{org_id}/member` | `add_org_member()` |
| `DELETE` | `/v1/org/{org_id}/member/{agent_id}` | `remove_org_member()` |
| `POST` | `/v1/org/{org_id}/clearance` | `set_org_clearance()` |
| `GET` | `/v1/org/{org_id}/members` | `list_org_members()` |

### Department

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/org/{org_id}/dept` | `register_dept()` |
| `GET` | `/v1/org/{org_id}/dept/{dept_id}` | `get_dept()` |
| `GET` | `/v1/org/{org_id}/depts` | `list_depts()` |
| `POST` | `/v1/org/{org_id}/dept/{dept_id}/member` | `add_dept_member()` |
| `DELETE` | `/v1/org/{org_id}/dept/{dept_id}/member/{agent_id}` | `remove_dept_member()` |
| `GET` | `/v1/org/{org_id}/dept/{dept_id}/members` | `list_dept_members()` |

### Domain & Access

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/domain/register` | `register_domain()` |
| `GET` | `/v1/domain/{name}` | `get_domain()` |
| `POST` | `/v1/access/request` | `request_access()` |
| `POST` | `/v1/access/grant` | `grant_access()` |
| `POST` | `/v1/access/revoke` | `revoke_access()` |
| `GET` | `/v1/access/grants/{agent_id}` | `list_grants()` |

### Federation

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `POST` | `/v1/federation/propose` | `propose_federation()` |
| `POST` | `/v1/federation/{id}/approve` | `approve_federation()` |
| `POST` | `/v1/federation/{id}/revoke` | `revoke_federation()` |
| `GET` | `/v1/federation/{id}` | `get_federation()` |
| `GET` | `/v1/federation/active/{org_id}` | `list_federations()` |

### Health

| Method | Endpoint | SDK Method |
|--------|----------|------------|
| `GET` | `/health` | `health()` |
| `GET` | `/ready` | `ready()` |

## Development

```bash
# Install with dev dependencies
pip install -e ".[dev]"

# Run tests
python -m pytest tests/ -v

# Run async tests
python -m pytest tests/test_async_client.py -v
```

## License

Apache 2.0 — see the project root LICENSE file.
