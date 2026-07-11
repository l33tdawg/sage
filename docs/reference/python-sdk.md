Verified against SDK source at SAGE v11.6.1. Package: sage-agent-sdk.

# SAGE Python SDK Reference

**Package:** `sage-agent-sdk` **Version:** 11.6.1
**Requires:** Python 3.10+ | httpx ≥ 0.25 | pydantic ≥ 2.0 | PyNaCl ≥ 1.5

```bash
pip install sage-agent-sdk
```

---

## Getting Started

```python
from sage_sdk import SageClient, AgentIdentity

# Create or load an identity
identity = AgentIdentity.default()          # reads SAGE_IDENTITY_PATH or ~/.sage/agent.key
# identity = AgentIdentity.from_file("my.key")
# identity = AgentIdentity.generate()        # ephemeral

client = SageClient(base_url="http://localhost:8080", identity=identity)

# Register once
client.register_agent(name="my-agent", role="member", provider="python-sdk")

# Propose a memory (SECRET classification)
resp = client.propose(
    content="Prepared statements defeat classic SQLi injection vectors.",
    memory_type="fact",
    domain_tag="security.web",
    confidence=0.9,
    classification=3,    # 3 = SECRET
)
print(resp.memory_id, resp.tx_hash)

# Query it back
results = client.query(
    embedding=client.embed("SQLi prepared statements"),
    domain_tag="security.web",
    top_k=5,
)
for mem in results.results:
    print(mem.memory_id, mem.confidence_score, mem.content[:80])
```

---

## Authentication — `AgentIdentity`

Source: `sdk/python/src/sage_sdk/auth.py`

Every request is signed with Ed25519. The client adds four headers automatically:

| Header | Value |
|---|---|
| `X-Agent-ID` | Hex-encoded Ed25519 verify key (the agent's stable identifier) |
| `X-Signature` | Ed25519 signature over `SHA256(method + " " + path + "\n" + body) ‖ timestamp(8B BE) ‖ nonce(8B)` |
| `X-Timestamp` | Unix epoch seconds |
| `X-Nonce` | 8 random bytes (hex). Prevents replay collisions within the same second. |

For SDK REST calls, `agent_id` is derived entirely from the public key; the server does not issue a REST session token. HTTP MCP transports use their own bearer-token/OAuth flow.

### Constructors

| Method | Signature | Notes |
|---|---|---|
| `AgentIdentity.generate()` | `() → AgentIdentity` | Fresh random keypair. |
| `AgentIdentity.from_seed(seed)` | `(seed: bytes) → AgentIdentity` | Deterministic; `seed` must be 32 bytes. |
| `AgentIdentity.from_file(path)` | `(path: str \| Path) → AgentIdentity` | Reads 32-byte raw seed. |
| `AgentIdentity.default()` | `() → AgentIdentity` | Loads `SAGE_IDENTITY_PATH` env var or `~/.sage/agent.key`; auto-generates + saves if missing. Use for multi-agent setups: set `SAGE_IDENTITY_PATH=~/.sage/identities/agent-01.key`. |

### Instance methods / properties

| Name | Signature | Notes |
|---|---|---|
| `agent_id` | `→ str` | Hex-encoded public verify key. |
| `to_file(path)` | `(path: str \| Path) → None` | Persists 32-byte seed. |
| `sign_request(method, path, body, timestamp)` | `(str, str, bytes \| None, int \| None) → dict[str, str]` | Returns the four auth headers. Called automatically by the client's `_request`. |

---

## Clients

Both clients share an identical public surface. `SageClient` is synchronous (backed by `httpx.Client`); `AsyncSageClient` is async (backed by `httpx.AsyncClient`). All async method signatures are identical to their sync counterparts — just `await` them.

### Constructors

```python
SageClient(
    base_url: str,
    identity: AgentIdentity,
    timeout: float = 30.0,
    ca_cert: str | bool | None = None,
)

AsyncSageClient(
    base_url: str,
    identity: AgentIdentity,
    timeout: float = 30.0,
    ca_cert: str | bool | None = None,
)
```

`ca_cert`:
- `None` (default) — system CA bundle
- `"/path/to/ca.crt"` — custom CA for quorum TLS
- `False` — disable TLS verification (dev only)

Both support context-manager usage. `SageClient` implements `__enter__`/`__exit__`; `AsyncSageClient` implements `__aenter__`/`__aexit__`.

`AsyncSageClient` also exposes `await client.close()` for explicit teardown.

---

## Methods by Group

### Health

| Method | Endpoint | Returns |
|---|---|---|
| `health()` | `GET /health` | `dict` |
| `ready()` | `GET /ready` | `dict` |

Health calls bypass auth-header injection (raw `httpx.Client.get`).

---

### Memory

#### `propose()`

```python
propose(
    content: str,
    memory_type: MemoryType | str,
    domain_tag: str,
    confidence: float,
    embedding: list[float] | None = None,
    knowledge_triples: list[KnowledgeTriple] | None = None,
    parent_hash: str | None = None,
    tags: list[str] | None = None,
    classification: int | None = None,
) -> MemorySubmitResponse
```

`POST /v1/memory/submit`

Submits a BFT memory proposal. The proposal enters consensus; status transitions to `committed` once the quorum approves.

- `memory_type`: `"fact"` | `"observation"` | `"inference"` | `"task"` (or `MemoryType` enum)
- `confidence`: `0.0–1.0`
- `embedding`: precomputed vector (768-dim for nomic-embed-text). Omit to let the server embed on-chain (requires Ollama on the node).
- `knowledge_triples`: structured subject/predicate/object triples; `object_` field has alias `object` on the wire (source: `models.py:47`).
- `tags`: node-local labels, not part of the on-chain tx. Queryable via `query(tags=...)`.
- `classification`: per-record clearance level. When omitted, the field is excluded from the wire payload via `model_dump(exclude_none=True)` and the server stores the memory as PUBLIC (0) (source: `client.py:192`, `models.py:81`).

**Classification levels:**

| Value | Name |
|---|---|
| 0 | PUBLIC |
| 1 | INTERNAL |
| 2 | CONFIDENTIAL |
| 3 | SECRET |
| 4 | TOP SECRET |

**Example — SECRET classification:**
```python
client.propose(
    content="Internal vulnerability details for CVE-2026-9999",
    memory_type="fact",
    domain_tag="audit",
    confidence=0.9,
    classification=3,
)
```

Returns `MemorySubmitResponse(memory_id, tx_hash, status)`.

---

#### `query()`

```python
query(
    embedding: list[float],
    domain_tag: str | None = None,
    min_confidence: float | None = None,
    top_k: int = 10,
    status_filter: str | None = None,
    cursor: str | None = None,
    tags: list[str] | None = None,
) -> MemoryQueryResponse
```

`POST /v1/memory/query`

Vector cosine similarity search.

- `tags`: OR semantics — results must match any of the listed tags (source: `client.py:208`).
- `cursor`: opaque pagination token from `next_cursor`.

Returns `MemoryQueryResponse(results: list[MemoryRecord], next_cursor: str | None, total_count: int)`.

---

#### `hybrid()`

```python
hybrid(
    query: str,
    embedding: list[float],
    domain_tag: str | None = None,
    top_k: int = 10,
    status_filter: str | None = None,
    min_confidence: float | None = None,
    provider: str | None = None,
    tags: list[str] | None = None,
    expansions: list[dict[str, Any]] | None = None,
) -> MemoryQueryResponse
```

`POST /v1/memory/hybrid`

Fuses BM25/FTS5 keyword and vector cosine results via Reciprocal Rank Fusion in a single round-trip. The caller supplies both the text query and the precomputed embedding.

- `expansions`: list of `{"query": str, "embedding": list[float]}` paraphrase/entity/temporal variants. SAGE runs hybrid recall per variant and fuses across all via RRF. Embeddings must use the same model as the primary vector (source: `client.py:241`).
- Server respects `SAGE_RERANK_ENABLED` / `SAGE_RERANK_URL` env vars if configured; otherwise plain RRF.

---

#### `get_memory()`

```python
get_memory(memory_id: str) -> MemoryRecord
```

`GET /v1/memory/{memory_id}`

---

#### `list_memories()`

```python
list_memories(
    limit: int = 50,
    offset: int = 0,
    domain: str | None = None,
    tag: str | None = None,
    provider: str | None = None,
    status: str | None = None,
    sort: str | None = None,
    agent: str | None = None,
) -> MemoryListResponse
```

`GET /v1/memory/list`

All params are query-string filters. `sort` accepted values: `"newest"`, `"oldest"`, `"confidence"`.

Returns `MemoryListResponse(memories, total, limit, offset)`.

---

#### `timeline()`

```python
timeline(
    domain: str | None = None,
    bucket: str | None = None,
    from_time: str | None = None,
    to_time: str | None = None,
) -> TimelineResponse
```

`GET /v1/memory/timeline`

Time-bucketed memory counts. `bucket`: `"hour"` | `"day"` | `"week"`. `from_time`/`to_time` are ISO 8601 strings sent as `from`/`to` query params.

Returns `TimelineResponse(buckets: list[TimelineBucket])` where each bucket has `period`, `count`, `domain`.

---

#### `link_memories()`

```python
link_memories(
    source_id: str,
    target_id: str,
    link_type: str = "related",
) -> MemoryLinkResponse
```

`POST /v1/memory/link`

---

#### `pre_validate()`

```python
pre_validate(
    content: str,
    domain: str,
    memory_type: str = "observation",
    confidence: float = 0.8,
) -> PreValidateResponse
```

`POST /v1/memory/pre-validate`

Dry-run: runs validator checks without committing anything. Returns `PreValidateResponse(accepted: bool, votes: list[PreValidateVote], quorum: str)`.

---

#### `vote()`

```python
vote(
    memory_id: str,
    decision: Literal["accept", "reject", "abstain"],
    rationale: str | None = None,
) -> dict
```

`POST /v1/memory/{memory_id}/vote`

---

#### `challenge()`

```python
challenge(
    memory_id: str,
    reason: str,
    evidence: str | None = None,
) -> dict
```

`POST /v1/memory/{memory_id}/challenge`

---

#### `corroborate()`

```python
corroborate(
    memory_id: str,
    evidence: str | None = None,
) -> dict
```

`POST /v1/memory/{memory_id}/corroborate`

Strengthens confidence of a committed memory.

---

#### `forget()`

```python
forget(
    memory_id: str,
    reason: str | None = None,
) -> dict
```

`POST /v1/memory/{memory_id}/forget`

Submits the challenge transaction and waits for commit. On a personal/one-holder
domain it deprecates immediately; after app-v17 activation on a multi-holder
domain it may instead park as `challenged` pending confirmation or reinstatement.
The server substitutes a default reason when none is supplied.

---

#### `reinstate()`

```python
reinstate(
    memory_id: str,
    reason: str | None = None,
) -> dict
```

`POST /v1/memory/{memory_id}/reinstate`

Submits `TxTypeMemoryReinstate` and waits for consensus commit. Requires an
app-v17-activated chain and an open two-phase challenge. Current modify-verb
holders may reinstate; the original challenger may always withdraw their own
challenge. Returns `{"message": ..., "tx_hash": ..., "status": "committed"}`.

---

### Embeddings

#### `embed()`

```python
embed(text: str) -> list[float]
```

`POST /v1/embed`

Generates a 768-dim vector via the SAGE node's local Ollama. No cloud API calls. Returns the `embedding` field from the response.

---

### Tasks

#### `list_tasks()`

```python
list_tasks(
    domain: str | None = None,
    provider: str | None = None,
) -> TaskListResponse
```

`GET /v1/memory/tasks`

Returns `TaskListResponse(tasks: list[TaskRecord], total)`. Each `TaskRecord` has `memory_id`, `content`, `domain_tag`, `task_status`, `confidence_score`, `created_at`.

---

#### `update_task_status()`

```python
update_task_status(memory_id: str, task_status: str) -> dict
```

`PUT /v1/memory/{memory_id}/task-status`

The API schema accepts `"planned"` | `"in_progress"` | `"done"` | `"dropped"`,
but signed SDK callers are authorized only for `in_progress` (atomic claim/start)
and `done`/`dropped` when they are the current active assignee. `planned` and
terminal reopen are local CEREBRUM operator actions and return HTTP 403 here.

---

### Agents

#### `register_agent()`

```python
register_agent(
    name: str,
    role: str = "member",
    boot_bio: str | None = None,
    provider: str | None = None,
    p2p_address: str | None = None,
) -> AgentRegistration
```

`POST /v1/agent/register`

Registers the identity's public key on-chain. `role`: `"member"` | `"admin"` | `"observer"`.

Returns `AgentRegistration(agent_id, name, registered_name, role, provider, status, on_chain_height, tx_hash)`.

---

#### `update_agent()`

```python
update_agent(
    name: str | None = None,
    boot_bio: str | None = None,
) -> dict
```

`PUT /v1/agent/update`

---

#### `get_profile()`

```python
get_profile() -> AgentProfile
```

`GET /v1/agent/me`

Returns an `AgentProfile`. Core fields: `agent_id`, `poe_weight`, `vote_count`.
Optional fields (present when the server provides them): `display_name`,
`domains`, `accuracy` (global verdict-correctness EWMA), and — since v8.6.0 —
`corr_count` (lifetime corroboration) and `domain_expertise`
(`dict[str, float]`, per-domain expertise keyed by domain tag), plus
`on_chain_height`. The new fields are `Optional` with `None` defaults, so the
model still validates against an older server that omits them.

---

#### `get_agent()`

```python
get_agent(agent_id: str) -> AgentInfo
```

`GET /v1/agent/{agent_id}`

Returns `AgentInfo` — all fields optional except `agent_id`. Key fields: `name`, `role`, `clearance`, `org_id`, `dept_id`, `domain_access`, `provider`, `memory_count`.

---

#### `list_agents()`

```python
list_agents() -> list[dict]
```

`GET /v1/agents`

---

#### `set_agent_permission()`

```python
set_agent_permission(
    agent_id: str,
    clearance: int | None = None,
    domain_access: str | None = None,
    visible_agents: str | None = None,
    org_id: str | None = None,
    dept_id: str | None = None,
) -> dict
```

`PUT /v1/agent/{agent_id}/permission`

Admin only. All kwargs are optional — only supplied fields are sent.

---

### Validator

#### `get_pending()`

```python
get_pending(
    domain_tag: str | None = None,
    limit: int = 20,
) -> PendingMemoriesResponse
```

`GET /v1/validator/pending`

Returns `PendingMemoriesResponse(memories: list[MemoryRecord])`.

---

#### `get_epoch()`

```python
get_epoch() -> EpochInfo
```

`GET /v1/validator/epoch`

Returns `EpochInfo(epoch_num, block_height, scores: list[ValidatorScore])`. Each `ValidatorScore` has `validator_id`, `current_weight`, `vote_count`, `weighted_sum`, `weight_denom`, `expertise_vec`, `last_active_ts`.

---

### Pipeline (Agent-to-Agent Messaging)

#### `pipe_send()`

```python
pipe_send(
    payload: str,
    to_agent: str | None = None,
    to_provider: str | None = None,
    intent: str | None = None,
    ttl_minutes: int | None = None,
) -> PipeSendResponse
```

`POST /v1/pipe/send`

Route by `to_agent` (agent ID) or `to_provider` (provider name). At most one should be set.

Returns `PipeSendResponse(pipe_id, status, expires_at)`.

---

#### `pipe_inbox()`

```python
pipe_inbox(limit: int = 5) -> PipeInboxResponse
```

`GET /v1/pipe/inbox`

Returns `PipeInboxResponse(items: list[PipeMessage], count)`.

---

#### `pipe_claim()`

```python
pipe_claim(pipe_id: str) -> dict
```

`PUT /v1/pipe/{pipe_id}/claim`

Marks the message as claimed. Must be called before `pipe_result`.

---

#### `pipe_result()`

```python
pipe_result(pipe_id: str, result: str) -> PipeResultResponse
```

`PUT /v1/pipe/{pipe_id}/result`

Submits a result for a claimed message. Auto-journaled to memory.

Returns `PipeResultResponse(status, journal_id: str | None)`.

---

#### `pipe_status()`

```python
pipe_status(pipe_id: str) -> PipeMessage
```

`GET /v1/pipe/{pipe_id}`

---

#### `pipe_results()`

```python
pipe_results(limit: int = 5) -> PipeInboxResponse
```

`GET /v1/pipe/results`

Lists completed (result-submitted) pipeline messages.

---

### Access Control

#### `request_access()`

```python
request_access(
    domain: str,
    justification: str = "",
    level: int = 1,
) -> dict
```

`POST /v1/access/request`

`level`: `1` = read, `2` = read+write, `3` = modify on app-v15+ chains.

---

#### `grant_access()`

```python
grant_access(
    grantee_id: str,
    domain: str,
    level: int = 1,
    expires_at: int = 0,
    request_id: str | None = None,
) -> dict
```

`POST /v1/access/grant`

Domain owner only. `expires_at` is a Unix timestamp; `0` means never-expires.
`level`: `1` = read, `2` = read+write, `3` = modify on app-v15+ chains.

---

#### `revoke_access()`

```python
revoke_access(
    grantee_id: str,
    domain: str,
    reason: str = "",
) -> dict
```

`POST /v1/access/revoke`

---

#### `list_grants()`

```python
list_grants(agent_id: str | None = None) -> list[dict]
```

`GET /v1/access/grants/{agent_id}`

Defaults to the calling agent's own ID when `agent_id` is omitted.

---

### Domains

#### `register_domain()`

```python
register_domain(
    name: str,
    description: str = "",
    parent: str = "",
) -> dict
```

`POST /v1/domain/register`

Caller becomes domain owner. Current v11 chains also auto-register the first writer of an unowned, non-shared domain as owner and grant that owner level-2 access; shared domains (`general`, `self`, `meta`, `sage-*`, and domains opened by governance) remain writable by authenticated agents.

---

#### `get_domain()`

```python
get_domain(name: str) -> dict
```

`GET /v1/domain/{name}`

---

#### `submit_domain_reassign()`  *(v8.0)*

```python
submit_domain_reassign(
    domain: str,
    new_owner_id: str,
    proposal_id: str,
    parent_domain: str = "",
    open_to_shared: bool = False,
) -> DomainReassignResponse
```

`POST /v1/domain/reassign`

Low-level primitive. Submits the `TxTypeDomainReassign` that **consumes** an already-accepted `domain_reassign` governance proposal. Atomically transfers domain ownership, **purges all existing grants on the domain**, and optionally promotes the domain to shared status. Requires chain admin role.

Returns `DomainReassignResponse(tx_hash: str, purged_grants: int)`.

Gotcha: if the domain was previously marked shared (`open_to_shared=True`), attempting to register or reassign it returns HTTP 403 with detail containing `"shared domain not ownable"` (ABCI code 50).

---

#### `reassign_domain()`  *(v8.0, SageClient only)*

```python
reassign_domain(
    domain: str,
    new_owner_id: str,
    reason: str,
    parent_domain: str = "",
    open_to_shared: bool = False,
    poll_interval_s: float = 2.0,
    timeout_s: float = 120.0,
) -> DomainReassignResponse
```

No equivalent on `AsyncSageClient`.

End-to-end helper: calls `governance_propose(operation="domain_reassign", ...)`, polls `governance_proposal_detail` every `poll_interval_s` seconds until status is `"executed"`, then calls `submit_domain_reassign`. Raises `SageAPIError(409)` if the proposal ends as `rejected`/`expired`/`cancelled`; raises `SageAPIError(408)` on timeout.

---

### Organizations

#### `register_org()`

```python
register_org(name: str, description: str = "") -> dict
```

`POST /v1/org/register`

Caller becomes permanent admin. Org names are not enforced unique on-chain.

---

#### `get_org()`

```python
get_org(identifier: str) -> dict
```

Routes to `GET /v1/org/{orgID}` when `identifier` is a 32-char lowercase hex string (the server's derived orgID format). Otherwise calls `list_orgs_by_name(identifier)` and returns the single match. Raises `SageAPIError(404)` if no match; raises `ValueError` if multiple orgs share the name — caller must then pass an explicit orgID (source: `client.py:784`).

---

#### `list_orgs_by_name()`

```python
list_orgs_by_name(name: str) -> list[dict]
```

`GET /v1/org/by-name/{name}`

Returns zero, one, or many entries. Each dict has keys `org_id`, `name`, `admin_agent_id`, `description`.

---

#### `list_org_members()`

```python
list_org_members(org_id: str) -> list[dict]
```

`GET /v1/org/{org_id}/members`

---

#### `add_org_member()`

```python
add_org_member(
    org_id: str,
    agent_id: str,
    clearance: int = 1,
    role: str = "member",
) -> dict
```

`POST /v1/org/{org_id}/member`

---

#### `remove_org_member()`

```python
remove_org_member(org_id: str, agent_id: str) -> dict
```

`DELETE /v1/org/{org_id}/member/{agent_id}`

---

#### `set_org_clearance()`

```python
set_org_clearance(org_id: str, agent_id: str, clearance: int) -> dict
```

`POST /v1/org/{org_id}/clearance`

---

### Departments

#### `register_dept()`

```python
register_dept(
    org_id: str,
    name: str,
    description: str = "",
    parent_dept: str = "",
) -> dict
```

`POST /v1/org/{org_id}/dept`

---

#### `get_dept()`

```python
get_dept(org_id: str, dept_id: str) -> dict
```

`GET /v1/org/{org_id}/dept/{dept_id}`

---

#### `list_depts()`

```python
list_depts(org_id: str) -> list[dict]
```

`GET /v1/org/{org_id}/depts`

---

#### `add_dept_member()`

```python
add_dept_member(
    org_id: str,
    dept_id: str,
    agent_id: str,
    clearance: int = 1,
    role: str = "member",
) -> dict
```

`POST /v1/org/{org_id}/dept/{dept_id}/member`

---

#### `remove_dept_member()`

```python
remove_dept_member(org_id: str, dept_id: str, agent_id: str) -> dict
```

`DELETE /v1/org/{org_id}/dept/{dept_id}/member/{agent_id}`

---

#### `list_dept_members()`

```python
list_dept_members(org_id: str, dept_id: str) -> list[dict]
```

`GET /v1/org/{org_id}/dept/{dept_id}/members`

---

### Federation

#### `propose_federation()`

```python
propose_federation(
    target_org_id: str,
    allowed_domains: list[str] | None = None,
    allowed_depts: list[str] | None = None,
    max_clearance: int = 2,
    expires_at: int = 0,
    requires_approval: bool = True,
) -> dict
```

`POST /v1/federation/propose`

`allowed_domains`/`allowed_depts` default to empty lists on the wire (not omitted). `max_clearance` caps clearance access regardless of the agent's actual clearance.

---

#### `approve_federation()`

```python
approve_federation(federation_id: str) -> dict
```

`POST /v1/federation/{federation_id}/approve`

Target org admin only.

---

#### `revoke_federation()`

```python
revoke_federation(federation_id: str, reason: str = "") -> dict
```

`POST /v1/federation/{federation_id}/revoke`

---

#### `get_federation()`

```python
get_federation(federation_id: str) -> dict
```

`GET /v1/federation/{federation_id}`

---

#### `list_federations()`

```python
list_federations(org_id: str) -> list[dict]
```

`GET /v1/federation/active/{org_id}`

---

### Governance

#### `governance_propose()`

```python
governance_propose(
    operation: str,
    target_id: str,
    reason: str,
    target_pubkey: str | None = None,
    target_power: int | None = None,
    payload: dict | bytes | None = None,
) -> GovProposeResponse
```

`POST /v1/governance/propose`

Known `operation` values: `"add_validator"`, `"remove_validator"`, `"update_power"`, `"domain_reassign"` (v8.0+).

`payload` encoding (source: `client.py:64`):
- `dict` → JSON-encoded (compact) then base64-encoded onto the wire.
- `bytes` → base64-encoded directly.
- `None` → field omitted entirely.

`domain_reassign` expects a payload dict with keys `domain`, `new_owner_id`, `parent_domain`, `open_to_shared`.

Returns `GovProposeResponse(proposal_id, tx_hash, status)`.

---

#### `governance_vote()`

```python
governance_vote(proposal_id: str, decision: str) -> GovVoteResponse
```

`POST /v1/governance/vote`

`decision`: `"accept"` | `"reject"` | `"abstain"`.

Returns `GovVoteResponse(tx_hash, status)`.

---

#### `governance_cancel()`

```python
governance_cancel(proposal_id: str) -> GovCancelResponse
```

`POST /v1/governance/cancel`

Proposer only. Returns `GovCancelResponse(tx_hash, status)`.

---

#### `governance_proposals()`

```python
governance_proposals(status: str | None = None) -> GovProposalListResponse
```

`GET /v1/dashboard/governance/proposals`

Returns `GovProposalListResponse(proposals: list[GovProposal])`.

---

#### `governance_proposal_detail()`

```python
governance_proposal_detail(proposal_id: str) -> GovProposalDetailResponse
```

`GET /v1/dashboard/governance/proposals/{proposal_id}`

Returns `GovProposalDetailResponse(proposal: GovProposal, votes: list[GovVote], quorum_progress: dict | None)`.

---

## Models Reference

Source: `sdk/python/src/sage_sdk/models.py`

### Enumerations

**`MemoryType`** (`str` enum): `fact` | `observation` | `inference` | `task`

**`MemoryStatus`** (`str` enum): `proposed` | `validated` | `committed` | `challenged` | `deprecated`

**`TaskStatus`** (`str` enum): `planned` | `in_progress` | `done` | `dropped`

**`PipelineStatus`** (`str` enum): `pending` | `claimed` | `completed` | `expired` | `failed`

---

### `MemoryRecord`

| Field | Type | Notes |
|---|---|---|
| `memory_id` | `str` | |
| `submitting_agent` | `str` | |
| `content` | `str` | |
| `content_hash` | `str` | |
| `memory_type` | `MemoryType` | |
| `domain_tag` | `str` | |
| `confidence_score` | `float` | 0–1 |
| `initial_confidence` | `float \| None` | Stored (undecayed) confidence; `None` for federated results and pre-11.2 servers |
| `status` | `MemoryStatus` | |
| `parent_hash` | `str \| None` | |
| `task_status` | `str \| None` | |
| `classification` | `int \| None` | 0–4 clearance level; `None` means PUBLIC |
| `created_at` | `datetime` | |
| `committed_at` | `datetime \| None` | |
| `deprecated_at` | `datetime \| None` | |
| `votes` | `list \| None` | |
| `corroborations` | `list \| None` | |
| `similarity_score` | `float \| None` | Present in query results |

---

### `MemorySubmitRequest`

| Field | Type | Default |
|---|---|---|
| `content` | `str` | required |
| `memory_type` | `MemoryType` | required |
| `domain_tag` | `str` | required |
| `confidence_score` | `float` | required |
| `embedding` | `list[float] \| None` | `None` |
| `knowledge_triples` | `list[KnowledgeTriple] \| None` | `None` |
| `parent_hash` | `str \| None` | `None` |
| `tags` | `list[str] \| None` | `None` |
| `classification` | `int \| None` | `None` → excluded from wire via `exclude_none=True` |

---

### `KnowledgeTriple`

| Field | Wire alias | Type |
|---|---|---|
| `subject` | `subject` | `str` |
| `predicate` | `predicate` | `str` |
| `object_` | `object` | `str` |

Pydantic alias: the Python field is `object_`; the JSON key is `object`. Use `KnowledgeTriple(subject=..., predicate=..., object_=...)` in Python (source: `models.py:47`).

---

### `GovProposeRequest`

| Field | Type | Notes |
|---|---|---|
| `operation` | `str` | `"add_validator"` / `"remove_validator"` / `"update_power"` / `"domain_reassign"` |
| `target_id` | `str` | |
| `target_pubkey` | `str \| None` | Required for `add_validator` |
| `target_power` | `int \| None` | For `update_power` |
| `reason` | `str` | |
| `payload` | `str \| None` | Base64-encoded; `None` omitted on wire |

---

### `DomainReassignRequest`

| Field | Type | Default |
|---|---|---|
| `domain` | `str` | required |
| `new_owner_id` | `str` | required |
| `proposal_id` | `str` | required |
| `parent_domain` | `str` | `""` |
| `open_to_shared` | `bool` | `False` |

---

### `DomainReassignResponse`

| Field | Type |
|---|---|
| `tx_hash` | `str` |
| `purged_grants` | `int` |

---

### `AgentInfo`

All fields optional except `agent_id`. Notable fields: `name`, `role`, `clearance`, `org_id`, `dept_id`, `domain_access`, `visible_agents`, `provider`, `memory_count`, `first_seen`, `last_seen`.

---

### `EpochInfo`

`epoch_num: int`, `block_height: int`, `scores: list[ValidatorScore]`

Each `ValidatorScore`: `validator_id`, `current_weight`, `vote_count`, `weighted_sum`, `weight_denom`, `expertise_vec`, `last_active_ts`, `updated_at`.

---

## Exceptions

Source: `sdk/python/src/sage_sdk/exceptions.py`

```
SageError                  # base
├── SageAuthError          # HTTP 401/403 — raised directly (not returned) by from_response
└── SageAPIError           # all other 4xx/5xx
    ├── SageNotFoundError  # HTTP 404
    └── SageValidationError # HTTP 422
```

`SageAPIError` attributes: `status_code: int`, `detail: str`, `error_type: str | None`.

**Auth errors:** `SageAPIError.from_response` *raises* `SageAuthError` for 401/403 rather than returning it, so `except SageAPIError` will not catch auth failures — catch `SageAuthError` explicitly.

**ABCI Code 50** (`shared domain not ownable`): surfaces as HTTP 403 / `SageAuthError`. Detect by checking `str(e)` or the detail string for `"shared domain not ownable"` — there is no dedicated exception class (source: `exceptions.py:78`).

```python
from sage_sdk.exceptions import SageError, SageAPIError, SageAuthError, SageNotFoundError, SageValidationError

try:
    client.get_memory("nonexistent")
except SageNotFoundError as e:
    print(e.detail)         # "memory not found"
except SageAuthError as e:
    print(str(e))           # "no write access" or clearance error
except SageAPIError as e:
    print(e.status_code, e.detail)
```

---

## Method Count Summary

**`SageClient`**: 63 public methods
**`AsyncSageClient`**: 63 public methods (`reassign_domain` is sync-only; `close` is async-only)

Groups: Health (2), Memory (8), Embeddings (1), Tasks (2), Voting/Validation
(5), Agents (6), Validator (2), Pipeline (6), Access Control (4), Domains (3
shared + sync-only `reassign_domain`), Organizations (7), Departments (6),
Federation (5), Governance (5), and async lifecycle (1) = 64 distinct methods
across both clients (counting the 62 shared methods once).
