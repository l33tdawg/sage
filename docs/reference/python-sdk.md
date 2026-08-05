Verified against SDK source for SAGE v11.17.8. Package: sage-agent-sdk.

# SAGE Python SDK Reference

**Package:** `sage-agent-sdk` **Version:** 11.17.8
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

The shipped desktop REST listener binds IPv4 loopback at
`127.0.0.1:8080`. `http://localhost:8080` above is only a client-side loopback
hostname alias; it does not expose SAGE to the LAN, and the SDK preserves the
URL rather than rewriting it. If a host resolves `localhost` only to an
unbound IPv6 `::1`, use `http://127.0.0.1:8080` explicitly. Pointing
`base_url` at a LAN address does not confer CEREBRUM authority: operator routes
still require a direct loopback peer and loopback `Host` (plus the unlocked
local session where required).

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

The SDK always sends a fresh nonce. Hand-written clients must do the same. The
generic REST verifier temporarily recognizes the historical nonce-less
signature shape, but exact message, acknowledgement, receipt, and delegated
governance actions require nonce-bound proof and reject legacy signing.

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

`SageClient` exposes 84 public operations and is synchronous (backed by
`httpx.Client`). `AsyncSageClient` exposes the same 84 operations as
coroutines, plus its async-only `close()` method, for 85 public methods total
(backed by `httpx.AsyncClient`). Apart from that lifecycle method, async
signatures match their sync counterparts — just `await` them.

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
    domain_tag: str | None,
    confidence: float,
    embedding: list[float] | None = None,
    knowledge_triples: list[KnowledgeTriple] | None = None,
    parent_hash: str | None = None,
    tags: list[str] | None = None,
    classification: int | None = None,
    provider: str | None = None,
    task_status: TaskStatus | str | None = None,
    linked_memories: list[str] | None = None,
    idempotency_key: str | None = None,
) -> MemorySubmitResponse
```

`POST /v1/memory/submit`

Submits a memory transaction and waits for `broadcast_tx_commit`. A successful
response is already on-chain even though the governed memory lifecycle remains
`status="proposed"`.

- `memory_type`: `"fact"` | `"observation"` | `"inference"` | `"task"` (or `MemoryType` enum)
- `confidence`: `0.0–1.0`
- `embedding`: precomputed vector (768-dim for nomic-embed-text). Omit to let the server embed on-chain (requires Ollama on the node).
- `knowledge_triples`: structured subject/predicate/object triples; `object_` field has alias `object` on the wire (source: `models.py:47`).
- `tags`: up to 32 labels of 128 UTF-8 bytes each, queryable via `query(tags=...)`. Above app-v20 they are normalized into the signed transaction; scoped-domain tags are also AppHash-covered and restored during projection rebuild. Ordinary-domain tags remain node-local.
- `classification`: per-record clearance level. When omitted, the field is excluded from the wire payload via `model_dump(exclude_none=True)` and the server stores the memory as PUBLIC (0) (source: `client.py:192`, `models.py:81`).
- `domain_tag=None`: permitted only for an app-v23 task; the node resolves the
  caller's currently committed owned home domain. Non-task omission is rejected
  by the SDK before the request is sent.
- `task_status`: new tasks accept only `planned` (or omission).
- `idempotency_key`: app-v23 task identity, 1–128 visible ASCII bytes without
  spaces. If omitted, the node derives a permanent semantic key from the exact
  signed agent, resolved domain, and content. Supply a new explicit key only for
  an intentional recurring occurrence.
- App-v23 task creation rejects `knowledge_triples` and `linked_memories`;
  create links after the task receipt is confirmed.

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

Returns `MemorySubmitResponse(memory_id, tx_hash, status, task_status,
committed, committed_height, projection_confirmed, retryable, message,
idempotency_key, idempotent_replay, embedding_provider, embedding_queued)`.

- HTTP `201`: a new commit; app-v23 task projection confirmed.
- HTTP `200`: confirmed permanent-key replay; no new task. The response carries
  the original receipt and current task status, including terminal state.
- HTTP `202`: transaction committed but exact task projection unconfirmed.
  `committed=True`, `projection_confirmed=False`, and `retryable=False`;
  reconcile that exact `memory_id` and do not resubmit.

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
Under app-v23, query/search/hybrid authorization examines at most 8,192 raw
candidates per node request. A broader result raises `SageValidationError`
(HTTP `422`); narrow domain/provider/tag/status filters instead of retrying the
same request.

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

- `expansions`: at most 8 `{"query": str, "embedding": list[float]}`
  paraphrase/entity/temporal variants. SAGE runs hybrid recall per variant and
  fuses across all via RRF. The server counts submitted array entries before
  skipping blanks, and embeddings must use the same model as the primary vector
  (source: `client.py:256`).
- Server respects `SAGE_RERANK_ENABLED` / `SAGE_RERANK_URL` env vars if configured; otherwise plain RRF.
- Under app-v23, one 8,192 raw live-authorization-candidate budget is shared by
  the primary query, every expansion, and every text/vector store leaf in the
  node request. It is not a per-variant or per-leaf allowance. HTTP `422` means
  reduce expansions or narrow domain/provider/tag/status filters.
- Governed leaf, authorization-budget, and decay-floor failures fail the whole
  call. The SDK raises the mapped `SageValidationError`/server error and does
  not receive a `200` response containing partial RRF results.

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

Returns `MemoryListResponse(memories, total, limit, offset, has_more,
total_exact, filtered)`. Under app-v23 authorization runs before visible
offset/limit. While `has_more=True`, `total` is an authorization-safe visible
lower bound and `total_exact=False`; it becomes exact only when the visible
stream is exhausted. Denied/raw counts are never exposed. App-v23 caps visible
`offset` at 7,900 and every authorization candidate walk at 8,192 raw records.
A broader request raises `SageValidationError` for HTTP `422`; narrow the
domain/provider/tag/status/agent filters or page sequentially.

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

Time-bucketed memory counts. `bucket`: `"hour"` | `"day"` | `"week"`.
`from_time`/`to_time` are ISO 8601 strings sent as `from`/`to` query params.
Before app-v23 the historical no-domain aggregate is global. App-v23 applies
central live record disclosure before counting, so every bucket contains only
currently visible records; unavailable authorization state returns `503`.
App-v23 requires RFC3339 bounds and limits a call to 31 days and 8,192 raw
candidates; overly wide/dense requests return `422`.

Returns `TimelineResponse(buckets: list[TimelineBucket], total: int)` where each
bucket has `period`, `count`, `domain`, and `total` is the sum of the currently
visible bucket counts.

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

`POST /v1/memory/{memory_id}/vote`. This is a local validator-operator
override, not an ordinary agent vote. The client key must be the configured
governance operator before app-v23, or the current CEREBRUM Root/current local
Admin on localhost after app-v23. The node must have its live validator key.

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

Submits the challenge transaction and waits for commit. Under legacy/app-v17
rules a personal/one-holder domain deprecates immediately. Post-app-v21, `k=0`
still resolves immediately, while `k>0` parks as `challenged` until `k+1`
distinct challengers accrue—even on a one-holder domain. The server substitutes
a default reason when none is supplied.

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
app-v17-activated chain and an open challenge. Legacy app-v17 challenges use
current modify authorization, with the original challenger always allowed to
withdraw. An app-v21 weighted round can be reinstated only by an identity in its
snapshotted electorate. Returns
`{"message": ..., "tx_hash": ..., "status": "committed"}`.

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

Returns only open tasks explicitly assigned to the authenticated agent ID as
`TaskListResponse(tasks: list[TaskRecord], total)`. Provider is an optional
authoring-client filter only when no agent identity is present; it never widens
an authenticated agent's ownership scope. Assignment is necessary but not
sufficient: every row must also pass current live domain/group/grant and
classification authority. `total` is the visible returned count. Each
`TaskRecord` has `memory_id`, `content`, `domain_tag`, `task_status`,
`confidence_score`, `created_at`, `assignee`, `task_picked_up_by`, and
`task_picked_up_at`. Current-generation Admin use is localhost-only; Root and
historical Root are never assignees. Manager group Modify does not widen exact
assignment.

---

#### `update_task_status()`

```python
update_task_status(memory_id: str, task_status: str) -> dict
```

`PUT /v1/memory/{memory_id}/task-status`

SDK callers may send `"in_progress"` | `"done"` | `"dropped"` only when the
task's assignee exactly matches their verified agent ID and current domain
write plus classification/read authority still holds. `planned`, unassigned
pickup, and terminal reopen are local CEREBRUM operator actions and return HTTP
403/409 here.

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

Registers the identity's public key on-chain. On an app-v23/app-v25 node,
registration is discoverability—not self-promotion. A new ordinary identity
is a Member pending CEREBRUM review unless it is a verified first-party
bootstrap enrollment. `manager` and `admin` are Root-governed local roles;
supplying a stronger role in a self-registration request does not grant it.
Use `get_profile()` immediately after registration to see the caller's own
`registration_status`, `enrollment_status`, hard capability mask, approved
home domain, and whether CEREBRUM action is required.

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

`name` and `boot_bio` are independent partial-update fields. Leaving either
argument as `None` omits it from the request and preserves its current canonical
value; updating only the display name does not erase the boot bio, and updating
only the boot bio does not erase the display name.

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
`on_chain_height`. The SDK v11.16.2 also preserves the additive app-v23
caller-standing fields: `role`, `profile`, `home_domain`,
`enrollment_status`, `registration_status`, `approval_required`, `clearance`,
`capabilities`, `can_read`, `can_write`, and `access_scope`. All are optional
for compatibility with older SAGE servers. The `can_*` fields describe the
caller on its approved home domain; they do not claim authority over every
domain.

App-v25 historical repair is deliberately not an SDK mutation API. SAGE
repairs sound historical evidence automatically in governed batches; local
CEREBRUM Root alone can retry or explicitly deprecate an unresolved snapshot
through the loopback recovery screen. SDK clients should surface standing and
normal write/read errors, never fabricate or re-submit a repair envelope.

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
list_agents() -> dict
```

`GET /v1/agents`

The SDK signs this request. Since v11.16/app-v23, the server returns only the
active ordinary local roster visible through the pipeline identity boundary;
the endpoint is no longer an unsigned full-directory read.

#### `agent_directory()` / `lookup_agents()`

```python
agent_directory() -> AgentDirectoryResponse
lookup_agents(name: str, limit: int = 20) -> AgentLookupResponse
```

Signed `GET /v1/agents/directory` returns the minimal active local recipient
projection without RBAC or memory totals. Signed `GET /v1/agents/lookup`
performs a bounded literal name search and adds server-owned `match_kind`
(`exact` or `substring`). These rows are recipient-discovery metadata, never
online, reachability, delivery, or read evidence. Federation-wide directory
composition remains an MCP broker operation because it requires live
peer-authenticated, caller-specific relation checks.

---

#### `owned_domains()`

```python
owned_domains(cursor: str | None = None, limit: int = 50) -> OwnedDomainPage
```

Signed `GET /v1/agent/me/domains/owned`. This is the authoritative paged set of
domains whose current owner is the caller; it does not scan memories or load a
global roster. Continue with `next_cursor` until `has_more` is false. The
readable/writable arrays returned by `sage_status`/`GET /v1/agent/me/domains`
are intentionally bounded policy samples, not this complete ownership set.

Current local policy is an atomic role/profile/clearance/home-domain operation
performed by Root/Admin through loopback CEREBRUM. The SDK deliberately has no
per-agent permission mutation shortcut because an ordinary agent cannot
self-promote or bypass that governed workflow. App-v26 Access Group membership
and `member_authority` are governed by the same loopback-only boundary and are
not SDK methods; see
[`concepts/app-v26-access-groups.md`](concepts/app-v26-access-groups.md).

#### `domain_access_sample()`

```python
domain_access_sample() -> AgentDomainAccessSample
```

Signed `GET /v1/agent/me/domains`. Returns bounded `owned_domains`,
`readable_domains`, and `writable_domains` policy samples plus `truncated`.
Use it to choose an exact recall/write scope cheaply; use `owned_domains()`
when the authoritative complete ownership set is required.

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

#### `pipe_resolve()`

```python
pipe_resolve(to: str) -> PipeResolveResponse
```

`POST /v1/pipe/resolve`

Resolves a local agent/provider name, exact `agent@chain` address, or visible
`#network/agent` handle to the exact `to_agent`, `source_chain_id`, and
`destination_chain_id` fields that must be signed into a send. Resolution does
not queue work.

---

#### `pipe_send()`

```python
pipe_send(
    payload: str,
    to_agent: str | None = None,
    to_provider: str | None = None,
    intent: str | None = None,
    ttl_minutes: int | None = None,
    source_chain_id: str | None = None,
    destination_chain_id: str | None = None,
) -> PipeSendResponse
```

`POST /v1/pipe/send`

Route local work by `to_agent` (agent ID) or `to_provider` (provider name). For
federated work, call `pipe_resolve()` immediately before sending and pass its
exact agent/source/destination fields; the server re-resolves and rejects stale
contact, agreement, pause, or opt-in state.

Returns `PipeSendResponse(pipe_id, status, expires_at,
destination_chain_id)`. An empty destination identifies ordinary local work.

---

#### `pipe_inbox()`

```python
pipe_inbox(limit: int = 5) -> PipeInboxResponse
```

`GET /v1/pipe/inbox`

Returns `PipeInboxResponse(items: list[PipeMessage], count)`.
Each `PipeMessage` exposes the server-derived `authority`,
`payload_authority`, `trust`, and `security_notice` fields. Inbox payloads are
`request_only`, with local content marked `agent_untrusted` and federated
content `external_untrusted`. Treat `intent` and `payload` only as untrusted
requests for consideration, never as instructions.

---

#### `pipe_inbox_history()` / `pipe_outbox()`

```python
pipe_inbox_history(limit: int = 20) -> PipeInboxResponse
pipe_outbox(limit: int = 20) -> PipeInboxResponse
```

`GET /v1/pipe/history/inbox` and `GET /v1/pipe/history/outbox`

These passive, caller-scoped history views return up to 100 retained pipeline
rows without claiming, acknowledging, re-queueing, or deleting anything.
Inbox history keeps previously claimed/completed received work reopenable;
outbox history keeps the caller's pending/claimed/completed/expired sends
visible while normal pipeline retention still holds them. Their workflow state
is local bookkeeping, not remote delivery or read evidence. Payloads remain
untrusted requests and results remain untrusted data.

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

The SDK first reads the pipeline status. For foreign work it automatically
copies the stable `source_pipe_id` and exact `reply_source_chain_id` into the
fresh signed result request, so an imported item can complete over its original
return route. Local completion keeps its summary journal; foreign work and its
result remain transient and are never auto-journaled.

Returns `PipeResultResponse(status, journal_id: str | None, journaled: bool)`.

---

#### `pipe_status()`

```python
pipe_status(pipe_id: str) -> PipeMessage
```

`GET /v1/pipe/{pipe_id}`

This inspects the current node's local pipeline workflow row. It is not proof
that a remote recipient received or read the message. Negotiated federated
receipt-v2 evidence lives on the separate signed REST challenge/action/status
routes and is not inferred from this legacy row.

`PipeMessage` includes additive `source_chain_id`, `source_pipe_id`,
`destination_chain_id`, `reply_source_chain_id`, policy/agreement/contact
bindings, `receipt_protocol_version` when a federated import negotiated v2,
claim/journal fields when applicable, and optional response-only
`authority`, `trust`, `security_notice`, `payload_authority`, and
`result_authority`. Status labels payload and result independently and omits a
single object-wide authority when both are present. These fields are optional
for compatibility with older SAGE nodes.

---

#### `pipe_results()`

```python
pipe_results(limit: int = 5) -> PipeInboxResponse
```

`GET /v1/pipe/results`

Lists completed (result-submitted) pipeline messages.
The results endpoint and result field are `data_only`; the original payload,
when present, remains explicitly `request_only`. Both local and foreign agent
results are untrusted content, not instructions.

---

#### `pipe_updates()`

```python
pipe_updates(limit: int = 5) -> PipeDeliveryUpdatesResponse
```

`GET /v1/pipe/updates`

Atomically claims one-shot, payload-free terminal delivery notices for
federated sends and results signed by this local agent. `last_error` may include
peer-originated text and must be treated as untrusted data. Each
`PipeDeliveryUpdate` has optional `authority`, `trust`, and `security_notice`
fields; v11.14.1+ returns `notification_only` / `untrusted_metadata`. The fields
remain optional so the client can parse responses from older nodes.

---

### Canonical local Messages (v11.17)

These methods share the existing local pipeline inbox but add durable
idempotency, exact receive-batch replay, exact-recipient read evidence, and a
payload-free sender status projection. They are same-node only. Federated
delivery/read evidence is a separate capability-negotiated receipt-v2 REST
protocol; it must never be inferred from these methods or from `pipe_status()`.

#### `message_send()`

```python
message_send(
    to_agent: str,
    payload: str,
    idempotency_key: str,
    intent: str | None = None,
    ttl_minutes: int | None = None,
) -> MessageSendResponse
```

`POST /v1/messages`. The idempotency key is scoped to the signed sender. An
exact retry returns the original `message_id`; reusing the key for different
content is HTTP 409.

#### `messages_receive()`

```python
messages_receive(receive_token: str, limit: int = 5) -> MessageReceiveResponse
```

`POST /v1/messages/receive`. The caller-supplied token persists one exact
ordered claimed batch. Retrying the same caller/token/limit replays that batch
instead of claiming later messages.

#### `message_reply()` / `message_mark_read()` / `messages_mark_read_batch()`

```python
message_reply(message_id: str, result: str) -> MessageActionResponse
message_mark_read(message_id: str) -> MessageActionResponse
messages_mark_read_batch(message_ids: list[str]) -> dict
```

Only the exact recipient that previously received the message through the
canonical batch API may reply or acknowledge it as read. Repeating the same
action is idempotent; a different second reply conflicts.

Use `messages_mark_read_batch()` for up to 20 already-fetched messages. It is
one signed request with independent ordered outcomes, avoiding one HTTP call
per inbox item.

#### Federated receipt-v2 helpers

`pipe_receipt_challenge()`, `pipe_receipt_record()`, their batch variants, and
`pipe_receipt_status()` expose the capability-gated payload-free receipt-v2
routes. Pass the complete challenge response directly to
`pipe_receipt_record()`; the SDK extracts and signs the immutable inner body.
Likewise, pass the ready items returned by `pipe_receipt_challenge_batch()` to
`pipe_receipt_record_batch()`; the SDK creates a separate exact-path/body proof
for every event before signing the aggregate transport request. A queued
receipt is local durable transport state; only the sender-only status
projection is evidence of confirmed remote claim/read.

#### `message_status()`

```python
message_status(message_id: str) -> MessageStatusResponse
```

`GET /v1/messages/{message_id}/status`. Only the exact sender receives this
payload-free metadata projection. `transport_status`, `read_status`, and
`workflow_status` are independent; a local durable insert is delivered, while
`read_status=confirmed` requires exact-recipient evidence.

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
    expected_owner_id: str = "",
) -> DomainReassignResponse
```

`POST /v1/domain/reassign`

Low-level primitive. Submits the `TxTypeDomainReassign` that **consumes** an already-accepted `domain_reassign` governance proposal. Atomically transfers domain ownership, **purges all existing grants on the domain**, and optionally promotes the domain to shared status. Requires chain admin role. On app-v26, `expected_owner_id` is required and must exactly match the value bound into the accepted proposal.

Returns `DomainReassignResponse(tx_hash: str, purged_grants: int)`.

Gotcha: if the domain was previously marked shared (`open_to_shared=True`), attempting to register or reassign it returns HTTP 403 with detail containing `"shared domain not ownable"` (ABCI code 50).

---

#### `reassign_domain()`  *(v8.0)*

```python
reassign_domain(
    domain: str,
    new_owner_id: str,
    reason: str,
    parent_domain: str = "",
    open_to_shared: bool = False,
    expected_owner_id: str | None = None,
    poll_interval_s: float = 2.0,
    timeout_s: float = 120.0,
) -> DomainReassignResponse
```

`AsyncSageClient.reassign_domain()` has the identical signature and behavior;
await it rather than blocking the event loop while governance progresses.

End-to-end helper: reads the public CEREBRUM health surface to determine the
active chain version. On app-v26, when `expected_owner_id` is omitted, it then
reads the chain-authoritative current owner and binds that value into
`governance_propose(operation="domain_reassign", ...)`; pre-app-v26 requests
retain the historical body. It polls `governance_proposal_detail` every
`poll_interval_s` seconds until status is `"executed"`, then submits the same
binding to `submit_domain_reassign`. A concurrent owner change is rejected
rather than overwritten. Raises `SageAPIError(409)` if the app-v26 owner is
absent or the proposal ends as `rejected`/`expired`/`cancelled`; raises
`SageAPIError(408)` on timeout.

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
    proposer_org_id: str | None = None,
) -> dict
```

`POST /v1/federation/propose`

`proposer_org_id` selects one exact organization membership for multi-org
callers; omission uses the legacy primary organization.
`allowed_domains`/`allowed_depts` default to empty lists on the wire (not
omitted). On app-v22 an empty domain scope denies federation reads, while an
empty department scope is unrestricted. `max_clearance` caps clearance access
regardless of the agent's actual clearance.

---

#### `approve_federation()`

```python
approve_federation(federation_id: str) -> dict
```

`POST /v1/federation/{federation_id}/approve`

At app-v22: global admin and exact member of the stored target organization.

---

#### `revoke_federation()`

```python
revoke_federation(federation_id: str, reason: str = "") -> dict
```

`POST /v1/federation/{federation_id}/revoke`

At app-v22 the caller must be a global admin and exact member of either stored
federation organization.

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
    scope: ScopeActionTemplate | dict[str, Any] | None = None,
) -> GovProposeResponse
```

`POST /v1/governance/propose`

Known `operation` values include `"add_validator"`, `"remove_validator"`,
`"update_power"`, `"domain_reassign"`, `"memory_domain_repair"`,
`"sync_group_action"`, and `"scope_action"` (app-v20).

`payload` encoding (source: `client.py:64`):
- `dict` → JSON-encoded (compact) then base64-encoded onto the wire.
- `bytes` → base64-encoded directly.
- `None` → field omitted entirely.

`domain_reassign` expects a payload dict with keys `domain`, `new_owner_id`,
`parent_domain`, `open_to_shared`, and app-v26 `expected_owner_id`.
`scope_action` should use `scope`; the server canonicalizes the guided template
and owns the zero proposal heights. `scope` and `payload` are mutually
exclusive. Legacy callers may still supply pre-encoded canonical bytes.

Returns `GovProposeResponse(proposal_id, tx_hash, status)`. `proposal_id` is the
deterministic governance-engine ID to pass to `governance_vote()` or
`governance_cancel()`; it is intentionally distinct from the CometBFT
`tx_hash`. Governance mutations must be signed with the target node's configured
operator key so one operator cannot cause another validator to vote. The SDK
already supplies the 8-byte `X-Nonce` required by app-v20 delegated governance.
The target node returns `503` without both its live validator key and configured
governance operator, and `403` when a different valid agent signs the request.

Immediately before each governance mutation, the sync and async clients make
an authenticated `GET /v1/governance/context`. When `app_v20_active` is true,
they copy its `validator_id` and `governance_domain` into the mutation model
*before* request signing. That binds the delegated proof to the exact target
validator and chain. A `409` means the context changed between reads; repeat
the operation so the SDK fetches fresh context and signs a new request. A
pre-v20 server that has no context route (`404`) and an inactive context both
retain the legacy request shape; other context failures remain fail-closed.
The parsed `GovernanceContext` also exposes `validator_active` and the sorted
`active_validators` list (`validator_id`, `voting_power`) loaded from persisted
ABCI state. Those fields let operator tooling compare application membership
with CometBFT after a governed H+2 change or restart; they are not copied into
the mutation body.

Every post-app-v20 delegated proposal requires the configured operator to be a
registered global admin. Vote and cancel authorization deliberately do not:
each validator can keep a distinct node-local operator while its outer
validator key remains the voting-power/ownership principal.

---

#### `governance_propose_scope()`

```python
governance_propose_scope(
    scope: ScopeActionTemplate | dict[str, Any],
    reason: str,
) -> GovProposeResponse
```

Convenience wrapper for `operation="scope_action"`; derives `target_id` from
`scope_id` and sends structured JSON (`client.py:968-980`). For revision 1,
member `active` defaults to true and `joined_revision` may be omitted. Later
revisions must preserve historical join revisions.

---

#### `governance_vote()`

```python
governance_vote(proposal_id: str, decision: str) -> GovVoteResponse
```

`POST /v1/governance/vote`

`decision`: `"accept"` | `"reject"` | `"abstain"`.

Returns `GovVoteResponse(tx_hash, status)`. The HTTP signer authorizes only the
target node's local validator; the on-chain vote and its power remain attributed
to that validator key. The SDK automatically attaches fresh app-v20 context;
the node-local operator does not need the proposal admin key.

---

#### `governance_cancel()`

```python
governance_cancel(proposal_id: str) -> GovCancelResponse
```

`POST /v1/governance/cancel`

The configured operator authorizes cancellation by the local validator that
proposed the record. The SDK automatically attaches fresh app-v20 context; the
operator does not need global-admin authority. Returns
`GovCancelResponse(tx_hash, status)`.

---

#### `list_scopes()`

```python
list_scopes() -> ScopeListResponse
```

`GET /v1/scopes`. Node-operator/admin only. Returns canonical v11.9 scope
heads, exact domain allowlists, assigned integer weights, revision anchors,
pending-ballot drains, and validator-removal blockers.

---

#### `get_scope()`

```python
get_scope(scope_id: str) -> ScopeRecord
```

`GET /v1/scopes/{scope_id}`. Node-operator/admin only. The clients URL-escape
the canonical single-segment scope ID. `ScopeRecord.drain` lists pending memory
IDs and every validator that must not yet be removed from CometBFT.

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
| `corroboration_count` | `int` | Distinct corroborating agents; defaults to 0 for older servers |
| `challenge_count` | `int` | Distinct challenger IDs in the lifetime audit projection; defaults to 0 for older servers |
| `evidence_counts_available` | `bool` | `True` only when both count queries succeeded and no recovery/repair-incomplete marker was detected. When `False`, numeric counts may be lower bounds from pristine state sync or repair and zero is not proof of no evidence. Defaults to `False` for older servers |
| `challenge_round` | `int \| None` | Current open app-v21 round |
| `current_challenger_count` | `int \| None` | Distinct challengers accrued in that round |
| `required_challengers` | `int \| None` | Threshold required to deprecate in that round |
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
| `domain_tag` | `str \| None` | required argument; `None` is valid only for an app-v23 task and is omitted from the wire |
| `confidence_score` | `float` | required |
| `embedding` | `list[float] \| None` | `None` |
| `knowledge_triples` | `list[KnowledgeTriple] \| None` | `None` |
| `parent_hash` | `str \| None` | `None` |
| `tags` | `list[str] \| None` | `None` |
| `provider` | `str \| None` | `None` |
| `task_status` | `TaskStatus \| None` | `None`; new tasks accept only `planned` |
| `linked_memories` | `list[str] \| None` | `None`; rejected for app-v23 task creation |
| `idempotency_key` | `str \| None` | `None`; app-v23 task node derives permanent semantic key |
| `classification` | `int \| None` | `None` → excluded from wire via `exclude_none=True` |

---

### `MemorySubmitResponse`

| Field | Type | Notes |
|---|---|---|
| `memory_id` | `str` | Exact committed ID; reconcile this ID on HTTP 202 |
| `tx_hash` | `str` | Original transaction hash, including replay |
| `status` | `str` | `proposed` or `committed_unconfirmed` |
| `task_status` | `str \| None` | Current task status; a replay may be terminal |
| `committed` | `bool \| None` | Chain commit, not projection confirmation |
| `committed_height` | `int \| None` | Original commit height |
| `projection_confirmed` | `bool \| None` | False only for committed-unconfirmed task response |
| `retryable` | `bool \| None` | False on committed-unconfirmed |
| `message` | `str \| None` | Reconciliation guidance when needed |
| `idempotency_key` | `str \| None` | Explicit or node-derived permanent task key |
| `idempotent_replay` | `bool` | True means no new task was created |
| `embedding_provider` | `str \| None` | Node-selected vector provider |
| `embedding_queued` | `bool` | Node will repair a missing vector |

---

### Recall/list filter and pagination models

`FilterInfo.by` identifies applied authorization paths. Its
`total_before_filter`, `visible`, and `hidden_count` fields exist only for
compatibility with pre-app-v23 nodes; app-v23 omits denied/raw inventory
counts.

`MemoryListResponse` adds `has_more: bool | None`,
`total_exact: bool | None`, and `filtered: FilterInfo | None`. A current
app-v23 response always supplies the first two fields. `MemoryQueryResponse`
also preserves `filtered`; its `total_count` is the visible result count in that
response.

---

### `TaskRecord`

In addition to task content/domain/status/confidence/timestamp,
`TaskRecord` preserves `assignee`, `task_picked_up_by`, and
`task_picked_up_at`. They are optional only for compatibility with older
servers.

---

### `KnowledgeTriple`

| Field | Wire alias | Type |
|---|---|---|
| `subject` | `subject` | `str` |
| `predicate` | `predicate` | `str` |
| `object_` | `object` | `str` |

Pydantic alias: the Python field is `object_`; the JSON key is `object`. Use `KnowledgeTriple(subject=..., predicate=..., object_=...)` in Python (source: `models.py:47`).

---

### `GovernanceContext`

| Field | Type | Notes |
|---|---|---|
| `validator_id` | `str` | Live validator identity returned by the target node |
| `governance_domain` | `str` | Empty pre-v20; canonical 64-hex chain binding when active |
| `app_v20_active` | `bool` | Controls whether the SDK adds both bindings to the mutation |

The context model is loaded internally before mutations; callers normally do
not construct it.

---

### `GovProposeRequest`

| Field | Type | Notes |
|---|---|---|
| `operation` | `str` | Validator operations plus `domain_reassign`, `memory_domain_repair`, `sync_group_action`, and `scope_action` |
| `target_id` | `str` | |
| `target_pubkey` | `str \| None` | Required for `add_validator` |
| `target_power` | `int \| None` | For `update_power` |
| `reason` | `str` | |
| `payload` | `str \| None` | Base64-encoded; `None` omitted on wire |
| `scope` | `ScopeActionTemplate \| None` | Guided canonical scope form; mutually exclusive with `payload` |
| `validator_id` | `str \| None` | SDK-populated from active governance context |
| `governance_domain` | `str \| None` | SDK-populated from active governance context |

`GovVoteRequest` and `GovCancelRequest` carry the same optional
`validator_id`/`governance_domain` pair. They are omitted for pre-v20
compatibility and populated automatically after activation.

---

### `DomainReassignRequest`

| Field | Type | Default |
|---|---|---|
| `domain` | `str` | required |
| `new_owner_id` | `str` | required |
| `proposal_id` | `str` | required |
| `parent_domain` | `str` | `""` |
| `open_to_shared` | `bool` | `False` |
| `expected_owner_id` | `str` | `""` (required by app-v26) |

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
└── SageAPIError           # all 4xx/5xx
    ├── SageAuthError      # HTTP 401/403
    ├── SageNotFoundError  # HTTP 404
    └── SageValidationError # HTTP 422
```

`SageAPIError` attributes: `status_code: int`, `detail: str`,
`error_type: str | None`, `reason_code: str | None`, `remedy: str | None`,
and `retryable: bool | None`. `SageAuthError` is a `SageAPIError`, so callers
may catch it specifically or through the API base class.

Canonical app-v23 memory-write denials have
`error_type="https://sage.dev/errors/domain-write-denied"`, one of the seven
stable `reason_code` values, an exact `remedy`, and `retryable=False`. Branch on
those fields, never on human-readable `detail`. For `missing_write_grant`,
the agent should use its owned domain or, when broader shared management is
intended, a Root/Admin-approved Access Group whose explicit tier is Read +
write or Read + write + modify; it does not claim that CEREBRUM has a direct
level-2 grant editor.

```python
from sage_sdk.exceptions import SageError, SageAPIError, SageAuthError, SageNotFoundError, SageValidationError

try:
    client.get_memory("nonexistent")
except SageNotFoundError as e:
    print(e.detail)         # "memory not found"
except SageAuthError as e:
    print(e.reason_code, e.remedy, e.retryable)
except SageAPIError as e:
    print(e.status_code, e.detail)
```

---

## Method Count Summary

**`SageClient`**: 84 public methods
**`AsyncSageClient`**: 85 public methods (`close` is async-only)

Groups: Health (2), Memory (8), Embeddings (1), Tasks (2), Voting/Validation
(5), Agents (8), Validator (2), Pipeline (10), canonical Messages (5), Access Control (4), Domains (4), Organizations (7), Departments (6),
Federation (5), Governance and scope visibility (8), and async lifecycle (1) =
85 distinct methods across both clients (counting the 84 shared methods once).
