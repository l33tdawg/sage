<!-- Verified against code at SAGE v8.1.1 (commit 2ca50ba). Cite file:line when behavior is non-obvious. -->

# SAGE REST API Reference

## Authentication

All endpoints under `/v1/` (except `GET /v1/agents`) require three headers on every request (`api/rest/middleware/auth.go:81-213`):

| Header | Format | Purpose |
|---|---|---|
| `X-Agent-ID` | 64-char hex Ed25519 pubkey | Identifies the agent |
| `X-Signature` | hex-encoded Ed25519 sig | Signs the canonical payload |
| `X-Timestamp` | unix epoch seconds | Prevents replay |
| `X-Nonce` | hex bytes (optional) | Sub-second replay protection; include for concurrent requests |

**Signed message construction** (`auth.go:156-180`):

```
canonical = METHOD + " " + PATH[?QUERY] + "\n" + BODY
message   = SHA-256(canonical) + bigEndian(timestamp_int64)
```

With nonce: `auth.VerifyRequestWithNonce` appends nonce bytes before the signature check. Without nonce: legacy `auth.VerifyRequest` applies.

**Constraints:**
- Timestamp must be within ±5 minutes of server time (`auth.go:79`).
- Duplicate `(agentID, signature)` pairs within the 5-minute window are rejected (replay cache, `auth.go:27-53`).
- Body is capped at 1 MB before reading for signature verification (`auth.go:143`).
- `X-Agent-ID` is the hex-encoded Ed25519 **public key** (32 bytes = 64 hex chars); it IS the agent identity on-chain.

**Errors** use RFC 7807 `application/problem+json` with `type`, `title`, `status`, `detail` fields.

---

## 1. Memory

### `POST /v1/memory/submit`

Submit a memory for BFT consensus. Blocks until `broadcast_tx_commit` returns (FinalizeBlock completes). Default timeout 60 s; override via `SAGE_TX_COMMIT_TIMEOUT_MS`.

**Request body** (`memory_handler.go:29-45`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `content` | string | yes | Natural-language memory text |
| `memory_type` | string | yes | One of: `fact`, `observation`, `inference`, `task` |
| `domain_tag` | string | yes | Domain label; agent must have write access |
| `confidence_score` | float64 | yes | 0.0–1.0 inclusive |
| `classification` | int | no | 0–4; see table below. **Omitting sends 0 (PUBLIC)** |
| `embedding` | []float32 | no | Precomputed vector; stored off-chain via supplementary cache |
| `knowledge_triples` | []KnowledgeTriple | no | `{subject, predicate, object}` triples |
| `parent_hash` | string | no | SHA-256 hex of parent memory for lineage |
| `task_status` | string | no | For `task` type: `planned`, `in_progress`, `done`, `dropped` |
| `tags` | []string | no | Node-local labels attached post-commit; OR-filter on query/search |
| `provider` | string | no | Stored off-chain only; not on-chain |

**Classification values** (`internal/tx/types.go:84-90`):

| Value | Name | Meaning |
|---|---|---|
| 0 | PUBLIC | Readable by any federated org |
| 1 | INTERNAL | Own org only (default clearance gate) |
| 2 | CONFIDENTIAL | Own org + explicit cross-org grants |
| 3 | SECRET | Own org, specific dept, explicit grant |
| 4 | TOPSECRET | Named agents only, dual-approval |

> **Critical:** An **omitted** `classification` field is deserialized as `0` (PUBLIC) by Go's JSON decoder and is stored as PUBLIC on-chain. This is the intended behavior since v6.8.6 — the prior code silently bumped `0→INTERNAL` at submission time, causing every cross-agent read of a PUBLIC memory to be blocked by the classification gate. (`internal/abci/app.go:960-969`). The codec still defaults old txs without a classification byte to INTERNAL for backward compatibility (`internal/tx/codec.go`), but new submissions from this REST endpoint are stored as-sent.

**Response** (HTTP 201):

```json
{
  "memory_id": "<uuid>",
  "tx_hash": "<hex>",
  "status": "proposed"
}
```

**Auth:** Ed25519 required. Agent must have write access to `domain_tag` if per-domain access control is configured (`memory_handler.go:382-385`). Observer-role agents are rejected.

**curl example:**

```bash
# Compute timestamp and sign with your Ed25519 key (see SDK for helpers)
BODY='{"content":"Go 1.22 dropped support for GOPATH mode","memory_type":"fact","domain_tag":"go-debugging","confidence_score":0.95}'
TS=$(date +%s)
# signature = ed25519_sign(SHA256("POST /v1/memory/submit\n" + BODY) + bigEndian(TS))
curl -X POST http://localhost:8080/v1/memory/submit \
  -H "Content-Type: application/json" \
  -H "X-Agent-ID: <64-hex-pubkey>" \
  -H "X-Signature: <hex-sig>" \
  -H "X-Timestamp: $TS" \
  -d "$BODY"
```

---

### `POST /v1/memory/query`

Vector similarity search. Requires a precomputed embedding.

**Request body** (`memory_handler.go:55-66`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `embedding` | []float32 | yes | Query vector; must match stored embedding dimension |
| `domain_tag` | string | no | Filter to domain |
| `provider` | string | no | Provider filter |
| `min_confidence` | float64 | no | Minimum decayed confidence threshold |
| `status_filter` | string | no | `proposed`, `validated`, `committed`, `challenged`, `deprecated` |
| `top_k` | int | no | Max results; default 10 |
| `cursor` | string | no | Opaque pagination cursor from previous response |
| `tags` | []string | no | OR-filter by tag; SQLite only, ignored on Postgres |

**Response** (HTTP 200):

```json
{
  "results": [
    {
      "memory_id": "<uuid>",
      "submitting_agent": "<hex-pubkey>",
      "content": "...",
      "content_hash": "<hex>",
      "memory_type": "fact",
      "domain_tag": "go-debugging",
      "confidence_score": 0.91,
      "classification": 0,
      "status": "committed",
      "parent_hash": "",
      "created_at": "2026-05-27T09:00:00Z",
      "committed_at": "2026-05-27T09:00:01Z"
    }
  ],
  "total_count": 1,
  "next_cursor": "",
  "filtered": {
    "by": ["rbac_submitting_agents", "classification"],
    "hidden_count": 3
  }
}
```

`confidence_score` in the response is the **decayed** value (time decay + corroboration boost applied server-side), not the raw submitted value.

`filtered` is present when agent-isolation RBAC or per-record classification gates silently hid records. Check the `X-SAGE-Filter-Applied` response header for the same info. Values in `by`: `rbac_submitting_agents`, `classification`.

**Access control stacking** (`memory_handler.go:533-589`): Domain access is checked first (agent's `DomainAccess` policy), then multi-org gate (if domain has a registered owner), then agent-isolation RBAC. These are alternatives — passing domain access disables agent isolation for that call.

**curl example:**

```bash
BODY='{"embedding":[...768 floats...],"domain_tag":"go-debugging","top_k":5}'
curl -X POST http://localhost:8080/v1/memory/query \
  -H "Content-Type: application/json" \
  -H "X-Agent-ID: <pubkey>" \
  -H "X-Signature: <sig>" \
  -H "X-Timestamp: $TS" \
  -d "$BODY"
```

---

### `POST /v1/memory/search`

Full-text search (FTS5/BM25). Same access control as `/v1/memory/query`.

**Request body** (`memory_handler.go:737-744`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | yes | Text search query |
| `domain_tag` | string | no | Domain filter |
| `provider` | string | no | Provider filter |
| `min_confidence` | float64 | no | |
| `status_filter` | string | no | |
| `top_k` | int | no | |
| `tags` | []string | no | OR-filter; SQLite only |

**Response:** Same shape as `/v1/memory/query`. Not available when vault (content encryption) is active — `GET /v1/embed/info` reports `semantic: true` in that case; use `/v1/memory/query` instead.

---

### `POST /v1/memory/hybrid`

Fused FTS5 + vector search via Reciprocal Rank Fusion. Requires at least one of `query` or `embedding`. Supports query expansions for multi-variant recall.

**Request body** (`memory_handler.go:928-946`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | no* | Text query (* at least one of query/embedding required) |
| `embedding` | []float32 | no* | Vector for similarity arm |
| `expansions` | []HybridExpansion | no | Paraphrase variants; each has `query` + `embedding` |
| `domain_tag` | string | no | |
| `provider` | string | no | |
| `min_confidence` | float64 | no | |
| `status_filter` | string | no | |
| `top_k` | int | no | Applied after RRF fusion across variants |
| `tags` | []string | no | |

**Response:** Same shape as `/v1/memory/query`.

---

### `GET /v1/memory/{memory_id}`

Fetch a single memory with votes and corroborations.

**Response** (HTTP 200):

```json
{
  "memory_id": "<uuid>",
  "submitting_agent": "<hex>",
  "content": "...",
  "content_hash": "<hex>",
  "memory_type": "fact",
  "domain_tag": "...",
  "confidence_score": 0.91,
  "classification": 0,
  "status": "committed",
  "created_at": "...",
  "committed_at": "...",
  "votes": [...],
  "corroborations": [...],
  "linked_memories": [...]
}
```

Access control: agent-isolation RBAC + multi-org classification gate apply. Own memories always visible. 403 if agent cannot see the submitter or lacks org clearance for the memory's classification. (`memory_handler.go:1119-1193`)

---

### `GET /v1/memory/list`

Paginated memory list with RBAC agent isolation. Read from off-chain store.

**Query parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `limit` | int | 50 | Max 200 |
| `offset` | int | 0 | |
| `domain` | string | | Filter by domain |
| `tag` | string | | Filter by single tag |
| `provider` | string | | |
| `status` | string | | |
| `sort` | string | | Store-defined sort field |
| `agent` | string | | Filter by submitting agent ID |

**Response:** `{"memories": [...], "total": N, "limit": N, "offset": N, "filtered": {...}}`

---

### `GET /v1/memory/timeline`

Memory creation counts grouped by time bucket. Agent isolation not applied to aggregate counts.

**Query parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `domain` | string | | Filter |
| `bucket` | string | `hour` | Time bucket size |
| `from` | RFC3339 | now-24h | |
| `to` | RFC3339 | now | |

**Response:** `{"buckets": [...]}`

---

### `GET /v1/memory/tasks`

Open task memories (type=`task`, status != `done`/`dropped`).

**Query parameters:** `domain`, `provider`

**Response:** `{"tasks": [{memory_id, content, domain_tag, task_status, confidence_score, created_at}], "total": N}`

---

### `POST /v1/memory/{memory_id}/vote`

Cast a validator vote on a proposed memory.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `decision` | string | yes | `accept`, `reject`, or `abstain` |
| `rationale` | string | no | Human-readable justification |

**Response** (HTTP 200): `{"message": "Vote recorded successfully.", "tx_hash": "<hex>"}`

---

### `POST /v1/memory/{memory_id}/challenge`

Challenge an existing memory. Broadcasts `TxTypeMemoryChallenge`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `reason` | string | yes |
| `evidence` | string | no |

**Response** (HTTP 200): `{"message": "Challenge submitted successfully.", "tx_hash": "<hex>"}`

---

### `POST /v1/memory/{memory_id}/forget`

Semantic alias for challenge (`vote_handler.go:255-325`). Submits a `TxTypeMemoryChallenge` with reason defaulting to `"deprecated by user"` when omitted.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `reason` | string | no |

**Response** (HTTP 200): `{"message": "Memory forgotten.", "tx_hash": "<hex>"}`

---

### `POST /v1/memory/{memory_id}/corroborate`

Corroborate a memory. Raises confidence via decay model.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `evidence` | string | no |

**Response** (HTTP 200): `{"message": "Corroboration recorded successfully.", "tx_hash": "<hex>"}`

---

### `PUT /v1/memory/{memory_id}/task-status`

Update task status for a `task`-type memory (off-chain only, no tx).

**Request body:**

| Field | Type | Required | Values |
|---|---|---|---|
| `task_status` | string | yes | `planned`, `in_progress`, `done`, `dropped` |

**Response** (HTTP 200): `{"memory_id": "...", "task_status": "..."}`

---

### `POST /v1/memory/link`

Link two memories. Off-chain relation, no tx.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `source_id` | string | yes | |
| `target_id` | string | yes | |
| `link_type` | string | no | Defaults to `related` |

**Response** (HTTP 200): `{"source_id": "...", "target_id": "...", "link_type": "..."}`

---

### `POST /v1/memory/pre-validate`

Dry-run the 4 app validators without submitting on-chain. Returns per-validator decisions.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `content` | string | yes |
| `domain` | string | no |
| `type` | string | no |
| `confidence` | float64 | no |

**Response** (HTTP 200):

```json
{
  "accepted": true,
  "quorum": "3/4",
  "votes": [
    {"validator": "...", "decision": "accept", "reason": "..."}
  ]
}
```

Returns 503 if not configured on this node.

---

## 2. Agents / Registration

### `GET /v1/agents`

List all registered agents. **No auth required.**

**Response** (HTTP 200): `{"agents": [...AgentEntry], "total": N}`

---

### `POST /v1/agent/register`

Register agent on-chain. Idempotent — returns existing record if already registered.

**Request body** (`agent_handler.go:20-26`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | yes | Display name |
| `role` | string | no | `admin`, `member`, `observer`; defaults to `member` |
| `boot_bio` | string | no | Agent system prompt / bio |
| `provider` | string | no | e.g. `claude-code`, `cursor` |
| `p2p_address` | string | no | Peer-to-peer address |

**Response (new, HTTP 201):**

```json
{
  "agent_id": "<hex>",
  "name": "...",
  "registered_name": "...",
  "role": "member",
  "provider": "claude-code",
  "status": "registered",
  "tx_hash": "<hex>",
  "on_chain_height": 42
}
```

**Response (existing, HTTP 200):** Same shape with `"status": "already_registered"`. `on_chain_height` is populated on both paths since v6.6.0.

**curl example:**

```bash
BODY='{"name":"my-agent","provider":"claude-code","role":"member"}'
curl -X POST http://localhost:8080/v1/agent/register \
  -H "Content-Type: application/json" \
  -H "X-Agent-ID: <pubkey>" \
  -H "X-Signature: <sig>" \
  -H "X-Timestamp: $TS" \
  -d "$BODY"
```

---

### `PUT /v1/agent/update`

Self-update only. Agent can only update its own name and bio. Broadcasts `TxTypeAgentUpdate`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `name` | string | no |
| `boot_bio` | string | no |

**Response** (HTTP 200): `{"agent_id": "...", "name": "...", "status": "updated", "tx_hash": "..."}`

---

### `GET /v1/agent/me`

Authenticated agent's profile, including the on-chain Proof-of-Experience
quorum-weight factors. Since v8.6.0 the response also exposes the lifetime
corroboration count and per-domain expertise; `accuracy`, `corr_count`, and
`domain_expertise` are read from the authoritative on-chain `vstats:` /
`vstats_domain:` records (not the off-chain mirror).

**Response** (HTTP 200):

```json
{
  "agent_id": "<hex>",
  "display_name": "...",
  "domains": ["go-debugging", "sage-development"],
  "poe_weight": 0.82,
  "vote_count": 127,
  "accuracy": 0.91,
  "corr_count": 34,
  "domain_expertise": { "go-debugging": 0.88, "sage-development": 0.71 },
  "on_chain_height": 42
}
```

- `accuracy` — global verdict-correctness EWMA (the α factor of the quorum weight).
- `corr_count` — lifetime count of votes that matched a terminal verdict (the δ factor). **(v8.6.0+)**
- `domain_expertise` — per-domain verdict-correctness EWMA (the β factor, from `vstats_domain:`), keyed by domain. Only present for domains the agent has actually voted in; omitted otherwise. **(v8.6.0+)**

---

### `GET /v1/agent/{id}`

Get a registered agent by ID. Auth required.

**Response** (HTTP 200): `AgentEntry` object from off-chain store.

---

### `PUT /v1/agent/{id}/permission`

Set clearance, domain access, and visibility on an agent. PATCH semantics: omitted fields preserve their on-chain value (`agent_handler.go:284-323`).

**Auth rules** (`agent_handler.go:213-240`): Self-set OR global `role=admin` OR org admin in any org the target belongs to. ABCI re-checks independently.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `clearance` | *int | no | 0–4; preserved if omitted |
| `domain_access` | *string | no | JSON: `[{"domain":"x","read":true,"write":false}]`; preserved if omitted |
| `visible_agents` | *string | no | JSON array of agent IDs, or `"*"` for all; preserved if omitted |
| `org_id` | *string | no | |
| `dept_id` | *string | no | |

All fields are nullable pointers — sending `null` explicitly resets to empty string / default.

**Response** (HTTP 200): `{"agent_id": "...", "status": "permissions_updated", "tx_hash": "..."}`

---

## 3. Access Control / Clearance

### `POST /v1/access/request`

Request read or read+write access to a domain. Broadcasts `TxTypeAccessRequest`.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `target_domain` | string | yes | |
| `justification` | string | no | |
| `requested_level` | int | no | 1=read, 2=read+write; defaults to 1 |

**Response** (HTTP 201): `{"status": "pending", "tx_hash": "..."}`

---

### `POST /v1/access/grant`

Grant domain access. Caller must own the domain or be admin. Broadcasts `TxTypeAccessGrant`.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `grantee_id` | string | yes | Hex agent ID |
| `domain` | string | yes | |
| `level` | int | no | 1=read, 2=read+write; defaults to 1 |
| `expires_at` | int64 | no | Unix timestamp, 0=permanent |
| `request_id` | string | no | Links to originating access request |

**Response** (HTTP 201): `{"status": "granted", "tx_hash": "..."}`

---

### `POST /v1/access/revoke`

Revoke domain access. Broadcasts `TxTypeAccessRevoke`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `grantee_id` | string | yes |
| `domain` | string | yes |
| `reason` | string | no |

**Response** (HTTP 200): `{"status": "revoked", "tx_hash": "..."}`

---

### `GET /v1/access/grants/{agent_id}`

List active grants for an agent. Cross-checks BadgerDB (chain truth) against the off-chain mirror and drops stale rows (`access_handler.go:264-276`).

**Response** (HTTP 200): Array of grant objects.

---

## 4. Domains

### `POST /v1/domain/register`

Register a domain. Caller becomes owner. Broadcasts `TxTypeDomainRegister`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `name` | string | yes |
| `description` | string | no |
| `parent` | string | no |

**Response** (HTTP 201): `{"status": "registered", "tx_hash": "..."}`

---

### `GET /v1/domain/{name}`

Get domain metadata. Ownership served from BadgerDB (chain-authoritative); description/created_at enriched from off-chain mirror (`access_handler.go:337-384`).

**Response** (HTTP 200): `{domain_name, owner_agent_id, parent_domain, created_height, description, created_at}`

---

### `POST /v1/domain/reassign`

Execute a domain ownership transfer that was authorized by an accepted governance proposal. Admin only; ABCI re-checks admin role. Broadcasts `TxTypeDomainReassign`.

**Pre-requisites:** A `gov_propose` with `operation=domain_reassign` must have reached `executed` status with 3/4 supermajority. The `proposal_id` binds the execution to that decision and is single-use.

**Request body** (`domain_reassign_handler.go:24-29`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `domain` | string | yes | Domain to reassign |
| `new_owner_id` | string | yes | Hex(64) new owner agent ID |
| `parent_domain` | string | no | Must match existing parent if supplied |
| `proposal_id` | string | yes | Hex of accepted gov_propose |
| `open_to_shared` | bool | no | If true, also writes `shared_domain:<name>` on-chain |

**Response** (HTTP 200):

```json
{"tx_hash": "<hex>", "purged_grants": 5}
```

`purged_grants` is parsed from the FinalizeBlock log. Previous owner's full grant chain-of-trust is wiped on transfer.

**Error behavior:** Unlike other endpoints, FinalizeBlock rejection messages are surfaced verbatim (not sanitized) so operators can diagnose `proposal not found`, `body mismatch`, `already consumed`, etc. (`domain_reassign_handler.go:162-195`)

---

## 5. Orgs / Departments / Federation

### `POST /v1/org/register`

Register an organization. Calling agent becomes admin. `org_id` is deterministic: `hex(SHA256(agentID + name)[:16])`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `name` | string | yes |
| `description` | string | no |

**Response** (HTTP 201): `{"status": "registered", "org_id": "...", "tx_hash": "..."}`

---

### `GET /v1/org/{org_id}`

Get org. Chain-authoritative from BadgerDB; description/created_at enriched from mirror.

---

### `GET /v1/org/by-name/{name}`

Look up orgs by name. Returns all matches (names are not unique on-chain). Empty result returns HTTP 200 with `{"orgs": []}`, not 404.

---

### `GET /v1/org/{org_id}/members`

List org members from chain; enriches `created_at` from mirror. Mirror-only rows (missing from chain) are silently dropped.

---

### `POST /v1/org/{org_id}/member`

Add agent to org. Admin only on-chain.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `agent_id` | string | yes | |
| `clearance` | int | no | 0–4; defaults to 1 (INTERNAL) |
| `role` | string | no | `admin`, `member`, `observer`; defaults to `member` |

**Response** (HTTP 201): `{"status": "added", "tx_hash": "..."}`

---

### `DELETE /v1/org/{org_id}/member/{agent_id}`

Remove agent from org. Admin only on-chain.

**Response** (HTTP 200): `{"status": "removed", "tx_hash": "..."}`

---

### `POST /v1/org/{org_id}/clearance`

Change an agent's clearance within the org. Admin only on-chain.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `agent_id` | string | yes |
| `clearance` | int | yes |

**Response** (HTTP 200): `{"status": "updated", "tx_hash": "..."}`

---

### `POST /v1/org/{org_id}/dept`

Register a department. `dept_id` is deterministic: `hex(SHA256(orgID + name)[:8])`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `name` | string | yes |
| `description` | string | no |
| `parent_dept` | string | no |

**Response** (HTTP 201): `{"status": "registered", "dept_id": "...", "tx_hash": "..."}`

---

### `GET /v1/org/{org_id}/dept/{dept_id}`

Get department.

---

### `GET /v1/org/{org_id}/depts`

List all departments in an org.

---

### `POST /v1/org/{org_id}/dept/{dept_id}/member`

Add agent to department.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `agent_id` | string | yes | |
| `clearance` | int | no | defaults to 1 |
| `role` | string | no | defaults to `member` |

**Response** (HTTP 201): `{"status": "added", "tx_hash": "..."}`

---

### `DELETE /v1/org/{org_id}/dept/{dept_id}/member/{agent_id}`

Remove agent from department.

**Response** (HTTP 200): `{"status": "removed", "tx_hash": "..."}`

---

### `GET /v1/org/{org_id}/dept/{dept_id}/members`

List department members.

---

### `POST /v1/federation/propose`

Propose a bilateral federation agreement. Caller must be in an org on-chain. Proposer's org is resolved from chain state automatically.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `target_org_id` | string | yes | |
| `allowed_domains` | []string | no | `["*"]` means all |
| `allowed_depts` | []string | no | |
| `max_clearance` | int | no | Ceiling clearance; defaults to 2 (CONFIDENTIAL) |
| `expires_at` | int64 | no | Unix timestamp; 0=permanent |
| `requires_approval` | bool | no | |

**Response** (HTTP 201): `{"status": "proposed", "tx_hash": "..."}`

---

### `POST /v1/federation/{fed_id}/approve`

Approve a pending federation. Caller must be in an org; approver org resolved from chain.

**Response** (HTTP 200): `{"status": "approved", "tx_hash": "..."}`

---

### `POST /v1/federation/{fed_id}/revoke`

Revoke an active federation.

**Request body:** `{"reason": "..."}` (optional)

**Response** (HTTP 200): `{"status": "revoked", "tx_hash": "..."}`

---

### `GET /v1/federation/{fed_id}`

Get federation by ID.

---

### `GET /v1/federation/active/{org_id}`

List active federations for an org.

---

## 6. Governance / Voting

### `POST /v1/governance/propose`

Submit a governance proposal. Broadcasts `TxTypeGovPropose`.

**Request body** (`governance_handler.go:22-30`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `operation` | string | yes | `add_validator`, `remove_validator`, `update_power`, `domain_reassign` |
| `target_id` | string | yes | Hex validator pubkey for validator ops; domain keying ID for `domain_reassign` |
| `reason` | string | yes | |
| `target_pubkey` | string | no | Hex Ed25519 pubkey, required for `add_validator` |
| `target_power` | int64 | no | Validator power for add/update ops |
| `expiry_blocks` | int64 | no | 0 = chain default |
| `payload` | string | no | Base64-encoded operation-specific body. For `domain_reassign`: base64(JSON `{domain, new_owner_id, parent_domain, open_to_shared}`). The executing `POST /v1/domain/reassign` tx must reproduce this payload byte-for-byte. |

**Response** (HTTP 200):

```json
{
  "proposal_id": "<tx_hash>",
  "tx_hash": "<tx_hash>",
  "status": "voting"
}
```

Note: `proposal_id` equals `tx_hash` in the response. ABCI derives the deterministic proposal ID internally.

---

### `POST /v1/governance/vote`

Vote on a governance proposal. Only validators can cast effective votes.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `proposal_id` | string | yes | From the propose response |
| `decision` | string | yes | `accept`, `reject`, or `abstain` |

**Response** (HTTP 200): `{"tx_hash": "...", "status": "recorded"}`

---

### `POST /v1/governance/cancel`

Cancel a pending governance proposal. Proposer or admin.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `proposal_id` | string | yes |

**Response** (HTTP 200): `{"tx_hash": "...", "status": "cancelled"}`

---

## 7. Validator

### `GET /v1/validator/pending`

Memories awaiting validator votes.

**Query parameters:** `domain_tag`, `limit` (1–100, default 20)

**Response** (HTTP 200): `{"memories": [...]}`

---

### `GET /v1/validator/epoch`

Current epoch validator scores (PoE weights).

**Response** (HTTP 200):

```json
{
  "epoch_num": 12,
  "block_height": 4400,
  "scores": [
    {"validator_id": "...", "accuracy": 0.91, "domain_score": 0.8, ...}
  ]
}
```

---

## 8. Embeddings

### `POST /v1/embed`

Generate a vector embedding via the node's local provider (Ollama or hash fallback). Use this to avoid running a separate embedder.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `text` | string | yes |

**Response** (HTTP 200):

```json
{"embedding": [...], "model": "nomic-embed-text", "dimension": 768}
```

Returns 503 if embedder not ready.

---

### `GET /v1/embed/info`

Report the active embedding provider's capabilities. Clients use this to decide between vector query and FTS5 search paths (`embed_handler.go:47-81`).

**Response** (HTTP 200):

```json
{"semantic": true, "provider": "ollama", "dimension": 768, "ready": true}
```

When vault (at-rest encryption) is active, `semantic` is forced `true` even if no embedder is configured — FTS5 cannot index encrypted content, so callers must not route to `/v1/memory/search`.

---

## 9. MCP Tokens / OAuth

### `POST /v1/mcp/tokens`

Issue a bearer token for MCP clients that cannot sign Ed25519 requests (ChatGPT, Cursor, etc.). Ed25519 auth required (admin use). Token plaintext shown **once only** — not stored, only SHA-256 digest persisted.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | no | Human label, e.g. `chatgpt-laptop` |
| `agent_id` | string | yes | 64-char hex Ed25519 pubkey to mint for |

**Response** (HTTP 201):

```json
{
  "id": "<uuid>",
  "name": "chatgpt-laptop",
  "agent_id": "<hex>",
  "token": "<base64url-32-bytes>",
  "created_at": "...",
  "use_hint": "Set Authorization: Bearer <token> on requests to /v1/mcp/sse or /v1/mcp/streamable. SAVE THIS TOKEN NOW — it is never shown again."
}
```

---

### `GET /v1/mcp/tokens`

List issued tokens as summaries (no token values returned).

**Response** (HTTP 200): `{"tokens": [{id, name, agent_id, created_at, last_used_at, revoked_at}]}`

---

### `DELETE /v1/mcp/tokens/{id}`

Revoke a token by ID. Idempotent.

**Response:** HTTP 204 No Content. 404 if `id` not found.

---

### OAuth 2.0 Endpoints (root-level, no `/v1/` prefix)

These support ChatGPT's MCP connector which requires a full OAuth 2.0 + PKCE flow. Not subject to Ed25519 auth. Mounted at the host root (`oauth_handlers.go:944-961`).

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/.well-known/oauth-authorization-server` | RFC 8414 discovery document |
| `GET` | `/.well-known/oauth-protected-resource` | RFC 9728 protected resource metadata |
| `POST` | `/oauth/register` | RFC 7591 Dynamic Client Registration (10 reqs/IP/hour) |
| `GET` | `/oauth/authorize` | Render PKCE consent screen (requires dashboard session when encryption on) |
| `POST` | `/oauth/authorize` | Submit consent; mints bearer + issues auth code; redirects to `redirect_uri` |
| `POST` | `/oauth/token` | Exchange auth code for bearer (`grant_type=authorization_code` only) |

The `access_token` returned by `/oauth/token` is the same bearer accepted by `Authorization: Bearer <token>` on `/v1/mcp/sse` and `/v1/mcp/streamable`. Token lifetime is controlled by revocation (`DELETE /v1/mcp/tokens/{id}`), not expiry — `expires_in: 0` in the token response.

---

## 10. Pipeline (Agent-to-Agent)

Async work routing between agents. State is off-chain (not consensus-validated). Requires `PipelineStore` support (SQLite yes, Postgres yes).

### `POST /v1/pipe/send`

Send a pipeline message to another agent or provider.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `to_agent` | string | no* | Direct agent_id (* one of to_agent or to_provider required) |
| `to_provider` | string | no* | Provider name or agent name; resolves to agent_id if unambiguous |
| `intent` | string | no | Human description of the work |
| `payload` | string | yes | Arbitrary content |
| `ttl_minutes` | int | no | 1–1440; defaults to 60 |

Target agent must be registered. `to_agent` takes precedence; `to_provider` resolves via provider field or name lookup.

**Response** (HTTP 201): `{"pipe_id": "pipe-<uuid>", "status": "pending", "expires_at": "..."}`

---

### `GET /v1/pipe/inbox`

Fetch pending messages for the authenticated agent (by agent_id or provider). Auto-claims all returned items.

**Query parameters:** `limit` (1–20, default 5)

**Response** (HTTP 200): `{"items": [...PipelineMessage], "count": N}`

---

### `PUT /v1/pipe/{pipe_id}/claim`

Atomically claim a pipeline message (prevents double-processing).

**Response** (HTTP 200): `{"pipe_id": "...", "status": "claimed"}`. HTTP 409 if already claimed.

---

### `PUT /v1/pipe/{pipe_id}/result`

Submit result for a claimed message. Triggers auto-journal: inserts an `observation` memory in domain `agent-pipeline` via off-chain insert.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `result` | string | yes |

**Response** (HTTP 200): `{"status": "completed", "journal_id": "<memory_id or empty>"}`

---

### `GET /v1/pipe/{pipe_id}`

Get current status of a pipeline message.

**Response** (HTTP 200): Full `PipelineMessage` object.

---

### `GET /v1/pipe/results`

Completed pipeline messages sent by the authenticated agent.

**Query parameters:** `limit` (1–20, default 5)

**Response** (HTTP 200): `{"items": [...], "count": N}`

---

## 11. Operational (No Auth)

### `GET /health`

Liveness probe. No auth.

**Response** (HTTP 200): `{"status": "healthy"}`

The `version` field is intentionally omitted — `/health` is reachable through the wizard tunnel allowlist, so it stays minimal to avoid version-fingerprinting an internet-exposed node (`internal/metrics/health.go`). Returns HTTP 503 `{"status": "unhealthy"}` when a dependency (PostgreSQL or CometBFT) is down.

---

### `GET /ready`

Readiness probe. Checks PostgreSQL and CometBFT. No auth.

**Response** (HTTP 200): `{"status": "ready", "postgres": true, "cometbft": true}`

503 when not ready.

---

## Node Operator Bypass

If the server has `nodeOperatorID` configured (`server.go:50-57`), requests signed with that key bypass agent-isolation RBAC (the cross-agent visibility filter) on all read paths. Domain access and classification gates still apply. This lifts the prefetch limit on nodes where the LLM's registered identity is separate from the operator key.

---

## Environment Variables

| Variable | Default | Effect |
|---|---|---|
| `SAGE_TX_COMMIT_TIMEOUT_MS` | 60000 | `broadcast_tx_commit` client timeout |
| `VALIDATOR_KEY_FILE` | — | Path to CometBFT `priv_validator_key.json`; required for quorum voting |
| `CORS_ALLOWED_ORIGINS` | `*` | Comma-separated allowed origins |

---

## OpenAPI Drift Notes

The following drift was found between `api/openapi.yaml` (version 3.5.0) and the actual handlers in `server.go`:

1. **Missing endpoints in OpenAPI** — The spec documents only a subset of implemented routes. These endpoints exist in code but have no OpenAPI path entry:
   - `POST /v1/memory/search`
   - `POST /v1/memory/hybrid`
   - `POST /v1/memory/link`
   - `GET /v1/memory/tasks`
   - `GET /v1/memory/list`
   - `GET /v1/memory/timeline`
   - `POST /v1/memory/pre-validate`
   - `POST /v1/memory/{memory_id}/forget`
   - `PUT /v1/memory/{memory_id}/task-status`
   - All access/grant/revoke/list endpoints (`/v1/access/*`)
   - All org/federation/department endpoints (`/v1/org/*`, `/v1/federation/*`)
   - All governance vote and cancel endpoints (`/v1/governance/vote`, `/v1/governance/cancel`)
   - All pipeline endpoints (`/v1/pipe/*`)
   - All embedding endpoints (`/v1/embed`, `/v1/embed/info`)
   - All MCP token endpoints (`/v1/mcp/tokens*`)
   - All OAuth endpoints (`/oauth/*`, `/.well-known/*`)
   - `GET /v1/agents` (public)

2. **`MemoryType` enum incomplete** — OpenAPI schema lists `fact`, `observation`, `inference` but the handler also accepts `task` (`memory_handler.go:364-367`, `internal/tx/types.go:79`).

3. **`VoteResponse` schema mismatch** — OpenAPI declares `vote_id` field; actual handler returns `tx_hash` (`vote_handler.go:168`).

4. **`classification` field absent from `MemorySubmitRequest` schema** — The spec's `MemorySubmitRequest` does not include `classification`, though the handler accepts and passes it through verbatim. The clearance values in `AgentSetPermissionRequest` description use "Guest" for 0, but the code calls it "PUBLIC".

5. **`/v1/agent/register` response code** — OpenAPI shows HTTP 200 for both new and existing. Handler returns HTTP 201 for new registrations and HTTP 200 for already-registered.

6. **`EpochInfo` schema** — OpenAPI declares `started_at` field (`api/openapi.yaml:543`); the actual `EpochResponse` struct in `vote_handler.go:86-91` does not include `started_at` or `block_height` is present but `epoch_num` comes from a different source. The handler returns `epoch_num`, `block_height`, `scores` — no `started_at`.
