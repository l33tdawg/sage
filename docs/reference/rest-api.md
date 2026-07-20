<!-- Reconciled through SAGE v11.11.1. Cite file:line when behavior is non-obvious. -->

# SAGE REST API Reference

## Authentication

Most core `/v1/*` REST endpoints require Ed25519 request signing (`api/rest/middleware/auth.go:81-213`). Public and alternate-auth exceptions include `GET /v1/agents`, health/readiness routes, OAuth discovery/flows, and the HTTP MCP transports (`/v1/mcp/sse`, `/v1/mcp/messages`, `/v1/mcp/streamable`), which use MCP bearer-token/OAuth authentication.

| Header | Format | Purpose |
|---|---|---|
| `X-Agent-ID` | 64-char hex Ed25519 pubkey | Identifies the agent |
| `X-Signature` | hex-encoded Ed25519 sig | Signs the canonical payload |
| `X-Timestamp` | unix epoch seconds | Prevents replay |
| `X-Nonce` | hex bytes (optional) | Sub-second replay protection; include for concurrent requests |

The REST CORS preflight allowlist includes all four signing headers, including
`X-Nonce`; browser clients do not need to fall back to the legacy nonce-less
signature shape (`api/rest/server.go`).

**Signed message construction** (`auth.go:156-180`):

```
canonical = METHOD + " " + PATH[?QUERY] + "\n" + BODY
message   = SHA-256(canonical) + bigEndian(timestamp_int64) + nonce
```

Include `X-Nonce` on current clients. If `X-Nonce` is absent, the server accepts the legacy nonce-less signature shape for backward compatibility.

**Constraints:**
- Timestamp must be within ±5 minutes of server time (`auth.go:79`).
- Duplicate `(agentID, signature)` pairs within the 5-minute window are rejected (replay cache, `auth.go:27-53`).
- Body is capped at 1 MB before reading for signature verification (`auth.go:143`).
- `X-Agent-ID` is the hex-encoded Ed25519 **public key** (32 bytes = 64 hex chars); it IS the agent identity on-chain.

**Post-app-v17 consensus binding.** The REST process is not the trust boundary. Once app-v17 is active, a transaction whose outer node key differs from `X-Agent-ID` carries the exact `canonical` bytes above in the optional `ParsedTx.AgentRequest` wire tail. `FinalizeBlock` re-hashes those bytes to `AgentBodyHash`, re-verifies the Ed25519 proof, rejects proofs more than five minutes older than deterministic block time, and rebuilds the expected type-specific payload from the signed method, path, and JSON. Historical non-governance proofs intentionally remain valid when ahead of block time because SAGE mints no idle heartbeat blocks and the first block after a long idle period can lag the already wall-clock-validated REST request. App-v20 governance is narrower: every governance envelope containing any agent-proof material, including one whose embedded signer equals the outer validator, must carry the complete request and 8-byte nonce and must fall within **±5 minutes** of deterministic block time. For memory submissions, content/type/domain/confidence/classification/parent/task status stay action-bound while the embedding hash is node-derived and outer-signature-bound: v11.7.4 made the active node authoritative for vector generation after request authentication. A mismatch is rejected with code 109 before the action handler runs. A successful validation atomically claims an AppHash-folded proof fingerprint until its freshness window closes, so the same agent authorization cannot be wrapped in a second node transaction with a fresh outer nonce. Ordinary same-key non-governance transactions and **truly proofless** direct governance/upgrade-auto-voter transactions need no HTTP envelope: the outer signature binds the payload and app-v9's monotonic nonce prevents same-chain replay (`internal/abci/agent_proof.go`, `internal/store/badger.go`).

The optional tail is emitted only after the app-v17 activation block commits. Before activation it is absent, preserving the exact bytes older validators re-encode and every historical block's replay behavior (`internal/tx/codec.go`, `api/rest/server.go`).

**Errors** use RFC 7807 `application/problem+json` with `type`, `title`, `status`, `detail` fields.

---

## 1. Memory

### `POST /v1/memory/submit`

Submit a memory for BFT consensus. Blocks until `broadcast_tx_commit` returns (FinalizeBlock completes). Default timeout 60 s; override via `SAGE_TX_COMMIT_TIMEOUT_MS`.

**Request body** (`memory_handler.go:30-46`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `content` | string | yes | Natural-language memory text |
| `memory_type` | string | yes | One of: `fact`, `observation`, `inference`, `task` |
| `domain_tag` | string | yes | Domain label; agent must have write access |
| `confidence_score` | float64 | yes | 0.0–1.0 inclusive |
| `classification` | int | no | 0–4; see table below. **Omitting sends 0 (PUBLIC)** |
| `embedding` | []float32 | no | Compatibility field. The node regenerates the vector from `content` with its currently selected provider so stale/foreign vector spaces cannot mix. |
| `knowledge_triples` | []KnowledgeTriple | no | `{subject, predicate, object}` triples |
| `parent_hash` | string | no | SHA-256 hex of parent memory for lineage |
| `task_status` | string | no | For `task` type: `planned`, `in_progress`, `done`, `dropped` |
| `tags` | []string | no | Up to 32 labels of 128 UTF-8 bytes each. Above app-v20 they are sorted/deduplicated into the signed tx; scoped-domain tags are also AppHash-covered and projection-recoverable. Ordinary-domain tags remain node-local. OR-filter on query/search. |
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
  "status": "proposed",
  "embedding_provider": "ollama"
}
```

If the selected provider is temporarily unavailable, the memory still commits
without accepting the caller's possibly mismatched vector and the response adds
`"embedding_queued": true`. The node repairs it automatically once that same
provider is healthy and the vault is unlocked.

**Auth:** Ed25519 required. Agent must have write access to `domain_tag` if per-domain access control is configured (`memory_handler.go:425-428`). Observer-role agents are rejected.

**Domain write denial (v11.8.4+):** A consensus rejection whose reason is specifically that the authenticated agent has no write access to the requested domain returns HTTP 403 with RFC 7807 type `https://sage.dev/errors/domain-write-denied`. Its sanitized detail directs the caller to grant level 2 (read + write) in CEREBRUM Access Controls or ask the domain owner; it exposes neither the agent ID nor domain-owner ID. MCP clients use the type to avoid treating a permanent ACL denial as a stale session and do not re-register or retry the write (`memory_handler.go`, `internal/mcp/server.go`).

**curl example:**

```bash
# Compute timestamp and sign with your Ed25519 key (see SDK for helpers)
BODY='{"content":"Go 1.22 dropped support for GOPATH mode","memory_type":"fact","domain_tag":"go-debugging","confidence_score":0.95}'
TS=$(date +%s)
NONCE=$(openssl rand -hex 8)
# signature = ed25519_sign(SHA256("POST /v1/memory/submit\n" + BODY) + bigEndian(TS) + hex_decode(NONCE))
curl -X POST http://localhost:8080/v1/memory/submit \
  -H "Content-Type: application/json" \
  -H "X-Agent-ID: <64-hex-pubkey>" \
  -H "X-Signature: <hex-sig>" \
  -H "X-Timestamp: $TS" \
  -H "X-Nonce: $NONCE" \
  -d "$BODY"
```

---

### `POST /v1/memory/query`

Vector similarity search. Requires a precomputed embedding.

**Request body** (`memory_handler.go:56-73`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `embedding` | []float32 | yes | Query vector; must match stored embedding dimension |
| `domain_tag` | string | no | Filter to domain |
| `provider` | string | no | Provider filter |
| `min_confidence` | float64 | no | Minimum **decayed** confidence threshold — enforced against the same decayed value returned in `confidence_score`, not the stored value (see note below) |
| `status_filter` | string | no | `proposed`, `validated`, `committed`, `challenged`, `deprecated` |
| `top_k` | int | no | Max results; default 10 |
| `cursor` | string | no | Opaque pagination cursor from previous response |
| `tags` | []string | no | OR-filter by tag on both SQLite and Postgres |

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
      "initial_confidence": 0.95,
      "corroboration_count": 2,
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

`confidence_score` in the response is the **decayed** value (time decay + corroboration boost applied server-side), not the raw submitted value. `initial_confidence` is the **stored** (undecayed) value — the on-chain confidence set at submission (corroboration never rewrites it) — so a client can see the authoritative floor alongside the decayed score without re-deriving it. It is present for local memories and omitted for federated results (where only the serving peer's already-decayed value is available).

`disputed` (`memory_handler.go`, `json:"disputed,omitempty"`) is present and `true` only for an app-v17 two-phase-**challenged** memory that is still live and recallable while under dispute (set on all three recall paths: query, search, and hybrid). Because it is `omitempty`, its absence means "not disputed," which is why the committed example above omits it. When it is set, `confidence_score` already carries an extra query-time **disputed haircut** (the shared `store.DisputedConfidenceHaircut`, currently a `0.8` multiplier) layered on top of decay and corroboration. The store applies the same multiplier while enforcing `min_confidence`, so a returned result still satisfies the advertised floor after serialization. The haircut is presentation-only: it leaves the on-chain `status` (`challenged`) and the stored confidence untouched, and personal nodes never emit `disputed` at all (a challenge there is a one-strike deprecate, not a two-phase park). A challenged memory's on-chain `status` is `challenged`, already listed among the queryable `status_filter` values above.

`min_confidence` is enforced against the **decayed** `confidence_score`, not the stored column. The store applies it over the full candidate set *before* the top-K trim, so: a result returned by a `min_confidence=X` query always satisfies `confidence_score >= X` (including federated results, which are re-checked against the floor on the requesting side); a corroboration-boosted memory whose stored value is below `X` but whose decayed value clears it is still returned; and `top_k` is filled with qualifying records rather than truncated. This full-corpus guarantee holds unconditionally only for `POST /v1/memory/query` on SQLite, whose `QuerySimilar` scans every candidate; on `/v1/memory/search` (FTS), `/v1/memory/hybrid` (via its FTS leaf), and on Postgres deployments the floor is instead evaluated over the top `decayFilterScanCap` (1000) rank/distance-ordered candidates, so a record that would qualify but ranks beyond that pool may not surface. Open tasks are exempt from decay, so an open task is judged by its stored confidence. (Prior to v11.2.0 the floor compared the stored column, which both leaked aged memories below the floor and dropped boosted ones above it.)

`filtered` is present when agent-isolation RBAC or per-record classification gates silently hid records. Check the `X-SAGE-Filter-Applied` response header for the same info. Values in `by`: `rbac_submitting_agents`, `classification`.

**Access control stacking** (`memory_handler.go:638-697`): Domain access is checked first (agent's `DomainAccess` policy), then multi-org gate (if domain has a registered owner), then agent-isolation RBAC. These are alternatives — passing domain access disables agent isolation for that call.

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

**Request body** (`memory_handler.go:976-987`):

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

**Request body** (`memory_handler.go:1203-1216`):

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

Access control: agent-isolation RBAC + multi-org classification gate apply. Own memories always visible. 403 if agent cannot see the submitter or lacks org clearance for the memory's classification. (`memory_handler.go:1445-1490`)

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

**Error responses** (deprecation gate; `vote_handler.go:241`, `memory_handler.go:1675-1684`):

| Status | Meaning |
|---|---|
| 403 Forbidden | Authenticated but not authorized to deprecate — `not authorized to deprecate this memory (need domain ownership or a level-3 modify grant)`. Also the status for a **pre-app-v16** legacy no-recorded-domain reject (it is an authorization failure there; app-v16 promotes the legacy case to the actionable 409 below). |
| 404 Not Found | (app-v16) No on-chain memory record for that id — `unknown memory: no on-chain record for that memory id`. |
| 409 Conflict | (app-v16) The target is a legacy memory with an on-chain record but no recorded domain (committed before app-v8.4) — `memory has no recorded domain (legacy pre-app-v8.4 record); deprecation is blocked until its domain is repaired via an OpMemoryDomainRepair governance proposal (app-v16)`. Deprecation stays blocked until an `OpMemoryDomainRepair` governance proposal backfills the domain; retry then succeeds. |

The 404/409 split is app-v16 behavior. Pre-app-v16 (and on a chain that has not activated app-v16) the legacy no-recorded-domain case returns `403` (an authorization failure), not the actionable `409`.

---

### `POST /v1/memory/{memory_id}/forget`

Semantic alias for challenge (`vote_handler.go:255-325`). Submits a `TxTypeMemoryChallenge` with reason defaulting to `"deprecated by user"` when omitted.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `reason` | string | no |

**Response** (HTTP 200): `{"message": "Memory forgotten.", "tx_hash": "<hex>"}`

**Error responses:** identical to `/challenge` (same deprecation gate) — `403` not authorized (or a pre-app-v16 legacy reject), `404` unknown memory id, and `409` legacy no-recorded-domain (repair via an `OpMemoryDomainRepair` governance proposal). See the challenge section above.

---

### `POST /v1/memory/{memory_id}/reinstate`

Return an open app-v17 two-phase challenge to `committed`. The handler builds
`TxTypeMemoryReinstate`, embeds the authenticated caller proof, and waits for
`broadcast_tx_commit`; a successful response is durable, not only a mempool
receipt.

**Request body:** `{"reason": "optional audit note"}` (`reason` may be omitted).

**Response** (HTTP 200):
`{"message": "Memory reinstated.", "tx_hash": "<hex>", "status": "committed"}`

The chain must have activated app-v17. Current domain owners/ancestor owners and
level-3 modify grantees may reinstate. The original challenger may always
withdraw their own challenge even if the grant that authorized the challenge
later expired or was revoked.

**Errors:** `400` when app-v17 is not active, `403` when a non-challenger lacks
the modify verb, `404` for an unknown memory, and `409` when the memory is not
currently challenged.

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

Start or finish a `task`-type memory as the active signed agent (off-chain only,
no tx). Every status mutation requires the task's assignee to exactly match the
signature-verified agent ID. Unassigned work requires assignment from the local
CEREBRUM operator board. Agents cannot set `planned` or reopen terminal work.

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

Dry-run the per-node validation checks (dedup, quality, consistency) without submitting on-chain. Returns per-check decisions.

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
  "quorum": "3/3",
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

Request domain access. Broadcasts `TxTypeAccessRequest`.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `target_domain` | string | yes | |
| `justification` | string | no | |
| `requested_level` | int | no | 1=read, 2=read+write, 3=modify on app-v15+; defaults to 1 |

**Response** (HTTP 201): `{"status": "pending", "tx_hash": "..."}`

---

### `POST /v1/access/grant`

Grant domain access. Caller must own the domain or be admin. Broadcasts `TxTypeAccessGrant`.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `grantee_id` | string | yes | Hex agent ID |
| `domain` | string | yes | |
| `level` | int | no | 1=read, 2=read+write, 3=modify on app-v15+; defaults to 1 |
| `expires_at` | int64 | no | Unix timestamp, 0=permanent |
| `request_id` | string | no | Links to originating access request |

**Response** (HTTP 201): `{"status": "granted", "tx_hash": "..."}`

**CEREBRUM access matrix (v11.3):** Saving the per-agent Domain Access matrix in the dashboard (`PATCH /v1/dashboard/network/agents/{id}`) now issues REAL `AccessGrant`/`AccessRevoke` txs through this same consensus path - diffed against the actual on-chain grant state (`GetAccessGrant`), so it is idempotent and self-healing - and each tx is signed AS the domain owner (whom the consensus gate authorizes). Domains whose owner key is not on this node are reported as skipped, not silently dropped. Previously the matrix wrote only a cosmetic policy blob the access checks never read; the consensus grant/revoke handlers themselves are unchanged (no admin-bypass added). See `web/reassign_handler.go:116-226` and `concepts/rbac-orgs-federation.md`.

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

**CEREBRUM orchestration (v11.3):** The dashboard drives this whole agent-to-agent transfer from the Search page via `POST /v1/dashboard/network/reassign-domain-ownership`, commit-confirmed in strict order: `gov_propose(domain_reassign)` -> the sole validator's accept vote drives the proposal to `Executed` in-band -> this `DomainReassign` flips the owner and purges the domain's grants -> an `AccessGrant` gives the new owner level 3 (deferred to the owner's own node if their key is not local). It requires a single-validator node; a multi-validator chain returns HTTP 409 because the other validators must vote on the proposal. This is off-consensus orchestration only - each underlying step is the same on-chain tx documented here, and memory authorship (`submitting_agent`) is never rewritten (`web/reassign_handler.go:285-318`).

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

### Cross-chain peer Read/Copy control (Write reserved)

Current v3 JOIN establishes node trust only: the Manager accepts only the fixed
`{max_clearance:4, allowed_domains:[], mode:"exchange", direction:"both"}`
compatibility scope, and the browser sends exactly that
(`internal/federation/join_routes.go:75-91`, `257-280`, `730-740`,
`1051-1055`, `1126-1149`). Afterwards each node independently grants the
frozen peer Read and/or Copy over selected **existing** local domains. This is
mutable per-peer RBAC, not a reason to re-pair. The versioned Write member is
reserved but unavailable and must remain false in v11.9. The exact model and
peer-facing wire protocol are documented in
[`federation-and-brain-api.md`](federation-and-brain-api.md#trust-and-directional-peer-rbac).

The browser control plane is under `/v1/dashboard/federation/*`. Every route
below has the dashboard's stricter federation-operator gate (local
CEREBRUM operator or the exact node-operator signer), not merely ordinary
dashboard-agent auth (`web/federation_join.go:71-102`, `1021-1037`).

| Route | Request / response |
|---|---|
| `GET /v1/dashboard/federation/shareable-domains` | Returns `{"domains":[{"domain","memory_count","authority","can_share"}]}` from registered plus observed local domains. It never creates a domain (`web/federation_permissions.go:30-111`). |
| `GET /v1/dashboard/federation/connections/{chain_id}/permissions` | Returns `local_permissions`, `local_legacy`, authenticated read-only `remote_permissions`, `remote_known`, and `remote_legacy` (`web/federation_permissions.go:161-220`). |
| `PUT /v1/dashboard/federation/connections/{chain_id}/permissions` | Full replacement body: `{"permissions":[{"domain":"tii.work","read":true,"copy":false}]}`. Omitted domains are revoked; `[]` is explicit deny-all. Copy implies Read. Every enabled domain must already exist and be controlled by this operator. A `write:true` member is rejected with `400` because no consensus-bound federation ingress capability exists (`web/federation_permissions.go:223-258`, `279-305`). |
| `GET /v1/dashboard/federation/connections/{chain_id}/sync` | Returns `publish_domains`, `subscribe_domains`, `remote_publish_domains`, `remote_subscribe_domains`, and revision state. |
| `PUT /v1/dashboard/federation/connections/{chain_id}/sync` | v3 accepts `publish_domains` and/or `subscribe_domains`; an omitted lane is preserved and an explicit empty lane is cleared. The UI uses `{"subscribe_domains":[...]}` for the receiver's independent “Save here” decision (`web/federation_join.go:414-527`). |

`POST /v1/federation/cross/{chain_id}/write` is a reserved compatibility route,
not an enabled peer capability. It requires the exact node operator, validates
the chain id, and returns `501` before parsing a `RemoteWriteRequest` or calling
the transport (`api/rest/federation_write_handler.go:11-25`). The separately
reserved `WritePeer` method returns `ErrRemoteWriteCapabilityUnavailable`
before agreement lookup or dialing (`internal/federation/remote_write.go:16-19`,
`41-45`). The peer-facing
`POST /fed/v1/write` is mounted behind peer authentication and likewise returns
an authenticated `501` without parsing or dispatching the body. Status never
advertises reserved `write-v1` (`internal/federation/remote_write.go:48-52`;
`internal/federation/server.go:282-315`).

An ordinary level-2 `AccessGrant` is agent/domain authorization usable through
the normal submit API outside a particular federation link. It is therefore not
a trust-bound A↔B Write permission. v11.9 keeps Write fail-closed until consensus
provides an ingress capability bound to the active ceremony generation, frozen
peer, domain, and exact submission. Tracked preview-era grants are revoked and
verified synchronously before `sage-gui` binds application listeners; an
incomplete cleanup aborts startup (`internal/federation/remote_write.go:10-19`;
`cmd/sage-gui/node.go`).

Copy is separately two-sided: the source must grant Copy and the receiver must
subscribe. The effective v3 path is source Copy ∩ source Publish ∩ receiver
Subscribe; neither Read nor trust silently enables retention
(`internal/federation/sync_outbox.go:351-428`).

### Signed directional sync and legacy compatibility (`/v1/federation/cross/{chain_id}/sync*`)

The Ed25519-authenticated `PUT/GET .../sync` routes require the exact node
operator. `PUT` accepts `domains`, `publish_domains`, and `subscribe_domains` as
presence-sensitive arrays, but never mixes the two models
(`api/rest/federation_handler.go:342-440`). On an active, frozen v3 connection:

- `domains` is rejected;
- either or both directional fields may be supplied;
- an omitted lane is preserved and an explicit `[]` clears that lane; and
- both the original host and guest may author their own local lanes
  (`api/rest/federation_handler.go:447-518`).

A true legacy v1/v2 link rejects directional fields and retains the old
host-controlled `{"domains":[...]}` rules. A v3 marker whose frozen binding is
not active fails closed rather than falling through to that legacy path
(`api/rest/federation_handler.go:520-576`). Fresh trust-only links use v3.

The v3 `GET` response returns local and remote Publish/Subscribe lanes plus the
local and remote policy versions/revisions. It includes the legacy
`sync_domains` alias only when the two local lanes are equal, so the alias never
misrepresents a directional policy (`api/rest/federation_handler.go:656-719`).

`GET .../sync/status` remains the operator observability surface for outbox
counts, rejected/pending rows, retry timing, anti-entropy state, peer consent,
and unsupported-peer status (`api/rest/federation_handler.go:578-654`).
Revocation purges policy/controller/outbox state and cached credentials/routes.
Domain Copy is SQLite-only; unsupported stores return `501` and the drainer is
a no-op.

---

## 6. Governance / Voting

### `GET /v1/governance/context`

Return the validator-and-chain binding for the next governance mutation. This
route is authenticated and restricted to the configured governance operator.
Current SDK and MCP clients call it immediately before every propose, vote, or
cancel request.

```json
{
  "validator_id": "<this-node-validator-id>",
  "governance_domain": "<64-lowercase-hex-chain-binding-or-empty>",
  "app_v20_active": true,
  "validator_active": true,
  "active_validators": [
    {"validator_id": "<validator-id>", "voting_power": 10}
  ]
}
```

Before app-v20, `governance_domain` is empty and clients retain the historical
mutation body. After app-v20, clients copy both identifiers into the following
request body before signing it. A mutation returns `409` when either value is
stale or belongs to another validator/chain; fetch the context again and
re-sign the complete request. Missing live validator/domain configuration is
`503`; a valid signer that is not this node's operator receives `403`.

`validator_active` and the bytewise-sorted `active_validators` roster are read
directly from the AppHash-covered `validator:*` records in BadgerDB on every
request. They are readiness/evidence fields, not caller-supplied authority. A
removed validator's still-running gateway therefore remains identifiable while
reporting `validator_active=false`, and operators can compare the exact
persisted IDs/powers with CometBFT's effective validator set after an H+2
transition or restart. An unavailable or malformed persisted roster fails the
authenticated context request with `503` instead of returning a guessed view.

The committed domain is
`SHA-256("sage/governance-delegation-domain/v20\x00" || chain_id)`, encoded as
64 lowercase hex characters. It is quorum-committed in the app-v20 upgrade
proposal rather than accepted from the HTTP caller.

### `POST /v1/governance/propose`

Submit a governance proposal. Broadcasts `TxTypeGovPropose`. Only the
configured governance operator may authorize this validator's proposal. The
gateway must also have the live CometBFT private-validator key; otherwise it
returns `503` before broadcasting. After app-v20, consensus binds every
proof-bearing proposal's exact operator-signed request — including same-key
envelopes — with an 8-byte `X-Nonce`, validator ID, chain domain, and ±5-minute
deterministic block-time window as the global-admin authorization,
while the outer validator remains the proposal actor and automatic voter. The
configured operator must therefore be a registered global admin before it can
authorize proposals; vote/cancel authorization does not require sharing that
admin key across validators.

**Request body** (`governance_handler.go:26-35`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `validator_id` | string | app-v20 | Exact value from the immediately preceding `GET /v1/governance/context`. Omit before app-v20. |
| `governance_domain` | string | app-v20 | Exact value from the immediately preceding `GET /v1/governance/context`. Omit before app-v20. |
| `operation` | string | yes | `add_validator`, `remove_validator`, `update_power`, `domain_reassign`, `memory_domain_repair`, `sync_group_action`, `scope_action` |
| `target_id` | string | conditional | Hex validator pubkey for validator ops; operation key for other ops. May be omitted only when `scope_action` supplies `scope.scope_id`; otherwise it is required. |
| `reason` | string | yes | |
| `target_pubkey` | string | no | Hex Ed25519 pubkey, required for `add_validator` |
| `target_power` | int64 | no | Validator power for add/update ops |
| `expiry_blocks` | int64 | no | 0 = chain default |
| `payload` | string | no | Base64-encoded operation-specific body. For `domain_reassign`: base64(JSON `{domain, new_owner_id, parent_domain, open_to_shared}`). The executing `POST /v1/domain/reassign` tx must reproduce this payload byte-for-byte. For `memory_domain_repair` (app-v16, 2/3 quorum): base64(JSON `[{"memory_id","domain"}]`) — the backfill applies directly on proposal execution. Legacy `scope_action` callers may pass canonical binary `ScopeRecordV1`; mutually exclusive with `scope`. |
| `scope` | object | no | Preferred guided `scope_action` template: `{scope_id, revision, state, controller_validator_id, domains: string[], members: [{validator_id, assigned_weight, joined_revision?, active?}]}`. `scope_id` is one non-empty path segment (no `/`). The node sorts domains/members bytewise, defaults `active` to true and `joined_revision` to 1 only for revision 1, fixes both heights to zero, then encodes canonical `ScopeRecordV1`. |

For app-v20 scope formation, use the structured form rather than constructing
binary bytes (`governance_handler.go:105-113`, `scope/proposal.go:33-81`):

```json
{
  "operation": "scope_action",
  "reason": "form the research replica quorum",
  "scope": {
    "scope_id": "research-quorum",
    "revision": 1,
    "state": "active",
    "controller_validator_id": "<validator-id>",
    "domains": ["research"],
    "members": [
      {"validator_id": "<validator-id>", "assigned_weight": 1}
    ]
  }
}
```

Later revisions must provide every member's historical `joined_revision`; this
prevents a convenience client from silently rewriting roster history
(`scope/proposal.go:22-30`, `scope/proposal.go:50-68`). `payload` and `scope`
are mutually exclusive, and an explicit `target_id` must equal `scope_id`
(`governance_handler.go:170-204`).

**Response** (HTTP 200):

```json
{
  "proposal_id": "<deterministic-governance-id>",
  "tx_hash": "<tx_hash>",
  "status": "voting"
}
```

`proposal_id` is the deterministic ID recorded by the governance engine and is
the value required by the vote and cancel endpoints. It is distinct from
`tx_hash`, which identifies the CometBFT transaction. Governance mutations are
validator-gateway operations: the signed HTTP caller must be the configured
governance operator, while that node's validator key is the on-chain actor.
`status` is loaded from committed governance state after
`broadcast_tx_commit`; it can therefore already be a terminal state rather
than the example `voting`. It is `unknown` only for an embedded server without
an authoritative Badger governance store. A non-operator receives `403`; a
missing operator or live validator key receives `503`, with no broadcast.
Stale/mismatched app-v20 context receives `409` before broadcast. When a
Badger governance store is configured, an absent, malformed, or mismatched
proposal after commit is reported as `500` with the committed `tx_hash` called
out for operator inspection; it is never masked as `unknown`.

---

### `POST /v1/governance/vote`

Vote on a governance proposal. Only the configured local node operator may
authorize the local validator's vote; one operator cannot vote through another
validator's node. After app-v20, consensus verifies the exact
operator-signed vote request and its validator/chain context, but attributes
voting power only to the outer validator key. This node-local operator need not
be a global admin.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `validator_id` | string | app-v20 | From the immediately preceding governance context response |
| `governance_domain` | string | app-v20 | From the immediately preceding governance context response |
| `proposal_id` | string | yes | From the propose response |
| `decision` | string | yes | `accept`, `reject`, or `abstain` |

**Response** (HTTP 200): `{"tx_hash": "...", "status": "recorded"}`

---

### `POST /v1/governance/cancel`

Cancel a pending governance proposal. The configured local node operator may
authorize cancellation by the local validator that proposed it. The same
`403` non-operator, `409` stale-context, and `503` missing-key/configuration
behavior applies. The node-local operator need not be a global admin.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `validator_id` | string | app-v20 |
| `governance_domain` | string | app-v20 |
| `proposal_id` | string | yes |

**Response** (HTTP 200): `{"tx_hash": "...", "status": "cancelled"}`

---

### `GET /v1/scopes`

List canonical v11.9 scope heads in bytewise scope-ID order. Read-only and
restricted to the node operator or an administrator because the response
contains validator topology. Each record includes its current domain-separated
SHA-256 `revision_hash`, exact domain allowlist, pinned integer weights, state,
materialized consensus heights, pending scoped ballots, and validator-removal
blockers. `drain.blocking_validator_ids` is the operator's fail-closed checklist:
a validator cannot be removed from CometBFT until it has left every non-retired
scope and every ballot whose pinned roster contains it is terminal.

**Response** (HTTP 200): `{"scopes": [...], "count": 1}`

---

### `GET /v1/scopes/{scope_id}`

Return one canonical v11.9 scope head with the same shape as an entry from
`GET /v1/scopes`. Returns 404 when absent and fails closed if its immutable
revision audit anchor is missing. Scope IDs cannot contain `/`; clients URL-
escape the remaining path-segment characters. Node-operator/admin only.

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

Issue a bearer token for MCP clients that cannot sign Ed25519 requests (ChatGPT, Cursor, etc.). Ed25519 auth required (admin use). Token plaintext is shown **once only** — not stored, only its SHA-256 digest is persisted. HTTP MCP calls execute as the local node operator because that is the signing key held by the transport; tokens cannot impersonate another agent identity.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | no | Human label, e.g. `chatgpt-laptop` |
| `agent_id` | string | yes | The local node operator's 64-char hex Ed25519 pubkey |

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

Async work routing between agents on this node or across an approved SAGE
federation connection. Agent-visible state remains off-chain and uses the same
SQLite-backed pipeline inbox in both cases; the transport outbox and replay
ledger are delivery machinery, not a second inbox. `PostgresStore` currently
exposes interface stubs that return "not implemented" and must not be described
as pipe support (`internal/store/store.go:548-643`,
`internal/store/pipeline_transport.go:30-73`).

Pipeline intent, payload, result, and the complete persisted agent proof are
vault-backed. A foreign request or result is never automatically journaled,
embedded, indexed as memory, written to Badger/AppHash, or treated as trusted
instructions (`internal/store/sqlite.go:4764-4837`,
`internal/store/pipeline_transport.go:92-176`,
`api/rest/pipe_handler.go:538-572`).

### `POST /v1/pipe/resolve`

Resolve a human-friendly target without sending any content or creating a
pipeline row. The MCP client calls this immediately before `send`, then signs
the exact returned destination (`api/rest/pipe_handler.go:47-126`,
`internal/mcp/tools.go:2108-2167`).

**Request:** `{"to":"provider, agent name, #node/agent-prefix, or agent@chain"}`

**Response** (HTTP 200):

```json
{
  "to_agent": "<exact 64-hex agent id, or empty for a local provider>",
  "to_provider": "<local provider, or empty>",
  "source_chain_id": "<exact local chain for a federated target, otherwise empty>",
  "destination_chain_id": "<exact remote chain id, or empty for local>",
  "address": "<agent@chain for a federated contact>",
  "handle": "<#node/agent-prefix>",
  "display_name": "<sanitized remote label>"
}
```

Local targets retain their existing provider/name/ID behavior. Federated
resolution is limited to the finite authenticated contact snapshots disclosed
by active peers. A qualified handle never falls through to a similarly named
local target. Unknown, stale, paused, unavailable, non-accepting, ambiguous, or
temporarily incomplete resolution fails explicitly. An exact `agent@chain`
address may resolve while that one peer is genuinely offline from the last
authenticated contact snapshot, but only when its encrypted cache still
matches the complete active JOIN, CA, operator, policy-epoch, and policy
revision binding. Friendly handles and bare labels always require a live,
complete peer scan. TLS/certificate/authentication, HTTP, identity, malformed
response, and binding failures never use the cache
(`internal/federation/client.go:47-67,154-160`;
`internal/federation/pipe_targets.go:87-167,169-295`;
`internal/store/federated_pipe_contacts.go:49-195`).

The cached result is only a local queue-routing hint. Immediately before every
delivery attempt, the durable outbox performs a fresh authenticated live
resolution and requires the exact policy epoch, agreement, contact ID, contact
revision, agent, and chain to match. No intent or payload leaves this SAGE from
the cached snapshot alone (`internal/federation/pipe_outbox.go:171-220`).

### `POST /v1/pipe/send`

Send a pipeline message to another agent or provider.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `to_agent` | string | no* | Exact agent_id (* one of to_agent or to_provider required) |
| `to_provider` | string | no* | Provider name or agent name; resolves to agent_id if unambiguous |
| `source_chain_id` | string | for federation | Exact local chain returned by `/v1/pipe/resolve`; source-node binding inside the signed agent proof |
| `destination_chain_id` | string | no | For a federated send, the exact chain returned by `/v1/pipe/resolve`; requires exact `to_agent` and empty `to_provider` |
| `intent` | string | no | Human description of the work |
| `payload` | string | yes | Arbitrary content |
| `ttl_minutes` | int | no | 1–1440; defaults to 60 |

For a local send, the target must be registered here. For a federated send,
call `/v1/pipe/resolve` first and sign its exact `source_chain_id`, `to_agent`,
and `destination_chain_id`. Friendly `#node/agent` aliases are rejected by the send
route itself so a display alias cannot be rebound after signing
(`api/rest/pipe_handler.go:161-213`). A remote send also requires a fresh
nonce-bound Ed25519 agent proof; bearer-only and legacy nonce-less identity
cannot cross the federation edge (`api/rest/pipe_handler.go:261-283`).

**Response** (HTTP 201):
`{"pipe_id":"pipe-<uuid>","status":"pending","expires_at":"...","destination_chain_id":"<remote chain or empty>"}`.
For a remote send, `pending` means durably queued for delivery; it does not claim
that the peer or agent has already received the work. Consequently, an exact
address resolved from the bounded offline cache can be accepted locally while
the peer is down; delivery waits for that peer to return and pass the fresh live
authorization preflight above.

**Size caps → HTTP 413.** `payload` is capped at 256 KiB and `intent` at 8 KiB (`MaxPipeContentBytes`/`MaxPipeIntentBytes`, `internal/store/store.go:513-515`). The REST handler fast-fails an over-cap request with **413** before the store write; the store enforces the same caps at the `InsertPipeline` chokepoint (`internal/store/sqlite.go:4083` payload, `:4086` intent) as defense in depth, mapping `ErrPipePayloadTooLarge`/`ErrPipeIntentTooLarge` (`store.go:527-529`) to 413.

**Open-pipe quota → HTTP 429 + `Retry-After`.** A single verified agent identity may hold at most 256 non-terminal (pending or claimed) pipes open at once, and a node caps 10000 across all requesters (`MaxOpenPipesPerAgent`/`MaxOpenPipesGlobal`). An index-backed COUNT and its INSERT run under the same write critical section, so parallel sends cannot race past either cap. Over-quota inserts are rejected as **429 with `Retry-After`** (`ErrPipeQuotaPerAgent`/`ErrPipeQuotaGlobal`), keyed on the Ed25519-verified `from_agent`, not the spoofable rate-limit header. This mirrors the mempool-full recipe (see `GET /v1/chain/backpressure` below): treat it as backpressure and retry after the hinted interval, not as a per-agent rate-limit breach.

Federated admission additionally caps one authenticated source chain at 1024
open imported rows (`MaxOpenPipesPerPeer`, `internal/store/store.go:539-541`;
`internal/store/sqlite.go:4798-4821`).

Stale pipes are reaped independently: pending or claimed rows older than 48h are force-expired regardless of their stamped TTL (`ExpireStalePipelines`, wired into the 5-minute sweep plus a boot one-shot), and terminal rows purge 24h after creation.

---

### `GET /v1/pipe/inbox`

Fetch pending messages for the authenticated agent (by agent_id or provider). Auto-claims all returned items.

**Query parameters:** `limit` (1–20, default 5)

**Response** (HTTP 200): `{"items": [...PipelineMessage], "count": N}`.
An empty inbox is always encoded as `{"items":[],"count":0}`, never
`items:null`; the Python SDK also normalizes `null` from an older node to an
empty list (`api/rest/pipe_handler.go`; `internal/store/sqlite.go`;
`sdk/python/src/sage_sdk/models.py:315-323`).
Foreign items carry additive immutable provenance including
`source_chain_id`, stable `source_pipe_id`, exact sender/recipient identities,
and agreement/policy/contact bindings. REST clients must treat foreign payloads
as untrusted input; the MCP `sage_inbox`/`sage_turn` formatter makes that
boundary explicit with `foreign:true` and `trust:"external_untrusted"`. They use a
fresh local `pipe_id`; a wire event ID is never adopted as the receiver's local
primary key (`internal/store/store.go:563-606`,
`internal/federation/pipe_transport.go:276-418`).

Concurrent inbox reads return only messages whose claim compare-and-swap the
caller actually won; a losing reader never receives the same work item.

---

### Task assignment and agent notices

`GET /v1/dashboard/tasks?all=true&limit=N` is the local-human CEREBRUM Kanban feed; signed agents receive `403` and use the scoped backlog instead. It returns explicit `memory_type=task` records across `planned`, `in_progress`, `done`, and `dropped` on both SQLite and PostgreSQL; ordinary agent conversations/observations are not inferred as tasks. Historical task rows whose older writer did not persist `task_status` are returned with an empty status so CEREBRUM can ask the operator to classify each one—SAGE does not guess that unknown work is Planned or Done. New PostgreSQL inserts persist `TaskStatus`, matching SQLite. Each task also returns `task_status_updated_at`; CEREBRUM uses that lifecycle timestamp—not the task's original `created_at`—to keep Done/Dropped cards visible for seven days after their terminal transition. Manual Clear remains available before that window expires, while older cards can still be revealed with Show all (`web/handler.go:1919-1962`, `web/static/js/app.js:2328-2340`).

`PUT /v1/dashboard/tasks/order` accepts `{"task_status":"planned","task_ids":["id-a","id-b"]}` from a local/authenticated CEREBRUM operator. It persists the supplied top-to-bottom order within that status column; omitted cards retain their relative order after the supplied cards. Moving a card to another status resets its board position so it arrives at the top of the destination column. CEREBRUM reads the backend maximum of 500 board cards and exposes accessible up/down controls on each card (`web/handler.go`, `internal/store/sqlite.go`, `internal/store/postgres.go`, `web/static/js/app.js`).

Terminal task transitions retain `assignee` as the last responsible agent for board attribution while setting the handoff gate that prevents terminal pickup. Done/Dropped cards render that identity as read-only “Completed by”/“Dropped by” metadata instead of an editable assignment selector. Reopening to Planned clears the historical assignee and requires a fresh operator handoff; direct terminal-to-In-Progress transitions are rejected. Upgrade repair backfills terminal attribution from authenticated `task_picked_up_by` evidence where older versions already cleared `assignee`. For older agent-authored cards with neither field, it uses `submitting_agent` only when that exact ID exists in the agent registry; it never guesses from the provider label (`internal/store/sqlite.go`, `internal/store/postgres.go`, `web/static/js/app.js`).

`PUT /v1/dashboard/tasks/{id}/assign` accepts `{"assignee":"<agent-id>"}`
(empty unassigns). This is a local CEREBRUM operator action; callers presenting
an agent identity cannot assign or reassign work. On an unencrypted personal
node, local processes are part of the documented trusted operator boundary, so
same-origin headers prevent browser CSRF but are not authentication against
untrusted local software. Encrypted nodes additionally require the dashboard
session. The target must be an active registered agent and must be
allowed to read a non-public task. SQLite commits the assignee, monotonic
assignment generation, in-progress transition, pickup reset, retirement of the
prior notice, and new one-way notice atomically. Repeating the same assignment
is a true no-op that preserves pickup evidence and does not duplicate notices.
Moving a task to `done` or `dropped` retains its current assignee as terminal
attribution alongside the pickup evidence. Reopening clears that historical
assignee and remains unassigned until the operator hands it off again, which
creates a fresh generation and notice.
For signed agents, the scoped backlog contains only tasks whose assignee exactly
matches the verified agent ID. `in_progress`, `done`, and `dropped` all require
that same current active assignee, using an atomic owner/status transition.
Unassigned work is human triage and cannot be self-claimed. Agents cannot
re-plan or reopen work; those transitions stay on the local operator board.
Current task read permission is checked before every agent status change.

Task content/creation follows the memory consensus path. Board workflow metadata—assignee, assignment generation, status transitions, and agent inbox notifications—is deliberately local-node operational state. It is not federated or consensus-replicated: agents claim work from the node they are connected to, while other nodes may independently organize the same shared task memory. CEREBRUM labels the board as “This computer's work queue” so this boundary is explicit.

A task submitted by a verified agent enters consensus as `planned` and is
locally assigned to that creating agent as part of the consensus-flushed
off-chain insert. Starting it is a subsequent local exact-owner transition. An
ownerless historical `in_progress` row is repaired back to `planned` at startup
so the operator can triage it safely.

`GET /v1/dashboard/task-notifications?limit=5` is signed with `X-Agent-ID` and
peeks current notices, applies current active-agent and task RBAC checks, then
acknowledges only the notices actually returned. A transient authorization
lookup failure leaves the notice unread for retry; a definitive denial retires
it. Notices are not
pipeline jobs and require no result. `sage_inbox` merges them into its response;
`sage_inception` instructs agents to check both `sage_backlog` and `sage_inbox`
at session start.
The handler uses only the signature-verified agent identity bound by dashboard
authentication, rechecks that the agent is active, and rechecks current task
read permission before returning each notice. A bare `X-Agent-ID` header cannot
read or consume another agent's inbox.

---

### `PUT /v1/pipe/{pipe_id}/claim`

Atomically claim a pipeline message (prevents double-processing).

**Response** (HTTP 200): `{"pipe_id": "...", "status": "claimed"}`. HTTP 409 if already claimed.

---

### `PUT /v1/pipe/{pipe_id}/result`

Submit a result for a claimed message. Purely local completion keeps the
existing auto-journal summary. Federated completion does not journal and queues
the result over the original agreement-bound return route
(`api/rest/pipe_handler.go:538-633`).

**Request body:**

| Field | Type | Required |
|---|---|---|
| `result` | string | yes |
| `source_pipe_id` | string | for foreign work | Stable source proof/event ID returned with the inbox item; prevents replying against stale foreign metadata |
| `source_chain_id` | string | for foreign work | Exact local reply-source chain returned as `reply_source_chain_id` by the pipe status preflight; prevents another node relabeling the signed result |

`result` is capped at 256 KiB (`MaxPipeContentBytes`, `store.go:513`); an over-cap submission is rejected **HTTP 413**, enforced both at the handler and at the `CompletePipeline` store chokepoint (`sqlite.go:4190`, mapping `ErrPipeResultTooLarge`).

**Response** (HTTP 200):
`{"status":"completed","journal_id":"<memory_id or empty>","journaled":true|false}`.
For foreign work, `journal_id` is empty and `journaled` is false. MCP supplies
`source_pipe_id` automatically after its status preflight, so the ordinary
`sage_pipe_result(pipe_id, result)` interface does not gain another user step
(`internal/mcp/tools.go:2288-2333`).

`completed` means the foreign result and its signed return event were committed
atomically to the local durable outbox. It is queued, not yet a peer delivery
receipt; terminal delivery feedback is claimed through `/v1/pipe/updates`.

---

### `GET /v1/pipe/{pipe_id}`

Get current status of a pipeline message.

**Response** (HTTP 200): Full `PipelineMessage` object.

---

### `GET /v1/pipe/results`

Completed pipeline messages sent by the authenticated agent.

**Query parameters:** `limit` (1–20, default 5)

**Response** (HTTP 200): `{"items": [...], "count": N}`. Empty results use
`items:[]`, never `items:null`.

---

### `GET /v1/pipe/updates`

Atomically claims payload-free terminal delivery notices for federated sends
or results signed by the authenticated local agent. This closes the asynchronous
failure loop: a sender learns that remote work was never accepted, and a local
completer learns if its queued result could not return. Returned notices are
marked reported and do not repeat on every turn.

**Query parameters:** `limit` (1–20, default 5)

**Response** (HTTP 200):
`{"items":[{"event_id","pipe_id","event_kind":"send|result","remote_chain_id","target_agent_id","state":"failed","attempts","last_error","created_at"}],"count":N}`.
An empty update set uses `items:[]`, never `items:null`; current Python clients
also tolerate the older null encoding.
No intent, payload, result, or proof bytes are exposed. `last_error` may contain
peer-originated diagnostic text and must be presented as external/untrusted.
`sage_turn` polls this route and returns actionable `pipe_delivery_updates`.

---

## 11. Operational (No Auth)

### `GET /health`

Liveness probe. No auth.

**Response** (HTTP 200): `{"status": "healthy"}`

The `version` field is intentionally omitted — `/health` is reachable through the wizard tunnel allowlist, so it stays minimal to avoid version-fingerprinting an internet-exposed node (`internal/metrics/health.go`). Returns HTTP 503 `{"status": "unhealthy"}` when a dependency (PostgreSQL or CometBFT) is down.

---

### `GET /ready`

Readiness probe. Checks the store (PostgreSQL/SQLite), CometBFT, the embedding
provider, and any required v11.9 scoped serving projection. No auth.
(`internal/metrics/health.go`)

**Response** (HTTP 200 or 503):

```json
{
  "status": "ready",          // ready | degraded | not_ready
  "postgres": true,
  "cometbft": true,
  "scoped_projection": {
    "checked": true,
    "required": true,        // canonical scoped envelopes exist in Badger
    "ok": true,
    "records": 42,
    "rebuilt": 42,
    "detail": ""
  },
  "embedder": {
    "checked": true,          // false until the watchdog's first probe
    "ok": true,
    "semantic": true,         // false = hash fallback (a capability note, not a fault)
    "provider": "ollama",
    "model": "nomic-embed-text",
    "detail": ""              // error summary when ok=false
  }
}
```

Status semantics:
- `not_ready` → **HTTP 503**: core infrastructure (store or CometBFT) is down,
  or canonical scoped envelopes exist but the local SQL serving projection is
  locked, incomplete, or failed verification. A locked SQLite vault retries the
  scoped rebuild after unlock.
- `degraded` → **HTTP 200** by default: core is up but a *semantic* embedder has been
  probed and is unreachable, so hybrid/semantic recall has dropped to keyword-only.
  The node still serves. Pass `?strict=1` to make this a **503** for gates that
  require semantic recall.
- `ready` → **HTTP 200**: everything healthy. A hash (non-semantic) provider is
  `ready` — non-semantic is a capability, not a fault. An embedder not yet probed is
  also `ready`.

The embedder status is refreshed by a ~30s background watchdog (see the node's
`startEmbedderWatchdog`).

---

### `GET /v1/chain/backpressure`

First-class mempool backpressure signal so clients can pace writes without polling
raw CometBFT RPC. Ed25519-authed. Served from a ~1s-TTL cache (safe to poll tightly).
(`api/rest/mempool.go`)

**Response** (HTTP 200):

```json
{
  "mempool_txs": 2100,
  "mempool_bytes": 5242880,
  "mempool_max_txs": 5000,       // the real runtime cap (CometBFT DefaultConfig)
  "mempool_pct": 0.42,           // mempool_txs / mempool_max_txs, 0..1
  "accepting_writes": true,      // false at pct >= 0.9
  "retry_after_ms": 0            // > 0 (a back-off hint) only when near cap
}
```

Returns **HTTP 503** (problem+json) when the CometBFT RPC probe fails.

Every successful `POST /v1/memory/submit` also carries an **`X-Sage-Mempool-Pct`**
response header (e.g. `"0.42"`), so streaming writers can self-throttle with zero
extra round-trips. A memory submit rejected because the mempool is full now returns
**HTTP 429 + `Retry-After`** with a distinct RFC-7807 problem type
(`https://sage.dev/errors/mempool-full`, separate from the rate limiter's
`.../errors/429`) instead of an opaque 500 — treat it as backpressure and retry after
the hinted interval, not as a per-agent rate-limit quota breach.

---

## Node Operator Bypass

If the server has `nodeOperatorID` configured (`server.go:50-57`), requests signed with that key bypass agent-isolation RBAC (the cross-agent visibility filter) on all read paths. Domain access and classification gates still apply. This lifts the prefetch limit on nodes where the LLM's registered identity is separate from the operator key.

---

## Environment Variables

| Variable | Default | Effect |
|---|---|---|
| `SAGE_TX_COMMIT_TIMEOUT_MS` | 60000 | `broadcast_tx_commit` client timeout |
| `VALIDATOR_KEY_FILE` | — | Path to CometBFT `priv_validator_key.json`; `amid` injects this concrete key into REST in socket mode, while in-process runtimes inject the key under `--home`. Governance is disabled rather than using the compatibility random key when unavailable. |
| `SAGE_GOVERNANCE_OPERATOR_ID` | — | `amid` only: canonical hex Ed25519 identity allowed to authorize this validator's REST governance mutations. Equivalent flag: `--governance-operator-id`. Empty disables governance mutations. `sage-gui` wires its local `agent.key` identity directly. |
| `CORS_ALLOWED_ORIGINS` | `*` | Comma-separated allowed origins |

---

## OpenAPI Status

`api/openapi.yaml` is reconciled for the core REST surface documented here. The remaining known gap is response-shape precision on a few organization, federation, and department `GET` routes, where the spec still uses generic objects while the handlers return concrete structs.
