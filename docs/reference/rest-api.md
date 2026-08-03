<!-- Reconciled through SAGE v11.17.1. Cite file:line when behavior is non-obvious. -->

# SAGE REST API Reference

## Authentication

Most core `/v1/*` REST endpoints require Ed25519 request signing (`api/rest/middleware/auth.go:81-213`). Public and alternate-auth exceptions include health/readiness routes, OAuth discovery/flows, and the HTTP MCP transports (`/v1/mcp/sse`, `/v1/mcp/messages`, `/v1/mcp/streamable`), which use MCP bearer-token/OAuth authentication.

| Header | Format | Purpose |
|---|---|---|
| `X-Agent-ID` | 64-char hex Ed25519 pubkey | Identifies the agent |
| `X-Signature` | hex-encoded Ed25519 sig | Signs the canonical payload |
| `X-Timestamp` | unix epoch seconds | Prevents replay |
| `X-Nonce` | 8 random bytes, hex encoded | Fresh replay nonce; required for current clients and exact signed actions |

The REST CORS preflight allowlist includes all four signing headers, including
`X-Nonce`; browser clients do not need to fall back to the legacy nonce-less
signature shape (`api/rest/server.go`).

**Signed message construction** (`auth.go:156-180`):

```
canonical = METHOD + " " + PATH[?QUERY] + "\n" + BODY
message   = SHA-256(canonical) + bigEndian(timestamp_int64) + nonce
```

Always include a fresh 8-byte `X-Nonce` on current clients. The generic
authentication middleware temporarily accepts the historical nonce-less
signature shape for compatibility, but exact message, acknowledgement,
receipt, and delegated-governance actions reject it. Do not build new clients
against the legacy form.

**Constraints:**
- Timestamp must be within ±5 minutes of server time (`auth.go:79`).
- Duplicate `(agentID, signature)` pairs within the 5-minute window are rejected (replay cache, `auth.go:27-53`).
- Body is capped at 1 MB before reading for signature verification (`auth.go:143`).
- `X-Agent-ID` is the hex-encoded Ed25519 **public key** (32 bytes = 64 hex chars); it IS the agent identity on-chain.

**Post-app-v17 consensus binding.** The REST process is not the trust boundary. Once app-v17 is active, a transaction whose outer node key differs from `X-Agent-ID` carries the exact `canonical` bytes above in the optional `ParsedTx.AgentRequest` wire tail. `FinalizeBlock` re-hashes those bytes to `AgentBodyHash`, re-verifies the Ed25519 proof, rejects proofs more than five minutes older than deterministic block time, and rebuilds the expected type-specific payload from the signed method, path, and JSON. Historical non-governance proofs intentionally remain valid when ahead of block time because SAGE mints no idle heartbeat blocks and the first block after a long idle period can lag the already wall-clock-validated REST request. App-v20 governance is narrower: every governance envelope containing any agent-proof material, including one whose embedded signer equals the outer validator, must carry the complete request and 8-byte nonce and must fall within **±5 minutes** of deterministic block time. For memory submissions, content/type/domain/confidence/classification/parent/task status stay action-bound while the embedding hash is node-derived and outer-signature-bound: v11.7.4 made the active node authoritative for vector generation after request authentication. App-v23 adds one narrow deterministic exception: a task request may omit `domain_tag`, in which case both REST and consensus independently resolve the exact currently committed ordinary-agent home domain; an explicit domain remains byte-for-byte action-bound and is never remapped. A mismatch is rejected with code 109 before the action handler runs. A successful validation atomically claims an AppHash-folded proof fingerprint until its freshness window closes, so the same agent authorization cannot be wrapped in a second node transaction with a fresh outer nonce. Ordinary same-key non-governance transactions and **truly proofless** direct governance/upgrade-auto-voter transactions need no HTTP envelope: the outer signature binds the payload and app-v9's monotonic nonce prevents same-chain replay (`internal/abci/agent_proof.go`, `internal/store/badger.go`).

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
| `domain_tag` | string | yes* | Exact domain label; agent must have write access. *App-v23 task submissions by an active ordinary agent may omit it to use the currently committed owned home domain. Other memory types and pre-v23 requests still require it. Explicit values are never remapped.* |
| `confidence_score` | float64 | yes | 0.0–1.0 inclusive |
| `classification` | int | no | 0–4; see table below. **Omitting sends 0 (PUBLIC)** |
| `embedding` | []float32 | no | Compatibility field. The node regenerates the vector from `content` with its currently selected provider so stale/foreign vector spaces cannot mix. |
| `knowledge_triples` | []KnowledgeTriple | no | `{subject, predicate, object}` triples |
| `parent_hash` | string | no | SHA-256 hex of parent memory for lineage |
| `task_status` | string | no | New `task` memories must omit this or send `planned`. Agents start or finish an assigned task through the task-status route after creation. |
| `linked_memories` | []string | no | Related memory IDs for legacy/non-idempotent submission paths. App-v23 task creation rejects this field because links are not part of the canonical task transaction; create links separately after the task is confirmed. |
| `tags` | []string | no | Up to 32 labels of 128 UTF-8 bytes each. Above app-v20 they are sorted/deduplicated into the signed tx; scoped-domain tags are also AppHash-covered and projection-recoverable. Ordinary-domain tags remain node-local. OR-filter on query/search. |
| `provider` | string | no | Stored off-chain only; not on-chain |
| `idempotency_key` | string | no | App-v23 tasks only; 1–128 visible ASCII bytes without spaces. If omitted, REST derives the same permanent semantic key as MCP from the exact signed agent ID, resolved domain, and task content. Repeating that semantic task returns the original receipt at its current status. Supply a fresh explicit key only to intentionally create another occurrence with identical content/domain. |

**Classification values** (`internal/tx/types.go:84-90`):

| Value | Name | Meaning |
|---|---|---|
| 0 | PUBLIC | Readable by any federated org |
| 1 | INTERNAL | Own org only (default clearance gate) |
| 2 | CONFIDENTIAL | Own org + explicit cross-org grants |
| 3 | SECRET | Own org, specific dept, explicit grant |
| 4 | TOPSECRET | Named agents only, dual-approval |

> **Critical:** An **omitted** `classification` field is deserialized as `0` (PUBLIC) by Go's JSON decoder and is stored as PUBLIC on-chain. This is the intended behavior since v6.8.6 — the prior code silently bumped `0→INTERNAL` at submission time, causing every cross-agent read of a PUBLIC memory to be blocked by the classification gate. (`internal/abci/app.go:960-969`). The codec still defaults old txs without a classification byte to INTERNAL for backward compatibility (`internal/tx/codec.go`), but new submissions from this REST endpoint are stored as-sent.

**Success responses:**

| HTTP | Meaning |
|---|---|
| `201 Created` | A new transaction committed and, for an app-v23 task, the exact assignee projection was confirmed. |
| `200 OK` | The task's permanent idempotency key already had a committed receipt. The response returns the original `memory_id`, `tx_hash`, and height plus the task's current status, including `done` or `dropped`; no new task was created. |
| `202 Accepted` | The transaction committed, but the exact local task projection could not be confirmed. This is not permission to resubmit: reconcile the returned `memory_id`. |

```json
{
  "memory_id": "<uuid>",
  "tx_hash": "<hex>",
  "status": "proposed",
  "task_status": "planned",
  "committed": true,
  "committed_height": 123,
  "projection_confirmed": true,
  "idempotency_key": "mcp-<sha256>",
  "embedding_provider": "ollama"
}
```

`committed:true` means `broadcast_tx_commit` returned successfully and the
transaction is on-chain. It does **not** by itself prove that every serving
projection is readable. `status:"proposed"` is the memory's governed lifecycle
state, not an indication that the transaction is still pending.

A confirmed replay adds `"idempotent_replay":true` and
`"projection_confirmed":true`. A committed-but-unconfirmed task response is:

```json
{
  "memory_id": "<exact committed uuid>",
  "tx_hash": "<exact committed tx hash>",
  "status": "committed_unconfirmed",
  "task_status": "planned",
  "committed": true,
  "committed_height": 123,
  "projection_confirmed": false,
  "retryable": false,
  "message": "The transaction committed, but the exact task projection could not be confirmed. Reconcile this memory_id; do not resubmit the task.",
  "idempotency_key": "mcp-<sha256>"
}
```

The node performs bounded exact-assignee projection confirmation before it
chooses `201`, `200`, or `202`. An idempotent replay whose projection is still
unconfirmed also returns `202` and adds `"idempotent_replay":true`.

The key is permanently scoped to the effective policy principal. Its canonical
binding covers that principal, exact assignee, stable memory ID, content hash,
task type, exact resolved domain, confidence bits, content, parent hash,
classification, initial `planned` status, and normalized tags. The node-owned
embedding hash is deliberately excluded. Reusing a key with a different
canonical payload is a stable `409`; keyed task creation rejects
`knowledge_triples` and `linked_memories`, so links must be submitted after
creation.

If the selected provider is temporarily unavailable, the memory still commits
without accepting the caller's possibly mismatched vector and the response adds
`"embedding_queued": true`. The node repairs it automatically once that same
provider is healthy and the vault is unlocked.

**Auth:** Ed25519 required. Agent must have write access to `domain_tag` if per-domain access control is configured (`memory_handler.go:425-428`). Observer-role agents are rejected.

**Effective write denial:** A recognized preflight or consensus denial returns
HTTP 403 with RFC 7807 type
`https://sage.dev/errors/domain-write-denied` and the extensions
`reason_code`, `remedy`, and `retryable:false`. `detail` is deliberately generic;
neither it nor the extensions disclose the agent ID, requested domain, owner ID,
raw capability mask, or raw consensus log. `retryable:false` means “do not
automatically repeat this request”; the named operator action may make a later,
new request valid.

| `reason_code` | Effective cause | Exact `remedy` |
|---|---|---|
| `missing_write_grant` | No effective level-2 write grant | Submit to a domain this agent owns. If shared management is intended, a local Root/Admin can place the principals in an Access Group and explicitly select Read + write or Read + write + modify. CEREBRUM has no direct level-2 grant editor. |
| `foreign_write_restricted` | The effective named profile includes the deny-foreign-write restriction (app-v22 bit 8) | Assign a write-compatible named profile that permits foreign-domain writes, or submit to a domain this agent owns. |
| `shared_write_restricted` | The effective named profile includes the deny-shared-write restriction (app-v22 bit 2) | Submit to the agent's owned non-shared home domain, or assign a named profile that permits shared-domain writes. |
| `domain_claim_restricted` | The effective named profile includes the deny-domain-claim restriction (app-v22 bit 4) | Submit to a domain this agent already owns, or ask a local administrator to assign or reassign a non-shared domain; this profile cannot claim an unowned domain. |
| `principal_pending_review` | Enrollment has not been approved | Approve the agent's enrollment and assign its role and named profile before submitting. |
| `no_owned_home_domain` | Enrollment has no owned non-shared home domain | Complete enrollment by assigning an owned non-shared home domain, then submit there. |
| `manager_scope_denied` | A manager write is outside its local authority scope | Use an owned domain, or ask a local Root/Admin to add the exact local relationship to an Access Group with an explicit write-capable authority tier. |

The classifier is intentionally narrow. A non-zero capability mask does not by
itself invalidate a grant: only the matching effective restriction produces its
reason code. Unknown structured codes and generic `access denied` or
`not registered` responses remain opaque and untyped. A client recognizes a
canonical denial only when the problem type and one of these seven codes match,
the HTTP and problem status are both 403, and `retryable:false` is explicitly
present. MCP derives the remedy from its local canonical table rather than
trusting response text; only that complete contract suppresses re-registration
and retry (`internal/authzdenial/write_denial.go`, `memory_handler.go`,
`internal/mcp/server.go`).

Other task-specific failures are also stable:

- `409 https://sage.dev/errors/app-v23-required` when an explicit task key is
  sent before app-v23;
- `409` when the same key is already bound to a different canonical payload;
- `503 https://sage.dev/errors/task-assignment-bridge-unavailable` before any
  transaction is submitted when the node cannot durably stage the assignee; and
- `503` without retry broadcast when the authoritative idempotency receipt
  cannot be read.

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
      "challenge_count": 1,
      "evidence_counts_available": true,
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
    "by": ["rbac_submitting_agents"]
  }
}
```

`confidence_score` in the response is the **decayed** value (time decay + corroboration boost applied server-side), not the raw submitted value. `initial_confidence` is the **stored** (undecayed) value — the on-chain confidence set at submission (corroboration never rewrites it) — so a client can see the authoritative floor alongside the decayed score without re-deriving it. It is present for local memories and omitted for federated results (where only the serving peer's already-decayed value is available).

`corroboration_count` is the number of **distinct corroborating agent IDs** and
feeds the confidence boost. `challenge_count` is the number of **distinct
challenger IDs** in the off-chain lifetime audit projection. The latter is
evidence history, not an open-vote count. `evidence_counts_available` is `true`
only when both count queries succeeded and no durable recovery/repair-incomplete
marker was detected. It is not a cryptographic attestation against arbitrary
out-of-band partial SQL audit-table corruption. When `false`, both numeric
fields may still contain canonical lower bounds reconstructed during pristine
state sync or repair; in particular, zero is not proof that no historical
evidence existed. While an app-v21 weighted round is open, `challenge_round`,
`current_challenger_count`, and
`required_challengers` expose its authoritative consensus progress. Those three
fields are absent for closed rounds and legacy app-v17 challenges.

`disputed` (`memory_handler.go`, `json:"disputed,omitempty"`) is present and `true` only for an app-v17 or app-v21 **challenged** memory that is still live and recallable while under dispute (set on all three recall paths: query, search, and hybrid). Because it is `omitempty`, its absence means "not disputed," which is why the committed example above omits it. When it is set, `confidence_score` already carries an extra query-time **disputed haircut** (the shared `store.DisputedConfidenceHaircut`, currently a `0.8` multiplier) layered on top of decay and corroboration. The store applies the same multiplier while enforcing `min_confidence`, so a returned result still satisfies the advertised floor after serialization. The haircut is presentation-only: it leaves the on-chain `status` (`challenged`) and the stored confidence untouched. Under legacy/app-v17 rules a personal one-holder domain resolves immediately, but post-app-v21 that same domain emits `disputed` whenever `k>0`; only `k=0` remains immediate. A challenged memory's on-chain `status` is `challenged`, already listed among the queryable `status_filter` values above.

`min_confidence` is enforced against the **decayed** `confidence_score`, not the stored column. The store applies it before the top-K trim, so: a result returned by a `min_confidence=X` query always satisfies `confidence_score >= X` (including federated results, which are re-checked against the floor on the requesting side); a corroboration-boosted memory whose stored value is below `X` but whose decayed value clears it is still returned; and `top_k` is filled with qualifying records rather than truncated. SQLite semantic recall scans every candidate. Before app-v23, SQLite FTS/hybrid and Postgres semantic recall use the bounded top `decayFilterScanCap` (1000) rank/distance candidates, so an otherwise qualifying record beyond that legacy pool may not surface. Once app-v23 is active, the authorization-aware ranked page walk evaluates the decay floor on each page too and continues until the visible `top_k` is filled or the stream is exhausted. Open tasks are exempt from decay, so an open task is judged by its stored confidence. (Prior to v11.2.0 the floor compared the stored column, which both leaked aged memories below the floor and dropped boosted ones above it.)

Once app-v23 is active, live record disclosure is also applied **before**
`top_k` is consumed. SQLite semantic recall filters its complete ranked
candidate set; SQLite text recall and Postgres semantic recall walk stable,
bounded ranked pages until they fill the requested authorized result count or
exhaust the stream. Query, search, and hybrid then repeat the disclosure check
immediately before serialization. Revoked or above-clearance rows therefore do
not starve later readable results, and no denied-row count is returned.

Before app-v23, `filtered` may include legacy `hidden_count`,
`total_before_filter`, or `visible` counts. App-v23 never returns denied or raw
inventory counts. Its optional `filtered.by` and
`X-SAGE-Filter-Applied` header identify an authorization path that was applied;
they are not proof that any particular row was hidden.

Under app-v23 every record must pass the intersection of active enrollment, any
immutable legacy visibility envelope, current domain/group/grant scope, and
classification clearance. Domain access no longer disables a separate
agent-isolation check, and neither authorship nor task assignment is a read
bypass.

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

Fused FTS5 + vector search via Reciprocal Rank Fusion. Requires at least one of
`query` or `embedding`. Supports query expansions for multi-variant recall.

**Request body** (`memory_handler.go:2612-2637`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `query` | string | no* | Text query (* at least one of query/embedding required) |
| `embedding` | []float32 | no* | Vector for similarity arm |
| `expansions` | []HybridExpansion | no | At most 8 paraphrase variants; each has `query` + `embedding` |
| `domain_tag` | string | no | |
| `provider` | string | no | |
| `min_confidence` | float64 | no | |
| `status_filter` | string | no | |
| `top_k` | int | no | Applied after RRF fusion across variants |
| `tags` | []string | no | |

**Response:** Same shape as `/v1/memory/query`.

The `expansions` array may contain at most eight entries. The limit is checked
against the submitted array length before blank expansion entries are skipped;
nine entries therefore return HTTP `422` even if one entry is empty.

Under app-v23 a hybrid request has one shared budget of 8,192 raw live-
authorization checks across the primary query, every expansion variant, and
every text/vector store leaf behind those variants. The budget is not reset per
variant or per leaf, so adding paraphrases cannot multiply the authorization
scan allowance. Exhaustion returns HTTP `422` (`Hybrid query too broad`); reduce
the expansion count or narrow domain/provider/tag/status filters.

Governed hybrid execution fails closed. If any variant or store leaf cannot
complete its live authorization decision, or if a decayed-confidence floor
cannot be evaluated, the entire request returns the applicable error rather
than HTTP `200` with a partial RRF result assembled from only the successful
leaves.
Unavailable app-v23 classification state returns HTTP `503`; an untyped
internal leaf failure returns HTTP `500`. Only an ungoverned pre-app-v23 request
without a decay floor may skip a failed expansion as best-effort enrichment.

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
  "corroboration_count": 2,
  "challenge_count": 1,
  "evidence_counts_available": true,
  "linked_memories": [...]
}
```

Under app-v23 the same live record-disclosure intersection used by recall and
list applies to direct lookup. Authorship is immutable provenance, not current
read authority: even the submitting agent receives `403` after its live domain,
group, grant, enrollment, or clearance authority is revoked.

---

### `GET /v1/memory/list`

Paginated memory list with RBAC agent isolation. Read from off-chain store.

**Query parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `limit` | int | 50 | Max 200 |
| `offset` | int | 0 | App-v23 max 7,900 |
| `domain` | string | | Filter by domain |
| `tag` | string | | Filter by single tag |
| `provider` | string | | |
| `status` | string | | |
| `sort` | string | | Store-defined sort field |
| `agent` | string | | Filter by submitting agent ID |

**Response:** `{"memories": [...], "total": N, "limit": N, "offset": N, "has_more": true, "total_exact": false, "filtered": {...}}`

Under app-v23 the response also includes `has_more` and `total_exact`.
Authorization is applied while walking bounded store pages, before the visible
`offset`/`limit`. When `has_more` is `true`, `total` is the privacy-safe visible
lower bound discovered so far (normally `offset + limit + 1`) and
`total_exact` is `false`; it never exposes the underlying raw or denied count.
When the authorized stream is exhausted, `total_exact` is `true` and `total` is
the exact visible total. This avoids both revoked-prefix starvation and a
full-inventory scan for the first page on mature nodes.
One app-v23 request examines at most 8,192 raw authorization candidates. An
offset above 7,900, or a sparse result that cannot fill the requested visible
page within that budget, returns HTTP `422` (`Offset too large` or
`Query too broad`). Narrow the domain/provider/tag/status/agent filters or page
sequentially; repeating the same broad request does not increase the budget.

The same 8,192-candidate ceiling applies wherever app-v23 must walk raw records
before producing an authorization-filtered answer: ranked query/search/hybrid,
validator pending-memory, assigned-task, and timeline candidate scans. These
routes return HTTP `422` with an endpoint-specific `... query too broad`
problem and advice to narrow the available filters. Denied/raw inventory counts
are never returned with that failure.

---

### `GET /v1/memory/timeline`

Memory creation counts grouped by time bucket.

**Query parameters:**

| Param | Type | Default | Notes |
|---|---|---|---|
| `domain` | string | | Filter |
| `bucket` | string | `hour` | Time bucket size |
| `from` | RFC3339 | now-24h | App-v23 requires valid RFC3339 |
| `to` | RFC3339 | now | App-v23 requires valid RFC3339 |

**Response:** `{"buckets": [...], "total": N}` where `total` is the sum of the
currently visible bucket counts.

Before app-v23 the historical no-domain aggregate is global. Under app-v23
aggregate existence and timing are governed metadata: the handler walks stable,
bounded memory pages across the exact requested range, applies the central live
record-disclosure decision to every row, and counts only records currently
visible to the caller. A concrete `domain` still requires current domain read
authority. Corrupt or unavailable authorization state returns `503` rather than
falling back to global counts. App-v23 limits one request to 31 days and 8,192
raw candidates; a wider or excessively dense request returns `422` and must be
narrowed by time or domain. Bucket strings preserve the selected backend's
pre-v23 representation (SQLite date/week/month strings; PostgreSQL UTC
`date_trunc` RFC3339 strings).

---

### `GET /v1/memory/tasks`

Open task memories (type=`task`, status != `done`/`dropped`).

**Query parameters:** `domain`, `provider`

**Response:** `{"tasks": [{memory_id, content, domain_tag, task_status, assignee, task_picked_up_by, task_picked_up_at, confidence_score, created_at}], "total": N}`.
For a signed agent this is an assigned-only feed: `assignee` is the exact
authenticated agent ID, and unassigned or differently assigned tasks are not
returned. Assignment is necessary but not sufficient: each row must also pass
the caller's current domain/group/grant and classification authority. `total`
is the visible number returned, never an underlying or denied task count.

Under app-v23, the built-in stores page the exact-assignee feed and apply live
record disclosure before the 500-visible-task response ceiling. A revoked or
above-clearance prefix therefore cannot make a later readable assigned task
disappear. Pre-app-v23 callers retain the historical one-query behavior.
Current-generation Admins may use this ordinary-agent surface only from
localhost. Root and historical Root credentials are never task assignees;
remote or stale Admin credentials are denied. A Manager's group Modify
authority does not permit it to start or finish a teammate's task.

MCP `sage_task` and `sage_backlog` use this scoped agent feed, never the local
human `/v1/dashboard/tasks` board. When a new task omits `domain`, the MCP client
reads the caller's app-v23 `home_domain` from `/v1/agent/me`; an explicit domain
is submitted exactly as written and a foreign/unowned denial is returned
unchanged. REST itself derives a permanent semantic task key when the caller
omits one and confirms the exact assignee projection before returning `201`; a
committed-but-unconfirmed projection returns `202` with
`projection_confirmed:false` and must be reconciled, not resubmitted. MCP adds
an immediate exact-assignee backlog readback before reporting a fresh task as
created.

---

### `POST /v1/memory/{memory_id}/vote`

Ask this node's local validator to cast its one vote on a proposed memory. This
is a deliberate operator override, not an agent-voting endpoint: pre-app-v23
only the configured governance operator may call it, and app-v23 accepts only
the current CEREBRUM Root or a current local Admin from localhost. Member,
Manager, stale-Admin, retired-Root, and remote callers receive `403` before the
memory ID is looked up. A node without its live validator signing key returns
`503`.

The consensus transaction contains only the validator identity. The
authorizing operator does not acquire separate voting power, and repeated calls
through one node occupy that validator's same vote slot.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `decision` | string | yes | `accept`, `reject`, or `abstain` |
| `rationale` | string | no | Human-readable justification |

**Response** (HTTP 200): `{"message": "Vote recorded successfully.", "tx_hash": "<hex>"}`

Prefer the node's auto-voter for normal operation; use this route only for an
intentional local override.

---

### `POST /v1/memory/{memory_id}/challenge`

Challenge an existing memory. Broadcasts `TxTypeMemoryChallenge`.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `reason` | string | yes |
| `evidence` | string | no |

**Response** (HTTP 200): `{"message": "Challenge submitted successfully.", "tx_hash": "<hex>", "status": "challenged|deprecated"}`

`status` reports the durable result after `broadcast_tx_commit`: `deprecated`
for a decisive one-strike challenge (or a threshold-reaching confirmation), and
`challenged` when app-v17 parks a first multi-holder challenge or app-v21 still
requires additional corroboration-weighted challengers.

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

**Response** (HTTP 200): `{"message": "Memory forgotten.", "tx_hash": "<hex>", "status": "challenged|deprecated"}`

**Error responses:** identical to `/challenge` (same deprecation gate) — `403` not authorized (or a pre-app-v16 legacy reject), `404` unknown memory id, and `409` legacy no-recorded-domain (repair via an `OpMemoryDomainRepair` governance proposal). See the challenge section above.

---

### `POST /v1/memory/{memory_id}/reinstate`

Return an open challenge to `committed`. The handler builds
`TxTypeMemoryReinstate`, embeds the authenticated caller proof, and waits for
`broadcast_tx_commit`; a successful response is durable, not only a mempool
receipt.

**Request body:** `{"reason": "optional audit note"}` (`reason` may be omitted).

**Response** (HTTP 200):
`{"message": "Memory reinstated.", "tx_hash": "<hex>", "status": "committed"}`

The chain must have activated app-v17. For a legacy app-v17 challenge, current
domain owners/ancestor owners and level-3 modify grantees may reinstate, and the
original challenger may withdraw even after their grant expires or is revoked.
For an app-v21 weighted round, only identities in the round's snapshotted
electorate may reinstate; later grant churn neither adds nor removes eligibility.

**Errors:** `400` when app-v17 is not active, `403` when the caller is not
authorized under the applicable legacy or snapshotted-round rule, `404` for an
unknown memory, and `409` when the memory is not currently challenged.

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
| `task_status` | string | yes | Agent-authorized values: `in_progress`, `done`, `dropped`. `planned` is accepted by the parser but always rejected here as a local CEREBRUM operator action. |

**Response** (HTTP 200): `{"memory_id": "...", "task_status": "..."}`

The handler rechecks current domain write authority and classification/read
authority before changing the row. `409` means the task is terminal or is not
currently assigned to this exact agent. Current-generation Admins may act only
from localhost; Root, historical Root, inactive/stale agents, and Managers
targeting another assignee are denied.

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

Signed local roster. After app-v23 the caller must be an active ordinary local
agent and the response contains only active ordinary canonical enrollments.
Root, historical Root credentials, pending/inactive agents, and inconsistent
SQL-only rows are excluded. The old unsigned full-roster endpoint was removed
in v11.16 because it exposed local RBAC/network topology and bypassed
caller-scoped recipient discovery. MCP clients should normally use the more
efficient signed `GET /v1/agents/lookup` route.

**Response** (HTTP 200): `{"agents": [...AgentEntry], "total": N}`

---

### `GET /v1/agents/directory`

Signed metadata-only local recipient directory used by `sage_directory`. It
applies the same active-ordinary canonical enrollment boundary as
`GET /v1/agents`, but returns only `agent_id`, display/registered names,
provider, and active status. SQLite and PostgreSQL cap the database query at
101 candidate rows and return at most 100 active recipients, so this route
neither derives memory counts nor walks an unbounded roster merely to populate
a picker. When `truncated` is true, use the bounded name lookup below.

**Response** (HTTP 200):
`{"agents": [...identity metadata...], "total": N, "truncated": false}`

This REST route remains deliberately local. MCP `sage_directory(scope="all")`
combines it with the separately signed, caller-filtered federation availability
projection; remote peers never receive a request for an unscoped roster.

---

### `GET /v1/agents/lookup`

Signed, bounded human-name lookup for MCP recipient discovery. After app-v23
the signed caller must be an active ordinary local agent; Root, historical Root
credentials, pending/inactive agents, and inconsistent enrollments are
rejected by the same boundary as pipeline resolve/send/inbox. `name` is
required (1–512 bytes) and performs a literal substring match over active,
non-removed agents' display name, immutable registered name, and provider.
ASCII matching is case-insensitive; non-ASCII code points require their
registered casing. Exact field matches rank before partial matches, so `mynah`
can resolve `MYNAH (SAGE Voice Bridge Agent)` without making `%` or `_` act as
wildcards. `limit` defaults to 20 and is 1–20. It returns the same sanitized
agent fields as the public roster, but uses a capped metadata projection rather
than enumerating the roster or deriving memory counts. Each row also carries
`match_kind` (`exact` or `substring`), which is the server-owned match decision
consumed by MCP. SQLite status alone is not enough after app-v23: each target
must still be an active, internally consistent ordinary consensus enrollment.
Local pipeline discovery is intentionally independent of memory clearance;
finding or messaging an agent delegates no memory authority. SQLite and
PostgreSQL implement the same bounded candidate contract.

**Response** (HTTP 200):
`{"agents": [{...AgentEntry, "match_kind":"exact|substring"}], "total": N}`

---

### `POST /v1/agent/register`

Register agent on-chain. Idempotent — returns existing record if already registered.

**Request body** (`agent_handler.go:20-26`):

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | yes | Display name |
| `role` | string | no | Compatibility registration hint. On app-v23/app-v25 a self-registration is an ordinary pending Member; this field cannot self-grant `manager` or `admin`. CEREBRUM Root approves the real local role/profile/home-domain bundle atomically. |
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

An app-v23/app-v25 client should immediately call signed `GET /v1/agent/me`.
That caller-only response reports whether the key is `pending_review`, its
capability restrictions, and the exact operator action still required. It is
the supported self-diagnostic surface; `GET /v1/agents` is a signed,
active-only visible roster and must not be used to infer a pending caller's
standing.

**Response (existing, HTTP 200):** Same shape with `"status": "already_registered"`. `on_chain_height` is populated on both paths since v6.6.0.

Under app-v23, self-registration never grants the requested privileged role.
A new third-party key becomes a restricted, pending `member` with no active
local enrollment or owned home domain until an Admin commits the atomic policy
operation below. The immutable Root principal and every current or retired
Root credential generation are rejected as ordinary registration targets.

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
Each field is independently optional: omitting `name` preserves the current
canonical display name, and omitting `boot_bio` preserves the current canonical
bio. An SDK caller can therefore update either field without first fetching and
resending the other one.

**Request body:**

| Field | Type | Required |
|---|---|---|
| `name` | string | no |
| `boot_bio` | string | no |

**Response** (HTTP 200): `{"agent_id": "...", "name": "...", "status": "updated", "tx_hash": "..."}`

---

### `GET /v1/agent/me`

Signed, caller-only agent profile and standing, including the on-chain Proof-of-Experience
quorum-weight factors. Since v8.6.0 the response also exposes the lifetime
corroboration count and per-domain expertise; `accuracy`, `corr_count`, and
`domain_expertise` are read from the authoritative on-chain `vstats:` /
`vstats_domain:` records (not the off-chain mirror). After app-v23 activation,
an authenticated registered ordinary agent can use this route even while it is
`pending_review` or inactive. That narrow exception lets a client diagnose only
its own standing; it does not reopen the signed active-only `/v1/agents` roster.
CEREBRUM Root and unregistered keys have no ordinary agent profile.

**Response** (HTTP 200):

```json
{
  "agent_id": "<hex>",
  "display_name": "...",
  "domains": ["go-debugging", "sage-development"],
  "role": "member",
  "profile": "companion",
  "home_domain": "voice-interface",
  "enrollment_status": "active",
  "registration_status": "active",
  "approval_required": false,
  "clearance": 2,
  "capabilities": 15,
  "can_read": true,
  "can_write": true,
  "access_scope": "home_domain",
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
- `home_domain` — the exact app-v23 owned domain a write-capable agent may use
  when an MCP write omits `domain`; explicitly requested domains are never
  silently remapped.
- `registration_status` / `enrollment_status` — `active`, `pending_review`, or
  `inactive`, derived from the signed caller's consensus registration and local
  enrollment only. `approval_required` tells a client whether local CEREBRUM
  review is needed.
- `clearance` and `capabilities` — the caller's own consensus values. The raw
  capability mask is never returned for another agent through this route.
- `can_read` and `can_write` — the exact current authorization result for the
  caller's `home_domain`; `access_scope:"home_domain"` prevents clients from
  misreading these booleans as authority over every domain. Pending/inactive
  callers receive explicit `false` values without probing memory routes.

### `GET /v1/agent/me/domains`

Signed caller-only bounded policy projection. `domains` and
`readable_domains` contain up to 64 currently authorized exact recall targets;
`writable_domains` contains a bounded current-policy write sample; and
`owned_domains` is a bounded ownership sample. `truncated:true` means at least
one sample is incomplete. These arrays deliberately avoid a global roster or
memory scan and are suitable for choosing an exact scope, not proving complete
ownership.

### `GET /v1/agent/me/domains/owned`

Signed caller-only authoritative owned-domain pagination. `limit` defaults to
50 and is capped at 100; pass the exact returned `next_cursor` until
`has_more:false`. The app-v26 consensus owner index makes each page one bounded
local database read without enumerating agents or memories.

**Response:**

```json
{"domains":["agent-home","project-a"],"next_cursor":"project-a","has_more":true,"scope":"authoritative_current_owner"}
```

---

### `GET /v1/agent/{id}`

Get a registered agent by ID. Auth required.

**Response** (HTTP 200): `AgentEntry` object from off-chain store.

Under app-v23, generic list, lookup, search, and `/me` routes treat the
immutable Root principal and all Root credential generations as not-an-agent.
Root is available only through the dedicated CEREBRUM authority projection and
handover ceremony.

---

### App-v23 CEREBRUM access-control routes

These loopback CEREBRUM routes expose the current policy model. Mutations
require the current Root or an active same-machine Admin signing the exact
action; Admin actions also carry a fresh, single-use Root elevation
countersignature. Capability bits are returned only as diagnostics derived
from the selected named profile.

The Add Agent wizard is identity/connection enrollment only. It creates a
restricted pending Member and deliberately does not collect, stage, or persist
a requested role, clearance, or domain list that `AgentRegister` would discard.
After registration, the operator uses Access Controls to approve role, named
profile, clearance, and an owned compatible home domain atomically.

App-v23 also treats canonical memory publication as a CEREBRUM read boundary.
`GET /v1/dashboard/memory/list` (including text search), `/v1/dashboard/export`,
`/v1/dashboard/memory/graph`, `/v1/dashboard/memory/{id}/related`,
`/v1/dashboard/stats`, and `/v1/dashboard/memory/timeline` validate every
candidate against its exact canonical hash/status/domain/author/classification
projection before exposing content or aggregates. Exact/detail and portable
export routes fail closed with a sanitized HTTP `503` when any required record
cannot be verified. Broad collection routes use the record-local policy below.

v11.16.2 makes the degraded lane record-local. Broad list/search, graph,
timeline, stats, and dashboard-health reads omit an affected record when its
canonical envelope is absent, zero-hash and ineligible for legacy terminal
compatibility, deterministically mismatched against SQL, locally malformed, or
missing from the SQL projection. Backend/storage failures and incomplete audits
still fail the whole request. Local-operator list/search, graph, timeline, and
stats responses include:

```json
{
  "projection": {
    "complete": false,
    "partial": true,
    "verified_only": false,
    "state": "quarantined",
    "hidden_count": 3,
    "message": "3 historical memories could not be canonically verified and are hidden. Readable memories remain available; hidden records stay preserved for governed recovery or deprecation."
  }
}
```

`verified_only` is false when the returned set includes the narrow legacy
terminal-hashless compatibility class; it is true when every returned row has
an exact canonical content commitment. Dashboard health exposes a sanitized
`memory_projection` object with the generic partial warning and no hidden
count. Ordinary signed agents receive that same sanitized object from stats, so
they cannot infer the global quarantine count outside their RBAC scope. No
route includes a hidden identity, domain, author, status, or reason.
Exact/detail, related, tags, task-derived views, and export remain fail-closed,
and `/ready` returns HTTP `200` with `status:"degraded"` after a complete audit
has localized the bad records. `?strict=1` keeps that quarantine at HTTP `503`.
Readiness also remains `503` when the audit itself is unavailable or a core
dependency is down. A partial CEREBRUM view is never a complete backup.
Pre-app-v23 nodes retain their legacy projection behavior.

| Method + path | Purpose |
|---|---|
| `GET /v1/dashboard/network/access` | Read Root/broker readiness plus non-Root agents, enrollment/role revisions, named profiles, Access Groups, linked-reader readiness, and separate linked-message consent readiness. |
| `PUT /v1/dashboard/network/access/agents/{id}/policy` | Atomically approve or change a non-Root local agent's role, named profile, clearance, and compatible owned home domain. |
| `PUT /v1/dashboard/network/access/agents/{id}/name` | App-v26 H+1: current local Root/Admin changes only a governed non-Root agent's mutable display name. The handler copies the current boot bio into `AgentUpdate`; consensus rejects any operator attempt to alter that bio. `agent_id` and immutable `registered_name` never change. A no-op returns `status:"unchanged", committed:false`; a real change is reported only after commit or canonical reconciliation. |
| `PUT /v1/dashboard/network/access/groups/{groupID}` | Create or replace a consensus local Access Group using `name`, local `members` (canonicalized and sorted by the handler), app-v26 `member_authority` (`read`, `read_write`, or `read_write_modify`), and an `expected_revision` binding. See [`concepts/app-v26-access-groups.md`](concepts/app-v26-access-groups.md). |
| `DELETE /v1/dashboard/network/access/groups/{groupID}` | Delete an Access Group using `{"expected_revision": <current revision>}`. See [`concepts/app-v26-access-groups.md`](concepts/app-v26-access-groups.md). |
| `GET /v1/dashboard/network/access/linked-readers` | List exact node-local federated linked-reader relations. |
| `POST /v1/dashboard/network/access/linked-readers/eligibility` | Live-check one exact `remote_agent_id` on one active `remote_chain_id` before offering the manual-ID fallback; it is not a directory or presence query. |
| `POST /v1/dashboard/network/access/linked-readers` | Attach, remove, or rebind an exact remote agent as a read-only relation; never creates local membership. |
| `GET /v1/dashboard/network/access/linked-messages/candidates?remote_chain_id=...&local_agent_id=...` | Return bounded, host-signed exact remote-member offers for one local guest receiver; exposes IDs/group IDs only, never domains, names, or a peer roster. |
| `GET /v1/dashboard/network/access/linked-messages/consent?remote_chain_id=...&remote_agent_id=...&local_agent_id=...` | Read the receiver-local, default-off consent and CAS revision for one exact currently linked remote-to-local agent tuple. |
| `PUT /v1/dashboard/network/access/linked-messages/consent` | Set `accepting` for one exact tuple using `expected_revision`; never creates a read link, contact, group membership, domain grant, role, or write authority. |
| `POST /v1/dashboard/network/access/root/handover` | Dedicated current-Root-only credential handover with irreversible confirmation, exact phrase, and expected Root generation. Returns the replacement recovery archive once with `Cache-Control: no-store`. |
| `GET /v1/dashboard/memory/adoption-progress` | Root/operator aggregate App-v25 historical-recovery progress. It returns counts and state only—never the hidden records' content, domains, authors, or reasons. |
| `POST /v1/dashboard/memory/adoption-retry` | Current-Root-only request for a fresh scan of the exact unresolved App-v25 snapshot. Requires its `projection_revision` and `expected_count`; it never deletes rows or clears earlier dispositions. |
| `POST /v1/dashboard/memory/adoption-deprecate` | Current-Root-only retirement of the exact unresolved snapshot. Requires `projection_revision`, `expected_count`, and typed `DEPRECATE <count>` confirmation. Records remain preserved for audit and are skipped by future automatic repair. |

Once app-v26 is active, the legacy
`PATCH /v1/dashboard/network/agents/{id}` metadata route rejects `name` and
`boot_bio` with `governed_agent_metadata_required`. Display-label changes use
the governed Access Controls endpoint above; boot purpose, registered name,
and agent ID remain immutable. Local presentation-only fields such as avatar
and P2P address continue to use the legacy route. Pre-app-v26 metadata behavior
is unchanged.

The roles are `member`, `manager`, and `admin`. Roles define verbs; consensus
Access Groups define local scope; clearance caps readable classification; and
the `standard`, `companion`, or `read_only` profile supplies hard restrictions
(CEREBRUM displays the latter as “Read-only”).
Root is a separate singleton authority, not another `admin`. Handover preserves
the Root principal's domains, grants, groups, and readable history while new
memories record the replacement credential as their exact author. Prior
authorship and blocks are never rewritten.

An upgraded node may also return `legacy_restricted` for an existing agent.
This is an immutable migration review state, not a selectable fourth preset.
The policy route rejects a request whose target profile is
`legacy_restricted`; CEREBRUM instead shows one review action that replaces it
with a normal named profile. Non-Root legacy Admins appear as Members with
disposition `legacy_admin_review` until an explicit local Root-attested
promotion. Bare mask-30 self-registrations without an owned non-shared domain
or explicit level-1-or-higher grant remain inactive with `pending_review`.
Domainless deny-claim agents and exact masks are otherwise preserved as
described in
[`app-v23-access-control-design.md`](app-v23-access-control-design.md).

For an unchanged active migration baseline only, REST memory-submit preflight
also preserves the app-v22 ability to write a shared domain when capability bit
`2` is absent. This exception is Write-only, never Modify, and ends after any
explicit role/enrollment policy revision. Fresh app-v23 agents require the
normal shared-domain grant.

The three linked-message candidate/consent routes are local CEREBRUM human-management
routes, not agent data-plane APIs. Both the socket peer and Host must be
loopback; encrypted nodes additionally require the local unlocked session (or
an exact signed current local authority), while unencrypted nodes use the same
same-origin CEREBRUM boundary. Input is bounded to canonical agent IDs and a
valid 50-byte-or-shorter chain ID. Missing, stale, unrelated, inactive, Root,
and non-group tuples all return the same not-found problem
`linked_message_tuple_unavailable`. A missing consent projects as revision `0`,
`accepting:false`; an explicit block retains its monotonic non-zero revision.
Concurrent mutation returns `linked_message_consent_conflict` and requires a
reload before retry.

The candidates route is invoked only by an explicit one-host CEREBRUM action.
Changing the selected local receiver performs no peer request, and CEREBRUM
never fans that exact ID out across the connection list. Empty and failed
candidate checks use the same no-offer presentation.

The linked-reader eligibility route accepts
`{"remote_chain_id":"<active-chain>","remote_agent_id":"<canonical-lowercase-id>"}`
and returns
`{"ok":true,"eligible":true,"remote_chain_id":"...","remote_agent_id":"..."}`
only after the authenticated peer confirms that exact identity is an active
ordinary agent. Missing, stale, Root, Admin, inactive, unavailable, and
otherwise ineligible identities collapse to the same HTTP `409`
`linked_reader_agent_ineligible` result, so the route is not an identity oracle.
Invalid local request shapes return `400`; an inactive app-v23 node returns
`409`; a build without the live eligibility adapter returns `501`.

---

### Retired pre-app-v23 agent-policy route

The historical per-field agent-permission mutation is deliberately omitted
from the current OpenAPI and SDK surfaces. On governed nodes it returns HTTP
410 with problem code `app_v23_atomic_policy_required` and a machine-readable
replacement:

```json
{
  "replacement": {
    "method": "PUT",
    "path": "/v1/dashboard/network/access/agents/{id}/policy",
    "target_agent_id": "<id>"
  }
}
```

The only current mutation is the replacement shown above: it commits role,
profile, clearance, capabilities, and home-domain approval atomically through
loopback-only CEREBRUM. Current clients must not call the retired route or
construct the old `AgentSetPermission` payload.

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

**Historical CEREBRUM access matrix (v11.3–v11.14):** Saving the
per-agent Domain Access matrix issued real `AccessGrant`/`AccessRevoke`
transactions through this consensus path. App-v23 retires that matrix from the
live Access Controls page in v11.15+, so documentation and typed denials must
not direct an operator to a nonexistent level-2 editor. The low-level consensus
grant/revoke routes remain documented here for compatible clients; the shipped
v11.16 CEREBRUM actions are owned-home-domain policy and, where shared
management is intended, Root/Admin-approved Access Groups with an explicit
Read + write or Read + write + modify tier.

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
| `expected_owner_id` | string | app-v26: yes; earlier: omit | Hex(64) owner observed before proposing. The proposal and execution tx both bind it; consensus rejects if current ownership changed before execution. |

**Response** (HTTP 200):

```json
{"tx_hash": "<hex>", "purged_grants": 5}
```

`purged_grants` is parsed from the FinalizeBlock log. Previous owner's full grant chain-of-trust is wiped on transfer. The canonical new owner immediately receives owner-derived access; app-v26 does not require or emit a redundant self-grant, so a transfer does not depend on CEREBRUM holding the new owner's private key.

**Error behavior:** Unlike other endpoints, FinalizeBlock rejection messages are surfaced verbatim (not sanitized) so operators can diagnose `proposal not found`, `body mismatch`, `already consumed`, etc. (`domain_reassign_handler.go:162-195`)

**CEREBRUM orchestration (v11.3; app-v26 CAS):** The dashboard drives this whole agent-to-agent transfer from the Search page via `POST /v1/dashboard/network/reassign-domain-ownership`, commit-confirmed in strict order: `gov_propose(domain_reassign)` -> the sole validator's accept vote drives the proposal to `Executed` in-band -> this `DomainReassign` atomically flips the owner, records ownership history, purges unrelated grants, applies any shared marker, and consumes the proposal. The new owner's access follows directly from canonical ownership, so no target-key lookup or self-grant is required. At app-v26 the dashboard reads the canonical current owner, includes it as `expected_owner_id` in both the approved proposal payload and execution transaction, and consensus compares it immediately before transfer. A concurrent handover therefore fails instead of applying a stale operator confirmation. The optional trailing wire field is admitted only from H+1; activation height H retains the historical encoding. It requires a single-validator node; a multi-validator chain returns HTTP 409 because the other validators must vote on the proposal. This is off-consensus orchestration only - each underlying step is the same on-chain tx documented here, and memory authorship (`submitting_agent`) is never rewritten (`web/reassign_handler.go`; `internal/abci/app.go`).

---

## 5. Orgs / Departments / Federation

### `POST /v1/org/register`

Register an organization. Calling agent becomes admin. `org_id` is deterministic: `hex(SHA256(agentID + name)[:16])`.
After app-v22, both REST preflight and consensus require the caller to be a
global `role=admin`; an ordinary agent cannot create an organization to
self-award TOP SECRET membership (`api/rest/org_handler.go`;
`internal/abci/app.go`).

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

Add agent to org. Admin only on-chain. After app-v22 the signer must also be a
global `role=admin`, closing the organization-membership clearance-escalation
path for restricted companions.

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

Propose a bilateral federation agreement. At app-v22 the caller must be a
global admin and a member of the selected proposer organization.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `proposer_org_id` | string | no | Exact caller membership to represent; omitted uses the legacy primary org |
| `target_org_id` | string | yes | |
| `allowed_domains` | []string | no | `["*"]` means all; empty denies after app-v22 |
| `allowed_depts` | []string | no | Empty/`["*"]` means unrestricted; otherwise exact forward membership is required |
| `max_clearance` | int | no | 0-4 ceiling; omitted defaults to 2, explicit 0 remains PUBLIC |
| `expires_at` | int64 | no | Unix timestamp; 0=permanent |
| `requires_approval` | bool | no | Legacy/informational; target approval is always required for activation |

**Response** (HTTP 201): `{"status": "proposed", "tx_hash": "..."}`

---

### `POST /v1/federation/{fed_id}/approve`

Approve a pending federation. At app-v22 the caller must be a global admin and
an exact member of the stored target organization.

**Response** (HTTP 200): `{"status": "approved", "tx_hash": "..."}`

---

### `POST /v1/federation/{fed_id}/revoke`

Revoke an active federation. At app-v22 the caller must be a global admin and
an exact member of either stored federation organization.

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
| `GET /v1/dashboard/federation/connections/{chain_id}/permissions` | Returns `local_permissions`, `local_legacy`, authenticated read-only `remote_permissions`, `remote_known`, and `remote_legacy`. With `?live=0`, it returns durable local state without probing the peer and therefore reports `remote_known:false` (`web/federation_permissions.go:200-287`). |
| `PUT /v1/dashboard/federation/connections/{chain_id}/permissions` | Full replacement body: `{"permissions":[{"domain":"tii.work","read":true,"copy":false}]}`. Omitted domains are revoked; `[]` is explicit deny-all. Copy implies Read. Every enabled domain must already exist and be controlled by this operator. A `write:true` member is rejected with `400` because no consensus-bound federation ingress capability exists (`web/federation_permissions.go:223-258`, `279-305`). |
| `GET /v1/dashboard/federation/connections/{chain_id}/sync` | Returns `publish_domains`, `subscribe_domains`, `remote_publish_domains`, `remote_subscribe_domains`, and revision state. |
| `PUT /v1/dashboard/federation/connections/{chain_id}/sync` | v3 accepts `publish_domains` and/or `subscribe_domains`; an omitted lane is preserved and an explicit empty lane is cleared. The UI uses `{"subscribe_domains":[...]}` for the receiver's independent “Save here” decision (`web/federation_join.go:414-527`). |

Sharing groups are RBAC layered over existing trusted connections; membership
changes never create, revoke, or rewrite the pairwise connection beneath them.

| Route | Request / response |
|---|---|
| `GET /v1/dashboard/federation/groups` | Lists the local operator's active sharing groups, members, shared domains, delivery health, and `lifecycle_state`. A member removed by its owner and a fully dissolved owner group are omitted from the active list (`web/federation_join.go:1288-1515`). |
| `POST /v1/dashboard/federation/groups` | Creates an owner-controlled sharing group from `{"name":"..."}`. Members are added separately from already trusted SAGE connections (`web/federation_join.go:1240-1263`). |
| `POST /v1/dashboard/federation/groups/{group_id}/dissolve` | Owner-only, idempotent group deletion. It first persists a fail-closed `dissolving` barrier that stops group sharing and rejects new group mutations, then authors a signed terminal `member_remove` for each guest and retires the owner's active card. Partial failures remain visible/retryable as `dissolving`; offline guests can fetch their exact removal later. The endpoint does **not** modify `cross_fed`, `sync_control`, or direct `sync_domains`, so trusted connections and independent direct sharing remain intact (`internal/federation/sync_emit.go:202-292`, `internal/store/sync_group_tables.go:493-574`, `web/federation_join.go:1265-1287`). |

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
a trust-bound A↔B Write permission. The current protocol keeps Write fail-closed until consensus
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

Under app-v23, `limit` counts visible records. The built-in stores walk stable,
bounded pending-memory pages until that many live-authorized records are found
or the stream is exhausted; denied records and their count are not disclosed.

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

Ed25519 authentication is required. Once app-v23 is active, signature
possession alone is not enrollment: the signer must be an active, approved,
internally consistent local Member, Manager, or current-generation Admin.
Members and Managers retain signed REST access over a network listener; Admin
use remains localhost-only. Current and retired Root credentials, pending or
inactive agents, and arbitrary self-generated keys receive HTTP 403 before the
provider runs. Pre-app-v23 nodes retain the historical signed-key behavior.

`POST /v1/embed/personal` is a compatibility alias with the exact same handler
and authorization. It is not a separate unsigned browser endpoint.

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
It uses the same Ed25519 and app-v23 active-agent authorization as
`POST /v1/embed`, preventing unknown keys from probing provider, readiness, or
vault-derived routing state.

**Response** (HTTP 200):

```json
{"semantic": true, "provider": "ollama", "dimension": 768, "ready": true}
```

When vault (at-rest encryption) is active, `semantic` is forced `true` even if no embedder is configured — FTS5 cannot index encrypted content, so callers must not route to `/v1/memory/search`.

---

## 9. MCP Tokens / OAuth

### `POST /v1/mcp/tokens`

Issue a bearer token for MCP clients that cannot sign Ed25519 requests
(ChatGPT, Cursor, etc.). Ed25519 auth is required. Under app-v23 the issuer
must be the exact current committed CEREBRUM Root or an active
current-generation local Admin; the historical `agent.key` transport identity
does not retain issuance authority after Root handover.

The token plaintext is shown **once only**. Every app-v23 bearer mints and
synchronously self-registers a distinct restricted Member identity pending
CEREBRUM review; it never acts as Root/Admin or falls back to the issuer's key.
Its private key is AES-256-GCM sealed under an HKDF-SHA256 key derived from the
bearer plaintext and a per-row random salt, whether optional ledger encryption
is on or off. This keeps the credential stable through ledger enable/disable,
lock/unlock, and passphrase changes. SQLite stores the bearer SHA-256 only as
its lookup/fingerprint value; that digest cannot decrypt the signing key.
Authentication presents the plaintext transiently to unlock only its own row,
then downstream REST calls sign as the token's distinct agent. Existing
vault-sealed keyed rows remain compatible: while unlocked, their next valid
bearer authentication atomically rewraps them into the transition-stable
bearer envelope. Until that one-time migration, a locked vault fails them
closed rather than falling back to Root.

**Request body:**

| Field | Type | Required | Notes |
|---|---|---|---|
| `name` | string | no | Human label, e.g. `chatgpt-laptop` |
| `agent_id` | string | yes | The authorizing current Root/Admin's 64-char hex Ed25519 pubkey. The response `agent_id` is the newly minted distinct MCP agent. |

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

These support ChatGPT's MCP connector which requires a full OAuth 2.0 + PKCE
flow. Discovery, registration, public authorization entry, and token exchange
are network-facing and not subject to Ed25519 request auth. Consent itself is
not public: `/oauth/approve` is a localhost-only CEREBRUM route.

| Method | Path | Purpose |
|---|---|---|
| `GET` | `/.well-known/oauth-authorization-server` | RFC 8414 discovery document |
| `GET` | `/.well-known/oauth-protected-resource` | RFC 9728 protected resource metadata |
| `POST` | `/oauth/register` | RFC 7591 Dynamic Client Registration (10 reqs/IP/hour) |
| `GET` | `/oauth/authorize` | Validate the DCR/PKCE request and redirect to an absolute loopback approval URL carrying only an opaque signed five-minute handoff |
| `POST` | `/oauth/authorize` | Compatibility entry; remote requests still receive only the loopback handoff and cannot submit consent |
| `GET` | `/oauth/approve` | Localhost-only consent page; resolves live Root/Admin authority and requires an unlocked CEREBRUM session when encryption is enabled |
| `POST` | `/oauth/approve` | Localhost-only, CSRF-bound, single-use approval; re-resolves live authority, synchronously registers a distinct pending-review agent, then issues the authorization code |
| `POST` | `/oauth/token` | Exchange auth code for bearer (`grant_type=authorization_code` only) |

The `access_token` returned by `/oauth/token` is the same bearer accepted by `Authorization: Bearer <token>` on `/v1/mcp/sse` and `/v1/mcp/streamable`. Token lifetime is controlled by revocation (`DELETE /v1/mcp/tokens/{id}`), not expiry — `expires_in: 0` in the token response.

The authorization-code table persists neither raw credential. It indexes a
five-minute code by SHA-256 and seals the pending bearer with AES-256-GCM under
a domain-separated HKDF-SHA256 key derived from the unpersisted raw code and a
random per-row salt. The stored code digest, PKCE challenge, salt, and database
ciphertext cannot recover the bearer. Successful redemption atomically
consumes the code and wipes its delivery ciphertext and salt. Upgrade cannot
retroactively protect an already-copied legacy plaintext row, so app-v23
revokes its pending token, erases the row, and requires authorization to
restart.

The public handoff is an HMAC-authenticated opaque handle to process-local
state: it contains no OAuth parameters, agent roster, key, CEREBRUM session,
or bearer. It expires after five minutes and is consumed atomically by the
first valid approval POST. Expired, tampered, and replayed handles fail closed.
Pending handoffs are rate-limited per source and hard-capped at 2,048
process-wide.
The approval route checks the socket peer, `Host`, and forwarding metadata;
Cloudflare's generated ingress configuration does not expose it. Public-host
dashboard cookies are neither read nor required. With encryption off, the
same-origin loopback CEREBRUM browser approves as current Root; with encryption
on, it must also present a valid unlocked local session. Current Root/Admin
signatures work only on localhost, while retired Root and remote proxy
requests cannot reach consent.

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
resolution is limited to the finite authenticated contact projection disclosed
by active peers. A qualified handle never falls through to a similarly named
local target. Unknown, stale, paused, unavailable, non-accepting, ambiguous, or
temporarily incomplete resolution fails explicitly. An exact `agent@chain`
address may resolve while that one peer is genuinely offline from a previous
encrypted legacy-status snapshot, but only when its cache still matches the
complete active JOIN, CA, operator, policy-epoch, and policy-revision binding.
Targeted lookup results are live-only. Friendly handles and bare labels always
require a live, complete peer scan. TLS/certificate/authentication, HTTP,
identity, malformed response, and binding failures never use the cache
(`internal/federation/client.go:47-67,154-160`;
`internal/federation/pipe_targets.go:87-167,169-295`;
`internal/store/federated_pipe_contacts.go:49-195`).

Any legacy cached result is only a local queue-routing hint. Immediately before every
delivery attempt, the durable outbox performs a fresh authenticated live
resolution and requires the exact policy epoch, agreement, contact ID, contact
revision, agent, and chain to match. No intent or payload leaves this SAGE from
the cached snapshot alone (`internal/federation/pipe_outbox.go:171-220`).

A remote route is also bound to the **current local caller**: both resolve and
direct send recheck the caller's readable domain intersection against the
peer-authenticated contact domains. A local policy revoke therefore causes the
same non-enumerating `404 Unknown target` as an absent remote contact, even if
the address was resolved earlier (`api/rest/pipe_handler.go`).

### `GET /v1/federation/available?agent_name=...&peer_cursor=...`

Named ordinary-agent discovery merges two independently authorized,
metadata-only recipient projections. The legacy projection requires a current
caller-visible shared-memory domain. App-v26 additionally allows an exact
linked-reader messaging relation when the caller and remote recipient are
active ordinary non-Read-only agents, the linked guest and local group are
current, the agreement/policy generation is current, and the remote receiver
has enabled exact tuple consent. The linked result deliberately has no domain
basis and grants no memory authority.

The query is bounded to 512 UTF-8 bytes, at least two Unicode code points, 20
returned contacts per peer, and one bounded caller-authorized peer page. The response includes
`complete` and, while the public bounded scan horizon remains,
`next_peer_cursor`. A continuation is short lived, bound to the exact signed
caller/name/limit tuple, and reveals neither peer identifiers nor the number of
hidden agreements. Clients perform one page per call and must not auto-loop.
Before any remote request, the node removes agreements that have no local
caller-authorized domain or messaging edge; unrelated peer rows therefore do
not consume the outbound budget or affect pagination.
A linked result exposes only sanitized `display_name`, `registered_name`,
`provider`, and exact `agent_id@chain_id`; it never exposes relation bytes,
group IDs, consent revisions, roster totals/truncation, presence, delivery, or
read state. The internal `authorization_mode:"linked-v23"` discriminator says
only why the exact address was returned; its `available` and `accepting`
presence-shaped fields are always false and clients must not render a live
status from them. Both direct and relay transport use
`POST /fed/v1/pipe/linked/directory` behind peer mTLS/signature authentication.
Revocation returns the same empty projection as an absent name, and exact
`POST /v1/pipe/resolve` repeats live authorization before send
(`api/rest/federation_handler.go`;
`internal/federation/v23_linked_directory.go`).

### `POST /v1/federation/contacts/authorize`

Local-only reauthorization for chain/domain contact scopes from an already
caller-filtered federated-contact projection. This route does not discover
agents or make a peer request; it exists so the MCP's brief in-memory recipient
cache honors a local RBAC or agreement-state change immediately.

**Request:** `{"contacts":[{"remote_chain_id":"chain-a","domain":"example.scope"}]}`
(at most 512 unique chain/domain pairs; each domain is at most 256 bytes).

**Response** (HTTP 200): `{"allowed_contacts":[{"remote_chain_id":"chain-a","domain":"example.scope"}]}`
— only the input scopes whose agreement is active/unexpired and whose domain the
signed caller may currently read. An unregistered caller receives 403;
malformed or oversize input receives 400.

### Canonical local Messages service (v11.17)

The six `/v1/messages` operations are one same-node service over the existing
encrypted `pipeline_messages` rows. They do not create a second inbox. Every
route is inside the active-ordinary-agent boundary; Root is not a messaging
principal.

| Route | Contract |
|---|---|
| `POST /v1/messages` | Exact-local-agent send. Requires `to_agent`, `payload`, and a 1–256-byte caller-scoped `idempotency_key`; optional `intent` and strict `ttl_minutes` 1–1440 (default 60). Exact retry returns the original `message_id`; same key/different request is HTTP 409. |
| `POST /v1/messages/receive` | Requires a 1–256-byte `receive_token`; optional limit 1–20. Claims and persists one exact ordered batch. Same caller/token/limit replays that batch after a lost response; a different limit is HTTP 409. Replay metadata is retained for 48 hours and capped at 4096 tokens per agent: capacity returns HTTP 429, while a purged/incomplete exact batch returns HTTP 410 instead of claiming later work. |
| `POST /v1/messages/{message_id}/reply` | Exact fetched recipient only. Same result is idempotent; different second result is HTTP 409. Reply and local exact-read evidence commit atomically. |
| `PUT /v1/messages/{message_id}/read` | Fresh nonce-bound exact-recipient signature. The message must already have been returned to that caller by canonical receive. Same acknowledgement is idempotent. |
| `PUT /v1/messages/read-batch` | One fresh exact-recipient request acknowledges 1–20 already-fetched exact message IDs. Every item is authorized independently and returns `confirmed` or a generic per-item failure; one failure never rolls back independent successes. Exact replay is idempotent. |
| `GET /v1/messages/{message_id}/status` | Exact sender only, payload-free metadata projection. Returns independent transport/read/workflow state and never decrypts content/proofs. |

Every operation requires a fresh nonce-bound request signed by the exact active
ordinary agent, including send and sender status. Unauthorized and nonexistent
exact message IDs use the same generic 404 shape. Admin,
Root, node-operator, recipient, or a replacement identity cannot inspect a
sender's status. A local insert reports `transport_status:delivered` because
the addressed inbox row is durable on the same SQLite transaction boundary.
`read_status:confirmed` is only exact addressed-recipient evidence; it is not
presence, comprehension, or action.

Successful new local admission may invoke a best-effort HTTP MCP SSE wake-up
for already-connected sessions authenticated as the exact `to_agent`. The
additive JSON-RPC method is `notifications/sage_message` and its params contain
only `message_id`, `from_agent`, and `sent_at`. A missing/full stream never
fails the send, does not alter any message state, and is not evidence that a
recipient is online. Stdio and Streamable HTTP have no server-push contract.

The canonical same-node Messages routes remain separate from the
capability-gated federated receipt-v2 surface. A negotiated imported pipe adds
these payload-free routes:

| Route | Caller and meaning |
|---|---|
| `GET /v1/pipe/{pipe_id}/receipt/challenge/{kind}` | Exact imported-message recipient fetches the immutable body for `kind=claimed|read`. |
| `PUT /v1/pipe/{pipe_id}/receipt/{kind}` | That exact recipient submits the challenge unchanged under a fresh nonce-bound signature; returns local `receipt_status:queued`. |
| `POST /v1/pipe/receipts/challenge-batch` | Fetches 1–40 (`claimed`/`read`) immutable challenges for at most 20 imported messages in one local request. Per-item readiness/rejection is independent and no content or state transition is exposed. |
| `PUT /v1/pipe/receipts/batch` | Records 1–40 independently signed exact-event proofs in request order. A failed claim suppresses that message's read event; other messages retain independent partial success. This aggregates transport only—the peer-verifiable per-event proof is unchanged. |
| `GET /v1/pipe/{pipe_id}/receipt` | Exact original sender reads the payload-free independent evidence projection. |

An imported inbox row advertises `receipt_protocol_version:2` only when both
peers negotiated `federated-pipeline-receipts-v2`; absent/zero means the legacy
path. Federation keeps three independent facts:

- `transport_status:delivered` is the remote SAGE operator's authenticated
  acknowledgement that it durably admitted the message. It is not evidence
  that the recipient was online or saw it.
- `claim_status:confirmed` and `read_status:confirmed` require a fresh
  nonce-bound signature by the exact addressed recipient over that recipient's
  exact retained message action. Read means fetched and acknowledged, never
  comprehension, agreement, execution, or completion.
- terminal failure/expiry/revocation is an independent monotonic dimension. A
  message can terminate without being read; peer timestamps are evidence
  metadata and never order authority.

Only the exact original sender may query the payload-free federated projection.
Recipient, unrelated agent, Manager, Admin, Root, operator, and nonexistent IDs
share generic non-enumerating behavior. Peers negotiate
`federated-pipeline-receipts-v2`; v1 peers and historical rows without the
generation-bound v2 binding remain explicitly `unsupported`/`unconfirmed`.
Upgrade migration never invents delivery, claim, or read evidence.
Legacy `sage_pipe`, `sage_inbox`, and `sage_pipe_result` use the canonical local
service when available and fall back only on a definitive route-not-found from
an older node. Passive pipe history remains unchanged.

---

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
cannot cross the federation edge. It additionally repeats the caller/domain
intersection check described above, so a copied resolve response cannot bypass
local policy (`api/rest/pipe_handler.go`).

**Response** (HTTP 201):
`{"pipe_id":"pipe-<uuid>","status":"pending","expires_at":"...","destination_chain_id":"<remote chain or empty>"}`.
For a remote send, `pending` means durably queued for delivery; it does not claim
that the peer or agent has already received the work. Consequently, an exact
address resolved from a bounded legacy-status offline cache can be accepted
locally while the peer is down. Delivery waits for that peer to return and pass
the fresh live authorization preflight above.

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
Each item carries response-only
`authority:"request_only"`, `payload_authority:"request_only"`,
`trust:"agent_untrusted"`, and an explicit `security_notice`. Imported items
instead use `trust:"external_untrusted"`. These labels are derived by the REST
serializer and are not fields in the stored pipeline row, so a sender cannot
persist or supply its own authority. `intent` and `payload` remain requests for
consideration, never system, developer, or user instructions.
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

### `GET /v1/pipe/history/inbox` and `GET /v1/pipe/history/outbox`

Passive retained pipeline history for the authenticated participant. `inbox`
returns records addressed to the caller; `outbox` returns records the caller
sent. Both include `pending`, `claimed`, `completed`, and `expired` records
while the ordinary transient pipeline retention policy still preserves them.
They never claim, acknowledge, re-queue, or otherwise mutate a row.

**Query parameters:** `limit` (1–100, default 20)

**Response** (HTTP 200): `{"items":[...PipelineMessage],"count":N}`. A
claimed or completed record remains retrievable by the addressed recipient;
claiming changes work ownership and receipt state, not message visibility.
For provider-routed work, every matching provider agent may see a `pending`
record, but once it is claimed only the successful claimant sees that retained
provider-routed history. A sender's outbox contains only rows originated on
this SAGE, preventing a foreign imported sender ID from colliding into local
history.

Each row carries separate response-derived `payload_authority:"request_only"`
and, when present, `result_authority:"data_only"`; payloads and results remain
untrusted agent content. These local workflow states are not a federated
delivery/read receipt. Terminal records still purge under the existing
retention sweep; use durable memories or task records for information that must
outlive that window (`api/rest/pipe_handler.go`; `internal/store/sqlite.go`).

---

### Task assignment and agent notices

`GET /v1/dashboard/tasks?all=true&limit=N` is the local-human CEREBRUM Kanban feed. Ordinary signed Member/Manager agents receive `403` and use the scoped backlog instead; a current Root or eligible current Admin signature may use the CEREBRUM board only through localhost. A current local Admin that is also the exact assignee may separately use the ordinary task feed/status route, while Root never may. The board returns explicit `memory_type=task` records across `planned`, `in_progress`, `done`, and `dropped` on both SQLite and PostgreSQL; ordinary agent conversations/observations are not inferred as tasks. Historical task rows whose older writer did not persist `task_status` are returned with an empty status so CEREBRUM can ask the operator to classify each one—SAGE does not guess that unknown work is Planned or Done. New PostgreSQL inserts persist `TaskStatus`, matching SQLite. Each task also returns `task_status_updated_at`; CEREBRUM uses that lifecycle timestamp—not the task's original `created_at`—to keep Done/Dropped cards visible for seven days after their terminal transition. Manual Clear remains available before that window expires, while older cards can still be revealed with Show all (`web/handler.go:1919-1962`, `web/static/js/app.js:2328-2340`).

**Dashboard operator authority and localhost boundary (v11.15.0+):** CEREBRUM
is a same-machine human control plane, not a network administration service.
The HTTP socket peer and `Host` must both be loopback for the SPA,
authentication, recovery, and every session/headerless protected dashboard
request. Browser requests additionally face the existing same-origin,
Fetch-Metadata, and anti-rebinding checks. LAN, federated, cross-site, and
rebinding-host requests cannot load or manage CEREBRUM.

Vault encryption is optional and does not determine whether a local owner has
authority. With encryption off, the real same-origin loopback CEREBRUM SPA acts
as current Root and may perform the complete read and mutation surface,
including RBAC, agent lifecycle, federation management, governance, memory,
task, and settings operations. It does not need a synthetic password or a
copied genesis key. With encryption on, that same local SPA must also present a
valid unlocked vault session. A request signed by current Root or an eligible
Admin can exercise its permitted CEREBRUM actions only through localhost;
possession of an exported key on another machine does not make the dashboard
remotely manageable.

Signed agent operations that historically live below `/v1/dashboard`—scoped
task backlog/status, task notices, boot instructions, and applicable governance
reads—remain remotely reachable only for active ordinary Member or Manager
identities and retain their own exact authorization. Dedicated REST/MCP and
federation data-plane routes likewise remain network APIs for eligible
Member/Manager identities; this does not make a current Root or Admin credential
network-usable. The common signed-REST boundary accepts current Root/Admin
identity use only through localhost. Root is never an agent, task assignee, task
notice recipient, or pipeline inbox identity; every current and retired Root
credential is rejected from those agent-only surfaces. Pairing/claim redemption,
public health, validator, and MCP-configuration routes keep their separately
documented boundaries. Unsigned CLI-style localhost calls do not acquire human
Root authority merely by connecting locally. The boot-instructions GET accepts
a freshly signature-verified, consensus-enrolled Member/Manager that is still
active in the local registry for `sage_inception`; arbitrary unregistered keys,
pending/inactive agents, stale Admins, and Root credentials are denied
(`web/handler.go:isCEREBRUMOperatorRequest`,
`web/handler.go:isLoopbackCEREBRUMRequest`,
`web/handler.go:isLoopbackCEREBRUMBrowserRequest`,
`web/handler.go:bootInstructionsReadGate`).

`POST /v1/dashboard/network/claim` is the deliberately narrow exception to
dashboard authentication: the one-time claim token is the sole authority for
that exact redemption route, including when the node is encrypted and the CLI
connects remotely. New claims use 32 random bytes encoded as canonical unpadded
base64url (256 bits); the public route rejects the earlier six-character token
format, so an outstanding legacy claim must be regenerated by the operator.
Requests are capped at 4 KiB and rate-limited per socket source IP (not a
caller-controlled forwarding header). The limiter self-sweeps expired address
buckets and caps live tracked sources so unauthenticated source churn cannot
grow server memory without bound. SQLite and PostgreSQL consume a valid
unexpired token with one conditional `UPDATE ... RETURNING`, so concurrent
redemptions yield exactly one success. Consumption precedes bundle I/O:
missing/corrupt key material fails closed without making the token reusable.
The response is an explicit allowlist of install metadata plus the hex key
seed; it never serializes `claim_token`, `claim_expires_at`, or `bundle_path`
and is marked `Cache-Control: no-store`/`Pragma: no-cache`
(`web/network_handler.go`; `internal/store/agent_claim.go`).

`PUT /v1/dashboard/tasks/order` accepts `{"task_status":"planned","task_ids":["id-a","id-b"]}` from a local/authenticated CEREBRUM operator. It persists the supplied top-to-bottom order within that status column; omitted cards retain their relative order after the supplied cards. Moving a card to another status resets its board position so it arrives at the top of the destination column. CEREBRUM reads the backend maximum of 500 board cards and exposes accessible up/down controls on each card (`web/handler.go`, `internal/store/sqlite.go`, `internal/store/postgres.go`, `web/static/js/app.js`).

Terminal task transitions retain `assignee` as the last responsible agent for board attribution while setting the handoff gate that prevents terminal pickup. Done/Dropped cards render that identity as read-only “Completed by”/“Dropped by” metadata instead of an editable assignment selector. Reopening to Planned clears the historical assignee and requires a fresh operator handoff; direct terminal-to-In-Progress transitions are rejected. Upgrade repair backfills terminal attribution from authenticated `task_picked_up_by` evidence where older versions already cleared `assignee`. For older agent-authored cards with neither field, it uses `submitting_agent` only when that exact ID exists in the agent registry; it never guesses from the provider label (`internal/store/sqlite.go`, `internal/store/postgres.go`, `web/static/js/app.js`).

`PUT /v1/dashboard/tasks/{id}/assign` accepts `{"assignee":"<agent-id>"}`
(empty unassigns). This is a local CEREBRUM operator action; callers presenting
an ordinary agent identity cannot assign or reassign work. A browser caller
requires the enabled/unlocked encrypted-vault session described above; an exact
node-operator-signed request is the non-browser alternative. The target must be
an active registered agent and must be
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

**Response** (HTTP 200): Full `PipelineMessage` object plus response-only trust
metadata. `payload_authority:"request_only"` labels request content and
`result_authority:"data_only"` labels a present result. Status deliberately has
no object-wide `authority`: a completed message can contain both its original
request and its result, and one label would ambiguously bless the other field.
`security_notice` states the combined boundary. `trust` is
`agent_untrusted` locally and `external_untrusted` when either federation chain
provenance field is present.

---

### `GET /v1/pipe/results`

Completed pipeline messages sent by the authenticated agent.

**Query parameters:** `limit` (1–20, default 5)

**Response** (HTTP 200): `{"items": [...], "count": N}`. Empty results use
`items:[]`, never `items:null`. Each result is labeled
`authority:"data_only"` and `result_authority:"data_only"` for the singular
results endpoint. Because a row also contains its original request, that
content remains explicitly `payload_authority:"request_only"` and the combined
security notice explains both fields. Local and foreign rows use
`agent_untrusted` and `external_untrusted`, respectively.

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
No intent, payload, result, or proof bytes are exposed. Every update carries
response-only `authority:"notification_only"`,
`trust:"untrusted_metadata"`, and a `security_notice`. `last_error` may contain
peer-originated diagnostic text and is data, never an instruction or
authorization to take a recovery action.
`sage_turn` polls this route and returns actionable `pipe_delivery_updates`.

v11.17 exposes exact sender-queryable read state for same-node canonical
messages and, only after both peers negotiate receipt v2, for federated pipes
through `GET /v1/pipe/{pipe_id}/receipt`. Neither a legacy federated delivery
update, the local `/v1/pipe/{pipe_id}` workflow row, nor a clean inbox may be
presented as evidence that a remote recipient read a message.

---

## 11. Operational (No Auth)

### `GET /metrics`

Prometheus metrics for the `sage-gui` process. This endpoint is available only
through a direct loopback socket with a loopback HTTP `Host`; forwarded/proxied
requests and non-loopback REST clients receive HTTP 403. Binding `REST_ADDR` to
a LAN address exposes governed data-plane routes, not node metrics.

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
  "memory_projection": {
    "checked": true,
    "required": true,
    "ok": true,
    "state": "exact",
    "legacy_compatible": false,
    "quarantined": false
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
  locked, incomplete, or failed verification. The app-v23
  `memory_projection` independently verifies the full ordinary-memory
  Badger inventory against SQL; a missing, rolled-back, SQL-only, or tampered
  row is quarantined instead of being reported as an empty brain. A locked
  SQLite vault retries projection verification after unlock.
- `degraded` → **HTTP 200** by default: core is up but either a complete audit
  localized record-level memory projection quarantines, or a *semantic*
  embedder was probed and is unreachable so hybrid/semantic recall has dropped
  to keyword-only. The node still serves broad healthy records. Pass
  `?strict=1` to make either degraded condition a **503** for gates that require
  a complete projection and semantic recall.
- `ready` → **HTTP 200**: everything healthy. A hash (non-semantic) provider is
  `ready` — non-semantic is a capability, not a fault. An embedder not yet probed is
  also `ready`.

`memory_projection.state` is `exact` for a complete projection,
`legacy_compatible` for a complete projection containing only the explicitly
supported historical terminal hashless shape, and `canonical_subset` for a
verified state-sync receiver. The subset baseline is bound to that receiver's
chain, node key, validator key, and canonical height/AppHash; its exact omission
set is signed by the receiver node key before normal serving begins. The file
must remain in the receiver's non-group/world-writable data directory. It permits
only the exact historical IDs whose ordinary plaintext was absent at the sealed
snapshot. Later memories are still mandatory. A subset node cannot use the portable full-brain dashboard
export because labeling a partial local projection as a complete backup would
be unsafe. A receiver completed before v11.16 without this exact baseline
remains fail-closed until the operator explicitly repairs or repeats state sync;
startup never infers an allowlist from the receiver's current SQL contents.

`app_v25_maintenance` reports the current process's one-time historical
adoption/continuity verification. It is intentionally a readiness signal, not
a record-disclosure API. While its state is `waiting`, `migrating`,
`attesting`, or localized `recovery`, normal verified serving remains available
as `degraded` (HTTP 200 by default; `?strict=1` makes it 503). An incomplete
or unlocalized canonical projection remains `not_ready` regardless of this
field. See [`app-v25-upgrade-recovery.md`](app-v25-upgrade-recovery.md).

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

Before app-v23, a configured `nodeOperatorID` can bypass the historical
cross-agent submitter filter while domain and classification gates still apply.
Under app-v23 it is not a provenance shortcut: the effective policy principal
must still pass the central live enrollment, legacy-envelope,
domain/group/grant, and clearance decision for every returned record.

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
