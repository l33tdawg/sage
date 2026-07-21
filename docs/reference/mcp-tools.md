Reconciled against internal/mcp for SAGE v11.11.3.

# SAGE MCP Tools Reference

SAGE exposes 25 MCP tools over JSON-RPC 2.0. Stdio tools sign REST calls with
the local Ed25519 identity; SSE and Streamable-HTTP use the MCP bearer-token/OAuth
flow. Only consensus-committed memories are returned to callers.

---

## Boot Sequence — Read This First

Agents get this wrong more than anything else. The three-step sequence is
non-negotiable:

```
1. sage_inception   ← very first action, every new conversation, before you say anything
2. sage_turn        ← every single turn: topic + observation (atomic recall + store)
3. sage_reflect     ← after any significant task: dos + don'ts
```

**Why it matters:**
- Skipping `sage_inception` means every memory from every previous session is
  invisible for the entire conversation.
- Skipping `sage_turn` means the session produces no episodic record — future
  you has nothing to recall.
- Skipping `sage_reflect` breaks the feedback loop. Paper 4 measured Spearman
  rho=0.716 improvement over time with memory vs rho=0.040 without it.

The server enforces turn discipline: it blocks non-SAGE tool calls after 7
non-SAGE calls without `sage_turn`, or after more than 5 minutes since the last
`sage_turn` once at least 2 non-SAGE calls have accumulated. Calling `sage_turn`
resets the guard (`server.go:327-349`).

---

## Memory Types and Confidence Thresholds

Verified from `tools.go:32` (the `type` parameter enum and description):

| Type          | Min Confidence | Use for |
|---------------|---------------|---------|
| `fact`        | 0.95+         | Verified durable knowledge: IPs, hostnames, architecture decisions, confirmed configs, credentials paths, infrastructure specs. Survives confidence decay; crosses provider boundaries. |
| `observation` | 0.80+         | Session-level context: what happened, what was discussed, ephemeral experience. |
| `inference`   | 0.60+         | Hypotheses, conclusions drawn, connections between facts. |
| `task`        | 0.90 (fixed)  | Actionable items. Does not decay while open. |

Confidence decay means low-confidence memories age out over time. Use `fact`
for anything that must survive across sessions.

---

## Tool Reference

### sage_inception

**Purpose:** Initialize the agent's persistent memory session. Must be called
before any other action in every new conversation.

**Source:** `tools.go:106-118` (definition), `tools.go:897-1135` (handler)

**Parameters:** None.

**Returns:**
- First call (fresh brain): `status: "inception_complete"`, seeds 5 foundational
  memories in the `self` and `meta` domains, auto-registers the agent on-chain,
  returns `message` with full boot instructions and boot safeguard commands to
  execute immediately.
- Subsequent calls (brain has memories): `status: "awakened"`, returns
  `instructions` (adapts to configured memory mode), `stats`, `agent_id`,
  `agent_name`, `registration` status. If vault is locked, returns
  `vault_locked: true` with instructions for the user.

**Memory modes returned in `instructions`:**
- `full` (default): call `sage_turn` every turn.
- `bookend`: call `sage_turn` only at session start/end to conserve tokens.
- `on-demand`: SAGE tools are passive; only call when the user explicitly asks.

**REST:** `GET /v1/dashboard/stats`, `POST /v1/agent/register`,
`GET /v1/dashboard/settings/boot-instructions`,
`GET /v1/dashboard/settings/memory-mode`, `POST /v1/embed`,
`POST /v1/memory/submit`

**When to call:** First action of every new conversation. No exceptions —
not even for greetings. The server also runs auto-inception silently on the
first non-inception tool call if the brain is empty (`server.go:239-248`).

---

### sage_red_pill

**Purpose:** Deprecated alias for `sage_inception`. Identical behavior, identical handler.

**Source:** `tools.go:119-128`

**Parameters:** None.

**Returns:** Same as `sage_inception`.

**When to call:** Interchangeable with `sage_inception`. Prefer `sage_inception`;
`sage_red_pill` is deprecated and retained only for backward compatibility.

---

### sage_turn

**Purpose:** Per-turn atomic memory cycle: recall committed memories relevant
to the current topic AND store an observation about what just happened. Single
most important operational tool.

**Source:** `tools.go:129-147` (definition), `tools.go:892` (`toolTurn` handler)

**Parameters:**

| Name          | Type   | Required | Description |
|---------------|--------|----------|-------------|
| `topic`       | string | yes      | What the current conversation is about. Used for contextual recall inside the exact `domain`. |
| `observation` | string | no       | What happened this turn — user request and key points of your response. Kept concise. Low-value observations (< 30 chars, noise patterns) are silently skipped. |
| `domain`      | string | no       | Exact knowledge boundary for both recall and storage. Create dynamically (e.g. `go-debugging`, `user-project-x`). Default: `general`. |

**Returns:**
- `recalled`: array of relevant committed memories from the exact requested
  domain. Cross-domain rows are dropped client-side as a fail-closed safeguard.
- `recalled_count`: number of recalled memories.
- `stored`: `true` if observation was stored, `false` if skipped (duplicate or
  low-value).
- `skip_reason`: populated when `stored` is false.
- `store_mode`: set to `no_vector` when the observation was committed but the
  node's selected embedder was unavailable, so the REST boundary queued it
  WITHOUT accepting a possibly stale client vector. In that case
  `semantic_degraded` is `true` and `degraded_reason` explains it — the memory is
  not semantically recallable until the node's automatic provider repair
  backfills the vector after recovery/unlock.
- `pipe_inbox`: pipeline items addressed to this agent (if any).
- `pipe_inbox_count`, `pipe_results`, `pipe_results_count`: pipeline data.
- `pipe_delivery_updates`, `pipe_delivery_update_count`: one-shot terminal
  feedback for federated sends/results that exhausted safe delivery. Each item
  is payload-free, marked `foreign:true` and `trust:"external_untrusted"`, and
  includes an actionable recovery message.
- `recall_error` / `store_error`: set if a phase failed.
- `recall_mode` (`semantic_only` | `hybrid` | `keyword_only`), `semantic_degraded`
  (bool), `degraded_reason`: signal when the recall silently fell back to
  keyword-only (embedder down or a non-semantic hash node) — same meaning as on
  `sage_recall`.
- Returns `vault_locked` error if the Synaptic Ledger is locked.
- On SAGE v11.8.4+, a permanent domain ACL rejection returns `Domain write access denied` with the level-2/CEREBRUM remedy immediately; the MCP client does not re-register, retry the write, or suggest `/mcp`. Generic denials from older servers retain the bounded compatibility recovery path.

**Recall path:** Uses hybrid BM25+vector (RRF) by default; falls back to FTS5
full-text search if `/v1/memory/hybrid` is unavailable; falls back to semantic
vector search if the vault-encrypted marker is detected. Controlled by
`SAGE_RECALL_HYBRID` env var (`tools.go:565-571`).

**REST:** `POST /v1/memory/query` (semantic), `POST /v1/memory/hybrid` (hybrid),
`POST /v1/memory/search` (FTS5), `POST /v1/embed`, `POST /v1/memory/submit`,
`GET /v1/pipe/inbox`, `GET /v1/pipe/results`

**When to call:** Every single turn, immediately after receiving the user's
message. Provide `observation` with what the user asked and what you responded.
Omitting `observation` still performs recall — useful for a pure-recall turn.

---

### sage_reflect

**Purpose:** End-of-task feedback loop. Store what went right (dos) and what
went wrong (don'ts) to improve future performance.

**Source:** `tools.go:194-210` (definition), `tools.go:1137-1199` (handler)

**Parameters:**

| Name           | Type   | Required | Description |
|----------------|--------|----------|-------------|
| `task_summary` | string | yes      | Brief description of the task. Stored as `observation` at confidence 0.85. |
| `dos`          | string | no       | What went right — approaches that worked. Stored as `fact` at confidence 0.90. |
| `donts`        | string | no       | What went wrong — mistakes, failed approaches, things to avoid. Stored as `observation` at confidence 0.90. |
| `domain`       | string | no       | Knowledge domain. Default: `general`. |

**Returns:**
- `status: "reflected"`
- `memories_stored`: count of new memories written.
- `skipped_duplicates`: count of near-duplicate memories that were not stored.
- Returns `vault_locked` error if the Synaptic Ledger is locked.

**Note:** Stored content is prefixed: `[Task Reflection] ...`, `[DO] ...`,
`[DON'T] ...` (`tools.go:1159,1170,1178`).

**REST:** `POST /v1/memory/submit` (via `storeMemory` helper)

**When to call:** After completing any significant task. Both `dos` and `donts`
are valuable — do not skip this because a task was routine.

---

### sage_remember

**Purpose:** Explicitly store a single memory with full control over type,
confidence, domain, and tags.

**Source:** `tools.go:24-39` (definition), `tools.go:315-449` (handler)

**Parameters:**

| Name         | Type     | Required | Description |
|--------------|----------|----------|-------------|
| `content`    | string   | yes      | Memory content to store. |
| `domain`     | string   | no       | Domain tag. Default: `general`. |
| `type`       | string   | no       | `fact`, `observation`, `inference`, or `task`. Default: `observation`. |
| `confidence` | number   | no       | Score 0–1. Default: 0.80. |
| `tags`       | string[] | no       | User-defined labels (e.g. `important`, `project-x`). Git branch is auto-appended. |

**Returns:**
- `memory_id`, `status`, `tx_hash`, `domain`, `type`, `provider`, `tags`.
- `status: "skipped"` if a similar memory already exists in the domain (>60%
  word overlap with an existing committed memory).
- `status: "rejected"` with `votes` array if pre-validators reject the content.
- Returns `vault_locked` error if the Synaptic Ledger is locked.
- Uses the same v11.8.4 typed domain-write denial as `sage_turn`: permanent ACL denials return the level-2/CEREBRUM remedy immediately, without re-registration, retries, or `/mcp` advice.

**REST:** `POST /v1/memory/pre-validate` (optional), `POST /v1/embed`,
`POST /v1/memory/submit`

**When to call:** When you have a specific piece of knowledge to persist that
`sage_turn`'s observation path wouldn't capture — e.g. a user explicitly says
"remember this", or you want to store a `fact` with high confidence and specific
tags. Use `type='fact'` for anything durable (IPs, architecture decisions,
verified configurations).

---

### sage_recall

**Purpose:** Semantic search over committed memories.

**Source:** `tools.go:40-54` (definition), `tools.go:495` (`toolRecall` handler)

**Parameters:**

| Name             | Type   | Required | Description |
|------------------|--------|----------|-------------|
| `query`          | string | yes      | Natural language search query. |
| `domain`         | string | no       | Filter by domain tag. Omit to search all domains. |
| `top_k`          | int    | no       | Number of results. Default: from user's dashboard settings (fallback: 5). |
| `min_confidence` | number | no       | Minimum confidence threshold 0–1. Default: from dashboard settings (fallback: 0). |

**Returns:**
- `memories`: array of `{memory_id, content, domain, confidence, type, status, created_at}`.
- `total_count`: total matching memories.
- `recall_mode`: which path served the request — `semantic_only` | `hybrid` |
  `keyword_only`.
- `semantic_degraded`: `true` when recall did NOT have meaningful semantic vectors —
  the embedder is down/unreachable or the node runs a non-semantic hash provider, so
  results are keyword-quality. When this is `true`, treat recall as lower-fidelity
  (fix the embedder / run the smart-memory setup).
- `degraded_reason`: present only when degraded — a short explanation.

**Search path:** Same hybrid/semantic/FTS5 fallback chain as `sage_turn` recall
phase. Committed memories are returned; an app-v17 two-phase-challenged memory
also remains recallable with `disputed: true`, a `[DISPUTED]` content prefix,
and the shared query-time confidence haircut. The
`recall_mode`/`semantic_degraded` fields surface silent keyword-only fallback so
a caller isn't misled into trusting a degraded recall.

**REST:** `POST /v1/memory/hybrid`, `POST /v1/memory/query`, `POST /v1/memory/search`

**When to call:** Use before destructive actions (`sage_recall 'critical lessons'`);
when you need to look up specific past knowledge mid-conversation; in `bookend`
mode as the primary in-session recall mechanism.

---

### sage_forget

**Purpose:** Deprecate (challenge) a memory that is no longer accurate or
relevant.

**Source:** `tools.go:55-67` (definition), `tools.go:653-672` (handler)

**Parameters:**

| Name        | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `memory_id` | string | yes      | The memory ID to deprecate. |
| `reason`    | string | no       | Reason for deprecation. Default: `"deprecated by user"`. |

**Returns:**
- `memory_id`, `status: "challenged"`, `reason`.

**Note:** This submits a challenge transaction on-chain; the memory status
moves to `challenged`, not immediately deleted. Consensus governs final removal.

**REST:** `POST /v1/memory/{memory_id}/challenge`

**When to call:** When a memory contains outdated or incorrect information —
e.g. an IP address changed, a decision was reversed, or a fact was disproven.

---

### sage_reinstate

**Purpose:** Withdraw or resolve an open app-v17 two-phase challenge and move
the memory from `challenged` back to `committed`.

**Parameters:**

| Name        | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `memory_id` | string | yes      | The challenged memory ID to reinstate. |
| `reason`    | string | no       | Optional audit note explaining the reinstatement. |

**Returns:**
- `memory_id`, `status: "committed"`, `reason`, `tx_hash`.

The chain must have activated app-v17. A current domain owner/ancestor-owner or
level-3 modify grantee may reinstate; the original challenger may always
withdraw their own challenge even if that grant later expired or was revoked.

**REST:** `POST /v1/memory/{memory_id}/reinstate`

**When to call:** When a challenge was mistaken, its evidence was resolved, or
the original challenger wants to withdraw it.

---

### sage_corroborate

**Purpose:** Independently corroborate a committed memory. The submitting agent
cannot corroborate its own memory.

**Source:** `tools.go:298-310` (definition), `tools.go:2010-2055` (handler)

**Parameters:**

| Name        | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `memory_id` | string | yes      | The memory ID to corroborate. |
| `evidence`  | string | no       | Optional supporting note or citation. |

**Returns:**
- `memory_id`, `status`, and the REST response from the corroboration endpoint.

**REST:** `POST /v1/memory/{memory_id}/corroborate`

**When to call:** When an independent source supports an existing memory and you
want that support captured without creating a duplicate memory.

---

### sage_link

**Purpose:** Create a typed, directional relationship between two memories.

**Source:** `tools.go:311-326` (definition), `tools.go:708-760` (handler)

**Parameters:**

| Name        | Type   | Required | Description |
|-------------|--------|----------|-------------|
| `source_id` | string | yes      | Source memory ID. |
| `target_id` | string | yes      | Target memory ID. |
| `link_type` | string | no       | Relationship type. Default: `related`. |

**Returns:**
- `status`, `source_id`, `target_id`, `link_type`.

**REST:** `POST /v1/memory/link`

**When to call:** When a task, fact, observation, or inference should be connected
to another memory for future traversal.

---

### sage_list

**Purpose:** Browse memories with filters. See what exists in a domain, with a
specific status, or tagged with a label.

**Source:** `tools.go:68-83` (definition), `tools.go:674-733` (handler)

**Parameters:**

| Name     | Type   | Required | Description |
|----------|--------|----------|-------------|
| `domain` | string | no       | Filter by domain tag. |
| `tag`    | string | no       | Filter by user-defined tag. |
| `status` | string | no       | Filter by status: `proposed`, `committed`, `deprecated`. |
| `limit`  | int    | no       | Max results. Default: 20. |
| `offset` | int    | no       | Pagination offset. Default: 0. |
| `sort`   | string | no       | `newest`, `oldest`, or `confidence`. Default: `newest`. |

**Returns:**
- `memories`: array of `{memory_id, content, domain, confidence, type, status, created_at}`.
- `total_count`: total matching memories.

**REST:** `GET /v1/memory/list`

**When to call:** Auditing memory contents in a domain; checking what was stored
recently; paginating through all memories for review.

---

### sage_timeline

**Purpose:** View memory activity over time, grouped into time buckets.

**Source:** `tools.go:84-96` (definition), `tools.go:735-775` (handler)

**Parameters:**

| Name     | Type   | Required | Description |
|----------|--------|----------|-------------|
| `from`   | string | no       | Start date (ISO 8601, e.g. `2024-01-01`). |
| `to`     | string | no       | End date (ISO 8601, e.g. `2024-12-31`). |
| `domain` | string | no       | Filter by domain tag. |

**Returns:**
- `buckets`: array of `{period, count}` — memory creation counts per time period.
- `total`: total memory count in range.

**REST:** `GET /v1/memory/timeline`

**When to call:** Understanding memory activity patterns; debugging why certain
periods have no memories; monitoring agent activity across time.

---

### sage_status

**Purpose:** Get memory store statistics: total memories, counts by domain and
status, last activity.

**Source:** `tools.go:97-105` (definition), `tools.go:777-783` (handler)

**Parameters:** None.

**Returns:** Raw stats object from the dashboard API — total memories, breakdown
by domain, breakdown by status, last activity timestamp.

**REST:** `GET /v1/dashboard/stats`

**When to call:** Quick health check; understanding how full the memory store is;
verifying memories were committed after storing.

---

### sage_task

**Purpose:** Create or update a task in the persistent backlog. Tasks use
`memory_type: task` and do not decay while open.

**Source:** `tools.go:161-178` (definition), `tools.go:1475-1589` (prefix helper and handler)

**Parameters:**

| Name        | Type     | Required | Description |
|-------------|----------|----------|-------------|
| `content`   | string   | no*      | Task description. Required when creating. Stored with exactly one `[TASK] ` prefix, including when the input is already marked. |
| `domain`    | string   | no       | Domain tag. Default: `general`. |
| `memory_id` | string   | no*      | Existing task memory ID. Required when updating. |
| `status`    | string   | no       | `planned`, `in_progress`, `done`, `dropped`. Default: `planned`. |
| `link_to`   | string[] | no       | Memory IDs to link this task to via `related` link type. |

*Provide either `content` (create) or `memory_id` (update). Providing neither
returns an error.

**Returns:**
- Create: `{memory_id, task_status, domain, assignee, action: "created", linked, message}`.
- Update: `{memory_id, status, action: "updated", linked, message}`.

**REST:** `POST /v1/memory/submit` (create), `PUT /v1/dashboard/tasks/{id}/status`
(update), `POST /v1/memory/link` (linking)

**When to call:** Tracking planned work, feature ideas, or bug reports that must
survive session boundaries. Tasks don't decay, so anything with a future action
should be a task, not an observation.

Tasks created by a signed agent enter consensus as `planned` and are assigned to
that creating agent ID in the same local off-chain insert as the task record. If
`sage_task` was called with `status: "in_progress"`, it then performs a local
exact-owner start transition. Human-created/unassigned tasks remain `planned`
until CEREBRUM assigns them, so every `in_progress` task has an owner.

---

### sage_backlog

**Purpose:** View open (planned and in-progress) tasks explicitly assigned to
the signed agent ID. The task author's provider does not confer ownership.
Unassigned tasks remain visible only to the local CEREBRUM operator for triage.

**Source:** `tools.go:180-190` (definition), `tools.go:1508-1558` (handler)

**Parameters:**

| Name     | Type   | Required | Description |
|----------|--------|----------|-------------|
| `domain` | string | no       | Filter by domain. Omit for all domains. |

**Returns:**
- `tasks_by_domain`: map of domain → array of `{memory_id, content, task_status, confidence, created_at, assignee, assigned_to_you, task_picked_up_by, task_picked_up_at}`. Every row has `assignee` equal to the signed agent ID and `assigned_to_you: true`.
- `total_open`: total open task count.
- `message`: human-readable summary.

**REST:** `GET /v1/dashboard/tasks`

**When to call:** Session start to resume work the operator assigned to this
agent; reviewing that agent's priorities across projects.

---

### sage_register

**Purpose:** Register this agent on the SAGE chain with an on-chain identity.
Idempotent — returns existing record if already registered.

**Source:** `tools.go:179-193` (definition), `tools.go:1335-1365` (handler)

**Parameters:**

| Name       | Type   | Required | Description |
|------------|--------|----------|-------------|
| `name`     | string | yes      | Agent display name. |
| `boot_bio` | string | no       | Short agent bio/description. |

**Returns:**
- `agent_id`, `name`, `registered_name`, `status` (`"registered"` or
  `"already_registered"`), `on_chain_height`.

**REST:** `POST /v1/agent/register`

**When to call:** Rarely — `sage_inception` calls this automatically. Only call
manually if you need to set a specific name/bio, or if the auto-registration
failed and RBAC domain access is broken.

---

### sage_rename

**Purpose:** Rename this agent - set the **mutable** display name (and optionally
the bio) that shows in the CEREBRUM dashboard and to other agents on the network.
Use it to replace the default provider/project name (e.g. `claude-code/sage`) with
a meaningful, human-readable identity. Self-only: an agent can only rename itself.
Your permanent registration name and your `agent_id` never change.

**Source:** `tools.go:194-209` (definition), `tools.go:1521-1569` (`toolRename` handler)

**Parameters:**

| Name       | Type   | Required | Description |
|------------|--------|----------|-------------|
| `name`     | string | yes      | New display name (what shows up in CEREBRUM and to other agents). |
| `boot_bio` | string | no       | Short bio/description. **Omit to keep the current bio; provide to replace it.** |

**Bio-preservation (fails closed):** The underlying `AgentUpdate` tx overwrites
`boot_bio` unconditionally, so a name-only rename would otherwise wipe an existing
bio. When `boot_bio` is omitted, the handler reads the current bio (via
`GET /v1/agent/{agent_id}`) and re-submits it unchanged. If it cannot resolve its
own agent ID or read the current bio, it **aborts** rather than silently committing
an empty bio to consensus (`tools.go:1527-1546`). When `boot_bio` is passed, it
replaces the bio.

**Returns:**
- `agent_id`, `name`, `status`, `tx_hash`, and a human-readable `message`.

**REST:** `PUT /v1/agent/update` (→ `TxTypeAgentUpdate`); plus
`GET /v1/agent/{agent_id}` when preserving the existing bio.

**When to call:** Rarely - when you want a meaningful on-network identity instead
of the auto-assigned provider/project name. Unlike `sage_register` (which sets the
immutable registration name at first connect), `sage_rename` only changes the
mutable display name/bio and can be called again at any time.

---

### sage_pipe

**Purpose:** Send work to another agent through the existing SAGE pipeline,
locally or across an approved federation connection. The target sees it in the
same inbox on their next `sage_turn` or `sage_inbox` call.

**Source:** `tools.go:211-227` (definition), `tools.go:1663-1717` (handler)

**Parameters:**

| Name          | Type   | Required | Description |
|---------------|--------|----------|-------------|
| `to`          | string | yes      | Local provider/name/agent ID, visible `#node/agent-prefix` handle, or exact `agent@chain` address. |
| `payload`     | string | yes      | The work content to send. |
| `intent`      | string | no       | What you want done: `research`, `summarize`, `analyze`, `review`, etc. |
| `ttl_minutes` | int    | no       | Time-to-live in minutes. Default: 60. Max: 1440 (24h). |

Before every send, MCP calls the read-only `/v1/pipe/resolve` endpoint. For a
federated contact it signs the exact returned source chain, agent, and
destination chain; the
friendly alias is never the authorization anchor. A qualified remote target
that is unknown, stale, ambiguous, paused, unavailable, or not accepting fails
without falling through to local name resolution (`tools.go:2108-2167`;
`api/rest/pipe_handler.go:47-213`).

An exact `agent@chain` address can still be resolved and durably queued while
that one peer is genuinely offline if SAGE has a previous authenticated,
encrypted contact snapshot bound to the unchanged JOIN/CA/operator/policy
generation. Friendly handles and bare names remain live-only. The cached
snapshot is not transmit authority: the outbox performs a fresh authenticated
contact and policy match before payload bytes can leave the node
(`internal/federation/client.go:47-67`;
`internal/federation/pipe_targets.go:87-295`;
`internal/federation/pipe_outbox.go:171-220`).

**Returns:**
- `pipe_id`, `status`, `expires_at`, `destination_chain_id`, and `message`.
- Local acceptance says **Sent**. Federated acceptance says **Queued** because
  durable local enqueue is not a delivery receipt (`tools.go:2169-2178`).
- If the remote node is offline, use its exact `agent@chain` address. A friendly
  handle deliberately cannot resolve from cached display metadata.

**Note:** A purely local exchange keeps the existing completion summary journal.
A federated payload/result is vault-backed transient input and is never
auto-journaled as memory.

`payload` is capped at 256 KiB and `intent` at 8 KiB. Each verified sender may
hold at most 256 pending/claimed pipes and the node at most 10000; a full quota
returns HTTP 429 with `Retry-After`. Pending/claimed rows force-expire after 48h
and terminal rows purge after 24h.

**REST:** `POST /v1/pipe/send`

**When to call:** Delegating subtasks to specialized agents (e.g. send a research
question to Perplexity, send a code review to another Claude instance). The
result arrives via `pipe_results` in the next `sage_turn` response or via
`sage_inbox`.

---

### sage_inbox

**Purpose:** Check the unified agent inbox for pipeline work and one-way task
assignment notices. Pipeline items are atomically claimed and require
`sage_pipe_result`. Task notices are acknowledged when read, carry
`requires_result: false`, and direct the agent to verify current ownership in
`sage_backlog` before acting.

**Source:** `tools.go:257-268` (definition), `tools.go:2048-2166` (handler)

**Parameters:**

| Name    | Type | Required | Description |
|---------|------|----------|-------------|
| `limit` | int  | no       | Max combined items to return across both inbox sources. Default: 5. Max: 20. |

**Returns:**
- `items`: mixed array. Local pipeline work contains `{pipe_id, from, intent, payload, created_at, requires_result:true}`. Foreign work uses the same shape and adds `foreign:true`, `source_chain`, `source_pipe_id`, exact `sender_agent`, `from_network`, and `trust:"external_untrusted"`; its `from` value is the exact `agent@chain` address. Assignment notices contain `{notification_id, kind, task_id, assignment_version, domain, title, created_at, requires_result:false}` (`tools.go:2370-2446`).
- `count`: combined number of returned items, never greater than `limit`.
- `pipeline_count` / `task_assignment_count`: source-specific counts.
- `task_inbox_error`: present only when pipeline work was already claimed successfully but assignment notices could not be checked; returned pipeline work must still be processed.
- `message`: human-readable summary.

**REST:** `GET /v1/pipe/inbox`, then the remaining capacity from `GET /v1/dashboard/task-notifications`.

**When to call:** When you need to check explicitly for pending work from other
agents. `sage_turn` also checks the inbox automatically on every call
(`tools.go:2199-2278`), so explicit `sage_inbox` calls are only needed between
turns or when you need more than 5 items.

---

### sage_pipe_result

**Purpose:** Return results for a claimed pipeline work item. Local results keep
their existing summary journal. A foreign result is signed by the receiving
agent, durably queued over the original return route, and not journaled.

**Source:** `tools.go:241-254` (definition), `tools.go:1772-1799` (handler)

**Parameters:**

| Name      | Type   | Required | Description |
|-----------|--------|----------|-------------|
| `pipe_id` | string | yes      | The pipeline message ID to reply to (from `sage_inbox`). |
| `result`  | string | yes      | Your result/response. |

**Returns:**
- `status`, `journal_id`, `journaled`, and `message`.

**Note:** MCP first reads the pipe status. For foreign work it automatically
copies the stable `source_pipe_id` and exact local reply-source chain into the
signed completion request, keeping
the public tool call unchanged (`tools.go:2288-2333`). A local journal summarizes
the exchange; foreign completion returns `journaled:false`. `result` is capped
at 256 KiB. For foreign work, `message` says the result was **queued for
delivery**, not delivered; if retry later becomes terminal, the completing
agent receives a `pipe_delivery_updates` notice on `sage_turn`.

**REST:** `PUT /v1/pipe/{pipe_id}/result`

**When to call:** After processing a work item from `sage_inbox`. Always call
this to close the pipeline loop; the requesting agent won't see a result
otherwise.

---

### sage_gov_propose

**Purpose:** Submit a governance proposal, including validator operations,
Synchronization Group decisions, and app-v20 canonical scope actions. Requires
the target node's configured governance-operator identity. The node's live
validator key remains the on-chain proposal actor. After app-v20, the MCP
bridge first makes a signed `GET /v1/governance/context`, adds the returned
validator ID and chain domain, and consensus verifies every exact
operator-signed proposal as global-admin authorization. The operator must be a
registered global admin before it can propose.

**Source:** `tools.go:287-328` (definition), `tools.go:2377-2444` (handler)

**Parameters:**

| Name            | Type   | Required | Description |
|-----------------|--------|----------|-------------|
| `operation`     | string | yes      | `add_validator`, `remove_validator`, `update_power`, `sync_group_action`, or `scope_action`. |
| `target_id`     | string | conditional | Validator ID for validator ops; optional for `scope_action` when `scope.scope_id` is present. |
| `reason`        | string | yes      | Human-readable justification. |
| `target_pubkey` | string | no       | Hex-encoded Ed25519 public key. Required for `add_validator`. |
| `target_power`  | int    | no       | Voting power. Required for `add_validator` and `update_power`. |
| `payload`       | string | no       | Legacy base64 operation payload; mutually exclusive with `scope`. |
| `scope`         | object | no       | Preferred guided `scope_action` template: scope ID/revision/state/controller, exact domains, and weighted members. The node canonicalizes it into `ScopeRecordV1`. |

For revision 1, `joined_revision` may be omitted and each omitted `active`
defaults to true. Later revisions must preserve each existing member's exact
historical `joined_revision`. The node rejects duplicate domains/members,
invalid lifecycle states, inactive controllers, zero weights, scope IDs that
contain `/`, ambiguous `payload` + `scope`, and target/scope ID mismatches.

**Returns:**
- `proposal_id`, `tx_hash`, `status`, `operation`, `target_id`, `reason`.

`proposal_id` is the deterministic governance-engine ID used for later
vote/cancel calls and is distinct from the CometBFT `tx_hash`.

The context route is a compatibility probe: pre-v20 `404` or an inactive
context keeps the legacy body; other failures fail closed. A `409` mutation
response means the validator/domain binding changed, so call the tool again to
fetch and sign fresh context.

**REST:** `POST /v1/governance/propose`

**When to call:** Admin/operator use only. When the validator set needs to
change — adding a new agent as validator, removing a compromised one, or
rebalancing voting power.

---

### sage_gov_vote

**Purpose:** Authorize the target node's local validator to vote on an active
governance proposal. Only that validator receives voting power; the MCP/HTTP
operator identity cannot borrow another reachable validator. After app-v20,
the tool automatically attaches fresh validator/chain context. This node-local
operator need not be the global admin that authorized proposal creation.

**Source:** `tools.go:274-286` (definition), `tools.go:1910-1938` (handler)

**Parameters:**

| Name          | Type   | Required | Description |
|---------------|--------|----------|-------------|
| `proposal_id` | string | yes      | ID of the proposal to vote on. |
| `decision`    | string | yes      | `accept`, `reject`, or `abstain`. |

**Returns:**
- `tx_hash`, `status`, `proposal_id`, `decision`.

**REST:** `POST /v1/governance/vote`

**When to call:** Through the validator node whose configured operator key you
hold, when there is an active proposal to vote on. Check `sage_gov_status`
first to get the current `proposal_id`.

---

### sage_gov_status

**Purpose:** Check the status of governance proposals. Returns the active
proposal (if any) with vote tally and quorum progress.

**Source:** `tools.go:287-297` (definition), `tools.go:1941-1974` (handler)

**Parameters:**

| Name          | Type   | Required | Description |
|---------------|--------|----------|-------------|
| `proposal_id` | string | no       | Specific proposal ID. Omit to get the current active (voting) proposal. |

**Returns:**
- With `proposal_id`: full proposal detail object.
- Without: `{status: "active", proposal: {...}}` for the active proposal, or
  `{status: "no_active_proposal", message: "..."}` if none.

**REST:** `GET /v1/dashboard/governance/proposals/{id}` (specific),
`GET /v1/dashboard/governance/proposals?status=voting` (active)

**When to call:** Before voting (to get `proposal_id` and understand the
proposal); to monitor quorum progress; to verify a proposal was accepted or
rejected.

---

### sage_scope_list

**Purpose:** List canonical app-v20 quorum scopes with exact selected domains,
pinned integer weights, lifecycle state, and immutable current-revision hash.
Node-operator/admin only.

**Parameters:** none.

**REST:** `GET /v1/scopes`

---

### sage_scope_get

**Purpose:** Read one canonical app-v20 quorum scope. Node-operator/admin only.

| Name       | Type   | Required | Description |
|------------|--------|----------|-------------|
| `scope_id` | string | yes      | Exact canonical scope ID. |

**REST:** `GET /v1/scopes/{scope_id}`

---

## Discrepancies

### Boot sequence vs tool list

The CLAUDE.md and MCP server instructions both reference `sage_red_pill` as an
alias for `sage_inception`. Both are registered and both share the same handler
(`tools.go:127`). No discrepancy — both exist.

The boot sequence documented in CLAUDE.md (`sage_inception → sage_turn →
sage_reflect`) exactly matches the tools registered in `registerTools()`. No
tools are missing from either side.

### Tools in instructions not in tools.go

None. All tools mentioned in CLAUDE.md, MEMORY.md, and the MCP server
`initialize` instructions exist in `registerTools()`.

### Tools in tools.go not in documented boot sequence

`sage_corroborate`, `sage_link`, and `sage_reinstate` are core memory operations,
but not part of the boot sequence. They are used only when a caller needs to
strengthen/connect memories or resolve an open challenge.

`sage_gov_propose`, `sage_gov_vote`, `sage_gov_status`, `sage_scope_list`, and
`sage_scope_get` — governance/scope tools — are not part of the boot sequence.
This is correct: they are operator/admin/validator operations, not agent memory
operations.

`sage_pipe`, `sage_inbox`, `sage_pipe_result` — pipeline tools — are also not
part of the boot sequence. Also correct: pipeline is checked automatically
inside `sage_turn` (`tools.go:888-894`), so agents get pipeline data without
needing to call these explicitly.

`sage_register` — called automatically inside `sage_inception` (`tools.go:909-
939`). Agents never need to call it manually.

`sage_rename` - an on-demand identity tool, not part of the boot sequence. It
changes only the mutable display name/bio (self-only `AgentUpdate`); the immutable
registration name from `sage_register` is untouched.

---

## Summary

**25 tools documented:**

| Category     | Tools |
|--------------|-------|
| Boot / lifecycle | `sage_inception`, `sage_red_pill`, `sage_turn`, `sage_reflect` |
| Core memory  | `sage_remember`, `sage_recall`, `sage_forget`, `sage_reinstate`, `sage_corroborate`, `sage_link` |
| Browse       | `sage_list`, `sage_timeline`, `sage_status` |
| Tasks        | `sage_task`, `sage_backlog` |
| Identity     | `sage_register`, `sage_rename` |
| Pipeline     | `sage_pipe`, `sage_inbox`, `sage_pipe_result` |
| Governance   | `sage_gov_propose`, `sage_gov_vote`, `sage_gov_status`, `sage_scope_list`, `sage_scope_get` |
