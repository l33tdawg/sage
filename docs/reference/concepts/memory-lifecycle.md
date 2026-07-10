<!-- Reconciled through SAGE v11.5.0. -->

# Memory Lifecycle

Verified against code at SAGE v11.5.0.

## Overview

A SAGE memory begins as an agent-signed REST request and ends as a consensus-committed (or deprecated) record that appears in queries. Every state change between those endpoints happens through a CometBFT-ordered transaction written to BadgerDB; the PostgreSQL mirror is a write-behind projection updated atomically in `Commit`, never in `FinalizeBlock`.

---

## Status Model

Defined in `internal/memory/model.go:10-16`.

```
proposed
   ├── validated   (intermediate — used by internal app-validator path)
   │      ├── committed
   │      └── deprecated
   ├── committed   (quorum reached)
   │      ├── challenged
   │      │      ├── committed   (challenge rejected)
   │      │      └── deprecated
   │      └── deprecated
   └── deprecated  (quorum failed, or challenge upheld)
```

Valid transitions (`internal/memory/lifecycle.go:9-14`):

| From        | Allowed targets              |
|-------------|------------------------------|
| proposed    | validated, deprecated        |
| validated   | committed, deprecated        |
| committed   | challenged, deprecated       |
| challenged  | committed, deprecated        |

`deprecated` is terminal — no forward transition exists.

---

## State Machine Walkthrough

### 1. Submit (REST → BadgerDB pending)

`POST /v1/memory/submit` → `handleSubmitMemory` (`api/rest/memory_handler.go`) builds a `TxTypeMemorySubmit` transaction, embeds the agent proof, signs with the node's Ed25519 key, and broadcasts via CometBFT RPC. After app-v17 activation, a delegated request also embeds the exact canonical HTTP request so consensus can reconstruct and compare the action; before activation that optional wire tail is absent for byte-identical compatibility. Before broadcast the REST handler stores embedding + provider + knowledge triples in the process-local `SupplementaryCache` (keyed by memoryID, 60 s TTL).

### 2. CheckTx

CometBFT calls `CheckTx`. The ABCI app decodes the tx, verifies the Ed25519 node signature, checks the outer-signer nonce for replay protection (BadgerDB `nonce:<agentID>`), and rejects post-fork tx types that arrive pre-fork. Once app-v17 is active it also performs advisory delegated-proof binding and replay checks; `FinalizeBlock` repeats them deterministically and remains authoritative.

### 3. FinalizeBlock — processMemorySubmit

`FinalizeBlock` (`app.go:543-664`) is the deterministic execution path. Key constraint from the code comment: **"No time.Now(), no map iteration without sorting, no goroutines, no external I/O except BadgerDB reads."** Block time from `req.Time` is used for all timestamps.

`processMemorySubmit` (`app.go:826-989`) does:

1. Post-app-v17, action-binds any delegated agent proof to the exact signed REST request, checks it against block time, and atomically consumes its proof fingerprint. It then verifies the agent Ed25519 identity proof embedded in the tx (same-key node transactions are already payload-bound by the outer signature).
2. Domain-access check: if domain has a registered owner, calls `HasAccessMultiOrg`; if unowned and not a shared domain, auto-registers the domain with the submitting agent as owner (also issues a level-2 access grant to the owner, buffered for Commit).
3. Generates memoryID deterministically from `SHA256(contentHash + ":" + height + ":" + agentID)` if not provided by the caller.
4. Writes content hash + status `"proposed"` to BadgerDB at key `memory:<memoryID>`. Post-v8.4 it also writes `memdomain:<memoryID>` for domain-weighted PoE; app-v16 and higher independently guarantee the same domain write so their mandatory-domain rule cannot create a fresh memory that challenge/forget authorization cannot resolve, including an app-v17 skip-ahead activation where the v8.4 PoE gate remains dormant.
5. Pops supplementary data (embedding, provider, triples) from `SupplementaryCache`.
6. Buffers a `pendingWrite{writeType:"memory"}` with the full `MemoryRecord` for PostgreSQL.
7. Writes classification to BadgerDB at key `mem_class:<memoryID>` — verbatim from the tx field (see `clearance-classification.md`).
8. Buffers a `pendingWrite{writeType:"mem_classification"}`.

### 4. Commit — offchain flush

`Commit` (`app.go:2596-2665`) runs after `FinalizeBlock` for each block. It:

1. Flushes all `pendingWrites` to PostgreSQL **inside a single database transaction** (via `RunInTx`), with exponential-backoff retry for `SQLITE_BUSY`.
2. If the flush fails after max retries, **panics** — consensus has committed the writes on-chain; losing the offchain projection would create undetectable divergence.
3. Saves ABCI state (`height`, `appHash`) to BadgerDB only after PostgreSQL success. This ordering ensures CometBFT replays the block on restart if PostgreSQL write failed.

### 5. Voting → Committed or Deprecated

Each validator (in personal mode: the single node's own auto-voter; in multi-node mode: every validator node, each voting with its own consensus key) broadcasts a `TxTypeMemoryVote` tx. Note `POST /v1/memory/{id}/vote` signs with the **node's** validator key, not a per-agent identity, so all REST votes through one node collapse into that node's single validator slot — see [`voter-operations.md`](voter-operations.md) §8.

`processMemoryVote` (`app.go:991-1041`):
- Rejects votes from non-validators.
- Stores vote in BadgerDB at key `state:vote:<memoryID>:<validatorID>` (value: `"accept"` / `"reject"` / `"abstain"`).
- Increments on-chain validator vote stats (used for PoE scoring at epoch boundaries).
- Calls `checkAndApplyQuorum`.

`checkAndApplyQuorum` (`app.go:3602-3818`):
- Loads all validators, reads each vote from BadgerDB.
- Current chains use PoE-weighted votes after the app-v3 fork, with equal-weight replay retained only for pre-fork blocks.
- Calls `validator.CheckQuorum` (threshold: `>= 2/3` of total weight, `internal/validator/quorum.go:6`).
- **Quorum reached** → `SetMemoryHash(memoryID, nil, "committed")` in BadgerDB, buffers `status_update{StatusCommitted}` for PostgreSQL.
- **All validators voted, quorum not reached** (e.g. 2-2 tie) → immediately deprecates to prevent the validator ticker from flooding the chain with re-votes.

### 6. Challenge → Deprecated (or Reinstated)

A committed memory may be challenged via `TxTypeMemoryChallenge`. Chains authorize challenges through the app-v15 domain-access rules: the challenger must be the domain owner or ancestor owner, or hold a level-3 modify grant. Before app-v17 activates (and, after it activates, on any domain with a single modify-verb holder), an authorized challenge is decisive on inclusion.

In that one-strike path there is no separate voting round: the challenged memory transitions from `committed` → `deprecated` in the same `processMemoryChallenge` call that includes the tx. app-v17 (below) replaces this with a quorum-scaled two-phase path when the domain has two or more modify-verb holders.

**app-v16 - domainless-forget remediation.** The deprecation gate keys off the on-chain `memdomain:<memoryID>` record. Legacy memories committed before app-v8.4 never received one, so the gate rejected even the owner's challenge/forget with a generic "no recorded domain" denial (Code 91). app-v16 hardens the gate to split that into two distinct denials — both still DENY (Code 91, no new authorization): a legacy record predating app-v8.4 ("repair via an `OpMemoryDomainRepair` governance proposal") versus a genuinely unknown memory ("no memory record and no recorded domain") (`app.go:3862-3867`). The domained-but-unauthorized case is unchanged (Code 92). To unblock a legacy record, an **`OpMemoryDomainRepair`** governance proposal (`governance.ProposalOp = 6`, app-v16-gated) backfills the missing domain: it is created through the normal admin-gated propose path with a JSON payload of `[{"memory_id":"…","domain":"…"}]`, requires the default **2/3 supermajority** (`ThresholdFor` is fork-unaware, so a new op must not retroactively change quorum — replay parity), and on execution writes `memdomain:` only for a memory that already exists on-chain, has no domain yet, and whose target domain is already registered — idempotent, never overwriting, skipping unknown, already-domained, or unregistered-target IDs (`applyMemoryDomainRepair`, `app.go:7161`). After repair, a normal challenge/forget by an authorized agent deprecates as usual. app-v16 also requires every submit to carry a non-empty `domain_tag` **and persists that domain under app-v16+ rules even when app-v8.4 was never independently activated**, so the domainless state cannot recur on a skip-ahead chain.

**app-v17 - quorum-scaled two-phase challenge + reinstate.** Ships dormant behind the app-v17 fork and activates only via the governed upgrade ladder (`postAppV17Rules`, strict `>`); every pre-fork block and the activation block itself replay byte-identically. Once active, `processMemoryChallenge` (`app.go:3897-4007`) branches on the memory's domain:

- **Count the modify-verb holders.** At challenge execution the handler enumerates the distinct modify-verb holders on the memory's domain from committed state (the owner, ancestor owners, and unexpired level-3 grantees), in sorted order (`ModifyVerbHolders`, `app.go:3948`).
- **One or fewer holders → legacy one-strike.** The outcome is byte-identical to the pre-fork deprecate above, so personal nodes and single-owner domains see zero change.
- **Two or more holders → park as `challenged`.** A live `committed` memory is parked `challenged` with an AppHash-folded challenge record carrying the challenger, execution height, the measured quorum, and the prior hash/status snapshot (`SetChallengeRecord`, `app.go:3968-3980`; `status_update` stamps `DisputedHeight`/`DisputedQuorum`, `app.go:3993-3999`). The measured count is persisted and **never re-measured** at resolution. A `priorStatus == committed` guard is load-bearing: it confines the two-phase machine to committed memories, so a `deprecated` memory can never be resurrected through the park path and a `proposed` one can never skip the content-validation quorum.
- **Confirm → `deprecated`.** A *distinct* modify-verb holder re-issues the challenge to finalize the deprecation; the original challenger **cannot** self-confirm (Code 93, `app.go:3915-3917`), which would collapse two-phase back into one-strike.

**`TxTypeMemoryReinstate` (`processMemoryReinstate`)** drives the `challenged → committed` transition now represented in the `validTransitions` map. It is a new dual-gated tx (CheckTx + handler, returning Code 10 "unknown tx type" pre-fork so a non-activated chain replays byte-identically) taking a `challenged` memory back to `committed` and restoring the original content hash captured in the challenge record (the commit and deprecate paths nil that hash, so a reinstate without the record would leave a hash-less husk). Current modify-verb holders may reinstate. The original challenger may **always withdraw** using the AppHash-folded `ChallengerID`, even if their level-3 grant expires or is revoked while the dispute is open. Rejections: Code 94 not-challenged/double-resolve, Code 92 unauthorized. The operation is reachable through REST (`POST /v1/memory/{id}/reinstate`), MCP (`sage_reinstate`), and both Python SDK clients (`reinstate()`).

**Disputed-but-recallable (off-chain surface).** While parked `challenged`, a memory stays recallable across REST and MCP recall on both SQLite and Postgres, flagged `disputed` in the response with a query-time confidence haircut (the shared `0.8` multiplier, presentation-only, leaving the on-chain status and stored confidence untouched; see `rest-api.md` on `POST /v1/memory/query`). The same multiplier is applied during `min_confidence` admission, preserving the guarantee that every returned `confidence_score` clears the caller's floor. The SQLite mirror carries the challenge metadata, clears it on confirm/reinstate, and its boot-time `ResolveChallengedMemories` sweep is scoped to legacy pre-v17 rows only.

### 7. Corroborate (does not change status)

`TxTypeMemoryCorroborate` (`processMemoryCorroborate`, `app.go:1166-1188`) writes a `Corroboration` row to PostgreSQL. It does **not** change the memory's status or BadgerDB entry. Confidence is recalculated at query time using the corroboration count (see below).

---

## Node-Local vs. On-Chain Data

| Data                            | Storage           | Notes                                                                                         |
|---------------------------------|-------------------|-----------------------------------------------------------------------------------------------|
| Content hash + status           | BadgerDB (on-chain) | Key `memory:<id>`. Every node maintains a consistent copy via BFT replication.               |
| Classification level            | BadgerDB (on-chain) | Key `mem_class:<id>`. Written in `processMemorySubmit`, mirrored to PostgreSQL. |
| Validator votes                 | BadgerDB (on-chain) | Key `state:vote:<memoryID>:<validatorID>`. Deterministic quorum input.                        |
| Access grants / domain owners  | BadgerDB (on-chain) | Keys `grant:<domain>:<agentID>`, `domain:<name>`. Written via tx, not REST-only.             |
| Full content, embedding vector  | PostgreSQL (off-chain) | Written in `Commit`; node-local until replicated by PostgreSQL shared service.              |
| Corroborations                  | PostgreSQL (off-chain) | Written in `Commit`. Count read at query time for confidence computation.                    |
| **Tags**                        | **SQLite only (node-local)** | `SetTags`/`GetTags` are **no-ops on PostgresStore** (`store/postgres.go:1383-1395`). Tags exist only in personal (SQLite) mode deployments and are never on-chain. The `QueryOptions.Tags` field is explicitly noted: "any-match filter on user-defined tags (SQLite-only)" (`store/store.go:89`). |
| Embedding vector (supplementary) | Process-local SupplementaryCache → PostgreSQL | Staged in-process pre-broadcast; only the receiving node has it in cache. |
| Knowledge triples               | PostgreSQL (off-chain) | Staged via SupplementaryCache, flushed in Commit.                                            |

---

## Memory Types and Confidence Semantics

Defined in `internal/tx/types.go:74-79` (wire) and `internal/memory/model.go:22-26` (model):

| Type        | Wire byte | Intended use                              | Suggested initial confidence |
|-------------|-----------|-------------------------------------------|------------------------------|
| fact        | 1         | Verified truths, architecture decisions   | 0.95+                        |
| observation | 2         | Noticed patterns, preferences             | 0.80+                        |
| inference   | 3         | Hypotheses, drawn conclusions             | 0.60+                        |
| task        | 4         | Work items; sub-statuses: planned / in_progress / done / dropped | caller-defined |

These are conventions carried in the tx and stored verbatim. The ABCI state machine does not enforce confidence ranges by type — the caller sets `ConfidenceScore` (float64 in [0,1]) and it is stored as-is.

---

## Confidence Decay Formula

Source: `internal/memory/confidence.go`.

```
conf(M, t) = conf₀ · exp(-λ_M · Δt_days) · (1 + 0.1 · ln(1 + corr_count))
```

Where:
- `conf₀` — initial confidence score at submission time
- `λ_M` — domain-specific decay rate (`GetDecayRate`, `confidence.go:20-25`)
- `Δt_days` — elapsed days since `CreatedAt`
- `corr_count` — number of committed corroborations on the memory

**Domain-specific decay rates** (`confidence.go:14-17`):

| Domain      | λ         | Half-life      |
|-------------|-----------|----------------|
| `crypto`    | 0.001     | ~693 days      |
| `vuln_intel`| 0.01      | ~69 days       |
| (default)   | 0.005     | ~139 days      |

Confidence is **computed at query time**, not stored. The PostgreSQL column holds `conf₀`. `handleQueryMemory` calls `memory.ComputeConfidenceForRecord` per record (`memory_handler.go:786`) — the task-aware variant, so open tasks are exempt from decay — and returns the decayed value.

**Task exception** (`confidence.go:29-33`): open tasks (`memory_type=task` with `task_status` in `{planned, in_progress}`) **never decay** — `ComputeConfidenceForRecord` short-circuits and returns `conf₀` unchanged.

---

## Corroboration Strengthening

Each corroboration adds to `corr_count`. The boost term `(1 + 0.1 · ln(1 + n))` is unbounded but logarithmic — the first few corroborations provide the most lift:

| Corroborations | Boost multiplier |
|----------------|-----------------|
| 0              | 1.000           |
| 1              | 1.069           |
| 5              | 1.179           |
| 10             | 1.240           |
| 20             | 1.310           |

Combined with decay: a memory with many corroborations decays more slowly in effective terms — the boost offsets the decay factor. Confidence is clamped to [0, 1].

---

## Deprecation

A memory reaches `deprecated` via these paths:

1. **Quorum failure**: all validators voted, `acceptWeight / totalWeight < 2/3` → deprecated in `checkAndApplyQuorum` (`app.go:3766`).
2. **Challenge (one-strike)**: a `TxTypeMemoryChallenge` is included in a block → immediately deprecated (`app.go:4011`). No secondary vote. This is the behavior before app-v17 activates, and after activation on any domain with a single modify-verb holder.
3. **Challenge confirmed (app-v17 two-phase)**: on a domain with two or more modify-verb holders the first authorized challenge parks the memory `challenged`; a second, *distinct* modify-verb holder's confirming challenge finalizes the deprecation (`app.go:3918-3943`). The original challenger cannot self-confirm.
4. **Explicit transition**: `ValidTransition(proposed → deprecated)` and `ValidTransition(validated → deprecated)` are also allowed for administrative paths, though no current public tx type drives them directly.

Deprecated memories remain in PostgreSQL for audit purposes and are queryable by ID but are excluded from default similarity search results (callers can override with `status_filter`).
