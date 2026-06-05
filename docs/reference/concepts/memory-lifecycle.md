<!-- Verified against code at SAGE v8.1.1 (commit 2ca50ba) -->

# Memory Lifecycle

Verified against code at SAGE v8.1.1 (commit 2ca50ba).

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

`POST /v1/memory/submit` → `handleMemorySubmit` (`api/rest/memory_handler.go`) builds a `TxTypeMemorySubmit` transaction, embeds agent proof, signs with the node's Ed25519 key, and broadcasts via CometBFT RPC. Before broadcast the REST handler stores embedding + provider + knowledge triples in the process-local `SupplementaryCache` (keyed by memoryID, 60 s TTL).

### 2. CheckTx

CometBFT calls `CheckTx` (`internal/abci/app.go:504-537`). The ABCI app decodes the tx, verifies the Ed25519 node signature, checks the nonce for replay protection (BadgerDB `nonce:<agentID>`), and rejects post-fork tx types that arrive pre-fork. Only structurally valid, correctly-signed, non-replayed txs enter the mempool.

### 3. FinalizeBlock — processMemorySubmit

`FinalizeBlock` (`app.go:543-664`) is the deterministic execution path. Key constraint from the code comment: **"No time.Now(), no map iteration without sorting, no goroutines, no external I/O except BadgerDB reads."** Block time from `req.Time` is used for all timestamps.

`processMemorySubmit` (`app.go:826-989`) does:

1. Verifies agent Ed25519 identity proof embedded in the tx.
2. Domain-access check: if domain has a registered owner, calls `HasAccessMultiOrg`; if unowned and not a shared domain, auto-registers the domain with the submitting agent as owner (also issues a level-2 access grant to the owner, buffered for Commit).
3. Generates memoryID deterministically from `SHA256(contentHash + ":" + height + ":" + agentID)` if not provided by the caller.
4. Writes content hash + status `"proposed"` to BadgerDB at key `memory:<memoryID>`.
5. Pops supplementary data (embedding, provider, triples) from `SupplementaryCache`.
6. Buffers a `pendingWrite{writeType:"memory"}` with the full `MemoryRecord` for PostgreSQL.
7. Writes classification to BadgerDB at key `memclass:<memoryID>` — verbatim from the tx field (see `clearance-classification.md`).
8. Buffers a `pendingWrite{writeType:"mem_classification"}`.

### 4. Commit — offchain flush

`Commit` (`app.go:2596-2665`) runs after `FinalizeBlock` for each block. It:

1. Flushes all `pendingWrites` to PostgreSQL **inside a single database transaction** (via `RunInTx`), with exponential-backoff retry for `SQLITE_BUSY`.
2. If the flush fails after max retries, **panics** — consensus has committed the writes on-chain; losing the offchain projection would create undetectable divergence.
3. Saves ABCI state (`height`, `appHash`) to BadgerDB only after PostgreSQL success. This ordering ensures CometBFT replays the block on restart if PostgreSQL write failed.

### 5. Voting → Committed or Deprecated

Each validator (in personal mode: the single node's own auto-voter; in multi-node mode: every validator node, each voting with its own consensus key, plus agents via REST `/v1/memory/{id}/vote`) broadcasts a `TxTypeMemoryVote` tx.

`processMemoryVote` (`app.go:991-1041`):
- Rejects votes from non-validators.
- Stores vote in BadgerDB at key `state:vote:<memoryID>:<validatorID>` (value: `"accept"` / `"reject"` / `"abstain"`).
- Increments on-chain validator vote stats (used for PoE scoring at epoch boundaries).
- Calls `checkAndApplyQuorum`.

`checkAndApplyQuorum` (`app.go:1043-1119`):
- Loads all validators, reads each vote from BadgerDB.
- Phase 1: all validators have **equal weight** (`weights[v.ID] = 1.0`).
- Calls `validator.CheckQuorum` (threshold: `>= 2/3` of total weight, `internal/validator/quorum.go:6`).
- **Quorum reached** → `SetMemoryHash(memoryID, nil, "committed")` in BadgerDB, buffers `status_update{StatusCommitted}` for PostgreSQL.
- **All validators voted, quorum not reached** (e.g. 2-2 tie) → immediately deprecates to prevent the validator ticker from flooding the chain with re-votes.

### 6. Challenge → Deprecated (or Reinstated)

A committed memory may be challenged via `TxTypeMemoryChallenge`. The code comment at `app.go:1133-1134` is authoritative: **"A challenge that passes BFT consensus (included in a block) is decisive — the memory is deprecated immediately. The block inclusion IS the consensus."**

There is no separate voting round for challenges. The challenged memory transitions from `committed` → `deprecated` in the same `processMemoryChallenge` call that includes the tx.

The `validTransitions` map (`lifecycle.go:9`) does list `challenged → committed` as a valid transition, indicating a path exists to reinstate a challenged memory, but no handler currently drives that transition via a tx type.

### 7. Corroborate (does not change status)

`TxTypeMemoryCorroborate` (`processMemoryCorroborate`, `app.go:1166-1188`) writes a `Corroboration` row to PostgreSQL. It does **not** change the memory's status or BadgerDB entry. Confidence is recalculated at query time using the corroboration count (see below).

---

## Node-Local vs. On-Chain Data

| Data                            | Storage           | Notes                                                                                         |
|---------------------------------|-------------------|-----------------------------------------------------------------------------------------------|
| Content hash + status           | BadgerDB (on-chain) | Key `memory:<id>`. Every node maintains a consistent copy via BFT replication.               |
| Classification level            | BadgerDB (on-chain) | Key `memclass:<id>`. Written in `processMemorySubmit` (`app.go:970`), mirrored to PostgreSQL. |
| Validator votes                 | BadgerDB (on-chain) | Key `state:vote:<memoryID>:<validatorID>`. Deterministic quorum input.                        |
| Access grants / domain owners  | BadgerDB (on-chain) | Keys `grant:<domain>:<agentID>`, `domain:<name>`. Written via tx, not REST-only.             |
| Full content, embedding vector  | PostgreSQL (off-chain) | Written in `Commit`; node-local until replicated by PostgreSQL shared service.              |
| Corroborations                  | PostgreSQL (off-chain) | Written in `Commit`. Count read at query time for confidence computation.                    |
| **Tags**                        | **SQLite only (node-local)** | `SetTags`/`GetTags` are **no-ops on PostgresStore** (`store/postgres.go:1383-1395`). Tags exist only in personal (SQLite) mode deployments and are never on-chain. The `QueryOptions.Tags` field is explicitly noted: "any-match filter on user-defined tags (SQLite-only)" (`store/store.go:77`). |
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

Confidence is **computed at query time**, not stored. The PostgreSQL column holds `conf₀`. `handleQueryMemory` calls `memory.ComputeConfidence` per record (`memory_handler.go:655`) and returns the decayed value.

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

A memory reaches `deprecated` via three paths:

1. **Quorum failure**: all validators voted, `acceptWeight / totalWeight < 2/3` → deprecated in `checkAndApplyQuorum` (`app.go:1095-1118`).
2. **Challenge**: a `TxTypeMemoryChallenge` is included in a block → immediately deprecated (`app.go:1135`). No secondary vote.
3. **Explicit transition**: `ValidTransition(proposed → deprecated)` and `ValidTransition(validated → deprecated)` are also allowed for administrative paths, though no current public tx type drives them directly.

Deprecated memories remain in PostgreSQL for audit purposes and are queryable by ID but are excluded from default similarity search results (callers can override with `status_filter`).
