<!-- Verified against code at SAGE v8.1.1 (commit 2ca50ba) -->

# Consensus, Confidence, and Decay

Verified against code at SAGE v8.1.1 (commit 2ca50ba).

## Overview

SAGE uses CometBFT v0.38.15 (ABCI 2.0) for Byzantine Fault Tolerant ordering. A memory transaction is not "committed" in the SAGE sense until it has survived BFT consensus **and** validator vote quorum. These are separate stages: CometBFT provides ordered, non-equivocal block inclusion; validator votes provide application-level semantic acceptance. Both are required before a memory becomes queryable with `status=committed`.

---

## CometBFT BFT Consensus Path

```
Agent REST submit
       │
       ▼
REST handler builds signed tx → broadcasts via /broadcast_tx_sync (CometBFT RPC)
       │
       ▼
CometBFT mempool (CheckTx validates signature + nonce)
       │
       ▼
Block proposer includes tx in block proposal (PrepareProposal — pass-through)
       │
       ▼
Other validators receive proposal (ProcessProposal — ACCEPT pass-through)
       │
       ▼
2/3+ validators prevote → precommit → block committed by CometBFT
       │
       ▼
FinalizeBlock called on all nodes deterministically with req.Time (not time.Now())
       │
       ▼
processMemorySubmit:
  - BadgerDB write: memory:<id> = hash + "proposed"
  - BadgerDB write: memclass:<id> = classification byte
  - pendingWrite buffered for PostgreSQL
       │
       ▼
Commit called (after FinalizeBlock completes):
  - PostgreSQL writes flushed inside single transaction
  - BadgerDB state (height + AppHash) saved AFTER PostgreSQL succeeds
```

**"Committed" in ABCI means**: the block was committed by CometBFT — i.e., 2/3+ of the voting power precommitted to the block. At this point the memory has `status=proposed` in SAGE, not yet `status=committed`. The SAGE-level "committed" requires an additional validator vote round (below).

### Determinism Requirement

`FinalizeBlock` is marked critical in `app.go:541-542`: **"This method MUST be deterministic. No time.Now(), no map iteration without sorting, no goroutines, no external I/O except BadgerDB reads."** `req.Time` (block time from the proposer) is used for all timestamps.

### Commit Ordering is Load-Bearing

`app.go:2586-2595` explains the flush ordering: PostgreSQL writes happen **before** `SaveState` updates the ABCI height in BadgerDB. If PostgreSQL fails, BadgerDB records the old height → CometBFT reads the behind height via `Info()` → replays the block on restart → `FinalizeBlock` re-populates `pendingWrites` → `Commit` retries. This ensures BadgerDB and PostgreSQL cannot permanently diverge.

---

## Validator Vote Quorum

After block inclusion, a memory has `status=proposed`. It needs validator votes to reach `status=committed`.

### Quorum Threshold

`internal/validator/quorum.go:6`:
```go
const QuorumThreshold = 2.0 / 3.0
```

`CheckQuorum` (`quorum.go:12-30`) sorts validator IDs for deterministic iteration, then checks:
```
acceptWeight / totalWeight >= 2/3
```

In Phase 1 all validators use **equal weight = 1.0** (`app.go:1055`): `weights[v.ID] = 1.0`. PoE weights computed at epoch boundaries are stored in `ValidatorInfo.PoEWeight` but not yet used for quorum decisions in Phase 1.

### Vote Lifecycle

```
Validator → POST /v1/memory/{id}/vote (accept | reject | abstain)
    ↓
TxTypeMemoryVote → processMemoryVote (app.go:991-1041)
    ↓
BadgerDB: state:vote:<memoryID>:<validatorID> = "accept"|"reject"|"abstain"
    ↓
checkAndApplyQuorum(memoryID, height, blockTime)
    ↓
    ┌─ quorum reached (acceptWeight/totalWeight >= 2/3)?
    │    → SetMemoryHash(id, nil, "committed") in BadgerDB
    │    → pendingWrite{status_update, StatusCommitted}
    │
    └─ all validators voted, quorum not reached?
         → SetMemoryHash(id, nil, "deprecated") in BadgerDB
         → pendingWrite{status_update, StatusDeprecated}
```

**All-voted-no-quorum path** (`app.go:1095-1118`): when all validators have cast votes but the 2/3 threshold is not met (e.g. 2 accept, 2 reject in a 4-validator setup), the memory is **immediately deprecated**. Without this, the memory would remain `proposed` forever and the auto-validator ticker would flood the chain with re-votes.

### In-Process App Validators (Personal Mode)

In single-node personal mode (`sage-gui serve`), four in-process app validators (sentinel, dedup, quality, consistency) replace the multi-node validator set. They sign and broadcast `TxTypeMemoryVote` transactions autonomously via CometBFT RPC. From the perspective of `checkAndApplyQuorum` they are indistinguishable from network validators — their votes land in BadgerDB via the same tx path.

---

## AppHash and State Root

`ComputeAppHash` (`internal/store/badger.go:216+`) SHA-256 hashes all BadgerDB state in deterministic sorted-key order. This is the `AppHash` returned in `ResponseFinalizeBlock` and stored in every CometBFT block header. It provides tamper-evidence: any state modification outside the tx path would produce a mismatched AppHash and halt consensus.

---

## Proof of Experience (PoE) Weight System

PoE weights are computed at epoch boundaries and influence future vote weighting (currently stored but not yet used for quorum in Phase 1).

### Epoch Boundaries

`internal/poe/epoch.go:10`:
```go
const EpochInterval = 100  // blocks per epoch
```

`IsEpochBoundary(height)` returns true when `height % 100 == 0 && height > 0`. At each boundary, `processEpoch` (`app.go:2457-2582`) recomputes weights for all validators.

### Weight Formula

`internal/poe/engine.go`:
```
W = exp(α·ln(A) + β·ln(D) + γ·ln(T) + δ·ln(S))
```

Where:
- `α = 0.4` — Accuracy component weight
- `β = 0.3` — Domain expertise component weight  
- `γ = 0.15` — Recency component weight
- `δ = 0.15` — Corroboration component weight
- `EpsilonFloor = 0.01` — prevents log(0)

### Component Definitions

**Accuracy (A)** — accept ratio with cold-start blending (`app.go:2479-2491`):
```
realAccuracy = AcceptVotes / TotalVotes
blendFactor = min(TotalVotes / 10, 1.0)   // full weight at K_min=10
A = blendFactor * realAccuracy + (1 - blendFactor) * 0.5   // 0.5 = cold-start prior
```
Cold-start: validators with < 10 votes are blended toward 0.5.

**Domain score (D)** — Phase 1 uses a fixed value of `0.5` for all validators (`app.go:2493-2494`). Per-domain tracking is deferred.

**Recency (T)** — `poe.RecencyScore(lastActive, now)` (`epoch.go:29-36`):
```
T = exp(-λ * Δt_hours)
λ = RecencyLambda = 0.01
```
Hours are approximated from block height difference: `blocksSinceLast * 3.0 / 3600.0` (assuming 3 s/block). A validator that voted 100 blocks ago (~5 min) has `T ≈ 0.999`; 1000 blocks ago (~50 min) `T ≈ 0.995`.

**Corroboration score (S)** — Phase 1 uses `poe.CorroborationScore(0, CorrMax)` (fixed, `app.go:2511`). Per-validator corroboration count tracking is deferred.

### Reputation Cap

`poe.NormalizeWeights` (`engine.go:37-82`) applies a 10% cap (`RepCap = 0.10`): no single validator can hold more than 10% of total normalized weight. Applied iteratively until stable, then normalized to sum to 1.0.

### Weight Storage

Epoch scores are buffered as `pendingWrite{writeType:"epoch_score"}` and `pendingWrite{writeType:"validator_score"}` and flushed to PostgreSQL in `Commit`. BadgerDB retains the raw vote stats (`IncrementVoteStats` at `app.go:1020`).

---

## Memory Confidence Scoring

### Formula

Source: `internal/memory/confidence.go:36-60`.

```
conf(M, t) = conf₀ · exp(-λ_M · Δt_days) · (1 + 0.1 · ln(1 + corr_count))
```

- `conf₀` — initial confidence at submission, stored in PostgreSQL
- `λ_M` — decay rate per day (domain-specific)
- `Δt_days` — `now.Sub(CreatedAt).Hours() / 24.0`
- `corr_count` — corroboration count from PostgreSQL at query time
- Result clamped to `[0, 1]`

**This is computed at query time, not stored.** PostgreSQL stores `conf₀`; the decayed value is computed in `handleQueryMemory` per-result.

### Domain Decay Rates

| Domain tag   | λ     | Half-life |
|--------------|-------|-----------|
| `crypto`     | 0.001 | ~693 days |
| `vuln_intel` | 0.01  | ~69 days  |
| (all others) | 0.005 | ~139 days |

### Task Exception

Open tasks (`memory_type=task` with `task_status` in `{planned, in_progress}`) are exempt from decay. `ComputeConfidenceForRecord` (`confidence.go:27-33`) returns `conf₀` unchanged. Completed (`done`) and dropped tasks decay normally.

### Corroboration Boost

Each corroboration adds to the boost factor `(1 + 0.1 · ln(1 + n))`:

| Corroborations | Boost factor |
|----------------|-------------|
| 0              | 1.000       |
| 1              | 1.069       |
| 5              | 1.179       |
| 10             | 1.240       |
| 20             | 1.310       |

Corroborations do not change the memory's `ConfidenceScore` column in PostgreSQL. They are counted at query time via `GetCorroborations`.

---

## Corroboration via Consensus

`POST /v1/memory/{id}/corroborate` → `TxTypeMemoryCorroborate` → `processMemoryCorroborate` (`app.go:1166-1188`):

1. Verifies agent Ed25519 identity proof.
2. Buffers a `Corroboration` row for PostgreSQL (via `pendingWrite{writeType:"corroborate"}`).
3. Does **not** update the memory's status in BadgerDB or PostgreSQL.
4. Does **not** change `ConfidenceScore` in PostgreSQL.

The only effect is adding a row to the `corroborations` table that is counted at query time. The confidence uplift is thus **retroactive** — all future queries against that memory will see a higher confidence score.

---

## Storage Split Summary

| Datum | BadgerDB (on-chain) | PostgreSQL (off-chain) |
|-------|--------------------|-----------------------|
| Memory content hash + status | Yes | Mirrored |
| Memory classification | Yes | Mirrored |
| Validator votes | Yes | Mirrored |
| Access grants / domain ownership | Yes | Mirrored |
| Validator vote stats (for PoE) | Yes | — |
| PoE epoch scores | — | Yes |
| Full memory content + embedding | — | Yes |
| Corroboration records | — | Yes |
| `conf₀` (stored confidence) | — | Yes |
| Tags (personal mode only) | — | SQLite only |

---

## State Diagram: Memory from Submit to Query

```
[REST Submit]
     │
     ▼
TxTypeMemorySubmit in mempool (CheckTx: sig + nonce valid)
     │
     ▼  (BFT block commit — 2/3+ CometBFT validators)
FinalizeBlock → BadgerDB: proposed
Commit       → PostgreSQL: status=proposed, conf₀ stored
     │
     ▼
TxTypeMemoryVote × N validators
     │
     ├── acceptWeight/totalWeight >= 2/3
     │        → BadgerDB: committed
     │        → PostgreSQL: status=committed, committed_at set
     │        → Memory appears in queries with status_filter=committed
     │
     └── all voted, no quorum
              → BadgerDB: deprecated
              → PostgreSQL: status=deprecated

[Later, any time after committed:]
TxTypeMemoryCorroborate
     → PostgreSQL: +1 corroboration row
     → Future query confidence = conf₀ · decay · (1 + 0.1·ln(1+count))

TxTypeMemoryChallenge (if committed)
     → BadgerDB: deprecated (immediate, no vote)
     → PostgreSQL: status=deprecated
```
