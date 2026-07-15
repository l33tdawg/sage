<!-- Core reconciled through SAGE v11.2.1; Comet dependency note updated for the 2026-07 v11.9 implementation. -->

# Consensus, Confidence, and Decay

Verified against code at SAGE v11.2.1; the Comet dependency note is verified
against the 2026-07 v11.9 implementation. The PoE weight system below is
fork-gated for replay safety: legacy equal-weight or stubbed branches are
retained only so old blocks replay byte-identical. Current live chains use
PoE-weighted quorum, verdict-correctness accuracy, corroboration, and
domain-aware weighting.

## Overview

The in-process `sage-gui` node uses CometBFT v0.38.23 (ABCI 2.0) for Byzantine
Fault Tolerant ordering. The root module's local replacement carries six
state-sync hardenings documented in `third_party/cometbft/README-SAGE.md`: an
inactive-syncer reactor guard; a narrowly scoped block-sync seal-abort sentinel;
effective-height supply and retention across consecutive empty-blockstore
restarts; ordered `seen commit -> positive state ->
successful block-sync switch -> durable completion marker`; atomic state plus
effective-height-marker bootstrap; and synchronous commit/marker persistence
with raw exact-lone-residue recovery. The standalone Docker node copies the
same six source files from exact v0.38.23 source commit
`feb2aea4dc271d612129afc958cb844713ec792b`. The source still declares core
semver `0.38.22`, so its commit-stamped runtime reports
`0.38.22+feb2aea4dc271d612129afc958cb844713ec792b`. Its current topology
harness does not yet perform the authorized state-sync transfer. A memory
transaction is not
"committed" in the SAGE sense until it has survived BFT
consensus **and** validator vote quorum. These are separate stages: CometBFT
provides ordered, non-equivocal block inclusion; validator votes provide
application-level semantic acceptance. Both are required before a memory
becomes queryable with `status=committed`.

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
  - BadgerDB write: mem_class:<id> = classification byte
  - pendingWrite buffered for PostgreSQL
       │
       ▼
Commit called (after FinalizeBlock completes):
  - PostgreSQL writes flushed inside single transaction
  - BadgerDB state (height + AppHash) saved AFTER PostgreSQL succeeds
```

**"Committed" in ABCI means**: the block was committed by CometBFT — i.e., 2/3+ of the voting power precommitted to the block. At this point the memory has `status=proposed` in SAGE, not yet `status=committed`. The SAGE-level "committed" requires an additional validator vote round (below).

### Determinism Requirement

`FinalizeBlock` is marked critical in `app.go:962-963`: **"This method MUST be deterministic. No time.Now(), no map iteration without sorting, no goroutines, no external I/O except BadgerDB reads."** `req.Time` (block time from the proposer) is used for all timestamps.

### Commit Ordering is Load-Bearing

`Commit` (`app.go:3256+`) explains the flush ordering: PostgreSQL writes happen **before** `SaveState` updates the ABCI height in BadgerDB. If PostgreSQL fails, BadgerDB records the old height → CometBFT reads the behind height via `Info()` → replays the block on restart → `FinalizeBlock` re-populates `pendingWrites` → `Commit` retries. This ensures BadgerDB and PostgreSQL cannot permanently diverge.

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

**Current quorum weighting** (`checkAndApplyQuorum`, `app.go`):
- Current chains consult each validator's persisted PoE weight (`v.PoEWeight`), with a `1/N` bootstrap fallback (`poeWeightOrFallback`) for validators with no weight yet.
- For a memory in a non-shared domain `D`, the weight is computed *per-memory* as `ComputeWeight(globalAccuracy, domainAccuracy(v,D), recency, corroboration)` — domain-conditional, read live from `vstats_domain:<v>:<D>` + `vstats:<v>`.
- Shared (`general`/`self`/`meta` and any `sage-*`-prefixed domain) and unknown-domain memories fall back to scalar PoE weight.
- Legacy equal-weight branches are retained only for byte-identical replay of pre-fork blocks.

`HasQuorum` stays ratio-only (`acceptWeight/totalWeight >= 2/3`) across all three, so the gate never moves the threshold itself.

### Vote Lifecycle

```
Validator → POST /v1/memory/{id}/vote (accept | reject | abstain)
    ↓
TxTypeMemoryVote → processMemoryVote (app.go:1461+)
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

**All-voted-no-quorum path** (`app.go:1664-1689`): when all validators have cast votes but the 2/3 threshold is not met (e.g. 2 accept, 2 reject in a 4-validator setup), the memory is **immediately deprecated**. Without this, the memory would remain `proposed` forever and the auto-validator ticker would flood the chain with re-votes.

### Per-Node Memory Auto-Voter (Personal Mode)

In single-node personal mode (`sage-gui serve`), the node's own auto-voter (`internal/voter`) runs the validation checks (dedup, quality, consistency) and casts ONE `TxTypeMemoryVote`, signed with the node's own consensus key (`priv_validator_key.json`) — no validator-set replacement. On a multi-node chain every node votes the same way with its own key. From the perspective of `checkAndApplyQuorum` these votes are ordinary validator votes — they land in BadgerDB via the same tx path.

---

## AppHash and State Root

`ComputeAppHash` (`internal/store/badger.go:221+`) SHA-256 hashes all BadgerDB state in deterministic sorted-key order. This is the `AppHash` returned in `ResponseFinalizeBlock` and stored in every CometBFT block header. It provides tamper-evidence: any state modification outside the tx path would produce a mismatched AppHash and halt consensus.

---

## Proof of Experience (PoE) Weight System

PoE weights are computed at epoch boundaries and drive quorum vote weighting. All four `ComputeWeight` factors below are real on a current chain. Legacy fixed/stub values are the pre-fork replay path only.

### Epoch Boundaries

`internal/poe/epoch.go:10`:
```go
const EpochInterval = 100  // blocks per epoch
```

`IsEpochBoundary(height)` returns true when `height % 100 == 0 && height > 0`. At each boundary, `processEpoch` (`app.go:3058+`) recomputes weights for all validators.

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

**Accuracy (A)** — **verdict-correctness EWMA** (`poe.EWMATracker.Accuracy()`, fed by `UpdateVerdictStats`). When a memory reaches a terminal verdict, each voting validator is credited `1.0` if its vote matched the final committed/deprecated verdict, else `0.0`, into an exponentially-weighted moving average (η=0.9) persisted in `vstats:<validatorID>`:
```
EWMA.Update(match ? 1.0 : 0.0)         // η=0.9 decay
realAccuracy = WeightedSum / WeightDenom
blendFactor  = min(Count / 10, 1.0)    // full weight at K_min=10
A = blendFactor * realAccuracy + (1 - blendFactor) * 0.5   // 0.5 cold-start prior
```
So accuracy measures *being right* (voting with the eventual consensus), not propensity to accept. Legacy replay uses the historical accept-ratio branch.

**Domain score (D)** — **real per-memory**. When quorum runs on a memory in a non-shared domain `D`, `domainAccuracy(v, D)` is the verdict-correctness EWMA of validator `v` *restricted to `D`*, persisted at `vstats_domain:<v>:<D>` (same codec as `vstats:`), cold-starting at 0.5. The memory's domain is recorded at submit in `memdomain:<memoryID>`. The per-epoch *scalar* `domain_score` in `processEpoch` stays `0.5` by design — it is only the fallback weight for shared/unknown-domain memories; the real domain factor is applied per-memory in `checkAndApplyQuorum`.

**Recency (T)** — `poe.RecencyScore(lastActive, now)` (`epoch.go:29-36`):
```
T = exp(-λ * Δt_hours)
λ = RecencyLambda = 0.01
```
Hours are approximated from block height difference: `blocksSinceLast * 3.0 / 3600.0` (assuming 3 s/block). A validator that voted 100 blocks ago (~5 min) has `T ≈ 0.999`; 1000 blocks ago (~50 min) `T ≈ 0.992`.

**Corroboration score (S)** — **real lifetime count**: `poe.CorroborationScore(CorrCount, CorrMax)` = `log(1+CorrCount) / log(1+CorrMax)`, where `CorrCount` (persisted in `vstats:<validatorID>`) increments each time the validator's vote matched a terminal verdict.

### Reputation Cap

`poe.NormalizeWeights` applies a 10% cap (`RepCap = 0.10`): no single validator can hold more than 10% of total normalized weight. Applied iteratively until stable, then normalized to sum to 1.0. (With < 10 validators the cap can't bind, so the set collapses to equal `1/N` — which is why small devnets behave like equal-weight quorum even post-`app-v3`.)

`processEpoch` uses `poe.NormalizeWeightsDeterministic`, which performs the weight-total summations in **sorted-key order**. The legacy `NormalizeWeights` branch is retained only for pre-fork replay.

### Weight Storage

Epoch scores are buffered as `pendingWrite{writeType:"epoch_score"}` and `pendingWrite{writeType:"validator_score"}` and flushed to PostgreSQL in `Commit`. BadgerDB retains the raw vote stats (`IncrementVoteStats` at `app.go:1490`).

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

`POST /v1/memory/{id}/corroborate` → `TxTypeMemoryCorroborate` → `processMemoryCorroborate` (`app.go:1767+`):

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
