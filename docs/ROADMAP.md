# SAGE Roadmap

**Status (2026-07):** **v11.0.0 is the current release.** The migration substrate (v7.5) and the consensus-rule work (v8.0 / v8.2 / v8.3) this roadmap was originally written around have all shipped and are carried forward in v11; their detailed histories are preserved further down under "Historical track". This document now looks forward from v11 to the v11.5 slate. Everything past v11 is planned, not promised, and carries no date.

**Hard constraint driving the whole plan:** no chain reset, no operator-typed commands. Existing chains must upgrade in place across all future releases.

---

## v11 — shipped (the sovereign-UX + federation release)

v11 is the "zero-terminal, sovereign" release. It takes SAGE from "works if you know the CLI" to "a person clicking buttons can stand up a private, semantic, federated memory node." What landed:

### Onboarding and setup

- **First-run onboarding wizard.** Fresh nodes get a three-step welcome (orientation, semantic memory, connect an AI tool). Closing it marks onboarding done; it is re-runnable any time from **Settings > Maintenance > Run setup**.
- **Guided semantic-memory setup.** One flow turns on the bundled embedder (Ollama + `nomic-embed-text`): detect Ollama, pull the model, re-embed existing memories as a durable background job with a progress banner that survives reloads, then switch recall over. Includes recovery-key backup and honest handling of undecryptable memories (surfaced, not silently dropped).
- **One-click managed reranker.** After a single consent click, SAGE downloads a pinned, sha256-checksum-verified llama.cpp engine build and the `bge-reranker-v2-m3` cross-encoder model, then runs and manages the sidecar process itself (loopback only, nothing leaves the machine). Recall results-per-query (k) is tunable 3-20. Bring-your-own TEI-compatible servers are still supported.
- **Connect-an-AI-tool flows.** A single dashboard flow branches three ways: same-machine one-click config writing (Claude Code, Codex, Cursor, Windsurf, Claude Desktop), remote MCP over the operator's own cloudflared tunnel (local-first, no shared cloud), and LAN node-join (another computer becomes a peer node sharing this node's memory).

### Federation

- **Whole-SAGE-to-whole-SAGE join ceremony.** Guided guest and host wizards, offline-bundled. Trust is anchored by a human scan-and-compare; the six-digit codes are TOTP (RFC-6238) proving co-possession under a 2-of-2 consent handshake. Scope grants (allowed domains + a 0-4 clearance ceiling) are enforced when serving recall, recorded as an on-chain treaty, and revocable (revoke erases no memories).
- **Off-consensus transport.** mTLS federation listener, read-only recall query proxy (foreign results are merge-in-response only, never persisted to your chain), and receipt exchange.
- **Consensus-layer federation primitives.** On-chain `cross_fed` exchange terms (Mode-1, tx 33/34) and the co-commit primitive (tx 31/32) landed at the app layer.

### Consensus and memory integrity

- **app-v15 verb-ladder.** Closed the ungated-deprecate hole (deprecation is now audit-only / consensus-gated) and added a grantable level-3 (modify).
- **Globally-unique `chain_id`** minted at genesis.
- **Orphaned-memory recovery** (old-key re-key) and `embedding_provider` stamped at insert so new memories no longer pose as unembedded.

### CEREBRUM (the dashboard)

- **MRI 3D brain is the default view** (three.js + 3d-force-graph bundled locally, so it renders fully offline); a 2D force-graph remains a toggle away and the choice is remembered.
- **Click-a-memory "train of thought"** board (Do's / Don'ts / Observations / Notes), computed from lineage, tags, content overlap, and same-lobe signals; hop card to card to walk the connectome.
- **Reading panel** collapses to the domain lobes by default (newest 30, most-recently-active first) with an expandable "how to read".
- **Live task board** with agent-vs-human authorship and atomic claim/ownership; the agent message bus merged in as a Messages tab.
- **Real search** (FTS with keyword fallback), bulk curation, status and tag filters, and corroboration counts on list + detail.
- **Settings reorganized** into focused tabs (Overview, Connection, Recall, Security, Maintenance, Updates), with in-place updates and node restart.

---

## v11.5 — planned

Forward-looking. Nothing here is committed to a date; these are the problems v11.5 is scoped to solve, in rough priority order. Treat every item as speculative until it ships.

### Shared-domain replication

Today federation is read-only recall exchange: borrowed answers are shown in the moment, tagged with their source, and never written to your chain. v11.5 adds opt-in **domain sync** so a shared domain can be *replicated* to a peer rather than only queried. The design centers on a **durable outbox** (writes to a shared domain are queued locally and delivered reliably across restarts and network gaps) plus **anti-entropy backfill** (periodic reconciliation so a peer that was offline catches up on what it missed). Bounded by the same scope grants as recall exchange; no silent widening of what crosses the link.

### Reinstate verb + quorum-scaled deprecation

Bring back a first-class deprecation verb with teeth: deprecation gated by consensus, with the required **quorum scaled to network size** so a small-LAN node and a large federation apply proportionate bars instead of one hardcoded threshold. Complements the v11 change that made deprecation audit-only.

### RBAC clarity + cross-scope memory transfer

Make the access model legible (who can read, write, and modify what, and why) and add a governed way to **transfer memories across scopes** (hand a memory from one agent, org, or domain to another) without losing attribution or bypassing clearance.

### libp2p NAT traversal + author-operated connectivity service

Replace the current same-LAN / bring-your-own-tunnel reachability story with **libp2p-based NAT traversal**, backed by an **author-operated connectivity service** (a relay / rendezvous the project runs) so two sovereign nodes behind home routers can find and reach each other without port-forwarding or a third-party cloud. Sovereignty is preserved: the service brokers connectivity only, it never sees or stores memory.

---

## Historical track — v7.5 to v8.3 (shipped, carried into v11)

The sections below are the original roadmap for the migration substrate and consensus-rule cleanups. They are retained as an implementation record; all of this shipped and is part of v11.

---

## TL;DR

- **v7.5 = the migration substrate.** Backup/restore + auto-upgrade machinery. Zero consensus-rule changes. This is what makes every subsequent release safe to land on existing chains.
- **v8.0 = the consensus-rule cleanup that's been waiting.** Three access-control fixes (ancestor-grant walk, auto-register on grant, `domain_reassign` recovery) — all gated behind the v7.5 fork-height mechanism so old chains upgrade without losing memory.
- **Validator scaling is not on this roadmap.** Paper 1's 4-specialist PoE model is sound for the deployment shapes SAGE actually targets. Going to N=7/10 is a federation-deployment concern blocked behind v6.0-dynamic-validator-governance, which itself isn't blocking any concrete user today. We document the rationale; we don't preempt.

---

## Paper 1 / PoE soundness — why we're not scaling validators

Two distinct axes get conflated in "should SAGE have 8 validators":

| Axis | What it controls | Where SAGE sits today |
|---|---|---|
| **A. BFT replication count (N=3f+1)** | Byzantine tolerance | N=4, f=1. Sufficient for sovereign single-machine and small-LAN. |
| **B. Specialist validator axes (PoE)** | Quality coverage | 4 specialists: Sentinel, Dedup, Quality, Consistency. |

Paper 1's contribution is Axis B — that **specialists > replicas**. That claim is intact at any N. Adding more replicas of Quality doesn't make decisions better; adding a new specialist axis does.

**When does scaling Axis A actually matter?**
- Sovereign single-machine: never. All validators run on one box; if it falls, they all fall.
- Small-LAN federation (today's reality): N=4 across 4 boxes is enough for tolerance of one box failing.
- Cross-org federation (v7.0+ roadmap, ~0% deployed): N=4 is thin if each org runs one validator and an org-level compromise = two-validator compromise. **This is the only regime where N=7/10 buys real safety, and we have no concrete deployment asking for it yet.**
- Public-internet adversarial: different system entirely (would need staking/slashing). Not on this roadmap.

**The Axis B research direction worth funding (v8+):** three plausible new specialist axes —
- **Provenance** — does the cited source actually contain this claim? (catches hallucinated memories)
- **Sensitivity** — does this leak PII / secrets / protected info? (catches accidental disclosure)
- **Relevance** — does the content match the domain it's filed under? (catches namespace pollution)

Each one is its own focused validator with its own evaluation pipeline. None ship as v7.5 or v8.0 — they're follow-on research, ideally as paper work alongside.

---

## v7.5 — Migration substrate (zero consensus changes) — IMPLEMENTED

**Theme:** "every future release upgrades in place." No consensus-rule changes in v7.5 itself; the whole release is plumbing.

### Implementation status (2026-05-19, branch: `v7.5-dev`, 11 commits since v7.1.2)

| Sub-component | Commit | Status |
|---|---|---|
| Upgrade tx types + codec | `80099f2` | ✅ landed |
| sage-launcher supervise mode + halt detection + rollback flow | `0189a96` | ✅ landed |
| internal/snapshot/ Take / Verify / Restore / Diagnose / Sweep | `a5d2a44` | ✅ landed |
| launcher ↔ snapshot.Restore adapter | `800aff8` | ✅ landed |
| HALT sentinel writer + empty-RollbackTo fallback | `a0013b1` | ✅ landed |
| LiveBadger handle plumbing (no lockfile conflict) | `ec3e188` | ✅ landed |
| Scheduled-snapshot trigger in app.Commit | `6baaaca` | ✅ landed |
| UpgradePlan persistence + FinalizeBlock activation | `4c788c6` | ✅ landed |
| Upgrade watchdog auto-propose on boot | `2e46a4d` | ✅ landed |
| End-to-end consensus-path integration test | this commit | ✅ landed |

Build: `go build ./...` clean. Tests: `go test ./...` green on all 22 packages. Lint: `golangci-lint run ./...` clean. Not yet pushed to origin (per "no main push until end-to-end working" policy).

### Components

**A. Backup / restore (`backup-restore.md`)**
- New `internal/snapshot/` package: `scheduler.go`, `verify.go`, `diagnose.go`.
- Snapshot layout `~/.sage/snapshots/<height>/{manifest.json, badger.backup, sage.db, cometbft-data.tar.zst, config.tar.zst, OK}` with atomic-rename + `OK` sentinel.
- Triggers: height-based (every 10k blocks), time-based (6h), pre-upgrade (synchronous, mandatory).
- Verify primitive *restores into a tmpdir and re-computes AppHash* — proves restorability, not just file integrity.
- Auto-restore on boot via `DiagnoseDataDir` → corrupt Badger / corrupt SQLite / mid-write CometBFT / `HALT` sentinel.
- Retention: K=5 latest + one anchor per binary version pinned (powers downgrade).
- `sage-launcher` extended to re-exec previous binary on post-upgrade halt (currently exists as a directory in the repo but doesn't yet re-exec).

**B. Auto-upgrade machinery (`upgrade-machinery.md`)**
- On-chain `UpgradePlan` record in BadgerDB, keyed `upgrade/plan`.
- Two new tx types reusing the existing governance pipeline:
  - `TxTypeUpgradePropose` (27) — body is an `UpgradePlan` minus `ActivationHeight`.
  - `TxTypeUpgradeCancel` (28) — quorum-gated pre-activation cancel.
- **Critical design choice:** `ActivationHeight` is *computed by the chain at proposal-execution time* (`ExecutedHeight + UpgradeDelayBlocks`), not picked by any individual validator. This eliminates the multi-validator drift problem.
- Activation via `ResponseFinalizeBlock.ConsensusParamUpdates.Version.App = N` — CometBFT-native, takes effect at H+1 atomically across all nodes.
- Self-seeding `upgrade_watchdog` goroutine: on boot, if binary's embedded `TargetAppVersion > state.AppVersion` and no plan exists, deterministically-staggered proposal via the watchdog (single-validator chains skip the stagger — proposer is the quorum).
- Three-layer rollback: pre-activation cancel tx (clean), halt-on-handler-panic (clean), post-activation `TxTypeUpgradeRevert` with pre-mutation snapshot restore (best-effort; we're honest about BFT rollback limits).
- Implements the CometBFT state-sync ABCI methods (`ListSnapshots`, etc., currently stubbed) — required for late-booting validators to catch up across an upgrade.

### Sequencing within v7.5

Backup/restore lands first because upgrade-machinery rollback depends on the snapshot/anchor infrastructure. Auto-upgrade lands second and uses backup/restore as the "pre-activation snapshot" + "downgrade target".

### Acceptance criteria — status

- ✅ **Existing v7.1.x chain on disk upgrades to v7.5 *with zero operator commands*.** Watchdog auto-proposes (when `upgradeTargetAppVersion > current`), `FinalizeBlock` activates deterministically at chain-computed height. `cmd/sage-launcher` supervises and re-execs on halt.
- ✅ **Snapshots verified post-write by tmpdir-restore.** `internal/snapshot/verify.go::Verify` reads the badger backup into a tmpdir, recomputes AppHash, compares to manifest. `TestTakeVerifyRestore_HappyPath` exercises take → verify → restore → AppHash-match end-to-end.
- ✅ **Synthetic upgrade-halt scenario.** `TestSnapshotScheduler_*` produces anchors; `cmd/sage-launcher` test suite exercises HALT-detection → rollback dispatch → re-exec. `TestHaltOnPanic_WritesSentinelAndRepanics` exercises the producer side.
- ⏳ **Multi-validator drift test.** Not yet exercised because v7.5-dev is single-machine only. The design guarantees no drift because `ActivationHeight` is chain-computed inside `FinalizeBlock`, not validator-chosen. To test, would spin up a 4-validator local cluster and assert all activate at the same block — queued for the federation work that gets v6.0-dynamic-validator-governance back on the roadmap.
- ✅ **Zero operator-facing CLI surface.** No new `sage-cli` subcommands. Internal `sage-launcher --supervise` flag is opt-in; the existing detached-launcher path (used by macOS .app / Windows .exe double-click) is untouched.

### End-to-end test coverage

`internal/abci/upgrade_e2e_test.go` exercises the full consensus path:

1. Build a signed `UpgradePropose` tx via `signAgentProof` (same helper the watchdog uses).
2. Encode with `tx.EncodeTx` and decode with `tx.DecodeTx` — proves wire format roundtrip.
3. Dispatch via `processTx` (same path `FinalizeBlock` takes for a real tx).
4. Assert `UpgradePlanRecord` persisted in BadgerDB with chain-computed activation height (proposeHeight + `defaultUpgradeDelayBlocks=200`).
5. `FinalizeBlock` at `activationHeight - 1` → no `ConsensusParamUpdates`.
6. `FinalizeBlock` at `activationHeight` → `ConsensusParamUpdates.Version.App` set, plan cleared, `AppliedUpgradeRecord` written.
7. `FinalizeBlock` at `activationHeight + 1` → no re-emission of `ConsensusParamUpdates`.

A companion test, `TestV75_EndToEnd_CancelBeforeActivation`, verifies the cancel path: propose → cancel → former activation block produces no `ConsensusParamUpdates` and no applied record.

---

## v8.0 — Access-control consensus cleanup

**Theme:** the three real bugs levelup surfaced in v7.1, all gated behind `app.v75ForkHeight` so old chains upgrade in place.

### Fixes (`access-control-fixes.md`)

**Fix 1 — `HasAccessOrAncestor` (ancestor-walk grants).** New sibling to `HasAccess`; walks dotted path most-specific-to-least, skips expired grants and continues. First valid match wins on clearance (no min-across-path). `HasAccessMultiOrg` swaps its internal exact-match for the ancestor-walk variant post-fork.

**Fix 2 — `processAccessGrant` auto-register on unowned.** Mirrors `processMemorySubmit`'s existing auto-register pattern. If the grant's target domain has no owner and isn't shared, granter auto-becomes owner. Shared domains (`general`, `self`, `meta`, `sage-*`) explicitly reject with a distinct error code. Composes with Fix 1 — granting on a parent auto-claims it and covers descendants.

**Fix 3 — `domain_reassign` governance primitive.** New `TxTypeDomainReassign` (27) linked to an accepted `gov_propose` via `ProposalID`. Reuses the *existing* `TransferDomain` primitive in `badger.go:593` (which has been waiting for this caller). Existing grants invalidated cleanly on reassign (recovery-grade semantics). `OpenToShared=true` flag stores a new on-chain `shared_domain:<name>` key; `isSharedDomain` becomes hybrid static+dynamic check.

### Cross-cutting

- All three gated by `app.v75ForkHeight`. Pre-fork blocks replay byte-identical to v7.1.1 — CI pins specific v7.1.x blocks at `bench/results/v7.1.x/` and asserts replay parity.
- New `sage_fork_branch_total{fork="v75",branch="pre|post"}` counter so we can confirm cutover live.
- All three fixes have pre-fork + post-fork tests. No "trust the gate works" — both branches exercised.

### Acceptance criteria

- All three fixes land in one v8.0 commit pair with the fork-gate plumbing.
- `domain_reassign` quorum threshold defaults to current gov quorum (2/3); proposal type field reserved so a higher threshold can be enforced by the quorum engine without an additional tx-type bump.
- Levelup's bootstrap script can be deleted post-v8.0 (probe-write + per-subdomain register-then-grant becomes unnecessary because grants cascade).
- ARCHITECTURE.md's "Known limitations (v7.1)" section gets struck through and replaced with the v8.0 semantics.

---

## v8.2 — PoE-weighted quorum (Phase-2 PoE)

**Theme:** wire the PoE engine's computed `v.PoEWeight` into `checkAndApplyQuorum`. The engine has been running every epoch since v6.x; the quorum branch ignored it and used `weights[v.ID] = 1.0`. v8.2 closes that gap with a single fork-gated swap. Pre-fork blocks replay byte-identical to v8.1.2.

### Implementation status (2026-05-28, branch: `v8.2-dev`)

| Sub-component | Commit | Status |
|---|---|---|
| `app-v3` fork-gate plumbing (`v8_2AppliedHeight`, `postV8_2Fork`, `refreshV8_2Fork`, `recordV8_2Branch`) + watchdog bump to 3 | `75896c9` | ✅ landed |
| CometBFT v0.38.15 → v0.38.23 (security + blocksync/nil-vote/ABCI-socket hardening) | `9a89677` | ✅ landed |
| `SetEpochWeights` / `GetEpochWeights` on-chain persistence (`poew:current` + `poew:<id>`) + W1-W5 | `398eaf8` | ✅ landed |
| `refreshPoEWeights` hydration + `processEpoch` persistence call + `checkAndApplyQuorum` post-fork branch + F1-F6 / L1-L3 / R1-R2 | `8da4230` | ✅ landed |

Build: `go build ./...` clean. Tests: `go test ./...` green (21 packages, 16 new v8.2 tests). Lint: `gofmt -l` clean. Not yet pushed (mirrors v7.5 / v8.0 "no main push until end-to-end working" policy).

### Fix matrix

**Fix 1 — On-chain PoE weight persistence.** New BadgerDB key prefix `poew:`. `processEpoch` writes `poew:current` (uvarint epoch number) + one `poew:<validatorID>` (IEEE-754 big-endian float64) per validator atomically, in sorted order. A validator removed via governance is pruned from `poew:*` on the next epoch boundary — no stale weight survives. ComputeAppHash picks up the new keys via the existing lex-sorted iterator (no change).

**Fix 2 — Boot-time hydration.** `refreshPoEWeights` called from both `NewSageApp` paths right after `LoadValidators`. A node restarting between epoch boundaries gets each validator's `PoEWeight` set from `poew:<id>` rather than the zero-value default. Closes the hazard where two peers restarting at different points between the same two boundaries would diverge consensus.

**Fix 3 — Fork-gated quorum swap.** `checkAndApplyQuorum` post-fork goes through `poeWeightOrFallback(v.PoEWeight, len(validators))`, returning `1/N` for validators with `PoEWeight == 0` (pre-first-epoch chain, mid-epoch governance add, or any defensive guard). Pre-fork keeps the `weights[v.ID] = 1.0` branch. `recordV8_2Branch` emits the existing `sage_fork_branch_total` metric so operators can confirm the gate flipped without scraping logs.

### Cross-cutting

- All three gated by `app.v8_2AppliedHeight`. Strict greater-than mirrors `postV8Fork`'s "applied at H+1" semantic; the activation block itself stays pre-fork so the only AppHash delta at H_act is the `MarkUpgradeApplied` write.
- `sage_fork_branch_total{fork="v8.2",branch="pre|post"}` counter — same metric name as the v8.0 fork, additional label.
- Pre-fork + post-fork tests for every change (F1-F6 + R1-R2). Cold-boot fallback (F3), mid-epoch validator add (F4), and gate-flip at H_act+1 (F5) all exercised.

### Acceptance criteria — status

- ✅ **Pre-fork replay parity.** R1 confirms no `poew:*` keys land on a pre-fork chain → ComputeAppHash sees the exact same keyspace v8.1.2 would have. R2 confirms post-fork chains DO write `poew:*`, so the gate is observable in the digest.
- ✅ **Cold-boot hazard closed.** F3 exercises a post-fork chain where every validator has `PoEWeight == 0` — the `1/N` fallback fires and quorum behaves identically to the equal-weight pre-fork branch.
- ✅ **Mid-epoch validator add not disenfranchised.** F4 exercises a validator added mid-epoch with `PoEWeight == 0` — they get `1/N` and their vote is load-bearing (the inverse case where their vote flips the outcome is also pinned).
- ✅ **Restart between epoch boundaries.** L3 brings a SageApp through one post-fork epoch, kills it, restarts on the same store, and confirms `PoEWeight` matches the values persisted at the epoch boundary — no re-running `processEpoch` required.
- ⏳ **Multi-validator drift test.** Same constraint as v7.5: single-machine deployment exercises the property by construction, but a 4-validator devnet activation run is the proper acceptance gate. Queued for the federation-deployment work.

### What's NOT in v8.2 (tracked as follow-on work)

- **Real EWMA accuracy.** ~~`processEpoch` still uses a manual cold-start blend rather than `internal/poe/ewma.go`'s `EWMATracker`.~~ ✅ **Shipped in v8.3** (verdict-correctness EWMA).
- **Per-domain expertise.** `domainScore = 0.5` hardcoded. Real domain scoring needs per-validator per-domain vote stats (`vstats_domain:<v>:<d>`) and a way for validators to publish which domains they vote in. v8.4+.
- **Per-validator corroboration.** ~~`corrScore` defaults to `CorroborationScore(0, CorrMax)`.~~ ✅ **Shipped in v8.3** (lifetime verdict-match count).
- **REST surface.** Vote handler already returns `PoEWeight` per memory; a chain-level `/v1/validators` endpoint with the live weight set is operator-friendly but not blocking. v8.5+.

---

## v8.3 — Real PoE signals: verdict-correctness EWMA + corroboration (Phase-2 PoE, second installment)

**Theme:** v8.2 made the *quorum* consult `v.PoEWeight`, but two of the four weight factors were still Phase-1 stubs. v8.3 makes them real, bundled behind one `app-v4` fork because they share an on-chain encoding migration and a single crediting event:

- **Accuracy (A, α=0.40)** — `EWMATracker.Accuracy()` over each validator's *verdict-correctness* (did its vote match the final committed/deprecated verdict), replacing the cold-start accept-ratio blend. This is what `ewma.go`'s `Update` always documented ("0.0=wrong, 1.0=correct").
- **Corroboration (S, δ=0.15)** — real lifetime verdict-match count via `CorroborationScore(CorrCount)`, replacing the hardcoded 0.

Both are credited at the **same event** — a memory reaching a terminal verdict in `checkAndApplyQuorum` — and persisted in the `vstats:<id>` record, which grows 24→56 bytes post-fork (length-dispatch decode keeps legacy records valid). Domain (D, still 0.5) stays a stub for v8.4+; Recency (T) was already real.

### Implementation status (branch: `v8.3-dev`)

| Piece | Commit | Status |
|---|---|---|
| `app-v4` fork-gate plumbing + watchdog `upgradeTargetAppVersion = 4` | `cdba53c` | ✅ landed |
| `vstats:` EWMA/corr persistence (fork-aware encode, length-dispatch decode, `UpdateVerdictStats`, V1-V6) | `72121c4` | ✅ landed |
| `checkAndApplyQuorum` verdict-match crediting (idempotent) + `processEpoch` consumes EWMA/corr + off-chain mirror parity + AV1-AV5 | `5101be0` | ✅ landed |

Build: `go build ./...` clean. Tests: abci/store/poe/rest green; v8.2 replay parity (R1) and quorum fork (F1-F6) intact; AV1-AV5 + V1-V6 new. `ConsensusForkVersion` stays 1 — the migration is fork-gated and backward-compatible, no destructive reset (same as v8.0/v8.2). Not yet tagged (mirrors the "no main push until end-to-end working" policy).

### Activation note (operators)

On a chain with vote history (e.g. tii-sentinel), accuracy **resets to the 0.5 cold-start at the app-v4 activation epoch** and re-accrues as verdict-correctness EWMA — a one-time, intended reweighting toward the equal-accuracy baseline (pre-fork "accuracy" was accept-propensity, a different signal that we deliberately don't carry forward). Rubber-stampers (always-accept) no longer score high unless their accepts match committed verdicts.

### What's NOT in v8.3

- **Per-domain expertise (D).** Still 0.5. Needs `vstats_domain:<v>:<d>` + validator domain declaration. v8.4+.
- **Collusion/phi gating.** `internal/poe/collusion.go`'s `PhiTracker` exists, tested, unwired. Separate policy decision.
- **Windowed corroboration.** `CorrCount` is monotonic-lifetime; with `CorrMax=20` it saturates fast for active validators (becomes a tenure signal). A decaying/windowed variant is future tuning.

---

## What's NOT on this roadmap (and why)

- **Scaling consensus validators to N=7/10.** Blocked behind v6.0-dynamic-validator-governance, which itself isn't blocking any user. We'll build it when a cross-org federation deployment asks for it.
- **New PoE specialist axes (Provenance, Sensitivity, Relevance).** Research direction, ideally paired with paper work. v8+ at earliest.
- **State-sync as a peer-bootstrapping mechanism (not just local backup).** The v7.5 backup/restore design implements the ABCI methods minimally for local upgrade flow; publishing snapshots over p2p so a fresh node can catch up without full block replay is a v8+ follow-up.
- **Encryption-at-rest for snapshots in federated mode.** Today snapshots contain `vault.key` plaintext. Acceptable for personal mode. Federation-grade encryption (age + out-of-band passphrase) is a follow-up.

---

## Resolved decisions

The six open questions from the design docs are answered. Recorded here so implementation work can start without re-litigating.

1. **Binary version embedding — BOTH.** ldflags for the version *string* (`-X main.version=v7.5.0`), generated `upgrade_manifest.go` checked into the repo for the upgrade *matrix* (the mapping from binary version to `TargetAppVersion`, schema migrations to run, prior-version anchor needed for rollback). The manifest is the auditable record; ldflags is the lightweight identity. Generated by `make upgrade-manifest` at release tag time.

2. **State-sync ABCI scope — MINIMAL in v7.5.** Implement `ListSnapshots`/`OfferSnapshot`/`LoadSnapshotChunk`/`ApplySnapshotChunk` only enough to support *local* restore from `~/.sage/snapshots/`. The full peer-state-sync flow (publishing snapshots over p2p so a fresh validator can catch up without block replay) is a v8+ follow-up. Keeps v7.5 scope tight; doesn't block anyone today because nobody is bootstrapping fresh validators against a v7.5 chain yet.

3. **`HasAccessOrAncestor` walk-depth cap — 16 segments.** Real-world SAGE domains are ≤4 segments (e.g. `pipeline.failures.pwn_buffer_overflow`). Cap at 16 is generous head-room while still bounding the worst-case BadgerDB read storm at 16 lookups. Names exceeding 16 segments are almost certainly pathological/malicious; return false rather than walk further.

4. **`domain_reassign` quorum threshold — 3/4 supermajority.** Documented in `governance/quorum.go` as a per-proposal-type override. Rationale: at N=4 (today's default) 3/4 collapses to "all-but-one" since you need 3 of 4 anyway under 2/3, so single-validator/small-LAN deployments see no change. At N=7+ (future cross-org federation) it meaningfully raises the bar — a recovery-grade primitive should not be passable by a bare 2/3 coalition that could be hostile. Default gov stays 2/3; this is a typed exception.

5. **Snapshot encryption-at-rest — tied to vault state.** If the node's Synaptic Ledger vault is encrypted (`vaultExpected=true` per memory `f0b040fa`), snapshots are encrypted at rest using the same vault key envelope (Argon2id + AES-256-GCM, mirroring the CA-key manifest crypto from v6.8.0). If the vault is plaintext (personal-mode default for unencrypted users), snapshots are plaintext. No new operator decision — the snapshot encryption state inherits the user's already-expressed encryption posture.

6. **`sage-launcher` re-exec wiring — IMPLEMENTED in commit `0189a96`.** `cmd/sage-launcher/supervisor.go` runs the supervised binary in foreground, detects HALT via sentinel + non-zero exit, invokes the injected `Restorer` (wired to `internal/snapshot.Restore` in commit `800aff8`), then `syscall.Exec`s into the rollback binary so the launcher's PID becomes the new binary (PID continuity for outer supervisors like launchd / systemd).

7. **Binary version embedding — DEFERRED PARTIALLY.** Current implementation uses a package-level Go constant `upgradeTargetAppVersion uint64 = 1` in `cmd/sage-gui/upgrade_watchdog.go`. ldflags-injected version string is already wired via the existing `version` variable. The generated `upgrade_manifest.go` (the auditable upgrade matrix decided in Q1) is queued for the first real version bump — the steady state at `upgradeTargetAppVersion = 1` doesn't need it yet because the watchdog short-circuits before tx-build.

### Remaining open question (not blocking start)

- **Same-org / federation ancestor-walk consistency.** `HasAccessMultiOrg` has three access paths (direct-grant, same-org clearance, federation). Fix 1 ancestor-walk needs to apply to all three for consistent semantics. The work is mechanical (replace `GetDomainOwner(domain)` with an ancestor-walking variant on the same-org and federation paths). Flagged for the v8.0 implementation phase, not a design-level question.

---

## Dependencies and sequencing

```
v7.5 plan:
  [snapshot/scheduler + verify + diagnose]   <-- foundation
       |
       v
  [sage-launcher re-exec on halt]           <-- prereq for rollback
       |
       v
  [upgrade tx types + ABCI handlers]        <-- consensus path
       |
       v
  [upgrade_watchdog + binary version embed] <-- automation
       |
       v
  [migration tests, multi-validator drift]  <-- acceptance gate
       |
       v
  TAG v7.5.0
       |
       v
v8.0 plan:
  [v75ForkHeight plumbing in app state]     <-- entry point
       |
       v
  [HasAccessOrAncestor + fork-gated callers]
       |
       v
  [processAccessGrant auto-register branch]
       |
       v
  [TxTypeDomainReassign + gov-propose linkage]
       |
       v
  [pre/post-fork test matrix, replay parity CI]
       |
       v
  TAG v8.0.0
```

---

## What this enables once shipped

- Levelup (and every future SAGE user) gets in-place chain upgrades. The bootstrap-script workaround they're running in v7.1.1 becomes unnecessary.
- We can ship consensus-breaking improvements at a healthy cadence without losing accumulated memory on existing chains.
- The federation roadmap (dynamic validator gov, internet federation) unblocks because every component can land via the same fork-gated upgrade path.
- The Paper 5 / Paper 6 research direction on new PoE specialist axes has a clean shipping path — each new specialist is just another v8.x consensus upgrade.

---

## What still has to happen before v7.5.0 is tagged

The substrate is implemented and tested locally. Before public release:

1. **Multi-validator drift test.** Spin up a 4-validator local quorum, boot all four on `v7.5-dev` at staggered chain heights, assert all four activate at the same block. The design eliminates drift (activation height is chain-computed inside `FinalizeBlock`, not validator-chosen), but the assurance level for the public release should be "tested on a real cluster" not just "argued from the design."
2. **Generated `upgrade_manifest.go`.** The auditable upgrade matrix. Trivial to add when the first real version bump is on the table; deferred until then because `upgradeTargetAppVersion = 1` short-circuits the watchdog before tx-build.
3. **End-to-end "real chain panic → real rollback" smoke test.** Force the chain to panic, observe HALT sentinel produced, observe `cmd/sage-launcher` re-exec the prior anchor's bundled binary, observe chain resume on old binary at restored height. The pieces are tested individually; the integration is not yet rehearsed on real disks.
4. **`docs/ARCHITECTURE.md` updates.** New "Upgrade machinery" section describing the watchdog → propose → activate → rollback cycle for operators. Pointer to `~/.sage/snapshots/` retention policy.
5. **Release-notes commit.** "v7.5 — Migration substrate" framing per the established release-notes style (capabilities-forward, no autopsies).

None of the above is blocking the v8.0 access-control work — that can proceed against the v7.5-dev branch in parallel.
