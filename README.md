# (S)AGE — Sovereign Agent Governed Experience

**Persistent, consensus-validated memory infrastructure for AI agents.**

SAGE gives AI agents institutional memory that persists across conversations, goes through BFT consensus validation, carries confidence scores, and decays naturally over time. Not a flat file. Not a vector DB bolted onto a chat app. Infrastructure — built on the same consensus primitives as distributed ledgers.

The architecture is described in [Paper 1: Agent Memory Infrastructure](papers/Paper1%20-%20Agent%20Memory%20Infrastructure%20-%20Byzantine-Resilient%20Institutional%20Memory%20for%20Multi-Agent%20Systems.pdf).

> **Just want to install it?** [Download here](https://l33tdawg.github.io/sage/) — double-click, done. Works with any AI.

<a href="https://glama.ai/mcp/servers/l33tdawg/s-age">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/l33tdawg/s-age/badge" alt="(S)AGE MCP server" />
</a>

---

## Architecture

```
Agent (Claude, ChatGPT, DeepSeek, Gemini, etc.)
  │ MCP / REST
  ▼
sage-gui
  ├── ABCI App (validation, confidence, decay, Ed25519 sigs)
  ├── App Validators (sentinel, dedup, quality, consistency — BFT 3/4 quorum)
  ├── Governance Engine (on-chain validator proposals + voting)
  ├── CometBFT consensus (single-validator or multi-agent network)
  ├── SQLite + optional AES-256-GCM encryption
  ├── CEREBRUM Dashboard (SPA, real-time SSE)
  └── Network Agent Manager (add/remove agents, key rotation, LAN pairing)
```

Personal mode runs a real CometBFT node with 4 in-process application validators — every memory write goes through pre-validation, signed vote transactions, and BFT quorum before committing. Same consensus pipeline as multi-node deployments. Add more agents from the dashboard when you're ready.

Full deployment guide (multi-agent networks, RBAC, federation, monitoring): **[Architecture docs](docs/ARCHITECTURE.md)**

---

## CEREBRUM Dashboard

![CEREBRUM — Neural network memory visualization](docs/screen-brain.png)

`http://localhost:8080/ui/` — force-directed neural graph, domain filtering, semantic search, real-time updates via SSE.

### Network Management

![Network — Multi-agent management](docs/screen-network.png)

Add agents, configure domain-level read/write permissions, manage clearance levels, rotate keys, download bundles — all from the dashboard.

### Settings

| Overview | Security | Configuration | Update |
|:---:|:---:|:---:|:---:|
| ![Overview](docs/screen-overview.png) | ![Security](docs/screen-security.png) | ![Config](docs/screen-config.png) | ![Update](docs/screen-update.png) |
| Chain health, peers, system status | Synaptic Ledger encryption, export | Boot instructions, cleanup, tooltips | One-click updates from dashboard |

---

## What's New in v9.2.3

**Operator path to activate the app-v7…app-v10 forks.** v9.2.3 is a non-fork patch: the committed app version stays 10 and nothing here touches consensus, the AppHash, or block replay. It closes a gap reported in [#32](https://github.com/l33tdawg/sage/issues/32) by [@ihubanov](https://github.com/ihubanov). The upgrade machinery's voting and processing halves were complete — `processUpgradePropose` activates a plan deterministically and validators auto-vote ACCEPT — but nothing in the tree could *submit* a plan for the governance-gated forks. The only `UpgradePropose` constructor was the boot watchdog, frozen at the deployment-safe default, so on a long-lived chain app-v7 (content-validation), app-v8 (quorum-gated upgrades), app-v9 (nonce/replay) and app-v10 (corroboration integrity) were unreachable past app-v6.

- **New `sage-gui upgrade` command.** `upgrade status` shows the chain's app version and the next fork; `upgrade propose --target N` submits a signed, admin-gated `UpgradePropose` that routes through the existing 2/3 governance quorum. Off-consensus: the tx is built and signed client-side, and `processUpgradePropose` is unchanged.
- **Strictly sequential activation, by design.** Targets must be `current + 1`. The app-v7…app-v10 fork gates are independent, but `currentAppVersion()` reports the highest active one and the on-chain regression guard rejects anything at or below current — so a jump (e.g. app-v6 → app-v10) would activate only the top fork and permanently strand the ones it skipped. The command refuses jumps and points you at the correct next step.
- **Honest result reporting.** The proposal is committed and the block-execution result is read back, so an already-pending plan or a non-admin proposer key surfaces as a failure rather than a false success.

SDK 9.2.3.

## Older releases

<details>
<summary>v9.2.2 — snapshot retention/pruning (KeepLast wired + boot staging sweep + snapshot CLI)</summary>

**Snapshot retention — the node now bounds its own disk.** v9.2.2 is a non-fork patch: the committed app version stays 10 and nothing here touches consensus or the AppHash. SAGE has taken periodic chain snapshots since v7.5 (every 10k blocks / 6h, plus before every upgrade), but it never reaped them — the `KeepLast` retention policy and the crash-staging sweep both existed yet were wired into nothing, so on a long-lived node snapshots accumulated without bound.

- **Snapshots are now pruned automatically.** After every successful snapshot the scheduler keeps the N newest (default 5) and removes the rest, and at boot it clears any backlog older builds left behind. One anchor per distinct binary version is always retained, so a downgrade-to-rollback stays possible no matter how aggressive the retention count. Tune it with `SAGE_SNAPSHOT_KEEP` (≥1). This is purely off-chain disk housekeeping — it touches only `DataDir/snapshots/`, never consensus state.
- **Crash-staging dirs are reaped at boot.** A snapshot that crashed mid-write left a `.staging-*` directory behind, and nothing ever removed them. The node now sweeps them on startup.
- **New `sage-gui snapshot` command.** `snapshot list` shows the on-disk inventory; `snapshot prune [--keep N]` runs the same retention on demand, for a one-off cleanup without restarting the node.

SDK 9.2.2.

</details>

<details>
<summary>v9.2.1 — FTS5 backfill startup hotfix + two-sided fork-branch metrics + on-chain nonce seed</summary>

**Startup and liveness hardening — no consensus change.** v9.2.1 is a non-fork patch on top of v9.2.0's `app-v10`: the committed app version stays 10 and all three fixes live off the consensus path, so every historical block replays byte-identically. It clears a startup wedge on large chains, completes the fork-activation metrics, and lifts the last cross-restart limits on the nonce allocator.

- **FTS5 backfill no longer wedges node startup on large chains.** The full-text-search backfill ran a full anti-join against the `memories_fts` table on *every* boot, synchronously, before the node produced its first block — and because FTS5 columns aren't B-tree indexed, that forced SQLite to materialize and sort the entire index (minutes of one-core-pegged CPU on an 830k-row chain) even when there was nothing to insert. Boot wedged: health dead, no consensus, until the sort finished. v9.2.1 adds a cheap count gate that skips the anti-join entirely when the index already covers every active memory (instant restart), and runs a genuinely-needed build asynchronously in the background while the node boots and produces blocks. It touches only the off-chain SQLite mirror — no consensus or AppHash involvement — and the bug predates v9.2.0.
- **Fork-activation metrics now record both sides of the split.** The `app-v9`/`app-v10` branch counters were only ever incremented inside the post-fork path, so the activation dashboard's "pre" label stayed empty and couldn't plot the pre/post ratio. v9.2.1 records the actual fork boolean on every relevant op. The "post" counts are byte-identical to before; only the previously-dead "pre" label now populates. Metrics-only — never part of the AppHash.
- **The nonce allocator seeds from the chain's committed nonce.** The `app-v9` consensus gate rejects any tx whose nonce is at or below the signer's highest committed nonce. The in-process `MonotonicNonce` allocator previously trusted the wall clock to exceed that on a fresh or post-restart process, with documented liveness-only limits. It now seeds each key's floor — once, on first use — from the highest nonce already committed on-chain (via `max()`, so it can only raise the floor, never regress a value an allocation already set). This lifts the cross-restart and cross-process liveness limits with no consensus rule, no fork, and no AppHash impact.

SDK 9.2.1.

</details>

<details>
<summary>v9.2.0 — app-v10 corroboration integrity guard + on-chain author field</summary>

**Corroboration integrity and an on-chain author field.** v9.2.0 activates a new independent `app-v10` consensus fork that makes corroboration a trustworthy multi-agent signal, and exposes corroboration on the MCP surface. Like every SAGE fork it is replay-safe by construction: no existing chain has activated `app-v10`, so every historical block replays byte-identically, and the new rules apply only after an operator governance-activates the fork.

- **Corroboration is now guarded in consensus.** Corroboration is the signal that moves a memory from attributed toward consensus, so it must come from independent agents. Post-`app-v10`, `FinalizeBlock` rejects a self-corroboration (an agent backing its own memory), a duplicate corroboration (the same agent backing a memory twice), and a corroboration of a memory that was never submitted. Previously none of these was checked, so a single agent could inflate a memory's corroboration weight. The checks read only on-chain state, so every validator reaches the same verdict.
- **The memory author is now an on-chain field.** A memory's submitting agent was recorded only in the off-chain SQL mirror; it is surfaced through the REST API and the Python SDK, but it was not part of consensus state. Post-`app-v10`, the author is written on-chain at submit time, immutably (the first writer wins, so a re-submission of the same id by a different agent cannot displace it). This is the authoritative source the corroboration guard checks. The guard is forward-looking: memories submitted before the fork have no on-chain author, so the self-corroboration check applies only to memories created after activation.
- **Corroborate is exposed on the MCP surface.** `sage_corroborate` joins the memory-lifecycle tools (remember, recall, forget, list), so an MCP-only client can reinforce a memory it has independently verified without dropping to signed REST. It wraps the existing endpoint and inherits the app-v10 guarantees. Thanks to [@ihubanov](https://github.com/ihubanov), who proposed and prototyped it (issue #31).
- **Independent, halt-safe fork.** `app-v10` ranks highest in the committed app version and subsumes the lower forks' rules, so a chain cannot activate `app-v10` and silently lose `app-v8`/`app-v9`'s guarantees. The upgrade watchdog stays targeted at `app-v6`, so `app-v10` never auto-fires; it activates only via an explicit governance upgrade plan.

SDK 9.2.0.

</details>

<details>
<summary>v9.1.0 — consensus-path nonce/replay enforcement + defense-in-depth hardening</summary>

**Consensus-path nonce/replay enforcement and defense-in-depth hardening.** v9.1.0 activates a new independent `app-v9` consensus fork that closes the replay boundary v9.0.0 flagged and tightens two more authorization seams. Like every SAGE fork it is replay-safe by construction: no existing chain has activated `app-v9`, so every historical block replays byte-identically, and the new rules apply only after an operator governance-activates the fork.

- **Nonce/replay is now enforced in the consensus path.** v9.0.0 verified tx signatures in `FinalizeBlock` but still checked nonces only at `CheckTx` (advisory). Post-`app-v9`, `FinalizeBlock` rejects a tx whose nonce was already consumed (and rejects the nonce-0 sentinel), so a Byzantine proposer can no longer replay a victim's previously-valid signed tx into a block. Because strict-monotonic nonces are now consensus-enforced, every in-process transaction producer (validators, REST/web handlers, the upgrade watchdog) moved onto a process-global, strictly-increasing nonce allocator keyed by signing identity, replacing wall-clock timestamps that could collide.
- **Admin role can no longer be self-granted over the wire.** Pre-`app-v9`, `agent_register` took the role straight from the payload, so any key could register itself as `admin`. Post-fork a wire `role="admin"` is silently downgraded to `member` (the registration still succeeds). The real upgrade-authority gate remains the 2/3 quorum plus the v9.0.0 consensus signature verification; this just removes the cheap path to a privileged role. Operator admins are unaffected: existing admins are grandfathered, and the operator-blessed bootstrap path is untouched.
- **Validators auto-vote on upgrades, gated on readiness.** Under `app-v8` an upgrade needs an explicit 2/3 governance vote, but nothing cast it automatically, so a proposal could expire unvoted. The in-process validators now auto-vote accept on an active upgrade proposal, but only if the running binary actually supports the target app version. An upgrade to a version the binary can't execute never draws a quorum, which neutralizes a halt footgun (committing an app version the binary doesn't understand) at the liveness layer, with no new consensus rule.
- **Independent, halt-safe fork.** `app-v9` ranks highest in the committed app version, and a higher independent fork now subsumes the lower forks' rules, so a chain cannot activate `app-v9` and silently lose `app-v8`'s guarantees. The upgrade watchdog stays targeted at `app-v6`, so `app-v9` never auto-fires; it activates only via an explicit governance upgrade plan.

SDK 9.1.0.

</details>

<details>
<summary>v9.0.0 — governance-gated upgrades + consensus-path signature verification</summary>

**Governance-gated upgrades and consensus-path signature verification.** v9.0.0 activates a new independent `app-v8` consensus fork that hardens how the chain authorizes high-value actions. It is replay-safe by construction: no existing chain has activated `app-v8`, so every historical block replays byte-identically, and the new rules apply only after an operator governance-activates the fork.

- **Upgrades now require a 2/3 governance quorum.** Pre-`app-v8`, a single Ed25519-verified `UpgradePropose` self-activated a chain-wide app-version bump, so any well-formed key could schedule a fork. Post-`app-v8`, that tx no longer self-activates: it must come from an admin agent and only creates a governance `OpUpgrade` proposal, reusing the existing governance engine. The upgrade plan is persisted and scheduled only after a 2/3 validator-power supermajority accepts. This is the real authority gate the v8.9.0 `UpgradePropose` doc note flagged as future work.
- **Transaction signatures are now verified in the consensus path.** `tx.VerifyTx` (the outer Ed25519 check) runs inside `FinalizeBlock`, not only at mempool admission (`CheckTx`). `CheckTx` is advisory: a Byzantine block proposer can include txs that never passed an honest node's mempool. Without this, a forged `UpgradePropose` or `GovVote` bearing a victim's public key (signed by the attacker) would execute, letting one proposer fabricate the very 2/3 quorum the upgrade gate relies on. The gate covers every tx type, so all governance (validator-set changes, memory votes, access control) is now authenticated in consensus, not just at the mempool.
- **Independent, halt-safe fork.** `app-v8` is decoupled from the PoE fork ladder (like `app-v7`) and ranks highest in the committed app version. The upgrade watchdog stays targeted at `app-v6`, so `app-v8` never auto-fires; it activates only via an explicit governance upgrade plan.

Also: `MemoryRecord` in the Python SDK now reads back the `provider` provenance tag the server emits (thanks to [@ihubanov](https://github.com/ihubanov), #30). SDK 9.0.0.

</details>

<details>
<summary>v8.9.0 — fail-safe content-validation routing + consensus-pure enforcement</summary>

**Hardened the Layer-2 content-validation seam so the gate cannot fail open, and made its enforcement a pure function of consensus state.** Three fixes to the generic, deployment-agnostic content gate from v8.7.0/v8.8.0. All are AppHash-neutral for existing chains: the gate only runs once a chain activates the `app-v7` fork, and a stock build (no validators compiled in) stays byte-identical to v8.8.1.

- **Fail-safe routing (was fail-open).** The router that maps a memory body to its `(domain, outcome_class)` validator used to return the empty class on any JSON error, so a malformed sibling field (a float or string `schema_version`), an array-wrapped body, or a cross-class value routed to an unregistered key and committed unvalidated. `parseOutcomeClass` now reads `outcome_class` independently of sibling-field types and unwraps a single-element array, so a malformed neighbor can no longer null the route.
- **Closed-domain registration.** New `RegisterClosedDomain(domain)` on the validator registry: once a domain has at least one registered validator, a submission whose `outcome_class` has no validator is rejected (`Code 18`) instead of passing through. Open domains keep the backward-compatible pass-through, so existing wiring is unaffected. With the router fix, all three bypass vectors above now reject rather than commit unvalidated.
- **Enforcement is consensus state, not a per-node flag.** Removed the runtime `SetContentValidationEnabled` toggle (breaking: callers should drop it). The gate now fires purely on `postAppV7Fork(height) && contentValidators != nil`, so two nodes on one binary cannot disagree on whether the gate is live. A node on an `app-v7` chain with no validators compiled in logs a startup warning that it will not enforce (a mixed fleet would diverge) but stays bootable.
- **Doc correctness: `UpgradePropose` is a single-signer authority op, not 2/3-quorum-gated.** The type comment claimed a quorum gate the code never implemented; it now documents the real model so operators protect the proposer key. A true authority gate is tracked for a future `app-v8`.

SDK 8.9.0.

</details>

<details>
<summary>v8.8.1 — /v1/embed reports the actual embedding model</summary>

**`POST /v1/embed` now reports the model that actually produced the embedding.** The handler previously wrote `model: "nomic-embed-text"` into every response regardless of the configured provider, so a node running the `openai-compatible` embedder mislabeled its vectors (e.g. `Alibaba-NLP/gte-Qwen2-1.5B-instruct` was reported as `nomic-embed-text`). `handleEmbed` now feature-detects the optional `embedding.Modeler` interface and reports the provider's real model, mirroring how the sibling `/v1/embed/info` already resolves `provider`. It falls back to the legacy default only for providers that don't expose a model (the hash provider), so that path is unchanged. Read-path REST only: no tx, no consensus, no AppHash contribution. SDK 8.8.1.

Thanks to [@ihubanov](https://github.com/ihubanov) for the fix (#29).

</details>

<details>
<summary>v8.8 — governance-activatable app-v7 content-validation + halt-safety floor</summary>

**Governance-activatable `app-v7` content-validation + halt-safety floor.** v8.8.0 makes the v8.7.0 content-validation gate safely switchable on. `app-v7` is now a fully wired, **governance-activatable** consensus fork, and SAGE exposes the registration API a deployment uses to plug in *its own* content validators. A node stays **byte-identical to v8.7.0** unless an operator both registers validators *and* activates the `app-v7` upgrade — existing chains are AppHash-neutral, and consensus core ships **zero deployment-specific schemas**.

- **Halt-safe `app-v7` activation.** `app-v7` is an independent gate, decoupled from the PoE fork ladder and the upgrade watchdog (which stays targeted at app-v6, so `app-v7` never auto-fires). The activation commit now carries an **unconditional version-non-regression floor**: an out-of-order activation can never commit a backward `version.app`, closing the `7→6` handshake-regression halt class (the v8.4.1/8.4.2 bug class). Activates only via an explicit governance upgrade plan.
- **Deployment registration API.** A deployment registers its own validators against `RegisterContentValidator(domain, outcome_class, …)` and arms them with `SetContentValidators` / `SetContentValidationEnabled` at boot; unset ⇒ nil registry ⇒ the gate stays dormant. `RoleResolver` exposes a deterministic, read-only on-chain role lookup so a deployment's validator can enforce signer authority from chain state rather than any self-asserted field. SAGE provides the mechanism; the schemas, allowlists, and role mappings live in — and are owned by — the deployment.
- **Determinism contract.** The gate runs only inside `FinalizeBlock`, before any state write, returning a deterministic `Code 18` reject identical on every replica; a deployment's validators must likewise be pure functions of the record (no clock / network / unsorted-map iteration) to preserve AppHash parity.

The new machinery is dormant-by-default and AppHash-neutral; the only consensus-touching change — the version-regression floor — is a strict safety improvement that never lowers a committed app version. SDK 8.8.0.

</details>

<details>
<summary>v8.7 — dormant Layer-2 content-validator plumbing + MCP write-path re-heal</summary>

**Layer-2 content-validator plumbing (dormant) + a memory-write resilience fix.** v8.7.0 lands the generic, deployment-agnostic scaffolding for a consensus-time content-aware schema gate, shipped **dormant** — no validators are registered, the gate is disabled by default, and its activation fork (`app-v7`) is not triggered, so the binary's on-chain behavior and AppHash are **byte-identical to v8.6.0** on every existing chain (full `replay_v8_*`/upgrade/quorum suites green; `golangci-lint` v2.12.2 = 0 issues).

- **Generic content-validator registry (`internal/contentvalidator`).** A deployment registers validators keyed by `(domain, outcome_class)`; a registered validator that rejects a record turns the submit into a deterministic on-chain reject (`Code 18`), evaluated identically on every replica *before any state write*. With nothing registered it is pure pass-through — the path every current deployment takes. It carries zero deployment-specific schemas; this is the plumbing only.
- **Triple-gated dormancy.** The gate fires only when content-validation is explicitly enabled **and** the `app-v7` fork is active **and** a registry is installed — all three default off. The fork gate is strict-`>` and deliberately independent of the v8.x PoE fork chain, so it cannot perturb PoE activation heights.
- **MCP write-path self-heal.** When a node is restarted under a live MCP session (e.g. an in-place upgrade), signed memory writes the node rejects at the identity/access layer now auto-re-handshake (re-register + fresh connections + a short bounded retry) instead of surfacing a bare `Broadcast error: access denied` until a manual reconnect. First-attempt writes are untouched; a genuine denial still fails fast with an actionable hint. Covers every store path (`sage_turn` / `sage_remember` / `sage_reflect` / `sage_task`).

The Layer-2 slice is dormant-by-default and AppHash-neutral; the MCP fix is operational-only (client write path) with zero consensus surface. SDK 8.7.0.

</details>

<details>
<summary>v8.6 — PoE observability + dead-code cleanup + cross-node determinism harness</summary>

**PoE observability + cleanup.** Three of the four Phase-2 quorum-weight factors have been real on-chain since v8.2–v8.4 but were invisible to clients and operators; v8.6.0 surfaces them and removes the last dead scaffold an audit of the Phase-2 work flagged. **No consensus-rule change** — the binary's on-chain behavior and AppHash are byte-identical to v8.5.1 (verified by the full `replay_v8_*` suite).

- **PoE factors are now readable.** `GET /v1/agent/me` returns `corr_count` (lifetime corroboration), per-domain `domain_expertise` (the β factor, from `vstats_domain:`), and an authoritative `accuracy` read straight from the on-chain `vstats:` EWMA rather than the off-chain mirror. The Python SDK `AgentProfile` model gains the matching optional fields — additive, so old-client ↔ new-server and new-client ↔ old-server both still round-trip.
- **The `sage_poe_weight` Prometheus gauge is fed.** It was declared but never `Set`; `processEpoch` now publishes each validator's normalized weight once per epoch (reset-then-repopulate, so a governance-removed validator doesn't leave a stale series). Process-local — no BadgerDB write, outside the AppHash path.
- **Dead Phase-1 scaffold removed.** The unused domain-vector machinery (`DomainRegistry`/`ExpertiseProfile`/`CosineSimilarity`/`ValidatorState`), the write-only `poeEngine` field, and four zero-caller `IsPostV8_x` accessors are deleted — proven AppHash-neutral (dead/off-chain only) by the replay suite. The always-empty off-chain `ExpertiseVec` column is intentionally retained to avoid a no-benefit SQL migration.
- **Cross-node determinism harness.** A new `make determinism` target (`test/integration/apphash_determinism_test.go`) stands up an isolated 4-validator devnet and asserts every node's committed AppHash is byte-identical at matched heights across an epoch boundary and a fork activation — turning the single-process determinism guarantee into a repeatable cross-node observation.

The dead-code removal was verified replay-safe — it touches no AppHash-affecting bytes — before landing. SDK 8.6.0.

</details>

<details>
<summary>v8.5 — PoE Phase 2 complete + app-v6 upgrade-machinery hardening</summary>

**Proof-of-Experience Phase 2 is complete.** Across the v8 line, all four factors of a validator's quorum weight are now real and consensus-active: **accuracy** (verdict-correctness EWMA, `app-v4`), **corroboration** (lifetime verdict-match count, `app-v4`), **recency**, and **domain expertise** (domain-conditional weight, `app-v5`). A 2/3 quorum is no longer a 2/3 *majority* — it's a 2/3 *weighted* vote where weight is a validator's demonstrated track record, in context.

**v8.5.0 hardens the upgrade machinery itself** behind a new fork (`app-v6`) so the consensus layer self-defends its own protocol activations. Three guards, each fork-gated (pre-fork blocks replay byte-identical):

- **Canonical-name guard.** `processUpgradePropose` now rejects any plan whose `Name` isn't the canonical `app-v<N>` for its `TargetAppVersion`. The v8.x fork gates activate by matching `plan.Name` against `app-v<N>`, so a plan named anything else bumps the CometBFT app version while leaving the gate false (the bug class fixed in v8.4.1/8.4.2). The consensus layer now refuses such a plan from *any* proposer, not just the watchdog.
- **Version-regression guard.** Rejects a propose whose `TargetAppVersion <= currentAppVersion()` — no silent regression or no-op upgrade. CometBFT provides no such check; the propose path is now the deterministic gate.
- **Revert safety.** A live in-band downgrade is replay-unsafe by construction (it clears a fork gate to a *past* height → AppHash divergence → halt), so `processUpgradeRevert` now explicitly rejects post-fork (Code 90) instead of accepting a silent no-op. The only correct downgrade is a forward upgrade + off-chain snapshot rewind.

Reviewed for correctness — 0 blockers, replay-safe, pre-fork byte-identical. SDK 8.5.0.

**v8.5.1 (patch) — PoE Phase 2 audit follow-ups.** Test-net and doc hardening from a full audit of the Phase 2 PoE work (v8.2→v8.5); **no consensus-rule or behavior change** (byte-identical replay, no new keyspace). Adds the previously-missing `replay_v8_3` byte-parity test — v8.3 is the one fork that *mutates the value bytes of an already-hashed key* (the `vstats:` 24→56-byte growth) rather than only adding key prefixes, so it was the highest-risk seam left without a dedicated AppHash test. Adds a regression guard proving the legacy map-iteration weight sum is genuinely order-sensitive (so `NormalizeWeightsDeterministic` is load-bearing, not a rename), closing the half-covered v8.4 consensus-split fix. Doc corrections: the domain-weight shared-domain fallback set is `{general, self, meta}` **plus the `sage-*` prefix family** (the docs had understated it as `{general, self}`); INDEX.md no longer contradicts ARCHITECTURE on PoE-weighted quorum; and the consensus-decay reference's `app.go` file:line anchors were re-checked against the v8.5 tree. SDK 8.5.1.

</details>

<details>
<summary>v8.4 — real Domain factor + the v8.4.1/8.4.2 upgrade-activation fix</summary>

**v8.4.2 (patch).** Two halves of one upgrade-activation fix:
- **Watchdog plan naming.** The v7.5 watchdog named the upgrade plan after the binary version (e.g. `8.2.1`) instead of the canonical `app-v<N>` form the activation path keys on. On a real chain this bumped the CometBFT app version on activation while leaving every `postV8_*Fork` gate false — silently disabling the v8.x PoE consensus rules the upgrade was meant to turn on (confirmed in production logs: plans activated as `name=8.0.0`/`name=8.2.1`). The plan name is now derived from a single source of truth (`tx.CanonicalUpgradeName`), with guard tests coupling both the activation constants *and* the reported app version to it.
- **`Info()` app version.** `Info()` previously hardcoded `AppVersion: 1`, so a node restarting on a post-fork chain reported a version below the one its consensus params had committed. It now derives the version from the activated fork gates (`currentAppVersion()`), matching the committed param. (Thanks @ihubanov.)

Both are byte-identical replay (no consensus-rule change). **Heal note:** existing v8.x chains are *not* state-reset on a same-fork update (the reset is gated on the consensus fork version, unchanged across v8.x). They heal forward instead: on the fixed binary the watchdog re-proposes `app-v<N>` (now correctly named), which activates ~200 blocks later and flips all gates from that height onward via monotonic reconcile — past blocks stay pre-fork (no retroactive replay change), so there is no AppHash divergence. A brief, non-fatal under-report window exists between restart and that activation.

Real Domain factor — the **last Phase-1 stub closed**. After v8.3 made accuracy and corroboration real, the Domain term (D, 30% of the PoE weight) was still a flat `0.5` constant, so a validator's *subject-matter expertise* counted for nothing. v8.4 makes it real behind a single fork (`app-v5`): a validator's vote on a memory in domain `D` is now weighted by its demonstrated verdict-correctness **in `D`**. Pre-fork blocks (and any v8.3.x chain) replay byte-identical to v8.3.0.

- **Domain-conditional quorum weight.** When a non-shared-domain memory is voted on, `checkAndApplyQuorum` weights each validator by `ComputeWeight(globalAccuracy, domainAccuracy(v,D), recency, corroboration)` — the domain term read live from a per-domain verdict-correctness EWMA, the global terms recomputed live from `vstats:<v>`. A proven `pwn_heap` expert out-weighs a generalist on `pwn_heap` memories, but not on `crypto` ones. Experts genuinely carry more weight (raw `ComputeWeight`, not the epoch-normalized scalar, since the RepCap collapses small validator sets to equal and would erase the signal).
- **Keyed per-domain stats — no positional-vector determinism trap.** Per-domain expertise lives at `vstats_domain:<validatorID>:<domain>`, reusing v8.3's exact 24/56-byte codec, rather than a positional `[]float64` indexed by a growing domain registry (which would make the tag→index ordering a consensus-split hazard). The memory's domain is recorded at submit in `memdomain:<id>` (the on-chain `memory:<id>` record only stores contentHash+status). Shared catch-alls (`general`, `self`, `meta`, and any `sage-*`-prefixed domain) and unknown/legacy memories fall back to the v8.2 scalar weight.
- **Consensus-drift hardening (from an adversarial audit of v8.3+v8.4).** Folded into the same fork: epoch-weight normalization now sums in **sorted-key order** (`NormalizeWeightsDeterministic`) — the legacy map-iteration sum was non-associative and could split the AppHash across replicas with ≥3 distinct-magnitude weights (a latent issue since v8.2, masked by equal-weight devnets). Also: re-submitting a memoryID that already reached a terminal verdict is now rejected (it previously rewound to `proposed` and let a fresh vote double-credit the verdict EWMA — a reputation-gaming vector); verdict crediting is gated on the on-chain status write succeeding; and the PoE fork gates are reconciled monotonic so a version jump can't activate a higher fork while a lower one stays off. Every fix is fork-gated or no-ops on existing chains (byte-identical replay).
- **Test coverage.** Store DS1-DS4 (per-domain codec/round-trip/independence/atomicity, `memdomain` get/set). Quorum DQ1-DQ7 (expert dissent flips a verdict; same votes → opposite outcome by domain; shared/unknown fall back; per-domain crediting + replay idempotency). An end-to-end test drives a real `app-v5` activation, asserting `memdomain:`/`vstats_domain:` appear only post-fork. Plus determinism (200× bit-identical `NormalizeWeightsDeterministic`), re-submit-guard (both fork sides), and monotonic-reconcile regressions. Full suite green; lint clean.

</details>

<details>
<summary>v8.3 — real PoE signals (verdict-correctness EWMA + corroboration)</summary>

- **v8.3 — accuracy & corroboration made real.** v8.2 made quorum *consult* PoE weights, but accuracy was still a cold-start accept-ratio blend (rewarding voting "accept", not being *right*) and corroboration a hardcoded default. v8.3 closed both behind one fork (`app-v4`): `accuracy` became the verdict-correctness EWMA (`poe.EWMATracker`, η=0.9 — did the vote match the final committed/deprecated verdict), `corr_score` the lifetime verdict-match `CorrCount`. Both credited once on the first proposed→terminal transition (prior status captured before any `SetMemoryHash`), persisted in `vstats:<id>` records grown 24→56 bytes with a lazy per-validator migration + length-dispatch decode. Off-chain `/v1/agent` accuracy re-sourced from the same EWMA. Pre-fork byte-identical to v8.2.1; an end-to-end test held pre-fork 0.65/0-corr vs post-fork 0.70/0.53 for consensus-aligned validators and 0.30 for a dissenter. SDK 8.3.0.

</details>

<details>
<summary>v8.2 — PoE-weighted quorum activation</summary>

- **v8.2 — PoE weights drive quorum.** The PoE engine had computed per-validator engagement scores every epoch since v6.x, but `checkAndApplyQuorum` ignored them and used a hardcoded `weights[v.ID] = 1.0` — quorum was a 2/3 *majority*, not a 2/3 *weighted vote*. v8.2 closed that with a single fork-gated swap (`app-v3`): post-fork quorum consults `v.PoEWeight` via `app.postV8_2Fork(height)`; the normalized weight set is persisted on-chain every epoch (`poew:current` + `poew:<id>`, pruned on governance set changes, rehydrated on restart); `poeWeightOrFallback` returns `1/N` for pre-first-epoch / mid-epoch-add / missing-entry cases, keeping the fallback in `NormalizeWeights`' numeric range without moving the ratio-only 2/3 threshold. Bundled CometBFT v0.38.15 → v0.38.23 (GHSA-hrhf-2vcr-ghch + blocksync/nil-vote/ABCI-socket hardening). Pre-fork byte-identical to v8.1.2; 16 new tests + a 4-validator devnet held byte-identical AppHash for 160+ blocks across two epoch boundaries.

</details>

<details>
<summary>Capability milestones across v3–v7 (full per-patch detail on the <a href="https://github.com/l33tdawg/sage/releases">Releases page</a>)</summary>

- **v8.1 — Governance + ancestor cleanup + O(1) per-block AppHash.** Three follow-up fixes after v8.0 surfaced edges: postgres quorum register-agent consensus halt (8.0.1), postgres quorum governance mirror (8.1.0), per-record clearance arg on SDK `propose()` (8.1.1), governance/ancestor walk cleanups + `ComputeAppHash` switched from `O(state)` per-block alloc to streaming SHA-256 over a lex-sorted iterator (8.1.2). Single-machine personal mode no longer churns GC pressure linearly with chain height.
- **v8.0 — Access-control consensus cleanup.** Three real bugs from v7.1 fixed behind a single fork (`app-v2`): subdomain grants now cascade via `HasAccessOrAncestor`, granting on an unowned domain auto-claims it, and `TxTypeDomainReassign` recovers lost-owner domains via a 3/4-supermajority gov proposal. Pre-fork byte-identical to v7.1.1. Python SDK 8.0.0 adds `submit_domain_reassign` + a high-level `reassign_domain` helper.
- **v7.7 — Agent profile fill-in.** `GET /v1/agent/me` now returns the full profile the OpenAPI schema promised — `display_name`, `domains`, `accuracy`, `on_chain_height` — so SDK consumers don't round-trip to `/v1/agent/{id}` plus the validator-score endpoint just to render a profile card.
- **v7.6 — Direct-write hooks for Claude Code and Codex.** `sage-gui hook session-start | session-end` signs REST calls to the local SAGE node directly; `mcp install` and `codex install` ship the unified 5-script lifecycle set; selfHeal migrates legacy installs and auto-installs hooks on MCP boot for pre-v7.6 projects (v7.6.2).
- **v7.5 — Migration substrate.** Hands-off in-place chain upgrades — scheduled snapshots with verify-by-restore, upgrade tx types with chain-computed activation height, auto-proposal watchdog, HALT sentinel + supervised rollback. v7.5.0 itself ships zero consensus-rule changes; it's the plumbing every later release rides on.
- **v7.1 — Recall quality + second benchmark.** Optional cross-encoder reranking and query expansion on `/v1/memory/hybrid`, LoCoMo benchmark (R@5=0.6394 stock), SAGE adapter shipped upstream to mem0's open-source evaluator. v7.1.1 closed the silent ghost-tx surface on RBAC/governance writes.
- **v7.0 — Hybrid recall + ambient capture.** BM25 + vector fused via Reciprocal Rank Fusion on a new `/v1/memory/hybrid` endpoint, direct-write lifecycle hooks for Claude Code, branch-aware memory tagging, LongMemEval-S benchmark at R@5=0.9053.
- **v6.8 — Hardening pass.** OAuth Dynamic Client Registration + persistent client metadata, mandatory `state` + HMAC-signed CSRF on `/oauth/authorize`, strict same-origin on CEREBRUM wizard endpoints, locked-down subprocess test seams. Admin-bootstrap escape hatch (6.8.5), cross-agent visibility hotfix (6.8.4), Windows wizard parity (6.8.1).
- **v6.7 — ChatGPT MCP connector.** OAuth 2.0 + PKCE wrapper, RFC 8414/7591/9728 discovery and Dynamic Client Registration, in-dashboard ChatGPT setup wizard (6.7.3, Cloudflare zone dropdown 6.7.4), HTTPS-capable HTTP MCP transport (`/v1/mcp/sse` + `/v1/mcp/streamable` on `:8443`) with bearer tokens.
- **v6.6 — Tags + multi-org + RBAC fixes.** Tags first-class on `/v1/memory/submit` and `/query`/`/search` filtering. Multi-org membership reverse index so agents in N orgs no longer silently lose access to N-1 of them. `PUT /v1/agent/{id}/permission` no longer silent-no-ops for non-admin self/org-admin callers. SQLITE_BUSY silent-drop fix at source (WAL pragma + writeMu-guarded BeginTx). Encrypted CA key in quorum manifest (Argon2id + AES-256-GCM envelope).
- **v6.5 — TLS everywhere.** Per-quorum ECDSA P-256 CA, dual-listener REST API (TLS `:8443` + local HTTP `:8080`), Python SDK `ca_cert` parameter. Stuck-proposed deprecation when quorum unreachable. RBAC ownership-theft fix + real broadcast errors surfaced to clients.
- **v6.0 — Dynamic validator governance.** Add/remove/repower validators without stopping the chain via on-chain governance proposals (2/3 BFT quorum). New `internal/governance/` package, in-dashboard Governance section.
- **v5.x — Consensus-first writes + FTS5.** All submissions go through BFT consensus before they surface in queries. 4-validator Docker cluster with fault injection in CI. FTS5 keyword search fallback. Nonce-based replay protection. Python SDK.
- **v4.x — App validators + RBAC + Synaptic Ledger.** Sentinel / Dedup / Quality / Consistency validators with 3/4 quorum. Agent isolation, domain-level permissions, clearance levels, multi-org federation. AES-256-GCM encryption with Argon2id key derivation.
- **v3.x — Multi-agent networks.** Add agents from dashboard, LAN pairing, key rotation, redeployment orchestrator. On-chain agent identity via CometBFT consensus. CEREBRUM dashboard.

</details>

---

## Research

| Paper | Key Result |
|-------|------------|
| [Agent Memory Infrastructure](papers/Paper1%20-%20Agent%20Memory%20Infrastructure%20-%20Byzantine-Resilient%20Institutional%20Memory%20for%20Multi-Agent%20Systems.pdf) | BFT consensus architecture for agent memory |
| [Consensus-Validated Memory](papers/Paper2%20-%20Consensus-Validated%20Memory%20Improves%20Agent%20Performance%20on%20Complex%20Tasks.pdf) | 50-vs-50 study: memory agents outperform memoryless |
| [Institutional Memory](papers/Paper3%20-%20Institutional%20Memory%20as%20Organizational%20Knowledge%20-%20AI%20Agents%20That%20Learn%20Their%20Jobs%20from%20Experience%20Not%20Instructions.pdf) | Agents learn from experience, not instructions |
| [Longitudinal Learning](papers/Paper4%20-%20Longitudinal%20Learning%20in%20Governed%20Multi-Agent%20Systems%20-%20How%20Institutional%20Memory%20Improves%20Agent%20Performance%20Over%20Time.pdf) | Cumulative learning: rho=0.716 with memory vs 0.040 without |

---

## Quick Start

```bash
git clone https://github.com/l33tdawg/sage.git && cd sage
go build -o sage-gui ./cmd/sage-gui/
./sage-gui setup    # Pick your AI, get MCP config
./sage-gui serve    # SAGE + Dashboard on :8080
```

Or grab a binary: [macOS DMG](https://github.com/l33tdawg/sage/releases/latest) (signed & notarized) | [Windows EXE](https://github.com/l33tdawg/sage/releases/latest) | [Linux tar.gz](https://github.com/l33tdawg/sage/releases/latest)

### Docker

```bash
docker pull ghcr.io/l33tdawg/sage:latest
docker run -p 8080:8080 -v ~/.sage:/root/.sage ghcr.io/l33tdawg/sage:latest
```

Pin a specific version with `ghcr.io/l33tdawg/sage:6.0.0`.

### Upgrading from an older version?

If you installed SAGE before v5.0 and your AI isn't doing turn-by-turn memory updates, re-run the installer in your project directory:

```bash
cd /path/to/your/project
sage-gui mcp install
```

This installs Claude Code hooks that enforce the memory lifecycle (boot, turn, reflect) — even if your `.mcp.json` is already configured. Restart your Claude Code session after running this.

---

## Documentation

| Doc | What's in it |
|-----|-------------|
| [Architecture & Deployment](docs/ARCHITECTURE.md) | Multi-agent networks, BFT, RBAC, federation, API reference |
| [Getting Started](docs/GETTING_STARTED.md) | Setup walkthrough, embedding providers, multi-agent network guide |
| [Security FAQ](SECURITY_FAQ.md) | Threat model, encryption, auth, signature scheme |
| [Connect Your AI](https://l33tdawg.github.io/sage/connect.html) | Interactive setup wizard for any provider |

---

## Stack

Go / CometBFT v0.38 / chi / SQLite / Ed25519 + AES-256-GCM + Argon2id / MCP

---

## License

Code: [Apache 2.0](LICENSE) | Papers: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Author

Dhillon Andrew Kannabhiran ([@l33tdawg](https://github.com/l33tdawg))

---

<p align="center"><em>A tribute to <a href="http://phenoelit.darklab.org/fx.html">Felix 'FX' Lindner</a> — who showed us <b>how much further curiosity can go.</b></em></p>
