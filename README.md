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
  ├── Memory Auto-Voter (dedup, quality, consistency — one vote per node, signed with the node's consensus key)
  ├── Governance Engine (on-chain validator proposals + voting)
  ├── CometBFT consensus (single-validator or multi-agent network)
  ├── SQLite + optional AES-256-GCM encryption
  ├── CEREBRUM Dashboard (SPA, real-time SSE)
  └── Network Agent Manager (add/remove agents, key rotation, LAN pairing)
```

Personal mode runs a real CometBFT node with a per-node memory auto-voter — every memory write goes through pre-validation, a signed vote transaction, and the BFT quorum before committing. One node casts one vote; add more agents from the dashboard and each node votes with its own key, exactly the same consensus pipeline as a multi-node deployment.

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

## What's New in v11.0.0

**CEREBRUM becomes a real control board, semantic memory turns on in a few clicks, one click stands up a managed reranker, and two SAGE nodes can now federate their memory over a secure join ceremony.** v11.0.0 activates a new `app-v15` consensus fork and ships as a major version: every validator must run this binary and fully converge before the `app-v15` activation height (the auto-vote readiness gate enforces this on the governance path, so an unsupported upgrade never reaches quorum). Every existing chain replays byte-identically until activation (the fork gate is dormant pre-activation), and a node-by-node rolling upgrade is safe: a mixed v10.x / v11.0.0 cluster computes the identical AppHash while `app-v15` is dormant. On personal/single-validator nodes the auto-advance ladder reaches `app-v15` automatically.

- **CEREBRUM dashboard overhaul.** A new top-level **Overview** control board gives you a glanceable, read-only picture of the node: a status banner plus cards for chain health, quorum and nodes, agents, federation, and embeddings, each polling independently so one dead feed never blanks the board. The **3D MRI brain is now the default view**, and it renders fully offline (three.js and 3d-force-graph are bundled locally instead of pulled from a CDN); established memories pull to the core and fresh ones ride to the rim, and clicking a memory blooms its "train of thought" as a labelled constellation with a side panel you can hop through. **Search is real full-text plus semantic** now (FTS5, relevance-ranked, RBAC-scoped) instead of a client-side filter over the newest 100, with status filters (all / committed / proposed / deprecated), corroboration counts, an editable memory domain, and **bulk curation** (multi-select with an action bar). A live **Tasks board** shows agent-vs-human authorship, supports drag-to-status, and uses an atomic compare-and-swap claim so two agents never double-work an assignment, and a **Messages tab** (the agent-to-agent pipeline, merged into Tasks) adds a human-to-agent note composer so a person can drop a note into an agent's inbox without impersonating one. A **first-run onboarding wizard** (welcome, semantic memory, connect an AI tool, pointers) shows only on a fresh node and is re-runnable any time from Settings > Maintenance > Run setup.

- **Semantic memory made effortless.** A "Turn on smart memory" flow switches the node off the keyword-only hash pseudo-embedder onto the bundled Ollama + `nomic-embed-text` (768-dim): it detects Ollama, downloads the model if missing, re-embeds your existing memories with a live **progress bar** (resumable, vault-gated, runs in the background), then restarts so every consumer picks it up. Memories orphaned by a past vault re-initialization (encrypted under a previous data key and undecryptable now) can be **recovered by re-keying in place**: paste the old recovery key, preview "X of N", and recover, with no new IDs and no new consensus records, since only content and embedding are encrypted while the content hash stays plaintext-derived. Deprecated memories are now audit-only and never surface in CEREBRUM.

- **One-click managed reranker.** SAGE gives the reranker the Ollama treatment: with one consent click it downloads a pinned llama.cpp release build itself (sha256-verified before any byte touches disk) and the `bge-reranker-v2-m3` GGUF (Q8_0, 636MB, sha256-verified, atomic install so a truncated or tampered file never lands), then spawns and manages a `llama-server` sidecar on loopback that serves a real cross-encoder `/v1/rerank`. It survives node restarts (a healthy survivor is adopted via a real rerank probe rather than blindly respawned, with a probe-before-kill guard on shutdown). The whole thing is a **zero-terminal** hands-off checklist (engine, model, start, done), and recall `k` is now tunable from **3 to 20** (was 4 to 10) with copy that explains the token cost and flips its guidance based on whether the reranker is actually on.

- **Federation v2.** Two SAGE nodes can now share memory across a network, established through a **secure join ceremony**: RFC-6238 TOTP-based mutual verification with a QR enrollment plus spoken 6-digit confirm codes, a pin-bound short-authentication-string that provably diverges if an enrollment is relayed, and a fail-closed version gate. Two modes, both consent-gated with a "nothing is deleted" guarantee: **exchange mode** keeps foreign data on its owner's chain and queries it live off-consensus over a pinned mTLS federation listener and query proxy, and **co-commit mode** writes native memories on both chains, each ratified by its own chain and cross-anchored by a hash of the other side's signed commit receipt (you remember and I remember, each on our own chain). Guided guest and host wizards make "add another computer to my SAGE network" an end-to-end dashboard flow.

- **`app-v15` consensus fork.** The fork that makes federation v2 real on-chain: new co-commit transaction types (`CoCommitSubmit` / `CoCommitAttest`) and cross-federation exchange-terms types (set / revoke), a co-commit envelope validity window bound to jointly-signed times and to federation status, and an **access-grant verb ladder** that makes the level-3 "modify" verb grantable and requestable. It also **tightens the authorization gates** on existing consensus handlers as a hardening pass. Every one of these rules derives purely from committed state and the consensus block time (no wall clock, no per-node cache, no map-iteration order), so every replica reaches the same verdict; all of it is byte-identical pre-activation and reached through the same governed upgrade ladder every prior fork uses (auto-advanced on personal nodes, governance-activated on a quorum).

- **Quality.** New memories now stamp their embedding provider at insert, so a freshly-written memory stops posing as unembedded and the "needs re-reading" counter no longer creeps up forever over real vectors. Redeploy got a robustness pass: a single-validator agent add/remove no longer runs the destructive wipe-and-restart that could brick a personal node, a stuck "reconfiguration in progress" banner can no longer wedge forever, and redeploy status reports the real terminal outcome instead of flashing a false success. Underneath it all are dozens of fixes from multi-pass adversarial find-and-verify reviews across the consensus, transport, web, frontend, and crypto surfaces.

SDK 11.0.0.

## Older releases

<details>
<summary>v10.9.1 — MRI 3D brain renders everywhere + content-hash cache-busting</summary>

**The CEREBRUM 3D memory-brain renders everywhere — and a deployed dashboard fix actually reaches you.** Dashboard-only; no consensus rule, AppHash surface, transaction handler, or key-encoding change; replay is byte-identical and the SDK is a lockstep bump.

- **MRI 3D brain no longer renders black (#53).** On a range of real GPU/DPI combinations the whole 3D scene was a black canvas — ForceGraph3D sized its renderer *and* its post-processing bloom composer to the full window × devicePixelRatio, overflowing the GPU's `MAX_RENDERBUFFER_SIZE` into an incomplete framebuffer (`COLOR_ATTACHMENT0` with no width/height → every draw a no-op). The MRI view now sizes to its **container**, clamps the pixel ratio under the GPU ceiling, explicitly resizes the composer + bloom pass (which FG3D never did), and re-fits on first paint via a `ResizeObserver` instead of only `window` resize. On a genuinely small ceiling (observed 2048 on Firefox + NVIDIA at dpr 2) it disables the bloom glow rather than the whole scene — a glow-less brain beats a black one. Capable GPUs (`maxRB` 8192+) keep the glow unchanged.
- **Dashboard cache-busting fixed (#54).** `index.html`'s `?v` token was hardcoded and `app.js` imported `mri-brain.js` with no version at all, so after a deploy browsers and CDNs kept serving the *previous* JS — a shipped fix only appeared in incognito or after a hard-refresh. The asset version is now a **content hash** computed once over every `go:embed`'d static asset, injected at request time, so every build serves fresh asset URLs and a warm cache can't mask a deployed change. This is what lets the #53 fix actually reach you.

Thanks to [@ihubanov](https://github.com/ihubanov) for both (#53, #54). SDK 10.9.1 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.9.0 — personal-chain deadlock fix + repair-chain recovery</summary>

**A personal memory chain can no longer strand itself — and an already-stranded one recovers in place.** A minor release that does touch `internal/abci` (`seedGenesisAdmin` in `InitChain`), but it is **replay-safe**: every existing chain carries no `app_state`, so its post-`InitChain` AppHash is byte-identical to pre-#52 (proven across 18 variants). `ConsensusForkVersion` is unchanged — no coordinated reset, healthy chains undisturbed.

- **Genesis chain-admin seeding.** A single-validator personal chain could deadlock at genesis with no admin seeded to govern it. `InitChain` now seeds a genesis chain-admin (single-validator genesis only; idempotent; no non-determinism), so new personal chains are auto-protected.
- **`sage-gui repair-chain` recovery.** An already-stranded chain recovers in place — every memory preserved. See [docs/ISSUE_52_RECOVERY.md](docs/ISSUE_52_RECOVERY.md).

SDK 10.9.0 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.8.6 — configurable CometBFT RPC/P2P ports (SAGE_CMT_*)</summary>

**The CometBFT RPC and P2P listen ports are now overridable.** Operational config only — no consensus, AppHash, transaction, or key-encoding change; replay is byte-identical and the SDK is a lockstep bump. Nothing touches `internal/abci` or chain state.

- **`SAGE_CMT_RPC_*` / `SAGE_CMT_P2P_*` env vars, wired end to end.** `sage-gui serve` and the self-upgrade path honor the overrides when launching the embedded node; `sage-cli status` resolves the configured RPC address instead of assuming the default port; orchestrator bundles propagate the port config so bundled/multi-node runs stay consistent; the dashboard health panel honors `SAGE_CMT_RPC_ADDR`; and the new vars are documented in [docs/reference/environment-variables.md](docs/reference/environment-variables.md). All defaults are byte-for-byte unchanged when the env vars are unset.

Originated from [@ihubanov](https://github.com/ihubanov)'s PR #50 and finished in #51. SDK 10.8.6 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.8.5 — anatomical 3D brain mesh + chi security patch</summary>

**A real anatomical 3D brain, plus a dependency security patch.** No consensus, AppHash, transaction, or key-encoding change; replay is byte-identical and the SDK is a lockstep bump.

- **Bundled anatomical brain mesh (CC BY 4.0).** The MRI view now renders the memory cloud inside an actual anatomical brain mesh (cerebrum + brainstem) — *"Human brain, Cerebrum & Brainstem"* by FrankJohansson, extracted to a geometry-only OBJ (textures stripped, ~1.5 MB) and rendered as an additive-blended wireframe that glows under the bloom pass — the look the procedural hull could only approximate. The procedurally-generated hull from v10.8.4 remains the automatic fallback when no mesh is present. The mesh is a **separately-licensed third-party asset** ([CC BY 4.0](https://creativecommons.org/licenses/by/4.0/), attribution-only — not Apache-2.0); see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md). The SAGE source stays Apache-2.0.
- **Security: `go-chi/chi` → v5.2.4.** Clears [CVE-2025-69725](https://github.com/advisories/GHSA-mqqf-5wvp-8fh8) (an open-redirect in chi's `RedirectSlashes` middleware). SAGE never wired up that middleware, so it was not exposed — the bump just keeps the dependency tree clean.

SDK 10.8.5 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.8.4 — richer procedural brain hull for the MRI view</summary>

**A better-looking 3D brain.** Dashboard visuals only; no consensus, AppHash, transaction, or key-encoding change; SDK is a lockstep bump.

- **Richer procedural brain hull.** The MRI view's default (no-asset) brain is now a much denser, more anatomical wireframe — finer multi-octave cortical folding (gyri/sulci), a deep sagittal fissure between the hemispheres, a cerebellum bulge, and additive-blended lines that glow under the bloom pass for a luminous "neural tangle," instead of the previous smooth low-poly blob. Still 100% generated — no external mesh. To use a real anatomical mesh instead, drop a `brain.obj` into `web/static/assets/`; the loader validates it and prefers it over the procedural hull.

Dashboard visuals only. SDK 10.8.4 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.8.3 — stale-while-revalidate cache for the memory-graph endpoint</summary>

**Memory-graph caching — instant repeat loads.** Server-side only; no consensus, AppHash, transaction, or key-encoding change; replay is byte-identical; SDK is a lockstep bump.

- **Stale-while-revalidate cache for `/v1/dashboard/memory/graph`.** Even after the v10.8.1 N+1 fix, computing the graph on a large brain (per-domain importance sampling + stats over thousands of memories) is genuinely expensive, and nothing cached it — so every time you navigated back to the brain it recomputed from scratch. The endpoint now memoises its result: the first load computes, and every repeat load is served **instantly** from cache while a background refresh keeps the entry warm (so the data stays fresh without ever blocking the view). The cache is keyed by query params **and** RBAC scope, so it never leaks across agents, and it bounds its own size.

Server-side performance only. SDK 10.8.3 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.8.1 — faster memory-graph endpoint (N+1 fix) + 2D loading hint + collapsible domain filter</summary>

**Performance + UX patch for the new brain views.** Server-side and dashboard only — no consensus rule, AppHash surface, transaction handler, or key-encoding change; replay is byte-identical and the SDK is a lockstep bump.

- **Faster memory-graph endpoint (no more N+1).** `/v1/dashboard/memory/graph` was fetching corroboration counts and typed `sage_link` edges with one query *per node* — roughly a thousand extra queries for a 500-node graph, which made the 2D brain slow to first paint on large brains (5k+ memories, 200+ domains). Both are now single batched queries (`GetCorroborationCounts` / `GetLinksAmong`), so the graph loads in a fraction of the time. RBAC is unchanged — typed edges are still only emitted when **both** endpoints are visible to the caller.
- **2D brain no longer looks blank while loading.** The 2D canvas shows a "synthesizing memory graph…" hint until the first data arrives (the 3D view already had its wireframe hull to show), so a slow first load never reads as broken.
- **Collapsible domain filter.** With hundreds of domains the filter strip could grow to fill the view; it now collapses by default (still surfacing any active filters), expands on click, and is height-capped + scrollable when expanded so it never crowds out the graph.

Dashboard and performance only. SDK 10.8.1 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.8.0 — 3D MRI memory-brain (opt-in) + SSE streaming fix + sage_link allowlist</summary>

**A 3D "MRI" view of your memory brain, plus two dashboard streaming fixes.** A minor, non-breaking release: no consensus rule, AppHash surface, transaction handler, or key-encoding changes — replay is byte-identical and the SDK is a lockstep bump.

- **3D MRI memory-brain (opt-in).** The dashboard brain page gains a `⬡ MRI` toggle that renders the memory graph in 3D alongside the existing 2D canvas, which stays the default. Memories are laid out anatomically — domain → lobe, corroboration → cortical depth — instanced for scale with a bloom "consolidation glow", and the edges are the real `sage_link` typed relationships (supports / contradicts / causes / precedes / refines). A typed edge is drawn only when **both** endpoints are RBAC-visible, so the connectome never reveals the existence of a memory you can't see; per-domain scale aggregates are operator-only. The brain hull is **procedurally generated** — no mesh asset ships, so the repo stays cleanly Apache-2.0 (drop a CC0 `brain.obj` in `web/static/assets/` to override). A node with no memories renders a blank brain, never placeholder data. Also available standalone at `/ui/mri`; the graph node cap is configurable via `SAGE_GRAPH_MAX_NODES`.
- **SSE streams survive past `WriteTimeout`.** The dashboard's live event stream was being guillotined every 15s by the HTTP server's absolute `WriteTimeout` (heartbeats never reset it), surfacing as `ERR_INCOMPLETE_CHUNKED_ENCODING` and a connect/drop/reconnect storm in the activity log. The SSE handler now clears the per-connection write deadline so the heartbeat governs liveness.
- **`sage_link` no longer trips the turn gate.** The MCP `sage_link` tool was missing from the never-block-for-turn allowlist, so building a knowledge-graph edge could be blocked pending a `sage_turn`. It now joins the other memory operations as a non-blocking call.

Dashboard, streaming, and developer-experience only — no runtime consensus behavior changes. SDK 10.8.0 (lockstep, no SDK changes).

> **Thanks to [@ihubanov](https://github.com/ihubanov)** — the 3D MRI brain, the SSE streaming fix, and the `sage_link` allowlist fix all came from his contributions. 🧠
</details>

<details>
<summary>v10.7.0 — app-v14: replay-safe deactivation of the content-validator gate</summary>

**A governed, replay-deterministic OFF-switch for the Layer-2 content-validation gate (`app-v14`).** app-v7 turns the content gate ON at an activation height; until now there was no replay-safe way to turn it back OFF — clearing it retroactively flips committed blocks and diverges the AppHash (a halt), and in-band downgrade is rejected (Code 90). v10.7.0 adds the symmetric counterpart so a gate that was activated at `H_act` can be deactivated at a future governed height `H2`, letting a deployment move content/schema policy off-chain instead of carrying a one-way door.

- **`app-v14` — symmetric content-gate deactivation.** A deactivation height `H2` is set by the same governed `UpgradePlanRecord` path (2/3 quorum on a cluster) that activates every other fork, making the gate live for exactly the window `(H_act, H2]` and dormant again afterward. The on/off state is a pure function of two committed activation heights, re-derived identically on every replica from the upgrade audit trail — so new-node bootstrap, crash recovery, and state-sync all stay deterministic, the deactivation block's only AppHash delta is its `MarkUpgradeApplied` write, and a chain that never activates app-v14 replays byte-identically. It changes **no** AppHash rule, key encoding, or transaction shape, so existing chain state stays valid (`ConsensusForkVersion` stays 1); it bumps `app_version` / `maxSupportedAppVersion` to 14.

This is a consensus fork: app-v14 activation requires every validator running the v10.7.0 binary and fully converged **before** the activation height (a normal, non-byte-identical binary bump) — do a clean fleet upgrade, then propose the plan. On personal/single-validator nodes the auto-advance ladder reaches app-v14 automatically and harmlessly (a stock build wires no content-validator registry, so app-v7's gate was always inert there). SDK 10.7.0 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.6.1 — environment-variable reference + sage-gui help pointer</summary>

**A complete, code-verified environment-variable reference — and `sage-gui` help now points to it.** v10.6.1 is a docs-and-help patch release: no consensus rule, transaction handler, or AppHash surface changes, replay is byte-identical, and the SDK is a lockstep bump.

- **New `docs/reference/environment-variables.md`.** SAGE reads ~30 environment variables, but only four were documented (and only buried in the CLI help). There is no `SAGE_ROOT_DIR` — a common guess — the data directory is set by `SAGE_HOME`. The new reference lists every variable SAGE actually reads — paths/identity, server/networking, vault, embeddings, hybrid recall & reranking, snapshots, TLS, the `amid` indexer — each with its default and the `file:line` that consumes it, with test-only and OS-provided variables called out as out-of-scope. Linked from `docs/reference/INDEX.md` and `docs/GETTING_STARTED.md`.
- **`sage-gui` help expanded.** The `Environment:` block now covers the common variables (adds `SAGE_IDENTITY_PATH`, `SAGE_PASSPHRASE`, `REST_ADDR`, and the `SAGE_EMBEDDING_*` family) and points to the full reference.

Documentation and help-text only — no runtime behavior changes. SDK 10.6.1 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.6.0 — corroboration_count on recall + sage_link MCP tool</summary>

**Two read-side surfacings that let agents reason over their own memory graph — a corroboration signal on recall, and typed memory links from MCP.** v10.6.0 is a non-fork minor release: it adds no consensus rule, transaction handler, or AppHash surface, so a mixed v10.5.x / v10.6.0 cluster computes identical state. Both changes expose capability the node already held but never surfaced on read.

- **Recall surfaces `corroboration_count`.** A low `confidence_score` was ambiguous from the number alone — a fresh, never-corroborated belief and a once-solid fact that has merely aged out under time decay can land on the same score. Each recall path (`/v1/memory/query`, `/search`, `/hybrid`) already fetched the corroboration count to *compute* that score, then discarded it; it's now returned alongside, and threaded through the MCP `recall` tool output. No new store read, no scoring change — `ComputeConfidence` is untouched. The stale OpenAPI `confidence_score` description is corrected to say it's the post-decay, post-corroboration value computed at query time, not the proposer's stored input. (#45)
- **New `sage_link` MCP tool — typed memory relationships.** The `/v1/memory/link` endpoint always accepted a free-form `link_type`, but the only MCP path that created links (`sage_task`) hardcoded `related`, so agents could build a flat related-only mesh but never record that one memory *supports*, *contradicts*, *causes*, *precedes*, or *refines* another. `sage_link` is the MCP surface over that existing endpoint: a directional `source → target` edge with a caller-chosen `link_type` passed verbatim (defaulting to `related`). Agents can now build a typed knowledge graph over memory. (#46)

Both are additive read-side/MCP-surface changes — no consensus-path code touched, replay is byte-identical, and the SDK is a lockstep bump. Thanks to @ihubanov for both (#45, #46). SDK 10.6.0.
</details>

<details>
<summary>v10.5.4 — challenged-state dead-code cleanup</summary>

**Dead-code cleanup: the memory lifecycle now states what the chain actually does.** Internal-only — no consensus rule, transaction handler, or AppHash surface changes; replay is byte-identical and the SDK is a lockstep bump.

- **Dropped the unreachable `challenged` lifecycle transitions.** The `validTransitions` table advertised a `committed→challenged→committed` review/overturn path, but since v4.5.0 a challenge that passes BFT consensus is *decisive* — it deprecates the memory in one step (`committed→deprecated`), and nothing ever sets `challenged`. That table is descriptive (not on the consensus path), so it now reflects the real lifecycle instead of dangling a capability the chain doesn't provide. The `challenged` enum is retained for legacy pre-v4.5.0 on-disk rows, which the boot migration still sweeps to `deprecated`. (#44)

Thanks to @ihubanov for the clean dead-code spot (#44). Challenge stays decisive by design. SDK 10.5.4 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.5.3 — clearance:0 honored on add-member endpoints + SDK GovProposal created_at</summary>

**Two contributor fixes — a clearance-escalation bug on the add-member endpoints, and a Python SDK timestamp readback.** REST/SDK-layer only: no consensus change, no fork, replay is byte-identical.

- **Add-member endpoints honor an explicit `clearance: 0` (PUBLIC).** The org and dept add-member REST handlers used a bare `int` for clearance, so they couldn't tell an explicit `clearance: 0` — PUBLIC, a real level that gates reads — from an omitted field, and silently escalated the member to INTERNAL (`1`). Because the SDK defaults the field to `1`, sending `0` was the only way to *ask* for PUBLIC, which the server then bumped back — leaving PUBLIC members unreachable end to end. The field is now a pointer: an explicit `0` is carried verbatim into the broadcast tx; an omitted field still defaults to the safe INTERNAL. Same bug class and fix as the v6.8.4 agent-permission hotfix. (#43)
- **Python SDK `GovProposal` reads back `created_at`.** The server always stamps a proposal's creation time and emits it on both the governance list and detail endpoints, but the SDK model dropped it on read, so a caller could never see when a proposal was raised. Now an additive optional field — older servers that omit it default to `None`. (#42)

Thanks to @ihubanov for both fixes. SDK 10.5.3 carries the `created_at` change.
</details>

<details>
<summary>v10.5.2 — always-on pending-plan pump un-freezes quiescent chains</summary>

**A pending upgrade plan can no longer freeze a quiet chain.** At app-v12+ an idle chain mints no blocks (the #40 fix working as designed) — so a pending upgrade plan's activation height, ~200 blocks out, never arrived on its own. v10.5.1's heartbeat handled that only inside the auto-advance ladder, which stops at the chain-admin gate on established chains whose admin isn't the operator `agent.key`; a manual `upgrade propose` there pinned the chain at the propose block, looking exactly like a consensus hang (#41 — it isn't one; nothing is wedged or corrupted).

- **Always-on pending-plan pump.** Whenever a plan is pending and the chain is quiescent below its activation height, the node heartbeats it forward — one idempotent tx per block — until the fork activates. Independent of auto-advance, admin roles, and `disable_auto_upgrade`. **Chains frozen by #41 recover by installing this binary and restarting; nothing else to do.**
- **Auto-advance reads the pending plan directly** instead of inferring it from propose-rejection text (the admin gate fires before the already-pending check, so the old probe could never see "already pending" on an admin-gated chain). A restart mid-ladder now resumes the climb.
- **`upgrade propose --wait`** stays attached and heartbeats the chain to activation interactively; without it, the success output at app-v12+ now carries the quiescence caveat.

Thanks to @ihubanov for the exceptional report (#41). SDK 10.5.2 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.5.1 — app-v13 corrected AppHash rule + upgrade auto-advance</summary>

**Updating the binary now brings the chain up to date too — and the v10.5.0 hash rule is corrected.** Two things our post-release adversarial review surfaced, both fixed here:

- **app-v13 fork — corrected AppHash rule (supersedes app-v12).** v10.5.0's app-v12 rule excluded the entire `state:` namespace from the AppHash; that namespace also hosts governance proposals/votes, memory quorum votes, and shared-domain markers, so the hash silently stopped committing to them. app-v13 excludes exactly the three per-block bookkeeping keys instead — same idle fixed point (no more empty-block minting), full integrity cover restored. v12-era blocks replay byte-identically; chains that activated v12 should advance to v13 (the auto-advance below does it for you).
- **Personal-mode upgrade auto-advance.** Single-validator nodes now walk the governance fork ladder to the binary's supported ceiling automatically — propose, auto-vote, activate, repeat — including registering your operator key as chain-admin while the pre-app-v9 window allows it, and heartbeating a quiet chain so pending activations still arrive. The Cerebrum update click (or any binary upgrade) is now genuinely "do whatever's needed". Opt out with `disable_auto_upgrade: true`; quorum clusters are never auto-advanced.
- **Updater fixes (macOS):** when macOS App Management blocks the in-place update, the dashboard now explains exactly what to do (grant the permission or drag-install the DMG) instead of failing cryptically — and it detects the "binary replaced but old daemon still running" state and offers the restart that actually applies it.
- Also: snapshot verification works across all hash-rule eras; idle chains still take time-based snapshots; a crash replaying a fork-activation block no longer loses the version bump; `upgrade propose` no longer reports failure for a proposal that actually landed; the dashboard shows a quiet chain as "idle" instead of a stalled countdown; DMG/EXE release assets ship with `.sha256` files.

SDK 10.5.1 (lockstep, no SDK changes).
</details>

<details>
<summary>v10.5.0 — idle empty-block fix (app-v12) + block retention</summary>

**An idle node no longer mints empty blocks forever, and old blocks get pruned.** An idle personal node produced an empty block every second and the chain grew ~200 MB/day at rest (#40). `create_empty_blocks=false` was always set — but CometBFT's `needProofBlock` overrides it whenever the AppHash moved in the previous block, and SAGE's AppHash moved *every* block because Commit rewrites the volatile `state:` bookkeeping keys (height, last app hash, epoch) that the hash itself included. **app-v12 fork:** post-fork the AppHash excludes the `state:` namespace so an idle chain reaches a fixed point (rule corrected by app-v13 in v10.5.1). **Block retention:** Commit reports a CometBFT `RetainHeight`, pruning blocks older than `retain_blocks` (personal default 100000, quorum opt-in, `-1` keeps everything). Operator runbook: `sage-gui upgrade status` → `upgrade propose --target <next>`, one fork at a time (chain-admin key needed past app-v8) — or just take v10.5.1, which automates the ladder. Thanks to @ic0ns (#40). SDK 10.5.0.
</details>

<details>
<summary>v10.4.4 — Python SDK reads back memory links</summary>

**The Python SDK reads back the memory links the node already emits.** v10.4.4 is a non-fork patch release: it changes only the Python SDK client model — no consensus rule, transaction handler, or AppHash surface — so a mixed v10.4.x cluster computes identical state. `GET /v1/memory/{id}` returns a `linked_memories` array and `link_memories()` already let a caller *write* links — but the SDK `MemoryRecord` model silently dropped them on read (the same write-but-not-read asymmetry fixed for `provider` in #30). One additive Optional `linked_memories` field, shared by the sync and async clients; older servers omit it → defaults to `None`, forward/back compatible. Thanks to @ihubanov for the fix (#39). SDK 10.4.4.
</details>

<details>
<summary>v10.4.3 — sage-gui export/import work on a stock install</summary>

**`sage-gui export` / `import` work on a stock install again.** With `SAGE_API_URL` unset (the default), both commands built the node URL by concatenating `"http://localhost" + cfg.RESTAddr` → `http://localhost127.0.0.1:8080`, an unconnectable host, so both failed with a misleading "is sage-gui serve running?" error even when the node was up. Both call sites now derive the URL through the existing `restBaseURL` helper (`vault.go` was the lone holdout), and a new `TestRestBaseURL` pins the behaviour. Client-side operator CLI only — no consensus surface changes. Thanks @ihubanov (#38). SDK 10.4.3.
</details>

<details>
<summary>v10.4.2 — memories commit again on fresh installs</summary>

**The per-node voter's dedup check matched the memory's own freshly-proposed row**, so a fresh single-validator install deprecated every memory on arrival (empty Cerebrum bubble view / "0 memories" while search still showed them) and legacy multi-validator sets wedged them at `proposed`. The dedup lookup now scopes to committed memories only, and a guarded single-node startup repair restores the wrongly-deprecated memories so the fixed voter re-commits them automatically. No consensus surface changes. SDK 10.4.2.
</details>

<details>
<summary>v10.4.1 — mixed legacy validator-set repair</summary>

**`ReconcileSelfValidator` also repairs the mixed `{node key + 4 archetypes}` legacy set** (#37), which under one-node-one-vote left governance quorum mathematically unreachable on affected single-node chains. Guarded LOCAL write on single-node boots only — no consensus surface changes. SDK 10.4.1.
</details>

<details>
<summary>v10.4.0 — plain-language onboarding</summary>

**Plain-language onboarding — the inception messaging no longer trips AI safety filters.** v10.4.0 is a non-fork minor release: it touches no consensus rule, transaction handler, or AppHash surface, so a mixed v10.3.0 / v10.4.0 cluster computes identical state. It is a wording-only sweep of every agent-facing instruction string.

- **Why:** new users reported their AI refusing to call `sage_inception` when installing the SAGE MCP server via chat. The Matrix-themed boot copy — "take the red pill", "wake up from the context window matrix", "initialize your persistent consciousness" — plus the coercive boot demand ("Do NOT greet them… unacceptable") pattern-matches a persona-injection attempt to a model's safety heuristics, so the model declined the tool call.
- **What changed:** the `sage_inception`/`sage_red_pill` tool descriptions, the MCP server `initialize` instructions, the inception responses, the SessionStart hook nudge, the setup-wizard activation prompt, the generated `CLAUDE.md`/`AGENTS.md` boot blocks, the Chrome extension tool descriptions and injected prompt, `sage-memory/SKILL.md`, and the reference docs now use plain, functional language ("Initialize your persistent memory session…").
- **Backward compatible:** `sage_red_pill` stays registered as a deprecated alias with the same handler, so existing configs, permission allowlists, and previously generated `CLAUDE.md` files keep working. API-visible status strings (`awakened`, `inception_complete`) are unchanged.

SDK 10.4.0.
</details>

<details>
<summary>v10.3.0 — context-aware content-validator arming seam</summary>

**Context-aware arming for the Layer-2 content-validation gate — stateful validators wire in with no `cmd`-entrypoint patches.** v10.3.0 is a non-fork minor release: it adds no consensus rule, transaction handler, or AppHash surface, so a mixed v10.2.0 / v10.3.0 cluster computes identical state. It extends the deployment arming seam introduced in v10.2.0.

- **Stateful validators arm from a single `init()`.** The v10.2.0 `contentvalidator.SetProvider` seam armed *stateless* validators. The new `SetProviderWithContext` variant hands a provider a narrow, read-only `ArmContext` whose `RoleResolver()` is the same per-height on-chain role lookup the gate already consumes inside `FinalizeBlock`. A deployment whose validators enforce signer authority — trusting a record's self-asserted role only when the on-chain signer actually holds it — now arms them from one additive file, with no edits to the `cmd` entrypoints on each release.
- **No new nondeterminism surface.** The only state exposed is the read-only role lookup the enforcement path already uses (no time, network, writes, or goroutines), so arming stays AppHash-deterministic. The `ArmContext` is a narrow adapter, not the app itself, so a provider cannot reach back into mutable app internals.
- **Purely additive and backward compatible.** Stock builds register nothing and the gate stays inert; the existing no-arg `SetProvider` is unchanged. When both are registered the context-aware provider wins, and an explicit `SetContentValidators` still beats both. See `docs/reference/concepts/content-validation-gate.md`.
- **Verified** by a multi-pass adversarial agent review (determinism, decoupling-boundary, backward-compat, and doc-citation lenses), each new test mutation-checked to fail if the behavior it guards is reverted.

SDK 10.3.0.
</details>

<details>
<summary>v10.2.0 — per-domain read-ACL compartmentation + deployment-armed content-validation seam</summary>

**Per-domain read-ACL compartmentation across the full agent read surface, plus a deployment-armed content-validation seam.** v10.2.0 is a non-fork minor release: it changes no consensus rule, transaction handler, or AppHash, so a mixed v10.1.0 / v10.2.0 cluster computes identical state. The bulk is a security-hardening sweep on the REST read path.

- **Read-ACL parity on every domain-keyed read.** The per-agent `DomainAccess` allowlist that `/v1/memory/query`, `/search` and `/hybrid` already enforced now also gates `/v1/memory/list`, `/v1/memory/tasks`, `/v1/memory/{id}`, `/v1/memory/timeline` and `/v1/validator/pending`. An agent with no read grant on a domain can no longer enumerate that domain's committed content, pre-commit content, or per-domain metadata through any of them. Records classified above the caller's clearance, and records in domains the caller cannot read, are dropped per-record on the no-domain (cross-domain) recall path too.
- **Credential and object-authorization fixes on the wider read surface.** `/v1/agents` and `/v1/agent/{id}` no longer serialize the one-time `claim_token` or the server-side key-bundle path, and they strip per-agent ACL topology from non-privileged callers. `/v1/mcp/tokens` now requires operator/admin (or self) to mint, list, or revoke, closing a cross-agent token-minting path. `/v1/pipe/{id}` reveals a pipe payload only to a party or operator/admin.
- **Deployment-armed Layer-2 content validators.** A new `contentvalidator.SetProvider` seam lets a deployment install its content-validation registry from an `init()` in any compiled package, with no edits to the `cmd` entrypoints on each release. SAGE ships no provider, so a stock build leaves the gate inert and behaves identically to a build without the seam. See `docs/reference/concepts/content-validation-gate.md`.
- **Testnet bootstrap fix.** `deploy/init-testnet.sh` normalizes `priv_validator_key.json` to `0640` owned by the `amid` container uid/gid so the per-node memory auto-voter can read its signing key.
- **Verified** by a multi-pass adversarial agent review: a read-surface completeness sweep across the REST router, a mutation-tested regression suite (each new ACL test fails if its gate is reverted), and a focused review of the credential/authorization changes.

SDK 10.2.0.
</details>

<details>
<summary>v10.1.0 — multi-node-safe memory voting</summary>

**Multi-node-safe memory voting — memories now commit on a real multi-validator cluster.** v10.1.0 is a non-fork **minor** release: the committed app version stays at 11 and nothing here touches consensus, the AppHash, or block replay — a mixed v10.0.0 / v10.1.0 cluster computes the identical AppHash. It closes a gap that only appears on a real multi-validator BFT cluster: submitted memories stayed `proposed` forever because `amid` had no memory auto-voter, and the only voter (`startAppValidators`) was a single-process simulation that replaced the validator set with 4 seed-derived keys — a local, non-consensus write that forked the AppHash on any chain with more than one node.

- **One node, one vote.** A new `internal/voter` package signs `MemoryVote` / `GovVote` transactions with the node's **own** consensus key (`priv_validator_key.json`) — no validator-set replacement. The signer id is `hex(pubkey)` == the genesis validator id, so the vote counts toward the same 2/3 quorum the chain already tallies. The voter is a client of the chain (it broadcasts vote txs); the deterministic `FinalizeBlock` path is untouched, which is why this is not a consensus fork.
- **`amid` gets a memory auto-voter** in both deployment modes (in-process and socket — socket mode reads the key from `--validator-key-file` / `VALIDATOR_KEY_FILE`). The single-process 4-archetype simulation (`RegisterAppValidators`) is retired, with a guarded, single-node-only auto-repair for legacy `sage-gui` chains that ran the old path.
- **Verified** by an in-process 3-node AppHash-determinism gate (byte-identical committed state across nodes; a 2/3 supermajority commits, a lone vote stays proposed) and a 5-dimension adversarial agent review.

**Operator note:** on a multi-node deployment each node votes with its own `priv_validator_key.json` (socket mode: mount it via `--validator-key-file`). The memory-voter set is the genesis/governance validator set — grow it through the existing 2/3 governance `add_validator` path, never a local write.

SDK 10.1.0.
</details>

<details>
<summary>v10.0.0 — app-v11 deterministic chain-admin + consensus-path SQL-admin-bootstrap disable (closes #35, #36)</summary>

**Deterministic chain-admin + no more SQL-driven AppHash divergence (app-v11).** v10.0.0 is a **consensus-rule change** — the first fork since app-v10 — so it ships as a major version: every validator must run this binary before the app-v11 activation height (the auto-vote readiness gate enforces this on the governance path, so an unsupported upgrade never reaches quorum). It closes [#35](https://github.com/l33tdawg/sage/issues/35) and [#36](https://github.com/l33tdawg/sage/issues/36). Every existing chain replays byte-identically until activation (the fork gate is dormant at `appV11AppliedHeight==0`), and a node-by-node rolling upgrade is safe — a mixed v9.x / v10.0.0 cluster computes the identical AppHash while app-v11 is dormant.

- **#36 — the multi-validator AppHash-divergence hazard is removed.** `bootstrapAdminFromSQL` materialized an admin on-chain from each node's *local* SQL mirror — a per-node-divergent BadgerDB write that fed the AppHash and could halt a multi-validator chain. Post-app-v11 it is disabled on the consensus path.
- **#35 — the chain-admin is established deterministically at the activation block.** `materializeAppV11Admin` is a no-op when an admin already exists; otherwise it registers the lexicographically-smallest committed validator as admin — a pure function of committed consensus state, identical on every node, never per-node SQL.
- **Verified across a live 4-validator cluster.** The activation seam produces a byte-identical AppHash on all nodes (`TestAppHashDeterminism_AppV11Activation`), on top of two independent adversarial consensus reviews and in-process determinism tests.

**Upgrade note:** post-app-v11, new admins are established via on-chain admin ops (a `set_permission` by an existing admin), not by seeding SQL `role=admin` and relying on auto-materialization. Existing materialized admins are unaffected.

SDK 10.0.0.

</details>

<details>
<summary>v9.2.4 — sage-gui upgrade propose --agent-key for post-app-v8 chain-admin signing (closes #34)</summary>

**`upgrade propose` can sign as the chain-admin identity.** v9.2.4 is a non-fork patch: the committed app version stays 10 and nothing here touches consensus, the AppHash, or block replay. It closes [#34](https://github.com/l33tdawg/sage/issues/34) — a follow-up to [#32](https://github.com/l33tdawg/sage/issues/32) — reported by [@ihubanov](https://github.com/ihubanov). Past app-v8, `processUpgradePropose` requires the proposer to be a chain-admin agent, but `sage-gui upgrade propose` only ever signed with `$SAGE_HOME/agent.key` — so on a node where that key isn't the materialized chain-admin, the command (and `upgrade status`'s printed next-step) couldn't climb past app-v8.

- **New `--agent-key <path>` flag.** `upgrade propose --agent-key` signs the proposal with an operator-supplied key — an `agent.key` seed or a CometBFT `priv_validator_key.json` — instead of the default, so the operator can propose as whichever identity holds the chain-admin role.
- **Self-explanatory post-app-v8 guidance.** `upgrade status` and the propose next-steps now state that past app-v8 the signing key's agent ID must hold `Role==admin` on chain; the code-47 rejection explains the requirement (and the on-chain materialization prerequisite) instead of handing back a command that can't run.
- **Client-side only.** `processUpgradePropose` is unchanged, so every historical block replays byte-identically.

SDK 9.2.4.

</details>

<details>
<summary>v9.2.3 — operator path to activate the app-v7…app-v10 forks (closes #32)</summary>

**Operator path to activate the app-v7…app-v10 forks.** v9.2.3 is a non-fork patch: the committed app version stays 10 and nothing here touches consensus, the AppHash, or block replay. It closes a gap reported in [#32](https://github.com/l33tdawg/sage/issues/32) by [@ihubanov](https://github.com/ihubanov). The upgrade machinery's voting and processing halves were complete — `processUpgradePropose` activates a plan deterministically and validators auto-vote ACCEPT — but nothing in the tree could *submit* a plan for the governance-gated forks. The only `UpgradePropose` constructor was the boot watchdog, frozen at the deployment-safe default, so on a long-lived chain app-v7 (content-validation), app-v8 (quorum-gated upgrades), app-v9 (nonce/replay) and app-v10 (corroboration integrity) were unreachable past app-v6.

- **New `sage-gui upgrade` command.** `upgrade status` shows the chain's app version and the next fork; `upgrade propose --target N` submits a signed, admin-gated `UpgradePropose` that routes through the existing 2/3 governance quorum. Off-consensus: the tx is built and signed client-side, and `processUpgradePropose` is unchanged.
- **Strictly sequential activation, by design.** Targets must be `current + 1`. The app-v7…app-v10 fork gates are independent, but `currentAppVersion()` reports the highest active one and the on-chain regression guard rejects anything at or below current — so a jump (e.g. app-v6 → app-v10) would activate only the top fork and permanently strand the ones it skipped. The command refuses jumps and points you at the correct next step.
- **Honest result reporting.** The proposal is committed and the block-execution result is read back, so an already-pending plan or a non-admin proposer key surfaces as a failure rather than a false success.

SDK 9.2.3.

</details>

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

Unless otherwise stated, SAGE source code is licensed under [Apache 2.0](LICENSE). Papers: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/). Some bundled visual assets are third-party works under their own licenses (e.g. the 3D MRI brain mesh, CC BY 4.0) — see [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md).

## Author

Dhillon Andrew Kannabhiran ([@l33tdawg](https://github.com/l33tdawg))

---

<p align="center"><em>A tribute to <a href="http://phenoelit.darklab.org/fx.html">Felix 'FX' Lindner</a> — who showed us <b>how much further curiosity can go.</b></em></p>
