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

![CEREBRUM MRI brain — memories mapped inside a 3D brain with focused related notes](docs/screen-brain.png)

`http://localhost:8080/ui/` — a dashboard-native operator console centered on the 3D MRI memory brain, with chain health, agents, federation, semantic memory, recall tuning, vault recovery, tasks, imports, and updates around it. Every major workflow is available from the browser; the CLI stays there for automation and recovery.

| Control Board | Federation | Recall Engine |
|:---:|:---:|:---:|
| ![CEREBRUM overview dashboard](docs/screen-overview.png) | ![Federation join dashboard](docs/screen-network.png) | ![Recall engine settings](docs/screen-config.png) |
| Chain health, quorum, agents, federation, and embeddings | LAN-first, human-verified joins between separate SAGE brains, scoped and revocable | Smart-memory setup, managed reranker install, and recall-depth tuning |

The dashboard also includes agent management, domain permissions, key rotation, import/export, software updates, and encryption controls.

---

## What's New in v11.0.3

**Federation actually works between separate nodes now, plus health/observability polish.** v11.0.3 changes **no consensus rule, AppHash, transaction, or key-encoding**; `app-v15` remains the active v11 consensus fork. The one upgrade-time behavior change is a one-time local **network-identity re-mint** on legacy nodes (details below) — your memories are backed up first and preserved.

- **Cross-node federation is fixed.** Every pre-v11 personal node was born with the *identical* network id `sage-personal`, so SAGE's self-federation guard treated any two nodes as the same network and refused every join. On its next boot a legacy node now re-mints a **globally-unique** network id, so two independent nodes can finally connect.
  - **What you'll see on upgrade (legacy nodes only):** a new network id; block height resets to 0 and the chain rebuilds itself (your memories in SQLite are backed up first and never wiped); the dashboard's self-signed HTTPS certificate is regenerated (**a one-time browser certificate warning** — accept it once); if you had already connected to another network, **re-join once**.
  - **Known limitation:** if you ran a **LAN network as host** on v11.0.x and turned Network Mode *off* before upgrading, your node is indistinguishable from a standalone node on disk and will be re-minted — your guests keep all their memories and simply **re-join once**. Guests are never wrongly re-minted.
- **Turning on encryption is discoverable.** The System Status "Synaptic Ledger Encryption" row now has an inline **Enable →** button (opens Settings → Security), instead of a dead-end "Off".
- **Idle chains are explained.** A new operator doc (`concepts/block-production-and-idle.md`) makes clear that a SAGE chain has no heartbeat — an idle chain mints no blocks, and a frozen height with an empty mempool is **healthy, not stuck**. `/v1/dashboard/health` now exposes `chain.idle` / `chain.stuck` / `last_block_age_seconds` so monitors alert on *stuck*, not a still height.
- **Embedding health is visible.** `GET /ready` now reflects the embedding provider: a down semantic embedder reports `degraded` (HTTP 200; `?strict=1` → 503) instead of a misleading `ready`, refreshed by a background watchdog. And `sage_recall` / `sage_turn` results carry `recall_mode` / `semantic_degraded` / `degraded_reason` so an agent knows when recall silently fell back to keyword-only.
- **Mempool backpressure signals.** New `GET /v1/chain/backpressure` (+ an `X-Sage-Mempool-Pct` header on every submit) lets clients pace writes without polling raw CometBFT RPC, and a mempool-full submit returns `429 + Retry-After` (a distinct problem type) instead of an opaque 500.
- **Guaranteed auto-commit is operable.** `--require-voter` / `voter.required` makes a deployment that needs automatic `proposed → committed` flow fail-fast rather than silently run voterless; `sage_voter_running` + `sage_proposed_oldest_age_seconds` metrics and a `/ready` voter block turn a stuck backlog into a first-class alarm; a new `concepts/voter-operations.md` runbook covers per-mode ownership, key safety, quorum math, and triage.
- **Safer upgrades + a security fix.** The pre-upgrade backup is verified by content (integrity + memory row-count parity) rather than file size, and an un-checkpointable write-ahead log aborts the migration instead of being discarded. Archive extraction for the managed Ollama runtime now validates symlink/hardlink targets against the extract root (CodeQL `go/unsafe-unzip-symlink`).

SDK 11.0.3.

## Older releases

<details>
<summary>v11.0.2 — managed Ollama setup + docs polish</summary>

**Smart memory setup now manages Ollama end to end.** v11.0.2 is a patch release on top of v11.0.1: no consensus rule, AppHash, transaction, key-encoding, or migration change. Existing v11 chains update in place; `app-v15` remains the active v11 consensus fork.

- **Managed Ollama runtime for semantic memory.** The CEREBRUM smart-memory wizard can now install a pinned Ollama runtime, start/adopt the local sidecar, pull `nomic-embed-text`, verify the embedding dimension, and remember the managed runtime preference across restarts. This gives Ollama the same dashboard-first setup path as the managed reranker.
- **Setup endpoints are wizard-gated.** The new install/start/pull routes run behind the dashboard setup security gate, and archive extraction refuses traversal, oversized payloads, incomplete downloads, and checksum mismatches before anything becomes active.
- **Security and deployment wording is clearer.** The public Security FAQ now separates SAGE Personal from Enterprise threat models, calls out local BadgerDB/SQLite storage accurately, and tightens the GitHub Pages privacy copy so optional connector traffic is not confused with a SAGE-hosted relay.
- **Docs stay current with v11 code truth.** The reference docs, benchmark READMEs, SDK README, roadmap, and environment-variable notes are updated for the v11.0.2 surface without changing consensus semantics.

SDK 11.0.2.
</details>

<details>
<summary>v11.0.1 — MRI-first CEREBRUM polish + security dependency update</summary>

**CEREBRUM is now fully MRI-first.** v11.0.1 is a launch-polish patch on top of v11.0.0: no consensus rule, AppHash, transaction, key-encoding, or migration change. Existing v11 chains update in place; `app-v15` remains the active v11 consensus fork.

- **MRI is the CEREBRUM view.** The legacy 2D brain option is no longer exposed in the dashboard. CEREBRUM opens directly into the 3D MRI memory brain, with the same offline three.js / 3d-force-graph bundle and anatomical mesh fallback path.
- **Focused memories are clearer and easier to leave.** Clicking a memory brings it into focus with a visible white focus ring, and clicking open space exits the focused train-of-thought view back to all memories.
- **Launch visuals now match the product.** The README leads with the real MRI brain screenshot, and the supporting screenshots are tracked with the docs so GitHub, package archives, and release pages show the correct launch surface.
- **Federation wording is tightened.** v11.0 federation is LAN-first, or reachable over a VPN/tunnel/operator-provided route. First-class internet/NAT traversal remains scoped for v11.5.
- **Security dependency update.** `golang.org/x/net` is bumped to `v0.55.0`, clearing Dependabot alert #6 (`GHSA-5cv4-jp36-h3mw` / `CVE-2026-25680`) in the Go module graph.
- **Docs and SDK metadata are lockstep.** The Python SDK version, reference headers, roadmap status, and MCP/Docker registry metadata are bumped to 11.0.1.

SDK 11.0.1.
</details>

<details>
<summary>v11.0.0 — CEREBRUM, managed reranker, federation join ceremony</summary>

**CEREBRUM becomes a real control board, semantic memory turns on in a few clicks, one click stands up a managed reranker, and two SAGE nodes can now federate their memory over a secure LAN-first join ceremony.** v11.0.0 activates a new `app-v15` consensus fork and ships as a major version: every validator must run this binary and fully converge before the `app-v15` activation height (the auto-vote readiness gate enforces this on the governance path, so an unsupported upgrade never reaches quorum). Every existing chain replays byte-identically until activation (the fork gate is dormant pre-activation), and a node-by-node rolling upgrade is safe: a mixed v10.x / v11.0.0 cluster computes the identical AppHash while `app-v15` is dormant. On personal/single-validator nodes the auto-advance ladder reaches `app-v15` automatically.

- **CEREBRUM dashboard overhaul.** A new top-level **Overview** control board gives you a glanceable, read-only picture of the node: a status banner plus cards for chain health, quorum and nodes, agents, federation, and embeddings, each polling independently so one dead feed never blanks the board. The **3D MRI brain is now the default view**, and it renders fully offline (three.js and 3d-force-graph are bundled locally instead of pulled from a CDN); established memories pull to the core and fresh ones ride to the rim, and clicking a memory blooms its "train of thought" as a labelled constellation with a side panel you can hop through. **Search is real full-text plus semantic** now (FTS5, relevance-ranked, RBAC-scoped) instead of a client-side filter over the newest 100, with status filters (all / committed / proposed / deprecated), corroboration counts, an editable memory domain, and **bulk curation** (multi-select with an action bar). A live **Tasks board** shows agent-vs-human authorship, supports drag-to-status, and uses an atomic compare-and-swap claim so two agents never double-work an assignment, and a **Messages tab** (the agent-to-agent pipeline, merged into Tasks) adds a human-to-agent note composer so a person can drop a note into an agent's inbox without impersonating one. A **first-run onboarding wizard** (welcome, semantic memory, connect an AI tool, pointers) shows only on a fresh node and is re-runnable any time from Settings > Maintenance > Run setup.

- **Semantic memory made effortless.** A "Turn on smart memory" flow switches the node off the keyword-only hash pseudo-embedder onto the bundled Ollama + `nomic-embed-text` (768-dim): it detects Ollama, downloads the model if missing, re-embeds your existing memories with a live **progress bar** (resumable, vault-gated, runs in the background), then restarts so every consumer picks it up. Memories orphaned by a past vault re-initialization (encrypted under a previous data key and undecryptable now) can be **recovered by re-keying in place**: paste the old recovery key, preview "X of N", and recover, with no new IDs and no new consensus records, since only content and embedding are encrypted while the content hash stays plaintext-derived. Deprecated memories are now audit-only and never surface in CEREBRUM.

- **One-click managed reranker.** SAGE gives the reranker the Ollama treatment: with one consent click it downloads a pinned llama.cpp release build itself (sha256-verified before any byte touches disk) and the `bge-reranker-v2-m3` GGUF (Q8_0, 636MB, sha256-verified, atomic install so a truncated or tampered file never lands), then spawns and manages a `llama-server` sidecar on loopback that serves a real cross-encoder `/v1/rerank`. It survives node restarts (a healthy survivor is adopted via a real rerank probe rather than blindly respawned, with a probe-before-kill guard on shutdown). The whole thing is a **zero-terminal** hands-off checklist (engine, model, start, done), and recall `k` is now tunable from **3 to 20** (was 4 to 10) with copy that explains the token cost and flips its guidance based on whether the reranker is actually on.

- **Federation v2.** Two SAGE nodes can now share memory on the same LAN, or over connectivity you explicitly provide, established through a **secure join ceremony**. First-class internet/NAT traversal is scoped for v11.5, not v11.0. The v11 ceremony uses RFC-6238 TOTP-based mutual verification with a QR enrollment plus spoken 6-digit confirm codes, a pin-bound short-authentication-string that provably diverges if an enrollment is relayed, and a fail-closed version gate. Two modes, both consent-gated with a "nothing is deleted" guarantee: **exchange mode** keeps foreign data on its owner's chain and queries it live off-consensus over a pinned mTLS federation listener and query proxy, and **co-commit mode** writes native memories on both chains, each ratified by its own chain and cross-anchored by a hash of the other side's signed commit receipt (you remember and I remember, each on our own chain). Guided guest and host wizards make "add another computer to my SAGE network" an end-to-end dashboard flow.

- **`app-v15` consensus fork.** The fork that makes federation v2 real on-chain: new co-commit transaction types (`CoCommitSubmit` / `CoCommitAttest`) and cross-federation exchange-terms types (set / revoke), a co-commit envelope validity window bound to jointly-signed times and to federation status, and an **access-grant verb ladder** that makes the level-3 "modify" verb grantable and requestable. It also **tightens the authorization gates** on existing consensus handlers as a hardening pass. Every one of these rules derives purely from committed state and the consensus block time (no wall clock, no per-node cache, no map-iteration order), so every replica reaches the same verdict; all of it is byte-identical pre-activation and reached through the same governed upgrade ladder every prior fork uses (auto-advanced on personal nodes, governance-activated on a quorum).

- **Quality.** New memories now stamp their embedding provider at insert, so a freshly-written memory stops posing as unembedded and the "needs re-reading" counter no longer creeps up forever over real vectors. Redeploy got a robustness pass: a single-validator agent add/remove no longer runs the destructive wipe-and-restart that could brick a personal node, a stuck "reconfiguration in progress" banner can no longer wedge forever, and redeploy status reports the real terminal outcome instead of flashing a false success. Underneath it all are dozens of fixes from multi-pass adversarial find-and-verify reviews across the consensus, transport, web, frontend, and crypto surfaces.

SDK 11.0.0.
</details>

<details>
<summary>v10.9.1 and earlier — full per-version changelog on the <a href="https://github.com/l33tdawg/sage/releases">Releases page</a></summary>

The v10.x line (MRI 3D brain, the app-v12/v13/v14 idle-block + AppHash fork ladder, multi-node-safe voting, per-domain read-ACLs) and the full v3–v9 history — consensus-first writes, PoE-weighted quorum, governance-gated upgrades, TLS, RBAC/multi-org, hybrid recall — are on the [Releases page](https://github.com/l33tdawg/sage/releases).

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
