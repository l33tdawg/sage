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

## What's New in v8.2

PoE-weighted quorum activation. The PoE engine has been computing per-validator engagement scores every epoch since v6.x, but `checkAndApplyQuorum` ignored them and used a hardcoded `weights[v.ID] = 1.0` — so quorum was effectively a 2/3 *majority*, not a 2/3 *weighted vote*. v8.2 closes that gap with a single fork-gated swap. Pre-fork blocks (and any v8.1.x chain) replay byte-identical to v8.1.2. Existing chains upgrade in place: the watchdog auto-proposes activation at `app-v3`, the fork takes effect at H+1, post-fork quorum reads each validator's persisted PoE weight.

- **PoE weights drive quorum (post-fork).** `checkAndApplyQuorum` consults `v.PoEWeight` instead of `1.0` on post-fork blocks via `app.postV8_2Fork(height)`. Strict greater-than mirrors CometBFT's "applied at H+1" semantic. Pre-fork keeps the equal-weight branch so v8.1.x chains and the activation block itself replay byte-identical. `recordV8_2Branch` emits `sage_fork_branch_total{fork="v8.2",branch="pre|post"}` so operators see the gate flip live on the same dashboard that tracked v8.0's cutover.
- **On-chain weight persistence (`poew:current` + `poew:<validatorID>`).** Every epoch boundary post-fork, `processEpoch` writes the normalized weight set atomically: a uvarint epoch marker plus one IEEE-754 big-endian float64 per validator. Validators removed via governance are pruned from `poew:*` in the same txn — no stale weight survives a set change. A node restart between epoch boundaries hydrates `PoEWeight` from BadgerDB via `refreshPoEWeights()` on boot, so two peers restarting at different points between the same two epoch boundaries don't diverge consensus.
- **Bootstrap fallback (1/N).** `poeWeightOrFallback` returns `1/N` when `PoEWeight == 0` — three cases hit it: pre-first-epoch chain (no `poew:*` keys yet), mid-epoch governance add (validator added between boundaries), and defensive guard for a missing `poew:<id>` entry. Picking `1/N` over a fixed `1.0` keeps the fallback contribution in the same numeric range as `NormalizeWeights`' output so a mid-epoch validator add doesn't suddenly weigh more than peers whose scores have been RepCap-capped at 0.10. Quorum decisions are ratio-only (`HasQuorum`: `acceptWeight/totalWeight >= 2/3`), so the choice doesn't move the threshold — the win is legibility of the `acceptWeight` / `totalWeight` audit log lines.
- **CometBFT v0.38.15 → v0.38.23.** Lands in the same release for hygiene: GHSA-hrhf-2vcr-ghch (v0.38.19), blocksync / nil-vote / ABCI-socket hardening, and the configurable `max-snapshot-chunks` parameter added in v0.38.21. Backward-compatible patch series — no ABCI API changes, no consensus-rule changes. Independent of the PoE fork-gate.
- **Test coverage.** 16 new tests across four files. Store-side W1-W5 pin the encoding (0.25 → `3F D0 00 00 00 00 00 00`), the round-trip, the stale-validator pruning, and the atomicity-on-malformed-input contract. F1-F6 pin every quorum branch: pre-fork equal weights, post-fork weighted, cold-boot fallback, mid-epoch validator add, gate flip at H_act+1, and end-to-end normalized-ratio matching. L1-L3 prove validator lifecycle through close/reopen. R1-R2 prove pre-fork blocks produce no `poew:*` keys (so AppHash matches v8.1.2's keyspace) and post-fork blocks diverge as expected. Live 4-validator devnet on the v8.2-dev binary held byte-identical AppHash across all 4 validators for 160+ blocks and two epoch boundaries.
- **Phase-1 remnants explicitly carved out for follow-on.** `domainScore` still hardcoded to 0.5; corroboration still uses a default. The math in `poe.ComputeWeight` already weights both — the inputs come from cold-start priors until per-domain expertise tracking (v8.4+) and per-validator corroboration counts (v8.4+) land.

## Older releases

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
