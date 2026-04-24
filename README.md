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

## What’s New in v6.6

- **Tags on propose + tag-filtered semantic recall (6.6.0)** — `POST /v1/memory/submit` now accepts `tags`, and `/v1/memory/query` and `/search` accept a `tags` filter (any-match / OR semantics). MCP `toolRemember` drops the old 2-step tag dance — single atomic submit. Python SDK: `propose(tags=...)` and `query(tags=...)`.
- **Offchain SQLITE_BUSY silent-drop fix (6.6.1)** — Under sustained SQLite lock contention, the offchain `Commit()` flush could exhaust its retry budget, log CRITICAL, and silently clear pending writes while BadgerDB had already advanced — CometBFT then skipped replay on restart and the writes were lost invisibly. Now: flush runs *before* BadgerDB state is saved, retry budget raised to 30 attempts, panic on exhaustion so CometBFT replays the block. First surfaced by Level Up: 521 accepted submits with zero new visible memories across 96 hours on 6.5.5.
- **Silent-filter observability (6.6.2)** — `/v1/memory/list`, `/query`, `/search` now set an `X-SAGE-Filter-Applied` header and a `filtered` JSON envelope whenever either silent-hide filter ran. `/list` includes `total_before_filter` + `visible` counts; `/query`/`/search` include `hidden_count`. Empty-domain vs RBAC-filtered is finally distinguishable.
- **Org-clearance-as-seeAll (6.6.2)** — TopSecret (clearance=4) org members bypass the `submitting_agents` RBAC filter automatically. Per-domain access control and per-record classification gates still apply. Closes the `visible_agents="*"` boilerplate for single-org deployments.
- **Admin bootstrap playbook (6.6.2)** — New `docs/ADMIN_BOOTSTRAP.md` documents three deployment patterns (single-org, multi-org federated, homogeneous-trust legacy) with setup commands and the chain-reset visibility gotcha.
- **ABCI healthcheck + chain-bootstrap-window doc (6.6.3)** — `deploy/Dockerfile.abci` ships a HEALTHCHECK with `start_period=5m` to cover the ~3min CometBFT cold-start window on fresh data dirs, so Docker doesn't false-flag containers as unhealthy during normal bootstrap. Doc adds the 503-vs-connection-refused diagnostic and orchestrator guidance for Kubernetes `startupProbe`.

<details>
<summary>Full v6.6.x changelog</summary>

- v6.6.3: ABCI HEALTHCHECK + chain-bootstrap-window doc
- v6.6.2: Silent-filter observability + org-clearance-as-seeAll + admin bootstrap docs
- v6.6.1: Offchain SQLITE_BUSY silent-drop fix (correctness; flush-before-badger reorder)
- v6.6.0: Tags on propose/query + `/v1/agent/register` response field rename to `on_chain_height`

</details>

### v6.5 Highlights

- **Encrypted Node-to-Node Communication (6.5.0)** — REST API TLS support for quorum mode. Per-quorum ECDSA P-256 certificate authority, auto-generated during `quorum-init`/`quorum-join`. Dual-listener pattern: TLS on `:8443` for network traffic, plain HTTP on `localhost:8080` for dashboard/MCP.
- **CometBFT P2P Already Encrypted (6.5.0)** — Verified that CometBFT v0.38.15 encrypts all validator-to-validator gossip via SecretConnection (X25519 DH + ChaCha20-Poly1305). No plaintext memories on the wire.
- **TLS Certificate Infrastructure (6.5.0)** — New `internal/tlsca/` package: CA generation, node cert generation, PEM I/O, TLS config builders. `sage-gui cert-status` CLI for expiry monitoring. Python SDK v6.1.0 adds `ca_cert` parameter.
- **Stuck-proposed deprecation + vote dedup (6.5.1)** — When all validators voted but quorum (2/3) wasn't reached (e.g. 2-2 tie), memories stayed in `proposed` forever and the validator ticker re-voted every 2 seconds (~1.4M redundant txs over 8 days for one stuck memory). Now: deprecate the memory when votes are in but quorum is missed, and track per-session voted memories to prevent re-vote.
- **`/v1/memory/{id}/forget` + SDK `forget()` (6.5.4)** — Closes a semantic gap where "forget" was the user-facing verb across MCP/dashboard/docs but only `/challenge` existed. New endpoint is a thin alias for challenge with an optional reason (defaults to "deprecated by user" — `challenge` requires a non-empty reason, `forget` is forgiving for dedup callers).
- **RBAC ownership theft fix + real broadcast errors (6.5.5)** — Two bugs masqueraded as generic "Failed to broadcast" errors when CometBFT was fine and FinalizeBlock was returning "access denied". Fix: reserve `general` and `self` as shared catch-all domains (never auto-registered), make `RegisterDomain` check-and-set instead of silent overwrite, add `TransferDomain` for explicit admin transfers, and surface the real broadcast error from REST handlers (403 on access-denied instead of generic 500).

<details>
<summary>Full v6.5.x changelog</summary>

- v6.5.5: RBAC ownership theft fix; real broadcast error surfacing
- v6.5.4: `/v1/memory/{id}/forget` endpoint + SDK `forget()` method
- v6.5.3: RBAC regression test backfill for Level Up bug reports
- v6.5.2: CI workaround for GitHub Pages duplicate-artifact errors (reverted in 6.5.3)
- v6.5.1: Deprecate stuck proposed memories when quorum cannot be reached; per-session vote dedup
- v6.5.0: TLS everywhere — encrypted REST API for quorum mode, per-quorum CA

</details>

### v6.0 Highlights

- **Dynamic Validator Governance** — Validators can now be added, removed, and have their power updated **without stopping the chain**. Admin agents submit governance proposals, validators vote on-chain with 2/3 BFT quorum, and CometBFT applies validator set changes at consensus level. Zero downtime.
- **On-Chain Governance Engine** — New `internal/governance/` package with deterministic integer-only quorum math, proposal lifecycle (voting → executed/rejected/expired/cancelled), proposer cooldown, min voting period, and power constraints. All state in BadgerDB, included in AppHash.
- **Governance Dashboard** — New Governance section in the CEREBRUM Network page. Active proposal cards with vote tally, quorum progress bar, expiry countdown, and one-click voting. Proposal history with status badges. "New Proposal" wizard for admins.
- **Security Constraints** — 1/3 max power for new validators (prevents single-add takeover), min 2 validators after removal, 50-block proposer cooldown (prevents grief), 500-block max proposal TTL (prevents governance lockup), admin-only proposals, validator-only voting.

### v5.x Highlights

- **FTS5 Full-Text Search** — Keyword-based recall fallback when embeddings aren’t semantic.
- **Docker Compose** — `docker-compose.sage-gui.yml` with Ollama sidecar for semantic embeddings.
- **Consensus-First Writes** — Memory submissions go through full BFT consensus before appearing in queries.
- **Byzantine Fault Tests in CI** — 4-validator Docker cluster with fault injection.
- **Nonce Replay Protection** — Random nonce in request signing prevents sub-second replay collisions.
- **Docker Env Vars** — `OLLAMA_URL` and `OLLAMA_MODEL` properly configure embeddings in Docker.

<details>
<summary>Full v5.x changelog</summary>

- v5.4.5: Docker env var support for OLLAMA_URL/OLLAMA_MODEL
- v5.4.4: Empty blocks fix for single-node idle timeout prevention
- v5.4.3: Null array fix (return `[]` not `null` for empty results)
- v5.4.2: Nonce verification threaded through full tx pipeline
- v5.4.1: Random nonce for replay protection
- v5.4.0: FTS5 search, Docker Compose with Ollama
- v5.3.x: Consensus-first writes, Byzantine CI tests, Docker hardening, write serialization
- v5.2.x: Immutable RegisteredName, self-updater fix, memory type guidance
- v5.1.0: Agent rename fix, self-healing name reconciliation
- v5.0.x: Agent pipeline, Python SDK, vault recovery, memory modes, MCP identity fix, Docker fix

</details>

### v4.x Highlights

- **4 Application Validators** — Sentinel, Dedup, Quality, Consistency with 3/4 BFT quorum.
- **RBAC** — Agent isolation by default, domain-level permissions, clearance levels, multi-org federation.
- **Synaptic Ledger** — AES-256-GCM encryption with Argon2id key derivation, vault lock/unlock.

### v3.x Highlights

- **Multi-Agent Networks** — Add agents from dashboard, LAN pairing, key rotation, redeployment orchestrator.
- **On-Chain Agent Identity** — Registration, permissions, and metadata through CometBFT consensus.
- **CEREBRUM Dashboard** — Brain graph, focus mode, timeline, search, draggable panels.

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
