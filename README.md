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

## What’s New in v6.0

- **Dynamic Validator Governance** — Validators can now be added, removed, and have their power updated **without stopping the chain**. Admin agents submit governance proposals, validators vote on-chain with 2/3 BFT quorum, and CometBFT applies validator set changes at consensus level. Zero downtime.
- **On-Chain Governance Engine** — New `internal/governance/` package with deterministic integer-only quorum math, proposal lifecycle (voting → executed/rejected/expired/cancelled), proposer cooldown, min voting period, and power constraints. All state in BadgerDB, included in AppHash.
- **Governance Dashboard** — New Governance section in the CEREBRUM Network page. Active proposal cards with vote tally, quorum progress bar, expiry countdown, and one-click voting. Proposal history with status badges. "New Proposal" wizard for admins.
- **Security Constraints** — 1/3 max power for new validators (prevents single-add takeover), min 2 validators after removal, 50-block proposer cooldown (prevents grief), 500-block max proposal TTL (prevents governance lockup), admin-only proposals, validator-only voting.
- **Foundation for v6.5/v7.0** — This governance layer is required before encrypted node-to-node tunnels (v6.5) and internet federation (v7.0). Without hot validator eviction, internet-facing nodes would require full chain restart to remove bad actors.

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
