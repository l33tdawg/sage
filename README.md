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

## What's New in v4.0

- **4 Application Validators** — Every memory now passes through 4 in-process validators before committing: **Sentinel** (baseline accept, ensures liveness), **Dedup** (rejects duplicate content by SHA-256 hash), **Quality** (rejects noise — greeting observations, short content, empty headers), **Consistency** (enforces confidence thresholds, required fields). Quorum requires 3/4 accept (BFT 2/3 threshold).
- **Pre-Validation Endpoint** — `POST /v1/memory/pre-validate` dry-runs all 4 validators without submitting on-chain. Returns per-validator decisions and quorum result. MCP tools use this to reject low-quality memories before they hit the chain.
- **Memory Quality Gates** — `sage_turn` filters low-value observations (greeting noise, short content). `sage_reflect` detects similar existing memories and skips duplicates. Boot safeguard dedup prevents the same inception reminder from accumulating across sessions.
- **Upgrade Cleanup** — On upgrade from v3.x, automatically deprecates duplicate boot safeguards, noise observations, very short memories, and content-hash duplicates. SQLite is backed up first. ~25-30 noisy memories cleaned per typical install.

### v3.6

- **Brain Graph Click-to-Focus** — Click any memory bubble to focus its domain group. Others fade out while focused memories arrange in a timeline row sorted by creation date. Click again to view detail, click empty space to exit.
- **Interactive Timeline** — Click time buckets at the bottom of the brain graph to filter memories by time range. Multi-select hours to narrow down. Clear button to reset.
- **Draggable Stats Panel** — Grab the "Memory Stats" header to reposition the panel anywhere. Position persists between sessions. Resize horizontally with the drag handle.
- **Chain Activity Log** — Collapsible real-time event stream at the bottom of every page. See memory stored/recalled/forgotten events and consensus votes as they happen. Drag the top edge to resize.
- **Agent Tab Ordering** — Admin agents appear first in brain view tabs for faster access.
- **Renamed to SAGE GUI** — Binary renamed from sage-lite to sage-gui. Upgrade migration handles old launchd plists automatically.

### v3.5

- **On-Chain Agent Identity** — Agent registration, metadata updates, and permission changes go through CometBFT consensus. Every identity operation is auditable, tamper-resistant, and federation-ready.
- **Auto-Registration** — Agents self-register on-chain during their first MCP connection. No manual setup needed.
- **Visible Agents** — Control which agents' memories each agent can see. Set per-agent visibility from the dashboard.
- **`sage_register` MCP Tool** — Agents can register themselves programmatically via MCP.
- **Permission Enforcement** — On-chain clearance levels and domain access are enforced on every memory operation, with BadgerDB as the source of truth.
- **Legacy Migration** — Existing agents auto-migrate to on-chain identity on first boot after upgrade.

### v3.0

- **Multi-Agent Networks** — Add and manage agents from the CEREBRUM dashboard. Each agent gets signing keys, role, clearance level, and per-domain read/write permissions.
- **LAN Pairing** — Generate a 6-character pairing code. New agents fetch their config over your local network in seconds.
- **Agent Key Rotation** — Rotate agent credentials with one click. Memories are re-attributed atomically.
- **Redeployment Orchestrator** — 9-phase state machine handles chain reconfiguration with rollback at every phase.
- **In-App Auto-Updater** — Check for updates, download, and restart from the Settings page.
- **Boot Instructions** — Customize what your AI does on startup from the admin dashboard.
- **Tabbed Settings** — Overview, Security, Configuration, and Update tabs keep everything organized.
- **Brain Graph Search** — Filter memories by content, domain, type, or agent. Only matching bubbles are shown.

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