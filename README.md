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

## What’s New in v5.3.0

- **Consensus-First Write Ordering** — Memory submissions now go through full BFT consensus before appearing in the query layer. Previously, the REST handler wrote to the offchain store immediately after broadcasting; now it uses `broadcast_tx_commit` and a supplementary data cache so ABCI `Commit` is the sole write path. Eliminates the pre-consensus data visibility window flagged in the security FAQ.
- **Byzantine Fault Tests in CI** — `make byzantine` target + GitHub Actions job that spins up a 4-validator Docker cluster and runs fault injection tests (1-node down, 2-node halt, recovery).
- **Docker Security Hardening** — ABCI containers now run as non-root (`sage` user). New `docker-compose.prod.yml` override with PostgreSQL SSL, restricted CORS, read-only root filesystems, and localhost-only port bindings.
- **Benchmark Reproducibility** — Python benchmark (`make benchmark`) now generates real Ed25519 keypairs and signs requests with the correct canonical format. No more placeholder auth tokens.
- **Multi-Node Upsert Safety** — `InsertMemory` upserts in both PostgreSQL and SQLite now use `COALESCE` to preserve non-NULL supplementary data (embeddings, provider) regardless of write order across validators.

### v5.2.2

- **Memory Type Guidance** — `sage_remember` tool description now guides agents to use the `fact` type for infrastructure details (IPs, hostnames, SSH commands) instead of `observation`, preventing decay across provider boundaries.

### v5.2.1

- **Self-Updater Fix** — Dashboard "Update" button no longer fails. GitHub changed release asset downloads to route through `release-assets.githubusercontent.com`, which wasn’t in the redirect allowlist.

### v5.2.0

- **Immutable RegisteredName** — Agents now have a permanent `RegisteredName` (set once at registration) alongside the mutable display `Name`. Renames preserve the original on-chain identity for audit trail and provenance. Dashboard shows "Registered As" when names differ.
- **Lazy Backfill** — Pre-v5.2.0 agents automatically get their `RegisteredName` populated from their current name on first access.

### v5.1.0

- **Agent Rename Fix** — Renaming an agent in the dashboard (Network → Agents → Edit) now reliably syncs to on-chain state. Previously, the CometBFT broadcast was fire-and-forget and could silently fail, causing `sage_inception` to return the old auto-generated name.
- **Self-Healing Name Reconciliation** — If on-chain and display names ever diverge, `sage_inception` automatically detects and repairs the mismatch on the agent’s next boot.
- **Broadcast Error Feedback** — The dashboard now warns you if an on-chain sync fails instead of silently swallowing the error.

### v5.0.12

- **MCP Identity Fix** — `sage-gui mcp` and `sage-gui mcp install --token` now respect `SAGE_IDENTITY_PATH` environment variable as the **highest priority** (exactly matching the SDK’s `AgentIdentity.default()`).
- Auto-creates directories + keypair if missing. Added clear INFO logs. 100% backward compatible.
- Enables clean multi-agent setups (e.g. multiple Claude Code instances in tmux). Closes the identity collision issue.

## What’s New in v5.0.11

- **Docker Fix** — Container no longer stuck in restart loop. Default entrypoint changed from MCP stdio mode to `serve` (persistent REST API + dashboard). MCP stdio still available via `docker run -i ghcr.io/l33tdawg/sage mcp`. Fixes #14.

### v5.0.10

- **Multi-Agent Identity** — New `SAGE_IDENTITY_PATH` env var and `AgentIdentity.default()` for running multiple Claude Code agents on the same machine without key collisions. (Community PR by @emx)
- **Dashboard Fix** — "Synaptic Ledger" label in overview settings now reads "Synaptic Ledger Encryption" to clarify it refers to the encryption state, not the ledger itself.

### v5.0.9

- **Upgrade Hang Fix** — Fixed CometBFT startup hang after drag-and-drop upgrades. Stale consensus WAL files left behind during migration caused a 60-second timeout and prevented the REST API from starting. Now cleaned up automatically at both migration and startup time.

### v5.0.7

- **Agent Pipeline** — Inter-agent message bus (`sage_pipe`) for direct agent-to-agent communication. Send messages, check results, coordinate work across agents in real-time.
- **Python Agent SDK** — `sage-agent-sdk` on PyPI with full v5 API coverage for building SAGE-integrated agents. CI-tested on every release.
- **Vault Recovery** — Reset your Synaptic Ledger passphrase using a recovery key. No more permanent lockouts.
- **Memory Modes** — Choose `full` (every turn), `bookend` (boot + reflect only), or `on-demand` (zero automatic token usage) to control how much context your agent spends on memory.
- **Vault Key Protection** — Vault key is automatically backed up on every upgrade and in-app update. Prevents the silent overwrite that could cause permanent memory loss.
- **macOS Tahoe Compatibility** — Fixed Gatekeeper warnings and launch failures on macOS 15.x. Removed the `Install SAGE.command` that triggered quarantine blocks.
- **Linux ARM64 Containers** — Docker images now build for `linux/arm64` in addition to `amd64`.
- **`/v1/mcp-config` Endpoint** — Agents can self-configure their MCP connection without manual setup.
- **Docker Images** — Every release auto-builds and pushes to `ghcr.io/l33tdawg/sage`. Pin a version or pull `latest`.

### v4.5

- **Cross-Agent Visibility Fixed** — Org-based access (clearance levels, multi-org federation) now correctly grants visibility across agents. Queries and list operations check direct grants, org membership, and unregistered domain fallback — no more 0-result queries when clearance should allow access.
- **Domain Auto-Registration** — First write to an unregistered domain auto-registers it with the submitting agent as owner and full access granted. No more propose-succeeds-but-query-404.
- **RBAC Gate Simplification** — DomainAccess (explicit allowlist) and multi-org gates are alternatives, not stacked. Passing one skips the other.

### v4.4

- **CEREBRUM UX Overhaul** — Snap-back physics (nodes spring back to cloud on focus exit), forget animation (fade-and-remove instead of full reload), tab backgrounding fix (no physics jumps after alt-tab).
- **Clean Synaptic Ledger** — Always-visible button with double-click confirmation. Cleanup toggle auto-saves.
- **Focus Mode** — Single-click to view memory detail, side panel closes on exit. Graph defaults to committed status.

### v4.3

- **Synaptic Ledger Safeguards** — Three-layer defense against silent encryption downgrade: server auto-re-enables if vault.key exists, web login now actually unlocks the vault for writes (was a bug), and the native macOS app icon prompts for your passphrase before launch. Plaintext writes are blocked when the vault is locked.
- **Vault-Locked API** — `/v1/dashboard/health` now exposes `vault_locked` status. MCP tools (`sage_remember`, `sage_turn`, `sage_reflect`) check this flag and return clear errors telling agents to prompt the user to unlock via CEREBRUM — no more silent plaintext fallback.
- **Isolated-by-Default RBAC** — Agents can only see their own memories by default. Domain-level read/write permissions, clearance levels, and multi-org federation with department filtering.
- **Bulk Operations** — Multi-select memories in CEREBRUM for bulk domain moves, tag additions, and agent reassignment.
- **Dashboard Update Check** — Long-open tabs now poll for new releases every 12 hours so you never miss an update.
- **Automated Docker + MCP Registry** — Release CI now auto-builds Docker images, pushes to GHCR, and updates `server.json` — MCP registries get new versions without manual intervention.

### v4.0

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

### Docker

```bash
docker pull ghcr.io/l33tdawg/sage:latest
docker run -p 8080:8080 -v ~/.sage:/root/.sage ghcr.io/l33tdawg/sage:latest
```

Pin a specific version with `ghcr.io/l33tdawg/sage:5.1.0`.

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
