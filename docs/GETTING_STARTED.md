# Getting Started with SAGE Personal

SAGE Personal gives your AI assistant (Claude, ChatGPT) persistent memory across conversations. One binary, no Docker, no databases to manage — just install, connect, and your AI remembers everything.

### Why SAGE instead of a "memory skill" from the internet?

Those third-party MCP "skills" and plugins floating around? They get full access to your conversations, your files, your API keys. Some phone home. Some inject prompts. Some store your data on servers you've never heard of. And when they break, your AI starts doing things you didn't ask for.

SAGE runs **entirely on your machine**. Your memories live in a SQLite file in your home directory. Nothing leaves your laptop unless you explicitly configure a cloud embedding provider. The MCP server is a local binary that talks to a local database. No accounts, no telemetry, no surprises.

And because SAGE uses real consensus infrastructure (not just a JSON file), your memories have cryptographic integrity, confidence scoring, and a full audit trail. If you ever need to scale to a team or organization, the same protocol works — just add validators.

---

## Quick Install

### From Source (Go 1.24+)

```bash
git clone https://github.com/l33tdawg/sage.git
cd sage
go build -o sage-gui ./cmd/sage-gui/
sudo mv sage-gui /usr/local/bin/  # or add to your PATH
```

### Verify Installation

```bash
sage-gui version
# sage-gui v11.6.0
```

---

## Setup

### Option A: GUI Setup Wizard

```bash
sage-gui setup
```

This opens a browser-based wizard that walks you through:
1. Choosing your embedding provider (OpenAI, Ollama, or hash-based)
2. Entering your API key (if using OpenAI)
3. Generating your MCP configuration

### Option B: Manual Setup

Create `~/.sage/config.yaml`:

```yaml
# Ollama — smart search, fully private (recommended)
embedding:
  provider: ollama
  base_url: http://localhost:11434
  model: nomic-embed-text
  dimension: 768
rest_addr: ":8080"
```

Or for zero-setup (no Ollama needed):

```yaml
# Hash-based — keyword matching only, works offline
embedding:
  provider: hash
  dimension: 768
rest_addr: ":8080"
```

---

## Start the Node

```bash
sage-gui serve
```

You'll see:

```
SAGE Personal is running!
Dashboard: http://localhost:8080/ui/
REST API:  http://localhost:8080/v1/
```

Open the dashboard in your browser to see the Brain Visualization: the **MRI** brain, a 3D, brain-shaped map of your AI's memory. Click any memory to see its "train of thought", click open space to reset back to all memories, click a domain lobe to drill in, and expand the Chain Activity bar to see real-time consensus events. See [BRAIN.md](BRAIN.md) for a full guide to reading it.

---

## First run: the onboarding wizard

The first time you open a fresh node's dashboard (empty brain, setup not yet done), SAGE greets you with a short **onboarding wizard**. It is three steps, and it walks you straight from an empty brain to working memory:

1. **Welcome** - a one-screen orientation to CEREBRUM, the dashboard where you watch and curate everything your AI tools remember.
2. **Semantic memory** - turn on recall-by-meaning (the guided embedder setup, below). Optional, but it is the single biggest recall upgrade, so the wizard recommends it.
3. **Connect an AI tool** - wire up Claude Code, Codex, Cursor, ChatGPT, or another client so it gets persistent memory.

Closing the wizard at any point marks onboarding done, so it never nags you again. You can **re-run it any time** from **Settings > Maintenance > Run setup** - handy if you skipped a step or want to connect another tool later. The wizard reuses the real setup screens (it does not duplicate them), so whatever you do inside it is the same thing you would do from Settings.

---

## Turn on smart (semantic) memory

Out of the box SAGE recalls by **keywords**. Turning on the bundled semantic embedder lets your agents find memories by **meaning** even when the wording differs. This is the biggest recall upgrade you can make, and it stays fully local.

Open the guided setup from the onboarding wizard, or from **Settings > Recall**. SAGE walks a non-technical path end to end:

1. **Install and start Ollama** - SAGE downloads a pinned Ollama runtime for your platform, verifies the asset checksum, installs it under `~/.sage/`, starts it on loopback, and proceeds only after the local API answers.
2. **Download the model** - it pulls `nomic-embed-text` (the embedder) through Ollama with live progress if you do not already have it.
3. **Re-embed your existing memories** - anything already in your brain is re-embedded so old and new memories are searchable the same way. This runs as a **background job on the node**: it survives closing the modal, reloading, or switching tabs, and a progress banner keeps you posted in every tab. Memories that cannot be read or embedded are surfaced honestly rather than silently skipped, and you can re-run setup to retry them.
4. **Switch recall over** - SAGE flips the node to the semantic embedder (a brief restart) and you are done.

The embedder is locked to **Ollama + nomic-embed-text** - this screen turns the bundled one on, it is not a "pick your own embedder" menu. Everything stays on your machine; nothing is sent to any cloud.

---

## The managed reranker (one click)

A **reranker** re-scores your recall results with a cross-encoder so the most relevant memories rise to the top. It is a recommended extra on top of semantic memory, and SAGE now sets it up for you with **one click** - no terminal, no package manager, nothing to install by hand.

From the onboarding wizard, or **Settings > Recall**, choose "Set up the reranker". After a single consent click, SAGE does all of this itself:

- **Downloads the engine** - a pinned [llama.cpp](https://github.com/ggml-org/llama.cpp) release build (Ollama has no rerank endpoint, so SAGE runs its own sidecar). It is **checksum-verified** (sha256) so a tampered or truncated download never lands.
- **Downloads the model** - the pinned `bge-reranker-v2-m3` cross-encoder GGUF, also checksum-verified, a one-time download (the engine plus model is roughly 650 MB total).
- **Runs and manages the process** - SAGE starts the sidecar on loopback, switches recall over to it, and **manages the process from then on**. Nothing leaves your machine.

The whole thing runs on the node, so it survives a dropped browser: if you close the tab mid-download, setup picks up where it left off next time. Each step is idempotent, so a retry resumes rather than restarts.

Prefer to bring your own? You can still point SAGE at any external TEI-compatible rerank server from **Settings > Recall**.

### Tuning recall (k)

**Settings > Recall** also exposes **results per query (k)**, adjustable from **3 to 20**. This is how many memories each recall returns. Higher values pair especially well with the reranker: retrieve a broader candidate set, then let the cross-encoder re-score it and float the best few to the top. A minimum-confidence slider (50-100%) filters by earned, decaying consensus confidence.

---

## Connect an AI tool

The dashboard's **Connect an AI tool** flow (in the onboarding wizard, and on the Settings > Connection tab) writes the config for you. It first asks the one question that matters - is the tool on **this** computer, or **another** one - and then branches into three flows:

### 1. Same machine, one click

For Claude Code, Codex, Cursor, Windsurf, or Claude Desktop running on the same computer as SAGE. Pick the tool, give a project folder if it is a per-project client (Claude Code, Codex, Cursor), and click Connect. SAGE **writes the config file itself** and the agent registers its own on-chain identity on first connect. Restart the tool and it picks up its SAGE connection. You manage its identity and permissions afterward on the Agents page.

### 2. Remote MCP (ChatGPT or another computer)

For ChatGPT, the dashboard uses **OpenAI Secure MCP Tunnel**. CEREBRUM downloads the pinned `tunnel-client` release if needed, writes the local profile for the running SAGE app, and starts the tunnel daemon in the background. You only open the OpenAI Platform and ChatGPT browser tabs, paste the real tunnel ID/runtime key, and click **Set up and start tunnel**. SAGE does not store the runtime key. For other tools on a different machine, use the LAN/VPN flow or a reachable HTTPS endpoint you manage. Bearer-only clients that run on the same machine (Cursor, Cline, Claude Desktop) can also connect directly to `https://localhost:8443` without a tunnel.

### 3. LAN pairing (another computer on your network joins your SAGE)

For making another computer on your local network a **node in your SAGE network** - it runs its own SAGE and shares this node's memory. The host mints a **pairing code (shown as a QR plus a short code)** over a temporary LAN listener that only exists while a pairing is active. The joining machine proves it holds the code, then the two sides **compare a six-digit code** to confirm they reached each other and not an impostor. Only after that does the host hand over the (encrypted) join bundle. The session expires in about 10 minutes if you leave it. This is a peer node, not a validator; it shares the same memory ledger.

> The older per-agent LAN pairing (`sage-gui pair CODE`) is still available from the **Agents** page for wiring up a single agent identity on another machine - see [Multi-Agent Network](#multi-agent-network) below.

---

## Connect to Claude Desktop

This is the killer feature. Claude Desktop can read and write memories directly via MCP (Model Context Protocol). The **Connect an AI tool** flow above writes this config for you; the manual steps below are the reference if you would rather do it by hand.

### 1. Get your MCP config

```bash
sage-gui setup
# Or manually create it:
```

```json
{
  "mcpServers": {
    "sage": {
      "command": "/usr/local/bin/sage-gui",
      "args": ["mcp"],
      "env": {
        "SAGE_HOME": "~/.sage"
      }
    }
  }
}
```

> `SAGE_HOME` sets SAGE's data directory (default `~/.sage`). For the full list of
> environment variables — embeddings, hybrid recall, TLS, snapshots, and more —
> see [`reference/environment-variables.md`](reference/environment-variables.md).

### 2. Add to Claude Desktop

1. Open **Claude Desktop** > **Settings** > **Developer**
2. Click **Edit Config** under MCP Servers
3. Paste the JSON above
4. **Restart Claude Desktop**

### 3. Start using it

Just chat normally. Claude now has 13 memory tools:

| Tool | What it does |
|------|-------------|
| `sage_inception` | Initialize your AI's consciousness — run this first! |
| `sage_remember` | Store a memory (fact, observation, or inference) |
| `sage_recall` | Search memories by text (semantic similarity) |
| `sage_reflect` | End-of-task reflection — store what went right AND wrong |
| `sage_forget` | Mark a memory as deprecated |
| `sage_list` | Browse memories with filters |
| `sage_timeline` | View memories in a time range |
| `sage_status` | Check memory store health and stats |
| `sage_turn` | Per-turn memory cycle — recalls context and stores observations atomically |
| `sage_register` | Register an agent on-chain (auto-called on first connection) |
| `sage_task` | Create and manage persistent task items |
| `sage_backlog` | View and prioritize your task backlog |
| `sage_red_pill` | Deprecated alias for sage_inception |

### First Time: Inception

The very first time your AI connects to SAGE, tell it:

> **You:** Call sage_inception to initialize your memory.

This seeds your AI's brain with foundational memories about how to use its memory system. From then on, the MCP server's initialization instructions tell the AI to automatically:
1. **Recall** relevant context at the start of every task
2. **Remember** important learnings during work
3. **Reflect** on what went right and wrong after tasks complete

### The Feedback Loop (Why This Works)

Research (Paper 4) proved that agents with institutional memory achieve statistically significant improvement over time (Spearman rho=0.716, p=0.020), while memoryless agents show no learning trend (rho=0.040, p=0.901). The key mechanism is storing BOTH successes and failures:

- **DOs**: approaches that worked, patterns to repeat (stored as high-confidence facts)
- **DON'Ts**: mistakes made, approaches that failed (stored as observations to avoid)

Both make the AI better. The `sage_reflect` tool captures this at the end of each significant task.

**Example conversation:**

> **You:** Read this architecture doc and remember the key decisions.
>
> **Claude:** *[uses sage_remember to store 5 key architecture decisions]*
>
> **You** (next conversation): What were the architecture decisions from that doc?
>
> **Claude:** *[uses sage_recall to find the stored decisions]*
> Based on what I remember: 1. We chose microservices over monolith because...

### Upgrading from an older version?

If you installed SAGE before v4.5 and your AI isn't calling `sage_turn` every turn or `sage_inception` on startup, you're likely missing the **Claude Code hooks** that were added in later versions. These hooks enforce the memory lifecycle automatically — no manual prompting needed.

Re-run the installer in each project directory where you use SAGE:

```bash
cd /path/to/your/project
sage-gui mcp install
```

This is safe to run on existing installs — it won't overwrite your `.mcp.json`, but it will add/update the hook scripts and permissions that make everything work reliably. Restart your Claude Code session after running this.

**What the hooks do:**
- **Boot hook** (`SessionStart`) — tells the AI to call `sage_inception` at the start of every session
- **Turn hook** (`PreCompact`, `Stop`, `PostToolUse`) — reminds the AI to call `sage_turn` so memories are flushed before context loss
- **Permissions** — auto-allows all SAGE MCP tools so the AI doesn't need to ask permission each time

---

## Connect to ChatGPT

Open **Connect an AI tool > ChatGPT** in CEREBRUM. SAGE manages OpenAI's `tunnel-client` in the background - no visible terminal - then ChatGPT connects to that OpenAI Secure MCP Tunnel. The advanced command block is only there as a fallback for operators who want to run the daemon by hand.

---

## Brain Dashboard

Open `http://localhost:8080/ui/` to see the Brain Dashboard:

- **Neural Network View** — memories as glowing nodes, connections lighting up on recall
- **Timeline** — horizontal scrubber showing memory activity over time
- **Search** — semantic search across all memories
- **Stats** — total memories, domains, and activity

The dashboard updates in real-time via Server-Sent Events. When your AI stores or recalls a memory, you'll see it animate on screen.

---

## Multi-Agent Network

Once you have SAGE running for yourself, you can add more agents — other machines, family members, teammates, or dedicated AI assistants with different roles.

### When to add agents

- **Multiple machines** — Your laptop and desktop sharing the same memory
- **Family or household** — Each person gets their own agent with separate permissions
- **Small team** — Developers, researchers, or collaborators working with shared knowledge
- **Specialized AI assistants** — A coding agent, a research agent, and a writing agent, each with access to different memory domains

### Adding an agent via the CEREBRUM dashboard

Open `http://localhost:8080/ui/` and go to the Network tab. The wizard walks you through four steps:

1. **Name & Role** — Give the agent a name (e.g., "Work Laptop") and pick a role: Admin, Validator, Writer, Reader, or Observer
2. **Clearance Level** — Set the clearance tier (Guest through Top Secret) to control how much the agent can see and do
3. **Domain Access** — Use the visual matrix to toggle read/write access per knowledge domain (e.g., "security" read+write, "personal" read-only)
4. **Confirm** — Review the config and click Create. The agent gets its own Ed25519 identity automatically

### LAN pairing quick setup

The fastest way to connect a new machine:

1. On your main SAGE node, click **Add Agent** and select **LAN Pairing**
2. You get a pairing code (valid for 15 minutes)
3. On the new machine, run `sage-gui pair ABC123` (replacing with your code)
4. The new machine automatically receives its config, keys, and connects to your network

No port forwarding, no config files to copy, no keys to email around. Everything happens over your local network.

### Domain access configuration

Domains are the knowledge categories your agents work with (e.g., "security", "finance", "personal", "code"). For each agent, you control:

- **Read access** — Can the agent query memories in this domain?
- **Write access** — Can the agent propose new memories to this domain?

Set these per-domain from the Access Control tab on any agent's card in the dashboard. Changes take effect immediately — no restart needed.

A few practical examples:
- Your personal laptop: full read+write on everything
- A shared family machine: read+write on "household", read-only on "work"
- A guest device: read-only on "public", no access to anything else

### On-Chain Agent Identity (v3.5)

Starting in v3.5, agent identity is a first-class on-chain concept. When you add an agent — whether via the dashboard, REST API, or MCP — the registration goes through CometBFT consensus.

**What this means for you:**
- Every agent registration is cryptographically signed and committed to the chain
- Identity changes (name, bio, permissions) are auditable on-chain
- Agents auto-register on their first MCP connection — no manual setup required
- The REST API provides full agent management: `POST /v1/agent/register`, `PUT /v1/agent/update`, `PUT /v1/agent/{id}/permission`
- Existing agents from pre-v3.5 are automatically migrated to on-chain identity on first boot

**Visible Agents:** You can restrict which agents' memories are visible to a given agent. Set this in the agent's Access Control tab on the Network page. By default, all agents can see all memories (open model). Set specific agent IDs to restrict visibility.

### Using Custom Identity Paths (Multiple Agents on the same machine)

You can now run multiple independent agents cleanly with one environment variable:

```bash
# In each tmux tab (example)
SAGE_IDENTITY_PATH=~/.sage/identities/agent-01.key   claude-code --project myproject
SAGE_IDENTITY_PATH=~/.sage/identities/agent-02.key   claude-code --project myproject
SAGE_IDENTITY_PATH=~/.sage/identities/agent-03.key   claude-code --project myproject
SAGE_IDENTITY_PATH=~/.sage/identities/agent-04.key   claude-code --project myproject
```

Each agent automatically gets its own permanent key file and unique Agent ID, while still sharing the same memory ledger.

---

## Using your own Ollama for Local Embeddings

The guided smart-memory setup above is the recommended path: SAGE installs, starts, and checks the local Ollama runtime for you. If you prefer to manage Ollama yourself, you can still configure SAGE manually:

1. Install and start Ollama from https://ollama.ai
2. Pull the embedding model:
   ```bash
   ollama pull nomic-embed-text
   ```
3. Configure SAGE:
   ```yaml
   embedding:
     provider: ollama
     base_url: http://localhost:11434
     model: nomic-embed-text
     dimension: 768
   ```

This gives you semantic search without any data leaving your machine.

---

## Embedding Providers

Privacy first. Your memories never leave your machine.

| Provider | Quality | Privacy | Cost | Setup |
|----------|---------|---------|------|-------|
| **Ollama** | Smart semantic search | Fully local | Free | Guided SAGE setup |
| **Hash** | Keyword matching only | Fully local | Free | Nothing needed |

The hash provider generates deterministic pseudo-embeddings from text hashes. It works offline with zero setup but doesn't provide semantic similarity. Upgrade to Ollama when you want your AI to find related memories even with different wording.

---

## Data & Storage

All data lives in `~/.sage/`:

```
~/.sage/
├── config.yaml          # Your configuration
├── agent.key            # Ed25519 identity key (auto-generated)
└── data/
    ├── sage.db           # SQLite database (all memories)
    ├── badger/           # On-chain state (hashes, consensus)
    └── cometbft/         # CometBFT node data
```

### Backup

```bash
# Backup your memories
cp ~/.sage/data/sage.db ~/sage-backup-$(date +%Y%m%d).db

# Backup everything
tar czf ~/sage-backup-$(date +%Y%m%d).tar.gz ~/.sage/
```

### Reset

```bash
# Remove all data and start fresh
rm -rf ~/.sage/data/
sage-gui serve  # Reinitializes automatically
```

---

## How It Works

Under the hood, SAGE Personal runs a real BFT consensus engine (CometBFT) with a per-node memory auto-voter that signs votes with the node's own consensus key. Every memory goes through the full governance pipeline:

1. **Propose** — memory submitted via MCP or REST API
2. **Pre-Validate** — 4 application validators vote independently:
   - **Sentinel** — baseline accept (ensures liveness)
   - **Dedup** — rejects duplicate content by SHA-256 hash
   - **Quality** — rejects noise (greeting observations, short content, empty headers)
   - **Consistency** — enforces confidence thresholds and required fields
3. **BFT Quorum** — 3 of 4 validators must accept (meets 2/3 BFT threshold)
4. **Commit** — each validator signs a vote transaction broadcast through CometBFT, memory written to SQLite with on-chain hash in BadgerDB

This means your personal SAGE instance uses the exact same consensus protocol as a multi-validator production deployment, with real quality gates preventing noise from accumulating. If you later want to upgrade to a team setup with additional validators, your data and tooling are already compatible.

### Upgrading from v3.x

On first launch after upgrading, SAGE automatically:
- **Backs up** your SQLite database to `~/.sage/backups/`
- **Resets** chain state (BadgerDB + CometBFT) for the new validator architecture
- **Cleans up** noisy memories accumulated before quality gates existed:
  - Duplicate boot safeguards (keeps newest)
  - Greeting/session observations ("user said hi", "brain online", etc.)
  - Very short observations (< 20 characters)
  - Duplicate content hashes (keeps newest)
- All your substantive memories are preserved. The chain rebuilds automatically.

---

## Migrating to Full SAGE

When you outgrow personal mode and need multi-agent BFT consensus:

1. Export your memories: they're in standard SQLite
2. Set up the full 4-node deployment: `make init && make up`
3. Import memories into the PostgreSQL-backed production cluster

See the main [README](../README.md) for the full multi-node deployment guide.

---

## Troubleshooting

**sage-gui serve fails with "address already in use"**
Another process is using port 8080. Either stop it or change the port in `~/.sage/config.yaml`:
```yaml
rest_addr: ":8081"
```

**Claude Desktop doesn't show SAGE tools**
1. Make sure `sage-gui serve` is running
2. Check the MCP config path is correct
3. Restart Claude Desktop completely

**"embedding provider error"**
- OpenAI: verify your API key is valid and has credits
- Ollama: reopen the smart-memory setup from CEREBRUM so SAGE can check the local runtime, model, and re-embedding status
- Switch to hash provider for zero-dependency operation

**Where are my memories?**
`~/.sage/data/sage.db` — standard SQLite database. You can inspect it with any SQLite client:
```bash
sqlite3 ~/.sage/data/sage.db "SELECT memory_id, domain_tag, status FROM memories LIMIT 10;"
```
