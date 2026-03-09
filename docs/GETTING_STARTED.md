# Getting Started with SAGE Personal

SAGE Personal gives your AI assistant (Claude, ChatGPT) persistent memory across conversations. One binary, no Docker, no databases to manage — just install, connect, and your AI remembers everything.

### Why SAGE instead of a "memory skill" from the internet?

Those third-party MCP "skills" and plugins floating around? They get full access to your conversations, your files, your API keys. Some phone home. Some inject prompts. Some store your data on servers you've never heard of. And when they break, your AI starts doing things you didn't ask for.

SAGE runs **entirely on your machine**. Your memories live in a SQLite file in your home directory. Nothing leaves your laptop unless you explicitly configure a cloud embedding provider. The MCP server is a local binary that talks to a local database. No accounts, no telemetry, no surprises.

And because SAGE uses real consensus infrastructure (not just a JSON file), your memories have cryptographic integrity, confidence scoring, and a full audit trail. If you ever need to scale to a team or organization, the same protocol works — just add validators.

---

## Quick Install

### From Source (Go 1.22+)

```bash
git clone https://github.com/l33tdawg/sage.git
cd sage
go build -o sage-lite ./cmd/sage-lite/
sudo mv sage-lite /usr/local/bin/  # or add to your PATH
```

### Verify Installation

```bash
sage-lite version
# sage-lite v1.0.0
```

---

## Setup

### Option A: GUI Setup Wizard

```bash
sage-lite setup
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
sage-lite serve
```

You'll see:

```
SAGE Personal is running!
Dashboard: http://localhost:8080/ui/
REST API:  http://localhost:8080/v1/
```

Open the dashboard in your browser to see the Brain Visualization — a living neural network of your AI's memory.

---

## Connect to Claude Desktop

This is the killer feature. Claude Desktop can read and write memories directly via MCP (Model Context Protocol).

### 1. Get your MCP config

```bash
sage-lite setup
# Or manually create it:
```

```json
{
  "mcpServers": {
    "sage": {
      "command": "/usr/local/bin/sage-lite",
      "args": ["mcp"],
      "env": {
        "SAGE_HOME": "~/.sage"
      }
    }
  }
}
```

### 2. Add to Claude Desktop

1. Open **Claude Desktop** > **Settings** > **Developer**
2. Click **Edit Config** under MCP Servers
3. Paste the JSON above
4. **Restart Claude Desktop**

### 3. Start using it

Just chat normally. Claude now has 8 memory tools:

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

---

## Connect to ChatGPT

ChatGPT supports MCP through its desktop app. The configuration is similar — add the sage-lite MCP server in ChatGPT's settings.

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
2. You get a 6-character code (valid for 5 minutes)
3. On the new machine, run `sage-lite pair ABC123` (replacing with your code)
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

---

## Using Ollama for Local Embeddings

For fully private, local-only operation:

1. Install Ollama: https://ollama.ai
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
| **Ollama** | Smart semantic search | Fully local | Free | Install Ollama |
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
sage-lite serve  # Reinitializes automatically
```

---

## How It Works

Under the hood, SAGE Personal runs a real BFT consensus engine (CometBFT) with a single validator. Every memory goes through the full governance pipeline:

1. **Propose** — memory submitted via MCP or REST API
2. **Validate** — auto-validator accepts the memory
3. **Commit** — memory written to SQLite with on-chain hash in BadgerDB

This means your personal SAGE instance uses the exact same protocol as a multi-validator production deployment. If you later want to upgrade to a team setup with multiple validators, your data and tooling are already compatible.

---

## Migrating to Full SAGE

When you outgrow personal mode and need multi-agent BFT consensus:

1. Export your memories: they're in standard SQLite
2. Set up the full 4-node deployment: `make init && make up`
3. Import memories into the PostgreSQL-backed production cluster

See the main [README](../README.md) for the full multi-node deployment guide.

---

## Troubleshooting

**sage-lite serve fails with "address already in use"**
Another process is using port 8080. Either stop it or change the port in `~/.sage/config.yaml`:
```yaml
rest_addr: ":8081"
```

**Claude Desktop doesn't show SAGE tools**
1. Make sure `sage-lite serve` is running
2. Check the MCP config path is correct
3. Restart Claude Desktop completely

**"embedding provider error"**
- OpenAI: verify your API key is valid and has credits
- Ollama: make sure Ollama is running (`ollama serve`)
- Switch to hash provider for zero-dependency operation

**Where are my memories?**
`~/.sage/data/sage.db` — standard SQLite database. You can inspect it with any SQLite client:
```bash
sqlite3 ~/.sage/data/sage.db "SELECT memory_id, domain_tag, status FROM memories LIMIT 10;"
```
