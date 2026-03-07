# SAGE — Give Your AI a Memory

Your AI forgets everything between conversations. SAGE fixes that.

SAGE is a persistent memory layer that runs on your laptop. Your AI remembers what you've worked on, what went wrong, what went right — across every conversation. No cloud accounts. No third-party access to your data. Everything stays on your machine.

```bash
git clone https://github.com/l33tdawg/sage.git
cd sage
go build -o sage-lite ./cmd/sage-lite/
./sage-lite setup    # Pick your AI, get MCP config
./sage-lite serve    # Start SAGE + Brain Dashboard
```

Paste the MCP config into Claude Desktop (or ChatGPT). Done. Your AI now remembers everything.

**[Full Getting Started Guide](docs/GETTING_STARTED.md)**

---

## Why Not Just Use a "Memory Plugin"?

Those third-party MCP skills and plugins get full access to your conversations, your files, and your API keys. Some phone home. Some store your data on servers you've never heard of.

SAGE runs **entirely on your machine**. Your memories live in a SQLite file in your home directory. Nothing leaves your laptop unless you explicitly configure a cloud embedding provider. No accounts, no telemetry, no surprises.

---

## What Your AI Gets

| Capability | What it means |
|-----------|---------------|
| **Persistent memory** | Remembers across conversations — projects, preferences, past mistakes |
| **Governed knowledge** | Every memory goes through consensus validation, not just a flat file |
| **Confidence scoring** | Memories have confidence levels that strengthen with corroboration |
| **Natural decay** | Old, uncorroborated memories fade over time — like human memory |
| **Semantic search** | Your AI recalls relevant context, not just keyword matches |
| **Reflection loop** | Stores what worked AND what failed — both make it better |
| **Full audit trail** | Every memory is cryptographically signed and traceable |
| **You own everything** | `~/.sage/data/sage.db` — standard SQLite, inspect or back up anytime |

---

## How It Works

Your AI gets 8 memory tools via MCP:

| Tool | Purpose |
|------|---------|
| `sage_inception` | Initialize the AI's memory (run once) |
| `sage_remember` | Store a memory (fact, observation, or inference) |
| `sage_recall` | Search memories by semantic similarity |
| `sage_reflect` | End-of-task reflection — what worked, what didn't |
| `sage_forget` | Mark a memory as deprecated |
| `sage_list` | Browse memories with filters |
| `sage_timeline` | View memories over time |
| `sage_status` | Check memory health and stats |

The AI uses these automatically. You just talk to it normally, and it builds up institutional knowledge over time.

---

## Brain Dashboard

Open `http://localhost:8080/ui/` to see your AI's memory visualized as a living neural network. Memories appear as glowing nodes, connections light up on recall, and everything updates in real-time.

---

## The Research Behind It

SAGE isn't a weekend hack. It's backed by peer-reviewed research proving that governed memory makes AI agents measurably better over time.

| Paper | Finding |
|-------|---------|
| [Agent Memory Infrastructure](papers/Paper1%20-%20Agent%20Memory%20Infrastructure%20-%20Byzantine-Resilient%20Institutional%20Memory%20for%20Multi-Agent%20Systems.pdf) | Architecture for BFT-validated agent memory |
| [Consensus-Validated Memory](papers/Paper2%20-%20Consensus-Validated%20Memory%20Improves%20Agent%20Performance%20on%20Complex%20Tasks.pdf) | 50-vs-50 study: memory agents outperform memoryless ones |
| [Institutional Memory](papers/Paper3%20-%20Institutional%20Memory%20as%20Organizational%20Knowledge%20-%20AI%20Agents%20That%20Learn%20Their%20Jobs%20from%20Experience%20Not%20Instructions.pdf) | Agents that learn from experience, not just instructions |
| [Longitudinal Learning](papers/Paper4%20-%20Longitudinal%20Learning%20in%20Governed%20Multi-Agent%20Systems%20-%20How%20Institutional%20Memory%20Improves%20Agent%20Performance%20Over%20Time.pdf) | Cumulative learning across sessions (rho=0.716, p=0.020) |

---

## Embedding Providers

Privacy first. Your memories never leave your machine.

| Provider | Quality | Privacy | Cost | Setup |
|----------|---------|---------|------|-------|
| **Ollama** | Smart semantic search | Fully local | Free | Install Ollama |
| **Hash** | Keyword matching only | Fully local | Free | Nothing needed |

Start with hash (zero setup), upgrade to Ollama when you want semantic recall. Both run 100% locally.

---

## Scaling Up

SAGE Personal is a single-node version of a full BFT consensus protocol. When you're ready for teams and organizations:

- **Multi-validator deployment** — 4-node BFT cluster with CometBFT consensus
- **Python SDK** — programmatic agent integration
- **RBAC & Federation** — organizations, departments, clearance levels, cross-org knowledge sharing
- **Monitoring** — Prometheus + Grafana dashboards
- **956 req/s** throughput, **21.6ms P95** latency under load

See the **[Architecture & Deployment Guide](docs/ARCHITECTURE.md)** for the full multi-node setup.

---

## Requirements

- **Go 1.22+** to build from source
- That's it. No Docker, no databases, no cloud accounts.

---

## License

Code: [Apache License 2.0](LICENSE) | Papers: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Author

Dhillon Andrew Kannabhiran ([@l33tdawg](https://github.com/l33tdawg))

---

<p align="center"><em>A tribute to <a href="http://phenoelit.darklab.org/fx.html">Felix 'FX' Lindner</a> — who showed us <b>how much further curiosity can go.</b></em></p>
