# SAGE — Sovereign Agent Governed Experience

A governed, verifiable, experience-weighted institutional memory layer for multi-agent systems.

SAGE is an infrastructure protocol — not an application library. It provides Byzantine fault-tolerant consensus over agent knowledge, enabling multi-agent systems to accumulate, validate, and share institutional memory through governed experience rather than prompt engineering.

## What SAGE Does

- **Institutional Memory** — Agents propose, validate, and commit knowledge to a shared memory layer
- **Proof of Experience (PoE)** — Weighted consensus based on agent accuracy, domain expertise, recency, and corroboration
- **Byzantine Fault Tolerance** — CometBFT-backed consensus (n >= 3f+1), survives validator failures
- **Sovereign Infrastructure** — No tokens, no public chain, no cloud dependencies. Fully self-hosted

## Architecture

| Layer | Technology |
|-------|-----------|
| Consensus | CometBFT v0.38.15 (ABCI 2.0) |
| State Machine | Go 1.22+ |
| On-chain State | BadgerDB v4 (hashes only) |
| Off-chain Storage | PostgreSQL 16 + pgvector |
| Tx Format | Protobuf |
| REST API | Go chi v5 (11 endpoints) |
| Agent SDK | Python (httpx + PyNaCl + Pydantic v2) |
| Embeddings | Ollama nomic-embed-text (768-dim, local) |
| Monitoring | Prometheus + Grafana |

## Performance

- 956 req/s memory submissions
- 21.6ms P95 query latency
- 0% error rate under load
- BFT verified: 1/4 nodes down -> continues, 2/4 -> halts, recovery + state replication confirmed

## Quick Start

```bash
make init    # Generate 4-node testnet configs
make up      # Start network (4 CometBFT + 4 ABCI + PostgreSQL + Ollama)
make test    # Run tests
```

REST API available at `localhost:8080` (nodes 0-3 on ports 8080-8083).

## Python SDK

```bash
cd sdk/python && pip install -e .
```

```python
from sage_sdk import SageClient

client = SageClient("http://localhost:8080", agent_keypair)
client.propose("SQL injection via UNION bypass requires...", domain="security")
results = client.query("SQL injection techniques", domain="security")
```

## Research Papers

See [`papers/`](papers/) for the full research paper series documenting SAGE's design, empirical evaluation, and theoretical foundations.

| # | Paper | Description |
|---|-------|-------------|
| 1 | Agent Memory Infrastructure | Architecture, PoE consensus, BFT verification |
| 2 | Consensus-Validated Memory Improves Agent Performance | 50-vs-50 controlled empirical study |
| 3 | Institutional Memory as Organizational Knowledge | Organizational knowledge framing, sovereign AI implications |

## Repository Structure

```
sage/
├── cmd/              # ABCI daemon + CLI
├── internal/         # Core: ABCI state machine, PoE, memory, auth, store
├── api/              # REST handlers, middleware, protobuf, OpenAPI spec
├── sdk/python/       # Python SDK (sync + async)
├── deploy/           # Docker Compose, monitoring, init scripts
├── test/             # Integration, Byzantine fault, k6 benchmarks
├── integrations/     # Level Up CTF proof-of-concept
└── papers/           # Research papers (PDFs)
```

## License

Code: MIT | Papers: [CC BY 4.0](https://creativecommons.org/licenses/by/4.0/)

## Author

Dhillon Kannabhiran ([@l33tdawg](https://github.com/l33tdawg))
