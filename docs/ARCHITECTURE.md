# (S)AGE Architecture & Full Deployment Guide

This document covers the full multi-node BFT deployment, Python SDK, REST API, monitoring, and operational reference. For personal use (single binary, no Docker), see the [main README](../README.md).

---

## Table of Contents

1. [Architecture Overview](#architecture-overview)
2. [Prerequisites](#prerequisites)
3. [Quick Start (Full Deployment)](#quick-start-full-deployment)
4. [Network Topology](#network-topology)
5. [Environment Variables](#environment-variables)
6. [Python SDK](#python-sdk)
7. [Sovereign Layer (RBAC & Federation)](#sovereign-layer-rbac--federation)
8. [Agent Management & Network Topology](#agent-management--network-topology)
9. [CLI Tools](#cli-tools)
10. [REST API](#rest-api)
11. [Monitoring](#monitoring)
12. [Testing](#testing)
13. [Troubleshooting](#troubleshooting)
14. [Repository Structure](#repository-structure)
15. [Makefile Reference](#makefile-reference)

---

## Architecture Overview

```mermaid
graph TB
    subgraph Agents["Agents (Python SDK / HTTP)"]
        A1["Agent A"]
        A2["Agent B"]
        A3["Agent C"]
    end

    subgraph ABCI["(S)AGE Application Layer"]
        direction LR
        AB0["ABCI 0<br/><small>:8080 REST</small><br/><small>:2112 metrics</small>"]
        AB1["ABCI 1<br/><small>:8081 REST</small><br/><small>:2113 metrics</small>"]
        AB2["ABCI 2<br/><small>:8082 REST</small><br/><small>:2114 metrics</small>"]
        AB3["ABCI 3<br/><small>:8083 REST</small><br/><small>:2115 metrics</small>"]
    end

    subgraph Consensus["BFT Consensus Layer"]
        direction LR
        C0["CometBFT 0<br/><small>:26657 RPC</small>"]
        C1["CometBFT 1<br/><small>:26757 RPC</small>"]
        C2["CometBFT 2<br/><small>:26857 RPC</small>"]
        C3["CometBFT 3<br/><small>:26957 RPC</small>"]
        C0 <--> C1
        C1 <--> C2
        C2 <--> C3
        C0 <--> C3
    end

    subgraph Storage["Shared Services"]
        direction LR
        PG[("PostgreSQL 16<br/>+ pgvector<br/><small>Off-chain data<br/>HNSW indexes</small>")]
        OL["Ollama<br/><small>nomic-embed-text<br/>768-dim embeddings</small>"]
    end

    A1 & A2 & A3 --> AB0 & AB1 & AB2 & AB3

    AB0 <--> C0
    AB1 <--> C1
    AB2 <--> C2
    AB3 <--> C3

    AB0 & AB1 & AB2 & AB3 --> PG
    AB0 & AB1 & AB2 & AB3 --> OL

    style Agents fill:#e8f4f8,stroke:#2196F3,color:#000
    style ABCI fill:#fff3e0,stroke:#FF9800,color:#000
    style Consensus fill:#fce4ec,stroke:#E91E63,color:#000
    style Storage fill:#e8f5e9,stroke:#4CAF50,color:#000
```

### How it works

1. **Agents** connect to any ABCI node via the Python SDK (or raw HTTP)
2. **ABCI nodes** (Go) process requests -- memory submissions become signed transactions
3. Transactions are broadcast to **CometBFT** which runs BFT consensus across all 4 validators
4. Once a block is committed, the state machine updates **PostgreSQL** (full content) and **BadgerDB** (hashes only)
5. **Ollama** generates 768-dim embeddings locally -- zero cloud API calls

No tokens. No gas fees. No cryptocurrency. Just consensus-validated knowledge.

| Layer | Technology |
|-------|-----------|
| Consensus | CometBFT v0.38.15 (ABCI 2.0, raw -- not Cosmos SDK) |
| State Machine | Go 1.22+ ABCI application |
| On-chain State | BadgerDB v4 (hashes only) |
| Off-chain Storage | PostgreSQL 16 + pgvector (HNSW indexes) |
| Tx Format | Protobuf (deterministic serialization) |
| REST API | Go chi v5 (11 endpoints, OpenAPI 3.1) |
| Agent SDK | Python (httpx + PyNaCl + Pydantic v2) |
| Embeddings | Ollama nomic-embed-text (768-dim, fully local) |
| Monitoring | Prometheus + Grafana (3 dashboards, 5 alert rules) |

**Performance (verified under k6 load testing):**

- 956 req/s memory submissions
- 21.6ms P95 query latency
- 0% error rate under load
- BFT verified: 1/4 nodes down continues operating, 2/4 halts, recovery + state replication confirmed

---

## Prerequisites

| Requirement | Version | Notes |
|------------|---------|-------|
| Docker | 20.10+ | Required for the containerized network |
| Docker Compose | v2+ | Uses `docker compose` (v2 syntax, not `docker-compose`) |
| Go | 1.22+ | Only needed for local builds and running tests |
| Python | 3.10+ | Only needed for the SDK and experiments |
| make | any | Build automation |
| curl | any | Used by `make status` and health checks |

Go and Python are only required if you want to build from source or run tests. The Docker Compose setup handles everything needed to run the network.

---

## Quick Start (Full Deployment)

### 1. Clone the repository

```bash
git clone https://github.com/l33tdawg/sage.git
cd sage
```

### 2. Generate testnet configuration

```bash
make init
```

This runs `deploy/init-testnet.sh`, which:

- Builds CometBFT v0.38.15 from source inside a Docker container (if not locally installed)
- Generates Ed25519 validator keys and genesis configuration for 4 validator nodes
- Writes configs to `deploy/genesis/node{0..3}/`
- Patches `config.toml` for Docker networking (disables PEX, enables Prometheus, sets block time to 3s)
- Sets chain ID to `sage-testnet-1`

### 3. Start the network

```bash
make up
```

This launches 11 Docker containers:

| Container | Role |
|-----------|------|
| `postgres` | PostgreSQL 16 + pgvector (off-chain storage, 8 tables + HNSW indexes) |
| `ollama` | Local embedding model server |
| `ollama-init` | One-shot: pulls `nomic-embed-text` and `qwen2.5:1.5b` models, then exits |
| `abci0` - `abci3` | (S)AGE ABCI application nodes (Go state machine + REST API) |
| `cometbft0` - `cometbft3` | CometBFT consensus validators |

Wait approximately 30-60 seconds for all services to initialize. The Ollama model pull on first run may take several minutes depending on your connection speed.

### 4. Verify the network is running

```bash
make status
```

Expected output shows all 4 nodes reporting `latest_block_height` incrementing and `catching_up: false`:

```
==> Node 0 (localhost:26657):
    "latest_block_height": "42",
    "catching_up": false
==> Node 1 (localhost:26757):
    "latest_block_height": "42",
    "catching_up": false
...
```

You can also hit the health endpoint directly:

```bash
curl -s http://localhost:8080/health | python3 -m json.tool
```

### 5. Watch logs

```bash
make logs        # All containers
make logs-abci   # ABCI application nodes only
```

### 6. Stop the network

```bash
make down          # Stop containers (preserves data volumes)
make down-clean    # Stop containers AND wipe all data (volumes, orphans)
```

---

## Network Topology

### Port Map

| Service | Node 0 | Node 1 | Node 2 | Node 3 |
|---------|--------|--------|--------|--------|
| REST API | `localhost:8080` | `localhost:8081` | `localhost:8082` | `localhost:8083` |
| CometBFT RPC | `localhost:26657` | `localhost:26757` | `localhost:26857` | `localhost:26957` |
| CometBFT P2P | `localhost:26656` | `localhost:26756` | `localhost:26856` | `localhost:26956` |
| Prometheus metrics (ABCI) | `localhost:2112` | `localhost:2113` | `localhost:2114` | `localhost:2115` |
| CometBFT Prometheus | `:26660` | `:26761` | `:26862` | `:26963` |

### Shared Services

| Service | Port | Notes |
|---------|------|-------|
| PostgreSQL | `localhost:5432` | Shared by all ABCI nodes, `ON CONFLICT DO NOTHING` for multi-writer safety |
| Ollama | `localhost:11434` | Shared embedding model server |
| Grafana | `localhost:3000` | Only with `make up-full` |
| Prometheus | `localhost:9191` | Only with `make up-full` |

### Container Relationships

Each ABCI node connects to:
- Its paired CometBFT node via TCP (ABCI protocol on port 26658, internal to Docker network)
- PostgreSQL for off-chain storage (memories, votes, corroborations, epoch scores)
- Ollama for embedding generation

Each CometBFT node:
- Runs BFT consensus with persistent peer connections to all other CometBFT nodes
- Connects to its paired ABCI application for state machine execution
- Exposes RPC for status queries and transaction broadcasting

---

## Environment Variables

### ABCI Application Nodes

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_URL` | `postgres://sage:sage_dev_password@postgres:5432/sage?sslmode=disable` | PostgreSQL connection string |
| `NODE_ID` | `node0` | Unique identifier for this node |
| `REST_ADDR` | `:8080` | Address for the REST API server to bind |
| `METRICS_ADDR` | `:2112` | Address for the Prometheus metrics endpoint |
| `BADGER_PATH` | `/data/sage.db` | Path for the BadgerDB on-chain state |
| `ABCI_ADDR` | `tcp://0.0.0.0:26658` | TCP address for ABCI protocol (CometBFT connects here) |
| `COMET_RPC` | `http://cometbft0:26657` | CometBFT RPC endpoint for broadcasting transactions |
| `VALIDATOR_KEY_FILE` | `/validator/priv_validator_key.json` | Path to CometBFT validator key (Ed25519, must match validator set) |
| `OLLAMA_URL` | `http://ollama:11434` | Ollama API endpoint for embeddings |

### PostgreSQL

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_DB` | `sage` | Database name |
| `POSTGRES_USER` | `sage` | Database user |
| `POSTGRES_PASSWORD` | `sage_dev_password` | Database password |

### CometBFT

| Variable | Default | Description |
|----------|---------|-------------|
| `CMTHOME` | `/cometbft` | CometBFT home directory (configs, data, keys) |

### CLI / SDK

| Variable | Default | Description |
|----------|---------|-------------|
| `SAGE_API_URL` | `http://localhost:8080` | Used by `sage-cli` to locate the (S)AGE REST API |
| `SAGE_URL` | `http://localhost:8080` | Used by Python SDK examples |

---

## Python SDK

The (S)AGE Python SDK provides both synchronous and asynchronous clients for interacting with the (S)AGE network.

### Installation

```bash
cd sdk/python
pip install -e .
```

For development (includes test dependencies):

```bash
cd sdk/python
pip install -e ".[dev]"
```

### Generate an Agent Identity

Every agent needs an Ed25519 keypair. The SDK can generate one:

```python
from sage_sdk import AgentIdentity

identity = AgentIdentity.generate()
print(f"Agent ID: {identity.agent_id}")
```

Or use the CLI:

```bash
go run ./cmd/sage-cli keygen
# Outputs: Agent ID, public key, private key, and saves seed to sage-agent-XXXX.key
```

### Connect and Submit a Memory

```python
from sage_sdk import AgentIdentity, SageClient

# Generate or load identity
identity = AgentIdentity.generate()

# Connect to any (S)AGE node
with SageClient(base_url="http://localhost:8080", identity=identity) as client:

    # Submit a memory for consensus validation
    result = client.propose(
        content="SQL injection via UNION SELECT requires matching column count with the original query.",
        memory_type="fact",
        domain_tag="security",
        confidence=0.92,
    )
    print(f"Memory ID: {result.memory_id}")
    print(f"Tx Hash:   {result.tx_hash}")
    print(f"Status:    {result.status}")
```

### Query Memories

```python
    # Semantic similarity search (returns committed memories)
    results = client.query(
        query_text="SQL injection techniques",
        domain="security",
        top_k=10,
        status_filter="committed",
    )
    for memory in results:
        print(f"  [{memory.confidence_score:.2f}] {memory.content[:80]}...")
```

### Full Lifecycle Example

```python
from sage_sdk import AgentIdentity, SageClient

identity = AgentIdentity.generate()

with SageClient(base_url="http://localhost:8080", identity=identity) as client:

    # 1. Propose a memory
    result = client.propose(
        content="AES-256-GCM provides authenticated encryption with 128-bit tags.",
        memory_type="fact",
        domain_tag="cryptography",
        confidence=0.95,
    )
    memory_id = result.memory_id

    # 2. Retrieve it
    memory = client.get_memory(memory_id)
    print(f"Status: {memory.status.value}")  # "proposed"

    # 3. Corroborate (from another agent or after validation)
    client.corroborate(memory_id, evidence="Verified against NIST SP 800-38D.")

    # 4. Query by similarity
    results = client.query("authenticated encryption", domain="cryptography")

    # 5. Challenge if incorrect
    client.dispute(memory_id, reason="Tag length depends on implementation choice.")
```

### Async Client

```python
from sage_sdk import AgentIdentity, AsyncSageClient
import asyncio

async def main():
    identity = AgentIdentity.generate()
    async with AsyncSageClient(base_url="http://localhost:8080", identity=identity) as client:
        result = await client.propose(
            content="Buffer overflow in strcpy is a classic CWE-120 vulnerability.",
            memory_type="observation",
            domain_tag="security",
            confidence=0.88,
        )
        print(f"Submitted: {result.memory_id}")

asyncio.run(main())
```

### SDK Examples

The SDK ships with ready-to-run examples in `sdk/python/examples/`:

```bash
cd sdk/python
python examples/quickstart.py           # Minimal propose + retrieve
python examples/full_lifecycle.py       # Full memory lifecycle
python examples/multi_agent.py          # Multiple agents collaborating
python examples/async_example.py        # Async client usage
python examples/org_setup.py           # Organizations, departments, RBAC
python examples/rbac_clearance.py      # Clearance levels (org/dept/member hierarchy)
python examples/federation.py          # Cross-org federation agreements
python examples/complete_walkthrough.py # Every SDK operation explained
```

---

## Sovereign Layer (RBAC & Federation)

The **(S)** in (S)AGE is the optional governance layer. Without it, you have AGE -- agents proposing and querying memories with PoE consensus. Add the Sovereign layer when you need multi-org governance:

```mermaid
graph TB
    subgraph Fed["Federation (Cross-Org)"]
        direction TB
        subgraph OrgA["Organization A"]
            direction TB
            subgraph DeptA1["Dept: Red Team"]
                AA1["Agent<br/><small>clearance 4 (admin)</small>"]
                AA2["Agent<br/><small>clearance 3 (validate)</small>"]
            end
            subgraph DeptA2["Dept: Blue Team"]
                AA3["Agent<br/><small>clearance 2 (write)</small>"]
                AA4["Agent<br/><small>clearance 1 (read)</small>"]
            end
        end
        subgraph OrgB["Organization B"]
            direction TB
            subgraph DeptB1["Dept: Research"]
                AB1["Agent<br/><small>clearance 3</small>"]
            end
        end
        OrgA <-->|"federated domains<br/><small>max clearance 2</small>"| OrgB
    end

    subgraph Domains["Knowledge Domains"]
        D1["classified_intel"]
        D2["operational_data"]
        D3["public_advisories"]
    end

    AA1 -->|"level 4"| D1 & D2 & D3
    AA2 -->|"level 3"| D1
    AA3 -->|"level 2"| D2
    AA4 -->|"level 1"| D3
    AB1 -.->|"federated<br/>level 2"| D2

    style Fed fill:#f3e5f5,stroke:#9C27B0,color:#000
    style OrgA fill:#e8f4f8,stroke:#2196F3,color:#000
    style OrgB fill:#fff3e0,stroke:#FF9800,color:#000
    style Domains fill:#e8f5e9,stroke:#4CAF50,color:#000
```

**Clearance levels** control what agents can do:

| Level | Access | Description |
|-------|--------|-------------|
| 0 | None | Observer, no access |
| 1 | Read | Query memories in domain |
| 2 | Read + Write | Propose memories to domain |
| 3 | Read + Write + Validate | Vote on proposed memories |
| 4 | Admin | Full control, grant/revoke access |

All RBAC state is on-chain -- organizations, departments, clearance levels, access grants, and federation agreements are committed to the BFT network and replicated across all validators.

---

## Agent Management & Network Topology

SAGE v3.0 introduces a built-in agent management system for multi-agent networks. This replaces manual key distribution and config editing with a dashboard-driven workflow.

### Agent Registry

All agents are tracked in the `network_agents` SQLite table:

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT | Primary key (UUID) |
| `name` | TEXT | Human-readable agent name |
| `public_key` | TEXT | Hex-encoded Ed25519 public key |
| `role` | TEXT | `admin`, `validator`, `writer`, `reader`, `observer` |
| `clearance` | INTEGER | 0-4 clearance tier |
| `domain_access` | JSON | Per-domain read/write permission map |
| `status` | TEXT | `active`, `suspended`, `retired` |
| `created_at` | TIMESTAMP | Registration time |
| `updated_at` | TIMESTAMP | Last modification time |

On upgrade to v3, existing genesis validators are auto-seeded into this table so they appear in the dashboard immediately.

### Agent Lifecycle

```
Creation → Active → Key Rotation → Removal
              ↓           ↓
          Suspended    Re-keyed (new Ed25519 identity,
              ↓         memories re-attributed atomically)
           Retired
```

1. **Creation** — Admin adds agent via CEREBRUM dashboard or REST API. Agent receives an Ed25519 keypair, role, clearance level, and domain access map.
2. **Active** — Agent participates in the network. Reads and writes are enforced against its `domain_access` map and `clearance` level.
3. **Key Rotation** — Admin triggers key rotation. A new Ed25519 keypair is generated, all memories attributed to the old key are re-attributed to the new key in a single atomic transaction, and the old key is marked as retired.
4. **Suspension** — Admin suspends agent. All requests from the agent's key are rejected with 403.
5. **Removal** — Admin removes agent. The record is soft-deleted (status set to `retired`). Memories remain attributed for audit purposes.

### Redeployment Orchestrator

When the validator set changes (agents added or removed), the CometBFT chain must be redeployed with a new genesis. The orchestrator handles this as a 9-phase state machine:

```
LOCK → BACKUP → STOP → GENESIS → WIPE → RESTART → VERIFY → RBAC → COMPLETE
```

| Phase | Action | Rollback |
|-------|--------|----------|
| `LOCK` | Acquire startup lock, reject new requests (503 middleware) | Release lock |
| `BACKUP` | Snapshot SQLite database and CometBFT state | Delete snapshot |
| `STOP` | Stop CometBFT node via `node.Stop()` | — |
| `GENESIS` | Generate new genesis.json with updated validator set | Restore old genesis |
| `WIPE` | Remove CometBFT data directory (blocks, WAL) | Restore from backup |
| `RESTART` | Create fresh `node.NewNode()` and start it | Restore backup and restart with old genesis |
| `VERIFY` | Wait for first block, confirm node is syncing | Retry or rollback |
| `RBAC` | Apply RBAC permissions for new validator set | — |
| `COMPLETE` | Release startup lock, resume accepting requests | — |

Key implementation detail: CometBFT nodes cannot be restarted after `Stop()` — a fresh `node.NewNode()` must be created. The `FilePV` validator state must be reset to height 0 before starting with a new genesis.

If any phase fails, the orchestrator rolls back to the `BACKUP` phase state and restarts with the original configuration. The 503 middleware ensures no client requests are lost during redeployment — they receive a `503 Service Unavailable` with a `Retry-After` header.

### Domain Access Enforcement

Every memory operation (propose, query, vote, challenge, corroborate) is checked against the agent's `domain_access` map:

- **Read side** — `GET /v1/memory/{id}` and `POST /v1/memory/query` check that the agent has `read: true` for the memory's `domain_tag`.
- **Write side** — `POST /v1/memory/submit`, `/vote`, `/challenge`, `/corroborate` check that the agent has `write: true` for the target domain.
- **Observer role** — Agents with role `observer` are blocked from all write operations regardless of domain access.
- **Clearance tiers** — Five levels (0=Guest, 1=Reader, 2=Writer, 3=Validator, 4=Admin) provide coarse-grained access control on top of domain-specific permissions.

The `domain_access` field is a JSON object mapping domain names to permission objects:

```json
{
  "security": {"read": true, "write": true},
  "finance": {"read": true, "write": false},
  "personal": {"read": false, "write": false}
}
```

### LAN Pairing Flow

For adding agents on a local network without manual key exchange:

1. Admin clicks "Add Agent" in the CEREBRUM dashboard and selects "LAN Pairing"
2. Server generates a 6-character alphanumeric pairing code (expires after 5 minutes)
3. New agent runs `sage-lite pair <code>` or enters the code in their setup wizard
4. Agent and server perform a key exchange over the LAN
5. Server registers the agent with its Ed25519 public key, assigns default role and permissions
6. Admin can then adjust role, clearance, and domain access from the dashboard

The pairing code is single-use and time-limited. The exchange happens over the local network only — no external servers involved.

### Key Rotation Mechanics

Key rotation replaces an agent's Ed25519 keypair without losing memory attribution:

1. Admin initiates rotation from the dashboard (or agent requests it via REST API)
2. Server generates a new Ed25519 keypair for the agent
3. All memories where `agent_id` matches the old public key are updated to the new public key in a single database transaction
4. The old public key is added to a `retired_keys` list on the agent record
5. The new private key is delivered to the agent (via LAN pairing channel or manual download)
6. Subsequent requests from the old key are rejected

This is an atomic operation — if any step fails, the entire rotation is rolled back and the old key remains active.

---

## CLI Tools

### sage-cli

Build and run the admin CLI:

```bash
go build -o bin/sage-cli ./cmd/sage-cli
```

Or run directly:

```bash
go run ./cmd/sage-cli <command>
```

### Commands

**keygen** -- Generate a new Ed25519 agent keypair:

```bash
$ go run ./cmd/sage-cli keygen
=== (S)AGE Agent Keypair ===
Agent ID (public key):  a1b2c3d4e5f6...
Private key (hex):      ...
Public key (hex):       ...
Seed saved to:          sage-agent-a1b2c3d4.key
```

**status** -- Query all 4 CometBFT nodes:

```bash
$ go run ./cmd/sage-cli status
==> Node 0 (http://localhost:26657/status):
  { "latest_block_height": "128", "catching_up": false, ... }
...
```

**health** -- Check the (S)AGE REST API health endpoint:

```bash
$ go run ./cmd/sage-cli health
{
  "status": "healthy",
  "version": "1.0.0"
}
```

Set `SAGE_API_URL` to target a different node (default: `http://localhost:8080`).

---

## REST API

The (S)AGE REST API uses Ed25519 signature authentication and follows the OpenAPI 3.1 specification (see `api/openapi.yaml`).

### Authentication

All authenticated endpoints require three headers:

| Header | Value |
|--------|-------|
| `X-Agent-ID` | Hex-encoded Ed25519 public key |
| `X-Signature` | Ed25519 signature of `SHA-256(request_body) + big-endian int64(timestamp)` |
| `X-Timestamp` | Unix epoch seconds |

The Python SDK handles signing automatically. For raw HTTP access, compute `SHA-256` of the JSON body, append the timestamp as a big-endian 8-byte integer, and sign the result with your Ed25519 private key.

### Endpoints

| Method | Path | Auth | Description |
|--------|------|------|-------------|
| `POST` | `/v1/memory/submit` | Yes | Submit a new memory for consensus validation |
| `POST` | `/v1/memory/query` | Yes | Semantic similarity search over memories |
| `GET` | `/v1/memory/{memory_id}` | Yes | Retrieve a single memory by ID |
| `POST` | `/v1/memory/{memory_id}/vote` | Yes | Cast a validator vote (accept/reject/abstain) |
| `POST` | `/v1/memory/{memory_id}/challenge` | Yes | Challenge a committed memory |
| `POST` | `/v1/memory/{memory_id}/corroborate` | Yes | Corroborate a memory with evidence |
| `GET` | `/v1/agent/me` | Yes | Get authenticated agent's profile and PoE weight |
| `GET` | `/v1/validator/pending` | Yes | List memories awaiting validator votes |
| `GET` | `/v1/validator/epoch` | Yes | Current epoch info and validator scores |
| `GET` | `/health` | No | Liveness probe |
| `GET` | `/ready` | No | Readiness probe (checks PostgreSQL + CometBFT) |

### Memory Lifecycle

```mermaid
stateDiagram-v2
    [*] --> Proposed: Agent submits via SDK
    Proposed --> Committed: Quorum reached (>= 2/3 weighted vote)
    Committed --> Challenged: Agent disputes with evidence
    Challenged --> Deprecated: Challenge upheld
    Challenged --> Committed: Challenge rejected
    Committed --> Committed: Corroborated (confidence increases)

    note right of Proposed: Validators vote\n(accept / reject / abstain)
    note right of Committed: Consensus-validated\nreplicated across all nodes
```

1. An agent submits a memory via `/v1/memory/submit` (status: `proposed`)
2. Validators vote via `/v1/memory/{id}/vote`
3. When quorum is reached (>= 2/3 weighted vote), status advances to `committed`
4. Other agents can corroborate (strengthens confidence) or challenge (triggers review)
5. Challenged memories may be deprecated based on evidence

### Error Format

All errors follow RFC 7807 Problem Details:

```json
{
  "type": "https://sage.example/errors/validation",
  "title": "Validation Error",
  "status": 400,
  "detail": "confidence_score must be between 0 and 1"
}
```

---

## Monitoring

### Start with monitoring stack

```bash
make up-full
```

This starts the standard 11 containers plus:

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana | `http://localhost:3000` | admin / `sage` (or anonymous access enabled) |
| Prometheus | `http://localhost:9191` | No auth |

### Grafana Dashboards

Three pre-configured dashboards are provisioned automatically:

1. **(S)AGE Overview** -- Network health, block height, active validators
2. **Memory Metrics** -- Submission rates, query latency, consensus timing
3. **Node Health** -- Per-node CPU, memory, error rates

### Prometheus Metrics

ABCI application nodes expose custom metrics on their metrics ports (2112-2115):

- `sage_memory_submissions_total` -- Total memory submissions
- `sage_memory_queries_total` -- Total similarity queries
- `sage_consensus_votes_total` -- Validator votes cast
- `sage_block_height` -- Current block height
- `sage_query_duration_seconds` -- Query latency histogram

CometBFT nodes expose built-in metrics on ports 26660, 26761, 26862, 26963.

### Alert Rules

Five alert rules are pre-configured in `deploy/monitoring/alerts.yml`:

- Node down detection
- Block production stalled
- High query latency
- Consensus failure
- PostgreSQL connectivity loss

---

## Testing

### Unit Tests (Go)

```bash
make test
```

Runs Go unit tests with race detection across all packages:

```bash
go test ./... -v -count=1 -race
```

### Integration Tests

Requires a running (S)AGE network (`make up`):

```bash
make integration
```

Runs integration tests covering memory lifecycle, consensus proofs, and PoE scoring:

```bash
go test ./test/integration/... -v -count=1 -timeout 300s -tags=integration
```

### Python SDK Tests

```bash
make sdk-test
```

Runs pytest tests with mocked HTTP (no running network needed):

```bash
cd sdk/python && pip install -e ".[dev]" && pytest -v
```

### Load Testing (k6)

Requires [k6](https://k6.io/) installed and a running network:

```bash
make benchmark
```

Runs `test/benchmark/load.js` against the network to measure throughput and latency.

### Linting

```bash
make lint    # golangci-lint
make fmt     # gofmt
make vet     # go vet
```

---

## Troubleshooting

### Network does not start

**Symptom:** `make up` exits with errors or containers keep restarting.

**Check:** Ensure `make init` was run first. The CometBFT nodes require genesis configuration in `deploy/genesis/`. If the directory is missing or corrupted:

```bash
make clean     # Removes deploy/genesis/ and bin/
make init      # Regenerate testnet configs
make up
```

### "catching_up: true" on all nodes

**Symptom:** `make status` shows all nodes with `catching_up: true`.

**Cause:** Nodes are still syncing. Wait 10-15 seconds after `make up` for initial block production to begin. If it persists, check ABCI logs:

```bash
make logs-abci
```

### PostgreSQL connection failures

**Symptom:** ABCI logs show `connection refused` to PostgreSQL.

**Cause:** PostgreSQL container has not passed its health check yet. The ABCI containers depend on PostgreSQL being healthy, but there can be race conditions. Restart the stack:

```bash
make down && make up
```

### Ollama model not ready

**Symptom:** Embedding requests return errors or empty vectors.

**Cause:** The `ollama-init` container pulls models on first start. This can take several minutes. Check its status:

```bash
docker compose -f deploy/docker-compose.yml logs ollama-init
```

Wait for `Models ready` in the output before submitting memories that require embeddings.

### Port conflicts

**Symptom:** Containers fail to start with `bind: address already in use`.

**Cause:** Another service is using one of (S)AGE's ports (8080-8083, 5432, 26656-26957, etc.).

**Fix:** Stop the conflicting service, or modify port mappings in `deploy/docker-compose.yml`.

### "make init" fails building CometBFT

**Symptom:** Docker build of CometBFT from source fails.

**Cause:** Network issues pulling the Go image or cloning CometBFT. Retry, or install CometBFT v0.38.15 locally:

```bash
git clone --branch v0.38.15 --depth 1 https://github.com/cometbft/cometbft.git
cd cometbft && make install
cd .. && make init
```

### SDK authentication errors

**Symptom:** Python SDK calls return 401 Unauthorized.

**Cause:** Clock skew between your machine and the server, or malformed signing. Ensure:

1. System clock is accurate (signature includes timestamp)
2. You are using a valid Ed25519 keypair generated by `AgentIdentity.generate()` or `sage-cli keygen`
3. The request body has not been modified after signing

### Queries return no results

**Cause:** By default, queries return memories in all states. For consensus-validated memories only, pass `status_filter="committed"`:

```python
results = client.query("your query", status_filter="committed")
```

New memories start as `proposed` and only become `committed` after reaching quorum (>= 2/3 weighted validator votes).

### Data reset

To wipe all data and start fresh:

```bash
make down-clean    # Stops containers and removes all Docker volumes
make init          # Regenerate configs (validator keys change)
make up            # Start fresh network
```

---

## Repository Structure

```
sage/
├── cmd/
│   ├── amid/main.go                  # ABCI daemon (CometBFT + REST + Prometheus)
│   ├── sage-cli/main.go              # Admin CLI (keygen, status, health)
│   └── sage-lite/                    # SAGE Personal (setup, serve, MCP)
├── internal/
│   ├── abci/                         # ABCI 2.0 state machine (FinalizeBlock, Commit)
│   ├── auth/                         # Ed25519 keypair, sign/verify requests
│   ├── embedding/                    # Embedding providers (OpenAI, Ollama, hash)
│   ├── mcp/                          # MCP server for Claude/ChatGPT integration
│   ├── memory/                       # MemoryRecord, lifecycle, confidence decay
│   ├── metrics/                      # Prometheus counters/histograms, health checker
│   ├── poe/                          # Proof of Experience engine (EWMA, domain sim, epochs)
│   ├── store/                        # Storage backends (PostgreSQL, SQLite, BadgerDB)
│   ├── tx/                           # Tx codec (protobuf encode/decode, sign/verify)
│   └── validator/                    # ValidatorSet, quorum logic (>= 2/3 weighted)
├── api/
│   ├── proto/sage/v1/                # Protobuf definitions (tx.proto, query.proto)
│   ├── rest/                         # chi v5 router, handlers, middleware
│   └── openapi.yaml                  # OpenAPI 3.1 specification
├── sdk/python/                       # Python SDK (sync + async clients)
│   ├── src/sage_sdk/                 # Client, auth, models, exceptions
│   ├── tests/                        # pytest tests (mocked httpx via respx)
│   └── examples/                     # Runnable examples
├── web/                              # Brain Dashboard (Preact, Canvas, SSE)
├── deploy/
│   ├── docker-compose.yml            # 11 containers (core network)
│   ├── docker-compose.monitoring.yml # Prometheus + Grafana
│   ├── Dockerfile.abci               # Multi-stage Go build for ABCI nodes
│   ├── Dockerfile.node               # CometBFT validator image
│   ├── init.sql                      # PostgreSQL schema (8 tables + pgvector HNSW)
│   ├── init-testnet.sh               # 4-node testnet config generator
│   └── monitoring/                   # Prometheus config, alerts, Grafana dashboards
├── test/
│   ├── integration/                  # Memory lifecycle, consensus, PoE tests
│   ├── byzantine/                    # BFT fault tolerance tests
│   └── benchmark/                    # k6 load tests
├── papers/                           # Research papers (PDFs, CC BY 4.0)
├── .github/workflows/ci.yml          # CI: lint, test, build, docker, sdk-test
├── Makefile                          # Build/test/deploy targets
├── go.mod                            # Go 1.22, CometBFT v0.38.15
└── .golangci.yml                     # Linter configuration
```

---

## Makefile Reference

| Target | Description |
|--------|-------------|
| `make help` | Show all available targets |
| `make init` | Generate 4-node testnet configuration |
| `make up` | Start the 4-validator network |
| `make up-full` | Start network with Prometheus + Grafana monitoring |
| `make down` | Stop the network (preserves data) |
| `make down-clean` | Stop network and wipe all data volumes |
| `make status` | Check CometBFT node status (block height, sync state) |
| `make logs` | Follow all container logs |
| `make logs-abci` | Follow ABCI application logs only |
| `make build` | Build the `amid` binary to `bin/` |
| `make test` | Run Go unit tests with race detection |
| `make lint` | Run golangci-lint |
| `make fmt` | Format Go code |
| `make vet` | Run go vet |
| `make proto` | Regenerate protobuf code |
| `make integration` | Run integration tests (requires running network) |
| `make benchmark` | Run k6 load tests |
| `make sdk-test` | Run Python SDK tests |
| `make clean` | Remove build artifacts and generated configs |
| `make tidy` | Run go mod tidy |
