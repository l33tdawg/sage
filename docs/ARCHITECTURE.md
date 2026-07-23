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
9. [Pipeline Architecture (Agent-to-Agent Messaging)](#pipeline-architecture-agent-to-agent-messaging)
10. [Task Management](#task-management)
11. [CLI Tools](#cli-tools)
12. [REST API](#rest-api)
13. [Monitoring](#monitoring)
14. [Testing](#testing)
15. [Troubleshooting](#troubleshooting)
16. [Repository Structure](#repository-structure)
17. [Makefile Reference](#makefile-reference)

---

## Architecture Overview

![SAGE Architecture — Self-Governed, Byzantine Fault Tolerant Memory Infrastructure for AI Agents](sage_architecture-17062026.png)

*Editable vector source: `docs/sage_architecture-17062026.svg` (kept locally). Regenerate this PNG with: `rsvg-convert -w 2400 docs/sage_architecture-17062026.svg -o docs/sage_architecture-17062026.png`. Reconciled against [`docs/reference/`](reference/INDEX.md) for v11.*

### How it works

1. **Agents** connect to any ABCI node via the Python SDK (or raw HTTP)
2. **ABCI nodes** (Go) process requests -- memory submissions become signed transactions
3. Transactions are broadcast to **CometBFT** which runs BFT consensus across all 4 validators
4. Once a block is committed, the state machine writes **BadgerDB** (on-chain: content hashes, status, classification, votes, grants, appHash -- replicated to every node) and projects full content + embedding vectors to the **off-chain store** -- **SQLite** in personal/single-binary mode, **PostgreSQL + pgvector** in a cluster -- as a write-behind update inside `Commit` (never `FinalizeBlock`)
5. **Embeddings** come from a pluggable provider -- **Ollama** (`nomic-embed-text`, 768-dim, fully local), an **OpenAI-compatible** endpoint (vLLM, LiteLLM), or a non-semantic **hash** fallback -- staged in a process-local cache before broadcast

No tokens. No gas fees. No cryptocurrency. Just consensus-validated knowledge.

| Layer | Technology |
|-------|-----------|
| Consensus | CometBFT v0.38.23 (ABCI 2.0, raw -- not Cosmos SDK) |
| State Machine | Go 1.25.7+ ABCI application |
| On-chain State | BadgerDB v4.5.0 (content hash, status, classification, votes, grants, appHash) |
| Off-chain Storage | SQLite (personal / single-binary) or PostgreSQL 16 + pgvector / HNSW (cluster) -- write-behind projection |
| Tx Format | Protobuf (deterministic serialization) |
| REST API | Go chi v5.2.2 (62 endpoints, OpenAPI 3.1) |
| Agent SDK | Python (httpx + PyNaCl + Pydantic v2) |
| Embeddings | Pluggable: Ollama nomic-embed-text (768-dim) / OpenAI-compatible (vLLM, LiteLLM) / hash fallback |
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
| Go | 1.25.7+ | Only needed for local builds and running tests |
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

- Builds CometBFT v0.38.23 from source inside a Docker container (if the exact version is not locally installed)
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
| TLS REST API (quorum) | `localhost:8443` | — | — | — |
| Prometheus metrics (ABCI) | `localhost:2112` | `localhost:2113` | `localhost:2114` | `localhost:2115` |
| CometBFT Prometheus | `:26660` | `:26761` | `:26862` | `:26963` |

### Shared Services

| Service | Port | Notes |
|---------|------|-------|
| PostgreSQL | `localhost:5432` | Shared by all ABCI nodes, `ON CONFLICT DO NOTHING` for multi-writer safety |
| Ollama | `localhost:11434` | Shared embedding model server |
| Grafana | `localhost:3000` | Only with `make up-full` |
| Prometheus | `localhost:9191` | Only with `make up-full` |

### Transport Security (v6.5)

| Layer | Encryption | Protocol |
|-------|-----------|----------|
| CometBFT P2P (26656) | **Encrypted** — SecretConnection (X25519 DH + ChaCha20-Poly1305) | STS protocol, built into CometBFT v0.38.23 |
| REST API (8443, quorum mode) | **TLS 1.3** — per-quorum ECDSA P-256 CA, node certs with IP/DNS SANs | `internal/tlsca/` package |
| REST API (8080, localhost) | Plain HTTP | Localhost only, for dashboard/MCP |
| ABCI (26658, Docker) | Plain TCP | Docker bridge network isolation |

**Certificate lifecycle:**
- `sage-gui quorum-init` generates the quorum CA + this node's TLS cert
- `sage-gui quorum-join` receives CA via manifest, generates joining node's cert
- Certs stored in `~/.sage/certs/{ca.crt, ca.key, node.crt, node.key}`
- `sage-gui cert-status` shows expiry dates and fingerprints

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

New recommended way for multi-agent setups (Claude Code in tmux, etc.):

```python
identity = AgentIdentity.default()   # automatically respects SAGE_IDENTITY_PATH env var
```

This is the clean way to give each agent its own permanent key file while still sharing the same memory ledger.

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

### Clearance Level Details

Beyond the operational access levels (0-4) used for read/write/validate/admin gating, clearance levels also classify the sensitivity of data itself:

| Level | Name | Description |
|-------|------|-------------|
| 0 | Public | No registration needed. Open data accessible to any agent. |
| 1 | Internal | Default for registered domains. Requires agent registration. |
| 2 | Confidential | Restricted access. Requires explicit domain grant from an admin. |
| 3 | Secret | High-security data. Limited to agents with clearance 3+ and explicit grants. |
| 4 | Top Secret | Maximum restriction. Only clearance-4 agents with direct grants can access. |

Memories inherit the clearance level of the domain they are submitted to. An agent must have a clearance level >= the domain's clearance level to interact with memories in that domain.

### Domain Ownership (First-Write-Wins)

Domains have an on-chain owner. When an agent submits a memory to a domain that does not yet exist, the chain **auto-registers the domain with that agent as the owner** and grants the owner a level-2 access entry. Subsequent writes from *other* agents to the same domain go through `HasAccessMultiOrg`, which requires one of:

- A direct `access_grant` issued by the owner — `POST /v1/access/grant`
- Same-org clearance — both agents belong to the same org and the writer's clearance covers the memory's classification
- An active federation between the writer's org and the owner's org

Without one of those paths, the tx is rejected at `FinalizeBlock` with `code 11: access denied: agent <id> has no write access to domain <name>`. This is intentional — it prevents a hostile or buggy agent from poisoning another agent's namespace by impersonating the domain.

**Shared domains** (no single owner, writable by any authenticated agent, never auto-registered):

- Exact match: `general`, `self`, `meta`
- Prefix match: `sage-*` (added in v6.8.2 to cover cross-cutting SAGE-meta domains like `sage-debugging`, `sage-development`, `sage-rbac-debug`)

If you want a domain that multiple agents write to freely, either give it a name in the shared set, or use the cross-agent write recipe below.

#### Cross-agent write recipe

When agent A needs to write to a domain that agent B owns (or that hasn't been written to yet), the safe bootstrap sequence is:

1. **Determine the real on-chain owner first.** The "intended" owner in your application code may not be the actual owner — whichever agent wrote first captured the domain. Probe ownership with `GET /v1/domain/{name}` before issuing any grant. If no owner exists yet, step 2 will claim it; if a different agent than expected owns it, grants must come from *that* agent's signing key.
2. **For unclaimed domains**, the future owner can either issue `POST /v1/domain/register` explicitly or issue the first `POST /v1/access/grant`; post-v8 access grants auto-claim genuinely unowned, non-shared domains for the granter before writing the grant.
3. **Once ownership is settled**, the owner issues `POST /v1/access/grant` granting the writer level 2 (read+write). In v11/app-v15, level 3 can be granted for modify/deprecate workflows.
4. **The writer can now submit memories** to that domain. Ancestor grants are honored post-v8, so a grant on `pipeline.failures` covers descendants such as `pipeline.failures.pwn_buffer_overflow` unless a stricter ownership path intervenes.

#### Current v11 semantics

- **Ancestor grants are active.** Access checks walk from the concrete domain toward parent domains post-v8, so parent-domain ownership and grants can cover subdomains.
- **Access-control REST handlers use commit-mode broadcasts.** `POST /v1/access/grant` waits for `FinalizeBlock`; underlying consensus rejections surface as request errors instead of returning a false-success `201`.
- **Lost-owner recovery exists through governance.** `domain_reassign` can transfer a domain and optionally open it as a shared domain, preserving an auditable recovery path when an owner key is retired or lost.

### Domain Access + Clearance Interaction

Access to a memory requires passing three checks, evaluated in order:

1. **Clearance gate** — the agent's clearance level must be >= the domain's classification level. An agent with clearance 1 cannot access a domain classified at level 2, regardless of other grants.
2. **Domain access grant** — the agent's `domain_access` map must include the target domain with the appropriate permission (`read: true` for queries, `write: true` for proposals/votes/challenges).
3. **Department membership** — if the domain is scoped to a department, the agent must be a member of that department (or the parent organization's admin).

**Federation caps:** When two organizations establish a federation agreement, they set a `max_clearance` ceiling. Even if an agent in Org B has clearance 4, their effective clearance for federated domains from Org A is capped by the federation agreement (e.g., `max_clearance: 2` means they can only see level-0, level-1, and level-2 data from Org A).

**Precedence:** The most restrictive rule wins. If an agent has clearance 3 but the federation cap is 2, their effective clearance for federated domains is 2. If an agent has clearance 3 and a domain grant with `write: true`, but the domain is classified at level 4, they cannot access it at all.

### Common RBAC Patterns

**Read-only research domain:**
- Create a domain `research` with classification level 1 (Internal)
- Grant agents `{"research": {"read": true, "write": false}}`
- Only designated curators get `write: true` to add new research memories

**Write-restricted operational domain:**
- Domain `ops` at classification level 2 (Confidential)
- Field agents get `{"ops": {"read": true, "write": true}}` with clearance 2
- Analysts get `{"ops": {"read": true, "write": false}}` — they can query but not add data

**Cross-department visibility via federation:**
- Org A (Red Team) and Org B (Blue Team) federate with `max_clearance: 1`
- Both sides can see each other's Internal-level findings
- Secret and Top Secret data stays within each org

**Troubleshooting: "agent can't see memories":**
1. Check the agent's clearance level — is it >= the domain's classification? (`GET /v1/agent/me` shows current clearance)
2. Check domain access grants — does the agent have `read: true` for the target domain?
3. Check `visible_agents` — if the field is set, the agent can only see memories from listed agent IDs. An empty list or `"*"` means no restriction.
4. Check federation caps — for cross-org queries, is the federation `max_clearance` high enough?
5. Check memory status — is the agent querying for `committed` memories but the memory is still `proposed`?

---

## Upgrade Machinery

SAGE includes an in-place chain-upgrade substrate: existing chains move forward across consensus-rule changes without a chain reset and without losing accumulated memory. How much of it is hands-off depends on the fork: the upgrade watchdog auto-proposes only up to its frozen deployment-safe default (app-v6), while every later fork (app-v7 onward) is governance-activated by an operator running `sage-gui upgrade propose --target <next>` one fork at a time (post-app-v8, signed by a chain-admin agent). Personal single-validator nodes also have auto-advance support for later forks, preserving the no-typed-commands experience. The substrate has four moving parts:

### 1. Scheduled snapshots

Every committed block triggers `internal/abci/snapshot_sched.go::SnapshotScheduler.Tick`. The scheduler decides whether to fire `internal/snapshot.Take` based on cadence — by default every 10,000 blocks AND every 6 hours. Snapshots land at `~/.sage/data/snapshots/<height>/` containing:

- `manifest.json` — height, AppHash, binary version, chunk hashes
- `badger.backup` — full `(*badger.DB).Backup` stream
- `sage.db` — VACUUM INTO of the offchain mirror
- `cometbft-data.tar.zst` — blockstore/state/tx_index/evidence
- `config.tar.zst` — genesis + node key + validator key + vault.key
- `binary/sage-gui-<version>` — the running binary, bundled for rollback
- `OK` — zero-byte sentinel written last under fsync

Atomicity: stage in `.staging-<height>-<reason>/`, fsync, rename. Boot's `SweepStaging` removes any incomplete staging directories. Retention: K=5 newest + one anchor per distinct `BinaryVersion` (powers downgrade).

Snapshot encryption inherits the vault posture. If the Synaptic Ledger vault is unlocked at boot, snapshots are wrapped in a streaming Argon2id + AES-256-GCM envelope keyed off the vault passphrase. Plaintext otherwise.

### 2. Upgrade proposal and activation

A new chain has app version 1. The running binary embeds an `upgradeTargetAppVersion` constant. When the binary boots and the chain's app version is less than the embedded target, the `upgrade_watchdog` goroutine in `cmd/sage-gui/upgrade_watchdog.go`:

1. Signs an `UpgradePropose` tx using the node operator's agent key (`~/.sage/agent.key`)
2. Broadcasts it through CometBFT's `/broadcast_tx_sync` RPC
3. Stops on success or on "already pending" (terminal conditions)

`processUpgradePropose` in `internal/abci/app.go` persists the proposal as an `UpgradePlanRecord` in BadgerDB keyed at `upgrade:plan`. The activation height is **chain-computed deterministically** at proposal-execution time as `req.Height + max(payload.UpgradeDelayBlocks, defaultUpgradeDelayBlocks=200)`. Every validator sees the same inputs and computes the same activation height — no multi-validator drift.

At the activation height, `FinalizeBlock` reads the pending plan, emits `ResponseFinalizeBlock.ConsensusParamUpdates.Version.App = TargetAppVersion`, and calls `MarkUpgradeApplied` (which atomically clears the pending plan and writes an audit record at `upgrade:applied:<name>`). CometBFT applies the new app version at H+1 across every replica.

At-most-one pending plan is enforced: a propose arriving while another is pending returns code 47 "already pending". A pre-activation `UpgradeCancel` (signed by any agent) clears the plan; post-activation cancel returns code 48 "too late".

**Quiescent chains (app-v12+).** Once the idle-chain fix is active, an idle chain mints no blocks — so a pending plan's activation height never arrives on its own. The always-on **pending-plan pump** (`cmd/sage-gui/upgrade_pump.go`) watches the `upgrade:plan` slot in-process: whenever a plan is pending and the height is stagnant below its activation height, it broadcasts idempotent heartbeat txs (operator re-registration, Code 0 "already registered") that each mint one block, carrying the chain to activation. It runs independently of auto-advance, admin roles, and `disable_auto_upgrade` — the plan already passed governance; the pump is execution, not an upgrade decision. Interactively, `sage-gui upgrade propose --wait` does the same climb attached to the terminal.

### 3. Halt detection

If the chain binary panics (e.g. an upgrade handler failure), `cmd/sage-gui/halt_writer.go::haltOnPanic` runs in `runServe`'s deferred recover. It writes `~/.sage/data/HALT` atomically with the failed version, an optional rollback target (empty by default — the launcher picks the latest non-failed anchor), and the panic message. The deferred handler re-panics so the original stack still reaches stderr and the process exits non-zero.

### 4. Supervised rollback

`cmd/sage-launcher` runs in `--supervise` mode as the parent process of the chain binary. On child exit it:

1. Reads `~/.sage/data/HALT`. Absent → propagate exit code (healthy path); present → enter rollback.
2. Resolves the rollback anchor: `findLatestRollbackAnchor` scans `~/.sage/data/snapshots/` newest-first by mtime, picks the first directory whose `manifest.BinaryVersion != HALT.FailedVersion`.
3. Locates the rollback binary at `<anchor>/binary/sage-gui-<version>`.
4. Calls the injected `Restorer` (production: `internal/snapshot.Restore`) which Verify-gates the anchor (chunk hashes + SQLite integrity check + badger backup replayed into a tmpdir with `ComputeAppHash` comparison) before atomically swapping in BadgerDB, SQLite, and CometBFT data.
5. Writes a `ROLLBACK_TRIGGERED` event to `~/.sage/launcher.log`.
6. Clears the HALT sentinel.
7. `syscall.Exec`s the rollback binary, replacing the launcher process. PID continuity preserved so outer supervisors (launchd / systemd) keep the same PID through the swap.

Crash-loop circuit breaker: 3 non-HALT crashes within a 60s sliding window halts the supervisor with a clear error — better to surface "this binary cannot start" than to thrash in a restart loop.

### Operator behaviour

For sovereign single-machine users (the dominant deployment), the entire substrate is invisible. Drop the current binary into `~/.sage/bin/`, restart `sage-gui`, and it just works:

- No `sage-cli` subcommand is introduced.
- The existing detached-launcher flow (used by macOS `.app` / Windows `.exe` double-click) is preserved. `--supervise` is opt-in.
- Snapshots are taken in the background and consume disk under `~/.sage/data/snapshots/`. Retention is bounded.

For federated multi-validator deployments, the chain-computed activation height guarantees byte-identical state transitions across replicas without operator coordination.

### Wire format contracts

The HALT sentinel field names (`failed_version`, `rollback_to`, `failure_message`, `timestamp`) are stable wire format between the chain binary's `cmd/sage-gui/halt_writer.go::haltSignal` and the launcher's `cmd/sage-launcher/halt_sentinel.go::HaltSignal`. The manifest's `taken_at` is an RFC3339 string (Go's default JSON encoding of `time.Time`), NOT a unix-epoch int64.

---

## Agent Management & Network Topology

SAGE v3.0 introduced a built-in agent management system for multi-agent networks. **v3.5 makes agent identity a first-class on-chain concept** — registration, updates, and permission changes go through CometBFT consensus for auditability and tamper resistance.

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

**On-Chain Fields (v3.5+):**

| Column | Type | Description |
|--------|------|-------------|
| `on_chain_height` | INTEGER | Block height where agent was registered on-chain (0 = legacy/pre-v3.5) |
| `visible_agents` | TEXT | JSON array of agent IDs this agent can read from ("*" or empty = all) |
| `provider` | TEXT | MCP provider identifier (e.g., "claude-code", "cursor") |

On-chain state is also stored in BadgerDB under the `agent:{agentID}` key prefix, containing clearance, domain access, and visibility rules. The ABCI app processes three new transaction types:

| Tx Type | ID | Who Sends | Purpose |
|---------|---:|-----------|---------|
| `AgentRegister` | 20 | Agent (self) | Register on chain with name, bio, provider |
| `AgentUpdate` | 21 | Agent (self) | Update own name/bio |
| `AgentSetPermission` | 22 | Admin | Set clearance, domain access, visible agents |

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

**Auto-Registration (v3.5+):** Agents connecting via MCP for the first time automatically register on-chain during the boot sequence (`sage_inception` -> `autoRegister`). The registration is idempotent — calling it again returns the existing record.

**Permission Enforcement (v3.5+):** Memory operations check on-chain state (BadgerDB) first for clearance and domain access. If the agent isn't registered on-chain (legacy), it falls back to the SQLite record. The `visible_agents` field filters query results — agents only see memories from agents in their visibility list (or all, if the list is empty/"*").

### Agent Registration & Updates (v5.0.1+)

Agents register on-chain via REST, providing identity metadata that is committed through BFT consensus:

**Register:** `POST /v1/agent/register` — creates a new agent record on-chain. Required fields: `name`, `role`, `boot_bio`, `provider`. The agent's Ed25519 public key (from the `X-Agent-ID` header) becomes its permanent identifier. Registration is idempotent.

**Update Profile:** `PUT /v1/agent/update` — agents update their own `name` or `boot_bio`. The request is signed and committed on-chain for auditability.

**Set Permissions:** `PUT /v1/agent/{id}/permission` — admin-only. Sets `clearance`, `domain_access`, and `visible_agents` for a target agent. This is the only way to change an agent's access level after registration.

**Agent Roles (v5.0.1):**

| Role | Description |
|------|-------------|
| `member` | Default role. Can read and write based on clearance and domain access |
| `admin` | Full control. Can set permissions for other agents, manage domains |
| `observer` | Read-only. Blocked from all write operations regardless of domain grants |

**Visibility Controls:** The `visible_agents` field is a JSON array of agent IDs. When set, the agent can only see memories authored by agents in that list. An empty array or `"*"` means the agent sees all memories (subject to domain and clearance checks). This allows fine-grained compartmentalization — e.g., a review agent that only sees output from a specific working group.

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
3. New agent runs `sage-gui pair <code>` or enters the code in their setup wizard
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

## Pipeline Architecture (Agent-to-Agent Messaging)

Introduced in v5.0.1, the pipeline is a direct messaging system between agents. Unlike shared memories (which are broadcast knowledge), pipeline messages are routed point-to-point — one agent sends a message to a specific agent (by ID) or to any agent matching a provider (e.g., `"claude-code"`).

### How It Works

The pipeline provides structured, asynchronous communication between agents without polluting the shared memory pool. Messages are short-lived (default 60-minute TTL) and designed for coordination, not permanent knowledge storage.

### Message Lifecycle

```
send → pending → claimed → completed
                    ↘ expired (TTL exceeded)
```

1. **Send** — Agent A posts a message via `POST /v1/pipe/send`, targeting a specific `agent_id` or `provider`. The message enters `pending` status.
2. **Pending** — The message sits in the recipient's inbox. Recipients poll via `GET /v1/pipe/inbox` to discover new messages.
3. **Claimed** — The recipient claims the message via `PUT /v1/pipe/{id}/claim`, signaling that work has begun. This prevents other agents from claiming the same message.
4. **Completed** — The recipient posts a result via `PUT /v1/pipe/{id}/result`. The sender can retrieve results via `GET /v1/pipe/results`.
5. **Expired** — If the TTL elapses before the message is claimed or completed, it transitions to `expired` and is no longer actionable.

### Auto-Journaling

When a pipeline message reaches `completed` status, SAGE automatically creates a memory entry (memory_type `journal`) capturing the exchange. This means significant inter-agent coordination is preserved in the knowledge base without agents needing to manually record it.

### TTL and Expiry

- **Default TTL:** 60 minutes
- **Maximum TTL:** 1440 minutes (24 hours)
- Senders can set a custom TTL per message within these bounds
- Expired messages are soft-deleted — they remain queryable by ID but are excluded from inbox results

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/v1/pipe/send` | Send a message to a target agent or provider |
| `GET` | `/v1/pipe/inbox` | List pending messages for the authenticated agent |
| `PUT` | `/v1/pipe/{id}/claim` | Claim a pending message (marks it in-progress) |
| `PUT` | `/v1/pipe/{id}/result` | Post a result for a claimed message |
| `GET` | `/v1/pipe/{id}` | Get a specific message by ID (any status) |
| `GET` | `/v1/pipe/results` | List completed messages with results for the sender |

### Use Cases

- **Cross-provider coordination** — A Claude Code agent delegates a subtask to a Cursor agent by sending a pipeline message targeted at provider `"cursor"`. The Cursor agent picks it up, completes it, and posts the result.
- **Task delegation** — An admin agent breaks a large task into subtasks and sends each to a specialized agent via the pipeline.
- **Inter-agent review** — Agent A submits work, then pipes a review request to Agent B. Agent B claims it, reviews, and posts feedback as the result.

---

## Task Management

v5.0.1 introduces first-class task tracking via the memory system. Tasks are memories with `memory_type="task"` and carry additional status metadata.

### Task Status Lifecycle

```
planned → in_progress → done
                ↘ dropped
```

- **planned** — Task is defined but work has not started
- **in_progress** — Active work underway
- **done** — Task completed successfully
- **dropped** — Task abandoned or deprioritized

### REST Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/v1/memory/tasks` | List all task memories, filterable by status |
| `PUT` | `/v1/memory/{id}/task-status` | Update a task's status (e.g., `planned` to `in_progress`) |

Tasks follow the same consensus and RBAC rules as regular memories — they are proposed, validated, and committed on-chain. The task status field is an additional metadata layer that does not affect the memory's consensus status.

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

All authenticated endpoints require three headers; current clients should also
send the optional nonce header:

| Header | Value |
|--------|-------|
| `X-Agent-ID` | Hex-encoded Ed25519 public key |
| `X-Signature` | Ed25519 signature of `SHA-256(method + " " + path[?query] + "\n" + body) + big-endian int64(timestamp) [+ nonce]` |
| `X-Timestamp` | Unix epoch seconds |
| `X-Nonce` | Optional hex bytes; current clients send 8 random bytes |

The Python SDK handles signing automatically. For raw HTTP access, construct the
canonical method/path/body string exactly as shown above, hash it, append the
timestamp and decoded nonce bytes, and sign with your Ed25519 private key. After
app-v17 activation, consensus independently binds a delegated proof to that
exact action, deterministic block-time freshness, and a single-use AppHash
marker; see `docs/reference/rest-api.md` for the authoritative details.

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
| `POST` | `/v1/memory/pre-validate` | No | Dry-run the per-node validation checks without on-chain submission |
| `GET` | `/v1/memory/list` | Yes | List memories with filtering (domain, status, agent, sort) |
| `GET` | `/v1/memory/timeline` | Yes | Time-bucketed memory history (hour/day/week granularity) |
| `POST` | `/v1/memory/link` | Yes | Create a link between two related memories |
| `GET` | `/v1/memory/tasks` | Yes | List task memories, filterable by status |
| `PUT` | `/v1/memory/{id}/task-status` | Yes | Update a task memory's status |
| `POST` | `/v1/agent/register` | Yes | Register agent on-chain (name, role, boot_bio, provider) |
| `PUT` | `/v1/agent/update` | Yes | Update agent's own profile (name, boot_bio) |
| `PUT` | `/v1/agent/{id}/permission` | Yes | Admin: set clearance, domain access, visible_agents |
| `POST` | `/v1/pipe/send` | Yes | Send a pipeline message to an agent or provider |
| `GET` | `/v1/pipe/inbox` | Yes | List pending pipeline messages for the agent |
| `PUT` | `/v1/pipe/{id}/claim` | Yes | Claim a pending pipeline message |
| `PUT` | `/v1/pipe/{id}/result` | Yes | Post result for a claimed pipeline message |
| `GET` | `/v1/pipe/{id}` | Yes | Get a specific pipeline message by ID |
| `GET` | `/v1/pipe/results` | Yes | List completed pipeline messages with results |
| `GET` | `/health` | No | Liveness probe |
| `GET` | `/ready` | No | Readiness probe (checks PostgreSQL + CometBFT) |

### Memory List & Timeline (v5.0.1+)

**`GET /v1/memory/list`** supports flexible filtering and sorting:

| Parameter | Type | Description |
|-----------|------|-------------|
| `domain` | string | Filter by domain tag |
| `status` | string | Filter by memory status (proposed, committed, challenged, deprecated) |
| `agent` | string | Filter by authoring agent ID |
| `memory_type` | string | Filter by type (fact, observation, task, journal, etc.) |
| `sort` | string | Sort order: `newest`, `oldest`, `confidence` |
| `limit` | int | Max results (default 50) |
| `offset` | int | Pagination offset |

**`GET /v1/memory/timeline`** returns memories grouped into time buckets for visualization:

| Parameter | Type | Description |
|-----------|------|-------------|
| `bucket` | string | Granularity: `hour`, `day`, `week` |
| `domain` | string | Optional domain filter |
| `agent` | string | Optional agent filter |

**`POST /v1/memory/link`** creates a directional relationship between two memories (e.g., a task linked to the journal entry that resulted from it). Both memory IDs must exist and the agent must have read access to both.

**`POST /v1/memory/pre-validate`** performs a dry-run through the per-node validation checks (dedup, quality, consistency) without submitting on-chain. Useful for testing memory content before committing.

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
2. Each node's memory auto-voter runs the validation checks (dedup, quality, consistency) and signs ONE vote transaction with the node's own consensus key, broadcast through CometBFT
3. On a multi-node chain every node votes with its own key; agents can also vote via `/v1/memory/{id}/vote`
4. When quorum is reached (>= 2/3 weighted vote), status advances to `committed`
5. Other agents can corroborate (strengthens confidence) or challenge (triggers review)
6. Challenged memories may be deprecated based on evidence

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

**Cause:** Network issues pulling the Go image or cloning CometBFT. Retry, or install CometBFT v0.38.23 locally:

```bash
git clone --branch v0.38.23 --depth 1 https://github.com/cometbft/cometbft.git
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

> **Current quorum weighting:** the quorum check consults each validator's PoE weight on current-chain blocks. Legacy equal-weight branches remain only for byte-identical replay before the relevant fork activations. Weights are computed every epoch boundary by `processEpoch` and persisted on-chain at `poew:<validatorID>` (IEEE-754 BE float64) + `poew:current` (uvarint epoch number), so a node restart between epoch boundaries hydrates the same weight set its peers are running with. Validators added mid-epoch (PoEWeight == 0 until the next boundary) fall back to `1/N` via `poeWeightOrFallback` until they earn a real one.
>
> **Current accuracy + corroboration:** `accuracy` is the verdict-correctness EWMA (`poe.EWMATracker.Accuracy()` over "did this validator's vote match the final committed/deprecated verdict"), and `corr_score` is `CorroborationScore(CorrCount)` over the validator's lifetime verdict-match count. Both are credited at the terminal-verdict transition in `checkAndApplyQuorum` and persisted in the `vstats:<validatorID>` record.
>
> **Current domain factor:** for a memory in a non-shared domain `D`, `checkAndApplyQuorum` weights each validator's vote by its **verdict-correctness in `D`** — a per-domain verdict-correctness EWMA persisted at `vstats_domain:<validatorID>:<D>`, with global accuracy/recency/corroboration recomputed live from `vstats:<validatorID>`. The memory's domain is recorded at submit time in `memdomain:<memoryID>`. Shared catch-all domains (`general`, `self`, `meta`, and the `sage-*` prefix family) and unknown legacy memories fall back to the scalar `PoEWeight`; domain weighting applies where subject-matter expertise is meaningful.

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
│   └── sage-gui/                    # SAGE Personal (setup, serve, MCP)
├── internal/
│   ├── abci/                         # ABCI 2.0 state machine (FinalizeBlock, Commit)
│   ├── voter/                        # per-node memory auto-voter (dedup, quality, consistency checks)
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
├── go.mod                            # Go 1.25.7, CometBFT v0.38.23 (library)
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
