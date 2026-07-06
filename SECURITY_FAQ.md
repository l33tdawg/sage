# SAGE Security FAQ

## Deployment Models and Threat Models

SAGE has two distinct deployment models with fundamentally different threat models:

| | SAGE Personal (sage-gui) | SAGE Enterprise |
|---|---|---|
| **What** | Single binary, single-node SAGE | Multi-node BFT consensus with CometBFT |
| **Where** | Localhost only (`127.0.0.1:8080`) | Distributed cluster, 4+ validators |
| **Who** | One user, one local operator | Multiple agents, teams, organizations |
| **Trust model** | User IS the only validator | Byzantine fault tolerance across validators |
| **Database** | Local BadgerDB/SQLite stores in `~/.sage/data/` | BadgerDB consensus state plus off-chain mirror |
| **Release** | v11.0.3 current release | v11.0.2 quorum/federation surface |

Many concerns raised about the enterprise codebase do not apply to SAGE Personal, and vice versa. Each item below is tagged with which deployment it affects.

---

## Current Security Model

### 1. Request Signatures Bind Method, Path, Body, Timestamp, and Optional Nonce

**Applies to:** Enterprise

Authenticated REST requests use Ed25519 signatures over the HTTP method, path including query string, body, timestamp, and optional `X-Nonce`; the server rejects signatures outside the five-minute timestamp window. See `api/rest/middleware/auth.go` and `docs/reference/rest-api.md`.

### 2. Replay Cache and Nonce Support

**Applies to:** Enterprise

Duplicate `(agent_id, signature)` pairs are rejected within the timestamp window, and clients can include `X-Nonce` for concurrent sub-second requests. Consensus transactions also carry monotonic nonces on the chain path.

### 3. Agent Admission and Organizational Scope

**Applies to:** Enterprise

**What it is:** Agent identity is key-based, but what an agent can read or write is governed by on-chain registration, organizations, domain ownership, grants, clearance, and federation scope.

SAGE does not use staking or permissioned key generation. Admission is enforced at the application layer: registering an agent does not automatically grant access to owned domains or classified records.

**SAGE Personal:** Not applicable. The single user runs the single node. There is no multi-agent admission scenario.

### 4. CometBFT RPC Exposed Alongside Authenticated REST API

**Applies to:** Enterprise

**What it is:** In the Docker Compose deployment, CometBFT's RPC port (26657) is exposed on the same network as the authenticated REST API. An attacker with network access could bypass the REST API's authentication by submitting transactions directly to CometBFT.

The Docker Compose configuration is designed for local development and research benchmarking, not production deployment. Production deployments should bind CometBFT RPC to internal-only networks and front the REST API with a reverse proxy. CometBFT RPC should never be internet-facing.

### 5. Docker Security Hardening

**Applies to:** Enterprise (Docker Compose config)

- ABCI containers now run as non-root (`sage` user) in `Dockerfile.abci`. CometBFT nodes remain root due to bind-mount ownership requirements, but are internal-only (no external traffic)
- Added `docker-compose.prod.yml` override with: PostgreSQL SSL required, CORS restricted (no wildcard, must be explicitly set), read-only root filesystems, CometBFT RPC bound to localhost only
- Usage: `docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d`
- The base `docker-compose.yml` retains development defaults for researcher friction reduction

**SAGE Personal:** Not applicable. sage-gui does not use Docker, does not run a database server, and binds only to localhost.

**Remaining:** Secrets management (e.g. Docker secrets, Vault integration) for validator keys. HSM-backed signing for production deployments.

### 6. REST API Acts as Validator-Signing Proxy

**Applies to:** Enterprise

**What it is:** The REST API receives agent requests and signs CometBFT transactions on behalf of the validator. This means the API server holds the validator's signing key and acts as a trusted proxy.

This is an architectural choice to simplify the agent-facing interface. Agents interact via standard REST; the API handles consensus mechanics. The tradeoff is that the API server becomes a high-value target. Production deployments should isolate API hosts and validator keys accordingly.

### 7. Consensus-First Write Ordering

**Applies to:** Enterprise

The REST handler uses `broadcast_tx_commit`, which blocks until the block is finalized. Supplementary off-chain data (embedding vectors, provider metadata, knowledge triples) is staged in a process-local cache and merged into the record by the ABCI app during `FinalizeBlock`. The off-chain store is written to only in `Commit`, inside an atomic transaction. The `InsertMemory` upsert uses `COALESCE` to safely handle multi-validator write races. Memories appear in the query layer after consensus.

### 8. PoE Scoring Is Live Behind Fork Gates

**Applies to:** Enterprise

PoE-weighted quorum is active post-`app-v3`, verdict-correctness and corroboration feed the weight model post-`app-v4`, and domain-aware weighting is active post-`app-v5`. Legacy equal-weight behavior remains only for byte-identical replay of pre-fork blocks.

### 9. No Schema Migration Tooling

**Applies to:** Both (but primarily Enterprise)

**What it is:** Database schema changes are applied via init scripts, not via a migration framework with versioning and rollback.

SAGE Personal uses SQLite with schema creation on first run, which is sufficient for a single-user tool. Enterprise PostgreSQL deployments should manage schema lifecycle as part of deployment operations.

### 10. Benchmark Reproducibility

**Applies to:** Enterprise

The primary benchmark tool is `test/benchmark/load_test.py`, which generates real Ed25519 keypairs per agent, signs requests with the correct canonical format (method + path + body + timestamp), and runs authenticated submission + query benchmarks. Run via `make benchmark`. The k6 scripts are retained for users with k6 Ed25519 extensions but are no longer the default benchmark entry point.

### 11. Byzantine Fault Tests in CI

**Applies to:** Enterprise

The `make byzantine` target and GitHub Actions CI job spin up a 4-validator Docker cluster, verify all nodes are online, and run the Byzantine fault test suite (`test/byzantine/`). Tests cover: 1-of-4 node failure (chain continues), 2-of-4 failure (chain halts), and recovery after restart.

---

## Operating Boundaries

The following boundaries describe the current v11 codebase:

**SAGE Personal (sage-gui v11.0.3):**
- Designed for single-user, single-machine use. Not a networked service.
- No authentication — anyone with access to your machine can access the API on localhost.
- SQLite database supports optional AES-256-GCM encryption at rest (Synaptic Ledger). Enable from CEREBRUM Settings → Security.

**SAGE Enterprise:**
- The 4-node Docker Compose setup is for benchmarking and experimentation.
- CometBFT P2P connections between validators are encrypted by default (SecretConnection: X25519 DH + ChaCha20-Poly1305).
- REST API supports TLS in quorum mode (v6.5+). Certificates are generated during `quorum-init`/`quorum-join` using a per-quorum ECDSA P-256 CA.
- No automated key/certificate rotation yet (planned for governance-driven rotation).
- RBAC is implemented but not battle-tested under adversarial conditions.
- Federation is implemented in v11 as LAN-first or operator-routed SAGE-to-SAGE recall exchange; first-class public-internet/NAT traversal is planned for v11.5.

We do not recommend running SAGE Enterprise in an adversarial or internet-facing environment without addressing the items listed above.

---

## Responsible Disclosure

If you find a security vulnerability in SAGE, please report it responsibly:

- **Email:** security concerns can be sent to the author directly via GitHub ([@l33tdawg](https://github.com/l33tdawg))
- **Do not** open a public GitHub issue for security vulnerabilities
- We will acknowledge receipt within 72 hours and aim to provide a fix or mitigation plan within 30 days
- Credit will be given to reporters in the changelog unless anonymity is requested

---

## Thank You

Security review makes SAGE better. We appreciate researchers who take the time to read the code, question the architecture, and hold us to a high standard. Every concern listed here came from exactly that kind of scrutiny.
