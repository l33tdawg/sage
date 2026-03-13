# SAGE Security FAQ

## Deployment Models and Threat Models

SAGE has two distinct deployment models with fundamentally different threat models:

| | SAGE Personal (sage-gui) | SAGE Enterprise |
|---|---|---|
| **What** | Single binary, single-node, SQLite | Multi-node BFT consensus with CometBFT |
| **Where** | Localhost only (`127.0.0.1:8080`) | Distributed cluster, 4+ validators |
| **Who** | One user, one AI agent | Multiple agents, teams, organizations |
| **Trust model** | User IS the only validator | Byzantine fault tolerance across validators |
| **Database** | SQLite file in `~/.sage/data/` | PostgreSQL |
| **Release** | v4.3.0 (current) | Research/enterprise architecture (Paper 1) |

Many concerns raised about the enterprise codebase do not apply to SAGE Personal, and vice versa. Each item below is tagged with which deployment it affects.

---

## Concerns Addressed

### 1. Request Signatures Only Cover Body + Timestamp, Not HTTP Method/Path

**Applies to:** Enterprise

**What it is:** HMAC request signatures currently sign the request body and a timestamp, but do not include the HTTP method or URL path. This means a signed POST body could theoretically be replayed against a different endpoint.

**Current status:** This is a valid gap in the enterprise signing scheme. SAGE Personal does not use request signatures — it runs on localhost with no network exposure.

**Planned:** Extend the signature envelope to include method, path, and content-type. Tracked for the next enterprise hardening pass.

### 2. No Replay Cache

**Applies to:** Enterprise

**What it is:** Signed requests include a timestamp for freshness, but there is no server-side cache of seen nonces/signatures to reject duplicate submissions within the validity window.

**Current status:** Acknowledged. The timestamp window provides coarse replay protection, but a dedicated nonce cache is needed for robust replay prevention.

**Planned:** Add a bounded nonce cache with TTL matching the timestamp validity window.

### 3. Agent Keys Are Free to Mint (No Admission/Stake)

**Applies to:** Enterprise

**What it is:** Any agent can generate a keypair and begin submitting memories. There is no admission control, staking mechanism, or registration gate.

**Current status:** This is by design for the research prototype. The RBAC and federation layer (described in Paper 1) provides organizational admission control, but it is not enforced at the key-generation level today.

**SAGE Personal:** Not applicable. The single user runs the single node. There is no multi-agent admission scenario.

**Planned:** Enterprise deployments will require key registration via an admin endpoint before an agent can submit transactions.

### 4. CometBFT RPC Exposed Alongside Authenticated REST API

**Applies to:** Enterprise

**What it is:** In the Docker Compose deployment, CometBFT's RPC port (26657) is exposed on the same network as the authenticated REST API. An attacker with network access could bypass the REST API's authentication by submitting transactions directly to CometBFT.

**Current status:** Valid concern. The Docker Compose configuration is designed for local development and research benchmarking, not production deployment.

**Planned:** Production deployment guides will bind CometBFT RPC to internal-only networks and front the REST API with a reverse proxy. CometBFT RPC should never be internet-facing.

### 5. Wildcard CORS, Default DB Passwords, Containers Running as Root

**Applies to:** Enterprise (Docker Compose config)

**What it is:** The development Docker Compose setup uses `CORS: *`, default PostgreSQL credentials, and containers run as root.

**Current status:** These are development defaults. They exist to minimize setup friction for researchers reproducing the Paper 1 benchmarks. They are not intended for production.

**SAGE Personal:** Not applicable. sage-gui does not use Docker, does not run a database server, and binds only to localhost.

**Planned:** Add a `docker-compose.prod.yml` with locked-down CORS, secrets management, and non-root containers. Document the difference clearly.

### 6. REST API Acts as Validator-Signing Proxy

**Applies to:** Enterprise

**What it is:** The REST API receives agent requests and signs CometBFT transactions on behalf of the validator. This means the API server holds the validator's signing key and acts as a trusted proxy.

**Current status:** This is an architectural choice to simplify the agent-facing interface. Agents interact via standard REST; the API handles consensus mechanics. The tradeoff is that the API server becomes a high-value target.

**Planned:** Evaluate moving to a model where the API server submits unsigned proposals and a separate signing service (or HSM-backed signer) handles validator key operations.

### 7. Pre-Commit Writes to Postgres Before Consensus Finalizes

**Applies to:** Enterprise

**What it is:** Memories are written to PostgreSQL during `CheckTx`/`DeliverTx` before the CometBFT block is finalized via `Commit`. If consensus fails, the database may contain uncommitted state.

**Current status:** This is a known deviation from strict consensus-first ordering. In practice, with a single-validator or cooperative 4-node testnet, the window for inconsistency is small. It is not acceptable for adversarial deployments.

**Planned:** Move to write-ahead pattern: buffer in memory during `DeliverTx`, flush to Postgres only in `Commit`. This is a prerequisite for production enterprise use.

### 8. PoE Scoring Exists but Live System Uses Weight 1.0

**Applies to:** Enterprise

**What it is:** The Proof-of-Experience (PoE) scoring framework is implemented in the codebase, but the live system applies a uniform weight of 1.0, effectively bypassing differentiated scoring.

**Current status:** Correct. The PoE framework is functional code validated in Paper 2's controlled experiments. The default weight of 1.0 means all memories are treated equally in the current deployment. This is intentional for the initial release — PoE tuning requires per-domain calibration.

**Planned:** Expose PoE weight configuration and provide calibration guidance in the enterprise deployment docs.

### 9. No Schema Migration Tooling

**Applies to:** Both (but primarily Enterprise)

**What it is:** Database schema changes are applied via init scripts, not via a migration framework with versioning and rollback.

**Current status:** Acknowledged. SAGE Personal uses SQLite with schema creation on first run, which is sufficient for a single-user tool. Enterprise PostgreSQL deployments need proper migrations.

**Planned:** Adopt a migration tool (golang-migrate or similar) for the enterprise schema. sage-gui will continue to self-initialize on startup.

### 10. Benchmark Not Reproducible (k6 Uses Placeholder Auth)

**Applies to:** Enterprise

**What it is:** The k6 load-testing scripts use placeholder authentication tokens, making it difficult for external researchers to reproduce the Paper 1 benchmarks exactly.

**Current status:** The benchmarks in Paper 1 were run against an authenticated cluster with real keys. The published k6 scripts were sanitized for release. This makes independent reproduction harder than it should be.

**Planned:** Provide a `make benchmark` target that spins up a fresh cluster, generates test keys, and runs the k6 suite end-to-end.

### 11. No Multi-Node Integration or Byzantine Tests in CI

**Applies to:** Enterprise

**What it is:** CI runs unit tests but does not spin up a multi-node CometBFT cluster or inject Byzantine faults.

**Current status:** Valid. Multi-node testing was performed manually for the research papers. It is not automated in CI due to resource requirements (4-node cluster + CometBFT).

**Planned:** Add a CI stage that runs a 4-node Docker cluster with basic liveness and consensus tests. Full Byzantine fault injection is a longer-term goal.

---

## Known Limitations

The following are honest limitations of the current codebase:

**SAGE Personal (sage-gui v4.3.0):**
- Designed for single-user, single-machine use. Not a networked service.
- No authentication — anyone with access to your machine can access the API on localhost.
- SQLite database supports optional AES-256-GCM encryption at rest (Synaptic Ledger). Enable from CEREBRUM Settings → Security.

**SAGE Enterprise:**
- The enterprise deployment is a research prototype, not production-hardened infrastructure.
- The 4-node Docker Compose setup is for benchmarking and experimentation.
- No TLS between validators in the default configuration.
- No automated key rotation or certificate management.
- RBAC is implemented but not battle-tested under adversarial conditions.
- Federation (cross-org memory sharing) is designed but not yet deployed at scale.

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
