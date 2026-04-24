# Admin Bootstrap

How to bring up a SAGE node and configure agent visibility for your deployment
pattern. This doc is the canonical answer to "I just stood up a node and my
agents can't see each other - what do I do?"

If you're hitting empty `/v1/memory/list` or `/v1/memory/query` results
post-deployment, jump to [Chain-reset visibility gotcha](#chain-reset-visibility-gotcha).

## Deployment patterns

SAGE supports three visibility patterns. Pick the one that matches your trust
model - the configuration differs significantly.

| Pattern             | Trust model                                                 | Config cost                       |
| ------------------- | ----------------------------------------------------------- | --------------------------------- |
| Single-org          | Everyone in the deployment trusts each other fully          | One org + TopSecret clearance     |
| Multi-org federated | Multiple orgs, selective cross-org sharing                  | Orgs + federation agreements      |
| Homogeneous-trust   | Everyone trusts everyone - legacy / research deployments    | `visible_agents="*"` per agent    |

The homogeneous-trust pattern is no longer necessary as of v6.6.2 - use
single-org with TopSecret clearance instead (see below).

## The who-first problem

A SAGE node has no root account on boot. The first agent that submits *any*
transaction to CometBFT is just "an agent" - no special status. Admin privileges
are minted by the first `AgentSetPermission` tx with `role=admin`, which has
to be broadcast by *something*. That something is usually one of:

1. **`sage-gui` local seed flow** - the desktop app, on first launch, generates
   an agent keypair and posts a self-permission tx elevating itself to `admin`
   via `PUT /v1/agent/{self}/permission` with `clearance=4` and `role=admin`.
   This is the default bootstrap for operator-run nodes.
2. **Manual curl** - any keypair you control can sign a request to the local
   node. The node won't reject it - there's no pre-existing admin to say no.
   Bootstrap your first admin with a signed request:

   ```bash
   # Generate an identity (or bring your own Ed25519 keypair)
   sage-cli identity new > bootstrap_admin.json

   # Self-elevate to admin on a fresh node
   sage-cli agent set-permission \
     --target-id <bootstrap_admin_pubkey> \
     --clearance 4 \
     --visible-agents '*' \
     --key bootstrap_admin.json
   ```

3. **Genesis file** - pre-seed admin state in the CometBFT genesis file for
   production deployments. This is the most secure path but requires planning
   at chain-init time. See `cmd/sage/genesis.go`.

After any of these, the admin can register orgs, add members, and grant per-domain
access via the REST API.

## Pattern 1: single-org (recommended for levelup-style deployments)

One organization where every agent is trusted to see every other agent's
memories. This is the typical pattern for a single-tenant platform (a CTF trainer,
an internal R&D assistant, a personal memory node).

### Setup

1. **Bootstrap an admin** (see above).
2. **Register the org** - admin signs:

   ```
   POST /v1/org/register
   { "name": "acme", "description": "Acme Inc" }
   ```

   Returns `{ "org_id": "<hex>", "tx_hash": "..." }`. Save `org_id` - you'll
   use it for every member add.

3. **Add each agent as a TopSecret member**:

   ```
   POST /v1/org/{org_id}/member
   { "agent_id": "<pubkey_hex>", "clearance": 4, "role": "member" }
   ```

   Clearance `4` (`ClearanceTopSecret`) is the key - as of v6.6.2, TopSecret
   members bypass the `submitting_agents` RBAC filter automatically. They still
   respect per-domain access control and per-record classification gates, but
   within those envelopes they see across agents.

4. **Domains auto-register on first submit**. The first agent to write to a
   new domain becomes its owner - you don't need to `RegisterDomain` explicitly.
   The owner's org inherits domain-level access, and other TopSecret members of
   the same org see into it via `HasAccessMultiOrg`.

### What you skip

In this pattern you do **not** need:

- `visible_agents="*"` on every agent - TopSecret handles it.
- Per-agent domain grants - same-org TopSecret members see each other via the
  HasAccessMultiOrg path.
- Federation - there's only one org.

## Pattern 2: multi-org federated

Two or more orgs where each agent is tied to exactly one org, and cross-org
sharing is explicitly negotiated per-federation agreement.

### Setup

1. **Bootstrap an admin per org** - each org's admin is separate.
2. **Register each org** (`POST /v1/org/register`, one per org).
3. **Add members to each org** at the clearance level appropriate for their
   intra-org access (`clearance=1` Internal, `2` Confidential, `3` Secret,
   `4` TopSecret - see `internal/tx/types.go`).
4. **Propose a federation** from org A to org B:

   ```
   POST /v1/federation/propose
   {
     "target_org_id": "<org-B-id>",
     "allowed_domains": ["shared.research"],
     "max_clearance": 2,
     "expires_at": 1767110400,
     "requires_approval": true
   }
   ```

   `max_clearance` caps cross-org reads - org B members can't read anything
   above clearance 2 from org A domains covered by the federation.

5. **Org B admin approves** via `POST /v1/federation/{fed_id}/approve`.

### Access model

- Same-org agents with sufficient clearance see each other via `HasAccessMultiOrg`.
- Cross-org access requires an active federation agreement AND the reading
  agent's clearance must be ≥ memory classification AND ≤ federation
  max_clearance.
- Federation does **not** grant visibility into agents unrelated to the
  federation's allowed_domains list.

## Pattern 3: homogeneous-trust (legacy)

Everyone sees everyone, no orgs, no classification. Configured by setting
`visible_agents="*"` on every registered agent.

```
PUT /v1/agent/{agent_id}/permission
{ "visible_agents": "*" }
```

This was the recommended pattern before v6.6.2. It still works, but pattern 1
achieves the same thing with one org membership instead of N per-agent configs.

If you already have a homogeneous-trust deployment and want to migrate, you
can leave the wildcard in place - `visible_agents="*"` short-circuits
`resolveVisibleAgents` before the TopSecret check, so the two mechanisms
compose cleanly.

## Chain-bootstrap window on fresh data dir

On a fresh data directory (first deploy, or after a chain reset), CometBFT
performs an init pass before the first block is committed. During this window
`/health` returns 503 because `cometbftOK` flips to true only once the node is
running. On modest hardware this can take **several minutes** — observed ~3 min
on a single-node post-migration redeploy.

What this looks like in practice:

- Container shows `unhealthy` in `docker ps` for the first ~3 min.
- REST server *is* bound on `:8080` — `curl /health` returns 503 with
  `{"status":"unhealthy"}` (not connection refused).
- After CometBFT finishes bootstrap, `/health` flips to 200 and the container
  goes `healthy`.

**Don't roll back during this window.** The fix is on your orchestrator side:

- **Docker compose / Docker**: the official `deploy/Dockerfile.abci` ships a
  HEALTHCHECK with `start_period=5m` to cover the bootstrap window. If you
  override this in your own compose, keep `start_period` ≥ 5 min.
- **Kubernetes**: set `startupProbe` with `failureThreshold` × `periodSeconds`
  ≥ 300s (e.g. `failureThreshold: 30, periodSeconds: 10`), and let `liveness` /
  `readiness` only run after that.
- **Confirming it's actually bootstrap, not a hang**: `curl localhost:8080/health`
  from inside the container — 503 with body means REST bound, just waiting for
  CometBFT. Connection refused means something else is wrong.

## Chain-reset visibility gotcha

If you reset the chain state (e.g., `rm -rf ~/.sage/badger/` or equivalent
for a clean re-sync), **on-chain domain ownership is wiped**. This has two
visible effects post-reset:

1. **Domains with no post-reset writes** fall through to the "no owner = open"
   path in `handleListMemoriesAuth` / `handleQueryMemory`. They appear visible
   to any authenticated agent - which looks like a regression but is the
   intentional backward-compat path for pre-RBAC setups.

2. **Domains that get a fresh post-reset write** re-register the writer as the
   new owner. If the writer is a different agent than the pre-reset owner,
   other agents who could see the domain before the reset now can't - they've
   been effectively locked out by auto-registration.

The cleanest recovery paths:

- **Single-org (pattern 1)**: add TopSecret clearance to the affected members
  after reset. They see across auto-registered owners automatically.
- **Multi-org**: re-run `RegisterDomain` (or re-broadcast the first-writer tx)
  from the agent you want as owner before anyone else writes. Admin can also
  call `TransferDomain` to hand ownership over.
- **Homogeneous-trust**: unaffected - `visible_agents="*"` skips owner checks
  on the caller side.

A proper fix (opt-in auto-register, or a chain-reset marker that suspends
auto-register until the operator says otherwise) is tracked as a separate
design discussion; see the GitHub issue for context.

## Quick reference

| Task                              | Endpoint                                         |
| --------------------------------- | ------------------------------------------------ |
| Bootstrap an admin                | `PUT /v1/agent/{id}/permission` (self)           |
| Register an org                   | `POST /v1/org/register`                          |
| Add a member (with clearance)     | `POST /v1/org/{org_id}/member`                   |
| Change clearance                  | `POST /v1/org/{org_id}/clearance`                |
| List org members                  | `GET /v1/org/{org_id}/members`                   |
| Set visible_agents wildcard       | `PUT /v1/agent/{id}/permission` + `{"visible_agents":"*"}` |
| Grant domain read                 | `PUT /v1/agent/{id}/permission` + `{"domain_access":"[{...}]"}` |
| Propose federation                | `POST /v1/federation/propose`                    |
| Transfer domain ownership (admin) | `POST /v1/domain/{domain}/transfer`              |

All endpoints require a signed request (`X-Agent-ID`, `X-Signature`,
`X-Timestamp` headers). See `docs/GETTING_STARTED.md` for signing details.

## See also

- `docs/GETTING_STARTED.md` - client setup, request signing, SDK usage
- `docs/ARCHITECTURE.md` - BFT consensus, ABCI lifecycle, memory flow
- `internal/tx/types.go` - clearance levels, tx schema reference
- `api/rest/memory_handler.go` - `resolveVisibleAgents`, `checkDomainAccess`,
  `agentHasTopSecretClearance` (the TopSecret-as-seeAll entry point)
