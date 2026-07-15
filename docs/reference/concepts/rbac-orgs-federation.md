<!-- Reconciled through SAGE v11.7.0. -->

# RBAC, Organizations, and Federation

Verified against code at SAGE v11.7.0.

## Overview

SAGE's access control is a layered system. From outermost to innermost, a query passes through:

1. **Ed25519 authentication and action binding** — every request must carry a valid agent signature; after app-v17 activation, consensus binds a delegated proof to the exact signed REST action, deterministic freshness window, and a single-use AppHash marker
2. **Agent-isolation RBAC** — `visible_agents` field restricts which agents' memories a given agent can see
3. **Domain-level access check** — `checkDomainAccess`: per-agent, per-domain allowlist (DomainAccess JSON)
4. **Multi-org domain gate** — `HasAccessMultiOrg`: org membership, same-org clearance, or federation agreement
5. **Per-record classification gate** — `if memClass > 0`: each result is individually checked against the querier's org-level clearance

All RBAC state is on-chain. Organizations, departments, clearance levels, access grants, and federation agreements are committed through BFT consensus and stored in BadgerDB, with PostgreSQL as the off-chain mirror. BadgerDB is the authoritative source for access control decisions; SQLite is used only as a fallback for data not yet broadcast on-chain.

---

## Organizations

### Registration

`POST /v1/org/register` → `handleOrgRegister` (`api/rest/org_handler.go:56+`) → `TxTypeOrgRegister` → `processOrgRegister` (`internal/abci/app.go`).

The REST handler precomputes `OrgID` as `hex(SHA256(admin_agent_pubkey + name)[:16])` before broadcasting. If a transaction arrives without an ID, ABCI derives a deterministic fallback from `adminID:name:height`. The registering agent becomes the `AdminAgent`. One admin per org at registration; additional admins can be added via `OrgAddMember` with role `"admin"`.

BadgerDB key: `org:<orgID>` with JSON-encoded org record.
Reverse index: `org_name:<name>:<orgID>` marker entries enable name lookup without assuming org names are unique.

### Membership

`POST /v1/org/{org_id}/member` → `TxTypeOrgAddMember` → `processOrgAddMember`.

Fields: `OrgID`, `AgentID`, `Clearance` (0-4), `Role` (`"admin"`, `"member"`, `"observer"`).

BadgerDB keys:
- `org_member:<orgID>:<agentID>` — the membership record
- `agent_org:<agentID>` → org ID (legacy single-slot reverse index)
- `agent_orgs:<agentID>:<orgID>` — marker entry for each membership

**Multi-org membership note:** An agent can belong to multiple organizations. `HasAccessMultiOrg` iterates `ListAgentOrgs(agentID)`, which prefix-scans `agent_orgs:<agentID>:` marker entries. The legacy `agent_org:` single-slot is retained for compatibility.

### Clearance Updates

`POST /v1/org/{org_id}/clearance` → `TxTypeOrgSetClearance` → updates the member's `Clearance` field both in BadgerDB and PostgreSQL.

### Removal

`DELETE /v1/org/{org_id}/member/{agent_id}` → `TxTypeOrgRemoveMember`. Removes the membership record and reverse index entry. Memories are not affected.

---

## Departments

Departments are sub-groups within an organization. They add a second scope axis for federation agreements.

### Registration

`POST /v1/org/{org_id}/dept` → `TxTypeDeptRegister`. `DeptID` is deterministic: `SHA256(orgID + name)[:16]` hex.

BadgerDB key: `dept:<orgID>:<deptID>`.

### Membership

`POST /v1/org/{org_id}/dept/{dept_id}/member` → `TxTypeDeptAddMember`. Fields: `OrgID`, `DeptID`, `AgentID`, `Clearance`, `Role`.

BadgerDB key: `dept_member:<orgID>:<deptID>:<agentID>`.
Reverse index: `agent_dept:<agentID>` → `{orgID, deptID}`.

Departments have their own `Clearance` field independent of the org-level membership clearance. When a federation agreement specifies `AllowedDepts`, only agents in those departments (within the allowed org) can access the federated domains.

---

## Agent Clearance Semantics

`ClearanceLevel` (0-4) in `internal/tx/types.go:84-90` and `internal/store/store.go:224-230`:

| Level | Name         | Operational meaning in RBAC                             |
|-------|--------------|----------------------------------------------------------|
| 0     | PUBLIC       | Observer; no org-based read uplift for classified data  |
| 1     | INTERNAL     | Can read INTERNAL (level 1) data within the org         |
| 2     | CONFIDENTIAL | Can read CONFIDENTIAL (level 2) data within the org     |
| 3     | SECRET       | Can read SECRET (level 3) data; dept-scoped grants      |
| 4     | TOP SECRET   | Full clearance within org; bypasses `visible_agents` filter (see below) |

**Note on ARCHITECTURE.md**: The doc (`docs/ARCHITECTURE.md:499`) describes the levels with "0=None, 1=Read, 2=Read+Write, 3=Read+Write+Validate, 4=Admin" — this describes *operational role tiers*, not the data-classification model. The code (`tx/types.go:84-90`) uses them as data classification labels. These two meanings coexist: clearance level ≥ memory classification is the gate in `HasAccessMultiOrg`. A level-4 (TOP SECRET-cleared) agent can read all classification levels.

---

## Domain Ownership and Access Grants

### First-Write-Wins Auto-Registration

When an agent submits a memory to a domain that has no registered owner and is not a shared domain, `processMemorySubmit` calls `badgerStore.RegisterDomain(domain, agentID, "", height)` (check-and-set). The submitting agent becomes owner and receives a level-2 access grant automatically.

**Shared domains** (never auto-registered, writable by any authenticated agent):
- Exact names: `general`, `self`, `meta` (`app.go:766-770`)
- Prefix match: `sage-*` (`app.go:780-782`)
- Any domain with on-chain `shared_domain:<name>` sentinel (set by `TxTypeDomainReassign` with `OpenToShared=true`)

### Explicit Grants

`POST /v1/access/grant` → `TxTypeAccessGrant` → `processAccessGrant` → `badgerStore.SetAccessGrant(domain, granteeID, level, expiresAt, granterID)`.

BadgerDB key: `grant:<domain>:<agentID>`, value: `level(1 byte) + expiresAt(8 bytes big-endian)`.

`Level` values: `1` = read, `2` = read+write, `3` = modify on app-v15+ chains (`internal/abci/app.go:3949-3955`).

`ExpiresAt`: Unix timestamp; `0` = permanent.

The REST handler uses `broadcast_tx_commit`, so a `FinalizeBlock` rejection is surfaced before the handler returns (`api/rest/access_handler.go:163-169`). A grant on a genuinely unowned, non-shared domain auto-registers the granter as owner before writing the grant; owned domains require owner or ancestor-owner authority (`internal/abci/app.go:3844-3964`).

### Access Requests

`POST /v1/access/request` → `TxTypeAccessRequest`. Creates a pending request in BadgerDB at `state:access_req:<requestID>` and mirrors to PostgreSQL. A domain owner (or admin) can then issue a grant referencing the `request_id`.

### Revocation

`POST /v1/access/revoke` → `TxTypeAccessRevoke` → `badgerStore.RevokeGrant`. Sets `RevokedAt` in PostgreSQL.

### CEREBRUM Dashboard: Real Grants + Agent-to-Agent Ownership Transfer (v11.3)

Two dashboard surfaces now write to the on-chain RBAC state above instead of merely displaying it. They reuse the existing transaction types; app-v18 fork-extends their optional wire payload and consensus authorization for administrator overrides.

- **The access matrix issues real grants.** Saving an agent's per-domain read/write matrix (`PATCH /v1/dashboard/network/agents/{id}`) reconciles the desired levels against the ACTUAL on-chain grant state (`GetAccessGrant`) and issues real `TxTypeAccessGrant` / `TxTypeAccessRevoke` txs for each divergence. The normal path signs as the effective leaf/ancestor domain owner. A genuinely unowned domain is atomically claimed by the genesis admin on its first grant, matching the existing consensus rule instead of being rejected in the dashboard. The reconcile is idempotent and self-healing. Earlier versions wrote only the advisory `DomainAccess` blob and not the enforced grant keys (`grant:<domain>:<agentID>`) that `HasAccessMultiOrg`'s direct-grant path actually checks, so cross-agent grants set from the matrix did not take effect (`web/reassign_handler.go`, `web/network_handler.go`).

- **app-v18 explicit genesis-admin override (v11.7 candidate).** CEREBRUM may offer **Admin override & assign** only when the target agent's private key is held on this node (local, not merely visible through federation). The confirmation identifies the effective original owner and desired read/write level. The transaction carries that expected owner and owning ancestor as a consensus-checked binding, so a concurrent ownership change rejects rather than applying a stale confirmation. Once app-v18 is activated, a registered global admin may sign `AccessGrant` / `AccessRevoke` even when it is not the domain owner; the grant remains an ordinary auditable `grant:<domain>:<agentID>` record and does **not** change domain ownership or memory authorship. Ordinary agents remain owner/ancestor-owner gated. Level 1 is read-only; memory submit and co-commit require the effective owner or an explicit level-2 direct/ancestor grant—org membership and federation clearance do not imply write authority. Pre-app-v18 blocks and the activation block retain the old rule byte-for-byte (`internal/abci/app.go`, `web/reassign_handler.go`).

- **Domain ownership transfers agent-to-agent via governance.** `POST /v1/dashboard/network/reassign-domain-ownership` orchestrates, commit-confirmed: `gov_propose(domain_reassign)` -> the sole validator's accept vote drives it to `Executed` -> `TxTypeDomainReassign` flips the owner and purges the domain's grants -> `TxTypeAccessGrant` gives the new owner level 3 (deferred to the owner's own node if their key is not local). This transfers OWNERSHIP and read/write ACCESS only; it does NOT rewrite memory authorship - every memory stays authored by its original `submitting_agent`. Single-validator node only; a multi-validator chain is rejected because the other validators must vote on the proposal (`web/reassign_handler.go:285-318`).

---

## Query Scoping — Full Access-Check Pipeline

A `POST /v1/memory/query` request passes through these gates in order (`memory_handler.go:517+`):

### Gate 1: checkDomainAccess (DomainAccess policy)

`checkDomainAccess` (`memory_handler.go:159-251`) reads the agent's `DomainAccess` JSON field (on-chain BadgerDB first, SQLite fallback):

- `role == "admin"` → bypass all checks, full access
- `role == "observer"` → write operations blocked
- `DomainAccess == ""` or empty list → no per-domain restrictions, allow all
- Otherwise: explicit allowlist model — domain must appear with `read: true` (for queries) or `write: true` (for submissions)

If `checkDomainAccess` approves the domain, `domainAccessApproved = true` and the multi-org gate (Gate 2) is skipped for the domain-level check. The per-record classification gate (Gate 5) still runs.

### Gate 2: Multi-org domain gate (domain level)

Applied when `domainAccessApproved == false` and the domain has a registered owner. Calls `HasAccessMultiOrg(domain, agentID, 0, time.Now(), postFork)` at the domain level (classification=0 means "any read access").

### Gate 3: Agent isolation — resolveVisibleAgents

`resolveVisibleAgents(agentID)` (`memory_handler.go:258-320`) returns `(allowedAgentIDs, seeAll)`:

- `agentID == nodeOperatorID` → `seeAll = true` (node operator bypass)
- `role == "admin"` → `seeAll = true`
- `visible_agents == "*"` → `seeAll = true`
- **Any org member with clearance=4 (TOP SECRET)** → `seeAll = true` (`agentHasTopSecretClearance` check, `memory_handler.go:310`)
- Otherwise: agent sees memories from `[agentID] + parsed(visible_agents)` list

If `seeAll == false`, `opts.SubmittingAgents` is set to the allowed list, which `QuerySimilar` uses to filter at the PostgreSQL level.

### Gate 4: Grant-aware seeAll override

Even if `seeAll == false`, for a specific `DomainTag` query:
- Direct grant on domain (`HasAccess`) → `seeAll = true`
- Org-level access (`HasAccessMultiOrg`) → `seeAll = true`
- Unregistered domain → `seeAll = true`

### Gate 5: Per-record classification gate

See `clearance-classification.md` for the full specification. Applied after the SQL query returns results.

---

## HasAccessMultiOrg Algorithm

Source: `internal/store/badger.go:3113-3188`.

```
HasAccessMultiOrg(domain, agentID, memoryClassification, blockTime, postFork):

1. Direct grant check:
   - post-fork: HasAccessOrAncestor(domain, agentID, level=1, blockTime)
   - pre-fork:  HasAccess(domain, agentID, level=1, blockTime)
   → if found: return true

2. ListAgentOrgs(agentID) → agentOrgs
   → if empty: return false (no org = only direct grants)

3. Resolve domain owner:
   - post-fork: ResolveOwningAncestor(domain) → walk dotted path to nearest owned ancestor
   - pre-fork:  GetDomainOwner(domain) → exact match only
   → if no owner found: return false

4. ListAgentOrgs(domainOwner) → domainOrgs

5. Same-org check: for each agentOrg in agentOrgs ∩ domainOrgs:
   GetMemberClearance(agentOrg, agentID) → clearance
   if clearance >= memoryClassification: return true

6. Federation check: for each (agentOrg, domainOrg) cross-product where agentOrg != domainOrg:
   FindFederation(agentOrg, domainOrg) → fedID
   GetFederation(fedID) → status, maxClearance, expiresAt, allowedDepts
   if status == "active" AND !expired AND memoryClassification <= maxClearance:
     (check dept scope if AllowedDepts != ["*"] and not empty)
     return true

return false
```

**Current semantics:** On live v11 chains, access checks use ancestor-walk behavior for grants and domain ownership. Exact-match behavior remains only for replaying pre-fork history.

---

## Federation

A federation is a bilateral agreement between two organizations.

### Proposal and Approval

`POST /v1/federation/propose` → `TxTypeFederationPropose` → persists `FederationEntry{status:"proposed"}` in BadgerDB and PostgreSQL.

`POST /v1/federation/{fed_id}/approve` → `TxTypeFederationApprove` → sets status to `"active"`.

The `FederationID` is deterministic: computed from the two org IDs + height to avoid collisions.

### Federation Record Fields (`internal/store/store.go:253-268`)

| Field            | Type     | Description                                                |
|------------------|----------|------------------------------------------------------------|
| `ProposerOrgID`  | string   | Org that proposed the agreement                            |
| `TargetOrgID`    | string   | Invited org                                                |
| `AllowedDomains` | []string | Which domains are shared; `["*"]` = all                    |
| `AllowedDepts`   | []string | Dept scope; `["*"]` or empty = all depts                   |
| `MaxClearance`   | 0-4      | Ceiling clearance for cross-org reads                      |
| `ExpiresAt`      | *time    | Nil = permanent                                            |
| `RequiresApproval` | bool   | Stored but not currently enforced at query time            |
| `Status`         | string   | `"proposed"`, `"active"`, `"revoked"`                      |

### MaxClearance Cap

`checkFederationAccess` (`badger.go:2156-2175`) enforces: `if memoryClassification > maxClearance → deny`. This means a federation with `max_clearance=1` (INTERNAL) cannot expose CONFIDENTIAL (2) or higher memories to the federated org, regardless of the individual agent's clearance within their own org.

### Revocation

`POST /v1/federation/{fed_id}/revoke` → `TxTypeFederationRevoke` → sets status to `"revoked"`. All subsequent `HasAccessMultiOrg` calls for this pair return false.

---

## v11.9 quorum scopes are not cross-chain federation

The v11.6–v11.8 federation path connects independent SAGE chains and copies
selected domains through an authenticated off-consensus transport. An app-v20
quorum scope instead lives inside one CometBFT chain and names validators that
already belong to that chain. It does not merge independent chains or replace
libp2p relay/NAT traversal (`internal/abci/app.go:627-631`,
`internal/governance/types.go:53-58`).

`OpScopeAction` governance creates, advances, pauses, or permanently retires the
canonical scope record. Its exact-domain allowlist and integer member weights
are stored in Badger with immutable per-revision audit anchors
(`internal/store/scope_state.go:46-185`). A scoped memory submission atomically
pins the current roster/denominator and a recoverable content envelope; quorum
requires a strict greater-than-two-thirds integer comparison, so exactly 2/3 is
not sufficient (`internal/store/scoped_memory_state.go:25-93`,
`internal/scope/ballot.go:154-162`).

Badger remains the recovery authority. After ordered block replay or verified
local snapshot restore, the node verifies each canonical envelope and rebuilds
its SQL serving projection in a transaction. `/ready` returns 503 while
canonical scoped records exist but that projection is locked, incomplete, or
invalid; a locked SQLite vault retries on operator unlock
(`internal/abci/scoped_recovery.go:21-86`, `internal/metrics/health.go`). The
catch-up integration proof compares every replayed AppHash and confirms that an
unselected domain has no canonical scoped envelope
(`internal/abci/appv20_recovery_integration_test.go:41-150`).

The existing snapshot package is an operator-local rollback format. It can
contain SQLite, CometBFT databases, node/validator keys, vault material, local
configuration, and a binary, so it must never be sent over ABCI/P2P
(`internal/snapshot/snapshot.go:1-9`). Its v11.9 integration test restores the
bundle, deletes SQLite, recomputes AppHash, reloads app-v20, and rebuilds scoped
content plus classification from Badger
(`internal/abci/appv20_recovery_integration_test.go:152-285`). Network state sync
is not yet implemented: the ABCI app advertises no snapshots, rejects offers,
returns no chunks, and aborts apply (`internal/abci/app.go:6547-6566`). A future
enabled network path must therefore use the separate dormant consensus-only,
key-free format described below.

Crash replay is height-bound rather than a nonce bypass. Scoped votes store the
immutable first decision and its FinalizeBlock height atomically; only an exact
submit envelope or exact vote at that same behind height can rebuild projection
writes after a failed Commit (`internal/store/scoped_memory_state.go:214-324`,
`internal/abci/scoped_memory.go:97-207`). The forced-failure integration test
reopens the stores after failed submit and quorum-reaching vote commits, matches
the original AppHashes, restores all SQL votes, and confirms that the same
signed vote at a later height is still rejected
(`internal/abci/appv20_recovery_integration_test.go:287-426`).

The dormant network-state-sync substrate is isolated in `internal/statesync`.
Its canonical metadata binds CometBFT's trusted height/AppHash to a bounded
chunked Badger backup, hashes every chunk and the complete stream, caps chunks
at 8 MiB and count at 512, and rejects the unrelated local rollback manifest
(`internal/statesync/format.go:1-230`). Its disk assembler accepts out-of-order
delivery, makes an exact duplicate idempotent, rejects corrupt/incorrect-size
chunks, and verifies the whole backup before an atomic file rename
(`internal/statesync/assembler.go:15-198`). Tests include malformed canonical
encodings, wrong trusted hashes, interrupted/incomplete assembly, and a real
Badger backup/load with an identical AppHash (`internal/statesync/format_test.go`).
The ABCI endpoints stay disabled even though these format proofs pass.

The producer and receiver foundations are also dormant. Export makes a bounded
live Badger backup, requires an app-v20 reload verifier to match the committed
AppHash, atomically publishes only metadata/chunks, rejects extra files and
symlinks, and hashes a chunk again immediately before read
(`internal/statesync/exporter.go:23-362`). Receiver preparation loads only into
a new staging directory, reopens it read-only so validation cannot backfill or
mutate consensus state, requires exact persisted height plus active app-v20,
recomputes the narrow AppHash, validates all canonical scoped state, and removes
the stage on failure (`internal/store/badger.go`,
`internal/abci/scoped_state_sync.go:19-139`,
`internal/abci/scoped_recovery.go:73-114`). No code swaps that directory into a
live application yet, so `ListSnapshots` still advertises none.

The accepted activation design deliberately avoids changing `BadgerStore.db`
inside a serving process. It replaces the whole ABCI application bundle during
boot, journals the filesystem/Comet persistence crash window, delays REST,
dashboard, projection, snapshot-scheduler, and worker construction until the
runtime is sealed, and requires an authenticated validator-only P2P allowlist.
This is design work, not enabled behavior; the detailed gates are in
[`../../v11.9-state-sync-activation.md`](../../v11.9-state-sync-activation.md).
The first non-activating pieces are code: a bounded canonical/fsynced journal
and pure Comet-vs-live recovery decision (`internal/statesync/activation.go`),
plus a writable no-migration opener that preserves the already verified
AppHash (`internal/store/badger.go:105-128`). No function performs the live
directory rename yet.

Scope proposals no longer require operators or agents to hand-encode that
binary record. REST/dashboard accept a structured `scope` template, MCP exposes
the same guided object, and both Python clients provide
`governance_propose_scope`. The server sorts domains and members canonically,
owns the zero proposal heights, preserves explicit historical join revisions,
and rejects `payload` plus `scope` ambiguity. CEREBRUM builds controller and
roster choices from the live CometBFT validator set's canonical Ed25519 IDs,
not from ordinary dashboard-agent IDs; the consensus execution path rechecks
the same authority. Scope IDs are one REST path segment and therefore cannot
contain `/`
(`internal/scope/proposal.go:9-94`, `api/rest/governance_handler.go:105-204`).

Operator/admin visibility is available through `GET /v1/scopes`,
`GET /v1/scopes/{scope_id}`, `sage_scope_list`, and `sage_scope_get`. These
surfaces expose topology and audit hashes, never grant domain ownership, RBAC,
federation access, or administrator authority.

---

## REST Endpoints Reference

| Method | Path | Tx Type | Description |
|--------|------|---------|-------------|
| POST | `/v1/org/register` | `TxTypeOrgRegister` | Register new org; requester becomes admin |
| GET | `/v1/org/{org_id}` | — | Get org details (BadgerDB) |
| GET | `/v1/org/by-name/{name}` | — | Lookup org by name (name→orgIDs reverse index) |
| GET | `/v1/org/{org_id}/members` | — | List org members |
| POST | `/v1/org/{org_id}/member` | `TxTypeOrgAddMember` | Add agent to org with clearance |
| DELETE | `/v1/org/{org_id}/member/{agent_id}` | `TxTypeOrgRemoveMember` | Remove member |
| POST | `/v1/org/{org_id}/clearance` | `TxTypeOrgSetClearance` | Change member clearance |
| POST | `/v1/org/{org_id}/dept` | `TxTypeDeptRegister` | Create department |
| POST | `/v1/org/{org_id}/dept/{dept_id}/member` | `TxTypeDeptAddMember` | Add agent to dept |
| DELETE | `/v1/org/{org_id}/dept/{dept_id}/member/{agent_id}` | `TxTypeDeptRemoveMember` | Remove from dept |
| POST | `/v1/federation/propose` | `TxTypeFederationPropose` | Propose cross-org federation |
| POST | `/v1/federation/{fed_id}/approve` | `TxTypeFederationApprove` | Approve pending federation |
| POST | `/v1/federation/{fed_id}/revoke` | `TxTypeFederationRevoke` | Revoke active federation |
| POST | `/v1/access/request` | `TxTypeAccessRequest` | Request domain access |
| POST | `/v1/access/grant` | `TxTypeAccessGrant` | Grant domain access to agent |
| POST | `/v1/access/revoke` | `TxTypeAccessRevoke` | Revoke domain access grant |
| GET | `/v1/access/grants/{agent_id}` | — | List active grants for agent |
| POST | `/v1/domain/register` | `TxTypeDomainRegister` | Explicitly register domain ownership |
| GET | `/v1/domain/{name}` | — | Get domain owner and metadata |
| GET | `/v1/scopes` | — | List canonical app-v20 quorum scopes (operator/admin) |
| GET | `/v1/scopes/{scope_id}` | — | Read one canonical app-v20 quorum scope (operator/admin) |

---

## ARCHITECTURE.md Discrepancy

`docs/ARCHITECTURE.md:498-505` presents clearance levels as operational tiers (0=None/1=Read/2=Read+Write/3=Validate/4=Admin). The authoritative code at `internal/tx/types.go:84-90` and `internal/store/store.go:224-230` uses these as data classification labels (PUBLIC/INTERNAL/CONFIDENTIAL/SECRET/TOP_SECRET). Both interpretations are in use simultaneously — the level integer gates both "what data can this agent see" (classification) and "what operations can this agent perform" (role). The ARCHITECTURE.md table conflates the two; this document is the accurate reference.
