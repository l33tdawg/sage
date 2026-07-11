<!-- Reconciled through SAGE v11.6.1. -->

# RBAC, Organizations, and Federation

Verified against code at SAGE v11.6.1.

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

---

## ARCHITECTURE.md Discrepancy

`docs/ARCHITECTURE.md:498-505` presents clearance levels as operational tiers (0=None/1=Read/2=Read+Write/3=Validate/4=Admin). The authoritative code at `internal/tx/types.go:84-90` and `internal/store/store.go:224-230` uses these as data classification labels (PUBLIC/INTERNAL/CONFIDENTIAL/SECRET/TOP_SECRET). Both interpretations are in use simultaneously — the level integer gates both "what data can this agent see" (classification) and "what operations can this agent perform" (role). The ARCHITECTURE.md table conflates the two; this document is the accurate reference.
