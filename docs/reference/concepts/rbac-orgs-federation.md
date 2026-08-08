<!-- Core document reconciled through SAGE v11.18.2/app-v26, including consensus-backed Access Group authority, Root continuity, linked federated readers, and the quorum/state-sync/governance-gateway sections. -->

# RBAC, Organizations, and Federation

Verified against SAGE v11.18.2. Legacy organization/federation sections retain
their historical context; app-v23 roles, Root, Access Groups, and app-v25
historical writer continuity are the current local-control model.

## Overview

SAGE's access control is a layered system. From outermost to innermost, a query passes through:

1. **Ed25519 authentication and action binding** — every request must carry a valid agent signature; after app-v17 activation, consensus binds a delegated proof to the exact signed REST action, deterministic freshness window, and a single-use AppHash marker
2. **Agent-isolation RBAC** — `visible_agents` field restricts which agents' memories a given agent can see
3. **Domain-level access check** — `checkDomainAccess`: per-agent, per-domain allowlist (DomainAccess JSON)
4. **Multi-org domain gate** — `HasAccessMultiOrg`: org membership, same-org clearance, or federation agreement
5. **Per-record classification gate** — `if memClass > 0`: each result is individually checked against the querier's org-level clearance

Consensus RBAC state includes organizations, departments, clearance levels,
access grants, app-v23 roles, local enrollment, and local Access Groups.
Federation exports, reader restrictions, and legacy linked-reader relations are
deliberately node-local and bound to the exact active agreement generation.
BadgerDB is the authoritative source for consensus access-control decisions;
SQLite/PostgreSQL are rebuildable serving projections and also hold the
explicitly node-local federation relation state.

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

### App-v25 recovered historical-domain continuity

Historical rows from before the app-v23 local-enrollment model are not blindly
treated as new claims. App-v25 derives continuity only from complete, exact
canonical evidence. The earliest verified historical local writer remains the
operational owner; other verified local writers become members of the exact
recovered Access Group and retain read/write authority on that domain. This is
not a federation mechanism and it does not alter memory authorship.

If the earliest writer is unavailable as a safe local principal, CEREBRUM Root
owns the recovered domain. SAGE never promotes a later writer by inference.
The recovered writer set is fixed at the app-v25 cutoff, so a new memory cannot
expand it. Linked federated readers remain read-only and never become group
members. See [`../app-v25-upgrade-recovery.md`](../app-v25-upgrade-recovery.md).

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

### App-v22 Agent Capabilities (historical predecessor and migration input)

App-v22 added a consensus-stored `uint32` capability/restriction mask to each
registered agent. Zero is the byte-compatible legacy behavior. Only a global
`role=admin` may change any persisted permission field (`clearance`,
`domain_access`, `visible_agents`, `org_id`, `dept_id`, or `capabilities`).
Existing self/org-admin authorization may submit an exactly equal no-op, but
cannot use another field to bypass capability review. A restricted process
therefore cannot clear its own restrictions through REST, CEREBRUM, or a
directly submitted transaction (`internal/store/agent_capabilities.go`;
`internal/tx/codec.go`; `internal/abci/app.go`).

| Bit | Meaning |
|---:|---|
| `1` | Read across domain and submitting-agent filters, but never above the agent's stored numeric clearance. |
| `2` | Deny writes to shared domains such as `general`, `self`, `meta`, `sage-*`, and dynamic shared domains. |
| `4` | Deny explicit domain registration and every implicit first-writer claim path. |
| `8` | Deny writes to domains owned by another agent, even if a level-2 grant exists. |
| `16` | Deny federated pipeline discovery/delivery; local inbox messaging remains unchanged. |

The co-located voice/companion preset is mask `15` (`1|2|4|8`), not `31`.
It can recall globally only within its clearance and can ask visible agents on
compatible federated SAGEs to act through their inboxes. Any requested memory
mutation is then a normal local action by the receiving agent under that
agent's own grants. This does not enable federation Write, which remains a
separate reserved, fail-closed transport surface.

The companion's own writable memory domain must be an assigned, non-shared
domain (for example `voice-interface`). Names under the static `sage-*` family,
including `sage-voice-bridge`, are ownerless shared domains; mask `15`
deliberately blocks writes there even if an old SQLite access projection still
contains a write toggle. The consensus capability—not that projection—is the
authoritative write boundary.

At app-v22 activation, existing registered agents keep mask `0` for replay and
upgrade compatibility. A key that self-registers after activation starts with
mask `30` (`2|4|8|16`): no shared/foreign writes, no domain claims, and no
federated recipient routing. This prevents a restricted process from minting a
fresh key to bypass its operator-assigned mask. A global administrator must
then assign the intended profile; the companion preset `15` is the explicit
step that adds clearance-bounded read-all and enables federated inbox routing.
Badger remains authoritative for the mask and every other policy field. The
ABCI projection persists the same mask to SQLite/Postgres for ordinary clients,
while the REST and dashboard detail/list reads overlay current Badger policy
onto legacy or crash-window SQL rows before serialization. A stale SQL zero
therefore cannot make a restricted agent appear unrestricted
(`internal/abci/app.go`; `internal/store/sqlite.go`;
`internal/store/postgres.go`; `api/rest/agent_handler.go`).

The same write and claim restrictions run in both ordinary memory submission
and co-commit consensus paths. App-v22 also requires global-admin authority for
every organization, department, and organization-federation mutation:
registration, add/remove member, clearance change, department
registration/add/remove, and federation propose/approve/revoke. Federation
mutations additionally require the global admin to be a member of the relevant
proposer, target, or revoking-side organization so bilateral attribution is
explicit. A restricted companion therefore cannot manufacture TOP SECRET
clearance or a sharing agreement through the org API
(`internal/abci/app.go`; `api/rest/memory_handler.go`;
`api/rest/org_handler.go`; `api/rest/pipe_handler.go`).

### App-v23 Roles, Access Groups, and CEREBRUM Root

App-v23 introduced the local policy model; app-v26 makes each Access Group's
member authority an explicit consensus field rather than deriving it from the
member's global role:

| Element | Meaning |
|---|---|
| `member` | Ordinary local agent. Group-derived verbs come from each group's app-v26 authority tier. |
| `manager` | Local management role. It does not silently upgrade a read-only group's data authority. |
| `admin` | Sudo-equivalent authority over normal local data, policy, governance, federation, and CEREBRUM operations. Root identity and recovery remain non-delegable. |
| Access Group | Scope over active local members and their current owned domain trees, plus one default member tier: Read, Read+Write, or Read+Write+Modify. It never changes domain ownership or memory authorship. |
| Clearance | Maximum record classification the principal may read. |
| Security profile | Hard restrictions that intersect and override role, group scope, and grants. |

Role transitions and onboarding are consensus operations, not independent
edits to legacy permission fields. Approval atomically binds local enrollment,
role, profile, clearance, and an owned non-shared home domain. A generic
self-registration remains a restricted pending Member and cannot manufacture
Manager/Admin authority or claim a domain. The first-party vendored Mynah
bootstrap is separately Root-bound and must commit its reviewed Companion
profile and home domain before `/ready` succeeds
(`internal/abci/appv23_genesis.go`; `internal/abci/appv23_local_rbac.go`;
`internal/store/appv23_local_rbac.go`).

CEREBRUM Root is one immutable authority principal with a rotatable current
credential. It is not an agent role and is excluded from ordinary agent lookup,
groups, messaging, lifecycle, organization/department, task, pipeline,
reassignment, pairing, claim, OAuth, and MCP-token surfaces. Handover preserves
Root-owned domains, groups, grants, and readable history without rewriting
earlier blocks. Historical memories retain their exact credential author;
memories written after handover record the replacement credential. Every
credential generation that has represented Root remains permanently ineligible
for ordinary registration or later reuse as Root
(`internal/store/appv23_local_rbac.go`; `internal/abci/app.go`;
[`app-v23-access-control-design.md`](../app-v23-access-control-design.md)).

App-v26 also closes orphaned current-domain authority during activation and
local-agent retirement. An inactive or missing directory principal, a retired
Root credential, or a noncanonical legacy owner label cannot remain the
effective current owner, so the stable CEREBRUM Root principal assumes current
control. The exact previous owner label is retained in the append-only
`appv26:domain-owner-history:` transition record; memory authors, existing
grant/granter evidence, shared-domain state, and earlier blocks are not
rewritten. Each transition records `domain_created_at` separately from
`transferred_at`, and the current `domain:` row retains its original registered
height. Root's historical-recovery inventory exposes these handovers to the
operator. A later ownership change remains a separate governed whole-domain
reassignment with an all-record blast radius
(`internal/store/appv26_domain_owner_retirement.go`;
`web/appv25_memory_legacy_adoption_control.go`).

An Access Group's local-member compartment is disjoint from federation. Local
rights are derived dynamically from current
ownership rather than expanded into pairwise grants. Group authority is
`read`, `read_write`, or `read_write_modify`; multiple groups form the union of
their scope and strongest applicable authority. Existing groups migrate to
the least-privileged `read` tier at app-v26 activation without changing their
operator revision. Leaving or deleting a group removes only that derived
relationship: every agent retains full authority over its own domain tree.
Ownership transfer changes derived scope immediately in consensus order. A
Every active trusted node connection is already a pairwise federation group.
An operator explicitly exports local ordinary agents into that federation;
each export derives the agent's current owned domain tree. Any active ordinary
agent on the peer receives live Read by default without a mirrored Access
Group, receiving domain, local same-name grant, or exact linked-reader row. A
receiver-local restriction may narrow that default for one local agent.
Disclosure remains bounded by peer policy, ownership, agreement generation,
export and requester classification ceilings, and the original requester's
nested signature. It never supplies Copy, Write, Modify, claim, ownership,
grants, roles, governance, or transitive access
(`internal/federation/agent_exports.go`;
`internal/federation/reader_restrictions.go`;
`internal/federation/v23_guest.go`).

An active exported agent is message-addressable by default unless
`DenyFederatedPipe` applies. This does not grant memory authority. The legacy
linked-reader relation does not itself open messaging: its pipeline transport
uses a separate `linked-v23` authorization mode and receiver-local,
default-off consent for one exact
`remote_chain_id + remote_agent_id -> local_agent_id` tuple. The local target
must be an active ordinary member of a currently linked group; the reverse
direction requires separate consent on the remote receiver's node. Consent is
CAS-revisioned and bound to the live JOIN/operator/CA/policy generation.
Directory/contact discovery grants nothing, and group change, pause, revoke,
re-pair, Root identity, or hard federated-pipe restriction fails closed
(`internal/federation/v23_linked_messaging.go`;
`internal/store/federated_linked_message_consent.go`).

An ordinary agent may resolve a friendly name across that exact messaging
edge without gaining a peer roster. The peer-authenticated lookup intersects
the query with the current signed linked relation, active non-Root/non-read-only
enrollment, group membership, agreement/policy generation, and the receiver's
exact consent. It returns only sanitized display/registered/provider names and
the exact `agent_id@chain_id` address—never group IDs, relation proofs, domains,
presence, result counts, truncation, delivery, or read state. Both
member-to-guest and guest-to-member lookup use the same direct-or-relay request
layer as exact send, and exact resolve/send repeats live authorization after
discovery (`internal/federation/v23_linked_directory.go`).

Capability bit names remain visible only as read-only compatibility diagnostics
behind named profiles. For current policy evaluation, hard restrictions are
checked first, then principal kind and enrollment, role verbs, group/resource
scope, clearance, explicit grants, and the active federation generation. See
[`../app-v23-access-control-design.md`](../app-v23-access-control-design.md).

App-v23 migration preserves exact app-v22 masks instead of forcing every old
agent into a fresh preset. Legacy Member masks `0`/`16` become Standard and
`15`/`31` become Companion; all other known masks use the migration-only
`legacy_restricted` review profile. It cannot be selected for a new or edited
agent. The earliest historical Admin becomes Root; every other historical
Admin becomes an active `legacy_restricted` Member with exact mask and
`legacy_admin_review` audit disposition until a localhost Root-attested
promotion. This avoids treating possession of an old exportable Admin key as
proof that the key is still on the node.

Migration never works around a hard deny. Existing non-shared ownership and
explicit grants remain intact. A domainless agent with `DenyDomainClaim`
remains domainless, while one without the bit receives a deterministic home it
could already have claimed. Bare mask `30` with neither ownership nor an
explicit level-1-or-higher grant is inactive pending review; the same exact mask
with an explicit grant stays active and domainless so its reviewed read remains
usable. ReadAll remains classification-bounded, DenyForeignDomainWrite still
overrides level-2 grants, and DenyFederatedPipe still removes recipient/contact
eligibility.

There is one narrow liveness grandfather: an unchanged active migrated Member
whose canonical disposition is `member`, `legacy_restricted`, or
`legacy_admin_review` may continue ordinary shared-domain memory submission
when bit `2` is absent. It is strictly `Write`, never level-3 `Modify`; it
requires the initial role and enrollment revisions and an exact match to the
immutable migration baseline. Any explicit policy review ends it. Fresh
app-v23 and direct-genesis agents always use normal explicit shared grants
(`internal/store/appv23_local_rbac.go`;
`internal/abci/appv23_local_rbac.go`; `api/rest/memory_handler.go`).

### CEREBRUM Dashboard: Real Grants + Agent-to-Agent Ownership Transfer (v11.3)

Two dashboard surfaces now write to the on-chain RBAC state above instead of merely displaying it. They reuse the existing transaction types; app-v18 fork-extends their optional wire payload and consensus authorization for administrator overrides.

- **The access matrix issues real grants.** Saving an agent's per-domain Read (level 1), Write (level 2), or Modify (level 3) matrix (`PATCH /v1/dashboard/network/agents/{id}`) reconciles the desired levels against the ACTUAL on-chain grant state (`GetAccessGrant`) and issues real `TxTypeAccessGrant` / `TxTypeAccessRevoke` txs for each divergence. Modify includes Read and Write and authorizes the app-v15 challenge/deprecate/reinstate verb ladder. The normal path signs as the effective leaf/ancestor domain owner. A genuinely unowned domain is atomically claimed by the genesis admin on its first grant, matching the existing consensus rule instead of being rejected in the dashboard. The reconcile is idempotent and self-healing. Permission-bearing dashboard edits are operator-only: an ordinary signed agent cannot make the node admin-sign its own clearance/domain/org/visibility elevation. Earlier versions wrote only the advisory `DomainAccess` blob and not the enforced grant keys (`grant:<domain>:<agentID>`) that `HasAccessMultiOrg`'s direct-grant path actually checks, so cross-agent grants set from the matrix did not take effect (`web/reassign_handler.go`, `web/network_handler.go`).

- **app-v18 explicit genesis-admin override (v11.7 candidate).** CEREBRUM may offer **Admin override & assign** only when the target agent's private key is held on this node (local, not merely visible through federation). The confirmation identifies the effective original owner and desired read/write level. The transaction carries that expected owner and owning ancestor as a consensus-checked binding, so a concurrent ownership change rejects rather than applying a stale confirmation. Once app-v18 is activated, a registered global admin may sign `AccessGrant` / `AccessRevoke` even when it is not the domain owner; the grant remains an ordinary auditable `grant:<domain>:<agentID>` record and does **not** change domain ownership or memory authorship. Ordinary agents remain owner/ancestor-owner gated. Level 1 is read-only; memory submit and co-commit require the effective owner or an explicit level-2 direct/ancestor grant—org membership and federation clearance do not imply write authority. Pre-app-v18 blocks and the activation block retain the old rule byte-for-byte (`internal/abci/app.go`, `web/reassign_handler.go`).

- **Domain ownership transfers agent-to-agent via governance.** `POST /v1/dashboard/network/reassign-domain-ownership` orchestrates, commit-confirmed: `gov_propose(domain_reassign)` -> the sole validator's accept vote drives it to `Executed` -> `TxTypeDomainReassign` atomically flips the owner, records chain-of-custody history, purges unrelated grants, and consumes the proposal. The canonical owner immediately receives owner-derived access, so app-v26 emits no redundant self-grant and never needs the target's private key on the CEREBRUM node. Post-app-v22, the approved new owner must also be a canonical, registered on-chain agent before any ownership mutation; malformed or unknown target IDs fail closed. This transfers current AUTHORITY only; it does NOT rewrite memory authorship - every memory stays authored by its original `submitting_agent`. In v11.17.9, recovery views label the canonical current owner separately from immutable authorship, target lists exclude that owner, and a retry after a completed transfer returns idempotent success instead of a misleading same-agent failure. Proof creation uses the fresher of host wall time and the latest committed CometBFT time, so an idle chain cannot make a newly signed proof stale on arrival; it fails closed when the committed time is more than five minutes ahead of the host. This is creation-side clock selection only: consensus still validates the unchanged app-v20 governance proof window of plus or minus five minutes against deterministic block time. Single-validator node only; a multi-validator chain is rejected because the other validators must vote on the proposal (`web/rbac_signing.go:56-90`; `internal/abci/agent_proof.go:79-83,227-242`; `web/reassign_handler.go`; `internal/abci/app.go`; `web/static/js/app.js`).

---

## Query Scoping — Full Access-Check Pipeline

On app-v23, the versioned policy evaluator first resolves the current
credential/principal kind and applies hard profile restrictions, active local
enrollment or exact linked-reader binding, role verbs, Access Group/resource
scope, clearance, explicit grants, and active federation generation. The
legacy gates below remain the storage/query mechanics and pre-v23 replay model;
an app-v23 Admin bypass is authority over normal local data, never permission
to treat Root or a federated linked reader as an ordinary agent
(`internal/store/appv23_local_rbac.go`; `api/rest/effective_write_denial.go`).

A `POST /v1/memory/query` request passes through these gates in order (`memory_handler.go:517+`):

### Gate 1: checkDomainAccess (DomainAccess policy)

`checkDomainAccess` (`memory_handler.go:159-251`) reads the agent's `DomainAccess` JSON field (on-chain BadgerDB first, SQLite fallback):

- `role == "admin"` → bypass all checks, full access
- `role == "observer"` → write operations blocked
- app-v22 `ReadAllDomains` → read allowlist bypass only; writes still follow
  the ordinary allowlist and all per-record classification checks still run
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
   GetFederation(fedID) → status, maxClearance, expiresAt
   if status == "active" AND !expired AND memoryClassification <= maxClearance:
     require AllowedDomains to contain "*", the exact domain, or a dotted ancestor
     if AllowedDepts is restrictive, require an exact forward membership in
       (agentOrg, allowedDept, agentID)
     return true

return false
```

**Current semantics:** On app-v22 and later chains, access checks use ancestor-walk
behavior for grants and domain ownership. `AllowedDomains` is authoritative:
empty denies, `"*"` allows all, and an exact or dotted ancestor covers a
descendant. Empty or `"*"` `AllowedDepts` is unrestricted; any other list
fails closed unless an exact forward department membership matches. The
legacy single `agent_dept` reverse slot is never authoritative for this check.
Pre-app-v22 replay retains the historical evaluator that ignored both scope
lists.

---

## Federation

A federation is a bilateral agreement between two organizations.

### Proposal and Approval

`POST /v1/federation/propose` → `TxTypeFederationPropose` → persists
`FederationEntry{status:"proposed"}` in BadgerDB and PostgreSQL. A multi-org
caller may provide `proposer_org_id`; it must name one of the caller's exact
memberships. Omission preserves the legacy primary-org default.

`POST /v1/federation/{fed_id}/approve` → `TxTypeFederationApprove` → sets status to `"active"`.

The `FederationID` is deterministic: computed from the two org IDs + height to avoid collisions.

### Federation Record Fields (`internal/store/store.go:253-268`)

| Field            | Type     | Description                                                |
|------------------|----------|------------------------------------------------------------|
| `ProposerOrgID`  | string   | Org that proposed the agreement                            |
| `TargetOrgID`    | string   | Invited org                                                |
| `AllowedDomains` | []string | Which domains are shared; `["*"]` = all; empty denies       |
| `AllowedDepts`   | []string | Dept scope; `["*"]` or empty = all depts                   |
| `MaxClearance`   | 0-4      | Ceiling clearance for cross-org reads                      |
| `ExpiresAt`      | *time    | Nil = permanent                                            |
| `RequiresApproval` | bool   | Legacy/informational; activation always requires target approval |
| `Status`         | string   | `"proposed"`, `"active"`, `"revoked"`                      |

### MaxClearance Cap

`checkFederationAccess` (`badger.go:2156-2175`) enforces: `if memoryClassification > maxClearance → deny`. This means a federation with `max_clearance=1` (INTERNAL) cannot expose CONFIDENTIAL (2) or higher memories to the federated org, regardless of the individual agent's clearance within their own org.

Every proposal remains `"proposed"` until an explicit target-organization
approval changes it to `"active"`, regardless of the stored
`RequiresApproval` value. That flag is retained for wire/storage compatibility;
it does not bypass bilateral activation.

### Revocation

`POST /v1/federation/{fed_id}/revoke` → `TxTypeFederationRevoke` → sets status to `"revoked"`. All subsequent `HasAccessMultiOrg` calls for this pair return false.

### Cross-chain peer trust and RBAC are separate

The organization federation above is an on-chain relationship between local
orgs. The cross-chain federation UI is a separate layer: JOIN establishes the
identity of another SAGE node, then each node applies a unilateral per-peer
domain policy to that trusted identity. A fresh ceremony freezes the remote
chain, remote operator key, CA pin, and policy epoch on both sides
(`internal/federation/join_routes.go:450-455`, `1219-1222`; the persisted peer
key is explicitly distinct from the historical controller key in
`internal/store/sync_tables.go:119-138`). The v3 Manager accepts only the fixed
`{max_clearance:4, allowed_domains:[], mode:"exchange", direction:"both"}`
compatibility scope and rejects any domain-bearing JOIN
(`internal/federation/join_routes.go:75-91`, `257-280`, `730-740`,
`1051-1055`, `1126-1149`). The browser sends that
same empty envelope (`web/static/js/app.js:11680-11694`, `11803-11806`). Thus
trust answers **who is connected**; it grants no current domain access by
itself.

For a trusted peer, the source node replaces a complete snapshot of concrete
existing domains with live Read/Copy capabilities and one reserved field:

| Capability | Meaning |
|---|---|
| **Read** | The peer may run live remote recall in the selected domain subtree. Results are borrowed for that response and are not retained merely because Read is enabled. |
| **Copy** | The source permits replication, but no copy moves unless the receiving node independently subscribes to save that domain locally. |
| **Write (reserved)** | Unavailable in the current protocol. The versioned field remains for compatibility but must be false; neither pairing nor an ordinary `AccessGrant` enables connection-scoped remote submission. |

`Copy` implies `Read`; the stored policy canonicalizes that invariant and rejects
`Write` (`internal/store/peer_rbac_policy.go:26-41`, `113-142`). The dashboard lists the
local node's already registered/observed domains and refuses a grant for a
domain the operator does not control (`web/federation_permissions.go:30-111`,
`292-305`). It shows the local snapshot as editable and the authenticated
peer's snapshot as read-only (`web/federation_permissions.go:161-220`). The
peer performs the same operation on its side for the reverse direction. Either
side may replace its snapshot as domains or working relationships change; no
new JOIN is involved (`internal/federation/peer_rbac.go:193-229`,
`internal/store/peer_rbac_policy.go:226-260`).

Existing v11.8 links also avoid re-pairing when the original ceremony artifacts
still prove the peer. Guest-side rows already name the remote controller; a
host-side row is recovered only from the exact two-member, epoch-matching,
CA-pinned enrollment roster. The recovered key is compare-and-frozen into the
active sync control and cannot later be rebound (`internal/federation/peer_rbac.go:62-118`,
`193-229`; `internal/store/sync_tables.go:378-415`). Ambiguous or incomplete
legacy evidence fails closed and must be re-enrolled.

Enforcement follows the verb, not the trust ceremony. Read gates both the
requested domain and every returned record at the peer boundary
(`internal/federation/server.go:318-363`, `427-453`). Write fails closed: the
peer endpoint returns authenticated `501`, `WritePeer` returns a typed error
before lookup or dial, status omits `write-v1`, and the permissions PUT rejects
`write:true` (`internal/federation/remote_write.go:9-52`;
`internal/federation/server.go:282-315`; `web/federation_permissions.go:223-258`).
Copy egress is the intersection of the source's published domains, the
receiver's subscription, and the source's current `Copy` grant;
ingress independently rechecks remote Publish ∩ local Subscribe
(`internal/federation/sync_outbox.go:351-428`).

The effective policy is also generation-linearized. Peer handlers re-resolve
the exact current agreement and frozen operator under the sync-policy read
lease before serving or admitting anything. Every local tx-33/tx-34 control
surface shares one agreement-mutation lease: JOIN retains it through
CA/seed/control/RBAC activation, a set retains it through matching CA
promotion, and revoke retains it through local capability purge. A legacy
tx-33 narrowing additionally takes the policy write side, so once the mutation
returns no in-flight response can still use the superseded broader agreement
(`internal/federation/server.go`; `internal/federation/join_routes.go`;
`api/rest/federation_handler.go`).

App-v23 also separates that control authority from the peer identity frozen by
JOIN. The stable transport key continues to sign peer requests, attestations,
origin proofs, and receipts, while Manager-originated tx-33/tx-34 and accepted
Copy `MemorySubmit` transactions resolve current CEREBRUM Root. Consensus no
longer permits a Member/Manager to set or replace a peer agreement merely by
owning every domain named in the old compatibility scope; current Root signs
directly and a current-generation Admin requires the ordinary one-action Root
elevation. Missing current Root key fails before broadcast and never falls back
to a retired transport credential (`internal/abci/app.go`;
`internal/federation/v23_guest_control.go`;
`internal/federation/join_routes.go`; `internal/federation/sync_server.go`).

Sync-group sharing is a separate RBAC lane, but it is not a second trust system.
Every inbound group route first requires the live signer to match the exact
chain/operator/CA tuple frozen by the still-active JOIN control; only then may an
exact active group owner/member/domain projection authorize that group's fanout
or relay without borrowing or requiring a direct pairwise Copy/Subscribe grant.
Removing the member/domain or revoking the JOIN closes the affected lane under
the same policy lease used by in-flight reads and writes. Direct v3 sharing still
requires the intersection above, and legacy direct/group paths retain their
historical tx-33 checks (`internal/federation/sync_policy.go`;
`internal/federation/sync_group_ceremony.go`;
`internal/federation/sync_journal_exchange.go`;
`internal/federation/sync_outbox.go`; `internal/federation/sync_server.go`).

Relayed provenance is identity-bound, not merely chain-bound. Admission records
the exact origin operator whose Ed25519 signature verified. Digest and relay
queries require that stored key to equal the origin roster key in the **same**
eligible group as the receiver and relayer; callers enumerate same-domain groups
rather than joining membership from one group to ownership in another. The sender
re-verifies the stored signature immediately before egress, and the receiver tries
only eligible group-pinned keys (`internal/store/sync_group_tables.go`;
`internal/federation/sync_outbox.go`; `internal/federation/sync_server.go`).

The Write denial is deliberately stronger than checking a normal level-2
`AccessGrant`. Such a grant authorizes an agent for a domain and remains usable
through the ordinary submit API outside this particular trusted link. It cannot
represent connection-scoped A↔B authority. A future Write design therefore
needs a consensus-bound ingress capability tied to the active ceremony
generation, frozen peer, domain, and exact submission
(`internal/federation/remote_write.go:10-19`).
Preview-era managed grants are cleanup-only: migration first clears stored
Write bits, then `sage-gui` revokes every tracked exact grant and confirms its
absence before binding any application listener. Failure to complete that retirement aborts
startup even when the federation transport itself is disabled or unavailable
(`internal/store/peer_rbac_policy.go:106-110`; `cmd/sage-gui/node.go`;
`web/federation_grant_cleanup.go:71-161`).

The distinction between “no policy yet” and “a policy granting nothing” is
security-significant. A **present empty** policy is explicit deny-all, and fresh
JOIN activation installs one for the frozen peer
(`internal/federation/peer_rbac.go:219-251`). Only an **absent** policy on a
legacy/unconfigured connection may fall back to historical tx-33 read/sync
scope. A frozen v3 binding whose policy row is unexpectedly absent is
synthesized as deny-all instead (`internal/store/peer_rbac_policy.go:3-6`,
`173-224`; `internal/federation/peer_rbac.go:121-190`;
`internal/federation/server.go:318-355`).

---

## v11.9 quorum scopes are not cross-chain federation

The v11.6+ cross-chain federation path connects independent SAGE chains and
copies selected domains through an authenticated off-consensus transport. An
app-v20 quorum scope instead lives inside one CometBFT chain and names validators that
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

App-v20 governance uses a dual-principal validator gateway. The configured
operator signs the exact REST or dashboard action, including its 8-byte nonce,
the target validator ID, and a committed chain domain; consensus requires that
proof within ±5 minutes of deterministic block time and checks single use,
payload equality, and both bindings before consuming it. This applies to every
proof-bearing governance envelope, including a same-key envelope whose embedded
operator equals the outer validator. Every delegated proposal operation also
requires the embedded operator's registered global-admin role. The outer
transaction is still signed by the live active validator and remains the
proposal owner, deterministic-ID actor,
automatic voter, vote-power holder, and canceller. Delegated vote/cancel calls
use each validator's node-local operator and do not require a shared global
admin key; the governance engine still enforces validator voting membership
and proposer-only cancellation. Truly proofless direct validator governance
(including the upgrade auto-voter), historical non-governance same-key
transactions, and pre-app-v20 replay keep their historical wire behavior
(`internal/abci/agent_proof.go`, `internal/abci/governance_agent_auth.go`,
`internal/abci/app.go`).

The chain domain is lowercase hex of
`SHA-256("sage/governance-delegation-domain/v20\x00" || exact chain_id bytes)`.
The non-empty chain ID is capped at CometBFT's 50-byte limit and is not trimmed
or case-folded. Validators approve the domain inside the signed target-20
upgrade transaction, quorum proposal payload, and pending plan. The activation
block persists its 32 raw bytes before consuming that plan; crash replay,
constructor recovery, and app-v20 state-sync verification require the canonical
state entry. Consequently a proof-bearing governance request observed on one
validator or chain — even a same-key request — cannot be rewrapped for another
outer validator or replayed on a differently named chain. A deliberately
proofless direct validator transaction retains its historical compatibility
semantics and is not domain-bound.
Operators fetch the current binding through authenticated
`GET /v1/governance/context`; a `409` mutation response requires a fresh read
and signature.

The ceremony discriminator is exact: name `app-v20`, target version `20`, and
a canonical lowercase 32-byte governance-domain hex tail. Empty, malformed, or
non-canonical historical target-20 tails remain on the frozen legacy replay
path and never activate the domain-bound rules. Every validator must therefore
restart on the identical v11.9 binary before the tagged ceremony; upgrading
only a greater-than-two-thirds subset is not a supported rollout boundary.

App-v22 has an additional predecessor-coverage invariant at every transition
boundary: proposal admission, approved-proposal execution, activation, and
startup/restart recovery. Ordinarily consensus storage contains canonical,
strictly ordered applied records from app-v6 through app-v21. At the app-v21 to
app-v22 boundary only, a governed v2 repair receipt may supply virtual coverage
for missing pre-app-v20 compatibility rungs. Retained-transition coverage must
bind the exact missing open-interval versions to a real later target activation
at the same Comet height and block hash; direct retained or explicitly attested
legacy-anchor claims remain virtual as well. No skipped rung is written under
`upgrade:applied:*`. Cached fork gates, invented per-rung heights, loose
subsumption, and missing/duplicate coverage do not satisfy the invariant. This
check is confined to the app-v22 transition and recovery boundary, so
historical pre-v22 replay retains its original behavior
(`internal/abci/appv22_agent_capabilities.go`, `internal/abci/app.go`).

App-v23 keeps that canonical app-v22 record as its immediate predecessor.
Activation block H still executes with v22 semantics; H+1 is the first v23
block. Canonical state-sync export/import includes the immutable Root principal,
current and retired Root credential markers, enrollment and role/profile
records, Access Groups and indexes, revisions, memory-author principal
projections, and every new AppHash-covered policy key. Import validates their
cross-index and bound invariants before the restored node may serve. Existing
browser-local visual groups are never promoted into ACLs, SQL is never treated
as consensus input, and pre-v23 replay retains historical AppHashes. Once a v23
state or transaction exists, recovery is a forward repair or a trusted
pre-activation snapshot—not an in-band downgrade to v22
(`internal/abci/scoped_state_sync.go`; `internal/abci/boot_state_sync_runtime.go`;
`internal/store/appv23_local_rbac.go`;
[`app-v23-access-control-design.md`](../app-v23-access-control-design.md)).

Operational prerequisite: before app-v20 activation, every operator intended
to expose a *proposal* gateway must be registered as a global admin. A topology
where only a validator key is admin can use a direct validator-key proposal or
register the separate operator first. Validator-local vote/cancel operators do
not need that admin role.

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
(`internal/abci/appv20_recovery_integration_test.go:152-285`). Network state
sync never accepts that rollback format. It is fail-closed by default and can
be armed only as the separate, explicit consensus-only path described below.

Crash replay is height-bound rather than a nonce bypass. Scoped votes store the
immutable first decision and its FinalizeBlock height atomically; only an exact
submit envelope or exact vote at that same behind height can rebuild projection
writes after a failed Commit (`internal/store/scoped_memory_state.go:214-324`,
`internal/abci/scoped_memory.go:97-207`). The forced-failure integration test
reopens the stores after failed submit and quorum-reaching vote commits, matches
the original AppHashes, restores all SQL votes, and confirms that the same
signed vote at a later height is still rejected
(`internal/abci/appv20_recovery_integration_test.go:287-426`).

The network-state-sync implementation is isolated in `internal/statesync`. Its
versioned wire image is a deterministic, lexicographically ordered stream of
the latest visible Badger keys and values, not a physical Badger backup. LSM
history, tombstones, and expired values never enter it; TTL/user metadata fails
export. Restore requires an empty database and rejects duplicate/out-of-order
keys, invalid lengths/counts, truncation, and trailing bytes
(`internal/statesync/canonical_state.go`). Canonical metadata binds CometBFT's
trusted height/AppHash to that bounded chunked stream, hashes every chunk and
the complete image, caps chunks at 8 MiB and count at 512, and rejects the
unrelated rollback manifest (`internal/statesync/format.go`). The disk assembler
accepts out-of-order delivery, makes an exact duplicate idempotent, rejects
corrupt/wrong-size chunks, and verifies the complete stream before atomic
publication (`internal/statesync/assembler.go`).

`quorum.state_sync` has mutually exclusive `serving` and `receiving` boot roles
and remains off when neither is set (`cmd/sage-gui/state_sync_config.go`). Both
roles require a strict locally installed JSON trust root. Its schema binds
chain ID, joining Comet node ID, the joining node's prospective validator
Ed25519 public key, app
application version 20, 21, or 22, expiry, snapshot floor, existing validator node
IDs, and approved
providers. Provider IDs must exactly equal validator IDs; v1 does not permit a
preferred subset. The loader rejects files over 64 KiB, symlinks, non-regular or
group/world-writable modes, open-time replacement, unknown fields, trailing
JSON, and non-canonical values (`internal/statesync/authorization.go`).
Every authorization and transfer session is pinned to one exact supported app
version: a v20 receiver accepts only a v20 image, a v21 receiver only a v21
image, and a v22 receiver only a v22 image. This preserves existing sessions
while preventing a transfer
from crossing an application-state-machine boundary.

Authorization and a successful transfer do not grant voting power: the sealed
receiver remains a non-validator until a separate signed governance action
adds that key. The prepared and activated application rosters must omit its
canonical validator ID, and both ordinary seal and journal-bearing crash
recovery require its address absent from CometBFT's persisted last, current,
and next validator sets. Normal activation order is durable `sealed` activation-journal fsync,
durable raw `quorum.state_sync.receiving: false` replacement, quarantine and
journal cleanup with parent-directory fsync, runtime transition to `Sealed`,
and only then REST/dashboard/MCP/federation or background service admission.
No serving endpoint may observe an earlier phase.

The in-process `sage-gui` path pins CometBFT v0.38.23 and hardens the effective
armed-node profile before the node starts: PEX/seeds/seed mode are off, ordinary
inbound and outbound limits are zero, unconditional/private IDs exactly equal
the authorization as capacity/privacy sets, authenticated peer filtering is
mandatory and admits only that exact ID set, and persistent peers are restricted
to it
(`cmd/sage-gui/state_sync_p2p_profile.go`). The receiver requires every approved
provider as a persistent peer and at least two distinct HTTP(S) RPC origins for
light-client verification. Provider expiry is rechecked on every list/load.
Receiver expiry is deadline-bound, rechecked through preparation and every
durable activation boundary, and permanently latched once observed so clock
rollback cannot revive the session. Address admission is only a pre-handshake
syntax/liveness gate; the post-handshake ABCI query enforces the authenticated
node ID before Comet adds the peer. Expiry rejects new admissions but does not
disconnect a peer already held by Comet; leaving the one-shot profile requires a
config transition and process restart.
Remote admission runs independently, so an outbound switch can briefly list a
locally approved provider before rejection closes the connection. The provider
has zero ordinary inbound capacity: an unknown authenticated ID is rejected
before `addPeer`. The integrated gate must separately require the live ABCI
query to return `111` for the unknown ID and `0` for the approved receiver,
observe a real provider-side zero-capacity rejection, sample no provider peer or
receiver snapshot/session/height/REST progress, drain both switches, and only
then admit the approved receiver. The final exact-source cold run remains
pending; these are acceptance requirements, not a claimed result.

The root module replaces upstream v0.38.23 with the provenance-recorded local
subset in `third_party/cometbft`. Six overlays prevent an inactive-syncer nil
dereference, recognize only SAGE's seal-abort sentinel as a graceful block-sync
shutdown, bridge positive state over an empty block store, retain that bridge
across consecutive empty-blockstore restarts, enforce `seen commit -> positive
state -> successful block-sync switch -> durable height/AppHash marker`, and
make the state/effective-height bootstrap atomic. Commit and completion-marker
writes are synchronous. If the state DB is independently proven empty, startup
inspects the raw block-store database and may remove only the exact lone valid
seen-commit residue. This avoids the upstream `NewBlockStore` panic on malformed
metadata; additional or malformed content is preserved and rejected.
`make test-cometbft-patch` race-tests both reactors, two consecutive empty-store
restarts, atomic bootstrap-marker persistence, ordered failure boundaries,
marker encoding, and raw exact-residue recovery. The pinned source commit is
`feb2aea4dc271d612129afc958cb844713ec792b`; because that v0.38.23 source still
declares core semver `0.38.22`, the stamped standalone runtime reports
`0.38.22+feb2aea4dc271d612129afc958cb844713ec792b`.
`third_party/cometbft/README-SAGE.md` records source/module provenance, exact
overlay files, and retained Apache-2.0 license/NOTICE.

At provider startup, effective block retention must be zero (`retain_blocks: 0`
is the quorum default); a positive pruning window is rejected until rolling
snapshots become block-base aware. Maintenance then sweeps only proven SAGE-
owned staging and retains eight verified snapshots. Export writes the canonical
latest-visible stream, restores it into an isolated database to match committed
app-v20 height/epoch/AppHash/scoped state, and atomically publishes metadata/
chunks before P2P exposure. `ListSnapshots` advertises snapshot `H` only after
live height reaches `H+2`; the two newest retained candidates may wait for those
light blocks while six older fallbacks remain. `LoadSnapshotChunk` uses that
same de-duplicated catalog and rehashes immediately before read
(`internal/statesync/exporter.go`,
`internal/abci/boot_state_sync_endpoints.go`).

A receiver accepts a role only when application Info and raw Badger are empty
and SQLite contains exactly the default domain seed rows while every other
application table is empty. Persisted Comet state must also be empty. Startup
may then remove only an exact lone valid seen-commit residue; afterward the
`state`, `blockstore`, `evidence`, and `tx_index` databases must have no entries.
The WAL is absent or a zero-length regular non-symlink file, FilePV signing
state is zero, and genesis cannot initialize a single-local-validator chain.
Offer and preparation separately preflight `2 * snapshot + 256 MiB` and
`4 * snapshot + 256 MiB`, respectively, on their target filesystems. Local
capacity failure is terminal rather than a provider rejection. Native restore
failures such as Unix `ENOSPC`/`EDQUOT` and Windows disk-full/quota equivalents
remain terminal even after a successful preflight. Each chunk
sender must be an approved existing validator/provider. Complete candidates
are restored and verified in isolation while live state remains open; a
malformed candidate can be rejected back to discovery for another provider,
bounded to eight rejections (`cmd/sage-gui/state_sync_runtime.go`,
`internal/abci/scoped_state_sync.go`,
`internal/abci/boot_state_sync_endpoints.go`).

Activation deliberately does not mutate `BadgerStore.db` inside a serving app.
Under the runtime's exclusive lease it journals and switches whole Badger
directories, reopens without migrations, constructs and publishes a complete
replacement `SageApp` bundle, then waits for the running Comet state store to
persist matching height/AppHash/app version and a valid seen commit, complete
the block-sync switch, and publish the exact durable completion marker. That
wait defaults to 30 minutes and any positive explicit duration is accepted.
Externally triggerable `Query` and `CheckTx` fail fast throughout every
unsealed phase, including PendingComet, so neither can hold Comet's shared in-
process ABCI-client mutex while the syncer performs its final `Info`. Consensus
methods fail fast before PendingComet; a consensus block call arriving after
the switch waits behind the seal without holding the bundle lease. `Info` and
the state-sync methods remain available throughout the receive phases. Only
after sealing and freezing bundle replacement may
projection, snapshot, REST/dashboard/MCP/federation, voter, or other workers
capture the final app/store graph
(`cmd/sage-gui/state_sync_runtime.go`, `cmd/sage-gui/state_sync_boot.go`,
`cmd/sage-gui/node.go`). Pre-handshake journal recovery still resolves crashes
before the canonical Badger directory opens. When recovery keeps a matching
activated directory, startup durably changes only the raw
`quorum.state_sync.receiving` YAML node to `false`, fsyncs that replacement and
its parent, and performs the same idempotent transition during matching crash
recovery before removing the journal. The process then resumes as an ordinary
synchronized node without re-arming on a later restart. From the
`prepared` transition onward, any local close, journal, rename, reopen, or
bundle-activation failure is terminal and does not consume provider fallback.
If shutdown/failure releases a waiting block call, it returns SAGE's seal-abort
sentinel; only that sentinel selects the graceful block-sync exit, while every
unrelated application failure retains upstream panic behavior.

This is implemented opt-in behavior, not by itself a v11.9 release claim. The
split real-Comet Docker topology does not itself run state sync or the scope
state-machine ceremony. Its firewall partitions preserve private per-validator
ABCI links, publish host REST/RPC only on loopback, use an owned temporary
fixture, prove the 2/4 peer split and halt, and require the same latest Comet
block hash plus live ABCI height/AppHash and `catching_up=false` after healing.
The standalone image
copies the same six overlays as `sage-gui`. A `v11.9*` release enables both
`V119_REQUIRE_SCOPED_RECONFIG=1` and
`V119_REQUIRE_AUTHORIZED_STATE_SYNC=1`; the scoped flag is routed to the
race-enabled signed OS-process oracle and composes with the real-Comet
partition proof. The real-transfer proof must use integrated
`sage-gui serve` processes; split `amid`/Comet containers do not own the boot
runtime or seal boundary.
Operationally, Comet P2P (normally raw TCP 26656) plus the configured Comet RPC
origins must be reachable. Internet validators need routable addresses, port
forwarding, or a VPN today; federation is not a validator tunnel. A future
tunnel layer is separate, out-of-scope work and is not a v11.9 release gate.
The remaining state-sync evidence is the final exact-source cold execution of
the integrated authorized provider-to-two-receiver transfer. Signed app-v20
formation/revision and pinned ballots are covered by the composite fault
workflow; its held subprocess is not described as a TCP partition. The detailed
schema and gates are in
[`../../v11.9-state-sync-activation.md`](../../v11.9-state-sync-activation.md).

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

### Federated pipeline receipt evidence (app-v26)

Receipt v2 is a distinct, capability-negotiated protocol; it does not overload
the v1 pipeline event or `/fed/v1/receipt` co-commit protocol. Every accepted
event binds the immutable origin message, content digest, exact sender and
recipient agents/chains, and the active agreement/policy/contact/linked-relation
generation. A new claim/read transition is admitted only after live relation
revalidation. Accepted historical evidence remains queryable by the original
sender after a later pause or revocation, but an event from the retired
generation cannot advance it.

Evidence dimensions are deliberately not one ordered status enum. Remote-node
durable admission, recipient claim, recipient read, and terminal outcome are
independent write-once facts. Network timestamps never decide which fact wins.
Only the exact original sender can query the payload-free projection; sovereign
Root and administrative roles do not become message participants. Legacy v1
and pre-v2 rows remain unsupported/unconfirmed, and migration creates no
evidence.

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
