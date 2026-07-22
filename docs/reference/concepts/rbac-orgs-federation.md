<!-- Core document reconciled through SAGE v11.11.6, including directional cross-chain peer RBAC and the quorum/state-sync/governance-gateway sections. -->

# RBAC, Organizations, and Federation

Verified against code at SAGE v11.7.0, with the v11.9 quorum/state-sync section
updated against the 2026-07 opt-in implementation and open release gates.

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
| **Write (reserved)** | Unavailable in v11.9. The versioned field remains for compatibility but must be false; neither pairing nor an ordinary `AccessGrant` enables connection-scoped remote submission. |

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
version 20, expiry, snapshot floor, existing validator node IDs, and approved
providers. Provider IDs must exactly equal validator IDs; v1 does not permit a
preferred subset. The loader rejects files over 64 KiB, symlinks, non-regular or
group/world-writable modes, open-time replacement, unknown fields, trailing
JSON, and non-canonical values (`internal/statesync/authorization.go`).

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
