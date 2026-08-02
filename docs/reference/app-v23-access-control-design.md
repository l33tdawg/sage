# App-v23 Access Control and Federation Design

Status: implementation contract through SAGE v11.17.0.

This document fixes the security and product invariants for app-v23. It is not
permission to weaken an invariant to preserve app-v22 runtime behavior.
Historical blocks must still replay byte-identically.

## Product acceptance gate

Mynah / SAGE Voice Bridge is a first-party vendored application that creates a
fresh SAGE node and its genesis-operator key. A clean installation must be able
to store its first memory without a manual CEREBRUM step.

Mynah has no released legacy population. It therefore installs a fresh SAGE
node whose authenticated genesis starts directly at consensus app version 23;
it does not create an app-v1 node and wait for the historical upgrade ladder.
A root-bound bootstrap manifest must atomically establish the bundled agent
with:

- its canonical Ed25519 agent identity;
- the Companion security profile (app-v22 capability mask `15`);
- its configured clearance;
- an owned, non-shared home domain; and
- active local enrollment on this SAGE authority.

The liveness `/health` endpoint may remain healthy while this reconciliation is
in progress. `/ready` is the serving gate and returns unavailable until the
exact enrollment is commit-confirmed **and app-v24 is active for the next
admitted transaction**. During the governed app-v23-to-app-v24 climb, consensus
also rejects memory and co-commit writes from the exact direct-genesis
Companion; readiness cannot be bypassed through REST, MCP, or a raw transaction.
The manifest is bound to keys, not the display name "Mynah". Generic
self-registered third-party keys retain the restricted default policy. If the
vendored bootstrap or app-v24 activation cannot be authenticated and committed,
startup fails loudly instead of exposing a healthy-but-mute agent.

For first-party task creation, an omitted domain deterministically resolves to
the caller's currently committed owned home domain. The REST process places
that exact domain in the transaction, and consensus independently derives it
from the signed omitted-domain request plus AppHash-covered enrollment state
before accepting the payload. An explicit domain is never remapped: an
explicit unowned or restricted domain returns the stable permanent write
denial and no transaction is submitted. A task-creation response is successful
only after commit and exact-assignee backlog readback; a client must never say
"added" merely because it attempted the write.

This genesis path is not a compatibility shortcut: it commits an explicit
app-v23 genesis marker, initializes every direct-genesis prerequisite, reports
app version 23 after `InitChain`, and restores the same state across restart and
state sync. The personal-node upgrade watchdog then advances through the
governed app-v24 boundary before serving the Companion, even when optional
future auto-upgrades are disabled. It must not mint a duplicate level-2 grant
for the Companion's home domain; ownership remains the canonical authority
source.

Existing standalone SAGE nodes still reach app-v23 through the governed
upgrade and deterministic migration path. There is no legacy Mynah-specific
repair path.

Other agents registered after app-v22 with mask `30` and no owned non-shared
domain are reported as `needs_approval`. They are never relaxed automatically:
an Admin reviews and commits the same atomic onboarding operation. This repairs
the broad post-v22 onboarding gap without turning migration into a mass
privilege escalation.

All other healthy legacy agents retain their effective data-plane authority
through migration: existing domain ownership, explicit grants, and ordinary
unrestricted domain-claim behavior remain available. App-v23 must not newly
mute a principal that could write before activation. An active Standard
Member or Manager without the hard `DenyDomainClaim` capability may therefore
claim an unowned non-shared domain; Companion, Read-only, pending, and
hard-denied principals may not.

## Principals and roles

CEREBRUM Root is an immutable authority principal with a rotatable credential.
It is not an ordinary agent role and cannot be removed or demoted.

At app-v26 H+1, the current local Root or an active current-generation local
Admin may change a governed non-Root agent's mutable display name from Access
Controls. This is deliberately narrower than generic metadata editing:
consensus requires the `AgentUpdate` boot bio to equal the target's current
boot bio, and the browser never supplies it. The target's `agent_id`, immutable
registered name, boot purpose, enrollment, role, profile, domains, and memory
authorship remain unchanged. A no-op is reported as uncommitted; an uncertain
broadcast is reconciled against canonical agent state before CEREBRUM reports
success. Root handover remains a rare separate card below the everyday
agent/group controls, not an agent rename or role action.

Root never appears in the agent roster, role editor, Access Group picker, or
agent-removal workflow. Generic agent lookup, search, messaging, pairing,
claim-token, key-rotation, bundle-download, OAuth, and MCP-token paths must
also treat the immutable Root principal and every current or retired Root
credential generation as not-an-agent. The same rule applies to task assignment, pipeline delivery,
organization/department membership and clearance, agent metadata,
memory-reassignment, and generic domain-transfer targets. Consensus rejects
these targets even when a caller bypasses REST and submits a raw transaction.
This check is repeated at credential redemption so a pairing code, claim token,
or bearer created before activation cannot disclose or delegate Root authority
after activation.

A Root handover does not create two Root principals: the replacement distinct
local credential proves possession, current Root authorizes the rotation, and
consensus commits the next credential generation. Before broadcast, CEREBRUM
durably stores the replacement and proves that the production key resolver can
load it as the proposed credential. After commit, CEREBRUM verifies that
consensus names that exact credential and that the local signing broker can
resolve it; the old credential then has no authority. The Root principal itself
is never deleted. Handover transfers only the current operational title: it
does not migrate, duplicate, or relabel any domain or memory. The replacement
credential immediately exercises the stable Root principal's authority over
all existing Root-owned domains and may write new memories there. Historical
memories remain readable with their original immutable author attribution,
including attribution to a retired Root credential where that is the ledger
truth. CEREBRUM presents handover as a separate two-confirmation
ceremony (including an exact typed warning), never through an agent card.
The second stage binds the request to the Root generation displayed when the
ceremony began; stale or replayed requests fail before generating or submitting
a replacement.
Promoted Admins cannot download, export, pair, claim, or recover Root key
material. Any recovery-bundle delivery is dedicated to the handover ceremony,
requires the current Root ceremony authority, and never makes the current Root
key available through a generic agent-bundle URL. The authenticated no-store
handover response delivers the replacement archive once; there is no endpoint
that downloads the currently live Root credential after the fact. Old local key material is
retained as a recovery archive until the newly committed credential is locally
resolvable; retaining bytes does not retain authority.

Root credential rotation is not federation transport-key rotation.
`config.yaml`'s `agent_key_file` is the stable node transport credential
(historically also the pre-v23 operator signer) frozen by existing JOIN
agreements. It may supply the initial Root credential at genesis, but after
handover the two roles diverge: the
replacement credential is current Root, while the old key has no Root authority
and remains only the domain-separated transport identity that peers already
pin. Handover must not rewrite `agent_key_file`, peer operator/CA/epoch
bindings, or agreement history. Rotating that transport identity requires an
explicit future peer re-key protocol or fresh bilateral re-pairing; silently
substituting the new Root would break every existing connection.

The inverse separation is equally strict: the stable transport key cannot keep
signing new local consensus mutations merely because peers still pin it.
Manager-driven JOIN/re-pair/policy changes (`CrossFedSet`), permanent revoke
(`CrossFedRevoke`), and a receiver's new local federated Copy `MemorySubmit`
resolve the exact current Root credential from the local key vault after
app-v23. Missing Root state/key fails before broadcast, with no fallback to
`agent_key_file`. Transport request signatures, agreement pins, JOIN
attestations, sync origin proofs, and co-commit transport receipts remain
unchanged. A current local Admin may initiate the corresponding localhost
control action; any Admin-attributed consensus envelope retains the standard
one-action Root elevation requirement.

Root authority continuity and memory provenance are separate. Historical
memories retain their original submitting identity forever. The handover never
rewrites authorship, ownership history, or prior blocks. Beginning with the
committed rotation, any new Root-originated memory records the replacement
credential identity that actually signed it, while authorization and governance
continue to project that current credential onto the singleton Root authority
principal. Root-owned domains, grants, groups, and read history stay attached
to that stable authority principal: handover performs no bulk domain transfer
and cannot orphan prior memories. The replacement credential must be able to
read and manage the same domains immediately, recall both pre- and
post-handover memories in one query, and write new memories there; the retired
credential must be denied. Tests cover those operations immediately before and
after the handover boundary.

Every credential generation that has ever represented Root is marked
permanently in consensus state. A retired credential can never self-register,
be targeted as an ordinary agent, or be selected as a later replacement Root.
This keeps the handover irreversible and prevents a compromised archived key
from being reactivated after a subsequent generation.

The only post-v23 agent roles are:

- `member`: ordinary local principal. At app-v26, each Access Group's explicit
  authority tier supplies the derived read/write/modify verbs for that group;
  ownership and compatible direct grants remain independent sources.
- `manager`: local management role. It does not silently widen a group's
  explicit app-v26 authority tier.
- `admin`: sudo-equivalent authority over normal local data, policy,
  governance, federation, and CEREBRUM operations. The root credential,
  root-recovery ceremony, and root identity remain non-delegable.

At app-v26, each Access Group supplies both its shared scope and its explicit
member authority tier. Roles retain their non-group management meaning;
clearance supplies the maximum classification. Capability restrictions are
hard denies and override ownership, group authority, roles, and grants. Local
enrollment supplies the authority boundary.

An Admin or Manager transition must be atomic with a compatible security
profile. The system must reject contradictory states such as Admin with the
restricted self-registration mask `30`, or Manager with
`DenyForeignDomainWrite` set.

## Local enrollment and host authority

Consensus may prove only that CEREBRUM Root enrolled an agent for this
authority generation and that the agent proved possession of its key. An
exportable Ed25519 key does not prove physical location.

CEREBRUM is a localhost-only human control plane. The SPA, login/lock/check,
recovery, and every headerless or session-authenticated dashboard route require
both a loopback socket peer and loopback HTTP `Host`; browser requests also
require the normal same-origin and anti-rebinding checks. With vault encryption
off, that narrowly identified local SPA acts as current Root for the complete
CEREBRUM surface. With encryption on, it must additionally present a valid
unlocked vault session. This distinction protects encrypted content at rest; it
does not turn encryption into the source of Root authority.

Current Root and eligible Admin signatures may authorize CEREBRUM management
only when the request also arrives through localhost. Copying an agent or Root
key to another computer must not create a remote administration path. Remote
signed-agent compatibility and REST/MCP/federation data-plane access apply only
to active ordinary Member or Manager identities under their route-specific
authorization. A current Root or Admin credential remains localhost-bound even
when it calls a data-plane REST route; a remote copy of either credential is
rejected at the common signed-REST boundary. Root is never reinterpreted as an
ordinary agent: it cannot own an agent task assignment, task-notice inbox, or
pipeline inbox, and every current or retired Root credential is rejected from
those surfaces. Historical agent task, notice, boot, and governance-read routes
below `/v1/dashboard` remain agent APIs for eligible Member/Manager callers;
their path does not turn them into human CEREBRUM management. Pairing and claim
redemption remain narrow token-bound exceptions. A process already executing
as the same operating-system user can still reach the unencrypted loopback
surface; stronger local-user isolation is an OS/keychain concern, not a
property HTTP headers can provide.

Host-level Admin actions require:

- the exact Admin agent signature over the action;
- an action-bound local CEREBRUM elevation/countersignature;
- authority/chain and installation generation;
- freshness and single-use replay protection.

The broker executes the operation without distributing the root private key.
After sanctioned node migration, delegated Admin elevation remains suspended
until locally rebound. Strong physical-device provenance requires a future
non-exportable Secure Enclave or TPM credential.

Consensus actions consume height-bounded elevation nonces in consensus state.
Node-local actions such as linked-reader mutation use a separately
domain-separated, short-lived root signature and consume its nonce atomically
with the local SQL mutation. Local authorization must never write
non-deterministic replay state into Badger's consensus namespace.

Optional human MFA, including TOTP, is deferred. If added later it belongs to
the local CEREBRUM unlock ceremony, never consensus execution: a shared
time-window secret is not a second signing key and validator wall-clock skew
must not affect deterministic authorization. App-v23 therefore adds no TOTP
setup burden for ordinary personal, family, or small-team nodes.

Legacy or keyless OAuth/MCP bearers must never inherit Root signing authority.
Activation durably revokes or disables every bearer whose fallback signer could
resolve to the Root principal or credential. After app-v23, token issuance
requires a distinct keyed token identity and synchronous normal
`AgentRegister`, producing a restricted Member pending review rather than Root.
An unavailable key registrar is a hard issuance failure, not permission to
fall back to the node Root key. New app-v23 tokens always seal their Ed25519
private key with AES-256-GCM using a domain-separated HKDF-SHA256 key derived
from the one-time bearer plaintext and a random per-row salt. This format is
independent of optional ledger enable/disable, lock/unlock, and passphrase
changes. The stored bearer digest is only an index/fingerprint and cannot
decrypt the envelope; authentication must present the actual bearer to unlock
only its own key. Existing vault-sealed keyed rows remain readable while the
vault is unlocked and atomically rewrap into the bearer format on their next
successful authentication. Before that migration a locked vault rejects them;
it never falls back to Root.

OAuth consent is also a local control-plane action. Public
`/oauth/authorize` validates DCR/PKCE input but never renders CEREBRUM or uses
a public-host dashboard cookie. It redirects to a loopback-only
`/oauth/approve` route with a signed opaque five-minute, single-use handoff.
That local route resolves current committed Root or an eligible local Admin
again on both render and submit. It is absent from discovery and the
Cloudflare ingress allowlist; stale Root, expiry, replay, tampering, and
remote/reverse-proxy locality all fail closed. Pending handoffs are also
per-source rate-limited and hard-capped in process memory.

The OAuth delivery boundary also stores neither raw authorization code nor raw
bearer. SQLite contains SHA-256(code) and an AES-256-GCM delivery envelope
whose domain-separated HKDF key requires the raw code plus a random per-row
salt. A database snapshot, including the stored digest and PKCE challenge,
cannot decrypt the bearer. Redemption is atomic and wipes the envelope.
Unsafe in-flight plaintext rows from pre-v23 cannot be safely upgraded after a
possible database copy; migration revokes their tokens and erases the rows.

## Access Groups

Access Groups are new app-v23 consensus objects. They are not the existing
browser `localStorage` groups and not federation `sync_group_*` replication
groups.

An Access Group has two disjoint compartments:

1. Local members. Only active, root-enrolled local agents may enter this set.
2. Federated linked readers. These are chain-qualified, node-local,
   Admin-signed read relations and can never be cast as local membership.

Local rights are derived dynamically from current effective domain ownership,
including owned descendants and future domains. Do not materialize pairwise
ordinary grants.

Properties (app-v26 extension):

- Every group persists one default local-member authority tier: Read,
  Read+Write, or Read+Write+Modify.
- The tier applies to fellow local members' owned domains independently of the
  member's global Member/Manager label. Admin and CEREBRUM Root retain their
  separate global authority.
- Multiple groups form the union of allowed scope and the strongest applicable
  authority.
- Hard capability denies and clearance still intersect that union.
- Shared and ownerless domains do not become group-owned.
- Membership never changes ownership or memory authorship.
- Removal or group deletion revokes only derived group access. Each agent keeps
  full authority over its own domain tree. Ownership transfer affects derived
  access immediately in consensus order.
- App-v26 whole-domain reassignment binds the operator-approved proposal and
  execution transaction to the exact canonical owner observed before the
  proposal. If another committed transaction changes ownership first, the
  stale transfer is rejected without mutation. The trailing wire binding is
  admitted only at H+1; activation height H retains the app-v25 encoding.
- App-v26 canonical ownership itself grants the active owner its policy-limited
  read/write/modify authority. Reassignment therefore purges only unrelated
  grants and does not emit a self-grant or require the target private key on
  the CEREBRUM node.
- Open challenge electorates remain frozen according to their existing
  consensus record; later membership churn does not rewrite history.

## Atomic onboarding

Third-party self-registration produces a visibly restricted, unapproved
principal. Approval is one commit-confirmed operation combining:

- local enrollment;
- role;
- named security profile/capability mask;
- clearance;
- owned non-shared home-domain creation or explicit transfer; and
- expected revision bindings.

When local CEREBRUM generates a new agent, it must durably store the target
seed and recovery bundle before submitting that target's self-signed
registration. The handler then waits for commit and rereads the canonical
restricted pending state before returning success. A background registration
or a `201` issued before consensus can strand an unapproved identity without
its consent key and is forbidden.

For an already self-registered SDK agent, CEREBRUM may offer the same approval
only when the exact target key is locally resolvable. The production resolver
recognizes the configured node operator key, `~/.sage/agent.key`,
`~/.sage/agents/*/agent.key`, `~/.sage/bundles/*/agent.key`, the configured
vendored-agent key, and the Python SDK's documented
`~/.sage/identities/*.key` location. It does not recursively scan arbitrary
paths or follow identity symlinks. A key outside those managed locations must
be deliberately imported before approval; apparent name/provider locality is
never substituted for key possession.

The primary UI exposes named profiles and effective permissions. Raw capability
bits remain a read-only advanced diagnostic surface; they are derived from the
selected strict profile and cannot be toggled independently.

An active Read-only principal has no home domain. Leaving Read-only is therefore
a new consent boundary, not a role-only update: CEREBRUM must use the atomic
dual-signed approval transaction to bind an owned non-shared home domain and
the target agent's exact local-key consent. The role-change transaction fails
closed if the target does not already retain a valid owned home domain.

## Central authorization

Every security-sensitive path must use a single versioned policy evaluator.
The policy algebra is:

```text
hard capability denies
∩ active local/federated principal kind
∩ role verbs
∩ group/resource scope
∩ clearance
∩ explicit grants
∩ active federation generation
```

ABCI is authoritative for writes and lifecycle mutations. REST/dashboard
checks are early diagnostics only. Direct CometBFT transactions must receive
the same result.

The evaluator returns a stable sanitized reason code as well as allow/deny.
Permanent write denials include at least:

- `missing_write_grant`;
- `foreign_write_restricted`;
- `shared_write_restricted`;
- `domain_claim_restricted`;
- `principal_pending_review`;
- `no_owned_home_domain`; and
- `manager_scope_denied`.

Remedies are derived from the reason. For `missing_write_grant`, use the owned
home domain as the narrow action or, only when shared management is actually
intended, have Root/Admin place the principals in an Access Group and
explicitly select Read + write or Read + write + modify.
CEREBRUM does not claim to offer a direct level-2 grant editor in this release.
Neither action is suggested when a capability restriction would override the
resulting scope.

## Federation v23

Runtime federation after app-v23 has no pre-v23 compatibility fallback.
Negotiation rejects peers that do not support the v23 agent-delegated query
protocol.

A visual drag of `X@Laptop-B` into a Laptop-A group means "Attach as linked
reader", never local membership.

Federated Query v2 requires:

- the existing exact outer peer-operator, CA-pin, agreement, and policy-epoch
  authentication;
- a nested original requesting-agent Ed25519 proof;
- exact source and destination authority binding;
- exact canonical query/domain/body binding;
- agreement generation, nonce, freshness, and replay protection; and
- an active linked-reader relation for that exact remote principal.

Effective disclosure intersects the linked reader's host-selected
classification ceiling, active group, current member-owned domain, active peer
Read policy, and both authentication layers.

Linked readers receive live Read only. They never receive Copy, Write, Modify,
claim, ownership, grants, roles, governance, or transitive remote-agent access.
The authenticated remote-write route remains `501`.

Linked-reader messaging uses the separate pipeline authorization mode
`linked-v23`; a read relation is never treated as a contact or inbox
permission. Messaging starts blocked. The receiving node's local CEREBRUM
operator must explicitly accept one exact tuple:

```text
(remote_chain_id, remote_source_agent_id, local_target_agent_id)
```

For a host group containing local agents B and C with linked reader
`X@Laptop-B`, Laptop A may independently allow `X -> B`, `X -> C`, both, or
neither. The reverse `B/C -> X` direction requires separate receiver-local
consent on Laptop B. Because Laptop B does not own Laptop A's guest row, it
discovers only bounded, host-signed member-to-guest offers for exact local X;
it never imports A's roster, names, domains, or group membership. Consent is
revisioned with compare-and-swap and bound to the current
JOIN/operator/CA/policy generation. It does not create a domain grant, contact,
membership, role, ownership, or transitive route.

CEREBRUM never fans an exact local receiver ID out to connected peers.
Changing the green local receiver is local UI state only. The operator must
select one host node and deliberately request its signed offers; that action
issues one bounded request to that host. Empty, unavailable, paused, stale, and
unrelated results use the same no-offer presentation so the control is not a
peer-status oracle.

The messaging picker is a separate exact-pair projection, not the advertised
remote-agent or Linked-reader discovery list. A locally hosted pair appears
only while its exact link is active, its agreement binding is current, and the
selected receiver is a current member of that link's exact local Access Group.
A peer-hosted offer retains its exact `local_agent_id` and disappears when a
different receiver is selected. Directory-only, unlinked, stale, and
receiver-mismatched identities cannot enable consent review or mutation.

Every send, destination admission, claim, completion, and result revalidates
the exact signed linked-reader row, host group and member, direction, agreement
binding, consent revision, and ordinary-agent eligibility. Root and every
retired Root credential are ineligible. Group removal, linked-reader pause or
revoke, agent suspension, hard federated-pipe restriction, policy pause,
agreement expiry, or re-pair fails closed. Directory/name lookup and
`peer_agent_id` never substitute for the exact linked identity.

Messages remain untrusted request data. A receiving local agent may
independently act under its own identity, but a message carries no delegated
authority and never auto-commits memory.

Federated guest records bind the exact remote chain, agent, CA/operator, and
agreement generation. Revoke, expiry, or re-pair moves them to
`rebind_required`; a new JOIN never resurrects an old relation.

## Fork and migration

- New transaction codecs ship dormant and are rejected by CheckTx and
  FinalizeBlock before app-v23.
- App-v23 requires the canonical predecessor ladder through app-v22.
- Activation block H executes with app-v22 semantics; H+1 enables app-v23.
- H is a deterministic migration quiescence barrier. A pending canonical
  app-v23 plan causes every transaction delivered at H to return code `96`;
  ordinary transaction execution resumes at H+1. This freezes the migration
  input at committed H-1 state and prevents a registration, domain transfer, or
  policy mutation in H from racing the activation projection.
- The activation manifest commits the exact root credential, legacy Admin
  roster/disposition, and migration schema digest.
- A roster of at most 512 agents is projected in the activation transaction.
  Larger valid rosters use a crash-recoverable promoted-stage protocol:
  deterministic logical agent/enrollment/role/disposition/domain rows are
  written in bounded 256-entry Badger transactions before FinalizeBlock opens
  H's speculative transaction. The reserved `0xff` stage namespace and its
  readiness record are excluded from AppHash and canonical network state sync
  before activation. Private full-database rollback snapshots may preserve
  these local bytes, but verification treats them as non-authoritative until
  the marker exists. Partial batches, stale bytes, a missing readiness record,
  or a pre-marker restart are therefore invisible and cause an exact
  idempotent rebuild from H-1 state.
- The final H transaction contains only Root/history, migration headers, and at
  most 32 Admin-index rows. Its `appv23:migration_state` write is the sole
  visibility edge: it commits the stage count and SHA-256 digest and promotes
  the exact prepared rows into AppHash atomically. This transaction size is
  independent of roster size and remains far below Badger's transaction
  bounds. The same transaction deletes the temporary readiness record: a
  crash or discarded speculative transaction before the marker retains it for
  exact replay, while a successful activation leaves no stale lifecycle
  sidecar. Once the marker commits, missing or changed stage bytes are
  consensus corruption and validation fails closed; they are never regenerated.
- Promoted rows remain immutable migration baselines. A later canonical
  app-v23 logical key overrides its staged counterpart, so ordinary role and
  enrollment changes do not rewrite migration provenance. AppHash,
  Take/Verify/Restore, and canonical state-sync export/import all apply the
  same marker-aware stage-inclusion rule.
- The immutable audit roster records every pre-v23 Admin. The earliest legacy
  Admin by `RegisteredAt`, with canonical Agent ID as the deterministic
  tie-breaker, becomes the singleton Root. Consensus cannot prove that any
  other exportable legacy Admin key is still local to this machine, so every
  other legacy Admin becomes an active Member with its exact capability mask,
  the migration-only `legacy_restricted` profile, and disposition
  `legacy_admin_review`. No historical Admin—including one within the 32-Admin
  runtime bound—is promoted automatically. A later Admin promotion requires
  an explicit current-Root-attested local review. The complete old Admin roster
  remains immutable audit evidence.
- Ordinary legacy Members retain their exact known app-v22 mask. Masks `0` and
  `16` map to `standard`; masks `15` and `31` map to `companion`; every other
  known mask maps to migration-only `legacy_restricted`. That profile is a
  read-only CEREBRUM review state, not a fresh policy preset: bootstrap,
  enrollment, policy-edit, REST, UI, and raw consensus mutation paths reject
  attempts to select it.
- A legacy principal that already owns a non-shared domain keeps its
  deterministic first owned domain as home. A domainless principal without
  `DenyDomainClaim` receives a deterministic owned home because app-v22 already
  allowed it to claim one. A domainless principal with `DenyDomainClaim`
  remains domainless; migration does not manufacture ownership that its old
  mask prohibited.
- Exact mask `30` plus no owned non-shared domain and no structurally valid
  explicit level-1-or-higher grant is the app-v22 bare self-registration
  fingerprint. It retains mask `30` but becomes inactive with
  `pending_review`. If an explicit grant exists, it remains active and
  domainless under `legacy_restricted`, preserving that reviewed read
  authority while all four deny bits continue to win.
- App-v22 let an active non-observer write an ownerless shared domain when bit
  `2` was absent. App-v23 preserves only that memory-submit `Write` authority
  for unchanged migrated dispositions `member`, `legacy_restricted`, and
  `legacy_admin_review`. The principal must still be active at enrollment and
  role revision `1`, its current profile/home/active fields must match the
  immutable migration disposition, and `DenySharedDomainWrite` must be absent.
  It never grants `Modify`, challenge, deprecate, or reinstate. Any explicit
  policy review increments a revision and permanently moves the principal to
  normal app-v23 shared-domain grant semantics. Fresh/direct-genesis agents
  and `bootstrap_preserved` principals never receive this grandfathering.
- Consensus never reads SQL, filesystem state, browser state, or local key
  discovery.
- Existing visual browser groups are migration drafts only and are never
  silently promoted into ACLs.
- Legacy `observer` maps to Member plus an explicit read-only profile.
- Migration-assigned and fresh first-party home domains confer authority by
  ownership. Activation and genesis do not synthesize a duplicate owner grant;
  explicit grants remain separate ACL state.
- Badger remains authoritative; SQL/Postgres are rebuildable projections.
- Canonical state sync includes every new consensus key and validates root,
  role, enrollment, group-index, revision, and bound invariants after restore.
- Pre-v23 replay retains historical AppHashes.
- After v23 state or transaction types are committed, recovery is a forward fix
  or a trusted pre-activation snapshot, not an in-band downgrade to v22.

## App-v24 readiness and memory-write barrier

App-v24 is the v11.16.0 memory-integrity successor to app-v23. It requires
app-v23 as its immediate semantic predecessor. Its activation block H executes
with app-v23 semantics; app-v24 rules begin at H+1. This strict boundary keeps
historical replay and the activation block byte-identical.

For a fresh first-party node whose authenticated genesis starts directly at
app-v23, `/ready` reports `waiting_for_app_v24` until a transaction admitted
now will execute under app-v24. Consensus independently rejects memory-submit
and co-commit writes from the exact committed Companion profile during this
brief interval. The restriction is deliberately narrow: it applies only to a
direct-v23-born chain and that Companion enrollment. Ordinary upgraded nodes
and all of their existing agents retain app-v23 write behavior while the
governed fork activates.

Once active, app-v24 requires every new `MemorySubmit.ContentHash` to equal the
exact SHA-256 of `Content`. Challenge, deprecate, and other terminal lifecycle
transitions preserve that canonical hash instead of replacing it with the
historical nil encoding. A Root-planned, validator-approved repair may re-anchor
eligible historical terminal rows from their unchanged canonical content and
status. Its plan is bounded, generation-bound, atomic, and idempotent; it never
rewrites memory content, authorship, domain ownership, or prior blocks.

## App-v25 historical recovery and writer continuity

App-v25 is the strict H+1 successor to app-v24. It makes the first accepted
memory ID an immutable canonical envelope: later submissions may replay that
exact envelope but cannot reuse the ID for different content, author, domain,
or classification. It also introduces governed adoption of complete historical
projection envelopes that were absent from canonical state.

After activation, CEREBRUM automatically scans historical local records. A
complete hash-verified record is adopted in a bounded Root-bound,
validator-attested batch without changing its content, authorship, domain,
classification, or earlier blocks. A conflicting or incomplete row is
quarantined on its own so it cannot blank the brain or make healthy agent work
unavailable. Root may retry the exact unresolved snapshot or explicitly retire
it from automatic repair; neither action deletes its historical evidence.

For each recovered local domain, the earliest verified historical local writer
is the operational owner. Other verified local historical writers are enrolled
in the exact local Access Group and receive read/write continuity for that
domain. The recovery cutoff is immutable: a new app-v25 memory cannot expand
the historical writer set. If the earliest writer is unavailable, CEREBRUM
Root owns the recovered domain; a later writer is never silently promoted.
Federated identities remain linked readers and never enter this local group.
See [`app-v25-upgrade-recovery.md`](app-v25-upgrade-recovery.md) for the
operator-facing recovery contract.

## Mandatory release gates

The v11.16.0 release and app-v24 activation are blocked until all of the
following pass:

1. Fresh vendored Mynah performs zero-touch Root bootstrap, remains
   `waiting_for_app_v24` before the fork, rejects direct pre-v24 writes at
   consensus, and succeeds on its first memory write after app-v24.
2. Generic fresh self-registration remains restricted and cannot mint
   Manager/Admin or claim a domain.
3. Every role × profile × ownership × group × clearance decision is covered by
   deterministic unit and raw-transaction tests.
4. Missing grants and each capability deny return the correct non-retryable
   reason/remedy.
5. Historical replay, activation crash replay, state-sync export/import, and
   AppHash determinism pass.
6. A two-SAGE test proves exact remote Agent X can read the attached group while
   Y and the remote peer operator alone cannot.
7. The same two-node test proves linked-reader messaging is default-off; exact
   receiver consent allows only X-to-B, reciprocal peer consent allows only
   B-to-X, and Y, C, Root, peer operator, directory-only, and contact-only
   identities remain denied.
8. Every federated Write/Copy/Modify path remains denied for linked readers and
   linked messages carry no domain/contact authority.
9. Removal, group change, pause, revoke, re-pair, key rotation, domain
   transfer, consent CAS races, and in-flight send/admission/result races fail
   closed.
10. Sensitive exact `role == "admin"` checks are replaced by or proven to be
   intentionally inside the versioned authorization boundary.
11. Independent consensus, federation, and implementation/UX adversarial
    reviews complete with no unresolved high- or critical-severity findings.
12. App-v24 submission-hash binding, terminal hash preservation, governed
    historical re-anchor, replay, crash recovery, and state-sync tests pass
    across the strict H/H+1 boundary.
