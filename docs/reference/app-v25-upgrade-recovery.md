<!-- Reconciled through SAGE v11.18.2: internal/abci/app.go (app-v25 gate and immutable envelope), web/appv25_memory_legacy_adoption_worker.go, web/appv25_domain_continuity_worker.go, web/appv25_memory_legacy_adoption_control.go, and internal/store/appv25_domain_continuity.go. -->

# App-v25 Upgrade, Historical Recovery, and Domain Continuity

App-v25 is SAGE v11.16.2's governed recovery upgrade. It fixes historical
projection defects without relabeling memories, changing their content, or
rewriting chain history.

## Activation boundary

App-v25 requires app-v24 as its immediate predecessor. The activation block
uses app-v24 semantics; app-v25 rules begin strictly at the following block
(H+1). A node never votes for an application version it cannot execute, and
app-v25 remains the recovery fork; the v11.17 binary's current supported
ceiling is app version 26.

This is a normal governed chain upgrade. Do not stop automatic advancement of
the mandatory upgrade ladder: validators must run the exact release binary and
let the installed governed upgrade complete together. Diverging a node around
this boundary is not a recovery strategy.

## What changes for new memories

From app-v25 onward, the first accepted submission for a memory ID establishes
one immutable canonical envelope:

- content hash;
- submitting author;
- domain;
- classification; and
- lifecycle status.

An exact replay is idempotent. A second submission with the same ID but a
different envelope is rejected before either canonical or serving state can be
mutated. This prevents the historical class of SQL-upsert/canonical-state
mismatch that caused write/read asymmetry.

## Automatic historical repair

After activation, CEREBRUM scans the local serving projection against the
canonical record inventory in the background. It only adopts a historical row
when the evidence is complete and exact: the SQL record must match the
canonical envelope and its content must hash to the recorded canonical hash.

Eligible rows are repaired in bounded, deterministic batches. Each batch is
bound to the current Root credential generation, re-attested by validators,
and committed through governance. The repair preserves the memory ID, content,
content hash, original author attribution, domain, classification, status, and
earlier blocks. It is idempotent: retrying a completed batch does not create a
second memory or rewrite history.

The worker can make ordinary records visible again while the rest of the scan
continues. This is intentional: a localized legacy problem is not a reason to
blank CEREBRUM, reject healthy writes, or make all agents appear disconnected.

## Local domain continuity

The same verified historical inventory restores local agent authority for
domains that predate the app-v23 enrollment model.

- The earliest verified historical local writer is the recovered operational
  domain owner.
- Every additional verified historical local writer is placed in the same
  dedicated local Access Group and receives read/write continuity for that
  exact recovered domain.
- The recovered writer set is frozen at the upgrade cutoff. New app-v25
  submissions cannot enlarge it.
- Memories already in the terminal `deprecated` state are preserved for audit
  but excluded from continuity evidence. Deprecated-only history can never
  recreate live ownership, a writer grant, or an Access Group.
- If the earliest writer is missing, retired, or cannot safely be activated as
  a local principal, CEREBRUM Root owns the recovered domain. A later writer is
  never silently promoted in its place.
- Root and an eligible local Admin retain their normal governing authority;
  federated agents remain linked readers and never become local group members
  or remote writers.

This restores operational access only. It never changes the immutable author
record on a historical memory.

Governance `executed` status is not treated as the success receipt by itself.
The worker verifies the exact canonical continuity record and every recovered
writer grant after execution. If an older app-v25 batch left that result
missing or revision-stale, it releases the stale proposal pointer and retries
the same frozen evidence through governance; it neither drops the domain nor
expands the writer set. Explicit policy changes remain conflicts and fail
closed instead of being overwritten by recovery.

## When a historical row cannot be repaired

A row is preserved but excluded when, for example, its canonical content hash
is absent or incorrect, its record-local envelope conflicts with canonical
state, required identity/domain/classification evidence is absent, or its
content cannot be decrypted. One excluded row is quarantined on its own; it
does not invalidate the verified remainder of the local projection.

Broad CEREBRUM views display verified records and a partial-projection state.
They do not call the brain empty. Exact/detail and export operations remain
strict for any record they would need to disclose.

After an audit has completed and localized the unsafe rows, `/ready` reports
HTTP `200` with `status: "degraded"`. It remains unavailable when the audit has
not completed, canonical storage is unavailable, or another actual serving
dependency is down. `degraded` therefore means normal work can continue on the
verified set; it is not a claim that every historical row is usable.

## Root resolution controls

Only the current local CEREBRUM Root may control unresolved historical rows.
The browser-only, loopback-protected recovery control exposes aggregate
progress, not the unresolved records' content or identities.

| Action | Route | Result |
|---|---|---|
| Inspect aggregate progress | `GET /v1/dashboard/memory/adoption-progress` | Current scan state and aggregate discovered/converted/remaining/recovery counts. |
| Retry scan | `POST /v1/dashboard/memory/adoption-retry` | Requests a new evidence scan for the exact current unresolved snapshot. It does not erase records, receipts, or previous decisions. |
| Explicitly deprecate unresolved snapshot | `POST /v1/dashboard/memory/adoption-deprecate` | Requires the exact snapshot revision/count plus typed `DEPRECATE <count>` confirmation. Preserves rows for audit and prevents automatic repair from retrying those exact IDs. |

These controls are intentionally not ordinary REST or MCP agent operations.
They are Root-only CEREBRUM recovery actions on localhost. An SDK client or
agent must not fabricate a replacement envelope, re-submit an unresolved
memory ID, or treat deprecation as deletion.

## What App-v25 does not do

- It does not delete memories or rewrite historical blocks.
- It does not turn a remote/federated agent into a local writer.
- It does not give a newly discovered agent access merely because it shares a
  display name with a historical author.
- It does not make a broad query's zero result proof that the global store is
  empty; authorization and verified-projection scope still apply.
- It does not expose Root recovery controls through a remote CEREBRUM session.

For generic roles, profiles, Access Groups, Root handover, and federation
boundaries, see [`app-v23-access-control-design.md`](app-v23-access-control-design.md).
