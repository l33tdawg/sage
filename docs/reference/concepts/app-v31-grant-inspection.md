# App-v31 direct-grant inspection

App-v31 adds `AgentCapabilityInspectDirectGrants` (bit 5, value 32) for reading
another local agent's direct grant metadata. It does not grant memory access,
change existing grants, or authorize writes. Admin and read-all status do not
imply this capability. Current and historical Root credentials cannot use the
inspection endpoint or be its target.

## Activation and assignment

An existing chain activates app-v31 through the governed upgrade path after
app-v30. If activation commits at height H, the new transaction rules begin at
H+1. An explicitly selected fresh app-v31 genesis starts with those rules at
block 1. Compiled support extends through 31; the automatic upgrade ceiling and
`latest` fresh-genesis selection remain 29. See [fresh genesis](../fresh-genesis.md).

Activation and migration assign no inspection permissions. Enrollment defaults
are unchanged. A current Root or authorized current-generation Admin must
explicitly assign the bit through the existing atomic agent-policy operation,
with its existing signatures, revision checks, and elevation requirements.
The bit is optional on ordinary named profiles; their existing role, clearance,
and write restrictions still apply. Root and migration-only profiles cannot
hold it. The historical capability mask and app-v23 policy validator remain
strict for transactions before activation.

Access Controls shows **Inspect direct grants** only when app-v31 is active.
Selecting it submits the policy change to consensus; it is not a browser-only
permission. Changing the role or operating mode clears that selection.

## Root rotation and revocation

Inspection requires an active enrollment bound to the current Root generation.
After Root rotation, a recorded inspection bit from the previous generation
cannot authorize a read. This does not suspend an ordinary Member's other
permissions. Access Controls reports inspection renewal separately from Admin
reauthorization.

A current Root or authorized current-generation Admin can explicitly renew the
same capability through the atomic policy operation. That assignment records
the current Root generation. A stale Admin cannot renew it. Clearing the bit
revokes inspection; other permissions retain their existing policy semantics.

## Reading a snapshot

Use the signed [REST snapshot endpoint](../rest-api.md#get-v1accessgrants-snapshotagent_id)
or the synchronous/asynchronous Python SDK's
[`inspect_direct_grants()`](../python-sdk.md#inspect_direct_grants).
Authorization, target metadata, grant rows, committed height, and AppHash come
from one held canonical read transaction. The endpoint does not fall back to
the SQL grant mirror or an Admin credential.

A successful response is a complete bounded observation of unexpired direct
grants for one target at the stated evaluation time. It is not effective access:
ownership, groups, inherited authority, and other policy can affect what the
agent may do. Separate target requests are separate snapshots. Denials, malformed
state, and exceeded read limits do not become successful empty or partial results.
