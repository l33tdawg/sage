<!-- Verified against SAGE v11.17.10/app-v26 code (2026-08-05). Cite file:line when behavior is non-obvious. -->

# App-v26 Access Groups and member authority

This is the authoritative contract for local Access Groups after governed
app-v26 activation. Access Groups are consensus RBAC objects. They are not
browser-only collections, organization/department membership, federation sync
groups, or a way to turn a remote agent into a local principal.

## The persisted object

Each group stores:

- a canonical `group_id` and human-readable `name`;
- a sorted, unique list of local agent IDs in `members`;
- one `member_authority` tier that applies to every local member relationship;
- a monotonically increasing `revision`; and
- the committing actor and height.

The three and only three authority values are:

| `member_authority` | Access to domains owned by another member |
|---|---|
| `read` | Read |
| `read_write` | Read and write |
| `read_write_modify` | Read, write, and domain-scoped modify |

The persisted shape and accepted strings are defined in
`internal/store/appv23_local_rbac.go:95-119`. The authorization evaluator maps
them to the three domain verbs in
`internal/store/appv23_local_rbac.go:4745-4768`.

`member_authority` is a group property, not a role inference. A Manager in a
`read` group receives only group-derived Read; a Member in a `read_write_modify`
group receives all three group-derived verbs. Multiple shared groups are
additive and the strongest applicable tier wins deterministically
(`internal/store/appv23_local_rbac.go:4702-4730`). Admin and current CEREBRUM
Root retain their separate global authority.

## Scope is dynamic ownership, not copied grants

For local agents A and B in the same group, A receives the group's tier over
the current non-shared domain tree owned by B, and B receives the same tier over
the current non-shared domain tree owned by A. SAGE resolves current ownership
at authorization time; it does not materialize pairwise `AccessGrant` rows.

Membership does not transfer domain ownership and does not rewrite memory
authorship. The canonical owner always keeps policy-limited Read, Write, and
Modify over its own domain tree, even when the group's tier is only `read`
(`internal/store/appv23_local_rbac.go:4649-4701`; exhaustive owner/member tests
in `internal/store/appv26_access_group_authority_test.go:33-83`). A later domain
ownership transfer changes the group's derived scope in consensus order.

Removing an agent or deleting the group removes only the derived relationship.
The removed agent keeps its own domains and any authority independently supplied
by ownership or an explicit compatible grant
(`internal/store/appv26_access_group_authority_test.go:85-137`).

Shared domains are deliberately outside ordinary group ownership. The local
group evaluator returns no group-derived authority for a shared resource;
recovered historical continuity and explicit grants are evaluated separately
(`internal/store/appv23_local_rbac.go:4659-4681`).

## Intersections and hard denies

An Access Group is one positive source of domain scope. It does not override:

- active local enrollment and compatible role/profile state;
- the caller's classification clearance;
- the Read-only profile's mutation deny;
- `DenySharedDomainWrite` on shared resources; or
- `DenyForeignDomainWrite` when mutating another owner's domain.

Those checks run before group-derived authority in the central evaluator
(`internal/store/appv23_local_rbac.go:4591-4697`). An explicit hard deny remains
a deny even when the group tier otherwise includes the verb.

## Who can be a member

Every member must be a canonical agent ID with active local approval. Root's
stable principal, the current Root credential, and every historical Root
credential are excluded. A delegated Admin approved by an obsolete Root
generation is suspended and cannot remain eligible for a current app-v26 group
mutation (`web/appv23_access_handler.go:1128-1162`;
`internal/store/appv23_local_rbac.go:4336-4363`).

Federated agents are never local group members. A federated linked reader is a
separate, chain-qualified, node-local relation that can supply bounded exact
Read only. It cannot acquire local Write, Modify, ownership, claims, grants, or
transitive access through a local group. See
[`../federation-and-brain-api.md`](../federation-and-brain-api.md) for that
separate contract.

## Mutation and concurrency contract

Creating, replacing, and deleting a group are current Root/Admin consensus
actions (`internal/abci/appv23_local_rbac.go:852-909`). The browser control
plane accepts:

```json
{
  "name": "Research",
  "members": ["<agent-id-a>", "<agent-id-b>"],
  "member_authority": "read_write",
  "expected_revision": 0
}
```

at `PUT /v1/dashboard/network/access/groups/{groupID}`. Update calls pass the
revision currently shown by the consensus projection. Delete accepts only
`{"expected_revision": <current revision>}` at the same path with `DELETE`
(`web/appv23_access_handler.go:289-298`, `1179-1299`). A missing group can be
created only with revision `0`; every replacement or deletion must match the
current revision, otherwise consensus returns a revision conflict. The members,
tier, and next revision are written atomically
(`internal/store/appv23_local_rbac.go:4292-4308`, `4380-4399`).

These are loopback CEREBRUM operator routes, not ordinary agent SDK or MCP
methods. Agents consume the resulting policy through normal memory operations;
they do not create groups by calling a superseded per-agent permission tool.
The current Python SDK and MCP tool registry intentionally expose no Access
Group mutation shortcut.

## Upgrade and replay boundary

App-v26 is a strict successor to app-v25. At activation height H, every legacy
group without an explicit tier is migrated deterministically to least-privilege
`read` without changing its existing revision or update height. The activation
also repairs eligible local home-domain state and moves orphaned current domain
ownership to stable CEREBRUM Root before validating the resulting image
(`internal/abci/app.go:4294-4324`;
`internal/store/appv23_local_rbac.go:4405-4481`).

The extended `member_authority` wire field is accepted only for transactions
that execute under app-v26 rules at H+1. Before that boundary, the appended
field is rejected so app-v25 and app-v26 validators cannot interpret the same
bytes differently (`internal/abci/app.go:3519-3539`). After activation, missing
or unknown authority fails closed at transaction admission, boot, and state-sync
validation; it never silently falls back to the old role-derived behavior
(`internal/abci/appv26_access_groups.go:5-10`, `76-80`;
`internal/store/appv23_local_rbac.go:4468-4481`).

## Operator checklist

1. Put only same-node, actively approved agents in a local Access Group.
2. Select the least authority the collaboration needs: `read`,
   `read_write`, or `read_write_modify`.
3. Treat the displayed revision as a compare-and-swap token; refresh after a
   conflict rather than resubmitting stale membership.
4. Remove an agent from a group to revoke only the shared relationship. Do not
   transfer or deprecate its own domains unless that is a separate intended
   governance action.
5. Configure federated linked readers and linked messaging separately; neither
   is local group membership.
