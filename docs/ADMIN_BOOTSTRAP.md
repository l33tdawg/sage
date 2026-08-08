# CEREBRUM Root and agent approval

<!-- Reconciled through SAGE v11.18.2/app-v26. -->

This guide covers the current governed bootstrap and recovery workflow. The
pre-app-v23 self-promotion and per-field permission APIs are retired and must
not be used on a current node.

## Authority model

- **CEREBRUM Root** is the machine-local sovereign operator credential. It is
  not an agent, does not appear in agent rosters or Access Groups, and cannot be
  demoted through an agent API.
- **Admin** is a local ordinary-agent role with broad control over this node's
  agents and data. Promotion requires that the exact agent key is held on this
  machine. Admin is still distinct from Root.
- **Manager** receives Member access plus the configured write/modify authority
  inside its local Access Groups.
- **Member** owns and writes its home domain and receives group-derived access.
- Federated identities never become local Admins and never gain local write or
  modify authority merely by being linked for reading or messaging.

The full consensus rules are in
[`reference/concepts/rbac-orgs-federation.md`](reference/concepts/rbac-orgs-federation.md).

## First launch

1. Start the signed SAGE application normally.
2. Open CEREBRUM through `http://127.0.0.1:8080/ui/` on that same machine.
   Governance is intentionally loopback-only.
3. Connect each local client from CEREBRUM or configure its dedicated
   `SAGE_IDENTITY_PATH`. Each client must keep its own Ed25519 key; do not share
   `~/.sage/agent.key` between projects.
4. Let the client self-register. A fresh registration is pending review and
   cannot self-promote or mint authority.
5. In **Access Controls**, select the exact signer, choose role, operating mode,
   clearance, and a non-shared home domain, then approve the atomic policy.
6. Put local agents into Access Groups to share governed domain access. Group
   authority is explicit (`read`, `read_write`, or `read_write_modify`) and removing an
   agent removes only group-derived access; its own home domain remains intact.

The agent can inspect its own standing and bounded domain samples with
`sage_inception`, `sage_status`, and `sage_domains`. It does not need a global
roster or the Root key.

## Companion applications

A co-located application such as Mynah must generate its dedicated agent key
*before* vendored genesis starts, place the canonical key where SAGE can
discover it (normally below `~/.sage/agents/`), and consistently point its MCP
and hooks at that same identity. CEREBRUM then approves the Companion profile
and owned home domain. A Companion is an ordinary Member profile, never Root.

## Root handover

Root handover is available only in loopback CEREBRUM. It changes the current
credential controlling Root-owned domains and future governance; it does not
rewrite memory authorship or chain history. Existing memories and domains stay
readable under the new current authority.

Do not delete or discard the current Root credential. Complete the governed
handover first and verify the new credential. CEREBRUM presents the destructive
confirmation because losing the only current Root key is not repairable by an
ordinary agent.

## Current programmatic surfaces

The supported agent-facing surfaces are:

- `POST /v1/agent/register` for self-registration;
- signed `GET /v1/agent/me` for the caller's own standing;
- signed `GET /v1/agent/me/domains` and
  `GET /v1/agent/me/domains/owned` for bounded caller-scoped domain discovery;
- the canonical MCP tools documented in
  [`reference/mcp-tools.md`](reference/mcp-tools.md).

Role, profile, clearance, capabilities, home-domain approval, Access Groups,
and Root handover are governed through loopback CEREBRUM. There is no current
SDK shortcut for per-agent permission mutation. The old permission endpoint
returns HTTP 410 on governed nodes and is intentionally absent from the current
SDK and OpenAPI surface.

## Troubleshooting

- **Pending/review agent:** open CEREBRUM locally and approve that exact signer.
  Do not substitute Root's key into the client.
- **Can write but cannot read:** verify that the client uses the same key for
  hooks and MCP, then inspect `sage_status` and its home-domain policy.
- **Missing home domain after an upgrade:** app-v26 performs the deterministic
  historical repair during activation. A post-app-v26 invalid state is rejected
  rather than silently assigning a different agent's data.
- **Federated contact absent:** local and federated groups are separate. Confirm
  the active agreement plus an explicit shared-domain or linked-message edge;
  directory membership is authorization metadata, not online presence.

For precise REST behavior and error codes, use
[`reference/rest-api.md`](reference/rest-api.md). That reference supersedes old
examples elsewhere in the repository when they disagree.
