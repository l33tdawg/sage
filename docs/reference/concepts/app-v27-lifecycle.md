<!-- Verified against SAGE v11.19.20/app-v27 code (2026-09-02). -->

# App-v27 record lifecycle and task canonicalization

App-v27 is a governed consensus upgrade from app-v26. It requires no state
migration. Activation height H retains app-v26 transaction semantics; app-v27
rules begin at H+1, and historical blocks continue to replay under their
original application versions.

App-v27 makes exactly two consensus changes.

## Static reserved shared-domain record authors

The immutable author principal of a record in `general`, `self`, `meta`, or a
`sage-*` domain may challenge that exact record and may reinstate its open
challenge without separately holding a level-3 Modify grant. This is
record-scoped lifecycle authority, not domain ownership or general Modify.

The exception does not apply to a domain made shared by governance. It also
does not override hard denials: the caller must remain an eligible active local
principal and must pass the applicable operating-profile/capability and
classification/clearance checks. An empty, unknown, or mismatched historical
author never creates authority.

When an app-v21 weighted challenge opens, an eligible record author is merged
deterministically into the frozen electorate. Later grant or membership churn
does not rewrite that round.

## Omitted new-task status

For a signed new-task submission executed under app-v27 rules, an omitted
`task_status` is canonically interpreted as `planned`. REST transaction
construction and consensus agent-proof verification derive the same action, so
the caller's signed request body remains valid.

Before app-v27, omission remains invalid and clients must explicitly send
`task_status: "planned"`. The version gate preserves historical proof behavior,
transaction interpretation, replay, and AppHash compatibility.
