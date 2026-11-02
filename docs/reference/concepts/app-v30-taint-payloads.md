# App-v30 taint payload categories

This describes the dormant app-v30 payload contract. It does not enable
app-v30 or change the automatic upgrade ceiling.

`reason_category` is an **opaque, author-selected byte**, not a SAGE enum.
Every value from 0 through 255 is valid for apply and clear. SAGE assigns no
global label, priority, authority or enforcement behavior to any value;
consumers must not infer those meanings from the byte. A deployment may
document its own display labels without changing the consensus contract.
The encoders preserve the byte without enum validation:
`EncodeTaintApplyPayload` (`internal/tx/taint.go:76`) and
`EncodeTaintClearPayload` (`internal/tx/taint.go:228`).

The exact byte is part of the canonical payload. Changing it changes the
apply decision ID or clear target ID, so it must not be normalized or
discarded. It does not change the identity of the apply decision referenced
by a clear. See `TaintApplyDecisionID` (`internal/tx/taint.go:260`) and
`TaintClearTargetID` (`internal/tx/taint.go:269`).

Version, action and scope kind remain defined discriminants: unsupported
values are rejected. The contract's rejection of unknown enum values
applies to those fields, not to `reason_category`. Structural validation,
size limits and canonical encoding checks still apply. See
`validateTaintApplyPayload` (`internal/tx/taint.go:276`),
`validateTaintClearPayload` (`internal/tx/taint.go:343`) and
`DecodeTaintApplyPayload` (`internal/tx/taint.go:110`).
