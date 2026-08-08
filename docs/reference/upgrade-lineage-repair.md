<!-- Verified against SAGE v11.18.2 lineage implementation (2026-08-08). -->

# Legacy upgrade-lineage repair (app-v21 → app-v22)

App-v22 requires complete, ordered predecessor coverage from `app-v6` through
`app-v21`. Normally that coverage is canonical persisted activation records.
Some old production chains skipped compatibility rungs in one real app-version
jump or activated them before per-rung persistence existed. The repair lane is
a narrow virtual-evidence ceremony for that case; it is not a state editor.

## Safety contract

- `sage-gui upgrade lineage status --json` and `doctor --json` are read-only.
- A repair is accepted only while the chain is exactly app-v21 and only inside
  an ordinary `app-v22` upgrade proposal.
- The v2 manifest binds the chain ID-derived governance domain, the exact
  current valid-lineage digest, and exact coverage of the missing-rung set.
- Skipped rungs are virtual compatibility evidence in an immutable audit
  receipt. Repair never writes `upgrade:applied:*` for a skipped rung and never
  pretends it activated independently. Present records are never overwritten.
- A retained transition names a real `from_version -> to_version` app-version
  jump, its actual committed height and block hash, and the exact ordered
  missing versions in that open interval. Its target must be a real canonical
  applied record at the same height.
- Direct retained-history claims and legacy anchors are also virtual. Anchor
  claims cannot be mixed with retained-Comet claims or transitions.
- Lineage-repair proposals never auto-vote. Proposal creation records no
  implicit proposer vote, the validator background voter abstains, and this is
  also true on a one-validator chain. Every validator operator must explicitly
  review and vote.

## Retained-Comet workflow

First take and verify the normal operator backup of consensus and projection
state. Do not edit Badger keys, copy another validator's database, or use a
repair manifest as a substitute for a backup.

On the proposing validator, create the candidate manifest:

```text
sage-gui upgrade lineage doctor --json --manifest-out repair.json
```

Copy only that manifest to each validator operator. On every validator,
independently run:

```text
sage-gui upgrade lineage verify --json --manifest repair.json
```

`doctor` scans every retained `block_results` entry from height 1 through the
committed tip and reconstructs the actual app-version sequence. For example,
an observed `app-v1 -> app-v7` update at height 376 with a real app-v7 record at
376 can cover missing app-v6; a later `app-v8 -> app-v11` update at height 992
can cover missing app-v9 and app-v10. The ordinary `app-v7 -> app-v8`
transition is still part of reconstruction even though it creates no manifest
claim. `doctor` never manufactures per-rung heights such as 375, 740, or 991.

A block hash in a manifest is not consensus-verified proof by itself; it is a
claim about one validator's local archive. `verify` independently replays the
full local sequence and requires byte-for-byte equivalent transition facts:
source, target, height, hash, and exact subsumed set. A changed intermediate
update therefore rejects the manifest even when both claimed endpoint blocks
still exist. Operators compare the emitted `manifest_digest` across validators
before proposing:

```text
sage-gui upgrade propose --target 22 --lineage-repair repair.json
```

After proposal creation, each validator inspects the immutable payload using
`sage_gov_status` (or CEREBRUM Governance) and explicitly calls
`sage_gov_vote` with `accept`, `reject`, or `abstain`. The REST equivalent is
the validator-local authenticated `POST /v1/governance/vote`. An operator must
not accept merely because another validator accepted.

After quorum, monitor `sage-gui upgrade status` until app-v22 activates, then
run `sage-gui upgrade lineage status --json` and confirm the immutable repair
audit and completed ladder on every validator before proceeding to app-v23.

## Pruned-history anchor fallback

If any required retained block result is unavailable, `doctor` will not blend
the surviving Comet claims with an anchor. Independent historical activations
remain in `heights`; a real jump whose intermediate rungs were never independent
activations belongs in `transitions`:

```json
{"heights":{"9":123},"transitions":[{"from_version":9,"to_version":12,"applied_height":140,"subsumed_versions":[10,11]}]}
```

An independently anchored height may be the next transition source. A validated
virtual transition target may also feed a later jump, but only at a strictly
earlier height:

```json
{"transitions":[{"from_version":1,"to_version":11,"applied_height":1000,"subsumed_versions":[6,7,8,9,10]},{"from_version":11,"to_version":15,"applied_height":1005,"subsumed_versions":[12,13,14]}]}
```

Subsumed compatibility rungs are not activations and cannot be transition
sources. Equal/reversed heights, an unproven source, overlapping direct and
transition coverage, overlapping transitions, or out-of-order bundles fail.

Generating an anchor manifest requires both `--legacy-anchor FILE` and the
deliberate `--acknowledge-unverified-anchor` flag. The manifest records
`operator-quorum-attested-unverified-history`; `verify` also requires the same
acknowledgement. These heights are audited operator assertions, not facts
recovered from retained history. An ACCEPT vote explicitly attests the exact
claims shown by `verify --json`.

An anchor transition has no block hash. It attests the actual source, target,
and height once, and gives the exact missing open-interval predecessors. If the
target itself lacks a canonical record, app-v6 through app-v19 may receive
virtual transition-target provenance at that same height; the target is not
listed in `subsumed_versions`. App-v20 and app-v21 can only be transition targets
when their real ceremony records already exist at that exact height. The anchor
digest commits the complete heights-and-transitions document, so editing any
field after review invalidates verification.

## Coordinated rollout and legacy-v1 detection

All validators must halt before any v2 repair proposal, install the identical
v11.18.1 binary, and confirm `upgrade lineage status --json` reports schema v2
and the same chain, app version, governance domain, missing set, and lineage
digest. Only then may one operator generate a manifest. Every validator must
verify that exact file and digest before anybody proposes or votes. A mixed
11.17.x/v11.18.1 validator set must not vote on or execute a v2 repair.

Before proposing, inspect both `upgrade lineage status --json` and the active
governance/pending upgrade plan. If `repair_audit.schema` is v1 and the chain is
already app-v22 or later, the v1 receipt is historical: do not generate a v2
repair; retain the receipt, roll all validators to v11.18.1 together, and confirm
the rungs are reported with `legacy-v1` provenance. If the chain is still
app-v21 and an approved/pending app-v22 payload contains a v1 repair, recovery
is finish-in-place: halt every validator before its activation height, preserve
full backups, and deploy v11.18.1 everywhere. On every node confirm `upgrade
lineage status --json` accepts any v1 receipt as `legacy-v1` compatibility and
`upgrade status` reports the identical bound app-v22 plan and activation height.
Only when all validators agree may they restart together and let that exact v1
plan activate through compatibility. If any audit, plan, activation height, or
record differs, remain stopped and do not resume or vote. There is no invented
cancel/migration command or Badger edit path. A v1 doctor or new v1 proposal is
never supported. The immutable v1 audit binds the retained governance proposal
and its approved payload; it does not derive that binding from or trust the
pending plan's `ProposerID` field.

## Persistence and recovery

Quorum execution stores the canonical manifest, its SHA-256 digest, proposal
ID, approval height, direct virtual evidence, and retained transitions in an
immutable AppHash-covered audit receipt. It does not materialize subsumed
versions as canonical applied-upgrade records. Boot and state-sync inspection
verify the audit, transition order/content, real transition targets, exact
virtual coverage, and canonical records together. A mismatch fails closed.

Implementation: `internal/abci/appv22_lineage_repair.go`,
`internal/store/upgrade_lineage.go`, `cmd/sage-gui/upgrade_lineage.go`, and the
app-v22 upgrade proposal path in `internal/abci/app.go`.
