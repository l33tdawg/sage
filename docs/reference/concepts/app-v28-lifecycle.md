<!-- Design contract for the app-v28 fork. NOT YET ACTIVATED: app-v27 remains the ceiling until the evidence below is in hand. -->

# App-v28 public-memory commitment

App-v28 is a governed consensus upgrade from app-v27. It carries a state
migration rather than only rule changes: the sparse public-memory Merkle index
over committed `PUBLIC=0` records becomes AppHash-covered at the activation
height.

Activation height H retains app-v27 transaction semantics. From H+1 the AppHash
is the composite of the legacy state root and the public-memory root.

## What the fork commits

The index is a sparse SHA-256 tree over the canonical form of every committed
`PUBLIC=0` record. It answers one question nothing else in SAGE can: is this
public record included in the committed public set, and has that set been
rewritten since.

`PublicMemoryCompositeHash` (`internal/store/public_memory_index.go:278`) is the
rule: SHA-256 over the domain tag `sage.app.public-memory.v1`, the legacy state
root, and the public-memory root. A node that has promoted the index computes a
different AppHash than a node that has not, for every block after H.

## Why this is a fork and not a feature flag

Promotion writes the root node into AppHash-covered state, and the composite rule
changes how every later AppHash is computed. Two nodes that disagree about
whether v28 is active disagree about every block after H. That is why the
ceiling, not the code, is the activation switch.

## What is already built, and inert

The store side is complete and pinned by tests, with no production caller.
`PreparePublicMemoryStage` (`internal/store/public_memory_stage.go:23`) builds the
index into a staging namespace that is excluded from all three AppHash modes, so
staging cannot move a hash — the stage test asserts that directly.
`PromotePublicMemoryStage` (`internal/store/public_memory_stage.go:115`) runs
inside the activation block's consensus transaction and refuses unless the staged
manifest names that exact height and the pre-activation AppHash.
`SyncPublicMemoryChanges` (`internal/store/public_memory_stage.go:192`) re-syncs
changed paths per block; `ValidatePublicMemoryStage`
(`internal/store/public_memory_stage.go:143`) refuses a node at boot whose
promoted root does not match its state. The promotion marker is literally
`public-memory:v28:promotion`.

## What the fork adds

1. stage the index before the activation block, at the recorded activation height
2. promote at H with the AppHash of H-1, inside the consensus transaction
3. sync changed public paths per block from H+1
4. select the composite AppHash rule from H+1, following the app-v13 pattern
5. validate the promoted stage at boot

Steps 1, 2, 3 and 5 are inert while no applied v28 record exists.

## The ceiling is the activation

The `maxSupportedAppVersion` ceiling in `internal/abci` is the version the
auto-voter will not vote past, and `currentAppVersion`
(`internal/abci/app.go:2608`) is the ladder that reports it. In personal mode
`personalAutoAdvanceCeiling` (`cmd/sage-gui/upgrade_watchdog.go:271`) returns
that ceiling, so bumping it is not a constant update: a personal node advances
its own chain to the new version on upgrade with no governance ceremony,
writing AppHash-covered state. The gate can ship dormant; the bump belongs in
the change that carries the evidence.

## Evidence required before the ceiling bump

- `make determinism`: byte-identical AppHash across a 4-node devnet crossing H
- both Consensus Fault Gates
- a promoted node surviving state-sync and restore (the app-v20 precedent)
- restart and replay equivalence across H

## Not in this fork

No change to clearance, RBAC, federation, messaging or task semantics; no new
transaction type; no chain reset. Historical blocks keep replaying under their
original application versions.

## Deliberately deferred

A second candidate was scoped for this fork: folding the co-commit tombstone
content-hash index into the AppHash. Today that index is a submission-boundary
check only, so a directly broadcast transaction is not covered by it. It is not
included here because it changes what a submission must carry and therefore the
contract of a different path, with its own evidence burden. A fork is scarce, so
this is recorded as a deliberate deferral rather than an oversight: if it is
wanted in v28 it has to be added before the ceiling bump, not after.

## What consumes the commitment

Nothing yet. No REST or MCP surface serves a public-memory proof, so v28 ships a
commitment whose proof surface arrives later as a non-consensus change — proofs
are derived from state, so serving them needs no fork. Committing before the
surface exists is deliberate: the commitment is what makes a later claim about
the public set verifiable, and a proof surface added without it would be
verifying against nothing.
