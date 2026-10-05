<!-- Design contract for the app-v28 fork. ACTIVATED: the evidence below landed with the v11.23.0 ceiling bump, which is what turned the compiled gate into an automatic rung. -->

# App-v28 public-memory commitment

App-v28 is a governed consensus upgrade from app-v27. It carries two changes that
are both consensus-visible, and it was scoped as one fork deliberately rather
than as two: the sparse public-memory Merkle index over committed `PUBLIC=0`
records is committed by the composite AppHash rule from H+1, and the co-commit
tombstone rule stops being a submission-boundary check and becomes a rule the
consensus path enforces.

Activation height H retains app-v27 transaction semantics. From H+1 the AppHash
is the composite of the legacy state root and the public-memory root.

## What the fork commits

The index is a sparse SHA-256 tree over the canonical form of every committed
`PUBLIC=0` record. It answers one question nothing else in SAGE can: is this
public record included in the committed public set, and has that set been
rewritten since.

`PublicMemoryCompositeHash` (`internal/store/public_memory_index.go:306`) is the
rule: SHA-256 over the domain tag `sage.app.public-memory.v1`, the legacy state
root, and the public-memory root. A node that has promoted the index computes a
different AppHash than a node that has not, for every block after H.

## Why this is a fork and not a feature flag

Promotion writes the root node into AppHash-covered state, and the composite rule
changes how every later AppHash is computed. Two nodes that disagree about
whether v28 is active disagree about every block after H. That is why the
ceiling, not the code, is the activation switch.

## Staging and promotion

The store lifecycle is wired into consensus execution in
`internal/abci/appv28_lifecycle.go`: preparation at line 126, promotion at line
153, per-block synchronization at line 177, and boot validation at line 86.
It shipped dormant in v11.22.0 and became active with the v11.23.0 ceiling bump.
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
3. sync changed public paths per block from H
4. select the composite AppHash rule from H+1, following the app-v13 pattern
5. validate the promoted stage at boot

Preparation runs only for a scheduled app-v28 activation. Promotion runs at H;
synchronization starts in the activation block so its writes join the committed
set. The composite hash and new transaction rule apply from H+1. Boot validation
requires intact promoted indexes once an applied v28 record exists
(`prepareAppV28StagesForActivation`, `internal/abci/appv28_lifecycle.go:111`;
`promoteAppV28Indexes`, `internal/abci/appv28_lifecycle.go:142`;
`syncAppV28IndexesForBlock`, `internal/abci/appv28_lifecycle.go:173`).

## The co-commit tombstone rule rides in this fork

A co-commit does not consult the voter: block inclusion is decisive for it.
Before app-v28, its tombstone check existed only at the local REST submission
boundary, so a directly broadcast transaction bypassed that guard. From H+1,
consensus checks the content hash against the promoted reverse index before
applying a co-commit (`processCoCommitSubmit`, `internal/abci/app.go:6043`).

Closing that gap required an index over consensus state. State
carries `memory:<id>` as a content hash plus status, so the predicate "these
exact bytes already left `proposed` under a different id" has no reverse lookup:
answering it by scanning every memory is not a rule that can run per
transaction. The fork therefore adds:

6. a consensus reverse index from content hash to the ids that carry it,
   maintained on every write that sets `memory:<id>`
7. a deterministic backfill of that index for existing records, staged before
   H and promoted with the same height-and-previous-AppHash discipline as the
   public-memory index
8. the rule itself in the co-commit path from H+1, rejecting an envelope whose
   content hash matches a record that is not `proposed` under a different id —
   the local guard stays as a fast refusal, but it stops being the only one

The activation block keeps app-v27 semantics, so the rule is strictly a
post-fork rule: a pre-fork block replays exactly as it did before.

## The ceiling is the activation

The `maxSupportedAppVersion` ceiling in `internal/abci` is the version the
auto-voter will not vote past, and `currentAppVersion`
(`internal/abci/app.go:2608`) is the ladder that reports it. In personal mode
`personalAutoAdvanceCeiling` (`cmd/sage-gui/upgrade_watchdog.go:271`) returns
that ceiling, so bumping it is not a constant update: a personal node advances
its own chain to the new version on upgrade with no governance ceremony,
writing AppHash-covered state. The gate can ship dormant; the bump belongs in
the change that carries the evidence. That is how app-v28 shipped: the gate and
its applied-height refresh were compiled in v11.22.0 with the ceiling still at
27, and v11.23.0 raises it to 28 in the change that carries the evidence below,
so a personal node now advances itself across the seam exactly as it did for
every earlier rung.

## Evidence that landed with the ceiling bump

- `TestAppHashDeterminism_AppV28Activation`
  (`test/integration/apphash_determinism_appv28_test.go`), run through
  `deploy/scripts/run-determinism.sh`: a fresh four-validator devnet walks
  app-v2 → app-v28 one rung at a time, asserting byte-identical AppHash at
  H-1/H/H+1 of every seam. The final rung casts explicit validator votes only
  while the target sits above the auto-vote ceiling, so the same test covers the
  dormant build and this converged one.
- Both Consensus Fault Gates on the change that carries the bump.
- A promoted node surviving state-sync and restore: the real-process state-sync
  gate now drives its provider through app-v28
  (`deploy/scripts/run-v11.9-state-sync.sh`, `TARGET_APP_VERSION=28`) and
  requires the pristine receiver and the provider to report exact app-v28 state
  with converging AppHash after the snapshot transfer.
- Restart and replay equivalence across H: the same gate's receiver
  pre-publication SIGKILL and provider SIGKILL phases, plus the replay family
  that pins historical blocks to their original app versions.

## Not in this fork

No change to clearance, RBAC, federation, messaging or task semantics; no new
transaction type; no chain reset. Historical blocks keep replaying under their
original application versions.

## Two migrations, one activation

Both halves backfill derived state at the same height, so the fork carries two
migrations behind one gate. That is the deliberate consequence of deciding to
open app-v28 once instead of twice: the evidence burden below covers both, and a
finding in either one blocks the same activation. The alternative — shipping the
public-memory commitment now and the tombstone rule in a later fork — was
rejected by the owner on 2026-09-15 because it would cost a second activation
ceremony for a chain that must upgrade in place.

The implementation commits the promoted tombstone keys through the ordinary
state hash. The app-v28 composite hash excludes the public-memory index nodes
and local/staging keys; it retains the promoted `cocommit-tombstone:v1:` entries
and promotion marker in the legacy component. The public-memory half is then
committed through its sparse-tree root (`ComputePublicMemoryAppHash`,
`internal/store/public_memory_stage.go:239`).

## What consumes the commitment

Nothing yet. No REST or MCP surface serves a public-memory proof, so v28 ships a
commitment whose proof surface arrives later as a non-consensus change — proofs
are derived from state, so serving them needs no fork. Committing before the
surface exists is deliberate: the commitment is what makes a later claim about
the public set verifiable, and a proof surface added without it would be
verifying against nothing.
