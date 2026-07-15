# Local CometBFT v0.38.23 hardening

This directory is a source-compatible, minimal copy of
[`github.com/cometbft/cometbft`](https://github.com/cometbft/cometbft) at
`v0.38.23`. It contains the packages reachable from SAGE's module graph, plus
the upstream `go.mod`, `go.sum`, `LICENSE`, and `NOTICE` files.

Provenance:

- upstream version: `v0.38.23`
- exact upstream source commit: `feb2aea4dc271d612129afc958cb844713ec792b`
- standalone runtime provenance: `0.38.22+feb2aea4dc271d612129afc958cb844713ec792b`;
  the pinned v0.38.23 source still declares `TMCoreSemVer` as `0.38.22`, and
  the Docker build stamps that exact source commit into `TMGitCommitHash`
- module checksum: `h1:jtCe5Do4EcHVf/FCyZMvkBm+AmsRGGyvswImXYAabdM=`
- module-file checksum: `h1:jtH//cs5e2U5dNiaYIPMBwWXZsedXJfIie77gVhuRaA=`
- source subset baseline: copied from the Go module cache via the package list
  produced by `go mod vendor`; the six production overlays below are the only
  intentional deviations from that v0.38.23 baseline
- license: Apache-2.0; the upstream `LICENSE` and `NOTICE` are retained here

SAGE carries six coordinated state-sync production overlays:

| File | Narrow deviation and reason |
| --- | --- |
| `statesync/reactor.go` | An oversized `SnapshotsResponse` checks `Reactor.syncer` under `Reactor.mtx` before rejecting through it. Upstream can dereference a nil syncer when the advertisement arrives while state sync is inactive. |
| `blocksync/reactor.go` | A SAGE boot-runtime seal-abort sentinel stops block sync gracefully while the node is already shutting down. Every unrelated application error retains upstream's panic behavior. |
| `node/node.go` | Reactor construction reads the effective state-sync-height bridge while the block store is empty and deliberately retains it across consecutive empty-blockstore restarts. Once a block is materialized the bridge is ignored, so stale evidence cannot override the real block-store height. |
| `node/setup.go` | The successful state-sync path persists the verified bootstrap commit before publishing positive-height `StateStore` state, switches successfully to block sync, and only then persists the completion marker consumed by SAGE's live seal. |
| `state/store.go` | `StateStore.Bootstrap` writes the effective state-sync height in the same synchronous batch as the bootstrapped state. A crash can therefore never expose positive state without the height bridge needed while the block store is still empty. |
| `store/store.go` | `BlockStore.SaveSeenCommit` uses `SetSync`; the post-switch height/AppHash marker is fixed-format and synchronous; and an exact lone seen-commit residue can be removed when startup has independently proved the state DB empty. Startup inspects the raw DB before constructing `BlockStore`, so malformed metadata is preserved for rejection instead of triggering the upstream constructor panic. |

The standalone validator image in `deploy/Dockerfile.node` copies all six
files over a fresh exact-v0.38.23 checkout, so split Docker topology tests and
the root module execute the same six Comet state-sync hardenings. Test-only
files are additive and do not replace upstream production sources.

Regression coverage is split accordingly:

- `statesync/reactor_sage_test.go` exercises the inactive-syncer oversized-
  advertisement path under the race detector;
- `blocksync/reactor_sage_test.go` proves only the direct or wrapped SAGE
  seal-abort sentinel selects the graceful reactor exit and that an empty block
  store starts catch-up from the effective synchronized height;
- `node/node_sage_test.go` proves the height bridge survives two consecutive
  empty-blockstore restarts and is ignored, but not destroyed, once a block is
  materialized;
- `node/setup_sage_test.go` proves `commit -> state -> switch -> marker`, with
  no later step after commit/state/switch failure;
- `state/store_sage_test.go` proves the bootstrapped state and effective-height
  marker are published by the same synchronous bootstrap operation;
- `store/store_sage_test.go` proves completion-marker encoding and exact-match
  commit-only crash-residue recovery, including preservation of malformed raw
  `blockStore` metadata without a constructor panic; and
- SAGE runtime tests prove block execution arriving after the switch waits
  behind the seal without holding the bundle lease, then resumes only after
  the runtime observes matching positive-height state, a valid commit, and the
  exact durable marker; the same tests prove `Query` and `CheckTx` remain
  fail-fast throughout every unsealed phase.

Run the local dependency gate with:

```sh
make test-cometbft-patch
```

When updating CometBFT, regenerate the subset from the exact replacement
version, retain its license files, compare each of the six deviations against
the new upstream behavior, reapply only still-required overlays, update the
tag, exact source commit, checksums, and runtime-provenance note above, copy the
same overlay set in `deploy/Dockerfile.node`, and run
both the local dependency gate and SAGE's full Go suite. Do not silently drop
the nil guard, seal-abort sentinel, effective-height bridge, atomic state/marker
bootstrap, commit ordering, synchronous flush, completion marker, or raw exact-
residue recovery merely because another overlay has been upstreamed independently.
