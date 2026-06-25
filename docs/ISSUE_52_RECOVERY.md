# Recovering a personal chain stranded at the upgrade admin-gate (issue #52)

## Symptom

A **personal (single-validator) chain** keeps producing **idle empty blocks** and stops
climbing the app-version fork ladder. The node log shows auto-advance giving up:

```
auto-advance halted: this node's agent.key is not the on-chain chain-admin, and past
app-v9 it can no longer self-grant that role (issue #52). Recover with `sage-gui repair-chain` ...
```

## Cause

On a personal chain the node auto-advances up the app-version fork ladder by signing
upgrade proposals with the operator key (`~/.sage/agent.key`). From **app-v8** onward,
opening a governance proposal requires the proposer to hold the on-chain **`admin`** role.
The operator key's self-grant "open door" closes at **app-v9**, and `materializeAppV11Admin`
elects the *validator* key, not the agent key. So a chain that crossed app-v9 **without its
operator agent key being admin** can never open another proposal — auto-advance deadlocks,
and CometBFT keeps minting empty blocks because the app hash never reaches a fixed point.

## Are you affected?

| Chain | Status |
|-------|--------|
| **New personal chains** (created on this release or later) | **Protected automatically.** Genesis seeds the operator key as `admin` (`app_state.sage.initial_admin`), so the ladder climb never strands. |
| **Existing personal chains** created before this release | **At risk / possibly already stranded** past app-v9. Use the recovery below. |
| **Multi-validator / federated chains** | **Not affected by this path.** Admin/governance is established via coordinated governance, not a local seed. Do **not** use `repair-chain` on these — recover through governance. |

> A plain same-fork upgrade does **not** auto-recover an already-stranded chain: heal only
> runs after a reset wipes the block/state stores, which a patch upgrade never triggers.
> Use the explicit recovery command below.

## Recovery — `sage-gui repair-chain`

This rebuilds the local consensus state so the chain re-initialises from genesis with your
operator key seeded as chain-admin. **Your memories (SQLite) and vault key are backed up and
preserved** — only the block/index state is rebuilt (the same safe reset used on a normal
fork-transition upgrade).

```sh
# 1. Stop the SAGE node.
#    (kill the `sage-gui serve` process / stop the service)

# 2. Run the repair (single-validator personal chains only; refuses multi-validator chains).
sage-gui repair-chain          # prompts for confirmation
#   or non-interactively:
sage-gui repair-chain --yes

# 3. Start the node again.
sage-gui serve
```

On restart the node re-initialises from genesis with the operator key as `admin`, and
auto-advance resumes climbing to the current app version (which carries the idle
empty-block fix). Backups are written under `~/.sage/backups/`.

### What `repair-chain` does

1. Backs up the vault key and the SQLite memory database (`~/.sage/backups/`).
2. Rebuilds the chain index (BadgerDB) and the CometBFT consensus log
   (`blockstore.db`, `state.db`, `tx_index.db`, `evidence.db`, `cs.wal`), and resets
   `priv_validator_state.json` to height 0. **Config and `genesis.json` are kept.**
3. Injects the operator key into `genesis.json` as `app_state.sage.initial_admin`
   (a `genesis.json.bak` is written first), so the next `InitChain` registers it as `admin`.

It refuses to run on a multi-validator chain or an uninitialised data directory, and it
will not proceed while the node is still running (it can't acquire the chain-index lock).

### If `genesis.json` was lost or corrupted

`repair-chain` needs a valid `genesis.json` (it errors with *"no initialised chain"*
otherwise). If yours was deleted or corrupted:

- **Restore the backup.** Every heal writes `genesis.json.bak` next to `genesis.json`
  (and `quorum-join` does too). Copy it back, then run `repair-chain`.
- **Or regenerate it.** `initCometBFTConfig` recreates a single-validator genesis
  **only when `genesis.json` is absent**, and the new genesis is seeded with your
  operator key as admin. Move the corrupt file aside and start the node
  (`sage-gui serve`) — it regenerates a seeded genesis and InitChains from it.

> A regenerated genesis has a new `genesis_time`/hash, so this is only valid for a
> single-node personal chain (a quorum's peers must share one genesis).

### If `repair-chain` says the chain index is locked

*"the chain index is locked — the SAGE node is still running; stop it and retry"* means
`repair-chain` could not acquire the BadgerDB lock. The node is (almost certainly) still
running — stop it and retry. (An **unreadable/corrupt** index, e.g. after an unclean
crash, is **not** treated as locked: `repair-chain` logs a warning and proceeds, because
the reset rebuilds the index from your SQLite memories anyway.)

> When the index is corrupt, `repair-chain` cannot re-read the live validator set, so it
> assumes a **single-validator personal chain** — which the `repair-chain` command already
> requires (it refuses on a quorum-configured node). Do not run it against a node that has
> joined a real multi-validator quorum.
