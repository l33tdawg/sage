# Block Production & the Idle Chain

> **Verified against:** `cmd/sage-gui/node.go` (`runServe`, `initCometBFTConfig`,
> `resolveRetainBlocks`), `internal/abci/app.go` (AppHash bookkeeping / fork gates),
> `internal/store/badger.go` (`appHashBookkeepingKeys`), `cmd/sage-gui/upgrade_pump.go`,
> `web/static/js/app.js` (dashboard idle display), `README.md` (version history).
> Behavior described is app-v13+ (SAGE v10.5.1 and later).

## TL;DR for operators

**SAGE has no heartbeat.** An idle SAGE chain mints **zero** blocks and its block
height does **not** advance. A frozen block height with an **empty mempool** is
**healthy, not stuck** — it is working exactly as designed. The dashboard shows this
state as **"Idle — waiting for activity"**, which is a green/normal state.

A block is minted only when there is something to record. If you want to confirm the
node is alive, submit any memory (or run any write) and watch the height advance
within ~2 seconds.

## When is a block minted?

SAGE runs CometBFT with **`create_empty_blocks = false`** (set in code at startup —
`cmd/sage-gui/node.go`, in the `config.DefaultConfig()` + overrides block; the on-disk
`config.toml` does **not** drive this). Consequences:

- **Idle** (mempool empty): no blocks are produced, indefinitely. Height stays put;
  the age of the last block grows without bound. This is normal.
- **A transaction mints a block.** Every memory submit, vote, agent registration,
  governance proposal, etc. enters the mempool and mints the next block.
- **One trailing "proof" block.** A transaction changes the app state hash, and
  CometBFT mints exactly one empty block right after a burst of activity to sign that
  new hash. That empty block leaves the hash unchanged, so production then stops.
  Net: a burst of writes ends with one extra empty block, then silence.
- **Minimum spacing between blocks** equals `timeout_commit`: **1s in personal mode,
  3s in quorum mode**.

> The dashboard's "~5 s to next block" countdown is a cosmetic UI value, not the real
> cadence. When the chain has been idle for 30 s the dashboard replaces the countdown
> with the **"Idle"** indicator.

## Why it used to tick every second (history)

Before **v10.5.0**, a personal node minted an empty block roughly every second
forever, even when idle, and its data directory grew by a couple hundred MB per day.
That was a bug (issue #40): the app hash changed on *every* block because per-block
bookkeeping keys (`state:height`, `state:app_hash`, `state:epoch`) were folded into
it, so CometBFT's proof-block logic never reached a fixed point and kept minting.

- **v10.5.0 (app-v12)** stopped empty-block production by excluding the whole `state:`
  key prefix from the app hash, so an idle chain's hash reaches a fixed point. This
  rule was over-broad — it also dropped governance/vote keys from hash coverage.
- **v10.5.1 (app-v13)** narrowed the exclusion to exactly the three bookkeeping keys
  above, restoring full hash coverage while keeping the idle-stop behavior.

Empty-block production stops at **app-v12**: a chain still **below app-v12** heartbeats
every second until upgraded. Take **v10.5.1+** (app-v13) to also get the corrected,
narrower hash rule. Run the upgrade ladder to climb.

## Healthy-idle vs. actually-stuck

Idle is healthy. A **stuck** chain is one that has transactions waiting but is not
producing blocks. Use this test (it mirrors the dashboard's own health logic):

> **Stuck** = mempool has pending transactions (**`n_txs > 0`**) **AND** the last block
> is older than ~30 s. Empty mempool + old block = **healthy idle**.

How to check:

- **Dashboard:** "Idle — waiting for activity", "(chain idle — not a stall)", and
  Block-rate "Idle" are all **normal** states. The dashboard only flags trouble
  (amber) when the node is catching up, the mempool is backing up, or the stuck
  condition above holds — never on block age alone.
- **CLI / curl:**
  - `curl -s localhost:26657/status | jq .result.sync_info` — `latest_block_height`,
    `latest_block_time`, `catching_up`.
  - `curl -s localhost:26657/num_unconfirmed_txs | jq .result.n_txs` — pending count.
  - `GET /v1/dashboard/health` → the `.chain` block (proxies the above).
- **Liveness probe:** submit any memory, then confirm the height advances within ~2 s
  (personal). If a pending mempool transaction does **not** mint a block, that is a
  real stall — investigate.

## Automated block producers

Some legitimate activity can make a "quiet" chain tick without operator writes:

- **Upgrade pending-plan pump** (`cmd/sage-gui/upgrade_pump.go`): while a consensus
  upgrade plan is pending below its activation height, the node heartbeats one block
  every ~5 s until it reaches that height.
- **Auto-advance / fork ladder:** similar, while the chain climbs app-version forks —
  it probes every ~2 s and heartbeats on stagnant ticks.

So during an upgrade you may see steady block production even with no user activity.
This is expected and self-limiting.

## Quorum vs. personal

Quorum (multi-validator) mode has the **same idle semantics** — `create_empty_blocks`
is `false` for both. Differences:

- `timeout_commit` is **3 s** (vs. 1 s personal), so active-cadence blocks are spaced
  ~3 s apart.
- All validators idle together; when the network is quiet, none of them produce
  blocks.
- **Monitoring:** an alert rule like "no new block in N minutes" will **false-positive**
  on a healthy idle SAGE network. Alert instead on **mempool-non-empty AND no new
  block** (the stuck condition above).

> **Scope note:** "no heartbeat" describes **sage-gui** nodes (personal and
> `quorum.enabled` mode). The `amid` docker-testnet validator binary runs with CometBFT
> defaults (`create_empty_blocks = true`), so a testnet built from it heartbeats
> continuously by design.

## Disk & block retention

Old blocks are pruned according to `retain_blocks` (`resolveRetainBlocks` in
`cmd/sage-gui/node.go`): personal mode keeps a rolling window (default ~100,000
blocks), quorum keeps everything unless an operator opts into a window, and `-1` keeps
everything. Pruning old blocks is **safe**: your memories live in SQLite/BadgerDB, not
in the historical block log, so a pruned block store never loses a memory.

## FAQ

**My block height hasn't moved in days — is the node dead?**
Almost certainly not. Check the mempool: empty mempool + old block = healthy idle. Do
a liveness probe (submit a memory; height should advance in ~2 s).

**Can I turn the heartbeat back on?**
No — there is no knob for it, and re-enabling empty blocks would reintroduce the
unbounded disk growth of issue #40. The `[consensus]` section of the on-disk
`config.toml` is **not** honored at runtime.

**Why did the height jump by a couple hundred during an upgrade?**
The pending-plan pump / auto-advance ladder produces heartbeat blocks while the chain
climbs to its target app-version. Expected.

**My old node still ticks every second — why?**
It hasn't climbed to **app-v12** yet (the idle fix). Run the upgrade ladder or install
v10.5.1+ (which also brings the corrected app-v13 hash rule).

**Why does the dashboard say "~5 s to next block" but blocks are ~1 s apart when
active?**
The countdown is a cosmetic UI value; the real minimum spacing is `timeout_commit`
(1 s personal / 3 s quorum). When idle for 30 s the dashboard shows "Idle" instead.
