# Voter Operations — Guaranteeing proposed → committed

> **Verified against:** `internal/voter/` (`voter.go`, `decision.go`, `key.go`),
> `internal/abci/app.go` (`processMemorySubmit`, `processMemoryVote`,
> `checkAndApplyQuorum`), `internal/validator/quorum.go`, `cmd/sage-gui/node.go`,
> `cmd/amid/main.go`. Behavior described is v11.1.0+.

A submitted memory is written `proposed` and only becomes `committed` when validator
votes reach quorum. The **auto-voter** — one goroutine per node, signing with the
node's own consensus key — is what casts those votes. If no voter runs, memories sit
at `proposed` forever. This doc is how you make auto-commit *operable*: the voter
provably runs (or the process exits), and a stuck backlog trips an alarm.

## 1. The pipeline

```
REST/MCP submit → processMemorySubmit (status='proposed')
   → per-node auto-voter casts a MemoryVote tx
   → processMemoryVote → checkAndApplyQuorum   ← the ONLY commit trigger
   → quorum reached: status='committed'
```

Co-commits are the one documented exception — they commit at submit (block inclusion
is the consensus), bypassing the voter.

## 2. Which process votes, per mode

| Mode | Voter | Key source |
|---|---|---|
| `sage-gui serve` | in-node goroutine, gated by `voter.*` config | `priv_validator_key.json` under the CometBFT home (auto) |
| `amid` in-process | on by default | `--home`'s validator key |
| `amid` socket | runs **only** if a key is provided | `--validator-key-file` / `VALIDATOR_KEY_FILE` |

`amid` socket mode is the silent-voterless trap: without the key flag the daemon runs
with **no** voter. Set **`--require-voter`** (or `VOTER_REQUIRED=true`) so a missing
key is a boot failure, not a warning you'll miss.

## 3. Configuration

**sage-gui** (`config.yaml`, or `SAGE_VOTER_*` env):

```yaml
voter:
  enabled: true        # SAGE_VOTER_ENABLED — false = no auto-voter (explicit choice)
  poll_interval: "2s"  # SAGE_VOTER_POLL_INTERVAL
  required: false       # SAGE_VOTER_REQUIRED — true = refuse to boot without a usable key
```

`voter.required: true` with `voter.enabled: false` is a config error. Boolean env
values accept `true/false/yes/no/on/off`; an unrecognized value **warns** rather than
silently disarming the safety flag.

**amid:** `--require-voter` flag / `VOTER_REQUIRED` env (default false).

> **`required` is a footgun on unrepaired chains.** It only checks that the consensus
> key file loads — not that the key is in the on-chain validator set. On a legacy
> chain whose key was never a validator (the retired 4-archetype era), `required=true`
> won't brick boot but the votes still won't count. `sage-gui` auto-repairs the
> single-node case (`ReconcileSelfValidator`); **`amid` does not** — confirm the key is
> in the validator set before arming `--require-voter` on a legacy amid cluster.

## 4. Key safety — exactly one voter per key

`priv_validator_key.json` is the **block-signing** key. This is why there is **no
standalone voter daemon**: exporting the key to a second process/host is a CometBFT
double-sign/equivocation hazard.

- Mount it **read-only**, mode `0640` with a shared group, on the **same host**.
- Never copy it to a second live process. Two processes signing with one key race on
  the monotonic nonce (app-v9 rejects `nonce <= last-committed`) and lose votes.
- On restart the nonce floor re-seeds from chain state (wired in both `sage-gui` and,
  as of v11.1.0, `amid`) so a restarted voter resumes above the committed nonce.

## 5. Quorum math

Commit requires `acceptWeight / totalWeight >= 2/3`, summed over **every** validator
(a silent validator is an implicit non-accept). A memory is **deprecated** only when
*all* validators have voted and quorum still failed (e.g. a 2–2 tie). Single-node
chains are self-terminating: 1/1 accept commits, 1/1 reject deprecates.

## 6. Observability & alarms

- **`/ready`** carries a `voter` block (running / validator id / oldest-proposed age).
- Prometheus (`GET /metrics`, loopback):
  - **`sage_voter_running`** — `1` while the voter goroutine is live. Alert on `0`.
  - **`sage_proposed_oldest_age_seconds`** — age of the oldest `proposed` memory. The
    direct stuck-memory alarm; alert when it exceeds a few poll intervals (e.g. `>300`).
  - **`sage_proposed_pending_count`** — backlog size.

> On a multi-node chain these gauges are **node-local**: every node alarms while only
> one peer's voter is down. The metric tells you *something* is stuck; §7 tells you
> *which* validator is silent.

## 7. Stuck-memory triage

1. **Is `sage_voter_running == 1` on every validator?** If not, that node's voter is
   down (missing key, crashed) — restart it / fix the key.
2. **Did the vote land on-chain?** Check the `vote:<memoryID>:<validatorID>` state key.
   If the voter ran but the vote is absent, the broadcast was lost (RPC down) — it is
   re-cast on the next tick.
3. **Code 13 (unauthorized validator)?** This node's consensus key isn't in the
   validator set — a legacy 4-archetype chain. `sage-gui` self-repairs; on `amid`, add
   the key to the set via governance.
4. **Partial vote below quorum?** A peer is offline/silent — bring the missing voters
   up.
5. **All voted, no quorum (tie)?** The memory is `deprecated` by design — resubmit
   revised content.

## 8. REST / SDK `vote()` — the honest caveat

`POST /v1/memory/{id}/vote` signs with the **node's** validator key, so every REST
vote through a node carries that node's validator identity and lands in the single
`vote:<id>:<nodeValidatorID>` slot — later votes overwrite earlier ones, including the
auto-voter's. **There is no per-agent vote weight.** A node with no validator key
configured returns `503` (voting disabled), not a signed tx that dies with Code 13.
Prefer letting the auto-voter run; use REST `vote()` only as a deliberate operator
override on a validator node.

## 9. What NOT to do

- No standalone voter daemon (block-signing-key exfiltration + nonce races).
- Don't manually re-flood votes on stuck memories — fix the voter/validator-set cause.
- Don't expect `proposed` rows to expire — there is no TTL by design (auto-deprecating
  aged proposals would be a consensus-fork change).

## 10. What "guaranteed auto-commit" means

`voter.required` / `--require-voter` guarantees the voter *runs or the process exits*.
It does **not** mean content is vetted: the voter's `Decide` is mechanical
(dedup / length / noise / confidence thresholds). Any well-formed submission above the
confidence floor becomes committed institutional memory with no human or semantic
review. Classification still gates *reads*, so this is a pollution risk, not a
disclosure one. If no replacement is needed, the removal path is
`sage_forget` / challenge. If the content is being corrected, use
`sage_remember(replaces_memory_id=...)`: it verifies the replacement is
committed before challenging the old memory, so an interrupted correction may
leave both records but never neither.

The `dedup` check matches any **other** memory that has left `proposed` (validated,
committed, challenged, or deprecated) carrying the same content hash. The candidate's
own row is always excluded, and other still-`proposed` rows are ignored so two in-flight
identical submissions cannot reject each other. Consequences: identical bytes are
rejected again after a rejection or deprecation (sticky rejection — the removal path is
`sage_forget`, and a corrected body must actually change the content to pass dedup), and
a correction submitted via `sage_remember(replaces_memory_id=...)` therefore carries a
new content hash.

That check lives in the voter, so it only covers paths that vote. A co-commit
(`POST /v1/cocommit/submit`) commits on block inclusion and never consults the voter at
all, so the same tombstone question is asked once more at the REST submission boundary
(`api/rest/cocommit_handler.go`): a content hash another row already carried past
`proposed` returns `409 Tombstoned content`, with the envelope's own `SharedID` excluded
so an idempotent re-send is not refused by the row it wrote, and a projection error fails
open rather than blocking the surface. Two limits are worth stating plainly. It is a
submission-boundary check, not a consensus rule — a node broadcasting a co-commit
transaction directly, without this node's REST surface, is not covered by it. And it
sits here *because* the consensus path deliberately reads no off-chain state: the
content-hash index lives in the serving projection, so a deterministic in-consensus
version of this check would mean folding that index into the app hash.
