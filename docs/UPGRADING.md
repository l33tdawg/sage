# Upgrading SAGE

This is the procedure for moving an **existing SAGE node** to a newer release,
including the long jump from v10.x to current v11.

Your chain advances **in place**. SAGE does not reset, rebuild, or re-genesis a
lived-in node during an upgrade, and no supported procedure asks you to export
SQLite and initialize a fresh chain. Memories, domains, grants, agent
identities, governance records, and block history all survive.

**The recovery commands in this guide require SAGE v11.18.0 or later.** That is
the first concrete release containing `backup --full`, `restore --from`,
`upgrade preflight`, and `upgrade lineage status|doctor|verify`. Install the
v11.18.0 binary before relying on any of them; an older binary may not recognize
the command, and an old `backup` implementation may interpret `--full`
differently.

---

## TL;DR for a personal (single-node) install

```bash
# 1. Install the new binary FIRST. It does not touch your data directory,
#    and it is what provides the backup/preflight commands below.
# 2. Stop SAGE.
# 3. Take a real backup and check the chain can make the climb:
sage-gui backup --full
sage-gui upgrade preflight

# 4. Start the new binary:
sage-gui serve

# 5. Watch the ladder climb (it is automatic on a personal node):
sage-gui upgrade status
```

> **Install SAGE v11.18.0 or later before you back up.** That is the concrete
> minimum for `backup --full`, `restore --from`, `upgrade preflight`, and the
> `upgrade lineage` commands; v10.x and older v11 binaries do not provide this
> complete contract. Installing a new binary is safe and
> reversible on its own: it changes no data and activates no fork until the node
> runs and governance approves each rung. Check what you have with
> `sage-gui upgrade lineage verify --help`; if it is not recognized, your
> binary predates the complete v11.18.0 recovery toolset.

On a personal node that is the whole upgrade. The node proposes and activates
each consensus fork by itself until it reaches the binary's ceiling.

Quorum clusters are manual — see [Quorum clusters](#quorum-clusters).

---

## What actually changes between releases

SAGE has two independent version numbers, and confusing them is the source of
most upgrade anxiety.

| | What it is | When it changes |
|---|---|---|
| **Release version** (`v11.x.y`) | The binary/semver you download | Every release |
| **App version** (`app-v26`) | The consensus state-machine version, activated by governance | Only when consensus rules change |

There is also a **consensus fork version**, currently `1`, which has never been
bumped. It is the refusal gate for genuinely incompatible on-disk state. Because
it is still `1`, **a v10.x data directory is compatible with a current v11
binary** — no migration tooling, no re-genesis.

Installing a new binary does *not* by itself change the app version. The binary
gains the *ability* to run newer forks; each fork then activates through
governance. That activation is the "ladder".

### Release → app version

Use this to work out how far your chain has to climb.

| Release | Introduces |
|---|---|
| v10.0 | app-v11 |
| v10.5.x | app-v12, app-v13 |
| v10.7.0 | app-v14 |
| v11.0 | app-v15 |
| v11.2 | app-v16 |
| v11.5 | app-v17 |
| v11.7 | app-v18 |
| v11.8 | app-v19 |
| v11.9 | app-v20 |
| v11.13.4 | app-v21 |
| v11.14.1 | app-v22 |
| v11.15.0 | app-v23 |
| v11.16.0 | app-v24 |
| v11.16.2 | app-v25 |
| v11.17.0 | app-v26 |
| v11.18.0 | no new app version; app-v26 remains the ceiling |
| v11.18.1 | MCP initialization plus safe schema-v2 skip-ahead lineage recovery; app-v26 remains the ceiling |
| v11.18.2 | Sender-side reply visibility (`sage_message_replies`); no new app version; app-v26 remains the ceiling |

A v10.x chain therefore sits somewhere around **app-v11 to app-v14**, and
current v11 binaries support up to **app-v26**. That is roughly a dozen rungs.
v11.18.0 does **not** introduce app-v27 and does not rewrite an existing
app-v22, app-v23, app-v24, app-v25, or app-v26 chain.

Forks activate **strictly one at a time**: every proposal must target the
chain's current version **+ 1**. Skipping is rejected — a jump from 14 to 26
would turn on app-v26 alone and permanently strand everything between.

---

## Step 1 — Install the new binary

Do this first. It touches no data, activates no fork, and it is what gives you
the backup and preflight commands the rest of this procedure uses.

```bash
# macOS / Windows / Linux download
# https://github.com/l33tdawg/sage/releases/latest

# From source
git clone https://github.com/l33tdawg/sage.git && cd sage
go build -o sage-gui ./cmd/sage-gui/

# Docker
docker pull ghcr.io/l33tdawg/sage:latest
```

> Replacing a binary on disk does **not** upgrade a running node. A long-lived
> `sage-gui serve` process keeps executing the code it started with. Stop and
> restart it, and confirm with `sage-gui upgrade status`.

### Desktop app (macOS .app / Windows installer)

The desktop builds are the primary release artifacts, and they need two extra
notes:

- **Quit SAGE fully** before the next step — closing the window is not enough on
  macOS; the node keeps running. The backup and preflight commands refuse while
  the node holds its instance lock, which is the check working correctly.
- **The macOS CLI is inside the bundle**, not on your `PATH`:
  `/Applications/SAGE.app/Contents/MacOS/sage-gui`. Use that full path for every
  `sage-gui` command in this guide, or add it to your `PATH`. The DMG is a
  drag-to-replace install, not a binary swap. On Windows the installer puts
  `sage-gui.exe` on the `PATH`.
- CEREBRUM's in-app update banner replaces the binary and restarts the node for
  you. That is fine for ordinary patch releases; for the v10 → v11 jump, take
  the backup and run preflight yourself first.

---

## Step 2 — Stop the node and take a real backup

```bash
sage-gui backup --full
```

> **`sage-gui backup` (without `--full`) is not sufficient before an upgrade.**
> It copies only `data/sage.db`, the SQLite *serving projection*. The canonical
> consensus state — memories, RBAC, governance, agent identities, block history
> — lives in BadgerDB and CometBFT and is **not** in that file. Restoring a
> `.db` copy cannot rebuild a chain.

`backup --full` writes a single `sage-full-<timestamp>.tar.gz` containing your
whole `SAGE_HOME` (config, agent keys, vault key) plus the data directory
(Badger, CometBFT, SQLite) when it lives elsewhere, with a manifest recording
the binary version, consensus fork, app version, and block height.

It **refuses to run while SAGE is running**, and that refusal is load-bearing:
archiving a live Badger LSM tree captures a torn state that will not restore.
Stop the node first. It also refuses to report success if the finished archive
contains no consensus database, so a misconfigured `data_dir` cannot hand you an
empty backup that looks complete.

> **The archive is unencrypted and contains every node secret** — `agent.key`,
> `vault.key`, TLS private keys, and MCP tokens. Treat the file as a credential:
> keep it on an encrypted volume, and never upload it as-is. Size it roughly at
> your current `~/.sage` footprint.

To restore:

```bash
sage-gui restore --from /path/to/sage-full-2026-08-07T09-12-33.tar.gz --force
```

`--force` is **required whenever a SAGE home already exists**, which on a real
node is always. Despite the name it is not destructive: it is what authorizes
the move-aside. The existing tree is renamed to
`~/.sage.pre-restore-<timestamp>` and never deleted, and the archive is
unpacked into a staging directory first, so a failure part-way through cannot
leave a half-populated tree at the live path.

The default backup location is inside `~/.sage`, which restore is about to
replace. It handles that for you: the archive is copied to a temporary directory
first, so the file cannot vanish mid-restore. Nothing extra to do.

### Docker

The data lives in the mounted volume, not the image, so back up the volume with
the container stopped:

```bash
docker stop sage
docker run --rm -v ~/.sage:/root/.sage ghcr.io/l33tdawg/sage:latest \
  backup --full --out /root/.sage/backups/pre-upgrade.tar.gz
```

The image's `ENTRYPOINT` is already `sage-gui`, so pass the subcommand directly —
`docker run … sage-gui backup` would try to run `sage-gui sage-gui backup`. The
archive lands in the mounted volume, so it survives the container.

> **Confirm the image is v11.18.0 or later before you rely on the backup.** An
> older image does not necessarily reject `--full` — it ignores the flag, writes
> the SQLite-only copy, and prints `Backup saved`. A success message, for the
> wrong thing, right before an irreversible climb.
>
> Check the tag you are actually going to run. `:latest` is resolved from your
> local cache, so a machine that pulled months ago still runs an old image under
> that name:
>
> ```bash
> docker run --rm ghcr.io/l33tdawg/sage:latest version
> docker run --rm ghcr.io/l33tdawg/sage:latest upgrade lineage verify --help
> ```
>
> If the version is below v11.18.0, or the second command errors with an unknown
> subcommand, that image predates the complete recovery commands. Re-run
> `docker pull ghcr.io/l33tdawg/sage:latest` and check again before going
> further. Checking a pinned `:11.18.0` instead would prove nothing — that tag
> has the commands by definition, so the check could never fail.

Then preflight the same way, using that same current image:

```bash
docker run --rm -v ~/.sage:/root/.sage ghcr.io/l33tdawg/sage:latest upgrade preflight
```

---

## Step 3 — Preflight

```bash
sage-gui upgrade preflight
```

Run this **with the node stopped**, after installing the new binary (Step 1) and
alongside the backup (Step 2). It is read-only: it inspects the consensus
database without writing, proposing, or mutating anything.

It answers the one question that is otherwise unanswerable until it is too late:
**will this chain survive the climb?**

### The predecessor-ladder invariant

app-v22 and app-v23 refuse to be proposed, approved, activated, *or restored*
unless consensus storage proves the complete predecessor ladder. Ordinarily
that is a canonical applied-upgrade record for app-v6 and every version from
app-v7 upward. A narrowly governed v2 repair receipt may instead give a missing
pre-app-v20 rung virtual compatibility coverage from an exact retained Comet
version jump (or an explicitly acknowledged audited anchor). A skipped rung is
never rewritten as an independent activation record. Invalid, ambiguous,
fabricated, or out-of-order evidence fails closed.

(app-v6's record is the single compatibility proof for the historical cumulative
app-v2 through app-v5 activation. Everything from app-v7 needs its own record.)

Why this matters on an old chain: a gap does not stop the climb early. The node
walks happily up to **app-v21** and only then fails closed — mid-ladder, long
after you committed to the upgrade. Preflight reads the same records with the
same rules and tells you up front.

A healthy result ends with:

```
VERDICT: clear to climb from app-v14 to app-v26.
```

A bad one names the exact rung:

```
VERDICT: this chain CANNOT reach app-v22.
  app-v17: missing canonical applied app-v17 record
```

If you get that: **do not delete the data directory and do not edit Badger.** A
present-but-invalid record cannot be overwritten; restore a complete stopped-
node backup taken before the damage or open an issue with the preflight output.
If the named rungs are absent, v11.18.0 has one narrow recovery path: let the
chain stop safely at app-v21, then use the governed lineage ceremony below.
Never invent activation heights merely to make the ladder pass.

### Governed legacy-lineage recovery at app-v21

This workflow exists only for an upgraded chain that is **exactly app-v21** and
is missing one or more canonical app-v6 through app-v21 activation records. It
does not modify an already-upgraded app-v22–app-v26 chain, repair an invalid
present record, or introduce app-v27.

1. Keep the stopped-node `backup --full` from Step 2. Start every validator on
   v11.18.0, allow lower healthy rungs to climb, and stop normal upgrade
   proposals once `upgrade status` reports app-v21.
2. On the proposing validator, inventory the live committed state and create a
   candidate from retained Comet history:

   ```bash
   sage-gui upgrade lineage status --json
   sage-gui upgrade lineage doctor --json --manifest-out repair.json
   ```

   `doctor` is read-only. It scans the complete retained history of Comet
   app-version updates. When history says `app-v8 -> app-v11` at height H and
   the canonical app-v11 record is really at H, it may cover missing app-v9 and
   app-v10 virtually with that single transition. It does not invent H-1/H-2
   heights and does not create fake app-v9/app-v10 activations.
3. Copy only `repair.json` to every validator operator. Each operator verifies
   the exact manifest independently against that validator's own chain and
   retained block results, then compares `manifest_digest` values:

   ```bash
   sage-gui upgrade lineage verify --json --manifest repair.json
   ```

   A block hash in the proposal is not self-proving; `verify` reconstructs the
   full app-version sequence from height 1 through the committed tip, including
   intermediate transitions that cover no rung, then reproduces every claimed
   jump, exact skipped-version set, target activation height, and block hash.
4. If retained history is pruned, use an independently audited anchor containing
   **every** missing version. Use `heights` only for genuine independent
   activations. Represent an actual skip as one `transitions` entry with its
   source version, target version, actual height, and exact missing open-interval
   versions; never manufacture separate H-1/H-2 heights. Do not mix the anchor
   with retained-Comet claims. Both
   creation and verification require the explicit unverified-history warning:

   ```bash
   sage-gui upgrade lineage doctor --json \
     --legacy-anchor audited-heights.json \
     --acknowledge-unverified-anchor \
     --manifest-out repair.json

   sage-gui upgrade lineage verify --json \
     --manifest repair.json \
     --acknowledge-unverified-anchor
   ```

   An anchor is an operator assertion, not recovered cryptographic history. An
   ACCEPT vote attests those exact claims. Its digest covers both maps and
   transition bundles. A missing target through app-v19 can be virtual at the
   transition height; app-v20/app-v21 targets require their real ceremony record.
   An independent anchored activation or a validated virtual transition target
   may source the next jump only at a strictly earlier height. A subsumed rung
   cannot. Equal/reversed heights, overlaps, and unproven sources fail closed.
5. After every validator reports the same eligible manifest digest, submit the
   exact app-v22 proposal:

   ```bash
   sage-gui upgrade propose --target 22 --lineage-repair repair.json
   ```

6. Automatic voting is disabled for every lineage-repair proposal, including
   on a one-validator chain. Each validator operator reopens the immutable
   payload in CEREBRUM Governance (or `sage_gov_status`) and explicitly votes
   ACCEPT, REJECT, or ABSTAIN. Do not accept merely because the proposer or
   another validator did.
7. After quorum and app-v22 activation, run `upgrade lineage status --json` on
   every validator and confirm the immutable repair audit and complete ladder
   before proposing app-v23.

Before step 1, coordinate a complete validator halt and install v11.18.1 on
every validator. Confirm every node reports the v2 lineage schema and identical
chain binding/digest before generating, proposing, or voting on a v2 manifest.
Never run this ceremony with a mixed 11.17.x/v11.18.1 validator set.

Also inspect `upgrade lineage status --json`, `sage_gov_status`, and the pending
upgrade shown by `upgrade status`. An already-executed v1 receipt on app-v22+
is historical: do not repair it again; retain it and confirm `legacy-v1`
provenance after the coordinated rollout. If app-v21 still has an approved or
pending app-v22 v1 payload, halt all validators before activation, preserve full
backups and the proposal/plan output, and do not create a competing proposal or
edit Badger. Upgrade all validators to v11.18.1, then confirm on every node that
`upgrade lineage status --json` accepts the v1 receipt with `legacy-v1`
provenance and `upgrade status` shows the identical bound app-v22 plan and
activation height. Only then restart all validators together and let that exact
plan finish in place. If any audit, plan, height, or record differs, stay
stopped and do not resume or vote. There is no cancel/migration command. New v1
doctor output, replacement v2 proposals, and storage edits are unsupported for
an already-approved v1 plan. The v1 audit binds the retained governance
proposal and approved payload directly; it does not use the pending plan's
`ProposerID` as that binding.

The manifest is chain/current-lineage bound. Direct historical and anchor
claims remain virtual compatibility evidence; retained-transition claims bind
missing rungs to one real target activation. None writes `upgrade:applied:*`
for a skipped rung. A changed digest, extra/missing/duplicate transition member,
mixed anchor evidence, future height, archive disagreement, payload change, or
insufficient explicit quorum fails closed.
See [`reference/upgrade-lineage-repair.md`](reference/upgrade-lineage-repair.md)
for the evidence and persistence contract.

### The app-v23 authority preview

Preflight also prints what app-v23 will do to your administrators, because this
surprises people more than anything else in the upgrade:

```
app-v23 authority preview (what activation will do to your admins):
  becomes CEREBRUM Root : ops-primary (3f9a1c…)
  demoted to Member     : 2 other Admin(s) …
```

See [What app-v23 does to your admins](#3-what-app-v23-does-to-your-admins).

---

## Step 4 — Climb the ladder

### Personal nodes (single validator)

Automatic. A node with quorum disabled runs an auto-advance worker that
proposes each next fork, waits for activation, and moves to the next rung until
it reaches the binary's ceiling. Start the node and watch:

```bash
sage-gui upgrade status
```

```
Chain app version : 26 (app-v26)
Binary supports   : up to app-v26
Next fork         : none — chain is at the highest version this binary supports
```

A dozen rungs takes a while: each activation waits out an upgrade delay of at
least 200 blocks. **This is normal.** An idle SAGE chain mints no blocks at all,
so the node submits harmless heartbeat transactions to tick a quiescent chain
toward each pending plan's activation height.

### Is it stuck, or just slow?

Each rung waits out at least 200 blocks. Personal nodes run
`timeout_commit = 1s` and the watchdog heartbeats a quiescent chain every 2s, so
budget **roughly 4–7 minutes per rung**; quorum clusters run `timeout_commit =
3s`, so **roughly 10 minutes**. A twelve-rung v10 → app-v26 climb is therefore
about **1 hour on a personal node** and **2 hours on a cluster**. Treat these as
order-of-magnitude, not a guarantee — a busy chain mints blocks faster.

`upgrade status` shows the app version but not the block height, so it looks
frozen between activations even when everything is fine. The reliable progress
signal is **block height rising**:

```bash
curl -s http://127.0.0.1:26657/status | grep latest_block_height
```

If the height is climbing, the upgrade is working — leave it alone. If the
height is static for more than a few minutes *while a plan is pending*, check
the node log for these two lines, which mean stop waiting and start diagnosing:

- `auto-advance halted:` — terminal. See
  [the admin-key failure](#1-auto-advance-halted--the-proposer-is-not-a-chain-admin) below.
- Repeated `propose rejected` — the proposal is not being accepted; the log's
  code and message say why.

A healthy idle chain minting no blocks is not a fault — see
[`reference/concepts/block-production-and-idle.md`](reference/concepts/block-production-and-idle.md).

### Quorum clusters

Manual, one rung at a time, from the node holding the admin key:

```bash
sage-gui upgrade status                    # shows the next target
sage-gui upgrade propose --target 15 --wait
sage-gui upgrade propose --target 16 --wait
# … repeat to the ceiling
```

`--wait` stays attached and heartbeats a quiescent chain until the fork
activates. Proposals route through the 2/3 governance quorum; validators
auto-vote ACCEPT if they support the target. Upgrade every validator's binary
before climbing — a validator that does not support the target cannot vote for
it. The only exception is an app-v22 proposal carrying `--lineage-repair`:
automatic voting is disabled and every validator must verify and vote
explicitly, as described above.

---

## The four things that actually go wrong

### 1. "auto-advance halted" — the proposer is not a chain admin

Past app-v8 the proposer must be a chain-admin agent: the signing key's agent ID
must hold `Role==admin` in the on-chain registry. If it does not, the proposal
is rejected at block execution (**code 47**) and auto-advance stops with:

```
auto-advance halted: this node's agent.key is not the on-chain chain-admin …
```

There is deliberately no automatic reset — rebuilding from SQLite would discard
canonical memory, RBAC, governance, and block history, and `repair-chain` is
disabled for the same reason.

Fix it by proposing with the key that *is* the chain admin:

```bash
sage-gui upgrade propose --target <N> --agent-key /path/to/chain-admin.key
```

`--agent-key` accepts an `agent.key` seed or a CometBFT
`priv_validator_key.json`. On many deployments the genesis validator key is the
admin.

If **no** key you hold is the chain admin, what you can do depends on where the
chain already is:

- **Below app-v9**, the wire `role=admin` self-grant is still open, so running
  any admin operation with your key materializes the role, and the climb can
  continue.
- **At app-v9 or above**, that door is closed by consensus. A chain whose admin
  key is lost recovers only from a complete stopped-node backup. There is no
  reset path: `repair-chain` is disabled precisely because rebuilding from
  SQLite would discard canonical history.

[`ISSUE_52_RECOVERY.md`](ISSUE_52_RECOVERY.md) is the authority for this failure
mode; read it before attempting anything else.

### 2. The signing identity changes at app-v23

Below app-v23, upgrade proposals are signed with the operator `agent.key`. From
app-v23 the default becomes **the current CEREBRUM Root credential**, resolved
from local key material (including recovery bundles). This is not a setting you
change; it is a consequence of Root becoming a distinct singleton authority.

Practical consequence: **keep the Root credential on the node host.** If Root
has been rotated, the stale genesis `agent.key` is no longer the right signer,
and `--agent-key` is an explicitly reviewed local Admin override rather than the
normal path.

### 3. What app-v23 does to your admins

app-v23 replaces capability-bit administration with roles, security profiles,
and Access Groups. The migration is deterministic and it is not gentle with a
multi-admin chain:

- **The earliest legacy Admin by registration height becomes the singleton
  CEREBRUM Root** (canonical Agent ID breaks ties). Root cannot be dragged into
  groups, messaged, demoted, or removed through ordinary agent controls.
- **Every other legacy Admin is demoted to an active Member** with its exact
  capability mask, the migration-only `legacy_restricted` profile, and
  disposition `legacy_admin_review`. Consensus cannot prove any other exportable
  legacy Admin key is still local to this machine, so none is promoted
  automatically — restoring one to Admin needs an explicit review attested by
  the current Root in CEREBRUM.
- The complete old Admin roster is kept as immutable audit evidence. Nothing is
  lost; authority is re-derived.
- Ordinary Members keep their exact app-v22 mask. Masks `0`/`16` map to
  `standard`, `15`/`31` to `companion`, everything else to `legacy_restricted`
  pending review.
- An agent matching the app-v22 bare self-registration fingerprint (mask `30`,
  no owned domain, no explicit grant) becomes **inactive** with `pending_review`
  and needs an administrator to assign an intentional profile.

Run `sage-gui upgrade preflight` beforehand to see exactly which agent becomes
Root and which ones land in review. Plan for it — do not discover it from a
support ticket.

Two more app-v23 mechanics worth knowing:

- **Activation block H is a quiescence barrier.** Every transaction delivered at
  H is rejected with **code 96**; normal execution resumes at H+1. This is
  intentional — it freezes the migration input so nothing races the activation.
  Brief write failures at exactly that height are expected, not a fault.
- **After app-v23 state or transaction types are committed, there is no in-band
  downgrade to app-v22.** Recovery is a forward fix or a trusted
  pre-activation snapshot. This is the point of no return in the ladder; it is
  the reason for Step 2.
- **Legacy MCP bearer tokens are revoked.** Activation durably retires every
  bearer token that has no key of its own. Any client authenticating over HTTP
  MCP with such a token — ChatGPT Work connectors, Cursor, Cline, Claude Desktop
  over `:8443` — stops working until you issue a new one with
  `sage-gui mcp-token create`. Clients using the stdio bridge (`sage-gui mcp`)
  are unaffected beyond a session restart. Plan this for the same maintenance
  window; it is the most common "the upgrade broke my agents" report.

### 4. app-v25 repairs historical memories, and may quarantine some

app-v25 makes new memory envelopes immutable and automatically repairs
historical rows. A row that cannot be repaired is quarantined record-locally and
surfaced honestly rather than silently dropped, with Root retry and deprecation
controls. If the UI shows partially displayed or repairing historical memories
after this rung, that is the documented behaviour — see
[`reference/app-v25-upgrade-recovery.md`](reference/app-v25-upgrade-recovery.md).

---

## Verifying you are done

```bash
sage-gui upgrade status     # app version == binary ceiling
sage-gui status             # node health
```

Then check that recall works from an actual agent — an MCP `sage_recall` on a
domain you know has content is the honest end-to-end test.

If your agents were connected over MCP, restart their sessions. A long-lived
MCP client keeps talking to the node it connected to, and short-lived `sage-gui
mcp` subprocesses proxy to the running node, so tool descriptions and behaviour
can look stale until both ends are restarted.

If you crossed app-v23, also reissue HTTP MCP bearer tokens — activation revoked
every legacy keyless bearer:

```bash
sage-gui mcp-token list      # revoked entries show here
sage-gui mcp-token create    # issue a replacement per client
```

---

## What never to do

- **Do not delete `~/.sage/data`** to fix an upgrade. That discards consensus
  history and is not an upgrade or repair procedure.
- **Do not export SQLite and initialize a new chain** and call it an upgrade.
  That is a different chain with none of your history.
- **Do not rely on `sage-gui backup`** (without `--full`) as pre-upgrade
  insurance. It backs up a rebuildable projection, not the chain.
- **Do not skip rungs.** Proposals must target current + 1.

---

## Related reference

- [`reference/upgrade-lineage-repair.md`](reference/upgrade-lineage-repair.md)
  — app-v21 → app-v22 evidence verification, explicit quorum, and immutable audit
- [`reference/app-v23-access-control-design.md`](reference/app-v23-access-control-design.md)
  — Root, roles, security profiles, Access Groups, and the full migration contract
- [`reference/app-v25-upgrade-recovery.md`](reference/app-v25-upgrade-recovery.md)
  — historical repair, quarantine, and Root resolution controls
- [`reference/concepts/app-v26-access-groups.md`](reference/concepts/app-v26-access-groups.md)
  — the current Access Group authority model
- [`reference/concepts/block-production-and-idle.md`](reference/concepts/block-production-and-idle.md)
  — why a healthy idle chain mints no blocks
- [`GETTING_STARTED.md`](GETTING_STARTED.md) — first-time setup
