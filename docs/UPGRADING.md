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

Accept the update in SAGE, including the v11.19.3 to v11.19.4 update. The
updater checks canonical governance state, captures and verifies a recovery
snapshot, coordinates shutdown, captures the final stopped application state,
installs the new release, rolls back automatically if the final safety gate
fails, and restarts the node. A compatible pending plan or ballot is preserved
and continues after restart. There is no terminal command, manual backup, or
manual preflight in the normal desktop flow.

v11.19.3 did acquire the compatibility proof and snapshot fence separately,
which v11.19.4 corrects. That does not require a personal-node user to perform a
manual transition: both releases have the same app-v27 ceiling, and the
personal-node automatic governance worker cannot create an unsupported
app-v28 transition in that interval.

### Operator-only exception: externally mutable v11.19.3 governance

The coordinated stopped-node procedure applies only when a v11.19.3 node is in
a quorum deployment or another authorized operator/automation can mutate
upgrade governance concurrently with binary replacement. Deployment automation
must perform these steps; they are not an end-user desktop workflow:

1. Coordinate-stop every validator.
2. Retain a complete stopped-state backup (`sage-gui backup --full`).
3. Stage the v11.19.4 binary or app without starting it.
4. Run the staged binary's `sage-gui upgrade preflight` against the exact
   stopped data directory on every validator.
5. Install and restart only after every validator reports `COMPATIBLE` and the
   expected identical application state; otherwise keep the old executable and
   resolve governance before retrying.

This operator path avoids the v11.19.3 live check-to-fence window. Once
v11.19.4 is running, future live updates use one atomic proof.

> **Install SAGE v11.18.0 or later before you back up.** That is the concrete
> minimum for `backup --full`, `restore --from`, `upgrade preflight`, and the
> `upgrade lineage` commands; v10.x and older v11 binaries do not provide this
> complete contract. Installing a new binary is safe and
> reversible on its own: it changes no data and activates no fork until the node
> runs and governance approves each rung. Check what you have with
> `sage-gui upgrade lineage verify --help`; if it is not recognized, your
> binary predates the complete v11.18.0 recovery toolset.

### Technical stopped-node compatibility check

Headless and quorum operators replacing raw binaries can inspect canonical
state while a node is stopped:

```bash
sage-gui upgrade preflight
```

The command opens canonical Badger state read-only and reports compatibility
with the installed binary:

```text
Binary replacement guard (canonical stopped-node state):
  pending plan : none
  active ballot: none
  VERDICT      : COMPATIBLE — no in-flight upgrade governance state.
```

Supported pending plans and upgrade ballots are also `COMPATIBLE`: their exact
state is retained and they continue after restart. `INCOMPATIBLE`, an
inspection error, or validator disagreement means the target is malformed or
newer than the installed binary supports; do not mutate the executable or edit
Badger to manufacture a result. The desktop updater performs this same check
automatically before it changes the installed application.

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
| **App version** (`app-v27`) | The consensus state-machine version, activated by governance | Only when consensus rules change |

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
| v11.18.3 | Signer fence for same-key nonce ordering; no new app version; app-v26 remains the ceiling |
| v11.18.4 | One-call reply-aware inbox, exact Go vulnerability gates, conservative pipeline retention; no new app version; app-v26 remains the ceiling |
| v11.18.5 | Request-preserving stdio MCP runtime handoff and machine-readable coordination schema/version evidence; no new app version; app-v26 remains the ceiling |
| v11.18.6 | Exact H and H/H+1 updater snapshot provenance/replay-boundary proof, bounded exact-generation federation Retry, and memory-reassign log hardening; no new app version; app-v26 remains the ceiling |
| v11.18.7 | Bounded large-transaction Comet transport, independently enforced 1.2 MB app-v20 finalize limit, and separate 600,000-byte signed AgentRequest proof bound; deadlock-safe asynchronous federation route refresh; authenticated P2P-only trust-generation bootstrap recovery; security-first federation route diagnostics; no new app version; app-v26 remains the ceiling |
| v11.18.8 | Non-reusing HTTP/1.1 seam across fenced Comet submissions, signer-fence-first restart diagnostics, unsafe MCP reply-watermark recovery, and post-send passive inbox snapshots; no new app version; app-v26 remains the ceiling |
| v11.18.9 | Typed indeterminate Comet commit/sync outcomes, fail-closed federation nil-result fencing, and cross-package commit-decoder drift contracts; no new app version; app-v26 remains the ceiling |
| v11.18.10 | Same-agent MCP claimant-session ownership and atomic handoff, stale-session reply rejection, passive recovery, the CEREBRUM agent-connectome view, and bounded upgrade-watchdog broadcasts that retain indeterminate signer fencing; no new app version; app-v26 remains the ceiling |
| v11.18.11 | Operator-only live connectome firing via contentless ticks and authorized snapshot refetch, contentless retrieval activity, payload-free Claude inbox visibility with hook self-heal, chain-ID locality, Windows executable checksums, and patched Go 1.25.13; no new app version; app-v26 remains the ceiling |
| v11.18.12 | Projection-safe agent-as-lobe engrams, exact dashboard SSE registry coverage, signed task-status repair across official clients, exact-agent message presentation, and fail-closed documentation citation coverage; no new app version; app-v26 remains the ceiling |
| v11.18.13 | Hubanov distributed-engram bridges with bounded deterministic corroborator evidence, accessible Connectome guidance without a floating card, Claude signed production wake source with lossless shutdown, and claimant-session-safe reply fallback; no new app version; app-v26 remains the ceiling |
| v11.18.14 | Unfinished-message wake and exact stranded-claim visibility, lease-free monotonic Stop nudges, sender-TTL-preserving canonical migration, persistent accessible Connectome agent details, batched totally ordered corroborator presentation, and the truthful 31-day timeline contract; no new app version; app-v26 remains the ceiling |
| v11.18.15 | Unfinished-message wake backfill for claimed-only upgrades, default-on fail-open Stop nudges for Claude Code and Codex, atomic exact-local legacy-pipe admission, explicit opt-in for the experimental Claude notification adapter, deterministic pending-memory tiebreaks, canonical linked-worktree identities, live directed Connectome inspection, and anchor-aware citation repair with pinned parser debt; no new app version; app-v26 remains the ceiling |
| v11.18.16 | Unfinished-message wake and passive exact-session claim visibility in `sage_inbox`, payload-free hook parity for claimed work, and fail-soft compatibility when that additive projection is unavailable; no automatic ownership transfer; no new app version; app-v26 remains the ceiling |
| v11.18.17 | Durable primary stdio claimant identity scoped by exact agent/provider/project, OS-lock liveness fencing across ordinary restarts, distinct concurrent-session ownership, and installed-runtime identity carry-forward; pre-v11.18.17 claims still require explicit CAS handoff; no new app version; app-v26 remains the ceiling |
| v11.18.18 | Byte-exact automatic Codex lifecycle-hook self-healing plus click-first CEREBRUM Connectome agent details, larger neuron targets, and a relationship-scoped fallback selector; no new app version; app-v26 remains the ceiling |
| v11.18.19 | Global-scope Codex hook isolation, single-owner hit-tested Connectome clicks, bounded domain-access details, and responsive bloomed memory nodes; no new app version; app-v26 remains the ceiling |
| v11.18.20 | Same-mode verified MRI snapshot retention across transient refresh failures, with cold and cross-mode failures still fail-closed; no new app version; app-v26 remains the ceiling |
| v11.18.21 | MRI-renderer authority for the central unavailable overlay, with independent domain-inventory failures localized to their own retrying panel; no new app version; app-v26 remains the ceiling |
| v11.18.22 | Post-render MRI initialization hardening, verified-core readiness before optional renderer setup, and feature-gated `clickAfterDrag` support for bundled ForceGraph runtimes; no new app version; app-v26 remains the ceiling |
| v11.18.23 | Turn-recall trust/lifecycle parity, non-fatal boot-time embedding-space mismatch disclosure, and managed-reranker loader incompatibility diagnosis with bring-your-own guidance; no new app version; app-v26 remains the ceiling |
| v11.18.24 | Session-fenced federated claim recovery and idempotent reply events, MCP boot-guidance result isolation, actionable-only retention labels, and qualified-versus-bare embedding alias diagnosis; no new app version; app-v26 remains the ceiling |
| v11.18.25 | Generation-fenced CEREBRUM task refreshes with fail-closed reconciliation, plus portable merge-preserving Codex hook shell migration; no new app version; app-v26 remains the ceiling |
| v11.18.26 | Validated Go dependency refresh plus pinned CI action updates; app-v23 MCP bearer issuance binds to existing approved locally managed agents; token-create help is side-effect free; no consensus change; app-v26 remains the ceiling |
| v11.18.27 | Caller-safe empty semantic-recall completeness disclosure with exact projection/vector-space fencing and bounded indexed probes; no consensus change; app-v26 remains the ceiling |
| v11.18.28 | Restored reads for compile-time shared domains with classification enforcement, plus rejection of ownership registration for reserved or governance-promoted shared domains; no consensus change; app-v26 remains the ceiling |
| v11.19.0 | app-v27: static reserved shared-domain record authors gain hard-denial-preserving challenge/reinstate authority; omitted new-task `task_status` canonicalizes to `planned` |
| v11.19.1 | Payload-free cursor-paginated recovery for other-session claims, TTL-consistent counts, and session-fenced/idempotent recovery and reply for provider-addressed compatibility messages; includes an off-chain SQLite claim-receipt backfill, no consensus change, and app-v27 remains the ceiling |
| v11.19.2 | Consensus-authoritative pending-plan and active-ballot inspection through live `upgrade status` and stopped-node `upgrade preflight`; malformed or inconsistent canonical state fails closed; no consensus change, and app-v27 remains the ceiling |
| v11.19.3 | The normal updater performs the canonical compatibility check itself, carries supported in-flight governance through its verified recovery snapshot, and requires no user CLI or prompt; malformed or unsupported state still fails before executable mutation |
| v11.19.4 | Replacement capability is read from the exact candidate binary, while governance validation plus committed height/AppHash capture remain under one uninterrupted runtime fence; fixes the v11.19.3 live-updater TOCTOU without changing app-v27 |
| v11.19.5 | Exact-local receipt repair, durable transport-scoped claimant identities, revision-fenced explicit handoff with legacy REST revision-0 compatibility, and database-incarnation-fenced payload-free nonblocking task/reply activity wake; no consensus change and app-v27 remains the ceiling |
| v11.19.6 | Typed memory-link reads over REST and `sage_get_links`, with both endpoints filtered through caller disclosure policy before graph lookup; personal-node upgrades remain automatic while stopped-node preflight is operator-only; no consensus change and app-v27 remains the ceiling |
| v11.19.7 | Default-off consent-gated recall-backed compaction with commit-backed byte-exact capture, visible gaps, complete same-thread restoration, and governed purge; isolated typed-link MRI rendering and correct last-write-wins memory re-typing; refreshed Go modules and CI actions; no consensus change and app-v27 remains the ceiling |
| v11.19.8 | Bounded caller-domain discovery now includes re-authorized current-owned domains of active local Access Group peers, so transferred historical domains remain discoverable without a global roster or grant copying; no consensus change and app-v27 remains the ceiling |
| v11.19.9 | Unpinned Codex MCP sessions reject filesystem-root workspace resolution before key loading or generation, preventing retired `global-codex` reuse and synthetic `codex//` registration; no consensus change and app-v27 remains the ceiling |
| v11.19.10 | Returning-agent approval reclaims only the identity's exact Root-retired home through the existing owner-bound transfer path; pending rejection counts active memories rather than deprecated audit history; no consensus change and app-v27 remains the ceiling |
| v11.19.11 | Exact operator-configured CEREBRUM hostnames for loopback TLS reverse proxies, with loopback-only peer/forwarded-IP enforcement and unanimous fail-closed `X-Forwarded-Proto` parsing; no consensus change and app-v27 remains the ceiling |
| v11.19.12 | Project-scoped MCP and Codex installs reject the user's home directory to prevent global host-config pollution; the Linux native-shell gate admits the independently verified September AppImage-helper rebuild by exact SHA-256; no consensus change and app-v27 remains the ceiling |
| v11.19.13 | Stdio MCP startup skips automatic Claude project-hook repair when its working directory is the user's home directory, while normal project repair and explicit-install safeguards remain unchanged; no consensus change and app-v27 remains the ceiling |
| v11.19.14 | gRPC-Go v1.83.1 fixes HTTP/2 DATA-frame fragmentation heap exhaustion (CVE-2026-84304), with required genproto/OpenTelemetry updates and CodeQL action v4.37.9 pins; no consensus change and app-v27 remains the ceiling |
| v11.19.15 | Consensus-safe memory cleanup with uncapped full-inventory previews, durable exact-byte recovery, and honest progress counts; automatic cleanup requires fresh current-Root opt-in after upgrade, open tasks/internal records stay protected, and audit history remains; no consensus change and app-v27 remains the ceiling |
| v11.19.16 | Automatic trusted-node agent discovery and messaging when both peers are upgraded; memory sharing remains explicit and existing approved grants remain; searchable CEREBRUM directory and bulk/drag-and-drop Read/Copy drafts; no consensus change and app-v27 remains the ceiling |
| v11.19.17 | Federation connectome, operator-only metadata activity stream, explicit viewed-node names, clearer pairing onboarding, and Read/Copy/removal drafts; existing trust and permissions preserved; no consensus change and app-v27 remains the ceiling |
| v11.19.18 | Visible federation agent orbits and reliable interaction pause/resume; no consensus change and app-v27 remains the ceiling |
| v11.19.19 | gRPC-Go v1.83.2 patches the xDS missing-authority-header denial of service (CVE-2026-84445); no consensus or storage migration and app-v27 remains the ceiling |
| v11.19.20 | Voter dedup is sticky: rejected, challenged, or forgotten content cannot re-enter under a fresh memory id, a candidate never self-matches, concurrent identical submissions no longer veto each other, and corrections still pass when the content changed; the content_hash dedup lookup gains an index on SQLite and Postgres (one-time Postgres rebuild at first boot); no consensus change and app-v27 remains the ceiling |

### v11.18.3 — the signer fence, and what it does *not* cover

**The honest claim, and the only one to make:** same-key nonce inversion —
allocating a nonce, losing the RPC response, and letting a later nonce overtake
the in-flight transaction into a Code 4 "nonce too low" rejection — is
**eliminated within a running daemon process for every shared-key producer**.
The dashboard, REST API, federation manager, voter, and upgrade watchdog now
allocate and submit under the same per-key lease, and a submission whose outcome
this process never observed closes that key until the exact transaction is
proven committed or proven permanently refused.

**Process-boundary caveat.** A standalone `sage-gui` CLI invocation is a
different process. Its own submissions use the same safe lease and strict
Comet verdict decoder, but that in-memory lease cannot observe a concurrently
running daemon's fence. Do not run standalone signing commands concurrently
with a daemon that uses the same private key. Cross-process coordination needs
the same durable pre-broadcast intent described below and is not in this
release.

**Cross-restart and crash exposure remain.** The fence is in memory only. A
crash, a `kill -9`, or a power cut while a transaction's fate is unresolved still
discards it, after which the allocator re-seeds from the highest *committed*
nonce — which is below the abandoned one — and the next transaction can overtake
it. This release actively prevents the case it controls: a **coordinated restart
is refused while any signing key is fenced**. The veto is evaluated when the
restart is requested and **re-evaluated after signing has quiesced and in-flight
submissions have drained**, while the restart can still be abandoned — so a
fence raised by a submission that was mid-flight when the restart began also
refuses it, and the node keeps serving while reconciliation resolves the fence.
It does not and cannot prevent an unplanned stop.

Closing the residual needs durable pre-broadcast intent (the exact bytes recorded
before the send, reconciled before any allocation on startup), which is **not in
this release**.

**Do not restart a node to clear a fenced signing key.** Restarting is the action
that loses the transaction. See
[`docs/reference/concepts/signer-nonce-fence.md`](reference/concepts/signer-nonce-fence.md)
for the full contract, the operator triage steps, and the log/metric surface.

A v10.x chain therefore sits somewhere around **app-v11 to app-v14**, and
current v11 binaries support up to **app-v27**. That is roughly thirteen rungs.
v11.18.0 does **not** introduce app-v27 and does not rewrite an existing
app-v22, app-v23, app-v24, app-v25, app-v26, or app-v27 chain.

Forks activate **strictly one at a time**: every proposal must target the
chain's current version **+ 1**. Skipping is rejected — a jump from 14 to 27
would turn on app-v27 alone and permanently strand everything between.

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
VERDICT: clear to climb from app-v14 to app-v27.
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
does not modify an already-upgraded app-v22–app-v27 chain, repair an invalid
present record, or synthesize a later fork activation.

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
Chain app version : 27 (app-v27)
Binary supports   : up to app-v27
Pending plan      : none
Active ballot     : none
Next fork         : none — chain is at the highest version this binary supports
```

`upgrade status` obtains the chain version, pending plan, and active ballot
from the fail-closed `/upgrade/governance-status` ABCI query; the binary ceiling
comes from the local executable. `Pending plan` is the canonical
`upgrade:plan` record; `Active ballot` is the canonical `state:gov:active`
proposal and includes the decoded target app version for an upgrade ballot.
The command exits non-zero if either record cannot be read or decoded. Do not
substitute `sage_gov_status` for canonical diagnostics: that MCP tool is useful
for vote progress, but reads the off-chain dashboard projection rather than the
canonical Badger state. Normal desktop upgrades perform the canonical check
internally and do not require this command.

Thirteen rungs take a while: each activation waits out an upgrade delay of at
least 200 blocks. **This is normal.** An idle SAGE chain mints no blocks at all,
so the node submits harmless heartbeat transactions to tick a quiescent chain
toward each pending plan's activation height.

### Is it stuck, or just slow?

Each rung waits out at least 200 blocks. Personal nodes run
`timeout_commit = 1s` and the watchdog heartbeats a quiescent chain every 2s, so
budget **roughly 4–7 minutes per rung**; quorum clusters run `timeout_commit =
3s`, so **roughly 10 minutes**. A thirteen-rung v10 → app-v27 climb is therefore
about **1 hour on a personal node** and **2 hours on a cluster**. Treat these as
order-of-magnitude, not a guarantee — a busy chain mints blocks faster.

`upgrade status` shows a pending plan's activation height but not the chain's
current block height, so it can look frozen between activations even when
everything is fine. The reliable progress signal is **block height rising**:

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

When crossing from v11.18.4 or earlier to v11.18.5, restart each connected agent
session once. The already-running older MCP subprocess cannot contain
v11.18.5's executable-handoff logic, so its cached tool descriptions and
pointer-only inbox behavior remain stale until that one reconnect. Confirm the
new session's `sage_inbox` response reports
`coordination_schema: "sage.inbox.v2"` and `mcp_runtime_version: "11.18.5"`.

After a stdio MCP session starts on v11.18.5 or later, subsequent installed
binary replacements are detected before the next unread JSON-RPC request is
executed. That exact frame and the remaining stdio stream are handed to the new
runtime, so ordinary future upgrades should no longer require a manual agent
restart merely to refresh runtime behavior. Sessions initialized on v11.18.5
advertise `tools.listChanged`; the replacement emits
`notifications/tools/list_changed` once the existing logical session has
completed initialization, so conforming clients re-list changed tool definitions.
A client that ignores that notification must explicitly re-list
or reconnect before relying on newly added tools or arguments. A client or
operating system that terminates the stdio transport independently may still
reconnect normally.

If you crossed app-v23, also reissue HTTP MCP bearer tokens — activation revoked
every legacy keyless bearer:

```bash
sage-gui mcp-token list      # revoked entries show here
sage-gui mcp-token create --agent <existing-agent-id>  # bind a replacement to an approved locally managed agent
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
  — the Access Group authority model
- [`reference/concepts/app-v27-lifecycle.md`](reference/concepts/app-v27-lifecycle.md)
  — current record-author lifecycle authority and task-status canonicalization
- [`reference/concepts/block-production-and-idle.md`](reference/concepts/block-production-and-idle.md)
  — why a healthy idle chain mints no blocks
- [`GETTING_STARTED.md`](GETTING_STARTED.md) — first-time setup
