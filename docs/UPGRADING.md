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
| v11.19.21 | Co-commit tombstones: a co-commit can no longer re-commit bytes the quorum already rejected (the REST submit boundary refuses a tombstoned content hash with 409 before it broadcasts; a transaction broadcast directly to the chain is not covered), and the MCP client reports the node's own dedup verdict instead of a >60%-word-overlap heuristic, so near-duplicates are stored and exact duplicates are skipped with the node's reason; no consensus change and app-v27 remains the ceiling |
| v11.19.22 | Build and dependency maintenance: the Go floor moves to patched Go 1.26.8 in both modules and every Go container builder (source builds now need Go 1.26.8+), and the Go dependency group is refreshed to pgx v5.11.0, klauspost/compress v1.20.0, x/crypto v0.57.0, x/sync v0.23.0, x/sys v0.48.0, x/tools v0.50.0 and modernc.org/sqlite v1.58.0/SQLite 3.53.4, with the store tests moved to pgxmock/v5; shipped binaries are unchanged in behaviour, there is no chain migration, and app-v27 remains the ceiling |
| v11.20.0 | Encrypted agent working state: vault-required private media (`GET`/`PUT /v1/private-media/{uuid}`, ciphertext-only rows, actor isolation, quotas and a startup disk-floor probe) and an actor-bound workflow journal (`GET`/`PUT /v1/workflows[/{uuid}]` with compare-and-swap revisions and an opt-out conversation guard), both reachable only from inside the app-v23 pipeline agent boundary; `GET /v1/messages/storage` plus a strict signed `require_encrypted_storage` send flag so a plaintext downgrade fails closed with a 503; SQLite opens with `synchronous=FULL` and verifies journal/synchronous mode at boot; vault publication is atomic; `sage-gui init-lantern-private` adds fresh-only Lantern bring-up behind a private-listener policy; a public-memory Merkle index, migration builder and stage/promote path ship dormant with no production caller, staged rows stay out of the AppHash, and promotion (named for app-v28) is unreachable; a per-connection agent discovery policy (all | selected | none, default all) can keep this node's agents out of a peer's listing and exact-name search without granting memory Read or delivery; no consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.20.1 | Federated reply delivery hardening: a reply stays admissible, and its retained outbox event keeps retrying, for seven days after its signed proof instead of 24 hours, the destination still admits the legacy 24-hour window, and a destination older than the longer window gets one automatic downgraded retry instead of a terminal failure; the startup retention migration that extends durable canonical sends no longer re-stamps reply rows (the receiver-local `msg-fed-…` id of an imported message matched its `msg-%` predicate), stamps the exact durable sentinel rather than SQLite's calendar `+100 years`, and repairs rows an earlier build already extended; reply envelopes are built from the signed proof so local retention state cannot reach the wire; refused proofs are logged with their exact reason on the destination and self-checked on the sender. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.20.2 | Indeterminate broadcast outcomes are reported honestly: when the node's own wait for block inclusion expires, the transaction is on the wire and may still commit, and every submit surface now answers `202` with `"status":"indeterminate"`, the exact `tx_hash` of the bytes that were broadcast, the allocated `nonce` and `"retryable":false` — instead of an opaque `500 Broadcast error` that was indistinguishable from a genuine internal fault and taught callers to re-sign a write that may already have committed. Definitive verdicts are explicitly excluded and unchanged: a CheckTx or FinalizeBlock rejection keeps its own status, and a full mempool keeps `429` with `Retry-After` because nothing was admitted. Generated testnets set `timeout_broadcast_tx_commit` explicitly at 45s instead of inheriting CometBFT's 10s, kept strictly below SAGE's client-side `SAGE_TX_COMMIT_TIMEOUT_MS` (60s) so the node — which knows whether it admitted the bytes — always answers first. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.20.3 | A peer body-limit refusal stays retryable: a federated event refused with `413` is no longer classified as permanently failed, because the per-route body cap of a peer is a build-time constant that moves when that peer upgrades and the refused body is frequently not oversized at all — one message was refused on 2026-09-15 while its signed body sat 2.6 KB under the route's 16 KiB cap and a larger message to the same peer was accepted minutes later. `413` now retries on an hourly floor like the other capability-shaped status, so the event stays pending and delivers when the peer can take it. The federation listener also stopped reporting a body it could not read as one that was too large: only a genuine `*http.MaxBytesError` is `413`, while a truncated upload, a mid-body disconnect or a stream reset is answered as a retryable read failure and logged with its cause. Failed delivery attempts are now logged with their event, peer, kind, attempt count, verdict and retry delay instead of only when recording the failure failed. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.20.4 | A signer fence survives the process that raised it, and a provably dead nonce has an exit. The fence was in-process state: a restart, crash or SIGKILL discarded it, the nonce allocator re-seeded each key from the highest COMMITTED nonce — below the abandoned one by definition — and the next action signed into that gap, after which the abandoned transaction was refused Code 4 on arrival and the loss surfaced as an unrelated replay failure. Every submission is now shadowed by a durable record written at the last boundary before the bytes reach the transport, carrying the signer, the transaction hash and the nonce but deliberately NOT the signed bytes (they routinely hold memory content that must not enter a plaintext table); it is retired only on a proven fate and re-raised as a fence at startup, so a node killed mid-submission refuses to sign that key instead of re-seeding past it. The second half is the exit for a fate the reconciler cannot prove on its own: `POST /v1/dashboard/signer-fence/lift` (CEREBRUM operator gate) accepts the exact transaction found in a committed block, or supersession by a higher committed nonce, both read from the node rather than asserted by the caller, and records that a superseded transaction's payload is permanently lost. A lookup miss is not proof: CometBFT indexes a transaction only once it is in a block, so a mempool-resident transaction answers not-found exactly as it does one second before it commits. Operator impact: a node that ends with an unproven submission now comes back holding a fence where it previously came back silently able to sign; the fence is visible in `GET /v1/dashboard/health` under `signer_fences` and is released only by the lift route above. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.20.5 | A peer whose address moved repairs its own route, and a flapping peer's windows are spent on the backlog. The stale-snapshot exemption that let a p2p-only agreement run the authenticated route exchange across trust generations now covers an agreement paired with a CONCRETE endpoint as well, because the same assumption fails there for a different reason: the stored address is real, so nothing looks unroutable, but every request dials a host that no longer answers while the peer's own traffic keeps arriving ("they can reach us, we cannot reach them"). Withholding the fallback also blocked the exchange, which is the only thing that can replace the stale snapshot, so such a pair could only recover by being re-paired. The exemption is about the path, not the agreement shape: the exchange may use a stale snapshot as an authenticated bootstrap hint for both, every other request still refuses a cross-generation route, and the exchange result is persisted only after revalidation against the exact agreement and binding. A moved host now fails with the trust-generation recovery code instead of a bare timeout, so the operator's next move is a route repair rather than a network investigation, while remaining an offline-class error so the outbox keeps retrying. Delivery: a successful delivery proves the peer reachable and makes the rest of that peer's pending backlog due immediately (attempt counts and last errors are untouched — only the sleep is cleared), and a drain pass covers 16 rows rather than 4 at unchanged concurrency; without this a freshly queued event, due on its first attempt, consumed a short connectivity window while older rows slept through it. Recall: the confidence floor is no longer silent — the store counts what it removed, REST reports `filtered.confidence_floor` and `filtered.hidden_by_confidence_floor` (and lists `confidence_floor` in `X-SAGE-Filter-Applied`), MCP returns `confidence_floor` plus a `filter_note` when anything was hidden, and the recall settings surface warns which write tiers a floor hides. A floor that removed nothing is disclosed as such. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.21.0 | The 11.21 line opens with the shell that admits it. The SSCP compatibility range lives in the shipped native shell rather than the daemon, so a minor bump is the release that widens it: the shell now accepts v11.10 through v11.21 daemons, which is what lets the fixes after this one ship as patch releases instead of a rebuilt shell each time. The daemon is otherwise the v11.20.5 build — the moved-peer route repair, the backlog wake, and the confidence-floor disclosure — so there is no new node behavior to re-verify and no migration to run. An 11.21 daemon under the 11.20.5 shell is refused control until the shell is updated with it, which is the compatibility gate working as designed rather than a fault. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.22.0 | App-v28 rides as a compiled, dormant gate: the sparse public-memory Merkle index becomes AppHash-covered at the activation height through a composite rule, and the co-commit tombstone rule moves from a REST-boundary check into consensus behind a content-hash reverse index maintained by every memory write and backfilled at activation. Nothing activates: `maxSupportedAppVersion` stays 27, every node's auto-voter abstains on v28, and activation waits for byte-identical AppHash across the seam on a four-validator devnet, both Consensus Fault Gates, state-sync/restore of a promoted node, and replay equivalence. The binary now distinguishes what it can execute (app-v28, compiled) from what it will auto-vote (app-v27): `upgrade status`/`upgrade preflight` print both, the version banner and full-backup stamp follow the compiled ceiling, state sync accepts restores up to it, and the acceptance gate plus the authorization ceiling moved with that split. New surfaces for the deliberate case: `sage-gui upgrade vote` casts accept/reject/abstain on the active upgrade ballot, and CEREBRUM's Governance view gains an App version panel that proposes the next rung and shows when a target is dormant. No chain reset, no transaction-type change, and historical blocks keep replaying under their original app versions; app-v27 remains the ceiling until this gate's evidence lands |
| v11.22.1 | Operator surface, not consensus. CEREBRUM's Federation page leads with trusted connections and states each link's discovery posture on its row, with per-agent Visible/Not-visible switches that save immediately under the connection's revision-bound agreement; the App version panel renders on nodes without governance scopes (it was nested behind the scope list, which stays empty until a `scope_action` commits); and the Python SDK gains `set_agent_access_policy`/`get_access_state` against the app-v23 enrollment routes, the clearance the memory-write gate actually reads. App-v28 stays compiled and dormant: the determinism ladder crosses every seam to app-v28 with byte-identical AppHash and the gate drives the whole ladder with auto-votes, but the contract's last item — a promoted node surviving state-sync and restore — currently fails in the real-process state-sync gate, so `maxSupportedAppVersion` stays 27 and the 27 → 28 bump ships in the release that turns that green. No consensus change, no chain migration, and app-v27 remains the ceiling |
| v11.23.0 | App-v28 activates: the auto-vote ceiling moves 27 → 28 and converges with the compiled ceiling, so a personal node advances its own chain across the seam on upgrade with no governance ceremony — the automatic path every earlier rung took. What activates is the gate v11.22.0 compiled: the sparse public-memory Merkle commitment over committed `PUBLIC=0` records becomes AppHash-covered through a composite rule (the legacy tree without the index nodes, composed with the index root), and the co-commit tombstone rule is enforced by the consensus path through a content-hash reverse index maintained by every memory write and backfilled at activation. The contract evidence landed with the bump: the four-validator determinism ladder crosses app-v2 → app-v28 with byte-identical AppHash at H-1/H/H+1 of every seam, both Consensus Fault Gates pass on the change, the real-process state-sync gate drives its provider through app-v28 so a pristine receiver restores and both sides report exact app-v28 state with converging AppHash, and the gate receiver pre-publication SIGKILL and provider SIGKILL phases plus the replay family pin restart and replay equivalence across H. The blocker v11.22.1 documented is fixed here: a v28 provider died on its state-sync serving boot because the verification family recomputed the pre-v28 AppHash rule unconditionally, and the rule is now selected in one function shared by the commit path and the verification path. This is a minor bump because the native shell SSCP compatibility range must widen to admit v11.23 daemons; fixes after this one ship as patches. No chain reset, no transaction-type change, and historical blocks keep replaying under their original app versions |
| v11.23.1 | An amid-only fleet can set enrollment clearance: amid mounts exactly the two dashboard access routes (GET /v1/dashboard/network/access and PUT /v1/dashboard/network/access/agents/{id}/policy) with the same handlers and the same operator gate CEREBRUM serves, so the emitted TxTypeAgentRoleChange is identical; the operator signs the exact request as the current Root, and the node configured broker key (--cerebrum-root-key-file / SAGE_CEREBRUM_ROOT_KEY_FILE) authenticates it. This is the record the memory-write gate reads: a submission classification is compared against the agent enrollment clearance, and membership clearance never satisfies it. Patch release inside the 11.23 line, so no shell range moves; no new transaction type, no consensus change, and no chain reset |
| v11.23.2 | A legacy hashless PUBLIC record quarantines instead of failing the app-v28 migration. A chain whose public corpus predates app-v28 could not open the fork: the stage is built outside the consensus transaction, so the 53 legacy PUBLIC records whose canonical content hash had been erased by a historical lifecycle transition aborted the activation block on every replay, and the node could not start at all - upgrade cancel is a consensus transaction and cannot run on a node that cannot boot. Those records are now folded out of the committed public set, exactly as the co-commit tombstone index already treats a record whose decoded hash is not 32 bytes; a record joins the set in the block that re-anchors its hash, and a missing hash on a record born under app-v25 or later is still a hard inconsistency that refuses the build. No transaction-type change, no upgrade height, no chain reset |
| v11.23.3 | A signer fence that could not be proven can no longer strand a node, and this release can be installed on one that is fenced. A fence restored from durable intent has no signed bytes, so re-submission cannot settle it: the node now re-reads its proofs on the live reconciler's schedule (the recorded hash in a committed block, or the signer's committed nonce having reached the fenced allocation, labelled spent when equal) and lifts on either. The shape no proof can settle is resolved at first boot when the evidence is unambiguous - the node is caught up, has seen no peer this run, holds no mempool copy, sees no committed fate, and the allocation is unspent - recorded as fence_abandoned with mode=automatic_unprovable, with the abandoned nonce reserved. The coordinated-restart veto refuses only when a fence's durable record cannot be confirmed (fail closed), so a fenced node can take an update again, and request-serving write paths answer 503 + Retry-After immediately instead of hanging until the caller's timeout. If a node is already fenced on v11.23.2 the in-app update is refused by that older rule: replace the app bundle in Applications and relaunch, and the first boot resolves the fence. No consensus change, no transaction-type change, no upgrade height, no chain reset |
| v11.23.4 | The fence fixes reach the nodes that keep a peer. v11.23.3 resolved a restored fence at first boot only when the node had never seen a peer since process start, and its operator abandon route refused outright whenever any peer was connected - so a federated desktop node, or a validator with a persistent peer, could sit behind a fence that no proof could settle with both recovery routes closed, still refusing every write. The peer observation is now anchored to the fence rather than to the process (a sighting from an earlier outage can no longer disable self-healing for every fence raised afterwards), the automatic route still requires no peer connected and none seen while this fence was held, and the operator abandon route accepts connected peers when the request carries a second acknowledgement, peer_redelivery_acknowledged, recording the peer count and the accepted route with the decision. The health surface gains a per-fence resolution class - reconciling (the exact bytes are being re-submitted until consensus answers) versus proof_or_operator (the bytes did not survive, so it lifts on a chain-read proof or an operator abandon) - and the explanation is rendered from it instead of promising self-healing for every fence. No consensus change, no transaction-type change, no upgrade height, no chain reset |
| v11.23.6 | A restored fence on a healthy but IDLE chain settles itself, and the operator has a real entry to settle one when it cannot. Since app-v12 every node runs create_empty_blocks=false, so a block mints exactly when a signed transaction enters the mempool; a fence restored from durable intent refuses to sign and its bytes are gone, so on a chain that was already quiet when the fence was raised the fence is holding the only thing that could produce its proof - no transaction, no block, and no block means no committed hash and no advanced committed nonce. The automatic route now settles that shape when the node is caught up, has no peer and has seen none under this fence, holds no mempool copy, sees no committed fate and holds an unspent allocation, and the chain's tip predates this fence - recorded as fence_abandoned with the abandoned allocation reserved and the mode string naming which evidence set settled it (mode=automatic_unprovable for the original startup resolution, mode=automatic_quiescent for the chain-that-has-minted-nothing-since rule added here), with the same stated residual as the existing automatic decision (a surviving copy of those bytes can still commit and lose its payload). The fence is still kept when the chain has minted since it was raised, when any peer is connected or was seen under it, when the mempool holds the transaction, while the node is catching up, when the tip cannot be read, and for every live fence. Two diagnosability and reachability defects from the field are closed in the same release: every outcome of the automatic route now reaches the fence's last_detail (the refusal reason AND a fault, because a fault was previously dropped silently - which made a node whose self-heal could not read its own evidence look identical to one that was merely waiting), and `sage-gui fence list` / `sage-gui fence abandon` give an operator standing at the node host a real entry that needs no credential and applies the same evidence gate the daemon's route applies, because the dashboard had no control for this action and an HTTP MCP bearer token is not accepted by the operator gate. No consensus change, no transaction-type change, no upgrade height, no chain reset |


| v11.23.7 | An app-v28 chain can install its own updates again. The signed update flow takes a pre-upgrade recovery snapshot and proves it by restoring the Badger backup and re-deriving the AppHash the manifest recorded, and since app-v28 that digest is the public-memory composite commitment - the app-v13 digest of the state WITHOUT the public-memory index nodes, composed with the sparse index root - while the proof only knew the legacy, app-v12 and app-v13 eras. Every app-v28 node therefore refused its own recovery snapshot with "AppHash mismatch under every hash rule (legacy/app-v12/app-v13)" and stopped every signed app update, and the version-changing restart behind it, with `Failed to install signed app update: verify pre-upgrade snapshot`. This is the same stale-rule defect the state-sync provider verification path carried until v11.23.0, in the one proof path that never moved onto the shared rule. The composite now joins the candidate set, read through the same internal/store implementation the commit path uses instead of a third copy of the rule; a state that predates app-v28 carries no public-memory commitment, so the composite reports as not-applicable there and the three older candidates decide alone, and the mismatch message now names the rules that were actually tried. A node on 11.23.6 or earlier cannot install this release from inside the app, because the failing gate runs in the installed binary: install 11.23.7 by hand once (quit SAGE, drag SAGE.app from the DMG into /Applications, relaunch) and every update after it is in-app again. No consensus change, no transaction-type change, no upgrade height, no chain reset |
| v11.23.8 | A node stops manufacturing signer fences when it is shut down, and the credential-free operator exit reaches the nodes that need it. Every coordinated restart already drained signing before it committed, so its teardown could not sever a broadcast; an ordinary signal or serve error had no drain at all, and the HTTP force-close could catch a submission mid-flight, raise an indeterminate outcome, write a durable fence record, and cost that payload at the next start. The ordinary exit now quiesces signing, gives the in-flight population a bounded five-second window to finish, and only then closes the listeners - signing is deliberately not resumed, an operator-ordered exit is never vetoed by the drain, and a submission that outlasts the window fails closed onto the durable record and the restored fence. `sage-gui fence abandon` also gains --peer-redelivery-acknowledged: the daemon route requires that second acknowledgement whenever peers are connected, because a peer is a route the abandoned bytes could still take back into this node's mempool, and the CLI had no way to carry it - so a federated desktop node or a validator with a persistent peer could only ever be refused by the route built to give it an exit. `fence list` names the peer requirement when peers are connected and states the restart step on every unproven record. No consensus change, no transaction-type change, no upgrade height, and no chain reset |
| v11.23.9 | CEREBRUM shows a held signing key, and keeps the last resolution visible after the hold ends. A signer fence refuses every write from its key and every coordinated restart while it waits for proof of an earlier submission's fate, and the dashboard said nothing about it - the field report that produced the fence workstream read as "reads are fine, writes time out, no error". System Status gains a row that appears only while a key is held: the count and oldest age, the node's explanation of why the hold is deliberate, and one line per fence naming its resolution (reconciling means the identical bytes are still being re-submitted and it clears itself; waiting on proof means the signed bytes did not survive the process, so the chain or an operator settles it), with the nonce, how long the key has been held, the attempt count and the last recorded detail. The last resolution is kept and rendered once the hold ends, so a fence that cleared itself is still visible afterwards. The panel never suggests restarting to clear a hold, because a restart discards the fence and loses the transaction it protects. The nonce also crosses the status payload as a decimal string: a nanosecond allocation exceeds JavaScript's safe integer range, and as a JSON number it reached the dashboard silently rounded in the one place an operator compares it against the chain. No consensus change, no transaction-type change, no upgrade height, and no chain reset |
| v11.23.10 | A held signing key can no longer park the node's whole write path. A signer fence refuses every write from its key while it waits for proof of an earlier submission's fate, and that wait was bounded only by the caller's context - which the REST submit path passes as `context.Background()`, a context that never cancels and has no deadline, because a client disconnect must not cancel an already-authorized durable write. One held fence therefore parked the single per-key nonce lease forever: the observed node carried 19+ handlers in the same frame for 122-768 minutes, every later writer for that key queued behind them, and the automatic voter whose reconciliation lifts a fence was queued behind them too, so the hold could not clear itself. Reads stayed fast and denied writes failed fast, which is why it read as "reads are fine, writes time out, no error". `WithNonceLease` now derives a bounded wait (90 s) when the caller supplies no deadline, so a held fence returns a retryable ErrSignerFenced/DeadlineExceeded instead of parking, the slot is released, and the reconciler is no longer starved; a caller's own shorter deadline is never loosened. Background goroutines started by the embedded web handler are counted and drained before a store closes, and local `make` builds pin to the host architecture. No consensus change, no transaction-type change, no upgrade height, and no chain reset |
| v11.23.11 | Typed committed-memory vote refusals can resolve signer fences for complete post-app-v25 targets, with exact signed-byte binding and an index recheck. General code 13 stays unresolved. CLI help runs without command side effects; inbound federated messages wake their exact local recipient. An optional, default-off memory-quality gate adds background judging and operator review for SQLite nodes; configure `SAGE_HUNCH_URL` and domain scope explicitly to enable it. No consensus execution change or chain migration. AMID operators must preserve pending signing intent separately because its entrypoint does not wire durable fence restoration. |
| v11.23.13 | REST response deadlines cover the full submission budget: embedding retries, bounded nonce-lease acquisition, consensus commit and response bookkeeping. The default is 285.75 seconds, capped at ten minutes; larger custom settings can outlast the cap. Vault recovery verifies the supplied key before replacing `vault.key`. The memory gate retains v11.23.11 behavior; unreleased evidence checks and the managed local judge are deferred. The cancelled v11.23.12 tag remains unchanged. No consensus execution change, app-version change or chain migration. |
| v11.23.14 | MCP tool calls stay responsive during slow HTTP requests, with bounded concurrency and cancellation. AMID persists and restores identity-only signer-fence intents before serving; retain `signer-fence-intents.sqlite` with the node data. A proven fence completes durable cleanup before reopening its signer, protecting the next recovery record. Disabled REST validator keys cannot sign ordinary writes. Block sync rounds quorum-blocking voting power up correctly. An experimental, default-off managed local judge restores evidence checks. Existing Hunch service URLs must use loopback; invalid or unavailable judges hold proposals for operator review. No consensus execution change, app-version change or chain migration. |
| v11.23.15 | MCP stdio bridges exit after confirmed client-process death, including blocked inherited pipes. CEREBRUM resolves correction lineage by content hash with bounded metadata lookups and visibility checks. Malformed reranking preserves the original recall ordering. Federation and message failures include bounded diagnostics and remedies. Unused transition helpers are removed; consensus lifecycle behavior, app-v28, and the qualified v15 local judge pin remain unchanged. No chain migration. |
| v11.23.16 | MCP task creation defaults to 0.90 through both `sage_remember` and `sage_task`; other remember types keep 0.80. Corrections resolve inherited type before applying the default. Explicit scores from 0 through 1 are preserved and malformed, non-finite or out-of-range values are rejected. `submitted_confidence` reports the signed score on submission receipts, including remember indeterminate outcomes and task replay or committed-but-unconfirmed responses; remember pre-validation skips or rejections and task-status updates omit it. Existing memory scores and task idempotency checks remain intact. No consensus execution change, app-version change or chain migration; app-v28 and the qualified v15 local judge pin remain unchanged. |

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
