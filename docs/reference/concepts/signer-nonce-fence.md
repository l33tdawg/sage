# The signer fence — same-key nonce ordering, and what it cannot prove

**Status: v11.23.10. This document describes a fence that survives the process
that raised it: a restart no longer loses the record, a restored fence re-reads
its own proofs from the chain, and the one shape no proof can settle has an
explicit operator exit that says so. Read "What the fence still cannot do" for
the limits that remain.**

Source of truth: `internal/tx/nonce.go` (the lease),
`internal/tx/nonce_fence.go` (the fence), `cmd/sage-gui/signer_fence_restart.go`
(the restart veto).

The typed committed-memory vote refusal described below is a development change
after v11.23.10; it is not available in that release.

---

## The failure this exists to stop

SAGE's consensus app enforces replay protection on a **per-signer nonce**. A
transaction is admitted only when its nonce is strictly greater than the highest
nonce already **committed** for that signing key
(`internal/abci/app.go`, CheckTx code 4 `nonce too low`, and the mirrored gate in
the consensus path).

So every producer signing with one key must not only *allocate* ascending nonces
— it must make sure they **arrive** in that order. Allocation order is not
arrival order. Two dashboard actions on one key could allocate N and N+1 and
reach CometBFT in the opposite order, and the late N was rejected Code 4. The
operator saw a random subset of a bulk action fail for no visible reason.

`tx.WithNonceLease` fixes the ordinary case: it holds a per-public-key slot
across *allocate → sign → broadcast*, so the emitted order is the allocated
order. Distinct keys never contend.

## The case the lease alone could not survive

The lease releases the slot when `submit` returns. If `submit` returned because
the **RPC connection broke** rather than because consensus answered, the
transaction carrying nonce N **may still be in flight**. Releasing there let the
next caller allocate a higher nonce and commit it — and the abandoned N was
rejected Code 4 when it finally landed. The lease's own error path reintroduced
the exact loss it was built to prevent.

The **signer fence** closes that: an adopter that cannot rule the transaction out
returns its error wrapped in `tx.Indeterminate(err, encodedBytes, resolver)`, and
the lease closes that signing key.

A **panic** out of `submit` is handled by evidence, not assumption. An adopter
calls `tx.RegisterSubmittedTx(sk, encodedBytes, resolver)` immediately before
handing the bytes to the transport (the web broadcast helpers do this at the
exact instant the request is issued). If `submit` then panics, the lease fences
the key on the registered bytes — a panic after the send is an unobserved
outcome, exactly like a broken connection — and reconciliation proves their fate
as usual (`cause=submit_panic`). A panic **before** any registration releases
the slot: nothing was sent, so there is nothing to protect, and fencing would
have reconciliation broadcast a transaction the caller was told never went out.
The registration is cleared on every return, so it cannot leak into the key's
next submission. An adopter that decodes a **definitive, hash-bound rejection
inside `submit`** should also retire the record at that moment with
`tx.ClearSubmittedTx(sk)` (the web commit helper does): nothing is in flight
once consensus has refused the bytes, and a panic in the leftover window before
`submit` returns would otherwise fence a transaction whose fate is already
decided. That fence cannot lift while the refusal's cause persists — re-submitting
CheckTx-refused bytes keeps drawing the same non-permanent-class refusal, and the
index never finds a never-included transaction — and because most CheckTx causes
are **mutable** state (authorization, clearance, membership, domain grants), it
can end worse than a stall: a later re-grant lets reconciliation's re-submit be
admitted and **commit** bytes the caller was already told were rejected, late and
silently. Both hazards argue for the same rule: clear on definitive proof.

### Only proven fate lifts a fence

Exactly two things lift it, and both are statements about **the exact bytes that
went out**:

| Outcome | Lifts? | Why |
|---|---|---|
| The exact tx hash is in a committed block | **yes** | Proof. |
| An indexed block result for that hash, non-zero code | **yes** | Consensus executed those bytes; they had their turn. |
| The signer's committed nonce has **reached** the fenced allocation (equal, or above) | **yes** | Read from the allocator's own floor source. Consensus refuses a nonce that is not strictly above the committed one, so the bytes can never commit again — the same monotonicity argument as the code-4 row below, checked directly instead of through a re-submission. When it is EQUAL the fate label is `spent`: the floor cannot say whether these bytes committed or were overtaken, so no surface may claim the payload was lost. |
| Re-submission refused with **CheckTx code 4** (nonce gate) | **yes** | For a positive nonce, the signer's committed nonce is monotone non-decreasing **on the chain**, so this refusal cannot un-happen there. For the nonce-zero sentinel, permanence instead comes from the already-activated app-v9 fork height: the rule cannot deactivate as height advances. Either way the bytes cannot commit *again*. Two caveats the code itself writes down: (a) positive-nonce code 4 proves supersession **or self-commit** — the gate is `nonce <= committed`, so a transaction that itself committed answers code 4 exactly like one overtaken by a higher nonce, and without the tx index the two are indistinguishable (see triage step 2); (b) a node-level rollback (snapshot restore, state-sync rewind) is the one event that can invalidate these monotonicity assumptions — accepted, because a restore invalidates the fence-holding process anyway. |
| A signed memory vote receives code 13 with codespace `sage/memory-vote-committed/v1`, bound to its exact hash | **yes** | This typed refusal requires a complete canonical target submitted after app-v25 activation and already committed. Its ballot cannot return to proposed through normal consensus transitions. The original vote might itself have committed before the target closed, so the resolver rechecks the index and does not claim that the original vote was lost. Historical, missing, hashless, proposed and deprecated targets do not receive this proof. |
| Re-submission refused with any other untyped CheckTx code | **no** | It refuses *this* submission. The older copy will be judged against whatever state exists wherever it arrives. Plain code 13, codes 3 (nonce lookup), 112 (backpressure), authorization codes and even decode/signature codes (fork-gated) can all flip back. |
| `/tx` says "not found" | **no** | CometBFT indexes a transaction only once it is in a block, so this is indistinguishable from one sitting in a mempool about to commit. |
| Duplicate already pending, mempool full, commit wait timed out | **no** | None is a verdict. |
| Transport / decode / RPC fault | **no** | Not evidence. |
| A deadline or retry budget expiring | **no** | A clock knows nothing about a transaction. |
| The resolver panicking | **no** | Caller code failing is not an answer. Recovered, logged, fence kept, retried. |
| No resolver wired, or no encoded bytes | **no** | Not having a way to ask is not evidence. |

A stale-vote mempool filter and an indeterminate broadcast can otherwise leave
a key held indefinitely: the vote's target commits, the filter refuses its exact
bytes with code 13, and those bytes never reach a block that could supply an
indexed verdict. The typed committed-memory refusal closes that narrow gap.
It does not parse a log message or make general code-13 rejections permanent.
CheckTx remains read-only and FinalizeBlock's existing vote rejection is unchanged.
The proof assumes forward consensus state, like the nonce-floor proof; an
out-of-band canonical-state rewind or local repair is not covered.

Downstream stale-vote filters must run after the typed check and preserve its
codespace. Older nodes that return plain code 13 continue to leave the fence held.
Updating application files alone does not install this runtime fix or settle an
already-running old binary's fence; rollout must preserve its durable intent and
follow the validator maintenance procedure.

A held fence therefore has **no timeout**. That is deliberate. A held fence fails
**loudly** — `tx.ErrSignerFenced` at the call site, `nonce_fence` events in the
log, `/v1/dashboard/health`, Prometheus. A wrongly lifted fence fails
**silently**: some later, unrelated action is rejected Code 4 for reasons nothing
can attribute. Loud beats silent.

### What keeps that survivable: re-submission

While fenced, reconciliation does not wait passively. It **re-submits the
byte-identical signed transaction** to force consensus to answer. Identical bytes
carry the identical nonce and hash, CometBFT de-dupes on hash, and the app's
nonce gate rejects a replay before any handler runs — so this cannot create a
second transaction or apply an effect twice. It converts "unknown" into "known"
in the common case.

**Byte identity is load-bearing.** If anything re-signs, re-stamps a timestamp or
re-allocates a nonce, the hash changes, de-duplication stops, and a genuine
second transaction races the first.

---

## What the fence still cannot do

The failure this section used to describe is closed. It was:

```
nonce N goes out; its fate is never observed        -> fence held, correct
the process restarts (crash, SIGKILL, or an update) -> the fence was GONE
the allocator re-seeded from the highest COMMITTED
  nonce for that key, which is still BELOW N        -> because N never committed
it issued some M in the gap; M committed
the late N finally arrived                          -> rejected Code 4
```

The fence is still in-process STATE, but it is no longer in-process ONLY. Every
submission is now shadowed by a durable record written before the bytes reach
the transport (`RegisterSubmittedTx`, `internal/tx/nonce.go:485`) and retired
only on a proven fate — a committed submit, a hash-bound definitive rejection
(`ClearSubmittedTx`, `internal/tx/nonce.go:537`), or a fence lift. At startup
`RestoreFencesFromIntents` (`internal/tx/nonce_fence_intent.go:148`) re-raises a
fence for every record whose fate was never proven, so the node comes back
REFUSING to sign those keys instead of re-seeding past them.

Two limits remain, and both are deliberate:

- **The durable record carries identity, not payload.** It holds the signer, the
  transaction hash and the nonce. It does not hold the signed bytes, because
  those routinely carry memory content that must not be copied into a plaintext
  table. A restored fence therefore cannot resolve by re-submission the way a
  live one does; it resolves on a proven fate **read from the chain**.
- **A crash between the record and the wire holds the key.** If the process dies
  after registering intent but before the bytes left the machine, the fence is
  raised for a transaction that may never have been sent. That is the safe
  direction — nothing is signed past an unresolved allocation — and it resolves
  through the same proof path, but it can hold a key that has nothing in flight.

### A restored fence re-reads its own proof

A fence restored from durable intent has no bytes to re-submit, so reading the
chain is the only thing that can settle it — and until v11.23.7 nothing did.
The restored path emitted one `fence_restored_waiting_for_proof` event and
parked. That made the lift route below the ONLY way out, and it made the restart
veto — which every update must pass — a permanent refusal for a node whose
transaction the chain had already settled: the fence held, the key refused to
sign, and the update could not be installed. Users hit exactly that and were
told, by the veto message itself, to wait for a reconciliation that had nothing
to do.

Now the node installs a proof reader at boot (`tx.SetFenceProverFunc`,
`internal/tx/nonce_fence.go`, wired from the same CometBFT RPC URL and the same
committed-nonce store the rest of the node uses) and a restored fence calls it on
the same backoff as live reconciliation. Every attempt is recorded, so
`attempts`, `last_cause` and `last_detail` in the health block describe what the
node is doing instead of sitting at zero. `last_cause=no_proof` means the chain
answered and has not settled the transaction; `last_cause=no_fence_prover` means
the hook was never wired — a wiring gap, fixed by installing it, not by waiting.

### Restarting does **not** clear a fence safely

If you have read anywhere — an older comment, an older log line, an older release
note — that restarting SAGE resolves a fence because `tx.SetNonceFloorFunc`
re-seeds the allocator from the highest committed on-chain nonce: **that was
false, and it was false in the direction that loses transactions.** The seed hook
raises the floor to what **committed**. A fence is about what is **in flight**,
which is by definition above that floor. Restarting discards the only record of
it.

Since durable intent landed, "the only record" is a statement about the RECORD,
not about the fence: a fence shadowed by an `signer_fence_intent` row is
re-raised at the next start, so the restart cannot lose it and the allocator
cannot seed past the abandoned nonce. What a restart does cost is the signed
bytes, and with them the re-submission proof — which is why the restored fence
re-reads its own proofs and why the abandon route exists.

The vetoes in `cmd/sage-gui/signer_fence_restart.go` and `web/update_handler.go`
follow exactly that line: a coordinated restart is refused while a fence's
durable record cannot be confirmed, and allowed when every held fence is
shadowed by one. The check fails closed — an unwired, unreadable or empty store
leaves every fence protected — and an allowed restart records
`fence_restart_allowed_durable` so the decision is visible in the log rather than
inferred. Refusing blanket-wide was itself a bug: the node would not take the
restart that installs the fix, and the fix was the only thing that could clear
the fence.

What follows replaces "there is no procedure at all" with two procedures that
state exactly what they are: a lift that requires proof, and an abandon that
requires an operator to accept the loss.

### Recovering a fence on proof

Three proofs are accepted, and nothing weaker. They are read by the NODE, not
asserted by the caller: the committed nonce floor comes from the same store the
allocator seeds from, and the transaction lookup goes to the node's own RPC.

- **Committed or rejected**: the exact recorded hash is in a committed block.
  The transaction's fate is known, the allocation is spent, and the key reopens.
- **Superseded**: a HIGHER nonce for the same signer has committed. Consensus
  refuses a stale nonce, so the fenced allocation can never be included. The key
  reopens and the fenced transaction's payload is permanently lost — which is a
  fact the lift records rather than a fact it hides.
- **Spent**: the signer's committed nonce has reached the fenced allocation (it
  is equal, not above). Consensus refuses a nonce that is not strictly above the
  committed one, so those exact bytes can never commit again and no later
  allocation can overtake anything. This is the same proof the re-submission path
  lifts on when CheckTx answers code 4, read directly from the store — which is
  what makes it reachable on a node whose transaction index is disabled or
  pruned. It is labelled `spent` rather than `superseded` because the nonce floor
  alone cannot say whether these bytes were the transaction that committed or
  were overtaken by another allocation of the same nonce; the index is what
  distinguishes those, and `fate_spent` is the label that admits it.

A missing lookup is NOT a proof and never becomes one here: CometBFT indexes a
transaction only once it is in a block, so a mempool-resident transaction
answers not-found exactly as one does a second before it commits.

Operator surface:

```
POST /v1/dashboard/signer-fence/lift     {"signer": "<hex key or prefix>", "reason": "optional"}
```

It is gated by the same CEREBRUM operator gate as the rest of the operator view
(`handleSignerFenceLift`, `web/signer_fence_lift.go:31`), and it answers `409`
with the reason when the evidence is not there yet. Lifting is also what retires
the durable record; a fence left held keeps its record and comes back on the next
start.

### Abandoning a fence nothing can prove

One shape has no proof and never will: a fence restored from durable intent whose
transaction never committed, on a node where nothing can deliver it back. The
allocation cannot be spent (nothing landed, so nothing advanced the signer's
committed nonce), the hash is in no block, the signed bytes died with the only
process that had them, and the fence therefore refuses to sign AND refuses every
update for as long as the node runs. That is what a process killed mid-submission
leaves behind, and the only exits used to be "wait forever" and hand-editing the
intent table. The daemon route below, the first-boot resolution, and the operator
CLI are the three exits that replaced it.

```
POST /v1/dashboard/signer-fence/abandon
{"signer": "<hex key or prefix>",
 "reason": "<why this decision is being taken>",
 "acknowledge_payload_loss": true,
 "peer_redelivery_acknowledged": true}   // required only when peers are connected
```

This is an OPERATOR DECISION, not a proof, and the request has to be spelled that
way: the acknowledgement and the reason are both required, because the lift it
performs is the only one in SAGE whose fate the chain never settled. It is gated
by the same CEREBRUM operator gate, and every precondition is read by the node
(`tx.ReadFenceAbandonEvidence`, `tx.AbandonUnprovableFence`):

- the fence must be **restored from durable intent** — a live fence still holds
  the exact bytes that went out, so reconciliation keeps re-submitting them and
  abandoning it would throw away a transaction consensus can still settle;
- its durable record must carry a nonce, so the abandoned allocation can be
  reserved: the next allocation for that signer is strictly above it, which is
  what keeps a same-nonce twin from being minted;
- the node's live P2P peer count is **read**, and when it is non-zero the
  request must carry a second acknowledgement, `peer_redelivery_acknowledged`.
  A connected peer is a route the transaction could still take back into this
  node's mempool, and the node cannot see whether any peer ever held those
  bytes — only the operator can. Making the peer count a flat veto instead
  stranded exactly the nodes that need this route (a federated desktop node, a
  validator with a persistent peer): the fence lifted on no proof, the
  automatic route is closed by the peer, and the operator route refused by
  construction;
- the transaction must not be in this node's mempool, and the mempool read must
  be complete (a truncated read cannot certify absence);
- the recorded hash must not be committed or rejected, and the signer's committed
  nonce must not have reached the fenced allocation. Either of those is a
  **proof**, and the refusal says to use the lift route instead.

Every refusal answers `409` with the evidence attached. An accepted decision is
recorded as a `fence_abandoned` event plus
`sage_nonce_fence_resolved_total{fate="abandoned"}` — never as a proven fate, so
a later post-mortem can tell the two apart. The residual the operator accepts is
stated in the response: a peer that holds the transaction from before the
shutdown, or a client that kept the signed bytes, can still deliver it; it
commits if it lands before the signer's next transaction, and is refused as a
replay if it lands after. Verify the effect on-chain before redoing that action by
hand.

### A live fence with no permanent refusal: the decision, and the operator CLI

One shape has neither a proof nor an automatic exit, and it is why the operator
CLI exists. A **live** fence — raised by the process that is still running, so
the signed bytes still exist — keeps re-submitting them, and the re-submission is
refused with a CheckTx code that is **not** the nonce gate: authorization, agent
proof, unknown tx type, a decode or signature code, admission backpressure.
`checkTxRefusalIsPermanent` accepts code 4 only, deliberately (its doc walks the
whole taxonomy), so those bytes are never labelled REJECTED and the fence never
lifts. The supersession proof is unreachable for the same fence, because it needs
the signer's committed nonce to move past the fenced allocation and a fenced key
cannot sign anything new: nothing it signed has committed, and it is not allowed
to sign again. On a node where no peer will ever deliver the transaction back, no
proof can exist and the hold has no exit.

Three exits were considered, and the third is the decision:

1. **Widen the permanent-refusal class.** Rejected on the evidence. Only the
   nonce gate is monotone. Decode, signature and unknown-type codes are
   fork-gated — the same bytes are code 1 on one binary and code 10 on another —
   and authorization, agent proof, clearance and the resource limit are ordinary
   mutable state that can be granted back in a later block. Promoting any of them
   to "permanent" would silently lose a transaction consensus had not refused.
2. **Extend the in-product abandon route to live fences.** Rejected. The daemon
   is itself re-submitting those bytes on a timer, so an in-product abandon races
   its own reconciler, and the reason the route is restricted to restored fences
   stays true for a live one: the bytes still exist and consensus can still
   settle them.
3. **Keep the hold, and give the operator a supported, evidence-checked exit.**
   Chosen, and shipped as `sage-gui fence list` and `sage-gui fence abandon`.

```
sage-gui fence list
sage-gui fence abandon --signer <hex-or-prefix> --reason "<why>" --acknowledge-payload-loss \
    [--peer-redelivery-acknowledged]
```

`fence list` prints every durable record and, for each one, asks the node whether
a proof is readable yet, so "stuck" and "pending" are told apart without a
dashboard. `fence abandon` runs the **same** validator the daemon's route runs
(`tx.RetireFenceIntent` → `validateAbandonEvidence`): a record with a nonce, a
readable peer count (connected peers require the second acknowledgement, exactly
as on the daemon route), a complete mempool read that does not hold the
transaction, no committed or rejected fate for the hash, and an unspent
allocation. An accepted decision reserves the abandoned allocation, deletes the
durable record, and records `fence_abandoned` with `mode=operator_cli` — never a
proven fate, so a post-mortem can still tell the two apart.

The command does not reach into a running process. A daemon that is up keeps its
in-process fence for that key until it restarts; the DURABLE record is what the
next start restores from, so the supported sequence is **retire, then restart**,
and the first boot will not re-raise it. The residual is the one the daemon route
states: a copy of those bytes that still exists somewhere commits if it lands
before the signer's next transaction, and is refused as a replay if it lands
after.

### The same decision at first boot, because the desktop has no operator

The operator routes above are not a recovery path for the product. A CEREBRUM
user whose node came back fenced has a node that refuses every write and an
updater that refuses to restart it, and nobody is going to run a `curl` from a
terminal. The instruction has to be "install the new version", so the node makes
the same evidence-backed decision itself at startup
(`tx.AutoResolveUnprovableFence`, wired in `cmd/sage-gui/node.go`):

- the fence is restored from durable intent (its bytes are gone, so no
  re-submission is possible);
- CometBFT reports `catching_up == false`, so the "no committed fate" answer came
  from a chain that has caught up — a node still replaying or state-syncing may
  simply not have indexed a transaction that DID commit;
- **no peer has been seen while THIS fence was held**, and none is connected
  now. This is a latch anchored to the fence, not to the process: a peer is how
  a transaction gets delivered back, so a fence that has coexisted with a
  connected peer is kept even after that peer disconnects — but a sighting from
  an earlier outage cannot answer for a fence raised afterwards, because that
  transaction did not exist when the peer was seen. (An earlier revision
  anchored on process start, which meant one transient peer switched the
  automatic path off for every future fence for the rest of the run.) A node
  that is P2P-connected now keeps its fence and needs a proof, or an operator
  who can see the topology and takes the acknowledgement above;
- the transaction is in no mempool copy on this node, the recorded hash is in no
  committed block, and the signer's committed nonce has not reached the fenced
  allocation.

The resolution records `fence_abandoned` with `mode=automatic_unprovable` and the
full evidence, and it retires the durable record. It is the same decision the
operator route makes, taken by the node when nothing could ever deliver the
transaction back; the same residual applies, and the same allocation is
reserved above the abandoned nonce.

### What this release does about a restart while fenced

The dominant road into that hole is not a crash — it is **this node's own updater
deciding to restart**. That one we control, so:

- **Coordinated restarts are VETOED while any signing key is fenced.**
  `tx.RestartVetoReason()` returns the operator-facing reason;
  `cmd/sage-gui/signer_fence_restart.go` enforces it on both restart entry points
  (`prepareAndQueueRestart` and the updater's `RequestRestartPrepared`).
  **It fails closed**: an unwired or panicking guard refuses the restart, and so
  does a fence whose durable record cannot be read. A fence whose record CAN be
  read does not refuse the restart — the record re-raises it at the next start —
  and the decision is logged as `fence_restart_allowed_durable`.
- **The veto is re-checked at drain time, not only at request time.** A check
  made only when the restart is requested is a time-of-check race: the drain's
  own force-close of in-flight HTTP handlers is precisely how an indeterminate
  outcome — a new fence — gets manufactured *after* the only veto that ran. So
  before committing to the drain, the node quiesces signing, waits for every
  in-flight and queued submission to finish (`tx.WaitForSigningIdle` — at that
  point every fence that was going to exist already exists), and re-evaluates
  the veto **while the restart can still be abandoned**. On a veto the restart
  is abandoned, signing resumes, and the node keeps serving so reconciliation
  can resolve the fence. A last-resort post-drain check covers the one adoption
  path that skips the ordered re-check — and it must be claimed precisely: it
  fails the shutdown gate, which **aborts the version transition and makes the
  loss loud** (the veto reason naming the fenced key lands in the log and the
  shutdown error). It **cannot save the fence**: at that point the process
  exits or execs a recovery binary either way, and the in-memory fence is lost
  with it. The pre-drain re-check is the only guard that actually preserves a
  fence, which is why it must never be weakened as "redundant with the
  tripwire".
- **Signing quiesces for every teardown, not only a committed restart**
  (`tx.QuiesceSigningForRestart`), so no new transaction is signed into a
  teardown — the most likely moment for a submission to end with an unobserved
  outcome. The quiesce is re-checked **after** a caller acquires its lease slot
  and after a fence wait, so callers already queued behind a slow commit when
  the restart begins are refused (`ErrSigningQuiesced`) instead of signing into
  the drain. An **ordinary exit** (a signal, or a serve error that is not a
  scheduled restart) drains the same way before anything force-closes a
  listener: signing is quiesced, `tx.WaitForSigningIdle` is given a small
  bounded window (five seconds) for submissions already in flight, and signing
  is **not** resumed afterwards — the process is leaving, and the point is to
  stop manufacturing fences, not to resolve them. A submission that outlasts
  the window is the fail-closed case and is covered by the durable intent and
  the restored fence.

A `kill -9`, a power cut, or a crash during the original RPC no longer loses the
RECORD — durable pre-broadcast intent landed in v11.20.4, so the fence is
re-raised at startup instead of the allocator re-seeding past it. What a crash
loses is the signed BYTES, and with them the cheapest proof: a restored fence
cannot re-submit, so it waits on the proof reader and, if no proof can exist, on
an operator decision. A node built WITHOUT an intent store still has the old
residual in full — that is what
`TestRestartWhileFencedLosesTheTransaction` in
`internal/tx/nonce_fence_safety_test.go` walks and asserts, so this document
cannot quietly stop being true about the unprotected deployment.

### The honest scope claim

> Same-key nonce inversion is eliminated **within a running process, for every
> producer that goes through the lease**. In the daemon that includes the
> dashboard, `api/rest`, federation, voter, and upgrade-watchdog producers.
> Across a restart, the RECORD survives (durable intent) rather than the bytes:
> the key comes back refusing to sign until a fate is proven, or until an
> operator abandons it with the evidence recorded. A standalone CLI is a
> separate process: its lease protects its own work but cannot coordinate with a
> concurrently running daemon using the same key, and that cross-process
> exposure remains.

Any stronger claim (lifecycle-wide elimination, "restart to recover") is wrong.

---

## Operating a fenced node

### How you find out

**Log** — one structured line per transition on stderr:

```
SAGE: nonce_fence event=fence_set signer=3d73cdbdffaacac7… tx_hash=1B5B9C… nonce=1770000000000000001 cause=transport note="…"
SAGE: nonce_fence event=reconcile_retry signer=3d73cdbdffaacac7… fence_age=2m1s attempt=37 cause=pending detail="…"
SAGE: nonce_fence event=fence_held  signer=3d73cdbdffaacac7… held_for=5m0s attempts=91 last_cause=pending
SAGE: nonce_fence event=fence_lift  signer=3d73cdbdffaacac7… fate=committed held_for=5m2s attempts=92
```

Events: `fence_set`, `fence_restored`, `fence_restored_waiting_for_proof`,
`reconcile_retry`, `resolver_panic`, `submit_panic`, `fence_abandoned`
(`mode=automatic_unprovable` for the startup resolution),
`fence_restart_allowed_durable`, `fate_committed`, `fate_rejected`, `fate_spent`,
`fate_abandoned`, `fence_lift`, `fence_held`, `signing_quiesced`,
`signing_resumed`, `fence_dropped_at_shutdown`. Retry and panic lines are
rate-limited, so a long hold cannot flood the log. The last one is the terminal
record: a process exit (signal, serve error, or a failed restart gate that
execs a recovery binary) discards every held fence, and this line — one per
fence, with signer prefix, hash, nonce, age and attempts — is what lets a later
Code 4 loss be traced back to the exit that dropped its record.

**What is never logged**: the signing key's file path, the encoded transaction,
or any raw error. Broadcast errors from `net/http` embed the request URL, and a
broadcast URL is `/broadcast_tx_commit?tx=0x<the entire signed transaction>` —
so the fence keeps only a **typed cause category** (`transport`, `timeout`,
`canceled`, `decode`, `rpc`, `pending`, `resolver_panic`, `submit_panic`,
`no_resolver`, `no_encoded_tx`) plus the transaction's **hash** as its own field.

**Status** — `GET /v1/dashboard/health` carries a `signer_fences` block. Public
callers get `active` and `oldest_age_seconds`; an operator session — or, on an
**unencrypted** node, the local loopback dashboard (the same read-level gate as
the rest of the operator status view, `isCEREBRUMReadRequest`) — also gets
per-fence `signer`, `tx_hash`, `nonce`, `held_seconds`, `attempts`, `cause`,
`resolution`, `last_cause` and `last_detail`. Everything in it is public-on-chain
data, but do not mistake the gate for credential-only access.

**Dashboard** — CEREBRUM's System Status panel renders that block
(`web/static/js/signer-fences.js`): a row that appears only while a key is held,
the server's explanation of why, and one line per fence naming its `resolution`
with the exit that belongs to it — reconciling means the identical bytes are
still being re-submitted, waiting-on-proof means the chain or an operator has to
settle it. The panel carries the same copy rule the rest of this document does:
it never suggests restarting, because a restart discards the fence rather than
settling it. Before this row existed the dashboard was silent about a hold,
which is how the reported incident read from the outside as "reads are fine,
writes time out, no error". The `nonce` crosses the wire as a decimal **string**
because a nanosecond allocation exceeds JavaScript's safe integer range, and a
rounded nonce is a wrong answer where the operator compares it against the chain.
The panel also keeps the **last resolution** visible after a hold has ended
(`signer_fences.last_resolution`, operator-only like the per-fence rows): a fence
that lifted leaves no held-fence row behind, and "it resolved itself" is the
outcome worth showing. Both paths record it — the proof lifts (`committed`,
`rejected`, `spent`) and the operator or automatic decisions taken without a
proof (`abandoned:<route>`) — so the fates stay as distinguishable in the UI as
they are in the log.

`resolution` is the field that says **how a fence can end**, and it exists
because "the fence is held" without it is ambiguous in a way that misled
callers: `reconciling` means the fence still holds the exact bytes that went out
and is re-submitting them until consensus answers, so it clears itself;
`proof_or_operator` means it was restored from durable intent and its signed
bytes did not survive, so it lifts only on a proof read from the chain or on an
explicit operator abandon. The block's `explanation` is rendered from the same
distinction, so it no longer promises self-healing for a fence that has nothing
to re-submit.

For a `proof_or_operator` fence, `last_detail` also carries the automatic
route's own refusal when it declined — "the node is still catching up", "N
peer(s) are connected", "a peer was connected while this fence was held". Those
reasons used to be computed and then discarded, so a node whose self-heal was
correctly refusing on evidence read identically to one whose self-heal was
broken; the status row now answers "why is this not clearing itself?" directly.

**Metrics** — `sage_nonce_fences_active`,
`sage_nonce_fence_oldest_age_seconds`, `sage_nonce_fence_indeterminate_total`,
`sage_nonce_fence_reconcile_failures_total{cause}`,
`sage_nonce_fence_resolved_total{fate}`. Alarm on
`sage_nonce_fence_oldest_age_seconds` climbing without bound.

### What a caller sees

`tx.ErrSignerFenced` means **nothing was signed or sent**. It is never a
consensus rejection: there is no verdict to report and nothing to undo. HTTP
surfaces map it to **503 with `Retry-After`**, never to a rejection status.
`tx.ErrSigningQuiesced` means the same thing during a restart.

**The refusal is immediate.** The request-serving paths (`api/rest`'s
`submitConsensusTx`, `web/rbac_signing.go`'s two broadcast helpers) ask
`tx.FenceForSigner` before they take the lease, because the lease's fence wait
blocks until the CALLER's deadline — which is how a fenced node looked to agents
as "writes timed out, no error other than the timeout". They now answer
immediately, name the transaction the key is held on, and say that nothing was
sent. Background producers still wait on the fence; a request that has a person
or an agent on the other end does not.

The other side of the same coin is a submit whose outcome this process could not
observe. That is **not** a failure and must not be reported as one: REST answers
`202` with `"status":"indeterminate"`, the exact `tx_hash` of the bytes that went
on the wire, the allocated `nonce`, and `"retryable":false` (see
`tx.IndeterminateDetails`). Withholding the hash is what forced callers to
guess, and a caller that "retries on error" here re-signs a **second**
transaction on top of one that may already be committed. Before this contract,
the ambiguity arrived as the same opaque 500 as a genuine internal fault.

### Triage

1. Read `last_cause`. `no_resolver` and `no_fence_prover` are wiring bugs —
   install one with `tx.SetTxResolverFunc` / `tx.SetFenceProverFunc`; both are
   re-read on every attempt, so a late install rescues fences that are already
   held. `transport` means the **connection to the node failed** — check the
   CometBFT RPC endpoint, and the fence resolves itself once it is reachable.
   `rpc` is the opposite of unreachable: the node **is answering**, with a
   decoded JSON-RPC error envelope — read `last_detail` for what it said (e.g. a
   persistent internal error on `/tx`) instead of chasing connectivity.
   `pending` means the node is answering and the transaction is genuinely
   unresolved — this normally clears on its own. `no_proof` means the proof
   reader answered and neither accepted proof holds yet; that is the state a
   restored fence sits in until the chain settles it, and `attempts` climbing
   with `no_proof` means the node is asking and the answer is "not yet".
2. Compare the fence's `nonce` against the signer's committed nonce on-chain.
   If the chain is at or above it, the next re-submission gets CheckTx code 4
   and the fence lifts. Read the lifted fate with care: **code 4 proves
   supersession _or_ self-commit** — a transaction that itself committed
   answers code 4 too. The resolver re-checks `/tx` once before labeling, but
   on a node whose indexer is disabled (`indexer="null"`) or pruned,
   `fate_rejected` can describe a transaction that actually **committed**.
   Verify the effect on-chain before redoing the action by hand, or you can
   apply it twice.
3. If `last_detail` keeps reporting `no_proof` with `attempts` climbing, and the
   fence is a **live** one whose re-submission is refused by anything other than
   the nonce gate, no proof can arrive: take the operator exit in "A live fence
   with no permanent refusal" (`sage-gui fence list`, then `fence abandon` with a
   reason and the acknowledgements).
4. Do **not** restart to clear it. A restart alone re-raises the fence from its
   durable record — that is the whole point of the record. Only after the CLI has
   retired it does a restart apply the decision.

### Tuning

`SAGE_TX_FENCE_ATTEMPT_MS` (per-attempt deadline, default 30s),
`SAGE_TX_FENCE_RETRY_MS` / `SAGE_TX_FENCE_RETRY_MAX_MS` (retry backoff, default
2s doubling to 60s), `SAGE_TX_FENCE_REPORT_MS` (how often a held fence
re-reports, default 60s). **None of these can lift a fence** — they only decide
how often this process asks and how often it complains.

---

## For adopters

Wrap **only** the genuinely ambiguous returns:

```go
err := tx.WithNonceLease(ctx, key, func(nonce uint64) error {
    ptx.Nonce = nonce
    // …sign, encode…
    if sendErr := broadcast(ctx, encoded); sendErr != nil {
        if isAmbiguous(sendErr) {                       // transport / decode / RPC envelope
            return tx.Indeterminate(sendErr, encoded, tx.CometTxResolver(rpcURL))
        }
        return sendErr                                  // definitive: releases normally
    }
    return nil
})
```

- **Never** mark a sign/encode failure or a real CheckTx/FinalizeBlock rejection
  indeterminate. Those are definitive, nothing is in flight, and fencing on them
  turns every ordinary validation failure into an outage for that key.
- **Always** pass the exact bytes you put on the wire. Reconciliation both
  identifies *and* re-submits by them; a fence raised without them can never be
  proven.
- Calling `Indeterminate` means "push this until consensus accepts or refuses
  it". A submission you saw fail can still commit minutes later from the
  reconciliation goroutine. That is the correct semantics — the allocated nonce
  must be consumed or proven dead before a higher one may be issued.
- **Every adopter sharing a signing key must fence.** One unfenced producer
  reopens the race for that key no matter how careful the others are — and the
  node's priv-validator key is shared across `web/`, `api/rest/`,
  `internal/federation/`, `internal/voter/` and the upgrade watchdog, so "the
  handler I changed" is never the whole set.

  This is a whole-repo property, not a per-package one, and it is checkable:

  ```
  grep -rn 'tx.MonotonicNonce' --include='*.go' | grep -v _test.go
  ```

  Every hit is a producer allocating a nonce outside the lock that makes it
  valid. `MonotonicNonce` is only safe where a key has a single submitter or
  submissions for it are already serialized by something else; anywhere else it
  is the original defect. Treat a non-empty result as an open item, not a
  finding about the file you happen to be reading.
