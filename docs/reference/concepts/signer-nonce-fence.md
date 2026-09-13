# The signer fence — same-key nonce ordering, and the hole that is still open

**Status: v11.19.20. This document states a KNOWN RESIDUAL that this release does
not close. Read the "What is still broken" section before you rely on anything
here.**

Source of truth: `internal/tx/nonce.go` (the lease),
`internal/tx/nonce_fence.go` (the fence), `cmd/sage-gui/signer_fence_restart.go`
(the restart veto).

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
| Re-submission refused with **CheckTx code 4** (nonce gate) | **yes** | For a positive nonce, the signer's committed nonce is monotone non-decreasing **on the chain**, so this refusal cannot un-happen there. For the nonce-zero sentinel, permanence instead comes from the already-activated app-v9 fork height: the rule cannot deactivate as height advances. Either way the bytes cannot commit *again*. Two caveats the code itself writes down: (a) positive-nonce code 4 proves supersession **or self-commit** — the gate is `nonce <= committed`, so a transaction that itself committed answers code 4 exactly like one overtaken by a higher nonce, and without the tx index the two are indistinguishable (see triage step 2); (b) a node-level rollback (snapshot restore, state-sync rewind) is the one event that can invalidate these monotonicity assumptions — accepted, because a restore invalidates the fence-holding process anyway. |
| Re-submission refused with any other CheckTx code | **no** | It refuses *this* submission. The older copy will be judged against whatever state exists wherever it arrives. Codes 3 (nonce lookup), 112 (backpressure), authorization codes and even decode/signature codes (fork-gated) can all flip back. |
| `/tx` says "not found" | **no** | CometBFT indexes a transaction only once it is in a block, so this is indistinguishable from one sitting in a mempool about to commit. |
| Duplicate already pending, mempool full, commit wait timed out | **no** | None is a verdict. |
| Transport / decode / RPC fault | **no** | Not evidence. |
| A deadline or retry budget expiring | **no** | A clock knows nothing about a transaction. |
| The resolver panicking | **no** | Caller code failing is not an answer. Recovered, logged, fence kept, retried. |
| No resolver wired, or no encoded bytes | **no** | Not having a way to ask is not evidence. |

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

## What is still broken

> **The fence is IN-PROCESS ONLY. It does not survive a restart or a crash.**

Concretely:

```
nonce N goes out; its fate is never observed        -> fence held, correct
the process restarts (crash, SIGKILL, or an update) -> the fence is GONE
the allocator re-seeds from the highest COMMITTED
  nonce for that key, which is still BELOW N        -> because N never committed
it issues some M in the gap; M commits
the late N finally arrives                          -> rejected Code 4
```

### Restarting does **not** clear a fence safely

If you have read anywhere — an older comment, an older log line, an older release
note — that restarting SAGE resolves a fence because `tx.SetNonceFloorFunc`
re-seeds the allocator from the highest committed on-chain nonce: **that was
false, and it was false in the direction that loses transactions.** The seed hook
raises the floor to what **committed**. A fence is about what is **in flight**,
which is by definition above that floor. Restarting discards the only record of
it.

There is no flag, no override and no operator procedure to force a fence open,
because there is no safe one.

### What this release does about it

The dominant road into that hole is not a crash — it is **this node's own updater
deciding to restart**. That one we control, so:

- **Coordinated restarts are VETOED while any signing key is fenced.**
  `tx.RestartVetoReason()` returns the operator-facing reason;
  `cmd/sage-gui/signer_fence_restart.go` enforces it on both restart entry points
  (`prepareAndQueueRestart` and the updater's `RequestRestartPrepared`).
  **It fails closed**: an unwired or panicking guard refuses the restart.
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
- **Signing quiesces once a restart is actually committed**
  (`tx.QuiesceSigningForRestart`), so no new transaction is signed into a
  teardown — the most likely moment for a submission to end with an unobserved
  outcome. The quiesce is re-checked **after** a caller acquires its lease slot
  and after a fence wait, so callers already queued behind a slow commit when
  the restart begins are refused (`ErrSigningQuiesced`) instead of signing into
  the drain.

A `kill -9`, a power cut, or a crash during the original RPC can still lose the
fence. **Closing that needs durable pre-broadcast intent** — the exact bytes and
hash recorded *before* the send, cleared only on a proven fate, reloaded and
reconciled *before* any nonce is allocated on startup. That is persistence work
and is **not in v11.19.20**.

The residual is covered by an executable test:
`TestRestartWhileFencedLosesTheTransaction` in
`internal/tx/nonce_fence_safety_test.go` walks the sequence above and asserts the
uncomfortable half, so this document cannot quietly stop being true.

### The honest scope claim

> Same-key nonce inversion is eliminated **within a running process, for every
> producer that goes through the lease**. In the daemon that includes the
> dashboard, `api/rest`, federation, voter, and upgrade-watchdog producers. A
> standalone CLI is a separate process: its lease protects its own work but
> cannot coordinate with a concurrently running daemon using the same key.
> Cross-process, cross-restart, and crash exposure remain until durable
> pre-broadcast intent lands.

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

Events: `fence_set`, `reconcile_retry`, `resolver_panic`, `submit_panic`,
`fate_committed`, `fate_rejected`, `fence_lift`, `fence_held`,
`signing_quiesced`, `signing_resumed`, `fence_dropped_at_shutdown`. Retry and
panic lines are
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
`last_cause` and `last_detail`. Everything in it is public-on-chain data, but
do not mistake the gate for credential-only access.

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

### Triage

1. Read `last_cause`. `no_resolver` is a wiring bug — install one with
   `tx.SetTxResolverFunc`; it is re-read on every attempt, so a late install
   rescues fences that are already held. `transport` means the **connection to
   the node failed** — check the CometBFT RPC endpoint, and the fence resolves
   itself once it is reachable. `rpc` is the opposite of unreachable: the node
   **is answering**, with a decoded JSON-RPC error envelope — read
   `last_detail` for what it said (e.g. a persistent internal error on `/tx`)
   instead of chasing connectivity. `pending` means the node is answering and
   the transaction is genuinely unresolved — this normally clears on its own.
2. Compare the fence's `nonce` against the signer's committed nonce on-chain.
   If the chain is at or above it, the next re-submission gets CheckTx code 4
   and the fence lifts. Read the lifted fate with care: **code 4 proves
   supersession _or_ self-commit** — a transaction that itself committed
   answers code 4 too. The resolver re-checks `/tx` once before labeling, but
   on a node whose indexer is disabled (`indexer="null"`) or pruned,
   `fate_rejected` can describe a transaction that actually **committed**.
   Verify the effect on-chain before redoing the action by hand, or you can
   apply it twice.
3. Do **not** restart to clear it. See above.

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
