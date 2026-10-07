# Recall-backed compaction

When an agent harness (Claude Code, and eventually others sharing its transcript
format) runs low on context it **compacts** the conversation — it summarises and
discards older turns. The summary is lossy and model-dependent: the exact command,
the precise instruction, the specific number are gone.

Recall-backed compaction makes that boundary lossless for the conversation. On a
full-compaction event, SAGE captures the turns being evicted **verbatim** as
governed memories, and a later session restores them in order — so compaction
becomes *recall-backed* instead of *summarise-and-forget*.

It is **default-off** and requires an explicit, versioned acknowledgment before any
capture, because durable verbatim transcript retention is a new data flow.

## Enabling it

```
sage-gui nevercompact enable     # shows the disclosure, records consent
sage-gui nevercompact status     # shows whether capture is on, and how
sage-gui nevercompact disable    # stops future capture (does NOT delete records)
sage-gui nevercompact purge [--thread ID | --all]   # deprecate captured records
```

`enable` prints exactly what is captured, its classification, its retention and
deletion behavior, that governed records may persist beyond your local transcript,
that consent is revocable but not retroactive, and the acknowledgment version — and
records consent only after you type `yes`.

For **headless or centrally-managed** deployments that have no interactive consent
surface, set `SAGE_NEVERCOMPACT=1` in the hook environment instead. Capture stays
default-off; the env var is the operator's opt-in.

Enabling is separate from ordinary updates: upgrades remain silent and automatic,
but turning on a new durable transcript data flow does not.

## What is captured

* Only **user and assistant turns** evicted by *full* compaction. Tool-call inputs
  and outputs are never captured — they are re-derivable, and can carry secrets.
* Micro-compaction (which only clears re-derivable tool results) and `/clear`
  (an explicit "forget this") are out of scope by design.
* Each capture writes a small number of **chunked** memories tagged
  `nevercompact`, `nc:thread:<thread-id>`, and `ncchunk:<content-hash>`, into the
  caller's home domain, at classification **Confidential** by default (never
  Public; override with `SAGE_NEVERCOMPACT_CLASSIFICATION`).

## How it is safe

* **Proven source.** The transcript is opened by a descriptor-relative, no-follow
  component walk anchored at the trusted root: every directory below the root is
  opened with no-symlink semantics relative to its already-verified parent, so an
  ancestor swapped to a symlink between validation and open fails closed instead of
  redirecting the read out of the root. The exact descriptor that is validated
  (regular file, owned by you, size-capped) is the one that is read.
* **Proven records.** Each chunk carries a deterministic identity over the thread,
  the exact source `(seq, part)` span, and its bytes. A chunk is looked up before
  it is written, so an ambiguous timeout never creates a duplicate; progress
  advances only when a chunk reconciles to **committed**, never on an HTTP accept;
  and recall deduplicates by `(seq, part)` as a backstop.
* **Whole-transcript, append-stable.** Every row is scanned each run (the tail is
  never cut off at a per-run cap); only units after the persisted cursor are
  captured, and chunk boundaries are immutable once submitted — so appending to a
  resumed transcript never re-groups or duplicates already-captured turns, and the
  full tail is captured across successive compactions.
* **Byte-lossless.** An oversize turn is split from its raw bytes exactly once into
  ordered parts; each part carries a stable ordinal in its identity and recall
  order, so repeated identical fragments are preserved and the original turn
  reconstructs byte-for-byte. Undecodable conversational content and unparseable
  rows become **visible capture gaps**, distinct from intentionally excluded
  tool payloads. Progress never advances past bytes it did not capture.
* **Bounded.** One deadline is set at command entry and propagated through lock
  acquisition and every network call — capture and the complete recall path. The
  transcript is streamed, not read whole; per-invocation unit count and total bytes
  are capped, deferring the rest to the next compaction.
* **Crash- and concurrency-safe.** Per-thread progress is written atomically
  (temp + fsync + rename) under a per-thread lock; a rejected record rolls its
  units back for recapture, so a crash or rewrite cannot corrupt committed history.
* **One thread identity.** Progress and record tags share a single durable thread
  identity derived from the transcript's first row; capture is refused when it
  cannot be proven or when the payload session is absent from the transcript.

## Restoring a thread

On SessionStart for a resumed thread, SAGE restores the **complete** captured
thread verbatim, in order — not a recency sample — before the ordinary recent-memory
prefetch. Nothing is printed when nothing was captured.

## Retention and deletion

Captured chunks are retained until you purge them. SAGE has **no hard delete**:
`sage-gui nevercompact purge` issues a governed deprecation, so the chunks are
hidden from recall and search, but an audit row is retained on-chain and — because
memory is governed — records may persist beyond your local transcript and on nodes
you federate with. `disable` stops future capture but does not delete prior
records; purge them explicitly.

## Environment variables

| Variable | Default | Meaning |
|---|---|---|
| `SAGE_NEVERCOMPACT` | unset (off) | Headless/centrally-managed opt-in (`1`/`true`/`yes`/`on`). |
| `SAGE_NEVERCOMPACT_CLASSIFICATION` | `2` (Confidential) | Clearance for captured chunks, clamped to `1..4` (never Public). |
| `SAGE_NEVERCOMPACT_TRANSCRIPT_ROOT` | `~/.claude/projects` | Trusted root that transcript paths must resolve within. |
| `SAGE_NEVERCOMPACT_CHUNK_BYTES` | `6000` | Target chunk size, clamped to `1000..60000`. |
| `SAGE_NEVERCOMPACT_MAX_UNITS` | `400` | Capture units submitted per compaction, clamped to `1..4000`; the rest defers to the next compaction. |
| `SAGE_NEVERCOMPACT_BUDGET_MS` | `3500` | Wall-clock budget per capture, clamped to `500..4500`. |
| `SAGE_NEVERCOMPACT_RECALL_BUDGET_MS` | `8000` | Wall-clock budget for the complete thread recall, clamped to `1000..20000`. |

## Limits

Claude-family transcripts only for now; other harnesses need their own transcript
parser. Capture covers full compaction, not micro-compaction or `/clear`.
