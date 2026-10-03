# Memory gate (optional, experimental)

The built-in voter checks trust the **author's self-declared confidence**. An
agent that stores a remark about its own session — "the attachment was lost;
the user must re-send the numbers" — as a 0.9-confidence fact gets it
committed, and later sessions recall it as if it were true of the world.

The memory gate lets a node ask a local judge a calibrated
question before it votes on a proposed memory: *is this lasting knowledge, or a
statement about the conversation it came from?* The judge returns a
probability, so the decision is a number with a threshold, not a guess. The
bundled adapter uses [Hunch](https://github.com/ihubanov/hunch), which reads
one constrained token's logprobs from a model you run.

It is **off by default** and changes nothing in consensus, the transaction
format or recall.

## The check

> Should this be stored as lasting memory: does it state something about the
> world, or a standing rule or method, that stays true outside the conversation
> it came from?

It names its look-alikes: facts about people, places, systems or events and
standing rules or methods are lasting; a lost or unreadable message, truncated
context, something that could not be done just now, or a request to re-send are
not. The wording is versioned (`sage-lasting/N`) and recorded with every
verdict.

## What the node does with the answer

Judging never happens inside the voter loop. The loop reads a **stored**
verdict; a proposed memory without one is handed to a small background
evaluator (bounded workers and queue) and simply not voted on that tick. A slow
or hung judge therefore cannot delay other votes, upgrade voting or backlog
telemetry.

The built-in checks (dedup, quality, consistency) are **always evaluated
fresh**, and the gate can only narrow their outcome:

| Built-in checks | Stored semantic verdict | The node votes |
|---|---|---|
| reject | anything | **reject** (built-in reason) |
| accept | none yet | nothing this tick — judged in the background |
| accept | pass (≥ 0.9) | accept |
| accept | reject (< 0.5) | **reject**, with the probability in the rationale |
| accept | held (in between) | nothing, until the operator decides in the review queue |
| accept | judge failed | nothing; held for operator review |

Verdicts are cached **per judge version**: check wording, policy, configured
models, the judge **service** (a credential-free hash of its scheme, host and
path — with no models configured the service's default model decides) and an
optional operator tag, `SAGE_HUNCH_JUDGE_REVISION`, to bump when a service's
default model changes behind the same URL. Changing any of them re-judges
pending memories. An operator's review decision
settles only the semantic question — the built-in checks still run when the
node votes — and is final for that memory across judge versions: a human
decision outranks a later judge.

## Review queue

**Settings → Memory gate** in the dashboard shows whether the gate is on, what
it sends where, and every memory it is holding, with *Keep as memory* / *Reject*
buttons. With the gate off it says so, and nothing is held. The same data is at
`GET /v1/dashboard/memory/review-queue` (operator-only). Reads go through the
dashboard's normal projection-integrity path (quarantined records are omitted);
a memory whose content cannot be decrypted is listed as unavailable, without
content, and cannot be decided until it can be read.

The queue pages past rows it may not show (internal or quarantined): one
request walks raw held rows until its visible page is full, the rows run out,
or the dashboard's interactive scan budget is spent, and returns `next_cursor`
when rows remain (*Load more* in the panel). The opaque cursor identifies the
last scanned row under the current judge version, so deciding earlier rows does
not skip memories on the next page. Refresh the queue if the judge version changes.
A decision is revalidated on the
server immediately before it is stored — the content must still be readable
and the record must pass the same integrity check — so a stale page or an API
client cannot decide a memory it could not have reviewed.

## Attach source evidence (v11.23.14)

For non-task memories, MCP `sage_remember` accepts an optional `evidence` string
(a quote, log line or document excerpt, at most 32 KiB). The bridge uploads it
separately before submitting the memory. REST clients first sign
`POST /v1/memory/evidence` with `{"evidence":"source text"}`, then include the
returned `evidence_id` in the same agent's signed `POST /v1/memory/submit`.
Only that random ID enters the submission proof; the source text stays in the
node's SQLite store. The Python SDK v11.23.14 does not yet expose this upload
or the `evidence_id` argument. See the [REST contract](rest-api.md#post-v1memoryevidence)
and [MCP parameter reference](mcp-tools.md#sage_remember)
(`api/rest/memory_evidence.go`; `internal/mcp/tools.go`, `toolRemember`).

With support judging enabled, every support judge must reach 0.9 to pass.
If all return below 0.5, support is rejected; other combinations are held for
review. This support check runs alongside lasting-memory judging and can only
narrow its outcome. It checks the claim's time frame and attribution as well as
its wording. A memory without evidence gets only the lasting check; missing or
unreadable claimed evidence holds the memory (`internal/voter/gate.go`,
`evaluate`, `combineSupport`).

Evidence uploaded but not claimed expires after one hour, with a cap of 64
unclaimed uploads per agent. Claimed text is retained while the memory is
proposed and pruned after its final decision. An unresolved claim with no memory
after 24 hours loses its text but keeps an expired marker, so a late submission
is held for review (`internal/store/sqlite_gate.go`, `PruneMemoryEvidence`).

## What leaves the node

Only the **text** of proposed memories in scope, plus their caller-supplied
evidence when the support check is enabled, is sent to the configured local
judge — no ids, domains, authors or other metadata. Judge transports accept
only loopback URLs, bypass environment proxies, and refuse redirects. A
configured judge that is unavailable or returns an unreadable verdict holds
the memory for review; it cannot fall back to automatic acceptance.

For the Hunch service adapter, scope is controlled by
`SAGE_HUNCH_INCLUDE_DOMAINS` (only these domains) and
`SAGE_HUNCH_EXEMPT_DOMAINS` (never these); the node logs the scope and the
judge URL at startup, and the review screen repeats it. Content that cannot be
produced in plaintext (a locked vault, a decryption failure) is never sent.

## Judges are pluggable

The voter depends only on a provider-neutral interface —
`LastingProbability(ctx, content) (float64, error)`. Hunch
([github.com/ihubanov/hunch](https://github.com/ihubanov/hunch)) is supplied as
one adapter (`internal/hunch.LastingJudge`); any other judge can implement the
same method.

## Several judges

`SAGE_HUNCH_MODELS=model-a,model-b` asks each model concurrently; the first
**leads**. With the default policy (`lead`) a memory passes when the lead is at
or above 0.9 and no other judge clearly objects (below 0.5), and fails only when
every judge is below 0.5. `SAGE_HUNCH_POLICY=all` requires every judge to reach
0.9. Judges from different model families have different blind spots; a second
judge is a cheap guard against the first's confident mistakes.

## Program-written memories

Catalog entries and other records written by programs, not distilled from a
conversation, should not be asked this question. List their domain prefixes in
`SAGE_HUNCH_EXEMPT_DOMAINS`; the built-in checks apply to them as before.

## Managed local judge

Set `SAGE_LOCAL_JUDGE_MODEL=sage-memory-judge:v15` to use the pinned judge
through SAGE's managed Ollama. Its GGUF download is verified by SHA-256 before
registration. Before sending content, the reader binds the managed tag to the
pinned registered GGUF blob reported by `/api/show` and refuses mismatched weights,
cloud aliases or models without local GGUF metadata. SAGE starts its managed
Ollama with `OLLAMA_NO_CLOUD=1`, overriding an inherited false setting.

The current managed adapter judges every domain. `SAGE_HUNCH_INCLUDE_DOMAINS`,
`SAGE_HUNCH_EXEMPT_DOMAINS` and `SAGE_HUNCH_EVIDENCE` configure only the Hunch
service adapter; they do not restrict managed local judging. The managed
adapter always checks supplied evidence (`cmd/sage-gui/local_judge.go`,
`localJudgeFromEnv`).

The reader requests thinking off and requires an actual first-token label and
valid token logprobs. Missing probabilities, thinking output, an unavailable
model or unreadable evidence hold the memory for review. An operator's review
decision remains final, and the built-in rejection checks still apply.
`SAGE_LOCAL_JUDGE_DEBIAS=1` asks both answer orders; changing this setting
invalidates cached verdicts. `SAGE_LOCAL_JUDGE_TIMEOUT` bounds the per-memory
judge budget (default `60s`), and `SAGE_LOCAL_JUDGE_REVISION` invalidates the
cache after changing model weights behind the same name.

The public seed set has been independently qualified with the real Go reader
and SAGE's pinned Ollama v0.31.1 on darwin/arm64, with external networking
blocked. Support precision was 0.9697 (one accepted trap), and lasting precision
was 1.0. This is experimental: private fresh holdouts, other platforms and
multilingual behavior have not been independently remeasured. See the
[qualification report](../../bench/judge-qualify/reports/sage-judge-v15-sage-runtime.md)
for per-item results and limits.

## Configuration (personal node)

| Variable | Meaning |
|---|---|
| `SAGE_LOCAL_JUDGE_MODEL` | Managed Ollama model; `sage-memory-judge:v15` selects the pinned build. Installation runs in the background. |
| `SAGE_LOCAL_JUDGE_DEBIAS` | Exactly `1` asks both answer orders and averages probabilities (twice the calls). |
| `SAGE_LOCAL_JUDGE_TIMEOUT` | Managed judge budget per memory (default `60s`). |
| `SAGE_LOCAL_JUDGE_REVISION` | Operator tag that invalidates managed-judge cached verdicts. |
| `SAGE_HUNCH_EVIDENCE` | `off` disables Hunch support checks; otherwise evidence is checked when supplied. Does not affect the managed adapter. |
| `SAGE_HUNCH_URL` | Loopback Hunch service URL; takes precedence over the managed local judge. Both judge settings unset = gate off. A refused URL holds memories without sending content. |
| `SAGE_HUNCH_API_KEY` | bearer key, if the service needs one |
| `SAGE_HUNCH_MODELS` | comma-separated judge models, first one leads (empty = service default, one judge) |
| `SAGE_HUNCH_POLICY` | `lead` (default) or `all` |
| `SAGE_HUNCH_INCLUDE_DOMAINS` | comma-separated domain prefixes to judge; when set, only these domains' text is sent |
| `SAGE_HUNCH_EXEMPT_DOMAINS` | comma-separated domain prefixes never judged (e.g. program-written catalogs) |
| `SAGE_HUNCH_TIMEOUT` | judge budget per memory (default `60s`) |
| `SAGE_HUNCH_JUDGE_REVISION` | free-form tag folded into the verdict-cache version; change it to re-judge after a service's default model changes |

## Operator endpoints

- `GET /v1/dashboard/memory/review-queue` — memories waiting for a decision
- `GET /v1/dashboard/memory/{id}/judgements` — the stored verdicts for one memory (no content)
- `POST /v1/dashboard/memory/{id}/review` `{"decision": "accept"|"reject", "note": "..."}`

## Measured on a real store

Two judges (lead policy), one real personal memory store, hand-checked by one
reader; small samples.

- 194 memories an operator had cleaned up as stale session state: **60%
  rejected**; every one the gate let through was, on reading, a user decision or
  preference rather than a session remark.
- 38 genuine written facts: **none rejected**, about 1 in 10 sent to review.
- A judge call took ~0.6 s (median); judges are asked concurrently.

## Scope and limits

- **Per node.** Verdicts and review decisions live in this node's SQLite store
  and shape this node's vote only. The PostgreSQL store does not implement the
  gate.
- **The judge is not deterministic across nodes**, which is why it runs in the
  voter (whose votes may legitimately disagree) and never in the state machine.
- **Qualify before trusting.** Accuracy depends on the model and on your
  memories; measure on a sample of your own before relying on the thresholds.
