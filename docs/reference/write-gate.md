# Memory gate (optional, experimental)

The built-in voter checks trust the **author's self-declared confidence**. An
agent that stores a remark about its own session — "the attachment was lost;
the user must re-send the numbers" — as a 0.9-confidence fact gets it
committed, and later sessions recall it as if it were true of the world.

The memory gate lets a node ask an operator-configured judge a calibrated
question before it votes on a proposed memory: *is this lasting knowledge, or a
statement about the conversation it came from?* — and, when the memory was
submitted with evidence, *does that evidence support it?* The judge returns a
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

## The evidence check (experimental)

An agent can submit a memory **together with text it says the memory is based
on** — a quote, a log line, an excerpt. A node running the gate then also asks
whether the memory is supported by that caller-supplied text:

> Is the assertion in `memory` supported by `evidence`, as stated — including
> its time frame and certainty?

It names the look-alikes that most often pass a plain "is it supported"
question: evidence that is about something else, only makes the claim
plausible, or supports a weaker claim; evidence about an **earlier time** ("at
the 2023 inspection the alarm was disabled") for a memory stated as true now
("the alarm is disabled"); and evidence that only **reports** what someone says
("the vendor says…", "reportedly") for a memory stated as fact. A memory that
keeps the evidence's time frame or attribution is supported.

A memory **without** evidence is judged exactly as before — never given a
guessed support score. With evidence, the memory passes only when **both**
checks pass, is voted down when either fails, and is otherwise held for review;
the review screen shows the evidence next to the memory. **Every judge must
agree** that the evidence supports the memory, whatever `SAGE_HUNCH_POLICY`
says (see "Measured" below for why).

Evidence travels **separately** from the memory, because a signed submission
body is carried inside the transaction as the agent's proof:

1. `POST /v1/memory/evidence` `{"evidence": "..."}` (up to 32 KiB) stores it on
   **this node only**, encrypted like memory content when the vault is on, and
   returns a random `evidence_id` (valid for an hour).
2. `POST /v1/memory/submit` with `evidence_id` claims it, once, for the memory —
   before the transaction is built, so the gate never judges the memory
   without it. The chain only ever sees the random id.

MCP agents pass `evidence` to `sage_remember`, which does both steps. Set
`SAGE_HUNCH_EVIDENCE=off` to disable the evidence check while keeping the
lasting check.

What it does **not** do:

- **It does not authenticate the source.** It checks the memory against the
  text the caller supplied; a caller can supply any text. It catches a memory
  that overstates, re-dates or de-attributes its own evidence, not a fabricated
  source.
- **Omitting evidence skips it.** A memory without evidence gets the lasting
  check only.
- **A judge failure falls back** to the built-in checks, as for the lasting
  check; nothing is recorded.
- **Other validators do not receive the evidence.** Only the node the agent
  uploaded it to can ask the evidence check; the rest judge the memory without
  it.

Only an **active agent** may upload evidence (above app-v23 the same
enrollment rule as other agent surfaces; Root is not an agent). An agent may
hold at most 64 unclaimed uploads.

### Evidence retention

Evidence exists only to judge and review a memory that is still **proposed**:

| State | Evidence |
|---|---|
| uploaded, not claimed | deleted one hour after upload |
| claimed, memory proposed (being judged or held for review) | kept |
| claimed, memory committed, rejected or deprecated | deleted — the vote it informed is final |
| claimed, submission provably never signed or sent (e.g. a fenced signer) | released back to unclaimed, so a retry can claim the same `evidence_id` |
| claimed, memory has not appeared (the submission failed after sending, or its outcome is indeterminate) | kept for 24 hours; then the **text** is deleted and the claim stays as an *expired* marker |
| expired marker, memory arrives later | **held for review** — never judged as if no evidence had been supplied; the review screen says the evidence expired, and the marker goes with the memory's decision |

A memory's absence after 24 hours does not prove its transaction can never
commit (an unresolved submission is reconciled until its fate is known, and
proof freshness is judged by block time), which is why the claim is marked
rather than forgotten. Pruning runs on every upload and every ten minutes while
the gate runs.

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
| accept | judge failed | accept (built-in checks only; retried after 10 minutes) |

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

## Local judge and data boundary

Only the **text** of proposed memories in scope — and, for a memory submitted
with evidence, the evidence text — is sent to the configured judge service —
no ids, domains, authors or other metadata. SAGE permits only HTTP(S) on
`localhost` or literal loopback IPs (`127.0.0.1`, `::1`). It rejects public and
LAN endpoints, does not resolve arbitrary hostnames, ignores environment
proxies, and does not follow redirects. Invalid configuration leaves the gate
disabled and logs an error. Source: `internal/hunch/local.go`
(`ValidateLocalURL`, `dialLoopback`, `localClient`) and
`cmd/sage-gui/write_gate.go` (`writeGateFromEnv`).

Scope is controlled by
`SAGE_HUNCH_INCLUDE_DOMAINS` (only these domains) and
`SAGE_HUNCH_EXEMPT_DOMAINS` (never these); the node logs the scope and the
judge URL at startup, and the review screen repeats it. Content that cannot be
produced in plaintext (a locked vault, a decryption failure) is never sent.

### The model must also run locally

The Hunch service is a separate trusted process. A loopback connection to it
cannot prove where that process runs inference: SAGE does not inspect or
control its outbound connections. A localhost proxy to a cloud model is not a
local judge and is not a supported deployment. Keep the gate off until both
the judge and its model backend have been verified as local; use network egress
isolation for those processes when that boundary must be enforced independently
of their configuration.

Set Hunch's backend explicitly to a locally hosted model that supports its
log-probability requirements. For example, for a model already running on this
machine at port 8000:

```sh
export HUNCH_BACKEND_URL=http://127.0.0.1:8000
export HUNCH_BACKEND_MODEL='your-locally-served-model-name'
python -m hunch
```

Check any `hunch.toml` as well: Hunch configuration files can take precedence
over environment variables. Do not rely on its automatic discovery of
`OPENAI_BASE_URL`, `ANTHROPIC_BASE_URL` or other general provider settings.
Those may select a remote service. Hunch's [configuration documentation](https://github.com/ihubanov/hunch#configuration)
describes its precedence and supported backends. Enable the SAGE gate only
after verifying this setup, with `SAGE_HUNCH_URL=http://127.0.0.1:8791`.

## Judges are pluggable

The voter depends only on provider-neutral interfaces —
`LastingProbability(ctx, content) (float64, error)` and, for the evidence
check, `SupportedProbability(ctx, content, evidence) (float64, error)`. Hunch
([github.com/ihubanov/hunch](https://github.com/ihubanov/hunch)) is supplied as
one adapter for each (`internal/hunch.LastingJudge`, `internal/hunch.SupportJudge`);
any other judge can implement the same methods.

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

## Configuration (personal node)

| Variable | Meaning |
|---|---|
| `SAGE_HUNCH_URL` | HTTP(S) URL of a local Hunch service: `localhost` or a loopback IP only. Remote/LAN URLs are refused. **Unset or invalid = gate off.** |
| `SAGE_HUNCH_API_KEY` | bearer key, if the service needs one |
| `SAGE_HUNCH_MODELS` | comma-separated judge models, first one leads (empty = service default, one judge) |
| `SAGE_HUNCH_POLICY` | `lead` (default) or `all` |
| `SAGE_HUNCH_INCLUDE_DOMAINS` | comma-separated domain prefixes to judge; when set, only these domains' text is sent |
| `SAGE_HUNCH_EXEMPT_DOMAINS` | comma-separated domain prefixes never judged (e.g. program-written catalogs) |
| `SAGE_HUNCH_EVIDENCE` | `off` disables the evidence check (default on; it only applies to memories submitted with evidence) |
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

**Evidence check**, two judges from different model families. Measured on
synthetic cases written by an independent author for this purpose; the wording
and pass criteria were fixed before the held-out set was written, and nothing
was tuned on it.

- On an earlier hard set (283 items), the first wording let **19 of 137**
  unsupported memories through under the lead policy — mostly claims about an
  earlier time stated as current, and reported claims stated as fact.
- With the current wording, on **160 fresh held-out** items (40 each: earlier
  time, reported claim, genuine look-alikes that keep the time frame or
  attribution, ordinary genuine support), requiring every judge: **0 of 80**
  unsupported memories passed (95% Wilson upper bound 4.6%); **0 of 80** genuine
  ones were rejected; **3 of 40** ordinary and **10 of 40** look-alike genuine
  memories were held for review — the look-alikes mostly with one judge at
  0.85–0.89. Under the lead policy the same run let 2 of 80 unsupported through,
  which is why the evidence check always requires every judge.
- A repeat run changed 8 of 160 verdicts. The items are synthetic and from one
  author: measure the review rate on your own evidence-bearing memories.

## Scope and limits

- **Per node.** Verdicts, review decisions and evidence live in this node's
  SQLite store and shape this node's vote only. The PostgreSQL store does not implement the
  gate.
- **The judge is not deterministic across nodes**, which is why it runs in the
  voter (whose votes may legitimately disagree) and never in the state machine.
- **Qualify before trusting.** Accuracy depends on the model and on your
  memories; measure on a sample of your own before relying on the thresholds.
