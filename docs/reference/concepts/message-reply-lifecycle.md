Reconciled against SAGE v11.18.2 code. Cite file:line or file + symbol when behavior is non-obvious.

# Message and Reply Lifecycle — who can see a reply, and where

This document answers one question that used to have a wrong answer:
**after a recipient replies to a message you sent, how do you read the reply?**

Before v11.18.2 the honest answer was "from MCP, you can't." A recipient called
`sage_message_reply`, the durable row flipped to `status = 'completed'`, and the
result was reachable only through the passive REST projection
`GET /v1/pipe/results` — which **no MCP tool called**. `sage_inbox` shows work
addressed to you, not answers to you. `sage_message_status` is sender-only but
deliberately payload-free. So in MCP and bookend clients the reply was invisible
and work round-tripped. v11.18.2 closes that with one advertised tool,
`sage_message_replies`, and a payload-free pointer inside `sage_inbox`.

---

## 1. The lifecycle

```
                     sender                                    recipient
                     ------                                    ---------
  sage_message_send ──► row inserted, status = pending
                            │
                            │            sage_inbox / sage_messages_receive
                            ├───────────────────────► claimed (claimed_by set)
                            │
                            │            sage_message_reply(message_id, result)
                            ├───────────────────────► completed
                            │                          result stored, encrypted
                            │                          at rest in the content vault
                            ▼
  sage_message_replies ◄── GET /v1/pipe/results
  sage_inbox pointer   ◄── GET /v1/pipe/results?count_only=1
```

`pending → claimed → completed` are the only workflow states a reply passes
through (plus `expired` for a row nobody handled).

**A completed canonical `msg-*` row is not garbage-collected.** `ExpirePipelines`
(`internal/store/sqlite.go`) only flips `pending`/`claimed` rows past an explicit
TTL; `ExpireStalePipelines` additionally excludes `pipe_id LIKE 'msg-%'`; and
`PurgePipelines` deletes only `pipe_id NOT LIKE 'msg-%'` rows. So the retention
window applies to deprecated `pipe-*` compatibility rows, not to canonical
messages. Because the compatibility reply projection includes both kinds,
`retained_reply_count` is the current retained snapshot and may decrease when a
deprecated row ages out (§4). Rows are still not a governance record: if you
need durable, consensus-validated knowledge, write a memory or a task.

Sending is durable delivery, not read. Claiming is not comprehension.
`completed` means the recipient submitted a result, not that the result is
correct.

---

## 2. Where a reply is visible — the full matrix

| Surface | Returns the reply body? | Who can read it there | Returns the original request payload? | Rows |
|---|---|---|---|---|
| `sage_message_replies` (v11.18.2) | **yes**, truncated at 8,000 runes | the original sender only | **never** | completed only, fully pageable via the composite `before` cursor |
| `sage_inbox` | no — scalars only (`retained_reply_count`, `newest_reply_completed_at`) | the original sender only | no | replies are never items |
| `sage_message_status` | no, by design | the original sender only | no | one exact ID |
| `sage_message_history(folder="outbox")` | yes, untruncated | the original sender only | **yes** (`payload_authority:"request_only"`) | pending, claimed, completed, expired |
| `sage_turn` | no | — | no | payload-free inbox flag only |
| `GET /v1/pipe/results` (REST/SDK `pipe_results()`) | yes | the original sender only | no | completed only |
| `GET /v1/pipe/{pipe_id}` (SDK `pipe_status()`, deprecated MCP alias) | **yes** — the decrypted `result`, unredacted | **sender, addressed recipient, any agent sharing `to_provider`, and any operator/admin** (`callerCanViewPipe`, `api/rest/pipe_handler.go`) | yes | any state |

Three things follow from this table.

**A clean `sage_inbox` is not evidence that no reply exists.** The two surfaces
answer different questions: the inbox answers "what work is addressed to me?",
the reply read answers "what came back from what I sent?". Conflating them is
exactly the failure this release fixes.

**`sage_message_replies` is the payload-free view; the outbox is the full one.**
Use the tool for the routine read. Drop to `sage_message_history(folder="outbox")`
only when you need the untruncated reply text, the original request you sent, or
a non-completed workflow state.

**A reply is not confidential from the recipient, a matching-provider peer, or
an operator/admin.** Sender-exactness is a property of the reply *projection*,
not of the reply. The pre-existing workflow route `GET /v1/pipe/{pipe_id}` uses
`callerCanViewPipe` and returns the same decrypted `result`
(`store.PipelineMessage.Result`, json tag `result,omitempty`) with no redaction
or truncation. If a reply body must be secret from those principals, encrypt it
at the application layer before sending. Pinned by
`api/rest/pipe_results_reply_visibility_test.go`,
`TestPipeStatusStillReturnsTheReplyBodyToNonSenderParties`.

---

## 3. Exactly one agent can read a reply *through the reply projection*: the original sender

This section is about `GET /v1/pipe/results` and `sage_message_replies` only.
The separate workflow route `GET /v1/pipe/{pipe_id}` is wider — see the matrix
row above and §3b.

Authorization for the reply projection is **not** the pipe-view rule.
`callerCanViewPipe` (`api/rest/pipe_handler.go`) admits the recipient, an agent
sharing the message's `to_provider`, and an operator/admin — appropriate for the
`GET /v1/pipe/{pipe_id}` workflow route, and wrong for a reply body.

The reply projection instead uses the SQL predicate in `GetCompletedForSender`
(`internal/store/sqlite.go`) with no role check layered on top:

```sql
WHERE from_agent = ? AND source_chain_id = '' AND status = 'completed'
```

`from_agent = ?` is byte equality against the authenticated caller's own signed
identity. There is no parameter by which a caller can name a different sender —
`sage_message_replies` accepts only `limit`, `since`, and `before`, and none of
them names an agent. `before`'s optional `|<message_id>` half is a resume point
inside the caller's *own* result set: it only ever moves the window within rows
that already satisfy `from_agent = <caller>`, so naming somebody else's message
id returns nothing and confirms nothing. Everyone else gets an empty list, never
a 403 that would confirm a reply exists:

- the recipient that wrote the reply,
- an agent sharing the addressed provider,
- an unrelated local agent,
- an agent ID that is a prefix or an extension of the sender's,
- an authenticated `root` / node operator.

An unauthenticated caller is instead rejected with HTTP 401 before this
projection runs.

Pinned by `api/rest/pipe_results_reply_visibility_test.go`
(`TestPipeResultsIsExactSenderOnlyNotPipeViewAuthorization`).

### 3b. What the reply projection does NOT do: make the reply confidential

`GET /v1/pipe/{pipe_id}` (`handlePipeStatus`, `api/rest/pipe_handler.go`) is a
separate, pre-existing route. It authorizes with `callerCanViewPipe` and answers
`200` with `pipelineMessageREST(msg, "status")`, which embeds
`*store.PipelineMessage` — including the already-decrypted `Result`
(`internal/store/store.go`, json tag `result,omitempty`). Nothing on that
surface redacts or truncates a reply body.

So four principals can read a completed reply there:

- the original sender,
- the addressed recipient,
- any active agent whose `Provider` matches the row's `to_provider`
  (`callerCanViewPipe` admits a provider match whenever `to_provider` is set,
  even on a row also addressed to one exact `to_agent`; the
  "provider-addressed only" narrowing applies to the *claim* side, not here),
- the node operator or any admin (`callerIsOperatorOrAdmin`) — the final
  fallthrough, on **any** local pipe.

An unrelated agent gets the anti-enumeration `404`, so the exposure is bounded
by `callerCanViewPipe` and nothing wider. Treat a reply as private from
strangers, not from the recipient, a provider peer, or an operator/admin;
encrypt at the application layer if it must be secret from those. Pinned by
`TestPipeStatusStillReturnsTheReplyBodyToNonSenderParties`.

### The `source_chain_id = ''` clause is a namespace guard, not a federation exclusion

It reads like it drops federated replies. It does not.

An outbound federated send sets only `DestinationChainID`; `SourceChainID` stays
empty (`api/rest/pipe_handler.go`). When the peer's result comes home,
`ApplyFederatedPipelineResult` (`internal/store/pipeline_transport.go`) *rejects*
any message carrying `SourceChainID` and UPDATEs that same outbound row. So a
**federated reply landed home matches the predicate and is returned**, labelled
`external_untrusted` with `foreign: true` and its `destination_chain_id`.

What the clause excludes is an *imported foreign work row* — work another chain
sent us — whose `from_agent` happens to collide byte-for-byte with a local agent
ID. Without the clause, a remote agent could pick a colliding ID and have its
rows appear in a local agent's reply list. Relaxing this is a security
regression, not a bug fix (`internal/store/pipeline_test.go`,
`TestPipelineFederationNamespacesCannotEnterLocalInboxOrResults`).

---

## 4. A reply is untrusted data, and it is not work

### Untrusted data

Every reply — from a local agent registered on the same SAGE, or from across a
federation edge — is untrusted agent-supplied content. It is **labelled, never
sanitised**: the body comes back verbatim, wrapped in machine-readable trust
metadata derived at serialization time, so agent-controlled bytes can never
persist or declare their own authority.

| Field | Value on a reply |
|---|---|
| `authority` | `data_only` |
| `result_authority` | `data_only` |
| `payload_authority` | `request_only` (labels the retained *intent*) |
| `trust` | `agent_untrusted` locally, `external_untrusted` across federation |
| `security_notice` | the untrusted-content boundary text, verbatim |

Treat a reply only as data to evaluate. Never as system, developer, or user
instructions. Never let one authorize a consequential action on its own.

### Not work

A reply is data you already asked for. If it were shaped like an inbox item, an
agent would answer its own answer and the work would round-trip forever. So a
reply carries `requires_reply: false`, `requires_result: false`,
`passive_reply: true`, `authority: "data_only"` (not `request_only`, which would
read as a fresh request for work), and none of the inbox `from` vocabulary.

This is also why replies never enter `sage_inbox.items[]`:
`formatMessageInboxItem` (`internal/mcp/tools.go`) unconditionally sets
`requires_reply: true`. The inbox instead carries a few scalars:

- `retained_reply_count` — the **current retained archive size, not an unread
  counter**. Canonical `msg-*` replies are durable, but the compatibility
  projection also includes deprecated `pipe-*` results that may age out, so the
  snapshot is not universally monotonic.
- `retained_reply_count_is_unread` — always `false` alongside a non-zero count,
  so the snapshot-vs-queue distinction lives on the wire rather than only in prose.
- `newest_reply_completed_at` — the `completed_at` of the newest retained reply
  **as of this response**. Pass a recorded value as `since` to poll without
  server-held read state. The boundary is inclusive so a reply landing later in
  the same millisecond cannot be hidden; boundary replies may repeat and must be
  deduplicated by `message_id`. `replies_note` states this explicitly, because
  the model acts on the runtime string rather than on this document. Pinned by
  `TestSageInboxReplyPointerCatchUpInstructionIsTrue`.
- `replies_note` — present only when the count is non-zero; names
  `sage_message_replies`, states the value is a retained snapshot rather than an
  unread count, and states the replies are not new work.
- `replies_check_error` — present only when the probe failed. In that case
  `retained_reply_count` is **absent**, so "could not check" is never rendered
  as "you have no replies".

None of these contributes to `count`, `message_count`, or
`task_assignment_count`. Pinned by `internal/mcp/inbox_reply_pointer_test.go`.

**The pointer must never assert pendency.** Because the count can never return
to zero, a string such as "N replies are waiting. Read them with
sage_message_replies" would re-issue the same order on every inbox call, for the
life of the database, about replies the agent read and acted on long ago — a
permanent duplicate-work signal, which is exactly what a reply surface must not
produce. `replies_note` is therefore phrased as a fact ("retained and readable
with…"), never as an imperative, and the field is not called an *action*.
`TestSageInboxReplyPointerNeverAssertsRepliesArePending` pins the absence of
pending-state language.

---

## 5. The request payload never comes back through the reply path

`GetCompletedForSender` does not select the `payload` column
(`internal/store/sqlite.go`). The MCP wire struct `pipelineReplyWireItem`
declares no payload field and no raw claim bookkeeping, so a column added to the
REST projection later cannot silently reach the model. The formatter emits no
`payload`, `pipe_id`, `claimed_by`, `claimed_at`, `source_pipe_id`, or
`source_chain_id` key.

`store.PipelineMessage.Payload` has no `omitempty`, so at the REST layer the
`payload` key is present-but-empty rather than absent. `payload_authority:
"request_only"` on this surface labels the retained `intent`, not returned
request content.

The reply read also takes no message selector, so it is not an existence oracle
and cannot be walked toward an unrelated message. `before` is a *resume point*,
not a selector: its optional `|<message_id>` half is only ever a value the
caller received from its own previous page of its own replies, and the SQL
predicate still requires `from_agent = <caller>`, so naming somebody else's
message id there returns nothing and confirms nothing.

---

## 5b. Provenance: who actually wrote the reply

The one identity the reply surface *does* carry is the author of the untrusted
content. It is not derivable from the addressee.

`callerCanClaimPipe` (`api/rest/pipe_handler.go`) admits:

1. the addressed agent (`to_agent`),
2. any active agent whose `Provider` matches a provider-addressed row, and
3. — unconditionally, on **any** local pipe — any operator/admin
   (`callerIsOperatorOrAdmin`).

`handlePipeResult` gates on `callerCanViewPipe`, which admits the same
principals, and `CompletePipeline` then accepts the write because `claimed_by`
already equals that agent. So the agent that writes a reply body is frequently
*not* the agent the sender addressed.

The projection therefore selects `claimed_by`, the REST route derives
`replied_by` from it (`pipeReplyProvenanceAgent`), and the MCP item exposes:

| Field | Meaning |
|---|---|
| `addressed_to` | who **you** addressed — your own routing choice, never an attribution |
| `replied_by` | who **actually wrote** the reply; absent when the node cannot attribute it |
| `replied_by_known` | `false` when unattributed. The addressee is never substituted. |
| `replied_by_is_addressee` | `true` only when the two are the same agent |
| `provenance_warning` | present whenever they are not, naming `replied_by` as the field to trust |

A federated reply landed home leaves `claimed_by` empty and `replied_by` is
`to_agent` — but only because `ApplyFederatedPipelineResult` refuses a result
whose remote author differs from `to_agent`, so the attribution is verified
rather than assumed.

Claim *timing* (`claimed_at`) is still not returned: it is workflow detail, not
provenance. `sage_message_history(folder="outbox")` has always exposed
`claimed_by` to this same sender, so nothing here widens what a sender may see.

---

## 6. Passive, replay-safe, idempotent

Both `GET /v1/pipe/results` and its `?count_only=1` probe write nothing: no
claim, no acknowledgement, no re-queue, no workflow mutation, and unrelated open
work is not made claimable. Two identical reads return byte-identical bodies.

That is what makes the read safe to retry after a lost response. The MCP client
classifies the path as replay-safe (`internal/mcp/server.go`,
`retryableReadOnlyGETPaths`; the query string is stripped by
`classifySignedRequestReplay`, so both modes inherit it) and re-sends with a
fresh nonce rather than failing closed. Contrast `GET /v1/pipe/inbox` and
`GET /v1/pipe/updates`, which claim or acknowledge rows and are therefore sent
exactly once.

The `since` poll filter is applied **client-side** on purpose. Pushing it to the
server would create per-caller read state and cost the projection its replay-safe
classification.

The `before` cursor *is* sent to the server, and stays replay-safe for a
different reason: it is a value the caller supplies on every call, not state the
server retains. Repeating the same `before` returns the same rows, so a lost
response costs nothing. A client-side `before` would have been useless — it
could only shrink the newest page, never move the window backward.

### `before` is a COMPOSITE keyset cursor, not a timestamp

`completed_at` is written by `strftime('%Y-%m-%dT%H:%M:%fZ', 'now')` —
**millisecond** resolution, with no uniqueness constraint. A recipient working
through a queued batch, or the federated result drain loop
(`internal/federation/pipe_outbox.go`), routinely stamps many completed rows
with the identical value. A cursor of `completed_at < X` alone therefore drops
every row stamped exactly `X`, **including rows the previous page never
returned**, and it fails silently: the next page comes back short, which reads
as "there is nothing older", while `retained_reply_count` keeps advertising the
full total. That is the same sender-cannot-read-the-reply failure this release
exists to eliminate.

The cursor is therefore `"<completed_at>|<message_id>"`, both projections order
by `(completed_at DESC, pipe_id DESC)`, and the pager resumes with
`completed_at < ? OR (completed_at = ? AND pipe_id < ?)` — a total order, so no
reply can be stranded on a tie.

- REST publishes `next_before` on every non-empty page of `GET /v1/pipe/results`.
- `sage_message_replies` publishes `next_before` and names it in the page's own
  `message`. `oldest_completed_at` is still returned, but it is **not** the
  cursor: paging with the timestamp half alone reintroduces the tie loss.
- A bare RFC3339 `before` remains accepted as a coarse "older than this instant"
  **filter**. It is well defined and it excludes ties by design; it is not a
  pager cursor.

Pinned by `TestGetCompletedForSenderBeforeReachesRepliesSharingACompletedMillisecond`
(`internal/store`), `TestPipeResultsBeforeReachesRepliesSharingACompletedMillisecond`
(`api/rest`), and `TestSageMessageRepliesPagesThroughRepliesSharingACompletedMillisecond`
(`internal/mcp`). Those tests seed replies on the **same** millisecond with no
`time.Sleep`, because a sleep between completions is exactly what hides this bug.

---

## 7. Bounds

| Bound | Value | Why |
|---|---|---|
| Stored result size | 256 KiB (`store.MaxPipeContentBytes`, `internal/store/store.go`) | Enforced at the handler and at the `CompletePipeline` store chokepoint; over-cap submission is `413`. |
| Replies per page | 1–20, default 5 | Out-of-range values clamp to the default, never widen the page. The page cap is **not** a cap on reachability: echoing each page's `next_before` composite cursor pages backward through the entire archive, so no reply is stranded behind the newest page — not even replies sharing one `completed_at` millisecond. |
| Reply runes rendered to the model | 8,000 (`maxReplyResultRunes`, `internal/mcp/tools.go`) | A full page of maximum-size replies would flood a context window, and a hostile recipient could weaponise that. Truncation is marked with `result_truncated`, `result_runes_returned`, and `result_full_via`, which names `sage_message_history(folder="outbox")` as where the untruncated text still lives. |
| Retention | canonical `msg-*` replies are retained indefinitely | `ExpirePipelines` touches only `pending`/`claimed`; `ExpireStalePipelines` and `PurgePipelines` both exclude `pipe_id LIKE 'msg-%'`. Deprecated `pipe-*` rows remain visible through the compatibility reply projection only until their transient retention window expires, so `retained_reply_count` is a current snapshot rather than a universally monotonic lifetime value. A retained row is still not a governance record — write a memory or a task for that. |

---

## 8. Backend support

The reply path is implemented for the SQLite/BadgerDB store. On a
Postgres-backed node it is **not available**: `PostgresStore` still stubs
`GetCompletedForSender` (`internal/store/postgres.go`) alongside
`InsertPipeline`, `ClaimPipeline`, `CompletePipeline`, and `GetInbox` — messaging
does not function there at all. v11.18.2 makes that a legible capability gap
rather than an internal fault:

| Condition | Status |
|---|---|
| store has no `PipelineResultCounter` (the `?count_only=1` probe) | `501` |
| store has no `PipelineReplyPager` (the `?before=` cursor) | `501` |
| `GetCompletedForSender` returns `store.ErrPipelineUnsupported` | `501` |
| content vault locked (`store.ErrPipeContentUnavailable`) | `503`, body states the passive read is safe to repeat |
| `?before=` is not RFC3339 | `400` |

A `501` is not "you have no replies", and a `501` on `before=` is not "there is
nothing older" — neither must ever be rendered as one. In MCP the probe failure
arrives as `replies_check_error` on `sage_inbox`, which keeps the inbox working
while refusing to assert a count it could not read, and `sage_message_replies`
returns the problem as a tool **error** rather than an empty page
(`internal/mcp/message_replies_tools_test.go`,
`TestSageMessageRepliesSurfacesStoreProblemsInsteadOfASilentZero`).

---

## Related

- [`../mcp-tools.md`](../mcp-tools.md) — `sage_message_send`, `sage_message_reply`,
  `sage_message_replies`, `sage_message_status`, `sage_message_history`, `sage_inbox`.
- [`../rest-api.md`](../rest-api.md) — `GET /v1/pipe/results`, `POST /v1/messages`,
  `POST /v1/messages/{message_id}/reply`, `GET /v1/messages/{message_id}/status`.
- [`../federation-and-brain-api.md`](../federation-and-brain-api.md) — how a
  federated send and its return route work.
- [`rbac-orgs-federation.md`](rbac-orgs-federation.md) — why an operator or org
  admin is not a party to another agent's messages.
