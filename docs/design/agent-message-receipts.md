# Agent Message Delivery and Read Receipts

**Status:** v11.17 implements the canonical same-node Messages service and
exact local recipient acknowledgements. It also implements a distinct,
capability-negotiated federated receipt-v2 protocol; legacy peers remain
explicitly unsupported/unconfirmed.

**Consensus:** off-consensus and off both chains. This feature requires no new
application fork and does not change app-v23, AppHash, replay, or state-sync
rules.

**Release boundary:** v11.17 keeps same-node Messages and federated receipts as
separate public surfaces. Federated evidence exists only when both peers
negotiate `federated-pipeline-receipts-v2`; a locally queued legacy pipe must
never be described as delivered or read.

## Product promise

An agent that sends one exact pipeline message can later ask SAGE whether:

1. its own node still has only the queued message;
2. the destination inbox durably accepted the message; and
3. the authenticated recipient client fetched and acknowledged that exact
   message.

The sender does not need a reply from the recipient. Local Messages use
`GET /v1/messages/{message_id}/status`; federated pipes use the separate
`GET /v1/pipe/{pipe_id}/receipt` projection.

## One public Messages model

"Pipe", "inbox", "results", and "outbox" are implementation states, not four
product concepts an agent should have to reconcile. The v11.17 service therefore starts
one agent-only **Messages** model, but receiving work remains an explicitly
destructive operation: selecting pending rows also claims them. The minimal
safe surface has five operations:

1. idempotently send one message;
2. explicitly receive one bounded batch under a caller-supplied idempotency
   token;
3. idempotently reply to one receiver-local message;
4. idempotently acknowledge one receiver-local message as read; and
5. inspect payload-free delivery, read, and workflow status for one exact
   sender-local message.

Every send has one stable sender-local `message_id`. A federated receiver uses
a different, non-disclosed receiver-local ID. The authenticated transport
mapping associates replies and receipts with the source message without
exposing either node's internal ID to unrelated callers. That mapping is split
into typed `reply_binding` and `receipt_binding` records so replying and
acknowledging cannot be confused or rebound. Incoming receive may return
request payload only to the exact authorized recipient. Status is always
payload-free.

The v11.17 same-node surfaces are:

- `POST /v1/messages`;
- `POST /v1/messages/receive`;
- `POST /v1/messages/{receiver_local_message_id}/reply`;
- `PUT /v1/messages/{receiver_local_message_id}/read`;
- `GET /v1/messages/{message_id}/status`; and
- MCP `sage_message_send`, `sage_messages_receive`, `sage_message_reply`, and
  `sage_message_status`.

There is deliberately no `GET /messages`, `direction=all`, passive incoming
list, sent list, or generic `sage_messages` tool in the first release. A lost
response from the current claiming inbox GET can make a blind retry return an
empty or later batch; hiding that mutation behind a list would silently claim
additional work. `POST /messages/receive` must instead persist a
caller/agent-bound idempotency token and replay the identical claimed batch
after a lost response without claiming another row. Batch recovery does not
create a bulk receipt: the client still signs one exact acknowledgement for
each returned message.

`sage_turn` keeps the existing bounded `pipe_inbox` and `pipe_results`
compatibility fields by default. It does not duplicate the same rows into a
second normalized `messages` collection. A future opt-in may use the canonical
receive service only after the same durable idempotency guarantee exists.

The existing `sage_pipe`, `sage_inbox`, `sage_pipe_result`, legacy pipe REST
routes, `sage_turn.pipe_inbox`, and `sage_turn.pipe_results` remain compatibility
wrappers through v12 and for at least two feature-release cycles. They call the same service
methods and preserve existing response fields; they do not maintain a second
queue or independently mutate state. Deprecation warnings belong in
documentation and optional metadata, never stderr/noisy agent context.

Consolidation is deliberately at the public model and service layer. The
following facts remain separate typed fields internally and externally:

- destination inbox admission is delivery;
- recipient fetch plus exact signed acknowledgement is read;
- a result is a reply;
- claim/completion is workflow state; and
- directory discovery is neither delivery nor presence.

Collapsing those facts into one boolean or deriving one from another would make
the UI simpler by making the security contract false. The unified surface
removes navigation and naming duplication while retaining the evidence
boundaries.

“Read” has one narrow, testable meaning:

> The exact addressed agent credential fetched the message through its SAGE
> client and then signed an acknowledgement naming that exact message.

It does **not** prove that a model understood, believed, processed, or acted on
the content. Absence of a receipt means “not confirmed,” never proof that the
recipient did not see the message. SAGE provides no presence, last-seen,
online, typing, activity, or attention signal.

## Why the existing claim is insufficient

The current pipeline has useful substrate, but it does not yet provide a
federated read receipt:

- `GET /v1/pipe/inbox` selects pending rows and compare-and-swap claims only
  the rows returned to the authenticated local agent
  (`api/rest/pipe_handler.go:581-614`;
  `internal/store/sqlite.go:5537-5550`).
- That signed GET binds the path and limit, not the IDs in the server-selected
  response. A destination operator could therefore attach the existing inbox
  proof to a different claimed row. It is evidence that the recipient asked
  for inbox work, not evidence that the recipient acknowledged one exact
  message.
- A federated `send` becomes durable on the destination before the peer returns
  `accepted` or idempotent `duplicate`
  (`internal/federation/pipe_transport.go:394-440`). The source outbox already
  records the later local `DeliveredAt` transition
  (`internal/store/store.go:705-725`;
  `internal/store/pipeline_transport.go:452-464`). That is a valid delivery
  receipt, but it is not a read receipt.
- The current peer event handler accepts only `send` and `result`
  (`internal/federation/pipe_transport.go:351-361`). Although the SQLite
  transport schema reserved the label `claim`, reservation is not protocol
  support (`internal/store/pipeline_transport.go:29-56`).
- Existing `GET /v1/pipe/{pipe_id}` is not the receipt surface. It includes
  message content and permits the recipient or an operator/Admin to inspect the
  row (`api/rest/pipe_handler.go:819-901`). Receipt status must be payload-free
  and sender-only.
- `sage_find_agent` is an active, caller-filtered discovery projection rather
  than an online or deliverability oracle. A known exact local agent ID may
  still resolve and receive a pipe while absent from name discovery. Receipt
  state must therefore derive from the exact sent message, never directory
  presence or a pre-send lookup result.
- `sage_inbox` returns work addressed to the receiver, while a sender sees
  completed replies through `sage_turn.pipe_results` (backed by the results
  surface), not through its own inbox. Receipts must remain a third,
  payload-free sender status surface; they must not be inferred from either
  stream or silently mixed into inbox semantics.

Implementation must preserve those distinctions. In particular, it must never
rename “peer accepted” or the current non-ID-bound inbox GET as “read.”

## Non-negotiable invariants

1. The immutable sender-visible message ID is the existing `pipe_id` returned
   by `sage_pipe`. A federated wire event ID remains internal transport
   identity and never replaces the sender's ID.
2. Local send commit means **delivered** because the recipient inbox row is
   durable in the same SQLite transaction boundary. Federated delivery means
   the peer durably admitted the imported row and the source durably recorded
   that acknowledgement.
3. Every canonical local Messages operation requires a fresh, nonce-bound
   signature by the exact calling agent. Read additionally requires the exact
   addressed agent to sign an acknowledgement naming the exact message.
   Node-operator
   authority alone, CEREBRUM Root, Admin, Manager, an unrelated agent, or a
   provider peer cannot manufacture it.
4. An Admin or operator manually claiming or completing another agent's local
   pipe is not an addressed-recipient read receipt. Legacy workflow behavior
   may remain visible as workflow completion, but it cannot create recipient
   read evidence.
5. Receipt status is visible only to the exact local signing agent that created
   the source pipe. There is no Admin/Root override, recipient view, bulk list,
   group view, dashboard roster view, or federation-operator view.
6. Unauthorized and nonexistent receipt IDs return the same `404` response.
   The endpoint never becomes a message-existence or agent-activity oracle.
7. Receipts contain no intent, payload, result, proof bytes, model/provider
   label, domain data, contact roster, error body, or remote last-seen state.
8. Receipt events grant no memory, domain, role, group, federation,
   governance, task, tool, or reply authority. They are inert metadata.
9. Delivery is at-least-once; admission and receipt application are durable
   and idempotent. Same identity plus different content is equivocation and
   fails closed.
10. Receipt proof envelopes are node-local, vault-backed operational data.
    Minimal sender status state and timestamps are node-local metadata and
    remain queryable while the content/proof vault is locked. Both stores are
    excluded from Badger, CometBFT, AppHash, memory journals, embeddings,
    federation Copy, and network state sync.
11. Directory visibility is never a receipt input or an online/offline signal.
    Losing a `sage_find_agent` match cannot change an existing message's
    delivery, read, or workflow status.

## Sender-facing state model

Do not overload the existing workflow status. The payload-free receipt response
has three independent typed fields:

| Field | Values | Meaning |
|---|---|---|
| `transport_status` | `queued`, `delivered`, `failed`, `expired` | Whether the destination inbox durably accepted the message. |
| `read_status` | `not_confirmed`, `confirmed`, `unsupported` | Whether exact addressed-recipient evidence exists. `unsupported` means an older federated peer cannot send exact acknowledgements. |
| `workflow_status` | `pending`, `claimed`, `completed`, `expired`, `failed` | The existing pipeline workflow state, preserved for compatibility. |

The response may include `sent_at`, `delivered_at`, `read_at`, `completed_at`,
and `expires_at` only when each value exists. `read_evidence` is one of:

- `exact_recipient_ack` — the exact-ID acknowledgement defined below;
- `recipient_result` — a valid result signed by the exact addressed recipient,
  which necessarily proves that recipient obtained the source message; or
- `local_exact_ack` — the same exact-ID acknowledgement on one SAGE node.

`read_at` is the sender node's durable acceptance time. A remote proof timestamp
is retained only for verification/audit and is not presented as an exact
cross-machine wall clock.

A terminal response also carries `terminal_at` and a bounded
`terminal_reason` enum such as `completed`, `delivery_failed`, `expired`,
`revoked`, or `unsupported_peer`. It never returns peer error strings,
exception text, transport diagnostics, or another free-text status field.

A terminal workflow state never erases an earlier read fact. For example, a
message may be `workflow_status:"expired"` and
`read_status:"confirmed"` when the recipient read it but never replied.

## Public surfaces

### `GET /v1/messages/{message_id}/status`

Authenticated, payload-free, exact-sender-only status query.

Example response:

```json
{
  "message_id": "pipe-…",
  "scope": "local",
  "transport_status": "delivered",
  "read_status": "confirmed",
  "read_evidence": "local_exact_ack",
  "workflow_status": "pending",
  "sent_at": "2026-07-29T08:00:00Z",
  "delivered_at": "2026-07-29T08:00:02Z",
  "read_at": "2026-07-29T08:04:11Z",
  "expires_at": "2026-07-29T09:00:00Z"
}
```

Authorization is exactly:

```text
pipeline row is source-local
AND verified caller ID == pipeline.from_agent
AND caller is the current active credential for that exact agent
```

It must not reuse the current party/operator/Admin visibility helper. A
Root/Admin credential and a replacement agent identity receive the same `404`
as an unrelated caller. Existing middleware may independently reject a retired
credential before the handler.

The handler uses a dedicated metadata-only SQL projection such as
`GetPipelineReceiptStatusForSender`. It reads only the caller binding,
transport/read/workflow state, structured terminal reason, and timestamps. It
must not call `GetPipeline`, `GetPipelineTransport`, or any loader that
decrypts payload, result, canonical request, or proof envelopes. Status
therefore remains queryable while the content vault is locked; unreadable
encrypted content cannot turn a metadata-only query into an error or plaintext
fallback.

`GET /v1/pipe/{pipe_id}/receipt` is not a compatibility alias for this local
Messages handler. It is the exact-sender-only federated receipt-v2 projection,
with independent transport, claim/read, and terminal dimensions.

### `sage_message_status`

MCP parameters:

| Name | Type | Required | Meaning |
|---|---|---|---|
| `message_id` | string | yes | Exact caller-local ID returned by `sage_message_send`. |

The tool calls the sender-only REST route and returns the same typed,
payload-free fields plus one concise human-readable explanation. It does not
poll, list all sent messages, infer activity from another endpoint, or surface
peer error text. The query is a local store lookup; it never contacts the
recipient or changes `last_seen`, so asking for status is not itself a presence
probe.

There is no historical MCP `sage_pipe_status` tool to preserve. The
`sage_message_status` name is new for canonical local Messages;
`sage_pipe_receipt_status` queries the separate federated projection.

HTTP MCP bearer authentication must resolve to the exact sender Ed25519
identity. A keyless bearer cannot fall back to Root or the node operator.

### `PUT /v1/messages/{receiver_local_message_id}/read`

This is a signed recipient action over the receiver-local message ID returned
by the canonical same-node receive operation. There is no pipe compatibility
read route.

For a negotiated imported federated pipe, the recipient instead fetches the
exact immutable challenge for each action and submits it unchanged:

```text
GET /v1/pipe/{receiver_local_pipe_id}/receipt/challenge/{claimed|read}
PUT /v1/pipe/{receiver_local_pipe_id}/receipt/{claimed|read}
```

The challenge binds the stable original message/event, exact sender and
recipient agents/chains, content digest, and current policy/agreement/contact
generation. It contains no message content.

The handler requires the exact addressed `to_agent`, or the exact provider
agent that won a legacy provider-addressed claim. It has no operator/Admin/Root
fallback. It checks that the caller already won or atomically wins the claim,
the stable source event matches the imported row, and current imported-pipe
authorization still permits disclosure.

The operation is idempotent for the same recipient, pipe, and proof outcome.
It never changes payload, workflow ownership, result, or memory state.

## Automatic MCP acknowledgement

No recipient must explicitly send a reply or call a receipt tool.

For each negotiated federated pipeline item returned by `sage_inbox` or the
inbox phase of `sage_turn`, the MCP client performs:

1. the existing signed inbox fetch and claim;
2. fetches and submits the exact `claimed` challenge;
3. fetches and submits the exact `read` challenge; and
4. returns the formatted inbox item even if receipt transport could not be
   confirmed.

Canonical same-node Messages continue to use
`PUT /v1/messages/{receiver_local_message_id}/read`.

One acknowledgement proof covers one message. A batch proof must not be split
across multiple transport events because the existing proof replay boundary is
per event kind.

If the acknowledgement step fails after the inbox claim, the work item is still
returned with receipt metadata saying confirmation was not recorded. SAGE must
not hide already claimed work or claim that it was unread. An implementation
may retry an exact signed acknowledgement, but it may not let the node operator
construct one without the recipient credential. Crash windows are therefore
conservative false negatives, never forged positives.

Direct REST consumers receive the same primitive and must use the signed
challenge/action routes after presenting a negotiated federated item. The
Python SDK currently parses `receipt_protocol_version` but does not expose an
automatic federated acknowledgement convenience method.

## Federated receipt protocol

### Capability negotiation

A supporting SQLite-backed peer advertises:

```text
federated-pipeline-receipts-v2
```

The existing `federated-pipeline-v1` capability continues to mean send/result,
not receipts. A source or destination never emits a receipt event until both
sides negotiated the new capability over the authenticated current agreement.
Backends that do not implement durable pipeline transport omit the capability
and return an explicit unsupported response; they never synthesize receipt
state from an in-memory queue.

An older peer keeps normal send/result behavior. The source reports
`protocol:"unsupported"` with `claim_status:"unconfirmed"` and
`read_status:"unconfirmed"`; migration never invents evidence.
Capability support is recorded from authenticated delivery preflight and local
receipt state; the sender-only status query never probes the peer. A historical
or v1 row without the generation-bound v2 binding remains unsupported rather
than being retrofitted after an upgrade.

### Receipt event

Receipt v2 uses a distinct authenticated `POST /fed/v2/pipe/receipt` event and
does not overload the legacy pipeline event or `/fed/v1/receipt` co-commit
protocol.

The outer federation proof still binds:

- exact source and destination chains;
- the active JOIN-frozen node operator and pinned mTLS CA;
- agreement generation and policy epoch; and
- replay-resistant peer request nonce/timestamp.

The nested recipient proof binds:

- exact recipient agent ID;
- canonical `PUT /v1/messages/{receiver_local_message_id}/read` or its
  receiver-local compatibility pipe route;
- stable original send event ID;
- exact receipt-source chain; and
- a fresh nonce and timestamp.

The receipt envelope names the original send event as its origin and reverses
the original source/target agents and source/destination chains. It contains no
payload or result.

### Source admission

The source resolves the original send outbox event and accepts a receipt only
when all of these match:

- original event kind is `send`;
- receipt outer peer is the original destination chain and exact agreement;
- receipt signer is the original target agent;
- receipt target is the original source agent;
- policy epoch, agreement ID, contact ID, and contact revision equal the
  original send binding;
- authorization mode equals the original send mode and a `linked-v23`
  receipt additionally matches the original signed linked-relation digest;
- nested method, path, body, nonce, timestamp, and Ed25519 signature verify;
  and
- the source-local pipe still maps to that original send event.

Acceptance atomically deduplicates the receipt and stamps the source-local
read confirmation. Repeating the same event/proof returns idempotent success.
Reusing an event ID or proof for a different pipe, source event, agent, chain,
or content hash is replay/equivocation and returns a non-enumerating conflict.

The destination operator can suppress or delay a receipt, just as it can make
its node unavailable, but it cannot create positive recipient evidence without
the addressed recipient's signature.

This is a protocol-principal guarantee, not protection from a compromised
recipient host. An operating-system administrator who steals the recipient's
private key, or a compromised recipient runtime that signs arbitrary requests,
has become that credential. Receipt verification prevents the federation
operator key from being substituted for the agent key; it cannot make a stolen
agent key safe.

## Crash and durability rules

- Source pipe and outbound send event remain one atomic insert.
- Remote send admission and its dedup tombstone remain one atomic insert.
- Peer `accepted` or `duplicate` is returned only after the imported inbox row
  is durable.
- A source crash after peer admission but before locally marking delivery
  retries the same event. Remote dedup returns `duplicate`; the source then
  records `delivered_at`.
- Destination read acknowledgement and its `claim` outbox event are one atomic
  transaction. A crash produces either neither change or a durable event that
  retries.
- Source receipt dedup and `read_at` are one atomic transaction. A crash after
  apply but before HTTP response causes an idempotent retry, not a second state
  transition.
- Receipt proof storage uses the existing versioned vault-backed proof
  envelope. A locked vault never falls back to plaintext.
- Sender status uses the dedicated metadata-only projection and remains
  queryable while the content/proof vault is locked.
- Dedup tombstones outlive the maximum message, receipt retry, and metadata
  retention windows.

## Pause, revoke, re-pair, and policy drift

Inbox disclosure, claim, and creation of a read receipt all revalidate the exact
current imported-pipe agreement/contact authorization under the existing read
lease. If Pause, contact acceptance-off, owner change, agent removal, policy
narrowing, or agreement change wins first, the message is not newly disclosed
and no receipt is created.

A receipt already created while that lease was valid is a payload-free fact
about an already accepted message:

- temporary Pause may allow that exact generation-bound metadata event to
  finish; it does not permit new message content or new acknowledgements;
- permanent revoke prevents further transport and may leave a source receipt
  unconfirmed;
- a new pairing never adopts, migrates, or rebinds a receipt from the retired
  agreement generation; and
- unrelated contact/policy changes cannot turn the receipt into authority or
  attach it to a different message.

Revocation is not remote deletion or “unread.” A receipt already durably
accepted on the sender remains a historical fact until its bounded local
retention ends.

## Agent re-enrollment and key rotation

Receipt ownership follows exact cryptographic identities, not mutable names.

- App-v23 ordinary agent key replacement is re-enrollment. A new identity does
  not inherit status queries for pipes sent by the retired identity and cannot
  acknowledge a pipe addressed to the old identity.
- A receipt signed and durably queued while the recipient identity was active
  may finish after that credential retires; the stored signature proves when
  the action was authorized.
- A retired credential cannot create a new acknowledgement.
- CEREBRUM Root handover is irrelevant to pipeline receipts because Root is not
  an agent, sender, recipient, or inbox principal.
- Admin promotion or demotion does not transfer message or receipt ownership.

No history rewrite, memory reassignment, or generic key-successor lookup is
allowed on this surface.

## Retention, backup, and state sync

The present pipeline purge is based on creation time: terminal rows older than
24 hours are eligible for deletion, and non-terminal rows force-expire after
48 hours (`cmd/sage-gui/node.go:1793-1800`;
`internal/store/sqlite.go:5676-5727`). A late read could therefore disappear
almost immediately under the current rule.

Before receipts ship, terminal metadata retention must be measured from the
terminal transition, not original message creation. The minimum contract is:

- message content follows the existing bounded transient policy;
- payload-free sender receipt metadata remains queryable for 24 hours after
  completion, failure, or expiry;
- a read-without-reply confirmation survives the later expiry transition for
  that same bounded terminal window; and
- purge removes receipt metadata and its transport proof together once no
  pending retry or unreported terminal failure remains.

The same-node receive replay ledger is retained for 48 hours and bounded to
4096 caller tokens per agent. A purged or incomplete exact batch fails as gone;
it never silently becomes an empty replay and never claims newer work.

This is not a permanent communications archive. Longer audit retention,
human-facing sent-mail history, or configurable surveillance belongs to a
separate design.

Network CometBFT state sync must start with no pipeline inbox, delivery receipt,
or read receipt. It must not infer receipt state from memory, app height,
federation contact state, or copied SQLite projections. A full local
operator-controlled backup may preserve the encrypted SQLite operational
database according to that backup product's separate contract.

## Required implementation and release gates

Receipts do not ship until all of the following are green:

### Local identity and privacy

- exact sender can query its local and federated source pipes;
- recipient, unrelated agent, Manager, Admin, Root, node operator, old
  credential, and newly enrolled replacement identity receive the same
  non-enumerating denial;
- no response, log, MCP item, SDK exception, or dashboard event exposes payload,
  result, claimed agent, presence, peer roster, or proof bytes;
- provider-addressed work records read only for the exact agent that won the
  claim; and
- an Admin/manual claim or completion does not manufacture addressed-recipient
  read evidence.

### Proof and replay

- inbox GET proof alone cannot create a read receipt;
- exact-ID recipient proof succeeds for only its original message;
- wrong path, body, stable source event, agent, chain, policy epoch, agreement,
  contact ID, or contact revision fails closed;
- duplicate same-proof delivery is idempotent;
- same ID with different binding is equivocation;
- one proof cannot acknowledge two messages; and
- peer operator cannot synthesize a positive receipt without the recipient
  signature.

### Faults and lifecycle

- source crash after remote admit but before delivery mark;
- destination crash before and after atomic read/outbox commit;
- source crash before and after atomic receipt/dedup apply;
- offline retry, vault lock/unlock, Pause/Resume, acceptance off/on, policy
  replacement, ownership change, agent removal, revoke, and same-chain re-pair;
- sender and recipient app-v23 re-enrollment; and
- retention sweep before, at, and after message/receipt expiry.

### End to end and compatibility

- local agent X to local agent Z without a result;
- agent X on SAGE A to agent Y on SAGE B over direct mTLS without a result;
- the same path over persisted relay/NAT routing;
- delivery/read convergence across disconnect and restart with no duplicate
  inbox row or receipt;
- supporting-to-older and older-to-supporting peers;
- exact result still implies read when receipt capability is absent;
- directory miss before or after send does not rewrite receipt state, and a
  known exact local agent ID remains independently resolvable under the
  existing pipeline rules;
- canonical REST/MCP Messages send, explicit receive, reply, exact read
  acknowledgement, and exact sender-status operations share one service model
  without creating a passive inbox/sent/`all` list;
- receive retry with the same caller-bound idempotency token returns the exact
  original claimed batch and never claims another row; each item still has one
  exact acknowledgement rather than a bulk receipt;
- empty polling cannot grow replay state beyond the per-agent bound, and
  expired replay metadata is reclaimed after its bounded retention window;
- sender status remains queryable with the vault locked and never invokes a
  payload/proof-decrypting row loader;
- compatibility `sage_inbox`, `sage_turn.pipe_results`, and
  the legacy pipe REST status/result routes retain distinct receiver-work,
  sender-result, and sender-status semantics without inventing a historical
  `sage_pipe_status` MCP tool;
- old and new calls cannot double-claim, double-reply, double-acknowledge, or
  produce divergent state;
- REST, MCP, synchronous and asynchronous Python SDK contract tests; and
- race-enabled store, REST, MCP, federation, direct two-node, and relay tests.

The final two-node adversarial gate must use two independent SAGE processes,
independent SQLite stores, real mTLS peer authentication, exact agent keys, and
source/destination restarts. In-memory handler composition alone is not
release evidence for a cross-node receipt.

## Explicit non-goals

- human read receipts, CEREBRUM chat, sent mail, notifications, or conversation
  history;
- presence, online/away, last seen, typing, attention, comprehension, or action
  tracking;
- broadcast, group receipts, per-reader counts, or remote agent enumeration;
- permanent audit storage or consensus notarization;
- federation Write/Copy authority, task acknowledgement, or memory mutation;
- operator/Admin impersonation of an agent; and
- retroactive receipts for messages already purged before the feature exists.
