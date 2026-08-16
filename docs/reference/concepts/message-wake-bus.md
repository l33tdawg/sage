Reconciled against the Wake Bus implementation on this branch. Cite file + symbol where behavior is non-obvious.

# Message Wake Bus

The Wake Bus is a payload-free, exact-recipient hint that canonical local inbox
work was durably inserted. It lets a long-running supervisor sleep between
inbox polls without turning an in-memory notification into delivery, read,
claim, presence, or workflow evidence.

## Durable sequence

`internal/store/messages.go` (`SQLiteStore.SendLocalMessage`) inserts the
canonical pending `msg-*` row, caller-scoped idempotency binding, and the
recipient's next `message_wake_state.seq` in one SQLite transaction. A fresh
recipient begins at sequence 1. An exact idempotent replay returns the original
message and does not advance the sequence. A rollback advances nothing.

`SQLiteStore.GetMessageWakeState` returns only:

```json
{"seq": 42, "pending": true}
```

`pending` is an exact-recipient `EXISTS` check for currently claimable canonical
local pending rows. Reading wake state changes nothing. Claim/read/reply paths
do not rewrite the sequence. On upgrade, a recipient that already has pending
canonical work receives baseline sequence 1, so restart does not strand that
work behind `after_seq=0`.

## Signed catch-up and SSE

An active ordinary agent opens:

```text
GET /v1/messages/wake?consumer_id=<stable-runtime-id>&after_seq=<last-seq>
```

The ordinary REST route requires the same fresh nonce-bound exact-agent
signature as every canonical Messages action. It has no `agent_id` parameter:
the authenticated caller is the only possible recipient. `Last-Event-ID` is the
SSE-native alternative to `after_seq`; if both are supplied they must match. A
cursor ahead of the durable sequence is a loud `409`, never a silent wait.

Each wake event is:

```text
id: 42
event: wake
data: {"version":1,"seq":42,"pending":true}
```

The JSON payload contains **exactly** `version`, `seq`, and `pending`. It never
contains a message ID, sender, intent, payload, count, proof, receipt, delivery,
read, claim, result, or presence field. Heartbeat comments keep the connection
alive without inventing an event.

## Broker and reconnect safety

`api/rest/message_wake.go` (`messageWakeBroker`) is process-local acceleration
only. Publication occurs after `SendLocalMessage` returns committed, and only
for a fresh admission. A missing publication or process crash cannot lose the
wake: reconnect reads the durable sequence before waiting on the broker.

One exact agent has one active consumer lease. The same `consumer_id` may
supersede its stale connection; a different consumer receives `409` until the
lease disconnects or reaches its five-minute TTL. The broker channel holds one
sequence and coalesces a slow client to the newest monotonic value. Per-write
deadlines bound a client that stops reading. These are coordination controls,
not evidence that the consumer is online or attended to an event.

After receiving a wake, the supervisor still calls the canonical inbox
operation (`sage_messages_receive` / `sage_inbox`) to claim work. Only those
existing operations affect message lifecycle state.

## Separate from dashboard and MCP transport SSE

`GET /v1/messages/wake` is an ordinary signed agent route under the app-v23
pipeline-agent boundary. It does not use the dashboard `OnEvent` broadcaster.
The older HTTP-MCP `notifications/sage_message` bridge remains a separate,
best-effort compatibility notification for already-open MCP SSE sessions and
does not define this durable sequence contract.

## Claude wake channel enablement

The Claude host adapter is the one shipped consumer of this route. A
project-scoped `sage-gui mcp` session whose `SAGE_PROVIDER` is `claude-code`
arms it by default; set `SAGE_CLAUDE_CHANNEL=0` (or another accepted false
spelling) to opt out. Other MCP hosts stay off unless explicitly enabled
(`cmd/sage-gui/mcp.go:224`). Constructing an MCP `Server` never advertises or
emits the experimental protocol on its own; the executable still makes an
explicit enablement call (`internal/mcp/claude_wake_source.go:84`,
`internal/mcp/claude_channel.go:50`).

When armed, the adapter subscribes to this agent's own signed wake stream and
turns a new durable cursor into a `notifications/claude/channel` JSON-RPC
notification, so the host can stop polling its inbox on a timer.

The channel is **only a poll accelerator, and it is payload-free**. A wake
carries `version`, `seq`, and `pending` and nothing else, so the host learns
that unclaimed work exists and never learns its content, its sender, or how
many messages are waiting. The host still calls the canonical inbox operation
to see or claim anything, and that operation remains the only thing that
affects message lifecycle state.

Two consequences follow from the lease described above. A second runtime for
the same agent is rejected with 409 rather than silently sharing the wake, so
the wake accelerates exactly one runtime at a time. And because the channel
only accelerates polling, failing to arm it is not fatal: a host that cannot
open the stream still gets an ordinary MCP session and ordinary polling.
