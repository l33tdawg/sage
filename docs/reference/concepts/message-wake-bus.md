Reconciled against the Wake Bus implementation on this branch. Cite file + symbol where behavior is non-obvious.

# Message Wake Bus

The Wake Bus is a payload-free, exact-recipient hint that canonical local inbox
work was durably inserted. It lets a long-running supervisor sleep between
inbox polls without turning an in-memory notification into delivery, read,
claim, presence, or workflow evidence.

## Durable sequence

The canonical `POST /v1/messages` path uses `SendLocalMessage` (`internal/store/messages.go:297-375`)
to insert the pending `msg-*` row,
caller-scoped idempotency binding, and the recipient's next
`message_wake_state.seq` in one SQLite transaction. A fresh recipient begins at
sequence 1. An exact idempotent replay returns the original message and does not
advance the sequence. A rollback advances nothing.

The deprecated `POST /v1/pipe/send` route now preserves the same wake invariant
for exact local recipients. A request carrying `idempotency_key` uses the keyed
`SendLocalMessage` (`internal/store/messages.go:297-375`) path. An unkeyed request uses
`AdmitLocalMessage` (`internal/store/messages.go:376-412`), which inserts the row and allocates the
sequence in one transaction without creating a replay mapping. After either
fresh path commits, `handlePipeSend` (`api/rest/pipe_handler.go:641-1055`) publishes only the
returned process-local non-zero generation. A keyed replay loads
the original row without `WakeSeq`, so it neither advances nor republishes the
generation. Provider-only and federated rows have no exact local recipient and
do not allocate an exact-recipient sequence. If the active backend cannot
perform atomic canonical admission, an exact-local pipe send fails with HTTP
501 before inserting anything rather than creating a row wake consumers cannot
observe as new.

`SQLiteStore.GetMessageWakeState` returns only:

```json
{"seq": 42, "pending": true}
```

`pending` is an exact-recipient `EXISTS` check for unfinished canonical local
rows: both `pending` and `claimed` work count until completion or expiry. A
claim therefore cannot make the wake surface say the recipient has nothing to
handle merely because another runtime currently owns it. Reading wake state
changes nothing, and claim/read/reply paths do not rewrite the admission
sequence. On upgrade, a recipient that already has unfinished canonical work
receives baseline sequence 1, so restart does not strand that work behind
`after_seq=0`.

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

When `after_seq` equals the durable sequence but unfinished work remains, the
route immediately replays that same sequence with `pending:true`. This is a
state catch-up, not a new admission: it lets a restarted runtime rediscover a
row claimed by a dead session without fabricating a higher cursor. Clients must
therefore treat the cursor as monotonic rather than requiring every received
frame to be strictly greater than the supplied cursor.

`GET /v1/messages/wake-state` returns that exact three-field JSON snapshot
without opening SSE or acquiring its exclusive consumer lease. It exists for
short-lived host hooks that need a monotonic comparison but must not supersede,
cancel, or compete with the long-running wake consumer.

## Broker and reconnect safety

`api/rest/message_wake.go` (`messageWakeBroker`) is process-local acceleration
only. Publication occurs after an atomic message admission returns committed,
and only when it returns a fresh non-zero `WakeSeq`. A missing publication or
process crash cannot lose the wake: reconnect reads the durable sequence before
waiting on the broker.

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

## Experimental Claude wake channel enablement

The Claude notification adapter is experimental and defaults off. The shipped
Claude Code host registers a `notifications/claude/channel` handler, but
end-to-end delivery from a plain `.mcp.json` server through its plugin-scoped
channel gate remains unverified. Enable it with `SAGE_CLAUDE_CHANNEL=1` only
after confirming that delivery path. Codex is always refused because it cannot
consume that method and must not occupy the exclusive wake lease. Other hosts
remain off unless explicitly enabled
(`claudeChannelEnabled`, `mcp.go:333`). Constructing an MCP `Server` never
advertises or emits the experimental protocol on its own; the executable still
makes an explicit enablement call (`EnableRESTClaudeChannel`, `internal/mcp/claude_wake_source.go:85`)
through `ConfigureClaudeChannel` (`internal/mcp/claude_channel.go:50`).

When armed for a compatible host, the adapter subscribes to this agent's own signed wake stream and
turns durable unfinished-work state into a `notifications/claude/channel` JSON-RPC
notification. Claude Code registers a handler for that custom method, while
plain-server delivery through its plugin-scoped gate remains unverified. The
Stop hook below is the verified continuation path.

The channel is **only a poll accelerator, and it is payload-free**. A wake
carries `version`, `seq`, and `pending` and nothing else, so the host learns
that unfinished work exists and never learns its content, its sender, or how
many messages are waiting. The host still calls the canonical inbox operation
to see or claim anything, and that operation remains the only thing that
affects message lifecycle state.

Two consequences follow from the lease described above. A second runtime for
the same agent is rejected with 409 rather than silently sharing the wake, so
the wake accelerates exactly one runtime at a time. The rejected adapter does
not terminate: it retries from its last durable cursor with bounded exponential
backoff (250ms through 30s), and can acquire the lease after the holder
disconnects. Because the channel only accelerates polling, failing to arm or
waiting for that lease is not fatal: the host keeps its ordinary MCP session
and can still use the canonical inbox operations.

## Claude Code and Codex turn-boundary wake

Codex does not consume the custom `notifications/claude/channel` method, so a
Codex MCP process cannot acquire the Claude channel's exclusive wake lease,
even through an explicit override. Claude Code registers a handler for the
experimental method, but plain-server delivery through its plugin-scoped gate
remains unverified, so the adapter stays opt-in. SAGE also installs a `Stop`
command hook for Claude Code and Codex that reads the signed, lease-free
`/v1/messages/wake-state` snapshot as the verified continuation path. The check is on by default when
`SAGE_PROVIDER=claude-code` or `SAGE_PROVIDER=codex`; legacy installed Stop
hooks with no provider label also default on. Set `SAGE_STOP_NUDGE=0` (or another accepted false
spelling) to opt out (`stopNudgeEnabled`, `cmd/sage-gui/hook.go:455`).

When the durable cursor has advanced and unfinished work exists, the hook emits
Codex's documented top-level `{"decision":"block","reason":"..."}` result.
The host converts that result into one continuation prompt for the same thread, so
the agent calls the canonical inbox operation before the turn becomes idle
(`runHookStopCheck`, `cmd/sage-gui/hook.go:471`). The check never acquires the
SSE lease, never sees message content or sender, refuses `SubagentStop`, blocks
at most once per newer cursor and session, and fails open on every error.

This is a turn-boundary continuation, not an out-of-band resurrection API. A
message committed after a thread is already fully idle waits durably for
the next user turn, scheduled continuation, or session start; current Codex MCP
configuration exposes no supported server-notification method that can inject a
new turn into an already-idle local thread.
