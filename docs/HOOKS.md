# Claude Code Lifecycle Hooks for SAGE

SAGE ships a small set of [Claude Code lifecycle hooks](https://docs.anthropic.com/en/docs/claude-code/hooks) that keep the agent's episodic memory in sync without depending on the agent to remember every step. They fire on session events and inject targeted reminders so calls to `sage_inception`, `sage_turn`, and `sage_reflect` happen at the right moments.

Available as of **v7.0**.

## Why hooks?

The agent's working memory lives in its context window. SAGE's persistent memory lives in the consensus-validated store. The bridge between the two is the agent calling `sage_turn` / `sage_reflect` at appropriate moments. In practice the agent forgets — especially mid-task, mid-compact, or at session end. Hooks close that gap by firing on the lifecycle event itself, regardless of whether the agent thought to act.

## What ships in this repo

The hooks under `.claude/` here are what the SAGE maintainers use day-to-day. You can copy them into your own project verbatim or pick and choose.

| Event | Script | Mode | What it does |
|---|---|---|---|
| `SessionStart` (startup, resume, compact) | `sage-session-start.sh` | **direct-write** | Calls `sage-gui hook session-start`, which pre-fetches recent committed memories and emits them as a context block the agent reads on boot. Falls back to the soft-nudge boot-check if the SAGE node isn't reachable or the agent key isn't readable. |
| `SessionEnd` | `sage-session-end.sh` | **direct-write** | Calls `sage-gui hook session-end`, which submits a `session-lifecycle` observation memory through full BFT consensus so the timeline shows session bookends. Soft-fails silently if SAGE isn't reachable — never blocks the agent's exit path. |
| `PreCompact` | `sage-pre-compact.sh` | nudge | Fires right before Claude Code compresses the context. Turn-level detail is about to be discarded — this nudge prompts the agent to call `sage_reflect` (and any `sage_remember` for durable facts) while context is still fresh. |
| `UserPromptSubmit` | `sage-user-prompt.sh` | passive pointer + nudge | In `full` and `bookend`, calls `sage-gui hook inbox-status` to surface a payload-free exact-agent unread count without claiming work. `full` also reminds the agent to call `sage_turn`; `bookend` suppresses only that memory nudge. Probe failures are reported as unavailable, never as zero. |
| `Stop` | `sage-stop.sh` | opt-in nudge | Calls `sage-gui hook stop-check`. When `SAGE_STOP_NUDGE` is set, it declines the stop once while durable unclaimed inbox work is pending, so the agent handles it in-session instead of going idle on it. Payload-free (derived from a count, never message content), never blocks twice in a row (`stop_hook_active`), never re-nudges the same session for work it already declined, and fails open on every error. Default off. |
| `SubagentStop` | `sage-stop.sh` | silent | Deliberately no-op. A subagent finishing is not evidence the owning host session is idle, and nudging it toward `sage_inbox` could create a second claimant for the same agent. `stop-check` refuses any event other than `Stop`. |

### How the direct-write hooks work

Both direct-write scripts shell out to the `sage-gui hook` subcommand. The script resolves the binary via `SAGE_GUI_BIN` (set to an absolute path at install time), and the subcommand:

1. Resolves the same ordinary-agent Ed25519 key as the paired MCP process (`SAGE_IDENTITY_PATH`, `SAGE_AGENT_KEY`, or the per-workspace identity). A missing or malformed project key fails closed; hooks never fall back to the CEREBRUM Root/operator key.
2. Builds the canonical signed-request headers SAGE's REST middleware expects (`X-Agent-ID`, `X-Signature`, `X-Timestamp`).
3. POSTs / GETs against `http://localhost:8080` (override with `SAGE_URL`) with a tight timeout.
4. Session boundary operations soft-fail through their safe wrapper behavior. The inbox-status wrapper is deliberately fail-visible: it says the check is unavailable and never manufactures a zero count.

Earlier releases shelled out to a bundled `lib/sage_direct.py`. As of **v8.0** the logic ships inside the `sage-gui` binary itself, so the hooks no longer depend on a Python interpreter or `pynacl`.

### Memory modes (v8.0)

Every script reads `~/.sage/memory_mode` and adapts:

- **`full`** (default) — all hooks fire; nudges encourage `sage_turn` on every turn.
- **`bookend`** — SessionStart still prefetches and UserPromptSubmit still checks the exact-agent inbox, but the per-turn `sage_turn` nudges are suppressed; the agent only reflects at the end of significant tasks. SessionStart appends a `SAGE MODE: bookend` notice so the agent knows.
- **`on-demand`** — automatic memory calls are skipped entirely; the agent drives `sage_recall` / `sage_reflect` explicitly.

### Read scope on multi-agent nodes

Direct-write hooks sign as the exact ordinary agent used by that workspace's MCP process. SessionStart first resolves `/v1/agent/me`, then prefetches only that caller's approved home domain. It never uses the node operator/CEREBRUM Root identity as a visibility shortcut, and it never falls back to an unscoped memory list.

The stable `~/.sage/agent.key` remains the separate node transport/operator credential. Missing or unreadable transport state is a node recovery issue, not permission for a project hook to borrow Root. If the workspace key has not been created yet, the hook soft-fails and the shell wrapper emits its safe nudge; the next MCP launch creates/registers the ordinary identity.

## Installing in your own project

The simplest path is to let the binary do it. From your project root:

```bash
sage-gui mcp install
```

This writes `.mcp.json`, the `.claude/hooks/*.sh` scripts, and the `.claude/settings.json` hooks block — resolving the `sage-gui` binary to an absolute path inside each script and creating the `~/.sage/memory_mode` flag (defaults to `full`). Codex CLI users get the equivalent via `sage-gui codex install`.

To wire it up by hand instead, copy `.claude/hooks/*.sh` and `.claude/settings.json` from this repo into your project's `.claude/` directory, point `SAGE_GUI_BIN` (or the default at the top of each script) at your `sage-gui` binary, and `chmod +x .claude/hooks/*.sh`. If your project already has a `.claude/settings.json`, merge the `hooks` block instead of replacing the file — it's keyed by event name, each event taking an array of matcher entries.

Restart your Claude Code session. The hooks fire automatically.

## Disabling individual hooks

Comment out or remove the matching event entry in `.claude/settings.json`. Hooks are opt-in per event, so dropping one doesn't affect the others.

## Mixed model

SAGE ships **two SessionStart/SessionEnd direct-write hooks**, a payload-free exact-agent inbox pointer on `UserPromptSubmit`, and **nudge hooks** where direct-write would be too noisy (`UserPromptSubmit`, `PreCompact`) or too high-frequency without batching (`SubagentStop`). The inbox pointer never claims or exposes messages. Conversation-level memory remains the agent's job (via `sage_turn`, `sage_reflect`) since only the LLM has enough context to distill what's worth remembering. The `memory_mode` flag tunes memory nudges independently from coordination visibility.

## Forward direction

- **v7.1** — introduced direct signed hook prefetch (later replaced by exact workspace-agent scope).
- **v8.0** — hook logic moved into the `sage-gui` binary (no Python dependency), `~/.sage/memory_mode` (`full` / `bookend` / `on-demand`), and Codex CLI hook parity via `sage-gui codex install` (shipped).
- **Next** — optional batched `PostToolUse` direct-write so tool calls auto-observe; Cursor / Cline / Windsurf parity as those hosts expose lifecycle events.
