# Claude Code Lifecycle Hooks for SAGE

SAGE ships a small set of [Claude Code lifecycle hooks](https://docs.anthropic.com/en/docs/claude-code/hooks) that keep the agent's episodic memory in sync without depending on the agent to remember every step. They fire on session events and inject targeted reminders so calls to `sage_inception`, `sage_turn`, and `sage_reflect` happen at the right moments.

Available as of **v7.0**.

## Why hooks?

The agent's working memory lives in its context window. SAGE's persistent memory lives in the consensus-validated store. The bridge between the two is the agent calling `sage_turn` / `sage_reflect` at appropriate moments. In practice the agent forgets — especially mid-task, mid-compact, or at session end. Hooks close that gap by firing on the lifecycle event itself, regardless of whether the agent thought to act.

## What ships in this repo

The hooks under `.claude/` here are what the SAGE maintainers use day-to-day. You can copy them into your own project verbatim or pick and choose.

| Event | Script | What it does |
|---|---|---|
| `SessionStart` (startup, resume, compact) | `sage-boot-check.sh` | Reminds the agent to call `sage_inception` before responding. New sessions, resumed sessions, and post-compaction sessions all need a memory boot. |
| `PreCompact` | `sage-pre-compact.sh` | Fires right before Claude Code compresses the context. Turn-level detail is about to be discarded — this nudge prompts the agent to call `sage_reflect` (and any `sage_remember` for durable facts) while context is still fresh. |
| `UserPromptSubmit` | `sage-user-prompt.sh` | Light reminder to call `sage_turn` early in the response, capturing the new conversational state. |
| `Stop` | `sage-stop.sh` | Reserved for future end-of-turn capture. Currently a no-op so it doesn't add visible chatter; the script is in place so the wiring is ready when v7.1 ships richer end-of-turn behaviour. |

## Installing in your own project

Copy `.claude/hooks/*.sh` and `.claude/settings.json` from this repo into your project's `.claude/` directory. The hook commands are relative paths (`bash .claude/hooks/...`), so as long as you preserve the directory layout no edits are needed.

If your project already has a `.claude/settings.json`, merge the `hooks` block instead of replacing the file. The `hooks` object is keyed by event name; each event takes an array of matcher entries.

After copying, mark the scripts executable:

```bash
chmod +x .claude/hooks/*.sh
```

Restart your Claude Code session. The hooks fire automatically.

## Disabling individual hooks

Comment out or remove the matching event entry in `.claude/settings.json`. Hooks are opt-in per event, so dropping one doesn't affect the others.

## Why not direct writes from the hook?

A hook *could* shell out, sign a request, and POST directly to a SAGE node — bypassing the agent entirely. That requires the hook to discover the SAGE endpoint, hold an agent key, and replicate the Ed25519 signing path that the MCP server already implements. It's a fine direction and on the roadmap for **v7.1** as part of the Recall Polish theme.

For v7.0 the simpler model — hooks nudge the agent, the agent calls SAGE — keeps the auth surface small and lets the hooks ship without depending on the SAGE node being reachable from the hook's environment. The agent already has the SAGE MCP tools available; the hook just makes sure they get used.

## Forward direction

- **v7.1** — direct-write hooks for environments where the SAGE node is local and the agent identity can be discovered safely (e.g. through the existing `.sage/agent.json` bundle).
- **v7.x** — Codex CLI hook parity using the same event shape, then Cursor / Cline / Windsurf as they expose lifecycle events.
