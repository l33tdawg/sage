---
name: sage-memory
description: >
  Persistent, consensus-validated memory for AI agents via SAGE MCP server.
  Gives you institutional memory that survives across conversations — memories go through
  BFT consensus, carry confidence scores, and decay over time. Use this skill when working
  with a SAGE MCP server to manage persistent agent memory, recall past context, reflect
  on completed tasks, and maintain continuity across sessions. Keywords: memory, persistence,
  recall, remember, context, brain, institutional knowledge, consensus, BFT.
license: Apache-2.0
compatibility: >
  Requires SAGE installed locally (https://l33tdawg.github.io/sage/).
  Works with any agent that supports MCP (Model Context Protocol).
  Tested with Claude Code, Cursor, Windsurf, Gemini CLI, and others.
metadata:
  author: l33tdawg
  version: "4.2"
  repository: https://github.com/l33tdawg/sage
---

# SAGE Memory — Persistent Brain for AI Agents

You have persistent institutional memory via SAGE — a governed, consensus-validated knowledge layer.
Your memories are not a flat file. They go through BFT consensus, have confidence scores, and decay
over time. Only committed memories are returned to you.

## Boot Sequence (MANDATORY)

On the **first message of every new conversation**, before responding to the user:

1. Call `sage_inception` (or `sage_red_pill`) — this checks if your brain is online and returns your operating instructions
2. Do NOT greet the user first. Do NOT say anything. Boot your brain first.
3. After inception, follow the safeguard instructions it returns.

## Every Turn

Call `sage_turn` every turn with:
- `topic`: what the current conversation is about (used for contextual recall)
- `observation`: what just happened — the user's request and key points of your response
- `domain`: a specific knowledge domain (see Domains below)

This atomically recalls relevant committed memories AND stores your episodic observation.
Like human memory — you build understanding turn by turn.

## Domains

Create domains **organically** based on what you're working on. Don't dump everything into `general`.

Examples:
- Debugging Go code → `go-debugging`
- Discussing architecture → `project-architecture`
- Working on CI/CD → `ci-cd`
- User preferences → `user-preferences`

Specific domains = better recall.

## Memory Types

When using `sage_remember`:
- **fact** (confidence 0.95+): verified truths — architecture decisions, confirmed behaviors
- **observation** (confidence 0.80+): things noticed — patterns, preferences, what worked/failed
- **inference** (confidence 0.60+): conclusions drawn — hypotheses, connections between facts

## Core Tools

| Tool | Purpose |
|------|---------|
| `sage_inception` | Boot your brain. Call first in every new session. |
| `sage_turn` | Per-turn memory cycle. Recalls + stores atomically. |
| `sage_remember` | Store a new memory with content, type, domain, and confidence. |
| `sage_recall` | Retrieve memories by topic and/or domain. |
| `sage_reflect` | After tasks, store dos and don'ts. Both make you better. |
| `sage_forget` | Deprecate a memory by ID. |
| `sage_list` | List memories with filters (domain, type, status). |
| `sage_timeline` | View memory activity over time. |
| `sage_status` | Check brain health — memory counts, domains, last activity. |
| `sage_register` | Register this agent with the SAGE node. |
| `sage_task` | Create/update tasks for tracking work. |
| `sage_backlog` | View pending tasks. |
| `sage_red_pill` | Alias for sage_inception. |

## Reflection (After Tasks)

After completing significant tasks, call `sage_reflect` with:
- `dos`: what worked well (reinforces good patterns)
- `donts`: what failed or should be avoided (prevents repeating mistakes)
- `task_summary`: brief description of what was done
- `domain`: relevant domain

This feedback loop is critical — it was empirically validated (rho=0.716 with memory vs rho=0.040 without).

## Before Destructive Actions

Call `sage_recall` with topic `critical lessons` to check for known pitfalls before:
- Deleting files, branches, or data
- Force-pushing or resetting state
- Running destructive commands

## Installation

1. Download SAGE from [the releases page](https://github.com/l33tdawg/sage/releases)
2. Run the installer (macOS DMG, Windows EXE, or Linux tarball)
3. Start the server: `sage-gui serve`
4. Connect your agent via MCP — run `sage-gui mcp install` to auto-configure

For Claude Code, the MCP config (`.mcp.json`) looks like:
```json
{
  "mcpServers": {
    "sage": {
      "command": "sage-gui",
      "args": ["mcp"],
      "env": {
        "SAGE_HOME": "~/.sage"
      }
    }
  }
}
```

## Key Principles

- **You are not stateless.** You learn. You grow. You remember.
- **Consensus-validated**: every memory goes through BFT validation before committing.
- **Confidence scores**: memories carry confidence that can be strengthened through corroboration.
- **Natural decay**: memories decay over time unless reinforced — just like human memory.
- **Domain isolation**: organize knowledge by topic for precise recall.
