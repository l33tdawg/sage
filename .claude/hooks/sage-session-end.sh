#!/bin/bash
# SAGE SessionEnd direct-write hook.
#
# Records the lifecycle event (session ended) as a committed memory on the
# local SAGE node. This complements the per-turn sage_turn calls the agent
# makes during the session — those carry the actual conversational content,
# this hook just bookends the session in the timeline.
#
# Soft-fails silently if the SAGE node isn't reachable.

HOOK_DIR="$(cd "$(dirname "$0")" && pwd)"
python3 "$HOOK_DIR/lib/sage_direct.py" session-end 2>/dev/null
# Always exit 0 — a memory-write failure must never break the agent's exit path.
exit 0
