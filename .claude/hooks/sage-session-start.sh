#!/bin/bash
# SAGE SessionStart direct-write hook.
#
# Attempts a direct call to the local SAGE node to pre-fetch recent committed
# memories and surface them as initial context. Falls back to the legacy
# soft-nudge boot-check if the node is unreachable, the agent key is missing,
# or any HTTP error occurs.
#
# Why fallback: SAGE may not be running, may not be installed, or may belong
# to a different operator on this machine. The nudge always works.

HOOK_DIR="$(cd "$(dirname "$0")" && pwd)"

if python3 "$HOOK_DIR/lib/sage_direct.py" session-start 2>/dev/null; then
  exit 0
fi

# Direct-write failed — fall back to the prompt-injection nudge so the
# agent still knows to boot.
exec bash "$HOOK_DIR/sage-boot-check.sh"
