#!/usr/bin/env python3
"""Direct-write SAGE hook helper.

Invoked by .claude/hooks/sage-session-*.sh. Reads the node operator's
Ed25519 key from ~/.sage/agent.key, signs a REST request to the local SAGE
node, and either fetches recent memories (session-start) or submits a
session observation (session-end). Soft-fails silently when:

 - the agent key file is missing (fresh install, no SAGE node)
 - the SAGE node is unreachable (offline / not running)
 - any HTTP error occurs (we don't want hook noise on every session)

When everything works, session-start writes a context block to stdout that
Claude Code injects into the agent's prompt, saving the agent a
sage_recall round-trip on every boot.
"""

from __future__ import annotations

import hashlib
import json
import os
import struct
import sys
import time
from pathlib import Path
from typing import Any

DEFAULT_SAGE_URL = "http://localhost:8080"
DEFAULT_KEY_PATH = "~/.sage/agent.key"
HTTP_TIMEOUT = 3.0  # tight — hooks should not stall the agent
RECENT_LIMIT = 10


def _key_bytes() -> bytes | None:
    """Return the Ed25519 seed (32 bytes) or None if no key is reachable.

    SAGE writes a 64-byte expanded key (seed || pub) on most nodes; we
    accept either form by taking the first 32 bytes.
    """
    path = os.environ.get("SAGE_AGENT_KEY", DEFAULT_KEY_PATH)
    expanded = Path(os.path.expanduser(path))
    if not expanded.is_file():
        return None
    data = expanded.read_bytes()
    if len(data) < 32:
        return None
    return data[:32]


def _signing_key(seed: bytes):
    """Lazy import so the script exits cleanly when pynacl is missing."""
    from nacl.signing import SigningKey  # type: ignore

    return SigningKey(seed)


def _signed_headers(sk, method: str, path: str, body: bytes) -> dict[str, str]:
    """Build the Ed25519-signed headers SAGE's REST middleware expects."""
    ts = int(time.time())
    canonical = f"{method} {path}\n".encode() + body
    body_hash = hashlib.sha256(canonical).digest()
    ts_bytes = struct.pack(">q", ts)
    signature = sk.sign(body_hash + ts_bytes).signature.hex()
    return {
        "Content-Type": "application/json",
        "X-Agent-ID": sk.verify_key.encode().hex(),
        "X-Signature": signature,
        "X-Timestamp": str(ts),
    }


def _request(method: str, path: str, body: bytes | None = None) -> dict[str, Any] | None:
    """Sign + send. Returns parsed JSON on 2xx, None otherwise."""
    seed = _key_bytes()
    if seed is None:
        return None
    try:
        sk = _signing_key(seed)
    except Exception:
        return None

    base = os.environ.get("SAGE_URL", DEFAULT_SAGE_URL).rstrip("/")
    body_bytes = body or b""
    headers = _signed_headers(sk, method, path, body_bytes)

    try:
        import urllib.request

        req = urllib.request.Request(
            base + path, data=body_bytes if body_bytes else None, method=method, headers=headers
        )
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as resp:
            return json.loads(resp.read().decode())
    except Exception:
        return None


def session_start() -> int:
    """Fetch recent memories and emit a context block."""
    # Use newest-first list with a small limit. Direct attribution to the
    # node-operator key means we get the operator's recent memories, which
    # is the right scope for a "what was I working on" prefetch.
    qs = f"?limit={RECENT_LIMIT}&sort=newest&status=committed"
    data = _request("GET", "/v1/memory/list" + qs)
    if not data:
        return 1  # caller falls back to nudge

    memories = data.get("memories") or data.get("results") or []
    if not memories:
        # Node reachable but no committed memories yet — emit a tiny ack.
        print("SAGE: connected, no recent memories to surface.")
        return 0

    print("SAGE: recent committed memories (direct-write SessionStart hook):")
    print()
    for m in memories[:RECENT_LIMIT]:
        domain = m.get("domain_tag") or m.get("domain") or "general"
        mtype = m.get("memory_type") or m.get("type") or "observation"
        content = (m.get("content") or "").replace("\n", " ").strip()
        if len(content) > 200:
            content = content[:200] + "..."
        print(f"  [{domain}/{mtype}] {content}")
    print()
    print("Use sage_recall for targeted retrieval; this list is just a warm prefetch.")
    return 0


def session_end() -> int:
    """Submit a lightweight session-ended observation via consensus.

    Read the hook payload from stdin (Claude Code passes session metadata as
    JSON). Best-effort — if stdin isn't a JSON payload we still record a
    minimal heartbeat so the timeline shows the session.
    """
    payload: dict[str, Any] = {}
    try:
        raw = sys.stdin.read()
        if raw.strip():
            payload = json.loads(raw)
    except Exception:
        payload = {}

    session_id = payload.get("session_id") or "unknown"
    reason = payload.get("reason") or payload.get("stop_reason") or "ended"

    content = (
        f"Claude Code session {session_id} ended ({reason}). "
        f"Direct-write SessionEnd hook recording the lifecycle event; "
        f"per-turn content is captured by the agent's own sage_turn calls."
    )

    body = json.dumps(
        {
            "content": content,
            "memory_type": "observation",
            "domain_tag": "session-lifecycle",
            "confidence_score": 0.85,
            "tags": ["claude-code", "session-end"],
        }
    ).encode()

    data = _request("POST", "/v1/memory/submit", body)
    if not data:
        return 1
    # Quiet on success — Stop hook output isn't visible to the agent anyway.
    return 0


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        return 2
    cmd = argv[1]
    if cmd == "session-start":
        return session_start()
    if cmd == "session-end":
        return session_end()
    return 2


if __name__ == "__main__":
    sys.exit(main(sys.argv))
