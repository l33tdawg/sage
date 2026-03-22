"""SAGE agent identity and request signing."""

from __future__ import annotations

import hashlib
import os
import struct
import time
from pathlib import Path

from nacl.encoding import HexEncoder
from nacl.signing import SigningKey


class AgentIdentity:
    """Ed25519 identity for SAGE agents.

    Manages keypair generation, persistence, and request signing.
    """

    DEFAULT_KEY_PATH = Path.home() / ".sage" / "agent.key"

    def __init__(self, signing_key: SigningKey) -> None:
        self._signing_key = signing_key
        self._verify_key = signing_key.verify_key

    @classmethod
    def generate(cls) -> AgentIdentity:
        """Generate a new random agent identity."""
        return cls(SigningKey.generate())

    @classmethod
    def from_seed(cls, seed: bytes) -> AgentIdentity:
        """Create an identity from a 32-byte seed."""
        return cls(SigningKey(seed))

    @classmethod
    def from_file(cls, path: str | Path) -> AgentIdentity:
        """Load an identity from a key file (32-byte seed)."""
        with open(path, "rb") as f:
            seed = f.read(32)
        return cls(SigningKey(seed))

    @classmethod
    def default(cls) -> AgentIdentity:
        """Load (or create) identity using SAGE_IDENTITY_PATH env var.

        This enables clean multi-agent setups (multiple Claude Code instances
        on the same machine).
        Example:
            SAGE_IDENTITY_PATH=~/.sage/identities/agent-01.key claude-code ...
        """
        custom_path = os.environ.get("SAGE_IDENTITY_PATH", "").strip()
        path = Path(custom_path).expanduser() if custom_path else cls.DEFAULT_KEY_PATH

        if path.exists():
            return cls.from_file(path)
        else:
            # Auto-generate and save (same behaviour as before)
            identity = cls.generate()
            path.parent.mkdir(parents=True, exist_ok=True)
            identity.to_file(path)
            return identity

    def to_file(self, path: str | Path) -> None:
        """Save the signing key seed to a file."""
        with open(path, "wb") as f:
            f.write(bytes(self._signing_key))

    @property
    def agent_id(self) -> str:
        """Hex-encoded public verify key (agent identifier)."""
        return self._verify_key.encode(encoder=HexEncoder).decode()

    def sign_request(
        self,
        method: str,
        path: str,
        body: bytes | None = None,
        timestamp: int | None = None,
    ) -> dict[str, str]:
        """Sign an HTTP request and return auth headers.

        The signed message is: SHA256(method + " " + path + "\\n" + body) || big-endian int64 timestamp.
        This binds signatures to specific endpoints, preventing cross-endpoint replay.
        """
        ts = timestamp or int(time.time())
        canonical = method.encode() + b" " + path.encode() + b"\n" + (body or b"")
        body_hash = hashlib.sha256(canonical).digest()
        message = body_hash + struct.pack(">q", ts)
        signed = self._signing_key.sign(message)
        return {
            "X-Agent-ID": self.agent_id,
            "X-Signature": signed.signature.hex(),
            "X-Timestamp": str(ts),
        }
