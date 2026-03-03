#!/usr/bin/env python3
"""(S)AGE Integration Bridge Example.

Production-ready template for connecting any multi-agent application to
(S)AGE. Wraps the AsyncSageClient with env-var config, lazy initialization,
graceful degradation, and a multi-strategy embedding pipeline.

When SAGE_ENABLED is not "true", every method returns empty/passthrough values
so the calling agent sees zero behavior change. This lets you wire SAGE into
any existing agent codebase with a single env-var toggle.

Usage:
    # With SAGE disabled (default) — shows graceful degradation
    python examples/sage_bridge_example.py

    # With SAGE enabled
    SAGE_ENABLED=true SAGE_AGENT_KEY_FILE=my_agent.key \
        python examples/sage_bridge_example.py

Environment variables:
    SAGE_ENABLED          "true" to activate (default "false")
    SAGE_ENDPOINT         REST base URL (default "http://localhost:8080")
    SAGE_AGENT_KEY_FILE   Path to 32-byte Ed25519 seed file
    SAGE_EMBEDDING_MODEL  "ollama" (default), "sage", or "hash"
    OLLAMA_URL            Ollama endpoint (default "http://localhost:11434")
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import logging
import os
import struct
import time
from typing import Any

import httpx

logger = logging.getLogger("sage_bridge")


class SageBridge:
    """Async bridge between your application and (S)AGE.

    Handles client lifecycle, embedding generation, and graceful degradation.
    Drop this class into any multi-agent project and configure via env vars.

    Configuration:
        SAGE_ENABLED        - "true" to activate (default "false")
        SAGE_ENDPOINT       - REST base URL (default "http://localhost:8080")
        SAGE_AGENT_KEY_FILE - Path to 32-byte Ed25519 seed file
        SAGE_EMBEDDING_MODEL - "ollama" (default), "sage", or "hash"
        OLLAMA_URL          - Ollama endpoint (default "http://localhost:11434")
    """

    def __init__(self) -> None:
        self._enabled = os.getenv("SAGE_ENABLED", "false").lower() == "true"
        self._endpoint = os.getenv("SAGE_ENDPOINT", "http://localhost:8080")
        self._key_file = os.getenv("SAGE_AGENT_KEY_FILE", "")
        self._embedding_model = os.getenv("SAGE_EMBEDDING_MODEL", "ollama")
        self._client: Any = None  # AsyncSageClient, lazy-init

    @property
    def enabled(self) -> bool:
        return self._enabled

    async def _get_client(self) -> Any:
        """Lazy-initialize the AsyncSageClient on first use."""
        if self._client is not None:
            return self._client

        from sage_sdk.async_client import AsyncSageClient
        from sage_sdk.auth import AgentIdentity

        if not self._key_file:
            logger.error("SAGE_AGENT_KEY_FILE not set — disabling SAGE")
            self._enabled = False
            return None

        try:
            identity = AgentIdentity.from_file(self._key_file)
            self._client = AsyncSageClient(
                base_url=self._endpoint,
                identity=identity,
            )
            logger.info(
                "SAGE client initialized (endpoint=%s, agent=%s)",
                self._endpoint,
                identity.agent_id,
            )
            return self._client
        except Exception:
            logger.exception("Failed to initialize SAGE client — disabling")
            self._enabled = False
            return None

    # ------------------------------------------------------------------
    # Public methods
    # ------------------------------------------------------------------

    async def query_knowledge(
        self,
        domain: str,
        top_k: int = 5,
    ) -> list[str]:
        """Query (S)AGE for relevant institutional knowledge.

        Args:
            domain: Domain tag to search (e.g. "security", "ops.networking").
            top_k: Maximum number of results to return.

        Returns:
            List of memory content strings. Empty list when disabled or on error.
        """
        if not self._enabled:
            return []

        try:
            client = await self._get_client()
            if client is None:
                return []

            embedding = self._generate_embedding(domain)
            response = await client.query(
                embedding=embedding,
                domain_tag=domain,
                top_k=top_k,
                status_filter="committed",  # Only consensus-validated memories
            )
            return [r.content for r in response.results]
        except Exception:
            logger.exception("SAGE query_knowledge failed (domain=%s)", domain)
            return []

    async def submit_observation(
        self,
        content: str,
        domain: str,
        confidence: float,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Submit an observation to (S)AGE institutional memory.

        Args:
            content: Natural language observation to store.
            domain: Domain tag (e.g. "security", "ops.networking").
            confidence: Confidence score 0.0-1.0.
            metadata: Optional key-value pairs stored as KnowledgeTriple triples.

        Returns:
            The memory_id on success, None when disabled or on error.
        """
        if not self._enabled:
            return None

        try:
            client = await self._get_client()
            if client is None:
                return None

            embedding = self._generate_embedding(content)

            knowledge_triples = None
            if metadata:
                from sage_sdk.models import KnowledgeTriple

                knowledge_triples = [
                    KnowledgeTriple(
                        subject=domain,
                        predicate=str(k),
                        **{"object": str(v)},
                    )
                    for k, v in metadata.items()
                ]

            response = await client.propose(
                content=content,
                memory_type="observation",
                domain_tag=domain,
                confidence=confidence,
                embedding=embedding,
                knowledge_triples=knowledge_triples,
            )
            logger.info("SAGE observation submitted: %s", response.memory_id)
            return response.memory_id
        except Exception:
            logger.exception(
                "SAGE submit_observation failed (domain=%s)", domain
            )
            return None

    async def enrich_context(
        self,
        domain: str,
        existing_context: str,
        top_k: int = 3,
    ) -> str:
        """Enrich an agent's context with institutional knowledge from (S)AGE.

        Queries relevant memories and prepends them to the agent's existing
        context. This is the primary integration point — call it before any
        agent action to inject accumulated institutional knowledge.

        Args:
            domain: Domain to query (e.g. "ops.deployment").
            existing_context: The agent's current context/prompt to enrich.
            top_k: Number of institutional memories to inject.

        Returns:
            Enriched context string. Returns existing_context unchanged when
            SAGE is disabled, no relevant memories exist, or on error.
        """
        if not self._enabled:
            return existing_context

        try:
            memories = await self.query_knowledge(domain=domain, top_k=top_k)
            if not memories:
                return existing_context

            sage_section = (
                "# Institutional Knowledge ((S)AGE)\n"
                + "\n".join(f"- {m}" for m in memories)
            )
            return f"{sage_section}\n\n{existing_context}"
        except Exception:
            logger.exception("SAGE enrich_context failed (domain=%s)", domain)
            return existing_context

    # ------------------------------------------------------------------
    # Embedding pipeline
    # ------------------------------------------------------------------

    def _generate_embedding(self, text: str) -> list[float]:
        """Generate a 768-dim embedding vector.

        Modes (SAGE_EMBEDDING_MODEL):
          - "ollama" — direct call to local Ollama nomic-embed-text (default)
          - "sage"   — call (S)AGE network's /v1/embed endpoint (remote agents)
          - "hash"   — deterministic SHA-256 pseudo-embedding (testing only)
        """
        if self._embedding_model == "ollama":
            try:
                return self._ollama_embed(text)
            except Exception as e:
                logger.warning(
                    "Ollama embedding failed, falling back to hash: %s", e
                )
                return self._hash_embed(text)
        elif self._embedding_model == "sage":
            try:
                return self._sage_embed(text)
            except Exception as e:
                logger.warning(
                    "SAGE embed endpoint failed, falling back to hash: %s", e
                )
                return self._hash_embed(text)
        return self._hash_embed(text)

    def _ollama_embed(self, text: str) -> list[float]:
        """Generate embedding via local Ollama nomic-embed-text."""
        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        resp = httpx.post(
            f"{ollama_url}/api/embed",
            json={"model": "nomic-embed-text", "input": text},
            timeout=30.0,
        )
        resp.raise_for_status()
        data = resp.json()
        embeddings = data.get("embeddings", [])
        if not embeddings:
            raise ValueError("Ollama returned no embeddings")
        return embeddings[0]

    def _sage_embed(self, text: str) -> list[float]:
        """Generate embedding via (S)AGE network's /v1/embed endpoint.

        For agents that don't run Ollama locally. The (S)AGE network
        runs Ollama and exposes it through the REST API.
        """
        from sage_sdk.auth import AgentIdentity

        import nacl.signing

        identity = AgentIdentity.from_file(self._key_file)

        # Build auth headers: SHA-256(body) + big-endian int64(timestamp)
        body_bytes = b'{"text": "' + text.encode("utf-8") + b'"}'
        timestamp = int(time.time())
        msg = hashlib.sha256(body_bytes).digest() + struct.pack(
            ">q", timestamp
        )
        signing_key = nacl.signing.SigningKey(identity._seed)
        sig = signing_key.sign(msg).signature
        headers = {
            "X-Agent-ID": identity.agent_id,
            "X-Signature": base64.b64encode(sig).decode(),
            "X-Timestamp": str(timestamp),
            "Content-Type": "application/json",
        }
        resp = httpx.post(
            f"{self._endpoint}/v1/embed",
            content=body_bytes,
            headers=headers,
            timeout=30.0,
        )
        resp.raise_for_status()
        return resp.json()["embedding"]

    @staticmethod
    def _hash_embed(text: str) -> list[float]:
        """Deterministic 768-dim pseudo-embedding via SHA-256.

        Fallback when Ollama is unavailable. Not semantic — only matches
        near-identical text. Used for testing and bootstrap.
        """
        dim = 768
        rounds = (dim * 4 + 31) // 32
        raw = b""
        current = text.encode("utf-8")
        for i in range(rounds):
            current = hashlib.sha256(current + struct.pack(">I", i)).digest()
            raw += current

        values: list[float] = []
        for j in range(dim):
            offset = j * 4
            n = struct.unpack(">I", raw[offset : offset + 4])[0]
            values.append((n / 2147483647.5) - 1.0)

        return values


# Module-level singleton for easy import:
#   from sage_bridge_example import sage_bridge
#   memories = await sage_bridge.query_knowledge("ops.deployment")
sage_bridge = SageBridge()


# ------------------------------------------------------------------
# Example usage
# ------------------------------------------------------------------


async def main() -> None:
    """Demonstrate SageBridge with graceful degradation."""

    logging.basicConfig(
        level=logging.INFO,
        format="%(name)s | %(levelname)s | %(message)s",
    )

    bridge = SageBridge()
    print(f"SAGE enabled: {bridge.enabled}")
    print(f"Endpoint:     {os.getenv('SAGE_ENDPOINT', 'http://localhost:8080')}")
    print("=" * 60)

    # --- 1. Query institutional knowledge ---
    print("\n[1/3] Querying institutional knowledge (domain='ops.deployment')...")
    memories = await bridge.query_knowledge(domain="ops.deployment", top_k=5)
    if memories:
        for i, m in enumerate(memories, 1):
            print(f"       [{i}] {m[:80]}")
    else:
        print("       (no results — SAGE disabled or no memories in this domain)")

    # --- 2. Submit an observation ---
    print("\n[2/3] Submitting an observation...")
    memory_id = await bridge.submit_observation(
        content="Rolling deployments with health checks reduce downtime by 40%.",
        domain="ops.deployment",
        confidence=0.85,
        metadata={
            "strategy": "rolling",
            "metric": "downtime_reduction_40pct",
        },
    )
    if memory_id:
        print(f"       Submitted: {memory_id}")
    else:
        print("       (skipped — SAGE disabled)")

    # --- 3. Enrich an agent's context ---
    print("\n[3/3] Enriching agent context with institutional knowledge...")
    agent_prompt = (
        "You are a deployment agent. Plan the rollout for service-api v2.3.1 "
        "to the production cluster."
    )
    enriched = await bridge.enrich_context(
        domain="ops.deployment",
        existing_context=agent_prompt,
        top_k=3,
    )
    if enriched != agent_prompt:
        print("       Context enriched with institutional knowledge:")
        print(f"       {enriched[:120]}...")
    else:
        print("       (unchanged — SAGE disabled or no relevant memories)")

    print("\n" + "=" * 60)
    print("Bridge example complete.")
    if not bridge.enabled:
        print(
            "\nTo run with SAGE enabled:\n"
            "  SAGE_ENABLED=true SAGE_AGENT_KEY_FILE=my_agent.key "
            "python examples/sage_bridge_example.py"
        )


if __name__ == "__main__":
    asyncio.run(main())
