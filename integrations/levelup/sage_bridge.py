"""SAGE integration bridge for Level Up CTF agents.

Provides a single entry point for all SAGE operations. When SAGE_ENABLED
is not "true", every method returns empty/passthrough values so the
calling agent sees zero behavior change.

Embedding models (SAGE_EMBEDDING_MODEL):
  - "ollama"  — local Ollama nomic-embed-text (768-dim, sovereign, default)
  - "hash"    — deterministic SHA-256 pseudo-embedding (768-dim, no deps)
"""

from __future__ import annotations

import hashlib
import logging
import os
import struct
from typing import Any

import httpx

logger = logging.getLogger("sage_bridge")


class SageBridge:
    """Thin async wrapper around sage_sdk.AsyncSageClient.

    Configuration via environment variables:
        SAGE_ENABLED        - "true" to activate (default "false")
        SAGE_ENDPOINT       - REST base URL (default "http://localhost:8080")
        SAGE_AGENT_KEY_FILE - Path to 32-byte Ed25519 seed file
        SAGE_EMBEDDING_MODEL - "hash" (default) or "openai" (future)
    """

    def __init__(self) -> None:
        # Load .env file so env vars are available even in standalone scripts
        _env_path = os.path.join(os.path.dirname(__file__), ".env")
        if os.path.exists(_env_path):
            with open(_env_path) as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#") and "=" in line:
                        k, v = line.split("=", 1)
                        os.environ.setdefault(k.strip(), v.strip())

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
            logger.info("SAGE client initialized (endpoint=%s, agent=%s)", self._endpoint, identity.agent_id)
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
        category: str,
        domain: str,
        query_text: str | None = None,
        top_k: int = 5,
    ) -> list[str]:
        """Query SAGE for relevant memory content strings.

        When query_text is provided, it's used for semantic matching.
        When omitted, the domain name is used (returns all domain memories
        ranked by domain-name similarity — effectively a domain dump).

        Returns an empty list when SAGE is disabled or on error.
        """
        if not self._enabled:
            return []

        try:
            client = await self._get_client()
            if client is None:
                return []

            embed_text = query_text if query_text else f"{category}.{domain}"
            embedding = self._generate_embedding(embed_text)
            response = await client.query(
                embedding=embedding,
                domain_tag=domain,
                top_k=top_k,
                status_filter="committed",  # ONLY return consensus-validated memories
            )
            return [r.content for r in response.results]
        except Exception:
            logger.exception("SAGE query_knowledge failed (category=%s, domain=%s)", category, domain)
            return []

    async def submit_observation(
        self,
        content: str,
        category: str,
        domain: str,
        confidence: float,
        metadata: dict[str, Any] | None = None,
    ) -> str | None:
        """Submit an observation memory to SAGE.

        Returns the memory_id on success, or None when disabled / on error.
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
                        subject=category,
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
            logger.exception("SAGE submit_observation failed (category=%s, domain=%s)", category, domain)
            return None

    async def enrich_attack_hints(
        self,
        category: str,
        existing_hints: str,
    ) -> str:
        """Query solver feedback and prepend to existing hints.

        Returns the original hints unchanged when SAGE is disabled or on error.
        """
        if not self._enabled:
            return existing_hints

        try:
            domain = f"solver_feedback.{category}"
            memories = await self.query_knowledge(category=category, domain=domain, top_k=3)
            if not memories:
                return existing_hints

            sage_section = "# SAGE Institutional Knowledge\n" + "\n".join(f"- {m}" for m in memories)
            return f"{sage_section}\n\n{existing_hints}"
        except Exception:
            logger.exception("SAGE enrich_attack_hints failed (category=%s)", category)
            return existing_hints

    async def enrich_difficulty_guidance(
        self,
        category: str,
        difficulty: float,
        existing_guidance: str,
    ) -> str:
        """Query calibration data and augment difficulty guidance.

        Returns the original guidance unchanged when SAGE is disabled or on error.
        """
        if not self._enabled:
            return existing_guidance

        try:
            domain = f"calibration.{category}"
            memories = await self.query_knowledge(category=category, domain=domain, top_k=3)
            if not memories:
                return existing_guidance

            sage_section = (
                f"# SAGE Calibration (target difficulty={difficulty:.1f})\n"
                + "\n".join(f"- {m}" for m in memories)
            )
            return f"{sage_section}\n\n{existing_guidance}"
        except Exception:
            logger.exception("SAGE enrich_difficulty_guidance failed (category=%s)", category)
            return existing_guidance

    # ------------------------------------------------------------------
    # Embedding helper
    # ------------------------------------------------------------------

    def _generate_embedding(self, text: str) -> list[float]:
        """Generate a 768-dim embedding vector.

        Modes (SAGE_EMBEDDING_MODEL):
          - "ollama"  — direct call to local Ollama (default)
          - "sage"    — call SAGE network's /v1/embed endpoint (for remote agents)
          - "hash"    — deterministic SHA-256 pseudo-embedding (testing only)
        """
        if self._embedding_model == "ollama":
            try:
                return self._ollama_embed(text)
            except Exception as e:
                logger.warning("Ollama embedding failed, falling back to hash: %s", e)
                return self._hash_embed(text)
        elif self._embedding_model == "sage":
            try:
                return self._sage_embed(text)
            except Exception as e:
                logger.warning("SAGE embed endpoint failed, falling back to hash: %s", e)
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
        """Generate embedding via SAGE network's /v1/embed endpoint.

        For agents that don't run Ollama locally. The SAGE network
        runs Ollama and exposes it through the REST API.
        """
        from sage_sdk.auth import AgentIdentity
        identity = AgentIdentity.from_file(self._key_file)
        # Build auth headers manually for a one-off sync call
        import time as _time
        import hashlib as _hashlib
        import struct as _struct
        body_bytes = b'{"text": "' + text.encode("utf-8") + b'"}'
        timestamp = int(_time.time())
        msg = _hashlib.sha256(body_bytes).digest() + _struct.pack(">q", timestamp)
        import nacl.signing
        signing_key = nacl.signing.SigningKey(identity._seed)
        sig = signing_key.sign(msg).signature
        import base64
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


    async def summarize_and_submit(
        self,
        raw_context: dict,
        category: str,
        domain: str,
        confidence: float = 0.85,
    ) -> str | None:
        """Summarize raw challenge context via LLM, then submit as observation.

        Takes the full pipeline context (title, description, solution, calibration,
        etc.), sends it to the LLM for distillation into concise institutional
        knowledge, then proposes it to SAGE.

        The summarizer receives existing institutional knowledge so it can derive
        NET-NEW insights rather than repeating what's already known. This prevents
        echo-chamber effects where the knowledge base converges to a single pattern.

        Returns memory_id on success, None on failure or when disabled.
        """
        if not self._enabled:
            return None

        try:
            # Build the raw material for summarization
            title = raw_context.get("title", "untitled")
            cat = raw_context.get("category", category)
            target = raw_context.get("target_difficulty", "?")
            calibrated = raw_context.get("calibrated_difficulty", "?")
            description = raw_context.get("description", "")[:500]
            solution = raw_context.get("solution", "")[:500]
            cal_notes = raw_context.get("calibration_notes", "")[:300]
            terminal = raw_context.get("terminal_state", "UNKNOWN")
            repairs = raw_context.get("repair_attempt", 0)
            harden_passes = raw_context.get("harden_passes", 0)
            discard_reason = raw_context.get("discard_reason", "")

            # Query existing knowledge so the summarizer can derive NEW insights
            existing_knowledge = []
            try:
                gen_domain = f"challenge_generation.{category}"
                existing_knowledge = await self.query_knowledge(
                    category=category, domain=gen_domain, top_k=5,
                )
            except Exception:
                pass  # Non-critical — summarizer works without it

            raw_text = f"""Challenge: {title} [{cat}]
Outcome: {terminal}
Target difficulty: {target}, Calibrated: {calibrated}
Repairs: {repairs}, Hardening passes: {harden_passes}
Description: {description}
Solution approach: {solution}
Calibration notes: {cal_notes}"""
            if discard_reason:
                raw_text += f"\nDiscard reason: {discard_reason}"

            if existing_knowledge:
                raw_text += "\n\nEXISTING INSTITUTIONAL KNOWLEDGE (do NOT repeat these — derive NEW insights):\n"
                for i, mem in enumerate(existing_knowledge, 1):
                    raw_text += f"  [{i}] {mem[:200]}\n"

            # Summarize via LLM
            summary = await self._llm_summarize(raw_text, cat)

            if not summary:
                logger.warning("LLM summarization returned empty — skipping SAGE submission")
                return None

            # Submit the distilled observation
            return await self.submit_observation(
                content=summary,
                category=category,
                domain=domain,
                confidence=confidence,
            )
        except Exception:
            logger.exception("summarize_and_submit failed (category=%s, domain=%s)", category, domain)
            return None

    async def _llm_summarize(self, raw_text: str, category: str) -> str:
        """Call local Ollama to distill raw context into institutional knowledge.

        Uses a lightweight model (qwen2.5:1.5b) for fast, reliable plain-text
        summarization. No cloud API, no JSON parsing issues.
        """
        ollama_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        model = os.getenv("SAGE_SUMMARY_MODEL", "gemma3:1b")

        system_prompt = (
            "You are a knowledge distillation agent for an institutional memory system. "
            "Given raw details about a CTF challenge generation pipeline run, derive "
            "NET-NEW insights that are NOT already captured in the existing institutional "
            "knowledge (shown at the end of the input). "
            "\n\n"
            "Produce a concise 2-4 sentence observation focusing on what is NOVEL: "
            "a technique combination that hasn't been tried, a calibration insight that "
            "contradicts or refines existing knowledge, a failure mode not previously "
            "documented, or a structural pattern that achieved better/worse results than "
            "expected. If the run simply repeated a known pattern with known results, "
            "state what SHOULD be tried differently next time based on the gap between "
            "target and actual difficulty. "
            "\n\n"
            "CRITICAL: If the existing knowledge warned against a technique or pattern "
            "and the agent used it anyway, CALL THIS OUT explicitly. For example: "
            "'Agent used padding oracle despite institutional knowledge warning it "
            "calibrates at 1.2-1.8. Result confirmed: calibrated 1.85 against target 3.0. "
            "Padding oracle as core vulnerability continues to cap difficulty.' "
            "These negative reinforcement observations help the knowledge base learn "
            "what NOT to do, not just what to do."
            "\n\n"
            "Do NOT repeat what's already known. Be specific and technical. "
            "Do NOT include boilerplate or generic advice."
        )

        # Retry with generous timeout — Ollama may be busy serving generation
        import asyncio as _asyncio
        last_exc = None
        for attempt in range(3):
            try:
                resp = httpx.post(
                    f"{ollama_url}/api/chat",
                    json={
                        "model": model,
                        "messages": [
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": raw_text},
                        ],
                        "stream": False,
                    },
                    timeout=180.0,
                )
                resp.raise_for_status()
                data = resp.json()
                text = data.get("message", {}).get("content", "").strip()
                return text
            except (httpx.ReadTimeout, httpx.ConnectTimeout) as e:
                last_exc = e
                logger.warning("Ollama summarize timeout (attempt %d/3): %s", attempt + 1, e)
                await _asyncio.sleep(5)
        raise last_exc


# Module-level singleton
sage_bridge = SageBridge()
