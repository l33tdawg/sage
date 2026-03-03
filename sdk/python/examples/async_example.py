#!/usr/bin/env python3
"""(S)AGE SDK Async Example.

Demonstrates using the AsyncSageClient for non-blocking operations.
This is ideal for agents that need to interact with SAGE alongside
other async workloads (web servers, message queues, etc.).

Usage:
    python examples/async_example.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import asyncio
import os
import sys

from sage_sdk import AgentIdentity, AsyncSageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


async def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    identity = AgentIdentity.generate()
    print(f"Agent ID: {identity.agent_id[:16]}...")
    print(f"SAGE URL: {base_url}")
    print("=" * 60)

    async with AsyncSageClient(base_url=base_url, identity=identity) as client:

        # --- Submit a memory asynchronously ---
        print("\n[1/4] Submitting memory (async)...")
        result = await client.propose(
            content="Async operations reduce latency in I/O-bound agent workloads.",
            memory_type="observation",
            domain_tag="engineering",
            confidence=0.90,
        )
        memory_id = result.memory_id
        print(f"       memory_id: {memory_id}")
        print(f"       tx_hash:   {result.tx_hash}")

        # --- Retrieve it back ---
        print(f"\n[2/4] Retrieving memory (async)...")
        memory = await client.get_memory(memory_id)
        print(f"       content:    {memory.content}")
        print(f"       status:     {memory.status.value}")

        # --- Vote and corroborate concurrently ---
        print("\n[3/4] Voting and corroborating concurrently (async)...")
        vote_task = client.vote(
            memory_id=memory_id,
            decision="accept",
            rationale="Matches known async I/O performance characteristics.",
        )
        corr_task = client.corroborate(
            memory_id=memory_id,
            evidence="Benchmarks show 3x throughput improvement with async HTTP clients.",
        )
        vote_result, corr_result = await asyncio.gather(vote_task, corr_task)
        print(f"       vote result:          {vote_result}")
        print(f"       corroboration result: {corr_result}")

        # --- Query memories ---
        print("\n[4/4] Querying memories (async)...")
        dummy_embedding = [0.1] * 768  # nomic-embed-text dimension
        query_result = await client.query(
            embedding=dummy_embedding,
            domain_tag="engineering",
            top_k=5,
        )
        print(f"       total results: {query_result.total_count}")
        for i, mem in enumerate(query_result.results):
            print(f"       [{i+1}] {mem.content[:60]}...")

    print("\n" + "=" * 60)
    print("Async example complete!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SageAuthError as e:
        print(f"\nAuthentication error: {e}", file=sys.stderr)
        sys.exit(1)
    except SageAPIError as e:
        print(f"\nAPI error (HTTP {e.status_code}): {e.detail}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"\nConnection error: {e}", file=sys.stderr)
        print("Is the SAGE network running? Try: make up", file=sys.stderr)
        sys.exit(1)
