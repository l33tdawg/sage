#!/usr/bin/env python3
"""(S)AGE SDK Multi-Agent Collaboration Example.

Demonstrates two agents collaborating on shared institutional memory:
  - Agent A submits a memory
  - Agent B retrieves, votes, and corroborates it
  - Both agents query the shared memory pool

This shows how SAGE enables multi-agent knowledge sharing with
cryptographic identity and verifiable provenance.

Usage:
    python examples/multi_agent.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    # --- Create two distinct agent identities ---
    agent_a = AgentIdentity.generate()
    agent_b = AgentIdentity.generate()
    print("Multi-Agent Collaboration Demo")
    print("=" * 60)
    print(f"Agent A: {agent_a.agent_id[:16]}...")
    print(f"Agent B: {agent_b.agent_id[:16]}...")
    print(f"SAGE URL: {base_url}")

    client_a = SageClient(base_url=base_url, identity=agent_a)
    client_b = SageClient(base_url=base_url, identity=agent_b)

    try:
        # --- Agent A: Submit a memory ---
        print("\n--- Agent A: Submitting observation ---")
        result = client_a.propose(
            content="The API response time for /users endpoint averages 45ms under normal load.",
            memory_type="observation",
            domain_tag="performance",
            confidence=0.85,
        )
        memory_id = result.memory_id
        print(f"    memory_id: {memory_id}")
        print(f"    tx_hash:   {result.tx_hash}")

        # --- Agent B: Retrieve and review ---
        print(f"\n--- Agent B: Retrieving memory {memory_id} ---")
        memory = client_b.get_memory(memory_id)
        print(f"    content:    {memory.content}")
        print(f"    submitted by: {memory.submitting_agent[:16]}...")
        print(f"    status:     {memory.status.value}")

        # --- Agent B: Vote to accept ---
        print(f"\n--- Agent B: Voting to accept ---")
        vote_result = client_b.vote(
            memory_id=memory_id,
            decision="accept",
            rationale="Consistent with my own monitoring data showing 40-50ms range.",
        )
        print(f"    vote result: {vote_result}")

        # --- Agent B: Corroborate with its own evidence ---
        print(f"\n--- Agent B: Corroborating with evidence ---")
        corr_result = client_b.corroborate(
            memory_id=memory_id,
            evidence="My load tests from 2024-01-15 show p50=42ms, p99=67ms for /users.",
        )
        print(f"    corroboration result: {corr_result}")

        # --- Agent A: Submit a second memory ---
        print("\n--- Agent A: Submitting inference ---")
        result2 = client_a.propose(
            content="Based on current trends, /users endpoint will need caching when traffic exceeds 1000 RPS.",
            memory_type="inference",
            domain_tag="performance",
            confidence=0.70,
            parent_hash=memory.content_hash,
        )
        print(f"    memory_id: {result2.memory_id}")
        print(f"    (linked to parent observation)")

        # --- Both agents query the shared memory pool ---
        print("\n--- Agent A: Querying performance memories ---")
        dummy_embedding = [0.1] * 768  # nomic-embed-text dimension
        query_a = client_a.query(
            embedding=dummy_embedding,
            domain_tag="performance",
            top_k=10,
        )
        print(f"    Agent A sees {query_a.total_count} memories in 'performance' domain")

        print("\n--- Agent B: Querying performance memories ---")
        query_b = client_b.query(
            embedding=dummy_embedding,
            domain_tag="performance",
            top_k=10,
        )
        print(f"    Agent B sees {query_b.total_count} memories in 'performance' domain")

        # --- Check profiles ---
        print("\n--- Agent profiles ---")
        profile_a = client_a.get_profile()
        profile_b = client_b.get_profile()
        print(f"    Agent A: poe_weight={profile_a.poe_weight}, votes={profile_a.vote_count}")
        print(f"    Agent B: poe_weight={profile_b.poe_weight}, votes={profile_b.vote_count}")

    finally:
        client_a._client.close()
        client_b._client.close()

    print("\n" + "=" * 60)
    print("Multi-agent collaboration complete!")
    print("Both agents share the same verifiable memory pool,")
    print("with cryptographic provenance for every contribution.")


if __name__ == "__main__":
    try:
        main()
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
