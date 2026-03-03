#!/usr/bin/env python3
"""(S)AGE SDK Full Lifecycle Example.

Demonstrates the complete memory lifecycle:
  submit -> get -> vote -> corroborate -> challenge -> query

This walks through every SDK operation in sequence.

Usage:
    python examples/full_lifecycle.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError
from sage_sdk.models import KnowledgeTriple


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    identity = AgentIdentity.generate()
    print(f"Agent ID: {identity.agent_id[:16]}...")
    print(f"SAGE URL: {base_url}")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=identity) as client:

        # --- 1. Check agent profile ---
        print("\n[1/7] Getting agent profile...")
        profile = client.get_profile()
        print(f"       agent_id:   {profile.agent_id[:16]}...")
        print(f"       poe_weight: {profile.poe_weight}")
        print(f"       vote_count: {profile.vote_count}")

        # --- 2. Submit a memory with knowledge triples ---
        print("\n[2/7] Submitting memory with knowledge triples...")
        result = client.propose(
            content="Python was created by Guido van Rossum and first released in 1991.",
            memory_type="fact",
            domain_tag="programming",
            confidence=0.98,
            knowledge_triples=[
                KnowledgeTriple(
                    subject="Python",
                    predicate="created_by",
                    object_="Guido van Rossum",
                ),
                KnowledgeTriple(
                    subject="Python",
                    predicate="first_released",
                    object_="1991",
                ),
            ],
        )
        memory_id = result.memory_id
        print(f"       memory_id: {memory_id}")
        print(f"       tx_hash:   {result.tx_hash}")

        # --- 3. Retrieve the memory ---
        print(f"\n[3/7] Retrieving memory {memory_id}...")
        memory = client.get_memory(memory_id)
        print(f"       content:    {memory.content}")
        print(f"       status:     {memory.status.value}")
        print(f"       confidence: {memory.confidence_score}")

        # --- 4. Vote to accept ---
        print(f"\n[4/7] Voting to accept memory {memory_id}...")
        vote_result = client.vote(
            memory_id=memory_id,
            decision="accept",
            rationale="Verified: Python 0.9.0 was released Feb 1991 by Guido van Rossum.",
        )
        print(f"       vote result: {vote_result}")

        # --- 5. Corroborate with evidence ---
        print(f"\n[5/7] Corroborating memory {memory_id}...")
        corr_result = client.corroborate(
            memory_id=memory_id,
            evidence="Confirmed via python.org/doc/essays/foreword — first public release was February 1991.",
        )
        print(f"       corroboration result: {corr_result}")

        # --- 6. Challenge the memory ---
        print(f"\n[6/7] Challenging memory {memory_id}...")
        challenge_result = client.challenge(
            memory_id=memory_id,
            reason="Clarification: Python 0.9.0 was released in Feb 1991, but 1.0 was Jan 1994.",
            evidence="https://docs.python.org/3/whatsnew/changelog.html",
        )
        print(f"       challenge result: {challenge_result}")

        # --- 7. Query memories by embedding ---
        print("\n[7/7] Querying memories (vector similarity search)...")
        # A dummy embedding vector (768-dim to match nomic-embed-text)
        dummy_embedding = [0.1] * 768
        query_result = client.query(
            embedding=dummy_embedding,
            domain_tag="programming",
            min_confidence=0.5,
            top_k=5,
        )
        print(f"       total results: {query_result.total_count}")
        for i, mem in enumerate(query_result.results):
            print(f"       [{i+1}] {mem.content[:60]}... (confidence={mem.confidence_score})")

    print("\n" + "=" * 60)
    print("Full lifecycle complete!")


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
