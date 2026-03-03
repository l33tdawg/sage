#!/usr/bin/env python3
"""(S)AGE SDK Quickstart Example.

Demonstrates the minimal flow: generate an agent identity, submit a memory,
and retrieve it back. This is the simplest possible (S)AGE interaction.

Usage:
    python examples/quickstart.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    # --- Step 1: Generate a fresh agent identity ---
    identity = AgentIdentity.generate()
    print(f"[1/4] Generated agent identity: {identity.agent_id[:16]}...")

    # --- Step 2: Connect to SAGE ---
    with SageClient(base_url=base_url, identity=identity) as client:
        print(f"[2/4] Connected to SAGE at {base_url}")

        # --- Step 3: Submit a memory ---
        print("[3/4] Submitting memory...")
        result = client.propose(
            content="The Burj Khalifa is 828 meters tall.",
            memory_type="fact",
            domain_tag="architecture",
            confidence=0.95,
        )
        print(f"       Memory submitted!")
        print(f"       memory_id: {result.memory_id}")
        print(f"       tx_hash:   {result.tx_hash}")
        print(f"       status:    {result.status}")

        # --- Step 4: Retrieve the memory ---
        print(f"[4/4] Retrieving memory {result.memory_id}...")
        memory = client.get_memory(result.memory_id)
        print(f"       content:    {memory.content}")
        print(f"       type:       {memory.memory_type.value}")
        print(f"       domain:     {memory.domain_tag}")
        print(f"       confidence: {memory.confidence_score}")
        print(f"       status:     {memory.status.value}")
        print(f"       created_at: {memory.created_at}")

    print("\nDone! Memory successfully submitted and retrieved.")


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
