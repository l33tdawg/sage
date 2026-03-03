#!/usr/bin/env python3
"""(S)AGE SDK Example: Cross-Organization Federation.

Demonstrates how two separate organizations can establish a federation
agreement to share knowledge across organizational boundaries:

  1. Create two independent organizations with their own agents
  2. Each org registers knowledge domains and submits memories
  3. Org A proposes a federation with Org B
  4. Org B approves the federation
  5. Federated access allows cross-org knowledge queries

This models real-world scenarios like:
  - Two security teams sharing threat intelligence
  - Research labs collaborating across institutions
  - National AI infrastructure sharing validated knowledge

Federation state is on-chain -- both orgs can independently verify
the agreement, and revocation is immediate and auditable.

Usage:
    python examples/federation.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    # ── Create agents for two organizations ───────────────────────

    alpha_admin = AgentIdentity.generate()
    alpha_agent = AgentIdentity.generate()
    beta_admin = AgentIdentity.generate()
    beta_agent = AgentIdentity.generate()

    print("=== Organization Alpha ===")
    print(f"  Admin: {alpha_admin.agent_id[:16]}...")
    print(f"  Agent: {alpha_agent.agent_id[:16]}...")
    print()
    print("=== Organization Beta ===")
    print(f"  Admin: {beta_admin.agent_id[:16]}...")
    print(f"  Agent: {beta_agent.agent_id[:16]}...")
    print()

    # ── Set up Organization Alpha ─────────────────────────────────

    print("[1/6] Setting up Organization Alpha...")
    with SageClient(base_url=base_url, identity=alpha_admin) as client:
        alpha_org = client.register_org(
            name="Alpha Research",
            description="Offensive security research",
        )
        alpha_org_id = alpha_org["org_id"]
        print(f"       org_id: {alpha_org_id}")

        # Add member
        client.add_org_member(
            org_id=alpha_org_id,
            agent_id=alpha_agent.agent_id,
            clearance=3,
            role="researcher",
        )

        # Register domain and submit knowledge
        client.register_domain(
            name="alpha_threat_intel",
            description="Alpha's threat intelligence findings",
        )
        client.grant_access(
            grantee_id=alpha_agent.agent_id,
            domain="alpha_threat_intel",
            level=2,
        )
    print()

    # ── Set up Organization Beta ──────────────────────────────────

    print("[2/6] Setting up Organization Beta...")
    with SageClient(base_url=base_url, identity=beta_admin) as client:
        beta_org = client.register_org(
            name="Beta Defense",
            description="Defensive security operations",
        )
        beta_org_id = beta_org["org_id"]
        print(f"       org_id: {beta_org_id}")

        # Add member
        client.add_org_member(
            org_id=beta_org_id,
            agent_id=beta_agent.agent_id,
            clearance=3,
            role="analyst",
        )

        # Register domain and submit knowledge
        client.register_domain(
            name="beta_defense_patterns",
            description="Beta's defensive detection patterns",
        )
        client.grant_access(
            grantee_id=beta_agent.agent_id,
            domain="beta_defense_patterns",
            level=2,
        )
    print()

    # ── Each org submits domain knowledge ─────────────────────────

    print("[3/6] Submitting organizational knowledge...")

    with SageClient(base_url=base_url, identity=alpha_agent) as client:
        client.propose(
            content="APT29 (Cozy Bear) uses WellMess malware with custom TLS certificate pinning for C2.",
            memory_type="observation",
            domain_tag="alpha_threat_intel",
            confidence=0.91,
        )
        print("       Alpha submitted threat intelligence")

    with SageClient(base_url=base_url, identity=beta_agent) as client:
        client.propose(
            content="WellMess C2 traffic detectable via JA3 hash 72a589da586844d7f0818ce684948eea on port 443.",
            memory_type="observation",
            domain_tag="beta_defense_patterns",
            confidence=0.88,
        )
        print("       Beta submitted defense pattern")
    print()

    # ── Alpha proposes federation with Beta ───────────────────────

    print("[4/6] Alpha proposes federation with Beta...")
    with SageClient(base_url=base_url, identity=alpha_admin) as client:
        federation = client.propose_federation(
            target_org_id=beta_org_id,
            allowed_domains=["alpha_threat_intel", "beta_defense_patterns"],
            max_clearance=2,  # Federated agents get at most clearance 2
            requires_approval=True,
        )
        fed_id = federation["federation_id"]
        print(f"       federation_id: {fed_id}")
        print(f"       status: {federation['status']}")  # "proposed"
    print()

    # ── Beta approves the federation ──────────────────────────────

    print("[5/6] Beta approves the federation...")
    with SageClient(base_url=base_url, identity=beta_admin) as client:
        approval = client.approve_federation(fed_id)
        print(f"       status: {approval['status']}")  # "active"
    print()

    # ── Verify federation ─────────────────────────────────────────

    print("[6/6] Verifying federation...")
    with SageClient(base_url=base_url, identity=alpha_admin) as client:
        fed_info = client.get_federation(fed_id)
        print(f"       Federation: {fed_info['proposer_org_id'][:12]}... <-> {fed_info['target_org_id'][:12]}...")
        print(f"       Status: {fed_info['status']}")
        print(f"       Allowed domains: {fed_info.get('allowed_domains', [])}")
        print(f"       Max clearance: {fed_info.get('max_clearance', 'N/A')}")

        # List all active federations for Alpha
        active = client.list_federations(alpha_org_id)
        print(f"       Alpha active federations: {len(active)}")
    print()

    print("Done! Two organizations are now federated.")
    print("Agents from both orgs can access shared domains up to clearance level 2.")
    print("The federation agreement is on-chain and independently verifiable.")


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
        print("Is the (S)AGE network running? Try: make up", file=sys.stderr)
        sys.exit(1)
