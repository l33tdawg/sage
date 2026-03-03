#!/usr/bin/env python3
"""(S)AGE SDK Example: RBAC Clearance Levels in Action.

Demonstrates how clearance levels work across organizations and
departments -- like security clearances in government agencies.

Scenario: A national cyber defense organization with three tiers:

  Organization: "National Cyber Command"
    |
    +-- Department: "Strategic Intelligence"  (clearance 4 = top secret)
    |     +-- Director (clearance 4, admin)
    |     +-- Senior Analyst (clearance 3, analyst)
    |
    +-- Department: "Tactical Operations"  (clearance 2 = operational)
    |     +-- Team Lead (clearance 3, lead)
    |     +-- Operator (clearance 2, member)
    |
    +-- Department: "Public Affairs"  (clearance 1 = public)
          +-- Spokesperson (clearance 1, member)

  Knowledge Domains:
    "classified_intel"    -- requires clearance >= 3 to write
    "operational_data"    -- requires clearance >= 2 to write
    "public_advisories"   -- requires clearance >= 1 to write

Each agent can only submit memories to domains their clearance
permits. The RBAC state is on-chain and auditable.

Usage:
    python examples/rbac_clearance.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    # ── Create agents at different clearance levels ───────────────

    director = AgentIdentity.generate()        # Top secret (clearance 4)
    senior_analyst = AgentIdentity.generate()   # Secret (clearance 3)
    team_lead = AgentIdentity.generate()        # Operational (clearance 3)
    operator = AgentIdentity.generate()         # Restricted (clearance 2)
    spokesperson = AgentIdentity.generate()     # Public (clearance 1)

    print("=== Agent Roster ===")
    print(f"  Director:       {director.agent_id[:16]}...  (will get clearance 4)")
    print(f"  Senior Analyst: {senior_analyst.agent_id[:16]}...  (will get clearance 3)")
    print(f"  Team Lead:      {team_lead.agent_id[:16]}...  (will get clearance 3)")
    print(f"  Operator:       {operator.agent_id[:16]}...  (will get clearance 2)")
    print(f"  Spokesperson:   {spokesperson.agent_id[:16]}...  (will get clearance 1)")
    print()

    # ── Build the organization ────────────────────────────────────

    print("[1/5] Building organization hierarchy...")

    with SageClient(base_url=base_url, identity=director) as client:

        # Create the org (director becomes admin automatically)
        org = client.register_org(
            name="National Cyber Command",
            description="National-level cyber defense coordination",
        )
        org_id = org["org_id"]
        print(f"  Created org: {org_id[:16]}...")

        # ── Create departments ────────────────────────────────────

        intel_dept = client.register_dept(
            org_id=org_id,
            name="Strategic Intelligence",
            description="Classified threat intelligence and attribution",
        )
        intel_id = intel_dept["dept_id"]

        ops_dept = client.register_dept(
            org_id=org_id,
            name="Tactical Operations",
            description="Active defense and incident response",
        )
        ops_id = ops_dept["dept_id"]

        public_dept = client.register_dept(
            org_id=org_id,
            name="Public Affairs",
            description="Public-facing advisories and communications",
        )
        public_id = public_dept["dept_id"]

        print(f"  Departments: Strategic Intelligence, Tactical Operations, Public Affairs")

        # ── Assign agents to departments with clearance levels ────
        #
        # Clearance levels determine what an agent can do:
        #   4 = Top Secret (admin -- full control, can grant/revoke)
        #   3 = Secret     (can read, write, AND validate memories)
        #   2 = Restricted (can read and write, but NOT validate)
        #   1 = Public     (read-only access)
        #   0 = None       (observer, no access)

        # Director: clearance 4 in Strategic Intelligence
        client.add_dept_member(
            org_id=org_id, dept_id=intel_id,
            agent_id=director.agent_id, clearance=4, role="director",
        )

        # Senior Analyst: clearance 3 in Strategic Intelligence
        client.add_dept_member(
            org_id=org_id, dept_id=intel_id,
            agent_id=senior_analyst.agent_id, clearance=3, role="senior_analyst",
        )

        # Team Lead: clearance 3 in Tactical Operations
        client.add_dept_member(
            org_id=org_id, dept_id=ops_id,
            agent_id=team_lead.agent_id, clearance=3, role="team_lead",
        )

        # Operator: clearance 2 in Tactical Operations
        client.add_dept_member(
            org_id=org_id, dept_id=ops_id,
            agent_id=operator.agent_id, clearance=2, role="operator",
        )

        # Spokesperson: clearance 1 in Public Affairs
        client.add_dept_member(
            org_id=org_id, dept_id=public_id,
            agent_id=spokesperson.agent_id, clearance=1, role="spokesperson",
        )

        print("  All agents assigned with clearance levels")
    print()

    # ── Register knowledge domains ────────────────────────────────

    print("[2/5] Registering knowledge domains...")

    with SageClient(base_url=base_url, identity=director) as client:

        client.register_domain(
            name="classified_intel",
            description="Classified threat intelligence -- APT attribution, zero-days, national threats",
        )
        client.register_domain(
            name="operational_data",
            description="Operational incident data -- IOCs, response actions, tactical findings",
        )
        client.register_domain(
            name="public_advisories",
            description="Public-facing security advisories and best practices",
        )
        print("  Domains: classified_intel, operational_data, public_advisories")
    print()

    # ── Grant access based on clearance ───────────────────────────

    print("[3/5] Granting domain access by clearance level...")

    with SageClient(base_url=base_url, identity=director) as client:

        # Director: write access to everything (clearance 4)
        for domain in ["classified_intel", "operational_data", "public_advisories"]:
            client.grant_access(
                grantee_id=director.agent_id, domain=domain, level=4,
            )
        print("  Director       -> ALL domains (level=4, admin)")

        # Senior Analyst: write classified + operational, read public
        client.grant_access(
            grantee_id=senior_analyst.agent_id, domain="classified_intel", level=3,
        )
        client.grant_access(
            grantee_id=senior_analyst.agent_id, domain="operational_data", level=2,
        )
        client.grant_access(
            grantee_id=senior_analyst.agent_id, domain="public_advisories", level=1,
        )
        print("  Senior Analyst -> classified(3), operational(2), public(1)")

        # Team Lead: write operational, read classified + public
        client.grant_access(
            grantee_id=team_lead.agent_id, domain="classified_intel", level=1,
        )
        client.grant_access(
            grantee_id=team_lead.agent_id, domain="operational_data", level=3,
        )
        client.grant_access(
            grantee_id=team_lead.agent_id, domain="public_advisories", level=1,
        )
        print("  Team Lead      -> classified(1), operational(3), public(1)")

        # Operator: write operational only
        client.grant_access(
            grantee_id=operator.agent_id, domain="operational_data", level=2,
        )
        print("  Operator       -> operational(2) only")

        # Spokesperson: write public only
        client.grant_access(
            grantee_id=spokesperson.agent_id, domain="public_advisories", level=2,
        )
        print("  Spokesperson   -> public(2) only")
    print()

    # ── Each agent submits memories at their clearance level ──────

    print("[4/5] Agents submitting memories at their access level...")

    # Director submits classified intel
    with SageClient(base_url=base_url, identity=director) as client:
        result = client.propose(
            content="APT41 (Double Dragon) has been observed using a new supply chain "
                    "vector targeting CI/CD pipelines via compromised GitHub Actions.",
            memory_type="observation",
            domain_tag="classified_intel",
            confidence=0.89,
        )
        print(f"  Director -> classified_intel: {result.memory_id[:16]}...")

    # Senior Analyst corroborates and adds their own
    with SageClient(base_url=base_url, identity=senior_analyst) as client:
        client.corroborate(
            memory_id=result.memory_id,
            evidence="Corroborated via SIGINT report SR-2026-0847. "
                     "Matches TTP pattern from APT41 tooling analysis.",
        )
        print(f"  Senior Analyst corroborated director's finding")

        result2 = client.propose(
            content="The compromised GitHub Action uses a typosquatted package name "
                    "differing by one character from the legitimate action.",
            memory_type="fact",
            domain_tag="classified_intel",
            confidence=0.92,
        )
        print(f"  Senior Analyst -> classified_intel: {result2.memory_id[:16]}...")

    # Team Lead submits operational data
    with SageClient(base_url=base_url, identity=team_lead) as client:
        result3 = client.propose(
            content="IOC: SHA-256 a3f7c9d2e1b8... observed in compromised runner. "
                    "C2 callback to 198.51.100.42:8443 every 300s.",
            memory_type="observation",
            domain_tag="operational_data",
            confidence=0.95,
        )
        print(f"  Team Lead -> operational_data: {result3.memory_id[:16]}...")

    # Operator submits tactical finding
    with SageClient(base_url=base_url, identity=operator) as client:
        result4 = client.propose(
            content="Blocking egress to 198.51.100.0/24 on port 8443 stops "
                    "C2 callback. No impact on legitimate services observed.",
            memory_type="observation",
            domain_tag="operational_data",
            confidence=0.88,
        )
        print(f"  Operator -> operational_data: {result4.memory_id[:16]}...")

    # Spokesperson submits public advisory
    with SageClient(base_url=base_url, identity=spokesperson) as client:
        result5 = client.propose(
            content="Advisory: Verify GitHub Action sources before use. "
                    "Pin actions to specific commit SHAs, not tags.",
            memory_type="fact",
            domain_tag="public_advisories",
            confidence=0.99,
        )
        print(f"  Spokesperson -> public_advisories: {result5.memory_id[:16]}...")
    print()

    # ── Verify the access structure ───────────────────────────────

    print("[5/5] Verifying access grants...")

    with SageClient(base_url=base_url, identity=director) as client:

        agents = [
            ("Director", director),
            ("Senior Analyst", senior_analyst),
            ("Team Lead", team_lead),
            ("Operator", operator),
            ("Spokesperson", spokesperson),
        ]

        for name, agent in agents:
            grants = client.list_grants(agent.agent_id)
            domains = []
            for g in grants:
                level = g.get("access_level", g.get("level", "?"))
                domains.append(f"{g['domain']}({level})")
            print(f"  {name:20s} -> {', '.join(domains) if domains else '(no grants)'}")

    print()
    print("=" * 60)
    print("RBAC HIERARCHY SUMMARY")
    print("=" * 60)
    print()
    print("  Clearance 4 (Top Secret / Admin)")
    print("    -> Director: full access to all domains, can grant/revoke")
    print()
    print("  Clearance 3 (Secret / Validate)")
    print("    -> Senior Analyst: write classified + operational, validate")
    print("    -> Team Lead: write operational, read classified, validate")
    print()
    print("  Clearance 2 (Restricted / Write)")
    print("    -> Operator: write operational data only")
    print("    -> Spokesperson: write public advisories only")
    print()
    print("  Clearance 1 (Public / Read)")
    print("    -> Cross-domain read access for awareness")
    print()
    print("All access grants are on-chain, auditable, and replicated")
    print("across the BFT validator network.")


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
