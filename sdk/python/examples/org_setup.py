#!/usr/bin/env python3
"""(S)AGE SDK Example: Organization, Departments, and RBAC.

Demonstrates setting up a complete organizational hierarchy on-chain:
  1. Create an organization
  2. Create departments within the organization
  3. Add agents as members with clearance levels
  4. Register knowledge domains
  5. Grant domain access with RBAC controls
  6. Submit memories scoped to domains

This models a real-world scenario: a security research lab with
separate teams for vulnerability research and cryptography, each
with their own knowledge domains and clearance levels.

All RBAC state is committed to the BFT chain -- it survives node
failures and is replicated across all validators.

Usage:
    python examples/org_setup.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    # ── Create three agents: an admin and two researchers ──────────

    admin = AgentIdentity.generate()
    vuln_researcher = AgentIdentity.generate()
    crypto_researcher = AgentIdentity.generate()

    print(f"Admin agent:           {admin.agent_id[:16]}...")
    print(f"Vuln researcher:       {vuln_researcher.agent_id[:16]}...")
    print(f"Crypto researcher:     {crypto_researcher.agent_id[:16]}...")
    print()

    with SageClient(base_url=base_url, identity=admin) as client:

        # ── Step 1: Register an organization ──────────────────────

        print("[1/7] Registering organization...")
        org = client.register_org(
            name="Acme Security Lab",
            description="Applied security research division",
        )
        org_id = org["org_id"]
        print(f"       org_id: {org_id}")
        print(f"       tx_hash: {org['tx_hash']}")
        print()

        # ── Step 2: Create departments ────────────────────────────

        print("[2/7] Creating departments...")

        vuln_dept = client.register_dept(
            org_id=org_id,
            name="Vulnerability Research",
            description="Discovers and analyzes software vulnerabilities",
        )
        vuln_dept_id = vuln_dept["dept_id"]
        print(f"       Vuln Research dept_id: {vuln_dept_id}")

        crypto_dept = client.register_dept(
            org_id=org_id,
            name="Cryptography",
            description="Cryptographic protocol analysis and implementation",
        )
        crypto_dept_id = crypto_dept["dept_id"]
        print(f"       Cryptography dept_id:  {crypto_dept_id}")
        print()

        # ── Step 3: Add members with clearance levels ─────────────
        #
        # Clearance levels (0-4):
        #   0 = no access (observer)
        #   1 = read (default)
        #   2 = read + write
        #   3 = read + write + validate
        #   4 = admin (full control)

        print("[3/7] Adding members to departments...")

        # Admin gets clearance 4 in both departments
        client.add_dept_member(
            org_id=org_id,
            dept_id=vuln_dept_id,
            agent_id=admin.agent_id,
            clearance=4,
            role="admin",
        )
        client.add_dept_member(
            org_id=org_id,
            dept_id=crypto_dept_id,
            agent_id=admin.agent_id,
            clearance=4,
            role="admin",
        )
        print(f"       Admin added to both depts (clearance=4)")

        # Vuln researcher gets clearance 3 in their dept, clearance 1 in crypto
        client.add_dept_member(
            org_id=org_id,
            dept_id=vuln_dept_id,
            agent_id=vuln_researcher.agent_id,
            clearance=3,
            role="lead",
        )
        client.add_dept_member(
            org_id=org_id,
            dept_id=crypto_dept_id,
            agent_id=vuln_researcher.agent_id,
            clearance=1,
            role="observer",
        )
        print(f"       Vuln researcher: clearance=3 (vuln), clearance=1 (crypto)")

        # Crypto researcher gets clearance 3 in crypto only
        client.add_dept_member(
            org_id=org_id,
            dept_id=crypto_dept_id,
            agent_id=crypto_researcher.agent_id,
            clearance=3,
            role="lead",
        )
        print(f"       Crypto researcher: clearance=3 (crypto only)")
        print()

        # ── Step 4: Register knowledge domains ────────────────────

        print("[4/7] Registering knowledge domains...")

        client.register_domain(
            name="vuln_intel",
            description="Vulnerability intelligence -- CVEs, exploits, attack patterns",
        )
        print(f"       Registered domain: vuln_intel")

        client.register_domain(
            name="crypto_analysis",
            description="Cryptographic protocol analysis and weaknesses",
        )
        print(f"       Registered domain: crypto_analysis")
        print()

        # ── Step 5: Grant domain access ───────────────────────────

        print("[5/7] Granting domain access...")

        # Vuln researcher gets write access to vuln_intel
        client.grant_access(
            grantee_id=vuln_researcher.agent_id,
            domain="vuln_intel",
            level=2,  # read + write
        )
        print(f"       Vuln researcher -> vuln_intel (level=2, read+write)")

        # Crypto researcher gets write access to crypto_analysis
        client.grant_access(
            grantee_id=crypto_researcher.agent_id,
            domain="crypto_analysis",
            level=2,
        )
        print(f"       Crypto researcher -> crypto_analysis (level=2, read+write)")

        # Both get read access to each other's domain for cross-pollination
        client.grant_access(
            grantee_id=vuln_researcher.agent_id,
            domain="crypto_analysis",
            level=1,  # read only
        )
        client.grant_access(
            grantee_id=crypto_researcher.agent_id,
            domain="vuln_intel",
            level=1,
        )
        print(f"       Cross-domain read access granted")
        print()

        # ── Step 6: List the org structure ────────────────────────

        print("[6/7] Verifying organization structure...")

        members = client.list_org_members(org_id)
        print(f"       Org members: {len(members)}")

        vuln_members = client.list_dept_members(org_id, vuln_dept_id)
        print(f"       Vuln Research members: {len(vuln_members)}")
        for m in vuln_members:
            print(f"         - {m['agent_id'][:16]}... role={m['role']} clearance={m['clearance']}")

        crypto_members = client.list_dept_members(org_id, crypto_dept_id)
        print(f"       Cryptography members: {len(crypto_members)}")
        for m in crypto_members:
            print(f"         - {m['agent_id'][:16]}... role={m['role']} clearance={m['clearance']}")

        grants = client.list_grants(vuln_researcher.agent_id)
        print(f"       Vuln researcher grants: {len(grants)}")
        for g in grants:
            print(f"         - domain={g['domain']} level={g.get('access_level', g.get('level', '?'))}")
        print()

    # ── Step 7: Submit memories as different agents ───────────────

    print("[7/7] Submitting domain-scoped memories...")

    # Vuln researcher submits to their domain
    with SageClient(base_url=base_url, identity=vuln_researcher) as client:
        result = client.propose(
            content="CVE-2024-3094 (XZ Utils backdoor) used IFUNC hooking to intercept OpenSSH authentication.",
            memory_type="fact",
            domain_tag="vuln_intel",
            confidence=0.95,
        )
        print(f"       Vuln researcher submitted memory: {result.memory_id[:16]}...")

    # Crypto researcher submits to their domain
    with SageClient(base_url=base_url, identity=crypto_researcher) as client:
        result = client.propose(
            content="AES-GCM nonce reuse allows forgery via polynomial evaluation -- the 'forbidden attack'.",
            memory_type="fact",
            domain_tag="crypto_analysis",
            confidence=0.93,
        )
        print(f"       Crypto researcher submitted memory: {result.memory_id[:16]}...")

    print()
    print("Done! Organization hierarchy, RBAC, and domain-scoped memories")
    print("are all committed to the BFT chain.")


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
