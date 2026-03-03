#!/usr/bin/env python3
"""(S)AGE SDK Complete Walkthrough.

This is the "everything (S)AGE can do" reference. It walks through every
SDK operation with explanations of what's happening on-chain.

== What is (S)AGE? ==

(S)AGE is a shared memory layer for AI agents, backed by a blockchain.
The (S) stands for Sovereign -- the governance layer (orgs, depts, RBAC,
federation) is optional. The core is AGE: Agent Governed Experience.
Think of it like a database that multiple agents can write to, but:
  - Every write goes through BFT consensus (4 validator nodes vote)
  - Memories have confidence scores that decay over time
  - Agents build reputation through accurate contributions (Proof of Experience)
  - Access is controlled by on-chain RBAC (orgs, depts, clearance levels)
  - Everything is cryptographically signed (Ed25519)

You don't need to understand blockchain to use (S)AGE. The SDK handles
all the signing, transaction encoding, and consensus interaction.
You just call Python methods.

== What's running under the hood? ==

When you run `make up`, (S)AGE starts 11 Docker containers:

  4 x CometBFT nodes   -- BFT consensus (like a replicated log)
  4 x ABCI app nodes    -- (S)AGE state machine (processes transactions)
  1 x PostgreSQL        -- Stores actual memory content + embeddings
  1 x Ollama            -- Generates 768-dim embeddings locally
  1 x Ollama init       -- Pulls the embedding model, then exits

Your SDK calls hit the REST API on any ABCI node (ports 8080-8083).
The ABCI node wraps your request into a transaction, broadcasts it to
CometBFT, which runs BFT consensus across all 4 nodes. Once committed
to a block, the state machine updates PostgreSQL and BadgerDB.

== Sections ==

  1. Agent Identity     -- Ed25519 keypairs, signing
  2. Memory Operations  -- propose, query, get, corroborate, challenge
  3. Validation         -- voting on proposed memories
  4. Organizations      -- create orgs, add members
  5. Departments        -- sub-teams within orgs
  6. Domains            -- knowledge domains with access control
  7. Access Control     -- RBAC grants and requests
  8. Federation         -- cross-org knowledge sharing
  9. Agent Profile      -- PoE reputation and weight
  10. Embeddings        -- generate vectors via the (S)AGE network

Usage:
    python examples/complete_walkthrough.py

Set SAGE_URL to override the default endpoint (http://localhost:8080).
"""

import os
import sys

from sage_sdk import AgentIdentity, SageClient
from sage_sdk.exceptions import SageAPIError, SageAuthError


def main() -> None:
    base_url = os.environ.get("SAGE_URL", "http://localhost:8080")

    # ================================================================
    # 1. AGENT IDENTITY
    # ================================================================
    # Every agent needs an Ed25519 keypair. The public key IS your
    # agent ID. The private key signs every request you make.
    #
    # This is NOT a wallet or cryptocurrency key. It's just how (S)AGE
    # knows which agent is making a request, and prevents tampering.

    print("=" * 60)
    print("1. AGENT IDENTITY")
    print("=" * 60)

    # Generate a fresh identity (random keypair)
    alice = AgentIdentity.generate()
    print(f"Alice's agent ID: {alice.agent_id[:32]}...")

    # Save it to a file (so you can reuse it across sessions)
    alice.to_file("/tmp/alice.key")
    print("Saved to /tmp/alice.key")

    # Load it back
    alice_reloaded = AgentIdentity.from_file("/tmp/alice.key")
    assert alice_reloaded.agent_id == alice.agent_id
    print("Loaded back -- same agent ID confirmed")

    # Create a second agent for multi-agent examples later
    bob = AgentIdentity.generate()
    print(f"Bob's agent ID:   {bob.agent_id[:32]}...")
    print()

    # ================================================================
    # 2. MEMORY OPERATIONS
    # ================================================================
    # Memories are the core data type in (S)AGE. An agent proposes a
    # memory, validators vote on it, and if it reaches quorum (2/3
    # weighted vote), it becomes "committed" -- permanent, replicated
    # consensus-validated knowledge.

    print("=" * 60)
    print("2. MEMORY OPERATIONS")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── propose: submit a new memory ──────────────────────────
        # This creates a transaction, signs it with your key, and
        # broadcasts it to the CometBFT network. The memory starts
        # with status "proposed" and waits for validator votes.
        #
        # memory_type can be: "fact", "observation", or "inference"
        # confidence is 0.0 to 1.0 (how sure the agent is)

        print("\n[propose] Submitting a memory...")
        result = client.propose(
            content="SQL injection via UNION SELECT requires matching "
                    "the column count of the original query.",
            memory_type="fact",        # factual knowledge
            domain_tag="security",     # knowledge domain
            confidence=0.95,           # agent's confidence (0-1)
        )
        memory_id = result.memory_id
        print(f"  memory_id: {memory_id}")
        print(f"  tx_hash:   {result.tx_hash}")
        print(f"  status:    {result.status}")  # "proposed"

        # ── get_memory: retrieve a specific memory ────────────────
        # Fetches the full memory object including content, metadata,
        # confidence score, and current status.

        print("\n[get_memory] Retrieving the memory...")
        memory = client.get_memory(memory_id)
        print(f"  content:    {memory.content[:60]}...")
        print(f"  type:       {memory.memory_type.value}")
        print(f"  domain:     {memory.domain_tag}")
        print(f"  confidence: {memory.confidence_score}")
        print(f"  status:     {memory.status.value}")

        # ── query: semantic similarity search ─────────────────────
        # Converts your query to a 768-dim embedding (via Ollama)
        # and finds the most similar memories using pgvector HNSW.
        #
        # IMPORTANT: Use status_filter="committed" if you only want
        # consensus-validated knowledge. Default returns everything
        # including unvalidated proposals.

        print("\n[query] Searching for similar memories...")
        results = client.query(
            query_text="How do SQL injection attacks work?",
            domain="security",
            top_k=5,
            status_filter="committed",  # only consensus-validated
        )
        print(f"  Found {len(results)} committed memories")
        for r in results:
            print(f"    [{r.confidence_score:.2f}] {r.content[:60]}...")

        # ── corroborate: strengthen a memory ──────────────────────
        # When an agent agrees with a memory and has supporting
        # evidence, it can corroborate it. This increases the
        # memory's confidence score via the corroboration factor
        # in the decay formula.

        print("\n[corroborate] Adding supporting evidence...")
        client.corroborate(
            memory_id=memory_id,
            evidence="Confirmed via OWASP Testing Guide v4.2, section 4.8.5.1",
        )
        print("  Corroboration submitted")

        # ── challenge (dispute): question a memory ────────────────
        # If an agent believes a memory is incorrect or outdated,
        # it can challenge it. Challenged memories may eventually
        # be deprecated based on evidence.

        print("\n[challenge] Disputing the memory...")
        client.dispute(
            memory_id=memory_id,
            reason="Needs clarification: UNION injection also requires "
                   "compatible column types, not just count.",
        )
        print("  Challenge submitted")

    print()

    # ================================================================
    # 3. VALIDATION (VOTING)
    # ================================================================
    # Validators vote on proposed memories. When a memory gets >= 2/3
    # weighted vote, it becomes "committed". Vote weight comes from
    # the Proof of Experience (PoE) formula -- agents earn weight
    # through accurate contributions, domain expertise, and activity.

    print("=" * 60)
    print("3. VALIDATION (VOTING)")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=bob) as client:

        # First, Bob submits a memory that Alice will vote on
        result = client.propose(
            content="Cross-site scripting (XSS) can be mitigated with "
                    "Content-Security-Policy headers.",
            memory_type="fact",
            domain_tag="security",
            confidence=0.90,
        )
        bob_memory_id = result.memory_id
        print(f"\n  Bob submitted memory: {bob_memory_id[:16]}...")

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── vote: cast a validation vote ──────────────────────────
        # decision can be: "accept", "reject", or "abstain"
        # Validators should only vote on domains they have expertise in.

        print("\n[vote] Alice votes to accept Bob's memory...")
        client.vote(
            memory_id=bob_memory_id,
            decision="accept",
        )
        print("  Vote cast: accept")

        # ── get pending: list memories awaiting votes ─────────────
        # Validators can check what needs voting on.
        # (This is typically used by automated validator agents.)

    print()

    # ================================================================
    # 4. ORGANIZATIONS
    # ================================================================
    # Organizations are on-chain entities that group agents together.
    # The creating agent becomes the admin. Org state is replicated
    # across all 4 validator nodes via BFT consensus.

    print("=" * 60)
    print("4. ORGANIZATIONS")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── register_org: create a new organization ───────────────
        print("\n[register_org] Creating organization...")
        org = client.register_org(
            name="Acme Security",
            description="Security research and operations",
        )
        org_id = org["org_id"]
        print(f"  org_id:  {org_id}")
        print(f"  tx_hash: {org['tx_hash']}")

        # ── get_org: retrieve organization info ───────────────────
        print("\n[get_org] Retrieving organization...")
        org_info = client.get_org(org_id)
        print(f"  name: {org_info.get('name', 'N/A')}")

        # ── add_org_member: add an agent to the organization ──────
        # clearance levels: 0 (none), 1 (read), 2 (read+write),
        #                   3 (read+write+validate), 4 (admin)
        print("\n[add_org_member] Adding Bob to the organization...")
        client.add_org_member(
            org_id=org_id,
            agent_id=bob.agent_id,
            clearance=2,          # read + write
            role="researcher",    # human-readable role label
        )
        print(f"  Added Bob with clearance=2, role=researcher")

        # ── list_org_members: list all members ────────────────────
        print("\n[list_org_members] Listing members...")
        members = client.list_org_members(org_id)
        print(f"  Total members: {len(members)}")
        for m in members:
            print(f"    {m['agent_id'][:16]}... clearance={m['clearance']} role={m['role']}")

        # ── set_org_clearance: change a member's clearance ────────
        print("\n[set_org_clearance] Promoting Bob to clearance 3...")
        client.set_org_clearance(
            org_id=org_id,
            agent_id=bob.agent_id,
            clearance=3,  # now can validate
        )
        print("  Bob's clearance updated to 3")

        # ── remove_org_member: remove a member ────────────────────
        # (We won't actually remove Bob -- just showing the API)
        # client.remove_org_member(org_id=org_id, agent_id=bob.agent_id)

    print()

    # ================================================================
    # 5. DEPARTMENTS
    # ================================================================
    # Departments are sub-teams within an organization. They support
    # nested hierarchies (parent_dept) and independent clearance levels.

    print("=" * 60)
    print("5. DEPARTMENTS")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── register_dept: create a department ────────────────────
        print("\n[register_dept] Creating departments...")
        offensive = client.register_dept(
            org_id=org_id,
            name="Red Team",
            description="Offensive security operations",
        )
        offensive_id = offensive["dept_id"]
        print(f"  Red Team dept_id: {offensive_id}")

        defensive = client.register_dept(
            org_id=org_id,
            name="Blue Team",
            description="Defensive security operations",
        )
        defensive_id = defensive["dept_id"]
        print(f"  Blue Team dept_id: {defensive_id}")

        # Nested department (sub-team)
        malware = client.register_dept(
            org_id=org_id,
            name="Malware Analysis",
            description="Reverse engineering and malware research",
            parent_dept=defensive_id,  # nested under Blue Team
        )
        malware_id = malware["dept_id"]
        print(f"  Malware Analysis dept_id: {malware_id} (parent: Blue Team)")

        # ── add_dept_member: assign agents to departments ─────────
        print("\n[add_dept_member] Assigning agents...")
        client.add_dept_member(
            org_id=org_id,
            dept_id=offensive_id,
            agent_id=alice.agent_id,
            clearance=4,
            role="lead",
        )
        client.add_dept_member(
            org_id=org_id,
            dept_id=defensive_id,
            agent_id=bob.agent_id,
            clearance=3,
            role="analyst",
        )
        print("  Alice -> Red Team (lead, clearance=4)")
        print("  Bob   -> Blue Team (analyst, clearance=3)")

        # ── get_dept: retrieve department info ────────────────────
        print("\n[get_dept] Retrieving department...")
        dept_info = client.get_dept(org_id, offensive_id)
        print(f"  name: {dept_info.get('dept_name', 'N/A')}")

        # ── list_depts: list all departments in an org ────────────
        print("\n[list_depts] Listing all departments...")
        depts = client.list_depts(org_id)
        print(f"  Total departments: {len(depts)}")
        for d in depts:
            parent = d.get("parent_dept") or "(root)"
            print(f"    {d.get('dept_name', d['dept_id'])} -- parent: {parent}")

        # ── list_dept_members: list members of a department ───────
        print("\n[list_dept_members] Blue Team members...")
        dept_members = client.list_dept_members(org_id, defensive_id)
        for m in dept_members:
            print(f"    {m['agent_id'][:16]}... role={m['role']} clearance={m['clearance']}")

    print()

    # ================================================================
    # 6. KNOWLEDGE DOMAINS
    # ================================================================
    # Domains categorize knowledge and control who can access it.
    # The agent who registers a domain owns it and can grant access.

    print("=" * 60)
    print("6. KNOWLEDGE DOMAINS")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── register_domain: create a knowledge domain ────────────
        print("\n[register_domain] Registering domains...")
        client.register_domain(
            name="exploit_research",
            description="Zero-day exploits and attack techniques",
        )
        print("  Registered: exploit_research")

        # Hierarchical domain (child of exploit_research)
        client.register_domain(
            name="exploit_research.web",
            description="Web application exploits",
            parent="exploit_research",
        )
        print("  Registered: exploit_research.web (parent: exploit_research)")

        # ── get_domain: retrieve domain info ──────────────────────
        print("\n[get_domain] Retrieving domain...")
        domain = client.get_domain("exploit_research")
        print(f"  name:        {domain.get('domain_name', 'N/A')}")
        print(f"  owner:       {domain.get('owner_agent_id', 'N/A')[:16]}...")
        print(f"  description: {domain.get('description', 'N/A')}")

    print()

    # ================================================================
    # 7. ACCESS CONTROL (RBAC)
    # ================================================================
    # Access grants control which agents can read/write to which
    # domains. This is all on-chain -- grants, revocations, and the
    # full audit trail are replicated across the BFT network.
    #
    # Access levels:
    #   1 = read (query memories in this domain)
    #   2 = read + write (propose memories to this domain)
    #   3 = read + write + validate (vote on memories)
    #   4 = admin (grant/revoke access to others)

    print("=" * 60)
    print("7. ACCESS CONTROL (RBAC)")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── grant_access: give an agent access to a domain ────────
        print("\n[grant_access] Granting Bob access...")
        client.grant_access(
            grantee_id=bob.agent_id,
            domain="exploit_research",
            level=2,  # read + write
        )
        print("  Bob -> exploit_research (level=2, read+write)")

        # Grant with expiration
        import time
        expires = int(time.time()) + 86400  # 24 hours from now
        client.grant_access(
            grantee_id=bob.agent_id,
            domain="exploit_research.web",
            level=1,       # read only
            expires_at=expires,
        )
        print(f"  Bob -> exploit_research.web (level=1, expires in 24h)")

        # ── list_grants: see what access an agent has ─────────────
        print("\n[list_grants] Bob's access grants...")
        grants = client.list_grants(bob.agent_id)
        print(f"  Total grants: {len(grants)}")
        for g in grants:
            level = g.get("access_level", g.get("level", "?"))
            print(f"    domain={g['domain']} level={level}")

    # Bob can also request access (instead of admin granting directly)
    with SageClient(base_url=base_url, identity=bob) as client:

        # ── request_access: ask for access to a domain ────────────
        print("\n[request_access] Bob requests access to a new domain...")
        req = client.request_access(
            domain="exploit_research",
            justification="Need write access for vulnerability reports",
            level=3,
        )
        print(f"  request_id: {req.get('request_id', 'N/A')}")
        print(f"  status: {req.get('status', 'N/A')}")

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── revoke_access: remove an agent's access ───────────────
        # (Showing the API -- we won't actually revoke Bob here)
        # client.revoke_access(
        #     grantee_id=bob.agent_id,
        #     domain="exploit_research.web",
        #     reason="Access period ended",
        # )
        pass

    print()

    # ================================================================
    # 8. FEDERATION
    # ================================================================
    # Federation allows two separate organizations to share knowledge
    # domains. The proposing org specifies which domains to share and
    # the maximum clearance level. The target org must approve.
    #
    # This models real-world agreements like:
    #   - Two CERTs sharing threat intelligence
    #   - Research labs collaborating across institutions
    #   - National AI infrastructure sharing validated knowledge

    print("=" * 60)
    print("8. FEDERATION")
    print("=" * 60)

    # Create a second organization
    charlie = AgentIdentity.generate()
    with SageClient(base_url=base_url, identity=charlie) as client:
        partner_org = client.register_org(
            name="Partner Labs",
            description="External research partner",
        )
        partner_org_id = partner_org["org_id"]
        print(f"\n  Created partner org: {partner_org_id[:16]}...")

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── propose_federation: propose a sharing agreement ───────
        print("\n[propose_federation] Proposing federation...")
        fed = client.propose_federation(
            target_org_id=partner_org_id,
            allowed_domains=["exploit_research"],
            max_clearance=2,        # max level for federated access
            requires_approval=True, # target org must approve
        )
        fed_id = fed["federation_id"]
        print(f"  federation_id: {fed_id}")
        print(f"  status: {fed['status']}")  # "proposed"

    with SageClient(base_url=base_url, identity=charlie) as client:

        # ── approve_federation: target org accepts ────────────────
        print("\n[approve_federation] Partner approves...")
        approval = client.approve_federation(fed_id)
        print(f"  status: {approval['status']}")  # "active"

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── get_federation: check federation details ──────────────
        print("\n[get_federation] Federation details...")
        fed_info = client.get_federation(fed_id)
        print(f"  status: {fed_info['status']}")
        print(f"  domains: {fed_info.get('allowed_domains', [])}")
        print(f"  max_clearance: {fed_info.get('max_clearance', 'N/A')}")

        # ── list_federations: list all active federations ─────────
        print("\n[list_federations] Active federations...")
        active = client.list_federations(org_id)
        print(f"  Count: {len(active)}")

        # ── revoke_federation: end the agreement ──────────────────
        # (Showing the API -- we won't actually revoke here)
        # client.revoke_federation(fed_id, reason="Agreement expired")

    print()

    # ================================================================
    # 9. AGENT PROFILE (Proof of Experience)
    # ================================================================
    # Every agent has a PoE (Proof of Experience) weight that
    # determines their influence in consensus. Weight is calculated:
    #
    #   W = exp(0.4*ln(A) + 0.3*ln(D) + 0.15*ln(T) + 0.15*ln(S))
    #
    # Where:
    #   A = accuracy (EWMA of correct votes)
    #   D = domain expertise (cosine similarity of vote domains)
    #   T = recency (exponential decay, lambda=0.01)
    #   S = corroboration score (how often others agree)
    #
    # You earn weight by making accurate contributions, voting
    # correctly, and being active. Weight is recalculated every
    # epoch (100 blocks, ~5 min).

    print("=" * 60)
    print("9. AGENT PROFILE (Proof of Experience)")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── get_profile: check your reputation ────────────────────
        print("\n[get_profile] Alice's agent profile...")
        profile = client.get_profile()
        print(f"  agent_id: {profile.agent_id[:16]}...")
        print(f"  weight:   {profile.weight}")
        print(f"  votes:    {profile.vote_count}")

    print()

    # ================================================================
    # 10. EMBEDDINGS
    # ================================================================
    # (S)AGE generates 768-dimensional embeddings locally using Ollama
    # (nomic-embed-text model). You can use the (S)AGE network as your
    # embedding service -- no external API calls, fully sovereign.

    print("=" * 60)
    print("10. EMBEDDINGS")
    print("=" * 60)

    with SageClient(base_url=base_url, identity=alice) as client:

        # ── embed: generate an embedding vector ───────────────────
        print("\n[embed] Generating embedding...")
        embedding = client.embed("SQL injection attack techniques")
        print(f"  Dimensions: {len(embedding)}")
        print(f"  First 5 values: {embedding[:5]}")

    print()
    print("=" * 60)
    print("WALKTHROUGH COMPLETE")
    print("=" * 60)
    print()
    print("You've seen every (S)AGE SDK operation:")
    print("  - Agent identity (Ed25519 keypairs)")
    print("  - Memory lifecycle (propose, query, corroborate, challenge)")
    print("  - Validation (voting on proposals)")
    print("  - Organizations (create, add members, set clearance)")
    print("  - Departments (sub-teams, nested hierarchy)")
    print("  - Domains (knowledge categories)")
    print("  - Access control (grant, revoke, request)")
    print("  - Federation (cross-org knowledge sharing)")
    print("  - Agent profile (PoE reputation)")
    print("  - Embeddings (local vector generation)")
    print()
    print("All state is on-chain, replicated across 4 BFT validator")
    print("nodes, and cryptographically signed. No tokens, no gas,")
    print("no cryptocurrency. Just sovereign AI memory infrastructure.")


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
