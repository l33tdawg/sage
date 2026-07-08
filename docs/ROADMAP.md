# SAGE Roadmap

**Status (2026-07):** **v11.4.0 is the current release.** This document looks forward from v11 to the v11.5 slate. Everything past v11 is planned, not promised, and carries no date.

**Hard constraint driving the whole plan:** no chain reset, no operator-typed commands. Existing chains must upgrade in place across all future releases.

---

## v11 — shipped (the sovereign-UX + federation release)

v11 is the "zero-terminal, sovereign" release. It takes SAGE from "works if you know the CLI" to "a person clicking buttons can stand up a private, semantic, federated memory node." What landed:

### Onboarding and setup

- **First-run onboarding wizard.** Fresh nodes get a three-step welcome (orientation, semantic memory, connect an AI tool). Closing it marks onboarding done; it is re-runnable any time from **Settings > Maintenance > Run setup**.
- **Guided semantic-memory setup.** One flow turns on the bundled embedder (Ollama + `nomic-embed-text`): detect Ollama, pull the model, re-embed existing memories as a durable background job with a progress banner that survives reloads, then switch recall over. Includes recovery-key backup and honest handling of undecryptable memories (surfaced, not silently dropped).
- **One-click managed reranker.** After a single consent click, SAGE downloads a pinned, sha256-checksum-verified llama.cpp engine build and the `bge-reranker-v2-m3` cross-encoder model, then runs and manages the sidecar process itself (loopback only, nothing leaves the machine). Recall results-per-query (k) is tunable 3-20. Bring-your-own TEI-compatible servers are still supported.
- **Connect-an-AI-tool flows.** A single dashboard flow branches three ways: same-machine one-click config writing (Claude Code, Codex, Cursor, Windsurf, Claude Desktop), remote MCP over the operator's own cloudflared tunnel (local-first, no shared cloud), and LAN node-join (another computer becomes a peer node sharing this node's memory).

### Federation

- **Whole-SAGE-to-whole-SAGE join ceremony.** Guided guest and host wizards, offline-bundled, and LAN-first in v11. Trust is anchored by a human scan-and-compare; the six-digit codes are TOTP (RFC-6238) proving co-possession under a 2-of-2 consent handshake. Scope grants (allowed domains + a 0-4 clearance ceiling) are enforced when serving recall, recorded as an on-chain treaty, and revocable (revoke erases no memories). v11 assumes same-LAN or operator-provided reachability; first-class internet/NAT traversal is planned for v11.5.
- **Off-consensus transport.** mTLS federation listener, read-only recall query proxy (foreign results are merge-in-response only, never persisted to your chain), and receipt exchange.
- **Consensus-layer federation primitives.** On-chain `cross_fed` exchange terms (Mode-1, tx 33/34) and the co-commit primitive (tx 31/32) landed at the app layer.

### Consensus and memory integrity

- **app-v15 verb-ladder.** Closed the ungated-deprecate hole (deprecation is now audit-only / consensus-gated) and added a grantable level-3 (modify).
- **Globally-unique `chain_id`** minted at genesis.
- **Orphaned-memory recovery** (old-key re-key) and `embedding_provider` stamped at insert so new memories no longer pose as unembedded.

### CEREBRUM (the dashboard)

- **MRI 3D brain is the CEREBRUM view** (three.js + 3d-force-graph bundled locally, so it renders fully offline).
- **Click-a-memory "train of thought"** board (Do's / Don'ts / Observations / Notes), computed from lineage, tags, content overlap, and same-lobe signals; hop card to card to walk the connectome.
- **Reading panel** collapses to the domain lobes by default (newest 30, most-recently-active first) with an expandable "how to read".
- **Live task board** with agent-vs-human authorship and atomic claim/ownership; the agent message bus merged in as a Messages tab.
- **Real search** (FTS with keyword fallback), bulk curation, status and tag filters, and corroboration counts on list + detail.
- **Settings reorganized** into focused tabs (Overview, Connection, Recall, Security, Maintenance, Updates), with in-place updates and node restart.

---

## v11.5 — planned

Forward-looking. Nothing here is committed to a date; these are the problems v11.5 is scoped to solve, in rough priority order. Treat every item as speculative until it ships.

### Shared-domain replication

Today federation is read-only recall exchange: borrowed answers are shown in the moment, tagged with their source, and never written to your chain. v11.5 adds opt-in **domain sync** so a shared domain can be *replicated* to a peer rather than only queried. The design centers on a **durable outbox** (writes to a shared domain are queued locally and delivered reliably across restarts and network gaps) plus **anti-entropy backfill** (periodic reconciliation so a peer that was offline catches up on what it missed). Bounded by the same scope grants as recall exchange; no silent widening of what crosses the link.

### Reinstate verb + quorum-scaled deprecation

Bring back a first-class deprecation verb with teeth: deprecation gated by consensus, with the required **quorum scaled to network size** so a small-LAN node and a large federation apply proportionate bars instead of one hardcoded threshold. Complements the v11 change that made deprecation audit-only.

### RBAC clarity + cross-scope memory transfer

Make the access model legible (who can read, write, and modify what, and why) and add a governed way to **transfer memories across scopes** (hand a memory from one agent, org, or domain to another) without losing attribution or bypassing clearance.

**Shipped across v11.3.0 and v11.4.0 - this item is now complete.** The CEREBRUM per-agent Domain Access matrix issues real on-chain access grants and revokes on Save (previously it saved a cosmetic blob the consensus checks never read), so what the matrix shows is what the chain enforces (v11.3.0). Cross-scope transfer shipped as governed domain-level reassignment: ownership of a domain moves to another agent through a governance-gated flow, from the Agents page or directly from a search selection (v11.4.0). The transfer moves ownership and read/write access - never authorship, which stays immutably attributed to whoever wrote each memory. Deliberate design choice: transfer operates on domains (tags), not individual memories, composed entirely from existing on-chain transactions rather than new consensus machinery.

### libp2p NAT traversal + author-operated connectivity service

Replace the current same-LAN / bring-your-own-tunnel reachability story with **libp2p-based NAT traversal**, backed by an **author-operated connectivity service** (a relay / rendezvous the project runs) so two sovereign nodes behind home routers can find and reach each other without port-forwarding or a third-party cloud. Sovereignty is preserved: the service brokers connectivity only, it never sees or stores memory.

---

## Current Foundation

The v11 line carries the upgrade substrate, domain-reassign recovery, ancestor grants, PoE-weighted quorum, verdict-correctness scoring, corroboration, and domain-aware validator weighting. Those are baseline capabilities now, not future roadmap work. Release-by-release history lives in the README changelog.
