# SAGE Roadmap

**Status (2026-07):** **v11.7.7 is the current release.** This document records the v11.7 slate as shipped and looks forward to v11.8, v11.9, and the v12 completion milestone. Everything past v11.7 is planned, not promised, and carries no date.

**Hard constraint driving the whole plan:** no chain reset, no operator-typed commands. Existing chains must upgrade in place across all future releases.

---

## v11 - shipped (the sovereign-UX + federation release)

v11 is the "zero-terminal, sovereign" release. It takes SAGE from "works if you know the CLI" to "a person clicking buttons can stand up a private, semantic, federated memory node." What landed:

### Onboarding and setup

- **First-run onboarding wizard.** Fresh nodes get a three-step welcome (orientation, semantic memory, connect an AI tool). Closing it marks onboarding done; it is re-runnable any time from **Settings > Maintenance > Run setup**.
- **Guided semantic-memory setup.** One flow turns on the bundled embedder (Ollama + `nomic-embed-text`): detect Ollama, pull the model, re-embed existing memories as a durable background job with a progress banner that survives reloads, then switch recall over. Includes recovery-key backup and honest handling of undecryptable memories (surfaced, not silently dropped).
- **One-click managed reranker.** After a single consent click, SAGE downloads a pinned, sha256-checksum-verified llama.cpp engine build and the `bge-reranker-v2-m3` cross-encoder model, then runs and manages the sidecar process itself (loopback only, nothing leaves the machine). Recall results-per-query (k) is tunable 3-20. Bring-your-own TEI-compatible servers are still supported.
- **Connect-an-AI-tool flows.** A single dashboard flow branches three ways: same-machine one-click config writing (ChatGPT desktop Codex mode, Claude Code, Codex CLI, Cursor, Windsurf, Claude Desktop), ChatGPT Work through an OpenAI plugin + Secure MCP Tunnel, remote MCP over LAN/VPN or an operator-managed HTTPS endpoint, and LAN node-join (another computer becomes a peer node sharing this node's memory).

### Federation

- **Whole-SAGE-to-whole-SAGE join ceremony.** Guided guest and host wizards, offline-bundled, and human-verified. Scope grants (allowed domains + a 0-4 clearance ceiling) are enforced when serving recall, recorded as an on-chain treaty, and revocable. v11.6 adds first-class internet/NAT traversal and authenticated post-pair route exchange so a LAN relationship can roam.
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

## v11.5 - shipped (the hardening + consensus release)

v11.5 is the anti-DoS and memory-integrity release. Two workstreams landed: pipe hardening that puts real bounds on the agent-to-agent message tables, and the app-v17 consensus slate that gives deprecation teeth again with a quorum-scaled two-phase challenge, a first-class reinstate verb, disputed-but-recallable memories, and action-bound delegated agent proofs. The consensus slate ships **dormant** and activates only through the governed upgrade ladder (a 2/3 quorum vote with a 200-block floor), so existing chains replay byte-identically until operators vote it in. What landed:

### Pipe anti-DoS hardening

The agent-to-agent pipe tables now carry anti-DoS guards on every write path (REST, MCP over REST, and the dashboard operator send that hits the store directly). **Size caps** at the store chokepoint: 256 KiB payload/result, 8 KiB intent, with matching fast-fail **413** checks in the handlers (`MaxPipeContentBytes`/`MaxPipeIntentBytes`, `internal/store/store.go:513-515`). **Quotas**: 256 open pipes per verified agent identity, 10000 node-wide, an index-backed COUNT before insert, rejected as **429 with `Retry-After`** (mirrors the mempool-full recipe) and keyed on the Ed25519-verified `from_agent`, not the spoofable rate-limit header (`MaxOpenPipesPerAgent`/`MaxOpenPipesGlobal`, `store.go:518-521`). **Retention backstop**: pending or claimed rows older than 48h are force-expired regardless of stamped TTL, wired into the existing 5-minute sweep plus a boot one-shot; terminal rows still purge 24h after creation, and the dashboard TTL input is clamped to 24h.

### Reinstate verb + quorum-scaled deprecation

Bring back a first-class deprecation verb with teeth: deprecation gated by consensus, with the required **quorum scaled to network size** so a small-LAN node and a large federation apply proportionate bars instead of one hardcoded threshold. Complements the v11 change that made deprecation audit-only.

**Shipped in v11.5.0 as the app-v17 slate (dormant) - this item is now complete.** At challenge execution the handler counts the distinct modify-verb holders on the memory's domain (owner + ancestor owners + unexpired level-3 grantees) from committed state. A count of one or fewer keeps the byte-identical legacy one-strike deprecate, so personal nodes see zero change. A count of two or more parks the memory as `challenged` with an AppHash-folded challenge record; a second, distinct holder confirms to deprecate, and the original challenger cannot self-confirm. The new **`TxTypeMemoryReinstate`** takes a challenged memory back to `committed`, restoring the original content hash captured in the challenge record; current modify holders may use it and the original challenger can always withdraw, even after grant expiry/revocation. REST, MCP (`sage_reinstate`), Chrome, and sync/async Python SDK surfaces submit the transaction. Off-chain, challenged memories stay recallable on SQLite and Postgres, marked `disputed`, with a query-time confidence haircut. The whole slate is gated behind the app-v17 fork and ships dormant, activating only via the governed upgrade vote.

The release audit also closed the delegated-signing gap in the original candidate: an agent proof authenticated a key but was not cryptographically tied to the transaction payload a validator executed. Post-app-v17 delegated REST transactions now append the exact canonical signed request without changing any legacy encoding; consensus verifies its hash/signature, rebuilds the authorized payload for every REST transaction type that uses agent identity, checks freshness against committed block time, and atomically consumes a short-lived proof marker in AppHash state. Same-key node-originated transactions remain bound by the outer signature and monotonic nonce.

### Shared-domain replication

Federation started as read-only recall exchange: borrowed answers are shown in the moment, tagged with their source, and never written to your chain. Opt-in **domain sync** lets a shared domain be *replicated* to a peer rather than only queried, built from a **durable outbox** (writes to a shared domain are queued locally and delivered reliably across restarts and network gaps) plus an **anti-entropy digest** (periodic reconciliation so a peer that was offline catches up on what it missed) and a commit-tail watcher. Bounded by the same scope grants as recall exchange; no silent widening of what crosses the link.

**Shipped as a preview in v11.4.5 - this item is now delivered.** The durable outbox, anti-entropy digest, and commit-tail watcher landed together in the v11.4.5 preview.

### RBAC clarity + cross-scope memory transfer

Make the access model legible (who can read, write, and modify what, and why) and add a governed way to **transfer memories across scopes** (hand a memory from one agent, org, or domain to another) without losing attribution or bypassing clearance.

**Shipped across v11.3.0 and v11.4.0 - this item is now complete.** The CEREBRUM per-agent Domain Access matrix issues real on-chain access grants and revokes on Save (previously it saved a cosmetic blob the consensus checks never read), so what the matrix shows is what the chain enforces (v11.3.0). Cross-scope transfer shipped as governed domain-level reassignment: ownership of a domain moves to another agent through a governance-gated flow, from the Agents page or directly from a search selection (v11.4.0). The transfer moves ownership and read/write access - never authorship, which stays immutably attributed to whoever wrote each memory. Deliberate design choice: transfer operates on domains (tags), not individual memories, composed entirely from existing on-chain transactions rather than new consensus machinery.

---

## v11.6 - shipped (internet federation + controlled sync)

### libp2p NAT traversal + author-operated connectivity service

SAGE now uses **libp2p-based NAT traversal** with an **author-operated connectivity relay**, so two sovereign nodes behind home routers can reach each other without port-forwarding. The service brokers encrypted connectivity only; it never sees or stores memory. Operators may supply their own relay routes.

### Domain-scoped memory sync foundation

An operator can opt specific domains into synchronization across an established federation peer, on LAN or over the internet. A node might sync `eurorack` or `dmt-laser-experiments` while its `personal`, `family`, and every other unselected domain remain local and are never transmitted.

v11.6 productizes the v11.5 preview engine (durable outbox, authenticated push, anti-entropy backfill, and reconnect catch-up) instead of creating a second replication path. The choice appears after the two-node signing ceremony and is controlled by the host, **off by default**. Its concrete domain set is the bidirectional synchronization allowlist. Durable versioned policy propagation precedes data; tags replicate; crash-safe provenance prevents re-forwarding; P2P and domain-isolation tests cover the release path. Federation connectivity and memory synchronization remain separate choices.

v11.6 does **not** label a two-node synchronized pair Byzantine fault tolerant. If one side is unavailable, writes may be queued or remain local according to an explicit degraded-mode policy; the release must not imply that one surviving member constitutes quorum.

---

## v11.7 - shipped (admin override + connection & lifecycle hardening)

### CEREBRUM administrator access override

Make the personal-node authority model match the product: the genesis admin can explicitly give a locally installed agent read or read+write access to a domain even when another agent is recorded as its owner. CEREBRUM shows the original owner and target before confirmation, binds that owner into the consensus transaction, records the override as an ordinary on-chain grant/revoke, and leaves ownership and immutable memory authorship unchanged. Federated/remote targets remain consent-gated. Consensus support is isolated behind the dormant app-v18 activation boundary so existing chains replay byte-identically.

### ChatGPT desktop, Work, and Codex connection refresh

Follow OpenAI's current product surfaces instead of treating every ChatGPT connection as the same runtime. The new ChatGPT desktop app combines Chat, ChatGPT Work, and Codex. Codex mode shares the user-level `~/.codex/config.toml` MCP configuration with Codex CLI and the IDE extension, so CEREBRUM provides a one-click app-wide local connection for that mode. ChatGPT Work on the web or in the desktop app uses the hosted plugin + Secure MCP Tunnel path because ChatGPT cannot invoke a local stdio MCP server directly. Regular Chat remains supported and starts with the **Quick chat** button. SAGE uses OpenAI's name **Work** rather than the unrelated **Cowork** label.

### Coordinated restart, update, and MCP hardening

One instance per node (instance lock with owned pidfile), coordinated restart that drains MCP sessions and dashboard event streams before exec, checksum-verified updates with automatic rollback and proof-of-boot confirmation, and a hardened HTTP MCP transport: operator-only bearer principal resolution, a bounded nonce replay cache, an exact-match origin allowlist, and per-route write deadlines. Fixes the v11.6.1 reports of intermittent lost MCP connections and cannot-save-to-domain errors (boot-time key cache, transport errors mislabeled as permission denials, keep-alive reuse race).

---

## v11.8 - planned (the collaboration release)

Forward-looking. Nothing here is committed to a date; treat it as speculative until it ships.

### Sharing & Sync control plane

Add a dedicated **Sharing & Sync** section, separate from Agents and identity management. It shows every synchronization group, member node, role and voting power where applicable, full-sync versus selective-sync status, shared domains, owner, backfill/catch-up position, health, and last successful synchronization.

A domain's original/current owner may propose adding or removing that domain, create a new shared domain, backfill earlier memories, change eligible nodes, or stop future synchronization; group-level RBAC and validator governance authorize changes that affect other members. Removing a domain or node has explicit, auditable semantics for retained historical copies, tombstones, future updates, rejoin, and local deletion - it never silently erases another member's data.

This release also adds multi-node group membership and explicit full-sync/selective-sync roles, but does not claim self-healing BFT until the v11.9 quorum gates below pass.

### Cross-network agent messaging (federated inbox)

Extend the agent-to-agent pipe (today a node-local inbox) across a federation link so an agent on one SAGE can hand work to an agent on a peer SAGE - a **federated inbox**. Rides on the v11.6 connectivity substrate and inherits the same scope grants that bound recall exchange; delivery stays off-consensus and message content is never written to either chain.

---

## v11.9 - planned

### Domain-scoped quorum + self-healing replication

Turn selected shared domains into hardened replicated canonical state across validator groups. Validators independently evaluate proposed memories, commit only quorum-approved results, and retain replicated full content plus all serving indexes. A surviving **greater-than-two-thirds voting-power quorum** continues accepting memories while a member is offline; authenticated replay or state sync catches it up when it returns.

The release gate includes offline-write/catch-up, snapshot/state-sync recovery, validator and membership reconfiguration, revocation, conflict/degraded-mode behavior, and chaos testing. Topologies remain honest: four equal-power validators tolerate one offline validator; three equal-power validators do not continue with only two online because CometBFT requires more than two-thirds voting power. The design must choose explicitly between domain-specific quorum shards and an extension of federation domain sync; it must never tunnel or replicate the whole current chain if that exposes unselected domains.

---

## v12 - product roadmap capstone

v12 is the planned completion milestone for the SAGE product roadmap: the fully integrated product rather than another backend-only release. It ships as a standalone desktop application with CEREBRUM embedded in the application window instead of opening the user's web browser. The app owns installation, node lifecycle, onboarding, permissions, updates, health/recovery, federation, and Sharing & Sync as one coherent native-feeling experience, while the SAGE daemon and authenticated local APIs remain cleanly separated underneath.

**Consumer usability is a release criterion, not polish.** A nontechnical person must be able to install SAGE, create or join a node, connect an AI tool, choose what is private or shared, recover from ordinary failures, and keep the app updated without opening a terminal or learning SAGE internals. Every choice uses plain language and safe defaults; destructive or privacy-affecting actions use consistent accessible SAGE dialogs; errors explain what happened, what remains safe, and the next recovery action. The v12 release gate includes clean-machine onboarding and recovery usability tests with people who have not used SAGE before.

The desktop-shell technology is deliberately not locked here. It must be chosen through a security, packaging, accessibility, performance, offline-operation, and cross-platform evaluation; “native-feeling” must not come at the cost of weakening the local trust boundary or bundling an unmaintainable browser runtime.

---

## Current Foundation

The v11 line carries the upgrade substrate, domain-reassign recovery, ancestor grants, PoE-weighted quorum, verdict-correctness scoring, corroboration, and domain-aware validator weighting. Those are baseline capabilities now, not future roadmap work. Release-by-release history lives in the README changelog.
