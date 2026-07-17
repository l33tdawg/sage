# SAGE Roadmap

**Status (2026-07):** **v11.9.2 is the current release.** The exact-source cold state-sync proof passed on the v11.9 release source, and the complete CI/security/fault matrix remains a mandatory publication invariant. This document keeps v11.8's deferred federated inbox distinct from delivered synchronization groups and looks forward to the v11.10–v11.13 productization bridge and v12. Nothing after v11.9 is promised or dated.

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

- **Whole-SAGE-to-whole-SAGE join ceremony.** Guided guest and host wizards, offline-bundled, and human-verified. JOIN establishes exact chain/operator/CA/epoch trust and is revocable; in v11.9 it grants zero domains by itself. Each node separately manages a mutable Read/Copy snapshot over existing domains without reconnecting. v11.6 added first-class internet/NAT traversal and authenticated post-pair route exchange so a LAN relationship can roam.
- **Off-consensus transport.** The pinned mTLS federation listener serves live Read results as merge-in-response-only data. Copy requires both the source's current offer and the receiver's independent subscription before a locally governed copy is retained. Cross-host Write remains reserved and fails closed with `501` in v11.9 until one trusted connection can be bound to one consensus-authorized submission.
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

## v11.8 - shipped (the collaboration release)

The synchronization-group control plane shipped in v11.8.2 and was maintained through v11.8.5. It remains an off-consensus, independent-chain sharing layer; it is not the same-chain Byzantine quorum introduced by v11.9.

### Sharing & Sync control plane

A dedicated **Sharing & Sync** section, separate from Agents and identity management, exposes synchronization groups, member nodes, roles, selective-sync state, shared domains, ownership, backfill/catch-up position, health, and recent synchronization.

Signed roster and per-domain journals govern membership, selective synchronization, backfill, controller rotation, removal, and rejoin. Retained historical copies are never silently erased from another independent chain.

This shipped multi-node sharing and full/selective synchronization without claiming self-healing BFT. That distinction remains permanent: v11.9's BFT scope is a separate same-chain model rather than an upgrade of the v11.8 journal into cross-chain consensus.

### Deferred from v11.8: cross-network agent messaging (federated inbox)

The agent-to-agent pipe remains a node-local inbox. Extending it across a federation link so an agent on one SAGE can hand work to an agent on a peer SAGE remains future work. The intended **federated inbox** would ride the v11.6 connectivity substrate, inherit recall-exchange scope grants, remain off-consensus, and keep message content off both chains; v11.8 did not ship it.

---

## v11.9 - shipped

### Colleague-style independent-chain federation

Federation now separates **trust** from **sharing**. A successful JOIN freezes the exact remote chain, operator, CA pin, and policy epoch but installs a present empty policy, so nothing is implicitly shared. Both operators independently choose from domains that already exist on their own node and can replace that complete snapshot at any time without re-pairing. Read permits live borrowed recall; Copy additionally needs the receiving node to opt in before it saves a locally governed copy. The versioned Write field and endpoint remain for compatibility but reject use until SAGE has a consensus capability bound to the active connection generation and exact submission.

Direct and group lanes fail closed on identity drift: the live operator, CA, agreement generation, and—where applicable—the exact active group roster/domain must all agree. Agreement set/narrowing, JOIN activation, and revocation share one mutation boundary across signed REST and dashboard paths, so a completed policy change cannot race an older broader response. The CEREBRUM Federation page exposes both directional snapshots, receiver Copy choices, dynamic existing-domain selection, and usable scrolling for large domain sets.

### Domain-scoped quorum + self-healing replication

Selected domains now have hardened replicated canonical state across validator groups. A v11.9 scope is contained inside one SAGE consensus chain: its on-chain roster names existing validators, its exact domain allowlist prevents scope bleed, and its canonical Badger content mirror lets recovering nodes rebuild serving indexes. Validators independently evaluate proposed memories and commit only quorum-approved results. A surviving **greater-than-two-thirds voting-power quorum** continues accepting memories while a member is offline; ordered committed-block replay catches it up when it returns. The separate network-safe ABCI state-sync path is implemented as an explicit authorized boot role with a deterministic latest-visible state stream, isolated verification, crash-safe whole-bundle activation, and seal-before-serving. The final exact-source integrated provider-to-receiver cold run passed on source identity `7080580b15e7e5158a04e8b294ab772e51f294633be2737f904276afec4c3458`. The v11.8 SQLite/cross-chain Synchronization Group remains an off-consensus sharing overlay; v11.9 does not relabel it as BFT. SAGE's existing full local rollback snapshot contains private node material and must never be reused as a network payload.

The release evidence covers offline-write/catch-up, state recovery, validator and membership reconfiguration, revocation, conflict/degraded behavior, and chaos. The real-Comet gate begins with three validators plus one live non-validator, drives the signed app-v20 ladder, then proves governed add, bounded power update, and removal at Comet's exact H+2 effective heights. Complete-pair restarts must preserve both Comet's set and the ABCI persisted roster; the removed gateway reports inactive, cannot cast a governance vote, and cannot reappear in later commits. Its post-change power layout leaves a connected greater-than-two-thirds side for the one-validator isolation and no quorum on either side of the later 2+2 split. State-sync activation is boot-only whole-application replacement with crash journaling, exact provider-equals-validator P2P authorization, H+2 snapshot eligibility, full pristine/disk gates, latched expiry, and final-store serving order. Providers require effective `retain_blocks=0`. While unsealed, `Query` and `CheckTx` fail fast; only consensus block calls may wait during the narrow PendingComet handoff. The scoped proof composes a race-enabled, dual-principal OS-process formation/revision oracle with the real Docker firewall P2P gate; the held subprocess is not called a TCP partition. The independent integrated wire gate proved reciprocal unauthorized-peer denial, two independent RPC origins, a receiver killed after durable completion but before runtime publication and restarted in ordinary mode, a second pristine receiver with `session < seal < REST`, provider restart, exact scoped projection, and block/AppHash convergence. Internet validators still require routable Comet TCP, port forwarding, or an operator VPN; federation is not a validator tunnel, and a future tunnel layer remains separate work. The network path must never reuse the private local rollback bundle or expose it to a federation peer.

---

## v11.10 – v11.13 - planned (the productization bridge to v12)

v11.9 is intended to complete the trust and replication **backend**. v12 is the **product**: a standalone native application in which every CEREBRUM function is mapped to a real app experience, with the web dashboard kept only as a still-supported fallback. The releases between them de-risk and stage that transition so v12 is an integration-and-polish capstone, not a from-scratch rewrite. Every step keeps the hard constraints — no chain reset, upgrade-in-place, the SAGE daemon and authenticated local APIs cleanly separated from any shell, and no weakening of the local trust boundary. Order and grouping are indicative; nothing here is dated.

### v11.10 - the native shell foundation

Choose the desktop-shell technology through the deliberate evaluation v12 requires, then ship the first additive, opt-in native application that embeds CEREBRUM in its own window. The browser CEREBRUM remains fully supported as the fallback. v11.10 is an architectural release, not a cosmetic wrapper, and must complete these deliverables:

- **Desktop-shell decision record.** Compare the credible macOS, Windows, and Linux options with a weighted scorecard covering threat surface and sandboxing, signed packaging/notarization, accessibility, performance and memory cost, offline operation, cross-platform maintenance, update behavior, and long-term ownership. Build small proof-of-concept spikes for the finalists, document the threat model, and record the chosen shell plus rejected alternatives before product code commits to it.
- **App–daemon trust and compatibility contract.** Specify authenticated bootstrap, credential/key storage, IPC versus loopback transport, origin/navigation restrictions, process and port ownership, startup readiness, crash detection and recovery, graceful drain/shutdown, version negotiation, and rollback-compatible independent updates. The shell receives no implicit validator/admin authority and cannot weaken the existing local API boundary.
- **Single-instance lifecycle and navigation ownership.** The native app owns its window, deep links, route restoration, external-link handoff, foreground/background behavior, and daemon supervision. Reopening SAGE focuses the existing native window; it does not depend on browser tab inspection or browser-specific automation. Browser CEREBRUM remains an explicit fallback, not an accidental second primary UI.
- **Measurable foundation gate.** Set budgets for cold/warm launch, idle CPU, memory, large-store interaction latency, and animation frame pacing on supported hardware. Use a monotonic animation clock and compositor-friendly transforms for continuous motion instead of coarse React timer rerenders; establish keyboard navigation, focus visibility, screen-reader naming, and reduced-motion architecture now even though v11.13 performs the full hardening pass. Automated packaging and smoke tests must cover clean install, daemon unavailable/recovery, app restart, deep-link routing, offline launch, and shell/daemon version skew on every supported OS.

No chain reset. The SAGE daemon and authenticated local APIs remain independently testable and operable underneath the shell.

### v11.11 - consumer onboarding and recovery

Make first run and recovery survivable by someone who has never used SAGE, natively and without a terminal. Guided create-or-join, connect-an-AI-tool, and choose-what-is-private-or-shared flows; recovery-key backup and restore; and honest, consistent dialogs for destructive or privacy-affecting actions that explain what happened, what remains safe, and the next step. Plain language and safe defaults throughout. The gate includes clean-machine onboarding and recovery usability tests with real nontechnical users — the same bar v12 sets, brought forward so it is proven early rather than at the end.

### v11.12 - native lifecycle: install, updates, health, permissions

Move the operational surface a desktop product needs out of terminal-and-dashboard-only paths and into the app: installation and OS-level permission prompts (login item, notifications, and any capability the product uses) with plain-language rationale; checksum-verified background updates with automatic rollback and proof-of-boot; continuous node-health monitoring with one-click guided recovery; and unobtrusive background/tray operation. Reuses the v11.7 coordinated-restart, verified-update, and instance-lock machinery rather than reinventing it.

### v11.13 - accessibility, performance, and offline hardening

Meet the accessibility bar v12 treats as a release criterion: full keyboard navigation, screen-reader labeling, sufficient contrast, and reduced-motion support across CEREBRUM and the native shell. Hold the v11.10 performance budgets for the embedded experience on large memory stores and the 3D connectome view. Verify fully offline operation end to end — the bundled embedder and reranker, onboarding, recall, and recovery all work with no network — so a sovereign node is genuinely sovereign.

---

## v12 - product roadmap capstone

v12 is the planned completion milestone for the SAGE product roadmap: the fully integrated product rather than another backend-only release. It ships as a standalone desktop application in which every CEREBRUM dashboard function is mapped to a real native app experience — the same capabilities, but presented as a proper application rather than a set of web pages. The web CEREBRUM remains a supported fallback for anyone who wants it; it simply is not the primary product surface. By v12 the v11.10–v11.13 bridge has already chosen the desktop shell, proven consumer onboarding and recovery, moved lifecycle into the app, and established accessibility/performance/offline gates, so v12 is the integration-and-polish capstone that ties it into one coherent product. The app owns installation, node lifecycle, onboarding, permissions, updates, health/recovery, federation, and Sharing & Sync as one coherent native-feeling experience, while the SAGE daemon and authenticated local APIs remain cleanly separated underneath.

**Consumer usability is a release criterion, not polish.** A nontechnical person must be able to install SAGE, create or join a node, connect an AI tool, choose what is private or shared, recover from ordinary failures, and keep the app updated without opening a terminal or learning SAGE internals. Every choice uses plain language and safe defaults; destructive or privacy-affecting actions use consistent accessible SAGE dialogs; errors explain what happened, what remains safe, and the next recovery action. The v12 release gate includes clean-machine onboarding and recovery usability tests with people who have not used SAGE before.

The desktop-shell technology is deliberately not locked here. It must be chosen through a security, packaging, accessibility, performance, offline-operation, and cross-platform evaluation; “native-feeling” must not come at the cost of weakening the local trust boundary or bundling an unmaintainable browser runtime.

---

## Current Foundation

The v11 line carries the upgrade substrate, domain-reassign recovery, ancestor grants, PoE-weighted quorum, verdict-correctness scoring, corroboration, and domain-aware validator weighting. Those are baseline capabilities now, not future roadmap work. Release-by-release history lives in the README changelog.
