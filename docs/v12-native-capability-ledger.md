# SAGE v12 native capability ledger

**Status:** Native Swift source reconciliation; every acceptance row remains open

**Baseline:** SAGE v11.19.0 / app-v27

**Product boundary:** [`native-cerebrum-macos-v12-adr.md`](native-cerebrum-macos-v12-adr.md)
**Evidence contract:** [`v12-native-acceptance-ledger.md`](v12-native-acceptance-ledger.md)

## Reading this ledger

This is the source-backed starting inventory for v12, not release evidence.
Under the current ADR, the macOS product surface is implemented with
SwiftUI/AppKit/Metal and contains no WebView renderer. `native-control` means a
control rendered and owned by that native application. A mapped placeholder is
navigation coverage only; it does not count as workflow implementation.

All rows below are **open**. The eventual machine inventory must split each
action family into one stable action ID per rendered control and generate the
full `(entry × macOS)` cross-product required by the evidence
schema.

## Reproducible inventory method

The current `scripts/v12-native-inventory.mjs` inventory is derived from the
browser SPA and historical Tauri shell, so it is not authoritative for the
Swift product. Before an RC, replace or extend it to discover `AppRoute`,
`RootView`, native views, toolbars, menus, inspectors, keyboard commands, and
typed `SAGEAPI` calls from `desktop/SAGECerebrumNative`, while reconciling those
calls with `DashboardHandler.RegisterRoutes` and delegated registrars.

For an RC, automation must supplement this method with rendered-control, form,
menu, context-menu, keyboard-command, feature-flag, role, empty-state, and error
state discovery. The baseline does not claim those dynamic branches are
exhaustive.

## Primary route inventory

The implemented Overview, Search, and Brain routes now use titlebar-owned route
titles with compact adaptive context/status bars. Brain keeps mode,
presentation, inspector, View Options, and Refresh in the primary toolbar;
View Options owns rotation, flow, shell visibility, and Whole Brain reset.
Hiding the Brain inspector preserves semantic selection while Escape clears it.
The active route is source-wired through focused scene commands for the standard
macOS View menu: global Focus Search, one routed Refresh, Search inspector and
clear-selection actions, and Brain-only mode, presentation, inspector, selection,
and View Options actions. Keyboard Shortcuts
is in Help. Development IDs are `global.command.focus-search`,
`global.command.keyboard-shortcuts`, route-specific `*.command.refresh`,
`search.command.{toggle-inspector,clear-selection}`, and
`brain.command.{toggle-inspector,mode-memory-map,mode-agent-network,presentation-interactive-map,presentation-list-view,clear-selection,view-options}`.
The DEBUG app-scene gate now launches the real executable, inventories the
materialized `NSApplication.mainMenu`, dispatches the unique **View > Focus
Search** target/action by parent, label, key and modifiers, and proves twice
that the exact mounted `NSSearchToolbarItem` field editor owns the captured
window's first-responder chain. The other rendered commands, keyboard-event
routing, focus return from route content, system-AX discovery, and VoiceOver
acceptance remain open for the RC inventory.

| Entry ID | Route | Mounted UI | Owner | Current native-window path | App-owned integration still required | Status |
|---|---|---|---|---|---|---|
| `overview.route` | `overview` | `OverviewView` | `native-control` | Implemented SwiftUI dashboard | Lifecycle/offline/accessibility/performance acceptance | implemented slice; acceptance open |
| `brain.route` | `brain` | `BrainView`, `MetalBrainView`, `BrainNodeInspectorView`, `AgentNeuronInspectorView` | `native-control` | Implemented Memory/Connectome modes with a pure routed-size layout policy, native compact navigator/toolbar, fit-driven headers/notices, selection-preserving inspector dismissal, hosted 620×540-to-expanded resize evidence, the shared anatomical CEREBRUM hull, time-invariant half-resolution Metal bloom, native spherical cells, nearest-rank p95-normalized traffic ribbons with bounded-cadence `last_fired` plasticity, deterministic reciprocal curves and self-loops, trimmed direction arrowheads, topology-aware focused-edge-preserving 2,048-ribbon LOD, direct Metal edge picking, shared-path GPU flow particles, reduced-motion-aware focus easing, coalesced selection announcements, source-level table/surface/inspector/related-card focus targets, generation-fenced native retry/Metal first-responder delivery, runtime Metal pipeline compilation, shared-encoder 4× MSAA offscreen GPU completion and relative bloom raster evidence, synchronized native tables, reducer-driven renderer-failure fallback/retry with held-progress state, mode/view cancellation, stale-attempt fencing, native accessibility-press activation and duplicate rejection, fail-closed handoff consumption, mount/capability-gated restoration, hosted immediate failure, held stale-success cancellation and successful restoration evidence, selected-agent engram bloom, directed-connection focus, and typed related-memory Train of Thought | Real VoiceOver/system-AX discovery plus table/inspector/related-card focus-return proof, cross-GPU/offline behavior, daemon lifecycle and deeper behavioral/accessibility/large-store/performance evidence | implemented slice; acceptance open |
| `search.route` | `search` | `SearchView`, `MemoryInspectorView` | `native-control` | Implemented SwiftUI table plus selection-independent trailing inspector lifecycle, toolbar/View-menu parity, Escape clear, source-level table/close focus targets, announcements, visible-result reconciliation and modal/mutation-safe command gating; real app-scene evidence covers rendered Focus Search dispatch and repeated mounted search-field-editor focus | Whole-domain transfer; remaining rendered commands and route focus return, keyboard-event routing, system-AX/VoiceOver, large-store and full accessibility acceptance | implemented slice; acceptance open |
| `tasks.route` | `tasks` | `NativePlaceholderView` | `native-control` target | Native destination mapped | Task and Messages workflows | implementation open |
| `import.route` | `importData` | `NativePlaceholderView` | `native-control` target | Native destination mapped | File import and restore workflows | implementation open |
| `network.route` | `network` | `NativePlaceholderView` | `native-control` target | Native destination mapped | Agents, keys, RBAC and governance workflows | implementation open |
| `access.route` | `access` | `NativePlaceholderView` | `native-control` target | Native destination mapped | Access-control workflows | implementation open |
| `federation.route` | `federation` | `NativePlaceholderView` | `native-control` target | Native destination mapped | Federation and Sharing & Sync workflows | implementation open |
| `settings.route` | `settings` | `NativePlaceholderView` | `native-control` target | Native destination mapped | Settings, maintenance, update and rollback workflows | implementation open |
| `session.route` | application state | `RootView`, `LoginView`, `NativeStateView` | `native-control` | Connecting, locked, failed and ready states implemented | Daemon launch/supervision and guided recovery | partial; acceptance open |

The current Swift application has no app-owned deep-link implementation. Deep
links from the historical Tauri prototype are not evidence for this product.

## Action-family inventory

| Action-family ID | Parent | User-visible capability | UI/API source anchors | Owner | Required v12 evidence or integration | Status |
|---|---|---|---|---|---|---|
| `session.login-lock-recover` | global/session | Connect, unlock and lock the local encrypted session; show native failure state | `RootView`, `AppSession`, `LoginView`, typed auth APIs | `native-control` | Daemon launch/supervision, recovery continuity, auth-denial and data-safety evidence | partial; acceptance open |
| `global.navigation-preferences` | global | Native split-view sidebar with three selectable implemented destinations, six non-selectable Coming Soon destinations, and fixed ready-session route commands | `RootView`, `AppRoute`, `SAGECerebrumNativeApp` | `native-control` | Functional Settings scene, deep links, restoration, remaining rendered-command/keyboard-event coverage, zoom and reduced-motion evidence | partial; app-scene Focus Search dispatch/focus covered; acceptance open |
| `onboarding.run` | global/settings | First-run setup and explicit rerun | Browser workflow and onboarding/embeddings/provider APIs are parity references | `native-control` target | Clean-machine native flow, restart continuity, permissions, offline/degraded paths | implementation open |
| `overview.inspect-health` | overview | Health, memory/agent/federation/consensus summary with independent snapshot quality and event-transport state | `OverviewView`, `OverviewViewModel`; health/stats/agents/validators/federation APIs and typed SSE transport | `native-control` | Paint/interactive latency, prolonged SSE loss, stale/degraded and typed offline evidence | implemented; acceptance open |
| `overview.resolve-adoption` | overview | Inspect, retry, assign, or deprecate historical adoption items | Browser workflow and adoption APIs are parity references | `native-control` target | Native implementation, Root/operator authorization, interruption and immutable history evidence | implementation open |
| `brain.explore-memory` | brain | Render the Memory Map (MRI), filter domain/status, orbit/zoom/select, inspect loaded memory, switch to synchronized List View (Accessible Table), preserve focus across invalidation notices, and fall back to that same table with an accessibility-pressable explicit retry when renderer initialization fails | `BrainView`, `BrainViewModel`, `MetalBrainView`; memory graph API and SSE | `native-control` | Real VoiceOver/system-AX discovery and focus delivery, MRI pacing, large-store/offline and deeper behavioral/accessibility evidence | implemented; hosted Memory press, held-progress, mode-cancelled stale-success and restoration transitions covered; acceptance open |
| `brain.inspect-related` | brain | Fetch an exact selected memory's related results and present a resizable Train of Thought pane grouped as Do, Don't, Observations and Notes, with separate typed anchor/related focus and responsive vertical budgets | `BrainView`, `BrainViewModel`; related-memory API | `native-control` | Full SwiftUI focus-transition and VoiceOver evidence plus broader failure-state acceptance | implemented; hosted narrow-window contract complete; acceptance open |
| `brain.connectome` | brain | Switch independently to Agent Network (Connectome); explore visible agents and directed retained local message traffic in the Interactive Map or synchronized List View; pick curved reciprocal and self-loop synapses directly; follow trimmed arrowheads and matching flow particles; bloom a selected agent's visible engrams; inspect incoming/outgoing/peer/activity counts; and focus keyboard-selectable directed connections | `BrainView`, `BrainViewModel`, `MetalBrainView`, `AgentNeuronInspectorView`; network synapses/engrams APIs and SSE | `native-control` | Independent per-mode presentation/camera state, real VoiceOver/responder proof, larger-graph performance and complete behavioral/accessibility evidence | implemented; acceptance open |
| `brain.mutate-memory` | brain/search | Edit tags, bulk tag and governed Forget | `SearchView`, `MemoryInspectorView`, `SearchViewModel`; bulk/tags/forget APIs | `native-control` | Whole-domain transfer, RBAC denial, partial-failure, history-integrity and recovery evidence | partial; acceptance open |
| `search.find-filter` | search | Search/list, filter by agent/domain/status/tag/date/sort, paginate, explicitly inspect one memory independently from bulk selection, hide/reopen details without losing identity, and clear with Escape | `SearchView`, `SearchViewModel`, `MemoryInspectorView`; memory list/tags APIs | `native-control` | Remaining rendered command and route-content responder delivery, keyboard-event routing, large-store latency, empty/error states and complete VoiceOver/system-AX evidence | presentation lifecycle plus real app-scene Focus Search dispatch/repeated mounted responder covered; acceptance open |
| `tasks.manage` | tasks | Create, assign, reorder, and change task status | Browser workflow and task APIs are parity references | `native-control` target | Native implementation, exact-agent authority, concurrent updates, keyboard alternative and offline behavior | implementation open |
| `messages.manage` | tasks | List message work/stats and send agent notes | Browser workflow and pipeline APIs are parity references | `native-control` target | Native implementation, payload/privacy boundaries, no implicit claim/read, busy/offline/error evidence | implementation open |
| `import.preview-confirm` | import | Select supported export, preview, confirm, and monitor import | Browser workflow and import APIs are parity references | `native-control` target | Native file handoff, malicious/large file handling, cancellation, partial recovery, hashes | implementation open |
| `export.backup` | overview/settings | Export/backup node data where surfaced | Dashboard export handler is the API reference | `native-control` target | Native save handoff, stopped-node contract, encryption and restore verification | implementation open |
| `agents.lifecycle` | network | List/create/update/remove agents, merge identities, download bundle | Browser workflow and agent APIs are parity references | `native-control` target | Native file/key handling, authority prompts, audit and rollback-safe failures | implementation open |
| `agents.keys-pairing` | network | Create pairing code, rotate key, hand over Root credential | Browser workflow and pairing/rotation APIs are parity references | `native-control` target | OS credential storage/handoff, anti-phishing copy, recovery and exact-authority evidence | implementation open |
| `agents.domains-governance` | network | Domain ownership, validator/governance proposal and vote operations | Browser workflow and governance APIs are parity references | `native-control` target | Consensus lifecycle, stale proposal, authorization, replay and failure evidence | implementation open |
| `connect.providers-network` | network/settings | Connect local/remote AI tools, network join, ChatGPT tunnel setup | Browser workflows and connect/pairing/wizard APIs are parity references | `native-control` target | App-owned subprocess/network permission prompts, restart recovery, no authority expansion | implementation open |
| `access.agent-policy` | access | Read/update agent role, profile, clearance, restrictions and domain policy | Browser workflow and app-v23 access APIs are parity references | `native-control` target | Unsaved-change guard, hard-deny precedence, no-authority and audit evidence | implementation open |
| `access.groups-linked-readers` | access | Create/update/delete Access Groups; manage linked readers and message consent | Browser workflow and group/reader/consent APIs are parity references | `native-control` target | CAS conflict, exact remote identity, fail-closed federation and privacy evidence | implementation open |
| `federation.master-join` | federation | Turn federation on/off; host/guest QR or spoken-code join/abort/approve/confirm | Browser workflow and federation join/settings APIs are parity references | `native-control` target | Camera/clipboard permissions, route trust, restart/unlock, cancellation and accessibility evidence | implementation open |
| `federation.connection-policy` | federation | Inspect reachability; set permissions/exports/restrictions; pause or revoke | Browser workflow and connection policy APIs are parity references | `native-control` target | Stale-route/trust-generation, mixed-version, authorization and offline recovery evidence | implementation open |
| `federation.sharing-sync` | federation | Configure Copy subscriptions, resend, groups, roles, domains and members | Browser workflow and sync/group APIs are parity references | `native-control` target | Provenance, no remote Write, retry/idempotency, member removal and data-retention evidence | implementation open |
| `settings.recall-models` | settings | Recall thresholds, memory mode, reranker/embedder setup/test/re-embed | Browser workflow and recall/model APIs are parity references | `native-control` target | App-owned downloads/subprocess permissions, offline model path, restart/progress recovery | implementation open |
| `settings.security-ledger` | settings/recovery | Enable/disable ledger, change passphrase, obtain/confirm recovery key | Browser workflow and ledger/recovery APIs are parity references | `native-control` target | Native privacy prompts, secret-safe UI, recovery-key backup and failed-change rollback | implementation open |
| `settings.maintenance` | settings | Cleanup preview/run, boot instructions, autostart | Browser workflow and maintenance APIs are parity references | `native-control` target | Native login-item integration, destructive confirmation, audit and recovery evidence | implementation open |
| `settings.update-restart-rollback` | settings/recovery | Check/apply update, restart and recover failed update | Existing daemon APIs and prototype behavior are design references | `native-control` target | Production signing, helper isolation, generation proof, failed-update rollback and data preservation | implementation open |
| `shell.lifecycle-recovery` | session/recovery | Single instance, daemon launch/attach, status polling and guided recovery | `AppSession` currently attaches; historical prototype is a design reference | `native-control` target | Native Swift ownership, clean install, daemon loss, incompatible pair, no duplicate daemon, performance signals | implementation open |
| `shell.external-handoff` | global/recovery | Open validated HTTPS documentation/browser fallback in OS browser | Native implementation target | `native-control` target | Scheme/origin denial, focus return, offline/error and assistive-technology evidence | implementation open |

## Inventory blind spots that block promotion

- The table groups actions; it is not yet the required stable ID for every
  rendered button, form submission, menu, keyboard command, conditional retry,
  destructive confirmation, and role/feature/error-state branch.
- Delegated route registrars for network, governance, federation, embeddings,
  reranker, ChatGPT tunnel, pairing, and network join must be expanded into the
  generated API/action manifest rather than treated as one family.
- Memory detail is an in-page state rather than a primary hash route; direct
  route restoration and deep-link semantics still need an explicit product
  decision and acceptance IDs.
- Brain uses separate memory-anchor, related-memory, agent, engram, and directed-connection focus.
  Agent and engram scene IDs are collision-safe, and the selected agent's
  bounded engrams render in both the inspector and Metal bloom. A selected
  related memory keeps the primary anchor open, can independently drive scene
  emphasis, and is reconciled against refreshed related-memory payloads.
- Brain SSE handling treats events as invalidation hints and purges graph,
  selection, inspector, engram, and related-memory state on `access`; promotion
  still requires behavioral tests and runtime evidence for coalescing, races,
  authorization changes, Table/Metal parity, and large snapshots.
- Overview, Search, and Brain use typed transport elements rather than synthetic
  event names. Snapshot quality remains visible independently from transport;
  Search and Brain key successful snapshot age to their active request scope,
  map backend partial projections, and keep pending-update state separate.
  Route-level tests cover terminal authorization and scope isolation; real
  URLSession EOF/backoff/cancellation timing remains acceptance-open.
- Browser CEREBRUM remains the Linux/Windows product and parity reference, but
  browser success cannot stand in for native macOS application evidence.
- The source generator does not prove the exact rendered UI/action cross-product
  by itself. Promotion remains blocked until runtime discovery is reconciled and
  the macOS ledger passes the semantic validator.
