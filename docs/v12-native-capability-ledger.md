# SAGE v12 native capability ledger

**Status:** Bounded source inventory; every acceptance row remains open

**Baseline:** SAGE v11.19.0 / app-v27

**Product boundary:** [`desktop-shell-v12-adr.md`](desktop-shell-v12-adr.md)
**Evidence contract:** [`v12-native-acceptance-ledger.md`](v12-native-acceptance-ledger.md)

## Reading this ledger

This is the source-backed starting inventory for v12, not release evidence. A
route shown inside the authenticated WebView proves only that the surface is
reachable. It does not prove that every action works in the native product, is
accessible, survives daemon loss, works offline, or passes the macOS
acceptance contract.

`web-control` means authenticated CEREBRUM content in the bounded WebView.
`native-control` means app-owned platform or trust-boundary behavior that must
remain available when CEREBRUM cannot render. A platform handoff is recorded as
a `native-control` because the application owns its validation and OS boundary;
it is not a CEREBRUM widget rewrite.

All rows below are **open**. The eventual machine inventory must split each
action family into one stable action ID per rendered control and generate the
full `(entry × macOS)` cross-product required by the evidence
schema.

## Reproducible inventory method

Run `node scripts/v12-native-inventory.mjs --root .` at the exact candidate
commit. The generator fails closed when its source seams cannot be parsed and
hashes every input. At the current baseline it discovers 9 primary SPA routes,
168 exported API actions, and 5 exact deep-link hosts. Its emitted blind spots
are blockers, not waivers: runtime controls and server authorization contracts
must still be reconciled into the release inventory.

The baseline ledger was derived from four source seams; the generator covers
the first, second, and fourth directly while the third remains an explicit
manual reconciliation step:

1. `App.applyHash`, the sidebar, and page mounts in
   `web/static/js/app.js` define the primary CEREBRUM routes.
2. Exported calls in `web/static/js/api.js` identify user-triggerable API action
   families.
3. `DashboardHandler.RegisterRoutes` in `web/handler.go`, plus its delegated
   network, governance, federation, setup, and pairing registrars, identifies
   authenticated server contracts and gates.
4. `parse_deep_link`, `navigation_allowed`, `attach_ready`, and recovery state
   handling in `desktop/sage-shell/src/main.rs` identify current app-owned
   navigation and recovery behavior.

For an RC, automation must supplement this method with rendered-control, form,
menu, context-menu, keyboard-command, feature-flag, role, empty-state, and error
state discovery. The baseline does not claim those dynamic branches are
exhaustive.

## Primary route inventory

| Entry ID | Route | Mounted UI | Owner | Current native-window path | App-owned integration still required | Status |
|---|---|---|---|---|---|---|
| `overview.route` | `#/overview` | `OverviewPage` | `web-control` | Bounded WebView; no direct `sage://overview` deep link | Paint/interactive marks, daemon-loss transition, complete action IDs | open |
| `brain.route` | `#/` (fallback route) | `MriView`, `BrainView`, `MemoryDetail` | `web-control` | Bounded WebView; `sage://brain[/…]` accepted | Route restoration, MRI frame marks, accessible graph/detail operation | open |
| `search.route` | `#/search` | `SearchPage` | `web-control` | Bounded WebView; `sage://search[/…]` accepted without query/fragment | Safe app-owned parameter handoff, action-level parity | open |
| `tasks.route` | `#/tasks`; legacy `#/pipeline` | `TasksPage` | `web-control` | Bounded WebView; `sage://tasks` and `sage://pipeline` accepted | Notification/deep-link intent restoration, task/message action parity | open |
| `import.route` | `#/import` | `ImportPage` | `web-control` | Bounded WebView; no direct deep link | Reviewed native file handoff and recovery-safe import progress | open |
| `network.route` | `#/network` | `NetworkPage` | `web-control` | Bounded WebView; no direct deep link | OS key/bundle/file handoffs, permission prompts, restart-safe wizards | open |
| `access.route` | `#/access` | `NetworkPage(accessMode=true)` | `web-control` | Bounded WebView; no direct deep link | App-owned privacy/authority confirmation boundary | open |
| `federation.route` | `#/federation` | `FederationPage` | `web-control` | Bounded WebView; no direct deep link | Camera/clipboard/external-route permission handoffs and recovery | open |
| `settings.route` | `#/settings` | `SettingsPage` | mixed: `web-control` plus required `native-control` lifecycle operations | Bounded WebView; `sage://settings` accepted | Native update/rollback, restart/recovery, privacy and permission controls | open |
| `recovery.route` | bundled recovery URL | `desktop/sage-shell/ui/index.html` via `show_recovery` | `native-control` | App-owned bundled surface for starting, unavailable, locked, draining, failed, incompatible | Guided retry/update/rollback, focus semantics, nontechnical-user recovery | open |
| `help.overlay` | in-page overlay | `HelpOverlay` | `web-control` | Bounded WebView | Offline completeness and validated external-document handoff | open |

The shell's current deep-link allowlist is exactly `brain`, `search`,
`pipeline`, `tasks`, and `settings`. It rejects queries, fragments, credentials,
unknown hosts, unsafe characters, and oversized routes. That is navigation
evidence only; it does not close any action row.

## Action-family inventory

| Action-family ID | Parent | User-visible capability | UI/API source anchors | Owner | Required v12 evidence or integration | Status |
|---|---|---|---|---|---|---|
| `session.login-lock-recover` | global/recovery | Check session, login, auto/manual lock, recover encrypted vault | `App`, `LoginScreen`; `checkAuth`, `login`, `lockSession`, `recoverVault`; dashboard auth/recovery routes | mixed | Native recovery continuity without exposing the passphrase; auth-denial and data-safety evidence | open |
| `global.navigation-preferences` | global | Sidebar navigation, Back/Forward guard, help, text size, theme | `App.applyHash`, `createAccessControlHistoryNavigator`, sidebar/top bar | mixed | Keyboard/focus/zoom/reduced-motion evidence and safe app deep-link restoration | open |
| `onboarding.run` | global/settings | First-run setup and explicit rerun | `OnboardingWizard`; onboarding, embeddings, provider-connect APIs | mixed | Clean-machine native flow, restart continuity, permissions, offline/degraded paths | open |
| `overview.inspect-health` | overview | Health, memory/agent/federation/consensus summary and live activity | `OverviewPage`, `HealthBar`, `ChainActivityLog`; stats/health/validators/scopes/SSE handlers | `web-control` | Paint/interactive latency, SSE loss, stale/degraded and offline evidence | open |
| `overview.resolve-adoption` | overview | Inspect, retry, assign, or deprecate historical adoption items | `MemoryAdoptionResolutionModal`; adoption progress/inventory/retry/assign/deprecate APIs | `web-control` | Root/operator authorization, interruption and immutable history evidence | open |
| `brain.explore-memory` | brain | Render MRI/graph, filter timeline/domain, select memory, inspect related train of thought | `MriView`, `BrainView`, `MemoryDetail`; graph/timeline/engrams/related handlers | `web-control` | Keyboard/screen-reader graph alternative, MRI pacing, large-store and offline evidence | open |
| `brain.mutate-memory` | brain/search | Edit metadata, tags, transfer/bulk update, forget/delete | `MemoryDetail`, `SearchPage`; update/delete/bulk/tags APIs | `web-control` | Confirmation, RBAC denial, partial-failure, history-integrity and recovery evidence | open |
| `search.find-filter` | search | Search/list, filter by agent/domain/status/type, paginate and inspect | `SearchPage`; memory list/tags APIs | `web-control` | Query parameter restoration, large-store latency, empty/error states, accessibility | open |
| `tasks.manage` | tasks | Create, assign, reorder, and change task status | `TasksPage`; task list/create/assign/order/status APIs | `web-control` | Exact-agent authority, concurrent updates, keyboard drag alternative and offline behavior | open |
| `messages.manage` | tasks | List message work/stats and send agent notes | `TasksPage`; pipeline list/stats/send APIs | `web-control` | Payload/privacy boundaries, no implicit claim/read, busy/offline/error evidence | open |
| `import.preview-confirm` | import | Select supported export, preview, confirm, and monitor import | `ImportPage`; import preview/confirm/upload handlers | mixed | Native file handoff, malicious/large file handling, cancellation, partial recovery, hashes | open |
| `export.backup` | overview/settings | Export/backup node data where surfaced | dashboard export handler and settings/maintenance UI | mixed | Native save handoff, stopped-node contract, encryption and restore verification | open |
| `agents.lifecycle` | network | List/create/update/remove agents, merge identities, download bundle | `NetworkPage`; `fetchAgents`, `createAgent`, `updateAgent`, `removeAgent`, `mergeAgent`, `downloadBundle` | mixed | Native file/key handling, authority prompts, audit and rollback-safe failures | open |
| `agents.keys-pairing` | network | Create pairing code, rotate key, hand over Root credential | `NetworkPage`; pairing, rotation and Root handover APIs | mixed | OS credential storage/handoff, anti-phishing copy, recovery and exact-authority evidence | open |
| `agents.domains-governance` | network | Domain ownership, validator/governance proposal and vote operations | `NetworkPage`; domain reassignment and governance API families/registrars | `web-control` | Consensus lifecycle, stale proposal, authorization, replay and failure evidence | open |
| `connect.providers-network` | network/settings | Connect local/remote AI tools, network join, ChatGPT tunnel setup | provider/tunnel/network-join panels; connect, pairing and wizard registrars | mixed | App-owned subprocess/network permission prompts, restart recovery, no shell authority expansion | open |
| `access.agent-policy` | access | Read/update agent role, profile, clearance, restrictions and domain policy | `NetworkPage(accessMode)`; app-v23 access APIs | `web-control` | Unsaved-change navigation guard, hard-deny precedence, no-authority and audit evidence | open |
| `access.groups-linked-readers` | access | Create/update/delete Access Groups; manage linked readers and message consent | access controls UI; group, linked-reader and consent APIs | `web-control` | CAS conflict, exact remote identity, fail-closed federation and privacy evidence | open |
| `federation.master-join` | federation | Turn federation on/off; host/guest QR or spoken-code join/abort/approve/confirm | `FederationMasterSwitch`, join wizards; federation join/settings APIs | mixed | Camera/clipboard permissions, route trust, restart/unlock, cancellation and accessibility evidence | open |
| `federation.connection-policy` | federation | Inspect reachability; set permissions/exports/restrictions; pause or revoke | `FederationPage`; connection policy/status/revoke API families | `web-control` | Stale-route/trust-generation, mixed-version, authorization and offline recovery evidence | open |
| `federation.sharing-sync` | federation | Configure Copy subscriptions, resend, groups, roles, domains and members | `SharingSyncGroupsPanel`; sync/group API families | `web-control` | Provenance, no remote Write, retry/idempotency, member removal and data-retention evidence | open |
| `settings.recall-models` | settings | Recall thresholds, memory mode, reranker/embedder setup/test/re-embed | `SettingsPage`, `MemoryMode`; recall/reranker/embedding APIs | mixed | App-owned downloads/subprocess permissions, offline model path, restart/progress recovery | open |
| `settings.security-ledger` | settings/recovery | Enable/disable ledger, change passphrase, obtain/confirm recovery key | `SettingsPage`; ledger APIs and unauthenticated recovery route | mixed | Native privacy prompts, secret-safe UI, recovery-key backup and failed-change rollback | open |
| `settings.maintenance` | settings | Cleanup preview/run, boot instructions, autostart | `SettingsPage`; cleanup/boot/autostart APIs | mixed | Native login-item integration, destructive confirmation, audit and recovery evidence | open |
| `settings.update-restart-rollback` | settings/recovery | Check/apply update, restart and recover failed update | `SettingsPage`, `UpdateBanner`; update/restart handlers | **`native-control` required** | Production signing, helper isolation, generation proof, failed-update rollback and data preservation | open |
| `shell.lifecycle-recovery` | recovery | Single instance, daemon launch/attach, status polling, origin pinning, route restoration | `supervise`, `attach_ready`, `show_recovery`, `control::status` | `native-control` | macOS clean install, daemon loss, incompatible pair, no duplicate daemon, performance signals | open |
| `shell.external-handoff` | global/recovery | Open validated HTTPS documentation/browser fallback in OS browser | `navigation_allowed`, `open_external`, recovery fallback validation | `native-control` | Scheme/origin denial, focus return, offline/error and assistive-technology evidence | open |

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
- Browser CEREBRUM and the bounded WebView share code, so tests must exercise
  both paths and measure native overhead; browser success cannot stand in for
  native application evidence.
- The source generator does not prove the exact rendered UI/action cross-product
  by itself. Promotion remains blocked until runtime discovery is reconciled and
  the macOS ledger passes the semantic validator.
