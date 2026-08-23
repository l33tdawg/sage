# v12 native product status

**Status date:** 2026-08-23

**Code baseline:** SAGE v11.19.0 / app-v27

**Release lines:** `11.19.x` remains stable; `v12-beta` carries experimental
native macOS CEREBRUM builds and must not alter stable publication.

**Product target:** production native application on macOS; browser CEREBRUM on Linux and Windows

**Product-boundary ADR:**
[`desktop-shell-v12-adr.md`](desktop-shell-v12-adr.md) — bounded WebView domain
controls are permitted; app-owned lifecycle/platform controls are mandatory.

This record separates four claims that must not be conflated:

1. a native executable exists;
2. CEREBRUM is available inside its window;
3. the application owns the complete product experience; and
4. a production package has passed distribution and human acceptance gates.

The repository has strong evidence for (1), and the existing SPA provides broad
desktop-window parity for (2). It has not completed (3) or (4).

## Current architecture

The selected foundation is Tauri 2. The Rust host owns single-instance behavior,
window focus, bounded deep links, daemon launch/supervision, SSCP status,
startup/recovery presentation, exact-origin navigation, and external-link
handoff. Once the daemon is authenticated and renderable, the window navigates
to the same daemon-served CEREBRUM HTML/JavaScript application used by browser
CEREBRUM.

The renderer has no privileged Tauri command bridge. SSCP/1 exposes status only;
unlock, restart, update, recovery, administration, federation, and memory actions
remain authenticated web/API workflows. This is a secure native lifecycle shell
around the web CEREBRUM, not yet the complete native-owned product described by
the v12 roadmap.

The definition is now recorded: the shell owns installation, onboarding, daemon
lifecycle, navigation, recovery, permissions/privacy prompts, updates/rollback,
external-link handoff, and acceptance evidence. Authenticated WebView content
may own domain controls, but those rows are `web-control`, not `native-control`.
WebView availability is never counted as native-control completion.

## Readiness estimate

These are planning estimates, not release evidence:

- native lifecycle and security foundation: **about 70%**;
- macOS production distribution work: **about 40%**; and
- the complete macOS v12 native product: **about 25–30%**.

The third estimate is intentionally lower because macOS has no
production-distributed Tauri package, and the app-owned platform
integrations and parity ledger remain open.

## Capability parity

| Primary product area | Current surface | Current classification | Acceptance status |
|---|---|---|---|
| Overview and node health | Embedded SPA | `web-control` | Route/action evidence open |
| MRI brain and memory detail | Embedded SPA | `web-control` | Route/action evidence open |
| Search, filtering, tags, transfer, and forget | Embedded SPA | `web-control` | Route/action evidence open |
| Tasks and agent Messages | Embedded SPA | `web-control` | Route/action evidence open |
| Imports and backup restoration | Embedded SPA | `web-control` plus native file handoff where required | Route/action evidence open |
| Agents, keys, RBAC, and governance | Embedded SPA | `web-control` plus app-owned permission boundary | Route/action evidence open |
| Access Controls | Embedded SPA | `web-control` plus app-owned permission boundary | Route/action evidence open |
| Federation and Sharing & Sync | Embedded SPA | `web-control` plus app-owned external-link/lifecycle boundary | Route/action evidence open |
| Settings, security, recall, maintenance, and updates | Embedded SPA | `web-control` plus app-owned update/rollback/privacy controls | Route/action evidence open |

Current count: **9/9 available as bounded WebView prototypes; 0/9 have complete
route/action acceptance evidence**. Native deep links cover only brain, search,
pipeline, tasks, and settings. The parity ledger must expand those routes into
actions, mark app-owned platform controls separately, and retain immutable
evidence for macOS.

Representative workflow prototypes are recorded in the superseding ADR. Their
current result is: recovery is app-owned; overview, MRI/memory, and settings are
bounded WebView controls with app-owned navigation/origin/lifecycle around them.
The parity ledger remains open until every action has an acceptance row.

## Platform evidence

| Platform | Evidence already present | Production blockers |
|---|---|---|
| macOS | Apple Silicon `.app`/DMG construction; installed lifecycle, single-instance, daemon-survival, uninstall-preservation, reinstall, offline-startup, RSS, and idle-CPU smoke | Production identity; Developer ID signing, notarization, and stapling; Gatekeeper clean-machine launch; Intel matrix; app-level update/failed-update rollback; complete offline workflows; VoiceOver; remaining performance signals and three-run evidence |
| Windows | x64 NSIS preview construction and lifecycle smoke | Not a v12 native product target; browser CEREBRUM is supported |
| Linux | x64 `.deb` and AppImage preview construction and lifecycle smoke | Not a v12 native product target; browser CEREBRUM is supported; optional native R&D remains blocked by `RUSTSEC-2024-0429` |

Current production distribution count: **0/1 target platform**. All current Tauri
packages are private `unsigned-preview-evidence` and retain the `SAGE Native
Preview` identity. The release workflow correctly blocks production promotion.

## macOS blockers

- Apply the frozen fully-native product definition through the route/action
  capability ledger and fail-closed acceptance contract.
- Extend shell ownership beyond status-only recovery into the approved lifecycle,
  update, rollback, permission, and guided-recovery operations without granting
  implicit Root/Admin authority.
- Produce signed production artifacts and installed clean-machine evidence for
  the supported macOS architecture matrix.
- Add paint, interactive-ready, recovery-shown, navigation-latency,
  native-overhead, and MRI frame-timing instrumentation. Today only two of nine
  performance measures are observable and only shell RSS is enforced.
- Complete VoiceOver, keyboard, zoom,
  contrast, reduced-motion, offline, daemon-loss, failed-update, rollback, and
  real nontechnical-user onboarding/recovery acceptance.
- Pass three consecutive benchmark runs on named baseline Mac hardware and
  retain immutable evidence.

## Relationship to Commons and Lantern

The macOS desktop product does not make the native shell a Lantern dependency.
Lantern remains a Linux/ARM64 headless consumer of the daemon and
authenticated APIs. Its v12 compatibility work is tracked separately: exact
machine-readable capability evidence, headless lifecycle/recovery, negative
authority tests, and deterministic committed-PUBLIC export. Commons-owned
intake, packs, fleet control, public query service, and hardware remain outside
the SAGE-core native application.

## Durable backlog lanes

The v12 work is consolidated into two SAGE tasks:

1. `f3291de2-d270-4c3b-b81b-3f29bc54b83b` — deliver native macOS CEREBRUM on
   `v12-beta`, from the first tester build through production acceptance; and
2. `26dee44b-4f7f-459d-a76b-91f0c2d7cdd4` — preserve and prove the separate
   headless Commons/Lantern contract.

The former tri-platform capstone, Linux production, and tri-platform acceptance
tasks are dropped and linked to the consolidated macOS task for history.

## Resume order

1. **Tester build:** establish a beta-specific identity/version and automated
   Apple Silicon `.app`/DMG build on `v12-beta`, preserving the bundled daemon,
   clean install, single-instance, offline, and uninstall-data tests.
2. **Active inventory:** use
   [`v12-native-capability-ledger.md`](v12-native-capability-ledger.md) for the
   CEREBRUM route/action inventory and validate it through the fail-closed
   [`v12-native-acceptance-ledger.md`](v12-native-acceptance-ledger.md). These
   produce a bounded implementation backlog, not release claims. The Linux
   spike is optional R&D and is not on the release path.
3. **App ownership:** execute the macOS capstone from the frozen ADR and
   route/action inventory, including app-owned lifecycle, recovery, updates, and
   rollback.
4. **Promotion evidence:** close signing, clean-machine, offline, accessibility,
   performance, failed-update, rollback, and nontechnical-user acceptance on
   macOS. Linux and Windows retain tested browser CEREBRUM support.
5. **Independent compatibility lane:** advance Commons/Lantern headless
   Linux/ARM64 compatibility and the app-v27 baseline review without coupling it
   to desktop-shell delivery.

The residual host-wake and stranded-claim recovery task remains a separate
pre-v12 carryover stream; it should not be folded into native-product scope.

## Completion rule

v12 may claim the native-product capstone only when macOS passes the
frozen capability, security, distribution, recovery, accessibility, offline,
performance, and human-usability matrices. Browser CEREBRUM is the supported
Linux and Windows product path; no native Linux or Windows application is
required for v12.
