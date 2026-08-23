# ADR: v12 native product boundary and capability parity

**Status:** Superseded on 2026-08-23 by
[`native-cerebrum-macos-v12-adr.md`](native-cerebrum-macos-v12-adr.md)

**Date:** 2026-08-23
**Supersedes:** [`desktop-shell-decision.md`](desktop-shell-decision.md) where it
left the meaning of “fully native” unresolved

## Decision

> Historical record only. The bounded-WebView decision below no longer defines
> the v12 macOS product. The current decision requires a fully native
> SwiftUI/AppKit/Metal CEREBRUM surface with no WebView product renderer.

SAGE v12 is a native desktop product on macOS. **Fully
native** means that the installed application owns the platform experience and
the trust boundary: installation, first-run onboarding, per-user daemon
lifecycle, single-instance behavior, window and deep-link navigation, health and
recovery, permissions and privacy prompts, updates, failed-update rollback,
external-link handoff, and the release/accessibility/performance acceptance
evidence.

The product **permits bounded WebView-rendered CEREBRUM** for authenticated
domain controls and data views. This is an intentional product boundary, not a
claim that the WebView is a native widget toolkit. The WebView may render the
overview, MRI/memory views, search, tasks, federation, Sharing & Sync, and
settings controls when all of the following remain true:

- the shell loads only the exact authenticated daemon origin negotiated through
  SSCP/1;
- the renderer remains untrusted and has no filesystem, process, raw IPC, or
  privileged Tauri command bridge;
- app-owned operations use explicit, authenticated contracts and do not grant
  Root, Admin, federation, signing, or vault authority implicitly;
- keyboard, screen-reader, zoom, contrast, reduced-motion, offline, daemon-loss,
  and recovery behavior is proven on macOS; and
- the parity ledger records every route **and action**. A route loading in the
  WebView is availability evidence only; it is never native-control completion.

Platform-native widgets are required for controls whose job is platform
integration or trust-boundary management: install/update/rollback, daemon
start/attach/recovery, OS external-link and file-picker handoff, permission and
privacy prompts, and any control that must operate while CEREBRUM is unavailable.
The authenticated CEREBRUM surface may own domain-specific controls, but those
controls must be labelled `web-control` in acceptance evidence rather than
`native-control`.

This keeps one governed CEREBRUM implementation and the supported browser
fallback while making the native shell accountable for the complete application
experience. Browser CEREBRUM remains the supported product on Linux and
Windows. Native clients for those platforms are outside the v12 commitment;
Linux experiments may continue as optional R&D but cannot gate macOS release.

## Options considered

| Option | Security | Accessibility | Staffing | Maintenance | Migration cost | Decision |
|---|---|---|---|---|---|---|
| Unbounded WebView wrapper | Reuses the browser surface but expands navigation/IPC risk and blurs authority | Depends on each platform WebView with no app-level ownership | Low initially | One codebase, weak product boundary | Low initially, high remediation risk | Rejected |
| **Bounded WebView plus app-owned native shell** | Exact-origin navigation, no privileged bridge, and native lifecycle controls preserve the current trust model | One CEREBRUM accessibility surface plus an explicit macOS matrix | **Manageable**: one web product plus a focused shell team | **Manageable**: shared domain UI and one native lifecycle adapter | **Lowest safe path** from the existing Tauri foundation | **Selected** |
| Native widgets for every CEREBRUM control | Smaller renderer surface is possible, but duplicated auth/RBAC and IPC paths increase integration risk | Potentially strong, but requires a parallel widget/accessibility implementation | Very high; needs platform UI specialists and API/UI duplication | Very high; every feature must stay in sync across browser and native surfaces | Very high; effectively a product rewrite | Rejected for v12 |

The selected option wins on migration and ownership without treating WebView
content as trusted native code. The all-native-widget option could be revisited
for a future product line if a funded parity program and platform-specific
design system exist; it is not a prerequisite for v12.

## Representative workflow prototypes

These prototypes establish the boundary and expose the remaining acceptance
work. They are not release evidence.

| Workflow | Prototype path | Boundary result | v12 interpretation |
|---|---|---|---|
| Overview / node health | Ready SSCP response pins the daemon UI origin, then `attach_ready` navigates the single window to daemon-served CEREBRUM | The shell owns launch, attach, origin validation, and window state; the overview is a `web-control` | Prove route/action parity, paint and interactive-ready timing, keyboard/screen-reader behavior, and daemon-loss recovery; do not call it a native widget mapping |
| MRI / memory detail | Bounded `sage://brain/...` and `sage://search/...` routes restore into the pinned origin | Deep links are app-owned; memory graph, search, transfer, forget, and related actions remain authenticated WebView controls | Inventory each route/action and prove classification/RBAC errors, offline behavior, accessibility, and navigation latency |
| Recovery | Bundled recovery HTML presents starting, locked, draining, failed, unavailable, and incompatible states; browser fallback is exposed only after authenticated origin validation | Recovery is a native-owned control surface and remains usable when CEREBRUM cannot render | Prove retry, daemon-loss, incompatible-version, update, rollback, and nontechnical-user recovery on macOS |
| Settings / maintenance | Bounded `sage://settings` opens the authenticated settings surface; update/ledger/restart APIs remain daemon-authorized | Settings are currently `web-control`; update/restart/rollback and vault/privacy prompts must have app-owned lifecycle contracts | Prove every settings action, not just route loading, and record which actions have native platform affordances |

The implementation evidence for these prototypes is the current
`desktop/sage-shell/src/main.rs`, `desktop/sage-shell/src/control.rs`, recovery
UI, and [`native-app-daemon-contract.md`](native-app-daemon-contract.md). The
current prototype therefore closes the boundary decision but does not close
capability parity.

## Acceptance consequences

The v12 parity ledger must have, at minimum, these fields for every CEREBRUM
route and action: platform, workflow, route/action identifier, owner
(`native-control` or `web-control`), authenticated API, offline behavior,
daemon-loss behavior, accessibility evidence, performance artifact, and
immutable artifact hash. Required native platform controls cannot be satisfied
by a WebView screenshot or a successful route load.

Promotion is blocked until macOS has complete rows for installation, onboarding,
node lifecycle, permissions, updates/rollback, health/recovery, federation,
Sharing & Sync, and every CEREBRUM route/action. Linux and Windows must retain a
verified browser CEREBRUM path, but they have no native acceptance rows and do
not gate the macOS application.

This ADR feeds [`v12-native-product-status.md`](v12-native-product-status.md),
[`native-shell-quality-gates.md`](native-shell-quality-gates.md), and the v12
capstone roadmap. The older ADR remains the Tauri foundation record; this ADR
is the authoritative definition of the v12 product boundary.
