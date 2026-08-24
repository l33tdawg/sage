# v12 native milestone and product-opportunity review

**Reviewed:** 2026-08-24 against the accepted SwiftUI/AppKit/Metal ADR, roadmap,
capability ledger, acceptance contract, current Swift source, and workflows.

The native foundation is genuine and the implemented Overview, Search, and
Brain slices are strong. It is not yet a production-complete v12 product. This
document keeps release blockers separate from ideas so useful enhancements do
not obscure the critical path.

## Must ship before production acceptance

### P0 — product completeness and trust

1. Replace the six placeholder destinations—Tasks & Messages, Import, Agents,
   Access Controls, Federation, and Settings—with complete native workflows.
2. Give the application ownership of daemon installation, single-instance
   launch, supervision, readiness, drain, restart, diagnostics, and safe
   recovery. It currently attaches to an already-running node.
3. Implement the native session trust bootstrap: bind a one-use session to the
   validated daemon generation, startup proof, endpoint, and application
   identity without granting implicit Root/Admin authority.
4. Replace the browser/Tauri inventory producer with an exact Swift-native
   route/action/menu/shortcut/API inventory and validate a real candidate
   ledger—not only validator unit tests.
5. Deliver clean-machine onboarding and recovery: create or join a node,
   connect an AI tool, explain privacy choices, create and verify recovery
   material, restore safely, and recover from a forgotten passphrase without a
   terminal.
6. After the product surface is complete, pass the signed/notarized update,
   rollback, offline, clean-machine, provenance, and three-run named-Mac gates.

### P1 — acceptance completeness

- Add typed native deep links, route/workspace restoration, external-link
  handoff, file panels, and normal macOS lifecycle integration.
- Replace the generic unavailable screen with state-specific starting,
  degraded, incompatible, disk-full, daemon-loss, and update-recovery journeys.
  Every error must say what happened, what remains safe, and what to do next.
- Complete 200% text/reflow, keyboard, VoiceOver, contrast, reduced-motion, and
  narrow-window acceptance; remove fixed-size clipping risks.
- Freeze and exercise the macOS/architecture/GPU/display support matrix.
- Bind Windows/Linux browser-continuity evidence to the exact same v12
  candidate identity.
- Extend the implemented route-level snapshot/event-stream contract into future
  global recovery surfaces. The sidebar is neutral; Overview, Search, and Brain
  now separate fetched-data age, partial/stale results, pending updates, and
  typed SSE reconnect state without claiming offline reachability.

## User-life improvements

These are ordered by user impact rather than novelty.

### Immediate v12 UX priorities

1. **Guided home and health.** Turn Overview into an actionable “what needs my
   attention” surface. Health cards should deep-link to the exact recovery or
   stale subsystem instead of merely displaying telemetry.
2. **Plain-language recovery model.** Use one consistent pattern for cause,
   safety, next action, diagnostics, and browser fallback across every screen.
3. **Global activity center.** Show imports, updates, synchronization,
   governance confirmations, and recoverable failures in one bounded history;
   do not make users hunt through routes for background work.
4. **Progressive onboarding.** Introduce node, memory, agents, federation, and
   governance only when relevant. Start with safe defaults and reveal advanced
   protocol language behind contextual help.
5. **Authoritative changing-state language.** Extend the implemented distinction
   between snapshot quality and event transport to onboarding/recovery, adding
   offline and permission-limited labels only when typed evidence can prove them.
6. **Safe action consistency.** Destructive/privacy-affecting actions use the
   same review sheet, consequence summary, exact scope, confirmation language,
   indeterminate-commit handling, and focus return.

### High-value enhancements after blockers

- Command palette across routes, memories, agents, settings, and help.
- Saved searches, pinned filters, recent items, and resumable workspaces.
- Shortcut discovery overlay plus a task-oriented native Help menu.
- Contextual “why this matters” explanations for governance and security.
- Optional multi-window memory and agent inspectors.
- Brain quality/performance presets with honest frame and data-density status.
- Explicitly safe Spotlight/Quick Look integration for approved local metadata.

## First UI/UX implementation slice

The first bounded navigation-honesty increment is implemented: unfinished
destinations are visibly grouped as non-selectable Coming Soon work; only the
three real routes retain fixed shortcuts; commands are disabled outside a ready
session for navigation and Lock; placeholder Settings no longer owns
`Command-,`; and Brain's primary
mode/presentation labels lead with plain language while preserving MRI and
Connectome as secondary technical terms. This does not complete those routes.

The remaining design increments should improve comprehension without creating
another large subsystem:

1. **Implemented:** Replace binary Live/Polling labels with separate snapshot and event-stream
   states: Loading, Updated N seconds ago, Partially Updated, Refresh Failed,
   independently signaled Update Available, and Event Updates
   Connecting/Connected/Reconnecting/Stopped. Snapshot age is request-scope keyed and
   backend partial projections remain visible beside pending updates. The
   sidebar footer is already neutral. Do not claim Offline until daemon
   lifecycle or typed reachability evidence can prove it.
   Route-level transition coverage is implemented; real URLSession
   EOF/backoff/cancellation timing remains an acceptance item.
2. **Implemented for Overview, Search, and Brain:** Remove duplicated page-title
   chrome. The unified macOS titlebar owns the route title and a compact,
   width-adaptive context/status bar remains in content.
3. **Implemented:** Simplify the Brain toolbar: mode, presentation, inspector,
   View Options, and Refresh remain visible; rotation, flow, shell visibility,
   and reset are grouped under View Options.
4. **Implemented for Brain:** Inspector hiding is independent of selection.
   Closing the inspector preserves semantic selection; Escape clears selection
   and returns focus. Search inspector lifecycle remains an acceptance item.
5. Add filtered-empty recovery in Search (**Clear Filters**), Retry on Brain
   detail errors, safe per-feed errors on Overview, and Diagnose/drill-down
   actions on unhealthy cards.
6. Add menu commands and discoverability for Focus Search, Refresh,
   Show/Hide Inspector, Memory Map/List View, Memory/Agent Network, Clear
   Selection, View Options, and Keyboard Shortcuts.
7. **Implemented for the current native slices:** General titles and metrics use
   standard SF Pro. Rounded typography is allowlisted to the CEREBRUM mark and
   Overview hero accent; SF Mono remains reserved for identifiers, hashes, and
   diagnostics.
8. Replace continuous decorative SSE pulsing with brief event-linked
    highlights, a coalesced “updates available” affordance when selection would
    be disturbed, and instant transitions under Reduce Motion.

The desired result is a calm macOS instrument: native and quiet by default,
dense when needed, immediately understandable, and fast for keyboard users.

## Delivery order

1. Finish the system-AX/VoiceOver and native-inventory foundations.
2. Implement daemon lifecycle and the session trust bootstrap.
3. Build onboarding, recovery, global status, and activity-center primitives.
4. Port the six missing destinations using those shared primitives.
5. Complete accessibility, offline, performance, and candidate-bound evidence.
6. Resume signing/notarization only when the complete product can be tested as
   a coherent user journey.
