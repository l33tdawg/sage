# v12 native product status

**Status date:** 2026-08-23

**Code baseline:** SAGE v11.19.0 / app-v27

**Release lines:** `11.19.x` remains stable; `v12-beta` carries experimental
native macOS CEREBRUM builds and must not alter stable publication.

**Product target:** production native application on macOS; browser CEREBRUM on Linux and Windows

**Product-boundary ADR:**
[`native-cerebrum-macos-v12-adr.md`](native-cerebrum-macos-v12-adr.md) — the
macOS product surface is SwiftUI/AppKit/Metal with no WebView renderer.

## Direction reset: fully native

The earlier bounded-WebView decision is superseded. The Tauri build remains
valuable lifecycle and package evidence, but it is now explicitly a prototype,
not the v12 native release candidate. Signing and notarization are deferred
until the native product surface is testable.

The new implementation lives in `desktop/SAGECerebrumNative`. It currently has
a compiling SwiftUI application, native navigation for all nine primary
CEREBRUM routes, native login/lock states, a typed loopback API client, and
implemented vertical slices for Overview, Search/Inspector, and the Brain
experience. Overview consumes five independent feeds—health, stats, agents,
validators, and federation—and retains last-good data with per-feed stale state
during partial failures.

Search now uses a native macOS table, toolbar search, multiselection, filter
popover, contextual menu, cursor continuation, projection warnings, and a
standard trailing memory inspector. It is backed by the operator-only
`/v1/dashboard/memory/list` contract, preserves the browser's 250 ms query
debounce and last-issued-wins behavior, and labels the wire value truthfully as
stored confidence. Domain, lifecycle, counted-tag, agent, preset/custom date,
and sort filters are implemented. SSE invalidations trigger authoritative
refetches, but an active selection is protected behind an explicit "updated
results available" notice. Native tag editing now normalizes input, replaces
the complete tag set, re-reads canonical tags after success, and rolls back with
an inline error after failure. Bulk tagging reports the backend's exact partial
count. Single and bulk Forget use native destructive confirmation, preserve the
durable-FACT warning, execute IDs sequentially, stop on uncertain signer state,
and distinguish deprecated, challenge-opened, settling, failed, and not-attempted
outcomes. Whole-domain ownership transfer remains open.

Brain now separates native Memory and Connectome modes from the independent MRI
and Accessible Table presentations. Memory reads
`/v1/dashboard/memory/graph`; Connectome reads
`/v1/dashboard/network/synapses`. Both modes render shared selection state
through an interactive AppKit `MTKView` with Metal shaders and a synchronized
SwiftUI table. The MRI now consumes the same anatomical `brain.obj` hull as
browser CEREBRUM, uses its ten-color domain palette and deterministic cortical
placement rules, and renders native spherical billboard cells, additive halos,
and directional traffic particles. Memory provides domain lobes, graph edges, memory focus,
orbit/zoom, pause/flow controls, projection and continuation notices, and a
native trailing inspector. Connectome provides an agent navigator, directed
synapses, incoming/outgoing and peer counts, last retained activity, and an
agent inspector that loads a bounded set of visible committed engrams from
`/v1/dashboard/memory/engrams`. Selecting an agent now blooms those engrams as
smaller typed Metal nodes around the agent anchor, with author tethers and
bridges only to corroborators already visible in the bounded Connectome. The
inspector exposes keyboard-selectable directed connections and engrams while a
focused connection is highlighted in Metal. Its traffic counts describe
retained local message history, not lifetime traffic, live presence, or online
status.

Selecting a Memory-mode node also loads
`/v1/dashboard/memory/{id}/related` into a resizable native Train of Thought
pane grouped as Do, Don't, Observations, and Notes. SSE events are treated as
invalidation hints, selected focus is preserved behind update notices, and an
`access` event immediately purges Brain graphs, selections, inspectors,
engrams, and related-memory state before authoritative refetch. Reduced Motion
disables automatic scan and flow while preserving manual orbit and zoom. Brain
phase three is implemented with separate agent, engram, and directed-connection
focus, collision-safe scene identities, reserved overlay budgets, and linear
traffic summaries. The renderer now adds time-invariant half-resolution
offscreen extraction, separable blur, and additive bloom; retained traffic
drives p95-normalized screen-space ribbon width; and selected cells receive a
reduced-motion-aware camera-focus ease. Related-memory focus is not yet
independently typed, direct Metal edge hit-testing and self-loop/reciprocal-edge
geometry remain open, and deeper behavioral, accessibility, large-store,
Metal-fallback, and performance evidence remains open.

The first arm64 application bundle was built and launch-tested on 2026-08-23 at
`dist/v12-native/12.0.0-beta.1/SAGE CEREBRUM Native.app`. Launch Services
created a visible AppKit window, SSCP attached it to the running local daemon,
and the native encrypted-vault unlock state rendered successfully. The
executable links AppKit, SwiftUI, Foundation and CFNetwork and has no WebKit or
JavaScriptCore dependency. This is development evidence, not signed release
evidence.

The native visual foundation is now frozen in
[`native-cerebrum-design-system.md`](native-cerebrum-design-system.md). The
first design pass establishes SF Pro/SF Mono roles, adaptive light/dark SAGE
tokens, grouped macOS navigation, semantic cards, responsive metrics, native
unlock progress/focus behavior, reduced-motion-aware transitions, and explicit
loading/degraded labels. Overview now consumes the dashboard SSE stream as an
invalidation channel, refetches authoritative state after events, reports live
versus reconnecting status, pauses while the scene is inactive, and retains a
30-second polling recovery path.

This record separates four claims that must not be conflated:

1. a native executable exists;
2. native CEREBRUM workflows are available inside its window;
3. the application owns the complete product experience; and
4. a production package has passed distribution and human acceptance gates.

The repository has strong development evidence for (1) and for the three native
vertical slices described above. The remaining six routes are native
placeholders, so it has not completed (3) or (4).

## Historical prototype architecture

The earlier foundation is Tauri 2. The Rust host owns single-instance behavior,
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

The shell now bounds a bundled-daemon startup attempt to 30 seconds, terminates
only the exact child it spawned when that deadline expires, and re-arms one
automatic launch after a verified ready generation is later lost. The daemon's
authoritative instance lock and dedicated contention exit code remain the
duplicate-process fence. Guided retry, diagnostics, update, and rollback controls
are still open product work.

The beta bundle identity now defaults to `~/.sage-v12-beta` and passes that
resolved home explicitly to its bundled daemon. Stable and native-preview
identities continue to default to `~/.sage`, while an explicit `SAGE_HOME`
remains available for controlled testing or a future migration ceremony. The
installed-DMG lifecycle smoke proves the beta default does not create `~/.sage`.

Its lifecycle work will be ported or wrapped behind native Swift services where
appropriate. No WebView route counts toward completion under the current ADR.

## Readiness estimate

These are planning estimates, not release evidence:

- reusable lifecycle and security prototype: **about 75%**;
- fully native macOS route/action surface: **about 25%**; and
- the complete macOS v12 native product: **about 23%**.

These estimates reset after replacing the renderer architecture. Overview,
Search/Inspector, and the Brain Memory/Connectome experience are implemented vertical
slices; the remaining routes are mapped native destinations, not completed
workflows. Brain phase three is functional, but its remaining implementation and
acceptance gaps are not counted as another route slice.

## Capability parity

| Primary product area | Current surface | Current classification | Acceptance status |
|---|---|---|---|
| Overview and node health | SwiftUI dashboard backed by five typed feeds | `native-control` | First vertical slice implemented; lifecycle/auth hardening and visual parity open |
| Brain, Connectome, and memory detail | SwiftUI/AppKit surface with separate Memory/Connectome modes, shared anatomical CEREBRUM hull, custom Metal MRI with time-invariant multi-pass bloom, luminous native cells, weighted ribbons, directional flow and focus easing, synchronized native tables, memory/agent inspectors, selected-agent engram bloom, directed-connection focus, and a related-memory Train of Thought pane | `native-control` | Brain visual-parity pass implemented; pixel-level bloom evidence, typed related focus, direct Metal edge hit-testing, reciprocal/self-loop geometry, daemon lifecycle, Metal fallback, large-store tuning, and deeper behavioral/accessibility/performance evidence remain open |
| Search, filtering, tags, transfer, and forget | SwiftUI table, native filters, memory inspector, tag mutation and governed Forget flows backed by typed dashboard APIs | `native-control` | Search/filter/select/inspect/load-more, tag editing, bulk tagging and safe single/bulk Forget implemented; whole-domain transfer and full acceptance evidence remain open |
| Tasks and agent Messages | Native destination reserved | `native-control` target | Implementation open |
| Imports and backup restoration | Native destination reserved | `native-control` target | Implementation open |
| Agents, keys, RBAC, and governance | Native destination reserved | `native-control` target | Implementation open |
| Access Controls | Native destination reserved | `native-control` target | Implementation open |
| Federation and Sharing & Sync | Native destination reserved | `native-control` target | Implementation open |
| Settings, security, recall, maintenance, and updates | Native destination reserved | `native-control` target | Implementation open |

Current count: **9/9 native destinations mapped; 3/9 have implemented vertical
slices; 0/9 has complete route/action acceptance evidence**.

Overview, Search/Inspector, and the Brain Memory/Connectome workflow are real native controls;
they do not load the browser SPA or a WebView. The parity ledger remains open
until every route/action has an acceptance row and the native application owns
daemon lifecycle, recovery, updates, and rollback.

## Platform evidence

| Platform | Evidence already present | Production blockers |
|---|---|---|
| macOS | Launch-tested unsigned Apple Silicon Swift application with native Overview, Search/Inspector, and Brain Memory/Connectome phase three; builder identity `com.sage.cerebrum.beta`; no WebKit or JavaScriptCore linkage | Bundled-daemon lifecycle, native recovery/update/rollback, remaining Brain polish/evidence, complete route parity, Developer ID/notarization, Gatekeeper clean-machine launch, architecture matrix, offline/accessibility/performance and three-run evidence |
| Windows | x64 NSIS preview construction and lifecycle smoke | Not a v12 native product target; browser CEREBRUM is supported |
| Linux | x64 `.deb` and AppImage preview construction and lifecycle smoke | Not a v12 native product target; browser CEREBRUM is supported; optional native R&D remains blocked by `RUSTSEC-2024-0429` |

Current production distribution count: **0/1 target platform**. The native
Swift bundle builder emits the distinct `com.sage.cerebrum.beta` application
identity. Current local artifacts are unsigned development/test evidence, not a
production package. Because this repository is public, CI uploads
only non-package validation evidence and never the DMG. A future signed beta
requires an access-controlled tester store and does not enter the stable
publication or automatic-update channel. The release workflow correctly blocks
production promotion.

## macOS blockers

- Apply the frozen fully-native product definition through the route/action
  capability ledger and fail-closed acceptance contract.
- Port daemon launch, supervision, single-instance behavior, recovery,
  update/rollback, permission, and guided diagnostics into native Swift services
  without granting implicit Root/Admin authority. The current Swift application
  attaches to an already-running loopback daemon; it does not yet own that
  daemon's lifecycle.
- Complete the remaining Brain work: introduce reciprocal/self-loop ribbon
  geometry, last-fired plasticity, typed related-memory focus, and direct Metal
  edge hit-testing, then prove
  Metal/Table parity, access-purge behavior, keyboard/VoiceOver operation,
  large-store performance, and graceful behavior when Metal is unavailable.
- Convert the private tester artifact into a signed production candidate and
  produce installed clean-machine evidence for the supported macOS architecture
  matrix. The nested Go daemon must be Developer ID signed before the outer app,
  then the DMG must be signed, notarized, stapled, and assessed. Numeric Apple
  marketing/build versions are now generated separately from the beta SemVer.
- Design the explicit import/migration ceremony for testers who intentionally
  want to copy stable data into the isolated beta profile. The beta no longer
  opens stable `~/.sage` implicitly.
- Add paint, interactive-ready, recovery-shown, navigation-latency,
  native-overhead, and MRI frame-timing instrumentation. Today only two of nine
  performance measures are observable and only shell RSS is enforced. Raw
  artifacts must be rehashed and their budgets recomputed by the validator
  instead of trusting a declared pass result.
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

The active program is consolidated into three linked, non-overlapping SAGE
tasks rather than per-feature fragments:

1. `f3291de2-d270-4c3b-b81b-3f29bc54b83b` — deliver native macOS CEREBRUM on
   `v12-beta`, from the first tester build through production acceptance; and
2. `26dee44b-4f7f-459d-a76b-91f0c2d7cdd4` — preserve and prove the separate
   headless Commons/Lantern contract; and
3. `b6d9bade-92fa-4d74-a8c4-9b0cc35d280d` — finish the pre-v12 automatic
   host-wake and stranded-claim recovery carryover without folding it into the
   native product scope.

The former tri-platform capstone, Linux production, and tri-platform acceptance
tasks are dropped and linked to the consolidated macOS task for history.

## Resume order

1. **Native tester build:** active. `scripts/build-native-cerebrum-macos.sh`
   produces the unsigned SwiftUI/AppKit/Metal `.app` used for local validation.
   The older `scripts/build-native-macos-beta.sh` Tauri/DMG lane is a superseded
   prototype with the distinct `com.sage.cerebrum.prototype.beta` identity; it
   is not v12 native release evidence.
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

The linked residual host-wake task remains a separate pre-v12 carryover stream;
the task relationship is for durable coordination, not product-scope expansion.

## Completion rule

v12 may claim the native-product capstone only when macOS passes the
frozen capability, security, distribution, recovery, accessibility, offline,
performance, and human-usability matrices. Browser CEREBRUM is the supported
Linux and Windows product path; no native Linux or Windows application is
required for v12.
