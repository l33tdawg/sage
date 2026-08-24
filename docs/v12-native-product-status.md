# v12 native product status

**Status date:** 2026-08-24

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
experience. Those implemented routes now let the native titlebar own the route
title and use a compact adaptive context/status bar instead of duplicating web
page-title chrome. General titles and metrics use standard SF Pro; rounded type
is restricted to the CEREBRUM mark and the Overview hero accent. Overview
consumes five independent feeds—health, stats, agents,
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

Search inspector identity is now independent from presentation and bulk
selection, matching browser CEREBRUM's separate expanded-detail and selection
states while retaining a native trailing inspector. Explicit Inspect or row
activation opens one memory; ordinary multiselection does not retarget it.
Hiding through the inspector, toolbar, or route-aware View command preserves the
memory and selection, while Escape clears both and requests results-table focus.
The same action path publishes stable focus/AX targets and concise announcements.
Commands fail closed during the filter popover, bulk-tag sheet, Forget dialog,
and active mutations. Repeated Focus Search requests now re-present the native
toolbar search field before one-shot consumption. Reducer/source evidence covers
these transitions, and real app-scene evidence covers the rendered Focus Search
item plus repeated mounted field-editor delivery, production inspect activation,
rendered Hide/Show Inspector dispatch, semantic identity preservation, and exact
results-table/close-button focus return. The expanded v2 gate is green. The v3
synthetic application-keyboard fixture is packaged and CI green. Physical
keyboard/HID and WindowServer routing, system
AX, real VoiceOver, localization, non-US layouts, installed-RC behavior, and
release acceptance remain open.

Brain now separates native Memory and Connectome modes from the independent MRI
and Accessible Table presentations. Memory reads
`/v1/dashboard/memory/graph`; Connectome reads
`/v1/dashboard/network/synapses`. Both modes render shared selection state
through an interactive AppKit `MTKView` with Metal shaders and a synchronized
SwiftUI table. If native renderer initialization fails, the presentation policy
immediately switches to that same table without clearing graph, selection, or
inspector state, announces the change to assistive technology, and offers a
bounded explicit **Try MRI Again** action without an automatic retry loop. The
MRI now consumes the same anatomical `brain.obj` hull as
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
disables automatic rotation and flow animation while preserving manual orbit
and zoom. Brain keeps mode, presentation, inspector, View Options, and Refresh in its primary
toolbar. View Options groups automatic rotation, flow animation, shell
visibility, and reset to Whole Brain. Hiding the Brain inspector preserves the
semantic selection; Escape remains the explicit clear-selection and focus-return
command. Native inspector dismissal now requests focus on the active Brain route
surface, and compact-navigator/View Options popovers gate global commands while
presented. Brain mode and View Options shortcuts now use Control-Command rather
than VoiceOver's Control-Option modifier space. The source is wired so the
standard macOS View menu owns one routed Refresh command and a ready-gated Focus
Search command that navigates to Search and requests presentation of its native
search field. Brain contributes route-scoped mode, presentation, inspector,
clear-selection, and View Options commands through a focused scene value, while
Help owns the accessible keyboard-shortcut reference.
The separate Navigate menu now checkmarks the active Overview, Brain, or Search
route. The typed source catalog, ready/exact-route/modal routing, one-shot search
request policy, and duplicate Command-R guard are covered. The green v2
app-scene gate inventories `NSApplication.mainMenu`, covers rendered Focus
Search and Search inspector Hide/Show, and proves the existing Search lifecycle.
The locally implemented v3 fixture additionally dispatches rendered **Navigate
> Brain**, then routes synthetic Command-3, Command-F, and Control-Command-I
keyDown/keyUp `NSEvent` pairs through `NSApplication.sendEvent`. Its local
keyDown monitor and exact checked-route, request/consumption, and responder
effects prove application-level routing in a green packaged local and CI run.
The packaged-local-green successor uses schema `sage.v12.native-app-scene.v4` and
scenario
`rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle`.
Its fail-closed contract adds exact backing `NSTableView` identity for both
Brain tables, deterministic `g1` selection, production **List View** reducer
dispatch through a focus-incapable DEBUG action bridge, production app-owned
resizable `HSplitView` inspector dispatch through that bridge, exact responder
ownership by the real AppKit inspector-close `NSButton`, `NSButton.performClick`
dismissal with zero remaining close controls and truthful bridge state, and restoration
to the exact currently mounted table in the same window without losing class,
rows, identifier, or selection before the Search sequence. Backing-object
reuse/replacement is recorded. The packaged local and CI v4 gates are green;
focused Brain View command routing remains open. This is synthetic in-process evidence, not physical
keyboard/HID, WindowServer, system AX, VoiceOver spoken output, installed-RC, localization, or
non-US-layout proof. Brain phase three is implemented with separate agent,
engram, and directed-connection focus, collision-safe scene identities, reserved
overlay budgets, and linear traffic summaries. The renderer now adds
time-invariant half-resolution
offscreen extraction, separable blur, and additive bloom; retained traffic
drives p95-normalized screen-space ribbon width; and selected cells receive a
reduced-motion-aware camera-focus ease. Reciprocal synapses now occupy deterministic curved lanes,
self-synapses render as finite loops, `last_fired` drives a truthful 30-minute
plasticity decay with a visible floor, and direct Metal ribbon picks synchronize
through the same directed connection identity as the accessible inspector. A
topology-aware deterministic cap bounds expanded ribbon geometry at 2,048,
retains the focused connection, reserves the selected agent's directional
neighborhood, and preserves useful per-neuron coverage. Ribbons now trim at
rendered neuron surfaces, terminate in direction arrowheads, and give a focused
connection reduced-motion-aware emphasis. One bounded GPU particle per rendered
edge now samples the same trimmed curve/self-loop function as its ribbon, with
no per-frame CPU geometry rebuild. The Brain also implements coalesced native
selection announcements and source-level focus targets for the summarized Metal
surface, synchronized table, inspector close control, and independently typed
related-memory cards. Related focus preserves its primary anchor, reconciles
against refreshed payloads, and clears before the anchor on Escape. A hosted
AppKit test now proves the production Metal surface can join a hosted native
responder chain. The production focus target now sits on the concrete Metal
representable rather than its decorative container; native identifiers and a
generation-fenced AppKit handoff prove failed retry returns first responder to
the retry button and successful restoration returns it to the mounted Metal
surface. A hardware runtime test now compiles the required Metal pipeline
family, renders the shared production scene encoder into a 4× MSAA offscreen
target, resolves it, executes the bloom chain, waits for successful GPU
completion, and verifies relative scene and bloom pixel changes without relying
on cross-GPU golden hashes. A pure recovery reducer now fences stale renderer
and retry completions by attempt, cancels held retry work on mode or view
invalidation, keeps keyboard and accessibility focus ownership independent, rejects MRI picker
bypass while unavailable, and drives retry through a separately injectable
asynchronous renderer seam while initial mount remains synchronous. A real
`NSHostingView`/`NSWindow` test proves the production fallback notice, retry
control, and synchronized memory table replace the failed MRI together without
clearing selection or duplicating the bounded announcement. The retry control is
now a native AppKit button with an explicit identifier, label, help, disabled
state, and in-progress value. Hosted acceptance activates its accessibility
press action, proves a failed retry leaves the table and selection mounted, then
uses a real prepared Metal renderer to prove recovery replaces the table while
preserving selection. A held-retry contract now proves the native button exposes
an explicit Button role, disabled state, “Trying MRI” label, and “In progress”
value; rejects a duplicate accessibility press; and leaves the fallback table
and selection mounted. Switching to Connectome cancels that task, restores the
enabled retry control beside the Connectome table, and releases a delayed stale
success without mounting either MRI or announcing restoration. Restoration
focus delivery and its bounded announcement are withheld until both the MRI
surface has mounted and that attempt reports renderer availability;
duplicate consumption of the one-shot handoff fails closed. The required macOS
CI lane explicitly enables the separate hardware probe. Brain now also resolves
a pure layout plan from its actual routed content size: below the expanded tier,
the fixed navigator and wide segmented controls become native toolbar menus,
headers and notices use fit-driven stacked variants, inspector dismissal keeps
semantic selection, and Train of Thought minimums stay inside the available
vertical budget. A hosted 620×540-to-expanded resize contract proves the
fallback table, retry control, navigator transition, selected memory, and
related-memory payload survive that presentation-only change. The hosted control
tests do not establish discovery through the system AX server. Real VoiceOver
evidence, system-AX discovery, table/inspector/related-card focus-return evidence,
cross-GPU/offline behavior, and deeper behavioral, accessibility, large-store,
and performance evidence remain open.

The named-Mac system-AX gate now has a direct `AXUIElement` harness and a
DEBUG-only deterministic failure/restoration fixture. It validates the target
process and application identity, pages a bounded children-only AX traversal,
performs one external retry press, and requires exact application/system focus
equality for the retry or Metal surface. The fixture is fail-closed outside
design preview and release CI rejects all fixture markers. See
[`v12-native-system-ax-acceptance.md`](v12-native-system-ax-acceptance.md).
Execution evidence is still open because the probe does not yet have macOS
Accessibility trust on this named Mac; audible VoiceOver remains a separate
operator gate.

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
loading/degraded labels. Overview, Search, and Brain now present fetched snapshot
quality separately from typed SSE transport state. Snapshot labels distinguish
loading, updated age, partial projection, cached refresh failure, and unavailable
data; transport labels only report event updates connecting, connected,
reconnecting, or terminally stopped after session authorization ends. Normal
stream EOF and transient errors reconnect with capped
backoff, authorization termination purges sensitive state, and a terminated
stream cancels its scheduled-refresh sibling. The 30-second recovery refresh no
longer fabricates an “Update available” claim; that independent,
selection-preserving signal is raised only by relevant SSE invalidation.
Search timestamps are keyed to the exact query/filter scope and Brain timestamps
to mode/domain/status, so an older result cannot appear current for a newly
selected scope.
The typed view-model transitions, scope isolation, partiality, and authorization
termination are unit-tested. Real URLSession EOF/error/backoff timing and
cancellation remain explicit promotion evidence rather than completed runtime
acceptance.

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
| Brain, Connectome, and memory detail | SwiftUI/AppKit surface with separate Memory/Connectome modes, responsive route-width policy and native compact navigator/toolbar, fit-driven headers/notices, selection-preserving inspector presentation, shared anatomical CEREBRUM hull, custom Metal MRI with time-invariant multi-pass bloom, luminous native cells, plastic weighted curved ribbons, self-loops, topology-aware bounded LOD, trimmed direction arrowheads, direct edge picking, shared-path GPU flow particles, coalesced selection announcements, native AppKit retry/Metal focus return, synchronized native tables, accessibility-pressable native retry with held-progress state, cancellation/fencing and mount-gated restoration, memory/agent inspectors, selected-agent engram bloom, directed-connection focus, independently typed related-memory Train of Thought, and hardware offscreen GPU/bloom raster evidence | `native-control` | Brain interaction/parity, hosted narrow-window transition, immediate retry failure, held stale-success cancellation, successful restoration, and retry/Metal first-responder delivery implemented; real VoiceOver/system-AX plus table/inspector/related focus proof, cross-GPU/offline behavior, daemon lifecycle, large-store tuning, and deeper behavioral/accessibility/performance evidence remain open |
| Search, filtering, tags, transfer, and forget | SwiftUI table, native filters, memory inspector, tag mutation and governed Forget flows backed by typed dashboard APIs | `native-control` | Search/filter/select/inspect/load-more, tag editing, bulk tagging and safe single/bulk Forget implemented; real app-scene Focus Search and Search inspector Show/Hide dispatch, identity preservation, and exact table/close focus return proven; whole-domain transfer, physical keyboard, system AX/VoiceOver, and release acceptance remain open |
| Tasks and agent Messages | Native destination reserved | `native-control` target | Implementation open |
| Imports and backup restoration | Native destination reserved | `native-control` target | Implementation open |
| Agents, keys, RBAC, and governance | Native destination reserved | `native-control` target | Implementation open |
| Access Controls | Native destination reserved | `native-control` target | Implementation open |
| Federation and Sharing & Sync | Native destination reserved | `native-control` target | Implementation open |
| Settings, security, recall, maintenance, and updates | Native destination reserved | `native-control` target | Implementation open |

Current count: **9/9 native destinations mapped; 3/9 have implemented vertical
slices; 0/9 has complete route/action acceptance evidence**.

The shell now presents the six unimplemented destinations in a separate,
non-selectable **Coming Soon** section instead of making them look equivalent
to working routes. Only Overview, Brain, and Search own fixed navigation
shortcuts (`Command-1` through `Command-3`), navigation and Lock commands are
disabled outside a ready session, and `Command-,` no longer opens placeholder
Settings. Navigate now checkmarks exactly the active implemented route. Brain
mode and View Options use Control-Command rather than the VoiceOver
Control-Option modifier chord, Brain popovers gate global commands, and native
Brain inspector dismissal requests route focus. These are locally implemented
product-honesty and keyboard-safety improvements, not route completion or
completed acceptance.

Overview, Search/Inspector, and the Brain Memory/Connectome workflow are real native controls;
they do not load the browser SPA or a WebView. The parity ledger remains open
until every route/action has an acceptance row and the native application owns
daemon lifecycle, recovery, updates, and rollback.

## Platform evidence

| Platform | Evidence already present | Production blockers |
|---|---|---|
| macOS | Launch-tested unsigned Apple Silicon Swift application shell with native unlock and daemon attachment; source-built native Overview, Search/Inspector, and Brain Memory/Connectome slices; previous v2 packaged/CI app-scene gate green; v3 checkmarked navigation, Brain command chords that avoid VoiceOver's default Control-Option chord, popover/focus cleanup, and synthetic `NSApplication.sendEvent` routing packaged and CI green; v4 exact current Brain/Search backing-table and inspector responder lifecycle packaged and CI green across SwiftUI backing replacement; hosted native responder and hardware Metal evidence; builder identity `com.sage.cerebrum.beta`; no WebKit or JavaScriptCore linkage | Physical keyboard/HID and WindowServer routing, localization/non-US-layout evidence, focused Brain View and remaining rendered commands, named-Mac system AX/VoiceOver spoken and focus evidence, bundled-daemon lifecycle, native recovery/update/rollback, remaining Brain polish/evidence, complete route parity, installed release acceptance, Developer ID/notarization, Gatekeeper clean-machine launch, architecture matrix, offline/accessibility and three-run evidence |
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
- Complete the remaining Brain work: prove real VoiceOver/system-AX discovery
  and table/inspector/related-card focus return, Metal/Table parity,
  access-purge behavior, broader keyboard operation,
  cross-GPU/offline behavior, and large-store performance.
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

The active program is consolidated into three non-overlapping program lanes,
with one current native execution child:

1. `f3291de2-d270-4c3b-b81b-3f29bc54b83b` — deliver native macOS CEREBRUM on
   `v12-beta`, from the first tester build through production acceptance;
2. `867fa87f-14b0-45ca-a11c-10dc48746257` — completed native child that delivered
   the expanded Search inspector app-scene lifecycle gate; and
3. `b945c6bb-8d06-45c7-91d1-ad1e15e6b84d` — completed native child that delivered
   packaged-and-CI-green v4 exact current Brain/Search table and inspector focus restoration; and
4. `03aebcf3-18e9-4d60-bddc-71c1b6e5ffdc` — current consolidated native execution child for
   focused Brain command routing, Connectome focus, remaining rendered commands,
   physical-keyboard routing, system-AX/VoiceOver,
   reflow/localization/contrast, transport timing, performance, cross-GPU,
   offline, daemon lifecycle, route parity, and release acceptance;
5. `26dee44b-4f7f-459d-a76b-91f0c2d7cdd4` — preserve and prove the separate
   headless Commons/Lantern contract; and
6. `b6d9bade-92fa-4d74-a8c4-9b0cc35d280d` — finish the pre-v12 automatic
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
