# v12 native app-scene acceptance

**Status, 2026-10-05:** the packaged v5 gate passed on the GitHub-hosted
macOS runner in [run 37300998981](https://github.com/l33tdawg/sage/actions/runs/37300998981)
for PR head `8c6f5ef0e7a470ea865282972e01d155105360a0`. The tested PR merge
checkout was `11d9c92ae218aef2825d2f82e427a75c9fc55c4d`; both commits have tree
`a0cf8418db94054ec0a1b709d91475fd8eb616b0`. Result
`20261005T111034Z-app-scene-3372` passed all 21 assertions in 12.416 seconds
on arm64 macOS 15.7.9. The same CI run passed the Swift suite, required hardware
Metal checks, release build, and release-binary fixture/linkage scan.

This is packaged app-scene and synthetic in-process keyboard evidence.
The result explicitly records `physical_keyboard_event_routing=false`,
`system_ax_server=false`, and `voiceover_spoken_evidence=false`. Final external
system-AX qualification still awaits an unlocked session on the named Mac;
physical keyboard, VoiceOver, and installed-release acceptance remain open.

This gate launches the packaged SwiftUI/AppKit executable rather than hosting a
view in the test runner. It therefore exercises the application's actual scene,
`NSApplication.mainMenu`, window, toolbar, and responder chain without requiring
macOS Accessibility permission.

Run it from the repository root:

```bash
bash scripts/v12-native-app-scene-acceptance.sh
```

The established v3 harness builds a PID-isolated DEBUG
`com.sage.cerebrum.beta` app with `DesignPreviewAPI`, captures the exact window
through an in-scene `NSViewRepresentable`, and then:

1. inventories at most 256 concrete rendered menu items;
2. requires exactly one checkmarked Overview/Brain/Search item in the rendered
   **Navigate** menu, resolves **Navigate > Brain**, and dispatches its real
   AppKit target/action from the initial Overview route;
3. constructs a synthetic Command-3 keyDown/keyUp pair and routes both events
   through `NSApplication.sendEvent`, while a local keyDown monitor and exact
   checked-menu/route transition prove application-level routing to Search;
4. resolves exactly one **View > Focus Search** item by parent, label, key
   equivalent, and modifier mask after menu validation, then sends a synthetic
   Command-F keyDown/keyUp pair through `NSApplication.sendEvent`;
5. requires the local monitor to observe exactly one matching keyDown, one
   request and one consumption before the exact mounted
   `NSSearchToolbarItem.searchField.currentEditor()` becomes the captured
   window's first responder;
6. moves focus to the uniquely identified mounted Search results `NSTableView`
   and repeats the same Focus Search proof against the same mounted search field;
7. uses a DEBUG-only bridge to activate the production inspect path for a
   deterministic preview memory and requires the native inspector close button
   to become first responder;
8. resolves and dispatches the rendered **View > Hide Inspector** item, then
   proves that the inspector presentation hides without clearing the inspected memory and
   that the exact same mounted results table regains first responder; and
9. resolves the replacement **View > Show Inspector** item, sends a synthetic
   Control-Command-I keyDown/keyUp pair through `NSApplication.sendEvent`, and
   requires one locally monitored keyDown, one inspector request/consumption,
   preserved memory identity, and exact native close-button focus.

The current schema is `sage.v12.native-app-scene.v5`, using scenario
`rendered-menu-application-keyboard-brain-search-inspector-focus-lifecycle`.
It retains the Search assertions and adds a fail-closed Brain lifecycle:

1. after **Navigate > Brain**, require eight rendered View commands, their
   exact shortcuts, enabled states and checkmarks;
2. dispatch **Brain Mode > Agent Network** through its rendered target/action,
   then send Control-Command-1 through `NSApplication.sendEvent` to return to
   Memory Map, checking the mounted command state after each action;
3. prepare deterministic memory `g1` through a DEBUG selection-only bridge that
   cannot invoke presentation, inspector or native-focus actions;
4. dispatch **Brain Presentation > List View** through its rendered menu item
   and require the uniquely identified backing `NSTableView` to own the
   captured window's first responder with `g1` selected. Then dispatch
   **Interactive Map** and return to List View with Control-Command-L,
   verifying the presentation change and exact selected-table focus;
5. dispatch **Show Inspector** through its rendered menu item and require the
   exact app-owned inspector-close `NSButton` to become first responder;
6. dismiss through that button and verify selection preservation, zero mounted
   close controls, and exact current table focus. SwiftUI may reuse or replace
   its backing table; both cases remain bound to the observed object identity;
7. reopen and hide the Brain inspector with Control-Command-I keyboard events,
   requiring one locally observed keyDown per command and exact close/table
   focus with selection preserved; and
8. open the real **Help > Keyboard Shortcuts** sheet. Require all three Navigate
   commands and all eight Brain commands to be disabled, attempt a Brain shortcut through the
   sheet's key window and a retained menu target/action, and verify unchanged
   Brain state and owner with no queued request. Escape dismisses the sheet and
   the commands must return.

The producer records actual mounted Brain mode and presentation values. It
never substitutes constant state for an observation. The strict validator and
mutation tests reject absent, duplicate or reordered commands; changed shortcuts;
DEBUG action bypasses; wrong responders; unconsumed requests; and weakened modal
or evidence boundaries. v4 remains historical evidence of the prior lifecycle,
which reached presentation and inspector actions through a DEBUG bridge and
could not establish rendered Brain menu routing.

The app writes one bounded JSON result to standard output and exits nonzero on
any assertion or timeout. The shell applies a separate 60-second deadline,
binds the result to the exact commit and a clean/dirty source fingerprint that
includes HEAD, its tree, tracked changes and untracked files. It checks that
fingerprint after the build and again after the runtime, before accepting any
result, so even a clean same-tree commit change rejects the run. It then
validates the result and evidence boundary, cleans up only the captured PID after
checking its executable path, and records the result, app log, manifest, and
SHA-256 hashes. CI uploads these diagnostics even when a later validation step
fails. Release scanning rejects the DEBUG app-scene fixture, Search bridge, and
acceptance-environment and v4/v5 schema markers if they leak into the release executable. The
AppKit menu coordinator is production code and remains in release builds.

## Evidence boundary

This is real app-scene and in-process AppKit evidence. The v5 gate verifies
concrete menu materialization, direct rendered target/action dispatch,
synthetic application keyboard-event routing through `NSApplication.sendEvent`,
exact route/request effects, mounted toolbar/table/control identity, semantic
inspector preservation, modal guards, and local first-responder ownership.
The result distinguishes
`application_keyboard_event_routing=true` and `synthetic_keyboard_events=true`
from `physical_keyboard_event_routing=false`; it also records
`system_ax_server=false` and `voiceover_spoken_evidence=false`. Those fields are
not interchangeable.

It does **not** prove physical keyboard or HID delivery, WindowServer event
routing, system-wide AX discovery or focus, TCC behavior, VoiceOver
navigation/reading/announcements, an installed release candidate, localization,
or non-US keyboard-layout behavior. Commands and environments outside the
bounded Brain and Search scenarios above stay in the named-Mac and release
candidate acceptance backlog.

The v5 gate adds rendered Brain menu, mode, inspector, modal-guard and exact
in-process AppKit first-responder evidence for the bounded lifecycle above. It still will
not prove physical HID or WindowServer delivery, system AX focus, VoiceOver
spoken output, localization, or non-US keyboard layouts; all remain open.

The macOS workflow runs this gate for pull requests targeting `v12-beta`, before
merge, as well as pushes to that branch. Permissions remain read-only and only
diagnostic evidence is uploaded; the unsigned application is not distributed.
The package resource check accepts both macOS `Contents/Resources/brain.obj`
and the flat SwiftPM bundle layout used by earlier supported toolchains.
