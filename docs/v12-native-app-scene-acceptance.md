# v12 native app-scene acceptance

**Status:** v3 packaged DEBUG fixture green locally; CI for this increment and
release acceptance remain open.

**Current durable task:** `b945c6bb-8d06-45c7-91d1-ad1e15e6b84d`

This gate launches the packaged SwiftUI/AppKit executable rather than hosting a
view in the test runner. It therefore exercises the application's actual scene,
`NSApplication.mainMenu`, window, toolbar, and responder chain without requiring
macOS Accessibility permission.

Run it from the repository root:

```bash
bash scripts/v12-native-app-scene-acceptance.sh
```

The harness builds a PID-isolated DEBUG `com.sage.cerebrum.beta` app with
`DesignPreviewAPI`, captures the exact window through an in-scene
`NSViewRepresentable`, and then:

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

The app writes one bounded JSON result to standard output and exits nonzero on
any assertion or timeout. The shell applies a separate 30-second deadline,
binds the result to the exact commit and a clean/dirty source-snapshot hash,
validates the result and evidence boundary, cleans up only the captured PID after
checking its executable path, and records the result, app log, manifest, and
SHA-256 hashes. CI uploads these diagnostics even when a later validation step
fails. Release scanning rejects the DEBUG app-scene fixture, Search bridge, and
acceptance-environment markers if they leak into the release executable. The
AppKit menu coordinator is production code and remains in release builds.

## Evidence boundary

This is real app-scene and in-process AppKit evidence. The green local v3
packaged run proves concrete menu materialization, direct rendered
target/action dispatch, synthetic application keyboard-event routing through
`NSApplication.sendEvent`, exact route/request effects, mounted
toolbar/table/control identity, semantic inspector preservation, and local
first-responder ownership. The v3 result distinguishes
`application_keyboard_event_routing=true` and `synthetic_keyboard_events=true`
from `physical_keyboard_event_routing=false`; it also records
`system_ax_server=false` and `voiceover_spoken_evidence=false`. Those fields are
not interchangeable.

It does **not** prove physical keyboard or HID delivery, WindowServer event
routing, system-wide AX discovery or focus, TCC behavior, VoiceOver
navigation/reading/announcements, an installed release candidate, localization,
or non-US keyboard-layout behavior. Commands and environments outside the
bounded Navigate-to-Brain, Command-3 Search, Command-F Focus Search, rendered
Search lifecycle, and Control-Command-I Show Inspector scenario stay in the
named-Mac and RC acceptance backlog.
