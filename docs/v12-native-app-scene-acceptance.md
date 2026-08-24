# v12 native app-scene acceptance

**Status:** expanded v2 DEBUG fixture green locally; the previous CI gate is green, this increment's CI run and release acceptance remain open.

This gate launches the packaged SwiftUI/AppKit executable rather than hosting a
view in the test runner. It therefore exercises the application's actual scene,
`NSApplication.mainMenu`, window, toolbar, and responder chain without requiring
macOS Accessibility permission.

Run it from the repository root:

```bash
bash scripts/v12-native-app-scene-acceptance.sh
```

The harness builds a PID-isolated DEBUG `com.sage.cerebrum.beta` app with `DesignPreviewAPI`,
captures the exact window through an in-scene `NSViewRepresentable`, and then:

1. inventories at most 256 concrete rendered menu items;
2. resolves exactly one **View > Focus Search** item by parent, label, key
   equivalent, and modifier mask after menu validation;
3. dispatches its real AppKit target/action;
4. requires one request and one consumption before the exact mounted
   `NSSearchToolbarItem.searchField.currentEditor()` becomes the captured
   window's first responder; and
5. moves focus to the uniquely identified mounted Search results `NSTableView`
   and repeats the same Focus Search proof against the same mounted search field;
6. uses a DEBUG-only bridge to activate the production inspect path for a
   deterministic preview memory and requires the native inspector close button
   to become first responder;
7. resolves and dispatches the rendered **View > Hide Inspector** item, then
   proves that the inspector presentation hides without clearing the inspected memory and
   that the exact same mounted results table regains first responder; and
8. resolves and dispatches the replacement **View > Show Inspector** item, then
   proves that the same memory remains inspected and the native close
   button regains first responder.

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

This is real app-scene and in-process AppKit evidence. It proves concrete menu
materialization, direct target/action dispatch, route/request effects, mounted
toolbar/table/control identity, semantic inspector preservation, and local
first-responder ownership. It deliberately records
`system_ax_server=false`, `voiceover_spoken_evidence=false`, and
`keyboard_event_routing=false`.

It does **not** prove physical keyboard event delivery, system-wide AX discovery or focus,
TCC behavior, VoiceOver navigation/reading/announcements, installed-candidate
behavior, or rendered commands outside the covered Focus Search and Search
Show/Hide Inspector lifecycle. Those stay in the named-Mac and RC acceptance
backlog.
