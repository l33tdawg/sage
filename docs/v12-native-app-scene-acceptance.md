# v12 native app-scene acceptance

**Status:** DEBUG fixture implemented; CI and release acceptance remain open.

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
5. moves focus to a fixture-owned in-window responder and repeats the same proof
   against the same mounted search field.

The app writes one bounded JSON result to standard output and exits nonzero on
any assertion or timeout. The shell applies a separate 30-second deadline,
binds the result to the exact commit and a clean/dirty source-snapshot hash,
validates the result and evidence boundary, cleans up only the captured PID after
checking its executable path, and records the result, app log, manifest, and
SHA-256 hashes. CI uploads these diagnostics even when a later validation step
fails. Release builds reject the fixture's type and environment markers.

## Evidence boundary

This is real app-scene and in-process AppKit evidence. It proves concrete menu
materialization, direct target/action dispatch, route/request effects, mounted
toolbar identity, and local first-responder ownership. It deliberately records
`system_ax_server=false`, `voiceover_spoken_evidence=false`, and
`keyboard_event_routing=false`.

It does **not** prove keyboard event delivery, system-wide AX discovery or focus,
TCC behavior, VoiceOver navigation/reading/announcements, installed-candidate
behavior, or any other rendered command. Those stay in the named-Mac and RC
acceptance backlog.
