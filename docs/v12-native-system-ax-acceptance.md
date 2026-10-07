# v12 native macOS system accessibility acceptance

**Status, 2026-10-05:** the bounded `brain-menu-focus` scenario has a
completed external system-AX runtime pass on the unlocked Apple M5 Max Mac
(arm64, macOS 27.0.1 build 26A434). Result
`20261005T125830Z-brain-menu-focus-15034` completed in seven seconds with
Accessibility trust, all seven rendered menu paths, Memory and Agent Network
selection preservation, and exact application/system focus on the actual
row-bearing tables and inspector-close button. Down-arrow and
Control-Command-I delivery used synthetic WindowServer events. Physical
keyboard and spoken VoiceOver acceptance remain separate operator gates.

This candidate run is bound to base commit
`984427cc46209a35db708b2fd062e322980735b5`, tree
`1cef3a055624475c71db791dc06f1f5b8f95daf0`, and dirty-source snapshot
`6be562b3fc450d87dce083e6b814a45dcf6531e35d8459ac0878d024f460de08`.
The result SHA-256 is
`23124811b7c9c8016fae6520457505f387a9aac6abe0ffd24e12435916000e33`.
It qualifies that candidate, not another branch head. Review the immutable
clean-head qualification and CI evidence recorded in
[PR #417](https://github.com/l33tdawg/sage/pull/417) before accepting a later
revision. Checksummed result, probe/app logs, and host manifest remain local;
operator evidence is not uploaded to the public repository.

The pass exposed and fixed two production menu lifecycle gaps: SwiftUI can
replace the main menu, and it can rebuild View/Navigate items during
`menuNeedsUpdate`. The coordinator observes replacement and restores current
route-owned commands after the upstream delegate updates, preserving its
optional callbacks and weak lifetime. It does not repair the menu through the
DEBUG acceptance fixture. Focus observations use the concrete mounted AppKit
first responder when SwiftUI's focus binding is nil; they never request or
manufacture focus. Compiler/preflight and strict result-validator mutation
checks also pass. Retry/Metal restoration, other command paths, installed
release, physical keyboard, and VoiceOver retain their own acceptance scope.

This gate targets the SwiftUI/AppKit/Metal application with bundle identifier
`com.sage.cerebrum.beta`. It does not target the historical Tauri shell and it
does not use System Events or Apple Events.

## Trust preflight

Build the probe at its stable path and check its current TCC status without
opening a prompt:

```bash
scripts/v12-native-system-ax.sh --preflight
```

Exit `77` means the probe is not trusted. To ask macOS to show the Accessibility
permission prompt, run the following once, grant access to the exact probe shown
by the command, and then relaunch the command. The prompt is asynchronous, so
the prompting invocation still exits `77` when it was not already trusted.

```bash
scripts/v12-native-system-ax.sh --preflight --prompt
```

The stable client executable is
`dist/v12-native/ax-tools/v12-native-system-ax`. Only this external probe needs
Accessibility permission; CEREBRUM itself requires no accessibility
entitlement.

## Scenarios

Run both deterministic DEBUG-only scenarios from a logged-in named Mac:

```bash
scripts/v12-native-system-ax.sh \
  --scenario retry-fail \
  --evidence dist/v12-native/ax-evidence

scripts/v12-native-system-ax.sh \
  --scenario retry-restore \
  --evidence dist/v12-native/ax-evidence
```

The launcher builds a DEBUG fixture, launches only the captured application
PID, and cleans up only when that PID still resolves to the expected executable.
The fixture cannot activate without `SAGE_NATIVE_DESIGN_PREVIEW=1`, accepts only
bounded retry delays, and is compiled out of release builds.

The direct `AXUIElement` probe:

- validates the target PID, bundle identifier, `AXApplication` role, and window;
- performs a single external `AXPress` on `brain-metal-retry`;
- observes the disabled “Trying MRI” / “In progress” transition;
- uses bounded, paged, children-only AX traversal;
- proves final focus by exact AX element equality in both the application and
  system-wide focused-element attributes; and
- emits bounded JSON plus a SHA-256 sidecar without dumping memory contents or
  the complete accessibility tree.

The failure scenario must return focus to the re-enabled retry button. The
restoration scenario must focus the concrete native Metal surface only after it
mounts.

The manual `v12 named-Mac system AX acceptance` workflow is restricted to the
protected `v12-beta` branch and a dedicated `[self-hosted, macOS, sage-v12-ax]`
runner with a logged-in Aqua session. It serializes runs without cancellation
and writes evidence to the protected runner-local `SAGE_AX_EVIDENCE_ROOT`; it
does not upload operator or screen evidence to this public repository.

## Brain menu, keyboard, and focus scenario

```bash
scripts/v12-native-system-ax.sh \
  --scenario brain-menu-focus \
  --evidence dist/v12-native/ax-evidence
```

This scenario launches only `DesignPreviewAPI` data on Overview. It uses the
system AX menu bar to navigate to Brain and select **View > Brain Presentation
> List View**. It requires the actual row-bearing `AXTable` or `AXOutline` with
the Brain table identifier; an identifier-bearing wrapper cannot satisfy the
check. Both the application's and the system's focused elements must equal that
exact object, and its focused attribute must be true.

From that observed focus, the external probe injects a Down-arrow event through
the WindowServer session event tap, selects the known preview memory, and uses
rendered AX menu actions to hide and reopen the inspector. It checks that
selection survives, that hiding removes the close control, that showing focuses
the actual inspector-close `AXButton`, and that pressing that button returns
focus to the currently mounted row-bearing table. It also injects
Control-Command-I and requires the same inspector focus effect. Finally it
switches to Agent Network through the rendered menu, selects the known preview
agent **Claude** (the first row after production agent-ID sorting), and repeats
inspector focus/return checks against the Connectome table.

A logged-in, unlocked graphical session is required; Accessibility trust alone
does not establish that the desktop can activate a window. Unlocking is a manual
operator action. The harness does not attempt to unlock the Mac.

Keyboard injection requires the target PID to own both foreground application
and system focus immediately before each event. The probe never sets AX focus,
changes the application's first responder, or calls an in-process acceptance
bridge. Menu lookup reacquires the full path from the current application menu bar
at every observation; it does not retain parent handles across an `AXPress`.
Control discovery skips table rows, while selected-row text is checked only
under the exact selected `AXRow`. Every AX RPC has the same bounded timeout
within this probe process, and the scenario retains its 45-second deadline.
Menu actions are issued once; bounded observation waits establish their
effects before the next assertion. An `AXError.cannotComplete` response is not
retried because a menu may already have entered tracking.

The result uses `sage.v12.native-system-ax.brain.v1`. It records the exact menu
paths, synthetic keyboard sequence, row-bearing table roles/counts, preserved
selection, and application/system focus equality. The launcher validates that
result, records probe diagnostics, hashes the executable and evidence, and
binds the result to the launched PID/version and rejects source changes during
probe compilation, app build, or execution. Its fingerprint includes the exact
Git commit and tree, so a clean checkout moving to another commit also fails. An isolated Swift scratch
directory prevents another build from replacing its executable. All app runs
must still be serialized because they share foreground system focus.

Run `bash scripts/v12-native-system-ax.test.sh` for compiler/preflight and
contract checks. The result validator rejects mutated evidence claiming wrapper
focus, missing rows, the wrong selected memory/agent or menu path, unproven physical keyboard or VoiceOver
results, or fixture-injected focus. It can also validate an existing result
without launching an app. Supply the captured `launched_pid` and
`requested_bundle_version` from that run's manifest:

```bash
scripts/v12-native-system-ax.sh --scenario brain-menu-focus \
  --validate-result /path/to/system-ax.json \
  --expected-pid 12345 --expected-version 12.0.0-beta.1
```

## Evidence boundary

A passing retry JSON document proves discovery, activation, state transition,
and keyboard-focus delivery through the macOS system accessibility server.
A passing Brain result adds the bounded rendered-menu lifecycle, retained
selection, and externally injected WindowServer keyboard effects described
above. These are synthetic events, not physical HID input. It does
not prove the VoiceOver cursor moved or that speech was audible. Real VoiceOver
acceptance must therefore run the same flow with VoiceOver enabled and retain
the operator, OS/build identity, spoken announcement result, and audiovisual
artifact required by the v12 acceptance ledger.
