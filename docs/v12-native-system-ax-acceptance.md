# v12 native macOS system accessibility acceptance

**Status:** executable harness implemented; this Mac still requires an explicit
Accessibility grant before system-AX scenarios can run. Spoken VoiceOver
acceptance remains a separate operator gate.

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

## Evidence boundary

A passing JSON document proves discovery, activation, state transition, and
keyboard-focus delivery through the macOS system accessibility server. It does
not prove the VoiceOver cursor moved or that speech was audible. Real VoiceOver
acceptance must therefore run the same flow with VoiceOver enabled and retain
the operator, OS/build identity, spoken announcement result, and audiovisual
artifact required by the v12 acceptance ledger.
