# v12 native macOS system accessibility acceptance

**Status, 2026-10-05:** the external probe reports Accessibility trust on the
current named Mac, and the compiler/preflight and result-validator mutation
checks pass. The **final AppKit implementation has no completed system-AX
runtime pass yet**: the Mac was locked during qualification, preventing native
window activation. A source-bound runtime result after manual unlock is
required. Spoken VoiceOver and physical keyboard acceptance remain separate
operator gates.

An earlier intermediate implementation reached external Navigate and List View
menu actions, exact application/system focus on the row-bearing Memory
`AXOutline`, and selection of the known preview row through an injected Down
arrow. That partial observation is neither a complete scenario pass nor
qualification of the final implementation. No focus, active-window, or
accessibility gate is waived because the desktop is locked.

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
agent, and repeats inspector focus/return checks against the Connectome table.

A logged-in, unlocked graphical session is required; Accessibility trust alone
does not establish that the desktop can activate a window. Unlocking is a manual
operator action. The harness does not attempt to unlock the Mac.

Keyboard injection requires the target PID to own both foreground application
and system focus immediately before each event. The probe never sets AX focus,
changes the application's first responder, or calls an in-process acceptance
bridge. Menu actions are issued once; bounded observation waits establish their
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
focus, missing rows, the wrong menu path, unproven physical keyboard or VoiceOver
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
