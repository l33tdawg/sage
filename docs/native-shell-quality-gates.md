# Native-shell quality gates

These gates are release criteria, not aspirational telemetry. A native package
is not promoted on any platform without immutable CI evidence for that platform.
Browser CEREBRUM and the existing Go release matrix remain mandatory.

## Current enforcement status

The tracked preview now enforces locked dependency compilation, Rust
format/test/Clippy, full platform shell-control tests, isolated Codex endpoint
acceptance tests, dependency audit, a license-bearing CycloneDX SBOM, and
unsigned package construction on the declared macOS, Windows, and Linux
targets. Each constructed package is unpacked and must contain exactly one
bundled daemon whose embedded Go OS/architecture matches the declared target
and whose embedded version matches the version supplied to the shell package
build. Those checks emit a machine-readable release-pair record beside
the package with the target, build version, packaged shell artifact size/hash,
and bundled daemon path/size/hash. They establish the foundation; unsigned CI
artifacts and their records are not release evidence.

The tagged release workflow now enforces that distinction from v11.11 onward.
It version-locks the Tauri and Cargo metadata to the tag, constructs private
per-platform package-pair and SBOM evidence, and then fails closed at the named
native standalone production-promotion gate. Those unsigned preview artifacts
are deliberately excluded from public GitHub release staging. The hold may be
removed only when the signed installed-runtime, recovery, performance, and
accessibility evidence below is produced and validated in the same publication
dependency chain. By release-owner decision, this freezes the entire v11.11
publication graph—not only the native assets—so Docker, SDK, MCP, legacy
installers, and the GitHub release cannot get ahead of standalone readiness.
Recovery runs for tags older than v11.11 remain supported. The temporary hold
also fails closed for later versions until their shell/daemon compatibility and
promotion path deliberately replace it; it must not be carried unchanged into
v12.

GitHub Dependabot alert 37 (`RUSTSEC-2024-0429` /
`GHSA-wrw7-89jp-8q8g`) is also an active promotion blocker. The Linux Tauri
stack currently receives `glib` 0.18.5 through Wry/WebKitGTK; the affected safe
iterator API can trigger undefined behavior and optimized-build crashes. As of
this gate record, current Wry 0.55.1 still pins the affected GTK/glib family and
upstream issue `tauri-apps/wry#1769` remains open, so no compatible dependency
upgrade exists. The alert must remain open until a tested upstream migration or
reviewed backport removes the affected code; it must not be dismissed merely to
make release status green.

Runtime promotion remains open until the install/launch/deep-link/offline,
performance, assistive-technology, signing/notarization, update/rollback, and
uninstall-preservation rows below have immutable platform results. Windows
named-pipe reads and writes now use overlapped cancellable deadlines with native
stalled/partial-frame tests in the code gate.

All three platforms now run an installed-package lifecycle smoke on a hosted
runner. Each one installs from the constructed package, launches the installed
executable against an isolated `SAGE_HOME`, proves single-instance handoff
(a second launch exits cleanly and the instance generation is unchanged), proves
the daemon survives an ordinary shell close, uninstalls, confirms the node data
root is preserved byte-for-byte, and reinstalls to a genuinely new generation.
The macOS harness mounts the built DMG and installs by copying the app out of
it, and derives the serving daemon PID from the control socket with
`LOCAL_PEERPID` — the direct analogue of the Windows `GetNamedPipeServerProcessId`
check — so cleanup only ever signals an exact, socket-derived PID whose
executable path matches the bundled daemon. No harness matches a process by name.

These runs are *constrained* evidence and must not be read as more than they are:

- they are **unsigned**. They prove nothing about code signing, notarization,
  stapling, Gatekeeper, or SmartScreen, and they are not rollback or
  update-failure evidence;
- macOS runs **arm64 only** on the declared runner image, installs into an
  isolated root rather than `/Applications`, and does not exercise Intel, deep
  links, or LaunchServices registration;
- the Windows row remains limited to the runner image's edition and cannot
  complete the Windows 11 / arm64 / signing / SmartScreen matrix;
- none of them measure the performance budgets or the accessibility gates below.

The exact OS build, architecture, and WebKit/WebView version observed on each run
are recorded in the uploaded runtime diagnostics so the constraint above is
checkable rather than asserted.

### Offline startup (macOS)

macOS additionally runs an offline startup smoke. It launches the shell with
every proxy variable pointed at a dead loopback port, waits for the daemon to
report a renderable SSCP state, and holds a settle window after ready — proving
the app boots and becomes usable with no reachable external service.

Throughout, it **continuously samples** the internet sockets of every process
running the exact staged shell executable or the exact bundled daemon path, and
fails on any non-loopback endpoint. Sampling is continuous rather than a single
end-state check because a one-shot check cannot see a transient boot-time
request. Wildcard binds are reported as well as outbound connections. As
everywhere else, processes are matched on absolute executable path, never name.

Enforcement limit, stated plainly: macOS has no unprivileged per-process network
namespace, so this harness does **not** kernel-block egress. It proves *no
external socket was observed and startup did not require one* — not *egress was
impossible*. The Linux `unshare -n` approach would be strictly stronger, but
Linux is not a distributed platform for this release.

The detector is verified in both directions against a synthetic bundle: a
deliberate outbound connection is caught and reported with the offending socket,
and a realistic loopback-only topology passes. A run that observes zero sockets
is not by itself evidence.

## Supported matrix

| Platform | Build/install floor | Required package evidence |
|---|---|---|
| macOS | oldest Apple-supported macOS that SAGE declares for the release; Intel and Apple Silicon where distributed | signed `.app`, notarized/stapled DMG, clean install, Gatekeeper launch, rollback |
| Windows | Windows 11 x64 and arm64 where distributed | signed NSIS installer, clean install/uninstall, SmartScreen/signature check, rollback |
| Linux | Ubuntu 24.04 LTS x64 plus each architecture distributed | AppImage and Debian package, offline launch, uninstall preserving `~/.sage` |

The exact OS image identifiers, WebView versions, CPU/RAM, and artifact hashes
must appear in the release evidence. “Builds on a developer machine” is not a
platform pass.

## Automated hard gates

- exact Cargo lockfile; `cargo fmt --check`, `cargo test`, Clippy with warnings
  denied, dependency audit, license/SBOM generation;
- release shell and existing `sage-gui` build on every matrix target;
- package, install, launch, re-launch/focus, deep link, close/reopen, uninstall;
- SSCP malformed/oversized frame, wrong peer, stale socket/pipe, malicious port
  occupant, protocol skew, daemon crash/restart, and generation-change tests;
- navigation denial for `file:`, `data:`, `javascript:`, non-pinned loopback,
  remote HTTP, redirects, popups, and page-initiated native calls;
- offline startup with outbound DNS/HTTP blocked and zero external requests;
- browser fallback and daemon-only operation after native-shell removal;
- package signature, notarization, update, failed-update rollback, previous
  version recovery, and preservation of `~/.sage` on uninstall.

## Performance budgets

Report p50/p95 and raw samples on named baseline hardware. Separate shell cost
from daemon boot, model boot, consensus, and queries.

| Measure | Blocking budget |
|---|---:|
| Warm re-open to focused existing window | <= 500 ms p95 |
| Cold launch to bundled recovery paint | <= 1,000 ms p95 |
| Ready daemon to interactive CEREBRUM | <= 2,000 ms p95 |
| Daemon loss to visible recovery action | <= 2,000 ms |
| Settled shell idle CPU | <= 1% p95 |
| Incremental shell RSS, daemon excluded | <= 200 MiB p95 |
| Shell/navigation input response | <= 100 ms p95 |
| Native overhead over same browser action | <= 25 ms p95 |
| MRI frame pacing | >= 55 FPS median; no recurring >100 ms stalls |

Three consecutive benchmark runs must pass. A regression of more than 10%
against the last published release fails even when the absolute ceiling passes,
unless the release record accepts the tradeoff with evidence.

## Accessibility gates

Automated semantic checks supplement, never replace, the OS smoke matrix:

- all actions are keyboard-complete with visible focus and logical order;
- every control/window/state has an accessible name and status changes use an
  appropriate live region without stealing focus;
- 200% zoom, high contrast, light/dark mode, and OS reduced-motion work without
  clipping or lost content;
- VoiceOver (macOS), Narrator (Windows), and Orca (Linux) can launch, identify
  daemon state, reach browser fallback, navigate primary CEREBRUM areas, and
  recover from daemon loss;
- camera denial/revocation and every permission error remain operable without a
  pointer or camera;
- no severity-critical axe violations and no unresolved serious violation in
  the native and browser surfaces.

## Recovery sign-off

Release sign-off records clean install, existing-data upgrade, interrupted
startup, daemon already running, stale control endpoint, locked vault, daemon
crash, coordinated restart, no network, disk full, incompatible shell/daemon,
failed update, rollback, browser fallback, uninstall, and reinstall. Each case
must state what data remains safe and must never start a second daemon.

The initial macOS candidate POC selected Tauri at 142,544 KiB settled RSS with
a 14,586,400-byte executable. The promoted attach/recovery foundation then
built to 3,847,408 bytes and measured 128,448 KiB RSS. These are selection and
local implementation baselines only; neither is release evidence for the full
shell.
