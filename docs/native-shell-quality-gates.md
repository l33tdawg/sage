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
bundled daemon whose embedded version matches the shell package version. Those
checks establish the foundation; they are not release evidence.

Runtime promotion remains open until the install/launch/deep-link/offline,
performance, assistive-technology, signing/notarization, update/rollback, and
uninstall-preservation rows below have immutable platform results. Windows
named-pipe reads and writes now use overlapped cancellable deadlines with native
stalled/partial-frame tests in the code gate; the Windows package/runtime row
still needs immutable runner evidence before promotion.

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
