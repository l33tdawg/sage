# Native-shell quality gates

These gates are release criteria, not aspirational telemetry. A native package
is not promoted on any platform without immutable CI evidence for that platform.
Browser CEREBRUM and the existing Go release matrix remain mandatory.

The current product boundary and the distinction between `native-control` and
`web-control` are defined in
[`native-cerebrum-macos-v12-adr.md`](native-cerebrum-macos-v12-adr.md).
The macOS v12 product accepts only native SwiftUI/AppKit/Metal controls;
Tauri/WebView rows are historical prototype evidence and cannot close a native
route/action row.
The machine-readable release evidence contract is
[`v12-native-acceptance-ledger.md`](v12-native-acceptance-ledger.md).

Each gate names the release milestone that must establish it. Most begin in
v11.11; the performance budgets beyond incremental shell RSS and the
accessibility gates are v11.14 hardening work. Because the shell remains a
private, undistributed alpha throughout v11 and first distribution is targeted
at v12, those rows block native-shell distribution rather than unrelated v11
release channels. A later distribution gate is still a gate: the bridge
releases must establish the architecture, record what is measurable, and ship
nothing that forecloses it.

## Current enforcement status

**v11.11 distributes no native shell on any platform.** The shell is alpha; see
"The native shell is alpha and does not gate releases" below. macOS is the sole
native-product target for v12. Linux already
compiles and runs its full installed-package lifecycle smoke in
[`native-shell.yml`](../.github/workflows/native-shell.yml), but the current
Tauri/Wry GTK3 dependency line is blocked from production distribution by the
unfixed advisory below. CI regression evidence is therefore real but incomplete:
Linux must gain a safe supported WebView path and the same production evidence
class as macOS before any Linux native distribution could be considered. Linux
native work is optional R&D and does not gate v12. See
[Linux v12 blocker](#linux-v12-blocker) below.

The tracked preview now enforces locked dependency compilation, Rust
format/test/Clippy, full platform shell-control tests, isolated Codex endpoint
acceptance tests, dependency audit, a license-bearing CycloneDX SBOM, and
unsigned package construction on macOS, Windows, and Linux. Each constructed
package is unpacked and
must contain exactly one bundled daemon whose embedded Go OS/architecture
matches the declared target
and whose embedded version matches the version supplied to the shell package
build. Those checks emit a machine-readable release-pair record beside
the package with the target, build version, packaged shell artifact size/hash,
and bundled daemon path/size/hash. They establish the foundation; unsigned CI
artifacts and their records are not release evidence.

## The native shell is alpha and does not gate releases

**Browser CEREBRUM is the product. The native shell is a background track.**
Through the v11.11–v11.14 bridge the shell is **alpha**: built, linted, and
runtime-tested in CI, never staged as a public release asset, and not intended
for end-user use. Users stay on the web version, and normal releases continue to
ship bug fixes and capabilities on their usual cadence — federation, agent
messaging, and the rest of the roadmap do not wait behind desktop packaging.

The tagged release workflow version-locks the Tauri and Cargo metadata to the
tag and constructs private per-platform package-pair and SBOM evidence. Those
artifacts are deliberately excluded from public GitHub release staging:
`stage-github-release` downloads only `release-assets-*`, and
`release-workflow.test.mjs` asserts native evidence never appears there. The
identity is pinned to `SAGE Native Preview` / `com.sage.native-preview` at the
metadata gate.

An earlier revision of this record froze the **entire** v11.11 publication graph
— Docker, SDK, MCP, legacy installers, and the GitHub release — until the native
shell carried signed, runtime, recovery, performance, and accessibility
evidence. That was a mistake and has been removed. It blocked every shipping
channel on productizing an artifact **no user receives**, which bought nothing:
the exclusion from public staging and the pinned preview identity already
prevent an unsigned shell from reaching anyone.

The promotion gate remains, but fail-closed on the thing that actually matters:
if a release ever declares a native release class other than
`unsigned-preview-evidence` — i.e. it intends to **distribute** the shell — it
fails until the signing/notarization, installed-runtime, update/rollback, and
recovery evidence below exists. **That bar applies at first distribution, which
the roadmap places at v12**, not at every release that merely builds the shell.

Recovery runs for tags older than v11.11 remain supported.

### `RUSTSEC-2024-0429` (glib) — dismissed, not fixed

GitHub Dependabot alert 37 (`RUSTSEC-2024-0429` / `GHSA-wrw7-89jp-8q8g`)
concerns `glib` 0.18.5, which the Linux Tauri stack receives through
Wry/WebKitGTK; the affected safe iterator API can trigger undefined behavior and
optimized-build crashes.

**The alert was dismissed as not-used on the decision that v11.11 does not
distribute a Linux native shell.** It was not fixed, and the vulnerable code is
still compiled by the non-distributed Linux CI build. This is a scope decision,
not a remediation. It rests on two verified facts:

- Wry declares its whole GTK chain under
  `cfg(any(target_os = "linux", target_os = "dragonfly", target_os = "freebsd",
  target_os = "openbsd", target_os = "netbsd"))`. macOS resolves the web view
  through WKWebView and Windows through WebView2, so **neither target platform
  compiles `gtk`, `webkit2gtk`, `soup3`, or `glib` at all**.
- No user receives the affected code, because the only artifact containing it is
  never published.

There is no upgrade path, and this is a hard version wall rather than a pending
release: Wry 0.55.1 is the latest published version and requires `gtk ^0.18`;
the GTK3 Rust binding line is capped at `gtk` 0.18.2, last released 2024-12-09,
which pins `glib` 0.18.x. The advisory is first fixed in `glib` 0.20.0, and
`glib` 0.20+ belongs to the GTK4 line — a differently named `gtk4` crate, not a
newer `gtk`. `cargo update`, a `--precise` pin, and a `[patch.crates-io]`
override all fail: the first two are semver-excluded by `gtk ^0.18`, and the
third compile-fails because `gtk`/`gdk`/`cairo-rs` 0.18 are not written against
the glib 0.20+ API.

`cargo audit` reads the lockfile rather than the per-target build graph, so it
still reports this advisory on every platform's CI run. That is expected and
must not be silenced.

> **This dismissal is conditional and must be revisited.** If a Linux native
> shell is ever distributed again, this alert must be re-opened and treated as
> release-blocking before that artifact ships. Do not treat the dismissed state
> as a standing judgement that the advisory is harmless.

### Linux native R&D blocker

The current dependency assessment and bounded spike decision are recorded in
[`design/v12-linux-native-path.md`](design/v12-linux-native-path.md).

The preferred production path is **upstream Wry GTK4/webkitgtk-6.0 support**,
tracked in `tauri-apps/wry#1769`. That migration also clears this advisory,
because the `gtk4` line depends on a remediated `glib` generation.

Linux native distribution is outside the v12 product commitment. Optional R&D
may revisit the upstream path, but owning an unreviewed fork of a
security-sensitive WebView layer remains unacceptable. Until one safe path
passes, Linux native distribution is blocked and browser CEREBRUM plus the CLI
remain the supported Linux product surfaces. This does not gate macOS.

The install/launch/deep-link/offline, performance, assistive-technology,
signing/notarization, update/rollback, and uninstall-preservation rows below are
the bar for **distributing** the native shell. They are not a v11.11 shipping
requirement, because v11.11 does not distribute it. Work through them as the
bridge releases land; they become release-blocking at first distribution.
Windows named-pipe reads and writes now use overlapped cancellable deadlines
with native stalled/partial-frame tests in the code gate.

The remaining performance budgets and the accessibility gates are the
**v11.14** accessibility/performance/offline hardening milestone. The current
private alpha records the evidence it can measure, including idle CPU, but
these rows become release-blocking when the native shell is first distributed
at v12. See the notes on each section below — private-alpha status is not
licence to ship a native package that lacks the required evidence.

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
impossible*. The Linux `unshare -n` approach is strictly stronger and remains
useful only for optional Linux native R&D evidence.

The detector is verified in both directions against a synthetic bundle: a
deliberate outbound connection is caught and reported with the offending socket,
and a realistic loopback-only topology passes. A run that observes zero sockets
is not by itself evidence.

## Supported matrix

| Platform | Build/install floor | Required package evidence |
|---|---|---|
| macOS | oldest Apple-supported macOS that SAGE declares for the release; Intel and Apple Silicon where distributed | signed `.app`, notarized/stapled DMG, clean install, Gatekeeper launch, rollback |
| Windows | Browser CEREBRUM support matrix; native preview CI is non-product evidence | Browser compatibility/accessibility/degraded-mode artifacts; no native package required |
| Linux | Browser CEREBRUM support matrix; native preview CI is optional R&D | Browser compatibility/accessibility/degraded-mode artifacts; no native package required |

The exact OS image identifiers, browser/WebView versions, CPU/RAM, and artifact hashes
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

These budgets are set in v11.11, instrumented/hardened through v11.14, and
become release-blocking for the first distributed native shell in v12. That split is the
roadmap's, not a relaxation: v11.11 "set budgets ... and establish [the]
architecture now even though v11.14 performs the full hardening pass", and
v11.14 "hold[s] the v11.11 performance budgets for the embedded experience on
large memory stores and the 3D connectome view". Nothing below is being
weakened; the column records the release at which each becomes release-blocking.

Report p50/p95 and raw samples on named baseline hardware. Separate shell cost
from daemon boot, model boot, consensus, and queries.

| Measure | Budget | Blocking from | Measurable today? |
|---|---:|---|---|
| Incremental shell RSS, daemon excluded | <= 200 MiB p95 | **v11.11** | yes — process RSS |
| Settled shell idle CPU | <= 1% p95 | first distribution (v12) | yes — sampled over the settle window |
| Warm re-open to focused existing window | <= 500 ms p95 | first distribution (v12) | partly — handoff is timable, "focused" needs a frontmost-window check |
| Cold launch to bundled recovery paint | <= 1,000 ms p95 | first distribution (v12) | no — needs a paint signal |
| Ready daemon to interactive CEREBRUM | <= 2,000 ms p95 | first distribution (v12) | no — needs an interactive signal |
| Daemon loss to visible recovery action | <= 2,000 ms | first distribution (v12) | no — needs a recovery-shown signal |
| Shell/navigation input response | <= 100 ms p95 | first distribution (v12) | no — needs UI automation and marks |
| Native overhead over same browser action | <= 25 ms p95 | first distribution (v12) | no — needs both paths instrumented |
| MRI frame pacing | >= 55 FPS median; no recurring >100 ms stalls | first distribution (v12) | no — needs frame timing and a real GPU |

**RSS blocks from v11.11 because it is the premise of the framework decision,
not because it is convenient.** `desktop-shell-decision.md` rejected Electron at
358,720 KiB against this exact 200 MiB ceiling and selected Tauri at 142,544 KiB;
the promoted foundation measured 128,448 KiB. If the shipped shell drifts past
200 MiB, SAGE has taken on Rust and a per-platform WebView matrix — and given up
Electron's stronger tooling and accessibility — for a benefit it no longer has.
That makes RSS the one performance number with a decision riding on it today.

Six of the nine measures are **not** blocked on hardware. They are blocked on
instrumentation that does not exist yet: the shell emits no paint, interactive,
recovery-shown, or frame-timing signal, so they cannot be observed from outside
the process at all. Building that instrumentation is the v11.14 hardening work.

macOS now measures the two rows that are observable without shell-side
instrumentation. `native-shell-macos-perf-smoke.sh` launches the staged app
against an isolated `SAGE_HOME`, waits for a renderable SSCP state, then samples
every process running the exact shell executable — the daemon is identified
separately and excluded from the budget, but recorded for context. Sampling
starts only after the app is renderable, because these are settled-state budgets
rather than startup peaks.

**Incremental shell RSS is enforced from v11.11**; settled idle CPU is recorded
only. The threshold logic lives in `native-shell-perf-evaluate.py` so its
FAILING path is testable without a macOS runner: the suite drives it over the
ceiling, exactly at it, with zero samples, and with two shell processes whose
sum breaches it. A harness whose failure path has never executed is not
evidence.

The remaining seven rows are still unmeasured, and are blocked on
instrumentation rather than hardware — the shell emits no paint, interactive,
recovery-shown or frame-timing signal, so they cannot be observed from outside
the process at all. Until that exists those rows are set budgets, not measured
ones, and nothing here should be read as evidence that they have been met.

For the first distributed native shell in v12, three consecutive benchmark runs must pass. A regression of more
than 10% against the last published release fails even when the absolute ceiling
passes, unless the release record accepts the tradeoff with evidence. Hosted CI
runners are acceptable for the RSS row; the latency and frame-pacing rows
require a named baseline machine, because runner variance is wider than those
budgets.

## Accessibility gates

These follow the same split as the performance budgets, and for the same
roadmap reason: v11.11 "establish[es] keyboard navigation, focus visibility,
screen-reader naming, and reduced-motion architecture now even though v11.14
performs the full hardening pass", and v11.14 "meet[s] the accessibility bar v12
treats as a release criterion". **The requirements below become release-blocking
for the first distributed native shell in v12.** v11.11 must establish the architecture that makes them
achievable, and must not ship anything that forecloses them.

The screen-reader matrix cannot be automated on hosted runners and needs a
manual pass on a named machine regardless of release.

Automated semantic checks supplement, never replace, the OS smoke matrix:

- all actions are keyboard-complete with visible focus and logical order;
- every control/window/state has an accessible name and status changes use an
  appropriate live region without stealing focus;
- 200% zoom, high contrast, light/dark mode, and OS reduced-motion work without
  clipping or lost content;
- VoiceOver (macOS) and Narrator (Windows) can launch, identify
  daemon state, reach browser fallback, navigate primary CEREBRUM areas, and
  recover from daemon loss;
- camera denial/revocation and every permission error remain operable without a
  pointer or camera;
- no unresolved serious or critical findings from system AX inspection,
  Accessibility Inspector, or the named-Mac VoiceOver pass on native surfaces;
  and no severity-critical axe violations or unresolved serious axe violations
  in browser CEREBRUM.

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
