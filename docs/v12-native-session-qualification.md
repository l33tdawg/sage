# Native session recovery qualification

This review candidate builds on PR #419's Foundation transport fixes. It adds
native attachment monitoring and session recovery without changing SSCP/1,
the accepted daemon-version range, or the daemon's authorization rules.

## Session ownership

`AppSession` retains the validated daemon generation, exact API origin and
startup proof. Its monitor owns connection discovery independently of the
currently displayed view. When discovery fails or the daemon identity changes,
the app removes protected content and retires the previous API client. A fresh
client uses a new private cookie store and checks authentication again before
the app restores an implemented route.

Lifecycle discovery uses a one-second socket deadline and a one-second poll
interval, targeting the contract's two-second loss detection plus scheduling
overhead. Direct SSCP transport calls retain their two-second default deadline.
Slow HTTP authentication runs separately and cannot block discovery polling.

Connection and authentication operations carry session/operation identities.
Late discovery, login, lock and unauthorized callbacks from retired work cannot
change the current session. Protected view identity also changes so old
Overview, Search and Brain state cannot survive a connection replacement.

Retiring a real `SAGEAPIClient` cancels its HTTP requests and event streams and
clears its private cookies. It cannot reconnect an event stream or send a new
request after invalidation. An ordinary session lock clears protected UI while
the daemon revokes the dashboard session. That action does not relock the
shared encryption vault or stop other agents.

## Evidence

The 2026-10-06 local Apple Silicon run passed all 20 real session cases, all
35 transport cases and 20 focused Swift lifecycle/contract tests, with zero
skips in those runs. The release application built successfully and passed
the existing linkage/debug-marker scans. The session's daemon-stop case took
0.937 seconds, including child shutdown and state observation. These are
development results with Swift 6.4; CI separately uses the pinned Swift 6.2
toolchain on the exact PR head.

The focused Swift lifecycle tests exercise delayed results, overlapping
operations, stale unauthorized callbacks and monitor cancellation. The real
session qualification compiles production Swift sources without `DEBUG` and
drives them through an owned disposable daemon profile. Its encrypted-vault
scenario uses the daemon's normal encryption and authentication endpoints.

Run on macOS:

```sh
swift test --package-path desktop/SAGECerebrumNative --disable-sandbox --filter 'AppSessionLifecycle|ShellControlQualification|nativeCommandsAreAvailableOnlyForAReadySession|focusSearchIsReadyGatedAndConsumedExactlyOnce'
bash scripts/v12-native-session-qualification.sh
```

The qualification must reject a wrong passphrase, open the real vault with the
correct passphrase, protect the native UI on session lock, discard the old
client when the daemon stops, and authenticate again after a new validated
generation appears. It records source hashes and executed assertions. Missing
tools, timeouts, failed assertions and incomplete runs fail the gate.

The daemon-stop case records its elapsed time and imposes a five-second
end-to-end ceiling. That measurement includes graceful child shutdown and the
probe's state observation; it is distinct from the discovery timing target.

The daemon fixture uses the existing `v119testfixture` governance timing bounds
with four allocated loopback ports, a hash embedder and federation disabled.
It never opens the installed app's profile. Profile keys and databases are
removed after the run and excluded from uploaded evidence; passphrases and
session cookies are not logged.

## Remaining boundaries

This is development evidence for attaching to and recovering from an
independently managed daemon. It does not implement native daemon launch,
supervision, stop, update or rollback. A transient discovery failure can recover
to the same generation after fresh validation; only a changed generation is
evidence of daemon replacement.

The native beta still uses the existing same-origin metadata bridge. The
accepted fully native ADR separately requires a one-use session bootstrap tied
to daemon generation, origin, startup proof and verified app identity. SSCP/1
is deliberately status-only, and its current Unix peer check proves the user
identity rather than the signed application identity. This candidate neither
adds an unverified app-identity claim nor treats the existing bridge as the
production bootstrap.

Production governance timing, installed cross-version pairs, physical keyboard
and VoiceOver behavior, signed clean-machine acceptance and the wider v12
product program remain open. Human review is required before merge; this
candidate does not advance the accepted native product baseline.
