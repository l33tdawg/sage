# Native app/daemon development transport qualification

This review candidate is stacked on the stable-backend convergence in
[PR #418](https://github.com/l33tdawg/sage/pull/418). It qualifies the production
Swift Foundation transport against an isolated beta daemon carrying the
published v11.23.15/app-v28 backend. PR #417's native menu/focus changes remain a
separate review unit; its socket, API and event transport sources are identical
to this candidate's parent.

## Failures repaired

An empty `daemon_version` indexed an empty array and terminated the native
client. Version parsing now rejects malformed semantic versions normally,
including empty identifiers, non-ASCII digits and forbidden leading zeroes.
The accepted range remains v11.10–11.19 and v12.0 beta. This qualification does
not widen it to stable v11.23 or final v12.0.

The socket's inactivity timeout restarted after every read. A trickled frame
could therefore hold the client beyond SSCP/1's two-second deadline. Connect,
write and read now share one monotonic deadline. The client verifies the Unix
peer UID, rejects unknown response fields and malformed generations/startup
proofs, handles interrupted I/O, and converts early close into a normal error
with `SO_NOSIGPIPE`.

`URLSession.AsyncBytes.lines` omits empty lines, including the blank separator
that dispatches an SSE event. Real streams connected and reconnected without
delivering events even though the parser's string-level unit fixture passed.
The client now delimits UTF-8 bytes itself, preserving LF, CRLF, CR, blank lines
and a leading BOM. An unfinished event does not dispatch at EOF, and a single
line is bounded at 1 MiB.

The connections feed stays readable while federation networking is off. Its
HTTP 200 response does not assert that networking is enabled. The native
Overview now reads `/v1/dashboard/settings/federation` before requesting
connections and returns the disabled state when the explicit setting is false.
That read endpoint is present in the oldest accepted v11.10.0 source.

Local Swift 6.4 also emits a standard macOS resource bundle rather than the
flat bundle used by the pinned Swift 6.2 build. Packaging preserves either
layout intact and verifies the included `brain.obj` in its actual location.

The first pinned Swift 6.2.3/macOS CI run passed the real socket and URLSession
fault checks but rejected a live app-v28 dashboard response. The older
Foundation `.iso8601` decoder does not accept the fractional seconds in
Go/CometBFT RFC3339Nano timestamps, which the local Swift 6.4 decoder accepted.
All native dashboard dates now use the explicit fractional/plain RFC3339
parser already used for Connectome, with regressions for nanosecond fractions
and UTC offsets. Probe failures retain their feed and decoding path so an
unrelated wire drift cannot be mistaken for the same issue.

## Reproducible gate

Run on macOS:

```sh
bash scripts/v12-native-transport-qualification.sh
swift test --package-path desktop/SAGECerebrumNative --disable-sandbox --filter ShellControlQualification
bash scripts/build-native-cerebrum-macos.sh
```

The transport gate compiles the shipping Foundation source files with no
`DEBUG` override. It uses real AF_UNIX connections and real ephemeral
URLSession sessions; no mocked URLProtocol or preview API is substituted. It
exercises fragmented/partial/oversized frames, malformed contracts, unsafe
paths/origins, stalled and trickled responses, explicit federation settings,
private cookies, login/lock, HTTP 401, exact-origin redirects, SSE EOF, empty
stream backoff, HTTP-error recovery, cancellation and terminal stream 401.

It then launches the converged `sage-gui serve` in an owned disposable profile
with four distinct allocated loopback ports. Federation is explicitly off and
the hash embedder avoids external services. The existing `v119testfixture`
build shortens the governed upgrade-delay floor to three blocks and the
proposer cooldown to one block. The daemon still performs the real signed
ladder through app-v28; authorization, SSCP, dashboard handlers, CometBFT,
storage and serving behavior remain production code. Production timing is not
claimed.

The shipping Swift client discovers that daemon through SSCP and decodes its
typed Overview/Search/Brain feeds. The fixture creates one canonical task in
that disposable profile while the native stream is connected, requiring its
real `task` event before cancellation. Native Search and Brain must then decode
the populated memory; native replace/bulk tag edits, canonical tag rereads and
the related-memory endpoint are exercised against it.
A stopped-daemon check then fails normally, and a clean daemon restart must
retain app-v28 and issue a different instance generation. This last check is
transport evidence; the native AppSession's automatic restart recovery is
still open.

The macOS PR lane pins Xcode 26.2 / Swift 6.2, checks out the exact PR
head, runs this gate and the native unit suite, then builds the release app and
checks its linkage and absence of debug/test transport markers. Artifacts
retain source identity, source and executable hashes, each executed assertion,
the owned daemon log and the final completion flag. Failures and missing tools
fail the gate; no qualification case is skipped. Profile databases and keys
are deleted after the run and are never uploaded.

## Acceptance boundary

This is development transport evidence for API schema 1 and SSCP/1. The beta
uses the existing same-origin metadata bridge. It does not complete the
one-use, generation-bound native session bootstrap required by the fully
native ADR, app-owned daemon lifecycle, AppSession restart recovery,
production timing, vault-unlock behavior against an encrypted real node,
cross-version installed pair/rollback qualification, physical HID, spoken
VoiceOver, signing/notarization, clean-machine acceptance or the v12 program.
The populated-data proof is one task in a fresh single-validator profile.
Ordinary-agent enrollments, populated Connectome/engram data and the complete
Search mutation/workflow matrix remain open.

The stable backend/API/SDK/browser/reference sources remain byte-identical to
the PR #418 parent. No live profile is migrated, no installed application is
replaced and no stable tag or publication is made. The native product baseline
remains the last accepted baseline until the remaining acceptance work and
human review are complete.
