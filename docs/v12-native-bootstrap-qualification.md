# Native application bootstrap qualification

This candidate follows the session recovery work in PR #420. SSCP/1 remains a
closed status-only protocol. SSCP/2 adds native transport admission for the
macOS beta, independently of vault login and CEREBRUM authority.

## Peer identity and issuance

The approved beta identifier is `com.sage.cerebrum.beta`. Ordinary Darwin builds
with cgo require a Developer ID application from team `2N7GKZ8D8Z`, that exact
identifier, hardened runtime, no current debugged status, and no `get-task-allow`
or runtime-relaxation entitlements.
The requirement is compiled into the daemon. There is no environment switch,
submitted bundle identifier, PID lookup or executable-path fallback. Other
platforms and no-cgo builds cannot issue native tickets; browser CEREBRUM, CLI,
MCP and SSCP/1 remain available.

The daemon reads `LOCAL_PEERTOKEN` from the accepted socket and uses its complete
audit token with Security.framework. This identifies the peer's current code,
not immutable connect-time code: an inherited socket may survive `exec`.
Therefore a request buffered before an exec is insufficient. The daemon verifies
the peer, sends a fresh random challenge, requires an Ed25519 answer on that same
socket, and verifies the same identity again before issuing a ticket. The
challenge binds the validated generation, exact origin, startup-proof value and
client's temporary public key. The client never supplies its trusted identity.

Four concurrent native peer checks are permitted. Frames share SSCP's two-second
socket deadline and are limited to 16 KiB. Security.framework runs with network
access disabled, but its synchronous IPC cannot be cancelled with a hard deadline;
late results are discarded. Ordinary JSON objects reject unknown and duplicate
request fields. Malformed, unavailable or unauthorized issue requests close the
connection. The native app does not retry them through browser-header imitation.

## Proof transcripts

Both signatures use Ed25519 over UTF-8 fields separated by a single newline,
with **no trailing newline**. Every binary value uses canonical unpadded base64url.
The strings below name the fields, rather than supplying example credentials:

```text
SAGE-NATIVE-PEER/1
socket_challenge
instance_generation
ui_origin
startup_proof
public_key
```

```text
SAGE-NATIVE-BOOTSTRAP/1
ticket
redemption_challenge
instance_generation
ui_origin
startup_proof
public_key
```

The empty startup proof is still a field and therefore leaves adjacent newline
separators. Socket and redemption challenges are independently random. The
SSCP/2 issue response echoes the attachment and public key; unknown response
fields or a changed binding cause the Swift client to refuse redemption.

## Ticket and transport session

Tickets contain 256 random bits, expire after 30 monotonic seconds, and are
stored only by hash. The process-local broker allows 128 outstanding tickets and
four per verified process identity. A new broker binds one daemon generation,
exact origin and startup-proof value. Ordinary independent starts use an explicit
empty startup proof; a startup proof never authenticates a user.

`POST /v1/dashboard/native/redeem` accepts the ticket and a second Ed25519 proof,
using the separate `SAGE-NATIVE-BOOTSTRAP/1` transcript. It verifies the server
challenge, every attachment field and temporary key, then atomically consumes the
ticket. Concurrent redemption has exactly one winner. Lost responses require a
new exchange. The endpoint sets no vault cookie and grants no agent, Root, Admin
or federation identity.

The returned random transport credential expires after 15 minutes and is sent in
`X-SAGE-Native-Session`, on the exact loopback origin only. It is stored by hash in
a bounded 128-entry broker. Native requests reject browser metadata, forwarding
headers and agent-signature mixtures. A supplied but invalid native credential
cannot fall back to a browser cookie or another identity. Revoke, expiry and
shutdown cancel in-flight native request contexts within one second plus runtime
scheduling; client-side retirement also cancels HTTP and SSE immediately.

## Vault and authority

The first native bootstrap slice requires an encrypted Synaptic Ledger. A native
transport credential alone cannot read protected memory or acquire Root authority.
Correct passphrase login still uses the daemon's normal vault and authorization
implementation. A vault cookie issued to a native client is bound to that specific
native admission: stripping or replacing the native header makes it unusable.
Dashboard lock and `POST /v1/dashboard/native/revoke` revoke admission and its
bound vault sessions. They do not relock the shared vault or stop agents.

On an unencrypted profile, native authentication returns an explicit instruction
to enable the Synaptic Ledger through local browser CEREBRUM. This prevents
transport admission from inheriting the browser's encryption-off operator policy.
Native onboarding for that case is outside this candidate.

The production AppSession obtains fresh admission before checking vault auth.
It retains PR #420's epoch and operation fences across connection, login, lock and
daemon replacement. Expired admission is retired before a request is sent, and
revoked-admission login failures trigger fresh bootstrap without hiding ordinary
wrong-passphrase errors. Unsupported or unsigned peers fail closed. The separate development
transport probe compiles the older metadata bridge with
`SAGE_LEGACY_TRANSPORT_QUALIFICATION`. Ordinary builds exclude that code entirely
and reject an API client without native admission. Release scans reject its
browser-header marker. The signed session gate uses production compilation.

## Reproduction and limits

Run the Go broker, real macOS code-signing peer checks, SSCP and dashboard tests:

```sh
CGO_ENABLED=1 go test -race ./internal/nativeidentity ./internal/nativebootstrap ./internal/shellcontrol ./web
CGO_ENABLED=0 go test ./internal/nativeidentity
swift test --package-path desktop/SAGECerebrumNative --disable-sandbox --filter 'NativeCredentialLifecycleQualification|NativeBootstrapQualification|AppSessionLifecycle|ShellControlQualification'
bash scripts/v12-native-session-qualification.sh
```

The real session gate signs its disposable probe ad hoc with hardened runtime,
pins its exact code hash in a daemon compiled with `nativebootstraptestfixture`,
and exercises production Swift, Security.framework, SSCP and HTTP against an
isolated encrypted app-v28 profile. The policy override symbol exists only in
that test build. The test does not qualify the Developer ID certificate chain or
prove signing, notarization, installation or Gatekeeper acceptance. Neither
profiles nor credentials are included in evidence. The `v119testfixture` tag
shortens governed activation timing; it does not change production timing.

The candidate does not launch, supervise, update or roll back a daemon, change
consensus/app version, widen daemon-version acceptance or advance the product
baseline. Existing no-cgo release artifacts do not gain bootstrap capability.
Native distribution must build the macOS daemon with cgo, sign the beta app with
the approved identity and complete signed clean-machine acceptance. Physical
keyboard, system accessibility and spoken VoiceOver acceptance remain separate.

A verified peer can exit after issuance; a bearer token is not continuous OS
process authentication. The ephemeral key and both challenges prove possession
at issuance/redemption, while TTL, explicit revocation and daemon generation
bound the later credential. No claim of blanket PID-reuse immunity is made.
