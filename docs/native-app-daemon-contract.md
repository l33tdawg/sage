# Native app-daemon trust and compatibility contract

**Contract:** SAGE Shell Control Protocol 1 (SSCP/1)  
**Scope:** native shell lifecycle only; never application administration

## Boundaries

`sage-gui` is authoritative for process ownership, consensus, storage,
authorization, sessions, updates, and recovery. The native shell is untrusted
presentation code. It may discover status and the exact CEREBRUM origin; it
never receives validator/admin keys, a vault passphrase, arbitrary filesystem
access, arbitrary process execution, or a privileged JavaScript bridge.

Browser CEREBRUM, CLI, MCP, and REST continue to work without the shell.

## Endpoint identity

- macOS/Linux: `${SAGE_HOME}/run/shell-control.sock` in a `0700` directory;
  socket mode `0600`; reject symlinks and non-sockets; verify peer credentials
  where the OS exposes them.
- Windows: `\\.\pipe\sage-shell-control-<profile-hash>`, where the hash binds
  both the current user SID and normalized absolute `SAGE_HOME`; the pipe DACL
  grants that SID only. Separate profiles cannot collide or attach across data
  roots. Local administrators are not treated as renderer peers.
- The daemon creates the endpoint only after acquiring the existing SAGE
  instance lock and removes only the endpoint generation it owns.
- The shell never accepts PID files, HTTP health, port ownership, or the
  endpoint pathname alone as proof of daemon identity.

When the shell starts a bundled daemon, a 256-bit startup challenge is sent on
an inherited anonymous pipe/handle. It is never placed in argv, environment,
URL, log output, or a persistent file. Attaching to an existing daemon relies
on endpoint ownership/ACL plus SSCP negotiation and a fresh instance generation.

## Framing and limits

Messages are UTF-8 JSON preceded by an unsigned big-endian 32-bit length.
Frames are at most 16 KiB. Unknown fields are rejected, reads/writes have a
two-second deadline, and one connection handles one request. Malformed,
oversized, partial, or repeated frames close without a response.

SSCP/1 supports one unprivileged request:

```json
{"control_protocol":1,"shell_protocol":1,"operation":"status"}
```

The response is:

```json
{
  "control_protocol": 1,
  "daemon_version": "11.11.0",
  "api_schema": 1,
  "min_shell_protocol": 1,
  "max_shell_protocol": 1,
  "instance_generation": "base64url-256-bit-random",
  "state": "starting|locked|ready|degraded|draining|failed",
  "ui_origin": "http://127.0.0.1:8080"
}
```

`ui_origin` is present only for `ready` or `degraded`, is loopback HTTP, has no
userinfo/query/fragment, and is canonicalized before comparison. A later
protocol may add bounded restart/stop and one-time launch tickets, but SSCP/1
intentionally cannot mutate daemon state.

## Compatibility

Shell protocol, SSCP, HTTP API schema, binary version, and consensus app version
are independent values.

1. The shell sends its protocol and requires it to fall within the daemon's
   inclusive range before loading CEREBRUM.
2. A matching release gets normal operation.
3. The immediately prior release may get only the bundled recovery screen and
   browser fallback. It never gets guessed API compatibility.
4. Unknown/future protocol, invalid ranges, or malformed semantic versions fail
   closed with an update/recovery action.
5. After daemon restart the shell discards all prior state, reconnects, and
   requires a different valid `instance_generation` before restoring its route.

Release evidence covers new shell/old daemon and old shell/new daemon. A full
bundle rollback must restore a compatible pair without altering `~/.sage`.

## Navigation and authorization

After a successful handshake the shell pins one `(scheme, host, port)` tuple.
Only that exact origin plus its paths/fragments may render. Explicit `https:`
links open in the system browser after validation. Everything else is denied,
including other loopback ports. Redirects are checked by the same rule.

The WebView keeps the daemon's CSP, Host/DNS-rebinding checks, same-origin
policy, cookies, and ordinary session/login flow. A control handshake grants no
RBAC role, agent identity, federation identity, or signing authority.

## Lifecycle and recovery

Repeated launch focuses the one window and queues a validated `sage://` route.
A startup timeout does not start a second daemon. Daemon loss becomes visible
within two seconds and leaves the shell open with retry, browser fallback, and
log-location guidance. Window close and explicit "Stop SAGE node" remain
separate operations; SSCP/1 provides no stop operation.
