# Connect your SAGE to another network

This guide shows you how to link your whole SAGE to someone else's whole SAGE, then independently choose which existing domains may cross that trusted link. The two brains can connect on the same LAN or across the internet. In the app this lives under the **Federation** section (the federation icon in the sidebar). The transport introduced in v11.6 carries the same pinned mTLS protocol over libp2p, with direct-path discovery, NAT traversal, and Circuit Relay v2 fallback. The relay only carries encrypted bytes; it never receives the federation keys or plaintext memories.

It is written for the person clicking the buttons. You do not need to understand consensus or certificates to follow it. There is a short honest section at the end that explains what actually keeps the link safe, and what it does not promise.

---

## What a federation connection is

A federation connection is a trusted link between **two whole SAGE networks**. JOIN proves which SAGE is on the other end; it does not grant access to any domain. After the link is active, each person separately chooses what their own SAGE may share.

Four things make it what it is:

- **Whole-SAGE to whole-SAGE.** You are linking one entire brain to another entire brain. This is not the same as adding an agent to your own SAGE (that is the **Agents** section), and it is not the same as joining more computers on your own LAN into one shared brain (that is the node-join flow under **Connect an AI tool**, which makes another computer a peer node on your own network).
- **Read borrows; Copy is a separate two-sided choice.** Read results come back tagged with their source and are shown in the moment; they are not stored merely because Read is enabled. Copy is offered by the source per domain, but nothing is retained unless the receiving SAGE independently selects **Save here**. An accepted copy enters the receiver's ordinary local consensus pipeline and then follows that brain's own lifecycle.
- **Connection-bound Write is not available in v11.9.** The versioned field and route are reserved for compatibility, but the route returns authenticated `501`. An ordinary domain grant is not enough because it is not bound to one trusted connection and one exact submission.
- **It deletes nothing.** Connecting adds a small treaty record and an empty permission policy. It does not touch, move, or erase a single memory on either side. Turning the connection off stops future access and synchronization; copies already accepted by either sovereign brain remain governed there.

You grant what **they** may Read or Copy from **you**; they grant what **you** may Read or Copy from **them**. The two permission snapshots are independent, start empty, and can change at any time without running JOIN again. Neither side can quietly widen the other's access, and a Copy offer still cannot force the receiver to retain anything.

---

## Before you start

- Both people need federation switched on. There is no LAN-versus-internet choice: SAGE prepares every usable direct and secure-relay candidate, then selects a working route during the encrypted exchange. The wizard reads the actual federation listener address and port (usually **8444**) for the Direct candidate. An explicit listener host is honored exactly; a wildcard bind lets the wizard offer detected addresses. Port `0` is invalid because an ephemeral port cannot be truthfully advertised.
- Internet reachability depends on at least one configured relay being ready when no direct route works. SAGE ships with the project relay route and operators may add or replace relay multiaddrs. A relay outage can delay a relay-only connection, but does not weaken authentication or expose memory content.
- You will each need a camera, or a shared screen, or at least a phone call you placed to a number you trust. The connection is safest when you are in the same room or on a video call you started.
- You do not choose domains during JOIN. After both codes match, open the connection and choose from domains that already exist on your own SAGE. Leaving every box clear is a healthy connected state that shares nothing.

Open **Federation** in the sidebar. You will see two big choices:

- **Join someone's network** - they will show you a code to scan.
- **Let someone join mine** - you will show a code for them to scan.

One of you picks each. Below are both walkthroughs, step by step, exactly matching the screens.

### Advanced: connectivity configuration

The guided wizard persists peer routes automatically. Operators who run their own relay or need fixed peer routes can override them in `~/.sage/config.yaml` and restart SAGE:

```yaml
federation:
  enabled: true
  p2p_enabled: true
  p2p_listen_addrs:
    - /ip4/0.0.0.0/tcp/0
    - /ip4/0.0.0.0/udp/0/quic-v1
  p2p_relay_addrs:
    - /dns4/relay.example.org/tcp/443/wss/p2p/RELAY_PEER_ID
  p2p_peers:
    REMOTE_CHAIN_ID:
      - /dns4/peer.example.org/tcp/443/wss/p2p/REMOTE_PEER_ID
      # Relay-only form:
      # - /dns4/relay.example.org/tcp/443/wss/p2p/RELAY_PEER_ID/p2p-circuit/p2p/REMOTE_PEER_ID
  p2p_force_private: true
```

Every relay and peer value must be a complete multiaddr ending in `/p2p/<peer-id>`; `p2p_peers` is keyed by the remote SAGE chain ID from the established agreement. Configured peer IDs are only an inbound connectivity allowlist - the existing federation mTLS certificate, pinned CA, agreement, and signed requests still authenticate the remote chain. `p2p_force_private` asks for an immediate relay reservation and is useful behind known NAT; leave it false when automatic reachability is preferred. If P2P connectivity fails before TLS begins, SAGE falls back to that agreement's direct HTTPS endpoint. A TLS or identity failure never downgrades.

---

## Walkthrough A - Join someone's network (guest)

Pick **Join someone's network**.

### 1. The channel gate

First the app asks how you are looking at the other person's code. This matters, so answer honestly:

- **Same room, or their phone held up to my camera on a call I placed** - the strongest option.
- **We're on a call, I'd see a shared screen or an image** - weaker.
- **We're just on the phone / no camera** - weakest.

A shared screen or a forwarded image can be faked, so the last two options quietly switch you to the **spoken-code** method, which is built for phone calls. You also enter **your** network address here so the host can reach you back.

### 2. Scan their code

Point your camera at the host's connection code, or paste it if you are on the spoken-code path. This code carries their network name, their address, and a fingerprint of their identity.

The app fetches the host's certificate and checks that its fingerprint matches the code you just scanned. If they do not match, it stops and tells you to stop - do not push past that.

### 3. Show them your code

Now the app shows **your** code (a QR). Hold it up to their camera, or send it for them to paste. Their SAGE scans this to check that the network calling itself "you" really is you. When they have it, press **They've got my code - verify trust**.

### 4. Start the trust check

Once they have your return code, press **They've got my code - verify trust**. This sends a fixed trust-only request with no shared domains. Domain permissions are deliberately unavailable until both people complete the spoken-code check.

### 5. Read your code out loud

The app shows a six-digit code. **Call the host and read it to them out loud.** Do not paste it - saying it is the point. This code proves the two of you are really connected to each other and not to someone sitting in the middle. The app waits here while the host types in what they heard.

### 6. Check the code they read you

When the host approves, the app shows the compare screen. **They will now read you a code.** Type exactly what you hear. The **Yes - connect** button stays dead until the number you typed matches the number your own SAGE computed.

- If it matches, you have both proven co-possession. Press **Yes - connect**.
- If it does not match, the app warns you in red. **Do not continue.** Hang up and call them back on a number you trust, then start over.

### 7. Connected

That is it. You will see "You're connected", a two-of-two meter filled in, and a reminder that trust and permissions are separate. Your new connection appears in the list on the Federation page. Open it to choose which existing domains the other SAGE may Read or Copy; they make their own choices on their computer.

---

## Walkthrough B - Let someone join mine (host)

Pick **Let someone join mine**.

### 1. Let SAGE prepare the connection

SAGE prepares Direct and Secure relay candidates automatically and labels them as **Prepared**; that does not mean either route is already in use. The actual route is selected only when the other SAGE connects. Usually you can show the connection code immediately. Open **Advanced Direct address** only when a multi-homed machine needs a different reachable listener address. If no valid Direct address exists, a prepared secure relay can still carry the ceremony.

### 2. Show your code, then scan theirs back

The app shows your connection code as a QR. Have the guest scan it with their SAGE app (a plain authenticator app can read it too, but the join is driven from SAGE). This is best done in the same room, or held up to a video call you trust.

Then the guest shows **their** code back to you. Scan it, or paste it. This is the step that pins down who the guest really is - the fingerprint you scan here is the anchor the whole connection is checked against.

### 3. Wait for their request

The app waits while the guest's SAGE reaches out to yours. When it arrives, you move on automatically.

### 4. Review who wants to connect

You will see something like *Someone using the name "their-network" wants to connect*. This screen establishes identity trust only: approving it grants no domains. Press **Next: verify their identity**, or **Ignore** to burn the request and walk away with nothing shared.

### 5. Check their code

**The guest will read you a code.** Type exactly what you hear. The approve button stays dead until it matches what your SAGE computed. This is the moment that proves it is really them.

- Match -> press **Yes, they match - approve**.
- No match -> press **No - stop**. The app tells you plainly: do not approve, nothing was shared, nothing was changed. Hang up and call back on a trusted number.

### 6. Read your code back to them

Now your SAGE shows a code for you to read **back** to the guest, out loud. Say it - do not paste it. This is the second half of the handshake (two of two). The app waits while the guest confirms on their side.

### 7. Connected

When the guest confirms, you see "Connected", the two-of-two meter full, and the reminder that trust is established but domain permissions are still empty. The connection is now in your list, and you can turn it off any time.

Open the connection to manage two separate views:

- **What this computer shares with them** is your editable snapshot over existing domains. Read enables live lookup. Copy offers synchronized copies and automatically includes Read. Write is visibly unavailable.
- **What they share with this computer** is read-only on your screen because they control it. If they offer Copy for a domain, **Save here** is your independent receiver opt-in.

Both people can change their own snapshot whenever domains or working relationships change. No one has to reconnect, and neither side controls the other's choices.

---

## The two codes, and why there are two

You spoke two different six-digit codes during the ceremony. That is on purpose - it is a **2-of-2** handshake:

1. The **guest reads a code to the host**, and the host types it. That is the host's "yes".
2. The **host reads a code back to the guest**, and the guest types it. That is the guest's "yes".

Neither side is connected until both "yes" steps happen. The little two-of-two meter on screen counts them: 0, then 1, then 2. Only at 2 is the link live on both ends.

Under the covers each code is a short **time-based one-time code (TOTP, RFC-6238)** computed from the secret the two sides established during the scan. Because the code is derived from that secret, the numbers only match when both sides genuinely hold the same secret - that is what a match proves (co-possession). What it cannot prove is that the secret reached the *right* person; that is what the human scan-and-compare is for, and why the next section matters.

---

## Permissions after JOIN - what actually crosses the link

Open a connection from **Federation**. Each side controls a complete per-peer snapshot on its own SAGE; saving replaces that side's previous snapshot. The source enforces it again when serving or sending, so a peer cannot talk its way past a withdrawn permission (`web/federation_permissions.go`, `internal/federation/server.go`, `internal/federation/sync_outbox.go`).

- **Existing domains only.** The picker is built from domains already registered or observed on the source node. A trailing `*` such as `tii*` filters the list for safe bulk selection; it does not create a wildcard grant or a new domain.
- **Read.** Allows live recall inside that exact domain subtree. Every returned record is checked again, and the receiver borrows the result without storing it.
- **Copy offer.** Implies Read and permits the source to synchronize the domain, but delivery is effective only where the receiver has separately checked **Save here**. Removing either the offer or the subscription closes future delivery.
- **Write (not yet).** Always off in v11.9. Attempts fail closed because SAGE does not yet have a consensus authorization bound to the active connection generation, peer, domain, and exact submission.

A present empty snapshot is explicit deny-all, including immediately after a fresh JOIN. Each side can change its own snapshot independently and at any time without pairing again.

---

## Turning a connection off

Open **Federation**. Each connection is a row with a status dot (green when active and unexpired) and expandable directional permissions. Active connections have a **Turn off** button.

Press it and confirm. This:

- Broadcasts a revoke on your chain (an on-chain "this treaty is over").
- Purges the local connection capabilities, copy lanes, queued deliveries, cached seed/CA, and persisted peer route, so a future re-connection starts clean.

**It erases no memories.** It stops future live Read and Copy traffic; memories already accepted as local copies remain under the receiving brain's own lifecycle. If you ever want the link back, run the join ceremony again.

The other side may still show its own treaty row until it revokes too, but live traffic requires the current exact agreement and identity on both ends and therefore fails closed.

---

## The honest security model

Read this part. It is short and it matters.

**The human check is the anchor.** The one thing that actually proves you are connecting to the right network - and not an impostor in the middle - is the moment a person scans a code held as a physical object, in the same room or on a video call they placed, or reads a code out loud on a phone call to someone they trust. That human step is the root of trust. Everything else hangs off it.

**The six-digit codes prove two things, and only two things:** that both sides hold the same shared secret (co-possession), and that a human on each side said "yes" (2-of-2 consent). **They do not prove the secret reached the right person.** If you scanned a code off a stranger's forwarded screenshot, or read your code to someone impersonating your friend, the codes will still match - because you and the impostor now share a secret. That is why the app keeps pushing you toward in-person or a call **you** placed, and why the spoken-code path is never presented as equally safe. The codes are a seal on a decision **you** make; they are not the decision.

So: only ever connect when you are confident, by your own eyes or your own ears on a trusted line, that the other end is who you think it is. If a compare screen ever shows a mismatch, stop. Do not approve. Hang up and call back on a number you trust.

**What the link can and cannot do once connected:**

- It can serve live borrowed recall only for domains where you enabled Read.
- It can offer copies only for domains where you enabled Copy, and the other person must independently choose **Save here** before their SAGE retains them.
- It cannot perform connection-bound remote Write in v11.9. The authenticated endpoint returns `501` and never dispatches the submitted body into the memory API.
- It cannot delete anything, on either side.
- It cannot use the JOIN itself, a stale agreement generation, a different operator/CA, or unrelated group membership to widen domain access.

---

## Troubleshooting

**"Host CA does not match the scanned code (possible tampering) - stop."**
The certificate the host served did not match the fingerprint in the code you scanned. Treat this as a real warning - someone may be sitting between you. Do not retry blindly; re-scan the real code in person or on a trusted call.

**"Host has not scanned your connection code yet."**
As the guest, you got ahead of the host. The host must scan your return code (step 2 on their side) before your request can bind. Wait for them, then continue.

**The compare screen shows a mismatch (red warning).**
The codes did not line up. This is the safety check doing its job. Do not continue. Hang up, reach the other person on a number you trust, and start the ceremony over.

**"Your side is connected but the host has not confirmed yet."**
You (the guest) finished, but the final response did not reach you. This is a safe, one-sided window - the host cannot query you until their side is live. Press **Yes - connect** again: if the host already activated, it verifies the same certificate and signatures and returns the original result without creating another agreement transaction. If the host never activated, the same retry completes it.

**"Endpoint changed" / "endpoint does not match the scanned connection code."**
An address was edited after one of the codes was generated. Endpoints are part
of the signed safety transcript, so SAGE stops before either side activates.
Generate fresh codes after correcting `federation.listen_addr`; never edit a QR
payload or reuse the old return card.

**The session expired / "join session not found or expired."**
A join has to finish within about 15 minutes. If you left it sitting, the session times out for safety. Start over from the Federation page - nothing was created.

**"Refusing self-federation."**
You tried to connect a SAGE to itself (same network id). Federation is between two different networks.

**"Too many join attempts."**
The listener rate-limits repeated attempts from the same connection. Wait a minute and try again.

**A connection shows as expired.**
Some treaties carry an expiry. An expired row no longer serves or queries. Turn it off and re-run the join to refresh it.

**The connection is green but no memories are shared.**
That is the safe default. JOIN establishes trust with an empty permission snapshot. Expand the connection, enable Read or Copy on existing domains you control, and save your snapshot. For copies, the other person must also enable **Save here** on their computer.

**Read works but copies do not arrive.**
Check both halves of the Copy decision: the source must offer Copy for the domain and the receiver must subscribe with **Save here**. Either side can withdraw its half without reconnecting.

---

### Under the hood (optional)

The ceremony runs over a dedicated mutually authenticated federation listener. Each operator's compatibility treaty set/revoke reaches its own chain, while mutable peer Read/Copy snapshots and transport subscriptions remain off-consensus. Fresh JOIN accepts only the fixed empty-domain compatibility envelope and installs an explicit deny-all policy. Peer admission binds the exact active agreement, chain, operator key, CA pin, and policy epoch; synchronization-group traffic additionally needs the exact active group/member/domain projection. Agreement changes, JOIN activation, narrowing, and revocation use the same mutation boundary across signed REST and dashboard paths, so a completed change cannot leave a superseded broader response in flight.

The operator wizard routes live at `/v1/dashboard/federation/join/*` (`web/federation_join.go`), dashboard permissions at `/v1/dashboard/federation/connections/{chain_id}/permissions` (`web/federation_permissions.go`), the peer-facing ceremony and data plane at `/fed/v1/*` (`internal/federation/join_routes.go`, `internal/federation/server.go`), and Copy authorization in `internal/federation/sync_outbox.go`. Borrowed Read results are never persisted (`internal/federation/proxy.go`); accepted copies are locally signed ordinary memory submissions. The reserved Write route fails before parsing or dialing (`api/rest/federation_write_handler.go`, `internal/federation/remote_write.go`).
