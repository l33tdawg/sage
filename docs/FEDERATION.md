# Connect your SAGE to another network

This guide shows you how to link your whole SAGE to someone else's whole SAGE, so the two brains can share memories across networks. In the app this lives under the **Federation** section (the federation icon in the sidebar).

It is written for the person clicking the buttons. You do not need to understand consensus or certificates to follow it. There is a short honest section at the end that explains what actually keeps the link safe, and what it does not promise.

---

## What a federation connection is

A federation connection is a treaty between **two whole SAGE networks**. Once it is on, your SAGE can ask the other SAGE questions and get answers back, within limits you both agree to.

Three things make it what it is:

- **Whole-SAGE to whole-SAGE.** You are linking one entire brain to another entire brain. This is not the same as adding an agent to your own SAGE (that is the **Agents** section), and it is not the same as joining more computers on your own LAN into one shared brain (that is the node-join flow under **Connect an AI tool**, which makes another computer a peer node on your own network).
- **Read-only recall exchange.** When your SAGE queries the other side, the answers come back tagged with where they came from and are shown to you in the moment. **They are never written into your own brain.** Nothing foreign is stored on your chain. Turn the link off and those borrowed answers simply stop arriving. (See `internal/federation/proxy.go` - foreign results are "merge-in-response only", never persisted.)
- **It deletes nothing.** Connecting adds a small treaty record. It does not touch, move, or erase a single memory on either side. The only thing that undoes a connection is turning it off (a revoke), and even that erases no memories - it just stops the two networks from reaching each other.

Every connection is one-directional in the sense that each side keeps its own treaty record. You grant what **they** can read from **you**; they grant what **you** can read from **them**. Neither side can quietly widen the other's access.

---

## Before you start

- Both people need their SAGE running and reachable at an `https://host:port` address (the federation listener, usually port **8444** on your network). The wizard fills in a sensible default like `https://192.168.1.20:8444` - change it if that is not how the other person reaches you.
- You will each need a camera, or a shared screen, or at least a phone call you placed to a number you trust. The connection is safest when you are in the same room or on a video call you started.
- Decide roughly what you are willing to share (which topics, and how sensitive) before you begin. You can leave it blank and share nothing, and you can change it later.

Open **Federation** in the sidebar. You will see two big choices:

- **Join someone's network** - they will show you a code to scan.
- **Let someone join mine** - you will show a code for them to scan.

One of you picks each. Below are both walkthroughs, step by step, exactly matching the screens.

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

Now the app shows **your** code (a QR). Hold it up to their camera, or send it for them to paste. Their SAGE scans this to check that the network calling itself "you" really is you. When they have it, press **They've got my code - next**.

### 4. Choose what you will share

Set what the host will be allowed to read from you:

- **Domains they can read** - a comma-separated list like `family, recipes`. Leave it blank to share nothing. Type `*` to share everything (use with care).
- **Highest level they can read** - a slider from 0 (lowest) to 4 (most sensitive). Anything above this ceiling is never served, even inside an allowed domain.

Press **Continue**.

### 5. Read your code out loud

The app shows a six-digit code. **Call the host and read it to them out loud.** Do not paste it - saying it is the point. This code proves the two of you are really connected to each other and not to someone sitting in the middle. The app waits here while the host types in what they heard.

### 6. Check the code they read you

When the host approves, the app shows the compare screen. **They will now read you a code.** Type exactly what you hear. The **Yes - connect** button stays dead until the number you typed matches the number your own SAGE computed.

- If it matches, you have both proven co-possession. Press **Yes - connect**.
- If it does not match, the app warns you in red. **Do not continue.** Hang up and call them back on a number you trust, then start over.

### 7. Connected

That is it. You will see "You're connected", a two-of-two meter filled in, and a reminder that nothing was deleted. Your new connection appears in the list on the Federation page.

---

## Walkthrough B - Let someone join mine (host)

Pick **Let someone join mine**.

### 1. Your network address

Enter the address the other person will use to reach you (default `https://your-host:8444`). Press **Show my connection code**.

### 2. Show your code, then scan theirs back

The app shows your connection code as a QR. Have the guest scan it with their SAGE app (a plain authenticator app can read it too, but the join is driven from SAGE). This is best done in the same room, or held up to a video call you trust.

Then the guest shows **their** code back to you. Scan it, or paste it. This is the step that pins down who the guest really is - the fingerprint you scan here is the anchor the whole connection is checked against.

### 3. Wait for their request

The app waits while the guest's SAGE reaches out to yours. When it arrives, you move on automatically.

### 4. Review who wants to connect and set their access

You will see something like *Someone using the name "their-network" wants to connect*, along with what **they** are offering to let you reach. Below that, set **what they can read on your network**:

- **Domains they can read** - blank shares nothing; `*` shares everything.
- **Highest level they can read** - the 0-4 ceiling.

Conservative by default. Press **Next: check their code**, or **Ignore** to walk away (which burns the request and shares nothing).

### 5. Check their code

**The guest will read you a code.** Type exactly what you hear. The approve button stays dead until it matches what your SAGE computed. This is the moment that proves it is really them.

- Match -> press **Yes, they match - approve**.
- No match -> press **No - stop**. The app tells you plainly: do not approve, nothing was shared, nothing was changed. Hang up and call back on a trusted number.

### 6. Read your code back to them

Now your SAGE shows a code for you to read **back** to the guest, out loud. Say it - do not paste it. This is the second half of the handshake (two of two). The app waits while the guest confirms on their side.

### 7. Connected

When the guest confirms, you see "Connected", the two-of-two meter full, and the reminder that nothing was deleted. The connection is now in your list, and you can turn it off any time.

---

## The two codes, and why there are two

You spoke two different six-digit codes during the ceremony. That is on purpose - it is a **2-of-2** handshake:

1. The **guest reads a code to the host**, and the host types it. That is the host's "yes".
2. The **host reads a code back to the guest**, and the guest types it. That is the guest's "yes".

Neither side is connected until both "yes" steps happen. The little two-of-two meter on screen counts them: 0, then 1, then 2. Only at 2 is the link live on both ends.

Under the covers each code is a short **time-based one-time code (TOTP, RFC-6238)** computed from the secret the two sides established during the scan. Because the code is derived from that secret, the numbers only match when both sides genuinely hold the same secret - that is what a match proves (co-possession). What it cannot prove is that the secret reached the *right* person; that is what the human scan-and-compare is for, and why the next section matters.

---

## Scope controls - what actually crosses the link

Two settings decide what the other side can ever read from you. They are enforced on **your** SAGE, when it serves a query, so a peer cannot talk its way past them (`internal/federation/server.go`).

- **Allowed domains.** A query for a domain that is not on your list gets nothing. Blank means you share nothing at all - the connection still authenticates and stays healthy, it just serves no memories. `*` means every domain.
- **Max clearance ceiling.** A number from 0 to 4. Any memory classified above your ceiling is hidden, even if its domain is allowed. A foreign network has no standing inside your organization, so it can never read above this ceiling no matter what - there is no grant that widens it.

You set your own ceiling for them; they set their own ceiling for you. The two are independent.

---

## Turning a connection off

Open **Federation**. Each connection is a row with a status dot (green when active and unexpired) and the shared domains. Active connections have a **Turn off** button.

Press it and confirm. This:

- Broadcasts a revoke on your chain (an on-chain "this treaty is over").
- Clears the shared secret and the peer's cached certificate from your node, so a future re-connection starts clean.

**It erases no memories.** It only stops the two networks from reaching each other. If you ever want the link back, just run the join ceremony again.

The other side keeps its own record until it revokes too - turning off your side stops you serving them and stops your queries counting against their treaty, but each network owns its own half.

---

## The honest security model

Read this part. It is short and it matters.

**The human check is the anchor.** The one thing that actually proves you are connecting to the right network - and not an impostor in the middle - is the moment a person scans a code held as a physical object, in the same room or on a video call they placed, or reads a code out loud on a phone call to someone they trust. That human step is the root of trust. Everything else hangs off it.

**The six-digit codes prove two things, and only two things:** that both sides hold the same shared secret (co-possession), and that a human on each side said "yes" (2-of-2 consent). **They do not prove the secret reached the right person.** If you scanned a code off a stranger's forwarded screenshot, or read your code to someone impersonating your friend, the codes will still match - because you and the impostor now share a secret. That is why the app keeps pushing you toward in-person or a call **you** placed, and why the spoken-code path is never presented as equally safe. The codes are a seal on a decision **you** make; they are not the decision.

So: only ever connect when you are confident, by your own eyes or your own ears on a trusted line, that the other end is who you think it is. If a compare screen ever shows a mismatch, stop. Do not approve. Hang up and call back on a number you trust.

**What the link can and cannot do once connected:**

- It can serve read-only recall within the domains and clearance ceiling you set.
- It cannot write anything into your brain. Borrowed answers are shown, tagged with their source, and never stored on your chain.
- It cannot delete anything, on either side.
- It cannot read above your ceiling or outside your allowed domains.

---

## Troubleshooting

**"Host CA does not match the scanned code (possible tampering) - stop."**
The certificate the host served did not match the fingerprint in the code you scanned. Treat this as a real warning - someone may be sitting between you. Do not retry blindly; re-scan the real code in person or on a trusted call.

**"Host has not scanned your connection code yet."**
As the guest, you got ahead of the host. The host must scan your return code (step 2 on their side) before your request can bind. Wait for them, then continue.

**The compare screen shows a mismatch (red warning).**
The codes did not line up. This is the safety check doing its job. Do not continue. Hang up, reach the other person on a number you trust, and start the ceremony over.

**"Your side is connected but the host has not confirmed yet."**
You (the guest) finished, but the host's final step has not landed. This is a safe, one-sided window - the host cannot query you until their side is live. Give it a moment; the ceremony retries. If it never completes, turn off your half from the Federation list and start again.

**The session expired / "join session not found or expired."**
A join has to finish within about 15 minutes. If you left it sitting, the session times out for safety. Start over from the Federation page - nothing was created.

**"Refusing self-federation."**
You tried to connect a SAGE to itself (same network id). Federation is between two different networks.

**"Too many join attempts."**
The listener rate-limits repeated attempts from the same connection. Wait a minute and try again.

**A connection shows as expired.**
Some treaties carry an expiry. An expired row no longer serves or queries. Turn it off and re-run the join to refresh it.

---

### Under the hood (optional)

The whole ceremony runs off-consensus over a dedicated mutually-authenticated federation listener; the only things ever written to a chain are each operator's own treaty record (a set on connect, a revoke on turn-off). The operator wizard routes live at `/v1/dashboard/federation/join/*` (`web/federation_join.go`), the peer-facing ceremony at `/fed/v1/join/*` (`internal/federation/join_routes.go`), and the scope enforcement on served recall at `internal/federation/server.go`. Foreign results are never persisted (`internal/federation/proxy.go`).
