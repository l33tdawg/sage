# Org-introduced federation links (RFC #443, Phase A + B)

Status: Phase A implemented (v11.25.8). Phase B1 implemented (§5). Phase B2a
implemented (§6). Phase B2b implemented (§7). Phase B2c implemented (§8).
Contract
agreed on #443 (v2 + the two implementation points of 2026-11-12). Line refs
are main @ 506eb377 (v11.25.8).
Baseline behaviour is pinned by `TestRFC443Baseline_*` (posted on #443).

## 0. Scope

**Phase A** is data and admission only. Nothing new activates a link. It ships
in two PRs:

- **A1 (merged, #553):**
  1. Org policy schema `sage.org-policy.v2`: `PeerAllowList` = enrollee
     `agent_pub` (64 lowercase hex). The canonical cap goes 16 KiB → 96 KiB, and
     the policy request cap 64 KiB → 256 KiB. The accepted policy body is now
     stored with its high-water mark, so eligibility reads the exact signed list.
  2. Link card `sage.org-link-card.v1`: the pure signed type and its
     validation (§1.2).
  3. Roster `sage.org-roster.v1` (org-signed), `POST`/`GET /v1/org/{org}/roster`,
     and a roster store with `AdmitWithAuthority` semantics plus card checks.
  4. `PeerEligible`: the single eligibility decision for later callers. Nothing
     calls it yet.
- **A2 (merged, #556):** the node-self-mint card route (§1.2, decided on
  #443), and the manual QR JOIN gate with the Root-signed override (§1.5).

**Phase B** adds the owner envelope, offers, intro handshake routes, the
`introduced_link` record and the intent state machine (§3), and the
`ActiveAgreement` hook. Only `ask` mode. It ships in two PRs:

- **B1 (this PR, §5):** data and local decisions. The owner envelope, the
  deterministic offers, the `introduced_link` table with its CAS transitions,
  the fail-closed `ActiveAgreement` / `ActiveAgreements` hook, and the
  envelope cap on `UpdateAgreementSharingAs`. Nothing signs, broadcasts or
  activates a link: no code path writes an `introduced_link` row yet.
- **B2:** activation. `E_intro` and the acceptance signatures, the
  `/fed/v1/intro/*` routes, owner accept, the §3.3 write path, the §3.4
  reconciler, the §3.5 crash and stale tests, and applying the pauses B1
  only reports.

## 1. Phase A objects

### 1.1 Policy v2

- `Schema = "sage.org-policy.v2"`. Validation for v2:
  - every allow-list entry is exactly 64 chars `[0-9a-f]` (hex-decodes to 32 bytes);
  - no duplicates;
  - at most 1024 entries.
- v1 keeps verifying byte-for-byte as today, so already accepted chains stay
  valid. But a v1 policy with a non-empty `PeerAllowList` grants **no** peering
  eligibility: `AllowsPeer` returns false with a reason that names v2, for the
  caller to surface.
  `TestRFC443Baseline_V1AllowListAcceptsChainIDsAndFreeText` records the
  before-state.
- `maxCanonicalBytes` goes 16 KiB → 96 KiB (`policy.go:47`). 1024 v2 entries
  measure 70,016 B canonical (`TestRFC443Baseline_AgentPubAllowListVsCanonicalCap`).
- The policy POST cap goes `1<<16` → `256<<10` (`org_policy_handler.go:103`).
  It is enforced by `http.MaxBytesReader` before `json.Decode`, as today, and an
  over-cap body now answers 413 instead of 400 (`decodeCappedJSON`).
- A new pure helper `PeerEligible(accepted AcceptedState, body *Policy, agentPub string, now int64) (bool, reason)`
  is the one function every peering path calls. It checks `Gate(OpNewPeering)`,
  that the body *is* the accepted policy (org, revision and digest), the v2
  schema rule, and allow-list membership.

### 1.2 Link card

Fields:
- `org_id`, `agent_pub`, `chain_id` (federation's chain-id pattern, narrowed to
  the 50-byte consensus bound)
- `ca_spki_pin` (32 B)
- `endpoint` (≤ 255 B, `https://host:port` with an explicit port, no path, query
  or userinfo)
- `p2p_peer_id` and `p2p_routes` (≤ 8)
- `card_rev` (uint64)
- `issued_at`, `expires_at` (window ≤ 30 d)

Encoding and signing:
- Canonical length-prefixed encoding with domain tag `sage-org-link-card-v1\x00`.
- Signed by the node's own agent key: `sig` verifies under `agent_pub`.
- Canonical form ≤ 4 KiB.

**Card route (A2): node-self-mint.** Decided on #443: each enrolled node mints
and signs its own card; there is no issuer-side enrollee registry.

- `POST /v1/org/{org}/card` (node operator; after app-v23 the current Root or a
  current local Admin). The body carries only `card_rev`, `issued_at`,
  `expires_at` and `endpoint`. `issued_at` must be within 300 s of the node's
  clock. The request cap is 16 KiB, checked before decode.
- `agent_pub` (the node's JOIN transport key, the same key peers see as
  `host_agent_id` / `guest_agent_id`), `chain_id`, `ca_spki_pin`, `p2p_peer_id`
  and `p2p_routes` all come from local runtime state, never from the request.
  The endpoint is the one operator-supplied field, the same value the operator
  gives `HostCreate`; it passes the JOIN endpoint rules and the card rules. An
  empty endpoint is allowed only when a p2p route is ready (p2p-only card).
- The node must hold an accepted policy for the org. The write is conditioned
  on those exact accepted-policy bytes, so a rotation in between refuses it.
- Signing is deterministic, so an identical retry has the same digest and is a
  no-op. Changing anything at the same `card_rev` is card equivocation.
- `GET /v1/org/{org}/card` serves the node's current card and any notice to
  whatever builds rosters.

Store, keyed per `agent_pub`:
- a higher `card_rev` replaces the old card;
- the same rev with the same digest is a no-op;
- the same rev with a different digest is equivocation: refused, and a durable
  notice is kept;
- a lower rev is refused.

The card route and roster delivery write the same per-`agent_pub` record
(`card/{org}/{agent_pub}`). The card floor (the highest `card_rev` per `agent_pub`) is kept in the node's
roster store (§1.4), so a later roster can never regress a card. The
`EnrollmentOutcome` schema and replay behaviour don't change.

### 1.3 Roster

Fields:
- `org_id`, `scope`, `revision`
- `issuer_key_id`, `key_epoch`
- `issued_at`, `not_before`, `expiry` (window ≤ 7 d)
- `cards[]` (≤ 1024)

Canonical form ≤ 1 MiB. It's signed by the org key, with authority from the same
pinned root and rotation chain as the policy.

Admission is exactly `AdmitWithAuthority` (`chain.go:211`): verify first, then
rollback / no-op / equivocation / apply. On top of that:
- every embedded card must verify under its own `agent_pub`;
- `agent_pub` and `chain_id` must each be unique;
- no card may have a `card_rev` lower than the highest one already accepted for
  that `agent_pub`.

One bad card rejects the whole roster: there is no partial apply.

`POST /v1/org/{org}/roster`: request cap 2 MiB (1 MiB canonical, plus JSON
framing, hex signatures and rotation attestations). It's checked before decode,
and the authenticated-request middleware raises its 1 MiB default to 2 MiB for
exactly this route. The roster signer is resolved exactly like policy delivery:
the accepted policy's trust root plus its rotation chain (`resolveOrgSigners`,
shared by both handlers). Roster scope must equal the accepted policy scope.
Admission does not check freshness, just like policy; staleness is enforced
where it matters (offers, Phase B) through `CurrentAt`.

### 1.4 Stores

All Phase A state lives in node-local SQLite through the existing `orgpolicy.KV`
bridge (`store.go:47-77`), with one compare-and-set transaction per admission.
Nothing goes in Badger. Restart reloads all of it, which is tested. The keys:
- `body/{org}`: the accepted policy body. It's written in the same transaction
  as `accepted/{org}`, on enrollment and on every applied update.
- `roster/{org}`, `roster-body/{org}`, `roster-hist/{org}/{rev}`: the roster
  high-water mark, body and history.
- `card/{org}/{agent_pub}`: the card floor.
- `roster-notice/{org}`: roster and card equivocation notices.

A node that accepted its policy before bodies were stored has no `body/{org}`.
`LoadAcceptedPolicy` then returns no body, so `PeerEligible` fails closed. An
identical re-delivery of the same revision (a no-op) backfills the body. That
write is conditioned on `accepted/{org}` being unchanged and the body absent, so
it can't extend expiry or authority. A stored body whose digest, org or revision
doesn't match the high-water mark is never returned.

### 1.5 Manual QR JOIN gate

A node is *fleet-managed* if it has a stored `EnrollmentOutcome` (found by
listing `orgpolicy/enroll/` in the node-local store). On such a node, JOIN is
"new peering". The peer must be eligible under **every** org the node is
enrolled in: `PeerEligible` = a current policy for `OpNewPeering`, the stored
body is the accepted policy, and its v2 allow-list names the peer's agent key.

federation stays independent of where policy lives: the REST layer installs a
`JoinPeerGate` (`Manager.SetJoinPeerGate`) backed by the org policy store.

| Side | Check | Where |
|---|---|---|
| host | guest `agent_pub` | at `/fed/v1/join/request`, before any guest material is staged |
| host | guest `agent_pub` | at `HostApprove`, before the transcript is frozen |
| host | guest `agent_pub` | in `hostConfirm`, under `LockAgreementMutation`, before the host's tx-33 |
| guest | host `agent_pub` | in `GuestRequest`, as soon as the host's key arrives (the QR does not carry it, so scan can't check) |
| guest | host `agent_pub` | in `GuestConfirm`, under the lease, before the guest's tx-33 |

A refusal returns `fleet JOIN refused: <reason>` (403 on the peer route) and
stops before that side's tx-33. Any storage error refuses. A node with no
enrollment, or no org policy backend, sees no change.

**Override.** When the peer is not eligible, a current override can let one
peer through. `sage.join-override.v1` fields:
- `peer_agent_pub`, `peer_chain_id`, `reason` (≤ 512 B)
- `revision` (monotonic per peer), `issued_at`, `expires_at` (window ≤ 30 d)
- `root_credential_id` and `sig`: signed by the node's current Root
  credential (after app-v23 the committed CEREBRUM Root; before it the node
  operator key).

`POST /v1/federation/join-overrides` stores one (16 KiB cap). Authority comes
from the Root signature, which is checked against the current Root at store
time and again at every use, so a Root rotation retires every override the old
Root signed. The HTTP caller must also be that current Root through a loopback
request, and only a node holding at least one `EnrollmentOutcome` may store or
list overrides. Same revision and digest is a no-op; a lower revision, or the
same revision with other content, is refused. Each use (side, stage, session,
time) is appended to the record under compare-and-set before the JOIN
continues; the last 256 uses are kept plus a total count.
`GET /v1/federation/join-overrides` uses the same local-Root boundary and lists
them with a `usable` / `expired` / `not_current_root` status. Overrides and
policy-eligible peers are separate: an eligible peer never consumes an override
use.

`TestRFC443Baseline_QRJoinIgnoresPeerAllowList` is flipped to
`TestRFC443Baseline_QRJoinEnforcesPeerAllowListOnFleetNode`.

## 2. Request and response caps

Every route has its own cap, enforced before decode. Server side uses
`http.MaxBytesReader`. Client side wraps the response in `io.LimitReader(cap+1)`
and refuses anything over the cap. None of them reuse `joinBodyCap`.

| Route | Request | Response |
|---|---|---|
| `POST /v1/org/{org}/policy` | 256 KiB | 16 KiB |
| `POST /v1/org/{org}/roster` | 2 MiB | 16 KiB |
| `POST /v1/org/{org}/card` | 16 KiB | 16 KiB |
| `POST /v1/federation/join-overrides` | 16 KiB | 4 KiB |
| `POST /v1/org/{org}/envelope` | 64 KiB | 4 KiB |
| `POST /v1/org/{org}/offers/{offer_id}/reoffer` | 1 KiB | 4 KiB |
| `POST /fed/v1/intro/request` (B) | 32 KiB | 32 KiB |
| `GET /fed/v1/intro/{offer_id}/accept` (B) | — | 8 KiB |
| `POST /fed/v1/intro/confirm` (B) | 8 KiB | 8 KiB |
| `GET /fed/v1/intro/{offer_id}/status` (B) | — | 4 KiB |

The intro handshake never carries the roster or full cards. Both nodes already
hold them through push, so E_intro binds only revisions and digests. A digest
mismatch is a retryable `roster_mismatch` / `policy_mismatch`. That keeps the
handshake bodies small even though the roster can be 1 MiB.

## 3. Intent → broadcast → bind/reconcile (Phase B)

The `introduced_link` SQLite row and the tx-33 broadcast are separate commit
domains. So activation is a durable state machine, not one atomic write.

### 3.1 Record

`introduced_link` is a SQLite table. Its columns:
- `link_id` (PK)
- `remote_chain_id`
- `offer_id`, `session_id`
- `kind` (`new` | `rekey`)
- `state`
- `e_intro` (canonical bytes)
- `host_accept_sig`, `guest_accept_sig`, `guest_ack_sig`
- `evidence`: policy/roster/card/envelope rev + digest, both sides
- `terms`: the exact `CrossFedTerms` to commit
- `prior_terms_digest`: chain terms for `remote_chain_id` when the intent was
  written, or `absent`
- `tx_bytes`, `tx_hash`, `tx_nonce`: NULL until signed
- `generation`: the agreement generation `terms` commits, derived by the store (§5.3)
- `staged_ref`: the staged CA + seed (required, bounded)
- `created_at`, `deadline`
- an append-only `introduced_link_history(link_id, at, from, to, reason)`

A partial unique index on `remote_chain_id WHERE state NOT IN
('aborted','revoked','active','paused')` allows **at most one in-flight intent
per remote chain**. That's what rules out a second agreement generation.

### 3.2 States

| State | Meaning | Next |
|---|---|---|
| `intent` | evidence, both acceptance sigs and terms are durable; nothing signed | → `signed`, or → `aborted(stale)` |
| `signed` | exact tx-33 bytes and hash are durable; broadcast may or may not have happened | → `committed` / `aborted` / `paused`, via §3.4 |
| `committed` | these bytes are in a block with code 0 | → `bound` (host) / `bound_pending_peer` (guest) / `revoked(bind_failed)` |
| `bound_pending_peer` | guest: local artifacts committed, host hasn't confirmed | → `active`, or → `revoked(peer_not_confirmed)` |
| `active` | the only state where `ActiveAgreement` passes | → `rekey_required` / `paused` / `revoked` |
| `paused` | agreement kept, no traffic; owner action needed | → `active` (§5 rules of v2) / `revoked` |
| `aborted(reason)`, `revoked(reason)` | terminal | — |

`ActiveAgreement` (`manager.go:841`) gets one more fail-closed check, after the
existing ones: if an `introduced_link` row is bound to the agreement's current
generation (§5.3) and the newest such row's state isn't `active`, it denies.
`ActiveAgreements` applies the same filter. Manual agreements have no row for
their generation and are unchanged.

### 3.3 Write path (both sides, under `LockAgreementMutation`)

1. **Re-check under the lease.** This is the *stale-intent gate*:
   - the offer isn't expired;
   - the policy/roster/card/envelope revisions are still the ones in the evidence;
   - `PeerEligible` holds;
   - the envelope still covers the peer;
   - the current chain terms for the remote hash to `prior_terms_digest`;
   - there's no other in-flight row.

   Any failure gives `aborted(stale: <which>)`. Nothing is signed.
2. **Stage the artifacts.** The peer CA goes through `StageRemoteCA` (`trust.go:100`).
   The pair seed goes to a staged 0600 file next to it, using the same
   stage/commit/rollback pattern. These survive a restart; in-memory seed copies
   are zeroized.
3. **Write `intent`.** One SQLite transaction holds the row, the history entry and
   `staged_ref`.
4. **Sign inside `tx.WithNonceLease`.** This is the existing
   `broadcastCrossFedSetLockedAs` (`join_routes.go:1734`), split so it builds and
   signs the tx-33. Then:
   - compare-and-set `intent → signed`, storing `tx_bytes`, `tx_hash`
     (= SHA-256 of the bytes) and `tx_nonce`;
   - only then call `broadcastContext` with **those bytes**.

   A crash before the CAS leaves `intent` with no bytes, and those bytes never
   left the process.
5. **Broadcast result:**
   - code 0 → CAS `signed → committed`;
   - CheckTx or FinalizeBlock rejection → `aborted(rejected: code)`, staged
     artifacts rolled back;
   - `tx.ErrSubmitIndeterminate` → stays `signed`. The existing signer fence goes
     up as today. The row is handed to the reconciler (§3.4).
6. **Bind** (`committed`). Commit the staged CA and seed, then
   `ActivateSyncControl` and the peer RBAC init, in today's order. Each step is
   idempotent. Then:
   - host → `active`;
   - guest → `bound_pending_peer`, then it sends `IntroConfirmWire`.

   If a bind step fails, tx-34 goes out (`revokeAgreementLockedAs`), the staged
   artifacts are rolled back, and the state becomes `revoked(bind_failed)`. That
   matches today's `undoPartialHostConfirmLocked` (`join_routes.go:666`).

**Guest one-sided window.** This is the 7.3b finding: today the guest is active
before the host confirms (`join_routes.go:1611`). An introduced guest instead
stays `bound_pending_peer`, and `ActiveAgreement` denies it. It becomes `active`
when one of these returns `active`:
- the host's `IntroConfirmWire` response;
- polling `GET /fed/v1/intro/{offer_id}/status`.

If the host refuses, or the deadline passes (`deadline` = intent time +
`JoinConfirmationOperationTimeout`, retried until the offer's 72 h expiry), the
guest broadcasts tx-34 and moves to `revoked(peer_not_confirmed)`. An introduced
link is never live on one side only.

### 3.4 Reconciler (start-up, then a maintenance tick)

It runs once at start-up and then on the existing one-minute `StartMaintenance` tick (`join_routes.go:1107`), takes
`LockAgreementMutation` for each row, and never broadcasts anything except the
stored bytes.

| Found after restart | Action |
|---|---|
| `intent` (no bytes) | The bytes never existed, so they can't have been broadcast. Roll back staged artifacts → `aborted(crash_before_broadcast)`. The offer goes back to `offered` if it hasn't expired. A new tap starts a new session; nothing is broadcast late. |
| `signed` | `tx.CometTxResolver(cometRPC)(ctx, tx_bytes)`. It checks the exact hash, then re-submits the **identical** bytes (same hash and nonce, so it can't create a second tx). See the verdict table below. |
| `committed` | Resume bind from the staged artifacts (idempotent). If the staged CA or seed is missing or corrupt → tx-34 → `revoked(bind_failed)`. |
| `bound_pending_peer` | Resume the host-status poll until the deadline, then follow the guest window rule above. |

Verdicts for a `signed` row:

| Verdict | Action |
|---|---|
| `TxVerdictCommitted` | Check the chain terms for `remote_chain_id` equal `terms`. If they do → `committed` → bind. If they don't (a later upsert) → `aborted(superseded_on_chain)`, chain untouched. |
| `TxVerdictRejected` / `TxVerdictSuperseded` | These bytes can never commit → roll back staged artifacts → `aborted(<verdict>)`. |
| `TxVerdictSpent` | The nonce floor can't tell whether *these* bytes landed. If the chain shows an active agreement for the remote with `terms` → `paused(reconcile_spent)`, and the owner is shown revoke/accept. Otherwise → `aborted(spent)`. It never auto-activates. |
| unresolved / error | Stays `signed`, and `ActiveAgreement` keeps denying. Retry with backoff. The owner can always revoke (tx-34 is safe whatever the fate). |

Every transition is a CAS on the expected `state` (and on `tx_hash` once signed),
so a concurrent tick or a late live call can't apply a transition twice.

### 3.5 Crash and stale tests (fault seams, real SQLite and Badger, Manager restarted on the same dirs)

1. **Crash before broadcast.** Kill after the `intent` write: restart →
   `aborted(crash_before_broadcast)`, no tx-33, no CA or seed on disk,
   `ActiveAgreement` denies. Kill after the `signed` CAS but before
   `broadcastContext`, with a resolver stub that re-submits and is rejected →
   `aborted`.
2. **Ambiguous commit.** `broadcastFn` returns `ErrSubmitIndeterminate`. The row
   stays `signed` and `ActiveAgreement` denies. Then the resolver gives:
   - `Committed` + matching terms → `active`;
   - `Rejected` → `aborted`;
   - `Spent` with matching terms → `paused(reconcile_spent)`;
   - unresolved → still `signed` after N ticks.

   In every case exactly one tx-33 exists (broadcast-counter + hash assertions).
3. **Crash after broadcast, before bind.** Commit, then kill before the CA commit:
   restart → bind completes from the staged artifacts → `active`. A variant
   deletes the staged seed first → tx-34 → `revoked(bind_failed)`.
4. **Stale-intent rejection.** Each of these gives `aborted(stale: …)` with zero
   broadcasts:
   - the offer expired between tap and lease;
   - the roster or policy revision moved;
   - the card was removed;
   - the peer left `PeerAllowList`;
   - the envelope narrowed;
   - the chain terms changed (`prior_terms_digest` mismatch);
   - a second in-flight intent for the same remote (unique index).

   A restored `signed` row whose chain terms were since upserted →
   `aborted(superseded_on_chain)`.
5. **Guest window.** The host refuses or times out → the guest broadcasts tx-34 →
   `revoked(peer_not_confirmed)`. `ActiveAgreement` denies throughout
   `bound_pending_peer`.

## 4. Phase A test plan

A1 implemented the policy, card type, roster, store, cap and eligibility items
below. A2 adds the card route and the JOIN gate items.

- Policy v2:
  - golden canonical bytes;
  - 1024 × agent_pub fits at 96 KiB, and 96 KiB + 1 is refused;
  - each v2 format rejection;
  - v1 with a non-empty list → `PeerEligible = false` plus a diagnostic.
- Policy POST: 256 KiB accepted, 256 KiB + 1 refused before decode (413).
- Cards:
  - signature under `agent_pub` required;
  - no accepted policy for the org → refused; fields from runtime state only;
  - higher / same / equivocation / lower rev;
  - restart keeps the highest rev and the notices.
- Roster:
  - every `AdmitWithAuthority` case;
  - a bad embedded card rejects the whole roster;
  - duplicate `agent_pub` / `chain_id`;
  - card regression;
  - 1 MiB canonical cap;
  - the 2 MiB request cap is checked before decode;
  - restart.
- JOIN gate:
  - fleet-managed and peer not eligible → refused on both sides, including the
    re-check under the lease (a policy swapped between request and approve);
  - eligible → JOIN completes;
  - valid override → completes and the use is recorded;
  - expired or non-Root override → refused;
  - not fleet-managed → unchanged (the existing ceremony tests stay green).
- `TestRFC443Baseline_*`: in A1, 7.6 (a v1 allow-list grants no eligibility)
  and the allow-list size test flipped. In A2, 7.1 (QR JOIN ignores
  `PeerAllowList`) flips to an enforcement test. The others stay as regression
  guards.

## 5. Phase B1: envelope, offers, record and hook

### 5.1 Owner envelope (`internal/orgpolicy/envelope.go`, `envelope_store.go`)

`sage.org-link-envelope.v1`, one per org per node. Fields:
- `org_id`, `org_root_key_id` (the accepted policy's `root_key_id`), `scope`
  (the roster scope it covers);
- `allowed_domains` (the tx-33 sharing rules: ≤ 64 tags of ≤ 512 bytes, no
  duplicates, `*` only alone; empty = trust-only links) and `max_clearance`
  (0–4);
- `mode` (`ask` only), `revision`, `prev_digest` (empty at revision 1,
  otherwise the stored envelope's digest), `issued_at` / `expires_at`
  (≤ 365 d), `root_credential_id`, `sig`.

It is signed by the node's current Root credential, stored only through
localhost by the current local Root (`POST /v1/org/{org}/envelope`), and
re-verified at every use against the Root current at that moment, so a Root
rotation retires it. Admission is conditioned on the exact accepted-policy
bytes:
- no stored envelope → revision must be 1;
- higher revision → `prev_digest` must chain to the stored digest;
- same revision and digest → no-op;
- same revision, other digest → refused, durable notice, governance event;
- lower revision → refused.

Every revision is kept in history.

**Covers** (the check offers and the sharing cap use): verifies under the
current Root, inside its window, an accepted policy exists for the org with the
same org root, and the accepted roster's scope equals the envelope scope.

**AllowsSharing:** the agreement's clearance ≤ `max_clearance`, and every
shared domain is covered by an allowed domain with the agreement-scope
semantics (`*` covers all, `eng` covers `eng` and `eng.public`, a `*` request
needs a `*` envelope).

### 5.2 Offers (`internal/orgpolicy/offer.go`)

Roles and `offer_id` follow contract v2 §5: host = lower `agent_pub`, and
`offer_id` = SHA-256 of `"sage/intro-offer/v1"`, org, host key, guest key, both
`card_rev`s and the kind. Strings are u32-length-prefixed and revisions are
big-endian u64. Card revisions come from the accepted roster's copies, which
both sides hold, so both compute the same id. A roster refresh that changes
neither card gives the same id.

`GET /v1/org/{org}/offers` (node operator) computes, per roster peer:

| Peer state | When |
|---|---|
| `offered` (kind `new`) | no active agreement for the peer chain, the peer is eligible (`PeerEligible` under this org) and its card is current |
| `none` | not eligible / card not current / own chain; `reason` says why |
| `manual` | an active agreement exists with no row bound to its generation, a terminal row, or a row introduced by another org |
| `in_flight` | the chain has an `intent` / `signed` / `committed` / `bound_pending_peer` row |
| `active` | the row bound to the agreement's generation is active and the peer is still covered |
| `rekey_required` (kind `rekey`) | active, but the peer card's CA pin differs from the agreement's |
| `pause_required` | active, but the peer is no longer covered, or the agreement's domains or clearance are outside the current envelope; `reason` says why |
| `paused` | the row bound to the agreement's generation is paused |
| `expired` | the local 72 h offer window passed |

Org-level preconditions come first (else `ready: false` with the reason): an
accepted policy, a current accepted roster that lists this node's card on its
own chain, and an envelope that covers. A row is read only under the org that
introduced it. Each node records when it first saw an
offer, so the 72 h window survives restart. `POST
/v1/org/{org}/offers/{offer_id}/reoffer` restarts the local window. B1 only
reports `pause_required`; B2 applies it under the lease.

### 5.3 Record and hook

`introduced_link` and `introduced_link_history` are node-local SQLite tables
(`internal/store/introduced_link.go`), never Badger. Columns are as in §3.1,
plus `org_id`, `peer_agent_pub`, `role` and `state_reason`.
- Each row is bound to one agreement generation: `generation` =
  `CrossFedGeneration` of the terms it commits, a SHA-256 over every chain term
  except the outbound `AllowedDomains` snapshot, the endpoint (since B2c, §8.3)
  and the status. Domains are left out because `UpdateAgreementSharingAs`
  rewrites them in place, and for an introduced agreement that rewrite is held
  inside the envelope. Any other change (pin, clearance, departments, expiry)
  is a new generation the row doesn't govern.
- The store derives `generation` from `terms` at intent, so the two can't
  disagree. `terms` must be the canonical tx-33 encoding of active terms for
  `remote_chain_id`; a caller-supplied `generation` that doesn't match is
  refused.
- `intent → signed` decodes `tx_bytes` and refuses them unless they are a
  validly signed cross_fed set tx whose terms are exactly the row's `terms`.
- The intent needs a non-empty `staged_ref` of at most 512 bytes: the CA and
  seed are staged before it is written.
- `InsertIntroducedLinkIntent` checks the fence inside its write transaction,
  and the partial unique index on `remote_chain_id` (in-flight states) backs it
  up.
- `TransitionIntroducedLink` enforces the §3.2 table. Out of `signed` and later
  states it is a CAS on the expected state and the stored `tx_hash`.
  `intent → signed` records the exact bytes, with `tx_hash` = SHA-256 of them.
  Terminal and paused states need a reason. Each transition appends history in
  the same transaction.

federation gets `SetIntroducedLinkSource` and `SetIntroducedSharingCap`, and the
REST layer installs both in `SetFederation`:
- `ActiveAgreement` denies when the newest row bound to the agreement's current
  generation isn't `active` (`ErrIntroducedLinkNotActive`).
  `ActiveAgreements` drops those chains.
- Rows for other generations of the chain are ignored. An older aborted attempt
  doesn't block a later manual agreement, and an old active row doesn't
  authorize a later tx-33 it never introduced.
- A storage error denies every agreement. Without the rows, a manual link
  can't be told apart from a paused introduced one.
- `UpdateAgreementSharingAs` on an agreement whose generation has a row calls
  the cap: that row's org envelope must cover and `AllowsSharing` must hold. If no cap is
  installed, the update is refused.
- Manual agreements and nodes with no node-local store are unchanged.

`GET /v1/federation/introduced-links` (node operator) lists rows and history. It
never echoes `e_intro` or `tx_bytes`.

## 6. Phase B2a: activation engine

B2a is the §3.3 write path, the §3.4 reconciler and the §3.5 tests. Nothing on
the wire calls it yet: the `/fed/v1/intro/*` routes, owner accept and the
host-status poll are B2b, so no link can activate from B2a alone.

### 6.1 Transcript and signatures (`internal/federation/handclasp_intro.go`)

- `IntroTranscript` is E_intro: each side's chain id, card `agent_pub`, CA
  pin, endpoint, nonce, scope digest, P2P peer id and routes, card and
  envelope revision + digest; plus org id, org root key id, policy and roster
  revision + digest (one value each), `offer_id`, kind, and the seed
  commitment. The seed itself is never in it.
- It's stored as canonical JSON (the `e_intro` column). Decoding refuses
  unknown fields, non-canonical order or whitespace, unsorted routes and
  trailing bytes, and the whole transcript is capped at 16 KiB.
- `Validate` enforces the §6 caps (8 routes, 255-byte endpoint, 50-byte chain
  id) and host = the lower `agent_pub`.
- The attestation is `SHA256("sage/handclasp-intro/E/v1" \0 canonical)`.
  `host-accept`, `guest-accept` and `guest-ack` are Ed25519 over
  `tag \0 E`, verified against the card `agent_pub`. None of them verifies
  as another, or as a manual-JOIN signature over the same bytes.

### 6.2 Write path (`internal/federation/introduced_activation.go`)

`ActivateIntroducedLink(ctx, IntroActivation)` takes one accepted session (the
transcript, all three signatures, the peer CA, the seed, the exact tx-33 terms,
the prior-terms digest, a deadline and a `StaleCheck`). Before anything else:

- all three signatures must verify over E_intro;
- this node's side must name the local chain, agent key and CA pin;
- the terms must name the peer's chain, endpoint and pin from the transcript,
  be `active` and trust-only, and match this side's scope digest;
- the seed must match the commitment;
- an introduced-link source must be installed, otherwise `ActiveAgreement`
  couldn't see the row (fail closed).

Then, under `LockAgreementMutation` and one generation mutation for the peer:

1. **Stale gate.** `StaleCheck` (the org-level checks: offer expiry,
   revisions, `PeerEligible`, envelope coverage), the chain terms against
   `prior_terms_digest`, and no in-flight row. A refusal is an
   `*IntroStaleError`; nothing is staged, written or signed, so no row is
   kept for it.
2. **Stage** the CA and seed (§6.3).
3. **Intent** row, then `PrepareSyncControl` (pending).
4. **Sign** inside the nonce lease. The `intent → signed` CAS stores the exact
   bytes and nonce from a hook between `EncodeTx` and `broadcastContext`, so
   the bytes never leave the process before they are durable.
5. **Broadcast result:**
   - success → `committed`;
   - `ErrSubmitIndeterminate` → stays `signed`, and `ErrIntroPending` hands it
     to the reconciler;
   - any other error → `aborted(rejected: …)`, staging rolled back and the
     pending sync control dropped.
6. **Bind.** The chain must hold exactly the row's terms. Then commit the
   staged CA and seed, `ActivateSyncControl`, persist the peer route, and
   install the deny-all peer RBAC snapshot. The host goes to `active`, the
   guest to `bound_pending_peer`.
   - The RBAC step resolves the peer operator from the agreement record
     (`initializePeerRBACPolicyFor`), because `ActiveAgreement` still denies
     the row here.
   - Any bind failure sends tx-34 and leaves `revoked(bind_failed: …)`.

B2a activated only kind `new`. Rekey landed in B2c (§8.4): it keeps the active
sync-control binding instead of preparing a new one.

### 6.3 Staging (`internal/federation/introduced_staging.go`)

- The CA and the seed envelope go under
  `<certs>/federation/<chain>/intro/<link_id>/`.
- `staged_ref` is `intro/<chain>/<link_id>`. It is compared against the row's
  own chain and link id, never used as a path.
- Commit moves both to the live files exactly as JOIN does. The seed epoch
  follows whatever is live at commit time, and a vault-locked node caches the
  seed at unlock.
- Commit is idempotent: with the staged files gone, it accepts a live CA and
  seed that already match the expected pin and pin-pair, so the reconciler can
  resume a bind interrupted half-way.

### 6.4 Reconciler (`internal/federation/introduced_reconcile.go`)

`ReconcileIntroducedLinks` runs once when `StartMaintenance` starts and then on
every tick, per row under the same locks, and follows §3.4:

- `intent` → `aborted(crash_before_broadcast)`.
- `signed` → the resolver (default `tx.CometTxResolver`) re-submits only the
  stored bytes, then:
  - committed with the chain holding the row's terms → bind; with other
    terms → `aborted(superseded_on_chain)`, chain untouched;
  - rejected, superseded or abandoned → `aborted(<verdict>)`;
  - spent with matching terms → `paused(reconcile_spent)`, never
    auto-activated; otherwise `aborted(spent)`;
  - unresolved → stays `signed`.
- `committed` → resume bind.
- `bound_pending_peer` → `active` once the host reports active (hook installed
  by B2b, or `GuestIntroConfirmed`). If the host reports aborted or revoked, or
  the deadline passes → tx-34 → `revoked(peer_not_confirmed)`.

### 6.5 Tests (`introduced_activation_test.go`, `handclasp_intro_test.go`)

- Two real nodes with Badger and SQLite. `restart()` closes and reopens both
  stores under a fresh Manager.
- A test-only fault seam (`introFaultFn`) stops the write path with no cleanup
  at `after_intent`, `after_signed` and `after_commit`.
- They cover every §3.5 case:
  - crash before broadcast, both variants;
  - ambiguous commit: committed, rejected, spent with and without terms,
    superseded, and unresolved after three passes, each asserting exactly one
    tx-33 and the stored hash;
  - crash before bind: resume, and a missing seed → tx-34;
  - the stale cases;
  - the guest window: the host refuses, the deadline passes, the host
    confirms.
- The org-level stale reasons (offer expiry, roster or policy revision, card
  removal, allow-list, envelope) reach the engine through `StaleCheck`. Their
  concrete checks are wired with the routes in B2b.

## 7. Phase B2b: the wire

B2b connects the B2a engine to the network. Two enrolled nodes of the same
org, each holding the same accepted policy and roster and an owner envelope
that covers the org, can now reach `active` on both sides from two owner
taps and no QR code. Only kind `new` is wired.

### 7.1 Owner accept (`api/rest/org_intro_handler.go`)

`POST /v1/org/{org_id}/offers/{offer_id}/accept` (current local Root over
loopback, 1 KiB cap) recomputes the offers, requires the offer to be open and
of kind `new`, and stores an `IntroAcceptance` (`internal/orgpolicy/intro_accept.go`)
bound to this node's current envelope revision and digest. A newer envelope
makes the acceptance stale until the owner accepts again. On the guest it
also starts the handshake driver; the host waits for the guest.

### 7.2 Routes (`internal/federation/intro_wire.go`)

| Route | Request | Response | Does |
|---|---|---|---|
| `POST /fed/v1/intro/request` | 32 KiB | 32 KiB | Guest nonce + evidence in; host nonce + evidence, the 32-byte seed, its commitment, the host CA and E_intro out. |
| `GET /fed/v1/intro/{offer_id}/accept?session_id=` | — | 8 KiB | `host_accept_sig` (200) once the host owner accepted under the current envelope; 202 while not. |
| `POST /fed/v1/intro/confirm` | 8 KiB | 8 KiB | Verifies guest-accept + guest-ack over E_intro, then runs the host's §3.3 write path. Idempotent: a replay reports the row. |
| `GET /fed/v1/intro/{offer_id}/status` | — | 4 KiB | The host's row state for the caller, else `awaiting_accept`, `awaiting_confirm` or `none`. |

- Each cap is checked before decode (`Content-Length`, then
  `MaxBytesReader`). Bodies decode strictly: unknown fields and trailing data
  are refused. The client reads every response through `LimitReader(cap+1)`.
- `policy_mismatch` and `roster_mismatch` are 409 with `retryable: true`.
- Rate limits share the JOIN per-connection budgets.

### 7.3 Pinning by roster card

- **Host side.** The guest presents its node certificate chained to its own
  CA. The federation handshake admits such a chain only when that CA's SPKI is
  the pin of a current roster card of another node, in a current roster of an
  org this node is enrolled in, and the leaf verifies against it
  (`introWindowAccepts`, next to the JOIN window in
  `verifyFederationClientCert`). Every route then requires that pin to be the
  pin of the offer's peer card, and the CA's CN to be the card's chain id.
- **Guest side.** The federation listener presents its own CA after its leaf
  (the CA is public; JOIN already serves it). On the first call the guest
  pins the server at handshake time from the host card alone: the chain must
  carry a CA whose SPKI is the card's pin and whose CN is the card's chain
  id, and the leaf must verify against it, else the handshake fails and no
  request is sent. As defence in depth the response's host CA must be that
  same CA. Later calls pin it as the root, and after bind the window probe
  uses the committed host CA.
- The guest rebuilds E_intro from its own view (its roster cards, policy,
  roster, envelope, both nonces, the host's envelope evidence and the seed
  commitment) and refuses the host's bytes unless they are identical.

### 7.4 Sessions and the seed

- Host sessions live in memory only, one per offer, bounded by the offer
  expiry and one hour. A restart loses them, and the guest driver starts a
  new session (new nonces, new E_intro, new seed). Nothing durable is lost:
  no row exists before both acceptances.
- The seed is generated on the host inside the pinned session and is never
  written to disk before B2a stages it with the intent. The host zeroizes it
  when its write path returns, when the session is replaced, expires or is
  dropped as stale; the guest zeroizes its copy when its attempt returns.
- Each host session has its own lifecycle lock and state (active →
  confirming → done, or retired). A confirm takes ownership and works on its
  own copy of the seed. While it owns the session a replacing request and a
  second confirm are refused (409, retryable), and a drop or an expiry sweep
  only flags the session; the confirm then retires and zeroizes it when it
  finishes. Nothing waits on a session while holding the session map lock.

### 7.5 Guest driver and window (`internal/federation/intro_guest.go`)

- After the owner accepts, the driver runs request → poll accept → verify
  host-accept → sign guest-accept + guest-ack → `ActivateIntroducedLink(guest)`
  → confirm. A confirm answer of `active` moves the row to `active`.
- Transient failures retry with backoff while the offer is open and the
  acceptance current; a closed offer, a stale acceptance or an activation
  failure stops the run. The maintenance tick restarts drivers for accepted,
  open offers, so a guest restart resumes with a fresh session.
- The B2a `bound_pending_peer` window now asks the host
  (`GET …/status`). While the host still waits for confirm (a lost confirm),
  the probe re-sends the stored guest signatures. `active` → active;
  `aborted`, `revoked` or `none` (the host has no session or row for ours, so
  it can never confirm) → tx-34 → `revoked(peer_not_confirmed)`; anything else
  waits for the deadline, which is 15 minutes after the guest activates.

### 7.6 StaleCheck

The engine's `StaleCheck` re-reads the offer under the agreement lease and
refuses unless the offer is still open (not expired, peer eligible, envelope
covering, no agreement), the policy and roster revision + digest, the org
root, both card revisions + digests and this node's envelope revision +
digest still equal E_intro's, and the owner acceptance names the current
envelope.

### 7.7 Deferred to B2c

Pauses, rekey and stale refusals: all three landed in B2c (§8).

### 7.8 Tests

- `intro_wire_test.go`: two real nodes serving their real routers over mTLS.
  End to end to `active` on both (one tx-33 each, the same seed committed on
  both, the host seed zeroized, a replayed confirm is a no-op); the host owner
  accepting later; an acceptance under an old envelope never signs; policy and
  roster mismatch; oversized bodies refused before decode; unknown fields and
  trailing data; expired and closed offers; a caller that is not the offer's
  peer card; a stranger CA failing the handshake; bad guest signatures; a
  host card pin that is not the server's CA, and another node answering at
  the host's endpoint, both failing the guest's first request at the
  handshake; a stale session at confirm; replacement, drop and expiry
  racing an in-flight confirm (scripted and unscripted, under `-race`); a lost confirm
  finished by the window's status probe; the window revoking when the host
  lost its session; restart of either side mid-handshake.
- `org_intro_handler_test.go`: owner accept (Root over loopback only, open
  offers only), the org source view, pin lookup, acceptance rebinding after a
  new envelope.
- `intro_accept_test.go`: acceptance store and validation.

## 8. Phase B2c: pauses, rotation, rekey, refusals

B2c applies what B1 only reported, follows endpoint and route changes of a
peer card, rekeys a link when a CA pin moves, and records stale refusals. The
contract is v2 §4 (rotation split) and §5 (states).

### 8.1 Policy pass (`internal/federation/introduced_policy.go`)

`ApplyIntroducedLinkPolicy` runs at start-up, on every maintenance tick, and
after any applied org admission (policy, roster, envelope, card mint; the REST
layer calls `NudgeIntroducedLinkPolicy`). For the governing row of each
introduced agreement (the row `IntroducedLinkForGeneration` returns for the
chain's current terms) it takes `LockAgreementMutation` and one sync-policy
generation mutation, re-reads the row, and applies:

| Condition (from `IntroOrgSource.IntroOrgPeers`) | Row |
|---|---|
| org not ready: no accepted policy, roster not current, envelope missing, expired or not covering | `active`/`rekey_required` → `paused(org not ready: …)` |
| the peer card is no longer in the roster | → `paused` |
| the peer card names another chain id | → `paused` (a chain id change is refused) |
| `pause_required`: peer left `PeerAllowList`, card expired, envelope no longer allows the agreement's sharing | → `paused(reason)` |
| `active`, a CA pin moved on either side (§8.4) | → `rekey_required` |
| `active`, endpoint- or route-only card change | followed in place (§8.3) |
| `paused`, resumable, and the card digest is the one the link last followed | → `active` |

Pausing needs no tx: `ActiveAgreement` denies a generation whose governing row
isn't active, so traffic stops at once, and resuming is the reverse CAS. An
org whose state can't be read leaves its rows as they are.

`paused` → `active` without a tap needs all of:
- the peer is covered again (`PeerEligible`, card current);
- the envelope still allows the agreement's domains and clearance;
- neither CA pin moved (the peer's against the agreement, this node's against
  the pin it presented in E_intro);
- the peer card digest equals the one the link last followed.

Anything else needs a new ask. orgpolicy reports the first three as
`resumable` on a `paused` offer row, with a reason when it isn't; the pass
checks the digest.

### 8.2 Own-pin rekey signal (`internal/orgpolicy/offer.go`)

`PeerAgreement.OwnPinHex` is the pin this node presented in the governing
row's E_intro. A rekey is due when the peer card pin differs from the
agreement's pin (as in B1) or this node's own card pin differs from
`OwnPinHex`. Without the second check only the peer of the node whose CA
rotated would ever offer the rekey, and the handshake needs both sides. Both
sides compute the same kind-`rekey` offer id from the same cards.

### 8.3 Rotation split

- Endpoint-only change (same pin, agent key and chain id): a tx-33 upsert of
  the current terms with the new endpoint, under the lease. `CrossFedGeneration`
  no longer covers the endpoint, so the upserted agreement keeps its
  generation and the same active row keeps governing it. An endpoint is only
  where to dial; trust is the pin.
- Route-only change: no tx. The route refresher re-exchanges routes.
- Either way the link records the card it now follows in
  `introduced_link_card` (revision + digest) with a history entry
  (`card_rotated rev N: endpoint|routes`). A card with a lower revision than
  the followed one is ignored.
- A pin change goes to `rekey_required`; a chain id change pauses.

The generation change is safe to make now because no production path wrote
an `introduced_link` row before B2b.

### 8.4 Rekey

- Offers: kind `rekey` in state `rekey_required` is an open offer. Owner
  accept takes it like a new one, and the wire runs the same handshake with
  E_intro kind `rekey`.
- Terms: the chain's current terms for the peer with only the pin and endpoint
  replaced from the peer's transcript side. Sharing, clearance, departments
  and expiry carry over, so a rekey never widens or narrows what the owner
  granted, and the envelope keeps capping it. The trust-only and scope checks
  apply to kind `new` only.
- Extra stale gate under the lease (`introRekeyGate`): the chain holds an
  active agreement governed by an `active` or `rekey_required` row of the
  same org and peer key, and the terms are exactly the carry-over terms.
  Otherwise it's a stale refusal (no agreement, a manual or other-org
  agreement, a paused link, terms that don't carry over).
- Write path: the same intent → signed → committed → bind machinery, with the
  same fault points and reconciler. A rekey skips `PrepareSyncControl`.
- Bind:
  - the staged CA replaces the live peer CA;
  - the seed commit rotates the epoch (the current seed becomes the
    previous one);
  - `RekeySyncControlPin` moves the active binding's pinned CA, keeping its
    epoch, controller and delivered policy;
  - the peer RBAC snapshot and routes are kept, and a route refresh is
    triggered;
  - the rows the rekey replaced go to `revoked(superseded_by_rekey: <link>)`.
- One agreement: the tx-33 upserts the same remote chain id. On the side
  whose peer's pin moved, that's a new generation. On the side whose own CA
  moved, the terms keep the peer pin, so the rekey row shares the generation
  of the link it replaces.
- Governance: `IntroducedLinkForGeneration` and the `ActiveAgreements` filter
  skip an aborted rekey row. It never put bytes on chain, so it must not take
  over a shared generation from the link it failed to replace. An aborted
  kind-`new` row keeps governing as before.
- Guest window: unchanged. A guest rekey that the host never confirms is
  revoked (tx-34) at the deadline, like a new link.

### 8.5 Stale refusals

A stale-gate refusal still writes no `introduced_link` row. It's appended to
`introduced_link_refusal` (org, offer, remote chain, kind, role, reason, time;
bounded to 1024 entries, oldest pruned), and `GET
/v1/federation/introduced-links` returns them as `refusals`. Plan errors that
never reach the gate aren't recorded.

### 8.6 Decisions for review

1. The endpoint left `CrossFedGeneration` (§8.3). The alternative, a new row
   per endpoint change, would need a consent record nobody signs.
2. Rekey terms carry the current sharing over rather than resetting to
   trust-only (§8.4).
3. Org not ready pauses every link of the org, including a roster that merely
   lapsed past its window. That's fail-closed; resume is automatic once a
   current roster with the same card arrives.
4. A pin change while paused needs a new ask (no rekey from `paused`).
5. A guest-side rekey that times out revokes the agreement, as a new link
   does.

### 8.7 Tests

- `introduced_policy_test.go`:
  - every pause trigger, each followed by an automatic resume;
  - resume refusals (not resumable; card changed while paused);
  - an unreadable org leaves rows alone;
  - the rotation split: endpoint upsert still governed, route-only, an older
    card ignored, a pin change to `rekey_required`, `rekey_required` →
    `paused`;
  - rekey binding the new pin with one upsert and no second agreement;
  - crash after commit resumed by the reconciler;
  - crash before broadcast keeping the old link;
  - the same-generation guest case (an aborted rekey hands governance back,
    a retry succeeds);
  - rekey gate refusals;
  - the stale refusal record;
  - the rekey end to end over the wire after a CA rotation, with both sides
    active on the new pin and the same new seed.
- `introduced_link_b2c_test.go`: an aborted rekey not governing, the card
  record, the bounded refusal log, `RekeySyncControlPin`.
- `offer_test.go`: own-pin rekey, `resumable` and its reasons.
- `org_intro_b2c_test.go`: the org peers view, accepting a rekey offer, the
  own pin from the transcript, the admission nudge, refusals in the list.
