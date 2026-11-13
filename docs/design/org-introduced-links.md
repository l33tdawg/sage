# Org-introduced federation links (RFC #443, Phase A + B)

Status: A1 merged (#553); this revision covers A2. Contract agreed on #443 (v2 + the two
implementation points of 2026-11-12). Line refs are main @ fe0deea7 (v11.25.7).
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
- **A2 (this PR):** the node-self-mint card route (§1.2, decided on #443), and
  the manual QR JOIN gate with the Root-signed override (§1.5).

**Phase B (next PR):** owner envelope, offers, intro handshake routes, the
`introduced_link` record and the intent state machine (§3), and the
`ActiveAgreement` hook. Only `ask` mode. §3 and §4 are designed here, as agreed,
so Phase A's stores and caps already fit them.

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
Root signed. Same revision and digest is a no-op; a lower revision, or the same
revision with other content, is refused. Each use (side, stage, session, time)
is appended to the record under compare-and-set before the JOIN continues; the
last 256 uses are kept plus a total count. `GET /v1/federation/join-overrides`
lists them with a `usable` / `expired` / `not_current_root` status. Overrides
and policy-eligible peers are separate: an eligible peer never consumes an
override use.

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
- `staged_ref`: the staged CA + seed
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
existing ones: if any `introduced_link` row exists for `remote_chain_id` and the
newest row's state isn't `active`, it denies. `ActiveAgreements` applies the same
filter. Manual agreements have no row and are unchanged.

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
