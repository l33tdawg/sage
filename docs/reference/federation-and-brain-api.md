<!-- Verified against code at SAGE v11.2.1. Cite file:line when behavior is non-obvious. This doc covers the v11 federation and brain graph surface; rest-api.md governs the core /v1/* endpoints. -->

# SAGE Federation and Brain HTTP API Reference (v11)

v11 adds two independent HTTP surfaces. The v11.0 federation transport assumes the two nodes are reachable by address - normally the same LAN, a VPN, or a tunnel the operator controls. Built-in internet/NAT traversal is planned for v11.5, not provided by the v11.0 listener.

1. **Cross-network federation** - a read-only recall exchange between two independent SAGE chains, plus a guided TOTP JOIN ceremony to establish the agreement. This spans three listeners: a dedicated peer-facing mTLS listener (`/fed/v1/*`), the node operator's REST control surface (`/v1/federation/*`), and a cookie-authed dashboard proxy (`/v1/dashboard/federation/*`).
2. **The brain as a tool** - the memory "train of thought" endpoint (`GET /v1/dashboard/memory/{id}/related`) that powers the MRI click-to-explore board.

### The trust / consensus boundary (read this first)

Everything documented here is **OFF-consensus**. Foreign query results are merged into REST responses only - never written to `InsertMemory`, BadgerDB, or anything AppHash-visible (`internal/federation/types.go:1-14`). The **only** federation writes that reach chain state are the two operators' own `TxTypeCrossFedSet` (tx-33) and `TxTypeCrossFedRevoke` (tx-34) broadcasts, each authorized on-chain by `crossFedAuthorized` and each fired only after a human confirmation. A peer receipt (Mode-2) reaches chain state exclusively as verbatim signed bytes inside a `TxTypeCoCommitAttest` broadcast, re-verified under consensus. Every trust check in this package fails **closed**: unreachable peer, revoked/expired/unknown agreement, missing remote CA, or SPKI pin mismatch each deny.

**The JOIN ceremony's peer-auth anchor is HUMAN** - an in-person / on-camera QR scan (or a spoken-code fallback). The TOTP factor proves co-possession of a shared seed and 2-of-2 consent; it does **not** prove the secret reached the right peer. Do not read the ceremony as machine-authenticated key exchange.

---

## 1. The federation mTLS listener - `/fed/v1/*` (peer-facing)

A **separate port and router** from the local API. Started from `cmd/sage-gui/node.go:770-798` on `cfg.Federation.ListenAddr` (default `0.0.0.0:8444`) with `TLS 1.3` and `ClientAuth = tls.RequireAnyClientCert` (`internal/federation/trust.go:264-275`) - the local REST/TLS listeners keep `NoClientCert` semantics untouched. The router is built in `internal/federation/server.go:48-60`.

### Two middleware groups on one listener

| Group | Routes | Middleware | Why |
|---|---|---|---|
| Established peers | `status`, `query`, `receipt` | `peerAuth` (`server.go:74-208`) | Requires an ACTIVE cross_fed agreement |
| Pre-agreement JOIN | `join/ca`, `join/request`, `join/status`, `join/confirm` | `joinAuth` (`join_routes.go:161-183`) | No agreement exists yet during a join |

### `peerAuth` - the established-peer authenticator (`server.go:74-208`)

Every `status`/`query`/`receipt` request is authenticated end-to-end:

1. The claimed sender chain (`X-Chain-ID`) must have an ACTIVE, unexpired agreement (`ActiveAgreement`, fail-closed on revoked/expired/unknown/self).
2. The mTLS client cert on this connection must verify against THAT agreement's pin-checked CA (`loadPinnedRemoteCA` + `verifyChainAgainstCA`, `ExtKeyUsageClientAuth`) - binding the transport identity to the claimed chain.
3. The chain-qualified Ed25519 signature must verify for `(sender=claimed chain, receiver=our chain)` with a required nonce and `±5 min` timestamp skew (`maxTimestampSkew`, `server.go:30`).
4. The signature must be fresh - a per-peer-chain sharded replay cache (`replayFresh`, `server.go:236-263`).

**Required headers** (`internal/federation/types.go:22-35`):

| Header | Value |
|---|---|
| `X-Chain-ID` | sender chain id |
| `X-Agent-ID` | 64-char hex Ed25519 pubkey of the requesting agent |
| `X-Signature` | hex Ed25519 sig over the chain-qualified canonical message |
| `X-Timestamp` | unix epoch seconds |
| `X-Nonce` | hex nonce, 1-64 bytes (required, not optional here) |
| `X-Sig-Version` | `2` (chain-qualified) or `3` (adds rotating per-agreement TOTP factor) |

**Signature version gate** (`server.go:156-194`): fail-closed and driven by the agreement's persisted `seed_established` flag + the in-memory seed cache - never by running a KDF. If a seed is established and unlocked, `v3` is required (verified against every known seed epoch via `verifyV3AnyEpoch`); established-but-locked returns `503 federation locked - unlock to resume`; no seed established accepts `v2`.

### `GET /fed/v1/status`

Authenticated reachability / identity preflight (`handleStatus`, `server.go:271-273`). Distinguishes "peer unreachable" from "peer misconfigured" in the activation runbook.

**Response** (`StatusResponse`, `types.go:113-119`):

| Field | Type | Notes |
|---|---|---|
| `chain_id` | string | the serving node's own chain id |
| `time` | int64 | serving node's unix time |

### `POST /fed/v1/query`

Scoped read-only recall served to an authenticated peer (`handleQuery`, `server.go:282-414`). Authorization is **agreement-level**: OUR side of the treaty (allowed domains, `MaxClearance` ceiling, committed-only) is enforced; local-agent RBAC is deliberately NOT consulted (a foreign chain has no local org membership).

**Request body** (`QueryRequest`, `types.go:44-56`):

| Field | Type | Notes |
|---|---|---|
| `mode` | string | `semantic`, `text`, or `hybrid` (`types.go:38-42`) |
| `query` | string | required for `text`; used by `hybrid` |
| `embedding` | []float32 | required for `semantic`; used by `hybrid` |
| `domain_tag` | string | must be covered by `AllowedDomains`; empty only under a `*` agreement, else `403` |
| `min_confidence` | float64 | optional filter |
| `top_k` | int | default 10, capped at 50 (`server.go:28-29`) |
| `tags` | []string | optional OR-filter |

**Response** (`QueryResponse`, `types.go:78-89`):

| Field | Type | Notes |
|---|---|---|
| `chain_id` | string | serving node's chain id |
| `results` | []MemoryResult | see `MemoryResult`, `types.go:62-76` |
| `total_count` | int | length of `results` |

Per-record enforcement runs as defense in depth over the store filter (`server.go:366-407`): non-committed, out-of-domain, or above-ceiling records are dropped. A classification read error hides the record (fail closed). The count of records hidden by the classification ceiling is **logged, never returned** - disclosing it would turn the response into an existence/keyword oracle (`types.go:82-88`).

### `POST /fed/v1/receipt`

Accepts a peer's Mode-2 `CommitReceipt` push and anchors it via `TxTypeCoCommitAttest` on the receiver's own chain (`handleReceipt`, `server.go:404-422`).

**Request body** (`ReceiptPush`, `types.go:97-101`):

| Field | Type | Notes |
|---|---|---|
| `receipt` | []byte | verbatim `tx.EncodeCommitReceipt` bytes (sans ValSig) |
| `val_sig` | []byte | sender's Ed25519 sig over exactly those bytes, from a declared coauthor of the SharedID |
| `signer_pub_key` | []byte | optional hint; receiver still resolves the signer against its recorded coauthor set |

**Response** (`ReceiptPushResponse`, `types.go:106-111`): `status` (`anchored` | `already_anchored`), `shared_id`, `tx_hash`, `height`.

### JOIN ceremony routes (behind `joinAuth`)

`joinAuth` (`join_routes.go:161-183`) requires a client cert but NOT an active agreement; it rate-limits on the direct TCP peer (never `X-Forwarded-For`), caps the body at 64 KB (`joinBodyCap`), and threads the client-cert leaf SPKI into context for per-session binding. Nothing here is on the consensus path.

| Method + path | Handler | Purpose |
|---|---|---|
| `GET /fed/v1/join/ca?session_id=…` | `handleJoinCA` (`join_routes.go:197-209`) | Serves the host's own CA PEM to a scanning guest (guest authenticates it by the scanned pin, not the transport). Returns `{chain_id, ca_pem}` (`JoinCAResp`). `404` if no live session. |
| `POST /fed/v1/join/request` | `handleJoinRequest` (`join_routes.go:214-312`) | Guest -> host. Binds the guest to the session, asserts the presented guest CA SPKI equals the scanned anchor pin, verifies the TLS client cert chains to that CA, stages (does not commit) the guest CA. |
| `GET /fed/v1/join/status?session_id=…` | `handleJoinStatus` (`join_routes.go:316-347`) | Guest polls state flags; once the host approves, also returns the host's granted scope. Only the bound client cert may read. |
| `POST /fed/v1/join/confirm` | `handleJoinConfirm` (`join_routes.go:352-375`) | Guest -> host approval #2. Verifies the guest's signatures over the frozen attestation E, then the host broadcasts its tx-33, commits the staged CA + seed, marks ACTIVE. |

**`JoinRequestWire`** (guest -> host, `join_routes.go:85-94`): `session_id`, `guest_chain`, `guest_agent_id` (hex ed25519), `guest_nonce` (hex 16B), `guest_pin` (hex 32B SPKI = scanned anchor), `guest_ca_pem`, `guest_endpoint`, `scope` (`{max_clearance, allowed_domains, mode, direction}`). No secret seed is carried - the seed rode the QR; the nonce is freshness-only.

**`JoinRequestResp`** (`join_routes.go:97-104`): `host_chain`, `host_agent_id`, `host_nonce` (hex 16B), `confirm_step`, `host_pin` (echo), `host_endpoint`.

**`JoinStatusResp`** (`join_routes.go:108-116`): `state`, `host_approved`, `aborted`, `expired`, `active`, and (once approved) `host_scope` + `host_endpoint`.

**`JoinConfirmWire`** (`join_routes.go:120-124`): `session_id`, `guest_sig` (hex ed25519 over E), `guest_ack_sig` (hex ed25519 over E). **`JoinConfirmResp`** (`join_routes.go:127-131`): `status`, `host_chain`, `tx_hash`.

---

## 2. Operator REST - `/v1/federation/*` (Ed25519-authed)

The node operator's control surface. These routes sit inside the standard `/v1/` group behind `middleware.Ed25519AuthMiddleware` (`api/rest/server.go:282`, `303-317`) - the same three-header Ed25519 auth as the rest of `/v1/*` (see `rest-api.md`). Actions that make outbound signed calls or dial peers additionally require the **node operator** identity (`requireNodeOperator`, `federation_handler.go:24-31`; fail-closed when no operator is configured). Errors use RFC 7807 `application/problem+json`.

### Cross_fed agreement builder (tx-33 / tx-34)

| Method + path | Handler | Auth | Purpose |
|---|---|---|---|
| `POST /v1/federation/cross` | `handleCrossFedSet` (`federation_handler.go:66-186`) | Ed25519; authz on-chain | Stage remote CA, derive SPKI pin, broadcast tx-33 CrossFedSet with that pin as `PeerPubKey`. |
| `GET /v1/federation/cross` | `handleCrossFedList` (`federation_handler.go:248-275`) | Ed25519 | List on-chain agreements (reads chain state directly; works without the transport wired). |
| `POST /v1/federation/cross/{chain_id}/revoke` | `handleCrossFedRevoke` (`federation_handler.go:189-231`) | Ed25519 | Broadcast tx-34 CrossFedRevoke. |
| `GET /v1/federation/cross/{chain_id}/status` | `handleCrossFedPeerStatus` (`federation_handler.go:281-313`) | Ed25519 + node operator | Live reachability preflight against the peer's `/fed/v1/status`. |

**`CrossFedSetRequest`** (`federation_handler.go:44-55`): `remote_chain_id` (≤50 chars), `endpoint` (`https://host[:port]`, no path/query/fragment), `remote_ca_pem`, `max_clearance` (0-4), `allowed_domains` (required; `["*"]` = chain-admin treaty), `allowed_depts` (v11.0 accepts only `["*"]` or empty - dept scoping not yet enforced), `expires_at` (optional, must be future). The remote CA is **staged** and only committed after the tx is authorized on-chain, so an unauthorized set can never overwrite a live pinned CA (`federation_handler.go:126-179`).

**`CrossFedSetResponse`** (`federation_handler.go:58-63`): `remote_chain_id`, `spki_pin` (hex), `tx_hash`, `status`. Returns `201 Created`.

`GET /v1/federation/cross` returns `{agreements: []CrossFedListEntry, total}` where each entry (`federation_handler.go:234-244`) carries `remote_chain_id`, `endpoint`, `spki_pin`, `max_clearance`, `allowed_domains`, `allowed_depts`, `expires_at`, `status`, `expired`.

`GET …/status` returns `{remote_chain_id, reachable, peer_time}` on success or `{remote_chain_id, reachable:false, error}` with `502` when unreachable - `reachable` stays a bool across both branches by design.

### JOIN ceremony - operator localhost control surface

Node-operator-only (`federationJoinReady`, `federation_join_handler.go:21-27`); off-consensus. Each endpoint drives the federation Manager, which does the peer-facing `/fed/v1/join/*` calls and fires the operator's own tx-33 after human confirmation.

| Method + path | Handler | Wizard step |
|---|---|---|
| `POST /v1/federation/join/host/create` | `handleJoinHostCreate` (`:39`) | H1: open session + enrollment QR |
| `POST /v1/federation/join/host/scan-return` | `handleJoinHostScanReturn` (`:63`) | Host scans the guest's return QR (the anchor) |
| `GET /v1/federation/join/host/{session_id}` | `handleJoinHostStatus` (`:80`) | Host wizard poll (CODE_G, then CODE_H after approve) |
| `POST /v1/federation/join/host/{session_id}/approve` | `handleJoinHostApprove` (`:103`) | Approval #1: verify heard code, set grant, freeze E |
| `POST /v1/federation/join/host/{session_id}/abort` | `handleJoinHostAbort` (`:126`) | Burn the session |
| `POST /v1/federation/join/guest/scan` | `handleJoinGuestScan` (`:143`) | Validate scanned host QR + fetch/pin host CA |
| `POST /v1/federation/join/guest/request` | `handleJoinGuestRequest` (`:173`) | Fire `/fed/v1/join/request`, return CODE_G/CODE_H |
| `POST /v1/federation/join/guest/confirm` | `handleJoinGuestConfirm` (`:212`) | Approval #2: broadcast guest tx-33, tell host to activate |

(All handlers in `api/rest/federation_join_handler.go`.) Bodies: `HostCreateBody{endpoint}`; `HostScanReturnBody{session_id, return_uri}`; `HostApproveBody{typed_code, max_clearance, allowed_domains, mode, direction}`; `GuestScanBody{uri, endpoint}`; `GuestRequestBody{session_id, endpoint, max_clearance, allowed_domains, mode, direction}`; `GuestConfirmBody{session_id, endpoint, host_scope{…}}`. Guest scan/request/confirm return the Manager result structs (`GuestScanResult`, `GuestRequestResult` with `code_g`/`code_h`/`confirm_step`, and `{session_id, status:"active", tx_hash}`).

---

## 3. Dashboard proxy - `/v1/dashboard/federation/*` (cookie-authed)

The browser holds a dashboard session, not the operator's signing key, so it cannot call the Ed25519-signed REST endpoints. These routes run behind the dashboard auth middleware **plus** `wizardSecurityGate` (a strict same-origin check, `web/handler.go:375-378`) because they broadcast tx-33/tx-34 and dial peers on the operator's behalf. Everything is off-consensus (`web/federation_join.go:14-20`). Every route 501s when the transport is not wired (`fedReady`, `federation_join.go:56-62`). Registered in `registerFederationRoutes` (`federation_join.go:66-81`).

| Method + path | Handler | Purpose |
|---|---|---|
| `GET /v1/dashboard/federation/connections` | `handleFedConnections` (`:95`) | List cross_fed agreements from chain state |
| `POST /v1/dashboard/federation/connections/{chain_id}/revoke` | `handleFedRevoke` (`:123`) | Revoke (drives `RevokeAgreement` -> tx-34 + local seed/CA purge) |
| `GET /v1/dashboard/federation/connections/{chain_id}/status` | `handleFedPeerStatus` (`:136`) | Peer reachability preflight |
| `POST /v1/dashboard/federation/join/host/create` | `handleFedHostCreate` (`:153`) | Host H1 |
| `POST /v1/dashboard/federation/join/host/scan-return` | `handleFedHostScanReturn` (`:172`) | Host scans guest return QR |
| `GET /v1/dashboard/federation/join/host/{session_id}` | `handleFedHostStatus` (`:191`) | Host wizard poll |
| `POST /v1/dashboard/federation/join/host/{session_id}/approve` | `handleFedHostApprove` (`:203`) | Host approval #1 |
| `POST /v1/dashboard/federation/join/host/{session_id}/abort` | `handleFedHostAbort` (`:231`) | Burn session |
| `POST /v1/dashboard/federation/join/guest/scan` | `handleFedGuestScan` (`:241`) | Guest scan host QR |
| `POST /v1/dashboard/federation/join/guest/request` | `handleFedGuestRequest` (`:263`) | Guest request |
| `GET /v1/dashboard/federation/join/guest/{session_id}/status` | `handleFedGuestStatus` (`:294`) | Guest poll host approval |
| `POST /v1/dashboard/federation/join/guest/confirm` | `handleFedGuestConfirm` (`:308`) | Guest approval #2 |

(All handlers in `web/federation_join.go`.) The request/response bodies mirror the operator REST bodies of §2 (same field names). `GET …/connections` returns `{local_chain_id, connections: []FedConnection}` where `FedConnection` (`:86-93`) = `{remote_chain_id, endpoint, max_clearance, allowed_domains, status, expired}`. `POST …/revoke` returns `{remote_chain_id, status:"revoked", tx_hash}`.

**Trust boundary:** these routes proxy the same off-consensus Manager as §2. The only chain writes are the two operators' own tx-33/tx-34, fired inside the Manager after each human confirmation.

---

## 4. The brain as a tool - `GET /v1/dashboard/memory/{id}/related`

Powers the MRI click-to-explore "train of thought" board. Cookie-authed dashboard route (`web/handler.go:328`), handler `handleMemoryRelated` (`web/memory_related.go:97-262`).

**Query params:** `k` (default 50, capped at 120; `memory_related.go:31-32`, `103-109`).

**Auth / RBAC:** an MCP-agent request (carrying `X-Agent-ID`) is restricted to its visible agents; the operator dashboard (cookie session, no `X-Agent-ID`) sees all (`resolveAgentRBAC`, `memory_related.go:118-123`). `404` if the memory is not found.

**How related memories are ranked** (no embeddings required, `memory_related.go:17-28`): chain lineage via `parent_hash` (weight 6.0, `chain`), shared tags (2.0, `same-topic`), full-text content overlap (FTS when available, else in-process word overlap on an encrypted vault; `similar`), and same-domain high-confidence filler (0.25, `same-lobe`) so the panel is never empty. Ties break on memory id for stability.

**Response** (`memory_related.go:256-261`):

| Field | Type | Notes |
|---|---|---|
| `id` | string | the clicked memory id |
| `domain` | string | its domain tag |
| `content` | string | truncated to 160 chars |
| `related` | []RelatedMemory | ranked list |

**`RelatedMemory`** (`memory_related.go:38-50`): `id`, `content` (≤160 chars), `domain`, `confidence`, `corroboration_count`, `status`, `created_at` (RFC 3339), `memory_type`, `kind` (`do` | `dont` | `observation` | `note` - the board columns, `classifyKind`, `:56-68`), `relation` (`chain` | `same-topic` | `similar` | `same-lobe`), `score`.

**Trust boundary:** read-only over local chain state; no consensus writes, no federation calls.

---

## v11.5 Domain sync (shared-domain replication)

Opt-in replication of committed memories in consented domain subtrees to a federation peer - store-and-forward on top of the same mTLS transport and treaty scope as recall exchange. Everything is off-consensus (no fork): copies enter the receiving chain only as ordinary locally-signed `TxTypeMemorySubmit` transactions that the receiver's own consensus gates re-validate.

### Consent model

Consent is **local, per side, and can only narrow the treaty**. Each operator configures `sync_domains` for a peer (`PUT /v1/federation/cross/{chain_id}/sync`, operator-only); every entry must be concrete (no `*`) and subtree-covered by the on-chain agreement's `allowed_domains`. It is deliberately **not** a field on the on-chain `cross_fed` record. The receiver enforces its **own** rows on every incoming push, so asymmetric consent means less flows, never more. Subtree semantics throughout: consenting `hr` covers `hr.public`.

### `POST /fed/v1/sync/push` (peer-facing, behind peerAuth)

`{items: [{origin_chain_id, origin_memory_id, origin_created_at, domain, classification, memory_type, confidence_score, content, content_hash, tags}]}` - max 8 items, 64KB content each; `content_hash` must equal sha256(content). The origin chain **must** equal the authenticated peer (no third-chain forwarding). Handler: `internal/federation/sync_server.go` `handleSyncPush`.

Admission gates, in order: treaty `allowed_domains` -> receiver's own `sync_domains` consent -> `classification <= max_clearance` -> idempotency replay from the `sync_origin` ledger -> cross-domain duplicate check (identical committed content in a **different** domain is terminally rejected and surfaced, never silently moved; same domain is an idempotent duplicate-success) -> vault-locked retry NACK -> admit via a locally-signed submit tx with the deterministic id `hex(sha256("sync1|" + origin_chain + "|" + origin_memory_id))`.

Per-item outcomes: `accepted`, `duplicate`, `rejected_cross_domain_dup`, `rejected_clearance`, `rejected_not_consented`, `rejected_domain_scope`, `retry`.

### `POST /fed/v1/sync/digest` (peer-facing, behind peerAuth)

Anti-entropy: the sender asks what the receiver has **already decided** about its memories in a domain subtree. `{domain, after, limit}` -> `{consented, consented_domains, origin_memory_ids (sorted asc), next_cursor}` - cursor-paged, 2000 ids per page. Answered from the admission ledger **including terminal rejections** (refused items are never re-offered) and deliberately not from the committed set: the receiver's sovereign lifecycle can deprecate a copy later without re-opening delivery. Handler: `handleSyncDigest`, same file.

`GET /fed/v1/status` now advertises `capabilities: ["sync"]` when the backend supports it; pre-v11.5 peers 404 on the sync routes and the sender parks deliveries on a 1-hour backoff.

### Operator surface (`/v1/federation/cross/{chain_id}/sync*`, operator-only)

| Route | Purpose |
|---|---|
| `PUT .../sync` | Replace the consented sync-domain set (validated against the active agreement). |
| `GET .../sync` | Read it back. |
| `GET .../sync/status` | Outbox counts per state, rejected rows with reasons, pending retry schedule, last anti-entropy run, the peer's advertised consent, peer-unsupported flag. |

Revoking an agreement (either revoke path) purges the peer's sync consent and queued deliveries.

### Delivery semantics - the honest version

- **Sender**: a 30s drainer scans committed memories in consented subtrees into a durable outbox (`sync_outbox`, survives restarts; recovery is the scan itself - no watermark), delivers in batches of 8, and re-runs **every** gate at send time (re-scoped agreements, re-domained or re-classified memories drop out). A commit-tail hook accelerates delivery; the poll is the correctness backstop. Backoff `min(30s * 2^attempts, 1h)`. Vault-locked content defers without burning attempts.
- **"Delivered" means handed to the peer's pipeline.** The copy lands `proposed` (co-commit-style direct commit does not apply) and the receiver's own voter governs it. Peer lifecycle stays sovereign: decay, corroboration, and deprecation never propagate in either direction.
- **The copy's on-chain author is the receiving node's operator agent** - everything entering chain state is a locally-signed tx. Origin truth (`origin_chain_id`, `origin_memory_id`, `origin_created_at`) lives in the receiver's off-consensus `sync_origin` ledger.
- **A synced copy is never re-forwarded** (no A->B->C fan-out): the sender excludes copies at scan time and the receiver rejects any push whose origin is not the authenticated peer.
- Copies arrive **without embeddings** (semantic recall needs a receiver-side re-embed; keyword/FTS recall works immediately). Tag replication is a non-goal in v11.5.
- SQLite-only: Postgres-backed nodes disable sync loudly (501 on the routes, drainer no-op, no `sync` capability).
