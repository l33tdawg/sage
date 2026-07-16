<!-- Verified against SAGE v11.9.0 code (2026-07-16). Cite file:line when behavior is non-obvious. This doc covers the v11 federation and brain graph surface; rest-api.md governs the core /v1/* endpoints. -->

# SAGE Federation and Brain HTTP API Reference (v11)

v11 adds two independent HTTP surfaces. v11.6 can carry the same mTLS HTTP protocol over direct HTTPS or libp2p, including NAT traversal and Circuit Relay v2 fallback. The dashboard JOIN wizard offers Same LAN and Internet setup: Internet enrollment carries bounded libp2p routes in the QR, while two v11.6 nodes paired on a LAN exchange and persist roaming routes over their authenticated agreement after signing. Older peers remain compatible with the legacy LAN enrollment. The relay is outside the trust boundary and sees only encrypted traffic.

1. **Cross-network federation** - a directional read/copy exchange between two independent SAGE chains, plus a guided TOTP JOIN ceremony that establishes node trust. Mutable per-domain peer RBAC is separate from that ceremony. Write is reserved in the versioned wire shape but unavailable in v11.9. This spans three listeners: a dedicated peer-facing mTLS listener (`/fed/v1/*`), the node operator's REST control surface (`/v1/federation/*`), and a cookie-authed dashboard proxy (`/v1/dashboard/federation/*`).
2. **The brain as a tool** - the memory "train of thought" endpoint (`GET /v1/dashboard/memory/{id}/related`) that powers the MRI click-to-explore board.

### The trust / consensus boundary (read this first)

Federation transport, routing, policy, and foreign recall results are **OFF-consensus**. Borrowed query results are merged into REST responses only and never persisted. Treaty lifecycle reaches chain state through the two operators' own `TxTypeCrossFedSet` (tx-33) and `TxTypeCrossFedRevoke` (tx-34) broadcasts. A peer receipt (Mode-2) reaches chain state as signed bytes inside `TxTypeCoCommitAttest`. Domain-sync copies enter independently as ordinary locally signed `TxTypeMemorySubmit` (tx-1) transactions and pass the receiver's consensus/RBAC gates. Remote Write does **not** enter this path in v11.9: the reserved route returns an authenticated `501` and never dispatches a body to `/v1/memory/submit` (`internal/federation/remote_write.go:41-52`). Trust and authorization checks fail **closed** on an unreachable peer, revoked/expired/unknown agreement, missing remote CA, SPKI pin mismatch, peer-operator mismatch, or absent domain permission; Write remains unavailable regardless of credentials.

**The JOIN ceremony's peer-auth anchor is HUMAN** - an in-person / on-camera QR scan (or a spoken-code fallback). The TOTP factor proves co-possession of a shared seed and 2-of-2 consent; it does **not** prove the secret reached the right peer. Do not read the ceremony as machine-authenticated key exchange.

### Trust and directional peer RBAC

JOIN answers one question: **which SAGE node is on the other end?** A fresh
ceremony freezes the remote chain, node-operator key, CA pin, and policy epoch
on each side (`internal/federation/join_routes.go:450-455`, `1219-1222`;
`internal/store/sync_tables.go:119-138`). The v3 ceremony accepts only
`max_clearance:4`, `allowed_domains:[]`, `mode:"exchange"`, and
`direction:"both"`; any other scope is rejected because mutable domain access
belongs after pairing (`internal/federation/join_routes.go:75-91`, `257-280`,
`730-740`, `1051-1055`, `1126-1149`).
The dashboard sends that exact compatibility envelope
(`web/static/js/app.js:11680-11694`, `11803-11806`, `11922-11931`). Its empty
domain list grants no legacy access. The tx-33 clearance value remains a
fail-closed classification ceiling during compatibility migration; the
trust-only UI uses the maximum so it does not silently narrow a later peer-RBAC
grant (`internal/federation/server.go:321-326`).

After pairing, each node owns one complete, mutable permission snapshot for the
other node's frozen operator:

| Permission | What it authorizes on the source node |
|---|---|
| **Read** | Live remote lookup in the selected existing-domain subtree. Results are borrowed, not retained. |
| **Copy** | Permission for the peer to retain synchronized local copies. The peer must separately opt in with its own subscription. |
| **Write (reserved)** | Nothing in v11.9. The field remains for mixed-version compatibility, but persisted and advertised peer policy keeps it false and permission updates reject it. |

For A → B, A edits “what A shares with B”; B can only observe that snapshot.
B edits its own independent “what B shares with A” snapshot for the reverse
direction (`web/federation_permissions.go:161-220`). `Copy` implies `Read`,
domains are concrete, and replacing the snapshot does not re-run JOIN
(`internal/store/peer_rbac_policy.go:34-53`, `113-142`, `226-260`). A node may
therefore start with zero shared domains, later add every existing `tii*`
domain, remove one, or change Read/Copy while the trusted connection stays
active. A request containing `write:true` is rejected; it is not silently
converted into an ordinary grant (`web/federation_permissions.go:223-258`).

An existing v11.8-era trusted connection can migrate without pairing again
when its original ceremony evidence still identifies the peer unambiguously.
Legacy guest rows already name the remote host operator. A legacy host row is
upgraded only when the deterministic enrollment group has exactly the local
operator and the CA-pinned remote member; that recovered key is then frozen by
epoch and CA pin and cannot be replaced (`internal/federation/peer_rbac.go:62-118`,
`193-229`; `internal/store/sync_tables.go:300-306`, `378-415`). If those exact
artifacts are absent or disagree, migration fails closed and re-pairing is
required instead of guessing an identity.

A configured snapshot with zero domain rows is explicit **deny-all**. Fresh
JOIN activation installs exactly that present, empty policy for the frozen peer
(`internal/federation/peer_rbac.go:219-251`). A genuinely missing policy is
different: only a legacy/unconfigured connection may fall back to its historical
tx-33 behavior. A frozen v3 binding with missing policy storage is synthesized
as deny-all instead of taking that fallback (`internal/store/peer_rbac_policy.go:3-6`,
`173-224`; `internal/federation/peer_rbac.go:121-190`).

---

## 1. The federation mTLS listener - `/fed/v1/*` (peer-facing)

A **separate port and router** from the local API. Started from `cmd/sage-gui/node.go:770-798` on `cfg.Federation.ListenAddr` (default `0.0.0.0:8444`) with `TLS 1.3` and `ClientAuth = tls.RequireAnyClientCert` (`internal/federation/trust.go:264-275`) - the local REST/TLS listeners keep `NoClientCert` semantics untouched. The router is built in `internal/federation/server.go:48-70`.

### Two middleware groups on one listener

| Group | Routes | Middleware | Why |
|---|---|---|---|
| Established peers | `status`, `query`, reserved `write`, `receipt`, `p2p/routes`, `sync/*` | `peerAuth` (`internal/federation/server.go:74-220`) | Requires an ACTIVE cross_fed agreement; v11.9 Write then returns `501` |
| Pre-agreement JOIN | `join/ca`, `join/request`, `join/status`, `join/confirm` | `joinAuth` (`internal/federation/join_routes.go:192-226`) | No agreement exists yet during a join |

### `peerAuth` - the established-peer authenticator (`internal/federation/server.go:74-220`)

Every established-peer request, including `query`, `write`, and `sync`, is authenticated end-to-end:

1. The claimed sender chain (`X-Chain-ID`) must have an ACTIVE, unexpired agreement (`ActiveAgreement`, fail-closed on revoked/expired/unknown/self).
2. The mTLS client cert on this connection must verify against THAT agreement's pin-checked CA (`loadPinnedRemoteCA` + `verifyChainAgainstCA`, `ExtKeyUsageClientAuth`) - binding the transport identity to the claimed chain.
3. The chain-qualified Ed25519 signature must verify for `(sender=claimed chain, receiver=our chain)` with a required nonce and `±5 min` timestamp skew (`maxTimestampSkew`, `server.go:30`).
4. The signature must be fresh - a per-peer-chain sharded replay cache (`replayFresh`, `server.go:240-275`).

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

Authenticated reachability / identity and permission preflight (`handleStatus`,
`internal/federation/server.go:285-319`). Distinguishes "peer unreachable" from
"peer misconfigured" and carries the caller-bound current grant.

**Response** (`StatusResponse`, `internal/federation/types.go:118-166`):

| Field | Type | Notes |
|---|---|---|
| `chain_id` | string | the serving node's own chain id |
| `time` | int64 | serving node's unix time |
| `capabilities` | []string | optional features. v11.9 may advertise `sync`; it never advertises reserved `write-v1` (`internal/federation/server.go:291-315`). |
| `peer_rbac_grant` | object, optional | the serving node's current `{policy_version, domains:[{domain,read,write,copy}]}` snapshot, disclosed only to the exact bound peer; present with zero rows means deny-all. The versioned `write` member is always false in v11.9 (`internal/federation/peer_rbac.go:270-285`). |
| `sharing_grant` | object, optional | legacy compatibility envelope (`allowed_domains`, `max_clearance`) |

### `POST /fed/v1/query`

Live read-only recall served to an authenticated peer
(`internal/federation/server.go:321-482`). When a `PeerRBACPolicy` exists, its
`Read` rows are authoritative for both the requested domain and every returned
record, including the configured-empty deny-all case. Only a missing policy
uses legacy tx-33 `AllowedDomains`. Committed-only and the compatibility
`MaxClearance` ceiling remain defense-in-depth gates; local org membership is
not substituted for the peer policy (`server.go:321-355`, `427-453`). Read does
not persist a copy on either node.

**Request body** (`QueryRequest`, `internal/federation/types.go:46-61`):

| Field | Type | Notes |
|---|---|---|
| `mode` | string | `semantic`, `text`, or `hybrid` (`types.go:38-42`) |
| `query` | string | required for `text`; used by `hybrid` |
| `embedding` | []float32 | required for `semantic`; used by `hybrid` |
| `embedding_provider` | string | exact vector-space ID; required whenever `embedding` is present. A serving peer filters candidates to this space and rejects an unstamped vector rather than comparing mixed spaces. |
| `domain_tag` | string | must be covered by a configured `Read` row (subtree semantics); legacy-only links use tx-33 scope. A configured concrete policy never permits an empty domain. |
| `min_confidence` | float64 | optional filter |
| `top_k` | int | default 10, capped at 50 (`server.go:28-29`) |
| `tags` | []string | optional OR-filter |

**Response** (`QueryResponse`, `internal/federation/types.go:83-94`):

| Field | Type | Notes |
|---|---|---|
| `chain_id` | string | serving node's chain id |
| `results` | []MemoryResult | see `MemoryResult`, `types.go:63-80` |
| `total_count` | int | length of `results` |

Per-record enforcement runs as defense in depth over the store filter
(`internal/federation/server.go:427-453`): non-committed, out-of-Read-policy,
or above-ceiling records are dropped. A classification read error hides the
record (fail closed). The count of records hidden by the classification ceiling
is **logged, never returned** - disclosing it would turn the response into an
existence/keyword oracle (`internal/federation/types.go:83-94`).

Semantic and vector-assisted hybrid federation are also vector-space gated: the
request's `embedding_provider` becomes `QueryOptions.VectorProvider`. Text-only
federation is unchanged. This makes a provider transition temporarily reduce
semantic coverage while rows are repaired, but never compares Ollama vectors with
hash or another model/dimension space (`server.go`, `types.go`).

### `POST /fed/v1/write`

This is a **reserved, fail-closed endpoint** in v11.9. It remains mounted behind
`peerAuth`, so an unauthenticated request is rejected before the handler. After
successful peer authentication, the handler returns `501 Not Implemented` with
`ErrRemoteWriteCapabilityUnavailable`; it deliberately does not parse the body
or dispatch credentials to `/v1/memory/submit`
(`internal/federation/server.go:48-55`; `internal/federation/remote_write.go:16-23`,
`48-52`).

The `write-v1` constant and `RemoteWriteRequest` envelope remain reserved for
mixed-version compatibility, but v11.9 never advertises the capability. The
outbound `WritePeer` method returns the typed unavailable error before agreement
lookup or dialing (`internal/federation/remote_write.go:9-45`), and status adds
only capabilities actually supported by the running node
(`internal/federation/server.go:282-315`).

This is a security boundary, not merely an unfinished UI. An ordinary
`AccessGrant` authorizes an agent for a domain and is reusable through the
normal submit API outside this particular A↔B connection. It therefore cannot
honestly represent connection-scoped Write. Enabling Write requires a future
consensus-bound ingress capability tied to the active ceremony generation,
frozen peer, domain, and exact submission; until then all federation Write paths
fail closed (`internal/federation/remote_write.go:10-19`;
`internal/store/peer_rbac_policy.go:26-41`, `106-110`, `128-130`).

Preview builds could leave an ordinary level-2 grant recorded in the managed
ledger. Migration clears every stored peer-policy Write bit before it can be
served, while `sage-gui` synchronously reconciles every managed grant before
binding its REST, TLS, federation, or federation-P2P application listeners.
Startup fails closed if the exact grant cannot be revoked and its absence
confirmed; cleanup is not
conditional on the federation Manager starting (`internal/store/peer_rbac_policy.go:106-110`;
`cmd/sage-gui/node.go`; `web/federation_grant_cleanup.go:71-161`).

### `POST /fed/v1/receipt`

Accepts a peer's Mode-2 `CommitReceipt` push and anchors it via `TxTypeCoCommitAttest` on the receiver's own chain (`handleReceipt`, `server.go:485-505`).

**Request body** (`ReceiptPush`, `types.go:97-101`):

| Field | Type | Notes |
|---|---|---|
| `receipt` | []byte | verbatim `tx.EncodeCommitReceipt` bytes (sans ValSig) |
| `val_sig` | []byte | sender's Ed25519 sig over exactly those bytes, from a declared coauthor of the SharedID |
| `signer_pub_key` | []byte | optional hint; receiver still resolves the signer against its recorded coauthor set |

**Response** (`ReceiptPushResponse`, `types.go:106-111`): `status` (`anchored` | `already_anchored`), `shared_id`, `tx_hash`, `height`.

### JOIN ceremony routes (behind `joinAuth`)

`joinAuth` (`internal/federation/join_routes.go:192-226`) requires a client cert
but NOT an active agreement; it rate-limits on the direct TCP peer (never
`X-Forwarded-For`), caps the body at 64 KB (`joinBodyCap`), and threads the
client-cert leaf SPKI into context for per-session binding. Nothing here is on
the consensus path.

| Method + path | Handler | Purpose |
|---|---|---|
| `GET /fed/v1/join/ca?session_id=…` | `handleJoinCA` (`join_routes.go:237-252`) | Serves the host's own CA PEM to a scanning guest (guest authenticates it by the scanned pin, not the transport). Returns `{chain_id, ca_pem}` (`JoinCAResp`). `404` if no live session. |
| `POST /fed/v1/join/request` | `handleJoinRequest` (`join_routes.go:254-370`) | Guest -> host. Rejects non-trust-only scope, binds the guest to the session, asserts the presented guest CA SPKI equals the scanned anchor pin, verifies the TLS client cert chains to that CA, and stages (does not commit) the guest CA. |
| `GET /fed/v1/join/status?session_id=…` | `handleJoinStatus` (`join_routes.go:370-406`) | Guest polls state flags; once the host approves, also returns the fixed compatibility scope. Only the bound client cert may read. |
| `POST /fed/v1/join/confirm` | `handleJoinConfirm` (`join_routes.go:406-440`) | Guest -> host approval #2. Verifies the guest's signatures over the frozen attestation E, then the host broadcasts its tx-33, commits the staged CA + seed, marks ACTIVE. |

Internet enrollment adds `x_sage_transport=p2p`, protocol, peer ID, and up to four complete peer multiaddrs to the otherwise compatible TOTP URI. Parsing is all-or-nothing: the peer ID must match every terminal `/p2p/<id>`, at least one route must contain `/p2p-circuit`, and malformed or partial P2P metadata is rejected. P2P route and peer identity digests are included in the frozen attestation E, so route substitution makes the signed confirmation fail. The spoken codes separately bind the seed, CA pins, session nonces, and step. Temporary pre-agreement stream admission is bounded by session and peer and becomes a durable allowlist entry only after the agreement commits.

### `POST /fed/v1/p2p/routes` (established peers)

Authenticated v11.6 peers use this endpoint after a LAN ceremony to exchange `JoinP2PBundle{peer_id, protocol, addrs}`. Both bundles receive the same strict validation as Internet enrollment and are persisted atomically with the runtime allowlist. Older peers return 404 and remain LAN-only; the agreement itself stays valid. Revocation removes the stored route and inbound admission.

**`JoinRequestWire`** (guest -> host, `internal/federation/join_routes.go:105-119`):
`session_id`, `guest_chain`, `guest_agent_id` (hex ed25519), `guest_nonce`
(hex 16B), `guest_pin` (hex 32B SPKI = scanned anchor), `guest_ca_pem`,
`guest_endpoint`, and the compatibility `scope` shape
(`{max_clearance, allowed_domains, mode, direction}`). No secret seed is carried
- the seed rode the QR; the nonce is freshness-only. The handler accepts only
the fixed trust-only `{4, [], "exchange", "both"}` values
(`internal/federation/join_routes.go:75-91`, `257-280`); do not interpret these
retained fields as mutable peer-RBAC policy.

**`JoinRequestResp`** (`join_routes.go:121-129`): `host_chain`, `host_agent_id`, `host_nonce` (hex 16B), `confirm_step`, `host_pin` (echo), `host_endpoint`.

**`JoinStatusResp`** (`join_routes.go:131-141`): `state`, `host_approved`, `aborted`, `expired`, `active`, and (once approved) `host_scope` + `host_endpoint`.

**`JoinConfirmWire`** (`join_routes.go:143-150`): `session_id`, `guest_sig`
(hex ed25519 over E), `guest_ack_sig` (hex ed25519 over E), and the signed
pairwise group invite proof. **`JoinConfirmResp`** (`join_routes.go:152-159`):
`status`, `host_chain`, `tx_hash`, plus optional sync-group bootstrap fields.

---

## 2. Operator REST - `/v1/federation/*` (Ed25519-authed)

The node operator's control surface. These routes sit inside the standard `/v1/` group behind `middleware.Ed25519AuthMiddleware` (`api/rest/server.go:393-423`) - the same signed-request auth as the rest of `/v1/*` (see `rest-api.md`). Trust, policy, and outbound peer actions additionally require the **exact node operator** identity (`requireNodeOperator`, `federation_handler.go:24-35`; fail-closed when no operator is configured). Errors use RFC 7807 `application/problem+json`.

### Cross_fed agreement builder (tx-33 / tx-34)

| Method + path | Handler | Auth | Purpose |
|---|---|---|---|
| `POST /v1/federation/cross` | `handleCrossFedSet` (`federation_handler.go:69-193`) | Exact node operator + on-chain authz | Stage remote CA, derive SPKI pin, broadcast tx-33 CrossFedSet with that pin as `PeerPubKey`. |
| `GET /v1/federation/cross` | `handleCrossFedList` (`federation_handler.go:266-297`) | Exact node operator | List on-chain agreements (reads chain state directly; works without the transport wired). |
| `POST /v1/federation/cross/{chain_id}/revoke` | `handleCrossFedRevoke` (`federation_handler.go:195-251`) | Exact node operator + on-chain authz | Broadcast tx-34 CrossFedRevoke. |
| `GET /v1/federation/cross/{chain_id}/status` | `handleCrossFedPeerStatus` (`federation_handler.go:299-333`) | Exact node operator | Live reachability preflight against the peer's `/fed/v1/status`. |
| `POST /v1/federation/cross/{chain_id}/write` | `handleCrossFedWrite` (`api/rest/federation_write_handler.go:11-25`) | Ed25519 + exact node operator | Reserved compatibility surface. After operator and chain-id validation it returns `501` before parsing an inner credential or dialing a peer. |
| `GET/PUT /v1/federation/cross/{chain_id}/sync` | `handleSyncDomainsGet/Set` (`api/rest/federation_handler.go:342-576`, `656-746`) | Ed25519 + exact node operator | Read or replace local v3 Publish/Subscribe lanes; true legacy links retain their `domains` contract. |

**`CrossFedSetRequest`** (`federation_handler.go:47-59`): `remote_chain_id` (≤50 chars), `endpoint` (`https://host[:port]`, no path/query/fragment), `remote_ca_pem`, `max_clearance` (0-4), `allowed_domains` (required; `["*"]` = chain-admin treaty), `allowed_depts` (v11.0 accepts only `["*"]` or empty - dept scoping not yet enforced), `expires_at` (optional, must be future). The remote CA is **staged** and only committed after the tx is authorized on-chain, so an unauthorized set can never overwrite a live pinned CA (`federation_handler.go:133-186`).

Agreement generation changes are process-wide serialized across Manager and
REST control surfaces. The lease covers tx-33 through its matching CA/JOIN
activation and tx-34 through local purge; tx-33 also takes the sync-policy
write side while it replaces the effective generation. The fixed lock order is
agreement mutation, then sync policy. This prevents set/revoke interleavings
from deleting or resurrecting the wrong local generation and prevents a
completed narrowing from leaving an old broad response in flight
(`internal/federation/manager.go`; `internal/federation/join_routes.go`;
`api/rest/federation_handler.go`).

**`CrossFedSetResponse`** (`federation_handler.go:61-67`): `remote_chain_id`, `spki_pin` (hex), `tx_hash`, `status`. Returns `201 Created`.

`GET /v1/federation/cross` returns `{agreements: []CrossFedListEntry, total}` where each entry (`federation_handler.go:253-264`) carries `remote_chain_id`, `endpoint`, `spki_pin`, `max_clearance`, `allowed_domains`, `allowed_depts`, `expires_at`, `status`, `expired`.

`GET …/status` returns `{remote_chain_id, reachable, peer_time}` on success or `{remote_chain_id, reachable:false, error}` with `502` when unreachable - `reachable` stays a bool across both branches by design.

`POST …/write` reserves the path and `RemoteWriteRequest` wire type for
compatibility and requires the exact configured node operator. In v11.9 the
control handler returns `501` before parsing any inner credential or calling a
transport implementation (`api/rest/federation_write_handler.go:11-25`). The
Manager's separately reserved `WritePeer` method likewise returns the typed
capability-unavailable error before agreement lookup or network dial
(`internal/federation/remote_write.go:41-45`). Once operator authentication and
chain-id validation succeed, even a malformed preview envelope receives the
same fail-closed `501`; its contents are never interpreted.

### JOIN ceremony - operator localhost control surface

Node-operator-only (`federationJoinReady`,
`api/rest/federation_join_handler.go`); off-consensus. Each endpoint drives the
federation Manager, which performs the peer-facing `/fed/v1/join/*` calls and
fires the operator's own compatibility tx-33 after human confirmation. This
low-level API retains the historical scope-shaped fields, but the Manager
accepts only the trust-only empty-domain values documented above. Mutable
authorization belongs to the post-JOIN permission endpoints.

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

The browser holds a dashboard session, not the operator's signing key, so it
cannot call the Ed25519-signed REST endpoints. The complete federation surface
is therefore mounted behind the stricter local-operator gate: an authenticated
CEREBRUM operator session or an exact node-operator-signed request. Ordinary
dashboard-authenticated agents cannot inspect ceremonies or change peer policy
(`web/federation_join.go:71-102`, `1021-1037`). Everything remains
off-consensus except the ordinary transactions explicitly described here.
Every route 501s when the transport is not wired (`fedReady`,
`federation_join.go:62-69`).

| Method + path | Handler | Purpose |
|---|---|---|
| `GET /v1/dashboard/federation/shareable-domains` | `handleFedShareableDomains` (`web/federation_permissions.go:30-111`) | List existing registered/observed local domains and whether this operator may share them; never creates a domain. |
| `GET /v1/dashboard/federation/connections` | `handleFedConnections` (`web/federation_join.go:718`) | List cross_fed agreements from chain state |
| `GET /v1/dashboard/federation/connections/{chain_id}/permissions` | `handleFedPermissionsGet` (`web/federation_permissions.go:161-220`) | Return editable `local_permissions` and the authenticated peer's read-only `remote_permissions`. |
| `PUT /v1/dashboard/federation/connections/{chain_id}/permissions` | `handleFedPermissionsPut` (`web/federation_permissions.go:254-402`) | Replace this node's complete existing-domain Read/Copy snapshot for the frozen peer. A true `write` field is rejected. |
| `GET/PUT /v1/dashboard/federation/connections/{chain_id}/sync` | `handleFedSyncGet/Set` (`web/federation_join.go:344-579`) | Read or change directional copy lanes; current UI changes only the receiver's `subscribe_domains` choice. |
| `POST /v1/dashboard/federation/connections/{chain_id}/revoke` | `handleFedRevoke` (`web/federation_join.go:757`) | Revoke (drives `RevokeAgreement` -> tx-34 + local seed/CA purge) |
| `GET /v1/dashboard/federation/connections/{chain_id}/status` | `handleFedPeerStatus` (`web/federation_join.go:778`) | Peer reachability preflight |
| `POST /v1/dashboard/federation/join/host/create` | `handleFedHostCreate` (`web/federation_join.go:795`) | Host H1; body includes `transport` (`lan` or `internet`) |
| `POST /v1/dashboard/federation/join/host/scan-return` | `handleFedHostScanReturn` (`web/federation_join.go:827`) | Host scans guest return QR |
| `GET /v1/dashboard/federation/join/host/{session_id}` | `handleFedHostStatus` (`web/federation_join.go:846`) | Host wizard poll |
| `POST /v1/dashboard/federation/join/host/{session_id}/approve` | `handleFedHostApprove` (`web/federation_join.go:858`) | Host approval #1 |
| `POST /v1/dashboard/federation/join/host/{session_id}/abort` | `handleFedHostAbort` (`web/federation_join.go:886`) | Burn session |
| `POST /v1/dashboard/federation/join/guest/scan` | `handleFedGuestScan` (`web/federation_join.go:896`) | Guest scan host QR |
| `POST /v1/dashboard/federation/join/guest/request` | `handleFedGuestRequest` (`web/federation_join.go:918`) | Guest request |
| `GET /v1/dashboard/federation/join/guest/{session_id}/status` | `handleFedGuestStatus` (`web/federation_join.go:949`) | Guest poll host approval |
| `POST /v1/dashboard/federation/join/guest/confirm` | `handleFedGuestConfirm` (`web/federation_join.go:963`) | Guest approval #2 |

(JOIN handlers live in `web/federation_join.go`.) Except for the dashboard-only
host `transport` choice and its fixed trust-only compatibility scope, the join
request/response bodies mirror the operator REST bodies of §2.

The permissions `PUT` body is
`{"permissions":[{"domain":"tii.work","read":true,"write":false,"copy":true}]}`.
It is a full snapshot: omitted domains are removed, and an empty array is
deny-all. Only concrete domains already present in the local catalogue and
controlled by the operator may be enabled
(`web/federation_permissions.go:223-251`, `292-305`). A trailing `*` in the UI
is a prefix filter, and bulk actions affect only visible rows, so filtering
`tii*` is a convenient way to grant the same verb across existing TII domains
without creating a wildcard permission (`web/static/js/app.js:12014-12021`,
`12153-12165`). Read and Copy stay boundary permissions and do not create an
ordinary AccessGrant. The versioned `write` member must remain false;
`write:true` returns `400` with the consensus-bound-ingress explanation
(`web/federation_permissions.go:223-258`, `279-289`). Saving Copy also aligns
the source's transport Publish lane while
preserving this node's independent Subscribe choices
(`web/federation_permissions.go:377-395`).

**Trust boundary:** JOIN/revoke changes trust with the operator's ordinary
tx-33/tx-34. Read and Copy policy remains off-consensus. Permission updates do
not mint an ordinary AccessGrant; preview-era managed grants are cleanup-only.
Federation Write is unavailable, while accepted copies enter through ordinary
tx-1 submission. `sage-gui` retires tracked preview grants synchronously before
binding application listeners and refuses startup if cleanup cannot be confirmed
(`web/federation_permissions.go:254-257`; `cmd/sage-gui/node.go`).

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

## Directional domain Copy (peer-RBAC v3; legacy v1/v2 compatible)

Copy is opt-in store-and-forward replication of committed memories in concrete
domain subtrees over the authenticated federation transport. It is not implied
by trust or Read. Everything about routing and copy policy is off-consensus (no
fork); accepted copies enter the receiving chain as ordinary locally signed
`TxTypeMemorySubmit` transactions and pass the receiver's normal consensus
gates.

### Source permission and receiver choice

For a direct v3 peer, the source grants `Copy` in its normal peer-RBAC snapshot
and publishes that domain. The receiver independently decides whether to retain
it in local `subscribe_domains`. Effective source egress is:

```
source PeerRBAC Copy ∩ source Publish ∩ receiver Subscribe
```

Receiver admission independently rechecks `remote Publish ∩ local Subscribe`.
An absent peer-RBAC policy and a configured-empty policy both deny v3 Copy; v3
never falls back to tx-33. These intersections and the fail-closed identity/CA
binding are applied by `pairwiseEgressPolicy` and `pairwiseIngressPolicy`
(`internal/federation/sync_outbox.go:351-428`).

Both original host and original guest may publish their own Copy offer and
subscription after JOIN. They cannot edit the other node's choice. Lanes are
complete, sorted, revisioned snapshots: an omitted lane is preserved by the
dashboard API, while an explicit empty array clears that lane. Changing either
lane requires no re-pair (`web/federation_join.go:414-527`). Subtree semantics
remain: selecting `hr` covers `hr.public`.

The permissions endpoint keeps the local Publish lane aligned with the current
peer-RBAC `Copy` rows while preserving the receiver's separate Subscribe lane
(`web/federation_permissions.go:600-615`). `SetDirectionalSyncPolicy` also
rejects any v3 Publish domain that lacks the source's current `Copy` grant, so a
caller cannot bypass the permissions UI (`internal/federation/sync_policy.go:252-306`).

### `PUT /fed/v1/sync/policy` (peer-facing, behind `peerAuth`)

Current direct peers exchange
`{version:3, epoch, revision, publish_domains, subscribe_domains}` and receive
`{status, revision}`. The receiver requires an active frozen chain/operator/CA
binding, validates concrete domains, verifies the monotonic revision and
snapshot hash, and atomically replaces that peer's remote lanes
(`internal/federation/sync_policy.go:154-196`, `221-249`). Replays are
idempotent; conflicting content at the same revision fails closed.

Versions 1 and 2 retain the historical host-controlled `domains` snapshot for
compatibility with legacy links. Current dashboard links use v3. A historical
tx-33/sync-domain fallback never becomes a v3 Copy grant.

### `POST /fed/v1/sync/push` (peer-facing, behind peerAuth)

`{items: [{origin_chain_id, origin_memory_id, origin_created_at, domain, classification, memory_type, confidence_score, content, content_hash, tags}]}` - max 8 items, 64KB content each, and at most 32 canonical tags of 128 bytes each; `content_hash` must equal sha256(content). For pairwise delivery the origin chain must equal the authenticated peer. A different origin is accepted only as an authorized sync-group relay with a verifiable origin signature (`internal/federation/sync_server.go:140-160`, `535-553`). Handler: `handleSyncPush` in that file.

For a direct v3 origin, admission uses the Copy/Subscribe intersection instead
of tx-33 domain scope. For a v3 group relay, the exact active group
owner/member/domain projection is a separate RBAC capability: it neither
borrows nor requires the direct pairwise Copy/Subscribe lanes. It still sits
under the original JOIN trust edge, so the live relayer must match that edge's
frozen chain/operator/CA binding before any group capability is considered.
Membership removal closes the group lane without changing transport trust;
JOIN revocation closes every lane. Legacy pairwise and legacy group-relay paths
retain their historical tx-33 treaty gates. All paths then enforce
`classification <= max_clearance`, idempotency through the `sync_origin`
ledger, cross-domain duplicate protection, vault-locked retry behavior, and
ordinary locally signed submission
(`internal/federation/sync_server.go:169-226`, `516-533`).

For relays, `sync_origin` also stores the exact verified origin operator key.
Digest enumeration and sender routing require that key and its signature to
match the origin roster entry in the same eligible group as sender/receiver;
same-domain groups are tried independently so a lexically earlier group with a
different origin key cannot shadow the correct route. The sender verifies the
persisted provenance again before the network call, and the receiver accepts
only a signature under one of its eligible same-group origin keys
(`internal/store/sync_group_tables.go`; `internal/federation/sync_outbox.go`;
`internal/federation/sync_server.go`).

Per-item outcomes: `accepted`, `duplicate`, `rejected_cross_domain_dup`,
`rejected_clearance`, `rejected_not_consented`, `rejected_domain_scope`,
`rejected_write_access`, `rejected_origin_sig`, `rejected_not_admitted`, and
`retry` (`internal/federation/types.go:215-252`).

### `POST /fed/v1/sync/digest` (peer-facing, behind peerAuth)

Anti-entropy: the sender asks which of its memories the receiver has already
**admitted** in a domain subtree. `{domain, after, limit}` -> `{consented,
consented_domains, origin_memory_ids (sorted asc), next_cursor}` - cursor-paged,
2000 ids per page. It is answered from the admitted-origin ledger, not the
current committed set, so the receiver's sovereign lifecycle can deprecate a
copy later without re-opening delivery. Policy/clearance rejections are not
recorded there and remain eligible after permissions change
(`internal/federation/sync_server.go:239-310`, `452-514`).

`GET /fed/v1/status` now advertises `capabilities: ["sync"]` when the backend supports it; pre-v11.5 peers 404 on the sync routes and the sender parks deliveries on a 1-hour backoff.

### Local operator surfaces

| Route | Purpose |
|---|---|
| `GET /v1/dashboard/federation/connections/{chain_id}/sync` | Return local/remote Publish and Subscribe lanes plus revision state. |
| `PUT /v1/dashboard/federation/connections/{chain_id}/sync` | For v3, replace `publish_domains` and/or `subscribe_domains`; omitted lanes are preserved. The current UI sends only `subscribe_domains` for the receiver's “Save here” choices. |
| `GET .../sync/status` | Outbox counts per state, rejected rows with reasons, pending retry schedule, last anti-entropy run, peer consent, and unsupported-peer state. |
| `GET/PUT /v1/federation/cross/{chain_id}/sync` | Signed operator surface for the same v3 directional lanes; true legacy v1/v2 links retain the old host-controlled `{"domains": [...]}` model. |

The v3 dashboard and signed REST GET/PUT response fields include
`publish_domains`, `subscribe_domains`, `remote_publish_domains`, and
`remote_subscribe_domains` (`web/federation_join.go:344-527`;
`api/rest/federation_handler.go:447-518`, `676-719`). The signed PUT accepts
either or both directional fields, preserves an omitted lane, and clears an
explicit empty lane. It rejects the legacy `domains` field on v3; legacy links
reject directional fields. Revoking an
agreement atomically purges the peer's sync domains, queued deliveries,
controller binding, cached CA/seed, and persisted P2P route. Durable
pending-origin quarantine markers are retained until an exact committed mirror
can be promoted, because deleting an ambiguous in-flight marker could later
make a committed foreign copy look native.

### Delivery semantics - the honest version

- **Sender**: a 30s drainer scans committed memories in effective Copy subtrees into a durable outbox (`sync_outbox`, survives restarts; recovery is the scan itself - no watermark), delivers in batches of 8, and re-runs **every** gate at send time (withdrawn Copy, removed receiver subscription, narrower legacy scope, or a re-domained/re-classified memory drops out). A commit-tail hook accelerates delivery; the poll is the correctness backstop. Backoff `min(30s * 2^attempts, 1h)`. Vault-locked content defers without burning attempts.
- **"Delivered" means handed to the peer's pipeline.** The copy lands `proposed` (co-commit-style direct commit does not apply) and the receiver's own voter governs it. Peer lifecycle stays sovereign: decay, corroboration, and deprecation never propagate in either direction.
- **The copy's on-chain author is the receiving node's operator agent** - everything entering chain state is a locally-signed tx. Origin truth (`origin_chain_id`, `origin_agent_pubkey`, `origin_memory_id`, `origin_created_at`) lives in the receiver's off-consensus `sync_origin` ledger.
- **Direct pairwise copies are never silently re-forwarded.** An A->B copy cannot become B->C merely because B trusts C. Re-forwarding exists only through an explicit sync-group relay in which A, B, and C are exact active members for that domain and A's stored origin signature verifies under A's same-group roster key.
- Before broadcasting a received copy, the receiver writes a durable pending-origin quarantine keyed to its deterministic local memory ID and immutable content/domain/classification/type/operator tuple. Commit watchers and restart scans treat pending and admitted origins alike for loop prevention. After a crash, retry promotes an exact already-committed copy without rebroadcasting. A definitive rejection removes the marker; an ambiguous timeout retains it, including across revocation, because the transaction may still commit later.
- Copies arrive **without embeddings** (semantic recall needs a receiver-side re-embed; keyword/FTS recall works immediately). User tags are replicated with the memory in v11.6.
- SQLite-only: Postgres-backed nodes disable sync loudly (501 on the routes, drainer no-op, no `sync` capability).
- **First sync into a domain that does not yet exist on the receiver auto-registers that domain** with the receiving node's operator agent as owner (a level-2 self-grant), exactly as an ordinary local submit into a new domain does. This is a consensus-visible RBAC effect triggered by a peer's push - benign (the receiver's own operator owns it, never the peer) but worth knowing: an operator wanting tighter control should pre-create the domains they consent to sync.
- **Rejections are re-evaluable, not permanent.** Only admissions are recorded on the receiver; a `rejected_not_consented` / `rejected_domain_scope` / `rejected_clearance` outcome is receiver-config-dependent and stays retryable on the sender (long backoff, attempt-capped), so widening permission, subscription, or clearance later lets a backlog self-heal. A `rejected_cross_domain_dup` is content-derived and terminal on the sender. The duplicate check only inspects domains visible to that peer (peer-RBAC Read for v3, treaty scope for legacy), so it never reveals hidden-domain holdings.
