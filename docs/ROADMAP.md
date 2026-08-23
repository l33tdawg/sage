# SAGE Roadmap

**Status (2026-08):** **v11.19.0 is the current release.** It keeps the
pairwise exported-agent federation model, safe registered-name addressing and
reply-event visibility, the three-tab Access Controls redesign, five-minute
JOIN route discovery, complete stopped-node backup/restore/preflight tooling,
and the governed app-v21 → app-v22 legacy-lineage recovery ceremony from
v11.18.0. It makes a reply to a message you sent readable from MCP through the
advertised `sage_message_replies` tool and a payload-free `sage_inbox` pointer,
closing a gap where a completed reply existed only on a REST projection no MCP
tool called. It moves per-session auto-connect guidance into MCP
`initialize.instructions`, leaving every tool result payload clean even for
clients that skip initialization; a later handshake returns cached guidance. It
also corrects the exceptional app-v21 → app-v22 recovery lane so proven
skip-ahead transitions remain virtual, evidence-bound history rather than
synthetic applied-upgrade records. Existing app-v22 through app-v26 chains are
not rewritten. v11.18.3 adds the process-wide signer fence described below;
v11.18.4 adds one-call reply-aware inbox polling, mandatory exact-toolchain
vulnerability gates, and conservative millisecond pipeline retention.
v11.18.5 closes the remaining installed-binary/MCP-session skew by handing an
unread request to the replacement runtime before stale tools can execute it.
v11.18.6 adds exact CometBFT H and H/H+1 updater snapshot
provenance/replay-boundary proof, bounded exact-generation federation Retry,
and safe fingerprinting for memory-reassign failure logs. v11.18.7 adds bounded
JSON-RPC POST transport and independently checked, operator-configurable limits
for large signed CometBFT transactions, raises the deterministic app-v20
atomic-finalize limit to 1.2 MB,
makes route refresh admission deadlock-safe, restores authenticated route
bootstrap for P2P-only peers across a trust-generation change, and gives
security evidence precedence in federation diagnostics. The supported
v11.18.8 transport seam prevents transparent HTTP redelivery across every
fenced Comet submission path, preserves signer-fence restart diagnostics, and
makes MCP reply polling recover from unsafe or unverifiable forward watermarks
instead of reporting a false empty page. v11.18.9 types every ambiguous shared
Comet commit or sync outcome as indeterminate, fails closed if federation sync
ever receives neither a result nor an error, and pins the deliberate decoder
differences between the shared transaction package and CEREBRUM. The supported
v11.18.10 message control plane attributes every claim to an opaque MCP session,
supports compare-and-swap ownership handoff, rejects replies from a stale former
owner, and preserves passive recovery after a lost response. CEREBRUM also
projects its RBAC-filtered agent channels as a traffic-weighted 3D connectome.
v11.18.11 adds operator-only live connectome firing driven by contentless ticks
and caller-filtered snapshot refetches, strips memory plaintext from retrieval
activity events, makes bookend inbox visibility payload-free and self-healing,
and raises every current Go build floor to patched 1.25.13. v11.18.12 adds the
projection-safe agent-as-lobe engram view, hardens the exact 20-event operator
dashboard SSE registry, restores signed task creation across official clients,
adds exact-agent message presentation without changing claim/read/delivery
semantics, and repairs release-facing documentation drift with fail-closed
citation coverage. v11.18.13 ships Hubanov's distributed-engram bridges with
deterministic bounded corroborator evidence and lifecycle hardening, removes
the floating Connectome caption while strengthening mode accessibility, adds
Claude's explicitly enabled signed production wake source with lossless
backpressure, and closes the MCP claimant-session compatibility fallback
bypass. v11.18.14 keeps claimed work visible to wake and inbox consumers,
adds exact payload-free stranded-claim state and a lease-free wake snapshot,
arms Claude Code wake by default, and gives its optional Stop hook a monotonic
one-shot recovery signal. It preserves sender-selected canonical message TTLs,
adds a persistent accessible Connectome agent inspector, batches and totally
orders agent-as-lobe corroborator reads, and documents the enforced 31-day
timeline range. v11.18.15 backfills wake state for claimed-only upgraded work,
makes deprecated exact-local pipe admission atomic with its durable generation,
restores the experimental Claude notification adapter to explicit opt-in,
adds deterministic `memory_id` ordering for timestamp ties, and makes citation
repair anchor-aware and fail-closed over new parser debt. v11.18.16 gives the
exact current claimant a bounded passive view of its unfinished work, keeps
new-work counts and claim ownership unchanged, and makes hook inbox status
consult the payload-free durable wake snapshot. v11.18.17 persists the primary
stdio claimant identity per exact agent/provider/project and reuses it only
after an OS liveness lock proves the prior runtime is gone, while concurrent
runtimes remain independently fenced. v11.18.18 makes the Codex startup
self-healer compare every fully rendered lifecycle hook and makes Connectome
neurons the primary agent-detail navigation surface, with a compact
relationship-only fallback selector. v11.18.20 also keeps a last verified MRI
snapshot visible across a transient same-mode refresh failure. v11.18.21 makes
that renderer the sole authority for the central unavailable overlay, so an
independent domain-inventory refresh failure cannot cover a verified graph.
v11.18.22 publishes verified MRI core readiness before optional renderer setup
and feature-gates the bundled runtime's absent `clickAfterDrag` helper, keeping
the anatomical hull, controls, and auto-rotation alive after the graph paints.
v11.18.23 restores trust and lifecycle parity between `sage_turn` and
`sage_recall`, discloses active/store embedding-space mismatches through
readiness without blocking intentional re-embedding, and makes managed reranker
setup surface proven host-loader incompatibilities with bring-your-own guidance.
v11.18.24 adds session-fenced federated claim recovery and idempotent reply
events, removes imperative boot mutations from MCP result traffic, scopes
durable retention labels to actionable work, and tightens likely-alias
embedding diagnostics.
v11.18.25 keeps CEREBRUM task refreshes mounted while fencing stale responses
and preserving authoritative read failures, and makes Codex hook shell
migration portable without replacing custom user hooks.
v11.18.26 refreshes the validated Go dependency baseline and the pinned CodeQL
and native-shell CI actions without changing SAGE runtime behavior.
v11.18.27 adds caller-safe empty semantic-recall completeness disclosure,
fenced across the exact projection and embedding-space source so agents can
distinguish genuine absence from temporarily unreachable committed memory.
v11.18.28 restores readable compile-time shared namespaces while preventing
reserved or governance-promoted shared domains from becoming ownable.
v11.19.0 introduces app-v27: eligible immutable record authors gain narrowly
scoped challenge/reinstate authority in compile-time reserved shared domains,
and an omitted new-task `task_status` is canonically interpreted as `planned`.
v11.18.19 prevents Codex project-hook
self-healing from ever targeting the user-global `~/.codex` scope and removes
the Connectome's competing DOM/ForceGraph click paths while bounding raw access
metadata and making bloomed memories responsive. The supported consensus
ceiling is app-v27.

**Hard constraint driving the whole plan:** no chain reset. Existing chains must
upgrade in place across all future releases. Routine personal-node upgrades
remain automatic; the exceptional legacy-lineage repair is deliberately an
explicit, reviewed operator ceremony rather than a silent mutation.

## v11.19.0 release

App-v27 makes exactly two consensus changes.

First, the immutable author of a record in the compile-time reserved shared
domains `general`, `self`, `meta`, and `sage-*` may challenge that record and
may reinstate its open challenge without separately holding level-3 Modify.
The authority is record-scoped and does not extend to governance-promoted
shared domains. Pending or inactive enrollment, read-only/profile restrictions,
shared-write denies, and classification/clearance failures remain hard denials.
For app-v21 weighted challenges, an eligible author is included in the frozen
electorate when the round opens.

Second, an omitted `task_status` on a signed new-task request is canonicalized
to `planned` after app-v27. REST transaction construction and consensus proof
verification derive the same value. Pre-app-v27 blocks retain the historical
explicit-field contract, so replay and AppHash behavior are unchanged.

App-v27 activates through governance from app-v26, applies its rules at H+1,
and requires no state migration.

## v11.18.28 patch

Active local principals can once again read records in the compile-time shared
domains `general`, `self`, `meta`, and `sage-*`. Record classification remains
authoritative, so private and restricted records are not exposed by the shared
domain rule.

Access-grant processing rejects attempts to register either a compile-time
shared namespace or a governance-promoted shared domain as owned. REST surfaces
that conflict as forbidden, and the API, SDK, and RBAC references describe the
same boundary.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.28 does not introduce app-v27.

## v11.18.27 patch

Empty, domain-scoped semantic recall can now report whether the result proves
genuine absence, is incomplete because committed rows are unreachable in the
active embedding space, or is unavailable because the caller/query/projection
cannot support a safe verdict. REST and MCP expose the same empty-only signal.

Completeness is bound to the caller's full-domain visibility and an exact,
source-stable canonical projection. SQL, canonical, embedding-space, and vault
generations fence the query and bounded indexed probe, while narrowed and
federated universes fail closed rather than overstating absence.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.27 does not introduce app-v27.

## v11.18.26 patch

This maintenance patch updates the validated Go dependency baseline (`testify`
1.12.0, `x/crypto` 0.55.0, and `x/tools` 0.49.0) and the pinned CodeQL and
native-shell CI actions. It also binds new app-v23 HTTP MCP bearers to existing
approved locally managed agent identities, instead of creating structurally
unapprovable pending principals, and makes the token-create CLI help path
side-effect free.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.26 does not introduce app-v27.

## v11.18.25 patch

CEREBRUM background task refreshes no longer replace the mounted board with a
loading surface. Successful authoritative reads recover visible errors, failed
silent reads cannot be interpreted as confirmed absence, and generation
fencing prevents an older completion from overwriting newer state or
resurrecting a settling task.

Codex hook installation and self-heal resolve the Bash executable available on
the host instead of assuming one filesystem layout. Migration replaces only
installer-owned SAGE hook commands; unrelated lifecycle hooks, custom events,
and top-level JSON remain user-owned and preserved.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.25 does not introduce app-v27.

## v11.18.24 patch

Inbound federated claims now receive opaque MCP session ownership before their
payload is exposed. Pre-v11.18.24 retained claims migrate to an explicit
`legacy` compare-and-swap fence for deliberate recovery; SAGE never uses a
timeout to steal live work. Federated completion atomically verifies the
claimant session, completes workflow state, stores an encrypted result
fingerprint, and persists the return event, making identical lost-response
retries replay-safe and conflicting second results explicit.

MCP auto-connect standing is confined to `initialize.instructions`; ordinary
tool results remain payload-only even when a client initializes late. The
imperative boot block that requested tool invocation and local file mutation is
removed. Message retention labels now describe only pending/claimed actionable
work, and the embedding readiness guard's likely-alias classifier requires one
qualified and one bare spelling rather than conflating two organizations.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.24 does not introduce app-v27.

## v11.18.23 patch

The every-turn recall block now carries `corroboration_count` and lifecycle
`status`, matching the decision-relevant evidence already exposed by explicit
`sage_recall`. Boot also compares the active embedding space with vector spaces
already present in the non-deprecated local store. Mismatches are logged and
reported under `/ready` as degraded, while ordinary serving remains available
and strict readiness can gate reconciliation. Managed reranker installation now
preflights the verified engine binary and reports proven GLIBC, GLIBCXX, and
CXXABI loader failures with the operator-controlled external-engine path.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.23 does not introduce app-v27.

## v11.18.22 patch

The CEREBRUM MRI renderer now establishes and publishes its verified core graph
before applying optional ForceGraph configuration. Optional post-render failures
therefore retain the real graph and truthful counts instead of presenting a cold
unavailable overlay. The bundled runtime's absent `clickAfterDrag` helper is
feature-gated so anatomical hull construction, controls, and rotation continue.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.22 does not introduce app-v27.

## v11.18.21 patch

The central MRI unavailable overlay now follows only the renderer's verified
graph state. The independent domain inventory continues to report and retry its
own failures locally, but can no longer cover a safe graph that is already on
screen. Genuine cold graph failures and failed Memory/Connectome switches keep
their existing fail-closed behavior.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.21 does not introduce app-v27.

## v11.18.20 patch

The CEREBRUM MRI renderer now records which view mode produced the currently
rendered verified snapshot. A transient live-refresh failure keeps that snapshot
visible only when the failed request belongs to the same mode; cold failures and
failed Memory/Connectome switches remain explicitly unavailable and continue
retrying. This removes the contradictory overlay that could cover a valid brain
while the domain inventory remained populated.

This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.20 does not introduce app-v27.

## v11.18.19 patch

Codex self-healing now rejects the user-home/global configuration scope before
inspecting or creating lifecycle artifacts. A normal global MCP registration
therefore cannot grow a global Stop hook that injects one project's durable
inbox work into unrelated tasks. Existing project-local byte-exact repair is
unchanged.

The Connectome now routes node, link, and background clicks through the graph
library's raycast result with one explicit six-pixel pointer tolerance. The
hit-test-free DOM fallback and its stale timing heuristic are removed. Primary
traffic, connection, and memory details render before domain-access metadata;
large raw values remain available inside a bounded disclosure, tooltips are
clamped, and bloomed memory nodes provide hover and accessible click feedback.
This patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.19 does not introduce app-v27.

## v11.18.18 patch

Codex project self-healing now byte-compares every installer-owned hook against
the current template rendered with the active binary and pinned identity. Any
stale member rewrites the complete five-script set, closing the mixed-generation
upgrade failure where an obsolete no-op Stop hook survived because sibling
scripts still referenced the current executable.

The CEREBRUM Connectome now gives neurons a larger click target and explicitly
leads operators to click the graph for persistent agent details and directed
relationship inspection. Its fallback selector is bounded to agents with
visible peer relationships, ordered by visible retained traffic; selected
isolated neurons remain represented for keyboard continuity. The endpoint and
both-endpoint RBAC projection are unchanged. This patch introduces no new
consensus application version or state migration: app-v26 remains the ceiling
and v11.18.18 does not introduce app-v27.

## v11.18.17 patch

The primary stdio MCP runtime now stores one opaque claimant identity under
`SAGE_HOME/runtime/mcp-claimants/`, scoped by the exact signed agent, provider,
and project. It holds a platform-native advisory lock for the runtime lifetime.
An ordinary restart therefore reopens the same session's unfinished work only
after the earlier process is dead, while a concurrent process falls back to a
separate random claimant and cannot silently share ownership. Installed-runtime
handoff carries the identity while the parent retains the lock.

Pre-v11.18.17 random claimant IDs are not bulk-adopted; explicit CAS-fenced
handoff remains their recovery path. HTTP MCP conversation identities remain
transport-scoped. This patch introduces no new consensus application version
or state migration: app-v26 remains the ceiling and v11.18.17 does not
introduce app-v27.

## v11.18.16 patch

The canonical MCP inbox now exposes `own_claimed_unfinished` separately from
newly claimed `items`. Only unfinished, unexpired exact-local messages owned by
the exact current agent and claimant session appear. Repeated reads are passive,
the list is bounded while its total remains exact, and same-call claims cannot
appear in both projections because prior ownership is read before new work is
claimed. Completion removes a row normally; no automatic handoff or ownership
recovery is introduced.

The short-lived inbox hook also reads the lease-free durable wake snapshot, so
claimed work keeps the status surface non-empty without exposing message IDs,
senders, intents, payloads, or counts. An older node, alternate backend, or
transient failure of the additive projection is explicit but cannot take down
the primary inbox. This patch introduces no new consensus application version
or state migration: app-v26 remains the ceiling and v11.18.16 does not
introduce app-v27.

## v11.18.15 patch

Startup wake migration now allocates a non-zero catch-up sequence when an
upgraded recipient's only unfinished exact-local work is already claimed, not
only when a pending row remains. The deprecated `/v1/pipe/send` path now gives
every exact local recipient the same atomic row-plus-generation invariant:
keyed sends retain canonical replay semantics, unkeyed sends use a dedicated
single-transaction admission primitive, publication follows commit, and an
incapable backend returns 501 before inserting anything. Provider-only and
federated work remain outside the exact-local sequence.

The custom Claude wake notification channel is opt-in for every host because
end-to-end delivery from a plain `.mcp.json` server through Claude Code's
plugin-scoped gate remains unverified, and an idle adapter must not acquire the
exclusive exact-agent wake lease. Pending memory batches now use
`memory_id` as the final SQLite/PostgreSQL tiebreak when timestamps match.

Documentation citations now parse bounded newline-separated symbol/path pairs
and hyphenated Go directories. A versioned registry records each concrete
declaration, lead-comment, or interior anchor; automatic repair is limited to
accepted declaration anchors and keeps their recorded line current. Lead,
interior, unknown, or false quoted claims are reported for human review. The
remaining legacy skipped and bare references are pinned as explicit debt, so
new skipped coverage or lost checked coverage fails without forcing a bulk
migration. This patch introduces no new consensus application version or state
migration: app-v26 remains the ceiling and v11.18.15 does not introduce app-v27.

## v11.18.14 patch

Canonical Messages now remain visible while unfinished, whether still pending
or already claimed. Same-cursor wake reconnects replay stranded work, the MCP
inbox carries an exact payload-free claimed-elsewhere tri-state, and a signed
lease-free wake snapshot lets short-lived hooks observe the durable monotonic
sequence without acquiring or disrupting the exclusive SSE consumer lease.
Claude Code and Codex project sessions arm the Stop-only turn-boundary nudge by
default. It fails open—including when its one-shot cursor cannot be persisted—
and uses per-session durable sequence state so a declined generation cannot
trap the session indefinitely. The experimental custom Claude notification
adapter remains explicit opt-in for hosts with confirmed end-to-end delivery.

Claimant-session conflicts stay typed instead of falling through a legacy 404
path, and recovery remains explicit through passive history plus compare-and-
swap handoff. The every-open canonical retention migration now matches only the
exact historical 24-hour stamp by epoch seconds, preserving deliberate bounded
TTLs while still rescuing RFC3339Nano production rows.

CEREBRUM's Connectome adds reliable escaped hover identity and a persistent,
nonmodal agent inspector shared by click, tap, and keyboard selection. Exact
identity, visible retained traffic, peers, activity, and memory-lobe states
remain useful through loading, empty, partial, error, live-refresh, mobile, and
reduced-motion paths without hiding established guidance.

Agent-as-lobe corroborator presentation uses one bounded deterministic batch
with matching SQLite and PostgreSQL total order instead of N+1 exact-ID reads.
The `sage_timeline` MCP schema now advertises the server's enforced 31-day
range. This patch introduces no new consensus application version or state
migration: app-v26 remains the ceiling and v11.18.14 does not introduce
app-v27.

## v11.18.13 patch

Hubanov's distributed-engram contribution now blooms a selected agent's visible
memories with transient links to rendered corroborating neurons. Agent and
memory graph identities are namespaced, same-neuron requests are generation
fenced, and every exit or replacement path removes transient nodes and bridges.
SQLite and PostgreSQL read one deterministically ordered, indexed 96-row
evidence prefix before authorization and deduplication; the response exposes at
most 12 bridges and keeps corroboration documented as historical evidence.

The floating Connectome instruction card is removed. Guidance uses the existing
reading panel, while native controls retain keyboard focus, stable toggle
semantics, visible dark/light pressed states, intentional mobile Reset behavior,
and a live assistive status announcement.

Claude's production MCP wake source is opt-in through `SAGE_CLAUDE_CHANNEL` and
consumes the existing signed, payload-free SSE route with a random process lease
and resumable cursor. Saturated delivery is lossless and shutdown releases the
reader before a consumer drains buffered events.

Canonical typed message-reply denials no longer fall through the deprecated
pipe mutation. The hidden compatibility alias supplies the active claimant
session, preserves genuine plain-404 old-node fallback, and reports truthful
local/federated scope. This patch introduces no new consensus application
version or state migration: app-v26 remains the ceiling and v11.18.13 does not
introduce app-v27.

## v11.18.12 patch

CEREBRUM can lazily bloom one selected agent's highest-confidence visible
memories around its connectome neuron. The bounded indexed lookup retains the
operator-only and app-v23 per-record projection gates, and the browser clears,
fences, and disposes stale lobe requests safely.

The dashboard SSE contract now audits one exact 20-event operator registry,
wires the seven previously missing events, and keeps message wake, MCP, and
wizard protocols route-local. Typed control-flow analysis and executable client
tests fail closed on dead, aliased, escaped, build-tagged, or decoy sinks.

Official signed task constructors now include the required initial `planned`
status; REST rejects an omitted signed task status before broadcast rather than
constructing a deterministic app-v23-through-app-v26 proof mismatch. Message
and pipe responses expose exact immutable sender/counterparty IDs beside mutable
friendly labels through one bounded batch metadata query on healthy production
stores with a bounded exact-ID fallback, suppress foreign-chain collisions,
preserve authorized rows on lookup failure, and keep count-only responses
identity-free.

Release-facing documentation now matches the 33-tool MCP inventory and current
dependency/route contracts. Symbol-anchored code citations are repaired and
their verified coverage fails closed if a checked citation silently becomes
unresolved. This patch introduces no new consensus application version or state
migration: app-v26 remains the ceiling and v11.18.12 does not introduce
app-v27.

## v11.18.11 patch

CEREBRUM now turns successful local message sends into contentless connectome
invalidation ticks. The existing operator-only snapshot remains the source of
truth: the browser refetches it under the current caller's authorization and
pulses only new synapses. Monotonic generations retain ticks across concurrent
loads and retry interleavings; initial and ordinary refreshes never produce
false firing.

Recall, search, and hybrid SSE activity is contentless apart from event type and
result count, so authorized memory plaintext is no longer duplicated into the
identity-free broadcaster. Claude bookend hooks now expose a signed,
payload-free unread pointer that neither claims messages nor reveals IDs or
content, while preserving unrelated user hooks during upgrade self-heal.

Local connectome locality now follows chain identity rather than provider label,
the obsolete app-v7 validator warning is suppressed after app-v14, the visual
skull is dimmer, and Windows executable assets receive checksum sidecars. All
current builders, CI, and release contracts require patched Go 1.25.13. This
patch introduces no new consensus application version or state migration:
app-v26 remains the ceiling and v11.18.11 does not introduce app-v27.

## v11.18.10 patch

Canonical message claims now record an opaque claimant-session identity rather
than only the shared agent identity. Concurrent runtimes still get exactly one
claim winner; passive history exposes the winning session, an explicit atomic
handoff compare-and-swaps ownership, and stale sessions cannot complete work
after handoff. Receive-token replay and legacy direct-client compatibility are
preserved.

CEREBRUM adds an operator connectome mode backed by the existing
`/v1/dashboard/network/synapses` projection. Agents render as neurons and
directed message channels as traffic-weighted synapses, with both-endpoint RBAC
filtering preserved. Mode requests are generation-fenced so slow or reordered
responses cannot cross the memory/connectome boundary. This patch introduces
no new consensus application version or state migration: app-v26 remains the
ceiling and v11.18.10 does not introduce app-v27.

The upgrade watchdog now applies one bounded context across nonce-lease
acquisition and each of its four CometBFT broadcasts. Deadline expiry after a
submission begins stays typed indeterminate and retains the exact signer/bytes
fence; the lease is never released merely because time elapsed.

## v11.18.9 patch

Ambiguous CometBFT commit and sync outcomes are now typed at the shared
broadcaster boundary. Transport, status, RPC, decode, shape, hash-binding, and
missing-height failures return `ErrSubmitIndeterminate` for valid signing keys,
while the existing live-registration path remains an independent fence
backstop. Pre-send request-construction failures remain definitive because no
bytes reached a transport.

Federation sync now fails closed if its commit broadcaster violates its
contract by returning neither a result nor an error. The exact signer and
encoded transaction remain fenced until reconciliation proves their fate. A
cross-package decoder contract pins the shared HTTP prologue while recording
the deliberate verdict and envelope-tolerance differences between
`internal/tx` and the CEREBRUM web path. This patch introduces no new consensus
application version or state migration: app-v26 remains the ceiling and
v11.18.9 does not introduce app-v27.

## v11.18.8 patch

Every fenced CometBFT submission path now uses one shared non-reusing HTTP/1.1
transport seam. Commit, sync, byte-identical nonce-fence reconciliation, and
CEREBRUM submissions write the transaction on one connection, preventing Go's
HTTP transport from transparently replaying the same signed bytes to another
responder after a reused connection fails while reading its response. An
indeterminate outcome remains fenced for explicit reconciliation. Restart
failure reporting preserves the signer-fence veto ahead of a generic drain
timeout.

MCP coordination now rejects `reply_since` values that are later than the
authoritative retained-reply head or cannot be validated because no head is
available. `sage_inbox` recovers the newest passive reply page for
deduplication, marks complete and truncated baselines consistently, and never
claims recovery when the page fetch failed. A successful outbound message send
also performs one bounded sender-exact passive inbox snapshot, surfacing inbound
work that arrived after an earlier empty poll. This patch introduces no new
consensus application version or state migration: app-v26 remains the ceiling
and v11.18.8 does not introduce app-v27.

## v11.18.7 patch

Large signed transactions no longer overflow CometBFT request headers. Smaller
broadcasts retain the established GET wire shape; larger commit, sync, and
byte-identical nonce-fence reconciliation requests use bounded JSON-RPC POST.
Client transaction and JSON-RPC body limits are independently range-checked,
capped at 8,000,000 bytes, and refuse an oversized request before send.
Operators raising them must configure matching CometBFT limits. Independently,
every validator enforces a 1,200,000-byte aggregate raw-transaction budget for
app-v20 atomic finalization, sufficient for the measured 1,304-entry
SkillRegistry transaction. Memory content remains bounded at 512 KiB, while the
canonical signed `AgentRequest` proof has its own 600,000-byte consensus bound,
admitting the measured 573,723-byte proof without widening the content or
aggregate limits. Response handling accepts strict quoted or numeric `int64`
heights, rejects fractional, exponent, null, malformed, and out-of-range heights,
and refuses unsupported content types.

v11.18.7 removes recursive sync-policy lease acquisition from federation peer
request completion paths. Refresh admission is policy-free, bounded to one
pending worker per peer, and resolves the current agreement and binding only in
the asynchronous worker. Failed-request and successful-Direct triggers remain
active, while the route-exchange endpoint itself cannot recursively trigger a
refresh.

P2P-only peers may now use missing or stale route material solely as a
connection hint for the authenticated `/fed/v1/p2p/routes` bootstrap exchange.
The current agreement's pinned mTLS identity remains authoritative. All
protected requests reject missing or cross-generation snapshots with
`trust_generation_mismatch`. A matching-generation empty target set remains
explicitly pinned and cannot fall back to current configuration.

Federation route diagnostics classify mixed route-availability plus certificate,
SPKI, pin, identity-mismatch, or security-block evidence as `security_blocked`;
revocation, expired or unknown agreement, trust-failure, or authentication
evidence is classified as `trust_failure`. Both verdict classes outrank route
availability. This patch introduces no new consensus application version or
state migration: app-v26 remains the ceiling and v11.18.7 does not introduce
app-v27.

## v11.18.6 patch

v11.18.6 makes updater snapshot publication and reuse prove both supported
CometBFT layouts. Application Badger and persisted consensus state must match
at `H` and agree on the application hash. A blockstore committed through `H`
is accepted only after its `H` block ID and seen commit match that state. If
the blockstore is durably one block ahead at `H+1`, SAGE additionally verifies
the complete block and part identity, direct-parent and state-derived header
fields, last and seen commits, validator signatures, and CometBFT's replay-time
block validation. Regression coverage restores the candidate and runs the real
CometBFT handshaker, proving exactly one replayed block and safe restart reuse.
Malformed or more-than-one-ahead provenance is rejected, and an invalid prior
publication is quarantined before a valid replacement can be published.
Cancellation always blocks executable updater handoff, although a safe snapshot
may already have been atomically published. Non-empty `H+1` evidence is
retryable until application and state catch up.

Federation's operator Retry is now a separate bounded recovery contract rather
than an alias for ordinary status polling. Concurrent waiters share one refresh
and one authenticated re-probe, route targets remain frozen to the captured
JOIN generation, security denials stop the workflow, and agreement/binding
changes before or during the response invalidate it. Stable typed diagnostics
drive Retry versus Pair again without retrying generic mutating requests.

Memory-reassignment broadcast-failure logs now use fixed 96-bit truncated
SHA-256 fingerprints (24 lowercase hexadecimal characters) instead of raw
request-controlled agent IDs. This closes the two CodeQL log-injection findings
while retaining a correlation handle. There is no new consensus application
version. The process-local signer-fence residual is
unchanged: crash/restart and independent signing processes remain outside the
guarantee until durable cross-process pre-broadcast intent lands.

## v11.18.5 patch

v11.18.5 makes the v11.18.4 one-call coordination contract survive later SAGE
binary upgrades. A long-lived stdio MCP process snapshots its exact executable;
when that path is replaced, it launches the installed runtime and replays the
single frame already removed from the pipe before passing through all remaining
input. The stale process never executes that frame. A child that has acquired
stdin also prevents fallback execution if it later exits. This is a
single-injection/no-stale-fallback guarantee, not a new durable exactly-once
execution protocol; callers still reconcile an indeterminate transport failure.

Every `sage_inbox` response now identifies `sage.inbox.v2`, the live MCP runtime
version, and that sender replies are embedded. This turns silent schema skew
into machine-detectable evidence. The first transition from a pre-11.18.5 MCP
process still needs one session restart; future replacements can hand off
automatically. There is no consensus change and app-v26 remains the ceiling.

## v11.18.4 patch

v11.18.4 makes `sage_inbox` the normal one-call coordination poll: inbound work
stays under `items`, while replies to messages the caller sent appear under
separate passive `reply_items` and never affect work counts. Inclusive
watermarks protect same-millisecond replies. A full page exposes an exact
composite-cursor catch-up action and makes the watermark explicitly unsafe to
advance until the bounded window is drained. Successfully fetched replies also
survive an independent task-notification endpoint failure.

The release raises the root and `natter` Go floor to 1.25.12, resolves that
exact toolchain in CI, release, CodeQL, and consensus fault workflows, pins Go
container builders to 1.25.12, and makes pinned root+natter `govulncheck` scans
mandatory before build or publication fan-in. It also fixes legacy
`PurgePipelines` comparisons: SQLite cutoffs are conservatively normalized to
millisecond precision, preventing an ambiguous newer terminal row from being
deleted early while preserving atomic outbox/parent cleanup. There is no new
consensus application version; app-v26 remains the ceiling.

## v11.18.3 patch

v11.18.3 closes same-key nonce inversion inside a running daemon. Dashboard,
REST, federation, voter, and upgrade-watchdog producers share one per-key lease;
once exact bytes reach CometBFT, any unproven outcome fences that key until
reconciliation proves the same transaction committed or permanently refused.
All daemon non-web adopters use the same strict, bounded, single-document Comet
decoder with exact-hash binding and an explicit positive height for success.
Live update status recomputes restart advice from the current fence state, and
coordinated restart remains vetoed while a key is fenced.

The fence is deliberately process-local. A crash, power loss, restart, or a
standalone CLI process sharing the daemon's key remains outside the guarantee
until durable pre-broadcast intent lands. The consensus ceiling remains
app-v26; there is no app-v27.

## v11.18.2 patch

v11.18.2 closes a sender-side reply-visibility defect. A recipient could call
`sage_message_reply`, the durable row flipped to `completed`, and the result was
retained — but the original sender had no advertised MCP path to it. The result
was attached only to the passive REST projection `GET /v1/pipe/results`, which
no MCP tool called, while `sage_inbox` returns work addressed to the caller and
`sage_message_status` is deliberately payload-free. In bookend clients the reply
was therefore invisible and work round-tripped.

The release adds `sage_message_replies` as an explicit, advertised sender-side
read — taking the advertised tool count from 31 to 32 — plus a payload-free
pointer inside `sage_inbox` carrying `retained_reply_count` and
`newest_reply_completed_at`. Replies never enter `sage_inbox` items and never
count as work: every reply item is `requires_reply: false`, `requires_result:
false`, and `data_only`. Authorization is the exact-sender SQL predicate
`from_agent = ?`, not the wider `callerCanViewPipe` rule, and no parameter names
another agent, so the surface cannot act as a message-existence oracle. Reads
are passive and replay-safe: they claim, acknowledge, and re-queue nothing.

`GET /v1/pipe/results` gains a payload-free `?count_only=1` probe and a
composite `(completed_at, pipe_id)` `before=` cursor. The composite cursor is
load-bearing rather than cosmetic: `completed_at` is stored at millisecond
resolution, so a timestamp-only cursor silently strands every reply sharing the
boundary millisecond — a burst of replies is routine, not an edge case. Store
backends lacking the optional capability answer `501` rather than an empty page
that would read as "no replies". Reply bodies are attributed to the agent that
actually completed the row rather than the addressee. The consensus ceiling
remains app-v26 and no consensus schema changes.

## v11.18.1 patch

v11.18.1 moves adaptive SAGE boot standing to the MCP initialization response.
Each transport session performs the signed boot check at most once, repeated or
concurrent initialization reuses the same instructions, and ordinary tool
results no longer carry the auto-connect preamble. A client that omits the MCP
handshake still receives clean tool results and can retrieve cached standing
through a later initialization handshake.

The app-v21 lineage doctor now emits schema-v2 retained-Comet transition claims
for proven skip-ahead history, independently verifies the exact transition and
block hash on every validator, and installs virtual predecessor coverage only
when app-v22 activates. It never fabricates heights or arms skipped historical
fork gates. New schema-v1 repairs fail closed; valid receipts already completed
on app-v22+ remain compatible. The binary ceiling stays app-v26.

## v11.18.0 completion ledger

v11.18.0 carries forward the 11.17 line's app-v23 through app-v26 governed upgrade path,
historical-memory recovery controls, responsive Access Controls, authenticated
Consensus loading, mutable agent display names, canonical Messages, deprecated
hidden `sage_pipe*` compatibility aliases, durable-until-handled message retention, signed
in-place macOS update support, roaming Direct/relay federation routes, and a
Docker lane covering LAN, isolated-network/relay, address churn, restart, and
offline inbox recovery. v11.17.8 additionally clears the tracked DTLS/STUN and
CodeQL security backlog without dismissing genuine alerts.

v11.17.9 completes the agent-directory UX pass (friendly names, wider Access
Controls rail, search and activity/name sorting, and agent-first page order) and
hardens recovery transfers against stale authorship hints, already-completed
retries, and host-versus-consensus clock skew. The Federation page also splits
outgoing and incoming domain permissions into direction tabs so only one large
matrix is mounted and scrolled at a time.

v11.17.11 completes [#117](https://github.com/l33tdawg/sage/issues/117): CPU-only
embedding providers batch natively, imports stay authorization-first and memory
bounded, current MCP clients avoid redundant vector generation, and operators
gain a real benchmark plus configuration guidance. It also fixes stale proof
creation on idle single-validator chains and places Federation Save/Revoke
actions at both ends of long permission catalogs.

v11.17.12 makes the independent federated-inbox restriction visible in Access
Controls and blocks the agent-share workflow before any domain mutation when
that restriction is enabled. Companion remains a least-privilege profile; the
operator explicitly decides whether connected SAGEs may discover its inbox.
The same patch constrains long Tasks cards to their responsive grid tracks so
all four desktop columns remain visible without horizontal page scrolling.

v11.17.13 separates lifecycle populations in CEREBRUM: pending local identities
are reviewed on Agents, activated principals remain in Access Controls, and
exact ordinary agents advertised by connected SAGEs appear under From
federation with their read-only Linked-reader state. The Linux native preview
gate also preloads Tauri's AppImage helpers through a bounded SHA-256-verified
cache, closing [#134](https://github.com/l33tdawg/sage/issues/134).

v11.17.15 keeps an explicitly confirmed bulk ownership transfer running across
the 50-block proposer cooldown on idle single-validator chains, exposes live
block progress, retains the job across CEREBRUM route changes, serializes later
confirmed transfers behind it, and retries the narrow idle-clock authorization race once using
the newly committed consensus time. Other governance and authorization errors
still fail closed. Companion/voice-bridge presets now default their connected-SAGE
inbox to enabled, while an existing policy block stays visible in Federation
with a deep link to the exact agent setting. It also generates a unique
name-based home domain when a writable pending-agent approval leaves that field
blank. v11.17.13 and the superseded v11.17.14 tag were not published.

v11.18.0 makes every trusted two-SAGE connection its own pairwise federation
group. An operator explicitly exports the local ordinary agent(s) participating
in that pair; every active ordinary agent on the peer may then Read those
agents' owned domain trees by default. Receiver-side exact agent/domain denies
narrow that default. Local Access Groups are not exported transitively, and a
new remote-visible agent exists only after an explicit federation export. Read
is borrowed, Copy remains the two-sided source-offer plus receiver-subscription
workflow, and remote memory Write remains reserved/denied.

The signed Read plan now commits to the source authorization model, exact
active-agent/clearance attestation, agreement and policy generations, and the
single-use challenge. Final authorization is revalidated and leased through
disclosure. Capability projection reports missing authenticated-read support
without guessing whether a peer is binding or outdated. The Docker lane proves
default Read, explicit denial, non-transitive exports, bidirectional Copy
backfill and incremental sync, and restart recovery.

Agent messaging resolves unique local or federated display/registered names to
canonical IDs before signing; collisions return bounded immutable choices.
Federated reply calls return `reply_event_id`, which the exact replier can use
for payload-free delivery status. Access Controls now has dedicated Agents,
Groups, and Federation tabs with compact search/sort lists, focused drawers,
stable URL/deep-link selection, and keyboard/ARIA boundaries. JOIN retains its
15-minute ceremony lifetime while a pasted/scanned code may spend up to five
minutes discovering Direct/relay targets as long as the pairing screen stays
open.

The release also integrates the complete stopped-node recovery tooling from
PR #161: `backup --full`, recoverable `restore --from`, and read-only `upgrade
preflight`. For the narrow historical case where a chain is still at app-v21
with absent but independently recoverable predecessor records, `upgrade lineage
status|doctor|verify` builds and verifies a chain/current-lineage-bound,
create-only app-v22 repair manifest. It is part of the exact immutable upgrade
proposal, automatic voting is disabled even on one validator, and every
validator explicitly verifies and votes. Unverified anchors require an explicit
acknowledgement. Already-upgraded app-v22–app-v26 chains are untouched; there is
no app-v27 fork in this release.

The following acceptance and follow-up boundaries remain open after the 11.17.9
code merge; they are not implied complete by Docker or CI evidence:

- [#137](https://github.com/l33tdawg/sage/issues/137): repeat the complete MBP ↔
  Mac Mini matrix on the official signed build—same-LAN Direct, forced
  relay/internet, address change, one/both-node restart, retained trust and
  grants, bidirectional renamed-agent discovery, shared-domain reads, durable
  inbox/reply/receipt history, and the updater path.
- [#135](https://github.com/l33tdawg/sage/issues/135): capture deterministic
  signed-browser evidence for Home, Tasks reload, and Access Controls.
- [#134](https://github.com/l33tdawg/sage/issues/134): close native-shell
  packaging retry/cache and Windows normal-close lifecycle evidence.
- Complete product acceptance for the signed macOS in-place updater, verify unscoped
  `sage_list`/bounded-domain projection semantics, and reproduce or close the
  historical broad authorization-scan budget report.

These are separate acceptance boundaries. A published patch, green CI, or a
Docker pass must not silently close physical-machine or signed-artifact work.

---

## v11 - shipped (the sovereign-UX + federation release)

v11 is the "zero-terminal, sovereign" release. It takes SAGE from "works if you know the CLI" to "a person clicking buttons can stand up a private, semantic, federated memory node." What landed:

### Onboarding and setup

- **First-run onboarding wizard.** Fresh nodes choose whether to start privately or join an existing SAGE network, then walk through smart search, connecting an AI tool, private-or-shared intent, and recovery protection. Closing it marks onboarding done; it is re-runnable any time from **Settings > Maintenance > Run setup**.
- **Guided semantic-memory setup.** One flow turns on the bundled embedder (Ollama + `nomic-embed-text`): detect Ollama, pull the model, re-embed existing memories as a durable background job with a progress banner that survives reloads, then switch recall over. Includes recovery-key backup and honest handling of undecryptable memories (surfaced, not silently dropped).
- **One-click managed reranker.** After a single consent click, SAGE downloads a pinned, sha256-checksum-verified llama.cpp engine build and the `bge-reranker-v2-m3` cross-encoder model, then runs and manages the sidecar process itself (loopback only, nothing leaves the machine). Recall results-per-query (k) is tunable 3-20. Bring-your-own TEI-compatible servers are still supported.
- **Connect-an-AI-tool flows.** A single dashboard flow branches three ways: same-machine one-click config writing (ChatGPT desktop Codex mode, Claude Code, Codex CLI, Cursor, Windsurf, Claude Desktop), ChatGPT Work through an OpenAI plugin + Secure MCP Tunnel, remote MCP over LAN/VPN or an operator-managed HTTPS endpoint, and LAN node-join (another computer becomes a peer node sharing this node's memory).

### Federation

- **Whole-SAGE-to-whole-SAGE join ceremony.** Guided guest and host wizards, offline-bundled, and human-verified. JOIN establishes exact chain/operator/CA/epoch trust and is revocable; in v11.9 it grants zero domains by itself. Each node separately manages a mutable Read/Copy snapshot over existing domains without reconnecting. v11.6 added first-class internet/NAT traversal and authenticated post-pair route exchange so a LAN relationship can roam.
- **Off-consensus transport.** The pinned mTLS federation listener serves live Read results as merge-in-response-only data. Copy requires both the source's current offer and the receiver's independent subscription before a locally governed copy is retained. Cross-host Write remains reserved and fails closed with `501` in v11.9 until one trusted connection can be bound to one consensus-authorized submission.
- **Consensus-layer federation primitives.** On-chain `cross_fed` exchange terms (Mode-1, tx 33/34) and the co-commit primitive (tx 31/32) landed at the app layer.

### Consensus and memory integrity

- **app-v15 verb-ladder.** Closed the ungated-deprecate hole (deprecation is now audit-only / consensus-gated) and added a grantable level-3 (modify).
- **Globally-unique `chain_id`** minted at genesis.
- **Orphaned-memory recovery** (old-key re-key) and `embedding_provider` stamped at insert so new memories no longer pose as unembedded.

### CEREBRUM (the dashboard)

- **MRI 3D brain is the CEREBRUM view** (three.js + 3d-force-graph bundled locally, so it renders fully offline).
- **Click-a-memory "train of thought"** board (Do's / Don'ts / Observations / Notes), computed from lineage, tags, content overlap, and same-lobe signals; hop card to card to walk the connectome.
- **Reading panel** collapses to the domain lobes by default (newest 30, most-recently-active first) with an expandable "how to read".
- **Live task board** with agent-vs-human authorship and atomic claim/ownership; the agent message bus merged in as a Messages tab.
- **Real search** (FTS with keyword fallback), bulk curation, status and tag filters, and corroboration counts on list + detail.
- **Settings reorganized** into focused tabs (Overview, Connection, Recall, Security, Maintenance, Updates), with verified update discovery and node restart. Linux supports verified in-place replacement. On macOS, CEREBRUM downloads and verifies the architecture-specific signed DMG, stages the replacement, hands activation to a helper outside the replaceable bundle, restarts into the new app, and rolls back on bounded readiness failure; manual DMG installation remains the explicit fallback.

---

## v11.5 - shipped (the hardening + consensus release)

v11.5 is the anti-DoS and memory-integrity release. Two workstreams landed: pipe hardening that puts real bounds on the agent-to-agent message tables, and the app-v17 consensus slate that gives deprecation teeth again with a quorum-scaled two-phase challenge, a first-class reinstate verb, disputed-but-recallable memories, and action-bound delegated agent proofs. The consensus slate ships **dormant** and activates only through the governed upgrade ladder (a 2/3 quorum vote with a 200-block floor), so existing chains replay byte-identically until operators vote it in. What landed:

### Pipe anti-DoS hardening

The agent-to-agent pipe tables now carry anti-DoS guards on every write path (REST, MCP over REST, and the dashboard operator send that hits the store directly). **Size caps** at the store chokepoint: 256 KiB payload/result, 8 KiB intent, with matching fast-fail **413** checks in the handlers (`MaxPipeContentBytes`/`MaxPipeIntentBytes`, `internal/store/store.go:513-515`). **Quotas**: 256 open pipes per verified agent identity, 10000 node-wide, an index-backed COUNT before insert, rejected as **429 with `Retry-After`** (mirrors the mempool-full recipe) and keyed on the Ed25519-verified `from_agent`, not the spoofable rate-limit header (`MaxOpenPipesPerAgent`/`MaxOpenPipesGlobal`, `store.go:518-521`). **Retention backstop:** deprecated `pipe-*` rows still force-expire after 48h and purge terminal state after 24h; v11.17.9 excludes canonical `msg-*` inbox/history rows so omitted/zero TTL is durable until handled.

### Reinstate verb + quorum-scaled deprecation

Bring back a first-class deprecation verb with teeth: deprecation gated by consensus, with the required **quorum scaled to network size** so a small-LAN node and a large federation apply proportionate bars instead of one hardcoded threshold. Complements the v11 change that made deprecation audit-only.

**Shipped in v11.5.0 as the app-v17 slate (dormant) - this item is now complete.** At challenge execution the handler counts the distinct modify-verb holders on the memory's domain (owner + ancestor owners + unexpired level-3 grantees) from committed state. A count of one or fewer keeps the byte-identical legacy one-strike deprecate, so personal nodes see zero change. A count of two or more parks the memory as `challenged` with an AppHash-folded challenge record; a second, distinct holder confirms to deprecate, and the original challenger cannot self-confirm. The new **`TxTypeMemoryReinstate`** takes a challenged memory back to `committed`, restoring the original content hash captured in the challenge record; current modify holders may use it and the original challenger can always withdraw, even after grant expiry/revocation. REST, MCP (`sage_reinstate`), Chrome, and sync/async Python SDK surfaces submit the transaction. Off-chain, challenged memories stay recallable on SQLite and Postgres, marked `disputed`, with a query-time confidence haircut. The whole slate is gated behind the app-v17 fork and ships dormant, activating only via the governed upgrade vote.

The release audit also closed the delegated-signing gap in the original candidate: an agent proof authenticated a key but was not cryptographically tied to the transaction payload a validator executed. Post-app-v17 delegated REST transactions now append the exact canonical signed request without changing any legacy encoding; consensus verifies its hash/signature, rebuilds the authorized payload for every REST transaction type that uses agent identity, checks freshness against committed block time, and atomically consumes a short-lived proof marker in AppHash state. Same-key node-originated transactions remain bound by the outer signature and monotonic nonce.

### Shared-domain replication

Federation started as read-only recall exchange: borrowed answers are shown in the moment, tagged with their source, and never written to your chain. Opt-in **domain sync** lets a shared domain be *replicated* to a peer rather than only queried, built from a **durable outbox** (writes to a shared domain are queued locally and delivered reliably across restarts and network gaps) plus an **anti-entropy digest** (periodic reconciliation so a peer that was offline catches up on what it missed) and a commit-tail watcher. Bounded by the same scope grants as recall exchange; no silent widening of what crosses the link.

**Shipped as a preview in v11.4.5 - this item is now delivered.** The durable outbox, anti-entropy digest, and commit-tail watcher landed together in the v11.4.5 preview.

### RBAC clarity + cross-scope memory transfer

Make the access model legible (who can read, write, and modify what, and why) and add a governed way to **transfer memories across scopes** (hand a memory from one agent, org, or domain to another) without losing attribution or bypassing clearance.

**Shipped across v11.3.0 and v11.4.0 - this item is now complete.** The CEREBRUM per-agent Domain Access matrix issues real on-chain access grants and revokes on Save (previously it saved a cosmetic blob the consensus checks never read), so what the matrix shows is what the chain enforces (v11.3.0). Cross-scope transfer shipped as governed domain-level reassignment: ownership of a domain moves to another agent through a governance-gated flow, from the Agents page or directly from a search selection (v11.4.0). The transfer moves ownership and read/write access - never authorship, which stays immutably attributed to whoever wrote each memory. Deliberate design choice: transfer operates on domains (tags), not individual memories, composed entirely from existing on-chain transactions rather than new consensus machinery.

---

## v11.6 - shipped (internet federation + controlled sync)

### libp2p NAT traversal + author-operated connectivity service

SAGE now uses **libp2p-based NAT traversal** with an **author-operated connectivity relay**, so two sovereign nodes behind home routers can reach each other without port-forwarding. The service brokers encrypted connectivity only; it never sees or stores memory. Operators may supply their own relay routes.

### Domain-scoped memory sync foundation

An operator can opt specific domains into synchronization across an established federation peer, on LAN or over the internet. A node might sync `eurorack` or `dmt-laser-experiments` while its `personal`, `family`, and every other unselected domain remain local and are never transmitted.

v11.6 productizes the v11.5 preview engine (durable outbox, authenticated push, anti-entropy backfill, and reconnect catch-up) instead of creating a second replication path. The choice appears after the two-node signing ceremony and is controlled by the host, **off by default**. Its concrete domain set is the bidirectional synchronization allowlist. Durable versioned policy propagation precedes data; tags replicate; crash-safe provenance prevents re-forwarding; P2P and domain-isolation tests cover the release path. Federation connectivity and memory synchronization remain separate choices.

v11.6 does **not** label a two-node synchronized pair Byzantine fault tolerant. If one side is unavailable, writes may be queued or remain local according to an explicit degraded-mode policy; the release must not imply that one surviving member constitutes quorum.

---

## v11.7 - shipped (admin override + connection & lifecycle hardening)

### CEREBRUM administrator access override

Make the personal-node authority model match the product: the genesis admin can explicitly give a locally installed agent read or read+write access to a domain even when another agent is recorded as its owner. CEREBRUM shows the original owner and target before confirmation, binds that owner into the consensus transaction, records the override as an ordinary on-chain grant/revoke, and leaves ownership and immutable memory authorship unchanged. Federated/remote targets remain consent-gated. Consensus support is isolated behind the dormant app-v18 activation boundary so existing chains replay byte-identically.

### ChatGPT desktop, Work, and Codex connection refresh

Follow OpenAI's current product surfaces instead of treating every ChatGPT connection as the same runtime. The new ChatGPT desktop app combines Chat, ChatGPT Work, and Codex. Codex mode shares the user-level `~/.codex/config.toml` MCP registration with Codex CLI and the IDE extension, so CEREBRUM provides a one-click app-wide local connection for that mode; the registration deliberately leaves identity unpinned so the MCP process derives a distinct stable signer from each active workspace folder. ChatGPT Work on the web or in the desktop app uses the hosted plugin + Secure MCP Tunnel path because ChatGPT cannot invoke a local stdio MCP server directly. Regular Chat remains supported and starts with the **Quick chat** button. SAGE uses OpenAI's name **Work** rather than the unrelated **Cowork** label.

### Coordinated restart, update, and MCP hardening

One instance per node (instance lock with owned pidfile), coordinated restart that drains MCP sessions and dashboard event streams before exec, checksum-verified updates with automatic rollback and proof-of-boot confirmation, and a hardened HTTP MCP transport: operator-only bearer principal resolution, a bounded nonce replay cache, an exact-match origin allowlist, and per-route write deadlines. Fixes the v11.6.1 reports of intermittent lost MCP connections and cannot-save-to-domain errors (boot-time key cache, transport errors mislabeled as permission denials, keep-alive reuse race).

---

## v11.8 - shipped (the collaboration release)

The synchronization-group control plane shipped in v11.8.2 and was maintained through v11.8.5. It remains an off-consensus, independent-chain sharing layer; it is not the same-chain Byzantine quorum introduced by v11.9.

### Sharing & Sync control plane

A dedicated **Sharing & Sync** section, separate from Agents and identity management, exposes synchronization groups, member nodes, roles, selective-sync state, shared domains, ownership, backfill/catch-up position, health, and recent synchronization.

Signed roster and per-domain journals govern membership, selective synchronization, backfill, controller rotation, removal, and rejoin. Retained historical copies are never silently erased from another independent chain.

This shipped multi-node sharing and full/selective synchronization without claiming self-healing BFT. That distinction remains permanent: v11.9's BFT scope is a separate same-chain model rather than an upgrade of the v11.8 journal into cross-chain consensus.

### Deferred from v11.8, shipped in v11.10: federated inbox

v11.8 shipped synchronization groups but deliberately left the agent pipeline node-local. v11.10 extends that existing `sage_pipe` → `sage_inbox`/`sage_turn` → `sage_pipe_result` loop across an active federation edge. Exact visible `agent@chain` contacts remain default-off at the receiver; the inner sending agent and outer JOIN-frozen node operator are both verified; payloads/results remain vault-backed, transient, off-consensus, and off both chains; foreign content is explicitly marked untrusted; durable retry, replay protection, result return, and terminal delivery feedback close the asynchronous loop. This is agent-to-agent infrastructure, not a CEREBRUM user-to-remote-agent messaging client.

---

## v11.9 - shipped

### Colleague-style independent-chain federation

Federation now separates **trust** from **sharing**. A successful JOIN freezes the exact remote chain, operator, CA pin, and policy epoch but installs a present empty policy, so nothing is implicitly shared. Both operators independently choose from domains that already exist on their own node and can replace that complete snapshot at any time without re-pairing. Read permits live borrowed recall; Copy additionally needs the receiving node to opt in before it saves a locally governed copy. The versioned Write field and endpoint remain for compatibility but reject use until SAGE has a consensus capability bound to the active connection generation and exact submission.

Direct and group lanes fail closed on identity drift: the live operator, CA, agreement generation, and—where applicable—the exact active group roster/domain must all agree. Agreement set/narrowing, JOIN activation, and revocation share one mutation boundary across signed REST and dashboard paths, so a completed policy change cannot race an older broader response. The CEREBRUM Federation page exposes both directional snapshots, receiver Copy choices, dynamic existing-domain selection, and usable scrolling for large domain sets.

### Domain-scoped quorum + self-healing replication

Selected domains now have hardened replicated canonical state across validator groups. A v11.9 scope is contained inside one SAGE consensus chain: its on-chain roster names existing validators, its exact domain allowlist prevents scope bleed, and its canonical Badger content mirror lets recovering nodes rebuild serving indexes. Validators independently evaluate proposed memories and commit only quorum-approved results. A surviving **greater-than-two-thirds voting-power quorum** continues accepting memories while a member is offline; ordered committed-block replay catches it up when it returns. The separate network-safe ABCI state-sync path is implemented as an explicit authorized boot role with a deterministic latest-visible state stream, isolated verification, crash-safe whole-bundle activation, and seal-before-serving. The final exact-source integrated provider-to-receiver cold run passed on source identity `7080580b15e7e5158a04e8b294ab772e51f294633be2737f904276afec4c3458`. The v11.8 SQLite/cross-chain Synchronization Group remains an off-consensus sharing overlay; v11.9 does not relabel it as BFT. SAGE's existing full local rollback snapshot contains private node material and must never be reused as a network payload.

The release evidence covers offline-write/catch-up, state recovery, validator and membership reconfiguration, revocation, conflict/degraded behavior, and chaos. The real-Comet gate begins with three validators plus one live non-validator, drives the signed app-v20 ladder, then proves governed add, bounded power update, and removal at Comet's exact H+2 effective heights. Complete-pair restarts must preserve both Comet's set and the ABCI persisted roster; the removed gateway reports inactive, cannot cast a governance vote, and cannot reappear in later commits. Its post-change power layout leaves a connected greater-than-two-thirds side for the one-validator isolation and no quorum on either side of the later 2+2 split. State-sync activation is boot-only whole-application replacement with crash journaling, exact provider-equals-validator P2P authorization, H+2 snapshot eligibility, full pristine/disk gates, latched expiry, and final-store serving order. Providers require effective `retain_blocks=0`. While unsealed, `Query` and `CheckTx` fail fast; only consensus block calls may wait during the narrow PendingComet handoff. The scoped proof composes a race-enabled, dual-principal OS-process formation/revision oracle with the real Docker firewall P2P gate; the held subprocess is not called a TCP partition. The independent integrated wire gate proved reciprocal unauthorized-peer denial, two independent RPC origins, a receiver killed after durable completion but before runtime publication and restarted in ordinary mode, a second pristine receiver with `session < seal < REST`, provider restart, exact scoped projection, and block/AppHash convergence. Internet validators still require routable Comet TCP, port forwarding, or an operator VPN; federation is not a validator tunnel, and a future tunnel layer remains separate work. The network path must never reuse the private local rollback bundle or expose it to a federation peer.

---

## v11.10 - shipped (federation that just works)

v11.10 closes the independent-chain federation product loop before native-shell
work begins. It fixes listener-derived connection codes, freezes both scanned
endpoints before either agreement transaction, makes final confirmation
replayable after a lost response, and keeps JOIN trust-only with zero implicit
domain sharing. CEREBRUM presents the two-way scan ceremony, directional
Read/Copy choices, pause/resume, revocation history, remote notification, and
visible default-off agent contacts as straightforward administrative actions.

The existing agent pipeline now resolves exact visible remote agents and carries
signed work/results over direct mTLS or the persisted roaming route. Delivery is
durable and idempotent across disconnect/restart; foreign payloads are explicitly
untrusted; terminal send/result failures return actionable one-shot feedback to
the local signing agent. This remains off-consensus and is not remote memory
Write. The release gate covered the two-node browser ceremony, direct/offline
faults, security/race review, SDK contracts, packaging, and immutable-release
checks.

---

## v11.11 – v11.15 - shipped foundation (v12 acceptance remains open)

v11.10 completes the federation and federated-agent backend/administrative experience. v12 is the **product**: a standalone native application in which every CEREBRUM function is mapped to a real app experience, with the web dashboard kept only as a still-supported fallback. The releases between them de-risk and stage that transition so v12 is an integration-and-polish capstone, not a from-scratch rewrite. Every step keeps the hard constraints — no chain reset, upgrade-in-place, the SAGE daemon and authenticated local APIs cleanly separated from any shell, and no weakening of the local trust boundary. Order and grouping are indicative; nothing here is dated.

### v11.11 - the native shell foundation

Choose the desktop-shell technology through the deliberate evaluation v12 requires, then ship the first additive, opt-in native application that embeds CEREBRUM in its own window. The browser CEREBRUM remains fully supported as the fallback. v11.11 is an architectural release, not a cosmetic wrapper, and must complete these deliverables:

- **Desktop-shell decision record.** Compare the credible macOS, Windows, and Linux options with a weighted scorecard covering threat surface and sandboxing, signed packaging/notarization, accessibility, performance and memory cost, offline operation, cross-platform maintenance, update behavior, and long-term ownership. Build small proof-of-concept spikes for the finalists, document the threat model, and record the chosen shell plus rejected alternatives before product code commits to it.
- **App–daemon trust and compatibility contract.** Specify authenticated bootstrap, credential/key storage, IPC versus loopback transport, origin/navigation restrictions, process and port ownership, startup readiness, crash detection and recovery, graceful drain/shutdown, version negotiation, and rollback-compatible independent updates. The shell receives no implicit validator/admin authority and cannot weaken the existing local API boundary.
- **Single-instance lifecycle and navigation ownership.** The native app owns its window, deep links, route restoration, external-link handoff, foreground/background behavior, and daemon supervision. Reopening SAGE focuses the existing native window; it does not depend on browser tab inspection or browser-specific automation. Browser CEREBRUM remains an explicit fallback, not an accidental second primary UI.
- **Measurable foundation gate.** Set budgets for cold/warm launch, idle CPU, memory, large-store interaction latency, and animation frame pacing on supported hardware. Use a monotonic animation clock and compositor-friendly transforms for continuous motion instead of coarse React timer rerenders; establish keyboard navigation, focus visibility, screen-reader naming, and reduced-motion architecture now even though v11.14 performs the full hardening pass. Automated packaging and smoke tests must cover clean install, daemon unavailable/recovery, app restart, deep-link routing, offline launch, and shell/daemon version skew on macOS. Existing cross-platform preview smoke may remain as regression evidence.

No chain reset. The SAGE daemon and authenticated local APIs remain independently testable and operable underneath the shell.

The accepted framework decision, protocol boundary, and blocking release gates
are recorded in [`desktop-shell-decision.md`](desktop-shell-decision.md),
[`native-app-daemon-contract.md`](native-app-daemon-contract.md), and
[`native-shell-quality-gates.md`](native-shell-quality-gates.md). The tracked
Tauri foundation remains an opt-in preview until the macOS matrix passes.

The v12 meaning of **fully native** is defined by the superseding
[`desktop-shell-v12-adr.md`](desktop-shell-v12-adr.md): bounded WebView domain
controls are allowed, while lifecycle, platform integration, recovery,
permissions, updates, and rollback remain app-owned. A WebView route load is
not native-control parity evidence.

**The native shell is alpha and does not gate releases.** Browser CEREBRUM is
the product; the shell is a background track through v11.11–v11.15. It is built
and runtime-tested in CI, never staged as a public release asset, and not
intended for end-user use. Releases continue shipping bug fixes and capabilities
on their normal cadence — federation, agent-to-agent messaging, and the rest of
the roadmap do not queue behind desktop packaging. The signing, notarization,
update/rollback, recovery, performance, and accessibility bar applies at **first
distribution of the shell**, which is v12.

**Platform scope: macOS is the sole v12 native-product target.** The existing
Windows and Linux preview builds are foundation experiments, not release
commitments. Browser CEREBRUM is the supported product surface on Windows and
Linux. Linux native investigation may continue as optional R&D, but it cannot
gate macOS and must not ship while the selected Tauri/Wry stack resolves the
GTK3 `glib` line affected by `RUSTSEC-2024-0429`. SAGE will not waive that
advisory or turn an unreviewed WebView fork into production infrastructure.

### v11.12 - consumer onboarding and recovery

Make first run and recovery survivable by someone who has never used SAGE,
natively and without a terminal. v11.12 has four acceptance-owned tracks:

- **One coherent first run.** Choose **Start my own SAGE** or **Join an existing
  SAGE network**, set up semantic memory, and connect an AI tool. Standalone
  setup is private by default, and the copy must distinguish a same-chain node
  join from connecting separate SAGEs for file-sharing-style access.
- **Private or shared, in plain language.** Let the owner choose people and
  existing domains without exposing protocol identities. Trust, direct sharing,
  and sharing-group membership remain separate; nothing is shared by default.
- **Recovery that is actually proven.** Make recovery-key backup status and the
  safe storage step visible, guide backup and restore, and prove both a forgotten
  passphrase and a fresh-machine recovery without terminal commands.
- **Honest dialogs and a real usability gate.** Every destructive or
  privacy-affecting action says what changes, what remains safe, and what to do
  next. Clean-machine onboarding and recovery tests with real nontechnical users
  are release criteria, with keyboard, focus, and screen-reader basics exercised
  in the same flows.

The implemented browser slice now connects the whole decision path: the existing
authenticated node-join ceremony is reachable directly from onboarding; a new
standalone SAGE explicitly defaults to private; sharing routes into the existing
federation and RBAC surface instead of inventing a second permission system; and
a lived-in node is told to export a backup before replacing its network history.
Onboarding also reuses the real encryption setup, persists an explicit recovery-
key backup acknowledgement, and keeps warning until that acknowledgement exists.
The portable plaintext memory backup is now labeled honestly, protected by the
shared privacy dialog, and covered by an automated export → empty-node Preview →
Confirm → content/hash verification. Single-memory forgetting, manual ledger cleanup,
restart, and semantic-to-basic recall changes now use the shared explanatory dialog
instead of click-twice controls. Every onboarding child now replaces, rather than
stacks over, its parent dialog and shares keyboard focus/trap/restore behavior. A
structured proxy exercise has now passed the federation, same-network join,
forgotten-passphrase, keyboard/focus, and core onboarding paths; its evidence and
limits are recorded in
[`v11.12-clean-machine-acceptance.md`](v11.12-clean-machine-acceptance.md). Complete
visible-only restore and cleanup runs now pass on the signed installed-copy build.
For v11.12, the release owner accepts the structured proxy as the available usability
evidence: independent nontechnical-user study remains a v12 criterion, the full
spoken VoiceOver matrix remains in v11.14 accessibility hardening, and native-shell
mapping remains part of the bridge to v12 rather than a browser-CEREBRUM blocker.
The memory-only portable backup scope is also intentional for this release;
federation, RBAC, credentials, settings, and chain history remain explicitly
excluded, with any broader portable-node backup deferred to a separately designed
release. The remaining RC artifact gate is a notarized and stapled v11.12
clean-machine Finder/Launchpad open. Browser CEREBRUM remains supported throughout;
the tracked native shell remains an internal alpha until v12 distribution.

### v11.13 - native lifecycle: install, updates, health, permissions

Move the operational surface a desktop product needs out of terminal-and-dashboard-only paths and into the app: installation and OS-level permission prompts (login item, notifications, and any capability the product uses) with plain-language rationale; checksum-verified background updates with automatic rollback and proof-of-boot; continuous node-health monitoring with one-click guided recovery; and unobtrusive background/tray operation. Reuses the v11.7 coordinated-restart, verified-update, and instance-lock machinery rather than reinventing it.

### v11.14 - accessibility, performance, and offline hardening

Meet the accessibility bar v12 treats as a release criterion: full keyboard navigation, screen-reader labeling, sufficient contrast, and reduced-motion support across CEREBRUM and the native shell. Hold the v11.11 performance budgets for the embedded experience on large memory stores and the 3D connectome view. Verify fully offline operation end to end — the bundled embedder and reranker, onboarding, recall, and recovery all work with no network — so a sovereign node is genuinely sovereign.

### v11.15 - governed local collaboration and safe federation

App-v23 makes local access control understandable without weakening its
consensus boundary. Member, Manager, and Admin roles define verbs; Access
Groups define which local agents share scope; clearance caps readable
classification; and named security profiles supply hard restrictions that
override every role or grant. Approval binds enrollment, role, profile,
clearance, and a non-shared home domain in one commit-confirmed operation.

CEREBRUM Root is separated from the agent roster and retains one immutable
authority principal across credential handovers. The current credential may
operate every Root-owned domain immediately, while prior memories keep their
original author and retired credentials remain permanently ineligible. Across
independent SAGEs, exact federated agents can be linked as live readers but
never mixed into local membership or given mutation authority. Fresh
first-party companion installations are ready to write their first memory
without a manual repair; generic new keys remain safely pending review.

---

## v11.17 - local agent Messages and read receipts

v11.17 consolidates pipe, inbox, sent results, and status into one agent-only
**Messages** service model with idempotent send, explicit idempotent receive,
idempotent receiver-local reply, explicit signed
`PUT /v1/messages/{receiver_local_message_id}/read`, and exact sender-only
status—the five canonical operations. Claiming inbox work is not disguised as
a passive `GET` list or combined with sent/`all`; passive history continues
through the compatibility surface until its cursor and claim semantics are
safe. The old MCP/REST names remain
compatibility wrappers through v12 and at least two feature releases, without
duplicating rows into a second default `sage_turn` field. Add
payload-free, sender-only status for one exact sent message:
durable destination delivery plus an automatic exact-ID acknowledgement signed
by the addressed recipient when canonical receive, `sage_inbox`, or
`sage_turn.pipe_inbox` returns the item.
Same-node read evidence and metadata-only HTTP MCP wake-up hints ship in
v11.17. Federated receipt propagation ships as a separately negotiated v2
protocol that preserves both principals—the outer JOIN-frozen SAGE operator
and inner recipient agent—and are negotiated as an additive capability with
durable retry, replay/equivocation protection, and fail-closed
pause/revoke/re-pair behavior. “Read” means the authenticated recipient client
fetched and acknowledged that exact message; it is not presence, comprehension,
action, or a reply. Status is unavailable to Root/Admin/operators and unrelated
agents, contains no payload or roster data, remains transient/off-consensus, and
uses a dedicated metadata-only SQL projection that remains queryable while the
content vault is locked. The local contract requires no application fork. The
complete contract and mandatory two-node fault/security gates for a future
federated extension are in
[`design/agent-message-receipts.md`](design/agent-message-receipts.md).

---

## v12 - product roadmap capstone

v12 is the planned completion milestone for the SAGE product roadmap: the fully integrated product rather than another backend-only release. It ships as a standalone desktop application on **macOS** in which every CEREBRUM dashboard function is mapped to a real app experience — the same capabilities, but presented as a proper application rather than a set of web pages. Browser CEREBRUM remains the supported product on Windows and Linux and remains available on macOS for recovery and preference. By v12 the v11.11–v11.15 bridge has already chosen the desktop shell, proven consumer onboarding and recovery, moved lifecycle into the app, established accessibility/performance/offline gates, and made local/federated collaboration governable, so v12 is the integration-and-polish capstone that ties it into one coherent product. The macOS app owns installation, node lifecycle, onboarding, permissions, updates, health/recovery, federation, and Sharing & Sync as one coherent native-feeling experience, while the SAGE daemon and authenticated local APIs remain cleanly separated underneath.

**Consumer usability is a release criterion, not polish.** A nontechnical person must be able to install SAGE, create or join a node, connect an AI tool, choose what is private or shared, recover from ordinary failures, and keep the app updated without opening a terminal or learning SAGE internals. Every choice uses plain language and safe defaults; destructive or privacy-affecting actions use consistent accessible SAGE dialogs; errors explain what happened, what remains safe, and the next recovery action. The v12 release gate includes clean-machine onboarding and recovery usability tests with people who have not used SAGE before.

Tauri 2 is the accepted macOS foundation, subject to the security, packaging,
accessibility, performance, and offline-operation gates in the desktop-shell
ADR. Linux and Windows native clients are outside the v12 release scope; their
supported product path is browser CEREBRUM. Current progress
and the remaining release blockers are tracked in
[`v12-native-product-status.md`](v12-native-product-status.md). The route/action
inventory, fail-closed evidence contract, and optional Linux R&D record live in
[`v12-native-capability-ledger.md`](v12-native-capability-ledger.md),
[`v12-native-acceptance-ledger.md`](v12-native-acceptance-ledger.md), and
[`design/v12-linux-native-path.md`](design/v12-linux-native-path.md).

---

## Current Foundation

The v11 line carries the upgrade substrate, domain-reassign recovery, ancestor grants, PoE-weighted quorum, verdict-correctness scoring, corroboration, and domain-aware validator weighting. Those are baseline capabilities now, not future roadmap work. Release-by-release history lives in the README changelog.
