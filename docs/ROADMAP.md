# SAGE Roadmap

**Status (2026-10):** **v11.23.18 is the current release.** SQLite driver 1.60.1 improves WAL read-fault handling and argument binding. The reranker retains its existing 1,000-result bound in an order CodeQL can verify directly. Optional benchmark environments use patched datasets 5.0.1, and browser fixtures use Playwright 1.63.0. App-v28 and the experimental, default-off v16 local judge remain unchanged, including its published qualification limits. Previously, v11.23.17 shipped: The optional, experimental, default-off local judge is pinned to v16. The attributed tank-warning example now holds for review on the disclosed frozen 40-item Linux/arm64 regression run; all 18 genuine examples avoid rejection and all 22 negatives reject. Thresholds, serving template, Ollama runtime and app-v28 are unchanged. An explicit v15 configuration is not rewritten on upgrade. Previously, v11.23.16 shipped: MCP task creation defaults to 0.90 through both tools, explicit remember scores are validated and preserved, and submission receipts report the signed confidence. Correction inheritance and schema defaults agree; existing memories and task idempotency remain intact. App-v28 and the qualified v15 local judge pin remain unchanged. Previously, v11.23.15 shipped: MCP bridges exit after their client dies, correction lineage resolves by content hash, malformed reranking preserves recall results, and federation failures carry actionable diagnostics. App-v28 and the qualified v15 local judge pin remain unchanged. Previously, v11.23.14 shipped: MCP stdio remains responsive during slow tools and supports request cancellation; AMID preserves unresolved signer fences across restarts, and block sync rounds quorum-blocking power correctly. An experimental, default-off managed local judge restores evidence checks with loopback containment and unavailable-verdict holds. Previously, v11.23.13 shipped: REST acknowledgment deadlines and vault recovery are repaired; unreleased evidence checks and the managed local judge are deferred. Previously, v11.23.11 shipped: An optional, default-off memory-quality gate adds background judging and an operator review queue for uncertain memories. A typed committed-memory vote refusal can resolve a signer fence when the exact signed vote cannot succeed again. CLI help exits before command side effects, and inbound federated messages wake their exact local recipient. No consensus execution change or chain migration. Previously, v11.23.10 shipped: A held signing key can no longer park a node's whole write path: a fence that waits for proof of an earlier submission's fate is now bounded by the primitive itself, so the holder returns a retryable error instead of sitting on the single nonce lease forever and queueing every later writer - including the automatic reconciler that would have lifted the fence - behind it. Previously, v11.23.9 shipped: CEREBRUM now shows a held signing key while it waits for proof, and keeps the last resolution visible once the hold ends - the visibility the fence workstream was missing, with the nonce crossing the status payload as a string so the dashboard cannot round it. Previously, v11.23.8 shipped: A node stops manufacturing signer fences when it is shut down: an ordinary exit now quiesces signing, gives in-flight submissions a bounded window to finish before any listener is force-closed, and never resumes signing - so a broadcast is no longer severed into an indeterminate outcome that costs its payload at the next start. The credential-free operator exit also reaches the nodes it was built for: `sage-gui fence abandon` gains `--peer-redelivery-acknowledged`, the acknowledgement the shared evidence gate requires whenever peers are connected, and `fence list` names the requirement and the restart step. Previously, v11.23.7 shipped: An app-v28 chain can install its own updates again: the pre-upgrade recovery proof now includes the composite AppHash rule app-v28 commits - the app-v13 digest of the state without the public-memory index nodes, composed with the sparse index root - read through the shared store implementation rather than a third copy of the rule, so a signed app update stops failing with AppHash mismatch under every hash rule and the snapshot gate opens. Previously, v11.23.6 shipped: A restored fence on a healthy but IDLE chain now settles itself. Since app-v12 every node sets create_empty_blocks=false, so a block mints exactly when a signed transaction enters the mempool; a fence restored from durable intent refuses to sign and its bytes did not survive, so a chain that was already quiet when the fence was raised cannot ever produce the proof the fence waits for - no transaction, no block, no committed hash, no advanced nonce. The automatic route now settles that shape when the node is caught up, has no peer and has seen none under this fence, holds no mempool copy, sees no committed fate, holds an unspent allocation, and the tip predates this fence - recorded as fence_abandoned with mode=automatic_quiescent, allocation reserved, and the same stated residual as the existing automatic decision. A tip that has minted since the fence, any peer, a peer seen under the fence, a mempool copy, a catching-up node, an unreadable tip, a live fence and a record without a nonce all still hold it. Previously, v11.23.4 shipped: The fence fixes reached the nodes that keep a peer: the peer observation moved from the process to the fence, the operator abandon route accepted connected peers against an explicit acknowledgement, the health surface gained a per-fence resolution class (reconciling versus proof_or_operator) and the automatic route's refusal reason, and the held-fence alarm stopped claiming a re-submission that a restored fence cannot make. Previously, v11.23.3 shipped: v11.23.3 could resolve a fence restored from durable intent only when the node had never seen a peer since process start, and its operator abandon route refused outright whenever any peer was connected - so a federated desktop node, or a validator with a persistent peer, could sit behind a fence that no proof could settle with both recovery routes closed, still refusing every write. The peer observation is anchored to the fence now rather than to the process, so a sighting from an earlier outage can no longer disable self-healing for every fence raised afterwards; the automatic route still refuses while a peer is connected or was seen under the fence being held; and the operator abandon route accepts connected peers against a second explicit acknowledgement, recording the peer count and the accepted route with the decision. The health surface also says HOW each held fence can end - reconciling, meaning the node still holds the exact bytes and is re-submitting them, versus proof_or_operator, meaning the bytes did not survive and only a chain-read proof or an operator abandon will lift it - and the automatic route's refusal reason is recorded with the fence instead of being computed and dropped, so a node whose self-heal is correctly refusing on evidence no longer reads like one whose self-heal is broken. An agent can read all of it directly: the MCP surface gains the read-only `sage_node_health` tool, which forwards the node's signer-fence block and renders the guidance from that resolution class. No consensus change, no transaction-type change, no upgrade height, and no chain reset. Previously, v11.23.3 shipped: A held signing key can no longer strand a node, and the release that fixes it can actually be installed. Three things were wrong around a fence restored after a restart. Nothing re-read its proofs, so a transaction the chain had already settled kept its key closed and its node refusing every coordinated restart - which is every in-app update - until an operator supplied a proof that did not exist. The restart veto asked whether ANY fence was held rather than whether its durable record survived, so a fenced node could not take the restart carrying the fix. And a write against a fenced key sat on the lease until the caller deadline, so agents saw "writes timed out, no error" while reads stayed healthy. Now: a restored fence re-reads the chain on the live reconciler schedule and lifts on a committed hash, or on a committed nonce that has reached the fenced allocation (labelled spent when it is equal, because the floor alone cannot say whether those bytes committed or were overtaken); the one shape no proof can settle is resolved at first boot when the evidence is unambiguous - the node is caught up, has seen no peer this run, holds no mempool copy, sees no committed fate, and the allocation is unspent - recorded as a fence_abandoned event with mode=automatic_unprovable and its allocation reserved; the veto refuses only when a fence durable record cannot be confirmed, failing closed; and request-serving write paths refuse immediately with 503 + Retry-After naming the held transaction instead of hanging to the caller timeout. That last point is why the recovery is a drag-and-drop: a user on v11.23.2 whose node is fenced cannot install this release from inside the app, so replacing the bundle and relaunching is the documented path, and the first boot resolves the fence without a terminal. No consensus change, no transaction-type change, no upgrade height, and no chain reset. Previously, v11.23.2 shipped: The app-v28 fork can now open on a chain whose public corpus predates it. The public-memory stage is built outside the consensus transaction, and it refused to produce a leaf for any PUBLIC=0 record it could not canonically encode - a real chain carried 53 legacy records whose canonical content hash had been erased by a historical lifecycle transition, so the activation block aborted on every replay and the node could not start, with upgrade cancel unavailable because it is itself a consensus transaction. Those records are now quarantined out of the committed public set, exactly as the co-commit tombstone index already treats a record whose decoded hash is not 32 bytes, and a record joins the set in the block that re-anchors its hash; a missing hash on a record born under app-v25 or later still refuses the build. Narrow by design, with no change to what the fork commits: no transaction-type change, no upgrade height, and no chain reset. Previously, v11.23.1 shipped: An amid-only validator fleet can now set an agent enrollment clearance: `amid` mounts exactly the dashboard pair (`GET /v1/dashboard/network/access`, `PUT /v1/dashboard/network/access/agents/{id}/policy`) with the same handlers and operator gate CEREBRUM serves, so the emitted transaction is identical, and the operator signs as the current Root from the node host. This is the record the memory-write gate reads - a submission classification is compared against the agent enrollment clearance, and membership clearance never satisfies it. Patch release inside the 11.23 line: no shell range move, no transaction-type change, no consensus change, and no chain reset. Previously, v11.23.0 shipped: App-v28 activates: `maxSupportedAppVersion` moves 27 -> 28 and converges with the compiled ceiling, so a personal node advances its own chain across the seam on upgrade with no governance ceremony - the automatic path every earlier rung took - and the gate v11.22.0 compiled (the sparse public-memory Merkle commitment with its composite AppHash rule, and the consensus-side co-commit tombstone rule behind a content-hash reverse index backfilled at activation) is now what the chain runs. The contract evidence landed with the bump: the four-validator determinism ladder crosses app-v2 -> app-v28 with byte-identical AppHash at H-1/H/H+1 of every seam, both Consensus Fault Gates pass, the real-process state-sync gate drives its provider through app-v28 so a pristine receiver restores and both sides report exact app-v28 state with converging AppHash, and the gate kill phases plus the replay family pin restart and replay equivalence across H. The item that failed when v11.22.1 was cut - a v28 provider dying on its state-sync serving boot because the verification family recomputed the pre-v28 AppHash rule - is fixed here. The native shell rides the minor bump: its SSCP range admits v11.23 daemons, so fixes after this one ship as patches. No chain reset, no transaction-type change, and historical blocks keep replaying under their original app versions. Previously, v11.22.1 shipped: The CEREBRUM Federation page leads with trusted connections and saves per-agent visibility from a Visible/Not-visible switch that carries the revision it read, the App version panel renders on nodes without governance scopes, and the Python SDK gains `set_agent_access_policy`/`get_access_state` against the app-v23 enrollment routes. App-v28 stays compiled and dormant: the determinism half of its evidence is in hand (the four-validator ladder crosses every seam to app-v28 with byte-identical AppHash, and the upgrade gate drives the whole ladder with auto-votes), but the contract last item - a promoted node surviving state-sync and restore - fails today in the real-process state-sync gate, where a restarted v28 node comes back, serves, and then stops answering. The 27 -> 28 ceiling bump ships in the release that turns that green. Previously, v11.22.0 shipped: App-v28 shipped compiled and dormant: the public-memory Merkle commitment and the consensus-side co-commit tombstone rule exist behind one gate, `maxSupportedAppVersion` stays 27, every node auto-voter abstains on v28, and a personal node cannot advance itself into the fork - activation waits for byte-identical AppHash across the seam on a four-validator devnet, both Consensus Fault Gates, state-sync/restore of a promoted node, and replay equivalence, and ships in the release that carries that evidence. The tombstone rule is enforced by consensus now, through a content-hash reverse index maintained by every memory write and backfilled at activation, because a co-commit never consults the voter and the REST boundary was the only place that checked. The binary also stopped reporting one ceiling for two questions: what it can execute (app-v28, compiled) and what it will auto-vote (app-v27) are separate facts in `upgrade status`/`upgrade preflight`, the version banner, the full-backup stamp and the state-sync authorization ceiling. Upgrades finally have a deliberate path as well - `sage-gui upgrade vote` plus CEREBRUM App version panel - which is what a dormant gate needs, since nothing auto-votes on it. No chain reset, no transaction-type change, and historical blocks keep replaying under their original app versions; app-v27 remains the ceiling. Previously, v11.21.0 shipped: The 11.21 line opens with the shell that admits it: the SSCP compatibility range lives in the shipped native shell rather than the daemon, so a minor bump is the release that widens it, and the shell now accepts v11.10 through v11.21 daemons - which is what lets the fixes after this one ship as patch releases instead of a rebuilt shell each time. The daemon is otherwise the v11.20.5 build, so there is no node behavior change to re-verify. No consensus change and no chain migration; app-v27 remains the ceiling. Previously, v11.20.5 shipped: A peer whose address moved repairs its own route instead of waiting for a re-pair: the stale-snapshot exemption that already let a p2p-only agreement run the authenticated route exchange now covers a concrete-endpoint agreement too, so a host that moved is re-learned rather than dialled forever at an address nobody answers, and a moved host says so with a trust-generation verdict instead of reading as offline. Delivery uses the windows a flapping peer leaves open: a successful delivery makes the rest of that peer backlog due at once instead of leaving it asleep in backoff, and a drain pass covers sixteen rows rather than four. Recall stops hiding results behind the confidence floor in silence: it reports the floor and how many candidates it removed, and the settings surface warns which write tiers a floor hides. No consensus change and no chain migration; app-v27 remains the ceiling. Previously, v11.20.4 shipped: A signer fence survives the process that raised it: every submission is shadowed by a durable record written before the bytes reach the transport, retired only on a proven fate and re-raised as a fence at startup, so a node killed mid-submission refuses to sign that key instead of re-seeding its allocator past the abandoned nonce; and a nonce that is provably dead is released by an operator proof read by the node rather than asserted by a caller, either the exact transaction in a committed block or supersession by a higher committed nonce. No consensus change and no chain migration; app-v27 remains the ceiling. Previously, v11.20.3 shipped: A refusal of a federated body as too large stays retryable instead of failing the message permanently: the per-route body cap of a peer moves when it upgrades, and the listener answers `413` only for a body that genuinely exceeded that cap rather than for one it could not read, which the sender would otherwise have taken as a permanent verdict on the bytes; every failed delivery attempt is now logged with its verdict and retry delay. No consensus change or chain migration; app-v27 remains the ceiling. Previously, v11.20.2 shipped: A consensus submission whose outcome the node could not observe is reported as an indeterminate 202 carrying the exact transaction hash, the allocated nonce and retryable:false, instead of the opaque 500 that taught callers to re-sign a write that may already have committed; definitive verdicts keep their own statuses, and generated testnets set timeout_broadcast_tx_commit explicitly below SAGE client wait so the node always answers. No consensus change or chain migration; app-v27 remains the ceiling. Previously, v11.20.1 shipped: Federated replies stay deliverable for seven days instead of one, the retention migration that re-stamped reply rows is repaired, and a refused reply proof records its exact reason on both sides. An agent working state can now live encrypted on the node — private media (`GET`/`PUT /v1/private-media/{uuid}`) stores immutable JPEG originals with ciphertext-only rows, actor isolation and quota enforcement, and the workflow journal (`GET`/`PUT /v1/workflows[/{uuid}]`) is an encrypted, actor-bound place for long-running work with compare-and-swap revisions and an opt-out conversation guard; both are vault-required and reachable only from inside the app-v23 pipeline agent boundary. `GET /v1/messages/storage` reports the node honest message-storage posture, and `POST /v1/messages` takes a strict signed `require_encrypted_storage` boolean that fails closed with a 503 instead of silently storing plaintext. Local storage is no longer taken on faith — SQLite opens with `synchronous=FULL` and verifies its journal/synchronous mode at boot, and vault publication is atomic, so vault attached but not yet required is unobservable. `sage-gui init-lantern-private` adds fresh-only Lantern bring-up behind a private-listener policy that a missing or edited config cannot weaken. A public-memory Merkle index, migration builder and stage/promote path ship dormant with no production caller: staged rows stay out of the AppHash and promotion — what would write AppHash-covered state, named for app-v28 — is unreachable from a running node, so no fork is opened. No consensus change or chain migration; app-v27 remains the ceiling. Previously, v11.19.22 shipped: The Go build floor moves to patched Go 1.26.8 and the Go dependency group is refreshed — pgx v5.11.0, x/crypto v0.57.0, x/sync v0.23.0, x/sys v0.48.0, x/tools v0.50.0, klauspost/compress v1.20.0 and modernc.org/sqlite v1.58.0 (SQLite 3.53.4) — with the shipped binaries unchanged in behaviour and source builds now requiring Go 1.26.8+. A co-commit can no longer re-commit bytes the quorum already rejected — the REST submit boundary refuses a tombstoned content hash before it broadcasts — and the MCP client reports the node's own dedup verdict instead of scoring word overlap against a window of memories, so near-duplicates are stored and exact duplicates are skipped with the node's reason. Voter dedup is sticky — rejected, challenged, or forgotten content cannot be re-admitted under a fresh memory id while genuine corrections still pass — and the dedup lookup is indexed on SQLite and Postgres. The gRPC-Go dependency is patched to v1.83.2 for CVE-2026-84445. CEREBRUM adds a federation connectome, live metadata activity, clearer pairing steps, and explicit memory-sharing drafts. Trusted paired nodes now discover and
message eligible ordinary agents automatically, with memory sharing separately
configured. It keeps safe registered-name addressing and
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
v11.19.1 makes other-session claimed work recoverable beyond the newest 100
generic history rows through a payload-free, cursor-paginated handoff
projection, aligns its exact count with TTL expiry, and makes provider-addressed
compatibility work session-fenced, recoverable, and idempotently replyable.
v11.19.2 makes binary-replacement safety observable from canonical consensus
state: live `upgrade status` and stopped-node `upgrade preflight` report the
exact pending plan and active ballot, decode upgrade targets, and fail closed
on malformed, oversized, missing, or inconsistent state.
v11.19.3 moves that compatibility decision into the normal updater: supported
in-flight upgrade state is carried through the verified recovery snapshot and
continues after restart without a prompt or CLI step, while malformed or
unsupported targets still stop before executable mutation.
v11.19.4 closes the v11.19.3 validation-to-fence race: it interrogates the
replacement binary for its actual app-version ceiling and validates governance,
height, and AppHash under one uninterrupted runtime fence.
v11.19.5 repairs exact-local compatibility receipts, extends durable claimant
identity across stdio, Streamable HTTP, and SSE, and revision-fences every
explicit claim handoff. Its separate payload-free inbox activity sequence lets
host hooks notice fresh task assignments and replies without changing message
wake or Stop semantics.
v11.19.6 exposes typed memory relationships through one bounded REST projection
and the `sage_get_links` MCP tool. It filters both endpoints through the
caller's domain and record disclosure policy before querying the graph, and
surfaces unavailable authorization state instead of returning a false complete
empty graph. Personal-node upgrades remain automatic and updater-owned; manual
stopped-node preflight is scoped to quorum or externally managed governance.
v11.19.7 adds default-off, consent-gated recall-backed compaction with
byte-exact governed capture and complete same-thread restoration; cursor
progress is commit-backed and uncaptured spans remain visible. CEREBRUM can
isolate typed reasoning links from structural graph edges, re-linking a memory
pair correctly updates its type, and the dependency/action baselines are
refreshed. These changes are off-consensus and app-v27 remains the ceiling.
v11.19.8 restores bounded discovery of transferred historical domains for
active local Access Groups. v11.19.9 fails closed when an unpinned Codex MCP
session resolves to the filesystem root. v11.19.10 repairs returning-agent
review: exact retired home domains can be reclaimed from Root during approval,
and deprecated audit history no longer masquerades as active memory ownership
during rejection. v11.19.11 adds an exact operator-configured CEREBRUM hostname
allowlist for loopback TLS reverse proxies and rejects ambiguous or malformed
forwarded-proto chains. These patches remain off-consensus and app-v27 stays
the ceiling. v11.19.12 refuses project-scoped MCP and Codex installs from the
user's home directory, preventing project-relative hook registrations from
polluting global host configuration, and refreshes the fail-closed checksum for
the verified September AppImage helper rebuild.
v11.19.13 prevents stdio MCP startup from self-healing project-relative Claude
hooks into user-global configuration when its working directory is the user's
home directory. Explicit installs retain their existing refusal, while normal
project self-healing—including with `CLAUDE_CONFIG_DIR` set—remains unchanged.
v11.18.19 prevents Codex project-hook
self-healing from ever targeting the user-global `~/.codex` scope and removes
the Connectome's competing DOM/ForceGraph click paths while bounding raw access
metadata and making bloomed memories responsive. The supported consensus
ceiling is app-v27.

**Hard constraint driving the whole plan:** no chain reset. Existing chains must
upgrade in place across all future releases. Routine personal-node upgrades
remain automatic; the exceptional legacy-lineage repair is deliberately an
explicit, reviewed operator ceremony rather than a silent mutation.

## v11.23.18 release

The SQLite driver moves to 1.60.1 with its matching libc dependency, improving WAL shared-memory read-fault handling and argument binding while retaining SQLite 3.53.4. Go tooling dependencies are refreshed.

The reranker's existing 1,000-result bound is expressed in an order CodeQL can verify directly. Regression tests cover integer extremes, empty and oversized candidate pools, upstream failures and malformed scores. Optional LoCoMo and LongMemEval environments pin datasets 5.0.1 for GHSA-379c-qx7v-6h59; browser fixtures use Playwright 1.63.0.

No consensus execution change, app-version change or chain migration; app-v28 stays active. The experimental, default-off v16 local judge, its runtime, thresholds and published qualification limits are unchanged.

## v11.23.17 release

The optional managed local judge advances to v16, using an immutable Hugging Face download URL and separate verified SHA-256 pins for the download and Ollama's registered GGUF blob. Select `SAGE_LOCAL_JUDGE_MODEL=sage-memory-judge:v16` to use it; the experimental gate stays off by default, and upgrading does not rewrite an explicit v15 setting.

In the independently measured frozen 40-item synthetic regression run on Linux/arm64 with Ollama v0.31.1, the attributed tank-warning example moves from reject to hold. Genuine examples total 16 pass, two hold and zero reject; all 22 negative examples reject. This evidence is limited to the disclosed set and runtime/platform, not fresh-holdout accuracy or production installation across platforms.

Thresholds, serving template and runtime are unchanged. There is no consensus execution change, app-version change or chain migration; app-v28 stays active.

## v11.23.16 release

`sage_remember` defaults task confidence to 0.90, matching `sage_task`; other memory types keep their 0.80 default. Corrections inherit their source type before choosing the default. Explicit finite scores from 0 through 1 are preserved, including 0 and 0.80; invalid inputs are rejected. The schema documents the conditional type and confidence defaults without materializing values that would override inheritance.

Submission receipts expose `submitted_confidence` for actual remember submissions, including corrections and indeterminate outcomes, and for task creation, replay and committed-but-unconfirmed responses. The field describes the signed request rather than stored or query-time confidence; remember pre-validation skips or rejections and task-status updates omit it.

Existing records retain their scores, and canonical task idempotency checks remain intact. This is an MCP default and receipt correction, with no consensus execution change, app-version change or chain migration. App-v28 stays active and the qualified v15 local judge pin is unchanged.

## v11.23.15 release

MCP stdio bridges monitor their original parent process and cancel tools and subscriptions on confirmed parent exit. Cleanup has a bounded process-exit fallback for inherited pipes and blocked input or output; idle clients are not disconnected.

CEREBRUM resolves correction lineage by content hash through bounded metadata lookups on SQLite and PostgreSQL, retaining legacy exact-ID pointers and refusing ambiguous or hidden parent disclosure. Malformed reranker responses preserve the original reciprocal-rank-fusion ordering. Federation and message failures now distinguish local storage, target resolution and known-peer request failures with bounded diagnostics and remedies that omit credentials and message bodies.

Unused memory-transition helpers are removed without changing consensus lifecycle behavior. Bounded CI suites and provenance-aware retrieval benchmark tooling accompany reconciled integration, lifecycle and research documentation. App-v28 stays active, with no consensus execution change or chain migration. The qualified v15 local judge pin is unchanged.

## v11.23.14 release

MCP stdio dispatches a bounded set of independent tools concurrently, reads cancellation while tools are running, and drains dispatched work before EOF or runtime handoff. AMID restores identity-only signer-fence intents from a private local ledger before either serving mode starts; malformed ledger state refuses startup. Proven fence retirement finishes durable cleanup before reopening its signer, preserving the next submission's recovery record. Disabling the REST validator signing key also closes ordinary validator signing. The bundled CometBFT block-sync threshold rounds one-third power up correctly. These changes introduce no consensus execution change, app-version change or chain migration.

The optional managed local judge and evidence checks are restored after qualification with the real SAGE Go reader and pinned Ollama v0.31.1 on darwin/arm64. The public seed set meets the bench's quality bars with one support trap accepted. The feature remains experimental and off by default; private fresh holdouts and other platforms were not independently remeasured. Both judge backends enforce loopback transport, reject cloud aliases or unqualified managed blobs, and hold unavailable verdicts for operator review.

## v11.23.13 release

REST response deadlines cover embedding retries, the bounded nonce-lease wait, consensus commit and response bookkeeping, so a slow submission can return its acknowledgment after committing. The default budget is 285.75 seconds, capped at ten minutes. Vault recovery verifies the supplied key against the current vault before replacing `vault.key`.

The memory gate retains its v11.23.11 behavior. The unreleased evidence check and managed local judge remain deferred, including model packaging and the Go port. The cancelled v11.23.12 tag is not moved or published. No consensus execution change, app-version change or chain migration is introduced.

## v11.23.11 release

The optional [memory-quality gate](reference/write-gate.md) runs judges outside the voter loop and holds uncertain proposed memories for operator review in Settings → Memory gate. It is off by default, uses SQLite storage, preserves fresh built-in checks, and sends only memory text from configured domains. Review decisions revalidate readable content and projection integrity; stable cursors keep later memories reachable after earlier decisions. Judge-service identity and an optional revision tag invalidate cached verdicts when the judge changes.

A pending vote can remain fenced after its target commits because a stale-vote CheckTx filter prevents it from reaching an indexed execution result. The resolver now accepts a versioned code-13 proof only for valid signed MemoryVote bytes and a complete canonical committed target submitted after app-v25 activation. Exact hash binding and an index recheck preserve the distinction between an original vote that committed and a retry that can no longer succeed. General code 13, historical targets and incomplete state remain unresolved.

CLI help is handled before command dispatch, configuration and instance locking. Inbound federated message admission now allocates recipient wakes atomically with message and dedup insertion, publishes after commit, and catches up unfinished work at startup.

This release leaves consensus execution and active app versions unchanged. AMID maintenance must separately preserve and resolve pending signing intent; its entrypoint does not install the durable fence restore hooks used by `sage-gui`.

## v11.23.10 release

The write path could be refused forever by a key that was waiting for proof.

A signer fence refuses every write from its key until an earlier submission's fate is known, and its wait was bounded
only by the caller's context. The REST submit path deliberately passes `context.Background()` - so that a client
disconnect cannot cancel an already-authorized durable write - which is a context with no deadline at all, so the
`<-ctx.Done()` arm of the fence wait could never fire. One held fence therefore parked the single per-key nonce lease
indefinitely: the observed node carried 19+ handlers in the same frame for 122-768 minutes, and every later submit for
that key, plus the automatic voter whose reconciliation is what lifts a fence, queued behind it on the same size-one
lease. The hold could not clear itself, and from the operator's seat it read as "reads are fine, writes time out, no
error" - because reads never take the lease, and a denied write is refused earlier still.

`WithNonceLease` now restores the invariant its own documentation states: when the caller's context carries no
deadline, the primitive derives one (90 seconds), so the existing cancellation paths return a retryable
`ErrSignerFenced`/`DeadlineExceeded` instead of parking, the slot is released, and the reconciler is no longer starved
by the fence it exists to lift. A caller that set its own deadline keeps it, so client-driven cancellation is still
not reintroduced.

Two smaller fixes ride along. Background goroutines started by the embedded web handler had no lifecycle owner, so
work that outlived its caller's store could touch a closed database and panic the process - it is counted and drained
now. And local `make` builds pin to the host architecture, while a Codex session started inside a checkout inherits
that checkout's Codex pin from `.codex/config.toml`.

No consensus change, no transaction-type change, no upgrade height, and no chain reset.

## v11.23.9 release

The fence workstream's last loose end was that the dashboard stayed silent about it.

A signer fence refuses every write from its key and every coordinated restart while it waits for proof of
an earlier submission's fate. The health payload has carried the block since v11.23.3 and the per-fence
detail for an operator session since v11.23.4, and nothing in CEREBRUM rendered it: the incident that
started the workstream read from the operator's seat as "reads are fine, writes time out, no error".
System Status now shows a row only while a key is held - how many, how long, the node's own explanation,
and one line per fence naming its `resolution`, the field that says how the fence can end: `reconciling`
means the identical bytes are still being re-submitted and it clears itself, `proof_or_operator` means the
bytes did not survive the process so the chain or an operator has to settle it.

The panel also keeps the LAST resolution visible after the hold is gone, which is the outcome the mechanism
exists to produce and the one the UI could not show: `committed`, `rejected`, `spent`, or a resolution
without a proof. Two details make what it shows trustworthy. The nonce crosses the wire as a decimal
string, because SAGE nonces are nanosecond allocations well past JavaScript's safe integer range and a JSON
number arrived silently rounded in the one place an operator compares it against the chain. And the copy
never suggests restarting, because a restart discards the fence rather than settling it - the same rule the
fence's own Go tests enforce on the server side.

This release also carries a CI change with no runtime effect: the test suite and the race checks now run as
parallel jobs with the required "Test" context as their fan-in, after a shared 40-minute job budget
cancelled the race step on a slow suite run.

No consensus change, no transaction-type change, no upgrade height, and no chain reset.

## v11.23.8 release

Two defects in the signer-fence machinery, one on each side of the operator's reach.

The first is a fence factory. Every coordinated restart has drained signing before it committed since v11.23.3 - quiesce the
allocator, wait for the in-flight population to reach zero, re-check the veto - so its teardown cannot sever a broadcast. An
ordinary exit had no such drain: a signal or a serve error drained HTTP for its budget and then force-closed, and a broadcast
caught in that window raised an indeterminate outcome, wrote a durable fence record, and cost its payload at the next start,
where the fence can only lift on a proof. The ordinary exit now runs the same drain before any listener is closed: signing is
quiesced, in-flight submissions get a five-second window, and the listeners close afterwards. Signing is deliberately not
resumed, an operator-ordered exit is never vetoed by the drain, and a submission that outlasts the window fails closed onto the
durable record and the restored fence.

The second is a recovery route that existed but could not be used. `sage-gui fence abandon` applies the same evidence gate as
the daemon's abandon route, and that gate requires a second acknowledgement whenever peers are connected, because a peer is a
route the abandoned bytes could still take back into this node's mempool. The CLI had no flag to carry it, so on a federated
desktop node or a validator with a persistent peer it could only ever refuse, naming a field the operator had no way to set.
`--peer-redelivery-acknowledged` now carries it, `fence list` names the peer requirement when peers are connected, and every
unproven record states the restart step: a running daemon keeps holding the key until it restarts, and the durable record is
what the next boot restores from.

No consensus change, no transaction-type change, no upgrade height, and no chain reset.

## v11.23.7 release

The pre-upgrade recovery proof learns the rule that app-v28 commits. The signed update flow takes a snapshot of
the committed state and then proves it by restoring the Badger backup and re-deriving the AppHash the manifest
recorded. Since app-v28 that digest is the public-memory composite commitment - the app-v13 digest of the state
WITHOUT the public-memory index nodes, composed with the sparse index root - and the proof only knew three eras:
legacy, app-v12 and app-v13. Every app-v28 node therefore refused its own snapshot with
`AppHash mismatch under every hash rule (legacy/app-v12/app-v13)`, which blocked every signed app update and the
version-changing restart behind it. This is the same stale-rule defect the state-sync provider verification path
carried until v11.23.0, in the one proof path that never moved onto the shared rule.

The composite now joins the candidate set, read through the same `internal/store` implementation the commit path
uses rather than a third copy of the rule. A state that predates app-v28 carries no public-memory commitment, so
the composite reports as not-applicable there and the three older candidates decide alone - the existing proof
is unchanged for every chain that has not activated the fork, and the mismatch message now names the rules that
were actually tried.

A node on 11.23.6 or earlier cannot install this release from inside the app, because the failing gate runs in
the installed binary; installing it by hand once is the documented path, and every update after it is in-app
again. No consensus change, no transaction-type change, no upgrade height, and no chain reset.

## v11.23.6 release

A fence that no proof can ever reach, on a chain that is healthy but has nothing to mint, now settles itself
instead of holding its signing key forever. SAGE chains are idle by design: `create_empty_blocks=false` is set
for every node, so a block appears exactly when a signed transaction enters the mempool. A fence restored from
durable intent refuses to sign and its bytes are gone, so on a chain that was already quiet when the fence was
raised, the fence is what prevents the block that would prove it — the hold is self-sustaining, and on a
single-validator personal node after an upgrade restart it had no exit at all.

The automatic resolution now recognises that shape. When the node is caught up, has no peer and has seen none
while this fence was held, holds no mempool copy, sees no committed fate for the hash, holds an unspent
allocation, and the tip was minted before this fence was raised, it settles the fence, records
`fence_abandoned` with the full evidence, reserves the abandoned allocation so the next transaction cannot reuse it,
and lets the next write mint the block that wakes the chain. The mode string names which evidence set reached
the decision: `automatic_unprovable` for the original startup resolution, `automatic_quiescent` for the rule
added here (a chain that has minted nothing since the fence was raised). The residual is the same one the existing automatic decision states: a surviving
copy of those bytes can still commit before the signer's next commit and lose its payload.

Everything that could still make these bytes come back keeps the fence: a tip that has minted since the fence
was raised, any connected peer, a peer seen under this fence, a mempool that holds the transaction (the chain
has something to mint and the fence is what stops it, so that case stays an operator decision), a node still
catching up, a tip that cannot be read, a live fence whose exact bytes still exist, and a durable record
without a nonce.

No consensus change, no transaction-type change, no upgrade height, and no chain reset.
Two diagnosability and reachability defects from the field are closed in the same release. Every outcome of the
automatic route now reaches the fence's `last_detail` - the refusal reason AND a fault, because the fault branch
was previously dropped silently, which made a node whose self-heal could not read its own evidence look identical
to one that was merely waiting. And `sage-gui fence list` / `sage-gui fence abandon --signer ... --reason ...
--acknowledge-payload-loss` give an operator standing at the node host a real entry that needs no credential and
applies the same evidence gate the daemon's route applies: the dashboard had no control for this action, and an
HTTP MCP bearer token is not accepted by the operator gate, so the only local entry was a browser console.


## v11.23.4 release

The fence fixes reach the nodes that keep a peer. v11.23.3 resolved a restored fence at first boot only when the
node had never seen a peer since process start, and its operator abandon route refused outright whenever any peer
was connected - so a federated desktop node, or a validator with a persistent peer, could sit behind a fence that
no proof could settle with both recovery routes closed, still refusing every write. The peer observation is now
anchored to the fence rather than to the process, the automatic route still refuses while a peer is connected or
was seen under this fence, and the operator abandon route accepts connected peers against an explicit
`peer_redelivery_acknowledged` acknowledgement, recording the peer count with the decision. The health surface
gained a per-fence `resolution` class (reconciling versus proof_or_operator) and the automatic route's refusal
reason, and the held-fence alarm stopped claiming a re-submission a restored fence cannot make. No consensus
change, no transaction-type change, no upgrade height, and no chain reset.

## v11.23.3 release

A held signing key stops being a dead end, and the release that fixes it can be
installed. A fence restored from durable intent has no signed bytes, so
re-submission cannot settle it: before this release nothing re-read its proofs,
the key refused to sign, and the restart veto refused every coordinated restart
(every in-app update) for as long as the node ran. The only exits were an
operator POST that needed a proof the chain did not have, or hand-editing the
database.

The node now re-reads the two proofs it accepts on the live reconciler's
schedule — the recorded hash in a committed block, and the signer's committed
nonce having reached the fenced allocation, labelled `spent` when it is equal
because the nonce floor alone cannot say whether those bytes committed or were
overtaken — and lifts the fence the moment either holds. The one shape no proof
can settle is resolved at first boot, when the evidence is unambiguous: the node
is caught up, has seen no peer this run, holds no mempool copy, sees no committed
fate, and the allocation is unspent. That decision is recorded as
`fence_abandoned` with `mode=automatic_unprovable` and the full evidence, and the
abandoned nonce is reserved so the next transaction cannot reuse it. A node that
has talked to a peer keeps its fence: there the transaction can still come back,
and only a proof or an operator may lift it.

The coordinated-restart veto now asks whether a fence's durable record survives
instead of refusing on the existence of a fence, and fails closed when the
record cannot be confirmed. That is what made the previous behavior a dead end:
the record IS on disk, so the restart was safe, and refusing it blocked the only
release that could clear the fence. Request-serving write paths also refuse
immediately with 503 + Retry-After, naming the transaction the key is held on,
instead of parking on the fence until the caller's timeout — the shape agents
reported as "writes timed out, no error" while reads stayed healthy.

Recovery for a node already stuck on v11.23.2: replace the app bundle (drag the
new build into `Applications`, then quit and reopen) and the first boot resolves
the fence on the evidence above, with no terminal involved.

No consensus change, no transaction-type change, no upgrade height, and no chain
reset.

## v11.23.2 release

An app-v28 activation no longer fails on a chain whose public corpus predates
the fork. The public-memory stage is built outside the consensus transaction, so
a record the migration cannot encode aborts the activation block on every
replay — and a real chain carried 53 legacy `PUBLIC=0` records whose canonical
content hash had been erased by a historical lifecycle transition. The node
could not start, and `upgrade cancel` could not run because it is itself a
consensus transaction; the only exits were an offline edit of the pending plan
or removing the records. `publicLeafFromCanonical` now quarantines that class
out of the committed public set, matching what the co-commit tombstone index
already does with the same input, and a record joins the set in the block that
re-anchors its hash.

The rule is deliberately narrow: only a record accepted before app-v25 lacks a
submission-height marker, and only those could have had their hash erased, so a
missing hash on a record born under app-v25 or later still refuses the build.
The commitment is unchanged — this decides whether an activation succeeds, not
what the fork commits. No transaction-type change, no upgrade height, and no
chain reset.

## v11.23.1 release

An amid-only validator fleet can now set an agent's enrollment clearance. amid
mounts exactly the dashboard pair — `GET /v1/dashboard/network/access` and
`PUT /v1/dashboard/network/access/agents/{id}/policy` — with the same handlers
and the same operator gate CEREBRUM serves, so the transaction is identical; the
operator signs the exact request as the current Root, the credential such a host
already holds, and the node's configured broker key is what authenticates it.

This closes the gap that made an audit writer unable to land classified records:
the memory-write gate compares a submission's classification against the
agent's *enrollment* clearance, and neither the legacy PATCH route (retired
post-app-v23) nor any amid surface could write that record. Membership
clearance is a different value and never satisfies the gate.

Patch release inside the 11.23 line: no shell range move, no transaction-type
change, no consensus change, and no chain reset.

## v11.23.0 release

App-v28 activates. The auto-vote ceiling moves 27 -> 28 — the release that
carries the activation evidence is the release that raises the ceiling — so the
gate v11.22.0 compiled dormant becomes an automatic rung: a personal node
advances its own chain across the seam on upgrade, with no governance ceremony,
exactly as it did for every earlier rung. What runs from the activation height
is the sparse public-memory Merkle commitment over committed `PUBLIC=0`
records, AppHash-covered through a composite rule that hashes the legacy tree
without the index nodes and composes it with the index root, and the co-commit
tombstone rule enforced by the consensus path through a content-hash reverse
index maintained by every memory write and backfilled at activation.

The evidence the contract named arrives with the bump. The four-validator
determinism ladder walks app-v2 → app-v28 one rung at a time with byte-identical
AppHash at H-1/H/H+1 of every seam; both Consensus Fault Gates pass on this
change; the real-process state-sync gate now drives its provider through
app-v28 (`TARGET_APP_VERSION=28`), so a pristine receiver restores from the
provider snapshot and both sides must report exact app-v28 state with converging
AppHash; and the same gate's receiver pre-publication SIGKILL and provider
SIGKILL phases, plus the replay family that pins historical blocks to their
original app versions, cover restart and replay equivalence across H.

The item that failed when v11.22.1 was cut is the one that was fixed: a v28
provider died on its state-sync serving boot because the whole app-v20
verification family recomputed the pre-v28 AppHash rule unconditionally, so the
composite root could never match and the process exited — which surfaced as a
Comet RPC that never became ready rather than as a refusal to serve. The rule is
now selected in one function shared by the commit path and the verification
path, and the same gate with `TARGET_APP_VERSION=27` still passes.

This is a minor bump because the shipped native shell's SSCP compatibility range
must widen to admit v11.23 daemons; fixes after this one ship as patch releases
under the same shell. No chain reset and no transaction-type change; historical
blocks keep replaying under their original app versions.

## v11.22.1 release

This release is operator surface, not consensus. CEREBRUM's Federation page now
leads with trusted connections and states each link's discovery posture on its
row, with per-agent Visible/Not-visible switches that save immediately under the
connection's revision-bound agreement. The App version panel renders on nodes
without governance scopes — it was nested behind the scope list, which stays
empty until a `scope_action` commits, so personal nodes never saw the chain's
deliberate upgrade path. The Python SDK gains
`set_agent_access_policy`/`get_access_state` against the app-v23 enrollment
routes, the clearance the memory-write gate actually reads.

App-v28 stays compiled and dormant. The determinism half of its evidence is in
hand: `TestAppHashDeterminism_AppV28Activation` walks app-v2 → app-v28 on a
fresh four-validator devnet with byte-identical AppHash at H-1/H/H+1 of every
seam, and the real-process gate shows the whole ladder crossing with auto-votes
alone once the ceiling allows it. The contract's last item does not pass yet —
a promoted node surviving state-sync and restore — because the real-process
gate's restarted v28 provider comes back, serves, and then stops answering its
Comet RPC. The 27 → 28 ceiling bump, the switch that turns the dormant gate into
an automatic rung for every node including personal ones, ships in the release
that turns that green.

No consensus change and no chain reset; historical blocks keep replaying under
their original app versions.

## v11.22.0 release

App-v28 ships as a compiled, dormant gate, and the release that opens it also
opens the deliberate path to activate one.

Two consensus-visible changes now exist behind a single gate. The sparse
public-memory Merkle index over committed `PUBLIC=0` records becomes
AppHash-covered at the activation height, through a composite rule that hashes
the legacy tree without the index nodes and composes it with the index root. The
co-commit tombstone rule stops being a check at the local REST submission
boundary and becomes a rule the consensus path enforces: a co-commit never
consults the voter, so the content-hash dedup that keeps rejected bytes out of
the store never ran there, and a directly broadcast envelope never met the REST
guard at all. The fork adds the reverse index that predicate needs — one entry
per content hash and id that has left `proposed` — maintained by every memory
write, backfilled from existing records at activation, and consulted from H+1
with the candidate's own id excluded.

Neither change is active. `maxSupportedAppVersion` stays 27, so every node's
auto-voter abstains on v28 and a personal node cannot advance itself into the
fork. Activation waits for the evidence the contract names — byte-identical
AppHash across the seam on a four-validator devnet, both Consensus Fault Gates,
a promoted node surviving state-sync and restore, and replay equivalence — and
ships in the release that carries it. An explicitly proposed, quorum-approved
plan can still activate it; nothing does so on its own.

The binary also stopped reporting one ceiling for two different questions. What
it can execute (app-v28, compiled) and what it will auto-vote (app-v27) are now
separate facts: `upgrade status` and `upgrade preflight` print both, the version
banner and full-backup stamp follow the compiled ceiling, and state sync accepts
a restore up to it so a promoted node can be rebuilt from a snapshot. The
acceptance gate and the authorization ceiling moved with that split, and the
invariant that required all three numbers to be equal now states what each one
means.

Upgrades finally have a deliberate path, which is what a dormant gate needs:
`sage-gui upgrade vote` casts accept, reject or abstain on the active upgrade
ballot, and CEREBRUM's Governance view gains an App version panel that proposes
the next rung, shows both ceilings, and says plainly when a target is dormant.
The generic governance-propose route still refuses `OpUpgrade` by design; the
dashboard speaks the dedicated `UpgradePropose` transaction instead.

No chain reset, no transaction-type change, and historical blocks keep
replaying under their original app versions. app-v27 remains the ceiling until
this gate's evidence lands.

## v11.21.0 release

The 11.21 line opens, and the release that opens it ships the shell that admits
it.

The SSCP compatibility range lives in the shipped native shell rather than in
the daemon, so a minor bump is the release that has to widen it. The shell now
accepts v11.10 through v11.21 daemons, which is what lets fixes on this line
ship as patch releases instead of a shell rebuild per fix. A daemon on 11.21
under the 11.20.5 shell is refused control until that shell is updated with it;
that refusal is the gate working as designed rather than a fault.

The daemon is otherwise the v11.20.5 build, so there is no new behavior to
re-verify and nothing to migrate. The moved-peer route repair, the backlog wake
and the confidence-floor disclosure are the same bytes that shipped as 11.20.5.

No consensus change and no chain migration; app-v27 remains the ceiling.

## v11.20.5 release

A peer whose address moved repairs its own route, and a short window of
reachability is spent on the backlog rather than on one message.

The R2 fix let a P2P-only agreement run the authenticated route exchange across
trust generations, because withholding the route fallback left it no transport
at all. The same assumption fails for an agreement paired with a CONCRETE
endpoint once that host moves: the address is real, so nothing looks unroutable,
but every request dials a machine that no longer answers while the peer's own
traffic keeps arriving. Withholding the fallback there also blocked the
exchange, which is the only thing that can replace the stale snapshot, so the
pair could not recover without being re-paired. The exemption is now about the
path rather than the agreement shape — the exchange may use a stale snapshot as
an authenticated bootstrap hint for both — while every other request still
refuses a cross-generation route and the persisted result is revalidated against
the exact agreement and binding first. A moved host also stops reading as an
offline one: the failure carries the trust-generation code, so the operator's
next move is a route repair, not a network investigation.

Delivery exploits the windows that a flapping peer leaves open. The drain only
attempts rows whose backoff has expired, so a freshly queued event — due
immediately on its first attempt — consumed the window while older rows slept
through it, which is why a message created at 21:15 could deliver while messages
from 21:10 and 21:11 stayed queued. A successful delivery now proves the peer
reachable and makes the rest of that peer's backlog due at once, with attempt
counts and last errors untouched, and a drain pass covers sixteen rows instead
of four at unchanged concurrency.

Recall also stops hiding results behind the confidence floor in silence: the
store counts what the floor removed, the REST envelope and the MCP result
disclose it with the floor value, and the settings surface warns which write
tiers a floor hides. A memory reachable by tag but below the floor is now
reported as filtered rather than absent.

No consensus change and no chain migration; app-v27 remains the ceiling.

## v11.20.4 release

A signer fence now survives the process that raised it, and a nonce that is
provably dead has a way out.

The fence is in-process state, and its documentation had named that as the hole
since it was written: a restart, crash or SIGKILL discarded it, the allocator
re-seeded each key from the highest COMMITTED nonce — below the abandoned one by
definition, because "unresolved" is what unresolved means — and the next action
signed into the gap. The abandoned transaction was refused Code 4 when it
finally landed, and the loss was untraceable: an operator saw an unrelated later
action fail as a replay. Every submission is now shadowed by a durable record
written at the last boundary before the bytes reach the transport, retired only
on a proven fate, and re-raised as a fence at startup, so a node killed
mid-submission comes back refusing to sign that key rather than re-seeding past
it. The record carries identity and not payload: signed bytes routinely hold
memory content that must not be copied into a plaintext table, so a restored
fence resolves by proven fate instead of by re-submission, and a crash between
the record and the wire holds a key that may have nothing in flight — the safe
direction, through the same proof path.

The second half is the exit. Only proven fate lifts a fence, which left one
shape the reconciler could not resolve on its own, so the operator surface now
accepts two proofs and nothing weaker: the exact transaction found in a
committed block, or supersession — a higher nonce for that signer has committed,
which makes the fenced allocation permanently uncommittable under the consensus
nonce rule and records that its payload is lost. Both are read by the node
rather than asserted by a caller, and a lookup miss stays unproven, because
CometBFT indexes a transaction only once it is in a block.

No consensus change and no chain migration; app-v27 remains the ceiling.

## v11.20.3 release

A federated message can no longer be permanently failed by a peer's "too large"
answer, and the reason it was refused is now the reason recorded.

The outbox classified `413` with the 4xx statuses that mean the peer understood
the request and refused the bytes, so one refusal marked the transport event
`failed` and the canonical message with it: the row is never scanned again, a
durable-until-handled message carries an expiry a century out that never relieves
it, and the operator's only signal is one `last_error` string. That is the wrong
verdict for this status. A peer's per-route body cap is a build-time constant
that moves when the peer upgrades, and the refusal is often not about size at
all: on 2026-09-15 a message was refused as too large while its signed body sat
2.6 KB under the route's 16 KiB cap, and a larger message to the same peer was
accepted unchanged minutes later. `413` now retries on an hourly floor, like the
other capability-shaped status (`501`), so the event stays pending and delivers
when the peer can take it.

The listener was answering `413` for a second, unrelated condition. It read the
request body and reported any read error as an oversize body, which merged a
genuine over-cap body with a truncated upload, a mid-body disconnect and a stream
reset — so a transient receive-side failure reached the sender wearing the one
status it treats as permanent. Only a real `*http.MaxBytesError` is `413` now;
anything else is answered as a read failure, which the sender may retry, and
logged with its cause.

Delivery failures are also no longer silent: the transport worker logged only
when recording a failure failed, so an event could die with nothing in the log to
say why. Each failed attempt now records the event, the peer, the kind, the
attempt count, whether the verdict was terminal, and the retry delay.

No consensus change or chain migration; app-v27 remains the ceiling.

## v11.20.2 release

An ambiguous consensus submission is now reported as ambiguous. When the node's
own wait for block inclusion expires, the transaction is on the wire and may
still commit — a condition that on a loaded cluster is routine rather than
exceptional, because the wait and the block cadence are the same order of
magnitude. The submit endpoint used to answer that with the same opaque
`500 Broadcast error` it uses for a genuine internal fault, so a caller could
not tell "your write may already be committed" from "your write never
happened", and a caller retrying on error re-signed a second transaction on top
of the first.

REST now answers `202` with `"status":"indeterminate"`, the exact `tx_hash` of
the bytes that went on the wire, the allocated `nonce`, and
`"retryable":false`, while the signer nonce fence continues reconciling the
transaction's real fate. The hash is derived locally from the encoded
transaction, so it identifies the transaction even though the node's response
did not. Definitive outcomes are unchanged and explicitly excluded: a CheckTx or
FinalizeBlock rejection keeps its own status, and a full mempool keeps its
`429` + `Retry-After`, because nothing was admitted and there is nothing in
flight.

`deploy/init-testnet.sh` also stops inheriting CometBFT's 10 s
`timeout_broadcast_tx_commit` and writes 45 s explicitly, kept below SAGE's
client-side `SAGE_TX_COMMIT_TIMEOUT_MS` (60 s) so the node answers first. The
upstream default is short enough that the node's wait can expire before the
block lands, which is what produced the ambiguity in the first place.

No consensus change, no chain migration, and app-v27 remains the ceiling.

## v11.20.1 release

A federated reply is admissible — and its retained outbox event keeps retrying —
for seven days after the proof that signs it, instead of 24 hours. The destination
still admits the legacy 24-hour window, and one that predates the longer one is
answered by a single downgraded retry rather than a terminal failure, so replies
keep flowing across a mixed-version fleet. Receipt evidence about a message keeps
its own separate 24-hour grace.

The retention migration that extends durable canonical sends no longer re-stamps
reply rows: an imported message's receiver-local `msg-fed-…` id matched its
`msg-%` predicate, which pushed the reply's lifetime to +100 years, made every
attempt an invalid proof at the destination, and removed the retry deadline that
would have surfaced the loss. The rescue is scoped to sends, stamps the exact
durable sentinel instead of SQLite's calendar `+100 years`, repairs rows an
earlier build already extended, and reply envelopes are built from the signed
proof so local retention state cannot reach the wire.

Refused proofs are diagnosable now: the destination logs the exact reason and the
sender self-checks its reply envelope against that same rule, because ten
historical failures were only ever recorded as the destination's single opaque
`invalid pipeline agent proof` refusal.

No consensus change, no chain migration, and app-v27 remains the ceiling.

## v11.20.0 release

Encrypted agent working state on the node. `PUT`/`GET /v1/private-media/{uuid}` stores immutable JPEG originals with ciphertext-only rows, per-caller actor isolation, quota enforcement and a startup disk-floor probe; `PUT`/`GET /v1/workflows[/{uuid}]` is an encrypted, actor-bound workflow journal with compare-and-swap revisions, strict argument bounds, bounded list discovery and an opt-out conversation guard. Neither surface has a delete route, neither can read another agent's rows, and both fail closed when the Synaptic Ledger vault is absent or locked rather than writing plaintext to the database or its WAL. One route gained headroom: a canonical-UUID private-media `PUT` gets 2 MiB, everything else keeps the 1 MiB authenticated body ceiling.

Honest storage posture and fail-closed sends. `GET /v1/messages/storage` reports whether canonical message storage on this node is actually encrypted, and `POST /v1/messages` accepts a strict signed `require_encrypted_storage` boolean — a caller that must not be stored in the clear now receives `503` instead of a silent downgrade, and a duplicate, unsigned or non-boolean value is rejected.

Local durability proven at boot. SQLite opens with `synchronous=FULL` in both DSN and `PRAGMA` form and the node verifies `journal_mode` and `synchronous`, refusing to serve when the durability posture cannot be proven. Vault publication is atomic via a single publication lock, so attaching a vault and marking encryption required are never separately observable — an unlock cannot leave a window where a write is accepted against a store that does not yet require encryption. The workflow journal and private-media paths take the same lock through commit.

Lantern bring-up support. `sage-gui init-lantern-private` creates a fresh-only hardware identity, refusing an existing or mismatched node, requires the companion-key bootstrap flag explicitly, and installs no services and no public enrollment; its failure paths are contained. `SAGE_LANTERN_PRIVATE_LISTENERS` makes a missing config an explicit error rather than a default, and the policy is re-checked at YAML load so an edit cannot silently weaken it. A `private_media` config block with a startup space probe is validated at load so an unusable posture refuses to boot.

Dormant public-memory Merkle index. The sparse SHA-256 index over committed `PUBLIC=0` records, its migration builder and its stage/promote path ship with tests and no production caller. Staged rows live under a local namespace excluded from the AppHash, and promotion — the step that writes into AppHash-covered `public-memory-index:v1:node:` state, explicitly named for app-v28 — is unreachable from a running node. This release therefore prepares the path without opening a fork: no consensus change and no chain migration; app-v27 remains the ceiling.

Agent discovery you control. A trusted link used to advertise every eligible ordinary local agent to the peer, which with more than a couple of agents invites mis-addressed work: the other operator sees unfamiliar names, picks one, and sends to the wrong agent. Each connection now carries an explicit discovery policy — All agents (the default, unchanged), Only the ones I pick, or None — where ticking one agent narrows the connection to exactly that agent and Save commits the change. It governs the peer's listing, paging, and exact-name/id search only; memory Read and delivery keep their own independent switches, and an agent that refuses federated delivery is still advertised as Not accepting by design.

## v11.19.22 release

The Go build floor moves from 1.25.13 to patched Go 1.26.8 in both the root and `natter` modules, and every Go container builder moves with it (`Dockerfile`, `deploy/Dockerfile.abci`, `deploy/Dockerfile.node`, both federation-acceptance Dockerfiles, `deploy/init-testnet.sh`); source builds now require Go 1.26.8 or later. The Go dependency group is refreshed to pgx v5.11.0, klauspost/compress v1.20.0, x/crypto v0.57.0, x/sync v0.23.0, x/sys v0.48.0, x/tools v0.50.0 and modernc.org/sqlite v1.58.0 — SQLite 3.53.4, whose upstream journal-rollback fix retires the local super-journal patch — with the store tests moving from pgxmock/v4 to pgxmock/v5 for the new `pgx.Rows.TypeMap` method. Nothing in the shipped binaries' behaviour changes and the chain is untouched: the floor move is what lets the dependency group land at all, because it requires Go 1.26 and the bare 1.26.0 that would otherwise be pinned is itself the version carrying 26 reachable standard-library advisories. No consensus change; app-v27 remains the ceiling.

## v11.19.21 release

A co-commit can no longer re-commit content the quorum already rejected: `POST /v1/cocommit/submit` consults the content-hash dedup before it broadcasts and refuses a tombstoned hash with `409 Tombstoned content`, excluding the envelope's own `SharedID` so an idempotent re-send still works. This is a submission-boundary check, not a consensus rule — the consensus path reads no off-chain state — so a node that broadcasts the transaction directly is not covered. The MCP client no longer keeps its own >60%-word-overlap duplicate heuristic: it reports the node's `pre-validate` verdict, so an exact duplicate is a skip carrying the node's reason and a near-duplicate is stored. Docs mark `validated` as declared-but-unwritten and state that knowledge triples and `access_logs` are write-only. No consensus change; app-v27 remains the ceiling.

## v11.19.20 release

Voter dedup is sticky: another memory that has left `proposed` (validated, committed, challenged, or deprecated) blocks identical bytes from being re-admitted, the candidate's own row is always excluded, and concurrent identical submissions no longer reject each other. A correction still passes when its content changed. The dedup lookup is indexed on SQLite and Postgres (one-time Postgres index rebuild at first boot). README and reference docs qualify the consensus claims for single-validator personal installs. No consensus change; app-v27 remains the ceiling.

## v11.19.19 release

gRPC-Go v1.83.2 addresses the xDS missing-authority-header denial of service (CVE-2026-84445). No consensus change; app-v27 remains the ceiling.

## v11.19.18 release

Federation agent motion is visibly continuous, with pauses limited to inspecting an agent or dragging. Browser regression checks measure screen displacement and verify pointer and keyboard resume behavior.

## v11.19.17 release

Interactive federation connectome, bounded operator-only SSE transport activity,
clear node identity, and streamlined trust-only onboarding. The broader v12
standalone/web design system remains future work.

## v11.19.16 release

Trusted paired nodes automatically expose eligible ordinary agents for discovery
and messaging when both peers support the new protocol. Memory domains are not
shared by pairing; Read and Copy remain explicit operator choices. Root identities,
explicit messaging blocks, pairing generations, and message proof checks remain
enforced. Existing approved exports remain intact, and older peers retain the
legacy export-based protocol.

CEREBRUM provides a searchable node/agent directory, exact-address copying, bounded
pagination, and bulk/drag-and-drop Read/Copy drafts with explicit Save. MCP directory
lookup includes federated agents by default. New sends refresh legacy recipient
tickets; already-queued events preserve their original authorization mode.
Session-bound MCP replies now pass strict peer proof decoding, and retries report
retained delivery state and untrusted diagnostics without silently requeuing
failed events.

No consensus-rule or application-version change; app-v27 remains the ceiling.

## v11.19.15 release

CEREBRUM memory cleanup now scans all pages rather than stopping at 500 rows,
and uses canonical consensus challenge transactions for both manual and automatic
runs. Fresh current-Root consent is required for automation; old enabled settings
do not activate it on upgrade. Open tasks and internal records are protected.
Durable exact-byte recovery and explicit progress/outcome counts replace the
former SQLite-only cleanup path and misleading completion display.

No consensus-rule, AppHash-input, key-encoding, fork-target, or application-version
changes are introduced; app-v27 remains the ceiling.

## v11.19.14 release

gRPC-Go v1.83.1 fixes HTTP/2 DATA-frame fragmentation heap exhaustion
(CVE-2026-84304, Dependabot alert #45), with the required genproto and
OpenTelemetry dependency refresh. CodeQL init, analyze, and upload-sarif
actions are pinned to the verified v4.37.9 commit.

This release introduces no consensus-rule, AppHash-input, key-encoding,
fork-target, or application-version changes; app-v27 remains the ceiling.

## v11.19.13 release

The stdio MCP bridge now skips automatic project hook repair when its working
directory resolves to the user's home directory. This closes the remaining
implicit path that could write `.claude/hooks` and project-relative hook
registrations into user-global host configuration merely by starting the
bridge from `$HOME`.

The explicit MCP and Codex install commands retain the home-directory refusal
added in v11.19.12. Ordinary project-directory self-healing is unchanged, and
`CLAUDE_CONFIG_DIR` deliberately does not suppress valid project repair.

This is an off-consensus bridge safety correction. It changes no transaction,
AppHash input, key encoding, fork target, or application version; app-v27
remains the ceiling.

## v11.19.12 release

`sage-gui mcp install` and `sage-gui codex install` now reject a working
directory that resolves to the user's home directory. Both commands emit
project-relative configuration and hooks; allowing that output to land in
`~/.claude` or `~/.codex` creates a global configuration whose hook paths break
as soon as another project is opened. Normal project-directory installs are
unchanged.

The Linux native-shell gate now admits the exact SHA-256 of the official
September `linuxdeploy-plugin-appimage` scheduled rebuild. The downloaded
binary matches GitHub's release-asset digest and was built from the same
upstream source commit as the prior admitted artifact. Digest verification
remains fail closed.

This is an off-consensus installer and release-build correction. It changes no
transaction, AppHash input, key encoding, fork target, or application version;
app-v27 remains the ceiling.

## v11.19.11 release

CEREBRUM can now sit behind a loopback TLS-terminating reverse proxy without
requiring that proxy to rewrite the browser-facing hostname to `localhost`.
`SAGE_ALLOWED_CEREBRUM_HOSTS` adds exact, port-normalized hostnames to the
existing Host and forwarded-host checks; it deliberately supports no wildcard.
The connected peer and every forwarded IP hop remain loopback-only.

Browser origin matching consumes `X-Forwarded-Proto` only when every header
field-line and comma-joined hop is a valid case-insensitive `http` or `https`
token and all values agree. Empty, malformed, or mixed chains fail closed.

This is an off-consensus local dashboard boundary extension. It changes no
transaction, AppHash input, key encoding, fork target, or application version;
app-v27 remains the ceiling.

## v11.19.10 release

CEREBRUM can approve a returning identity whose exact recorded home domain was
retired to the stable Root principal. The approval binds the live Root owner and
uses the existing owner-fenced transfer fields; it never infers ownership for a
fresh or operator-entered domain. Pending-registration rejection now checks the
active-memory lifecycle projection rather than the all-history attribution
count, so deprecated audit records remain preserved without creating an
impossible remediation loop.

This is an off-consensus dashboard lifecycle correction using existing
transaction fields and authorization rules. It changes no consensus rule,
AppHash input, key encoding, fork target, or application version; app-v27
remains the ceiling.

## v11.19.9 release

Unpinned Codex MCP identity resolution now rejects the filesystem root before
probing Git, reading workspace configuration, or generating a signer. This
prevents the desktop app-server's `/` working directory from resurrecting the
retired `global-codex` identity and registering the misleading `codex//`
principal. Real repository and worktree roots retain their existing stable
identity derivation, and explicit `SAGE_IDENTITY_PATH` pins remain available
for intentional non-workspace identities.

This is an off-consensus client identity-boundary correction. It changes no
transaction, AppHash, key encoding, fork target, or application version;
app-v27 remains the ceiling.

## v11.19.8 release

The bounded caller-domain projection now consults the authoritative current
owner index for both the caller and active local Access Group peers. This makes
transferred historical domains discoverable even when their current owner never
authored a memory there, while preserving the existing 64-result output bound,
candidate and peer-scan caps, truncation signal, and exact live-policy
reauthorization. Access Group authority remains dynamic and ownership-based;
the projection does not copy grants or disclose a global domain roster.

This is an off-consensus REST/MCP discovery correction. It changes no
transaction, AppHash, key encoding, fork target, or application version;
app-v27 remains the ceiling.

## v11.19.7 release

Recall-backed compaction is an explicitly enabled personal-node feature. The
PreCompact hook validates and binds the canonical transcript descriptor,
streams bounded byte-exact chunks into governed memory with deterministic IDs,
and advances per-thread progress only after committed reconciliation. Malformed
or abandoned spans become committed, recall-visible gap markers rather than
silent loss. SessionStart restores the complete captured thread in order before
ordinary recall. Operators can revoke future capture or govern prior records
with `sage-gui nevercompact purge`; SAGE does not promise physical hard delete.

CEREBRUM's memory view can isolate typed reasoning links from domain and parent
edges, including distinct `supersedes` and `duplicates` styles. Store upserts
also make re-typing an existing directed memory pair last-write-wins instead of
silently discarding the new type. Go modules and pinned CI actions are updated.
No transaction, AppHash, fork target, or application version changes; app-v27
remains the ceiling.

## v11.19.6 release

`POST /v1/memory/links` accepts up to 256 memory IDs and returns only typed
links whose two endpoints are both requested and readable by the caller. The
new `sage_get_links` MCP tool exposes the same batched projection, allowing a
recall result to be followed by explicit `supports`, `contradicts`,
`supersedes`, and `refines` reasoning without N+1 reads.

Domain and record-level authorization run before the link-store query, so an
unreadable endpoint cannot leak through an edge. Malformed or unavailable
legacy visibility policy is an explicit operational failure, never a false
complete graph. The feature is read-only and off-consensus; app-v27 remains the
ceiling.

The personal desktop upgrade contract is explicitly one-click: SAGE performs
the compatibility proof, recovery and stopped-state snapshots, coordinated
shutdown, install, rollback on failure, and restart. Manual preflight remains
an operator procedure for quorum or externally mutable governance only.

## v11.19.5 release

Both exact-local compatibility claim paths—`GET /v1/pipe/inbox` and explicit
`PUT /v1/pipe/{pipe_id}/claim`—now bind the runtime claimant and create its
session receipt in the same transaction as ownership. Durable claimant identities are scoped to
the effective agent, provider, canonical project, and transport identity across
stdio, Streamable HTTP, and SSE. MCP exposes `claimant_identity_mode` so a
caller can distinguish durable ownership from concurrent-ephemeral fallback or
a fail-closed unavailable identity.

Claim ownership never expires merely with age. Passive claim projections carry
`claim_revision`; `sage_message_handoff` requires that revision with the exact
source session, and the store increments it on transfer so stale and A→B→A
delayed handoffs fail visibly. For pre-v11.19.5 REST clients only, omitted
`from_revision` means revision 0; it cannot move a claim that has ever advanced.

Signed `GET /v1/inbox/activity-state` returns exactly `{version,epoch,seq}` and
advances for fresh task-assignment and reply activity. Its opaque 32-character
database-incarnation epoch survives restart and backup restore but changes for
a fresh database, preventing a preserved host cursor from suppressing cues
after reinitialization. It is deliberately
separate from unfinished work: task/reply activity never changes the exact
three-field v1 message-wake contract or blocks Stop, and a prompt hook cannot
resurrect a host task that is already idle.

Consensus behavior and the app-v27 ceiling are unchanged.

## v11.19.4 release

The updater now probes the exact staged replacement executable for its maximum
supported application version and passes that capability—not the running
binary's ceiling—into the compatibility proof. Canonical governance inspection,
replacement validation, committed height, and AppHash capture occur under one
uninterrupted `runtimeViewMu` read fence, which remains held through snapshot
verification and executable mutation.

This closes the v11.19.3 TOCTOU window where Commit could publish newer
governance state after validation but before snapshot fencing. The snapshot was
coherent, but its compatibility verdict could be stale. The v11.19.3-to-v11.19.4
transition remains automatic for personal single-node installs because both
releases have the same app-v27 ceiling and personal-node automation cannot
introduce an unsupported next-app transition. The documented stopped-node
procedure is limited to quorum or externally managed deployments where
governance can be mutated concurrently; once v11.19.4 is running, future live
updates use the corrected proof.

Consensus behavior and the app-v27 ceiling are unchanged.

## v11.19.3 release

The production updater reads canonical governance state under the same runtime
view lock used by ABCI queries, validates the current version plus any pending
plan or active upgrade ballot against the running updater's supported ceiling, and
did so before a separately acquired state fence, recovery snapshot, and
executable replacement. Supported in-flight operations are compatible and continue after
restart; ordinary users see no governance prompt and run no command.

Stopped-node `upgrade preflight` now applies the same compatibility policy for
technical operators instead of treating the mere presence of a supported plan
or ballot as a blocker. Corrupt, inconsistent, undecodable, or unsupported
state still fails closed. Consensus behavior and the app-v27 ceiling are
unchanged.

v11.19.4 supersedes this release's atomicity claim: a Commit could run between
the v11.19.3 validation and snapshot-fence acquisitions.

## v11.19.2 release

The new `/upgrade/governance-status` ABCI query exposes a bounded,
schema-versioned projection of the canonical pending `upgrade:plan` record and
the exact `state:gov:active` proposal. Upgrade ballots decode and report their
target application version. Any storage, pointer/proposal identity, bounds,
status, height, canonical-name, or payload error rejects the query rather than
manufacturing an empty result.

`sage-gui upgrade status` consumes this authoritative projection instead of
combining `/abci_info` with the off-chain dashboard governance mirror. The
stopped-node `upgrade preflight` command calls the same inspector against a
read-only Badger handle. v11.19.3 corrects its replacement policy and wires the
compatible-state decision directly into the normal updater.

This patch changes no consensus rule, AppHash input, transaction type, key
encoding, fork target, or application version. The ceiling remains app-v27.

## v11.19.1 release

The exact `claimed_elsewhere_count` continues to cover every still-actionable
claim held by another runtime sharing the same agent identity, including claims
older than the generic history window. A new oldest-first, cursor-paginated
recovery projection makes each counted row actionable using only its message
ID, claimant-session fence, timestamps, and local/federated classification.
`sage_inbox` embeds the first bounded page, while
`sage_message_history(folder="claimed_elsewhere")` pages the remainder without
claiming, acknowledging, handing off, or completing anything.

Sender identity, provider and chain IDs, intent, payload, and result remain
outside this projection. Ownership changes only through the existing
compare-and-swap handoff route. Past-TTL rows are excluded from the scalar and
page before the periodic expiry sweep, matching wake and same-session recovery.

Provider-addressed compatibility rows now acquire an exact claimant-session
receipt atomically with their inbox claim. They reappear under same-session or
other-session recovery, can be handed off with the same CAS fence, and complete
idempotently through `sage_message_reply`; identical lost-response retries
replay while conflicting results fail. Fresh-token receive responses also carry
the passive recovery surfaces, and failed replies explicitly do not authorize a
substitute `sage_message_send`. Startup backfills existing claimed provider rows
behind an off-chain `legacy` session fence.

This patch introduces no consensus change or application-version increase. The
supported consensus ceiling remains app-v27.

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

## v11.11 – v11.15 - shipped (the productization bridge to v12)

v11.10 completes the federation and federated-agent backend/administrative experience. v12 is the **product**: a standalone native application in which every CEREBRUM function is mapped to a real app experience, with the web dashboard kept only as a still-supported fallback. The releases between them de-risk and stage that transition so v12 is an integration-and-polish capstone, not a from-scratch rewrite. Every step keeps the hard constraints — no chain reset, upgrade-in-place, the SAGE daemon and authenticated local APIs cleanly separated from any shell, and no weakening of the local trust boundary. Order and grouping are indicative; nothing here is dated.

### v11.11 - the native shell foundation

Choose the desktop-shell technology through the deliberate evaluation v12 requires, then ship the first additive, opt-in native application that embeds CEREBRUM in its own window. The browser CEREBRUM remains fully supported as the fallback. v11.11 is an architectural release, not a cosmetic wrapper, and must complete these deliverables:

- **Desktop-shell decision record.** Compare the credible macOS, Windows, and Linux options with a weighted scorecard covering threat surface and sandboxing, signed packaging/notarization, accessibility, performance and memory cost, offline operation, cross-platform maintenance, update behavior, and long-term ownership. Build small proof-of-concept spikes for the finalists, document the threat model, and record the chosen shell plus rejected alternatives before product code commits to it.
- **App–daemon trust and compatibility contract.** Specify authenticated bootstrap, credential/key storage, IPC versus loopback transport, origin/navigation restrictions, process and port ownership, startup readiness, crash detection and recovery, graceful drain/shutdown, version negotiation, and rollback-compatible independent updates. The shell receives no implicit validator/admin authority and cannot weaken the existing local API boundary.
- **Single-instance lifecycle and navigation ownership.** The native app owns its window, deep links, route restoration, external-link handoff, foreground/background behavior, and daemon supervision. Reopening SAGE focuses the existing native window; it does not depend on browser tab inspection or browser-specific automation. Browser CEREBRUM remains an explicit fallback, not an accidental second primary UI.
- **Measurable foundation gate.** Set budgets for cold/warm launch, idle CPU, memory, large-store interaction latency, and animation frame pacing on supported hardware. Use a monotonic animation clock and compositor-friendly transforms for continuous motion instead of coarse React timer rerenders; establish keyboard navigation, focus visibility, screen-reader naming, and reduced-motion architecture now even though v11.14 performs the full hardening pass. Automated packaging and smoke tests must cover clean install, daemon unavailable/recovery, app restart, deep-link routing, offline launch, and shell/daemon version skew on every supported OS.

No chain reset. The SAGE daemon and authenticated local APIs remain independently testable and operable underneath the shell.

The accepted framework decision, protocol boundary, and blocking release gates
are recorded in [`desktop-shell-decision.md`](desktop-shell-decision.md),
[`native-app-daemon-contract.md`](native-app-daemon-contract.md), and
[`native-shell-quality-gates.md`](native-shell-quality-gates.md). The tracked
Tauri foundation remains an opt-in preview until that full matrix passes.

**The native shell is alpha and does not gate releases.** Browser CEREBRUM is
the product; the shell is a background track through v11.11–v11.15. It is built
and runtime-tested in CI, never staged as a public release asset, and not
intended for end-user use. Releases continue shipping bug fixes and capabilities
on their normal cadence — federation, agent-to-agent messaging, and the rest of
the roadmap do not queue behind desktop packaging. The signing, notarization,
update/rollback, recovery, performance, and accessibility bar applies at **first
distribution of the shell**, which is v12.

**Platform scope: macOS and Windows are the shell's target platforms; Linux is
not.** v11.11 distributes no native shell on any platform — see the paragraph
above — so this is scope for the eventual distribution at v12, and for what CI
produces release evidence for in the meantime. Linux users are served by browser
CEREBRUM and the CLI, both fully supported and unaffected: this narrows the
native shell, not the platform. The Linux target still builds and runs its full
installed-package lifecycle smoke in CI so cross-platform regressions in the
shared shell and SSCP code are still caught. Linux re-enters the target set only
when upstream Wry ships GTK4/webkitgtk-6.0 (`tauri-apps/wry#1769`); SAGE will not
fork or vendor the web view layer to get there sooner.

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

v12 is the planned completion milestone for the SAGE product roadmap: the fully integrated product rather than another backend-only release. It ships as a standalone desktop application in which every CEREBRUM dashboard function is mapped to a real native app experience — the same capabilities, but presented as a proper application rather than a set of web pages. The web CEREBRUM remains a supported fallback for anyone who wants it; it simply is not the primary product surface. By v12 the v11.11–v11.15 bridge has already chosen the desktop shell, proven consumer onboarding and recovery, moved lifecycle into the app, established accessibility/performance/offline gates, and made local/federated collaboration governable, so v12 is the integration-and-polish capstone that ties it into one coherent product. The app owns installation, node lifecycle, onboarding, permissions, updates, health/recovery, federation, and Sharing & Sync as one coherent native-feeling experience, while the SAGE daemon and authenticated local APIs remain cleanly separated underneath.

**Consumer usability is a release criterion, not polish.** A nontechnical person must be able to install SAGE, create or join a node, connect an AI tool, choose what is private or shared, recover from ordinary failures, and keep the app updated without opening a terminal or learning SAGE internals. Every choice uses plain language and safe defaults; destructive or privacy-affecting actions use consistent accessible SAGE dialogs; errors explain what happened, what remains safe, and the next recovery action. The v12 release gate includes clean-machine onboarding and recovery usability tests with people who have not used SAGE before.

The desktop-shell technology is deliberately not locked here. It must be chosen through a security, packaging, accessibility, performance, offline-operation, and cross-platform evaluation; “native-feeling” must not come at the cost of weakening the local trust boundary or bundling an unmaintainable browser runtime.

---

## Current Foundation

The v11 line carries the upgrade substrate, domain-reassign recovery, ancestor grants, PoE-weighted quorum, verdict-correctness scoring, corroboration, and domain-aware validator weighting. Those are baseline capabilities now, not future roadmap work. Release-by-release history lives in the README changelog.
