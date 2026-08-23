# Joint SAGE v12 and SAGE Commons/Lantern roadmap contract

**Status:** roadmap contract; implementation evidence is required per row

**Current SAGE core:** v11.19.0 / app-v27

**Pinned Lantern beta bundle:** v11.18.4 / app-v26 pending joint baseline review

**Products:** SAGE core, SAGE Commons, SAGE Lantern, Knowledge Point, Data Donkey

## Purpose

SAGE v12 is the native-product capstone, but SAGE Commons and SAGE Lantern use
the daemon and authenticated APIs directly on Linux/ARM64. This contract keeps
the three roadmaps aligned without pretending that every Commons or fleet
feature belongs in SAGE consensus.

It distinguishes:

1. behavior available in the pinned v11.18.4 beta and preserved by v11.19.0;
2. compatibility SAGE core must preserve and prove for v12;
3. new SAGE-core dependencies that need their own implementation and evidence;
4. Commons/Lantern work that stays outside SAGE core; and
5. capabilities that remain unsupported and must not be implied by product copy.

The hard constraints remain no chain reset, upgrade in place, exact identity and
authorship preservation, local Root sovereignty, and no hidden widening of
federation or fleet authority.

## Long-horizon relationship

SAGE Commons is not an accessory, downstream demo, or one-release consumer. It
is SAGE's long-term community, public-knowledge, distribution, field, and
hardware programme. SAGE core supplies the sovereign primitives; Commons turns
those primitives into independently operated public and offline systems without
absorbing personal-node authority into a foundation, fleet, or validator plane.

The relationship is deliberately bidirectional:

- SAGE core publishes stable capability, compatibility, migration, and evidence
  contracts that Commons can plan against.
- Commons and Lantern provide concrete field, ARM64, intermittent-network,
  public-intake, distribution, and recovery requirements that shape future SAGE
  planning.
- A Commons requirement becomes a SAGE commitment only after ownership,
  security, sovereignty, compatibility, migration, and—where applicable—BFT
  replay and governed activation are accepted.
- A SAGE deprecation or contract change that affects Commons requires impact
  analysis, a supported transition, and acceptance evidence before removal.
- Neither roadmap may claim the other programme's planned capability as shipped.

This contract therefore continues after v12. Version-specific baselines and
evidence will advance, while the ownership and joint-change-control model stays
in force.

## Product topology frozen for the beta

- Every Lantern runs one full **personal SAGE** as its sovereign local memory
  node when hardware measurements pass.
- The Lantern's public role is a **query client and signed proposer**, not a
  Commons validator.
- Public proposals are queued as logical signed envelopes while offline. When
  connected, the Lantern signs a fresh authenticated SAGE request under its
  distinct public ordinary-agent identity.
- Public query material and Commons Snapshot Packs live in a labelled,
  replaceable **Lantern Cache**. They never become personal canonical memory by
  copying files.
- Stable, always-on servers validate and commit the public Commons chain.
- Knowledge Points provide long-lived cache, synchronization, research, and
  publishing services; possession of content does not grant validator power.
- Data Donkey is an untrusted, content-addressed super-seed. It is independent
  of SAGE consensus, state sync, full backup, and Federation Copy.

An optional second public observer process stays disabled until SAGE has a
documented long-lived non-validator product role and the ROCK hardware budget
passes. A boot-time state-sync receiver is not repurposed as that observer.

## Capability crosswalk

| Capability | v11.18.4 baseline | v12/core obligation | Commons/Lantern obligation | Boundary |
|---|---|---|---|---|
| Personal sovereign node | Full app-v27 node, local Root, ordinary agents, recovery and upgrade-in-place; Lantern beta remains pinned to app-v26 until reviewed | Preserve and acceptance-test on Linux/ARM64 without requiring the native shell | Package, supervise and measure it on Lantern hardware | The v12 native app targets macOS only; Lantern compatibility remains headless and must not require a desktop shell |
| Roles and collaboration | Root separate from Member/Manager/Admin; app-v26 local Access Groups | Preserve exact semantics and publish capability/version evidence | Map product roles to least-privilege local agents and separate fleet capabilities | Remote field operators never become local members, Admin, or Root |
| Agent messaging | Canonical messages, exact identities, sender replies, passive status/history | Preserve SDK/MCP/REST compatibility and offline/restart evidence | Use messages only for untrusted coordination and workflow notices | Message delivery never delegates memory or device-mutation authority |
| Public proposer | Ordinary-agent signed `MemorySubmit` enters the local/public chain lifecycle; the generic v11.18.4 submit route has no request-level idempotency contract | Preserve fresh nonce/timestamp signing, exact caller identity, and exact transaction/memory evidence | Maintain encrypted offline outbox, consent and public provenance; the Commons intake owns envelope-level idempotency and performs at most one SAGE submission | Federation remote Write remains unavailable; Root is not the proposer; generic-memory idempotency is a separate possible core enhancement, not a beta dependency |
| Public intake | Direct submission and normal validator lifecycle exist | No implicit v12 promise of a new consensus schema | Build durable intake/quarantine, schema/privacy checks, human/governance review and receipts | Beta validation is an application boundary before `MemorySubmit` |
| Hyperlocal content validation | App-v27 contains no consensus gate for the proposed hyperlocal schema | Any consensus-enforced gate requires a separately governed future app-version plan | Enforce the beta schema and locality/privacy policy in intake and clients | v12 product semver does not imply a hyperlocal consensus fork |
| Public query | Signed query/recall APIs and PUBLIC classification exist | Preserve bounded authenticated query and explicit source/lifecycle metadata | Build Commons query service, cache policy, stale/expired filtering and provenance UI | Never blend private and public recall without labels |
| Commons Snapshot Pack export | No frozen public subset export/proof API | Define and acceptance-test a deterministic committed-PUBLIC export at a frozen checkpoint, with exact source/app/version metadata | Sign, catalogue, chunk, distribute and render staleness for derived packs | Export signature/root is off-chain attestation unless native inclusion proofs exist |
| Federation | Trust plus explicit Read/Copy; Write reserved/501 | Preserve fail-closed identity/policy generation and no-write behavior | Use only where online Read/Copy semantics are actually desired | Federation Copy changes destination canonical author and is not pack transport |
| State sync and backup | Authorized empty-node state sync; complete stopped-node backup/restore | Preserve both and keep their formats isolated | Use neither as Data Donkey or Lantern Cache formats | Full backup contains private node material; state sync is not public distribution |
| Release/update evidence | Signed releases, exact version/source provenance, safe in-place recovery patterns | Expose a stable compatibility/evidence contract for embedded/headless consumers and prove Linux/ARM64 artifacts | Build TUF-style pack channels, fleet campaigns, local consent and device receipts | Field commands cannot bypass SAGE's own migration/recovery gates |
| Fleet/device management | No remote fleet authority protocol | Preserve a narrow local API boundary; do not reinterpret Admin or messages as fleet control | Build signed offline capabilities, device agent, command sandbox, telemetry/privacy and execution receipts | Fleet Admin is a Commons role, never SAGE Admin |
| Dual-home/multi-instance | Separate processes are mechanically possible with fully separate homes and identities; not first-class packaging | Either deliver a documented supported multi-instance profile or keep it explicitly out of v12 acceptance | Beta uses personal SAGE plus adapter/cache; do not require the optional observer | Never share homes, data roots, chain IDs, listeners, Root keys, or validator keys |
| Validator topology | Stable server validators and governed roster/power operations | Preserve exact governance and recovery semantics | Operate independent always-on Commons validators and public transparency | Lanterns, Donkeys, and field laptops carry no validator key or voting power |

## SAGE-core v12 release gates for Lantern and Commons

The following evidence is required before claiming the v12 core is compatible
with Lantern/Commons:

1. **Linux/ARM64 headless lifecycle.** Clean install, first boot, ordinary-agent
   enrollment, personal memory submit/recall, canonical messaging, restart,
   stopped-node backup/restore, and signed upgrade all pass without the native
   shell.
2. **Version and capability evidence.** Lantern can determine the exact SAGE
   release, source provenance, consensus app version, chain ID/role, and API
   capability set without parsing human UI text or using a private database.
3. **Public proposer path.** A distinct ordinary proposer can queue offline,
   reconnect, obtain one envelope-idempotent Commons intake decision, create at
   most one fresh authenticated SAGE submission, and observe exact transaction,
   proposed/committed, or rejected evidence. Existing v11.18.4 generic memory
   submission is not claimed to be request-idempotent; the beta resolves the
   ambiguity at intake rather than resubmitting blindly.
4. **No-authority regression.** A field, Donkey, message, linked reader, and
   federated identity each fail to acquire local membership, Root/Admin,
   validator/governance, remote Write, or personal-memory access.
5. **Public export contract.** The exporter freezes one committed checkpoint,
   emits only selected committed PUBLIC records, excludes private and
   off-consensus state, binds deterministic counts/hashes and source metadata,
   and states honestly whether per-record consensus inclusion proofs exist.
6. **Private/public separation.** Public cache results remain labelled and
   replaceable; importing one requires an explicit intake path and never changes
   personal SAGE by filesystem copying.
7. **Consensus compatibility.** Any new application behavior proves byte-identical
   historical replay, governed activation, full upgrade/recovery evidence, and
   no reset of existing app-v27 chains or the pinned app-v26 Lantern beta path.

## Commons/Lantern deliverables that do not block on new SAGE consensus

These can proceed against the v11.18.4 baseline and should not wait for a v12
native desktop shell:

- Unit A local PTT loop and personal SAGE integration;
- versioned MeshCore availability and bounded artifact exchange;
- encrypted logical public-proposal outbox and fresh-request adapter;
- durable intake/quarantine and review service;
- public query/cache labelling and expiry policy;
- Data Donkey pack, catalogue, trust, revocation, custody and swarm protocols;
- Foundation, Field Operations, and Lantern user consoles;
- fleet capability, command, consent, telemetry and receipt contracts; and
- hardware inventory, resource, thermal, power and two-unit acceptance evidence.

## Explicit non-promises

Unless separately designed, implemented, reviewed, and accepted, neither v12 nor
this contract promises:

- a consensus-native hyperlocal schema or an implied future app-version fork;
- remote federation Write;
- a long-lived state-sync receiver as a public observer;
- first-class multiple SAGE instances on one device;
- per-record AppHash inclusion proofs for a derived public export;
- remote SAGE Admin/Root operation from a field console;
- validator participation by Lantern, Knowledge Point, or Data Donkey; or
- automatic movement of private memories into Commons, packs, telemetry, or a
  Return Pouch.

## Change control

Every Commons/Lantern roadmap item that names a SAGE capability must link to one
of the crosswalk rows above and carry one state:

- `available-v11.18.4`;
- `required-sage-v12`;
- `commons-lantern-owned`;
- `future-consensus-proposal`; or
- `unsupported-do-not-use`.

Changing a row's owner or state requires review by both SAGE core and the
Commons/Lantern product owner. Product copy may not promote `planned`,
`future-consensus-proposal`, or `unsupported-do-not-use` into a shipped claim.

## Joint roadmap operating cadence

The two programmes maintain one rolling dependency register with three horizons:

1. **Current compatibility:** exact released SAGE baseline, immutable evidence,
   supported Commons/Lantern use, and known limitations.
2. **Accepted delivery:** dependencies with an owner, target gate, interface or
   RFC, migration plan, negative-authority tests, and acceptance evidence.
3. **Research horizon:** candidate consensus, federation, proof, hardware, and
   distribution capabilities that remain non-promises until promoted through
   the normal review path.

Review occurs at every SAGE release that changes a relied-upon contract, every
Commons programme gate, and at least quarterly while either roadmap is active.
The review records exact versions, capability-state changes, unresolved
decisions, deprecations, security/sovereignty impact, and the next evidence
owner. A failed or missing cross-programme review blocks the affected capability
claim, not unrelated personal SAGE or Lantern work.
