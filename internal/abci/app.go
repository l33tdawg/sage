package abci

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/contentvalidator"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	memorytags "github.com/l33tdawg/sage/internal/tags"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"

	cryptoproto "github.com/cometbft/cometbft/proto/tendermint/crypto"
)

// pendingWrite represents a PostgreSQL write buffered until Commit.
type pendingWrite struct {
	writeType string // "memory", "memory_tags", "triples", "challenge", "vote", "corroborate", "epoch_score", "validator_score", "status_update", "access_grant", "access_request", "access_revoke", "access_log", "domain_register", "access_request_status", "org_register", "org_member", "org_member_remove", "org_member_clearance", "federation", "federation_approve", "federation_revoke", "mem_classification", "dept_register", "dept_member", "dept_member_remove", "agent_register", "agent_update", "agent_permission"
	data      interface{}
}

type memoryTagsData struct {
	MemoryID string
	Tags     []string
}

// statusUpdate carries the fields needed to update a memory's status in PostgreSQL.
type statusUpdate struct {
	MemoryID string
	Status   memory.MemoryStatus
	At       time.Time
	// app-v17 two-phase challenge (C3/C-D3): populated ONLY when Status is
	// "challenged" (park). DisputedHeight is the challenge-EXECUTION height — the
	// off-chain mirror records it so ResolveChallengedMemories can distinguish a
	// live v17 dispute from a legacy pre-fork challenged row, and DisputedQuorum
	// carries the deterministically-measured modify-verb-holder count for display.
	// Both zero for every non-park status update.
	DisputedHeight int64
	DisputedQuorum uint32
}

// accessRevokeData carries the fields needed to revoke a grant in PostgreSQL.
type accessRevokeData struct {
	Domain    string
	GranteeID string
	Height    int64
}

// accessRequestStatusUpdate carries the fields needed to update an access request status in PostgreSQL.
type accessRequestStatusUpdate struct {
	RequestID string
	Status    string
	Height    int64
}

// orgMemberRemoveData carries the fields needed to remove an org member in PostgreSQL.
type orgMemberRemoveData struct {
	OrgID   string
	AgentID string
	Height  int64
}

// orgClearanceData carries the fields needed to update a member's clearance in PostgreSQL.
type orgClearanceData struct {
	OrgID     string
	AgentID   string
	Clearance store.ClearanceLevel
}

// federationApproveData carries the fields needed to approve a federation in PostgreSQL.
type federationApproveData struct {
	FederationID string
	Height       int64
}

// federationRevokeData carries the fields needed to revoke a federation in PostgreSQL.
type federationRevokeData struct {
	FederationID string
	Height       int64
}

// deptMemberRemoveData carries the fields needed to remove a dept member in PostgreSQL.
type deptMemberRemoveData struct {
	OrgID   string
	DeptID  string
	AgentID string
	Height  int64
}

// agentUpdateData carries the fields needed to update an agent in offchain store.
type agentUpdateData struct {
	AgentID string
	Name    string
	BootBio string
}

// agentPermissionData carries the fields needed to update agent permissions in offchain store.
type agentPermissionData struct {
	AgentID       string
	Clearance     int
	DomainAccess  string
	VisibleAgents string
	OrgID         string
	DeptID        string
}

// memClassificationData carries the fields needed to update a memory's classification in PostgreSQL.
type memClassificationData struct {
	MemoryID       string
	Classification store.ClearanceLevel
}

// memoryReassignData carries the fields needed to reassign memories in offchain store.
type memoryReassignData struct {
	SourceAgentID string
	TargetAgentID string
}

// govProposalData carries governance proposal data for offchain write in Commit.
type govProposalData struct {
	ProposalID    string
	Operation     string
	TargetID      string
	TargetPower   int64
	ProposerID    string
	Status        string
	CreatedHeight int64
	ExpiryHeight  int64
	Reason        string
}

// govVoteData carries governance vote data for offchain write in Commit.
type govVoteData struct {
	ProposalID  string
	ValidatorID string
	Decision    string
	Height      int64
}

// govStatusUpdateData carries governance proposal status update for offchain write in Commit.
type govStatusUpdateData struct {
	ProposalID     string
	Status         string
	ExecutedHeight int64
}

// validatorSetAdapter wraps ValidatorSet to satisfy governance.ValidatorProvider.
type validatorSetAdapter struct {
	vs *validator.ValidatorSet
}

func (a *validatorSetAdapter) GetValidator(id string) (int64, bool) {
	v, ok := a.vs.GetValidator(id)
	if !ok {
		return 0, false
	}
	return v.Power, true
}

func (a *validatorSetAdapter) GetAll() map[string]int64 {
	result := make(map[string]int64)
	for _, v := range a.vs.GetAll() {
		result[v.ID] = v.Power
	}
	return result
}

func (a *validatorSetAdapter) Size() int {
	return a.vs.Size()
}

// triplesData carries knowledge triples buffered for Commit.
type triplesData struct {
	MemoryID string
	Triples  []memory.KnowledgeTriple
}

// suppCacheEntry wraps supplementary data with a timestamp for eviction.
type suppCacheEntry struct {
	data     *memory.SupplementaryData
	storedAt time.Time
}

// SupplementaryCache bridges REST API → ABCI for data that doesn't travel on-chain.
// The REST handler stores supplementary data here before broadcasting a tx; ABCI
// FinalizeBlock reads it when building the pending write for Commit, ensuring
// strict consensus-first ordering while preserving off-chain data like embeddings.
type SupplementaryCache struct {
	mu    sync.RWMutex
	items map[string]*suppCacheEntry

	// evicting is true only while the live REST bridge has non-empty work and
	// one bounded eviction loop owns it. Starting the loop lazily matters: most
	// standalone ABCI instances never receive supplementary REST data, and a
	// constructor-started goroutine would outlive short-lived replay/test apps.
	evicting bool

	// finalizeParent/consumed are populated only on a transaction-local clone.
	// They let a successful Commit remove exactly the cache entries consumed by
	// FinalizeBlock, while a panic/discard leaves the live REST bridge untouched.
	finalizeParent *SupplementaryCache
	consumed       map[string]*suppCacheEntry
}

// NewSupplementaryCache creates an empty cache. The automatic eviction loop is
// started lazily by the first Put and exits after the cache becomes empty.
func NewSupplementaryCache() *SupplementaryCache {
	return &SupplementaryCache{items: make(map[string]*suppCacheEntry)}
}

// Put stores supplementary data for a memory ID.
func (c *SupplementaryCache) Put(memoryID string, data *memory.SupplementaryData) {
	now := time.Now()
	startEvictor := false
	c.mu.Lock()
	c.pruneExpiredLocked(now)
	c.items[memoryID] = &suppCacheEntry{data: data, storedAt: now}
	if c.finalizeParent == nil && !c.evicting {
		c.evicting = true
		startEvictor = true
	}
	c.mu.Unlock()
	if startEvictor {
		go c.evictLoop()
	}
}

// Pop retrieves and removes supplementary data for a memory ID.
func (c *SupplementaryCache) Pop(memoryID string) *memory.SupplementaryData {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pruneExpiredLocked(time.Now())
	entry := c.items[memoryID]
	delete(c.items, memoryID)
	if entry != nil && c.finalizeParent != nil {
		c.consumed[memoryID] = entry
	}
	if entry != nil {
		return entry.data
	}
	return nil
}

// cloneForFinalize snapshots the cache without starting another eviction
// goroutine. Entry pointers are immutable after Put; retaining the pointer also
// lets commitConsumed avoid deleting a newer replacement inserted concurrently.
func (c *SupplementaryCache) cloneForFinalize() *SupplementaryCache {
	if c == nil {
		return nil
	}
	c.mu.RLock()
	defer c.mu.RUnlock()
	clone := &SupplementaryCache{
		items:          make(map[string]*suppCacheEntry, len(c.items)),
		finalizeParent: c,
		consumed:       make(map[string]*suppCacheEntry),
	}
	for memoryID, entry := range c.items {
		clone.items[memoryID] = entry
	}
	return clone
}

// commitConsumed publishes only successful FinalizeBlock cache removals.
func (c *SupplementaryCache) commitConsumed() {
	if c == nil || c.finalizeParent == nil || len(c.consumed) == 0 {
		return
	}
	parent := c.finalizeParent
	parent.mu.Lock()
	defer parent.mu.Unlock()
	for memoryID, consumed := range c.consumed {
		if parent.items[memoryID] == consumed {
			delete(parent.items, memoryID)
		}
	}
}

func (c *SupplementaryCache) evictLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		c.pruneExpiredLocked(time.Now())
		if len(c.items) == 0 {
			c.evicting = false
			c.mu.Unlock()
			return
		}
		c.mu.Unlock()
	}
}

func (c *SupplementaryCache) pruneExpiredLocked(now time.Time) {
	for id, entry := range c.items {
		if now.Sub(entry.storedAt) > 60*time.Second {
			delete(c.items, id)
		}
	}
}

// appV20AtomicFinalize holds a complete speculative application graph and its
// uncommitted Badger transaction between FinalizeBlock and Commit. The live app
// continues serving CheckTx/Query from the committed graph until Commit
// atomically publishes this one.
type appV20AtomicFinalize struct {
	app   *SageApp
	store *store.BadgerStore
}

// SageApp implements the CometBFT ABCI 2.0 Application interface.
type SageApp struct {
	badgerStore   *store.BadgerStore
	offchainStore store.OffchainStore
	validators    *validator.ValidatorSet
	phiTracker    *poe.PhiTracker
	govEngine     *governance.Engine
	state         *AppState
	logger        zerolog.Logger
	Version       string

	// Buffered writes — only flushed to PostgreSQL in Commit
	pendingWrites []pendingWrite

	// flushMaxRetries bounds the SQLITE_BUSY retry loop in Commit. Exposed
	// as a field (not a const) so tests can shrink it to keep the panic
	// path fast.
	flushMaxRetries int

	// SuppCache bridges REST→ABCI for off-chain data (embeddings, triples, provider).
	// Nil in standalone ABCI mode without co-located REST API.
	SuppCache *SupplementaryCache

	// snapshotScheduler is the v7.5 Commit-tail snapshot trigger. nil
	// disables scheduled snapshots; SetSnapshotScheduler wires one
	// in after construction so existing NewSageApp callers don't break.
	snapshotScheduler *SnapshotScheduler

	// syncNotifier is the v11.5 domain-sync Commit-tail hook: when set it
	// receives the memory IDs whose status became committed in this block,
	// strictly AFTER the offchain flush + SaveState (so the SQLite mirror row
	// is guaranteed visible to the callback). Dispatched on a fresh goroutine
	// (the snapshotScheduler.Tick fast/non-blocking contract). NEVER reads or
	// writes consensus state — purely a low-latency nudge; the federation
	// drainer's poll scan is the correctness backstop.
	//
	// An atomic.Pointer (not a bare field) because SetSyncNotifier is wired
	// after the federation Manager is built, which on an existing chain can be
	// AFTER the consensus loop has begun producing blocks — so the install
	// races Commit's read. The atomic makes that publish/read data-race-free;
	// nil load = disabled, a no-op.
	syncNotifier atomic.Pointer[func([]string)]

	// v8AppliedHeight is the block at which the v8.0 access-control fork
	// activated. Zero means not yet activated — handlers must take the
	// pre-fork (v7.1.1-equivalent) branch. Populated from the persisted
	// AppliedUpgradeRecord on boot, refreshed in FinalizeBlock when the
	// v8 plan activates. Pre-fork blocks replay byte-identical to v7.1.1
	// because every fork-gated handler reads this field deterministically.
	v8AppliedHeight int64

	// v8_2AppliedHeight is the block at which the v8.2 PoE-weighted
	// quorum fork activated. Same semantics as v8AppliedHeight: zero
	// means pre-fork (Phase-1 equal-weight quorum, byte-identical to
	// v8.1.2); non-zero means quorum after this height consults the
	// persisted poew:<id> weights via postV8_2Fork's strict-greater-than
	// gate.
	v8_2AppliedHeight int64

	// v8_3AppliedHeight is the block at which the v8.3 PoE-signal fork
	// activated. Same semantics as v8_2AppliedHeight: zero means pre-fork
	// (Phase-1 accuracy = cold-start accept-ratio blend, corroboration = 0,
	// vstats:<id> records stay 24 bytes, byte-identical to v8.2.x); non-zero
	// means blocks after this height feed verdict-correctness EWMA and a real
	// per-validator corroboration count into processEpoch, and grow vstats:<id>
	// to 56 bytes. Gated by postV8_3Fork's strict-greater-than.
	v8_3AppliedHeight int64

	// v8_4AppliedHeight is the block at which the v8.4 Domain-factor fork
	// activated. Same semantics as the prior gates: zero means pre-fork
	// (domain is the neutral 0.5 constant everywhere, byte-identical to v8.3.x);
	// non-zero means blocks after this height (a) record each memory's domain in
	// memdomain:<id> at submit, (b) credit a per-domain verdict-correctness EWMA
	// in vstats_domain:<v>:<D> at terminal verdict, and (c) weight a validator's
	// quorum vote on a non-shared-domain memory by its live verdict-correctness
	// IN that domain. Gated by postV8_4Fork's strict-greater-than.
	v8_4AppliedHeight int64

	// v8_5AppliedHeight is the block at which the v8.5 / app-v6 fork
	// (upgrade-machinery hardening) activated. Zero means pre-fork:
	// processUpgradePropose accepts non-canonical plan names and version
	// regressions (byte-identical to v8.4.x), and processUpgradeRevert is the
	// Code-0 no-op stub. Non-zero means blocks after this height (a) reject a
	// propose whose Name != CanonicalUpgradeName(TargetAppVersion), (b) reject
	// a propose whose TargetAppVersion <= currentAppVersion(), and (c) reject
	// an upgrade revert outright (in-band downgrade is replay-unsafe). Gated by
	// postV8_5Fork's strict-greater-than.
	v8_5AppliedHeight int64

	// Layer-2 content-validation gate (deployment-agnostic core). Both are
	// zero-valued by default so a node that never wires a registry and never
	// activates the app-v7 fork behaves bit-for-bit as before. Enforcement is a
	// pure function of consensus state (the app-v7 fork height) AND whether a
	// validator registry is compiled in — there is NO separate runtime enable
	// flag, so two nodes on the same binary cannot disagree on whether the gate
	// is live.
	contentValidators  *contentvalidator.ContentValidatorRegistry // nil => no gate
	appV7AppliedHeight int64                                      // 0   => fork dormant

	// appV8AppliedHeight gates the app-v8 fork: once set (> 0), UpgradePropose
	// no longer self-activates a plan — it routes through the existing 2/3
	// governance quorum (governance.OpUpgrade) and the plan is persisted only
	// after the supermajority accepts. Zero by default, so every existing chain
	// (none has activated app-v8) replays the pre-fork self-activating branch
	// byte-identically. INDEPENDENT gate, like appV7AppliedHeight — NOT part of
	// the v8.x PoE monotonic ladder (excluded from reconcilePoEForkMonotonicity).
	appV8AppliedHeight int64 // 0 => fork dormant

	// appV9AppliedHeight gates the app-v9 fork (v9.1). Once set (> 0) two
	// consensus tightenings take effect at H+1: (1) nonce/replay protection is
	// enforced in the CONSENSUS path (processTx), not only in CheckTx, and (2)
	// processAgentRegister stops honouring a wire-supplied role="admin"
	// (self-grant) and downgrades it to "member". Zero by default, so every
	// existing chain (none has activated app-v9) replays the pre-fork branches
	// byte-identically. INDEPENDENT gate, like appV7/appV8 — NOT part of the
	// v8.x PoE monotonic ladder.
	appV9AppliedHeight int64 // 0 => fork dormant

	// appV10AppliedHeight gates the app-v10 fork (v9.2). Once set (> 0) the
	// corroboration integrity guard takes effect at H+1: processMemorySubmit
	// records the memory's author on-chain (memauthor:), and
	// processMemoryCorroborate rejects a self-corroboration (corroborator is the
	// on-chain author) or a duplicate corroboration (same agent twice). Zero by
	// default, so every existing chain replays the pre-fork branches
	// byte-identically. INDEPENDENT gate, like appV7/appV8/appV9.
	appV10AppliedHeight int64 // 0 => fork dormant

	// appV11AppliedHeight gates the app-v11 fork (v10.0): the per-node SQL→chain
	// admin bootstrap (bootstrapAdminFromSQL) is disabled on the consensus path
	// (it wrote a BadgerDB agent: record off per-node SQL — an AppHash-divergence
	// hazard, #36), and the chain-admin is instead established deterministically
	// at the app-v11 activation block (#35, materializeAppV11Admin). Zero by
	// default, so every existing chain replays the pre-fork branches byte-identically.
	// INDEPENDENT gate, like appV7/appV8/appV9/appV10.
	appV11AppliedHeight int64 // 0 => fork dormant

	// appV12AppliedHeight gates the app-v12 fork (issue #40: idle chain growth).
	// Once set (> 0), FinalizeBlock computes the AppHash EXCLUDING the volatile
	// state: keys (state:height / state:app_hash / state:epoch) that Commit's
	// SaveState rewrites every block. Pre-fork those keys feed the hash, so the
	// AppHash changes on EVERY block — including empty ones — and CometBFT's
	// needProofBlock forces a new empty block each timeout_commit to sign the
	// new hash, overriding CreateEmptyBlocks=false forever. Post-fork the hash
	// is a pure function of real consensus state and reaches a fixed point when
	// the chain is idle, so needProofBlock goes quiet and empty-block production
	// stops. Zero by default, so every existing chain replays the pre-fork hash
	// rule byte-identically. INDEPENDENT gate, like appV7..appV11.
	// KNOWN-FLAWED RULE, superseded by app-v13: the whole state:-prefix
	// exclusion also dropped real consensus state (gov:*, vote:*, …) from the
	// hash. Retained so v12-era blocks replay byte-identically.
	appV12AppliedHeight int64 // 0 => fork dormant

	// appV13AppliedHeight gates the app-v13 fork (v10.5.1): the corrected
	// AppHash rule. Once set (> 0), FinalizeBlock hashes via
	// ComputeAppHashExcludingBookkeeping, skipping EXACTLY the three SaveState
	// bookkeeping keys (state:height / state:app_hash / state:epoch) instead of
	// app-v12's whole state: prefix — restoring hash cover over the governance
	// engine (state:gov:*), memory quorum votes (state:vote:*), consumed
	// markers, and shared-domain sentinels while keeping the idle fixed point
	// that stops empty-block production (issue #40). Zero by default; v12-era
	// and earlier blocks replay under their own rules. INDEPENDENT gate, like
	// appV7..appV12.
	appV13AppliedHeight int64 // 0 => fork dormant

	// appV14AppliedHeight gates the app-v14 fork (v10.7.0): the symmetric
	// DEACTIVATION of the app-v7 Layer-2 content-validation gate. Once set (> 0),
	// postAppV7Fork returns false for every block AFTER this height, so the
	// content gate — armed by app-v7 — is turned OFF again chain-wide at a future
	// governed height, without retroactively flipping any committed block. The
	// gate is therefore live for exactly the window (appV7AppliedHeight,
	// appV14AppliedHeight]. Zero by default, so every existing chain (none has
	// activated app-v14) keeps the gate live indefinitely and replays
	// byte-identically. INDEPENDENT gate, like appV7..appV13 — NOT part of the
	// v8.x PoE monotonic ladder, and it changes NO AppHash rule (it only toggles
	// the content gate), so snapshot/verify and the FinalizeBlock hash-rule
	// switch are untouched.
	appV14AppliedHeight int64 // 0 => fork dormant (content gate never deactivated)

	// appV15AppliedHeight gates the app-v15 fork (v11): an EMPTY, next-free
	// scaffolding gate. It wires NO new behavior yet — no new tx types, no new
	// AppHash rule, no callsite consumes a v15-specific consensus change. Like
	// appV7..appV14 it is an INDEPENDENT gate, NOT part of the v8.x PoE monotonic
	// ladder. Zero by default, so every existing chain replays byte-identically.
	// Unlike app-v14's deactivation it IS additive/subsuming: postAppV15Fork is
	// OR'd into postAppV8Rules..postAppV11Rules so a skip-ahead-to-15 chain still
	// enforces the lower additive rules. The field/const/predicate/activation arm
	// are wired now so the next behavioral fork only adds rule logic + AppHash handling.
	appV15AppliedHeight int64 // 0 => fork dormant

	// appV16AppliedHeight gates the app-v16 fork (v11.2): the domainless-forget
	// remediation. It enables (a) the OpMemoryDomainRepair governance backfill of
	// legacy memdomain: records, and (b) the hardened deprecation gate (split the
	// domain=="" reject into legacy-vs-unknown; require a domain at submit so the
	// state can't recur). INDEPENDENT/additive gate; strict >, 0 by default, so
	// every existing chain replays byte-identically. Activated via
	// {Name:"app-v16", TargetAppVersion:16}.
	appV16AppliedHeight int64 // 0 => fork dormant

	// appV17AppliedHeight gates the app-v17 fork (v11.5): the memory-lifecycle
	// consensus sprint. It enables (a) TxTypeMemoryReinstate (drives the
	// challenged -> committed transition), (b) the quorum-scaled two-phase
	// challenge (deterministic modify-verb-holder count at challenge execution:
	// <=1 keeps the legacy one-strike deprecate, >=2 parks the memory as
	// `challenged` until a SECOND distinct modify-verb holder confirms or
	// reinstates), (c) the stale vote:* cleanup rider on co-commit
	// squat-reclaim, and (d) deterministic action binding, freshness, and
	// single-use state for delegated agent proofs. INDEPENDENT/additive gate;
	// strict >, 0 by default, so
	// every existing chain replays byte-identically. Activated via
	// {Name:"app-v17", TargetAppVersion:17}.
	appV17AppliedHeight int64 // 0 => fork dormant

	// appV18AppliedHeight gates the app-v18 RBAC administrator override. Once
	// active, an on-chain global admin may grant or revoke domain access without
	// taking ownership of that domain. This is an explicit, auditable override;
	// ordinary agents remain owner/ancestor-owner gated. INDEPENDENT/additive,
	// strict >, and dormant on every existing chain until governance activates
	// {Name:"app-v18", TargetAppVersion:18}.
	appV18AppliedHeight int64 // 0 => fork dormant

	// appV19AppliedHeight gates the app-v19 fork (v11.8): an EMPTY next-free
	// scaffolding gate, structurally identical to app-v15. It wires NO new
	// consensus behavior — no new tx types, no new AppHash rule, and no
	// processTx/FinalizeBlock branch keys off it. Unlike app-v18 (which carries a
	// LIVE admin RBAC override), app-v19 is deliberately behavior-empty on the
	// consensus path; its sole effect is an OFF-consensus read-default flip in
	// web/handler.go gated on IsAppV19ActiveForNextTx. INDEPENDENT/additive gate,
	// strict >, 0 by default, so every existing chain replays byte-identically.
	// postAppV19Fork is OR'd into the lower postAppV<k>Rules helpers purely for
	// skip-ahead subsumption — it is NOT OR'd into postAppV18Rules, so activating
	// app-v19 never lights up app-v18's admin override. Activated via
	// {Name:"app-v19", TargetAppVersion:19}.
	appV19AppliedHeight int64 // 0 => fork dormant

	// appV20AppliedHeight gates the v11.9 domain-scoped quorum foundation.
	// It is independent/additive, strict >, and dormant on every current chain;
	// the activation block is still evaluated under the prior rules. app-v20
	// subsumes the additive v8..v17/v19 rule chain but deliberately does not
	// enable app-v18's independent administrator override. The v11.9 scope
	// transaction/state branches must key off postAppV20Fork, never this field
	// directly, so pre-activation replay remains byte-identical.
	appV20AppliedHeight int64 // 0 => fork dormant

	// retainBlocks, when > 0, is the number of most-recent blocks Commit asks
	// CometBFT to keep: ResponseCommit.RetainHeight = height - retainBlocks
	// (clamped at 0 = keep everything). Pruning is LOCAL and advisory — it never
	// enters consensus state or the AppHash, so nodes may safely disagree on it.
	// Memory content lives in BadgerDB/SQLite, not in old blocks, so pruning old
	// consensus history is safe; the window only needs to cover crash replay and
	// (in quorum mode) peer catch-up + the evidence max-age. Set via
	// SetRetainBlocks from operator config (issue #40).
	retainBlocks int64

	// expectedGovernanceDomain is OFF-consensus local readiness derived from
	// this process's actual CometBFT chain_id. The upgrade auto-voter refuses an
	// app-v20 proposal whose quorum payload names another chain's domain. Socket
	// mode may learn it after the ABCI listener is already live, so the dedicated
	// mutex makes that one-time runtime binding safe against the voter goroutine.
	expectedGovernanceDomainMu sync.RWMutex
	expectedGovernanceDomain   string

	// pendingAppV20Finalize is consensus-loop owned. It is non-nil only after a
	// successful post-app-v20 FinalizeBlock and before its matching Commit.
	pendingAppV20Finalize *appV20AtomicFinalize

	// appV20MutationFaultHook is an unexported deterministic unit-test seam.
	// Production instances leave it nil. The scoped store invokes it after every
	// successful typed Badger mutation while the outer transaction is uncommitted.
	appV20MutationFaultHook func(int)

	// Finalize-only bootstrap context. A raw target-20 transaction may select
	// crash-atomic storage before activation, but it must not enable any v20
	// admission rule until authenticated ceremony state is committed.
	appV20StrictFinalize       bool
	appV20FinalizeTxCount      int
	appV20FinalizeBudgetOK     bool
	appV20ProposalBootstrapped bool
}

// v8UpgradeName is the canonical name for the v8.0 activation record. The
// v7.5 watchdog names upgrade plans "app-v<TargetAppVersion>" so the lookup
// must match. Centralised here to keep the fork-gate accessors honest.
const v8UpgradeName = "app-v2"

// v8_2UpgradeName is the canonical name for the v8.2 activation record.
// Same naming discipline as v8UpgradeName: "app-v<TargetAppVersion>".
const v8_2UpgradeName = "app-v3"

// v8_3UpgradeName is the canonical name for the v8.3 activation record.
// Same naming discipline: "app-v<TargetAppVersion>".
const v8_3UpgradeName = "app-v4"

// v8_4UpgradeName is the canonical name for the v8.4 activation record.
// Same naming discipline: "app-v<TargetAppVersion>".
const v8_4UpgradeName = "app-v5"

// v8_5UpgradeName is the canonical name for the v8.5 / app-v6 activation
// record. Same naming discipline: "app-v<TargetAppVersion>".
const v8_5UpgradeName = "app-v6"

// appV7UpgradeName is the canonical activation-record name for the
// content-validator (Layer-2 schema gate) fork. Same naming discipline:
// "app-v<TargetAppVersion>". This fork is an INDEPENDENT feature gate and is
// deliberately NOT part of the v8.x PoE monotonic chain.
const appV7UpgradeName = "app-v7"

// appV8UpgradeName is the canonical activation-record name for the app-v8
// fork (UpgradePropose routed through 2/3 governance quorum). Same naming
// discipline: "app-v<TargetAppVersion>". Like app-v7, this is an INDEPENDENT
// feature gate, deliberately NOT part of the v8.x PoE monotonic chain — it
// changes the upgrade-proposal handler, which is orthogonal to the PoE ladder.
const appV8UpgradeName = "app-v8"

// appV9UpgradeName is the canonical activation-record name for the app-v9 fork
// (v9.1: consensus-path nonce enforcement + admin self-grant downgrade). Same
// naming discipline: "app-v<TargetAppVersion>". Like app-v7/app-v8, an
// INDEPENDENT feature gate, deliberately NOT part of the v8.x PoE monotonic
// chain. Governance-only — the watchdog target stays at 6, so app-v9 only
// activates via an explicit governance plan {Name:"app-v9", TargetAppVersion:9}.
const appV9UpgradeName = "app-v9"

// appV10UpgradeName is the canonical activation-record name for the app-v10 fork
// (v9.2: corroboration integrity guard + on-chain author field). Same naming
// discipline. Like app-v7/v8/v9, an INDEPENDENT feature gate, NOT part of the
// v8.x PoE monotonic chain. Governance-only — the watchdog target stays at 6.
const appV10UpgradeName = "app-v10"

// appV11UpgradeName is the canonical activation-record name for the app-v11 fork
// (v10.0: deterministic genesis chain-admin + consensus-path SQL-admin-bootstrap
// disable). Same naming discipline. Like app-v7/v8/v9/v10, an INDEPENDENT feature
// gate, NOT part of the v8.x PoE monotonic chain. Governance-only — the watchdog
// target stays at 6, so app-v11 activates only via an explicit governance plan
// {Name:"app-v11", TargetAppVersion:11}.
const appV11UpgradeName = "app-v11"

// appV12UpgradeName is the canonical activation-record name for the app-v12 fork
// (issue #40: AppHash excludes the volatile state: keys so an idle chain reaches
// a hash fixed point and CometBFT stops forcing empty proof blocks). Same naming
// discipline. Like app-v7..v11, an INDEPENDENT feature gate, NOT part of the
// v8.x PoE monotonic chain. Governance-only — the watchdog target stays at 6, so
// app-v12 activates only via an explicit governance plan {Name:"app-v12",
// TargetAppVersion:12}.
const appV12UpgradeName = "app-v12"

// appV13UpgradeName is the canonical activation-record name for the app-v13
// fork (v10.5.1: the corrected narrow-exclusion AppHash rule superseding
// app-v12's flawed whole-prefix exclusion — see ComputeAppHashExcludingBookkeeping).
// Same naming discipline. Like app-v7..v12, an INDEPENDENT feature gate, NOT
// part of the v8.x PoE monotonic chain. Governance-activated — auto-proposed
// on personal nodes by the v10.5.1 watchdog auto-advance, or manually via
// {Name:"app-v13", TargetAppVersion:13}.
const appV13UpgradeName = "app-v13"

// appV14UpgradeName is the canonical activation-record name for the app-v14
// fork (v10.7.0: the symmetric DEACTIVATION of the app-v7 content-validation
// gate). Same naming discipline. Like app-v7..v13, an INDEPENDENT feature gate,
// NOT part of the v8.x PoE monotonic chain. Governance-activated on quorum
// clusters via {Name:"app-v14", TargetAppVersion:14}; on personal nodes the
// v10.5.1 auto-advance ladder reaches it once the binary supports it — harmless
// there because a stock build wires no content-validator registry, so app-v7's
// gate was always inert and app-v14 merely records its (no-op) deactivation.
const appV14UpgradeName = "app-v14"

// appV15UpgradeName is the canonical activation-record name for the app-v15
// fork (v11: EMPTY next-free scaffolding gate — wires no new behavior yet).
// INDEPENDENT feature gate, NOT part of the v8.x PoE monotonic chain.
// Governance-activated via {Name:"app-v15", TargetAppVersion:15}; the name is
// validated purely as CanonicalUpgradeName(15) == "app-v15" — no allowlist.
// On personal nodes the auto-advance ladder reaches it once the binary supports
// it — harmless because the gate is empty (a governance activation with zero
// behavior/AppHash change).
const appV15UpgradeName = "app-v15"

// appV16UpgradeName is the canonical activation-record name for the app-v16 fork
// (v11.2 domainless-forget remediation). Governance-activated via
// {Name:"app-v16", TargetAppVersion:16}; validated purely as
// CanonicalUpgradeName(16) == "app-v16" — no allowlist.
const appV16UpgradeName = "app-v16"

// appV17UpgradeName is the canonical activation-record name for the app-v17
// fork (v11.5 memory-lifecycle sprint: TxTypeMemoryReinstate + quorum-scaled
// two-phase challenge + stale-vote cleanup rider + delegated agent-proof
// action binding/freshness/single-use enforcement). Governance-activated via
// {Name:"app-v17", TargetAppVersion:17}; validated purely as
// CanonicalUpgradeName(17) == "app-v17" — no allowlist.
const appV17UpgradeName = "app-v17"

// appV18UpgradeName activates the global-admin domain-access override used by
// CEREBRUM for explicit local-agent assignments. Ownership and authorship are
// unchanged; only grant/revoke authorization gains an admin path.
const appV18UpgradeName = "app-v18"

// appV19UpgradeName is the canonical activation-record name for the app-v19
// fork (v11.8: EMPTY next-free scaffolding gate — wires no new consensus
// behavior). Like app-v15, this is an INDEPENDENT feature gate, NOT part of the
// v8.x PoE monotonic chain. Governance-activated via {Name:"app-v19",
// TargetAppVersion:19}; validated purely as CanonicalUpgradeName(19) ==
// "app-v19" — no allowlist. On personal nodes the auto-advance ladder reaches
// it once the binary supports it — harmless because the gate is empty (a
// governance activation with zero consensus behavior/AppHash change). Its only
// effect is off-consensus: the local-agents-default-READ flip in
// web/handler.go, gated on IsAppV19ActiveForNextTx.
const appV19UpgradeName = "app-v19"

// appV20UpgradeName is the canonical activation-record name for the v11.9
// domain-scoped quorum foundation. It is the next independent fork after the
// behavior-empty app-v19 gate and must not reuse app-v18, whose activation has
// a separate live RBAC-administrator effect.
const appV20UpgradeName = "app-v20"

// governanceDelegationDomainStateKey holds the stable, consensus-derived
// domain that post-app-v20 governance authorizations must sign. It is approved
// inside the app-v20 upgrade proposal and materialized exactly at activation,
// so consensus never depends on a per-process CometBFT chain-id setting.
const governanceDelegationDomainStateKey = "governance_delegation_domain_v20"

// appV20LegacyResourceAuditStateKey is written only after an authenticated,
// registered admin's target-20 UpgradePropose has completed the exhaustive
// legacy resource audit. Once present, the atomic predicate keeps every later
// pre-activation block resource-limited, so the audited invariant cannot be
// invalidated by a lexicographically earlier new key.
const appV20LegacyResourceAuditStateKey = "appv20_legacy_resource_audit_complete_v1"

var appV20LegacyResourceAuditValue = []byte("complete-v1")

func isCanonicalAppV20CeremonyDomain(encoded string) bool {
	domain, err := hex.DecodeString(encoded)
	return err == nil && len(domain) == sha256.Size && hex.EncodeToString(domain) == encoded
}

// isAppV20CeremonyUpgrade distinguishes the v11.9 activation ceremony from a
// legacy proposal that merely used the previously-unreserved numeric target
// 20. v11.8 encoded no GovernanceDomain and accepted arbitrary future target
// versions into upgrade governance. Only the exact canonical triple is the
// v11.9 ceremony tag. Empty, malformed, non-canonical, differently named, and
// otherwise trailing-domain target-20 payloads remain on the historical path
// forever so genesis replay cannot reclassify a previously valid transaction.
func isAppV20CeremonyUpgrade(name string, targetAppVersion uint64, governanceDomain string) bool {
	return name == appV20UpgradeName &&
		targetAppVersion == 20 &&
		isCanonicalAppV20CeremonyDomain(governanceDomain)
}

func isAppV20CeremonyProposal(proposal *governance.ProposalState) bool {
	if proposal == nil || proposal.Operation != governance.OpUpgrade {
		return false
	}
	var payload UpgradeProposalPayload
	if err := json.Unmarshal(proposal.Payload, &payload); err != nil {
		return false
	}
	return proposal.TargetID == appV20UpgradeName &&
		isAppV20CeremonyUpgrade(payload.Name, payload.TargetAppVersion, payload.GovernanceDomain)
}

func isAppV20CeremonyPlan(plan *store.UpgradePlanRecord) bool {
	return plan != nil && isAppV20CeremonyUpgrade(plan.Name, plan.TargetAppVersion, plan.GovernanceDomain)
}

// postV8Fork is the consensus-side fork-gate predicate. Use it inside
// processTx and other height-aware paths. Strict greater-than mirrors
// CometBFT's "applied at H+1" semantic — the fork takes effect on the
// block immediately following the activation block.
func (app *SageApp) postV8Fork(height int64) bool {
	return app.v8AppliedHeight > 0 && height > app.v8AppliedHeight
}

// IsPostV8Fork is the off-consensus accessor used by REST handlers and
// other callers that don't carry a deterministic block height. It reads
// the cached AppState.Height — sufficient for advisory access checks
// outside the consensus pipeline.
func (app *SageApp) IsPostV8Fork() bool {
	return app.v8AppliedHeight > 0 && app.state != nil && app.state.Height > app.v8AppliedHeight
}

// refreshV8Fork populates v8AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain
// picks up the gate without waiting for activation) and after the
// activation block in FinalizeBlock.
func (app *SageApp) refreshV8Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(v8UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", v8UpgradeName).Msg("read v8 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.v8AppliedHeight = rec.AppliedHeight
}

// recordV8Branch increments the sage_fork_branch_total counter so
// operators can confirm a chain has flipped post-fork on the same
// dashboard that tracks tx volume. Called from every fork-gated
// consensus handler (HasAccessMultiOrg's consensus callers, the
// processAccessGrant gate, and processDomainReassign).
func recordV8Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("v8", branch).Inc()
}

// postV8_2Fork is the consensus-side fork-gate predicate for the v8.2
// PoE-weighted quorum activation. Strict greater-than mirrors postV8Fork
// (and CometBFT's "applied at H+1" semantic): the activation block H_act
// itself still runs the pre-fork branch so the only AppHash delta at
// H_act is the MarkUpgradeApplied write. Quorum decisions on H > H_act
// consult the persisted poew:<id> weights (with bootstrap fallback for
// validators whose entry is missing).
func (app *SageApp) postV8_2Fork(height int64) bool {
	return app.v8_2AppliedHeight > 0 && height > app.v8_2AppliedHeight
}

// refreshV8_2Fork populates v8_2AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain
// picks up the gate without waiting for activation) and after the
// activation block in FinalizeBlock.
func (app *SageApp) refreshV8_2Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(v8_2UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", v8_2UpgradeName).Msg("read v8.2 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.v8_2AppliedHeight = rec.AppliedHeight
}

// recordV8_2Branch is the v8.2 sibling of recordV8Branch. Same metric
// name (sage_fork_branch_total) with fork="v8.2" so dashboards can plot
// the two activations side by side.
func recordV8_2Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("v8.2", branch).Inc()
}

// postV8_3Fork is the consensus-side fork-gate predicate for the v8.3
// PoE-signal activation (verdict-correctness EWMA accuracy + real
// corroboration count + 56-byte vstats: records). Strict greater-than
// mirrors postV8_2Fork: the activation block H_act itself still runs the
// pre-fork branch (Phase-1 accuracy/corroboration, 24-byte vstats writes,
// no verdict-match crediting) so the only AppHash delta at H_act is the
// MarkUpgradeApplied write. Blocks H > H_act feed the real signals.
func (app *SageApp) postV8_3Fork(height int64) bool {
	return app.v8_3AppliedHeight > 0 && height > app.v8_3AppliedHeight
}

// refreshV8_3Fork populates v8_3AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain
// picks up the gate without waiting for activation) and after the
// activation block in FinalizeBlock.
func (app *SageApp) refreshV8_3Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(v8_3UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", v8_3UpgradeName).Msg("read v8.3 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.v8_3AppliedHeight = rec.AppliedHeight
}

// recordV8_3Branch is the v8.3 sibling of recordV8_2Branch. Same metric
// name (sage_fork_branch_total) with fork="v8.3" so dashboards can plot
// all three activations side by side.
func recordV8_3Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("v8.3", branch).Inc()
}

// postV8_4Fork is the consensus-side fork-gate predicate for the v8.4
// Domain-factor activation (memdomain:<id> writes at submit, per-domain
// verdict-correctness EWMA in vstats_domain:<v>:<D>, and domain-conditional
// quorum weight). Strict greater-than mirrors postV8_3Fork: the activation
// block H_act itself still runs the pre-fork branch (no memdomain: write, no
// per-domain crediting, neutral-domain quorum) so the only AppHash delta at
// H_act is the MarkUpgradeApplied write. Blocks H > H_act enable the domain
// signal.
func (app *SageApp) postV8_4Fork(height int64) bool {
	return app.v8_4AppliedHeight > 0 && height > app.v8_4AppliedHeight
}

// refreshV8_4Fork populates v8_4AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain
// picks up the gate without waiting for activation) and after the
// activation block in FinalizeBlock.
func (app *SageApp) refreshV8_4Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(v8_4UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", v8_4UpgradeName).Msg("read v8.4 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.v8_4AppliedHeight = rec.AppliedHeight
}

// recordV8_4Branch is the v8.4 sibling of recordV8_3Branch. Same metric
// name (sage_fork_branch_total) with fork="v8.4" so dashboards can plot
// all four activations side by side.
func recordV8_4Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("v8.4", branch).Inc()
}

// postV8_5Fork is the consensus-side fork-gate predicate for the v8.5 / app-v6
// upgrade-machinery hardening (processUpgradePropose canonical-name + version-
// regression guards, processUpgradeRevert rejection). Strict greater-than
// mirrors postV8_4Fork: the activation block H_act itself still runs the
// pre-fork branch (lenient propose, no-op revert stub) so the only AppHash
// delta at H_act is the MarkUpgradeApplied write. Blocks H > H_act enforce the
// guards.
func (app *SageApp) postV8_5Fork(height int64) bool {
	return app.v8_5AppliedHeight > 0 && height > app.v8_5AppliedHeight
}

// refreshV8_5Fork populates v8_5AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain
// picks up the gate without waiting for activation) and after the
// activation block in FinalizeBlock.
func (app *SageApp) refreshV8_5Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(v8_5UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", v8_5UpgradeName).Msg("read v8.5 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.v8_5AppliedHeight = rec.AppliedHeight
}

// postAppV7Fork is the consensus-side fork-gate predicate for the
// content-validator (Layer-2 schema gate) activation. Strict greater-than
// mirrors postV8_5Fork: the activation block H_act itself still runs the
// pre-fork branch (gate dormant) so the only AppHash delta at H_act is the
// MarkUpgradeApplied write. The gate only ever runs when this returns true
// AND contentValidators != nil.
//
// app-v14 adds the symmetric DEACTIVATION boundary: once app-v14 is active the
// gate turns OFF again for heights past the app-v14 activation height, so the
// gate is live for exactly the window (appV7AppliedHeight, appV14AppliedHeight].
func (app *SageApp) postAppV7Fork(height int64) bool {
	if app.appV7AppliedHeight == 0 || height <= app.appV7AppliedHeight {
		return false // pre-app-v7, or the activation block itself: gate dormant.
	}
	// Content-validator gate DEACTIVATION (app-v14). Strict > mirrors the
	// activation boundary above: the deactivation block H2 itself still runs
	// gated (height <= H2) so its only AppHash delta is the MarkUpgradeApplied
	// write, and enforcement stops at H2+1. Inert on every chain that has not
	// activated app-v14 (appV14AppliedHeight == 0), so all existing history
	// replays byte-identically. Replay-safe by construction: the live window is a
	// pure function of two committed activation heights, re-derived identically
	// on every replica from the upgrade audit trail.
	if app.appV14AppliedHeight > 0 && height > app.appV14AppliedHeight {
		return false
	}
	return true
}

// refreshAppV7Fork populates appV7AppliedHeight from the persisted
// upgrade audit trail. Called on boot (so a node restarting on a post-fork
// chain picks up the gate without waiting for activation) and after the
// activation block in FinalizeBlock.
func (app *SageApp) refreshAppV7Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV7UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV7UpgradeName).Msg("read app-v7 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV7AppliedHeight = rec.AppliedHeight
}

// postAppV8Fork is the consensus-side fork-gate predicate for the app-v8
// activation (UpgradePropose routed through 2/3 governance quorum). Strict
// greater-than mirrors postAppV7Fork: the activation block H_act itself still
// runs the pre-fork branch (gate dormant), so the propose that SEEDED app-v8
// — which ran at some height < H_act while this returned false — self-activates
// via the old path, avoiding any chicken-and-egg. Post-activation, every later
// UpgradePropose routes through governance.
func (app *SageApp) postAppV8Fork(height int64) bool {
	return app.appV8AppliedHeight > 0 && height > app.appV8AppliedHeight
}

// refreshAppV8Fork populates appV8AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain picks
// up the gate without waiting for activation) and after the activation block in
// FinalizeBlock. Returns nil-record on every existing chain (no "app-v8" record
// has ever been written), so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV8Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV8UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV8UpgradeName).Msg("read app-v8 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV8AppliedHeight = rec.AppliedHeight
}

// recordAppV8Branch records which branch (pre/post app-v8) processUpgradePropose
// took, as a Prometheus counter for the fork-activation dashboard. Metrics do
// NOT enter the AppHash (ComputeAppHash reads BadgerDB only), so this is purely
// observational and never affects replay.
func recordAppV8Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("app-v8", branch).Inc()
}

// postAppV9Fork is the consensus-side fork-gate predicate for the app-v9
// activation (v9.1: consensus-path nonce enforcement + admin self-grant
// downgrade). Strict greater-than mirrors postAppV8Fork: the activation block
// itself still runs the pre-fork branches, and every existing chain (none has
// activated app-v9) returns false, so historical blocks replay byte-identically.
func (app *SageApp) postAppV9Fork(height int64) bool {
	return app.appV9AppliedHeight > 0 && height > app.appV9AppliedHeight
}

// postAppV10Fork is the consensus-side fork-gate predicate for the app-v10
// activation (v9.2: corroboration integrity guard + on-chain author field).
// Strict greater-than mirrors the other gates; every existing chain (none has
// activated app-v10) returns false, so historical blocks replay byte-identically.
func (app *SageApp) postAppV10Fork(height int64) bool {
	return app.appV10AppliedHeight > 0 && height > app.appV10AppliedHeight
}

// postAppV11Fork is the consensus-side fork-gate predicate for the app-v11
// activation (v10.0: deterministic genesis chain-admin + SQL-admin-bootstrap
// disable). Strict greater-than mirrors the other gates; every existing chain
// (none has activated app-v11) returns false, so historical blocks replay
// byte-identically.
func (app *SageApp) postAppV11Fork(height int64) bool {
	return app.appV11AppliedHeight > 0 && height > app.appV11AppliedHeight
}

// postAppV12Fork is the consensus-side fork-gate predicate for the app-v12
// activation (issue #40: state:-key exclusion from the AppHash). Strict
// greater-than mirrors the other gates — the activation block H_act itself is
// still hashed under the pre-fork rule, so the MarkUpgradeApplied write at
// H_act lands in a hash every pre-upgrade replica can reproduce; the new rule
// takes effect at H_act+1. Every existing chain (none has activated app-v12)
// returns false, so historical blocks replay byte-identically.
func (app *SageApp) postAppV12Fork(height int64) bool {
	return app.appV12AppliedHeight > 0 && height > app.appV12AppliedHeight
}

// postAppV13Fork is the consensus-side fork-gate predicate for the app-v13
// activation (v10.5.1: the corrected narrow-exclusion AppHash rule). Strict
// greater-than mirrors the other gates — the activation block H_act itself is
// still hashed under whatever rule was previously in force (v12-broad on an
// upgraded chain, legacy on a skip-ahead chain), so every pre-upgrade replica
// reproduces it; the narrow rule takes effect at H_act+1.
func (app *SageApp) postAppV13Fork(height int64) bool {
	return app.appV13AppliedHeight > 0 && height > app.appV13AppliedHeight
}

// postAppV15Fork is the consensus-side fork-gate predicate for the app-v15
// activation (v11: EMPTY next-free scaffolding gate). Strict greater-than mirrors
// the other additive gates — the activation block itself is NOT past-fork, so it
// commits under the pre-fork rules and replay is boundary-safe. app-v15 wires no
// AppHash rule and no callsite keys off this directly today; it is OR'd into
// postAppV8Rules..postAppV11Rules purely for skip-ahead subsumption safety. Every
// existing chain (none has activated app-v15) returns false, so historical blocks
// replay byte-identically. (Template: postAppV13Fork — app-v14 has no predicate,
// it is a deactivation embedded in postAppV7Fork.)
func (app *SageApp) postAppV15Fork(height int64) bool {
	return app.appV15AppliedHeight > 0 && height > app.appV15AppliedHeight
}

// postAppV16Fork is the consensus-side fork-gate predicate for app-v16 (v11.2:
// domainless-forget remediation — OpMemoryDomainRepair backfill + hardened
// deprecation gate). Strict greater-than mirrors the other additive gates so the
// activation block itself commits under pre-fork rules and replay is boundary-safe.
// Every existing chain (none has activated app-v16) returns false, so historical
// blocks replay byte-identically. (Template: postAppV15Fork.)
func (app *SageApp) postAppV16Fork(height int64) bool {
	return app.appV16AppliedHeight > 0 && height > app.appV16AppliedHeight
}

// postAppV17Fork is the consensus-side fork-gate predicate for app-v17 (v11.5:
// TxTypeMemoryReinstate + quorum-scaled two-phase challenge + stale-vote
// cleanup rider + delegated-proof hardening). Strict greater-than mirrors the
// activation block itself commits under pre-fork rules and replay is
// boundary-safe. Every existing chain (none has activated app-v17) returns
// false, so historical blocks replay byte-identically. (Template: postAppV16Fork.)
func (app *SageApp) postAppV17Fork(height int64) bool {
	return app.appV17AppliedHeight > 0 && height > app.appV17AppliedHeight
}

// postAppV18Fork is the consensus-side boundary for the administrator RBAC
// override. The activation block retains v17 behavior; v18 starts at H+1.
func (app *SageApp) postAppV18Fork(height int64) bool {
	return app.appV18AppliedHeight > 0 && height > app.appV18AppliedHeight
}

// postAppV19Fork is the consensus-side fork-gate predicate for app-v19 (v11.8:
// EMPTY next-free scaffolding gate). Strict greater-than mirrors the other
// additive gates so the activation block itself commits under pre-fork rules and
// replay is boundary-safe. app-v19 wires no AppHash rule and no processTx branch
// keys off this directly; it is OR'd into the lower postAppV<k>Rules helpers
// purely for skip-ahead subsumption safety (but NOT into postAppV18Rules, so it
// never enables app-v18's admin override). Every existing chain (none has
// activated app-v19) returns false, so historical blocks replay byte-identically.
// (Template: postAppV15Fork — the empty scaffolding gate, NOT postAppV18Fork.)
func (app *SageApp) postAppV19Fork(height int64) bool {
	return app.appV19AppliedHeight > 0 && height > app.appV19AppliedHeight
}

// postAppV20Fork is the consensus-side boundary for the v11.9 scoped-quorum
// rules. It is strict so the applied-upgrade record is committed under the
// previous rule set and all replicas begin the new behavior at H+1.
func (app *SageApp) postAppV20Fork(height int64) bool {
	return app.appV20AppliedHeight > 0 && height > app.appV20AppliedHeight
}

// postAppV17Rules reports whether app-v17's consensus rules are in force at
// this height. app-v18 subsumes those additive rules on a skip-ahead chain;
// historical blocks still collapse to exactly their original gate.
//
// (TxTypeMemoryReinstate + quorum-scaled challenge + delegated-proof
// hardening) in this same sprint.
//
//nolint:unused // C1 mints the empty gate; the first callsites land with C2/C3
func (app *SageApp) postAppV17Rules(height int64) bool {
	return app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

func (app *SageApp) postAppV18Rules(height int64) bool {
	return app.postAppV18Fork(height)
}

// postAppV19Rules reports whether app-v19's consensus rules are in force at this
// height. app-v19 is behavior-EMPTY on the consensus path (like app-v15's empty
// gate), so this is exactly postAppV19Fork today; it exists as a named helper so
// the NEXT independent fork can OR itself in here without touching every callsite,
// mirroring postAppV15Rules/postAppV18Rules. NOTE: app-v19 is deliberately NOT
// OR'd into postAppV18Rules — activating app-v19 must never enable app-v18's live
// admin RBAC override; the subsumption chain runs v19 -> v8..v17/v15/v16 only.
//
//nolint:unused // behavior-empty gate; no consensus callsite reads it (D adds zero processTx branch)
func (app *SageApp) postAppV19Rules(height int64) bool {
	return app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// IsAppV17ActiveForNextTx is the REST-side transaction-construction accessor.
// After FinalizeBlock(H_activation), state.Height and appV17AppliedHeight both
// equal H_activation; a transaction broadcast then can first execute at H+1,
// where the strict consensus gate is active. Evaluate the canonical rule
// predicate at that next executable height rather than duplicating its fork
// list here: skip-ahead activations (including app-v19 without app-v17/v18)
// must construct exactly the envelope FinalizeBlock will require.
func (app *SageApp) IsAppV17ActiveForNextTx() bool {
	if app.state == nil {
		return false
	}
	return app.postAppV17Rules(app.state.Height + 1)
}

// IsAppV18ActiveForNextTx is the dashboard-side readiness accessor for an
// explicit administrator access override broadcast after the activation block.
func (app *SageApp) IsAppV18ActiveForNextTx() bool {
	return app.appV18AppliedHeight > 0 && app.state != nil && app.state.Height >= app.appV18AppliedHeight
}

// IsAppV19ActiveForNextTx is the off-consensus readiness accessor for the
// app-v19 fork. After FinalizeBlock(H_activation), state.Height and
// appV19AppliedHeight both equal H_activation, so this reports true immediately
// at the activation block (>=), whereas the strict consensus gate postAppV19Fork
// (>) only turns true at H+1. app-v19 has no consensus behavior, so the only
// consumer is web/handler.go's local-agents-default-READ flip (DashboardHandler
// .AppV19ActiveFn).
func (app *SageApp) IsAppV19ActiveForNextTx() bool {
	return app.state != nil && app.postAppV19Rules(app.state.Height+1)
}

// IsAppV20ActiveForNextTx is the construction-side gate for v11.9 scope
// governance. It flips immediately after the activation block commits, so REST,
// MCP, and CEREBRUM do not submit op==8 while consensus still treats it as the
// historical inert unknown operation.
func (app *SageApp) IsAppV20ActiveForNextTx() bool {
	return app.state != nil && app.postAppV20Fork(app.state.Height+1)
}

// postAppV16Rules reports whether app-v16's consensus rules are in force at this
// height. Same subsumption logic as the gates below it: app-v16's rules are
// active whenever app-v16 OR any higher independent gate (app-v17) is, so a
// skip-ahead-to-17 chain still enforces the domainless-forget remediation.
// Collapses to exactly postAppV16Fork on every existing chain
// (appV17AppliedHeight==0), so historical blocks replay byte-identically.
func (app *SageApp) postAppV16Rules(height int64) bool {
	return app.postAppV16Fork(height) || app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// shouldRecordMemoryDomain reports whether a successful submit must persist its
// memdomain:<id> record. v8.4 introduced that record for domain-weighted PoE,
// while app-v16 independently made a non-empty domain mandatory so newly
// submitted memories can always pass the challenge/forget authorization gate.
// app-v17 subsumes app-v16, including on a skip-ahead chain whose v8.4 gate is
// intentionally still dormant. Keeping this as a narrow write predicate avoids
// activating v8.4's unrelated weighting and terminal-resubmit rules.
func (app *SageApp) shouldRecordMemoryDomain(height int64) bool {
	return app.postV8_4Fork(height) || app.postAppV16Rules(height)
}

// postAppV8Rules reports whether app-v8's consensus rules (consensus-path
// signature verification + quorum/admin-gated upgrade governance) are in force
// at this height. app-v7/v8/v9/v10 are INDEPENDENT gates — governance MAY
// skip-activate a higher one without the lower, because the upgrade-propose
// regression guard only checks target > currentAppVersion(), and
// reconcilePoEForkMonotonicity excludes these gates. But a HIGHER app version
// SUBSUMES the lower's rules: a chain that reports version 10 must still enforce
// app-v8's quorum-gated upgrades and sig-verification. So app-v8's rules are
// active whenever app-v8 OR any higher independent gate (app-v9, app-v10) is.
// Every callsite that gates an app-v8 rule MUST use this, not postAppV8Fork alone
// — otherwise a skip-ahead chain silently drops the rule while advertising the
// higher version (the skip-ahead class of bug). On every existing chain the
// higher gates are 0, so this collapses to exactly postAppV8Fork and historical
// blocks replay byte-identically.
func (app *SageApp) postAppV8Rules(height int64) bool {
	return app.postAppV8Fork(height) || app.postAppV9Fork(height) || app.postAppV10Fork(height) || app.postAppV11Fork(height) || app.postAppV12Fork(height) || app.postAppV13Fork(height) || app.postAppV15Fork(height) || app.postAppV16Fork(height) || app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// postAppV9Rules reports whether app-v9's consensus rules (consensus-path
// nonce/replay enforcement + admin self-grant downgrade) are in force at this
// height. Same subsumption logic as postAppV8Rules: app-v9's rules are active
// whenever app-v9 OR any higher independent gate (app-v10, app-v11) is, so an
// app-v10/v11-without-app-v9 chain still enforces them. Collapses to exactly
// postAppV9Fork on every existing chain (appV10/appV11AppliedHeight==0), so replay
// is byte-identical.
func (app *SageApp) postAppV9Rules(height int64) bool {
	return app.postAppV9Fork(height) || app.postAppV10Fork(height) || app.postAppV11Fork(height) || app.postAppV12Fork(height) || app.postAppV13Fork(height) || app.postAppV15Fork(height) || app.postAppV16Fork(height) || app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// postAppV10Rules reports whether app-v10's consensus rules (corroboration
// integrity guard + on-chain memory author) are in force at this height. Same
// subsumption logic: app-v10's rules are active whenever app-v10 OR any higher
// independent gate (app-v11) is, so an app-v11-without-app-v10 chain still
// enforces them. Collapses to exactly postAppV10Fork on every existing chain
// (appV11AppliedHeight==0), so historical blocks replay byte-identically. Added
// when app-v11 landed — app-v10 was the highest fork until then and needed no
// subsumption helper.
func (app *SageApp) postAppV10Rules(height int64) bool {
	return app.postAppV10Fork(height) || app.postAppV11Fork(height) || app.postAppV12Fork(height) || app.postAppV13Fork(height) || app.postAppV15Fork(height) || app.postAppV16Fork(height) || app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// postAppV11Rules reports whether app-v11's consensus rules (the per-node
// SQL-admin-bootstrap disable, #36) are in force at this height. Same
// subsumption logic as the gates below it: app-v11's rules are active whenever
// app-v11 OR any higher independent gate (app-v12) is, so an
// app-v12-without-app-v11 chain still enforces them. Collapses to exactly
// postAppV11Fork on every existing chain (appV12AppliedHeight==0), so
// historical blocks replay byte-identically.
func (app *SageApp) postAppV11Rules(height int64) bool {
	return app.postAppV11Fork(height) || app.postAppV12Fork(height) || app.postAppV13Fork(height) || app.postAppV15Fork(height) || app.postAppV16Fork(height) || app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// postAppV12Rules reports whether app-v12's consensus rule (the FLAWED
// whole-prefix state: AppHash exclusion, issue #40) is in force at this
// height. DELIBERATELY NOT subsumed by app-v13: the hash rules are mutually
// exclusive REPLACEMENTS, not additive rules — when app-v13 is active the
// narrow rule supersedes this one. FinalizeBlock checks postAppV13Rules
// FIRST, so this helper only ever selects the broad rule on a chain (or a
// replayed height range) where v12 activated but v13 has not yet — exactly
// the v12-era blocks that must replay byte-identically.
func (app *SageApp) postAppV12Rules(height int64) bool {
	return app.postAppV12Fork(height)
}

// postAppV13Rules reports whether app-v13's consensus rule (the corrected
// narrow bookkeeping-key AppHash exclusion) is in force at this height.
// app-v13 is the highest independent gate today, so for now this is exactly
// postAppV13Fork; it exists as a named helper so the NEXT fork can OR itself
// in here without touching every callsite. NOTE for the next AppHash-rule
// fork: hash rules REPLACE each other — mirror the postAppV12Rules precedence
// pattern (check the newest rule first in FinalizeBlock) rather than ORing
// into this helper blindly.
func (app *SageApp) postAppV13Rules(height int64) bool {
	return app.postAppV13Fork(height)
}

// postAppV15Rules reports whether app-v15's consensus rules (the verb-ladder
// deprecation-authz gate on processMemoryChallenge + the level-3 grant-cap
// loosening) are in force at this height. These are ADDITIVE rules (like
// app-v10/v11), so each NEXT independent fork ORs itself in here at one site
// instead of touching every callsite: app-v15's rules are active whenever
// app-v15 OR any higher independent gate (app-v16, app-v17) is. Collapses to
// exactly postAppV15Fork on every chain where the higher gates are unset, so
// historical blocks replay byte-identically. Do NOT OR this into
// postAppV12Rules/postAppV13Rules — those are mutually-exclusive
// AppHash-REPLACEMENT rules, deliberately non-subsumed.
func (app *SageApp) postAppV15Rules(height int64) bool {
	return app.postAppV15Fork(height) || app.postAppV16Fork(height) || app.postAppV17Fork(height) || app.postAppV18Fork(height) || app.postAppV19Fork(height) || app.postAppV20Fork(height)
}

// refreshAppV9Fork populates appV9AppliedHeight from the persisted upgrade
// audit trail. Called on boot (so a node restarting on a post-fork chain picks
// up the gate without waiting for activation) and after the activation block in
// FinalizeBlock. Returns nil-record on every existing chain (no "app-v9" record
// has ever been written), so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV9Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV9UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV9UpgradeName).Msg("read app-v9 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV9AppliedHeight = rec.AppliedHeight
}

// refreshAppV10Fork populates appV10AppliedHeight from the persisted upgrade
// audit trail. Called on boot and after the activation block in FinalizeBlock.
// Returns nil-record on every existing chain (no "app-v10" record has ever been
// written), so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV10Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV10UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV10UpgradeName).Msg("read app-v10 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV10AppliedHeight = rec.AppliedHeight
}

// refreshAppV11Fork populates appV11AppliedHeight from the persisted upgrade
// audit trail. Called on boot and after the activation block in FinalizeBlock.
// Returns nil-record on every existing chain (no "app-v11" record has ever been
// written), so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV11Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV11UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV11UpgradeName).Msg("read app-v11 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV11AppliedHeight = rec.AppliedHeight
}

// refreshAppV12Fork populates appV12AppliedHeight from the persisted upgrade
// audit trail. Called on boot and after the activation block in FinalizeBlock.
// Returns nil-record on every existing chain (no "app-v12" record has ever been
// written), so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV12Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV12UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV12UpgradeName).Msg("read app-v12 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV12AppliedHeight = rec.AppliedHeight
}

// appliedUpgradeTargetAtHeight reports the TargetAppVersion of an upgrade
// whose audit record says it activated at exactly this height, if any. Used by
// FinalizeBlock to re-emit the version.app ConsensusParamUpdates when a
// crashed node replays an activation block whose plan MarkUpgradeApplied
// already deleted (the plan delete + audit write are durable in BadgerDB
// independent of ABCI Commit). Scans the canonical activation-record names
// (app-v2..app-v<max>) — the audit trail is keyed by name, and every
// gate-effective activation uses a canonical name (non-canonical legacy names
// never flipped a gate, so they have nothing to re-emit). Reads committed
// consensus state only — deterministic across replicas.
func (app *SageApp) appliedUpgradeTargetAtHeight(height int64) (uint64, bool) {
	for v := uint64(2); v <= maxSupportedAppVersion; v++ {
		rec, err := app.badgerStore.GetAppliedUpgrade(tx.CanonicalUpgradeName(v))
		if err != nil || rec == nil {
			continue
		}
		if rec.AppliedHeight == height {
			return rec.TargetAppVersion, true
		}
	}
	return 0, false
}

// refreshAppV13Fork populates appV13AppliedHeight from the persisted upgrade
// audit trail. Called on boot and after the activation block in FinalizeBlock.
// Returns nil-record on every chain that has not activated app-v13, so the
// gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV13Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV13UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV13UpgradeName).Msg("read app-v13 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV13AppliedHeight = rec.AppliedHeight
}

// refreshAppV14Fork populates appV14AppliedHeight from the persisted upgrade
// audit trail. Called on boot and after the activation block in FinalizeBlock,
// so a node restarting on a post-app-v14 chain re-derives the content-gate
// DEACTIVATION height and stops enforcing past it without waiting for the
// activation. Returns nil-record on every chain that has not activated app-v14,
// so the deactivation stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV14Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV14UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV14UpgradeName).Msg("read app-v14 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV14AppliedHeight = rec.AppliedHeight
}

// refreshAppV15Fork populates appV15AppliedHeight from the persisted upgrade
// audit trail. Called from both constructors on boot so a node restarting on a
// post-app-v15 chain re-derives the activation height. Returns nil-record on
// every chain that has not activated app-v15, so the gate stays dormant and
// replay is unaffected. (FinalizeBlock sets the field inline at the activation
// block; this refresh runs only on construction.)
func (app *SageApp) refreshAppV15Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV15UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV15UpgradeName).Msg("read app-v15 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV15AppliedHeight = rec.AppliedHeight
}

// refreshAppV16Fork populates appV16AppliedHeight from the persisted upgrade
// record on construction. Returns a nil-record on every chain that has not
// activated app-v16, so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV16Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV16UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV16UpgradeName).Msg("read app-v16 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV16AppliedHeight = rec.AppliedHeight
}

// refreshAppV17Fork populates appV17AppliedHeight from the persisted upgrade
// record on construction. Returns a nil-record on every chain that has not
// activated app-v17, so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV17Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV17UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV17UpgradeName).Msg("read app-v17 applied-upgrade record")
		return
	}
	if rec == nil {
		return
	}
	app.appV17AppliedHeight = rec.AppliedHeight
}

func (app *SageApp) refreshAppV18Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV18UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV18UpgradeName).Msg("read app-v18 applied-upgrade record")
		return
	}
	if rec != nil {
		app.appV18AppliedHeight = rec.AppliedHeight
	}
}

// refreshAppV19Fork populates appV19AppliedHeight from the persisted upgrade
// record on construction. Returns a nil-record on every chain that has not
// activated app-v19, so the gate stays dormant and replay is unaffected.
func (app *SageApp) refreshAppV19Fork() {
	rec, err := app.badgerStore.GetAppliedUpgrade(appV19UpgradeName)
	if err != nil {
		app.logger.Warn().Err(err).Str("name", appV19UpgradeName).Msg("read app-v19 applied-upgrade record")
		return
	}
	if rec != nil {
		app.appV19AppliedHeight = rec.AppliedHeight
	}
}

// validateAppliedAppV20State restores the v11.9 gate and enforces the persisted
// activation invariant during construction. A missing record leaves the gate
// dormant for pre-v11.9 chains. A structurally valid applied target-20 record
// without the separate domain key is a legacy unsupported-future activation,
// not a v11.9 ceremony: v11.8 could create that record but had no domain field.
// It therefore remains dormant (and retains the historical version-handshake
// failure) instead of retroactively enabling app-v20. Once the domain key is
// present, it must be the immutable canonical 32-byte value and the proposal's
// resource-audit marker must still be retained, or startup fails. The two-key
// tag prevents a stray domain key from reclassifying a legacy applied record.
// The cached height is assigned only after every ceremony check succeeds.
func (app *SageApp) validateAppliedAppV20State() error {
	app.appV20AppliedHeight = 0
	rec, err := app.badgerStore.GetAppliedUpgrade(appV20UpgradeName)
	if err != nil {
		return fmt.Errorf("read applied %s record: %w", appV20UpgradeName, err)
	}
	if rec == nil {
		return nil
	}
	if rec.Name != appV20UpgradeName {
		return fmt.Errorf("applied %s record has name %q", appV20UpgradeName, rec.Name)
	}
	if rec.TargetAppVersion != 20 {
		return fmt.Errorf(
			"applied %s record has target app version %d, want 20",
			appV20UpgradeName,
			rec.TargetAppVersion,
		)
	}
	if rec.AppliedHeight <= 0 {
		return fmt.Errorf(
			"applied %s record has non-positive height %d",
			appV20UpgradeName,
			rec.AppliedHeight,
		)
	}
	if app.state == nil {
		return fmt.Errorf("applied %s record cannot be checked without app state", appV20UpgradeName)
	}
	// MarkUpgradeApplied runs during FinalizeBlock before Commit persists the
	// new AppState height, so exactly state.Height+1 is the legitimate crash
	// window. Anything farther ahead cannot have been produced by this state
	// machine and must not silently defer the fork gate.
	if app.state.Height < rec.AppliedHeight-1 {
		return fmt.Errorf(
			"applied %s height %d is ahead of persisted app height %d",
			appV20UpgradeName,
			rec.AppliedHeight,
			app.state.Height,
		)
	}
	domain, err := app.badgerStore.GetState(governanceDelegationDomainStateKey)
	if err != nil {
		return fmt.Errorf(
			"applied %s at height %d cannot read governance delegation domain: %w",
			appV20UpgradeName,
			rec.AppliedHeight,
			err,
		)
	}
	if len(domain) == 0 {
		// Old binaries could apply an unsupported target-20 plan, but they could
		// not write either v11.9 ceremony tag. Preserve only that exact legacy
		// state; a retained marker without its domain is a partial ceremony.
		auditComplete, auditErr := app.appV20LegacyResourceAuditComplete()
		if auditErr != nil {
			return fmt.Errorf(
				"applied %s at height %d has invalid retained ceremony marker: %w",
				appV20UpgradeName,
				rec.AppliedHeight,
				auditErr,
			)
		}
		if auditComplete {
			return fmt.Errorf(
				"applied %s at height %d retains its ceremony marker but has no governance delegation domain",
				appV20UpgradeName,
				rec.AppliedHeight,
			)
		}
		return nil
	}
	if len(domain) != sha256.Size {
		return fmt.Errorf(
			"applied %s at height %d has invalid governance delegation domain: governance delegation domain has length %d, want %d",
			appV20UpgradeName,
			rec.AppliedHeight,
			len(domain),
			sha256.Size,
		)
	}
	auditComplete, auditErr := app.appV20LegacyResourceAuditComplete()
	if auditErr != nil {
		return fmt.Errorf(
			"applied %s at height %d has invalid retained ceremony marker: %w",
			appV20UpgradeName,
			rec.AppliedHeight,
			auditErr,
		)
	}
	if !auditComplete {
		return fmt.Errorf(
			"applied %s at height %d is missing its retained legacy resource audit marker",
			appV20UpgradeName,
			rec.AppliedHeight,
		)
	}
	app.appV20AppliedHeight = rec.AppliedHeight
	return nil
}

// GovernanceDelegationDomain returns the lowercase hex chain domain that
// clients bind into every delegated post-app-v20 governance request. Empty
// means app-v20 has not materialized the domain (or the store is unreadable),
// in which case the REST/dashboard governance gateway must fail closed.
func (app *SageApp) GovernanceDelegationDomain() string {
	domain, err := app.governanceDelegationDomain()
	if err != nil {
		return ""
	}
	return hex.EncodeToString(domain)
}

func (app *SageApp) governanceDelegationDomain() ([]byte, error) {
	domain, err := app.badgerStore.GetState(governanceDelegationDomainStateKey)
	if err != nil {
		return nil, fmt.Errorf("load governance delegation domain: %w", err)
	}
	if len(domain) != sha256.Size {
		return nil, fmt.Errorf("governance delegation domain has length %d, want %d", len(domain), sha256.Size)
	}
	return domain, nil
}

// ensureGovernanceDelegationDomain performs the one-time activation write. It
// is idempotent so replay after a crash between FinalizeBlock and Commit reads
// the already materialized value instead of deriving a second one.
func (app *SageApp) ensureGovernanceDelegationDomain(encoded string) error {
	domain, err := hex.DecodeString(encoded)
	if err != nil || len(domain) != sha256.Size || hex.EncodeToString(domain) != encoded {
		return fmt.Errorf("approved governance delegation domain must be canonical 32-byte lowercase hex")
	}
	existing, err := app.badgerStore.GetState(governanceDelegationDomainStateKey)
	if err != nil {
		return fmt.Errorf("read existing governance delegation domain: %w", err)
	}
	if len(existing) != 0 {
		if len(existing) != sha256.Size {
			return fmt.Errorf("existing governance delegation domain has length %d, want %d", len(existing), sha256.Size)
		}
		if !bytes.Equal(existing, domain) {
			return fmt.Errorf("existing governance delegation domain differs from the quorum-approved app-v20 plan")
		}
		return nil
	}
	return app.badgerStore.SetState(governanceDelegationDomainStateKey, domain)
}

// recordAppV9Branch records which branch (pre/post app-v9) a gated handler took,
// as a Prometheus counter for the fork-activation dashboard. Metrics do NOT
// enter the AppHash (ComputeAppHash reads BadgerDB only), so this is purely
// observational and never affects replay.
func recordAppV9Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("app-v9", branch).Inc()
}

// recordAppV10Branch is the app-v10 sibling of recordAppV9Branch. Metrics-only,
// never in the AppHash.
func recordAppV10Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("app-v10", branch).Inc()
}

// recordAppV11Branch is the app-v11 sibling of recordAppV10Branch. Metrics-only,
// never in the AppHash.
func recordAppV11Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("app-v11", branch).Inc()
}

// recordAppV15Branch is the app-v15 sibling of recordAppV11Branch. Metrics-only,
// never in the AppHash — counts the pre/post split of the verb-ladder gate.
func recordAppV15Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("app-v15", branch).Inc()
}

// materializeAppV11Admin establishes a deterministic on-chain chain-admin at the
// app-v11 activation block so disabling the per-node SQL admin bootstrap (#36) can
// never leave the chain admin-less (#35). It is a NO-OP when an admin already
// exists — the normal case, since a post-app-v8 chain needed an admin to PROPOSE
// this very upgrade — and only fires for the degenerate state where none does
// (e.g. a skip-ahead that proposed app-v11 from a pre-app-v8 height, before the
// admin gate). The admin is the lexicographically-smallest committed validator: a
// pure function of the committed validator set (BadgerDB), identical on every
// replica, so the RegisterAgent write keeps the AppHash in lockstep — never per-node
// SQL. (A governance-supplied admin ID in the upgrade plan is a possible future
// refinement.) Called once, from the FinalizeBlock activation arm.
func (app *SageApp) materializeAppV11Admin(height int64) {
	// Already have an admin? Do nothing — don't mint an unwanted validator-admin.
	agents, err := app.badgerStore.ListRegisteredAgents()
	if err != nil {
		app.logger.Error().Err(err).Msg("app-v11 admin materialize: list agents failed")
		return
	}
	for i := range agents {
		if agents[i].Role == "admin" {
			return
		}
	}
	// No admin on chain — deterministically pick the smallest committed validator.
	vals, err := app.badgerStore.LoadValidators()
	if err != nil {
		app.logger.Error().Err(err).Msg("app-v11 admin materialize: load validators failed")
		return
	}
	smallest := ""
	for id := range vals {
		if smallest == "" || id < smallest {
			smallest = id
		}
	}
	if smallest == "" {
		app.logger.Error().Int64("height", height).Msg("app-v11 admin materialize: no validators to derive an admin from — chain left admin-less")
		return
	}
	// If the chosen validator is already a registered agent (e.g. a member with
	// metadata), elevate it to admin while PRESERVING its identity fields rather
	// than blind-overwriting them; register fresh otherwise. Reads committed state
	// only, so the choice and the written bytes are identical on every replica.
	name, bio, provider, p2p := "chain-admin", "", "", ""
	if existing, gErr := app.badgerStore.GetRegisteredAgent(smallest); gErr == nil && existing != nil {
		name, bio, provider, p2p = existing.Name, existing.BootBio, existing.Provider, existing.P2PAddress
	}
	if regErr := app.badgerStore.RegisterAgent(smallest, name, "admin", bio, provider, p2p, height); regErr != nil {
		app.logger.Error().Err(regErr).Str("admin_id", smallest[:16]).Msg("app-v11 admin materialize: RegisterAgent failed")
		return
	}
	app.logger.Warn().Str("admin_id", smallest[:16]).Int64("height", height).Msg("app-v11: no on-chain admin at activation — materialized the smallest validator as chain-admin")
}

// recordV8_5Branch is the v8.5 sibling of recordV8_4Branch. Same metric
// name (sage_fork_branch_total) with fork="v8.5" so dashboards can plot
// all five activations side by side.
func recordV8_5Branch(postFork bool) {
	branch := "pre"
	if postFork {
		branch = "post"
	}
	metrics.ForkBranchTotal.WithLabelValues("v8.5", branch).Inc()
}

// SetContentValidators installs the Layer-2 content-validator registry. nil (the
// default) leaves the gate inert — no registry, no enforcement. This is the ONLY
// runtime knob for the gate: there is no separate enable flag. Once a registry is
// wired AND the chain has activated the app-v7 fork, enforcement is automatic and
// chain-wide (driven by consensus state), not a per-node toggle. Boot-only: call
// once before the chain starts producing blocks; not safe to call concurrently
// with FinalizeBlock.
func (app *SageApp) SetContentValidators(r *contentvalidator.ContentValidatorRegistry) {
	app.contentValidators = r
}

// armContentValidatorsFromProvider installs the Layer-2 content-validation gate
// from a deployment-registered provider (contentvalidator.SetProvider) when one
// is present and an explicit registry was not already wired via
// SetContentValidators. SAGE registers no provider, so a stock build leaves the
// gate inert and every submission passes through. This is the release-stable
// arming seam that replaces per-release patches to the cmd entrypoints; see
// internal/contentvalidator/provider.go. Boot-only, called from the constructors.
func (app *SageApp) armContentValidatorsFromProvider() {
	if app.contentValidators != nil {
		return
	}
	// A context-aware provider takes precedence: it asked for live arm-time
	// chain state (the role resolver), so it is the richer registration. We hand
	// it a narrow armContext rather than the *SageApp directly, so a provider
	// cannot downcast back into mutable app internals — keeping this seam a real
	// decoupling boundary, not just an ergonomic alias.
	if reg := contentvalidator.BuildFromProviderWithContext(armContext{app: app}); reg != nil {
		app.contentValidators = reg
		if contentvalidator.HasProvider() {
			app.logger.Warn().Msg("both a context-aware and a no-arg content-validator provider are registered; using the context-aware one")
		}
		app.logger.Info().Msg("Layer-2 content-validation gate armed from registered context provider")
		return
	}
	if reg := contentvalidator.BuildFromProvider(); reg != nil {
		app.contentValidators = reg
		app.logger.Info().Msg("Layer-2 content-validation gate armed from registered provider")
	}
}

// armContext is the minimal, read-only view of app state handed to a
// context-aware content-validator provider (contentvalidator.ProviderWithContext)
// at arm time. It deliberately wraps *SageApp behind a narrow interface so a
// provider can capture arm-time chain state WITHOUT being able to reach back
// into mutable app internals (no downcast to *SageApp). The only state it
// exposes — the per-call read-only on-chain role lookup — is exactly what the
// gate already consumes inside FinalizeBlock, so it adds no new nondeterminism
// surface.
type armContext struct{ app *SageApp }

// RoleResolver satisfies contentvalidator.ArmContext by forwarding the app's
// existing deterministic, read-only role lookup.
func (c armContext) RoleResolver() func(agentID string) string { return c.app.RoleResolver() }

// ContentValidationEnforcementWarning returns a non-empty operator warning when
// this node will NOT enforce the Layer-2 content-validation gate on a chain that
// has already activated it — i.e. the app-v7 fork is live (appV7AppliedHeight > 0)
// but no validator registry is compiled in. Such a node is internally consistent
// and MUST stay bootable (a generic-only fleet is a valid deployment), so this is
// an advisory, not a fatal guard: returning an error here would brick a healthy
// app-v7 chain on restart.
//
// The hazard it surfaces is a MIXED fleet — if some validators run a registry-
// wired binary and others do not, the wired nodes reject (Code 18) where the bare
// nodes write (Code 0), diverging the AppHash. A local boot check cannot see
// peers, so it cannot prove parity; it can only flag that THIS node won't enforce
// so operators ensure every validator runs the same registry-wired build before
// activating app-v7. Returns "" when there is nothing to warn about.
func (app *SageApp) ContentValidationEnforcementWarning() string {
	if app.appV7AppliedHeight > 0 && app.contentValidators == nil {
		return fmt.Sprintf(
			"content-validation fork app-v7 is active at height %d but this node has no "+
				"content-validator registry compiled in: it will NOT enforce the Layer-2 gate. "+
				"If any peer validator DOES enforce, this node will diverge (it writes Code 0 "+
				"where an enforcing peer rejects Code 18). Ensure every validator runs the same "+
				"registry-wired build.",
			app.appV7AppliedHeight)
	}
	return ""
}

// RoleResolver returns a deterministic, read-only role lookup over on-chain
// agent state, intended to be captured ONCE at boot by a deployment's content
// validators so they can enforce signer-role authority from chain state rather
// than from a self-asserted role string in the record body.
//
// The returned closure performs a single read-only Badger lookup per call and
// returns "" when the agent is unknown or the read errors. GetRegisteredAgent
// returns (nil, err) on a missing key, so the `|| a == nil` guard is
// load-bearing: it prevents a nil-pointer deref on the *OnChainAgent returned
// alongside the not-found error.
//
// Consensus-safety: pure w.r.t. consensus — no time, no goroutines, no network,
// no writes; only a read-only Badger View. The string it returns is the RAW
// on-chain role as registered; any mapping from that to a deployment's own
// schema role vocabulary is the deployment's concern, not the chain's.
func (app *SageApp) RoleResolver() func(agentID string) string {
	return func(agentID string) string {
		a, err := app.badgerStore.GetRegisteredAgent(agentID)
		if err != nil || a == nil {
			return ""
		}
		return a.Role
	}
}

// reconcilePoEForkMonotonicity makes the v8.x PoE fork gates monotonic: a higher
// fork being active implies every lower one is too. If an upgrade jumps straight
// to a higher app version (e.g. app-v2 → app-v5, skipping app-v3/app-v4), the
// intermediate gates would otherwise stay 0, producing an incoherent state where
// postV8_4Fork is true but postV8_2Fork is false — domain-conditional weighting
// live without the PoE-weighted quorum it assumes, 56-byte vstats written while
// SetEpochWeights is suppressed (poe-drift audit). This backfills any unset lower
// gate to the height of the higher activation.
//
// It is IN-MEMORY only — derived identically on every replica from the same
// persisted activation records, so it never writes a key and never changes the
// AppHash keyspace by itself. On a sequentially-upgraded chain (every real chain)
// each lower gate already carries its own earlier height, so the `== 0` backfills
// never fire and the reconciliation is a no-op — replay stays byte-identical.
// Applied after the gate refreshes on boot AND after the activation block sets a
// gate, so the two paths converge on the same coherent state.
func (app *SageApp) reconcilePoEForkMonotonicity() {
	// Gates ordered low→high. Walk from the top down, carrying the height of the
	// nearest SET gate above; an unset gate inherits that height. This keeps the
	// activation heights non-decreasing (v8 ≤ v8_2 ≤ v8_3 ≤ v8_4 ≤ v8_5) so each
	// lower fork is active wherever a higher one is — backfilling a skipped gate to
	// the NEAREST higher activation, not the topmost (which would wrongly push a low
	// gate's activation later than an already-set intermediate gate's).
	// NOTE: appV7AppliedHeight (content-validator fork) is DELIBERATELY
	// excluded from this slice. It is an independent feature gate, not part of
	// the v8 PoE monotonic chain (v8 <= v8_2 <= ... <= v8_5); coupling its
	// activation height to the PoE forks would be wrong.
	gates := []*int64{&app.v8AppliedHeight, &app.v8_2AppliedHeight, &app.v8_3AppliedHeight, &app.v8_4AppliedHeight, &app.v8_5AppliedHeight}
	var nearestAbove int64
	for i := len(gates) - 1; i >= 0; i-- {
		if *gates[i] > 0 {
			nearestAbove = *gates[i]
		} else if nearestAbove > 0 {
			*gates[i] = nearestAbove
		}
	}
}

// refreshPoEWeights hydrates each validator's in-memory PoEWeight from the
// last poew:<id> set persisted by processEpoch. Called on boot after
// LoadValidators so a node restarting between epoch boundaries does NOT
// reset weights to zero — that would diverge consensus from any peer
// still running with the in-memory values. On a fresh chain (no
// poew:current marker yet) this is a no-op and the bootstrap fallback in
// checkAndApplyQuorum takes care of pre-first-epoch quorum decisions.
//
// Validators present in the persisted set but absent from the in-memory
// set are ignored (they were removed via governance after the epoch
// boundary). Validators present in the in-memory set but absent from the
// persisted set keep PoEWeight == 0 and hit the bootstrap fallback path.
func (app *SageApp) refreshPoEWeights() {
	weights, ok, err := app.badgerStore.GetEpochWeights()
	if err != nil {
		app.logger.Warn().Err(err).Msg("load epoch weights")
		return
	}
	if !ok {
		return // pre-first-epoch chain
	}
	for _, v := range app.validators.GetAll() {
		if w, present := weights[v.ID]; present {
			v.PoEWeight = w
		}
	}
	app.logger.Info().Int("hydrated", len(weights)).Msg("PoE weights restored from BadgerDB")
}

// poeWeightOrFallback returns the validator's persisted PoE weight if it's
// positive, otherwise an equal-share fallback of 1/N. Three call sites use
// the fallback path:
//
//   - Between activation block H_act and the first post-fork epoch boundary,
//     no poew:* keys exist yet — every validator hits the fallback so quorum
//     behaves identically to the pre-fork equal-weight branch.
//   - A validator added via governance at a non-boundary block carries
//     PoEWeight == 0 until the next processEpoch runs — they get 1/N until
//     then so they aren't silently disenfranchised for up to 100 blocks.
//   - Defensive guard against a poew:<id> entry being missing for an
//     otherwise-active validator (should not happen, but cheap to handle).
//
// Returning 1/N (rather than a fixed 1.0) keeps the fallback contribution
// in the same numeric range as NormalizeWeights' output, so a mid-epoch
// validator add doesn't suddenly contribute more weight than peers whose
// weights have been capped at RepCap. Quorum decisions are ratio-only
// (HasQuorum: acceptWeight/totalWeight >= 2/3) so the choice doesn't
// affect the threshold either way — the win is legibility of the
// acceptWeight/totalWeight audit log lines.
func poeWeightOrFallback(w float64, n int) float64 {
	if w > 0 {
		return w
	}
	if n <= 0 {
		return 1.0
	}
	return 1.0 / float64(n)
}

// poeFactorsPostV83 computes the post-v8.3 PoE input factors (accuracy,
// recency, corroboration) for a validator from its on-chain vstats record and
// the current block context. It is the single source of truth shared by
// processEpoch's per-epoch scalar weight and v8.4's per-memory domain-conditional
// quorum weight — any divergence between the two computations would split
// consensus, so both call here rather than inlining the arithmetic twice. A nil
// stats (validator never reached a terminal verdict) reads as the EWMA
// cold-start prior (0.5 accuracy), epsilon-floor recency, and zero corroboration.
//
// The same record shape backs both the global vstats:<v> stats and the per-domain
// vstats_domain:<v>:<D> stats, so this helper computes a domain-scoped accuracy
// just as readily as the global one — v8.4's quorum path calls it twice.
func poeFactorsPostV83(stats *store.ValidatorStats, height int64, blockTime time.Time) (accuracy, recency, corroboration float64) {
	var tracker poe.EWMATracker
	if stats != nil {
		tracker = poe.EWMATracker{
			WeightedSum: stats.EWMAWeightedSum,
			WeightDenom: stats.EWMAWeightDenom,
			Count:       int64(stats.EWMACount), // #nosec G115 -- non-negative
		}
	}
	accuracy = tracker.Accuracy()

	if stats != nil && stats.LastBlockHeight > 0 {
		blocksSinceLast := height - int64(stats.LastBlockHeight) // #nosec G115 -- block height fits in int64
		if blocksSinceLast < 0 {
			blocksSinceLast = 0
		}
		hoursSinceLast := float64(blocksSinceLast) * 3.0 / 3600.0
		recency = poe.RecencyScore(blockTime.Add(-time.Duration(hoursSinceLast*float64(time.Hour))), blockTime)
	} else {
		recency = poe.EpsilonFloor
	}

	corrCount := 0
	if stats != nil {
		corrCount = int(stats.CorrCount) // #nosec G115 -- bounded count
	}
	corroboration = poe.CorroborationScore(corrCount, poe.CorrMax)
	return
}

// defaultFlushMaxRetries caps Commit's SQLITE_BUSY retry loop. At 30 tries
// with backoff capped at 5s, sustained contention is tolerated for several
// minutes before the node panics — long enough to absorb realistic lock
// pile-ups, short enough that a broken store surfaces in operator-visible
// time rather than hanging the consensus pipeline forever.
const defaultFlushMaxRetries = 30

// NewSageApp creates a new SAGE ABCI application.
func NewSageApp(badgerPath string, postgresURL string, logger zerolog.Logger) (*SageApp, error) {
	bs, err := store.NewBadgerStore(badgerPath)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}

	ctx := context.Background()
	ps, err := store.NewPostgresStore(ctx, postgresURL)
	if err != nil {
		_ = bs.CloseBadger()
		return nil, fmt.Errorf("connect postgres: %w", err)
	}

	state, err := LoadState(bs)
	if err != nil {
		_ = ps.Close()
		_ = bs.CloseBadger()
		return nil, fmt.Errorf("load state: %w", err)
	}

	valSet := validator.NewValidatorSet()

	app := &SageApp{
		badgerStore:     bs,
		offchainStore:   ps,
		validators:      valSet,
		phiTracker:      poe.NewPhiTracker(50),
		govEngine:       governance.NewEngine(bs, &validatorSetAdapter{vs: valSet}),
		state:           state,
		logger:          logger.With().Str("component", "abci").Logger(),
		SuppCache:       NewSupplementaryCache(),
		flushMaxRetries: defaultFlushMaxRetries,
	}
	app.refreshV8Fork()
	app.refreshV8_2Fork()
	app.refreshV8_3Fork()
	app.refreshV8_4Fork()
	app.refreshV8_5Fork()
	app.refreshAppV7Fork()
	app.refreshAppV8Fork()
	app.refreshAppV9Fork()
	app.refreshAppV10Fork()
	app.refreshAppV11Fork()
	app.refreshAppV12Fork()
	app.refreshAppV13Fork()
	app.refreshAppV14Fork()
	app.refreshAppV15Fork()
	app.refreshAppV16Fork()
	app.refreshAppV17Fork()
	app.refreshAppV18Fork()
	app.refreshAppV19Fork()
	if invariantErr := app.validateAppliedAppV20State(); invariantErr != nil {
		_ = ps.Close()
		_ = bs.CloseBadger()
		return nil, invariantErr
	}
	app.reconcilePoEForkMonotonicity()

	// Reload persisted validators from BadgerDB (survives restart)
	persistedVals, err := bs.LoadValidators()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to load persisted validators")
	} else if len(persistedVals) > 0 {
		for id, power := range persistedVals {
			info := restoredValidatorInfo(id, power)
			if addErr := app.validators.AddValidator(info); addErr != nil {
				logger.Warn().Err(addErr).Str("validator", id).Msg("failed to restore validator")
			}
		}
		logger.Info().Int("validators", app.validators.Size()).Msg("validators restored from state")
	}
	app.refreshPoEWeights()

	app.armContentValidatorsFromProvider()

	return app, nil
}

// NewSageAppWithStores creates a SAGE ABCI application with pre-created stores.
// This allows plugging in any OffchainStore implementation (PostgresStore, SQLiteStore, etc.).
func NewSageAppWithStores(bs *store.BadgerStore, offchain store.OffchainStore, logger zerolog.Logger) (*SageApp, error) {
	state, err := LoadState(bs)
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	valSet := validator.NewValidatorSet()

	app := &SageApp{
		badgerStore:     bs,
		offchainStore:   offchain,
		validators:      valSet,
		phiTracker:      poe.NewPhiTracker(50),
		govEngine:       governance.NewEngine(bs, &validatorSetAdapter{vs: valSet}),
		state:           state,
		logger:          logger.With().Str("component", "abci").Logger(),
		SuppCache:       NewSupplementaryCache(),
		flushMaxRetries: defaultFlushMaxRetries,
	}
	app.refreshV8Fork()
	app.refreshV8_2Fork()
	app.refreshV8_3Fork()
	app.refreshV8_4Fork()
	app.refreshV8_5Fork()
	app.refreshAppV7Fork()
	app.refreshAppV8Fork()
	app.refreshAppV9Fork()
	app.refreshAppV10Fork()
	app.refreshAppV11Fork()
	app.refreshAppV12Fork()
	app.refreshAppV13Fork()
	app.refreshAppV14Fork()
	app.refreshAppV15Fork()
	app.refreshAppV16Fork()
	app.refreshAppV17Fork()
	app.refreshAppV18Fork()
	app.refreshAppV19Fork()
	if invariantErr := app.validateAppliedAppV20State(); invariantErr != nil {
		// The caller owns both injected stores. Returning an error must not
		// close process-shared projection or consensus handles behind it.
		return nil, invariantErr
	}
	app.reconcilePoEForkMonotonicity()

	persistedVals, err := bs.LoadValidators()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to load persisted validators")
	} else if len(persistedVals) > 0 {
		for id, power := range persistedVals {
			info := restoredValidatorInfo(id, power)
			if addErr := app.validators.AddValidator(info); addErr != nil {
				logger.Warn().Err(addErr).Str("validator", id).Msg("failed to restore validator")
			}
		}
		logger.Info().Int("validators", app.validators.Size()).Msg("validators restored from state")
	}
	app.refreshPoEWeights()

	app.armContentValidatorsFromProvider()

	return app, nil
}

// restoredValidatorInfo hydrates the Ed25519 public key omitted by the legacy
// validator:<id> persistence format. The ID is itself the canonical hex public
// key on healthy chains, so this is an in-memory reconstruction and does not
// change AppHash or historical replay. Malformed/non-canonical legacy IDs remain
// loaded for pre-v20 compatibility; appV20ConsensusReadiness rejects them.
func restoredValidatorInfo(id string, power int64) *validator.ValidatorInfo {
	info := &validator.ValidatorInfo{ID: id, Power: power}
	if publicKey, err := auth.AgentIDToPublicKey(id); err == nil {
		info.PublicKey = append(ed25519.PublicKey(nil), publicKey...)
	}
	return info
}

// currentAppVersion reports the consensus app version this node announces to
// CometBFT in Info(): the highest activated fork's target version, or 1 if
// none has activated. FinalizeBlock bumps consensus_params.version.app to
// plan.TargetAppVersion when a fork activates, so a node restarting on a
// post-fork chain MUST report the same version here or the CometBFT handshake
// sees an app-version regression against the committed consensus params.
//
// The PoE fork gates (v8..v8_5) activate in order (reconcilePoEForkMonotonicity
// guarantees a higher PoE fork implies every lower one is applied too), so a
// top-down check returns the current version. The fields are populated by the
// refresh*Fork calls in both constructors before CometBFT ever calls Info(). A
// new fork must add its case here, mirroring the *UpgradeName constants
// (app-vN → version N).
//
// app-v7 (content-validation activation) is an INDEPENDENT feature gate,
// NOT a member of the v8 PoE monotonic ladder. Its version (7) is deliberately
// DECOUPLED from both the PoE chain and the watchdog target. But because 7 is
// the HIGHEST version any activation can commit, its case MUST rank FIRST: once
// FinalizeBlock bumps committed consensus_params.version.app to 7 on app-v7
// activation, Info() has to report 7 too — regardless of which PoE gates are
// also set — or the next handshake sees a 7→6 regression and halts the chain.
// app-v7 can activate even though the PoE gates below it have NOT (it is
// excluded from reconcilePoEForkMonotonicity), so it cannot lean on the
// top-down PoE ordering; it is checked as a standalone top case.
//
// LOCKSTEP INVARIANT (PoE ladder only): the highest PoE case here (app-v6 →
// v8_5AppliedHeight) MUST equal cmd/sage-gui's upgradeTargetAppVersion and the
// highest v8*UpgradeName fork. The app-v6 version-regression guard
// (processUpgradePropose) rejects TargetAppVersion <= currentAppVersion(), and
// the watchdog reads this value via /abci_info — if a future PoE fork bumps the
// watchdog target without extending this switch, the ceiling lags, the guard
// wrongly accepts the live version, and the watchdog re-proposes in a loop.
// Adding a PoE app-vN means: add a v8_(N-1)AppliedHeight gate, a case returning
// N here, bump the watchdog target, and extend
// TestUpgradeNameConstantsAreCanonical — all together.
//
// app-v7 is INTENTIONALLY EXEMPT from that lockstep: the watchdog target stays
// at 6 (cmd/sage-gui/upgrade_watchdog.go) so app-v7 NEVER auto-fires and only
// activates via an explicit governance plan {Name:"app-v7", TargetAppVersion:7}.
// Post-fix the top case here is 7 while the watchdog target is 6 BY DESIGN —
// and that is also loop-safe: an app-v7-active chain reports 7, watchdog target
// 6 <= 7, so the watchdog stops without re-proposing.
func (app *SageApp) currentAppVersion() uint64 {
	switch {
	case app.appV20AppliedHeight > 0:
		return 20 // app-v20 (v11.9 domain-scoped quorum foundation) — highest gate
	case app.appV19AppliedHeight > 0:
		return 19 // app-v19 (empty scaffolding gate, v11.8 — off-consensus default-read flip only) — highest gate
	case app.appV18AppliedHeight > 0:
		return 18 // app-v18 (explicit global-admin RBAC override) — independent gate, ranks below app-v19 (19 > 18)
	case app.appV17AppliedHeight > 0:
		return 17 // app-v17 (reinstate + quorum challenge + delegated-proof hardening, v11.5)
	case app.appV16AppliedHeight > 0:
		return 16 // app-v16 (domainless-forget remediation, v11.2) — independent gate, ranks above app-v15 (16 > 15)
	case app.appV15AppliedHeight > 0:
		return 15 // app-v15 (empty fork-gate scaffolding, v11) — independent gate, ranks above app-v14 (15 > 14)
	case app.appV14AppliedHeight > 0:
		return 14 // app-v14 (content-validator gate deactivation, v10.7.0) — independent gate, ranks above app-v13 (14 > 13)
	case app.appV13AppliedHeight > 0:
		return 13 // app-v13 (corrected narrow AppHash exclusion, v10.5.1) — independent gate, ranks above app-v12 (13 > 12)
	case app.appV12AppliedHeight > 0:
		return 12 // app-v12 (whole-prefix state: AppHash exclusion, issue #40 — superseded by app-v13) — independent gate, ranks above app-v11 (12 > 11)
	case app.appV11AppliedHeight > 0:
		return 11 // app-v11 (activation-block deterministic chain-admin + SQL-admin-bootstrap disable) — independent gate, ranks above app-v10 (11 > 10)
	case app.appV10AppliedHeight > 0:
		return 10 // app-v10 (corroboration integrity guard + on-chain author) — independent gate, ranks above app-v9 (10 > 9)
	case app.appV9AppliedHeight > 0:
		return 9 // app-v9 (consensus-path nonce + admin self-grant downgrade) — independent gate, ranks above app-v8 (9 > 8)
	case app.appV8AppliedHeight > 0:
		return 8 // app-v8 (quorum-gated upgrades) — independent gate, ranks above app-v7 (8 > 7)
	case app.appV7AppliedHeight > 0:
		return 7 // app-v7 (content-validation activation) — independent gate, ranks above the PoE ladder
	case app.v8_5AppliedHeight > 0:
		return 6 // app-v6 (v8.5 upgrade-machinery hardening)
	case app.v8_4AppliedHeight > 0:
		return 5 // app-v5 (v8.4 domain-factor)
	case app.v8_3AppliedHeight > 0:
		return 4 // app-v4 (v8.3 PoE signals)
	case app.v8_2AppliedHeight > 0:
		return 3 // app-v3 (v8.2 PoE-weighted quorum)
	case app.v8AppliedHeight > 0:
		return 2 // app-v2 (v8.0 access-control)
	default:
		return 1
	}
}

// maxSupportedAppVersion is the highest app version this binary has a compiled
// fork gate for (currently app-v20). It is the readiness ceiling for upgrade
// auto-voting: a validator must never vote to activate an upgrade it cannot
// execute — doing so would commit consensus version.app=N while the binary
// still runs at N-1, halting the chain on the next CometBFT handshake (the
// maxSupportedAppVersion footgun). Bump this in lockstep with every new
// appV<N>UpgradeName fork gate added above.
const maxSupportedAppVersion uint64 = 20

// MaxSupportedAppVersion returns the highest app version this binary has a
// compiled fork gate for. Operator tooling (cmd/sage-gui `upgrade propose`)
// reads it to refuse proposing a target this binary cannot execute — the same
// readiness ceiling the auto-voter enforces via ActiveUpgradeVote. Exported
// because cmd/sage-gui lives outside this package; see maxSupportedAppVersion
// for the footgun this guards against.
func MaxSupportedAppVersion() uint64 { return maxSupportedAppVersion }

// SetExpectedGovernanceDelegationDomain derives the app-v20 domain from the
// runtime's authoritative CometBFT chain_id. It affects only whether this node
// auto-votes an upgrade; it never enters FinalizeBlock or AppHash computation.
func (app *SageApp) SetExpectedGovernanceDelegationDomain(chainID string) error {
	domain, err := governance.DelegationDomainForChainID(chainID)
	if err != nil {
		return err
	}
	app.expectedGovernanceDomainMu.Lock()
	defer app.expectedGovernanceDomainMu.Unlock()
	if app.expectedGovernanceDomain != "" && app.expectedGovernanceDomain != domain {
		return fmt.Errorf(
			"governance delegation domain already bound to %q; refusing rebind to %q",
			app.expectedGovernanceDomain,
			domain,
		)
	}
	app.expectedGovernanceDomain = domain
	return nil
}

func (app *SageApp) expectedGovernanceDelegationDomain() string {
	app.expectedGovernanceDomainMu.RLock()
	defer app.expectedGovernanceDomainMu.RUnlock()
	return app.expectedGovernanceDomain
}

func (app *SageApp) appV20BoundedConsensusReadiness() error {
	validators := app.validators.GetAll()
	if len(validators) > maxAppV20Validators {
		return fmt.Errorf("validator set has %d members, app-v20 limit %d", len(validators), maxAppV20Validators)
	}
	for _, current := range validators {
		if len(current.ID) > maxAppV20IdentifierBytes {
			return fmt.Errorf("validator identifier has %d bytes, app-v20 limit %d", len(current.ID), maxAppV20IdentifierBytes)
		}
		if len(current.PublicKey) != ed25519.PublicKeySize {
			return fmt.Errorf("validator %q has public key length %d, want %d", current.ID, len(current.PublicKey), ed25519.PublicKeySize)
		}
		if canonicalID := auth.PublicKeyToAgentID(current.PublicKey); current.ID != canonicalID {
			return fmt.Errorf("validator id %q does not canonically bind to its Ed25519 public key (want %q)", current.ID, canonicalID)
		}
	}
	active, err := app.govEngine.GetActiveProposal()
	if err != nil {
		return fmt.Errorf("read active governance proposal: %w", err)
	}
	if active != nil {
		if err := appV20Identifiers(
			id("active proposal id", active.ProposalID),
			id("active proposal target", active.TargetID),
			id("active proposal proposer", active.ProposerID),
		); err != nil {
			return err
		}
		if err := appV20Metadata("active proposal reason", active.Reason); err != nil {
			return err
		}
		if len(active.Payload) > maxAppV20ContentBytes {
			return fmt.Errorf("active proposal payload has %d bytes, app-v20 limit %d", len(active.Payload), maxAppV20ContentBytes)
		}
	}
	plan, planErr := app.badgerStore.GetUpgradePlan()
	if planErr != nil && !errors.Is(planErr, store.ErrNoUpgradePlan) {
		return fmt.Errorf("read pending upgrade plan: %w", planErr)
	}
	if plan != nil {
		if err := appV20Identifiers(
			id("pending upgrade name", plan.Name),
			id("pending upgrade digest", plan.BinarySHA256),
			id("pending upgrade proposer", plan.ProposerID),
			id("pending governance domain", plan.GovernanceDomain),
		); err != nil {
			return err
		}
	}
	return nil
}

func (app *SageApp) appV20LegacyResourceAuditComplete() (bool, error) {
	value, err := app.badgerStore.GetState(appV20LegacyResourceAuditStateKey)
	if err != nil {
		return false, fmt.Errorf("read app-v20 legacy resource audit marker: %w", err)
	}
	if value == nil {
		return false, nil
	}
	if !bytes.Equal(value, appV20LegacyResourceAuditValue) {
		return false, fmt.Errorf("app-v20 legacy resource audit marker is malformed")
	}
	return true, nil
}

func (app *SageApp) appV20ConsensusReadiness() error {
	if err := app.appV20BoundedConsensusReadiness(); err != nil {
		return err
	}
	complete, err := app.appV20LegacyResourceAuditComplete()
	if err != nil {
		return err
	}
	if !complete {
		return fmt.Errorf("authenticated legacy resource audit has not completed")
	}
	return nil
}

// appV20AuthenticatedProposalReadiness performs the one exhaustive legacy
// keyspace audit. It is called only after UpgradePropose has passed signature,
// canonical-field, registered-admin, and pending-plan authorization checks.
// Once the proposal exists, every intervening target-20 ceremony block is on
// the atomic/resource-limited path, so auto-voter ticks, quorum execution, and
// activation need only the bounded current-record checks above and never repeat
// this full scan. The caller persists the completion marker only after
// govEngine.Propose succeeds; a rejected transaction must never certify an
// audit or leave strict mode enabled.
func (app *SageApp) appV20AuthenticatedProposalReadiness() error {
	if err := app.appV20BoundedConsensusReadiness(); err != nil {
		return err
	}
	complete, err := app.appV20LegacyResourceAuditComplete()
	if err != nil {
		return err
	}
	if complete {
		return nil
	}
	if err := app.badgerStore.ValidateAppV20ResourceBounds(
		maxAppV20IdentifierBytes,
		maxAppV20MetadataBytes,
		maxAppV20EncodedRecordBytes,
		maxAppV20Validators,
	); err != nil {
		return fmt.Errorf("legacy consensus state is not app-v20-ready: %w", err)
	}
	return nil
}

// ActiveUpgradeVote reports the currently active OpUpgrade governance proposal,
// if any, for the in-process app-validators' auto-vote. It returns the proposal
// ID, the target app version, and whether THIS binary supports that target
// (target <= maxSupportedAppVersion). ok is false when there is no active
// upgrade proposal. The auto-voter accepts only when supported==true, so an
// upgrade the local binary cannot run never draws this node's voting power
// toward the 2/3 quorum — the liveness-layer guard against the
// maxSupportedAppVersion halt footgun (no consensus-rule change, no divergence
// risk, unlike a propose-time reject keyed on a per-binary constant).
//
// Read-only (badger MVCC reads via the governance engine), so it is safe to
// call from the validator goroutine concurrently with FinalizeBlock — the same
// pattern the memory-vote auto-voter already uses.
func (app *SageApp) ActiveUpgradeVote() (proposalID string, targetVersion uint64, supported, ok bool) {
	prop, err := app.govEngine.GetActiveProposal()
	if err != nil || prop == nil {
		return "", 0, false, false
	}
	if prop.Operation != governance.OpUpgrade {
		return "", 0, false, false
	}
	var payload UpgradeProposalPayload
	if uErr := json.Unmarshal(prop.Payload, &payload); uErr != nil {
		app.logger.Warn().Err(uErr).Str("proposal_id", prop.ProposalID).Msg("active OpUpgrade proposal has unparseable payload; skipping auto-vote")
		return "", 0, false, false
	}
	supported = payload.TargetAppVersion <= maxSupportedAppVersion
	appV20Ceremony := isAppV20CeremonyUpgrade(payload.Name, payload.TargetAppVersion, payload.GovernanceDomain)
	if payload.TargetAppVersion == 20 && !appV20Ceremony {
		// v11.8 could create a legacy empty-domain target-20 ballot. It remains
		// replayable, but this v11.9 binary must never vote it into activation.
		supported = false
	}
	if appV20Ceremony && app.currentAppVersion() != 19 {
		app.logger.Warn().
			Str("proposal_id", prop.ProposalID).
			Uint64("current_app_version", app.currentAppVersion()).
			Msg("app-v20 upgrade requires app-v19 as its immediate predecessor; skipping auto-vote")
		supported = false
	}
	expectedGovernanceDomain := app.expectedGovernanceDelegationDomain()
	if appV20Ceremony &&
		(expectedGovernanceDomain == "" || payload.GovernanceDomain != expectedGovernanceDomain) {
		app.logger.Warn().
			Str("proposal_id", prop.ProposalID).
			Str("proposed_domain", payload.GovernanceDomain).
			Str("expected_domain", expectedGovernanceDomain).
			Msg("app-v20 upgrade governance domain does not match this CometBFT chain; skipping auto-vote")
		supported = false
	}
	if appV20Ceremony && supported {
		if readinessErr := app.appV20ConsensusReadiness(); readinessErr != nil {
			app.logger.Warn().Err(readinessErr).Str("proposal_id", prop.ProposalID).
				Msg("local consensus state is not app-v20-ready; skipping auto-vote")
			supported = false
		}
	}
	return prop.ProposalID, payload.TargetAppVersion, supported, true
}

// UpgradeProposalHasVote reports whether voterID already has a recorded vote on
// the given proposal. The upgrade auto-voter (internal/voter) uses it to
// make broadcasting self-healing: it re-votes every tick until the vote is
// confirmed on-chain, instead of suppressing the vote for the proposal's
// lifetime after a single fire-and-forget broadcast that may have been dropped
// (mempool full, RPC blip). The governance engine rejects duplicate votes, so a
// confirmed vote is never double-counted; on a lookup error we report false so a
// (harmless, idempotently-rejected) re-vote is attempted rather than risking a
// silently lost vote. Read-only (badger MVCC), safe from the validator goroutine.
func (app *SageApp) UpgradeProposalHasVote(proposalID, voterID string) bool {
	votes, err := app.govEngine.GetProposalVotes(proposalID)
	if err != nil {
		return false
	}
	_, ok := votes[voterID]
	return ok
}

// Info returns application info for CometBFT handshake.
func (app *SageApp) Info(_ context.Context, req *abcitypes.RequestInfo) (*abcitypes.ResponseInfo, error) {
	ver := app.Version
	if ver == "" {
		ver = "dev"
	}
	return &abcitypes.ResponseInfo{
		Data:             "sage",
		Version:          ver,
		AppVersion:       app.currentAppVersion(),
		LastBlockHeight:  app.state.Height,
		LastBlockAppHash: app.state.AppHash,
	}, nil
}

// InitChain initializes the chain with genesis validators.
func (app *SageApp) InitChain(_ context.Context, req *abcitypes.RequestInitChain) (*abcitypes.ResponseInitChain, error) {
	valMap := make(map[string]int64, len(req.Validators))

	for _, v := range req.Validators {
		publicKey := append(ed25519.PublicKey(nil), v.PubKey.GetEd25519()...)
		info := &validator.ValidatorInfo{
			ID:        hex.EncodeToString(publicKey),
			PublicKey: publicKey,
			Power:     v.Power,
		}
		if err := app.validators.AddValidator(info); err != nil {
			app.logger.Warn().Err(err).Str("validator", info.ID).Msg("failed to add genesis validator")
		} else {
			valMap[info.ID] = info.Power
		}
	}

	// Persist validators to BadgerDB so they survive restarts
	if err := app.badgerStore.SaveValidators(valMap); err != nil {
		app.logger.Error().Err(err).Msg("failed to persist validators")
	}

	// issue #52: optionally seed the genesis chain-admin from app_state. Runs AFTER
	// the validator set is loaded (it needs the count for the single-validator gate).
	app.seedGenesisAdmin(req)

	metrics.ValidatorCount.Set(float64(app.validators.Size()))
	app.logger.Info().Int("validators", app.validators.Size()).Msg("chain initialized")

	return &abcitypes.ResponseInitChain{}, nil
}

// seedGenesisAdmin registers the genesis app_state's `sage.initial_admin` as the
// chain-admin at height 1, so a personal single-node chain can climb the fork
// ladder to app-v13 without ever stranding at the propose admin-gate (issue #52).
//
// It is deliberately decoupled from the fork-version gates: the chain still births
// at app-v1 and climbs; this only seeds an admin AGENT record. currentAppVersion()
// reads only the fork-applied heights, never agent records, so there is no version
// handshake skew (the trap that retired the earlier app_version-coupled seed).
//
// Consensus-safety invariants (all enforced here, all covered by tests):
//   - empty app_state  -> no write -> byte-identical to the pre-#52 InitChain (the
//     replay/state-sync safety proof: every existing chain has no app_state).
//   - quorum (len(Validators) != 1) -> no write: a multi-validator genesis could
//     carry a per-node initial_admin and fork the height-1 AppHash.
//   - any unmarshal / hex-decode error or wrong length -> no write, no panic.
//   - already registered -> no write: InitChain re-runs on reset/state-sync, and we
//     must never clobber an evolved record back to genesis defaults.
//
// The parse uses a fixed typed struct (never map[string]interface{}, which would
// admit float64 / iteration-order non-determinism).
func (app *SageApp) seedGenesisAdmin(req *abcitypes.RequestInitChain) {
	if len(req.AppStateBytes) == 0 {
		return // no app_state: identical to every chain born before this change
	}
	if len(req.Validators) != 1 {
		return // single-validator (personal) chains only — see invariants above
	}
	var as struct {
		Sage struct {
			InitialAdmin string `json:"initial_admin"`
		} `json:"sage"`
	}
	if err := json.Unmarshal(req.AppStateBytes, &as); err != nil {
		return // malformed app_state -> no seed
	}
	raw, err := hex.DecodeString(strings.TrimSpace(as.Sage.InitialAdmin))
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return // missing / non-hex / wrong length -> no seed
	}
	// Canonical lowercase hex: the signer derives lowercase (PublicKeyToAgentID) and
	// the propose gate compares agent_id verbatim, so an uppercase seed would strand.
	adminID := hex.EncodeToString(raw)
	if app.badgerStore.IsAgentRegistered(adminID) {
		return // idempotent across reset / state-sync re-InitChain
	}
	if err := app.badgerStore.RegisterAgent(adminID, "genesis-admin", "admin", "", "", "", 1); err != nil {
		app.logger.Error().Err(err).Str("admin_id", adminID[:16]).Msg("issue#52 genesis admin seed: RegisterAgent failed")
		return
	}
	app.logger.Info().Str("admin_id", adminID[:16]).Msg("issue#52: seeded genesis chain-admin from app_state.sage.initial_admin")
}

// ValidatorIDs returns a snapshot of the current consensus validator-set IDs
// (hex-encoded Ed25519 public keys). Read-only and safe to call from the voter
// goroutine concurrently with FinalizeBlock — same contract as ActiveUpgradeVote.
func (app *SageApp) ValidatorIDs() []string {
	all := app.validators.GetAll()
	ids := make([]string, 0, len(all))
	for _, v := range all {
		ids = append(ids, v.ID)
	}
	return ids
}

// ReconcileSelfValidator is a ONE-TIME, SINGLE-NODE-ONLY local repair for chains
// that previously ran the retired RegisterAppValidators path (the 4-archetype
// simulation). Those chains persisted 4 seed-derived "app validator" keys into the
// validator:* keyspace — in two shapes, depending on the chain's birth version:
//
//   - {4 archetypes, selfID absent}: the node's votes are rejected under the
//     per-node voter (signer not in the set, processMemoryVote Code 13) and
//     memories stop committing.
//   - {selfID + 4 archetypes} (issue #37): genesis persisted the node's consensus
//     key to validator:*, and the old path's SaveValidators upsert added the
//     archetypes without ever deleting it. The node votes fine, but the 4 phantom
//     archetypes hold 4/5 of the power and never vote, so every governance quorum
//     (acceptPower*3 >= totalPower*2 over ALL validators) is unreachable.
//
// It collapses both shapes to {selfID} via a LOCAL full-replace of the
// validator:* keyspace — the same keyspace the old path wrote, which is folded
// into ComputeAppHash. Such a local, non-consensus validator write is SAFE ONLY on
// a single-node chain (no peers to diverge from); on a multi-validator chain it
// would fork the AppHash. The guard therefore refuses unless ALL of the following
// hold:
//
//  1. the set MINUS selfID equals EXACTLY the supplied archetypeIDs and nothing
//     else (same size, same ids) — the unambiguous fingerprint of
//     "RegisterAppValidators ran on this node." A healthy set ({selfID} alone, or
//     any genesis-seeded quorum) has no archetype members, and a real N-validator
//     quorum has non-archetype members — neither matches, so the repair cannot
//     fire there and re-running after a repair is a permanent no-op.
//  2. singleNode is true — the caller (cmd/sage-gui) asserts this node is not in
//     quorum mode.
//
// archetypeIDs is supplied by the caller, which owns the seed→key derivation, so
// this consensus package stays ignorant of that scheme. Returns (changed, error);
// (false, nil) is the guard declining — the normal, healthy path.
// ValidatorCount returns the number of consensus validators currently in the
// set. A count of 1 means a single-validator chain, where agent add/remove never
// changes the validator set (so a full genesis-regenerating redeploy is
// unnecessary). Read-only; safe to call from the dashboard layer.
func (app *SageApp) ValidatorCount() int {
	return app.validators.Size()
}

func (app *SageApp) ReconcileSelfValidator(selfID string, archetypeIDs []string, singleNode bool) (bool, error) {
	if !singleNode || selfID == "" || len(archetypeIDs) == 0 {
		return false, nil
	}
	current := app.validators.GetAll()

	// Partition the set into the node's own key and everything else; keep the
	// node's existing power when present (10 otherwise, the genesis default).
	selfPower := int64(10)
	selfPresent := false
	others := make([]string, 0, len(current))
	for _, v := range current {
		if v.ID == selfID {
			selfPresent = true
			selfPower = v.Power
			continue
		}
		others = append(others, v.ID)
	}

	// (1) the non-self members must equal EXACTLY the archetype fingerprint
	// (same size, same ids).
	if len(others) != len(archetypeIDs) {
		return false, nil
	}
	fingerprint := make(map[string]struct{}, len(archetypeIDs))
	for _, id := range archetypeIDs {
		fingerprint[id] = struct{}{}
	}
	for _, id := range others {
		if _, ok := fingerprint[id]; !ok {
			return false, nil // a non-archetype validator is present → not a legacy single-node chain
		}
	}

	// All guards passed: this is a legacy single-node chain. Persist a clean
	// validator:* set FIRST (full-replace drops the stale archetype keys so they
	// cannot resurrect as phantom non-voting validators on restart). Only mutate the
	// in-memory set AFTER the durable write succeeds, so a persist failure leaves
	// in-memory and disk consistently un-repaired — safely retried on next restart
	// rather than leaving the node voting with a key that isn't on disk.
	if err := app.badgerStore.ReplaceValidators(map[string]int64{selfID: selfPower}); err != nil {
		return false, fmt.Errorf("reconcile persist: %w", err)
	}
	for _, id := range archetypeIDs {
		_ = app.validators.RemoveValidator(id)
	}
	if !selfPresent {
		if err := app.validators.AddValidator(restoredValidatorInfo(selfID, selfPower)); err != nil {
			return false, fmt.Errorf("reconcile self validator: %w", err)
		}
	}
	metrics.ValidatorCount.Set(float64(app.validators.Size()))
	app.logger.Warn().
		Str("self", selfID[:16]).
		Int("removed_archetypes", len(archetypeIDs)).
		Msg("legacy app-validators replaced by node consensus key (single-node repair)")
	return true, nil
}

// RepairSelfDupRejectedMemories is a SINGLE-NODE-ONLY local repair for memories
// wrongly deprecated by the voter dedup self-match bug (v10.1.0 collapsed the
// archetype checks into one all-must-pass verdict, and dedupCheck's
// FindByContentHash matched the proposed memory's own row — so the node's single
// validator rejected every memory as a "duplicate" of itself, and the
// all-voted-no-quorum branch deprecated it on arrival).
//
// The off-chain store owns the candidate fingerprint (deprecated + exactly one
// vote: selfID reject "duplicate content%" + never challenged — see
// RepairSelfDupRejected); this side flips the matching chain state per candidate:
// memstatus back to proposed (content hash preserved) and the bogus vote:* key
// dropped, so the fixed voter re-votes each memory through a real block and
// quorum recommits it within a few ticks.
//
// Like ReconcileSelfValidator, this is a local, non-consensus write to state
// folded into ComputeAppHash — safe ONLY with no peers to diverge from. The guard
// therefore refuses unless singleNode is asserted by the caller AND the live
// validator set is exactly {selfID} (run it AFTER ReconcileSelfValidator so a
// just-collapsed legacy set qualifies). The chain flip is idempotent
// (already-proposed candidates are left as-is), so a crash between the chain and
// mirror writes is healed by the next startup's pass.
func (app *SageApp) RepairSelfDupRejectedMemories(ctx context.Context, selfID string, singleNode bool) (int, error) {
	if !singleNode || selfID == "" || app.offchainStore == nil {
		return 0, nil
	}
	if vals := app.validators.GetAll(); len(vals) != 1 || vals[0].ID != selfID {
		return 0, nil
	}
	return app.offchainStore.RepairSelfDupRejected(ctx, selfID, func(memoryID string) error {
		hash, status, err := app.badgerStore.GetMemoryHash(memoryID)
		if err != nil {
			return err
		}
		// Idempotent re-entry: a prior pass already flipped the chain side.
		if status != string(memory.StatusDeprecated) && status != string(memory.StatusProposed) {
			return fmt.Errorf("memory %s has chain status %q — not repairing", memoryID, status)
		}
		if status == string(memory.StatusDeprecated) {
			if err := app.badgerStore.SetMemoryHash(memoryID, hash, string(memory.StatusProposed)); err != nil {
				return err
			}
		}
		return app.badgerStore.DeleteState(fmt.Sprintf("vote:%s:%s", memoryID, selfID))
	})
}

// CheckTx validates a transaction before it enters the mempool.
func (app *SageApp) CheckTx(_ context.Context, req *abcitypes.RequestCheckTx) (*abcitypes.ResponseCheckTx, error) {
	// v11.9 advisory hygiene is intentionally active before the app-v20 fork.
	// Every validator must restart onto this binary before the tagged ceremony,
	// so rejecting a transaction that can never fit app-v20's global atomic
	// budget keeps a stale, individually-oversized mempool head from surviving
	// the transition. This is NOT a consensus rule: pre-v20 ProcessProposal and
	// FinalizeBlock retain their historical behavior for directly delivered and
	// replayed blocks.
	if len(req.Tx) > maxAppV20AtomicFinalizeTxBytes {
		return &abcitypes.ResponseCheckTx{
			Code: appV20ResourceLimitCode,
			Log: fmt.Sprintf(
				"app-v20 resource limit: raw transaction has %d bytes, limit %d",
				len(req.Tx),
				maxAppV20AtomicFinalizeTxBytes,
			),
		}, nil
	}
	parsedTx, err := tx.DecodeTx(req.Tx)
	if err != nil {
		return &abcitypes.ResponseCheckTx{Code: 1, Log: fmt.Sprintf("decode error: %v", err)}, nil
	}
	postAppV20 := app.IsAppV20ActiveForNextTx()
	if postAppV20 {
		if tagErr := tx.ActivateMemorySubmitTags(parsedTx); tagErr != nil {
			return &abcitypes.ResponseCheckTx{Code: 1, Log: fmt.Sprintf("decode app-v20 extension: %v", tagErr)}, nil
		}
	}
	// Static field bounds are likewise safe advisory admission hygiene before
	// activation. FinalizeBlock repeats them only under committed ceremony state,
	// which is the load-bearing replay boundary.
	if resourceErr := validateAppV20TxResources(parsedTx); resourceErr != nil {
		return &abcitypes.ResponseCheckTx{Code: appV20ResourceLimitCode, Log: "app-v20 resource limit: " + resourceErr.Error()}, nil
	}

	// Verify Ed25519 signature
	valid, err := tx.VerifyTx(parsedTx)
	if err != nil || !valid {
		metrics.TxRejectedTotal.WithLabelValues("invalid_signature").Inc()
		return &abcitypes.ResponseCheckTx{Code: 2, Log: "invalid signature"}, nil
	}

	// Check nonce (replay protection)
	agentID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
	currentNonce, err := app.badgerStore.GetNonce(agentID)
	if err != nil {
		return &abcitypes.ResponseCheckTx{Code: 3, Log: fmt.Sprintf("nonce lookup error: %v", err)}, nil
	}
	// app-v9: reject the nonce-0 sentinel at mempool admission too, mirroring the
	// consensus-path gate (processTx). Gated on the app-v9 fork via state.Height so
	// pre-fork behaviour is unchanged.
	if parsedTx.Nonce == 0 && app.postAppV9Rules(app.state.Height) {
		metrics.TxRejectedTotal.WithLabelValues("replay_nonce").Inc()
		return &abcitypes.ResponseCheckTx{Code: 4, Log: "nonce 0 not permitted"}, nil
	}
	if parsedTx.Nonce <= currentNonce && currentNonce > 0 {
		metrics.TxRejectedTotal.WithLabelValues("replay_nonce").Inc()
		return &abcitypes.ResponseCheckTx{Code: 4, Log: fmt.Sprintf("nonce too low: got %d, expected > %d", parsedTx.Nonce, currentNonce)}, nil
	}

	// v8.0: reject post-fork tx types pre-fork so they never even hit the
	// mempool. Symmetric with the execution-side Code 10 returned by
	// processDomainReassign — keeps the wire surface honest and avoids
	// burning mempool slots on txs that can't execute yet.
	if parsedTx.Type == tx.TxTypeDomainReassign && !app.postV8Fork(app.state.Height) {
		return &abcitypes.ResponseCheckTx{Code: 10, Log: "unknown tx type"}, nil
	}

	// v11 (app-v15): reject co-commit txs pre-fork so they never reach the mempool.
	// LOAD-BEARING for mixed-binary safety: an old binary DECODE-fails these type
	// bytes (Code 1) while a v11 binary returns Code 10 — a v11 proposer seating a
	// type-31/32 tx pre-activation would diverge LastResultsHash across the set.
	// CheckTx has no block height, so gate on cached state height (single-chain, so
	// no cross-chain key collision). Symmetric with the exec-side Code 10.
	if (parsedTx.Type == tx.TxTypeCoCommitSubmit || parsedTx.Type == tx.TxTypeCoCommitAttest ||
		parsedTx.Type == tx.TxTypeCrossFedSet || parsedTx.Type == tx.TxTypeCrossFedRevoke) &&
		!app.postAppV15Fork(app.state.Height) {
		return &abcitypes.ResponseCheckTx{Code: 10, Log: "unknown tx type"}, nil
	}

	// v11.5 (app-v17): reject the new memory-reinstate tx pre-fork so it never
	// reaches the mempool. Symmetric with the exec-side Code 10 in
	// processMemoryReinstate. Same mixed-binary reasoning as the co-commit block:
	// an old binary DECODE-fails type byte 35 (Code 1) while a v17 binary returns
	// Code 10, so a v17 proposer seating one pre-activation would diverge
	// LastResultsHash. CheckTx has no block height, so gate on cached state
	// height (postAppV17Fork, not Rules). (TxTypeMemoryChallenge is a legacy type
	// with no CheckTx gate — C3's count-scaled behavior is layered inside its
	// handler behind postAppV17Rules, so nothing to add here for it.)
	if parsedTx.Type == tx.TxTypeMemoryReinstate && !app.IsAppV17ActiveForNextTx() {
		return &abcitypes.ResponseCheckTx{Code: 10, Log: "unknown tx type"}, nil
	}

	// v11 (app-v15): keep post-fork AccessQuery txs out of the mempool. INVERTED
	// vs the block above — TxTypeAccessQuery is a KNOWN type today, so it is
	// rejected once the fork IS active (whereas the co-commit types are rejected
	// until active). The exec-side Code 10 is the load-bearing determinism
	// guarantee; this is mempool hygiene so a post-fork proposer never seats one.
	// Safe under mixed binaries: this fires only post-activation, by which point
	// every validator runs the v15 binary (activation requires the rolled binary).
	if parsedTx.Type == tx.TxTypeAccessQuery && app.postAppV15Fork(app.state.Height) {
		return &abcitypes.ResponseCheckTx{Code: 10, Log: "unknown tx type"}, nil
	}

	// app-v17: advisory mempool validation for delegated agent proofs. Wall
	// time is acceptable in CheckTx (which is not consensus); FinalizeBlock
	// repeats the check against req.Time and atomically consumes the proof.
	if app.IsAppV17ActiveForNextTx() {
		if proofErr := app.enforceDelegatedAgentProof(parsedTx, time.Now(), false, postAppV20); proofErr != nil {
			metrics.TxRejectedTotal.WithLabelValues("agent_proof_binding").Inc()
			return &abcitypes.ResponseCheckTx{Code: 109, Log: fmt.Sprintf("agent proof rejected: %v", proofErr)}, nil
		}
	}

	return &abcitypes.ResponseCheckTx{Code: 0}, nil
}

func cloneValidatorSetForFinalize(source *validator.ValidatorSet) *validator.ValidatorSet {
	clone := validator.NewValidatorSet()
	if source == nil {
		return clone
	}
	for _, current := range source.GetAll() {
		info := &validator.ValidatorInfo{
			ID:        current.ID,
			PublicKey: append(ed25519.PublicKey(nil), current.PublicKey...),
			Power:     current.Power,
			PoEWeight: current.PoEWeight,
		}
		if err := clone.AddValidator(info); err != nil {
			panic(fmt.Sprintf("sage: clone validator %s for app-v20 FinalizeBlock: %v", current.ID, err))
		}
	}
	return clone
}

// cloneForAppV20Finalize constructs an independent mutable consensus graph.
// Process-owned handles (logger, projection, schedulers, content validator
// registry) are shared but never mutated by FinalizeBlock. Every mutable
// consensus object is copied and published only after the Badger transaction
// commits durably.
func (app *SageApp) cloneForAppV20Finalize(scopedStore *store.BadgerStore) *SageApp {
	clonedValidators := cloneValidatorSetForFinalize(app.validators)
	var clonedState *AppState
	if app.state != nil {
		clonedState = &AppState{
			Height: app.state.Height, AppHash: append([]byte(nil), app.state.AppHash...),
			EpochNum: app.state.EpochNum,
		}
	}
	clone := &SageApp{
		badgerStore: scopedStore, offchainStore: app.offchainStore,
		validators: clonedValidators, phiTracker: app.phiTracker.Clone(),
		govEngine: governance.NewEngine(scopedStore, &validatorSetAdapter{vs: clonedValidators}),
		state:     clonedState, logger: app.logger, Version: app.Version,
		flushMaxRetries: app.flushMaxRetries, SuppCache: app.SuppCache.cloneForFinalize(),
		snapshotScheduler: app.snapshotScheduler,
		v8AppliedHeight:   app.v8AppliedHeight, v8_2AppliedHeight: app.v8_2AppliedHeight,
		v8_3AppliedHeight: app.v8_3AppliedHeight, v8_4AppliedHeight: app.v8_4AppliedHeight,
		v8_5AppliedHeight: app.v8_5AppliedHeight,
		contentValidators: app.contentValidators, appV7AppliedHeight: app.appV7AppliedHeight,
		appV8AppliedHeight: app.appV8AppliedHeight, appV9AppliedHeight: app.appV9AppliedHeight,
		appV10AppliedHeight: app.appV10AppliedHeight, appV11AppliedHeight: app.appV11AppliedHeight,
		appV12AppliedHeight: app.appV12AppliedHeight, appV13AppliedHeight: app.appV13AppliedHeight,
		appV14AppliedHeight: app.appV14AppliedHeight, appV15AppliedHeight: app.appV15AppliedHeight,
		appV16AppliedHeight: app.appV16AppliedHeight, appV17AppliedHeight: app.appV17AppliedHeight,
		appV18AppliedHeight: app.appV18AppliedHeight, appV19AppliedHeight: app.appV19AppliedHeight,
		appV20AppliedHeight:      app.appV20AppliedHeight,
		retainBlocks:             app.retainBlocks,
		expectedGovernanceDomain: app.expectedGovernanceDelegationDomain(),
	}
	if notifier := app.syncNotifier.Load(); notifier != nil {
		clone.syncNotifier.Store(notifier)
	}
	return clone
}

// publishAppV20Finalize makes the fully committed speculative graph live.
func (app *SageApp) publishAppV20Finalize(clone *SageApp) {
	app.validators = clone.validators
	app.phiTracker = clone.phiTracker
	app.govEngine = governance.NewEngine(app.badgerStore, &validatorSetAdapter{vs: app.validators})
	app.state = clone.state
	app.pendingWrites = clone.pendingWrites
	app.v8AppliedHeight = clone.v8AppliedHeight
	app.v8_2AppliedHeight = clone.v8_2AppliedHeight
	app.v8_3AppliedHeight = clone.v8_3AppliedHeight
	app.v8_4AppliedHeight = clone.v8_4AppliedHeight
	app.v8_5AppliedHeight = clone.v8_5AppliedHeight
	app.appV7AppliedHeight = clone.appV7AppliedHeight
	app.appV8AppliedHeight = clone.appV8AppliedHeight
	app.appV9AppliedHeight = clone.appV9AppliedHeight
	app.appV10AppliedHeight = clone.appV10AppliedHeight
	app.appV11AppliedHeight = clone.appV11AppliedHeight
	app.appV12AppliedHeight = clone.appV12AppliedHeight
	app.appV13AppliedHeight = clone.appV13AppliedHeight
	app.appV14AppliedHeight = clone.appV14AppliedHeight
	app.appV15AppliedHeight = clone.appV15AppliedHeight
	app.appV16AppliedHeight = clone.appV16AppliedHeight
	app.appV17AppliedHeight = clone.appV17AppliedHeight
	app.appV18AppliedHeight = clone.appV18AppliedHeight
	app.appV19AppliedHeight = clone.appV19AppliedHeight
	app.appV20AppliedHeight = clone.appV20AppliedHeight
}

// requiresAppV20StrictRulesAt turns on v20 admission/resource/error semantics
// only from committed ceremony state. A raw target-20 transaction is excluded:
// during a rolling binary upgrade, a forged bootstrap transaction must execute
// every unrelated legacy transaction exactly as the old binary would.
func (app *SageApp) requiresAppV20StrictRulesAt(height int64) bool {
	if app.postAppV20Fork(height) ||
		(app.appV20AppliedHeight > 0 && height == app.appV20AppliedHeight && app.state != nil && app.state.Height < height) {
		return true
	}
	active, err := app.govEngine.GetActiveProposal()
	if err != nil {
		// Store uncertainty must never downgrade the durability boundary. The
		// atomic path will surface the same read failure while preserving the old
		// durable state; the legacy path could otherwise partially publish a block.
		return true
	}
	if isAppV20CeremonyProposal(active) {
		return true
	}
	plan, err := app.badgerStore.GetUpgradePlan()
	if err == nil && isAppV20CeremonyPlan(plan) {
		return true
	}
	if err != nil && !errors.Is(err, store.ErrNoUpgradePlan) {
		return true
	}
	auditComplete, auditErr := app.appV20LegacyResourceAuditComplete()
	if auditErr != nil || auditComplete {
		return true
	}
	return false
}

// requiresAppV20AtomicFinalizeAt is a durability selector, not a fork-rule
// selector. A raw target-20 bootstrap/cancel may safely choose the speculative
// transaction boundary, while requiresAppV20StrictRulesAt remains false until
// the authenticated proposal and audit marker are committed.
func (app *SageApp) requiresAppV20AtomicFinalizeAt(height int64, txs [][]byte) bool {
	if app.requiresAppV20StrictRulesAt(height) {
		return true
	}
	for _, rawTx := range txs {
		parsed, err := tx.DecodeTx(rawTx)
		if err != nil {
			continue
		}
		if parsed.Type == tx.TxTypeUpgradePropose && parsed.UpgradePropose != nil &&
			isAppV20CeremonyUpgrade(parsed.UpgradePropose.Name, parsed.UpgradePropose.TargetAppVersion, parsed.UpgradePropose.GovernanceDomain) {
			return true
		}
	}
	return false
}

func (app *SageApp) appV20PendingPlanFreezesValidatorReconfiguration() bool {
	plan, err := app.badgerStore.GetUpgradePlan()
	return err == nil && isAppV20CeremonyPlan(plan) && app.appV20AppliedHeight == 0
}

func (app *SageApp) requiresAppV20AtomicFinalize(req *abcitypes.RequestFinalizeBlock) bool {
	return req != nil && app.requiresAppV20AtomicFinalizeAt(req.Height, req.Txs)
}

// FinalizeBlock processes all transactions in a block. Every post-app-v20
// block executes against one speculative Badger transaction that remains
// uncommitted until ABCI Commit. This makes the complete state transition —
// including proof claims, nonces, governance, ordinary state, AppHash, and the
// Commit handshake tuple — one crash-atomic durability unit.
// Pre-app-v20 blocks retain their historical incremental path byte-for-byte.
func (app *SageApp) FinalizeBlock(ctx context.Context, req *abcitypes.RequestFinalizeBlock) (*abcitypes.ResponseFinalizeBlock, error) {
	if app.pendingAppV20Finalize != nil {
		panic("sage: FinalizeBlock called before Commit completed the prior app-v20 block")
	}
	if !app.requiresAppV20AtomicFinalize(req) {
		return app.finalizeBlockUncommitted(ctx, req)
	}
	if app.postAppV20Fork(req.Height) && len(app.validators.GetAll()) > maxAppV20Validators {
		return nil, fmt.Errorf("sage: app-v20 validator set has %d members, limit %d", len(app.validators.GetAll()), maxAppV20Validators)
	}

	scopedStore := app.badgerStore.BeginConsensusTransaction(app.appV20MutationFaultHook)
	working := app.cloneForAppV20Finalize(scopedStore)
	staged := false
	defer func() {
		if !staged {
			scopedStore.DiscardConsensusTransaction()
		}
	}()
	response, err := working.finalizeBlockUncommitted(ctx, req)
	if err != nil {
		return nil, err
	}
	if txnErr := scopedStore.ConsensusTransactionError(); txnErr != nil {
		return nil, fmt.Errorf("sage: discard poisoned app-v20 FinalizeBlock transaction: %w", txnErr)
	}
	app.pendingAppV20Finalize = &appV20AtomicFinalize{app: working, store: scopedStore}
	staged = true
	return response, nil
}

// finalizeBlockUncommitted contains the historical deterministic state machine.
// CRITICAL: No time.Now(), no map iteration without sorting, no goroutines, and
// no external I/O except the supplied BadgerStore view.
func (app *SageApp) finalizeBlockUncommitted(_ context.Context, req *abcitypes.RequestFinalizeBlock) (*abcitypes.ResponseFinalizeBlock, error) {
	start := time.Now()
	app.logger.Debug().Int64("height", req.Height).Int("txs", len(req.Txs)).Msg("finalizing block")

	// Clear pending writes from previous block
	app.pendingWrites = nil
	postAppV20 := app.postAppV20Fork(req.Height)
	strictV20Rules := app.requiresAppV20StrictRulesAt(req.Height)
	atomicBudgetOK := !strictV20Rules || appV20AtomicFinalizeBudget(req.Txs)
	app.appV20StrictFinalize = strictV20Rules
	app.appV20FinalizeTxCount = len(req.Txs)
	app.appV20FinalizeBudgetOK = appV20AtomicFinalizeBudget(req.Txs)
	app.appV20ProposalBootstrapped = false
	defer func() {
		app.appV20StrictFinalize = false
		app.appV20FinalizeTxCount = 0
		app.appV20FinalizeBudgetOK = false
		app.appV20ProposalBootstrapped = false
	}()

	txResults := make([]*abcitypes.ExecTxResult, len(req.Txs))

	for i, rawTx := range req.Txs {
		parsedTx, err := tx.DecodeTx(rawTx)
		if err != nil {
			txResults[i] = &abcitypes.ExecTxResult{Code: 1, Log: err.Error()}
			continue
		}
		if postAppV20 {
			if err := tx.ActivateMemorySubmitTags(parsedTx); err != nil {
				txResults[i] = &abcitypes.ExecTxResult{Code: 1, Log: "invalid app-v20 transaction extension"}
				continue
			}
		}
		if strictV20Rules {
			if resourceErr := validateAppV20TxResources(parsedTx); resourceErr != nil {
				txResults[i] = &abcitypes.ExecTxResult{Code: appV20ResourceLimitCode, Log: "app-v20 resource limit: " + resourceErr.Error()}
				continue
			}
		}

		// app-v15 (v11): reject non-canonical tx encodings. DecodeTx tolerates
		// trailing bytes and a few fields have more than one valid wire form (e.g.
		// the co-commit optional-window), so a signed tx is malleable to distinct
		// byte strings - and CometBFT tx-hashes - carrying the SAME signature.
		// EncodeTx emits the one canonical form, so requiring the delivered bytes to
		// re-encode identically pins every accepted tx to a single encoding. Fork-
		// gated: pre-v15 blocks replay with the old lenient behavior (byte-identical
		// AppHash); v15+ enforce canonicalization. Legit txs are built via EncodeTx,
		// so they always pass.
		if app.postAppV15Fork(req.Height) {
			if canon, encErr := tx.EncodeTx(parsedTx); encErr != nil || !bytes.Equal(canon, rawTx) {
				txResults[i] = &abcitypes.ExecTxResult{Code: 1, Log: "non-canonical tx encoding rejected"}
				continue
			}
		}
		if !atomicBudgetOK {
			txResults[i] = &abcitypes.ExecTxResult{Code: appV20AtomicFinalizeBudgetCode, Log: appV20AtomicFinalizeBudgetLog}
			continue
		}

		// Use req.Time for deterministic timestamps (NOT time.Now())
		blockTime := req.Time

		result := app.processTx(parsedTx, req.Height, blockTime)
		txResults[i] = result

		if result.Code == 0 {
			agentID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
			if nonceErr := app.badgerStore.SetNonce(agentID, parsedTx.Nonce); nonceErr != nil {
				app.logger.Error().Err(nonceErr).Msg("failed to update nonce")
			}
		}
	}

	// Governance post-processing: evaluate active proposal after ALL txs are processed.
	// This handles single-block auto-approve (proposal created + quorum in same block).
	var valUpdates []abcitypes.ValidatorUpdate
	var executedProposal *governance.ProposalState
	var govErr error
	postAppV20Governance := app.postAppV20Fork(req.Height)
	freezeValidatorReconfiguration := app.appV20PendingPlanFreezesValidatorReconfiguration()
	var target20ProposalBefore *governance.ProposalState
	if strictV20Rules {
		auditComplete, auditErr := app.appV20LegacyResourceAuditComplete()
		if auditErr != nil {
			return nil, fmt.Errorf("sage: app-v20 atomic audit-marker read failed: %w", auditErr)
		}
		if auditComplete {
			activeBefore, activeErr := app.govEngine.GetActiveProposal()
			if activeErr != nil {
				return nil, fmt.Errorf("sage: app-v20 atomic active-proposal read failed: %w", activeErr)
			}
			if isAppV20CeremonyProposal(activeBefore) {
				target20ProposalBefore = activeBefore
			}
		}
	}
	if app.appV20ProposalBootstrapped {
		// The authenticated bootstrap proposal must first commit its audit marker.
		// Governance evaluation begins in the following block, disabling the old
		// single-block auto-approve edge for target-20.
	} else if postAppV20Governance || freezeValidatorReconfiguration {
		executedProposal, govErr = app.govEngine.ProcessBlockValidated(req.Height, func(proposal *governance.ProposalState) error {
			if !isValidatorSetGovernanceOperation(proposal.Operation) {
				return nil
			}
			if freezeValidatorReconfiguration && !postAppV20Governance {
				return errors.New("validator reconfiguration is frozen while the app-v20 activation plan is pending")
			}
			return app.validateAppV20ValidatorOperation(
				proposal.Operation,
				proposal.TargetID,
				proposal.TargetPubKey,
				proposal.TargetPower,
			)
		})
	} else {
		executedProposal, govErr = app.govEngine.ProcessBlock(req.Height)
	}
	if govErr != nil {
		app.logger.Error().Err(govErr).Msg("governance post-processing failed")
		if strictV20Rules {
			return nil, fmt.Errorf("sage: app-v20 atomic governance post-processing failed: %w", govErr)
		}
	}
	if target20ProposalBefore != nil && executedProposal == nil {
		activeAfter, activeErr := app.govEngine.GetActiveProposal()
		if activeErr != nil {
			return nil, fmt.Errorf("sage: app-v20 atomic terminal-proposal read failed: %w", activeErr)
		}
		if activeAfter == nil {
			if markerErr := app.badgerStore.DeleteState(appV20LegacyResourceAuditStateKey); markerErr != nil {
				return nil, fmt.Errorf("sage: clear audit marker for terminal app-v20 proposal %s: %w", target20ProposalBefore.ProposalID, markerErr)
			}
		}
	}
	if executedProposal != nil {
		app.logger.Info().
			Str("proposal_id", executedProposal.ProposalID).
			Uint8("operation", uint8(executedProposal.Operation)).
			Str("target", executedProposal.TargetID).
			Msg("governance proposal executed")

		update, applyErr := app.applyGovernanceProposal(executedProposal, req.Height)
		if applyErr != nil {
			if strictV20Rules {
				return nil, fmt.Errorf("sage: app-v20 atomic governance proposal %s failed to apply: %w", executedProposal.ProposalID, applyErr)
			}
			if app.postAppV20Fork(req.Height) && isValidatorSetGovernanceOperation(executedProposal.Operation) {
				// The validated execution path above makes every expected validator
				// operation infallible until it mutates the set. Never let an
				// unexpected invariant violation commit StatusExecuted without the
				// matching CometBFT update.
				panic(fmt.Sprintf("sage: validated app-v20 governance proposal %s failed to apply: %v", executedProposal.ProposalID, applyErr))
			}
			app.logger.Error().Err(applyErr).Msg("failed to apply governance proposal")
		} else if update != nil {
			valUpdates = append(valUpdates, *update)
		}

		// Buffer offchain status update for Commit
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "gov_status_update",
			data: govStatusUpdateData{
				ProposalID:     executedProposal.ProposalID,
				Status:         string(governance.StatusExecuted),
				ExecutedHeight: req.Height,
			},
		})
	}

	// Check epoch boundary
	if poe.IsEpochBoundary(req.Height) {
		if epochErr := app.processEpoch(req.Height, req.Time, strictV20Rules); epochErr != nil {
			return nil, fmt.Errorf("sage: app-v20 atomic epoch processing failed: %w", epochErr)
		}
	}

	// v7.5 upgrade plan activation. If a plan is pending and its
	// ActivationHeight matches this block, bump the chain's app
	// version via ConsensusParamUpdates and mark the plan applied.
	// CometBFT applies the new app version at H+1 across every node
	// atomically. Read-then-mark inside FinalizeBlock keeps the
	// transition deterministic across replicas.
	var consensusParamUpdates *cmtproto.ConsensusParams
	plan, planErr := app.badgerStore.GetUpgradePlan()
	if strictV20Rules && planErr != nil && !errors.Is(planErr, store.ErrNoUpgradePlan) {
		return nil, fmt.Errorf("sage: app-v20 atomic upgrade-plan read failed: %w", planErr)
	}
	if planErr == nil && plan != nil && plan.ActivationHeight == req.Height {
		appV20CeremonyPlan := isAppV20CeremonyPlan(plan)
		// The app-v20 quorum approved this exact chain domain in the upgrade
		// payload. Persist it BEFORE atomically consuming the pending plan so a
		// crash can never leave an applied app-v20 record without its domain.
		if appV20CeremonyPlan {
			if plan.Name != appV20UpgradeName || plan.TargetAppVersion != 20 || !isCanonicalAppV20CeremonyDomain(plan.GovernanceDomain) {
				panic(fmt.Sprintf(
					"sage: refuse malformed app-v20 ceremony activation at height %d: name=%q target_app_version=%d governance_domain=%q",
					req.Height,
					plan.Name,
					plan.TargetAppVersion,
					plan.GovernanceDomain,
				))
			}
			if current := app.currentAppVersion(); current != 19 {
				panic(fmt.Sprintf("sage: refuse app-v20 activation at height %d: current committed app version is %d, want 19", req.Height, current))
			}
			if readinessErr := app.appV20ConsensusReadiness(); readinessErr != nil {
				panic(fmt.Sprintf("sage: refuse unsafe app-v20 activation at height %d: %v", req.Height, readinessErr))
			}
			if commitErr := app.validateAppV20ActivationValidatorCommit(req.DecidedLastCommit); commitErr != nil {
				panic(fmt.Sprintf("sage: refuse unsafe app-v20 activation validator commit at height %d: %v", req.Height, commitErr))
			}
			if domainErr := app.ensureGovernanceDelegationDomain(plan.GovernanceDomain); domainErr != nil {
				app.logger.Error().Err(domainErr).Int64("height", req.Height).
					Msg("CRITICAL: failed to materialize app-v20 governance delegation domain")
				panic(fmt.Sprintf("sage: governance delegation domain failed at height %d: %v", req.Height, domainErr))
			}
		}
		// Version-non-regression floor (deterministic on every replica): never
		// commit a consensus version.app lower than the chain's current app
		// version. app-v7 (content-validation) is an INDEPENDENT gate that can be
		// activated out of order relative to the PoE ladder (app-v2..app-v6); a
		// late app-v6 activation after app-v7 would otherwise commit version.app=6
		// while currentAppVersion() already reports 7, and the next CometBFT
		// handshake would see a 7->6 regression and halt every node (the
		// v8.4.1/8.4.2 bug class). On a would-be regression, skip ONLY the version
		// bump — the feature gate below still activates and the plan is still
		// marked applied, so app-v6's behavior turns on without rewinding the
		// committed version. currentAppVersion() reads in-memory fork heights set
		// identically on every node, so this branch is replica-deterministic.
		if plan.TargetAppVersion >= app.currentAppVersion() {
			consensusParamUpdates = &cmtproto.ConsensusParams{
				Version: &cmtproto.VersionParams{App: plan.TargetAppVersion},
			}
		} else {
			app.logger.Error().
				Str("name", plan.Name).
				Uint64("target_app_version", plan.TargetAppVersion).
				Uint64("current_app_version", app.currentAppVersion()).
				Int64("height", req.Height).
				Msg("upgrade plan would regress consensus version.app; skipping the version bump (feature gate still activates) to prevent a handshake halt")
		}
		if markErr := app.badgerStore.MarkUpgradeApplied(plan.Name, plan.TargetAppVersion, req.Height); markErr != nil {
			app.logger.Error().Err(markErr).
				Str("name", plan.Name).
				Uint64("target_app_version", plan.TargetAppVersion).
				Int64("height", req.Height).
				Msg("failed to mark upgrade applied — chain state will be inconsistent with audit trail")
			if strictV20Rules {
				return nil, fmt.Errorf("sage: app-v20 atomic mark upgrade %s applied: %w", plan.Name, markErr)
			}
		}
		if plan.Name == v8UpgradeName {
			app.v8AppliedHeight = req.Height
		}
		if plan.Name == v8_2UpgradeName {
			app.v8_2AppliedHeight = req.Height
		}
		if plan.Name == v8_3UpgradeName {
			app.v8_3AppliedHeight = req.Height
		}
		if plan.Name == v8_4UpgradeName {
			app.v8_4AppliedHeight = req.Height
		}
		if plan.Name == v8_5UpgradeName {
			app.v8_5AppliedHeight = req.Height
		}
		if plan.Name == appV7UpgradeName {
			app.appV7AppliedHeight = req.Height
		}
		if plan.Name == appV8UpgradeName {
			app.appV8AppliedHeight = req.Height
		}
		if plan.Name == appV9UpgradeName {
			app.appV9AppliedHeight = req.Height
		}
		if plan.Name == appV10UpgradeName {
			app.appV10AppliedHeight = req.Height
		}
		if plan.Name == appV13UpgradeName {
			app.appV13AppliedHeight = req.Height
		}
		if plan.Name == appV14UpgradeName {
			app.appV14AppliedHeight = req.Height
		}
		if plan.Name == appV15UpgradeName {
			app.appV15AppliedHeight = req.Height
		}
		if plan.Name == appV16UpgradeName {
			app.appV16AppliedHeight = req.Height
		}
		if plan.Name == appV17UpgradeName {
			app.appV17AppliedHeight = req.Height
		}
		if plan.Name == appV18UpgradeName {
			app.appV18AppliedHeight = req.Height
		}
		if plan.Name == appV19UpgradeName {
			app.appV19AppliedHeight = req.Height
		}
		if appV20CeremonyPlan {
			app.appV20AppliedHeight = req.Height
		}
		if plan.Name == appV12UpgradeName {
			app.appV12AppliedHeight = req.Height
		}
		if plan.Name == appV11UpgradeName {
			app.appV11AppliedHeight = req.Height
			// app-v11 disables the per-node SQL admin bootstrap (#36). Establish a
			// deterministic on-chain chain-admin at the activation block so the chain
			// is never left admin-less afterward (#35). Pure function of committed
			// consensus state — never per-node SQL — so every replica computes the
			// identical write and the AppHash stays in lockstep.
			app.materializeAppV11Admin(req.Height)
		}
		// Keep the PoE fork gates monotonic if this activation jumped past an
		// intermediate version (e.g. straight to app-v5) — backfill any unset
		// lower gate so postV8_4Fork ⟹ postV8_3Fork ⟹ … holds. No-op for a
		// sequentially-upgraded chain. See reconcilePoEForkMonotonicity.
		app.reconcilePoEForkMonotonicity()
		// Diagnostic for the maxSupportedAppVersion footgun: if this activation
		// commits an app version this binary has no compiled fork gate for, the
		// node WILL halt on its next CometBFT handshake (consensus version.app
		// outruns currentAppVersion()). Surface it loudly here so the operator
		// sees a clear cause instead of a cryptic handshake-version mismatch.
		// (The auto-vote readiness gate normally prevents an unsupported upgrade
		// from ever reaching quorum; this catches a hand-voted or forced plan.)
		if plan.TargetAppVersion > maxSupportedAppVersion {
			app.logger.Error().
				Str("name", plan.Name).
				Uint64("target_app_version", plan.TargetAppVersion).
				Uint64("max_supported_app_version", maxSupportedAppVersion).
				Int64("height", req.Height).
				Msg("ACTIVATED UPGRADE EXCEEDS THIS BINARY'S MAX SUPPORTED APP VERSION — node will halt on restart; deploy a binary that supports this app version")
		}
		app.logger.Info().
			Str("name", plan.Name).
			Uint64("target_app_version", plan.TargetAppVersion).
			Int64("height", req.Height).
			Msg("upgrade activated — app version takes effect at H+1")
	} else if planErr != nil && !errors.Is(planErr, store.ErrNoUpgradePlan) {
		app.logger.Error().Err(planErr).Msg("failed to read upgrade plan")
	} else if target, ok := app.appliedUpgradeTargetAtHeight(req.Height); ok {
		// Crash-replay of an activation block (v10.5.1 review finding):
		// MarkUpgradeApplied durably deleted the plan and wrote the audit
		// record DURING the original FinalizeBlock — BadgerDB commits
		// independently of ABCI Commit — so if the node crashed before Commit,
		// the replayed H_act finds no pending plan and would silently drop the
		// version.app bump that every non-crashed replica emitted. Re-emit it
		// from the audit trail. Deterministic: the audit record is committed
		// consensus state, and the regression-floor check below coincides with
		// the original execution's (currentAppVersion() already includes this
		// activation via the boot-time refresh*Fork, so target >= current iff
		// the original emitted the bump).
		if target >= app.currentAppVersion() {
			consensusParamUpdates = &cmtproto.ConsensusParams{
				Version: &cmtproto.VersionParams{App: target},
			}
			app.logger.Warn().
				Uint64("target_app_version", target).
				Int64("height", req.Height).
				Msg("re-emitting version.app bump for a replayed activation block (crash recovery)")
		}
	}

	// A replayed activation must find the app-v20 domain that was persisted
	// before the pending plan was consumed above. Missing state is consensus
	// corruption, not a condition under which the gateway may invent a domain.
	if app.appV20AppliedHeight == req.Height {
		if _, domainErr := app.governanceDelegationDomain(); domainErr != nil {
			app.logger.Error().Err(domainErr).Int64("height", req.Height).
				Msg("CRITICAL: applied app-v20 is missing its governance delegation domain")
			panic(fmt.Sprintf("sage: applied app-v20 has no governance delegation domain at height %d: %v", req.Height, domainErr))
		}
	}

	// Update state
	app.state.Height = req.Height

	// Compute deterministic AppHash under the hash rule in force at this
	// height. The rules REPLACE each other, newest first:
	//   app-v13 (narrow): excludes exactly the three SaveState bookkeeping
	//     keys — the corrected issue-#40 rule. Idle fixed point, full hash
	//     cover over gov:*/vote:*/sentinel state.
	//   app-v12 (broad, FLAWED, superseded): excludes the whole state:
	//     prefix. Selected only for the v12-era height range so those blocks
	//     replay byte-identically.
	//   legacy: everything included (hash never reaches a fixed point —
	//     the original issue-#40 behavior, kept for pre-v12 replay).
	// Each activation block H_act itself still hashes under the previous
	// rule (strict-> gates), so the flip lands at H_act+1.
	var appHash []byte
	var err error
	switch {
	case app.postAppV13Rules(req.Height):
		appHash, err = app.badgerStore.ComputeAppHashExcludingBookkeeping()
	case app.postAppV12Rules(req.Height):
		appHash, err = app.badgerStore.ComputeAppHashExcludingState()
	default:
		appHash, err = ComputeAppHash(app.badgerStore)
	}
	if err != nil {
		// A node that cannot compute the canonical hash MUST NOT invent one:
		// the old computeBlockHash fallback committed a per-node hash that
		// silently diverged from every healthy replica (review finding,
		// v10.5.1). Halting matches Commit's offchain-flush failure policy —
		// crash, let the operator fix the I/O fault, and replay the block.
		app.logger.Error().Err(err).Int64("height", req.Height).
			Msg("CRITICAL: AppHash computation failed — halting instead of committing a divergent fallback hash")
		panic(fmt.Sprintf("sage: AppHash computation failed at height %d: %v", req.Height, err))
	}
	app.state.AppHash = appHash

	metrics.FinalizeBlockDuration.Observe(time.Since(start).Seconds())

	return &abcitypes.ResponseFinalizeBlock{
		TxResults:             txResults,
		AppHash:               appHash,
		ValidatorUpdates:      valUpdates,
		ConsensusParamUpdates: consensusParamUpdates,
	}, nil
}

// processTx handles a single transaction deterministically.
func (app *SageApp) processTx(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	// app-v8: verify the tx's outer Ed25519 signature in the CONSENSUS path, not
	// only in CheckTx. CheckTx (mempool admission) is advisory — a Byzantine block
	// proposer can place txs into a block that never passed any honest node's
	// CheckTx. Without this gate FinalizeBlock would EXECUTE a forged tx (e.g. an
	// UpgradePropose or GovVote bearing a victim's PublicKey but signed by the
	// attacker), letting a single proposer forge governance proposals and votes and
	// drive the 2/3 quorum that app-v8 relies on. tx.VerifyTx is the identical check
	// CheckTx runs (app.go CheckTx) and is deterministic (EncodeTx + SHA-256 +
	// ed25519.Verify), so every replica reaches the same verdict.
	//
	// Gating: each consensus-security check rides the OR of its own gate and every
	// HIGHER independent security gate, because a higher app version subsumes the
	// lower's rules. The independent feature gates (app-v7/v8/v9) are NOT a
	// monotonic ladder and governance MAY skip-activate a higher one without the
	// lower (the upgrade-propose regression guard only checks target >
	// currentAppVersion()). If the sig-verify were gated on postAppV8Fork ALONE, a
	// chain that activated app-v9 without app-v8 would report version 9 yet skip
	// BOTH this check and the app-v9 nonce check below — silently disabling forgery
	// AND replay protection on a chain advertising the strongest hardening. So:
	//   sig-verify  fires on app-v8 OR app-v9   (forgery protection)
	//   nonce check fires on app-v9             (replay protection)
	// On every chain today appV9AppliedHeight==0, so each `|| postAppV9Fork`
	// collapses to the v9.0.0 behaviour and historical blocks replay byte-identically.
	postV9 := app.postAppV9Rules(height)
	if app.postAppV8Rules(height) {
		if valid, err := tx.VerifyTx(parsedTx); err != nil || !valid {
			return &abcitypes.ExecTxResult{Code: 2, Log: "invalid tx signature (rejected in consensus path)"}
		}
		// app-v9: enforce nonce/replay protection in the CONSENSUS path, not only
		// in CheckTx. The app-v8 sig-verification above stops a Byzantine proposer
		// from FORGING a tx; this stops it from REPLAYING a victim's own
		// previously-valid signed tx. The check is byte-for-byte the same predicate
		// CheckTx runs (app.go CheckTx) so the two paths never disagree on an
		// honest tx, and it reads only BadgerDB (GetNonce), so every replica reaches
		// the same verdict deterministically.
		//
		// Ordering safety (the reason this was deferred from app-v8): a strict
		// "nonce must exceed the highest burned" rule is order-sensitive, and the
		// in-process app-validators fire BURSTS of votes (one tx per pending memory,
		// UnixNano nonces). Under an honest proposer this is safe — CometBFT's FIFO
		// mempool reaps a single sender's txs in broadcast order, i.e. ascending
		// nonce, so every tx in the burst passes. Under a Byzantine proposer that
		// reorders a sender's txs to drop some, the proposer gains NO new power: it
		// could already drop those same txs by simply omitting them from the block
		// (censorship >= reorder-drop). So this gate only ever REJECTS true replays;
		// it never costs liveness an honest network would have had.
		if postV9 {
			// Nonce 0 is a permanently-invalid sentinel. The replay predicate below
			// disables itself when the stored nonce is 0 (the legitimate first-tx
			// exemption), and SetNonce(0) would keep it 0 forever — so a nonce-0 tx
			// would be replayable indefinitely. No production producer emits nonce 0
			// (all use MonotonicNonce / UnixNano, always large+positive), so this
			// rejects nothing that ever committed; it just closes the hole that
			// app-v9 would otherwise elevate to a consensus invariant.
			if parsedTx.Nonce == 0 {
				metrics.TxRejectedTotal.WithLabelValues("replay_nonce_consensus").Inc()
				return &abcitypes.ExecTxResult{Code: 4, Log: "nonce 0 not permitted (rejected in consensus path)"}
			}
			agentID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
			currentNonce, nerr := app.badgerStore.GetNonce(agentID)
			if nerr != nil {
				return &abcitypes.ExecTxResult{Code: 4, Log: fmt.Sprintf("nonce lookup error: %v", nerr)}
			}
			if parsedTx.Nonce <= currentNonce && currentNonce > 0 {
				metrics.TxRejectedTotal.WithLabelValues("replay_nonce_consensus").Inc()
				return &abcitypes.ExecTxResult{Code: 4, Log: fmt.Sprintf("nonce too low: got %d, expected > %d (rejected in consensus path)", parsedTx.Nonce, currentNonce)}
			}
		}
	}

	// app-v17: a REST node may outer-sign on behalf of a different agent, so
	// the historical proof check alone is insufficient: it authenticates an
	// agent but not the action. Bind the exact signed HTTP request to the
	// type-specific payload, enforce freshness using deterministic block time,
	// and consume the proof once in consensus state before any handler mutates
	// state. Same-key node-originated transactions are already action-bound by
	// the outer signature and app-v9 nonce and bypass this delegated-only gate.
	if app.postAppV17Rules(height) {
		if proofErr := app.enforceDelegatedAgentProof(parsedTx, blockTime, true, app.postAppV20Fork(height)); proofErr != nil {
			metrics.TxRejectedTotal.WithLabelValues("agent_proof_binding_consensus").Inc()
			return &abcitypes.ExecTxResult{Code: 109, Log: fmt.Sprintf("agent proof rejected: %v", proofErr)}
		}
	}

	switch parsedTx.Type {
	case tx.TxTypeMemorySubmit:
		return app.processMemorySubmit(parsedTx, height, blockTime)
	case tx.TxTypeMemoryVote:
		return app.processMemoryVote(parsedTx, height, blockTime)
	case tx.TxTypeMemoryChallenge:
		return app.processMemoryChallenge(parsedTx, height, blockTime)
	case tx.TxTypeMemoryCorroborate:
		return app.processMemoryCorroborate(parsedTx, height, blockTime)
	case tx.TxTypeAccessRequest:
		return app.processAccessRequest(parsedTx, height, blockTime)
	case tx.TxTypeAccessGrant:
		return app.processAccessGrant(parsedTx, height, blockTime)
	case tx.TxTypeAccessRevoke:
		return app.processAccessRevoke(parsedTx, height, blockTime)
	case tx.TxTypeAccessQuery:
		return app.processAccessQuery(parsedTx, height, blockTime)
	case tx.TxTypeDomainRegister:
		return app.processDomainRegister(parsedTx, height, blockTime)
	case tx.TxTypeOrgRegister:
		return app.processOrgRegister(parsedTx, height, blockTime)
	case tx.TxTypeOrgAddMember:
		return app.processOrgAddMember(parsedTx, height, blockTime)
	case tx.TxTypeOrgRemoveMember:
		return app.processOrgRemoveMember(parsedTx, height, blockTime)
	case tx.TxTypeOrgSetClearance:
		return app.processOrgSetClearance(parsedTx, height, blockTime)
	case tx.TxTypeFederationPropose:
		return app.processFederationPropose(parsedTx, height, blockTime)
	case tx.TxTypeFederationApprove:
		return app.processFederationApprove(parsedTx, height, blockTime)
	case tx.TxTypeFederationRevoke:
		return app.processFederationRevoke(parsedTx, height, blockTime)
	case tx.TxTypeDeptRegister:
		return app.processDeptRegister(parsedTx, height, blockTime)
	case tx.TxTypeDeptAddMember:
		return app.processDeptAddMember(parsedTx, height, blockTime)
	case tx.TxTypeDeptRemoveMember:
		return app.processDeptRemoveMember(parsedTx, height, blockTime)
	case tx.TxTypeAgentRegister:
		return app.processAgentRegister(parsedTx, height, blockTime)
	case tx.TxTypeAgentUpdate:
		return app.processAgentUpdate(parsedTx, height, blockTime)
	case tx.TxTypeAgentSetPermission:
		return app.processAgentSetPermission(parsedTx, height, blockTime)
	case tx.TxTypeMemoryReassign:
		return app.processMemoryReassign(parsedTx, height, blockTime)
	case tx.TxTypeGovPropose:
		return app.processGovPropose(parsedTx, height, blockTime)
	case tx.TxTypeGovVote:
		return app.processGovVote(parsedTx, height, blockTime)
	case tx.TxTypeGovCancel:
		return app.processGovCancel(parsedTx, height, blockTime)
	case tx.TxTypeUpgradePropose:
		return app.processUpgradePropose(parsedTx, height, blockTime)
	case tx.TxTypeUpgradeCancel:
		return app.processUpgradeCancel(parsedTx, height, blockTime)
	case tx.TxTypeUpgradeRevert:
		return app.processUpgradeRevert(parsedTx, height, blockTime)
	case tx.TxTypeDomainReassign:
		return app.processDomainReassign(parsedTx, height, blockTime)
	case tx.TxTypeCoCommitSubmit:
		return app.processCoCommitSubmit(parsedTx, height, blockTime)
	case tx.TxTypeCoCommitAttest:
		return app.processCoCommitAttest(parsedTx, height, blockTime)
	case tx.TxTypeCrossFedSet:
		return app.processCrossFedSet(parsedTx, height, blockTime)
	case tx.TxTypeCrossFedRevoke:
		return app.processCrossFedRevoke(parsedTx, height, blockTime)
	case tx.TxTypeMemoryReinstate:
		return app.processMemoryReinstate(parsedTx, height, blockTime)
	default:
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
}

// parseOutcomeClass extracts the outcome_class routing key from a content body.
// It is a pure function of the input bytes and is fail-SAFE, not fail-open.
//
// Two hardenings over the naive envelope decode that close a fail-open routing hole:
//   - It decodes ONLY outcome_class and ignores every sibling envelope field, so
//     a malformed sibling — a float/string/overflowing schema_version, etc. — can
//     no longer abort the whole Unmarshal and null the route. (The old struct
//     carried a `SchemaVersion int` that was never read here yet, by type-checking,
//     let any non-int value bypass the gate by forcing this to "".)
//   - It unwraps a top-level single-element JSON array first, matching the rest of
//     the body-reading stack, so a "[ {…} ]" body routes by its real class instead
//     of failing the object decode.
//
// On any error it returns "" — which, for a CLOSED domain, the registry treats as
// an unregistered class and REJECTS rather than passing through. Deterministic
// across all validators.
func parseOutcomeClass(content string) string {
	var env struct {
		OutcomeClass string `json:"outcome_class"`
	}
	if err := json.Unmarshal([]byte(unwrapSingleElementJSONArray(content)), &env); err != nil {
		return ""
	}
	return env.OutcomeClass
}

// unwrapSingleElementJSONArray returns the inner element of a top-level
// single-element JSON array ("[ {…} ]" => "{…}"); for any other shape (object,
// empty, non-JSON, multi-element array) it returns content unchanged. Pure
// function of the bytes. The router and SAGE's body readers must resolve the
// SAME routing key — a body the readers unwrap to one object would otherwise
// null the route here and bypass the gate.
func unwrapSingleElementJSONArray(content string) string {
	trimmed := strings.TrimSpace(content)
	if trimmed == "" || trimmed[0] != '[' {
		return content
	}
	var arr []json.RawMessage
	if err := json.Unmarshal([]byte(trimmed), &arr); err != nil || len(arr) != 1 {
		return content
	}
	return string(arr[0])
}

// txMemoryTypeToString converts the wire-format MemoryType (uint8) to the model string.
func txMemoryTypeToString(mt tx.MemoryType) string {
	switch mt {
	case tx.MemoryTypeFact:
		return string(memory.TypeFact)
	case tx.MemoryTypeObservation:
		return string(memory.TypeObservation)
	case tx.MemoryTypeInference:
		return string(memory.TypeInference)
	case tx.MemoryTypeTask:
		return string(memory.TypeTask)
	default:
		return string(memory.TypeFact)
	}
}

// voteDecisionToString converts the wire-format VoteDecision (uint8) to a string.
func voteDecisionToString(d tx.VoteDecision) string {
	switch d {
	case tx.VoteDecisionAccept:
		return "accept"
	case tx.VoteDecisionReject:
		return "reject"
	case tx.VoteDecisionAbstain:
		return "abstain"
	default:
		return "reject"
	}
}

// sharedDomains are reserved catch-all domain names writable by any authenticated agent.
// They are never auto-registered with an owner, so ownership cannot be "captured" on first write.
var sharedDomains = map[string]struct{}{
	"general": {},
	"self":    {},
	"meta":    {},
}

// sharedDomainPrefixes match cross-cutting domain families that follow the
// same "no single owner" semantics as the entries in sharedDomains. Any domain
// whose name begins with one of these prefixes is treated as shared and is
// never auto-registered. Used for SAGE-meta domains like `sage-debugging`,
// `sage-development`, `sage-rbac-debug`, etc., which are conceptually
// network-wide rather than agent-owned and got captured by whichever agent
// happened to write first after a chain reset (see internal post-mortem on
// post-chain-reset domain ownership capture).
var sharedDomainPrefixes = []string{
	"sage-",
}

// isSharedDomainStatic encodes the compile-time shared-domain rules (the
// explicit set + sage-* prefix). Pre-fork it's the entire decision; post-fork
// it's the first leg of the hybrid check used by SageApp.isSharedDomain.
//
// Kept as a package-level function (not a method) so it can be unit-tested
// independently of a live SageApp instance and so the pre-fork code path
// stays byte-identical to v7.1.1.
func isSharedDomainStatic(name string) bool {
	if _, ok := sharedDomains[name]; ok {
		return true
	}
	for _, p := range sharedDomainPrefixes {
		if strings.HasPrefix(name, p) {
			return true
		}
	}
	return false
}

// isSharedDomain is the v8.0 hybrid shared-domain predicate. Pre-fork it
// behaves exactly like the static rule set (so replay of pre-fork blocks is
// byte-identical to v7.1.1). Post-fork it additionally honours the on-chain
// shared_domain:<name> sentinel that TxTypeDomainReassign writes when
// OpenToShared=true — letting governance promote a domain to shared without
// shipping a binary release.
//
// height is the BLOCK height of the tx currently being processed; the
// caller passes it through so the gate stays deterministic across replicas
// (no implicit reads of mutable app state).
func (app *SageApp) isSharedDomain(name string, height int64) bool {
	if isSharedDomainStatic(name) {
		return true
	}
	if app.postV8Fork(height) {
		v, _ := app.badgerStore.GetState("shared_domain:" + name)
		if len(v) > 0 {
			return true
		}
	}
	return false
}

func (app *SageApp) processMemorySubmit(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	submit := parsedTx.MemorySubmit
	if submit == nil {
		return &abcitypes.ExecTxResult{Code: 11, Log: "missing memory submit payload"}
	}
	if app.postAppV20Fork(height) {
		if err := memorytags.ValidateCanonical(submit.Tags); err != nil {
			return &abcitypes.ExecTxResult{Code: 19, Log: "memory tags are not canonical: " + err.Error()}
		}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// app-v16: a memory MUST carry a domain. Pre-app-v16 an empty domain_tag created
	// a domainless memory (no memdomain: key) that could never be deprecated — the
	// legacy state this fork remediates. Reject it at submit so it cannot recur.
	// (The REST submit handler already requires a domain; this closes the raw-tx
	// path.) Fork-gated (postAppV16Rules, strict >) and placed BEFORE any write, so
	// pre-fork blocks replay byte-identically.
	if app.postAppV16Rules(height) && submit.DomainTag == "" {
		return &abcitypes.ExecTxResult{Code: 11, Log: "memory submit rejected: a non-empty domain_tag is required (app-v16)"}
	}

	// Domain write-access check: if the domain has a registered owner, verify the agent has write access.
	// If the domain doesn't exist, auto-register it with the submitting agent as owner.
	// Reserved shared domains (e.g. "general", "self") are writable by any authenticated agent
	// and are never auto-registered — they are conventional catch-alls without single-owner semantics.
	if submit.DomainTag != "" && !app.isSharedDomain(submit.DomainTag, height) {
		var domainOwner string
		var domainErr error
		if app.postAppV18Rules(height) {
			domainOwner, _, domainErr = app.badgerStore.ResolveOwningAncestor(submit.DomainTag)
		} else {
			domainOwner, domainErr = app.badgerStore.GetDomainOwner(submit.DomainTag)
		}
		if app.postAppV18Rules(height) && domainErr != nil {
			return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("access denied: invalid domain ownership path: %v", domainErr)}
		}
		if domainErr == nil && domainOwner != "" {
			// Domain is owned — check write access (level 2).
			postFork := app.postV8Fork(height)
			recordV8Branch(postFork)
			hasAccess := app.postAppV18Rules(height) && domainOwner == agentID
			var accessErr error
			if hasAccess {
				// app-v18: the effective owner inherently controls writes; an
				// explicit self-grant is an optimization, not an authority source.
			} else if app.postAppV18Rules(height) {
				hasAccess, accessErr = app.badgerStore.HasWriteAccessMultiOrg(submit.DomainTag, agentID, blockTime, true)
			} else {
				hasAccess, accessErr = app.badgerStore.HasAccessMultiOrg(submit.DomainTag, agentID, 0, blockTime, postFork)
			}
			if accessErr != nil || !hasAccess {
				return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("access denied: agent %s has no write access to domain %s", agentID[:16], submit.DomainTag)}
			}
		} else {
			// Domain not registered — auto-register with submitting agent as owner.
			// RegisterDomain is check-and-set: it returns ErrDomainAlreadyRegistered on race,
			// in which case we fall through to the access check on the next tx.
			if regErr := app.badgerStore.RegisterDomain(submit.DomainTag, agentID, "", height); regErr != nil {
				if !errors.Is(regErr, store.ErrDomainAlreadyRegistered) {
					app.logger.Error().Err(regErr).Str("domain", submit.DomainTag).Msg("failed to auto-register domain")
				}
			} else {
				app.logger.Info().Str("domain", submit.DomainTag).Str("owner", agentID[:16]).Msg("auto-registered domain on first memory submit")
				// Mirror the auto-register to the off-chain accessStore so
				// GET /v1/domain/{name} on a mirror-backed deployment, plus
				// any analytics/ops tooling reading Postgres directly, see
				// the domain. Without this, the auto-register path leaves
				// Badger and the mirror diverged from the moment of
				// creation — the inverse of the v7.5.3 read-side fix.
				app.pendingWrites = append(app.pendingWrites, pendingWrite{
					writeType: "domain_register",
					data: &store.DomainEntry{
						DomainName:    submit.DomainTag,
						OwnerAgentID:  agentID,
						CreatedHeight: height,
						CreatedAt:     blockTime,
					},
				})
				// Also grant the owner full access
				if grantErr := app.badgerStore.SetAccessGrant(submit.DomainTag, agentID, 2, 0, agentID); grantErr != nil {
					app.logger.Error().Err(grantErr).Str("domain", submit.DomainTag).Msg("failed to auto-grant owner access")
				} else {
					app.pendingWrites = append(app.pendingWrites, pendingWrite{
						writeType: "access_grant",
						data: &store.AccessGrantEntry{
							Domain:        submit.DomainTag,
							GranteeID:     agentID,
							GranterID:     agentID,
							Level:         2,
							CreatedHeight: height,
							CreatedAt:     blockTime,
						},
					})
				}
			}
		}
	}

	// Generate memory ID if not provided.
	memoryID := memoryIDForSubmit(submit, height, agentID)

	// #3 (reverse): a normal memory must not clobber an existing co-commit that owns
	// this id. The processCoCommitSubmit reclaim covers a normal squat being taken
	// over by a co-commit; this covers the opposite (a normal submit landing on an
	// already-co-committed SharedID). Fork-gated on postAppV15Rules so pre-fork
	// blocks replay byte-identical; also closes it on a skip-ahead chain where the
	// v8.4 terminal-status guard below is dormant.
	if app.postAppV15Rules(height) {
		if existingCore, ccErr := app.badgerStore.GetCoCommitCore(memoryID); ccErr == nil && len(existingCore) > 0 {
			return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("memory %s is a co-committed id and cannot be overwritten by a normal submit", memoryID)}
		}
	}

	// Store hash on-chain (BadgerDB)
	contentHash := submit.ContentHash
	if len(contentHash) == 0 {
		ch := sha256.Sum256([]byte(submit.Content))
		contentHash = ch[:]
	}

	// v8.4: a memoryID that already reached a terminal verdict (committed or
	// deprecated) must not be re-opened. Pre-v8.4, processMemorySubmit reset the
	// on-chain status to proposed unconditionally, so re-submitting an existing
	// ID rewound a committed memory to proposed; a fresh vote then re-reached
	// quorum and the verdict-correctness EWMA / corroboration credit fired AGAIN
	// — a repeatable reputation-gaming vector once v8.3 made those credits matter
	// (poe-drift audit finding). Post-fork the proposed->terminal transition is
	// one-way: reject the re-submit. Re-submitting a still-proposed memory stays
	// allowed (legitimate re-broadcast). Fork-gated so pre-v8.4 blocks replay
	// byte-identical.
	if app.postV8_4Fork(height) {
		if _, st, gErr := app.badgerStore.GetMemoryHash(memoryID); gErr == nil &&
			(st == string(memory.StatusCommitted) || st == string(memory.StatusDeprecated)) {
			return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf(
				"memory %s already reached terminal status %q; re-submit rejected", memoryID, st)}
		}
	}

	memType := txMemoryTypeToString(submit.MemoryType)

	// Layer-2 content-aware schema gate. Deterministic: pure function of submit
	// bytes + frozen in-binary schemas + read-only Badger lookups. Runs ONLY in
	// FinalizeBlock so the reject is byte-identical on every validator and feeds
	// ComputeAppHash. Enforcement is a pure function of consensus state (the
	// app-v7 fork height) AND a compiled-in registry — no runtime enable flag —
	// so it is replay-safe and cannot be toggled per-node out of band. Placed
	// BEFORE SetMemoryHash so a rejected record never mutates Badger / the AppHash.
	if app.postAppV7Fork(height) && app.contentValidators != nil {
		recView := &memory.MemoryRecord{
			MemoryID:        memoryID,
			SubmittingAgent: agentID,
			Content:         submit.Content,
			ContentHash:     contentHash,
			MemoryType:      memory.MemoryType(memType),
			DomainTag:       submit.DomainTag,
			ConfidenceScore: submit.ConfidenceScore,
		}
		outcomeClass := parseOutcomeClass(submit.Content)
		if rejected, reason := app.contentValidators.Validate(submit.DomainTag, outcomeClass, recView); rejected {
			return &abcitypes.ExecTxResult{
				Code: 18,
				Log:  fmt.Sprintf("content schema rejected for (%s,%s): %s", submit.DomainTag, outcomeClass, reason),
			}
		}
	}

	// app-v20: an active exact-domain scope pins its roster and canonical
	// recoverable envelope in the same Badger transaction as the ordinary memory
	// record. Unscoped domains retain the byte-identical historical path.
	scopedSubmission := false
	if app.postAppV20Fork(height) {
		var scopeErr error
		scopedSubmission, scopeErr = app.setScopedMemorySubmission(submit, memoryID, agentID, contentHash, height, blockTime)
		if scopeErr != nil {
			return &abcitypes.ExecTxResult{Code: 19, Log: "scoped memory submit rejected: " + scopeErr.Error()}
		}
	}

	if !scopedSubmission {
		if setErr := app.badgerStore.SetMemoryHash(memoryID, contentHash, string(memory.StatusProposed)); setErr != nil {
			return &abcitypes.ExecTxResult{Code: 12, Log: fmt.Sprintf("badger write error: %v", setErr)}
		}
	}

	// v8.4 records the memory's domain on-chain so checkAndApplyQuorum can read it
	// deterministically at verdict time. app-v16+ also requires the record because
	// challenge/forget authorization is domain-based and app-v17 may be activated
	// independently of v8.4. Both gates are strict >, keeping historical blocks and
	// each activation block byte-identical. Shared domains are recorded too — the
	// quorum decides per-vote whether to use domain-conditional weight or the scalar.
	if !scopedSubmission && submit.DomainTag != "" && app.shouldRecordMemoryDomain(height) {
		if domErr := app.badgerStore.SetMemoryDomain(memoryID, submit.DomainTag); domErr != nil {
			app.logger.Error().Err(domErr).Str("memory_id", memoryID).Msg("set memory domain")
		}
	}

	// app-v10: record the memory's author (submitting agent) on-chain so the
	// corroboration guard can reject self-corroboration deterministically — the
	// memory:<id> record stores only contentHash+status, not the author. Written
	// IMMUTABLY (only when unset): a still-proposed memory may be re-submitted
	// (legitimate re-broadcast), and a client-supplied memoryID could be reused by
	// a different agent, so overwriting would let a re-submitter displace the
	// original author and then slip that original author past the self-check.
	// First post-fork writer wins. Post-fork only; the strict-> gate keeps pre-fork
	// blocks + the activation block byte-identical (no memauthor: key enters the
	// AppHash keyspace until H_act+1).
	if !scopedSubmission && app.postAppV10Rules(height) {
		if existing, gErr := app.badgerStore.GetMemoryAuthor(memoryID); gErr == nil && existing == "" {
			if authErr := app.badgerStore.SetMemoryAuthor(memoryID, agentID); authErr != nil {
				app.logger.Error().Err(authErr).Str("memory_id", memoryID).Msg("app-v10 set memory author")
			}
		}
	}

	// Buffer PostgreSQL write for Commit — this is the ONLY path that writes
	// memories to the offchain store, enforcing consensus-first ordering.
	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: agentID,
		Content:         submit.Content,
		ContentHash:     contentHash,
		EmbeddingHash:   submit.EmbeddingHash,
		MemoryType:      memory.MemoryType(memType),
		DomainTag:       submit.DomainTag,
		ConfidenceScore: submit.ConfidenceScore,
		Status:          memory.StatusProposed,
		ParentHash:      submit.ParentHash,
		TaskStatus:      memory.TaskStatus(submit.TaskStatus),
		CreatedAt:       blockTime,
	}

	// Enrich with off-chain supplementary data (embedding vector, provider,
	// knowledge triples) that the REST handler cached before broadcasting.
	// Only the node that received the REST request will have this data.
	var suppTriples []memory.KnowledgeTriple
	if app.SuppCache != nil {
		if supp := app.SuppCache.Pop(memoryID); supp != nil {
			record.Embedding = supp.Embedding
			record.Provider = supp.Provider
			record.Assignee = supp.Assignee
			record.EmbeddingProvider = supp.EmbeddingProvider
			if len(supp.EmbeddingHash) > 0 {
				record.EmbeddingHash = supp.EmbeddingHash
			}
			suppTriples = supp.KnowledgeTriples
		}
	}

	// Memory must be inserted before triples (FK constraint: knowledge_triples.memory_id → memories).
	app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "memory", data: record})
	if scopedSubmission && len(submit.Tags) > 0 {
		app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "memory_tags", data: &memoryTagsData{
			MemoryID: memoryID,
			Tags:     append([]string(nil), submit.Tags...),
		}})
	}

	if len(suppTriples) > 0 {
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "triples",
			data:      &triplesData{MemoryID: memoryID, Triples: suppTriples},
		})
	}

	// Store memory classification on-chain. v6.8.6: caller's classification
	// is honored verbatim — a submitted value of 0 means PUBLIC (cross-org
	// readable subject to domain-access checks), not "missing → INTERNAL".
	// Prior versions silently bumped 0→INTERNAL here, which combined with
	// the per-record classification gate in handleQueryMemory to drop every
	// cross-agent read where the reader had no shared org with the writer —
	// even when visible_agents="*" had been granted. Wire-format backward
	// compat for old txs that omit the classification byte still defaults
	// to INTERNAL in tx/codec.go decodeMemorySubmit.
	classification := uint8(submit.Classification)
	if !scopedSubmission {
		if classErr := app.badgerStore.SetMemoryClassification(memoryID, classification); classErr != nil {
			app.logger.Error().Err(classErr).Str("memory_id", memoryID).Msg("failed to set memory classification")
		}
	}

	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "mem_classification",
		data: &memClassificationData{
			MemoryID:       memoryID,
			Classification: store.ClearanceLevel(classification),
		},
	})

	metrics.MemoriesTotal.WithLabelValues(memType, submit.DomainTag, "proposed").Inc()

	return &abcitypes.ExecTxResult{
		Code: 0,
		Data: []byte(memoryID),
		Log:  fmt.Sprintf("memory %s submitted", memoryID),
	}
}

// memoryIDForSubmit is shared by normal execution and the app-v20 exact-block
// crash-replay recognizer. Keep this byte-for-byte equivalent to the historical
// derivation: it intentionally hashes the submitted ContentHash field before
// processMemorySubmit's empty-hash fallback.
func memoryIDForSubmit(submit *tx.MemorySubmit, height int64, agentID string) string {
	if submit.MemoryID != "" {
		return submit.MemoryID
	}
	h := sha256.Sum256([]byte(fmt.Sprintf("%x:%d:%s", submit.ContentHash, height, agentID)))
	return hex.EncodeToString(h[:16])
}

// maxCoCommitCoauthors caps the coauthor count on a single co-commit envelope.
// Each coauthor costs one ed25519.Verify on the FinalizeBlock consensus hot path,
// so an uncapped count is an asymmetric liveness-DoS (CheckTx does O(1) work).
// 64 is generous for real collaboration (family/dept/org circles); org-scale
// N-party fan-out is the deferred star/Merkle anchoring design, not one envelope.
const maxCoCommitCoauthors = 64

// A reclaimed normal-memory squat may have accumulated arbitrarily many stale
// vote keys. Above app-v20 the complete block shares one bounded Badger
// transaction, so opportunistic hygiene must not let one co-commit consume the
// transaction budget. The terminal-status guard already prevents leftover
// votes from changing the committed co-commit; later maintenance can remove
// them outside this consensus transition.
const maxAppV20CoCommitStaleVotePrune = 64

// Domain reassignment must invalidate every old grant. Truncation would be a
// security bug, so app-v20 preflights and rejects an oversized grant set before
// changing ownership; operators can revoke/drain it over earlier blocks.
const maxAppV20DomainReassignGrantPurge = 256

// maxRemoteChainIDLen mirrors CometBFT's MaxChainIDLen — a cross_fed remote_chain_id
// can never be longer than a real genesis ChainID.
const maxRemoteChainIDLen = 50

// isAllZeroBytes reports whether every byte is zero — used to reject an all-zero
// ed25519 pubkey (a small-order point that can pass Verify for crafted messages),
// mirroring the guard in VerifyAgentProof.
func isAllZeroBytes(b []byte) bool {
	for _, x := range b {
		if x != 0 {
			return false
		}
	}
	return true
}

// processCoCommitSubmit commits a jointly-signed co-commit envelope (tx 31) as a
// NATIVE local memory on THIS chain, keyed by the content-derived, height-free
// SharedID and cross-anchorable by peers. It clones processMemorySubmit's write
// discipline with two differences: (1) the LOCAL submitter passes local authz,
// while every FOREIGN coauthor is verified by STANDALONE ed25519 signature only
// (never through HasAccessMultiOrg/ListAgentOrgs — a foreign pubkey has no local
// org membership and would correctly fail); (2) the id is the SharedID, not a
// height-derived id, so both chains agree on it before either commits.
// Dual-gated on postAppV15Fork; every reject returns BEFORE any state write.
func (app *SageApp) processCoCommitSubmit(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	// GATE 2 (exec reject): before ANY payload deref / state read / nonce effect.
	postV15 := app.postAppV15Fork(height)
	recordAppV15Branch(postV15)
	if !postV15 {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}

	env := parsedTx.CoCommitSubmit
	if env == nil {
		return &abcitypes.ExecTxResult{Code: 93, Log: "missing CoCommitSubmit payload"}
	}

	// app-v16: a co-committed memory MUST carry a domain, same as processMemorySubmit.
	// Otherwise it lands as a domainless committed record (SetMemoryHash below writes
	// no memdomain: when env.Domain=="") that can never be deprecated — recreating the
	// legacy state this fork remediates. Reject BEFORE any write. Fork-gated
	// (postAppV16Rules, strict >) so pre-fork blocks replay byte-identically.
	if app.postAppV16Rules(height) && env.Domain == "" {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit rejected: a non-empty domain is required (app-v16)"}
	}

	// LOCAL submitter identity (the node broadcasting to its own chain).
	localID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 94, Log: fmt.Sprintf("co-commit: agent identity verification failed: %v", err)}
	}

	if len(env.Coauthors) == 0 {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: envelope has no coauthors"}
	}
	if len(env.ContentHash) == 0 {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: envelope has no content hash"}
	}
	// #5: cap coauthor count BEFORE the per-coauthor verify loop (one ed25519.Verify
	// each on the FinalizeBlock hot path; CheckTx does O(1) — an uncapped count is an
	// asymmetric liveness DoS). Independent of the codec's allocation bound.
	if len(env.Coauthors) > maxCoCommitCoauthors {
		return &abcitypes.ExecTxResult{Code: 95, Log: fmt.Sprintf("co-commit: too many coauthors (%d > %d)", len(env.Coauthors), maxCoCommitCoauthors)}
	}
	// #4: a co-commit is inherently multi-party — require >=2 DISTINCT coauthor
	// pubkeys and reject in-envelope duplicates. Without this, a single self-coauthor
	// envelope degenerates into a voter-skipping single-node direct-commit (P1 alone
	// permits len==1). Combined with P1 (submitter is a coauthor), >=2 distinct
	// guarantees >=1 genuine foreign party. Membership-only map use (no iteration) —
	// deterministic.
	seenPub := make(map[string]struct{}, len(env.Coauthors))
	for _, c := range env.Coauthors {
		k := string(c.PubKey)
		if _, dup := seenPub[k]; dup {
			return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: duplicate coauthor pubkey"}
		}
		seenPub[k] = struct{}{}
	}
	if len(seenPub) < 2 {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: requires at least 2 distinct coauthors"}
	}
	// P2: only schema version 1 exists in the v11.0 MVP. Reject unknown versions now
	// — before any version-gated interpretation ships — rather than persisting an
	// uninterpretable envelope into the AppHash.
	if env.SchemaVersion != 1 {
		return &abcitypes.ExecTxResult{Code: 95, Log: fmt.Sprintf("co-commit: unsupported schema version %d", env.SchemaVersion)}
	}

	// Verify EVERY coauthor's standalone ed25519 signature over CanonicalCoreBytes.
	// No registration lookup — foreign coauthor authority was established once on
	// their home chain and is proven here by signature alone (self-attesting).
	core := tx.CanonicalCoreBytes(env)
	for _, c := range env.Coauthors {
		if len(c.PubKey) != ed25519.PublicKeySize || len(c.Sig) != ed25519.SignatureSize || isAllZeroBytes(c.PubKey) {
			return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: malformed coauthor proof"}
		}
		if !auth.Verify(ed25519.PublicKey(c.PubKey), core, c.Sig) {
			return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: coauthor signature invalid"}
		}
	}

	// P1: the LOCAL submitter must itself be one of the coauthors — you commit what
	// you co-signed. Binds the relay to a participant so a non-party cannot replay a
	// jointly-signed envelope onto an unrelated chain. (localID = hex(AgentPubKey);
	// the full cross-network freshness/federation-status bind is deferred footgun E.)
	localIsCoauthor := false
	for _, c := range env.Coauthors {
		if hex.EncodeToString(c.PubKey) == localID {
			localIsCoauthor = true
			break
		}
	}
	if !localIsCoauthor {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit: submitter is not one of the coauthors"}
	}

	// Bind the id to the SIGNED core: recompute the content-derived SharedID and
	// require it to match the envelope's claim (rejects a spoofed/renamed id).
	coreHash := tx.CoreHashOf(env)
	if env.SharedID != tx.ComputeSharedID(coreHash, env.Coauthors, env.AgreementNonce) {
		return &abcitypes.ExecTxResult{Code: 96, Log: "co-commit: SharedID does not match signed core"}
	}
	sharedID := env.SharedID

	// Footgun E — freshness + federation-status bind (both determinism-safe: only
	// the block's own deterministic blockTime and on-chain cross_fed state, never
	// time.Now). (1) Enforce the jointly-signed validity window; NotBefore/NotAfter
	// live in CanonicalCoreBytes so every coauthor committed to them. (2) Reject if
	// ANY coauthor's home chain has a cross_fed record that is not "active"
	// (revoked). Together these stop a stale counter-signed envelope from being
	// resurrected after a federation is rotated (window lapses) or revoked (status).
	// Note: a coauthor whose chain has NO cross_fed record here — including the LOCAL
	// coauthor, since a chain never self-federates — is not rejected; the app has no
	// deterministic self-chain_id to require a positive "active", so this gate is the
	// determinism-safe "reject if explicitly revoked", not "require active".
	bt := blockTime.Unix()
	if env.NotBefore != 0 && bt < env.NotBefore {
		return &abcitypes.ExecTxResult{Code: 98, Log: "co-commit: envelope not yet valid (NotBefore)"}
	}
	if env.NotAfter != 0 && bt >= env.NotAfter {
		return &abcitypes.ExecTxResult{Code: 98, Log: "co-commit: envelope validity window expired (NotAfter)"}
	}
	for _, c := range env.Coauthors {
		if _, _, _, _, _, _, status, gErr := app.badgerStore.GetCrossFed(c.ChainID); gErr == nil && status != "active" {
			return &abcitypes.ExecTxResult{Code: 98, Log: fmt.Sprintf("co-commit: coauthor chain %s federation not active (%s)", c.ChainID, status)}
		}
	}

	// LOCAL authz for the LOCAL submitter only, on the envelope's domain (foreign
	// coauthors are NOT run through this). Mirror processMemorySubmit's owned-domain
	// write gate; auto-register an unowned, non-shared domain to the local submitter.
	if env.Domain != "" && !app.isSharedDomain(env.Domain, height) {
		var domainOwner string
		var domainErr error
		if app.postAppV18Rules(height) {
			domainOwner, _, domainErr = app.badgerStore.ResolveOwningAncestor(env.Domain)
		} else {
			domainOwner, domainErr = app.badgerStore.GetDomainOwner(env.Domain)
		}
		if app.postAppV18Rules(height) && domainErr != nil {
			return &abcitypes.ExecTxResult{Code: 97, Log: fmt.Sprintf("co-commit: invalid domain ownership path: %v", domainErr)}
		}
		if domainErr == nil && domainOwner != "" {
			hasAccess := app.postAppV18Rules(height) && domainOwner == localID
			var accessErr error
			if hasAccess {
				// See processMemorySubmit: ownership itself is sufficient authority.
			} else if app.postAppV18Rules(height) {
				hasAccess, accessErr = app.badgerStore.HasWriteAccessMultiOrg(env.Domain, localID, blockTime, true)
			} else {
				hasAccess, accessErr = app.badgerStore.HasAccessMultiOrg(env.Domain, localID, 0, blockTime, app.postAppV8Rules(height))
			}
			if accessErr != nil || !hasAccess {
				return &abcitypes.ExecTxResult{Code: 97, Log: fmt.Sprintf("co-commit: agent %s has no write access to domain %s", localID[:16], env.Domain)}
			}
		} else {
			// Domain not registered — auto-register with the local submitter as owner.
			// M1: mirror processMemorySubmit — ALSO create the owner's level-2
			// self-grant and mirror both writes off-chain, or an org-less owner is
			// locked out of their own domain on every subsequent write (HasAccessMultiOrg
			// has no owner shortcut).
			if regErr := app.badgerStore.RegisterDomain(env.Domain, localID, "", height); regErr != nil {
				if !errors.Is(regErr, store.ErrDomainAlreadyRegistered) {
					app.logger.Error().Err(regErr).Str("domain", env.Domain).Msg("co-commit: auto-register domain")
				}
			} else {
				app.pendingWrites = append(app.pendingWrites, pendingWrite{
					writeType: "domain_register",
					data: &store.DomainEntry{
						DomainName:    env.Domain,
						OwnerAgentID:  localID,
						CreatedHeight: height,
						CreatedAt:     blockTime,
					},
				})
				if grantErr := app.badgerStore.SetAccessGrant(env.Domain, localID, 2, 0, localID); grantErr != nil {
					app.logger.Error().Err(grantErr).Str("domain", env.Domain).Msg("co-commit: auto-grant owner access")
				} else {
					app.pendingWrites = append(app.pendingWrites, pendingWrite{
						writeType: "access_grant",
						data: &store.AccessGrantEntry{
							Domain:        env.Domain,
							GranteeID:     localID,
							GranterID:     localID,
							Level:         2,
							CreatedHeight: height,
							CreatedAt:     blockTime,
						},
					})
				}
			}
		}
	}

	// app-v20: co-commit has its own immediate-commit authority model and cannot
	// safely inherit a scope ballot without a new jointly-signed envelope schema.
	// Refuse it for an active/paused scoped domain so it cannot bypass the pinned
	// scoped quorum path. Retired scopes release their mapping and resume the
	// ordinary unscoped lifecycle by design.
	if err := app.rejectScopedCoCommit(env.Domain, height); err != nil {
		return &abcitypes.ExecTxResult{Code: 97, Log: "co-commit: " + err.Error()}
	}

	// #3 namespace-collision defense + resubmit guard. SharedID is content-derived,
	// height-free, and published in the CommitReceipt, so it is publicly predictable
	// — an attacker can front-run a NORMAL memory into memory:<sharedID> to try to
	// block or hijack the co-commit. Distinguish the two cases by cocommit:core:
	//   - cocommit:core:<sharedID> present ⇒ THIS chain already co-committed this
	//     SharedID ⇒ reject the (idempotent) re-submit.
	//   - absent but memory:<sharedID> present ⇒ a front-run SQUAT. The co-commit is
	//     the cryptographic owner of this id (sha256 preimage resistance means no
	//     attacker can target an arbitrary existing id, only this predictable one),
	//     so it RECLAIMS the slot below — overwriting the squat's content + author.
	//     This defeats both the denial and the hijack variants of the collision.
	if existingCore, ccErr := app.badgerStore.GetCoCommitCore(sharedID); ccErr == nil && len(existingCore) > 0 {
		return &abcitypes.ExecTxResult{Code: 98, Log: fmt.Sprintf("co-commit %s already committed on this chain", sharedID)}
	}

	// app-v17 (C5): squat-reclaim stale-vote hygiene. The cocommit:core guard above
	// proved this SharedID was NOT previously co-committed, so a pre-existing
	// memory:<sharedID> is a front-run squat that the reclaim below overwrites to
	// committed. If that squat was submitted as a normal proposed memory it may have
	// accrued vote:<sharedID>:* keys, which survive as orphaned consensus state
	// forever (no consensus-path cleanup exists). Clear them here. Above app-v20,
	// clear only a deterministic prefix so cleanup cannot exhaust the block-wide
	// Badger transaction; remaining keys are inert under the terminal-status
	// guard. This is
	// defense-in-depth behind the app-v15 priorStatus==proposed terminal-write guard
	// in checkAndApplyQuorum, which already stops a stale vote from re-flipping the
	// reclaimed (now committed) memory. Deterministic: PrefixKeys returns the keys
	// lexicographically sorted and deletion order is AppHash-neutral. Gated
	// postAppV17Rules (strict >), so a reclaim on a pre-fork/never-activated chain
	// leaves every vote key exactly as today ⇒ byte-identical replay.
	if app.postAppV17Rules(height) {
		if _, _, mhErr := app.badgerStore.GetMemoryHash(sharedID); mhErr == nil {
			votePruneLimit := 0
			if app.postAppV20Fork(height) {
				votePruneLimit = maxAppV20CoCommitStaleVotePrune
			}
			voteKeys, vkErr := app.badgerStore.PrefixKeysLimit("vote:"+sharedID+":", votePruneLimit)
			if vkErr != nil {
				return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit: stale-vote scan error: %v", vkErr)}
			}
			for _, vk := range voteKeys {
				if delErr := app.badgerStore.DeleteState(vk); delErr != nil {
					return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit: stale-vote clear error: %v", delErr)}
				}
			}
		}
	}

	// Write the native local memory keyed by SharedID, COMMITTED immediately. A
	// co-commit is already ratified by every coauthor's signature (verified above),
	// and its inclusion in this block IS BFT consensus — the same "block inclusion
	// is decisive" logic as processMemoryChallenge. It must NOT go through the local
	// content-quality quorum voter: the envelope carries only ContentHash (no raw
	// content), so the voter's qualityCheck would REJECT it (content < 20 chars) and
	// deprecate it one block later — the feature would never commit. Deterministic
	// across replicas; the voter's GetPendingByDomain returns proposed only, so a
	// committed co-commit is never picked up.
	if setErr := app.badgerStore.SetMemoryHash(sharedID, env.ContentHash, string(memory.StatusCommitted)); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit: badger write error: %v", setErr)}
	}
	if env.Domain != "" {
		if domErr := app.badgerStore.SetMemoryDomain(sharedID, env.Domain); domErr != nil {
			app.logger.Error().Err(domErr).Str("memory_id", sharedID).Msg("co-commit: set memory domain")
		}
	}
	// memauthor = LOCAL submitter; foreign coauthors are recorded in
	// cocommit:coauthors, not memauthor. Written UNCONDITIONALLY (not first-writer-
	// wins): a genuine co-commit re-submit is already rejected above, so the only
	// paths here are a fresh co-commit or a squat-reclaim (#3) — in the reclaim case
	// the author MUST be overwritten to localID to correct the squatter's record.
	if authErr := app.badgerStore.SetMemoryAuthor(sharedID, localID); authErr != nil {
		app.logger.Error().Err(authErr).Str("memory_id", sharedID).Msg("co-commit: set memory author")
	}
	classification := uint8(env.Classification)
	if classErr := app.badgerStore.SetMemoryClassification(sharedID, classification); classErr != nil {
		app.logger.Error().Err(classErr).Str("memory_id", sharedID).Msg("co-commit: set memory classification")
	}

	// Write the co-commit cross-anchor keys (pure functions of tx bytes -> AppHash).
	if wErr := app.badgerStore.SetCoCommitShared(sharedID, env.SchemaVersion); wErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit: badger write error: %v", wErr)}
	}
	if wErr := app.badgerStore.SetCoCommitCore(sharedID, coreHash); wErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit: badger write error: %v", wErr)}
	}
	if wErr := app.badgerStore.SetCoCommitCoauthors(sharedID, tx.EncodeCoauthorsCanonical(env.Coauthors)); wErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit: badger write error: %v", wErr)}
	}

	// Buffer the off-chain memory + classification writes (consensus-first; flush
	// in Commit). Content is not in the envelope (only its hash); the REST layer
	// supplies embedding/provider via SuppCache where available.
	record := &memory.MemoryRecord{
		MemoryID:        sharedID,
		SubmittingAgent: localID,
		ContentHash:     env.ContentHash,
		MemoryType:      memory.MemoryType(txMemoryTypeToString(env.MemoryType)),
		DomainTag:       env.Domain,
		ConfidenceScore: env.ConfidenceScore,
		Status:          memory.StatusCommitted, // committed at submit (see SetMemoryHash above)
		CreatedAt:       blockTime,
	}
	if app.SuppCache != nil {
		if supp := app.SuppCache.Pop(sharedID); supp != nil {
			record.Embedding = supp.Embedding
			record.Provider = supp.Provider
			record.EmbeddingProvider = supp.EmbeddingProvider
			if len(supp.EmbeddingHash) > 0 {
				record.EmbeddingHash = supp.EmbeddingHash
			}
			// Backfill raw content into the OFF-chain record only ("where
			// available" — same per-node asymmetry as embeddings). The badger/
			// AppHash writes above saw only env.ContentHash and are untouched.
			// Hash-gated so a buggy/malicious local caller can't make the
			// off-chain mirror disagree with the jointly-signed envelope.
			if supp.Content != "" {
				if h := sha256.Sum256([]byte(supp.Content)); bytes.Equal(h[:], env.ContentHash) {
					record.Content = supp.Content
				}
			}
		}
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "memory", data: record})
	// #1/#2: the memory INSERT does not write committed_at; only UpdateStatus (driven
	// by a status_update pendingWrite) does. The normal quorum path emits one on
	// commit — mirror it here so a co-committed row gets committed_at = blockTime
	// instead of NULL. Also converts a reclaimed squat's off-chain row to committed.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "status_update",
		data: &statusUpdate{
			MemoryID: sharedID,
			Status:   memory.StatusCommitted,
			At:       blockTime,
		},
	})
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "mem_classification",
		data: &memClassificationData{
			MemoryID:       sharedID,
			Classification: store.ClearanceLevel(classification),
		},
	})

	return &abcitypes.ExecTxResult{
		Code: 0,
		Data: []byte(sharedID),
		Log:  fmt.Sprintf("co-commit %s committed (%d coauthors)", sharedID, len(env.Coauthors)),
	}
}

// processCoCommitAttest records a peer's signed CommitReceipt (tx 32) as a
// cross-anchor for a co-committed memory. Three binds, all fail-closed: (1) the
// receipt is validly signed by PeerPubKey; (2) PeerPubKey is a DECLARED coauthor
// for PeerChainID in the SharedID's recorded coauthor set (so an attacker can't
// sign over the public CoreHash with a throwaway key); (3) the receipt's CoreHash
// matches this chain's recorded shared core. The full cross-network validator-set
// / federation-status verification remains deferred (footgun E). Determinism
// (footgun T): the receipt enters the chain ONLY as verbatim signed bytes;
// CommitTime is opaque DATA, never compared to blockTime or used as a branch.
func (app *SageApp) processCoCommitAttest(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	postV15 := app.postAppV15Fork(height)
	recordAppV15Branch(postV15)
	if !postV15 {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}

	att := parsedTx.CoCommitAttest
	if att == nil {
		return &abcitypes.ExecTxResult{Code: 93, Log: "missing CoCommitAttest payload"}
	}
	if len(att.PeerPubKey) != ed25519.PublicKeySize || len(att.PeerSig) != ed25519.SignatureSize || isAllZeroBytes(att.PeerPubKey) {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit attest: malformed peer proof"}
	}
	// The peer validator must have signed the verbatim receipt bytes.
	if !auth.Verify(ed25519.PublicKey(att.PeerPubKey), att.Receipt, att.PeerSig) {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit attest: peer signature invalid"}
	}
	// Bind on the SIGNED receipt's fields, not the unsigned convenience copies.
	receipt, decErr := tx.DecodeCommitReceipt(att.Receipt)
	if decErr != nil {
		return &abcitypes.ExecTxResult{Code: 96, Log: fmt.Sprintf("co-commit attest: bad receipt: %v", decErr)}
	}
	if receipt.SharedID != att.SharedID || receipt.ChainID != att.PeerChainID {
		return &abcitypes.ExecTxResult{Code: 96, Log: "co-commit attest: receipt SharedID/ChainID mismatch"}
	}
	// Fail-closed CoreHash bind: this chain must hold the same shared core.
	localCore, coreErr := app.badgerStore.GetCoCommitCore(att.SharedID)
	if coreErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit attest: badger read error: %v", coreErr)}
	}
	if len(localCore) == 0 {
		return &abcitypes.ExecTxResult{Code: 97, Log: fmt.Sprintf("co-commit attest: no local co-commit for SharedID %s", att.SharedID)}
	}
	if !bytes.Equal(localCore, receipt.CoreHash) {
		return &abcitypes.ExecTxResult{Code: 97, Log: "co-commit attest: receipt CoreHash does not match local core"}
	}
	// H2: bind the attesting peer key to a DECLARED coauthor for that chain. The
	// CoreHash is public on-chain state (a pure function of the tx-31 envelope), so
	// without this bind an attacker could sign a receipt over it with any throwaway
	// key and forge a cross-anchor claiming a peer that never committed. Require
	// (PeerPubKey, PeerChainID) to appear in the SharedID's recorded coauthor set.
	// (The full cross-network validator-set / federation-status bind is deferred
	// footgun E; this is the minimal in-scope peer-identity check.)
	caBlob, caErr := app.badgerStore.GetCoCommitCoauthors(att.SharedID)
	if caErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit attest: badger read error: %v", caErr)}
	}
	coauthors, caDecErr := tx.DecodeCoauthorsCanonical(caBlob)
	if caDecErr != nil {
		return &abcitypes.ExecTxResult{Code: 96, Log: fmt.Sprintf("co-commit attest: coauthor decode error: %v", caDecErr)}
	}
	peerIsCoauthor := false
	for _, c := range coauthors {
		if c.ChainID == att.PeerChainID && bytes.Equal(c.PubKey, att.PeerPubKey) {
			peerIsCoauthor = true
			break
		}
	}
	if !peerIsCoauthor {
		return &abcitypes.ExecTxResult{Code: 95, Log: "co-commit attest: peer key is not a recorded coauthor for its chain"}
	}

	// Store sha256(verbatim receipt) as the cross-anchor. Idempotent, late-bindable.
	anchor := sha256.Sum256(att.Receipt)
	if wErr := app.badgerStore.SetCoCommitAnchor(att.SharedID, att.PeerChainID, anchor[:]); wErr != nil {
		return &abcitypes.ExecTxResult{Code: 99, Log: fmt.Sprintf("co-commit attest: badger write error: %v", wErr)}
	}
	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("co-commit %s anchored to peer %s", att.SharedID, att.PeerChainID)}
}

// crossFedAuthorized reports whether senderID may set/revoke Mode-1 exchange terms.
// Two tiers, BOTH reachable by solo/org-less nodes (do NOT clone isOrgAdmin, which
// 403s a personal chain — plan §9.7): the on-chain chain-admin (global role
// "admin", materialized on every chain incl. solo), OR the owner/ancestor-owner of
// EVERY concrete domain the terms scope to. A wildcard/all-domains ("*") agreement
// is a chain-level treaty → chain-admin only. Pure read-only Badger — deterministic.
func (app *SageApp) crossFedAuthorized(senderID string, allowedDomains []string) bool {
	if a, err := app.badgerStore.GetRegisteredAgent(senderID); err == nil && a != nil && a.Role == "admin" {
		return true
	}
	if len(allowedDomains) == 0 {
		return false
	}
	for _, d := range allowedDomains {
		if d == "*" || d == "" {
			return false // all-domains treaty requires chain-admin
		}
		owns, err := app.badgerStore.IsDomainOwnerOrAncestor(d, senderID)
		if err != nil || !owns {
			return false
		}
	}
	return true
}

// processCrossFedSet sets/updates (idempotent upsert) this chain's Mode-1 exchange
// terms for a remote chain (tx 33). Dual-gated on postAppV15Fork. The record is a
// unilateral LOCAL declaration authorized by a local admin — the foreign
// counterparty cannot sign a local approve; mutual trust is established
// off-consensus at the mTLS layer via the pinned PeerPubKey (transport phase).
// Determinism: no time.Now; ExpiresAt is stored as DATA; every reject returns
// before the single Badger write.
func (app *SageApp) processCrossFedSet(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	postV15 := app.postAppV15Fork(height)
	recordAppV15Branch(postV15)
	if !postV15 {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	t := parsedTx.CrossFedTerms
	if t == nil {
		return &abcitypes.ExecTxResult{Code: 100, Log: "missing CrossFedTerms payload"}
	}
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 101, Log: fmt.Sprintf("cross_fed: agent identity verification failed: %v", err)}
	}
	if t.RemoteChainID == "" || len(t.RemoteChainID) > maxRemoteChainIDLen {
		return &abcitypes.ExecTxResult{Code: 102, Log: "cross_fed: invalid remote_chain_id"}
	}
	if t.Endpoint == "" || len(t.PeerPubKey) == 0 {
		return &abcitypes.ExecTxResult{Code: 103, Log: "cross_fed: missing endpoint or peer key"}
	}
	if t.Status != "active" {
		return &abcitypes.ExecTxResult{Code: 104, Log: "cross_fed: status must be active for a set tx"}
	}
	// NOTE: the self-federation guard (remote_chain_id == our own chain_id) is
	// enforced OFF-consensus — at the tx-builder (REST/CLI) and the transport-phase
	// query proxy, where the local chain_id is freely available. It is deliberately
	// NOT a consensus rule: the ABCI app has no reliable, deterministic source for
	// its own chain_id after a restart across ALL deployment modes (amid's
	// standalone ABCI-server mode has no genesis file to read, and persisting
	// req.ChainId would shift the height-1 AppHash), so a handler-side check would
	// risk validators diverging. A self-referential terms record is inert on-chain.
	// AUTHZ over the NEW payload scope: the setter must be entitled to the scope
	// they declare (chain-admin, or owner of every listed domain).
	if !app.crossFedAuthorized(senderID, t.AllowedDomains) {
		return &abcitypes.ExecTxResult{Code: 106, Log: fmt.Sprintf("cross_fed: agent %s not authorized to set terms", senderID[:16])}
	}
	// AUTHZ over the EXISTING record on an UPSERT: this is a per-remote-chain
	// singleton keyed only on remote_chain_id, so without this check any principal
	// who owns ANY domain could hijack another party's (or the chain-admin's
	// wildcard) agreement by re-scoping the slot to a throwaway domain they own —
	// substituting attacker PeerPubKey/Endpoint (a confused-deputy that poisons the
	// exact trust anchor the transport phase pins to) and locking the real owner out
	// of revoke. Mirror the revoke path (which authorizes against the STORED scope):
	// modifying an existing record requires authority over its CURRENT scope too.
	if _, _, _, _, existingDomains, _, _, gErr := app.badgerStore.GetCrossFed(t.RemoteChainID); gErr == nil {
		if !app.crossFedAuthorized(senderID, existingDomains) {
			return &abcitypes.ExecTxResult{Code: 106, Log: fmt.Sprintf("cross_fed: agent %s not authorized to modify the existing terms for %s", senderID[:16], t.RemoteChainID)}
		}
	}
	if setErr := app.badgerStore.SetCrossFed(t.RemoteChainID, t.Endpoint, t.PeerPubKey,
		uint8(t.MaxClearance), t.ExpiresAt, t.AllowedDomains, t.AllowedDepts, "active"); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 107, Log: fmt.Sprintf("cross_fed: badger write error: %v", setErr)}
	}
	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(t.RemoteChainID), Log: fmt.Sprintf("cross_fed terms set for %s", t.RemoteChainID)}
}

// processCrossFedRevoke tears down an exchange agreement (tx 34). Same authority as
// the set (chain-admin or owner of the agreement's scoped domains). Reason is
// decoded but not persisted (mirror processFederationRevoke).
func (app *SageApp) processCrossFedRevoke(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	postV15 := app.postAppV15Fork(height)
	recordAppV15Branch(postV15)
	if !postV15 {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	r := parsedTx.CrossFedRevoke
	if r == nil {
		return &abcitypes.ExecTxResult{Code: 100, Log: "missing CrossFedRevoke payload"}
	}
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 101, Log: fmt.Sprintf("cross_fed: agent identity verification failed: %v", err)}
	}
	_, _, _, _, allowedDomains, _, status, gErr := app.badgerStore.GetCrossFed(r.RemoteChainID)
	if gErr != nil {
		return &abcitypes.ExecTxResult{Code: 108, Log: fmt.Sprintf("cross_fed: unknown agreement %s", r.RemoteChainID)}
	}
	if status != "active" {
		return &abcitypes.ExecTxResult{Code: 108, Log: fmt.Sprintf("cross_fed: agreement %s is not active", r.RemoteChainID)}
	}
	if !app.crossFedAuthorized(senderID, allowedDomains) {
		return &abcitypes.ExecTxResult{Code: 106, Log: fmt.Sprintf("cross_fed: agent %s not authorized to revoke terms", senderID[:16])}
	}
	if upErr := app.badgerStore.UpdateCrossFedStatus(r.RemoteChainID, "revoked"); upErr != nil {
		return &abcitypes.ExecTxResult{Code: 107, Log: fmt.Sprintf("cross_fed: badger write error: %v", upErr)}
	}
	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(r.RemoteChainID), Log: fmt.Sprintf("cross_fed terms revoked for %s", r.RemoteChainID)}
}

func (app *SageApp) processMemoryVote(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	vote := parsedTx.MemoryVote
	if vote == nil {
		return &abcitypes.ExecTxResult{Code: 13, Log: "missing vote payload"}
	}

	validatorID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
	decision := voteDecisionToString(vote.Decision)

	// Verify voter is in the validator set before recording.
	if _, isValidator := app.validators.GetValidator(validatorID); !isValidator {
		return &abcitypes.ExecTxResult{Code: 13, Log: fmt.Sprintf("vote rejected: %s is not in the validator set", validatorID[:16])}
	}

	// app-v20: if this memory has a pinned scope ballot, only a member of that
	// exact historical ballot may vote. Roster updates are prospective and a
	// live-scope lookup must never rewrite an in-flight denominator.
	var scopedBallot *scope.Ballot
	if app.postAppV20Fork(height) {
		var ballotErr error
		scopedBallot, ballotErr = app.badgerStore.GetScopeBallot(vote.MemoryID)
		if ballotErr != nil {
			return &abcitypes.ExecTxResult{Code: 13, Log: "vote rejected: scope ballot lookup failed: " + ballotErr.Error()}
		}
		if scopedBallot != nil && !scopeBallotHasMember(*scopedBallot, validatorID) {
			return &abcitypes.ExecTxResult{Code: 13, Log: fmt.Sprintf("vote rejected: %s is not a pinned member of scope ballot %s", validatorID[:16], scopedBallot.ScopeID)}
		}
		if scopedBallot == nil {
			if domain, domainErr := app.badgerStore.GetMemoryDomain(vote.MemoryID); domainErr == nil && len(domain) > maxAppV20IdentifierBytes {
				return &abcitypes.ExecTxResult{Code: appV20ResourceLimitCode, Log: fmt.Sprintf("vote rejected: legacy memory domain has %d bytes, app-v20 limit %d", len(domain), maxAppV20IdentifierBytes)}
			}
		}
	}

	app.logger.Info().
		Str("memory_id", vote.MemoryID).
		Str("validator_id", validatorID[:16]).
		Str("decision", decision).
		Msg("processing vote")

	// Store vote on-chain
	newVote := true
	if scopedBallot != nil {
		var voteErr error
		newVote, voteErr = app.badgerStore.SetScopedVote(vote.MemoryID, validatorID, decision, height)
		if voteErr != nil {
			return &abcitypes.ExecTxResult{Code: 14, Log: fmt.Sprintf("scoped vote rejected: %v", voteErr)}
		}
	} else {
		voteKey := fmt.Sprintf("vote:%s:%s", vote.MemoryID, validatorID)
		if err := app.badgerStore.SetState(voteKey, []byte(decision)); err != nil {
			return &abcitypes.ExecTxResult{Code: 14, Log: fmt.Sprintf("badger write error: %v", err)}
		}
	}

	// Increment on-chain validator vote stats for PoE scoring
	if newVote {
		accepted := decision == "accept"
		uHeight := uint64(height) // #nosec G115 -- height is always non-negative
		if err := app.badgerStore.IncrementVoteStats(validatorID, accepted, uHeight, app.postV8_3Fork(height)); err != nil {
			app.logger.Error().Err(err).Str("validator", validatorID).Msg("failed to increment vote stats")
		}
	}

	// Buffer PostgreSQL vote write
	voteRecord := &store.ValidationVote{
		MemoryID:    vote.MemoryID,
		ValidatorID: validatorID,
		Decision:    decision,
		Rationale:   vote.Rationale,
		BlockHeight: height,
		CreatedAt:   blockTime,
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "vote", data: voteRecord})

	metrics.VotesTotal.WithLabelValues(decision).Inc()

	// Check quorum — gather all votes for this memory (sorted by validator ID)
	app.checkAndApplyQuorum(vote.MemoryID, height, blockTime)

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("vote recorded for memory %s", vote.MemoryID)}
}

func (app *SageApp) checkAndApplyQuorum(memoryID string, height int64, blockTime time.Time) {
	// app-v20 scope ballots use their pinned integer denominator. The unscoped
	// path below remains untouched and continues to use current PoE weighting.
	if app.postAppV20Fork(height) {
		ballot, err := app.badgerStore.GetScopeBallot(memoryID)
		if err != nil {
			app.logger.Error().Err(err).Str("memory_id", memoryID).Msg("scope ballot lookup failed during quorum")
			return
		}
		if ballot != nil {
			app.checkAndApplyScopedQuorum(*ballot, height, blockTime)
			return
		}
	}

	// Get all validators sorted
	validators := app.validators.GetAll()
	votes := make(map[string]bool)
	weights := make(map[string]float64)

	// v8.2: post-fork blocks consult PoEWeight; pre-fork blocks keep
	// the Phase-1 equal-weight branch so they replay byte-identical to
	// v8.1.2. Recorded on the same sage_fork_branch_total metric (with
	// fork="v8.2") so operators can confirm the gate flipped without
	// scraping logs.
	postFork := app.postV8_2Fork(height)
	recordV8_2Branch(postFork)

	// v8.3: capture the memory's status BEFORE any SetMemoryHash write below,
	// so verdict-match crediting fires exactly once — on the transition INTO a
	// terminal state. A replayed vote on an already-committed/deprecated memory
	// re-enters here with priorStatus != "proposed" and credits nothing.
	// Fail-closed: any GetMemoryHash error (incl. not-found) leaves priorStatus
	// == "" (not "proposed"), so we never panic or mis-credit.
	creditVerdict := app.postV8_3Fork(height)
	recordV8_3Branch(creditVerdict)
	// app-v15 (v11): gate the terminal-status WRITE below on priorStatus==proposed
	// so a stale/replayed vote on an already-terminal memory cannot re-fire
	// SetMemoryHash - which would clobber a reclaimed co-commit's real content hash
	// to nil (committed branch) or flip it to deprecated. Capture priorStatus when
	// EITHER verdict-crediting (v8.3) or this guard is active.
	guardTerminalWrite := app.postAppV15Fork(height)
	var priorStatus string
	if creditVerdict || guardTerminalWrite {
		if _, st, err := app.badgerStore.GetMemoryHash(memoryID); err == nil {
			priorStatus = st
		}
	}
	// When the guard is active, only a currently-proposed memory may transition to
	// a terminal status here - the proposed -> committed/deprecated move is one-way.
	canWriteTerminal := !guardTerminalWrite || priorStatus == string(memory.StatusProposed)

	// v8.4: resolve the memory's domain so the weight loop can condition each
	// validator's vote on its verdict-correctness IN that domain. Domain
	// weighting engages only post-fork, for a recorded non-shared domain — shared
	// catch-alls (general, self) and unknown/legacy memories (no memdomain: key)
	// fall back to the v8.2 scalar weight, since subject-matter expertise is
	// meaningless there. isSharedDomain is height-aware (honours governance
	// promotions), keeping the gate deterministic across replicas.
	domainWeighting := app.postV8_4Fork(height)
	recordV8_4Branch(domainWeighting)
	var memDomain string
	if domainWeighting {
		if d, derr := app.badgerStore.GetMemoryDomain(memoryID); derr == nil {
			memDomain = d
		}
	}
	useDomainWeight := domainWeighting && memDomain != "" && !app.isSharedDomain(memDomain, height)

	app.logger.Debug().
		Str("memory_id", memoryID).
		Int("num_validators", len(validators)).
		Bool("post_v8_2_fork", postFork).
		Bool("post_v8_3_fork", creditVerdict).
		Bool("post_v8_4_fork", domainWeighting).
		Str("mem_domain", memDomain).
		Bool("use_domain_weight", useDomainWeight).
		Msg("checking quorum")

	for _, v := range validators {
		switch {
		case useDomainWeight:
			// v8.4: domain-conditional weight. The three global factors (accuracy,
			// recency, corroboration) come from the validator's vstats:<v> record,
			// recomputed live at quorum time via the shared helper; the domain
			// factor is its verdict-correctness EWMA in vstats_domain:<v>:<D>. All
			// inputs are committed on-chain state, so the per-memory weight is a
			// deterministic function of chain state (replay-stable). Weights are
			// the raw ComputeWeight outputs — NOT normalized — because HasQuorum is
			// ratio-only and every validator in THIS call takes this same branch
			// (useDomainWeight is a per-memory property), so the ratio is
			// well-defined without the per-epoch RepCap normalization (which, with
			// N<10 validators, would collapse all weights to equal and erase the
			// domain signal). Domain experts thus carry genuinely more weight on
			// in-domain memories. See docs/v8.4-PLAN.md.
			gStats, _ := app.badgerStore.GetValidatorStats(v.ID)
			acc, rec, corr := poeFactorsPostV83(gStats, height, blockTime)
			// Domain accuracy defaults to the EWMA cold-start prior (0.5). A
			// decode error / nil record is treated as cold-start, mirroring the
			// nil-tolerance of the gStats path (poeFactorsPostV83's stats!=nil
			// guard) — a corrupt record is byte-identical across replicas, so this
			// stays deterministic rather than panicking one node into a halt.
			domainScore := poe.NewEWMATracker().Accuracy()
			if dStats, derr := app.badgerStore.GetValidatorDomainStats(v.ID, memDomain); derr == nil && dStats != nil {
				domTracker := poe.EWMATracker{
					WeightedSum: dStats.EWMAWeightedSum,
					WeightDenom: dStats.EWMAWeightDenom,
					Count:       int64(dStats.EWMACount), // #nosec G115 -- non-negative
				}
				domainScore = domTracker.Accuracy()
			}
			weights[v.ID] = poe.ComputeWeight(acc, domainScore, rec, corr)
		case postFork:
			// Bootstrap fallback (1/N) covers three cases: pre-first-epoch
			// chain, mid-epoch governance add, and defensive guard for a
			// missing poew:<id> entry. HasQuorum is ratio-only so the
			// transient mix of "real" and "1/N" weights doesn't move the
			// 2/3 threshold — see docs/v8.2-PLAN.md "Bootstrap fallback".
			weights[v.ID] = poeWeightOrFallback(v.PoEWeight, len(validators))
		default:
			weights[v.ID] = 1.0 // Phase 1, pre-fork
		}
		voteKey := fmt.Sprintf("vote:%s:%s", memoryID, v.ID)
		voteData, err := app.badgerStore.GetState(voteKey)
		if err == nil && voteData != nil {
			votes[v.ID] = string(voteData) == "accept"
			app.logger.Debug().
				Str("memory_id", memoryID).
				Str("validator", v.ID[:16]).
				Str("decision", string(voteData)).
				Float64("weight", weights[v.ID]).
				Msg("found vote")
		}
	}

	app.logger.Debug().
		Str("memory_id", memoryID).
		Int("votes_found", len(votes)).
		Int("validators", len(validators)).
		Msg("quorum check votes gathered")

	reached, acceptWeight, totalWeight := validator.CheckQuorum(votes, weights)
	// v8.3: track whether THIS call drives the memory to a terminal verdict,
	// and which one, so we credit verdict-match exactly once below.
	var becameTerminal, finalAccepted bool
	if canWriteTerminal && reached {
		// Transition to committed on-chain (BadgerDB). becameTerminal and the
		// off-chain status mirror are gated on the on-chain write SUCCEEDING — a
		// swallowed SetMemoryHash error must NOT leave us crediting a verdict (or
		// mirroring a committed status to Postgres) for a memory whose on-chain
		// status never actually changed. poe-drift audit finding.
		if err := app.badgerStore.SetMemoryHash(memoryID, nil, string(memory.StatusCommitted)); err == nil {
			app.logger.Info().
				Str("memory_id", memoryID).
				Int64("height", height).
				Float64("accept_weight", acceptWeight).
				Float64("total_weight", totalWeight).
				Msg("memory committed by quorum")
			becameTerminal, finalAccepted = true, true

			// Buffer PostgreSQL status update — flushes in Commit
			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "status_update",
				data: &statusUpdate{
					MemoryID: memoryID,
					Status:   memory.StatusCommitted,
					At:       blockTime,
				},
			})
		} else {
			app.logger.Error().Err(err).Str("memory_id", memoryID).Int64("height", height).
				Msg("commit status write failed — verdict NOT credited, status unchanged")
		}
	} else if canWriteTerminal && len(votes) >= len(validators) && len(validators) > 0 {
		// All validators voted but quorum not reached (e.g. 2-2 tie) — deprecate.
		// Without this, the memory stays "proposed" forever and the validator
		// ticker resubmits votes every 2 seconds, flooding the chain. Same
		// write-success gating as the committed branch above.
		if err := app.badgerStore.SetMemoryHash(memoryID, nil, string(memory.StatusDeprecated)); err == nil {
			app.logger.Info().
				Str("memory_id", memoryID).
				Int64("height", height).
				Float64("accept_weight", acceptWeight).
				Float64("total_weight", totalWeight).
				Int("votes", len(votes)).
				Int("validators", len(validators)).
				Msg("memory rejected — all validators voted, quorum not reached")
			becameTerminal, finalAccepted = true, false

			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "status_update",
				data: &statusUpdate{
					MemoryID: memoryID,
					Status:   memory.StatusDeprecated,
					At:       blockTime,
				},
			})
		} else {
			app.logger.Error().Err(err).Str("memory_id", memoryID).Int64("height", height).
				Msg("deprecate status write failed — verdict NOT credited, status unchanged")
		}
	}

	// v8.3: credit per-validator verdict-correctness EWMA + corroboration count
	// on the FIRST transition into a terminal verdict. priorStatus == proposed
	// guarantees once-only crediting (idempotent under replayed votes); the
	// challenge path (processMemoryChallenge) does not reach here, so it never
	// credits. Suppressed pre-fork → byte-identical replay.
	if creditVerdict && becameTerminal && priorStatus == string(memory.StatusProposed) {
		matches := make(map[string]bool, len(votes))
		for vid, votedAccept := range votes {
			matches[vid] = votedAccept == finalAccepted
		}
		if err := app.badgerStore.UpdateVerdictStats(matches); err != nil {
			app.logger.Error().Err(err).Str("memory_id", memoryID).Msg("v8.3 verdict-stats update")
		}
		// v8.4: also credit per-domain verdict-correctness for non-shared domains,
		// so domainAccuracy(v, D) reflects how often v is right specifically in D.
		// Same once-per-terminal-transition guard (priorStatus == proposed) and the
		// same match map as the global credit above — the two are independent
		// accumulators (vstats: vs vstats_domain:<D>) fed from one event. Gated by
		// useDomainWeight, which implies postV8_4Fork (⟹ creditVerdict) and a
		// recorded non-shared domain; shared/unknown domains credit nothing
		// per-domain (quorum falls back to the global weight for them anyway).
		if useDomainWeight {
			if err := app.badgerStore.UpdateDomainVerdictStats(memDomain, matches); err != nil {
				app.logger.Error().Err(err).Str("memory_id", memoryID).Str("domain", memDomain).Msg("v8.4 domain verdict-stats update")
			}
		}
	}
}

func (app *SageApp) processMemoryChallenge(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	challenge := parsedTx.MemoryChallenge
	if challenge == nil {
		return &abcitypes.ExecTxResult{Code: 15, Log: "missing challenge payload"}
	}

	// Verify challenger identity.
	challengerID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 15, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// app-v15 (verb-ladder): deprecation is the privileged "modify" verb. PRE-FORK
	// ANY authenticated agent could deprecate ANY memory by client-supplied ID —
	// the ungated-deprecate hole. POST-FORK the challenger must be the domain
	// owner/ancestor-owner OR hold a level-3 (modify) grant on the memory's domain.
	// All inputs derive from committed BadgerDB state + the consensus blockTime —
	// no time.Now, no remote call, no per-node cache — so every replica reaches the
	// same verdict. The whole block is skipped pre-fork (postAppV15Rules is false on
	// every existing chain and on the activation block itself, strict >), so
	// historical blocks replay byte-identically. Every reject returns BEFORE the
	// SetMemoryHash deprecate below, so a rejected challenge never mutates Badger or
	// the AppHash (same validate-before-write discipline as processMemorySubmit).
	postV15 := app.postAppV15Rules(height)
	recordAppV15Branch(postV15)
	var domain string
	if postV15 {
		var dErr error
		domain, dErr = app.badgerStore.GetMemoryDomain(challenge.MemoryID)
		if dErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: domain lookup failed: %v", dErr)}
		}
		if domain == "" {
			// No recorded domain. Under app-v16, split the reject so integrators can
			// tell a real LEGACY memory (exists on-chain but predates the app-v8.4
			// memdomain: key) from a bogus/never-submitted ID, and point them at the
			// remediation. BOTH still deny — we never bypass the modify gate here
			// (that was the ungated-deprecate hole); the fix is an
			// OpMemoryDomainRepair governance backfill of the domain, after which
			// normal deprecation authorizes. Fork-gated (postAppV16Rules, strict >),
			// so pre-fork blocks keep the original single message and replay
			// byte-identically.
			if app.postAppV16Rules(height) {
				if _, _, hErr := app.badgerStore.GetMemoryHash(challenge.MemoryID); hErr == nil {
					return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: memory %s has no recorded domain (legacy memory predating app-v8.4); deprecation is blocked until its domain is repaired via an OpMemoryDomainRepair governance proposal", challenge.MemoryID)}
				}
				return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: unknown memory %s (no memory record and no recorded domain); not authorized to deprecate", challenge.MemoryID)}
			}
			// Legacy/undomained memory, or a memoryID that was never submitted:
			// nothing to authorize against. Deny-by-default rather than bypass the
			// modify gate (a pre-v8.4 memory with no memdomain: key, or a bogus ID).
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: memory %s has no recorded domain; not authorized to deprecate", challenge.MemoryID)}
		}
		isAdmin, adErr := app.badgerStore.IsDomainOwnerOrAncestor(domain, challengerID)
		if adErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: domain-owner lookup failed: %v", adErr)}
		}
		hasModify, hErr := app.badgerStore.HasAccessOrAncestor(domain, challengerID, 3, blockTime)
		if hErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: access lookup failed: %v", hErr)}
		}
		if !isAdmin && !hasModify {
			return &abcitypes.ExecTxResult{Code: 92, Log: fmt.Sprintf("challenge: agent %s not authorized to deprecate memory %s (need domain ownership or a level-3 modify grant)", challengerID[:16], challenge.MemoryID)}
		}
	}

	// app-v17 (C3): quorum-scaled two-phase challenge. Where more than one agent
	// can modify a memory's domain, a single challenger no longer deprecates
	// unilaterally — the memory is parked CHALLENGED pending either a SECOND
	// distinct modify-verb holder's CONFIRM (deprecate) or any holder's REINSTATE
	// (recommit, TxTypeMemoryReinstate). When the modify-verb-holder count is <= 1
	// (personal nodes) the outcome is byte-identical to the legacy one-strike
	// deprecate below — zero observable change. Every input is committed Badger
	// state read at blockTime; the whole block is skipped pre-fork (postAppV17Rules
	// is false on every existing chain and on the activation block itself, strict
	// >), so historical blocks replay byte-identically, and each reject returns
	// BEFORE any write.
	if app.postAppV17Rules(height) {
		// priorHash/priorStatus are read once. A never-submitted or nil-hash memory
		// reads ("", "") on error — treated as a fresh (non-challenged) target.
		priorHash, priorStatus, _ := app.badgerStore.GetMemoryHash(challenge.MemoryID)

		// CONFIRM phase: the memory is ALREADY parked CHALLENGED. The authz gate
		// above proved this challenger holds the modify verb; a DISTINCT holder
		// finalizes the deprecation, but the ORIGINAL challenger cannot confirm
		// their own challenge (that would collapse two-phase back into one-strike).
		// C-D3: the persisted quorum is NEVER re-measured here.
		if priorStatus == string(memory.StatusChallenged) {
			rec, rErr := app.badgerStore.GetChallengeRecord(challenge.MemoryID)
			if rErr != nil {
				return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: challenge record lookup failed: %v", rErr)}
			}
			if rec == nil {
				return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: memory %s is challenged but has no open challenge record", challenge.MemoryID)}
			}
			if rec.ChallengerID == challengerID {
				return &abcitypes.ExecTxResult{Code: 93, Log: fmt.Sprintf("challenge: agent %s already opened this challenge; a DISTINCT modify-verb holder must confirm", challengerID[:16])}
			}
			// Confirm → deprecated (terminal). Unscoped memories retain the legacy
			// nil-hash shape; app-v20 scoped memories preserve their canonical hash
			// so state-sync projection recovery remains verifiable.
			var resolvedHash []byte
			if _, scopedHash, scopedErr := app.scopedLifecycleState(challenge.MemoryID, height); scopedErr != nil {
				return &abcitypes.ExecTxResult{Code: 16, Log: "challenge: " + scopedErr.Error()}
			} else if scopedHash != nil {
				resolvedHash = scopedHash
			}
			if err := app.badgerStore.ResolveChallenge(challenge.MemoryID, resolvedHash, string(memory.StatusDeprecated)); err != nil {
				return &abcitypes.ExecTxResult{Code: 16, Log: err.Error()}
			}
			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "challenge",
				data: &store.ChallengeEntry{
					MemoryID:     challenge.MemoryID,
					ChallengerID: challengerID,
					Reason:       challenge.Reason,
					Evidence:     challenge.Evidence,
					BlockHeight:  height,
					CreatedAt:    blockTime,
				},
			})
			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "status_update",
				data:      &statusUpdate{MemoryID: challenge.MemoryID, Status: memory.StatusDeprecated, At: blockTime},
			})
			metrics.ChallengesTotal.Inc()
			return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("memory %s challenge confirmed → deprecated by %s", challenge.MemoryID, challengerID[:16])}
		}

		// FRESH challenge: count the distinct modify-verb holders on the domain at
		// THIS execution height (C-D3: persisted in the challenge record, never
		// re-measured at confirm/reinstate).
		holders, hsErr := app.badgerStore.ModifyVerbHolders(domain, blockTime)
		if hsErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("challenge: modify-holder enumeration failed: %v", hsErr)}
		}
		if len(holders) >= 2 && priorStatus == string(memory.StatusCommitted) {
			// >= 2 holders AND the target is a live COMMITTED memory: park
			// CHALLENGED. The priorStatus==committed guard is load-bearing — it
			// confines the two-phase machine to committed memories so it can never
			// REVERSE a terminal state or SKIP validation. Without it a single
			// modify-verb holder could park a `deprecated` memory (then reinstate
			// it, resurrecting a two-holder deprecation) or a still-`proposed` one
			// (bypassing the content-validation quorum and committing it straight
			// through reinstate). A non-committed target (deprecated / proposed /
			// validated) instead falls through to the legacy one-strike deprecate
			// below: idempotent for an already-deprecated memory (SetMemoryHash(nil,
			// deprecated) keeps it deprecated) and byte-identical to pre-fork
			// otherwise. Keep the prior content hash on the memory record (do NOT
			// nil it) so a later reinstate restores the real memory, not a hash-less
			// husk; also persist it in the challenge record as the authoritative
			// restoration source.
			if err := app.badgerStore.OpenChallenge(challenge.MemoryID, priorHash, string(memory.StatusChallenged), &store.ChallengeRecord{
				ChallengerID:    challengerID,
				Domain:          domain,
				ExecutionHeight: height,
				QuorumCount:     uint32(len(holders)), // #nosec G115 -- holder count fits in uint32
				PriorHash:       priorHash,
				PriorStatus:     priorStatus,
			}); err != nil {
				return &abcitypes.ExecTxResult{Code: 16, Log: err.Error()}
			}
			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "challenge",
				data: &store.ChallengeEntry{
					MemoryID:     challenge.MemoryID,
					ChallengerID: challengerID,
					Reason:       challenge.Reason,
					Evidence:     challenge.Evidence,
					BlockHeight:  height,
					CreatedAt:    blockTime,
				},
			})
			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "status_update",
				data: &statusUpdate{
					MemoryID:       challenge.MemoryID,
					Status:         memory.StatusChallenged,
					At:             blockTime,
					DisputedHeight: height,
					DisputedQuorum: uint32(len(holders)), // #nosec G115 -- holder count fits in uint32
				},
			})
			metrics.ChallengesTotal.Inc()
			return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("memory %s challenged (quorum %d) by %s — awaiting confirm/reinstate", challenge.MemoryID, len(holders), challengerID[:16])}
		}
		// count <= 1, or a non-committed target (deprecated/proposed/validated):
		// fall through to the legacy one-strike deprecate below.
	}

	// A challenge that passes BFT consensus (included in a block) is decisive —
	// the memory is deprecated immediately. The block inclusion IS the consensus.
	// A pending scoped ballot transitions atomically with the ordinary status;
	// an already-committed ballot remains the historical acceptance proof while
	// the later lifecycle status changes and retains the canonical content hash.
	if scopedBallot, scopedHash, scopedErr := app.scopedLifecycleState(challenge.MemoryID, height); scopedErr != nil {
		return &abcitypes.ExecTxResult{Code: 16, Log: "challenge: " + scopedErr.Error()}
	} else if scopedBallot != nil && scopedBallot.State == scope.BallotPending {
		if err := app.badgerStore.SetScopedMemoryVerdict(challenge.MemoryID, scope.BallotDeprecated); err != nil {
			return &abcitypes.ExecTxResult{Code: 16, Log: err.Error()}
		}
	} else if scopedBallot != nil {
		if err := app.badgerStore.SetMemoryHash(challenge.MemoryID, scopedHash, string(memory.StatusDeprecated)); err != nil {
			return &abcitypes.ExecTxResult{Code: 16, Log: err.Error()}
		}
	} else if err := app.badgerStore.SetMemoryHash(challenge.MemoryID, nil, string(memory.StatusDeprecated)); err != nil {
		return &abcitypes.ExecTxResult{Code: 16, Log: err.Error()}
	}

	// Buffer challenge audit trail to off-chain store.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "challenge",
		data: &store.ChallengeEntry{
			MemoryID:     challenge.MemoryID,
			ChallengerID: challengerID,
			Reason:       challenge.Reason,
			Evidence:     challenge.Evidence,
			BlockHeight:  height,
			CreatedAt:    blockTime,
		},
	})

	// Buffer status update — deprecated immediately.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "status_update",
		data: &statusUpdate{
			MemoryID: challenge.MemoryID,
			Status:   memory.StatusDeprecated,
			At:       blockTime,
		},
	})

	metrics.ChallengesTotal.Inc()
	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("memory %s deprecated by %s", challenge.MemoryID, challengerID[:16])}
}

// processMemoryReinstate handles TxTypeMemoryReinstate (v11.5 / app-v17): the
// second phase of the quorum-scaled two-phase challenge. It moves a CHALLENGED
// memory back to committed, restoring the real content hash captured in the
// on-chain challenge record so the memory is not left a hash-less husk. Any
// distinct modify-verb holder may reinstate; the ORIGINAL challenger may always
// withdraw their own challenge this way, even after losing the grant that opened
// it. Dual-gated with the CheckTx pre-fork reject: pre-fork it returns Code
// 10 ("unknown tx type") and mutates nothing, so a chain that never activates
// app-v17 replays byte-identically. Every reject returns BEFORE any write.
func (app *SageApp) processMemoryReinstate(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	postFork := app.postAppV17Fork(height)
	if !postFork {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	rein := parsedTx.MemoryReinstate
	if rein == nil {
		return &abcitypes.ExecTxResult{Code: 15, Log: "missing reinstate payload"}
	}

	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 15, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// The memory must currently be CHALLENGED. Reinstating anything else
	// (never-challenged, already committed, already deprecated, or double-
	// reinstate of an already-reinstated memory) is an invalid transition and is
	// cleanly rejected without any state write.
	_, status, mErr := app.badgerStore.GetMemoryHash(rein.MemoryID)
	if mErr != nil {
		return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: memory %s not found", rein.MemoryID)}
	}
	if status != string(memory.StatusChallenged) {
		return &abcitypes.ExecTxResult{Code: 94, Log: fmt.Sprintf("reinstate: memory %s is not challenged (status=%s)", rein.MemoryID, status)}
	}
	rec, rErr := app.badgerStore.GetChallengeRecord(rein.MemoryID)
	if rErr != nil {
		return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: challenge record lookup failed: %v", rErr)}
	}
	if rec == nil {
		return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: memory %s is challenged but has no open challenge record", rein.MemoryID)}
	}

	// Authz normally mirrors the challenge verb ladder — reinstatement is a
	// modify verb, so the caller must currently own the memory's domain/ancestor
	// OR hold a live level-3 grant. The original challenger is the deliberate
	// exception: app-v17's state machine promises that they may ALWAYS withdraw
	// their own challenge. Checking current grants before the challenge record
	// made that promise false when their grant expired or was revoked while the
	// dispute was open. The persisted, AppHash-folded ChallengerID is the stable
	// proof of who opened it, so that identity may withdraw even after losing the
	// modify verb; every other caller still passes the current-state ladder.
	if rec.ChallengerID != agentID {
		domain, dErr := app.badgerStore.GetMemoryDomain(rein.MemoryID)
		if dErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: domain lookup failed: %v", dErr)}
		}
		if domain == "" {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: memory %s has no recorded domain; not authorized", rein.MemoryID)}
		}
		isAdmin, adErr := app.badgerStore.IsDomainOwnerOrAncestor(domain, agentID)
		if adErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: domain-owner lookup failed: %v", adErr)}
		}
		hasModify, hErr := app.badgerStore.HasAccessOrAncestor(domain, agentID, 3, blockTime)
		if hErr != nil {
			return &abcitypes.ExecTxResult{Code: 91, Log: fmt.Sprintf("reinstate: access lookup failed: %v", hErr)}
		}
		if !isAdmin && !hasModify {
			return &abcitypes.ExecTxResult{Code: 92, Log: fmt.Sprintf("reinstate: agent %s not authorized to reinstate memory %s (need domain ownership or a level-3 modify grant)", agentID[:16], rein.MemoryID)}
		}
	}

	// Restore: challenged → committed, recovering the real content hash from the
	// challenge record (the authoritative snapshot taken at challenge-execution
	// height), then clear the open-challenge record.
	if err := app.badgerStore.ResolveChallenge(rein.MemoryID, rec.PriorHash, string(memory.StatusCommitted)); err != nil {
		return &abcitypes.ExecTxResult{Code: 16, Log: err.Error()}
	}

	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "status_update",
		data:      &statusUpdate{MemoryID: rein.MemoryID, Status: memory.StatusCommitted, At: blockTime},
	})

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("memory %s reinstated → committed by %s", rein.MemoryID, agentID[:16])}
}

func (app *SageApp) processMemoryCorroborate(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	corrob := parsedTx.MemoryCorroborate
	if corrob == nil {
		return &abcitypes.ExecTxResult{Code: 17, Log: "missing corroborate payload"}
	}

	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// app-v10: corroboration integrity guard. Corroboration is the multi-agent
	// trust signal that moves a memory from attributed toward consensus, so it must
	// come from someone OTHER than the author and count at most once per agent.
	// Both checks read BadgerDB only (the memauthor: / corrob: keys), so every
	// replica reaches the same verdict deterministically. Gated post-fork; pre-fork
	// blocks replay byte-identically. Forward-looking: a memory submitted before
	// app-v10 has no memauthor: record (author == ""), so the self-check is skipped
	// for it. The corrob: dedup marker is written in the consensus path (immediate,
	// not buffered) so two same-agent corroborations in ONE block are caught.
	postV10 := app.postAppV10Rules(height)
	recordAppV10Branch(postV10) // both branches counted so the dashboard sees the pre/post split
	if postV10 {
		// The memory must already exist on-chain. memoryID is client-supplied, and
		// without this an attacker corroborates an ID it controls BEFORE submitting
		// it: the author is empty so the self-check is skipped, a corrob: marker is
		// written, then the attacker submits that exact ID and becomes its immutable
		// first-writer author — holding a self-corroboration marker on its own
		// memory and defeating the guard. The check also stops unbounded
		// attacker-chosen corrob: keys (one per tx) from permanently entering the
		// AppHash keyspace. memory:<id> is written by EVERY submit (ungated, pre- or
		// post-fork), so a legitimately submitted memory — including a pre-app-v10
		// one with no memauthor: record — still passes.
		if _, _, gErr := app.badgerStore.GetMemoryHash(corrob.MemoryID); gErr != nil {
			return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: unknown memory %s", corrob.MemoryID)}
		}
		author, aErr := app.badgerStore.GetMemoryAuthor(corrob.MemoryID)
		if aErr != nil {
			return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: author lookup failed: %v", aErr)}
		}
		if author != "" && author == agentID {
			return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: agent %s cannot corroborate its own memory %s", agentID[:16], corrob.MemoryID)}
		}
		// M2: a co-committed memory records only the LOCAL relay submitter in
		// memauthor; its true (co-)authors live in cocommit:coauthors. Extend the
		// self-corroboration guard so no recorded coauthor can corroborate the
		// jointly-authored memory. Non-co-commit memories have no cocommit:coauthors
		// key (no-op — byte-identical replay), and co-commit memories only exist
		// post-app-v15, so the data itself gates this without a separate fork check.
		if caBlob, caErr := app.badgerStore.GetCoCommitCoauthors(corrob.MemoryID); caErr == nil && len(caBlob) > 0 {
			if coauthors, decErr := tx.DecodeCoauthorsCanonical(caBlob); decErr == nil {
				for _, c := range coauthors {
					if hex.EncodeToString(c.PubKey) == agentID {
						return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: agent %s cannot corroborate its own co-authored memory %s", agentID[:16], corrob.MemoryID)}
					}
				}
			}
		}
		already, hErr := app.badgerStore.HasCorroborated(corrob.MemoryID, agentID)
		if hErr != nil {
			return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: dedup lookup failed: %v", hErr)}
		}
		if already {
			return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: agent %s already corroborated memory %s", agentID[:16], corrob.MemoryID)}
		}
		if sErr := app.badgerStore.SetCorroborated(corrob.MemoryID, agentID); sErr != nil {
			return &abcitypes.ExecTxResult{Code: 17, Log: fmt.Sprintf("corroborate: badger write error: %v", sErr)}
		}
	}

	// Buffer corroboration write
	corr := &store.Corroboration{
		MemoryID:  corrob.MemoryID,
		AgentID:   agentID,
		Evidence:  corrob.Evidence,
		CreatedAt: blockTime,
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "corroborate", data: corr})

	metrics.CorroborationsTotal.Inc()
	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("corroboration for memory %s", corrob.MemoryID)}
}

func (app *SageApp) processAccessRequest(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	req := parsedTx.AccessRequest
	if req == nil {
		return &abcitypes.ExecTxResult{Code: 30, Log: "missing access request payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 30, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Validate level. app-v15 (verb-ladder) raises the requestable cap to 3
	// (=modify) in lockstep with the grant cap. Pre-fork stays byte-identical
	// (reject >2, Code 31, same log) for replay safety.
	if app.postAppV15Rules(height) {
		if req.RequestedLevel < 1 || req.RequestedLevel > 3 {
			return &abcitypes.ExecTxResult{Code: 31, Log: "invalid access level: must be 1 (read), 2 (read+write), or 3 (modify)"}
		}
	} else {
		if req.RequestedLevel < 1 || req.RequestedLevel > 2 {
			return &abcitypes.ExecTxResult{Code: 31, Log: "invalid access level: must be 1 (read) or 2 (read+write)"}
		}
	}

	// Generate deterministic request ID
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", agentID, req.TargetDomain, height)))
	requestID := hex.EncodeToString(h[:16])

	// Store in BadgerDB
	if setErr := app.badgerStore.SetAccessRequest(requestID, agentID, req.TargetDomain, "pending", height); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 32, Log: fmt.Sprintf("badger write error: %v", setErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "access_request",
		data: &store.AccessRequestEntry{
			RequestID:     requestID,
			RequesterID:   agentID,
			TargetDomain:  req.TargetDomain,
			Justification: req.Justification,
			Status:        "pending",
			CreatedHeight: height,
			CreatedAt:     blockTime,
		},
	})

	app.logger.Info().Str("request_id", requestID).Str("agent", agentID).Str("domain", req.TargetDomain).Msg("access request created")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(requestID), Log: fmt.Sprintf("access request %s created", requestID)}
}

func (app *SageApp) processAccessGrant(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	grant := parsedTx.AccessGrant
	if grant == nil {
		return &abcitypes.ExecTxResult{Code: 33, Log: "missing access grant payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	granterID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 33, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Authorization: granter must own the domain or be ancestor domain owner.
	// Fork-gated: post-v8.0 relaxes this to mirror processMemorySubmit's
	// auto-register pattern — if the target domain is genuinely unowned
	// (no leaf owner AND no ancestor owner) and not a shared domain, the
	// granter auto-claims ownership and the grant proceeds. Shared domains
	// (general/self/meta/sage-*) are explicitly non-ownable and reject
	// with the distinct Code 50 so callers can tell the two failures
	// apart. Pre-fork blocks replay byte-identical to v7.1.1.
	postFork := app.postV8Fork(height) || app.postAppV8Rules(height)
	recordV8Branch(postFork)
	ownerBindingPresent := grant.ExpectedOwnerID != "" || grant.ExpectedOwnedDomain != ""
	if app.postAppV18Rules(height) && ownerBindingPresent {
		currentOwner, currentOwnedDomain, resolveErr := app.badgerStore.ResolveOwningAncestor(grant.Domain)
		if resolveErr != nil || grant.ExpectedOwnerID == "" || grant.ExpectedOwnedDomain == "" ||
			grant.ExpectedOwnerID != currentOwner || grant.ExpectedOwnedDomain != currentOwnedDomain {
			return &abcitypes.ExecTxResult{Code: 34, Log: "administrator override rejected: domain ownership changed or was not bound"}
		}
	}
	if postFork {
		isOwner, _ := app.badgerStore.IsDomainOwnerOrAncestor(grant.Domain, granterID)
		adminOverride := false
		if app.postAppV18Rules(height) {
			if granter, lookupErr := app.badgerStore.GetRegisteredAgent(granterID); lookupErr == nil {
				adminOverride = granter.Role == "admin"
			}
		}
		if !isOwner {
			// Empty-domain guard: must not auto-register the empty string
			// (which would otherwise be flagged as "not shared, no
			// ancestors" and silently captured). Treat as invariant
			// failure with the existing missing-payload code.
			if grant.Domain == "" {
				return &abcitypes.ExecTxResult{Code: 33, Log: "missing grant domain"}
			}
			// Distinguish "no owner anywhere" from "owned by someone
			// else": only the former is eligible for auto-claim.
			// IsDomainOwnerOrAncestor returns false in both cases.
			leafOwner, _ := app.badgerStore.GetDomainOwner(grant.Domain)
			anyAncestorOwned := false
			segs := strings.Split(grant.Domain, ".")
			for i := len(segs) - 1; i >= 1 && !anyAncestorOwned; i-- {
				ancestor := strings.Join(segs[:i], ".")
				if owner, _ := app.badgerStore.GetDomainOwner(ancestor); owner != "" {
					anyAncestorOwned = true
				}
			}
			if leafOwner != "" || anyAncestorOwned {
				if !adminOverride {
					return &abcitypes.ExecTxResult{Code: 34, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", granterID[:16], grant.Domain)}
				}
				if !ownerBindingPresent {
					return &abcitypes.ExecTxResult{Code: 34, Log: "administrator override rejected: domain ownership changed or was not bound"}
				}
				// Owned domain + app-v18 global admin: bypass ownership for this
				// grant only. Ownership itself remains unchanged.
			} else {
				// Unowned. Shared domains are never auto-registered —
				// granting on them is a category error, reject explicitly.
				if app.isSharedDomain(grant.Domain, height) {
					return &abcitypes.ExecTxResult{Code: 50, Log: fmt.Sprintf("shared domain not ownable: %s", grant.Domain)}
				}
				// Auto-register the granter as owner of this unowned,
				// non-shared domain. Mirrors processMemorySubmit's
				// auto-register branch exactly — same pendingWrites shape,
				// same idempotent owner self-grant.
				regErr := app.badgerStore.RegisterDomain(grant.Domain, granterID, "", height)
				if regErr != nil && !errors.Is(regErr, store.ErrDomainAlreadyRegistered) {
					app.logger.Error().Err(regErr).Str("domain", grant.Domain).Msg("auto-register on grant failed")
					return &abcitypes.ExecTxResult{Code: 34, Log: "auto-register failed"}
				}
				if errors.Is(regErr, store.ErrDomainAlreadyRegistered) {
					// Same-block race: another tx registered the domain
					// between our GetDomainOwner check and the RegisterDomain
					// check-and-set. processMemorySubmit can swallow this
					// because submits don't depend on the writer owning the
					// domain — the next-block access check covers it. Grants
					// DO depend on ownership, so we must re-check and reject
					// the loser. No pendingWrites are appended on the loser
					// path, mirroring TestAutoRegisterRaceLoss_NoSpuriousMirrorWrites.
					if owner, _ := app.badgerStore.GetDomainOwner(grant.Domain); owner != granterID {
						return &abcitypes.ExecTxResult{Code: 34, Log: fmt.Sprintf("access denied: %s lost auto-register race for domain %s", granterID[:16], grant.Domain)}
					}
				} else {
					// First-write success. Mirror the auto-register to the
					// off-chain accessStore so the mirror stays in sync from
					// the moment the domain is created (v7.5.4 invariant).
					app.logger.Info().Str("domain", grant.Domain).Str("owner", granterID[:16]).Msg("auto-registered domain on first access grant")
					app.pendingWrites = append(app.pendingWrites, pendingWrite{
						writeType: "domain_register",
						data: &store.DomainEntry{
							DomainName:    grant.Domain,
							OwnerAgentID:  granterID,
							CreatedHeight: height,
							CreatedAt:     blockTime,
						},
					})
					// Owner self-grant (level 2). Idempotent — if the grantee
					// below happens to be the granter itself, the outer
					// SetAccessGrant call is a no-op overwrite at the same
					// level. Mirrors processMemorySubmit.
					if grantErr := app.badgerStore.SetAccessGrant(grant.Domain, granterID, 2, 0, granterID); grantErr != nil {
						app.logger.Error().Err(grantErr).Str("domain", grant.Domain).Msg("failed to auto-grant owner access on grant path")
					} else {
						app.pendingWrites = append(app.pendingWrites, pendingWrite{
							writeType: "access_grant",
							data: &store.AccessGrantEntry{
								Domain:        grant.Domain,
								GranteeID:     granterID,
								GranterID:     granterID,
								Level:         2,
								CreatedHeight: height,
								CreatedAt:     blockTime,
							},
						})
					}
				}
			}
			// Fall through to level validation + the grantee's SetAccessGrant below.
		}
	} else {
		// Pre-fork (v7.1.1-byte-identical): strict ownership check.
		isOwner, err := app.badgerStore.IsDomainOwnerOrAncestor(grant.Domain, granterID)
		if err != nil || !isOwner {
			return &abcitypes.ExecTxResult{Code: 34, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", granterID[:16], grant.Domain)}
		}
	}

	// Validate level. app-v15 (verb-ladder) raises the grantable cap to 3 (=modify),
	// so the deprecate/supersede verb can be delegated. Pre-fork stays byte-identical
	// (reject >2, Code 35, same log) for replay safety.
	if app.postAppV15Rules(height) {
		if grant.Level < 1 || grant.Level > 3 {
			return &abcitypes.ExecTxResult{Code: 35, Log: "invalid access level: must be 1 (read), 2 (read+write), or 3 (modify)"}
		}
	} else {
		if grant.Level < 1 || grant.Level > 2 {
			return &abcitypes.ExecTxResult{Code: 35, Log: "invalid access level: must be 1 (read) or 2 (read+write)"}
		}
	}

	// Write grant to BadgerDB
	if setErr := app.badgerStore.SetAccessGrant(grant.Domain, grant.GranteeID, grant.Level, grant.ExpiresAt, granterID); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 36, Log: fmt.Sprintf("badger write error: %v", setErr)}
	}

	// Update access request status if request_id provided
	if grant.RequestID != "" {
		if updateErr := app.badgerStore.UpdateAccessRequestStatus(grant.RequestID, "granted"); updateErr != nil {
			app.logger.Warn().Err(updateErr).Str("request_id", grant.RequestID).Msg("failed to update access request status")
		}
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "access_request_status",
			data: &accessRequestStatusUpdate{
				RequestID: grant.RequestID,
				Status:    "granted",
				Height:    height,
			},
		})
	}

	// Buffer PostgreSQL write
	var expiresAt *time.Time
	if grant.ExpiresAt > 0 {
		t := time.Unix(grant.ExpiresAt, 0)
		expiresAt = &t
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "access_grant",
		data: &store.AccessGrantEntry{
			Domain:        grant.Domain,
			GranteeID:     grant.GranteeID,
			GranterID:     granterID,
			Level:         grant.Level,
			ExpiresAt:     expiresAt,
			CreatedHeight: height,
			CreatedAt:     blockTime,
		},
	})

	app.logger.Info().Str("granter", granterID[:16]).Str("grantee", grant.GranteeID[:16]).Str("domain", grant.Domain).Uint8("level", grant.Level).Msg("access granted")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("access granted to %s for domain %s", grant.GranteeID[:16], grant.Domain)}
}

func (app *SageApp) processAccessRevoke(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	revoke := parsedTx.AccessRevoke
	if revoke == nil {
		return &abcitypes.ExecTxResult{Code: 37, Log: "missing access revoke payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	revokerID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 37, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Authorization: revoker must own the domain or ancestor. app-v18 adds an
	// explicit global-admin override, matching the grant path while preserving
	// every pre-v18 block byte-for-byte.
	ownerBindingPresent := revoke.ExpectedOwnerID != "" || revoke.ExpectedOwnedDomain != ""
	if app.postAppV18Rules(height) && ownerBindingPresent {
		currentOwner, currentOwnedDomain, resolveErr := app.badgerStore.ResolveOwningAncestor(revoke.Domain)
		if resolveErr != nil || revoke.ExpectedOwnerID == "" || revoke.ExpectedOwnedDomain == "" ||
			revoke.ExpectedOwnerID != currentOwner || revoke.ExpectedOwnedDomain != currentOwnedDomain {
			return &abcitypes.ExecTxResult{Code: 38, Log: "administrator override rejected: domain ownership changed or was not bound"}
		}
	}
	isOwner, err := app.badgerStore.IsDomainOwnerOrAncestor(revoke.Domain, revokerID)
	adminOverride := false
	if app.postAppV18Rules(height) {
		if revoker, lookupErr := app.badgerStore.GetRegisteredAgent(revokerID); lookupErr == nil {
			adminOverride = revoker.Role == "admin"
		}
	}
	if err != nil || (!isOwner && !adminOverride) {
		return &abcitypes.ExecTxResult{Code: 38, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", revokerID[:16], revoke.Domain)}
	}
	if !isOwner && adminOverride {
		if !ownerBindingPresent {
			return &abcitypes.ExecTxResult{Code: 38, Log: "administrator override rejected: domain ownership changed or was not bound"}
		}
	}

	// Delete grant from BadgerDB
	if delErr := app.badgerStore.DeleteAccessGrant(revoke.Domain, revoke.GranteeID); delErr != nil {
		return &abcitypes.ExecTxResult{Code: 39, Log: fmt.Sprintf("badger write error: %v", delErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "access_revoke",
		data: &accessRevokeData{
			Domain:    revoke.Domain,
			GranteeID: revoke.GranteeID,
			Height:    height,
		},
	})

	app.logger.Info().Str("revoker", revokerID[:16]).Str("grantee", revoke.GranteeID[:16]).Str("domain", revoke.Domain).Msg("access revoked")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("access revoked for %s on domain %s", revoke.GranteeID[:16], revoke.Domain)}
}

func (app *SageApp) processAccessQuery(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	// app-v15: quarantine the on-chain similarity query. It reads app.offchainStore
	// (per-node SQLite; embeddings are staged divergently via SuppCache, so replicas
	// hold NULL and are excluded, and the cosine ranking is a non-stable sort) and
	// folds the result into ExecTxResult.Data — which CometBFT hashes into
	// LastResultsHash → the block header. Different validators therefore return
	// different Data → header/AppHash divergence → BFT halt. On-chain similarity
	// search over per-node vector state can NEVER be deterministic, so post-fork this
	// tx type is a deterministic reject (identical Code 10, empty Data on every node);
	// the real use case is served off the consensus path by the REST query endpoints.
	// Gated on postAppV15Rules for skip-ahead safety; collapses to a no-op on every
	// existing chain (appV15AppliedHeight==0), so pre-activation replay is
	// byte-identical. Mirrors the app-v11 disable of the analogous bootstrapAdminFromSQL
	// per-node read. The reject returns BEFORE any offchainStore read or state effect.
	postV15 := app.postAppV15Rules(height)
	recordAppV15Branch(postV15)
	if postV15 {
		return &abcitypes.ExecTxResult{Code: 10, Log: "access_query disabled on-chain (non-deterministic per-node read); use the REST similarity endpoints"}
	}
	query := parsedTx.AccessQuery
	if query == nil {
		return &abcitypes.ExecTxResult{Code: 40, Log: "missing access query payload"}
	}

	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 40, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Multi-org access gate: checks direct grants, org membership, clearance, federation
	hasAccess, err := app.badgerStore.HasAccessMultiOrg(query.Domain, agentID, 0, blockTime, app.postV8Fork(height))
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 41, Log: fmt.Sprintf("access check error: %v", err)}
	}
	if !hasAccess {
		return &abcitypes.ExecTxResult{Code: 20, Log: "access denied"}
	}

	// Query PostgreSQL for matching memories
	topK := int(query.TopK)
	if topK <= 0 {
		topK = 10
	}

	opts := store.QueryOptions{
		DomainTag:    query.Domain,
		StatusFilter: "committed",
		TopK:         topK,
	}

	ctx := context.Background()
	records, err := app.offchainStore.QuerySimilar(ctx, query.Embedding, opts)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 42, Log: fmt.Sprintf("query error: %v", err)}
	}

	// Return content hashes (not full content)
	memoryIDs := make([]string, 0, len(records))
	for _, r := range records {
		memoryIDs = append(memoryIDs, r.MemoryID)
	}

	// Write audit log
	if logErr := app.badgerStore.AppendAccessLog(height, agentID, query.Domain, "query"); logErr != nil {
		app.logger.Error().Err(logErr).Msg("failed to write access log")
	}

	// Buffer audit log to PostgreSQL
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "access_log",
		data: &store.AccessLogEntry{
			AgentID:     agentID,
			Domain:      query.Domain,
			Action:      "query",
			MemoryIDs:   memoryIDs,
			BlockHeight: height,
			CreatedAt:   blockTime,
		},
	})

	// Encode memory IDs as response data (JSON)
	responseData, _ := json.Marshal(memoryIDs)

	return &abcitypes.ExecTxResult{Code: 0, Data: responseData, Log: fmt.Sprintf("query returned %d memories", len(memoryIDs))}
}

func (app *SageApp) processDomainRegister(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	reg := parsedTx.DomainRegister
	if reg == nil {
		return &abcitypes.ExecTxResult{Code: 43, Log: "missing domain register payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	ownerID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 40, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Check domain doesn't already exist
	existingOwner, err := app.badgerStore.GetDomainOwner(reg.DomainName)
	if err == nil && existingOwner != "" {
		return &abcitypes.ExecTxResult{Code: 44, Log: fmt.Sprintf("domain %s already exists", reg.DomainName)}
	}

	// If parent domain specified, verify registrant owns parent
	if reg.ParentDomain != "" {
		isOwner, parentErr := app.badgerStore.IsDomainOwnerOrAncestor(reg.ParentDomain, ownerID)
		if parentErr != nil || !isOwner {
			return &abcitypes.ExecTxResult{Code: 45, Log: fmt.Sprintf("access denied: %s does not own parent domain %s", ownerID[:16], reg.ParentDomain)}
		}
	}

	// Write to BadgerDB
	if regErr := app.badgerStore.RegisterDomain(reg.DomainName, ownerID, reg.ParentDomain, height); regErr != nil {
		return &abcitypes.ExecTxResult{Code: 46, Log: fmt.Sprintf("badger write error: %v", regErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "domain_register",
		data: &store.DomainEntry{
			DomainName:    reg.DomainName,
			OwnerAgentID:  ownerID,
			ParentDomain:  reg.ParentDomain,
			Description:   reg.Description,
			CreatedHeight: height,
			CreatedAt:     blockTime,
		},
	})

	app.logger.Info().Str("domain", reg.DomainName).Str("owner", ownerID[:16]).Msg("domain registered")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("domain %s registered", reg.DomainName)}
}

// isOrgAdmin checks if an agent is an admin of the given organization.
func (app *SageApp) isOrgAdmin(orgID, agentID string) bool {
	_, role, err := app.badgerStore.GetMemberClearance(orgID, agentID)
	return err == nil && role == "admin"
}

// verifyAgentIdentity extracts and verifies the agent's Ed25519 identity proof
// embedded in the transaction. Returns the verified agent ID (hex pubkey) or error.
// This is the critical on-chain identity verification — ABCI trusts NO payload fields
// for agent identity. The agent's original Ed25519 signature is re-verified here.
func verifyAgentIdentity(parsedTx *tx.ParsedTx) (string, error) {
	if len(parsedTx.AgentPubKey) == 0 || len(parsedTx.AgentSig) == 0 || len(parsedTx.AgentBodyHash) == 0 {
		return "", fmt.Errorf("no agent identity proof in transaction")
	}
	return auth.VerifyAgentProof(parsedTx.AgentPubKey, parsedTx.AgentSig, parsedTx.AgentBodyHash, parsedTx.AgentTimestamp, parsedTx.AgentNonce)
}

func (app *SageApp) processOrgRegister(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	reg := parsedTx.OrgRegister
	if reg == nil {
		return &abcitypes.ExecTxResult{Code: 50, Log: "missing org register payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	adminID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 50, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Generate deterministic org ID if not provided
	orgID := reg.OrgID
	if orgID == "" {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", adminID, reg.Name, height)))
		orgID = hex.EncodeToString(h[:16])
	}

	// Check org doesn't already exist
	_, _, getErr := app.badgerStore.GetOrg(orgID)
	if getErr == nil {
		return &abcitypes.ExecTxResult{Code: 51, Log: fmt.Sprintf("org %s already exists", orgID)}
	}

	// Register org on-chain
	if regErr := app.badgerStore.RegisterOrg(orgID, reg.Name, reg.Description, adminID, height); regErr != nil {
		return &abcitypes.ExecTxResult{Code: 52, Log: fmt.Sprintf("badger write error: %v", regErr)}
	}

	// Auto-add admin as member with TOP_SECRET clearance
	if addErr := app.badgerStore.AddOrgMember(orgID, adminID, uint8(tx.ClearanceTopSecret), "admin", height); addErr != nil {
		app.logger.Error().Err(addErr).Msg("failed to add admin as org member")
	}

	// Buffer PostgreSQL writes
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "org_register",
		data: &store.OrgEntry{
			OrgID: orgID, Name: reg.Name, Description: reg.Description,
			AdminAgentID: adminID, CreatedHeight: height, CreatedAt: blockTime,
		},
	})
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "org_member",
		data: &store.OrgMemberEntry{
			OrgID: orgID, AgentID: adminID, Clearance: store.ClearanceTopSecret,
			Role: "admin", CreatedHeight: height, CreatedAt: blockTime,
		},
	})

	app.logger.Info().Str("org_id", orgID).Str("name", reg.Name).Str("admin", adminID[:16]).Msg("organization registered")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(orgID), Log: fmt.Sprintf("org %s registered", orgID)}
}

func (app *SageApp) processOrgAddMember(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	add := parsedTx.OrgAddMember
	if add == nil {
		return &abcitypes.ExecTxResult{Code: 53, Log: "missing org add member payload"}
	}

	// Verify agent identity on-chain — only org admins can add members.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 54, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}
	if !app.isOrgAdmin(add.OrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 54, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], add.OrgID)}
	}

	// Validate clearance level (0-4)
	if uint8(add.Clearance) > 4 {
		return &abcitypes.ExecTxResult{Code: 55, Log: "invalid clearance level: must be 0-4"}
	}

	// Add member on-chain
	if addErr := app.badgerStore.AddOrgMember(add.OrgID, add.AgentID, uint8(add.Clearance), add.Role, height); addErr != nil {
		return &abcitypes.ExecTxResult{Code: 55, Log: fmt.Sprintf("badger write error: %v", addErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "org_member",
		data: &store.OrgMemberEntry{
			OrgID: add.OrgID, AgentID: add.AgentID, Clearance: store.ClearanceLevel(add.Clearance),
			Role: add.Role, CreatedHeight: height, CreatedAt: blockTime,
		},
	})

	app.logger.Info().Str("org_id", add.OrgID).Str("agent", add.AgentID[:16]).Str("role", add.Role).Msg("member added to org")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("member %s added to org %s", add.AgentID[:16], add.OrgID)}
}

func (app *SageApp) processOrgRemoveMember(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	rem := parsedTx.OrgRemoveMember
	if rem == nil {
		return &abcitypes.ExecTxResult{Code: 56, Log: "missing org remove member payload"}
	}

	// Verify agent identity on-chain — only org admins can remove members.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 57, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}
	if !app.isOrgAdmin(rem.OrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 57, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], rem.OrgID)}
	}

	// Remove member on-chain
	if remErr := app.badgerStore.RemoveOrgMember(rem.OrgID, rem.AgentID); remErr != nil {
		return &abcitypes.ExecTxResult{Code: 57, Log: fmt.Sprintf("badger write error: %v", remErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "org_member_remove",
		data: &orgMemberRemoveData{
			OrgID: rem.OrgID, AgentID: rem.AgentID, Height: height,
		},
	})

	app.logger.Info().Str("org_id", rem.OrgID).Str("agent", rem.AgentID[:16]).Msg("member removed from org")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("member %s removed from org %s", rem.AgentID[:16], rem.OrgID)}
}

func (app *SageApp) processOrgSetClearance(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	sc := parsedTx.OrgSetClearance
	if sc == nil {
		return &abcitypes.ExecTxResult{Code: 58, Log: "missing org set clearance payload"}
	}

	// Verify agent identity on-chain — only org admins can change clearances.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 59, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}
	if !app.isOrgAdmin(sc.OrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 59, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], sc.OrgID)}
	}

	// Validate clearance level (0-4)
	if uint8(sc.Clearance) > 4 {
		return &abcitypes.ExecTxResult{Code: 59, Log: "invalid clearance level: must be 0-4"}
	}

	// Update clearance on-chain
	if setErr := app.badgerStore.SetMemberClearance(sc.OrgID, sc.AgentID, uint8(sc.Clearance)); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 59, Log: fmt.Sprintf("badger write error: %v", setErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "org_member_clearance",
		data: &orgClearanceData{
			OrgID: sc.OrgID, AgentID: sc.AgentID, Clearance: store.ClearanceLevel(sc.Clearance),
		},
	})

	app.logger.Info().Str("org_id", sc.OrgID).Str("agent", sc.AgentID[:16]).Uint8("clearance", uint8(sc.Clearance)).Msg("member clearance updated")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("clearance set for %s in org %s", sc.AgentID[:16], sc.OrgID)}
}

func (app *SageApp) processFederationPropose(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	prop := parsedTx.FederationPropose
	if prop == nil {
		return &abcitypes.ExecTxResult{Code: 60, Log: "missing federation propose payload"}
	}

	// Verify agent identity on-chain — only org admins can propose federations.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 61, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Verify the sender is a member of the proposer org and is its admin.
	// Multi-org members can act as admin in any org they belong to.
	memberOf, memberErr := app.badgerStore.IsAgentInOrg(senderID, prop.ProposerOrgID)
	if memberErr != nil || !memberOf {
		return &abcitypes.ExecTxResult{Code: 61, Log: fmt.Sprintf("agent %s is not a member of proposer org %s", senderID[:16], prop.ProposerOrgID)}
	}
	if !app.isOrgAdmin(prop.ProposerOrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 61, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], prop.ProposerOrgID)}
	}

	// Generate deterministic federation ID
	h := sha256.Sum256([]byte(fmt.Sprintf("%s:%s:%d", prop.ProposerOrgID, prop.TargetOrgID, height)))
	fedID := hex.EncodeToString(h[:16])

	// Store federation as proposed (pass AllowedDepts via variadic arg)
	if setErr := app.badgerStore.SetFederation(fedID, prop.ProposerOrgID, prop.TargetOrgID,
		prop.AllowedDomains, uint8(prop.MaxClearance), prop.ExpiresAt, prop.RequiresApproval, "proposed", prop.AllowedDepts); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 62, Log: fmt.Sprintf("badger write error: %v", setErr)}
	}

	// Buffer PostgreSQL write
	var expiresAt *time.Time
	if prop.ExpiresAt > 0 {
		t := time.Unix(prop.ExpiresAt, 0)
		expiresAt = &t
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "federation",
		data: &store.FederationEntry{
			FederationID: fedID, ProposerOrgID: prop.ProposerOrgID, TargetOrgID: prop.TargetOrgID,
			AllowedDomains: prop.AllowedDomains, AllowedDepts: prop.AllowedDepts,
			MaxClearance: store.ClearanceLevel(prop.MaxClearance),
			ExpiresAt:    expiresAt, RequiresApproval: prop.RequiresApproval,
			Status: "proposed", CreatedHeight: height, CreatedAt: blockTime,
		},
	})

	app.logger.Info().Str("federation_id", fedID).Str("proposer", prop.ProposerOrgID[:16]).Str("target", prop.TargetOrgID[:16]).Msg("federation proposed")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(fedID), Log: fmt.Sprintf("federation %s proposed", fedID)}
}

func (app *SageApp) processFederationApprove(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	approve := parsedTx.FederationApprove
	if approve == nil {
		return &abcitypes.ExecTxResult{Code: 63, Log: "missing federation approve payload"}
	}

	// Verify agent identity on-chain.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Get federation details
	_, targetOrg, _, _, status, err := app.badgerStore.GetFederation(approve.FederationID)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("federation %s not found", approve.FederationID)}
	}

	// Verify status is "proposed"
	if status != "proposed" {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("federation %s is %s, not proposed", approve.FederationID, status)}
	}

	// Verify the sender is a member of the target org and is its admin.
	// Multi-org members can approve federations on behalf of any org they
	// belong to as admin.
	memberOf, memberErr := app.badgerStore.IsAgentInOrg(senderID, targetOrg)
	if memberErr != nil || !memberOf {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("agent %s is not a member of target org %s", senderID[:16], targetOrg)}
	}
	if !app.isOrgAdmin(targetOrg, senderID) {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("access denied: %s is not admin of target org %s", senderID[:16], targetOrg)}
	}

	// Update federation status to "active"
	if updateErr := app.badgerStore.UpdateFederationStatus(approve.FederationID, "active"); updateErr != nil {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("badger write error: %v", updateErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "federation_approve",
		data: &federationApproveData{
			FederationID: approve.FederationID, Height: height,
		},
	})

	app.logger.Info().Str("federation_id", approve.FederationID).Str("approver_org", approve.ApproverOrgID[:16]).Msg("federation approved")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("federation %s approved", approve.FederationID)}
}

func (app *SageApp) processFederationRevoke(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	revoke := parsedTx.FederationRevoke
	if revoke == nil {
		return &abcitypes.ExecTxResult{Code: 65, Log: "missing federation revoke payload"}
	}

	// Verify agent identity on-chain.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Get federation details
	proposerOrg, targetOrg, _, _, status, err := app.badgerStore.GetFederation(revoke.FederationID)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("federation %s not found", revoke.FederationID)}
	}

	// Must be active to revoke
	if status != "active" && status != "proposed" {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("federation %s is %s, cannot revoke", revoke.FederationID, status)}
	}

	// Verify the sender is a member of either federated org and is its admin.
	// Multi-org members can revoke from whichever side of the federation they
	// hold an admin role on.
	inProposer, _ := app.badgerStore.IsAgentInOrg(senderID, proposerOrg)
	inTarget, _ := app.badgerStore.IsAgentInOrg(senderID, targetOrg)
	var revokerOrg string
	switch {
	case inProposer && app.isOrgAdmin(proposerOrg, senderID):
		revokerOrg = proposerOrg
	case inTarget && app.isOrgAdmin(targetOrg, senderID):
		revokerOrg = targetOrg
	default:
		return &abcitypes.ExecTxResult{Code: 66, Log: "only admins of either federated org can revoke federations"}
	}

	// Update federation status to "revoked"
	if updateErr := app.badgerStore.UpdateFederationStatus(revoke.FederationID, "revoked"); updateErr != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("badger write error: %v", updateErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "federation_revoke",
		data: &federationRevokeData{
			FederationID: revoke.FederationID, Height: height,
		},
	})

	app.logger.Info().Str("federation_id", revoke.FederationID).Str("revoker_org", revokerOrg[:16]).Msg("federation revoked")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("federation %s revoked", revoke.FederationID)}
}

func (app *SageApp) processDeptRegister(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	reg := parsedTx.DeptRegister
	if reg == nil {
		return &abcitypes.ExecTxResult{Code: 70, Log: "missing dept register payload"}
	}

	// Verify agent identity on-chain — only org admins can create departments.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 71, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Verify org exists
	_, _, getErr := app.badgerStore.GetOrg(reg.OrgID)
	if getErr != nil {
		return &abcitypes.ExecTxResult{Code: 71, Log: fmt.Sprintf("org %s not found", reg.OrgID)}
	}

	// Verify sender is org admin
	if !app.isOrgAdmin(reg.OrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 71, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], reg.OrgID)}
	}

	// Use provided DeptID or generate deterministic one
	deptID := reg.DeptID
	if deptID == "" {
		h := sha256.Sum256([]byte(reg.OrgID + reg.DeptName))
		deptID = hex.EncodeToString(h[:16])
	}

	// Check dept doesn't already exist
	_, _, deptErr := app.badgerStore.GetDept(reg.OrgID, deptID)
	if deptErr == nil {
		return &abcitypes.ExecTxResult{Code: 72, Log: fmt.Sprintf("dept %s already exists in org %s", deptID, reg.OrgID)}
	}

	// Register department on-chain
	if regErr := app.badgerStore.RegisterDept(reg.OrgID, deptID, reg.DeptName, reg.Description, reg.ParentDept, height); regErr != nil {
		return &abcitypes.ExecTxResult{Code: 73, Log: fmt.Sprintf("badger write error: %v", regErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "dept_register",
		data: &store.DeptEntry{
			OrgID: reg.OrgID, DeptID: deptID, DeptName: reg.DeptName,
			Description: reg.Description, ParentDept: reg.ParentDept,
			CreatedHeight: height, CreatedAt: blockTime,
		},
	})

	app.logger.Info().Str("org_id", reg.OrgID).Str("dept_id", deptID).Str("name", reg.DeptName).Msg("department registered")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(deptID), Log: fmt.Sprintf("dept %s registered in org %s", deptID, reg.OrgID)}
}

func (app *SageApp) processDeptAddMember(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	add := parsedTx.DeptAddMember
	if add == nil {
		return &abcitypes.ExecTxResult{Code: 74, Log: "missing dept add member payload"}
	}

	// Verify agent identity on-chain — only org admins can add dept members.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 75, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}
	if !app.isOrgAdmin(add.OrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 75, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], add.OrgID)}
	}

	// Verify dept exists
	_, _, deptErr := app.badgerStore.GetDept(add.OrgID, add.DeptID)
	if deptErr != nil {
		return &abcitypes.ExecTxResult{Code: 76, Log: fmt.Sprintf("dept %s not found in org %s", add.DeptID, add.OrgID)}
	}

	// Validate clearance level (0-4)
	if uint8(add.Clearance) > 4 {
		return &abcitypes.ExecTxResult{Code: 76, Log: "invalid clearance level: must be 0-4"}
	}

	// Add member to department on-chain
	if addErr := app.badgerStore.AddDeptMember(add.OrgID, add.DeptID, add.AgentID, uint8(add.Clearance), add.Role, height); addErr != nil {
		return &abcitypes.ExecTxResult{Code: 77, Log: fmt.Sprintf("badger write error: %v", addErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "dept_member",
		data: &store.DeptMemberEntry{
			OrgID: add.OrgID, DeptID: add.DeptID, AgentID: add.AgentID,
			Clearance: store.ClearanceLevel(add.Clearance), Role: add.Role,
			CreatedHeight: height, CreatedAt: blockTime,
		},
	})

	app.logger.Info().Str("org_id", add.OrgID).Str("dept_id", add.DeptID).Str("agent", add.AgentID[:16]).Msg("member added to dept")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("member %s added to dept %s", add.AgentID[:16], add.DeptID)}
}

func (app *SageApp) processDeptRemoveMember(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	rem := parsedTx.DeptRemoveMember
	if rem == nil {
		return &abcitypes.ExecTxResult{Code: 78, Log: "missing dept remove member payload"}
	}

	// Verify agent identity on-chain — only org admins can remove dept members.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 79, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}
	if !app.isOrgAdmin(rem.OrgID, senderID) {
		return &abcitypes.ExecTxResult{Code: 79, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], rem.OrgID)}
	}

	// Remove member from department on-chain
	if remErr := app.badgerStore.RemoveDeptMember(rem.OrgID, rem.DeptID, rem.AgentID); remErr != nil {
		return &abcitypes.ExecTxResult{Code: 79, Log: fmt.Sprintf("badger write error: %v", remErr)}
	}

	// Buffer PostgreSQL write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "dept_member_remove",
		data: &deptMemberRemoveData{
			OrgID: rem.OrgID, DeptID: rem.DeptID, AgentID: rem.AgentID, Height: height,
		},
	})

	app.logger.Info().Str("org_id", rem.OrgID).Str("dept_id", rem.DeptID).Str("agent", rem.AgentID[:16]).Msg("member removed from dept")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("member %s removed from dept %s", rem.AgentID[:16], rem.DeptID)}
}

func (app *SageApp) processAgentRegister(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	reg := parsedTx.AgentRegister
	if reg == nil {
		return &abcitypes.ExecTxResult{Code: 60, Log: "missing agent register payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 60, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Use authenticated agent ID if payload didn't specify one
	regAgentID := reg.AgentID
	if regAgentID == "" {
		regAgentID = agentID
	}

	// Idempotent: if already registered, still buffer offchain write to backfill on_chain_height.
	// IMPORTANT (v6.8.4): copy ALL on-chain fields from the existing record — including OrgID,
	// DeptID, DomainAccess, and VisibleAgents — into the AgentEntry. The flush handler at
	// case "agent_register" falls back to UpdateAgent on UNIQUE-constraint failure, and
	// UpdateAgent writes the whole row. Omitting permission fields here silently zeros out
	// network_agents.{org_id, dept_id, domain_access, visible_agents} in the SQL mirror on
	// every re-register, breaking cross-agent visibility for any agent whose bridge calls
	// register_agent() at startup after permissions were granted.
	if app.badgerStore.IsAgentRegistered(regAgentID) {
		existing, _ := app.badgerStore.GetRegisteredAgent(regAgentID)
		if existing != nil {
			app.pendingWrites = append(app.pendingWrites, pendingWrite{
				writeType: "agent_register",
				data: &store.AgentEntry{
					AgentID:        regAgentID,
					Name:           existing.Name,
					RegisteredName: existing.RegisteredName,
					Role:           existing.Role,
					BootBio:        existing.BootBio,
					Provider:       existing.Provider,
					P2PAddress:     existing.P2PAddress,
					Status:         "active",
					Clearance:      int(existing.Clearance),
					OrgID:          existing.OrgID,
					DeptID:         existing.DeptID,
					DomainAccess:   existing.DomainAccess,
					VisibleAgents:  existing.VisibleAgents,
					OnChainHeight:  existing.RegisteredAt,
					CreatedAt:      blockTime,
				},
			})
			return &abcitypes.ExecTxResult{Code: 0, Data: []byte(regAgentID), Log: fmt.Sprintf("agent %s already registered", regAgentID[:16])}
		}
	}

	// Register agent on-chain (BadgerDB)
	role := reg.Role
	if role == "" {
		role = "member"
	}
	// app-v9: close the self-grantable-admin hole. Pre-fork, processAgentRegister
	// took role straight from the wire, so ANY key could self-register as "admin"
	// with only a self-signature (audited at bootstrapAdminFromSQL: "pre-fix,
	// anyone could grab admin on a fresh chain by being first to call
	// /v1/agent/register with role=admin"). The 2/3 governance quorum + app-v8
	// consensus sig-verification are the REAL upgrade gate, so this is
	// defence-in-depth, but it removes the cheap path to a privileged role.
	//
	// Post-fork the global "admin" role can no longer be MINTED over the wire; a
	// self-registration claiming it is silently downgraded to "member" (the tx
	// still succeeds — gentler than a reject, and a reject would turn a
	// previously-accepted tx into a rejected one, a needless replay regression).
	// Legitimate admins are unaffected: an already-registered admin re-registering
	// hits the idempotent branch above (existing.Role preserved), and a fresh
	// operator admin still arrives via the operator-blessed bootstrapAdminFromSQL
	// path (processAgentSetPermission). Org-scoped roles are set elsewhere
	// (processOrgAddMember, gated by org-admin) and are untouched.
	if role == "admin" {
		postV9 := app.postAppV9Rules(height)
		recordAppV9Branch(postV9) // both branches counted so the dashboard sees the pre/post split
		if postV9 {
			app.logger.Warn().Str("agent_id", regAgentID[:16]).Msg("app-v9: wire-supplied role=admin on self-registration downgraded to member")
			role = "member"
		}
	}
	if regErr := app.badgerStore.RegisterAgent(regAgentID, reg.Name, role, reg.BootBio, reg.Provider, reg.P2PAddress, height); regErr != nil {
		return &abcitypes.ExecTxResult{Code: 61, Log: fmt.Sprintf("badger write error: %v", regErr)}
	}

	// Buffer offchain write — create agent in SQLite/Postgres
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "agent_register",
		data: &store.AgentEntry{
			AgentID:        regAgentID,
			Name:           reg.Name,
			RegisteredName: reg.Name, // Immutable — original identity preserved forever
			Role:           role,
			BootBio:        reg.BootBio,
			Provider:       reg.Provider,
			P2PAddress:     reg.P2PAddress,
			Status:         "active",
			Clearance:      1, // Default: INTERNAL
			OnChainHeight:  height,
			CreatedAt:      blockTime,
		},
	})

	app.logger.Info().Str("agent_id", regAgentID[:16]).Str("name", reg.Name).Str("provider", reg.Provider).Msg("agent registered on-chain")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(regAgentID), Log: fmt.Sprintf("agent %s registered", regAgentID[:16])}
}

func (app *SageApp) processAgentUpdate(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	upd := parsedTx.AgentUpdateTx
	if upd == nil {
		return &abcitypes.ExecTxResult{Code: 62, Log: "missing agent update payload"}
	}

	// Verify agent identity — only the agent itself can update its own metadata.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 62, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	targetID := upd.AgentID
	if targetID == "" {
		targetID = senderID
	}

	// Self-update only
	if senderID != targetID {
		return &abcitypes.ExecTxResult{Code: 63, Log: fmt.Sprintf("access denied: %s cannot update agent %s", senderID[:16], targetID[:16])}
	}

	// Agent must be registered
	if !app.badgerStore.IsAgentRegistered(targetID) {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("agent %s not registered", targetID[:16])}
	}
	if app.appV20StrictFinalize {
		current, getErr := app.badgerStore.GetRegisteredAgent(targetID)
		if getErr != nil {
			return &abcitypes.ExecTxResult{Code: 65, Log: fmt.Sprintf("agent record lookup failed: %v", getErr)}
		}
		candidate := *current
		candidate.Name = upd.Name
		candidate.BootBio = upd.BootBio
		if sizeErr := validateAppV20AgentRecord(&candidate); sizeErr != nil {
			return &abcitypes.ExecTxResult{Code: appV20ResourceLimitCode, Log: "app-v20 resource limit: " + sizeErr.Error()}
		}
	}

	// Update mutable display name + bio only.
	// RegisteredName is the permanent on-chain identity and is NEVER modified by AgentUpdate.
	if updErr := app.badgerStore.UpdateAgentMeta(targetID, upd.Name, upd.BootBio); updErr != nil {
		return &abcitypes.ExecTxResult{Code: 65, Log: fmt.Sprintf("badger write error: %v", updErr)}
	}

	// Buffer offchain write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "agent_update",
		data: &agentUpdateData{
			AgentID: targetID,
			Name:    upd.Name,
			BootBio: upd.BootBio,
		},
	})

	app.logger.Info().Str("agent_id", targetID[:16]).Str("name", upd.Name).Msg("agent metadata updated")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(targetID), Log: fmt.Sprintf("agent %s updated", targetID[:16])}
}

// bootstrapAdminFromSQL handles the v6.8.5 admin-bootstrap escape hatch.
//
// Some deployment paths write `network_agents.role='admin'` to SQL out of
// band and rely on a fire-and-forget chain register tx to materialize the
// matching on-chain record (cmd/sage-gui/node.go:1145 startup seeding,
// web/network_handler.go:163 GUI Create Agent form). When the broadcast
// silently drops — CometBFT not yet ready, network blip, retry budget
// exhausted — SQL has the admin row but BadgerDB doesn't, and every
// subsequent admin-only op fails with "sender agent X not registered".
// Levelup's prod chain hit exactly this state (visibility kept working
// only via permissions baked in before the divergence).
//
// This helper detects the SQL-but-not-chain split and self-heals it.
// On match (SQL row exists with role="admin") we register the sender
// on-chain at the current height with the SQL-derived role, mirror the
// SQL clearance/orgs onto BadgerDB, and buffer an agent_register
// pendingWrite so on_chain_height is reconciled in the SQL mirror.
//
// Security invariants (audited 2026-05-03 — security@ for the picky):
//   - Trigger requires `sqlAgent.Role == "admin"` (strict equality). No
//     role-string fuzzing, no escalation from member/observer.
//   - Only fires when BadgerDB has NO record at all for the sender.
//     If BadgerDB knows the agent (any role), this is a no-op — no
//     downgrade-then-upgrade attack.
//   - SQL `role='admin'` is the existing trust source: only operator-
//     authenticated paths can set it (sage-gui startup with local
//     validator key, GUI Create Agent under authMiddleware, direct
//     filesystem write). This fix introduces no new write path.
//   - Net hardening: pre-fix, anyone could grab admin on a fresh chain
//     by being first to call /v1/agent/register with role="admin".
//     Post-fix, an unregistered set_agent_permission caller still has
//     to back their claim with a pre-existing operator-blessed SQL row.
//   - The downstream auth gates in processAgentSetPermission (clearance
//     ceiling, org-scope check, target-org consent) still run after the
//     bootstrap. Auto-register only fixes the "is sender on chain at
//     all" question, not "what is the sender allowed to do".
//
// Returns the freshly registered OnChainAgent (suitable for the caller's
// downstream auth check) and true on success, or (nil, false) if SQL has
// no admin record for senderID.
func (app *SageApp) bootstrapAdminFromSQL(senderID string, height int64, blockTime time.Time) (*store.OnChainAgent, bool) {
	// app-v11 (#36): disable the SQL→chain admin bootstrap on the consensus path.
	// It reads app.offchainStore (per-node SQLite, seeded divergently — each node
	// admins its own validator slot keyed by its own agent.key) and writes a
	// BadgerDB agent: record that enters the AppHash, so on a multi-validator chain
	// it diverges the AppHash and halts consensus. Post-app-v11 the chain-admin is
	// established deterministically at the app-v11 activation block (#35) instead; callers fall through
	// to their existing not-registered rejection. Gated on postAppV11Rules
	// (subsumption-OR) so a skip-ahead chain that activates a higher fork without
	// app-v11 still suppresses it. Collapses to a no-op on every existing chain
	// (appV11AppliedHeight==0), so historical blocks replay byte-identically.
	//
	// An existing chain keeps any admin it already materialized (the agent: record
	// persists in BadgerDB) — only the FUTURE self-heal is removed. The admin-less
	// case is covered deterministically at the app-v11 activation block by
	// materializeAppV11Admin (from the committed validator set), so disabling this
	// per-node-SQL path here cannot strand a chain.
	postV11 := app.postAppV11Rules(height)
	recordAppV11Branch(postV11)
	if postV11 {
		return nil, false
	}
	if app.offchainStore == nil {
		return nil, false
	}
	sqlAgent, err := app.offchainStore.GetAgent(context.Background(), senderID)
	if err != nil || sqlAgent == nil || sqlAgent.Role != "admin" {
		return nil, false
	}

	// Register on-chain with the SQL-derived role. Use the SQL display
	// name as both Name and (via RegisterAgent) RegisteredName.
	if regErr := app.badgerStore.RegisterAgent(senderID, sqlAgent.Name, "admin", sqlAgent.BootBio, sqlAgent.Provider, sqlAgent.P2PAddress, height); regErr != nil {
		app.logger.Warn().Err(regErr).Str("agent_id", senderID[:16]).Msg("admin bootstrap: badger RegisterAgent failed")
		return nil, false
	}

	// Mirror SQL's clearance + org assignments onto the on-chain record so
	// downstream auth checks (clearance ceiling, org-scope) match SQL.
	// Skip when SQL has nothing to mirror — RegisterAgent already seeded
	// clearance=1 (INTERNAL default).
	if sqlAgent.Clearance > 0 || sqlAgent.OrgID != "" || sqlAgent.DeptID != "" || sqlAgent.DomainAccess != "" || sqlAgent.VisibleAgents != "" {
		clearance := uint8(sqlAgent.Clearance) // #nosec G115 -- clearance is 0-4
		if permErr := app.badgerStore.SetAgentPermission(senderID, clearance, sqlAgent.DomainAccess, sqlAgent.VisibleAgents, sqlAgent.OrgID, sqlAgent.DeptID); permErr != nil {
			app.logger.Warn().Err(permErr).Str("agent_id", senderID[:16]).Msg("admin bootstrap: badger SetAgentPermission failed (continuing — chain register succeeded)")
		}
	}

	// Buffer SQL flush so on_chain_height (and any drift on RegisteredName /
	// permission columns) is reconciled. Mirrors the v6.8.4 idempotent
	// re-register path's full-field copy so the SQL mirror doesn't get
	// blanked.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "agent_register",
		data: &store.AgentEntry{
			AgentID:        senderID,
			Name:           sqlAgent.Name,
			RegisteredName: sqlAgent.RegisteredName,
			Role:           "admin",
			BootBio:        sqlAgent.BootBio,
			Provider:       sqlAgent.Provider,
			P2PAddress:     sqlAgent.P2PAddress,
			Status:         "active",
			Clearance:      sqlAgent.Clearance,
			OrgID:          sqlAgent.OrgID,
			DeptID:         sqlAgent.DeptID,
			DomainAccess:   sqlAgent.DomainAccess,
			VisibleAgents:  sqlAgent.VisibleAgents,
			OnChainHeight:  height,
			CreatedAt:      blockTime,
		},
	})

	app.logger.Info().Str("agent_id", senderID[:16]).Str("name", sqlAgent.Name).Int64("height", height).Msg("admin bootstrap: auto-registered SQL-trusted admin on-chain (v6.8.5)")

	onChain, getErr := app.badgerStore.GetRegisteredAgent(senderID)
	if getErr != nil || onChain == nil {
		return nil, false
	}
	return onChain, true
}

func (app *SageApp) processAgentSetPermission(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	perm := parsedTx.AgentSetPermission
	if perm == nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: "missing agent set permission payload"}
	}

	// Verify sender's on-chain identity (Ed25519 proof embedded in tx).
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Sender must be registered. v6.8.5: when BadgerDB has no record but
	// SQL has them with role="admin", auto-register on chain (see
	// bootstrapAdminFromSQL for the security invariants). Symmetric with
	// v6.8.4's idempotent-re-register graceful-edge-case philosophy.
	senderAgent, senderErr := app.badgerStore.GetRegisteredAgent(senderID)
	if senderErr != nil {
		if recovered, ok := app.bootstrapAdminFromSQL(senderID, height, blockTime); ok {
			senderAgent = recovered
		} else {
			return &abcitypes.ExecTxResult{Code: 67, Log: fmt.Sprintf("sender agent %s not registered", senderID[:16])}
		}
	}

	// Target agent must be registered (read first so we can compute auth against its org).
	if !app.badgerStore.IsAgentRegistered(perm.AgentID) {
		return &abcitypes.ExecTxResult{Code: 68, Log: fmt.Sprintf("target agent %s not registered", perm.AgentID[:16])}
	}

	// Read the target's current on-chain permissions so we can detect
	// privilege-changing fields (clearance raise, org rewrite, etc.) that
	// require additional authority beyond bare "can write to this row".
	targetAgent, targetErr := app.badgerStore.GetRegisteredAgent(perm.AgentID)
	if targetErr != nil {
		return &abcitypes.ExecTxResult{Code: 68, Log: fmt.Sprintf("target agent %s lookup failed", perm.AgentID[:16])}
	}

	// Auth model: a caller may write `agent_set_permission` on a target if
	// any of these hold:
	//   1. Self-set — the caller IS the target. Cannot raise own clearance,
	//      cannot move themselves into an org they don't already belong to.
	//   2. Global admin — caller's on-chain `Role == "admin"` (legacy
	//      deployment-admin from genesis bootstrap). Treated as max
	//      clearance.
	//   3. Org admin — caller is an org member with role="admin" in an org
	//      the target also belongs to. Cannot raise the target's clearance
	//      above the caller's own clearance in that org. Cannot move the
	//      target into an org the caller is not also an admin of.
	const globalAdminClearance uint8 = 4 // TopSecret — admin's effective ceiling
	authMode := ""
	callerMaxClearance := senderAgent.Clearance
	switch {
	case senderID == perm.AgentID:
		authMode = "self"
	case senderAgent.Role == "admin":
		authMode = "global"
		callerMaxClearance = globalAdminClearance
	default:
		targetOrgs, listErr := app.badgerStore.ListAgentOrgs(perm.AgentID)
		if listErr == nil {
			for _, orgID := range targetOrgs {
				cl, role, mErr := app.badgerStore.GetMemberClearance(orgID, senderID)
				if mErr == nil && role == "admin" {
					authMode = "org"
					if cl > callerMaxClearance {
						callerMaxClearance = cl
					}
				}
			}
		}
	}
	if authMode == "" {
		return &abcitypes.ExecTxResult{Code: 67, Log: "access denied"}
	}

	// Clamp: new clearance must not exceed the caller's effective ceiling.
	// Self-set callers can lower but not raise; org admins are bounded by
	// their own clearance in the shared org; global admins go to TopSecret.
	if perm.Clearance > callerMaxClearance {
		return &abcitypes.ExecTxResult{Code: 67, Log: "access denied"}
	}

	// OrgID change requires extra authority: an org admin moving the target
	// to a different org must also be admin of the destination org; a
	// self-setter must already belong to the destination org (no
	// self-onboarding into restricted orgs).
	if perm.OrgID != "" && perm.OrgID != targetAgent.OrgID {
		switch authMode {
		case "org":
			_, role, _ := app.badgerStore.GetMemberClearance(perm.OrgID, senderID)
			if role != "admin" {
				return &abcitypes.ExecTxResult{Code: 67, Log: "access denied"}
			}
		case "self":
			members, _ := app.badgerStore.ListAgentOrgs(senderID)
			inDest := false
			for _, m := range members {
				if m == perm.OrgID {
					inDest = true
					break
				}
			}
			if !inDest {
				return &abcitypes.ExecTxResult{Code: 67, Log: "access denied"}
			}
		}
	}
	if app.appV20StrictFinalize {
		candidate := *targetAgent
		candidate.Clearance = perm.Clearance
		candidate.DomainAccess = perm.DomainAccess
		candidate.VisibleAgents = perm.VisibleAgents
		if perm.OrgID != "" {
			candidate.OrgID = perm.OrgID
		}
		if perm.DeptID != "" {
			candidate.DeptID = perm.DeptID
		}
		if sizeErr := validateAppV20AgentRecord(&candidate); sizeErr != nil {
			return &abcitypes.ExecTxResult{Code: appV20ResourceLimitCode, Log: "app-v20 resource limit: " + sizeErr.Error()}
		}
	}

	// Update permissions on-chain
	if permErr := app.badgerStore.SetAgentPermission(perm.AgentID, perm.Clearance, perm.DomainAccess, perm.VisibleAgents, perm.OrgID, perm.DeptID); permErr != nil {
		return &abcitypes.ExecTxResult{Code: 69, Log: fmt.Sprintf("badger write error: %v", permErr)}
	}

	// Buffer offchain write
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "agent_permission",
		data: &agentPermissionData{
			AgentID:       perm.AgentID,
			Clearance:     int(perm.Clearance),
			DomainAccess:  perm.DomainAccess,
			VisibleAgents: perm.VisibleAgents,
			OrgID:         perm.OrgID,
			DeptID:        perm.DeptID,
		},
	})

	app.logger.Info().Str("agent_id", perm.AgentID[:16]).Uint8("clearance", perm.Clearance).Str("set_by", senderID[:16]).Msg("agent permissions updated")

	return &abcitypes.ExecTxResult{Code: 0, Data: []byte(perm.AgentID), Log: fmt.Sprintf("agent %s permissions updated", perm.AgentID[:16])}
}

func (app *SageApp) processMemoryReassign(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	reassign := parsedTx.MemoryReassign
	if reassign == nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: "missing memory reassign payload"}
	}

	// Verify sender is admin — only admins can reassign memories.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// v6.8.5: same admin-bootstrap escape hatch as processAgentSetPermission.
	senderAgent, senderErr := app.badgerStore.GetRegisteredAgent(senderID)
	if senderErr != nil {
		if recovered, ok := app.bootstrapAdminFromSQL(senderID, height, blockTime); ok {
			senderAgent = recovered
		} else {
			return &abcitypes.ExecTxResult{Code: 67, Log: fmt.Sprintf("sender agent %s not registered", senderID[:16])}
		}
	}
	if senderAgent.Role != "admin" {
		return &abcitypes.ExecTxResult{Code: 67, Log: fmt.Sprintf("access denied: %s is not an admin", senderID[:16])}
	}

	// Target agent must be registered on-chain
	if !app.badgerStore.IsAgentRegistered(reassign.TargetAgentID) {
		return &abcitypes.ExecTxResult{Code: 68, Log: fmt.Sprintf("target agent %s not registered", reassign.TargetAgentID[:16])}
	}

	// Source agent does NOT need to be registered — that's the whole point:
	// unregistered agents have orphaned memories that need to be merged.

	// Buffer offchain write — the actual SQLite UPDATE happens in flushPendingWrites
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "memory_reassign",
		data: &memoryReassignData{
			SourceAgentID: reassign.SourceAgentID,
			TargetAgentID: reassign.TargetAgentID,
		},
	})

	app.logger.Info().
		Str("source", reassign.SourceAgentID[:16]).
		Str("target", reassign.TargetAgentID[:16]).
		Str("admin", senderID[:16]).
		Msg("memory reassignment approved on-chain")

	return &abcitypes.ExecTxResult{
		Code: 0,
		Data: []byte(reassign.TargetAgentID),
		Log:  fmt.Sprintf("memories reassigned from %s to %s", reassign.SourceAgentID[:16], reassign.TargetAgentID[:16]),
	}
}

func (app *SageApp) processEpoch(height int64, blockTime time.Time, strict ...bool) error {
	strictConsensusErrors := len(strict) > 0 && strict[0]
	epochNum := poe.EpochNumber(height)
	app.state.EpochNum = epochNum
	app.logger.Info().Int64("epoch", epochNum).Int64("height", height).Msg("epoch boundary")
	metrics.EpochCurrent.Set(float64(epochNum))

	validators := app.validators.GetAll()

	// Historical epochs scan the vstats prefix exactly as before. App-v20 reads
	// only the bounded current validator roster, so stale statistics left by
	// years of validator churn cannot turn one epoch boundary into an unbounded
	// transaction/read workload.
	allStats := make(map[string]*store.ValidatorStats, len(validators))
	if strictConsensusErrors || app.postAppV20Fork(height) {
		for _, current := range validators {
			stats, err := app.badgerStore.GetValidatorStats(current.ID)
			if err != nil {
				app.logger.Error().Err(err).Str("validator", current.ID).Msg("failed to get app-v20 validator stats for epoch")
				return fmt.Errorf("read validator stats for %q: %w", current.ID, err)
			}
			allStats[current.ID] = stats
		}
	} else {
		var err error
		allStats, err = app.badgerStore.GetAllValidatorStats()
		if err != nil {
			app.logger.Error().Err(err).Msg("failed to get validator stats for epoch")
			if strictConsensusErrors {
				return fmt.Errorf("scan validator stats: %w", err)
			}
			return nil
		}
	}

	// v8.3: post-fork, accuracy is the verdict-correctness EWMA persisted in
	// vstats: (UpdateVerdictStats) and corroboration is the real per-validator
	// verdict-match count. Pre-fork keeps the Phase-1 accept-ratio blend +
	// hardcoded-0 corroboration so v8.2.x blocks replay byte-identical. The
	// strict-> gate means the activation block H_act itself is still pre-fork.
	postV83 := app.postV8_3Fork(height)

	// Compute raw PoE weights for each validator
	rawWeights := make(map[string]float64, len(validators))
	epochDetails := make(map[string]*store.EpochScore, len(validators))

	for _, v := range validators {
		stats := allStats[v.ID]

		// Accuracy (A), recency (T), corroboration (S).
		var accuracy, recencyScore, corrScore float64
		if postV83 {
			// Post-fork: verdict-correctness EWMA accuracy, recency from last
			// vote, and the real lifetime corroboration count — all from the
			// shared poeFactorsPostV83 helper so the per-epoch scalar weight and
			// v8.4's per-memory domain-conditional quorum weight compute these
			// three factors with byte-identical arithmetic. A validator absent
			// from vstats: reconstructs as EWMATracker{0,0,0} → 0.5 cold-start.
			accuracy, recencyScore, corrScore = poeFactorsPostV83(stats, height, blockTime)
		} else {
			// Phase-1 (pre-fork): accept-ratio accuracy blend, recency from last
			// vote, corroboration hardcoded 0 — byte-identical to v8.2.x.
			if stats != nil && stats.TotalVotes > 0 {
				realAccuracy := float64(stats.AcceptVotes) / float64(stats.TotalVotes)
				// Cold-start blending: blend with 0.5 prior, full weight at K_min=10
				blendFactor := float64(stats.TotalVotes) / 10.0
				if blendFactor > 1.0 {
					blendFactor = 1.0
				}
				accuracy = blendFactor*realAccuracy + (1.0-blendFactor)*0.5
			} else {
				accuracy = 0.5 // Cold-start prior
			}

			if stats != nil && stats.LastBlockHeight > 0 {
				// Approximate: each block ~3s, so blocks_ago * 3s = seconds since last active
				blocksSinceLast := height - int64(stats.LastBlockHeight) // #nosec G115 -- block height fits in int64
				if blocksSinceLast < 0 {
					blocksSinceLast = 0
				}
				hoursSinceLast := float64(blocksSinceLast) * 3.0 / 3600.0
				recencyScore = poe.RecencyScore(blockTime.Add(-time.Duration(hoursSinceLast*float64(time.Hour))), blockTime)
			} else {
				recencyScore = poe.EpsilonFloor // No activity
			}

			corrScore = poe.CorroborationScore(0, poe.CorrMax)
		}

		// Domain (D): the per-epoch scalar weight keeps the neutral 0.5 baseline
		// even post-v8.4. v8.4 realizes the domain factor PER-MEMORY in
		// checkAndApplyQuorum — a validator's vote on a domain-D memory is
		// weighted by its verdict-correctness in D. This scalar is only the
		// fallback weight for shared/unknown-domain memories, where subject-matter
		// expertise is meaningless by design, so 0.5 (neutral) is the right
		// baseline here. See docs/v8.4-PLAN.md.
		domainScore := 0.5

		// Compute PoE weight
		weight := poe.ComputeWeight(accuracy, domainScore, recencyScore, corrScore)
		rawWeights[v.ID] = weight

		epochDetails[v.ID] = &store.EpochScore{
			EpochNum:     epochNum,
			BlockHeight:  height,
			ValidatorID:  v.ID,
			Accuracy:     accuracy,
			DomainScore:  domainScore,
			RecencyScore: recencyScore,
			CorrScore:    corrScore,
			RawWeight:    weight,
		}

		app.logger.Info().
			Str("validator", v.ID).
			Float64("accuracy", accuracy).
			Float64("domain", domainScore).
			Float64("recency", recencyScore).
			Float64("corr", corrScore).
			Float64("raw_weight", weight).
			Int64("epoch", epochNum).
			Msg("epoch score computed")
	}

	// Normalize weights with rep cap. v8.4: post-fork, sum the weight map in
	// sorted-key order (NormalizeWeightsDeterministic) so the persisted poew:<id>
	// float64 bits — and therefore the AppHash at every epoch boundary — are
	// independent of Go's randomized map-iteration order. The legacy
	// (map-order) sum is non-associative and could split AppHash across honest
	// replicas with ≥3 distinct-magnitude weights; it is retained pre-fork ONLY
	// so v8.2/v8.3 blocks replay byte-identical. Gated like every other v8.x
	// consensus-rule change. See docs/v8.4-PLAN.md / the poe-drift audit.
	var normalized map[string]float64
	if app.postV8_4Fork(height) {
		normalized = poe.NormalizeWeightsDeterministic(rawWeights)
	} else {
		normalized = poe.NormalizeWeights(rawWeights)
	}

	// v8.2: persist the normalized weight set on-chain so a node restart
	// between epoch boundaries does not reset PoEWeight to zero (which
	// would diverge consensus). Pre-fork the call is suppressed so v8.1.x
	// chains and v8.2 pre-activation blocks replay byte-identical — the
	// new poew:* keys only enter the BadgerDB key space (and therefore
	// the AppHash) after the activation block. See docs/v8.2-PLAN.md
	// "Activation-block edge case" for why H_act itself stays pre-fork.
	if app.postV8_2Fork(height) {
		if err := app.badgerStore.SetEpochWeights(uint64(epochNum), normalized); err != nil { // #nosec G115 -- epochNum is non-negative
			app.logger.Error().Err(err).Int64("epoch", epochNum).Msg("persist epoch weights")
			if strictConsensusErrors {
				return fmt.Errorf("persist epoch %d weights: %w", epochNum, err)
			}
		}
	}

	// Update validator PoE weights and buffer PostgreSQL writes
	for _, v := range validators {
		normWeight := normalized[v.ID]
		v.PoEWeight = normWeight

		detail := epochDetails[v.ID]
		detail.CappedWeight = normWeight
		detail.NormalizedWeight = normWeight

		// Buffer epoch score write for Commit
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "epoch_score",
			data:      detail,
		})

		// Buffer validator score update for Commit. The off-chain mirror feeds
		// the REST /v1/agent Accuracy (vote_handler reconstructs an EWMATracker
		// from these fields). Post-fork, source them from the on-chain
		// verdict-correctness EWMA so REST matches the accuracy actually driving
		// quorum weight; pre-fork keep the accept-ratio source. This mirror is
		// read only by REST handlers, never in FinalizeBlock — not a consensus
		// input — but keeping it honest avoids the operator-facing divergence.
		stats := allStats[v.ID]
		var voteCount int64
		var weightedSum, weightDenom float64
		if stats != nil {
			if postV83 {
				weightedSum = stats.EWMAWeightedSum
				weightDenom = stats.EWMAWeightDenom
				voteCount = int64(stats.EWMACount) // #nosec G115 -- non-negative
			} else {
				voteCount = int64(stats.TotalVotes) // #nosec G115 -- vote count fits in int64
				weightedSum = float64(stats.AcceptVotes)
				weightDenom = float64(stats.TotalVotes)
			}
		}

		now := blockTime
		scoreUpdate := &store.ValidatorScore{
			ValidatorID:   v.ID,
			WeightedSum:   weightedSum,
			WeightDenom:   weightDenom,
			VoteCount:     voteCount,
			ExpertiseVec:  []float64{},
			LastActiveTS:  &now,
			CurrentWeight: normWeight,
			UpdatedAt:     blockTime,
		}
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "validator_score",
			data:      scoreUpdate,
		})
	}

	// Publish the per-validator PoE weight gauge from the normalized epoch
	// weights. Process-local (no BadgerDB write, no pendingWrite, no ordering
	// effect) so it stays outside the AppHash path; co-located with EpochCurrent
	// for the same once-per-epoch cadence. Reset-then-repopulate prunes stale
	// series for governance-removed validators.
	metrics.SetPoEWeights(normalized)
	return nil
}

// Commit persists finalized state.
// THIS is where PostgreSQL writes happen — never in FinalizeBlock.
//
// Ordering is load-bearing: the offchain flush runs BEFORE SaveState so that
// if the flush fails, BadgerDB's recorded height stays behind the block we
// just processed. On restart CometBFT reads the behind height via Info() and
// replays the block, giving FinalizeBlock another chance to populate
// pendingWrites and Commit another chance to flush. Persisting BadgerDB
// first would produce silent on-chain-vs-offchain divergence: ABCI would
// claim it's caught up, CometBFT would skip replay, and the SQLite row
// would be lost forever.
func (app *SageApp) Commit(_ context.Context, req *abcitypes.RequestCommit) (*abcitypes.ResponseCommit, error) {
	ctx := context.Background()
	working := app
	pendingAtomic := app.pendingAppV20Finalize
	if pendingAtomic != nil {
		working = pendingAtomic.app
	} else if app.appV20AppliedHeight > 0 && app.state != nil && app.state.Height >= app.appV20AppliedHeight {
		panic("sage: Commit called without a matching post-app-v20 FinalizeBlock")
	}
	atomicPublished := pendingAtomic == nil
	defer func() {
		if pendingAtomic != nil && !atomicPublished {
			pendingAtomic.store.DiscardConsensusTransaction()
			app.pendingAppV20Finalize = nil
		}
	}()

	// Flush pending writes to the offchain store atomically within a single
	// database transaction. If any write fails, the entire batch rolls back,
	// preventing partial state divergence between BadgerDB and the query layer.
	//
	// Retry on SQLITE_BUSY with exponential backoff capped at 5s per attempt.
	// The SQLite driver's busy_timeout (15s per statement) already absorbs
	// short contention windows; this budget handles sustained lock contention
	// across the whole batch. On exhaustion we panic rather than silently
	// drop: consensus has already committed these writes on-chain, so losing
	// them from the offchain store would produce divergence the read API
	// cannot detect.
	// flushedWrites survives the pendingWrites nil-out below so the sync
	// notifier at the Commit tail can filter what is durably present. On an
	// exact block replay the projection receipt may skip an already-committed
	// batch; retaining this list still lets the post-Badger notifier re-fire its
	// idempotent enqueue after the original process died before reaching it. A
	// flush failure panics before the tail, so this never reports missing writes.
	var flushedWrites []pendingWrite
	if len(working.pendingWrites) > 0 {
		writes := working.pendingWrites
		flushedWrites = writes
		maxRetries := working.flushMaxRetries
		if maxRetries <= 0 {
			maxRetries = defaultFlushMaxRetries
		}
		var lastErr error
		runProjectionTx := working.offchainStore.RunInTx
		if projectionWritesAgentContacts(writes) {
			runProjectionTx = working.offchainStore.RunInAgentContactTx
		}
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
				working.logger.Warn().Int("attempt", attempt+1).Dur("backoff", backoff).Int("count", len(writes)).
					Msg("retrying offchain flush after SQLITE_BUSY")
				time.Sleep(backoff)
			}
			lastErr = runProjectionTx(ctx, func(tx store.OffchainStore) error {
				for _, pw := range writes {
					if payloadErr := validatePendingWritePayload(pw); payloadErr != nil {
						return payloadErr
					}
				}
				apply, claimErr := tx.ClaimProjectionBatch(ctx, working.state.Height, working.state.AppHash)
				if claimErr != nil {
					return claimErr
				}
				if !apply {
					return working.mergeProjectionReplayEnrichment(ctx, tx, writes)
				}
				return working.flushPendingWrites(ctx, tx, writes)
			})
			if lastErr == nil {
				break
			}
			// Only retry on transient lock contention; other errors won't clear
			// by waiting and are better surfaced immediately via panic below.
			if !strings.Contains(lastErr.Error(), "SQLITE_BUSY") &&
				!strings.Contains(lastErr.Error(), "database is locked") {
				break
			}
		}
		if lastErr != nil {
			working.logger.Error().Err(lastErr).Int("count", len(writes)).Int("attempts", maxRetries).
				Msg("CRITICAL: atomic flush of pending writes failed — halting node to preserve on-chain/offchain consistency")
			panic(fmt.Sprintf(
				"sage: offchain flush failed after %d attempts (%d writes pending): %v — "+
					"consensus cannot advance without offchain commit; fix DB contention and restart to replay this block",
				maxRetries, len(writes), lastErr,
			))
		}
		if pendingAtomic != nil {
			runAppV20CommitBoundaryHook(AppV20CommitAfterOffchainFlush)
		}
	}
	working.pendingWrites = nil

	// Save state to BadgerDB only after the offchain flush has succeeded.
	if err := SaveState(working.badgerStore, working.state); err != nil {
		working.logger.Error().Err(err).Int64("height", working.state.Height).
			Msg("CRITICAL: durable consensus-state commit failed — halting before reporting Commit success")
		panic(fmt.Sprintf(
			"sage: durable consensus-state commit failed at height %d: %v — "+
				"the node cannot report ABCI Commit success; repair storage and restart so CometBFT can replay safely",
			working.state.Height, err,
		))
	}
	if pendingAtomic != nil {
		if err := pendingAtomic.store.CommitConsensusTransaction(); err != nil {
			working.logger.Error().Err(err).Int64("height", working.state.Height).
				Msg("CRITICAL: app-v20 FinalizeBlock transaction commit failed — halting")
			panic(fmt.Sprintf(
				"sage: app-v20 atomic FinalizeBlock commit failed at height %d: %v — repair storage and restart",
				working.state.Height, err,
			))
		}
		runAppV20CommitBoundaryHook(AppV20CommitAfterBadgerSync)
		working.SuppCache.commitConsumed()
		app.publishAppV20Finalize(working)
		app.pendingWrites = nil
		app.pendingAppV20Finalize = nil
		atomicPublished = true
	}

	// v7.5 snapshot scheduler: post-SaveState is the only point where
	// BadgerDB + SQLite + CometBFT-state-as-of-this-height are mutually
	// consistent. Tick is fast (mutex + decision); any actual disk work
	// runs on its own goroutine so Commit returns promptly.
	if app.snapshotScheduler != nil {
		app.snapshotScheduler.Tick(app.state.Height, app.state.AppHash)
	}

	// v11.5 domain-sync watcher: report this block's committed memory IDs,
	// post-flush and post-SaveState. status_update -> committed is the single
	// choke point for every commit transition (quorum path + co-commit
	// mirror), so filtering the just-flushed writes catches them all. A block
	// replay after a flush panic legitimately re-fires this — the watcher's
	// enqueue is INSERT OR IGNORE idempotent by design.
	if notify := app.syncNotifier.Load(); notify != nil {
		var committed []string
		for _, pw := range flushedWrites {
			if pw.writeType != "status_update" {
				continue
			}
			if su, ok := pw.data.(*statusUpdate); ok && su.Status == memory.StatusCommitted {
				committed = append(committed, su.MemoryID)
			}
		}
		if len(committed) > 0 {
			fn := *notify
			go fn(committed)
		}
	}

	// Ask CometBFT to prune blocks older than the retain window (issue #40).
	// RetainHeight is advisory and LOCAL — it never enters the AppHash, so a
	// node may prune (or not) independently of its peers. 0 means keep
	// everything, which is also the pre-config default.
	var retainHeight int64
	if app.retainBlocks > 0 && app.state.Height > app.retainBlocks {
		retainHeight = app.state.Height - app.retainBlocks
	}

	return &abcitypes.ResponseCommit{RetainHeight: retainHeight}, nil
}

// projectionWritesAgentContacts reports whether one atomic off-chain mirror
// batch can change the peer-visible agent identity/availability projection.
// Keep this deliberately narrow: unrelated consensus projection traffic must
// not wait behind a slow federated contact reader.
func projectionWritesAgentContacts(writes []pendingWrite) bool {
	for _, pw := range writes {
		switch pw.writeType {
		case "agent_register", "agent_update", "agent_permission":
			return true
		}
	}
	return false
}

// SetRetainBlocks configures the block-retention window Commit reports to
// CometBFT via ResponseCommit.RetainHeight: blocks older than the most recent
// n are eligible for pruning from the blockstore. n <= 0 disables pruning
// (retain everything). Local node policy only — never consensus state. Call
// before the node starts; it is not synchronized against a running Commit.
func (app *SageApp) SetRetainBlocks(n int64) {
	if n < 0 {
		n = 0
	}
	app.retainBlocks = n
}

// SetSyncNotifier installs the domain-sync commit hook (see the field doc).
// nil disables the nudge (the drainer's poll scan still syncs). Safe to call
// concurrently with Commit: the store is an atomic.Pointer, so the install is
// race-free even when the consensus loop is already running.
func (app *SageApp) SetSyncNotifier(fn func([]string)) {
	if fn == nil {
		app.syncNotifier.Store(nil)
		return
	}
	app.syncNotifier.Store(&fn)
}

// flushPendingWrites executes all buffered writes against the given store (which
// may be a transaction-scoped store). Returns the first error encountered,
// causing the wrapping transaction to roll back.
func validatePendingWritePayload(pw pendingWrite) error { //nolint:cyclop // exhaustive protocol-to-store type matrix
	valid := false
	switch pw.writeType {
	case "memory":
		v, ok := pw.data.(*memory.MemoryRecord)
		valid = ok && v != nil
	case "memory_tags":
		v, ok := pw.data.(*memoryTagsData)
		valid = ok && v != nil
	case "triples":
		v, ok := pw.data.(*triplesData)
		valid = ok && v != nil
	case "challenge":
		v, ok := pw.data.(*store.ChallengeEntry)
		valid = ok && v != nil
	case "vote":
		v, ok := pw.data.(*store.ValidationVote)
		valid = ok && v != nil
	case "corroborate":
		v, ok := pw.data.(*store.Corroboration)
		valid = ok && v != nil
	case "epoch_score":
		v, ok := pw.data.(*store.EpochScore)
		valid = ok && v != nil
	case "validator_score":
		v, ok := pw.data.(*store.ValidatorScore)
		valid = ok && v != nil
	case "status_update":
		v, ok := pw.data.(*statusUpdate)
		valid = ok && v != nil
	case "access_grant":
		v, ok := pw.data.(*store.AccessGrantEntry)
		valid = ok && v != nil
	case "access_request":
		v, ok := pw.data.(*store.AccessRequestEntry)
		valid = ok && v != nil
	case "access_revoke":
		v, ok := pw.data.(*accessRevokeData)
		valid = ok && v != nil
	case "access_log":
		v, ok := pw.data.(*store.AccessLogEntry)
		valid = ok && v != nil
	case "domain_register":
		v, ok := pw.data.(*store.DomainEntry)
		valid = ok && v != nil
	case "access_request_status":
		v, ok := pw.data.(*accessRequestStatusUpdate)
		valid = ok && v != nil
	case "org_register":
		v, ok := pw.data.(*store.OrgEntry)
		valid = ok && v != nil
	case "org_member":
		v, ok := pw.data.(*store.OrgMemberEntry)
		valid = ok && v != nil
	case "org_member_remove":
		v, ok := pw.data.(*orgMemberRemoveData)
		valid = ok && v != nil
	case "org_member_clearance":
		v, ok := pw.data.(*orgClearanceData)
		valid = ok && v != nil
	case "federation":
		v, ok := pw.data.(*store.FederationEntry)
		valid = ok && v != nil
	case "federation_approve":
		v, ok := pw.data.(*federationApproveData)
		valid = ok && v != nil
	case "federation_revoke":
		v, ok := pw.data.(*federationRevokeData)
		valid = ok && v != nil
	case "mem_classification":
		v, ok := pw.data.(*memClassificationData)
		valid = ok && v != nil
	case "dept_register":
		v, ok := pw.data.(*store.DeptEntry)
		valid = ok && v != nil
	case "dept_member":
		v, ok := pw.data.(*store.DeptMemberEntry)
		valid = ok && v != nil
	case "dept_member_remove":
		v, ok := pw.data.(*deptMemberRemoveData)
		valid = ok && v != nil
	case "agent_register":
		v, ok := pw.data.(*store.AgentEntry)
		valid = ok && v != nil
	case "agent_update":
		v, ok := pw.data.(*agentUpdateData)
		valid = ok && v != nil
	case "agent_permission":
		v, ok := pw.data.(*agentPermissionData)
		valid = ok && v != nil
	case "memory_reassign":
		v, ok := pw.data.(*memoryReassignData)
		valid = ok && v != nil
	case "gov_proposal":
		_, valid = pw.data.(govProposalData)
	case "gov_vote":
		_, valid = pw.data.(govVoteData)
	case "gov_status_update":
		_, valid = pw.data.(govStatusUpdateData)
	default:
		return fmt.Errorf("unknown pending-write type %q", pw.writeType)
	}
	if !valid {
		return fmt.Errorf("pending-write type %q has invalid payload %T", pw.writeType, pw.data)
	}
	return nil
}

// mergeProjectionReplayEnrichment is the only write path reachable after an
// exact block receipt already exists. A cluster's validators share PostgreSQL,
// but only the REST receiver has process-local SupplementaryCache data. The
// receipt winner may therefore have written a bare memory. Let a later receiver
// fill immutable/off-consensus memory enrichment and exact-unique triples while
// keeping lifecycle status, timestamps, access logs, governance, and every
// other historical sink structurally unreachable from this branch.
func projectionReplayEnrichmentAllowed(writeType string) bool {
	return writeType == "memory" || writeType == "triples"
}

func (app *SageApp) mergeProjectionReplayEnrichment(ctx context.Context, s store.OffchainStore, writes []pendingWrite) error {
	postgres, ok := s.(*store.PostgresStore)
	if !ok {
		return nil
	}
	for _, pw := range writes {
		if !projectionReplayEnrichmentAllowed(pw.writeType) {
			continue
		}
		var err error
		switch pw.writeType {
		case "memory":
			err = postgres.MergeProjectionSupplementary(ctx, pw.data.(*memory.MemoryRecord))
		case "triples":
			d := pw.data.(*triplesData)
			err = postgres.InsertTriples(ctx, d.MemoryID, d.Triples)
		default:
			return fmt.Errorf("replay enrichment allowlist has no handler for %q", pw.writeType)
		}
		if err != nil {
			return fmt.Errorf("merge replay enrichment %s: %w", pw.writeType, err)
		}
	}
	return nil
}

func (app *SageApp) flushPendingWrites(ctx context.Context, s store.OffchainStore, writes []pendingWrite) error {
	for _, pw := range writes {
		if err := validatePendingWritePayload(pw); err != nil {
			return err
		}
		var err error
		switch pw.writeType {
		case "memory":
			if record, ok := pw.data.(*memory.MemoryRecord); ok {
				err = s.InsertMemory(ctx, record)
			}
		case "memory_tags":
			if tags, ok := pw.data.(*memoryTagsData); ok {
				err = s.SetTags(ctx, tags.MemoryID, tags.Tags)
			}
		case "triples":
			if td, ok := pw.data.(*triplesData); ok {
				err = s.InsertTriples(ctx, td.MemoryID, td.Triples)
			}
		case "challenge":
			if ch, ok := pw.data.(*store.ChallengeEntry); ok {
				err = s.InsertChallenge(ctx, ch)
			}
		case "vote":
			if vote, ok := pw.data.(*store.ValidationVote); ok {
				err = s.InsertVote(ctx, vote)
			}
		case "corroborate":
			if corr, ok := pw.data.(*store.Corroboration); ok {
				err = s.InsertCorroboration(ctx, corr)
			}
		case "epoch_score":
			if epoch, ok := pw.data.(*store.EpochScore); ok {
				err = s.InsertEpochScore(ctx, epoch)
			}
		case "validator_score":
			if score, ok := pw.data.(*store.ValidatorScore); ok {
				err = s.UpdateScore(ctx, score)
			}
		case "status_update":
			if su, ok := pw.data.(*statusUpdate); ok {
				var writeErr error
				if su.Status == memory.StatusChallenged {
					// app-v17 park: reflect the disputed status AND the C-D3
					// challenge-execution height + measured quorum in the SQLite
					// mirror, so ResolveChallengedMemories can leave the live dispute
					// alone and the dashboard can show the quorum. disputed_height/
					// disputed_quorum are SQLite-only columns; a non-SQLite mirror
					// (Postgres) falls back to a plain status update so the row still
					// reads 'challenged'.
					if sq, isSQLite := s.(*store.SQLiteStore); isSQLite {
						writeErr = sq.MarkDisputed(ctx, su.MemoryID, su.DisputedHeight, int(su.DisputedQuorum), su.At)
					} else {
						writeErr = s.UpdateStatus(ctx, su.MemoryID, su.Status, su.At)
					}
				} else {
					writeErr = s.UpdateStatus(ctx, su.MemoryID, su.Status, su.At)
				}
				if writeErr != nil {
					err = writeErr
				} else {
					app.logger.Info().
						Str("memory_id", su.MemoryID).
						Str("status", string(su.Status)).
						Msg("memory status updated")
				}
			}
		case "access_grant":
			if grant, ok := pw.data.(*store.AccessGrantEntry); ok {
				err = s.InsertAccessGrant(ctx, grant)
			}
		case "access_request":
			if req, ok := pw.data.(*store.AccessRequestEntry); ok {
				err = s.InsertAccessRequest(ctx, req)
			}
		case "access_revoke":
			if revoke, ok := pw.data.(*accessRevokeData); ok {
				err = s.RevokeGrant(ctx, revoke.Domain, revoke.GranteeID, revoke.Height)
			}
		case "access_log":
			if logEntry, ok := pw.data.(*store.AccessLogEntry); ok {
				err = s.InsertAccessLog(ctx, logEntry)
			}
		case "domain_register":
			if domain, ok := pw.data.(*store.DomainEntry); ok {
				err = s.InsertDomain(ctx, domain)
			}
		case "access_request_status":
			if ars, ok := pw.data.(*accessRequestStatusUpdate); ok {
				err = s.UpdateAccessRequestStatus(ctx, ars.RequestID, ars.Status, ars.Height)
			}
		case "org_register":
			if org, ok := pw.data.(*store.OrgEntry); ok {
				err = s.InsertOrg(ctx, org)
			}
		case "org_member":
			if member, ok := pw.data.(*store.OrgMemberEntry); ok {
				err = s.InsertOrgMember(ctx, member)
			}
		case "org_member_remove":
			if d, ok := pw.data.(*orgMemberRemoveData); ok {
				err = s.RemoveOrgMember(ctx, d.OrgID, d.AgentID, d.Height)
			}
		case "org_member_clearance":
			if d, ok := pw.data.(*orgClearanceData); ok {
				err = s.UpdateMemberClearance(ctx, d.OrgID, d.AgentID, d.Clearance)
			}
		case "federation":
			if fed, ok := pw.data.(*store.FederationEntry); ok {
				err = s.InsertFederation(ctx, fed)
			}
		case "federation_approve":
			if d, ok := pw.data.(*federationApproveData); ok {
				err = s.ApproveFederation(ctx, d.FederationID, d.Height)
			}
		case "federation_revoke":
			if d, ok := pw.data.(*federationRevokeData); ok {
				err = s.RevokeFederation(ctx, d.FederationID, d.Height)
			}
		case "mem_classification":
			if d, ok := pw.data.(*memClassificationData); ok {
				err = s.UpdateMemoryClassification(ctx, d.MemoryID, d.Classification)
			}
		case "dept_register":
			if dept, ok := pw.data.(*store.DeptEntry); ok {
				err = s.InsertDept(ctx, dept)
			}
		case "dept_member":
			if member, ok := pw.data.(*store.DeptMemberEntry); ok {
				err = s.InsertDeptMember(ctx, member)
			}
		case "dept_member_remove":
			if d, ok := pw.data.(*deptMemberRemoveData); ok {
				err = s.RemoveDeptMember(ctx, d.OrgID, d.DeptID, d.AgentID, d.Height)
			}
		case "agent_register":
			if agent, ok := pw.data.(*store.AgentEntry); ok {
				// Try to create; only a verified existing row may take the
				// idempotent update path. Treating an arbitrary insert failure as
				// "already exists" can commit a projection receipt after writing
				// nothing (and hides the original storage error).
				createErr := s.CreateAgent(ctx, agent)
				if createErr != nil {
					existing, lookupErr := s.GetAgent(ctx, agent.AgentID)
					switch {
					case lookupErr != nil:
						err = fmt.Errorf("create agent failed and existence lookup failed: %w", errors.Join(createErr, lookupErr))
					case existing == nil:
						err = createErr
					default:
						err = s.UpdateAgent(ctx, agent)
					}
				}
			}
		case "agent_update":
			if d, ok := pw.data.(*agentUpdateData); ok {
				existing, getErr := s.GetAgent(ctx, d.AgentID)
				switch {
				case getErr != nil:
					err = fmt.Errorf("get agent %s for update: %w", d.AgentID, getErr)
				case existing == nil:
					err = fmt.Errorf("agent %s is missing from offchain store during update", d.AgentID)
				default:
					existing.Name = d.Name
					existing.BootBio = d.BootBio
					err = s.UpdateAgent(ctx, existing)
				}
			}
		case "agent_permission":
			if d, ok := pw.data.(*agentPermissionData); ok {
				existing, getErr := s.GetAgent(ctx, d.AgentID)
				if getErr == nil && existing != nil {
					existing.Clearance = d.Clearance
					existing.DomainAccess = d.DomainAccess
					existing.VisibleAgents = d.VisibleAgents
					if d.OrgID != "" {
						existing.OrgID = d.OrgID
					}
					if d.DeptID != "" {
						existing.DeptID = d.DeptID
					}
					err = s.UpdateAgent(ctx, existing)
				} else {
					// Agent exists on-chain but not in SQLite — create it so permissions persist
					now := time.Now()
					agent := &store.AgentEntry{
						AgentID:       d.AgentID,
						Clearance:     d.Clearance,
						DomainAccess:  d.DomainAccess,
						VisibleAgents: d.VisibleAgents,
						OrgID:         d.OrgID,
						DeptID:        d.DeptID,
						Status:        "active",
						CreatedAt:     now,
						FirstSeen:     &now,
					}
					if createErr := s.CreateAgent(ctx, agent); createErr != nil {
						if getErr != nil {
							createErr = errors.Join(getErr, createErr)
						}
						err = fmt.Errorf("agent %s not in offchain store and create failed: %w", d.AgentID[:16], createErr)
					} else {
						app.logger.Info().Str("agent_id", d.AgentID[:16]).Msg("created offchain agent record for permission sync")
					}
				}
			}
		case "memory_reassign":
			if d, ok := pw.data.(*memoryReassignData); ok {
				count, reassignErr := s.ReassignMemories(ctx, d.SourceAgentID, d.TargetAgentID)
				if reassignErr != nil {
					err = reassignErr
				} else {
					app.logger.Info().
						Str("source", d.SourceAgentID[:16]).
						Str("target", d.TargetAgentID[:16]).
						Int64("count", count).
						Msg("memories reassigned in offchain store")
				}
			}
		case "gov_proposal":
			if d, ok := pw.data.(govProposalData); ok {
				err = s.InsertGovProposal(ctx, &store.GovProposal{
					ProposalID:    d.ProposalID,
					Operation:     d.Operation,
					TargetAgentID: d.TargetID,
					TargetPower:   d.TargetPower,
					ProposerID:    d.ProposerID,
					Status:        d.Status,
					CreatedHeight: d.CreatedHeight,
					ExpiryHeight:  d.ExpiryHeight,
					Reason:        d.Reason,
				})
			}
		case "gov_vote":
			if d, ok := pw.data.(govVoteData); ok {
				err = s.InsertGovVote(ctx, &store.GovVote{
					ProposalID:  d.ProposalID,
					ValidatorID: d.ValidatorID,
					Decision:    d.Decision,
					Height:      d.Height,
				})
			}
		case "gov_status_update":
			if d, ok := pw.data.(govStatusUpdateData); ok {
				var execHeight *int64
				if d.ExecutedHeight > 0 {
					execHeight = &d.ExecutedHeight
				}
				err = s.UpdateGovProposalStatus(ctx, d.ProposalID, d.Status, execHeight)
			}
		}
		if err != nil {
			return fmt.Errorf("flush %s: %w", pw.writeType, err)
		}
	}
	return nil
}

// isAuthenticatedAppV20BootstrapProposal recognizes the one pre-activation
// transaction that is authorized to switch the chain onto the v20 ceremony.
// It is deliberately stricter than a syntactic target-20 check: a forged raw
// transaction must not censor otherwise-valid legacy traffic. Every input is
// deterministic consensus state or the proposed block tuple; no wall clock or
// process-local expected-domain setting is consulted.
func (app *SageApp) isAuthenticatedAppV20BootstrapProposal(rawTx []byte, height int64, blockTime time.Time) bool {
	parsed, err := tx.DecodeTx(rawTx)
	if err != nil || parsed.Type != tx.TxTypeUpgradePropose || parsed.UpgradePropose == nil ||
		!isAppV20CeremonyUpgrade(parsed.UpgradePropose.Name, parsed.UpgradePropose.TargetAppVersion, parsed.UpgradePropose.GovernanceDomain) {
		return false
	}
	if app.currentAppVersion() != 19 || parsed.UpgradePropose.Name != appV20UpgradeName {
		return false
	}
	canonical, err := tx.EncodeTx(parsed)
	if err != nil || !bytes.Equal(canonical, rawTx) {
		return false
	}
	if !isCanonicalAppV20CeremonyDomain(parsed.UpgradePropose.GovernanceDomain) {
		return false
	}
	if resourceErr := validateAppV20TxResources(parsed); resourceErr != nil {
		return false
	}
	valid, err := tx.VerifyTx(parsed)
	if err != nil || !valid || parsed.Nonce == 0 {
		return false
	}
	proposerID := auth.PublicKeyToAgentID(parsed.PublicKey)
	currentNonce, err := app.badgerStore.GetNonce(proposerID)
	if err != nil || (currentNonce > 0 && parsed.Nonce <= currentNonce) {
		return false
	}
	if _, identityErr := verifyAgentIdentity(parsed); identityErr != nil {
		return false
	}
	if app.postAppV17Rules(height) {
		if proofErr := app.enforceDelegatedAgentProof(parsed, blockTime, false, false); proofErr != nil {
			return false
		}
	}
	proposer, err := app.badgerStore.GetRegisteredAgent(proposerID)
	if err != nil || proposer == nil || proposer.Role != "admin" {
		return false
	}
	if err := app.govEngine.CheckProposerEligibility(proposerID, height); err != nil {
		return false
	}
	if plan, err := app.badgerStore.GetUpgradePlan(); err == nil && plan != nil {
		return false
	} else if err != nil && !errors.Is(err, store.ErrNoUpgradePlan) {
		return false
	}
	return app.appV20AuthenticatedProposalReadiness() == nil
}

// PrepareProposal is historical pass-through before app-v20 except for an
// authenticated target-20 bootstrap, which must be isolated into its own block
// so ordinary mempool traffic cannot make the ceremony transaction self-reject.
// After ceremony state exists, it applies the global block budget required by
// the single-transaction finalize path; governance and ordinary transactions
// may safely coexist there.
func (app *SageApp) PrepareProposal(_ context.Context, req *abcitypes.RequestPrepareProposal) (*abcitypes.ResponsePrepareProposal, error) {
	if app.requiresAppV20StrictRulesAt(req.Height) {
		// Seat bounded stale entries even when their static fields violate a
		// v20 limit. FinalizeBlock returns deterministic Code 111 before any
		// mutation, and Comet removes every transaction present in a committed
		// block regardless of its result code. Filtering here would leave an
		// entry admitted before the ceremony in the mempool forever when an
		// external Comet operator disabled recheck.
		return &abcitypes.ResponsePrepareProposal{
			Txs: prepareAppV20Proposal(req.Txs, req.MaxTxBytes),
		}, nil
	}
	recognizedButUnfit := make(map[int]struct{})
	for i, rawTx := range req.Txs {
		if app.isAuthenticatedAppV20BootstrapProposal(rawTx, req.Height, req.Time) {
			selected := prepareAppV20Proposal([][]byte{rawTx}, req.MaxTxBytes)
			if len(selected) == 1 {
				return &abcitypes.ResponsePrepareProposal{Txs: selected}, nil
			}
			recognizedButUnfit[i] = struct{}{}
		}
	}
	if len(recognizedButUnfit) > 0 {
		// A recognized ceremony transaction that cannot fit MaxTxBytes must not
		// produce empty proposals forever. Exclude it (a mixed proposal would be
		// rejected by peers) and byte-pack ordinary traffic that does fit.
		return &abcitypes.ResponsePrepareProposal{
			Txs: prepareProposalExcludingIndexes(req.Txs, recognizedButUnfit, req.MaxTxBytes),
		}, nil
	}
	return &abcitypes.ResponsePrepareProposal{Txs: req.Txs}, nil
}

// ProcessProposal enforces the dedicated authenticated-bootstrap block on every
// validator, then mirrors the app-v20 global atomic budget so a Byzantine
// proposer cannot defer an oversized-block failure to FinalizeBlock. Invalid or
// forged raw target-20 transactions retain legacy mixed-block behavior.
func (app *SageApp) ProcessProposal(_ context.Context, req *abcitypes.RequestProcessProposal) (*abcitypes.ResponseProcessProposal, error) {
	strictV20Rules := app.requiresAppV20StrictRulesAt(req.Height)
	if !strictV20Rules && len(req.Txs) > 1 {
		for _, rawTx := range req.Txs {
			if app.isAuthenticatedAppV20BootstrapProposal(rawTx, req.Height, req.Time) {
				return &abcitypes.ResponseProcessProposal{Status: abcitypes.ResponseProcessProposal_REJECT}, nil
			}
		}
	}
	if strictV20Rules {
		if !validateAppV20Proposal(req.Txs) {
			return &abcitypes.ResponseProcessProposal{Status: abcitypes.ResponseProcessProposal_REJECT}, nil
		}
	}
	return &abcitypes.ResponseProcessProposal{Status: abcitypes.ResponseProcessProposal_ACCEPT}, nil
}

// Query handles ABCI queries.
func (app *SageApp) Query(_ context.Context, req *abcitypes.RequestQuery) (*abcitypes.ResponseQuery, error) {
	switch req.Path {
	case "/status":
		return &abcitypes.ResponseQuery{
			Code:  0,
			Value: []byte(fmt.Sprintf(`{"height":%d,"epoch":%d}`, app.state.Height, app.state.EpochNum)),
		}, nil
	default:
		return &abcitypes.ResponseQuery{Code: 1, Log: "unknown query path"}, nil
	}
}

// ListSnapshots deliberately advertises no network snapshots. The existing
// internal/snapshot bundle contains private local material and is not an ABCI
// payload; state sync stays disabled until a consensus-only format exists.
func (app *SageApp) ListSnapshots(_ context.Context, req *abcitypes.RequestListSnapshots) (*abcitypes.ResponseListSnapshots, error) {
	return &abcitypes.ResponseListSnapshots{}, nil
}

// OfferSnapshot rejects while network-safe state sync is unimplemented.
func (app *SageApp) OfferSnapshot(_ context.Context, req *abcitypes.RequestOfferSnapshot) (*abcitypes.ResponseOfferSnapshot, error) {
	return &abcitypes.ResponseOfferSnapshot{Result: abcitypes.ResponseOfferSnapshot_REJECT}, nil
}

// LoadSnapshotChunk returns no local rollback-bundle bytes over the network.
func (app *SageApp) LoadSnapshotChunk(_ context.Context, req *abcitypes.RequestLoadSnapshotChunk) (*abcitypes.ResponseLoadSnapshotChunk, error) {
	return &abcitypes.ResponseLoadSnapshotChunk{}, nil
}

// ApplySnapshotChunk aborts while network-safe state sync is unimplemented.
func (app *SageApp) ApplySnapshotChunk(_ context.Context, req *abcitypes.RequestApplySnapshotChunk) (*abcitypes.ResponseApplySnapshotChunk, error) {
	return &abcitypes.ResponseApplySnapshotChunk{Result: abcitypes.ResponseApplySnapshotChunk_ABORT}, nil
}

// ExtendVote is not used in Phase 1.
func (app *SageApp) ExtendVote(_ context.Context, req *abcitypes.RequestExtendVote) (*abcitypes.ResponseExtendVote, error) {
	return &abcitypes.ResponseExtendVote{}, nil
}

// VerifyVoteExtension is not used in Phase 1.
func (app *SageApp) VerifyVoteExtension(_ context.Context, req *abcitypes.RequestVerifyVoteExtension) (*abcitypes.ResponseVerifyVoteExtension, error) {
	return &abcitypes.ResponseVerifyVoteExtension{Status: abcitypes.ResponseVerifyVoteExtension_ACCEPT}, nil
}

// Close cleans up resources.
func (app *SageApp) Close() error {
	if err := app.CloseConsensusState(); err != nil {
		return err
	}
	return app.offchainStore.Close()
}

// CloseConsensusState closes only bundle-owned consensus storage. Boot state
// sync uses this while replacing a complete SageApp graph; the off-chain
// projection is process-owned and must remain open for the replacement bundle.
func (app *SageApp) CloseConsensusState() error {
	if app.pendingAppV20Finalize != nil {
		app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
		app.pendingAppV20Finalize = nil
	}
	return app.badgerStore.CloseBadger()
}

// GetOffchainStore returns the off-chain store for REST handlers.
func (app *SageApp) GetOffchainStore() store.OffchainStore {
	return app.offchainStore
}

// GetPostgresStore returns the postgres store for backward compatibility.
// Returns nil if the off-chain store is not a PostgresStore.
func (app *SageApp) GetPostgresStore() *store.PostgresStore {
	ps, _ := app.offchainStore.(*store.PostgresStore)
	return ps
}

// GetBadgerStore returns the badger store for REST handlers.
func (app *SageApp) GetBadgerStore() *store.BadgerStore {
	return app.badgerStore
}

// GetGovEngine returns the governance engine for REST handlers.
func (app *SageApp) GetGovEngine() *governance.Engine {
	return app.govEngine
}

// ---------------------------------------------------------------------------
// Governance transaction handlers
// ---------------------------------------------------------------------------

func isValidatorSetGovernanceOperation(op governance.ProposalOp) bool {
	switch op {
	case governance.OpAddValidator, governance.OpRemoveValidator, governance.OpUpdatePower:
		return true
	default:
		return false
	}
}

// validateAppV20ValidatorOperation binds the SAGE validator identity used by
// governance to the exact Ed25519 key CometBFT will update. Before app-v20 the
// two values were independently accepted, so a proposal could mutate validator
// A in the app while returning a ValidatorUpdate for key B. Keep this helper
// fork-scoped at every call site to preserve historical replay.
func (app *SageApp) validateAppV20ValidatorOperation(
	op governance.ProposalOp,
	targetID string,
	targetPubKey []byte,
	targetPower int64,
) error {
	if !isValidatorSetGovernanceOperation(op) {
		return fmt.Errorf("operation %d is not a validator-set operation", op)
	}

	idPubKey, err := auth.AgentIDToPublicKey(targetID)
	if err != nil {
		return fmt.Errorf("target validator ID must be a 32-byte lowercase-hex Ed25519 public key: %w", err)
	}
	canonicalID := auth.PublicKeyToAgentID(idPubKey)
	if targetID != canonicalID {
		return fmt.Errorf("target validator ID must use canonical lowercase hex")
	}

	if op == governance.OpAddValidator && len(targetPubKey) == 0 {
		return fmt.Errorf("add_validator requires target_pubkey")
	}
	if len(targetPubKey) != 0 {
		if len(targetPubKey) != ed25519.PublicKeySize {
			return fmt.Errorf("target_pubkey has length %d, want %d", len(targetPubKey), ed25519.PublicKeySize)
		}
		if !bytes.Equal(targetPubKey, idPubKey) {
			return fmt.Errorf("target_pubkey does not match target validator ID")
		}
	}
	if op != governance.OpAddValidator {
		if existing, exists := app.validators.GetValidator(targetID); exists && len(existing.PublicKey) != 0 {
			if len(existing.PublicKey) != ed25519.PublicKeySize || !bytes.Equal(existing.PublicKey, idPubKey) {
				return fmt.Errorf("existing validator public key does not match its canonical validator ID")
			}
		}
	}

	if err := app.govEngine.ValidateValidatorOperationV20(op, targetID, targetPower); err != nil {
		return err
	}
	if op == governance.OpAddValidator {
		if _, exists := app.validators.GetValidator(targetID); !exists && len(app.validators.GetAll()) >= maxAppV20Validators {
			return fmt.Errorf("validator set already has the app-v20 maximum of %d members", maxAppV20Validators)
		}
	}
	if op == governance.OpRemoveValidator {
		if err := app.requireScopeValidatorRemovalReady(targetID); err != nil {
			return fmt.Errorf("validator removal blocked: %w", err)
		}
	}
	return nil
}

// processGovPropose handles a TxTypeGovPropose transaction.
func (app *SageApp) processGovPropose(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if parsedTx.GovPropose == nil {
		return &abcitypes.ExecTxResult{Code: 70, Log: "missing governance propose payload"}
	}

	proposerID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
	gp := parsedTx.GovPropose
	op := governance.ProposalOp(gp.Operation)
	if isValidatorSetGovernanceOperation(op) && app.appV20PendingPlanFreezesValidatorReconfiguration() {
		return &abcitypes.ExecTxResult{Code: 72, Log: "governance propose: validator reconfiguration is frozen while the app-v20 activation plan is pending"}
	}

	// Historically the outer governance actor also had to be the registered
	// admin. App-v20 permits an exact action-bound operator/admin request signer
	// to authorize a distinct active validator key, while proposal identity,
	// voting power, cooldown, and deterministic ID remain attached to the outer
	// signer. Direct/no-proof proposals retain the historical outer-admin rule.
	delegated := app.postAppV20Fork(height) && isDelegatedGovernanceProof(parsedTx)
	if delegated {
		outerValidator, active := app.validators.GetValidator(proposerID)
		if !active || outerValidator.Power <= 0 {
			return &abcitypes.ExecTxResult{Code: 72, Log: "delegated governance outer actor is not an active on-chain validator"}
		}
		authorizerID, proofErr := verifyAgentIdentity(parsedTx)
		if proofErr != nil {
			return &abcitypes.ExecTxResult{Code: 72, Log: "delegated governance authorizer is invalid: " + proofErr.Error()}
		}
		authorizer, authorizerErr := app.badgerStore.GetRegisteredAgent(authorizerID)
		if authorizerErr != nil || authorizer == nil || authorizer.Role != "admin" {
			return &abcitypes.ExecTxResult{Code: 72, Log: "only admin agents can authorize governance proposals"}
		}
	} else {
		agent, err := app.badgerStore.GetRegisteredAgent(proposerID)
		if err != nil {
			return &abcitypes.ExecTxResult{Code: 71, Log: "proposer not registered: " + err.Error()}
		}
		if agent == nil || agent.Role != "admin" {
			return &abcitypes.ExecTxResult{Code: 72, Log: "only admin agents can propose governance changes"}
		}
	}
	// Keep the two branches separate: an outer validator that also happens to be
	// admin must never bypass validation of its distinct embedded operator.

	// app-v8: OpUpgrade proposals must NOT be creatable on the generic gov path —
	// they would bypass processUpgradePropose's canonical-name + regression +
	// at-most-one-pending guards (a non-canonical name bumps version.app yet
	// flips no fork gate: the v8.4.x halt class). Force them through
	// processUpgradePropose, which is the only sanctioned creation site. Gated
	// behind postAppV8Fork so pre-fork chains (where op==5 was an undefined no-op
	// that errored at apply) replay byte-identically.
	if app.postAppV8Rules(height) && op == governance.OpUpgrade {
		return &abcitypes.ExecTxResult{Code: 72, Log: "governance propose: OpUpgrade must be created via UpgradePropose, not GovPropose"}
	}

	// app-v16: OpMemoryDomainRepair proposals must carry a valid, non-empty backfill
	// payload — the {memory_id, domain} list is the whole point of the op. Reject a
	// malformed/empty one at propose time so a supermajority-passed proposal never
	// fails at apply. Fork-gated (postAppV16Rules) so pre-fork chains — where op==6
	// was an undefined no-op that created an inert proposal — replay byte-identically.
	if app.postAppV16Rules(height) && op == governance.OpMemoryDomainRepair {
		var entries []tx.MemoryDomainRepairEntry
		if uErr := json.Unmarshal(gp.Payload, &entries); uErr != nil || len(entries) == 0 {
			return &abcitypes.ExecTxResult{Code: 72, Log: "governance propose: OpMemoryDomainRepair requires a non-empty JSON [{memory_id,domain}] payload"}
		}
		for _, e := range entries {
			if e.MemoryID == "" || e.Domain == "" {
				return &abcitypes.ExecTxResult{Code: 72, Log: "governance propose: OpMemoryDomainRepair entries require non-empty memory_id and domain"}
			}
		}
	}

	// app-v20: a scope action is the governance decision itself. Its payload is
	// one canonical, versioned scope-record template; consensus supplies only
	// the execution heights after quorum. Keep this validation fork-aware so a
	// historical pre-v20 op==8 remains the same inert unknown operation that an
	// older binary would record and later fail to apply.
	if app.postAppV20Fork(height) && op == governance.OpScopeAction {
		if _, scopeErr := app.prepareScopeProposal(gp.TargetID, gp.TargetPubKey, gp.TargetPower, gp.Payload, proposerID, height, false); scopeErr != nil {
			return &abcitypes.ExecTxResult{Code: 72, Log: "governance propose: invalid OpScopeAction: " + scopeErr.Error()}
		}
	}
	if app.postAppV20Fork(height) {
		if isValidatorSetGovernanceOperation(op) {
			if validatorErr := app.validateAppV20ValidatorOperation(op, gp.TargetID, gp.TargetPubKey, gp.TargetPower); validatorErr != nil {
				return &abcitypes.ExecTxResult{Code: 72, Log: "governance propose: invalid validator operation: " + validatorErr.Error()}
			}
		} else {
			switch op {
			case governance.OpDomainReassign,
				governance.OpMemoryDomainRepair,
				governance.OpSyncGroupAction,
				governance.OpScopeAction:
				// Each known non-validator operation has its own validation and/or
				// intentionally inert attestation semantics.
			case governance.OpUpgrade:
				// Rejected by the dedicated post-app-v8 guard above.
			default:
				return &abcitypes.ExecTxResult{Code: 72, Log: fmt.Sprintf("governance propose: unknown operation %d", op)}
			}
		}
	}

	proposalID, propErr := app.govEngine.Propose(
		proposerID, op, gp.TargetID, gp.TargetPubKey,
		gp.TargetPower, gp.ExpiryBlocks, gp.Reason, height,
		gp.Payload,
	)
	if propErr != nil {
		return &abcitypes.ExecTxResult{Code: 73, Log: "governance propose failed: " + propErr.Error()}
	}

	app.logger.Info().
		Str("proposal_id", proposalID).
		Str("proposer", proposerID).
		Uint8("operation", uint8(gp.Operation)).
		Str("target", gp.TargetID).
		Msg("governance proposal created")

	// Buffer offchain proposal write for Commit.
	expiryBlocks := gp.ExpiryBlocks
	if expiryBlocks <= 0 {
		expiryBlocks = governance.DefaultExpiryBlocks
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "gov_proposal",
		data: govProposalData{
			ProposalID:    proposalID,
			Operation:     app.governanceOperationName(op, height),
			TargetID:      gp.TargetID,
			TargetPower:   gp.TargetPower,
			ProposerID:    proposerID,
			Status:        string(governance.StatusVoting),
			CreatedHeight: height,
			ExpiryHeight:  height + expiryBlocks,
			Reason:        gp.Reason,
		},
	})

	// Buffer the auto-vote for Commit.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "gov_vote",
		data: govVoteData{
			ProposalID:  proposalID,
			ValidatorID: proposerID,
			Decision:    "accept",
			Height:      height,
		},
	})

	return &abcitypes.ExecTxResult{Code: 0, Log: "proposal created: " + proposalID}
}

// processGovVote handles a TxTypeGovVote transaction.
func (app *SageApp) processGovVote(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if parsedTx.GovVote == nil {
		return &abcitypes.ExecTxResult{Code: 74, Log: "missing governance vote payload"}
	}

	voterID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
	gv := parsedTx.GovVote
	if app.postAppV20Fork(height) {
		if err := app.authorizeDelegatedScopeMutation(parsedTx, gv.ProposalID); err != nil {
			return &abcitypes.ExecTxResult{Code: 76, Log: "governance vote authorization failed: " + err.Error()}
		}
	}

	decisionStr := voteDecisionToGovString(gv.Decision)
	if decisionStr == "" {
		return &abcitypes.ExecTxResult{Code: 75, Log: "invalid vote decision"}
	}

	if err := app.govEngine.Vote(gv.ProposalID, voterID, decisionStr, height); err != nil {
		return &abcitypes.ExecTxResult{Code: 76, Log: "governance vote failed: " + err.Error()}
	}

	app.logger.Info().
		Str("proposal_id", gv.ProposalID).
		Str("voter", voterID).
		Str("decision", decisionStr).
		Msg("governance vote recorded")

	// Buffer offchain vote write for Commit.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "gov_vote",
		data: govVoteData{
			ProposalID:  gv.ProposalID,
			ValidatorID: voterID,
			Decision:    decisionStr,
			Height:      height,
		},
	})

	return &abcitypes.ExecTxResult{Code: 0, Log: "vote recorded"}
}

// processGovCancel handles a TxTypeGovCancel transaction.
func (app *SageApp) processGovCancel(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if parsedTx.GovCancel == nil {
		return &abcitypes.ExecTxResult{Code: 77, Log: "missing governance cancel payload"}
	}

	cancellerID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
	gc := parsedTx.GovCancel
	cancelsAppV20 := false
	auditComplete, auditErr := app.appV20LegacyResourceAuditComplete()
	if auditErr != nil {
		panic(fmt.Sprintf("sage: cannot inspect app-v20 audit marker before governance cancellation: %v", auditErr))
	}
	if auditComplete {
		proposal, proposalErr := app.govEngine.LoadProposal(gc.ProposalID)
		if proposalErr != nil {
			return &abcitypes.ExecTxResult{Code: 78, Log: "governance cancel failed: " + proposalErr.Error()}
		}
		cancelsAppV20 = isAppV20CeremonyProposal(proposal)
	}
	if app.postAppV20Fork(height) {
		if err := app.authorizeDelegatedScopeMutation(parsedTx, gc.ProposalID); err != nil {
			return &abcitypes.ExecTxResult{Code: 78, Log: "governance cancel authorization failed: " + err.Error()}
		}
	}

	if err := app.govEngine.Cancel(gc.ProposalID, cancellerID, height); err != nil {
		return &abcitypes.ExecTxResult{Code: 78, Log: "governance cancel failed: " + err.Error()}
	}
	if cancelsAppV20 {
		if err := app.badgerStore.DeleteState(appV20LegacyResourceAuditStateKey); err != nil {
			panic(fmt.Sprintf("sage: failed to clear app-v20 audit marker with cancelled proposal %s: %v", gc.ProposalID, err))
		}
	}

	app.logger.Info().
		Str("proposal_id", gc.ProposalID).
		Str("canceller", cancellerID).
		Msg("governance proposal cancelled")

	// Buffer offchain status update for Commit.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "gov_status_update",
		data: govStatusUpdateData{
			ProposalID: gc.ProposalID,
			Status:     string(governance.StatusCancelled),
		},
	})

	return &abcitypes.ExecTxResult{Code: 0, Log: "proposal cancelled"}
}

// ---------------------------------------------------------------------------
// v7.5 auto-upgrade machinery — STUB HANDLERS
//
// These three handlers (UpgradePropose / UpgradeCancel / UpgradeRevert)
// are intentionally stubs for v7.5 task #0. They prove that the codec and
// the ABCI dispatch switch can round-trip the new tx types end-to-end:
//   - identity verification runs
//   - payload validation runs
//   - the action is logged
//   - the tx is accepted with code 0
//
// They DO NOT mutate ABCI state (no BadgerDB writes, no pendingWrites,
// no governance.Engine calls, no consensus param updates). State wiring
// — UpgradePlan persistence, the upgrade-watchdog goroutine, the
// FinalizeBlock activation check, the rollback snapshot — lands in a
// later commit alongside the watchdog. See:
//   - docs/ROADMAP.md (v7.5/v8 section)
//   - /tmp/sage-roadmap/upgrade-machinery.md (full design)
//
// Error code allocation: codes 47, 48, 49 (each handler reuses its single
// code for both missing-payload and identity-verification failures, in
// line with the processOrgRegister / processOrgAddMember pattern).

// defaultUpgradeDelayBlocks is the chain-side floor on how far in the
// future an UpgradePlan's ActivationHeight is computed from the
// proposal-execution block. Used when the proposal payload's
// UpgradeDelayBlocks is zero or below the floor. ~10 min at 3s blocks.
const defaultUpgradeDelayBlocks = int64(200)

// UpgradeProposalPayload is the JSON body carried in a governance OpUpgrade
// proposal's Payload (app-v8). It fully determines the UpgradePlanRecord that
// gets persisted once the proposal reaches 2/3 quorum, so the supermajority is
// approving an exact, immutable plan — there is nothing for an executor to
// substitute (unlike OpDomainReassign, which needs a follow-up body-match tx).
type UpgradeProposalPayload struct {
	Name               string `json:"name"`
	TargetAppVersion   uint64 `json:"target_app_version"`
	BinarySHA256       string `json:"binary_sha256,omitempty"`
	UpgradeDelayBlocks int64  `json:"upgrade_delay_blocks,omitempty"`
	GovernanceDomain   string `json:"governance_domain,omitempty"`
}

// applyUpgradeProposal persists the pending UpgradePlanRecord for an executed
// (2/3-accepted) app-v8 OpUpgrade proposal. Runs in FinalizeBlock's governance
// post-processing (before the activation block), so the plan's ActivationHeight
// (= height + delay, delay >= 200) is always in the future and never activates
// in the same block. Deterministic on every replica: it reads only consensus
// state (the proposal payload, currentAppVersion, the pending-plan slot).
func (app *SageApp) applyUpgradeProposal(proposal *governance.ProposalState, height int64) error {
	if proposal == nil {
		return errors.New("apply upgrade proposal: nil proposal")
	}
	markerTagsCeremony := false
	if proposal.TargetID == appV20UpgradeName {
		auditComplete, err := app.appV20LegacyResourceAuditComplete()
		if err != nil {
			return fmt.Errorf("read app-v20 ceremony audit marker: %w", err)
		}
		markerTagsCeremony = auditComplete
	}
	var p UpgradeProposalPayload
	if err := json.Unmarshal(proposal.Payload, &p); err != nil {
		// Should never happen — we marshalled this payload ourselves at propose
		// time. Legacy proposals retain their deterministic skip; an app-v20
		// ceremony fails closed so the outer consensus transaction can discard the
		// proposal status change together with every other block mutation.
		app.logger.Error().Err(err).Str("proposal_id", proposal.ProposalID).Msg("app-v8: cannot decode upgrade proposal payload; skipping activation")
		if markerTagsCeremony {
			return fmt.Errorf("decode app-v20 upgrade proposal payload: %w", err)
		}
		return nil
	}
	appV20Ceremony := proposal.TargetID == appV20UpgradeName &&
		isAppV20CeremonyUpgrade(p.Name, p.TargetAppVersion, p.GovernanceDomain)
	if markerTagsCeremony && !appV20Ceremony {
		return fmt.Errorf("app-v20 ceremony audit marker does not match the exact proposal triple")
	}

	// Canonical-name re-guard at EXECUTE time (defense in depth). The sanctioned
	// creation path (processUpgradePropose) already enforces this, but a proposal
	// that predates the app-v8 gate could in principle have been created via the
	// generic gov path with a non-canonical name; persisting it would bump
	// version.app while flipping no fork gate (the v8.4.x halt class). Skip it.
	if want := tx.CanonicalUpgradeName(p.TargetAppVersion); p.Name != want {
		app.logger.Warn().
			Str("name", p.Name).
			Uint64("target_app_version", p.TargetAppVersion).
			Str("want", want).
			Msg("app-v8: approved upgrade has a non-canonical name; skipping plan persist")
		if appV20Ceremony {
			return fmt.Errorf("app-v20 upgrade has non-canonical name %q (want %q)", p.Name, want)
		}
		return nil
	}
	if appV20Ceremony {
		if p.TargetAppVersion != 20 || p.Name != appV20UpgradeName {
			return fmt.Errorf("app-v20 ceremony has name %q and target version %d", p.Name, p.TargetAppVersion)
		}
		if current := app.currentAppVersion(); current != 19 {
			app.logger.Warn().
				Uint64("current_app_version", current).
				Msg("app-v20 upgrade does not immediately follow app-v19; skipping plan persist")
			return fmt.Errorf("app-v20 upgrade requires current app version 19, got %d", current)
		}
		if !isCanonicalAppV20CeremonyDomain(p.GovernanceDomain) {
			app.logger.Warn().Str("name", p.Name).Msg("app-v20 upgrade has no canonical governance delegation domain; skipping plan persist")
			return fmt.Errorf("app-v20 upgrade has no canonical governance delegation domain")
		}
		if readinessErr := app.appV20ConsensusReadiness(); readinessErr != nil {
			app.logger.Warn().Err(readinessErr).Str("name", p.Name).Msg("app-v20 upgrade readiness failed; skipping plan persist")
			return fmt.Errorf("app-v20 upgrade readiness failed: %w", readinessErr)
		}
	}

	// Execution-height regression re-guard: the chain's committed app version
	// may have advanced (another upgrade activated) between propose and quorum.
	// currentAppVersion() reads in-memory fork heights set identically on every
	// replica at this point, so this branch is replica-deterministic.
	if p.TargetAppVersion <= app.currentAppVersion() {
		app.logger.Warn().
			Str("name", p.Name).
			Uint64("target_app_version", p.TargetAppVersion).
			Uint64("current_app_version", app.currentAppVersion()).
			Msg("app-v8: approved upgrade no longer advances the app version (regression/no-op); skipping plan persist")
		if appV20Ceremony {
			return fmt.Errorf("app-v20 upgrade target version %d does not advance current version %d", p.TargetAppVersion, app.currentAppVersion())
		}
		return nil
	}

	// At-most-one-pending-plan re-guard at EXECUTE time. The propose-time check
	// ran when no plan was pending, but the gov:active singleton only serialises
	// PROPOSALS — a plan persisted by a prior approved upgrade (awaiting its
	// ActivationHeight) is a separate slot. Never overwrite it; SetUpgradePlan
	// would clobber the single 'upgrade:plan' key silently.
	existing, getErr := app.badgerStore.GetUpgradePlan()
	if getErr != nil && !errors.Is(getErr, store.ErrNoUpgradePlan) {
		app.logger.Error().Err(getErr).Str("name", p.Name).Msg("app-v8: read pending upgrade plan failed")
		if appV20Ceremony {
			return fmt.Errorf("read pending plan before app-v20 upgrade execution: %w", getErr)
		}
	}
	if existing != nil {
		app.logger.Warn().
			Str("name", p.Name).
			Str("pending_plan", existing.Name).
			Int64("pending_activation_height", existing.ActivationHeight).
			Msg("app-v8: a plan is already pending; skipping persist of the newly-approved upgrade")
		if appV20Ceremony {
			return fmt.Errorf("cannot persist app-v20 upgrade plan while plan %q is pending", existing.Name)
		}
		return nil
	}

	delay := p.UpgradeDelayBlocks
	if floor := effectiveUpgradeDelayFloorBlocks(); delay < floor {
		delay = floor
	}
	rec := &store.UpgradePlanRecord{
		Name:             p.Name,
		TargetAppVersion: p.TargetAppVersion,
		ActivationHeight: height + delay,
		BinarySHA256:     p.BinarySHA256,
		GovernanceDomain: p.GovernanceDomain,
		ProposedAt:       height,
		ProposerID:       proposal.ProposerID,
	}
	if setErr := app.badgerStore.SetUpgradePlan(rec); setErr != nil {
		app.logger.Error().Err(setErr).Str("name", p.Name).Msg("app-v8: persist approved upgrade plan failed")
		return setErr
	}
	app.logger.Info().
		Str("name", p.Name).
		Uint64("target_app_version", p.TargetAppVersion).
		Int64("activation_height", rec.ActivationHeight).
		Str("proposal_id", proposal.ProposalID).
		Int64("height", height).
		Msg("app-v8: upgrade plan persisted after 2/3 governance quorum")
	return nil
}

// processUpgradePropose handles a TxTypeUpgradePropose transaction.
// On success, persists an UpgradePlanRecord in BadgerDB with
// ActivationHeight = height + max(payload.UpgradeDelayBlocks, defaultUpgradeDelayBlocks).
// At most one pending plan at a time — a proposal arriving while
// another is pending is rejected (code 47).
func (app *SageApp) processUpgradePropose(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	prop := parsedTx.UpgradePropose
	if prop == nil {
		return &abcitypes.ExecTxResult{Code: 47, Log: "missing upgrade propose payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	proposerID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Validate required fields.
	if prop.Name == "" {
		return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: name is required"}
	}
	if prop.TargetAppVersion == 0 {
		return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: target_app_version must be > 0"}
	}
	appV20Ceremony := isAppV20CeremonyUpgrade(prop.Name, prop.TargetAppVersion, prop.GovernanceDomain)
	if appV20Ceremony {
		// app-v20 changes the consensus persistence boundary and is deliberately
		// not a skip-ahead upgrade. Requiring the immediately preceding app version
		// prevents an old chain from reaching the Internet-federation rules without
		// every intervening consensus invariant being active.
		if current := app.currentAppVersion(); current != 19 {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
				"upgrade propose: app-v20 requires current committed app version 19 (got %d)", current)}
		}
		if !isCanonicalAppV20CeremonyDomain(prop.GovernanceDomain) {
			return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: app-v20 requires a canonical 32-byte lowercase governance_domain"}
		}
	}

	// app-v6 (postV8_5Fork): self-defending consensus guards on the proposal.
	// Pre-fork this entire block is skipped, so historical blocks — which
	// accepted non-canonical names / regressions with Code 0 — replay
	// byte-identically. At this point Name != "" (rejected above) and
	// TargetAppVersion != 0 are already guaranteed, so CanonicalUpgradeName(0)
	// is never compared. recordV8_5Branch fires once here for the whole
	// handler, mirroring processDomainReassign's single recordV8Branch call.
	postV85 := app.postV8_5Fork(height)
	recordV8_5Branch(postV85)
	if postV85 {
		// Change 1: the plan Name MUST be the canonical fork-gate activation
		// key for its TargetAppVersion. A mismatch (e.g. a human binary label
		// "v8.5.0" instead of "app-v6") bumps the CometBFT app version at
		// activation but leaves every postV8_*Fork gate false forever,
		// silently disabling the consensus rules the upgrade was meant to
		// enable. The watchdog already derives Name from CanonicalUpgradeName;
		// this defends the chain against any other proposer that does not.
		if want := tx.CanonicalUpgradeName(prop.TargetAppVersion); prop.Name != want {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
				"upgrade propose: non-canonical name %q for target_app_version=%d (want %q)",
				prop.Name, prop.TargetAppVersion, want)}
		}

		// Change 2: regression / no-op guard. CometBFT offers no protection
		// against a plan that bumps consensus_params.version.app DOWNWARD
		// (a fatal app-version regression at the handshake) or re-proposes the
		// current version (a no-op that burns the single pending-plan slot).
		// currentAppVersion() is the chain's committed version — the same value
		// Info() announces and FinalizeBlock bumps version.app to — derived
		// deterministically from the activated fork gates, so every replica
		// evaluates this identically at this height. <= rejects equality too
		// (the no-op case); skip-ahead stays legal (reconcile backfills gates).
		cur := app.currentAppVersion()
		if prop.TargetAppVersion <= cur {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
				"upgrade propose: target_app_version %d must exceed current committed app version %d (regression/no-op rejected)",
				prop.TargetAppVersion, cur)}
		}
	}

	// app-v8: once active, an UpgradePropose no longer self-activates. It is
	// routed through the existing 2/3 governance quorum (governance.OpUpgrade) —
	// the UpgradePlanRecord is persisted only after a validator supermajority
	// accepts (applyUpgradeProposal). Gated by postAppV8Fork's strict
	// greater-than, so pre-fork chains (every chain that exists today) replay the
	// self-activating path below BYTE-IDENTICALLY, and app-v8's OWN activating
	// propose — which runs while appV8AppliedHeight is still 0 — takes that same
	// old path (no chicken-and-egg). recordAppV8Branch fires once for the handler.
	// Uses postAppV8Rules (app-v8 OR app-v9), so a chain that skip-activated app-v9
	// without app-v8 still routes upgrades through the 2/3 quorum instead of
	// falling back to the legacy single-signer self-activating path below.
	postV8 := app.postAppV8Rules(height)
	recordAppV8Branch(postV8)
	// The exactly tagged app-v20 ceremony is never allowed to enter the legacy
	// single-signer path, even if a malformed chain is missing the app-v8 gate.
	// The exact-v19 prerequisite above plus this forced routing makes its
	// GovernanceDomain effective only after validator-supermajority approval.
	if postV8 || appV20Ceremony {
		// Canonical-name + regression guards run UNCONDITIONALLY on the app-v8
		// path: app-v8 is an INDEPENDENT gate, so it can be active on a chain
		// where postV8_5Fork is false and the guards above were skipped. Keeping
		// a non-canonical name (which would bump version.app yet flip no fork
		// gate — the v8.4.x halt class) or a version regression off the governance
		// ballot in the first place. Idempotent when both forks are active.
		if want := tx.CanonicalUpgradeName(prop.TargetAppVersion); prop.Name != want {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
				"upgrade propose: non-canonical name %q for target_app_version=%d (want %q)",
				prop.Name, prop.TargetAppVersion, want)}
		}
		if prop.TargetAppVersion <= app.currentAppVersion() {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
				"upgrade propose: target_app_version %d must exceed current committed app version %d (regression/no-op rejected)",
				prop.TargetAppVersion, app.currentAppVersion())}
		}

		// The governance proposer is the TX-SIGNING identity — the same
		// validator-power identity every other gov handler (processGovPropose,
		// processGovVote) and the validator set use. Keying the auto-accept vote
		// by the signing key is what lets a validator-proposer's own vote count
		// toward the 2/3 quorum (the agent-proof key may differ and would not).
		govProposerID := auth.PublicKeyToAgentID(parsedTx.PublicKey)

		// Admin-gated: seeding a chain-wide upgrade proposal occupies the single
		// governance slot, so restrict it to admin agents (mirrors
		// processGovPropose). This also closes a griefing vector — otherwise any
		// Ed25519 key could squat gov:active and stall validator-set governance,
		// and the per-proposer cooldown is defeated by key rotation. The
		// auto-upgrade watchdog targets a pre-app-v8 version, so it never reaches
		// this branch; app-v8+ upgrades are operator-driven.
		proposer, getErr := app.badgerStore.GetRegisteredAgent(govProposerID)
		if getErr != nil {
			return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: proposer not registered: " + getErr.Error()}
		}
		if proposer.Role != "admin" {
			return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: under app-v8 only admin agents may propose upgrades (2/3 governance quorum required)"}
		}

		// Don't open a new upgrade vote while a previously-approved plan is still
		// pending activation: the gov:active singleton only serialises PROPOSALS,
		// not the separate pending-plan slot (see applyUpgradeProposal's re-guard).
		if existing, planErr := app.badgerStore.GetUpgradePlan(); planErr == nil && existing != nil {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
				"upgrade propose: plan %q is already pending (activation_height=%d)",
				existing.Name, existing.ActivationHeight)}
		}
		if appV20Ceremony {
			// Bootstrap is deliberately a dedicated block. Before this proposal
			// and its audit marker commit, a raw/forged target-20 transaction may
			// select only the crash-atomic storage boundary; it must not impose
			// v20 admission rules on unrelated legacy transactions in the block.
			if app.badgerStore.InConsensusTransaction() && !app.appV20StrictFinalize {
				if app.appV20FinalizeTxCount != 1 {
					return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: app-v20 bootstrap must be the only transaction in its block"}
				}
				if !app.appV20FinalizeBudgetOK {
					return &abcitypes.ExecTxResult{Code: appV20AtomicFinalizeBudgetCode, Log: appV20AtomicFinalizeBudgetLog}
				}
			}
			if resourceErr := validateAppV20TxResources(parsedTx); resourceErr != nil {
				return &abcitypes.ExecTxResult{Code: appV20ResourceLimitCode, Log: "app-v20 resource limit: " + resourceErr.Error()}
			}
			if readinessErr := app.appV20AuthenticatedProposalReadiness(); readinessErr != nil {
				return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: app-v20 readiness failed: " + readinessErr.Error()}
			}
		}

		payload, mErr := json.Marshal(UpgradeProposalPayload{
			Name:               prop.Name,
			TargetAppVersion:   prop.TargetAppVersion,
			BinarySHA256:       prop.BinarySHA256,
			UpgradeDelayBlocks: prop.UpgradeDelayBlocks,
			GovernanceDomain:   prop.GovernanceDomain,
		})
		if mErr != nil {
			return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf("upgrade propose: encode payload: %v", mErr)}
		}

		reason := "app-version upgrade to " + prop.Name
		proposalID, propErr := app.govEngine.Propose(
			govProposerID, governance.OpUpgrade, prop.Name, nil, 0,
			0 /* default expiry */, reason, height, payload,
		)
		if propErr != nil {
			return &abcitypes.ExecTxResult{Code: 47, Log: "upgrade propose: governance propose failed: " + propErr.Error()}
		}
		if appV20Ceremony {
			if markerErr := app.badgerStore.SetState(appV20LegacyResourceAuditStateKey, appV20LegacyResourceAuditValue); markerErr != nil {
				panic(fmt.Sprintf("sage: failed to persist app-v20 legacy resource audit marker after proposal %s: %v", proposalID, markerErr))
			}
			if app.badgerStore.InConsensusTransaction() {
				app.appV20ProposalBootstrapped = true
			}
		}

		// Mirror processGovPropose's offchain buffering so the proposal and the
		// proposer's auto-accept vote surface in the SQL mirror / dashboard.
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "gov_proposal",
			data: govProposalData{
				ProposalID:    proposalID,
				Operation:     opToString(governance.OpUpgrade),
				TargetID:      prop.Name,
				ProposerID:    govProposerID,
				Status:        string(governance.StatusVoting),
				CreatedHeight: height,
				ExpiryHeight:  height + governance.DefaultExpiryBlocks,
				Reason:        reason,
			},
		})
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "gov_vote",
			data: govVoteData{
				ProposalID:  proposalID,
				ValidatorID: govProposerID,
				Decision:    "accept",
				Height:      height,
			},
		})

		app.logger.Info().
			Str("name", prop.Name).
			Uint64("target_app_version", prop.TargetAppVersion).
			Str("proposal_id", proposalID).
			Str("proposer_id", govProposerID).
			Int64("height", height).
			Msg("app-v8: upgrade routed through 2/3 governance quorum (awaiting validator votes)")

		return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf(
			"upgrade proposal created (awaiting 2/3 quorum): proposal_id=%s name=%s target_app_version=%d",
			proposalID, prop.Name, prop.TargetAppVersion)}
	}

	// Refuse if another plan is already pending. At-most-one semantics
	// keep the FinalizeBlock activation check deterministic and avoid
	// the question of "which plan wins" when two arrive in the same block.
	if existing, getErr := app.badgerStore.GetUpgradePlan(); getErr == nil && existing != nil {
		return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf(
			"upgrade propose: plan %q is already pending (activation_height=%d)",
			existing.Name, existing.ActivationHeight)}
	}

	// Activation height is chain-computed, not validator-chosen. Each
	// node sees the same height + delay, so every replica resolves the
	// same number deterministically — no multi-validator drift.
	delay := prop.UpgradeDelayBlocks
	if floor := effectiveUpgradeDelayFloorBlocks(); delay < floor {
		delay = floor
	}
	rec := &store.UpgradePlanRecord{
		Name:             prop.Name,
		TargetAppVersion: prop.TargetAppVersion,
		ActivationHeight: height + delay,
		BinarySHA256:     prop.BinarySHA256,
		GovernanceDomain: prop.GovernanceDomain,
		ProposedAt:       height,
		ProposerID:       proposerID,
	}
	if setErr := app.badgerStore.SetUpgradePlan(rec); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 47, Log: fmt.Sprintf("upgrade propose: persist failed: %v", setErr)}
	}

	app.logger.Info().
		Str("name", prop.Name).
		Uint64("target_app_version", prop.TargetAppVersion).
		Int64("activation_height", rec.ActivationHeight).
		Str("binary_sha256", prop.BinarySHA256).
		Str("proposer_id", proposerID).
		Int64("height", height).
		Time("block_time", blockTime).
		Msg("upgrade plan persisted")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf(
		"upgrade plan accepted: name=%s target_app_version=%d activation_height=%d",
		prop.Name, prop.TargetAppVersion, rec.ActivationHeight)}
}

// processUpgradeCancel handles a TxTypeUpgradeCancel transaction. On
// success, deletes the pending UpgradePlan. Refuses if:
//   - no plan is pending (code 48)
//   - the pending plan's name doesn't match (code 48)
//   - the plan's ActivationHeight has already passed (code 48) — cancel
//     after activation has no meaning; use TxTypeUpgradeRevert instead
func (app *SageApp) processUpgradeCancel(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	cancel := parsedTx.UpgradeCancel
	if cancel == nil {
		return &abcitypes.ExecTxResult{Code: 48, Log: "missing upgrade cancel payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	cancellerID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 48, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// app-v8: a pending plan post-fork was approved by a 2/3 quorum, so it must
	// not be deletable by a lone non-admin key (approve needs 2/3, cancel would
	// otherwise need 1). Require admin to cancel post-fork — matching the
	// admin-gated propose path. Gated behind postAppV8Rules (app-v8 OR app-v9) so
	// pre-fork chains replay the single-signer behaviour byte-identically, while an
	// app-v9-without-app-v8 chain still admin-gates cancel.
	if app.postAppV8Rules(height) {
		canceller, regErr := app.badgerStore.GetRegisteredAgent(auth.PublicKeyToAgentID(parsedTx.PublicKey))
		if regErr != nil {
			return &abcitypes.ExecTxResult{Code: 48, Log: "upgrade cancel: canceller not registered: " + regErr.Error()}
		}
		if canceller.Role != "admin" {
			return &abcitypes.ExecTxResult{Code: 48, Log: "upgrade cancel: under app-v8 only admin agents may cancel a pending upgrade plan"}
		}
	}

	// Validate required fields.
	if cancel.Name == "" {
		return &abcitypes.ExecTxResult{Code: 48, Log: "upgrade cancel: name is required"}
	}

	plan, getErr := app.badgerStore.GetUpgradePlan()
	if getErr != nil || plan == nil {
		return &abcitypes.ExecTxResult{Code: 48, Log: "upgrade cancel: no plan pending"}
	}
	if plan.Name != cancel.Name {
		return &abcitypes.ExecTxResult{Code: 48, Log: fmt.Sprintf(
			"upgrade cancel: name mismatch (pending=%q, cancel=%q)", plan.Name, cancel.Name)}
	}
	if height >= plan.ActivationHeight {
		return &abcitypes.ExecTxResult{Code: 48, Log: fmt.Sprintf(
			"upgrade cancel: too late (height=%d >= activation_height=%d)", height, plan.ActivationHeight)}
	}
	if delErr := app.badgerStore.DeleteUpgradePlan(); delErr != nil {
		return &abcitypes.ExecTxResult{Code: 48, Log: fmt.Sprintf("upgrade cancel: delete failed: %v", delErr)}
	}
	if isAppV20CeremonyPlan(plan) {
		if markerErr := app.badgerStore.DeleteState(appV20LegacyResourceAuditStateKey); markerErr != nil {
			panic(fmt.Sprintf("sage: failed to clear app-v20 audit marker with cancelled plan %s: %v", plan.Name, markerErr))
		}
	}

	app.logger.Info().
		Str("name", cancel.Name).
		Str("canceller_id", cancellerID).
		Str("reason", cancel.Reason).
		Int64("height", height).
		Int64("would_have_activated_at", plan.ActivationHeight).
		Msg("upgrade plan cancelled")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("upgrade plan %q cancelled", cancel.Name)}
}

// processUpgradeRevert handles a TxTypeUpgradeRevert transaction.
//
// Pre-app-v6 (postV8_5Fork false) this is a byte-identical Code-0 no-op stub —
// every block on every chain that exists today replays unchanged. Post-app-v6
// it is an EXPLICIT REJECT (Code 90): a live in-band downgrade is replay-unsafe
// by construction. Clearing a fork gate retroactively flips the execution
// branch of committed blocks H_act+1..H_revert, so their AppHashes no longer
// reproduce on replay → CometBFT halt. True rollback is a forward upgrade plus
// an off-chain snapshot rewind, not a consensus-rule tx. The identity + payload
// validation (Code 49) runs on BOTH branches, before the fork gate, so those
// failures stay byte-identical pre- and post-fork.
func (app *SageApp) processUpgradeRevert(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	revert := parsedTx.UpgradeRevert
	if revert == nil {
		return &abcitypes.ExecTxResult{Code: 49, Log: "missing upgrade revert payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	proposerID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 49, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Validate required fields.
	if revert.Name == "" {
		return &abcitypes.ExecTxResult{Code: 49, Log: "upgrade revert: name is required"}
	}

	postFork := app.postV8_5Fork(height)
	recordV8_5Branch(postFork)

	if !postFork {
		// Pre-app-v6: byte-identical no-op stub (Code 0, no state mutation).
		app.logger.Info().
			Str("name", revert.Name).
			Uint64("target_app_version", revert.TargetAppVersion).
			Int64("reverting_from_height", revert.RevertingFromHeight).
			Str("proposer_id", proposerID).
			Str("payload_proposer_id", revert.ProposerID).
			Int64("height", height).
			Msg("upgrade revert received (pre-fork stub — no state mutation)")

		return &abcitypes.ExecTxResult{Code: 0, Log: "upgrade tx accepted (pre-fork stub — no state mutation)"}
	}

	// Post-app-v6: reject. An in-band downgrade is replay-unsafe — clearing a
	// fork gate retroactively flips the execution branch of committed blocks
	// H_act+1..H_revert, so their AppHashes no longer reproduce on replay.
	// True rollback = forward upgrade + off-chain snapshot rewind, not a tx.
	app.logger.Warn().
		Str("name", revert.Name).
		Uint64("target_app_version", revert.TargetAppVersion).
		Int64("reverting_from_height", revert.RevertingFromHeight).
		Str("proposer_id", proposerID).
		Int64("height", height).
		Msg("upgrade revert rejected: in-band downgrade is replay-unsafe; use a forward upgrade + snapshot rollback")

	return &abcitypes.ExecTxResult{Code: 90, Log: "upgrade revert: in-band downgrade unsupported (replay-unsafe); use a forward upgrade and off-chain snapshot rollback"}
}

// ---------------------------------------------------------------------------
// v8.0: TxTypeDomainReassign — governance-gated domain ownership recovery
// ---------------------------------------------------------------------------
//
// Recovery primitive for domains that have been owner-captured (e.g. by a
// rogue first-writer after a chain reset). Authorized via a previously
// executed GovOpDomainReassign proposal that required a 3/4 supermajority
// (see governance.ThresholdFor). Reuses BadgerStore.TransferDomain — the
// proposal+supermajority+admin gate is the long-missing authorized caller
// referenced at TransferDomain's docstring.
//
// Error code allocation 80–88. 50 is reserved for Fix 2 (shared-domain
// grant rejection); 47–49 are upgrade-machinery codes.
//
// processDomainReassign is fork-gated. Pre-fork it returns Code 10
// ("unknown tx type") — symmetric with CheckTx's pre-fork gate, so the
// pre-fork replay AppHash is byte-identical to v7.1.1 (no state mutation,
// no nonce burn since FinalizeBlock only burns nonce on Code 0).

func (app *SageApp) processDomainReassign(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	postFork := app.postV8Fork(height)
	recordV8Branch(postFork)
	if !postFork {
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
	req := parsedTx.DomainReassign
	if req == nil {
		return &abcitypes.ExecTxResult{Code: 80, Log: "missing DomainReassign payload"}
	}

	// Verify agent identity on-chain via embedded Ed25519 proof.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 33, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Submitter must be a chain admin — this is the recovery primitive
	// gate. Defence in depth alongside the governance supermajority: the
	// proposal already required 3/4 of validators to accept, but we keep
	// the admin requirement on the execution tx so a non-admin can't race
	// in with an executed proposal ID after the proposer goes offline.
	agent, getErr := app.badgerStore.GetRegisteredAgent(senderID)
	if getErr != nil {
		return &abcitypes.ExecTxResult{Code: 80, Log: "domain reassign: sender not registered: " + getErr.Error()}
	}
	if agent.Role != "admin" {
		return &abcitypes.ExecTxResult{Code: 80, Log: "domain reassign: only admin agents can execute reassignment"}
	}

	// Load and validate the linked proposal.
	prop, err := app.govEngine.LoadProposal(req.ProposalID)
	if err != nil || prop == nil {
		return &abcitypes.ExecTxResult{Code: 81, Log: fmt.Sprintf("proposal not found: %s", req.ProposalID)}
	}
	if prop.Status != governance.StatusExecuted {
		return &abcitypes.ExecTxResult{Code: 82, Log: fmt.Sprintf("proposal not executed (status=%s)", prop.Status)}
	}
	if prop.Operation != governance.OpDomainReassign {
		return &abcitypes.ExecTxResult{Code: 82, Log: fmt.Sprintf("wrong operation type: got %d, want %d (OpDomainReassign)", prop.Operation, governance.OpDomainReassign)}
	}

	// Body match — the executing tx must reproduce the proposal's payload
	// exactly. Prevents an admin from substituting a different reassign
	// target after the supermajority has approved a specific one.
	var payload tx.DomainReassign
	if jsonErr := json.Unmarshal(prop.Payload, &payload); jsonErr != nil {
		return &abcitypes.ExecTxResult{Code: 83, Log: fmt.Sprintf("proposal payload decode: %v", jsonErr)}
	}
	if payload.Domain != req.Domain || payload.NewOwnerID != req.NewOwnerID || payload.OpenToShared != req.OpenToShared {
		return &abcitypes.ExecTxResult{Code: 83, Log: "proposal body mismatch (domain/new_owner/open_to_shared)"}
	}

	// Consumed-once check: a proposal authorizes exactly one execution.
	consumedKey := "gov:proposal:" + req.ProposalID + ":consumed"
	consumed, _ := app.badgerStore.GetState(consumedKey)
	if len(consumed) > 0 {
		return &abcitypes.ExecTxResult{Code: 84, Log: "proposal already consumed"}
	}

	// TTL — proposals stay executable for 2× the default expiry window.
	// After that the admin must re-propose, so stale recovery decisions
	// don't sit on the shelf indefinitely.
	if height > prop.CreatedHeight+2*governance.DefaultExpiryBlocks {
		return &abcitypes.ExecTxResult{Code: 85, Log: fmt.Sprintf(
			"proposal stale: created=%d, current=%d, ttl=%d blocks",
			prop.CreatedHeight, height, 2*governance.DefaultExpiryBlocks)}
	}

	// Existence + parent consistency.
	existingOwner, existingParent, _, domErr := app.badgerStore.GetDomainOwnerAndMeta(req.Domain)
	if domErr != nil {
		return &abcitypes.ExecTxResult{Code: 86, Log: fmt.Sprintf("domain not found: %s", req.Domain)}
	}
	if req.ParentDomain != "" && req.ParentDomain != existingParent {
		return &abcitypes.ExecTxResult{Code: 87, Log: fmt.Sprintf(
			"parent mismatch: tx=%q, existing=%q", req.ParentDomain, existingParent)}
	}
	parent := req.ParentDomain
	if parent == "" {
		parent = existingParent
	}
	if app.postAppV20Fork(height) {
		grantCount, exceeded, countErr := app.badgerStore.CountGrantsByDomainUpTo(req.Domain, maxAppV20DomainReassignGrantPurge)
		if countErr != nil {
			return &abcitypes.ExecTxResult{Code: 89, Log: fmt.Sprintf("grant invalidation preflight failed: %v", countErr)}
		}
		if exceeded {
			return &abcitypes.ExecTxResult{Code: 89, Log: fmt.Sprintf(
				"domain has at least %d grants; app-v20 reassignment limit is %d — revoke grants before retrying",
				grantCount, maxAppV20DomainReassignGrantPurge,
			)}
		}
	}

	// Execute the transfer — chain-authoritative.
	if transferErr := app.badgerStore.TransferDomain(req.Domain, req.NewOwnerID, parent, height); transferErr != nil {
		return &abcitypes.ExecTxResult{Code: 88, Log: fmt.Sprintf("transfer failed: %v", transferErr)}
	}

	// Invalidate ALL grants on the domain — the previous owner's
	// chain-of-trust does not survive the reassignment. The new owner
	// must re-grant explicitly.
	purged, purgeErr := app.badgerStore.DeleteGrantsByDomain(req.Domain)
	if purgeErr != nil {
		app.logger.Error().Err(purgeErr).Str("domain", req.Domain).Msg("failed to purge grants on domain reassignment")
		// Continue — chain ownership has already flipped; the purge is
		// best-effort cleanup. A future reassign or revoke can complete
		// the cleanup; we don't roll back the transfer.
	}

	// Optionally promote the domain to shared via the on-chain sentinel.
	// post-fork isSharedDomain reads this key and treats the domain as
	// catch-all-writable.
	if req.OpenToShared {
		if shErr := app.badgerStore.SetSharedDomain(req.Domain); shErr != nil {
			app.logger.Error().Err(shErr).Str("domain", req.Domain).Msg("failed to set shared_domain sentinel")
		}
	}

	// Mark proposal consumed — one-shot semantics.
	if cErr := app.badgerStore.SetState(consumedKey, []byte{1}); cErr != nil {
		app.logger.Error().Err(cErr).Str("proposal_id", req.ProposalID).Msg("failed to mark proposal consumed")
	}

	// Mirror the new ownership to the offchain store. The chain is
	// authoritative for owner reads (see v7.5.3); the mirror is advisory
	// for ops/analytics. InsertDomain is ON CONFLICT DO NOTHING in SQLite
	// today, so the mirror's owner column will lag — that's acceptable
	// for v8.0; a follow-up commit can add an UpsertDomain path if
	// dashboards need eventual consistency.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "domain_register",
		data: &store.DomainEntry{
			DomainName:    req.Domain,
			OwnerAgentID:  req.NewOwnerID,
			ParentDomain:  parent,
			CreatedHeight: height,
			CreatedAt:     blockTime,
		},
	})

	app.logger.Info().
		Str("domain", req.Domain).
		Str("previous_owner", existingOwner).
		Str("new_owner", req.NewOwnerID[:16]).
		Str("proposal_id", req.ProposalID).
		Bool("open_to_shared", req.OpenToShared).
		Int("purged_grants", purged).
		Int64("height", height).
		Msg("domain reassigned via governance")

	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf(
		"domain reassigned: %s -> %s (purged %d grants, open_to_shared=%t)",
		req.Domain, req.NewOwnerID, purged, req.OpenToShared)}
}

// applyGovernanceProposal applies an executed governance proposal to the validator set
// and returns a CometBFT ValidatorUpdate. Called from FinalizeBlock post-processing.
func (app *SageApp) applyGovernanceProposal(proposal *governance.ProposalState, height int64) (*abcitypes.ValidatorUpdate, error) {
	// app-v8: an executed OpUpgrade proposal persists the pending UpgradePlan
	// (no validator-set effect, no target pubkey). MUST be dispatched BEFORE
	// the validator-set pubkey derivation below — its 32-byte guard would
	// otherwise reject this proposal (TargetID="app-v<N>" is not a hex pubkey)
	// and the upgrade would silently never activate after quorum. Gated behind
	// postAppV8Fork: pre-fork an op==5 proposal (which could only have been
	// created by the generic gov path on a chain that predates this gate) falls
	// through to the unknown-op error below, exactly as it did before app-v8 —
	// so historical replay stays byte-identical.
	if proposal.Operation == governance.OpUpgrade && app.postAppV8Rules(height) {
		return nil, app.applyUpgradeProposal(proposal, height)
	}

	// app-v16: OpMemoryDomainRepair applies its {memory_id → domain} backfill
	// directly at proposal execution (like OpUpgrade — no validator-set effect and
	// no 32-byte pubkey target, so it MUST be dispatched BEFORE the pubkey derivation
	// below, whose length guard would otherwise reject it). Gated behind
	// postAppV16Rules so a pre-fork op==6 proposal (creatable only via the generic
	// gov path on a chain predating this gate) falls through to the unknown-op error
	// below exactly as before — historical replay stays byte-identical.
	if proposal.Operation == governance.OpMemoryDomainRepair && app.postAppV16Rules(height) {
		return nil, app.applyMemoryDomainRepair(proposal, height)
	}

	// app-v20: quorum execution directly installs the exact scope record voted
	// on by validators. Dispatch before validator pubkey derivation because the
	// TargetID is a scope ID, not an Ed25519 key. Before the fork op==8 retains
	// the historical unknown-op path for byte-identical replay.
	if proposal.Operation == governance.OpScopeAction && app.postAppV20Fork(height) {
		return nil, app.applyScopeProposal(proposal, height)
	}

	// OpDomainReassign has NO validator-set effect: the transfer is applied by the
	// separate DomainReassign tx (processDomainReassign), not here. Return cleanly
	// so it does not fall through to the validator-pubkey derivation below, whose
	// hex.DecodeString would fail on the domain-name target and log a spurious
	// "failed to apply governance proposal" ERR on every reassign. AppHash-neutral
	// and ungated: the caller appends a validator update ONLY on (err==nil &&
	// update!=nil), so both this clean (nil,nil) return and the prior (nil,error)
	// path append nothing - identical valUpdates and pendingWrites, so historical
	// replay stays byte-identical; the only change is the absence of the error log.
	if proposal.Operation == governance.OpDomainReassign {
		return nil, nil
	}
	if app.postAppV20Fork(height) && isValidatorSetGovernanceOperation(proposal.Operation) {
		if err := app.validateAppV20ValidatorOperation(
			proposal.Operation,
			proposal.TargetID,
			proposal.TargetPubKey,
			proposal.TargetPower,
		); err != nil {
			return nil, fmt.Errorf("app-v20 validator proposal validation: %w", err)
		}
	}

	pubKeyBytes := proposal.TargetPubKey
	if len(pubKeyBytes) == 0 {
		// Derive from target ID (which is hex-encoded pubkey for non-app validators)
		decoded, err := hex.DecodeString(proposal.TargetID)
		if err != nil {
			return nil, fmt.Errorf("cannot derive pubkey from target ID %s: %w", proposal.TargetID, err)
		}
		pubKeyBytes = decoded
	}

	if len(pubKeyBytes) != 32 {
		return nil, fmt.Errorf("invalid Ed25519 public key length: %d", len(pubKeyBytes))
	}

	// Build the CometBFT ValidatorUpdate using protobuf types directly.
	protoKey := cryptoproto.PublicKey{
		Sum: &cryptoproto.PublicKey_Ed25519{Ed25519: pubKeyBytes},
	}

	switch proposal.Operation {
	case governance.OpAddValidator:
		// Add to in-memory validator set.
		info := &validator.ValidatorInfo{
			ID:        proposal.TargetID,
			PublicKey: pubKeyBytes,
			Power:     proposal.TargetPower,
		}
		if err := app.validators.AddValidator(info); err != nil {
			// Validator might already exist from a previous run — update power instead.
			app.logger.Warn().Err(err).Msg("add validator failed, attempting power update")
			if upErr := app.validators.UpdatePower(proposal.TargetID, proposal.TargetPower); upErr != nil {
				return nil, fmt.Errorf("add/update validator: %w", upErr)
			}
		}

		// Persist updated validator set.
		if err := app.persistValidators(app.postAppV20Fork(height)); err != nil {
			return nil, fmt.Errorf("persist added validator set: %w", err)
		}

		app.logger.Info().
			Str("validator", proposal.TargetID).
			Int64("power", proposal.TargetPower).
			Int64("height", height).
			Msg("validator added via governance")

		return &abcitypes.ValidatorUpdate{PubKey: protoKey, Power: proposal.TargetPower}, nil

	case governance.OpRemoveValidator:
		if err := app.validators.RemoveValidator(proposal.TargetID); err != nil {
			return nil, fmt.Errorf("remove validator: %w", err)
		}

		if err := app.persistValidators(app.postAppV20Fork(height)); err != nil {
			return nil, fmt.Errorf("persist removed validator set: %w", err)
		}

		app.logger.Info().
			Str("validator", proposal.TargetID).
			Int64("height", height).
			Msg("validator removed via governance")

		return &abcitypes.ValidatorUpdate{PubKey: protoKey, Power: 0}, nil

	case governance.OpUpdatePower:
		if err := app.validators.UpdatePower(proposal.TargetID, proposal.TargetPower); err != nil {
			return nil, fmt.Errorf("update power: %w", err)
		}

		if err := app.persistValidators(app.postAppV20Fork(height)); err != nil {
			return nil, fmt.Errorf("persist updated validator powers: %w", err)
		}

		app.logger.Info().
			Str("validator", proposal.TargetID).
			Int64("power", proposal.TargetPower).
			Int64("height", height).
			Msg("validator power updated via governance")

		return &abcitypes.ValidatorUpdate{PubKey: protoKey, Power: proposal.TargetPower}, nil

	case governance.OpDomainReassign:
		// No validator-set effect at acceptance — the actual reassignment
		// runs in a follow-up TxTypeDomainReassign that the admin submits
		// after the proposal reaches Status=Executed. Returning (nil, nil)
		// keeps FinalizeBlock from emitting a spurious "failed to apply
		// governance proposal" log on every successful 3/4 supermajority
		// for a domain-reassign proposal.
		return nil, nil

	default:
		return nil, fmt.Errorf("unknown governance operation: %d", proposal.Operation)
	}
}

func (app *SageApp) requireScopeValidatorRemovalReady(validatorID string) error {
	drain, err := app.badgerStore.GetScopeValidatorDrain(validatorID)
	if err != nil {
		return err
	}
	if drain.Ready() {
		return nil
	}
	parts := make([]string, 0, 2)
	if len(drain.ActiveScopeIDs) > 0 {
		parts = append(parts, "active scope memberships ["+strings.Join(drain.ActiveScopeIDs, ",")+"]")
	}
	if len(drain.PendingMemoryIDs) > 0 {
		parts = append(parts, "pending scoped ballots ["+strings.Join(drain.PendingMemoryIDs, ",")+"]")
	}
	return fmt.Errorf("validator %q still has %s; update or retire its scopes and resolve every pinned ballot before removal", validatorID, strings.Join(parts, " and "))
}

// applyMemoryDomainRepair executes an OpMemoryDomainRepair (app-v16) proposal: it
// backfills the on-chain memdomain: record for legacy memories that predate app-v8.4
// (and so never received one), from the supermajority-attested {memory_id, domain}
// payload. Idempotent and defensive: it writes SetMemoryDomain ONLY for a memory that
// (a) exists on-chain (GetMemoryHash succeeds — skips a bogus/never-submitted ID) and
// (b) has no domain recorded yet (skips one already domained), and it never
// overwrites. Iteration is in payload order, which is byte-identical across replicas
// (same proposal payload), so no explicit sort is needed for determinism; every input
// is committed Badger state + the payload bytes, so each replica computes the same
// writes and the AppHash stays in lockstep. Dispatched from applyGovernanceProposal
// under postAppV16Rules.
func (app *SageApp) applyMemoryDomainRepair(proposal *governance.ProposalState, height int64) error {
	var entries []tx.MemoryDomainRepairEntry
	if err := json.Unmarshal(proposal.Payload, &entries); err != nil {
		return fmt.Errorf("memory domain repair: decode payload: %w", err)
	}
	repaired := 0
	for _, e := range entries {
		if e.MemoryID == "" || e.Domain == "" {
			continue
		}
		// Existence: only a real, submitted memory gets a domain. GetMemoryHash
		// errors on an unknown ID — skip it (never fabricate authorization state).
		if _, _, hErr := app.badgerStore.GetMemoryHash(e.MemoryID); hErr != nil {
			continue
		}
		// Idempotent: repair only fills the gap — never overwrite an existing domain.
		existing, dErr := app.badgerStore.GetMemoryDomain(e.MemoryID)
		if dErr != nil {
			return fmt.Errorf("memory domain repair: domain lookup %s: %w", e.MemoryID, dErr)
		}
		if existing != "" {
			continue
		}
		// The target domain must already be REGISTERED (have an owner). This (a)
		// keeps the backfill honest — a legacy memory's domain was auto-registered
		// at its original submit, so the real target always resolves — and (b) closes
		// a capture hole: repairing into an unregistered/shared domain would leave the
		// memory owned by nobody, so whoever registered that domain NEXT would inherit
		// deprecate authority over it. Deterministic committed-state read → AppHash-safe.
		if owner, ownErr := app.badgerStore.GetDomainOwner(e.Domain); ownErr != nil || owner == "" {
			continue
		}
		if err := app.badgerStore.SetMemoryDomain(e.MemoryID, e.Domain); err != nil {
			return fmt.Errorf("memory domain repair: set domain %s: %w", e.MemoryID, err)
		}
		repaired++
	}
	app.logger.Info().
		Str("proposal_id", proposal.ProposalID).
		Int("entries", len(entries)).
		Int("repaired", repaired).
		Int64("height", height).
		Msg("app-v16 memory domain repair applied")
	return nil
}

// persistValidators saves the current validator set to BadgerDB. app-v20 uses
// a complete replacement so a removed validator's old key cannot resurrect on
// restart. Historical blocks retain the upsert-only encoding for replay parity.
func (app *SageApp) persistValidators(replace bool) error {
	valMap := make(map[string]int64)
	for _, v := range app.validators.GetAll() {
		valMap[v.ID] = v.Power
	}
	if replace {
		return app.badgerStore.ReplaceValidators(valMap)
	}
	return app.badgerStore.SaveValidators(valMap)
}

// opToString converts a governance ProposalOp to a human-readable string.
func opToString(op governance.ProposalOp) string {
	switch op {
	case governance.OpAddValidator:
		return "add_validator"
	case governance.OpRemoveValidator:
		return "remove_validator"
	case governance.OpUpdatePower:
		return "update_power"
	case governance.OpDomainReassign:
		return "domain_reassign"
	case governance.OpUpgrade:
		return "upgrade"
	case governance.OpMemoryDomainRepair:
		return "memory_domain_repair"
	case governance.OpScopeAction:
		return "scope_action"
	default:
		return fmt.Sprintf("unknown_%d", op)
	}
}

// governanceOperationName keeps the off-chain projection replay-compatible:
// before app-v20, op==8 was unknown and must retain its historical label even
// though the upgraded binary knows the future operation name.
func (app *SageApp) governanceOperationName(op governance.ProposalOp, height int64) string {
	if op == governance.OpScopeAction && !app.postAppV20Fork(height) {
		return fmt.Sprintf("unknown_%d", op)
	}
	return opToString(op)
}

// voteDecisionToGovString converts a tx.VoteDecision to governance vote string.
func voteDecisionToGovString(d tx.VoteDecision) string {
	switch d {
	case tx.VoteDecisionAccept:
		return "accept"
	case tx.VoteDecisionReject:
		return "reject"
	case tx.VoteDecisionAbstain:
		return "abstain"
	default:
		return ""
	}
}
