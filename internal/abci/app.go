package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
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
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"

	cryptoproto "github.com/cometbft/cometbft/proto/tendermint/crypto"
)

// pendingWrite represents a PostgreSQL write buffered until Commit.
type pendingWrite struct {
	writeType string // "memory", "triples", "challenge", "vote", "corroborate", "epoch_score", "validator_score", "status_update", "access_grant", "access_request", "access_revoke", "access_log", "domain_register", "access_request_status", "org_register", "org_member", "org_member_remove", "org_member_clearance", "federation", "federation_approve", "federation_revoke", "mem_classification", "dept_register", "dept_member", "dept_member_remove", "agent_register", "agent_update", "agent_permission"
	data      interface{}
}

// statusUpdate carries the fields needed to update a memory's status in PostgreSQL.
type statusUpdate struct {
	MemoryID string
	Status   memory.MemoryStatus
	At       time.Time
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
}

// NewSupplementaryCache creates a cache with automatic eviction of stale entries.
func NewSupplementaryCache() *SupplementaryCache {
	c := &SupplementaryCache{items: make(map[string]*suppCacheEntry)}
	go c.evictLoop()
	return c
}

// Put stores supplementary data for a memory ID.
func (c *SupplementaryCache) Put(memoryID string, data *memory.SupplementaryData) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.items[memoryID] = &suppCacheEntry{data: data, storedAt: time.Now()}
}

// Pop retrieves and removes supplementary data for a memory ID.
func (c *SupplementaryCache) Pop(memoryID string) *memory.SupplementaryData {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := c.items[memoryID]
	delete(c.items, memoryID)
	if entry != nil {
		return entry.data
	}
	return nil
}

func (c *SupplementaryCache) evictLoop() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for range ticker.C {
		c.mu.Lock()
		now := time.Now()
		for id, entry := range c.items {
			if now.Sub(entry.storedAt) > 60*time.Second {
				delete(c.items, id)
			}
		}
		c.mu.Unlock()
	}
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
func (app *SageApp) postAppV7Fork(height int64) bool {
	return app.appV7AppliedHeight > 0 && height > app.appV7AppliedHeight
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
	return app.postAppV8Fork(height) || app.postAppV9Fork(height) || app.postAppV10Fork(height)
}

// postAppV9Rules reports whether app-v9's consensus rules (consensus-path
// nonce/replay enforcement + admin self-grant downgrade) are in force at this
// height. Same subsumption logic as postAppV8Rules: app-v9's rules are active
// whenever app-v9 OR any higher independent gate (app-v10) is, so an
// app-v10-without-app-v9 chain still enforces them. Collapses to exactly
// postAppV9Fork on every existing chain (appV10AppliedHeight==0), so replay is
// byte-identical.
func (app *SageApp) postAppV9Rules(height int64) bool {
	return app.postAppV9Fork(height) || app.postAppV10Fork(height)
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
	app.reconcilePoEForkMonotonicity()

	// Reload persisted validators from BadgerDB (survives restart)
	persistedVals, err := bs.LoadValidators()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to load persisted validators")
	} else if len(persistedVals) > 0 {
		for id, power := range persistedVals {
			info := &validator.ValidatorInfo{ID: id, Power: power}
			if addErr := app.validators.AddValidator(info); addErr != nil {
				logger.Warn().Err(addErr).Str("validator", id).Msg("failed to restore validator")
			}
		}
		logger.Info().Int("validators", app.validators.Size()).Msg("validators restored from state")
	}
	app.refreshPoEWeights()

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
	app.reconcilePoEForkMonotonicity()

	persistedVals, err := bs.LoadValidators()
	if err != nil {
		logger.Warn().Err(err).Msg("failed to load persisted validators")
	} else if len(persistedVals) > 0 {
		for id, power := range persistedVals {
			info := &validator.ValidatorInfo{ID: id, Power: power}
			if addErr := app.validators.AddValidator(info); addErr != nil {
				logger.Warn().Err(addErr).Str("validator", id).Msg("failed to restore validator")
			}
		}
		logger.Info().Int("validators", app.validators.Size()).Msg("validators restored from state")
	}
	app.refreshPoEWeights()

	return app, nil
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
	case app.appV10AppliedHeight > 0:
		return 10 // app-v10 (corroboration integrity guard + on-chain author) — independent gate, highest version, must rank first (10 > 9)
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
// fork gate for (currently app-v10). It is the readiness ceiling for upgrade
// auto-voting: a validator must never vote to activate an upgrade it cannot
// execute — doing so would commit consensus version.app=N while the binary
// still runs at N-1, halting the chain on the next CometBFT handshake (the
// maxSupportedAppVersion footgun). Bump this in lockstep with every new
// appV<N>UpgradeName fork gate added above.
const maxSupportedAppVersion uint64 = 10

// MaxSupportedAppVersion returns the highest app version this binary has a
// compiled fork gate for. Operator tooling (cmd/sage-gui `upgrade propose`)
// reads it to refuse proposing a target this binary cannot execute — the same
// readiness ceiling the auto-voter enforces via ActiveUpgradeVote. Exported
// because cmd/sage-gui lives outside this package; see maxSupportedAppVersion
// for the footgun this guards against.
func MaxSupportedAppVersion() uint64 { return maxSupportedAppVersion }

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
	return prop.ProposalID, payload.TargetAppVersion, payload.TargetAppVersion <= maxSupportedAppVersion, true
}

// UpgradeProposalHasVote reports whether voterID already has a recorded vote on
// the given proposal. The upgrade auto-voter (startAppValidators) uses it to
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
		info := &validator.ValidatorInfo{
			ID:    hex.EncodeToString(v.PubKey.GetEd25519()),
			Power: v.Power,
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

	metrics.ValidatorCount.Set(float64(app.validators.Size()))
	app.logger.Info().Int("validators", app.validators.Size()).Msg("chain initialized")

	return &abcitypes.ResponseInitChain{}, nil
}

// RegisterAppValidators replaces the validator set with application-level validators.
// This removes the genesis personal validator (which no longer votes) so that only
// the 4 app validators participate in quorum. Called from startAppValidators.
func (app *SageApp) RegisterAppValidators(validators map[string]int64) error {
	// Remove existing genesis validators that are NOT in the new app validator set.
	// This prevents phantom validators that never vote from blocking quorum.
	for _, existing := range app.validators.GetAll() {
		if _, isAppValidator := validators[existing.ID]; !isAppValidator {
			_ = app.validators.RemoveValidator(existing.ID)
			app.logger.Info().Str("id", existing.ID[:16]).Msg("removed genesis validator (replaced by app validators)")
		}
	}

	for id, power := range validators {
		info := &validator.ValidatorInfo{
			ID:    id,
			Power: power,
		}
		if err := app.validators.AddValidator(info); err != nil {
			// Already exists is OK (restart case)
			if !strings.Contains(err.Error(), "already exists") {
				return fmt.Errorf("register app validator %s: %w", id[:16], err)
			}
		}
	}
	// Persist updated validator set (app validators only)
	valMap := make(map[string]int64)
	for _, v := range app.validators.GetAll() {
		valMap[v.ID] = v.Power
	}
	return app.badgerStore.SaveValidators(valMap)
}

// CheckTx validates a transaction before it enters the mempool.
func (app *SageApp) CheckTx(_ context.Context, req *abcitypes.RequestCheckTx) (*abcitypes.ResponseCheckTx, error) {
	parsedTx, err := tx.DecodeTx(req.Tx)
	if err != nil {
		return &abcitypes.ResponseCheckTx{Code: 1, Log: fmt.Sprintf("decode error: %v", err)}, nil
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

	return &abcitypes.ResponseCheckTx{Code: 0}, nil
}

// FinalizeBlock processes all transactions in a block.
// CRITICAL: This method MUST be deterministic. No time.Now(), no map iteration without sorting,
// no goroutines, no external I/O except BadgerDB reads.
func (app *SageApp) FinalizeBlock(_ context.Context, req *abcitypes.RequestFinalizeBlock) (*abcitypes.ResponseFinalizeBlock, error) {
	start := time.Now()
	app.logger.Debug().Int64("height", req.Height).Int("txs", len(req.Txs)).Msg("finalizing block")

	// Clear pending writes from previous block
	app.pendingWrites = nil

	txResults := make([]*abcitypes.ExecTxResult, len(req.Txs))
	var processedMemoryIDs []string

	for i, rawTx := range req.Txs {
		parsedTx, err := tx.DecodeTx(rawTx)
		if err != nil {
			txResults[i] = &abcitypes.ExecTxResult{Code: 1, Log: err.Error()}
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

			if parsedTx.Type == tx.TxTypeMemorySubmit && parsedTx.MemorySubmit != nil {
				processedMemoryIDs = append(processedMemoryIDs, parsedTx.MemorySubmit.MemoryID)
			}
		}
	}

	// Governance post-processing: evaluate active proposal after ALL txs are processed.
	// This handles single-block auto-approve (proposal created + quorum in same block).
	var valUpdates []abcitypes.ValidatorUpdate
	executedProposal, govErr := app.govEngine.ProcessBlock(req.Height)
	if govErr != nil {
		app.logger.Error().Err(govErr).Msg("governance post-processing failed")
	}
	if executedProposal != nil {
		app.logger.Info().
			Str("proposal_id", executedProposal.ProposalID).
			Uint8("operation", uint8(executedProposal.Operation)).
			Str("target", executedProposal.TargetID).
			Msg("governance proposal executed")

		update, applyErr := app.applyGovernanceProposal(executedProposal, req.Height)
		if applyErr != nil {
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
		app.processEpoch(req.Height, req.Time)
	}

	// v7.5 upgrade plan activation. If a plan is pending and its
	// ActivationHeight matches this block, bump the chain's app
	// version via ConsensusParamUpdates and mark the plan applied.
	// CometBFT applies the new app version at H+1 across every node
	// atomically. Read-then-mark inside FinalizeBlock keeps the
	// transition deterministic across replicas.
	var consensusParamUpdates *cmtproto.ConsensusParams
	if plan, planErr := app.badgerStore.GetUpgradePlan(); planErr == nil && plan != nil && plan.ActivationHeight == req.Height {
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
	}

	// Update state
	app.state.Height = req.Height

	// Compute deterministic AppHash
	appHash, err := ComputeAppHash(app.badgerStore)
	if err != nil {
		app.logger.Error().Err(err).Msg("failed to compute app hash")
		appHash = computeBlockHash(processedMemoryIDs, req.Height)
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

	// Verify agent identity on-chain via embedded Ed25519 proof.
	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Domain write-access check: if the domain has a registered owner, verify the agent has write access.
	// If the domain doesn't exist, auto-register it with the submitting agent as owner.
	// Reserved shared domains (e.g. "general", "self") are writable by any authenticated agent
	// and are never auto-registered — they are conventional catch-alls without single-owner semantics.
	if submit.DomainTag != "" && !app.isSharedDomain(submit.DomainTag, height) {
		domainOwner, domainErr := app.badgerStore.GetDomainOwner(submit.DomainTag)
		if domainErr == nil && domainOwner != "" {
			// Domain is owned — check write access (level 2).
			postFork := app.postV8Fork(height)
			recordV8Branch(postFork)
			hasAccess, accessErr := app.badgerStore.HasAccessMultiOrg(submit.DomainTag, agentID, 0, blockTime, postFork)
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

	// Generate memory ID if not provided
	memoryID := submit.MemoryID
	if memoryID == "" {
		// Deterministic ID from content hash + height + agent (NO uuid.New()!)
		h := sha256.Sum256([]byte(fmt.Sprintf("%x:%d:%s", submit.ContentHash, height, agentID)))
		memoryID = hex.EncodeToString(h[:16])
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

	if setErr := app.badgerStore.SetMemoryHash(memoryID, contentHash, string(memory.StatusProposed)); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 12, Log: fmt.Sprintf("badger write error: %v", setErr)}
	}

	// v8.4: record the memory's domain on-chain so checkAndApplyQuorum can read
	// it deterministically at verdict time — the memory:<id> record stores only
	// contentHash+status, not the domain. Post-fork only and only for a non-empty
	// domain; the strict-> gate keeps pre-fork blocks and the activation block
	// byte-identical (no memdomain: key enters the AppHash keyspace until H_act+1).
	// Shared domains are recorded too — the quorum decides per-vote, via its own
	// height-aware isSharedDomain check, whether to use the domain-conditional
	// weight or fall back to the scalar.
	if submit.DomainTag != "" && app.postV8_4Fork(height) {
		if domErr := app.badgerStore.SetMemoryDomain(memoryID, submit.DomainTag); domErr != nil {
			app.logger.Error().Err(domErr).Str("memory_id", memoryID).Msg("v8.4 set memory domain")
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
	if app.postAppV10Fork(height) {
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
			if len(supp.EmbeddingHash) > 0 {
				record.EmbeddingHash = supp.EmbeddingHash
			}
			suppTriples = supp.KnowledgeTriples
		}
	}

	// Memory must be inserted before triples (FK constraint: knowledge_triples.memory_id → memories).
	app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "memory", data: record})

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
	if classErr := app.badgerStore.SetMemoryClassification(memoryID, classification); classErr != nil {
		app.logger.Error().Err(classErr).Str("memory_id", memoryID).Msg("failed to set memory classification")
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

	app.logger.Info().
		Str("memory_id", vote.MemoryID).
		Str("validator_id", validatorID[:16]).
		Str("decision", decision).
		Msg("processing vote")

	// Store vote on-chain
	voteKey := fmt.Sprintf("vote:%s:%s", vote.MemoryID, validatorID)
	if err := app.badgerStore.SetState(voteKey, []byte(decision)); err != nil {
		return &abcitypes.ExecTxResult{Code: 14, Log: fmt.Sprintf("badger write error: %v", err)}
	}

	// Increment on-chain validator vote stats for PoE scoring
	accepted := decision == "accept"
	uHeight := uint64(height) // #nosec G115 -- height is always non-negative
	if err := app.badgerStore.IncrementVoteStats(validatorID, accepted, uHeight, app.postV8_3Fork(height)); err != nil {
		app.logger.Error().Err(err).Str("validator", validatorID).Msg("failed to increment vote stats")
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
	var priorStatus string
	if creditVerdict {
		if _, st, err := app.badgerStore.GetMemoryHash(memoryID); err == nil {
			priorStatus = st
		}
	}

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
	if reached {
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
	} else if len(votes) >= len(validators) && len(validators) > 0 {
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

	// A challenge that passes BFT consensus (included in a block) is decisive —
	// the memory is deprecated immediately. The block inclusion IS the consensus.
	if err := app.badgerStore.SetMemoryHash(challenge.MemoryID, nil, string(memory.StatusDeprecated)); err != nil {
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
	postV10 := app.postAppV10Fork(height)
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

	// Validate level
	if req.RequestedLevel < 1 || req.RequestedLevel > 2 {
		return &abcitypes.ExecTxResult{Code: 31, Log: "invalid access level: must be 1 (read) or 2 (read+write)"}
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
	postFork := app.postV8Fork(height)
	recordV8Branch(postFork)
	if postFork {
		isOwner, _ := app.badgerStore.IsDomainOwnerOrAncestor(grant.Domain, granterID)
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
				return &abcitypes.ExecTxResult{Code: 34, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", granterID[:16], grant.Domain)}
			}
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
			// Fall through to level validation + the grantee's SetAccessGrant below.
		}
	} else {
		// Pre-fork (v7.1.1-byte-identical): strict ownership check.
		isOwner, err := app.badgerStore.IsDomainOwnerOrAncestor(grant.Domain, granterID)
		if err != nil || !isOwner {
			return &abcitypes.ExecTxResult{Code: 34, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", granterID[:16], grant.Domain)}
		}
	}

	// Validate level
	if grant.Level < 1 || grant.Level > 2 {
		return &abcitypes.ExecTxResult{Code: 35, Log: "invalid access level: must be 1 (read) or 2 (read+write)"}
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

	// Authorization: revoker must own the domain or ancestor
	isOwner, err := app.badgerStore.IsDomainOwnerOrAncestor(revoke.Domain, revokerID)
	if err != nil || !isOwner {
		return &abcitypes.ExecTxResult{Code: 38, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", revokerID[:16], revoke.Domain)}
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

func (app *SageApp) processEpoch(height int64, blockTime time.Time) {
	epochNum := poe.EpochNumber(height)
	app.state.EpochNum = epochNum
	app.logger.Info().Int64("epoch", epochNum).Int64("height", height).Msg("epoch boundary")
	metrics.EpochCurrent.Set(float64(epochNum))

	// Gather per-validator vote stats from BadgerDB (deterministic, on-chain data only)
	allStats, err := app.badgerStore.GetAllValidatorStats()
	if err != nil {
		app.logger.Error().Err(err).Msg("failed to get validator stats for epoch")
		return
	}

	validators := app.validators.GetAll()

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
	if len(app.pendingWrites) > 0 {
		writes := app.pendingWrites
		maxRetries := app.flushMaxRetries
		if maxRetries <= 0 {
			maxRetries = defaultFlushMaxRetries
		}
		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond
				if backoff > 5*time.Second {
					backoff = 5 * time.Second
				}
				app.logger.Warn().Int("attempt", attempt+1).Dur("backoff", backoff).Int("count", len(writes)).
					Msg("retrying offchain flush after SQLITE_BUSY")
				time.Sleep(backoff)
			}
			lastErr = app.offchainStore.RunInTx(ctx, func(tx store.OffchainStore) error {
				return app.flushPendingWrites(ctx, tx, writes)
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
			app.logger.Error().Err(lastErr).Int("count", len(writes)).Int("attempts", maxRetries).
				Msg("CRITICAL: atomic flush of pending writes failed — halting node to preserve on-chain/offchain consistency")
			panic(fmt.Sprintf(
				"sage: offchain flush failed after %d attempts (%d writes pending): %v — "+
					"consensus cannot advance without offchain commit; fix DB contention and restart to replay this block",
				maxRetries, len(writes), lastErr,
			))
		}
	}
	app.pendingWrites = nil

	// Save state to BadgerDB only after the offchain flush has succeeded.
	if err := SaveState(app.badgerStore, app.state); err != nil {
		app.logger.Error().Err(err).Msg("failed to save state")
	}

	// v7.5 snapshot scheduler: post-SaveState is the only point where
	// BadgerDB + SQLite + CometBFT-state-as-of-this-height are mutually
	// consistent. Tick is fast (mutex + decision); any actual disk work
	// runs on its own goroutine so Commit returns promptly.
	if app.snapshotScheduler != nil {
		app.snapshotScheduler.Tick(app.state.Height, app.state.AppHash)
	}

	return &abcitypes.ResponseCommit{}, nil
}

// flushPendingWrites executes all buffered writes against the given store (which
// may be a transaction-scoped store). Returns the first error encountered,
// causing the wrapping transaction to roll back.
func (app *SageApp) flushPendingWrites(ctx context.Context, s store.OffchainStore, writes []pendingWrite) error {
	for _, pw := range writes {
		var err error
		switch pw.writeType {
		case "memory":
			if record, ok := pw.data.(*memory.MemoryRecord); ok {
				err = s.InsertMemory(ctx, record)
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
				if writeErr := s.UpdateStatus(ctx, su.MemoryID, su.Status, su.At); writeErr != nil {
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
				// Try to create; if it already exists (idempotent), update instead
				createErr := s.CreateAgent(ctx, agent)
				if createErr != nil {
					// Agent may already exist from direct SQLite write — update it
					err = s.UpdateAgent(ctx, agent)
				}
			}
		case "agent_update":
			if d, ok := pw.data.(*agentUpdateData); ok {
				existing, getErr := s.GetAgent(ctx, d.AgentID)
				if getErr == nil {
					existing.Name = d.Name
					existing.BootBio = d.BootBio
					err = s.UpdateAgent(ctx, existing)
				}
			}
		case "agent_permission":
			if d, ok := pw.data.(*agentPermissionData); ok {
				existing, getErr := s.GetAgent(ctx, d.AgentID)
				if getErr == nil {
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

// PrepareProposal prepares a block proposal (pass-through in Phase 1).
func (app *SageApp) PrepareProposal(_ context.Context, req *abcitypes.RequestPrepareProposal) (*abcitypes.ResponsePrepareProposal, error) {
	return &abcitypes.ResponsePrepareProposal{Txs: req.Txs}, nil
}

// ProcessProposal validates a block proposal (pass-through in Phase 1).
func (app *SageApp) ProcessProposal(_ context.Context, req *abcitypes.RequestProcessProposal) (*abcitypes.ResponseProcessProposal, error) {
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

// ListSnapshots is not used in Phase 1.
func (app *SageApp) ListSnapshots(_ context.Context, req *abcitypes.RequestListSnapshots) (*abcitypes.ResponseListSnapshots, error) {
	return &abcitypes.ResponseListSnapshots{}, nil
}

// OfferSnapshot is not used in Phase 1.
func (app *SageApp) OfferSnapshot(_ context.Context, req *abcitypes.RequestOfferSnapshot) (*abcitypes.ResponseOfferSnapshot, error) {
	return &abcitypes.ResponseOfferSnapshot{Result: abcitypes.ResponseOfferSnapshot_REJECT}, nil
}

// LoadSnapshotChunk is not used in Phase 1.
func (app *SageApp) LoadSnapshotChunk(_ context.Context, req *abcitypes.RequestLoadSnapshotChunk) (*abcitypes.ResponseLoadSnapshotChunk, error) {
	return &abcitypes.ResponseLoadSnapshotChunk{}, nil
}

// ApplySnapshotChunk is not used in Phase 1.
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
	if err := app.badgerStore.CloseBadger(); err != nil {
		return err
	}
	return app.offchainStore.Close()
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

// processGovPropose handles a TxTypeGovPropose transaction.
func (app *SageApp) processGovPropose(parsedTx *tx.ParsedTx, height int64, _ time.Time) *abcitypes.ExecTxResult {
	if parsedTx.GovPropose == nil {
		return &abcitypes.ExecTxResult{Code: 70, Log: "missing governance propose payload"}
	}

	proposerID := auth.PublicKeyToAgentID(parsedTx.PublicKey)

	// Verify proposer is an admin-role agent.
	agent, err := app.badgerStore.GetRegisteredAgent(proposerID)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 71, Log: "proposer not registered: " + err.Error()}
	}
	if agent.Role != "admin" {
		return &abcitypes.ExecTxResult{Code: 72, Log: "only admin agents can propose governance changes"}
	}

	gp := parsedTx.GovPropose
	op := governance.ProposalOp(gp.Operation)

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
			Operation:     opToString(op),
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

	if err := app.govEngine.Cancel(gc.ProposalID, cancellerID, height); err != nil {
		return &abcitypes.ExecTxResult{Code: 78, Log: "governance cancel failed: " + err.Error()}
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
}

// applyUpgradeProposal persists the pending UpgradePlanRecord for an executed
// (2/3-accepted) app-v8 OpUpgrade proposal. Runs in FinalizeBlock's governance
// post-processing (before the activation block), so the plan's ActivationHeight
// (= height + delay, delay >= 200) is always in the future and never activates
// in the same block. Deterministic on every replica: it reads only consensus
// state (the proposal payload, currentAppVersion, the pending-plan slot).
func (app *SageApp) applyUpgradeProposal(proposal *governance.ProposalState, height int64) error {
	var p UpgradeProposalPayload
	if err := json.Unmarshal(proposal.Payload, &p); err != nil {
		// Should never happen — we marshalled this payload ourselves at propose
		// time. Log and skip rather than fail the block (deterministic skip:
		// every replica sees the same bytes and the same error).
		app.logger.Error().Err(err).Str("proposal_id", proposal.ProposalID).Msg("app-v8: cannot decode upgrade proposal payload; skipping activation")
		return nil
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
		return nil
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
		return nil
	}

	// At-most-one-pending-plan re-guard at EXECUTE time. The propose-time check
	// ran when no plan was pending, but the gov:active singleton only serialises
	// PROPOSALS — a plan persisted by a prior approved upgrade (awaiting its
	// ActivationHeight) is a separate slot. Never overwrite it; SetUpgradePlan
	// would clobber the single 'upgrade:plan' key silently.
	if existing, getErr := app.badgerStore.GetUpgradePlan(); getErr == nil && existing != nil {
		app.logger.Warn().
			Str("name", p.Name).
			Str("pending_plan", existing.Name).
			Int64("pending_activation_height", existing.ActivationHeight).
			Msg("app-v8: a plan is already pending; skipping persist of the newly-approved upgrade")
		return nil
	}

	delay := p.UpgradeDelayBlocks
	if delay < defaultUpgradeDelayBlocks {
		delay = defaultUpgradeDelayBlocks
	}
	rec := &store.UpgradePlanRecord{
		Name:             p.Name,
		TargetAppVersion: p.TargetAppVersion,
		ActivationHeight: height + delay,
		BinarySHA256:     p.BinarySHA256,
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
	if postV8 {
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

		payload, mErr := json.Marshal(UpgradeProposalPayload{
			Name:               prop.Name,
			TargetAppVersion:   prop.TargetAppVersion,
			BinarySHA256:       prop.BinarySHA256,
			UpgradeDelayBlocks: prop.UpgradeDelayBlocks,
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
	if delay < defaultUpgradeDelayBlocks {
		delay = defaultUpgradeDelayBlocks
	}
	rec := &store.UpgradePlanRecord{
		Name:             prop.Name,
		TargetAppVersion: prop.TargetAppVersion,
		ActivationHeight: height + delay,
		BinarySHA256:     prop.BinarySHA256,
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
		app.persistValidators()

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

		app.persistValidators()

		app.logger.Info().
			Str("validator", proposal.TargetID).
			Int64("height", height).
			Msg("validator removed via governance")

		return &abcitypes.ValidatorUpdate{PubKey: protoKey, Power: 0}, nil

	case governance.OpUpdatePower:
		if err := app.validators.UpdatePower(proposal.TargetID, proposal.TargetPower); err != nil {
			return nil, fmt.Errorf("update power: %w", err)
		}

		app.persistValidators()

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

// persistValidators saves the current validator set to BadgerDB.
func (app *SageApp) persistValidators() {
	valMap := make(map[string]int64)
	for _, v := range app.validators.GetAll() {
		valMap[v.ID] = v.Power
	}
	if err := app.badgerStore.SaveValidators(valMap); err != nil {
		app.logger.Error().Err(err).Msg("failed to persist validators after governance change")
	}
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
	default:
		return fmt.Sprintf("unknown_%d", op)
	}
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
