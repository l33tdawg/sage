package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/auth"
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
	ProposalID  string
	Operation   string
	TargetID    string
	TargetPower int64
	ProposerID  string
	Status      string
	CreatedHeight int64
	ExpiryHeight  int64
	Reason      string
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
	ProposalID    string
	Status        string
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
	poeEngine     *poe.DomainRegistry
	phiTracker    *poe.PhiTracker
	govEngine     *governance.Engine
	state         *AppState
	logger        zerolog.Logger
	Version       string

	// Buffered writes — only flushed to PostgreSQL in Commit
	pendingWrites []pendingWrite

	// SuppCache bridges REST→ABCI for off-chain data (embeddings, triples, provider).
	// Nil in standalone ABCI mode without co-located REST API.
	SuppCache *SupplementaryCache
}

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

	// Initialize domain registry with default domains
	domains := []string{"crypto", "vuln_intel", "challenge_generation", "solver_feedback", "calibration", "infrastructure"}
	domainReg := poe.NewDomainRegistry(domains)

	valSet := validator.NewValidatorSet()

	app := &SageApp{
		badgerStore:   bs,
		offchainStore: ps,
		validators:    valSet,
		poeEngine:     domainReg,
		phiTracker:    poe.NewPhiTracker(50),
		govEngine:     governance.NewEngine(bs, &validatorSetAdapter{vs: valSet}),
		state:         state,
		logger:        logger.With().Str("component", "abci").Logger(),
		SuppCache:     NewSupplementaryCache(),
	}

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

	return app, nil
}

// NewSageAppWithStores creates a SAGE ABCI application with pre-created stores.
// This allows plugging in any OffchainStore implementation (PostgresStore, SQLiteStore, etc.).
func NewSageAppWithStores(bs *store.BadgerStore, offchain store.OffchainStore, logger zerolog.Logger) (*SageApp, error) {
	state, err := LoadState(bs)
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	domains := []string{"crypto", "vuln_intel", "challenge_generation", "solver_feedback", "calibration", "infrastructure"}
	domainReg := poe.NewDomainRegistry(domains)

	valSet := validator.NewValidatorSet()

	app := &SageApp{
		badgerStore:   bs,
		offchainStore: offchain,
		validators:    valSet,
		poeEngine:     domainReg,
		phiTracker:    poe.NewPhiTracker(50),
		govEngine:     governance.NewEngine(bs, &validatorSetAdapter{vs: valSet}),
		state:         state,
		logger:        logger.With().Str("component", "abci").Logger(),
		SuppCache:     NewSupplementaryCache(),
	}

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

	return app, nil
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
		AppVersion:       1,
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
	if parsedTx.Nonce <= currentNonce && currentNonce > 0 {
		metrics.TxRejectedTotal.WithLabelValues("replay_nonce").Inc()
		return &abcitypes.ResponseCheckTx{Code: 4, Log: fmt.Sprintf("nonce too low: got %d, expected > %d", parsedTx.Nonce, currentNonce)}, nil
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
		TxResults:        txResults,
		AppHash:          appHash,
		ValidatorUpdates: valUpdates,
	}, nil
}

// processTx handles a single transaction deterministically.
func (app *SageApp) processTx(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
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
	default:
		return &abcitypes.ExecTxResult{Code: 10, Log: "unknown tx type"}
	}
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
	if submit.DomainTag != "" {
		domainOwner, domainErr := app.badgerStore.GetDomainOwner(submit.DomainTag)
		if domainErr == nil && domainOwner != "" {
			// Domain is owned — check write access (level 2).
			hasAccess, accessErr := app.badgerStore.HasAccessMultiOrg(submit.DomainTag, agentID, 0, blockTime)
			if accessErr != nil || !hasAccess {
				return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("access denied: agent %s has no write access to domain %s", agentID[:16], submit.DomainTag)}
			}
		} else {
			// Domain not registered — auto-register with submitting agent as owner
			if regErr := app.badgerStore.RegisterDomain(submit.DomainTag, agentID, "", height); regErr != nil {
				app.logger.Error().Err(regErr).Str("domain", submit.DomainTag).Msg("failed to auto-register domain")
			} else {
				app.logger.Info().Str("domain", submit.DomainTag).Str("owner", agentID[:16]).Msg("auto-registered domain on first memory submit")
				// Also grant the owner full access
				if grantErr := app.badgerStore.SetAccessGrant(submit.DomainTag, agentID, 2, 0, agentID); grantErr != nil {
					app.logger.Error().Err(grantErr).Str("domain", submit.DomainTag).Msg("failed to auto-grant owner access")
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

	if setErr := app.badgerStore.SetMemoryHash(memoryID, contentHash, string(memory.StatusProposed)); setErr != nil {
		return &abcitypes.ExecTxResult{Code: 12, Log: fmt.Sprintf("badger write error: %v", setErr)}
	}

	memType := txMemoryTypeToString(submit.MemoryType)

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

	// Store memory classification on-chain
	classification := uint8(submit.Classification)
	if classification == 0 {
		classification = uint8(tx.ClearanceInternal) // Default to INTERNAL
	}
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
	if err := app.badgerStore.IncrementVoteStats(validatorID, accepted, uHeight); err != nil {
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

	app.logger.Debug().
		Str("memory_id", memoryID).
		Int("num_validators", len(validators)).
		Msg("checking quorum")

	for _, v := range validators {
		weights[v.ID] = 1.0 // Phase 1: equal weights
		voteKey := fmt.Sprintf("vote:%s:%s", memoryID, v.ID)
		voteData, err := app.badgerStore.GetState(voteKey)
		if err == nil && voteData != nil {
			votes[v.ID] = string(voteData) == "accept"
			app.logger.Debug().
				Str("memory_id", memoryID).
				Str("validator", v.ID[:16]).
				Str("decision", string(voteData)).
				Msg("found vote")
		}
	}

	app.logger.Debug().
		Str("memory_id", memoryID).
		Int("votes_found", len(votes)).
		Int("validators", len(validators)).
		Msg("quorum check votes gathered")

	reached, acceptWeight, totalWeight := validator.CheckQuorum(votes, weights)
	if reached {
		// Transition to committed on-chain (BadgerDB)
		if err := app.badgerStore.SetMemoryHash(memoryID, nil, string(memory.StatusCommitted)); err == nil {
			app.logger.Info().
				Str("memory_id", memoryID).
				Int64("height", height).
				Float64("accept_weight", acceptWeight).
				Float64("total_weight", totalWeight).
				Msg("memory committed by quorum")
		}

		// Buffer PostgreSQL status update — flushes in Commit
		app.pendingWrites = append(app.pendingWrites, pendingWrite{
			writeType: "status_update",
			data: &statusUpdate{
				MemoryID: memoryID,
				Status:   memory.StatusCommitted,
				At:       blockTime,
			},
		})
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

	// Authorization: granter must own the domain or be ancestor domain owner
	isOwner, err := app.badgerStore.IsDomainOwnerOrAncestor(grant.Domain, granterID)
	if err != nil || !isOwner {
		return &abcitypes.ExecTxResult{Code: 34, Log: fmt.Sprintf("access denied: %s is not owner of domain %s", granterID[:16], grant.Domain)}
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
	hasAccess, err := app.badgerStore.HasAccessMultiOrg(query.Domain, agentID, 0, blockTime)
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

	// Verify the sender actually belongs to the proposer org and is admin.
	senderOrg, orgErr := app.badgerStore.GetAgentOrg(senderID)
	if orgErr != nil {
		return &abcitypes.ExecTxResult{Code: 61, Log: fmt.Sprintf("agent %s is not in any organization", senderID[:16])}
	}
	if senderOrg != prop.ProposerOrgID {
		return &abcitypes.ExecTxResult{Code: 61, Log: fmt.Sprintf("agent org %s does not match proposer org %s", senderOrg, prop.ProposerOrgID)}
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
			ExpiresAt: expiresAt, RequiresApproval: prop.RequiresApproval,
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

	// Verify the sender's org matches the target org and they're admin.
	senderOrg, orgErr := app.badgerStore.GetAgentOrg(senderID)
	if orgErr != nil {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("agent %s is not in any organization", senderID[:16])}
	}
	if senderOrg != targetOrg {
		return &abcitypes.ExecTxResult{Code: 64, Log: fmt.Sprintf("approver org %s does not match target org %s", senderOrg, targetOrg)}
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

	// Verify the sender's org is one of the federated orgs and they're admin.
	revokerOrg, orgErr := app.badgerStore.GetAgentOrg(senderID)
	if orgErr != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("agent %s is not in any organization", senderID[:16])}
	}
	if revokerOrg != proposerOrg && revokerOrg != targetOrg {
		return &abcitypes.ExecTxResult{Code: 66, Log: "only admins of either org can revoke federations"}
	}
	if !app.isOrgAdmin(revokerOrg, senderID) {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("access denied: %s is not admin of org %s", senderID[:16], revokerOrg)}
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

	// Idempotent: if already registered, still buffer offchain write to backfill on_chain_height
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

func (app *SageApp) processAgentSetPermission(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) *abcitypes.ExecTxResult {
	perm := parsedTx.AgentSetPermission
	if perm == nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: "missing agent set permission payload"}
	}

	// Verify sender is admin — only admins can set permissions on other agents.
	senderID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return &abcitypes.ExecTxResult{Code: 66, Log: fmt.Sprintf("agent identity verification failed: %v", err)}
	}

	// Check sender is registered and is an admin
	senderAgent, senderErr := app.badgerStore.GetRegisteredAgent(senderID)
	if senderErr != nil {
		return &abcitypes.ExecTxResult{Code: 67, Log: fmt.Sprintf("sender agent %s not registered", senderID[:16])}
	}
	if senderAgent.Role != "admin" {
		return &abcitypes.ExecTxResult{Code: 67, Log: fmt.Sprintf("access denied: %s is not an admin", senderID[:16])}
	}

	// Target agent must be registered
	if !app.badgerStore.IsAgentRegistered(perm.AgentID) {
		return &abcitypes.ExecTxResult{Code: 68, Log: fmt.Sprintf("target agent %s not registered", perm.AgentID[:16])}
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

	senderAgent, senderErr := app.badgerStore.GetRegisteredAgent(senderID)
	if senderErr != nil {
		return &abcitypes.ExecTxResult{Code: 67, Log: fmt.Sprintf("sender agent %s not registered", senderID[:16])}
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

	// Compute raw PoE weights for each validator
	rawWeights := make(map[string]float64, len(validators))
	epochDetails := make(map[string]*store.EpochScore, len(validators))

	for _, v := range validators {
		stats := allStats[v.ID]

		// Accuracy: accept ratio with cold-start blending (EWMA simplified for Phase 1)
		var accuracy float64
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

		// Domain: Phase 1 uses default 0.5 (no per-domain tracking yet)
		domainScore := 0.5

		// Recency: exp(-lambda * hours_since_last_vote)
		var recencyScore float64
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

		// Corroboration: Phase 1 uses default (no per-validator corroboration count yet)
		corrScore := poe.CorroborationScore(0, poe.CorrMax)

		// Compute PoE weight
		weight := poe.ComputeWeight(accuracy, domainScore, recencyScore, corrScore)
		rawWeights[v.ID] = weight

		epochDetails[v.ID] = &store.EpochScore{
			EpochNum:    epochNum,
			BlockHeight: height,
			ValidatorID: v.ID,
			Accuracy:    accuracy,
			DomainScore: domainScore,
			RecencyScore: recencyScore,
			CorrScore:   corrScore,
			RawWeight:   weight,
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

	// Normalize weights with rep cap
	normalized := poe.NormalizeWeights(rawWeights)

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

		// Buffer validator score update for Commit
		stats := allStats[v.ID]
		var voteCount int64
		var weightedSum, weightDenom float64
		if stats != nil {
			voteCount = int64(stats.TotalVotes) // #nosec G115 -- vote count fits in int64
			weightedSum = float64(stats.AcceptVotes)
			weightDenom = float64(stats.TotalVotes)
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
}

// Commit persists finalized state.
// THIS is where PostgreSQL writes happen — never in FinalizeBlock.
func (app *SageApp) Commit(_ context.Context, req *abcitypes.RequestCommit) (*abcitypes.ResponseCommit, error) {
	ctx := context.Background()

	// Save state to BadgerDB
	if err := SaveState(app.badgerStore, app.state); err != nil {
		app.logger.Error().Err(err).Msg("failed to save state")
	}

	// Flush pending writes to the offchain store atomically within a single
	// database transaction. If any write fails, the entire batch rolls back,
	// preventing partial state divergence between BadgerDB and the query layer.
	//
	// Retry with exponential backoff on SQLITE_BUSY — consensus already succeeded
	// so these writes MUST eventually land in the offchain store.
	if len(app.pendingWrites) > 0 {
		writes := app.pendingWrites
		const maxRetries = 5
		var lastErr error
		for attempt := 0; attempt < maxRetries; attempt++ {
			if attempt > 0 {
				backoff := time.Duration(1<<uint(attempt-1)) * 100 * time.Millisecond // 100ms, 200ms, 400ms, 800ms
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
			// Only retry on SQLITE_BUSY; other errors are not transient.
			if !strings.Contains(lastErr.Error(), "SQLITE_BUSY") &&
				!strings.Contains(lastErr.Error(), "database is locked") {
				break
			}
		}
		if lastErr != nil {
			app.logger.Error().Err(lastErr).Int("count", len(writes)).Int("attempts", maxRetries).
				Msg("CRITICAL: atomic flush of pending writes failed — offchain store may be behind on-chain state")
		}
	}
	app.pendingWrites = nil

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

	proposalID, propErr := app.govEngine.Propose(
		proposerID, op, gp.TargetID, gp.TargetPubKey,
		gp.TargetPower, gp.ExpiryBlocks, gp.Reason, height,
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

// applyGovernanceProposal applies an executed governance proposal to the validator set
// and returns a CometBFT ValidatorUpdate. Called from FinalizeBlock post-processing.
func (app *SageApp) applyGovernanceProposal(proposal *governance.ProposalState, height int64) (*abcitypes.ValidatorUpdate, error) {
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
