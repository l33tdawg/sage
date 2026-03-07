package abci

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

// pendingWrite represents a PostgreSQL write buffered until Commit.
type pendingWrite struct {
	writeType string // "memory", "challenge", "vote", "corroborate", "epoch_score", "validator_score", "status_update", "access_grant", "access_request", "access_revoke", "access_log", "domain_register", "access_request_status", "org_register", "org_member", "org_member_remove", "org_member_clearance", "federation", "federation_approve", "federation_revoke", "mem_classification", "dept_register", "dept_member", "dept_member_remove"
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

// memClassificationData carries the fields needed to update a memory's classification in PostgreSQL.
type memClassificationData struct {
	MemoryID       string
	Classification store.ClearanceLevel
}

// SageApp implements the CometBFT ABCI 2.0 Application interface.
type SageApp struct {
	badgerStore   *store.BadgerStore
	offchainStore store.OffchainStore
	validators    *validator.ValidatorSet
	poeEngine     *poe.DomainRegistry
	phiTracker    *poe.PhiTracker
	state         *AppState
	logger        zerolog.Logger

	// Buffered writes — only flushed to PostgreSQL in Commit
	pendingWrites []pendingWrite
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

	app := &SageApp{
		badgerStore:   bs,
		offchainStore: ps,
		validators:    validator.NewValidatorSet(),
		poeEngine:     domainReg,
		phiTracker:    poe.NewPhiTracker(50),
		state:         state,
		logger:        logger.With().Str("component", "abci").Logger(),
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

	app := &SageApp{
		badgerStore:   bs,
		offchainStore: offchain,
		validators:    validator.NewValidatorSet(),
		poeEngine:     domainReg,
		phiTracker:    poe.NewPhiTracker(50),
		state:         state,
		logger:        logger.With().Str("component", "abci").Logger(),
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
	return &abcitypes.ResponseInfo{
		Data:             "sage",
		Version:          "1.0.0",
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
		TxResults: txResults,
		AppHash:   appHash,
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
	if submit.DomainTag != "" {
		domainOwner, domainErr := app.badgerStore.GetDomainOwner(submit.DomainTag)
		if domainErr == nil && domainOwner != "" {
			// Domain is owned — check write access (level 2).
			hasAccess, accessErr := app.badgerStore.HasAccessMultiOrg(submit.DomainTag, agentID, 0, blockTime)
			if accessErr != nil || !hasAccess {
				return &abcitypes.ExecTxResult{Code: 11, Log: fmt.Sprintf("access denied: agent %s has no write access to domain %s", agentID[:16], submit.DomainTag)}
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

	// Buffer PostgreSQL write for Commit
	record := &memory.MemoryRecord{
		MemoryID:        memoryID,
		SubmittingAgent: agentID,
		Content:         submit.Content,
		ContentHash:     contentHash,
		MemoryType:      memory.MemoryType(memType),
		DomainTag:       submit.DomainTag,
		ConfidenceScore: submit.ConfidenceScore,
		Status:          memory.StatusProposed,
		CreatedAt:       blockTime,
	}
	app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "memory", data: record})

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

	// Update on-chain status
	if err := app.badgerStore.SetMemoryHash(challenge.MemoryID, nil, string(memory.StatusChallenged)); err != nil {
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

	// Buffer status update for the memory.
	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "status_update",
		data: &statusUpdate{
			MemoryID: challenge.MemoryID,
			Status:   memory.StatusChallenged,
			At:       blockTime,
		},
	})

	metrics.ChallengesTotal.Inc()
	return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("challenge submitted for memory %s by %s", challenge.MemoryID, challengerID[:16])}
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
	return auth.VerifyAgentProof(parsedTx.AgentPubKey, parsedTx.AgentSig, parsedTx.AgentBodyHash, parsedTx.AgentTimestamp)
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
	if len(app.pendingWrites) > 0 {
		writes := app.pendingWrites
		if err := app.offchainStore.RunInTx(ctx, func(tx store.OffchainStore) error {
			return app.flushPendingWrites(ctx, tx, writes)
		}); err != nil {
			app.logger.Error().Err(err).Int("count", len(writes)).
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
