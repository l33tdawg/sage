package abci

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// setScopedMemorySubmission writes the app-v20 supplementary state when the
// submitted exact domain belongs to an active scope. The boolean is false for
// an ordinary unscoped domain, preserving the historical submit path.
func (app *SageApp) setScopedMemorySubmission(submit *tx.MemorySubmit, memoryID, agentID string, contentHash []byte, height int64, blockTime time.Time) (bool, error) {
	record, err := app.badgerStore.GetScopeForDomain(submit.DomainTag)
	if err != nil {
		return false, err
	}
	if record == nil {
		return false, nil
	}
	if record.State != scope.StateActive {
		return false, fmt.Errorf("scope %q is not active", record.ScopeID)
	}

	members := make([]scope.BallotMember, 0, len(record.Members))
	var total uint64
	for _, member := range record.Members {
		if !member.Active {
			continue
		}
		validatorInfo, ok := app.validators.GetValidator(member.ValidatorID)
		if !ok || validatorInfo.Power <= 0 {
			return false, fmt.Errorf("active scope member %q is not an active on-chain validator", member.ValidatorID)
		}
		if math.MaxUint64-total < member.AssignedWeight {
			return false, errors.New("active scope weight total overflows uint64")
		}
		total += member.AssignedWeight
		members = append(members, scope.BallotMember{
			ValidatorID:     member.ValidatorID,
			EffectiveWeight: member.AssignedWeight,
		})
	}
	if len(members) == 0 || total == 0 {
		return false, errors.New("scope has no active weighted members")
	}

	// The canonical mirror must prove the ordinary memory hash. Supplying a raw
	// ContentHash that does not hash the submitted content is rejected for a
	// scoped memory rather than making recovery attest an inconsistent envelope.
	wantHash := sha256.Sum256([]byte(submit.Content))
	if !bytes.Equal(contentHash, wantHash[:]) {
		return false, errors.New("content_hash does not match canonical content")
	}

	ballot := scope.Ballot{
		MemoryID:        memoryID,
		ScopeID:         record.ScopeID,
		ScopeRevision:   record.Revision,
		SubmittedHeight: height,
		State:           scope.BallotPending,
		Members:         members,
		TotalWeight:     total,
	}
	content := scope.Content{
		MemoryID:          memoryID,
		ScopeID:           record.ScopeID,
		ScopeRevision:     record.Revision,
		SubmittingAgentID: agentID,
		ContentHash:       append([]byte(nil), contentHash...),
		MemoryType:        byte(submit.MemoryType),
		Domain:            submit.DomainTag,
		ConfidenceScore:   submit.ConfidenceScore,
		Content:           submit.Content,
		ParentHash:        submit.ParentHash,
		Classification:    byte(submit.Classification),
		TaskStatus:        submit.TaskStatus,
		SubmittedHeight:   height,
		SubmittedUnix:     blockTime.Unix(),
	}
	if err := app.badgerStore.SetScopedMemorySubmission(ballot, content); err != nil {
		return false, err
	}
	return true, nil
}

// replayScopedFinalizeTx recognizes only an exact app-v20 scoped transaction
// whose Badger effects already exist at the same height while persisted
// state.Height is still behind. That state can arise when FinalizeBlock
// succeeded but Commit crashed before its SQL flush/SaveState boundary. The
// canonical envelope or immutable vote-height record is the replay witness.
//
// The function performs no consensus write. It reconstructs the projection
// batch that the failed Commit lost and returns the original successful result.
// A later block, changed payload/decision, ordinary unscoped transaction, or
// merely reused nonce cannot satisfy the height-bound evidence.
func (app *SageApp) replayScopedFinalizeTx(parsedTx *tx.ParsedTx, height int64, blockTime time.Time) (*abcitypes.ExecTxResult, bool) {
	if app.state == nil || app.state.Height >= height || !app.postAppV20Fork(height) {
		return nil, false
	}

	switch parsedTx.Type {
	case tx.TxTypeMemorySubmit:
		submit := parsedTx.MemorySubmit
		if submit == nil {
			return nil, false
		}
		submittingAgent, err := verifyAgentIdentity(parsedTx)
		if err != nil {
			return nil, false
		}
		memoryID := memoryIDForSubmit(submit, height, submittingAgent)
		content, err := app.badgerStore.GetScopedContent(memoryID)
		if err != nil || content == nil {
			return nil, false
		}
		effectiveHash := submit.ContentHash
		if len(effectiveHash) == 0 {
			computed := sha256.Sum256([]byte(submit.Content))
			effectiveHash = computed[:]
		}
		if content.MemoryID != memoryID || content.SubmittingAgentID != submittingAgent ||
			content.SubmittedHeight != height || content.SubmittedUnix != blockTime.Unix() ||
			content.Domain != submit.DomainTag || content.Content != submit.Content ||
			content.MemoryType != byte(submit.MemoryType) ||
			content.Classification != byte(submit.Classification) ||
			content.TaskStatus != submit.TaskStatus ||
			math.Float64bits(content.ConfidenceScore) != math.Float64bits(submit.ConfidenceScore) ||
			!bytes.Equal(content.ContentHash, effectiveHash) ||
			content.ParentHash != submit.ParentHash {
			return nil, false
		}
		onChainHash, status, err := app.badgerStore.GetMemoryHash(memoryID)
		if err != nil || !bytes.Equal(onChainHash, content.ContentHash) {
			return nil, false
		}
		ballot, err := app.badgerStore.GetScopeBallot(memoryID)
		if err != nil || ballot == nil || ballot.SubmittedHeight != height ||
			ballot.MemoryID != memoryID || ballot.ScopeID != content.ScopeID ||
			ballot.ScopeRevision != content.ScopeRevision {
			return nil, false
		}
		if err := validateRecoveredScopeStatus(ballot.State, status); err != nil {
			return nil, false
		}

		app.pendingWrites = append(app.pendingWrites,
			pendingWrite{writeType: "memory", data: &memory.MemoryRecord{
				MemoryID: memoryID, SubmittingAgent: submittingAgent,
				Content: submit.Content, ContentHash: append([]byte(nil), content.ContentHash...),
				EmbeddingHash: append([]byte(nil), submit.EmbeddingHash...),
				MemoryType:    memory.MemoryType(txMemoryTypeToStringValue(content.MemoryType)),
				DomainTag:     content.Domain, ConfidenceScore: content.ConfidenceScore,
				Status: memory.MemoryStatus(status), ParentHash: content.ParentHash,
				TaskStatus: memory.TaskStatus(content.TaskStatus), CreatedAt: blockTime,
			}},
			pendingWrite{writeType: "mem_classification", data: &memClassificationData{
				MemoryID: memoryID, Classification: store.ClearanceLevel(content.Classification),
			}},
		)
		return &abcitypes.ExecTxResult{
			Code: 0, Data: []byte(memoryID), Log: fmt.Sprintf("memory %s submitted", memoryID),
		}, true

	case tx.TxTypeMemoryVote:
		vote := parsedTx.MemoryVote
		if vote == nil {
			return nil, false
		}
		validatorID := auth.PublicKeyToAgentID(parsedTx.PublicKey)
		decision := voteDecisionToString(vote.Decision)
		storedDecision, storedHeight, ok, err := app.badgerStore.GetScopedVote(vote.MemoryID, validatorID)
		if err != nil || !ok || storedHeight != height || storedDecision != decision {
			return nil, false
		}
		ballot, err := app.badgerStore.GetScopeBallot(vote.MemoryID)
		if err != nil || ballot == nil || !scopeBallotHasMember(*ballot, validatorID) {
			return nil, false
		}
		app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "vote", data: &store.ValidationVote{
			MemoryID: vote.MemoryID, ValidatorID: validatorID, Decision: decision,
			Rationale: vote.Rationale, BlockHeight: height, CreatedAt: blockTime,
		}})
		if ballot.State == scope.BallotCommitted || ballot.State == scope.BallotDeprecated {
			status := memory.StatusDeprecated
			if ballot.State == scope.BallotCommitted {
				status = memory.StatusCommitted
			}
			app.pendingWrites = append(app.pendingWrites, pendingWrite{writeType: "status_update", data: &statusUpdate{
				MemoryID: vote.MemoryID, Status: status, At: blockTime,
			}})
		}
		return &abcitypes.ExecTxResult{Code: 0, Log: fmt.Sprintf("vote recorded for memory %s", vote.MemoryID)}, true
	}

	return nil, false
}

func scopeBallotHasMember(ballot scope.Ballot, validatorID string) bool {
	for _, member := range ballot.Members {
		if member.ValidatorID == validatorID {
			return true
		}
	}
	return false
}

// scopedLifecycleState returns the verified canonical state needed by later
// challenge/recovery transitions. A scoped terminal transition must preserve
// this hash even though the historical unscoped path stores a nil hash.
func (app *SageApp) scopedLifecycleState(memoryID string, height int64) (*scope.Ballot, []byte, error) {
	if !app.postAppV20Fork(height) {
		return nil, nil, nil
	}
	ballot, err := app.badgerStore.GetScopeBallot(memoryID)
	if err != nil || ballot == nil {
		return ballot, nil, err
	}
	content, err := app.badgerStore.GetScopedContent(memoryID)
	if err != nil {
		return nil, nil, err
	}
	if content == nil || content.MemoryID != ballot.MemoryID || content.ScopeID != ballot.ScopeID || content.ScopeRevision != ballot.ScopeRevision {
		return nil, nil, errors.New("scope ballot has no matching canonical content")
	}
	computed := sha256.Sum256([]byte(content.Content))
	if !bytes.Equal(computed[:], content.ContentHash) {
		return nil, nil, errors.New("scoped canonical content hash is invalid")
	}
	return ballot, append([]byte(nil), content.ContentHash...), nil
}

func (app *SageApp) rejectScopedCoCommit(domain string, height int64) error {
	if !app.postAppV20Fork(height) || domain == "" {
		return nil
	}
	record, err := app.badgerStore.GetScopeForDomain(domain)
	if err != nil {
		return err
	}
	if record != nil {
		return fmt.Errorf("domain %q belongs to scope %q; co-commit is unsupported for scoped domains", domain, record.ScopeID)
	}
	return nil
}

// checkAndApplyScopedQuorum evaluates only the pinned ballot snapshot. The v1
// effective weight is the governance-assigned integer copied verbatim at
// submission; live PoE and later scope revisions cannot change this decision.
func (app *SageApp) checkAndApplyScopedQuorum(ballot scope.Ballot, height int64, blockTime time.Time) {
	if ballot.State != scope.BallotPending {
		return
	}
	votes := make(map[string]string, len(ballot.Members))
	var acceptWeight uint64
	for _, member := range ballot.Members {
		voteData, err := app.badgerStore.GetState(fmt.Sprintf("vote:%s:%s", ballot.MemoryID, member.ValidatorID))
		if err != nil || voteData == nil {
			continue
		}
		decision := string(voteData)
		votes[member.ValidatorID] = decision
		if decision == "accept" {
			if math.MaxUint64-acceptWeight < member.EffectiveWeight {
				app.logger.Error().Str("memory_id", ballot.MemoryID).Msg("scoped accept weight overflow")
				return
			}
			acceptWeight += member.EffectiveWeight
		}
	}

	reached := scope.HasStrictSupermajority(acceptWeight, ballot.TotalWeight)
	allVoted := len(votes) == len(ballot.Members)
	if !reached && !allVoted {
		return
	}
	verdict := scope.BallotDeprecated
	status := memory.StatusDeprecated
	finalAccepted := false
	if reached {
		verdict = scope.BallotCommitted
		status = memory.StatusCommitted
		finalAccepted = true
	}
	if err := app.badgerStore.SetScopedMemoryVerdict(ballot.MemoryID, verdict); err != nil {
		app.logger.Error().Err(err).Str("memory_id", ballot.MemoryID).Msg("scoped verdict write failed")
		return
	}

	app.pendingWrites = append(app.pendingWrites, pendingWrite{
		writeType: "status_update",
		data: &statusUpdate{
			MemoryID: ballot.MemoryID,
			Status:   status,
			At:       blockTime,
		},
	})

	// Feed the same verdict-correctness accumulators used by ordinary quorum,
	// once, from terminal non-abstain votes. Ballot state prevents repeat credit.
	matches := make(map[string]bool, len(votes))
	for validatorID, decision := range votes {
		if decision == "abstain" {
			continue
		}
		matches[validatorID] = (decision == "accept") == finalAccepted
	}
	if len(matches) > 0 {
		if err := app.badgerStore.UpdateVerdictStats(matches); err != nil {
			app.logger.Error().Err(err).Str("memory_id", ballot.MemoryID).Msg("scoped verdict stats update")
		}
		if content, err := app.badgerStore.GetScopedContent(ballot.MemoryID); err == nil && content != nil {
			if err := app.badgerStore.UpdateDomainVerdictStats(content.Domain, matches); err != nil {
				app.logger.Error().Err(err).Str("memory_id", ballot.MemoryID).Str("domain", content.Domain).Msg("scoped domain verdict stats update")
			}
		}
	}
	app.logger.Info().
		Str("memory_id", ballot.MemoryID).
		Str("scope_id", ballot.ScopeID).
		Uint64("scope_revision", ballot.ScopeRevision).
		Uint64("accept_weight", acceptWeight).
		Uint64("total_weight", ballot.TotalWeight).
		Bool("accepted", finalAccepted).
		Int64("height", height).
		Msg("scoped memory reached terminal quorum")
}
