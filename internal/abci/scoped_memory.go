package abci

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
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
		Tags:              append([]string(nil), submit.Tags...),
		SubmittedHeight:   height,
		SubmittedUnix:     blockTime.Unix(),
	}
	if err := app.badgerStore.SetScopedMemorySubmission(ballot, content); err != nil {
		return false, err
	}
	return true, nil
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
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
