package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
)

// RebuildScopedProjection verifies every AppHash-covered scoped envelope and
// upserts it into the local SQL projection. It performs no consensus write and
// is safe to rerun after snapshot/state sync or loss of the projection DB.
// Callers should keep the affected scope unready if this returns an error.
func (app *SageApp) RebuildScopedProjection(ctx context.Context) (int, error) {
	verified, err := app.verifiedScopedContents()
	if err != nil {
		return 0, err
	}
	rebuilt := 0
	for _, recovered := range verified {
		content := recovered.content
		status := recovered.status

		record := &memory.MemoryRecord{
			MemoryID:        content.MemoryID,
			SubmittingAgent: content.SubmittingAgentID,
			Content:         content.Content,
			ContentHash:     append([]byte(nil), content.ContentHash...),
			MemoryType:      memory.MemoryType(txMemoryTypeToStringValue(content.MemoryType)),
			DomainTag:       content.Domain,
			ConfidenceScore: content.ConfidenceScore,
			Status:          memory.MemoryStatus(status),
			ParentHash:      content.ParentHash,
			TaskStatus:      memory.TaskStatus(content.TaskStatus),
			CreatedAt:       time.Unix(content.SubmittedUnix, 0).UTC(),
		}
		// Keep the row and its classification projection atomic. A crash or a
		// transient SQL error must leave either the previous projection or the
		// complete verified replacement, never canonical content with stale
		// clearance metadata (or the reverse).
		if err := app.offchainStore.RunInTx(ctx, func(projection store.OffchainStore) error {
			if err := projection.InsertMemory(ctx, record); err != nil {
				return fmt.Errorf("insert projection: %w", err)
			}
			if err := projection.UpdateMemoryClassification(ctx, content.MemoryID, store.ClearanceLevel(content.Classification)); err != nil {
				return fmt.Errorf("classification projection: %w", err)
			}
			if err := projection.SetTags(ctx, content.MemoryID, content.Tags); err != nil {
				return fmt.Errorf("tag projection: %w", err)
			}
			projected, err := projection.GetMemory(ctx, content.MemoryID)
			if err != nil || !recoveredProjectionMatches(projected, record) {
				return errors.New("projection verification failed")
			}
			projectedTags, err := projection.GetTags(ctx, content.MemoryID)
			if err != nil || !equalStrings(projectedTags, content.Tags) {
				return errors.New("tag projection verification failed")
			}
			return nil
		}); err != nil {
			return rebuilt, fmt.Errorf("scoped recovery %q: %w", content.MemoryID, err)
		}
		rebuilt++
	}
	return rebuilt, nil
}

type verifiedScopedContent struct {
	content scope.Content
	status  string
}

// VerifyScopedCanonicalState validates every AppHash-covered scoped envelope,
// ballot, content hash, and ordinary lifecycle record without consulting or
// mutating the SQL projection. State-sync preparation runs this before a staged
// Badger database is eligible for activation.
func (app *SageApp) VerifyScopedCanonicalState() (int, error) {
	verified, err := app.verifiedScopedContents()
	return len(verified), err
}

func (app *SageApp) verifiedScopedContents() ([]verifiedScopedContent, error) {
	contents, err := app.badgerStore.ListScopedContents()
	if err != nil {
		return nil, err
	}
	verified := make([]verifiedScopedContent, 0, len(contents))
	for _, content := range contents {
		ballot, err := app.badgerStore.GetScopeBallot(content.MemoryID)
		if err != nil {
			return nil, fmt.Errorf("scoped recovery %q: invalid ballot: %w", content.MemoryID, err)
		}
		if ballot == nil {
			return nil, fmt.Errorf("scoped recovery %q: missing ballot", content.MemoryID)
		}
		if ballot.MemoryID != content.MemoryID || ballot.ScopeID != content.ScopeID ||
			ballot.ScopeRevision != content.ScopeRevision || ballot.SubmittedHeight != content.SubmittedHeight {
			return nil, fmt.Errorf("scoped recovery %q: ballot/content identity mismatch", content.MemoryID)
		}
		computed := sha256.Sum256([]byte(content.Content))
		if !bytes.Equal(computed[:], content.ContentHash) {
			return nil, fmt.Errorf("scoped recovery %q: canonical content hash mismatch", content.MemoryID)
		}
		onChainHash, status, err := app.badgerStore.GetMemoryHash(content.MemoryID)
		if err != nil || !bytes.Equal(onChainHash, content.ContentHash) {
			return nil, fmt.Errorf("scoped recovery %q: ordinary memory hash mismatch", content.MemoryID)
		}
		if err := validateRecoveredScopeStatus(ballot.State, status); err != nil {
			return nil, fmt.Errorf("scoped recovery %q: %w", content.MemoryID, err)
		}
		verified = append(verified, verifiedScopedContent{content: content, status: status})
	}
	return verified, nil
}

func validateRecoveredScopeStatus(ballotState scope.BallotState, status string) error {
	switch ballotState {
	case scope.BallotPending:
		if status != string(memory.StatusProposed) {
			return fmt.Errorf("pending ballot has ordinary status %q", status)
		}
	case scope.BallotCommitted:
		// A later challenge/deprecation changes the ordinary lifecycle but does
		// not erase the original accepted-ballot proof.
		if status != string(memory.StatusCommitted) && status != string(memory.StatusChallenged) && status != string(memory.StatusDeprecated) {
			return fmt.Errorf("committed ballot has ordinary status %q", status)
		}
	case scope.BallotDeprecated:
		if status != string(memory.StatusDeprecated) {
			return fmt.Errorf("deprecated ballot has ordinary status %q", status)
		}
	default:
		return fmt.Errorf("unknown ballot state %d", ballotState)
	}
	return nil
}

func txMemoryTypeToStringValue(value byte) string {
	switch value {
	case 1:
		return "fact"
	case 2:
		return "observation"
	case 3:
		return "inference"
	case 4:
		return "task"
	default:
		return ""
	}
}

func recoveredProjectionMatches(actual, expected *memory.MemoryRecord) bool {
	if actual == nil || expected == nil {
		return false
	}
	return actual.MemoryID == expected.MemoryID &&
		actual.SubmittingAgent == expected.SubmittingAgent &&
		actual.Content == expected.Content &&
		bytes.Equal(actual.ContentHash, expected.ContentHash) &&
		actual.MemoryType == expected.MemoryType &&
		actual.DomainTag == expected.DomainTag &&
		math.Float64bits(actual.ConfidenceScore) == math.Float64bits(expected.ConfidenceScore) &&
		actual.Status == expected.Status &&
		// TaskStatus is deliberately excluded. It is a local workflow field that
		// can legitimately advance after submission; InsertMemory preserves an
		// existing value and restores the submitted value only into a lost row.
		actual.ParentHash == expected.ParentHash
}
