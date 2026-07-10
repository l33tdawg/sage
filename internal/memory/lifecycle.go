package memory

import (
	"fmt"
	"time"
)

// validTransitions defines the allowed state transitions.
//
// NOTE: this map is documentation / SQLite hygiene only — it has ZERO production
// callers. Consensus writes statuses imperatively via BadgerStore.SetMemoryHash
// (internal/abci/app.go), which performs no transition check. The map is kept in
// sync so it does not contradict chain behavior.
//
// app-v17 (v11.5) reintroduced a REACHABLE `challenged` state via the quorum-
// scaled two-phase challenge: in an org network a challenge on a memory whose
// domain has >= 2 modify-verb holders parks it CHALLENGED (committed→challenged),
// pending a second holder's CONFIRM (challenged→deprecated) or any holder's
// REINSTATE (challenged→committed, TxTypeMemoryReinstate). On personal nodes
// (modify-holder count <= 1) a challenge is still decisive in one step
// (committed→deprecated), so both edges are legal. `deprecated` is terminal.
var validTransitions = map[MemoryStatus][]MemoryStatus{
	StatusProposed:   {StatusValidated, StatusChallenged, StatusDeprecated},
	StatusValidated:  {StatusCommitted, StatusDeprecated},
	StatusCommitted:  {StatusChallenged, StatusDeprecated},
	StatusChallenged: {StatusCommitted, StatusDeprecated},
}

// ValidTransition checks if a state transition is allowed.
func ValidTransition(from, to MemoryStatus) bool {
	allowed, ok := validTransitions[from]
	if !ok {
		return false
	}
	for _, s := range allowed {
		if s == to {
			return true
		}
	}
	return false
}

// Transition attempts to transition a memory record to a new status.
func Transition(record *MemoryRecord, to MemoryStatus, now time.Time) error {
	if !ValidTransition(record.Status, to) {
		return fmt.Errorf("invalid transition from %s to %s", record.Status, to)
	}

	record.Status = to

	switch to {
	case StatusCommitted:
		record.CommittedAt = &now
	case StatusDeprecated:
		record.DeprecatedAt = &now
	}

	return nil
}
