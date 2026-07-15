package rest

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/scope"
)

type scopeDomainResponse struct {
	Name    string `json:"name"`
	Subtree bool   `json:"subtree"`
}

type scopeMemberResponse struct {
	ValidatorID             string `json:"validator_id"`
	AssignedWeight          uint64 `json:"assigned_weight"`
	JoinedRevision          uint64 `json:"joined_revision"`
	Active                  bool   `json:"active"`
	PendingBallotCount      int    `json:"pending_ballot_count"`
	ValidatorRemovalBlocked bool   `json:"validator_removal_blocked"`
}

type scopeDrainResponse struct {
	PendingBallotCount   int      `json:"pending_ballot_count"`
	PendingMemoryIDs     []string `json:"pending_memory_ids"`
	BlockingValidatorIDs []string `json:"blocking_validator_ids"`
}

type scopeRecordResponse struct {
	ScopeID               string                `json:"scope_id"`
	Revision              uint64                `json:"revision"`
	RevisionHash          string                `json:"revision_hash"`
	State                 string                `json:"state"`
	ControllerValidatorID string                `json:"controller_validator_id"`
	CreatedHeight         int64                 `json:"created_height"`
	UpdatedHeight         int64                 `json:"updated_height"`
	Domains               []scopeDomainResponse `json:"domains"`
	Members               []scopeMemberResponse `json:"members"`
	Drain                 scopeDrainResponse    `json:"drain"`
}

func (s *Server) handleListScopes(w http.ResponseWriter, r *http.Request) {
	if !s.scopeReadAuthorized(r) {
		writeProblem(w, http.StatusForbidden, "Forbidden", "scope topology is visible only to the node operator or an administrator.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Scope state unavailable", "canonical scope storage is not configured.")
		return
	}
	records, err := s.badgerStore.ListScopeRecords()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Scope state invalid", err.Error())
		return
	}
	pending, err := s.badgerStore.ListPendingScopeBallots()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Scope state invalid", err.Error())
		return
	}
	views := make([]scopeRecordResponse, 0, len(records))
	for i := range records {
		view, err := s.scopeRecordViewWithBallots(&records[i], pending)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "Scope state invalid", err.Error())
			return
		}
		views = append(views, view)
	}
	writeJSON(w, http.StatusOK, map[string]any{"scopes": views, "count": len(views)})
}

func (s *Server) handleGetScope(w http.ResponseWriter, r *http.Request) {
	if !s.scopeReadAuthorized(r) {
		writeProblem(w, http.StatusForbidden, "Forbidden", "scope topology is visible only to the node operator or an administrator.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Scope state unavailable", "canonical scope storage is not configured.")
		return
	}
	record, err := s.badgerStore.GetScopeRecord(chi.URLParam(r, "scope_id"))
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Scope state invalid", err.Error())
		return
	}
	if record == nil {
		writeProblem(w, http.StatusNotFound, "Scope not found", "No canonical scope exists with that id.")
		return
	}
	view, err := s.scopeRecordView(record)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Scope state invalid", err.Error())
		return
	}
	writeJSON(w, http.StatusOK, view)
}

func (s *Server) scopeReadAuthorized(r *http.Request) bool {
	callerID := middleware.ContextAgentID(r.Context())
	return s.callerIsOperatorOrAdmin(r.Context(), callerID)
}

func (s *Server) scopeRecordView(record *scope.Record) (scopeRecordResponse, error) {
	pending, err := s.badgerStore.ListPendingScopeBallots()
	if err != nil {
		return scopeRecordResponse{}, err
	}
	return s.scopeRecordViewWithBallots(record, pending)
}

func (s *Server) scopeRecordViewWithBallots(record *scope.Record, pending []scope.Ballot) (scopeRecordResponse, error) {
	digest, err := s.badgerStore.GetScopeRevisionHash(record.ScopeID, record.Revision)
	if err != nil {
		return scopeRecordResponse{}, err
	}
	if len(digest) != sha256.Size {
		return scopeRecordResponse{}, fmt.Errorf("scope %q revision %d is missing its audit anchor", record.ScopeID, record.Revision)
	}
	domains := make([]scopeDomainResponse, 0, len(record.Domains))
	for _, domain := range record.Domains {
		domains = append(domains, scopeDomainResponse{Name: domain.Name, Subtree: domain.Subtree})
	}
	pendingByValidator := make(map[string]int)
	pendingMemoryIDs := make([]string, 0)
	blockingValidators := make(map[string]struct{})
	for _, ballot := range pending {
		if ballot.ScopeID != record.ScopeID {
			continue
		}
		pendingMemoryIDs = append(pendingMemoryIDs, ballot.MemoryID)
		for _, member := range ballot.Members {
			pendingByValidator[member.ValidatorID]++
			blockingValidators[member.ValidatorID] = struct{}{}
		}
	}
	members := make([]scopeMemberResponse, 0, len(record.Members))
	for _, member := range record.Members {
		if record.State != scope.StateRetired && member.Active {
			blockingValidators[member.ValidatorID] = struct{}{}
		}
		members = append(members, scopeMemberResponse{
			ValidatorID: member.ValidatorID, AssignedWeight: member.AssignedWeight,
			JoinedRevision: member.JoinedRevision, Active: member.Active,
			PendingBallotCount:      pendingByValidator[member.ValidatorID],
			ValidatorRemovalBlocked: (record.State != scope.StateRetired && member.Active) || pendingByValidator[member.ValidatorID] > 0,
		})
	}
	blockingValidatorIDs := make([]string, 0, len(blockingValidators))
	for validatorID := range blockingValidators {
		blockingValidatorIDs = append(blockingValidatorIDs, validatorID)
	}
	sort.Strings(blockingValidatorIDs)
	return scopeRecordResponse{
		ScopeID: record.ScopeID, Revision: record.Revision, RevisionHash: hex.EncodeToString(digest),
		State: scopeStateString(record.State), ControllerValidatorID: record.ControllerValidatorID,
		CreatedHeight: record.CreatedHeight, UpdatedHeight: record.UpdatedHeight,
		Domains: domains, Members: members,
		Drain: scopeDrainResponse{
			PendingBallotCount: len(pendingMemoryIDs), PendingMemoryIDs: pendingMemoryIDs,
			BlockingValidatorIDs: blockingValidatorIDs,
		},
	}, nil
}

func scopeStateString(state scope.State) string {
	switch state {
	case scope.StateActive:
		return "active"
	case scope.StatePaused:
		return "paused"
	case scope.StateRetired:
		return "retired"
	default:
		return "unknown"
	}
}
