package web

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/l33tdawg/sage/internal/store"
)

const appV25LegacyDeprecationConfirmation = "DEPRECATE %d"
const appV26LegacyRecoveryRequestMaxBytes = 160 << 10

type appV25LegacyRecoveryController interface {
	GetLegacyMemoryAdoptionProgress(context.Context) (*store.LegacyMemoryAdoptionProgress, error)
	ValidateLegacyMemoryRecoverySnapshot(context.Context, uint64, int) error
	DeprecateLegacyMemoryRecoverySnapshot(context.Context, uint64, int, string) (int, error)
	ListLegacyMemoryRecoveryInventoryPage(context.Context, string, int) ([]store.LegacyMemoryRecoveryInventoryItem, string, error)
	AssignLegacyMemoryRecoverySelection(context.Context, uint64, int, []string, string, string) (int, error)
	DeprecateLegacyMemoryRecoverySelection(context.Context, uint64, int, []string, string) (int, error)
}

type appV25LegacyRecoveryControlRequest struct {
	ProjectionRevision uint64   `json:"projection_revision"`
	ExpectedCount      int      `json:"expected_count"`
	Confirmation       string   `json:"confirmation,omitempty"`
	MemoryIDs          []string `json:"memory_ids,omitempty"`
	TargetAgentID      string   `json:"target_agent_id,omitempty"`
}

type appV26LegacyRecoveryInventoryView struct {
	MemoryID             string                               `json:"memory_id"`
	Reason               string                               `json:"reason"`
	Domain               string                               `json:"domain"`
	Author               string                               `json:"historical_author,omitempty"`
	ContentPreview       string                               `json:"content_preview"`
	Assignable           bool                                 `json:"assignable"`
	AssignedTarget       string                               `json:"assigned_target,omitempty"`
	AuthorityOwnerID     string                               `json:"authority_owner_id,omitempty"`
	AuthorityOwnedDomain string                               `json:"authority_owned_domain,omitempty"`
	AuthorityStatus      string                               `json:"authority_status"`
	AuthorityHistory     []store.AppV26DomainOwnershipHistory `json:"authority_history,omitempty"`
}

func appV26LegacyContentPreview(content string) string {
	content = strings.Join(strings.Fields(content), " ")
	runes := []rune(content)
	if len(runes) <= 280 {
		return content
	}
	return string(runes[:280]) + "…"
}

func appV26LegacyRecoveryAssignable(item store.LegacyMemoryRecoveryInventoryItem) bool {
	if item.Reason != "author_identity_unresolved" || item.MemoryID == "" ||
		item.Domain == "" || item.SubmittingAgent == "" || item.EvidenceError != "" ||
		len(item.ContentHash) != sha256.Size ||
		(item.Status != "proposed" && item.Status != "committed") ||
		item.Classification > uint8(store.ClearanceTopSecret) {
		return false
	}
	digest := sha256.Sum256([]byte(item.Content))
	return bytes.Equal(digest[:], item.ContentHash)
}

// handleAppV26LegacyRecoveryInventory is Root-loopback-only and paginated.
// It joins previews from the original encrypted memory table at request time;
// recovery tables remain content-free.
func (h *DashboardHandler) handleAppV26LegacyRecoveryInventory(w http.ResponseWriter, r *http.Request) {
	_, controller, ok := h.appV25LegacyRecoveryControl(w, r)
	if !ok {
		return
	}
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 100 {
		limit = 50
	}
	items, next, err := controller.ListLegacyMemoryRecoveryInventoryPage(
		r.Context(), r.URL.Query().Get("after"), limit,
	)
	if err != nil {
		writeAppV25LegacyRecoveryControlError(w, err)
		return
	}
	views := make([]appV26LegacyRecoveryInventoryView, 0, len(items))
	for _, item := range items {
		view := appV26LegacyRecoveryInventoryView{
			MemoryID: item.MemoryID, Reason: item.Reason, Domain: item.Domain,
			Author:          item.SubmittingAgent,
			ContentPreview:  appV26LegacyContentPreview(item.Content),
			Assignable:      appV26LegacyRecoveryAssignable(item),
			AssignedTarget:  item.AssignedTarget,
			AuthorityStatus: "unavailable",
		}
		if h.BadgerStore != nil && strings.TrimSpace(item.Domain) != "" {
			owner, ownedDomain, ownerErr := h.BadgerStore.ResolveAppV23OwningAncestor(item.Domain)
			switch {
			case ownerErr != nil:
				view.AuthorityStatus = "lookup_failed"
			case owner == "" || ownedDomain == "":
				// A grant must never auto-claim an unresolved historical domain.
				// Ownership requires its own explicit recovery/governance decision.
				view.AuthorityStatus = "unowned"
			default:
				view.AuthorityStatus = "available"
				view.AuthorityOwnerID = owner
				view.AuthorityOwnedDomain = ownedDomain
				history, historyErr := h.BadgerStore.ListAppV26DomainOwnershipHistory(ownedDomain)
				if historyErr == nil {
					view.AuthorityHistory = history
				}
			}
		}
		views = append(views, view)
	}
	type agentView struct {
		AgentID string `json:"agent_id"`
		Name    string `json:"name"`
	}
	agents := make([]agentView, 0)
	if h.BadgerStore != nil {
		onChain, listErr := h.BadgerStore.ListRegisteredAgents()
		if listErr != nil {
			writeError(w, http.StatusServiceUnavailable, "active agent inventory is unavailable")
			return
		}
		for _, agent := range onChain {
			if h.appV23IsRootIdentity(agent.AgentID) {
				continue
			}
			enrollment, enrollmentErr := h.BadgerStore.GetAppV23Enrollment(agent.AgentID)
			role, roleErr := h.BadgerStore.GetAppV23Role(agent.AgentID)
			if enrollmentErr != nil || roleErr != nil {
				writeError(w, http.StatusServiceUnavailable, "active agent inventory is unavailable")
				return
			}
			if enrollment != nil && role != nil && enrollment.Active &&
				(enrollment.Profile == store.AppV23ProfileStandard ||
					enrollment.Profile == store.AppV23ProfileCompanion) &&
				store.ValidAppV23Role(role.Role) &&
				store.AppV23ProfileAllowsRole(enrollment.Profile, role.Role) {
				name := agent.Name
				if name == "" {
					name = agent.RegisteredName
				}
				agents = append(agents, agentView{AgentID: agent.AgentID, Name: name})
			}
		}
	}
	progress, _ := controller.GetLegacyMemoryAdoptionProgress(r.Context())
	writeJSONResp(w, http.StatusOK, map[string]any{
		"items": views, "next_after": next, "agents": agents, "progress": progress,
		"assignment_active": h.appV26IsActive(),
	})
}

func (h *DashboardHandler) handleAppV26LegacyAdoptionAssign(w http.ResponseWriter, r *http.Request) {
	actor, controller, ok := h.appV25LegacyRecoveryControl(w, r)
	if !ok {
		return
	}
	if !h.appV26IsActive() {
		writeError(w, http.StatusConflict, "historical memory assignment requires governed app-v26")
		return
	}
	request, ok := decodeAppV25LegacyRecoveryControlRequest(w, r)
	if !ok {
		return
	}
	if len(request.MemoryIDs) == 0 || strings.TrimSpace(request.TargetAgentID) == "" {
		writeError(w, http.StatusBadRequest, "memory_ids and target_agent_id are required")
		return
	}
	eligible, err := h.appV26LegacyRecoveryTargetEligible(request.TargetAgentID)
	if err != nil {
		writeError(w, http.StatusServiceUnavailable, "target agent state is unavailable")
		return
	}
	if !eligible {
		writeError(w, http.StatusConflict, "target must be an active ordinary local agent")
		return
	}
	assigned, err := controller.AssignLegacyMemoryRecoverySelection(
		r.Context(), request.ProjectionRevision, request.ExpectedCount,
		request.MemoryIDs, request.TargetAgentID, actor.ID,
	)
	if err != nil {
		writeAppV25LegacyRecoveryControlError(w, err)
		return
	}
	epoch := h.requestAppV25LegacyAdoptionRetry()
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"status": "assignment_queued", "assigned": assigned,
		"target_agent_id": request.TargetAgentID, "retry_epoch": epoch,
		"access_scope": "operational_principal_only",
		"message":      "The Root-governed principal repair is queued for canonical adoption; historical authorship, content, and domain permissions remain unchanged.",
	})
}

func (h *DashboardHandler) appV26LegacyRecoveryTargetEligible(targetAgentID string) (bool, error) {
	targetAgentID = strings.TrimSpace(targetAgentID)
	if h.BadgerStore == nil || targetAgentID == "" || h.appV23IsRootIdentity(targetAgentID) ||
		!h.BadgerStore.IsAgentRegistered(targetAgentID) {
		return false, nil
	}
	enrollment, err := h.BadgerStore.GetAppV23Enrollment(targetAgentID)
	if err != nil {
		return false, err
	}
	role, err := h.BadgerStore.GetAppV23Role(targetAgentID)
	if err != nil {
		return false, err
	}
	return enrollment != nil && role != nil && enrollment.Active &&
		(enrollment.Profile == store.AppV23ProfileStandard ||
			enrollment.Profile == store.AppV23ProfileCompanion) &&
		store.ValidAppV23Role(role.Role) &&
		store.AppV23ProfileAllowsRole(enrollment.Profile, role.Role), nil
}

func (h *DashboardHandler) appV25LegacyAdoptionWakeChannel() <-chan struct{} {
	h.appV25AdoptionWakeOnce.Do(func() {
		if h.appV25AdoptionWake == nil {
			h.appV25AdoptionWake = make(chan struct{}, 1)
		}
	})
	return h.appV25AdoptionWake
}

func (h *DashboardHandler) requestAppV25LegacyAdoptionRetry() uint64 {
	_ = h.appV25LegacyAdoptionWakeChannel()
	epoch := h.appV25AdoptionRetry.Add(1)
	select {
	case h.appV25AdoptionWake <- struct{}{}:
	default:
	}
	return epoch
}

func decodeAppV25LegacyRecoveryControlRequest(
	w http.ResponseWriter,
	r *http.Request,
) (appV25LegacyRecoveryControlRequest, bool) {
	var request appV25LegacyRecoveryControlRequest
	// A selected recovery mutation accepts up to 256 bounded historical IDs.
	// Four KiB admitted only a fraction of that documented/store-level bound.
	r.Body = http.MaxBytesReader(w, r.Body, appV26LegacyRecoveryRequestMaxBytes)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		writeError(w, http.StatusBadRequest, "invalid memory recovery request")
		return request, false
	}
	if request.ProjectionRevision == 0 || request.ExpectedCount <= 0 {
		writeError(w, http.StatusBadRequest,
			"projection_revision and a positive expected_count are required")
		return request, false
	}
	return request, true
}

func (h *DashboardHandler) appV25LegacyRecoveryControl(
	w http.ResponseWriter,
	r *http.Request,
) (*appV23ControlActor, appV25LegacyRecoveryController, bool) {
	if !h.appV23IsActive() {
		writeError(w, http.StatusConflict,
			"historical memory recovery control requires the governed CEREBRUM access model")
		return nil, nil, false
	}
	actor, ok := h.requireAppV23ControlActor(w, r, false)
	if !ok {
		return nil, nil, false
	}
	if !actor.IsRoot {
		writeAppV23AccessError(w, http.StatusForbidden, "current_root_required",
			"Only the current CEREBRUM Root may resolve preserved historical memories.")
		return nil, nil, false
	}
	controller, ok := h.store.(appV25LegacyRecoveryController)
	if !ok {
		writeError(w, http.StatusNotImplemented,
			"historical memory recovery control is unavailable for this storage backend")
		return nil, nil, false
	}
	return actor, controller, true
}

func (h *DashboardHandler) validateAppV25LegacyRecoveryControlSnapshot(
	ctx context.Context,
	controller appV25LegacyRecoveryController,
	request appV25LegacyRecoveryControlRequest,
) error {
	progress, err := controller.GetLegacyMemoryAdoptionProgress(ctx)
	if err != nil {
		return err
	}
	if progress == nil || progress.State != "recovery" || progress.Remaining != 0 ||
		progress.Revision != request.ProjectionRevision ||
		progress.Recovery != request.ExpectedCount {
		return store.ErrLegacyMemoryRecoverySnapshotChanged
	}
	// The durable recovery rows, their revision, and the published aggregate
	// are the exact operator-facing inventory. Do not require the *global*
	// memory projection revision or a process-local boot receipt here: ordinary
	// post-upgrade writes advance those independently and used to make an
	// unchanged recovery queue impossible to resolve forever.
	return controller.ValidateLegacyMemoryRecoverySnapshot(
		ctx, request.ProjectionRevision, request.ExpectedCount,
	)
}

func writeAppV25LegacyRecoveryControlError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrLegacyMemoryRecoverySnapshotChanged) {
		writeError(w, http.StatusConflict,
			"the historical memory recovery inventory changed; review the current count and try again")
		return
	}
	writeError(w, http.StatusServiceUnavailable,
		"historical memory recovery control is temporarily unavailable")
}

// handleAppV25LegacyAdoptionRetry asks the single background worker to discard
// its cached observation and run a fresh stable scan. It never clears rejection
// receipts, recovery rows, memory rows, or canonical state.
func (h *DashboardHandler) handleAppV25LegacyAdoptionRetry(w http.ResponseWriter, r *http.Request) {
	_, controller, ok := h.appV25LegacyRecoveryControl(w, r)
	if !ok {
		return
	}
	request, ok := decodeAppV25LegacyRecoveryControlRequest(w, r)
	if !ok {
		return
	}
	if err := h.validateAppV25LegacyRecoveryControlSnapshot(
		r.Context(), controller, request,
	); err != nil {
		writeAppV25LegacyRecoveryControlError(w, err)
		return
	}
	epoch := h.requestAppV25LegacyAdoptionRetry()
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"status":              "retry_requested",
		"projection_revision": request.ProjectionRevision,
		"expected_count":      request.ExpectedCount,
		"retry_epoch":         epoch,
		"message":             "SAGE will re-check every preserved historical record now.",
	})
}

// handleAppV25LegacyAdoptionDeprecate records a separate Root-authorized local
// disposition for the exact unresolved inventory. Original memory rows and
// chain history are preserved byte-for-byte; future adoption scans skip only
// these exact IDs. This deliberately does not fabricate canonical memory state.
func (h *DashboardHandler) handleAppV25LegacyAdoptionDeprecate(w http.ResponseWriter, r *http.Request) {
	actor, controller, ok := h.appV25LegacyRecoveryControl(w, r)
	if !ok {
		return
	}
	request, ok := decodeAppV25LegacyRecoveryControlRequest(w, r)
	if !ok {
		return
	}
	confirmationCount := request.ExpectedCount
	if len(request.MemoryIDs) > 0 {
		confirmationCount = len(request.MemoryIDs)
	}
	expectedConfirmation := fmt.Sprintf(appV25LegacyDeprecationConfirmation, confirmationCount)
	if request.Confirmation != expectedConfirmation {
		writeError(w, http.StatusBadRequest,
			"confirmation must exactly match "+expectedConfirmation)
		return
	}
	if err := h.validateAppV25LegacyRecoveryControlSnapshot(
		r.Context(), controller, request,
	); err != nil {
		writeAppV25LegacyRecoveryControlError(w, err)
		return
	}
	var deprecated int
	var err error
	if len(request.MemoryIDs) > 0 {
		deprecated, err = controller.DeprecateLegacyMemoryRecoverySelection(
			r.Context(), request.ProjectionRevision, request.ExpectedCount,
			request.MemoryIDs, actor.ID,
		)
	} else {
		deprecated, err = controller.DeprecateLegacyMemoryRecoverySnapshot(
			r.Context(), request.ProjectionRevision, request.ExpectedCount, actor.ID,
		)
	}
	if err != nil {
		writeAppV25LegacyRecoveryControlError(w, err)
		return
	}
	epoch := h.requestAppV25LegacyAdoptionRetry()
	progress, _ := controller.GetLegacyMemoryAdoptionProgress(r.Context())
	writeJSONResp(w, http.StatusOK, map[string]any{
		"status":              "explicitly_deprecated",
		"deprecated":          deprecated,
		"projection_revision": request.ProjectionRevision,
		"retry_epoch":         epoch,
		"history_preserved":   true,
		"progress":            progress,
		"message":             "The preserved records remain stored for audit but are retired from automatic repair and normal memory views.",
	})
}
