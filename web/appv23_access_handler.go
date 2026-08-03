package web

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	appV23AccessBodyLimit         = 64 << 10
	appV23LegacyRestrictedProfile = store.AppV23ProfileLegacyRestricted
)

type appV23AgentAccessView struct {
	AgentID              string `json:"agent_id"`
	Name                 string `json:"name,omitempty"`
	RegisteredName       string `json:"registered_name,omitempty"`
	Avatar               string `json:"avatar,omitempty"`
	Status               string `json:"status,omitempty"`
	Provider             string `json:"provider,omitempty"`
	Role                 string `json:"role"`
	Profile              string `json:"profile,omitempty"`
	HomeDomain           string `json:"home_domain,omitempty"`
	Clearance            uint8  `json:"clearance"`
	Capabilities         uint32 `json:"capabilities"`
	EnrollmentActive     bool   `json:"enrollment_active"`
	EnrollmentRevision   uint64 `json:"enrollment_revision"`
	RoleRevision         uint64 `json:"role_revision"`
	NeedsApproval        bool   `json:"needs_approval"`
	NeedsReauthorization bool   `json:"needs_reauthorization"`
	LocalKeyAvailable    bool   `json:"local_key_available"`
}

type appV23BrokerView struct {
	Available    bool   `json:"available"`
	CredentialID string `json:"credential_id,omitempty"`
	ReasonCode   string `json:"reason_code,omitempty"`
	Message      string `json:"message,omitempty"`
}

type appV23LinkedReadersView struct {
	Status     string `json:"status"`
	ReasonCode string `json:"reason_code"`
	Message    string `json:"message"`
}

type appV23LinkedMessagesView struct {
	Status     string `json:"status"`
	ReasonCode string `json:"reason_code"`
	Message    string `json:"message"`
}

type appV23FederatedGuestControl interface {
	PrepareFederatedGuestMutation(
		context.Context,
		string,
		federation.FederatedGuestMutationInput,
	) (*federation.PreparedFederatedGuestMutation, error)
	MintFederatedGuestElevation(
		context.Context,
		string,
		federation.FederatedGuestMutation,
	) (*federation.FederatedGuestElevation, error)
	CommitFederatedGuestMutation(
		context.Context,
		string,
		federation.FederatedGuestMutation,
	) (*store.FederatedGroupGuest, error)
	ListFederatedGuestLinks(
		context.Context,
		string,
		string,
		string,
	) ([]federation.FederatedGuestLinkView, error)
	ListFederatedGuestIdentities(
		context.Context,
		string,
	) ([]store.FederatedGuestIdentity, error)
}

type appV23FederatedGuestEligibility interface {
	CheckRemoteFederatedGuestAgentEligibility(
		context.Context,
		string,
		string,
	) error
}

// appV23LinkedMessageConsentControl is deliberately narrower than the
// linked-reader manager. A read link and receiver-local message acceptance are
// separate capabilities, and neither interface can manufacture the other.
type appV23LinkedMessageConsentControl interface {
	ListLinkedMessageConsentCandidates(
		context.Context,
		string,
		string,
		string,
	) ([]federation.LinkedMessageConsentCandidate, error)
	ListRemoteHostedLinkedMessageConsentCandidates(
		context.Context,
		string,
		string,
		string,
	) ([]federation.LinkedMessageConsentCandidate, error)
	GetLinkedMessageConsent(
		context.Context,
		string,
		string,
		string,
	) (*store.FederatedLinkedMessageConsent, error)
	SetLinkedMessageConsentCAS(
		context.Context,
		string,
		string,
		string,
		int64,
		bool,
	) (int64, error)
}

type appV23FederatedGuestEligibilityRequest struct {
	RemoteChainID string `json:"remote_chain_id"`
	RemoteAgentID string `json:"remote_agent_id"`
}

type appV23LinkedMessageConsentRequest struct {
	RemoteChainID    string `json:"remote_chain_id"`
	RemoteAgentID    string `json:"remote_agent_id"`
	LocalAgentID     string `json:"local_agent_id"`
	ExpectedRevision int64  `json:"expected_revision"`
	Accepting        *bool  `json:"accepting"`
}

type appV23LinkedMessageConsentView struct {
	RemoteChainID string `json:"remote_chain_id"`
	RemoteAgentID string `json:"remote_agent_id"`
	LocalAgentID  string `json:"local_agent_id"`
	Revision      int64  `json:"revision"`
	Accepting     bool   `json:"accepting"`
}

type appV23PolicyRequest struct {
	Role         string `json:"role"`
	Profile      string `json:"profile"`
	HomeDomain   string `json:"home_domain,omitempty"`
	Clearance    uint8  `json:"clearance"`
	Capabilities uint32 `json:"capabilities"`
}

type appV26AgentDisplayNameRequest struct {
	Name string `json:"name"`
}

func appV23PolicyNeedsHomeReapproval(
	enrollment *store.AppV23LocalEnrollment,
	nextProfile string,
) bool {
	return enrollment != nil && enrollment.Active &&
		(enrollment.Profile == store.AppV23ProfileReadOnly ||
			enrollment.Profile == store.AppV23ProfileLegacyRestricted) &&
		enrollment.HomeDomain == "" &&
		nextProfile != store.AppV23ProfileReadOnly
}

// handleAppV26AgentDisplayName gives the local human operator a governed way
// to change only an agent's mutable display label. The registered name,
// agent_id, and boot bio are copied from current consensus state and cannot be
// supplied by the browser. Consensus enforces the same invariant at H+1, so a
// forged AgentUpdate cannot use this route's authority to rewrite identity or
// purpose metadata.
func (h *DashboardHandler) handleAppV26AgentDisplayName() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
		if !h.appV26IsActive() {
			writeAppV23AccessError(w, http.StatusConflict, "app_v26_inactive",
				"Operator display-name changes require governed app-v26 activation.")
			return
		}
		agentID := strings.TrimSpace(chi.URLParam(r, "id"))
		if _, err := auth.AgentIDToPublicKey(agentID); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_agent_id",
				"Agent ID must be canonical lowercase Ed25519 hex.")
			return
		}
		if h.appV23IsRootIdentity(agentID) {
			writeAppV23AccessError(w, http.StatusForbidden, "root_identity_immutable",
				"CEREBRUM Root is sovereign authority and cannot be renamed as an agent.")
			return
		}
		var req appV26AgentDisplayNameRequest
		if err := decodeAppV23AccessJSON(w, r, &req); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		if !utf8.ValidString(req.Name) || req.Name == "" || len(req.Name) > 128 {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_display_name",
				"Display name must be 1–128 UTF-8 bytes after trimming.")
			return
		}
		current, err := h.BadgerStore.GetRegisteredAgent(agentID)
		if err != nil || current == nil {
			writeAppV23AccessError(w, http.StatusNotFound, "agent_not_registered",
				"The selected local agent is not registered in consensus state.")
			return
		}
		if req.Name == current.Name {
			writeJSONResp(w, http.StatusOK, map[string]any{
				"ok": true, "status": "unchanged", "committed": false,
				"agent_id": agentID, "name": current.Name,
				"registered_name": current.RegisteredName,
			})
			return
		}
		ptx := &tx.ParsedTx{
			Type: tx.TxTypeAgentUpdate,
			AgentUpdateTx: &tx.AgentUpdate{
				AgentID: agentID,
				Name:    req.Name,
				BootBio: current.BootBio,
			},
		}
		hash, height, _, broadcastErr := h.signAndBroadcastAppV23ControlContext(r.Context(), ptx, actor)
		reconciled := false
		if broadcastErr != nil {
			if isIndeterminateCommitError(broadcastErr) {
				reconciled = waitForAppV23CommittedState(func() bool {
					updated, lookupErr := h.BadgerStore.GetRegisteredAgent(agentID)
					return lookupErr == nil && updated != nil && updated.Name == req.Name &&
						updated.BootBio == current.BootBio &&
						updated.RegisteredName == current.RegisteredName
				})
				if !reconciled {
					writeAppV23CommitUnconfirmed(w, "agent display-name change")
					return
				}
			} else {
				writeAppV23AccessError(w, http.StatusConflict, "consensus_rejected",
					"The display-name change was not committed.")
				return
			}
		}
		projectionReady := false
		projectionWarning := "The display name committed, but the local Agents view is still catching up. Do not submit the rename again."
		if agentStore, storeOK := h.store.(store.AgentStore); storeOK {
			local, localErr := agentStore.GetAgent(r.Context(), agentID)
			if localErr == nil && local != nil {
				projected := *local
				projected.Name = req.Name
				projected.RegisteredName = current.RegisteredName
				projected.BootBio = current.BootBio
				if updateErr := agentStore.UpdateAgent(r.Context(), &projected); updateErr == nil {
					projectionReady = true
					projectionWarning = ""
				}
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "status": "committed", "committed": true,
			"agent_id": agentID, "name": req.Name,
			"registered_name": current.RegisteredName,
			"tx_hash":         hash, "height": height, "reconciled": reconciled,
			"projection_ready": projectionReady, "projection_warning": projectionWarning,
		})
	}
}

type appV23GroupRequest struct {
	Name             string   `json:"name"`
	Members          []string `json:"members"`
	MemberAuthority  string   `json:"member_authority"`
	ExpectedRevision uint64   `json:"expected_revision"`
}

type appV23GroupDeleteRequest struct {
	ExpectedRevision uint64 `json:"expected_revision"`
}

type appV23ControlActor struct {
	ID      string
	IsRoot  bool
	Root    *store.AppV23RootState
	Key     ed25519.PrivateKey
	RootKey ed25519.PrivateKey
}

func (a *appV23ControlActor) PolicyPrincipalID() string {
	if a != nil && a.IsRoot && a.Root != nil {
		return a.Root.PrincipalID
	}
	if a == nil {
		return ""
	}
	return a.ID
}

func writeAppV23AccessError(w http.ResponseWriter, status int, code, message string) {
	writeJSONResp(w, status, map[string]any{
		"ok":    false,
		"code":  code,
		"error": message,
	})
}

func decodeAppV23AccessJSON(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, appV23AccessBodyLimit)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(dst); err != nil {
		return fmt.Errorf("invalid request body: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain exactly one JSON object")
	}
	return nil
}

func (h *DashboardHandler) appV23IsActive() bool {
	return h.AppV23ActiveFn != nil && h.AppV23ActiveFn()
}

func (h *DashboardHandler) appV26IsActive() bool {
	return h.AppV26ActiveFn != nil && h.AppV26ActiveFn()
}

func (h *DashboardHandler) appV23IsRootIdentity(agentID string) bool {
	if !h.appV23IsActive() {
		return false
	}
	if h.BadgerStore == nil {
		return true
	}
	root, err := h.BadgerStore.GetAppV23Root()
	if err != nil || root == nil {
		// This helper guards ordinary-agent surfaces whose signatures cannot
		// propagate a store error. Hide/reject targets while Root state is
		// unavailable rather than risk exposing a sovereign credential.
		return true
	}
	wasRoot, err := h.BadgerStore.IsAppV23RootCredential(agentID)
	if err != nil {
		return true
	}
	return wasRoot || agentID == root.PrincipalID || agentID == root.CredentialID
}

// appV23RootBrokerKey resolves only the currently committed root credential.
// It never assumes that the historical genesis key remains Root after a
// credential rotation.
func (h *DashboardHandler) appV23RootBrokerKey() (*store.AppV23RootState, ed25519.PrivateKey, appV23BrokerView) {
	view := appV23BrokerView{}
	if h.BadgerStore == nil {
		view.ReasonCode = "consensus_state_unavailable"
		view.Message = "Consensus access-control state is unavailable."
		return nil, nil, view
	}
	root, err := h.BadgerStore.GetAppV23Root()
	if err != nil || root == nil {
		view.ReasonCode = "root_state_unavailable"
		view.Message = "The committed CEREBRUM Root state is unavailable."
		return root, nil, view
	}
	view.CredentialID = root.CredentialID
	if h.ResolveAgentKeyFn != nil {
		if key, ok := h.ResolveAgentKeyFn(root.CredentialID); ok &&
			len(key) == ed25519.PrivateKeySize &&
			agentIDForKey(key) == root.CredentialID {
			view.Available = true
			return root, key, view
		}
	}
	if len(h.AdminSigningKey) == ed25519.PrivateKeySize &&
		agentIDForKey(h.AdminSigningKey) == root.CredentialID {
		view.Available = true
		return root, h.AdminSigningKey, view
	}
	view.ReasonCode = "root_key_unavailable"
	view.Message = "This machine does not hold the currently committed CEREBRUM Root credential."
	return root, nil, view
}

// appV23SignedRequestActorID returns only a principal that authenticated this
// exact HTTP request. The historical NodeOperatorAgentID is deliberately not
// consulted: after tx-39 it names a stale credential until restart and must
// never be able to drive the current Root signing broker.
func (h *DashboardHandler) appV23SignedRequestActorID(r *http.Request) string {
	if actorID := strings.TrimSpace(verifiedDashboardAgentID(r.Context())); actorID != "" {
		return actorID
	}
	actorID := strings.TrimSpace(r.Header.Get("X-Agent-ID"))
	if actorID != "" && h.validAgentSignature(r) {
		return actorID
	}
	return ""
}

func (h *DashboardHandler) appV23SessionHasRootAuthority(r *http.Request) bool {
	if !isLoopbackRemote(r.RemoteAddr) ||
		!hostIsLoopback(r.Host) ||
		!isLocalRequest(r) {
		return false
	}
	// With no vault password configured, the same-machine CEREBRUM SPA is the
	// only human control surface available. Treat that narrowly verified
	// browser request as current Root; do not require users to enable encryption
	// merely to make RBAC, federation, or other dashboard mutations work.
	// isLoopbackCEREBRUMBrowserRequest rejects unsigned CLI-style localhost
	// calls, LAN callers, cross-origin/rebinding requests, and signed agents.
	if !h.Encrypted.Load() {
		return isLoopbackCEREBRUMBrowserRequest(r)
	}
	secFetch := strings.TrimSpace(r.Header.Get("Sec-Fetch-Site"))
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	switch secFetch {
	case "same-origin", "none":
		if origin != "" && !originMatchesRequest(r, origin) {
			return false
		}
	case "":
		if origin != "" && !originMatchesRequest(r, origin) {
			return false
		}
	default:
		return false
	}
	cookie, err := r.Cookie(sessionCookieName)
	return err == nil && h.validSession(cookie.Value)
}

func (h *DashboardHandler) appV23LocalActorKey(actorID string) (ed25519.PrivateKey, bool) {
	if h.ResolveAgentKeyFn != nil {
		if key, ok := h.ResolveAgentKeyFn(actorID); ok &&
			len(key) == ed25519.PrivateKeySize &&
			agentIDForKey(key) == actorID {
			return key, true
		}
	}
	if len(h.AdminSigningKey) == ed25519.PrivateKeySize &&
		agentIDForKey(h.AdminSigningKey) == actorID {
		return h.AdminSigningKey, true
	}
	return nil, false
}

// appV23ResolveControlActor binds dashboard authority and transaction signing
// to one exact, current principal. A browser vault session acts as current
// Root. A signed request acts as its signer and is accepted only when that
// signer is current Root or an active, current-generation, same-machine Admin.
func (h *DashboardHandler) appV23ResolveControlActor(r *http.Request) (*appV23ControlActor, string, string) {
	if !isLoopbackCEREBRUMRequest(r) || !isLocalRequest(r) {
		return nil, "local_cerebrum_required",
			"CEREBRUM administration is available only from this machine on localhost."
	}
	if h.BadgerStore == nil {
		return nil, "consensus_state_unavailable", "Consensus access-control state is unavailable."
	}
	root, rootKey, broker := h.appV23RootBrokerKey()
	if root == nil {
		return nil, broker.ReasonCode, broker.Message
	}
	if !broker.Available {
		return nil, broker.ReasonCode, broker.Message
	}

	actorID := h.appV23SignedRequestActorID(r)
	if actorID == "" {
		if !h.appV23SessionHasRootAuthority(r) {
			return nil, "current_local_admin_required",
				"Sign this exact request as the current Root or an active same-machine Admin, or unlock the local CEREBRUM vault."
		}
		actorID = root.CredentialID
	}
	actorKey, ok := h.appV23LocalActorKey(actorID)
	if !ok {
		return nil, "local_actor_key_unavailable",
			"The authenticated CEREBRUM principal is not backed by its exact key on this machine."
	}
	actor := &appV23ControlActor{
		ID: actorID, IsRoot: actorID == root.CredentialID,
		Root: root, Key: actorKey, RootKey: rootKey,
	}
	if actor.IsRoot {
		return actor, "", ""
	}
	wasRoot, markerErr := h.BadgerStore.IsAppV23RootCredential(actorID)
	if markerErr != nil {
		return nil, "root_state_unavailable",
			"Could not verify CEREBRUM Root credential history."
	}
	if wasRoot {
		return nil, "stale_root_credential",
			"The authenticated credential is no longer the current CEREBRUM Root."
	}
	enrollment, err := h.BadgerStore.GetAppV23Enrollment(actorID)
	if err != nil {
		return nil, "enrollment_state_unavailable", "Could not load the authenticated Admin enrollment."
	}
	role, err := h.BadgerStore.GetAppV23Role(actorID)
	if err != nil {
		return nil, "role_state_unavailable", "Could not load the authenticated Admin role."
	}
	if enrollment == nil || role == nil || !enrollment.Active ||
		role.Role != store.AppV23RoleAdmin ||
		enrollment.RootGeneration != root.Generation ||
		store.ValidateAppV23Policy(
			role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
		) != nil {
		return nil, "current_local_admin_required",
			"The authenticated principal is not an active current-generation local Admin."
	}
	return actor, "", ""
}

func (h *DashboardHandler) requireAppV23ControlActor(
	w http.ResponseWriter,
	r *http.Request,
	requireRPC bool,
) (*appV23ControlActor, bool) {
	if !h.appV23IsActive() {
		writeAppV23AccessError(w, http.StatusConflict, "app_v23_inactive",
			"Access-control changes require app-v23 activation.")
		return nil, false
	}
	actor, code, message := h.appV23ResolveControlActor(r)
	if actor == nil {
		status := http.StatusForbidden
		switch code {
		case "consensus_state_unavailable", "root_state_unavailable",
			"root_key_unavailable", "local_actor_key_unavailable",
			"enrollment_state_unavailable", "role_state_unavailable":
			status = http.StatusServiceUnavailable
		}
		writeAppV23AccessError(w, status, code, message)
		return nil, false
	}
	if requireRPC && strings.TrimSpace(h.CometBFTRPC) == "" {
		writeAppV23AccessError(w, http.StatusServiceUnavailable, "consensus_rpc_unavailable",
			"The commit-confirmed consensus broadcaster is unavailable.")
		return nil, false
	}
	return actor, true
}

// IsLocalCEREBRUMRequest exposes the canonical localhost boundary to the OAuth
// package without duplicating CEREBRUM's peer/Host/forwarding checks there.
// It answers locality only; callers must separately resolve live authority.
func (h *DashboardHandler) IsLocalCEREBRUMRequest(r *http.Request) bool {
	return isLoopbackCEREBRUMRequest(r) && isLocalRequest(r)
}

// ResolveOAuthControlActor returns the exact current local authority permitted
// to approve creation of a distinct MCP identity. After app-v23 it resolves
// live committed Root/Admin state on every request, so a historical transport
// key or retired Root fails closed. Before app-v23 it preserves the legacy
// exact-operator/encrypted-session rule needed to complete the forced upgrade.
func (h *DashboardHandler) ResolveOAuthControlActor(r *http.Request) (string, bool) {
	if !h.IsLocalCEREBRUMRequest(r) {
		return "", false
	}
	if h.appV23IsActive() {
		actor, _, _ := h.appV23ResolveControlActor(r)
		if actor == nil {
			return "", false
		}
		return actor.ID, true
	}
	ok, _ := h.IsRequestAuthenticated(r)
	if !ok {
		return "", false
	}
	operatorID := strings.TrimSpace(h.NodeOperatorAgentID)
	return operatorID, len(operatorID) == ed25519.PublicKeySize*2
}

func (h *DashboardHandler) appV23AttachElevation(
	ptx *tx.ParsedTx,
	actor *appV23ControlActor,
) error {
	if actor == nil || actor.Root == nil || actor.IsRoot {
		return nil
	}
	if ptx == nil || ptx.LocalElevation != nil {
		return errors.New("invalid app-v23 control action")
	}
	heightBytes, err := h.BadgerStore.GetState("height")
	if err != nil || len(heightBytes) != 8 {
		return errors.New("committed consensus height is unavailable")
	}
	committedHeight := int64(binary.BigEndian.Uint64(heightBytes)) // #nosec G115 -- committed heights are bounded positive int64.
	if committedHeight < 0 {
		return errors.New("committed consensus height is invalid")
	}
	nonce := make([]byte, 16)
	if _, nonceErr := rand.Read(nonce); nonceErr != nil {
		return errors.New("could not generate local elevation nonce")
	}
	proof := &tx.LocalElevationProof{
		RootGeneration:   actor.Root.Generation,
		ValidFromHeight:  committedHeight + 1,
		ValidUntilHeight: committedHeight + 1 + store.AppV23MaxElevationWindow,
		Nonce:            hex.EncodeToString(nonce),
	}
	actionBytes, err := tx.PayloadBytes(ptx)
	if err != nil {
		return errors.New("could not encode the app-v23 control action")
	}
	proof.Signature = ed25519.Sign(
		actor.RootKey,
		tx.AppV23ElevationSignBytes(actor.Root.Scope, actor.ID, ptx.Type, actionBytes, proof),
	)
	ptx.LocalElevation = proof
	return nil
}

func (h *DashboardHandler) signAndBroadcastAppV23Control(
	ptx *tx.ParsedTx,
	actor *appV23ControlActor,
) (string, int64, string, error) {
	return h.signAndBroadcastAppV23ControlContext(context.Background(), ptx, actor)
}

func (h *DashboardHandler) signAndBroadcastAppV23ControlContext(
	ctx context.Context,
	ptx *tx.ParsedTx,
	actor *appV23ControlActor,
) (string, int64, string, error) {
	if err := h.appV23AttachElevation(ptx, actor); err != nil {
		return "", 0, "", err
	}
	return h.signAndBroadcastCommitContext(ctx, ptx, actor.Key)
}

const appV23CommitReconcileTimeout = 2 * time.Second

func waitForAppV23CommittedState(check func() bool) bool {
	deadline := time.NewTimer(appV23CommitReconcileTimeout)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer deadline.Stop()
	defer ticker.Stop()
	for {
		if check() {
			return true
		}
		select {
		case <-deadline.C:
			return false
		case <-ticker.C:
		}
	}
}

func (h *DashboardHandler) appV23PolicyMatches(ptx *tx.ParsedTx) bool {
	if h.BadgerStore == nil || ptx == nil {
		return false
	}
	var agentID, role, profile, homeDomain string
	var clearance uint8
	var capabilities store.AgentCapabilities
	var minimumEnrollmentRevision, minimumRoleRevision uint64
	switch ptx.Type {
	case tx.TxTypeLocalAgentApprove:
		approval := ptx.LocalAgentApprove
		if approval == nil || !approval.Active {
			return false
		}
		agentID, role, profile, homeDomain = approval.AgentID, approval.Role, approval.Profile, approval.HomeDomain
		clearance = approval.Clearance
		capabilities = store.AgentCapabilities(approval.Capabilities)
		minimumEnrollmentRevision = approval.ExpectedRevision + 1
		minimumRoleRevision = approval.ExpectedRoleRevision + 1
	case tx.TxTypeAgentRoleChange:
		change := ptx.AgentRoleChange
		if change == nil {
			return false
		}
		agentID, role, profile = change.AgentID, change.Role, change.Profile
		clearance = change.Clearance
		capabilities = store.AgentCapabilities(change.Capabilities)
		minimumEnrollmentRevision = change.EnrollmentRevision
		minimumRoleRevision = change.ExpectedRevision + 1
	default:
		return false
	}
	enrollment, enrollmentErr := h.BadgerStore.GetAppV23Enrollment(agentID)
	roleState, roleErr := h.BadgerStore.GetAppV23Role(agentID)
	if enrollmentErr != nil || roleErr != nil || enrollment == nil || roleState == nil {
		return false
	}
	if !enrollment.Active || enrollment.Profile != profile || enrollment.Clearance != clearance ||
		enrollment.Capabilities != capabilities || enrollment.Revision < minimumEnrollmentRevision ||
		roleState.Role != role || roleState.Revision < minimumRoleRevision {
		return false
	}
	return ptx.Type != tx.TxTypeLocalAgentApprove || enrollment.HomeDomain == homeDomain
}

func (h *DashboardHandler) appV23GroupMatches(mutation *tx.AccessGroupMutate) bool {
	if h.BadgerStore == nil || mutation == nil {
		return false
	}
	group, err := h.BadgerStore.GetAppV23AccessGroup(mutation.GroupID)
	if err != nil {
		return false
	}
	if mutation.Delete {
		return group == nil
	}
	if group == nil || group.Name != mutation.Name || group.MemberAuthority != mutation.MemberAuthority ||
		group.Revision < mutation.ExpectedRevision+1 ||
		len(group.Members) != len(mutation.Members) {
		return false
	}
	for i := range group.Members {
		if group.Members[i] != mutation.Members[i] {
			return false
		}
	}
	return true
}

func writeAppV23CommitUnconfirmed(w http.ResponseWriter, action string) {
	writeJSONResp(w, http.StatusAccepted, map[string]any{
		"ok":        false,
		"status":    "confirmation_pending",
		"code":      "consensus_commit_unconfirmed",
		"retryable": false,
		"error":     "The " + action + " may already be committed. CEREBRUM will refresh consensus state; do not submit it again until the current state is shown.",
	})
}

// handleAppV23AccessState returns the consensus-authoritative Access Group and
// enrollment projection used by CEREBRUM. It intentionally does not read or
// import the browser's historical localStorage groups.
func (h *DashboardHandler) handleAppV23AccessState(agentStore store.AgentStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// This response is live consensus authority state. Browser/proxy caching
		// can otherwise resurrect a pre-commit roster after rename, approval, or
		// group changes and tempt the operator into submitting a duplicate.
		w.Header().Set("Cache-Control", "no-store")
		if h.BadgerStore == nil {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "consensus_state_unavailable",
				"Consensus access-control state is unavailable.")
			return
		}
		groups, err := h.BadgerStore.ListAppV23AccessGroups()
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "group_state_unavailable",
				"Could not load consensus access groups.")
			return
		}
		root, _, broker := h.appV23RootBrokerKey()
		if root == nil && h.appV23IsActive() {
			writeAppV23AccessError(w, http.StatusServiceUnavailable, "root_state_unavailable",
				"Committed CEREBRUM Root state is unavailable.")
			return
		}
		if root == nil && broker.ReasonCode != "root_state_unavailable" {
			writeAppV23AccessError(w, http.StatusInternalServerError, broker.ReasonCode, broker.Message)
			return
		}
		sqlAgents, err := agentStore.ListAgents(r.Context())
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "agent_projection_unavailable",
				"Could not load local agent metadata.")
			return
		}
		metadata := make(map[string]*store.AgentEntry, len(sqlAgents))
		for _, agent := range sqlAgents {
			if agent != nil {
				metadata[agent.AgentID] = agent
			}
		}
		onChainAgents, err := h.BadgerStore.ListRegisteredAgents()
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "agent_state_unavailable",
				"Could not load consensus agent state.")
			return
		}
		views := make([]appV23AgentAccessView, 0, len(onChainAgents))
		for _, agent := range onChainAgents {
			if h.appV23IsRootIdentity(agent.AgentID) {
				continue
			}
			view := appV23AgentAccessView{
				AgentID:        agent.AgentID,
				Name:           agent.Name,
				RegisteredName: agent.RegisteredName,
				Role:           agent.Role,
				Clearance:      agent.Clearance,
				Capabilities:   uint32(agent.Capabilities),
			}
			if local := metadata[agent.AgentID]; local != nil {
				// Consensus is authoritative for a governed display-name once it
				// exists.  The SQLite row is only a compatibility fallback for
				// older registrations whose on-chain name is empty.  Letting a
				// stale local projection override a committed rename makes the
				// operator think the transaction was lost and invites a duplicate
				// submission after a projection write/restart failure.
				if view.Name == "" && local.Name != "" {
					view.Name = local.Name
				}
				if view.RegisteredName == "" {
					view.RegisteredName = local.RegisteredName
				}
				view.Avatar = local.Avatar
				view.Status = local.Status
				view.Provider = local.Provider
			}
			if h.ResolveAgentKeyFn != nil {
				if key, ok := h.ResolveAgentKeyFn(agent.AgentID); ok {
					view.LocalKeyAvailable = len(key) == ed25519.PrivateKeySize &&
						agentIDForKey(key) == agent.AgentID
				}
			}
			enrollment, enrollmentErr := h.BadgerStore.GetAppV23Enrollment(agent.AgentID)
			if enrollmentErr != nil {
				writeAppV23AccessError(w, http.StatusInternalServerError, "enrollment_state_unavailable",
					"Could not load local enrollment state.")
				return
			}
			// A Root-rejected pending identity remains in immutable chain history,
			// but its removed node-local projection keeps it out of the actionable
			// review queue. A later signed registration request restores that
			// projection and makes the identity visible for a fresh review.
			local := metadata[agent.AgentID]
			if local == nil && (enrollment == nil || !enrollment.Active) {
				// ListAgents intentionally excludes removed rows. Consult the exact
				// identity only for an already non-active consensus principal so a
				// rejected pending registration does not reappear merely because the
				// broad local roster correctly hid its removed projection.
				if exact, exactErr := agentStore.GetAgent(r.Context(), agent.AgentID); exactErr == nil {
					local = exact
				}
			}
			if local != nil &&
				local.Status == "removed" && (enrollment == nil || !enrollment.Active) {
				continue
			}
			role, roleErr := h.BadgerStore.GetAppV23Role(agent.AgentID)
			if roleErr != nil {
				writeAppV23AccessError(w, http.StatusInternalServerError, "role_state_unavailable",
					"Could not load local role state.")
				return
			}
			if enrollment != nil {
				view.Profile = enrollment.Profile
				view.HomeDomain = enrollment.HomeDomain
				view.Clearance = enrollment.Clearance
				view.Capabilities = uint32(enrollment.Capabilities)
				view.EnrollmentActive = enrollment.Active
				view.EnrollmentRevision = enrollment.Revision
			}
			if role != nil {
				view.Role = role.Role
				view.RoleRevision = role.Revision
			}
			view.NeedsReauthorization = enrollment != nil && enrollment.Active &&
				role != nil && role.Role == store.AppV23RoleAdmin &&
				root != nil && enrollment.RootGeneration != root.Generation
			// A delegated Admin is intentionally suspended by Root handover.
			// Project that effective state instead of misleading CEREBRUM with
			// the raw persisted Active bit from the previous Root generation.
			if view.NeedsReauthorization {
				view.EnrollmentActive = false
			}
			view.NeedsApproval = enrollment == nil || !enrollment.Active ||
				view.NeedsReauthorization
			views = append(views, view)
		}
		sort.Slice(views, func(i, j int) bool {
			return views[i].AgentID < views[j].AgentID
		})
		linkedReaders := appV23LinkedReadersView{
			Status:     "unavailable",
			ReasonCode: "linked_readers_api_unavailable",
			Message:    "Federated Linked readers are unavailable until the node-local app-v23 guest manager is active.",
		}
		if _, ok := h.Federation.(appV23FederatedGuestControl); ok {
			linkedReaders = appV23LinkedReadersView{
				Status:     "ready",
				ReasonCode: "linked_readers_ready",
				Message:    "Linked readers are node-local read-only capabilities bound to the active federation agreement.",
			}
		}
		linkedMessages := appV23LinkedMessagesView{
			Status:     "unavailable",
			ReasonCode: "linked_messages_api_unavailable",
			Message:    "Exact linked-agent messaging consent is unavailable in this build.",
		}
		if _, ok := h.Federation.(appV23LinkedMessageConsentControl); ok {
			linkedMessages = appV23LinkedMessagesView{
				Status:     "ready",
				ReasonCode: "linked_messages_ready",
				Message:    "Messaging starts blocked and requires receiver-local consent for one exact remote-to-local agent pair.",
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok":                     true,
			"active":                 h.appV23IsActive(),
			"group_authority_active": h.appV26IsActive(),
			"root":                   root,
			"broker":                 broker,
			"agents":                 views,
			"groups":                 groups,
			"profiles": []string{
				store.AppV23ProfileStandard,
				store.AppV23ProfileCompanion,
				store.AppV23ProfileReadOnly,
			},
			"roles": []string{
				store.AppV23RoleMember,
				store.AppV23RoleManager,
				store.AppV23RoleAdmin,
			},
			"linked_readers":  linkedReaders,
			"linked_messages": linkedMessages,
		})
	}
}

func (h *DashboardHandler) handleAppV23AgentPolicy() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
		root := actor.Root
		agentID := strings.TrimSpace(chi.URLParam(r, "id"))
		if _, err := auth.AgentIDToPublicKey(agentID); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_agent_id",
				"Agent ID must be canonical lowercase Ed25519 hex.")
			return
		}
		if h.appV23IsRootIdentity(agentID) {
			writeAppV23AccessError(w, http.StatusForbidden, "root_policy_immutable",
				"CEREBRUM Root is sovereign and cannot be edited as an agent role.")
			return
		}
		var req appV23PolicyRequest
		if err := decodeAppV23AccessJSON(w, r, &req); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		req.Role = strings.TrimSpace(strings.ToLower(req.Role))
		req.Profile = strings.TrimSpace(strings.ToLower(req.Profile))
		req.HomeDomain = strings.TrimSpace(req.HomeDomain)
		if req.Profile == appV23LegacyRestrictedProfile {
			writeAppV23AccessError(w, http.StatusBadRequest, "legacy_profile_migration_only",
				"Legacy restrictions are migration-only. Select Standard, Companion, or Read-only to replace them.")
			return
		}
		capabilities := store.AgentCapabilities(req.Capabilities)
		if err := store.ValidateAppV23Policy(req.Role, req.Profile, capabilities, req.Clearance); err != nil {
			code := "invalid_policy"
			if req.Role == store.AppV23RoleAdmin {
				code = "invalid_admin_policy"
			}
			writeAppV23AccessError(w, http.StatusBadRequest, code, err.Error())
			return
		}
		if req.Profile == store.AppV23ProfileRoot {
			writeAppV23AccessError(w, http.StatusForbidden, "root_profile_reserved",
				"The Root profile cannot be delegated.")
			return
		}
		if req.Role == store.AppV23RoleAdmin &&
			(req.Profile != store.AppV23ProfileStandard ||
				req.Clearance != 4 || req.Capabilities != uint32(store.AgentCapabilityReadAllDomains)) {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_admin_policy",
				"Admin requires the Standard profile, Top Secret clearance, and exactly the Read-all capability.")
			return
		}
		if req.Role == store.AppV23RoleAdmin {
			if h.ResolveAgentKeyFn == nil {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "admin_requires_local_key",
					"Only an agent whose exact key is held on this machine can be promoted to Admin.")
				return
			}
			targetKey, targetOK := h.ResolveAgentKeyFn(agentID)
			if !targetOK || len(targetKey) != ed25519.PrivateKeySize || agentIDForKey(targetKey) != agentID {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "admin_requires_local_key",
					"Only an agent whose exact key is held on this machine can be promoted to Admin.")
				return
			}
		}
		enrollment, err := h.BadgerStore.GetAppV23Enrollment(agentID)
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "enrollment_state_unavailable",
				"Could not load the current enrollment.")
			return
		}
		roleState, err := h.BadgerStore.GetAppV23Role(agentID)
		if err != nil {
			writeAppV23AccessError(w, http.StatusInternalServerError, "role_state_unavailable",
				"Could not load the current role.")
			return
		}

		var ptx *tx.ParsedTx
		mode := "update"
		homeReapproval := appV23PolicyNeedsHomeReapproval(enrollment, req.Profile)
		generationReauthorization := enrollment != nil && enrollment.Active &&
			roleState != nil && roleState.Role == store.AppV23RoleAdmin &&
			enrollment.RootGeneration != root.Generation
		if enrollment == nil || !enrollment.Active || homeReapproval ||
			generationReauthorization {
			if generationReauthorization {
				mode = "reauthorize"
			} else if homeReapproval {
				mode = "reapprove"
			} else {
				mode = "approve"
			}
			if req.Profile != store.AppV23ProfileReadOnly &&
				(req.HomeDomain == "" || store.IsSharedDomainName(req.HomeDomain)) {
				writeAppV23AccessError(w, http.StatusBadRequest, "home_domain_required",
					"Approval or migration-profile review requires a non-shared home domain owned by this local agent.")
				return
			}
			if h.ResolveAgentKeyFn == nil {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "target_key_unavailable",
					"The local agent key is required to consent to approval or replace a domainless migration profile.")
				return
			}
			targetKey, targetOK := h.ResolveAgentKeyFn(agentID)
			if !targetOK || len(targetKey) != ed25519.PrivateKeySize || agentIDForKey(targetKey) != agentID {
				writeAppV23AccessError(w, http.StatusServiceUnavailable, "target_key_unavailable",
					"The local agent key is required to consent to approval or replace a domainless migration profile.")
				return
			}
			approval := &tx.LocalAgentApprove{
				AgentID:          agentID,
				Active:           true,
				Role:             req.Role,
				Profile:          req.Profile,
				HomeDomain:       req.HomeDomain,
				Clearance:        req.Clearance,
				Capabilities:     req.Capabilities,
				Scope:            root.Scope,
				ExpectedRevision: 0,
			}
			if enrollment != nil {
				approval.ExpectedRevision = enrollment.Revision
			}
			if roleState != nil {
				approval.ExpectedRoleRevision = roleState.Revision
			}
			approval.TargetSignature = ed25519.Sign(
				targetKey,
				tx.LocalAgentApprovalSignBytes(actor.ID, approval),
			)
			ptx = &tx.ParsedTx{Type: tx.TxTypeLocalAgentApprove, LocalAgentApprove: approval}
		} else {
			if roleState == nil {
				writeAppV23AccessError(w, http.StatusConflict, "role_state_missing",
					"The active enrollment has no committed role state.")
				return
			}
			if req.HomeDomain != "" && req.HomeDomain != enrollment.HomeDomain {
				writeAppV23AccessError(w, http.StatusConflict, "home_domain_change_requires_reapproval",
					"An active agent's owned home domain cannot be changed by a role update.")
				return
			}
			ptx = &tx.ParsedTx{
				Type: tx.TxTypeAgentRoleChange,
				AgentRoleChange: &tx.AgentRoleChange{
					AgentID:            agentID,
					ExpectedRevision:   roleState.Revision,
					EnrollmentRevision: enrollment.Revision,
					Role:               req.Role,
					ExpectedProfile:    enrollment.Profile,
					Profile:            req.Profile,
					Clearance:          req.Clearance,
					Capabilities:       req.Capabilities,
				},
			}
		}
		hash, height, _, err := h.signAndBroadcastAppV23ControlContext(r.Context(), ptx, actor)
		reconciled := false
		if err != nil {
			if isIndeterminateCommitError(err) {
				reconciled = waitForAppV23CommittedState(func() bool {
					return h.appV23PolicyMatches(ptx)
				})
				if !reconciled {
					writeAppV23CommitUnconfirmed(w, "agent policy update")
					return
				}
			} else {
				writeAppV23AccessError(w, http.StatusConflict, "consensus_rejected",
					"The consensus policy update was not committed.")
				return
			}
		}
		projectionReady := true
		projectionWarning := ""
		if ptx.Type == tx.TxTypeLocalAgentApprove {
			projectionReady = false
			agentStore, ok := h.store.(store.AgentStore)
			if !ok {
				projectionWarning = "The agent was approved, but its local Agents view projection is not available yet."
			} else {
				projectionCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				_, projectionErr := store.EnsureAppV23AgentProjection(
					projectionCtx, agentStore, h.BadgerStore, agentID, nil,
				)
				cancel()
				if projectionErr == nil {
					projectionReady = true
				} else {
					projectionWarning = "The agent was approved on-chain, but its local Agents view is still being repaired. Do not approve it again."
				}
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok":                 true,
			"mode":               mode,
			"tx_type":            ptx.Type,
			"tx_hash":            hash,
			"height":             height,
			"reconciled":         reconciled,
			"projection_ready":   projectionReady,
			"projection_warning": projectionWarning,
		})
	}
}

func canonicalAppV23GroupMembers(
	badgerStore *store.BadgerStore,
	root *store.AppV23RootState,
	members []string,
) ([]string, error) {
	canonical := append([]string(nil), members...)
	sort.Strings(canonical)
	for i, member := range canonical {
		if _, err := auth.AgentIDToPublicKey(member); err != nil {
			return nil, fmt.Errorf("member %q is not a canonical local agent ID", member)
		}
		wasRoot, markerErr := badgerStore.IsAppV23RootCredential(member)
		if markerErr != nil {
			return nil, errors.New("CEREBRUM Root credential history is unavailable")
		}
		if wasRoot || (root != nil && (member == root.PrincipalID || member == root.CredentialID)) {
			return nil, errors.New("CEREBRUM Root cannot be placed in an Access Group")
		}
		if i > 0 && canonical[i-1] == member {
			return nil, fmt.Errorf("member %q is duplicated", member)
		}
		enrollment, err := badgerStore.GetAppV23Enrollment(member)
		if err != nil || enrollment == nil || !enrollment.Active {
			return nil, fmt.Errorf("member %q does not have active local approval", member)
		}
		role, roleErr := badgerStore.GetAppV23Role(member)
		if roleErr != nil || role == nil {
			return nil, fmt.Errorf("member %q does not have committed local role state", member)
		}
		if root != nil && role.Role == store.AppV23RoleAdmin &&
			enrollment.RootGeneration != root.Generation {
			return nil, fmt.Errorf("member %q is suspended until the current CEREBRUM Root reauthorizes it", member)
		}
	}
	return canonical, nil
}

func validAppV23GroupID(groupID string) bool {
	if len(groupID) == 0 || len(groupID) > 64 {
		return false
	}
	for i := range groupID {
		c := groupID[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') &&
			c != '-' && c != '_' && c != '.' {
			return false
		}
	}
	return true
}

func (h *DashboardHandler) handleAppV23AccessGroupPut() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
		root := actor.Root
		groupID := strings.TrimSpace(chi.URLParam(r, "groupID"))
		if !validAppV23GroupID(groupID) {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_group_id",
				"Group ID must be 1–64 lowercase letters, digits, dots, dashes, or underscores.")
			return
		}
		var req appV23GroupRequest
		if err := decodeAppV23AccessJSON(w, r, &req); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		req.Name = strings.TrimSpace(req.Name)
		req.MemberAuthority = strings.TrimSpace(strings.ToLower(req.MemberAuthority))
		if req.Name == "" || len(req.Name) > 128 {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_group_name",
				"Group name must be 1–128 characters.")
			return
		}
		if h.appV26IsActive() {
			if err := store.ValidateAppV26GroupAuthority(req.MemberAuthority); err != nil {
				writeAppV23AccessError(w, http.StatusBadRequest, "invalid_group_authority", err.Error())
				return
			}
		} else if req.MemberAuthority != "" {
			writeAppV23AccessError(w, http.StatusConflict, "app_v26_inactive",
				"Group member authority becomes editable after governed app-v26 activation.")
			return
		}
		members, err := canonicalAppV23GroupMembers(h.BadgerStore, root, req.Members)
		if err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_group_members", err.Error())
			return
		}
		ptx := &tx.ParsedTx{
			Type: tx.TxTypeAccessGroupMutate,
			AccessGroupMutate: &tx.AccessGroupMutate{
				GroupID:          groupID,
				Name:             req.Name,
				MemberAuthority:  req.MemberAuthority,
				ExpectedRevision: req.ExpectedRevision,
				Members:          members,
			},
		}
		hash, height, _, err := h.signAndBroadcastAppV23ControlContext(r.Context(), ptx, actor)
		reconciled := false
		if err != nil {
			if isIndeterminateCommitError(err) {
				reconciled = waitForAppV23CommittedState(func() bool {
					return h.appV23GroupMatches(ptx.AccessGroupMutate)
				})
				if !reconciled {
					writeAppV23CommitUnconfirmed(w, "Access Group update")
					return
				}
			} else {
				writeAppV23AccessError(w, http.StatusConflict, "consensus_rejected",
					"The consensus group update was not committed.")
				return
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "tx_type": ptx.Type, "tx_hash": hash, "height": height,
			"reconciled": reconciled,
		})
	}
}

func (h *DashboardHandler) handleAppV23AccessGroupDelete() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, true)
		if !ok {
			return
		}
		groupID := strings.TrimSpace(chi.URLParam(r, "groupID"))
		if !validAppV23GroupID(groupID) {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_group_id",
				"Group ID must be 1–64 lowercase letters, digits, dots, dashes, or underscores.")
			return
		}
		var req appV23GroupDeleteRequest
		if err := decodeAppV23AccessJSON(w, r, &req); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		ptx := &tx.ParsedTx{
			Type: tx.TxTypeAccessGroupMutate,
			AccessGroupMutate: &tx.AccessGroupMutate{
				GroupID:          groupID,
				ExpectedRevision: req.ExpectedRevision,
				Delete:           true,
			},
		}
		hash, height, _, err := h.signAndBroadcastAppV23ControlContext(r.Context(), ptx, actor)
		reconciled := false
		if err != nil {
			if isIndeterminateCommitError(err) {
				reconciled = waitForAppV23CommittedState(func() bool {
					return h.appV23GroupMatches(ptx.AccessGroupMutate)
				})
				if !reconciled {
					writeAppV23CommitUnconfirmed(w, "Access Group deletion")
					return
				}
			} else {
				writeAppV23AccessError(w, http.StatusConflict, "consensus_rejected",
					"The consensus group deletion was not committed.")
				return
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "tx_type": ptx.Type, "tx_hash": hash, "height": height,
			"reconciled": reconciled,
		})
	}
}

func (h *DashboardHandler) appV23FederatedGuestDriver(
	w http.ResponseWriter,
) (appV23FederatedGuestControl, bool) {
	driver, ok := h.Federation.(appV23FederatedGuestControl)
	if !ok {
		writeAppV23AccessError(w, http.StatusNotImplemented, "linked_readers_api_unavailable",
			"The node-local app-v23 Linked readers manager is unavailable.")
	}
	return driver, ok
}

func (h *DashboardHandler) handleAppV23LinkedReadersList() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.appV23IsActive() {
			writeAppV23AccessError(w, http.StatusConflict, "app_v23_inactive",
				"Linked readers require app-v23 activation.")
			return
		}
		driver, ok := h.appV23FederatedGuestDriver(w)
		if !ok {
			return
		}
		actor, ok := h.requireAppV23ControlActor(w, r, false)
		if !ok {
			return
		}
		remoteChainID := strings.TrimSpace(r.URL.Query().Get("remote_chain_id"))
		remoteAgentID := strings.TrimSpace(r.URL.Query().Get("remote_agent_id"))
		if remoteChainID == "" && remoteAgentID == "" {
			identities, err := driver.ListFederatedGuestIdentities(
				r.Context(), actor.ID,
			)
			if err != nil {
				writeAppV23AccessError(w, http.StatusConflict, "linked_reader_inventory_denied",
					"The existing Linked-reader identity inventory could not be loaded.")
				return
			}
			// New attach/rebind operations enter this inventory only after the
			// remote peer's exact-ID eligibility oracle excludes every Root
			// generation. Existing durable rows remain visible even while the
			// peer is offline so the operator can pause or revoke them.
			if identities == nil {
				identities = []store.FederatedGuestIdentity{}
			}
			writeJSONResp(w, http.StatusOK, map[string]any{
				"ok": true, "identities": identities, "total": len(identities),
			})
			return
		}
		if remoteChainID == "" {
			writeAppV23AccessError(w, http.StatusBadRequest, "remote_chain_required",
				"A detected remote chain is required.")
			return
		}
		if _, err := auth.AgentIDToPublicKey(remoteAgentID); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_remote_agent_id",
				"A detected canonical remote Agent ID is required.")
			return
		}
		links, err := driver.ListFederatedGuestLinks(
			r.Context(), actor.ID, remoteChainID, remoteAgentID,
		)
		if err != nil {
			writeAppV23AccessError(w, http.StatusConflict, "linked_reader_list_denied",
				"The linked-reader list request was denied.")
			return
		}
		if links == nil {
			links = []federation.FederatedGuestLinkView{}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "links": links, "total": len(links),
		})
	}
}

func (h *DashboardHandler) handleAppV23LinkedReaderEligibility() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !h.appV23IsActive() {
			writeAppV23AccessError(w, http.StatusConflict, "app_v23_inactive",
				"Linked readers require app-v23 activation.")
			return
		}
		if _, ok := h.requireAppV23ControlActor(w, r, false); !ok {
			return
		}
		checker, ok := h.Federation.(appV23FederatedGuestEligibility)
		if !ok {
			writeAppV23AccessError(w, http.StatusNotImplemented,
				"linked_reader_eligibility_unavailable",
				"The live peer agent eligibility check is unavailable.")
			return
		}
		var input appV23FederatedGuestEligibilityRequest
		if err := decodeAppV23AccessJSON(w, r, &input); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.RemoteChainID = strings.TrimSpace(input.RemoteChainID)
		input.RemoteAgentID = strings.TrimSpace(input.RemoteAgentID)
		if input.RemoteChainID == "" {
			writeAppV23AccessError(w, http.StatusBadRequest, "remote_chain_required",
				"An active remote chain is required.")
			return
		}
		if _, err := auth.AgentIDToPublicKey(input.RemoteAgentID); err != nil ||
			input.RemoteAgentID != strings.ToLower(input.RemoteAgentID) {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_remote_agent_id",
				"A canonical lowercase remote Agent ID is required.")
			return
		}
		if err := checker.CheckRemoteFederatedGuestAgentEligibility(
			r.Context(), input.RemoteChainID, input.RemoteAgentID,
		); err != nil {
			writeAppV23AccessError(w, http.StatusConflict,
				"linked_reader_agent_ineligible",
				"The peer did not confirm this exact identity as an active ordinary agent.")
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "eligible": true,
			"remote_chain_id": input.RemoteChainID,
			"remote_agent_id": input.RemoteAgentID,
		})
	}
}

func (h *DashboardHandler) handleAppV23LinkedReaderMutation() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, false)
		if !ok {
			return
		}
		driver, ok := h.appV23FederatedGuestDriver(w)
		if !ok {
			return
		}
		var input federation.FederatedGuestMutationInput
		if err := decodeAppV23AccessJSON(w, r, &input); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.Operation = strings.TrimSpace(strings.ToLower(input.Operation))
		input.GroupID = strings.TrimSpace(input.GroupID)
		input.RemoteChainID = strings.TrimSpace(input.RemoteChainID)
		input.RemoteAgentID = strings.TrimSpace(input.RemoteAgentID)
		prepared, err := driver.PrepareFederatedGuestMutation(
			r.Context(), actor.ID, input,
		)
		if err != nil {
			status := http.StatusConflict
			if errors.Is(err, store.ErrAppV23RevisionConflict) {
				status = http.StatusConflict
			}
			writeAppV23AccessError(w, status, "linked_reader_prepare_denied",
				"The linked-reader mutation could not be prepared.")
			return
		}
		if prepared == nil || len(prepared.SigningBytes) == 0 ||
			prepared.Guest.AuthorizedBy != actor.ID {
			writeAppV23AccessError(w, http.StatusInternalServerError, "linked_reader_prepare_invalid",
				"The federation manager returned an invalid actor-bound mutation.")
			return
		}
		prepared.Guest.Signature = ed25519.Sign(actor.Key, prepared.SigningBytes)
		mutation := federation.FederatedGuestMutation{
			Operation:        prepared.Operation,
			ExpectedRevision: prepared.ExpectedRevision,
			Guest:            prepared.Guest,
		}
		if !actor.IsRoot {
			elevation, elevationErr := driver.MintFederatedGuestElevation(
				r.Context(), actor.ID, mutation,
			)
			if elevationErr != nil {
				writeAppV23AccessError(w, http.StatusForbidden, "linked_reader_elevation_denied",
					"The current Root did not authorize this exact Admin action.")
				return
			}
			mutation.Elevation = elevation
		}
		guest, err := driver.CommitFederatedGuestMutation(
			r.Context(), actor.ID, mutation,
		)
		if err != nil {
			status := http.StatusConflict
			if errors.Is(err, store.ErrAppV23RevisionConflict) {
				status = http.StatusConflict
			}
			writeAppV23AccessError(w, status, "linked_reader_commit_denied",
				"The linked-reader mutation was not committed.")
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "guest": guest,
		})
	}
}

func (h *DashboardHandler) appV23LinkedMessageConsentDriver(
	w http.ResponseWriter,
) (appV23LinkedMessageConsentControl, bool) {
	driver, ok := h.Federation.(appV23LinkedMessageConsentControl)
	if !ok {
		writeAppV23AccessError(w, http.StatusNotImplemented, "linked_messages_api_unavailable",
			"The exact linked-agent messaging consent manager is unavailable.")
	}
	return driver, ok
}

func validAppV23LinkedMessageTupleInput(
	remoteChainID, remoteAgentID, localAgentID string,
) bool {
	if len(remoteChainID) == 0 || len(remoteChainID) > 50 ||
		federation.ValidateChainID(remoteChainID) != nil {
		return false
	}
	for _, agentID := range []string{remoteAgentID, localAgentID} {
		if agentID != strings.ToLower(agentID) {
			return false
		}
		if _, err := auth.AgentIDToPublicKey(agentID); err != nil {
			return false
		}
	}
	return true
}

func validAppV23LinkedMessageCandidateInput(remoteChainID, localAgentID string) bool {
	if len(remoteChainID) == 0 || len(remoteChainID) > 50 ||
		federation.ValidateChainID(remoteChainID) != nil ||
		localAgentID != strings.ToLower(localAgentID) {
		return false
	}
	_, err := auth.AgentIDToPublicKey(localAgentID)
	return err == nil
}

// appV23LinkedMessageTupleAvailable proves that this control represents a
// current exact linked-reader relationship, not an arbitrary advertised
// identity. The local node may host the group (guest -> member), or the peer
// may return a bounded host-signed offer for its member -> this node's local
// guest. Neither projection carries domains, names, contacts, or roster
// authority; messaging consent remains a separate default-off row.
func (h *DashboardHandler) appV23LinkedMessageTupleAvailable(
	ctx context.Context,
	driver appV23LinkedMessageConsentControl,
	actorID, remoteChainID, remoteAgentID, localAgentID string,
) bool {
	if !validAppV23LinkedMessageTupleInput(
		remoteChainID, remoteAgentID, localAgentID,
	) {
		return false
	}
	hosted, err := driver.ListLinkedMessageConsentCandidates(
		ctx, actorID, remoteChainID, remoteAgentID,
	)
	if err == nil {
		for _, candidate := range hosted {
			if candidate.RemoteChainID == remoteChainID &&
				candidate.RemoteAgentID == remoteAgentID &&
				candidate.LocalAgentID == localAgentID {
				return true
			}
		}
	}
	remoteHosted, err := driver.ListRemoteHostedLinkedMessageConsentCandidates(
		ctx, actorID, remoteChainID, localAgentID,
	)
	if err == nil {
		for _, candidate := range remoteHosted {
			if candidate.RemoteChainID == remoteChainID &&
				candidate.RemoteAgentID == remoteAgentID &&
				candidate.LocalAgentID == localAgentID {
				return true
			}
		}
	}
	return false
}

func appV23LinkedMessageConsentProjection(
	remoteChainID, remoteAgentID, localAgentID string,
	consent *store.FederatedLinkedMessageConsent,
) appV23LinkedMessageConsentView {
	view := appV23LinkedMessageConsentView{
		RemoteChainID: remoteChainID,
		RemoteAgentID: remoteAgentID,
		LocalAgentID:  localAgentID,
	}
	if consent != nil {
		view.Revision = consent.Revision
		view.Accepting = consent.Accepting
	}
	return view
}

// handleAppV23RemoteHostedLinkedMessageCandidates exposes only the bounded,
// host-signed member->local-guest offers needed by the receiver's CEREBRUM UI.
// It is not a directory or roster endpoint and returns no names or domains.
func (h *DashboardHandler) handleAppV23RemoteHostedLinkedMessageCandidates() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, false)
		if !ok {
			return
		}
		driver, ok := h.appV23LinkedMessageConsentDriver(w)
		if !ok {
			return
		}
		remoteChainID := strings.TrimSpace(r.URL.Query().Get("remote_chain_id"))
		localAgentID := strings.TrimSpace(r.URL.Query().Get("local_agent_id"))
		if !validAppV23LinkedMessageCandidateInput(remoteChainID, localAgentID) {
			writeAppV23AccessError(w, http.StatusNotFound, "linked_message_candidates_unavailable",
				"No exact peer-hosted messaging candidates are available.")
			return
		}
		candidates, err := driver.ListRemoteHostedLinkedMessageConsentCandidates(
			r.Context(), actor.ID, remoteChainID, localAgentID,
		)
		if err != nil || len(candidates) > federation.MaxLinkedMessageConsentCandidates {
			writeAppV23AccessError(w, http.StatusConflict, "linked_message_candidates_unavailable",
				"The exact peer-hosted messaging candidates could not be loaded.")
			return
		}
		filtered := make([]federation.LinkedMessageConsentCandidate, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate.RemoteChainID == remoteChainID &&
				candidate.LocalAgentID == localAgentID {
				filtered = append(filtered, candidate)
			}
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true, "candidates": filtered, "total": len(filtered),
		})
	}
}

func (h *DashboardHandler) handleAppV23LinkedMessageConsentGet() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, false)
		if !ok {
			return
		}
		consentDriver, ok := h.appV23LinkedMessageConsentDriver(w)
		if !ok {
			return
		}
		remoteChainID := strings.TrimSpace(r.URL.Query().Get("remote_chain_id"))
		remoteAgentID := strings.TrimSpace(r.URL.Query().Get("remote_agent_id"))
		localAgentID := strings.TrimSpace(r.URL.Query().Get("local_agent_id"))
		if !h.appV23LinkedMessageTupleAvailable(
			r.Context(), consentDriver, actor.ID,
			remoteChainID, remoteAgentID, localAgentID,
		) {
			writeAppV23AccessError(w, http.StatusNotFound, "linked_message_tuple_unavailable",
				"That exact linked-agent messaging tuple is unavailable.")
			return
		}
		consent, err := consentDriver.GetLinkedMessageConsent(
			r.Context(), remoteChainID, remoteAgentID, localAgentID,
		)
		if err != nil {
			writeAppV23AccessError(w, http.StatusConflict, "linked_message_consent_unavailable",
				"The exact receiver-local messaging consent could not be loaded.")
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true,
			"consent": appV23LinkedMessageConsentProjection(
				remoteChainID, remoteAgentID, localAgentID, consent,
			),
		})
	}
}

func (h *DashboardHandler) handleAppV23LinkedMessageConsentPut() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		actor, ok := h.requireAppV23ControlActor(w, r, false)
		if !ok {
			return
		}
		consentDriver, ok := h.appV23LinkedMessageConsentDriver(w)
		if !ok {
			return
		}
		var input appV23LinkedMessageConsentRequest
		if err := decodeAppV23AccessJSON(w, r, &input); err != nil {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request", err.Error())
			return
		}
		input.RemoteChainID = strings.TrimSpace(input.RemoteChainID)
		input.RemoteAgentID = strings.TrimSpace(input.RemoteAgentID)
		input.LocalAgentID = strings.TrimSpace(input.LocalAgentID)
		if input.Accepting == nil || input.ExpectedRevision < 0 {
			writeAppV23AccessError(w, http.StatusBadRequest, "invalid_request",
				"accepting and a non-negative expected_revision are required.")
			return
		}
		if !h.appV23LinkedMessageTupleAvailable(
			r.Context(), consentDriver, actor.ID,
			input.RemoteChainID, input.RemoteAgentID, input.LocalAgentID,
		) {
			writeAppV23AccessError(w, http.StatusNotFound, "linked_message_tuple_unavailable",
				"That exact linked-agent messaging tuple is unavailable.")
			return
		}
		revision, err := consentDriver.SetLinkedMessageConsentCAS(
			r.Context(),
			input.RemoteChainID, input.RemoteAgentID, input.LocalAgentID,
			input.ExpectedRevision, *input.Accepting,
		)
		if err != nil {
			if errors.Is(err, store.ErrLinkedMessageConsentConflict) {
				writeAppV23AccessError(w, http.StatusConflict, "linked_message_consent_conflict",
					"Messaging consent changed. Reload its current state before trying again.")
				return
			}
			if errors.Is(err, federation.ErrRemotePipeTargetNotFound) {
				writeAppV23AccessError(w, http.StatusNotFound, "linked_message_tuple_unavailable",
					"That exact linked-agent messaging tuple is unavailable.")
				return
			}
			writeAppV23AccessError(w, http.StatusConflict, "linked_message_consent_denied",
				"The exact receiver-local messaging consent was not changed.")
			return
		}
		writeJSONResp(w, http.StatusOK, map[string]any{
			"ok": true,
			"consent": appV23LinkedMessageConsentView{
				RemoteChainID: input.RemoteChainID,
				RemoteAgentID: input.RemoteAgentID,
				LocalAgentID:  input.LocalAgentID,
				Revision:      revision,
				Accepting:     *input.Accepting,
			},
		})
	}
}
