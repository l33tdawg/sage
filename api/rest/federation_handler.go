package rest

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// maxRemoteChainIDLenREST mirrors the consensus maxRemoteChainIDLen so the JOIN
// builder fails fast instead of after a wasted stage+broadcast.
const maxRemoteChainIDLenREST = 50

// requireNodeOperator gates an off-consensus federation action that is performed
// with the node OPERATOR's key (outbound signed calls) and has no other authz
// (unlike the cross_fed set/revoke txs, which are authorized on-chain). Fails
// closed when no operator id is configured. Mirrors the federated-recall gate.
func (s *Server) requireNodeOperator(w http.ResponseWriter, r *http.Request) bool {
	callerID := middleware.ContextAgentID(r.Context())
	if s.nodeOperatorID == "" || callerID != s.nodeOperatorID {
		writeProblem(w, http.StatusForbidden, "Operator only", "This federation action is restricted to the node operator.")
		return false
	}
	return true
}

// v11 Mode-1 exchange agreement setup — the REST tx-builder for
// TxTypeCrossFedSet/Revoke (33/34). This is the operator half of the
// federation-JOIN ceremony: the peers exchange CA certificates + endpoints
// out-of-band, then each side calls POST /v1/federation/cross, which persists
// the remote CA on disk, computes its SPKI pin, and submits the terms tx with
// that pin as PeerPubKey. AUTHORIZATION is enforced on-chain
// (crossFedAuthorized: chain-admin or owner of every scoped domain) — the
// REST layer only builds the tx. The self-federation guard lives HERE (and in
// federation.ActiveAgreement), deliberately not in consensus.

// CrossFedSetRequest is the JSON body for POST /v1/federation/cross.
type CrossFedSetRequest struct {
	RemoteChainID string `json:"remote_chain_id"`
	// Endpoint is the remote federation listener base URL (https://host:8444).
	Endpoint string `json:"endpoint"`
	// RemoteCAPEM is the remote chain's CA certificate (PEM), exchanged
	// out-of-band. Its SPKI fingerprint becomes the on-chain pinned key.
	RemoteCAPEM    string   `json:"remote_ca_pem"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	AllowedDepts   []string `json:"allowed_depts,omitempty"`
	ExpiresAt      int64    `json:"expires_at,omitempty"`
}

// CrossFedSetResponse reports the persisted trust anchor and the committed tx.
type CrossFedSetResponse struct {
	RemoteChainID string `json:"remote_chain_id"`
	SPKIPin       string `json:"spki_pin"`
	TxHash        string `json:"tx_hash"`
	Status        string `json:"status"`
}

// federationAgreementMutationLeaser is deliberately narrower than
// FederationService. Legacy REST still builds tx-33/tx-34 itself, but it must
// participate in the Manager's process-wide agreement generation critical
// section so it cannot race JOIN, sharing updates, or dashboard revocation.
type federationAgreementMutationLeaser interface {
	LockAgreementMutation() func()
}

type federationSyncPolicyGenerationLeaser interface {
	BeginSyncPolicyGenerationMutation(remoteChainID string) *federation.SyncPolicyGenerationMutation
}

var _ federationAgreementMutationLeaser = (*federation.Manager)(nil)
var _ federationSyncPolicyGenerationLeaser = (*federation.Manager)(nil)

func (s *Server) lockFederationAgreementMutation(w http.ResponseWriter) (func(), bool) {
	leaser, ok := s.federation.(federationAgreementMutationLeaser)
	if !ok {
		// Never substitute a REST-local mutex here: it would serialize only this
		// handler and leave Manager-driven JOIN/revoke races intact.
		writeProblem(w, http.StatusNotImplemented, "Federation control unavailable",
			"The federation transport does not expose the shared agreement mutation lease.")
		return nil, false
	}
	return leaser.LockAgreementMutation(), true
}

// handleCrossFedSet handles POST /v1/federation/cross.
func (s *Server) handleCrossFedSet(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	if s.federation == nil {
		writeProblem(w, http.StatusNotImplemented, "Federation disabled", "The federation transport is not wired on this node.")
		return
	}
	var req CrossFedSetRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	if err := federation.ValidateChainID(req.RemoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	// Match the consensus cap (maxRemoteChainIDLen=50) so an over-length id fails
	// fast at validation instead of after a wasted stage+broadcast round-trip
	// that the on-chain handler rejects with Code 102.
	if len(req.RemoteChainID) > maxRemoteChainIDLenREST {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", "remote_chain_id exceeds the maximum length (50).")
		return
	}
	// Self-federation guard — enforced off-consensus by design (the ABCI app
	// has no deterministic source for its own chain id; see processCrossFedSet).
	if req.RemoteChainID == s.federation.LocalChainID() {
		writeProblem(w, http.StatusBadRequest, "Self federation", "remote_chain_id equals this chain's id.")
		return
	}
	// Require scheme+host only: the federation client appends a fixed /fed/v1/*
	// path (and the V2 signature covers it), so a stored path/query/fragment
	// would make every call to this peer 404 and mis-sign. Fail fast at JOIN.
	endpoint, err := url.Parse(req.Endpoint)
	if err != nil || endpoint.Scheme != "https" || endpoint.Host == "" ||
		(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		writeProblem(w, http.StatusBadRequest, "Invalid endpoint", "endpoint must be an https://host[:port] URL with no path, query, or fragment.")
		return
	}
	if req.MaxClearance < 0 || req.MaxClearance > 4 {
		writeProblem(w, http.StatusBadRequest, "Invalid clearance", "max_clearance must be 0..4.")
		return
	}
	if len(req.AllowedDomains) == 0 {
		writeProblem(w, http.StatusBadRequest, "Missing scope", "allowed_domains is required (\"*\" for a chain-admin treaty).")
		return
	}
	// Department scoping is NOT yet enforced on the query path (cross-chain dept
	// semantics are an open design question — plan §9.2). Reject a restrictive
	// allowed_depts rather than silently store a scope that does nothing (a
	// false sense of narrowing). "*" (or empty) is the only accepted value.
	for _, d := range req.AllowedDepts {
		if d != "*" {
			writeProblem(w, http.StatusBadRequest, "Dept scoping unsupported",
				"allowed_depts is not enforced in v11.0; omit it or use [\"*\"]. Scope with allowed_domains instead.")
			return
		}
	}
	if req.ExpiresAt != 0 && req.ExpiresAt <= time.Now().Unix() {
		writeProblem(w, http.StatusBadRequest, "Invalid expiry", "expires_at is in the past.")
		return
	}

	// Serialize the complete agreement generation change with every Manager
	// control surface. The lease starts before CA staging and is retained through
	// tx-33 plus promotion of that exact CA. Lock order is agreement mutation ->
	// sync-policy W (below); reversing it would deadlock Manager paths.
	agreementUnlock, ok := s.lockFederationAgreementMutation(w)
	if !ok {
		return
	}
	defer agreementUnlock()
	// A raw legacy tx-33 can still replace an agreement used by a pending sync
	// policy delivery. Quiesce that exact peer while the CA/terms generation is
	// staged and committed; small handler fakes have no delivery engine.
	if leaser, hasDelivery := s.federation.(federationSyncPolicyGenerationLeaser); hasDelivery {
		generation := leaser.BeginSyncPolicyGenerationMutation(req.RemoteChainID)
		defer generation.Restore()
	}

	// STAGE the remote CA to a pending sidecar and derive the pin, but do NOT
	// commit it to the live path until the terms tx is authorized on-chain.
	// Authorization (crossFedAuthorized) lives in the consensus handler, so an
	// unauthorized caller's tx is rejected — and because we only commit the CA
	// after that succeeds, an unauthorized set can never overwrite an existing
	// agreement's live pinned CA on disk (a silent availability kill).
	pin, commitCA, rollbackCA, err := s.federation.StageRemoteCA(req.RemoteChainID, []byte(req.RemoteCAPEM))
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote CA", err.Error())
		return
	}
	// A successful tx-33 may replace a live legacy agreement. Lease the same
	// policy write barrier used by federation peer handlers from before the
	// committed broadcast through promotion of its matching CA. Consequently a
	// narrowing waits for old-generation responses, while every reader admitted
	// after this handler returns resolves the new agreement and trust anchor.
	if ss := s.syncStore(); ss != nil {
		policyUnlock := ss.LockSyncPolicyWrite()
		defer policyUnlock()
	}

	setTx := &tx.ParsedTx{
		Type:      tx.TxTypeCrossFedSet,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		CrossFedTerms: &tx.CrossFedTerms{
			RemoteChainID:  req.RemoteChainID,
			Endpoint:       req.Endpoint,
			PeerPubKey:     pin,
			MaxClearance:   tx.ClearanceLevel(req.MaxClearance), // #nosec G115 -- validated 0..4
			AllowedDomains: req.AllowedDomains,
			AllowedDepts:   req.AllowedDepts,
			ExpiresAt:      req.ExpiresAt,
			Status:         "active",
		},
	}
	s.embedAgentAuth(r.Context(), setTx)
	if signErr := tx.SignTx(setTx, s.signingKey); signErr != nil {
		rollbackCA()
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(setTx)
	if err != nil {
		rollbackCA()
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	hash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		rollbackCA() // authz/consensus rejected — leave any existing live CA untouched
		s.logger.Error().Err(err).Str("remote", req.RemoteChainID).Msg("cross_fed set rejected")
		status, msg := broadcastErrorPublic(err)
		writeProblem(w, status, "Agreement rejected", msg)
		return
	}
	if err := commitCA(); err != nil {
		// The terms are on-chain but the CA didn't land — the agreement is
		// inert (pin mismatch fails closed) until re-provisioned. Surface it.
		s.logger.Error().Err(err).Str("remote", req.RemoteChainID).Msg("cross_fed CA commit failed post-broadcast")
		writeProblem(w, http.StatusInternalServerError, "CA commit failed", "Agreement recorded on-chain but the CA could not be persisted; re-run the join to provision it.")
		return
	}
	writeJSON(w, http.StatusCreated, &CrossFedSetResponse{
		RemoteChainID: req.RemoteChainID,
		SPKIPin:       hex.EncodeToString(pin),
		TxHash:        hash,
		Status:        "active",
	})
}

// handleCrossFedRevoke handles POST /v1/federation/cross/{chain_id}/revoke.
func (s *Server) handleCrossFedRevoke(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	var req struct {
		Reason string `json:"reason,omitempty"`
	}
	_ = decodeJSON(r, &req) // reason is optional; an empty body is fine

	// A live Manager owns the complete permanent-disconnect workflow: notify
	// the exact authenticated peer, revalidate the ceremony generation, commit
	// tx-34, purge capabilities, and retain a human-readable local event. Keep
	// the legacy REST transaction path below for deliberately transport-less
	// deployments and small handler fakes.
	if notifier, ok := s.federation.(interface {
		RevokeAgreementNotifying(string) (*federation.RevokeAgreementResult, error)
	}); ok {
		result, err := notifier.RevokeAgreementNotifying(remoteChainID)
		if err != nil {
			s.logger.Error().Err(err).Str("remote", remoteChainID).Msg("cross_fed notifying revoke rejected")
			writeProblem(w, http.StatusBadGateway, "Revoke rejected", err.Error())
			return
		}
		if result == nil {
			writeProblem(w, http.StatusBadGateway, "Revoke rejected", "Federation revoke returned no result.")
			return
		}
		out := map[string]any{
			"remote_chain_id": remoteChainID,
			"tx_hash":         result.TxHash,
			"status":          "revoked",
			"peer_notified":   result.PeerNotified,
		}
		if result.NoticeError != "" {
			out["notification_warning"] = result.NoticeError
		}
		writeJSON(w, http.StatusOK, out)
		return
	}

	// Hold the same generation lease as tx-33/JOIN from before tx-34 through
	// the complete local capability purge. A completed revoke therefore cannot
	// later delete artifacts belonging to a newer agreement generation.
	agreementUnlock, ok := s.lockFederationAgreementMutation(w)
	if !ok {
		return
	}
	defer agreementUnlock()

	revokeTx := &tx.ParsedTx{
		Type:      tx.TxTypeCrossFedRevoke,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		CrossFedRevoke: &tx.CrossFedRevoke{
			RemoteChainID: remoteChainID,
			Reason:        req.Reason,
		},
	}
	s.embedAgentAuth(r.Context(), revokeTx)
	if err := tx.SignTx(revokeTx, s.signingKey); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(revokeTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	hash, err := s.broadcastTxCommit(encoded)
	if err != nil {
		s.logger.Error().Err(err).Str("remote", remoteChainID).Msg("cross_fed revoke rejected")
		status, msg := broadcastErrorPublic(err)
		writeProblem(w, status, "Revoke rejected", msg)
		return
	}
	// Off-consensus sync purge: consent + queued deliveries die with the
	// agreement (tx-34 touches only chain state; node-local sync tables are
	// this handler's job, mirroring Manager.RevokeAgreement's seed/CA purge).
	if cleaner, ok := s.federation.(interface{ PurgeLocalFederationState(string) }); ok {
		cleaner.PurgeLocalFederationState(remoteChainID)
	} else if ss := s.syncStore(); ss != nil {
		if purgeErr := ss.PurgeSyncPeerState(r.Context(), remoteChainID); purgeErr != nil {
			s.logger.Warn().Err(purgeErr).Str("remote", remoteChainID).Msg("revoke: sync state purge failed")
		}
	}
	if ss := s.syncStore(); ss != nil {
		if eventErr := ss.SetFederationConnectionEvent(r.Context(), store.FederationConnectionEvent{
			RemoteChainID: remoteChainID,
			Event:         store.FederationConnectionRevokedLocally,
			Message:       "This operator permanently revoked trust.",
		}); eventErr != nil {
			s.logger.Warn().Err(eventErr).Str("remote", remoteChainID).Msg("revoke: connection event write failed")
		}
	}
	writeJSON(w, http.StatusOK, map[string]string{
		"remote_chain_id": remoteChainID,
		"tx_hash":         hash,
		"status":          "revoked",
	})
}

// CrossFedListEntry is one agreement in GET /v1/federation/cross.
type CrossFedListEntry struct {
	RemoteChainID  string   `json:"remote_chain_id"`
	Endpoint       string   `json:"endpoint"`
	SPKIPin        string   `json:"spki_pin"`
	MaxClearance   int      `json:"max_clearance"`
	AllowedDomains []string `json:"allowed_domains"`
	AllowedDepts   []string `json:"allowed_depts,omitempty"`
	ExpiresAt      int64    `json:"expires_at,omitempty"`
	Status         string   `json:"status"`
	Expired        bool     `json:"expired,omitempty"`
}

// handleCrossFedList handles GET /v1/federation/cross — reads on-chain state
// directly, so it works even when the transport isn't wired.
func (s *Server) handleCrossFedList(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusNotImplemented, "No chain state", "This node has no on-chain store wired.")
		return
	}
	records, err := s.badgerStore.ListCrossFed()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to list agreements.")
		return
	}
	now := time.Now().Unix()
	entries := make([]CrossFedListEntry, 0, len(records))
	for _, rec := range records {
		entries = append(entries, CrossFedListEntry{
			RemoteChainID:  rec.RemoteChainID,
			Endpoint:       rec.Endpoint,
			SPKIPin:        hex.EncodeToString(rec.PeerPubKey),
			MaxClearance:   int(rec.MaxClearance),
			AllowedDomains: rec.AllowedDomains,
			AllowedDepts:   rec.AllowedDepts,
			ExpiresAt:      rec.ExpiresAt,
			Status:         rec.Status,
			Expired:        rec.ExpiresAt != 0 && now >= rec.ExpiresAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"agreements": entries, "total": len(entries)})
}

// handleCrossFedPeerStatus handles GET /v1/federation/cross/{chain_id}/status —
// the live reachability/identity preflight against a peer's federation
// listener (mTLS + signed request), used by the activation runbook to
// distinguish "peer unreachable" from "peer misconfigured".
func (s *Server) handleCrossFedPeerStatus(w http.ResponseWriter, r *http.Request) {
	if s.federation == nil {
		writeProblem(w, http.StatusNotImplemented, "Federation disabled", "The federation transport is not wired on this node.")
		return
	}
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	ctx, cancel := contextWithFedTimeout(r)
	defer cancel()
	status, err := s.federation.PeerStatus(ctx, remoteChainID)
	if err != nil {
		// reachable stays a bool across both branches so typed clients don't break
		// on the error path (the whole point of this endpoint is the
		// unreachable-vs-misconfigured distinction).
		writeJSON(w, http.StatusBadGateway, map[string]any{
			"remote_chain_id": remoteChainID,
			"reachable":       false,
			"error":           err.Error(),
		})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": remoteChainID,
		"reachable":       true,
		"peer_time":       status.Time,
	})
}

// ---- cross-chain domain sync -------------------------------------------------
//
// v3 separates JOIN trust from mutable Copy policy: either frozen peer may
// author its own Publish and Subscribe lanes. Legacy v1/v2 links retain the
// host-controlled/bilateral {domains:[...]} contract until they re-pair.
// Policy remains off-consensus because extending tx-33 would fork.

// SyncDomainsRequest is the JSON body for PUT /v1/federation/cross/{chain_id}/sync.
type SyncDomainsRequest struct {
	Domains          *[]string `json:"domains"`
	PublishDomains   *[]string `json:"publish_domains"`
	SubscribeDomains *[]string `json:"subscribe_domains"`
}

type directionalSyncPolicyDriver interface {
	SetDirectionalSyncPolicy(context.Context, string, []string, []string) (*federation.DirectionalSyncPolicyResult, error)
}

func hasV3SyncMarker(control *store.SyncControl) bool {
	return control != nil && (control.PolicyVersion >= federation.SyncPolicyVersionPeerRBAC ||
		control.RemotePolicyVersion >= federation.SyncPolicyVersionPeerRBAC)
}

func isActiveFrozenV3SyncControl(control *store.SyncControl) bool {
	return control != nil && control.BindingState == "active" && control.PeerAgentID != "" &&
		control.PolicyEpoch != "" && control.RemoteCAPin != "" && hasV3SyncMarker(control)
}

// maxSyncDomainsREST bounds a single consent set — generous for real
// deployments, small enough to keep validation and status surfaces cheap.
const maxSyncDomainsREST = 100

// syncStore returns the SQLite store the sync tables live on, or nil when this
// node runs another backend. Sync is SQLite-only (mcp_tokens precedent): the
// Postgres store has no sync tables and must refuse loudly, not half-run.
func (s *Server) syncStore() *store.SQLiteStore {
	ss, _ := s.store.(*store.SQLiteStore)
	return ss
}

// handleSyncDomainsSet handles PUT /v1/federation/cross/{chain_id}/sync.
// A frozen v3 connection replaces either local directional lane independently;
// a true legacy connection retains the old complete {domains:[...]} snapshot.
func (s *Server) handleSyncDomainsSet(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	ss := s.syncStore()
	if ss == nil {
		writeProblem(w, http.StatusNotImplemented, "Sync unavailable", "Domain sync requires the SQLite store backend.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusNotImplemented, "No chain state", "This node has no on-chain store wired.")
		return
	}
	// Consent only attaches to a live agreement: the on-chain record is the
	// treaty; sync_domains can only narrow inside it.
	_, _, _, expiresAt, allowedDomains, _, status, err := s.badgerStore.GetCrossFed(remoteChainID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "No agreement", "No federation agreement exists for this chain.")
		return
	}
	if status != "active" || (expiresAt != 0 && time.Now().Unix() >= expiresAt) {
		writeProblem(w, http.StatusConflict, "Agreement not active", "Sync consent requires an active, unexpired agreement.")
		return
	}

	var req SyncDomainsRequest
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid body", "Expected domains, publish_domains, or subscribe_domains as an array.")
		return
	}
	if req.Domains == nil && req.PublishDomains == nil && req.SubscribeDomains == nil {
		writeProblem(w, http.StatusBadRequest, "Missing sync policy", "Expected domains, publish_domains, or subscribe_domains as an array.")
		return
	}
	if req.Domains != nil && (req.PublishDomains != nil || req.SubscribeDomains != nil) {
		writeProblem(w, http.StatusBadRequest, "Mixed sync policy", "Legacy domains cannot be combined with directional fields.")
		return
	}
	validateDomains := func(domains *[]string) bool {
		if domains == nil {
			return true
		}
		if len(*domains) > maxSyncDomainsREST {
			writeProblem(w, http.StatusBadRequest, "Too many domains", "A sync permission set is capped at 100 domains.")
			return false
		}
		for _, d := range *domains {
			if d == "" {
				writeProblem(w, http.StatusBadRequest, "Invalid domain", "Sync domains must be non-empty.")
				return false
			}
			if d == "*" {
				// Concrete domains only. A wildcard would silently grant every
				// future domain, defeating the explicit per-domain policy.
				writeProblem(w, http.StatusBadRequest, "Invalid domain", "Sync domains must be concrete (no \"*\").")
				return false
			}
		}
		return true
	}
	if !validateDomains(req.Domains) || !validateDomains(req.PublishDomains) || !validateDomains(req.SubscribeDomains) {
		return
	}
	control, controlErr := ss.GetSyncControl(r.Context(), remoteChainID)
	if controlErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Sync policy read failed", "Could not verify sync policy ownership.")
		return
	}
	if isActiveFrozenV3SyncControl(control) {
		if req.Domains != nil {
			writeProblem(w, http.StatusConflict, "Directional sync required", "This v3 connection accepts publish_domains and/or subscribe_domains, not legacy domains.")
			return
		}
		driver, ok := s.federation.(directionalSyncPolicyDriver)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Sync policy unavailable", "Directional sync policy is not wired on this node.")
			return
		}
		publish, readErr := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionLocalPublish)
		if readErr != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the current publish policy.")
			return
		}
		subscribe, readErr := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionLocalSubscribe)
		if readErr != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the current subscription policy.")
			return
		}
		if publish == nil {
			publish = []string{}
		}
		if subscribe == nil {
			subscribe = []string{}
		}
		if req.PublishDomains != nil {
			publish = append([]string{}, (*req.PublishDomains)...)
		}
		if req.SubscribeDomains != nil {
			subscribe = append([]string{}, (*req.SubscribeDomains)...)
		}
		result, policyErr := driver.SetDirectionalSyncPolicy(r.Context(), remoteChainID, publish, subscribe)
		if policyErr != nil {
			writeProblem(w, http.StatusConflict, "Sync policy denied", policyErr.Error())
			return
		}
		if result == nil {
			writeProblem(w, http.StatusInternalServerError, "Sync policy unavailable", "Directional sync returned no policy result.")
			return
		}
		remotePublish, readErr := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionRemotePublish)
		if readErr != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the peer's publish policy.")
			return
		}
		remoteSubscribe, readErr := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionRemoteSubscribe)
		if readErr != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the peer's subscription policy.")
			return
		}
		for _, values := range []*[]string{&remotePublish, &remoteSubscribe} {
			if *values == nil {
				*values = []string{}
			}
		}
		latest, readErr := ss.GetSyncControl(r.Context(), remoteChainID)
		if readErr != nil || latest == nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read back the directional sync policy.")
			return
		}
		resultPublish := append([]string{}, result.PublishDomains...)
		resultSubscribe := append([]string{}, result.SubscribeDomains...)
		resp := map[string]any{
			"remote_chain_id":          remoteChainID,
			"publish_domains":          resultPublish,
			"subscribe_domains":        resultSubscribe,
			"remote_publish_domains":   remotePublish,
			"remote_subscribe_domains": remoteSubscribe,
			"sync_role":                latest.Role,
			"policy_version":           result.Version,
			"revision":                 result.Revision,
			"delivered_revision":       latest.DeliveredRevision,
			"remote_policy_version":    latest.RemotePolicyVersion,
			"remote_revision":          latest.RemoteRevision,
			"state":                    result.State,
		}
		// The legacy field describes one bilateral set. It is truthful only
		// when both locally-authored directional lanes are identical.
		if slices.Equal(resultPublish, resultSubscribe) {
			resp["sync_domains"] = resultPublish
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if req.PublishDomains != nil || req.SubscribeDomains != nil {
		writeProblem(w, http.StatusConflict, "Legacy sync policy", "This connection has no active frozen v3 policy; use legacy domains or re-pair first.")
		return
	}
	legacyDomains := append([]string(nil), (*req.Domains)...)
	for _, d := range legacyDomains {
		if !federation.DomainAllowed(allowedDomains, d) {
			writeProblem(w, http.StatusBadRequest, "Domain outside agreement",
				"Sync domain "+strconv.Quote(d)+" is not covered by the agreement's allowed domains.")
			return
		}
	}
	if control != nil {
		// A v3 marker without an active frozen binding must fail closed. Never
		// route it through SetHostSyncPolicy or the legacy local table.
		if hasV3SyncMarker(control) {
			writeProblem(w, http.StatusConflict, "Directional sync unavailable", "This v3 sync binding is not active and frozen; re-pair before changing it.")
			return
		}
		if control.Role == "guest" {
			writeProblem(w, http.StatusConflict, "Host-managed sync", "This connection's sync policy is managed by the host node.")
			return
		}
		if driver, ok := s.federation.(interface {
			SetHostSyncPolicy(context.Context, string, []string) (*federation.HostSyncPolicyResult, error)
		}); ok {
			result, policyErr := driver.SetHostSyncPolicy(r.Context(), remoteChainID, legacyDomains)
			if policyErr != nil {
				writeProblem(w, http.StatusForbidden, "Sync policy denied", policyErr.Error())
				return
			}
			writeJSON(w, http.StatusOK, map[string]any{"remote_chain_id": remoteChainID,
				"sync_domains": result.Domains, "sync_role": "host", "revision": result.Revision, "state": result.State})
			return
		}
		writeProblem(w, http.StatusNotImplemented, "Sync policy unavailable", "Host-managed sync is not wired on this node.")
		return
	}
	if err = ss.SetSyncDomains(r.Context(), remoteChainID, legacyDomains); err != nil {
		s.logger.Error().Err(err).Str("remote", remoteChainID).Msg("set sync domains failed")
		writeProblem(w, http.StatusInternalServerError, "Write error", "Failed to persist sync domains.")
		return
	}
	saved, err := ss.GetSyncDomains(r.Context(), remoteChainID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read back sync domains.")
		return
	}
	if saved == nil {
		saved = []string{}
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"remote_chain_id": remoteChainID,
		"sync_domains":    saved,
	})
}

// handleSyncStatus handles GET /v1/federation/cross/{chain_id}/sync/status —
// the operator's observability surface for one peer's replication state:
// per-state outbox counts, the terminal rejections with their reasons (the
// B-D1 "surface, never silently move" requirement lands here), the pending
// retry schedule, and the anti-entropy bookkeeping (last reconcile, the
// peer's advertised consent, unsupported flag).
func (s *Server) handleSyncStatus(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	ss := s.syncStore()
	if ss == nil {
		writeProblem(w, http.StatusNotImplemented, "Sync unavailable", "Domain sync requires the SQLite store backend.")
		return
	}
	ctx := r.Context()
	counts, err := ss.CountSyncOutboxByState(ctx, remoteChainID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read outbox counts.")
		return
	}
	domains, err := ss.GetSyncDomains(ctx, remoteChainID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read sync domains.")
		return
	}
	if domains == nil {
		domains = []string{}
	}
	rejected, err := ss.ListSyncOutbox(ctx, remoteChainID, store.SyncStateRejected, 50)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read rejected rows.")
		return
	}
	pending, err := ss.ListSyncOutbox(ctx, remoteChainID, store.SyncStatePending, 50)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read pending rows.")
		return
	}
	type outboxRow struct {
		MemoryID      string `json:"memory_id"`
		Reason        string `json:"reason,omitempty"`
		Attempts      int    `json:"attempts,omitempty"`
		NextAttemptAt string `json:"next_attempt_at,omitempty"`
	}
	rejectedRows := make([]outboxRow, 0, len(rejected))
	for _, it := range rejected {
		rejectedRows = append(rejectedRows, outboxRow{MemoryID: it.MemoryID, Reason: it.LastError})
	}
	pendingRows := make([]outboxRow, 0, len(pending))
	for _, it := range pending {
		pendingRows = append(pendingRows, outboxRow{
			MemoryID: it.MemoryID, Reason: it.LastError,
			Attempts: it.Attempts, NextAttemptAt: it.NextAttemptAt.UTC().Format(time.RFC3339),
		})
	}
	resp := map[string]any{
		"remote_chain_id": remoteChainID,
		"sync_domains":    domains,
		"outbox_counts":   counts,
		"rejected":        rejectedRows,
		"pending":         pendingRows,
	}
	if s.federation != nil {
		if st, ok := s.federation.SyncReconcileInfo(remoteChainID); ok {
			resp["last_reconcile"] = st.LastReconcile.UTC().Format(time.RFC3339)
			resp["peer_consented_domains"] = st.PeerConsented
			resp["peer_unsupported"] = st.PeerUnsupported
		}
	}
	writeJSON(w, http.StatusOK, resp)
}

// handleSyncDomainsGet handles GET /v1/federation/cross/{chain_id}/sync.
func (s *Server) handleSyncDomainsGet(w http.ResponseWriter, r *http.Request) {
	if !s.requireNodeOperator(w, r) {
		return
	}
	remoteChainID := chi.URLParam(r, "chain_id")
	if err := federation.ValidateChainID(remoteChainID); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid remote chain id", err.Error())
		return
	}
	ss := s.syncStore()
	if ss == nil {
		writeProblem(w, http.StatusNotImplemented, "Sync unavailable", "Domain sync requires the SQLite store backend.")
		return
	}
	control, controlErr := ss.GetSyncControl(r.Context(), remoteChainID)
	if controlErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read sync policy ownership.")
		return
	}
	if isActiveFrozenV3SyncControl(control) {
		publish, err := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionLocalPublish)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the local publish policy.")
			return
		}
		subscribe, err := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionLocalSubscribe)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the local subscription policy.")
			return
		}
		remotePublish, err := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionRemotePublish)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the peer's publish policy.")
			return
		}
		remoteSubscribe, err := ss.GetDirectionalSyncDomains(r.Context(), remoteChainID, store.SyncDirectionRemoteSubscribe)
		if err != nil {
			writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read the peer's subscription policy.")
			return
		}
		for _, values := range []*[]string{&publish, &subscribe, &remotePublish, &remoteSubscribe} {
			if *values == nil {
				*values = []string{}
			}
		}
		resp := map[string]any{
			"remote_chain_id":          remoteChainID,
			"publish_domains":          publish,
			"subscribe_domains":        subscribe,
			"remote_publish_domains":   remotePublish,
			"remote_subscribe_domains": remoteSubscribe,
			"sync_role":                control.Role,
			"policy_version":           control.PolicyVersion,
			"revision":                 control.Revision,
			"delivered_revision":       control.DeliveredRevision,
			"remote_policy_version":    control.RemotePolicyVersion,
			"remote_revision":          control.RemoteRevision,
		}
		if slices.Equal(publish, subscribe) {
			resp["sync_domains"] = publish
		}
		writeJSON(w, http.StatusOK, resp)
		return
	}
	if hasV3SyncMarker(control) {
		writeProblem(w, http.StatusConflict, "Directional sync unavailable", "This v3 sync binding is not active and frozen; re-pair before reading it as a legacy policy.")
		return
	}
	domains, err := ss.GetSyncDomains(r.Context(), remoteChainID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read sync domains.")
		return
	}
	if domains == nil {
		domains = []string{}
	}
	resp := map[string]any{
		"remote_chain_id":    remoteChainID,
		"sync_domains":       domains,
		"sync_role":          "legacy",
		"policy_version":     federation.SyncPolicyVersionLegacy,
		"revision":           int64(0),
		"delivered_revision": int64(0),
	}
	if control != nil {
		resp["sync_role"] = control.Role
		resp["policy_version"] = control.PolicyVersion
		resp["revision"] = control.Revision
		resp["delivered_revision"] = control.DeliveredRevision
		resp["remote_policy_version"] = control.RemotePolicyVersion
		resp["remote_revision"] = control.RemoteRevision
	}
	writeJSON(w, http.StatusOK, resp)
}
