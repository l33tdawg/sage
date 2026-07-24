package rest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
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

const (
	maxFederatedContactAuthorizationDomains = 512
	maxFederatedContactAuthorizationBytes   = 256
)

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
	response := map[string]any{
		"remote_chain_id": remoteChainID,
		"reachable":       true,
		"peer_time":       status.Time,
	}
	if status.NetworkName != "" {
		response["network_name"] = status.NetworkName
	}
	if len(status.Capabilities) > 0 {
		response["capabilities"] = status.Capabilities
	}
	// These are the remote SAGE's authenticated, directional disclosures to
	// this node. Exposing them on the already operator-only status route gives
	// MCP clients the same read-only discovery CEREBRUM uses without granting
	// any new capability or leaking the local node's outbound policy.
	if status.PeerRBACGrant != nil {
		response["peer_rbac_grant"] = status.PeerRBACGrant
	}
	if status.SharingGrant != nil {
		response["sharing_grant"] = status.SharingGrant
	}
	if status.PipeContacts != nil {
		response["pipe_contacts"] = status.PipeContacts
	}
	writeJSON(w, http.StatusOK, response)
}

type availableFederationPermission struct {
	Domain string `json:"domain"`
	Read   bool   `json:"read"`
	Copy   bool   `json:"copy"`
}

type availableFederationSync struct {
	SubscribedDomains []string       `json:"subscribed_domains"`
	SavedCopies       int            `json:"saved_copies"`
	SavedByDomain     map[string]int `json:"saved_by_domain,omitempty"`
	LastReconcile     string         `json:"last_reconcile,omitempty"`
	PeerUnsupported   bool           `json:"peer_unsupported,omitempty"`
}

type availableFederationConnection struct {
	RemoteChainID      string                          `json:"remote_chain_id"`
	NetworkName        string                          `json:"network_name,omitempty"`
	Reachable          bool                            `json:"reachable"`
	Capabilities       []string                        `json:"capabilities,omitempty"`
	SharingPaused      bool                            `json:"sharing_paused,omitempty"`
	RemotePermissions  []availableFederationPermission `json:"remote_permissions"`
	SharedReadDomains  []string                        `json:"shared_read_domains"`
	CopyOfferedDomains []string                        `json:"copy_offered_domains"`
	RemoteAgents       []federation.PipeContact        `json:"remote_agents,omitempty"`
	Sync               *availableFederationSync        `json:"sync,omitempty"`
}

const maxAgentFederationTargets = 64

func statusAllowsFederatedRead(status *federation.StatusResponse, domain string) bool {
	if status == nil || domain == "" {
		return false
	}
	if status.PeerRBACGrant != nil {
		if status.PeerRBACGrant.Paused {
			return false
		}
		for _, grant := range status.PeerRBACGrant.Domains {
			if (grant.Read || grant.Copy) && federation.DomainAllowed([]string{grant.Domain}, domain) {
				return true
			}
		}
		return false
	}
	return status.SharingGrant != nil &&
		federation.DomainAllowed(status.SharingGrant.AllowedDomains, domain)
}

// federationRecallTargets converts an agent request into the finite set of
// currently authenticated peers that actually expose the requested domain.
// Failures and non-sharing peers are deliberately omitted so ordinary recall
// cannot enumerate hidden federation topology through queried/error fields.
func (s *Server) federationRecallTargets(ctx context.Context, requested []string, domain string) []string {
	if s.federation == nil || s.badgerStore == nil || domain == "" {
		return nil
	}
	records, err := s.badgerStore.ListCrossFed()
	if err != nil {
		return nil
	}
	active := make(map[string]struct{}, len(records))
	now := time.Now().Unix()
	for _, record := range records {
		if record.Status == "active" && (record.ExpiresAt == 0 || now < record.ExpiresAt) {
			active[record.RemoteChainID] = struct{}{}
		}
	}
	wildcard := len(requested) == 0 || (len(requested) == 1 && requested[0] == "*")
	candidates := make([]string, 0, len(active))
	if wildcard {
		for chain := range active {
			candidates = append(candidates, chain)
		}
	} else {
		seen := make(map[string]struct{})
		for _, raw := range requested {
			chain := strings.TrimSpace(raw)
			if chain == "" {
				continue
			}
			if _, ok := active[chain]; !ok {
				continue
			}
			if _, ok := seen[chain]; ok {
				continue
			}
			seen[chain] = struct{}{}
			candidates = append(candidates, chain)
		}
	}
	sort.Strings(candidates)
	if len(candidates) > maxAgentFederationTargets {
		candidates = candidates[:maxAgentFederationTargets]
	}

	type probe struct {
		chain  string
		status *federation.StatusResponse
	}
	probes := make([]probe, len(candidates))
	sem := make(chan struct{}, 8)
	var wg sync.WaitGroup
	for i, chain := range candidates {
		wg.Add(1)
		go func(index int, chainID string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				return
			}
			status, statusErr := s.federation.PeerStatus(ctx, chainID)
			if statusErr == nil {
				probes[index] = probe{chain: chainID, status: status}
			}
		}(i, chain)
	}
	wg.Wait()
	out := make([]string, 0, len(probes))
	for _, peer := range probes {
		if statusAllowsFederatedRead(peer.status, domain) {
			out = append(out, peer.chain)
		}
	}
	return out
}

// handleFederationAvailable is the ordinary-agent control-plane view. It
// returns only active peers and only the intersection between the peer's
// authenticated outbound grant and the local caller's readable subtrees. It
// never returns endpoints, CA pins, agreement generations, TOTP material, or
// mutation controls.
func (s *Server) handleFederationAvailable(w http.ResponseWriter, r *http.Request) {
	if s.federation == nil || s.badgerStore == nil {
		writeJSON(w, http.StatusOK, map[string]any{
			"connections": []availableFederationConnection{},
			"total":       0,
			"message":     "No federation transport is available on this SAGE.",
		})
		return
	}
	callerID := middleware.ContextAgentID(r.Context())
	if callerID == "" {
		writeProblem(w, http.StatusForbidden, "Access denied", "A registered agent identity is required for federation discovery.")
		return
	}
	records, err := s.badgerStore.ListCrossFed()
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to discover federation connections.")
		return
	}
	now := time.Now().Unix()
	active := make([]string, 0, len(records))
	for _, record := range records {
		if record.Status == "active" && (record.ExpiresAt == 0 || now < record.ExpiresAt) {
			active = append(active, record.RemoteChainID)
		}
	}
	sort.Strings(active)

	type peerResult struct {
		chain  string
		status *federation.StatusResponse
		err    error
	}
	results := make([]peerResult, len(active))
	ctx, cancel := context.WithTimeout(r.Context(), fedRecallTimeout())
	defer cancel()
	const maxConcurrentDiscovery = 8
	sem := make(chan struct{}, maxConcurrentDiscovery)
	var wg sync.WaitGroup
	for i, chainID := range active {
		wg.Add(1)
		go func(index int, chain string) {
			defer wg.Done()
			select {
			case sem <- struct{}{}:
				defer func() { <-sem }()
			case <-ctx.Done():
				results[index] = peerResult{chain: chain, err: ctx.Err()}
				return
			}
			status, statusErr := s.federation.PeerStatus(ctx, chain)
			results[index] = peerResult{chain: chain, status: status, err: statusErr}
		}(i, chainID)
	}
	wg.Wait()

	connections := make([]availableFederationConnection, 0, len(results))
	for _, peer := range results {
		// An unavailable peer has no current authenticated grant to filter.
		// Omit it for ordinary callers instead of leaking raw topology.
		if peer.err != nil || peer.status == nil {
			continue
		}
		connection := availableFederationConnection{
			RemoteChainID: peer.chain,
			NetworkName:   peer.status.NetworkName,
			Reachable:     true,
			Capabilities:  append([]string(nil), peer.status.Capabilities...),
		}
		permissions := make([]availableFederationPermission, 0)
		if peer.status.PeerRBACGrant != nil {
			connection.SharingPaused = peer.status.PeerRBACGrant.Paused
			for _, grant := range peer.status.PeerRBACGrant.Domains {
				for _, visible := range s.federationVisibleRemoteScopes(r.Context(), callerID, grant.Domain) {
					read := !connection.SharingPaused && (grant.Read || grant.Copy)
					copyAllowed := !connection.SharingPaused && grant.Copy
					if !read && !copyAllowed {
						continue
					}
					permissions = append(permissions, availableFederationPermission{Domain: visible, Read: read, Copy: copyAllowed})
				}
			}
		} else if peer.status.SharingGrant != nil {
			for _, remoteScope := range peer.status.SharingGrant.AllowedDomains {
				for _, visible := range s.federationVisibleRemoteScopes(r.Context(), callerID, remoteScope) {
					permissions = append(permissions, availableFederationPermission{Domain: visible, Read: true})
				}
			}
		}
		permissions = normalizeAvailablePermissions(permissions)
		if len(permissions) == 0 {
			continue
		}
		connection.RemotePermissions = permissions
		for _, permission := range permissions {
			if permission.Read {
				connection.SharedReadDomains = append(connection.SharedReadDomains, permission.Domain)
			}
			if permission.Copy {
				connection.CopyOfferedDomains = append(connection.CopyOfferedDomains, permission.Domain)
			}
		}
		if peer.status.PipeContacts != nil && !peer.status.PipeContacts.Paused {
			connection.RemoteAgents = filterAvailablePipeContacts(peer.status.PipeContacts.Contacts, connection.SharedReadDomains)
		}
		connection.Sync = s.availableFederationSync(r.Context(), peer.chain, connection.SharedReadDomains)
		connections = append(connections, connection)
	}
	sort.Slice(connections, func(i, j int) bool {
		if connections[i].NetworkName != connections[j].NetworkName {
			return connections[i].NetworkName < connections[j].NetworkName
		}
		return connections[i].RemoteChainID < connections[j].RemoteChainID
	})
	writeJSON(w, http.StatusOK, map[string]any{
		"connections": connections,
		"total":       len(connections),
		"message":     "Use sage_recall with scope=auto and one of these exact domains. Federation mutations remain operator-only.",
	})
}

// handleFederatedContactAuthorize is a local-only, caller-scoped policy check
// for a previously discovered contact projection. It deliberately does not
// read a peer or disclose contacts: MCP uses it to revalidate its short-lived
// in-memory cache after a local RBAC change.
func (s *Server) handleFederatedContactAuthorize(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Domains []string `json:"domains"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if len(req.Domains) > maxFederatedContactAuthorizationDomains {
		writeProblem(w, http.StatusBadRequest, "Too many domains", "at most 512 domains may be authorized per request")
		return
	}
	callerID := middleware.ContextAgentID(r.Context())
	if callerID == "" {
		writeProblem(w, http.StatusForbidden, "Access denied", "A registered agent identity is required for federation contact authorization.")
		return
	}
	seen := make(map[string]struct{}, len(req.Domains))
	allowed := make([]string, 0, len(req.Domains))
	for _, domain := range req.Domains {
		domain = strings.TrimSpace(domain)
		if domain == "" || len(domain) > maxFederatedContactAuthorizationBytes {
			writeProblem(w, http.StatusBadRequest, "Invalid domain", "each domain must be between 1 and 256 bytes")
			return
		}
		if _, duplicate := seen[domain]; duplicate {
			continue
		}
		seen[domain] = struct{}{}
		if len(s.federationVisibleRemoteScopes(r.Context(), callerID, domain)) > 0 {
			allowed = append(allowed, domain)
		}
	}
	sort.Strings(allowed)
	writeJSON(w, http.StatusOK, map[string]any{"allowed_domains": allowed})
}

func normalizeAvailablePermissions(in []availableFederationPermission) []availableFederationPermission {
	merged := make(map[string]availableFederationPermission)
	for _, permission := range in {
		current := merged[permission.Domain]
		current.Domain = permission.Domain
		current.Read = current.Read || permission.Read
		current.Copy = current.Copy || permission.Copy
		merged[permission.Domain] = current
	}
	out := make([]availableFederationPermission, 0, len(merged))
	for _, permission := range merged {
		out = append(out, permission)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Domain < out[j].Domain })
	return out
}

func (s *Server) federationVisibleRemoteScopes(ctx context.Context, callerID, remoteScope string) []string {
	if remoteScope == "" {
		return nil
	}
	if s.nodeOperatorID != "" && callerID == s.nodeOperatorID {
		return []string{remoteScope}
	}
	var role, domainAccess string
	if s.badgerStore != nil {
		if agent, err := s.badgerStore.GetRegisteredAgent(callerID); err == nil && agent != nil {
			role, domainAccess = agent.Role, agent.DomainAccess
		}
	}
	if role == "" && s.agentStore != nil {
		if agent, err := s.agentStore.GetAgent(ctx, callerID); err == nil && agent != nil {
			role, domainAccess = agent.Role, agent.DomainAccess
		}
	}
	if role == "" {
		return nil
	}
	if role == "admin" || domainAccess == "" {
		return []string{remoteScope}
	}
	var access []struct {
		Domain string `json:"domain"`
		Read   bool   `json:"read"`
	}
	if json.Unmarshal([]byte(domainAccess), &access) != nil {
		return nil
	}
	if len(access) == 0 {
		return []string{remoteScope}
	}
	visible := make([]string, 0, len(access))
	for _, local := range access {
		if !local.Read || local.Domain == "" {
			continue
		}
		switch {
		case federation.DomainAllowed([]string{local.Domain}, remoteScope):
			visible = append(visible, remoteScope)
		case federation.DomainAllowed([]string{remoteScope}, local.Domain):
			visible = append(visible, local.Domain)
		}
	}
	sort.Strings(visible)
	return slices.Compact(visible)
}

func filterAvailablePipeContacts(contacts []federation.PipeContact, allowed []string) []federation.PipeContact {
	out := make([]federation.PipeContact, 0)
	for _, contact := range contacts {
		filtered := contact
		// ContactID is an authorization-bound mutation token for the operator
		// surface, not ordinary-agent discovery metadata.
		filtered.ContactID = ""
		filtered.Domains = nil
		for _, domain := range contact.Domains {
			for _, scope := range allowed {
				switch {
				case federation.DomainAllowed([]string{scope}, domain.Domain):
					filtered.Domains = append(filtered.Domains, domain)
				case federation.DomainAllowed([]string{domain.Domain}, scope):
					// The contact was advertised for a broader parent than this
					// caller may read. Return only the authorized intersection;
					// ancestor ownership generation is outside that subtree.
					narrowed := domain
					narrowed.Domain = scope
					narrowed.OwningDomain = ""
					narrowed.OwnerHeight = 0
					filtered.Domains = append(filtered.Domains, narrowed)
				default:
					continue
				}
				if len(filtered.Domains) > 0 {
					break
				}
			}
		}
		if len(filtered.Domains) > 0 {
			out = append(out, filtered)
		}
	}
	return out
}

func (s *Server) availableFederationSync(ctx context.Context, remoteChainID string, allowed []string) *availableFederationSync {
	ss := s.syncStore()
	if ss == nil {
		return nil
	}
	var subscribed []string
	if control, err := ss.GetSyncControl(ctx, remoteChainID); err == nil && isActiveFrozenV3SyncControl(control) {
		subscribed, _ = ss.GetDirectionalSyncDomains(ctx, remoteChainID, store.SyncDirectionLocalSubscribe)
	} else {
		subscribed, _ = ss.GetSyncDomains(ctx, remoteChainID)
	}
	filtered := make([]string, 0, len(subscribed))
	for _, domain := range subscribed {
		for _, scope := range allowed {
			switch {
			case federation.DomainAllowed([]string{scope}, domain):
				filtered = append(filtered, domain)
			case federation.DomainAllowed([]string{domain}, scope):
				filtered = append(filtered, scope)
			default:
				continue
			}
			if len(filtered) > 0 {
				break
			}
		}
	}
	sort.Strings(filtered)
	counts, _ := ss.CountAdmittedSyncOriginsByDomain(ctx, remoteChainID)
	visibleCounts := make(map[string]int)
	total := 0
	for domain, count := range counts {
		if federation.DomainAllowed(allowed, domain) {
			visibleCounts[domain] = count
			total += count
		}
	}
	summary := &availableFederationSync{
		SubscribedDomains: slices.Compact(filtered),
		SavedCopies:       total,
		SavedByDomain:     visibleCounts,
	}
	if s.federation != nil {
		if reconcile, ok := s.federation.SyncReconcileInfo(remoteChainID); ok {
			summary.LastReconcile = reconcile.LastReconcile.UTC().Format(time.RFC3339)
			summary.PeerUnsupported = reconcile.PeerUnsupported
		}
	}
	return summary
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
	UpdateDirectionalSyncPolicy(context.Context, string, *[]string, *[]string) (*federation.DirectionalSyncPolicyResult, error)
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
		result, policyErr := driver.UpdateDirectionalSyncPolicy(r.Context(), remoteChainID, req.PublishDomains, req.SubscribeDomains)
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
