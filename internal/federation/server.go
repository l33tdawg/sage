package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
)

// Federation listener limits. The listener faces authenticated peers only
// (mTLS handshake gates strangers), but a peer is still another failure
// domain — bound everything.
const (
	maxFedBodyBytes = 4 << 20 // embeddings are ~30KB; 4MB is generous headroom
	// Contact lookup accepts only one 512-byte selector and a small limit. Keep
	// its authenticated-but-untrusted body well below the general memory route
	// budget so a paired peer cannot turn directory discovery into a read DoS.
	maxPipeContactLookupRequestBytes = 2 << 10
	maxFedTopK                       = 50
	defaultFedTopK                   = 10
	maxTimestampSkew                 = 5 * time.Minute
	// maxReplayEntriesPerChain bounds each PEER CHAIN's replay shard. Total
	// listener memory is bounded by (active peer chains × this) — and one
	// peer's flood is confined to its own shard.
	maxReplayEntriesPerChain = 4000
)

type peerCtxKey struct{}

type peerCeremonyBinding struct {
	PeerAgentID string
	PolicyEpoch string
	RemoteCAPin string
	State       string
}

// peerIdentity is what peerAuth binds for downstream handlers.
type peerIdentity struct {
	ChainID          string
	AgentID          string
	Agreement        *store.CrossFedRecord
	Ceremony         *peerCeremonyBinding
	CeremonyCaptured bool
}

// Router returns the federation listener's HTTP handler. EVERY route sits
// behind peerAuth — there is no unauthenticated surface on this listener.
func (m *Manager) Router() http.Handler {
	r := chi.NewRouter()
	r.Use(m.transportEnabledMiddleware)
	r.Group(func(r chi.Router) {
		r.Use(m.peerAuth)
		r.Get("/fed/v1/status", m.handleStatus)
		r.Post("/fed/v1/query", m.handleQuery)
		r.Post("/fed/v1/write", m.handleRemoteWrite)
		r.Post("/fed/v1/receipt", m.handleReceipt)
		r.Post("/fed/v1/connection/revoke-notice", m.handleRevokeNotice)
		r.Post("/fed/v1/pipe/event", m.handlePipeEvent)
		r.Post("/fed/v1/pipe/contacts/lookup", m.handlePipeContactLookup)
		r.Post("/fed/v1/sync/push", m.handleSyncPush)       // v11.5 domain sync
		r.Post("/fed/v1/sync/digest", m.handleSyncDigest)   // v11.5 anti-entropy
		r.Post("/fed/v1/sync/journal", m.handleSyncJournal) // v11.8 group journal exchange
		r.Post("/fed/v1/sync/group/domain-add/head", m.handleDomainAddHead)
		r.Post("/fed/v1/sync/group/domain-add/admit", m.handleDomainAddAdmit)
		r.Post("/fed/v1/sync/group/self-role/head", m.handleSelfRoleHead)
		r.Post("/fed/v1/sync/group/self-role/admit", m.handleSelfRoleAdmit)
		r.Post("/fed/v1/sync/group/epoch-rotate/cosign", m.handleEpochRotateCosign)
		r.Post("/fed/v1/sync/group/subchains", m.handleGroupSubchains)
		r.Post("/fed/v1/sync/group/member-invite/accept", m.handleMemberInviteAccept)
		r.Post("/fed/v1/sync/group/member-invite/bootstrap", m.handleMemberBootstrap)
		r.Put("/fed/v1/sync/policy", m.handleSyncPolicy) // v11.6 host-controlled sync
		r.Post("/fed/v1/p2p/routes", m.handleP2PRoutes)  // v11.6 authenticated LAN roaming upgrade
	})
	// The pre-agreement JOIN ceremony routes sit behind joinAuth, NOT peerAuth
	// (no active agreement exists yet during a join).
	m.mountJoinRoutes(r)
	return r
}

func (m *Manager) transportEnabledMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !m.transportIsEnabled() {
			httpError(w, http.StatusServiceUnavailable, "federation transport is disabled")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// peerAuth authenticates a federation request end-to-end:
//
//  1. the claimed sender chain (X-Chain-ID) must have an ACTIVE, unexpired
//     agreement (fail-closed on revoked/expired/unknown/self);
//  2. the mTLS client certificate presented on THIS connection must verify
//     against THAT agreement's pin-checked CA — binding the transport identity
//     to the claimed chain, not merely to "some peer" (the handshake already
//     required membership of some active agreement);
//  3. the chain-qualified Ed25519 signature (X-Sig-Version=2) must verify for
//     (sender=claimed chain, receiver=our chain) with a required nonce and
//     bounded timestamp skew;
//  4. the signature must be fresh (chain-scoped replay cache).
func (m *Manager) peerAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		sigVersion := r.Header.Get(HeaderSigVersion)
		if sigVersion != SigVersion2 && sigVersion != SigVersion3 {
			httpError(w, http.StatusUnauthorized, "unsupported X-Sig-Version")
			return
		}
		peerChain := r.Header.Get(HeaderChainID)
		agentID := r.Header.Get(HeaderAgentID)
		sigHex := r.Header.Get(HeaderSignature)
		tsStr := r.Header.Get(HeaderTimestamp)
		nonceHex := r.Header.Get(HeaderNonce)
		if peerChain == "" || agentID == "" || sigHex == "" || tsStr == "" || nonceHex == "" {
			httpError(w, http.StatusUnauthorized, "missing authentication headers")
			return
		}

		// Snapshot consensus terms and the off-consensus JOIN epoch beneath the
		// same mutation lease used by tx-33/tx-34 activation. Reading the epoch
		// only after signature verification would let an E1 request be mislabeled
		// E2 if an otherwise-identical re-pair completed during authentication.
		agreement, ceremony, err := m.snapshotPeerAuthGeneration(r.Context(), peerChain)
		if err != nil {
			m.logger.Warn().Err(err).Str("peer", peerChain).Msg("federation request denied: no active agreement")
			httpError(w, http.StatusForbidden, "no active agreement")
			return
		}

		// Bind the connection's client cert to the CLAIMED chain.
		ca, err := m.loadPinnedRemoteCA(peerChain, agreement.PeerPubKey)
		if err != nil {
			m.logger.Warn().Err(err).Str("peer", peerChain).Msg("federation request denied: pinned CA unavailable")
			httpError(w, http.StatusForbidden, "agreement trust anchor unavailable")
			return
		}
		if r.TLS == nil || len(r.TLS.PeerCertificates) == 0 {
			httpError(w, http.StatusForbidden, "client certificate required")
			return
		}
		rawCerts := make([][]byte, 0, len(r.TLS.PeerCertificates))
		for _, c := range r.TLS.PeerCertificates {
			rawCerts = append(rawCerts, c.Raw)
		}
		if certErr := verifyChainAgainstCA(rawCerts, ca, x509.ExtKeyUsageClientAuth); certErr != nil {
			m.logger.Warn().Err(certErr).Str("peer", peerChain).Msg("federation request denied: client cert does not match claimed chain")
			httpError(w, http.StatusForbidden, "client certificate does not match claimed chain")
			return
		}

		ts, err := strconv.ParseInt(tsStr, 10, 64)
		if err != nil {
			httpError(w, http.StatusUnauthorized, "invalid timestamp")
			return
		}
		if skew := time.Since(time.Unix(ts, 0)); skew > maxTimestampSkew || skew < -maxTimestampSkew {
			httpError(w, http.StatusUnauthorized, "timestamp outside acceptance window")
			return
		}
		pub, err := auth.AgentIDToPublicKey(agentID)
		if err != nil {
			httpError(w, http.StatusUnauthorized, "invalid agent id")
			return
		}
		nonce, err := hex.DecodeString(nonceHex)
		if err != nil || len(nonce) == 0 || len(nonce) > 64 {
			httpError(w, http.StatusUnauthorized, "invalid nonce")
			return
		}
		sig, err := hex.DecodeString(sigHex)
		if err != nil {
			httpError(w, http.StatusUnauthorized, "invalid signature encoding")
			return
		}

		bodyLimit := int64(maxFedBodyBytes)
		if r.Method == http.MethodPost && r.URL.Path == "/fed/v1/pipe/contacts/lookup" {
			bodyLimit = maxPipeContactLookupRequestBytes
		}
		r.Body = http.MaxBytesReader(w, r.Body, bodyLimit)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			httpError(w, http.StatusRequestEntityTooLarge, "request body too large")
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		reqPath := r.URL.Path
		if r.URL.RawQuery != "" {
			reqPath += "?" + r.URL.RawQuery
		}
		// Fail-closed version gate (§2.6.3): the required signature version is
		// driven by the agreement's persisted seed_established flag + the
		// in-memory seed cache — NEVER by running a KDF (DoS note). Reads the
		// plaintext seed header; if a seed is established but not unlocked, DENY
		// (503) — never silently accept v2.
		established := m.seedEstablished(peerChain)
		candidates := m.seedCandidates(peerChain)
		switch {
		case established && len(candidates) > 0:
			if sigVersion != SigVersion3 {
				httpError(w, http.StatusUnauthorized, "X-Sig-Version 3 required")
				return
			}
			if !m.verifyV3AnyEpoch(pub, peerChain, agreement.PeerPubKey, candidates, r.Method, reqPath, body, ts, nonce, sig) {
				// Epoch mismatch after trying every known seed epoch — a genuine
				// cross-peer desync, not a local lock. Loud alarm + diagnostic
				// (§2.6.4); never a silent blackout.
				m.logger.Error().Str("peer", peerChain).Msg("federation seed desync — v3 factor verified against no known epoch; re-enroll required")
				httpError(w, http.StatusUnauthorized, "federation seed desync — re-enroll required")
				return
			}
		case established && len(candidates) == 0:
			// Seed established but not unlocked (locked vault / I/O error) — a
			// local operator unlock problem, NOT a reason to strip the factor.
			m.logger.Warn().Str("peer", peerChain).Msg("federation locked: seed established but not in cache")
			httpError(w, http.StatusServiceUnavailable, "federation locked — unlock to resume")
			return
		default:
			// No seed established (legacy peer / non-active agreement) — accept v2.
			if sigVersion != SigVersion2 {
				httpError(w, http.StatusUnauthorized, "X-Sig-Version 2 required")
				return
			}
			if !auth.VerifyRequestV2(pub, peerChain, m.localChainID, r.Method, reqPath, body, ts, nonce, sig) {
				m.logger.Warn().Str("peer", peerChain).Str("agent", agentID[:16]).Msg("federation request denied: bad signature")
				httpError(w, http.StatusUnauthorized, "signature verification failed")
				return
			}
		}

		if !m.replayFresh(peerChain, agentID+":"+sigHex, ts) {
			httpError(w, http.StatusUnauthorized, "replayed signature")
			return
		}

		identity := &peerIdentity{
			ChainID:          peerChain,
			AgentID:          agentID,
			Agreement:        agreement,
			Ceremony:         ceremony,
			CeremonyCaptured: true,
		}
		if _, err := m.currentRequestAgreementBound(r.Context(), identity); err != nil {
			httpError(w, http.StatusForbidden, "federation agreement generation changed during authentication")
			return
		}
		ctx := context.WithValue(r.Context(), peerCtxKey{}, identity)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

func (m *Manager) snapshotPeerAuthGeneration(ctx context.Context, peerChain string) (*store.CrossFedRecord, *peerCeremonyBinding, error) {
	unlock := m.LockAgreementMutation()
	defer unlock()
	agreement, err := m.ActiveAgreement(peerChain)
	if err != nil {
		return nil, nil, err
	}
	var ceremony *peerCeremonyBinding
	if ss := m.syncStore(); ss != nil {
		control, controlErr := ss.GetSyncControl(ctx, peerChain)
		if controlErr != nil {
			return nil, nil, fmt.Errorf("read federation ceremony binding: %w", controlErr)
		}
		if control != nil {
			ceremony = &peerCeremonyBinding{
				PeerAgentID: control.PeerAgentID,
				PolicyEpoch: control.PolicyEpoch,
				RemoteCAPin: control.RemoteCAPin,
				State:       control.BindingState,
			}
		}
	}
	return agreement, ceremony, nil
}

// verifyV3AnyEpoch tries the v3 signature against each candidate seed (current
// + previous epoch during a rotation cutover), deriving k_totp from the seed +
// the (own, peer) pin pair. Returns true on the first match.
func (m *Manager) verifyV3AnyEpoch(pub ed25519.PublicKey, peerChain string, peerPin []byte, candidates [][]byte, method, path string, body []byte, ts int64, nonce, sig []byte) bool {
	ownPin, err := m.ownPin()
	if err != nil {
		m.logger.Error().Err(err).Msg("v3 verify: own pin unavailable")
		return false
	}
	for _, seed := range candidates {
		k := DeriveKTOTP(seed, m.localChainID, ownPin, peerChain, peerPin)
		if auth.VerifyRequestV3(pub, k, peerChain, m.localChainID, method, path, body, ts, nonce, sig) {
			return true
		}
	}
	return false
}

// replayFresh records a signature under its peer chain's shard and reports
// whether it was unseen. SHARDED per peer chain with a PER-CHAIN cap: a single
// peer flooding distinct valid sigs fills only its OWN shard, so it can lock
// out only itself — never other peers (the earlier global cap let any one peer
// DoS the whole listener). Within a shard, expired entries (ts older than the
// skew horizon) are evicted first, so honest steady-state traffic never hits
// the cap; the cap only bites a peer actively flooding, and empty shards are
// dropped to bound the outer map to chains with live traffic.
func (m *Manager) replayFresh(chainID, sigKey string, ts int64) bool {
	m.replayMu.Lock()
	defer m.replayMu.Unlock()
	now := time.Now().Unix()
	horizon := int64(maxTimestampSkew / time.Second)

	shard := m.seenSigs[chainID]
	if shard == nil {
		shard = make(map[string]int64)
		m.seenSigs[chainID] = shard
	}
	for k, seenTS := range shard {
		if now-seenTS > horizon {
			delete(shard, k)
		}
	}
	if _, seen := shard[sigKey]; seen {
		return false
	}
	if len(shard) >= maxReplayEntriesPerChain {
		return false // this peer is flooding its own shard; reject (fail closed) — others unaffected
	}
	shard[sigKey] = ts
	// The outer map is bounded by the number of active agreements (peerAuth
	// gates on ActiveAgreement before we get here), so no outer-map eviction is
	// needed — only chains with a live agreement can ever create a shard.
	return true
}

func peerFromCtx(ctx context.Context) *peerIdentity {
	p, _ := ctx.Value(peerCtxKey{}).(*peerIdentity)
	return p
}

// sameAgreementGeneration requires a request authenticated by peerAuth to
// still name the exact active trust snapshot at the handler's policy
// linearization point. The CA pin is the transport trust anchor; the remaining
// fields prevent an in-flight request from retaining a superseded treaty scope
// or clearance after the agreement is replaced.
func sameAgreementGeneration(authenticated, current *store.CrossFedRecord) bool {
	return authenticated != nil && current != nil &&
		authenticated.RemoteChainID == current.RemoteChainID &&
		authenticated.Endpoint == current.Endpoint &&
		bytes.Equal(authenticated.PeerPubKey, current.PeerPubKey) &&
		authenticated.MaxClearance == current.MaxClearance &&
		authenticated.ExpiresAt == current.ExpiresAt &&
		slices.Equal(authenticated.AllowedDomains, current.AllowedDomains) &&
		slices.Equal(authenticated.AllowedDepts, current.AllowedDepts) &&
		authenticated.Status == current.Status
}

// currentRequestAgreementBound closes the gap between peerAuth and a handler's
// effective-policy read. Callers hold the sync-policy read lease, so a revoke
// whose local purge completes before this check is denied, while a revoke that
// starts afterward cannot complete until the response has finished. Where a
// JOIN-era sync control exists, the live signer must also be the exact operator
// frozen by that ceremony; a legacy agreement with no such local binding keeps
// its historical treaty behavior.
func (m *Manager) currentRequestAgreementBound(ctx context.Context, peer *peerIdentity) (*store.CrossFedRecord, error) {
	if peer == nil || peer.Agreement == nil || peer.ChainID == "" || peer.AgentID == "" {
		return nil, fmt.Errorf("authenticated peer identity is incomplete")
	}
	current, err := m.ActiveAgreement(peer.ChainID)
	if err != nil {
		return nil, err
	}
	if !sameAgreementGeneration(peer.Agreement, current) {
		return nil, fmt.Errorf("authenticated federation agreement generation changed")
	}
	ss := m.syncStore()
	if ss == nil {
		return current, nil
	}
	control, err := ss.GetSyncControl(ctx, peer.ChainID)
	if err != nil {
		return nil, fmt.Errorf("read peer operator binding: %w", err)
	}
	if control == nil {
		if peer.CeremonyCaptured && peer.Ceremony != nil {
			return nil, fmt.Errorf("authenticated federation ceremony generation changed")
		}
		return current, nil
	}
	if peer.CeremonyCaptured {
		if peer.Ceremony == nil || peer.Ceremony.PeerAgentID != control.PeerAgentID ||
			peer.Ceremony.PolicyEpoch != control.PolicyEpoch || peer.Ceremony.RemoteCAPin != control.RemoteCAPin ||
			peer.Ceremony.State != control.BindingState || control.BindingState != "active" {
			return nil, fmt.Errorf("authenticated federation ceremony generation changed")
		}
	}
	peerOperator, err := m.resolvePeerOperatorAgentID(ctx, current)
	if err != nil {
		return nil, err
	}
	if peerOperator != peer.AgentID {
		return nil, fmt.Errorf("requesting operator does not match the active federation binding")
	}
	return current, nil
}

// handleStatus — authenticated reachability/identity preflight. Capabilities
// advertises optional route groups so senders can feature-detect: "sync" only
// when the backend actually supports it (SQLite-only).
func (m *Manager) handleStatus(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	ss := m.syncStore()
	policyUnlock := func() {}
	ownerUnlock := func() {}
	contactUnlock := func() {}
	if ss != nil {
		policyUnlock = ss.LockSyncPolicyRead()
		if m.badger != nil {
			ownerUnlock = m.badger.LockDomainOwnershipRead()
		}
		contactUnlock = ss.LockAgentContactRead()
	}
	releaseSnapshot := func() {
		contactUnlock()
		ownerUnlock()
		policyUnlock()
	}
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		releaseSnapshot()
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}
	var caps []string
	if m.syncStore() != nil {
		caps = append(caps, CapabilitySync)
		caps = append(caps, CapabilityFederatedPipeline)
		caps = append(caps, CapabilityFederatedPipelineContactLookup)
	}
	policy, err := m.getPeerRBACPolicyForAgreement(r.Context(), agreement)
	if err != nil {
		releaseSnapshot()
		m.logger.Error().Err(err).Str("peer", peer.ChainID).Msg("federation status peer RBAC lookup failed")
		httpError(w, http.StatusInternalServerError, "peer RBAC lookup failed")
		return
	}
	var peerRBACGrant *PeerRBACGrant
	var pipeContacts *PipeContactGrant
	if policy != nil {
		if policy.PeerAgentID != peer.AgentID {
			releaseSnapshot()
			httpError(w, http.StatusForbidden, "requesting operator is not bound to this peer RBAC policy")
			return
		}
		peerRBACGrant = peerRBACGrantFromPolicy(policy)
		if !clientRequestsPipeContactLookup(r) {
			pipeContacts, err = m.buildPipeContactStatusGrant(r.Context(), peer, policy)
			if err != nil {
				releaseSnapshot()
				m.logger.Error().Err(err).Str("peer", peer.ChainID).Msg("federation status pipe contact projection failed")
				httpError(w, http.StatusInternalServerError, "pipe contact projection failed")
				return
			}
			// Preserve v1 rolling compatibility: v11.13.0 peers reject a v1
			// snapshot with more than 1,024 contacts. Keep a deterministic valid
			// subset for them. New peers explicitly request compact status then use
			// the targeted lookup route, so no roster-wide RBAC scan is needed.
			pipeContacts = boundedPipeContactStatusSnapshot(pipeContacts)
		}
	}
	domains := []string{}
	if policy == nil {
		domains = append(domains, agreement.AllowedDomains...)
	}
	response := &StatusResponse{
		ChainID:      m.localChainID,
		NetworkName:  sanitizeName(m.NetworkName()),
		Time:         time.Now().Unix(),
		Capabilities: caps,
		SharingGrant: &SharingGrant{
			AllowedDomains: domains,
			MaxClearance:   agreement.MaxClearance,
		},
		PeerRBACGrant: peerRBACGrant,
		PipeContacts:  pipeContacts,
	}
	// The leases protect construction of one coherent authorization snapshot,
	// not a peer-controlled socket write. Once the value is materialized, slow
	// status readers must not delay consensus agent projection.
	releaseSnapshot()
	writeJSON(w, http.StatusOK, response)
}

func clientRequestsPipeContactLookup(r *http.Request) bool {
	for _, capability := range strings.Split(r.Header.Get(HeaderClientCapabilities), ",") {
		if strings.TrimSpace(capability) == CapabilityFederatedPipelineContactLookup {
			return true
		}
	}
	return false
}

func fitsPipeContactStatusSnapshot(grant *PipeContactGrant) bool {
	if grant == nil || len(grant.Contacts) > maxPipeContactStatusContacts {
		return false
	}
	encoded, err := json.Marshal(grant)
	return err == nil && len(encoded) <= maxPipeContactStatusBytes
}

// boundedPipeContactStatusSnapshot gives legacy v1 peers a valid, deterministic
// subset. Source contacts are already agent-ID sorted, and we skip only a
// contact that would breach the byte ceiling so later compact contacts remain
// discoverable through the legacy status path too.
func boundedPipeContactStatusSnapshot(grant *PipeContactGrant) *PipeContactGrant {
	if grant == nil {
		return nil
	}
	bounded := *grant
	bounded.Contacts = make([]PipeContact, 0, min(len(grant.Contacts), maxPipeContactStatusContacts))
	// Marshal the invariant grant once and each contact once. Re-marshaling the
	// progressively growing grant for every candidate makes a 1,024-contact
	// legacy status request quadratic in its JSON size while the policy/domain
	// snapshot locks are held.
	emptyEncoded, err := json.Marshal(&bounded)
	if err != nil {
		return &bounded
	}
	encodedLen := len(emptyEncoded)
	for _, contact := range grant.Contacts {
		if len(bounded.Contacts) >= maxPipeContactStatusContacts {
			break
		}
		encodedContact, marshalErr := json.Marshal(contact)
		if marshalErr != nil {
			continue
		}
		candidateLen := encodedLen + len(encodedContact)
		if len(bounded.Contacts) != 0 {
			candidateLen++ // comma between JSON array elements
		}
		if candidateLen <= maxPipeContactStatusBytes {
			bounded.Contacts = append(bounded.Contacts, contact)
			encodedLen = candidateLen
		}
	}
	return &bounded
}

// handlePipeContactLookup resolves a bounded subset of the authenticated
// peer's live shared-domain recipient projection. It deliberately rebuilds
// authorization under the same leases as status and inbound delivery; this is
// discovery/routing, not a global agent directory.
func (m *Manager) handlePipeContactLookup(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req PipeContactLookupRequest
	r.Body = http.MaxBytesReader(w, r.Body, maxPipeContactLookupRequestBytes)
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	req.Target = strings.TrimSpace(req.Target)
	req.Name = strings.TrimSpace(req.Name)
	if (req.Target == "") == (req.Name == "") || len(req.Target) > 512 || len(req.Name) > 512 {
		httpError(w, http.StatusBadRequest, "provide exactly one valid contact selector")
		return
	}
	if req.Limit <= 0 {
		req.Limit = maxPipeContactLookupResults
	}
	if req.Limit > maxPipeContactLookupResults {
		httpError(w, http.StatusBadRequest, "contact lookup limit is too large")
		return
	}

	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		httpError(w, http.StatusServiceUnavailable, "pipe contact lookup is unavailable")
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	contactUnlock := ss.LockAgentContactRead()
	released := false
	releaseSnapshot := func() {
		if released {
			return
		}
		released = true
		contactUnlock()
		ownerUnlock()
		policyUnlock()
	}
	defer releaseSnapshot()
	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		releaseSnapshot()
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}
	policy, err := m.getPeerRBACPolicyForAgreement(r.Context(), agreement)
	if err != nil {
		m.logger.Error().Err(err).Str("peer", peer.ChainID).Msg("federation pipe contact lookup policy failed")
		releaseSnapshot()
		httpError(w, http.StatusInternalServerError, "peer RBAC lookup failed")
		return
	}
	if policy == nil || policy.PeerAgentID != peer.AgentID {
		releaseSnapshot()
		httpError(w, http.StatusForbidden, "requesting operator is not bound to this peer RBAC policy")
		return
	}
	grant, total, err := m.buildPipeContactLookupGrant(r.Context(), peer, policy, req)
	if err != nil {
		m.logger.Error().Err(err).Str("peer", peer.ChainID).Msg("federation pipe contact lookup projection failed")
		releaseSnapshot()
		httpError(w, http.StatusInternalServerError, "pipe contact lookup failed")
		return
	}
	response, err := boundedPipeContactLookupResponse(grant, total)
	if err != nil {
		releaseSnapshot()
		httpError(w, http.StatusRequestEntityTooLarge, "contact lookup response is too large")
		return
	}
	// Authorization is materialized above. Do not let a slow authenticated peer
	// hold the consensus projection leases while it drains a response body.
	releaseSnapshot()
	writeJSON(w, http.StatusOK, response)
}

func filterPipeContactLookup(grant *PipeContactGrant, req PipeContactLookupRequest) (*PipeContactGrant, int) {
	limit := req.Limit
	if limit <= 0 || limit > maxPipeContactLookupResults {
		limit = maxPipeContactLookupResults
	}
	filtered := *grant
	// Keep allocation size independent of the peer-controlled request. The
	// handler validates Limit too, but this helper is also used internally and
	// must remain safe if a future caller skips that boundary validation.
	filtered.Contacts = make([]PipeContact, 0, min(maxPipeContactLookupResults, len(grant.Contacts)))
	exact := make([]PipeContact, 0)
	partial := make([]PipeContact, 0)
	for _, contact := range grant.Contacts {
		if req.Target != "" {
			if pipeContactMatchesTarget(contact, req.Target) {
				exact = append(exact, contact)
			}
			continue
		}
		isExact, isPartial := pipeContactMatchesName(req.Name, contact)
		if isExact {
			exact = append(exact, contact)
		} else if isPartial {
			partial = append(partial, contact)
		}
	}
	matches := exact
	if len(matches) == 0 {
		matches = partial
	}
	sort.Slice(matches, func(i, j int) bool {
		if strings.EqualFold(matches[i].DisplayName, matches[j].DisplayName) {
			return matches[i].AgentID < matches[j].AgentID
		}
		return strings.ToLower(matches[i].DisplayName) < strings.ToLower(matches[j].DisplayName)
	})
	total := len(matches)
	filtered.Contacts = append(filtered.Contacts, matches[:min(limit, total)]...)
	return &filtered, total
}

// boundedPipeContactLookupResponse applies a byte cap after semantic matching.
// A human-name match can contain a wide shared-domain basis even when it has
// only twenty contacts. The total remains the full matched count, so clients
// know that a re-query with a more exact selector may be needed.
func boundedPipeContactLookupResponse(grant *PipeContactGrant, total int) (*PipeContactLookupResponse, error) {
	if grant == nil || total < 0 || total < len(grant.Contacts) {
		return nil, fmt.Errorf("invalid pipe contact lookup result")
	}
	bounded := *grant
	bounded.Contacts = make([]PipeContact, 0, len(grant.Contacts))
	for _, contact := range grant.Contacts {
		candidate := bounded
		candidate.Contacts = append(append([]PipeContact(nil), bounded.Contacts...), contact)
		response := &PipeContactLookupResponse{
			Grant:     &candidate,
			Total:     total,
			Truncated: total > len(candidate.Contacts),
		}
		encoded, err := json.Marshal(response)
		if err != nil {
			return nil, err
		}
		if len(encoded) <= maxPipeContactLookupBytes {
			bounded.Contacts = candidate.Contacts
		}
	}
	if total > 0 && len(bounded.Contacts) == 0 {
		return nil, fmt.Errorf("one pipe contact exceeds the lookup response limit")
	}
	return &PipeContactLookupResponse{
		Grant:     &bounded,
		Total:     total,
		Truncated: total > len(bounded.Contacts),
	}, nil
}

func pipeContactMatchesTarget(contact PipeContact, target string) bool {
	if agentID, chainID := splitPipeAddress(target); chainID != "" {
		return contact.AgentID == agentID && contact.Address == contact.AgentID+"@"+chainID
	}
	if strings.HasPrefix(target, "#") {
		return strings.EqualFold(contact.Handle, target)
	}
	return strings.EqualFold(contact.AgentID, target) || strings.EqualFold(contact.DisplayName, target)
}

func pipeContactMatchesName(query string, contact PipeContact) (exact bool, partial bool) {
	for _, candidate := range []string{contact.DisplayName, contact.RegisteredName, contact.Provider} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if strings.EqualFold(candidate, query) {
			return true, false
		}
	}
	return false, false
}

// handleQuery serves a scoped read-only recall to an authenticated peer.
// A configured directional peer-RBAC snapshot is authoritative for domain
// scope, including an empty deny-all snapshot. Legacy connections without a
// snapshot retain their tx-33 domain scope. The tx-33 MaxClearance ceiling is
// retained in both cases as fail-closed legacy classification metadata until a
// dedicated peer-RBAC clearance field exists.
func (m *Manager) handleQuery(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req QueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	// Lease the effective policy from authorization through the completed
	// response write. A permissions update/revoke takes the write side, so after
	// it returns no query can still disclose data under the old Read snapshot.
	if ss := m.syncStore(); ss != nil {
		policyUnlock := ss.LockSyncPolicyRead()
		defer policyUnlock()
	}
	agreement, agreementErr := m.currentRequestAgreementBound(r.Context(), peer)
	if agreementErr != nil {
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}
	peerPolicy, policyErr := m.getPeerRBACPolicyForAgreement(r.Context(), agreement)
	if policyErr != nil {
		m.logger.Error().Err(policyErr).Str("peer", peer.ChainID).Msg("federation query peer RBAC lookup failed")
		httpError(w, http.StatusInternalServerError, "peer RBAC lookup failed")
		return
	}
	if peerPolicy != nil && peerPolicy.PeerAgentID != peer.AgentID {
		httpError(w, http.StatusForbidden, "requesting operator is not bound to this peer RBAC policy")
		return
	}

	// Scope gate on the REQUESTED domain. Once peer RBAC is configured it fully
	// replaces tx-33 AllowedDomains, and concrete domain rows mean an unscoped
	// query is always denied. Legacy links keep the wildcard treaty behavior.
	requestAllowed := peerRBACAllowsRead(peerPolicy, req.DomainTag)
	if peerPolicy == nil {
		requestAllowed = DomainAllowed(agreement.AllowedDomains, req.DomainTag)
	}
	if !requestAllowed {
		if req.DomainTag == "" {
			httpError(w, http.StatusForbidden, "a permitted domain_tag is required")
		} else {
			httpError(w, http.StatusForbidden, "domain read is not permitted")
		}
		return
	}

	topK := req.TopK
	if topK <= 0 {
		topK = defaultFedTopK
	}
	if topK > maxFedTopK {
		topK = maxFedTopK
	}
	now := time.Now()
	opts := store.QueryOptions{
		DomainTag:    req.DomainTag,
		StatusFilter: string(memory.StatusCommitted), // committed-only, non-negotiable
		TopK:         topK,
		Tags:         req.Tags,
	}
	if len(req.Embedding) > 0 {
		if req.EmbeddingProvider == "" {
			httpError(w, http.StatusBadRequest, "embedding_provider is required with an embedding")
			return
		}
		opts.VectorProvider = req.EmbeddingProvider
	}
	// min_confidence is a DECAYED floor (parity with local recall): the store filters
	// the decayed value over the full candidate set before trim — so top_k is filled
	// and corroboration-boosted memories aren't starved — pinned to `now` so it
	// matches the ConfidenceScore serialized below. Read-only serving path; no
	// consensus/AppHash concern.
	if req.MinConfidence > 0 {
		opts.DecayFloor = req.MinConfidence
		opts.DecayNow = now
	}

	var records []*memory.MemoryRecord
	var err error
	switch req.Mode {
	case ModeSemantic:
		if len(req.Embedding) == 0 {
			httpError(w, http.StatusBadRequest, "semantic mode requires an embedding")
			return
		}
		records, err = m.memStore.QuerySimilar(r.Context(), req.Embedding, opts)
	case ModeText:
		if req.Query == "" {
			httpError(w, http.StatusBadRequest, "text mode requires a query")
			return
		}
		records, err = m.memStore.SearchByText(r.Context(), req.Query, opts)
	case ModeHybrid:
		if req.Query == "" && len(req.Embedding) == 0 {
			httpError(w, http.StatusBadRequest, "hybrid mode requires a query or an embedding")
			return
		}
		records, err = m.memStore.SearchHybrid(r.Context(), req.Query, req.Embedding, opts)
	default:
		httpError(w, http.StatusBadRequest, "mode must be semantic, text, or hybrid")
		return
	}
	if err != nil {
		m.logger.Error().Err(err).Str("peer", peer.ChainID).Msg("federation query failed")
		httpError(w, http.StatusInternalServerError, "query failed")
		return
	}

	// Per-record treaty enforcement (defense in depth over the store filter):
	// domain coverage, committed status, classification ≤ ceiling.
	results := make([]*MemoryResult, 0, len(records))
	hidden := 0
	for _, rec := range records {
		if rec.Status != memory.StatusCommitted {
			hidden++
			continue
		}
		recordAllowed := peerRBACAllowsRead(peerPolicy, rec.DomainTag)
		if peerPolicy == nil {
			recordAllowed = DomainAllowed(agreement.AllowedDomains, rec.DomainTag)
		}
		if !recordAllowed {
			hidden++
			continue
		}
		// Fail CLOSED on a classification read error: GetMemoryClassification
		// returns (0, err) on a corrupt/unreadable entry, and 0 ≤ every ceiling
		// — swallowing the error would DISCLOSE an arbitrarily-classified record
		// across the federation boundary. This is the sole clearance gate on the
		// egress path, so an error hides the record.
		memClass, classErr := m.badger.GetMemoryClassification(rec.MemoryID)
		if classErr != nil || memClass > agreement.MaxClearance {
			hidden++
			continue
		}
		corrs, corrErr := m.memStore.GetCorroborations(r.Context(), rec.MemoryID)
		if corrErr != nil && req.MinConfidence > 0 {
			// Fail closed under a floor: a boost-less (understated) confidence could
			// wrongly drop this record on the requesting side. Hide rather than mislead.
			hidden++
			continue
		}
		results = append(results, &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    memory.ComputeConfidenceForRecord(rec, now, len(corrs)),
			CorroborationCount: len(corrs),
			Classification:     int(memClass),
			Status:             string(rec.Status),
			CreatedAt:          rec.CreatedAt,
			CommittedAt:        rec.CommittedAt,
		})
	}
	// hidden is logged, NOT returned — see QueryResponse (classification oracle).
	m.logger.Info().Str("peer", peer.ChainID).Str("domain", req.DomainTag).Int("served", len(results)).Int("hidden", hidden).Msg("federation recall served")
	writeJSON(w, http.StatusOK, &QueryResponse{
		ChainID:    m.localChainID,
		Results:    results,
		TotalCount: len(results),
	})
}

// handleReceipt accepts a peer's CommitReceipt push (Mode-2 cross-anchor
// delivery) and anchors it via TxTypeCoCommitAttest on our own chain.
func (m *Manager) handleReceipt(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var push ReceiptPush
	if err := json.NewDecoder(r.Body).Decode(&push); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	// A receipt is an authenticated peer-triggered consensus write. Hold the
	// same policy lease used by data-serving/mutation routes, re-resolve the exact
	// request agreement/operator beneath it, and retain the lease through the
	// blocking attest broadcast. A completed revoke can therefore never be
	// followed by a stale receipt write.
	if ss := m.syncStore(); ss != nil {
		policyUnlock := ss.LockSyncPolicyRead()
		defer policyUnlock()
	}
	if _, err := m.currentRequestAgreementBound(r.Context(), peer); err != nil {
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}
	resp, err := m.handleIncomingReceiptValidated(peer.ChainID, &push)
	if err != nil {
		m.logger.Warn().Err(err).Str("peer", peer.ChainID).Msg("receipt push rejected")
		httpError(w, http.StatusUnprocessableEntity, fmt.Sprintf("receipt rejected: %v", err))
		return
	}
	writeJSON(w, http.StatusOK, resp)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
