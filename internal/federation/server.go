package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/idfmt"
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
	// The linked-reader eligibility oracle accepts exactly one 64-hex ID.
	// Keep its authenticated request budget independent of recall embeddings.
	maxFederatedGuestEligibilityRequestBytes = 1 << 10
	minPipeContactHumanSelectorRunes         = 2
	pipeContactLookupQueryTimeout            = 2 * time.Second
	maxFedTopK                               = 50
	defaultFedTopK                           = 10
	maxTimestampSkew                         = 5 * time.Minute
	// maxReplayEntriesPerChain bounds each PEER CHAIN's replay shard. Total
	// listener memory is bounded by (active peer chains × this) — and one
	// peer's flood is confined to its own shard.
	maxReplayEntriesPerChain = 4000
	queryPlanTTL             = 90 * time.Second
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
		r.Post("/fed/v1/query/plan", m.handleQueryPlan)
		r.Post("/fed/v1/query", m.handleQuery)
		r.Post("/fed/v1/write", m.handleRemoteWrite)
		r.Post("/fed/v1/receipt", m.handleReceipt)
		r.Post("/fed/v1/connection/revoke-notice", m.handleRevokeNotice)
		r.Post("/fed/v1/pipe/event", m.handlePipeEvent)
		r.Post("/fed/v1/pipe/contacts/lookup", m.handlePipeContactLookup)
		r.Post("/fed/v1/pipe/linked/resolve", m.handleLinkedMessageResolve)
		r.Post("/fed/v1/pipe/linked/directory", m.handleLinkedMessageDirectory)
		r.Post("/fed/v1/pipe/linked/revalidate", m.handleLinkedMessageRevalidate)
		r.Post("/fed/v1/pipe/linked/consent-offer", m.handleLinkedMessageConsentOffer)
		r.Post("/fed/v1/pipe/linked/consent-candidates", m.handleLinkedMessageConsentCandidates)
		r.Post("/fed/v1/guest/agent/eligibility", m.handleFederatedGuestAgentEligibility)
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
		if r.Method == http.MethodPost {
			switch r.URL.Path {
			case "/fed/v1/pipe/contacts/lookup":
				bodyLimit = maxPipeContactLookupRequestBytes
			case "/fed/v1/guest/agent/eligibility":
				bodyLimit = maxFederatedGuestEligibilityRequestBytes
			case "/fed/v1/pipe/linked/resolve":
				bodyLimit = maxLinkedMessageResolveBytes
			case "/fed/v1/pipe/linked/directory":
				bodyLimit = maxLinkedMessageDirectoryRequestBytes
			case "/fed/v1/pipe/linked/revalidate":
				bodyLimit = maxLinkedMessageResolveBytes
			case "/fed/v1/pipe/linked/consent-offer":
				bodyLimit = maxLinkedMessageResolveBytes
			case "/fed/v1/pipe/linked/consent-candidates":
				bodyLimit = maxLinkedMessageResolveBytes
			}
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
				m.logger.Warn().Str("peer", peerChain).Str("agent", idfmt.Prefix(agentID)).Msg("federation request denied: bad signature")
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
	if _, readyErr := m.v23BindingReady(r.Context(), agreement, peer.AgentID); readyErr == nil {
		if digest, digestErr := m.agreementBindingDigestV23(r.Context(), agreement, peer.AgentID); digestErr == nil {
			response.FederationProtocolVersion = FederationProtocolV23
			response.QueryAgreementBindingDigest = digest
			response.Capabilities = append(response.Capabilities,
				CapabilityFederationV23, CapabilityQueryAgentProofV2,
				CapabilityFederatedGuestAgentEligibility)
		}
	} else {
		m.logger.Debug().Err(readyErr).Str("peer", peer.ChainID).
			Msg("federation status has no ready v23 query binding")
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
	if req.Name != "" && utf8.RuneCountInString(req.Name) < minPipeContactHumanSelectorRunes {
		httpError(w, http.StatusBadRequest, "contact name selector is too short")
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
	// Human substring matching may require a bounded-output SQLite scan. Do
	// that work before taking the revocation-sensitive policy/ownership/contact
	// leases, then reload only the resulting (max 64) IDs under the leases
	// below. A concurrent removal therefore either disappears on reload or
	// waits for the live RBAC projection; a peer cannot hold revoke behind a
	// leading-wildcard roster scan.
	var prefetchedNameCandidateIDs []string
	humanTarget := req.Target != "" &&
		!strings.HasPrefix(req.Target, "#") &&
		!isCanonicalAgentID(req.Target)
	if _, chainID := splitPipeAddress(req.Target); chainID != "" {
		humanTarget = false
	}
	if req.Name != "" || humanTarget {
		query := req.Name
		if query == "" {
			query = req.Target
		}
		if utf8.RuneCountInString(query) < minPipeContactHumanSelectorRunes {
			httpError(w, http.StatusBadRequest, "contact name selector is too short")
			return
		}
		lookupCtx, cancelLookup := context.WithTimeout(r.Context(), pipeContactLookupQueryTimeout)
		candidates, candidateErr := ss.FindPipeContactLookupCandidates(lookupCtx, query, maxPipeContactLookupCandidates)
		cancelLookup()
		if candidateErr != nil {
			if errors.Is(candidateErr, context.DeadlineExceeded) || errors.Is(candidateErr, context.Canceled) {
				httpError(w, http.StatusServiceUnavailable, "pipe contact candidate lookup timed out")
				return
			}
			httpError(w, http.StatusInternalServerError, "pipe contact candidate lookup failed")
			return
		}
		prefetchedNameCandidateIDs = make([]string, 0, len(candidates))
		for _, candidate := range candidates {
			if candidate != nil {
				prefetchedNameCandidateIDs = append(prefetchedNameCandidateIDs, candidate.AgentID)
			}
		}
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
	grant, total, err := m.buildPipeContactLookupGrant(r.Context(), peer, policy, req, prefetchedNameCandidateIDs)
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
	query = strings.TrimSpace(query)
	if query == "" {
		return false, false
	}
	foldedQuery := strings.ToLower(query)
	for _, candidate := range []string{contact.DisplayName, contact.RegisteredName, contact.Provider} {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if strings.EqualFold(candidate, query) {
			return true, false
		}
		if strings.Contains(strings.ToLower(candidate), foldedQuery) {
			partial = true
		}
	}
	return false, partial
}

// handleQuery serves a scoped read-only recall to an authenticated peer.
// app-v23 requires a nested original-agent proof and one signed, generation-
// bound local guest link. Peer-wide tx-33/peer-RBAC scope is deliberately not a
// fallback: a valid peer operator is transport authentication, not user access.
// handleQueryPlan issues the exact destination state an original agent must
// sign before a recall. The outer peer operator signature authenticates the
// source node, but is never itself enough to consume this challenge or read.
func (m *Manager) handleQueryPlan(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var req QueryPlanRequest
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		httpError(w, http.StatusBadRequest, "invalid trailing JSON")
		return
	}
	if _, err := auth.AgentIDToPublicKey(req.AgentID); err != nil || req.DomainTag == "" {
		httpError(w, http.StatusBadRequest, "valid agent_id and exact domain_tag are required")
		return
	}

	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusServiceUnavailable, "federation v23 policy store is unavailable")
		return
	}
	policyUnlock := ss.LockSyncPolicyRead()
	defer policyUnlock()
	if m.badger == nil {
		httpError(w, http.StatusServiceUnavailable, "federation v23 authorization store is unavailable")
		return
	}
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	defer ownerUnlock()

	agreement, err := m.currentRequestAgreementBound(r.Context(), peer)
	if err != nil {
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}
	policy, err := m.v23BindingReady(r.Context(), agreement, peer.AgentID)
	if err != nil || !peerRBACAllowsRead(policy, req.DomainTag) {
		httpError(w, http.StatusForbidden, "peer Read grant is inactive for this domain")
		return
	}
	digest, err := m.agreementBindingDigestV23(r.Context(), agreement, peer.AgentID)
	if err != nil {
		httpError(w, http.StatusForbidden, "federation v23 binding is unavailable")
		return
	}
	if _, err := m.authorizeFederatedGuestRead(
		r.Context(), peer, agreement, req.AgentID, req.DomainTag,
	); err != nil {
		httpError(w, http.StatusForbidden, "remote agent has no active linked-reader grant for this domain")
		return
	}

	challengeBytes := make([]byte, 32)
	if _, err := rand.Read(challengeBytes); err != nil {
		httpError(w, http.StatusInternalServerError, "could not generate query challenge")
		return
	}
	now := time.Now()
	expiresAt := now.Add(queryPlanTTL).Unix()
	challengeID := hex.EncodeToString(challengeBytes)
	if err := m.queryChallengeStore.IssueFederatedQueryChallenge(r.Context(), store.FederatedQueryChallenge{
		ChallengeID:            challengeID,
		RemoteChainID:          peer.ChainID,
		PeerAgentID:            peer.AgentID,
		RequestedAgentID:       req.AgentID,
		DomainTag:              req.DomainTag,
		AgreementBindingDigest: digest,
		ExpiresAt:              expiresAt,
	}, now); err != nil {
		m.logger.Warn().Err(err).Str("peer", peer.ChainID).Msg("federation v23 query plan denied")
		httpError(w, http.StatusServiceUnavailable, "could not persist query challenge")
		return
	}
	writeJSON(w, http.StatusOK, &QueryPlanResponse{
		ProtocolVersion:        FederationProtocolV23,
		SourceChainID:          peer.ChainID,
		DestinationChainID:     m.localChainID,
		AgreementBindingDigest: digest,
		QueryChallenge:         challengeID,
		ExpiresAt:              expiresAt,
	})
}

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
	// Global federation disclosure lock order is:
	//   sync-policy lease -> Badger authorization/ownership lease -> SQL reads.
	// Consensus group membership/domain ownership and the node-local guest link
	// therefore remain one coherent authorization snapshot until the response
	// has been completely written.
	if m.badger != nil {
		ownerUnlock := m.badger.LockDomainOwnershipRead()
		defer ownerUnlock()
	}
	agreement, agreementErr := m.currentRequestAgreementBound(r.Context(), peer)
	if agreementErr != nil {
		httpError(w, http.StatusForbidden, "federation agreement is no longer active for this operator")
		return
	}
	policy, policyErr := m.v23BindingReady(r.Context(), agreement, peer.AgentID)
	if policyErr != nil || !peerRBACAllowsRead(policy, req.DomainTag) {
		httpError(w, http.StatusForbidden, "peer Read grant is inactive for this domain")
		return
	}
	guestCeiling, authErr := m.validateQueryEnvelopeV23(r.Context(), peer, agreement, &req)
	if authErr != nil {
		m.logger.Warn().Err(authErr).Str("peer", peer.ChainID).Msg("federation v23 query denied")
		httpError(w, http.StatusForbidden, authErr.Error())
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
		Provider:     req.Provider,
		StatusFilter: string(memory.StatusCommitted), // committed-only, non-negotiable
		TopK:         topK,
		Tags:         req.Tags,
	}
	// Admission happens before TopK is consumed. Besides preventing denied rows
	// from starving a valid page, ValidateMemoryProjection closes the
	// SQL-before-Badger publication/crash window: an off-chain row is not
	// federatable until its exact hash/status/domain/author snapshot exists in
	// canonical state.
	opts.CandidateFilter = func(rec *memory.MemoryRecord) (bool, error) {
		if rec == nil || rec.Status != memory.StatusCommitted ||
			!peerRBACAllowsRead(policy, rec.DomainTag) {
			return false, nil
		}
		canonical, projectionErr := m.badger.ValidateMemoryProjection(rec)
		if projectionErr != nil {
			if errors.Is(projectionErr, store.ErrMemoryProjectionUnpublished) {
				return false, nil
			}
			return false, projectionErr
		}
		recordCeiling, guestErr := m.authorizeFederatedGuestRead(
			r.Context(), peer, agreement, req.AgentProof.AgentID, rec.DomainTag,
		)
		if guestErr != nil {
			return false, nil
		}
		return canonical.Classification <= guestCeiling &&
			canonical.Classification <= recordCeiling, nil
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
	recordIDs := make([]string, len(records))
	for i, rec := range records {
		recordIDs[i] = rec.MemoryID
	}
	corrCounts, corrErr := m.memStore.GetCorroborationCounts(r.Context(), recordIDs)
	challengeCounts, challengeErr := m.memStore.GetChallengeCounts(r.Context(), recordIDs)
	if challengeErr != nil {
		m.logger.Warn().Err(challengeErr).Str("peer", peer.ChainID).Msg("federation challenge counts unavailable")
	}
	evidenceComplete, evidenceCompleteErr := store.EvidenceProjectionCompleteness(
		r.Context(), m.memStore, recordIDs,
	)
	if evidenceCompleteErr != nil {
		m.logger.Warn().Err(evidenceCompleteErr).Str("peer", peer.ChainID).
			Msg("federation evidence projection completeness unavailable")
	}
	results := make([]*MemoryResult, 0, len(records))
	hidden := 0
	for _, rec := range records {
		if rec.Status != memory.StatusCommitted {
			hidden++
			continue
		}
		if !peerRBACAllowsRead(policy, rec.DomainTag) {
			hidden++
			continue
		}
		recordCeiling, guestErr := m.authorizeFederatedGuestRead(
			r.Context(), peer, agreement, req.AgentProof.AgentID, rec.DomainTag,
		)
		if guestErr != nil {
			hidden++
			continue
		}
		// Re-check the exact canonical projection immediately before
		// serialization so a concurrent consensus publication cannot swap the
		// row's hash/status/domain/author underneath the pre-TopK decision.
		canonical, projectionErr := m.badger.ValidateMemoryProjection(rec)
		if projectionErr != nil ||
			canonical.Classification > guestCeiling ||
			canonical.Classification > recordCeiling {
			hidden++
			continue
		}
		if corrErr != nil && req.MinConfidence > 0 {
			// Fail closed under a floor: a boost-less (understated) confidence could
			// wrongly drop this record on the requesting side. Hide rather than mislead.
			hidden++
			continue
		}
		corrCount := corrCounts[rec.MemoryID]
		results = append(results, &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    memory.ComputeConfidenceForRecord(rec, now, corrCount),
			CorroborationCount: corrCount,
			ChallengeCount:     challengeCounts[rec.MemoryID],
			EvidenceCountsAvailable: corrErr == nil && challengeErr == nil &&
				evidenceCompleteErr == nil && evidenceComplete[rec.MemoryID],
			Classification: int(canonical.Classification),
			Status:         string(rec.Status),
			CreatedAt:      rec.CreatedAt,
			CommittedAt:    rec.CommittedAt,
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
