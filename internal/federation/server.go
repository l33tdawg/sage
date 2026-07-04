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
	"strconv"
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
	maxFedBodyBytes  = 4 << 20 // embeddings are ~30KB; 4MB is generous headroom
	maxFedTopK       = 50
	defaultFedTopK   = 10
	maxTimestampSkew = 5 * time.Minute
	// maxReplayEntriesPerChain bounds each PEER CHAIN's replay shard. Total
	// listener memory is bounded by (active peer chains × this) — and one
	// peer's flood is confined to its own shard.
	maxReplayEntriesPerChain = 4000
)

type peerCtxKey struct{}

// peerIdentity is what peerAuth binds for downstream handlers.
type peerIdentity struct {
	ChainID   string
	AgentID   string
	Agreement *store.CrossFedRecord
}

// Router returns the federation listener's HTTP handler. EVERY route sits
// behind peerAuth — there is no unauthenticated surface on this listener.
func (m *Manager) Router() http.Handler {
	r := chi.NewRouter()
	r.Group(func(r chi.Router) {
		r.Use(m.peerAuth)
		r.Get("/fed/v1/status", m.handleStatus)
		r.Post("/fed/v1/query", m.handleQuery)
		r.Post("/fed/v1/receipt", m.handleReceipt)
	})
	// The pre-agreement JOIN ceremony routes sit behind joinAuth, NOT peerAuth
	// (no active agreement exists yet during a join).
	m.mountJoinRoutes(r)
	return r
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

		agreement, err := m.ActiveAgreement(peerChain)
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

		r.Body = http.MaxBytesReader(w, r.Body, maxFedBodyBytes)
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

		ctx := context.WithValue(r.Context(), peerCtxKey{}, &peerIdentity{
			ChainID:   peerChain,
			AgentID:   agentID,
			Agreement: agreement,
		})
		next.ServeHTTP(w, r.WithContext(ctx))
	})
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

// handleStatus — authenticated reachability/identity preflight.
func (m *Manager) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, &StatusResponse{ChainID: m.localChainID, Time: time.Now().Unix()})
}

// handleQuery serves a scoped read-only recall to an authenticated peer.
// Authorization is AGREEMENT-level: the peer node has already authorized its
// own requesting agent under its local rules; here we enforce OUR side of the
// treaty — allowed domains, the MaxClearance ceiling, committed-only — and
// nothing else. Local-agent RBAC (resolveVisibleAgents et al.) is deliberately
// NOT consulted: a foreign chain has no local org membership (the same reason
// co-commit verifies coauthors standalone).
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

	// Scope gate on the REQUESTED domain. A domainless query is only accepted
	// under a wildcard agreement — otherwise the store would search across all
	// domains and rely purely on post-filtering; reject instead (fail closed,
	// and the error tells the caller to scope).
	if !DomainAllowed(peer.Agreement.AllowedDomains, req.DomainTag) {
		if req.DomainTag == "" {
			httpError(w, http.StatusForbidden, "agreement is domain-scoped: a domain_tag is required")
		} else {
			httpError(w, http.StatusForbidden, "domain not covered by agreement")
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
	opts := store.QueryOptions{
		DomainTag:     req.DomainTag,
		MinConfidence: req.MinConfidence,
		StatusFilter:  string(memory.StatusCommitted), // committed-only, non-negotiable
		TopK:          topK,
		Tags:          req.Tags,
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
	now := time.Now()
	results := make([]*MemoryResult, 0, len(records))
	hidden := 0
	for _, rec := range records {
		if rec.Status != memory.StatusCommitted {
			hidden++
			continue
		}
		if !DomainAllowed(peer.Agreement.AllowedDomains, rec.DomainTag) {
			hidden++
			continue
		}
		// Fail CLOSED on a classification read error: GetMemoryClassification
		// returns (0, err) on a corrupt/unreadable entry, and 0 ≤ every ceiling
		// — swallowing the error would DISCLOSE an arbitrarily-classified record
		// across the federation boundary. This is the sole clearance gate on the
		// egress path, so an error hides the record.
		memClass, classErr := m.badger.GetMemoryClassification(rec.MemoryID)
		if classErr != nil || memClass > peer.Agreement.MaxClearance {
			hidden++
			continue
		}
		corrs, _ := m.memStore.GetCorroborations(r.Context(), rec.MemoryID)
		results = append(results, &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    memory.ComputeConfidence(rec.ConfidenceScore, rec.CreatedAt, now, len(corrs), rec.DomainTag),
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
	resp, err := m.HandleIncomingReceipt(peer.ChainID, &push)
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
