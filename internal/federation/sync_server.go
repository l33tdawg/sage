package federation

// Receive side of v11.5 domain sync: POST /fed/v1/sync/push.
//
// A pushed item is admitted by wrapping it in a LOCALLY-SIGNED
// TxTypeMemorySubmit broadcast through CometBFT (the HandleIncomingReceipt
// wrap-in-local-tx pattern) — federation code never writes chain state
// directly. The copy lands as a native memory, authored on-chain by this
// node's operator agent; origin truth (chain, id, timestamp) lives in the
// off-consensus sync_origin ledger, which is also the idempotency replay
// source and the anti-entropy digest source.
//
// Gate order (all before ANY store write or broadcast):
//  1. peerAuth (router middleware): active agreement, cert-to-chain bind,
//     chain-qualified sig, nonce + replay cache, body cap.
//  2. Structural validation — protocol violations fail the WHOLE batch 400:
//     origin chain must equal the authenticated peer (a peer may only push
//     its OWN memories; kills third-chain laundering at the door), content
//     hash must verify, caps respected.
//  3. Treaty scope: item domain covered by the agreement's AllowedDomains.
//  4. Receiver consent: item domain covered by the receiver's OWN
//     sync_domains rows for this peer (asymmetric consent narrows).
//  5. Clearance: sender-asserted classification <= agreement MaxClearance
//     (no admission path checks clearance today; this is the enforcement).
//  6. Idempotency: a recorded sync_origin decision replays verbatim.
//  7. B-D1: same content committed in a DIFFERENT domain -> terminal reject,
//     surfaced; same domain -> duplicate success. Never rely on InsertMemory's
//     ON CONFLICT (it silently overwrites domain_tag).
//  8. Vault locked -> per-item retry NACK, nothing recorded.
//  9. Admit via broadcast; classify the ABCI result; record the decision.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// syncPushBudget bounds how long one push batch may spend admitting items —
// safely inside the federation listener's 15s write timeout. Items not
// reached in time return "retry" (the sender's outbox redelivers).
const syncPushBudget = 12 * time.Second

// syncMemoryID derives the deterministic local id for a synced copy. Domain-
// separated ("sync1|") and origin-keyed, so every redelivery of the same
// origin pair rebuilds the SAME id and the consensus terminal-status guard
// turns retries into duplicate-successes.
func syncMemoryID(originChainID, originMemoryID string) string {
	sum := sha256.Sum256([]byte("sync1|" + originChainID + "|" + originMemoryID))
	return hex.EncodeToString(sum[:])
}

// syncStore returns the SQLite store the sync tables live on, or nil on any
// other backend (sync is SQLite-only and must refuse loudly).
func (m *Manager) syncStore() *store.SQLiteStore {
	ss, _ := m.memStore.(*store.SQLiteStore)
	return ss
}

// handleSyncPush implements POST /fed/v1/sync/push (behind peerAuth).
func (m *Manager) handleSyncPush(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	ss := m.syncStore()
	if ss == nil {
		httpError(w, http.StatusNotImplemented, "domain sync requires the SQLite store backend")
		return
	}
	var req SyncPushRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}
	if len(req.Items) == 0 {
		httpError(w, http.StatusBadRequest, "empty batch")
		return
	}
	if len(req.Items) > SyncPushMaxItems {
		httpError(w, http.StatusBadRequest, fmt.Sprintf("batch exceeds %d items", SyncPushMaxItems))
		return
	}
	// Structural validation is all-or-nothing: violations are sender bugs (or
	// malice), not policy outcomes, and rejecting the batch keeps the per-item
	// outcome enum small and honest.
	for i := range req.Items {
		if err := validateSyncItem(peer.ChainID, &req.Items[i]); err != nil {
			httpError(w, http.StatusBadRequest, fmt.Sprintf("item %d: %v", i, err))
			return
		}
	}

	// Receiver-side consent rows for this peer, loaded once per batch.
	consented, err := ss.GetSyncDomains(r.Context(), peer.ChainID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, "consent lookup failed")
		return
	}

	deadline := time.Now().Add(syncPushBudget)
	results := make([]SyncItemResult, 0, len(req.Items))
	for i := range req.Items {
		item := &req.Items[i]
		if time.Now().After(deadline) {
			// Out of listener budget — the rest of the batch retries later.
			results = append(results, SyncItemResult{OriginMemoryID: item.OriginMemoryID, Outcome: SyncOutcomeRetry})
			continue
		}
		res := m.admitSyncItem(r, ss, peer, consented, item)
		results = append(results, res)
	}
	writeJSON(w, http.StatusOK, &SyncPushResponse{Results: results})
}

// validateSyncItem enforces the structural (protocol-level) invariants.
func validateSyncItem(peerChainID string, item *SyncItem) error {
	if item.OriginMemoryID == "" || item.OriginChainID == "" {
		return errors.New("origin_chain_id and origin_memory_id are required")
	}
	if item.OriginChainID != peerChainID {
		// A peer may only push memories it claims as its OWN. Forwarded
		// third-chain content is refused at the door (no A->B->C laundering);
		// the sender-side no-re-forwarding gate is the polite half of this.
		return fmt.Errorf("origin chain %q does not match authenticated peer %q", item.OriginChainID, peerChainID)
	}
	if item.Domain == "" {
		return errors.New("domain is required")
	}
	if item.Content == "" {
		return errors.New("content is required")
	}
	if len(item.Content) > SyncMaxItemContent {
		return fmt.Errorf("content exceeds %d bytes", SyncMaxItemContent)
	}
	if item.Classification < 0 || item.Classification > 4 {
		return errors.New("classification out of range")
	}
	sum := sha256.Sum256([]byte(item.Content))
	if hex.EncodeToString(sum[:]) != strings.ToLower(item.ContentHash) {
		return errors.New("content_hash does not match content")
	}
	return nil
}

// admitSyncItem runs gates 3-9 for one item and returns its outcome.
func (m *Manager) admitSyncItem(r *http.Request, ss *store.SQLiteStore, peer *peerIdentity, consented []string, item *SyncItem) SyncItemResult {
	ctx := r.Context()
	out := SyncItemResult{OriginMemoryID: item.OriginMemoryID}

	record := func(outcome, localID string) {
		if err := ss.RecordSyncOrigin(ctx, store.SyncOrigin{
			OriginChainID:   item.OriginChainID,
			OriginMemoryID:  item.OriginMemoryID,
			OriginCreatedAt: item.OriginCreatedAt,
			LocalMemoryID:   localID,
			DomainTag:       item.Domain,
			Outcome:         outcome,
		}); err != nil {
			m.logger.Error().Err(err).Str("origin", item.OriginMemoryID).Msg("sync: record origin failed")
		}
	}

	// Gate 3 — treaty scope.
	if !DomainAllowed(peer.Agreement.AllowedDomains, item.Domain) {
		record(store.SyncOutcomeRejectedDomainScope, "")
		out.Outcome = SyncOutcomeRejectedScope
		return out
	}
	// Gate 4 — receiver consent (concrete rows only; DomainAllowed gives the
	// same subtree semantics: consented "hr" covers item domain "hr.public").
	if !DomainAllowed(consented, item.Domain) {
		record(store.SyncOutcomeRejectedNotConsented, "")
		out.Outcome = SyncOutcomeRejectedConsent
		return out
	}
	// Gate 5 — clearance ceiling.
	if item.Classification > int(peer.Agreement.MaxClearance) {
		record(store.SyncOutcomeRejectedClearance, "")
		out.Outcome = SyncOutcomeRejectedClearance
		return out
	}
	// Gate 6 — idempotency: replay a recorded decision verbatim.
	if prior, err := ss.GetSyncOrigin(ctx, item.OriginChainID, item.OriginMemoryID); err == nil {
		out.Outcome, out.LocalMemoryID = replaySyncOutcome(prior)
		return out
	} else if !errors.Is(err, sql.ErrNoRows) {
		out.Outcome = SyncOutcomeRetry
		return out
	}
	// Gate 7 — B-D1 cross-domain duplicate (committed-only, domain-aware).
	matches, err := ss.FindCommittedByContentHashDomains(ctx, item.ContentHash)
	if err != nil {
		out.Outcome = SyncOutcomeRetry
		return out
	}
	for _, mt := range matches {
		if mt.DomainTag == item.Domain {
			// Same content already committed in the SAME domain: idempotent
			// success, mapped to the existing local row.
			record(store.SyncOutcomeAdmitted, mt.MemoryID)
			out.Outcome = SyncOutcomeDuplicate
			out.LocalMemoryID = mt.MemoryID
			return out
		}
	}
	if len(matches) > 0 {
		// Identical content lives in a DIFFERENT domain: reject + surface,
		// never silently move (locked decision B-D1).
		record(store.SyncOutcomeRejectedDupXDomain, "")
		out.Outcome = SyncOutcomeRejectedXDomainDup
		return out
	}
	// Gate 8 — vault locked: the store would reject the mirror write; NACK
	// without recording so the sender redelivers after unlock.
	if ss.VaultLocked() {
		out.Outcome = SyncOutcomeRetry
		return out
	}

	// Gate 9 — admit through consensus.
	localID := syncMemoryID(item.OriginChainID, item.OriginMemoryID)
	outcome, txHash := m.broadcastSyncSubmit(localID, item)
	switch outcome {
	case SyncOutcomeAccepted, SyncOutcomeDuplicate:
		record(store.SyncOutcomeAdmitted, localID)
		m.logger.Info().Str("origin", item.OriginChainID+"/"+item.OriginMemoryID).
			Str("local", localID).Str("tx", txHash).Str("outcome", outcome).Msg("sync: item admitted")
	case SyncOutcomeRejectedScope:
		record(store.SyncOutcomeRejectedDomainScope, "")
	}
	out.Outcome = outcome
	if outcome == SyncOutcomeAccepted || outcome == SyncOutcomeDuplicate {
		out.LocalMemoryID = localID
	}
	return out
}

// replaySyncOutcome maps a recorded sync_origin decision back onto the wire.
func replaySyncOutcome(prior *store.SyncOrigin) (string, string) {
	switch prior.Outcome {
	case store.SyncOutcomeAdmitted:
		return SyncOutcomeDuplicate, prior.LocalMemoryID
	case store.SyncOutcomeRejectedDupXDomain:
		return SyncOutcomeRejectedXDomainDup, ""
	case store.SyncOutcomeRejectedClearance:
		return SyncOutcomeRejectedClearance, ""
	case store.SyncOutcomeRejectedNotConsented:
		return SyncOutcomeRejectedConsent, ""
	case store.SyncOutcomeRejectedDomainScope:
		return SyncOutcomeRejectedScope, ""
	default:
		// Unknown ledger state — fail toward redelivery rather than lying.
		return SyncOutcomeRetry, ""
	}
}

// broadcastSyncSubmit wraps the item in a locally-signed TxTypeMemorySubmit
// and broadcasts it, classifying the result. Retries ONCE on a nonce race
// with a freshly-encoded tx (never re-broadcast stored bytes: post-v15
// canonical encoding + nonce monotonicity both reject).
func (m *Manager) broadcastSyncSubmit(localID string, item *SyncItem) (string, string) {
	// Bound concurrent blocking broadcasts (shared with receipt pushes).
	m.broadcastSem <- struct{}{}
	defer func() { <-m.broadcastSem }()

	for attempt := 0; attempt < 2; attempt++ {
		encoded, err := m.buildSyncSubmitTx(localID, item)
		if err != nil {
			m.logger.Error().Err(err).Str("local", localID).Msg("sync: build submit tx failed")
			return SyncOutcomeRetry, ""
		}
		hash, _, err := m.broadcastTxCommit(encoded)
		outcome := classifySyncBroadcast(err)
		if outcome == syncBcastNonceRace && attempt == 0 {
			continue // fresh nonce on the rebuild
		}
		switch outcome {
		case syncBcastOK:
			return SyncOutcomeAccepted, hash
		case syncBcastDuplicate:
			return SyncOutcomeDuplicate, ""
		case syncBcastScopeReject:
			return SyncOutcomeRejectedScope, ""
		default:
			if err != nil {
				m.logger.Warn().Err(err).Str("local", localID).Msg("sync: submit broadcast failed")
			}
			return SyncOutcomeRetry, ""
		}
	}
	return SyncOutcomeRetry, ""
}

// buildSyncSubmitTx constructs the operator-signed MemorySubmit for a synced
// copy (the RevokeAgreement operator-auth pattern: agent proof over a
// canonical body hash + timestamp, then the tx signature).
func (m *Manager) buildSyncSubmitTx(localID string, item *SyncItem) ([]byte, error) {
	contentHash, err := hex.DecodeString(item.ContentHash)
	if err != nil {
		return nil, fmt.Errorf("decode content hash: %w", err)
	}
	body := []byte("sync_admit:" + localID)
	bodyHash := sha256.Sum256(body)
	ts := time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts)) // #nosec G115 -- ts non-negative
	agentSig := ed25519.Sign(m.agentKey, append(append([]byte{}, bodyHash[:]...), tsBytes...))

	memType := syncMemoryTypeToTx(item.MemoryType)
	confidence := item.ConfidenceScore
	if confidence <= 0 || confidence > 1 {
		confidence = 0.5 // sender-asserted junk defaults to proposal-grade; the voter decides
	}
	ptx := &tx.ParsedTx{
		Type:      tx.TxTypeMemorySubmit,
		Nonce:     tx.MonotonicNonce(m.agentKey),
		Timestamp: time.Now(),
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        localID,
			ContentHash:     contentHash,
			MemoryType:      memType,
			DomainTag:       item.Domain,
			ConfidenceScore: confidence,
			Content:         item.Content,
			Classification:  tx.ClearanceLevel(item.Classification), // #nosec G115 -- validated 0-4
		},
		AgentPubKey:    m.agentPub,
		AgentSig:       agentSig,
		AgentBodyHash:  bodyHash[:],
		AgentTimestamp: ts,
	}
	if err := tx.SignTx(ptx, m.agentKey); err != nil {
		return nil, fmt.Errorf("sign sync submit tx: %w", err)
	}
	return tx.EncodeTx(ptx)
}

// syncMemoryTypeToTx maps the wire memory-type string onto the tx enum.
// Unknown or empty types default to fact — the copy still goes through the
// receiver's voter, and "task" deliberately maps to fact too: a synced copy
// must never appear on the receiver's live task board as claimable work.
func syncMemoryTypeToTx(s string) tx.MemoryType {
	switch s {
	case "observation":
		return tx.MemoryTypeObservation
	case "inference":
		return tx.MemoryTypeInference
	default:
		return tx.MemoryTypeFact
	}
}

// Broadcast outcome classes for a sync admit.
type syncBcastClass int

const (
	syncBcastOK syncBcastClass = iota
	syncBcastDuplicate
	syncBcastScopeReject
	syncBcastNonceRace
	syncBcastRetry
)

// classifySyncBroadcast maps a broadcastTxCommit error onto an outcome class.
// ABCI error codes are overloaded (Code 11 spans malformed/authz/terminal),
// so this branches on Log text — an acknowledged soft contract with
// internal/abci/app.go's literals, pinned by TestClassifySyncBroadcast so a
// wording change over there fails loudly over here.
func classifySyncBroadcast(err error) syncBcastClass {
	if err == nil {
		return syncBcastOK
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "already reached terminal status"),
		strings.Contains(msg, "cannot be overwritten by a normal submit"):
		// The deterministic sync id already landed (a prior delivery raced
		// the origin record, or an identical redelivery) — success-equivalent.
		return syncBcastDuplicate
	case strings.Contains(msg, "no write access to domain"):
		// The operator agent lacks write access to the target domain on THIS
		// chain — receiver-side configuration, terminal until re-consented.
		return syncBcastScopeReject
	case strings.Contains(msg, "nonce too low"), strings.Contains(msg, "nonce 0 not permitted"):
		return syncBcastNonceRace
	default:
		return syncBcastRetry
	}
}
