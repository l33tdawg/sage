package rest

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/tx"
)

// v11 Mode-2 co-commit REST surface: submit the jointly-signed envelope to the
// LOCAL chain (tx 31) and drive the Phase-B receipt exchange (push our signed
// CommitReceipt to each foreign coauthor chain, which anchors it via tx 32).
// The envelope's coauthor signatures are produced out-of-band by the
// participants (each over the identical CanonicalCoreBytes); this handler is
// the LOCAL submitter's entry point.

// maxCoCommitCoauthorsREST mirrors the consensus cap (maxCoCommitCoauthors=64)
// so the REST pre-verify loop can't be abused for CPU exhaustion.
const maxCoCommitCoauthorsREST = 64

// CoCommitCoauthorJSON is one coauthor entry on the wire (hex-encoded).
type CoCommitCoauthorJSON struct {
	PubKey  string `json:"pub_key"`  // hex ed25519 public key (32 bytes)
	ChainID string `json:"chain_id"` // coauthor's home chain
	Sig     string `json:"sig"`      // hex ed25519 signature over CanonicalCoreBytes (64 bytes)
}

// CoCommitSubmitRequest is the JSON body for POST /v1/cocommit/submit.
type CoCommitSubmitRequest struct {
	SchemaVersion uint32 `json:"schema_version,omitempty"` // default 1
	// Content is the raw shared content. It is hashed for the on-chain
	// envelope; the raw text is staged off-chain (SuppCache) so the local
	// record is recallable. Optional if content_hash is supplied (hash-only
	// co-commit), but then the local copy has no searchable content.
	Content     string `json:"content,omitempty"`
	ContentHash string `json:"content_hash,omitempty"` // hex sha256; derived from content when empty
	MemoryType  string `json:"memory_type"`
	DomainTag   string `json:"domain_tag"`
	// Classification is the FROZEN shared band — identical on every chain (the
	// clearance scale is compile-time constant; see plan §9.8).
	Classification  int     `json:"classification,omitempty"`
	ConfidenceScore float64 `json:"confidence_score"`
	// CreatedAtUnix is the jointly-signed authored time (DATA, part of
	// CanonicalCoreBytes — every coauthor must have signed this exact value).
	CreatedAtUnix  int64                  `json:"created_at_unix"`
	AgreementNonce string                 `json:"agreement_nonce"` // hex
	Coauthors      []CoCommitCoauthorJSON `json:"coauthors"`
	// NotBefore/NotAfter are the jointly-signed validity window (unix seconds;
	// 0 = unbounded). All coauthors must sign the identical window. Enforced
	// on-chain vs blockTime (footgun E).
	NotBefore int64     `json:"not_before,omitempty"`
	NotAfter  int64     `json:"not_after,omitempty"`
	Embedding []float32 `json:"embedding,omitempty"`
	Provider  string    `json:"provider,omitempty"`
	// SkipReceiptDelivery suppresses the post-commit receipt push (deliver
	// later via POST /v1/cocommit/{shared_id}/receipt/send).
	SkipReceiptDelivery bool `json:"skip_receipt_delivery,omitempty"`
}

// CoCommitSubmitResponse reports the committed co-commit and the Phase-B
// receipt fan-out outcome (best-effort; failed deliveries leave the anchor
// "unconfirmed" and are retried via the resend endpoint).
type CoCommitSubmitResponse struct {
	SharedID          string                               `json:"shared_id"`
	CoreHash          string                               `json:"core_hash"`
	TxHash            string                               `json:"tx_hash"`
	Height            int64                                `json:"height"`
	Status            string                               `json:"status"`
	ReceiptDeliveries map[string]federation.DeliveryResult `json:"receipt_deliveries,omitempty"`
}

// fedOpTimeout bounds outbound federation legs launched from REST handlers.
func contextWithFedTimeout(r *http.Request) (context.Context, context.CancelFunc) {
	return context.WithTimeout(r.Context(), fedRecallTimeout())
}

// handleCoCommitSubmit handles POST /v1/cocommit/submit.
func (s *Server) handleCoCommitSubmit(w http.ResponseWriter, r *http.Request) {
	var req CoCommitSubmitRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid JSON", err.Error())
		return
	}
	if !memory.IsValidMemoryType(memory.MemoryType(req.MemoryType)) {
		writeProblem(w, http.StatusBadRequest, "Invalid memory type", "memory_type must be fact, observation, inference, or task.")
		return
	}
	if req.DomainTag == "" {
		writeProblem(w, http.StatusBadRequest, "Missing domain", "domain_tag is required.")
		return
	}
	if req.Classification < 0 || req.Classification > 4 {
		writeProblem(w, http.StatusBadRequest, "Invalid classification", "classification must be 0..4.")
		return
	}
	if req.SchemaVersion == 0 {
		req.SchemaVersion = 1
	}

	// Resolve the content hash: derive from raw content, or accept the caller's
	// (hash-only envelope). When both are present they must agree.
	var contentHash []byte
	if req.Content != "" {
		h := sha256.Sum256([]byte(req.Content))
		contentHash = h[:]
		if req.ContentHash != "" {
			claimed, err := hex.DecodeString(req.ContentHash)
			if err != nil || !bytes.Equal(claimed, contentHash) {
				writeProblem(w, http.StatusBadRequest, "Hash mismatch", "content_hash does not match sha256(content).")
				return
			}
		}
	} else {
		claimed, err := hex.DecodeString(req.ContentHash)
		if err != nil || len(claimed) != sha256.Size {
			writeProblem(w, http.StatusBadRequest, "Invalid content hash", "content or a hex sha256 content_hash is required.")
			return
		}
		contentHash = claimed
	}

	nonce, err := hex.DecodeString(req.AgreementNonce)
	if err != nil || len(nonce) == 0 {
		writeProblem(w, http.StatusBadRequest, "Invalid nonce", "agreement_nonce must be non-empty hex.")
		return
	}

	// Reject an oversized coauthor set up front, mirroring the consensus cap
	// (maxCoCommitCoauthors=64) — otherwise a malicious local caller could
	// force thousands of ed25519 verifies below before the on-chain reject.
	if len(req.Coauthors) > maxCoCommitCoauthorsREST {
		writeProblem(w, http.StatusBadRequest, "Too many coauthors", "a co-commit envelope may declare at most 64 coauthors.")
		return
	}

	coauthors := make([]tx.CoCommitCoauthor, 0, len(req.Coauthors))
	for i, c := range req.Coauthors {
		pub, pubErr := hex.DecodeString(c.PubKey)
		sig, sigErr := hex.DecodeString(c.Sig)
		if pubErr != nil || sigErr != nil || len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize || c.ChainID == "" {
			writeProblem(w, http.StatusBadRequest, "Invalid coauthor", fmt.Sprintf("coauthor %d is malformed.", i))
			return
		}
		coauthors = append(coauthors, tx.CoCommitCoauthor{PubKey: pub, ChainID: c.ChainID, Sig: sig})
	}

	if req.NotBefore != 0 && req.NotAfter != 0 && req.NotAfter <= req.NotBefore {
		writeProblem(w, http.StatusBadRequest, "Invalid validity window", "not_after must be greater than not_before.")
		return
	}
	env := &tx.CoCommitSubmit{
		SchemaVersion:   req.SchemaVersion,
		ContentHash:     contentHash,
		MemoryType:      memoryTypeToTx(req.MemoryType),
		Domain:          req.DomainTag,
		Classification:  tx.ClearanceLevel(req.Classification), // #nosec G115 -- validated 0..4
		ConfidenceScore: req.ConfidenceScore,
		CreatedAtUnix:   req.CreatedAtUnix,
		AgreementNonce:  nonce,
		Coauthors:       coauthors,
		NotBefore:       req.NotBefore,
		NotAfter:        req.NotAfter,
	}
	coreHash := tx.CoreHashOf(env)
	env.SharedID = tx.ComputeSharedID(coreHash, env.Coauthors, nonce)

	// Pre-verify every coauthor signature over the canonical core so a broken
	// envelope fails here with a useful error instead of an opaque consensus
	// reject (the chain re-verifies under consensus rules regardless).
	core := tx.CanonicalCoreBytes(env)
	for _, c := range env.Coauthors {
		if !ed25519.Verify(ed25519.PublicKey(c.PubKey), core, c.Sig) {
			writeProblem(w, http.StatusBadRequest, "Bad coauthor signature",
				"a coauthor signature does not verify over the canonical core — all parties must sign byte-identical CanonicalCoreBytes.")
			return
		}
	}

	// Stage the raw content + embedding off-chain, keyed by SharedID, for the
	// ABCI app to fold into the local record at commit (consensus-first write).
	if s.suppCache != nil && (req.Content != "" || len(req.Embedding) > 0) {
		var embeddingHash []byte
		if len(req.Embedding) > 0 {
			embeddingHash = hashEmbedding(req.Embedding)
		}
		s.suppCache.Put(env.SharedID, &memory.SupplementaryData{
			Content:           req.Content,
			Embedding:         req.Embedding,
			EmbeddingHash:     embeddingHash,
			Provider:          req.Provider,
			EmbeddingProvider: s.embedderStampFor(req.Embedding),
		})
	}

	submitTx := &tx.ParsedTx{
		Type:           tx.TxTypeCoCommitSubmit,
		Nonce:          tx.MonotonicNonce(s.signingKey),
		Timestamp:      time.Now(),
		CoCommitSubmit: env,
	}
	s.embedAgentAuth(r.Context(), submitTx)
	if signErr := tx.SignTx(submitTx, s.signingKey); signErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}
	encoded, err := tx.EncodeTx(submitTx)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}
	hash, height, err := s.broadcastTxCommitWithHeight(encoded)
	if err != nil {
		s.logger.Error().Err(err).Str("shared_id", env.SharedID).Msg("co-commit submit rejected")
		status, msg := broadcastErrorPublic(err)
		writeProblem(w, status, "Co-commit rejected", msg)
		return
	}

	resp := &CoCommitSubmitResponse{
		SharedID: env.SharedID,
		CoreHash: hex.EncodeToString(coreHash),
		TxHash:   hash,
		Height:   height,
		Status:   "committed",
	}

	// Phase B: push our signed receipt to every foreign coauthor chain.
	// Best-effort — failures leave the designed "unconfirmed" steady state.
	if !req.SkipReceiptDelivery && s.federation != nil {
		ctx, cancel := contextWithFedTimeout(r)
		defer cancel()
		resp.ReceiptDeliveries = s.federation.DeliverReceipts(ctx, env.SharedID, height, time.Now().Unix())
	}

	if s.OnEvent != nil {
		s.OnEvent("cocommit", env.SharedID, req.DomainTag, "co-commit committed", map[string]any{
			"shared_id": env.SharedID,
			"coauthors": len(env.Coauthors),
		})
	}
	writeJSON(w, http.StatusCreated, resp)
}

// handleCoCommitReceiptSend handles POST /v1/cocommit/{shared_id}/receipt/send —
// the idempotent Phase-B retry (peer was offline, agreement was set up after
// the co-commit, post-rollback re-anchoring). Height/commit_time are advisory
// receipt data; zero values are protocol-legal (the anchor binds
// SharedID+CoreHash).
func (s *Server) handleCoCommitReceiptSend(w http.ResponseWriter, r *http.Request) {
	if s.federation == nil {
		writeProblem(w, http.StatusNotImplemented, "Federation disabled", "The federation transport is not wired on this node.")
		return
	}
	// Receipt delivery pushes an operator-key-signed receipt to peers; it has no
	// other authz, so gate it to the node operator (consistent with peer status).
	if !s.requireNodeOperator(w, r) {
		return
	}
	sharedID := chi.URLParam(r, "shared_id")
	if !isHexID(sharedID) {
		writeProblem(w, http.StatusBadRequest, "Invalid shared_id", "shared_id must be a 64-char hex id.")
		return
	}
	var req struct {
		Height     int64 `json:"height,omitempty"`
		CommitTime int64 `json:"commit_time,omitempty"`
	}
	_ = decodeJSON(r, &req) // empty body → advisory zeros

	ctx, cancel := contextWithFedTimeout(r)
	defer cancel()
	deliveries := s.federation.DeliverReceipts(ctx, sharedID, req.Height, req.CommitTime)
	writeJSON(w, http.StatusOK, map[string]any{
		"shared_id":          sharedID,
		"receipt_deliveries": deliveries,
	})
}

// handleCoCommitStatus handles GET /v1/cocommit/{shared_id}/status — local
// commit + per-peer anchor confirmation, read directly from on-chain state.
func (s *Server) handleCoCommitStatus(w http.ResponseWriter, r *http.Request) {
	sharedID := chi.URLParam(r, "shared_id")
	if !isHexID(sharedID) {
		writeProblem(w, http.StatusBadRequest, "Invalid shared_id", "shared_id must be a 64-char hex id.")
		return
	}
	if s.badgerStore == nil {
		writeProblem(w, http.StatusNotImplemented, "No chain state", "This node has no on-chain store wired.")
		return
	}
	core, err := s.badgerStore.GetCoCommitCore(sharedID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read co-commit state.")
		return
	}
	if len(core) == 0 {
		writeProblem(w, http.StatusNotFound, "Not found", "No co-commit with that shared_id on this chain.")
		return
	}
	anchors, err := s.badgerStore.ListCoCommitAnchors(sharedID)
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Read error", "Failed to read anchors.")
		return
	}
	type coauthorStatus struct {
		PubKey   string `json:"pub_key"`
		ChainID  string `json:"chain_id"`
		Anchored bool   `json:"anchored"`
	}
	var coauthorList []coauthorStatus
	localChain := ""
	if s.federation != nil {
		localChain = s.federation.LocalChainID()
	}
	if blob, cErr := s.badgerStore.GetCoCommitCoauthors(sharedID); cErr == nil && len(blob) > 0 {
		if coauthors, dErr := tx.DecodeCoauthorsCanonical(blob); dErr == nil {
			for _, c := range coauthors {
				_, anchored := anchors[c.ChainID]
				coauthorList = append(coauthorList, coauthorStatus{
					PubKey:   hex.EncodeToString(c.PubKey),
					ChainID:  c.ChainID,
					Anchored: anchored || (localChain != "" && c.ChainID == localChain), // local commit is its own confirmation
				})
			}
		}
	}
	anchorHex := make(map[string]string, len(anchors))
	for chain, anchor := range anchors {
		anchorHex[chain] = hex.EncodeToString(anchor)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"shared_id": sharedID,
		"core_hash": hex.EncodeToString(core),
		"committed": true,
		"coauthors": coauthorList,
		"anchors":   anchorHex,
	})
}

// isHexID reports whether s is a 64-char lowercase/uppercase hex string (a
// sha256-derived SharedID).
func isHexID(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}

// hashEmbedding mirrors handleSubmitMemory's embedding-hash derivation.
func hashEmbedding(embedding []float32) []byte {
	h := sha256.New()
	for _, v := range embedding {
		fmt.Fprintf(h, "%f", v)
	}
	return h.Sum(nil)
}
