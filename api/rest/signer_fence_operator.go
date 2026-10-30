package rest

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	maxSignerFenceOperatorBody = 4 << 10
	signerFenceRecoveryTimeout = 10 * time.Second
)

type signerFenceStatusResponse struct {
	Active           int                       `json:"active"`
	OldestAgeSeconds int64                     `json:"oldest_age_seconds"`
	Explanation      string                    `json:"explanation"`
	Signers          []signerFenceStatusRow    `json:"signers"`
	LastResolution   *signerFenceResolutionRow `json:"last_resolution,omitempty"`
}

type signerFenceStatusRow struct {
	Signer          string `json:"signer"`
	SignerPublicKey string `json:"signer_public_key"`
	TxHash          string `json:"tx_hash"`
	HeldSeconds     int64  `json:"held_seconds"`
	Since           string `json:"since"`
	Attempts        int    `json:"attempts"`
	Cause           string `json:"cause"`
	Resolution      string `json:"resolution"`
	LastCause       string `json:"last_cause"`
	LastDetail      string `json:"last_detail"`
	LastAttemptAt   string `json:"last_attempt_at,omitempty"`
	Nonce           string `json:"nonce,omitempty"`
}

type signerFenceResolutionRow struct {
	Mode        string `json:"mode"`
	Signer      string `json:"signer"`
	TxHash      string `json:"tx_hash"`
	HeldSeconds int64  `json:"held_seconds"`
	At          string `json:"at"`
	Detail      string `json:"detail"`
	Nonce       string `json:"nonce,omitempty"`
}

// requireSignerFenceOperator keeps fence recovery on the cluster node local.
// This is deliberately narrower than general governance mutation authority:
// the configured governance operator is the only HTTP identity allowed to
// inspect detailed fence state or ask this process to retire a fence. The proof
// itself is never accepted from the request; both handlers read it from this
// node's committed nonce state and CometBFT RPC.
func (s *Server) requireSignerFenceOperator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.governanceOperatorID == "" {
			writeProblem(w, http.StatusServiceUnavailable, "Fence controls unavailable",
				"The local node governance operator identity is not configured.")
			return
		}
		callerID := strings.TrimSpace(middleware.ContextAgentID(r.Context()))
		callerPub, err := auth.AgentIDToPublicKey(callerID)
		if err != nil || auth.PublicKeyToAgentID(callerPub) != s.governanceOperatorID {
			writeProblem(w, http.StatusForbidden, "Fence controls denied",
				"Only the configured local governance operator may inspect or lift signer fences.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// handleSignerFenceStatus exposes the detailed operator view that the public
// health endpoint intentionally omits. Prometheus already publishes the
// coarse alarm pair (active count and oldest age); this route makes the held
// transaction, reconciliation state and proof/operator resolution class
// actionable without requiring access to the desktop CEREBRUM surface.
func (s *Server) handleSignerFenceStatus(w http.ResponseWriter, _ *http.Request) {
	held := tx.FencedSigners()
	response := signerFenceStatusResponse{
		Active:      len(held),
		Explanation: signerFenceOperatorExplanation(held),
		Signers:     make([]signerFenceStatusRow, 0, len(held)),
	}
	if len(held) > 0 {
		response.OldestAgeSeconds = int64(held[0].HeldFor.Round(time.Second).Seconds())
	}
	for _, fence := range held {
		row := signerFenceStatusRow{
			Signer:          fence.SignerPubKeyPrefix,
			SignerPublicKey: fence.SignerPubKeyHex,
			TxHash:          fence.TxHash,
			HeldSeconds:     int64(fence.HeldFor.Round(time.Second).Seconds()),
			Since:           fence.Since.UTC().Format(time.RFC3339),
			Attempts:        fence.Attempts,
			Cause:           fence.Cause,
			Resolution:      fence.Resolution,
			LastCause:       fence.LastCause,
			LastDetail:      fence.LastDetail,
		}
		if !fence.LastAttemptAt.IsZero() {
			row.LastAttemptAt = fence.LastAttemptAt.UTC().Format(time.RFC3339)
		}
		if fence.HasNonce {
			// Nonces are nanosecond allocations and exceed JavaScript's safe
			// integer range, so keep them as strings across the REST boundary.
			row.Nonce = strconv.FormatUint(fence.Nonce, 10)
		}
		response.Signers = append(response.Signers, row)
	}
	if last, ok := tx.LastFenceResolution(); ok {
		row := signerFenceResolutionRow{
			Mode:        last.Mode,
			Signer:      last.SignerPubKeyPrefix,
			TxHash:      last.TxHash,
			HeldSeconds: int64(last.HeldFor.Round(time.Second).Seconds()),
			At:          last.At.UTC().Format(time.RFC3339),
			Detail:      last.Detail,
		}
		if last.HasNonce {
			row.Nonce = strconv.FormatUint(last.Nonce, 10)
		}
		response.LastResolution = &row
	}
	writeJSON(w, http.StatusOK, response)
}

// handleSignerFenceLift reads the proof from this node rather than trusting the
// request. The request names at most a held signer and an operator note.
func (s *Server) handleSignerFenceLift(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSignerFenceOperatorBody)
	var request struct {
		Signer string `json:"signer"`
		Reason string `json:"reason"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		var maxBytesErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxBytesErr):
			writeProblem(w, http.StatusRequestEntityTooLarge, "Fence lift request too large",
				"The signer-fence lift body must be 4 KiB or smaller.")
		case errors.Is(err, io.EOF):
			writeProblem(w, http.StatusBadRequest, "Invalid fence lift request",
				"Request body must name the held signer.")
		default:
			writeProblem(w, http.StatusBadRequest, "Invalid fence lift request", err.Error())
		}
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, http.StatusBadRequest, "Invalid fence lift request",
			"Request body must contain exactly one JSON object.")
		return
	}

	signer := strings.TrimSpace(request.Signer)
	if signer == "" {
		writeProblem(w, http.StatusBadRequest, "Missing signer",
			"signer must be the held fence's full public key or its displayed prefix.")
		return
	}
	reason, err := normalizeSignerFenceReason(request.Reason)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid operator reason", err.Error())
		return
	}

	target, heldCount := signerFenceForSigner(signer)
	if target == nil {
		writeProblem(w, http.StatusNotFound, "No matching signer fence",
			"No signer fence is held for that key or prefix (held fences: "+
				strconv.Itoa(heldCount)+").")
		return
	}

	recoveryCtx, cancel := context.WithTimeout(r.Context(), signerFenceRecoveryTimeout)
	defer cancel()
	proof, err := tx.ProveFenceLiftFromChain(
		recoveryCtx,
		s.cometbftRPC,
		s.signerFenceNonceFloor(),
		*target,
	)
	if err != nil {
		var unproven *tx.FenceLiftUnprovenError
		if errors.As(err, &unproven) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":  "no proof of this transaction's fate yet",
				"detail": unproven.Reason,
				"signer": target.SignerPubKeyPrefix,
			})
			return
		}
		writeProblem(w, http.StatusBadGateway, "Fence proof unavailable", err.Error())
		return
	}
	if reason != "" {
		proof.Detail += " | operator note: " + reason
	}
	if err := tx.LiftFenceWithProof(recoveryCtx, target.SignerPubKeyHex, proof); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{
			"error":  "the fence refused the proof",
			"detail": err.Error(),
			"signer": target.SignerPubKeyPrefix,
		})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"lifted": true,
		"signer": target.SignerPubKeyPrefix,
		"proof": map[string]any{
			"kind":            proof.Kind,
			"detail":          proof.Detail,
			"committed_nonce": proof.CommittedNonce,
		},
		"note": "the fence is lifted and its durable intent retired; a superseded lift means the fenced " +
			"transaction can never commit and its payload is permanently lost",
	})
}

// handleSignerFenceAbandon is the AMID equivalent of the CEREBRUM operator's
// exit for the one fence no proof can settle: a restored fence with no signed
// bytes, no committed or rejected fate, no mempool copy, and no spendable
// allocation. It deliberately leaves every precondition to the node-side
// evidence reader and tx.AbandonUnprovableFence; the request can only accept
// the possible payload loss and acknowledge any live peer redelivery route.
func (s *Server) handleSignerFenceAbandon(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxSignerFenceOperatorBody)
	var request struct {
		Signer                     string `json:"signer"`
		Reason                     string `json:"reason"`
		AcknowledgePayloadLoss     bool   `json:"acknowledge_payload_loss"`
		PeerRedeliveryAcknowledged bool   `json:"peer_redelivery_acknowledged"`
	}
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		var maxBytesErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxBytesErr):
			writeProblem(w, http.StatusRequestEntityTooLarge, "Fence abandon request too large",
				"The signer-fence abandon body must be 4 KiB or smaller.")
		case errors.Is(err, io.EOF):
			writeProblem(w, http.StatusBadRequest, "Invalid fence abandon request",
				"Request body must name the held signer and acknowledge payload loss.")
		default:
			writeProblem(w, http.StatusBadRequest, "Invalid fence abandon request", err.Error())
		}
		return
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		writeProblem(w, http.StatusBadRequest, "Invalid fence abandon request",
			"Request body must contain exactly one JSON object.")
		return
	}

	signer := strings.TrimSpace(request.Signer)
	if signer == "" {
		writeProblem(w, http.StatusBadRequest, "Missing signer",
			"signer must be the held fence's full public key or its displayed prefix.")
		return
	}
	if !request.AcknowledgePayloadLoss {
		writeProblem(w, http.StatusBadRequest, "Payload loss acknowledgement required",
			"acknowledge_payload_loss must be true: this route abandons a transaction whose fate was never "+
				"proven, and a late copy of its payload can still be lost or refused as a replay.")
		return
	}
	reason, err := normalizeSignerFenceReason(request.Reason)
	if err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid operator reason", err.Error())
		return
	}
	if reason == "" {
		writeProblem(w, http.StatusBadRequest, "Missing operator reason",
			"reason is required: the abandonment decision and its evidence are recorded.")
		return
	}

	target, heldCount := signerFenceForSigner(signer)
	if target == nil {
		writeProblem(w, http.StatusNotFound, "No matching signer fence",
			"No signer fence is held for that key or prefix (held fences: "+
				strconv.Itoa(heldCount)+").")
		return
	}

	recoveryCtx, cancel := context.WithTimeout(r.Context(), signerFenceRecoveryTimeout)
	defer cancel()
	evidence, err := tx.ReadFenceAbandonEvidence(
		recoveryCtx,
		s.cometbftRPC,
		s.signerFenceNonceFloor(),
		*target,
	)
	if err != nil {
		writeProblem(w, http.StatusBadGateway, "Fence evidence unavailable",
			"The node's own abandonment evidence could not be read, so nothing was abandoned: "+err.Error())
		return
	}
	evidence.PeerRedeliveryAcknowledged = request.PeerRedeliveryAcknowledged

	if err := tx.AbandonUnprovableFence(
		recoveryCtx,
		target.SignerPubKeyHex,
		reason,
		evidence,
	); err != nil {
		var refused *tx.FenceAbandonRefusedError
		if errors.As(err, &refused) {
			writeJSON(w, http.StatusConflict, map[string]any{
				"error":    "the fence was not abandoned",
				"detail":   refused.Reason,
				"signer":   target.SignerPubKeyPrefix,
				"evidence": evidence,
			})
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Fence abandon failed",
			"The fence was not abandoned: "+err.Error())
		return
	}

	nonce := ""
	if target.HasNonce {
		nonce = strconv.FormatUint(target.Nonce, 10)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"abandoned": true,
		"signer":    target.SignerPubKeyPrefix,
		"tx_hash":   target.TxHash,
		"nonce":     nonce,
		"evidence":  evidence,
		"recorded": "the decision is in the node log as a fence_abandoned event and in " +
			"sage_nonce_fence_resolved_total{fate=\"abandoned\"}; the durable intent is retired, and the " +
			"abandoned nonce is reserved so the next allocation for this signer is above it",
		"warning": "the transaction this fence protected was never proven: if a copy of it still exists " +
			"anywhere and lands before the signer's next commit, it can still commit, and a later transaction " +
			"would then be refused as a replay. Verify the effect on-chain before redoing that action by hand.",
	})
}

func signerFenceForSigner(signer string) (*tx.FencedSigner, int) {
	held := tx.FencedSigners()
	for i := range held {
		if strings.EqualFold(held[i].SignerPubKeyHex, signer) ||
			strings.EqualFold(held[i].SignerPubKeyPrefix, signer) {
			return &held[i], len(held)
		}
	}
	return nil, len(held)
}

func (s *Server) signerFenceNonceFloor() func(ed25519.PublicKey) (uint64, bool) {
	if s.badgerStore == nil {
		return nil
	}
	return func(pub ed25519.PublicKey) (uint64, bool) {
		nonce, err := s.badgerStore.GetNonce(auth.PublicKeyToAgentID(pub))
		if err != nil || nonce == 0 {
			return 0, false
		}
		return nonce, true
	}
}

func normalizeSignerFenceReason(reason string) (string, error) {
	reason = strings.TrimSpace(reason)
	if len(reason) > 512 {
		return "", errors.New("reason must be at most 512 bytes")
	}
	for _, r := range reason {
		if r < 0x20 || r == 0x7f {
			return "", errors.New("reason must not contain control characters")
		}
	}
	return reason, nil
}

func signerFenceOperatorExplanation(held []tx.FencedSigner) string {
	if len(held) == 0 {
		return "No signing key is fenced. The Prometheus alert pair remains zero."
	}
	reconciling, proofOrOperator := 0, 0
	for _, fence := range held {
		if fence.Resolution == "reconciling" {
			reconciling++
		} else {
			proofOrOperator++
		}
	}
	switch {
	case reconciling > 0 && proofOrOperator > 0:
		return "Some fences still hold their signed bytes and are reconciling; the others can lift only " +
			"on proof that this node reads from the chain or through an explicitly acknowledged operator " +
			"abandonment."
	case proofOrOperator > 0:
		return "These fences were restored without their signed bytes and can lift only on proof that this " +
			"node reads from the chain or through an explicitly acknowledged operator abandonment."
	default:
		return "The node is reconciling the exact signed bytes and will lift a fence only after consensus " +
			"proves the transaction's fate."
	}
}
