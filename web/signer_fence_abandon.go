package web

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/l33tdawg/sage/internal/tx"
)

// handleSignerFenceAbandon is the operator's exit for the ONE fence the node
// cannot prove anything about: one restored from durable intent whose signed
// bytes did not survive the process that sent them, on a node where nothing can
// deliver that transaction back (no peers, no copy in the mempool, no committed
// fate, allocation not spent).
//
// WHY A SEPARATE ROUTE FROM lift. The lift route accepts PROOFS and refuses
// everything weaker; that is its whole contract, and folding an unproven exit
// into it would make "409 no proof yet" ambiguous — the operator could no
// longer tell whether the node had proved something or merely been told to give
// up. Here the semantics are the opposite: the node has already established
// that no proof can be expected, and the operator is explicitly accepting that
// a transaction's payload may be lost. The request must SAY so
// (acknowledge_payload_loss) and must carry a reason, both of which are
// recorded with the decision, because this is the only lift in SAGE whose fate
// the chain never settled.
//
// PEERS ARE AN ACKNOWLEDGEMENT, NOT A VETO. A connected peer is a route the
// fenced transaction could still take back into this node's mempool, and the
// node cannot see whether any peer ever held those bytes. What it CAN see is
// that the route exists, so when peers are connected the request must set
// peer_redelivery_acknowledged — a separate field from acknowledge_payload_loss
// because they are different facts: the first says "I know a peer could still
// deliver this", the second says "I accept that the payload may be lost". A
// blanket refusal whenever a peer was connected made this route unusable on
// exactly the nodes that need it (federated desktop nodes and validators with
// persistent peers), which is how an unprovable fence became a node that could
// never write again. The automatic route keeps the strict rule: it abandons
// only when no peer is connected and none has been seen since the fence was
// raised.
//
// WHAT THE NODE DECIDES, NOT THE CALLER. Every precondition is read by
// ReadFenceAbandonEvidence and re-checked in tx.AbandonUnprovableFence against
// the live fence. The request body cannot assert a nonce, a peer count, an
// empty mempool or a missing block — it can only ask.
func (h *DashboardHandler) handleSignerFenceAbandon(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Signer string `json:"signer"`
		Reason string `json:"reason"`
		// AcknowledgePayloadLoss must be true. The field name is the warning:
		// there is no way to spell this request without asserting that the
		// transaction the fence is protecting may be discarded.
		AcknowledgePayloadLoss bool `json:"acknowledge_payload_loss"`
		// PeerRedeliveryAcknowledged must be true when the node has peers
		// connected. It is deliberately a SECOND field rather than folded into
		// AcknowledgePayloadLoss: "a peer could still deliver this" and "the
		// payload may be lost" are different facts about different actors, and
		// an operator reading the recorded decision must be able to tell which
		// one was accepted.
		PeerRedeliveryAcknowledged bool `json:"peer_redelivery_acknowledged"`
	}
	decoder := json.NewDecoder(io.LimitReader(r.Body, 4<<10))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil && !errors.Is(err, io.EOF) {
		writeJSONResp(w, http.StatusBadRequest, map[string]any{
			"error": "invalid request body", "detail": err.Error(),
		})
		return
	}
	signer := strings.TrimSpace(request.Signer)
	if signer == "" {
		writeJSONResp(w, http.StatusBadRequest, map[string]any{
			"error": "signer is required (the fence's signer public key or the prefix /health prints)",
		})
		return
	}
	if !request.AcknowledgePayloadLoss {
		writeJSONResp(w, http.StatusBadRequest, map[string]any{
			"error": "acknowledge_payload_loss must be true",
			"detail": "this route abandons a transaction the chain never gave a verdict on: if any copy of it " +
				"still exists, it will be refused as a replay and its payload will be lost. Set " +
				"acknowledge_payload_loss to true to take that decision.",
		})
		return
	}
	if strings.TrimSpace(request.Reason) == "" {
		writeJSONResp(w, http.StatusBadRequest, map[string]any{
			"error":  "reason is required",
			"detail": "the decision is recorded with the fence's evidence, so it must say why it was taken",
		})
		return
	}

	target, heldCount := fenceForSigner(signer)
	if target == nil {
		writeJSONResp(w, http.StatusNotFound, map[string]any{
			"error": "no signer fence is held for that signer (use the full public key from the health row's signer_public_key; the signer field is a truncated display form)",
			"held":  heldCount,
		})
		return
	}

	evidence, err := tx.ReadFenceAbandonEvidence(r.Context(), h.CometBFTRPC, h.signerFenceNonceFloor(), *target)
	if err != nil {
		writeJSONResp(w, http.StatusBadGateway, map[string]any{
			"error":  "the node's own evidence could not be read, so nothing was abandoned",
			"detail": err.Error(),
			"signer": target.SignerPubKeyPrefix,
		})
		return
	}
	evidence.PeerRedeliveryAcknowledged = request.PeerRedeliveryAcknowledged

	if err := tx.AbandonUnprovableFence(r.Context(), target.SignerPubKeyHex, request.Reason, evidence); err != nil {
		var refused *tx.FenceAbandonRefusedError
		if errors.As(err, &refused) {
			writeJSONResp(w, http.StatusConflict, map[string]any{
				"error":    "the fence was not abandoned",
				"detail":   refused.Reason,
				"signer":   target.SignerPubKeyPrefix,
				"evidence": evidence,
			})
			return
		}
		writeJSONResp(w, http.StatusInternalServerError, map[string]any{
			"error":  "the fence was not abandoned",
			"detail": err.Error(),
		})
		return
	}

	writeJSONResp(w, http.StatusOK, map[string]any{
		"abandoned": true,
		"signer":    target.SignerPubKeyPrefix,
		"tx_hash":   target.TxHash,
		"nonce":     target.Nonce,
		"evidence":  evidence,
		"recorded": "the decision is in the node log as a fence_abandoned event and in " +
			"sage_nonce_fence_resolved_total{fate=\"abandoned\"}; the durable intent is retired, and the " +
			"abandoned nonce is reserved so the next allocation for this signer is above it",
		"warning": "the transaction this fence protected was never proven: if a copy of it still exists " +
			"anywhere and lands before the signer's next commit, it can still commit, and a later transaction " +
			"would then be refused as a replay. Verify the effect on-chain before redoing that action by hand.",
	})
}
