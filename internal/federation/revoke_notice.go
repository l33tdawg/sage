package federation

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
)

const revokeNoticeTimeout = 5 * time.Second

// RevokeAgreementNotifying permanently revokes a connection while making a
// best-effort authenticated notification to the exact peer first. Notification
// failure never weakens or blocks the local operator's right to revoke.
func (m *Manager) RevokeAgreementNotifying(remoteChainID string) (*RevokeAgreementResult, error) {
	result := &RevokeAgreementResult{}
	unlock := m.LockAgreementMutation()
	defer unlock()
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		if m.crossFedStatus(remoteChainID) == "revoked" {
			// Idempotent concurrent peer/local revoke: the desired terminal state
			// already committed while this caller waited for the generation lease.
			return result, nil
		}
		return nil, err
	}
	ss := m.syncStore()
	var control *store.SyncControl
	if ss != nil {
		control, err = ss.GetSyncControl(context.Background(), remoteChainID)
	}
	peerBound := false
	if err == nil && control != nil && control.BindingState == "active" && control.PolicyEpoch != "" {
		peerBound = m.syncControlPeerBound(control, &peerIdentity{
			ChainID: remoteChainID, AgentID: control.PeerAgentID, Agreement: agreement,
		})
	}
	if !peerBound {
		result.NoticeError = "peer notification unavailable: connection has no exact active ceremony generation"
	}
	generation := m.BeginSyncPolicyGenerationMutation(remoteChainID)
	defer generation.Restore()

	// Commit our irreversible tx-34 first. The peer is never asked to revoke if
	// local consensus rejects. Keep the exact old CA/seed only long enough to
	// sign the best-effort notice; ActiveAgreement already denies this edge.
	hash, err := m.broadcastRevokeAgreementLockedReason(remoteChainID, "operator disconnect")
	if err != nil {
		return nil, err
	}
	result.TxHash = hash
	if peerBound {
		noticeCtx, cancel := context.WithTimeout(context.Background(), revokeNoticeTimeout)
		noticeErr := m.sendRevokeNotice(noticeCtx, agreement, control.PolicyEpoch)
		cancel()
		if noticeErr != nil {
			result.NoticeError = noticeErr.Error()
		} else {
			result.PeerNotified = true
		}
	}
	m.recordConnectionEvent(remoteChainID, store.FederationConnectionRevokedLocally,
		"This operator permanently revoked trust.")
	m.purgeLocalFederationStateQuiesced(remoteChainID)
	generation.Retire()
	return result, nil
}

func (m *Manager) sendRevokeNotice(ctx context.Context, agreement *store.CrossFedRecord, policyEpoch string) error {
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/connection/revoke-notice", &RevokeNotice{
		PolicyEpoch: policyEpoch,
		Reason:      "The peer operator permanently revoked this connection.",
	})
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return fmt.Errorf("peer returned %d: %s", status, truncate(body, 200))
	}
	var response RevokeNoticeResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return fmt.Errorf("decode revoke notice response: %w", err)
	}
	if response.Status != "revoked" && response.Status != "already_revoked" {
		return fmt.Errorf("peer returned unexpected revoke notice status %q", response.Status)
	}
	return nil
}

func (m *Manager) handleRevokeNotice(w http.ResponseWriter, r *http.Request) {
	peer := peerFromCtx(r.Context())
	if peer == nil || peer.Agreement == nil {
		httpError(w, http.StatusForbidden, "unauthenticated")
		return
	}
	var notice RevokeNotice
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 4096)).Decode(&notice); err != nil {
		httpError(w, http.StatusBadRequest, "invalid revoke notice")
		return
	}
	notice.PolicyEpoch = strings.TrimSpace(notice.PolicyEpoch)
	if notice.PolicyEpoch == "" || len(notice.PolicyEpoch) > 256 || len(notice.Reason) > 512 {
		httpError(w, http.StatusBadRequest, "invalid revoke notice binding")
		return
	}
	response, err := m.acceptPeerRevokeNotice(r.Context(), peer, notice)
	if err != nil {
		m.logger.Warn().Err(err).Str("peer", peer.ChainID).Msg("authenticated peer revoke notice rejected")
		httpError(w, http.StatusConflict, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, response)
}

// acceptPeerRevokeNotice lets either exact peer end a bilateral trust link. It
// never sends another notice, avoiding revoke loops. The peer identity, current
// agreement generation, operator key and ceremony epoch are all revalidated
// beneath the agreement mutation lease before our own tx-34 is signed.
func (m *Manager) acceptPeerRevokeNotice(ctx context.Context, peer *peerIdentity, notice RevokeNotice) (*RevokeNoticeResponse, error) {
	unlock := m.LockAgreementMutation()
	defer unlock()
	current, err := m.currentRequestAgreementBound(ctx, peer)
	if err != nil {
		if m.crossFedStatus(peer.ChainID) == "revoked" {
			return &RevokeNoticeResponse{Status: "already_revoked"}, nil
		}
		return nil, err
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, fmt.Errorf("peer revoke requires the SQLite store backend")
	}
	control, err := ss.GetSyncControl(ctx, peer.ChainID)
	if err != nil || control == nil || control.BindingState != "active" ||
		control.PolicyEpoch != notice.PolicyEpoch {
		return nil, fmt.Errorf("revoke notice does not match the active ceremony epoch")
	}
	peerOperator, err := m.resolvePeerOperatorAgentID(ctx, current)
	if err != nil || peerOperator != peer.AgentID {
		return nil, fmt.Errorf("revoke notice operator does not match the frozen peer")
	}
	generation := m.BeginSyncPolicyGenerationMutation(peer.ChainID)
	defer generation.Restore()

	hash, err := m.revokeAgreementLockedReason(peer.ChainID, "peer disconnect")
	if err != nil {
		return nil, err
	}
	generation.Retire()
	m.recordConnectionEvent(peer.ChainID, store.FederationConnectionRevokedByPeer,
		"The peer operator permanently revoked trust.")
	return &RevokeNoticeResponse{Status: "revoked", TxHash: hash}, nil
}

func (m *Manager) crossFedStatus(remoteChainID string) string {
	if m.badger == nil {
		return ""
	}
	_, _, _, _, _, _, status, err := m.badger.GetCrossFed(remoteChainID)
	if err != nil {
		return ""
	}
	return status
}

func (m *Manager) recordConnectionEvent(remoteChainID, event, message string) {
	if ss := m.syncStore(); ss != nil {
		if err := ss.SetFederationConnectionEvent(context.Background(), store.FederationConnectionEvent{
			RemoteChainID: remoteChainID,
			Event:         event,
			Message:       message,
		}); err != nil {
			m.logger.Warn().Err(err).Str("remote", remoteChainID).Msg("record federation connection event")
		}
	}
}
