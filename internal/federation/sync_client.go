package federation

// Sender side of v11.5 domain sync: the /fed/v1/sync/* client wrappers.
// Same shape as QueryPeer/PushReceipt — ActiveAgreement (fail-closed) ->
// doPeerRequest (mTLS + pinned CA + chain-qualified V2/V3 signing) ->
// classify -> unmarshal.

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/l33tdawg/sage/internal/store"
)

// ErrSyncUnsupported reports that the peer does not expose /fed/v1/sync/*:
// either a pre-v11.5 binary (chi 404s unknown routes BEFORE peerAuth, so a
// 404/405 is distinguishable from an auth failure) or a Postgres-backed node
// (501). The outbox parks such rows on a long backoff instead of burning the
// retry budget.
var ErrSyncUnsupported = errors.New("peer does not support domain sync")

// classifySyncStatus maps an HTTP status from a /fed/v1/sync/* call onto the
// three outcomes both SyncPush and SyncDigest share. Extracted so the
// load-bearing unsupported-peer detection (404/405/501, distinct from auth
// churn) is unit-testable and a refactor that breaks it fails a test.
func classifySyncStatus(status int) (ok bool, unsupported bool) {
	switch status {
	case http.StatusOK:
		return true, false
	case http.StatusNotFound, http.StatusMethodNotAllowed, http.StatusNotImplemented:
		return false, true
	default:
		return false, false
	}
}

// SyncPush delivers one batch of items to a peer's sync admission handler.
func (m *Manager) SyncPush(ctx context.Context, remoteChainID string, req *SyncPushRequest) (*SyncPushResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/push", req)
	if err != nil {
		return nil, err
	}
	if ok, unsupported := classifySyncStatus(status); !ok {
		if unsupported {
			return nil, ErrSyncUnsupported
		}
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out SyncPushResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sync push response: %w", err)
	}
	// Bind the response to the batch: exactly one result per pushed item.
	// A peer answering with a different shape is misbehaving; treating it as
	// an error retries the batch rather than mis-mapping outcomes onto rows.
	if len(out.Results) != len(req.Items) {
		return nil, fmt.Errorf("peer %s returned %d results for %d items", remoteChainID, len(out.Results), len(req.Items))
	}
	return &out, nil
}

// SyncDigest fetches one page of the peer's admission set for a domain
// subtree (anti-entropy reconciliation).
func (m *Manager) SyncDigest(ctx context.Context, remoteChainID string, req *SyncDigestRequest) (*SyncDigestResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/sync/digest", req)
	if err != nil {
		return nil, err
	}
	if ok, unsupported := classifySyncStatus(status); !ok {
		if unsupported {
			return nil, ErrSyncUnsupported
		}
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out SyncDigestResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sync digest response: %w", err)
	}
	return &out, nil
}

func (m *Manager) SyncPolicyPush(ctx context.Context, remoteChainID string, req *SyncPolicyRequest) (*SyncPolicyResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	return m.syncPolicyPushAgreement(ctx, agreement, req)
}

// syncPolicyPushAgreement sends with the exact agreement snapshot captured at
// delivery linearization. It must never re-resolve remoteChainID to a newer E2
// after the caller built an E1 policy payload.
func (m *Manager) syncPolicyPushAgreement(ctx context.Context, agreement *store.CrossFedRecord, req *SyncPolicyRequest) (*SyncPolicyResponse, error) {
	if agreement == nil {
		return nil, fmt.Errorf("sync policy agreement is unavailable")
	}
	remoteChainID := agreement.RemoteChainID
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPut, "/fed/v1/sync/policy", req)
	if err != nil {
		return nil, err
	}
	if ok, unsupported := classifySyncStatus(status); !ok {
		if unsupported {
			return nil, ErrSyncUnsupported
		}
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out SyncPolicyResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode sync policy response: %w", err)
	}
	if out.Revision != req.Revision {
		return nil, fmt.Errorf("peer acknowledged wrong sync policy revision")
	}
	return &out, nil
}
