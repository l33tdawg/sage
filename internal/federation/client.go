package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// Outbound federation client — dials a peer's federation listener over mTLS
// (our node cert as client cert, the agreement's pinned CA as the only trust
// root) and signs every request with the chain-qualified scheme
// (X-Sig-Version=2), so the request is valid for exactly the
// (our chain → their chain) pair.

const maxFedResponseBytes = 16 << 20

// doPeerRequest performs one signed mTLS request against an agreement's
// endpoint. Fail-closed by construction: no agreement, bad endpoint scheme,
// missing/pin-mismatched CA, or TLS failure all error before any bytes leave.
func (m *Manager) doPeerRequest(ctx context.Context, agreement *store.CrossFedRecord, method, path string, payload any) ([]byte, int, error) {
	endpoint, err := url.Parse(strings.TrimRight(agreement.Endpoint, "/"))
	if err != nil {
		return nil, 0, fmt.Errorf("agreement %s: invalid endpoint: %w", agreement.RemoteChainID, err)
	}
	if endpoint.Scheme != "https" {
		return nil, 0, fmt.Errorf("agreement %s: endpoint must be https", agreement.RemoteChainID)
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return nil, 0, fmt.Errorf("marshal request: %w", err)
	}

	tlsCfg, err := m.clientTLSConfig(agreement.RemoteChainID, agreement.PeerPubKey)
	if err != nil {
		return nil, 0, err
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint.String()+path, bytes.NewReader(body))
	if err != nil {
		return nil, 0, fmt.Errorf("build request: %w", err)
	}

	nonce := make([]byte, 16)
	if _, err := rand.Read(nonce); err != nil {
		return nil, 0, fmt.Errorf("generate nonce: %w", err)
	}
	ts := time.Now().Unix()
	sig := auth.SignRequestV2(m.agentKey, m.localChainID, agreement.RemoteChainID, method, path, body, ts, nonce)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderSigVersion, SigVersion2)
	req.Header.Set(HeaderChainID, m.localChainID)
	req.Header.Set(HeaderAgentID, hex.EncodeToString(m.agentPub))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderNonce, hex.EncodeToString(nonce))
	req.Header.Set(HeaderSignature, hex.EncodeToString(sig))

	client := &http.Client{Transport: &http.Transport{TLSClientConfig: tlsCfg}}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("peer %s unreachable: %w", agreement.RemoteChainID, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxFedResponseBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read peer response: %w", err)
	}
	return respBody, resp.StatusCode, nil
}

// QueryPeer runs one scoped recall against a remote chain.
func (m *Manager) QueryPeer(ctx context.Context, remoteChainID string, qr *QueryRequest) (*QueryResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/query", qr)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out QueryResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode peer response: %w", err)
	}
	return &out, nil
}

// PushReceipt delivers our signed CommitReceipt to one peer.
func (m *Manager) PushReceipt(ctx context.Context, remoteChainID string, push *ReceiptPush) (*ReceiptPushResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost, "/fed/v1/receipt", push)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out ReceiptPushResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode peer response: %w", err)
	}
	return &out, nil
}

// PeerStatus runs the authenticated reachability preflight against one peer.
func (m *Manager) PeerStatus(ctx context.Context, remoteChainID string) (*StatusResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodGet, "/fed/v1/status", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d: %s", remoteChainID, status, truncate(body, 200))
	}
	var out StatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode peer response: %w", err)
	}
	if out.ChainID != remoteChainID {
		return nil, fmt.Errorf("peer identifies as %q, agreement expects %q", out.ChainID, remoteChainID)
	}
	return &out, nil
}

// DeliverReceipts builds this chain's signed receipt for sharedID once and
// pushes it to every foreign coauthor chain (Mode-2 Phase-B anchoring).
// Best-effort per peer: failures are reported, never fatal — a missing anchor
// is the designed "unconfirmed" steady state, retried via the idempotent
// resend endpoint.
func (m *Manager) DeliverReceipts(ctx context.Context, sharedID string, height, commitTime int64) map[string]DeliveryResult {
	results := make(map[string]DeliveryResult)
	push, err := m.BuildSignedReceipt(sharedID, height, commitTime)
	if err != nil {
		results["*"] = DeliveryResult{Status: "error", Error: err.Error()}
		return results
	}
	chains, err := m.ForeignCoauthorChains(sharedID)
	if err != nil {
		results["*"] = DeliveryResult{Status: "error", Error: err.Error()}
		return results
	}
	for _, chain := range chains {
		resp, pushErr := m.PushReceipt(ctx, chain, push)
		if pushErr != nil {
			results[chain] = DeliveryResult{Status: "error", Error: pushErr.Error()}
			continue
		}
		results[chain] = DeliveryResult{Status: resp.Status, TxHash: resp.TxHash}
	}
	return results
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
