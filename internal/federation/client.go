package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
)

// receiptDeliveryTimeout bounds a single receipt push (which blocks on the
// peer's broadcast_tx_commit). Broadcast-scale, not read-scale; env-tunable.
const defaultReceiptDeliveryTimeout = 20 * time.Second

func receiptDeliveryTimeout() time.Duration {
	if v := os.Getenv("SAGE_FED_RECEIPT_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultReceiptDeliveryTimeout
}

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
	if _, readErr := rand.Read(nonce); readErr != nil {
		return nil, 0, fmt.Errorf("generate nonce: %w", readErr)
	}
	ts := time.Now().Unix()

	// Sign v3 (rotating TOTP factor) when a shared seed is unlocked in cache for
	// this agreement; otherwise v2. The receiver's fail-closed gate rejects v2
	// once a seed is established, so a downgrade cannot be forced.
	sigVersion := SigVersion2
	var sig []byte
	if seed, ok := m.currentSeed(agreement.RemoteChainID); ok {
		if ownPin, pErr := m.ownPin(); pErr == nil {
			k := DeriveKTOTP(seed, m.localChainID, ownPin, agreement.RemoteChainID, agreement.PeerPubKey)
			sig = auth.SignRequestV3(m.agentKey, k, m.localChainID, agreement.RemoteChainID, method, path, body, ts, nonce)
			sigVersion = SigVersion3
		}
	}
	if sig == nil {
		sig = auth.SignRequestV2(m.agentKey, m.localChainID, agreement.RemoteChainID, method, path, body, ts, nonce)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(HeaderSigVersion, sigVersion)
	req.Header.Set(HeaderChainID, m.localChainID)
	req.Header.Set(HeaderAgentID, hex.EncodeToString(m.agentPub))
	req.Header.Set(HeaderTimestamp, strconv.FormatInt(ts, 10))
	req.Header.Set(HeaderNonce, hex.EncodeToString(nonce))
	req.Header.Set(HeaderSignature, hex.EncodeToString(sig))

	transport := &http.Transport{TLSClientConfig: tlsCfg}
	if peerDial := m.peerDialFunc(); peerDial != nil {
		directDialer := &net.Dialer{}
		transport.DialContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
			conn, handled, dialErr := peerDial(dialCtx, agreement.RemoteChainID)
			if handled {
				if dialErr != nil {
					return nil, fmt.Errorf("peer %s p2p dial: %w", agreement.RemoteChainID, dialErr)
				}
				if conn == nil {
					return nil, fmt.Errorf("peer %s p2p dial returned no connection", agreement.RemoteChainID)
				}
				return conn, nil
			}
			return directDialer.DialContext(dialCtx, network, address)
		}
	}
	client := &http.Client{Transport: transport}
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
//
// Each push runs CONCURRENTLY with its OWN broadcast-scale deadline derived
// from context.Background() — NOT the caller's read ctx. Each push blocks on the
// PEER's broadcast_tx_commit (~a block) plus a fresh mTLS handshake, so sharing
// the 4s recall-read budget across sequential peers timed out every peer after
// the first (star anchoring with 3+ participants). The caller's ctx is honored
// only for outright cancellation.
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

	sem := make(chan struct{}, maxFanOutConcurrency)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, chain := range chains {
		wg.Add(1)
		//nolint:gosec // Receipt delivery intentionally uses per-peer broadcast deadlines independent of the caller's read ctx.
		go func(chain string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			// Per-peer deadline, independent of the caller's read budget, but
			// still cancellable if the caller's ctx is cancelled.
			pctx, cancel := context.WithTimeout(context.Background(), receiptDeliveryTimeout())
			defer cancel()
			pctx = mergeCancel(pctx, ctx)
			resp, pushErr := m.PushReceipt(pctx, chain, push)
			mu.Lock()
			if pushErr != nil {
				results[chain] = DeliveryResult{Status: "error", Error: pushErr.Error()}
			} else {
				results[chain] = DeliveryResult{Status: resp.Status, TxHash: resp.TxHash}
			}
			mu.Unlock()
		}(chain)
	}
	wg.Wait()
	return results
}

// mergeCancel returns a context that is cancelled when EITHER parent is (its own
// deadline, or the caller's cancellation) — so a per-peer deadline bounds the
// push while a client disconnect still aborts it.
func mergeCancel(primary, alsoCancelOn context.Context) context.Context {
	ctx, cancel := context.WithCancel(primary)
	go func() {
		select {
		case <-alsoCancelOn.Done():
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		return string(b[:n]) + "…"
	}
	return string(b)
}
