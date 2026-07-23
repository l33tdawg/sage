package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
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

// ErrPeerOffline marks only ordinary dial/name-resolution failures for which
// an exact, previously authenticated routing snapshot may be used to enqueue
// work locally. TLS, certificate, HTTP, identity, and decode failures never
// wrap this sentinel and therefore never permit cached authorization fallback.
var ErrPeerOffline = errors.New("federation peer is offline")

func isPeerOfflineDialError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPeerOffline) {
		return true
	}
	var dnsErr *net.DNSError
	if errors.As(err, &dnsErr) {
		return true
	}
	var opErr *net.OpError
	if errors.As(err, &opErr) && opErr.Op == "dial" && opErr.Timeout() {
		return true
	}
	return errors.Is(err, syscall.ECONNREFUSED) || errors.Is(err, syscall.ENETUNREACH) ||
		errors.Is(err, syscall.EHOSTUNREACH)
}

func authenticatePeerRoute(tlsCfg *tls.Config) PeerRouteAuthenticator {
	return func(ctx context.Context, result PeerRouteDialResult, dialErr error) (PeerRouteDialResult, error) {
		if dialErr != nil {
			closeRouteResult(result)
			result.Conn = nil
			return result, dialErr
		}
		if result.Conn == nil {
			return result, errors.New("route returned no connection")
		}
		if result.Authenticated || tlsCfg == nil {
			result.Authenticated = true
			return result, nil
		}
		raw := result.Conn
		conn := tls.Client(raw, tlsCfg.Clone())
		if err := conn.HandshakeContext(ctx); err != nil {
			_ = raw.Close()
			result.Conn = nil
			return result, err
		}
		result.Conn = conn
		result.Authenticated = true
		return result, nil
	}
}

// doPeerRequest performs one signed mTLS request against an agreement's
// endpoint. Fail-closed by construction: no agreement, bad endpoint scheme,
// missing/pin-mismatched CA, or TLS failure all error before any bytes leave.
func (m *Manager) doPeerRequest(ctx context.Context, agreement *store.CrossFedRecord, method, path string, payload any) ([]byte, int, error) {
	if !m.transportIsEnabled() {
		err := errors.New("federation transport is disabled")
		m.recordRouteFailure(agreement.RemoteChainID, err, false)
		return nil, 0, err
	}
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
	authenticate := authenticatePeerRoute(tlsCfg)
	p2pOnly := agreement.Endpoint == joinP2POnlyEndpoint
	var selectedMu sync.Mutex
	selected := PeerRouteDialResult{Kind: RouteKindDirect, Target: endpoint.Host}
	routeDial := m.peerRouteDialFunc()
	if routeDial == nil {
		if legacyDial := m.peerDialFunc(); legacyDial != nil {
			routeDial = func(dialCtx context.Context, chain string, _ PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
				start := time.Now()
				conn, handled, dialErr := legacyDial(dialCtx, chain)
				return PeerRouteDialResult{Conn: conn, Kind: RouteKindP2PDirect, Latency: time.Since(start)}, handled, dialErr
			}
		}
	}
	if routeDial != nil || !p2pOnly {
		directDialer := &net.Dialer{}
		transport.DialTLSContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
			attempts := make([]routeDialAttempt, 0, 2)
			if !p2pOnly {
				attempts = append(attempts, routeDialAttempt{
					dial: func(attemptCtx context.Context) (PeerRouteDialResult, error) {
						start := time.Now()
						conn, dialErr := directDialer.DialContext(attemptCtx, network, address)
						return authenticate(attemptCtx, PeerRouteDialResult{
							Conn: conn, Kind: RouteKindDirect, Target: address,
							Latency: time.Since(start),
						}, dialErr)
					},
				})
			}
			if routeDial != nil {
				delay := routeCandidateDelay
				if p2pOnly {
					delay = 0
				}
				attempts = append(attempts, routeDialAttempt{
					delay: delay,
					dial: func(attemptCtx context.Context) (PeerRouteDialResult, error) {
						result, handled, dialErr := routeDial(attemptCtx, agreement.RemoteChainID, authenticate)
						if !handled {
							return PeerRouteDialResult{}, errors.New("peer has no configured p2p route")
						}
						if !result.Authenticated {
							return authenticate(attemptCtx, result, dialErr)
						}
						return result, dialErr
					},
				})
			}
			winner, dialErr := raceRouteDials(dialCtx, attempts)
			if dialErr != nil {
				if isSecurityTransportError(dialErr) {
					return nil, fmt.Errorf("peer %s route authentication failed: %w", agreement.RemoteChainID, dialErr)
				}
				return nil, fmt.Errorf("%w: peer %s routes unavailable: %v", ErrPeerOffline, agreement.RemoteChainID, dialErr)
			}
			selectedMu.Lock()
			selected = winner
			selectedMu.Unlock()
			return winner.Conn, nil
		}
	} else {
		transport.DialTLSContext = func(context.Context, string, string) (net.Conn, error) {
			return nil, fmt.Errorf("%w: peer %s has no p2p dialer", ErrPeerOffline, agreement.RemoteChainID)
		}
	}
	// Deliberately no client-wide Timeout or shorter ResponseHeaderTimeout: the
	// caller's context is authoritative. Some authenticated receipt and ceremony
	// operations include a bounded consensus commit wait; their call sites own
	// the exact budget. The reserved peer Write route returns 501 before dialing.
	client := &http.Client{Transport: transport}
	defer client.CloseIdleConnections()
	resp, err := client.Do(req)
	if err != nil {
		securityFailure := isSecurityTransportError(err)
		m.recordRouteFailure(agreement.RemoteChainID, err, securityFailure)
		if !securityFailure && path != "/fed/v1/p2p/routes" {
			m.triggerRouteRefresh(agreement.RemoteChainID)
		}
		if isPeerOfflineDialError(err) {
			return nil, 0, fmt.Errorf("%w: peer %s: %v", ErrPeerOffline, agreement.RemoteChainID, err)
		}
		return nil, 0, fmt.Errorf("peer %s unreachable: %w", agreement.RemoteChainID, err)
	}
	selectedMu.Lock()
	chosen := selected
	selectedMu.Unlock()
	m.recordRouteSuccess(agreement.RemoteChainID, chosen)
	if chosen.Kind == RouteKindDirect && routeDial != nil && path != "/fed/v1/p2p/routes" {
		m.maybeTriggerRouteRefresh(agreement.RemoteChainID)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, maxFedResponseBytes))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read peer response: %w", err)
	}
	return respBody, resp.StatusCode, nil
}

func isSecurityTransportError(err error) bool {
	if err == nil {
		return false
	}
	var unknownAuthority x509.UnknownAuthorityError
	var hostname x509.HostnameError
	var invalidCert x509.CertificateInvalidError
	var recordHeader tls.RecordHeaderError
	if errors.As(err, &unknownAuthority) || errors.As(err, &hostname) ||
		errors.As(err, &invalidCert) || errors.As(err, &recordHeader) {
		return true
	}
	// crypto/tls wraps several alert types in private concrete errors. They are
	// distinguishable from dial errors by the stable operation/error text.
	text := strings.ToLower(err.Error())
	return strings.Contains(text, "tls:") || strings.Contains(text, "x509:") ||
		strings.Contains(text, "certificate") || strings.Contains(text, "spki")
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
		return nil, fmt.Errorf("peer %s returned %d: %s", agreement.RemoteChainID, status, truncate(body, 200))
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
	var control *store.SyncControl
	if ss := m.syncStore(); ss != nil {
		if current, controlErr := ss.GetSyncControl(ctx, remoteChainID); controlErr == nil {
			control = current
		}
	}
	out, err := m.fetchPeerStatus(ctx, agreement)
	if err != nil {
		return nil, err
	}
	// A status response is authenticated by the exact active agreement, so its
	// cosmetic network label is safe to cache for the dashboard. This also heals
	// labels missing from pre-friendly-name JOIN ceremonies without changing any
	// trust or authorization state.
	m.rememberPeerName(remoteChainID, out.NetworkName)
	// Preserve the last authenticated contact projection for exact-address
	// offline queueing. This is best-effort for general status callers; the
	// target resolver performs the same refresh as a required operation using
	// its own immutable request-time binding.
	if control != nil {
		if cacheErr := m.refreshRemotePipeContactCache(ctx, agreement, control, out); cacheErr != nil {
			m.logger.Debug().Err(cacheErr).Str("peer", remoteChainID).Msg("could not refresh authenticated remote pipe contact cache")
		}
	}
	return out, nil
}

// fetchPeerStatus performs only the authenticated network exchange. Cache
// callers supply their request-time agreement/control binding separately so a
// delayed response can never be relabeled with post-response policy state.
func (m *Manager) fetchPeerStatus(ctx context.Context, agreement *store.CrossFedRecord) (*StatusResponse, error) {
	if agreement == nil {
		return nil, fmt.Errorf("peer status agreement is unavailable")
	}
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodGet, "/fed/v1/status", nil)
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer %s returned %d: %s", agreement.RemoteChainID, status, truncate(body, 200))
	}
	var out StatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("decode peer response: %w", err)
	}
	if out.ChainID != agreement.RemoteChainID {
		return nil, fmt.Errorf("peer identifies as %q, agreement expects %q", out.ChainID, agreement.RemoteChainID)
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
