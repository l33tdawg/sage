package federation

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
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
	"slices"
	"sort"
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

// maxFederatedQueryAuthorizationLease bounds how long a peer can retain the
// source node's sync-policy and Badger authorization read leases after final
// attestation. context.WithTimeout preserves any earlier caller deadline; this
// is only the hard ceiling for direct Manager callers without one.
const maxFederatedQueryAuthorizationLease = 5 * time.Second

func boundedFederatedQueryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, maxFederatedQueryAuthorizationLease)
}

// A v11.13.0 peer ignores the compact-status preference and can legally send
// its older full v1 contact snapshot. Allow that compatibility retry one at a
// time so an eight-peer named discovery fan-out never multiplies the legacy
// 16 MiB response allowance.
const maxConcurrentLegacyPipeStatusFallbacks = 1

// ErrPeerOffline marks only ordinary dial/name-resolution failures for which
// an exact, previously authenticated routing snapshot may be used to enqueue
// work locally. TLS, certificate, HTTP, identity, and decode failures never
// wrap this sentinel and therefore never permit cached authorization fallback.
var ErrPeerOffline = errors.New("federation peer is offline")

var errPeerResponseLimit = errors.New("peer response exceeds size limit")

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
	return m.doPeerRequestWithHeaders(ctx, agreement, method, path, payload, nil)
}

func (m *Manager) doPeerRequestWithHeaders(ctx context.Context, agreement *store.CrossFedRecord, method, path string, payload any, headers http.Header) ([]byte, int, error) {
	if _, traced := ctx.Value(peerRouteTraceKey{}).(*peerRouteTrace); !traced {
		ctx, _ = WithPeerRouteAttemptTrace(ctx)
	}
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
	for key, values := range headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}

	transport := &http.Transport{TLSClientConfig: tlsCfg}
	authenticate := authenticatePeerRoute(tlsCfg)
	p2pOnly := agreement.Endpoint == joinP2POnlyEndpoint
	var selectedMu sync.Mutex
	selected := PeerRouteDialResult{Kind: RouteKindDirect, Target: endpoint.Host}
	routeDial := m.peerRouteDialFunc()
	var frozenRouteTargets []string
	if routeDial == nil {
		if legacyDial := m.peerDialFunc(); legacyDial != nil {
			routeDial = func(dialCtx context.Context, chain string, _ []string, _ PeerRouteAuthenticator) (PeerRouteDialResult, bool, error) {
				start := time.Now()
				conn, handled, dialErr := legacyDial(dialCtx, chain)
				return PeerRouteDialResult{Conn: conn, Kind: RouteKindP2PDirect, Latency: time.Since(start)}, handled, dialErr
			}
		}
	}
	generationBlockedP2POnly := false
	generationBlockedConcrete := false
	if generation := requiredRouteGeneration(ctx); generation != "" && routeDial != nil {
		hooks := m.joinP2PHooks()
		snapshot, ok := RouteSnapshot{}, false
		if hooks.LoadSnapshot != nil {
			snapshot, ok = hooks.LoadSnapshot(agreement.RemoteChainID)
		}
		if !ok || snapshot.Generation != generation {
			// A Retry may use an expired snapshot as an authenticated bootstrap
			// hint, but never a route learned by another trust generation.
			//
			// R2. The original rule ended "Direct HTTPS remains available and is
			// still pinned by the active agreement", and that is FALSE for a
			// P2P-only agreement. Its endpoint is the joinP2POnlyEndpoint
			// sentinel, which join_routes.go documents as never to be attempted
			// as a TCP fallback, so nulling routeDial leaves no transport at all:
			// the request falls through to a plain dial of an unroutable address.
			// That also blocks the route exchange which is the ONLY thing that
			// could replace the stale snapshot, so the peer can never recover its
			// generation and stays unreachable until it is paired again.
			//
			// R2b. The same assumption fails for an agreement paired with a
			// CONCRETE endpoint when that host moves: the endpoint is a real
			// address, so nothing looks unroutable, but every outbound request
			// dials a machine that no longer answers while the peer's own
			// outbound traffic keeps arriving — the asymmetry operators report
			// as "they can reach us, we cannot reach them". Nulling routeDial
			// there also blocks the route exchange, which is the only thing that
			// can replace the stale snapshot with the peer's current addresses,
			// so the pair can never recover on its own and the queue drains only
			// during the windows when the old address happens to answer.
			//
			// So the stale snapshot is admitted as a bootstrap hint for exactly
			// one path: the authenticated route exchange that upgrades the
			// generation. That exemption is about the PATH, not about the
			// agreement shape — it applies to p2p-only pairs (R2) and to
			// concrete-endpoint pairs (R2b) alike. Every other request keeps the
			// original rule.
			//
			// This does not import stale trust. A snapshot address is a HINT
			// ABOUT WHERE TO CONNECT, not a credential: the connection is still
			// completed through the pinned mTLS handshake bound to the CURRENT
			// agreement, so a stale or reassigned address fails authentication
			// rather than being trusted. The rule's purpose — no cross-generation
			// route inside a protected request — is preserved exactly.
			if path == p2pRoutesExchangePath {
				if ok {
					// A snapshot exists but under another generation: use its
					// addresses as the bootstrap hint.
					frozenRouteTargets = append([]string{}, snapshot.Addrs...)
				}
				// No snapshot at all leaves frozenRouteTargets nil, which the
				// dialer reads as "normal caller, may load current config" — the
				// only route material available to reach the exchange. Both cases
				// are still completed through the pinned mTLS handshake bound to
				// the CURRENT agreement, and whatever the exchange returns is
				// persisted only after p2p_routes.go's exact agreement+binding
				// revalidation, so nothing enters durable state unverified.
			} else {
				routeDial = nil
				generationBlockedP2POnly = p2pOnly
				// A concrete-endpoint agreement keeps its one attempt (the stored
				// address), but the caller must be able to tell a moved host from
				// an offline one: the fallback was withheld here, not absent.
				generationBlockedConcrete = !p2pOnly
			}
		} else {
			// Preserve a non-nil empty slice: nil means a normal caller whose dialer
			// may load current config, while Retry must stay pinned to exact G even
			// when G has no usable targets.
			frozenRouteTargets = append([]string{}, snapshot.Addrs...)
		}
	}
	if generationBlockedP2POnly {
		// Fail with the actual reason instead of dialing the sentinel. Without
		// this the caller sees a connection error against 127.0.0.1:65535, which
		// reads as "peer offline" and sends an operator looking at the network
		// rather than at the trust generation that actually needs repairing.
		return nil, 0, routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
			fmt.Errorf("%w: peer %s is p2p-only and has no authenticated route for the required trust generation",
				ErrTrustGenerationChanged, agreement.RemoteChainID))
	}
	if routeDial != nil || !p2pOnly {
		directDialer := &net.Dialer{}
		transport.DialTLSContext = func(dialCtx context.Context, network, address string) (net.Conn, error) {
			attempts := make([]routeDialAttempt, 0, 2)
			if !p2pOnly {
				attempts = append(attempts, routeDialAttempt{
					dial: func(attemptCtx context.Context) (PeerRouteDialResult, error) {
						complete := BeginPeerRouteAttempt(attemptCtx, RouteKindDirect, address)
						start := time.Now()
						conn, dialErr := directDialer.DialContext(attemptCtx, network, address)
						result, dialErr := authenticate(attemptCtx, PeerRouteDialResult{
							Conn: conn, Kind: RouteKindDirect, Target: address,
							Latency: time.Since(start),
						}, dialErr)
						complete(dialErr)
						return result, dialErr
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
						noteP2PSelectorStarted(attemptCtx)
						result, handled, dialErr := routeDial(attemptCtx, agreement.RemoteChainID, frozenRouteTargets, authenticate)
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
				if generationBlockedConcrete {
					// The P2P fallback was withheld for this request because the
					// route snapshot is from another trust generation, so the
					// single attempt was the stored endpoint. Say that, instead of
					// letting a moved host read as "peer offline": the peer's own
					// traffic may still be arriving, and the repair is a route
					// exchange (or a re-pair), not a network investigation.
					return nil, routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
						fmt.Errorf("%w: peer %s: the stored endpoint is the only transport this agreement "+
							"allowed for this request (no authenticated route for the required trust "+
							"generation): %v", ErrPeerOffline, agreement.RemoteChainID, dialErr))
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
		verdict := peerTransportVerdict(err)
		failure := newPeerRequestFailure(ctx, agreement.Endpoint, method, path, verdict, 0, err)
		m.recordRouteFailure(agreement.RemoteChainID, failure, securityFailure)
		if !securityFailure && path != p2pRoutesExchangePath {
			// UI/status polling must not create a refresh storm while a peer is
			// offline. One bounded refresh per minute is enough; the lifecycle
			// ticker and address-change publisher remain correctness backstops.
			m.maybeTriggerRouteRefresh(agreement.RemoteChainID)
		}
		if isPeerOfflineDialError(err) {
			// Keep a route-recovery verdict discoverable through this wrapper: the
			// dial layer already established WHY only one transport was offered
			// (a stale generation withheld the fallback), and flattening the inner
			// error with %v here would reduce that to prose the caller cannot
			// branch on. ErrPeerOffline stays in the chain either way, so retry
			// classification is unchanged.
			if code := RouteRecoveryFailureCode(err); code != "" {
				cause := routeRecoveryError(code,
					fmt.Errorf("%w: peer %s: %v", ErrPeerOffline, agreement.RemoteChainID, err))
				return nil, 0, newPeerRequestFailure(ctx, agreement.Endpoint, method, path, verdict, 0, cause)
			}
			cause := fmt.Errorf("%w: peer %s: %v", ErrPeerOffline, agreement.RemoteChainID, err)
			return nil, 0, newPeerRequestFailure(ctx, agreement.Endpoint, method, path, verdict, 0, cause)
		}
		cause := fmt.Errorf("peer %s unreachable: %w", agreement.RemoteChainID, err)
		return nil, 0, newPeerRequestFailure(ctx, agreement.Endpoint, method, path, verdict, 0, cause)
	}
	selectedMu.Lock()
	chosen := selected
	selectedMu.Unlock()
	m.recordRouteSuccess(agreement.RemoteChainID, chosen)
	if chosen.Kind == RouteKindDirect && routeDial != nil && path != p2pRoutesExchangePath {
		m.maybeTriggerRouteRefresh(agreement.RemoteChainID)
	}
	defer resp.Body.Close()
	responseLimit := peerResponseLimit(path, headers)
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, responseLimit+1))
	if err != nil {
		return nil, resp.StatusCode, fmt.Errorf("read peer response: %w", err)
	}
	if int64(len(respBody)) > responseLimit {
		return nil, resp.StatusCode, fmt.Errorf("%w: %d-byte limit", errPeerResponseLimit, responseLimit)
	}
	return respBody, resp.StatusCode, nil
}

// peerResponseLimit keeps named recipient discovery bounded even if a remote
// peer is buggy or malicious and ignores its compact-status preference or its
// own targeted-response contract.
func peerResponseLimit(path string, headers http.Header) int64 {
	if path == "/fed/v1/pipe/contacts/lookup" {
		return int64(maxPipeContactLookupBytes)
	}
	if path == "/fed/v1/pipe/linked/consent-candidates" {
		return int64(maxLinkedMessageCandidateResponseBytes)
	}
	if path == "/fed/v1/pipe/linked/directory" {
		return int64(maxLinkedMessageDirectoryInventoryBytes)
	}
	if path == "/fed/v1/pipe/linked/resolve" ||
		path == "/fed/v1/pipe/linked/revalidate" ||
		path == "/fed/v1/pipe/linked/consent-offer" {
		return int64(maxLinkedMessageResolveBytes)
	}
	if path == "/fed/v1/status" && clientRequestsCapability(headers, CapabilityFederatedPipelineContactLookup) {
		// A compact status has no contact roster. The peer policy/capability
		// envelope is far smaller than this, while the limit bounds all eight
		// named-discovery workers even if a remote peer ignores the preference.
		return int64(maxPipeContactStatusBytes)
	}
	return int64(maxFedResponseBytes)
}

func clientRequestsCapability(headers http.Header, capability string) bool {
	for _, value := range headers.Values(HeaderClientCapabilities) {
		for _, advertised := range strings.Split(value, ",") {
			if strings.TrimSpace(advertised) == capability {
				return true
			}
		}
	}
	return false
}

func isSecurityTransportError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrRouteExchangeDenied) || RouteRecoveryFailureCode(err) == RouteRecoverySecurityBlocked {
		return true
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
		strings.Contains(text, "certificate") || strings.Contains(text, "spki") ||
		strings.Contains(text, "pin mismatch") || strings.Contains(text, "identity mismatch") ||
		strings.Contains(text, "authentication failed")
}

// QueryPeer runs one scoped recall against a remote chain.
func (m *Manager) QueryPeer(ctx context.Context, remoteChainID string, qr *QueryRequest) (*QueryResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	if qr == nil {
		return nil, errors.New("v23 federation query request is required")
	}
	expectedDigest := qr.PlanAgreementBindings[remoteChainID]
	expectedChallenge := qr.PlanChallenges[remoteChainID]
	expectedAuthorizationModel, modelBound := qr.PlanAuthorizationModels[remoteChainID]
	expectedAttestation, attestationBound := qr.PlanAuthorizationAttestations[remoteChainID]
	if !modelBound || !attestationBound {
		return nil, fmt.Errorf(
			"v23 federation query signed plan is missing the source authorization tuple for %s; call POST /v1/federation/recall-plan again and re-sign the recall request with a current client",
			remoteChainID,
		)
	}
	if expectedDigest == "" || expectedChallenge == "" {
		return nil, fmt.Errorf("v23 federation query has no agent-signed plan for %s", remoteChainID)
	}
	peerStatus, err := m.fetchPeerStatus(ctx, agreement)
	if err != nil {
		return nil, fmt.Errorf("v23 federation negotiation with %s failed: %w", remoteChainID, err)
	}
	if validationErr := validatePeerV23Status(peerStatus); validationErr != nil {
		return nil, fmt.Errorf("peer %s does not support required federation protocol v23: %w", remoteChainID, validationErr)
	}
	if peerStatus.QueryAgreementBindingDigest != expectedDigest {
		return nil, fmt.Errorf("peer %s federation binding changed after the agent signed its recall plan", remoteChainID)
	}
	v23req, err := cloneQueryRequest(qr)
	if err != nil {
		return nil, err
	}
	if v23req.AgentProof == nil {
		return nil, fmt.Errorf("v23 federation query requires the original agent proof")
	}
	v23req.ProtocolVersion = FederationProtocolV23
	v23req.SourceChainID = m.localChainID
	v23req.DestinationChainID = remoteChainID
	v23req.AgreementBindingDigest = expectedDigest
	v23req.QueryChallenge = expectedChallenge
	currentAuthorizationModel := ""
	if slices.Contains(peerStatus.Capabilities, CapabilityPeerExportReadV1) {
		currentAuthorizationModel = SourceAuthorizationPeerExportV1
	}
	if currentAuthorizationModel != expectedAuthorizationModel {
		return nil, fmt.Errorf(
			"peer %s federation authorization model changed after the agent signed its recall plan (planned=%q current=%q)",
			remoteChainID, authorizationModelDiagnostic(expectedAuthorizationModel),
			authorizationModelDiagnostic(currentAuthorizationModel),
		)
	}
	v23req.SourceAuthorizationModel = expectedAuthorizationModel
	ss := m.syncStore()
	if ss == nil || m.badger == nil {
		return nil, errors.New("local federation agent policy is unavailable")
	}
	queryCtx, cancelQuery := boundedFederatedQueryContext(ctx)
	defer cancelQuery()
	// Global authorization lock order is sync-policy then Badger. Hold both from
	// the final source-agent attestation through the bounded peer request so a
	// completed enrollment, suspension, clearance, or reader-policy mutation
	// guarantees that no later disclosure begins from the superseded snapshot.
	readerUnlock := ss.LockSyncPolicyRead()
	ownerUnlock := m.badger.LockDomainOwnershipRead()
	locksHeld := true
	releaseAuthorization := func() {
		if !locksHeld {
			return
		}
		ownerUnlock()
		readerUnlock()
		locksHeld = false
	}
	defer releaseAuthorization()
	if expectedAuthorizationModel == SourceAuthorizationPeerExportV1 {
		ceiling, eligible, attestErr := m.attestLocalFederatedAgentLocked(v23req.AgentProof.AgentID)
		if attestErr != nil || !eligible {
			return nil, errors.New("federated recall requires an active ordinary source agent")
		}
		if !expectedAttestation.Eligible || expectedAttestation.MaxClassification != ceiling {
			return nil, fmt.Errorf("peer %s source authorization attestation changed after the agent signed its recall plan", remoteChainID)
		}
	} else if expectedAttestation.Eligible || expectedAttestation.MaxClassification != 0 {
		return nil, fmt.Errorf("peer %s signed legacy authorization plan has a non-legacy source attestation", remoteChainID)
	}
	v23req.SourceAgentEligible = expectedAttestation.Eligible
	v23req.SourceAgentMaxClassification = expectedAttestation.MaxClassification
	readerAllowed, readerErr := m.federatedReaderAllowsLocked(
		queryCtx, agreement, v23req.AgentProof.AgentID, v23req.DomainTag,
	)
	if readerErr != nil || !readerAllowed {
		return nil, errors.New("local federation access controls deny this peer domain")
	}
	body, status, err := m.doPeerRequest(queryCtx, agreement, http.MethodPost, "/fed/v1/query", v23req)
	releaseAuthorization()
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

// AvailableRecallDomains asks one authenticated peer to apply its exact live
// negotiated peer-export (or legacy linked-reader) gates to a bounded candidate
// set. Unlike PlanRecall this does not issue challenges and is safe for
// directory/discovery projections.
func (m *Manager) AvailableRecallDomains(
	ctx context.Context, remoteChainID, agentID string, domains []string,
) ([]string, error) {
	if len(domains) > MaxQueryAvailabilityDomains {
		return nil, fmt.Errorf("query availability exceeds %d domains", MaxQueryAvailabilityDomains)
	}
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	peerStatus, err := m.fetchPeerStatus(ctx, agreement)
	if err != nil {
		return nil, fmt.Errorf("federation discovery negotiation failed: %w", err)
	}
	request := &QueryAvailabilityRequest{AgentID: agentID, DomainTags: domains}
	if slices.Contains(peerStatus.Capabilities, CapabilityPeerExportReadV1) {
		ceiling, eligible, attestErr := m.attestLocalFederatedAgent(agentID)
		if attestErr != nil || !eligible {
			return nil, errors.New("federation discovery requires an active ordinary source agent")
		}
		request.SourceAuthorizationModel = SourceAuthorizationPeerExportV1
		request.SourceAgentEligible = true
		request.SourceAgentMaxClassification = ceiling
	}
	ss := m.syncStore()
	if ss == nil {
		return nil, errors.New("federated reader restrictions require SQLite")
	}
	readerUnlock := ss.LockSyncPolicyRead()
	allowedDomains := make([]string, 0, len(domains))
	for _, domain := range domains {
		allowed, allowErr := m.federatedReaderAllowsLocked(ctx, agreement, agentID, domain)
		if allowErr != nil {
			readerUnlock()
			return nil, allowErr
		}
		if allowed {
			allowedDomains = append(allowedDomains, domain)
		}
	}
	if len(allowedDomains) == 0 {
		readerUnlock()
		return []string{}, nil
	}
	request.DomainTags = allowedDomains
	body, status, err := m.doPeerRequest(ctx, agreement, http.MethodPost,
		"/fed/v1/query/available", request)
	readerUnlock()
	if err != nil {
		return nil, err
	}
	if status != http.StatusOK {
		return nil, fmt.Errorf("peer returned %d: %s", status, truncate(body, 200))
	}
	var response QueryAvailabilityResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, fmt.Errorf("decode query availability: %w", err)
	}
	if response.ProtocolVersion != FederationProtocolV23 ||
		response.SourceChainID != m.localChainID ||
		response.DestinationChainID != remoteChainID || response.AgentID != agentID {
		return nil, errors.New("peer returned mismatched query availability")
	}
	allowedCandidates := make(map[string]struct{}, len(domains))
	for _, domain := range domains {
		allowedCandidates[domain] = struct{}{}
	}
	seen := make(map[string]struct{}, len(response.ReadableDomains))
	for _, domain := range response.ReadableDomains {
		if _, ok := allowedCandidates[domain]; !ok {
			return nil, errors.New("peer returned query availability outside the candidate set")
		}
		if _, duplicate := seen[domain]; duplicate {
			return nil, errors.New("peer returned duplicate query availability domain")
		}
		seen[domain] = struct{}{}
	}
	sort.Strings(response.ReadableDomains)
	return response.ReadableDomains, nil
}

// PlanRecall expands wildcard targets to an exact finite set and obtains one
// authenticated, durable destination challenge per peer. The returned maps are
// designed to be copied verbatim into the original agent-signed recall body.
func (m *Manager) PlanRecall(ctx context.Context, targets []string, agentID, domain string) (*RecallPlan, error) {
	if _, err := auth.AgentIDToPublicKey(agentID); err != nil {
		return nil, fmt.Errorf("valid recall agent id is required: %w", err)
	}
	if domain == "" || domain != strings.TrimSpace(domain) {
		return nil, errors.New("an exact recall domain is required")
	}
	var chains []string
	if len(targets) == 0 || (len(targets) == 1 && targets[0] == "*") {
		for _, agreement := range m.ActiveAgreements() {
			chains = append(chains, agreement.RemoteChainID)
		}
	} else {
		seen := make(map[string]struct{}, len(targets))
		for _, target := range targets {
			target = strings.TrimSpace(target)
			if target == "" || target == "*" {
				return nil, errors.New("wildcard federation target must be used alone")
			}
			if _, duplicate := seen[target]; duplicate {
				continue
			}
			seen[target] = struct{}{}
			chains = append(chains, target)
			if len(chains) >= maxFanOutTargets {
				break
			}
		}
	}
	sort.Strings(chains)
	plan := &RecallPlan{
		ProtocolVersion:           FederationProtocolV23,
		SourceChainID:             m.localChainID,
		AgreementBindings:         make(map[string]string),
		QueryChallenges:           make(map[string]string),
		AuthorizationModels:       make(map[string]string),
		AuthorizationAttestations: make(map[string]SourceAuthorizationAttestation),
		ExpiresAt:                 make(map[string]int64),
		Errors:                    make(map[string]string),
	}
	for _, chain := range chains {
		agreement, err := m.ActiveAgreement(chain)
		if err != nil {
			plan.Errors[chain] = err.Error()
			continue
		}
		status, err := m.fetchPeerStatus(ctx, agreement)
		if err != nil {
			plan.Errors[chain] = err.Error()
			continue
		}
		if validationErr := validatePeerV23Status(status); validationErr != nil {
			plan.Errors[chain] = validationErr.Error()
			continue
		}
		request := &QueryPlanRequest{AgentID: agentID, DomainTag: domain}
		authorizationModel := ""
		authorizationAttestation := SourceAuthorizationAttestation{}
		if slices.Contains(status.Capabilities, CapabilityPeerExportReadV1) {
			ceiling, eligible, attestErr := m.attestLocalFederatedAgent(agentID)
			if attestErr != nil || !eligible {
				plan.Errors[chain] = "federated recall requires an active ordinary source agent"
				continue
			}
			request.SourceAuthorizationModel = SourceAuthorizationPeerExportV1
			authorizationModel = SourceAuthorizationPeerExportV1
			request.SourceAgentEligible = true
			request.SourceAgentMaxClassification = ceiling
			authorizationAttestation = SourceAuthorizationAttestation{
				Eligible: true, MaxClassification: ceiling,
			}
		}
		ss := m.syncStore()
		if ss == nil {
			plan.Errors[chain] = "federated reader restrictions require SQLite"
			continue
		}
		readerUnlock := ss.LockSyncPolicyRead()
		readerAllowed, readerErr := m.federatedReaderAllowsLocked(ctx, agreement, agentID, domain)
		if readerErr != nil || !readerAllowed {
			readerUnlock()
			plan.Errors[chain] = "local federation access controls deny this peer domain"
			continue
		}
		body, httpStatus, err := m.doPeerRequest(ctx, agreement, http.MethodPost,
			"/fed/v1/query/plan", request)
		readerUnlock()
		if err != nil {
			plan.Errors[chain] = err.Error()
			continue
		}
		if httpStatus != http.StatusOK {
			plan.Errors[chain] = fmt.Sprintf("peer returned %d: %s", httpStatus, truncate(body, 200))
			continue
		}
		var destination QueryPlanResponse
		if err := json.Unmarshal(body, &destination); err != nil {
			plan.Errors[chain] = fmt.Sprintf("decode recall plan: %v", err)
			continue
		}
		if destination.ProtocolVersion != FederationProtocolV23 ||
			destination.SourceChainID != m.localChainID ||
			destination.DestinationChainID != chain ||
			destination.AgreementBindingDigest != status.QueryAgreementBindingDigest ||
			destination.SourceAuthorizationModel != authorizationModel ||
			destination.SourceAgentEligible != authorizationAttestation.Eligible ||
			destination.SourceAgentMaxClassification != authorizationAttestation.MaxClassification ||
			destination.QueryChallenge == "" || destination.ExpiresAt <= time.Now().Unix() {
			plan.Errors[chain] = "peer returned a stale or mismatched v23 recall plan"
			continue
		}
		plan.Destinations = append(plan.Destinations, chain)
		plan.AgreementBindings[chain] = destination.AgreementBindingDigest
		plan.QueryChallenges[chain] = destination.QueryChallenge
		plan.AuthorizationModels[chain] = authorizationModel
		plan.AuthorizationAttestations[chain] = authorizationAttestation
		plan.ExpiresAt[chain] = destination.ExpiresAt
	}
	if len(plan.Errors) == 0 {
		plan.Errors = nil
	}
	return plan, nil
}

func authorizationModelDiagnostic(model string) string {
	if model == "" {
		return "legacy-linked-reader"
	}
	return model
}

func validatePeerV23Status(status *StatusResponse) error {
	if status == nil {
		return errors.New("status is unavailable")
	}
	if status.FederationProtocolVersion != FederationProtocolV23 {
		return fmt.Errorf("protocol version is %d", status.FederationProtocolVersion)
	}
	if !slices.Contains(status.Capabilities, CapabilityFederationV23) ||
		!slices.Contains(status.Capabilities, CapabilityQueryAgentProofV2) {
		return errors.New("required v23 capabilities are absent")
	}
	digest, err := hex.DecodeString(status.QueryAgreementBindingDigest)
	if err != nil || len(digest) != sha256.Size || status.QueryAgreementBindingDigest != strings.ToLower(status.QueryAgreementBindingDigest) {
		return errors.New("query agreement binding is absent or invalid")
	}
	return nil
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

// RetryPeerStatus is the operator recovery contract. Concurrent clicks share
// one exact-generation refresh and one authenticated status re-probe. Ordinary
// PeerStatus polling deliberately does not enter this workflow.
func (m *Manager) RetryPeerStatus(ctx context.Context, remoteChainID string) (*StatusResponse, error) {
	if !m.transportIsEnabled() {
		return nil, routeRecoveryError(RouteRecoveryDisabled,
			errors.New("federation transport is disabled; turn federation on before retrying"))
	}
	agreement, binding, err := m.routeRefreshAgreementBinding(ctx, remoteChainID)
	if err != nil {
		return nil, err
	}
	key := routeRefreshKey(remoteChainID, agreement, binding)
	m.routeRetryMu.Lock()
	if m.routeRetryActive == nil {
		m.routeRetryActive = make(map[string]*peerStatusRetryCall)
	}
	call := m.routeRetryActive[key]
	if call == nil {
		call = &peerStatusRetryCall{done: make(chan struct{})}
		m.routeRetryActive[key] = call
		// The shared retry must survive cancellation of the first waiter so a
		// concurrent waiter cannot inherit a prematurely canceled generation.
		// Preserve request values while bounding the detached workflow below.
		workflowBase := context.WithoutCancel(ctx)
		go m.runPeerStatusRetry(workflowBase, remoteChainID, agreement, binding, key, call)
	}
	m.routeRetryMu.Unlock()

	select {
	case <-call.done:
		return call.status, call.err
	case <-ctx.Done():
		return nil, fmt.Errorf("federation retry timed out: %w", ctx.Err())
	}
}

func (m *Manager) runPeerStatusRetry(parent context.Context, remoteChainID string, expectedAgreement *store.CrossFedRecord, binding p2pRouteBinding, key string, call *peerStatusRetryCall) {
	defer func() {
		close(call.done)
		time.AfterFunc(routeRetryDedupWindow, func() {
			m.routeRetryMu.Lock()
			if m.routeRetryActive[key] == call {
				delete(m.routeRetryActive, key)
			}
			m.routeRetryMu.Unlock()
		})
	}()
	workflowCtx, cancel := context.WithTimeout(parent, m.routeRefreshBudget(remoteChainID)+6*time.Second)
	defer cancel()
	generation := routeBindingID(binding)
	hint := m.routeRecoveryHint(remoteChainID, generation)
	refreshErr := waitRouteRefresh(workflowCtx, m.beginRouteRefreshExact(workflowCtx, remoteChainID, expectedAgreement, binding))
	if errors.Is(refreshErr, ErrTrustGenerationChanged) || errors.Is(refreshErr, ErrLegacyRouteBinding) || isSecurityTransportError(refreshErr) {
		call.err = classifyRouteRecoveryError(refreshErr, hint)
		return
	}
	ss := m.syncStore()
	if ss == nil {
		call.err = fmt.Errorf("%w: SQLite route binding is unavailable", ErrTrustGenerationChanged)
		return
	}
	unlock := ss.LockSyncPolicyRead()
	agreement, current, err := m.currentP2PRouteBinding(workflowCtx, remoteChainID)
	unlock()
	if err != nil || current != binding || !sameRouteAgreement(agreement, expectedAgreement) {
		call.err = routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
			fmt.Errorf("%w: active agreement or binding changed before authenticated re-probe", ErrTrustGenerationChanged))
		return
	}
	status, probeErr := m.fetchPeerStatus(withRouteGeneration(workflowCtx, generation), agreement)
	if probeErr != nil {
		var combined error
		if refreshErr != nil {
			combined = fmt.Errorf("route refresh failed (%v); authenticated re-probe failed: %w", refreshErr, probeErr)
		} else {
			combined = fmt.Errorf("authenticated re-probe failed: %w", probeErr)
		}
		call.err = classifyRouteRecoveryError(combined, m.routeRecoveryHint(remoteChainID, generation))
		return
	}
	unlock = ss.LockSyncPolicyRead()
	postAgreement, postBinding, postErr := m.currentP2PRouteBinding(workflowCtx, remoteChainID)
	unlock()
	if postErr != nil || postBinding != binding || !sameRouteAgreement(postAgreement, expectedAgreement) {
		call.err = routeRecoveryError(RouteRecoveryTrustGenerationMismatch,
			fmt.Errorf("%w: active agreement or binding changed during authenticated re-probe", ErrTrustGenerationChanged))
		return
	}
	m.rememberPeerName(remoteChainID, status.NetworkName)
	call.status = status
}

// PeerStatusForPipeLookup is the compact counterpart for named recipient
// discovery. It deliberately skips legacy contact-cache refresh: targeted
// contacts are live-only and no compact status can prove an offline route is
// still current.
func (m *Manager) PeerStatusForPipeLookup(ctx context.Context, remoteChainID string) (*StatusResponse, error) {
	agreement, err := m.ActiveAgreement(remoteChainID)
	if err != nil {
		return nil, err
	}
	out, err := m.fetchPeerStatusForPipeLookup(ctx, agreement)
	if err != nil {
		return nil, err
	}
	m.rememberPeerName(remoteChainID, out.NetworkName)
	return out, nil
}

// fetchPeerStatus performs only the authenticated network exchange. Cache
// callers supply their request-time agreement/control binding separately so a
// delayed response can never be relabeled with post-response policy state.
func (m *Manager) fetchPeerStatus(ctx context.Context, agreement *store.CrossFedRecord) (*StatusResponse, error) {
	return m.fetchPeerStatusWithHeaders(ctx, agreement, http.Header{
		HeaderClientCapabilities: {CapabilityNodeMessaging},
	})
}

// fetchPeerStatusForPipeLookup asks a v11.13.1 peer to advertise capability
// and policy without constructing its legacy contact snapshot. Old peers
// ignore the advisory header and return the v1-compatible snapshot instead.
func (m *Manager) fetchPeerStatusForPipeLookup(ctx context.Context, agreement *store.CrossFedRecord) (*StatusResponse, error) {
	compact, err := m.fetchPeerStatusWithHeaders(ctx, agreement, http.Header{
		HeaderClientCapabilities: {CapabilityFederatedPipelineContactLookup + "," + CapabilityNodeMessaging},
	})
	if !errors.Is(err, errPeerResponseLimit) {
		return compact, err
	}
	// v11.13.0 did not understand the compact-status preference. Its legacy
	// status body remains compatible up to the historic 16 MiB transport cap,
	// but must be fetched serially and filtered immediately by the caller.
	if sem := m.legacyPipeStatusFallbackSem; sem != nil {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return m.fetchPeerStatus(ctx, agreement)
}

func (m *Manager) fetchPeerStatusWithHeaders(ctx context.Context, agreement *store.CrossFedRecord, headers http.Header) (*StatusResponse, error) {
	if agreement == nil {
		return nil, fmt.Errorf("peer status agreement is unavailable")
	}
	ctx, _ = WithPeerRouteAttemptTrace(ctx)
	body, status, err := m.doPeerRequestWithHeaders(ctx, agreement, http.MethodGet, "/fed/v1/status", nil, headers)
	if err != nil {
		if PeerRequestFailureDiagnostic(err) == nil {
			err = newPeerRequestFailure(ctx, agreement.Endpoint, http.MethodGet, "/fed/v1/status", peerTransportVerdict(err), status, err)
		}
		m.recordPeerRequestFailure(agreement.RemoteChainID, err)
		return nil, err
	}
	if status != http.StatusOK {
		// Never retain the peer's response body in an operator failure string.
		err = newPeerRequestFailure(ctx, agreement.Endpoint, http.MethodGet, "/fed/v1/status", "http_failure", status,
			fmt.Errorf("peer %s returned %d", agreement.RemoteChainID, status))
		m.recordPeerRequestFailure(agreement.RemoteChainID, err)
		return nil, err
	}
	var out StatusResponse
	if err := json.Unmarshal(body, &out); err != nil {
		failure := newPeerRequestFailure(ctx, agreement.Endpoint, http.MethodGet, "/fed/v1/status", "invalid_response", status,
			fmt.Errorf("decode peer response: %w", err))
		m.recordPeerRequestFailure(agreement.RemoteChainID, failure)
		return nil, failure
	}
	if out.ChainID != agreement.RemoteChainID {
		failure := newPeerRequestFailure(ctx, agreement.Endpoint, http.MethodGet, "/fed/v1/status", "peer_identity_mismatch", status,
			fmt.Errorf("peer identifies as %q, agreement expects %q", out.ChainID, agreement.RemoteChainID))
		m.recordPeerRequestFailure(agreement.RemoteChainID, failure)
		return nil, failure
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
