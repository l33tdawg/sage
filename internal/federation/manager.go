package federation

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/x509"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// maxConcurrentReceiptBroadcasts caps how many receipt pushes may be doing a
// blocking broadcast_tx_commit at once (each can occupy a goroutine up to the
// commit timeout).
const maxConcurrentReceiptBroadcasts = 4

// Config wires a Manager into the node.
type Config struct {
	// LocalChainID is this network's globally-unique chain id (v11 Phase 0),
	// read off-consensus from genesis/config — the receiver side of every
	// chain-qualified signature and the self-federation guard.
	LocalChainID string
	// NetworkName is this node's operator-chosen FRIENDLY label, shown to peers
	// during the join ceremony in place of the raw chain id. Cosmetic and
	// UNAUTHENTICATED — never used for any trust decision. Empty = peers see the
	// chain id. Runtime-updatable via Manager.SetNetworkName.
	NetworkName string
	// CertsDir is the node's TLS material directory (~/.sage/certs): node
	// cert/key for both listener and client, remote CAs under federation/.
	CertsDir string
	// CometRPC is the local CometBFT RPC base URL for broadcasting attest txs.
	CometRPC string
	// AgentKey is the node operator's ed25519 key (~/.sage/agent.key). It signs
	// outbound federation requests and CommitReceipts — for receipts it must be
	// a DECLARED coauthor of the SharedID (the peer's attest handler enforces
	// that bind on-chain).
	AgentKey ed25519.PrivateKey
	// Badger is the on-chain state store — cross_fed agreements, cocommit
	// cores/coauthors/anchors, memory classifications. READ-ONLY here.
	Badger *store.BadgerStore
	// MemStore serves the actual memory content for peer queries.
	MemStore store.MemoryStore
	Logger   zerolog.Logger
}

// Manager is the off-consensus federation transport: trust resolution,
// the mTLS listener's handlers, the outbound client, and receipt exchange.
type Manager struct {
	localChainID string
	certsDir     string
	cometRPC     string
	agentKey     ed25519.PrivateKey
	agentPub     ed25519.PublicKey
	badger       *store.BadgerStore
	memStore     store.MemoryStore
	logger       zerolog.Logger

	// peerDialFn is the optional v11.6 connectivity seam. It may handle a
	// remote chain via a libp2p stream; handled=false preserves the original
	// direct TCP/HTTPS dial path byte-for-byte. Set once during node boot.
	peerDialMu sync.RWMutex
	peerDialFn PeerDialFunc

	// replayMu guards seenSigs — the federation listener's replay cache,
	// SHARDED BY PEER CHAIN so one peer's flood can never evict or lock out
	// another peer (a shared global cap would let any single authenticated peer
	// DoS the whole listener by filling it with distinct valid sigs). Each
	// chain gets its own bounded sub-map. The outer map is bounded by the number
	// of chains that ever held an active agreement AND sent a request (peerAuth
	// gates on ActiveAgreement first, so an attacker can't add shards) — empty
	// shards aren't actively reaped, but that ceiling is operator-scale.
	replayMu sync.Mutex
	seenSigs map[string]map[string]int64

	// caMu guards caCache — parsed pinned CAs keyed by "chainID:hexpin", so the
	// mTLS handshake and per-request verify don't re-read+parse the CA from disk
	// on every connection (unauthenticated-handshake amplification).
	caMu    sync.RWMutex
	caCache map[string]*x509.Certificate

	// broadcastSem bounds concurrent receipt-triggered broadcast_tx_commit
	// calls: each blocks up to the commit timeout, so an unbounded push flood
	// would otherwise pin a goroutine per push. A small pool caps the hold.
	broadcastSem chan struct{}

	// syncPushFn is the outbox drainer's delivery seam: nil in production
	// (the drainer calls m.SyncPush, the real mTLS client), non-nil only in
	// tests so drain logic is testable without a TLS peer.
	syncPushFn func(ctx context.Context, remoteChainID string, req *SyncPushRequest) (*SyncPushResponse, error)

	// syncNudge wakes the outbox drainer ahead of its ticker when the commit
	// watcher enqueues new work. Buffered-1 (a pending nudge covers all
	// batches until consumed). Created by StartSyncDrainer BEFORE its
	// goroutine launches; nil when the drainer is disabled — nudgeSync is
	// nil-safe. Wiring order matters: StartSyncDrainer runs before
	// SetSyncNotifier in runServe, so the watcher never sees a half-built channel.
	syncNudge chan struct{}

	// syncDigestFn is the anti-entropy seam (same pattern as syncPushFn):
	// nil in production (m.SyncDigest), non-nil only in tests.
	syncDigestFn func(ctx context.Context, remoteChainID string, req *SyncDigestRequest) (*SyncDigestResponse, error)

	// syncStatusMu guards syncReconcile — per-peer anti-entropy bookkeeping
	// (last run, the peer's advertised consent, unsupported flag) surfaced by
	// the sync status endpoint. Node-local observability only.
	syncStatusMu  sync.Mutex
	syncReconcile map[string]SyncReconcileStatus

	// seedMu guards seedCache — per-agreement TOTP seeds (v11 join ceremony),
	// keyed by remote chain id → the candidate seeds (current + previous epoch
	// during a rotation cutover). Populated once at unlock; zeroized on
	// revoke/expiry. The fail-closed v3 gate reads the persisted seed_established
	// header (readable while locked) separately, never this cache's KDF.
	seedMu    sync.RWMutex
	seedCache map[string][][]byte

	// vaultPassphrase, when non-empty, wraps the seed at rest with Argon2id+
	// AES-GCM (a strictly stronger protection domain than agent.key). Empty =
	// the 0600-plaintext floor (the honest fallback: v3's gain shrinks to
	// cert-holder→seed-holder scoping — see the join-ceremony honesty ledger).
	vaultPassphrase string

	// ownPinCache is this node's own CA SPKI fingerprint (the pin peers hold for
	// us), loaded once for the v3 factor's pin-pair (RT-10 authoritative self-pin).
	ownPinMu    sync.Mutex
	ownPinCache []byte

	// joins is the host-side JoinSession registry (v11 ceremony); guestDrafts is
	// the guest-side counterpart holding the transient per-ceremony secret (the
	// scanned seed + host material + exchanged nonces) between the QR scan and
	// the guest's tx-33, keyed by the host's session id. Both are node-local and
	// off-consensus. Initialized in NewManager.
	joins       *JoinStore
	guestMu     sync.Mutex
	guestDrafts map[string]*guestDraft

	// broadcastFn submits an encoded tx to the local chain and returns
	// (txHash, height). Defaults to broadcastTxCommit; overridable in tests so
	// the join ceremony can be driven without a live CometBFT node.
	broadcastFn func(txBytes []byte) (string, int64, error)

	// nameMu guards networkName — the friendly, operator-editable label peers
	// see during the join ceremony. Runtime-mutable (dashboard rename) without a
	// node restart, so it is read under the lock every time a ceremony message
	// is built. Cosmetic + unauthenticated; never a trust input.
	nameMu      sync.RWMutex
	networkName string
}

// PeerDialFunc returns a stream-backed connection for remoteChainID. handled
// is false when no p2p route is configured and the caller should use direct
// TCP. Federation TLS still authenticates the returned connection end-to-end.
type PeerDialFunc func(ctx context.Context, remoteChainID string) (conn net.Conn, handled bool, err error)

// SetPeerDialFunc installs the optional libp2p dial seam.
func (m *Manager) SetPeerDialFunc(fn PeerDialFunc) {
	m.peerDialMu.Lock()
	m.peerDialFn = fn
	m.peerDialMu.Unlock()
}

func (m *Manager) peerDialFunc() PeerDialFunc {
	m.peerDialMu.RLock()
	defer m.peerDialMu.RUnlock()
	return m.peerDialFn
}

// NetworkName returns this node's current friendly label (may be empty).
func (m *Manager) NetworkName() string {
	m.nameMu.RLock()
	defer m.nameMu.RUnlock()
	return m.networkName
}

// SetNetworkName updates the friendly label at runtime (dashboard rename). The
// caller is responsible for sanitizing + persisting; this only refreshes the
// value the live ceremony hands to peers.
func (m *Manager) SetNetworkName(name string) {
	m.nameMu.Lock()
	m.networkName = name
	m.nameMu.Unlock()
}

// broadcast dispatches an encoded tx through the (possibly test-injected) path.
func (m *Manager) broadcast(txBytes []byte) (string, int64, error) {
	if m.broadcastFn != nil {
		return m.broadcastFn(txBytes)
	}
	return m.broadcastTxCommit(txBytes)
}

// JoinStore returns the host-side join session registry.
func (m *Manager) JoinStore() *JoinStore { return m.joins }

// NewManager builds the federation transport manager. It is safe to construct
// even when federation is unused — every entry point re-checks agreement state.
func NewManager(cfg Config) *Manager {
	pub, _ := cfg.AgentKey.Public().(ed25519.PublicKey)
	return &Manager{
		localChainID: cfg.LocalChainID,
		networkName:  cfg.NetworkName,
		certsDir:     cfg.CertsDir,
		cometRPC:     cfg.CometRPC,
		agentKey:     cfg.AgentKey,
		agentPub:     pub,
		badger:       cfg.Badger,
		memStore:     cfg.MemStore,
		logger:       cfg.Logger.With().Str("component", "federation").Logger(),
		seenSigs:     make(map[string]map[string]int64),
		caCache:      make(map[string]*x509.Certificate),
		broadcastSem: make(chan struct{}, maxConcurrentReceiptBroadcasts),
		seedCache:    make(map[string][][]byte),
		joins:        NewJoinStore(),
		guestDrafts:  make(map[string]*guestDraft),
	}
}

// cachedCA / putCA / invalidateCACache manage the parsed-CA cache. Keyed by
// (chain, pin) so a re-provisioned CA (new pin) is a natural miss; commit of a
// new CA also drops the chain's entries explicitly.
func (m *Manager) cachedCA(key string) *x509.Certificate {
	m.caMu.RLock()
	defer m.caMu.RUnlock()
	return m.caCache[key]
}

func (m *Manager) putCA(key string, cert *x509.Certificate) {
	m.caMu.Lock()
	defer m.caMu.Unlock()
	m.caCache[key] = cert
}

func (m *Manager) invalidateCACache(remoteChainID string) {
	m.caMu.Lock()
	defer m.caMu.Unlock()
	for k := range m.caCache {
		if strings.HasPrefix(k, remoteChainID+":") {
			delete(m.caCache, k)
		}
	}
}

// LocalChainID exposes the local chain id to REST handlers (self-fed guard,
// provenance stamping).
func (m *Manager) LocalChainID() string { return m.localChainID }

// ActiveAgreement resolves ONE agreement and enforces every off-consensus
// gate: valid id, not self, exists, status active, not expired. Every deny is
// fail-closed — callers never fall back to a weaker check.
func (m *Manager) ActiveAgreement(remoteChainID string) (*store.CrossFedRecord, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return nil, err
	}
	if remoteChainID == m.localChainID {
		// The self-federation guard lives HERE (and in the tx-builder), not in
		// consensus — the ABCI app has no deterministic source for its own
		// chain id (see processCrossFedSet).
		return nil, fmt.Errorf("agreement %s: refusing self-federation", remoteChainID)
	}
	endpoint, peerPubKey, maxClearance, expiresAt, allowedDomains, allowedDepts, status, err := m.badger.GetCrossFed(remoteChainID)
	if err != nil {
		return nil, fmt.Errorf("no agreement for %s: %w", remoteChainID, err)
	}
	rec := &store.CrossFedRecord{
		RemoteChainID:  remoteChainID,
		Endpoint:       endpoint,
		PeerPubKey:     peerPubKey,
		MaxClearance:   maxClearance,
		ExpiresAt:      expiresAt,
		AllowedDomains: allowedDomains,
		AllowedDepts:   allowedDepts,
		Status:         status,
	}
	if rec.Status != "active" {
		return nil, fmt.Errorf("agreement %s: status %q", remoteChainID, rec.Status)
	}
	if rec.ExpiresAt != 0 && time.Now().Unix() >= rec.ExpiresAt {
		return nil, fmt.Errorf("agreement %s: expired", remoteChainID)
	}
	return rec, nil
}

// ActiveAgreements enumerates every agreement that passes the ActiveAgreement
// gates. Invalid/self/revoked/expired records are silently skipped — this
// feeds the handshake verifier and the "*" recall fan-out.
func (m *Manager) ActiveAgreements() []store.CrossFedRecord {
	all, err := m.badger.ListCrossFed()
	if err != nil {
		m.logger.Warn().Err(err).Msg("list cross_fed agreements failed")
		return nil
	}
	active := make([]store.CrossFedRecord, 0, len(all))
	for _, rec := range all {
		if ValidateChainID(rec.RemoteChainID) != nil || rec.RemoteChainID == m.localChainID {
			continue
		}
		if rec.Status != "active" {
			continue
		}
		if rec.ExpiresAt != 0 && time.Now().Unix() >= rec.ExpiresAt {
			continue
		}
		active = append(active, rec)
	}
	return active
}

// DomainAllowed reports whether domain falls inside an agreement's
// AllowedDomains scope: "*" wildcard, exact match, or dotted-ancestor coverage
// (an allowed "hr" covers "hr.public" — the same subtree semantics as the
// grant ancestor-walk). Empty domain is NEVER allowed under a non-wildcard
// scope: an unscoped query against a scoped agreement must be rejected, not
// widened.
func DomainAllowed(allowed []string, domain string) bool {
	for _, a := range allowed {
		if a == "*" {
			return true
		}
		if a == "" || domain == "" {
			continue
		}
		if a == domain || strings.HasPrefix(domain, a+".") {
			return true
		}
	}
	return false
}

// --- Receipt production + handling (Mode 2 cross-anchor) --------------------

// BuildSignedReceipt constructs and signs this chain's CommitReceipt for a
// locally-committed co-commit. height/commitTime are advisory data (the anchor
// binds SharedID+CoreHash — plan footgun S) supplied by the caller from the
// broadcast result; zero values are protocol-legal for late/rebuilt receipts.
//
// The signing key must be a DECLARED coauthor of the SharedID for THIS chain —
// otherwise the peer's attest handler would reject the anchor on-chain, so we
// fail fast here instead of shipping a receipt that can never bind.
func (m *Manager) BuildSignedReceipt(sharedID string, height, commitTime int64) (*ReceiptPush, error) {
	core, err := m.badger.GetCoCommitCore(sharedID)
	if err != nil {
		return nil, fmt.Errorf("read co-commit core: %w", err)
	}
	if len(core) == 0 {
		return nil, fmt.Errorf("no local co-commit for SharedID %s", sharedID)
	}
	coauthors, err := m.coauthorsOf(sharedID)
	if err != nil {
		return nil, err
	}
	signerDeclared := false
	for _, c := range coauthors {
		if c.ChainID == m.localChainID && bytes.Equal(c.PubKey, m.agentPub) {
			signerDeclared = true
			break
		}
	}
	if !signerDeclared {
		return nil, fmt.Errorf("node operator key is not a declared coauthor of %s for chain %s — the peer would reject the anchor", sharedID, m.localChainID)
	}
	receipt := &tx.CommitReceipt{
		ChainID:    m.localChainID,
		SharedID:   sharedID,
		LocalMemID: sharedID, // co-commits are keyed by SharedID locally
		Height:     height,
		CommitTime: commitTime,
		CoreHash:   core,
	}
	receiptBytes := tx.EncodeCommitReceipt(receipt)
	return &ReceiptPush{
		Receipt:      receiptBytes,
		ValSig:       ed25519.Sign(m.agentKey, receiptBytes),
		SignerPubKey: append([]byte(nil), m.agentPub...),
	}, nil
}

// ForeignCoauthorChains returns the distinct non-local chain ids declared in a
// SharedID's coauthor set — the receipt fan-out targets.
func (m *Manager) ForeignCoauthorChains(sharedID string) ([]string, error) {
	coauthors, err := m.coauthorsOf(sharedID)
	if err != nil {
		return nil, err
	}
	seen := make(map[string]bool, len(coauthors))
	chains := make([]string, 0, len(coauthors))
	for _, c := range coauthors {
		if c.ChainID == m.localChainID || seen[c.ChainID] {
			continue
		}
		seen[c.ChainID] = true
		chains = append(chains, c.ChainID)
	}
	return chains, nil
}

func (m *Manager) coauthorsOf(sharedID string) ([]tx.CoCommitCoauthor, error) {
	blob, err := m.badger.GetCoCommitCoauthors(sharedID)
	if err != nil {
		return nil, fmt.Errorf("read co-commit coauthors: %w", err)
	}
	if len(blob) == 0 {
		return nil, fmt.Errorf("no coauthor record for SharedID %s", sharedID)
	}
	coauthors, err := tx.DecodeCoauthorsCanonical(blob)
	if err != nil {
		return nil, fmt.Errorf("decode coauthors: %w", err)
	}
	return coauthors, nil
}

// HandleIncomingReceipt validates a peer's pushed CommitReceipt and records it
// as a cross-anchor by broadcasting a TxTypeCoCommitAttest to OUR OWN chain.
// peerChainID is the AUTHENTICATED sender (mTLS + chain-qualified signature) —
// the receipt's self-declared ChainID must match it, so a compromised peer
// cannot deliver receipts "from" a third chain.
//
// Everything here is a fast-fail predicate; the consensus attest handler
// re-verifies every bind deterministically. Idempotent: an identical existing
// anchor short-circuits without a tx.
func (m *Manager) HandleIncomingReceipt(peerChainID string, push *ReceiptPush) (*ReceiptPushResponse, error) {
	if push == nil || len(push.Receipt) == 0 || len(push.ValSig) != ed25519.SignatureSize {
		return nil, fmt.Errorf("malformed receipt push")
	}
	receipt, err := tx.DecodeCommitReceipt(push.Receipt)
	if err != nil {
		return nil, fmt.Errorf("undecodable receipt: %w", err)
	}
	if receipt.ChainID != peerChainID {
		return nil, fmt.Errorf("receipt chain id %q does not match authenticated peer %q", receipt.ChainID, peerChainID)
	}
	localCore, err := m.badger.GetCoCommitCore(receipt.SharedID)
	if err != nil {
		return nil, fmt.Errorf("read co-commit core: %w", err)
	}
	if len(localCore) == 0 {
		return nil, fmt.Errorf("no local co-commit for SharedID %s", receipt.SharedID)
	}
	if !bytes.Equal(localCore, receipt.CoreHash) {
		return nil, fmt.Errorf("receipt CoreHash does not match local core for %s", receipt.SharedID)
	}

	// Resolve the signer: it must be a DECLARED coauthor for the peer's chain
	// whose key verifies ValSig over the verbatim receipt bytes. The optional
	// SignerPubKey hint is only trusted after the same membership + signature
	// checks any candidate goes through.
	signer := m.resolveReceiptSigner(receipt.SharedID, peerChainID, push)
	if signer == nil {
		return nil, fmt.Errorf("receipt signature matches no declared coauthor of %s for chain %s", receipt.SharedID, peerChainID)
	}

	// First-write-wins idempotency, keyed on the SEMANTIC pair
	// (SharedID, peerChainID) — NOT sha256(receipt). Height/CommitTime are
	// non-semantic, attacker-chosen bytes inside the signed receipt, so a
	// declared-coauthor peer could vary them to mint a different receipt hash
	// on every push and force a fresh consensus attest each time. Because the
	// anchor binds CoreHash (content-derived, identical for a given SharedID),
	// the first valid anchor is as authoritative as any later one — so once ANY
	// anchor exists for the pair, further pushes are a no-op. This also matches
	// the plan's "idempotent, late-bindable" receipt design.
	if existing, aErr := m.badger.GetCoCommitAnchor(receipt.SharedID, peerChainID); aErr == nil && len(existing) > 0 {
		return &ReceiptPushResponse{Status: "already_anchored", SharedID: receipt.SharedID}, nil
	}

	// Bound concurrent blocking broadcasts (each occupies a goroutine up to the
	// commit timeout).
	m.broadcastSem <- struct{}{}
	defer func() { <-m.broadcastSem }()
	// Re-check under the semaphore: a racing push for the same pair may have
	// anchored while we waited.
	if existing, aErr := m.badger.GetCoCommitAnchor(receipt.SharedID, peerChainID); aErr == nil && len(existing) > 0 {
		return &ReceiptPushResponse{Status: "already_anchored", SharedID: receipt.SharedID}, nil
	}

	attest := &tx.ParsedTx{
		Type:      tx.TxTypeCoCommitAttest,
		Nonce:     tx.MonotonicNonce(m.agentKey),
		Timestamp: time.Now(),
		CoCommitAttest: &tx.CoCommitAttest{
			SharedID:    receipt.SharedID,
			PeerChainID: receipt.ChainID,
			PeerPubKey:  signer,
			Receipt:     push.Receipt,
			PeerSig:     push.ValSig,
			CommitTime:  receipt.CommitTime, // DATA only, never a branch input
			CoreHash:    receipt.CoreHash,
		},
	}
	if signErr := tx.SignTx(attest, m.agentKey); signErr != nil {
		return nil, fmt.Errorf("sign attest tx: %w", signErr)
	}
	encoded, err := tx.EncodeTx(attest)
	if err != nil {
		return nil, fmt.Errorf("encode attest tx: %w", err)
	}
	hash, height, err := m.broadcastTxCommit(encoded)
	if err != nil {
		return nil, fmt.Errorf("broadcast attest: %w", err)
	}
	m.logger.Info().Str("shared_id", receipt.SharedID).Str("peer", peerChainID).Str("tx", hash).Msg("peer receipt anchored")
	return &ReceiptPushResponse{Status: "anchored", SharedID: receipt.SharedID, TxHash: hash, Height: height}, nil
}

// resolveReceiptSigner returns the declared-coauthor public key (for
// peerChainID) that verifies ValSig over the receipt bytes, or nil.
func (m *Manager) resolveReceiptSigner(sharedID, peerChainID string, push *ReceiptPush) ed25519.PublicKey {
	coauthors, err := m.coauthorsOf(sharedID)
	if err != nil {
		return nil
	}
	// Hint first (cheap), then the full per-chain scan (≤ 64 coauthors).
	candidates := make([][]byte, 0, len(coauthors))
	if len(push.SignerPubKey) == ed25519.PublicKeySize {
		candidates = append(candidates, push.SignerPubKey)
	}
	for _, c := range coauthors {
		if c.ChainID == peerChainID {
			candidates = append(candidates, c.PubKey)
		}
	}
	for _, cand := range candidates {
		if len(cand) != ed25519.PublicKeySize {
			continue
		}
		declared := false
		for _, c := range coauthors {
			if c.ChainID == peerChainID && bytes.Equal(c.PubKey, cand) {
				declared = true
				break
			}
		}
		if declared && ed25519.Verify(ed25519.PublicKey(cand), push.Receipt, push.ValSig) {
			return ed25519.PublicKey(cand)
		}
	}
	return nil
}
