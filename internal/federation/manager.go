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

// syncPolicyDeliveryLease is per peer: one unreachable colleague must never
// delay the admin from pausing another. Pause intent is published before the
// lease is acquired so it can cancel an in-flight same-peer PUT immediately.
type syncPolicyDeliveryLease struct {
	mu               sync.Mutex
	stateMu          sync.Mutex
	pauseRequested   bool
	pauseRevision    uint64
	pauseMutations   int
	pendingPauses    int
	generationBlocks int
	cancel           context.CancelFunc
}

// SyncPolicyGenerationMutation blocks and cancels outbound policy-label
// delivery for one peer while an agreement generation is replaced or retired.
// Call exactly one terminal method. Methods are idempotent so deferred Restore
// can safely guard early returns before Activate/Retire succeeds.
type SyncPolicyGenerationMutation struct {
	lease         *syncPolicyDeliveryLease
	pauseRevision uint64
	once          sync.Once
}

func (g *SyncPolicyGenerationMutation) finish(resetOperatorPause bool) {
	if g == nil || g.lease == nil {
		return
	}
	g.once.Do(func() {
		g.lease.stateMu.Lock()
		if resetOperatorPause && g.lease.pauseRevision == g.pauseRevision && g.lease.pauseMutations == 0 {
			// A fresh JOIN starts with an empty, unpaused deny-all policy. Never
			// inherit the retired connection's steady in-memory Pause intent, but
			// never erase a Pause/Resume that began while this generation held
			// the delivery lease.
			if g.lease.pauseRequested {
				g.lease.pauseRequested = false
				g.lease.pauseRevision++
			}
		}
		if g.lease.generationBlocks > 0 {
			g.lease.generationBlocks--
		}
		g.lease.stateMu.Unlock()
		g.lease.mu.Unlock()
	})
}

// Restore releases a failed generation mutation without touching the
// operator's independent Pause/Resume intent.
func (g *SyncPolicyGenerationMutation) Restore() { g.finish(false) }

// Activate releases a completely installed active generation for delivery.
func (g *SyncPolicyGenerationMutation) Activate() { g.finish(true) }

// Retire releases a purged/revoked generation while keeping delivery blocked.
func (g *SyncPolicyGenerationMutation) Retire() { g.finish(false) }

// beginPauseMutation publishes one operator mutation before waiting on the
// delivery lease. Pause is fail-closed immediately so it can cancel an active
// PUT; Resume keeps the old latch until its durable policy update succeeds.
func (l *syncPolicyDeliveryLease) beginPauseMutation(paused bool) {
	l.stateMu.Lock()
	l.pauseRevision++
	l.pauseMutations++
	if paused {
		l.pendingPauses++
		l.pauseRequested = true
	}
	cancel := l.cancel
	l.stateMu.Unlock()
	if paused && cancel != nil {
		cancel()
	}
}

// completePauseMutation commits the ordered durable state when it is known.
// A later Pause publishes before it can acquire the lease, so an earlier
// Resume must never clear the latch while any Pause is still pending. A nil
// durable state means the ordered read itself failed; retain the fail-closed
// in-memory state. Advancing the revision again prevents a generation token
// captured mid-mutation from treating it as an inherited Pause.
func (l *syncPolicyDeliveryLease) completePauseMutation(requestedPause bool, durablePause *bool) {
	l.stateMu.Lock()
	if requestedPause && l.pendingPauses > 0 {
		l.pendingPauses--
	}
	if durablePause != nil {
		l.pauseRequested = *durablePause
	}
	if l.pendingPauses > 0 {
		l.pauseRequested = true
	}
	l.pauseRevision++
	if l.pauseMutations > 0 {
		l.pauseMutations--
	}
	l.stateMu.Unlock()
}

func (l *syncPolicyDeliveryLease) deliveryBlocked() (paused bool, generation bool) {
	l.stateMu.Lock()
	defer l.stateMu.Unlock()
	return l.pauseRequested, l.generationBlocks > 0
}

func (l *syncPolicyDeliveryLease) registerCancel(cancel context.CancelFunc) bool {
	l.stateMu.Lock()
	l.cancel = cancel
	blocked := l.pauseRequested || l.generationBlocks > 0
	l.stateMu.Unlock()
	if blocked {
		cancel()
	}
	return blocked
}

func (l *syncPolicyDeliveryLease) clearCancel(cancel context.CancelFunc) {
	l.stateMu.Lock()
	// A delivery owns l.mu, so no second delivery can replace this callback.
	// Clear unconditionally; the parameter documents which lifecycle ended.
	_ = cancel
	l.cancel = nil
	l.stateMu.Unlock()
}

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
	// PostV20ForNextTx gates the backward-compatible MemorySubmit tag extension.
	// It must remain absent before activation because older validators reproduce
	// the historical payload without that extension when checking signatures.
	PostV20ForNextTx func() bool
	Logger           zerolog.Logger
}

// Manager is the off-consensus federation transport: trust resolution,
// the mTLS listener's handlers, the outbound client, and receipt exchange.
type Manager struct {
	localChainID     string
	certsDir         string
	cometRPC         string
	agentKey         ed25519.PrivateKey
	agentPub         ed25519.PublicKey
	badger           *store.BadgerStore
	memStore         store.MemoryStore
	postV20ForNextTx func() bool
	logger           zerolog.Logger

	// peerDialFn is the optional v11.6 connectivity seam. It may handle a
	// remote chain via a libp2p stream; handled=false preserves the original
	// direct TCP/HTTPS dial path byte-for-byte. Set once during node boot.
	peerDialMu sync.RWMutex
	peerDialFn PeerDialFunc

	joinP2PMu sync.RWMutex
	joinP2P   JoinP2PHooks

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
	syncPushFn       func(ctx context.Context, remoteChainID string, req *SyncPushRequest) (*SyncPushResponse, error)
	syncPolicyPushFn func(ctx context.Context, remoteChainID string, req *SyncPolicyRequest) (*SyncPolicyResponse, error)
	// pipeEventPushFn is the federated pipeline delivery seam. Production uses
	// the authenticated mTLS client; tests inject it to exercise retry and
	// revalidation without a listener.
	pipeEventPushFn       func(ctx context.Context, remoteChainID string, event *PipeEvent) (*PipeEventResponse, error)
	pipeTargetResolveFn   func(ctx context.Context, target string) (*RemotePipeTarget, error)
	pipeResultPreflightFn func(ctx context.Context, msg *store.PipelineMessage, outbox *store.PipelineTransportOutbox) error
	// syncPolicyDeliveryLeases linearize outbound policy-label disclosure with
	// Pause per remote chain. The map mutex is held only for lookup/creation;
	// each per-peer lease may span a bounded network request without blocking an
	// unrelated peer or either node's inbound SQLite policy writer.
	syncPolicyDeliveryLeasesMu sync.Mutex
	syncPolicyDeliveryLeases   map[string]*syncPolicyDeliveryLease
	// syncPolicyFinalGateHook is a deterministic test barrier immediately before
	// the delivery-vs-pause lease. It is nil in production.
	syncPolicyFinalGateHook func(remoteChainID string)
	// syncPolicyBeforePushHook is a deterministic test barrier after the exact
	// agreement/control payload is built but before any network call.
	syncPolicyBeforePushHook func(remoteChainID string)
	// syncPolicyPauseLeaseHook is a deterministic test barrier after Pause or
	// Resume owns the per-peer delivery lease but before it reads the bound
	// policy snapshot. It is nil in production.
	syncPolicyPauseLeaseHook func(remoteChainID string)

	// syncNudge wakes the outbox drainer ahead of its ticker when the commit
	// watcher enqueues new work. Buffered-1 (a pending nudge covers all
	// batches until consumed). Created by StartSyncDrainer BEFORE its
	// goroutine launches; nil when the drainer is disabled — nudgeSync is
	// nil-safe. Wiring order matters: StartSyncDrainer runs before
	// SetSyncNotifier in runServe, so the watcher never sees a half-built channel.
	syncNudge     chan struct{}
	syncStartOnce sync.Once
	syncStopOnce  sync.Once
	syncCancel    context.CancelFunc
	syncWG        sync.WaitGroup

	// syncDigestFn is the anti-entropy seam (same pattern as syncPushFn):
	// nil in production (m.SyncDigest), non-nil only in tests.
	syncDigestFn func(ctx context.Context, remoteChainID string, req *SyncDigestRequest) (*SyncDigestResponse, error)

	// syncJournalFn is the v11.8 group-journal exchange seam (same pattern):
	// nil in production (m.SyncJournalPull), non-nil only in tests so the
	// pull/verify/ingest logic is testable without a live TLS peer.
	syncJournalFn func(ctx context.Context, remoteChainID string, req *SyncJournalRequest) (*SyncJournalResponse, error)
	// Entitlement-safe domain-subchain discovery for production journal
	// reconciliation. The peer returns only subchains this authenticated member
	// may read; tests inject the seam without a TLS listener.
	syncJournalSubchainsFn func(ctx context.Context, remoteChainID, groupID string) ([]string, error)

	// controllerGovGate is the v11.8 §8 multi-validator authorization seam for a
	// CONTROLLER-AFFECTING emit (create/dissolve group, add a domain to the shared
	// set, remove/role-change ANOTHER member). On a single-validator personal node
	// the controller commits directly (validatorCount()<=1) and this is never
	// consulted; on a multi-validator deployment a controller-affecting change
	// SHOULD additionally carry a passed tx-24/25 GovPropose/Vote (NOT tx-30
	// DomainReassign). It returns whether such a passage authorizes the change for
	// groupID. nil (production default today) => fail-closed on multi-validator:
	// authorizeControllerAffecting refuses rather than silently committing a
	// group-affecting change without the quorum §8 requires. Tests inject it.
	controllerGovGate func(ctx context.Context, groupID, actionDigest string) (bool, error)

	// syncStatusMu guards syncReconcile — per-peer anti-entropy bookkeeping
	// (last run, the peer's advertised consent, unsupported flag) surfaced by
	// the sync status endpoint. Node-local observability only.
	syncStatusMu  sync.Mutex
	syncReconcile map[string]SyncReconcileStatus

	// syncAnchorFn is the D1 removal-anchor seam (same test-seam pattern as
	// syncPushFn): nil in production (anchorSubchain broadcasts a locally-signed
	// MemorySubmit of the affected sub-chain head into sage-syncaudit-<group>),
	// non-nil only in tests so enforceRemovalBatch is testable without consensus.
	// The subchain arg distinguishes the roster head (member removals) from a
	// per-domain sub-chain head (domain removals — which never advance the roster head).
	syncAnchorFn func(ctx context.Context, groupID, subchain, head string) error
	// syncAnchorMu guards syncAnchoredHead — the last head this node anchored on-chain
	// per (group|subchain), so a removal-enforcement batch never re-anchors an unchanged
	// head (docs §5.6 rate-limit: at most once per distinct sub-chain head).
	syncAnchorMu     sync.Mutex
	syncAnchoredHead map[string]string

	// journalMu serializes v11.8 group-journal appends within this node so a
	// read-head -> build+sign -> append sequence is atomic against concurrent
	// appenders (the (group_id,subchain,seq) PK is the backstop; this avoids the
	// retryable-loser churn). The append advances sync_group's head/revision
	// cache best-effort — the sync_group_log rows are the source of truth and the
	// fold re-derives the head, so a crash between append and cache-advance is
	// harmless.
	journalMu sync.Mutex

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
	joins           *JoinStore
	guestMu         sync.Mutex
	guestDrafts     map[string]*guestDraft
	guestGeneration uint64
	guestConfirmMu  sync.Mutex // activation artifacts are keyed by remote chain; serialize confirm/rollback ownership

	// broadcastFn submits an encoded tx to the local chain and returns
	// (txHash, height). Defaults to broadcastTxCommit; overridable in tests so
	// the join ceremony can be driven without a live CometBFT node.
	broadcastFn func(txBytes []byte) (string, int64, error)

	// agreementMutationMu serializes every local tx-33/tx-34 control surface,
	// including legacy REST through LockAgreementMutation. A sharing update is a
	// read-current-terms -> replace-scope operation, so the read, committed
	// broadcast, and matching local activation/purge must be one generation
	// critical section or a concurrent update/revoke could clobber newer terms,
	// reactivate a revoked agreement, or purge newly activated capabilities.
	agreementMutationMu sync.Mutex

	// nameMu guards networkName — the friendly, operator-editable label peers
	// see during the join ceremony. Runtime-mutable (dashboard rename) without a
	// node restart, so it is read under the lock every time a ceremony message
	// is built. Cosmetic + unauthenticated; never a trust input.
	nameMu      sync.RWMutex
	networkName string
}

// LockAgreementMutation acquires the process-wide federation agreement
// mutation lease and returns its release function. Every control surface that
// can commit a tx-33/tx-34 agreement generation must hold this lease from
// before the committed broadcast through the matching node-local activation
// or purge. Callers that also need the SQLite sync-policy gate must acquire
// this lease first; the global lock order is agreement mutation -> policy.
//
// This is exported only so the legacy REST transaction builder can participate
// in the same critical section as Manager-driven JOIN, sharing updates, and
// revocation. Callers must invoke the returned function exactly once.
func (m *Manager) LockAgreementMutation() func() {
	m.agreementMutationMu.Lock()
	return m.agreementMutationMu.Unlock
}

func (m *Manager) syncPolicyDeliveryLease(remoteChainID string) *syncPolicyDeliveryLease {
	m.syncPolicyDeliveryLeasesMu.Lock()
	defer m.syncPolicyDeliveryLeasesMu.Unlock()
	if m.syncPolicyDeliveryLeases == nil {
		m.syncPolicyDeliveryLeases = make(map[string]*syncPolicyDeliveryLease)
	}
	lease := m.syncPolicyDeliveryLeases[remoteChainID]
	if lease == nil {
		lease = &syncPolicyDeliveryLease{}
		m.syncPolicyDeliveryLeases[remoteChainID] = lease
	}
	return lease
}

// BeginSyncPolicyGenerationMutation joins the canonical agreement -> per-peer
// delivery -> SQLite policy lock order. It publishes the block before waiting,
// cancels an in-flight policy PUT, and prevents an E1 payload from being sent
// after E2 or a completed revoke becomes authoritative.
func (m *Manager) BeginSyncPolicyGenerationMutation(remoteChainID string) *SyncPolicyGenerationMutation {
	lease := m.syncPolicyDeliveryLease(remoteChainID)
	lease.stateMu.Lock()
	lease.generationBlocks++
	cancel := lease.cancel
	lease.stateMu.Unlock()
	if cancel != nil {
		cancel()
	}
	lease.mu.Lock()
	// Capture only after this generation owns the delivery lease. Operator
	// mutations that completed while Begin waited belong to the retiring
	// generation and may be reset by Activate; mutations that start after this
	// point advance the revision and must survive into the fresh generation.
	lease.stateMu.Lock()
	pauseRevision := lease.pauseRevision
	lease.stateMu.Unlock()
	return &SyncPolicyGenerationMutation{lease: lease, pauseRevision: pauseRevision}
}

// PeerDialFunc returns a stream-backed connection for remoteChainID. handled
// is false when no p2p route is configured and the caller should use direct
// TCP. Federation TLS still authenticates the returned connection end-to-end.
type PeerDialFunc func(ctx context.Context, remoteChainID string) (conn net.Conn, handled bool, err error)

// JoinP2PBundle is sourced from the live node transport, never from a browser.
// Addrs contain both direct and relay-circuit targets so one enrollment can
// roam LAN -> internet -> LAN without changing federation trust.
type JoinP2PBundle struct {
	PeerID   string   `json:"peer_id"`
	Protocol string   `json:"protocol"`
	Addrs    []string `json:"addrs"`
}

// JoinP2PHooks keep federation independent of libp2p while giving the JOIN
// ceremony narrowly-scoped bootstrap, admission and crash-safe route seams.
type JoinP2PHooks struct {
	LocalBundle func() (JoinP2PBundle, error)
	DialTarget  func(context.Context, string) (net.Conn, error)
	Begin       func(sessionID string, expiresAt time.Time)
	BindPeer    func(sessionID string, targets []string, expiresAt time.Time) error
	End         func(sessionID string)
	Persist     func(remoteChainID string, targets []string) error
	Remove      func(remoteChainID string) error
}

func (m *Manager) SetJoinP2PHooks(h JoinP2PHooks) {
	m.joinP2PMu.Lock()
	m.joinP2P = h
	m.joinP2PMu.Unlock()
}

func (m *Manager) joinP2PHooks() JoinP2PHooks {
	m.joinP2PMu.RLock()
	defer m.joinP2PMu.RUnlock()
	return m.joinP2P
}

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
	m := &Manager{
		localChainID:     cfg.LocalChainID,
		networkName:      cfg.NetworkName,
		certsDir:         cfg.CertsDir,
		cometRPC:         cfg.CometRPC,
		agentKey:         cfg.AgentKey,
		agentPub:         pub,
		badger:           cfg.Badger,
		memStore:         cfg.MemStore,
		postV20ForNextTx: cfg.PostV20ForNextTx,
		logger:           cfg.Logger.With().Str("component", "federation").Logger(),
		seenSigs:         make(map[string]map[string]int64),
		caCache:          make(map[string]*x509.Certificate),
		broadcastSem:     make(chan struct{}, maxConcurrentReceiptBroadcasts),
		seedCache:        make(map[string][][]byte),
		joins:            NewJoinStore(),
		guestDrafts:      make(map[string]*guestDraft),
	}
	// Production governance proof is read from the durable consensus state. Tests
	// may replace the seam, but production never defaults to a permissive stub.
	m.controllerGovGate = m.passedControllerGovernance
	return m
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
	if m.badger == nil {
		return nil, fmt.Errorf("agreement %s: consensus state store is unavailable", remoteChainID)
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
	if m.badger == nil {
		m.logger.Warn().Msg("list cross_fed agreements skipped: consensus state store is unavailable")
		return nil
	}
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
	// Direct callers do not carry peerAuth's request snapshot, but they still
	// must linearize the consensus write with connection revocation. The HTTP
	// handler performs the stronger exact-generation/operator check under its
	// own lease and calls handleIncomingReceiptValidated directly.
	if ss := m.syncStore(); ss != nil {
		policyUnlock := ss.LockSyncPolicyRead()
		defer policyUnlock()
	}
	if _, err := m.ActiveAgreement(peerChainID); err != nil {
		return nil, fmt.Errorf("receipt sender has no active federation agreement: %w", err)
	}
	return m.handleIncomingReceiptValidated(peerChainID, push)
}

// handleIncomingReceiptValidated contains the receipt/consensus validation.
// The caller holds the sync-policy read lease and has resolved the current
// agreement at that lease's linearization point.
func (m *Manager) handleIncomingReceiptValidated(peerChainID string, push *ReceiptPush) (*ReceiptPushResponse, error) {
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
