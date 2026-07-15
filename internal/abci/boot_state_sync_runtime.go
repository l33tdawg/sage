package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/blocksync"
)

// BootStateSyncPhase is the process-local portion of the one-shot boot state
// sync lifecycle. Durable directory phases live in statesync.ActivationJournal.
type BootStateSyncPhase byte

const (
	BootStateSyncDisabled BootStateSyncPhase = iota
	BootStateSyncDiscovering
	BootStateSyncAssembling
	BootStateSyncPrepared
	BootStateSyncPendingComet
	BootStateSyncSealed
	BootStateSyncFailed

	bootStateSyncCheckTxBlockedCode uint32 = 110
)

// ErrBootStateSyncPersistencePending means the active application is ahead of
// the running Comet state store. Startup may retry this observation until its
// configured deadline; the runtime remains pending rather than failing.
var ErrBootStateSyncPersistencePending = errors.New("CometBFT has not persisted the activated application state yet")

// ErrBootStateSyncConsensusServingBlocked means CometBFT attempted an ordinary
// consensus or mempool method before receiver activation reached PendingComet,
// or after the attempt failed. Calls arriving during PendingComet wait for the
// seal without holding the bundle lease, so block sync cannot race activation.
// Info and the four state-sync snapshot methods remain available while armed.
var ErrBootStateSyncConsensusServingBlocked = fmt.Errorf(
	"%w: ordinary ABCI serving is blocked during boot state sync",
	blocksync.ErrStateSyncSealAborted,
)

// ConsensusBundle is an immutable reference to one complete ABCI application
// graph and the state it reported when constructed. Replacing the application,
// store, validator set, governance engine, or caches separately is forbidden.
type ConsensusBundle struct {
	application        abcitypes.Application
	expectedHeight     int64
	expectedHash       []byte
	expectedAppVersion uint64
	cleanup            func() error
	closeOnce          sync.Once
	closeErr           error
}

// NewConsensusBundle captures the application's current Info state. Callers
// must finish constructing the complete SageApp graph before wrapping it.
func NewConsensusBundle(ctx context.Context, application abcitypes.Application) (*ConsensusBundle, error) {
	return NewConsensusBundleWithCleanup(ctx, application, nil)
}

// NewConsensusBundleWithCleanup attaches a consensus-only cleanup callback.
// It must never close a process-owned off-chain projection shared with the
// replacement bundle.
func NewConsensusBundleWithCleanup(ctx context.Context, application abcitypes.Application, cleanup func() error) (*ConsensusBundle, error) {
	if application == nil {
		return nil, cleanupFailedConsensusBundle(cleanup, errors.New("consensus bundle application is required"))
	}
	info, err := application.Info(ctx, &abcitypes.RequestInfo{})
	if err != nil {
		return nil, cleanupFailedConsensusBundle(cleanup, fmt.Errorf("read consensus bundle state: %w", err))
	}
	if info == nil {
		return nil, cleanupFailedConsensusBundle(cleanup, errors.New("consensus bundle Info response is nil"))
	}
	if err := validateBundleState(info.LastBlockHeight, info.LastBlockAppHash, info.AppVersion); err != nil {
		return nil, cleanupFailedConsensusBundle(cleanup, err)
	}
	return &ConsensusBundle{
		application:        application,
		expectedHeight:     info.LastBlockHeight,
		expectedHash:       append([]byte(nil), info.LastBlockAppHash...),
		expectedAppVersion: info.AppVersion,
		cleanup:            cleanup,
	}, nil
}

// ExpectedState returns a private copy of the bundle's construction-time state.
func (b *ConsensusBundle) ExpectedState() (int64, []byte) {
	if b == nil {
		return 0, nil
	}
	return b.expectedHeight, append([]byte(nil), b.expectedHash...)
}

// ExpectedAppVersion returns the app protocol version captured with Info.
func (b *ConsensusBundle) ExpectedAppVersion() uint64 {
	if b == nil {
		return 0
	}
	return b.expectedAppVersion
}

// Close releases the complete application graph at most once when it exposes
// a Close method. Candidate bundles are closed automatically if activation
// rejects them; the active bundle is closed by BootStateSyncRuntime.Close.
func (b *ConsensusBundle) Close() error {
	if b == nil {
		return nil
	}
	b.closeOnce.Do(func() {
		if b.cleanup != nil {
			b.closeErr = b.cleanup()
		}
	})
	return b.closeErr
}

// BootStateSyncRuntime owns the only application reference given to CometBFT.
// Normal ABCI calls hold a read lease for their full duration. The final state
// sync activation holds the write lease, so it cannot close or replace a
// bundle while any delegated call is still using it.
type BootStateSyncRuntime struct {
	mu                   sync.RWMutex
	bundle               *ConsensusBundle
	phase                BootStateSyncPhase
	normalServing        bool
	configureApplication func(abcitypes.Application) error
	consensusServingGate chan struct{}
	consensusGateOnce    sync.Once

	endpointsMu sync.RWMutex
	serving     *StateSyncServingController
	receiving   *StateSyncReceiverController
}

var _ abcitypes.Application = (*BootStateSyncRuntime)(nil)

// NewBootStateSyncRuntime creates a dormant runtime. State-sync endpoints stay
// empty/reject/abort until a later release gate explicitly arms a receiver.
func NewBootStateSyncRuntime(bundle *ConsensusBundle) (*BootStateSyncRuntime, error) {
	if err := validateConsensusBundle(bundle); err != nil {
		return nil, err
	}
	return &BootStateSyncRuntime{
		bundle: bundle, phase: BootStateSyncDisabled,
		consensusServingGate: make(chan struct{}),
	}, nil
}

// Phase reports the current process-local boot state-sync phase.
func (r *BootStateSyncRuntime) Phase() BootStateSyncPhase {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.phase
}

// ExpectedState returns the active bundle's construction-time state.
func (r *BootStateSyncRuntime) ExpectedState() (int64, []byte) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.ExpectedState()
}

// ExpectedAppVersion returns the active bundle's construction-time app version.
func (r *BootStateSyncRuntime) ExpectedAppVersion() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.ExpectedAppVersion()
}

// RequireNormalServingReady rejects ordinary service admission while a boot
// state-sync attempt is incomplete or failed. Disabled is ready because no
// state-sync receiver is armed; an armed receiver must reach Sealed first.
// CometBFT itself starts before this check so it can perform state sync, but
// REST, MCP, federation, voters, and other normal serving must stay behind it.
func (r *BootStateSyncRuntime) RequireNormalServingReady() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	switch r.phase {
	case BootStateSyncDisabled, BootStateSyncSealed:
		return nil
	default:
		return fmt.Errorf("normal serving requires disabled or sealed boot state-sync runtime, got phase %d", r.phase)
	}
}

// AcquireNormalServingApplication freezes the boot-only replacement seam and
// returns the final application graph to ordinary services. Once this method
// succeeds the runtime can no longer be armed or replace its bundle: concrete
// REST/worker references may safely escape because their application and store
// remain the same until Runtime.Close.
func (r *BootStateSyncRuntime) AcquireNormalServingApplication() (abcitypes.Application, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.phase {
	case BootStateSyncDisabled, BootStateSyncSealed:
		r.normalServing = true
		return r.bundle.application, nil
	default:
		return nil, fmt.Errorf("normal serving requires disabled or sealed boot state-sync runtime, got phase %d", r.phase)
	}
}

// ConfigureApplicationBundles installs process-local policy that must be
// applied before either the initial or an activated application may Commit.
// Typical policy includes binary display version and block retention. It must
// be configured while the receiver is disabled; activation applies the same
// callback to a verified candidate before publishing it to CometBFT.
func (r *BootStateSyncRuntime) ConfigureApplicationBundles(configure func(abcitypes.Application) error) error {
	if configure == nil {
		return errors.New("application bundle configuration is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncDisabled || r.normalServing {
		return errors.New("application bundles must be configured before state sync or normal serving")
	}
	if err := configure(r.bundle.application); err != nil {
		return fmt.Errorf("configure initial application bundle: %w", err)
	}
	r.configureApplication = configure
	return nil
}

// Close waits for delegated calls and releases the active complete bundle.
func (r *BootStateSyncRuntime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.phase {
	case BootStateSyncDiscovering, BootStateSyncAssembling, BootStateSyncPrepared, BootStateSyncPendingComet:
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
	}
	return r.bundle.Close()
}

func (r *BootStateSyncRuntime) Info(ctx context.Context, req *abcitypes.RequestInfo) (*abcitypes.ResponseInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.Info(ctx, req)
}

func (r *BootStateSyncRuntime) Query(ctx context.Context, req *abcitypes.RequestQuery) (*abcitypes.ResponseQuery, error) {
	if err := r.lockNonBlockingServing(); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.Query(ctx, req)
}

func (r *BootStateSyncRuntime) CheckTx(ctx context.Context, req *abcitypes.RequestCheckTx) (*abcitypes.ResponseCheckTx, error) {
	if err := r.lockNonBlockingServing(); err != nil {
		// CometBFT's mempool treats an application-transport error as an
		// invariant violation and panics. Reject peer/RPC transactions through
		// the ABCI protocol while boot state sync is not sealed; never turn the
		// intentional serving gate into a process-kill primitive.
		return &abcitypes.ResponseCheckTx{
			Code:      bootStateSyncCheckTxBlockedCode,
			Codespace: "sage-state-sync",
			Log:       "normal transaction serving is unavailable during boot state sync",
		}, nil
	}
	defer r.mu.RUnlock()
	return r.bundle.application.CheckTx(ctx, req)
}

func (r *BootStateSyncRuntime) InitChain(ctx context.Context, req *abcitypes.RequestInitChain) (*abcitypes.ResponseInitChain, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.InitChain(ctx, req)
}

func (r *BootStateSyncRuntime) PrepareProposal(ctx context.Context, req *abcitypes.RequestPrepareProposal) (*abcitypes.ResponsePrepareProposal, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.PrepareProposal(ctx, req)
}

func (r *BootStateSyncRuntime) ProcessProposal(ctx context.Context, req *abcitypes.RequestProcessProposal) (*abcitypes.ResponseProcessProposal, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.ProcessProposal(ctx, req)
}

func (r *BootStateSyncRuntime) FinalizeBlock(ctx context.Context, req *abcitypes.RequestFinalizeBlock) (*abcitypes.ResponseFinalizeBlock, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.FinalizeBlock(ctx, req)
}

func (r *BootStateSyncRuntime) ExtendVote(ctx context.Context, req *abcitypes.RequestExtendVote) (*abcitypes.ResponseExtendVote, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.ExtendVote(ctx, req)
}

func (r *BootStateSyncRuntime) VerifyVoteExtension(ctx context.Context, req *abcitypes.RequestVerifyVoteExtension) (*abcitypes.ResponseVerifyVoteExtension, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.VerifyVoteExtension(ctx, req)
}

func (r *BootStateSyncRuntime) Commit(ctx context.Context, req *abcitypes.RequestCommit) (*abcitypes.ResponseCommit, error) {
	if err := r.lockConsensusServing(ctx); err != nil {
		return nil, err
	}
	defer r.mu.RUnlock()
	return r.bundle.application.Commit(ctx, req)
}

// lockNonBlockingServing is used by externally triggerable query/mempool
// calls. They must never wait in PendingComet: Comet's in-process ABCI clients
// share one mutex, and a waiting Query or CheckTx could otherwise prevent the
// state-sync syncer from acquiring that mutex for its final Info verification.
// It returns with r.mu held for reading on success.
func (r *BootStateSyncRuntime) lockNonBlockingServing() error {
	r.mu.RLock()
	switch r.phase {
	case BootStateSyncDisabled, BootStateSyncSealed:
		return nil
	default:
		phase := r.phase
		r.mu.RUnlock()
		return fmt.Errorf("%w: phase %d", ErrBootStateSyncConsensusServingBlocked, phase)
	}
}

// lockConsensusServing returns with r.mu held for reading on success. Before
// PendingComet ordinary calls fail fast. During the narrow post-SwitchToBlockSync
// window they wait without owning the lease; the seal/failure transition wakes
// them to re-check phase before any application method can run.
func (r *BootStateSyncRuntime) lockConsensusServing(ctx context.Context) error {
	if ctx == nil {
		return errors.New("ordinary ABCI serving requires a context")
	}
	for {
		r.mu.RLock()
		switch r.phase {
		case BootStateSyncDisabled, BootStateSyncSealed:
			return nil
		case BootStateSyncPendingComet:
			gate := r.consensusServingGate
			r.mu.RUnlock()
			select {
			case <-ctx.Done():
				return fmt.Errorf("%w: wait for boot state-sync seal: %w", ErrBootStateSyncConsensusServingBlocked, ctx.Err())
			case <-gate:
			}
		default:
			phase := r.phase
			r.mu.RUnlock()
			return fmt.Errorf("%w: phase %d", ErrBootStateSyncConsensusServingBlocked, phase)
		}
	}
}

func (r *BootStateSyncRuntime) setBootStateSyncPhaseLocked(next BootStateSyncPhase) {
	r.phase = next
	if next == BootStateSyncSealed || next == BootStateSyncFailed {
		r.consensusGateOnce.Do(func() { close(r.consensusServingGate) })
	}
}

// transitionBootStateSync advances the strict one-shot in-memory lifecycle.
// Endpoint code lives in this package and must use this gate rather than
// assigning phase directly.
func (r *BootStateSyncRuntime) transitionBootStateSync(next BootStateSyncPhase) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.normalServing {
		return errors.New("boot state sync cannot transition after normal serving acquired the application")
	}
	if !validBootStateSyncTransition(r.phase, next) {
		return fmt.Errorf("invalid boot state-sync transition %d -> %d", r.phase, next)
	}
	r.setBootStateSyncPhaseLocked(next)
	return nil
}

// activatePreparedBundle executes activation while holding the exclusive
// runtime lease and publishes the replacement only if it reports the trusted
// snapshot state. Any error permanently fails state sync for this process.
func (r *BootStateSyncRuntime) activatePreparedBundle(expectedHeight int64, expectedHash []byte, expectedAppVersion uint64, activate func(current *ConsensusBundle) (*ConsensusBundle, error)) error {
	return r.activatePreparedBundleAuthorized(expectedHeight, expectedHash, expectedAppVersion, nil, activate)
}

func (r *BootStateSyncRuntime) activatePreparedBundleAuthorized(
	expectedHeight int64,
	expectedHash []byte,
	expectedAppVersion uint64,
	authorize func() error,
	activate func(current *ConsensusBundle) (*ConsensusBundle, error),
) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncPrepared {
		return fmt.Errorf("consensus bundle activation requires prepared phase, got %d", r.phase)
	}
	if activate == nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return errors.New("consensus bundle activator is required")
	}
	if err := validateBundleState(expectedHeight, expectedHash, expectedAppVersion); err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return err
	}
	if expectedAppVersion != 20 {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return errors.New("state sync activation requires app version 20")
	}
	if expectedHeight < r.bundle.expectedHeight ||
		(expectedHeight == r.bundle.expectedHeight && !bytes.Equal(expectedHash, r.bundle.expectedHash)) ||
		expectedAppVersion < r.bundle.expectedAppVersion ||
		(expectedHeight == r.bundle.expectedHeight && expectedAppVersion != r.bundle.expectedAppVersion) {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return errors.New("trusted snapshot would regress or fork the current consensus bundle")
	}
	if authorize != nil {
		if err := authorize(); err != nil {
			r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
			return fmt.Errorf("authorize consensus bundle activation: %w", err)
		}
	}
	current := r.bundle
	// The exclusive lease guarantees no delegated ABCI call still owns the
	// current store graph. Close it before the activator performs any directory
	// transaction: Badger directories cannot be safely renamed/reopened while
	// their process still has the database open (notably on Windows). Once the
	// close begins, every failure is terminal for this process; the old bundle
	// remains intentionally unusable and recovery belongs to the boot journal.
	if err := current.Close(); err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return fmt.Errorf("close current consensus bundle before activation: %w", err)
	}
	next, err := activate(current)
	if err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return closeRejectedConsensusBundle(next, err)
	}
	if next == current {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return errors.New("consensus bundle activation did not replace the bundle")
	}
	if err := validateConsensusBundle(next); err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return closeRejectedConsensusBundle(next, err)
	}
	if next.expectedHeight != expectedHeight || !bytes.Equal(next.expectedHash, expectedHash) || next.expectedAppVersion != expectedAppVersion {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return closeRejectedConsensusBundle(next, errors.New("replacement consensus bundle does not match trusted snapshot state and app version"))
	}
	if r.configureApplication != nil {
		if err := r.configureApplication(next.application); err != nil {
			r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
			return closeRejectedConsensusBundle(next, fmt.Errorf("configure replacement application bundle: %w", err))
		}
	}
	r.bundle = next
	r.setBootStateSyncPhaseLocked(BootStateSyncPendingComet)
	return nil
}

// sealActivatedBundle verifies CometBFT and the live application agree after
// bootstrap before allowing boot to proceed to ordinary services.
func (r *BootStateSyncRuntime) sealActivatedBundle(ctx context.Context, cometHeight int64, cometAppHash []byte, cometAppVersion uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.sealActivatedBundleLocked(ctx, cometHeight, cometAppHash, cometAppVersion)
}

// SealActivatedBundle observes CometBFT state persisted after bootstrap. A
// dormant runtime is a no-op. An armed runtime is sealed only after persisted
// height/hash/version exactly match the live application. The bool reports
// whether durable activation-directory cleanup is now required before serving.
func (r *BootStateSyncRuntime) SealActivatedBundle(ctx context.Context, cometHeight int64, cometAppHash []byte, cometAppVersion uint64) (bool, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.phase {
	case BootStateSyncDisabled:
		return false, nil
	case BootStateSyncPendingComet:
		if err := r.sealActivatedBundleLocked(ctx, cometHeight, cometAppHash, cometAppVersion); err != nil {
			return false, err
		}
		return true, nil
	default:
		return false, fmt.Errorf("consensus bundle sealing requires disabled or pending-Comet phase, got %d", r.phase)
	}
}

// SealActivatedBundleFromComet holds the exclusive application lease while it
// observes Comet's live state store. If Comet persistence is still behind the
// active application, callers receive ErrBootStateSyncPersistencePending and
// may retry without poisoning the one-shot runtime. Equal-height conflict or
// impossible Comet-ahead state fails the runtime permanently.
func (r *BootStateSyncRuntime) SealActivatedBundleFromComet(
	ctx context.Context,
	readComet func() (height int64, appHash []byte, appVersion uint64, err error),
) (bool, int64, []byte, uint64, error) {
	if readComet == nil {
		return false, 0, nil, 0, errors.New("running CometBFT state reader is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	switch r.phase {
	case BootStateSyncDisabled:
		return false, 0, nil, 0, nil
	case BootStateSyncPendingComet:
	default:
		return false, 0, nil, 0, fmt.Errorf("consensus bundle sealing requires disabled or pending-Comet phase, got %d", r.phase)
	}
	if err := ctx.Err(); err != nil {
		return false, 0, nil, 0, err
	}
	info, err := r.bundle.application.Info(ctx, &abcitypes.RequestInfo{})
	if err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return false, 0, nil, 0, fmt.Errorf("read activated consensus bundle state: %w", err)
	}
	if info == nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return false, 0, nil, 0, errors.New("activated consensus bundle Info response is nil")
	}
	if validateErr := validateBundleState(info.LastBlockHeight, info.LastBlockAppHash, info.AppVersion); validateErr != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return false, 0, nil, 0, validateErr
	}
	cometHeight, cometHash, cometVersion, err := readComet()
	if err != nil {
		return false, 0, nil, 0, err
	}
	cometHash = append([]byte(nil), cometHash...)
	// Comet's live state store is canonically empty before its state-sync
	// bootstrap persists the trusted state. Height/hash/version 0 is therefore
	// ordinary persistence lag, not evidence that poisons the one-shot runtime.
	if cometHeight == 0 && len(cometHash) == 0 {
		return false, 0, nil, cometVersion, ErrBootStateSyncPersistencePending
	}
	if validateErr := validateBundleState(cometHeight, cometHash, cometVersion); validateErr != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return false, cometHeight, cometHash, cometVersion, validateErr
	}
	if cometHeight < info.LastBlockHeight {
		return false, cometHeight, cometHash, cometVersion, ErrBootStateSyncPersistencePending
	}
	if cometHeight > info.LastBlockHeight || !bytes.Equal(cometHash, info.LastBlockAppHash) || cometVersion != info.AppVersion {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return false, cometHeight, cometHash, cometVersion, errors.New("persisted CometBFT state conflicts with the active application")
	}
	if err := r.sealActivatedBundleLocked(ctx, cometHeight, cometHash, cometVersion); err != nil {
		return false, cometHeight, cometHash, cometVersion, err
	}
	return true, cometHeight, cometHash, cometVersion, nil
}

func (r *BootStateSyncRuntime) sealActivatedBundleLocked(ctx context.Context, cometHeight int64, cometAppHash []byte, cometAppVersion uint64) error {
	if r.phase != BootStateSyncPendingComet {
		return fmt.Errorf("consensus bundle sealing requires pending-Comet phase, got %d", r.phase)
	}
	if err := validateBundleState(cometHeight, cometAppHash, cometAppVersion); err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return err
	}
	if cometHeight < r.bundle.expectedHeight ||
		(cometHeight == r.bundle.expectedHeight && !bytes.Equal(cometAppHash, r.bundle.expectedHash)) ||
		cometAppVersion < r.bundle.expectedAppVersion ||
		(cometHeight == r.bundle.expectedHeight && cometAppVersion != r.bundle.expectedAppVersion) {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return errors.New("persisted CometBFT state is below or conflicts with the activated snapshot")
	}
	info, err := r.bundle.application.Info(ctx, &abcitypes.RequestInfo{})
	if err != nil {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return fmt.Errorf("read activated consensus bundle state: %w", err)
	}
	if info == nil || info.LastBlockHeight != cometHeight || !bytes.Equal(info.LastBlockAppHash, cometAppHash) || info.AppVersion != cometAppVersion {
		r.setBootStateSyncPhaseLocked(BootStateSyncFailed)
		return errors.New("activated consensus bundle does not match persisted CometBFT state")
	}
	r.setBootStateSyncPhaseLocked(BootStateSyncSealed)
	return nil
}

func validBootStateSyncTransition(current, next BootStateSyncPhase) bool {
	if next == BootStateSyncFailed {
		return current != BootStateSyncSealed && current != BootStateSyncFailed
	}
	switch current {
	case BootStateSyncDisabled:
		return next == BootStateSyncDiscovering
	case BootStateSyncDiscovering:
		return next == BootStateSyncAssembling
	case BootStateSyncAssembling:
		return next == BootStateSyncPrepared || next == BootStateSyncDiscovering
	default:
		return false
	}
}

func validateConsensusBundle(bundle *ConsensusBundle) error {
	if bundle == nil || bundle.application == nil {
		return errors.New("consensus bundle application is required")
	}
	return validateBundleState(bundle.expectedHeight, bundle.expectedHash, bundle.expectedAppVersion)
}

func validateBundleState(height int64, appHash []byte, appVersion uint64) error {
	if appVersion == 0 {
		return errors.New("consensus bundle app version must be positive")
	}
	if height < 0 {
		return errors.New("consensus bundle height cannot be negative")
	}
	if height == 0 {
		if len(appHash) != 0 {
			return errors.New("fresh consensus bundle must have an empty AppHash")
		}
		return nil
	}
	if len(appHash) != sha256.Size {
		return errors.New("consensus bundle AppHash must be SHA-256 sized")
	}
	return nil
}

func closeRejectedConsensusBundle(bundle *ConsensusBundle, cause error) error {
	if bundle == nil {
		return cause
	}
	if closeErr := bundle.Close(); closeErr != nil {
		return errors.Join(cause, fmt.Errorf("close rejected consensus bundle: %w", closeErr))
	}
	return cause
}

func cleanupFailedConsensusBundle(cleanup func() error, cause error) error {
	if cleanup == nil {
		return cause
	}
	if cleanupErr := cleanup(); cleanupErr != nil {
		return errors.Join(cause, fmt.Errorf("cleanup failed consensus bundle construction: %w", cleanupErr))
	}
	return cause
}
