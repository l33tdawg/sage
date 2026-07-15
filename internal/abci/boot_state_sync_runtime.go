package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"

	abcitypes "github.com/cometbft/cometbft/abci/types"
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
	mu     sync.RWMutex
	bundle *ConsensusBundle
	phase  BootStateSyncPhase
}

var _ abcitypes.Application = (*BootStateSyncRuntime)(nil)

// NewBootStateSyncRuntime creates a dormant runtime. State-sync endpoints stay
// empty/reject/abort until a later release gate explicitly arms a receiver.
func NewBootStateSyncRuntime(bundle *ConsensusBundle) (*BootStateSyncRuntime, error) {
	if err := validateConsensusBundle(bundle); err != nil {
		return nil, err
	}
	return &BootStateSyncRuntime{bundle: bundle, phase: BootStateSyncDisabled}, nil
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

// Close waits for delegated calls and releases the active complete bundle.
func (r *BootStateSyncRuntime) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.bundle.Close()
}

func (r *BootStateSyncRuntime) Info(ctx context.Context, req *abcitypes.RequestInfo) (*abcitypes.ResponseInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.Info(ctx, req)
}

func (r *BootStateSyncRuntime) Query(ctx context.Context, req *abcitypes.RequestQuery) (*abcitypes.ResponseQuery, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.Query(ctx, req)
}

func (r *BootStateSyncRuntime) CheckTx(ctx context.Context, req *abcitypes.RequestCheckTx) (*abcitypes.ResponseCheckTx, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.CheckTx(ctx, req)
}

func (r *BootStateSyncRuntime) InitChain(ctx context.Context, req *abcitypes.RequestInitChain) (*abcitypes.ResponseInitChain, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.InitChain(ctx, req)
}

func (r *BootStateSyncRuntime) PrepareProposal(ctx context.Context, req *abcitypes.RequestPrepareProposal) (*abcitypes.ResponsePrepareProposal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.PrepareProposal(ctx, req)
}

func (r *BootStateSyncRuntime) ProcessProposal(ctx context.Context, req *abcitypes.RequestProcessProposal) (*abcitypes.ResponseProcessProposal, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.ProcessProposal(ctx, req)
}

func (r *BootStateSyncRuntime) FinalizeBlock(ctx context.Context, req *abcitypes.RequestFinalizeBlock) (*abcitypes.ResponseFinalizeBlock, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.FinalizeBlock(ctx, req)
}

func (r *BootStateSyncRuntime) ExtendVote(ctx context.Context, req *abcitypes.RequestExtendVote) (*abcitypes.ResponseExtendVote, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.ExtendVote(ctx, req)
}

func (r *BootStateSyncRuntime) VerifyVoteExtension(ctx context.Context, req *abcitypes.RequestVerifyVoteExtension) (*abcitypes.ResponseVerifyVoteExtension, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.VerifyVoteExtension(ctx, req)
}

func (r *BootStateSyncRuntime) Commit(ctx context.Context, req *abcitypes.RequestCommit) (*abcitypes.ResponseCommit, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.bundle.application.Commit(ctx, req)
}

// ListSnapshots remains dormant here instead of delegating to SageApp. The
// runtime will eventually enforce the validator-only serving profile.
func (r *BootStateSyncRuntime) ListSnapshots(context.Context, *abcitypes.RequestListSnapshots) (*abcitypes.ResponseListSnapshots, error) {
	return &abcitypes.ResponseListSnapshots{}, nil
}

// OfferSnapshot remains fail-closed until boot receiving is explicitly armed.
func (r *BootStateSyncRuntime) OfferSnapshot(context.Context, *abcitypes.RequestOfferSnapshot) (*abcitypes.ResponseOfferSnapshot, error) {
	return &abcitypes.ResponseOfferSnapshot{Result: abcitypes.ResponseOfferSnapshot_REJECT}, nil
}

// LoadSnapshotChunk never delegates to the local rollback snapshot machinery.
func (r *BootStateSyncRuntime) LoadSnapshotChunk(context.Context, *abcitypes.RequestLoadSnapshotChunk) (*abcitypes.ResponseLoadSnapshotChunk, error) {
	return &abcitypes.ResponseLoadSnapshotChunk{}, nil
}

// ApplySnapshotChunk remains fail-closed until the full receiver is armed.
func (r *BootStateSyncRuntime) ApplySnapshotChunk(context.Context, *abcitypes.RequestApplySnapshotChunk) (*abcitypes.ResponseApplySnapshotChunk, error) {
	return &abcitypes.ResponseApplySnapshotChunk{Result: abcitypes.ResponseApplySnapshotChunk_ABORT}, nil
}

// transitionBootStateSync advances the strict one-shot in-memory lifecycle.
// Endpoint code lives in this package and must use this gate rather than
// assigning phase directly.
func (r *BootStateSyncRuntime) transitionBootStateSync(next BootStateSyncPhase) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if !validBootStateSyncTransition(r.phase, next) {
		return fmt.Errorf("invalid boot state-sync transition %d -> %d", r.phase, next)
	}
	r.phase = next
	return nil
}

// activatePreparedBundle executes activation while holding the exclusive
// runtime lease and publishes the replacement only if it reports the trusted
// snapshot state. Any error permanently fails state sync for this process.
func (r *BootStateSyncRuntime) activatePreparedBundle(expectedHeight int64, expectedHash []byte, expectedAppVersion uint64, activate func(current *ConsensusBundle) (*ConsensusBundle, error)) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncPrepared {
		return fmt.Errorf("consensus bundle activation requires prepared phase, got %d", r.phase)
	}
	if activate == nil {
		r.phase = BootStateSyncFailed
		return errors.New("consensus bundle activator is required")
	}
	if err := validateBundleState(expectedHeight, expectedHash, expectedAppVersion); err != nil {
		r.phase = BootStateSyncFailed
		return err
	}
	if expectedAppVersion != 20 {
		r.phase = BootStateSyncFailed
		return errors.New("state sync activation requires app version 20")
	}
	if expectedHeight < r.bundle.expectedHeight ||
		(expectedHeight == r.bundle.expectedHeight && !bytes.Equal(expectedHash, r.bundle.expectedHash)) ||
		expectedAppVersion < r.bundle.expectedAppVersion ||
		(expectedHeight == r.bundle.expectedHeight && expectedAppVersion != r.bundle.expectedAppVersion) {
		r.phase = BootStateSyncFailed
		return errors.New("trusted snapshot would regress or fork the current consensus bundle")
	}
	current := r.bundle
	next, err := activate(current)
	if err != nil {
		r.phase = BootStateSyncFailed
		return closeRejectedConsensusBundle(next, err)
	}
	if next == current {
		r.phase = BootStateSyncFailed
		return errors.New("consensus bundle activation did not replace the bundle")
	}
	if err := validateConsensusBundle(next); err != nil {
		r.phase = BootStateSyncFailed
		return closeRejectedConsensusBundle(next, err)
	}
	if next.expectedHeight != expectedHeight || !bytes.Equal(next.expectedHash, expectedHash) || next.expectedAppVersion != expectedAppVersion {
		r.phase = BootStateSyncFailed
		return closeRejectedConsensusBundle(next, errors.New("replacement consensus bundle does not match trusted snapshot state and app version"))
	}
	if err := current.Close(); err != nil {
		r.phase = BootStateSyncFailed
		return closeRejectedConsensusBundle(next, fmt.Errorf("close replaced consensus bundle: %w", err))
	}
	r.bundle = next
	r.phase = BootStateSyncPendingComet
	return nil
}

// sealActivatedBundle verifies CometBFT and the live application agree after
// bootstrap before allowing boot to proceed to ordinary services.
func (r *BootStateSyncRuntime) sealActivatedBundle(ctx context.Context, cometHeight int64, cometAppHash []byte, cometAppVersion uint64) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncPendingComet {
		return fmt.Errorf("consensus bundle sealing requires pending-Comet phase, got %d", r.phase)
	}
	if err := validateBundleState(cometHeight, cometAppHash, cometAppVersion); err != nil {
		r.phase = BootStateSyncFailed
		return err
	}
	if cometHeight < r.bundle.expectedHeight ||
		(cometHeight == r.bundle.expectedHeight && !bytes.Equal(cometAppHash, r.bundle.expectedHash)) ||
		cometAppVersion < r.bundle.expectedAppVersion ||
		(cometHeight == r.bundle.expectedHeight && cometAppVersion != r.bundle.expectedAppVersion) {
		r.phase = BootStateSyncFailed
		return errors.New("persisted CometBFT state is below or conflicts with the activated snapshot")
	}
	info, err := r.bundle.application.Info(ctx, &abcitypes.RequestInfo{})
	if err != nil {
		r.phase = BootStateSyncFailed
		return fmt.Errorf("read activated consensus bundle state: %w", err)
	}
	if info == nil || info.LastBlockHeight != cometHeight || !bytes.Equal(info.LastBlockAppHash, cometAppHash) || info.AppVersion != cometAppVersion {
		r.phase = BootStateSyncFailed
		return errors.New("activated consensus bundle does not match persisted CometBFT state")
	}
	r.phase = BootStateSyncSealed
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
		return next == BootStateSyncPrepared
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
