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
	application    abcitypes.Application
	expectedHeight int64
	expectedHash   []byte
}

// NewConsensusBundle captures the application's current Info state. Callers
// must finish constructing the complete SageApp graph before wrapping it.
func NewConsensusBundle(ctx context.Context, application abcitypes.Application) (*ConsensusBundle, error) {
	if application == nil {
		return nil, errors.New("consensus bundle application is required")
	}
	info, err := application.Info(ctx, &abcitypes.RequestInfo{})
	if err != nil {
		return nil, fmt.Errorf("read consensus bundle state: %w", err)
	}
	if info == nil {
		return nil, errors.New("consensus bundle Info response is nil")
	}
	if err := validateBundleState(info.LastBlockHeight, info.LastBlockAppHash); err != nil {
		return nil, err
	}
	return &ConsensusBundle{
		application:    application,
		expectedHeight: info.LastBlockHeight,
		expectedHash:   append([]byte(nil), info.LastBlockAppHash...),
	}, nil
}

// ExpectedState returns a private copy of the bundle's construction-time state.
func (b *ConsensusBundle) ExpectedState() (int64, []byte) {
	if b == nil {
		return 0, nil
	}
	return b.expectedHeight, append([]byte(nil), b.expectedHash...)
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
func (r *BootStateSyncRuntime) activatePreparedBundle(expectedHeight int64, expectedHash []byte, activate func(current *ConsensusBundle) (*ConsensusBundle, error)) error {
	if activate == nil {
		return errors.New("consensus bundle activator is required")
	}
	if err := validateBundleState(expectedHeight, expectedHash); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncPrepared {
		return fmt.Errorf("consensus bundle activation requires prepared phase, got %d", r.phase)
	}
	next, err := activate(r.bundle)
	if err != nil {
		r.phase = BootStateSyncFailed
		return err
	}
	if next == r.bundle {
		r.phase = BootStateSyncFailed
		return errors.New("consensus bundle activation did not replace the bundle")
	}
	if err := validateConsensusBundle(next); err != nil {
		r.phase = BootStateSyncFailed
		return err
	}
	if next.expectedHeight != expectedHeight || !bytes.Equal(next.expectedHash, expectedHash) {
		r.phase = BootStateSyncFailed
		return errors.New("replacement consensus bundle does not match trusted snapshot state")
	}
	r.bundle = next
	r.phase = BootStateSyncPendingComet
	return nil
}

// sealActivatedBundle verifies CometBFT and the live application agree after
// bootstrap before allowing boot to proceed to ordinary services.
func (r *BootStateSyncRuntime) sealActivatedBundle(ctx context.Context, cometHeight int64, cometAppHash []byte) error {
	if err := validateBundleState(cometHeight, cometAppHash); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncPendingComet {
		return fmt.Errorf("consensus bundle sealing requires pending-Comet phase, got %d", r.phase)
	}
	info, err := r.bundle.application.Info(ctx, &abcitypes.RequestInfo{})
	if err != nil {
		r.phase = BootStateSyncFailed
		return fmt.Errorf("read activated consensus bundle state: %w", err)
	}
	if info == nil || info.LastBlockHeight != cometHeight || !bytes.Equal(info.LastBlockAppHash, cometAppHash) {
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
	return validateBundleState(bundle.expectedHeight, bundle.expectedHash)
}

func validateBundleState(height int64, appHash []byte) error {
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
