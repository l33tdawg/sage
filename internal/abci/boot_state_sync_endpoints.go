package abci

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"

	"github.com/l33tdawg/sage/internal/statesync"
)

// StateSyncServingControllerConfig is the complete explicit gate for the two
// requester-anonymous ABCI serving endpoints. Authorization construction has
// already proved that the validator-only Comet P2P profile is armed.
type StateSyncServingControllerConfig struct {
	Authorization *statesync.ServingAuthorization
	SnapshotRoot  string
	// MaxSnapshotHeight returns the highest height for which the live chain has
	// both H+1 and H+2 light blocks. CometBFT cannot verify a tip snapshot.
	MaxSnapshotHeight func(context.Context) (uint64, error)
	Now               func() time.Time
}

// StateSyncServingController serves only the public consensus snapshot format.
// It never opens or delegates to SAGE's operator-only rollback snapshots.
type StateSyncServingController struct {
	authorization *statesync.ServingAuthorization
	snapshotRoot  string
	maxHeight     func(context.Context) (uint64, error)
	now           func() time.Time
}

// NewStateSyncServingController builds a still-dormant controller. The runtime
// must additionally be armed with ArmStateSyncServing before it is reachable.
func NewStateSyncServingController(config StateSyncServingControllerConfig) (*StateSyncServingController, error) {
	if config.Authorization == nil {
		return nil, errors.New("state sync serving authorization is required")
	}
	if config.MaxSnapshotHeight == nil {
		return nil, errors.New("state sync serving requires a live eligible-height reader")
	}
	now := stateSyncClock(config.Now)
	if err := config.Authorization.ValidateAt(now()); err != nil {
		return nil, err
	}
	root, err := validateStateSyncControllerRoot(config.SnapshotRoot, "snapshot")
	if err != nil {
		return nil, err
	}
	return &StateSyncServingController{
		authorization: config.Authorization,
		snapshotRoot:  root,
		maxHeight:     config.MaxSnapshotHeight,
		now:           now,
	}, nil
}

// StateSyncPreparedActivation is an app-verified candidate that is safe to
// promote only while the runtime holds its exclusive application lease.
// Activate must recheck authorize immediately before every durable activation
// boundary. Discard removes an unpromoted candidate after rejection.
type StateSyncPreparedActivation struct {
	Activate func(
		ctx context.Context,
		current *ConsensusBundle,
		authorize func() error,
	) (*ConsensusBundle, error)
	Discard func() error
}

// StateSyncReceivePreparer restores and fully verifies an assembled canonical
// state image in an isolated directory while the live application remains
// open. A malformed provider snapshot must fail here, before live state closes,
// so CometBFT can reject it and offer a different authorized snapshot.
type StateSyncReceivePreparer func(
	ctx context.Context,
	metadata statesync.Metadata,
	assembledStatePath string,
) (*StateSyncPreparedActivation, error)

// StateSyncReceiverControllerConfig is the explicit one-shot receiving gate.
// StagingRoot must be an existing real directory outside the live Badger path.
type StateSyncReceiverControllerConfig struct {
	Authorization    *statesync.ReceivingAuthorization
	StagingRoot      string
	Prepare          StateSyncReceivePreparer
	Now              func() time.Time
	RequireDiskSpace func(path string, required uint64) error
}

// StateSyncReceiverController owns exactly one accepted snapshot session.
// Its mutex serializes OfferSnapshot and ApplySnapshotChunk, including final
// activation, because Comet may issue ABCI calls concurrently.
type StateSyncReceiverController struct {
	mu               sync.Mutex
	authorization    *statesync.ReceivingAuthorization
	stagingRoot      string
	prepare          StateSyncReceivePreparer
	now              func() time.Time
	requireDiskSpace func(path string, required uint64) error
	session          *stateSyncReceiveSession
	completed        *stateSyncCompletedSession
	expired          bool
	rejected         uint32
}

const maxRejectedStateSyncSnapshots = 8

type stateSyncReceiveSession struct {
	dir       string
	metadata  statesync.Metadata
	assembler *statesync.Assembler
}

type stateSyncCompletedSession struct {
	metadata statesync.Metadata
}

// NewStateSyncReceiverController builds a still-dormant receiver. Arming it is
// a separate, explicit runtime operation that begins the one-shot phase graph.
func NewStateSyncReceiverController(config StateSyncReceiverControllerConfig) (*StateSyncReceiverController, error) {
	if config.Authorization == nil {
		return nil, errors.New("state sync receiving authorization is required")
	}
	if config.Prepare == nil {
		return nil, errors.New("state sync receiving preparer is required")
	}
	now := stateSyncClock(config.Now)
	requireDiskSpace := config.RequireDiskSpace
	if requireDiskSpace == nil {
		requireDiskSpace = statesync.RequireAvailableDiskSpace
	}
	if err := config.Authorization.ValidateAt(now()); err != nil {
		return nil, err
	}
	root, err := validateStateSyncControllerRoot(config.StagingRoot, "staging")
	if err != nil {
		return nil, err
	}
	return &StateSyncReceiverController{
		authorization:    config.Authorization,
		stagingRoot:      root,
		prepare:          config.Prepare,
		now:              now,
		requireDiskSpace: requireDiskSpace,
	}, nil
}

// ArmStateSyncServing explicitly enables network snapshot advertisement. It
// does not change the boot receiver phase, so a normal validator remains ready.
func (r *BootStateSyncRuntime) ArmStateSyncServing(controller *StateSyncServingController) error {
	if controller == nil {
		return errors.New("state sync serving controller is required")
	}
	if err := controller.authorization.ValidateAt(controller.now()); err != nil {
		return err
	}
	r.endpointsMu.Lock()
	defer r.endpointsMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncDisabled {
		return fmt.Errorf("state sync serving requires disabled receiver phase, got %d", r.phase)
	}
	if r.normalServing {
		return errors.New("state sync serving cannot be armed after normal serving acquired the application")
	}
	if r.serving != nil || r.receiving != nil {
		return errors.New("state sync endpoint controller is already armed")
	}
	r.serving = controller
	return nil
}

// ArmStateSyncReceiver explicitly starts the one-shot receiving phase. Serving
// and receiving cannot be combined in one process during this release gate.
func (r *BootStateSyncRuntime) ArmStateSyncReceiver(controller *StateSyncReceiverController) error {
	if controller == nil {
		return errors.New("state sync receiving controller is required")
	}
	if err := controller.authorization.ValidateAt(controller.now()); err != nil {
		return err
	}
	r.endpointsMu.Lock()
	defer r.endpointsMu.Unlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.phase != BootStateSyncDisabled {
		return fmt.Errorf("state sync receiving requires disabled phase, got %d", r.phase)
	}
	if r.normalServing {
		return errors.New("state sync receiving cannot be armed after normal serving acquired the application")
	}
	if r.serving != nil || r.receiving != nil {
		return errors.New("state sync endpoint controller is already armed")
	}
	r.receiving = controller
	r.setBootStateSyncPhaseLocked(BootStateSyncDiscovering)
	return nil
}

// ListSnapshots remains empty unless serving and its primary P2P authorization
// have both been explicitly armed.
func (r *BootStateSyncRuntime) ListSnapshots(ctx context.Context, _ *abcitypes.RequestListSnapshots) (*abcitypes.ResponseListSnapshots, error) {
	controller := r.servingController()
	if controller == nil {
		return &abcitypes.ResponseListSnapshots{}, nil
	}
	if err := controller.authorization.ValidateAt(controller.now()); err != nil {
		return &abcitypes.ResponseListSnapshots{}, nil
	}
	if err := ctx.Err(); err != nil {
		return &abcitypes.ResponseListSnapshots{}, err
	}
	snapshots, err := controller.catalog(ctx)
	if err != nil {
		return &abcitypes.ResponseListSnapshots{}, err
	}
	response := &abcitypes.ResponseListSnapshots{Snapshots: make([]*abcitypes.Snapshot, 0, len(snapshots))}
	for _, snapshot := range snapshots {
		response.Snapshots = append(response.Snapshots, &abcitypes.Snapshot{
			Height:   snapshot.Metadata.Height,
			Format:   statesync.Format,
			Chunks:   uint32(len(snapshot.Metadata.ChunkHashes)), // #nosec G115 -- metadata is bounded by MaxChunks
			Hash:     append([]byte(nil), snapshot.Hash...),
			Metadata: append([]byte(nil), snapshot.Encoded...),
		})
	}
	return response, nil
}

// LoadSnapshotChunk serves only a snapshot selected from the same de-duplicated
// public catalog as ListSnapshots. The ABCI request has no requester identity;
// the already-validated Comet P2P profile is therefore the primary gate.
func (r *BootStateSyncRuntime) LoadSnapshotChunk(ctx context.Context, request *abcitypes.RequestLoadSnapshotChunk) (*abcitypes.ResponseLoadSnapshotChunk, error) {
	controller := r.servingController()
	if controller == nil || request == nil || request.Format != statesync.Format {
		return &abcitypes.ResponseLoadSnapshotChunk{}, nil
	}
	if err := controller.authorization.ValidateAt(controller.now()); err != nil {
		return &abcitypes.ResponseLoadSnapshotChunk{}, nil
	}
	if err := ctx.Err(); err != nil {
		return &abcitypes.ResponseLoadSnapshotChunk{}, err
	}
	snapshots, err := controller.catalog(ctx)
	if err != nil {
		return &abcitypes.ResponseLoadSnapshotChunk{}, err
	}
	for _, snapshot := range snapshots {
		if snapshot.Metadata.Height != request.Height {
			continue
		}
		chunk, err := snapshot.LoadChunk(request.Chunk)
		if err != nil {
			return &abcitypes.ResponseLoadSnapshotChunk{}, err
		}
		return &abcitypes.ResponseLoadSnapshotChunk{Chunk: chunk}, nil
	}
	return &abcitypes.ResponseLoadSnapshotChunk{}, nil
}

// OfferSnapshot accepts at most one fully bound v1 snapshot. Invalid offers
// may be skipped while discovering; any offer after session acceptance aborts
// and permanently fails this process-local attempt.
func (r *BootStateSyncRuntime) OfferSnapshot(ctx context.Context, request *abcitypes.RequestOfferSnapshot) (*abcitypes.ResponseOfferSnapshot, error) {
	controller := r.receivingController()
	if controller == nil {
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_REJECT), nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if r.Phase() != BootStateSyncDiscovering {
		controller.fail(r)
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ABORT), nil
	}
	if err := ctx.Err(); err != nil {
		controller.fail(r)
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ABORT), nil
	}
	if err := controller.requireAuthorizedLocked(); err != nil {
		controller.fail(r)
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ABORT), nil
	}
	if request == nil || request.Snapshot == nil {
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_REJECT), nil
	}
	snapshot := request.Snapshot
	if snapshot.Format != statesync.Format {
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_REJECT_FORMAT), nil
	}
	if snapshot.Height < controller.authorization.SnapshotHeightFloor() || snapshot.Height > math.MaxInt64 {
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_REJECT), nil
	}
	metadata, err := statesync.ValidateOffer(
		snapshot.Format, snapshot.Height, snapshot.Chunks,
		snapshot.Hash, snapshot.Metadata, request.AppHash,
	)
	if err != nil {
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_REJECT), nil
	}
	requiredDisk, err := statesync.ReceiveStagingDiskRequirement(metadata.BackupSize)
	if err != nil {
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_REJECT), nil
	}
	if err := controller.requireDiskSpace(controller.stagingRoot, requiredDisk); err != nil {
		controller.fail(r)
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ABORT), nil
	}
	session, err := controller.newSession(metadata)
	if err != nil {
		controller.fail(r)
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ABORT), nil
	}
	controller.session = session
	if err := r.transitionBootStateSync(BootStateSyncAssembling); err != nil {
		controller.fail(r)
		return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ABORT), nil
	}
	return stateSyncOfferResponse(abcitypes.ResponseOfferSnapshot_ACCEPT), nil
}

// ApplySnapshotChunk enforces the authorized provider set before touching the
// assembler. Final ACCEPT is returned only after the verified backup has been
// synchronously installed and the runtime has reached PendingComet.
func (r *BootStateSyncRuntime) ApplySnapshotChunk(ctx context.Context, request *abcitypes.RequestApplySnapshotChunk) (*abcitypes.ResponseApplySnapshotChunk, error) {
	controller := r.receivingController()
	if controller == nil {
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	controller.mu.Lock()
	defer controller.mu.Unlock()
	if request == nil || !statesync.ValidCometNodeID(request.Sender) {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if err := ctx.Err(); err != nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if err := controller.requireAuthorizedLocked(); err != nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	phase := r.Phase()
	if phase == BootStateSyncDiscovering && controller.session == nil && controller.completed == nil {
		return &abcitypes.ResponseApplySnapshotChunk{
			Result:        abcitypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT,
			RejectSenders: []string{request.Sender},
		}, nil
	}
	if (phase != BootStateSyncAssembling || controller.session == nil) &&
		(phase != BootStateSyncPendingComet || controller.completed == nil) {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if !controller.authorization.AllowsProvider(request.Sender) {
		return &abcitypes.ResponseApplySnapshotChunk{
			Result:        abcitypes.ResponseApplySnapshotChunk_RETRY,
			RefetchChunks: []uint32{request.Index},
			RejectSenders: []string{request.Sender},
		}, nil
	}
	if phase == BootStateSyncPendingComet {
		if controller.completed.matches(request.Index, request.Chunk) {
			return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ACCEPT), nil
		}
		controller.fail(r)
		return &abcitypes.ResponseApplySnapshotChunk{
			Result:        abcitypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT,
			RejectSenders: []string{request.Sender},
		}, nil
	}
	if !validStateSyncTransportChunk(controller.session.metadata, request.Index, request.Chunk) {
		return &abcitypes.ResponseApplySnapshotChunk{
			Result:        abcitypes.ResponseApplySnapshotChunk_RETRY,
			RefetchChunks: []uint32{request.Index},
			RejectSenders: []string{request.Sender},
		}, nil
	}
	if err := controller.session.assembler.AddChunk(request.Index, request.Chunk); err != nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if len(controller.session.assembler.Missing()) != 0 {
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ACCEPT), nil
	}
	operationCtx, cancel, err := controller.authorizedContextLocked(ctx)
	if err != nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	defer cancel()
	statePath := filepath.Join(controller.session.dir, "canonical.state")
	if err := controller.session.assembler.AssembleContext(operationCtx, statePath); err != nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if err := controller.requireAuthorizedLocked(); err != nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	metadata := cloneStateSyncMetadata(controller.session.metadata)
	prepared, err := controller.prepare(operationCtx, cloneStateSyncMetadata(metadata), statePath)
	if err != nil {
		if prepared != nil && prepared.Discard != nil {
			_ = prepared.Discard()
		}
		if operationErr := operationCtx.Err(); operationErr != nil {
			if errors.Is(operationErr, context.DeadlineExceeded) {
				controller.expired = true
			}
			controller.fail(r)
			return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
		}
		if errors.Is(err, statesync.ErrDiskCapacity) {
			controller.fail(r)
			return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
		}
		if controller.rejectCurrentSnapshot(r) {
			return &abcitypes.ResponseApplySnapshotChunk{
				Result:        abcitypes.ResponseApplySnapshotChunk_REJECT_SNAPSHOT,
				RejectSenders: []string{request.Sender},
			}, nil
		}
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if prepared == nil || prepared.Activate == nil || prepared.Discard == nil {
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if err := controller.requireAuthorizedLocked(); err != nil {
		_ = prepared.Discard()
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if err := r.transitionBootStateSync(BootStateSyncPrepared); err != nil {
		_ = prepared.Discard()
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	if err := r.activatePreparedBundleAuthorized(
		int64(metadata.Height), metadata.AppHash, statesync.RequiredAppVersion,
		controller.requireAuthorizedLocked,
		func(current *ConsensusBundle) (*ConsensusBundle, error) {
			return prepared.Activate(operationCtx, current, controller.requireAuthorizedLocked)
		},
	); err != nil {
		_ = prepared.Discard()
		controller.fail(r)
		return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ABORT), nil
	}
	_ = prepared.Discard()
	controller.completed = &stateSyncCompletedSession{metadata: metadata}
	// Activation and its crash journal have already committed. Disposable chunk
	// cleanup cannot safely turn PendingComet into Failed; boot recovery may
	// remove leftovers after a crash or transient unlink failure.
	_ = os.RemoveAll(controller.session.dir)
	controller.session = nil
	return stateSyncApplyResponse(abcitypes.ResponseApplySnapshotChunk_ACCEPT), nil
}

func (r *BootStateSyncRuntime) servingController() *StateSyncServingController {
	r.endpointsMu.RLock()
	defer r.endpointsMu.RUnlock()
	return r.serving
}

func (r *BootStateSyncRuntime) receivingController() *StateSyncReceiverController {
	r.endpointsMu.RLock()
	defer r.endpointsMu.RUnlock()
	return r.receiving
}

func (controller *StateSyncServingController) catalog(ctx context.Context) ([]*statesync.Snapshot, error) {
	maxHeight, err := controller.maxHeight(ctx)
	if err != nil {
		return nil, fmt.Errorf("read state sync eligible height: %w", err)
	}
	if maxHeight == 0 {
		return nil, nil
	}
	snapshots, err := statesync.ListSnapshots(controller.snapshotRoot)
	if err != nil {
		return nil, err
	}
	// ABCI LoadSnapshotChunk identifies a snapshot only by height and format.
	// Select the first entry from ListSnapshots' deterministic newest/hash order
	// for each height so advertisement and subsequent chunk loads cannot diverge.
	selected := make([]*statesync.Snapshot, 0, len(snapshots))
	seenHeights := make(map[uint64]struct{}, len(snapshots))
	for _, snapshot := range snapshots {
		if snapshot.Metadata.Height < controller.authorization.SnapshotHeightFloor() || snapshot.Metadata.Height > maxHeight {
			continue
		}
		if _, exists := seenHeights[snapshot.Metadata.Height]; exists {
			continue
		}
		seenHeights[snapshot.Metadata.Height] = struct{}{}
		selected = append(selected, snapshot)
	}
	return selected, nil
}

func (controller *StateSyncReceiverController) newSession(metadata statesync.Metadata) (*stateSyncReceiveSession, error) {
	for attempt := 0; attempt < 4; attempt++ {
		entropy := make([]byte, 16)
		if _, err := rand.Read(entropy); err != nil {
			return nil, fmt.Errorf("generate state sync session ID: %w", err)
		}
		dir := filepath.Join(controller.stagingRoot, "session-"+hex.EncodeToString(entropy))
		assembler, err := statesync.NewAssembler(dir, metadata)
		if errors.Is(err, os.ErrExist) {
			continue
		}
		if err != nil {
			return nil, err
		}
		return &stateSyncReceiveSession{dir: dir, metadata: cloneStateSyncMetadata(metadata), assembler: assembler}, nil
	}
	return nil, errors.New("allocate fresh state sync staging directory")
}

func (controller *StateSyncReceiverController) fail(runtime *BootStateSyncRuntime) {
	if controller.session != nil {
		_ = os.RemoveAll(controller.session.dir)
		controller.session = nil
	}
	phase := runtime.Phase()
	if phase != BootStateSyncFailed && phase != BootStateSyncSealed {
		_ = runtime.transitionBootStateSync(BootStateSyncFailed)
	}
}

func (controller *StateSyncReceiverController) rejectCurrentSnapshot(runtime *BootStateSyncRuntime) bool {
	if controller.session == nil {
		controller.fail(runtime)
		return false
	}
	if err := os.RemoveAll(controller.session.dir); err != nil {
		controller.fail(runtime)
		return false
	}
	controller.session = nil
	controller.rejected++
	if controller.rejected >= maxRejectedStateSyncSnapshots {
		controller.fail(runtime)
		return false
	}
	if err := runtime.transitionBootStateSync(BootStateSyncDiscovering); err != nil {
		controller.fail(runtime)
		return false
	}
	return true
}

func (controller *StateSyncReceiverController) requireAuthorizedLocked() error {
	if controller.expired {
		return errors.New("state sync receiving authorization is permanently expired")
	}
	if err := controller.authorization.ValidateAt(controller.now()); err != nil {
		controller.expired = true
		return err
	}
	return nil
}

func (controller *StateSyncReceiverController) authorizedContextLocked(parent context.Context) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, errors.New("state sync receiving context is required")
	}
	if err := controller.requireAuthorizedLocked(); err != nil {
		return nil, nil, err
	}
	remaining := controller.authorization.ExpiresAt().Sub(controller.now())
	if remaining <= 0 {
		controller.expired = true
		return nil, nil, errors.New("state sync receiving authorization is expired")
	}
	ctx, cancel := context.WithTimeout(parent, remaining)
	return ctx, cancel, nil
}

func validStateSyncTransportChunk(metadata statesync.Metadata, index uint32, chunk []byte) bool {
	if len(chunk) == 0 || index >= uint32(len(metadata.ChunkHashes)) { // #nosec G115 -- metadata count is bounded
		return false
	}
	expected := uint64(metadata.ChunkSize)
	if int(index) == len(metadata.ChunkHashes)-1 {
		prefix := uint64(metadata.ChunkSize) * uint64(len(metadata.ChunkHashes)-1) // #nosec G115 -- MaxChunks bounds conversion
		if prefix >= metadata.BackupSize {
			return false
		}
		expected = metadata.BackupSize - prefix
	}
	if uint64(len(chunk)) != expected { // #nosec G115 -- chunk length is non-negative
		return false
	}
	hash := sha256.Sum256(chunk)
	return bytes.Equal(hash[:], metadata.ChunkHashes[index])
}

func (session *stateSyncCompletedSession) matches(index uint32, chunk []byte) bool {
	if session == nil || index >= uint32(len(session.metadata.ChunkHashes)) {
		return false
	}
	want := session.metadata.ChunkHashes[index]
	if len(chunk) == 0 {
		return false
	}
	// Assembler already validated canonical length before activation. Matching
	// the committed digest is sufficient for post-activation retransmission.
	hash := sha256.Sum256(chunk)
	return bytes.Equal(hash[:], want)
}

func cloneStateSyncMetadata(metadata statesync.Metadata) statesync.Metadata {
	clone := metadata
	clone.AppHash = append([]byte(nil), metadata.AppHash...)
	clone.BackupHash = append([]byte(nil), metadata.BackupHash...)
	clone.ChunkHashes = make([][]byte, len(metadata.ChunkHashes))
	for index, hash := range metadata.ChunkHashes {
		clone.ChunkHashes[index] = append([]byte(nil), hash...)
	}
	return clone
}

func stateSyncOfferResponse(result abcitypes.ResponseOfferSnapshot_Result) *abcitypes.ResponseOfferSnapshot {
	return &abcitypes.ResponseOfferSnapshot{Result: result}
}

func stateSyncApplyResponse(result abcitypes.ResponseApplySnapshotChunk_Result) *abcitypes.ResponseApplySnapshotChunk {
	return &abcitypes.ResponseApplySnapshotChunk{Result: result}
}

func stateSyncClock(now func() time.Time) func() time.Time {
	if now == nil {
		return time.Now
	}
	return now
}

func validateStateSyncControllerRoot(root, label string) (string, error) {
	if root == "" || !filepath.IsAbs(root) || filepath.Clean(root) != root {
		return "", fmt.Errorf("state sync %s root must be a clean absolute path", label)
	}
	info, err := os.Lstat(root)
	if err != nil {
		return "", fmt.Errorf("inspect state sync %s root: %w", label, err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("state sync %s root must be a real directory", label)
	}
	return root, nil
}
