package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strings"

	"github.com/cometbft/cometbft/p2p"

	"github.com/l33tdawg/sage/internal/store"
)

const (
	stateSyncProjectionBaselineName        = "state-sync-projection-baseline.json"
	stateSyncProjectionBaselinePendingName = "state-sync-projection-baseline.pending.json"
	stateSyncProjectionBaselineVersion     = 1
	maxStateSyncProjectionBaselineBytes    = 64 << 20
	maxStateSyncProjectionBaselineIDs      = 1_000_000
	maxStateSyncProjectionBaselineIDBytes  = 512
)

// stateSyncProjectionBaseline is a node-local safety boundary, not consensus
// state. It records exactly which canonical IDs lacked ordinary plaintext SQL
// when an authenticated state-sync receiver first entered normal serving.
// Later canonical IDs are never added, so losing any post-sync projection row
// fails closed instead of being mistaken for expected snapshot history.
type stateSyncProjectionBaseline struct {
	Version            int      `json:"version"`
	ChainID            string   `json:"chain_id"`
	NodeID             string   `json:"node_id"`
	NodePublicKey      string   `json:"node_public_key"`
	ValidatorPublicKey string   `json:"validator_public_key"`
	SealedHeight       int64    `json:"sealed_height"`
	SealedAppHash      string   `json:"sealed_app_hash"`
	AllowedMissingIDs  []string `json:"allowed_missing_ids"`
	Digest             string   `json:"digest"`
	Signature          string   `json:"signature"`
	rawV2              *stateSyncProjectionBaselineV2
}

type stateSyncProjectionBaselinePayload struct {
	Version            int      `json:"version"`
	ChainID            string   `json:"chain_id"`
	NodeID             string   `json:"node_id"`
	NodePublicKey      string   `json:"node_public_key"`
	ValidatorPublicKey string   `json:"validator_public_key"`
	SealedHeight       int64    `json:"sealed_height"`
	SealedAppHash      string   `json:"sealed_app_hash"`
	AllowedMissingIDs  []string `json:"allowed_missing_ids"`
}

func stateSyncProjectionBaselinePath(dataDir string) string {
	return filepath.Join(dataDir, stateSyncProjectionBaselineName)
}

func stateSyncProjectionBaselinePendingPath(dataDir string) string {
	return filepath.Join(dataDir, stateSyncProjectionBaselinePendingName)
}

func stateSyncProjectionLocalIdentity(cometHome string) (*p2p.NodeKey, []byte, error) {
	nodeKey, err := p2p.LoadNodeKey(filepath.Join(cometHome, "config", "node_key.json"))
	if err != nil {
		return nil, nil, fmt.Errorf("load state-sync receiver node identity: %w", err)
	}
	validatorPublicKey, err := localValidatorPubKey(cometHome)
	if err != nil {
		return nil, nil, fmt.Errorf("load state-sync receiver validator identity: %w", err)
	}
	if len(validatorPublicKey) != 32 {
		return nil, nil, errors.New("state-sync receiver validator identity is invalid")
	}
	return nodeKey, validatorPublicKey, nil
}

func (baseline *stateSyncProjectionBaseline) payload() stateSyncProjectionBaselinePayload {
	if baseline == nil {
		return stateSyncProjectionBaselinePayload{}
	}
	return stateSyncProjectionBaselinePayload{
		Version:            baseline.Version,
		ChainID:            baseline.ChainID,
		NodeID:             baseline.NodeID,
		NodePublicKey:      baseline.NodePublicKey,
		ValidatorPublicKey: baseline.ValidatorPublicKey,
		SealedHeight:       baseline.SealedHeight,
		SealedAppHash:      baseline.SealedAppHash,
		AllowedMissingIDs:  append([]string(nil), baseline.AllowedMissingIDs...),
	}
}

func stateSyncProjectionBaselineDigest(payload stateSyncProjectionBaselinePayload) (string, error) {
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode state-sync projection baseline digest: %w", err)
	}
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:]), nil
}

func stateSyncProjectionBaselineSignatureMessage(digest string) ([]byte, error) {
	decoded, err := hex.DecodeString(digest)
	if err != nil || len(decoded) != sha256.Size || digest != strings.ToLower(digest) {
		return nil, errors.New("state-sync projection baseline digest is invalid")
	}
	message := append([]byte("SAGE-STATE-SYNC-PROJECTION-BASELINE-V1\x00"), decoded...)
	return message, nil
}

func signStateSyncProjectionBaseline(
	baseline *stateSyncProjectionBaseline,
	nodeKey *p2p.NodeKey,
) error {
	if baseline == nil || nodeKey == nil || nodeKey.PrivKey == nil {
		return errors.New("state-sync projection baseline signer is missing")
	}
	if baseline.Version == stateSyncProjectionBaselineRawVersion {
		v2, err := baseline.verifiedRawV2()
		if err != nil {
			return err
		}
		if err := v2.sign(nodeKey); err != nil {
			return err
		}
		baseline.Signature = v2.Signature
		return nil
	}
	message, err := stateSyncProjectionBaselineSignatureMessage(baseline.Digest)
	if err != nil {
		return err
	}
	signature, err := nodeKey.PrivKey.Sign(message)
	if err != nil {
		return fmt.Errorf("sign state-sync projection baseline: %w", err)
	}
	baseline.Signature = hex.EncodeToString(signature)
	return nil
}

func verifyStateSyncProjectionBaselineSignature(
	baseline *stateSyncProjectionBaseline,
	nodeKey *p2p.NodeKey,
) error {
	if baseline == nil || nodeKey == nil || nodeKey.PrivKey == nil {
		return errors.New("state-sync projection baseline verifier is missing")
	}
	if baseline.Version == stateSyncProjectionBaselineRawVersion {
		v2, err := baseline.verifiedRawV2()
		if err != nil {
			return err
		}
		return v2.verify(nodeKey)
	}
	signature, err := hex.DecodeString(baseline.Signature)
	if err != nil || len(signature) == 0 ||
		baseline.Signature != strings.ToLower(baseline.Signature) {
		return errors.New("state-sync projection baseline signature is invalid")
	}
	message, err := stateSyncProjectionBaselineSignatureMessage(baseline.Digest)
	if err != nil {
		return err
	}
	if !nodeKey.PubKey().VerifySignature(message, signature) {
		return errors.New("state-sync projection baseline signature verification failed")
	}
	return nil
}

func newStateSyncProjectionBaselinePending(
	chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	sealedHeight uint64,
	sealedAppHash []byte,
	allowedMissingIDs []string,
) (*stateSyncProjectionBaseline, error) {
	if sealedHeight == 0 || sealedHeight > math.MaxInt64 {
		return nil, errors.New("state-sync projection baseline authorization height is invalid")
	}
	if nodeKey == nil || nodeKey.PrivKey == nil {
		return nil, errors.New("state-sync projection baseline requires a node signing key")
	}
	pending := &stateSyncProjectionBaseline{
		Version:            stateSyncProjectionBaselineVersion,
		ChainID:            chainID,
		NodeID:             string(nodeKey.ID()),
		NodePublicKey:      hex.EncodeToString(nodeKey.PubKey().Bytes()),
		ValidatorPublicKey: hex.EncodeToString(validatorPublicKey),
		SealedHeight:       int64(sealedHeight), // #nosec G115 -- max int64 checked above
		SealedAppHash:      hex.EncodeToString(sealedAppHash),
		AllowedMissingIDs:  append([]string(nil), allowedMissingIDs...),
	}
	var err error
	pending.Digest, err = stateSyncProjectionBaselineDigest(pending.payload())
	if err != nil {
		return nil, err
	}
	if err := signStateSyncProjectionBaseline(pending, nodeKey); err != nil {
		return nil, err
	}
	if err := validateStateSyncProjectionBaselineIdentity(
		pending,
		chainID,
		nodeKey,
		validatorPublicKey,
		pending.SealedHeight,
		sealedAppHash,
	); err != nil {
		return nil, err
	}
	return pending, nil
}

func validateStateSyncProjectionBaselineShape(baseline *stateSyncProjectionBaseline) error {
	if baseline == nil {
		return errors.New("state-sync projection baseline is missing")
	}
	if baseline.Version == stateSyncProjectionBaselineRawVersion {
		_, err := baseline.verifiedRawV2()
		return err
	}
	if len(baseline.AllowedMissingIDs) > maxStateSyncProjectionBaselineIDs {
		return errors.New("state-sync projection baseline contains too many memory IDs")
	}
	previous := ""
	for _, memoryID := range baseline.AllowedMissingIDs {
		if strings.TrimSpace(memoryID) == "" || len(memoryID) > maxStateSyncProjectionBaselineIDBytes {
			return errors.New("state-sync projection baseline contains an invalid memory ID")
		}
		if previous != "" && memoryID <= previous {
			return errors.New("state-sync projection baseline memory IDs are not strictly sorted")
		}
		previous = memoryID
	}
	if strings.TrimSpace(baseline.Digest) == "" {
		return errors.New("state-sync projection baseline digest is missing")
	}
	wantDigest, err := stateSyncProjectionBaselineDigest(baseline.payload())
	if err != nil {
		return err
	}
	if baseline.Digest != wantDigest {
		return errors.New("state-sync projection baseline digest mismatch")
	}
	return nil
}

func validateStateSyncProjectionBaselineIdentity(
	baseline *stateSyncProjectionBaseline,
	chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	currentHeight int64,
	currentAppHash []byte,
) error {
	if baseline == nil {
		return errors.New("state-sync projection baseline authorization is missing")
	}
	if baseline.Version != stateSyncProjectionBaselineVersion && baseline.Version != stateSyncProjectionBaselineRawVersion {
		return fmt.Errorf("unsupported state-sync projection baseline version %d", baseline.Version)
	}
	if strings.TrimSpace(chainID) == "" || baseline.ChainID != chainID {
		return errors.New("state-sync projection baseline belongs to a different chain")
	}
	if nodeKey == nil || nodeKey.PrivKey == nil ||
		baseline.NodeID != string(nodeKey.ID()) {
		return errors.New("state-sync projection baseline belongs to a different node")
	}
	if baseline.NodePublicKey != hex.EncodeToString(nodeKey.PubKey().Bytes()) {
		return errors.New("state-sync projection baseline belongs to a different node public key")
	}
	if len(validatorPublicKey) != 32 ||
		baseline.ValidatorPublicKey != hex.EncodeToString(validatorPublicKey) {
		return errors.New("state-sync projection baseline belongs to a different validator key")
	}
	if len(currentAppHash) != sha256.Size {
		return errors.New("canonical state has an invalid AppHash for projection baseline validation")
	}
	sealedHash, err := hex.DecodeString(baseline.SealedAppHash)
	if err != nil || len(sealedHash) != sha256.Size ||
		baseline.SealedAppHash != strings.ToLower(baseline.SealedAppHash) {
		return errors.New("state-sync projection baseline has an invalid AppHash")
	}
	if baseline.SealedHeight <= 0 || currentHeight < baseline.SealedHeight {
		return errors.New("state-sync projection baseline height is ahead of canonical state")
	}
	if currentHeight == baseline.SealedHeight && !bytes.Equal(currentAppHash, sealedHash) {
		return errors.New("state-sync projection baseline AppHash does not match canonical state")
	}
	if err := validateStateSyncProjectionBaselineShape(baseline); err != nil {
		return err
	}
	return verifyStateSyncProjectionBaselineSignature(baseline, nodeKey)
}

// captureStateSyncProjectionBaselinePending freezes the exact omission
// authorization while the authenticated snapshot is still closed and before
// its activation journal can be removed. Scoped envelopes are canonically
// recoverable; every other canonical memory is historical snapshot data whose
// ordinary SQL row is intentionally absent on a state-sync receiver.
func captureStateSyncProjectionBaselinePending(
	preparedBadgerPath, dataDir, chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	sealedHeight uint64,
	sealedAppHash []byte,
) error {
	prepared, err := store.OpenBadgerStoreWithoutMigrations(preparedBadgerPath)
	if err != nil {
		return fmt.Errorf("open prepared state-sync projection inventory: %w", err)
	}
	defer func() { _ = prepared.CloseBadger() }()

	inventory, err := prepared.MemoryProjectionInventory(maxStateSyncProjectionBaselineIDs, maxStateSyncProjectionBaselineBytes)
	if err != nil {
		return fmt.Errorf("read prepared canonical projection inventory: %w", err)
	}
	canonicalIDs := inventory.MemoryIDs
	scopedIDs := make(map[string]struct{}, len(inventory.ScopedContentIDs))
	for _, memoryID := range inventory.ScopedContentIDs {
		scopedIDs[memoryID] = struct{}{}
	}
	allowedMissingIDs := make([]string, 0, len(canonicalIDs))
	for _, memoryID := range canonicalIDs {
		if _, recoverable := scopedIDs[memoryID]; !recoverable {
			allowedMissingIDs = append(allowedMissingIDs, memoryID)
		}
	}
	// Every newly minted authorization must bind the exact prepared snapshot,
	// including legacy V1 captures. This does not reinterpret existing V1 files.
	if sealedHeight == 0 || sealedHeight > math.MaxInt64 || !inventory.HeightPresent || inventory.FinalizedHeight != int64(sealedHeight) || !inventory.AppHashPresent || !bytes.Equal(inventory.AppHash, sealedAppHash) {
		return errors.New("state-sync projection inventory does not match its authenticated finalized snapshot")
	}
	constructor := newStateSyncProjectionBaselinePending
	if inventory.RawIdentityRules {
		if !inventory.RawIdentityRulesAt(int64(sealedHeight)) {
			return errors.New("state-sync raw identity rules do not apply at the authenticated snapshot")
		}
		constructor = newStateSyncProjectionBaselineV2Pending
	}
	pending, err := constructor(chainID, nodeKey, validatorPublicKey, sealedHeight, sealedAppHash, allowedMissingIDs)
	if err != nil {
		return err
	}
	if err := writeStateSyncProjectionBaseline(
		stateSyncProjectionBaselinePendingPath(dataDir),
		pending,
	); err != nil {
		return fmt.Errorf("persist state-sync projection baseline authorization: %w", err)
	}
	return nil
}

func removeMatchingStateSyncProjectionBaselinePending(
	dataDir, chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	sealedHeight uint64,
	sealedAppHash []byte,
) error {
	path := stateSyncProjectionBaselinePendingPath(dataDir)
	pending, err := loadStateSyncProjectionBaseline(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := validateStateSyncProjectionBaselineShape(pending); err != nil {
		return err
	}
	if sealedHeight == 0 || sealedHeight > math.MaxInt64 {
		return errors.New("state-sync projection baseline authorization height is invalid")
	}
	if nodeKey == nil || nodeKey.PrivKey == nil {
		return errors.New("state-sync projection baseline node identity is missing")
	}
	if pending.ChainID != chainID ||
		pending.NodeID != string(nodeKey.ID()) ||
		pending.NodePublicKey != hex.EncodeToString(nodeKey.PubKey().Bytes()) ||
		pending.ValidatorPublicKey != hex.EncodeToString(validatorPublicKey) ||
		pending.SealedHeight != int64(sealedHeight) || // #nosec G115 -- max int64 checked above
		pending.SealedAppHash != hex.EncodeToString(sealedAppHash) {
		// A later authorized ceremony owns this marker. An older candidate must
		// not remove it while cleaning up its own prepared directory.
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return syncStateSyncDirectory(dataDir)
}

// validateStateSyncProjectionBaselinePendingAndComplete never derives a new
// authorization from live SQL. It only consumes the exact, identity-bound
// inventory captured from the closed prepared snapshot before activation.
func validateStateSyncProjectionBaselinePendingAndComplete(
	cfg *Config,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	sealedHeight uint64,
	sealedAppHash []byte,
) error {
	if cfg == nil {
		return errors.New("state-sync receiver config is required")
	}
	if sealedHeight == 0 || sealedHeight > math.MaxInt64 {
		return errors.New("state-sync projection baseline authorization height is invalid")
	}
	pending, err := loadStateSyncProjectionBaseline(
		stateSyncProjectionBaselinePendingPath(cfg.DataDir),
	)
	if err != nil {
		return fmt.Errorf("load durable state-sync projection baseline authorization: %w", err)
	}
	if pending.SealedHeight != int64(sealedHeight) || // #nosec G115 -- max int64 checked above
		pending.SealedAppHash != hex.EncodeToString(sealedAppHash) {
		return errors.New("state-sync projection baseline authorization does not match the activation journal")
	}
	if err := validateStateSyncProjectionBaselineIdentity(
		pending,
		cfg.ChainID,
		nodeKey,
		validatorPublicKey,
		int64(sealedHeight), // #nosec G115 -- max int64 checked above
		sealedAppHash,
	); err != nil {
		return fmt.Errorf("validate durable state-sync projection baseline authorization: %w", err)
	}
	if err := completeStateSyncReceivingRole(cfg); err != nil {
		return err
	}
	return nil
}

func validateStateSyncProjectionBaseline(
	baseline *stateSyncProjectionBaseline,
	chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	currentHeight int64,
	currentAppHash []byte,
	canonical *store.BadgerStore,
) error {
	if err := validateStateSyncProjectionBaselineIdentity(
		baseline,
		chainID,
		nodeKey,
		validatorPublicKey,
		currentHeight,
		currentAppHash,
	); err != nil {
		return err
	}
	inventory, err := canonical.MemoryProjectionInventory(maxStateSyncProjectionBaselineIDs, maxStateSyncProjectionBaselineBytes)
	if err != nil {
		return err
	}
	if baseline.Version == stateSyncProjectionBaselineRawVersion {
		if !inventory.RawIdentityRulesAt(baseline.SealedHeight) || inventory.FinalizedHeight != currentHeight || !inventory.AppHashPresent || !bytes.Equal(inventory.AppHash, currentAppHash) {
			return errors.New("state-sync V2 baseline is not qualified by canonical finalized state")
		}
	}
	canonicalIDs := inventory.MemoryIDs
	canonicalSet := make(map[string]struct{}, len(canonicalIDs))
	for _, memoryID := range canonicalIDs {
		canonicalSet[memoryID] = struct{}{}
	}
	for _, memoryID := range baseline.AllowedMissingIDs {
		if _, ok := canonicalSet[memoryID]; !ok {
			return errors.New("state-sync projection baseline names a non-canonical memory ID")
		}
	}
	return nil
}

func writeStateSyncProjectionBaseline(
	path string,
	baseline *stateSyncProjectionBaseline,
) error {
	if path == "" || baseline == nil {
		return errors.New("state-sync projection baseline path and payload are required")
	}
	encoded, err := encodeStateSyncProjectionBaselineDocument(baseline)
	if err != nil {
		return fmt.Errorf("encode state-sync projection baseline: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create state-sync projection baseline directory: %w", err)
	}
	if err := validateStateSyncProjectionBaselineDirectory(filepath.Dir(path)); err != nil {
		return err
	}
	if err := atomicWriteConfig(path, encoded); err != nil {
		return fmt.Errorf("persist state-sync projection baseline: %w", err)
	}
	return nil
}

func validateStateSyncProjectionBaselineDirectory(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect state-sync projection baseline directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state-sync projection baseline directory must be a real directory")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("state-sync projection baseline directory must not be group/world writable")
	}
	return nil
}

func loadStateSyncProjectionBaseline(path string) (*stateSyncProjectionBaseline, error) {
	if err := validateStateSyncProjectionBaselineDirectory(filepath.Dir(path)); err != nil {
		return nil, err
	}
	before, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("state-sync projection baseline must be a regular file")
	}
	if before.Mode().Perm()&0o022 != 0 {
		return nil, errors.New("state-sync projection baseline must not be group/world writable")
	}
	if before.Size() <= 0 || before.Size() > maxStateSyncProjectionBaselineBytes {
		return nil, errors.New("state-sync projection baseline has an invalid size")
	}
	file, err := os.Open(path) //nolint:gosec // validated node-local state file
	if err != nil {
		return nil, fmt.Errorf("open state-sync projection baseline: %w", err)
	}
	defer func() { _ = file.Close() }()
	after, err := file.Stat()
	if err != nil || !os.SameFile(before, after) {
		return nil, errors.New("state-sync projection baseline changed while opening")
	}
	encoded, err := io.ReadAll(io.LimitReader(file, maxStateSyncProjectionBaselineBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read state-sync projection baseline: %w", err)
	}
	if len(encoded) > maxStateSyncProjectionBaselineBytes {
		return nil, errors.New("state-sync projection baseline exceeds the maximum size")
	}
	baseline, err := decodeStateSyncProjectionBaselineDocument(encoded)
	if err != nil {
		return nil, fmt.Errorf("decode state-sync projection baseline: %w", err)
	}
	return baseline, nil
}

func ensureStateSyncProjectionBaseline(
	dataDir, chainID string,
	nodeKey *p2p.NodeKey,
	validatorPublicKey []byte,
	currentHeight int64,
	currentAppHash []byte,
	canonical *store.BadgerStore,
) (*stateSyncProjectionBaseline, error) {
	path := stateSyncProjectionBaselinePath(dataDir)
	baseline, err := loadStateSyncProjectionBaseline(path)
	if errors.Is(err, os.ErrNotExist) {
		pending, pendingErr := loadStateSyncProjectionBaseline(
			stateSyncProjectionBaselinePendingPath(dataDir),
		)
		if pendingErr != nil {
			return nil, fmt.Errorf("load durable state-sync projection baseline authorization: %w", pendingErr)
		}
		if pendingErr = validateStateSyncProjectionBaseline(
			pending, chainID, nodeKey, validatorPublicKey, currentHeight, currentAppHash, canonical,
		); pendingErr != nil {
			return nil, pendingErr
		}
		baseline = pending
		if err = writeStateSyncProjectionBaseline(path, baseline); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	}
	if err := validateStateSyncProjectionBaseline(
		baseline,
		chainID,
		nodeKey,
		validatorPublicKey,
		currentHeight,
		currentAppHash,
		canonical,
	); err != nil {
		return nil, err
	}
	pendingPath := stateSyncProjectionBaselinePendingPath(dataDir)
	if removeErr := os.Remove(pendingPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return nil, fmt.Errorf("remove sealed state-sync projection baseline authorization: %w", removeErr)
	}
	if syncErr := syncStateSyncDirectory(dataDir); syncErr != nil {
		return nil, fmt.Errorf("sync state-sync projection baseline directory: %w", syncErr)
	}
	return baseline, nil
}

func baselineAllowedMissingIDs(
	baseline *stateSyncProjectionBaseline,
) map[string]struct{} {
	if baseline == nil {
		return nil
	}
	allowed := make(map[string]struct{}, len(baseline.AllowedMissingIDs))
	for _, memoryID := range baseline.AllowedMissingIDs {
		allowed[memoryID] = struct{}{}
	}
	return allowed
}

func stateSyncProjectionMissingAllowed(
	allowed map[string]struct{},
	canonical *store.BadgerStore,
	memoryID string,
) bool {
	if _, ok := allowed[memoryID]; !ok || canonical == nil {
		return false
	}
	// A historical ordinary memory can later be enrolled into scoped canonical
	// content. From that point forward its SQL projection is reconstructible and
	// must no longer be excused by the snapshot omission baseline.
	content, err := canonical.GetScopedContent(memoryID)
	return err == nil && content == nil
}
