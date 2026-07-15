package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/cometbft/cometbft/config"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"

	sageabci "github.com/l33tdawg/sage/internal/abci"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	defaultStateSyncServingDir   = "state-sync/snapshots"
	defaultStateSyncReceivingDir = "state-sync/receiving"
	stateSyncSessionsDirname     = "sessions"
	stateSyncCometTempDirname    = "comet-temp"
	stateSyncDisposableMarker    = ".sage-state-sync-disposable-v1"
)

func repairMissingBlockStoreArtifacts(
	cometCfg *config.Config,
	pv *privval.FilePV,
	preserveStateSyncEvidence bool,
	logger zerolog.Logger,
) (*privval.FilePV, error) {
	if cometCfg == nil || pv == nil {
		return nil, errors.New("missing-blockstore repair requires CometBFT config and validator state")
	}
	blockStoreDBPath := filepath.Join(cometCfg.RootDir, "data", "blockstore.db")
	if _, statErr := os.Stat(blockStoreDBPath); statErr == nil {
		return pv, nil
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect CometBFT block store before legacy repair: %w", statErr)
	}
	if preserveStateSyncEvidence {
		return pv, nil
	}

	pvStatePath := cometCfg.PrivValidatorStateFile()
	if signedHeight := pv.LastSignState.Height; signedHeight > 0 {
		logger.Warn().
			Int64("signed_height", signedHeight).
			Msg("validator signed ahead of missing block store — resetting signing state to prevent height regression")
		resetState := []byte(`{"height":"0","round":0,"step":0}`)
		if err := os.WriteFile(pvStatePath, resetState, 0o600); err != nil {
			return nil, fmt.Errorf("reset validator state: %w", err)
		}
		pv = privval.LoadFilePV(cometCfg.PrivValidatorKeyFile(), pvStatePath)
	}

	// Replaying an old WAL against deliberately reset ordinary-node databases
	// can hang startup. Receiver/recovery paths never enter this branch.
	csWalDir := filepath.Join(cometCfg.RootDir, "data", "cs.wal")
	if _, walErr := os.Stat(csWalDir); walErr == nil {
		logger.Warn().Msg("removing stale consensus WAL (block store missing)")
		if err := os.RemoveAll(csWalDir); err != nil {
			return nil, fmt.Errorf("remove stale consensus WAL: %w", err)
		}
	} else if !errors.Is(walErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect stale consensus WAL: %w", walErr)
	}
	return pv, nil
}

// armConfiguredStateSync is the production bridge between the opt-in operator
// config and the boot-only ABCI runtime. It is called after the complete
// initial application bundle exists but before node.NewNode can expose any P2P
// endpoint or perform an ABCI handshake.
func armConfiguredStateSync(
	ctx context.Context,
	cfg *Config,
	cometCfg *config.Config,
	pv *privval.FilePV,
	nodeKey *p2p.NodeKey,
	app *sageabci.SageApp,
	offchain *store.SQLiteStore,
	runtime *sageabci.BootStateSyncRuntime,
	logger zerolog.Logger,
) error {
	if cfg == nil || cometCfg == nil || pv == nil || nodeKey == nil || app == nil || offchain == nil || runtime == nil {
		return errors.New("state sync production wiring requires complete config, key, application, store, and runtime inputs")
	}
	stateSyncCfg := cfg.Quorum.StateSync
	if !stateSyncCfg.armed() {
		return nil
	}
	if validationErr := stateSyncCfg.validate(cfg.Quorum.Enabled); validationErr != nil {
		return validationErr
	}
	if stateSyncCfg.Serving && resolveRetainBlocks(cfg.RetainBlocks, cfg.Quorum.Enabled) > 0 {
		return errors.New("state sync serving requires retain_blocks=0 until rolling snapshots are block-base aware")
	}

	authorizationPath := resolveStateSyncPath(cfg.DataDir, stateSyncCfg.AuthorizationFile, "")
	join, err := statesync.LoadJoinAuthorization(authorizationPath)
	if err != nil {
		return fmt.Errorf("load state sync join authorization: %w", err)
	}
	if profileErr := configureValidatorStateSyncP2P(cometCfg.P2P, cfg.Quorum.Peers, stateSyncCfg.AuthorizedPeerIDs); profileErr != nil {
		return fmt.Errorf("configure validator-only state sync P2P profile: %w", profileErr)
	}
	// CometBFT's transport-level limit matches the bounded SAGE v1 format. The
	// locally pinned Comet patch makes rejection safe even when a provider is
	// serving with no receiver syncer allocated.
	cometCfg.StateSync.MaxSnapshotChunks = statesync.MaxChunks
	profile, err := buildValidatorStateSyncProfile(cometCfg, pv, nodeKey)
	if err != nil {
		return err
	}

	if stateSyncCfg.Serving {
		authorization, authErr := statesync.NewServingAuthorization(join, profile, time.Now())
		if authErr != nil {
			return fmt.Errorf("authorize state sync serving: %w", authErr)
		}
		root := resolveStateSyncPath(cfg.DataDir, stateSyncCfg.SnapshotDir, defaultStateSyncServingDir)
		if rootErr := prepareStateSyncRoot(root, filepath.Join(cfg.DataDir, "badger")); rootErr != nil {
			return rootErr
		}
		if maintenanceErr := statesync.MaintainProviderSnapshotRoot(root); maintenanceErr != nil {
			return fmt.Errorf("maintain state sync provider snapshots: %w", maintenanceErr)
		}
		info, infoErr := app.Info(ctx, nil)
		if infoErr != nil {
			return fmt.Errorf("read state sync provider application state: %w", infoErr)
		}
		if info == nil {
			return errors.New("read state sync provider application state: nil response")
		}
		if info.AppVersion != statesync.RequiredAppVersion {
			return fmt.Errorf("state sync serving requires app version %d, got %d", statesync.RequiredAppVersion, info.AppVersion)
		}
		if info.LastBlockHeight <= 0 || len(info.LastBlockAppHash) != sha256.Size {
			return errors.New("state sync serving requires a committed positive-height application state")
		}
		if uint64(info.LastBlockHeight) < authorization.SnapshotHeightFloor() { // #nosec G115 -- positive checked above
			return errors.New("state sync provider height is below the authorization snapshot floor")
		}
		if _, exportErr := ensureStateSyncServingSnapshot(
			ctx,
			app.GetBadgerStore(),
			root,
			uint64(info.LastBlockHeight), // #nosec G115 -- positive checked above
			info.LastBlockAppHash,
			stateSyncCfg.effectiveChunkSize(),
		); exportErr != nil {
			return fmt.Errorf("publish state sync provider snapshot: %w", exportErr)
		}
		controller, controllerErr := sageabci.NewStateSyncServingController(sageabci.StateSyncServingControllerConfig{
			Authorization: authorization,
			SnapshotRoot:  root,
			MaxSnapshotHeight: func(readCtx context.Context) (uint64, error) {
				latest, readErr := app.Info(readCtx, &abcitypes.RequestInfo{})
				if readErr != nil {
					return 0, readErr
				}
				if latest == nil || latest.LastBlockHeight <= 2 {
					return 0, nil
				}
				return uint64(latest.LastBlockHeight - 2), nil // #nosec G115 -- positive after subtraction
			},
		})
		if controllerErr != nil {
			return fmt.Errorf("construct state sync serving endpoints: %w", controllerErr)
		}
		if armErr := runtime.ArmStateSyncServing(controller); armErr != nil {
			return fmt.Errorf("arm state sync serving endpoints: %w", armErr)
		}
		if configErr := cometCfg.ValidateBasic(); configErr != nil {
			return fmt.Errorf("validate state sync provider CometBFT config: %w", configErr)
		}
		logger.Info().
			Str("snapshot_dir", root).
			Int64("height", info.LastBlockHeight).
			Int64("eligible_after_height", info.LastBlockHeight+2).
			Msg("authorized validator state-sync serving armed")
		return nil
	}

	authorization, err := statesync.NewReceivingAuthorization(join, profile, time.Now())
	if err != nil {
		return fmt.Errorf("authorize state sync receiver: %w", err)
	}
	base := resolveStateSyncPath(cfg.DataDir, stateSyncCfg.SnapshotDir, defaultStateSyncReceivingDir)
	if rootErr := prepareStateSyncRoot(base, filepath.Join(cfg.DataDir, "badger")); rootErr != nil {
		return rootErr
	}
	sessionsRoot := filepath.Join(base, stateSyncSessionsDirname)
	cometTempRoot := filepath.Join(base, stateSyncCometTempDirname)
	for _, root := range []string{sessionsRoot, cometTempRoot} {
		if err := prepareDisposableStateSyncRoot(root, filepath.Join(cfg.DataDir, "badger")); err != nil {
			return err
		}
	}
	if freshErr := requireFreshStateSyncReceiver(ctx, cometCfg, pv, app, offchain); freshErr != nil {
		return freshErr
	}
	if err := configureCometStateSyncReceiver(cometCfg.StateSync, stateSyncCfg, cometTempRoot); err != nil {
		return err
	}
	controller, err := sageabci.NewStateSyncReceiverController(sageabci.StateSyncReceiverControllerConfig{
		Authorization: authorization,
		StagingRoot:   sessionsRoot,
		Prepare: newStateSyncReceivePreparer(
			cfg.DataDir,
			filepath.Join(cfg.DataDir, "badger"),
			offchain,
			logger,
		),
	})
	if err != nil {
		return fmt.Errorf("construct state sync receiver endpoints: %w", err)
	}
	if err := runtime.ArmStateSyncReceiver(controller); err != nil {
		return fmt.Errorf("arm state sync receiver endpoints: %w", err)
	}
	if err := cometCfg.ValidateBasic(); err != nil {
		return fmt.Errorf("validate state sync receiver CometBFT config: %w", err)
	}
	logger.Info().
		Str("staging_dir", base).
		Int64("trust_height", stateSyncCfg.TrustHeight).
		Msg("authorized one-shot validator state-sync receiver armed")
	return nil
}

func resolveStateSyncPath(dataDir, configured, fallback string) string {
	path := strings.TrimSpace(configured)
	if path == "" {
		path = fallback
	}
	path = expandTilde(path)
	if !filepath.IsAbs(path) {
		path = filepath.Join(dataDir, filepath.FromSlash(path))
	}
	return filepath.Clean(path)
}

func prepareStateSyncRoot(root, liveBadgerPath string) error {
	if root == "" || liveBadgerPath == "" || !filepath.IsAbs(root) || !filepath.IsAbs(liveBadgerPath) ||
		filepath.Clean(root) != root || filepath.Clean(liveBadgerPath) != liveBadgerPath {
		return errors.New("state sync root must be a clean absolute path")
	}
	physicalRoot, err := resolvePathThroughExistingAncestor(root)
	if err != nil {
		return fmt.Errorf("resolve state sync root: %w", err)
	}
	physicalLive, err := resolvePathThroughExistingAncestor(liveBadgerPath)
	if err != nil {
		return fmt.Errorf("resolve live Badger path: %w", err)
	}
	if pathsOverlap(physicalRoot, physicalLive) {
		return errors.New("state sync staging/snapshot root must not overlap the live Badger directory")
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return fmt.Errorf("create state sync root: %w", err)
	}
	info, err := os.Lstat(root)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("state sync root must be a real directory")
	}
	if info.Mode().Perm()&0o022 != 0 {
		return errors.New("state sync root must not be group/world writable")
	}
	physicalRoot, err = filepath.EvalSymlinks(root)
	if err != nil {
		return fmt.Errorf("resolve created state sync root: %w", err)
	}
	physicalLive, err = filepath.EvalSymlinks(liveBadgerPath)
	if err != nil {
		return fmt.Errorf("resolve live Badger path: %w", err)
	}
	if pathsOverlap(physicalRoot, physicalLive) {
		return errors.New("physical state sync root overlaps the live Badger directory")
	}
	return nil
}

func resolvePathThroughExistingAncestor(path string) (string, error) {
	path = filepath.Clean(path)
	remaining := make([]string, 0, 4)
	current := path
	for {
		if _, err := os.Lstat(current); err == nil {
			resolved, resolveErr := filepath.EvalSymlinks(current)
			if resolveErr != nil {
				return "", resolveErr
			}
			for index := len(remaining) - 1; index >= 0; index-- {
				resolved = filepath.Join(resolved, remaining[index])
			}
			return filepath.Clean(resolved), nil
		} else if !errors.Is(err, os.ErrNotExist) {
			return "", err
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", errors.New("state sync path has no existing ancestor")
		}
		remaining = append(remaining, filepath.Base(current))
		current = parent
	}
}

func prepareDisposableStateSyncRoot(root, liveBadgerPath string) error {
	_, statErr := os.Lstat(root)
	created := errors.Is(statErr, os.ErrNotExist)
	if statErr != nil && !created {
		return fmt.Errorf("inspect disposable state sync root: %w", statErr)
	}
	if err := prepareStateSyncRoot(root, liveBadgerPath); err != nil {
		return err
	}
	ownedRoot, err := os.OpenRoot(root)
	if err != nil {
		return fmt.Errorf("open disposable state sync root: %w", err)
	}
	defer func() { _ = ownedRoot.Close() }()
	cleanupUnownedRoot := created
	defer func() {
		if cleanupUnownedRoot {
			_ = ownedRoot.RemoveAll(stateSyncDisposableMarker)
			_ = ownedRoot.Close()
			_ = os.Remove(root)
		}
	}()
	if created {
		marker, err := ownedRoot.OpenFile(stateSyncDisposableMarker, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return fmt.Errorf("create disposable state sync marker: %w", err)
		}
		if _, err := marker.Write([]byte("SAGE state-sync disposable root v1\n")); err != nil {
			_ = marker.Close()
			return err
		}
		if err := marker.Sync(); err != nil {
			_ = marker.Close()
			return err
		}
		if err := marker.Close(); err != nil {
			return err
		}
		cleanupUnownedRoot = false
	} else if err := requireStateSyncDisposableMarker(ownedRoot); err != nil {
		return err
	}
	directory, err := ownedRoot.Open(".")
	if err != nil {
		return err
	}
	entries, readErr := directory.ReadDir(-1)
	closeErr := directory.Close()
	if readErr != nil {
		return readErr
	}
	if closeErr != nil {
		return closeErr
	}
	for _, entry := range entries {
		if entry.Name() == stateSyncDisposableMarker {
			continue
		}
		if err := ownedRoot.RemoveAll(entry.Name()); err != nil {
			return fmt.Errorf("remove stale disposable state sync entry %q: %w", entry.Name(), err)
		}
	}
	return syncStateSyncDirectory(root)
}

func requireStateSyncDisposableMarker(root *os.Root) error {
	if root == nil {
		return errors.New("existing disposable state sync root handle is required")
	}
	info, err := root.Lstat(stateSyncDisposableMarker)
	if err != nil || !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 || info.Size() > 128 {
		return errors.New("existing disposable state sync root is missing its secure ownership marker")
	}
	contents, err := root.ReadFile(stateSyncDisposableMarker)
	if err != nil {
		return err
	}
	if string(contents) != "SAGE state-sync disposable root v1\n" {
		return errors.New("disposable state sync ownership marker is invalid")
	}
	return nil
}

func syncStateSyncDirectory(path string) error {
	directory, err := os.Open(path) //nolint:gosec // validated state-sync directory
	if err != nil {
		return err
	}
	defer func() { _ = directory.Close() }()
	return directory.Sync()
}

func pathsOverlap(left, right string) bool {
	left = filepath.Clean(left)
	right = filepath.Clean(right)
	if left == right {
		return true
	}
	leftWithSeparator := left + string(os.PathSeparator)
	rightWithSeparator := right + string(os.PathSeparator)
	return strings.HasPrefix(leftWithSeparator, rightWithSeparator) || strings.HasPrefix(rightWithSeparator, leftWithSeparator)
}

func buildValidatorStateSyncProfile(cometCfg *config.Config, pv *privval.FilePV, nodeKey *p2p.NodeKey) (statesync.ValidatorP2PProfile, error) {
	if cometCfg == nil || cometCfg.P2P == nil || pv == nil || nodeKey == nil {
		return statesync.ValidatorP2PProfile{}, errors.New("state sync P2P profile requires Comet config and local keys")
	}
	chainID, err := readChainIDFromGenesis(cometCfg.RootDir)
	if err != nil {
		return statesync.ValidatorP2PProfile{}, fmt.Errorf("read live state sync chain ID: %w", err)
	}
	if chainID == "" {
		return statesync.ValidatorP2PProfile{}, errors.New("read live state sync chain ID: empty chain ID")
	}
	publicKey, err := pv.GetPubKey()
	if err != nil {
		return statesync.ValidatorP2PProfile{}, fmt.Errorf("read state sync validator public key: %w", err)
	}
	if publicKey == nil || publicKey.Type() != cmted25519.KeyType || len(publicKey.Bytes()) != cmted25519.PubKeySize {
		return statesync.ValidatorP2PProfile{}, errors.New("state sync requires an Ed25519 local validator public key")
	}
	persistentIDs, err := stateSyncPersistentPeerIDs(cometCfg.P2P.PersistentPeers)
	if err != nil {
		return statesync.ValidatorP2PProfile{}, err
	}
	return statesync.ValidatorP2PProfile{
		ChainID:                 chainID,
		LocalNodeID:             string(nodeKey.ID()),
		LocalValidatorPublicKey: append([]byte(nil), publicKey.Bytes()...),
		PEX:                     cometCfg.P2P.PexReactor,
		Seeds:                   splitStateSyncCSV(cometCfg.P2P.Seeds),
		MaxInboundPeers:         cometCfg.P2P.MaxNumInboundPeers,
		UnconditionalPeerIDs:    splitStateSyncCSV(cometCfg.P2P.UnconditionalPeerIDs),
		PrivatePeerIDs:          splitStateSyncCSV(cometCfg.P2P.PrivatePeerIDs),
		PersistentPeerIDs:       persistentIDs,
	}, nil
}

func stateSyncPersistentPeerIDs(value string) ([]string, error) {
	peers := splitStateSyncCSV(value)
	ids := make([]string, 0, len(peers))
	for _, peer := range peers {
		address, err := p2p.NewNetAddressString(peer)
		if err != nil {
			return nil, fmt.Errorf("parse state sync persistent peer %q: %w", peer, err)
		}
		ids = append(ids, string(address.ID))
	}
	return ids, nil
}

func splitStateSyncCSV(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	trimmed := make([]string, 0, len(parts))
	for _, part := range parts {
		if value := strings.TrimSpace(part); value != "" {
			trimmed = append(trimmed, value)
		}
	}
	return trimmed
}

func ensureStateSyncServingSnapshot(
	ctx context.Context,
	badgerStore *store.BadgerStore,
	root string,
	height uint64,
	appHash []byte,
	chunkSize uint32,
) (*statesync.Snapshot, error) {
	if badgerStore == nil || badgerStore.DB() == nil {
		return nil, errors.New("state sync snapshot export requires the live Badger store")
	}
	existing, err := statesync.ListSnapshots(root)
	if err != nil {
		return nil, err
	}
	for _, snapshot := range existing {
		if snapshot.Metadata.Height != height {
			continue
		}
		if !bytes.Equal(snapshot.Metadata.AppHash, appHash) {
			return nil, errors.New("state sync catalog contains a conflicting snapshot at the current height")
		}
		return snapshot, nil
	}
	return statesync.Export(
		ctx,
		badgerStore.DB(),
		root,
		height,
		appHash,
		chunkSize,
		sageabci.AppV20StateSyncBackupVerifier(height),
	)
}

func requireFreshStateSyncReceiver(ctx context.Context, cometCfg *config.Config, pv *privval.FilePV, app *sageabci.SageApp, offchain *store.SQLiteStore) error {
	if cometCfg == nil || pv == nil || app == nil || offchain == nil {
		return errors.New("state sync receiver freshness check requires Comet config, validator key, application, and projection")
	}
	info, err := app.Info(ctx, nil)
	if err != nil {
		return fmt.Errorf("read state sync receiver application state: %w", err)
	}
	if info == nil {
		return errors.New("read state sync receiver application state: nil response")
	}
	if info.LastBlockHeight != 0 || len(info.LastBlockAppHash) != 0 {
		return errors.New("state sync receiving requires an empty local application state")
	}
	badgerStore := app.GetBadgerStore()
	if badgerStore == nil || badgerStore.DB() == nil {
		return errors.New("state sync receiver has no local Badger store")
	}
	badgerEmpty := true
	if err := badgerStore.DB().View(func(txn *badger.Txn) error {
		iterator := txn.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		iterator.Rewind()
		badgerEmpty = !iterator.Valid()
		return nil
	}); err != nil {
		return fmt.Errorf("inspect state sync receiver Badger store: %w", err)
	}
	if !badgerEmpty {
		return errors.New("state sync receiving requires an empty Badger keyspace")
	}
	if err := offchain.RequirePristineStateSyncProjection(ctx); err != nil {
		return err
	}
	cometHeight, cometHash, err := readPersistedCometState(cometCfg)
	if err != nil {
		return fmt.Errorf("read state sync receiver Comet state: %w", err)
	}
	if cometHeight != 0 || len(cometHash) != 0 {
		return errors.New("state sync receiving requires an empty local CometBFT state")
	}
	if _, err := recoverIncompleteCometStateSyncBootstrap(cometCfg); err != nil {
		return err
	}
	for _, database := range []string{"state", "blockstore", "evidence", "tx_index"} {
		if err := requireEmptyCometDatabase(cometCfg, database); err != nil {
			return err
		}
	}
	if cometCfg.Consensus == nil {
		return errors.New("state sync receiver requires CometBFT consensus config")
	}
	if walPath := cometCfg.Consensus.WalFile(); walPath != "" {
		if info, err := os.Lstat(walPath); err == nil {
			if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() != 0 {
				return errors.New("state sync receiving requires an absent or empty consensus WAL")
			}
		} else if !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("inspect state sync receiver consensus WAL: %w", err)
		}
	}
	lastSign := pv.LastSignState
	if lastSign.Height != 0 || lastSign.Round != 0 || lastSign.Step != 0 || len(lastSign.Signature) != 0 || len(lastSign.SignBytes) != 0 {
		return errors.New("state sync receiving requires a fresh validator signing state")
	}
	genesis, err := cmttypes.GenesisDocFromFile(cometCfg.GenesisFile())
	if err != nil {
		return fmt.Errorf("read state sync receiver genesis: %w", err)
	}
	localPublicKey, err := pv.GetPubKey()
	if err != nil || localPublicKey == nil {
		return errors.New("read state sync receiver validator public key")
	}
	if len(genesis.Validators) == 1 && genesis.Validators[0].PubKey != nil &&
		bytes.Equal(genesis.Validators[0].PubKey.Address(), localPublicKey.Address()) {
		return errors.New("state sync receiving would be skipped because genesis names only the local validator")
	}
	return nil
}

// recoverIncompleteCometStateSyncBootstrap runs only after the state store has
// been proven canonically empty. It removes the exact lone seen-commit residue
// left by a crash between Comet's commit-first and state-bootstrap writes; all
// ambiguous block-store content remains for the freshness gate to reject.
func recoverIncompleteCometStateSyncBootstrap(cometCfg *config.Config) (bool, error) {
	if cometCfg == nil {
		return false, errors.New("recover incomplete state sync bootstrap requires CometBFT config")
	}
	db, err := config.DefaultDBProvider(&config.DBContext{ID: "blockstore", Config: cometCfg})
	if err != nil {
		return false, fmt.Errorf("open state sync receiver CometBFT block store: %w", err)
	}
	recovered, recoverErr := cmtstore.RecoverIncompleteStateSyncBootstrapDB(db)
	closeErr := db.Close()
	if closeErr != nil {
		closeErr = fmt.Errorf("close state sync receiver CometBFT block store: %w", closeErr)
	}
	if recoverErr != nil {
		return false, errors.Join(recoverErr, closeErr)
	}
	if closeErr != nil {
		return false, closeErr
	}
	return recovered, nil
}

func requireEmptyCometDatabase(cometCfg *config.Config, database string) error {
	db, err := config.DefaultDBProvider(&config.DBContext{ID: database, Config: cometCfg})
	if err != nil {
		return fmt.Errorf("open state sync receiver CometBFT %s database: %w", database, err)
	}
	iterator, iterErr := db.Iterator(nil, nil)
	if iterErr != nil {
		_ = db.Close()
		return fmt.Errorf("inspect state sync receiver CometBFT %s database: %w", database, iterErr)
	}
	populated := iterator.Valid()
	iteratorCloseErr := iterator.Close()
	dbCloseErr := db.Close()
	if iteratorCloseErr != nil {
		return fmt.Errorf("close state sync receiver CometBFT %s iterator: %w", database, iteratorCloseErr)
	}
	if dbCloseErr != nil {
		return fmt.Errorf("close state sync receiver CometBFT %s database: %w", database, dbCloseErr)
	}
	if populated {
		return fmt.Errorf("state sync receiving requires an empty CometBFT %s database", database)
	}
	return nil
}

func configureCometStateSyncReceiver(cometCfg *config.StateSyncConfig, cfg QuorumStateSyncConfig, tempDir string) error {
	if cometCfg == nil {
		return errors.New("CometBFT state sync config is required")
	}
	trustPeriod, err := time.ParseDuration(strings.TrimSpace(cfg.TrustPeriod))
	if err != nil || trustPeriod <= 0 {
		return errors.New("state sync receiver trust period must be a positive duration")
	}
	cometCfg.Enable = true
	cometCfg.TempDir = tempDir
	cometCfg.RPCServers = make([]string, 0, len(cfg.RPCServers))
	for _, server := range cfg.RPCServers {
		cometCfg.RPCServers = append(cometCfg.RPCServers, strings.TrimRight(strings.TrimSpace(server), "/"))
	}
	cometCfg.TrustHeight = cfg.TrustHeight
	cometCfg.TrustHash = strings.TrimSpace(cfg.TrustHash)
	cometCfg.TrustPeriod = trustPeriod
	cometCfg.MaxSnapshotChunks = statesync.MaxChunks
	if err := cometCfg.ValidateBasic(); err != nil {
		return fmt.Errorf("validate CometBFT state sync receiver config: %w", err)
	}
	return nil
}

func newStateSyncReceivePreparer(
	dataDir string,
	liveBadgerPath string,
	offchain *store.SQLiteStore,
	logger zerolog.Logger,
) sageabci.StateSyncReceivePreparer {
	return func(ctx context.Context, metadata statesync.Metadata, statePath string) (*sageabci.StateSyncPreparedActivation, error) {
		if offchain == nil {
			return nil, errors.New("state sync finalizer requires the process-owned off-chain store")
		}
		requiredDisk, err := statesync.ReceivePreparationDiskRequirement(metadata.BackupSize)
		if err != nil {
			return nil, fmt.Errorf("calculate state sync preparation capacity: %w", err)
		}
		if err := statesync.RequireAvailableDiskSpace(dataDir, requiredDisk); err != nil {
			return nil, fmt.Errorf("reserve state sync preparation capacity: %w", err)
		}
		encoded, err := statesync.EncodeMetadata(metadata)
		if err != nil {
			return nil, fmt.Errorf("encode trusted state sync metadata: %w", err)
		}
		metadataHash := sha256.Sum256(encoded)
		preparedName, err := allocateStateSyncActivationName(dataDir, "badger.state-sync-prepared-")
		if err != nil {
			return nil, err
		}
		quarantineName, err := allocateStateSyncActivationName(dataDir, "badger.state-sync-quarantine-")
		if err != nil {
			return nil, err
		}
		preparedPath := filepath.Join(dataDir, preparedName)
		if prepareErr := sageabci.PrepareAppV20StateSyncBackup(ctx, statePath, preparedPath, metadata.Height, metadata.AppHash); prepareErr != nil {
			if statesync.IsLocalDiskCapacityError(prepareErr) {
				return nil, fmt.Errorf("%w: prepare received app-v20 state: %w", statesync.ErrDiskCapacity, prepareErr)
			}
			return nil, fmt.Errorf("prepare received app-v20 state: %w", prepareErr)
		}
		journal := statesync.ActivationJournal{
			Phase:          statesync.ActivationPrepared,
			Height:         metadata.Height,
			AppHash:        append([]byte(nil), metadata.AppHash...),
			MetadataHash:   append([]byte(nil), metadataHash[:]...),
			PreparedName:   preparedName,
			QuarantineName: quarantineName,
			LiveName:       filepath.Base(liveBadgerPath),
		}
		journalPath := filepath.Join(dataDir, stateSyncActivationJournalName)
		return &sageabci.StateSyncPreparedActivation{
			Activate: func(activationCtx context.Context, _ *sageabci.ConsensusBundle, authorize func() error) (*sageabci.ConsensusBundle, error) {
				if authorize == nil {
					return nil, errors.New("state sync activation authorization guard is required")
				}
				if activationErr := statesync.ActivatePreparedDirectoryAuthorized(
					dataDir,
					journalPath,
					journal,
					func(path string, height uint64, appHash []byte) error {
						return sageabci.VerifyActivatedAppV20StateSyncDirectory(activationCtx, path, height, appHash)
					},
					authorize,
				); activationErr != nil {
					return nil, fmt.Errorf("activate received app-v20 state: %w", activationErr)
				}
				activatedStore, err := store.OpenBadgerStoreWithoutMigrations(liveBadgerPath)
				if err != nil {
					return nil, fmt.Errorf("open activated state sync Badger store: %w", err)
				}
				activatedApp, err := sageabci.NewSageAppWithStores(activatedStore, offchain, logger)
				if err != nil {
					_ = activatedStore.CloseBadger()
					return nil, fmt.Errorf("construct activated SAGE application: %w", err)
				}
				bundle, err := sageabci.NewConsensusBundleWithCleanup(activationCtx, activatedApp, activatedApp.CloseConsensusState)
				if err != nil {
					return nil, fmt.Errorf("construct activated consensus bundle: %w", err)
				}
				return bundle, nil
			},
			Discard: func() error {
				if _, err := os.Lstat(journalPath); err == nil {
					return nil
				} else if !errors.Is(err, os.ErrNotExist) {
					return fmt.Errorf("inspect state sync activation journal before candidate cleanup: %w", err)
				}
				if err := os.RemoveAll(preparedPath); err != nil {
					return fmt.Errorf("discard prepared state sync candidate: %w", err)
				}
				return syncStateSyncDirectory(dataDir)
			},
		}, nil
	}
}

func allocateStateSyncActivationName(root, prefix string) (string, error) {
	for attempt := 0; attempt < 8; attempt++ {
		entropy := make([]byte, 16)
		if _, err := rand.Read(entropy); err != nil {
			return "", fmt.Errorf("generate state sync activation name: %w", err)
		}
		name := prefix + hex.EncodeToString(entropy)
		_, err := os.Lstat(filepath.Join(root, name))
		if errors.Is(err, os.ErrNotExist) {
			return name, nil
		}
		if err != nil {
			return "", fmt.Errorf("inspect state sync activation name: %w", err)
		}
	}
	return "", errors.New("allocate unique state sync activation directory name")
}
