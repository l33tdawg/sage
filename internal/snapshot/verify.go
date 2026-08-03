package snapshot

// verify.go is the *proof of restorability* for a snapshot. It is the
// gate that runs after staging-write completes (before final rename in
// Take) and again on boot before AutoRestoreLatest accepts a snapshot.
//
// Three layers of check:
//  1. Per-chunk SHA-256 must match the manifest. Catches bit-rot and
//     truncation.
//  2. SQLite PRAGMA integrity_check + foreign_key_check on the
//     restored mirror. Catches logical corruption that the byte hash
//     would miss (rare with VACUUM INTO but possible from disk).
//  3. Replay the badger.backup into a tmpdir DB, compute its AppHash,
//     compare to the manifest. THIS IS THE PROOF: if AppHash matches,
//     the snapshot is functionally restorable. Without this we'd be
//     trusting the chunk hash but not that the contents combine into a
//     consistent on-chain state.
//
// Tarballs are inspected lazily — Verify confirms their hash + that
// they untar cleanly, but does not extract them. Full extraction
// happens in Restore.

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	cmtjson "github.com/cometbft/cometbft/libs/json"
	"github.com/cometbft/cometbft/p2p"
	"github.com/cometbft/cometbft/privval"
	cmtstate "github.com/cometbft/cometbft/state"
	cmtstore "github.com/cometbft/cometbft/store"
	cmttypes "github.com/cometbft/cometbft/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/klauspost/compress/zstd"

	"github.com/l33tdawg/sage/internal/consensuskeys"
	"github.com/l33tdawg/sage/internal/vault"
)

// AppHashComputer lets the verifier hash a restored BadgerDB without
// importing internal/store (which would create a cycle once this
// package is wired into the live process). The default impl is set
// by an init hook in the integration commit.
type AppHashComputer func(badgerPath string) ([]byte, error)

// DefaultAppHashComputer is the function invoked by Verify to compute
// the AppHash from a restored BadgerDB. The integration commit will
// replace this with one wired to store.BadgerStore.ComputeAppHash.
//
// Until then it walks the DB key-by-key and computes the same
// sorted-key sha256 hash that store.BadgerStore.ComputeAppHash does.
// Functionally equivalent; structurally duplicated to keep this
// package import-clean.
var DefaultAppHashComputer AppHashComputer = computeAppHashStandalone

// VerifyOptions controls verify-time behaviour. Most callers pass the
// zero value.
type VerifyOptions struct {
	// VaultPassphrase is required if the manifest's Encrypted flag is
	// true. Verify will return an error otherwise.
	VaultPassphrase string

	// SkipAppHash disables the BadgerDB replay step. Used by tests that
	// stub a manifest with a known-empty AppHash, NOT for production
	// flows where the replay is the whole point.
	SkipAppHash bool

	// AppHashComputer overrides DefaultAppHashComputer for this call.
	// Used by the integration commit to inject the real
	// store.BadgerStore.ComputeAppHash.
	AppHashComputer AppHashComputer

	// RequireRecoveryConfig makes verification prove that the config archive
	// contains the three identities required to boot the same chain.
	RequireRecoveryConfig bool
	// RequireVaultKey additionally requires vault.key in the config archive.
	RequireVaultKey bool

	// CometDBBackend is the backend used by the captured CometBFT databases.
	// When ExpectedCometHeight is positive, verification extracts and opens the
	// captured state and block stores and binds them to the manifest tuple.
	CometDBBackend       string
	ExpectedCometHeight  int64
	ExpectedCometAppHash []byte
}

// Verify checks that the snapshot at dir is structurally complete and
// functionally restorable. It does NOT modify the snapshot directory.
//
// Returns nil when the snapshot is restorable. Returns a descriptive
// error otherwise — the error wraps which check failed so the caller
// can decide whether to drop staging or fall through to the next
// candidate snapshot.
func Verify(dir string) error {
	return VerifyWithOptions(dir, VerifyOptions{})
}

// ReadManifest returns the immutable descriptor for one finalized or staging
// snapshot. Callers still must run VerifyWithOptions before treating it as a
// recovery point; this helper exists so an updater can bind a verified bundle
// to the exact height, AppHash, and running-binary provenance it requested.
func ReadManifest(dir string) (*Manifest, error) {
	manifestBytes, err := os.ReadFile(filepath.Join(dir, chunkManifest))
	if err != nil {
		return nil, fmt.Errorf("read snapshot manifest: %w", err)
	}
	var manifest Manifest
	if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
		return nil, fmt.Errorf("parse snapshot manifest: %w", err)
	}
	return &manifest, nil
}

// VerifyWithOptions is the configurable form of Verify.
func VerifyWithOptions(dir string, opts VerifyOptions) error {
	if dir == "" {
		return errors.New("verify: empty dir")
	}
	if _, err := os.Stat(filepath.Join(dir, OKSentinel)); err != nil {
		// Verify is also called *before* the OK is written (from Take's
		// pre-rename verify path). The staging-dir path skips this check
		// by passing dir = staging dir explicitly; callers using Verify
		// on a finalized snapshot already filtered for OK presence.
		// We don't fail here — readers higher up know to look for OK.
		_ = err
	}

	manifestBytes, err := os.ReadFile(filepath.Join(dir, chunkManifest))
	if err != nil {
		return fmt.Errorf("verify: read manifest: %w", err)
	}
	var m Manifest
	if jerr := json.Unmarshal(manifestBytes, &m); jerr != nil {
		return fmt.Errorf("verify: parse manifest: %w", jerr)
	}

	if m.SchemaVersion > SchemaVersion {
		return fmt.Errorf("verify: snapshot schema_version=%d > binary supports %d",
			m.SchemaVersion, SchemaVersion)
	}
	if m.Encrypted && opts.VaultPassphrase == "" {
		return errors.New("verify: manifest is encrypted but no VaultPassphrase supplied")
	}

	// 1. Hash every chunk listed in the manifest.
	for _, c := range m.Chunks {
		p := filepath.Join(dir, c.Name)
		st, statErr := os.Stat(p)
		if statErr != nil {
			return fmt.Errorf("verify: missing chunk %q: %w", c.Name, statErr)
		}
		if st.Size() != c.Size {
			// For encrypted chunks the manifest stores the on-disk size,
			// so a mismatch is always a problem.
			return fmt.Errorf("verify: chunk %q size=%d, manifest=%d", c.Name, st.Size(), c.Size)
		}
		var got string
		if m.Encrypted && isEncryptedChunk(c.Name) {
			ch, hErr := hashPlaintextEncryptedFile(p, opts.VaultPassphrase)
			if hErr != nil {
				return fmt.Errorf("verify: decrypt+hash %q: %w", c.Name, hErr)
			}
			got = ch.SHA256
		} else {
			ch, hErr := hashFile(p)
			if hErr != nil {
				return fmt.Errorf("verify: hash %q: %w", c.Name, hErr)
			}
			got = ch.SHA256
		}
		if got != c.SHA256 {
			return fmt.Errorf("verify: chunk %q hash mismatch (got %s want %s)", c.Name, got, c.SHA256)
		}
	}

	// 2. SQLite integrity_check + foreign_key_check on the (decrypted)
	// snapshot.
	sqliteSrc, sqliteCleanup, err := materialize(dir, &m, chunkSQLite, opts.VaultPassphrase)
	if err != nil {
		return fmt.Errorf("verify: materialize sqlite: %w", err)
	}
	if sqliteSrc != "" {
		defer sqliteCleanup()
		if icErr := sqliteIntegrityCheck(sqliteSrc); icErr != nil {
			return fmt.Errorf("verify: sqlite: %w", icErr)
		}
	}

	// 3. Restore badger.backup into a tmp DB and compare AppHash.
	if !opts.SkipAppHash && len(m.AppHash) > 0 {
		badgerSrc, badgerCleanup, materErr := materialize(dir, &m, chunkBadger, opts.VaultPassphrase)
		if materErr != nil {
			return fmt.Errorf("verify: materialize badger: %w", materErr)
		}
		defer badgerCleanup()
		if badgerSrc == "" {
			return errors.New("verify: badger backup missing")
		}
		// The manifest's AppHash was computed under whichever consensus hash
		// rule was in force when the snapshot was taken: legacy (all keys),
		// app-v12 (whole state: prefix excluded), or app-v13 (the three
		// SaveState bookkeeping keys excluded). The manifest carries no rule
		// marker — pre-10.5.1 manifests predate the rules entirely — so the
		// proof accepts a match under ANY rule. Still a strong proof: each
		// candidate is a full-keyspace digest of the restored DB; an
		// adversarial or corrupt backup matches none of them.
		var candidates [][]byte
		if opts.AppHashComputer != nil {
			got, replayErr := replayBadgerAndHash(badgerSrc, opts.AppHashComputer)
			if replayErr != nil {
				return fmt.Errorf("verify: badger replay: %w", replayErr)
			}
			candidates = [][]byte{got}
		} else {
			all, replayErr := replayBadgerAndHash(badgerSrc, computeAppHashAllRulesStandalone)
			if replayErr != nil {
				return fmt.Errorf("verify: badger replay: %w", replayErr)
			}
			candidates = splitConcatenatedHashes(all)
		}
		matched := false
		for _, c := range candidates {
			if bytes.Equal(c, m.AppHash) {
				matched = true
				break
			}
		}
		if !matched {
			return fmt.Errorf("verify: AppHash mismatch under every hash rule (legacy/app-v12/app-v13): want %x", m.AppHash)
		}
	}

	// 4. CometBFT tarball: header-walk to confirm it's not truncated
	// and contains at least blockstore.db. Full untar happens in
	// Restore.
	cometSrc, cometCleanup, err := materialize(dir, &m, chunkCometData, opts.VaultPassphrase)
	if err != nil {
		return fmt.Errorf("verify: materialize cometbft tar: %w", err)
	}
	if cometSrc != "" {
		defer cometCleanup()
		if err := tarHeaderWalk(cometSrc); err != nil {
			return fmt.Errorf("verify: cometbft tar: %w", err)
		}
		if opts.ExpectedCometHeight > 0 {
			if err := verifyCometState(cometSrc, opts.CometDBBackend, opts.ExpectedCometHeight, opts.ExpectedCometAppHash); err != nil {
				return fmt.Errorf("verify: cometbft state provenance: %w", err)
			}
		}
	} else if opts.ExpectedCometHeight > 0 {
		return errors.New("verify: cometbft data missing")
	}

	if opts.RequireRecoveryConfig {
		cfgSrc, cfgCleanup, cfgErr := materialize(dir, &m, chunkConfig, opts.VaultPassphrase)
		if cfgErr != nil {
			return fmt.Errorf("verify: materialize config tar: %w", cfgErr)
		}
		defer cfgCleanup()
		if cfgSrc == "" {
			return errors.New("verify: recovery config archive missing")
		}
		if err := verifyRecoveryConfig(cfgSrc, opts.RequireVaultKey, opts.VaultPassphrase); err != nil {
			return fmt.Errorf("verify: recovery config: %w", err)
		}
	}

	return nil
}

func verifyRecoveryConfig(archivePath string, requireVault bool, vaultPassphrase string) error {
	root, err := os.MkdirTemp("", "sage-verify-config-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(root) }()
	if err := untarZstd(archivePath, root); err != nil {
		return fmt.Errorf("extract: %w", err)
	}
	configDir := filepath.Join(root, "cometbft", "config")
	genesisRaw, err := os.ReadFile(filepath.Join(configDir, "genesis.json"))
	if err != nil {
		return fmt.Errorf("read genesis.json: %w", err)
	}
	genesis, err := cmttypes.GenesisDocFromJSON(genesisRaw)
	if err != nil {
		return fmt.Errorf("parse genesis.json: %w", err)
	}
	nodeKey, err := p2p.LoadNodeKey(filepath.Join(configDir, "node_key.json"))
	if err != nil || nodeKey == nil || nodeKey.PrivKey == nil || nodeKey.ID() == "" {
		return fmt.Errorf("parse node_key.json: %w", errors.Join(err, errors.New("node identity is incomplete")))
	}
	validatorRaw, err := os.ReadFile(filepath.Join(configDir, "priv_validator_key.json"))
	if err != nil {
		return fmt.Errorf("read priv_validator_key.json: %w", err)
	}
	var validatorKey privval.FilePVKey
	if err := cmtjson.Unmarshal(validatorRaw, &validatorKey); err != nil {
		return fmt.Errorf("parse priv_validator_key.json: %w", err)
	}
	if validatorKey.PrivKey == nil {
		return errors.New("priv_validator_key.json has no private key")
	}
	derivedPub := validatorKey.PrivKey.PubKey()
	if derivedPub == nil || (validatorKey.PubKey != nil && !validatorKey.PubKey.Equals(derivedPub)) ||
		(len(validatorKey.Address) > 0 && !bytes.Equal(validatorKey.Address, derivedPub.Address())) {
		return errors.New("priv_validator_key.json identity fields do not match")
	}
	// Do not require membership in the immutable genesis validator list. SAGE
	// supports governed validator-set changes and one-shot state-sync receivers
	// deliberately begin as non-validators. The recovery invariant here is that
	// the private/public/address identity is internally consistent and bootable;
	// the captured Comet state remains the authority for current membership.
	_ = genesis
	if requireVault {
		vaultPath := filepath.Join(root, "vault.key")
		if err := verifyVaultKey(vaultPath, vaultPassphrase); err != nil {
			return fmt.Errorf("open vault.key: %w", err)
		}
	}
	return nil
}

func verifyVaultKey(path, passphrase string) (err error) {
	if passphrase == "" {
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		var envelope struct {
			Salt         []byte `json:"salt"`
			EncryptedKey []byte `json:"encrypted_key"`
			Nonce        []byte `json:"nonce"`
			VerifyHash   []byte `json:"verify_hash"`
		}
		if parseErr := json.Unmarshal(raw, &envelope); parseErr != nil {
			return parseErr
		}
		if len(envelope.Salt) != 16 || len(envelope.Nonce) != 12 ||
			len(envelope.EncryptedKey) != 48 || len(envelope.VerifyHash) != sha256.Size {
			return errors.New("invalid vault key envelope lengths")
		}
		return nil
	}
	// The production loader historically assumed structurally valid nonce and
	// ciphertext lengths and can panic on a corrupt file. Snapshot verification
	// must classify that as an unusable recovery identity, never crash updater.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("invalid vault key structure: %v", recovered)
		}
	}()
	_, err = vault.Open(path, passphrase)
	return err
}

func verifyCometState(archivePath, backend string, expectedHeight int64, expectedAppHash []byte) (err error) {
	if backend == "" {
		return errors.New("CometBFT DB backend is required")
	}
	if expectedHeight <= 0 || len(expectedAppHash) != sha256.Size {
		return errors.New("expected CometBFT height and AppHash are invalid")
	}
	root, err := os.MkdirTemp("", "sage-verify-comet-*")
	if err != nil {
		return err
	}
	defer func() { _ = os.RemoveAll(root) }()
	if extractErr := untarZstd(archivePath, root); extractErr != nil {
		return fmt.Errorf("extract: %w", extractErr)
	}
	// Comet's block store intentionally panics on corrupt encodings. Convert a
	// torn live-file capture into a verification error rather than crashing the
	// updater process.
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("open captured CometBFT databases: %v", recovered)
		}
	}()
	stateDB, err := dbm.NewDB("state", dbm.BackendType(backend), root)
	if err != nil {
		return fmt.Errorf("open state DB: %w", err)
	}
	state, loadErr := cmtstate.NewStore(stateDB, cmtstate.StoreOptions{}).Load()
	stateCloseErr := stateDB.Close()
	if loadErr != nil {
		return fmt.Errorf("load state DB: %w", loadErr)
	}
	if stateCloseErr != nil {
		return fmt.Errorf("close state DB: %w", stateCloseErr)
	}
	if state.LastBlockHeight != expectedHeight || !bytes.Equal(state.AppHash, expectedAppHash) {
		return fmt.Errorf("state tuple is height=%d AppHash=%x, expected height=%d AppHash=%x", state.LastBlockHeight, state.AppHash, expectedHeight, expectedAppHash)
	}
	if !state.LastBlockID.IsComplete() {
		return errors.New("state has an incomplete LastBlockID")
	}
	blockDB, err := dbm.NewDB("blockstore", dbm.BackendType(backend), root)
	if err != nil {
		return fmt.Errorf("open blockstore DB: %w", err)
	}
	blockStore := cmtstore.NewBlockStore(blockDB)
	defer func() { _ = blockStore.Close() }()
	if blockStore.Height() != expectedHeight {
		return fmt.Errorf("blockstore height=%d, expected %d", blockStore.Height(), expectedHeight)
	}
	meta := blockStore.LoadBlockMeta(expectedHeight)
	if meta == nil || !meta.BlockID.Equals(state.LastBlockID) {
		return errors.New("blockstore tip does not match state LastBlockID")
	}
	seenCommit := blockStore.LoadSeenCommit(expectedHeight)
	if seenCommit == nil || seenCommit.Height != expectedHeight || !seenCommit.BlockID.Equals(state.LastBlockID) {
		return errors.New("blockstore tip is missing the matching seen commit")
	}
	if err := seenCommit.ValidateBasic(); err != nil {
		return fmt.Errorf("blockstore tip seen commit is invalid: %w", err)
	}
	return nil
}

func walkTarNames(path string, visit func(string)) error {
	return walkTar(path, func(hdr *tar.Header, tr io.Reader) error {
		name := filepath.ToSlash(filepath.Clean(hdr.Name))
		if strings.HasPrefix(name, "../") || name == ".." || filepath.IsAbs(name) {
			return fmt.Errorf("tar entry escapes root: %q", hdr.Name)
		}
		visit(name)
		_, err := io.Copy(io.Discard, tr)
		return err
	})
}

func walkTar(path string, visit func(*tar.Header, io.Reader) error) error {
	in, err := os.Open(path) //nolint:gosec // verified snapshot-owned path
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	zr, err := zstd.NewReader(in)
	if err != nil {
		return err
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if err := visit(hdr, tr); err != nil {
			return err
		}
	}
}

// materialize returns a filesystem path containing the plaintext bytes
// of the named chunk. For plaintext snapshots that's the chunk file
// itself; for encrypted snapshots it decrypts into a temp file and
// returns a cleanup func.
func materialize(dir string, m *Manifest, base, passphrase string) (string, func(), error) {
	cleanup := func() {}
	// Find the chunk entry; encryption suffix may differ from base.
	var name string
	for _, c := range m.Chunks {
		if c.Name == base || c.Name == base+encryptedSuffix {
			name = c.Name
			break
		}
	}
	if name == "" {
		return "", cleanup, nil // missing chunk is not always fatal
	}
	src := filepath.Join(dir, name)
	if !m.Encrypted {
		return src, cleanup, nil
	}
	tmp, err := os.CreateTemp("", "sage-snapshot-verify-*")
	if err != nil {
		return "", cleanup, err
	}
	tmpPath := tmp.Name()
	cleanup = func() { _ = os.Remove(tmpPath) }
	in, err := os.Open(src) //nolint:gosec // src is dir-derived
	if err != nil {
		_ = tmp.Close()
		cleanup()
		return "", func() {}, err
	}
	defer func() { _ = in.Close() }()
	er, err := newEnvelopeReader(in, passphrase)
	if err != nil {
		_ = tmp.Close()
		cleanup()
		return "", func() {}, err
	}
	if _, err := io.Copy(tmp, er); err != nil {
		_ = tmp.Close()
		cleanup()
		return "", func() {}, err
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return tmpPath, cleanup, nil
}

// isEncryptedChunk reports whether the chunk name has the .enc suffix
// applied by Take. Manifest chunks are stored under their on-disk name
// (with suffix), so encrypted-only chunks have it.
func isEncryptedChunk(name string) bool {
	return len(name) > len(encryptedSuffix) && name[len(name)-len(encryptedSuffix):] == encryptedSuffix
}

func sqliteIntegrityCheck(path string) error {
	dsn := path + "?_busy_timeout=15000&mode=ro"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer func() { _ = db.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var res string
	if qErr := db.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&res); qErr != nil {
		return fmt.Errorf("integrity_check: %w", qErr)
	}
	if res != "ok" {
		return fmt.Errorf("integrity_check returned %q", res)
	}
	// foreign_key_check returns zero rows on success.
	rows, err := db.QueryContext(ctx, "PRAGMA foreign_key_check")
	if err != nil {
		return fmt.Errorf("foreign_key_check: %w", err)
	}
	defer func() { _ = rows.Close() }()
	if rows.Next() {
		return errors.New("foreign_key_check returned violations")
	}
	return nil
}

// replayBadgerAndHash loads a badger.backup file into a fresh DB under
// t.TempDir()-style directory and hashes the result. The DB is closed
// and removed before return.
func replayBadgerAndHash(backupPath string, hasher AppHashComputer) ([]byte, error) {
	tmp, err := os.MkdirTemp("", "sage-snapshot-verify-badger-*")
	if err != nil {
		return nil, err
	}
	defer func() { _ = os.RemoveAll(tmp) }()

	opts := badger.DefaultOptions(tmp)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open tmp badger: %w", err)
	}
	in, err := os.Open(backupPath) //nolint:gosec // backupPath is dir-derived
	if err != nil {
		_ = db.Close()
		return nil, err
	}
	defer func() { _ = in.Close() }()
	if loadErr := db.Load(in, 16); loadErr != nil {
		_ = db.Close()
		return nil, fmt.Errorf("load backup: %w", loadErr)
	}
	if closeErr := db.Close(); closeErr != nil {
		return nil, fmt.Errorf("close tmp badger: %w", closeErr)
	}
	return hasher(tmp)
}

// computeAppHashStandalone walks a freshly-opened BadgerDB and emits
// the same hash as internal/store.BadgerStore.ComputeAppHash. Kept here
// (rather than reaching into internal/store) so this package builds
// without a circular dependency. The integration commit can swap in
// the canonical implementation via DefaultAppHashComputer.
func computeAppHashStandalone(badgerPath string) ([]byte, error) {
	opts := badger.DefaultOptions(badgerPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	defer func() { _ = db.Close() }()

	type kv struct{ key, val []byte }
	var entries []kv
	err = db.View(func(txn *badger.Txn) error {
		_, markerErr := txn.Get([]byte(consensuskeys.AppV23MigrationState))
		appV23Active := markerErr == nil
		if markerErr != nil && !errors.Is(markerErr, badger.ErrKeyNotFound) {
			return markerErr
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if consensuskeys.IsAppHashExcludedLocalKey(item.Key()) &&
				(!appV23Active || !consensuskeys.IsAppV23MigrationStageKey(item.Key())) {
				continue
			}
			k := append([]byte(nil), item.Key()...)
			if vErr := item.Value(func(v []byte) error {
				val := append([]byte(nil), v...)
				entries = append(entries, kv{key: k, val: val})
				return nil
			}); vErr != nil {
				return vErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Sorted by key for determinism — BadgerDB iterates sorted by
	// default, but store.ComputeAppHash sorts explicitly so we mirror.
	// (no-op here because BadgerDB iteration is already sorted)
	h := sha256.New()
	for _, e := range entries {
		h.Write(e.key)
		h.Write(e.val)
	}
	return h.Sum(nil), nil
}

// appHashBookkeepingKeys mirrors internal/store's app-v13 exclusion set: the
// three SaveState bookkeeping keys. Kept in sync by
// TestStandaloneHashesMatchStore (the same parity pin computeAppHashStandalone
// has against ComputeAppHash).
var appHashBookkeepingKeys = [][]byte{
	[]byte("state:height"),
	[]byte("state:app_hash"),
	[]byte("state:epoch"),
}

// computeAppHashAllRulesStandalone walks the restored BadgerDB ONCE and emits
// the digest under each consensus hash-rule era, concatenated (3 × 32 bytes):
// legacy (all keys — pre-app-v12), app-v12 (whole "state:" prefix excluded —
// the superseded broad rule), app-v13 (the three bookkeeping keys excluded).
// Shaped as an AppHashComputer so it rides replayBadgerAndHash's tmp-restore
// plumbing; Verify splits the concatenation and accepts a match on any rule.
func computeAppHashAllRulesStandalone(badgerPath string) ([]byte, error) {
	opts := badger.DefaultOptions(badgerPath)
	opts.Logger = nil
	db, err := badger.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("open badger: %w", err)
	}
	defer func() { _ = db.Close() }()

	legacy, v12, v13 := sha256.New(), sha256.New(), sha256.New()
	statePrefix := []byte("state:")
	err = db.View(func(txn *badger.Txn) error {
		_, markerErr := txn.Get([]byte(consensuskeys.AppV23MigrationState))
		appV23Active := markerErr == nil
		if markerErr != nil && !errors.Is(markerErr, badger.ErrKeyNotFound) {
			return markerErr
		}
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			if consensuskeys.IsAppHashExcludedLocalKey(item.Key()) &&
				(!appV23Active || !consensuskeys.IsAppV23MigrationStageKey(item.Key())) {
				continue
			}
			k := append([]byte(nil), item.Key()...)
			if vErr := item.Value(func(v []byte) error {
				legacy.Write(k)
				legacy.Write(v)
				if !bytes.HasPrefix(k, statePrefix) {
					v12.Write(k)
					v12.Write(v)
				}
				bookkeeping := false
				for _, bk := range appHashBookkeepingKeys {
					if bytes.Equal(k, bk) {
						bookkeeping = true
						break
					}
				}
				if !bookkeeping {
					v13.Write(k)
					v13.Write(v)
				}
				return nil
			}); vErr != nil {
				return vErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	out := legacy.Sum(nil)
	out = append(out, v12.Sum(nil)...)
	out = append(out, v13.Sum(nil)...)
	return out, nil
}

// splitConcatenatedHashes splits the 3×32-byte output of
// computeAppHashAllRulesStandalone back into individual candidates.
func splitConcatenatedHashes(all []byte) [][]byte {
	var out [][]byte
	for i := 0; i+sha256.Size <= len(all); i += sha256.Size {
		out = append(out, all[i:i+sha256.Size])
	}
	return out
}

// tarHeaderWalk decompresses+walks a tar.zst, returning an error only
// on truncation/corruption. Each file's header is consumed but its
// body is discarded.
func tarHeaderWalk(path string) error {
	in, err := os.Open(path) //nolint:gosec // path is dir-derived
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	zr, err := zstd.NewReader(in)
	if err != nil {
		return fmt.Errorf("zstd: %w", err)
	}
	defer zr.Close()
	tr := tar.NewReader(zr)
	count := 0
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("tar header at entry %d: %w", count, err)
		}
		// G110 mitigation: cap inflation per tar entry. 10 GiB is far
		// above any realistic SAGE chunk and below "infinite". If a
		// crafted entry tries to expand past this we abort.
		const maxTarEntryBytes = int64(10) << 30
		if _, err := io.CopyN(io.Discard, tr, maxTarEntryBytes); err != nil && err != io.EOF {
			return fmt.Errorf("tar body %q: %w", hdr.Name, err)
		}
		count++
	}
	if count == 0 {
		return errors.New("tar contains no entries")
	}
	return nil
}

// HashChunk hashes a single file and returns its hex SHA-256. Exposed
// for callers that want to recompute a chunk's hash outside Verify.
func HashChunk(path string) (string, error) {
	f, err := os.Open(path) //nolint:gosec // caller-supplied path
	if err != nil {
		return "", err
	}
	defer func() { _ = f.Close() }()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
