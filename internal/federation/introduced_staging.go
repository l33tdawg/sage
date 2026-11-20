package federation

// Restart-safe staging for introduced links (RFC #443 Phase B2, design §3.3
// step 2).
//
// The manual JOIN stages the peer CA in a random temp file that only an
// in-memory closure knows, and the pair seed in a fixed per-chain .pending
// file. Neither survives a restart, so an introduced link stages both under a
// per-link directory:
//
//	<certsDir>/federation/<chain>/intro/<link_id>/ca.crt
//	<certsDir>/federation/<chain>/intro/<link_id>/seed.json
//
// staged_ref on the introduced_link row is "intro/<chain>/<link_id>". The
// reference is never used as a path: it must equal the reference recomputed
// from the row's own chain and link id.
//
// commit moves the CA to the live ca.crt and the seed to the live
// totp.seed.json (keeping any current seed as the previous epoch), exactly as
// the JOIN path does once its tx-33 is on chain. It is idempotent: a commit
// that finds the staged files gone accepts a live CA and seed that already
// match the expected pin and pin-pair, so the reconciler can resume a bind
// interrupted half-way.

import (
	"bytes"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/l33tdawg/sage/internal/tlsca"
)

var introLinkIDPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// ErrIntroStagedMissing means a staged artifact is gone and the live file does
// not already hold the expected value.
var ErrIntroStagedMissing = errors.New("introduced link: staged artifact missing or corrupt")

func introStagedRef(remoteChainID, linkID string) string {
	return "intro/" + remoteChainID + "/" + linkID
}

func (m *Manager) introStageDir(remoteChainID, linkID string) (string, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return "", err
	}
	if !introLinkIDPattern.MatchString(linkID) {
		return "", errors.New("introduced link: bad link id")
	}
	return filepath.Join(m.certsDir, "federation", remoteChainID, "intro", linkID), nil
}

func (m *Manager) introStageDirForRef(remoteChainID, linkID, ref string) (string, error) {
	if ref != introStagedRef(remoteChainID, linkID) {
		return "", errors.New("introduced link: staged_ref does not match the row")
	}
	return m.introStageDir(remoteChainID, linkID)
}

// stageIntroArtifacts writes the peer CA and the pair seed under the link's
// staging directory and returns the staged reference and the CA SPKI pin.
// Nothing live changes.
func (m *Manager) stageIntroArtifacts(remoteChainID, linkID string, caPEM, seed, boundPinPair []byte, createdAt int64) (string, []byte, error) {
	dir, err := m.introStageDir(remoteChainID, linkID)
	if err != nil {
		return "", nil, err
	}
	cert, err := parseCACertPEM(caPEM)
	if err != nil {
		return "", nil, err
	}
	if cnErr := requireChainCN(cert, remoteChainID); cnErr != nil {
		return "", nil, cnErr
	}
	if len(seed) < 16 || len(boundPinPair) != 32 {
		return "", nil, errors.New("stage introduced link: bad seed/pin-pair length")
	}
	env := TOTPSeedEnvelope{
		V: 1, SeedEstablished: true, BoundPinPair: append([]byte(nil), boundPinPair...),
		ChainID: remoteChainID, CreatedAt: createdAt,
	}
	if pass := m.getVaultPassphrase(); pass != "" {
		wrapped, wErr := tlsca.EncryptCAKey(string(seed), pass)
		if wErr != nil {
			return "", nil, fmt.Errorf("wrap seed: %w", wErr)
		}
		env.Encrypted, env.Body = true, wrapped
	} else {
		env.Body = encodeB64(seed)
	}
	blob, err := json.Marshal(&env)
	if err != nil {
		return "", nil, fmt.Errorf("marshal staged seed: %w", err)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", nil, fmt.Errorf("create introduced staging dir: %w", err)
	}
	if err := writeFileSync(filepath.Join(dir, tlsca.CACertFile), caPEM); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	if err := writeFileSync(filepath.Join(dir, "seed.json"), blob); err != nil {
		_ = os.RemoveAll(dir)
		return "", nil, err
	}
	return introStagedRef(remoteChainID, linkID), SPKIFingerprint(cert), nil
}

func writeFileSync(path string, b []byte) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600) // #nosec G304 -- validated components
	if err != nil {
		return fmt.Errorf("stage %s: %w", filepath.Base(path), err)
	}
	if _, err := f.Write(b); err != nil {
		_ = f.Close()
		return fmt.Errorf("stage %s: %w", filepath.Base(path), err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("stage %s: %w", filepath.Base(path), err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("stage %s: %w", filepath.Base(path), err)
	}
	return nil
}

// commitIntroArtifacts makes the staged CA and seed live. expectPin is the CA
// SPKI pin the agreement commits; expectPinPair is the seed's pin binding.
func (m *Manager) commitIntroArtifacts(remoteChainID, linkID, ref string, expectPin, expectPinPair []byte) error {
	dir, err := m.introStageDirForRef(remoteChainID, linkID, ref)
	if err != nil {
		return err
	}
	if err := m.commitIntroCA(remoteChainID, filepath.Join(dir, tlsca.CACertFile), expectPin); err != nil {
		return err
	}
	if err := m.commitIntroSeed(remoteChainID, filepath.Join(dir, "seed.json"), expectPinPair); err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("clear introduced staging dir: %w", err)
	}
	return nil
}

func (m *Manager) commitIntroCA(remoteChainID, staged string, expectPin []byte) error {
	final := m.remoteCAPath(remoteChainID)
	b, err := os.ReadFile(staged) // #nosec G304 -- validated components
	if errors.Is(err, os.ErrNotExist) {
		// Resumed after the rename: accept only the expected live CA.
		if live, lErr := os.ReadFile(final); lErr == nil { // #nosec G304
			if cert, pErr := parseCACertPEM(live); pErr == nil && subtle.ConstantTimeCompare(SPKIFingerprint(cert), expectPin) == 1 {
				return nil
			}
		}
		return ErrIntroStagedMissing
	}
	if err != nil {
		return fmt.Errorf("read staged CA: %w", err)
	}
	cert, err := parseCACertPEM(b)
	if err != nil || requireChainCN(cert, remoteChainID) != nil || subtle.ConstantTimeCompare(SPKIFingerprint(cert), expectPin) != 1 {
		return ErrIntroStagedMissing
	}
	if err := os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return fmt.Errorf("create federation certs dir: %w", err)
	}
	if err := os.Rename(staged, final); err != nil {
		return fmt.Errorf("commit remote CA: %w", err)
	}
	m.invalidateCACache(remoteChainID)
	return nil
}

func (m *Manager) commitIntroSeed(remoteChainID, staged string, expectPinPair []byte) error {
	final := m.seedPath(remoteChainID)
	b, err := os.ReadFile(staged) // #nosec G304 -- validated components
	if errors.Is(err, os.ErrNotExist) {
		if env, hErr := m.readSeedHeader(remoteChainID); hErr == nil && env.SeedEstablished &&
			bytes.Equal(env.BoundPinPair, expectPinPair) {
			return nil
		}
		return ErrIntroStagedMissing
	}
	if err != nil {
		return fmt.Errorf("read staged seed: %w", err)
	}
	var env TOTPSeedEnvelope
	if decErr := json.Unmarshal(b, &env); decErr != nil || env.ChainID != remoteChainID ||
		!env.SeedEstablished || !bytes.Equal(env.BoundPinPair, expectPinPair) {
		return ErrIntroStagedMissing
	}
	// The epoch follows whatever is live at commit time, not at staging.
	env.Epoch = 1
	if prev, hErr := m.readSeedHeader(remoteChainID); hErr == nil {
		env.Epoch = prev.Epoch + 1
	}
	blob, err := json.Marshal(&env)
	if err != nil {
		return fmt.Errorf("marshal seed envelope: %w", err)
	}
	pending := final + ".intro.pending"
	if err := writeFileSync(pending, blob); err != nil {
		return err
	}
	if _, sErr := os.Stat(final); sErr == nil {
		_ = os.Rename(final, m.seedPrevPath(remoteChainID))
	}
	if err := os.Rename(pending, final); err != nil {
		_ = os.Remove(pending)
		return fmt.Errorf("commit seed: %w", err)
	}
	if err := os.Remove(staged); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("clear staged seed: %w", err)
	}
	if seed, ok := m.loadSeedFromDisk(final); ok {
		m.cacheSeed(remoteChainID, seed) // vault-locked: loaded at unlock instead
	}
	return nil
}

// rollbackIntroArtifacts removes the link's staging directory. It never
// touches live files.
func (m *Manager) rollbackIntroArtifacts(remoteChainID, linkID, ref string) error {
	dir, err := m.introStageDirForRef(remoteChainID, linkID, ref)
	if err != nil {
		return err
	}
	if err := os.RemoveAll(dir); err != nil {
		return fmt.Errorf("roll back introduced staging: %w", err)
	}
	return nil
}
