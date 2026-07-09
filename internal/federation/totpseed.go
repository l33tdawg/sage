package federation

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/l33tdawg/sage/internal/tlsca"
)

func encodeB64(b []byte) string          { return base64.StdEncoding.EncodeToString(b) }
func decodeB64(s string) ([]byte, error) { return base64.StdEncoding.DecodeString(s) }

// SetVaultPassphrase wires the vault passphrase so seeds are wrapped at rest
// (Argon2id+AES-GCM) — a strictly stronger protection domain than agent.key.
// Empty (default) uses the 0600-plaintext floor. Must be set before
// LoadSeedsIntoCache. Also (re)loads any established seeds into the cache.
func (m *Manager) SetVaultPassphrase(pass string) int {
	m.seedMu.Lock()
	m.vaultPassphrase = pass
	m.clearSeedCacheLocked()
	m.seedMu.Unlock()
	return m.LoadSeedsIntoCache()
}

func (m *Manager) getVaultPassphrase() string {
	m.seedMu.RLock()
	defer m.seedMu.RUnlock()
	return m.vaultPassphrase
}

func (m *Manager) clearSeedCacheLocked() {
	if m.seedCache == nil {
		m.seedCache = make(map[string][][]byte)
		return
	}
	for chain, candidates := range m.seedCache {
		for _, seed := range candidates {
			zeroize(seed)
		}
		delete(m.seedCache, chain)
	}
}

// Per-agreement TOTP seed lifecycle (v11 join ceremony, §2.8). Node-local,
// off-consensus, co-located with the pinned CA so the seed tracks the CA's
// lifecycle. The envelope's seed_established / epoch / bound_pin_pair are
// PLAINTEXT header fields — readable while the body is vault-locked — so the
// fail-closed v3 version gate can know v3 is required without ever running a KDF.

// TOTPSeedEnvelope is the on-disk seed record. When Encrypted, Body is
// Argon2id+AES-256-GCM (via tlsca manifest crypto) over the raw seed; otherwise
// Body is the raw seed at the 0600-plaintext floor.
type TOTPSeedEnvelope struct {
	V               int    `json:"v"`
	Epoch           uint32 `json:"epoch"`
	SeedEstablished bool   `json:"seed_established"`
	BoundPinPair    []byte `json:"bound_pin_pair"` // 32B, plaintext header (RT-10 binding record)
	ChainID         string `json:"chain_id"`
	CreatedAt       int64  `json:"created_at"`
	Encrypted       bool   `json:"encrypted"`
	Body            string `json:"body"` // wrapped envelope string (Encrypted) OR base64 raw seed
}

func (m *Manager) seedPath(remoteChainID string) string {
	return filepath.Join(m.certsDir, "federation", remoteChainID, "totp.seed.json")
}
func (m *Manager) seedPrevPath(remoteChainID string) string {
	return filepath.Join(m.certsDir, "federation", remoteChainID, "totp.seed.prev.json")
}

// readSeedHeader reads ONLY the plaintext header (seed_established/epoch/pin
// binding) — never decrypts. Used by the fail-closed v3 gate (§2.6.3).
func (m *Manager) readSeedHeader(remoteChainID string) (*TOTPSeedEnvelope, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return nil, err
	}
	b, err := os.ReadFile(m.seedPath(remoteChainID)) // #nosec G304 -- path components validated
	if err != nil {
		return nil, err
	}
	var env TOTPSeedEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, fmt.Errorf("decode seed envelope: %w", err)
	}
	return &env, nil
}

// seedEstablished reports whether an active seed record exists for a chain
// (readable while vault-locked). Drives the fail-closed v3 gate.
//
// A genuinely absent file means no seed was ever established (the v2 path is
// correct). But any OTHER error (disk I/O, corrupt JSON) is AMBIGUOUS: we cannot
// prove no seed exists, so we must fail CLOSED - report established=true so the
// gate demands v3 (or 503s when the seed is not cached) instead of silently
// downgrading to the weaker v2 factor.
func (m *Manager) seedEstablished(remoteChainID string) bool {
	env, err := m.readSeedHeader(remoteChainID)
	if err != nil {
		return !errors.Is(err, os.ErrNotExist)
	}
	return env.SeedEstablished
}

// stageSeedFn returns a commit/rollback pair that persists a seed ONLY after the
// caller's on-chain authz (tx-33) succeeds — mirroring StageRemoteCA. The seed
// lands (and seed_established flips true) only when the CA lands. commit does an
// ATOMIC rename overwrite (a per-pair seed IS re-keyable, unlike a vault master
// key — refuse-overwrite would strand the two sides on different seeds after a
// rotation), keeping any existing file as the previous epoch for cutover.
func (m *Manager) stageSeed(remoteChainID string, seed []byte, boundPinPair []byte, createdAt int64) (commit func() error, rollback func(), err error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return nil, nil, err
	}
	if len(seed) < 16 || len(boundPinPair) != 32 {
		return nil, nil, fmt.Errorf("stage seed: bad seed/pin length")
	}
	dir := filepath.Dir(m.seedPath(remoteChainID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, nil, fmt.Errorf("create federation certs dir: %w", err)
	}

	// Determine the next epoch from any existing record.
	epoch := uint32(1)
	if prev, hErr := m.readSeedHeader(remoteChainID); hErr == nil {
		epoch = prev.Epoch + 1
	}

	env := TOTPSeedEnvelope{
		V: 1, Epoch: epoch, SeedEstablished: true,
		BoundPinPair: append([]byte(nil), boundPinPair...),
		ChainID:      remoteChainID, CreatedAt: createdAt,
	}
	pass := m.getVaultPassphrase()
	if pass != "" {
		wrapped, wErr := tlsca.EncryptCAKey(string(seed), pass)
		if wErr != nil {
			return nil, nil, fmt.Errorf("wrap seed: %w", wErr)
		}
		env.Encrypted = true
		env.Body = wrapped
	} else {
		env.Encrypted = false
		env.Body = encodeB64(seed)
	}
	blob, mErr := json.Marshal(&env)
	if mErr != nil {
		return nil, nil, fmt.Errorf("marshal seed envelope: %w", mErr)
	}

	pending := m.seedPath(remoteChainID) + ".pending"
	if wErr := os.WriteFile(pending, blob, 0o600); wErr != nil {
		return nil, nil, fmt.Errorf("write pending seed: %w", wErr)
	}
	commit = func() error {
		final := m.seedPath(remoteChainID)
		// Preserve any current epoch as .prev for the cutover window.
		if _, sErr := os.Stat(final); sErr == nil {
			_ = os.Rename(final, m.seedPrevPath(remoteChainID))
		}
		if rErr := os.Rename(pending, final); rErr != nil {
			_ = os.Remove(pending)
			return fmt.Errorf("commit seed: %w", rErr)
		}
		m.cacheSeed(remoteChainID, seed) // unlock into cache
		return nil
	}
	rollback = func() { _ = os.Remove(pending) }
	return commit, rollback, nil
}

// LoadSeedsIntoCache decrypts every established seed into the in-memory cache
// once at node unlock (never on the per-request auth path — §2.6.3 DoS note).
// Called after the vault passphrase is available. Returns the count loaded.
func (m *Manager) LoadSeedsIntoCache() int {
	agreements := m.ActiveAgreements()
	n := 0
	for i := range agreements {
		chain := agreements[i].RemoteChainID
		if seed, ok := m.loadSeedFromDisk(m.seedPath(chain)); ok {
			m.cacheSeed(chain, seed)
			// Also load the previous-epoch seed as a cutover candidate.
			if prev, pok := m.loadSeedFromDisk(m.seedPrevPath(chain)); pok {
				m.appendSeedCandidate(chain, prev)
			}
			n++
		}
	}
	return n
}

func (m *Manager) loadSeedFromDisk(path string) ([]byte, bool) {
	b, err := os.ReadFile(path) // #nosec G304 -- path from validated chain id
	if err != nil {
		return nil, false
	}
	var env TOTPSeedEnvelope
	if err := json.Unmarshal(b, &env); err != nil {
		return nil, false
	}
	if env.Encrypted {
		pass := m.getVaultPassphrase()
		if pass == "" {
			return nil, false // locked
		}
		plain, dErr := tlsca.DecryptCAKeyBytes(env.Body, pass)
		if dErr != nil {
			return nil, false
		}
		// []byte (not string) so the caller's cache-purge can zeroize it.
		return plain, true
	}
	seed, dErr := decodeB64(env.Body)
	if dErr != nil {
		return nil, false
	}
	return seed, true
}

// cacheSeed sets the current seed as the sole candidate for a chain.
func (m *Manager) cacheSeed(remoteChainID string, seed []byte) {
	m.seedMu.Lock()
	defer m.seedMu.Unlock()
	if m.seedCache == nil {
		m.seedCache = make(map[string][][]byte)
	}
	m.seedCache[remoteChainID] = [][]byte{append([]byte(nil), seed...)}
}

// appendSeedCandidate adds a previous-epoch seed as an additional verify
// candidate during a rotation cutover window.
func (m *Manager) appendSeedCandidate(remoteChainID string, seed []byte) {
	m.seedMu.Lock()
	defer m.seedMu.Unlock()
	if m.seedCache == nil {
		m.seedCache = make(map[string][][]byte)
	}
	m.seedCache[remoteChainID] = append(m.seedCache[remoteChainID], append([]byte(nil), seed...))
}

// seedCandidates returns the in-memory seed candidates (current + cutover
// previous) for a chain, or nil if locked/absent.
func (m *Manager) seedCandidates(remoteChainID string) [][]byte {
	m.seedMu.RLock()
	defer m.seedMu.RUnlock()
	return m.seedCache[remoteChainID]
}

// currentSeed returns the primary (current-epoch) seed for outbound signing.
func (m *Manager) currentSeed(remoteChainID string) ([]byte, bool) {
	m.seedMu.RLock()
	defer m.seedMu.RUnlock()
	c := m.seedCache[remoteChainID]
	if len(c) == 0 {
		return nil, false
	}
	return c[0], true
}

// purgeSeed zeroizes the cache entry and deletes the on-disk seed files for a
// chain (revoke / expiry / abort / RT-8 replace). Because the fail-closed gate
// reads seed_established, this relaxes the peer back to v2-accepted for a future
// re-add — and, being tied to the non-active state, never strands a servable
// agreement.
func (m *Manager) purgeSeed(remoteChainID string) {
	m.seedMu.Lock()
	if c, ok := m.seedCache[remoteChainID]; ok {
		for _, s := range c {
			for i := range s {
				s[i] = 0
			}
		}
		delete(m.seedCache, remoteChainID)
	}
	m.seedMu.Unlock()
	if ValidateChainID(remoteChainID) == nil {
		_ = os.Remove(m.seedPath(remoteChainID))
		_ = os.Remove(m.seedPrevPath(remoteChainID))
	}
}
