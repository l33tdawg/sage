package federation

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"

	"github.com/l33tdawg/sage/internal/tlsca"
)

// Trust model (plan §4, Phase-1 LAN trust ladder):
//
//   - Each cross_fed agreement pins the REMOTE CHAIN'S CA by SPKI fingerprint:
//     the on-chain PeerPubKey field holds sha256(SubjectPublicKeyInfo) of the
//     remote CA certificate (32 bytes — same width as an ed25519 key, but it is
//     a PIN, not a key). Pinning the CA rather than a leaf survives the yearly
//     node-cert rotation (tlsca nodeValidityYears=1) while still binding the
//     agreement to exactly one issuing authority.
//   - The remote CA CERTIFICATE itself is provisioned out-of-band during the
//     federation-JOIN ceremony and stored under
//     <certsDir>/federation/<remote_chain_id>/ca.crt. The on-chain pin makes
//     the disk file tamper-evident: a swapped CA fails the pin and everything
//     fails closed.
//   - Hostname verification is deliberately replaced by pin verification on
//     BOTH directions: node certs carry only loopback SANs by default, and a
//     per-agreement pinned CA that has only ever signed the peer's node certs
//     is a strictly narrower trust statement than any hostname match.

// chainIDPattern is the allowed shape of a remote chain id wherever it is used
// as a path component (remote CA directory) — minted ids are
// <prefix>-<base32>, legacy ids are "sage-personal"/"sage-quorum". Rejecting
// everything else (path separators, "..", uppercase) closes path traversal
// through operator-supplied ids.
var chainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

// ValidateChainID rejects chain ids that could not have been minted by SAGE or
// that would be unsafe as a filesystem path component.
func ValidateChainID(chainID string) error {
	if !chainIDPattern.MatchString(chainID) {
		return fmt.Errorf("invalid chain id %q", chainID)
	}
	if chainID == "." || chainID == ".." {
		return fmt.Errorf("invalid chain id %q", chainID)
	}
	return nil
}

// SPKIFingerprint returns sha256(SubjectPublicKeyInfo) — the RFC 7469-style
// public-key pin of a certificate.
func SPKIFingerprint(cert *x509.Certificate) []byte {
	sum := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
	return sum[:]
}

// ownPin returns this node's OWN authoritative CA SPKI fingerprint (the pin the
// PEER holds for us). Read from <certsDir>/ca.crt and cached. Load-bearing for
// RT-10: this is the node's own authoritative pin, never a scanned/echoed value.
func (m *Manager) ownPin() ([]byte, error) {
	m.ownPinMu.Lock()
	defer m.ownPinMu.Unlock()
	if m.ownPinCache != nil {
		return m.ownPinCache, nil
	}
	cert, err := tlsca.ReadCert(filepath.Join(m.certsDir, tlsca.CACertFile))
	if err != nil {
		return nil, fmt.Errorf("read own CA for pin: %w", err)
	}
	m.ownPinCache = SPKIFingerprint(cert)
	return m.ownPinCache, nil
}

// remoteCAPath is where the out-of-band-provisioned CA certificate for a
// remote chain lives. Callers must ValidateChainID first.
func (m *Manager) remoteCAPath(remoteChainID string) string {
	return filepath.Join(m.certsDir, "federation", remoteChainID, tlsca.CACertFile)
}

// caCNPrefix is the CommonName prefix every SAGE-minted CA carries
// (tlsca.GenerateCA sets CN = "sage-ca-<chainID>"). Binding the CN to the
// claimed chain id closes the shared-CA impersonation gap: even if an operator
// (mis)provisions one CA under two agreements, that CA's CN can name only ONE
// chain, so it authenticates only that chain.
const caCNPrefix = "sage-ca-"

// StageRemoteCA parses+validates a PEM CA for remoteChainID and writes it to a
// PENDING sidecar file (never the live path), returning its SPKI pin plus
// commit/rollback closures. The caller broadcasts the terms tx FIRST (on-chain
// authz) and only then commits — so an UNAUTHORIZED set can never overwrite an
// existing agreement's live CA on disk (the confused-deputy availability kill).
// commit atomically renames pending→live and drops the CA cache entry; rollback
// removes the pending file.
func (m *Manager) StageRemoteCA(remoteChainID string, caPEM []byte) (pin []byte, commit func() error, rollback func(), err error) {
	if err = ValidateChainID(remoteChainID); err != nil {
		return nil, nil, nil, err
	}
	cert, err := parseCACertPEM(caPEM)
	if err != nil {
		return nil, nil, nil, err
	}
	if err = requireChainCN(cert, remoteChainID); err != nil {
		return nil, nil, nil, err
	}
	final := m.remoteCAPath(remoteChainID)
	if err = os.MkdirAll(filepath.Dir(final), 0o700); err != nil {
		return nil, nil, nil, fmt.Errorf("create federation certs dir: %w", err)
	}
	// UNIQUE pending file per stage (os.CreateTemp, mode 0600). A fixed
	// "<final>.pending" path would be SHARED across concurrent sets for the same
	// chain, so one caller's rollback (os.Remove) could delete another's staged
	// CA mid-join — a griefing availability race triggerable by any
	// authenticated agent (authz is on-chain, reached only after broadcast). A
	// per-request temp file isolates commit/rollback completely.
	f, err := os.CreateTemp(filepath.Dir(final), "ca-*.pending")
	if err != nil {
		return nil, nil, nil, fmt.Errorf("stage remote CA: %w", err)
	}
	pending := f.Name()
	if _, wErr := f.Write(caPEM); wErr != nil {
		_ = f.Close()
		_ = os.Remove(pending)
		return nil, nil, nil, fmt.Errorf("write pending remote CA: %w", wErr)
	}
	if cErr := f.Close(); cErr != nil {
		_ = os.Remove(pending)
		return nil, nil, nil, fmt.Errorf("finalize pending remote CA: %w", cErr)
	}
	commit = func() error {
		if renErr := os.Rename(pending, final); renErr != nil {
			_ = os.Remove(pending) // don't leak the temp on a failed commit
			return fmt.Errorf("commit remote CA: %w", renErr)
		}
		m.invalidateCACache(remoteChainID)
		return nil
	}
	rollback = func() { _ = os.Remove(pending) }
	return SPKIFingerprint(cert), commit, rollback, nil
}

// StoreRemoteCA immediately persists a validated remote CA (stage + commit in
// one shot). Used by internal callers and tests; the REST JOIN path uses the
// staged variant so the disk write is gated on on-chain authz.
func (m *Manager) StoreRemoteCA(remoteChainID string, caPEM []byte) ([]byte, error) {
	pin, commit, rollback, err := m.StageRemoteCA(remoteChainID, caPEM)
	if err != nil {
		return nil, err
	}
	if err := commit(); err != nil {
		rollback()
		return nil, err
	}
	return pin, nil
}

// parseCACertPEM decodes the first CERTIFICATE block and requires it to be a CA.
func parseCACertPEM(caPEM []byte) (*x509.Certificate, error) {
	block, _ := pem.Decode(caPEM)
	if block == nil || block.Type != "CERTIFICATE" {
		return nil, errors.New("remote CA: no CERTIFICATE PEM block")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("remote CA: parse: %w", err)
	}
	if !cert.IsCA {
		return nil, errors.New("remote CA: certificate is not a CA")
	}
	return cert, nil
}

// requireChainCN enforces that a CA's CommonName names exactly remoteChainID.
func requireChainCN(cert *x509.Certificate, remoteChainID string) error {
	if cert.Subject.CommonName != caCNPrefix+remoteChainID {
		return fmt.Errorf("remote CA CommonName %q does not name chain %q (expected %s%s)",
			cert.Subject.CommonName, remoteChainID, caCNPrefix, remoteChainID)
	}
	return nil
}

// loadPinnedRemoteCA loads the on-disk CA for remoteChainID and verifies (a) its
// SPKI fingerprint against the agreement's on-chain pin, AND (b) its CommonName
// names the claimed chain. Fail-closed: missing file, parse failure, pin
// mismatch, or CN mismatch all deny. Parsed CAs are cached by (chain, pin) so
// the mTLS handshake / per-request verify don't re-read+parse from disk each
// time (unauthenticated-handshake amplification).
func (m *Manager) loadPinnedRemoteCA(remoteChainID string, expectedPin []byte) (*x509.Certificate, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return nil, err
	}
	if len(expectedPin) != sha256.Size {
		return nil, fmt.Errorf("agreement for %s: pinned key is not a 32-byte SPKI fingerprint", remoteChainID)
	}
	cacheKey := remoteChainID + ":" + hex.EncodeToString(expectedPin)
	if cert := m.cachedCA(cacheKey); cert != nil {
		return cert, nil
	}
	caPEM, err := os.ReadFile(m.remoteCAPath(remoteChainID)) // #nosec G304 -- path components validated
	if err != nil {
		return nil, fmt.Errorf("remote CA for %s not provisioned: %w", remoteChainID, err)
	}
	cert, err := parseCACertPEM(caPEM)
	if err != nil {
		return nil, fmt.Errorf("remote CA for %s: %w", remoteChainID, err)
	}
	if subtle.ConstantTimeCompare(SPKIFingerprint(cert), expectedPin) != 1 {
		return nil, fmt.Errorf("remote CA for %s: SPKI pin mismatch (on-disk CA does not match the on-chain agreement)", remoteChainID)
	}
	if err := requireChainCN(cert, remoteChainID); err != nil {
		return nil, err
	}
	m.putCA(cacheKey, cert)
	return cert, nil
}

// verifyChainAgainstCA verifies a presented raw certificate chain against a
// single pinned CA root for the given key usage. Used on both directions:
// server side (peer client certs, ExtKeyUsageClientAuth) and client side (peer
// server certs, ExtKeyUsageServerAuth — replacing hostname verification with
// pin verification).
func verifyChainAgainstCA(rawCerts [][]byte, ca *x509.Certificate, usage x509.ExtKeyUsage) error {
	if len(rawCerts) == 0 {
		return errors.New("no peer certificate presented")
	}
	leaf, err := x509.ParseCertificate(rawCerts[0])
	if err != nil {
		return fmt.Errorf("parse peer leaf certificate: %w", err)
	}
	intermediates := x509.NewCertPool()
	for _, raw := range rawCerts[1:] {
		if c, parseErr := x509.ParseCertificate(raw); parseErr == nil {
			intermediates.AddCert(c)
		}
	}
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{usage},
	})
	return err
}

// ServerTLSConfig builds the tls.Config for the DEDICATED federation listener.
// Unlike the local API (tlsca.ServerTLSConfig, ClientAuth=NoClientCert), a
// client certificate is REQUIRED and must verify — at handshake time — against
// the pinned CA of at least one active, unexpired agreement. The precise
// binding of the presented cert to the CLAIMED chain id happens per-request in
// peerAuth; the handshake check exists so unauthenticated strangers never
// reach HTTP at all.
//
// ClientAuth is RequireAnyClientCert (not RequireAndVerifyClientCert) because
// verification is per-agreement: there is no single ClientCAs pool — each
// agreement pins its own CA and agreements change at runtime. The custom
// VerifyPeerCertificate below IS the verification, evaluated against live
// agreement state on every handshake.
func (m *Manager) ServerTLSConfig() (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(m.certsDir, tlsca.NodeCertFile),
		filepath.Join(m.certsDir, tlsca.NodeKeyFile),
	)
	if err != nil {
		return nil, fmt.Errorf("load node certificate for federation listener: %w", err)
	}
	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		MinVersion:             tls.VersionTLS13,
		ClientAuth:             tls.RequireAnyClientCert,
		SessionTicketsDisabled: true,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return m.verifyFederationClientCert(rawCerts)
		},
	}, nil
}

// verifyFederationClientCert accepts a client chain iff it verifies against the
// pin-checked CA of at least one active agreement OR it is a guest connecting
// during an in-flight JOIN ceremony (there is no agreement yet during a join).
// Fail-closed on every path; the error is deliberately generic (handshake
// errors leak to strangers).
//
// The join-window acceptance is a COARSE handshake gate only: it lets a scanning
// guest reach the /fed/v1/join/* routes while the host has a session open. The
// REAL gates are per-route - the 64-bit session id from the QR, the assertion
// that the guest CA's SPKI equals the pin the host scanned off the return QR,
// and the per-session TLS-cert binding (RT-2/RT-5). A stranger who reaches the
// join routes without a valid session id and a CA matching a scanned pin gets
// nowhere, and the RT-3 rate-limit + per-session fail cap bound the surface.
func (m *Manager) verifyFederationClientCert(rawCerts [][]byte) error {
	agreements := m.ActiveAgreements()
	for i := range agreements {
		ca, err := m.loadPinnedRemoteCA(agreements[i].RemoteChainID, agreements[i].PeerPubKey)
		if err != nil {
			continue // unprovisioned/pin-mismatched agreement can authenticate nobody
		}
		if verifyChainAgainstCA(rawCerts, ca, x509.ExtKeyUsageClientAuth) == nil {
			return nil
		}
	}
	// A bound guest cert (post-request) verifies against its exact leaf SPKI; an
	// as-yet-unbound guest is accepted only while a host session is genuinely
	// open (deliberately opened by the operator, 15-min TTL). Both close as soon
	// as the ceremony finishes.
	if m.joinWindowAccepts(rawCerts) {
		return nil
	}
	return errors.New("federation: client certificate matches no active agreement")
}

// joinWindowAccepts reports whether an in-flight join session should let this
// client cert through the handshake. Post-bind sessions require the exact leaf
// SPKI; pre-bind sessions (awaiting the first /join/request or serving the CA
// fetch) accept any cert (the per-route pin assertion is the real gate).
func (m *Manager) joinWindowAccepts(rawCerts [][]byte) bool {
	open := m.joins.OpenSessions()
	if len(open) == 0 {
		return false
	}
	var leafSPKI []byte
	if len(rawCerts) > 0 {
		if leaf, err := x509.ParseCertificate(rawCerts[0]); err == nil {
			leafSPKI = SPKIFingerprint(leaf)
		}
	}
	for _, js := range open {
		if len(js.BoundCertSPKI) == 0 {
			return true // pre-bind window (CA fetch / first request)
		}
		if leafSPKI != nil && subtle.ConstantTimeCompare(js.BoundCertSPKI, leafSPKI) == 1 {
			return true // the bound guest re-connecting for status/confirm
		}
	}
	return false
}

// clientTLSConfig builds the outbound tls.Config for dialing one agreement's
// endpoint: present our node cert (client cert), and accept exactly the server
// chains that verify against that agreement's pinned CA. Hostname verification
// is replaced by pin verification (see the trust-model comment above) — hence
// InsecureSkipVerify + a mandatory VerifyPeerCertificate.
func (m *Manager) clientTLSConfig(remoteChainID string, expectedPin []byte) (*tls.Config, error) {
	ca, err := m.loadPinnedRemoteCA(remoteChainID, expectedPin)
	if err != nil {
		return nil, err
	}
	cert, err := tls.LoadX509KeyPair(
		filepath.Join(m.certsDir, tlsca.NodeCertFile),
		filepath.Join(m.certsDir, tlsca.NodeKeyFile),
	)
	if err != nil {
		return nil, fmt.Errorf("load node client certificate: %w", err)
	}
	return &tls.Config{
		Certificates:           []tls.Certificate{cert},
		MinVersion:             tls.VersionTLS13,
		SessionTicketsDisabled: true,
		InsecureSkipVerify:     true, // #nosec G402 -- verification happens in VerifyPeerCertificate against the pinned per-agreement CA; hostname matching is intentionally replaced by the SPKI pin (loopback-SAN node certs)
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyChainAgainstCA(rawCerts, ca, x509.ExtKeyUsageServerAuth)
		},
	}, nil
}
