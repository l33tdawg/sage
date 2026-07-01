package federation

import (
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"crypto/x509"
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

// remoteCAPath is where the out-of-band-provisioned CA certificate for a
// remote chain lives. Callers must ValidateChainID first.
func (m *Manager) remoteCAPath(remoteChainID string) string {
	return filepath.Join(m.certsDir, "federation", remoteChainID, tlsca.CACertFile)
}

// StoreRemoteCA parses a PEM CA certificate, persists it under the federation
// certs directory for remoteChainID, and returns its SPKI pin — the value the
// operator submits on-chain as CrossFedTerms.PeerPubKey. Part of the JOIN
// ceremony (exchange CA + endpoint out-of-band, then set terms).
func (m *Manager) StoreRemoteCA(remoteChainID string, caPEM []byte) ([]byte, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return nil, err
	}
	cert, err := parseCACertPEM(caPEM)
	if err != nil {
		return nil, err
	}
	dir := filepath.Dir(m.remoteCAPath(remoteChainID))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create federation certs dir: %w", err)
	}
	if err := os.WriteFile(m.remoteCAPath(remoteChainID), caPEM, 0o600); err != nil {
		return nil, fmt.Errorf("write remote CA: %w", err)
	}
	return SPKIFingerprint(cert), nil
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

// loadPinnedRemoteCA loads the on-disk CA for remoteChainID and verifies its
// SPKI fingerprint against the agreement's on-chain pin. Fail-closed: missing
// file, parse failure, or pin mismatch all deny.
func (m *Manager) loadPinnedRemoteCA(remoteChainID string, expectedPin []byte) (*x509.Certificate, error) {
	if err := ValidateChainID(remoteChainID); err != nil {
		return nil, err
	}
	if len(expectedPin) != sha256.Size {
		return nil, fmt.Errorf("agreement for %s: pinned key is not a 32-byte SPKI fingerprint", remoteChainID)
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
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS13,
		ClientAuth:   tls.RequireAnyClientCert,
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return m.verifyFederationClientCert(rawCerts)
		},
	}, nil
}

// verifyFederationClientCert accepts a client chain iff it verifies against
// the pin-checked CA of at least one active agreement. Fail-closed on every
// path; the error is deliberately generic (handshake errors leak to strangers).
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
	return errors.New("federation: client certificate matches no active agreement")
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
		Certificates:       []tls.Certificate{cert},
		MinVersion:         tls.VersionTLS13,
		InsecureSkipVerify: true, // #nosec G402 -- verification happens in VerifyPeerCertificate against the pinned per-agreement CA; hostname matching is intentionally replaced by the SPKI pin (loopback-SAN node certs)
		VerifyPeerCertificate: func(rawCerts [][]byte, _ [][]*x509.Certificate) error {
			return verifyChainAgainstCA(rawCerts, ca, x509.ExtKeyUsageServerAuth)
		},
	}, nil
}
