package shellcontrol

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"net"
	"time"

	"github.com/l33tdawg/sage/internal/nativebootstrap"
	"github.com/l33tdawg/sage/internal/nativeidentity"
)

// EnableNativeBootstrap installs a compiled signing policy once. It is never
// configured by a request or an environment variable. SSCP/1 stays status-only.
func (s *Server) EnableNativeBootstrap(requirement string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !nativeidentity.Supported() || requirement == "" || s.nativeRequirement != "" || s.state == StateDraining || s.state == StateFailed {
		return nativebootstrap.ErrUnavailable
	}
	broker, err := nativebootstrap.New(nativebootstrap.Binding{Generation: s.generation, Origin: s.origin, StartupProof: s.startupProof})
	if err != nil {
		return err
	}
	s.nativeRequirement = requirement
	s.nativeBroker = broker
	return nil
}

func (s *Server) NativeBootstrap() *nativebootstrap.Broker {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.nativeBroker
}

func (s *Server) NativeBinding() nativebootstrap.Binding {
	return nativebootstrap.Binding{Generation: s.generation, Origin: s.origin, StartupProof: s.startupProof}
}

type nativeIssueRequest struct {
	ControlProtocol int     `json:"control_protocol"`
	ShellProtocol   int     `json:"shell_protocol"`
	Operation       string  `json:"operation"`
	Generation      string  `json:"instance_generation"`
	Origin          string  `json:"ui_origin"`
	StartupProof    *string `json:"startup_proof"`
	PublicKey       string  `json:"public_key"`
}

type nativeIssueResponse struct {
	ControlProtocol  int    `json:"control_protocol"`
	Operation        string `json:"operation"`
	Generation       string `json:"instance_generation"`
	Origin           string `json:"ui_origin"`
	StartupProof     string `json:"startup_proof"`
	Ticket           string `json:"ticket"`
	Challenge        string `json:"challenge"`
	PublicKey        string `json:"public_key"`
	ExpiresInSeconds int    `json:"expires_in_seconds"`
}

type nativePeerChallenge struct {
	ControlProtocol int    `json:"control_protocol"`
	Operation       string `json:"operation"`
	Challenge       string `json:"challenge"`
}

type nativePeerProof struct {
	ControlProtocol int    `json:"control_protocol"`
	Operation       string `json:"operation"`
	Signature       string `json:"signature"`
}

func nativePeerSignatureMessage(req nativeIssueRequest, challenge string) []byte {
	return []byte("SAGE-NATIVE-PEER/1\n" + challenge + "\n" + req.Generation + "\n" + req.Origin + "\n" + *req.StartupProof + "\n" + req.PublicKey)
}

func decodeNativeProofValue(raw string, size int) ([]byte, bool) {
	decoded, err := base64.RawURLEncoding.DecodeString(raw)
	return decoded, err == nil && len(decoded) == size && base64.RawURLEncoding.EncodeToString(decoded) == raw
}

func (s *Server) handleNativeIssue(conn net.Conn, payload []byte, deadline time.Time) {
	if conn.SetDeadline(deadline) != nil {
		return
	}
	var req nativeIssueRequest
	if !time.Now().Before(deadline) || nativebootstrap.DecodeObject(payload, &req) != nil || req.ControlProtocol != 2 || req.ShellProtocol != 1 || req.Operation != "native-session.issue" || req.StartupProof == nil {
		return
	}
	key, keyOK := decodeNativeProofValue(req.PublicKey, ed25519.PublicKeySize)
	binding := nativebootstrap.Binding{Generation: req.Generation, Origin: req.Origin, StartupProof: *req.StartupProof}
	if !keyOK || binding != s.NativeBinding() {
		return
	}
	s.mu.RLock()
	requirement, broker := s.nativeRequirement, s.nativeBroker
	state := s.state
	s.mu.RUnlock()
	if requirement == "" || broker == nil || (state != StateReady && state != StateDegraded) {
		return
	}
	select {
	case s.nativeChecks <- struct{}{}:
		defer func() { <-s.nativeChecks }()
	default:
		return
	}
	peer, err := nativeidentity.Verify(conn, requirement)
	if err != nil || !time.Now().Before(deadline) {
		return
	}
	// LOCAL_PEERTOKEN describes the current socket peer. A process could buffer
	// the initial request before exec'ing permitted code, so that request alone
	// cannot prove the verified application owns the public key. Challenge it on
	// this socket after verification, then recheck the exact process/code identity.
	var nonce [32]byte
	if _, randomErr := rand.Read(nonce[:]); randomErr != nil {
		return
	}
	challenge := base64.RawURLEncoding.EncodeToString(nonce[:])
	encodedChallenge, err := json.Marshal(nativePeerChallenge{ControlProtocol: 2, Operation: "native-session.prove", Challenge: challenge})
	if err != nil || !time.Now().Before(deadline) || writeFrame(conn, encodedChallenge) != nil {
		return
	}
	proofPayload, err := readFrame(conn)
	if err != nil || !time.Now().Before(deadline) {
		return
	}
	var proof nativePeerProof
	if nativebootstrap.DecodeObject(proofPayload, &proof) != nil || proof.ControlProtocol != 2 || proof.Operation != "native-session.prove" {
		return
	}
	signature, signatureOK := decodeNativeProofValue(proof.Signature, ed25519.SignatureSize)
	if !signatureOK || !ed25519.Verify(key, nativePeerSignatureMessage(req, challenge), signature) {
		return
	}
	after, err := nativeidentity.Verify(conn, requirement)
	if err != nil || after != peer || !time.Now().Before(deadline) {
		return
	}
	// Hold the state lock through issuance so draining cannot race a new ticket.
	s.mu.RLock()
	defer s.mu.RUnlock()
	if !time.Now().Before(deadline) || (s.state != StateReady && s.state != StateDegraded) {
		return
	}
	ticket, err := broker.Issue(nativebootstrap.Peer{ProcessBinding: peer.ProcessBinding, Identifier: peer.Identifier, TeamID: peer.TeamID, CDHash: peer.CDHash}, binding, req.PublicKey)
	if err != nil {
		return
	}
	encoded, err := json.Marshal(nativeIssueResponse{ControlProtocol: 2, Operation: "native-session.issue", Generation: binding.Generation, Origin: binding.Origin, StartupProof: binding.StartupProof, Ticket: ticket.Ticket, Challenge: ticket.Challenge, PublicKey: ticket.PublicKey, ExpiresInSeconds: ticket.ExpiresInSeconds})
	if err == nil && time.Now().Before(deadline) {
		_ = writeFrame(conn, encoded)
	}
}
