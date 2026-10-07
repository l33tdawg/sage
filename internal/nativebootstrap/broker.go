// Package nativebootstrap admits a verified local application to a native
// transport. Admission is not a dashboard login, vault unlock, or RBAC role.
package nativebootstrap

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	TicketTTL         = 30 * time.Second
	AdmissionTTL      = 15 * time.Minute
	MaxTickets        = 128
	MaxTicketsPerPeer = 4
	MaxAdmissions     = 128
)

var (
	ErrInvalid     = errors.New("invalid native bootstrap proof")
	ErrUnavailable = errors.New("native bootstrap unavailable")
	ErrCapacity    = errors.New("native bootstrap capacity reached")
)

// Binding is the exact daemon attachment authenticated through shell control.
// StartupProof is the empty string for an independently started daemon.
type Binding struct {
	Generation   string
	Origin       string
	StartupProof string
}

// Peer must come from the platform's verified socket-peer identity, never from
// request fields. The broker does not establish code-signing trust itself.
// ProcessBinding includes the audit-token process incarnation, not just a PID.
type Peer struct {
	ProcessBinding string
	Identifier     string
	TeamID         string
	CDHash         string
}

type Ticket struct {
	Ticket           string
	Challenge        string
	PublicKey        string
	Binding          Binding
	ExpiresInSeconds int
}

type Redemption struct {
	Ticket    string
	Challenge string
	PublicKey string
	Signature string
	Binding   Binding
}

// Admission authorizes only native transport admission. It must never be used
// as a vault session cookie or an agent, Root, Admin, or federation credential.
type Admission struct {
	Token            string
	ExpiresInSeconds int
}

type pendingTicket struct {
	publicKey     [ed25519.PublicKeySize]byte
	challengeHash [sha256.Size]byte
	peerHash      [sha256.Size]byte
	expires       time.Time
}

type admittedSession struct{ expires time.Time }

// Broker owns one immutable daemon attachment. Tickets and admission tokens
// exist in storage only as SHA-256 hashes, and no state is persisted.
type Broker struct {
	mu       sync.Mutex
	binding  Binding
	retired  bool
	tickets  map[[sha256.Size]byte]pendingTicket
	sessions map[[sha256.Size]byte]admittedSession
	now      func() time.Time
	random   io.Reader
}

func New(binding Binding) (*Broker, error) {
	if !validBinding(binding) {
		return nil, ErrInvalid
	}
	return &Broker{
		binding:  binding,
		tickets:  make(map[[sha256.Size]byte]pendingTicket),
		sessions: make(map[[sha256.Size]byte]admittedSession),
		now:      time.Now,
		random:   rand.Reader,
	}, nil
}

func (b *Broker) Issue(peer Peer, binding Binding, publicKey string) (Ticket, error) {
	key, ok := decodeCanonical(publicKey, ed25519.PublicKeySize)
	if !ok || binding != b.binding || !validPeer(peer) {
		return Ticket{}, ErrInvalid
	}
	peerHash := hashPeer(peer)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retired {
		return Ticket{}, ErrUnavailable
	}
	now := b.now()
	b.expire(now)
	if len(b.tickets) >= MaxTickets {
		return Ticket{}, ErrCapacity
	}
	peerTickets := 0
	for _, pending := range b.tickets {
		if pending.peerHash == peerHash {
			peerTickets++
		}
	}
	if peerTickets >= MaxTicketsPerPeer {
		return Ticket{}, ErrCapacity
	}
	ticket, ticketHash, err := b.freshSecret(func(hash [sha256.Size]byte) bool {
		_, exists := b.tickets[hash]
		return exists
	})
	if err != nil {
		return Ticket{}, err
	}
	challenge, _, err := b.freshSecret(func(hash [sha256.Size]byte) bool { return hash == ticketHash })
	if err != nil {
		return Ticket{}, err
	}
	challengeBytes, _ := decodeCanonical(challenge, 32)
	var storedKey [ed25519.PublicKeySize]byte
	copy(storedKey[:], key)
	b.tickets[ticketHash] = pendingTicket{
		publicKey: storedKey, challengeHash: sha256.Sum256(challengeBytes),
		peerHash: peerHash, expires: now.Add(TicketTTL),
	}
	return Ticket{ticket, challenge, publicKey, binding, int(TicketTTL.Seconds())}, nil
}

// SignatureMessage is the SSCP/2 redemption transcript. Redeem validates every
// field's canonical representation before verifying this transcript.
func SignatureMessage(r Redemption) []byte {
	return []byte("SAGE-NATIVE-BOOTSTRAP/1\n" + r.Ticket + "\n" + r.Challenge + "\n" +
		r.Binding.Generation + "\n" + r.Binding.Origin + "\n" + r.Binding.StartupProof + "\n" + r.PublicKey)
}

func (b *Broker) Redeem(r Redemption) (Admission, error) {
	ticket, ticketOK := decodeCanonical(r.Ticket, 32)
	challenge, challengeOK := decodeCanonical(r.Challenge, 32)
	key, keyOK := decodeCanonical(r.PublicKey, ed25519.PublicKeySize)
	signature, signatureOK := decodeCanonical(r.Signature, ed25519.SignatureSize)
	if !ticketOK || !challengeOK || !keyOK || !signatureOK || r.Binding != b.binding {
		return Admission{}, ErrInvalid
	}
	ticketHash := sha256.Sum256(ticket)
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retired {
		return Admission{}, ErrUnavailable
	}
	now := b.now()
	b.expire(now)
	pending, exists := b.tickets[ticketHash]
	if !exists || pending.challengeHash != sha256.Sum256(challenge) ||
		string(pending.publicKey[:]) != string(key) ||
		!ed25519.Verify(pending.publicKey[:], SignatureMessage(r), signature) {
		return Admission{}, ErrInvalid
	}
	if len(b.sessions) >= MaxAdmissions {
		return Admission{}, ErrCapacity
	}
	token, tokenHash, err := b.freshSecret(func(hash [sha256.Size]byte) bool {
		_, exists := b.sessions[hash]
		return exists || hash == ticketHash
	})
	if err != nil {
		return Admission{}, err
	}
	// Validation, capacity reservation, one-use consumption and admission are
	// one locked operation. Concurrent redemption has exactly one winner.
	delete(b.tickets, ticketHash)
	b.sessions[tokenHash] = admittedSession{expires: now.Add(AdmissionTTL)}
	return Admission{token, int(AdmissionTTL.Seconds())}, nil
}

func (b *Broker) Verify(token string, binding Binding) bool {
	raw, ok := decodeCanonical(token, 32)
	if !ok || binding != b.binding {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.retired {
		return false
	}
	b.expire(b.now())
	_, exists := b.sessions[sha256.Sum256(raw)]
	return exists
}

func (b *Broker) Revoke(token string) {
	raw, ok := decodeCanonical(token, 32)
	if !ok {
		return
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.sessions, sha256.Sum256(raw))
}

// Invalidate permanently retires this broker on drain, failure or close. A
// replacement daemon must construct a new broker with its fresh generation.
func (b *Broker) Invalidate() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retired = true
	clear(b.tickets)
	clear(b.sessions)
}

func (b *Broker) expire(now time.Time) {
	for hash, pending := range b.tickets {
		if !now.Before(pending.expires) {
			delete(b.tickets, hash)
		}
	}
	for hash, session := range b.sessions {
		if !now.Before(session.expires) {
			delete(b.sessions, hash)
		}
	}
}

func (b *Broker) freshSecret(collision func([sha256.Size]byte) bool) (string, [sha256.Size]byte, error) {
	for range 4 {
		var raw [32]byte
		if _, err := io.ReadFull(b.random, raw[:]); err != nil {
			return "", [sha256.Size]byte{}, fmt.Errorf("%w: secure randomness failed", ErrUnavailable)
		}
		hash := sha256.Sum256(raw[:])
		if !collision(hash) {
			return base64.RawURLEncoding.EncodeToString(raw[:]), hash, nil
		}
	}
	return "", [sha256.Size]byte{}, fmt.Errorf("%w: secure randomness collision", ErrUnavailable)
}

func decodeCanonical(value string, size int) ([]byte, bool) {
	if len(value) != base64.RawURLEncoding.EncodedLen(size) {
		return nil, false
	}
	decoded, err := base64.RawURLEncoding.Strict().DecodeString(value)
	return decoded, err == nil && len(decoded) == size && base64.RawURLEncoding.EncodeToString(decoded) == value
}

func validBinding(binding Binding) bool {
	if _, ok := decodeCanonical(binding.Generation, 32); !ok {
		return false
	}
	if binding.StartupProof != "" {
		proof, err := hex.DecodeString(binding.StartupProof)
		if err != nil || len(proof) != sha256.Size || hex.EncodeToString(proof) != binding.StartupProof {
			return false
		}
	}
	u, err := url.Parse(binding.Origin)
	if err != nil || u.Scheme != "http" || u.User != nil || u.Opaque != "" || u.Path != "" ||
		u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.RawFragment != "" {
		return false
	}
	if u.Hostname() != "127.0.0.1" && u.Hostname() != "localhost" && u.Hostname() != "::1" {
		return false
	}
	port, err := strconv.ParseUint(u.Port(), 10, 16)
	return err == nil && port != 0 && strconv.FormatUint(port, 10) == u.Port() &&
		u.String() == binding.Origin && strings.ToLower(u.Host) == u.Host
}

func validPeer(peer Peer) bool {
	if peer.ProcessBinding == "" || peer.Identifier == "" || peer.CDHash == "" {
		return false
	}
	for _, field := range []string{peer.ProcessBinding, peer.Identifier, peer.TeamID, peer.CDHash} {
		if len(field) > 512 || strings.ContainsAny(field, "\x00\r\n") {
			return false
		}
	}
	return true
}

func hashPeer(peer Peer) [sha256.Size]byte {
	hash := sha256.New()
	for _, field := range []string{peer.ProcessBinding, peer.Identifier, peer.TeamID, peer.CDHash} {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(field)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(field))
	}
	var result [sha256.Size]byte
	copy(result[:], hash.Sum(nil))
	return result
}
