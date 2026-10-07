package nativebootstrap

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func testBinding() Binding {
	return Binding{Generation: base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{1}, 32)), Origin: "http://127.0.0.1:18080"}
}

func testPeer(index int) Peer {
	return Peer{ProcessBinding: fmt.Sprintf("audit-token-process-incarnation-%d", index), Identifier: "com.sage.native.test", TeamID: "TEAM", CDHash: strings.Repeat("a", 40)}
}

func newTestBroker(t *testing.T) (*Broker, *time.Time) {
	t.Helper()
	b, err := New(testBinding())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now() // Preserve the monotonic component, as production time.Now does.
	b.now = func() time.Time { return now }
	return b, &now
}

func testKey(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.RawURLEncoding.EncodeToString(public), private
}

func issueRedemption(t *testing.T, b *Broker, peer Peer) (Ticket, Redemption, ed25519.PrivateKey) {
	t.Helper()
	public, private := testKey(t)
	ticket, err := b.Issue(peer, b.binding, public)
	if err != nil {
		t.Fatal(err)
	}
	redemption := Redemption{Ticket: ticket.Ticket, Challenge: ticket.Challenge, PublicKey: public, Binding: b.binding}
	redemption.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, SignatureMessage(redemption)))
	return ticket, redemption, private
}

func TestAdmissionIsBoundAndOneUse(t *testing.T) {
	b, _ := newTestBroker(t)
	ticket, redemption, _ := issueRedemption(t, b, testPeer(1))
	if ticket.ExpiresInSeconds != 30 || ticket.Binding != b.binding || ticket.PublicKey != redemption.PublicKey {
		t.Fatal("incorrect ticket contract")
	}
	if b.Verify(ticket.Ticket, b.binding) {
		t.Fatal("ticket must not be a transport admission")
	}
	admission, err := b.Redeem(redemption)
	if err != nil {
		t.Fatal(err)
	}
	if admission.ExpiresInSeconds != 900 || admission.Token == ticket.Ticket {
		t.Fatal("incorrect admission contract")
	}
	if !b.Verify(admission.Token, b.binding) {
		t.Fatal("admission missing")
	}
	if _, err := b.Redeem(redemption); !errors.Is(err, ErrInvalid) {
		t.Fatalf("ticket replay: %v", err)
	}
	changed := b.binding
	changed.Generation = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	if b.Verify(admission.Token, changed) {
		t.Fatal("different generation admitted")
	}
	changed = b.binding
	changed.Origin = "http://localhost:18080"
	if b.Verify(admission.Token, changed) {
		t.Fatal("different exact origin admitted")
	}
	changed = b.binding
	changed.StartupProof = strings.Repeat("b", 64)
	if b.Verify(admission.Token, changed) {
		t.Fatal("different startup proof admitted")
	}
	b.Revoke(admission.Token)
	if b.Verify(admission.Token, b.binding) {
		t.Fatal("revoked admission survived")
	}
	b.Revoke(admission.Token) // Idempotent.
	b.Revoke("malformed")
}

func TestConcurrentRedemptionHasOneWinner(t *testing.T) {
	b, _ := newTestBroker(t)
	_, redemption, _ := issueRedemption(t, b, testPeer(1))
	const attempts = 64
	start := make(chan struct{})
	var wg sync.WaitGroup
	var winners atomic.Int32
	for range attempts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			admission, err := b.Redeem(redemption)
			if err == nil {
				winners.Add(1)
				if !b.Verify(admission.Token, b.binding) {
					t.Error("winning admission not valid")
				}
			} else if !errors.Is(err, ErrInvalid) {
				t.Errorf("unexpected failure: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if winners.Load() != 1 || len(b.sessions) != 1 || len(b.tickets) != 0 {
		t.Fatalf("winners=%d, admissions=%d, remaining tickets=%d", winners.Load(), len(b.sessions), len(b.tickets))
	}
}

func TestTamperingDoesNotConsumeTicket(t *testing.T) {
	canonicalOther := base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{9}, 32))
	cases := map[string]func(*Redemption){
		"ticket":                  func(r *Redemption) { r.Ticket = canonicalOther },
		"challenge":               func(r *Redemption) { r.Challenge = canonicalOther },
		"generation":              func(r *Redemption) { r.Binding.Generation = canonicalOther },
		"origin":                  func(r *Redemption) { r.Binding.Origin = "http://127.0.0.1:18081" },
		"startup proof":           func(r *Redemption) { r.Binding.StartupProof = strings.Repeat("a", 64) },
		"public key":              func(r *Redemption) { r.PublicKey = canonicalOther },
		"signature":               func(r *Redemption) { r.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64)) },
		"padded ticket":           func(r *Redemption) { r.Ticket += "=" },
		"padded key":              func(r *Redemption) { r.PublicKey += "=" },
		"padded signature":        func(r *Redemption) { r.Signature += "==" },
		"invalid ticket pad bits": func(r *Redemption) { r.Ticket = r.Ticket[:42] + "B" },
		"invalid key pad bits":    func(r *Redemption) { r.PublicKey = r.PublicKey[:42] + "B" },
		"newline challenge":       func(r *Redemption) { r.Challenge += "\n" },
		"domain":                  func(r *Redemption) { r.Signature = base64.RawURLEncoding.EncodeToString(make([]byte, 64)) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			b, _ := newTestBroker(t)
			_, original, private := issueRedemption(t, b, testPeer(1))
			changed := original
			mutate(&changed)
			if name == "domain" {
				changed.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, []byte("OTHER/1\n"+string(SignatureMessage(original)))))
			}
			if _, err := b.Redeem(changed); !errors.Is(err, ErrInvalid) {
				t.Fatalf("tampered request: %v", err)
			}
			if _, err := b.Redeem(original); err != nil {
				t.Fatalf("invalid attempt consumed legitimate ticket: %v", err)
			}
		})
	}
}

func TestOtherPrivateKeyCannotRedeem(t *testing.T) {
	b, _ := newTestBroker(t)
	_, original, _ := issueRedemption(t, b, testPeer(1))
	otherPublic, otherPrivate := testKey(t)
	changed := original
	changed.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(otherPrivate, SignatureMessage(changed)))
	if _, err := b.Redeem(changed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("unbound private key: %v", err)
	}
	changed.PublicKey = otherPublic
	changed.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(otherPrivate, SignatureMessage(changed)))
	if _, err := b.Redeem(changed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("replaced key pair: %v", err)
	}
	if _, err := b.Redeem(original); err != nil {
		t.Fatal(err)
	}
}

func TestStartupProofParticipatesInTranscript(t *testing.T) {
	binding := testBinding()
	binding.StartupProof = strings.Repeat("a", 64)
	b, err := New(binding)
	if err != nil {
		t.Fatal(err)
	}
	_, original, private := issueRedemption(t, b, testPeer(1))
	changed := original
	changed.Binding.StartupProof = ""
	changed.Signature = base64.RawURLEncoding.EncodeToString(ed25519.Sign(private, SignatureMessage(changed)))
	if _, err := b.Redeem(changed); !errors.Is(err, ErrInvalid) {
		t.Fatalf("removed proof: %v", err)
	}
	if _, err := b.Redeem(original); err != nil {
		t.Fatal(err)
	}
}

func TestMonotonicExpiryAndCapacityReclamation(t *testing.T) {
	t.Run("ticket boundary", func(t *testing.T) {
		b, now := newTestBroker(t)
		_, redemption, _ := issueRedemption(t, b, testPeer(1))
		*now = now.Add(TicketTTL)
		if _, err := b.Redeem(redemption); !errors.Is(err, ErrInvalid) {
			t.Fatalf("expired ticket: %v", err)
		}
		if len(b.tickets) != 0 {
			t.Fatal("expired ticket retained")
		}
	})
	t.Run("ticket just before expiry", func(t *testing.T) {
		b, now := newTestBroker(t)
		_, redemption, _ := issueRedemption(t, b, testPeer(1))
		*now = now.Add(TicketTTL - time.Nanosecond)
		if _, err := b.Redeem(redemption); err != nil {
			t.Fatal(err)
		}
	})
	t.Run("admission boundary", func(t *testing.T) {
		b, now := newTestBroker(t)
		_, redemption, _ := issueRedemption(t, b, testPeer(1))
		admission, err := b.Redeem(redemption)
		if err != nil {
			t.Fatal(err)
		}
		*now = now.Add(AdmissionTTL - time.Nanosecond)
		if !b.Verify(admission.Token, b.binding) {
			t.Fatal("admission expired too early")
		}
		*now = now.Add(time.Nanosecond)
		if b.Verify(admission.Token, b.binding) || len(b.sessions) != 0 {
			t.Fatal("expired admission survived")
		}
	})
	t.Run("peer capacity expires", func(t *testing.T) {
		b, now := newTestBroker(t)
		public, _ := testKey(t)
		for range MaxTicketsPerPeer {
			if _, err := b.Issue(testPeer(1), b.binding, public); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := b.Issue(testPeer(1), b.binding, public); !errors.Is(err, ErrCapacity) {
			t.Fatalf("peer limit: %v", err)
		}
		if _, err := b.Issue(testPeer(2), b.binding, public); err != nil {
			t.Fatalf("independent process blocked: %v", err)
		}
		*now = now.Add(TicketTTL)
		if _, err := b.Issue(testPeer(1), b.binding, public); err != nil {
			t.Fatalf("expired capacity not reclaimed: %v", err)
		}
		if len(b.tickets) != 1 {
			t.Fatal("expired tickets retained")
		}
	})
}

func TestGlobalTicketLimit(t *testing.T) {
	b, _ := newTestBroker(t)
	public, _ := testKey(t)
	for i := range MaxTickets {
		if _, err := b.Issue(testPeer(i), b.binding, public); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Issue(testPeer(MaxTickets), b.binding, public); !errors.Is(err, ErrCapacity) {
		t.Fatalf("global ticket limit: %v", err)
	}
	if len(b.tickets) != MaxTickets {
		t.Fatal("unbounded ticket storage")
	}
}

func TestAdmissionLimitDoesNotConsumeUnadmittedTicket(t *testing.T) {
	b, _ := newTestBroker(t)
	var first Admission
	for i := range MaxAdmissions {
		_, redemption, _ := issueRedemption(t, b, testPeer(i))
		admission, err := b.Redeem(redemption)
		if err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			first = admission
		}
	}
	_, redemption, _ := issueRedemption(t, b, testPeer(MaxAdmissions))
	if _, err := b.Redeem(redemption); !errors.Is(err, ErrCapacity) {
		t.Fatalf("admission limit: %v", err)
	}
	if len(b.sessions) != MaxAdmissions || len(b.tickets) != 1 {
		t.Fatal("capacity failure changed admission state")
	}
	b.Revoke(first.Token)
	if _, err := b.Redeem(redemption); err != nil {
		t.Fatalf("capacity was not reusable after revoke: %v", err)
	}
}

func TestInvalidateRetiresAllStateAndNewDaemonRejectsOldSecrets(t *testing.T) {
	b, _ := newTestBroker(t)
	_, redeemed, _ := issueRedemption(t, b, testPeer(1))
	admission, err := b.Redeem(redeemed)
	if err != nil {
		t.Fatal(err)
	}
	_, outstanding, _ := issueRedemption(t, b, testPeer(2))
	b.Invalidate()
	b.Invalidate()
	if b.Verify(admission.Token, b.binding) || len(b.tickets) != 0 || len(b.sessions) != 0 {
		t.Fatal("retired state survived")
	}
	if _, err := b.Redeem(outstanding); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("retired redemption: %v", err)
	}
	if _, err := b.Issue(testPeer(3), b.binding, outstanding.PublicKey); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("retired issue: %v", err)
	}
	nextBinding := b.binding
	nextBinding.Generation = base64.RawURLEncoding.EncodeToString(bytes.Repeat([]byte{2}, 32))
	next, err := New(nextBinding)
	if err != nil {
		t.Fatal(err)
	}
	if next.Verify(admission.Token, b.binding) || next.Verify(admission.Token, nextBinding) {
		t.Fatal("old admission transferred to new daemon")
	}
	if _, err := next.Redeem(outstanding); !errors.Is(err, ErrInvalid) {
		t.Fatalf("old ticket transferred: %v", err)
	}
	_, fresh, _ := issueRedemption(t, next, testPeer(1))
	if _, err := next.Redeem(fresh); err != nil {
		t.Fatal(err)
	}
}

func TestInvalidateRacesLeaveNoCredentialValid(t *testing.T) {
	b, _ := newTestBroker(t)
	_, redemption, _ := issueRedemption(t, b, testPeer(1))
	var wg sync.WaitGroup
	start := make(chan struct{})
	admissions := make(chan Admission, 32)
	for range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if admission, err := b.Redeem(redemption); err == nil {
				admissions <- admission
			}
		}()
	}
	wg.Add(1)
	go func() { defer wg.Done(); <-start; b.Invalidate() }()
	close(start)
	wg.Wait()
	close(admissions)
	for admission := range admissions {
		if b.Verify(admission.Token, b.binding) {
			t.Fatal("racing admission survived retirement")
		}
	}
	if len(b.sessions) != 0 || len(b.tickets) != 0 {
		t.Fatal("retired broker retained state")
	}
}

func TestSecretsAreHashedInMemory(t *testing.T) {
	b, _ := newTestBroker(t)
	ticket, redemption, _ := issueRedemption(t, b, testPeer(1))
	rawTicket, _ := decodeCanonical(ticket.Ticket, 32)
	if _, exists := b.tickets[sha256.Sum256(rawTicket)]; !exists {
		t.Fatal("ticket key is not its hash")
	}
	for hash, pending := range b.tickets {
		if bytes.Equal(hash[:], rawTicket) {
			t.Fatal("plaintext ticket key")
		}
		for i := range reflect.TypeOf(pending).NumField() {
			if reflect.TypeOf(pending).Field(i).Type.Kind() == reflect.String {
				t.Fatal("pending state must not store opaque string secrets")
			}
		}
	}
	admission, err := b.Redeem(redemption)
	if err != nil {
		t.Fatal(err)
	}
	rawAdmission, _ := decodeCanonical(admission.Token, 32)
	if _, exists := b.sessions[sha256.Sum256(rawAdmission)]; !exists {
		t.Fatal("admission key is not its hash")
	}
	for hash := range b.sessions {
		if bytes.Equal(hash[:], rawAdmission) {
			t.Fatal("plaintext admission key")
		}
	}
	if len(b.tickets) != 0 {
		t.Fatal("consumed ticket retained")
	}
	if strings.Contains(fmt.Sprintf("%#v", b), ticket.Ticket) || strings.Contains(fmt.Sprintf("%#v", b), admission.Token) {
		t.Fatal("broker retains printable secret")
	}
}

func TestRandomFailuresFailClosedWithoutConsumingTicket(t *testing.T) {
	b, _ := newTestBroker(t)
	public, _ := testKey(t)
	b.random = bytes.NewReader(nil)
	if _, err := b.Issue(testPeer(1), b.binding, public); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed random ticket: %v", err)
	}
	if len(b.tickets) != 0 {
		t.Fatal("failed issuance stored ticket")
	}
	b.random = rand.Reader
	_, redemption, _ := issueRedemption(t, b, testPeer(1))
	b.random = bytes.NewReader(nil)
	if _, err := b.Redeem(redemption); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("failed random admission: %v", err)
	}
	if len(b.tickets) != 1 || len(b.sessions) != 0 {
		t.Fatal("random failure consumed ticket")
	}
	b.random = rand.Reader
	if _, err := b.Redeem(redemption); err != nil {
		t.Fatal(err)
	}
}

func TestRandomCollisionCannotOverwriteTicket(t *testing.T) {
	b, _ := newTestBroker(t)
	public, _ := testKey(t)
	ticketBytes := bytes.Repeat([]byte{1}, 32)
	challengeBytes := bytes.Repeat([]byte{2}, 32)
	b.random = io.MultiReader(bytes.NewReader(ticketBytes), bytes.NewReader(challengeBytes))
	first, err := b.Issue(testPeer(1), b.binding, public)
	if err != nil {
		t.Fatal(err)
	}
	b.random = bytes.NewReader(bytes.Repeat(ticketBytes, 4))
	if _, err := b.Issue(testPeer(2), b.binding, public); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("collision overwrite: %v", err)
	}
	if len(b.tickets) != 1 {
		t.Fatal("collision changed storage")
	}
	raw, _ := decodeCanonical(first.Ticket, 32)
	if b.tickets[sha256.Sum256(raw)].peerHash != hashPeer(testPeer(1)) {
		t.Fatal("collision replaced original peer")
	}
}

func TestCanonicalBindingAndIssueValidation(t *testing.T) {
	invalidOrigins := []string{
		"https://127.0.0.1:18080", "http://evil.example:18080", "http://LOCALHOST:18080",
		"http://127.0.0.1", "http://127.0.0.1:0", "http://127.0.0.1:65536", "http://127.0.0.1:018080",
		"http://127.0.0.1:18080/", "http://127.0.0.1:18080?", "http://127.0.0.1:18080?x=1",
		"http://127.0.0.1:18080#", "http://127.0.0.1:18080#fragment", "http://user@127.0.0.1:18080",
	}
	for _, origin := range invalidOrigins {
		t.Run(origin, func(t *testing.T) {
			binding := testBinding()
			binding.Origin = origin
			if _, err := New(binding); !errors.Is(err, ErrInvalid) {
				t.Fatalf("invalid origin accepted: %v", err)
			}
		})
	}
	for _, origin := range []string{"http://127.0.0.1:18080", "http://localhost:18080", "http://[::1]:18080"} {
		binding := testBinding()
		binding.Origin = origin
		if _, err := New(binding); err != nil {
			t.Fatalf("valid origin %s: %v", origin, err)
		}
	}
	for _, generation := range []string{"", strings.Repeat("A", 42), strings.Repeat("A", 42) + "B", testBinding().Generation + "="} {
		binding := testBinding()
		binding.Generation = generation
		if _, err := New(binding); !errors.Is(err, ErrInvalid) {
			t.Fatal("noncanonical generation accepted")
		}
	}
	for _, proof := range []string{"a", strings.Repeat("A", 64), strings.Repeat("g", 64)} {
		binding := testBinding()
		binding.StartupProof = proof
		if _, err := New(binding); !errors.Is(err, ErrInvalid) {
			t.Fatal("noncanonical startup proof accepted")
		}
	}
	b, _ := newTestBroker(t)
	public, _ := testKey(t)
	for _, peer := range []Peer{{}, {ProcessBinding: "pid", Identifier: "id"}, {ProcessBinding: "pid\n", Identifier: "id", CDHash: "hash"}} {
		if _, err := b.Issue(peer, b.binding, public); !errors.Is(err, ErrInvalid) {
			t.Fatal("missing process identity admitted")
		}
	}
	if _, err := b.Issue(testPeer(1), b.binding, public+"="); !errors.Is(err, ErrInvalid) {
		t.Fatal("noncanonical key admitted")
	}
	changed := b.binding
	changed.Origin = "http://localhost:18080"
	if _, err := b.Issue(testPeer(1), changed, public); !errors.Is(err, ErrInvalid) {
		t.Fatal("wrong attachment admitted")
	}
}

func TestSignatureTranscriptExactWireContract(t *testing.T) {
	r := Redemption{Ticket: "ticket", Challenge: "challenge", PublicKey: "key", Binding: Binding{Generation: "generation", Origin: "http://127.0.0.1:18080", StartupProof: ""}}
	want := "SAGE-NATIVE-BOOTSTRAP/1\nticket\nchallenge\ngeneration\nhttp://127.0.0.1:18080\n\nkey"
	if string(SignatureMessage(r)) != want {
		t.Fatalf("transcript mismatch: %q", SignatureMessage(r))
	}
}
