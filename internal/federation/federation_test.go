package federation

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tlsca"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- scaffolding -------------------------------------------------------------

type testChain struct {
	chainID  string
	certsDir string
	caPEM    []byte
	mgr      *Manager
	badger   *store.BadgerStore
	mem      store.MemoryStore
	agentPub ed25519.PublicKey
	agentKey ed25519.PrivateKey
}

// newTestChain provisions a full single-node "chain": its own CA + node cert,
// badger + sqlite stores, operator agent key, and a federation Manager.
func newTestChain(t *testing.T, chainID string) *testChain {
	t.Helper()
	dir := t.TempDir()
	certsDir := filepath.Join(dir, "certs")

	caCert, caKey, err := tlsca.LoadOrGenerateCA(certsDir, chainID)
	if err != nil {
		t.Fatalf("generate CA: %v", err)
	}
	nodeCert, nodeKey, err := tlsca.GenerateNodeCert(caCert, caKey, "node-"+chainID, nil)
	if err != nil {
		t.Fatalf("generate node cert: %v", err)
	}
	if err := tlsca.WriteCert(filepath.Join(certsDir, tlsca.NodeCertFile), nodeCert); err != nil {
		t.Fatal(err)
	}
	if err := tlsca.WriteKey(filepath.Join(certsDir, tlsca.NodeKeyFile), nodeKey); err != nil {
		t.Fatal(err)
	}

	badger, err := store.NewBadgerStore(filepath.Join(dir, "badger"))
	if err != nil {
		t.Fatalf("badger: %v", err)
	}
	t.Cleanup(func() { _ = badger.CloseBadger() })

	sqlite, err := store.NewSQLiteStore(context.Background(), filepath.Join(dir, "mem.db"))
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	t.Cleanup(func() { _ = sqlite.Close() })

	pub, priv, err := auth.GenerateKeypair()
	if err != nil {
		t.Fatal(err)
	}

	mgr := NewManager(Config{
		LocalChainID: chainID,
		CertsDir:     certsDir,
		AgentKey:     priv,
		Badger:       badger,
		MemStore:     sqlite,
		Logger:       zerolog.Nop(),
	})
	return &testChain{
		chainID:  chainID,
		certsDir: certsDir,
		caPEM:    []byte(tlsca.EncodeCertPEM(caCert)),
		mgr:      mgr,
		badger:   badger,
		mem:      sqlite,
		agentPub: pub,
		agentKey: priv,
	}
}

// federate records an ACTIVE agreement on `on` about `peer`: provisions the
// peer CA on disk and writes the cross_fed record with its SPKI pin.
func federate(t *testing.T, on, peer *testChain, endpoint string, domains []string, ceiling uint8, expiresAt int64) {
	t.Helper()
	pin, err := on.mgr.StoreRemoteCA(peer.chainID, peer.caPEM)
	if err != nil {
		t.Fatalf("store remote CA: %v", err)
	}
	if err := on.badger.SetCrossFed(peer.chainID, endpoint, pin, ceiling, expiresAt, domains, nil, "active"); err != nil {
		t.Fatalf("set cross_fed: %v", err)
	}
}

// startListener serves a chain's federation router over its mTLS config.
func startListener(t *testing.T, c *testChain) *httptest.Server {
	t.Helper()
	tlsCfg, err := c.mgr.ServerTLSConfig()
	if err != nil {
		t.Fatalf("server tls config: %v", err)
	}
	ts := httptest.NewUnstartedServer(c.mgr.Router())
	ts.TLS = tlsCfg
	ts.StartTLS()
	t.Cleanup(ts.Close)
	return ts
}

// fakeComet emulates the local CometBFT /broadcast_tx_commit RPC, capturing
// every broadcast tx's raw bytes.
func fakeComet(t *testing.T) (*httptest.Server, *[][]byte) {
	t.Helper()
	var captured [][]byte
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		txParam := r.URL.Query().Get("tx")
		if len(txParam) > 2 && txParam[:2] == "0x" {
			if raw, err := hex.DecodeString(txParam[2:]); err == nil {
				captured = append(captured, raw)
			}
		}
		fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"CAFE","height":"42"}}`)
	}))
	t.Cleanup(ts.Close)
	return ts, &captured
}

func insertCommitted(t *testing.T, c *testChain, id, domain, content string) {
	t.Helper()
	h := sha256.Sum256([]byte(content))
	err := c.mem.InsertMemory(context.Background(), &memory.MemoryRecord{
		MemoryID:        id,
		SubmittingAgent: hex.EncodeToString(c.agentPub),
		Content:         content,
		ContentHash:     h[:],
		MemoryType:      memory.TypeFact,
		DomainTag:       domain,
		ConfidenceScore: 0.9,
		Status:          memory.StatusCommitted,
		CreatedAt:       time.Now().Add(-time.Hour),
	})
	if err != nil {
		t.Fatalf("insert memory: %v", err)
	}
}

func seedCoCommit(t *testing.T, c *testChain, sharedID string, core []byte, coauthors []tx.CoCommitCoauthor) {
	t.Helper()
	if err := c.badger.SetCoCommitCore(sharedID, core); err != nil {
		t.Fatal(err)
	}
	if err := c.badger.SetCoCommitCoauthors(sharedID, tx.EncodeCoauthorsCanonical(coauthors)); err != nil {
		t.Fatal(err)
	}
}

// --- unit: trust helpers -----------------------------------------------------

func TestValidateChainID(t *testing.T) {
	for _, ok := range []string{"sage-personal", "acme-ab3xyz42", "a", "chain.b_1"} {
		if err := ValidateChainID(ok); err != nil {
			t.Errorf("%q should be valid: %v", ok, err)
		}
	}
	for _, bad := range []string{"", "..", "../etc", "a/b", "a\\b", "UPPER", "-lead", ".lead", "x" + string(make([]byte, 200))} {
		if err := ValidateChainID(bad); err == nil {
			t.Errorf("%q should be rejected", bad)
		}
	}
}

func TestDomainAllowed(t *testing.T) {
	cases := []struct {
		allowed []string
		domain  string
		want    bool
	}{
		{[]string{"*"}, "anything", true},
		{[]string{"*"}, "", true},
		{[]string{"hr"}, "hr", true},
		{[]string{"hr"}, "hr.public", true},
		{[]string{"hr"}, "hrx", false}, // prefix but not a subtree
		{[]string{"hr"}, "finance", false},
		{[]string{"hr"}, "", false},          // unscoped query under scoped treaty
		{[]string{"hr.public"}, "hr", false}, // child never covers parent
		{nil, "anything", false},             // empty scope allows nothing
	}
	for _, tc := range cases {
		if got := DomainAllowed(tc.allowed, tc.domain); got != tc.want {
			t.Errorf("DomainAllowed(%v, %q) = %v, want %v", tc.allowed, tc.domain, got, tc.want)
		}
	}
}

func TestPinMismatchFailsClosed(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")
	c := newTestChain(t, "chain-c")

	// Provision C's CA on disk under B's chain id, but pin B's real CA
	// on-chain: the on-disk anchor no longer matches the agreement.
	if _, err := a.mgr.StoreRemoteCA(b.chainID, c.caPEM); err != nil {
		t.Fatal(err)
	}
	realPin, err := a.mgr.StoreRemoteCA("scratch-"+b.chainID, b.caPEM)
	if err != nil {
		t.Fatal(err)
	}
	if err := a.badger.SetCrossFed(b.chainID, "https://127.0.0.1:1", realPin, 4, 0, []string{"*"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.mgr.clientTLSConfig(b.chainID, realPin); err == nil {
		t.Fatal("client TLS config accepted a pin-mismatched on-disk CA")
	}
}

func TestSelfFederationRefused(t *testing.T) {
	a := newTestChain(t, "chain-a")
	pin := make([]byte, 32)
	if err := a.badger.SetCrossFed("chain-a", "https://127.0.0.1:1", pin, 4, 0, []string{"*"}, nil, "active"); err != nil {
		t.Fatal(err)
	}
	if _, err := a.mgr.ActiveAgreement("chain-a"); err == nil {
		t.Fatal("self-federation accepted")
	}
}

func TestExpiredAgreementDenied(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")
	federate(t, a, b, "https://127.0.0.1:1", []string{"*"}, 4, time.Now().Unix()-10)
	if _, err := a.mgr.ActiveAgreement(b.chainID); err == nil {
		t.Fatal("expired agreement accepted")
	}
}

// --- e2e: mTLS listener + query client ----------------------------------------

func TestFederatedQueryEndToEnd(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")

	insertCommitted(t, b, "mem-1", "shared.notes", "the bridge protocol works end to end")

	listener := startListener(t, b)
	federate(t, b, a, "https://unused.invalid", []string{"shared"}, 2, 0) // B's terms about A
	federate(t, a, b, listener.URL, []string{"shared"}, 2, 0)             // A's terms about B

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := a.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{
		Mode:      ModeText,
		Query:     "bridge protocol",
		DomainTag: "shared.notes",
		TopK:      5,
	})
	if err != nil {
		t.Fatalf("QueryPeer: %v", err)
	}
	if resp.ChainID != b.chainID {
		t.Fatalf("peer identified as %q", resp.ChainID)
	}
	if len(resp.Results) != 1 || resp.Results[0].MemoryID != "mem-1" {
		t.Fatalf("expected mem-1, got %+v", resp.Results)
	}

	// The proxy fan-out stamps provenance + chain-qualifies the author.
	outcomes := a.mgr.FanOutRecall(ctx, nil, &QueryRequest{Mode: ModeText, Query: "bridge protocol", DomainTag: "shared.notes"})
	if len(outcomes) != 1 || outcomes[0].Err != nil {
		t.Fatalf("fan-out: %+v", outcomes)
	}
	got := outcomes[0].Results[0]
	if got.SourceChainID != b.chainID {
		t.Errorf("SourceChainID = %q", got.SourceChainID)
	}
	if want := hex.EncodeToString(b.agentPub) + "@" + b.chainID; got.SubmittingAgent != want {
		t.Errorf("SubmittingAgent = %q, want %q", got.SubmittingAgent, want)
	}
}

func TestQueryDomainScopeDenied(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")
	insertCommitted(t, b, "mem-priv", "private.vault", "sealed content")

	listener := startListener(t, b)
	federate(t, b, a, "https://unused.invalid", []string{"shared"}, 2, 0)
	federate(t, a, b, listener.URL, []string{"shared"}, 2, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{Mode: ModeText, Query: "sealed", DomainTag: "private.vault"}); err == nil {
		t.Fatal("out-of-scope domain served")
	}
	// Unscoped query under a scoped treaty is also refused.
	if _, err := a.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{Mode: ModeText, Query: "sealed"}); err == nil {
		t.Fatal("unscoped query served under scoped treaty")
	}
}

func TestClassificationCeilingHidesRecords(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")
	insertCommitted(t, b, "mem-secret", "shared.notes", "classified bridge details")
	if err := b.badger.SetMemoryClassification("mem-secret", 3); err != nil {
		t.Fatal(err)
	}

	listener := startListener(t, b)
	federate(t, b, a, "https://unused.invalid", []string{"shared"}, 1, 0) // ceiling 1 < class 3
	federate(t, a, b, listener.URL, []string{"shared"}, 1, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := a.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{Mode: ModeText, Query: "classified bridge", DomainTag: "shared.notes"})
	if err != nil {
		t.Fatalf("QueryPeer: %v", err)
	}
	if len(resp.Results) != 0 {
		t.Fatalf("classified record leaked past the ceiling: %+v", resp.Results)
	}
	if resp.HiddenByPolicy != 1 {
		t.Errorf("HiddenByPolicy = %d, want 1", resp.HiddenByPolicy)
	}
}

func TestRevokedAgreementDeniedServerSide(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")
	insertCommitted(t, b, "mem-1", "shared.notes", "revocation must fail closed")

	listener := startListener(t, b)
	federate(t, b, a, "https://unused.invalid", []string{"shared"}, 2, 0)
	federate(t, a, b, listener.URL, []string{"shared"}, 2, 0)
	if err := b.badger.UpdateCrossFedStatus(a.chainID, "revoked"); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := a.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{Mode: ModeText, Query: "revocation", DomainTag: "shared.notes"}); err == nil {
		t.Fatal("revoked agreement still served")
	}
}

func TestHandshakeRejectsUnpinnedClient(t *testing.T) {
	b := newTestChain(t, "chain-b")
	c := newTestChain(t, "chain-c")

	listener := startListener(t, b)
	// B has NO agreement about C; C nonetheless knows B's CA and endpoint.
	federate(t, c, b, listener.URL, []string{"*"}, 4, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := c.mgr.QueryPeer(ctx, b.chainID, &QueryRequest{Mode: ModeText, Query: "anything", DomainTag: "x"}); err == nil {
		t.Fatal("stranger chain passed the mTLS handshake")
	}
}

// --- e2e: receipt exchange (Mode-2 cross-anchor) -------------------------------

func TestReceiptExchangeEndToEnd(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")

	comet, captured := fakeComet(t)
	b.mgr.cometRPC = comet.URL

	sharedID := hex.EncodeToString(sha256Bytes("shared-envelope"))
	core := sha256Bytes("canonical-core")
	coauthors := []tx.CoCommitCoauthor{
		{PubKey: a.agentPub, ChainID: a.chainID, Sig: make([]byte, ed25519.SignatureSize)},
		{PubKey: b.agentPub, ChainID: b.chainID, Sig: make([]byte, ed25519.SignatureSize)},
	}
	seedCoCommit(t, a, sharedID, core, coauthors)
	seedCoCommit(t, b, sharedID, core, coauthors)

	listener := startListener(t, b)
	federate(t, b, a, "https://unused.invalid", []string{"shared"}, 2, 0)
	federate(t, a, b, listener.URL, []string{"shared"}, 2, 0)

	push, err := a.mgr.BuildSignedReceipt(sharedID, 7, 1751400000)
	if err != nil {
		t.Fatalf("BuildSignedReceipt: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := a.mgr.PushReceipt(ctx, b.chainID, push)
	if err != nil {
		t.Fatalf("PushReceipt: %v", err)
	}
	if resp.Status != "anchored" || resp.TxHash != "CAFE" {
		t.Fatalf("unexpected push response: %+v", resp)
	}

	// The broadcast tx must be a well-formed CoCommitAttest bound to A.
	if len(*captured) != 1 {
		t.Fatalf("expected 1 broadcast tx, got %d", len(*captured))
	}
	ptx, err := tx.DecodeTx((*captured)[0])
	if err != nil {
		t.Fatalf("decode broadcast tx: %v", err)
	}
	if ptx.Type != tx.TxTypeCoCommitAttest || ptx.CoCommitAttest == nil {
		t.Fatalf("broadcast tx is not a CoCommitAttest: %+v", ptx.Type)
	}
	att := ptx.CoCommitAttest
	if att.SharedID != sharedID || att.PeerChainID != a.chainID {
		t.Errorf("attest binds wrong identity: %+v", att)
	}
	if hex.EncodeToString(att.PeerPubKey) != hex.EncodeToString(a.agentPub) {
		t.Errorf("attest PeerPubKey is not A's declared coauthor key")
	}
	if !ed25519.Verify(a.agentPub, att.Receipt, att.PeerSig) {
		t.Error("attest PeerSig does not verify over the verbatim receipt bytes")
	}
	receipt, err := tx.DecodeCommitReceipt(att.Receipt)
	if err != nil {
		t.Fatalf("receipt undecodable: %v", err)
	}
	if receipt.ChainID != a.chainID || receipt.Height != 7 || string(receipt.CoreHash) != string(core) {
		t.Errorf("receipt fields wrong: %+v", receipt)
	}

	// Idempotency: with the anchor already on-chain, a re-push is a no-op.
	anchor := sha256.Sum256(push.Receipt)
	if err := b.badger.SetCoCommitAnchor(sharedID, a.chainID, anchor[:]); err != nil {
		t.Fatal(err)
	}
	resp2, err := a.mgr.PushReceipt(ctx, b.chainID, push)
	if err != nil {
		t.Fatalf("re-push: %v", err)
	}
	if resp2.Status != "already_anchored" {
		t.Fatalf("re-push status = %q, want already_anchored", resp2.Status)
	}
	if len(*captured) != 1 {
		t.Fatalf("idempotent re-push broadcast a duplicate tx")
	}
}

func TestReceiptRejectsMismatchedChannel(t *testing.T) {
	a := newTestChain(t, "chain-a")
	b := newTestChain(t, "chain-b")

	sharedID := hex.EncodeToString(sha256Bytes("envelope-2"))
	core := sha256Bytes("core-2")
	coauthors := []tx.CoCommitCoauthor{
		{PubKey: a.agentPub, ChainID: a.chainID, Sig: make([]byte, ed25519.SignatureSize)},
		{PubKey: b.agentPub, ChainID: b.chainID, Sig: make([]byte, ed25519.SignatureSize)},
	}
	seedCoCommit(t, a, sharedID, core, coauthors)
	seedCoCommit(t, b, sharedID, core, coauthors)

	push, err := a.mgr.BuildSignedReceipt(sharedID, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	// Receipt authored by chain-a arriving over a channel authenticated as
	// chain-x must be refused (a compromised peer cannot relay third-party
	// receipts as its own).
	if _, err := b.mgr.HandleIncomingReceipt("chain-x", push); err == nil {
		t.Fatal("receipt accepted over a mismatched authenticated channel")
	}
	// A receipt whose signer is not a declared coauthor must be refused.
	forgedSig := ed25519.Sign(b.agentKey, push.Receipt) // wrong signer for chain-a
	if _, err := b.mgr.HandleIncomingReceipt(a.chainID, &ReceiptPush{Receipt: push.Receipt, ValSig: forgedSig}); err == nil {
		t.Fatal("receipt with non-coauthor signature accepted")
	}
}

func TestBuildSignedReceiptRequiresDeclaredCoauthor(t *testing.T) {
	a := newTestChain(t, "chain-a")
	sharedID := hex.EncodeToString(sha256Bytes("envelope-3"))
	core := sha256Bytes("core-3")
	// Coauthor set does NOT include A's operator key.
	other, _, _ := auth.GenerateKeypair()
	seedCoCommit(t, a, sharedID, core, []tx.CoCommitCoauthor{
		{PubKey: other, ChainID: a.chainID, Sig: make([]byte, ed25519.SignatureSize)},
	})
	if _, err := a.mgr.BuildSignedReceipt(sharedID, 0, 0); err == nil {
		t.Fatal("receipt built with an undeclared signing key")
	}
}

func TestReplayCacheRejectsDuplicates(t *testing.T) {
	a := newTestChain(t, "chain-a")
	ts := time.Now().Unix()
	if !a.mgr.replayFresh("chain-b:agent:sig1", ts) {
		t.Fatal("first sighting rejected")
	}
	if a.mgr.replayFresh("chain-b:agent:sig1", ts) {
		t.Fatal("replay accepted")
	}
	if !a.mgr.replayFresh("chain-c:agent:sig1", ts) {
		t.Fatal("chain-scoping broken: different chain's identical sig rejected")
	}
}

func sha256Bytes(s string) []byte {
	h := sha256.Sum256([]byte(s))
	return h[:]
}

// Guard against JSON round-trip surprises on the wire types (byte slices are
// base64 in encoding/json — make sure a push survives).
func TestReceiptPushJSONRoundTrip(t *testing.T) {
	in := &ReceiptPush{Receipt: []byte{1, 2, 3}, ValSig: make([]byte, 64), SignerPubKey: make([]byte, 32)}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out ReceiptPush
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if string(out.Receipt) != string(in.Receipt) || len(out.ValSig) != 64 || len(out.SignerPubKey) != 32 {
		t.Fatalf("round trip mangled the push: %+v", out)
	}
}
