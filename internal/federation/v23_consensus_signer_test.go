package federation

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func bootstrapRotatedFederationRoot(
	t *testing.T,
	chainID string,
) (*testChain, string, string, ed25519.PrivateKey) {
	t.Helper()
	node := newTestChain(t, chainID)
	node.mgr.postV23ForNextTx = func() bool { return true }
	transportID := hex.EncodeToString(node.agentPub)
	companionPub, _, companionKeyErr := ed25519.GenerateKey(rand.Reader)
	if companionKeyErr != nil {
		t.Fatal(companionKeyErr)
	}
	if err := node.badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: transportID, Scope: node.chainID,
		AgentID: hex.EncodeToString(companionPub),
		Profile: store.AppV23ProfileStandard, HomeDomain: "companion.home",
		Clearance: 2, Height: 1, BootstrapDigest: strings.Repeat("a7", 32),
	}); err != nil {
		t.Fatal(err)
	}
	currentPub, currentKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	currentID := hex.EncodeToString(currentPub)
	if err := node.badger.RotateAppV23RootCredential(1, currentID, 2); err != nil {
		t.Fatal(err)
	}
	return node, transportID, currentID, currentKey
}

func testCrossFedTerms(remote string) *tx.CrossFedTerms {
	return &tx.CrossFedTerms{
		RemoteChainID: remote,
		Endpoint:      "https://peer.invalid",
		PeerPubKey:    bytes.Repeat([]byte{0x53}, ed25519.PublicKeySize),
		MaxClearance:  tx.ClearanceTopSecret,
		AllowedDomains: []string{
			"*",
		},
		Status: "active",
	}
}

func v23ConsensusTestSyncItem() *SyncItem {
	content := "federated copy uses local Root authority"
	contentHash := sha256.Sum256([]byte(content))
	return &SyncItem{
		ContentHash:     hex.EncodeToString(contentHash[:]),
		MemoryType:      "fact",
		Domain:          "shared.copy",
		Content:         content,
		Classification:  0,
		ConfidenceScore: 0.9,
	}
}

func assertFederationConsensusSigner(t *testing.T, parsed *tx.ParsedTx, want string) {
	t.Helper()
	if parsed == nil {
		t.Fatal("missing captured transaction")
		return
	}
	if got := hex.EncodeToString(parsed.PublicKey); got != want {
		t.Fatalf("outer signer = %s, want %s", got, want)
	}
	if got := hex.EncodeToString(parsed.AgentPubKey); got != want {
		t.Fatalf("embedded actor = %s, want %s", got, want)
	}
}

func TestAppV23FederationConsensusMutationsUseCurrentRootAndKeepTransportPin(t *testing.T) {
	node, transportID, currentID, currentKey := bootstrapRotatedFederationRoot(
		t, "v23-control-signer",
	)
	node.mgr.SetRootKeyResolver(func(credentialID string) (ed25519.PrivateKey, bool) {
		return currentKey, credentialID == currentID
	})

	var captured []*tx.ParsedTx
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		parsed, err := tx.DecodeTx(encoded)
		if err == nil {
			captured = append(captured, parsed)
		}
		return "captured", 3, err
	}

	terms := testCrossFedTerms("peer-v23-set")
	frozenPeerPin := append([]byte(nil), terms.PeerPubKey...)
	if _, err := node.mgr.broadcastCrossFedSet(terms); err != nil {
		t.Fatal(err)
	}
	if len(captured) != 1 {
		t.Fatalf("set broadcasts = %d, want 1", len(captured))
	}
	assertFederationConsensusSigner(t, captured[0], currentID)
	if !bytes.Equal(captured[0].CrossFedTerms.PeerPubKey, frozenPeerPin) {
		t.Fatal("tx-33 changed the peer transport pin while rotating local control authority")
	}

	if _, err := node.mgr.broadcastRevokeAgreementLockedReason(
		"peer-v23-revoke", "current Root test",
	); err != nil {
		t.Fatal(err)
	}
	if len(captured) != 2 {
		t.Fatalf("revoke broadcasts = %d, want 2 total", len(captured))
	}
	assertFederationConsensusSigner(t, captured[1], currentID)

	copyTx, err := node.mgr.buildSyncSubmitTx("copy-v23-root", v23ConsensusTestSyncItem())
	if err != nil {
		t.Fatal(err)
	}
	parsedCopy, err := tx.DecodeTx(copyTx)
	if err != nil {
		t.Fatal(err)
	}
	assertFederationConsensusSigner(t, parsedCopy, currentID)

	if got := hex.EncodeToString(node.mgr.agentPub); got != transportID {
		t.Fatalf("transport identity changed: got %s want %s", got, transportID)
	}
}

func TestAppV23FederationConsensusMutationsFailClosedWithoutCurrentRoot(t *testing.T) {
	node, transportID, currentID, _ := bootstrapRotatedFederationRoot(
		t, "v23-control-fail-closed",
	)
	// The vault has only the retired Root/transport key. It must never satisfy a
	// request for the replacement Root or be used as a compatibility fallback.
	node.mgr.SetRootKeyResolver(func(credentialID string) (ed25519.PrivateKey, bool) {
		return node.agentKey, credentialID == transportID
	})
	broadcasts := 0
	node.mgr.broadcastFn = func([]byte) (string, int64, error) {
		broadcasts++
		return "unexpected", 3, nil
	}

	if _, err := node.mgr.broadcastCrossFedSet(testCrossFedTerms("peer-stale-set")); err == nil ||
		!strings.Contains(err.Error(), currentID) {
		t.Fatalf("stale Root set error = %v, want current credential refusal", err)
	}
	if _, err := node.mgr.broadcastRevokeAgreementLockedReason(
		"peer-stale-revoke", "stale Root",
	); err == nil || !strings.Contains(err.Error(), currentID) {
		t.Fatalf("stale Root revoke error = %v, want current credential refusal", err)
	}
	if _, err := node.mgr.buildSyncSubmitTx("copy-stale-root", v23ConsensusTestSyncItem()); err == nil ||
		!strings.Contains(err.Error(), currentID) {
		t.Fatalf("stale Root copy error = %v, want current credential refusal", err)
	}
	if broadcasts != 0 {
		t.Fatalf("unavailable current Root still broadcast %d transactions", broadcasts)
	}

	// Even when genesis Root and transport happen to be the same bytes, the
	// explicit post-v23 resolver is mandatory for consensus control.
	genesisNode := newTestChain(t, "v23-control-no-resolver")
	genesisNode.mgr.postV23ForNextTx = func() bool { return true }
	companion, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := genesisNode.badger.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: hex.EncodeToString(genesisNode.agentPub), Scope: genesisNode.chainID,
		AgentID: hex.EncodeToString(companion), Profile: store.AppV23ProfileStandard,
		HomeDomain: "companion.home", Clearance: 2, Height: 1,
		BootstrapDigest: strings.Repeat("b8", 32),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := genesisNode.mgr.broadcastCrossFedSet(
		testCrossFedTerms("peer-no-resolver"),
	); err == nil || !strings.Contains(err.Error(), "resolver") {
		t.Fatalf("missing resolver error = %v, want fail-closed refusal", err)
	}
}

func TestPreAppV23FederationConsensusMutationsKeepLegacyTransportSigner(t *testing.T) {
	node := newTestChain(t, "legacy-control-signer")
	transportID := hex.EncodeToString(node.agentPub)
	var captured *tx.ParsedTx
	node.mgr.broadcastFn = func(encoded []byte) (string, int64, error) {
		var err error
		captured, err = tx.DecodeTx(encoded)
		return "captured", 1, err
	}
	if _, err := node.mgr.broadcastCrossFedSet(testCrossFedTerms("peer-legacy")); err != nil {
		t.Fatal(err)
	}
	assertFederationConsensusSigner(t, captured, transportID)

	copyTx, err := node.mgr.buildSyncSubmitTx("copy-legacy", v23ConsensusTestSyncItem())
	if err != nil {
		t.Fatal(err)
	}
	parsedCopy, err := tx.DecodeTx(copyTx)
	if err != nil {
		t.Fatal(err)
	}
	assertFederationConsensusSigner(t, parsedCopy, transportID)
}
