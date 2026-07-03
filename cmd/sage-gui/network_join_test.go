package main

import (
	"encoding/base64"
	"os"
	"testing"
	"time"

	"github.com/cometbft/cometbft/crypto/ed25519"
	cmttypes "github.com/cometbft/cometbft/types"
)

// testGenesisJSON builds a valid genesis document (single validator) with the
// given chain_id and returns its canonical JSON bytes.
func testGenesisJSON(t *testing.T, chainID string) []byte {
	t.Helper()
	pk := ed25519.GenPrivKey().PubKey()
	gen := cmttypes.GenesisDoc{
		ChainID:         chainID,
		GenesisTime:     time.Unix(1700000000, 0).UTC(),
		ConsensusParams: cmttypes.DefaultConsensusParams(),
		Validators: []cmttypes.GenesisValidator{{
			Address: pk.Address(),
			PubKey:  pk,
			Power:   10,
			Name:    "host",
		}},
	}
	if err := gen.ValidateAndComplete(); err != nil {
		t.Fatalf("build genesis: %v", err)
	}
	dir := t.TempDir()
	path := dir + "/genesis.json"
	if err := gen.SaveAs(path); err != nil {
		t.Fatalf("save genesis: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read genesis: %v", err)
	}
	return data
}

const goodHostPeer = "0123456789abcdef0123456789abcdef01234567@192.168.1.5:26656"

func TestValidateNodeJoinBundle_Valid(t *testing.T) {
	gen := testGenesisJSON(t, "sage-quorum-abc123")
	b := NodeJoinBundle{
		ChainID:    "sage-quorum-abc123",
		GenesisB64: base64.StdEncoding.EncodeToString(gen),
		HostPeer:   goodHostPeer,
	}
	out, err := validateNodeJoinBundle(b)
	if err != nil {
		t.Fatalf("expected valid, got %v", err)
	}
	if len(out) != len(gen) {
		t.Fatalf("returned genesis bytes mismatch")
	}
}

func TestValidateNodeJoinBundle_Rejects(t *testing.T) {
	gen := testGenesisJSON(t, "sage-quorum-abc123")
	genB64 := base64.StdEncoding.EncodeToString(gen)

	cases := []struct {
		name string
		b    NodeJoinBundle
	}{
		{"empty chain_id", NodeJoinBundle{ChainID: "", GenesisB64: genB64, HostPeer: goodHostPeer}},
		{"host_peer no @", NodeJoinBundle{ChainID: "x", GenesisB64: genB64, HostPeer: "0123456789abcdef0123456789abcdef01234567_192.168.1.5:26656"}},
		{"host_peer bad node id", NodeJoinBundle{ChainID: "x", GenesisB64: genB64, HostPeer: "shortid@192.168.1.5:26656"}},
		{"host_peer non-ip host", NodeJoinBundle{ChainID: "x", GenesisB64: genB64, HostPeer: "0123456789abcdef0123456789abcdef01234567@example.com:26656"}},
		{"host_peer no port", NodeJoinBundle{ChainID: "x", GenesisB64: genB64, HostPeer: "0123456789abcdef0123456789abcdef01234567@192.168.1.5"}},
		{"bad base64", NodeJoinBundle{ChainID: "x", GenesisB64: "!!!not base64!!!", HostPeer: goodHostPeer}},
		{"unparseable genesis", NodeJoinBundle{ChainID: "x", GenesisB64: base64.StdEncoding.EncodeToString([]byte("{not genesis")), HostPeer: goodHostPeer}},
		{"chain_id mismatch", NodeJoinBundle{ChainID: "sage-quorum-DIFFERENT", GenesisB64: genB64, HostPeer: goodHostPeer}},
	}
	for _, c := range cases {
		if _, err := validateNodeJoinBundle(c.b); err == nil {
			t.Errorf("%s: expected error, got nil", c.name)
		}
	}
}
