package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	cmttypes "github.com/cometbft/cometbft/types"
	cmttime "github.com/cometbft/cometbft/types/time"
)

func TestMintChainID_ShapeAndLength(t *testing.T) {
	pk := []byte("0123456789abcdef0123456789abcdef") // 32-byte fake ed25519 pubkey
	id, err := mintChainID("sage-personal", [][]byte{pk}, time.Unix(1_700_000_000, 0))
	if err != nil {
		t.Fatalf("mintChainID: %v", err)
	}
	if len(id) > maxChainIDLen {
		t.Fatalf("id %q length %d exceeds MaxChainIDLen %d", id, len(id), maxChainIDLen)
	}
	prefix := "sage-personal-"
	if !strings.HasPrefix(id, prefix) {
		t.Fatalf("id %q missing prefix %q", id, prefix)
	}
	suffix := strings.TrimPrefix(id, prefix)
	if len(suffix) != chainIDSuffixLen {
		t.Fatalf("suffix %q length %d, want %d", suffix, len(suffix), chainIDSuffixLen)
	}
	// Suffix must be lowercase base32 (a-z, 2-7) so it survives CometBFT p2p/genesis validation.
	for _, r := range suffix {
		if (r < 'a' || r > 'z') && (r < '2' || r > '7') {
			t.Fatalf("suffix %q has non-base32 char %q", suffix, r)
		}
	}
}

func TestMintChainID_UniquePerCall(t *testing.T) {
	pk := []byte("0123456789abcdef0123456789abcdef")
	gt := time.Unix(1_700_000_000, 0)
	id1, err := mintChainID("sage-quorum", [][]byte{pk}, gt)
	if err != nil {
		t.Fatal(err)
	}
	id2, err := mintChainID("sage-quorum", [][]byte{pk}, gt)
	if err != nil {
		t.Fatal(err)
	}
	// Identical validator set + genesis time must STILL differ thanks to the 16-byte salt.
	if id1 == id2 {
		t.Fatalf("two mints with identical inputs collided: %q", id1)
	}
}

func TestSortedPubkeyBytes_OrderIndependent(t *testing.T) {
	a := []byte("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	b := []byte("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")
	c := []byte("cccccccccccccccccccccccccccccccc")

	// The order-independent digest material MUST be byte-identical regardless of
	// input order — this is what makes two founders derive the same chain_id from
	// the same validator set. (The final mintChainID id mixes in random salt, so
	// it can't be compared across orderings; this helper can.) Mutation guard:
	// deleting the sort.Slice in sortedPubkeyBytes makes this fail.
	abc := sortedPubkeyBytes([][]byte{a, b, c})
	cba := sortedPubkeyBytes([][]byte{c, b, a})
	bca := sortedPubkeyBytes([][]byte{b, c, a})
	if !bytes.Equal(abc, cba) || !bytes.Equal(abc, bca) {
		t.Fatalf("sorted pubkey material must be order-independent:\n abc=%x\n cba=%x\n bca=%x", abc, cba, bca)
	}
	// And it must actually contain all the material (len = sum of inputs).
	if len(abc) != len(a)+len(b)+len(c) {
		t.Fatalf("sorted material length %d, want %d", len(abc), len(a)+len(b)+len(c))
	}
}

func TestReadChainIDFromGenesis_RoundTrip(t *testing.T) {
	cometHome := t.TempDir()
	configDir := filepath.Join(cometHome, "config")
	if err := os.MkdirAll(configDir, 0700); err != nil {
		t.Fatal(err)
	}

	want := "sage-personal-abcdefghij234567abcd"
	// A minimal but valid genesis: ValidateAndComplete needs a validator set.
	gen := cmttypes.GenesisDoc{
		ChainID:         want,
		GenesisTime:     cmttime.Now(),
		ConsensusParams: cmttypes.DefaultConsensusParams(),
	}
	if err := gen.SaveAs(filepath.Join(configDir, "genesis.json")); err != nil {
		t.Fatalf("save genesis: %v", err)
	}

	got, err := readChainIDFromGenesis(cometHome)
	if err != nil {
		t.Fatalf("readChainIDFromGenesis: %v", err)
	}
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func TestReadChainIDFromGenesis_Missing(t *testing.T) {
	if _, err := readChainIDFromGenesis(t.TempDir()); err == nil {
		t.Fatal("expected error for missing genesis.json, got nil")
	}
}
