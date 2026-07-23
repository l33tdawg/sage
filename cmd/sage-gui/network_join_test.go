package main

import (
	"database/sql"
	"encoding/base64"
	"os"
	"path/filepath"
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

func TestResetProjectionReceiptsForJoinPreservesUserData(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "sage.db")
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	if _, err := db.Exec(`
		CREATE TABLE memories (memory_id TEXT PRIMARY KEY, content TEXT NOT NULL);
		CREATE TABLE abci_projection_batches (
			block_height INTEGER PRIMARY KEY,
			app_hash BLOB NOT NULL
		);
		INSERT INTO memories(memory_id, content) VALUES ('keep-me', 'local memory');
		INSERT INTO abci_projection_batches(block_height, app_hash) VALUES (15, zeroblob(32));
	`); err != nil {
		_ = db.Close()
		t.Fatalf("seed SQLite: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close SQLite: %v", err)
	}

	if err := resetProjectionReceiptsForJoin(dbPath); err != nil {
		t.Fatalf("reset receipts: %v", err)
	}

	db, err = sql.Open("sqlite", dbPath)
	if err != nil {
		t.Fatalf("reopen SQLite: %v", err)
	}
	defer func() { _ = db.Close() }()
	var memories, receipts int
	if err := db.QueryRow(`SELECT COUNT(*) FROM memories`).Scan(&memories); err != nil {
		t.Fatalf("count memories: %v", err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM abci_projection_batches`).Scan(&receipts); err != nil {
		t.Fatalf("count receipts: %v", err)
	}
	if memories != 1 {
		t.Fatalf("memories = %d, want 1", memories)
	}
	if receipts != 0 {
		t.Fatalf("projection receipts = %d, want 0", receipts)
	}
}

func TestResetProjectionReceiptsForJoinAcceptsFreshNode(t *testing.T) {
	if err := resetProjectionReceiptsForJoin(filepath.Join(t.TempDir(), "missing.db")); err != nil {
		t.Fatalf("fresh node reset: %v", err)
	}
}

func TestValidateNodeJoinCompatibilityRejectsDifferentDevBuild(t *testing.T) {
	b := NodeJoinBundle{
		AppVersion:   version,
		BinarySHA256: "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
	}
	if err := validateNodeJoinCompatibility(b); err == nil {
		t.Fatal("expected a different executable fingerprint to be rejected")
	}
}

func TestValidateNodeJoinCompatibilityAcceptsRunningBuild(t *testing.T) {
	sha, err := runningBinarySHA256()
	if err != nil {
		t.Fatalf("fingerprint running test binary: %v", err)
	}
	if err := validateNodeJoinCompatibility(NodeJoinBundle{AppVersion: version, BinarySHA256: sha}); err != nil {
		t.Fatalf("running build rejected: %v", err)
	}
}
