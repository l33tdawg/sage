//go:build integration

package integration

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	"github.com/cometbft/cometbft/privval"
	cmttypes "github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

func freshV30VoteKeyFixture(t *testing.T) (*cmttypes.GenesisDoc, string, []ed25519.PrivateKey) {
	t.Helper()
	directory := filepath.Join(t.TempDir(), "vote-keys")
	require.NoError(t, os.Mkdir(directory, 0o700))
	genesis := &cmttypes.GenesisDoc{}
	keys := make([]ed25519.PrivateKey, 0, cometRPCCount)
	for node := 0; node < cometRPCCount; node++ {
		_, key, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		validator := cmted25519.PrivKey(key)
		privval.NewFilePV(validator,
			filepath.Join(directory, fmt.Sprintf("node%d.json", node)),
			filepath.Join(directory, fmt.Sprintf("state%d.json", node))).Save()
		genesis.Validators = append(genesis.Validators, cmttypes.GenesisValidator{
			PubKey: validator.PubKey(), Power: 1,
		})
		keys = append(keys, key)
	}
	return genesis, directory, keys
}

func TestFreshV30ValidatorKeyExportBindsFullRoster(t *testing.T) {
	genesis, directory, want := freshV30VoteKeyFixture(t)
	// Roster order is public configuration, not a mapping from key export names.
	genesis.Validators[0], genesis.Validators[3] = genesis.Validators[3], genesis.Validators[0]
	got, err := readFreshV30ValidatorKeys(genesis, directory)
	require.NoError(t, err)
	require.Len(t, got, len(want))
	for node := range want {
		require.True(t, bytes.Equal(want[node], got[node]), "exported validator%d key differs", node)
	}
	info, err := os.Stat(directory)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o700), info.Mode().Perm())
	for node, key := range got {
		info, err := os.Stat(filepath.Join(directory, fmt.Sprintf("node%d.json", node)))
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), info.Mode().Perm())
		message := []byte("fresh-v30 explicit fixture governance vote")
		require.True(t, ed25519.Verify(key.Public().(ed25519.PublicKey), message, ed25519.Sign(key, message)))
	}
}

func TestFreshV30ValidatorKeyExportRejectsWrongBindings(t *testing.T) {
	for _, name := range []string{
		"missing key", "duplicate key", "foreign key", "public mismatch",
		"seed mismatch", "incomplete roster", "duplicate roster", "no export directory",
	} {
		t.Run(name, func(t *testing.T) {
			genesis, directory, _ := freshV30VoteKeyFixture(t)
			path := filepath.Join(directory, "node0.json")
			switch name {
			case "missing key":
				require.NoError(t, os.Remove(path))
			case "duplicate key":
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(filepath.Join(directory, "node1.json"), raw, 0o600))
			case "foreign key":
				_, key, err := ed25519.GenerateKey(rand.Reader)
				require.NoError(t, err)
				privval.NewFilePV(cmted25519.PrivKey(key), path, filepath.Join(directory, "foreign-state.json")).Save()
			case "public mismatch", "seed mismatch":
				raw, err := os.ReadFile(path)
				require.NoError(t, err)
				var document map[string]json.RawMessage
				require.NoError(t, json.Unmarshal(raw, &document))
				field := "pub_key"
				if name == "seed mismatch" {
					field = "priv_key"
				}
				var keyField map[string]string
				require.NoError(t, json.Unmarshal(document[field], &keyField))
				decoded, err := base64.StdEncoding.DecodeString(keyField["value"])
				require.NoError(t, err)
				decoded[0] ^= 1
				keyField["value"] = base64.StdEncoding.EncodeToString(decoded)
				document[field], err = json.Marshal(keyField)
				require.NoError(t, err)
				raw, err = json.Marshal(document)
				require.NoError(t, err)
				require.NoError(t, os.WriteFile(path, raw, 0o600))
			case "incomplete roster":
				genesis.Validators = genesis.Validators[:3]
			case "duplicate roster":
				genesis.Validators[1] = genesis.Validators[0]
			case "no export directory":
				directory = ""
			}
			keys, err := readFreshV30ValidatorKeys(genesis, directory)
			require.Error(t, err)
			require.Nil(t, keys)
		})
	}
}
