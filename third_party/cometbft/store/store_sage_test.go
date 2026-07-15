package store

import (
	"crypto/sha256"
	"encoding/binary"
	"testing"
	"time"

	dbm "github.com/cometbft/cometbft-db"
	cmted25519 "github.com/cometbft/cometbft/crypto/ed25519"
	cmtproto "github.com/cometbft/cometbft/proto/tendermint/types"
	"github.com/cometbft/cometbft/types"
	"github.com/stretchr/testify/require"
)

func sageStateSyncBootstrapTestCommit(t *testing.T, height int64) *types.Commit {
	t.Helper()
	blockHash := sha256.Sum256([]byte("state-sync block"))
	partsHash := sha256.Sum256([]byte("state-sync parts"))
	blockID := types.BlockID{
		Hash:          blockHash[:],
		PartSetHeader: types.PartSetHeader{Total: 1, Hash: partsHash[:]},
	}
	privateKey := cmted25519.GenPrivKey()
	vote := &types.Vote{
		Type:             cmtproto.PrecommitType,
		Height:           height,
		Round:            0,
		BlockID:          blockID,
		Timestamp:        time.Unix(1, 0).UTC(),
		ValidatorAddress: privateKey.PubKey().Address(),
		ValidatorIndex:   0,
	}
	signature, err := privateKey.Sign(types.VoteSignBytes("sage-state-sync-recovery-test", vote.ToProto()))
	require.NoError(t, err)
	vote.Signature = signature
	return &types.Commit{
		Height:     height,
		BlockID:    blockID,
		Signatures: []types.CommitSig{vote.CommitSig()},
	}
}

func TestStateSyncBootstrapCompleteRoundTrip(t *testing.T) {
	db := dbm.NewMemDB()
	blockStore := NewBlockStore(db)

	height, appHash, err := blockStore.LoadStateSyncBootstrapComplete()
	require.NoError(t, err)
	require.Zero(t, height)
	require.Nil(t, appHash)

	want := sha256.Sum256([]byte("state-sync bootstrap"))
	require.NoError(t, blockStore.SaveStateSyncBootstrapComplete(42, want[:]))
	height, appHash, err = blockStore.LoadStateSyncBootstrapComplete()
	require.NoError(t, err)
	require.Equal(t, int64(42), height)
	require.Equal(t, want[:], appHash)

	appHash[0] ^= 0xff
	_, loadedAgain, err := blockStore.LoadStateSyncBootstrapComplete()
	require.NoError(t, err)
	require.Equal(t, want[:], loadedAgain, "callers receive a private AppHash copy")
}

func TestStateSyncBootstrapCompleteRejectsInvalidWrites(t *testing.T) {
	blockStore := NewBlockStore(dbm.NewMemDB())
	require.ErrorContains(t, blockStore.SaveStateSyncBootstrapComplete(0, make([]byte, sha256.Size)), "positive")
	require.ErrorContains(t, blockStore.SaveStateSyncBootstrapComplete(1, []byte("short")), "32 bytes")

	height, appHash, err := blockStore.LoadStateSyncBootstrapComplete()
	require.NoError(t, err)
	require.Zero(t, height)
	require.Nil(t, appHash)
}

func TestStateSyncBootstrapCompleteRejectsMalformedRecords(t *testing.T) {
	tests := map[string][]byte{
		"short": []byte("short"),
		"magic": make([]byte, stateSyncBootstrapCompleteValueSize),
	}
	versionOffset := len(stateSyncBootstrapCompleteMagic)
	valid := make([]byte, stateSyncBootstrapCompleteValueSize)
	copy(valid, stateSyncBootstrapCompleteMagic)
	valid[versionOffset] = 1
	binary.BigEndian.PutUint64(valid[versionOffset+1:], 1)
	tests["version"] = append([]byte(nil), valid...)
	tests["version"][versionOffset] = 2
	tests["zero height"] = append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(tests["zero height"][versionOffset+1:], 0)
	tests["overflow height"] = append([]byte(nil), valid...)
	binary.BigEndian.PutUint64(tests["overflow height"][versionOffset+1:], uint64(1)<<63)

	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			db := dbm.NewMemDB()
			require.NoError(t, db.SetSync(stateSyncBootstrapCompleteKey, encoded))
			_, _, err := NewBlockStore(db).LoadStateSyncBootstrapComplete()
			require.Error(t, err)
		})
	}
}

func TestRecoverIncompleteStateSyncBootstrapRemovesOnlyExactSeenCommit(t *testing.T) {
	db := dbm.NewMemDB()
	blockStore := NewBlockStore(db)
	require.NoError(t, blockStore.SaveSeenCommit(42, sageStateSyncBootstrapTestCommit(t, 42)))
	recoverable, err := IsIncompleteStateSyncBootstrapDB(db)
	require.NoError(t, err)
	require.True(t, recoverable)
	require.NotNil(t, blockStore.LoadSeenCommit(42), "inspection never removes the commit")

	recovered, err := blockStore.RecoverIncompleteStateSyncBootstrap()
	require.NoError(t, err)
	require.True(t, recovered)
	require.Nil(t, blockStore.LoadSeenCommit(42))
	iterator, err := db.Iterator(nil, nil)
	require.NoError(t, err)
	require.False(t, iterator.Valid())
	require.NoError(t, iterator.Close())
}

func TestRecoverIncompleteStateSyncBootstrapPreservesAmbiguousData(t *testing.T) {
	tests := map[string]func(dbm.DB){
		"additional key": func(db dbm.DB) {
			require.NoError(t, db.SetSync([]byte("other"), []byte("value")))
		},
		"malformed commit": func(db dbm.DB) {
			require.NoError(t, db.SetSync(calcSeenCommitKey(42), []byte("not a commit")))
		},
		"canonical all-absent commit": func(db dbm.DB) {
			commit := sageStateSyncBootstrapTestCommit(t, 42)
			commit.Signatures = []types.CommitSig{types.NewCommitSigAbsent()}
			require.NoError(t, NewBlockStore(db).SaveSeenCommit(42, commit))
		},
		"canonical short commit signature": func(db dbm.DB) {
			commit := sageStateSyncBootstrapTestCommit(t, 42)
			commit.Signatures[0].Signature = []byte{0x01}
			require.NoError(t, NewBlockStore(db).SaveSeenCommit(42, commit))
		},
		"noncanonical commit with unknown field": func(db dbm.DB) {
			canonical, err := db.Get(calcSeenCommitKey(42))
			require.NoError(t, err)
			// Valid unknown protobuf field 99 (wire type 0, value 1). A decoder
			// may ignore it, but this byte sequence was not emitted by
			// SaveSeenCommit and must remain untouched.
			noncanonical := append(append([]byte(nil), canonical...), 0x98, 0x06, 0x01)
			require.NoError(t, db.SetSync(calcSeenCommitKey(42), noncanonical))
		},
	}
	for name, contaminate := range tests {
		t.Run(name, func(t *testing.T) {
			db := dbm.NewMemDB()
			blockStore := NewBlockStore(db)
			require.NoError(t, blockStore.SaveSeenCommit(42, sageStateSyncBootstrapTestCommit(t, 42)))
			contaminate(db)
			recoverable, err := IsIncompleteStateSyncBootstrapDB(db)
			require.NoError(t, err)
			require.False(t, recoverable)
			recovered, err := blockStore.RecoverIncompleteStateSyncBootstrap()
			require.NoError(t, err)
			require.False(t, recovered)
			seen, err := db.Get(calcSeenCommitKey(42))
			require.NoError(t, err)
			require.NotEmpty(t, seen)
		})
	}
}

func TestRecoverIncompleteStateSyncBootstrapDBPreservesMalformedMetadataWithoutPanic(t *testing.T) {
	db := dbm.NewMemDB()
	require.NoError(t, db.SetSync(blockStoreKey, []byte("malformed block-store metadata")))

	recovered, err := RecoverIncompleteStateSyncBootstrapDB(db)
	require.NoError(t, err)
	require.False(t, recovered)
	encoded, err := db.Get(blockStoreKey)
	require.NoError(t, err)
	require.Equal(t, []byte("malformed block-store metadata"), encoded)
}
