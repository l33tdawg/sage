package abci

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

const appV30ReplayChain = "app-v30-committed-replay"

type appV30ReplayDisk struct {
	canonicalPath  string
	projectionPath string
	app            *SageApp
}

func (disk *appV30ReplayDisk) reopen(t *testing.T) {
	t.Helper()
	if disk.app != nil {
		require.NoError(t, disk.app.Close())
		disk.app = nil
	}
	canonical, err := store.NewBadgerStore(disk.canonicalPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), disk.projectionPath)
	if err != nil {
		_ = canonical.CloseBadger()
		require.NoError(t, err)
	}
	disk.app, err = NewSageAppWithStores(canonical, projection, zerolog.Nop())
	if err != nil {
		_ = projection.Close()
		_ = canonical.CloseBadger()
		require.NoError(t, err)
	}
	// The real process restores this binding from its immutable Comet config;
	// the canonical scope digest alone cannot reconstruct a chain ID.
	require.NoError(t, disk.app.SetExpectedGovernanceDelegationDomain(appV30ReplayChain))
}

// Capture every visible key and value, including height/AppHash bookkeeping.
// This is stronger than comparing only the AppHash or selected namespaces.
func appV30ReplayKeyspace(t *testing.T, app *SageApp) map[string][]byte {
	t.Helper()
	entries := make(map[string][]byte)
	require.NoError(t, app.badgerStore.DB().View(func(transaction *badger.Txn) error {
		iterator := transaction.NewIterator(badger.DefaultIteratorOptions)
		defer iterator.Close()
		for iterator.Rewind(); iterator.Valid(); iterator.Next() {
			item := iterator.Item()
			value, err := item.ValueCopy(nil)
			if err != nil {
				return err
			}
			entries[string(item.KeyCopy(nil))] = value
		}
		return nil
	}))
	return entries
}

func appV30ReplaySignedTx(t *testing.T, signer agentKey, parsed *tx.ParsedTx, nonce uint64, height int64) []byte {
	t.Helper()
	parsed.Nonce = nonce
	parsed.Timestamp = time.Unix(1_800_000_000+height, 0).UTC()
	// Same-key envelopes use the ordinary direct proof contract. Both layers
	// are signed once, before any replay, with a fixed authenticated timestamp.
	bodyHash := sha256.Sum256([]byte("app-v30 replay fixture proof"))
	parsed.AgentPubKey = append([]byte(nil), signer.pub...)
	parsed.AgentBodyHash = bodyHash[:]
	parsed.AgentTimestamp = parsed.Timestamp.Unix()
	message := binary.BigEndian.AppendUint64(append([]byte(nil), bodyHash[:]...), uint64(parsed.AgentTimestamp))
	parsed.AgentSig = ed25519.Sign(signer.priv, message)
	require.NoError(t, tx.SignTx(parsed, signer.priv))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	decoded, err := tx.DecodeTx(raw)
	require.NoError(t, err)
	if parsed.Type == tx.TxTypeMemorySubmit {
		// DecodeTx deliberately leaves versioned tails dormant. Restore the
		// signed fields here only for this independent cryptographic check;
		// FinalizeBlock must still choose the real gate at each tested height.
		require.NoError(t, tx.ActivateMemorySubmitExtensions(decoded, true, true))
	}
	valid, err := tx.VerifyTx(decoded)
	require.NoError(t, err)
	require.True(t, valid)
	roundTrip, err := tx.EncodeTx(decoded)
	require.NoError(t, err)
	require.Equal(t, raw, roundTrip, "activating parsed fields must preserve the exact signed wire bytes")
	return raw
}

func appV30ReplaySubmit(t *testing.T, signer agentKey, id string, parents []tx.ParentRef, nonce uint64, height int64) []byte {
	t.Helper()
	contentHash := sha256.Sum256([]byte(id))
	return appV30ReplaySignedTx(t, signer, &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID: id, Content: id, ContentHash: contentHash[:],
			MemoryType: tx.MemoryTypeObservation, DomainTag: "general",
			Classification: tx.ClearanceInternal, ConfidenceScore: 0.8,
			DerivedFrom: parents,
		},
	}, nonce, height)
}

func appV30ReplayCommit(t *testing.T, app *SageApp, request *abcitypes.RequestFinalizeBlock) *abcitypes.ResponseFinalizeBlock {
	t.Helper()
	response, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, response.AppHash, app.state.AppHash)
	return response
}

// This is a committed replay proof, not a live-network or old-binary comparison.
// A real signed upgrade ballot creates the plan; no applied-height fixture or
// direct provenance/lineage write is used. Two independent disk restorations
// execute the identical signed blocks at H-1, H and H+1. One also discards each
// speculative FinalizeBlock and reopens before replaying and committing it.
func TestAppV30CommittedReplayBoundary(t *testing.T) {
	ctx := context.Background()
	signer := deterministicAgentKey(t)
	directory := t.TempDir()
	seed := &appV30ReplayDisk{canonicalPath: filepath.Join(directory, "source"), projectionPath: filepath.Join(directory, "source.db")}
	seed.reopen(t)
	t.Cleanup(func() {
		if seed.app != nil {
			require.NoError(t, seed.app.Close())
		}
	})
	manifest := FreshGenesisManifest{
		Version: FreshGenesisManifestVersion, AppVersion: 29, RootID: signer.id,
		Validators: []FreshGenesisValidator{{ValidatorID: signer.id, Power: 10}},
	}
	_, err := seed.app.InitChain(ctx, freshGenesisRequest(t, appV30ReplayChain, manifest, signer, nil))
	require.NoError(t, err)
	parent := tx.ParentRef{SourceChainID: appV30ReplayChain, MemoryID: "replay-parent", ContentHash: sha256.Sum256([]byte("replay-parent"))}
	first := appV30ReplayCommit(t, seed.app, &abcitypes.RequestFinalizeBlock{
		Height: 1, Time: time.Unix(1_800_000_001, 0).UTC(),
		Txs: [][]byte{
			appV30ReplaySubmit(t, signer, parent.MemoryID, nil, 1, 1),
			appV30ReplaySignedTx(t, signer, &tx.ParsedTx{Type: tx.TxTypeMemoryVote, MemoryVote: &tx.MemoryVote{MemoryID: parent.MemoryID, Decision: tx.VoteDecisionAccept}}, 2, 1),
			appV30ReplaySignedTx(t, signer, &tx.ParsedTx{Type: tx.TxTypeUpgradePropose, UpgradePropose: &tx.UpgradePropose{Name: appV30UpgradeName, TargetAppVersion: 30, ProposerID: signer.id, UpgradeDelayBlocks: defaultUpgradeDelayBlocks}}, 3, 1),
		},
	})
	require.Len(t, first.TxResults, 3)
	for _, result := range first.TxResults {
		require.Zero(t, result.Code, result.Log)
	}
	_, status, err := seed.app.badgerStore.GetMemoryHash(parent.MemoryID)
	require.NoError(t, err)
	require.Equal(t, "committed", status)
	plan, err := seed.app.badgerStore.GetUpgradePlan()
	require.NoError(t, err)
	require.Equal(t, int64(1)+defaultUpgradeDelayBlocks, plan.ActivationHeight)
	// Honor the production delay and sequential heights when making the one
	// durable baseline. The repeated boundary proof itself is just three blocks.
	for height := int64(2); height < plan.ActivationHeight-1; height++ {
		appV30ReplayCommit(t, seed.app, &abcitypes.RequestFinalizeBlock{Height: height, Time: time.Unix(1_800_000_000+height, 0).UTC()})
	}
	baseline := appV30ReplayKeyspace(t, seed.app)
	var backup bytes.Buffer
	require.NoError(t, statesync.WriteCanonicalState(ctx, seed.app.badgerStore.DB(), &backup))
	require.NoError(t, seed.app.Close())
	seed.app = nil
	projectionBytes, err := os.ReadFile(seed.projectionPath)
	require.NoError(t, err)
	newReplica := func(t *testing.T) *appV30ReplayDisk {
		t.Helper()
		path := t.TempDir()
		disk := &appV30ReplayDisk{canonicalPath: filepath.Join(path, "canonical"), projectionPath: filepath.Join(path, "projection.db")}
		canonical, openErr := store.NewBadgerStore(disk.canonicalPath)
		require.NoError(t, openErr)
		require.NoError(t, statesync.RestoreCanonicalState(ctx, bytes.NewReader(backup.Bytes()), canonical.DB()))
		require.NoError(t, canonical.CloseBadger())
		require.NoError(t, os.WriteFile(disk.projectionPath, projectionBytes, 0o600))
		disk.reopen(t)
		t.Cleanup(func() { require.NoError(t, disk.app.Close()) })
		require.Equal(t, baseline, appV30ReplayKeyspace(t, disk.app), "each replay starts from the identical durable key/value set")
		return disk
	}

	requests := make([]*abcitypes.RequestFinalizeBlock, 3)
	for i := range requests {
		height := plan.ActivationHeight - 1 + int64(i)
		requests[i] = &abcitypes.RequestFinalizeBlock{
			Height: height, Time: time.Unix(1_800_000_000+height, 0).UTC(),
			Txs: [][]byte{
				appV30ReplaySubmit(t, signer, "legacy-"+string(rune('a'+i)), nil, uint64(4+2*i), height),
				appV30ReplaySubmit(t, signer, "derived-"+string(rune('a'+i)), []tx.ParentRef{parent}, uint64(5+2*i), height),
			},
		}
	}
	firstReplay := newReplica(t)
	responses := make([]*abcitypes.ResponseFinalizeBlock, len(requests))
	keyspaces := make([]map[string][]byte, len(requests))
	for i, request := range requests {
		responses[i] = appV30ReplayCommit(t, firstReplay.app, request)
		require.Len(t, responses[i].TxResults, 2)
		require.Zero(t, responses[i].TxResults[0].Code, responses[i].TxResults[0].Log)
		if i < 2 {
			require.Equal(t, uint32(1), responses[i].TxResults[1].Code)
			require.Equal(t, "invalid memory submit extension", responses[i].TxResults[1].Log)
		} else {
			require.Zero(t, responses[i].TxResults[1].Code, responses[i].TxResults[1].Log)
		}
		if i == 1 {
			require.Equal(t, uint64(30), responses[i].ConsensusParamUpdates.Version.App)
		} else {
			require.Nil(t, responses[i].ConsensusParamUpdates)
		}
		keyspaces[i] = appV30ReplayKeyspace(t, firstReplay.app)
		t.Logf("committed boundary H=%d keys=%d AppHash=%x", request.Height, len(keyspaces[i]), responses[i].AppHash)
		firstReplay.reopen(t)
		require.Equal(t, keyspaces[i], appV30ReplayKeyspace(t, firstReplay.app), "reopening must preserve every committed key/value")
	}
	secondReplay := newReplica(t)
	for i, request := range requests {
		before := appV30ReplayKeyspace(t, secondReplay.app)
		staged, finalizeErr := secondReplay.app.FinalizeBlock(ctx, request)
		require.NoError(t, finalizeErr)
		require.Equal(t, responses[i], staged)
		require.Equal(t, before, appV30ReplayKeyspace(t, secondReplay.app), "FinalizeBlock alone must not mutate durable state")
		require.NotNil(t, secondReplay.app.pendingAppV20Finalize)
		secondReplay.app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
		secondReplay.app.pendingAppV20Finalize = nil
		secondReplay.reopen(t)
		replayed := appV30ReplayCommit(t, secondReplay.app, request)
		require.Equal(t, responses[i], replayed, "same signed bytes must reproduce the complete ABCI response")
		require.Equal(t, keyspaces[i], appV30ReplayKeyspace(t, secondReplay.app), "committed replay must reproduce every key and value, not only AppHash")
	}
	refs, err := secondReplay.app.badgerStore.GetMemoryLineage("derived-c")
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{parent}, refs)
	for i, id := range []string{"legacy-a", "legacy-b", "legacy-c", "derived-c"} {
		_, found, provenanceErr := secondReplay.app.badgerStore.GetAppV30Provenance(id)
		require.NoError(t, provenanceErr)
		require.Equal(t, i >= 2, found, "provenance must start strictly at H+1")
	}

	// A same-binary app29 control postpones only the pending plan. Before and
	// at H, ordinary tx results and ALL other canonical keys must stay equal.
	// Exclusions are the exact plan/applied records and their saved AppHash;
	// this deliberately does not claim equality to an independently built old binary.
	legacy := newReplica(t)
	postponed := *plan
	postponed.ActivationHeight += 1_000
	require.NoError(t, legacy.app.badgerStore.SetUpgradePlan(&postponed))
	for i, request := range requests[:2] {
		response := appV30ReplayCommit(t, legacy.app, request)
		require.Equal(t, responses[i].TxResults, response.TxResults)
		require.Nil(t, response.ConsensusParamUpdates)
		controlKeys := appV30ReplayKeyspace(t, legacy.app)
		boundaryKeys := make(map[string][]byte, len(keyspaces[i]))
		for key, value := range keyspaces[i] {
			boundaryKeys[key] = value
		}
		for _, key := range []string{"upgrade:plan", "upgrade:applied:app-v30", "state:app_hash"} {
			delete(controlKeys, key)
			delete(boundaryKeys, key)
		}
		require.Equal(t, controlKeys, boundaryKeys, "app-v30 activation must not reinterpret ordinary activation-block writes")
	}
}
