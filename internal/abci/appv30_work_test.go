package abci

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	badger "github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func makeMemoryTaintWorkTx(
	t *testing.T,
	root agentKey,
	work *tx.MemoryTaintWork,
	nonce uint64,
) *tx.ParsedTx {
	t.Helper()
	pubKey, sig, bodyHash, timestamp := signAgentProof(t, root, []byte("taint work"))
	parsed := &tx.ParsedTx{
		Type:            tx.TxTypeMemoryTaintWork,
		MemoryTaintWork: work,
		Nonce:           nonce,
		AgentPubKey:     pubKey,
		AgentSig:        sig,
		AgentBodyHash:   bodyHash,
		AgentTimestamp:  timestamp,
	}
	require.NoError(t, tx.SignTx(parsed, root.priv))
	return parsed
}

func TestMemoryTaintWorkGatesAndAdvancesExpansion(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	decisionID := strings.Repeat("e", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("taint work payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkExpand,
		DecisionID:   decisionID,
		Generation:   header.Generation,
		FrontierHead: header.FrontierHead,
		ParentRefID:  header.ParentRefID,
		Cursor:       header.LastEdge,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)

	app.appV30AppliedHeight = 30
	gated := app.processMemoryTaintWork(parsed, 20)
	require.Equal(t, uint32(10), gated.Code)
	app.appV30AppliedHeight = 19

	stale := *work
	stale.FrontierHead = 99
	staleResult := app.processMemoryTaintWork(
		makeMemoryTaintWorkTx(t, root, &stale, 2),
		20,
	)
	require.Equal(t, appV30TaintWorkStateCode, staleResult.Code)

	result := app.processMemoryTaintWork(parsed, 20)
	require.Equal(t, uint32(0), result.Code, result.Log)
	require.Equal(t, uint8(1), app.appV30WorkTxCount)
	header, found, err = app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusComplete, header.Status)
	require.Equal(t, uint64(1), header.FrontierHead)

	app.appV30WorkTxCount = maxAppV30TaintWorkPerBlock
	limited := app.processMemoryTaintWork(
		makeMemoryTaintWorkTx(t, root, work, 3),
		20,
	)
	require.Equal(t, appV30TaintWorkLimitCode, limited.Code)
}

func TestMemoryTaintWorkRunsInsideMeteredAppV30Stage(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	app.state.Height = 19
	decisionID := strings.Repeat("c", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("metered taint work payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkExpand,
		DecisionID:   decisionID,
		Generation:   1,
		FrontierHead: 0,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	working := app.cloneForAppV20Finalize(scoped)
	result, err := working.processAppV30MeteredTx(
		parsed,
		20,
		time.Unix(parsed.AgentTimestamp, 0).UTC(),
	)
	require.NoError(t, err)
	require.Equal(t, uint32(0), result.Code, result.Log)
	require.NoError(t, scoped.CommitConsensusTransaction())

	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusComplete, header.Status)
	require.Equal(t, uint64(1), header.FrontierHead)
	require.EqualValues(t, 1, working.appV30WorkTxCount)
	require.Zero(t, working.badgerStore.AppV30WriteStageStats())
}

func TestMemoryTaintCursorStaysSpeculativeUntilCommit(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	app.state.Height = 19
	targetState, err := app.badgerStore.GetMemoryDisclosureState(target.MemoryID)
	require.NoError(t, err)

	// Real canonical children force one full-budget execution to stop partway
	// through the target's reverse edges, rather than completing an empty graph.
	const childCount = 220
	children := make([]tx.ParentRef, 0, childCount)
	seed := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(seed.DiscardConsensusTransaction)
	for i := 0; i < childCount; i++ {
		childID := fmt.Sprintf("cursor-child-%03d", i)
		child := tx.ParentRef{
			SourceChainID: chainID,
			MemoryID:      childID,
			ContentHash:   sha256.Sum256([]byte(childID)),
		}
		require.NoError(t, seed.SetMemoryHash(childID, child.ContentHash[:], "committed"))
		require.NoError(t, seed.SetMemoryDomain(childID, targetState.Domain))
		require.NoError(t, seed.SetMemoryAuthor(childID, targetState.Author))
		require.NoError(t, seed.SetMemoryAuthorPrincipal(childID, targetState.AuthorPrincipal))
		require.NoError(t, seed.SetMemoryClassification(childID, targetState.Classification))
		require.NoError(t, seed.ValidateMemoryLineageParents(childID, chainID, []tx.ParentRef{target}))
		require.NoError(t, seed.SetMemoryLineage(childID, []tx.ParentRef{target}))
		children = append(children, child)
	}
	require.NoError(t, seed.CommitConsensusTransaction())

	decisionID := strings.Repeat("8", taint.DecisionIDBytes)
	payloadHash := sha256.Sum256([]byte("discard-and-replay taint work payload"))
	created, err := app.badgerStore.ApplyTaintDecision(chainID, decisionID, target, payloadHash)
	require.NoError(t, err)
	require.True(t, created)
	beforeRevision := mustTaintRevision(t, app.badgerStore)
	beforeHeader, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.Header{
		Action: taint.ActionApply, Status: taint.StatusExpanding,
		Generation: 1, FrontierTail: 1, Materialized: 1, PayloadHash: payloadHash,
	}, beforeHeader)

	// Typed scoped reads cover memberships and validate both directions of the
	// membership/index relation plus frontier generation, bounds and cardinality.
	assertState := func(reader *store.BadgerStore, header taint.Header, revision uint64, materializedChildren int) {
		t.Helper()
		actual, found, err := reader.GetTaintDecision(decisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, header, actual)
		require.Equal(t, revision, mustTaintRevision(t, reader))
		require.NoError(t, reader.ValidateTaintState())
		members, err := reader.GetEffectiveTaintDecisions(target.MemoryID)
		require.NoError(t, err)
		require.Equal(t, []string{decisionID}, members)
		for i, child := range children {
			members, err := reader.GetEffectiveTaintDecisions(child.MemoryID)
			require.NoError(t, err)
			if i < materializedChildren {
				require.Equal(t, []string{decisionID}, members, child.MemoryID)
			} else {
				require.Empty(t, members, child.MemoryID)
			}
		}
	}
	stateHash := func(reader *store.BadgerStore) []byte {
		t.Helper()
		hash, err := reader.ComputeAppHashExcludingBookkeeping()
		require.NoError(t, err)
		return hash
	}
	// This raw snapshot intentionally reads the owning durable DB only. DB()
	// does not expose a scoped transaction, so staged checks use typed reads.
	durableTaintBytes := func() map[string][]byte {
		t.Helper()
		values := make(map[string][]byte)
		require.NoError(t, app.badgerStore.DB().View(func(txn *badger.Txn) error {
			for _, prefix := range [][]byte{
				taint.DecisionPrefix(), []byte("tainttarget:"), []byte("taintm:"),
				[]byte("taintd:"), []byte("taintq:"), taint.RevisionKey(), taint.ActiveCountKey(),
			} {
				options := badger.DefaultIteratorOptions
				options.Prefix = prefix
				options.PrefetchValues = false
				iterator := txn.NewIterator(options)
				for iterator.Seek(prefix); iterator.ValidForPrefix(prefix); iterator.Next() {
					key := iterator.Item().KeyCopy(nil)
					value, err := iterator.Item().ValueCopy(nil)
					if err != nil {
						iterator.Close()
						return err
					}
					values[string(key)] = append([]byte{}, value...)
				}
				iterator.Close()
			}
			return nil
		}))
		return values
	}
	assertState(app.badgerStore, beforeHeader, beforeRevision, 0)
	beforeBytes := durableTaintBytes()
	beforeHash := stateHash(app.badgerStore)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkExpand,
		DecisionID:   decisionID,
		Generation:   beforeHeader.Generation,
		FrontierHead: beforeHeader.FrontierHead,
		ParentRefID:  beforeHeader.ParentRefID,
		Cursor:       beforeHeader.LastEdge,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)

	// Discard models a speculative Badger transaction being abandoned. It does
	// not run FinalizeBlock, ABCI Commit, SQL recovery or a process crash.
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(scoped.DiscardConsensusTransaction)
	working := app.cloneForAppV20Finalize(scoped)
	result, err := working.processAppV30MeteredTx(parsed, 20, time.Unix(parsed.AgentTimestamp, 0).UTC())
	require.NoError(t, err)
	require.Zero(t, result.Code, result.Log)
	require.EqualValues(t, 1, working.appV30WorkTxCount)
	require.Zero(t, scoped.AppV30WriteStageStats())
	stagedHeader, found, err := scoped.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusExpanding, stagedHeader.Status)
	require.NotEmpty(t, stagedHeader.LastEdge)
	materialized := int(stagedHeader.Materialized - beforeHeader.Materialized)
	require.Positive(t, materialized)
	require.Less(t, materialized, childCount)
	targetRefID, err := tx.ParentRefIDHex(target)
	require.NoError(t, err)
	expectedHeader := beforeHeader
	expectedHeader.FrontierHead = 1
	expectedHeader.FrontierTail = uint64(materialized + 1)
	expectedHeader.Scanned = uint32(materialized)
	expectedHeader.Materialized = uint32(materialized + 1)
	expectedHeader.MaxDepth = 1
	expectedHeader.ParentRefID = targetRefID
	expectedHeader.ParentMemoryID = target.MemoryID
	expectedHeader.LastEdge = children[materialized-1].MemoryID
	assertState(scoped, expectedHeader, beforeRevision+1, materialized)
	stagedHash := stateHash(scoped)
	require.NotEqual(t, beforeHash, stagedHash)

	// Canonical readers must still see the complete original state even while
	// the scoped header, memberships, indexes and frontier have advanced.
	assertState(app.badgerStore, beforeHeader, beforeRevision, 0)
	require.Equal(t, beforeBytes, durableTaintBytes())
	scoped.DiscardConsensusTransaction()
	assertState(app.badgerStore, beforeHeader, beforeRevision, 0)
	require.Equal(t, beforeBytes, durableTaintBytes())
	require.Equal(t, beforeHash, stateHash(app.badgerStore))

	// The same signed envelope is replayed from the unchanged durable prestate.
	replayStore := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(replayStore.DiscardConsensusTransaction)
	replay := app.cloneForAppV20Finalize(replayStore)
	replayed, err := replay.processAppV30MeteredTx(parsed, 20, time.Unix(parsed.AgentTimestamp, 0).UTC())
	require.NoError(t, err)
	require.Zero(t, replayed.Code, replayed.Log)
	require.Zero(t, replayStore.AppV30WriteStageStats())
	assertState(replayStore, expectedHeader, beforeRevision+1, materialized)
	require.Equal(t, stagedHash, stateHash(replayStore), "fresh replay must reproduce all staged canonical bytes")
	require.NoError(t, replayStore.CommitConsensusTransaction())
	assertState(app.badgerStore, expectedHeader, beforeRevision+1, materialized)
	require.Equal(t, stagedHash, stateHash(app.badgerStore))

	// Independently enumerate every durable taint key/value: the target queue
	// entry was popped, and each processed child gained one membership, index
	// and correctly bound frontier entry. Unprocessed children gained none.
	expectedBytes := make(map[string][]byte, len(beforeBytes)+3*materialized)
	for key, value := range beforeBytes {
		expectedBytes[key] = append([]byte{}, value...)
	}
	headerBytes, err := taint.EncodeHeader(expectedHeader)
	require.NoError(t, err)
	expectedBytes[string(taint.DecisionKey(decisionID))] = headerBytes
	expectedBytes[string(taint.RevisionKey())] = binary.BigEndian.AppendUint64(nil, beforeRevision+1)
	delete(expectedBytes, string(taint.FrontierKey(decisionID, beforeHeader.Generation, 0)))
	for i, child := range children[:materialized] {
		membership, err := taint.EncodeMembership(taint.Membership{
			Generation: beforeHeader.Generation, Depth: 1, Origin: taint.OriginExpand,
		})
		require.NoError(t, err)
		childRefID, err := tx.ParentRefIDHex(child)
		require.NoError(t, err)
		queue, err := taint.EncodeQueueEntry(taint.QueueEntry{
			ParentRefID: childRefID, MemoryID: child.MemoryID, Depth: 1,
		})
		require.NoError(t, err)
		expectedBytes[string(taint.MembershipKey(child.MemoryID, decisionID))] = membership
		expectedBytes[string(taint.DecisionIndexKey(decisionID, child.MemoryID))] = []byte{}
		expectedBytes[string(taint.FrontierKey(decisionID, beforeHeader.Generation, uint64(i+1)))] = queue
	}
	require.Equal(t, expectedBytes, durableTaintBytes())

	// Resubmit the same work payload in a fresh signed envelope so the cursor
	// guard, rather than outer nonce replay protection, must reject it. This is
	// a stale-cursor no-second-advance assertion, not full exactly-once execution.
	repeated := makeMemoryTaintWorkTx(t, root, work, 2)
	repeatedStore := app.badgerStore.BeginConsensusTransaction(nil)
	t.Cleanup(repeatedStore.DiscardConsensusTransaction)
	repeatedApp := app.cloneForAppV20Finalize(repeatedStore)
	rejected, err := repeatedApp.processAppV30MeteredTx(repeated, 20, time.Unix(repeated.AgentTimestamp, 0).UTC())
	require.NoError(t, err)
	require.Equal(t, appV30TaintWorkStateCode, rejected.Code, rejected.Log)
	require.Contains(t, rejected.Log, "taint expansion cursor is stale")
	require.Zero(t, repeatedApp.appV30WorkTxCount)
	require.Zero(t, repeatedStore.AppV30WriteStageStats())
	assertState(repeatedStore, expectedHeader, beforeRevision+1, materialized)
	require.Equal(t, stagedHash, stateHash(repeatedStore))
	require.NoError(t, repeatedStore.CommitConsensusTransaction())
	assertState(app.badgerStore, expectedHeader, beforeRevision+1, materialized)
	require.Equal(t, expectedBytes, durableTaintBytes())
	require.Equal(t, stagedHash, stateHash(app.badgerStore))
}

func mustTaintRevision(t *testing.T, s *store.BadgerStore) uint64 {
	t.Helper()
	revision, err := s.TaintRevision()
	require.NoError(t, err)
	return revision
}

func TestMemoryTaintWorkAdvancesCleanup(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	decisionID := strings.Repeat("f", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("cleanup work payload")),
	)
	require.NoError(t, err)
	require.True(t, created)
	changed, err := app.badgerStore.ClearTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, changed)
	header, found, err := app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)

	work := &tx.MemoryTaintWork{
		Version:      tx.MemoryTaintWorkVersion,
		Kind:         tx.MemoryTaintWorkCleanup,
		DecisionID:   decisionID,
		Generation:   header.Generation,
		CleanupPhase: uint8(header.CleanupPhase),
		Cursor:       header.LastEdge,
	}
	result := app.processMemoryTaintWork(
		makeMemoryTaintWorkTx(t, root, work, 1),
		20,
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
	header, found, err = app.badgerStore.GetTaintDecision(decisionID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.CleanupQueue, header.CleanupPhase)
	decisions, err := app.badgerStore.GetEffectiveTaintDecisions(target.MemoryID)
	require.NoError(t, err)
	require.Empty(t, decisions)
}

func TestMemoryTaintWorkCheckTxRejectsBeforeAppV30(t *testing.T) {
	app, root, target, chainID := setupAppV30TaintGovernance(t)
	decisionID := strings.Repeat("a", taint.DecisionIDBytes)
	_, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		target,
		sha256.Sum256([]byte("check tx work payload")),
	)
	require.NoError(t, err)
	work := &tx.MemoryTaintWork{
		Version:    tx.MemoryTaintWorkVersion,
		Kind:       tx.MemoryTaintWorkExpand,
		DecisionID: decisionID,
		Generation: 1,
	}
	parsed := makeMemoryTaintWorkTx(t, root, work, 1)
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)

	app.state.Height = 19
	app.appV30AppliedHeight = 30
	response, err := app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
	require.NoError(t, err)
	require.Equal(t, uint32(10), response.Code)
	require.Contains(t, response.Log, "unknown tx type")
}
