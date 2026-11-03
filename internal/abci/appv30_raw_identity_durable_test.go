package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/dgraph-io/badger/v4"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

type durableRawIdentityCase struct {
	name           string
	id             string
	classification tx.ClearanceLevel
	content        string
	hash           [32]byte
	height         int64
}

// This exercises the same-key signed transaction route, not delegated REST's
// server-generated UUID route. Fresh app-v30 activation is signed and persisted
// by InitChain; no cached fork height or direct canonical seeding grants access.
func TestAppV30RawMemoryIDsSurviveSignedCommitAndDurableReopen(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	canonicalPath := filepath.Join(directory, "canonical")
	projectionPath := filepath.Join(directory, "projection.db")
	app := openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
	t.Cleanup(func() {
		if app != nil {
			require.NoError(t, app.Close())
		}
	})
	manifest, root, companion := freshGenesisFixture(t, 30, 4, true)
	require.NotNil(t, companion)
	signer := *companion
	_, err := app.InitChain(ctx, freshGenesisRequest(t, "raw-identity-durable", manifest, root, companion))
	require.NoError(t, err)
	origin, err := app.badgerStore.GetFreshGenesisActivation()
	require.NoError(t, err)
	require.NotNil(t, origin)
	require.Equal(t, uint64(30), origin.AppVersion)
	require.NoError(t, app.badgerStore.ValidateFreshGenesisInitialState())
	require.True(t, app.IsAppV30ActiveForNextTx())
	require.Zero(t, app.state.Height)
	require.Empty(t, app.state.AppHash)
	for version := uint64(2); version <= 30; version++ {
		applied, readErr := app.badgerStore.GetAppliedUpgrade(fmt.Sprintf("app-v%d", version))
		require.NoError(t, readErr)
		require.Nil(t, applied, "the persisted fresh origin must not fabricate upgrade history")
	}

	cases := durableRawIdentityCases(t)
	var nonce uint64
	var height int64
	blockTime := time.Now().UTC().Truncate(time.Second)
	// The existing atomic-finalize limit is 64 transactions and the independent
	// generated-write budget is 4 MiB. Several genuine blocks cover the byte
	// alphabet without relaxing either limit or mistaking a block cap for an ID cap.
	const batchSize = 32
	for start := 0; start < len(cases); start += batchSize {
		end := min(start+batchSize, len(cases))
		height++
		blockTime = blockTime.Add(time.Second)
		var rawTxs [][]byte
		var estimatedWrites int64
		for i := start; i < end; i++ {
			tc := &cases[i]
			tc.height = height
			nonce++
			parsed := makeMemorySubmitTx(t, signer, "fresh-home", tc.content)
			parsed.MemorySubmit.MemoryID = tc.id
			parsed.MemorySubmit.Classification = tc.classification
			parsed.Timestamp = blockTime
			raw := encodeDirectH0Tx(t, parsed, signer, nonce)
			require.Equal(t, parsed.AgentPubKey, parsed.PublicKey, "%s: same-key signed route", tc.name)
			require.False(t, validRESTMemoryID(tc.id), "%s: do not claim delegated REST arbitrary-ID support", tc.name)
			decoded, decodeErr := tx.DecodeTx(raw)
			require.NoError(t, decodeErr, tc.name)
			require.NoError(t, tx.ActivateMemorySubmitExtensions(decoded, true, true), tc.name)
			require.Equal(t, []byte(tc.id), []byte(decoded.MemorySubmit.MemoryID), "%s: signed wire identity bytes", tc.name)
			require.Equal(t, tc.classification, decoded.MemorySubmit.Classification, tc.name)
			require.NoError(t, validateAppV20TxResources(decoded), tc.name)
			checked, checkErr := app.CheckTx(ctx, &abcitypes.RequestCheckTx{Tx: raw})
			require.NoError(t, checkErr, tc.name)
			require.Zero(t, checked.Code, "%s: CheckTx: %s", tc.name, checked.Log)
			estimatedWrites += appV30GeneratedWriteEstimate(decoded)
			rawTxs = append(rawTxs, raw)
			_, readErr := app.badgerStore.GetMemoryDisclosureStateAppV30(tc.id)
			require.ErrorIs(t, readErr, store.ErrMemoryDisclosureNotFound, "%s: CheckTx must not publish", tc.name)
		}
		require.True(t, appV20AtomicFinalizeBudget(rawTxs), "the fixture must respect actual entry and raw-byte caps")
		require.LessOrEqual(t, estimatedWrites, appV30BlockWriteBudgetBytes, "the fixture must respect the generated-write planner")
		previousHeight := app.state.Height
		previousHash := append([]byte(nil), app.state.AppHash...)
		previousNonce, readErr := app.badgerStore.GetNonce(signer.id)
		require.NoError(t, readErr)
		finalized, finalizeErr := app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
			Height: height, Time: blockTime, Txs: rawTxs,
		})
		require.NoError(t, finalizeErr, "admitted raw IDs must not abort FinalizeBlock")
		require.NotNil(t, finalized)
		require.Len(t, finalized.TxResults, end-start)
		require.Len(t, finalized.AppHash, 32)
		require.NotNil(t, app.pendingAppV20Finalize)
		for i, result := range finalized.TxResults {
			tc := cases[start+i]
			require.Zero(t, result.Code, "%s: FinalizeBlock: %s", tc.name, result.Log)
			require.Equal(t, []byte(tc.id), result.Data, "%s: actual execution result identity", tc.name)
			_, readErr = app.badgerStore.GetMemoryDisclosureStateAppV30(tc.id)
			require.ErrorIs(t, readErr, store.ErrMemoryDisclosureNotFound, "%s: canonical publication waits for Commit", tc.name)
			_, readErr = app.offchainStore.GetMemory(ctx, tc.id)
			require.ErrorIs(t, readErr, store.ErrMemoryNotFound, "%s: SQL publication waits for Commit", tc.name)
		}
		require.Equal(t, previousHeight, app.state.Height)
		require.Equal(t, previousHash, app.state.AppHash)
		uncommittedNonce, readErr := app.badgerStore.GetNonce(signer.id)
		require.NoError(t, readErr)
		require.Equal(t, previousNonce, uncommittedNonce)
		_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
		require.NoError(t, err)
		require.Nil(t, app.pendingAppV20Finalize)
		require.Equal(t, height, app.state.Height)
		require.Equal(t, finalized.AppHash, app.state.AppHash)
		committedNonce, readErr := app.badgerStore.GetNonce(signer.id)
		require.NoError(t, readErr)
		require.Equal(t, nonce, committedNonce)
		assertDurableRawIdentityRecords(t, app, signer, cases[start:end])
	}

	committedHash := append([]byte(nil), app.state.AppHash...)
	publicRoot, err := app.badgerStore.PublicMemoryRoot()
	require.NoError(t, err)
	// All cases share one final root here. Retain the exact branch bytes, not
	// merely helper success, for comparison after complete constructor recovery.
	proofs := make(map[string]store.PublicMemoryBranch)
	for _, tc := range cases {
		if tc.classification == tx.ClearancePublic {
			proof, proofErr := app.badgerStore.PublicMemoryBranch(tc.id)
			require.NoError(t, proofErr, tc.name)
			proofs[tc.id] = *proof
		}
	}
	oldCanonical := app.badgerStore
	oldProjection := app.offchainStore
	require.NoError(t, app.Close())
	app = nil
	app = openFreshGenesisH0TestApp(t, canonicalPath, projectionPath)
	require.NotSame(t, oldCanonical, app.badgerStore)
	require.NotSame(t, oldProjection, app.offchainStore)
	info, err := app.Info(ctx, nil)
	require.NoError(t, err)
	require.Equal(t, uint64(30), info.AppVersion)
	require.Equal(t, height, info.LastBlockHeight)
	require.Equal(t, committedHash, info.LastBlockAppHash)
	reopenedOrigin, err := app.badgerStore.GetFreshGenesisActivation()
	require.NoError(t, err)
	require.Equal(t, origin, reopenedOrigin)
	require.NoError(t, app.badgerStore.ValidateFreshGenesisLineage())
	require.True(t, app.IsAppV30ActiveForNextTx(), "constructor recovery must restore the persisted gate")
	recoveredNonce, err := app.badgerStore.GetNonce(signer.id)
	require.NoError(t, err)
	require.Equal(t, nonce, recoveredNonce)
	recoveredRoot, err := app.badgerStore.PublicMemoryRoot()
	require.NoError(t, err)
	require.Equal(t, publicRoot, recoveredRoot)
	assertDurableRawIdentityRecords(t, app, signer, cases)
	for id, before := range proofs {
		after, proofErr := app.badgerStore.PublicMemoryBranch(id)
		require.NoError(t, proofErr)
		require.Equal(t, before, *after, "canonical leaf and every sibling must survive durable reopen")
	}
	// Recovery must also support the next real Finalize/Commit pair rather than
	// only reporting cached metadata. An empty block changes no public leaf.
	next, err := app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
		Height: height + 1, Time: blockTime.Add(time.Second),
	})
	require.NoError(t, err)
	require.Empty(t, next.TxResults)
	_, err = app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, height+1, app.state.Height)
	require.Equal(t, next.AppHash, app.state.AppHash)
	unchangedRoot, err := app.badgerStore.PublicMemoryRoot()
	require.NoError(t, err)
	require.Equal(t, publicRoot, unchangedRoot)
}

func durableRawIdentityCases(t *testing.T) []durableRawIdentityCase {
	t.Helper()
	cases := []durableRawIdentityCase{
		{name: "public-ascii-256", id: strings.Repeat("a", 256), classification: tx.ClearancePublic},
		{name: "public-ascii-257", id: strings.Repeat("b", 257), classification: tx.ClearancePublic},
		{name: "public-ascii-512", id: strings.Repeat("p", 512), classification: tx.ClearancePublic},
		{name: "internal-ascii-512", id: strings.Repeat("i", 512), classification: tx.ClearanceInternal},
		{name: "public-whitespace-512", id: strings.Repeat(" ", 512), classification: tx.ClearancePublic},
		{name: "internal-whitespace-512", id: "\t" + strings.Repeat(" ", 511), classification: tx.ClearanceInternal},
		{name: "public-nul-512", id: strings.Repeat("\x00", 512), classification: tx.ClearancePublic},
		{name: "internal-nul-512", id: strings.Repeat("\x00", 511) + "\x01", classification: tx.ClearanceInternal},
		{name: "public-non-utf8-512", id: strings.Repeat("\xff", 512), classification: tx.ClearancePublic},
		{name: "internal-non-utf8-512", id: strings.Repeat("\xfe", 512), classification: tx.ClearanceInternal},
	}
	for value := 0; value < 256; value++ {
		cases = append(cases, durableRawIdentityCase{
			name: fmt.Sprintf("public-single-byte-%02x", value),
			id:   string([]byte{byte(value)}), classification: tx.ClearancePublic,
		})
	}
	seen := make(map[string]bool)
	for i := range cases {
		tc := &cases[i]
		require.False(t, seen[tc.id], "%s: fixture identities must be distinct", tc.name)
		seen[tc.id] = true
		require.NotEmpty(t, tc.id)
		require.LessOrEqual(t, len(tc.id), 512)
		tc.content = fmt.Sprintf("signed raw identity durable fixture %03d", i)
		tc.hash = sha256.Sum256([]byte(tc.content))
	}
	require.Len(t, cases, 266)
	return cases
}

func assertDurableRawIdentityRecords(t *testing.T, app *SageApp, signer agentKey, cases []durableRawIdentityCase) {
	t.Helper()
	ctx := context.Background()
	root, err := app.badgerStore.PublicMemoryRoot()
	require.NoError(t, err)
	for _, tc := range cases {
		canonical, readErr := app.badgerStore.GetMemoryDisclosureStateAppV30(tc.id)
		require.NoError(t, readErr, tc.name)
		require.Equal(t, tc.hash[:], canonical.ContentHash, tc.name)
		require.Equal(t, string(memory.StatusProposed), canonical.Status, tc.name)
		require.True(t, canonical.DomainRecorded, tc.name)
		require.Equal(t, "fresh-home", canonical.Domain, tc.name)
		require.True(t, canonical.AuthorRecorded, tc.name)
		require.Equal(t, signer.id, canonical.Author, tc.name)
		require.True(t, canonical.AuthorPrincipalRecorded, tc.name)
		require.Equal(t, signer.id, canonical.AuthorPrincipal, tc.name)
		require.True(t, canonical.ClassificationRecorded, tc.name)
		require.Equal(t, uint8(tc.classification), canonical.Classification, tc.name)
		observedHeight, recorded, heightErr := app.badgerStore.GetMemorySubmissionHeight(tc.id)
		require.NoError(t, heightErr, tc.name)
		require.True(t, recorded, tc.name)
		require.Equal(t, tc.height, observedHeight, tc.name)
		projection, projectionErr := app.offchainStore.GetMemory(ctx, tc.id)
		require.NoError(t, projectionErr, tc.name)
		require.Equal(t, []byte(tc.id), []byte(projection.MemoryID), "%s: SQL must not normalize raw identities", tc.name)
		require.Equal(t, tc.content, projection.Content, tc.name)
		require.Equal(t, tc.hash[:], projection.ContentHash, tc.name)
		require.Equal(t, memory.StatusProposed, projection.Status, tc.name)
		require.Equal(t, signer.id, projection.SubmittingAgent, tc.name)
		require.Equal(t, "fresh-home", projection.DomainTag, tc.name)

		proof, proofErr := app.badgerStore.PublicMemoryBranch(tc.id)
		if tc.classification != tx.ClearancePublic {
			require.ErrorIs(t, proofErr, store.ErrPublicMemoryIndex, "%s: INTERNAL must not acquire a public branch", tc.name)
			require.Nil(t, proof)
			continue
		}
		require.NoError(t, proofErr, tc.name)
		require.NotNil(t, proof)
		expectedLeaf := store.PublicMemoryLeaf{
			MemoryID: tc.id, ContentHash: tc.hash, Author: signer.id,
			AuthorPrincipal: signer.id, Domain: "fresh-home", Status: string(memory.StatusProposed),
		}
		require.Equal(t, expectedLeaf, proof.Leaf, tc.name)
		require.NoError(t, store.VerifyPublicMemoryBranch(*proof, root), tc.name)
		expectedBytes := durableRawIdentityLeafBytes(expectedLeaf)
		expectedLeafHash := sha256.Sum256(expectedBytes)
		path := sha256.Sum256(append([]byte("sage.public-memory.path.v1\x00"), []byte(tc.id)...))
		key := binary.BigEndian.AppendUint16([]byte("public-memory-index:v1:node:"), 256)
		key = append(key, path[:]...)
		require.NoError(t, app.badgerStore.DB().View(func(view *badger.Txn) error {
			item, itemErr := view.Get(key)
			if itemErr != nil {
				return itemErr
			}
			return item.Value(func(value []byte) error {
				require.Equal(t, expectedLeafHash[:], value, "%s: stored leaf commits the independently encoded raw bytes", tc.name)
				return nil
			})
		}), tc.name)
		folded := expectedLeafHash
		for depth := 255; depth >= 0; depth-- {
			left, right := folded, proof.Siblings[depth]
			if path[depth/8]&(1<<uint(7-depth%8)) != 0 {
				left, right = right, left
			}
			preimage := append([]byte("sage.public-memory.node.v1\x00"), left[:]...)
			folded = sha256.Sum256(append(preimage, right[:]...))
		}
		require.Equal(t, root, folded, "%s: independently encoded leaf must reconstruct the persisted root", tc.name)
		tampered := *proof
		changedID := append([]byte(nil), []byte(tc.id)...)
		changedID[0] ^= 1
		tampered.Leaf.MemoryID = string(changedID)
		require.ErrorIs(t, store.VerifyPublicMemoryBranch(tampered, root), store.ErrPublicMemoryIndex, tc.name)
		tampered = *proof
		tampered.Leaf.ContentHash[0] ^= 1
		require.ErrorIs(t, store.VerifyPublicMemoryBranch(tampered, root), store.ErrPublicMemoryIndex, tc.name)
	}
}

// The expected encoding is independent of the production private canonical()
// routine. Reading its persisted depth-256 digest and folding the branch tests
// exact u16 lengths, raw ID bytes, field order, separator and content hash.
func durableRawIdentityLeafBytes(leaf store.PublicMemoryLeaf) []byte {
	var encoded bytes.Buffer
	encoded.WriteString("sage.public-memory.leaf.v1\x00")
	for _, field := range []string{leaf.MemoryID, leaf.Author, leaf.AuthorPrincipal, leaf.Domain, leaf.Status} {
		var length [2]byte
		binary.BigEndian.PutUint16(length[:], uint16(len(field))) // #nosec G115 -- expected fixture fields are bounded to 512 bytes
		encoded.Write(length[:])
		encoded.WriteString(field)
	}
	encoded.WriteByte(0)
	encoded.Write(leaf.ContentHash[:])
	return encoded.Bytes()
}
