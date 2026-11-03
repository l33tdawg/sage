package abci

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	cmtTypes "github.com/cometbft/cometbft/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/poe"
	"github.com/l33tdawg/sage/internal/statesync"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

// This draft targets the final composed app30 source, including the wide-ID
// public-index and recovery adapters. It intentionally keeps 512-byte IDs.
// Its direct-store prehistory is structural setup, not a signed network ledger.
type appV30ComposedFixture struct {
	app       *SageApp
	bs        *store.BadgerStore
	sql       *store.SQLiteStore
	path      string
	chain     string
	root      agentKey
	parents   []tx.ParentRef
	decisions []string
	work      []*tx.MemoryTaintWork
}

func composedAppV30ID(prefix string) string {
	return prefix + strings.Repeat("x", taint.MaxMemoryIDBytes-len(prefix))
}

func newAppV30ComposedFixture(t *testing.T) *appV30ComposedFixture {
	t.Helper()
	dir := t.TempDir()
	f := &appV30ComposedFixture{
		path:  filepath.Join(dir, "badger"),
		chain: strings.Repeat("c", cmtTypes.MaxChainIDLen),
		root:  deterministicScopedAgent(201),
	}
	var err error
	f.bs, err = store.NewBadgerStore(f.path)
	require.NoError(t, err)
	// One owner joins the mirror worker before closing either store, including
	// on a failed assertion. No independent store-close callback follows a join failure.
	t.Cleanup(func() {
		if f.app != nil {
			require.NoError(t, f.app.Close())
			return
		}
		require.NoError(t, f.bs.CloseBadger())
		if f.sql != nil {
			require.NoError(t, f.sql.Close())
		}
	})
	f.sql, err = store.NewSQLiteStore(context.Background(), filepath.Join(dir, "projection.db"))
	require.NoError(t, err)
	domain, err := governance.DelegationDomainForChainID(f.chain)
	require.NoError(t, err)
	companion := deterministicScopedAgent(233)
	require.NoError(t, f.bs.BootstrapAppV23Genesis(store.AppV23GenesisBootstrap{
		RootID: f.root.id, Scope: domain, AgentID: companion.id,
		Profile: store.AppV23ProfileCompanion, HomeDomain: "fixture-home",
		Clearance: 1, Capabilities: 15, Height: 1,
		BootstrapDigest: strings.Repeat("5b", 32), ActivateAtGenesis: true,
		ValidatorID: f.root.id, ValidatorPower: 10,
	}))
	require.NoError(t, f.bs.MarkUpgradeApplied(appV24UpgradeName, 24, 2))
	require.NoError(t, f.bs.MarkUpgradeApplied(appV25UpgradeName, 25, 3))
	require.NoError(t, f.bs.MigrateAppV26AccessGroupAuthorities(4))
	require.NoError(t, f.bs.MarkUpgradeApplied(appV26UpgradeName, 26, 4))
	require.NoError(t, f.bs.MarkUpgradeApplied(appV27UpgradeName, 27, 5))
	prior, err := f.bs.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, f.bs.PreparePublicMemoryStage(context.Background(), 6))
	require.NoError(t, f.bs.PrepareCoCommitTombstoneStage(context.Background(), 6))
	seed := f.bs.BeginConsensusTransaction(nil)
	t.Cleanup(seed.DiscardConsensusTransaction)
	require.NoError(t, seed.PromotePublicMemoryStage(6, prior))
	require.NoError(t, seed.PromoteCoCommitTombstoneStage(6, prior))
	require.NoError(t, seed.MarkUpgradeApplied(appV28UpgradeName, 28, 6))
	require.NoError(t, seed.MarkUpgradeApplied(appV29UpgradeName, 29, 7))
	require.NoError(t, seed.MarkUpgradeApplied(appV30UpgradeName, 30, 8))
	require.NoError(t, seed.CommitConsensusTransaction())

	rootState, err := f.bs.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	seedMemory := func(id string) tx.ParentRef {
		ref := tx.ParentRef{SourceChainID: f.chain, MemoryID: id, ContentHash: sha256.Sum256([]byte(id))}
		require.NoError(t, f.bs.SetMemoryHash(id, ref.ContentHash[:], "committed"))
		require.NoError(t, f.bs.SetMemoryDomain(id, "general"))
		require.NoError(t, f.bs.SetMemoryAuthor(id, f.root.id))
		require.NoError(t, f.bs.SetMemoryAuthorPrincipal(id, rootState.PrincipalID))
		require.NoError(t, f.bs.SetMemoryClassification(id, uint8(store.ClearanceInternal)))
		return ref
	}
	target := seedMemory(composedAppV30ID("00-target-"))
	for i := 0; i < taint.MaxParents; i++ {
		parent := seedMemory(composedAppV30ID(fmt.Sprintf("01-parent-%02d-", i)))
		require.NoError(t, f.bs.SetMemoryLineage(parent.MemoryID, []tx.ParentRef{target}))
		f.parents = append(f.parents, parent)
	}
	tx.SortParentRefs(f.parents)
	// More than two reachable 1024-unit expansion passes. Four decisions will
	// retain real work after the first pass; no counter/header forging is used.
	for i := 0; i < 2*taint.MaxNewMembershipsPerExec; i++ {
		child := seedMemory(composedAppV30ID(fmt.Sprintf("02-child-%04d-", i)))
		require.NoError(t, f.bs.SetMemoryLineage(child.MemoryID, []tx.ParentRef{target}))
	}
	credentials := []string{f.root.id}
	for i := 1; i < tx.MaxTaintCredentialSetEntries; i++ {
		credentials = append(credentials, fmt.Sprintf("%064x", i))
	}
	sort.Strings(credentials)
	for i := 0; i < taint.MaxActiveDecisions; i++ {
		apply := appV30ApplyPayload(target)
		apply.ScopeKind = tx.TaintScopeCredentialSet
		apply.Credentials = append([]string(nil), credentials...)
		evidence := sha256.Sum256([]byte(fmt.Sprintf("composed decision %d", i)))
		apply.EvidenceDigest = evidence[:]
		payload, err := tx.EncodeTaintApplyPayload(apply)
		require.NoError(t, err)
		decision, err := tx.TaintApplyDecisionID(payload)
		require.NoError(t, err)
		created, err := f.bs.ApplyTaintDecisionWithScope(f.chain, decision, target,
			sha256.Sum256(payload), store.TaintDecisionScopeFromPayload(apply))
		require.NoError(t, err)
		require.True(t, created)
		expanded, err := f.bs.ExpandTaintDecision(f.chain, decision, taint.PerExecutionUnitBudget)
		require.NoError(t, err)
		require.Greater(t, expanded.Materialized, taint.MaxParents)
		if i < maxAppV30TaintWorkPerBlock {
			header, found, err := f.bs.GetTaintDecision(decision)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, taint.StatusExpanding, header.Status)
			f.work = append(f.work, &tx.MemoryTaintWork{
				Version: tx.MemoryTaintWorkVersion, Kind: tx.MemoryTaintWorkExpand,
				DecisionID: decision, Generation: header.Generation,
				FrontierHead: header.FrontierHead, ParentRefID: header.ParentRefID,
				Cursor: header.LastEdge,
			})
		} else {
			for pass := 0; expanded.Status != taint.StatusComplete; pass++ {
				require.Less(t, pass, 16)
				expanded, err = f.bs.ExpandTaintDecision(f.chain, decision, taint.PerExecutionUnitBudget)
				require.NoError(t, err)
			}
		}
		f.decisions = append(f.decisions, decision)
	}
	for _, parent := range f.parents {
		members, err := f.bs.GetEffectiveTaintDecisions(parent.MemoryID)
		require.NoError(t, err)
		require.Len(t, members, taint.MaxActiveDecisions)
	}
	require.NoError(t, f.bs.ValidateLineageIndexes())
	require.NoError(t, f.bs.ValidateTaintState())
	// Persist the actual tuple the constructor will reopen, not cached fork fields.
	state := &AppState{Height: 18, EpochNum: poe.EpochNumber(18)}
	for range 2 {
		state.AppHash, err = f.bs.ComputePublicMemoryAppHash()
		require.NoError(t, err)
		require.NoError(t, SaveState(f.bs, state))
	}
	f.app, err = NewSageAppWithStores(f.bs, f.sql, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, f.app.SetExpectedGovernanceDelegationDomain(f.chain))
	return f
}

func composedAppV30Encode(t *testing.T, parsed *tx.ParsedTx, root agentKey, nonce uint64, at time.Time) []byte {
	t.Helper()
	parsed.Nonce, parsed.Timestamp = nonce, at
	require.NoError(t, tx.SignTx(parsed, root.priv))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

func (f *appV30ComposedFixture) reopen(t *testing.T) {
	t.Helper()
	require.NoError(t, f.app.CloseConsensusState())
	f.app = nil
	var err error
	f.bs, err = store.NewBadgerStore(f.path)
	require.NoError(t, err)
	f.app, err = NewSageAppWithStores(f.bs, f.sql, zerolog.Nop())
	require.NoError(t, err)
	require.NoError(t, f.app.SetExpectedGovernanceDelegationDomain(f.chain))
}

func composedAppV30CanonicalBytes(t *testing.T, bs *store.BadgerStore) []byte {
	t.Helper()
	var out bytes.Buffer
	require.NoError(t, statesync.WriteCanonicalState(context.Background(), bs.DB(), &out))
	return out.Bytes()
}

func TestAppV30ComposedBlockCommitReopenAndReplay(t *testing.T) {
	f := newAppV30ComposedFixture(t)
	ctx := context.Background()
	at := time.Unix(1_800_000_020, 0).UTC()
	clearDecision := f.decisions[maxAppV30TaintWorkPerBlock]
	clearPayload, clearTarget := scopeReservationClearPayload(t, clearDecision)
	propose := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose,
		GovPropose: &tx.GovPropose{Operation: tx.GovOpMemoryTaint,
			TargetID: clearTarget, Payload: clearPayload, Reason: "composed clear proposal"},
	}
	domain, err := governance.DelegationDomainForChainID(f.chain)
	require.NoError(t, err)
	proofBody := governanceJSON(t, map[string]any{
		"validator_id": f.root.id, "governance_domain": domain,
		"operation": "memory_taint", "target_id": clearTarget,
		"reason": "composed clear proposal", "payload": base64.StdEncoding.EncodeToString(clearPayload),
	})
	attachGovernanceRequestProof(t, propose, f.root, f.root, "POST", "/v1/governance/propose",
		proofBody, at.Add(-time.Second), []byte("mixclear"))
	// A real signed warmup creates the carried proposal and its SQL projection.
	warmup, err := f.app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
		Height: 19, Time: at.Add(-time.Second), Txs: [][]byte{composedAppV30Encode(t, propose, f.root, 1, at.Add(-time.Second))},
	})
	require.NoError(t, err)
	require.Len(t, warmup.TxResults, 1)
	require.Zero(t, warmup.TxResults[0].Code, warmup.TxResults[0].Log)
	_, err = f.app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	proposal, err := f.app.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.NotNil(t, proposal)
	require.Equal(t, int64(19), proposal.CreatedHeight)
	votes, err := f.app.govEngine.GetProposalVotes(proposal.ProposalID)
	require.NoError(t, err)
	require.Empty(t, votes)
	carry, err := f.app.appV30PendingGovernanceReservation(20)
	require.NoError(t, err)
	require.Equal(t, int64(appV30PendingGovernanceBytes), carry)

	vote := &tx.ParsedTx{Type: tx.TxTypeGovVote,
		GovVote: &tx.GovVote{ProposalID: proposal.ProposalID, Decision: tx.VoteDecisionAccept}}
	voteBody := governanceJSON(t, map[string]any{
		"validator_id": f.root.id, "governance_domain": domain,
		"proposal_id": proposal.ProposalID, "decision": "accept",
	})
	attachGovernanceRequestProof(t, vote, f.root, f.root, "POST", "/v1/governance/vote",
		voteBody, at, []byte("mixvote1"))
	txs := [][]byte{composedAppV30Encode(t, vote, f.root, 2, at)}
	nonce := uint64(3)
	workBefore := make(map[string]taint.Header)
	for _, work := range f.work {
		header, found, err := f.bs.GetTaintDecision(work.DecisionID)
		require.NoError(t, err)
		require.True(t, found)
		workBefore[work.DecisionID] = header
		parsed := makeMemoryTaintWorkTx(t, f.root, work, nonce)
		txs = append(txs, composedAppV30Encode(t, parsed, f.root, nonce, at))
		nonce++
	}
	// Exercise the real co-commit dispatcher with the full signer bound, not
	// just a manually written provenance tuple or an estimator-only envelope.
	env, _ := buildCoCommitEnvelope(t, f.root, "general", []byte("composed-block"), "fixture-peer")
	keys := []agentKey{f.root}
	env.Coauthors = []tx.CoCommitCoauthor{{PubKey: f.root.pub, ChainID: f.chain}}
	for i := 1; i < taint.MaxSigners; i++ {
		key := deterministicScopedAgent(byte(i + 10))
		keys = append(keys, key)
		env.Coauthors = append(env.Coauthors, tx.CoCommitCoauthor{PubKey: key.pub, ChainID: fmt.Sprintf("fixture-peer-%02d", i)})
	}
	core := tx.CanonicalCoreBytes(env)
	for i := range env.Coauthors {
		env.Coauthors[i].Sig = ed25519.Sign(keys[i].priv, core)
	}
	env.SharedID = tx.ComputeSharedID(tx.CoreHashOf(env), env.Coauthors, env.AgreementNonce)
	txs = append(txs, composedAppV30Encode(t, coCommitSubmitTx(t, f.root, env), f.root, nonce, at))
	nonce++

	var next []byte
	var submitIDs []string
	for i := 0; ; i++ {
		require.Less(t, i, maxAppV20AtomicFinalizeEntries)
		parsed := makeMemorySubmitTx(t, f.root, "general", fmt.Sprintf("composed content %d", i))
		parsed.MemorySubmit.MemoryID = composedAppV30ID(fmt.Sprintf("03-submission-%03d-", i))
		parsed.MemorySubmit.DerivedFrom = append([]tx.ParentRef(nil), f.parents...)
		raw := composedAppV30Encode(t, parsed, f.root, nonce, at)
		candidate := append(append([][]byte(nil), txs...), raw)
		_, err := f.app.appV30BlockGeneratedWriteEstimate(candidate, 20)
		if err != nil {
			require.ErrorIs(t, err, errAppV30GeneratedWriteBudgetExceeded)
			next = raw
			break
		}
		txs = candidate
		submitIDs = append(submitIDs, parsed.MemorySubmit.MemoryID)
		nonce++
	}
	require.NotEmpty(t, submitIDs)
	require.True(t, appV20AtomicFinalizeBudget(append(append([][]byte(nil), txs...), next)))
	estimate, err := f.app.appV30BlockGeneratedWriteEstimate(txs, 20)
	require.NoError(t, err)
	require.Greater(t, estimate, appV30BlockWriteBudgetBytes*9/10)
	require.LessOrEqual(t, estimate, appV30BlockWriteBudgetBytes)
	require.Less(t, appV30BlockWriteBudgetBytes-estimate, f.app.appV30EstimatedTxBytes(next, 20))

	before := snapshotAppV30ConstructorDB(t, f.bs)
	over := append(append([][]byte(nil), txs...), next)
	prepared, err := f.app.PrepareProposal(ctx, &abcitypes.RequestPrepareProposal{
		Height: 20, Time: at, MaxTxBytes: 2_000_000, Txs: over,
	})
	require.NoError(t, err)
	require.Equal(t, txs, prepared.Txs)
	acceptedProposal, err := f.app.ProcessProposal(ctx, &abcitypes.RequestProcessProposal{Height: 20, Time: at, Txs: txs})
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseProcessProposal_ACCEPT, acceptedProposal.Status)
	rejectedProposal, err := f.app.ProcessProposal(ctx, &abcitypes.RequestProcessProposal{Height: 20, Time: at, Txs: over})
	require.NoError(t, err)
	require.Equal(t, abcitypes.ResponseProcessProposal_REJECT, rejectedProposal.Status)
	require.Equal(t, before, snapshotAppV30ConstructorDB(t, f.bs), "proposal planning must not mutate canonical state")
	discard := func() {
		require.NotNil(t, f.app.pendingAppV20Finalize)
		f.app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
		f.app.pendingAppV20Finalize = nil
	}
	// The backstop must reject every transaction before charging/consuming
	// nonces, advancing work, or executing the carried Clear.
	rejected, err := f.app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{Height: 20, Time: at, Txs: over})
	require.NoError(t, err)
	require.Len(t, rejected.TxResults, len(over))
	for _, result := range rejected.TxResults {
		require.Equal(t, appV30GeneratedWriteBudgetCode, result.Code, result.Log)
	}
	rejectedHeader, found, err := f.app.pendingAppV20Finalize.store.GetTaintDecision(clearDecision)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusComplete, rejectedHeader.Status)
	rejectedNonce, err := f.app.pendingAppV20Finalize.store.GetNonce(f.root.id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), rejectedNonce)
	discard()
	require.Equal(t, before, snapshotAppV30ConstructorDB(t, f.bs))

	request := &abcitypes.RequestFinalizeBlock{Height: 20, Time: at, Txs: txs}
	first, err := f.app.FinalizeBlock(ctx, request)
	require.NoError(t, err)
	require.Len(t, first.TxResults, len(txs))
	for i, result := range first.TxResults {
		require.Zero(t, result.Code, "tx %d: %s", i, result.Log)
	}
	require.EqualValues(t, maxAppV30TaintWorkPerBlock, f.app.pendingAppV20Finalize.app.appV30WorkTxCount)
	require.NoError(t, f.app.pendingAppV20Finalize.store.ValidateTaintState())
	require.Equal(t, before, snapshotAppV30ConstructorDB(t, f.bs))
	discard()
	require.Equal(t, before, snapshotAppV30ConstructorDB(t, f.bs))
	// This is identical signed wire replay from durable prestate, not a new
	// nonce envelope. It simulates discard; it is not a process SIGKILL.
	replayed, err := f.app.FinalizeBlock(ctx, request)
	require.NoError(t, err)
	require.Equal(t, first.TxResults, replayed.TxResults)
	require.Equal(t, first.AppHash, replayed.AppHash)
	require.Equal(t, first.ConsensusParamUpdates, replayed.ConsensusParamUpdates)
	_, err = f.app.Commit(ctx, &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Nil(t, f.app.pendingAppV20Finalize)
	for _, work := range f.work {
		header, found, err := f.bs.GetTaintDecision(work.DecisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.Greater(t, header.Materialized, workBefore[work.DecisionID].Materialized)
		require.Equal(t, taint.StatusExpanding, header.Status, "stale-cursor control requires remaining work")
	}
	proposal, err = f.app.govEngine.LoadProposal(proposal.ProposalID)
	require.NoError(t, err)
	require.Equal(t, governance.StatusExecuted, proposal.Status)
	header, found, err := f.bs.GetTaintDecision(clearDecision)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, taint.StatusCleaning, header.Status)
	for _, id := range submitIDs {
		parents, err := f.bs.GetMemoryLineage(id)
		require.NoError(t, err)
		require.Equal(t, f.parents, parents)
		members, err := f.bs.GetEffectiveTaintDecisions(id)
		require.NoError(t, err)
		require.Len(t, members, taint.MaxActiveDecisions-1)
		require.NotContains(t, members, clearDecision, "Clear withdraws admitted holds before cleanup completes")
		provenance, found, err := f.bs.GetAppV30Provenance(id)
		require.NoError(t, err)
		require.True(t, found)
		require.Len(t, provenance, 1)
		require.Equal(t, f.root.id, provenance[0].CredentialID)
		require.True(t, provenance[0].Exact)
	}
	coSigners, found, err := f.bs.GetAppV30Provenance(env.SharedID)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, coSigners, taint.MaxSigners)
	require.NoError(t, f.bs.ValidateLineageIndexes())
	require.NoError(t, f.bs.ValidateTaintState())
	canonicalCommitted := composedAppV30CanonicalBytes(t, f.bs)
	info, err := f.app.Info(ctx, &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.Equal(t, int64(20), info.LastBlockHeight)
	require.Equal(t, replayed.AppHash, info.LastBlockAppHash)
	f.reopen(t)
	require.Equal(t, canonicalCommitted, composedAppV30CanonicalBytes(t, f.bs), "valid constructor reopen must preserve the complete canonical stream")
	reopened, err := f.app.Info(ctx, &abcitypes.RequestInfo{})
	require.NoError(t, err)
	require.Equal(t, info, reopened)
	computed, err := f.bs.ComputePublicMemoryAppHash()
	require.NoError(t, err)
	require.Equal(t, replayed.AppHash, computed)

	// After Commit, the old exact envelope is rejected by its consumed nonce.
	beforeCheck := snapshotAppV30ConstructorDB(t, f.bs)
	check, err := f.app.CheckTx(ctx, &abcitypes.RequestCheckTx{Tx: txs[1]})
	require.NoError(t, err)
	require.Equal(t, uint32(4), check.Code, check.Log)
	require.Contains(t, check.Log, "nonce too low")
	require.Equal(t, beforeCheck, snapshotAppV30ConstructorDB(t, f.bs))
	beforeStale := snapshotAppV30ConstructorDB(t, f.bs)
	// A fresh envelope isolates the persisted cursor fence from nonce rejection.
	staleRaw := composedAppV30Encode(t, makeMemoryTaintWorkTx(t, f.root, f.work[0], nonce+1), f.root, nonce+1, at.Add(time.Second))
	stale, err := f.app.FinalizeBlock(ctx, &abcitypes.RequestFinalizeBlock{
		Height: 21, Time: at.Add(time.Second), Txs: [][]byte{staleRaw},
	})
	require.NoError(t, err)
	require.Len(t, stale.TxResults, 1)
	require.Equal(t, appV30TaintWorkStateCode, stale.TxResults[0].Code, stale.TxResults[0].Log)
	require.Contains(t, stale.TxResults[0].Log, "cursor is stale")
	require.Zero(t, f.app.pendingAppV20Finalize.app.appV30WorkTxCount)
	for _, work := range f.work {
		pending, found, err := f.app.pendingAppV20Finalize.store.GetTaintDecision(work.DecisionID)
		require.NoError(t, err)
		require.True(t, found)
		durable, found, err := f.bs.GetTaintDecision(work.DecisionID)
		require.NoError(t, err)
		require.True(t, found)
		require.Equal(t, durable, pending)
	}
	discard()
	require.Equal(t, beforeStale, snapshotAppV30ConstructorDB(t, f.bs))
	t.Logf("mixed block: txs=%d submits=%d work=%d coauthors=%d carried=%d reserved=%d limit=%d; aggregate physical generated bytes not independently summed", len(txs), len(submitIDs), len(f.work), len(coSigners), carry, estimate, appV30BlockWriteBudgetBytes)
}
