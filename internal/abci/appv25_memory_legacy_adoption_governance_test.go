package abci

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func appV25LegacyAdoptionPayload(
	t *testing.T,
	fixture appV24ReanchorGovernanceFixture,
	entries []tx.MemoryLegacyAdoptionEntry,
) ([]byte, string) {
	t.Helper()
	root, err := fixture.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, root)
	payload, err := tx.EncodeMemoryLegacyAdoptionPayload(tx.MemoryLegacyAdoptionPayload{
		Version:          1,
		RootCredentialID: root.CredentialID,
		RootGeneration:   root.Generation,
		PlanDigest:       bytes.Repeat([]byte{0x25}, sha256.Size),
		Entries:          entries,
	})
	require.NoError(t, err)
	targetID, err := tx.MemoryLegacyAdoptionTargetID(payload)
	require.NoError(t, err)
	return payload, targetID
}

func appV25LegacyAdoptionProposal(
	t *testing.T,
	fixture appV24ReanchorGovernanceFixture,
	payload []byte,
	targetID string,
	nonce uint64,
	blockTime time.Time,
) *tx.ParsedTx {
	t.Helper()
	body := governanceJSON(t, struct {
		ValidatorID      string `json:"validator_id"`
		GovernanceDomain string `json:"governance_domain"`
		Operation        string `json:"operation"`
		TargetID         string `json:"target_id"`
		Reason           string `json:"reason"`
		Payload          string `json:"payload"`
	}{
		ValidatorID:      fixture.validator.id,
		GovernanceDomain: governanceReplayTestDomain,
		Operation:        appV25MemoryLegacyAdoptionOperationName,
		TargetID:         targetID,
		Reason:           "adopt verified legacy memory envelopes",
		Payload:          base64.StdEncoding.EncodeToString(payload),
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: nonce, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpMemoryLegacyAdopt,
			TargetID:  targetID,
			Reason:    "adopt verified legacy memory envelopes",
			Payload:   append([]byte(nil), payload...),
		},
	}
	attachGovernanceRequestProof(
		t,
		parsed,
		fixture.root,
		fixture.validator,
		"POST",
		"/v1/governance/propose",
		body,
		blockTime,
		[]byte("adoptv25"),
	)
	return parsed
}

func encodeAppV25LegacyAdoptionTx(t *testing.T, parsed *tx.ParsedTx) []byte {
	t.Helper()
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

func TestAppV25ConstantsAndStrictForkBoundary(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(25), appV25UpgradeName)
	require.Equal(t, uint64(26), MaxSupportedAppVersion())

	app := setupTestApp(t)
	app.appV25AppliedHeight = 50
	require.False(t, app.postAppV25Fork(49))
	require.False(t, app.postAppV25Fork(50),
		"activation height must execute under app-v24 semantics")
	require.True(t, app.postAppV25Fork(51),
		"app-v25 semantics must begin strictly at H+1")
}

func TestAppV25ActivationCrashReplayProducesIdenticalAppHash(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 10
	app.appV21AppliedHeight = 20
	app.appV22AppliedHeight = 30
	app.appV23AppliedHeight = 40
	app.appV24AppliedHeight = 50
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(
		appV24UpgradeName, 24, 50,
	))
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV25UpgradeName, TargetAppVersion: 25,
		ActivationHeight: 60, ProposedAt: 59,
	}))
	request := &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: time.Unix(25_060, 0).UTC(),
	}
	first, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.NotNil(t, first.ConsensusParamUpdates)
	require.NotNil(t, first.ConsensusParamUpdates.Version)
	require.Equal(t, uint64(25), first.ConsensusParamUpdates.Version.App)
	firstHash := append([]byte(nil), first.AppHash...)

	// A crash after FinalizeBlock cannot publish the speculative applied
	// record. Replaying the same request must reproduce the exact state hash.
	app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
	app.pendingAppV20Finalize = nil
	applied, err := app.badgerStore.GetAppliedUpgrade(appV25UpgradeName)
	require.NoError(t, err)
	require.Nil(t, applied)
	require.Equal(t, uint64(24), app.currentAppVersion())

	replayed, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, firstHash, replayed.AppHash)
	require.NotNil(t, replayed.ConsensusParamUpdates)
	require.Equal(t, uint64(25), replayed.ConsensusParamUpdates.Version.App)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, uint64(25), app.currentAppVersion())
	require.False(t, app.postAppV25Rules(60))
	require.True(t, app.postAppV25Rules(61))
}

func TestAppV25MemoryLegacyAdoptionIsStrictHPlusOneAndExplicitVote(t *testing.T) {
	fixture := setupAppV24ReanchorGovernanceFixture(t, 1)
	fixture.app.appV25AppliedHeight = 10
	root, err := fixture.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	payload, targetID := appV25LegacyAdoptionPayload(t, fixture, []tx.MemoryLegacyAdoptionEntry{{
		MemoryID:        "legacy-boundary",
		Status:          "committed",
		ContentHash:     bytes.Repeat([]byte{0x42}, sha256.Size),
		Domain:          "historical/domain",
		Author:          root.CredentialID,
		AuthorPrincipal: root.PrincipalID,
		Classification:  1,
	}})

	atActivation := appV25LegacyAdoptionProposal(
		t, fixture, payload, targetID, 1, time.Unix(25_010, 0).UTC(),
	)
	result := fixture.app.processTx(atActivation, 10, atActivation.Timestamp)
	require.Equal(t, uint32(72), result.Code, result.Log)
	require.Contains(t, result.Log, "unknown operation 10")

	afterActivation := appV25LegacyAdoptionProposal(
		t, fixture, payload, targetID, 2, time.Unix(25_011, 0).UTC(),
	)
	result = fixture.app.processTx(afterActivation, 11, afterActivation.Timestamp)
	require.Zero(t, result.Code, result.Log)

	proposalID := governance.ComputeProposalID(
		fixture.validator.id, 11, governance.OpMemoryLegacyAdopt, targetID,
	)
	proposal, err := fixture.app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	require.Equal(t, governance.StatusVoting, proposal.Status)
	vote, err := fixture.app.badgerStore.GetState(
		"gov:vote:" + proposalID + ":" + fixture.validator.id,
	)
	require.NoError(t, err)
	require.Empty(t, vote, "op 10 must require an explicit validator attestation")
	require.Equal(
		t,
		appV25MemoryLegacyAdoptionOperationName,
		fixture.app.governanceOperationName(governance.OpMemoryLegacyAdopt, 11),
	)
	require.Equal(
		t,
		"unknown_10",
		fixture.app.governanceOperationName(governance.OpMemoryLegacyAdopt, 10),
	)
}

func TestAppV26MemoryLegacyAssignmentIsStrictHPlusOne(t *testing.T) {
	fixture := setupAppV24ReanchorGovernanceFixture(t, 1)
	fixture.app.appV25AppliedHeight = 1
	fixture.app.appV26AppliedHeight = 10
	payload, targetID := appV25LegacyAdoptionPayload(t, fixture, []tx.MemoryLegacyAdoptionEntry{{
		MemoryID: "legacy-assigned-boundary", Status: "committed",
		ContentHash: bytes.Repeat([]byte{0x26}, sha256.Size),
		Domain: "historical/domain", Author: "retired application label",
		AuthorPrincipal: fixture.admin.id, Classification: 1,
	}})

	_, err := fixture.app.validateAppV25MemoryLegacyAdoptionFields(
		targetID, nil, 0, payload, 10, true,
	)
	require.Error(t, err, "the activation height must retain app-v25 principal rules")
	_, err = fixture.app.validateAppV25MemoryLegacyAdoptionFields(
		targetID, nil, 0, payload, 11, true,
	)
	require.NoError(t, err, "H+1 permits Root-governed mapping to an active local Admin")
}

func TestAppV25MemoryLegacyAdoptionSingleValidatorAppliesCompleteEnvelopeAtomically(t *testing.T) {
	fixture := setupAppV24ReanchorGovernanceFixture(t, 1)
	fixture.app.appV25AppliedHeight = 1
	root, err := fixture.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	entries := []tx.MemoryLegacyAdoptionEntry{
		{
			MemoryID:        "legacy-a",
			Status:          "committed",
			ContentHash:     bytes.Repeat([]byte{0xa1}, sha256.Size),
			Domain:          "historical/no-current-owner",
			Author:          root.CredentialID,
			AuthorPrincipal: root.PrincipalID,
			Classification:  1,
		},
		{
			MemoryID:        "legacy-b",
			Status:          "deprecated",
			ContentHash:     bytes.Repeat([]byte{0xb2}, sha256.Size),
			Domain:          "historical/no-current-owner",
			Author:          root.CredentialID,
			AuthorPrincipal: root.PrincipalID,
			Classification:  2,
		},
	}
	payload, targetID := appV25LegacyAdoptionPayload(t, fixture, entries)
	proposal := appV25LegacyAdoptionProposal(
		t, fixture, payload, targetID, 1, time.Unix(25_102, 0).UTC(),
	)
	response, err := fixture.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 2,
		Time:   proposal.Timestamp,
		Txs:    [][]byte{encodeAppV25LegacyAdoptionTx(t, proposal)},
	})
	require.NoError(t, err)
	require.Len(t, response.TxResults, 1)
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
	for _, entry := range entries {
		_, _, stateErr := fixture.app.pendingAppV20Finalize.app.badgerStore.GetMemoryHash(entry.MemoryID)
		require.Error(t, stateErr, "proposal creation alone must never adopt a row")
	}
	commitGovernanceReplayBlock(t, fixture.app)

	proposalID := governance.ComputeProposalID(
		fixture.validator.id, 2, governance.OpMemoryLegacyAdopt, targetID,
	)
	vote := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: 2, Timestamp: time.Unix(25_103, 0).UTC(),
		GovVote: &tx.GovVote{
			ProposalID: proposalID,
			Decision:   tx.VoteDecisionAccept,
		},
	}
	require.NoError(t, tx.SignTx(vote, fixture.validator.priv))
	response, err = fixture.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 3,
		Time:   vote.Timestamp,
		Txs:    [][]byte{encodeAppV25LegacyAdoptionTx(t, vote)},
	})
	require.NoError(t, err)
	require.Len(t, response.TxResults, 1)
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
	executed, err := fixture.app.pendingAppV20Finalize.app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	require.Equal(t, governance.StatusExecuted, executed.Status)
	for _, entry := range entries {
		state, stateErr := fixture.app.pendingAppV20Finalize.app.badgerStore.GetMemoryDisclosureState(entry.MemoryID)
		require.NoError(t, stateErr)
		require.Equal(t, entry.ContentHash, state.ContentHash)
		require.Equal(t, entry.Status, state.Status)
		require.Equal(t, entry.Domain, state.Domain)
		require.Equal(t, entry.Author, state.Author)
		require.Equal(t, entry.AuthorPrincipal, state.AuthorPrincipal)
		require.Equal(t, entry.Classification, state.Classification)
	}
	commitGovernanceReplayBlock(t, fixture.app)
}

func TestAppV25MemoryLegacyAdoptionRootBindingCannotBeReplayedAfterRotation(t *testing.T) {
	fixture := setupAppV24ReanchorGovernanceFixture(t, 1)
	fixture.app.appV25AppliedHeight = 1
	root, err := fixture.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	payload, targetID := appV25LegacyAdoptionPayload(t, fixture, []tx.MemoryLegacyAdoptionEntry{{
		MemoryID:        "legacy-root-bound",
		Status:          "committed",
		ContentHash:     bytes.Repeat([]byte{0xcc}, sha256.Size),
		Domain:          "historical/domain",
		Author:          root.CredentialID,
		AuthorPrincipal: root.PrincipalID,
		Classification:  1,
	}})

	decoded, err := tx.DecodeMemoryLegacyAdoptionPayload(payload)
	require.NoError(t, err)
	decoded.RootGeneration++
	stalePayload, err := tx.EncodeMemoryLegacyAdoptionPayload(*decoded)
	require.NoError(t, err)
	staleTarget, err := tx.MemoryLegacyAdoptionTargetID(stalePayload)
	require.NoError(t, err)
	require.NotEqual(t, hex.EncodeToString([]byte(targetID)), hex.EncodeToString([]byte(staleTarget)))

	parsed := appV25LegacyAdoptionProposal(
		t, fixture, stalePayload, staleTarget, 1, time.Unix(25_202, 0).UTC(),
	)
	result := fixture.app.processTx(parsed, 2, parsed.Timestamp)
	require.Equal(t, uint32(72), result.Code, result.Log)
	require.Contains(t, result.Log, "Root binding is stale")
}
