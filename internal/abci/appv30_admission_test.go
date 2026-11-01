package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

func activateAppV30ForMemorySubmitTest(t *testing.T, app *SageApp, height int64) {
	t.Helper()
	app.appV20AppliedHeight = 2
	app.appV21AppliedHeight = 3
	app.appV22AppliedHeight = 4
	app.appV23AppliedHeight = 5
	app.appV24AppliedHeight = 6
	app.appV25AppliedHeight = 7
	app.appV26AppliedHeight = 8
	app.appV27AppliedHeight = 9
	app.appV28AppliedHeight = 10
	app.appV29AppliedHeight = 11
	app.appV30AppliedHeight = height - 1
}

func TestAppV30MemorySubmitWritesImmutableProvenance(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-provenance", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	activateAppV30ForMemorySubmitTest(t, app, 20)

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, root, "general", "app-v30 provenance content"),
		20,
		appV23BlockTime(),
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
	memoryID := string(result.Data)

	signers, found, err := app.badgerStore.GetAppV30Provenance(memoryID)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, []store.AppV30ProvenanceSigner{{
		CredentialID:   root.id,
		PrincipalID:    rootState.PrincipalID,
		RootGeneration: rootState.Generation,
		Exact:          true,
	}}, signers)
}

func TestAppV30ProvenanceStaysDormantBeforeHPlusOne(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-dormant", 2))
	activateAppV30ForMemorySubmitTest(t, app, 20)
	app.appV30AppliedHeight = 20

	result := app.processMemorySubmit(
		makeMemorySubmitTx(t, root, "general", "activation block has no v30 marker"),
		20,
		appV23BlockTime(),
	)
	require.Equal(t, uint32(0), result.Code, result.Log)
	_, found, err := app.badgerStore.GetAppV30Provenance(string(result.Data))
	require.NoError(t, err)
	require.False(t, found)
}

func TestAppV30ProvenanceConflictRejectsBeforeMemoryWrites(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	other := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-conflict", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	activateAppV30ForMemorySubmitTest(t, app, 20)

	submit := makeMemorySubmitTx(t, root, "general", "conflicting provenance content")
	memoryID := memoryIDForSubmit(submit.MemorySubmit, 20, root.id)
	require.NoError(t, app.badgerStore.SetAppV30Provenance(
		memoryID,
		[]store.AppV30ProvenanceSigner{{
			CredentialID:   other.id,
			PrincipalID:    rootState.PrincipalID,
			RootGeneration: 0,
			Exact:          true,
		}},
	))
	pendingBefore := len(app.pendingWrites)

	result := app.processMemorySubmit(submit, 20, appV23BlockTime())
	require.Equal(t, uint32(11), result.Code)
	require.Contains(t, result.Log, store.ErrAppV30ProvenanceConflict.Error())
	require.Len(t, app.pendingWrites, pendingBefore)
}

func TestAppV30LineageAdmissionWritesBothIndexesAndInheritsTaint(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-lineage", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	const chainID = "app-v30-lineage-chain"
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(chainID))
	activateAppV30ForMemorySubmitTest(t, app, 20)

	parentContentHash := sha256.Sum256([]byte("committed parent"))
	parent := tx.ParentRef{
		SourceChainID: chainID,
		MemoryID:      "lineage-parent",
		ContentHash:   parentContentHash,
	}
	require.NoError(t, app.badgerStore.SetMemoryHash(
		parent.MemoryID,
		parent.ContentHash[:],
		"committed",
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain(parent.MemoryID, "general"))
	require.NoError(t, app.badgerStore.SetMemoryAuthor(parent.MemoryID, root.id))
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(
		parent.MemoryID,
		rootState.PrincipalID,
	))
	require.NoError(t, app.badgerStore.SetMemoryClassification(
		parent.MemoryID,
		uint8(store.ClearanceInternal),
	))
	decisionID := strings.Repeat("d", taint.DecisionIDBytes)
	created, err := app.badgerStore.ApplyTaintDecision(
		chainID,
		decisionID,
		parent,
		sha256.Sum256([]byte("taint payload")),
	)
	require.NoError(t, err)
	require.True(t, created)

	submit := makeMemorySubmitTx(t, root, "general", "derived child")
	submit.MemorySubmit.DerivedFrom = []tx.ParentRef{parent}
	scoped := app.badgerStore.BeginConsensusTransaction(nil)
	defer scoped.DiscardConsensusTransaction()
	working := app.cloneForAppV20Finalize(scoped)
	result := working.processMemorySubmit(submit, 20, appV23BlockTime())
	require.Equal(t, uint32(0), result.Code, result.Log)
	childID := string(result.Data)

	invisibleLineage, err := app.badgerStore.GetMemoryLineage(childID)
	require.NoError(t, err)
	require.Empty(t, invisibleLineage)
	invisibleDecisions, err := app.badgerStore.GetEffectiveTaintDecisions(childID)
	require.NoError(t, err)
	require.Empty(t, invisibleDecisions)
	require.NoError(t, scoped.CommitConsensusTransaction())

	lineage, err := app.badgerStore.GetMemoryLineage(childID)
	require.NoError(t, err)
	require.Equal(t, []tx.ParentRef{parent}, lineage)
	decisions, err := app.badgerStore.GetEffectiveTaintDecisions(childID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)
	revision, err := app.badgerStore.TaintRevision()
	require.NoError(t, err)
	require.Equal(t, uint64(2), revision, "apply and admission inheritance each advance taintrev")

	parentID, err := tx.ParentRefIDHex(parent)
	require.NoError(t, err)
	require.Len(t, parentID, 64)
	children, err := app.badgerStore.ListLineageChildren(parentID, "", 10)
	require.NoError(t, err)
	require.Equal(t, []string{childID}, children)
}

func TestAppV30LineagePreflightRejectsParentOutsideClearance(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, store.AgentCapabilityReadAllDomains)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 3, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v30-clearance", 2))
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)
	const chainID = "app-v30-clearance-chain"
	require.NoError(t, app.SetExpectedGovernanceDelegationDomain(chainID))
	activateAppV30ForMemorySubmitTest(t, app, 20)

	parentContentHash := sha256.Sum256([]byte("top secret parent"))
	parent := tx.ParentRef{
		SourceChainID: chainID,
		MemoryID:      "classified-parent",
		ContentHash:   parentContentHash,
	}
	require.NoError(t, app.badgerStore.SetMemoryHash(
		parent.MemoryID,
		parent.ContentHash[:],
		"committed",
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain(parent.MemoryID, "general"))
	require.NoError(t, app.badgerStore.SetMemoryAuthor(parent.MemoryID, root.id))
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(
		parent.MemoryID,
		rootState.PrincipalID,
	))
	require.NoError(t, app.badgerStore.SetMemoryClassification(
		parent.MemoryID,
		uint8(store.ClearanceTopSecret),
	))

	submit := makeMemorySubmitTx(t, member, "general", "unauthorized child")
	submit.MemorySubmit.DerivedFrom = []tx.ParentRef{parent}
	_, err = app.preflightAppV30Lineage(
		submit,
		member.id,
		memoryIDForSubmit(submit.MemorySubmit, 20, member.id),
		submit.MemorySubmit.DerivedFrom,
		20,
		appV23BlockTime(),
	)
	require.ErrorContains(t, err, "outside caller clearance")
}

func TestAppV30CoCommitWritesEverySignerProvenance(t *testing.T) {
	app, root, _, _ := setupAppV30TaintGovernance(t)
	app.appV15AppliedHeight = 5
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, rootState)

	envelope, foreign := buildCoCommitEnvelope(
		t,
		root,
		"general",
		[]byte("app-v30-coauthor-provenance"),
		"sage-v30-foreign",
	)
	result := app.processCoCommitSubmit(
		coCommitSubmitTx(t, root, envelope),
		20,
		appV23BlockTime(),
	)
	require.Equal(t, uint32(0), result.Code, result.Log)

	signers, found, err := app.badgerStore.GetAppV30Provenance(envelope.SharedID)
	require.NoError(t, err)
	require.True(t, found)
	require.Len(t, signers, 2)
	byCredential := make(map[string]store.AppV30ProvenanceSigner, len(signers))
	for _, signer := range signers {
		byCredential[signer.CredentialID] = signer
	}
	require.Equal(t, store.AppV30ProvenanceSigner{
		CredentialID:   root.id,
		PrincipalID:    rootState.PrincipalID,
		RootGeneration: rootState.Generation,
		Exact:          true,
	}, byCredential[root.id])
	foreignID := hex.EncodeToString(foreign.pub)
	require.Equal(t, store.AppV30ProvenanceSigner{
		CredentialID: foreignID,
		PrincipalID:  foreignID,
		Exact:        true,
	}, byCredential[foreignID])
}

func TestAppV30CoCommitConflictPreservesAcceptedMemory(t *testing.T) {
	for _, test := range []struct {
		name   string
		height int64
	}{
		{name: "same_block", height: 20},
		{name: "later_block", height: 21},
	} {
		t.Run(test.name, func(t *testing.T) {
			app, root, _, _ := setupAppV30TaintGovernance(t)
			app.appV15AppliedHeight = 5
			frontRunner := newAgentKey(t)
			registerAppV23Agent(t, app, frontRunner, store.AppV23RoleMember, 2, 0)
			require.NoError(t, app.badgerStore.ApproveAppV23LocalAgent(
				store.AppV23LocalEnrollment{
					AgentID: frontRunner.id, ApprovedBy: root.id, RootGeneration: 1,
					Profile: store.AppV23ProfileStandard, HomeDomain: "front-runner-home",
					Clearance: 4, Active: true, UpdatedHeight: 19,
				}, store.AppV23RoleMember, 0, 0,
			))
			envelope, _ := buildCoCommitEnvelope(
				t, root, "general", []byte("app-v30 co-commit collision"), "sage-peer",
			)

			// An admitted ordinary member can select the predictable SharedID
			// for a normal memory in its own home. Both transactions enter the
			// consensus dispatcher with valid outer signatures and wire bytes.
			submit := makeMemorySubmitTx(t, frontRunner, "front-runner-home", "accepted normal memory before co-commit")
			submit.MemorySubmit.MemoryID = envelope.SharedID
			submit.Nonce = 1
			require.NoError(t, tx.SignTx(submit, frontRunner.priv))
			rawSubmit, err := tx.EncodeTx(submit)
			require.NoError(t, err)
			decodedSubmit, err := tx.DecodeTx(rawSubmit)
			require.NoError(t, err)
			accepted := app.processTx(decodedSubmit, 20, appV23BlockTime())
			require.Zero(t, accepted.Code, accepted.Log)
			before, err := app.badgerStore.GetMemoryDisclosureState(envelope.SharedID)
			require.NoError(t, err)
			beforeProvenance, found, err := app.badgerStore.GetAppV30Provenance(envelope.SharedID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, []store.AppV30ProvenanceSigner{{
				CredentialID: frontRunner.id, PrincipalID: frontRunner.id, Exact: true,
			}}, beforeProvenance)
			beforeHash, err := app.badgerStore.ComputeAppHash()
			require.NoError(t, err)
			pendingBefore := len(app.pendingWrites)

			coCommit := coCommitSubmitTx(t, root, envelope)
			coCommit.Nonce = 1
			require.NoError(t, tx.SignTx(coCommit, root.priv))
			rawCoCommit, err := tx.EncodeTx(coCommit)
			require.NoError(t, err)
			decodedCoCommit, err := tx.DecodeTx(rawCoCommit)
			require.NoError(t, err)
			result := app.processTx(decodedCoCommit, test.height, appV23BlockTime())
			require.Equal(t, uint32(98), result.Code)
			require.Contains(t, result.Log, store.ErrAppV30ProvenanceConflict.Error())

			after, err := app.badgerStore.GetMemoryDisclosureState(envelope.SharedID)
			require.NoError(t, err)
			require.Equal(t, before, after, "a rejected co-commit must preserve the accepted hash and author")
			afterProvenance, found, err := app.badgerStore.GetAppV30Provenance(envelope.SharedID)
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, beforeProvenance, afterProvenance)
			afterHash, err := app.badgerStore.ComputeAppHash()
			require.NoError(t, err)
			require.Equal(t, beforeHash, afterHash, "provenance conflicts must reject before canonical mutation")
			require.Len(t, app.pendingWrites, pendingBefore)
			core, err := app.badgerStore.GetCoCommitCore(envelope.SharedID)
			require.NoError(t, err)
			require.Empty(t, core)
		})
	}
}
