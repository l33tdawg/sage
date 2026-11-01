package abci

import (
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
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
	memberSigners, err := app.appV30ProvenanceSigners(member.id, member.id, 20)
	require.NoError(t, err)
	_, err = app.preflightAppV30Admission(
		submit,
		member.id,
		memoryIDForSubmit(submit.MemorySubmit, 20, member.id),
		submit.MemorySubmit.DerivedFrom,
		memberSigners,
		20,
		appV23BlockTime(),
	)
	require.ErrorIs(t, err, store.ErrMemoryLineageUnavailable)
	require.NotContains(t, err.Error(), parent.MemoryID)

	wrongHash := parent
	wrongHash.ContentHash = sha256.Sum256([]byte("wrong parent hash"))
	_, wrongHashErr := app.preflightAppV30Admission(
		submit,
		member.id,
		memoryIDForSubmit(submit.MemorySubmit, 20, member.id),
		[]tx.ParentRef{wrongHash},
		memberSigners,
		20,
		appV23BlockTime(),
	)
	require.ErrorIs(t, wrongHashErr, store.ErrMemoryLineageUnavailable)
	require.Equal(t, err.Error(), wrongHashErr.Error(),
		"unauthorized and wrong-hash parent probes must be indistinguishable")

	missing := parent
	missing.MemoryID = "missing-parent"
	_, missingErr := app.preflightAppV30Admission(
		submit,
		member.id,
		memoryIDForSubmit(submit.MemorySubmit, 20, member.id),
		[]tx.ParentRef{missing},
		memberSigners,
		20,
		appV23BlockTime(),
	)
	require.ErrorIs(t, missingErr, store.ErrMemoryLineageUnavailable)
	require.Equal(t, err.Error(), missingErr.Error(),
		"unauthorized and missing-parent probes must be indistinguishable")
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

func TestAppV30MemorySubmitInheritsMatchingCredentialScope(t *testing.T) {
	app, root, target, _ := setupAppV30TaintGovernance(t)

	apply := appV30ApplyPayload(target)
	apply.ScopeKind = tx.TaintScopeCredentialSet
	apply.Credentials = []string{root.id}
	payload, err := tx.EncodeTaintApplyPayload(apply)
	require.NoError(t, err)
	decisionID, err := tx.TaintApplyDecisionID(payload)
	require.NoError(t, err)
	require.NoError(t, app.applyAppV30TaintProposal(&governance.ProposalState{
		Operation: governance.OpMemoryTaint,
		TargetID:  decisionID,
		Payload:   payload,
	}, 20))

	submit := makeMemorySubmitTx(t, root, "general", "scope admission child")
	result := app.processMemorySubmit(submit, 20, appV23BlockTime())
	require.Equal(t, uint32(0), result.Code, result.Log)
	memoryID := memoryIDForSubmit(submit.MemorySubmit, 20, root.id)
	decisions, err := app.badgerStore.GetEffectiveTaintDecisions(memoryID)
	require.NoError(t, err)
	require.Equal(t, []string{decisionID}, decisions)
}
