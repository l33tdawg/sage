package abci

import (
	"context"
	"crypto/sha256"
	"sort"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func setupAppV25SharedContinuityFixture(
	t *testing.T,
) (*SageApp, agentKey, agentKey, agentKey, agentKey, string, []string) {
	t.Helper()
	app := setupTestApp(t)
	root := newAgentKey(t)
	writerA := newAgentKey(t)
	writerB := newAgentKey(t)
	outsider := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(
		t, app, writerA, store.AppV23RoleMember, 2,
		store.DefaultSelfRegisteredAgentCapabilities,
	)
	registerAppV23Agent(
		t, app, writerB, store.AppV23RoleMember, 3,
		store.DefaultSelfRegisteredAgentCapabilities,
	)
	registerAppV23Agent(t, app, outsider, store.AppV23RoleMember, 2, 0)
	const domain = "historical-shared-abci"
	require.NoError(t, app.badgerStore.RegisterDomain(domain, root.id, "", 4))
	require.NoError(t, app.badgerStore.EnsureAppV23Root(
		"app-v25-continuity-abci", 100,
	))
	writers := []string{writerA.id, writerB.id}
	sort.Strings(writers)
	plan := sha256.Sum256([]byte("app-v25-continuity-abci-plan"))
	require.NoError(t, app.badgerStore.ApplyAppV25DomainContinuity(
		domain, writers, plan[:], 1, 120,
	))
	app.v8AppliedHeight = 1
	app.appV8AppliedHeight = 1
	app.appV15AppliedHeight = 1
	app.appV20AppliedHeight = 1
	app.appV21AppliedHeight = 1
	app.appV22AppliedHeight = 1
	app.appV23AppliedHeight = 1
	app.appV24AppliedHeight = 1
	app.appV25AppliedHeight = 1
	return app, root, writerA, writerB, outsider, domain, writers
}

func TestAppV25SharedContinuityAllowsSubmitAndCoCommitUntilExplicitRevoke(t *testing.T) {
	app, root, writerA, writerB, outsider, domain, writers :=
		setupAppV25SharedContinuityFixture(t)

	unrelated := makeMemorySubmitTx(
		t, outsider, domain, "unrelated legacy member cannot enter recovered group domain",
	)
	unrelated.MemorySubmit.MemoryID = "app-v25-continuity-unrelated"
	unrelatedResult := app.processMemorySubmit(
		unrelated, 121, appV23BlockTime(),
	)
	require.NotEqual(t, uint32(0), unrelatedResult.Code, unrelatedResult.Log)
	require.Contains(t, unrelatedResult.Log, "missing_write_grant")

	ordinaryShared := makeMemorySubmitTx(
		t, outsider, "general", "ordinary legacy shared write remains available",
	)
	ordinaryShared.MemorySubmit.MemoryID = "app-v25-continuity-ordinary-shared"
	ordinaryResult := app.processMemorySubmit(
		ordinaryShared, 121, appV23BlockTime(),
	)
	require.Equal(t, uint32(0), ordinaryResult.Code, ordinaryResult.Log)

	allowed, denial, err := app.appV23DomainDecision(
		makeMemorySubmitTx(t, writerA, domain, "continuity decision"),
		writerA.id,
		domain,
		store.AppV23VerbWrite,
		121,
		appV23BlockTime(),
	)
	require.NoError(t, err)
	require.True(t, allowed)
	require.Empty(t, denial)

	submit := makeMemorySubmitTx(
		t, writerA, domain, "historical writer can write again",
	)
	submit.MemorySubmit.MemoryID = "app-v25-continuity-submit"
	submitResult := app.processMemorySubmit(submit, 121, appV23BlockTime())
	require.Equal(t, uint32(0), submitResult.Code, submitResult.Log)
	submitHeight, found, err := app.badgerStore.GetMemorySubmissionHeight(
		submit.MemorySubmit.MemoryID,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(121), submitHeight)

	envelope, _ := buildCoCommitEnvelope(
		t, writerA, domain, []byte("app-v25-continuity-cocommit"), "sage-b",
	)
	coCommitResult := app.processCoCommitSubmit(
		coCommitSubmitTx(t, writerA, envelope), 122, appV23BlockTime(),
	)
	require.Equal(t, uint32(0), coCommitResult.Code, coCommitResult.Log)
	coCommitHeight, found, err := app.badgerStore.GetMemorySubmissionHeight(
		envelope.SharedID,
	)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(122), coCommitHeight)

	groupID := store.AppV25DomainContinuityGroupID(writers)
	group, err := app.badgerStore.GetAppV23AccessGroup(groupID)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		root.id, groupID, group.Name, []string{writerB.id},
		group.Revision, false, 123,
	))

	revoked := makeMemorySubmitTx(
		t, writerA, domain, "removed historical writer stays revoked",
	)
	revoked.MemorySubmit.MemoryID = "app-v25-continuity-revoked"
	revokedResult := app.processMemorySubmit(
		revoked, 124, appV23BlockTime(),
	)
	require.NotEqual(t, uint32(0), revokedResult.Code, revokedResult.Log)
	require.Contains(t, revokedResult.Log, "shared")

	remaining := makeMemorySubmitTx(
		t, writerB, domain, "remaining historical writer still works",
	)
	remaining.MemorySubmit.MemoryID = "app-v25-continuity-remaining"
	remainingResult := app.processMemorySubmit(
		remaining, 125, appV23BlockTime(),
	)
	require.Equal(t, uint32(0), remainingResult.Code, remainingResult.Log)
}

func TestAppV25DomainContinuityConflictIsRejectedBeforeExecutedState(t *testing.T) {
	fixture := setupAppV24ReanchorGovernanceFixture(t, 1)
	fixture.app.appV25AppliedHeight = 1

	const domain = "continuity-preexecute-conflict"
	require.NoError(t, fixture.app.badgerStore.RegisterDomain(
		domain, fixture.admin.id, "", 2,
	))
	root, err := fixture.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, root)
	plan := sha256.Sum256([]byte("continuity-preexecute-conflict-plan"))
	payload, err := tx.EncodeDomainContinuityPayload(tx.DomainContinuityPayload{
		Version:          tx.DomainContinuityPayloadLegacyVersion,
		RootCredentialID: root.CredentialID,
		RootGeneration:   root.Generation,
		PlanDigest:       plan[:],
		Domain:           domain,
		Writers:          []string{fixture.validator.id},
	})
	require.NoError(t, err)
	targetID, err := tx.DomainContinuityTargetID(payload)
	require.NoError(t, err)

	proposalID, err := fixture.app.govEngine.ProposeDomainContinuityAdoption(
		fixture.validator.id,
		targetID,
		nil,
		0,
		governance.DefaultExpiryBlocks,
		"reject conflicting current owner before execution",
		2,
		payload,
	)
	require.NoError(t, err)
	voteTime := time.Unix(25_700, 0).UTC()
	vote := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: voteTime,
		GovVote: &tx.GovVote{
			ProposalID: proposalID,
			Decision:   tx.VoteDecisionAccept,
		},
	}
	require.NoError(t, tx.SignTx(vote, fixture.validator.priv))
	rawVote, err := tx.EncodeTx(vote)
	require.NoError(t, err)
	response, err := fixture.app.FinalizeBlock(
		context.Background(),
		&abcitypes.RequestFinalizeBlock{
			Height: 2,
			Time:   voteTime,
			Txs:    [][]byte{rawVote},
		},
	)
	require.NoError(t, err)
	require.Len(t, response.TxResults, 1)
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
	speculative := fixture.app.pendingAppV20Finalize.app

	stored, err := speculative.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	require.Equal(t, governance.StatusRejected, stored.Status)
	active, err := speculative.govEngine.GetActiveProposal()
	require.NoError(t, err)
	require.Nil(t, active)
	owner, err := speculative.badgerStore.GetDomainOwner(domain)
	require.NoError(t, err)
	require.Equal(t, fixture.admin.id, owner)
	record, err := speculative.badgerStore.GetAppV25DomainContinuity(domain)
	require.NoError(t, err)
	require.Nil(t, record)
	commitGovernanceReplayBlock(t, fixture.app)
}

func TestAppV25DomainContinuityV2ValidatesAndAppliesWholeBatch(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	writer := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(
		t, app, writer, store.AppV23RoleMember, 2,
		store.DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, app.badgerStore.RegisterDomain("batch-a", root.id, "", 3))
	require.NoError(t, app.badgerStore.RegisterDomain("batch-b", root.id, "", 4))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("batch-abci", 100))
	app.appV25AppliedHeight = 1
	rootState, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	entries := []tx.DomainContinuityEntry{
		{Domain: "batch-a", Owner: writer.id, Writers: []string{writer.id}},
		{Domain: "batch-b", Owner: writer.id, Writers: []string{writer.id}},
	}
	plan := sha256.Sum256([]byte("batch-abci-plan"))
	payload, err := tx.EncodeDomainContinuityPayload(tx.DomainContinuityPayload{
		Version:          tx.DomainContinuityPayloadVersion,
		RootCredentialID: rootState.CredentialID,
		RootGeneration:   rootState.Generation, PlanDigest: plan[:], Entries: entries,
	})
	require.NoError(t, err)
	targetID, err := tx.DomainContinuityTargetID(payload)
	require.NoError(t, err)
	decoded, err := app.validateAppV25DomainContinuityFields(targetID, nil, 0, payload, 120)
	require.NoError(t, err)
	require.Len(t, tx.DomainContinuityEntries(decoded), 2)
	require.NoError(t, app.applyDomainContinuityAdoption(&governance.ProposalState{
		Operation: governance.OpDomainContinuityAdopt,
		TargetID:  targetID, Payload: payload,
	}, 120))
	for _, entry := range entries {
		record, recordErr := app.badgerStore.GetAppV25DomainContinuity(entry.Domain)
		require.NoError(t, recordErr)
		require.NotNil(t, record)
		allowed, allowErr := app.badgerStore.AuthorizeAppV23LocalDomain(
			writer.id, entry.Domain, store.AppV23VerbWrite, false,
		)
		require.NoError(t, allowErr)
		require.True(t, allowed.Allowed)
	}
}

func TestAppV25DomainContinuityGovernedReplayRepairsStaleGrantAfterCommit(t *testing.T) {
	fixture := setupAppV24ReanchorGovernanceFixture(t, 1)
	fixture.app.appV25AppliedHeight = 1

	const (
		staleDomain   = "continuity-governed-replay-a"
		laterDomain   = "continuity-governed-replay-b"
		missingDomain = "continuity-governed-replay-c"
	)
	for height, domain := range []string{staleDomain, laterDomain, missingDomain} {
		require.NoError(t, fixture.app.badgerStore.RegisterDomain(
			domain, fixture.root.id, "", int64(height+2),
		))
	}

	// The old singleton continuity path bound each grant to the enrollment
	// revision current at that call.  Allocating the writer's later home domain
	// incremented the enrollment revision and made the first grant ineffective.
	firstPlan := sha256.Sum256([]byte("governed-replay-legacy-a"))
	require.NoError(t, fixture.app.badgerStore.ApplyAppV25DomainContinuity(
		staleDomain, []string{fixture.validator.id}, firstPlan[:], 1, 120,
	))
	secondPlan := sha256.Sum256([]byte("governed-replay-legacy-b"))
	require.NoError(t, fixture.app.badgerStore.ApplyAppV25DomainContinuity(
		laterDomain, []string{fixture.validator.id}, secondPlan[:], 1, 121,
	))
	allowed, err := fixture.app.badgerStore.AppV25AllowsHistoricalDomainWrite(
		fixture.validator.id, staleDomain,
	)
	require.NoError(t, err)
	require.False(t, allowed, "fixture must reproduce the revision-stale grant")
	missingRecord, err := fixture.app.badgerStore.GetAppV25DomainContinuity(missingDomain)
	require.NoError(t, err)
	require.Nil(t, missingRecord, "fixture must also exercise the missing-record recovery variant")

	root, err := fixture.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, root)
	entries := []tx.DomainContinuityEntry{
		{Domain: staleDomain, Owner: fixture.validator.id, Writers: []string{fixture.validator.id}},
		{Domain: laterDomain, Owner: fixture.validator.id, Writers: []string{fixture.validator.id}},
		{Domain: missingDomain, Owner: fixture.validator.id, Writers: []string{fixture.validator.id}},
	}
	plan := sha256.Sum256([]byte("governed-replay-exact-evidence"))
	payload, err := tx.EncodeDomainContinuityPayload(tx.DomainContinuityPayload{
		Version:          tx.DomainContinuityPayloadVersion,
		RootCredentialID: root.CredentialID,
		RootGeneration:   root.Generation,
		PlanDigest:       plan[:],
		Entries:          entries,
	})
	require.NoError(t, err)
	targetID, err := tx.DomainContinuityTargetID(payload)
	require.NoError(t, err)
	proposalID, err := fixture.app.govEngine.ProposeDomainContinuityAdoption(
		fixture.validator.id,
		targetID,
		nil,
		0,
		governance.DefaultExpiryBlocks,
		"replay exact historical continuity evidence",
		130,
		payload,
	)
	require.NoError(t, err)

	voteTime := time.Unix(25_900, 0).UTC()
	vote := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: voteTime,
		GovVote: &tx.GovVote{
			ProposalID: proposalID,
			Decision:   tx.VoteDecisionAccept,
		},
	}
	require.NoError(t, tx.SignTx(vote, fixture.validator.priv))
	rawVote, err := tx.EncodeTx(vote)
	require.NoError(t, err)
	response, err := fixture.app.FinalizeBlock(
		context.Background(),
		&abcitypes.RequestFinalizeBlock{
			Height: 131,
			Time:   voteTime,
			Txs:    [][]byte{rawVote},
		},
	)
	require.NoError(t, err)
	require.Len(t, response.TxResults, 1)
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
	commitGovernanceReplayBlock(t, fixture.app)

	proposal, err := fixture.app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	require.Equal(t, governance.StatusExecuted, proposal.Status)
	for _, domain := range []string{staleDomain, laterDomain, missingDomain} {
		allowed, allowErr := fixture.app.badgerStore.AppV25AllowsHistoricalDomainWrite(
			fixture.validator.id, domain,
		)
		require.NoError(t, allowErr)
		require.True(t, allowed, "governed replay must publish the final grant revision for %s", domain)
		decision, authErr := fixture.app.badgerStore.AuthorizeAppV23PolicyPrincipalDomain(
			fixture.validator.id, domain, store.AppV23VerbWrite, false,
		)
		require.NoError(t, authErr)
		require.True(t, decision.Allowed, "ordinary policy authorization must converge for %s", domain)
	}
}
