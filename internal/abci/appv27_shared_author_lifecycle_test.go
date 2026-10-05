package abci

import (
	"context"
	"crypto/sha256"
	"sort"
	"testing"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

func seedAppV27SharedAuthorMemory(
	t *testing.T, app *SageApp, memoryID string, author agentKey, status memory.MemoryStatus,
) []byte {
	t.Helper()
	hash := sha256.Sum256([]byte("app-v27-" + memoryID))
	require.NoError(t, app.badgerStore.SetMemoryHash(memoryID, hash[:], string(status)))
	require.NoError(t, app.badgerStore.SetMemoryDomain(memoryID, "general"))
	require.NoError(t, app.badgerStore.SetMemoryClassification(memoryID, 0))
	require.NoError(t, app.badgerStore.SetMemoryAuthor(memoryID, author.id))
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(memoryID, author.id))
	return hash[:]
}

func newAppV27SharedAuthorFixture(t *testing.T) (*SageApp, agentKey, agentKey) {
	t.Helper()
	app := setupTestApp(t)
	root := newAgentKey(t)
	author := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, author, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("app-v27-shared-author", 3))
	app.appV17AppliedHeight = 7
	app.appV21AppliedHeight = 9
	app.appV23AppliedHeight = 10
	app.appV27AppliedHeight = 20
	return app, root, author
}

func TestAppV27ConstantsRefreshAndStrictBoundary(t *testing.T) {
	require.Equal(t, tx.CanonicalUpgradeName(27), appV27UpgradeName)
	require.Equal(t, uint64(28), MaxSupportedAppVersion())
	app := setupTestApp(t)
	app.state.Height = 101
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV26UpgradeName, 26, 90))
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV27UpgradeName, 27, 100))
	app.appV26AppliedHeight = 90
	require.NoError(t, app.refreshAppV27Fork())
	require.Equal(t, int64(100), app.appV27AppliedHeight)
	require.NoError(t, app.validateAppV27Prerequisite())
	require.False(t, app.postAppV27Fork(100))
	require.True(t, app.postAppV27Fork(101))
}

func TestAppV27ActivationCommitsVersionAndAppliedRecord(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 10
	app.appV21AppliedHeight = 20
	app.appV22AppliedHeight = 30
	app.appV23AppliedHeight = 35
	app.appV24AppliedHeight = 40
	app.appV25AppliedHeight = 45
	app.appV26AppliedHeight = 50
	app.state.Height = 59
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV26UpgradeName, 26, 50))
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV27UpgradeName, TargetAppVersion: 27,
		ActivationHeight: 60, ProposedAt: 59,
	}))

	response, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 60, Time: appV23BlockTime(),
	})
	require.NoError(t, err)
	require.NotNil(t, response.ConsensusParamUpdates)
	require.Equal(t, uint64(27), response.ConsensusParamUpdates.Version.App)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, uint64(27), app.currentAppVersion())
	record, err := app.badgerStore.GetAppliedUpgrade(appV27UpgradeName)
	require.NoError(t, err)
	require.Equal(t, &store.AppliedUpgradeRecord{
		Name: appV27UpgradeName, TargetAppVersion: 27, AppliedHeight: 60,
	}, record)
}

func TestAppV27StaticSharedAuthorChallengeIsForwardOnlyAndFrozen(t *testing.T) {
	app, root, author := newAppV27SharedAuthorFixture(t)

	preID := "app-v27-author-pre-boundary"
	seedAppV27SharedAuthorMemory(t, app, preID, author, memory.StatusCommitted)
	pre := app.processMemoryChallenge(
		makeMemoryChallengeTx(t, author, preID, "activation block remains app-v26"),
		20, appV23BlockTime(),
	)
	require.NotZero(t, pre.Code, "the activation block must retain app-v26 authorization")

	postID := "app-v27-author-post-boundary"
	seedAppV27SharedAuthorMemory(t, app, postID, author, memory.StatusCommitted)
	corroborated := app.processMemoryCorroborate(
		makeMemoryCorroborateTx(t, root, postID, "root supports shared record"),
		21, appV23BlockTime(),
	)
	require.Zero(t, corroborated.Code, corroborated.Log)
	post := app.processMemoryChallenge(
		makeMemoryChallengeTx(t, author, postID, "author controls exact shared record"),
		21, appV23BlockTime(),
	)
	require.Zero(t, post.Code, post.Log)
	record, err := app.badgerStore.GetChallengeRecordV21(postID)
	require.NoError(t, err)
	require.NotNil(t, record)
	i := sort.SearchStrings(record.Electorate, author.id)
	require.Less(t, i, len(record.Electorate))
	require.Equal(t, author.id, record.Electorate[i],
		"the author must be part of the immutable app-v21 electorate")
}

func TestAppV27StaticSharedAuthorCanReinstateLegacyRound(t *testing.T) {
	app, root, author := newAppV27SharedAuthorFixture(t)
	memoryID := "app-v27-author-reinstate"
	priorHash := seedAppV27SharedAuthorMemory(t, app, memoryID, author, memory.StatusCommitted)
	require.NoError(t, app.badgerStore.OpenChallenge(
		memoryID, priorHash, string(memory.StatusChallenged), &store.ChallengeRecord{
			ChallengerID: root.id, Domain: "general", ExecutionHeight: 19,
			QuorumCount: 2, PriorHash: priorHash, PriorStatus: string(memory.StatusCommitted),
		},
	))

	result := app.processMemoryReinstate(
		makeMemoryReinstateTx(t, author, memoryID, "author restores exact record"),
		21, appV23BlockTime(),
	)
	require.Zero(t, result.Code, result.Log)
	_, status, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	require.Equal(t, string(memory.StatusCommitted), status)
}

func TestAppV27AuthorAuthorityNeverExtendsBeyondStaticSharedDomains(t *testing.T) {
	app, _, author := newAppV27SharedAuthorFixture(t)
	memoryID := "app-v27-dynamic-shared-excluded"
	hash := sha256.Sum256([]byte(memoryID))
	require.NoError(t, app.badgerStore.SetMemoryHash(memoryID, hash[:], string(memory.StatusCommitted)))
	require.NoError(t, app.badgerStore.SetMemoryDomain(memoryID, "governance-promoted-shared"))
	require.NoError(t, app.badgerStore.SetMemoryClassification(memoryID, 0))
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(memoryID, author.id))

	principal, err := app.appV27StaticSharedAuthorPrincipal(
		memoryID, "governance-promoted-shared", 21, appV23BlockTime(),
	)
	require.NoError(t, err)
	require.Empty(t, principal)
}

func TestAppV27AuthorAuthorityPreservesProfileHardDenial(t *testing.T) {
	app, root, author := newAppV27SharedAuthorFixture(t)
	memoryID := "app-v27-read-only-author"
	seedAppV27SharedAuthorMemory(t, app, memoryID, author, memory.StatusCommitted)
	enrollment, err := app.badgerStore.GetAppV23Enrollment(author.id)
	require.NoError(t, err)
	role, err := app.badgerStore.GetAppV23Role(author.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		root.id, author.id, role.Role, enrollment.Profile, store.AppV23ProfileReadOnly,
		enrollment.Clearance, store.AgentCapabilityReadAllDomains,
		role.Revision, enrollment.Revision, 19,
	))

	principal, err := app.appV27StaticSharedAuthorPrincipal(
		memoryID, "general", 21, appV23BlockTime(),
	)
	require.NoError(t, err)
	require.Empty(t, principal, "record authorship must not override a ReadOnly profile")
}

func TestAppV27IneligibleLegacyAuthorCannotVetoAdminChallenge(t *testing.T) {
	app, root, author := newAppV27SharedAuthorFixture(t)
	memoryID := "app-v27-ineligible-author-no-veto"
	seedAppV27SharedAuthorMemory(t, app, memoryID, author, memory.StatusCommitted)
	unknownAuthor := newAgentKey(t)
	require.NoError(t, app.badgerStore.SetMemoryAuthorPrincipal(memoryID, unknownAuthor.id))

	result := app.processMemoryChallenge(
		makeMemoryChallengeTx(t, root, memoryID, "admin remains independently authorized"),
		21, appV23BlockTime(),
	)
	require.Zero(t, result.Code, result.Log)
}

func TestAppV27AuthorCanReinstatePreV27FrozenRound(t *testing.T) {
	app, root, author := newAppV27SharedAuthorFixture(t)
	memoryID := "app-v27-author-pre-v27-round"
	priorHash := seedAppV27SharedAuthorMemory(t, app, memoryID, author, memory.StatusCommitted)
	other := newAgentKey(t)
	electorate := []string{root.id, other.id}
	sort.Strings(electorate)
	require.NoError(t, app.badgerStore.SetCorroborated(memoryID, other.id))
	evidenceHash := sha256.Sum256([]byte("pre-v27-round"))
	_, err := app.badgerStore.OpenChallengeV21(store.OpenChallengeV21Input{
		MemoryID: memoryID, OpenerID: root.id, Domain: "general",
		ExecutionHeight: 19, ExpectedPriorStatus: string(memory.StatusCommitted),
		ChallengedStatus: string(memory.StatusChallenged),
		ResolvedStatus:   string(memory.StatusDeprecated), ResolvedHash: priorHash,
		Electorate: electorate, EvidenceHash: evidenceHash[:],
	})
	require.NoError(t, err)

	result := app.processMemoryReinstate(
		makeMemoryReinstateTx(t, author, memoryID, "app-v27 author authority"),
		21, appV23BlockTime(),
	)
	require.Zero(t, result.Code, result.Log)
}
