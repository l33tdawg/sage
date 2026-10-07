package abci

import (
	"context"
	"crypto/sha256"
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/require"
)

// The rule's content hash is derived from the envelope's domain, so both halves
// of this test can seed or re-submit exactly those bytes.
func coCommitTombstoneHash(domain string) []byte {
	sum := sha256.Sum256([]byte("co-committed content " + domain))
	return sum[:]
}

func promoteCoCommitTombstoneIndexForTest(t *testing.T, app *SageApp, height int64) {
	t.Helper()
	base, err := app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.PrepareCoCommitTombstoneStage(context.Background(), height))
	transaction := app.badgerStore.BeginConsensusTransaction(nil)
	require.NoError(t, transaction.PromoteCoCommitTombstoneStage(height, base))
	require.NoError(t, transaction.CommitConsensusTransaction())
	require.NoError(t, app.badgerStore.ValidateCoCommitTombstoneStage())
}

// The co-commit tombstone rule is a consensus rule from H+1: the same envelope
// and the same state are admitted before the fork and refused after it, because
// a different memory carrying those exact bytes has already left proposed.
func TestAppV28CoCommitTombstoneRule(t *testing.T) {
	const domain = "family.photos"

	// Pre-fork control: nothing consults the index, so the co-commit commits.
	preFork := setupTestApp(t)
	preFork.appV15AppliedHeight = 5
	localPre := newAgentKey(t)
	envPre, _ := buildCoCommitEnvelope(t, localPre, domain, []byte("pre"), "sage-b")
	pre := preFork.processCoCommitSubmit(coCommitSubmitTx(t, localPre, envPre), 10, time.Now())
	require.Zero(t, pre.Code, "pre-fork submit: %s", pre.Log)

	// Post-fork: a deprecated record carries the same content hash.
	postFork := setupTestApp(t)
	postFork.appV15AppliedHeight = 5
	require.NoError(t, postFork.badgerStore.SetMemoryHash(
		"legacy-deprecated", coCommitTombstoneHash(domain), string(memory.StatusDeprecated)))
	promoteCoCommitTombstoneIndexForTest(t, postFork, 5)
	postFork.appV28AppliedHeight = 5

	localPost := newAgentKey(t)
	envPost, _ := buildCoCommitEnvelope(t, localPost, domain, []byte("post"), "sage-c")
	post := postFork.processCoCommitSubmit(coCommitSubmitTx(t, localPost, envPost), 10, time.Now())
	require.Equal(t, uint32(97), post.Code, "post-fork submit: %s", post.Log)
	require.Contains(t, post.Log, "already left proposed")
	_, status, err := postFork.badgerStore.GetMemoryHash(envPost.SharedID)
	require.Error(t, err, "a refused co-commit writes nothing")
	require.Empty(t, status)

	// Strict H+1 boundary: the activation block itself is still app-v27.
	atActivation := setupTestApp(t)
	atActivation.appV15AppliedHeight = 1 // the co-commit gate must already be open at H
	require.NoError(t, atActivation.badgerStore.SetMemoryHash(
		"legacy-deprecated", coCommitTombstoneHash(domain), string(memory.StatusDeprecated)))
	promoteCoCommitTombstoneIndexForTest(t, atActivation, 5)
	atActivation.appV28AppliedHeight = 5
	localAt := newAgentKey(t)
	envAt, _ := buildCoCommitEnvelope(t, localAt, domain, []byte("at"), "sage-d")
	at := atActivation.processCoCommitSubmit(coCommitSubmitTx(t, localAt, envAt), 5, time.Now())
	require.Zero(t, at.Code, "the activation block keeps app-v27 semantics: %s", at.Log)
}

// A record that is still proposed does not tombstone its bytes, and the
// candidate's own id never counts against itself.
func TestAppV28CoCommitTombstoneRuleAllowsProposedAndSelf(t *testing.T) {
	const domain = "family.photos"
	app := setupTestApp(t)
	app.appV15AppliedHeight = 5
	require.NoError(t, app.badgerStore.SetMemoryHash(
		"still-proposed", coCommitTombstoneHash(domain), string(memory.StatusProposed)))
	promoteCoCommitTombstoneIndexForTest(t, app, 5)
	app.appV28AppliedHeight = 5

	local := newAgentKey(t)
	env, _ := buildCoCommitEnvelope(t, local, domain, []byte("proposed"), "sage-e")
	res := app.processCoCommitSubmit(coCommitSubmitTx(t, local, env), 10, time.Now())
	require.Zero(t, res.Code, "an identical PROPOSED record is not a tombstone: %s", res.Log)
}
