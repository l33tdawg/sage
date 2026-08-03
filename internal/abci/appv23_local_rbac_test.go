package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/authzdenial"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

func appV23BlockTime() time.Time { return time.Unix(1_700_000_000, 0).UTC() }

func signAppV23Outer(t *testing.T, parsed *tx.ParsedTx, signer agentKey, nonce uint64) {
	t.Helper()
	parsed.Nonce = nonce
	parsed.Timestamp = appV23BlockTime()
	require.NoError(t, tx.SignTx(parsed, signer.priv))
}

func attachAppV23Elevation(t *testing.T, parsed *tx.ParsedTx, root, admin agentKey, scope, nonce string, height int64) {
	t.Helper()
	action, err := tx.PayloadBytes(parsed)
	require.NoError(t, err)
	proof := &tx.LocalElevationProof{
		RootGeneration: 1, ValidFromHeight: height, ValidUntilHeight: height + 2,
		Nonce: nonce,
	}
	proof.Signature = ed25519.Sign(root.priv, tx.AppV23ElevationSignBytes(
		scope, admin.id, parsed.Type, action, proof,
	))
	parsed.LocalElevation = proof
}

func registerAppV23Agent(t *testing.T, app *SageApp, key agentKey, role string, height int64, caps store.AgentCapabilities) {
	t.Helper()
	require.NoError(t, app.badgerStore.RegisterAgentWithCapabilities(
		key.id, key.id, role, "", "", "", height, caps,
	))
}

func promoteAppV23TestAdmin(
	t *testing.T,
	app *SageApp,
	root, admin agentKey,
	height int64,
) {
	t.Helper()
	enrollment, err := app.badgerStore.GetAppV23Enrollment(admin.id)
	require.NoError(t, err)
	role, err := app.badgerStore.GetAppV23Role(admin.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		root.id, admin.id, store.AppV23RoleAdmin,
		enrollment.Profile, store.AppV23ProfileStandard,
		4, store.AgentCapabilityReadAllDomains,
		role.Revision, enrollment.Revision, height,
	))
}

func TestAppV23MemoryReassignIsRetiredWithoutProjectionMutation(t *testing.T) {
	app := setupTestApp(t)
	source := newAgentKey(t)
	target := newAgentKey(t)
	content := "immutable historical authorship"
	contentHash := memory.ComputeContentHash(content)
	record := &memory.MemoryRecord{
		MemoryID:        "app-v23-immutable-author",
		SubmittingAgent: source.id,
		Content:         content,
		ContentHash:     contentHash,
		MemoryType:      memory.TypeObservation,
		DomainTag:       "history",
		ConfidenceScore: 0.9,
		Status:          memory.StatusProposed,
		CreatedAt:       appV23BlockTime(),
	}
	require.NoError(t, app.offchainStore.InsertMemory(context.Background(), record))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		record.MemoryID, record.ContentHash, string(record.Status),
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain(record.MemoryID, record.DomainTag))
	require.NoError(t, app.badgerStore.SetMemoryAuthor(record.MemoryID, source.id))
	require.NoError(t, app.badgerStore.SetMemoryClassification(
		record.MemoryID, uint8(store.ClearanceInternal),
	))

	app.appV23AppliedHeight = 10
	pendingBefore := len(app.pendingWrites)
	result := app.processMemoryReassign(
		makeMemoryReassignTx(t, source, source.id, target.id),
		11, appV23BlockTime(),
	)
	require.Equal(t, uint32(66), result.Code, result.Log)
	require.Contains(t, result.Log, "canonical authorship is immutable")
	require.Len(t, app.pendingWrites, pendingBefore)

	projected, err := app.offchainStore.GetMemory(context.Background(), record.MemoryID)
	require.NoError(t, err)
	require.Equal(t, source.id, projected.SubmittingAgent)
	canonicalAuthor, err := app.badgerStore.GetMemoryAuthor(record.MemoryID)
	require.NoError(t, err)
	require.Equal(t, source.id, canonicalAuthor)
	_, err = app.badgerStore.ValidateMemoryProjection(projected)
	require.NoError(t, err, "the rejected mutation must leave CEREBRUM disclosure valid")
}

func TestAppV23DomainReassignCannotReleaseActiveRequiredHome(t *testing.T) {
	for _, tc := range []struct {
		name         string
		openToShared bool
	}{
		{name: "transfer"},
		{name: "dynamic shared promotion", openToShared: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := setupTestApp(t)
			activateV8(t, app, 1)
			root := newAgentKey(t)
			source := newAgentKey(t)
			target := newAgentKey(t)
			registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 2, 0)
			registerAppV23Agent(t, app, source, store.AppV23RoleMember, 3, 0)
			registerAppV23Agent(t, app, target, store.AppV23RoleMember, 4, 0)
			require.NoError(t, app.badgerStore.EnsureAppV23Root("domain-reassign-scope", 10))
			app.appV23AppliedHeight = 10

			sourceEnrollment, err := app.badgerStore.GetAppV23Enrollment(source.id)
			require.NoError(t, err)
			require.True(t, sourceEnrollment.Active)
			require.NotEmpty(t, sourceEnrollment.HomeDomain)

			newOwner := target.id
			if tc.openToShared {
				newOwner = source.id
			}
			body := tx.DomainReassign{
				Domain:       sourceEnrollment.HomeDomain,
				NewOwnerID:   newOwner,
				OpenToShared: tc.openToShared,
			}
			body.ProposalID = seedExecutedReassignProposal(t, app, root.id, body, 11)
			result := app.processDomainReassign(
				makeDomainReassignTx(t, root, &body, 1),
				12,
				appV23BlockTime(),
			)
			require.Equal(t, uint32(88), result.Code, result.Log)
			require.Contains(t, result.Log, "required home domain")
			owner, err := app.badgerStore.GetDomainOwner(sourceEnrollment.HomeDomain)
			require.NoError(t, err)
			require.Equal(t, source.id, owner)
			shared, err := app.badgerStore.GetState("shared_domain:" + sourceEnrollment.HomeDomain)
			require.NoError(t, err)
			require.Empty(t, shared)
			require.NoError(t, app.badgerStore.ValidateAppV23State())
		})
	}
}

func TestQueryAppV23RootReturnsCurrentPublicSemanticState(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("query-scope", 2))

	response, err := app.Query(context.Background(), &abcitypes.RequestQuery{Path: "/appv23/root"})
	require.NoError(t, err)
	require.Zero(t, response.Code)
	var value struct {
		PrincipalID   string `json:"principal_id"`
		CredentialID  string `json:"credential_id"`
		Generation    uint64 `json:"generation"`
		HistoryDigest string `json:"history_digest"`
	}
	require.NoError(t, json.Unmarshal(response.Value, &value))
	require.Equal(t, rootKey.id, value.PrincipalID)
	require.Equal(t, rootKey.id, value.CredentialID)
	require.Equal(t, uint64(1), value.Generation)
	require.Len(t, value.HistoryDigest, sha256.Size*2)
	initialHistoryDigest := value.HistoryDigest

	newRoot := newAgentKey(t)
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, newRoot.id, 3))
	response, err = app.Query(context.Background(), &abcitypes.RequestQuery{Path: "/appv23/root"})
	require.NoError(t, err)
	require.Zero(t, response.Code)
	require.NoError(t, json.Unmarshal(response.Value, &value))
	require.Equal(t, rootKey.id, value.PrincipalID)
	require.Equal(t, newRoot.id, value.CredentialID)
	require.Equal(t, uint64(2), value.Generation)
	require.Len(t, value.HistoryDigest, sha256.Size*2)
	require.NotEqual(t, initialHistoryDigest, value.HistoryDigest)
}

func TestAppV23RootRotationMapsNewCredentialAndDeniesOld(t *testing.T) {
	app := setupTestApp(t)
	oldRoot := newAgentKey(t)
	newRoot := newAgentKey(t)
	admin := newAgentKey(t)
	registerAppV23Agent(t, app, oldRoot, "admin", 1, 0)
	registerAppV23Agent(t, app, admin, "admin", 2, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("root-home", oldRoot.id, "", 3))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("rotation-scope", 10))
	promoteAppV23TestAdmin(t, app, oldRoot, admin, 10)
	_, _, effectiveAdminRole, err := app.appV23Actor(admin.id)
	require.NoError(t, err)
	require.Equal(t, store.AppV23RoleAdmin, effectiveAdminRole.Role)
	app.appV23AppliedHeight = 10

	prePub, preSig, preHash, preTS := signAgentProof(t, oldRoot, []byte("pre-rotation-root-write"))
	preRotationWrite := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID: "pre-rotation-root-write", ContentHash: make([]byte, 32),
			MemoryType: tx.MemoryTypeObservation, DomainTag: "root-home",
			Classification: 0, ConfidenceScore: 0.9,
		},
		AgentPubKey: prePub, AgentSig: preSig,
		AgentBodyHash: preHash, AgentTimestamp: preTS,
	}
	signAppV23Outer(t, preRotationWrite, oldRoot, 1)
	preRotationResult := app.processTx(preRotationWrite, 11, appV23BlockTime())
	require.Zero(t, preRotationResult.Code, preRotationResult.Log)

	rotation := &tx.RootCredentialRotate{
		ExpectedGeneration: 1,
		NewCredentialID:    newRoot.id,
		Scope:              "rotation-scope",
	}
	rotation.NewCredentialSignature = ed25519.Sign(
		newRoot.priv, tx.RootCredentialRotationSignBytes(oldRoot.id, rotation),
	)
	pub, sig, bodyHash, ts := signAgentProof(t, oldRoot, []byte("rotate-root"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeRootCredentialRotate, RootCredentialRotate: rotation,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	result := app.processRootCredentialRotateV23(parsed, 11, appV23BlockTime())
	require.Zero(t, result.Code, result.Log)

	root, enrollment, role, err := app.appV23Actor(newRoot.id)
	require.NoError(t, err)
	require.Equal(t, oldRoot.id, root.PrincipalID)
	require.Equal(t, newRoot.id, root.CredentialID)
	require.Equal(t, oldRoot.id, enrollment.AgentID)
	require.Equal(t, store.AppV23RoleAdmin, role.Role)
	_, _, _, err = app.appV23Actor(oldRoot.id)
	require.True(t, errors.Is(err, store.ErrAppV23NeedsApproval))
	require.False(t, app.isGlobalAdminAgent(oldRoot.id, 12))
	require.True(t, app.isGlobalAdminAgent(newRoot.id, 12))
	oldRootTx := makeAgentRegisterTx(t, oldRoot, "stale-root", "admin", "", "", "")
	require.Error(t, app.enforceAppV23ControlElevation(oldRootTx, 12),
		"the rotated credential must not fall through to legacy Role==admin checks")
	pubCurrent, sigCurrent, hashCurrent, tsCurrent := signAgentProof(t, newRoot, []byte("current-root-write"))
	currentRootWrite := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID: "current-root-write", ContentHash: make([]byte, 32),
			MemoryType: tx.MemoryTypeObservation, DomainTag: "root-home",
			Classification: 0, ConfidenceScore: 0.9,
		},
		AgentPubKey: pubCurrent, AgentSig: sigCurrent,
		AgentBodyHash: hashCurrent, AgentTimestamp: tsCurrent,
	}
	signAppV23Outer(t, currentRootWrite, newRoot, 1)
	currentRootResult := app.processTx(currentRootWrite, 12, appV23BlockTime())
	require.Zero(t, currentRootResult.Code, currentRootResult.Log)
	require.Equal(t, newRoot.id, func() string {
		author, authorErr := app.badgerStore.GetMemoryAuthor("current-root-write")
		require.NoError(t, authorErr)
		return author
	}(), "new memories must preserve the exact current Root credential as author provenance")
	authorPrincipal, err := app.badgerStore.GetMemoryAuthorPrincipal("current-root-write")
	require.NoError(t, err)
	require.Equal(t, oldRoot.id, authorPrincipal,
		"Root authority continuity remains bound to the immutable principal")
	preRotationAuthor, err := app.badgerStore.GetMemoryAuthor("pre-rotation-root-write")
	require.NoError(t, err)
	require.Equal(t, oldRoot.id, preRotationAuthor,
		"Root handover must not rewrite the seller's historical exact authorship")

	_, _, _, err = app.appV23Actor(admin.id)
	require.True(t, errors.Is(err, store.ErrAppV23NeedsApproval), "rotation suspends stale-generation delegated Admin")
	staleAdminTx := makeAgentRegisterTx(t, admin, "stale-admin", "admin", "", "", "")
	require.Error(t, app.enforceAppV23ControlElevation(staleAdminTx, 12),
		"a stale-generation Admin must not fall through without elevation")
	adminEnrollment, err := app.badgerStore.GetAppV23Enrollment(admin.id)
	require.NoError(t, err)
	adminRole, err := app.badgerStore.GetAppV23Role(admin.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		newRoot.id, admin.id, store.AppV23RoleAdmin,
		adminEnrollment.Profile, adminEnrollment.Profile,
		4, store.AgentCapabilityReadAllDomains,
		adminRole.Revision, adminEnrollment.Revision, 12,
	))
	_, rebound, _, err := app.appV23Actor(admin.id)
	require.NoError(t, err)
	require.Equal(t, uint64(2), rebound.RootGeneration)
	pub2, sig2, bodyHash2, ts2 := signAgentProof(t, admin, []byte("rebound-admin-data"))
	adminTx := &tx.ParsedTx{
		Type: tx.TxTypeMemorySubmit,
		MemorySubmit: &tx.MemorySubmit{
			MemoryID: "rebound-admin-data", ContentHash: make([]byte, 32),
			MemoryType: tx.MemoryTypeObservation, DomainTag: rebound.HomeDomain,
			ConfidenceScore: 0.9,
		},
		AgentPubKey: pub2, AgentSig: sig2, AgentBodyHash: bodyHash2, AgentTimestamp: ts2,
	}
	action, err := tx.PayloadBytes(adminTx)
	require.NoError(t, err)
	proof := &tx.LocalElevationProof{
		RootGeneration: 2, ValidFromHeight: 13, ValidUntilHeight: 14,
		Nonce: "rebound_admin_nonce_01",
	}
	proof.Signature = ed25519.Sign(newRoot.priv, tx.AppV23ElevationSignBytes(
		"rotation-scope", admin.id, adminTx.Type, action, proof,
	))
	adminTx.LocalElevation = proof
	require.NoError(t, app.enforceAppV23ControlElevation(adminTx, 13))
}

// TestAppV23RootHeartbeatAdmittedByCheckTxAndRejectedAtExecution pins the
// pending-upgrade pump's intentional ABCI split. A current Root-signed
// AgentRegister must enter the mempool so an idle chain produces a block, but
// execution must still reject it so CEREBRUM Root never becomes a roster agent.
func TestAppV23RootHeartbeatAdmittedByCheckTxAndRejectedAtExecution(t *testing.T) {
	app := setupTestApp(t)
	genesisRoot := newAgentKey(t)
	currentRoot := newAgentKey(t)
	registerAppV23Agent(t, app, genesisRoot, "admin", 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("heartbeat-scope", 10))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, currentRoot.id, 11))
	app.appV23AppliedHeight = 10
	registeredBefore := app.badgerStore.IsAgentRegistered(currentRoot.id)
	require.False(t, registeredBefore,
		"rotated Root credential must begin outside the ordinary agent roster")
	rootBefore, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, currentRoot.id, rootBefore.CredentialID)
	nonceBefore, err := app.badgerStore.GetNonce(currentRoot.id)
	require.NoError(t, err)

	heartbeat := makeAgentRegisterTx(t, currentRoot, "operator-admin", "admin", "node operator key", "", "")
	signAppV23Outer(t, heartbeat, currentRoot, 1)
	raw, err := tx.EncodeTx(heartbeat)
	require.NoError(t, err)

	admission, err := app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: raw})
	require.NoError(t, err)
	require.Zero(t, admission.Code, admission.Log)

	finalized, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 12,
		Time:   appV23BlockTime(),
		Txs:    [][]byte{raw},
	})
	require.NoError(t, err)
	require.Len(t, finalized.TxResults, 1)
	require.Equal(t, appV23ControlDenied(), finalized.TxResults[0])
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, int64(12), app.state.Height)

	nonceAfter, err := app.badgerStore.GetNonce(currentRoot.id)
	require.NoError(t, err)
	require.Equal(t, nonceBefore, nonceAfter,
		"execution-denied heartbeat must not burn the Root nonce")
	rootAfter, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, rootBefore, rootAfter)
	require.Equal(t, registeredBefore, app.badgerStore.IsAgentRegistered(currentRoot.id),
		"Root heartbeat must not materialize a rotated Root credential as an ordinary agent")

	nextHeartbeat := makeAgentRegisterTx(t, currentRoot, "operator-admin", "admin", "node operator key", "", "")
	signAppV23Outer(t, nextHeartbeat, currentRoot, 2)
	nextRaw, err := tx.EncodeTx(nextHeartbeat)
	require.NoError(t, err)
	nextAdmission, err := app.CheckTx(context.Background(), &abcitypes.RequestCheckTx{Tx: nextRaw})
	require.NoError(t, err)
	require.Zero(t, nextAdmission.Code, nextAdmission.Log,
		"a freshly nonced Root heartbeat must remain admissible after denied execution")
}

func TestAppV23RootTaskProjectionNeverPersistsRootAsAssignee(t *testing.T) {
	app := setupTestApp(t)
	initialRoot := newAgentKey(t)
	currentRoot := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, initialRoot, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("task-projection-scope", 10))
	app.appV23AppliedHeight = 10
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(
		1, currentRoot.id, 11,
	))

	memberEnrollment, err := app.badgerStore.GetAppV23Enrollment(member.id)
	require.NoError(t, err)
	require.NotNil(t, memberEnrollment)
	require.NotEmpty(t, memberEnrollment.HomeDomain)
	app.SuppCache = NewSupplementaryCache()

	for i, tc := range []struct {
		name       string
		assignee   string
		want       string
		stageCache bool
	}{
		{
			name: "human CEREBRUM task stays unassigned",
		},
		{
			name:     "current Root credential stripped",
			assignee: currentRoot.id, stageCache: true,
		},
		{
			name:     "historical Root credential and immutable principal stripped",
			assignee: initialRoot.id, stageCache: true,
		},
		{
			name:     "ordinary member assignee preserved",
			assignee: member.id, want: member.id, stageCache: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			memoryID := fmt.Sprintf("root-task-%d", i)
			if tc.stageCache {
				app.SuppCache.Put(memoryID, &memory.SupplementaryData{
					Assignee: tc.assignee,
				})
			}
			parsed := makeMemorySubmitTx(
				t, currentRoot, memberEnrollment.HomeDomain, tc.name,
			)
			parsed.MemorySubmit.MemoryID = memoryID
			parsed.MemorySubmit.MemoryType = tx.MemoryTypeTask
			parsed.MemorySubmit.TaskStatus = string(memory.TaskStatusPlanned)
			before := len(app.pendingWrites)
			result := app.processMemorySubmit(parsed, 12, appV23BlockTime())
			require.Zero(t, result.Code, result.Log)

			var projected *memory.MemoryRecord
			for _, write := range app.pendingWrites[before:] {
				if write.writeType != "memory" {
					continue
				}
				record, ok := write.data.(*memory.MemoryRecord)
				if ok && record.MemoryID == memoryID {
					projected = record
					break
				}
			}
			require.NotNil(t, projected)
			require.Equal(t, currentRoot.id, projected.SubmittingAgent,
				"Root authorship retains exact credential provenance")
			require.Equal(t, tc.want, projected.Assignee)
		})
	}
}

func TestAppV23DoubleRootHandoverPreservesTitleAndExactAuthorship(t *testing.T) {
	app := setupTestApp(t)
	initialRoot := newAgentKey(t)
	secondRoot := newAgentKey(t)
	currentRoot := newAgentKey(t)
	registerAppV23Agent(t, app, initialRoot, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("double-handover-scope", 10))
	app.appV23AppliedHeight = 10

	const domain = "root-title-continuity"
	require.NoError(t, app.badgerStore.RegisterDomain(domain, initialRoot.id, "", 10))
	require.NoError(t, app.badgerStore.SetAccessGrant(
		domain, initialRoot.id, 3, 0, initialRoot.id,
	))

	submit := func(signer agentKey, memoryID string, height int64) *abcitypes.ExecTxResult {
		t.Helper()
		pub, sig, bodyHash, ts := signAgentProof(t, signer, []byte(memoryID))
		return app.processMemorySubmit(&tx.ParsedTx{
			Type: tx.TxTypeMemorySubmit,
			MemorySubmit: &tx.MemorySubmit{
				MemoryID: memoryID, ContentHash: make([]byte, 32),
				MemoryType: tx.MemoryTypeObservation, DomainTag: domain,
				Content: memoryID, Classification: tx.ClearancePublic,
				ConfidenceScore: 0.9,
			},
			AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}, height, appV23BlockTime())
	}

	require.Zero(t, submit(initialRoot, "root-generation-one-memory", 11).Code)
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, secondRoot.id, 12))
	require.Zero(t, submit(secondRoot, "root-generation-two-memory", 13).Code)
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(2, currentRoot.id, 14))
	require.Zero(t, submit(currentRoot, "root-generation-three-memory", 15).Code)

	state, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, initialRoot.id, state.PrincipalID)
	require.Equal(t, currentRoot.id, state.CredentialID)
	require.Equal(t, uint64(3), state.Generation)
	owner, err := app.badgerStore.GetDomainOwner(domain)
	require.NoError(t, err)
	require.Equal(t, initialRoot.id, owner,
		"operational handover must not rewrite stable domain ownership")
	level, _, granter, err := app.badgerStore.GetAccessGrant(domain, initialRoot.id)
	require.NoError(t, err)
	require.Equal(t, uint8(3), level)
	require.Equal(t, initialRoot.id, granter,
		"operational handover must not copy or reallocate the Root grant")

	for _, verb := range []store.AppV23DomainVerb{
		store.AppV23VerbRead, store.AppV23VerbWrite, store.AppV23VerbModify,
	} {
		decision, authErr := app.badgerStore.AuthorizeAppV23LocalDomain(
			currentRoot.id, domain, verb, false,
		)
		require.NoError(t, authErr)
		require.True(t, decision.Allowed, "newest Root must inherit verb %d immediately", verb)
	}
	for _, retired := range []agentKey{initialRoot, secondRoot} {
		decision, authErr := app.badgerStore.AuthorizeAppV23LocalDomain(
			retired.id, domain, store.AppV23VerbRead, false,
		)
		require.NoError(t, authErr)
		require.False(t, decision.Allowed)
		require.NotZero(t, submit(retired, "retired-"+retired.id[:8], 15).Code,
			"retired Root credentials must lose every data-plane action")
	}

	memories := []struct {
		id     string
		author string
	}{
		{"root-generation-one-memory", initialRoot.id},
		{"root-generation-two-memory", secondRoot.id},
		{"root-generation-three-memory", currentRoot.id},
	}
	for _, memory := range memories {
		_, _, readErr := app.badgerStore.GetMemoryHash(memory.id)
		require.NoError(t, readErr, "newest Root authority must retain recall of historical memory")
		author, authorErr := app.badgerStore.GetMemoryAuthor(memory.id)
		require.NoError(t, authorErr)
		require.Equal(t, memory.author, author,
			"handover must retain the exact credential-generation provenance")
		principal, principalErr := app.badgerStore.GetMemoryAuthorPrincipal(memory.id)
		require.NoError(t, principalErr)
		require.Equal(t, initialRoot.id, principal,
			"all generations retain one immutable Root authority identity")
	}
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23RootRotationRejectsRegisteredAgentCredentialThroughProcessTx(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	memberKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, memberKey, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("member-home", memberKey.id, "", 3))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("collision-scope", 10))
	app.appV23AppliedHeight = 10
	app.appV22AppliedHeight = 9
	app.appV17AppliedHeight = 8
	app.appV9AppliedHeight = 7

	rotation := &tx.RootCredentialRotate{
		ExpectedGeneration: 1,
		NewCredentialID:    memberKey.id,
		Scope:              "collision-scope",
	}
	rotation.NewCredentialSignature = ed25519.Sign(
		memberKey.priv, tx.RootCredentialRotationSignBytes(rootKey.id, rotation),
	)
	pub, sig, bodyHash, ts := signAgentProof(t, rootKey, []byte("root-agent-collision"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeRootCredentialRotate, RootCredentialRotate: rotation,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	signAppV23Outer(t, parsed, rootKey, 1)

	result := app.processTx(parsed, 11, appV23BlockTime())
	require.Equal(t, uint32(110), result.Code, result.Log)
	root, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, rootKey.id, root.CredentialID)
	require.Equal(t, uint64(1), root.Generation)
	_, enrollment, role, err := app.appV23Actor(memberKey.id)
	require.NoError(t, err)
	require.Equal(t, memberKey.id, enrollment.AgentID)
	require.Equal(t, store.AppV23RoleMember, role.Role)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23EveryRootGenerationIsExcludedFromGenericConsensusTargets(t *testing.T) {
	app := setupTestApp(t)
	initialRoot := newAgentKey(t)
	secondRoot := newAgentKey(t)
	currentRoot := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, initialRoot, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, member, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("generic-root-target-scope", 10))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, secondRoot.id, 11))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(2, currentRoot.id, 12))
	app.appV23AppliedHeight = 10
	activateV8(t, app, 1)

	const orgID = "generic-root-target-org"
	const deptID = "generic-root-target-dept"
	require.NoError(t, app.badgerStore.RegisterOrg(orgID, "Target Org", "", member.id, 2))
	require.NoError(t, app.badgerStore.RegisterDept(orgID, deptID, "Target Dept", "", "", 3))

	withProof := func(parsed *tx.ParsedTx, signer agentKey, label string) *tx.ParsedTx {
		t.Helper()
		pub, sig, bodyHash, ts := signAgentProof(t, signer, []byte(label))
		parsed.AgentPubKey = pub
		parsed.AgentSig = sig
		parsed.AgentBodyHash = bodyHash
		parsed.AgentTimestamp = ts
		return parsed
	}
	requireDenied := func(name string, result *abcitypes.ExecTxResult) {
		t.Helper()
		require.Equal(t, uint32(110), result.Code, "%s: %s", name, result.Log)
	}
	requireMemoryReassignRetired := func(name string, result *abcitypes.ExecTxResult) {
		t.Helper()
		require.Equal(t, uint32(66), result.Code, "%s: %s", name, result.Log)
		require.Contains(t, result.Log, "immutable")
	}

	rootGenerations := []agentKey{initialRoot, secondRoot, currentRoot}
	for _, rootIdentity := range rootGenerations {
		rootIdentity := rootIdentity
		t.Run(rootIdentity.id[:8], func(t *testing.T) {
			target := rootIdentity.id
			requireDenied("org add", app.processOrgAddMember(withProof(&tx.ParsedTx{
				Type: tx.TxTypeOrgAddMember,
				OrgAddMember: &tx.OrgAddMember{
					OrgID: orgID, AgentID: target, Clearance: tx.ClearanceInternal, Role: "member",
				},
			}, currentRoot, "org-add-"+target), 13, appV23BlockTime()))
			requireDenied("org remove", app.processOrgRemoveMember(withProof(&tx.ParsedTx{
				Type:            tx.TxTypeOrgRemoveMember,
				OrgRemoveMember: &tx.OrgRemoveMember{OrgID: orgID, AgentID: target},
			}, currentRoot, "org-remove-"+target), 13, appV23BlockTime()))
			requireDenied("org clearance", app.processOrgSetClearance(withProof(&tx.ParsedTx{
				Type: tx.TxTypeOrgSetClearance,
				OrgSetClearance: &tx.OrgSetClearance{
					OrgID: orgID, AgentID: target, Clearance: tx.ClearanceSecret,
				},
			}, currentRoot, "org-clearance-"+target), 13, appV23BlockTime()))
			requireDenied("dept add", app.processDeptAddMember(withProof(&tx.ParsedTx{
				Type: tx.TxTypeDeptAddMember,
				DeptAddMember: &tx.DeptAddMember{
					OrgID: orgID, DeptID: deptID, AgentID: target,
					Clearance: tx.ClearanceInternal, Role: "member",
				},
			}, currentRoot, "dept-add-"+target), 13, appV23BlockTime()))
			requireDenied("dept remove", app.processDeptRemoveMember(withProof(&tx.ParsedTx{
				Type: tx.TxTypeDeptRemoveMember,
				DeptRemoveMember: &tx.DeptRemoveMember{
					OrgID: orgID, DeptID: deptID, AgentID: target,
				},
			}, currentRoot, "dept-remove-"+target), 13, appV23BlockTime()))
			requireDenied("access grant", app.processAccessGrant(
				makeAccessGrantTx(t, currentRoot, target, "general", 1),
				13, appV23BlockTime(),
			))
			requireDenied("access revoke", app.processAccessRevoke(
				makeAccessRevokeTx(t, currentRoot, target, "general"),
				13, appV23BlockTime(),
			))
			requireMemoryReassignRetired("memory reassign source", app.processMemoryReassign(
				makeMemoryReassignTx(t, currentRoot, target, member.id),
				13, appV23BlockTime(),
			))
			requireMemoryReassignRetired("memory reassign target", app.processMemoryReassign(
				makeMemoryReassignTx(t, currentRoot, member.id, target),
				13, appV23BlockTime(),
			))

			domain := "generic-root-target-" + target[:12]
			require.NoError(t, app.badgerStore.RegisterDomain(domain, member.id, "", 4))
			body := tx.DomainReassign{Domain: domain, NewOwnerID: target}
			body.ProposalID = seedExecutedReassignProposal(t, app, currentRoot.id, body, 12)
			requireDenied("domain reassign target", app.processDomainReassign(
				makeDomainReassignTx(t, currentRoot, &body, 1),
				13, appV23BlockTime(),
			))
			owner, err := app.badgerStore.GetDomainOwner(domain)
			require.NoError(t, err)
			require.Equal(t, member.id, owner)
		})
	}

	for _, signer := range rootGenerations {
		registerResult := app.processAgentRegister(
			makeAgentRegisterTx(t, signer, "root-as-agent", "member", "", "", ""),
			13, appV23BlockTime(),
		)
		require.NotZero(t, registerResult.Code, "Root generation must not re-register as an agent")
		updateResult := app.processAgentUpdate(
			makeAgentUpdateTx(t, signer, signer.id, "root-as-agent", "forbidden"),
			13, appV23BlockTime(),
		)
		require.NotZero(t, updateResult.Code, "Root generation must not use generic agent update")
	}
	requireDenied("root access request", app.processAccessRequest(
		makeAccessRequestTx(t, currentRoot, "general", 1),
		13, appV23BlockTime(),
	))
	rootOrgResult := app.processOrgRegister(withProof(&tx.ParsedTx{
		Type: tx.TxTypeOrgRegister,
		OrgRegister: &tx.OrgRegister{
			OrgID: "root-authority-org", Name: "Root Authority Org",
		},
	}, currentRoot, "root-org-register"), 13, appV23BlockTime())
	require.Zero(t, rootOrgResult.Code, rootOrgResult.Log)
	memberOf, err := app.badgerStore.IsAgentInOrg(currentRoot.id, "root-authority-org")
	require.NoError(t, err)
	require.False(t, memberOf, "Root may govern an org but must never become an ordinary member")
	memberOf, err = app.badgerStore.IsAgentInOrg(initialRoot.id, "root-authority-org")
	require.NoError(t, err)
	require.False(t, memberOf, "the immutable Root principal must stay out of org membership")
}

func TestAppV23AccessGroupOverflowRejectedThroughProcessTx(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("group-limit-scope", 10))
	app.appV23AppliedHeight = 10
	app.appV22AppliedHeight = 9
	app.appV17AppliedHeight = 8
	app.appV9AppliedHeight = 7
	for i := 0; i < store.AppV23MaxGroups; i++ {
		require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
			rootKey.id, fmt.Sprintf("group-%03d", i), "", nil,
			0, false, int64(11+i),
		))
	}
	require.NoError(t, app.badgerStore.ValidateAppV23State())

	mutation := &tx.AccessGroupMutate{GroupID: "group-overflow"}
	pub, sig, bodyHash, ts := signAgentProof(t, rootKey, []byte("group-overflow"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeAccessGroupMutate, AccessGroupMutate: mutation,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	signAppV23Outer(t, parsed, rootKey, 1)
	result := app.processTx(parsed, 300, appV23BlockTime())
	require.Equal(t, uint32(110), result.Code, result.Log)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23PendingPrincipalCannotCreateAccessRequestThroughRawTx(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	pendingKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("pending-access-request-scope", 10))
	require.NoError(t, app.badgerStore.RegisterAgentWithCapabilities(
		pendingKey.id, "pending", store.AppV23RoleMember, "", "", "", 11,
		store.DefaultSelfRegisteredAgentCapabilities,
	))
	app.appV23AppliedHeight = 10
	app.appV22AppliedHeight = 9

	request := makeAccessRequestTx(t, pendingKey, "pending-target-domain", 1)
	signAppV23Outer(t, request, pendingKey, 1)
	result := app.processTx(request, 12, appV23BlockTime())
	require.Equal(t, uint32(110), result.Code, result.Log)
	require.Contains(t, result.Log, "access denied")

	requestHash := sha256.Sum256([]byte(fmt.Sprintf(
		"%s:%s:%d", pendingKey.id, request.AccessRequest.TargetDomain, 12,
	)))
	_, _, _, err := app.badgerStore.GetAccessRequest(hex.EncodeToString(requestHash[:16]))
	require.Error(t, err, "a pending principal must not leave an on-chain access-request record")
}

func TestAppV23AccessGroupRejectsRootMembershipThroughProcessTx(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	memberKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, memberKey, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("root-group-scope", 10))
	app.appV23AppliedHeight = 10
	members := []string{rootKey.id, memberKey.id}
	sort.Strings(members)
	mutation := &tx.AccessGroupMutate{
		GroupID: "root-scope", Name: "Root scope", Members: members,
	}
	pub, sig, bodyHash, ts := signAgentProof(t, rootKey, []byte("root-group-member"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeAccessGroupMutate, AccessGroupMutate: mutation,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	signAppV23Outer(t, parsed, rootKey, 1)
	result := app.processTx(parsed, 11, appV23BlockTime())
	require.Equal(t, uint32(110), result.Code, result.Log)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23LocalApprovalCannotStealActiveHomeThroughProcessTx(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	sourceKey := newAgentKey(t)
	targetKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, sourceKey, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(
		t, app, targetKey, store.AppV23RoleMember, 3,
		store.DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("home-approval-scope", 10))
	app.appV23AppliedHeight = 10
	sourceEnrollment, err := app.badgerStore.GetAppV23Enrollment(sourceKey.id)
	require.NoError(t, err)
	targetEnrollment, err := app.badgerStore.GetAppV23Enrollment(targetKey.id)
	require.NoError(t, err)
	targetRole, err := app.badgerStore.GetAppV23Role(targetKey.id)
	require.NoError(t, err)

	approval := &tx.LocalAgentApprove{
		AgentID: targetKey.id, Active: true, Role: store.AppV23RoleMember,
		Profile: store.AppV23ProfileCompanion, HomeDomain: sourceEnrollment.HomeDomain,
		ExpectedHomeDomainOwner: sourceKey.id, TransferHomeDomain: true,
		Clearance: 0, Capabilities: 15,
		ExpectedRevision: targetEnrollment.Revision, ExpectedRoleRevision: targetRole.Revision,
		Scope: "home-approval-scope",
	}
	approval.TargetSignature = ed25519.Sign(
		targetKey.priv, tx.LocalAgentApprovalSignBytes(rootKey.id, approval),
	)
	pub, sig, bodyHash, ts := signAgentProof(t, rootKey, []byte("steal-active-home"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeLocalAgentApprove, LocalAgentApprove: approval,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	signAppV23Outer(t, parsed, rootKey, 1)
	result := app.processTx(parsed, 11, appV23BlockTime())
	require.Equal(t, uint32(110), result.Code, result.Log)
	owner, err := app.badgerStore.GetDomainOwner(sourceEnrollment.HomeDomain)
	require.NoError(t, err)
	require.Equal(t, sourceKey.id, owner)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23LocalApprovalAcceptsCompanionWithFederatedMessagingDisabled(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	companionKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(
		t, app, companionKey, store.AppV23RoleMember, 2,
		store.DefaultSelfRegisteredAgentCapabilities,
	)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("companion-approval-scope", 10))
	app.appV23AppliedHeight = 10

	enrollment, err := app.badgerStore.GetAppV23Enrollment(companionKey.id)
	require.NoError(t, err)
	role, err := app.badgerStore.GetAppV23Role(companionKey.id)
	require.NoError(t, err)
	approval := &tx.LocalAgentApprove{
		AgentID: companionKey.id, Active: true, Role: store.AppV23RoleMember,
		Profile: store.AppV23ProfileCompanion, HomeDomain: "voice-interface",
		Clearance: 1, Capabilities: uint32(
			store.AgentCapabilities(15) | store.AgentCapabilityDenyFederatedPipe,
		),
		ExpectedRevision: enrollment.Revision, ExpectedRoleRevision: role.Revision,
		Scope: "companion-approval-scope",
	}
	approval.TargetSignature = ed25519.Sign(
		companionKey.priv, tx.LocalAgentApprovalSignBytes(rootKey.id, approval),
	)
	pub, sig, bodyHash, ts := signAgentProof(t, rootKey, []byte("approve-companion-no-pipe"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeLocalAgentApprove, LocalAgentApprove: approval,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	signAppV23Outer(t, parsed, rootKey, 1)

	result := app.processTx(parsed, 11, appV23BlockTime())
	require.Zero(t, result.Code, result.Log)
	approved, err := app.badgerStore.GetAppV23Enrollment(companionKey.id)
	require.NoError(t, err)
	require.Equal(
		t,
		store.AgentCapabilities(15)|store.AgentCapabilityDenyFederatedPipe,
		approved.Capabilities,
	)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23RoleChangeCannotExitReadOnlyWithoutHomeThroughProcessTx(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	observerKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, observerKey, "observer", 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("readonly-role-scope", 10))
	app.appV23AppliedHeight = 10
	enrollment, err := app.badgerStore.GetAppV23Enrollment(observerKey.id)
	require.NoError(t, err)
	role, err := app.badgerStore.GetAppV23Role(observerKey.id)
	require.NoError(t, err)

	change := &tx.AgentRoleChange{
		AgentID: observerKey.id, ExpectedRevision: role.Revision,
		EnrollmentRevision: enrollment.Revision,
		Role:               store.AppV23RoleMember, ExpectedProfile: store.AppV23ProfileReadOnly,
		Profile: store.AppV23ProfileStandard, Clearance: enrollment.Clearance,
	}
	pub, sig, bodyHash, ts := signAgentProof(t, rootKey, []byte("readonly-role-exit"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeAgentRoleChange, AgentRoleChange: change,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	signAppV23Outer(t, parsed, rootKey, 1)
	result := app.processTx(parsed, 11, appV23BlockTime())
	require.Equal(t, uint32(110), result.Code, result.Log)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23DomainReassignCannotOrphanOrShareActiveHomeThroughProcessTx(t *testing.T) {
	app := setupTestApp(t)
	activateV8(t, app, 1)
	rootKey := newAgentKey(t)
	sourceKey := newAgentKey(t)
	targetKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, sourceKey, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, app, targetKey, store.AppV23RoleMember, 3, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("home-reassign-scope", 10))
	app.appV23AppliedHeight = 10
	sourceEnrollment, err := app.badgerStore.GetAppV23Enrollment(sourceKey.id)
	require.NoError(t, err)

	transfer := tx.DomainReassign{
		Domain: sourceEnrollment.HomeDomain, NewOwnerID: targetKey.id,
	}
	transfer.ProposalID = seedExecutedReassignProposal(t, app, rootKey.id, transfer, 5)
	transferResult := app.processTx(
		makeDomainReassignTx(t, rootKey, &transfer, 1),
		11, appV23BlockTime(),
	)
	require.Equal(t, uint32(88), transferResult.Code, transferResult.Log)
	owner, err := app.badgerStore.GetDomainOwner(sourceEnrollment.HomeDomain)
	require.NoError(t, err)
	require.Equal(t, sourceKey.id, owner)

	promote := tx.DomainReassign{
		Domain: sourceEnrollment.HomeDomain, NewOwnerID: sourceKey.id, OpenToShared: true,
	}
	promote.ProposalID = seedExecutedReassignProposal(t, app, rootKey.id, promote, 6)
	promoteResult := app.processTx(
		makeDomainReassignTx(t, rootKey, &promote, 2),
		12, appV23BlockTime(),
	)
	require.Equal(t, uint32(88), promoteResult.Code, promoteResult.Log)
	shared, err := app.badgerStore.GetState("shared_domain:" + sourceEnrollment.HomeDomain)
	require.NoError(t, err)
	require.Empty(t, shared)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
}

func TestAppV23RotatedRootCannotCorroborateForeignCoauthoredMemory(t *testing.T) {
	app := setupTestApp(t)
	oldRoot := newAgentKey(t)
	newRoot := newAgentKey(t)
	relay := newAgentKey(t)
	registerAppV23Agent(t, app, oldRoot, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, relay, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("coauthor-rotation-scope", 10))
	app.appV15AppliedHeight = 5
	app.appV23AppliedHeight = 10

	relayEnrollment, err := app.badgerStore.GetAppV23Enrollment(relay.id)
	require.NoError(t, err)
	require.NotNil(t, relayEnrollment)
	require.NotEmpty(t, relayEnrollment.HomeDomain)

	envelope := &tx.CoCommitSubmit{
		SchemaVersion:   1,
		ContentHash:     make([]byte, 32),
		MemoryType:      tx.MemoryTypeFact,
		Domain:          relayEnrollment.HomeDomain,
		Classification:  tx.ClearancePublic,
		ConfidenceScore: 0.9,
		CreatedAtUnix:   appV23BlockTime().Unix(),
		AgreementNonce:  []byte("rotated-root-foreign-coauthor"),
		Coauthors: []tx.CoCommitCoauthor{
			{PubKey: relay.pub, ChainID: "sage-local"},
			{PubKey: oldRoot.pub, ChainID: "sage-root-peer"},
		},
	}
	core := tx.CanonicalCoreBytes(envelope)
	envelope.Coauthors[0].Sig = ed25519.Sign(relay.priv, core)
	envelope.Coauthors[1].Sig = ed25519.Sign(oldRoot.priv, core)
	envelope.SharedID = tx.ComputeSharedID(
		tx.CoreHashOf(envelope), envelope.Coauthors, envelope.AgreementNonce,
	)
	submit := app.processCoCommitSubmit(
		coCommitSubmitTx(t, relay, envelope), 11, appV23BlockTime(),
	)
	require.Zero(t, submit.Code, submit.Log)
	author, err := app.badgerStore.GetMemoryAuthor(envelope.SharedID)
	require.NoError(t, err)
	require.Equal(t, relay.id, author,
		"Root must be a foreign coauthor so the ordinary memauthor guard does not decide the test")

	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, newRoot.id, 12))
	corroborate := app.processMemoryCorroborate(
		makeMemoryCorroborateTx(t, newRoot, envelope.SharedID, "self after rotation"),
		13,
		appV23BlockTime(),
	)
	require.Equal(t, uint32(17), corroborate.Code)
	require.Contains(t, corroborate.Log, "cannot corroborate its own co-authored memory")
	hasCorroborated, err := app.badgerStore.HasCorroborated(envelope.SharedID, oldRoot.id)
	require.NoError(t, err)
	require.False(t, hasCorroborated,
		"the rotated credential must not create a corroboration under Root's immutable principal")
}

func TestAppV23RetiredRootCannotCoauthorAfterLaterHandover(t *testing.T) {
	app := setupTestApp(t)
	initialRoot := newAgentKey(t)
	secondRoot := newAgentKey(t)
	currentRoot := newAgentKey(t)
	relay := newAgentKey(t)
	registerAppV23Agent(t, app, initialRoot, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, app, relay, store.AppV23RoleMember, 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("retired-coauthor-scope", 10))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, secondRoot.id, 11))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(2, currentRoot.id, 12))
	app.appV15AppliedHeight = 5
	app.appV23AppliedHeight = 10

	relayEnrollment, err := app.badgerStore.GetAppV23Enrollment(relay.id)
	require.NoError(t, err)
	for _, retired := range []agentKey{initialRoot, secondRoot} {
		envelope := &tx.CoCommitSubmit{
			SchemaVersion:   1,
			ContentHash:     make([]byte, 32),
			MemoryType:      tx.MemoryTypeFact,
			Domain:          relayEnrollment.HomeDomain,
			Classification:  tx.ClearancePublic,
			ConfidenceScore: 0.9,
			CreatedAtUnix:   appV23BlockTime().Unix(),
			AgreementNonce:  []byte("retired-coauthor-" + retired.id[:8]),
			Coauthors: []tx.CoCommitCoauthor{
				{PubKey: relay.pub, ChainID: "sage-local"},
				{PubKey: retired.pub, ChainID: "sage-retired-root"},
			},
		}
		core := tx.CanonicalCoreBytes(envelope)
		envelope.Coauthors[0].Sig = ed25519.Sign(relay.priv, core)
		envelope.Coauthors[1].Sig = ed25519.Sign(retired.priv, core)
		envelope.SharedID = tx.ComputeSharedID(
			tx.CoreHashOf(envelope), envelope.Coauthors, envelope.AgreementNonce,
		)
		result := app.processCoCommitSubmit(
			coCommitSubmitTx(t, relay, envelope), 13, appV23BlockTime(),
		)
		require.Equal(t, uint32(95), result.Code, result.Log)
		require.Contains(t, result.Log, "retired Root credential")
		_, _, lookupErr := app.badgerStore.GetMemoryHash(envelope.SharedID)
		require.Error(t, lookupErr, "rejected retired-Root coauthor must leave no memory state")
	}
}

func TestAppV23RotatedRootCoCommitPreservesExactSignerAndImmutableAuthority(t *testing.T) {
	app := setupTestApp(t)
	oldRoot := newAgentKey(t)
	newRoot := newAgentKey(t)
	registerAppV23Agent(t, app, oldRoot, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("coauthor-provenance-scope", 10))
	app.appV15AppliedHeight = 5
	app.appV23AppliedHeight = 10
	rootEnrollment, err := app.badgerStore.GetAppV23Enrollment(oldRoot.id)
	require.NoError(t, err)
	require.NotNil(t, rootEnrollment)
	const rootDomain = "root-cocommit-provenance"
	require.NoError(t, app.badgerStore.RegisterDomain(rootDomain, oldRoot.id, "", 11))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, newRoot.id, 12))

	envelope, _ := buildCoCommitEnvelope(
		t, newRoot, rootDomain, []byte("rotated-root-exact-author"), "sage-peer",
	)
	result := app.processCoCommitSubmit(
		coCommitSubmitTx(t, newRoot, envelope), 13, appV23BlockTime(),
	)
	require.Zero(t, result.Code, result.Log)
	author, err := app.badgerStore.GetMemoryAuthor(envelope.SharedID)
	require.NoError(t, err)
	require.Equal(t, newRoot.id, author)
	authorPrincipal, err := app.badgerStore.GetMemoryAuthorPrincipal(envelope.SharedID)
	require.NoError(t, err)
	require.Equal(t, oldRoot.id, authorPrincipal)
	owner, err := app.badgerStore.GetDomainOwner(rootDomain)
	require.NoError(t, err)
	require.Equal(t, oldRoot.id, owner,
		"Root-owned domain authority remains on the immutable principal")

	var projected *memory.MemoryRecord
	for _, pending := range app.pendingWrites {
		if record, ok := pending.data.(*memory.MemoryRecord); ok &&
			record.MemoryID == envelope.SharedID {
			projected = record
			break
		}
	}
	require.NotNil(t, projected)
	require.Equal(t, newRoot.id, projected.SubmittingAgent,
		"off-chain federation/recall projection must preserve the exact signer")
}

func TestReplayAppV23ActivationCommitReopenAndCrashReplay(t *testing.T) {
	consensusPath := filepath.Join(t.TempDir(), "consensus")
	projectionPath := filepath.Join(t.TempDir(), "projection.db")

	badgerStore, err := store.NewBadgerStore(consensusPath)
	require.NoError(t, err)
	projection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	require.NoError(t, err)
	app, err := NewSageAppWithStores(badgerStore, projection, zerolog.Nop())
	require.NoError(t, err)

	rootKey := newAgentKey(t)
	memberKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 18, 0)
	registerAppV23Agent(t, app, memberKey, store.AppV23RoleMember, 19, 0)
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 0, nil)
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV22UpgradeName, 22, 17))
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.appV22AppliedHeight = 17
	app.state.Height = 29
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV23UpgradeName, TargetAppVersion: 23, ActivationHeight: 30, ProposedAt: 29,
	}))

	activation, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 30, Time: appV23BlockTime(),
	})
	require.NoError(t, err)
	require.NotNil(t, activation.ConsensusParamUpdates)
	require.NotNil(t, activation.ConsensusParamUpdates.Version)
	require.Equal(t, uint64(23), activation.ConsensusParamUpdates.Version.App)
	require.Equal(t, int64(30), app.pendingAppV20Finalize.app.appV23AppliedHeight)
	require.NoError(t, app.pendingAppV20Finalize.app.badgerStore.ValidateAppV23State())
	migratedRoot, err := app.pendingAppV20Finalize.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, rootKey.id, migratedRoot.PrincipalID)
	require.NotEmpty(t, activation.AppHash)
	activationHash := append([]byte(nil), activation.AppHash...)

	// A crash before Commit discards the speculative post-v20 transaction.
	// Replaying H must execute the activation again from the unchanged durable
	// plan and reproduce both the version bump and the exact AppHash.
	app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
	app.pendingAppV20Finalize = nil
	uncommittedRoot, err := app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Nil(t, uncommittedRoot)
	uncommittedApplied, err := app.badgerStore.GetAppliedUpgrade(appV23UpgradeName)
	require.NoError(t, err)
	require.Nil(t, uncommittedApplied)

	replay, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 30, Time: appV23BlockTime(),
	})
	require.NoError(t, err)
	require.NotNil(t, replay.ConsensusParamUpdates)
	require.NotNil(t, replay.ConsensusParamUpdates.Version)
	require.Equal(t, uint64(23), replay.ConsensusParamUpdates.Version.App)
	require.Equal(t, activationHash, replay.AppHash,
		"crash replay of the activation height must reproduce the exact AppHash")
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, int64(30), app.appV23AppliedHeight)
	require.Equal(t, uint64(23), app.currentAppVersion())
	require.Equal(t, activationHash, app.state.AppHash)
	require.NoError(t, app.badgerStore.ValidateAppV23State())

	require.NoError(t, app.Close())
	reopenedBadger, err := store.NewBadgerStore(consensusPath)
	require.NoError(t, err)
	reopenedProjection, err := store.NewSQLiteStore(context.Background(), projectionPath)
	require.NoError(t, err)
	reopened, err := NewSageAppWithStores(reopenedBadger, reopenedProjection, zerolog.Nop())
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.Close() })

	require.Equal(t, int64(30), reopened.state.Height)
	require.Equal(t, activationHash, reopened.state.AppHash)
	require.Equal(t, int64(30), reopened.appV23AppliedHeight)
	require.Equal(t, uint64(23), reopened.currentAppVersion())
	require.True(t, reopened.IsAppV23ActiveForNextTx())
	require.NoError(t, reopened.badgerStore.ValidateAppV23State())
	reopenedRoot, err := reopened.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.Equal(t, migratedRoot, reopenedRoot)

	next, err := reopened.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 31, Time: appV23BlockTime().Add(time.Second),
	})
	require.NoError(t, err)
	require.Nil(t, next.ConsensusParamUpdates,
		"the version bump belongs only to the activation height")
	require.Equal(t, activationHash, next.AppHash,
		"an empty H+1 block must preserve the migrated consensus AppHash")
	_, err = reopened.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.Equal(t, int64(31), reopened.state.Height)
	require.True(t, reopened.postAppV23Rules(31))
	require.NoError(t, reopened.badgerStore.ValidateAppV23State())
}

func TestReplayAppV23LargeRosterActivationBarrierAndStage(t *testing.T) {
	app := setupTestApp(t)
	rootKey := newAgentKey(t)
	registerAppV23Agent(t, app, rootKey, store.AppV23RoleAdmin, 1, 0)
	for i := 0; i < 512; i++ {
		registerAppV23Agent(
			t, app, newAgentKey(t), store.AppV23RoleMember, int64(i+2), 0,
		)
	}
	seedAppV22PredecessorLadder(t, app.badgerStore, 1, 0, nil)
	require.NoError(t, app.badgerStore.MarkUpgradeApplied(appV22UpgradeName, 22, 17))
	seedTestGovernanceDelegationDomain(t, app.badgerStore)
	app.appV20AppliedHeight = 15
	app.appV21AppliedHeight = 16
	app.appV22AppliedHeight = 17
	app.state.Height = 29
	require.NoError(t, app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
		Name: appV23UpgradeName, TargetAppVersion: 23, ActivationHeight: 30, ProposedAt: 29,
	}))

	lateKey := newAgentKey(t)
	lateTx, err := tx.EncodeTx(makeAgentRegisterTx(
		t, lateKey, "must-wait-for-h-plus-one", store.AppV23RoleMember, "", "", "",
	))
	require.NoError(t, err)
	request := &abcitypes.RequestFinalizeBlock{
		Height: 30, Time: appV23BlockTime(), Txs: [][]byte{lateTx},
	}
	first, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, first.TxResults, 1)
	require.Equal(t, uint32(96), first.TxResults[0].Code)
	require.Contains(t, first.TxResults[0].Log, "activation barrier")
	require.False(t, app.pendingAppV20Finalize.app.badgerStore.IsAgentRegistered(lateKey.id))
	migration, err := app.pendingAppV20Finalize.app.badgerStore.GetAppV23MigrationState()
	require.NoError(t, err)
	require.Positive(t, migration.StageCount)
	require.NoError(t, app.pendingAppV20Finalize.app.badgerStore.ValidateAppV23State())
	firstHash := append([]byte(nil), first.AppHash...)

	app.pendingAppV20Finalize.store.DiscardConsensusTransaction()
	app.pendingAppV20Finalize = nil
	replay, err := app.FinalizeBlock(context.Background(), request)
	require.NoError(t, err)
	require.Len(t, replay.TxResults, 1)
	require.Equal(t, uint32(96), replay.TxResults[0].Code)
	require.Equal(t, firstHash, replay.AppHash)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.ValidateAppV23State())
	require.False(t, app.badgerStore.IsAgentRegistered(lateKey.id))
}

func TestAppV23DelegatedAdminElevationSignatureAndReplay(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	admin := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, admin, "admin", 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("elevation-scope", 10))
	promoteAppV23TestAdmin(t, app, root, admin, 10)
	app.appV23AppliedHeight = 10

	mutation := &tx.AccessGroupMutate{GroupID: "operators", Name: "Operators"}
	proof := &tx.LocalElevationProof{
		RootGeneration: 1, ValidFromHeight: 11, ValidUntilHeight: 12,
		Nonce: "elevation_nonce_0001",
	}
	proof.Signature = ed25519.Sign(root.priv, tx.AppV23ElevationSignBytes(
		"elevation-scope", admin.id, tx.TxTypeAccessGroupMutate,
		tx.AccessGroupMutateActionBytes(mutation), proof,
	))
	pub, sig, bodyHash, ts := signAgentProof(t, admin, []byte("mutate-group"))
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeAccessGroupMutate, AccessGroupMutate: mutation,
		LocalElevation: proof,
		AgentPubKey:    pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
	}
	result := app.processAccessGroupMutateV23(parsed, 11, appV23BlockTime())
	require.Zero(t, result.Code, result.Log)
	replay := app.processAccessGroupMutateV23(parsed, 11, appV23BlockTime())
	require.NotZero(t, replay.Code)
	require.Equal(t, "access denied", replay.Log)

	mutation2 := &tx.AccessGroupMutate{GroupID: "operators-2", Name: "Operators 2"}
	parsed.AccessGroupMutate = mutation2
	invalid := app.processAccessGroupMutateV23(parsed, 11, appV23BlockTime())
	require.NotZero(t, invalid.Code, "the countersignature must bind the exact action")
}

func TestAppV23AgentRegisterAlwaysCreatesRestrictedPendingMember(t *testing.T) {
	for _, requestedRole := range []string{"observer", "manager", "admin"} {
		t.Run(requestedRole, func(t *testing.T) {
			app := setupTestApp(t)
			root := newAgentKey(t)
			registerAppV23Agent(t, app, root, "admin", 1, 0)
			require.NoError(t, app.badgerStore.EnsureAppV23Root("registration-scope", 10))
			app.appV23AppliedHeight = 10
			agent := newAgentKey(t)
			parsed := makeAgentRegisterTx(t, agent, "fresh", requestedRole, "", "", "")
			require.NoError(t, app.enforceAppV23ControlElevation(parsed, 11),
				"only a previously unknown key may reach first self-registration")
			result := app.processAgentRegister(parsed, 11, appV23BlockTime())
			require.Zero(t, result.Code, result.Log)
			require.NoError(t, app.enforceAppV23ControlElevation(parsed, 12),
				"a pending ordinary principal may repeat only its idempotent self-registration")
			record, err := app.badgerStore.GetRegisteredAgent(agent.id)
			require.NoError(t, err)
			require.Equal(t, store.AppV23RoleMember, record.Role)
			require.Equal(t, store.DefaultSelfRegisteredAgentCapabilities, record.Capabilities)
			enrollment, err := app.badgerStore.GetAppV23Enrollment(agent.id)
			require.NoError(t, err)
			require.Nil(t, enrollment)

			pub, sig, bodyHash, ts := signAgentProof(t, agent, []byte("pending-write"))
			pendingWrite := &tx.ParsedTx{
				Type: tx.TxTypeMemorySubmit,
				MemorySubmit: &tx.MemorySubmit{
					MemoryID: "pending-write", ContentHash: make([]byte, 32),
					MemoryType: tx.MemoryTypeObservation, DomainTag: "pending-home",
					ConfidenceScore: 0.9,
				},
				AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
			}
			require.NoError(t, app.enforceAppV23ControlElevation(pendingWrite, 12),
				"data-plane policy must produce the stable pending-review denial")
			denied := app.processMemorySubmit(pendingWrite, 12, appV23BlockTime())
			require.Equal(t, appV23Denial(authzdenial.CodePrincipalPendingReview), denied)
		})
	}
}

func TestAppV23PendingAgentReregisterPassesConsensusTransactionGate(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	pending := newAgentKey(t)
	registerAppV23Agent(t, app, root, store.AppV23RoleAdmin, 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("pending-reregister-scope", 10))
	app.appV23AppliedHeight = 10
	app.state.Height = 10

	first := makeAgentRegisterTx(t, pending, "pending", store.AppV23RoleMember, "", "", "")
	signAppV23Outer(t, first, pending, 1)
	firstRaw, err := tx.EncodeTx(first)
	require.NoError(t, err)
	firstBlock, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 11, Time: appV23BlockTime(), Txs: [][]byte{firstRaw},
	})
	require.NoError(t, err)
	require.Len(t, firstBlock.TxResults, 1)
	require.Zero(t, firstBlock.TxResults[0].Code, firstBlock.TxResults[0].Log)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)

	ctx := context.Background()
	registeredProjection, err := app.offchainStore.GetAgent(ctx, pending.id)
	require.NoError(t, err)
	require.Equal(t, "active", registeredProjection.Status)
	require.NoError(t, app.offchainStore.RemoveAgent(ctx, pending.id),
		"operator rejection removes only the local pending projection")

	// Exercise FinalizeBlock again, not just processAgentRegister. The latter
	// misses enforceAppV23ControlElevation and allowed the live reject/re-request
	// regression to escape the earlier unit test.
	retry := makeAgentRegisterTx(t, pending, "ignored replacement", store.AppV23RoleAdmin, "", "", "")
	signAppV23Outer(t, retry, pending, 2)
	retryRaw, err := tx.EncodeTx(retry)
	require.NoError(t, err)
	retryBlock, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 12, Time: appV23BlockTime().Add(time.Second), Txs: [][]byte{retryRaw},
	})
	require.NoError(t, err)
	require.Len(t, retryBlock.TxResults, 1)
	require.Zero(t, retryBlock.TxResults[0].Code, retryBlock.TxResults[0].Log)
	_, err = app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)

	record, err := app.badgerStore.GetRegisteredAgent(pending.id)
	require.NoError(t, err)
	require.Equal(t, store.AppV23RoleMember, record.Role)
	require.Equal(t, store.DefaultSelfRegisteredAgentCapabilities, record.Capabilities)
	require.Equal(t, int64(11), record.RegisteredAt, "idempotent retry must preserve canonical registration history")
	enrollment, err := app.badgerStore.GetAppV23Enrollment(pending.id)
	require.NoError(t, err)
	require.Nil(t, enrollment, "re-request must not approve or activate the pending identity")
	restoredProjection, err := app.offchainStore.GetAgent(ctx, pending.id)
	require.NoError(t, err)
	require.Equal(t, "active", restoredProjection.Status)
	require.Nil(t, restoredProjection.RemovedAt)

	// The exception is registration-only. A pending principal remains unable to
	// mutate even its own canonical metadata through the control plane.
	update := makeAgentUpdateTx(t, pending, pending.id, "not allowed", "")
	signAppV23Outer(t, update, pending, 3)
	updateRaw, err := tx.EncodeTx(update)
	require.NoError(t, err)
	deniedBlock, err := app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{
		Height: 13, Time: appV23BlockTime().Add(2 * time.Second), Txs: [][]byte{updateRaw},
	})
	require.NoError(t, err)
	require.Len(t, deniedBlock.TxResults, 1)
	require.Equal(t, uint32(110), deniedBlock.TxResults[0].Code)
}

func TestAppV23ReadOnlyWriteDenialNeverPrescribesGrant(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	reader := newAgentKey(t)
	foreign := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("readonly-scope", 10))
	registerAppV23Agent(t, app, reader, "member", 11, store.DefaultSelfRegisteredAgentCapabilities)
	require.NoError(t, app.badgerStore.ApproveAppV23LocalAgent(
		store.AppV23LocalEnrollment{
			AgentID: reader.id, ApprovedBy: root.id, RootGeneration: 1,
			Profile: store.AppV23ProfileReadOnly, Clearance: 0,
			Capabilities: store.AgentCapabilityReadAllDomains,
			Active:       true, UpdatedHeight: 12,
		},
		store.AppV23RoleMember, 0, 0,
	))
	require.NoError(t, app.badgerStore.RegisterDomain("reader-owned", reader.id, "", 12))
	require.NoError(t, app.badgerStore.RegisterDomain("foreign-owned", foreign.id, "", 12))
	app.appV23AppliedHeight = 10

	for _, domain := range []string{"reader-owned", "foreign-owned", "general"} {
		allowed, code, err := app.appV23DomainDecision(
			&tx.ParsedTx{}, reader.id, domain, store.AppV23VerbWrite, 13, appV23BlockTime(),
		)
		require.NoError(t, err)
		require.False(t, allowed)
		require.Equal(t, authzdenial.CodeForeignWriteRestricted, code)
		require.NotEqual(t, authzdenial.CodeMissingWriteGrant, code)
	}
}

func TestAppV23LifecycleMutationsEnforceMemoryClassification(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	manager := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, owner, "member", 2, 0)
	registerAppV23Agent(t, app, manager, "member", 3, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("classified-home", owner.id, "", 4))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("classification-scope", 10))
	managerEnrollment, err := app.badgerStore.GetAppV23Enrollment(manager.id)
	require.NoError(t, err)
	managerRole, err := app.badgerStore.GetAppV23Role(manager.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		root.id, manager.id, store.AppV23RoleManager,
		managerEnrollment.Profile, store.AppV23ProfileStandard,
		1, 0, managerRole.Revision, managerEnrollment.Revision, 11,
	))
	members := []string{manager.id, owner.id}
	sort.Strings(members)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		root.id, "classified-team", "Classified Team",
		members, 0, false, 12,
	))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		"classified-memory", make([]byte, 32), string(memory.StatusCommitted),
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain("classified-memory", "classified-home"))
	require.NoError(t, app.badgerStore.SetMemoryClassification("classified-memory", 4))
	app.appV23AppliedHeight = 10

	makeProof := func(label string) (pub, sig, bodyHash []byte, timestamp int64) {
		return signAgentProof(t, manager, []byte(label))
	}
	pub, sig, bodyHash, timestamp := makeProof("classified-challenge")
	challenge := &tx.ParsedTx{
		Type: tx.TxTypeMemoryChallenge,
		MemoryChallenge: &tx.MemoryChallenge{
			MemoryID: "classified-memory", Reason: "must not disclose", Evidence: "classified",
		},
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: timestamp,
	}
	require.Equal(t, appV23ControlDenied(), app.processMemoryChallenge(challenge, 13, appV23BlockTime()))

	pub, sig, bodyHash, timestamp = makeProof("classified-reinstate")
	reinstate := &tx.ParsedTx{
		Type:            tx.TxTypeMemoryReinstate,
		MemoryReinstate: &tx.MemoryReinstate{MemoryID: "classified-memory", Reason: "classified"},
		AgentPubKey:     pub,
		AgentSig:        sig,
		AgentBodyHash:   bodyHash,
		AgentTimestamp:  timestamp,
	}
	require.Equal(t, appV23ControlDenied(), app.processMemoryReinstate(reinstate, 13, appV23BlockTime()))

	pub, sig, bodyHash, timestamp = makeProof("classified-corroborate")
	corroborate := &tx.ParsedTx{
		Type: tx.TxTypeMemoryCorroborate,
		MemoryCorroborate: &tx.MemoryCorroborate{
			MemoryID: "classified-memory", Evidence: "classified",
		},
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: timestamp,
	}
	require.Equal(t, appV23ControlDenied(), app.processMemoryCorroborate(corroborate, 13, appV23BlockTime()))
	_, status, err := app.badgerStore.GetMemoryHash("classified-memory")
	require.NoError(t, err)
	require.Equal(t, string(memory.StatusCommitted), status)
}

func appV23RotatedRootChallengeFixture(
	t *testing.T,
	memoryID string,
) (*SageApp, agentKey, agentKey, agentKey, agentKey, string) {
	t.Helper()
	app := setupTestApp(t)
	oldRoot := newAgentKey(t)
	newRoot := newAgentKey(t)
	owner := newAgentKey(t)
	manager := newAgentKey(t)
	registerAppV23Agent(t, app, oldRoot, "admin", 1, 0)
	registerAppV23Agent(t, app, owner, "member", 2, 0)
	registerAppV23Agent(t, app, manager, "member", 3, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("rotated-root-challenge", owner.id, "", 4))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("rotated-root-challenge-scope", 10))

	managerEnrollment, err := app.badgerStore.GetAppV23Enrollment(manager.id)
	require.NoError(t, err)
	managerRole, err := app.badgerStore.GetAppV23Role(manager.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		oldRoot.id, manager.id, store.AppV23RoleManager,
		managerEnrollment.Profile, store.AppV23ProfileStandard,
		managerEnrollment.Clearance, 0,
		managerRole.Revision, managerEnrollment.Revision, 11,
	))
	members := []string{owner.id, manager.id}
	sort.Strings(members)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		oldRoot.id, "rotated-root-challenge-team", "Rotated Root Challenge Team",
		members, 0, false, 11,
	))
	require.NoError(t, app.badgerStore.RotateAppV23RootCredential(1, newRoot.id, 12))
	require.NoError(t, app.badgerStore.SetMemoryHash(
		memoryID, []byte("rotated-root-challenge-hash"), string(memory.StatusCommitted),
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain(memoryID, "rotated-root-challenge"))
	require.NoError(t, app.badgerStore.SetMemoryClassification(memoryID, 1))
	require.NoError(t, app.badgerStore.SetCorroborated(memoryID, manager.id))

	app.appV15AppliedHeight = 1
	app.appV17AppliedHeight = 2
	app.appV21AppliedHeight = 3
	app.appV23AppliedHeight = 10
	return app, oldRoot, newRoot, owner, manager, memoryID
}

func TestAppV23RotatedRootRemainsInFreshChallengeLifecycle(t *testing.T) {
	t.Run("opens as immutable root principal", func(t *testing.T) {
		app, oldRoot, newRoot, _, _, memoryID := appV23RotatedRootChallengeFixture(
			t, "rotated-root-opens",
		)
		result := app.processMemoryChallenge(
			makeMemoryChallengeTx(t, newRoot, memoryID, "root opens"),
			13, appV23BlockTime(),
		)
		require.Zero(t, result.Code, result.Log)
		record, err := app.badgerStore.GetChallengeRecordV21(memoryID)
		require.NoError(t, err)
		require.NotNil(t, record, "one corroborator must keep the round open")
		require.Contains(t, record.Electorate, oldRoot.id,
			"the current Root credential must enter the electorate as its immutable principal")
		openerVote, err := app.badgerStore.GetChallengeVoteV21(memoryID, oldRoot.id)
		require.NoError(t, err)
		require.NotNil(t, openerVote)
		require.Equal(t, record.Round, openerVote.Round)
		require.Equal(t, uint32(1), record.EligibleCorroborators)
		require.Equal(t, uint32(2), record.RequiredChallengers)
		var challengeAudit *store.ChallengeEntry
		for _, pending := range app.pendingWrites {
			if entry, ok := pending.data.(*store.ChallengeEntry); ok &&
				entry.MemoryID == memoryID {
				challengeAudit = entry
				break
			}
		}
		require.NotNil(t, challengeAudit)
		require.Equal(t, newRoot.id, challengeAudit.ChallengerID,
			"challenge audit provenance must keep the exact current Root credential")
	})

	t.Run("endorses an owner-opened round", func(t *testing.T) {
		app, oldRoot, newRoot, owner, _, memoryID := appV23RotatedRootChallengeFixture(
			t, "rotated-root-endorses",
		)
		opened := app.processMemoryChallenge(
			makeMemoryChallengeTx(t, owner, memoryID, "owner opens"),
			13, appV23BlockTime(),
		)
		require.Zero(t, opened.Code, opened.Log)
		record, err := app.badgerStore.GetChallengeRecordV21(memoryID)
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Contains(t, record.Electorate, oldRoot.id)

		endorsed := app.processMemoryChallenge(
			makeMemoryChallengeTx(t, newRoot, memoryID, "root endorses"),
			14, appV23BlockTime().Add(time.Second),
		)
		require.Zero(t, endorsed.Code, endorsed.Log)
		_, status, err := app.badgerStore.GetMemoryHash(memoryID)
		require.NoError(t, err)
		require.Equal(t, string(memory.StatusDeprecated), status)
	})

	t.Run("reinstates an owner-opened round", func(t *testing.T) {
		app, oldRoot, newRoot, owner, _, memoryID := appV23RotatedRootChallengeFixture(
			t, "rotated-root-reinstates",
		)
		opened := app.processMemoryChallenge(
			makeMemoryChallengeTx(t, owner, memoryID, "owner opens"),
			13, appV23BlockTime(),
		)
		require.Zero(t, opened.Code, opened.Log)
		record, err := app.badgerStore.GetChallengeRecordV21(memoryID)
		require.NoError(t, err)
		require.NotNil(t, record)
		require.Contains(t, record.Electorate, oldRoot.id)

		reinstated := app.processMemoryReinstate(
			makeMemoryReinstateTx(t, newRoot, memoryID, "root reinstates"),
			14, appV23BlockTime().Add(time.Second),
		)
		require.Zero(t, reinstated.Code, reinstated.Log)
		hash, status, err := app.badgerStore.GetMemoryHash(memoryID)
		require.NoError(t, err)
		require.Equal(t, string(memory.StatusCommitted), status)
		require.Equal(t, []byte("rotated-root-challenge-hash"), hash)
		var reinstateAudit *store.AccessLogEntry
		for _, pending := range app.pendingWrites {
			if entry, ok := pending.data.(*store.AccessLogEntry); ok &&
				entry.Action == "memory_reinstate" && len(entry.MemoryIDs) == 1 &&
				entry.MemoryIDs[0] == memoryID {
				reinstateAudit = entry
				break
			}
		}
		require.NotNil(t, reinstateAudit)
		require.Equal(t, newRoot.id, reinstateAudit.AgentID,
			"reinstate audit provenance must keep the exact current Root credential")
		require.Equal(t, "rotated-root-challenge", reinstateAudit.Domain)
	})

	t.Run("corroborates with exact credential provenance", func(t *testing.T) {
		app, _, newRoot, _, _, memoryID := appV23RotatedRootChallengeFixture(
			t, "rotated-root-corroborates",
		)
		result := app.processMemoryCorroborate(
			makeMemoryCorroborateTx(t, newRoot, memoryID, "root independently verified"),
			13, appV23BlockTime(),
		)
		require.Zero(t, result.Code, result.Log)
		var corroboration *store.Corroboration
		for _, pending := range app.pendingWrites {
			if entry, ok := pending.data.(*store.Corroboration); ok &&
				entry.MemoryID == memoryID {
				corroboration = entry
				break
			}
		}
		require.NotNil(t, corroboration)
		require.Equal(t, newRoot.id, corroboration.AgentID,
			"corroboration audit provenance must keep the exact current Root credential")
	})
}

func TestAppV23FreshChallengeElectorateUsesGroupReadAndClassification(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	highManager := newAgentKey(t)
	lowManager := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, owner, "member", 2, 0)
	registerAppV23Agent(t, app, highManager, "member", 3, 0)
	registerAppV23Agent(t, app, lowManager, "member", 4, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("classified-group-domain", owner.id, "", 5))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("classified-group-scope", 10))

	setPolicy := func(agentID, role string, clearance uint8) {
		t.Helper()
		enrollment, err := app.badgerStore.GetAppV23Enrollment(agentID)
		require.NoError(t, err)
		roleState, err := app.badgerStore.GetAppV23Role(agentID)
		require.NoError(t, err)
		require.NoError(t, app.badgerStore.SetAppV23Policy(
			root.id, agentID, role,
			enrollment.Profile, store.AppV23ProfileStandard,
			clearance, 0,
			roleState.Revision, enrollment.Revision, 11,
		))
	}
	setPolicy(owner.id, store.AppV23RoleMember, 4)
	setPolicy(highManager.id, store.AppV23RoleManager, 4)
	setPolicy(lowManager.id, store.AppV23RoleManager, 1)
	members := []string{owner.id, highManager.id, lowManager.id}
	sort.Strings(members)
	require.NoError(t, app.badgerStore.MutateAppV23AccessGroup(
		root.id, "classified-group", "Classified Group", members, 0, false, 12,
	))

	const memoryID = "classified-group-memory"
	require.NoError(t, app.badgerStore.SetMemoryHash(
		memoryID, []byte("classified-group-hash"), string(memory.StatusCommitted),
	))
	require.NoError(t, app.badgerStore.SetMemoryDomain(memoryID, "classified-group-domain"))
	require.NoError(t, app.badgerStore.SetMemoryClassification(memoryID, 4))
	require.NoError(t, app.badgerStore.SetCorroborated(memoryID, highManager.id))
	require.NoError(t, app.badgerStore.SetCorroborated(memoryID, lowManager.id))
	hasLegacyRead, err := app.badgerStore.HasAccessOrAncestor(
		"classified-group-domain", highManager.id, 1, appV23BlockTime(),
	)
	require.NoError(t, err)
	require.False(t, hasLegacyRead,
		"the high-clearance corroborator must be eligible through its group, not a legacy grant")

	app.appV15AppliedHeight = 1
	app.appV17AppliedHeight = 2
	app.appV21AppliedHeight = 3
	app.appV23AppliedHeight = 10
	opened := app.processMemoryChallenge(
		makeMemoryChallengeTx(t, root, memoryID, "challenge classified group memory"),
		13, appV23BlockTime(),
	)
	require.Zero(t, opened.Code, opened.Log)
	record, err := app.badgerStore.GetChallengeRecordV21(memoryID)
	require.NoError(t, err)
	require.NotNil(t, record)
	require.Contains(t, record.Electorate, highManager.id,
		"a group-authorized high-clearance corroborator must protect the memory")
	require.NotContains(t, record.Electorate, lowManager.id,
		"a low-clearance manager cannot be frozen into a high-classification electorate")
	require.Equal(t, uint32(1), record.EligibleCorroborators,
		"only the group-authorized corroborator within clearance may affect the threshold")
	require.Equal(t, uint32(2), record.RequiredChallengers)

	endorsed := app.processMemoryChallenge(
		makeMemoryChallengeTx(t, highManager, memoryID, "withdraw high-clearance support"),
		14, appV23BlockTime().Add(time.Second),
	)
	require.Zero(t, endorsed.Code, endorsed.Log)
	_, status, err := app.badgerStore.GetMemoryHash(memoryID)
	require.NoError(t, err)
	require.Equal(t, string(memory.StatusDeprecated), status)
}

func TestAppV23PrivilegedControlInventory(t *testing.T) {
	for _, txType := range []tx.TxType{
		tx.TxTypeAccessGrant, tx.TxTypeAccessRevoke, tx.TxTypeDomainRegister,
		tx.TxTypeOrgRegister, tx.TxTypeOrgAddMember, tx.TxTypeOrgRemoveMember, tx.TxTypeOrgSetClearance,
		tx.TxTypeFederationPropose, tx.TxTypeFederationApprove, tx.TxTypeFederationRevoke,
		tx.TxTypeDeptRegister, tx.TxTypeDeptAddMember, tx.TxTypeDeptRemoveMember,
		tx.TxTypeAgentRegister, tx.TxTypeAgentUpdate, tx.TxTypeAgentSetPermission,
		tx.TxTypeMemoryReassign, tx.TxTypeGovPropose, tx.TxTypeGovVote, tx.TxTypeGovCancel,
		tx.TxTypeUpgradePropose, tx.TxTypeUpgradeCancel, tx.TxTypeUpgradeRevert,
		tx.TxTypeDomainReassign, tx.TxTypeCrossFedSet, tx.TxTypeCrossFedRevoke,
		tx.TxTypeLocalAgentApprove, tx.TxTypeAgentRoleChange, tx.TxTypeAccessGroupMutate,
	} {
		require.True(t, appV23ControlPlaneType(txType), "type %d escaped delegated-Admin elevation", txType)
	}
	require.False(t, appV23ControlPlaneType(tx.TxTypeMemorySubmit))
	require.False(t, appV23ControlPlaneType(tx.TxTypeMemoryChallenge))
	require.False(t, appV23ControlPlaneType(tx.TxTypeMemoryCorroborate))
}

func TestAppV23AccessGrantRevokeRequiresDelegatedAdminElevation(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	admin := newAgentKey(t)
	owner := newAgentKey(t)
	grantee := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, admin, "admin", 2, 0)
	registerAppV23Agent(t, app, owner, "member", 3, 0)
	registerAppV23Agent(t, app, grantee, "member", 4, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("owner-domain", owner.id, "", 5))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("grant-scope", 10))
	promoteAppV23TestAdmin(t, app, root, admin, 10)
	app.appV23AppliedHeight = 10
	app.appV22AppliedHeight = 9
	app.appV18AppliedHeight = 8

	grant := makeAccessGrantTx(t, admin, grantee.id, "owner-domain", 2)
	grant.AccessGrant.ExpectedOwnerID = owner.id
	grant.AccessGrant.ExpectedOwnedDomain = "owner-domain"
	signAppV23Outer(t, grant, admin, 1)
	denied := app.processTx(grant, 11, time.Now())
	require.NotZero(t, denied.Code)
	require.Equal(t, "access denied", denied.Log)

	grant = makeAccessGrantTx(t, admin, grantee.id, "owner-domain", 2)
	grant.AccessGrant.ExpectedOwnerID = owner.id
	grant.AccessGrant.ExpectedOwnedDomain = "owner-domain"
	attachAppV23Elevation(t, grant, root, admin, "grant-scope", "grant_elevation_0001", 11)
	signAppV23Outer(t, grant, admin, 2)
	granted := app.processTx(grant, 11, time.Now())
	require.Zero(t, granted.Code, granted.Log)
	level, _, granter, err := app.badgerStore.GetAccessGrant("owner-domain", grantee.id)
	require.NoError(t, err)
	require.Equal(t, uint8(2), level)
	require.Equal(t, admin.id, granter)

	revoke := makeAccessRevokeTx(t, admin, grantee.id, "owner-domain")
	revoke.AccessRevoke.ExpectedOwnerID = owner.id
	revoke.AccessRevoke.ExpectedOwnedDomain = "owner-domain"
	attachAppV23Elevation(t, revoke, root, admin, "grant-scope", "revoke_elevation_001", 12)
	signAppV23Outer(t, revoke, admin, 3)
	revoked := app.processTx(revoke, 12, time.Now())
	require.Zero(t, revoked.Code, revoked.Log)
	_, _, _, err = app.badgerStore.GetAccessGrant("owner-domain", grantee.id)
	require.ErrorIs(t, err, store.ErrAccessGrantNotFound)
}

func TestAppV23EveryDelegatedAdminTxRequiresAndConsumesElevation(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	admin := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, admin, "admin", 2, 0)
	registerAppV23Agent(t, app, member, "member", 3, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("all-admin-tx-scope", 10))
	promoteAppV23TestAdmin(t, app, root, admin, 10)
	app.appV23AppliedHeight = 10

	makeSubmit := func(signer agentKey, memoryID string) *tx.ParsedTx {
		pub, sig, bodyHash, ts := signAgentProof(t, signer, []byte(memoryID))
		return &tx.ParsedTx{
			Type: tx.TxTypeMemorySubmit,
			MemorySubmit: &tx.MemorySubmit{
				MemoryID: memoryID, ContentHash: make([]byte, 32),
				MemoryType: tx.MemoryTypeObservation, DomainTag: "local-" + signer.id,
				ConfidenceScore: 0.9,
			},
			AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: ts,
		}
	}

	adminTx := makeSubmit(admin, "admin-own-domain")
	require.Error(t, app.enforceAppV23ControlElevation(adminTx, 11))
	attachAppV23Elevation(t, adminTx, root, admin, "all-admin-tx-scope", "admin_data_nonce_001", 11)
	require.NoError(t, app.enforceAppV23ControlElevation(adminTx, 11))
	require.Error(t, app.enforceAppV23ControlElevation(adminTx, 11), "the centrally consumed proof must reject replay")

	rootTx := makeSubmit(root, "root-data")
	require.NoError(t, app.enforceAppV23ControlElevation(rootTx, 11), "current root acts without sudo trailer")

	memberTx := makeSubmit(member, "member-data")
	attachAppV23Elevation(t, memberTx, root, member, "all-admin-tx-scope", "member_bad_nonce_001", 11)
	require.Error(t, app.enforceAppV23ControlElevation(memberTx, 11), "non-admin must never carry root elevation")
}

func TestAppV23ExplicitGrantExtendsMemberAndManagerScope(t *testing.T) {
	app := setupTestApp(t)
	root := newAgentKey(t)
	owner := newAgentKey(t)
	member := newAgentKey(t)
	manager := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, owner, "member", 2, 0)
	registerAppV23Agent(t, app, member, "member", 3, 0)
	registerAppV23Agent(t, app, manager, "member", 4, 0)
	require.NoError(t, app.badgerStore.RegisterDomain("foreign-domain", owner.id, "", 5))
	require.NoError(t, app.badgerStore.EnsureAppV23Root("grant-data-scope", 10))
	managerEnrollment, err := app.badgerStore.GetAppV23Enrollment(manager.id)
	require.NoError(t, err)
	managerRole, err := app.badgerStore.GetAppV23Role(manager.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		root.id, manager.id, store.AppV23RoleManager,
		managerEnrollment.Profile, store.AppV23ProfileStandard,
		managerEnrollment.Clearance, 0,
		managerRole.Revision, managerEnrollment.Revision, 11,
	))
	app.appV23AppliedHeight = 10
	now := appV23BlockTime()
	require.NoError(t, app.badgerStore.SetAccessGrant("foreign-domain", member.id, 2, now.Add(time.Hour).Unix(), root.id))
	require.NoError(t, app.badgerStore.SetAccessGrant("foreign-domain", manager.id, 3, now.Add(time.Hour).Unix(), root.id))

	memberWrite, code, err := app.appV23DomainDecision(
		&tx.ParsedTx{}, member.id, "foreign-domain", store.AppV23VerbWrite, 12, now,
	)
	require.NoError(t, err)
	require.True(t, memberWrite)
	require.Empty(t, code)
	memberModify, _, err := app.appV23DomainDecision(
		&tx.ParsedTx{}, member.id, "foreign-domain", store.AppV23VerbModify, 12, now,
	)
	require.NoError(t, err)
	require.False(t, memberModify)
	managerModify, code, err := app.appV23DomainDecision(
		&tx.ParsedTx{}, manager.id, "foreign-domain", store.AppV23VerbModify, 12, now,
	)
	require.NoError(t, err)
	require.True(t, managerModify, "a level-3 grant extends Manager authority outside its groups")
	require.Empty(t, code)

	expired := now.Add(2 * time.Hour)
	memberExpired, _, err := app.appV23DomainDecision(
		&tx.ParsedTx{}, member.id, "foreign-domain", store.AppV23VerbWrite, 12, expired,
	)
	require.NoError(t, err)
	require.False(t, memberExpired)
}

func TestAppV23SubmitClassificationCannotExceedEnrollmentClearance(t *testing.T) {
	t.Run("pre-v23 replay remains unchanged", func(t *testing.T) {
		legacy := setupTestApp(t)
		agent := newAgentKey(t)
		registerAppV23Agent(t, legacy, agent, "member", 1, 0)
		require.NoError(t, legacy.badgerStore.RegisterDomain("legacy-domain", agent.id, "", 1))
		require.NoError(t, legacy.badgerStore.SetAccessGrant("legacy-domain", agent.id, 2, 0, agent.id))
		submit := makeMemorySubmitTx(t, agent, "legacy-domain", "legacy classified")
		submit.MemorySubmit.Classification = tx.ClearanceTopSecret
		result := legacy.processMemorySubmit(submit, 2, appV23BlockTime())
		require.Zero(t, result.Code, result.Log)
	})

	app := setupTestApp(t)
	root := newAgentKey(t)
	member := newAgentKey(t)
	registerAppV23Agent(t, app, root, "admin", 1, 0)
	registerAppV23Agent(t, app, member, "member", 2, 0)
	require.NoError(t, app.badgerStore.EnsureAppV23Root("classification-scope", 10))
	enrollment, err := app.badgerStore.GetAppV23Enrollment(member.id)
	require.NoError(t, err)
	role, err := app.badgerStore.GetAppV23Role(member.id)
	require.NoError(t, err)
	require.NoError(t, app.badgerStore.SetAppV23Policy(
		root.id, member.id, store.AppV23RoleMember,
		enrollment.Profile, store.AppV23ProfileStandard,
		0, enrollment.Capabilities,
		role.Revision, enrollment.Revision, 11,
	))
	enrollment, err = app.badgerStore.GetAppV23Enrollment(member.id)
	require.NoError(t, err)
	app.appV23AppliedHeight = 10

	tooHigh := makeMemorySubmitTx(t, member, enrollment.HomeDomain, "too secret")
	tooHigh.MemorySubmit.Classification = tx.ClearanceTopSecret
	denied := app.processMemorySubmit(tooHigh, 12, appV23BlockTime())
	require.NotZero(t, denied.Code)
	require.Equal(t, "access denied", denied.Log)

	public := makeMemorySubmitTx(t, member, enrollment.HomeDomain, "public fact")
	public.MemorySubmit.Classification = tx.ClearancePublic
	allowed := app.processMemorySubmit(public, 12, appV23BlockTime())
	require.Zero(t, allowed.Code, allowed.Log)

	rootTopSecret := makeMemorySubmitTx(t, root, "root-secret", "root classified fact")
	rootTopSecret.MemorySubmit.Classification = tx.ClearanceTopSecret
	rootAllowed := app.processMemorySubmit(rootTopSecret, 12, appV23BlockTime())
	require.Zero(t, rootAllowed.Code, rootAllowed.Log)
}

func TestAppV23ValidatorIdentityPlaneRemainsSeparateFromLocalAgents(t *testing.T) {
	setup := func(t *testing.T) (*SageApp, agentKey) {
		t.Helper()
		app := setupTestApp(t)
		root := newAgentKey(t)
		validatorKey := newAgentKey(t)
		registerAppV23Agent(t, app, root, "admin", 1, 0)
		require.NoError(t, app.badgerStore.EnsureAppV23Root("validator-identity-scope", 10))
		app.appV23AppliedHeight = 10
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: validatorKey.id, PublicKey: validatorKey.pub, Power: 10,
		}))
		return app, validatorKey
	}

	t.Run("validator-only memory vote succeeds", func(t *testing.T) {
		app, validatorKey := setup(t)
		const memoryID = "app-v23-validator-only-vote"
		require.NoError(t, app.badgerStore.SetMemoryHash(
			memoryID, make([]byte, 32), string(memory.StatusProposed),
		))
		vote := &tx.ParsedTx{
			Type: tx.TxTypeMemoryVote, Nonce: 1, Timestamp: appV23BlockTime(),
			MemoryVote: &tx.MemoryVote{
				MemoryID: memoryID, Decision: tx.VoteDecisionAccept,
			},
		}
		require.NoError(t, tx.SignTx(vote, validatorKey.priv))

		result := app.processTx(vote, 11, appV23BlockTime())
		require.Zero(t, result.Code, result.Log)
	})

	t.Run("validator-only memory vote rejects elevation smuggling", func(t *testing.T) {
		app, validatorKey := setup(t)
		vote := &tx.ParsedTx{
			Type: tx.TxTypeMemoryVote, Nonce: 1, Timestamp: appV23BlockTime(),
			MemoryVote: &tx.MemoryVote{
				MemoryID: "app-v23-elevation-smuggling", Decision: tx.VoteDecisionAccept,
			},
			LocalElevation: &tx.LocalElevationProof{
				RootGeneration:  1,
				ValidFromHeight: 11, ValidUntilHeight: 11,
				Nonce: "validator_smuggle_001",
			},
		}
		require.NoError(t, tx.SignTx(vote, validatorKey.priv))

		result := app.processTx(vote, 11, appV23BlockTime())
		require.Equal(t, uint32(110), result.Code)
		require.Equal(t, "access denied", result.Log)
	})

	t.Run("direct validator governance vote succeeds", func(t *testing.T) {
		app, validatorKey := setup(t)
		proposalID, err := app.govEngine.Propose(
			"app-v23-review-proposer",
			governance.OpUpdatePower,
			validatorKey.id,
			nil,
			10,
			governance.DefaultExpiryBlocks,
			"validator identity plane regression",
			11,
			nil,
		)
		require.NoError(t, err)
		vote := &tx.ParsedTx{
			Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: appV23BlockTime(),
			GovVote: &tx.GovVote{
				ProposalID: proposalID, Decision: tx.VoteDecisionAccept,
			},
		}
		require.NoError(t, tx.SignTx(vote, validatorKey.priv))

		result := app.processTx(vote, 12, appV23BlockTime())
		require.Zero(t, result.Code, result.Log)
	})
}
