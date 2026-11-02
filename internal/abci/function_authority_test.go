package abci

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	abcitypes "github.com/cometbft/cometbft/abci/types"
	"github.com/l33tdawg/sage/internal/contentvalidator"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/require"
)

const authorityTestFunction = "lab-observation-producer"
const authorityTestDomain = "lab-observations"
const authorityTestOutcome = "lab_observation"

type authorityTestFixture struct {
	app    *SageApp
	root   agentKey
	member agentKey
	other  agentKey
	nonces map[string]uint64
	serial int
}

func newAuthorityTestFixture(t *testing.T) *authorityTestFixture {
	return newAuthorityTestFixtureThrough(t, true)
}

func newAuthorityTestFixtureThrough(t *testing.T, includeV31 bool) *authorityTestFixture {
	t.Helper()
	f := &authorityTestFixture{app: setupTestApp(t), root: newAgentKey(t), member: newAgentKey(t), other: newAgentKey(t), nonces: make(map[string]uint64)}
	registerAppV23Agent(t, f.app, f.root, store.AppV23RoleAdmin, 1, 0)
	registerAppV23Agent(t, f.app, f.member, store.AppV23RoleMember, 2, 0)
	registerAppV23Agent(t, f.app, f.other, store.AppV23RoleMember, 3, 0)
	require.NoError(t, f.app.badgerStore.EnsureAppV23Root("function-authority-fixture", 35))
	require.NoError(t, f.app.badgerStore.RegisterDomain(authorityTestDomain, f.member.id, "", 36))
	require.NoError(t, f.app.badgerStore.SetAccessGrant(authorityTestDomain, f.member.id, 2, 0, f.member.id))
	require.NoError(t, f.app.badgerStore.SetAccessGrant(authorityTestDomain, f.other.id, 2, 0, f.member.id))
	f.installValidator(nil)
	stageAppV28Activation(t, f.app, 60)
	finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 60, Time: appV23BlockTime()})
	finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 61, Time: appV23BlockTime()})
	for _, activation := range []struct {
		name    string
		version uint64
		height  int64
	}{
		{appV29UpgradeName, 29, 62}, {appV30UpgradeName, 30, 64}, {appV31UpgradeName, 31, 66},
	} {
		if activation.version == 31 && !includeV31 {
			continue
		}
		require.NoError(t, f.app.badgerStore.SetUpgradePlan(&store.UpgradePlanRecord{
			Name: activation.name, TargetAppVersion: activation.version,
			ActivationHeight: activation.height, ProposedAt: activation.height - 1,
		}))
		finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: activation.height, Time: appV23BlockTime()})
		finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: activation.height + 1, Time: appV23BlockTime()})
	}
	require.Equal(t, includeV31, f.app.postAppV31Fork(68))
	return f
}

func (f *authorityTestFixture) installValidator(alter func(*contentvalidator.FunctionAuthorityRequest)) {
	registry := contentvalidator.NewContentValidatorRegistry()
	registry.RegisterClosedDomain(authorityTestDomain)
	registry.RegisterContentValidator(authorityTestDomain, authorityTestOutcome, func(*memory.MemoryRecord) error {
		return errors.New("legacy functional role rejects member")
	})
	registry.RegisterFunctionContentValidator(authorityTestDomain, authorityTestOutcome, authorityTestFunction,
		func(rec *memory.MemoryRecord, ctx contentvalidator.ValidationContext) error {
			request := contentvalidator.FunctionAuthorityRequest{Function: authorityTestFunction, Domain: rec.DomainTag, Subject: rec.SubmittingAgent}
			if alter != nil {
				alter(&request)
			}
			result, err := ctx.AuthorizeFunction(request)
			if err != nil {
				return err
			}
			if !result.Allowed || result.Function != request.Function || result.Domain != request.Domain || result.Subject != request.Subject {
				return errors.New("explicit function designation denied")
			}
			return nil
		})
	f.app.SetContentValidators(registry)
}

func (f *authorityTestFixture) control(t *testing.T, signer agentKey, action tx.FunctionAuthority) []byte {
	t.Helper()
	if action.Scope == "" {
		root, err := f.app.badgerStore.GetAppV23Root()
		require.NoError(t, err)
		require.NotNil(t, root)
		action.Scope = root.Scope
	}
	pub, sig, bodyHash, stamp := signAgentProof(t, signer, tx.FunctionAuthorityActionBytes(&action))
	parsed := &tx.ParsedTx{Type: tx.TxTypeFunctionAuthority, FunctionAuthority: &action,
		AgentPubKey: pub, AgentSig: sig, AgentBodyHash: bodyHash, AgentTimestamp: stamp}
	f.nonces[signer.id]++
	signAppV23Outer(t, parsed, signer, f.nonces[signer.id])
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw
}

func (f *authorityTestFixture) submit(t *testing.T, signer agentKey) ([]byte, *tx.ParsedTx) {
	t.Helper()
	f.serial++
	content := fmt.Sprintf(`{"outcome_class":%q,"message":"synthetic core authority fixture %d"}`, authorityTestOutcome, f.serial)
	return f.submitContent(t, signer, content)
}

func (f *authorityTestFixture) submitContent(t *testing.T, signer agentKey, content string) ([]byte, *tx.ParsedTx) {
	t.Helper()
	parsed := makeMemorySubmitTx(t, signer, authorityTestDomain, content)
	f.nonces[signer.id]++
	signAppV23Outer(t, parsed, signer, f.nonces[signer.id])
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)
	return raw, parsed
}

func (f *authorityTestFixture) activate(t *testing.T, height, activation int64) {
	t.Helper()
	raw := f.control(t, f.root, tx.FunctionAuthority{Action: "activate", Function: authorityTestFunction, Domain: authorityTestDomain, ActivationHeight: activation})
	response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: height, Time: appV23BlockTime(), Txs: [][]byte{raw}})
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
}

func authorityAssign(subject string, revision uint64) tx.FunctionAuthority {
	return tx.FunctionAuthority{Action: "assign", Function: authorityTestFunction, Domain: authorityTestDomain, Subject: subject, ExpectedRevision: revision}
}

func TestFunctionAuthorityRootPolicyFutureBoundaryAndLegacyReplay(t *testing.T) {
	f := newAuthorityTestFixture(t)
	f.activate(t, 68, 70)
	assign := f.control(t, f.root, authorityAssign(f.member.id, 0))
	response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 69, Time: appV23BlockTime(), Txs: [][]byte{assign}})
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
	_, parsed := f.submit(t, f.member)
	rec := &memory.MemoryRecord{SubmittingAgent: f.member.id, DomainTag: authorityTestDomain, Content: parsed.MemorySubmit.Content}
	denied, why := f.app.validateMemoryContentAt(parsed, rec, authorityTestOutcome, 69, appV23BlockTime())
	require.True(t, denied)
	require.Contains(t, why, "legacy functional role")
	denied, why = f.app.validateMemoryContentAt(parsed, rec, authorityTestOutcome, 70, appV23BlockTime())
	require.False(t, denied, why)
	raw, _ := f.submit(t, f.member)
	request := &abcitypes.RequestFinalizeBlock{Height: 70, Time: appV23BlockTime(), Txs: [][]byte{raw}}
	response = finalizeAndCommit(t, f.app, request)
	require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
	d, err := f.app.badgerStore.GetFunctionDesignation(authorityTestFunction, authorityTestDomain, f.member.id)
	require.NoError(t, err)
	require.Equal(t, uint64(1), d.Revision)
	// A committed-height replay must not rerun assignment or mutate its revision.
	replayed := finalizeAndCommit(t, f.app, request)
	require.Equal(t, response.AppHash, replayed.AppHash)
	after, err := f.app.badgerStore.GetFunctionDesignation(authorityTestFunction, authorityTestDomain, f.member.id)
	require.NoError(t, err)
	require.Equal(t, d, after)
}

func TestFunctionAuthorityOnlyRootControlsAndSelfGrantDoesNotDesignate(t *testing.T) {
	f := newAuthorityTestFixture(t)
	f.activate(t, 68, 69)
	selfAssign := f.control(t, f.member, authorityAssign(f.member.id, 0))
	unassignedSubmit, _ := f.submit(t, f.member)
	response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 69, Time: appV23BlockTime(), Txs: [][]byte{selfAssign, unassignedSubmit}})
	require.NotZero(t, response.TxResults[0].Code, "owner/self-granter cannot create designation")
	require.Equal(t, uint32(18), response.TxResults[1].Code, response.TxResults[1].Log)
	d, err := f.app.badgerStore.GetFunctionDesignation(authorityTestFunction, authorityTestDomain, f.member.id)
	require.NoError(t, err)
	require.Nil(t, d)
	p, err := f.app.badgerStore.GetFunctionAuthorityPolicy(authorityTestFunction, authorityTestDomain)
	require.NoError(t, err)
	require.Equal(t, f.root.id, p.CreatedBy)
}

func TestFunctionAuthorityAuthenticRootWrongScopeCannotActivate(t *testing.T) {
	f := newAuthorityTestFixture(t)
	root, err := f.app.badgerStore.GetAppV23Root()
	require.NoError(t, err)
	foreignScope := root.Scope + "-other-chain"
	// Sign the foreign scope authentically with the executing chain's current
	// Root. Signature validity must not substitute for exact chain scope.
	raw := f.control(t, f.root, tx.FunctionAuthority{Scope: foreignScope, Action: "activate",
		Function: authorityTestFunction, Domain: authorityTestDomain, ActivationHeight: 69})
	parsed, err := tx.DecodeTxWithFunctionAuthority(raw, true)
	require.NoError(t, err)
	require.Equal(t, foreignScope, parsed.FunctionAuthority.Scope)
	actor, err := f.app.verifyCurrentAgentIdentity(parsed, 68)
	require.NoError(t, err, "wrong-scope request still has an authentic current Root proof")
	require.Equal(t, root.CredentialID, actor)
	response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{
		Height: 68, Time: appV23BlockTime(), Txs: [][]byte{raw},
	})
	require.Equal(t, uint32(110), response.TxResults[0].Code)
	require.Equal(t, "access denied", response.TxResults[0].Log)
	policy, err := f.app.badgerStore.GetFunctionAuthorityPolicy(authorityTestFunction, authorityTestDomain)
	require.NoError(t, err)
	require.Nil(t, policy)
}

func TestFunctionAuthorityActiveClosedDomainRejectsUnrecognizedContent(t *testing.T) {
	for _, content := range []string{
		`{"outcome_class":`,
		`{"outcome_class":"unregistered_lab_outcome","message":"synthetic malformed input fixture"}`,
		`{"message":"synthetic missing outcome fixture"}`,
	} {
		t.Run(content, func(t *testing.T) {
			f := newAuthorityTestFixture(t)
			f.activate(t, 68, 69)
			assign := f.control(t, f.root, authorityAssign(f.member.id, 0))
			response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{
				Height: 69, Time: appV23BlockTime(), Txs: [][]byte{assign},
			})
			require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
			require.Empty(t, f.app.contentValidators.FunctionFor(authorityTestDomain, parseOutcomeClass(content)))
			raw, parsed := f.submitContent(t, f.member, content)
			response = finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{
				Height: 70, Time: appV23BlockTime(), Txs: [][]byte{raw},
			})
			require.Equal(t, uint32(18), response.TxResults[0].Code, response.TxResults[0].Log)
			require.Contains(t, response.TxResults[0].Log, "closed domain")
			memoryID := memoryIDForSubmit(parsed.MemorySubmit, 70, f.member.id)
			_, _, err := f.app.badgerStore.GetMemoryHash(memoryID)
			require.Error(t, err, "refused closed-domain content must not persist")
		})
	}
}

func TestFunctionAuthorityScopedAssignSubmitRevokeSubmitOrder(t *testing.T) {
	f := newAuthorityTestFixture(t)
	f.activate(t, 68, 69)
	assign := f.control(t, f.root, authorityAssign(f.member.id, 0))
	accepted, _ := f.submit(t, f.member)
	revokeAction := authorityAssign(f.member.id, 1)
	revokeAction.Action = "revoke"
	revoke := f.control(t, f.root, revokeAction)
	denied, _ := f.submit(t, f.member)
	response, err := f.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 69, Time: appV23BlockTime(), Txs: [][]byte{assign, accepted, revoke, denied}})
	require.NoError(t, err)
	require.Len(t, response.TxResults, 4)
	for i := 0; i < 3; i++ {
		require.Zero(t, response.TxResults[i].Code, "tx%d: %s", i, response.TxResults[i].Log)
	}
	require.Equal(t, uint32(18), response.TxResults[3].Code, response.TxResults[3].Log)
	d, err := f.app.badgerStore.GetFunctionDesignation(authorityTestFunction, authorityTestDomain, f.member.id)
	require.NoError(t, err)
	require.Nil(t, d, "live readers must not see speculative assignment/revocation")
	_, err = f.app.Commit(context.Background(), &abcitypes.RequestCommit{})
	require.NoError(t, err)
	d, err = f.app.badgerStore.GetFunctionDesignation(authorityTestFunction, authorityTestDomain, f.member.id)
	require.NoError(t, err)
	require.True(t, d.Revoked)
	require.Equal(t, uint64(2), d.Revision)
}

func TestFunctionAuthorityAbortedFinalizeDoesNotLeakDesignationOrMemory(t *testing.T) {
	f := newAuthorityTestFixture(t)
	f.activate(t, 68, 69)
	before, err := f.app.badgerStore.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	assign := f.control(t, f.root, authorityAssign(f.member.id, 0))
	submit, _ := f.submit(t, f.member)
	response, err := f.app.FinalizeBlock(context.Background(), &abcitypes.RequestFinalizeBlock{Height: 69, Time: appV23BlockTime(), Txs: [][]byte{assign, submit}})
	require.NoError(t, err)
	for _, result := range response.TxResults {
		require.Zero(t, result.Code, result.Log)
	}
	require.NotNil(t, f.app.pendingAppV20Finalize)
	memoryID := string(response.TxResults[1].Data)
	_, _, err = f.app.badgerStore.GetMemoryHash(memoryID)
	require.Error(t, err, "successful speculative submission must not leak before Commit")
	// Exercise the normal consensus shutdown path which discards pending state.
	dbPath := f.app.badgerStore.DB().Opts().Dir
	require.NoError(t, f.app.CloseConsensusState())
	reopened, err := store.NewBadgerStore(dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = reopened.CloseBadger() })
	d, err := reopened.GetFunctionDesignation(authorityTestFunction, authorityTestDomain, f.member.id)
	require.NoError(t, err)
	require.Nil(t, d)
	_, _, err = reopened.GetMemoryHash(memoryID)
	require.Error(t, err)
	after, err := reopened.ComputeAppHashExcludingBookkeeping()
	require.NoError(t, err)
	require.Equal(t, before, after)
}

func TestFunctionAuthorityExpiryAndExactScopeThroughRealSubmit(t *testing.T) {
	for _, mode := range []string{"expiry", "cross-function", "cross-function-request", "cross-domain", "cross-subject", "missing-exact-grant"} {
		t.Run(mode, func(t *testing.T) {
			f := newAuthorityTestFixture(t)
			f.activate(t, 68, 69)
			action := authorityAssign(f.member.id, 0)
			if mode == "expiry" {
				action.ExpiresAt = appV23BlockTime().Unix() + 1
			}
			if mode == "cross-function" || mode == "cross-function-request" {
				action.Function = "another-function"
				f.policyFor(t, action.Function, authorityTestDomain)
			}
			assign := f.control(t, f.root, action)
			response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 69, Time: appV23BlockTime(), Txs: [][]byte{assign}})
			require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
			if mode == "cross-domain" {
				f.installValidator(func(r *contentvalidator.FunctionAuthorityRequest) { r.Domain = "other-domain" })
			}
			if mode == "cross-function-request" {
				// G is genuinely assigned and active, but this schema is bound to F.
				// A trusted-looking callback cannot substitute G for selected F.
				f.installValidator(func(r *contentvalidator.FunctionAuthorityRequest) { r.Function = "another-function" })
			}
			if mode == "cross-subject" {
				f.installValidator(func(r *contentvalidator.FunctionAuthorityRequest) { r.Subject = f.other.id })
			}
			if mode == "missing-exact-grant" {
				require.NoError(t, f.app.badgerStore.DeleteAccessGrant(authorityTestDomain, f.member.id))
			}
			submit, _ := f.submit(t, f.member)
			when := appV23BlockTime()
			if mode == "expiry" {
				when = when.Add(time.Second)
			}
			response = finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 70, Time: when, Txs: [][]byte{submit}})
			require.Equal(t, uint32(18), response.TxResults[0].Code, response.TxResults[0].Log)
		})
	}
}

func TestFunctionAuthorityDesignationCannotBypassCurrentPrincipalPolicy(t *testing.T) {
	for _, mode := range []string{"inactive", "read-only", "unknown-role", "unregistered"} {
		t.Run(mode, func(t *testing.T) {
			f := newAuthorityTestFixture(t)
			f.activate(t, 68, 69)
			assign := f.control(t, f.root, authorityAssign(f.member.id, 0))
			response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 69, Time: appV23BlockTime(), Txs: [][]byte{assign}})
			require.Zero(t, response.TxResults[0].Code, response.TxResults[0].Log)
			enrollment, err := f.app.badgerStore.GetAppV23Enrollment(f.member.id)
			require.NoError(t, err)
			role, err := f.app.badgerStore.GetAppV23Role(f.member.id)
			require.NoError(t, err)
			signer := f.member
			switch mode {
			case "inactive":
				updated := *enrollment
				updated.ApprovedBy = f.root.id
				updated.Active = false
				updated.Profile = store.AppV23ProfileStandard
				updated.HomeDomain = ""
				updated.UpdatedHeight = 70
				updated.RetireOwnedDomainsToRoot = true
				require.NoError(t, f.app.badgerStore.ApproveAppV23LocalAgent(updated, store.AppV23RoleMember, enrollment.Revision, role.Revision))
			case "read-only":
				require.NoError(t, f.app.badgerStore.SetAppV23Policy(f.root.id, f.member.id, store.AppV23RoleMember,
					enrollment.Profile, store.AppV23ProfileReadOnly, 1, store.AgentCapabilityReadAllDomains,
					role.Revision, enrollment.Revision, 70))
			case "unknown-role":
				// Malformed consensus policy is seeded only to verify fail-closed
				// behavior; there is intentionally no valid mutation creating it.
				role.Role = "unknown-function-role"
				wire, err := json.Marshal(role)
				require.NoError(t, err)
				require.NoError(t, f.app.badgerStore.SetRawForTest([]byte("appv23:role:"+f.member.id), wire))
			case "unregistered":
				signer = newAgentKey(t)
			}
			raw, _ := f.submit(t, signer)
			response = finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 70, Time: appV23BlockTime(), Txs: [][]byte{raw}})
			require.NotZero(t, response.TxResults[0].Code, "an existing designation cannot override current %s policy", mode)
		})
	}
}

func (f *authorityTestFixture) policyFor(t *testing.T, function, domain string) {
	t.Helper()
	raw := f.control(t, f.root, tx.FunctionAuthority{Action: "activate", Function: function, Domain: domain, ActivationHeight: 70})
	parsed, err := tx.DecodeTxWithFunctionAuthority(raw, true)
	require.NoError(t, err)
	result := f.app.processTx(parsed, 69, appV23BlockTime())
	require.Zero(t, result.Code, result.Log)
}

func TestFunctionAuthorityRaw41HistoricalUnknownBeforeMalformedPayload(t *testing.T) {
	// Deliberately malformed payload remains historical unknown-type before v31.
	for _, malformed := range []bool{false, true} {
		f := newAuthorityTestFixtureThrough(t, false)
		wire := f.control(t, f.root, authorityAssign(f.member.id, 0))
		if malformed {
			wire = wire[:len(wire)-1]
		}
		parsed, err := f.app.decodeConsensusTx(wire, 66)
		require.Nil(t, parsed)
		require.ErrorIs(t, err, tx.ErrUnknownTxType)
		response := finalizeAndCommit(t, f.app, &abcitypes.RequestFinalizeBlock{Height: 66, Time: appV23BlockTime(), Txs: [][]byte{wire}})
		require.Equal(t, uint32(1), response.TxResults[0].Code)
		require.Equal(t, tx.ErrUnknownTxType.Error(), response.TxResults[0].Log)
	}
}
