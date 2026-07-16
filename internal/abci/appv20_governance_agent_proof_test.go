package abci

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/l33tdawg/sage/internal/validator"
)

type delegatedScopeGovFixture struct {
	app       *SageApp
	admin     agentKey
	proposer  agentKey
	voter     agentKey
	candidate agentKey
	domain    string
	template  scope.ProposalTemplate
	payload   []byte
}

func setupDelegatedScopeGovFixture(t *testing.T) delegatedScopeGovFixture {
	t.Helper()
	app := setupTestApp(t)
	app.appV20AppliedHeight = 1
	domain := "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a"
	require.NoError(t, app.ensureGovernanceDelegationDomain(domain))

	admin := newAgentKey(t)
	registerAgent(t, app, admin, "operator-admin", "admin")
	proposer := newAgentKey(t)
	registerAgent(t, app, proposer, "validator-proposer", "member")
	voter := newAgentKey(t)
	registerAgent(t, app, voter, "validator-voter", "member")
	candidate := newAgentKey(t)

	for _, actor := range []agentKey{proposer, voter} {
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: actor.id, PublicKey: actor.pub, Power: 10,
		}))
	}
	require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{
		proposer.id: 10,
		voter.id:    10,
	}))
	require.NoError(t, app.badgerStore.RegisterDomain("research", admin.id, "", 1))

	template := scope.ProposalTemplate{
		ScopeID:               "scope-research",
		Revision:              1,
		State:                 "active",
		ControllerValidatorID: proposer.id,
		Domains:               []string{"research"},
		Members: []scope.ProposalMember{{
			ValidatorID: proposer.id, AssignedWeight: 10,
		}},
	}
	payload, err := scope.EncodeProposalTemplate(template)
	require.NoError(t, err)
	return delegatedScopeGovFixture{
		app: app, admin: admin, proposer: proposer, voter: voter, candidate: candidate, domain: domain,
		template: template, payload: payload,
	}
}

func governanceJSON(t *testing.T, value any) []byte {
	t.Helper()
	body, err := json.Marshal(value)
	require.NoError(t, err)
	return body
}

func attachGovernanceRequestProof(
	t *testing.T,
	parsed *tx.ParsedTx,
	authorizer, outer agentKey,
	method, path string,
	body []byte,
	proofTime time.Time,
	nonce []byte,
) {
	t.Helper()
	request := append([]byte(method+" "+path+"\n"), body...)
	bodyHash := sha256.Sum256(request)
	var sig []byte
	if len(nonce) == 0 {
		sig = auth.SignRequest(authorizer.priv, method, path, body, proofTime.Unix())
	} else {
		sig = auth.SignRequestWithNonce(authorizer.priv, method, path, body, proofTime.Unix(), nonce)
	}
	parsed.AgentPubKey = append(ed25519.PublicKey(nil), authorizer.pub...)
	parsed.AgentSig = sig
	parsed.AgentTimestamp = proofTime.Unix()
	parsed.AgentBodyHash = append([]byte(nil), bodyHash[:]...)
	parsed.AgentNonce = append([]byte(nil), nonce...)
	parsed.AgentRequest = request
	require.NoError(t, tx.SignTx(parsed, outer.priv))
}

func delegatedScopeProposalTx(t *testing.T, fixture delegatedScopeGovFixture, route string, proofTime time.Time, nonce []byte) *tx.ParsedTx {
	t.Helper()
	body := governanceJSON(t, struct {
		ValidatorID      string                  `json:"validator_id"`
		GovernanceDomain string                  `json:"governance_domain"`
		Operation        string                  `json:"operation"`
		Reason           string                  `json:"reason"`
		Scope            *scope.ProposalTemplate `json:"scope"`
	}{ValidatorID: fixture.proposer.id, GovernanceDomain: fixture.domain, Operation: "scope_action", Reason: "authorize canonical scope", Scope: &fixture.template})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: proofTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpScopeAction,
			TargetID:  fixture.template.ScopeID,
			Reason:    "authorize canonical scope",
			Payload:   fixture.payload,
		},
	}
	attachGovernanceRequestProof(t, parsed, fixture.admin, fixture.proposer, "POST", route, body, proofTime, nonce)
	return parsed
}

func delegatedValidatorAddProposalTx(t *testing.T, fixture delegatedScopeGovFixture, proofTime time.Time, nonce []byte) *tx.ParsedTx {
	t.Helper()
	body := governanceJSON(t, map[string]any{
		"validator_id":      fixture.proposer.id,
		"governance_domain": fixture.domain,
		"operation":         "add_validator",
		"target_id":         fixture.candidate.id,
		"target_pubkey":     fixture.candidate.id,
		"target_power":      5,
		"reason":            "add approved validator",
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: proofTime,
		GovPropose: &tx.GovPropose{
			Operation:    tx.GovOpAddValidator,
			TargetID:     fixture.candidate.id,
			TargetPubKey: fixture.candidate.pub,
			TargetPower:  5,
			Reason:       "add approved validator",
		},
	}
	attachGovernanceRequestProof(t, parsed, fixture.admin, fixture.proposer, "POST", "/v1/governance/propose", body, proofTime, nonce)
	return parsed
}

func createActiveScopeProposal(t *testing.T, fixture delegatedScopeGovFixture, height int64) string {
	t.Helper()
	proposalID, err := fixture.app.govEngine.Propose(
		fixture.proposer.id,
		governance.OpScopeAction,
		fixture.template.ScopeID,
		nil,
		0,
		governance.DefaultExpiryBlocks,
		"scope proposal",
		height,
		fixture.payload,
	)
	require.NoError(t, err)
	return proposalID
}

func delegatedScopeVoteTx(t *testing.T, fixture delegatedScopeGovFixture, route, proposalID string, proofTime time.Time, nonce []byte) *tx.ParsedTx {
	t.Helper()
	body := governanceJSON(t, map[string]any{
		"validator_id": fixture.voter.id, "governance_domain": fixture.domain,
		"proposal_id": proposalID, "decision": "accept",
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: proofTime,
		GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept},
	}
	attachGovernanceRequestProof(t, parsed, fixture.admin, fixture.voter, "POST", route, body, proofTime, nonce)
	return parsed
}

func TestAppV20DelegatedScopeProposalUsesAdminAuthorizerAndOuterValidatorActor(t *testing.T) {
	for _, route := range []string{
		"/v1/governance/propose",
		"/v1/dashboard/governance/propose",
	} {
		t.Run(route, func(t *testing.T) {
			fixture := setupDelegatedScopeGovFixture(t)
			blockTime := time.Now().Truncate(time.Second)
			parsed := delegatedScopeProposalTx(t, fixture, route, blockTime, []byte("nonce001"))

			result := fixture.app.processTx(parsed, 2, blockTime)
			require.Zero(t, result.Code, result.Log)
			proposalID := governance.ComputeProposalID(
				fixture.proposer.id, 2, governance.OpScopeAction, fixture.template.ScopeID,
			)
			proposal, err := fixture.app.govEngine.LoadProposal(proposalID)
			require.NoError(t, err)
			assert.Equal(t, fixture.proposer.id, proposal.ProposerID)
			assert.NotEqual(t, fixture.admin.id, proposal.ProposerID)
			outerVote, err := fixture.app.badgerStore.GetState("gov:vote:" + proposalID + ":" + fixture.proposer.id)
			require.NoError(t, err)
			assert.Equal(t, []byte("accept"), outerVote)
			authorizerVote, err := fixture.app.badgerStore.GetState("gov:vote:" + proposalID + ":" + fixture.admin.id)
			require.NoError(t, err)
			assert.Nil(t, authorizerVote, "admin authorizer must receive no validator power")
			_, adminIsValidator := fixture.app.validators.GetValidator(fixture.admin.id)
			assert.False(t, adminIsValidator)
		})
	}
}

func TestAppV20DelegatedNonScopeProposalUsesAdminAuthorizerAndOuterValidatorActor(t *testing.T) {
	fixture := setupDelegatedScopeGovFixture(t)
	blockTime := time.Now().Truncate(time.Second)
	parsed := delegatedValidatorAddProposalTx(t, fixture, blockTime, []byte("nonce010"))

	result := fixture.app.processTx(parsed, 2, blockTime)
	require.Zero(t, result.Code, result.Log)
	proposalID := governance.ComputeProposalID(
		fixture.proposer.id, 2, governance.OpAddValidator, fixture.candidate.id,
	)
	proposal, err := fixture.app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	assert.Equal(t, fixture.proposer.id, proposal.ProposerID)
	assert.Equal(t, governance.OpAddValidator, proposal.Operation)
	outerVote, err := fixture.app.badgerStore.GetState("gov:vote:" + proposalID + ":" + fixture.proposer.id)
	require.NoError(t, err)
	assert.Equal(t, []byte("accept"), outerVote)
	adminVote, err := fixture.app.badgerStore.GetState("gov:vote:" + proposalID + ":" + fixture.admin.id)
	require.NoError(t, err)
	assert.Nil(t, adminVote)
}

func TestAppV20DelegatedScopeVoteUsesBothRouteFamiliesAndOuterVotingPower(t *testing.T) {
	for _, route := range []string{
		"/v1/governance/vote",
		"/v1/dashboard/governance/vote",
	} {
		t.Run(route, func(t *testing.T) {
			fixture := setupDelegatedScopeGovFixture(t)
			proposalID := createActiveScopeProposal(t, fixture, 2)
			blockTime := time.Now().Truncate(time.Second)
			parsed := delegatedScopeVoteTx(t, fixture, route, proposalID, blockTime, []byte("nonce002"))

			result := fixture.app.processTx(parsed, 3, blockTime)
			require.Zero(t, result.Code, result.Log)
			outerVote, err := fixture.app.badgerStore.GetState("gov:vote:" + proposalID + ":" + fixture.voter.id)
			require.NoError(t, err)
			assert.Equal(t, []byte("accept"), outerVote)
			authorizerVote, err := fixture.app.badgerStore.GetState("gov:vote:" + proposalID + ":" + fixture.admin.id)
			require.NoError(t, err)
			assert.Nil(t, authorizerVote)
		})
	}
}

func TestAppV20DelegatedScopeCancelKeepsOuterProposalOwnership(t *testing.T) {
	fixture := setupDelegatedScopeGovFixture(t)
	proposalID := createActiveScopeProposal(t, fixture, 2)
	blockTime := time.Now().Truncate(time.Second)
	body := governanceJSON(t, map[string]any{
		"validator_id": fixture.proposer.id, "governance_domain": fixture.domain,
		"proposal_id": proposalID,
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovCancel, Nonce: 1, Timestamp: blockTime,
		GovCancel: &tx.GovCancel{ProposalID: proposalID},
	}
	attachGovernanceRequestProof(t, parsed, fixture.admin, fixture.proposer, "POST", "/v1/governance/cancel", body, blockTime, []byte("nonce003"))

	result := fixture.app.processTx(parsed, 3, blockTime)
	require.Zero(t, result.Code, result.Log)
	proposal, err := fixture.app.govEngine.LoadProposal(proposalID)
	require.NoError(t, err)
	assert.Equal(t, governance.StatusCancelled, proposal.Status)
	assert.Equal(t, fixture.proposer.id, proposal.ProposerID)
}

func TestAppV20DelegatedScopeProofRejectsMutationTransplantReplayAndStaleness(t *testing.T) {
	t.Run("proposal payload mutation", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime, []byte("nonce004"))
		parsed.GovPropose.TargetID = "scope-substituted"
		require.NoError(t, tx.SignTx(parsed, fixture.proposer.priv))

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(109), result.Code, result.Log)
		assert.Contains(t, result.Log, "payload differs")
	})

	t.Run("route and transaction transplant", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		proposalID := createActiveScopeProposal(t, fixture, 2)
		blockTime := time.Now().Truncate(time.Second)
		proposalRequest := governanceJSON(t, struct {
			ValidatorID      string                  `json:"validator_id"`
			GovernanceDomain string                  `json:"governance_domain"`
			Operation        string                  `json:"operation"`
			Reason           string                  `json:"reason"`
			Scope            *scope.ProposalTemplate `json:"scope"`
		}{ValidatorID: fixture.voter.id, GovernanceDomain: fixture.domain, Operation: "scope_action", Reason: "authorize canonical scope", Scope: &fixture.template})
		parsed := &tx.ParsedTx{
			Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: blockTime,
			GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept},
		}
		attachGovernanceRequestProof(t, parsed, fixture.admin, fixture.voter, "POST", "/v1/dashboard/governance/propose", proposalRequest, blockTime, []byte("nonce005"))

		result := fixture.app.processTx(parsed, 3, blockTime)
		assert.Equal(t, uint32(109), result.Code, result.Log)
		assert.Contains(t, result.Log, "does not authorize a governance vote")
	})

	t.Run("proof replay under fresh outer nonce", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		proposalID := createActiveScopeProposal(t, fixture, 2)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeVoteTx(t, fixture, "/v1/governance/vote", proposalID, blockTime, []byte("nonce006"))
		first := fixture.app.processTx(parsed, 3, blockTime)
		require.Zero(t, first.Code, first.Log)

		replayed := *parsed
		replayed.Nonce = 2
		require.NoError(t, tx.SignTx(&replayed, fixture.voter.priv))
		second := fixture.app.processTx(&replayed, 4, blockTime.Add(time.Second))
		assert.Equal(t, uint32(109), second.Code, second.Log)
		assert.Contains(t, second.Log, "already consumed")
	})

	t.Run("stale proof", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime.Add(-5*time.Minute-time.Second), []byte("nonce007"))

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(109), result.Code, result.Log)
		assert.Contains(t, result.Log, "older than the 5-minute consensus window")
	})

	t.Run("missing request nonce", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime, nil)

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(109), result.Code, result.Log)
		assert.Contains(t, result.Log, "requires an 8-byte request nonce")
	})
}

func TestAppV20DelegatedScopeRequiresAdminAuthorizerAndActiveOuterValidator(t *testing.T) {
	t.Run("embedded signer is not admin", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		nonAdmin := newAgentKey(t)
		registerAgent(t, fixture.app, nonAdmin, "not-admin", "member")
		// Even an outer validator that is itself an on-chain admin must not let
		// the distinct embedded operator bypass proposal authorization.
		registerAgent(t, fixture.app, fixture.proposer, "validator-admin", "admin")
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime, []byte("nonce008"))
		body := governanceJSON(t, struct {
			ValidatorID      string                  `json:"validator_id"`
			GovernanceDomain string                  `json:"governance_domain"`
			Operation        string                  `json:"operation"`
			Reason           string                  `json:"reason"`
			Scope            *scope.ProposalTemplate `json:"scope"`
		}{ValidatorID: fixture.proposer.id, GovernanceDomain: fixture.domain, Operation: "scope_action", Reason: "authorize canonical scope", Scope: &fixture.template})
		attachGovernanceRequestProof(t, parsed, nonAdmin, fixture.proposer, "POST", "/v1/governance/propose", body, blockTime, []byte("nonce008"))

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(72), result.Code, result.Log)
		assert.Contains(t, result.Log, "only admin agents can authorize")
	})

	t.Run("outer signer is not validator", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		outsider := newAgentKey(t)
		registerAgent(t, fixture.app, outsider, "outer-outsider", "member")
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime, []byte("nonce009"))
		body := governanceJSON(t, struct {
			ValidatorID      string                  `json:"validator_id"`
			GovernanceDomain string                  `json:"governance_domain"`
			Operation        string                  `json:"operation"`
			Reason           string                  `json:"reason"`
			Scope            *scope.ProposalTemplate `json:"scope"`
		}{ValidatorID: outsider.id, GovernanceDomain: fixture.domain, Operation: "scope_action", Reason: "authorize canonical scope", Scope: &fixture.template})
		attachGovernanceRequestProof(t, parsed, fixture.admin, outsider, "POST", "/v1/governance/propose", body, blockTime, []byte("nonce009"))

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(72), result.Code, result.Log)
		assert.Contains(t, result.Log, "not an active on-chain validator")
	})
}

func TestAppV20GovernanceBindingRejectsTransplantsBeforeConsumingProof(t *testing.T) {
	t.Run("active validator outer transplant", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		intended := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime, []byte("nonce011"))
		stolen := *intended
		stolen.Nonce = 77
		require.NoError(t, tx.SignTx(&stolen, fixture.voter.priv))

		attack := fixture.app.processTx(&stolen, 2, blockTime)
		assert.Equal(t, uint32(109), attack.Code, attack.Log)
		assert.Contains(t, attack.Log, "does not authorize outer validator")

		accepted := fixture.app.processTx(intended, 2, blockTime)
		require.Zero(t, accepted.Code, accepted.Log, "a rejected wrapper must not burn the intended proof")
	})

	t.Run("other chain domain", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(t, fixture, "/v1/governance/propose", blockTime, []byte("nonce012"))
		wrongBody := governanceJSON(t, struct {
			ValidatorID      string                  `json:"validator_id"`
			GovernanceDomain string                  `json:"governance_domain"`
			Operation        string                  `json:"operation"`
			Reason           string                  `json:"reason"`
			Scope            *scope.ProposalTemplate `json:"scope"`
		}{
			ValidatorID: fixture.proposer.id, GovernanceDomain: "6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b",
			Operation: "scope_action", Reason: "authorize canonical scope", Scope: &fixture.template,
		})
		attachGovernanceRequestProof(t, parsed, fixture.admin, fixture.proposer, "POST", "/v1/governance/propose", wrongBody, blockTime, []byte("nonce012"))

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(109), result.Code, result.Log)
		assert.Contains(t, result.Log, "does not match this chain")
	})
}

func TestAppV20SameKeyGovernanceProofCannotReplayAcrossChainDomain(t *testing.T) {
	const originDomain = "5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a"
	const otherDomain = "6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b6b"
	blockTime := time.Now().Truncate(time.Second)
	validatorKey := newAgentKey(t)
	candidate := newAgentKey(t)

	setupChain := func(t *testing.T, domain string) *SageApp {
		t.Helper()
		app := setupTestApp(t)
		app.appV20AppliedHeight = 1
		require.NoError(t, app.ensureGovernanceDelegationDomain(domain))
		registerAgent(t, app, validatorKey, "same-key-validator-admin", "admin")
		require.NoError(t, app.validators.AddValidator(&validator.ValidatorInfo{
			ID: validatorKey.id, PublicKey: validatorKey.pub, Power: 10,
		}))
		require.NoError(t, app.badgerStore.SaveValidators(map[string]int64{validatorKey.id: 10}))
		return app
	}

	body := governanceJSON(t, map[string]any{
		"validator_id":      validatorKey.id,
		"governance_domain": originDomain,
		"operation":         "add_validator",
		"target_id":         candidate.id,
		"target_pubkey":     candidate.id,
		"target_power":      3,
		"reason":            "same-key chain-bound proposal",
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpAddValidator, TargetID: candidate.id,
			TargetPubKey: candidate.pub, TargetPower: 3,
			Reason: "same-key chain-bound proposal",
		},
	}
	attachGovernanceRequestProof(
		t, parsed, validatorKey, validatorKey, "POST", "/v1/governance/propose",
		body, blockTime, []byte("samekey1"),
	)
	assert.True(t, hasAgentProofMaterial(parsed))
	assert.False(t, isDelegatedGovernanceProof(parsed), "same-key proof keeps direct authorization semantics")

	origin := setupChain(t, originDomain)
	originResult := origin.processTx(parsed, 2, blockTime)
	require.Zero(t, originResult.Code, originResult.Log)

	other := setupChain(t, otherDomain)
	replayed := other.processTx(parsed, 2, blockTime)
	assert.Equal(t, uint32(109), replayed.Code, replayed.Log)
	assert.Contains(t, replayed.Log, "governance_domain does not match this chain")
	active, err := other.govEngine.GetActiveProposal()
	require.NoError(t, err)
	assert.Nil(t, active)
}

func TestAppV20SameKeyGovernanceProofMustNameOuterValidator(t *testing.T) {
	fixture := setupDelegatedScopeGovFixture(t)
	registerAgent(t, fixture.app, fixture.proposer, "same-key-validator-admin", "admin")
	blockTime := time.Now().Truncate(time.Second)
	body := governanceJSON(t, map[string]any{
		"validator_id": fixture.voter.id, "governance_domain": fixture.domain,
		"operation": "add_validator", "target_id": fixture.candidate.id,
		"target_pubkey": fixture.candidate.id, "target_power": 5,
		"reason": "wrong validator binding",
	})
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpAddValidator, TargetID: fixture.candidate.id,
			TargetPubKey: fixture.candidate.pub, TargetPower: 5,
			Reason: "wrong validator binding",
		},
	}
	attachGovernanceRequestProof(
		t, parsed, fixture.proposer, fixture.proposer, "POST", "/v1/governance/propose",
		body, blockTime, []byte("samekey2"),
	)

	result := fixture.app.processTx(parsed, 2, blockTime)
	assert.Equal(t, uint32(109), result.Code, result.Log)
	assert.Contains(t, result.Log, "does not authorize outer validator")
}

func TestAppV20SameKeyPartialProofCannotFallBackToDirectMode(t *testing.T) {
	fixture := setupDelegatedScopeGovFixture(t)
	registerAgent(t, fixture.app, fixture.proposer, "same-key-validator-admin", "admin")
	blockTime := time.Now().Truncate(time.Second)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: blockTime,
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpAddValidator, TargetID: fixture.candidate.id,
			TargetPubKey: fixture.candidate.pub, TargetPower: 5,
			Reason: "incomplete same-key proof",
		},
		// Any non-zero proof field selects app-v20's bound-proof mode. This
		// historical partial envelope must not silently degrade to direct mode.
		AgentPubKey: fixture.proposer.pub,
	}
	require.NoError(t, tx.SignTx(parsed, fixture.proposer.priv))

	result := fixture.app.processTx(parsed, 2, blockTime)
	assert.Equal(t, uint32(109), result.Code, result.Log)
	assert.Contains(t, result.Log, "requires an 8-byte request nonce")
}

func TestAppV20GovernanceProofConsensusWindowHasFutureBound(t *testing.T) {
	t.Run("one second beyond future bound", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(
			t, fixture, "/v1/governance/propose",
			blockTime.Add(5*time.Minute+time.Second), []byte("future01"),
		)

		result := fixture.app.processTx(parsed, 2, blockTime)
		assert.Equal(t, uint32(109), result.Code, result.Log)
		assert.Contains(t, result.Log, "more than 5 minutes ahead of consensus time")
	})

	t.Run("exact future boundary", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		blockTime := time.Now().Truncate(time.Second)
		parsed := delegatedScopeProposalTx(
			t, fixture, "/v1/governance/propose",
			blockTime.Add(5*time.Minute), []byte("future02"),
		)

		result := fixture.app.processTx(parsed, 2, blockTime)
		require.Zero(t, result.Code, result.Log)
	})
}

func TestAppV20NonGovernanceProofKeepsFutureTimestampCompatibility(t *testing.T) {
	app := setupTestApp(t)
	app.appV20AppliedHeight = 1
	agent := newAgentKey(t)
	outer := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	proofTime := blockTime.Add(30 * time.Minute)
	request := []byte("POST /v1/domain/register\n{\"name\":\"app-v20-idle-compatible\"}")
	parsed := makeDelegatedDomainRegisterTx(
		t, agent, outer, request, proofTime, "app-v20-idle-compatible", "", true,
	)

	result := app.processTx(parsed, 2, blockTime)
	require.Zero(t, result.Code, result.Log)
}

func TestAppV20ScopeVoteAndCancelAcceptValidatorLocalNonAdminOperator(t *testing.T) {
	t.Run("vote", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		localOperator := newAgentKey(t)
		registerAgent(t, fixture.app, localOperator, "validator-local-operator", "member")
		proposalID := createActiveScopeProposal(t, fixture, 2)
		blockTime := time.Now().Truncate(time.Second)
		body := governanceJSON(t, map[string]any{
			"validator_id": fixture.voter.id, "governance_domain": fixture.domain,
			"proposal_id": proposalID, "decision": "accept",
		})
		parsed := &tx.ParsedTx{Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: blockTime, GovVote: &tx.GovVote{
			ProposalID: proposalID, Decision: tx.VoteDecisionAccept,
		}}
		attachGovernanceRequestProof(t, parsed, localOperator, fixture.voter, "POST", "/v1/governance/vote", body, blockTime, []byte("nonce013"))

		result := fixture.app.processTx(parsed, 3, blockTime)
		require.Zero(t, result.Code, result.Log)
	})

	t.Run("cancel", func(t *testing.T) {
		fixture := setupDelegatedScopeGovFixture(t)
		localOperator := newAgentKey(t)
		registerAgent(t, fixture.app, localOperator, "validator-local-operator", "member")
		proposalID := createActiveScopeProposal(t, fixture, 2)
		blockTime := time.Now().Truncate(time.Second)
		body := governanceJSON(t, map[string]any{
			"validator_id": fixture.proposer.id, "governance_domain": fixture.domain,
			"proposal_id": proposalID,
		})
		parsed := &tx.ParsedTx{Type: tx.TxTypeGovCancel, Nonce: 1, Timestamp: blockTime, GovCancel: &tx.GovCancel{ProposalID: proposalID}}
		attachGovernanceRequestProof(t, parsed, localOperator, fixture.proposer, "POST", "/v1/governance/cancel", body, blockTime, []byte("nonce014"))

		result := fixture.app.processTx(parsed, 3, blockTime)
		require.Zero(t, result.Code, result.Log)
	})
}

func TestAppV20DirectGovernanceVoteStillNeedsNoAgentProof(t *testing.T) {
	fixture := setupDelegatedScopeGovFixture(t)
	proposalID, err := fixture.app.govEngine.Propose(
		fixture.proposer.id, governance.OpUpgrade, "app-v20-test", nil, 0,
		governance.DefaultExpiryBlocks, "direct upgrade vote", 2, nil,
	)
	require.NoError(t, err)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovVote, Nonce: 1, Timestamp: time.Now(),
		GovVote: &tx.GovVote{ProposalID: proposalID, Decision: tx.VoteDecisionAccept},
	}
	require.NoError(t, tx.SignTx(parsed, fixture.voter.priv))
	raw, err := tx.EncodeTx(parsed)
	require.NoError(t, err)

	resp := finalizeScopeBlock(t, fixture.app, 3, time.Now().Truncate(time.Second), raw)
	assert.Zero(t, resp.TxResults[0].Code, resp.TxResults[0].Log)
}

func TestPreAppV20DelegatedGovernanceProofBehaviorRemainsUnchanged(t *testing.T) {
	app, outer := setupGovTestApp(t)
	app.appV17AppliedHeight = 1
	requestSigner := newAgentKey(t)
	blockTime := time.Now().Truncate(time.Second)
	record := scopeTemplate("future-scope", outer.id, "future-domain", 1, scope.StateActive, scope.Member{
		ValidatorID: outer.id, AssignedWeight: 10, JoinedRevision: 1, Active: true,
	})
	payload, err := scope.Encode(record)
	require.NoError(t, err)
	parsed := &tx.ParsedTx{
		Type: tx.TxTypeGovPropose, Nonce: 1, Timestamp: time.Now(),
		GovPropose: &tx.GovPropose{
			Operation: tx.GovOpScopeAction, TargetID: record.ScopeID,
			Reason: "historical inert scope", Payload: payload,
		},
		AgentPubKey:    requestSigner.pub,
		AgentSig:       make([]byte, ed25519.SignatureSize),
		AgentTimestamp: blockTime.Add(24 * time.Hour).Unix(),
	}
	require.NoError(t, tx.SignTx(parsed, outer.priv))

	result := app.processTx(parsed, 2, blockTime)
	assert.Zero(t, result.Code, result.Log)
}
