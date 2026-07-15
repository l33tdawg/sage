package web

import (
	"encoding/base64"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestResolveDashboardGovProposalPayloadBuildsCanonicalScope(t *testing.T) {
	template := &scope.ProposalTemplate{
		ScopeID: "scope-a", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a",
		Domains:               []string{"research.private", "research"},
		Members: []scope.ProposalMember{
			{ValidatorID: "validator-b", AssignedWeight: 1},
			{ValidatorID: "validator-a", AssignedWeight: 2},
		},
	}
	targetID, payload, err := resolveDashboardGovProposalPayload(tx.GovOpScopeAction, "", "", template)
	require.NoError(t, err)
	assert.Equal(t, "scope-a", targetID)
	record, err := scope.Decode(payload)
	require.NoError(t, err)
	assert.Equal(t, []scope.Domain{{Name: "research"}, {Name: "research.private"}}, record.Domains)
}

func TestResolveDashboardGovProposalPayloadRejectsAmbiguity(t *testing.T) {
	template := &scope.ProposalTemplate{
		ScopeID: "scope-a", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a", Domains: []string{"research"},
		Members: []scope.ProposalMember{{ValidatorID: "validator-a", AssignedWeight: 1}},
	}
	_, _, err := resolveDashboardGovProposalPayload(tx.GovOpScopeAction, "scope-a", base64.StdEncoding.EncodeToString([]byte("raw")), template)
	assert.ErrorContains(t, err, "mutually exclusive")
	_, _, err = resolveDashboardGovProposalPayload(tx.GovOpScopeAction, "scope-b", "", template)
	assert.ErrorContains(t, err, "does not match")
}
