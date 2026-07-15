package rest

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/tx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseGovOp_MemoryDomainRepair guards that the app-v16 domain-repair op is
// reachable through the REST governance-propose surface. The final fresh-eyes
// review found the op fully implemented in consensus but unmappable end-to-end:
// parseGovOp 400'd "memory_domain_repair", so no client could ever create the
// proposal and the headline v11.2 remediation was unreachable. This pins the
// mapping (and that it numerically matches governance.OpMemoryDomainRepair = 6).
func TestParseGovOp_MemoryDomainRepair(t *testing.T) {
	op, err := parseGovOp("memory_domain_repair")
	require.NoError(t, err)
	assert.Equal(t, tx.GovOpMemoryDomainRepair, op)
	assert.Equal(t, uint8(6), uint8(op), "must match governance.OpMemoryDomainRepair = 6")
}

func TestResolveGovProposalPayloadBuildsCanonicalScope(t *testing.T) {
	template := &scope.ProposalTemplate{
		ScopeID: "scope-a", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a",
		Domains:               []string{"research.private", "research"},
		Members: []scope.ProposalMember{
			{ValidatorID: "validator-b", AssignedWeight: 1},
			{ValidatorID: "validator-a", AssignedWeight: 2},
		},
	}
	targetID, payload, err := resolveGovProposalPayload(tx.GovOpScopeAction, "", "", template)
	require.NoError(t, err)
	assert.Equal(t, "scope-a", targetID)
	record, err := scope.Decode(payload)
	require.NoError(t, err)
	assert.Equal(t, []scope.Domain{{Name: "research"}, {Name: "research.private"}}, record.Domains)
	assert.Zero(t, record.CreatedHeight)
	assert.Zero(t, record.UpdatedHeight)
}

func TestResolveGovProposalPayloadRejectsAmbiguousOrMismatchedScope(t *testing.T) {
	template := &scope.ProposalTemplate{
		ScopeID: "scope-a", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a", Domains: []string{"research"},
		Members: []scope.ProposalMember{{ValidatorID: "validator-a", AssignedWeight: 1}},
	}
	_, _, err := resolveGovProposalPayload(tx.GovOpScopeAction, "scope-a", base64.StdEncoding.EncodeToString([]byte("raw")), template)
	assert.ErrorContains(t, err, "mutually exclusive")
	_, _, err = resolveGovProposalPayload(tx.GovOpScopeAction, "scope-b", "", template)
	assert.ErrorContains(t, err, "does not match")
	_, _, err = resolveGovProposalPayload(tx.GovOpAddValidator, "validator-a", "", template)
	assert.ErrorContains(t, err, "only valid")
	_, _, err = resolveGovProposalPayload(tx.GovOpScopeAction, "scope-a", "", nil)
	assert.ErrorContains(t, err, "requires either")
}

func TestScopeActionConstructionFailsClosedBeforeAppV20(t *testing.T) {
	server := &Server{}
	req := httptest.NewRequest(http.MethodPost, "/v1/governance/propose", strings.NewReader(`{
		"operation":"scope_action","target_id":"scope-a","reason":"form scope","payload":"AQ=="
	}`))
	resp := httptest.NewRecorder()
	server.handleGovPropose(resp, req)
	assert.Equal(t, http.StatusConflict, resp.Code)
	assert.Contains(t, resp.Body.String(), "app-v20")
}

// TestParseGovOp_KnownAndUnknown pins the rest of the mapping so the repair addition
// didn't disturb the legacy ops, and an unknown op still errors.
func TestParseGovOp_KnownAndUnknown(t *testing.T) {
	for s, want := range map[string]tx.GovProposalOp{
		"add_validator":     tx.GovOpAddValidator,
		"remove_validator":  tx.GovOpRemoveValidator,
		"update_power":      tx.GovOpUpdatePower,
		"domain_reassign":   tx.GovOpDomainReassign,
		"sync_group_action": tx.GovOpSyncGroupAction,
		"scope_action":      tx.GovOpScopeAction,
	} {
		got, err := parseGovOp(s)
		require.NoError(t, err, s)
		assert.Equal(t, want, got, s)
	}
	_, err := parseGovOp("bogus_op")
	assert.Error(t, err, "unknown op must be rejected")
}
