package scope

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func boolPointer(value bool) *bool { return &value }

func TestEncodeProposalTemplateCanonicalizesGuidedInput(t *testing.T) {
	template := ProposalTemplate{
		ScopeID:               "scope-research",
		Revision:              1,
		State:                 "active",
		ControllerValidatorID: "validator-a",
		Domains:               []string{"research.private", "research"},
		Members: []ProposalMember{
			{ValidatorID: "validator-b", AssignedWeight: 2},
			{ValidatorID: "validator-a", AssignedWeight: 3},
		},
	}

	encoded, err := EncodeProposalTemplate(template)
	require.NoError(t, err)
	record, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, []Domain{{Name: "research"}, {Name: "research.private"}}, record.Domains)
	assert.Equal(t, "validator-a", record.Members[0].ValidatorID)
	assert.Equal(t, uint64(1), record.Members[0].JoinedRevision)
	assert.True(t, record.Members[0].Active)
	assert.Equal(t, int64(0), record.CreatedHeight)
	assert.Equal(t, int64(0), record.UpdatedHeight)
	assert.Equal(t, "research.private", template.Domains[0], "builder must not mutate caller input")
}

func TestEncodeProposalTemplatePreservesExplicitInactiveMember(t *testing.T) {
	template := ProposalTemplate{
		ScopeID: "scope-research", Revision: 2, State: "paused",
		ControllerValidatorID: "validator-a", Domains: []string{"research"},
		Members: []ProposalMember{
			{ValidatorID: "validator-a", AssignedWeight: 3, JoinedRevision: 1},
			{ValidatorID: "validator-b", AssignedWeight: 2, JoinedRevision: 2, Active: boolPointer(false)},
		},
	}
	encoded, err := EncodeProposalTemplate(template)
	require.NoError(t, err)
	record, err := Decode(encoded)
	require.NoError(t, err)
	assert.Equal(t, StatePaused, record.State)
	assert.False(t, record.Members[1].Active)
}

func TestEncodeProposalTemplateFailsClosed(t *testing.T) {
	base := ProposalTemplate{
		ScopeID: "scope-research", Revision: 1, State: "active",
		ControllerValidatorID: "validator-a", Domains: []string{"research"},
		Members: []ProposalMember{{ValidatorID: "validator-a", AssignedWeight: 1}},
	}

	tests := []struct {
		name   string
		mutate func(*ProposalTemplate)
	}{
		{"unknown state", func(v *ProposalTemplate) { v.State = "forming" }},
		{"duplicate domain", func(v *ProposalTemplate) { v.Domains = append(v.Domains, "research") }},
		{"duplicate member", func(v *ProposalTemplate) { v.Members = append(v.Members, v.Members[0]) }},
		{"zero weight", func(v *ProposalTemplate) { v.Members[0].AssignedWeight = 0 }},
		{"inactive controller", func(v *ProposalTemplate) { v.Members[0].Active = boolPointer(false) }},
		{"missing historical join revision", func(v *ProposalTemplate) { v.Revision = 2 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := base
			input.Domains = append([]string(nil), base.Domains...)
			input.Members = append([]ProposalMember(nil), base.Members...)
			test.mutate(&input)
			_, err := EncodeProposalTemplate(input)
			assert.Error(t, err)
		})
	}
}
