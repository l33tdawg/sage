package scope

import (
	"errors"
	"fmt"
	"sort"
)

// ProposalTemplate is the human-facing form of a ScopeRecordV1 governance
// payload. Consensus-owned heights are deliberately absent. Domains and
// members may arrive in any order; EncodeProposalTemplate makes their order
// canonical before invoking the strict wire codec.
type ProposalTemplate struct {
	ScopeID               string           `json:"scope_id"`
	Revision              uint64           `json:"revision"`
	State                 string           `json:"state"`
	ControllerValidatorID string           `json:"controller_validator_id"`
	Domains               []string         `json:"domains"`
	Members               []ProposalMember `json:"members"`
}

// ProposalMember is one guided roster entry. Active defaults to true when
// omitted. JoinedRevision may be omitted only when creating revision 1; later
// revisions must state the historical join revision explicitly so a client
// cannot accidentally rewrite roster history.
type ProposalMember struct {
	ValidatorID    string `json:"validator_id"`
	AssignedWeight uint64 `json:"assigned_weight"`
	JoinedRevision uint64 `json:"joined_revision,omitempty"`
	Active         *bool  `json:"active,omitempty"`
}

// EncodeProposalTemplate converts a guided JSON template into the canonical
// binary payload consumed by app-v20 governance. It never mutates the input.
func EncodeProposalTemplate(template ProposalTemplate) ([]byte, error) {
	state, err := parseProposalState(template.State)
	if err != nil {
		return nil, err
	}
	if template.Revision == 0 {
		return nil, errors.New("revision must be non-zero")
	}

	domains := make([]Domain, len(template.Domains))
	for i, name := range template.Domains {
		domains[i] = Domain{Name: name}
	}
	sort.Slice(domains, func(i, j int) bool { return domains[i].Name < domains[j].Name })

	members := make([]Member, len(template.Members))
	for i, input := range template.Members {
		joinedRevision := input.JoinedRevision
		if joinedRevision == 0 {
			if template.Revision != 1 {
				return nil, fmt.Errorf("member %q must specify joined_revision for revision %d", input.ValidatorID, template.Revision)
			}
			joinedRevision = 1
		}
		active := true
		if input.Active != nil {
			active = *input.Active
		}
		members[i] = Member{
			ValidatorID:    input.ValidatorID,
			AssignedWeight: input.AssignedWeight,
			JoinedRevision: joinedRevision,
			Active:         active,
		}
	}
	sort.Slice(members, func(i, j int) bool { return members[i].ValidatorID < members[j].ValidatorID })

	return Encode(Record{
		ScopeID:               template.ScopeID,
		Revision:              template.Revision,
		State:                 state,
		ControllerValidatorID: template.ControllerValidatorID,
		CreatedHeight:         0,
		UpdatedHeight:         0,
		Domains:               domains,
		Members:               members,
	})
}

func parseProposalState(value string) (State, error) {
	switch value {
	case "active":
		return StateActive, nil
	case "paused":
		return StatePaused, nil
	case "retired":
		return StateRetired, nil
	default:
		return 0, fmt.Errorf("state must be active, paused, or retired")
	}
}
