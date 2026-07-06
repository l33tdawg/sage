package rest

import (
	"testing"

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

// TestParseGovOp_KnownAndUnknown pins the rest of the mapping so the repair addition
// didn't disturb the legacy ops, and an unknown op still errors.
func TestParseGovOp_KnownAndUnknown(t *testing.T) {
	for s, want := range map[string]tx.GovProposalOp{
		"add_validator":    tx.GovOpAddValidator,
		"remove_validator": tx.GovOpRemoveValidator,
		"update_power":     tx.GovOpUpdatePower,
		"domain_reassign":  tx.GovOpDomainReassign,
	} {
		got, err := parseGovOp(s)
		require.NoError(t, err, s)
		assert.Equal(t, want, got, s)
	}
	_, err := parseGovOp("bogus_op")
	assert.Error(t, err, "unknown op must be rejected")
}
