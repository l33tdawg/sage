package tx

import (
	"crypto/sha256"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testDigest(seed string) []byte {
	sum := sha256.Sum256([]byte(seed))
	return sum[:]
}

func testTaintApplyPayload(scope uint8) TaintApplyPayload {
	payload := TaintApplyPayload{
		Version:           TaintApplyPayloadVersion,
		Action:            TaintActionApply,
		TargetChainID:     "sage-local-1",
		TargetMemoryID:    "memory-1",
		TargetContentHash: testDigest("target"),
		ScopeKind:         scope,
		ReasonCategory:    3,
		EvidenceDigest:    testDigest("evidence"),
	}
	switch scope {
	case TaintScopeRootGeneration:
		payload.PrincipalID = "root-principal"
		payload.GenerationFrom = 2
		payload.GenerationTo = 4
	case TaintScopeCredentialSet:
		payload.Credentials = []string{"credential-a", "credential-b"}
	}
	return payload
}

func TestTaintApplyPayloadRoundTripAndCanonicalStrictness(t *testing.T) {
	for _, scope := range []uint8{
		TaintScopeDirect,
		TaintScopeRootGeneration,
		TaintScopeCredentialSet,
	} {
		t.Run(string(rune('0'+scope)), func(t *testing.T) {
			payload := testTaintApplyPayload(scope)
			encoded, err := EncodeTaintApplyPayload(payload)
			require.NoError(t, err)

			decoded, err := DecodeTaintApplyPayload(encoded)
			require.NoError(t, err)
			assert.Equal(t, payload, *decoded)

			reencoded, err := EncodeTaintApplyPayload(*decoded)
			require.NoError(t, err)
			assert.Equal(t, encoded, reencoded)

			_, err = DecodeTaintApplyPayload(append(encoded, 0))
			require.ErrorContains(t, err, "trailing bytes")
		})
	}
}

func TestTaintPayloadIdentitiesAreDomainSeparated(t *testing.T) {
	applyEncoded, err := EncodeTaintApplyPayload(testTaintApplyPayload(TaintScopeDirect))
	require.NoError(t, err)
	decisionID, err := TaintApplyDecisionID(applyEncoded)
	require.NoError(t, err)
	assert.Len(t, decisionID, 64)

	clearPayload := TaintClearPayload{
		Version:        TaintClearPayloadVersion,
		Action:         TaintActionClear,
		DecisionID:     testDigest("decision"),
		ReasonCategory: 4,
		EvidenceDigest: testDigest("clear-evidence"),
	}
	clearEncoded, err := EncodeTaintClearPayload(clearPayload)
	require.NoError(t, err)
	targetID, err := TaintClearTargetID(clearEncoded)
	require.NoError(t, err)
	assert.Len(t, targetID, 64)
	assert.NotEqual(t, decisionID, targetID)

	decodedClear, err := DecodeTaintClearPayload(clearEncoded)
	require.NoError(t, err)
	assert.Equal(t, clearPayload, *decodedClear)
}

func TestTaintApplyPayloadValidation(t *testing.T) {
	valid := testTaintApplyPayload(TaintScopeDirect)
	tests := []struct {
		name   string
		mutate func(*TaintApplyPayload)
		want   string
	}{
		{"version", func(p *TaintApplyPayload) { p.Version = 2 }, "unsupported apply version"},
		{"action", func(p *TaintApplyPayload) { p.Action = TaintActionClear }, "apply action"},
		{"empty chain", func(p *TaintApplyPayload) { p.TargetChainID = "" }, "target chain ID length"},
		{"long chain", func(p *TaintApplyPayload) { p.TargetChainID = strings.Repeat("c", MaxTaintChainIDBytes+1) }, "target chain ID length"},
		{"empty memory", func(p *TaintApplyPayload) { p.TargetMemoryID = "" }, "target memory ID length"},
		{"long memory", func(p *TaintApplyPayload) { p.TargetMemoryID = strings.Repeat("m", MaxTaintMemoryIDBytes+1) }, "target memory ID length"},
		{"target hash", func(p *TaintApplyPayload) { p.TargetContentHash = []byte("short") }, "target content hash length"},
		{"evidence hash", func(p *TaintApplyPayload) { p.EvidenceDigest = []byte("short") }, "evidence digest length"},
		{"unknown scope", func(p *TaintApplyPayload) { p.ScopeKind = 9 }, "unsupported scope kind"},
		{"direct data", func(p *TaintApplyPayload) { p.PrincipalID = "root" }, "direct-only scope"},
		{"root principal", func(p *TaintApplyPayload) {
			p.ScopeKind = TaintScopeRootGeneration
			p.GenerationFrom, p.GenerationTo = 1, 2
		}, "Root principal ID length"},
		{"root zero", func(p *TaintApplyPayload) {
			p.ScopeKind = TaintScopeRootGeneration
			p.PrincipalID = "root"
			p.GenerationTo = 2
		}, "invalid Root generation range"},
		{"root reverse", func(p *TaintApplyPayload) {
			p.ScopeKind = TaintScopeRootGeneration
			p.PrincipalID = "root"
			p.GenerationFrom, p.GenerationTo = 3, 2
		}, "invalid Root generation range"},
		{"credential empty", func(p *TaintApplyPayload) { p.ScopeKind = TaintScopeCredentialSet }, "credential count"},
		{"credential duplicate", func(p *TaintApplyPayload) {
			p.ScopeKind = TaintScopeCredentialSet
			p.Credentials = []string{"b", "b"}
		}, "strictly raw-byte sorted and unique"},
		{"credential unsorted", func(p *TaintApplyPayload) {
			p.ScopeKind = TaintScopeCredentialSet
			p.Credentials = []string{"b", "a"}
		}, "strictly raw-byte sorted and unique"},
		{"credential too long", func(p *TaintApplyPayload) {
			p.ScopeKind = TaintScopeCredentialSet
			p.Credentials = []string{strings.Repeat("c", MaxTaintCredentialBytes+1)}
		}, "credential 0 length"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := valid
			tc.mutate(&payload)
			_, err := EncodeTaintApplyPayload(payload)
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestTaintClearPayloadValidation(t *testing.T) {
	valid := TaintClearPayload{
		Version:        TaintClearPayloadVersion,
		Action:         TaintActionClear,
		DecisionID:     testDigest("decision"),
		ReasonCategory: 1,
		EvidenceDigest: testDigest("evidence"),
	}
	tests := []struct {
		name   string
		mutate func(*TaintClearPayload)
		want   string
	}{
		{"version", func(p *TaintClearPayload) { p.Version = 2 }, "unsupported clear version"},
		{"action", func(p *TaintClearPayload) { p.Action = TaintActionApply }, "clear action"},
		{"decision", func(p *TaintClearPayload) { p.DecisionID = []byte("short") }, "decision ID length"},
		{"evidence", func(p *TaintClearPayload) { p.EvidenceDigest = []byte("short") }, "evidence digest length"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			payload := valid
			tc.mutate(&payload)
			_, err := EncodeTaintClearPayload(payload)
			require.ErrorContains(t, err, tc.want)
		})
	}

	encoded, err := EncodeTaintClearPayload(valid)
	require.NoError(t, err)
	bogus := append([]byte(nil), encoded...)
	bogus[0] = 2
	_, err = DecodeTaintClearPayload(bogus)
	require.ErrorContains(t, err, "unsupported clear version")

	_, err = DecodeTaintClearPayload(encoded[:len(encoded)-1])
	require.ErrorContains(t, err, "invalid encoded size")
}

func TestTaintCredentialSetBound(t *testing.T) {
	payload := testTaintApplyPayload(TaintScopeCredentialSet)
	payload.Credentials = make([]string, MaxTaintCredentialSetEntries)
	for i := range payload.Credentials {
		payload.Credentials[i] = string(rune('a' + i))
	}
	encoded, err := EncodeTaintApplyPayload(payload)
	require.NoError(t, err)
	decoded, err := DecodeTaintApplyPayload(encoded)
	require.NoError(t, err)
	assert.Equal(t, payload.Credentials, decoded.Credentials)
}
