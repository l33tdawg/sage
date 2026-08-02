package authzdenial

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestClassifyEffectiveWriteDenialMatrix(t *testing.T) {
	tests := []struct {
		name string
		log  string
		code Code
	}{
		{"legacy missing level 2", "access denied: agent 0123456789abcdef has no write access to domain secret", CodeMissingWriteGrant},
		{"legacy preflight missing level 2", "agent does not have write access to domain 'secret'", CodeMissingWriteGrant},
		{"legacy deny foreign bit 8", "access denied: agent 0123456789abcdef cannot write domain secret it does not own", CodeForeignWriteRestricted},
		{"legacy deny shared bit 2", "access denied: agent 0123456789abcdef cannot write shared domain general", CodeSharedWriteRestricted},
		{"legacy deny claim bit 4", "access denied: agent 0123456789abcdef cannot claim unowned domain new-home", CodeDomainClaimRestricted},
		{"structured pending enrollment", "access denied: denial_code=principal_pending_review", CodePrincipalPendingReview},
		{"structured missing home", "access denied: denial_code=no_owned_home_domain", CodeNoOwnedHomeDomain},
		{"structured future marker", "tx rejected in FinalizeBlock (code 11): access denied: denial_code=manager_scope_denied target=private", CodeManagerScopeDenied},
		{"structured CheckTx marker", "tx rejected in CheckTx (code 110): access denied: denial_code=shared_write_restricted", CodeSharedWriteRestricted},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			denial, ok := ClassifyText(test.log)
			require.True(t, ok)
			assert.Equal(t, test.code, denial.Code)
			assert.NotEmpty(t, denial.Remedy)
			assert.False(t, denial.Retryable)
		})
	}
}

func TestClassifyDoesNotGuessFromGenericOrNonzeroCapabilityMask(t *testing.T) {
	for _, message := range []string{
		"access denied",
		"agent is not registered",
		"capability mask is 15",
		"capability mask is non-zero",
		"unknown agent capability bits: 0x80",
		"denial_code=attacker_invented",
		"access denied: target domain named x-denial_code=manager_scope_denied",
		"access denied: denial_code=control_plane_denied",
		"access denied: denial_code=future_code cannot write shared domain secret",
		"diagnostic says agent 0123456789abcdef cannot write shared domain secret",
		"access denied: agent not-a-hex-agent cannot write shared domain secret",
	} {
		t.Run(fmt.Sprintf("%q", message), func(t *testing.T) {
			_, ok := ClassifyText(message)
			assert.False(t, ok)
		})
	}
}

func TestClassifyLegacyGrammarCannotBeForgedByDomain(t *testing.T) {
	tests := []struct {
		message string
		code    Code
	}{
		{
			"agent does not have write access to domain 'x cannot write shared domain general'",
			CodeMissingWriteGrant,
		},
		{
			"access denied: agent 0123456789abcdef has no write access to domain x cannot claim unowned domain y",
			CodeMissingWriteGrant,
		},
		{
			"access denied: agent 0123456789abcdef cannot write shared domain x it does not own",
			CodeSharedWriteRestricted,
		},
	}
	for _, test := range tests {
		denial, ok := ClassifyText(test.message)
		require.True(t, ok)
		assert.Equal(t, test.code, denial.Code)
	}
}

func TestMissingWriteGrantRemedyNamesOnlyCurrentAppV26Actions(t *testing.T) {
	denial, ok := Definition(CodeMissingWriteGrant)
	require.True(t, ok)
	assert.Contains(t, denial.Remedy, "domain this agent owns")
	assert.Contains(t, denial.Remedy, "Access Group")
	assert.Contains(t, denial.Remedy, "Read + write")
	assert.Contains(t, denial.Remedy, "Read + write + modify")
	assert.Contains(t, denial.Remedy, "no direct level-2 grant editor")
	assert.NotContains(t, denial.Remedy, "Manager")
	assert.NotContains(t, denial.Remedy, "Grant this agent level 2")
}

func TestManagerScopeRemedyNamesOnlyCurrentAppV26Actions(t *testing.T) {
	denial, ok := Definition(CodeManagerScopeDenied)
	require.True(t, ok)
	assert.Contains(t, denial.Remedy, "domain this manager owns")
	assert.Contains(t, denial.Remedy, "Access Group")
	assert.Contains(t, denial.Remedy, "Read + write")
	assert.Contains(t, denial.Remedy, "Read + write + modify")
	assert.Contains(t, denial.Remedy, "Manager role alone does not widen group authority")
	assert.NotContains(t, denial.Remedy, "level 2")
}

func TestValidateProblemRequiresCompleteCanonicalContract(t *testing.T) {
	retryFalse := false
	retryTrue := true
	denial, ok := ValidateProblem(ProblemTypeURI, CodeMissingWriteGrant, &retryFalse)
	require.True(t, ok)
	assert.Equal(t, CodeMissingWriteGrant, denial.Code)

	for _, test := range []struct {
		problemType string
		code        Code
		retryable   *bool
	}{
		{"https://sage.dev/errors/403", CodeMissingWriteGrant, &retryFalse},
		{ProblemTypeURI, "unknown_write_denial", &retryFalse},
		{ProblemTypeURI, CodeMissingWriteGrant, nil},
		{ProblemTypeURI, CodeMissingWriteGrant, &retryTrue},
	} {
		_, valid := ValidateProblem(test.problemType, test.code, test.retryable)
		assert.False(t, valid)
	}
}

func TestDefinitionsAreCompleteAndSanitized(t *testing.T) {
	codes := KnownCodes()
	require.Equal(t, []Code{
		CodeMissingWriteGrant,
		CodeForeignWriteRestricted,
		CodeSharedWriteRestricted,
		CodeDomainClaimRestricted,
		CodePrincipalPendingReview,
		CodeNoOwnedHomeDomain,
		CodeManagerScopeDenied,
	}, codes)
	seen := make(map[Code]bool, len(codes))
	for _, code := range codes {
		require.False(t, seen[code], "duplicate code %q", code)
		seen[code] = true
		denial, ok := Definition(code)
		require.True(t, ok)
		assert.Equal(t, code, denial.Code)
		assert.NotEmpty(t, denial.Remedy)
		assert.NotContains(t, denial.Remedy, "0123")
		assert.NotContains(t, denial.Remedy, "secret")
		assert.False(t, denial.Retryable)
	}
}
