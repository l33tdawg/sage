package contentvalidator

import (
	"testing"
	"time"

	"github.com/l33tdawg/sage/internal/memory"
	"github.com/stretchr/testify/require"
)

func TestFunctionRegistrationRejectsMissingLegacyAtBoot(t *testing.T) {
	for _, explicitNil := range []bool{false, true} {
		r := NewContentValidatorRegistry()
		r.RegisterClosedDomain("example-records")
		if explicitNil {
			r.RegisterContentValidator("example-records", "example", nil)
		}
		require.PanicsWithValue(t, "content validator: function registration requires a non-nil legacy replay validator", func() {
			r.RegisterFunctionContentValidator("example-records", "example", "example-producer", func(*memory.MemoryRecord, ValidationContext) error { return nil })
		})
		require.Empty(t, r.FunctionFor("example-records", "example"), "failed boot registration must not leave dispatch metadata")
	}
}

func TestFunctionRegistrationKeepsClosedDomainRouting(t *testing.T) {
	r := NewContentValidatorRegistry()
	r.RegisterClosedDomain("example-records")
	r.RegisterContentValidator("example-records", "example", func(*memory.MemoryRecord) error { return nil })
	r.RegisterFunctionContentValidator("example-records", "example", "example-producer", func(*memory.MemoryRecord, ValidationContext) error { return nil })
	ctx := ValidationContext{Height: 1, BlockTime: time.Unix(1, 0), AuthorizeFunction: func(FunctionAuthorityRequest) (FunctionAuthorityResult, error) { return FunctionAuthorityResult{}, nil }}
	for _, class := range []string{"", "unknown"} {
		rejected, reason := r.ValidateWithContext("example-records", class, &memory.MemoryRecord{}, ctx)
		require.True(t, rejected)
		require.Contains(t, reason, "unrecognized outcome_class")
	}
}
