package abci

import (
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/contentvalidator"
)

// TestContentValidationEnforcementWarning pins the non-fatal advisory: it returns
// a warning ONLY when the app-v7 fork is active but no registry is compiled in
// (this node won't enforce) — and stays empty (bootable, no noise) for every
// other shape, including the default node and a correctly-wired enforcing node.
func TestContentValidationEnforcementWarning(t *testing.T) {
	reg := contentvalidator.NewContentValidatorRegistry()

	cases := []struct {
		name        string
		validators  *contentvalidator.ContentValidatorRegistry
		appV7Height int64
		wantWarn    bool
	}{
		{"default: no fork, no registry", nil, 0, false},
		// The flagged case: fork active but this node has no registry → won't
		// enforce. Must warn, but (tested elsewhere) must NOT block boot.
		{"app-v7 active, no registry warns", nil, 100, true},
		{"app-v7 active, registry wired is silent", reg, 100, false},
		{"no fork, registry wired is silent", reg, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			app := &SageApp{}
			app.appV7AppliedHeight = tc.appV7Height
			app.SetContentValidators(tc.validators)

			warn := app.ContentValidationEnforcementWarning()
			if tc.wantWarn {
				if warn == "" {
					t.Fatalf("want a warning, got empty")
				}
				if !strings.Contains(warn, "app-v7") || !strings.Contains(warn, "diverge") {
					t.Fatalf("warning missing key context: %q", warn)
				}
			} else if warn != "" {
				t.Fatalf("want no warning, got %q", warn)
			}
		})
	}
}
