package web

import "testing"

// The dashboard task handler had the same unconditional-prefix bug as the MCP
// path: a task typed as "[TASK] ..." in CEREBRUM was stored "[TASK] [TASK] ...".
func TestApplyTaskPrefixIdempotent(t *testing.T) {
	tests := []struct{ name, content, want string }{
		{"bare content gets the prefix", "Ship the exporter", "[TASK] Ship the exporter"},
		{"already prefixed is left alone", "[TASK] Ship the exporter", "[TASK] Ship the exporter"},
		{"only the leading marker counts", "Fix the [TASK] label", "[TASK] Fix the [TASK] label"},
		{"empty content still gets the prefix", "", "[TASK] "},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := applyTaskPrefix(tt.content); got != tt.want {
				t.Errorf("applyTaskPrefix(%q) = %q, want %q", tt.content, got, tt.want)
			}
		})
	}
	for _, c := range []string{"Ship it", "[TASK] Ship it", ""} {
		if once, twice := applyTaskPrefix(c), applyTaskPrefix(applyTaskPrefix(c)); once != twice {
			t.Errorf("not idempotent for %q: once=%q twice=%q", c, once, twice)
		}
	}
}
