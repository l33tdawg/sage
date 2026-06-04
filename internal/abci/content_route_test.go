package abci

import "testing"

// TestParseOutcomeClass pins the fail-SAFE routing contract: a malformed SIBLING
// envelope field (e.g. a float/string/overflowing schema_version) must NOT be
// able to null the route, and a single-element array body must route by its real
// class. Before the hardening these all returned "" — which, on a closed domain,
// is exactly the unvalidated-commit bypass the gate is meant to stop.
func TestParseOutcomeClass(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		// Baseline: a clean envelope routes by its class.
		{"plain object", `{"schema_version":1,"outcome_class":"action"}`, "action"},

		// Type-confused sibling: schema_version as an integral float, a fractional
		// float, a string, or an overflowing integer must NOT abort routing.
		{"float sibling", `{"schema_version":1.0,"outcome_class":"action"}`, "action"},
		{"fractional float sibling", `{"schema_version":1.5,"outcome_class":"action"}`, "action"},
		{"string sibling", `{"schema_version":"1","outcome_class":"action"}`, "action"},
		{"overflow int sibling", `{"schema_version":99999999999999999999,"outcome_class":"action"}`, "action"},
		{"object sibling", `{"schema_version":{"x":1},"outcome_class":"action"}`, "action"},

		// Single-element array body: unwrapped, then routed by its real class.
		{"array-wrapped", `[{"schema_version":1,"outcome_class":"action"}]`, "action"},
		{"array-wrapped with float sibling", `[{"schema_version":1.0,"outcome_class":"action"}]`, "action"},
		{"array-wrapped padded", "  [ {\"outcome_class\":\"action\"} ]  ", "action"},

		// Genuinely unroutable bodies still yield "" (the closed-domain reject key).
		{"missing outcome_class", `{"schema_version":1}`, ""},
		{"non-string outcome_class", `{"outcome_class":5}`, ""},
		{"non-json", "this is not json", ""},
		{"empty string", "", ""},
		{"empty array", "[]", ""},
		{"multi-element array not unwrapped", `[{"outcome_class":"a"},{"outcome_class":"b"}]`, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := parseOutcomeClass(tt.content); got != tt.want {
				t.Fatalf("parseOutcomeClass(%q) = %q, want %q", tt.content, got, tt.want)
			}
		})
	}
}

// TestParseOutcomeClassDeterminism: routing is a pure function of the bytes.
// Consensus correctness depends on every validator deriving the same key.
func TestParseOutcomeClassDeterminism(t *testing.T) {
	const body = `[{"schema_version":1.0,"outcome_class":"action"}]`
	if a, b := parseOutcomeClass(body), parseOutcomeClass(body); a != b || a != "action" {
		t.Fatalf("non-deterministic or wrong: %q vs %q", a, b)
	}
}

// TestUnwrapSingleElementJSONArray pins the unwrap helper: exactly the
// single-element-array shape is unwrapped; every other shape is returned
// unchanged so non-array bodies route identically to before.
func TestUnwrapSingleElementJSONArray(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"single element", `[{"a":1}]`, `{"a":1}`},
		{"single element padded", "  [ {\"a\":1} ]  ", `{"a":1}`},
		{"object untouched", `{"a":1}`, `{"a":1}`},
		{"multi element untouched", `[{"a":1},{"b":2}]`, `[{"a":1},{"b":2}]`},
		{"empty array untouched", `[]`, `[]`},
		{"non-json untouched", `nope`, `nope`},
		{"empty untouched", ``, ``},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := unwrapSingleElementJSONArray(tt.in); got != tt.want {
				t.Fatalf("unwrapSingleElementJSONArray(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
