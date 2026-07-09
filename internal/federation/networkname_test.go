package federation

import "testing"

// TestSanitizeName pins the defanging of a friendly network label (applied to
// both operator input and untrusted inbound peer names): control chars and
// newlines stripped, internal whitespace collapsed, trimmed, length capped.
func TestSanitizeName(t *testing.T) {
	cases := []struct{ in, want string }{
		{"", ""},
		{"   ", ""},
		{"Dhillon's Mac", "Dhillon's Mac"},
		{"  trim me  ", "trim me"},
		{"line1\nline2", "line1 line2"},    // newline -> single space (no log/row forgery)
		{"tabs\there", "tabs here"},        // tab -> space
		{"a\x00b\x07c", "abc"},             // NUL + BEL dropped
		{"multi     space", "multi space"}, // internal runs collapse
		{"trailing\n", "trailing"},         // trailing newline stripped
	}
	for _, c := range cases {
		if got := sanitizeName(c.in); got != c.want {
			t.Errorf("sanitizeName(%q) = %q, want %q", c.in, got, c.want)
		}
	}

	// Length cap: an oversized hostile label is truncated to maxNetworkNameLen runes.
	long := ""
	for i := 0; i < 200; i++ {
		long += "x"
	}
	got := sanitizeName(long)
	if len([]rune(got)) > maxNetworkNameLen {
		t.Errorf("sanitizeName did not cap length: got %d runes (cap %d)", len([]rune(got)), maxNetworkNameLen)
	}

	// Exported wrapper matches the internal one.
	if SanitizeNetworkName("  hi\nthere  ") != sanitizeName("  hi\nthere  ") {
		t.Error("SanitizeNetworkName must delegate to sanitizeName")
	}
}
