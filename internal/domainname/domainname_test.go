package domainname

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestValidateLexicalContract(t *testing.T) {
	for _, name := range []string{
		"technical/hardware", "research.eurorack", "voice_interface", "研发.笔记",
		"/", "technical//hardware", "a\u200bb", strings.Repeat("a", MaxBytes), strings.Repeat("é", MaxBytes/2),
	} {
		t.Run("valid_"+name, func(t *testing.T) { require.NoError(t, Validate(name)) })
	}
	for _, tc := range []struct{ name, value, message string }{
		{"empty", "", "domain name is empty"},
		{"too long", strings.Repeat("a", MaxBytes+1), "domain name exceeds 512 bytes"},
		{"UTF8 byte length", strings.Repeat("é", MaxBytes/2+1), "domain name exceeds 512 bytes"},
		{"invalid UTF8", string([]byte{0xff}), "domain name is not valid UTF-8"},
		{"leading dot", ".leading", "domain name has an empty dotted segment"},
		{"trailing dot", "trailing.", "domain name has an empty dotted segment"},
		{"empty dotted", "double..segment", "domain name has an empty dotted segment"},
		{"colon", "grant:alias", "domain name contains reserved delimiter ':'"},
		{"space", "has space", "domain name contains whitespace"},
		{"tab", "has\ttab", "domain name contains whitespace"},
		{"newline", "has\nnewline", "domain name contains whitespace"},
		{"Unicode space", "has\u00a0space", "domain name contains whitespace"},
		{"control", "has\x00control", "domain name contains a control character"},
		{"DEL", "has\x7fcontrol", "domain name contains a control character"},
		{"length before UTF8", string([]byte{0xff}) + strings.Repeat("a", MaxBytes), "domain name exceeds 512 bytes"},
		{"dotted before colon", ".domain:alias", "domain name has an empty dotted segment"},
		{"first rune error", "space :colon", "domain name contains whitespace"},
	} {
		t.Run(tc.name, func(t *testing.T) { require.EqualError(t, Validate(tc.value), tc.message) })
	}
}
