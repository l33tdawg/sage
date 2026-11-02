// Package domainname defines the shared lexical domain contract used by wire
// admission and consensus storage. It has no store or transaction dependency.
package domainname

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxBytes is the existing consensus domain-name byte limit.
const MaxBytes = 512

// Validate rejects ambiguous key delimiters, empty dotted segments, whitespace,
// control characters and invalid UTF-8. Slash and Unicode domains retain their
// existing vocabulary. Validation order and error strings are unchanged.
func Validate(name string) error {
	if name == "" {
		return errors.New("domain name is empty")
	}
	if len(name) > MaxBytes {
		return fmt.Errorf("domain name exceeds %d bytes", MaxBytes)
	}
	if !utf8.ValidString(name) {
		return errors.New("domain name is not valid UTF-8")
	}
	if strings.HasPrefix(name, ".") || strings.HasSuffix(name, ".") ||
		strings.Contains(name, "..") {
		return errors.New("domain name has an empty dotted segment")
	}
	for _, r := range name {
		switch {
		case r == ':':
			return errors.New("domain name contains reserved delimiter ':'")
		case unicode.IsSpace(r):
			return errors.New("domain name contains whitespace")
		case unicode.IsControl(r):
			return errors.New("domain name contains a control character")
		}
	}
	return nil
}
