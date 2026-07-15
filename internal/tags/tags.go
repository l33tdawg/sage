// Package tags defines the canonical representation of user-authored memory
// tags shared by REST, federation, the transaction codec, and app-v20 scoped
// consensus state.
package tags

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	MaxCount = 32
	MaxBytes = 128
)

// Normalize validates, sorts, and deduplicates tags. Empty input is represented
// as nil so there is one canonical in-memory value and one wire encoding.
func Normalize(values []string) ([]string, error) {
	if len(values) == 0 {
		return nil, nil
	}
	if len(values) > MaxCount {
		return nil, fmt.Errorf("tags exceed %d entries", MaxCount)
	}
	canonical := append([]string(nil), values...)
	for _, value := range canonical {
		if value == "" || value != strings.TrimSpace(value) {
			return nil, errors.New("tag is empty or padded")
		}
		if !utf8.ValidString(value) {
			return nil, errors.New("tag is not valid UTF-8")
		}
		if len(value) > MaxBytes {
			return nil, fmt.Errorf("tag exceeds %d bytes", MaxBytes)
		}
	}
	sort.Strings(canonical)
	out := canonical[:0]
	for _, value := range canonical {
		if len(out) == 0 || out[len(out)-1] != value {
			out = append(out, value)
		}
	}
	return out, nil
}

// ValidateCanonical rejects any representation Normalize would change.
func ValidateCanonical(values []string) error {
	canonical, err := Normalize(values)
	if err != nil {
		return err
	}
	if len(canonical) != len(values) {
		return errors.New("tags contain duplicates")
	}
	for i := range canonical {
		if canonical[i] != values[i] {
			return errors.New("tags are not in canonical byte order")
		}
	}
	return nil
}
