package tx

import (
	"encoding/hex"
	"errors"
	"math"
	"strings"
	"unicode/utf8"
)

// TxTypeFunctionAuthority is dormant until the app-v31 consensus boundary.
const TxTypeFunctionAuthority TxType = 41

// FunctionAuthority is a Root-controlled policy activation or scoped functional
// designation. It does not change an ordinary role, profile or domain grant.
type FunctionAuthority struct {
	Action           string `json:"action"`
	Function         string `json:"function"`
	Domain           string `json:"domain"`
	Scope            string `json:"scope"`
	Subject          string `json:"subject"`
	ActivationHeight int64  `json:"activation_height"`
	ExpectedRevision uint64 `json:"expected_revision"`
	ExpiresAt        int64  `json:"expires_at"`
}

// ValidateFunctionAuthority checks the closed wire structure. Canonical domain
// policy, activation timing, Root authority and designation state are checked
// independently by consensus against its executing store.
func ValidateFunctionAuthority(a *FunctionAuthority) error {
	if a == nil {
		return errors.New("function authority is nil")
	}
	if len(a.Function) == 0 || len(a.Function) > 64 || a.Function[0] < 'a' || a.Function[0] > 'z' {
		return errors.New("invalid function authority function")
	}
	for i := 1; i < len(a.Function); i++ {
		c := a.Function[i]
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '-' && c != '_' {
			return errors.New("invalid function authority function")
		}
	}
	if len(a.Domain) == 0 || len(a.Domain) > 512 || !utf8.ValidString(a.Domain) {
		return errors.New("invalid function authority domain")
	}
	if len(a.Scope) == 0 || len(a.Scope) > 512 || !utf8.ValidString(a.Scope) {
		return errors.New("invalid function authority scope")
	}
	if a.ExpectedRevision == math.MaxUint64 || a.ExpiresAt < 0 {
		return errors.New("invalid function authority revision or expiry")
	}
	switch a.Action {
	case "activate":
		if a.Subject != "" || a.ActivationHeight <= 0 || a.ExpectedRevision != 0 || a.ExpiresAt != 0 {
			return errors.New("invalid function authority activation fields")
		}
	case "assign", "revoke":
		decoded, err := hex.DecodeString(a.Subject)
		if err != nil || len(decoded) != 32 || a.Subject != strings.ToLower(a.Subject) || a.ActivationHeight != 0 {
			return errors.New("invalid function authority designation fields")
		}
		if a.Action == "revoke" && a.ExpiresAt != 0 {
			return errors.New("function authority revocation cannot carry expiry")
		}
	default:
		return errors.New("invalid function authority action")
	}
	return nil
}

func encodeFunctionAuthority(a *FunctionAuthority) ([]byte, error) {
	if err := ValidateFunctionAuthority(a); err != nil {
		return nil, err
	}
	buf := appendBytes(nil, []byte(a.Action))
	buf = appendBytes(buf, []byte(a.Function))
	buf = appendBytes(buf, []byte(a.Domain))
	buf = appendBytes(buf, []byte(a.Scope))
	buf = appendBytes(buf, []byte(a.Subject))
	buf = appendInt64(buf, a.ActivationHeight)
	buf = appendUint64(buf, a.ExpectedRevision)
	return appendInt64(buf, a.ExpiresAt), nil
}

// FunctionAuthorityActionBytes binds every semantic field to the agent proof.
// Invalid structures have no signing representation.
func FunctionAuthorityActionBytes(a *FunctionAuthority) []byte {
	encoded, err := encodeFunctionAuthority(a)
	if err != nil {
		return nil
	}
	return encoded
}

func decodeFunctionAuthority(data []byte) (*FunctionAuthority, error) {
	// Five length prefixes, at most 8+64+512+512+64 bytes, and three uint64s.
	if len(data) > 5*4+8+64+512+512+64+3*8 {
		return nil, ErrInvalidTxData
	}
	a := &FunctionAuthority{}
	off := 0
	for _, target := range []*string{&a.Action, &a.Function, &a.Domain, &a.Scope, &a.Subject} {
		b, next, err := readBytes(data, off)
		if err != nil {
			return nil, ErrInvalidTxData
		}
		*target, off = string(b), next
	}
	var err error
	a.ActivationHeight, off, err = readInt64(data, off)
	if err != nil {
		return nil, ErrInvalidTxData
	}
	a.ExpectedRevision, off, err = readUint64(data, off)
	if err != nil {
		return nil, ErrInvalidTxData
	}
	a.ExpiresAt, off, err = readInt64(data, off)
	if err != nil || off != len(data) {
		return nil, ErrInvalidTxData
	}
	if err := ValidateFunctionAuthority(a); err != nil {
		return nil, ErrInvalidTxData
	}
	return a, nil
}
