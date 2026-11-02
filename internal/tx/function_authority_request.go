package tx

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
)

// MaxFunctionAuthorityRequestSize bounds both REST and delegated consensus
// request parsing. Existing transaction routes retain their historical decoder.
const MaxFunctionAuthorityRequestSize = 8 << 10

// DecodeFunctionAuthorityRequest is the shared closed typed contract for Tx41.
// A valid signature cannot authorize unknown, duplicate, case-aliased, missing
// or null fields. The eight semantic fields are all explicitly present.
func DecodeFunctionAuthorityRequest(body []byte) (*FunctionAuthority, error) {
	if len(body) == 0 || len(body) > MaxFunctionAuthorityRequestSize {
		return nil, errors.New("function authority request is empty or exceeds its bound")
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return nil, errors.New("function authority request must be one object")
	}
	fields := make(map[string]bool, 8)
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return nil, errors.New("invalid function authority request field")
		}
		name, ok := key.(string)
		if !ok || fields[name] {
			return nil, errors.New("duplicate function authority request field")
		}
		switch name {
		case "action", "function", "domain", "scope", "subject", "activation_height", "expected_revision", "expires_at":
		default:
			return nil, errors.New("unknown function authority request field")
		}
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errors.New("invalid function authority request value")
		}
		fields[name] = true
	}
	if _, err := decoder.Token(); err != nil || len(fields) != 8 {
		return nil, errors.New("incomplete function authority request")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return nil, errors.New("trailing function authority request data")
	}
	var request FunctionAuthority
	if err := json.Unmarshal(body, &request); err != nil {
		return nil, errors.New("invalid function authority request types")
	}
	if err := ValidateFunctionAuthority(&request); err != nil {
		return nil, err
	}
	return &request, nil
}
