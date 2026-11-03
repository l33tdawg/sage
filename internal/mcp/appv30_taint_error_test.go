package mcp

import (
	"bytes"
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSignedAPITaintProblemsExposeTypedState(t *testing.T) {
	for _, tc := range []struct {
		problemType string
		state       string
	}{
		{"https://sage.dev/errors/memory-tainted", "tainted"},
		{"https://sage.dev/errors/memory-taint-pending", "taint_pending"},
	} {
		t.Run(tc.state, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"type":   tc.problemType,
				"title":  "Memory withheld",
				"status": http.StatusUnavailableForLegalReasons,
				"detail": "governance taint",
			})
			require.NoError(t, err)
			response := &http.Response{
				StatusCode: http.StatusUnavailableForLegalReasons,
				Header: http.Header{
					"Content-Type": []string{"application/problem+json"},
				},
			}
			err = decodeSignedJSONResponse(response, body, nil)
			var problem *apiProblemError
			require.True(t, errors.As(err, &problem))
			require.Equal(t, tc.state, problem.TaintState)
			require.Contains(t, err.Error(), "[taint_state="+tc.state+"]")
		})
	}
}

func TestTaintTypingRequiresCanonical451ProblemEnvelope(t *testing.T) {
	body := []byte(`{
		"type":"https://sage.dev/errors/memory-tainted",
		"title":"Memory withheld",
		"status":403,
		"detail":"mismatched status"
	}`)
	response := &http.Response{
		StatusCode: http.StatusUnavailableForLegalReasons,
		Header: http.Header{
			"Content-Type": []string{"application/problem+json"},
		},
	}
	err := decodeSignedJSONResponse(response, body, nil)
	var problem *apiProblemError
	require.True(t, errors.As(err, &problem))
	require.Empty(t, problem.TaintState)

	body = bytes.Replace(body, []byte(`"status":403`), []byte(`"status":451`), 1)
	response.Header.Set("Content-Type", "application/json")
	err = decodeSignedJSONResponse(response, body, nil)
	require.True(t, errors.As(err, &problem))
	require.Empty(t, problem.TaintState)
}

func TestTaintTypingRequiresExactProblemMediaType(t *testing.T) {
	status := http.StatusUnavailableForLegalReasons
	for _, contentType := range []string{
		"application/problem+json-spoofed",
		"application/problem+json; broken",
	} {
		t.Run(contentType, func(t *testing.T) {
			require.Empty(t, appV30TaintStateFromProblem(
				"https://sage.dev/errors/memory-tainted", &status, contentType, status,
			))
		})
	}
	for _, contentType := range []string{
		"application/problem+json; charset=utf-8",
		"Application/Problem+JSON",
	} {
		t.Run(contentType, func(t *testing.T) {
			require.Equal(t, "tainted", appV30TaintStateFromProblem(
				"https://sage.dev/errors/memory-tainted", &status, contentType, status,
			))
		})
	}
}
