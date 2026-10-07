package federation

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

// A peer's answer to an oversized body decides whether the sender retries or
// strands the event, so the gate has to separate "too many bytes" from "the
// bytes never arrived". Collapsing the two into 413 permanently failed a
// deliverable message: the refused body was 2.6 KB under the route cap and the
// same peer accepted a larger one unchanged minutes later.
func TestFederationBodyReadSeparatesOverflowFromReadFailure(t *testing.T) {
	const limit = 64
	path := "/fed/v1/pipe/linked/resolve"

	t.Run("overflow is a size verdict", func(t *testing.T) {
		request := httptest.NewRequest(
			http.MethodPost, path, bytes.NewReader(bytes.Repeat([]byte{'x'}, limit+1)),
		)
		_, err := readFederationBody(httptest.NewRecorder(), request, limit)
		require.ErrorIs(t, err, errFederationBodyTooLarge)

		status, message := federationBodyError(err)
		require.Equal(t, http.StatusRequestEntityTooLarge, status)
		require.Equal(t, "request body too large", message)
	})

	t.Run("body exactly at the cap is accepted", func(t *testing.T) {
		body := bytes.Repeat([]byte{'y'}, limit)
		request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
		read, err := readFederationBody(httptest.NewRecorder(), request, limit)
		require.NoError(t, err)
		require.Equal(t, body, read)
	})

	t.Run("truncated upload is not a size verdict", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodPost, path, nil)
		request.Body = io.NopCloser(io.MultiReader(
			strings.NewReader("partial"),
			iotest.ErrReader(io.ErrUnexpectedEOF),
		))

		_, err := readFederationBody(httptest.NewRecorder(), request, limit)
		require.Error(t, err)
		require.NotErrorIs(t, err, errFederationBodyTooLarge,
			"a truncated body must never read as an oversize refusal")

		status, message := federationBodyError(err)
		require.Equal(t, http.StatusInternalServerError, status,
			"a read failure has to stay retryable for the sender")
		require.Equal(t, "request body could not be read", message)
	})

}
