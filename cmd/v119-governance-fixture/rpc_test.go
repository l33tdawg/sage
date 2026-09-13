//go:build v119testfixture

package main

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type rpcRoundTrip func(*http.Request) (*http.Response, error)

func (call rpcRoundTrip) RoundTrip(request *http.Request) (*http.Response, error) {
	return call(request)
}

func TestCometRPCFixedUnsignedReads(t *testing.T) {
	for _, path := range []string{"/status", "/abci_info", "/block?height=42", "/validators?height=42&per_page=100"} {
		t.Run(path, func(t *testing.T) {
			body, err := cometRPCWithTransport(path, rpcRoundTrip(func(request *http.Request) (*http.Response, error) {
				if request.URL.String() != "http://127.0.0.1:26657"+path || request.Method != "GET" || request.Body != nil || len(request.Header) != 0 {
					t.Fatal("RPC request must be fixed-loopback, unsigned and bodyless")
				}
				deadline, present := request.Context().Deadline()
				if !present || time.Until(deadline) > 5*time.Second {
					t.Fatal("missing bounded deadline")
				}
				return &http.Response{StatusCode: 200, Body: io.NopCloser(strings.NewReader(`{"result":{}}`))}, nil
			}))
			if err != nil || string(body) != `{"result":{}}` {
				t.Fatal("valid response rejected")
			}
		})
	}
}

func TestCometRPCRejectsPathsBeforeTransport(t *testing.T) {
	for _, path := range []string{"http://example.com/status", "//example.com/status", "/broadcast_tx_commit", "/status?x=1", "/block?height=0", "/block?height=01", "/validators?height=1&per_page=99", "/status\n"} {
		_, err := cometRPCWithTransport(path, rpcRoundTrip(func(*http.Request) (*http.Response, error) {
			t.Fatal("invalid path reached transport")
			return nil, nil
		}))
		if err == nil {
			t.Fatal("invalid path accepted")
		}
	}
}

func TestCometRPCRejectsRedirectErrorsAndInvalidBodies(t *testing.T) {
	for _, sample := range []struct {
		status int
		body   string
	}{
		{302, `{"secret":"do not print"}`}, {503, `{"secret":"do not print"}`},
		{200, "not JSON"}, {200, strings.Repeat("x", maxResponseBytes+1)},
	} {
		calls := 0
		_, err := cometRPCWithTransport("/status", rpcRoundTrip(func(*http.Request) (*http.Response, error) {
			calls++
			return &http.Response{StatusCode: sample.status, Header: http.Header{"Location": {"http://example.com/"}}, Body: io.NopCloser(strings.NewReader(sample.body))}, nil
		}))
		if err == nil || calls != 1 || strings.Contains(err.Error(), "secret") {
			t.Fatal("unsafe error/redirect handling")
		}
	}
	_, err := cometRPCWithTransport("/status", rpcRoundTrip(func(*http.Request) (*http.Response, error) {
		return nil, errors.New("secret transport error")
	}))
	if err == nil || strings.Contains(err.Error(), "secret") {
		t.Fatal("transport error not sanitized")
	}
}
