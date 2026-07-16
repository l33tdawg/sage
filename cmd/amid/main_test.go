package main

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func rpcTestResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
		Request:    request,
	}
}

type recordingGovernanceDomainBinder struct {
	mu      sync.Mutex
	chainID string
	calls   int
}

func (b *recordingGovernanceDomainBinder) SetExpectedGovernanceDelegationDomain(chainID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.calls++
	if b.chainID != "" && b.chainID != chainID {
		return errors.New("governance domain already bound")
	}
	b.chainID = chainID
	return nil
}

func (b *recordingGovernanceDomainBinder) bindCalls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls
}

func (b *recordingGovernanceDomainBinder) boundChainID() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.chainID
}

func TestGovernanceDomainBindingRetriesUntilCometRPCIsReady(t *testing.T) {
	var requests atomic.Int32
	client := newGovernanceDomainHTTPClient()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if requests.Add(1) < 3 {
			return rpcTestResponse(request, http.StatusServiceUnavailable, "starting"), nil
		}
		return rpcTestResponse(request, http.StatusOK, `{"result":{"node_info":{"network":"sage-v11-9-chaos"}}}`), nil
	})

	binder := &recordingGovernanceDomainBinder{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := bindExpectedGovernanceDomainFromRPCUntilReady(
		ctx,
		client,
		binder,
		"http://comet.test",
		time.Millisecond,
		5*time.Millisecond,
		zerolog.Nop(),
	)
	if err != nil {
		t.Fatalf("bind governance domain: %v", err)
	}
	if got := requests.Load(); got != 3 {
		t.Fatalf("RPC requests = %d, want 3", got)
	}
	if got := binder.boundChainID(); got != "sage-v11-9-chaos" {
		t.Fatalf("bound chain ID = %q, want %q", got, "sage-v11-9-chaos")
	}
}

func TestGovernanceDomainBindingStopsOnContextCancellation(t *testing.T) {
	client := newGovernanceDomainHTTPClient()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return rpcTestResponse(request, http.StatusServiceUnavailable, "starting"), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	err := bindExpectedGovernanceDomainFromRPCUntilReady(
		ctx,
		client,
		&recordingGovernanceDomainBinder{},
		"http://comet.test",
		time.Millisecond,
		5*time.Millisecond,
		zerolog.Nop(),
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("binding error = %v, want context deadline exceeded", err)
	}
}

func TestConfigureGovernanceDomainRejectsUnusableRPCResponses(t *testing.T) {
	tests := []struct {
		name   string
		status int
		body   string
	}{
		{name: "non-200", status: http.StatusServiceUnavailable, body: "starting"},
		{name: "malformed JSON", status: http.StatusOK, body: `{"result":`},
		{name: "missing network", status: http.StatusOK, body: `{"result":{"node_info":{}}}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			client := newGovernanceDomainHTTPClient()
			client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
				return rpcTestResponse(request, tc.status, tc.body), nil
			})
			binder := &recordingGovernanceDomainBinder{}
			_, err := configureExpectedGovernanceDomainFromRPC(
				context.Background(),
				client,
				binder,
				"http://comet.test",
			)
			if err == nil {
				t.Fatal("unusable CometBFT response unexpectedly bound a governance domain")
			}
			if calls := binder.bindCalls(); calls != 0 {
				t.Fatalf("binder calls = %d, want 0", calls)
			}
		})
	}
}

func TestConfigureGovernanceDomainRefusesHTTPRedirects(t *testing.T) {
	var redirectedRequests atomic.Int32
	client := newGovernanceDomainHTTPClient()
	client.Transport = roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.Host == "comet.test" {
			response := rpcTestResponse(request, http.StatusTemporaryRedirect, "")
			response.Header.Set("Location", "https://wrong-authority.test/status")
			return response, nil
		}
		redirectedRequests.Add(1)
		return rpcTestResponse(request, http.StatusOK, `{"result":{"node_info":{"network":"wrong-authority"}}}`), nil
	})

	binder := &recordingGovernanceDomainBinder{}
	_, err := configureExpectedGovernanceDomainFromRPC(
		context.Background(),
		client,
		binder,
		"http://comet.test",
	)
	if err == nil {
		t.Fatal("redirected CometBFT authority unexpectedly bound a governance domain")
	}
	if got := redirectedRequests.Load(); got != 0 {
		t.Fatalf("redirect target requests = %d, want 0", got)
	}
	if calls := binder.bindCalls(); calls != 0 {
		t.Fatalf("binder calls = %d, want 0", calls)
	}
}
