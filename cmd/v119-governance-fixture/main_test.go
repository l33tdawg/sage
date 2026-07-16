//go:build v119testfixture

package main

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/l33tdawg/sage/internal/auth"
)

func TestLoadKeyAcceptsSeedAndPrivateKey(t *testing.T) {
	t.Parallel()

	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(i + 1)
	}
	want := ed25519.NewKeyFromSeed(seed)

	for name, data := range map[string][]byte{
		"seed":        seed,
		"private-key": want,
	} {
		name, data := name, data
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), "operator.key")
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			got, err := loadKey(path)
			if err != nil {
				t.Fatalf("loadKey: %v", err)
			}
			if !got.Equal(want) {
				t.Fatal("loaded private key differs from fixture input")
			}
		})
	}
}

func TestLoadKeyRejectsUnexpectedLength(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "operator.key")
	if err := os.WriteFile(path, []byte("not-an-ed25519-key"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadKey(path); err == nil {
		t.Fatal("loadKey accepted an unexpected key length")
	}
}

func TestFixtureNodeBaseURLIsClosedOverFourInternalGateways(t *testing.T) {
	t.Parallel()

	for index := 0; index < 4; index++ {
		got, err := fixtureNodeBaseURL(index)
		if err != nil {
			t.Fatalf("fixtureNodeBaseURL(%d): %v", index, err)
		}
		want := fmt.Sprintf("http://abci%d:8080", index)
		if got != want {
			t.Fatalf("fixtureNodeBaseURL(%d) = %q, want %q", index, got, want)
		}
	}
	for _, index := range []int{-1, 4, 100} {
		if _, err := fixtureNodeBaseURL(index); err == nil {
			t.Fatalf("fixtureNodeBaseURL(%d) accepted an out-of-range gateway", index)
		}
	}
}

func TestSignedRequestAuthenticatesExactRequest(t *testing.T) {
	t.Parallel()

	seed := make([]byte, ed25519.SeedSize)
	for i := range seed {
		seed[i] = byte(255 - i)
	}
	key := ed25519.NewKeyFromSeed(seed)
	pub := key.Public().(ed25519.PublicKey)
	const (
		method = http.MethodPost
		path   = "/v1/governance/propose"
		body   = `{"operation":"add_validator"}`
	)

	client := &http.Client{Transport: roundTripper(func(r *http.Request) (*http.Response, error) {
		if r.Method != method || r.URL.RequestURI() != path {
			t.Fatalf("wrong request target: %s %s", r.Method, r.URL.RequestURI())
		}
		gotBody, err := io.ReadAll(r.Body)
		if err != nil || string(gotBody) != body {
			t.Fatalf("wrong body: %q (%v)", gotBody, err)
		}
		if got := r.Header.Get("X-Agent-ID"); got != auth.PublicKeyToAgentID(pub) {
			t.Fatalf("wrong agent identity: %q", got)
		}
		timestamp, err := strconv.ParseInt(r.Header.Get("X-Timestamp"), 10, 64)
		if err != nil {
			t.Fatalf("invalid timestamp: %v", err)
		}
		nonce, err := hex.DecodeString(r.Header.Get("X-Nonce"))
		if err != nil || len(nonce) != 8 {
			t.Fatalf("invalid nonce: %x (%v)", nonce, err)
		}
		signature, err := hex.DecodeString(r.Header.Get("X-Signature"))
		if err != nil || !auth.VerifyRequestWithNonce(pub, method, path, gotBody, timestamp, nonce, signature) {
			t.Fatalf("invalid signature: %x (%v)", signature, err)
		}
		return fixtureHTTPResponse(http.StatusCreated, `{"accepted":true}`), nil
	})}

	response, err := signedRequestWithClient(client, key, "http://fixture.invalid/", method, path, []byte(body))
	if err != nil {
		t.Fatalf("signedRequest: %v", err)
	}
	if got, want := string(response), `{"accepted":true}`; got != want {
		t.Fatalf("response = %q, want %q", got, want)
	}
}

func TestSignedRequestFailsClosedOnHTTPErrorAndOversizeResponse(t *testing.T) {
	t.Parallel()

	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	t.Run("HTTP error", func(t *testing.T) {
		client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
			return fixtureHTTPResponse(http.StatusForbidden, "rejected"), nil
		})}
		if _, err := signedRequestWithClient(client, key, "http://fixture.invalid", http.MethodGet, "/denied", nil); err == nil {
			t.Fatal("signedRequest accepted a non-2xx response")
		}
	})

	t.Run("response bound", func(t *testing.T) {
		client := &http.Client{Transport: roundTripper(func(*http.Request) (*http.Response, error) {
			return fixtureHTTPResponse(http.StatusOK, strings.Repeat("x", maxResponseBytes+1)), nil
		})}
		if _, err := signedRequestWithClient(client, key, "http://fixture.invalid", http.MethodGet, "/oversize", nil); err == nil {
			t.Fatal("signedRequest accepted an oversized response")
		}
	})
}

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func fixtureHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     fmt.Sprintf("%d %s", status, http.StatusText(status)),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
