//go:build v119testfixture

// Command v119-governance-fixture is a release-test-only authenticated REST
// client. It is built only into the explicit v11.9 fixture image and never into
// the production amid or sage-gui targets. The client does not own validator
// authority: it can only sign as the per-run operator key mounted by the fault
// harness, while each ABCI gateway independently signs the outer transaction
// with its live CometBFT validator key.
package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
)

const maxResponseBytes = 1 << 20

func main() {
	keyPath := flag.String("key", "", "path to a 32-byte Ed25519 seed or 64-byte private key")
	nodeIndex := flag.Int("node", -1, "fixture ABCI node index (0-3) for request mode")
	method := flag.String("method", http.MethodGet, "HTTP method for request mode")
	path := flag.String("path", "", "absolute REST path for request mode")
	body := flag.String("body", "", "exact JSON body for request mode")
	flag.Parse()

	if *keyPath == "" {
		fatal(errors.New("--key is required"))
	}
	key, err := loadKey(*keyPath)
	if err != nil {
		fatal(err)
	}

	args := flag.Args()
	if len(args) != 1 {
		fatal(errors.New("exactly one mode is required: identity or request"))
	}
	switch args[0] {
	case "identity":
		pub, ok := key.Public().(ed25519.PublicKey)
		if !ok || len(pub) != ed25519.PublicKeySize {
			fatal(errors.New("private key did not yield an Ed25519 public key"))
		}
		fmt.Println(hex.EncodeToString(pub))
	case "request":
		baseURL, nodeErr := fixtureNodeBaseURL(*nodeIndex)
		if nodeErr != nil || *path == "" || !strings.HasPrefix(*path, "/") {
			fatal(errors.New("request mode requires --node 0..3 and an absolute --path"))
		}
		response, requestErr := signedRequest(key, baseURL, strings.ToUpper(*method), *path, []byte(*body))
		if requestErr != nil {
			fatal(requestErr)
		}
		_, _ = os.Stdout.Write(response)
		if len(response) == 0 || response[len(response)-1] != '\n' {
			fmt.Println()
		}
	default:
		fatal(fmt.Errorf("unknown mode %q", args[0]))
	}
}

func fixtureNodeBaseURL(index int) (string, error) {
	switch index {
	case 0:
		return "http://abci0:8080", nil
	case 1:
		return "http://abci1:8080", nil
	case 2:
		return "http://abci2:8080", nil
	case 3:
		return "http://abci3:8080", nil
	default:
		return "", fmt.Errorf("fixture node index %d is outside 0..3", index)
	}
}

func loadKey(path string) (ed25519.PrivateKey, error) {
	data, err := os.ReadFile(path) //nolint:gosec // explicit fixture-owned path
	if err != nil {
		return nil, fmt.Errorf("read operator key: %w", err)
	}
	switch len(data) {
	case ed25519.SeedSize:
		return ed25519.NewKeyFromSeed(data), nil
	case ed25519.PrivateKeySize:
		return ed25519.PrivateKey(append([]byte(nil), data...)), nil
	default:
		return nil, fmt.Errorf("operator key has %d bytes; want %d or %d", len(data), ed25519.SeedSize, ed25519.PrivateKeySize)
	}
}

func signedRequest(key ed25519.PrivateKey, baseURL, method, path string, body []byte) ([]byte, error) {
	client := &http.Client{
		Timeout: 90 * time.Second,
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
	return signedRequestWithClient(client, key, baseURL, method, path, body)
}

func signedRequestWithClient(client *http.Client, key ed25519.PrivateKey, baseURL, method, path string, body []byte) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, method, strings.TrimRight(baseURL, "/")+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	timestamp := time.Now().Unix()
	nonce := make([]byte, 8)
	if _, err = rand.Read(nonce); err != nil {
		return nil, fmt.Errorf("generate request nonce: %w", err)
	}
	pub := key.Public().(ed25519.PublicKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(pub))
	req.Header.Set("X-Timestamp", fmt.Sprintf("%d", timestamp))
	req.Header.Set("X-Nonce", hex.EncodeToString(nonce))
	req.Header.Set("X-Signature", hex.EncodeToString(auth.SignRequestWithNonce(key, method, path, body, timestamp, nonce)))

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("perform request: %w", err)
	}
	defer resp.Body.Close()
	limited := io.LimitReader(resp.Body, maxResponseBytes+1)
	response, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("read response: %w", err)
	}
	if len(response) > maxResponseBytes {
		return nil, errors.New("REST response exceeds 1 MiB fixture limit")
	}
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("REST %s %s returned HTTP %d: %s", method, path, resp.StatusCode, strings.TrimSpace(string(response)))
	}
	return response, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "ERROR:", err)
	os.Exit(1)
}
