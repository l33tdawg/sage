//go:build v119testfixture

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"time"
)

var cometReadPath = regexp.MustCompile(`^/(status|abci_info|block\?height=[1-9][0-9]{0,15}|validators\?height=[1-9][0-9]{0,15}&per_page=100)$`)

func cometRPC(path string) ([]byte, error) {
	transport := &http.Transport{
		Proxy:                  nil,
		DialContext:            (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		DisableKeepAlives:      true,
		DisableCompression:     true,
		MaxResponseHeaderBytes: 16384,
		ResponseHeaderTimeout:  5 * time.Second,
	}
	defer transport.CloseIdleConnections()
	return cometRPCWithTransport(path, transport)
}

func cometRPCWithTransport(path string, transport http.RoundTripper) ([]byte, error) {
	if !cometReadPath.MatchString(path) {
		return nil, errors.New("invalid Comet RPC path")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:26657"+path, nil)
	if err != nil {
		return nil, errors.New("invalid Comet RPC request")
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	response, err := client.Do(request)
	if err != nil {
		return nil, errors.New("Comet RPC unavailable")
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return nil, errors.New("Comet RPC unexpected status")
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, maxResponseBytes+1))
	if err != nil || len(body) > maxResponseBytes || !json.Valid(body) {
		return nil, errors.New("Comet RPC invalid or oversized response")
	}
	return body, nil
}
