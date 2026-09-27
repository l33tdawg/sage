// Package hunch is a minimal client for a Hunch judge service
// (github.com/ihubanov/hunch): POST /v1/judge turns a typed yes/no question
// about some context into a calibrated probability, read from the logprobs of
// one constrained token on a model the operator runs.
//
// SAGE uses it only OFF-CHAIN, from the per-node memory voter. A judge call is
// not deterministic across nodes, so its output may shape this node's vote and
// node-local annotations, never consensus state.
package hunch

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Check is one yes/no question. YesIf and NoIf name the look-alike cases; on
// Hunch's own benchmarks naming the look-alike is worth more accuracy than the
// choice of model.
type Check struct {
	Kind     string `json:"kind"`
	Question string `json:"question"`
	YesIf    string `json:"yes_if,omitempty"`
	NoIf     string `json:"no_if,omitempty"`
}

// Client talks to one Hunch service.
type Client struct {
	BaseURL string
	APIKey  string
	Model   string // empty = the service's default model
	HTTP    *http.Client
}

// New returns a client with a bounded per-request timeout.
func New(baseURL, apiKey, model string, timeout time.Duration) *Client {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	return &Client{
		BaseURL: strings.TrimRight(baseURL, "/"),
		APIKey:  apiKey,
		Model:   model,
		HTTP:    &http.Client{Timeout: timeout},
	}
}

// ErrNoVerdict means the service answered but did not return a probability for
// every requested check. Callers must treat it as "no judgement", never as a
// middle score.
var ErrNoVerdict = errors.New("hunch: no verdict")

type judgeRequest struct {
	Context any              `json:"context"`
	Checks  map[string]Check `json:"checks"`
	Model   string           `json:"model,omitempty"`
}

type judgeResponse struct {
	Model   string `json:"model"`
	Results map[string]struct {
		Kind string   `json:"kind"`
		PYes *float64 `json:"p_yes"`
	} `json:"results"`
	Error *struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

// YesNo asks every check about the same context and returns p_yes per check id.
// It fails closed: a transport error, a non-200 answer, or any check without a
// probability is an error, and no partial map is returned.
func (c *Client) YesNo(ctx context.Context, judgeContext any, checks map[string]Check) (map[string]float64, error) {
	if c == nil || c.BaseURL == "" {
		return nil, errors.New("hunch: client not configured")
	}
	if len(checks) == 0 {
		return nil, errors.New("hunch: no checks")
	}
	for id, ch := range checks {
		if ch.Kind == "" {
			ch.Kind = "yesno"
			checks[id] = ch
		}
	}
	body, err := json.Marshal(judgeRequest{Context: judgeContext, Checks: checks, Model: c.Model})
	if err != nil {
		return nil, fmt.Errorf("hunch: encode request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/v1/judge", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("hunch: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.APIKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
	}
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hunch: request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("hunch: read response: %w", err)
	}
	var out judgeResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("hunch: decode response (HTTP %d): %w", resp.StatusCode, err)
	}
	if resp.StatusCode != http.StatusOK {
		if out.Error != nil {
			return nil, fmt.Errorf("hunch: HTTP %d %s: %s", resp.StatusCode, out.Error.Code, out.Error.Message)
		}
		return nil, fmt.Errorf("hunch: HTTP %d", resp.StatusCode)
	}
	ps := make(map[string]float64, len(checks))
	for id := range checks {
		r, ok := out.Results[id]
		if !ok || r.PYes == nil || *r.PYes < 0 || *r.PYes > 1 {
			return nil, fmt.Errorf("%w for check %q", ErrNoVerdict, id)
		}
		ps[id] = *r.PYes
	}
	return ps, nil
}
