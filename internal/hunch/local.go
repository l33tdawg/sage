package hunch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
	"sync"
	"time"
)

// LocalClient asks a model served on this machine — SAGE's own Ollama — the same question Hunch's
// client asks a vLLM service, and reads the answer the same way: the first generated token's
// probabilities.
//
// Two things differ from a vLLM backend, and both are the reason this exists rather than pointing
// the existing client at Ollama:
//
//   - Ollama has no structured-output constraint for a bare label (`structured_outputs` is a vLLM
//     parameter it ignores, and `format` would put the answer in JSON, moving the label off the
//     first token). So the model is asked for the label, and if it answers something else the
//     verdict is UNAVAILABLE — never a renormalised guess. The gate then holds the memory for
//     review, which is the safe direction.
//   - The probabilities Ollama reports are pre-constraint, so P(Y) and P(N) must be read where the
//     model actually put them rather than assumed to be the only two candidates.
type LocalClient struct {
	// BaseURL is an OpenAI-compatible endpoint on this machine, e.g. Ollama's /v1.
	BaseURL string
	// Model is the served model name. Required: a local judge must know what it is asking.
	Model string
	// ExpectedGGUFSHA256 binds a managed judge to its verified model blob.
	ExpectedGGUFSHA256 string
	// The direct loopback transport is shared across concurrent gate workers.
	httpOnce   sync.Once
	httpClient *http.Client
	// TopLogprobs is how many alternatives to read per position (default 20, Ollama's cap).
	TopLogprobs int
	// Debias asks every check in both answer orders and averages, which cancels the model's
	// preference for the first-listed option. Costs two backend calls per check.
	Debias bool
	// Timeout bounds one backend call (default 30s).
	Timeout time.Duration
}

// ErrLabelMissing means the model's first token was not one of the labels, so no verdict can be
// read from it. Callers must treat it as "no judgement", never as a middle score.
var ErrLabelMissing = fmt.Errorf("hunch: the model did not answer with a label")

// ErrLabelMass means the label tokens carried no probability mass at the answer position.
var ErrLabelMass = fmt.Errorf("hunch: the label tokens carried no probability at the answer position")

func (c *LocalClient) http() *http.Client {
	c.httpOnce.Do(func() {
		t := c.Timeout
		if t <= 0 {
			t = 30 * time.Second
		}
		c.httpClient = localClient(t)
	})
	return c.httpClient
}

// YesNo implements the judge backend over a locally served model. It satisfies the same interface
// as the Hunch service client, so the existing adapters and the gate need no change.
func (c *LocalClient) YesNo(ctx context.Context, judgeContext any, checks map[string]Check) (map[string]float64, error) {
	if c == nil || c.BaseURL == "" {
		return nil, fmt.Errorf("hunch: local client not configured")
	}
	if err := ValidateLocalURL(c.BaseURL); err != nil {
		return nil, err
	}
	if strings.TrimSpace(c.Model) == "" {
		return nil, fmt.Errorf("hunch: local client needs a model name")
	}
	if len(checks) == 0 {
		return nil, fmt.Errorf("hunch: no checks")
	}
	if err := c.requireLocalModel(ctx); err != nil {
		return nil, err
	}
	block, err := toJudgeContext(judgeContext)
	if err != nil {
		return nil, err
	}
	out := make(map[string]float64, len(checks))
	for id, ch := range checks {
		if ch.Kind == "" {
			ch.Kind = "yesno"
		}
		if ch.Kind != "yesno" {
			return nil, fmt.Errorf("hunch: check %q: local judge supports yesno only, got %q", id, ch.Kind)
		}
		p, err := c.ask(ctx, block, ch)
		if err != nil {
			return nil, fmt.Errorf("hunch: check %q: %w", id, err)
		}
		out[id] = p
	}
	return out, nil
}

// ask renders one check — in both answer orders when Debias is set — and averages the probabilities.
func (c *LocalClient) ask(ctx context.Context, block JudgeContext, ch Check) (float64, error) {
	orders := []bool{false}
	if c.Debias {
		orders = []bool{false, true}
	}
	var sum float64
	for _, nFirst := range orders {
		p, err := c.oneCall(ctx, block, yesNoQuestion(ch, nFirst))
		if err != nil {
			return 0, err
		}
		sum += p
	}
	return sum / float64(len(orders)), nil
}

type chatRequest struct {
	Model           string              `json:"model"`
	Messages        []map[string]string `json:"messages"`
	MaxTokens       int                 `json:"max_tokens"`
	Temperature     float64             `json:"temperature"`
	Logprobs        bool                `json:"logprobs"`
	TopLogprobs     int                 `json:"top_logprobs"`
	ReasoningEffort string              `json:"reasoning_effort,omitempty"`
}

type chatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
		Logprobs *struct {
			Content []struct {
				Token       string   `json:"token"`
				Logprob     *float64 `json:"logprob"`
				TopLogprobs []struct {
					Token   string   `json:"token"`
					Logprob *float64 `json:"logprob"`
				} `json:"top_logprobs"`
			} `json:"content"`
		} `json:"logprobs"`
	} `json:"choices"`
}

func (c *LocalClient) oneCall(ctx context.Context, block JudgeContext, question string) (float64, error) {
	top := c.TopLogprobs
	if top <= 0 {
		top = 20
	}
	body := chatRequest{
		Model: c.Model, Messages: messages(block, question),
		MaxTokens: 1, Temperature: 0, Logprobs: true, TopLogprobs: top,
		// "none" is Ollama's spelling for "do not think"; a thinking model would otherwise
		// spend the single-token budget on reasoning and never answer.
		ReasoningEffort: "none",
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return 0, err
	}
	url := strings.TrimRight(c.BaseURL, "/") + "/chat/completions"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	payload, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return 0, err
	}
	if resp.StatusCode/100 != 2 {
		return 0, fmt.Errorf("local judge HTTP %d: %s", resp.StatusCode, truncate(string(payload), 200))
	}
	return parseLabelProbability(payload)
}

// parseLabelProbability reads P(Y) over P(Y)+P(N) at the first generated token. It fails closed:
// a first token that is not a label, or labels carrying no mass, is an error rather than a score.
func parseLabelProbability(payload []byte) (float64, error) {
	var out chatResponse
	if err := json.Unmarshal(payload, &out); err != nil {
		return 0, fmt.Errorf("local judge returned unreadable JSON: %w", err)
	}
	if len(out.Choices) == 0 || out.Choices[0].Logprobs == nil || len(out.Choices[0].Logprobs.Content) == 0 {
		return 0, fmt.Errorf("local judge returned no token probabilities")
	}
	first := out.Choices[0].Logprobs.Content[0]
	chosen := strings.TrimSpace(first.Token)
	if !isLabel(chosen) {
		return 0, fmt.Errorf("%w: first token %q (caller must treat this as unavailable)", ErrLabelMissing, first.Token)
	}
	if !validLogprob(first.Logprob) {
		return 0, fmt.Errorf("%w: selected label probability missing or invalid", ErrLabelMass)
	}
	var y, n float64
	for _, alt := range first.TopLogprobs {
		if !validLogprob(alt.Logprob) {
			return 0, fmt.Errorf("%w: alternative probability missing or invalid", ErrLabelMass)
		}
		switch strings.TrimSpace(alt.Token) {
		case "Y":
			y += math.Exp(*alt.Logprob)
		case "N":
			n += math.Exp(*alt.Logprob)
		}
	}
	// Read the answer exactly as Hunch's engine does (hunch/engine.py _parse): the chosen (greedy)
	// token is already confirmed to be a label, so renormalise over whatever label mass is present.
	// On a confident item the OTHER label can fall out of top-k entirely — that is real certainty
	// (Hunch returns 1.0/0.0 there), not a missing verdict, so we must NOT fail closed on it; doing
	// so would hold exactly the most-confident-correct memories. If the chosen label itself carried
	// no top-k mass, fall back to its own logprob so it always contributes (Hunch does the same).
	if chosen == "Y" && y == 0 {
		y = math.Exp(*first.Logprob)
	}
	if chosen == "N" && n == 0 {
		n = math.Exp(*first.Logprob)
	}
	if y+n == 0 {
		return 0, fmt.Errorf("%w: first token %q carried no label mass", ErrLabelMass, first.Token)
	}
	return y / (y + n), nil
}

func validLogprob(p *float64) bool {
	return p != nil && !math.IsNaN(*p) && !math.IsInf(*p, 0) && *p <= 0
}

// Check metadata before sending either memory or evidence. A loopback Ollama
// endpoint can serve a cloud alias, including one renamed to a local-looking tag.
func (c *LocalClient) requireLocalModel(ctx context.Context) error {
	raw, err := json.Marshal(map[string]string{"model": c.Model})
	if err != nil {
		return err
	}
	base := strings.TrimSuffix(strings.TrimRight(c.BaseURL, "/"), "/v1")
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, base+"/api/show", bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.http().Do(req)
	if err != nil {
		return fmt.Errorf("local judge model metadata unavailable: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("local judge model metadata HTTP %d", resp.StatusCode)
	}
	var model struct {
		Modelfile   string `json:"modelfile"`
		RemoteHost  string `json:"remote_host"`
		RemoteModel string `json:"remote_model"`
		Details     struct {
			Format string `json:"format"`
		} `json:"details"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&model); err != nil {
		return fmt.Errorf("local judge model metadata: %w", err)
	}
	if model.RemoteHost != "" || model.RemoteModel != "" || model.Details.Format != "gguf" {
		return fmt.Errorf("local judge requires a local GGUF model; cloud or unknown model refused")
	}

	if c.ExpectedGGUFSHA256 != "" && !hasPinnedGGUF(model.Modelfile, c.ExpectedGGUFSHA256) {
		return fmt.Errorf("local judge model does not match the pinned GGUF; refusing to judge")
	}
	return nil
}

func hasPinnedGGUF(modelfile, digest string) bool {
	for _, line := range strings.Split(modelfile, "\n") {
		directive, arg, _ := strings.Cut(strings.TrimSpace(line), " ")
		if !strings.EqualFold(directive, "FROM") {
			continue
		}
		blob := strings.ReplaceAll(strings.Trim(strings.TrimSpace(arg), "\""), "\\", "/")
		return strings.HasSuffix(blob, "/sha256-"+digest)
	}
	return false
}

func isLabel(tok string) bool {
	switch strings.TrimSpace(tok) {
	case "Y", "N":
		return true
	default:
		return false
	}
}

// toJudgeContext accepts the shapes callers already pass (a map with memory/evidence, or the typed
// struct) and returns the ordered struct the renderer needs.
func toJudgeContext(v any) (JudgeContext, error) {
	switch t := v.(type) {
	case JudgeContext:
		return t, nil
	case *JudgeContext:
		if t == nil {
			return JudgeContext{}, fmt.Errorf("hunch: nil context")
		}
		return *t, nil
	case map[string]string:
		return JudgeContext{Memory: t["memory"], Evidence: t["evidence"]}, nil
	default:
		return JudgeContext{}, fmt.Errorf("hunch: unsupported context type %T", v)
	}
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n]
}
