package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// Reranker scores how well each candidate text answers a query. Used after
// hybrid recall (BM25 + vector via RRF) to refine the top-K ordering with a
// cross-encoder model. v7.1 hybrid path oversamples from RRF, sends the
// candidates here, and returns the reranker-sorted top-K to the caller.
type Reranker interface {
	// Rerank scores `texts` against `query` and returns one RerankResult per
	// input in the same order as the input slice (caller sorts as needed).
	// Implementations should respect ctx deadlines and return all entries with
	// a finite score even when the upstream model rejects some inputs.
	Rerank(ctx context.Context, query string, texts []string) ([]RerankResult, error)
}

// RerankResult carries the upstream score for a single candidate, keyed by
// its position in the input slice so the caller can map back to the original
// memory record without re-correlating by content.
type RerankResult struct {
	Index int
	Score float64
}

// HTTPReranker calls a TEI-compatible /rerank endpoint via plain HTTP. We
// deliberately match HuggingFace TEI's request/response shape so operators
// can drop in `text-embeddings-inference --model-id BAAI/bge-reranker-v2-m3`
// (or any TEI-compatible server) without writing a SAGE-specific adapter.
type HTTPReranker struct {
	baseURL string
	model   string
	timeout time.Duration
	client  *http.Client
}

// NewHTTPReranker returns a reranker that POSTs to `<baseURL>/rerank`.
// `model` is informational only; TEI servers serve one model per process so
// the field shows up in observability rather than gating the request.
func NewHTTPReranker(baseURL, model string, timeout time.Duration) *HTTPReranker {
	return &HTTPReranker{
		baseURL: strings.TrimRight(baseURL, "/"),
		model:   model,
		timeout: timeout,
		client:  &http.Client{Timeout: timeout},
	}
}

// Model exposes the configured model identifier so REST handlers can surface
// it next to the embedder pill on the dashboard.
func (r *HTTPReranker) Model() string { return r.model }

// URL exposes the upstream endpoint for observability.
func (r *HTTPReranker) URL() string { return r.baseURL }

type tEIRerankRequest struct {
	Query     string   `json:"query"`
	Texts     []string `json:"texts"`
	RawScores bool     `json:"raw_scores,omitempty"`
}

type tEIRerankResponse struct {
	Index int     `json:"index"`
	Score float64 `json:"score"`
}

// Rerank issues a single /rerank call. The TEI response is a list of
// {index, score} objects ordered by score descending; we preserve the
// upstream ordering in the returned slice so callers can decide whether to
// re-sort or read top-N directly.
func (r *HTTPReranker) Rerank(ctx context.Context, query string, texts []string) ([]RerankResult, error) {
	if r == nil || r.baseURL == "" {
		return nil, fmt.Errorf("reranker not configured")
	}
	if len(texts) == 0 {
		return nil, nil
	}

	body, err := json.Marshal(tEIRerankRequest{Query: query, Texts: texts})
	if err != nil {
		return nil, fmt.Errorf("marshal rerank request: %w", err)
	}

	reqCtx := ctx
	if r.timeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, r.timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(reqCtx, "POST", r.baseURL+"/rerank", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build rerank request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("rerank request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		// Bound the error body so a misconfigured upstream can't flood the
		// SAGE node logs with multi-megabyte HTML pages.
		buf, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("rerank http %d: %s", resp.StatusCode, strings.TrimSpace(string(buf)))
	}

	var raw []tEIRerankResponse
	if err := json.NewDecoder(resp.Body).Decode(&raw); err != nil {
		return nil, fmt.Errorf("decode rerank response: %w", err)
	}

	out := make([]RerankResult, 0, len(raw))
	for _, r := range raw {
		out = append(out, RerankResult{Index: r.Index, Score: r.Score})
	}
	return out, nil
}

// RerankerConfig captures the env-driven knobs that gate and tune the
// reranker. Resolve via ResolveRerankerConfig at server startup; pass the
// resulting Reranker to the store via its SetReranker method.
type RerankerConfig struct {
	Enabled    bool
	URL        string
	Model      string
	TimeoutMS  int
	Oversample int // RRF returns TopK * Oversample candidates before rerank
}

// Default knobs picked so the reranker is OFF unless an operator explicitly
// turns it on with both SAGE_RERANK_ENABLED and SAGE_RERANK_URL set. The
// oversample default of 2 means: for top-K=10, send 20 candidates to the
// reranker, keep the top-10 reranker scores.
const (
	defaultRerankerTimeoutMS  = 2000
	defaultRerankerOversample = 2
	defaultRerankerModel      = "BAAI/bge-reranker-v2-m3"
)

// ResolveRerankerConfig reads the SAGE_RERANK_* env vars and returns the
// effective configuration. An operator turns the reranker on by setting
// SAGE_RERANK_ENABLED=1 and SAGE_RERANK_URL=<tei-endpoint>; everything else
// is optional.
func ResolveRerankerConfig() RerankerConfig {
	cfg := RerankerConfig{
		Enabled:    envTrue("SAGE_RERANK_ENABLED"),
		URL:        strings.TrimSpace(os.Getenv("SAGE_RERANK_URL")),
		Model:      strings.TrimSpace(os.Getenv("SAGE_RERANK_MODEL")),
		TimeoutMS:  defaultRerankerTimeoutMS,
		Oversample: defaultRerankerOversample,
	}
	if cfg.Model == "" {
		cfg.Model = defaultRerankerModel
	}
	if v := os.Getenv("SAGE_RERANK_TIMEOUT_MS"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			cfg.TimeoutMS = n
		}
	}
	if v := os.Getenv("SAGE_RERANK_OVERSAMPLE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			cfg.Oversample = n
		}
	}
	return cfg
}

// envTrue returns true when the env var is set to any of the common truthy
// shapes. Empty string and "0"/"false"/"no" all return false.
func envTrue(name string) bool {
	v := strings.ToLower(strings.TrimSpace(os.Getenv(name)))
	switch v {
	case "1", "true", "yes", "on":
		return true
	}
	return false
}

// BuildReranker returns a configured HTTPReranker when the config is enabled
// and a URL is present. Otherwise it returns nil so the store can treat
// "reranker absent" as "skip the rerank pass" without branching.
func BuildReranker(cfg RerankerConfig) Reranker {
	if !cfg.Enabled || cfg.URL == "" {
		return nil
	}
	return NewHTTPReranker(cfg.URL, cfg.Model, time.Duration(cfg.TimeoutMS)*time.Millisecond)
}
