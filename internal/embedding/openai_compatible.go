package embedding

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

// OpenAICompatibleClient generates embeddings via any server that speaks the
// OpenAI /v1/embeddings request/response shape (OpenAI itself, vLLM, LiteLLM,
// Text Embeddings Inference, llama.cpp's server, etc.).
//
// Why this exists alongside the Ollama client:
//   - SAGE deployments often share an embeddings stack with other services
//     (e.g. a multilingual vLLM serving gte-Qwen2 or BAAI bge-m3).
//   - The hash provider isn't semantic; the Ollama provider is locked to the
//     /api/embed shape. This third option lets operators reuse whatever
//     OpenAI-compatible endpoint they already run.
//
// BFT app-validator semantics are unaffected — embedder swap is below the
// validator layer; this only changes how text turns into a float32 vector.
type OpenAICompatibleClient struct {
	baseURL    string
	model      string
	apiKey     string
	dimension  int
	httpClient *http.Client
	mu         sync.RWMutex
	ready      bool
}

// NewOpenAICompatibleClient creates a client for an OpenAI-compatible
// /v1/embeddings endpoint.
//
//   - baseURL:    e.g. "https://api.openai.com" or "http://my-vllm:9501".
//     The "/v1/embeddings" path is appended automatically.
//   - model:      model identifier the upstream expects (e.g.
//     "text-embedding-3-small", "Alibaba-NLP/gte-Qwen2-1.5B-instruct").
//   - apiKey:     bearer token. If empty, no Authorization header is sent —
//     useful for self-hosted endpoints on a trusted network.
//   - dimension:  expected output dimension. The response is validated to
//     match this; mismatches are surfaced as errors to make
//     misconfiguration loud rather than silently corrupt the
//     vector store. Must be > 0.
func NewOpenAICompatibleClient(baseURL, model, apiKey string, dimension int) *OpenAICompatibleClient {
	return &OpenAICompatibleClient{
		baseURL:   baseURL,
		model:     model,
		apiKey:    apiKey,
		dimension: dimension,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// openAIEmbedRequest mirrors the OpenAI POST /v1/embeddings request body.
// "input" can be a string or an array of strings; we send a single string
// per call to keep parity with the Ollama and Hash providers.
type openAIEmbedRequest struct {
	Model string `json:"model"`
	Input string `json:"input"`
}

// openAIEmbedResponse mirrors the OpenAI POST /v1/embeddings response. We
// only need the first item's "embedding" field; everything else (usage,
// model echo, object type) is ignored.
type openAIEmbedResponse struct {
	Data []struct {
		Embedding []float64 `json:"embedding"`
	} `json:"data"`
}

// Embed generates an embedding vector for the given text via the configured
// OpenAI-compatible endpoint and validates the response dimension.
func (c *OpenAICompatibleClient) Embed(ctx context.Context, text string) ([]float32, error) {
	body, err := json.Marshal(openAIEmbedRequest{
		Model: c.model,
		Input: text,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal embed request: %w", err)
	}

	url := c.baseURL + "/v1/embeddings"
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create embed request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		httpReq.Header.Set("Authorization", "Bearer "+c.apiKey)
	}

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("openai-compatible embed request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // 1 MB max
		return nil, fmt.Errorf("openai-compatible embed error (status %d): %s", resp.StatusCode, string(respBody))
	}

	var embedResp openAIEmbedResponse
	if err := json.NewDecoder(resp.Body).Decode(&embedResp); err != nil {
		return nil, fmt.Errorf("decode embed response: %w", err)
	}

	if len(embedResp.Data) == 0 || len(embedResp.Data[0].Embedding) == 0 {
		return nil, fmt.Errorf("openai-compatible endpoint returned no embeddings")
	}

	f64 := embedResp.Data[0].Embedding
	if c.dimension > 0 && len(f64) != c.dimension {
		return nil, fmt.Errorf("openai-compatible embed dimension mismatch: configured %d, got %d", c.dimension, len(f64))
	}

	// Convert float64 to float32 for pgvector parity with the Ollama provider.
	result := make([]float32, len(f64))
	for i, v := range f64 {
		result[i] = float32(v)
	}

	c.mu.Lock()
	c.ready = true
	c.mu.Unlock()

	return result, nil
}

// Dimension returns the configured output dimension.
func (c *OpenAICompatibleClient) Dimension() int {
	return c.dimension
}

// Ready returns true once at least one successful embedding has been generated.
func (c *OpenAICompatibleClient) Ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.ready
}

// Semantic returns true — OpenAI-compatible endpoints are expected to serve
// real embedding models (text-embedding-3-*, gte-Qwen2, bge-m3, etc.).
func (c *OpenAICompatibleClient) Semantic() bool {
	return true
}

// Name returns the canonical provider name. The REST /v1/embed/info handler
// uses this when present so semantic providers other than Ollama don't get
// mislabeled in operator-facing health output.
func (c *OpenAICompatibleClient) Name() string {
	return "openai-compatible"
}

// Ping checks whether the configured endpoint is reachable. The OpenAI
// embeddings spec doesn't define a dedicated health endpoint, so we issue
// a minimal embed request — most servers will return a fast response or a
// recognizable 4xx that still proves the endpoint is alive.
func (c *OpenAICompatibleClient) Ping(ctx context.Context) error {
	_, err := c.Embed(ctx, "ping")
	return err
}
