package embedding

import "context"

// Provider is the interface for embedding generation.
type Provider interface {
	// Embed generates a vector embedding for the given text.
	Embed(ctx context.Context, text string) ([]float32, error)
	// Dimension returns the output dimension of this provider.
	Dimension() int
	// Ready returns true if the provider is operational.
	Ready() bool
	// Semantic returns true if embeddings carry semantic meaning (e.g. Ollama).
	// Hash-based providers return false — cosine similarity is meaningless.
	Semantic() bool
}
