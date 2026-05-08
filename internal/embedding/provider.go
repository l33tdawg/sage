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

// Named is an optional interface a Provider can implement to expose its
// canonical name. Operator-facing surfaces (e.g. /v1/embed/info) prefer this
// over inferring "ollama" vs "hash" from Semantic() alone, so providers other
// than the original two don't get mislabeled.
type Named interface {
	Name() string
}
