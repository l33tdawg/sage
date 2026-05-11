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

// Modeler is an optional interface a Provider can implement to expose the
// model identifier it serves. The CEREBRUM dashboard surfaces this in the
// embedder status pill so operators running multi-model stacks (vLLM /
// LiteLLM / Ollama with several embedding models loaded) can confirm at a
// glance which one SAGE actually talks to. Providers that don't implement
// this simply don't get a model label in the UI.
type Modeler interface {
	Model() string
}

// Pinger is an optional interface a Provider can implement to expose a
// cheap liveness check. The dashboard health endpoint prefers Ping over
// Ready when present, because Ready is a sticky "has-ever-succeeded" flag
// — useful for /v1/embed/info but unhelpful for a real-time operator pill
// where the upstream embed server may have gone away after boot.
type Pinger interface {
	Ping(ctx context.Context) error
}
