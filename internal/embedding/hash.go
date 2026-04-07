package embedding

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"math"
)

// HashProvider generates deterministic pseudo-embeddings from SHA-256 hashes.
// Not semantically meaningful, but useful for testing without external dependencies.
type HashProvider struct {
	dimension int
}

// NewHashProvider creates a hash-based embedding provider.
func NewHashProvider(dimension int) *HashProvider {
	if dimension <= 0 {
		dimension = 768
	}
	return &HashProvider{dimension: dimension}
}

// Embed generates a deterministic pseudo-embedding by hashing the text with SHA-256
// and expanding the hash to fill the configured dimension.
func (p *HashProvider) Embed(_ context.Context, text string) ([]float32, error) {
	result := make([]float32, p.dimension)

	// Use SHA-256 as seed material, re-hash to generate more bytes as needed.
	h := sha256.Sum256([]byte(text))
	seed := h[:]

	for i := 0; i < p.dimension; i++ {
		// Every 8 floats, generate a new hash block from the previous one.
		if i > 0 && i%8 == 0 {
			next := sha256.Sum256(seed)
			seed = next[:]
		}
		// Each float32 uses 4 bytes from the current hash block.
		offset := (i % 8) * 4
		bits := binary.BigEndian.Uint32(seed[offset : offset+4])
		// Map uint32 to [-1, 1].
		result[i] = (float32(bits)/float32(math.MaxUint32))*2 - 1
	}

	// Normalize to unit length.
	var norm float32
	for _, v := range result {
		norm += v * v
	}
	norm = float32(math.Sqrt(float64(norm)))
	if norm > 0 {
		for i := range result {
			result[i] /= norm
		}
	}

	return result, nil
}

// Dimension returns the output dimension of this provider.
func (p *HashProvider) Dimension() int {
	return p.dimension
}

// Ready always returns true since hash embeddings require no external service.
func (p *HashProvider) Ready() bool {
	return true
}

// Semantic returns false — hash embeddings are deterministic but not semantically meaningful.
func (p *HashProvider) Semantic() bool {
	return false
}
