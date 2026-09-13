package memory

import (
	"crypto/sha256"
)

// ComputeContentHash computes the SHA-256 hash of memory content.
func ComputeContentHash(content string) []byte {
	h := sha256.Sum256([]byte(content))
	return h[:]
}
