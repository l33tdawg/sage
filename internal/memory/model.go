package memory

import (
	"time"
)

// MemoryStatus represents the lifecycle status of a memory.
type MemoryStatus string

const (
	StatusProposed   MemoryStatus = "proposed"
	StatusValidated  MemoryStatus = "validated"
	StatusCommitted  MemoryStatus = "committed"
	StatusChallenged MemoryStatus = "challenged"
	StatusDeprecated MemoryStatus = "deprecated"
)

// MemoryType represents the type of memory.
type MemoryType string

const (
	TypeFact        MemoryType = "fact"
	TypeObservation MemoryType = "observation"
	TypeInference   MemoryType = "inference"
)

// MemoryRecord represents a memory object in the SAGE system.
type MemoryRecord struct {
	MemoryID        string       `json:"memory_id"`
	SubmittingAgent string       `json:"submitting_agent"`
	Content         string       `json:"content"`
	ContentHash     []byte       `json:"content_hash"`
	Embedding       []float32    `json:"embedding,omitempty"`
	EmbeddingHash   []byte       `json:"embedding_hash,omitempty"`
	MemoryType      MemoryType   `json:"memory_type"`
	DomainTag       string       `json:"domain_tag"`
	Provider        string       `json:"provider,omitempty"`
	ConfidenceScore float64      `json:"confidence_score"`
	Status          MemoryStatus `json:"status"`
	ParentHash      string       `json:"parent_hash,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	CommittedAt     *time.Time   `json:"committed_at,omitempty"`
	DeprecatedAt    *time.Time   `json:"deprecated_at,omitempty"`
}

// KnowledgeTriple represents a subject-predicate-object triple.
type KnowledgeTriple struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
}

// ValidMemoryTypes returns all valid memory types.
func ValidMemoryTypes() []MemoryType {
	return []MemoryType{TypeFact, TypeObservation, TypeInference}
}

// IsValidMemoryType checks if a memory type is valid.
func IsValidMemoryType(mt MemoryType) bool {
	switch mt {
	case TypeFact, TypeObservation, TypeInference:
		return true
	}
	return false
}

// IsValidStatus checks if a status is valid.
func IsValidStatus(s MemoryStatus) bool {
	switch s {
	case StatusProposed, StatusValidated, StatusCommitted, StatusChallenged, StatusDeprecated:
		return true
	}
	return false
}
