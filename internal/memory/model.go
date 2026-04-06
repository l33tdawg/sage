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
	TypeTask        MemoryType = "task"
)

// TaskStatus represents the lifecycle status of a task memory.
type TaskStatus string

const (
	TaskStatusPlanned    TaskStatus = "planned"
	TaskStatusInProgress TaskStatus = "in_progress"
	TaskStatusDone       TaskStatus = "done"
	TaskStatusDropped    TaskStatus = "dropped"
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
	TaskStatus      TaskStatus   `json:"task_status,omitempty"`
	CreatedAt       time.Time    `json:"created_at"`
	CommittedAt     *time.Time   `json:"committed_at,omitempty"`
	DeprecatedAt    *time.Time   `json:"deprecated_at,omitempty"`
}

// MemoryLink represents a relationship between two memories.
type MemoryLink struct {
	SourceID  string `json:"source_id"`
	TargetID  string `json:"target_id"`
	LinkType  string `json:"link_type"`
	CreatedAt string `json:"created_at,omitempty"`
}

// KnowledgeTriple represents a subject-predicate-object triple.
type KnowledgeTriple struct {
	Subject   string `json:"subject"`
	Predicate string `json:"predicate"`
	Object    string `json:"object"`
}

// SupplementaryData holds off-chain data that accompanies a memory submission but
// is too large for on-chain consensus (e.g. embedding vectors). The REST handler
// stages this in a process-local cache; the ABCI app reads it during FinalizeBlock
// to build the complete record that Commit flushes to the offchain store.
type SupplementaryData struct {
	Embedding        []float32
	EmbeddingHash    []byte
	Provider         string
	KnowledgeTriples []KnowledgeTriple
}

// ValidMemoryTypes returns all valid memory types.
func ValidMemoryTypes() []MemoryType {
	return []MemoryType{TypeFact, TypeObservation, TypeInference, TypeTask}
}

// IsValidMemoryType checks if a memory type is valid.
func IsValidMemoryType(mt MemoryType) bool {
	switch mt {
	case TypeFact, TypeObservation, TypeInference, TypeTask:
		return true
	}
	return false
}

// IsValidTaskStatus checks if a task status is valid.
func IsValidTaskStatus(ts TaskStatus) bool {
	switch ts {
	case TaskStatusPlanned, TaskStatusInProgress, TaskStatusDone, TaskStatusDropped:
		return true
	}
	return false
}

// IsOpenTask returns true if this memory is a task that hasn't been completed or dropped.
func (r *MemoryRecord) IsOpenTask() bool {
	return r.MemoryType == TypeTask && r.TaskStatus != TaskStatusDone && r.TaskStatus != TaskStatusDropped
}

// IsValidStatus checks if a status is valid.
func IsValidStatus(s MemoryStatus) bool {
	switch s {
	case StatusProposed, StatusValidated, StatusCommitted, StatusChallenged, StatusDeprecated:
		return true
	}
	return false
}
