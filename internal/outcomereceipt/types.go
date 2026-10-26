// Package outcomereceipt defines the off-consensus evidence envelope used to
// exchange reproducible SAGE memory-revaluation results with external value
// systems such as Noesis.
//
// A receipt is evidence, not authority. Admission to SAGE remains a separately
// committed consensus fact, and a receipt never changes a memory's status,
// confidence, access policy, or validator weight.
package outcomereceipt

import "time"

const (
	// VersionV1 is the first public-only outcome receipt format.
	VersionV1 = "sage.noesis.outcome-receipt/v1"

	// ArmControl is a run in which the candidate memory is masked.
	ArmControl = "control"
	// ArmTreatment is a run in which the candidate memory is present.
	ArmTreatment = "treatment"

	// StatusPositive means the treatment improved the declared metric.
	StatusPositive = "positive"
	// StatusNeutral means the measured effect did not clear the declared bar.
	StatusNeutral = "neutral"
	// StatusNegative means the treatment degraded the declared metric.
	StatusNegative = "negative"
	// StatusInconclusive means the evidence cannot support a directional claim.
	StatusInconclusive = "inconclusive"
	// StatusFailed means the evaluation itself did not complete.
	StatusFailed = "failed"

	// SignatureSchemeEd25519 is the only signature scheme supported by v1.
	SignatureSchemeEd25519 = "ed25519"
	// RoleSAGEAgent identifies the SAGE agent whose memory is evaluated.
	RoleSAGEAgent = "sage_agent"
	// RoleEvaluator identifies an independently run evaluation harness or judge.
	RoleEvaluator = "evaluator"
	// RoleNoesis identifies an optional Noesis-side attestation.
	RoleNoesis = "noesis"
)

// Receipt is the canonical evidence body. All referenced SAGE records are
// public (classification 0) in v1 so the receipt can be exchanged without
// leaking restricted memory existence or content.
type Receipt struct {
	Version    string        `json:"version"`
	CreatedAt  time.Time     `json:"created_at"`
	Producer   Producer      `json:"producer"`
	TaskSet    TaskSet       `json:"task_set"`
	Candidate  MemoryRef     `json:"candidate"`
	Evaluation Evaluation    `json:"evaluation"`
	Runs       []Run         `json:"runs"`
	Summary    Summary       `json:"summary"`
	Evidence   []EvidenceRef `json:"evidence"`
	Caveats    []string      `json:"caveats,omitempty"`
}

// Producer identifies the harness that generated the receipt.
type Producer struct {
	Name               string `json:"name"`
	Version            string `json:"version"`
	ImplementationHash string `json:"implementation_hash"`
}

// TaskSet is the immutable set of public SAGE task memories used by the
// experiment. Digest is the SHA-256 of the canonical versioned task list.
type TaskSet struct {
	ID      string    `json:"id"`
	Version string    `json:"version"`
	Digest  string    `json:"digest"`
	Tasks   []TaskRef `json:"tasks"`
}

// TaskRef identifies one committed public SAGE task memory.
type TaskRef struct {
	ChainID        string `json:"chain_id"`
	MemoryID       string `json:"memory_id"`
	ContentHash    string `json:"content_hash"`
	Domain         string `json:"domain"`
	AuthorAgentID  string `json:"author_agent_id"`
	Classification int    `json:"classification"`
}

// MemoryRef identifies the public candidate memory whose effect is measured.
type MemoryRef struct {
	ChainID        string `json:"chain_id"`
	MemoryID       string `json:"memory_id"`
	ContentHash    string `json:"content_hash"`
	Domain         string `json:"domain"`
	AuthorAgentID  string `json:"author_agent_id"`
	Classification int    `json:"classification"`
}

// Evaluation pins the judge and retrieval procedure from before the measured
// runs. A receipt whose judge changes mid-experiment is invalid.
type Evaluation struct {
	Judge          JudgeSpec       `json:"judge"`
	Retrieval      RetrievalPolicy `json:"retrieval"`
	Metric         string          `json:"metric"`
	Unit           string          `json:"unit"`
	HigherIsBetter bool            `json:"higher_is_better"`
	Repeats        int             `json:"repeats"`
	ForcedContext  bool            `json:"forced_context"`
	StartedAt      time.Time       `json:"started_at"`
	CompletedAt    time.Time       `json:"completed_at"`
}

// JudgeSpec pins every judge input that can change the result.
type JudgeSpec struct {
	ModelID         string `json:"model_id"`
	ModelHash       string `json:"model_hash"`
	RubricHash      string `json:"rubric_hash"`
	PromptHash      string `json:"prompt_hash"`
	EnvironmentHash string `json:"environment_hash"`
}

// RetrievalPolicy identifies the SAGE retrieval configuration used to build
// context. QueryHash remains per-run because each task has its own query.
type RetrievalPolicy struct {
	ID      string `json:"id"`
	Version string `json:"version"`
	TopK    int    `json:"top_k"`
}

// Run is one measured arm. ContextMemoryIDs preserves retrieval and prompt
// order because that order may change model behaviour. MaskedMemoryIDs and
// ArtifactHashes are sets and are sorted by CanonicalJSON before hashing.
type Run struct {
	TaskMemoryID     string   `json:"task_memory_id"`
	Trial            int      `json:"trial"`
	Arm              string   `json:"arm"`
	Seed             uint64   `json:"seed"`
	QueryHash        string   `json:"query_hash"`
	ContextMemoryIDs []string `json:"context_memory_ids,omitempty"`
	MaskedMemoryIDs  []string `json:"masked_memory_ids,omitempty"`
	Score            string   `json:"score,omitempty"`
	Passed           *bool    `json:"passed,omitempty"`
	LatencyMicros    int64    `json:"latency_micros,omitempty"`
	ArtifactHashes   []string `json:"artifact_hashes,omitempty"`
	Error            string   `json:"error,omitempty"`
}

// Summary is the evaluator's declared aggregate outcome. It does not alter the
// SAGE lifecycle status of the candidate memory.
type Summary struct {
	Status        string `json:"status"`
	Pairs         int    `json:"pairs"`
	ControlMean   string `json:"control_mean,omitempty"`
	TreatmentMean string `json:"treatment_mean,omitempty"`
	Delta         string `json:"delta,omitempty"`
	CI95Lower     string `json:"ci95_lower,omitempty"`
	CI95Upper     string `json:"ci95_upper,omitempty"`
}

// EvidenceRef points to a content-addressed artifact. URI is optional and is
// never treated as proof without a matching Digest.
type EvidenceRef struct {
	Kind      string `json:"kind"`
	MediaType string `json:"media_type"`
	Digest    string `json:"digest"`
	URI       string `json:"uri,omitempty"`
}

// Signature is detached from Receipt so every signer covers the same body.
type Signature struct {
	Scheme    string `json:"scheme"`
	Role      string `json:"role"`
	SignerID  string `json:"signer_id"`
	PublicKey string `json:"public_key"`
	Signature string `json:"signature"`
}

// SignedReceipt is the portable JSON envelope exchanged between SAGE-side
// tooling, evaluators, and an external value system.
type SignedReceipt struct {
	Receipt    Receipt     `json:"receipt"`
	Signatures []Signature `json:"signatures"`
}
