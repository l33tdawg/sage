// Package federation implements the v11 OFF-consensus cross-network transport:
// the dedicated mTLS federation listener (RequireAnyClientCert + per-agreement
// pinned-CA verification), the authenticated cross-chain query client, the
// read-only recall proxy (Mode 1 — exchange), and the co-commit receipt
// exchange (Mode 2 — cross-anchor delivery).
//
// NOTHING in this package runs on the consensus path. Foreign query results are
// merged into REST responses only — never written to InsertMemory, BadgerDB, or
// anything AppHash-visible. Peer receipts reach chain state exclusively as
// verbatim signed bytes inside a TxTypeCoCommitAttest broadcast through
// CometBFT, where processCoCommitAttest re-verifies everything under consensus
// rules. All trust checks here fail CLOSED: unreachable peer, revoked or
// expired agreement, missing remote CA, or SPKI pin mismatch each deny.
package federation

import "time"

// Federation request headers. X-Agent-ID/X-Signature/X-Timestamp/X-Nonce keep
// their local-API meanings; X-Chain-ID and X-Sig-Version are federation-only —
// the Ed25519 canonical message is chain-qualified (X-Sig-Version=2) so a
// request signed for one (sender, receiver) chain pair verifies on no other.
const (
	HeaderChainID    = "X-Chain-ID"
	HeaderAgentID    = "X-Agent-ID"
	HeaderSignature  = "X-Signature"
	HeaderTimestamp  = "X-Timestamp"
	HeaderNonce      = "X-Nonce"
	HeaderSigVersion = "X-Sig-Version"

	// SigVersion2 is the chain-qualified signature scheme (auth.SignRequestV2).
	SigVersion2 = "2"
)

// Query modes — which store search the serving peer runs.
const (
	ModeSemantic = "semantic" // vector similarity over the supplied embedding
	ModeText     = "text"     // full-text search over the query string
	ModeHybrid   = "hybrid"   // BM25 ⊕ vector RRF
)

// QueryRequest is the body of POST /fed/v1/query — one endpoint, mode-switched,
// mirroring the three local recall endpoints. The serving peer enforces its own
// cross_fed agreement scope (AllowedDomains, MaxClearance ceiling, committed-
// only) regardless of what is asked for.
type QueryRequest struct {
	Mode          string    `json:"mode"`
	Query         string    `json:"query,omitempty"`
	Embedding     []float32 `json:"embedding,omitempty"`
	DomainTag     string    `json:"domain_tag,omitempty"`
	MinConfidence float64   `json:"min_confidence,omitempty"`
	TopK          int       `json:"top_k,omitempty"`
	Tags          []string  `json:"tags,omitempty"`
}

// MemoryResult is one shared memory record as served across a federation
// bridge. JSON field names deliberately mirror api/rest.MemoryResult so the
// recall proxy maps 1:1. SourceChainID is stamped by the QUERYING side (the
// proxy) — a serving peer asserting its own provenance would be self-reported.
type MemoryResult struct {
	MemoryID           string     `json:"memory_id"`
	SubmittingAgent    string     `json:"submitting_agent"`
	Content            string     `json:"content"`
	ContentHash        string     `json:"content_hash"`
	MemoryType         string     `json:"memory_type"`
	DomainTag          string     `json:"domain_tag"`
	ConfidenceScore    float64    `json:"confidence_score"`
	CorroborationCount int        `json:"corroboration_count"`
	Classification     int        `json:"classification"`
	Status             string     `json:"status"`
	CreatedAt          time.Time  `json:"created_at"`
	CommittedAt        *time.Time `json:"committed_at,omitempty"`
	SourceChainID      string     `json:"source_chain_id,omitempty"`
}

// QueryResponse is the body returned by POST /fed/v1/query.
type QueryResponse struct {
	ChainID    string          `json:"chain_id"`
	Results    []*MemoryResult `json:"results"`
	TotalCount int             `json:"total_count"`
	// NOTE: the count of records hidden by the classification ceiling is
	// deliberately NOT returned to the peer. Disclosing it turns the response
	// into an existence/keyword oracle for higher-classified content in the
	// allowed domain (iterate keywords, watch the hidden count). It is logged
	// server-side instead. Only non-classification hides (domain/status defense
	// in depth) would ever be safe to disclose.
}

// ReceiptPush is the body of POST /fed/v1/receipt — Mode-2 cross-anchor
// delivery. Receipt carries the VERBATIM tx.EncodeCommitReceipt bytes (sans
// ValSig); ValSig is the sender's ed25519 signature over exactly those bytes,
// made with a key that is a DECLARED coauthor of the SharedID (the consensus
// attest handler re-verifies that bind). SignerPubKey is an optional hint —
// the receiver still resolves the signer against its recorded coauthor set.
type ReceiptPush struct {
	Receipt      []byte `json:"receipt"`
	ValSig       []byte `json:"val_sig"`
	SignerPubKey []byte `json:"signer_pub_key,omitempty"`
}

// ReceiptPushResponse reports what the receiving chain did with a pushed
// receipt. Status is one of "anchored" (attest tx committed now),
// "already_anchored" (idempotent replay — identical anchor exists on-chain).
type ReceiptPushResponse struct {
	Status   string `json:"status"`
	SharedID string `json:"shared_id"`
	TxHash   string `json:"tx_hash,omitempty"`
	Height   int64  `json:"height,omitempty"`
}

// StatusResponse is the body of GET /fed/v1/status — the reachability +
// identity preflight (distinguishes "peer not upgraded/misconfigured" from
// "peer unreachable" in the activation runbook).
type StatusResponse struct {
	ChainID string `json:"chain_id"`
	Time    int64  `json:"time"`
}

// DeliveryResult is the per-peer outcome of a receipt fan-out.
type DeliveryResult struct {
	Status string `json:"status"` // "anchored", "already_anchored", or "error"
	TxHash string `json:"tx_hash,omitempty"`
	Error  string `json:"error,omitempty"`
}

// PeerRecallOutcome is one peer's contribution to a federated recall fan-out.
// Err is non-nil when the peer was skipped or failed (revoked, expired, pin
// mismatch, unreachable, remote error) — the caller discloses these instead of
// silently narrowing the result set.
type PeerRecallOutcome struct {
	ChainID string
	Results []*MemoryResult
	Err     error
}
