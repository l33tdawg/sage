// Package federation implements the v11 OFF-consensus cross-network transport:
// the dedicated mTLS federation listener (RequireAnyClientCert + per-agreement
// pinned-CA verification), the authenticated cross-chain query client, the
// read-only recall proxy (Mode 1 — exchange), and the co-commit receipt
// exchange (Mode 2 — cross-anchor delivery).
//
// NOTHING in this package writes chain state DIRECTLY. Foreign query results are
// merged into REST responses only — never written to InsertMemory, BadgerDB, or
// anything AppHash-visible. Where federation data DOES reach chain state it does
// so exclusively as a signed transaction broadcast through CometBFT, which
// re-verifies everything under consensus rules: peer receipts as verbatim bytes
// inside a TxTypeCoCommitAttest, and (v11.5 domain sync) an accepted push as a
// locally-signed TxTypeMemorySubmit — see handleSyncPush. All trust checks here
// fail CLOSED: unreachable peer, revoked or expired agreement, missing remote
// CA, or SPKI pin mismatch each deny.
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
	// HeaderClientCapabilities is an advisory, authenticated-peer feature
	// preference. It never grants access; it lets a new peer request a compact
	// status response before making a bounded targeted lookup.
	HeaderClientCapabilities = "X-Sage-Capabilities"

	// SigVersion2 is the chain-qualified signature scheme (auth.SignRequestV2).
	SigVersion2 = "2"
	// SigVersion3 adds the rotating per-agreement TOTP factor (auth.SignRequestV3),
	// required once a shared seed is established for the agreement.
	SigVersion3 = "3"
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
	Mode      string    `json:"mode"`
	Query     string    `json:"query,omitempty"`
	Embedding []float32 `json:"embedding,omitempty"`
	// EmbeddingProvider identifies the exact vector space of Embedding. Serving
	// peers fail closed when a vector is supplied without it.
	EmbeddingProvider string   `json:"embedding_provider,omitempty"`
	DomainTag         string   `json:"domain_tag,omitempty"`
	MinConfidence     float64  `json:"min_confidence,omitempty"`
	TopK              int      `json:"top_k,omitempty"`
	Tags              []string `json:"tags,omitempty"`
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
	ChainID     string `json:"chain_id"`
	NetworkName string `json:"network_name,omitempty"`
	Time        int64  `json:"time"`
	// Capabilities advertises optional route groups (e.g. "sync"). Additive
	// courtesy signal; the authoritative unsupported-peer detection is the
	// 404/405/501 on the sync routes themselves (see CapabilitySync).
	Capabilities []string `json:"capabilities,omitempty"`
	// SharingGrant is this server's unilateral, outbound grant to the
	// authenticated caller. It is intentionally optional: absent means an older
	// peer that does not advertise its grant, while present with an empty domain
	// list means this peer currently shares nothing.
	SharingGrant *SharingGrant `json:"sharing_grant,omitempty"`
	// PeerRBACGrant is advertised only to the exact chain+operator identity bound
	// to the snapshot. Its absence preserves compatibility with legacy peers.
	PeerRBACGrant *PeerRBACGrant `json:"peer_rbac_grant,omitempty"`
	// PipeContacts is the finite, peer-scoped projection of domain owners and
	// active non-owner agents with current local read access to domains in
	// PeerRBACGrant. An unavailable owner may remain as diagnostic metadata, but
	// is never routable. Presence never advertises the federated-pipeline
	// capability or authorizes delivery. Older peers ignore this additive field.
	PipeContacts *PipeContactGrant `json:"pipe_contacts,omitempty"`
}

// SharingGrant is the serving node's current read envelope for one
// authenticated peer. Sync/copy policy is deliberately separate.
type SharingGrant struct {
	AllowedDomains []string `json:"allowed_domains"`
	MaxClearance   uint8    `json:"max_clearance"`
}

// SharingUpdateResult describes the full directional sharing snapshot that a
// successful tx-33 update committed locally.
type SharingUpdateResult struct {
	Domains      []string `json:"domains"`
	MaxClearance uint8    `json:"max_clearance"`
	TxHash       string   `json:"tx_hash"`
}

// PeerRBACGrant is the serving node's independent capability snapshot for the
// authenticated peer. Nil/omitted means a legacy peer; present with zero domain
// rows is explicit deny-all.
type PeerRBACGrant struct {
	PolicyVersion int                   `json:"policy_version"`
	Revision      int64                 `json:"revision,omitempty"`
	Paused        bool                  `json:"paused,omitempty"`
	Domains       []PeerRBACDomainGrant `json:"domains"`
}

// RevokeNotice is sent best-effort to the exact authenticated peer before a
// permanent local tx-34 revoke. PolicyEpoch prevents a delayed notice from a
// retired JOIN generation terminating a newly paired connection.
type RevokeNotice struct {
	PolicyEpoch string `json:"policy_epoch"`
	Reason      string `json:"reason,omitempty"`
}

type RevokeNoticeResponse struct {
	Status string `json:"status"`
	TxHash string `json:"tx_hash,omitempty"`
}

type RevokeAgreementResult struct {
	TxHash       string `json:"tx_hash"`
	PeerNotified bool   `json:"peer_notified"`
	NoticeError  string `json:"notice_error,omitempty"`
}

type PeerRBACDomainGrant struct {
	Domain string `json:"domain"`
	Read   bool   `json:"read"`
	Write  bool   `json:"write"`
	Copy   bool   `json:"copy"`
}

const (
	PipeContactVersion = 1
	// maxPipeContactStatusContacts is the largest v1 contact snapshot we put
	// on the long-lived status route. v11.13.0 validates this same v1 envelope,
	// so increasing it would make a newer peer advertise an unusable snapshot
	// to an already released peer. Larger projections use the additive,
	// authenticated lookup route below instead.
	maxPipeContactStatusContacts = 1024
	// Keep enough room below the federated client's 16 MiB response ceiling for
	// the rest of StatusResponse. A small contact count can still be large when
	// every recipient has a wide shared-domain basis.
	maxPipeContactStatusBytes = 1 << 20
	// The lookup route has its own response cap so a small result count cannot
	// still produce a response too large for a federation client to consume.
	maxPipeContactLookupBytes   = 4 << 20
	maxPipeContactLookupResults = 20
)

const CapabilityFederatedPipeline = "federated-pipeline-v1"

// CapabilityFederatedPipelineContactLookup advertises the v11.13 targeted
// contact route. It lets a peer resolve one address or search a human name
// without transferring an unbounded roster in /fed/v1/status.
const CapabilityFederatedPipelineContactLookup = "federated-pipeline-contact-lookup-v1"

// PipeContactGrant is the serving node's peer-scoped agent-address snapshot.
// A routable contact is an active local agent that is either the effective
// owner of a shared domain or currently holds normal local read access to it.
// Unavailable owners remain diagnostic metadata. Revision is content-addressed
// over the exact federation binding, domain ownership, agent availability,
// access projection, and all contacts; it invalidates the complete cached
// snapshot. Individual deliveries derive a target-specific authorization
// revision so an unrelated contact cannot invalidate exact-address queued work.
type PipeContactGrant struct {
	Version     int           `json:"version"`
	AgreementID string        `json:"agreement_id"`
	Revision    string        `json:"revision"`
	Paused      bool          `json:"paused"`
	Contacts    []PipeContact `json:"contacts"`
}

// PipeContactLookupRequest is an authenticated, peer-scoped contact query.
// Target is used by the delivery resolver (normally a canonical
// agent_id@chain_id address); Name is used by caller-authorized discovery.
// Exactly one selector is required.
type PipeContactLookupRequest struct {
	Target string `json:"target,omitempty"`
	Name   string `json:"name,omitempty"`
	Limit  int    `json:"limit,omitempty"`
}

// PipeContactLookupResponse is intentionally small even when the peer has a
// very large shared-domain recipient projection. Grant retains the normal v1
// contact binding, but contains only the bounded matched contacts.
type PipeContactLookupResponse struct {
	Grant     *PipeContactGrant `json:"grant"`
	Total     int               `json:"total"`
	Truncated bool              `json:"truncated,omitempty"`
}

// PipeContact is one local agent associated with a shared domain. It is
// routable only while Available and Accepting. Handle is a copy-friendly alias
// only; Address and AgentID preserve the exact machine identity. ContactID is
// the authorization-bound identity of this exact domain-access projection; it
// changes when the peer/policy/owner generation changes and must accompany
// every acceptance mutation.
type PipeContact struct {
	AgentID        string              `json:"agent_id"`
	ContactID      string              `json:"contact_id,omitempty"`
	DisplayName    string              `json:"display_name,omitempty"`
	RegisteredName string              `json:"registered_name,omitempty"`
	Provider       string              `json:"provider,omitempty"`
	Address        string              `json:"address,omitempty"`
	Handle         string              `json:"handle,omitempty"`
	Available      bool                `json:"available"`
	Accepting      bool                `json:"accepting"`
	Domains        []PipeContactDomain `json:"domains"`
}

// PipeContactDomain records why a contact is visible. OwningDomain differs
// from Domain when the shared leaf inherits its effective owner from an
// ancestor.
type PipeContactDomain struct {
	Domain       string `json:"domain"`
	OwningDomain string `json:"owning_domain"`
	OwnerHeight  int64  `json:"owner_height"`
}

// RemotePipeTarget is the exact, current peer-scoped route produced from a
// qualified agent address. Display fields are advisory; transport binds the
// full chain/agent/contact/policy tuple.
type RemotePipeTarget struct {
	ChainID         string `json:"chain_id"`
	AgentID         string `json:"agent_id"`
	ContactID       string `json:"contact_id"`
	ContactRevision string `json:"contact_revision"`
	PolicyEpoch     string `json:"policy_epoch"`
	AgreementID     string `json:"agreement_id"`
	Address         string `json:"address"`
	Handle          string `json:"handle,omitempty"`
	DisplayName     string `json:"display_name,omitempty"`
	// Domains is the current, peer-authenticated visibility basis for this
	// target. REST rechecks the local caller against it on resolve and send;
	// it is not exposed to ordinary pipeline clients.
	Domains []PipeContactDomain `json:"-"`
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

// ---- v11.5 domain-sync wire protocol (/fed/v1/sync/*) -----------------------
//
// Sync admission is the ONE deliberate exception to "foreign data is never
// written": a pushed item reaches chain state EXCLUSIVELY by being wrapped in
// a locally-signed TxTypeMemorySubmit broadcast through CometBFT, where the
// receiver's own consensus gates (write access, domain requirement, terminal-
// status guard) re-validate it. Federation code still never writes BadgerDB
// or InsertMemory directly.

// CapabilitySync advertises the /fed/v1/sync/* routes on StatusResponse.
// Informational only in v11.5: the sender detects an unsupported peer from the
// push/digest 404/405/501 (mapped to ErrSyncUnsupported), which is the
// load-bearing check; this field is a courtesy preflight signal, not consulted
// before enqueueing.
const CapabilitySync = "sync"

const (
	// SyncPushMaxItems bounds one push batch: 8 blocking broadcast_tx_commit
	// admits fit inside the federation listener's write budget; bigger
	// batches are a protocol error, not a truncation.
	SyncPushMaxItems = 8
	// SyncMaxItemContent bounds one item's content bytes (sender enforces at
	// enqueue; receiver rejects the batch as malformed on violation).
	SyncMaxItemContent = 64 * 1024
	// SyncMaxTags and SyncMaxTagBytes bound off-consensus tag metadata so a
	// peer cannot turn a small memory batch into unbounded SQLite work.
	SyncMaxTags     = 32
	SyncMaxTagBytes = 128
)

// Wire outcomes for one pushed item. Terminal rejections are recorded in the
// receiver's sync_origin ledger and replayed on redelivery; "retry" records
// nothing and invites the sender's backoff.
const (
	SyncOutcomeAccepted           = "accepted"
	SyncOutcomeDuplicate          = "duplicate" // success-equivalent: already admitted / already present in the same domain
	SyncOutcomeRejectedXDomainDup = "rejected_cross_domain_dup"
	SyncOutcomeRejectedClearance  = "rejected_clearance"
	SyncOutcomeRejectedConsent    = "rejected_not_consented"
	SyncOutcomeRejectedScope      = "rejected_domain_scope"
	// SyncOutcomeRejectedWriteAccess: the receiver ADMITTED the item through
	// consent/scope/clearance but the receiver's federation operator agent
	// lacks RBAC write access to the domain on its chain, so the locally-signed
	// submit tx was included in a block and REJECTED at FinalizeBlock. Distinct
	// from the cheap gate-level rejected_domain_scope: this one costs the
	// receiver a full consensus round per attempt, so the sender treats it as a
	// #14-class dead-end (attempts-capped), not an infinite cheap retry.
	SyncOutcomeRejectedWriteAccess = "rejected_write_access"
	// SyncOutcomeRejectedOriginSig: the item carried an origin signature that
	// did not verify against the origin agent's key — a forged / corrupted /
	// mis-attributed item (v11.8 mesh-backfill anti-forgery, docs §4.4).
	// Content-derived and terminal: it will never verify, so the sender must
	// not retry it forever.
	SyncOutcomeRejectedOriginSig = "rejected_origin_sig"
	// SyncOutcomeSuppressed: the receiver locally deleted this origin memory and
	// holds a memory-scope local_suppress tombstone for it (docs §10 Gate 6.5).
	// TERMINAL and anti-resurrection: the item is NOT recorded to sync_origin and
	// NEVER broadcast, and the sender must never redeliver it (a local delete on
	// the receiver is durable and sovereign). This is the RECEIVER-INTERNAL value;
	// handleSyncPush COLLAPSES it to the generic SyncOutcomeRejectedNotAdmitted on the
	// wire so the sender learns "not admitted", never that a tombstone exists (I5).
	SyncOutcomeSuppressed = "suppressed"
	// SyncOutcomeRejectedNotAdmitted is the generic terminal wire reason a suppressed
	// item is reported as (I5): indistinguishable from an ordinary durable reject, so a
	// pushing peer cannot learn that the receiver sovereignly deleted the item. Terminal
	// on the sender (never retried) — it is ONLY ever emitted for a local_suppress hit.
	SyncOutcomeRejectedNotAdmitted = "rejected_not_admitted"
	SyncOutcomeRetry               = "retry"
)

// SyncItem is one memory offered for replication. Content travels raw (the
// /fed/v1/query MemoryResult precedent); content_hash must equal
// sha256(content) — the receiver verifies before admitting. Classification,
// type, and confidence are sender-asserted; the receiver enforces its own
// clearance ceiling and its voter governs the copy's lifecycle.
type SyncItem struct {
	OriginChainID   string   `json:"origin_chain_id"`
	OriginMemoryID  string   `json:"origin_memory_id"`
	OriginCreatedAt string   `json:"origin_created_at,omitempty"`
	Domain          string   `json:"domain"`
	Classification  int      `json:"classification"`
	MemoryType      string   `json:"memory_type,omitempty"`
	ConfidenceScore float64  `json:"confidence_score,omitempty"`
	Content         string   `json:"content"`
	ContentHash     string   `json:"content_hash"`
	Tags            []string `json:"tags,omitempty"`
	// OriginSig is the ORIGIN agent's ed25519 signature over the canonical
	// provenance+content+classification bytes (originSigMessage). It is produced
	// at the origin's native commit and carried VERBATIM by every relayer, so a
	// v11.8 group mesh-backfill relayer provably cannot forge, mis-attribute, or
	// re-classify an item — it can only delay/withhold. OPTIONAL on the wire for
	// rolling compatibility with pre-v11.8 2-node pairs (an empty sig skips
	// verification); when present it is verified at admission BEFORE the
	// idempotency step. Base64 on the wire (the ReceiptPush.ValSig convention).
	OriginSig []byte `json:"origin_sig,omitempty"`
}

// SyncPushRequest is the body of POST /fed/v1/sync/push.
type SyncPushRequest struct {
	Items []SyncItem `json:"items"`
}

// SyncItemResult is the per-item admission outcome. Reasons are enum codes
// only — they never echo receiver content (the sender already possesses the
// item, so per-item outcomes are not an oracle the way query hiding is).
type SyncItemResult struct {
	OriginMemoryID string `json:"origin_memory_id"`
	Outcome        string `json:"outcome"`
	LocalMemoryID  string `json:"local_memory_id,omitempty"`
}

// SyncPushResponse is the body of a successful push.
type SyncPushResponse struct {
	Results []SyncItemResult `json:"results"`
}

// SyncDigestRequest is the body of POST /fed/v1/sync/digest — the SENDER asks
// the RECEIVER "what have you already decided about my memories in this
// domain". After is an exclusive origin_memory_id cursor ("" = start).
type SyncDigestRequest struct {
	Domain string `json:"domain"`
	After  string `json:"after,omitempty"`
	Limit  int    `json:"limit,omitempty"`
	// GroupID selects the v11.8 multi-node backfill path (docs §9.2). When set,
	// handleSyncDigest applies the membership+domain hard-gate and serves ANY
	// origin chain's admitted ids for the shared domain (not just the requester's).
	// Empty preserves the pairwise 2-node behaviour (origin_chain_id = requester).
	GroupID string `json:"group_id,omitempty"`
}

// SyncDigestMaxIDs caps one digest page (~140KB of 64-char ids — well under
// the client's 16MB read limit, cheap to serve).
const SyncDigestMaxIDs = 2000

// SyncDigestResponse enumerates the receiver's ADMISSION set — every origin id
// it has ADMITTED (sync_origin.outcome='admitted'); terminal rejections are NOT
// recorded and so are NOT listed here. This is what lets anti-entropy skip
// re-offering already-admitted items after a sender-side outbox loss; a
// previously-rejected item may be re-offered and simply re-rejected (idempotent,
// content-derived on the receiver). Deliberately NOT the committed set: the
// receiver's sovereign voter may deprecate a copy later without re-opening
// delivery. ConsentedDomains surfaces asymmetric consent instead of silently
// dropping (the sender can show "peer has not consented to X").
type SyncDigestResponse struct {
	Consented        bool     `json:"consented"`
	ConsentedDomains []string `json:"consented_domains"`
	OriginMemoryIDs  []string `json:"origin_memory_ids"`
	NextCursor       string   `json:"next_cursor,omitempty"`
}
