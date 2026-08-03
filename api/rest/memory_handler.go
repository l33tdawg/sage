package rest

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/l33tdawg/sage/api/rest/middleware"
	"github.com/l33tdawg/sage/internal/authzdenial"
	"github.com/l33tdawg/sage/internal/federation"
	"github.com/l33tdawg/sage/internal/idfmt"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/metrics"
	"github.com/l33tdawg/sage/internal/store"
	memorytags "github.com/l33tdawg/sage/internal/tags"
	"github.com/l33tdawg/sage/internal/taskidempotency"
	"github.com/l33tdawg/sage/internal/tx"
)

// --- Request / Response types ------------------------------------------------

// SubmitMemoryRequest is the JSON body for POST /v1/memory/submit.
type SubmitMemoryRequest struct {
	Content          string                   `json:"content"`
	MemoryType       string                   `json:"memory_type"`
	DomainTag        string                   `json:"domain_tag"`
	Provider         string                   `json:"provider,omitempty"`
	ConfidenceScore  float64                  `json:"confidence_score"`
	Classification   int                      `json:"classification,omitempty"`
	Embedding        []float32                `json:"embedding,omitempty"`
	KnowledgeTriples []memory.KnowledgeTriple `json:"knowledge_triples,omitempty"`
	ParentHash       string                   `json:"parent_hash,omitempty"`
	TaskStatus       string                   `json:"task_status,omitempty"`
	LinkedMemories   []string                 `json:"linked_memories,omitempty"`
	// Tags remain node-local for ordinary domains. Above app-v20 they are also
	// carried canonically by the transaction so scoped domains can recover the
	// same query projection on every validator and after state sync.
	Tags []string `json:"tags,omitempty"`
	// IdempotencyKey is available for app-v23 task submissions. It is covered
	// by the caller's signed request and consensus-bound to the exact task and
	// assignee.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
}

// SubmitMemoryResponse is the JSON body for a successful submission.
type SubmitMemoryResponse struct {
	MemoryID            string `json:"memory_id"`
	TxHash              string `json:"tx_hash"`
	Status              string `json:"status"`
	TaskStatus          string `json:"task_status,omitempty"`
	Committed           bool   `json:"committed"`
	CommittedHeight     int64  `json:"committed_height"`
	ProjectionConfirmed *bool  `json:"projection_confirmed,omitempty"`
	Retryable           *bool  `json:"retryable,omitempty"`
	Message             string `json:"message,omitempty"`
	IdempotencyKey      string `json:"idempotency_key,omitempty"`
	IdempotentReplay    bool   `json:"idempotent_replay,omitempty"`
	EmbeddingProvider   string `json:"embedding_provider,omitempty"`
	EmbeddingQueued     bool   `json:"embedding_queued,omitempty"`
}

// QueryMemoryRequest is the JSON body for POST /v1/memory/query.
type QueryMemoryRequest struct {
	Embedding         []float32 `json:"embedding"`
	EmbeddingProvider string    `json:"embedding_provider,omitempty"`
	// Query is optional for local vector search, but lets federated semantic
	// recall ask peers for hybrid vector+text coverage. The text arm is the
	// deterministic fallback when two SAGEs use different embedding spaces.
	Query         string  `json:"query,omitempty"`
	DomainTag     string  `json:"domain_tag,omitempty"`
	Provider      string  `json:"provider,omitempty"`
	MinConfidence float64 `json:"min_confidence,omitempty"`
	StatusFilter  string  `json:"status_filter,omitempty"`
	TopK          int     `json:"top_k,omitempty"`
	Cursor        string  `json:"cursor,omitempty"`
	// Tags, when non-empty, restricts results to memories tagged with ANY
	// of the listed values (OR semantics) on both supported SQL backends.
	Tags []string `json:"tags,omitempty"`
	// Federated opts this recall into the v11 cross-network proxy: results
	// from every active cross_fed peer are merged in (read-only, stamped with
	// source_chain_id). FederateChains narrows the fan-out to named chains
	// ("*" = all active). Both default off — local behaviour is unchanged.
	Federated      bool     `json:"federated,omitempty"`
	FederateChains []string `json:"federate_chains,omitempty"`
	// FederationContext is mandatory for app-v23 federated recall. Because it
	// lives inside the caller-signed REST body, a peer operator cannot replay
	// the proof from another source chain or after a destination policy rotates.
	FederationContext *FederatedRecallProofContext `json:"federation_context,omitempty"`
}

type FederatedRecallProofContext struct {
	SourceChainID     string            `json:"source_chain_id"`
	AgreementBindings map[string]string `json:"agreement_bindings"`
	QueryChallenges   map[string]string `json:"query_challenges"`
}

func federationPlanFields(ctx *FederatedRecallProofContext) (map[string]string, map[string]string) {
	if ctx == nil {
		return nil, nil
	}
	return ctx.AgreementBindings, ctx.QueryChallenges
}

func (s *Server) requireFederatedEmbeddingProvider(
	federated bool,
	chains []string,
	embedding []float32,
	requested string,
) error {
	if !s.isPostV23ForNextTx() || (!federated && len(chains) == 0) || len(embedding) == 0 {
		return nil
	}
	active := s.activeEmbeddingProvider()
	if requested == "" {
		return errors.New("embedding_provider is required for app-v23 federated vector recall")
	}
	if active == "" || requested != active {
		return errors.New("embedding_provider does not match this node's active embedding vector space")
	}
	return nil
}

// recallMemoryClassification preserves the historical best-effort behavior
// before app-v23, but once clearance is consensus-enforced it fails closed on
// corrupt/unreadable classification state. Treating an error as PUBLIC (the
// uint8 zero value) would disclose content above the caller's clearance.
func (s *Server) recallMemoryClassification(memoryID string) (uint8, error) {
	if s.badgerStore == nil {
		return 0, nil
	}
	classification, err := s.badgerStore.GetMemoryClassification(memoryID)
	if err != nil && s.isPostV23ForNextTx() {
		return 0, err
	}
	return classification, nil
}

// QueryMemoryResponse is the JSON body for a successful query.
type QueryMemoryResponse struct {
	Results    []*MemoryResult `json:"results"`
	NextCursor string          `json:"next_cursor,omitempty"`
	TotalCount int             `json:"total_count"`
	Filtered   *FilterInfo     `json:"filtered,omitempty"`
	// Federation discloses the cross-network fan-out when a federated recall
	// ran: which peers were queried and which failed (fail-closed peers are
	// reported, never silently dropped).
	Federation *FederationInfo `json:"federation,omitempty"`
}

// FederationInfo discloses the outcome of a federated recall fan-out.
type FederationInfo struct {
	Queried  []string             `json:"queried"`
	Merged   int                  `json:"merged"`
	Errors   map[string]string    `json:"errors,omitempty"`
	Coverage []FederationCoverage `json:"coverage,omitempty"`
}

// FederationCoverage makes partial/fallback search explicit instead of
// allowing an empty peer contribution to look authoritative.
type FederationCoverage struct {
	ChainID    string `json:"chain_id"`
	Status     string `json:"status"`
	SearchMode string `json:"search_mode"`
	Matched    int    `json:"matched,omitempty"`
	Included   int    `json:"included,omitempty"`
	Fallback   string `json:"fallback,omitempty"`
}

// FilterInfo surfaces server-side filters that hid data from the caller.
// Populated when any silent-hide filter ran so clients can distinguish
// "empty result" from "access-limited result" without guessing.
// See X-SAGE-Filter-Applied response header for the same info in header form.
type FilterInfo struct {
	By                []string `json:"by"`
	TotalBeforeFilter *int     `json:"total_before_filter,omitempty"`
	Visible           *int     `json:"visible,omitempty"`
	HiddenCount       *int     `json:"hidden_count,omitempty"`
}

const (
	filterHeader           = "X-SAGE-Filter-Applied"
	filterBySubmittingAgts = "rbac_submitting_agents"
	filterByClassification = "classification"
)

// MemoryResult is a memory record with computed confidence.
type MemoryResult struct {
	MemoryID        string `json:"memory_id"`
	SubmittingAgent string `json:"submitting_agent"`
	Content         string `json:"content"`
	ContentHash     string `json:"content_hash"`
	MemoryType      string `json:"memory_type"`
	DomainTag       string `json:"domain_tag"`
	// ConfidenceScore is the DECAYED confidence: the stored value after time decay
	// and the corroboration boost, computed at read time. This is the number the
	// min_confidence floor is enforced against (rest-api.md).
	ConfidenceScore float64 `json:"confidence_score"`
	// InitialConfidence is the STORED (undecayed) confidence — the on-chain value
	// set at submission (corroboration never rewrites it), before any time decay. Exposing it
	// alongside the decayed ConfidenceScore lets a reader see the authoritative
	// floor without re-deriving it (the two diverge as a memory ages). A pointer so
	// a legitimate stored 0.0 still serializes (a plain omitempty float would drop
	// it); non-nil for local memories, nil for federated results, where only the
	// serving peer's already-decayed value is available. (v11.2.0)
	InitialConfidence *float64 `json:"initial_confidence,omitempty"`
	// CorroborationCount is the number of distinct corroborations backing this
	// memory — the multiplier behind the corroboration boost in ConfidenceScore.
	// Exposing it lets readers distinguish a low score caused by no corroboration
	// (a fresh, untested belief) from one caused by time decay (a once-solid fact).
	CorroborationCount int `json:"corroboration_count"`
	// ChallengeCount is the number of distinct challengers in the off-chain audit
	// projection for this memory. It is a lifetime evidence count; Status and
	// Disputed remain authoritative for whether a challenge is currently open.
	ChallengeCount int `json:"challenge_count"`
	// EvidenceCountsAvailable is true only when both count queries succeeded and
	// no durable recovery/repair-incomplete marker was detected. Current servers
	// always emit this field.
	EvidenceCountsAvailable bool `json:"evidence_counts_available"`
	// App-v21 open-round progress is authoritative Badger state. These fields are
	// absent when there is no currently-open weighted challenge round.
	ChallengeRound         *uint64 `json:"challenge_round,omitempty"`
	CurrentChallengerCount *uint32 `json:"current_challenger_count,omitempty"`
	RequiredChallengers    *uint32 `json:"required_challengers,omitempty"`
	Classification         int     `json:"classification"`
	Status                 string  `json:"status"`
	// Disputed is true for an app-v17/app-v21 CHALLENGED memory: still live and
	// recallable, but under dispute pending confirm/reinstate. Off-chain surface
	// signal only — the on-chain status is carried in Status ("challenged"). When
	// set, ConfidenceScore already reflects the disputed haircut. Personal nodes
	// never produce this (a challenge there is one-strike deprecate).
	Disputed    bool       `json:"disputed,omitempty"`
	ParentHash  string     `json:"parent_hash,omitempty"`
	TaskStatus  string     `json:"task_status,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
	CommittedAt *time.Time `json:"committed_at,omitempty"`
	// SourceChainID is v11 federation provenance: empty for local memories,
	// the origin chain for both borrowed live results and retained synced
	// copies. SourceKind distinguishes those two cases.
	SourceChainID  string `json:"source_chain_id,omitempty"`
	SourceKind     string `json:"source_kind,omitempty"` // local_native | federated_live | federated_copy
	OriginMemoryID string `json:"origin_memory_id,omitempty"`
	OriginAgentID  string `json:"origin_agent_id,omitempty"`
	Foreign        bool   `json:"foreign,omitempty"`
	// Foreign federation content is authenticated and policy-filtered, but it
	// remains external input to the consuming agent.
	Trust string `json:"trust,omitempty"`
}

// MemoryDetailResponse is a memory record with votes and corroborations.
type MemoryDetailResponse struct {
	MemoryID                string                  `json:"memory_id"`
	SubmittingAgent         string                  `json:"submitting_agent"`
	Content                 string                  `json:"content"`
	ContentHash             string                  `json:"content_hash"`
	MemoryType              string                  `json:"memory_type"`
	DomainTag               string                  `json:"domain_tag"`
	ConfidenceScore         float64                 `json:"confidence_score"`
	Classification          int                     `json:"classification"`
	Status                  string                  `json:"status"`
	ParentHash              string                  `json:"parent_hash,omitempty"`
	TaskStatus              string                  `json:"task_status,omitempty"`
	CreatedAt               time.Time               `json:"created_at"`
	CommittedAt             *time.Time              `json:"committed_at,omitempty"`
	Votes                   []*store.ValidationVote `json:"votes,omitempty"`
	Corroborations          []*store.Corroboration  `json:"corroborations,omitempty"`
	CorroborationCount      int                     `json:"corroboration_count"`
	ChallengeCount          int                     `json:"challenge_count"`
	EvidenceCountsAvailable bool                    `json:"evidence_counts_available"`
	ChallengeRound          *uint64                 `json:"challenge_round,omitempty"`
	CurrentChallengerCount  *uint32                 `json:"current_challenger_count,omitempty"`
	RequiredChallengers     *uint32                 `json:"required_challengers,omitempty"`
	LinkedMemories          []memory.MemoryLink     `json:"linked_memories,omitempty"`
}

// CometBFT broadcast_tx_commit response structure.
// Unlike broadcast_tx_sync, this waits for the block to be finalized,
// ensuring ABCI Commit has flushed writes before we return.
type cometCommitResponse struct {
	Result struct {
		CheckTx struct {
			Code int    `json:"code"`
			Log  string `json:"log"`
		} `json:"check_tx"`
		TxResult struct {
			Code int    `json:"code"`
			Data string `json:"data"`
			Log  string `json:"log"`
		} `json:"tx_result"`
		Hash   string `json:"hash"`
		Height int64  `json:"height,string"`
	} `json:"result"`
	Error *struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
		// Data carries the actionable detail for mempool rejections —
		// CometBFT returns message="Internal error" with the real cause
		// ("mempool is full: number of txs N (max: M)") in error.data.
		Data string `json:"data"`
	} `json:"error"`
}

// --- Domain Access Enforcement -----------------------------------------------

// checkDomainAccess verifies an agent has the required access level for a domain.
// Returns nil if allowed, descriptive error if denied.
// Unregistered agents (not in network_agents) are always allowed for backwards compatibility.
// Admins bypass all checks. Observers cannot write.
func checkDomainAccess(ctx context.Context, agentStore store.AgentStore, badgerStore *store.BadgerStore, agentID, domain, action string) error {
	return checkDomainAccessWithCapabilities(ctx, agentStore, badgerStore, agentID, domain, action, false)
}

func checkDomainAccessWithCapabilities(ctx context.Context, agentStore store.AgentStore, badgerStore *store.BadgerStore, agentID, domain, action string, capabilitiesActive bool) error {
	if agentID == "" {
		return nil // No agent identity — allow
	}

	// Check on-chain state first (if BadgerDB available)
	if badgerStore != nil {
		if capabilitiesActive {
			if _, _, err := badgerStore.GetRegisteredAgentCapabilities(agentID); err != nil {
				return fmt.Errorf("agent capability policy is invalid: %w", err)
			}
		}
		onChainAgent, err := badgerStore.GetRegisteredAgent(agentID)
		if err == nil && onChainAgent != nil {
			// Use on-chain clearance and domain access
			if onChainAgent.Role == "admin" {
				return nil
			}
			if action == "write" && onChainAgent.Role == "observer" {
				return fmt.Errorf("observer agents cannot submit memories")
			}
			if capabilitiesActive && action == "read" && onChainAgent.Capabilities.Has(store.AgentCapabilityReadAllDomains) {
				return nil
			}
			if onChainAgent.DomainAccess != "" {
				var access []struct {
					Domain string `json:"domain"`
					Read   bool   `json:"read"`
					Write  bool   `json:"write"`
					Modify bool   `json:"modify"`
				}
				if err := json.Unmarshal([]byte(onChainAgent.DomainAccess), &access); err != nil {
					return fmt.Errorf("agent domain access policy is invalid")
				}
				if len(access) > 0 {
					for _, a := range access {
						if federation.DomainAllowed([]string{a.Domain}, domain) {
							if action == "read" && (a.Read || a.Modify) {
								return nil
							}
							if action == "write" && (a.Write || a.Modify) {
								return nil
							}
							if action == "modify" && a.Modify {
								return nil
							}
							return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
						}
					}
					return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
				}
			}
			// On-chain agent with no domain access restrictions — allow
			return nil
		}
	}

	// Fallback to SQLite agent store
	if agentStore == nil {
		return nil // No agent store — allow
	}

	agent, err := agentStore.GetAgent(ctx, agentID)
	if err != nil {
		return nil // Agent not registered in network_agents — backwards compat
	}

	if agent.Role == "admin" {
		return nil // Admins have full access
	}

	if action == "write" && agent.Role == "observer" {
		return fmt.Errorf("observer agents cannot submit memories")
	}

	// Parse domain_access JSON: [{"domain":"x","read":true,"write":false}, ...]
	if agent.DomainAccess == "" {
		return nil // No restrictions configured — allow all
	}

	var access []struct {
		Domain string `json:"domain"`
		Read   bool   `json:"read"`
		Write  bool   `json:"write"`
		Modify bool   `json:"modify"`
	}
	if err := json.Unmarshal([]byte(agent.DomainAccess), &access); err != nil {
		return fmt.Errorf("agent domain access policy is invalid")
	}

	if len(access) == 0 {
		return nil // Empty list means no restrictions
	}

	for _, a := range access {
		if federation.DomainAllowed([]string{a.Domain}, domain) {
			if action == "read" && (a.Read || a.Modify) {
				return nil
			}
			if action == "write" && (a.Write || a.Modify) {
				return nil
			}
			if action == "modify" && a.Modify {
				return nil
			}
			return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
		}
	}

	// Domain not in the access list — deny (explicit allowlist model)
	return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
}

func (s *Server) checkDomainAccess(ctx context.Context, agentID, domain, action string) error {
	if s.isPostV23ForNextTx() {
		currentErr := checkAppV23DomainAccess(s.badgerStore, agentID, domain, action)
		if action != "read" || s.badgerStore == nil {
			return currentErr
		}
		policyID, err := appV23PolicyPrincipal(s.badgerStore, agentID)
		if err != nil {
			return errors.New("app-v23 access-control state is invalid")
		}
		legacy, err := s.badgerStore.AppV23LegacyReadCompatibility(
			policyID, domain, 0, time.Now(),
		)
		if err != nil {
			recovered, recoveredErr := s.badgerStore.AuthorizeAppV25RecoveredDirectRead(
				agentID, domain,
			)
			if recoveredErr == nil && recovered {
				return nil
			}
			return errors.New("app-v23 access-control state is invalid")
		}
		// The frozen H-1 allowlist remains authoritative over ordinary grants,
		// but it must not make a directly governed current owner, recovered-group
		// member, or Admin write-only after recovery/transfer.
		if legacy.ExplicitDomainRestriction && !legacy.Allowed {
			recovered, recoveredErr := s.badgerStore.AuthorizeAppV25RecoveredDirectRead(
				agentID, domain,
			)
			if recoveredErr != nil || !recovered {
				return fmt.Errorf("agent does not have read access to domain '%s'", domain)
			}
			return nil
		}
		if currentErr == nil || legacy.Allowed {
			return nil
		}
		return currentErr
	}
	return checkDomainAccessWithCapabilities(
		ctx, s.agentStore, s.badgerStore, agentID, domain, action, s.isPostV22ForNextTx(),
	)
}

const domainReadDeniedProblemType = "https://sage.dev/errors/domain-read-denied"

// writeDomainReadAccessError keeps a policy denial machine-distinguishable
// from every other 403. The MCP turn path uses this exact problem type only to
// defer the harmless first-read denial for a domain that does not exist until
// the same turn's first write commits. A broken policy backend must remain a
// retryable service failure; otherwise a successful later write could hide a
// genuine authorization-state outage behind the first-use compatibility path.
func writeDomainReadAccessError(w http.ResponseWriter, accessErr error) {
	detail := "Current domain read authority could not be resolved."
	if accessErr != nil {
		detail = accessErr.Error()
	}
	if strings.Contains(detail, "access-control state is invalid") ||
		strings.Contains(detail, "access-control state is unavailable") {
		writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable", detail)
		return
	}
	writeProblemTyped(
		w,
		http.StatusForbidden,
		domainReadDeniedProblemType,
		"Access denied",
		detail,
	)
}

// appV23PolicyPrincipal maps only the current Root credential to its immutable
// policy principal and rejects the retired Root credential. Callers still
// retain the original authenticated credential for signature provenance and
// for AuthorizeAppV23LocalDomain.
func appV23PolicyPrincipal(badgerStore *store.BadgerStore, credentialID string) (string, error) {
	if badgerStore == nil || credentialID == "" {
		return "", errors.New("app-v23 access-control state is unavailable")
	}
	root, err := badgerStore.GetAppV23Root()
	if err != nil {
		return "", err
	}
	if root != nil && credentialID == root.CredentialID {
		return root.PrincipalID, nil
	}
	if root != nil {
		wasRoot, markerErr := badgerStore.IsAppV23RootCredential(credentialID)
		if markerErr != nil {
			return "", markerErr
		}
		if wasRoot {
			return "", errors.New("authenticated credential is a retired Root credential")
		}
	}
	return credentialID, nil
}

// checkAppV23DomainAccess is the shared REST-side projection of app-v23 local
// data authority. Consensus remains authoritative for mutations; this helper
// prevents read endpoints and early write diagnostics from silently falling
// back to the historical empty-allowlist-means-everything behavior.
//
// Access Groups and ordinary grants are a union. Hard profile restrictions are
// evaluated first by AuthorizeAppV23LocalDomain and cannot be bypassed by a
// level-2/3 grant.
func checkAppV23DomainAccess(badgerStore *store.BadgerStore, agentID, domain, action string) error {
	if badgerStore == nil || agentID == "" || domain == "" {
		return errors.New("app-v23 access-control state is unavailable")
	}
	var verb store.AppV23DomainVerb
	var grantLevel uint8
	switch action {
	case "read":
		verb, grantLevel = store.AppV23VerbRead, 1
	case "write":
		verb, grantLevel = store.AppV23VerbWrite, 2
	case "modify":
		verb, grantLevel = store.AppV23VerbModify, 3
	default:
		return fmt.Errorf("unsupported app-v23 domain action %q", action)
	}
	if action == "write" {
		return checkAppV23EffectiveWriteAccess(badgerStore, agentID, domain, time.Now())
	}
	policyID, err := appV23PolicyPrincipal(badgerStore, agentID)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	enrollment, err := badgerStore.GetAppV23Enrollment(policyID)
	if err != nil || enrollment == nil || !enrollment.Active {
		return errors.New("agent enrollment is pending local review")
	}
	shared, err := badgerStore.IsAppV23SharedDomain(domain)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	decision, err := badgerStore.AuthorizeAppV23LocalDomain(
		agentID, domain, verb, shared,
	)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if decision.ExplicitDeny {
		return fmt.Errorf("the agent's named security profile denies %s access", action)
	}
	if decision.Allowed {
		return nil
	}
	hasGrant, err := badgerStore.HasAppV23AccessOrAncestor(
		domain, agentID, grantLevel, time.Now(), shared,
	)
	if err == nil && hasGrant {
		return nil
	}
	return fmt.Errorf("agent does not have %s access to domain '%s'", action, domain)
}

func appV23WriteDenial(code authzdenial.Code) error {
	if _, ok := authzdenial.Definition(code); !ok {
		return errors.New("app-v23 access-control state is invalid")
	}
	return fmt.Errorf("access denied: denial_code=%s", code)
}

// appV23OmittedTaskDomain resolves only the narrow app-v23 task convenience:
// an active ordinary agent may omit domain_tag and let the node select the
// home domain already committed in its enrollment. Explicit domains are never
// rewritten, and consensus independently derives the same value from the
// signed request plus AppHash-covered enrollment state.
func appV23OmittedTaskDomain(
	badgerStore *store.BadgerStore,
	agentID string,
) (string, error) {
	if badgerStore == nil || agentID == "" {
		return "", errors.New("app-v23 access-control state is unavailable")
	}
	enrollment, err := badgerStore.GetAppV23Enrollment(agentID)
	if err != nil {
		return "", errors.New("app-v23 access-control state is invalid")
	}
	if enrollment == nil || !enrollment.Active {
		return "", appV23WriteDenial(authzdenial.CodePrincipalPendingReview)
	}
	if enrollment.HomeDomain == "" {
		return "", appV23WriteDenial(authzdenial.CodeNoOwnedHomeDomain)
	}
	return enrollment.HomeDomain, nil
}

// checkAppV23EffectiveWriteAccess mirrors the app-v23 consensus write
// decision so REST preflight returns the same stable seven-code denial
// taxonomy. It is advisory only; FinalizeBlock remains authoritative and
// re-evaluates the decision against the committed state.
func checkAppV23EffectiveWriteAccess(
	badgerStore *store.BadgerStore,
	credentialID, domain string,
	at time.Time,
) error {
	root, err := badgerStore.GetAppV23Root()
	if err != nil || root == nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	policyID, err := appV23PolicyPrincipal(badgerStore, credentialID)
	if err != nil {
		return appV23WriteDenial(authzdenial.CodePrincipalPendingReview)
	}
	enrollment, err := badgerStore.GetAppV23Enrollment(policyID)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if enrollment == nil || !enrollment.Active {
		return appV23WriteDenial(authzdenial.CodePrincipalPendingReview)
	}
	role, err := badgerStore.GetAppV23Role(policyID)
	if err != nil || role == nil ||
		store.ValidateAppV23Policy(
			role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
		) != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if policyID != root.PrincipalID && role.Role == store.AppV23RoleAdmin &&
		enrollment.RootGeneration != root.Generation {
		return appV23WriteDenial(authzdenial.CodePrincipalPendingReview)
	}
	if enrollment.Profile == store.AppV23ProfileReadOnly {
		return appV23WriteDenial(authzdenial.CodeForeignWriteRestricted)
	}
	if credentialID != root.CredentialID &&
		(enrollment.HomeDomain != "" ||
			!store.AppV23AllowsMigratedDomainless(
				enrollment.Profile, enrollment.Capabilities,
			)) {
		homeShared, sharedErr := badgerStore.IsAppV23SharedDomain(enrollment.HomeDomain)
		homeOwner, _, ownerErr := badgerStore.ResolveAppV23OwningAncestor(enrollment.HomeDomain)
		if enrollment.HomeDomain == "" || sharedErr != nil || homeShared ||
			ownerErr != nil || homeOwner != credentialID {
			return appV23WriteDenial(authzdenial.CodeNoOwnedHomeDomain)
		}
	}
	restored, err := badgerStore.AppV25AllowsHistoricalDomainWrite(
		policyID,
		domain,
	)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if restored {
		// Match the consensus app-v25 compatibility decision exactly. The
		// entitlement is exact-domain, validator-attested, and bound to the
		// unchanged enrollment revision/current ownership or recovered group,
		// so it bypasses only the historical mask-2/mask-8 migration lockout.
		return nil
	}

	shared, err := badgerStore.IsAppV23SharedDomain(domain)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if shared && enrollment.Capabilities.Has(store.AgentCapabilityDenySharedDomainWrite) {
		return appV23WriteDenial(authzdenial.CodeSharedWriteRestricted)
	}
	if shared {
		if role.Role == store.AppV23RoleAdmin {
			return nil
		}
		recoveredGroup, recoveredGroupErr :=
			badgerStore.AuthorizeAppV25RecoveredGroupDomain(policyID, domain, store.AppV23VerbWrite)
		if recoveredGroupErr != nil {
			return errors.New("app-v23 access-control state is invalid")
		}
		if recoveredGroup {
			return nil
		}
		grandfathered, grandfatherErr :=
			badgerStore.AppV23AllowsGrandfatheredSharedDomainWrite(policyID, domain)
		if grandfatherErr != nil {
			return errors.New("app-v23 access-control state is invalid")
		}
		if grandfathered {
			return nil
		}
		hasGrant, grantErr := badgerStore.HasAppV23AccessOrAncestor(
			domain, credentialID, 2, at, true,
		)
		if grantErr != nil {
			return errors.New("app-v23 access-control state is invalid")
		}
		if hasGrant {
			return nil
		}
		decision, decisionErr := badgerStore.AuthorizeAppV23LocalDomain(
			credentialID, domain, store.AppV23VerbWrite, true,
		)
		if decisionErr != nil {
			return errors.New("app-v23 access-control state is invalid")
		}
		if decision.ExplicitDeny {
			return errors.New("app-v23 access-control state is invalid")
		}
		if decision.Allowed {
			return nil
		}
		if role.Role == store.AppV23RoleManager {
			return appV23WriteDenial(authzdenial.CodeManagerScopeDenied)
		}
		return appV23WriteDenial(authzdenial.CodeMissingWriteGrant)
	}

	owner, _, err := badgerStore.ResolveAppV23OwningAncestor(domain)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if owner == "" {
		if enrollment.Capabilities.Has(store.AgentCapabilityDenyDomainClaim) {
			return appV23WriteDenial(authzdenial.CodeDomainClaimRestricted)
		}
		if role.Role == store.AppV23RoleAdmin {
			return nil
		}
		if (enrollment.Profile == store.AppV23ProfileStandard ||
			enrollment.Profile == store.AppV23ProfileLegacyRestricted) &&
			(role.Role == store.AppV23RoleMember ||
				role.Role == store.AppV23RoleManager) {
			return nil
		}
		return appV23WriteDenial(authzdenial.CodeDomainClaimRestricted)
	}
	if owner == credentialID || role.Role == store.AppV23RoleAdmin {
		return nil
	}
	if enrollment.Capabilities.Has(store.AgentCapabilityDenyForeignDomainWrite) {
		return appV23WriteDenial(authzdenial.CodeForeignWriteRestricted)
	}
	hasGrant, err := badgerStore.HasAppV23AccessOrAncestor(
		domain, credentialID, 2, at, false,
	)
	if err != nil {
		return errors.New("app-v23 access-control state is invalid")
	}
	if hasGrant {
		return nil
	}
	decision, err := badgerStore.AuthorizeAppV23LocalDomain(
		credentialID, domain, store.AppV23VerbWrite, false,
	)
	if err != nil || decision.ExplicitDeny {
		return errors.New("app-v23 access-control state is invalid")
	}
	if decision.Allowed {
		return nil
	}
	if role.Role == store.AppV23RoleManager {
		return appV23WriteDenial(authzdenial.CodeManagerScopeDenied)
	}
	return appV23WriteDenial(authzdenial.CodeMissingWriteGrant)
}

// --- Agent Isolation (RBAC) --------------------------------------------------

// resolveVisibleAgents determines which agents' memories the given agent can see.
// Returns (allowedAgentIDs, seeAll). If seeAll is true, no filtering needed.
// RBAC is on-chain: checks BadgerDB for role and visible_agents.
func (s *Server) resolveVisibleAgents(agentID string) ([]string, bool) {
	if agentID == "" {
		return nil, true // No identity = legacy/internal, allow all
	}

	// App-v23 visibility is domain-derived, not submitter-derived. Validate the
	// current credential against policy state before considering any historical
	// node-operator shortcut.
	if s.isPostV23ForNextTx() && s.badgerStore != nil {
		root, rootErr := s.badgerStore.GetAppV23Root()
		if rootErr != nil || root == nil ||
			(agentID == root.PrincipalID && root.CredentialID != root.PrincipalID) {
			return []string{agentID}, false
		}
		policyID, policyErr := appV23PolicyPrincipal(s.badgerStore, agentID)
		if policyErr != nil {
			return []string{agentID}, false
		}
		enrollment, err := s.badgerStore.GetAppV23Enrollment(policyID)
		if err != nil || enrollment == nil || !enrollment.Active {
			return []string{agentID}, false
		}
		role, err := s.badgerStore.GetAppV23Role(policyID)
		if err != nil || role == nil ||
			store.ValidateAppV23Policy(
				role.Role, enrollment.Profile, enrollment.Capabilities, enrollment.Clearance,
			) != nil ||
			(policyID != root.PrincipalID && role.Role == store.AppV23RoleAdmin &&
				enrollment.RootGeneration != root.Generation) {
			return []string{agentID}, false
		}
		visibleAgents, restricted, visibilityErr :=
			s.badgerStore.AppV23LegacyVisibleAgents(policyID)
		if visibilityErr != nil {
			return []string{agentID}, false
		}
		if restricted {
			if visibleAgents == "*" {
				return nil, true
			}
			var list []string
			if json.Unmarshal([]byte(visibleAgents), &list) != nil {
				return []string{agentID}, false
			}
			allowed := make([]string, 1, 1+len(list))
			allowed[0] = agentID
			allowed = append(allowed, list...)
			return allowed, false
		}
		return nil, true
	}

	// v7.1: the node operator (whoever signs with ~/.sage/agent.key) bypasses
	// the cross-agent visibility filter. The operator owns the node, so the
	// agent-isolation gate doesn't apply to them — only multi-agent peer
	// visibility does. Lifts the v7.0 SessionStart-hook prefetch limit on
	// nodes where the LLM identity is registered separately from the
	// operator. Per-domain access and classification gates still run.
	if s.nodeOperatorID != "" && agentID == s.nodeOperatorID {
		return nil, true
	}

	// Resolve visible_agents from the best available source.
	// On-chain (BadgerDB) is checked first, then SQLite as fallback
	// since dashboard writes may not have been broadcast to chain yet.
	var role, visibleAgents string
	var capabilities store.AgentCapabilities

	if s.badgerStore != nil {
		agent, err := s.badgerStore.GetRegisteredAgent(agentID)
		if err == nil && agent != nil {
			if s.isPostV22ForNextTx() && !agent.Capabilities.Valid() {
				return []string{agentID}, false
			}
			role = agent.Role
			visibleAgents = agent.VisibleAgents
			capabilities = agent.Capabilities
		} else if s.isPostV22ForNextTx() {
			if _, _, capabilityErr := s.badgerStore.GetRegisteredAgentCapabilities(agentID); capabilityErr != nil {
				return []string{agentID}, false
			}
		}
	}

	// Fallback to SQLite if on-chain state has no visible_agents
	if visibleAgents == "" && s.agentStore != nil {
		ctx := context.Background()
		if sqlAgent, err := s.agentStore.GetAgent(ctx, agentID); err == nil && sqlAgent != nil {
			if role == "" {
				role = sqlAgent.Role
			}
			visibleAgents = sqlAgent.VisibleAgents
		}
	}

	if role == "admin" {
		return nil, true
	}
	if s.isPostV22ForNextTx() && capabilities.Has(store.AgentCapabilityReadAllDomains) {
		return nil, true
	}
	if visibleAgents == "*" {
		return nil, true
	}

	// Org-clearance-as-seeAll: a TopSecret member of any org is trusted to
	// see across agents within their org's visibility envelope. Per-domain
	// access control (HasAccessMultiOrg, checkDomainAccess) and per-record
	// classification gates still apply - this only lifts the submitting_agents
	// filter, not the domain-access filters. In single-org deployments this
	// closes the "visible_agents=\"*\" on every agent" boilerplate.
	if s.badgerStore != nil && s.agentHasTopSecretClearance(agentID) {
		return nil, true
	}

	allowed := []string{agentID} // Always see own
	if visibleAgents != "" {
		var list []string
		if json.Unmarshal([]byte(visibleAgents), &list) == nil {
			allowed = append(allowed, list...)
		}
	}
	return allowed, false
}

func (s *Server) appV23LegacyVisibilityRestricted(agentID string) bool {
	if !s.isPostV23ForNextTx() || s.badgerStore == nil || agentID == "" {
		return false
	}
	policyID, err := appV23PolicyPrincipal(s.badgerStore, agentID)
	if err != nil {
		return true
	}
	_, restricted, err := s.badgerStore.AppV23LegacyVisibleAgents(policyID)
	return err != nil || restricted
}

func (s *Server) appV25RecoveredReadOverridesLegacyVisibility(agentID, domain string) bool {
	if !s.isPostV23ForNextTx() || s.badgerStore == nil || agentID == "" || domain == "" {
		return false
	}
	allowed, err := s.badgerStore.AuthorizeAppV25RecoveredDirectRead(agentID, domain)
	return err == nil && allowed
}

// hasMemoryReadAccess applies app-v22's domain-independent read capability
// without turning it into a clearance bypass. A companion may discover and
// recall every domain, but records above its on-chain clearance remain hidden.
func (s *Server) hasMemoryReadAccess(domain, agentID string, classification uint8, at time.Time) (bool, error) {
	if s.isPostV23ForNextTx() && s.badgerStore != nil {
		policyID, policyErr := appV23PolicyPrincipal(s.badgerStore, agentID)
		if policyErr != nil {
			return false, policyErr
		}
		enrollment, err := s.badgerStore.GetAppV23Enrollment(policyID)
		if err != nil || enrollment == nil || !enrollment.Active {
			return false, nil
		}
		legacy, err := s.badgerStore.AppV23LegacyReadCompatibility(
			policyID, domain, classification, at,
		)
		if err != nil {
			return false, err
		}
		if classification > enrollment.Clearance {
			// Current app-v23 authority remains bounded by enrollment
			// clearance. The migration-only org/federation path may instead
			// rely on its immutable per-membership clearance, exactly as v22
			// did, so do not discard a baseline-approved result here.
			return legacy.Allowed, nil
		}
		shared, err := s.badgerStore.IsAppV23SharedDomain(domain)
		if err != nil {
			return false, err
		}
		decision, err := s.badgerStore.AuthorizeAppV23LocalDomain(
			agentID, domain, store.AppV23VerbRead, shared,
		)
		if err != nil {
			return false, err
		}
		if decision.ExplicitDeny {
			return false, nil
		}
		policyAllowed := decision.Allowed
		grantAllowed := false
		if !policyAllowed {
			grantAllowed, err = s.badgerStore.HasAppV23AccessOrAncestor(
				domain, agentID, 1, at, shared,
			)
			if err != nil {
				return false, err
			}
		}
		if legacy.ExplicitDomainRestriction && !legacy.Allowed {
			return false, nil
		}
		return policyAllowed || grantAllowed || legacy.Allowed, nil
	}
	if s.isPostV22ForNextTx() && s.badgerStore != nil {
		if _, _, err := s.badgerStore.GetRegisteredAgentCapabilities(agentID); err != nil {
			return false, fmt.Errorf("agent capability policy is invalid: %w", err)
		}
		agent, err := s.badgerStore.GetRegisteredAgent(agentID)
		if err == nil && agent != nil && agent.Capabilities.Has(store.AgentCapabilityReadAllDomains) {
			return classification <= agent.Clearance, nil
		}
	}
	return s.badgerStore.HasAccessMultiOrgWithFederationPolicy(
		domain, agentID, classification, at,
		s.isPostV8Fork(), s.isPostV22ForNextTx(),
	)
}

// agentHasTopSecretClearance reports whether the agent is a TopSecret-cleared
// member of any org they belong to. Used to lift the submitting_agents filter
// for trusted agents without forcing admins to configure visible_agents="*"
// per member. Iterates every org membership — TS in one org is enough.
func (s *Server) agentHasTopSecretClearance(agentID string) bool {
	if s.badgerStore == nil {
		return false
	}
	orgIDs, err := s.badgerStore.ListAgentOrgs(agentID)
	if err != nil || len(orgIDs) == 0 {
		return false
	}
	for _, orgID := range orgIDs {
		clearance, _, gerr := s.badgerStore.GetMemberClearance(orgID, agentID)
		if gerr != nil {
			continue
		}
		if clearance >= uint8(tx.ClearanceTopSecret) {
			return true
		}
	}
	return false
}

// --- Handlers ----------------------------------------------------------------

// handleSubmitMemory handles POST /v1/memory/submit.
func (s *Server) handleSubmitMemory(w http.ResponseWriter, r *http.Request) {
	var req SubmitMemoryRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Validate required fields.
	if req.Content == "" {
		writeProblem(w, http.StatusBadRequest, "Missing content", "content is required.")
		return
	}
	if !memory.IsValidMemoryType(memory.MemoryType(req.MemoryType)) {
		writeProblem(w, http.StatusBadRequest, "Invalid memory type",
			"memory_type must be one of: fact, observation, inference, task.")
		return
	}
	agentID := middleware.ContextAgentID(r.Context())
	if req.ConfidenceScore < 0 || req.ConfidenceScore > 1 {
		writeProblem(w, http.StatusBadRequest, "Invalid confidence score",
			"confidence_score must be between 0 and 1.")
		return
	}
	if req.IdempotencyKey != "" && req.MemoryType != string(memory.TypeTask) {
		writeProblem(w, http.StatusBadRequest, "Invalid idempotency key", "idempotency_key is supported only for task memories.")
		return
	}
	explicitTaskIdempotency := req.IdempotencyKey != ""
	if explicitTaskIdempotency {
		if validationErr := taskidempotency.ValidateKey(req.IdempotencyKey); validationErr != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid idempotency key", validationErr.Error())
			return
		}
		if !s.isPostV23ForNextTx() {
			writeProblemTyped(
				w,
				http.StatusConflict,
				"https://sage.dev/errors/app-v23-required",
				"App-v23 required",
				"Durable task idempotency requires app-v23. Upgrade the node before retrying with idempotency_key.",
			)
			return
		}
	}

	if req.MemoryType == string(memory.TypeTask) {
		if !s.requireAppV23ActiveOrdinaryAgent(
			w, agentID, "ordinary REST task submission",
		) {
			return
		}
		if req.TaskStatus == "" {
			req.TaskStatus = string(memory.TaskStatusPlanned)
		}
		if req.TaskStatus != string(memory.TaskStatusPlanned) {
			writeProblem(w, http.StatusBadRequest, "Invalid initial task status", "A new task must enter consensus as planned; its assigned agent may start it after creation.")
			return
		}
	}
	if req.DomainTag == "" {
		if req.MemoryType != string(memory.TypeTask) || !s.isPostV23ForNextTx() {
			writeProblem(w, http.StatusBadRequest, "Missing domain tag", "domain_tag is required.")
			return
		}
		req.DomainTag, err = appV23OmittedTaskDomain(s.badgerStore, agentID)
		if err != nil {
			if denial, ok := authzdenial.Classify(err); ok {
				writeEffectiveWriteDenial(w, denial)
			} else {
				writeProblem(w, http.StatusServiceUnavailable, "Access control unavailable",
					"The node cannot resolve the agent's approved home domain.")
			}
			return
		}
	}
	if req.MemoryType == string(memory.TypeTask) &&
		s.isPostV23ForNextTx() &&
		req.IdempotencyKey == "" {
		req.IdempotencyKey, err = taskidempotency.SemanticKey(
			agentID, req.DomainTag, req.Content,
		)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid task identity", err.Error())
			return
		}
	}
	if req.IdempotencyKey != "" {
		if !explicitTaskIdempotency {
			if validateErr := taskidempotency.ValidateKey(req.IdempotencyKey); validateErr != nil {
				writeProblem(w, http.StatusBadRequest, "Invalid idempotency key", validateErr.Error())
				return
			}
		}
		if len(req.KnowledgeTriples) > 0 || len(req.LinkedMemories) > 0 {
			writeProblem(w, http.StatusBadRequest, "Unsupported idempotent task payload", "knowledge_triples and linked_memories are not part of the canonical task transaction; submit links separately after task creation.")
			return
		}
	}

	// Enforce domain access policy from network_agents registry. This registry
	// denial is as permanent as the consensus-layer one, so it carries the same
	// problem type: an untyped 403 here is indistinguishable from the transient
	// one a restarted node emits, and MCP clients burn a full re-registration
	// and retry cycle against an ACL that will never yield.
	if accessErr := s.checkDomainAccess(r.Context(), agentID, req.DomainTag, "write"); accessErr != nil {
		if denial, ok := authzdenial.Classify(accessErr); ok {
			writeEffectiveWriteDenial(w, denial)
		} else {
			writeProblem(w, http.StatusForbidden, "Access denied", "memory write access denied")
		}
		return
	}

	// The node - not the agent - is authoritative for the active vector space.
	// Always regenerate from the current setting, even when a client supplied a
	// vector: a long-lived MCP process may have minted it before a provider switch.
	// If the active provider is temporarily unavailable, preserve the observation
	// without a vector; the repair worker backfills it in the same vector space.
	if s.embedder != nil {
		if emb, embedErr := s.embedder.Embed(r.Context(), req.Content); embedErr != nil {
			s.logger.Warn().Err(embedErr).Msg("active embedding provider unavailable; memory queued for repair")
			req.Embedding = nil
		} else {
			req.Embedding = emb
		}
	}
	embeddingProvider := s.embedderStampFor(req.Embedding)

	memoryID := generateUUID()
	taskPolicyPrincipal := ""
	if req.IdempotencyKey != "" && s.isPostV23ForNextTx() {
		taskPolicyPrincipal, err = appV23PolicyPrincipal(s.badgerStore, agentID)
		if err != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Task idempotency unavailable", "The node cannot resolve the caller's current policy principal.")
			return
		}
		memoryID, err = taskidempotency.MemoryID(taskPolicyPrincipal, req.IdempotencyKey)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid idempotency key", err.Error())
			return
		}
	}

	// Compute content hash.
	contentHash := sha256.Sum256([]byte(req.Content))

	// Compute embedding hash if provided.
	var embeddingHash []byte
	if len(req.Embedding) > 0 {
		h := sha256.New()
		for _, v := range req.Embedding {
			fmt.Fprintf(h, "%f", v)
		}
		embeddingHash = h.Sum(nil)
	}

	// Build the on-chain transaction. v6.8.6: REST passes the caller's
	// classification through verbatim — 0 means PUBLIC. The prior
	// 0→INTERNAL bump here turned an absent or explicitly-public submission
	// into INTERNAL on the wire, which then triggered the per-record
	// classification gate in handleQueryMemory for every cross-agent read.
	classification := req.Classification
	var consensusTags []string
	if s.isPostV20ForNextTx() {
		consensusTags, err = memorytags.Normalize(req.Tags)
		if err != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid tags", err.Error())
			return
		}
		req.Tags = consensusTags
	}

	submitTx := &tx.ParsedTx{
		Type:      tx.TxTypeMemorySubmit,
		Nonce:     tx.MonotonicNonce(s.signingKey),
		Timestamp: time.Now(),
		MemorySubmit: &tx.MemorySubmit{
			MemoryID:        memoryID,
			ContentHash:     contentHash[:],
			EmbeddingHash:   embeddingHash,
			MemoryType:      memoryTypeToTx(req.MemoryType),
			DomainTag:       req.DomainTag,
			ConfidenceScore: req.ConfidenceScore,
			Content:         req.Content,
			ParentHash:      req.ParentHash,
			Classification:  tx.ClearanceLevel(classification), // #nosec G115 -- validated small int
			TaskStatus:      req.TaskStatus,
			Tags:            consensusTags,
		},
	}
	taskPayloadDigest := ""
	if req.IdempotencyKey != "" && s.isPostV23ForNextTx() {
		payloadDigest, digestErr := taskidempotency.PayloadDigest(
			taskPolicyPrincipal, agentID, submitTx.MemorySubmit,
		)
		if digestErr != nil {
			writeProblem(w, http.StatusBadRequest, "Invalid idempotent task payload", digestErr.Error())
			return
		}
		taskPayloadDigest = taskidempotency.Hex(payloadDigest)
		releaseTaskIdempotency := s.acquireTaskIdempotencyLock(
			taskPolicyPrincipal, req.IdempotencyKey,
		)
		defer releaseTaskIdempotency()
		if s.writeTaskIdempotencyReplayIfCommitted(
			w, req, taskPolicyPrincipal, agentID, taskPayloadDigest,
		) {
			return
		}
	}
	if req.MemoryType == string(memory.TypeTask) &&
		s.isPostV23ForNextTx() &&
		s.suppCache == nil {
		// A committed idempotency receipt may be replayed without the
		// process-local assignment bridge, but a first write must never use
		// the key as a bridge bypass. The authoritative replay lookup above
		// runs first; reaching here means this is a new task.
		writeProblemTyped(
			w,
			http.StatusServiceUnavailable,
			"https://sage.dev/errors/task-assignment-bridge-unavailable",
			"Task assignment unavailable",
			"The node cannot durably stage the task assignee. No transaction was submitted; retry after the node is repaired.",
		)
		return
	}

	// Embed agent's cryptographic proof for on-chain identity verification.
	s.embedAgentAuth(r.Context(), submitTx)

	// Sign the transaction with the node's signing key.
	if err = s.signTx(submitTx); err != nil {
		s.logger.Error().Err(err).Msg("failed to sign submit tx")
		writeProblem(w, http.StatusInternalServerError, "Signing error", "Failed to sign transaction.")
		return
	}

	encoded, err := tx.EncodeTx(submitTx)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode submit tx")
		writeProblem(w, http.StatusInternalServerError, "Encoding error", "Failed to encode transaction.")
		return
	}

	// Stage supplementary off-chain data (embedding vector, provider, triples)
	// in the process-local cache. The ABCI app reads this during FinalizeBlock
	// and includes it in the pending write that Commit flushes to the store.
	// This ensures memories only appear in the query layer AFTER consensus.
	if s.suppCache != nil {
		taskAssignee := ""
		if req.MemoryType == string(memory.TypeTask) {
			taskAssignee = agentID
		}
		supplementary := &memory.SupplementaryData{
			Embedding:         req.Embedding,
			EmbeddingHash:     embeddingHash,
			Provider:          req.Provider,
			Assignee:          taskAssignee,
			EmbeddingProvider: embeddingProvider,
			KnowledgeTriples:  req.KnowledgeTriples,
		}
		if req.MemoryType == string(memory.TypeTask) {
			if lifetimeCache, ok := s.suppCache.(SuppCacheLifetimeWriter); ok {
				lifetimeCache.PutFor(
					memoryID,
					supplementary,
					broadcastTxCommitTimeout()+30*time.Second,
				)
			} else {
				s.suppCache.Put(memoryID, supplementary)
			}
		} else {
			s.suppCache.Put(memoryID, supplementary)
		}
	}

	// Broadcast via CometBFT RPC and wait for block finalization.
	// broadcast_tx_commit blocks until the block containing this tx is committed,
	// meaning ABCI Commit has already flushed the memory to the offchain store.
	txHash, committedHeight, err := s.broadcastTxCommitWithHeight(encoded)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to broadcast submit tx")
		if req.IdempotencyKey != "" && s.isPostV23ForNextTx() &&
			s.writeTaskIdempotencyReplayIfCommitted(
				w, req, taskPolicyPrincipal, agentID, taskPayloadDigest,
			) {
			return
		}
		if isMempoolFullErr(err) {
			status, publicMsg := broadcastErrorPublic(err)
			// Chain backpressure, not a client fault: tell the writer when
			// to come back (one block interval drains the mempool) and
			// stamp the distinct problem type so this 429 is
			// distinguishable from the rate limiter's quota 429. Header
			// must be set before writeProblemTyped calls WriteHeader —
			// mirrors middleware/ratelimit.go.
			w.Header().Set("Retry-After", "1")
			writeProblemTyped(w, status, mempoolFullProblemType, "Mempool full", publicMsg)
			return
		}
		if denial, ok := authzdenial.Classify(err); ok {
			writeEffectiveWriteDenial(w, denial)
			return
		}
		status, publicMsg := broadcastErrorPublic(err)
		writeProblem(w, status, "Broadcast error", publicMsg)
		return
	}

	taskProjectionConfirmed := (*bool)(nil)
	if req.MemoryType == string(memory.TypeTask) && s.isPostV23ForNextTx() {
		confirmed := s.confirmCommittedTaskProjection(
			memoryID,
			req.DomainTag,
			agentID,
			memory.TaskStatus(req.TaskStatus),
		)
		if confirmed && taskPolicyPrincipal != "" {
			confirmed = s.reconcileCommittedTaskTags(memoryID, req.Tags)
		}
		taskProjectionConfirmed = &confirmed
		if !confirmed {
			retryable := false
			s.logger.Error().
				Str("memory_id", memoryID).
				Str("tx_hash", txHash).
				Int64("committed_height", committedHeight).
				Msg("task transaction committed but exact assignee projection was not confirmed")
			writeJSON(w, http.StatusAccepted, SubmitMemoryResponse{
				MemoryID:            memoryID,
				TxHash:              txHash,
				Status:              "committed_unconfirmed",
				TaskStatus:          req.TaskStatus,
				Committed:           true,
				CommittedHeight:     committedHeight,
				ProjectionConfirmed: taskProjectionConfirmed,
				Retryable:           &retryable,
				Message:             "The transaction committed, but the exact task projection could not be confirmed. Reconcile this memory_id; do not resubmit the task.",
				IdempotencyKey:      req.IdempotencyKey,
				EmbeddingProvider:   embeddingProvider,
				EmbeddingQueued:     len(req.Embedding) == 0,
			})
			return
		}
	}

	metrics.MemoriesTotal.WithLabelValues(req.MemoryType, req.DomainTag, string(memory.StatusProposed)).Inc()

	// Materialize user-defined tags after the block is committed. Above app-v20
	// scoped tags are also carried by the signed transaction, mirrored in
	// AppHash-covered scoped content, and flushed during Commit. This call is an
	// idempotent serving-projection fallback for scoped records and remains the
	// node-local source for ordinary unscoped domains. broadcastTxCommit has
	// already returned after Commit flushed the memory, so SetTags will find it.
	//
	// Use a fresh short-timeout context rather than r.Context() — the
	// client may have disconnected (SIGKILL, network drop) between
	// broadcastTxCommit completing and here, and with r.Context() every
	// interrupted submit leaves an untagged orphan row that the next
	// idempotency run re-proposes as a duplicate. The commit has already
	// landed on-chain; projection finalisation must not depend on the HTTP
	// request staying alive.
	tagCtx, tagCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer tagCancel()

	if len(req.Tags) > 0 {
		if setErr := s.store.SetTags(tagCtx, memoryID, req.Tags); setErr != nil {
			s.logger.Warn().Err(setErr).Str("memory_id", memoryID).Msg("failed to set tags on committed memory")
		}
	}

	// Update agent's last activity timestamp. Same disconnect rationale as
	// SetTags above — the memory is on-chain, the last-seen update is
	// post-commit bookkeeping and should not fail just because the client
	// went away.
	if agentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(tagCtx, agentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", agentID).Msg("failed to update agent last_seen")
		}
	}

	// Emit event for SSE chain activity log
	if s.OnEvent != nil {
		s.OnEvent("remember", memoryID, req.DomainTag, truncateContent(req.Content, 80), map[string]any{
			"full_content": req.Content,
			"memory_type":  req.MemoryType,
			"confidence":   req.ConfidenceScore,
		})
	}

	// Backpressure hint for streaming writers: current mempool fill fraction
	// (0..1) from the ~1s-TTL cached sampler, so clients can self-throttle
	// with zero extra round-trips. Best-effort — omitted when the probe
	// fails, and must be set before writeJSON calls WriteHeader.
	if smp, ok := s.mempool.sample(); ok {
		w.Header().Set("X-Sage-Mempool-Pct", formatMempoolPct(smp.Pct))
	}

	writeJSON(w, http.StatusCreated, SubmitMemoryResponse{
		MemoryID:            memoryID,
		TxHash:              txHash,
		Status:              string(memory.StatusProposed),
		TaskStatus:          req.TaskStatus,
		Committed:           true,
		CommittedHeight:     committedHeight,
		ProjectionConfirmed: taskProjectionConfirmed,
		IdempotencyKey:      req.IdempotencyKey,
		EmbeddingProvider:   embeddingProvider,
		EmbeddingQueued:     len(req.Embedding) == 0,
	})
}

// confirmCommittedTaskProjection refuses to turn a chain commit into a false
// "task added" response until the exact local serving row is present with the
// immutable creator assignment. The short poll covers bounded projection
// scheduling jitter; it never resubmits and therefore cannot duplicate a task.
func (s *Server) confirmCommittedTaskProjection(
	memoryID, domain, assignee string,
	status memory.TaskStatus,
) bool {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		tasks, err := s.store.GetOpenTasks(ctx, domain, "", assignee)
		if err == nil {
			for _, task := range tasks {
				if task != nil &&
					task.MemoryID == memoryID &&
					task.MemoryType == memory.TypeTask &&
					task.DomainTag == domain &&
					task.TaskStatus == status &&
					task.Assignee == assignee {
					return true
				}
			}
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
}

// confirmCommittedTaskRecord verifies the immutable task identity and assignee
// directly by memory_id. Unlike GetOpenTasks, this remains authoritative after
// the task reaches done or dropped, so a delayed retry cannot misreport a
// durable terminal task as an unconfirmed commit.
func (s *Server) confirmCommittedTaskRecord(
	memoryID, domain, assignee string,
) (memory.TaskStatus, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	ticker := time.NewTicker(25 * time.Millisecond)
	defer ticker.Stop()
	for {
		task, err := s.store.GetMemory(ctx, memoryID)
		if err == nil &&
			task != nil &&
			task.MemoryID == memoryID &&
			task.MemoryType == memory.TypeTask &&
			task.DomainTag == domain &&
			task.Assignee == assignee {
			switch task.TaskStatus {
			case memory.TaskStatusPlanned,
				memory.TaskStatusInProgress,
				memory.TaskStatusDone,
				memory.TaskStatusDropped:
				return task.TaskStatus, true
			}
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-ticker.C:
		}
	}
}

// setDecayFloor moves a min_confidence request onto the store's DECAYED-confidence
// floor (store.QueryOptions.DecayFloor): the store then filters on the decayed value
// over the full candidate set before the top-K trim — so top_k is filled with
// qualifying records and corroboration-boosted memories are not starved — pinned to
// `now` so the value the store filters on is exactly the one this handler serializes
// below. The legacy stored-column MinConfidence filter is disabled so it cannot also
// run (it is decay-blind in both directions). No-op when no floor is requested, so
// the fast path is unchanged. `now` must be the same instant used for serialization.
func setDecayFloor(opts *store.QueryOptions, now time.Time) {
	if opts.MinConfidence > 0 {
		opts.DecayFloor = opts.MinConfidence
		opts.DecayNow = now
		opts.MinConfidence = 0
	}
}

const appV23DisclosureRecallScanLimit = 100

func requestedRecallTopK(topK int) int {
	if topK <= 0 {
		return 10
	}
	if topK > appV23DisclosureRecallScanLimit {
		return appV23DisclosureRecallScanLimit
	}
	return topK
}

// disclosureRecallTopK over-fetches only after app-v23, then the handler
// applies live per-record authority and trims back to the caller's requested
// size. Revoked rows therefore cannot starve later authorized candidates.
func (s *Server) disclosureRecallTopK(topK int) int {
	if s.isPostV23ForNextTx() {
		return appV23DisclosureRecallScanLimit
	}
	return topK
}

// appV23RecallCandidateFilter moves live disclosure ahead of the store's TopK
// consumption. Ranked stores keep walking bounded pages until they fill TopK
// with authorized rows or exhaust the stream. Handlers still re-evaluate every
// returned record immediately before serialization to close revocation races.
func (s *Server) appV23RecallCandidateFilter(
	agentID string,
	at time.Time,
) func(*memory.MemoryRecord) (bool, error) {
	if !s.isPostV23ForNextTx() {
		return nil
	}
	return func(rec *memory.MemoryRecord) (bool, error) {
		disclosure, err := s.evaluateAppV23RecordDisclosure(agentID, rec, at)
		if err != nil {
			if isUnsafeAppV23Projection(err) {
				return false, nil
			}
			return false, appV23RecordDisclosureError(err)
		}
		return disclosure.Allowed, nil
	}
}

func trimRecallResults(results []*MemoryResult, topK int) []*MemoryResult {
	limit := requestedRecallTopK(topK)
	if len(results) > limit {
		return results[:limit]
	}
	return results
}

// initialConfidencePtr returns a pointer to a stored (undecayed) confidence for the
// InitialConfidence response field — a pointer, not an omitempty float, so a
// legitimate stored 0.0 still serializes; nil is reserved for federated results.
func initialConfidencePtr(v float64) *float64 { return &v }

// disputedConfidenceHaircut is shared with the store's decayed-floor admission
// logic. Keeping one value ensures a min_confidence=X query never admits a
// challenged row and then serializes confidence_score below X after the haircut.
// Off-chain presentation only; consensus/AppHash and stored confidence are
// untouched.
const disputedConfidenceHaircut = store.DisputedConfidenceHaircut

// markDisputed reports whether rec is a live-but-disputed (challenged) memory and,
// if so, applies the confidence haircut to conf in place. Off-chain recall surface
// only — the on-chain status is unchanged and still carried in rec.Status.
func markDisputed(rec *memory.MemoryRecord, conf *float64) bool {
	if rec.Status != memory.StatusChallenged {
		return false
	}
	*conf *= disputedConfidenceHaircut
	return true
}

// corroborationCounts batch-fetches corroboration counts for a set of records,
// keyed by memory ID — one query in place of the per-record GetCorroborations N+1
// the recall loops used to run. These feed the SERIALIZED confidence_score. On a
// store error it returns (nil, err): the caller may degrade to no boost (a slightly
// low displayed score) on the no-floor path, but MUST fail closed under a
// min_confidence floor — otherwise a boosted record the store correctly kept could
// serialize a confidence_score below the floor the caller queried with.
func (s *Server) corroborationCounts(ctx context.Context, records []*memory.MemoryRecord) (map[string]int, error) {
	if len(records) == 0 {
		return nil, nil
	}
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.MemoryID
	}
	counts, err := s.store.GetCorroborationCounts(ctx, ids)
	if err != nil {
		s.logger.Warn().Err(err).Msg("batch corroboration count failed")
		return nil, err
	}
	return counts, nil
}

// challengeCounts batch-fetches lifetime distinct-challenger audit counts for
// the recall response. A missing map key means zero. Callers retain the error so
// evidence_counts_available can distinguish projection failure from true zero.
func (s *Server) challengeCounts(ctx context.Context, records []*memory.MemoryRecord) (map[string]int, error) {
	if len(records) == 0 {
		return nil, nil
	}
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.MemoryID
	}
	counts, err := s.store.GetChallengeCounts(ctx, ids)
	if err != nil {
		s.logger.Warn().Err(err).Msg("batch challenge count failed")
		return nil, err
	}
	return counts, nil
}

// evidenceProjectionCompleteness distinguishes a complete native lifetime audit
// from a recovery-built canonical lower bound. Optional/legacy stores default to
// complete; production SQLite/Postgres persist one-way incomplete markers.
func (s *Server) evidenceProjectionCompleteness(ctx context.Context, records []*memory.MemoryRecord) (map[string]bool, error) {
	if len(records) == 0 {
		return nil, nil
	}
	ids := make([]string, len(records))
	for i, rec := range records {
		ids[i] = rec.MemoryID
	}
	complete, err := store.EvidenceProjectionCompleteness(ctx, s.store, ids)
	if err != nil {
		s.logger.Warn().Err(err).Msg("batch evidence projection completeness failed")
		return nil, err
	}
	return complete, nil
}

type challengeRoundProgress struct {
	round    uint64
	current  uint32
	required uint32
}

// challengeRoundProgressFor reads the canonical open app-v21 record for each
// local result. A legacy challenge or a closed v21 round has no record and
// therefore emits no progress fields.
func (s *Server) challengeRoundProgressFor(records []*memory.MemoryRecord) (map[string]challengeRoundProgress, error) {
	if s.badgerStore == nil || len(records) == 0 {
		return nil, nil
	}
	progress := make(map[string]challengeRoundProgress)
	for _, rec := range records {
		open, err := s.badgerStore.GetChallengeRecordV21(rec.MemoryID)
		if err != nil {
			s.logger.Warn().Err(err).Str("memory_id", rec.MemoryID).Msg("app-v21 challenge progress unavailable")
			return nil, err
		}
		if open == nil {
			continue
		}
		progress[rec.MemoryID] = challengeRoundProgress{
			round: open.Round, current: open.ChallengerCount, required: open.RequiredChallengers,
		}
	}
	return progress, nil
}

func applyChallengeRoundProgress(result *MemoryResult, progress challengeRoundProgress, ok bool) {
	if !ok {
		return
	}
	round, current, required := progress.round, progress.current, progress.required
	result.ChallengeRound = &round
	result.CurrentChallengerCount = &current
	result.RequiredChallengers = &required
}

// handleQueryMemory handles POST /v1/memory/query.
func (s *Server) handleQueryMemory(w http.ResponseWriter, r *http.Request) {
	var req QueryMemoryRequest
	var err error
	if err = decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	if len(req.Embedding) == 0 {
		writeProblem(w, http.StatusBadRequest, "Missing embedding", "embedding is required for similarity search.")
		return
	}
	if providerErr := s.requireFederatedEmbeddingProvider(
		req.Federated, req.FederateChains, req.Embedding, req.EmbeddingProvider,
	); providerErr != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid embedding provider", providerErr.Error())
		return
	}

	// Network agent domain access enforcement (read side)
	// checkDomainAccess verifies the agent's DomainAccess policy (explicit allowlist).
	// If the agent passes this check (no restrictions or explicit read permission),
	// we skip the multi-org gate — the two systems are alternatives, not stacked AND.
	domainAccessApproved := false
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := s.checkDomainAccess(r.Context(), agentID, req.DomainTag, "read"); accessErr != nil {
			writeDomainReadAccessError(w, accessErr)
			return
		}
		domainAccessApproved = true
	}

	// Multi-org access control gate — only enforce when domain has a registered owner
	// AND the agent wasn't already approved by the DomainAccess policy above.
	if req.DomainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(req.DomainTag)
		if domainErr == nil && domainOwner != "" {
			agentID := middleware.ContextAgentID(r.Context())
			hasAccess, accessErr := s.hasMemoryReadAccess(req.DomainTag, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", req.DomainTag))
				return
			}
		}
	}

	// Resolve agent isolation RBAC — determines which agents' memories are visible
	queryAgentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(queryAgentID)
	legacyVisibilityRestricted := s.appV23LegacyVisibilityRestricted(queryAgentID)
	if s.appV25RecoveredReadOverridesLegacyVisibility(queryAgentID, req.DomainTag) {
		seeAll = true
		legacyVisibilityRestricted = false
	}

	// If checkDomainAccess already approved read access for this domain,
	// skip agent isolation — the agent is authorized to see everything in the domain.
	if !seeAll && !legacyVisibilityRestricted && domainAccessApproved {
		seeAll = true
	}

	// Grant-aware override: if querying a specific domain, skip agent isolation when:
	// (a) the agent has a direct grant on the domain, or
	// (b) the agent has org-level access (clearance >= classification), or
	// (c) the domain has no registered owner (no access policy = open visibility)
	if !seeAll && !legacyVisibilityRestricted && req.DomainTag != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(req.DomainTag, queryAgentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.hasMemoryReadAccess(req.DomainTag, queryAgentID, 0, time.Now())
			if hasOrgAccess {
				seeAll = true
			} else {
				// Unregistered domains have no access policy — don't enforce agent isolation
				_, ownerErr := s.badgerStore.GetDomainOwner(req.DomainTag)
				if ownerErr != nil {
					seeAll = true
				}
			}
		}
	}

	start := time.Now()

	opts := store.QueryOptions{
		DomainTag:      req.DomainTag,
		Provider:       req.Provider,
		VectorProvider: s.activeEmbeddingProvider(),
		MinConfidence:  req.MinConfidence,
		StatusFilter:   req.StatusFilter,
		TopK:           s.disclosureRecallTopK(req.TopK),
		Cursor:         req.Cursor,
		Tags:           req.Tags,
		// app-v17: keep disputed-but-live memories recallable (flagged + hair-cut
		// at serialize). A no-op unless the caller's filter is "committed".
		IncludeDisputed: true,
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}
	opts.CandidateFilter = s.appV23RecallCandidateFilter(queryAgentID, start)
	// min_confidence is a DECAYED-confidence floor (rest-api.md): hand it to the
	// store as DecayFloor, which filters the decayed value over the full candidate
	// set before the top-K trim, pinned to `start` so it matches what we serialize.
	setDecayFloor(&opts, start)

	var records []*memory.MemoryRecord
	records, err = s.store.QuerySimilar(r.Context(), req.Embedding, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to query memories")
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Memory classification state is unavailable; retry later.")
			return
		}
		if errors.Is(err, store.ErrCandidateFilterScanBudgetExceeded) {
			writeProblem(w, http.StatusUnprocessableEntity, "Recall query too broad",
				"Too many candidates require authorization; choose a domain, provider, tag, or tighter status filter.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Query error", "Failed to query memories.")
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Serialize with task-aware decay (open tasks don't decay), pinned to the same
	// instant the store filtered on; classification filtering as before.
	now := start
	corrCounts, ccErr := s.corroborationCounts(r.Context(), records)
	challengeCounts, challengeCountErr := s.challengeCounts(r.Context(), records)
	evidenceComplete, evidenceCompleteErr := s.evidenceProjectionCompleteness(r.Context(), records)
	roundProgress, roundProgressErr := s.challengeRoundProgressFor(records)
	if roundProgressErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to read challenge progress.")
		return
	}
	if opts.DecayFloor > 0 && ccErr != nil {
		// Fail closed under a floor: without accurate corroboration counts a boosted
		// record the store kept could serialize below the floor. (No floor: degrade.)
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to compute confidence scores.")
		return
	}
	results := make([]*MemoryResult, 0, len(records))
	hiddenByClassification := 0
	for _, rec := range records {
		var memClass uint8
		if s.isPostV23ForNextTx() {
			disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
				queryAgentID, rec, now,
			)
			if disclosureErr != nil {
				if isUnsafeAppV23Projection(disclosureErr) {
					continue
				}
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
					"Memory classification state is unavailable; retry later.")
				return
			}
			if !disclosure.Allowed {
				hiddenByClassification++
				continue
			}
			memClass = disclosure.Classification
		} else {
			// Preserve the pre-v23 author exception and historical classification
			// behavior byte-for-byte. App-v23 uses the centralized decision above.
			if rec.SubmittingAgent != queryAgentID && req.DomainTag == "" && rec.DomainTag != "" {
				if accessErr := s.checkDomainAccess(r.Context(), queryAgentID, rec.DomainTag, "read"); accessErr != nil {
					continue
				}
			}
			if s.badgerStore != nil {
				var classErr error
				memClass, classErr = s.recallMemoryClassification(rec.MemoryID)
				if classErr != nil {
					writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
						"Memory classification state is unavailable; retry later.")
					return
				}
				if memClass > 0 {
					domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
					if domErr == nil && domainOwner != "" {
						hasAccess, _ := s.hasMemoryReadAccess(rec.DomainTag, queryAgentID, memClass, now)
						if !hasAccess && rec.SubmittingAgent != queryAgentID {
							s.logger.Info().
								Str("memory_id", rec.MemoryID).
								Str("domain", rec.DomainTag).
								Str("submitter", idfmt.Prefix(rec.SubmittingAgent)).
								Str("querier", idfmt.Prefix(queryAgentID)).
								Str("domain_owner", idfmt.Prefix(domainOwner)).
								Uint8("classification", memClass).
								Msg("classification gate hid memory: querier has no shared-org path to writer at required clearance")
							hiddenByClassification++
							continue
						}
					}
				}
			}
		}

		// Corroboration count (batch-fetched) feeds the boost; task-aware compute so
		// open tasks serialize their non-decaying stored confidence. The store already
		// applied the decayed floor over the full candidate set, so no drop here.
		corrCount := corrCounts[rec.MemoryID]
		currentConf := memory.ComputeConfidenceForRecord(rec, now, corrCount)
		// app-v17: flag disputed-but-live (challenged) memories and hair-cut their
		// surfaced confidence. Off-chain only — the on-chain status stays in
		// rec.Status and committed rows are untouched.
		disputed := markDisputed(rec, &currentConf)

		result := &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    currentConf,
			InitialConfidence:  initialConfidencePtr(rec.ConfidenceScore),
			CorroborationCount: corrCount,
			ChallengeCount:     challengeCounts[rec.MemoryID],
			EvidenceCountsAvailable: ccErr == nil && challengeCountErr == nil &&
				evidenceCompleteErr == nil && evidenceComplete[rec.MemoryID],
			Classification: int(memClass),
			Status:         string(rec.Status),
			Disputed:       disputed,
			ParentHash:     rec.ParentHash,
			CreatedAt:      rec.CreatedAt,
			CommittedAt:    rec.CommittedAt,
		}
		progress, hasProgress := roundProgress[rec.MemoryID]
		applyChallengeRoundProgress(result, progress, hasProgress)
		results = append(results, result)
	}
	results = trimRecallResults(results, req.TopK)

	// Update agent's last activity timestamp on recall
	if queryAgentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), queryAgentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", queryAgentID).Msg("failed to update agent last_seen on recall")
		}
	}

	// Emit recall event for SSE chain activity log with full retrieved memory details
	if s.OnEvent != nil && len(results) > 0 {
		domain := req.DomainTag
		if domain == "" && len(results) > 0 {
			domain = results[0].DomainTag
		}
		// Build rich detail for expandable chain activity rows
		retrieved := make([]map[string]any, 0, len(results))
		for _, r := range results {
			retrieved = append(retrieved, map[string]any{
				"memory_id":  r.MemoryID,
				"content":    r.Content,
				"domain":     r.DomainTag,
				"confidence": r.ConfidenceScore,
				"type":       r.MemoryType,
			})
		}
		s.OnEvent("recall", "", domain, fmt.Sprintf("%d memories retrieved", len(results)), map[string]any{
			"retrieved": retrieved,
		})
	}

	resp := QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	}
	if s.isPostV23ForNextTx() {
		hiddenByClassification = 0
	}
	setFilterInfo(w, &resp, filterApplied, hiddenByClassification)

	federatedMode := federation.ModeSemantic
	if req.Query != "" {
		federatedMode = federation.ModeHybrid
	}
	agreementBindings, queryChallenges := federationPlanFields(req.FederationContext)
	s.mergeFederatedRecall(r, &resp, req.Federated, req.FederateChains, &federation.QueryRequest{
		Mode:                  federatedMode,
		Query:                 req.Query,
		Embedding:             req.Embedding,
		EmbeddingProvider:     req.EmbeddingProvider,
		Provider:              req.Provider,
		DomainTag:             req.DomainTag,
		MinConfidence:         req.MinConfidence,
		TopK:                  req.TopK,
		Tags:                  req.Tags,
		PlanAgreementBindings: agreementBindings,
		PlanChallenges:        queryChallenges,
	})

	writeJSON(w, http.StatusOK, resp)
}

// setFilterInfo attaches the X-SAGE-Filter-Applied header and the `filtered`
// envelope field to a query/search response when either silent-hide filter ran.
// submittingAgentsApplied indicates the store-level agent-isolation filter was
// used; hiddenByClassification is the count of records dropped by the
// in-handler classification+multi-org gate.
func setFilterInfo(w http.ResponseWriter, resp *QueryMemoryResponse, submittingAgentsApplied bool, hiddenByClassification int) {
	if !submittingAgentsApplied && hiddenByClassification == 0 {
		return
	}
	var applied []string
	if submittingAgentsApplied {
		applied = append(applied, filterBySubmittingAgts)
	}
	if hiddenByClassification > 0 {
		applied = append(applied, filterByClassification)
	}
	w.Header().Set(filterHeader, strings.Join(applied, ","))
	info := &FilterInfo{By: applied}
	if hiddenByClassification > 0 {
		hc := hiddenByClassification
		info.HiddenCount = &hc
	}
	resp.Filtered = info
}

// defaultFedRecallTimeout bounds the cross-network fan-out so a slow or dead
// peer can't stall a local recall. Overridable via SAGE_FED_RECALL_TIMEOUT_MS
// (mirrors broadcastTxCommitTimeout's pattern).
const defaultFedRecallTimeout = 4 * time.Second

func fedRecallTimeout() time.Duration {
	if v := os.Getenv("SAGE_FED_RECALL_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultFedRecallTimeout
}

// mergeFederatedRecall fuses read-only results from cross_fed peers with the
// local recall ranking (v11 Mode-1 exchange). Opt-in per request; no-op when the
// federation transport isn't wired or the request didn't ask. Remote results
// arrive already filtered by the SERVING peer's treaty enforcement (its
// AllowedDomains + MaxClearance + committed-only) and are merged into the
// response ONLY — nothing touches local stores, embeddings, or AppHash state.
// Peer failures are disclosed in resp.Federation.Errors, never silently
// dropped (fail-closed plus disclosure, same philosophy as FilterInfo).
//
// AUTHORIZATION: operators/admins retain broad read authority; ordinary
// registered agents may delegate only an exact domain covered by their local
// read subtree. Remote records are then filtered again against that caller's
// clearance, independently of the chain-to-chain treaty ceiling.
func (s *Server) mergeFederatedRecall(r *http.Request, resp *QueryMemoryResponse, federated bool, chains []string, fedReq *federation.QueryRequest) {
	s.enrichStoredCopyProvenance(r.Context(), resp.Results)
	if s.federation == nil || (!federated && len(chains) == 0) {
		return
	}
	callerID := middleware.ContextAgentID(r.Context())
	allowed, callerClearance := s.federationCallerCanRead(r.Context(), callerID, fedReq.DomainTag)
	if !allowed {
		resp.Federation = &FederationInfo{
			Queried: []string{},
			Errors:  map[string]string{"*": "caller is not authorized to read this federated domain"},
		}
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), fedRecallTimeout())
	defer cancel()
	// app-v23 forwards the original caller proof verbatim. The remote node
	// independently verifies this signature and compares the signed recall body
	// with the transport query; the local node operator cannot substitute a
	// different agent or broaden its domain.
	if proof := middleware.ContextAgentAuth(r.Context()); proof != nil {
		fedReq.AgentProof = &federation.QueryAgentProof{
			AgentID:          callerID,
			Signature:        append([]byte(nil), proof.Signature...),
			Timestamp:        proof.Timestamp,
			Nonce:            append([]byte(nil), proof.Nonce...),
			CanonicalRequest: append([]byte(nil), proof.CanonicalRequest...),
		}
	}

	targets := chains
	privilegedTopology := s.nodeOperatorID != "" && callerID == s.nodeOperatorID
	if s.isPostV23ForNextTx() {
		privilegedTopology = s.callerIsOperatorOrAdmin(r.Context(), callerID)
	}
	if !privilegedTopology {
		targets = s.federationRecallTargets(ctx, chains, fedReq.DomainTag)
		if len(targets) == 0 {
			resp.Federation = &FederationInfo{
				Queried: []string{},
				Errors:  map[string]string{"*": "no reachable authorized federation peer currently exposes this domain"},
			}
			return
		}
	}
	outcomes := s.federation.FanOutRecall(ctx, targets, fedReq)
	if len(outcomes) == 0 {
		resp.Federation = &FederationInfo{Queried: []string{}}
		return
	}
	info := &FederationInfo{Coverage: make([]FederationCoverage, 0, len(outcomes))}
	type rankedResult struct {
		result *MemoryResult
		score  float64
		order  int
	}
	const federationRRFK = 60
	ranked := make([]rankedResult, 0, len(resp.Results)+16)
	rankedIndex := make(map[string]int)
	rankIdentity := func(result *MemoryResult) string {
		if result.ContentHash != "" {
			return "hash:" + strings.ToLower(result.ContentHash)
		}
		if result.SourceChainID != "" && result.OriginMemoryID != "" {
			return "origin:" + result.SourceChainID + ":" + result.OriginMemoryID
		}
		return "memory:" + result.SourceChainID + ":" + result.MemoryID
	}
	preference := func(result *MemoryResult) int {
		switch result.SourceKind {
		case "local_native":
			return 3
		case "federated_copy":
			return 2
		default:
			return 1
		}
	}
	addRanked := func(result *MemoryResult, score float64, order int) {
		key := rankIdentity(result)
		if index, exists := rankedIndex[key]; exists {
			ranked[index].score += score
			if preference(result) > preference(ranked[index].result) ||
				(preference(result) == preference(ranked[index].result) &&
					result.ConfidenceScore > ranked[index].result.ConfidenceScore) {
				ranked[index].result = result
			}
			return
		}
		rankedIndex[key] = len(ranked)
		ranked = append(ranked, rankedResult{result: result, score: score, order: order})
	}
	for i, local := range resp.Results {
		addRanked(local, 1.0/float64(federationRRFK+i+1), i)
	}
	nextOrder := len(ranked)
	for _, outcome := range outcomes {
		info.Queried = append(info.Queried, outcome.ChainID)
		coverage := FederationCoverage{
			ChainID:    outcome.ChainID,
			Status:     "ok",
			SearchMode: fedReq.Mode,
		}
		if fedReq.Mode == federation.ModeHybrid {
			coverage.Fallback = "keyword arm covers embedding-provider mismatch"
		}
		if outcome.Err != nil {
			coverage.Status = "error"
			info.Coverage = append(info.Coverage, coverage)
			if info.Errors == nil {
				info.Errors = make(map[string]string)
			}
			info.Errors[outcome.ChainID] = outcome.Err.Error()
			s.logger.Warn().Err(outcome.Err).Str("peer", outcome.ChainID).Msg("federated recall peer failed")
			continue
		}
		for peerRank, fr := range outcome.Results {
			// Enforce the caller's decayed floor on peer results too: fr.ConfidenceScore
			// is already the serving peer's decayed value, so this keeps the
			// min_confidence contract ("every returned result satisfies confidence_score
			// >= X") intact across the federation bridge — including against peers on an
			// older build whose serving side still filtered on the stored column.
			if fedReq.MinConfidence > 0 && fr.ConfidenceScore < fedReq.MinConfidence {
				continue
			}
			// The remote SAGE enforces the chain-to-chain ceiling; this local
			// gate additionally enforces the invoking agent's own clearance.
			if fr.Classification > callerClearance {
				continue
			}
			coverage.Matched++
			addRanked(&MemoryResult{
				MemoryID:                fr.MemoryID,
				SubmittingAgent:         fr.SubmittingAgent,
				Content:                 fr.Content,
				ContentHash:             fr.ContentHash,
				MemoryType:              fr.MemoryType,
				DomainTag:               fr.DomainTag,
				ConfidenceScore:         fr.ConfidenceScore,
				CorroborationCount:      fr.CorroborationCount,
				ChallengeCount:          fr.ChallengeCount,
				EvidenceCountsAvailable: fr.EvidenceCountsAvailable,
				Classification:          fr.Classification,
				Status:                  fr.Status,
				CreatedAt:               fr.CreatedAt,
				CommittedAt:             fr.CommittedAt,
				SourceChainID:           fr.SourceChainID,
				SourceKind:              "federated_live",
				OriginMemoryID:          fr.MemoryID,
				OriginAgentID:           fr.SubmittingAgent,
				Foreign:                 true,
				Trust:                   "external_untrusted",
			}, 1.0/float64(federationRRFK+peerRank+1), nextOrder)
			nextOrder++
		}
		info.Coverage = append(info.Coverage, coverage)
	}

	// Reciprocal-rank fuse every local/peer list, then apply one global cap.
	// This avoids source-order append bias and guarantees top_k is a response
	// bound rather than a per-peer multiplier.
	sort.SliceStable(ranked, func(i, j int) bool {
		if ranked[i].score != ranked[j].score {
			return ranked[i].score > ranked[j].score
		}
		if ranked[i].result.ConfidenceScore != ranked[j].result.ConfidenceScore {
			return ranked[i].result.ConfidenceScore > ranked[j].result.ConfidenceScore
		}
		return ranked[i].order < ranked[j].order
	})
	limit := fedReq.TopK
	if limit <= 0 {
		limit = 10
	}
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	resp.Results = make([]*MemoryResult, 0, len(ranked))
	includedByChain := make(map[string]int)
	for _, rr := range ranked {
		resp.Results = append(resp.Results, rr.result)
		if rr.result.SourceKind == "federated_live" {
			info.Merged++
			includedByChain[rr.result.SourceChainID]++
		}
	}
	for i := range info.Coverage {
		info.Coverage[i].Included = includedByChain[info.Coverage[i].ChainID]
	}
	resp.TotalCount = len(resp.Results)
	resp.Federation = info
}

// federationCallerCanRead applies the local caller's federation delegation
// policy. Unlike the historical operator-only gate it lets ordinary registered
// agents use the same domain subtree semantics as normal SAGE access grants,
// while unknown identities fail closed.
func (s *Server) federationCallerCanRead(ctx context.Context, callerID, domain string) (bool, int) {
	if callerID == "" {
		return false, 0
	}
	if s.isPostV23ForNextTx() {
		active, err := s.appV23ActiveOrdinaryAgent(callerID)
		if err != nil || !active {
			return false, 0
		}
	}
	if s.isPostV23ForNextTx() && s.badgerStore != nil {
		if domain == "" {
			return false, 0
		}
		policyID, policyErr := appV23PolicyPrincipal(s.badgerStore, callerID)
		if policyErr != nil {
			return false, 0
		}
		enrollment, err := s.badgerStore.GetAppV23Enrollment(policyID)
		if err != nil || enrollment == nil || !enrollment.Active {
			return false, 0
		}
		if err := checkAppV23DomainAccess(s.badgerStore, callerID, domain, "read"); err != nil {
			return false, int(enrollment.Clearance)
		}
		return true, int(enrollment.Clearance)
	}
	if s.nodeOperatorID != "" && callerID == s.nodeOperatorID {
		return true, 4
	}
	if domain == "" {
		return false, 0
	}
	if s.badgerStore != nil {
		agent, err := s.badgerStore.GetRegisteredAgent(callerID)
		if err == nil && agent != nil {
			if s.isPostV22ForNextTx() && !agent.Capabilities.Valid() {
				return false, 0
			}
			if agent.Role == "admin" {
				return true, 4
			}
			if s.isPostV22ForNextTx() && agent.Capabilities.Has(store.AgentCapabilityReadAllDomains) {
				return true, int(agent.Clearance)
			}
			if agent.DomainAccess == "" {
				return true, int(agent.Clearance)
			}
			var access []struct {
				Domain string `json:"domain"`
				Read   bool   `json:"read"`
			}
			if err := json.Unmarshal([]byte(agent.DomainAccess), &access); err != nil {
				return false, int(agent.Clearance)
			}
			if len(access) == 0 {
				return true, int(agent.Clearance)
			}
			{
				for _, entry := range access {
					if entry.Read && federation.DomainAllowed([]string{entry.Domain}, domain) {
						return true, int(agent.Clearance)
					}
				}
			}
			return false, int(agent.Clearance)
		} else if s.isPostV22ForNextTx() {
			if _, _, capabilityErr := s.badgerStore.GetRegisteredAgentCapabilities(callerID); capabilityErr != nil {
				return false, 0
			}
		}
	}
	if s.agentStore != nil {
		agent, err := s.agentStore.GetAgent(ctx, callerID)
		if err != nil || agent == nil {
			return false, 0
		}
		if agent.Role == "admin" {
			return true, 4
		}
		if agent.DomainAccess == "" {
			return true, agent.Clearance
		}
		var access []struct {
			Domain string `json:"domain"`
			Read   bool   `json:"read"`
		}
		if json.Unmarshal([]byte(agent.DomainAccess), &access) != nil {
			return false, agent.Clearance
		}
		if len(access) == 0 {
			return true, agent.Clearance
		}
		for _, entry := range access {
			if entry.Read && federation.DomainAllowed([]string{entry.Domain}, domain) {
				return true, agent.Clearance
			}
		}
	}
	return false, 0
}

type syncOriginLookup interface {
	GetSyncOriginByLocalMemoryID(context.Context, string) (*store.SyncOrigin, error)
}

func (s *Server) enrichStoredCopyProvenance(ctx context.Context, results []*MemoryResult) {
	lookup, ok := s.store.(syncOriginLookup)
	if !ok {
		for _, result := range results {
			if result.SourceKind == "" {
				result.SourceKind = "local_native"
			}
		}
		return
	}
	for _, result := range results {
		origin, err := lookup.GetSyncOriginByLocalMemoryID(ctx, result.MemoryID)
		if errors.Is(err, sql.ErrNoRows) || (err == nil && origin == nil) {
			result.SourceKind = "local_native"
			continue
		}
		if err != nil {
			// Provenance is a trust boundary. A database/migration failure must
			// never silently launder a retained foreign copy into local-native
			// content. Over-taint the result until the ledger is readable.
			result.SourceKind = "federated_copy"
			result.Foreign = true
			result.Trust = "external_untrusted"
			continue
		}
		result.SourceChainID = origin.OriginChainID
		result.SourceKind = "federated_copy"
		result.OriginMemoryID = origin.OriginMemoryID
		result.OriginAgentID = origin.OriginAgentPubkey
		result.Foreign = true
		result.Trust = "external_untrusted"
		if origin.ContentHash != "" {
			result.ContentHash = origin.ContentHash
		}
		result.Classification = origin.Classification
		if origin.MemoryType != "" {
			result.MemoryType = origin.MemoryType
		}
	}
}

// SearchMemoryRequest is the JSON body for POST /v1/memory/search.
type SearchMemoryRequest struct {
	Query         string   `json:"query"`
	DomainTag     string   `json:"domain_tag,omitempty"`
	Provider      string   `json:"provider,omitempty"`
	MinConfidence float64  `json:"min_confidence,omitempty"`
	StatusFilter  string   `json:"status_filter,omitempty"`
	TopK          int      `json:"top_k,omitempty"`
	Tags          []string `json:"tags,omitempty"`
	// v11 federated recall opt-in — see QueryMemoryRequest.
	Federated         bool                         `json:"federated,omitempty"`
	FederateChains    []string                     `json:"federate_chains,omitempty"`
	FederationContext *FederatedRecallProofContext `json:"federation_context,omitempty"`
}

// handleSearchMemory handles POST /v1/memory/search — FTS5 full-text search.
// Same access control as handleQueryMemory but uses text matching instead of embeddings.
func (s *Server) handleSearchMemory(w http.ResponseWriter, r *http.Request) {
	var req SearchMemoryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Query == "" {
		writeProblem(w, http.StatusBadRequest, "Missing query", "query field is required for text search.")
		return
	}

	// Domain access control (same as handleQueryMemory)
	domainAccessApproved := false
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := s.checkDomainAccess(r.Context(), agentID, req.DomainTag, "read"); accessErr != nil {
			writeDomainReadAccessError(w, accessErr)
			return
		}
		domainAccessApproved = true
	}

	// Multi-org access control gate
	if req.DomainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(req.DomainTag)
		if domainErr == nil && domainOwner != "" {
			agentID := middleware.ContextAgentID(r.Context())
			hasAccess, accessErr := s.hasMemoryReadAccess(req.DomainTag, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", req.DomainTag))
				return
			}
		}
	}

	// Agent isolation RBAC
	queryAgentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(queryAgentID)
	legacyVisibilityRestricted := s.appV23LegacyVisibilityRestricted(queryAgentID)
	if s.appV25RecoveredReadOverridesLegacyVisibility(queryAgentID, req.DomainTag) {
		seeAll = true
		legacyVisibilityRestricted = false
	}

	if !seeAll && !legacyVisibilityRestricted && domainAccessApproved {
		seeAll = true
	}

	if !seeAll && !legacyVisibilityRestricted && req.DomainTag != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(req.DomainTag, queryAgentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.hasMemoryReadAccess(req.DomainTag, queryAgentID, 0, time.Now())
			if hasOrgAccess {
				seeAll = true
			} else {
				_, ownerErr := s.badgerStore.GetDomainOwner(req.DomainTag)
				if ownerErr != nil {
					seeAll = true
				}
			}
		}
	}

	start := time.Now()

	opts := store.QueryOptions{
		DomainTag:     req.DomainTag,
		Provider:      req.Provider,
		MinConfidence: req.MinConfidence,
		StatusFilter:  req.StatusFilter,
		TopK:          s.disclosureRecallTopK(req.TopK),
		Tags:          req.Tags,
		// app-v17: keep disputed-but-live memories recallable (flagged + hair-cut
		// at serialize). A no-op unless the caller's filter is "committed".
		IncludeDisputed: true,
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}
	opts.CandidateFilter = s.appV23RecallCandidateFilter(queryAgentID, start)
	// min_confidence is a DECAYED-confidence floor (rest-api.md): hand it to the
	// store as DecayFloor, which filters the decayed value over the full candidate
	// set before the top-K trim, pinned to `start` so it matches what we serialize.
	setDecayFloor(&opts, start)

	records, err := s.store.SearchByText(r.Context(), req.Query, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to search memories")
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Memory classification state is unavailable; retry later.")
			return
		}
		if errors.Is(err, store.ErrCandidateFilterScanBudgetExceeded) {
			writeProblem(w, http.StatusUnprocessableEntity, "Search query too broad",
				"Too many candidates require authorization; choose a domain, provider, tag, or tighter status filter.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Search error", err.Error())
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Serialize with task-aware decay (same as handleQueryMemory), pinned to the
	// store's filter instant.
	now := start
	corrCounts, ccErr := s.corroborationCounts(r.Context(), records)
	challengeCounts, challengeCountErr := s.challengeCounts(r.Context(), records)
	evidenceComplete, evidenceCompleteErr := s.evidenceProjectionCompleteness(r.Context(), records)
	roundProgress, roundProgressErr := s.challengeRoundProgressFor(records)
	if roundProgressErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to read challenge progress.")
		return
	}
	if opts.DecayFloor > 0 && ccErr != nil {
		// Fail closed under a floor: without accurate corroboration counts a boosted
		// record the store kept could serialize below the floor. (No floor: degrade.)
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to compute confidence scores.")
		return
	}
	results := make([]*MemoryResult, 0, len(records))
	hiddenByClassification := 0
	for _, rec := range records {
		// Per-record domain-read filter — parity with list/tasks/pending (see
		// handleQueryMemory). Drops cross-domain content the caller has no read
		// grant on, on the no-domain path; skips own records and concrete-domain
		// requests already gated up front.
		if rec.SubmittingAgent != queryAgentID && req.DomainTag == "" && rec.DomainTag != "" {
			if accessErr := s.checkDomainAccess(r.Context(), queryAgentID, rec.DomainTag, "read"); accessErr != nil {
				continue
			}
		}
		var memClass uint8
		if s.isPostV23ForNextTx() {
			disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
				queryAgentID, rec, now,
			)
			if disclosureErr != nil {
				if isUnsafeAppV23Projection(disclosureErr) {
					continue
				}
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
					"Memory classification state is unavailable; retry later.")
				return
			}
			if !disclosure.Allowed {
				hiddenByClassification++
				continue
			}
			memClass = disclosure.Classification
		} else if s.badgerStore != nil {
			var classErr error
			memClass, classErr = s.recallMemoryClassification(rec.MemoryID)
			if classErr != nil {
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
					"Memory classification state is unavailable; retry later.")
				return
			}
			if memClass > 0 {
				domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
				if domErr == nil && domainOwner != "" {
					hasAccess, _ := s.hasMemoryReadAccess(rec.DomainTag, queryAgentID, memClass, now)
					if !hasAccess && rec.SubmittingAgent != queryAgentID {
						s.logger.Info().
							Str("memory_id", rec.MemoryID).
							Str("domain", rec.DomainTag).
							Str("submitter", idfmt.Prefix(rec.SubmittingAgent)).
							Str("querier", idfmt.Prefix(queryAgentID)).
							Str("domain_owner", idfmt.Prefix(domainOwner)).
							Uint8("classification", memClass).
							Msg("classification gate hid memory: querier has no shared-org path to writer at required clearance")
						hiddenByClassification++
						continue
					}
				}
			}
		}

		corrCount := corrCounts[rec.MemoryID]
		currentConf := memory.ComputeConfidenceForRecord(rec, now, corrCount)
		// app-v17: flag disputed-but-live (challenged) memories and hair-cut their
		// surfaced confidence. Off-chain only — the on-chain status stays in
		// rec.Status and committed rows are untouched.
		disputed := markDisputed(rec, &currentConf)

		result := &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    currentConf,
			InitialConfidence:  initialConfidencePtr(rec.ConfidenceScore),
			CorroborationCount: corrCount,
			ChallengeCount:     challengeCounts[rec.MemoryID],
			EvidenceCountsAvailable: ccErr == nil && challengeCountErr == nil &&
				evidenceCompleteErr == nil && evidenceComplete[rec.MemoryID],
			Classification: int(memClass),
			Status:         string(rec.Status),
			Disputed:       disputed,
			ParentHash:     rec.ParentHash,
			CreatedAt:      rec.CreatedAt,
			CommittedAt:    rec.CommittedAt,
		}
		progress, hasProgress := roundProgress[rec.MemoryID]
		applyChallengeRoundProgress(result, progress, hasProgress)
		results = append(results, result)
	}
	results = trimRecallResults(results, req.TopK)

	// Update agent last activity
	if queryAgentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), queryAgentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", queryAgentID).Msg("failed to update agent last_seen on search")
		}
	}

	// Emit search event for SSE chain activity log
	if s.OnEvent != nil && len(results) > 0 {
		domain := req.DomainTag
		if domain == "" && len(results) > 0 {
			domain = results[0].DomainTag
		}
		retrieved := make([]map[string]any, 0, len(results))
		for _, r := range results {
			retrieved = append(retrieved, map[string]any{
				"memory_id":  r.MemoryID,
				"content":    r.Content,
				"domain":     r.DomainTag,
				"confidence": r.ConfidenceScore,
				"type":       r.MemoryType,
			})
		}
		s.OnEvent("search", "", domain, fmt.Sprintf("%d memories found via text search", len(results)), map[string]any{
			"retrieved": retrieved,
		})
	}

	resp := QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	}
	if s.isPostV23ForNextTx() {
		hiddenByClassification = 0
	}
	setFilterInfo(w, &resp, filterApplied, hiddenByClassification)

	agreementBindings, queryChallenges := federationPlanFields(req.FederationContext)
	s.mergeFederatedRecall(r, &resp, req.Federated, req.FederateChains, &federation.QueryRequest{
		Mode:                  federation.ModeText,
		Query:                 req.Query,
		Provider:              req.Provider,
		DomainTag:             req.DomainTag,
		MinConfidence:         req.MinConfidence,
		TopK:                  req.TopK,
		Tags:                  req.Tags,
		PlanAgreementBindings: agreementBindings,
		PlanChallenges:        queryChallenges,
	})

	writeJSON(w, http.StatusOK, resp)
}

// HybridSearchMemoryRequest is the JSON body for POST /v1/memory/hybrid —
// the unified path that fuses FTS5/BM25 and vector cosine results via
// weighted Reciprocal Rank Fusion. Callers send both the text query and the
// precomputed embedding so the server can run both indexes in one round trip.
//
// v7.1 adds optional Expansions: a list of paraphrase/entity/temporal variants
// of the primary query. When non-empty the server runs SearchHybrid once per
// variant (in addition to the primary), then RRFs the variant rankings into
// one final list. Callers must include both the text and embedding for each
// expansion so SAGE doesn't need to know which embedder generated the primary.
type HybridSearchMemoryRequest struct {
	Query             string            `json:"query"`
	Embedding         []float32         `json:"embedding"`
	EmbeddingProvider string            `json:"embedding_provider,omitempty"`
	Expansions        []HybridExpansion `json:"expansions,omitempty"`
	DomainTag         string            `json:"domain_tag,omitempty"`
	Provider          string            `json:"provider,omitempty"`
	MinConfidence     float64           `json:"min_confidence,omitempty"`
	StatusFilter      string            `json:"status_filter,omitempty"`
	TopK              int               `json:"top_k,omitempty"`
	Tags              []string          `json:"tags,omitempty"`
	// v11 federated recall opt-in — see QueryMemoryRequest.
	Federated         bool                         `json:"federated,omitempty"`
	FederateChains    []string                     `json:"federate_chains,omitempty"`
	FederationContext *FederatedRecallProofContext `json:"federation_context,omitempty"`
}

// HybridExpansion carries a single paraphrase/entity/temporal variant of the
// primary query plus the precomputed embedding for that variant. The caller
// is responsible for using the same embedder that produced the primary
// embedding so the vectors live in the same space as the stored memories.
type HybridExpansion struct {
	Query     string    `json:"query"`
	Embedding []float32 `json:"embedding"`
}

// maxHybridExpansions bounds authenticated query fan-out. Expansion recall is
// best-effort relevance enrichment, not an unbounded batch API; without a cap a
// single signed request could multiply SQL/vector and live authorization work.
const maxHybridExpansions = 8

// handleHybridSearchMemory handles POST /v1/memory/hybrid.
// Runs SearchByText and QuerySimilar in parallel and fuses them via RRF.
// Access control mirrors handleQueryMemory exactly — the only differences are
// the underlying store call and that the request carries both a query string
// and an embedding.
func (s *Server) handleHybridSearchMemory(w http.ResponseWriter, r *http.Request) {
	var req HybridSearchMemoryRequest
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.Query == "" && len(req.Embedding) == 0 {
		writeProblem(w, http.StatusBadRequest, "Missing inputs",
			"hybrid search requires at least one of `query` or `embedding`")
		return
	}
	if len(req.Expansions) > maxHybridExpansions {
		writeProblem(w, http.StatusUnprocessableEntity, "Too many hybrid expansions",
			fmt.Sprintf("hybrid search accepts at most %d expansion variants per request", maxHybridExpansions))
		return
	}
	if err := s.requireFederatedEmbeddingProvider(
		req.Federated, req.FederateChains, req.Embedding, req.EmbeddingProvider,
	); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid embedding provider", err.Error())
		return
	}

	domainAccessApproved := false
	if req.DomainTag != "" {
		agentID := middleware.ContextAgentID(r.Context())
		if accessErr := s.checkDomainAccess(r.Context(), agentID, req.DomainTag, "read"); accessErr != nil {
			writeDomainReadAccessError(w, accessErr)
			return
		}
		domainAccessApproved = true
	}

	if req.DomainTag != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(req.DomainTag)
		if domainErr == nil && domainOwner != "" {
			agentID := middleware.ContextAgentID(r.Context())
			hasAccess, accessErr := s.hasMemoryReadAccess(req.DomainTag, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", req.DomainTag))
				return
			}
		}
	}

	queryAgentID := middleware.ContextAgentID(r.Context())
	allowedAgents, seeAll := s.resolveVisibleAgents(queryAgentID)
	legacyVisibilityRestricted := s.appV23LegacyVisibilityRestricted(queryAgentID)
	if s.appV25RecoveredReadOverridesLegacyVisibility(queryAgentID, req.DomainTag) {
		seeAll = true
		legacyVisibilityRestricted = false
	}

	if !seeAll && !legacyVisibilityRestricted && domainAccessApproved {
		seeAll = true
	}

	if !seeAll && !legacyVisibilityRestricted && req.DomainTag != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(req.DomainTag, queryAgentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.hasMemoryReadAccess(req.DomainTag, queryAgentID, 0, time.Now())
			if hasOrgAccess {
				seeAll = true
			} else {
				_, ownerErr := s.badgerStore.GetDomainOwner(req.DomainTag)
				if ownerErr != nil {
					seeAll = true
				}
			}
		}
	}

	start := time.Now()

	opts := store.QueryOptions{
		DomainTag:      req.DomainTag,
		Provider:       req.Provider,
		VectorProvider: s.activeEmbeddingProvider(),
		MinConfidence:  req.MinConfidence,
		StatusFilter:   req.StatusFilter,
		TopK:           s.disclosureRecallTopK(req.TopK),
		Tags:           req.Tags,
		// app-v17: keep disputed-but-live memories recallable (flagged + hair-cut
		// at serialize). A no-op unless the caller's filter is "committed".
		IncludeDisputed: true,
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}
	opts.CandidateFilter = s.appV23RecallCandidateFilter(queryAgentID, start)
	// min_confidence is a DECAYED-confidence floor (rest-api.md): hand it to the
	// store as DecayFloor. Carried on the fused opts, both hybrid sub-queries filter
	// the decayed value before their trim, pinned to `start` for serialize-parity.
	setDecayFloor(&opts, start)

	records, err := s.runHybridWithExpansions(r.Context(), req, opts)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to hybrid-search memories")
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Memory classification state is unavailable; retry later.")
			return
		}
		if errors.Is(err, store.ErrCandidateFilterScanBudgetExceeded) {
			writeProblem(w, http.StatusUnprocessableEntity, "Hybrid query too broad",
				"Too many candidates require authorization; choose a domain, provider, tag, or tighter status filter.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Hybrid search error", err.Error())
		return
	}

	metrics.RecordQuery(req.DomainTag, time.Since(start))

	// Serialize with task-aware decay, pinned to the store's filter instant.
	now := start
	corrCounts, ccErr := s.corroborationCounts(r.Context(), records)
	challengeCounts, challengeCountErr := s.challengeCounts(r.Context(), records)
	evidenceComplete, evidenceCompleteErr := s.evidenceProjectionCompleteness(r.Context(), records)
	roundProgress, roundProgressErr := s.challengeRoundProgressFor(records)
	if roundProgressErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to read challenge progress.")
		return
	}
	if opts.DecayFloor > 0 && ccErr != nil {
		// Fail closed under a floor: without accurate corroboration counts a boosted
		// record the store kept could serialize below the floor. (No floor: degrade.)
		writeProblem(w, http.StatusInternalServerError, "Recall error", "Failed to compute confidence scores.")
		return
	}
	results := make([]*MemoryResult, 0, len(records))
	hiddenByClassification := 0
	for _, rec := range records {
		// Per-record domain-read filter — parity with list/tasks/pending (see
		// handleQueryMemory). Drops cross-domain content the caller has no read
		// grant on, on the no-domain path; skips own records and concrete-domain
		// requests already gated up front.
		if rec.SubmittingAgent != queryAgentID && req.DomainTag == "" && rec.DomainTag != "" {
			if accessErr := s.checkDomainAccess(r.Context(), queryAgentID, rec.DomainTag, "read"); accessErr != nil {
				continue
			}
		}
		var memClass uint8
		if s.isPostV23ForNextTx() {
			disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
				queryAgentID, rec, now,
			)
			if disclosureErr != nil {
				if isUnsafeAppV23Projection(disclosureErr) {
					continue
				}
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
					"Memory classification state is unavailable; retry later.")
				return
			}
			if !disclosure.Allowed {
				hiddenByClassification++
				continue
			}
			memClass = disclosure.Classification
		} else if s.badgerStore != nil {
			var classErr error
			memClass, classErr = s.recallMemoryClassification(rec.MemoryID)
			if classErr != nil {
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
					"Memory classification state is unavailable; retry later.")
				return
			}
			if memClass > 0 {
				domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
				if domErr == nil && domainOwner != "" {
					hasAccess, _ := s.hasMemoryReadAccess(rec.DomainTag, queryAgentID, memClass, now)
					if !hasAccess && rec.SubmittingAgent != queryAgentID {
						s.logger.Info().
							Str("memory_id", rec.MemoryID).
							Str("domain", rec.DomainTag).
							Str("submitter", idfmt.Prefix(rec.SubmittingAgent)).
							Str("querier", idfmt.Prefix(queryAgentID)).
							Str("domain_owner", idfmt.Prefix(domainOwner)).
							Uint8("classification", memClass).
							Msg("classification gate hid memory (hybrid path)")
						hiddenByClassification++
						continue
					}
				}
			}
		}

		corrCount := corrCounts[rec.MemoryID]
		currentConf := memory.ComputeConfidenceForRecord(rec, now, corrCount)
		// app-v17: flag disputed-but-live (challenged) memories and hair-cut their
		// surfaced confidence. Off-chain only — the on-chain status stays in
		// rec.Status and committed rows are untouched.
		disputed := markDisputed(rec, &currentConf)

		result := &MemoryResult{
			MemoryID:           rec.MemoryID,
			SubmittingAgent:    rec.SubmittingAgent,
			Content:            rec.Content,
			ContentHash:        hex.EncodeToString(rec.ContentHash),
			MemoryType:         string(rec.MemoryType),
			DomainTag:          rec.DomainTag,
			ConfidenceScore:    currentConf,
			InitialConfidence:  initialConfidencePtr(rec.ConfidenceScore),
			CorroborationCount: corrCount,
			ChallengeCount:     challengeCounts[rec.MemoryID],
			EvidenceCountsAvailable: ccErr == nil && challengeCountErr == nil &&
				evidenceCompleteErr == nil && evidenceComplete[rec.MemoryID],
			Classification: int(memClass),
			Status:         string(rec.Status),
			Disputed:       disputed,
			ParentHash:     rec.ParentHash,
			CreatedAt:      rec.CreatedAt,
			CommittedAt:    rec.CommittedAt,
		}
		progress, hasProgress := roundProgress[rec.MemoryID]
		applyChallengeRoundProgress(result, progress, hasProgress)
		results = append(results, result)
	}
	results = trimRecallResults(results, req.TopK)

	if queryAgentID != "" && s.agentStore != nil {
		if updateErr := s.agentStore.UpdateAgentLastSeen(r.Context(), queryAgentID, time.Now()); updateErr != nil {
			s.logger.Warn().Err(updateErr).Str("agent_id", queryAgentID).Msg("failed to update agent last_seen on hybrid search")
		}
	}

	if s.OnEvent != nil && len(results) > 0 {
		domain := req.DomainTag
		if domain == "" {
			domain = results[0].DomainTag
		}
		retrieved := make([]map[string]any, 0, len(results))
		for _, r := range results {
			retrieved = append(retrieved, map[string]any{
				"memory_id":  r.MemoryID,
				"content":    r.Content,
				"domain":     r.DomainTag,
				"confidence": r.ConfidenceScore,
				"type":       r.MemoryType,
			})
		}
		s.OnEvent("hybrid", "", domain, fmt.Sprintf("%d memories retrieved via hybrid search", len(results)), map[string]any{
			"retrieved": retrieved,
		})
	}

	resp := QueryMemoryResponse{
		Results:    results,
		TotalCount: len(results),
	}
	if s.isPostV23ForNextTx() {
		hiddenByClassification = 0
	}
	setFilterInfo(w, &resp, filterApplied, hiddenByClassification)

	agreementBindings, queryChallenges := federationPlanFields(req.FederationContext)
	s.mergeFederatedRecall(r, &resp, req.Federated, req.FederateChains, &federation.QueryRequest{
		Mode:                  federation.ModeHybrid,
		Query:                 req.Query,
		Embedding:             req.Embedding,
		EmbeddingProvider:     req.EmbeddingProvider,
		Provider:              req.Provider,
		DomainTag:             req.DomainTag,
		MinConfidence:         req.MinConfidence,
		TopK:                  req.TopK,
		Tags:                  req.Tags,
		PlanAgreementBindings: agreementBindings,
		PlanChallenges:        queryChallenges,
	})

	writeJSON(w, http.StatusOK, resp)
}

// handleGetMemory handles GET /v1/memory/{memory_id}.
func (s *Server) handleGetMemory(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory ID", "memory_id path parameter is required.")
		return
	}

	rec, err := s.store.GetMemory(r.Context(), memoryID)
	if err != nil {
		writeProblem(w, http.StatusNotFound, "Memory not found",
			fmt.Sprintf("No memory found with ID %s.", memoryID))
		return
	}

	// Agent isolation RBAC check: can this agent see this memory's author?
	agentID := middleware.ContextAgentID(r.Context())
	var appV23Classification *uint8
	if s.isPostV23ForNextTx() {
		disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
			agentID, rec, time.Now(),
		)
		if disclosureErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Memory authorization state is unavailable; retry later.")
			return
		}
		if !disclosure.Allowed {
			writeProblem(w, http.StatusForbidden, "Access denied",
				"You do not currently have read access to this memory.")
			return
		}
		classification := disclosure.Classification
		appV23Classification = &classification
	}
	if agentID != rec.SubmittingAgent {
		allowedAgents, seeAll := s.resolveVisibleAgents(agentID)
		if !seeAll {
			visible := false
			for _, a := range allowedAgents {
				if a == rec.SubmittingAgent {
					visible = true
					break
				}
			}
			if !visible {
				writeProblem(w, http.StatusForbidden, "Access denied",
					"You do not have visibility into this agent's memories.")
				return
			}
		}
	}

	// Access control gate: DomainAccess allowlist + multi-org classification.
	// Submitting agent always has access to their own memory.
	if rec.DomainTag != "" && agentID != rec.SubmittingAgent {
		// Per-agent DomainAccess allowlist gate — parity with query/search/hybrid/
		// list. Without it, an agent explicitly denied read on this domain could
		// still fetch the record one-by-one by id. Runs regardless of badgerStore
		// (checkDomainAccess falls back to the SQLite agentStore), matching the
		// sibling handlers. The submitter always retains access to its own memory.
		if accessErr := s.checkDomainAccess(r.Context(), agentID, rec.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		// Per-record classification — only when the domain has a registered owner.
		if s.badgerStore != nil {
			domainOwner, domainErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
			if domainErr == nil && domainOwner != "" {
				classification, _ := s.badgerStore.GetMemoryClassification(memoryID)
				hasAccess, accessErr := s.hasMemoryReadAccess(rec.DomainTag, agentID, classification, time.Now())
				if accessErr != nil || !hasAccess {
					writeProblem(w, http.StatusForbidden, "Access denied",
						fmt.Sprintf("No read access to domain %s", rec.DomainTag))
					return
				}
			}
		}
	}

	votes, _ := s.store.GetVotes(r.Context(), memoryID)
	corrs, corrRowsErr := s.store.GetCorroborations(r.Context(), memoryID)
	corrCounts, corrCountErr := s.store.GetCorroborationCounts(r.Context(), []string{memoryID})
	corrCountAvailable := corrCountErr == nil
	if corrCountErr != nil && corrRowsErr == nil {
		// The detail endpoint already has the rows, so retain an accurate distinct
		// count/confidence boost if the optimized aggregate query is unavailable.
		distinct := make(map[string]struct{}, len(corrs))
		for _, corr := range corrs {
			distinct[corr.AgentID] = struct{}{}
		}
		corrCounts = map[string]int{memoryID: len(distinct)}
		corrCountAvailable = true
	}
	challengeCounts, challengeCountErr := s.store.GetChallengeCounts(r.Context(), []string{memoryID})
	evidenceComplete, evidenceCompleteErr := store.EvidenceProjectionCompleteness(
		r.Context(), s.store, []string{memoryID},
	)
	roundProgress, roundProgressErr := s.challengeRoundProgressFor([]*memory.MemoryRecord{rec})
	if roundProgressErr != nil {
		writeProblem(w, http.StatusInternalServerError, "Memory error", "Failed to read challenge progress.")
		return
	}
	progress, hasProgress := roundProgress[memoryID]
	var challengeRound *uint64
	var currentChallengerCount, requiredChallengers *uint32
	if hasProgress {
		round, current, required := progress.round, progress.current, progress.required
		challengeRound = &round
		currentChallengerCount = &current
		requiredChallengers = &required
	}

	// Apply confidence decay.
	currentConf := memory.ComputeConfidenceForRecord(rec, time.Now(), corrCounts[memoryID])

	// Surface the on-chain classification so GET /v1/memory/{id} matches the
	// `classification` field the query/search/hybrid responses already return
	// (handleQueryMemory et al.). Read-display only — enforcement still happens
	// via the HasAccessMultiOrg gate above; this just stops the detail view from
	// silently reporting every record as PUBLIC (0). Guarded on badgerStore like
	// that gate, since some deployment modes run without it.
	var memClass uint8
	if appV23Classification != nil {
		memClass = *appV23Classification
	} else if s.badgerStore != nil {
		memClass, _ = s.badgerStore.GetMemoryClassification(memoryID)
	}

	writeJSON(w, http.StatusOK, MemoryDetailResponse{
		MemoryID:           rec.MemoryID,
		SubmittingAgent:    rec.SubmittingAgent,
		Content:            rec.Content,
		ContentHash:        hex.EncodeToString(rec.ContentHash),
		MemoryType:         string(rec.MemoryType),
		DomainTag:          rec.DomainTag,
		ConfidenceScore:    currentConf,
		Classification:     int(memClass),
		Status:             string(rec.Status),
		ParentHash:         rec.ParentHash,
		CreatedAt:          rec.CreatedAt,
		CommittedAt:        rec.CommittedAt,
		Votes:              votes,
		Corroborations:     corrs,
		CorroborationCount: corrCounts[memoryID],
		ChallengeCount:     challengeCounts[memoryID],
		EvidenceCountsAvailable: corrCountAvailable && challengeCountErr == nil &&
			evidenceCompleteErr == nil && evidenceComplete[memoryID],
		ChallengeRound:         challengeRound,
		CurrentChallengerCount: currentChallengerCount,
		RequiredChallengers:    requiredChallengers,
	})
}

// handlePreValidate handles POST /v1/memory/pre-validate.
// Runs the per-node validation checks (dedup, quality, consistency) against
// proposed content without submitting on-chain. The node votes accept iff all pass.
func (s *Server) handlePreValidate(w http.ResponseWriter, r *http.Request) {
	if s.PreValidateFunc == nil {
		writeProblem(w, http.StatusServiceUnavailable, "Not configured", "Pre-validation not configured on this node.")
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1 MB limit
	var req struct {
		Content    string  `json:"content"`
		Domain     string  `json:"domain"`
		Type       string  `json:"type"`
		Confidence float64 `json:"confidence"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	// Compute content hash (same as memory submission)
	hash := sha256.Sum256([]byte(req.Content))
	contentHash := hex.EncodeToString(hash[:])

	votes := s.PreValidateFunc(req.Content, contentHash, req.Domain, req.Type, req.Confidence)

	acceptCount := 0
	for _, v := range votes {
		if v.Decision == "accept" {
			acceptCount++
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"accepted": acceptCount == len(votes), // the node votes accept iff every check passes
		"votes":    votes,
		"quorum":   fmt.Sprintf("%d/%d", acceptCount, len(votes)),
	})
}

// --- Helpers -----------------------------------------------------------------

// broadcastTxCommit sends a transaction to CometBFT and waits for block finalization
// (CheckTx + FinalizeBlock). All REST handlers use this so that consensus-side
// rejections surface as real HTTP errors rather than 201 + ghost tx_hash.
func (s *Server) broadcastTxCommit(txBytes []byte) (string, error) {
	hash, _, err := s.broadcastTxCommitWithHeight(txBytes)
	return hash, err
}

// defaultBroadcastTxCommitTimeout bounds how long broadcastTxCommit
// waits for /broadcast_tx_commit to return. CometBFT's own server-
// side BroadcastTxCommitMaxWaitMs defaults to 10s; setting the client
// equal to that has no headroom — under any consensus slowness or
// mempool backlog the client times out before the server gets a
// chance to reply with the genuine FinalizeBlock result.
//
// 60s matches the typical Cosmos-ecosystem client-side default and
// gives ~10× the quorum-mode TimeoutCommit (3s) of room. Operators
// who run unusually slow consensus (single-validator with heavy
// validators, network-partitioned multi-node) can override via the
// SAGE_TX_COMMIT_TIMEOUT_MS environment variable.
const defaultBroadcastTxCommitTimeout = 60 * time.Second

func broadcastTxCommitTimeout() time.Duration {
	if v := os.Getenv("SAGE_TX_COMMIT_TIMEOUT_MS"); v != "" {
		if ms, err := strconv.Atoi(v); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return defaultBroadcastTxCommitTimeout
}

// broadcastTxCommitWithHeight is broadcastTxCommit plus the committed block
// height. Callers that need the on-chain height in their response (e.g.
// agent registration telemetry) use this variant; everything else stays
// on the simpler (hash, error) signature via broadcastTxCommit.
func (s *Server) broadcastTxCommitWithHeight(txBytes []byte) (string, int64, error) {
	return s.broadcastTxCommitWithHeightContext(context.Background(), txBytes)
}

// broadcastTxCommitWithHeightContext is the cancellation-aware form used by
// issuance workflows that must not leave a broadcast running after the caller
// has rolled back local state. Ordinary REST handlers retain the historical
// background+timeout behavior through broadcastTxCommitWithHeight.
func (s *Server) broadcastTxCommitWithHeightContext(parent context.Context, txBytes []byte) (string, int64, error) {
	txHex := hex.EncodeToString(txBytes)
	url := fmt.Sprintf("%s/broadcast_tx_commit?tx=0x%s", s.cometbftRPC, txHex)

	ctx, cancel := context.WithTimeout(parent, broadcastTxCommitTimeout())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil) // #nosec G107 -- internal CometBFT RPC
	if err != nil {
		return "", 0, fmt.Errorf("create broadcast request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", 0, fmt.Errorf("broadcast tx commit: %w", err)
	}
	defer resp.Body.Close()

	var result cometCommitResponse
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return "", 0, fmt.Errorf("decode broadcast commit response: %w", err)
	}

	if result.Error != nil {
		if result.Error.Data != "" {
			// error.data carries the real cause for mempool rejections
			// (message is just "Internal error") — fold it in so
			// broadcastErrorPublic can classify the failure.
			return "", 0, fmt.Errorf("broadcast error: %s: %s", result.Error.Message, result.Error.Data)
		}
		return "", 0, fmt.Errorf("broadcast error: %s", result.Error.Message)
	}

	if result.Result.CheckTx.Code != 0 {
		return "", 0, fmt.Errorf("tx rejected in CheckTx (code %d): %s", result.Result.CheckTx.Code, result.Result.CheckTx.Log)
	}

	if result.Result.TxResult.Code != 0 {
		return "", 0, fmt.Errorf("tx rejected in FinalizeBlock (code %d): %s", result.Result.TxResult.Code, result.Result.TxResult.Log)
	}

	return result.Result.Hash, result.Result.Height, nil
}

// broadcastErrorPublic returns the HTTP status and a sanitized public
// message for an error returned by broadcastTxCommit. The raw error string
// from CometBFT carries the full FinalizeBlock log, which previously leaked
// agent-id prefixes, internal codes, and "agent X not registered" oracles
// to REST callers. This helper maps known failure classes to canonical
// terse strings so the public response stays opaque while the server log
// retains the full diagnostic.
func broadcastErrorPublic(err error) (int, string) {
	if err == nil {
		return http.StatusInternalServerError, "internal error"
	}
	msg := err.Error()
	switch {
	case strings.Contains(msg, "access denied"):
		return http.StatusForbidden, "access denied"
	case strings.Contains(msg, "not in the validator set"):
		return http.StatusForbidden, "access denied"
	case strings.Contains(msg, "agent identity verification failed"):
		return http.StatusUnauthorized, "agent identity verification failed"
	case strings.Contains(msg, "not registered"):
		return http.StatusNotFound, "not found"
	case strings.Contains(msg, "mempool is full"):
		// CometBFT backpressure, not a fault. handleSubmitMemory upgrades
		// this to Retry-After + the distinct mempool-full problem type;
		// other broadcast callers at least surface the right status
		// instead of an opaque 500.
		return http.StatusTooManyRequests, "mempool full, retry later"
	// app-v16 deprecation-gate outcomes. These keys are matched BEFORE the generic
	// FinalizeBlock catch (the same error string would otherwise collapse to a
	// 400 "request rejected") and are ORDERED: the app-v16 "unknown memory" reject
	// also ends with "not authorized to deprecate", so the "no memory record" 404
	// must win over the 403. The 409 keys on the post-fork-only phrase "legacy
	// memory predating app-v8.4", so a PRE-app-v16 chain's domainless reject (which
	// lacks that phrase) falls through to the 403 authz case, not a 409 that would
	// advise an unavailable remediation.
	case strings.Contains(msg, "legacy memory predating app-v8.4"):
		return http.StatusConflict, "memory has no recorded domain (legacy pre-app-v8.4 record); deprecation is blocked until its domain is repaired via an OpMemoryDomainRepair governance proposal (app-v16)"
	case strings.Contains(msg, "no memory record"):
		return http.StatusNotFound, "unknown memory: no on-chain record for that memory id"
	case strings.Contains(msg, "not authorized to deprecate"):
		return http.StatusForbidden, "not authorized to deprecate this memory (need domain ownership or a level-3 modify grant)"
	case strings.Contains(msg, "reinstate:") && strings.Contains(msg, "not authorized"):
		return http.StatusForbidden, "not authorized to reinstate this memory (need domain ownership or a level-3 modify grant)"
	case strings.Contains(msg, "reinstate: memory") && strings.Contains(msg, "not found"):
		return http.StatusNotFound, "memory not found"
	case strings.Contains(msg, "is not challenged"):
		return http.StatusConflict, "memory is not currently challenged"
	case strings.Contains(msg, "delegated agent action mismatch"):
		return http.StatusConflict, "agent proof does not match the submitted action; update or reconnect the SAGE MCP client and retry"
	case strings.Contains(msg, "delegated agent proof timestamp") && strings.Contains(msg, "consensus window"):
		return http.StatusConflict, "agent proof expired before consensus; retry the request"
	case strings.Contains(msg, "tx rejected in CheckTx"):
		return http.StatusBadRequest, "request rejected"
	case strings.Contains(msg, "tx rejected in FinalizeBlock"):
		return http.StatusBadRequest, "request rejected"
	}
	return http.StatusInternalServerError, "internal error"
}

// mempoolFullProblemType is the RFC 7807 problem type stamped on the 429
// returned when CometBFT rejects a broadcast because its mempool is full.
// Distinct from the rate limiter's status-derived type
// (https://sage.dev/errors/429) so clients can tell chain backpressure
// ("everyone slow down") from a per-agent quota breach ("you slow down").
const mempoolFullProblemType = "https://sage.dev/errors/mempool-full"

// isMempoolFullErr reports whether a broadcast error is CometBFT's
// mempool-is-full rejection. The detail arrives in the JSON-RPC error.data
// field ("mempool is full: number of txs N (max: M)"), which
// broadcastTxCommitWithHeight folds into the wrapped error string.
func isMempoolFullErr(err error) bool {
	return err != nil && strings.Contains(err.Error(), "mempool is full")
}

func memoryTypeToTx(mt string) tx.MemoryType {
	switch mt {
	case "fact":
		return tx.MemoryTypeFact
	case "observation":
		return tx.MemoryTypeObservation
	case "inference":
		return tx.MemoryTypeInference
	case "task":
		return tx.MemoryTypeTask
	default:
		return tx.MemoryTypeFact
	}
}

// handleUpdateTaskStatus handles PUT /v1/memory/{memory_id}/task-status.
func (s *Server) handleUpdateTaskStatus(w http.ResponseWriter, r *http.Request) {
	memoryID := chi.URLParam(r, "memory_id")
	if memoryID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing memory_id", "memory_id is required.")
		return
	}

	var req struct {
		TaskStatus string `json:"task_status"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}

	ts := memory.TaskStatus(req.TaskStatus)
	if !memory.IsValidTaskStatus(ts) {
		writeProblem(w, http.StatusBadRequest, "Invalid task status",
			"task_status must be one of: planned, in_progress, done, dropped.")
		return
	}

	credentialID := middleware.ContextAgentID(r.Context())
	if credentialID == "" || s.agentStore == nil {
		writeProblem(w, http.StatusForbidden, "Active agent required", "A registered active agent identity is required.")
		return
	}
	agentID := credentialID
	if s.isPostV23ForNextTx() {
		active, activeErr := s.appV23ActiveOrdinaryAgent(credentialID)
		if activeErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Current local agent policy state could not be resolved.")
			return
		}
		if !active {
			writeProblem(w, http.StatusForbidden, "Active agent required",
				"Task status is an ordinary-agent action; CEREBRUM Root and inactive or stale identities cannot claim or complete tasks.")
			return
		}
		var policyErr error
		agentID, policyErr = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if policyErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Current Root policy state could not be resolved.")
			return
		}
	}
	agent, err := s.agentStore.GetAgent(r.Context(), agentID)
	if err != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil {
		writeProblem(w, http.StatusForbidden, "Active agent required", "A registered active agent identity is required.")
		return
	}
	rec, err := s.store.GetMemory(r.Context(), memoryID)
	if err != nil || rec == nil || rec.MemoryType != memory.TypeTask {
		writeProblem(w, http.StatusNotFound, "Task not found", "No task was found with that ID.")
		return
	}
	taskAction := "read"
	if s.isPostV23ForNextTx() {
		taskAction = "write"
	}
	if accessErr := s.checkDomainAccess(r.Context(), credentialID, rec.DomainTag, taskAction); accessErr != nil {
		writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
		return
	}
	if s.isPostV23ForNextTx() {
		disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
			credentialID, rec, time.Now(),
		)
		if disclosureErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "Task authorization could not be verified; retry later.")
			return
		}
		if !disclosure.Allowed {
			writeProblem(w, http.StatusForbidden, "Access denied", "No verified read access to this task.")
			return
		}
	} else if reader, ok := s.store.(interface {
		GetMemoryClassificationLocal(context.Context, string) (int, error)
	}); ok {
		classification, classErr := reader.GetMemoryClassificationLocal(r.Context(), memoryID)
		if classErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "Task authorization could not be verified; retry later.")
			return
		}
		if classification > 0 {
			if s.badgerStore == nil {
				writeProblem(w, http.StatusForbidden, "Access denied", "No verified read access to this classified task.")
				return
			}
			allowed, accessErr := s.hasMemoryReadAccess(rec.DomainTag, credentialID, uint8(classification), time.Now())
			if accessErr != nil || !allowed {
				writeProblem(w, http.StatusForbidden, "Access denied", "No verified read access to this classified task.")
				return
			}
		}
	} else {
		writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "This datastore cannot verify task classification for an agent status change.")
		return
	}

	var changed bool
	switch ts {
	case memory.TaskStatusInProgress:
		changed, err = s.store.ClaimTask(r.Context(), memoryID, agentID)
	case memory.TaskStatusDone, memory.TaskStatusDropped:
		assignmentStore, ok := s.store.(store.TaskAssignmentStore)
		if !ok {
			writeProblem(w, http.StatusNotImplemented, "Task completion unavailable", "This datastore does not support agent-owned task completion.")
			return
		}
		changed, err = assignmentStore.CompleteTaskAsAgent(r.Context(), memoryID, agentID, ts)
	case memory.TaskStatusPlanned:
		writeProblem(w, http.StatusForbidden, "Operator action required", "Only the local CEREBRUM task board can reopen or re-plan work.")
		return
	}
	if err != nil {
		writeProblem(w, http.StatusInternalServerError, "Task update failed", err.Error())
		return
	}
	if !changed {
		writeProblem(w, http.StatusConflict, "Task update conflict", "The task is terminal or not currently assigned to this agent.")
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"memory_id":   memoryID,
		"task_status": req.TaskStatus,
	})
}

// handleLinkMemories handles POST /v1/memory/link.
func (s *Server) handleLinkMemories(w http.ResponseWriter, r *http.Request) {
	var req struct {
		SourceID string `json:"source_id"`
		TargetID string `json:"target_id"`
		LinkType string `json:"link_type"`
	}
	if err := decodeJSON(r, &req); err != nil {
		writeProblem(w, http.StatusBadRequest, "Invalid request body", err.Error())
		return
	}
	if req.SourceID == "" || req.TargetID == "" {
		writeProblem(w, http.StatusBadRequest, "Missing IDs", "source_id and target_id are required.")
		return
	}
	if req.LinkType == "" {
		req.LinkType = "related"
	}

	credentialID := middleware.ContextAgentID(r.Context())
	if credentialID == "" || s.agentStore == nil || s.badgerStore == nil {
		writeProblem(w, http.StatusForbidden, "Active agent required", "A registered active agent identity is required.")
		return
	}
	agentID := credentialID
	if s.isPostV23ForNextTx() {
		var policyErr error
		agentID, policyErr = appV23PolicyPrincipal(s.badgerStore, credentialID)
		if policyErr != nil {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Current Root policy state could not be resolved.")
			return
		}
	}
	agent, err := s.agentStore.GetAgent(r.Context(), agentID)
	onChainAgent, onChainErr := s.badgerStore.GetRegisteredAgent(agentID)
	if err != nil || agent == nil || agent.Status != "active" || agent.RemovedAt != nil ||
		onChainErr != nil || onChainAgent == nil {
		writeProblem(w, http.StatusForbidden, "Active agent required", "A registered active agent identity is required.")
		return
	}

	source, sourceErr := s.store.GetMemory(r.Context(), req.SourceID)
	target, targetErr := s.store.GetMemory(r.Context(), req.TargetID)
	if sourceErr != nil || targetErr != nil || source == nil || target == nil {
		writeProblem(w, http.StatusNotFound, "Memory not found", "One or more memories were not found.")
		return
	}

	allowedSubmitters, seeAll := s.resolveVisibleAgents(credentialID)
	if !seeAll &&
		(!containsAgentID(allowedSubmitters, source.SubmittingAgent) ||
			!containsAgentID(allowedSubmitters, target.SubmittingAgent)) {
		writeProblem(w, http.StatusNotFound, "Memory not found", "One or more memories were not found.")
		return
	}

	if accessErr := s.checkDomainAccess(r.Context(), credentialID, source.DomainTag, "modify"); accessErr != nil {
		writeProblem(w, http.StatusForbidden, "Access denied", "Modify access to the source memory is required.")
		return
	}
	if !s.isPostV23ForNextTx() {
		sourceOwner, ownerErr := s.badgerStore.IsDomainOwnerOrAncestor(source.DomainTag, agentID)
		sourceModify, modifyErr := s.badgerStore.HasAccessOrAncestor(source.DomainTag, agentID, 3, time.Now())
		if ownerErr != nil || modifyErr != nil || (!sourceOwner && !sourceModify && onChainAgent.Role != "admin") {
			writeProblem(w, http.StatusForbidden, "Access denied", "Modify access to the source memory is required.")
			return
		}
	}

	for _, rec := range []*memory.MemoryRecord{source, target} {
		if accessErr := s.checkDomainAccess(r.Context(), credentialID, rec.DomainTag, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", "Read access to both memories is required.")
			return
		}
		if s.isPostV23ForNextTx() {
			disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
				credentialID, rec, time.Now(),
			)
			if disclosureErr != nil {
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "Memory authorization could not be verified; retry later.")
				return
			}
			if !disclosure.Allowed {
				writeProblem(w, http.StatusForbidden, "Access denied", "Read access to both memories is required.")
				return
			}
		} else {
			if onChainAgent.Role == "admin" {
				continue
			}
			classification, classErr := s.badgerStore.GetMemoryClassification(rec.MemoryID)
			if classErr != nil {
				writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable", "Memory authorization could not be verified; retry later.")
				return
			}
			allowed, accessErr := s.hasMemoryReadAccess(rec.DomainTag, credentialID, classification, time.Now())
			if accessErr != nil || !allowed {
				writeProblem(w, http.StatusForbidden, "Access denied", "Read access to both memories is required.")
				return
			}
		}
	}

	if err := s.store.LinkMemories(r.Context(), req.SourceID, req.TargetID, req.LinkType); err != nil {
		writeProblem(w, http.StatusInternalServerError, "Link failed", err.Error())
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{
		"source_id": req.SourceID,
		"target_id": req.TargetID,
		"link_type": req.LinkType,
	})
}

func containsAgentID(agentIDs []string, target string) bool {
	for _, agentID := range agentIDs {
		if agentID == target {
			return true
		}
	}
	return false
}

// handleGetOpenTasks handles GET /v1/memory/tasks.
func (s *Server) handleGetOpenTasks(w http.ResponseWriter, r *http.Request) {
	domain := r.URL.Query().Get("domain")
	provider := r.URL.Query().Get("provider")

	agentID := middleware.ContextAgentID(r.Context())
	if !s.requireAppV23ActiveOrdinaryAgent(w, agentID, "the open-task agent API") {
		return
	}

	// Per-domain read ACL — parity with /v1/memory/list and the query/hybrid
	// recall path. This handler returns full task CONTENT, so without the gate an
	// agent could enumerate the content of every open task in a domain it has no
	// read grant on — and, because `domain` is optional, across ALL domains at
	// once. When a specific domain is requested we 403 up front; the per-record
	// filter below covers the cross-domain (no-domain) case. checkDomainAccess is
	// the operative per-domain gate; the multi-org block below mirrors query/list
	// for shape-parity (a no-op once checkDomainAccess approves a concrete domain).
	domainAccessApproved := false
	if domain != "" {
		if accessErr := s.checkDomainAccess(r.Context(), agentID, domain, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	if domain != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domErr := s.badgerStore.GetDomainOwner(domain)
		if domErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.hasMemoryReadAccess(domain, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domain))
				return
			}
		}
	}

	var tasks []*memory.MemoryRecord
	var err error
	if s.isPostV23ForNextTx() {
		if pager, ok := s.store.(store.OpenTaskPageStore); ok {
			tasks, err = s.collectAppV23VisibleRecords(
				r.Context(), agentID, 500,
				func(ctx context.Context, limit, offset int) ([]*memory.MemoryRecord, error) {
					return pager.GetOpenTasksPage(
						ctx, domain, provider, agentID, limit, offset,
					)
				},
			)
		} else {
			// Compatibility fallback for external MemoryStore implementations.
			// Built-in SQLite/Postgres stores implement the paging extension.
			tasks, err = s.store.GetOpenTasks(r.Context(), domain, provider, agentID)
		}
	} else {
		tasks, err = s.store.GetOpenTasks(r.Context(), domain, provider, agentID)
	}
	if err != nil {
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Task authorization state is unavailable; retry later.")
			return
		}
		if errors.Is(err, errAppV23DisclosureScanBudget) {
			writeProblem(w, http.StatusUnprocessableEntity, "Task query too broad",
				"Too many task candidates require authorization; choose a domain or provider filter.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Failed to get tasks", err.Error())
		return
	}

	// Per-record gate — parity with handleListMemoriesAuth. Drops tasks whose
	// domain the caller cannot read (covers the no-domain cross-domain board) and
	// tasks classified above the caller's clearance. App-v23 deliberately has no
	// authorship bypass. GetOpenTasks has already applied exact immutable-assignee
	// isolation; this second pass protects tasks assigned to the caller but
	// submitted by a different agent.
	if s.badgerStore != nil {
		now := time.Now()
		kept := tasks[:0]
		for _, rec := range tasks {
			if s.isPostV23ForNextTx() {
				disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
					agentID, rec, now,
				)
				if disclosureErr != nil {
					if isUnsafeAppV23Projection(disclosureErr) {
						continue
					}
					writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
						"Task authorization state is unavailable; retry later.")
					return
				}
				if !disclosure.Allowed {
					continue
				}
			} else if rec.SubmittingAgent != agentID {
				// Domain-read filter — only needed when no single domain was pre-gated.
				if domain == "" && rec.DomainTag != "" {
					if accessErr := s.checkDomainAccess(r.Context(), agentID, rec.DomainTag, "read"); accessErr != nil {
						continue
					}
				}
				// Classification filter.
				if rec.DomainTag != "" {
					memClass, _ := s.badgerStore.GetMemoryClassification(rec.MemoryID)
					if memClass > 0 {
						domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
						if domErr == nil && domainOwner != "" {
							hasAccess, _ := s.hasMemoryReadAccess(rec.DomainTag, agentID, memClass, now)
							if !hasAccess {
								continue
							}
						}
					}
				}
			}
			kept = append(kept, rec)
		}
		tasks = kept
	}

	type taskResult struct {
		MemoryID        string  `json:"memory_id"`
		Content         string  `json:"content"`
		DomainTag       string  `json:"domain_tag"`
		TaskStatus      string  `json:"task_status"`
		Assignee        string  `json:"assignee"`
		TaskPickedUpBy  string  `json:"task_picked_up_by,omitempty"`
		TaskPickedUpAt  string  `json:"task_picked_up_at,omitempty"`
		ConfidenceScore float64 `json:"confidence_score"`
		CreatedAt       string  `json:"created_at"`
	}

	results := make([]taskResult, 0, len(tasks))
	for _, t := range tasks {
		pickedUpAt := ""
		if t.TaskPickedUpAt != nil {
			pickedUpAt = t.TaskPickedUpAt.UTC().Format(time.RFC3339)
		}
		results = append(results, taskResult{
			MemoryID:        t.MemoryID,
			Content:         t.Content,
			DomainTag:       t.DomainTag,
			TaskStatus:      string(t.TaskStatus),
			Assignee:        t.Assignee,
			TaskPickedUpBy:  t.TaskPickedUpBy,
			TaskPickedUpAt:  pickedUpAt,
			ConfidenceScore: t.ConfidenceScore,
			CreatedAt:       t.CreatedAt.Format("2006-01-02T15:04:05Z"),
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"tasks": results,
		"total": len(results),
	})
}

// generateUUID creates a random UUID v4 string without an external dependency.
func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	// Set version 4 and variant bits per RFC 4122.
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func truncateContent(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen] + "..."
}

const appV23MaxVisibleOffset = 7_900

var errAppV23VisibleOffsetTooLarge = errors.New("offset exceeds the app-v23 visible pagination limit")

// listAppV23VisibleMemories paginates after live authorization, rather than
// applying SQL offset/limit first. That makes total/offset/limit describe only
// records the caller may currently read and prevents revoked rows from starving
// a page. Each store read is bounded and the walk stops after one extra visible
// row, so mature nodes do not pay a full-inventory scan merely to render page 1.
// `total` is therefore an authorization-safe lower bound while hasMore is true;
// it becomes exact only when the filtered stream is exhausted.
func (s *Server) listAppV23VisibleMemories(
	ctx context.Context,
	opts store.ListOptions,
	agentID string,
) ([]*memory.MemoryRecord, int, bool, bool, error) {
	requestedLimit, requestedOffset := opts.Limit, opts.Offset
	if requestedOffset < 0 {
		return nil, 0, false, false, errors.New("offset must not be negative")
	}
	if requestedOffset > appV23MaxVisibleOffset {
		return nil, 0, false, false, errAppV23VisibleOffsetTooLarge
	}
	scan := opts
	scan.Limit = 200
	scan.Offset = 0
	scan.StablePaging = true
	// Keep only the requested page plus one look-ahead row. Even a large but
	// valid offset therefore consumes O(limit), not O(offset), memory.
	visible := make([]*memory.MemoryRecord, 0, requestedLimit+1)
	visibleSeen := 0
	now := time.Now()
	exhausted := false
	for {
		if scan.Offset >= appV23DisclosureScanBudget {
			return nil, 0, false, false, errAppV23DisclosureScanBudget
		}
		scan.Limit = min(200, appV23DisclosureScanBudget-scan.Offset)
		batch, total, err := s.store.ListMemories(ctx, scan)
		if err != nil {
			return nil, 0, false, false, err
		}
		batchExhaustsStream := len(batch) < scan.Limit || scan.Offset+len(batch) >= total
		for _, rec := range batch {
			disclosure, err := s.evaluateAppV23RecordDisclosure(agentID, rec, now)
			if err != nil {
				if isUnsafeAppV23Projection(err) {
					continue
				}
				return nil, 0, false, false, appV23RecordDisclosureError(err)
			}
			if disclosure.Allowed {
				visibleSeen++
				if visibleSeen > requestedOffset {
					visible = append(visible, rec)
				}
				if len(visible) > requestedLimit {
					break
				}
			}
		}
		if len(visible) > requestedLimit {
			exhausted = batchExhaustsStream
			break
		}
		scan.Offset += len(batch)
		if batchExhaustsStream {
			exhausted = true
			break
		}
	}
	totalVisibleLowerBound := visibleSeen
	hasMore := len(visible) > requestedLimit
	if hasMore {
		visible = visible[:requestedLimit]
	}
	// Breaking after the look-ahead row can happen inside the store's final
	// batch. In that case the backing stream is exhausted, but the authorized
	// stream has not been counted to completion because unvisited rows remain
	// in that batch. A page with has_more=true is therefore always a lower
	// bound, never an exact total.
	totalExact := exhausted && !hasMore
	return visible, totalVisibleLowerBound, hasMore, totalExact, nil
}

// handleListMemoriesAuth handles GET /v1/memory/list (authenticated, agent-isolated).
// Mirrors the dashboard list endpoint but applies RBAC agent isolation.
func (s *Server) handleListMemoriesAuth(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit, _ := strconv.Atoi(q.Get("limit"))
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	offset := 0
	if rawOffset := q.Get("offset"); rawOffset != "" {
		var offsetErr error
		offset, offsetErr = strconv.Atoi(rawOffset)
		if offsetErr != nil || offset < 0 {
			writeProblem(w, http.StatusBadRequest, "Invalid offset",
				"offset must be a non-negative integer.")
			return
		}
	}
	postV23 := s.isPostV23ForNextTx()
	if postV23 && offset > appV23MaxVisibleOffset {
		writeProblem(w, http.StatusUnprocessableEntity, "Offset too large",
			fmt.Sprintf("offset must not exceed %d under app-v23; narrow the query or page sequentially.", appV23MaxVisibleOffset))
		return
	}

	agentID := middleware.ContextAgentID(r.Context())
	domainFilter := q.Get("domain")

	// Per-domain read ACL — parity with /v1/memory/query, /search and /hybrid.
	// Until this gate was added, list skipped checkDomainAccess entirely, so an
	// agent with no read grant on a domain could enumerate that domain's record
	// CONTENT via list even though hybrid/query correctly 403 — a §5
	// compartmentation hole reported against a multi-node deployment on v10.1.0.
	// Gating here, before the store query, closes it for EVERY status (not just
	// committed). Mirrors handleQueryMemory's two-step domain gate.
	domainAccessApproved := false
	if domainFilter != "" {
		if accessErr := s.checkDomainAccess(r.Context(), agentID, domainFilter, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	// Multi-org access control gate — only enforce when the domain has a
	// registered owner AND the agent wasn't already approved above.
	if domainFilter != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domainErr := s.badgerStore.GetDomainOwner(domainFilter)
		if domainErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.hasMemoryReadAccess(domainFilter, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domainFilter))
				return
			}
		}
	}

	allowedAgents, seeAll := s.resolveVisibleAgents(agentID)
	legacyVisibilityRestricted := s.appV23LegacyVisibilityRestricted(agentID)
	if s.appV25RecoveredReadOverridesLegacyVisibility(agentID, domainFilter) {
		seeAll = true
		legacyVisibilityRestricted = false
	}

	// Grant-aware override: if listing a specific domain, skip agent isolation when:
	// (a) the agent has a direct grant on the domain, or
	// (b) the agent has org-level access (clearance >= classification), or
	// (c) the domain has no registered owner (no access policy = open visibility)
	if !seeAll && !legacyVisibilityRestricted && domainFilter != "" && s.badgerStore != nil {
		hasGrant, _ := s.badgerStore.HasAccess(domainFilter, agentID, 1, time.Now())
		if hasGrant {
			seeAll = true
		} else {
			hasOrgAccess, _ := s.hasMemoryReadAccess(domainFilter, agentID, 0, time.Now())
			if hasOrgAccess {
				seeAll = true
			} else {
				_, ownerErr := s.badgerStore.GetDomainOwner(domainFilter)
				if ownerErr != nil {
					seeAll = true
				}
			}
		}
	}

	opts := store.ListOptions{
		DomainTag: domainFilter,
		Tag:       q.Get("tag"),
		Provider:  q.Get("provider"),
		Status:    q.Get("status"),
		Limit:     limit,
		Offset:    offset,
		Sort:      q.Get("sort"),
	}
	// Apply single-agent filter from query param (only if allowed)
	if agent := q.Get("agent"); agent != "" {
		opts.SubmittingAgent = agent
	}
	filterApplied := !seeAll
	if filterApplied {
		opts.SubmittingAgents = allowedAgents
	}

	var records []*memory.MemoryRecord
	var total int
	var hasMore, totalExact bool
	var err error
	if postV23 {
		records, total, hasMore, totalExact, err = s.listAppV23VisibleMemories(
			r.Context(), opts, agentID,
		)
	} else {
		records, total, err = s.store.ListMemories(r.Context(), opts)
	}
	if err != nil {
		status := http.StatusInternalServerError
		title := "Query error"
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			status = http.StatusServiceUnavailable
			title = "Authorization unavailable"
		} else if errors.Is(err, errAppV23VisibleOffsetTooLarge) {
			status = http.StatusUnprocessableEntity
			title = "Offset too large"
		} else if errors.Is(err, errAppV23DisclosureScanBudget) {
			status = http.StatusUnprocessableEntity
			title = "Query too broad"
		}
		writeProblem(w, status, title, err.Error())
		return
	}

	// Per-record gate — parity with the query/hybrid recall path. The up-front
	// domain gate only runs when a domain filter is supplied; on the no-domain
	// path the store returns records across ALL domains, so we additionally drop
	// records whose domain the caller cannot read (a seeAll caller — visible_agents
	// "*", TopSecret, operator — otherwise gets PUBLIC content from domains it has
	// no grant on). The classification gate then drops records classified above the
	// caller's clearance even within a readable domain. App-v23 deliberately has
	// no authorship bypass.
	hiddenByClassification := 0
	if s.badgerStore != nil && !postV23 {
		now := time.Now()
		kept := records[:0]
		for _, rec := range records {
			if rec.SubmittingAgent != agentID {
				// Domain-read filter — only needed when no single domain was pre-gated.
				if domainFilter == "" && rec.DomainTag != "" {
					if accessErr := s.checkDomainAccess(r.Context(), agentID, rec.DomainTag, "read"); accessErr != nil {
						continue
					}
				}
				// Classification filter.
				memClass, _ := s.badgerStore.GetMemoryClassification(rec.MemoryID)
				if memClass > 0 {
					domainOwner, domErr := s.badgerStore.GetDomainOwner(rec.DomainTag)
					if domErr == nil && domainOwner != "" {
						hasAccess, _ := s.hasMemoryReadAccess(rec.DomainTag, agentID, memClass, now)
						if !hasAccess {
							hiddenByClassification++
							continue
						}
					}
				}
			}
			kept = append(kept, rec)
		}
		records = kept
	}

	body := map[string]any{
		"memories": records,
		"total":    total,
		"limit":    limit,
		"offset":   offset,
	}

	if s.isPostV23ForNextTx() {
		body["has_more"] = hasMore
		body["total_exact"] = totalExact
		if filterApplied {
			info := &FilterInfo{By: []string{filterBySubmittingAgts}}
			w.Header().Set(filterHeader, filterBySubmittingAgts)
			body["filtered"] = info
		}
	} else if filterApplied || hiddenByClassification > 0 {
		applied := make([]string, 0, 2)
		info := &FilterInfo{}
		if filterApplied {
			// Second count-only query without the SubmittingAgents filter so the
			// caller can distinguish empty-domain from rbac-hidden. Limit:1 keeps
			// row materialization bounded; store.ListMemories computes total anyway.
			unfilteredOpts := opts
			unfilteredOpts.SubmittingAgents = nil
			unfilteredOpts.Limit = 1
			unfilteredOpts.Offset = 0
			_, totalBefore, countErr := s.store.ListMemories(r.Context(), unfilteredOpts)
			applied = append(applied, filterBySubmittingAgts)
			visible := total
			info.Visible = &visible
			if countErr == nil {
				info.TotalBeforeFilter = &totalBefore
			}
		}
		if hiddenByClassification > 0 {
			applied = append(applied, filterByClassification)
			hc := hiddenByClassification
			info.HiddenCount = &hc
		}
		info.By = applied
		w.Header().Set(filterHeader, strings.Join(applied, ","))
		body["filtered"] = info
	}

	writeJSON(w, http.StatusOK, body)
}

const (
	appV23TimelineMaxRange      = 31 * 24 * time.Hour
	appV23TimelineMaxRawRecords = appV23DisclosureScanBudget
)

var errAppV23TimelineWorkLimit = errors.New("app-v23 timeline work limit exceeded")

func appV23TimelinePeriod(at time.Time, bucket string, postgresShape bool) string {
	at = at.UTC()
	if postgresShape {
		switch bucket {
		case "hour":
			return at.Truncate(time.Hour).Format(time.RFC3339)
		case "week":
			weekday := (int(at.Weekday()) + 6) % 7
			return time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC).
				AddDate(0, 0, -weekday).Format(time.RFC3339)
		case "month":
			return time.Date(at.Year(), at.Month(), 1, 0, 0, 0, 0, time.UTC).
				Format(time.RFC3339)
		default:
			return time.Date(at.Year(), at.Month(), at.Day(), 0, 0, 0, 0, time.UTC).
				Format(time.RFC3339)
		}
	}
	switch bucket {
	case "hour":
		return at.Truncate(time.Hour).Format("2006-01-02T15:00:00Z")
	case "week":
		// Match SQLite strftime("%Y-W%W"): Monday starts the week and days
		// before the first Monday belong to week 00.
		yearStart := time.Date(at.Year(), time.January, 1, 0, 0, 0, 0, time.UTC)
		firstMondayOffset := (8 - int(yearStart.Weekday())) % 7
		yearDay := at.YearDay() - 1
		week := 0
		if yearDay >= firstMondayOffset {
			week = ((yearDay - firstMondayOffset) / 7) + 1
		}
		return fmt.Sprintf("%04d-W%02d", at.Year(), week)
	case "month":
		return at.Format("2006-01")
	default:
		return at.Format("2006-01-02")
	}
}

// getAppV23VisibleTimeline applies the same per-record live disclosure decision
// as list/query before counting. Aggregate existence and timing are governed
// metadata: a denied record must not increment a public bucket. Raw store pages
// stay bounded and deterministic; unlike the ordinary list route, aggregation
// intentionally walks the complete requested time range to produce exact counts.
func (s *Server) getAppV23VisibleTimeline(
	ctx context.Context,
	agentID string,
	from, to time.Time,
	domain, bucket string,
) ([]store.TimelineBucket, error) {
	const pageSize = 200
	formatter, hasFormatter := s.store.(store.TimelinePeriodFormatter)
	// SQLite stores RFC3339Nano as text, whose optional fractional component is
	// not perfectly lexicographic at a whole-second boundary. Widen the SQL
	// range by one second and apply the exact time predicate below.
	opts := store.ListOptions{
		DomainTag:    domain,
		CreatedFrom:  from.UTC().Add(-time.Second).Format(time.RFC3339Nano),
		CreatedTo:    to.UTC().Add(time.Second).Format(time.RFC3339Nano),
		Limit:        pageSize,
		Sort:         "oldest",
		StablePaging: true,
	}
	counts := make(map[string]int)
	now := time.Now()
	for {
		batch, total, err := s.store.ListMemories(ctx, opts)
		if err != nil {
			return nil, err
		}
		if total > appV23TimelineMaxRawRecords ||
			opts.Offset > appV23TimelineMaxRawRecords-len(batch) {
			return nil, errAppV23TimelineWorkLimit
		}
		for _, rec := range batch {
			if rec.CreatedAt.Before(from) || rec.CreatedAt.After(to) {
				continue
			}
			disclosure, disclosureErr := s.evaluateAppV23RecordDisclosure(
				agentID, rec, now,
			)
			if disclosureErr != nil {
				if isUnsafeAppV23Projection(disclosureErr) {
					continue
				}
				return nil, appV23RecordDisclosureError(disclosureErr)
			}
			if disclosure.Allowed {
				period := appV23TimelinePeriod(rec.CreatedAt, bucket, false)
				if hasFormatter {
					period = formatter.FormatTimelinePeriod(rec.CreatedAt, bucket)
				}
				counts[period]++
			}
		}
		opts.Offset += len(batch)
		if len(batch) < pageSize || opts.Offset >= total || len(batch) == 0 {
			break
		}
	}

	periods := make([]string, 0, len(counts))
	for period := range counts {
		periods = append(periods, period)
	}
	sort.Strings(periods)
	buckets := make([]store.TimelineBucket, 0, len(periods))
	for _, period := range periods {
		buckets = append(buckets, store.TimelineBucket{
			Period: period,
			Count:  counts[period],
			Domain: domain,
		})
	}
	return buckets, nil
}

// handleTimelineAuth handles GET /v1/memory/timeline. Before app-v23 the
// historical aggregate route is preserved. App-v23 treats aggregate
// existence/timing as governed data and counts only records that pass the
// caller's current live disclosure decision.
func (s *Server) handleTimelineAuth(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	domain := q.Get("domain")
	bucket := q.Get("bucket")
	if bucket == "" {
		bucket = "hour"
	}

	// Per-domain read ACL — parity with the other domain-keyed reads. The buckets
	// are aggregate counts (no content), but submission volume over time for a
	// domain is still a metadata signal the caller should hold a read grant for.
	// Pre-v23 global (no-domain) counts retain their historical behavior. Under
	// app-v23 the per-record aggregation below filters both global and scoped
	// timelines through current disclosure authority. checkDomainAccess is the
	// operative up-front gate for a concrete domain.
	agentID := middleware.ContextAgentID(r.Context())
	domainAccessApproved := false
	if domain != "" {
		if accessErr := s.checkDomainAccess(r.Context(), agentID, domain, "read"); accessErr != nil {
			writeProblem(w, http.StatusForbidden, "Access denied", accessErr.Error())
			return
		}
		domainAccessApproved = true
	}
	if domain != "" && !domainAccessApproved && s.badgerStore != nil {
		domainOwner, domErr := s.badgerStore.GetDomainOwner(domain)
		if domErr == nil && domainOwner != "" {
			hasAccess, accessErr := s.hasMemoryReadAccess(domain, agentID, 0, time.Now())
			if accessErr != nil || !hasAccess {
				writeProblem(w, http.StatusForbidden, "Access denied",
					fmt.Sprintf("No read access to domain %s", domain))
				return
			}
		}
	}

	from := time.Now().Add(-24 * time.Hour)
	to := time.Now()
	if v := q.Get("from"); v != "" {
		t, parseErr := time.Parse(time.RFC3339, v)
		if parseErr != nil {
			if s.isPostV23ForNextTx() {
				writeProblem(w, http.StatusBadRequest, "Invalid timeline range",
					"from must be an RFC3339 timestamp.")
				return
			}
		} else {
			from = t
		}
	}
	if v := q.Get("to"); v != "" {
		t, parseErr := time.Parse(time.RFC3339, v)
		if parseErr != nil {
			if s.isPostV23ForNextTx() {
				writeProblem(w, http.StatusBadRequest, "Invalid timeline range",
					"to must be an RFC3339 timestamp.")
				return
			}
		} else {
			to = t
		}
	}
	if s.isPostV23ForNextTx() {
		if from.After(to) {
			writeProblem(w, http.StatusBadRequest, "Invalid timeline range",
				"from must be earlier than or equal to to.")
			return
		}
		if to.Sub(from) > appV23TimelineMaxRange {
			writeProblem(w, http.StatusUnprocessableEntity, "Timeline range too large",
				"App-v23 governed timelines are limited to 31 days per request; choose a narrower range.")
			return
		}
	}

	var buckets []store.TimelineBucket
	var err error
	if s.isPostV23ForNextTx() {
		buckets, err = s.getAppV23VisibleTimeline(
			r.Context(), agentID, from, to, domain, bucket,
		)
	} else {
		buckets, err = s.store.GetTimeline(r.Context(), from, to, domain, bucket)
	}
	if err != nil {
		if errors.Is(err, errAppV23TimelineWorkLimit) {
			writeProblem(w, http.StatusUnprocessableEntity, "Timeline result too large",
				"The governed timeline contains too many records to authorize safely; choose a narrower range or domain.")
			return
		}
		if errors.Is(err, errAppV23RecordDisclosureUnavailable) {
			writeProblem(w, http.StatusServiceUnavailable, "Authorization unavailable",
				"Timeline authorization state is unavailable; retry later.")
			return
		}
		writeProblem(w, http.StatusInternalServerError, "Query error", err.Error())
		return
	}

	total := 0
	for _, timelineBucket := range buckets {
		total += timelineBucket.Count
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"buckets": buckets,
		"total":   total,
	})
}

// runHybridWithExpansions runs SearchHybrid once for the primary query plus
// once per expansion variant, then fuses the variant rankings into a single
// list via RRF across variants. With no expansions the call collapses to a
// single SearchHybrid invocation (zero overhead vs the v7.0 path).
func (s *Server) runHybridWithExpansions(ctx context.Context, req HybridSearchMemoryRequest, opts store.QueryOptions) ([]*memory.MemoryRecord, error) {
	if len(req.Expansions) > maxHybridExpansions {
		return nil, fmt.Errorf(
			"hybrid search accepts at most %d expansion variants per request",
			maxHybridExpansions,
		)
	}

	// One governed request gets one authorization budget across the primary,
	// every expansion, and every store leaf behind SearchHybrid. Reusing the
	// same closure is intentional: a caller cannot reset the live Badger/RBAC
	// work ceiling merely by adding another paraphrase.
	if candidateFilter := opts.CandidateFilter; candidateFilter != nil {
		candidateChecks := 0
		opts.CandidateFilter = func(rec *memory.MemoryRecord) (bool, error) {
			if candidateChecks >= store.CandidateFilterScanBudget {
				return false, store.ErrCandidateFilterScanBudgetExceeded
			}
			candidateChecks++
			return candidateFilter(rec)
		}
	}

	// Primary query always runs first; expansions follow. Each variant gets
	// the same QueryOptions (domain, RBAC, filters) so the only thing that
	// varies between calls is the query text + its embedding.
	type variant struct {
		query     string
		embedding []float32
	}
	variants := make([]variant, 0, 1+len(req.Expansions))
	variants = append(variants, variant{query: req.Query, embedding: req.Embedding})
	for _, e := range req.Expansions {
		// Skip empties so callers can pass best-effort expansion lists
		// without us amplifying the no-op into wasted SearchHybrid work.
		if e.Query == "" && len(e.Embedding) == 0 {
			continue
		}
		variants = append(variants, variant{query: e.Query, embedding: e.Embedding})
	}

	// Fast path: one variant means the legacy single-call behaviour applies.
	if len(variants) == 1 {
		return s.store.SearchHybrid(ctx, variants[0].query, variants[0].embedding, opts)
	}

	// Multi-variant path: each call gets its own ranked list. We RRF-merge
	// across variants so a memory that ranks high under several paraphrases
	// outscores one that only matched a single variant. Keeps the constant
	// in lockstep with internal/store/hybrid.go's RRF_K so the rank-fusion
	// shape is consistent end-to-end.
	const rrfKAcrossVariants = 60

	scores := make(map[string]float64)
	records := make(map[string]*memory.MemoryRecord)
	successfulVariants := 0
	var firstVariantErr error

	for _, v := range variants {
		got, err := s.store.SearchHybrid(ctx, v.query, v.embedding, opts)
		if err != nil {
			// Governed recall and decayed floors both fail closed. In
			// particular, never turn an exhausted aggregate authorization
			// budget into a misleading 200 with partial expansion results.
			if opts.CandidateFilter != nil || opts.DecayFloor > 0 {
				return nil, err
			}
			if firstVariantErr == nil {
				firstVariantErr = err
			}
			// Otherwise a single variant failing shouldn't drop the whole recall —
			// the user paid for an expansion, not a fragility tax. Log and continue
			// with the variants that did succeed.
			s.logger.Warn().Err(err).Str("variant_query", v.query).Msg("expansion variant SearchHybrid failed; skipping")
			continue
		}
		successfulVariants++
		for rank, rec := range got {
			scores[rec.MemoryID] += 1.0 / float64(rrfKAcrossVariants+rank+1)
			if _, ok := records[rec.MemoryID]; !ok {
				records[rec.MemoryID] = rec
			}
		}
	}

	if successfulVariants == 0 && firstVariantErr != nil {
		return nil, firstVariantErr
	}
	if len(records) == 0 {
		return nil, nil
	}

	type ranked struct {
		mem   *memory.MemoryRecord
		score float64
	}
	out := make([]ranked, 0, len(records))
	for id, rec := range records {
		out = append(out, ranked{mem: rec, score: scores[id]})
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].score > out[j].score })

	topK := opts.TopK
	if topK <= 0 {
		topK = 10
	}
	// Cap topK to prevent uncontrolled allocation on attacker-supplied
	// JSON like {"top_k": 2147483647}. Anything above 1k is unreasonable
	// for a single search response.
	const maxTopK = 1000
	if topK > maxTopK {
		topK = maxTopK
	}
	if topK > len(out) {
		topK = len(out)
	}
	final := make([]*memory.MemoryRecord, topK)
	for i := 0; i < topK; i++ {
		final[i] = out[i].mem
	}
	return final, nil
}
