package abci

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/l33tdawg/sage/internal/store"
	memorytags "github.com/l33tdawg/sage/internal/tags"
	"github.com/l33tdawg/sage/internal/tx"
)

const delegatedAgentProofSkew = 5 * time.Minute

var delegatedChainIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)

type signedAgentRequest struct {
	method string
	path   string
	body   []byte
}

// enforceDelegatedAgentProof closes the delegated-signing gap that existed in
// the original app-v17 candidate. When the outer transaction signer is not the
// agent, consensus must independently prove that the signed HTTP request maps
// to this exact type-specific payload. Same-key node-originated transactions
// are already bound by the outer transaction signature and nonce.
func (app *SageApp) enforceDelegatedAgentProof(parsedTx *tx.ParsedTx, consensusTime time.Time, claim bool, bindMemoryTags bool) error {
	if !txUsesAgentIdentity(parsedTx.Type) || bytes.Equal(parsedTx.PublicKey, parsedTx.AgentPubKey) {
		return nil
	}
	if len(parsedTx.AgentRequest) == 0 {
		return fmt.Errorf("delegated agent proof is missing its signed request")
	}
	if len(parsedTx.AgentRequest) > tx.MaxAgentRequestSize {
		return fmt.Errorf("delegated agent request exceeds the size limit")
	}

	requestHash := sha256.Sum256(parsedTx.AgentRequest)
	if !bytes.Equal(requestHash[:], parsedTx.AgentBodyHash) {
		return fmt.Errorf("delegated agent request hash does not match the signed proof")
	}
	agentID, err := verifyAgentIdentity(parsedTx)
	if err != nil {
		return fmt.Errorf("delegated agent signature is invalid: %w", err)
	}
	if !agentProofTimestampFresh(parsedTx.AgentTimestamp, consensusTime) {
		return fmt.Errorf("delegated agent proof timestamp is older than the 5-minute consensus window")
	}

	req, err := parseSignedAgentRequest(parsedTx.AgentRequest)
	if err != nil {
		return err
	}
	if bindErr := app.verifySignedAgentAction(parsedTx, agentID, req, bindMemoryTags); bindErr != nil {
		return fmt.Errorf("delegated agent action mismatch: %w", bindErr)
	}

	fingerprint := delegatedAgentProofFingerprint(parsedTx)
	if parsedTx.AgentTimestamp > math.MaxInt64-int64(delegatedAgentProofSkew/time.Second) {
		return fmt.Errorf("delegated agent proof timestamp overflows its expiry")
	}
	expiresAt := parsedTx.AgentTimestamp + int64(delegatedAgentProofSkew/time.Second)
	if claim {
		return app.badgerStore.ClaimAgentProof(
			fingerprint[:],
			consensusTime,
			expiresAt,
		)
	}

	used, err := app.badgerStore.HasAgentProof(fingerprint[:], consensusTime, expiresAt)
	if err != nil {
		return fmt.Errorf("delegated agent proof replay lookup: %w", err)
	}
	if used {
		return store.ErrAgentProofReplayed
	}
	return nil
}

func txUsesAgentIdentity(txType tx.TxType) bool {
	switch txType {
	case tx.TxTypeMemorySubmit,
		tx.TxTypeMemoryChallenge,
		tx.TxTypeMemoryCorroborate,
		tx.TxTypeAccessRequest,
		tx.TxTypeAccessGrant,
		tx.TxTypeAccessRevoke,
		tx.TxTypeAccessQuery,
		tx.TxTypeDomainRegister,
		tx.TxTypeOrgRegister,
		tx.TxTypeOrgAddMember,
		tx.TxTypeOrgRemoveMember,
		tx.TxTypeOrgSetClearance,
		tx.TxTypeFederationPropose,
		tx.TxTypeFederationApprove,
		tx.TxTypeFederationRevoke,
		tx.TxTypeDeptRegister,
		tx.TxTypeDeptAddMember,
		tx.TxTypeDeptRemoveMember,
		tx.TxTypeAgentRegister,
		tx.TxTypeAgentUpdate,
		tx.TxTypeAgentSetPermission,
		tx.TxTypeMemoryReassign,
		tx.TxTypeUpgradePropose,
		tx.TxTypeUpgradeCancel,
		tx.TxTypeUpgradeRevert,
		tx.TxTypeDomainReassign,
		tx.TxTypeCoCommitSubmit,
		tx.TxTypeCrossFedSet,
		tx.TxTypeCrossFedRevoke,
		tx.TxTypeMemoryReinstate:
		return true
	default:
		return false
	}
}

// agentProofTimestampFresh keeps only a LOWER bound (reject captured-old proofs);
// the upper bound was dropped in v11.7.6 so a future-dated proof from a node whose
// deterministic block time lags its wall clock (idle single-validator chains) is
// still accepted. Like the embedding relaxation above, this is an UNGATED v11.7.6
// consensus rule baked into committed history and MUST NOT be fork-gated below its
// real activation — see the REPLAY-CRITICAL note on verifySignedAgentAction.
func agentProofTimestampFresh(timestamp int64, consensusTime time.Time) bool {
	now := consensusTime.Unix()
	skew := int64(delegatedAgentProofSkew / time.Second)
	min := int64(math.MinInt64)
	if now >= math.MinInt64+skew {
		min = now - skew
	}
	// Reject captured old authorizations, but do not reject a proof merely
	// because it is ahead of consensus time. SAGE intentionally mints no idle
	// heartbeat blocks; after a long idle period a single-validator chain's next
	// deterministic block time can lag the REST/MCP wall clock by more than five
	// minutes. The HTTP boundary already checks the signed timestamp against its
	// wall clock before constructing the transaction, and the agent signature
	// means a future-dated proof still cannot be forged by the outer node.
	return timestamp >= min
}

func delegatedAgentProofFingerprint(parsedTx *tx.ParsedTx) [sha256.Size]byte {
	h := sha256.New()
	_, _ = h.Write([]byte("sage/delegated-agent-proof/v17\x00"))
	writeProofPart := func(part []byte) {
		var size [4]byte
		binary.BigEndian.PutUint32(size[:], uint32(len(part))) // #nosec G115 -- every proof field is protocol-bounded
		_, _ = h.Write(size[:])
		_, _ = h.Write(part)
	}
	writeProofPart(parsedTx.AgentPubKey)
	writeProofPart(parsedTx.AgentSig)
	var timestamp [8]byte
	binary.BigEndian.PutUint64(timestamp[:], uint64(parsedTx.AgentTimestamp)) // #nosec G115 -- preserve signed bits
	_, _ = h.Write(timestamp[:])
	writeProofPart(parsedTx.AgentBodyHash)
	writeProofPart(parsedTx.AgentNonce)
	writeProofPart(parsedTx.AgentRequest)
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

func parseSignedAgentRequest(raw []byte) (signedAgentRequest, error) {
	line, body, ok := bytes.Cut(raw, []byte{'\n'})
	if !ok || len(line) == 0 {
		return signedAgentRequest{}, fmt.Errorf("signed request has no canonical request line")
	}
	method, path, ok := strings.Cut(string(line), " ")
	if !ok || method == "" || path == "" || !strings.HasPrefix(path, "/") {
		return signedAgentRequest{}, fmt.Errorf("signed request line is malformed")
	}
	if strings.ContainsAny(method, " \t\r") || strings.ContainsAny(path, "\r\n") {
		return signedAgentRequest{}, fmt.Errorf("signed request line contains invalid characters")
	}
	return signedAgentRequest{method: method, path: path, body: body}, nil
}

func (r signedAgentRequest) pathParts() ([]string, error) {
	path := r.path
	if q := strings.IndexByte(path, '?'); q >= 0 {
		path = path[:q]
	}
	if path == "" || path[0] != '/' {
		return nil, fmt.Errorf("request path is malformed")
	}
	parts := strings.Split(strings.TrimPrefix(path, "/"), "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return nil, fmt.Errorf("request path contains an empty or relative segment")
		}
	}
	return parts, nil
}

func decodeSignedJSON(body []byte, target any, optional bool) error {
	if len(bytes.TrimSpace(body)) == 0 {
		if optional {
			return nil
		}
		return fmt.Errorf("signed request body is empty")
	}
	if err := json.Unmarshal(body, target); err != nil {
		return fmt.Errorf("signed request body is invalid JSON: %w", err)
	}
	return nil
}

func requireAgentRoute(req signedAgentRequest, method string, want ...string) ([]string, error) {
	if req.method != method {
		return nil, fmt.Errorf("method %q does not authorize %s", req.method, method)
	}
	parts, err := req.pathParts()
	if err != nil {
		return nil, err
	}
	if len(parts) != len(want) {
		return nil, fmt.Errorf("path %q does not authorize this transaction type", req.path)
	}
	params := make([]string, 0, len(want))
	for i := range want {
		if strings.HasPrefix(want[i], ":") {
			params = append(params, parts[i])
			continue
		}
		if parts[i] != want[i] {
			return nil, fmt.Errorf("path %q does not authorize this transaction type", req.path)
		}
	}
	return params, nil
}

func memoryTypeFromRequest(value string) (tx.MemoryType, error) {
	switch value {
	case "fact":
		return tx.MemoryTypeFact, nil
	case "observation":
		return tx.MemoryTypeObservation, nil
	case "inference":
		return tx.MemoryTypeInference, nil
	case "task":
		return tx.MemoryTypeTask, nil
	default:
		return 0, fmt.Errorf("invalid memory_type %q", value)
	}
}

func validRESTMemoryID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	raw, err := hex.DecodeString(strings.ReplaceAll(value, "-", ""))
	return err == nil && len(raw) == 16 && raw[6]>>4 == 4 && raw[8]&0xc0 == 0x80
}

func compareAgentPayload(actual, expected *tx.ParsedTx) error {
	actualPayload, err := tx.PayloadBytes(actual)
	if err != nil {
		return fmt.Errorf("encode delivered payload: %w", err)
	}
	expectedPayload, err := tx.PayloadBytes(expected)
	if err != nil {
		return fmt.Errorf("encode signed-request payload: %w", err)
	}
	if !bytes.Equal(actualPayload, expectedPayload) {
		return fmt.Errorf("transaction payload differs from the signed request")
	}
	return nil
}

// REPLAY-CRITICAL (H-2) — DO NOT FORK-GATE THE RELAXATION BELOW.
// The delegated MemorySubmit binding here uses the NODE-derived EmbeddingHash
// (expected.EmbeddingHash = actual.MemorySubmit.EmbeddingHash), and the sibling
// agentProofTimestampFresh keeps only a lower time bound. Both relaxations shipped
// UNGATED in the v11.7.6 release ("restore reliable MCP turns", commit 534b6fd) and
// are therefore the consensus rule from that height onward. They form a strict
// SUPERSET of the prior v11.7.5 rule (everything the old rule accepted, the new rule
// still accepts), so all already-committed history replays byte-identically under the
// current binary. Do NOT wrap this — or the enforceDelegatedAgentProof call at
// app.go's FinalizeBlock path — in an old-below/new-above fork such as
// postAppV19Rules(height): app-v19 activates strictly ABOVE all v11.7.6/7 history
// (personal nodes auto-advance it only after the v11.8 binary boots; multi-validator
// needs a v11.8 quorum), so such a gate would replay committed delegated sage_turn
// writes under the stricter OLD rule, Code-109-reject them, and diverge on
// AppHash/LastResultsHash — crashing every chain that upgraded through v11.7.6. If the
// embedding/timestamp posture must be tightened, it has to be a NEW forward-only fork
// that changes behaviour only ABOVE its own activation height, never a retroactive
// re-strictification. Guarded by TestReplayGuardDelegatedMemorySubmitAcceptedBelowAppV19.
func (app *SageApp) verifySignedAgentAction(actual *tx.ParsedTx, agentID string, req signedAgentRequest, bindMemoryTags bool) error { //nolint:gocyclo,maintidx // exhaustive protocol routing is intentionally centralized
	expected := &tx.ParsedTx{Type: actual.Type}

	switch actual.Type {
	case tx.TxTypeMemorySubmit:
		if _, err := requireAgentRoute(req, "POST", "v1", "memory", "submit"); err != nil {
			return err
		}
		var body struct {
			Content         string    `json:"content"`
			MemoryType      string    `json:"memory_type"`
			DomainTag       string    `json:"domain_tag"`
			ConfidenceScore float64   `json:"confidence_score"`
			Classification  int       `json:"classification,omitempty"`
			Embedding       []float32 `json:"embedding,omitempty"`
			ParentHash      string    `json:"parent_hash,omitempty"`
			TaskStatus      string    `json:"task_status,omitempty"`
			Tags            []string  `json:"tags,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		memoryType, err := memoryTypeFromRequest(body.MemoryType)
		if err != nil {
			return err
		}
		if body.Content == "" || body.DomainTag == "" || body.ConfidenceScore < 0 || body.ConfidenceScore > 1 || body.Classification < 0 || body.Classification > 4 {
			return fmt.Errorf("signed memory submission fails the REST contract")
		}
		if actual.MemorySubmit == nil || !validRESTMemoryID(actual.MemorySubmit.MemoryID) {
			return fmt.Errorf("memory submit has no valid server-generated UUID")
		}
		if len(actual.MemorySubmit.EmbeddingHash) != 0 && len(actual.MemorySubmit.EmbeddingHash) != sha256.Size {
			return fmt.Errorf("memory submit has an invalid node-generated embedding hash")
		}
		contentHash := sha256.Sum256([]byte(body.Content))
		var canonicalTags []string
		if bindMemoryTags {
			canonicalTags, err = memorytags.Normalize(body.Tags)
			if err != nil {
				return fmt.Errorf("signed memory tags fail the REST contract: %w", err)
			}
		}
		expected.MemorySubmit = &tx.MemorySubmit{
			MemoryID:    actual.MemorySubmit.MemoryID,
			ContentHash: contentHash[:],
			// v11.7.4 made the receiving node authoritative for the active
			// embedding space: REST regenerates the vector after authenticating
			// the request. The outer node signature binds that derived hash; it
			// cannot equal a stale client vector after a provider cutover and is
			// therefore deliberately not reconstructed from the signed JSON.
			// Agent authority remains bound to content, type, domain, confidence,
			// classification, parent, and task status below.
			EmbeddingHash:   actual.MemorySubmit.EmbeddingHash,
			MemoryType:      memoryType,
			DomainTag:       body.DomainTag,
			ConfidenceScore: body.ConfidenceScore,
			Content:         body.Content,
			ParentHash:      body.ParentHash,
			Classification:  tx.ClearanceLevel(body.Classification), // #nosec G115 -- range checked above
			TaskStatus:      body.TaskStatus,
			Tags:            canonicalTags,
		}

	case tx.TxTypeMemoryChallenge:
		parts, err := req.pathParts()
		if err != nil {
			return err
		}
		if req.method != "POST" || len(parts) != 4 || parts[0] != "v1" || parts[1] != "memory" {
			return fmt.Errorf("path %q does not authorize a memory challenge", req.path)
		}
		memoryID := parts[2]
		switch parts[3] {
		case "challenge":
			var body struct {
				Reason   string `json:"reason"`
				Evidence string `json:"evidence,omitempty"`
			}
			if err := decodeSignedJSON(req.body, &body, false); err != nil {
				return err
			}
			if body.Reason == "" {
				return fmt.Errorf("challenge reason is required")
			}
			expected.MemoryChallenge = &tx.MemoryChallenge{MemoryID: memoryID, Reason: body.Reason, Evidence: body.Evidence}
		case "forget":
			var body struct {
				Reason string `json:"reason,omitempty"`
			}
			if err := decodeSignedJSON(req.body, &body, false); err != nil {
				return err
			}
			if body.Reason == "" {
				body.Reason = "deprecated by user"
			}
			expected.MemoryChallenge = &tx.MemoryChallenge{MemoryID: memoryID, Reason: body.Reason}
		default:
			return fmt.Errorf("path %q does not authorize a memory challenge", req.path)
		}

	case tx.TxTypeMemoryReinstate:
		params, err := requireAgentRoute(req, "POST", "v1", "memory", ":memory_id", "reinstate")
		if err != nil {
			return err
		}
		var body struct {
			Reason string `json:"reason,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		expected.MemoryReinstate = &tx.MemoryReinstate{MemoryID: params[0], Reason: body.Reason}

	case tx.TxTypeMemoryCorroborate:
		params, err := requireAgentRoute(req, "POST", "v1", "memory", ":memory_id", "corroborate")
		if err != nil {
			return err
		}
		var body struct {
			Evidence string `json:"evidence,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		expected.MemoryCorroborate = &tx.MemoryCorroborate{MemoryID: params[0], Evidence: body.Evidence}

	case tx.TxTypeAccessRequest:
		if _, err := requireAgentRoute(req, "POST", "v1", "access", "request"); err != nil {
			return err
		}
		var body struct {
			TargetDomain   string `json:"target_domain"`
			Justification  string `json:"justification,omitempty"`
			RequestedLevel int    `json:"requested_level,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.TargetDomain == "" || body.RequestedLevel < 0 || body.RequestedLevel > math.MaxUint8 {
			return fmt.Errorf("signed access request fails the REST contract")
		}
		if body.RequestedLevel == 0 {
			body.RequestedLevel = 1
		}
		expected.AccessRequest = &tx.AccessRequest{RequesterID: agentID, TargetDomain: body.TargetDomain, Justification: body.Justification, RequestedLevel: uint8(body.RequestedLevel)}

	case tx.TxTypeAccessGrant:
		if _, err := requireAgentRoute(req, "POST", "v1", "access", "grant"); err != nil {
			return err
		}
		var body struct {
			GranteeID string `json:"grantee_id"`
			Domain    string `json:"domain"`
			Level     int    `json:"level,omitempty"`
			ExpiresAt int64  `json:"expires_at,omitempty"`
			RequestID string `json:"request_id,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.GranteeID == "" || body.Domain == "" || body.Level < 0 || body.Level > math.MaxUint8 {
			return fmt.Errorf("signed access grant fails the REST contract")
		}
		if body.Level == 0 {
			body.Level = 1
		}
		expected.AccessGrant = &tx.AccessGrant{GranterID: agentID, GranteeID: body.GranteeID, Domain: body.Domain, Level: uint8(body.Level), ExpiresAt: body.ExpiresAt, RequestID: body.RequestID}

	case tx.TxTypeAccessRevoke:
		if _, err := requireAgentRoute(req, "POST", "v1", "access", "revoke"); err != nil {
			return err
		}
		var body struct {
			GranteeID string `json:"grantee_id"`
			Domain    string `json:"domain"`
			Reason    string `json:"reason,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.GranteeID == "" || body.Domain == "" {
			return fmt.Errorf("signed access revoke fails the REST contract")
		}
		expected.AccessRevoke = &tx.AccessRevoke{RevokerID: agentID, GranteeID: body.GranteeID, Domain: body.Domain, Reason: body.Reason}

	case tx.TxTypeDomainRegister:
		if _, err := requireAgentRoute(req, "POST", "v1", "domain", "register"); err != nil {
			return err
		}
		var body struct {
			Name        string `json:"name"`
			Description string `json:"description,omitempty"`
			Parent      string `json:"parent,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.Name == "" {
			return fmt.Errorf("domain name is required")
		}
		expected.DomainRegister = &tx.DomainRegister{DomainName: body.Name, OwnerAgentID: agentID, Description: body.Description, ParentDomain: body.Parent}

	case tx.TxTypeOrgRegister:
		if _, err := requireAgentRoute(req, "POST", "v1", "org", "register"); err != nil {
			return err
		}
		var body struct {
			Name        string `json:"name"`
			Description string `json:"description,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.Name == "" {
			return fmt.Errorf("organization name is required")
		}
		idHash := sha256.Sum256([]byte(agentID + body.Name))
		expected.OrgRegister = &tx.OrgRegister{OrgID: hex.EncodeToString(idHash[:16]), Name: body.Name, Description: body.Description, AdminAgent: agentID}

	case tx.TxTypeOrgAddMember:
		params, err := requireAgentRoute(req, "POST", "v1", "org", ":org_id", "member")
		if err != nil {
			return err
		}
		var body struct {
			AgentID   string `json:"agent_id"`
			Clearance *int   `json:"clearance,omitempty"`
			Role      string `json:"role,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		clearance := 1
		if body.Clearance != nil {
			clearance = *body.Clearance
		}
		if body.AgentID == "" || clearance < 0 || clearance > 4 {
			return fmt.Errorf("signed org-member request fails the REST contract")
		}
		if body.Role == "" {
			body.Role = "member"
		}
		expected.OrgAddMember = &tx.OrgAddMember{OrgID: params[0], AgentID: body.AgentID, Clearance: tx.ClearanceLevel(clearance), Role: body.Role}

	case tx.TxTypeOrgRemoveMember:
		params, err := requireAgentRoute(req, "DELETE", "v1", "org", ":org_id", "member", ":agent_id")
		if err != nil {
			return err
		}
		expected.OrgRemoveMember = &tx.OrgRemoveMember{OrgID: params[0], AgentID: params[1]}

	case tx.TxTypeOrgSetClearance:
		params, err := requireAgentRoute(req, "POST", "v1", "org", ":org_id", "clearance")
		if err != nil {
			return err
		}
		var body struct {
			AgentID   string `json:"agent_id"`
			Clearance int    `json:"clearance"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.AgentID == "" || body.Clearance < 0 || body.Clearance > 4 {
			return fmt.Errorf("signed clearance request fails the REST contract")
		}
		expected.OrgSetClearance = &tx.OrgSetClearance{OrgID: params[0], AgentID: body.AgentID, Clearance: tx.ClearanceLevel(body.Clearance)}

	case tx.TxTypeFederationPropose:
		if _, err := requireAgentRoute(req, "POST", "v1", "federation", "propose"); err != nil {
			return err
		}
		var body struct {
			TargetOrgID      string   `json:"target_org_id"`
			AllowedDomains   []string `json:"allowed_domains,omitempty"`
			AllowedDepts     []string `json:"allowed_depts,omitempty"`
			MaxClearance     int      `json:"max_clearance,omitempty"`
			ExpiresAt        int64    `json:"expires_at,omitempty"`
			RequiresApproval bool     `json:"requires_approval,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.MaxClearance == 0 {
			body.MaxClearance = 2
		}
		if body.TargetOrgID == "" || body.MaxClearance < 0 || body.MaxClearance > 4 {
			return fmt.Errorf("signed federation proposal fails the REST contract")
		}
		orgID, err := app.badgerStore.GetAgentOrg(agentID)
		if err != nil {
			return fmt.Errorf("derive proposing organization: %w", err)
		}
		expected.FederationPropose = &tx.FederationPropose{ProposerOrgID: orgID, TargetOrgID: body.TargetOrgID, AllowedDomains: body.AllowedDomains, AllowedDepts: body.AllowedDepts, MaxClearance: tx.ClearanceLevel(body.MaxClearance), ExpiresAt: body.ExpiresAt, RequiresApproval: body.RequiresApproval}

	case tx.TxTypeFederationApprove:
		params, err := requireAgentRoute(req, "POST", "v1", "federation", ":fed_id", "approve")
		if err != nil {
			return err
		}
		orgID, err := app.badgerStore.GetAgentOrg(agentID)
		if err != nil {
			return fmt.Errorf("derive approving organization: %w", err)
		}
		expected.FederationApprove = &tx.FederationApprove{FederationID: params[0], ApproverOrgID: orgID}

	case tx.TxTypeFederationRevoke:
		params, err := requireAgentRoute(req, "POST", "v1", "federation", ":fed_id", "revoke")
		if err != nil {
			return err
		}
		var body struct {
			Reason string `json:"reason,omitempty"`
		}
		if decodeErr := decodeSignedJSON(req.body, &body, true); decodeErr != nil {
			return decodeErr
		}
		orgID, err := app.badgerStore.GetAgentOrg(agentID)
		if err != nil {
			return fmt.Errorf("derive revoking organization: %w", err)
		}
		expected.FederationRevoke = &tx.FederationRevoke{FederationID: params[0], RevokerOrgID: orgID, Reason: body.Reason}

	case tx.TxTypeDeptRegister:
		params, err := requireAgentRoute(req, "POST", "v1", "org", ":org_id", "dept")
		if err != nil {
			return err
		}
		var body struct {
			Name        string `json:"name"`
			Description string `json:"description,omitempty"`
			ParentDept  string `json:"parent_dept,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.Name == "" {
			return fmt.Errorf("department name is required")
		}
		idHash := sha256.Sum256([]byte(params[0] + body.Name))
		expected.DeptRegister = &tx.DeptRegister{OrgID: params[0], DeptID: hex.EncodeToString(idHash[:8]), DeptName: body.Name, Description: body.Description, ParentDept: body.ParentDept}

	case tx.TxTypeDeptAddMember:
		params, err := requireAgentRoute(req, "POST", "v1", "org", ":org_id", "dept", ":dept_id", "member")
		if err != nil {
			return err
		}
		var body struct {
			AgentID   string `json:"agent_id"`
			Clearance *int   `json:"clearance,omitempty"`
			Role      string `json:"role,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		clearance := 1
		if body.Clearance != nil {
			clearance = *body.Clearance
		}
		if body.AgentID == "" || clearance < 0 || clearance > 4 {
			return fmt.Errorf("signed department-member request fails the REST contract")
		}
		if body.Role == "" {
			body.Role = "member"
		}
		expected.DeptAddMember = &tx.DeptAddMember{OrgID: params[0], DeptID: params[1], AgentID: body.AgentID, Clearance: tx.ClearanceLevel(clearance), Role: body.Role}

	case tx.TxTypeDeptRemoveMember:
		params, err := requireAgentRoute(req, "DELETE", "v1", "org", ":org_id", "dept", ":dept_id", "member", ":agent_id")
		if err != nil {
			return err
		}
		expected.DeptRemoveMember = &tx.DeptRemoveMember{OrgID: params[0], DeptID: params[1], AgentID: params[2]}

	case tx.TxTypeAgentRegister:
		if _, err := requireAgentRoute(req, "POST", "v1", "agent", "register"); err != nil {
			return err
		}
		var body struct {
			Name       string `json:"name"`
			Role       string `json:"role"`
			BootBio    string `json:"boot_bio"`
			Provider   string `json:"provider"`
			P2PAddress string `json:"p2p_address"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if body.Name == "" {
			return fmt.Errorf("agent name is required")
		}
		if body.Role == "" {
			body.Role = "member"
		}
		expected.AgentRegister = &tx.AgentRegister{AgentID: agentID, Name: body.Name, Role: body.Role, BootBio: body.BootBio, Provider: body.Provider, P2PAddress: body.P2PAddress}

	case tx.TxTypeAgentUpdate:
		if _, err := requireAgentRoute(req, "PUT", "v1", "agent", "update"); err != nil {
			return err
		}
		var body struct {
			Name    string `json:"name"`
			BootBio string `json:"boot_bio"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		expected.AgentUpdateTx = &tx.AgentUpdate{AgentID: agentID, Name: body.Name, BootBio: body.BootBio}

	case tx.TxTypeAgentSetPermission:
		params, err := requireAgentRoute(req, "PUT", "v1", "agent", ":agent_id", "permission")
		if err != nil {
			return err
		}
		var body struct {
			Clearance     *int    `json:"clearance"`
			DomainAccess  *string `json:"domain_access"`
			VisibleAgents *string `json:"visible_agents"`
			OrgID         *string `json:"org_id"`
			DeptID        *string `json:"dept_id"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		var existing *store.OnChainAgent
		if body.Clearance == nil || body.DomainAccess == nil || body.VisibleAgents == nil || body.OrgID == nil || body.DeptID == nil {
			if current, getErr := app.badgerStore.GetRegisteredAgent(params[0]); getErr == nil {
				existing = current
			}
		}
		clearance := 1
		if body.Clearance != nil {
			clearance = *body.Clearance
		} else if existing != nil {
			clearance = int(existing.Clearance)
		}
		if clearance < 0 || clearance > 4 {
			return fmt.Errorf("permission clearance is outside 0..4")
		}
		backfillString := func(value *string, current string) string {
			if value != nil {
				return *value
			}
			return current
		}
		var domainAccess, visibleAgents, orgID, deptID string
		if existing != nil {
			domainAccess = existing.DomainAccess
			visibleAgents = existing.VisibleAgents
			orgID = existing.OrgID
			deptID = existing.DeptID
		}
		expected.AgentSetPermission = &tx.AgentSetPermission{
			AgentID:       params[0],
			Clearance:     uint8(clearance),
			DomainAccess:  backfillString(body.DomainAccess, domainAccess),
			VisibleAgents: backfillString(body.VisibleAgents, visibleAgents),
			OrgID:         backfillString(body.OrgID, orgID),
			DeptID:        backfillString(body.DeptID, deptID),
		}

	case tx.TxTypeDomainReassign:
		if _, err := requireAgentRoute(req, "POST", "v1", "domain", "reassign"); err != nil {
			return err
		}
		var body struct {
			Domain       string `json:"domain"`
			NewOwnerID   string `json:"new_owner_id"`
			ParentDomain string `json:"parent_domain,omitempty"`
			ProposalID   string `json:"proposal_id"`
			OpenToShared bool   `json:"open_to_shared,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		newOwner, ownerErr := hex.DecodeString(body.NewOwnerID)
		proposalID, proposalErr := hex.DecodeString(body.ProposalID)
		if body.Domain == "" || ownerErr != nil || len(newOwner) != ed25519.PublicKeySize || proposalErr != nil || len(proposalID) == 0 {
			return fmt.Errorf("signed domain-reassign request fails the REST contract")
		}
		expected.DomainReassign = &tx.DomainReassign{Domain: body.Domain, NewOwnerID: body.NewOwnerID, ParentDomain: body.ParentDomain, ProposalID: body.ProposalID, OpenToShared: body.OpenToShared}

	case tx.TxTypeCoCommitSubmit:
		if _, err := requireAgentRoute(req, "POST", "v1", "cocommit", "submit"); err != nil {
			return err
		}
		var body struct {
			SchemaVersion   uint32  `json:"schema_version,omitempty"`
			Content         string  `json:"content,omitempty"`
			ContentHash     string  `json:"content_hash,omitempty"`
			MemoryType      string  `json:"memory_type"`
			DomainTag       string  `json:"domain_tag"`
			Classification  int     `json:"classification,omitempty"`
			ConfidenceScore float64 `json:"confidence_score"`
			CreatedAtUnix   int64   `json:"created_at_unix"`
			AgreementNonce  string  `json:"agreement_nonce"`
			Coauthors       []struct {
				PubKey  string `json:"pub_key"`
				ChainID string `json:"chain_id"`
				Sig     string `json:"sig"`
			} `json:"coauthors"`
			NotBefore int64 `json:"not_before,omitempty"`
			NotAfter  int64 `json:"not_after,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		memoryType, err := memoryTypeFromRequest(body.MemoryType)
		if err != nil {
			return err
		}
		if body.DomainTag == "" || body.Classification < 0 || body.Classification > 4 || len(body.Coauthors) > 64 {
			return fmt.Errorf("signed co-commit request fails the REST contract")
		}
		if body.NotBefore != 0 && body.NotAfter != 0 && body.NotAfter <= body.NotBefore {
			return fmt.Errorf("co-commit not_after must be greater than not_before")
		}
		if body.SchemaVersion == 0 {
			body.SchemaVersion = 1
		}
		var contentHash []byte
		if body.Content != "" {
			hash := sha256.Sum256([]byte(body.Content))
			contentHash = hash[:]
			if body.ContentHash != "" {
				claimed, decodeErr := hex.DecodeString(body.ContentHash)
				if decodeErr != nil || !bytes.Equal(claimed, contentHash) {
					return fmt.Errorf("co-commit content_hash does not match content")
				}
			}
		} else {
			claimed, decodeErr := hex.DecodeString(body.ContentHash)
			if decodeErr != nil || len(claimed) != sha256.Size {
				return fmt.Errorf("co-commit requires content or a sha256 content_hash")
			}
			contentHash = claimed
		}
		nonce, err := hex.DecodeString(body.AgreementNonce)
		if err != nil || len(nonce) == 0 {
			return fmt.Errorf("co-commit agreement_nonce must be non-empty hex")
		}
		coauthors := make([]tx.CoCommitCoauthor, 0, len(body.Coauthors))
		for _, coauthor := range body.Coauthors {
			pub, pubErr := hex.DecodeString(coauthor.PubKey)
			sig, sigErr := hex.DecodeString(coauthor.Sig)
			if pubErr != nil || sigErr != nil || len(pub) != ed25519.PublicKeySize || len(sig) != ed25519.SignatureSize || coauthor.ChainID == "" {
				return fmt.Errorf("co-commit contains a malformed coauthor")
			}
			coauthors = append(coauthors, tx.CoCommitCoauthor{PubKey: pub, ChainID: coauthor.ChainID, Sig: sig})
		}
		envelope := &tx.CoCommitSubmit{
			SchemaVersion:   body.SchemaVersion,
			ContentHash:     contentHash,
			MemoryType:      memoryType,
			Domain:          body.DomainTag,
			Classification:  tx.ClearanceLevel(body.Classification),
			ConfidenceScore: body.ConfidenceScore,
			CreatedAtUnix:   body.CreatedAtUnix,
			AgreementNonce:  nonce,
			Coauthors:       coauthors,
			NotBefore:       body.NotBefore,
			NotAfter:        body.NotAfter,
		}
		coreHash := tx.CoreHashOf(envelope)
		envelope.SharedID = tx.ComputeSharedID(coreHash, coauthors, nonce)
		expected.CoCommitSubmit = envelope

	case tx.TxTypeCrossFedSet:
		if _, err := requireAgentRoute(req, "POST", "v1", "federation", "cross"); err != nil {
			return err
		}
		var body struct {
			RemoteChainID  string   `json:"remote_chain_id"`
			Endpoint       string   `json:"endpoint"`
			RemoteCAPEM    string   `json:"remote_ca_pem"`
			MaxClearance   int      `json:"max_clearance"`
			AllowedDomains []string `json:"allowed_domains"`
			AllowedDepts   []string `json:"allowed_depts,omitempty"`
			ExpiresAt      int64    `json:"expires_at,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, false); err != nil {
			return err
		}
		if !delegatedChainIDPattern.MatchString(body.RemoteChainID) || body.RemoteChainID == "." || body.RemoteChainID == ".." || len(body.RemoteChainID) > 50 || body.MaxClearance < 0 || body.MaxClearance > 4 || len(body.AllowedDomains) == 0 {
			return fmt.Errorf("signed cross-federation request fails the REST contract")
		}
		endpoint, endpointErr := url.Parse(body.Endpoint)
		if endpointErr != nil || endpoint.Scheme != "https" || endpoint.Host == "" ||
			(endpoint.Path != "" && endpoint.Path != "/") || endpoint.RawQuery != "" || endpoint.Fragment != "" {
			return fmt.Errorf("cross-federation endpoint must be an https origin")
		}
		for _, dept := range body.AllowedDepts {
			if dept != "*" {
				return fmt.Errorf("cross-federation department scope is unsupported")
			}
		}
		if body.ExpiresAt != 0 && body.ExpiresAt <= actual.AgentTimestamp {
			return fmt.Errorf("cross-federation expiry predates the signed request")
		}
		block, _ := pem.Decode([]byte(body.RemoteCAPEM))
		if block == nil || block.Type != "CERTIFICATE" {
			return fmt.Errorf("remote CA has no CERTIFICATE PEM block")
		}
		cert, err := x509.ParseCertificate(block.Bytes)
		if err != nil || !cert.IsCA || cert.Subject.CommonName != "sage-ca-"+body.RemoteChainID {
			return fmt.Errorf("remote CA does not identify the requested chain")
		}
		pin := sha256.Sum256(cert.RawSubjectPublicKeyInfo)
		expected.CrossFedTerms = &tx.CrossFedTerms{RemoteChainID: body.RemoteChainID, Endpoint: body.Endpoint, PeerPubKey: pin[:], MaxClearance: tx.ClearanceLevel(body.MaxClearance), AllowedDomains: body.AllowedDomains, AllowedDepts: body.AllowedDepts, ExpiresAt: body.ExpiresAt, Status: "active"}

	case tx.TxTypeCrossFedRevoke:
		params, err := requireAgentRoute(req, "POST", "v1", "federation", "cross", ":chain_id", "revoke")
		if err != nil {
			return err
		}
		if !delegatedChainIDPattern.MatchString(params[0]) || params[0] == "." || params[0] == ".." || len(params[0]) > 50 {
			return fmt.Errorf("cross-federation revoke has an invalid chain id")
		}
		var body struct {
			Reason string `json:"reason,omitempty"`
		}
		if err := decodeSignedJSON(req.body, &body, true); err != nil {
			return err
		}
		expected.CrossFedRevoke = &tx.CrossFedRevoke{RemoteChainID: params[0], Reason: body.Reason}

	default:
		return fmt.Errorf("transaction type %d has no delegated REST action", actual.Type)
	}

	return compareAgentPayload(actual, expected)
}
