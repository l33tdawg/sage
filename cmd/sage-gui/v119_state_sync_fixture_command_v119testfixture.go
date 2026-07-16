//go:build v119testfixture

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/scope"
	"github.com/l33tdawg/sage/internal/store"
)

const (
	v119StateSyncFixtureCommand        = "v119-state-sync-fixture"
	v119StateSyncFixtureScopeID        = "v119-state-sync-wire-scope"
	v119StateSyncFixtureDomain         = "v119-state-sync"
	v119StateSyncFixtureContent        = "Canonical app-v20 scoped content must survive authorized state sync and rebuild the exact serving projection."
	v119StateSyncFixtureClassification = 3
)

var v119StateSyncFixtureTags = []string{"state-sync", "v11.9"}

func init() {
	optionalCommandHandler = runV119StateSyncFixtureCommand
}

func runV119StateSyncFixtureCommand(args []string) (bool, error) {
	if len(args) == 0 || args[0] != v119StateSyncFixtureCommand {
		return false, nil
	}
	if len(args) < 2 {
		return true, errors.New("fixture subcommand is required")
	}
	switch args[1] {
	case "install-scoped-proof":
		memoryID, err := installV119StateSyncScopedProof(context.Background())
		if err != nil {
			return true, err
		}
		fmt.Println(memoryID)
		return true, nil
	case "verify-scoped-projection":
		if len(args) != 3 || args[2] == "" {
			return true, errors.New("verify-scoped-projection requires one memory ID")
		}
		return true, verifyV119StateSyncScopedProjection(context.Background(), args[2])
	case "receiving":
		cfg, err := LoadConfig()
		if err != nil {
			return true, fmt.Errorf("load fixture config: %w", err)
		}
		fmt.Println(cfg.Quorum.StateSync.Receiving)
		return true, nil
	default:
		return true, fmt.Errorf("unknown fixture subcommand %q", args[1])
	}
}

func installV119StateSyncScopedProof(parent context.Context) (string, error) {
	cfg, err := LoadConfig()
	if err != nil {
		return "", fmt.Errorf("load fixture config: %w", err)
	}
	key, err := loadProposeSigningKey(cfg.AgentKey)
	if err != nil {
		return "", fmt.Errorf("load fixture operator key: %w", err)
	}
	baseURL := os.Getenv("SAGE_API_URL")
	if baseURL == "" {
		baseURL = restBaseURL(cfg.RESTAddr)
	}
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()

	contextBody, err := v119FixtureSignedRequest(ctx, baseURL, key, http.MethodGet, "/v1/governance/context", nil)
	if err != nil {
		return "", fmt.Errorf("read governance context: %w", err)
	}
	var governanceContext struct {
		ValidatorID      string `json:"validator_id"`
		GovernanceDomain string `json:"governance_domain"`
		AppV20Active     bool   `json:"app_v20_active"`
	}
	if err = json.Unmarshal(contextBody, &governanceContext); err != nil {
		return "", fmt.Errorf("decode governance context: %w", err)
	}
	if !governanceContext.AppV20Active || governanceContext.ValidatorID == "" || governanceContext.GovernanceDomain == "" {
		return "", errors.New("app-v20 validator-bound governance context is not active")
	}

	template := scope.ProposalTemplate{
		ScopeID:               v119StateSyncFixtureScopeID,
		Revision:              1,
		State:                 "active",
		ControllerValidatorID: governanceContext.ValidatorID,
		Domains:               []string{v119StateSyncFixtureDomain},
		Members: []scope.ProposalMember{{
			ValidatorID: governanceContext.ValidatorID, AssignedWeight: 1,
		}},
	}
	proposeBody, err := json.Marshal(struct {
		ValidatorID      string                  `json:"validator_id"`
		GovernanceDomain string                  `json:"governance_domain"`
		Operation        string                  `json:"operation"`
		TargetID         string                  `json:"target_id"`
		Reason           string                  `json:"reason"`
		Scope            *scope.ProposalTemplate `json:"scope"`
	}{
		ValidatorID: governanceContext.ValidatorID, GovernanceDomain: governanceContext.GovernanceDomain,
		Operation: "scope_action", TargetID: template.ScopeID,
		Reason: "v11.9 authorized state-sync canonical projection proof", Scope: &template,
	})
	if err != nil {
		return "", fmt.Errorf("encode scope proposal: %w", err)
	}
	proposeResponse, err := v119FixtureSignedRequest(ctx, baseURL, key, http.MethodPost, "/v1/governance/propose", proposeBody)
	if err != nil {
		return "", fmt.Errorf("propose governed scope: %w", err)
	}
	var proposed struct {
		ProposalID string `json:"proposal_id"`
	}
	if err = json.Unmarshal(proposeResponse, &proposed); err != nil || proposed.ProposalID == "" {
		return "", errors.New("scope proposal response omitted proposal_id")
	}
	// A one-validator fixture can cross quorum in the proposal block because the
	// authenticated proposer is the complete voting set. Exercise the signed
	// vote endpoint only when the canonical scope has not already executed. If a
	// proposal executes between this read and the vote, accept the failed vote
	// only after independently observing that exact scope as active.
	scopeActive, _ := v119StateSyncScopeActive(ctx, baseURL, key, governanceContext.ValidatorID)
	if !scopeActive {
		voteBody, marshalErr := json.Marshal(struct {
			ValidatorID      string `json:"validator_id"`
			GovernanceDomain string `json:"governance_domain"`
			ProposalID       string `json:"proposal_id"`
			Decision         string `json:"decision"`
		}{
			ValidatorID: governanceContext.ValidatorID, GovernanceDomain: governanceContext.GovernanceDomain,
			ProposalID: proposed.ProposalID, Decision: "accept",
		})
		if marshalErr != nil {
			return "", fmt.Errorf("encode scope vote: %w", marshalErr)
		}
		if _, voteErr := v119FixtureSignedRequest(ctx, baseURL, key, http.MethodPost, "/v1/governance/vote", voteBody); voteErr != nil {
			scopeActive, _ = v119StateSyncScopeActive(ctx, baseURL, key, governanceContext.ValidatorID)
			if !scopeActive {
				return "", fmt.Errorf("approve governed scope: %w", voteErr)
			}
		}
	}
	if err = waitV119StateSyncScope(ctx, baseURL, key, governanceContext.ValidatorID); err != nil {
		return "", err
	}

	submitBody, err := json.Marshal(struct {
		Content         string   `json:"content"`
		MemoryType      string   `json:"memory_type"`
		DomainTag       string   `json:"domain_tag"`
		ConfidenceScore float64  `json:"confidence_score"`
		Classification  int      `json:"classification"`
		Tags            []string `json:"tags"`
	}{
		Content: v119StateSyncFixtureContent, MemoryType: string(memory.TypeFact), DomainTag: v119StateSyncFixtureDomain,
		ConfidenceScore: 0.93, Classification: v119StateSyncFixtureClassification, Tags: v119StateSyncFixtureTags,
	})
	if err != nil {
		return "", fmt.Errorf("encode scoped memory: %w", err)
	}
	submitResponse, err := v119FixtureSignedRequest(ctx, baseURL, key, http.MethodPost, "/v1/memory/submit", submitBody)
	if err != nil {
		return "", fmt.Errorf("submit scoped memory: %w", err)
	}
	var submitted struct {
		MemoryID string `json:"memory_id"`
	}
	if err = json.Unmarshal(submitResponse, &submitted); err != nil || submitted.MemoryID == "" {
		return "", errors.New("scoped memory response omitted memory_id")
	}
	if err = waitV119StateSyncMemoryCommitted(ctx, baseURL, key, submitted.MemoryID); err != nil {
		return "", err
	}
	return submitted.MemoryID, nil
}

func waitV119StateSyncScope(ctx context.Context, baseURL string, key ed25519.PrivateKey, validatorID string) error {
	for {
		if active, _ := v119StateSyncScopeActive(ctx, baseURL, key, validatorID); active {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for governed scope: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func v119StateSyncScopeActive(ctx context.Context, baseURL string, key ed25519.PrivateKey, validatorID string) (bool, error) {
	path := "/v1/scopes/" + v119StateSyncFixtureScopeID
	body, err := v119FixtureSignedRequest(ctx, baseURL, key, http.MethodGet, path, nil)
	if err != nil {
		return false, err
	}
	var got struct {
		ScopeID               string `json:"scope_id"`
		Revision              uint64 `json:"revision"`
		RevisionHash          string `json:"revision_hash"`
		State                 string `json:"state"`
		ControllerValidatorID string `json:"controller_validator_id"`
		CreatedHeight         int64  `json:"created_height"`
		UpdatedHeight         int64  `json:"updated_height"`
		Domains               []struct {
			Name    string `json:"name"`
			Subtree bool   `json:"subtree"`
		} `json:"domains"`
		Members []struct {
			ValidatorID    string `json:"validator_id"`
			AssignedWeight uint64 `json:"assigned_weight"`
			JoinedRevision uint64 `json:"joined_revision"`
			Active         bool   `json:"active"`
		} `json:"members"`
	}
	if err = json.Unmarshal(body, &got); err != nil {
		return false, fmt.Errorf("decode governed scope: %w", err)
	}
	revisionHash, hashErr := hex.DecodeString(got.RevisionHash)
	return got.ScopeID == v119StateSyncFixtureScopeID &&
		got.Revision == 1 &&
		len(revisionHash) == sha256.Size && hashErr == nil &&
		got.State == "active" &&
		got.ControllerValidatorID == validatorID &&
		got.CreatedHeight > 0 && got.UpdatedHeight == got.CreatedHeight &&
		len(got.Domains) == 1 &&
		got.Domains[0].Name == v119StateSyncFixtureDomain && !got.Domains[0].Subtree &&
		len(got.Members) == 1 &&
		got.Members[0].ValidatorID == validatorID &&
		got.Members[0].AssignedWeight == 1 &&
		got.Members[0].JoinedRevision == 1 &&
		got.Members[0].Active, nil
}

func waitV119StateSyncMemoryCommitted(ctx context.Context, baseURL string, key ed25519.PrivateKey, memoryID string) error {
	path := "/v1/memory/" + memoryID
	for {
		body, err := v119FixtureSignedRequest(ctx, baseURL, key, http.MethodGet, path, nil)
		if err == nil {
			var got struct {
				MemoryID string `json:"memory_id"`
				Status   string `json:"status"`
			}
			if json.Unmarshal(body, &got) == nil && got.MemoryID == memoryID && got.Status == string(memory.StatusCommitted) {
				return nil
			}
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait for scoped memory commitment: %w", ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func verifyV119StateSyncScopedProjection(ctx context.Context, memoryID string) error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("load fixture config: %w", err)
	}
	projection, err := store.NewSQLiteStore(ctx, filepath.Join(cfg.DataDir, "sage.db"))
	if err != nil {
		return fmt.Errorf("open synchronized projection: %w", err)
	}
	defer func() { _ = projection.Close() }()
	record, err := projection.GetMemory(ctx, memoryID)
	if err != nil {
		return fmt.Errorf("read synchronized scoped memory: %w", err)
	}
	wantHash := sha256.Sum256([]byte(v119StateSyncFixtureContent))
	switch {
	case record.Content != v119StateSyncFixtureContent:
		return errors.New("synchronized scoped content does not match canonical content")
	case !bytes.Equal(record.ContentHash, wantHash[:]):
		return errors.New("synchronized scoped content hash is not canonical")
	case record.MemoryType != memory.TypeFact:
		return fmt.Errorf("synchronized scoped memory type %q, want fact", record.MemoryType)
	case record.DomainTag != v119StateSyncFixtureDomain:
		return fmt.Errorf("synchronized scoped domain %q, want %q", record.DomainTag, v119StateSyncFixtureDomain)
	case record.Status != memory.StatusCommitted:
		return fmt.Errorf("synchronized scoped status %q, want committed", record.Status)
	}
	classification, err := projection.GetMemoryClassificationLocal(ctx, memoryID)
	if err != nil {
		return fmt.Errorf("read synchronized scoped classification: %w", err)
	}
	if classification != v119StateSyncFixtureClassification {
		return fmt.Errorf("synchronized scoped classification %d, want %d", classification, v119StateSyncFixtureClassification)
	}
	tags, err := projection.GetTags(ctx, memoryID)
	if err != nil {
		return fmt.Errorf("read synchronized scoped tags: %w", err)
	}
	if !slices.Equal(tags, v119StateSyncFixtureTags) {
		return fmt.Errorf("synchronized scoped tags %v, want %v", tags, v119StateSyncFixtureTags)
	}
	return nil
}

func v119FixtureSignedRequest(ctx context.Context, baseURL string, key ed25519.PrivateKey, method, path string, body []byte) ([]byte, error) {
	var nonce [8]byte
	if _, err := io.ReadFull(rand.Reader, nonce[:]); err != nil {
		return nil, fmt.Errorf("generate request nonce: %w", err)
	}
	timestamp := time.Now().Unix()
	signature := auth.SignRequestWithNonce(key, method, path, body, timestamp, nonce[:])
	publicKey, ok := key.Public().(ed25519.PublicKey)
	if !ok {
		return nil, errors.New("fixture signing key does not expose an Ed25519 public key")
	}
	request, err := http.NewRequestWithContext(ctx, method, baseURL+path, bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("create signed request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Agent-ID", auth.PublicKeyToAgentID(publicKey))
	request.Header.Set("X-Signature", hex.EncodeToString(signature))
	request.Header.Set("X-Timestamp", fmt.Sprintf("%d", timestamp))
	request.Header.Set("X-Nonce", hex.EncodeToString(nonce[:]))
	response, err := tlsAwareClient(baseURL).Do(request)
	if err != nil {
		return nil, fmt.Errorf("send signed request: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	responseBody, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("read signed response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", response.StatusCode, responseBody)
	}
	return responseBody, nil
}
