//go:build v119testfixture

package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"time"

	"github.com/cometbft/cometbft/privval"
	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/memory"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/taint"
	"github.com/l33tdawg/sage/internal/tx"
)

const (
	// The real governed scope created by install-scoped-proof retains these
	// payloads canonically. Ordinary memory plaintext remains node-local.
	v30SyncDomain         = v119StateSyncFixtureDomain
	v30SyncOrdinaryDomain = "ordinary-v30-sync"
)

type v30SyncBranch struct {
	Parent   tx.ParentRef `json:"parent"`
	ChildID  string       `json:"child_id"`
	Decision string       `json:"decision"`
}

type v30SyncProof struct {
	AgentID  string        `json:"agent_id"`
	RootID   string        `json:"root_id"`
	Active   v30SyncBranch `json:"active"`
	Cleared  v30SyncBranch `json:"cleared"`
	Ordinary v30SyncBranch `json:"ordinary"`
}

// This command only prepares the owned fixture home. The ordinary production
// generator signs an explicit app30 origin; serve does not walk an upgrade ladder.
func initV30StateSyncFixture() error {
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	bootstrap := &VendoredAgentBootstrapConfig{AgentKeyFile: filepath.Join(SageHome(), "v30-proof-agent.key"), HomeDomain: v30SyncDomain, Clearance: 1}
	if err = initCometBFTConfigWithGenesisVersion(filepath.Join(cfg.DataDir, "cometbft"), cfg.AgentKey, bootstrap, 30); err != nil {
		return err
	}
	// An offline-created genesis is persisted origin evidence on first serve.
	// Use the ordinary exclusive/synced initializer stamps so the production
	// markerless-origin guard can remain fail-closed (diagnostic fork stays 1).
	return writeFreshPersonalMarkers(SageHome())
}

func loadV30SyncProof() (*v30SyncProof, error) {
	raw, err := os.ReadFile(filepath.Join(SageHome(), "v30-proof.json"))
	if err != nil {
		return nil, err
	}
	var proof v30SyncProof
	if err = json.Unmarshal(raw, &proof); err != nil {
		return nil, err
	}
	if !v119CanonicalHash(proof.AgentID) || !v119CanonicalHash(proof.RootID) || !v119CanonicalHash(proof.Active.Decision) || !v119CanonicalHash(proof.Cleared.Decision) || !v119CanonicalHash(proof.Ordinary.Decision) {
		return nil, errors.New("noncanonical app30 proof identities")
	}
	return &proof, nil
}

func v30SyncClient() (ed25519.PrivateKey, string, error) {
	key, err := loadProposeSigningKey(filepath.Join(SageHome(), "v30-proof-agent.key"))
	if err != nil {
		return nil, "", err
	}
	cfg, err := LoadConfig()
	if err != nil {
		return nil, "", err
	}
	return key, restBaseURL(cfg.RESTAddr), nil
}

func installV30StateSyncProof(parent context.Context) error {
	ctx, cancel := context.WithTimeout(parent, 3*time.Minute)
	defer cancel()
	agent, api, err := v30SyncClient()
	if err != nil {
		return err
	}
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	root, err := loadProposeSigningKey(cfg.AgentKey)
	if err != nil {
		return err
	}
	genesis, err := readChainIDFromGenesis(filepath.Join(cfg.DataDir, "cometbft"))
	if err != nil {
		return err
	}
	proof := v30SyncProof{AgentID: auth.PublicKeyToAgentID(agent.Public().(ed25519.PublicKey)), RootID: auth.PublicKeyToAgentID(root.Public().(ed25519.PublicKey))}
	for _, entry := range []struct {
		name   string
		branch *v30SyncBranch
		key    ed25519.PrivateKey
		domain string
	}{{"cleared", &proof.Cleared, agent, v30SyncDomain}, {"active", &proof.Active, agent, v30SyncDomain}, {"ordinary", &proof.Ordinary, root, v30SyncOrdinaryDomain}} {
		content := "fresh app30 wire " + entry.name + " parent"
		parentID, submitErr := submitV30SyncMemory(ctx, api, entry.key, entry.domain, content, nil)
		if submitErr != nil {
			return submitErr
		}
		entry.branch.Parent = tx.ParentRef{SourceChainID: genesis, MemoryID: parentID, ContentHash: sha256.Sum256([]byte(content))}
		entry.branch.ChildID, err = submitV30SyncMemory(ctx, api, entry.key, entry.domain, "fresh app30 wire "+entry.name+" child", &entry.branch.Parent)
		if err != nil {
			return err
		}
		evidence := sha256.Sum256([]byte("fresh app30 wire " + entry.name + " evidence"))
		payload, encodeErr := tx.EncodeTaintApplyPayload(tx.TaintApplyPayload{Version: tx.TaintApplyPayloadVersion, Action: tx.TaintActionApply,
			TargetChainID: genesis, TargetMemoryID: parentID, TargetContentHash: entry.branch.Parent.ContentHash[:], ScopeKind: tx.TaintScopeDirect,
			ReasonCategory: 1, EvidenceDigest: evidence[:]})
		if encodeErr != nil {
			return encodeErr
		}
		entry.branch.Decision, err = tx.TaintApplyDecisionID(payload)
		if err != nil {
			return err
		}
		if err = governV30Sync(ctx, cfg, root, entry.branch.Decision, payload); err != nil {
			return err
		}
		for _, id := range []string{parentID, entry.branch.ChildID} {
			if err = waitV30SyncDisclosure(ctx, api, agent, id, false); err != nil {
				return err
			}
		}
		if entry.name == "cleared" {
			decision, decodeErr := hex.DecodeString(entry.branch.Decision)
			if decodeErr != nil {
				return decodeErr
			}
			payload, err = tx.EncodeTaintClearPayload(tx.TaintClearPayload{Version: tx.TaintClearPayloadVersion, Action: tx.TaintActionClear,
				DecisionID: decision, ReasonCategory: 2, EvidenceDigest: evidence[:]})
			if err != nil {
				return err
			}
			target, targetErr := tx.TaintClearTargetID(payload)
			if targetErr != nil {
				return targetErr
			}
			if err = governV30Sync(ctx, cfg, root, target, payload); err != nil {
				return err
			}
			for _, id := range []string{parentID, entry.branch.ChildID} {
				if err = waitV30SyncDisclosure(ctx, api, agent, id, true); err != nil {
					return err
				}
			}
		}
	}
	raw, err := json.MarshalIndent(proof, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(SageHome(), "v30-proof.json"), raw, 0600)
}

func submitV30SyncMemory(ctx context.Context, api string, key ed25519.PrivateKey, domain, content string, parent *tx.ParentRef) (string, error) {
	body := map[string]any{"content": content, "memory_type": "fact", "domain_tag": domain, "confidence_score": 0.93}
	if parent != nil {
		body["derived_from"] = []map[string]string{{"source_chain_id": parent.SourceChainID, "memory_id": parent.MemoryID, "content_hash": hex.EncodeToString(parent.ContentHash[:])}}
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return "", err
	}
	response, err := v119FixtureSignedRequest(ctx, api, key, http.MethodPost, "/v1/memory/submit", raw)
	if err != nil {
		return "", err
	}
	var submitted struct {
		MemoryID string `json:"memory_id"`
	}
	if err = json.Unmarshal(response, &submitted); err != nil || submitted.MemoryID == "" {
		return "", errors.New("app30 submit omitted memory_id")
	}
	return submitted.MemoryID, waitV119StateSyncMemoryCommitted(ctx, api, key, submitted.MemoryID)
}

func governV30Sync(ctx context.Context, cfg *Config, root ed25519.PrivateKey, target string, payload []byte) error {
	proposal := &tx.ParsedTx{Type: tx.TxTypeGovPropose, Timestamp: time.Now(), GovPropose: &tx.GovPropose{
		Operation: tx.GovOpMemoryTaint, TargetID: target, Payload: payload, Reason: "fresh app30 real network state-sync proof"}}
	result, err := broadcastV30Sync(ctx, root, proposal)
	if err != nil {
		return err
	}
	id := governance.ComputeProposalID(auth.PublicKeyToAgentID(root.Public().(ed25519.PublicKey)), result.Height, governance.OpMemoryTaint, target)
	cometHome := filepath.Join(cfg.DataDir, "cometbft")
	pv := privval.LoadFilePV(filepath.Join(cometHome, "config", "priv_validator_key.json"), filepath.Join(cometHome, "data", "priv_validator_state.json"))
	validatorKey := ed25519.PrivateKey(pv.Key.PrivKey.Bytes())
	_, err = broadcastV30Sync(ctx, validatorKey, &tx.ParsedTx{Type: tx.TxTypeGovVote, Timestamp: time.Now(), GovVote: &tx.GovVote{ProposalID: id, Decision: tx.VoteDecisionAccept}})
	return err
}

func broadcastV30Sync(ctx context.Context, key ed25519.PrivateKey, parsed *tx.ParsedTx) (*broadcastCommitResp, error) {
	var result *broadcastCommitResp
	err := tx.WithNonceLease(ctx, key, func(nonce uint64) error {
		parsed.Nonce = nonce
		if err := tx.SignTx(parsed, key); err != nil {
			return err
		}
		raw, err := tx.EncodeTx(parsed)
		if err != nil {
			return err
		}
		result, err = broadcastTxCommitWithSigner(ctx, cmtRPCClientURL(), key, raw)
		if err != nil {
			return err
		}
		if result.CheckTxCode != 0 || result.TxResultCode != 0 {
			return fmt.Errorf("app30 signed transaction rejected: check=%d %s; deliver=%d %s", result.CheckTxCode, result.CheckTxLog, result.TxResultCode, result.TxResultLog)
		}
		return nil
	})
	return result, err
}

func waitV30SyncDisclosure(ctx context.Context, api string, key ed25519.PrivateKey, id string, readable bool) error {
	for {
		body, err := v119FixtureSignedRequest(ctx, api, key, http.MethodGet, "/v1/memory/"+id, nil)
		if readable && err == nil {
			var record memory.MemoryRecord
			if json.Unmarshal(body, &record) == nil && record.MemoryID == id && record.Status == memory.StatusCommitted {
				return nil
			}
		}
		if !readable && err != nil && (bytes.Contains([]byte(err.Error()), []byte("HTTP 451:"))) &&
			(bytes.Contains([]byte(err.Error()), []byte("https://sage.dev/errors/memory-tainted")) || bytes.Contains([]byte(err.Error()), []byte("https://sage.dev/errors/memory-taint-pending"))) {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("app30 disclosure %s readable=%t: %w (last error %v)", id, readable, ctx.Err(), err)
		case <-time.After(250 * time.Millisecond):
		}
	}
}

func verifyV30StateSyncServing(parent context.Context, restored bool) error {
	ctx, cancel := context.WithTimeout(parent, 45*time.Second)
	defer cancel()
	proof, err := loadV30SyncProof()
	if err != nil {
		return err
	}
	agent, api, err := v30SyncClient()
	if err != nil {
		return err
	}
	for _, branch := range []v30SyncBranch{proof.Active, proof.Cleared} {
		for _, id := range []string{branch.Parent.MemoryID, branch.ChildID} {
			if err = waitV30SyncDisclosure(ctx, api, agent, id, branch.Decision == proof.Cleared.Decision); err != nil {
				return err
			}
		}
	}
	for _, id := range []string{proof.Ordinary.Parent.MemoryID, proof.Ordinary.ChildID} {
		if restored {
			_, requestErr := v119FixtureSignedRequest(ctx, api, agent, http.MethodGet, "/v1/memory/"+id, nil)
			if requestErr == nil || !bytes.Contains([]byte(requestErr.Error()), []byte("HTTP 404:")) || !bytes.Contains([]byte(requestErr.Error()), []byte("https://sage.dev/errors/404")) {
				return fmt.Errorf("ordinary node-local payload unexpectedly disclosed after state sync: %s: %v", id, requestErr)
			}
		} else if err = waitV30SyncDisclosure(ctx, api, agent, id, false); err != nil {
			return err
		}
	}
	return nil
}

// Run only with serve stopped. No SQL bytes are copied into either receiver;
// its projection is rebuilt from the transferred canonical snapshot.
func verifyV30StateSyncCanonical(ctx context.Context, expectedHash string) error {
	proof, err := loadV30SyncProof()
	if err != nil {
		return err
	}
	cfg, err := LoadConfig()
	if err != nil {
		return err
	}
	canonical, err := store.OpenBadgerStoreWithoutMigrations(filepath.Join(cfg.DataDir, "badger"))
	if err != nil {
		return err
	}
	defer func() { _ = canonical.CloseBadger() }()
	origin, err := canonical.GetFreshGenesisActivation()
	if err != nil || origin == nil || origin.AppVersion != 30 || origin.RootID != proof.RootID || origin.Agent == nil || origin.Agent.AgentID != proof.AgentID || len(origin.Validators) != 1 {
		return fmt.Errorf("restored fresh app30 origin mismatch: %v", err)
	}
	for version := 2; version <= 30; version++ {
		applied, appliedErr := canonical.GetAppliedUpgrade(fmt.Sprintf("app-v%d", version))
		if appliedErr != nil || applied != nil {
			return fmt.Errorf("restored fresh origin invented app-v%d upgrade: %v", version, appliedErr)
		}
	}
	for _, validate := range []func() error{canonical.ValidateFreshGenesisLineage, canonical.ValidateLineageIndexes, canonical.ValidateTaintState} {
		if err = validate(); err != nil {
			return err
		}
	}
	hash, err := canonical.ComputePublicMemoryAppHash()
	if err != nil || hex.EncodeToString(hash) != expectedHash {
		return fmt.Errorf("restored canonical AppHash %x, want %s: %v", hash, expectedHash, err)
	}
	projection, err := store.NewSQLiteStore(ctx, filepath.Join(cfg.DataDir, "sage.db"))
	if err != nil {
		return err
	}
	defer func() { _ = projection.Close() }()
	canonicalRevision, effective, err := canonical.SnapshotEffectiveTaintMemberships()
	if err != nil {
		return err
	}
	mirrorRevision, err := projection.MemoryTaintMirrorRevision(ctx)
	if err != nil || mirrorRevision != canonicalRevision {
		return fmt.Errorf("rebuilt SQL taint revision %d, want %d: %v", mirrorRevision, canonicalRevision, err)
	}
	wantEffective := []string{proof.Active.Parent.MemoryID, proof.Active.ChildID, proof.Ordinary.Parent.MemoryID, proof.Ordinary.ChildID}
	slices.Sort(wantEffective)
	if !slices.Equal(effective, wantEffective) {
		return fmt.Errorf("restored effective taint set %v, want %v", effective, wantEffective)
	}
	if err = verifyV30SQLTaintIDs(ctx, filepath.Join(cfg.DataDir, "sage.db"), wantEffective); err != nil {
		return err
	}
	visible, total, err := projection.ListMemories(ctx, store.ListOptions{DomainTag: v30SyncDomain, SubmittingAgent: proof.AgentID, Limit: 10, ExcludeTainted: true})
	if err != nil || total != 2 || len(visible) != 2 {
		return fmt.Errorf("rebuilt SQL taint filter total=%d, want two cleared rows: %v", total, err)
	}
	visibleIDs := []string{visible[0].MemoryID, visible[1].MemoryID}
	wantVisible := []string{proof.Cleared.Parent.MemoryID, proof.Cleared.ChildID}
	slices.Sort(visibleIDs)
	slices.Sort(wantVisible)
	if !slices.Equal(visibleIDs, wantVisible) {
		return fmt.Errorf("rebuilt SQL taint filter IDs=%v, want %v", visibleIDs, wantVisible)
	}
	for _, branch := range []v30SyncBranch{proof.Active, proof.Cleared, proof.Ordinary} {
		ordinary := branch.Decision == proof.Ordinary.Decision
		parents, lineageErr := canonical.GetMemoryLineage(branch.ChildID)
		if lineageErr != nil || len(parents) != 1 || parents[0] != branch.Parent {
			return fmt.Errorf("restored forward lineage mismatch: %v", lineageErr)
		}
		parentID, parentErr := tx.ParentRefIDHex(branch.Parent)
		if parentErr != nil {
			return parentErr
		}
		children, childErr := canonical.ListLineageChildren(parentID, "", 10)
		if childErr != nil || !slices.Equal(children, []string{branch.ChildID}) {
			return fmt.Errorf("restored reverse lineage mismatch: %v", childErr)
		}
		header, found, headerErr := canonical.GetTaintDecision(branch.Decision)
		if headerErr != nil || !found || (branch.Decision == proof.Cleared.Decision && header.Status != taint.StatusClean) ||
			(branch.Decision != proof.Cleared.Decision && header.Status != taint.StatusComplete) {
			return fmt.Errorf("restored taint decision state mismatch: %v (status %d)", headerErr, header.Status)
		}
		for _, id := range []string{branch.Parent.MemoryID, branch.ChildID} {
			signers, present, signerErr := canonical.GetAppV30Provenance(id)
			want := []store.AppV30ProvenanceSigner{{CredentialID: proof.AgentID, PrincipalID: proof.AgentID, Exact: true}}
			if ordinary {
				want = []store.AppV30ProvenanceSigner{{CredentialID: proof.RootID, PrincipalID: proof.RootID, RootGeneration: 1, Exact: true}}
			}
			if signerErr != nil || !present || !slices.Equal(signers, want) {
				return fmt.Errorf("restored signed provenance mismatch for %s: %v", id, signerErr)
			}
			verdict, verdictErr := canonical.ClassifyMemoryTaint(id)
			if verdictErr != nil || !verdict.CoverageComplete || verdict.LineageMissing ||
				(branch.Decision != proof.Cleared.Decision && (verdict.Outcome != store.TaintServingTainted || !slices.Equal(verdict.DecisionIDs, []string{branch.Decision}))) ||
				(branch.Decision == proof.Cleared.Decision && verdict.Outcome != store.TaintServingClear) {
				return fmt.Errorf("restored canonical taint classification mismatch for %s: %+v %v", id, verdict, verdictErr)
			}
			record, recordErr := projection.GetMemory(ctx, id)
			if ordinary {
				if !errors.Is(recordErr, store.ErrMemoryNotFound) {
					return fmt.Errorf("ordinary plaintext was incorrectly fabricated during SQL rebuild: %s: %v", id, recordErr)
				}
				content := "fresh app30 wire ordinary parent"
				if id == branch.ChildID {
					content = "fresh app30 wire ordinary child"
				}
				wantHash := sha256.Sum256([]byte(content))
				contentHash, status, hashErr := canonical.GetMemoryHash(id)
				author, authorErr := canonical.GetMemoryAuthor(id)
				if hashErr != nil || authorErr != nil || status != string(memory.StatusCommitted) || !bytes.Equal(contentHash, wantHash[:]) || author != proof.RootID {
					return fmt.Errorf("ordinary canonical envelope was not preserved: %s: %v %v", id, hashErr, authorErr)
				}
				continue
			}
			if recordErr != nil || record.Status != memory.StatusCommitted || record.DomainTag != v30SyncDomain || record.SubmittingAgent != proof.AgentID {
				return fmt.Errorf("rebuilt SQL envelope mismatch for %s: %v", id, recordErr)
			}
			contentHash := sha256.Sum256([]byte(record.Content))
			if !bytes.Equal(contentHash[:], record.ContentHash) {
				return fmt.Errorf("rebuilt SQL content hash mismatch for %s", id)
			}
			_, disposition, classifyErr := canonical.ClassifyMemoryProjection(record)
			if branch.Decision == proof.Active.Decision {
				if !errors.Is(classifyErr, store.ErrMemoryProjectionTainted) || disposition != store.MemoryProjectionTainted {
					return fmt.Errorf("rebuilt SQL row lacks canonical active hold: %s %v", disposition, classifyErr)
				}
			} else if classifyErr != nil || disposition != store.MemoryProjectionExact {
				return fmt.Errorf("rebuilt SQL cleared row is not exact: %s %v", disposition, classifyErr)
			}
		}
	}
	fmt.Printf("fresh app30 canonical origin, ordinary+scoped forward/reverse lineage and exact provenance, active+cleared taint, scoped SQL rebuild, ordinary payload404 and AppHash=%s verified\n", expectedHash)
	return nil
}

func verifyV30SQLTaintIDs(ctx context.Context, path string, expected []string) error {
	db, err := sql.Open("sqlite", "file:"+path+"?mode=ro")
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()
	rows, err := db.QueryContext(ctx, "SELECT memory_id FROM memory_taint_mirror ORDER BY memory_id")
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()
	var actual []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			return err
		}
		actual = append(actual, id)
	}
	if err = rows.Err(); err != nil {
		return err
	}
	if !slices.Equal(actual, expected) {
		return fmt.Errorf("rebuilt ordinary+scoped SQL taint mirror IDs=%v, want %v", actual, expected)
	}
	return nil
}
