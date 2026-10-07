package mcp

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

var errDurableClaimantIdentityBusy = errors.New("durable claimant identity is owned by a live concurrent runtime")

type durableClaimantIdentity struct {
	Version           int    `json:"version"`
	AgentID           string `json:"agent_id"`
	Provider          string `json:"provider"`
	Project           string `json:"project"`
	TransportScope    string `json:"transport_scope,omitempty"`
	ClaimantSessionID string `json:"claimant_session_id"`
}

// acquireDurableClaimantIdentity gives the primary runtime for one stable scope
// a claimant identity that survives ordinary process restarts. Empty
// transportScope preserves the historical stdio path; HTTP callers add their
// server-derived bearer scope. The OS lock is the liveness fence. Contention is
// distinct from storage failure because only a live competitor may safely make
// this runtime use an independent ephemeral identity.
func acquireDurableClaimantIdentity(agentID, provider, project, transportScope string) (string, io.Closer, error) {
	home := strings.TrimSpace(os.Getenv("SAGE_HOME"))
	if home == "" {
		userHome, err := os.UserHomeDir()
		if err != nil {
			return "", nil, fmt.Errorf("resolve SAGE home for claimant identity: %w", err)
		}
		home = filepath.Join(userHome, ".sage")
	}
	scopeInput := agentID + "\x00" + provider + "\x00" + project
	if transportScope != "" {
		scopeInput += "\x00" + transportScope
	}
	scope := sha256.Sum256([]byte(scopeInput))
	dir := filepath.Join(home, "runtime", "mcp-claimants")
	if err := os.MkdirAll(dir, 0700); err != nil { //nolint:gosec // operator-selected SAGE_HOME, scope leaf is a local hash
		return "", nil, fmt.Errorf("create claimant identity directory: %w", err)
	}
	base := hex.EncodeToString(scope[:])
	lease, acquired, err := tryLockClaimantFile(filepath.Join(dir, base+".lock"))
	if err != nil {
		return "", nil, err
	}
	if !acquired {
		return "", nil, errDurableClaimantIdentityBusy
	}

	identityPath := filepath.Join(dir, base+".json")
	if raw, readErr := os.ReadFile(identityPath); readErr == nil { //nolint:gosec // private path under SAGE_HOME
		var saved durableClaimantIdentity
		if json.Unmarshal(raw, &saved) == nil && saved.Version == 1 && saved.AgentID == agentID &&
			saved.Provider == provider && saved.Project == project && saved.TransportScope == transportScope &&
			validMCPClaimantSessionID(saved.ClaimantSessionID) {
			return saved.ClaimantSessionID, lease, nil
		}
		_ = lease.Close()
		return "", nil, fmt.Errorf("durable claimant identity is corrupt or does not match its scope")
	} else if !errors.Is(readErr, os.ErrNotExist) {
		_ = lease.Close()
		return "", nil, fmt.Errorf("read durable claimant identity: %w", readErr)
	}

	id := newMCPClaimantSessionID()
	if id == "" {
		_ = lease.Close()
		return "", nil, fmt.Errorf("generate durable claimant identity")
	}
	record := durableClaimantIdentity{
		Version: 1, AgentID: agentID, Provider: provider, Project: project,
		TransportScope: transportScope, ClaimantSessionID: id,
	}
	raw, err := json.Marshal(record)
	if err != nil {
		_ = lease.Close()
		return "", nil, err
	}
	tmp, err := os.CreateTemp(dir, base+".tmp-") //nolint:gosec // private path under SAGE_HOME
	if err != nil {
		_ = lease.Close()
		return "", nil, fmt.Errorf("create claimant identity temp file: %w", err)
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }() //nolint:gosec // path returned by os.CreateTemp in the private claimant directory
	if chmodErr := tmp.Chmod(0600); chmodErr != nil {
		_ = tmp.Close()
		_ = lease.Close()
		return "", nil, chmodErr
	}
	if _, writeErr := tmp.Write(raw); writeErr != nil {
		_ = tmp.Close()
		_ = lease.Close()
		return "", nil, writeErr
	}
	if syncErr := tmp.Sync(); syncErr != nil {
		_ = tmp.Close()
		_ = lease.Close()
		return "", nil, syncErr
	}
	if closeErr := tmp.Close(); closeErr != nil {
		_ = lease.Close()
		return "", nil, closeErr
	}
	if renameErr := os.Rename(tmpPath, identityPath); renameErr != nil { //nolint:gosec // both paths are fixed children of the private claimant directory
		_ = lease.Close()
		return "", nil, fmt.Errorf("persist claimant identity: %w", renameErr)
	}
	// Sync the directory entry as well as the file contents where the platform
	// supports it. Without this Unix fence, a crash after rename can lose an
	// identity that this process already acknowledged.
	if syncErr := syncClaimantIdentityDirectory(dir); syncErr != nil {
		_ = lease.Close()
		return "", nil, syncErr
	}
	return id, lease, nil
}

func validMCPClaimantSessionID(id string) bool {
	if len(id) != len("mcp-")+32 || !strings.HasPrefix(id, "mcp-") {
		return false
	}
	_, err := hex.DecodeString(id[len("mcp-"):])
	return err == nil
}
