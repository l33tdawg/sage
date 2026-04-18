package abci

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

// Full app tests require PostgreSQL — these are basic sanity checks.

func TestComputeBlockHash(t *testing.T) {
	h1 := computeBlockHash([]string{"a", "b", "c"}, 1)
	h2 := computeBlockHash([]string{"c", "b", "a"}, 1) // Different order, same result (sorted)
	h3 := computeBlockHash([]string{"a", "b", "c"}, 2) // Different height

	assert.Equal(t, h1, h2, "should be deterministic regardless of input order")
	assert.NotEqual(t, h1, h3, "different height should produce different hash")
}

// ---------------------------------------------------------------------------
// Test helpers for agent processor tests
// ---------------------------------------------------------------------------

// setupTestApp creates a SageApp backed by temp BadgerDB + SQLite.
func setupTestApp(t *testing.T) *SageApp {
	t.Helper()
	bs := setupTestBadger(t)
	dbPath := filepath.Join(t.TempDir(), "test-offchain.db")
	sqlite, err := store.NewSQLiteStore(context.Background(), dbPath)
	require.NoError(t, err)
	t.Cleanup(func() { sqlite.Close() })

	logger := zerolog.Nop()
	app, err := NewSageAppWithStores(bs, sqlite, logger)
	require.NoError(t, err)
	return app
}

// agentKey represents a test agent's Ed25519 keypair and derived hex ID.
type agentKey struct {
	pub  ed25519.PublicKey
	priv ed25519.PrivateKey
	id   string // hex-encoded public key
}

// newAgentKey generates a fresh Ed25519 keypair for testing.
func newAgentKey(t *testing.T) agentKey {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	return agentKey{pub: pub, priv: priv, id: hex.EncodeToString(pub)}
}

// signAgentProof builds the AgentPubKey/AgentSig/AgentBodyHash/AgentTimestamp
// fields that verifyAgentIdentity expects on a ParsedTx.
func signAgentProof(t *testing.T, ak agentKey, body []byte) (pubKey, sig, bodyHash []byte, ts int64) {
	t.Helper()
	h := sha256.Sum256(body)
	ts = time.Now().Unix()
	tsBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(tsBytes, uint64(ts))
	message := append(h[:], tsBytes...)
	sig = ed25519.Sign(ak.priv, message)
	return ak.pub, sig, h[:], ts
}

// makeAgentRegisterTx builds a signed ParsedTx for TxTypeAgentRegister.
func makeAgentRegisterTx(t *testing.T, ak agentKey, name, role, bio, provider, p2p string) *tx.ParsedTx {
	t.Helper()
	body := []byte(name + role + bio)
	pubKey, sig, bodyHash, ts := signAgentProof(t, ak, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeAgentRegister,
		AgentRegister: &tx.AgentRegister{
			AgentID:    ak.id,
			Name:       name,
			Role:       role,
			BootBio:    bio,
			Provider:   provider,
			P2PAddress: p2p,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// makeAgentUpdateTx builds a signed ParsedTx for TxTypeAgentUpdate.
func makeAgentUpdateTx(t *testing.T, sender agentKey, targetID, name, bio string) *tx.ParsedTx {
	t.Helper()
	body := []byte(name + bio)
	pubKey, sig, bodyHash, ts := signAgentProof(t, sender, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeAgentUpdate,
		AgentUpdateTx: &tx.AgentUpdate{
			AgentID: targetID,
			Name:    name,
			BootBio: bio,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// makeAgentSetPermissionTx builds a signed ParsedTx for TxTypeAgentSetPermission.
func makeAgentSetPermissionTx(t *testing.T, sender agentKey, targetID string, clearance uint8, domainAccess, visibleAgents, orgID, deptID string) *tx.ParsedTx {
	t.Helper()
	body := []byte(targetID)
	pubKey, sig, bodyHash, ts := signAgentProof(t, sender, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeAgentSetPermission,
		AgentSetPermission: &tx.AgentSetPermission{
			AgentID:       targetID,
			Clearance:     clearance,
			DomainAccess:  domainAccess,
			VisibleAgents: visibleAgents,
			OrgID:         orgID,
			DeptID:        deptID,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

// registerAgent is a convenience that registers an agent and asserts success.
func registerAgent(t *testing.T, app *SageApp, ak agentKey, name, role string) {
	t.Helper()
	ptx := makeAgentRegisterTx(t, ak, name, role, "test bio", "test-provider", "/ip4/127.0.0.1/tcp/26656")
	result := app.processAgentRegister(ptx, 1, time.Now())
	require.Equal(t, uint32(0), result.Code, "register should succeed: %s", result.Log)
}

// ---------------------------------------------------------------------------
// Agent register tests
// ---------------------------------------------------------------------------

func TestProcessAgentRegister(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	ptx := makeAgentRegisterTx(t, ak, "test-agent", "member", "I am a test agent", "claude-code", "/ip4/127.0.0.1/tcp/26656")
	result := app.processAgentRegister(ptx, 1, time.Now())

	// Returns code 0
	assert.Equal(t, uint32(0), result.Code, "expected success, got: %s", result.Log)
	assert.Equal(t, ak.id, string(result.Data))

	// Agent is stored in BadgerDB
	assert.True(t, app.badgerStore.IsAgentRegistered(ak.id))
	agent, err := app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "test-agent", agent.Name)
	assert.Equal(t, "test-agent", agent.RegisteredName, "RegisteredName should match initial Name at registration")
	assert.Equal(t, "member", agent.Role)
	assert.Equal(t, "I am a test agent", agent.BootBio)
	assert.Equal(t, "claude-code", agent.Provider)
	assert.Equal(t, uint8(1), agent.Clearance, "default clearance should be INTERNAL (1)")

	// Pending write is buffered for offchain store
	require.Len(t, app.pendingWrites, 1)
	assert.Equal(t, "agent_register", app.pendingWrites[0].writeType)
	entry, ok := app.pendingWrites[0].data.(*store.AgentEntry)
	require.True(t, ok)
	assert.Equal(t, ak.id, entry.AgentID)
	assert.Equal(t, "test-agent", entry.RegisteredName, "pending write should include RegisteredName")
	assert.Equal(t, "active", entry.Status)
}

func TestProcessAgentRegisterIdempotent(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// First registration
	ptx := makeAgentRegisterTx(t, ak, "agent-v1", "member", "bio", "claude-code", "")
	r1 := app.processAgentRegister(ptx, 1, time.Now())
	require.Equal(t, uint32(0), r1.Code)

	// Second registration of the same agent — idempotent success
	ptx2 := makeAgentRegisterTx(t, ak, "agent-v2", "admin", "new bio", "chatgpt", "")
	r2 := app.processAgentRegister(ptx2, 2, time.Now())
	assert.Equal(t, uint32(0), r2.Code, "idempotent re-register should succeed")

	// Original data should be preserved (not overwritten)
	agent, err := app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "agent-v1", agent.Name, "idempotent register should keep original name")
	assert.Equal(t, "agent-v1", agent.RegisteredName, "RegisteredName must survive idempotent re-registration")

	// 2 pending writes: first registration + idempotent backfill write for on_chain_height
	assert.Len(t, app.pendingWrites, 2, "idempotent register should buffer backfill write for on_chain_height")
	idempotentEntry, ok := app.pendingWrites[1].data.(*store.AgentEntry)
	require.True(t, ok)
	assert.Equal(t, "agent-v1", idempotentEntry.RegisteredName, "idempotent pending write should carry original RegisteredName")
}

// ---------------------------------------------------------------------------
// Agent update tests
// ---------------------------------------------------------------------------

func TestProcessAgentUpdate(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Register first
	registerAgent(t, app, ak, "original-name", "member")

	// Update metadata
	ptx := makeAgentUpdateTx(t, ak, ak.id, "updated-name", "updated bio")
	result := app.processAgentUpdate(ptx, 2, time.Now())
	assert.Equal(t, uint32(0), result.Code, "update should succeed: %s", result.Log)

	// Verify on-chain state changed
	agent, err := app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "updated-name", agent.Name)
	assert.Equal(t, "updated bio", agent.BootBio)
	assert.Equal(t, "original-name", agent.RegisteredName, "RegisteredName must be immutable across updates")

	// Verify pending write was buffered (1 from register + 1 from update)
	require.Len(t, app.pendingWrites, 2)
	assert.Equal(t, "agent_update", app.pendingWrites[1].writeType)
	upd, ok := app.pendingWrites[1].data.(*agentUpdateData)
	require.True(t, ok)
	assert.Equal(t, "updated-name", upd.Name)
}

func TestProcessAgentUpdateSelfOnly(t *testing.T) {
	app := setupTestApp(t)
	owner := newAgentKey(t)
	other := newAgentKey(t)

	// Register the owner agent
	registerAgent(t, app, owner, "owner-agent", "member")
	// Register the other agent too (so it exists)
	registerAgent(t, app, other, "other-agent", "member")

	// Try to update owner's metadata using other's key
	ptx := makeAgentUpdateTx(t, other, owner.id, "hacked-name", "hacked bio")
	result := app.processAgentUpdate(ptx, 3, time.Now())
	assert.Equal(t, uint32(63), result.Code, "updating another agent should fail with code 63")
	assert.Contains(t, result.Log, "access denied")

	// Verify owner's data was not changed
	agent, err := app.badgerStore.GetRegisteredAgent(owner.id)
	require.NoError(t, err)
	assert.Equal(t, "owner-agent", agent.Name, "name should remain unchanged")
}

func TestRegisteredNameImmutableAcrossMultipleUpdates(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Register with initial name
	registerAgent(t, app, ak, "original-identity", "member")

	// Verify RegisteredName is set at registration
	agent, err := app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "original-identity", agent.RegisteredName)
	assert.Equal(t, "original-identity", agent.Name)

	// First rename
	ptx1 := makeAgentUpdateTx(t, ak, ak.id, "display-name-v1", "bio v1")
	r1 := app.processAgentUpdate(ptx1, 2, time.Now())
	assert.Equal(t, uint32(0), r1.Code)

	agent, err = app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "display-name-v1", agent.Name, "Name should be updated")
	assert.Equal(t, "original-identity", agent.RegisteredName, "RegisteredName must remain immutable after first rename")

	// Second rename
	ptx2 := makeAgentUpdateTx(t, ak, ak.id, "display-name-v2", "bio v2")
	r2 := app.processAgentUpdate(ptx2, 3, time.Now())
	assert.Equal(t, uint32(0), r2.Code)

	agent, err = app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "display-name-v2", agent.Name, "Name should be updated again")
	assert.Equal(t, "original-identity", agent.RegisteredName, "RegisteredName must remain immutable after second rename")
}

func TestRegisteredNameBackfillForLegacyAgent(t *testing.T) {
	app := setupTestApp(t)
	ak := newAgentKey(t)

	// Simulate a legacy agent by writing directly to BadgerDB without RegisteredName.
	// We use the raw badger transaction to bypass RegisterAgent (which sets RegisteredName).
	legacyData, err := json.Marshal(map[string]any{
		"agent_id":      ak.id,
		"name":          "legacy-agent",
		"role":          "member",
		"clearance":     1,
		"registered_at": 1,
		// Note: no "registered_name" field — simulating pre-v5.2.0 data
	})
	require.NoError(t, err)
	err = app.badgerStore.SetRawForTest([]byte("agent:"+ak.id), legacyData)
	require.NoError(t, err)

	// GetRegisteredAgent should backfill RegisteredName from Name
	agent, err := app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "legacy-agent", agent.RegisteredName, "backfill should set RegisteredName = Name for legacy agents")

	// Now update the display name
	ptx := makeAgentUpdateTx(t, ak, ak.id, "renamed-agent", "new bio")
	r := app.processAgentUpdate(ptx, 2, time.Now())
	assert.Equal(t, uint32(0), r.Code)

	// RegisteredName should still be the backfilled value (from the Name at read time,
	// then preserved through the read-modify-write in UpdateAgentMeta)
	agent, err = app.badgerStore.GetRegisteredAgent(ak.id)
	require.NoError(t, err)
	assert.Equal(t, "renamed-agent", agent.Name)
	assert.Equal(t, "legacy-agent", agent.RegisteredName, "backfilled RegisteredName must survive updates")
}

// ---------------------------------------------------------------------------
// Agent set permission tests
// ---------------------------------------------------------------------------

func TestProcessAgentSetPermission(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	target := newAgentKey(t)

	// Register admin agent with role "admin"
	registerAgent(t, app, admin, "admin-agent", "admin")
	// Register target agent as regular member
	registerAgent(t, app, target, "target-agent", "member")

	// Admin sets permissions on target
	ptx := makeAgentSetPermissionTx(t, admin, target.id, 3, `["crypto","vuln_intel"]`, `["*"]`, "org-123", "dept-456")
	result := app.processAgentSetPermission(ptx, 3, time.Now())
	assert.Equal(t, uint32(0), result.Code, "set permission should succeed: %s", result.Log)

	// Verify on-chain state changed
	agent, err := app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	assert.Equal(t, uint8(3), agent.Clearance, "clearance should be updated to SECRET (3)")
	assert.Equal(t, `["crypto","vuln_intel"]`, agent.DomainAccess)
	assert.Equal(t, `["*"]`, agent.VisibleAgents)
	assert.Equal(t, "org-123", agent.OrgID)
	assert.Equal(t, "dept-456", agent.DeptID)

	// Verify pending write buffered (2 registers + 1 permission)
	require.Len(t, app.pendingWrites, 3)
	assert.Equal(t, "agent_permission", app.pendingWrites[2].writeType)
	perm, ok := app.pendingWrites[2].data.(*agentPermissionData)
	require.True(t, ok)
	assert.Equal(t, target.id, perm.AgentID)
	assert.Equal(t, 3, perm.Clearance)
}

// ---------------------------------------------------------------------------
// Memory reassign tests
// ---------------------------------------------------------------------------

// makeMemoryReassignTx builds a signed ParsedTx for TxTypeMemoryReassign.
func makeMemoryReassignTx(t *testing.T, sender agentKey, sourceID, targetID string) *tx.ParsedTx {
	t.Helper()
	body := []byte(sourceID + targetID)
	pubKey, sig, bodyHash, ts := signAgentProof(t, sender, body)
	return &tx.ParsedTx{
		Type: tx.TxTypeMemoryReassign,
		MemoryReassign: &tx.MemoryReassign{
			SourceAgentID: sourceID,
			TargetAgentID: targetID,
		},
		AgentPubKey:    pubKey,
		AgentSig:       sig,
		AgentBodyHash:  bodyHash,
		AgentTimestamp: ts,
	}
}

func TestProcessMemoryReassign(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	target := newAgentKey(t)
	orphanID := "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"

	// Register admin agent
	registerAgent(t, app, admin, "admin-agent", "admin")
	// Register target agent
	registerAgent(t, app, target, "target-agent", "member")

	// Admin reassigns memories from orphan to target
	ptx := makeMemoryReassignTx(t, admin, orphanID, target.id)
	result := app.processMemoryReassign(ptx, 3, time.Now())

	assert.Equal(t, uint32(0), result.Code, "reassign should succeed: %s", result.Log)
	assert.Equal(t, target.id, string(result.Data))
	assert.Contains(t, result.Log, "reassigned")

	// Verify pending write was buffered (2 registers + 1 reassign)
	require.Len(t, app.pendingWrites, 3)
	assert.Equal(t, "memory_reassign", app.pendingWrites[2].writeType)
	d, ok := app.pendingWrites[2].data.(*memoryReassignData)
	require.True(t, ok)
	assert.Equal(t, orphanID, d.SourceAgentID)
	assert.Equal(t, target.id, d.TargetAgentID)
}

func TestProcessMemoryReassignAdminOnly(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	member := newAgentKey(t)
	target := newAgentKey(t)
	orphanID := "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"

	// Register all agents
	registerAgent(t, app, admin, "admin-agent", "admin")
	registerAgent(t, app, member, "member-agent", "member")
	registerAgent(t, app, target, "target-agent", "member")

	// Non-admin tries to reassign — should fail
	ptx := makeMemoryReassignTx(t, member, orphanID, target.id)
	result := app.processMemoryReassign(ptx, 4, time.Now())
	assert.Equal(t, uint32(67), result.Code, "non-admin should fail with code 67")
	assert.Contains(t, result.Log, "not an admin")

	// Only 3 pending writes (3 registers, no reassign)
	assert.Len(t, app.pendingWrites, 3, "failed reassign should not buffer a write")
}

func TestProcessMemoryReassignTargetMustExist(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	orphanID := "deadbeef1234567890abcdef1234567890abcdef1234567890abcdef12345678"
	fakeTargetID := "aaaaaaa1234567890abcdef1234567890abcdef1234567890abcdef12345678"

	// Register admin
	registerAgent(t, app, admin, "admin-agent", "admin")

	// Try to reassign to a non-existent target
	ptx := makeMemoryReassignTx(t, admin, orphanID, fakeTargetID)
	result := app.processMemoryReassign(ptx, 2, time.Now())
	assert.Equal(t, uint32(68), result.Code, "unregistered target should fail with code 68")
	assert.Contains(t, result.Log, "not registered")
}

func TestProcessMemoryReassignSourceCanBeUnregistered(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	target := newAgentKey(t)
	// Source is a totally made-up ID that's never been registered — this should still work
	unregisteredSourceID := "bbbbbbbb1234567890abcdef1234567890abcdef1234567890abcdef12345678"

	registerAgent(t, app, admin, "admin-agent", "admin")
	registerAgent(t, app, target, "target-agent", "member")

	ptx := makeMemoryReassignTx(t, admin, unregisteredSourceID, target.id)
	result := app.processMemoryReassign(ptx, 3, time.Now())
	assert.Equal(t, uint32(0), result.Code, "unregistered source should be fine: %s", result.Log)
}

func TestProcessMemoryReassignMissingPayload(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	registerAgent(t, app, admin, "admin-agent", "admin")

	// Nil payload
	ptx := &tx.ParsedTx{
		Type:           tx.TxTypeMemoryReassign,
		MemoryReassign: nil,
	}
	result := app.processMemoryReassign(ptx, 2, time.Now())
	assert.Equal(t, uint32(66), result.Code, "nil payload should fail with code 66")
	assert.Contains(t, result.Log, "missing")
}

func TestProcessAgentSetPermissionAdminOnly(t *testing.T) {
	app := setupTestApp(t)
	member := newAgentKey(t)
	target := newAgentKey(t)

	// Register both as regular members (not admin)
	registerAgent(t, app, member, "member-agent", "member")
	registerAgent(t, app, target, "target-agent", "member")

	// Non-admin tries to set permissions
	ptx := makeAgentSetPermissionTx(t, member, target.id, 4, `["*"]`, `["*"]`, "", "")
	result := app.processAgentSetPermission(ptx, 3, time.Now())
	assert.Equal(t, uint32(67), result.Code, "non-admin should fail with code 67")
	assert.Contains(t, result.Log, "not an admin")

	// Verify target's permissions were not changed
	agent, err := app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	assert.Equal(t, uint8(1), agent.Clearance, "clearance should remain at default INTERNAL (1)")
}

// Regression for Level Up bug 1: setting visible_agents="*" (bare-string wildcard)
// must persist end-to-end — badger on-chain AND offchain pending write — so the
// REST query path can return seeAll=true.
func TestProcessAgentSetPermission_WildcardVisibleAgents_PersistsThroughStack(t *testing.T) {
	app := setupTestApp(t)
	admin := newAgentKey(t)
	target := newAgentKey(t)

	registerAgent(t, app, admin, "admin-agent", "admin")
	registerAgent(t, app, target, "target-agent", "member")

	// Bare-string "*" is the wildcard sentinel the read path recognises —
	// not a JSON array like ["*"] which would be treated as a literal agent-id list.
	ptx := makeAgentSetPermissionTx(t, admin, target.id, 1, "", "*", "", "")
	result := app.processAgentSetPermission(ptx, 2, time.Now())
	require.Equal(t, uint32(0), result.Code, "set permission should succeed: %s", result.Log)

	// Assert 1: on-chain (BadgerDB) carries the bare wildcard verbatim.
	onChain, err := app.badgerStore.GetRegisteredAgent(target.id)
	require.NoError(t, err)
	assert.Equal(t, "*", onChain.VisibleAgents, "BadgerDB must store the bare-string wildcard")

	// Assert 2: the offchain pending write carries it too, so flushPendingWrites
	// will sync it to SQLite/Postgres for dashboard read paths.
	var permWrite *agentPermissionData
	for _, pw := range app.pendingWrites {
		if d, ok := pw.data.(*agentPermissionData); ok && d.AgentID == target.id {
			permWrite = d
			break
		}
	}
	require.NotNil(t, permWrite, "expected agent_permission pending write for target")
	assert.Equal(t, "*", permWrite.VisibleAgents, "pending SQLite write must carry the wildcard")
}
