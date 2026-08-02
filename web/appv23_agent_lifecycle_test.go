package web

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/governance"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAppV23RemoveCommitsDeactivationAndGroupCleanupBeforeSQLProjection(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.MutateAppV23AccessGroup(
		fixture.rootID, "team", "Team", []string{fixture.agentID}, 0, false, 2,
	))
	require.NoError(t, fixture.badger.RegisterDomain("removal-shared", fixture.agentID, "", 2))
	require.NoError(t, fixture.badger.SetSharedDomain("removal-shared"))
	require.NoError(t, fixture.badger.SetAccessGrant(
		"removal-shared", fixture.rootID, 2, 0, fixture.agentID,
	))
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	for _, agent := range []*store.AgentEntry{
		{AgentID: fixture.rootID, Name: "CEREBRUM", Role: "admin", Status: "active", Clearance: 4},
		{AgentID: fixture.agentID, Name: "Companion", Role: "member", Status: "active", Clearance: 1},
	} {
		require.NoError(t, sqlStore.CreateAgent(context.Background(), agent))
	}

	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		approval := captured.LocalAgentApprove
		require.NotNil(t, approval)
		require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(
			store.AppV23LocalEnrollment{
				AgentID: approval.AgentID, ApprovedBy: hex.EncodeToString(captured.AgentPubKey),
				RootGeneration: 1, Profile: approval.Profile, HomeDomain: approval.HomeDomain,
				Clearance: approval.Clearance, Capabilities: store.AgentCapabilities(approval.Capabilities),
				Active: approval.Active, UpdatedHeight: 3,
				RetireOwnedDomainsToRoot: !approval.Active,
			},
			approval.Role, approval.ExpectedRevision, approval.ExpectedRoleRevision,
		))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"REMOVE","height":"3"}}`)
	}))
	defer rpc.Close()

	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	h.store = sqlStore
	req := appV23AccessRequest(
		t, http.MethodDelete, "/agents/"+fixture.agentID+"?force=true",
		"id", fixture.agentID, nil,
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleRemoveAgent(sqlStore).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, captured)
	assert.Equal(t, tx.TxTypeLocalAgentApprove, captured.Type)
	assert.False(t, captured.LocalAgentApprove.Active)
	enrollment, err := fixture.badger.GetAppV23Enrollment(fixture.agentID)
	require.NoError(t, err)
	require.NotNil(t, enrollment)
	assert.False(t, enrollment.Active)
	groups, err := fixture.badger.ListAppV23AgentGroups(fixture.agentID)
	require.NoError(t, err)
	assert.Empty(t, groups)
	root, err := fixture.badger.GetAppV23Root()
	require.NoError(t, err)
	owner, err := fixture.badger.GetDomainOwner("removal-shared")
	require.NoError(t, err)
	assert.Equal(t, root.PrincipalID, owner)
	level, _, granter, err := fixture.badger.GetAccessGrant("removal-shared", fixture.rootID)
	require.NoError(t, err)
	assert.Equal(t, uint8(2), level)
	assert.Equal(t, fixture.agentID, granter, "agent removal must not rewrite unrelated grant history")
	history, err := fixture.badger.ListAppV26DomainOwnershipHistory("removal-shared")
	require.NoError(t, err)
	require.Len(t, history, 1)
	assert.Equal(t, fixture.agentID, history[0].PreviousOwner)
	assert.Equal(t, root.PrincipalID, history[0].NewOwner)
	projected, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	assert.Equal(t, "removed", projected.Status)
}

func TestAppV23RemoveDirectoryOnlyAgentSettlesAndIsIdempotent(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	_, localKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	directoryOnlyID := agentIDForKey(localKey)
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: directoryOnlyID, Name: "Directory-only", Role: "member", Status: "active", Clearance: 1,
	}))

	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.store = sqlStore
	remove := func() *httptest.ResponseRecorder {
		req := appV23AccessRequest(t, http.MethodDelete, "/agents/"+directoryOnlyID+"?force=true", "id", directoryOnlyID, nil)
		req = appV23AccessAs(req, fixture.rootID)
		rec := httptest.NewRecorder()
		h.handleRemoveAgent(sqlStore).ServeHTTP(rec, req)
		return rec
	}

	first := remove()
	require.Equal(t, http.StatusOK, first.Code, first.Body.String())
	assert.Contains(t, first.Body.String(), `"local_only":true`)
	projected, err := sqlStore.GetAgent(context.Background(), directoryOnlyID)
	require.NoError(t, err)
	assert.Equal(t, "removed", projected.Status)

	second := remove()
	require.Equal(t, http.StatusOK, second.Code, second.Body.String())
	assert.Contains(t, second.Body.String(), `"already_removed":true`)
}

func TestAppV26RemoveDoesNotTrustStaleRemovedProjectionOverActiveConsensus(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Stale projection", Role: "member",
		Status: "removed", Clearance: 1,
	}))

	var calls int
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		parsed, decodeErr := tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		require.NotNil(t, parsed.LocalAgentApprove)
		approval := parsed.LocalAgentApprove
		require.False(t, approval.Active)
		require.NoError(t, fixture.badger.ApproveAppV23LocalAgent(
			store.AppV23LocalEnrollment{
				AgentID: approval.AgentID, ApprovedBy: fixture.rootID, RootGeneration: 1,
				Profile: approval.Profile, HomeDomain: approval.HomeDomain,
				Clearance: approval.Clearance, Capabilities: store.AgentCapabilities(approval.Capabilities),
				Active: false, UpdatedHeight: 3, RetireOwnedDomainsToRoot: true,
			}, approval.Role, approval.ExpectedRevision, approval.ExpectedRoleRevision,
		))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"REMOVE","height":"3"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	h.store = sqlStore
	req := appV23AccessRequest(t, http.MethodDelete, "/agents/"+fixture.agentID+"?force=true", "id", fixture.agentID, nil)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleRemoveAgent(sqlStore).ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Equal(t, 1, calls, "active consensus authority must be deactivated despite a stale local removed row")
	enrollment, err := fixture.badger.GetAppV23Enrollment(fixture.agentID)
	require.NoError(t, err)
	require.False(t, enrollment.Active)
}

func TestAppV23NonRootKeyRotationFailsClosedWithoutSQLMutation(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	agent := &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Companion", Role: "member",
		Status: "active", Clearance: 1,
	}
	require.NoError(t, sqlStore.CreateAgent(context.Background(), agent))
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	req := appV23AccessRequest(
		t, http.MethodPost, "/agents/"+fixture.agentID+"/rotate-key",
		"id", fixture.agentID, nil,
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleRotateAgentKey(sqlStore).ServeHTTP(rec, req)

	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "agent_key_rotation_requires_reenrollment")
	projected, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	assert.Equal(t, fixture.agentID, projected.AgentID)
}

func TestAppV23LegacyAgentPolicyRouteIsGoneButDisplayMetadataStaysLocal(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Companion", Role: "member",
		Status: "active", Clearance: 1,
	}))

	rpcCalls := 0
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		rpcCalls++
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"UNEXPECTED","height":"3"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)

	policyReq := appV23AccessRequest(
		t, http.MethodPut, "/agents/"+fixture.agentID,
		"id", fixture.agentID, map[string]any{"clearance": 4, "name": "Must Not Persist"},
	)
	policyReq = appV23AccessAs(policyReq, fixture.rootID)
	policyRec := httptest.NewRecorder()
	h.handleUpdateAgent(sqlStore).ServeHTTP(policyRec, policyReq)
	require.Equal(t, http.StatusGone, policyRec.Code, policyRec.Body.String())
	assert.Contains(t, policyRec.Body.String(), "legacy_permission_route_retired")
	unchanged, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	assert.Equal(t, "Companion", unchanged.Name)
	assert.Equal(t, 1, unchanged.Clearance)

	memberMetadataReq := appV23AccessRequest(
		t, http.MethodPut, "/agents/"+fixture.agentID,
		"id", fixture.agentID, map[string]any{"name": "Self Renamed"},
	)
	memberMetadataReq = appV23AccessAs(memberMetadataReq, fixture.agentID)
	memberMetadataRec := httptest.NewRecorder()
	h.handleUpdateAgent(sqlStore).ServeHTTP(memberMetadataRec, memberMetadataReq)
	require.Equal(t, http.StatusForbidden, memberMetadataRec.Code, memberMetadataRec.Body.String())

	metadataReq := appV23AccessRequest(
		t, http.MethodPut, "/agents/"+fixture.agentID,
		"id", fixture.agentID, map[string]any{"name": "Local Companion", "boot_bio": "local label"},
	)
	metadataReq = appV23AccessAs(metadataReq, fixture.rootID)
	metadataRec := httptest.NewRecorder()
	h.handleUpdateAgent(sqlStore).ServeHTTP(metadataRec, metadataReq)
	require.Equal(t, http.StatusOK, metadataRec.Code, metadataRec.Body.String())
	assert.Contains(t, metadataRec.Body.String(), `"metadata_scope":"local"`)
	updated, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	assert.Equal(t, "Local Companion", updated.Name)
	assert.Equal(t, "local label", updated.BootBio)
	assert.Zero(t, rpcCalls, "post-v23 display metadata must not use a stale validator or Root key")
}

func TestAppV26LegacyAgentMetadataCannotBypassGovernedRename(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Consensus Name", BootBio: "immutable purpose",
		Role: "member", Status: "active", Clearance: 1,
	}))

	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.AppV26ActiveFn = func() bool { return true }
	req := appV23AccessRequest(
		t, http.MethodPatch, "/agents/"+fixture.agentID,
		"id", fixture.agentID, map[string]any{
			"name": "Local Impostor", "boot_bio": "rewritten purpose",
		},
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleUpdateAgent(sqlStore).ServeHTTP(rec, req)

	require.Equal(t, http.StatusGone, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "governed_agent_metadata_required")
	unchanged, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	assert.Equal(t, "Consensus Name", unchanged.Name)
	assert.Equal(t, "immutable purpose", unchanged.BootBio)

	// Non-identity presentation metadata remains a local dashboard concern.
	avatarReq := appV23AccessRequest(
		t, http.MethodPatch, "/agents/"+fixture.agentID,
		"id", fixture.agentID, map[string]any{"avatar": "robot"},
	)
	avatarReq = appV23AccessAs(avatarReq, fixture.rootID)
	avatarRec := httptest.NewRecorder()
	h.handleUpdateAgent(sqlStore).ServeHTTP(avatarRec, avatarReq)
	require.Equal(t, http.StatusOK, avatarRec.Code, avatarRec.Body.String())
	updated, err := sqlStore.GetAgent(context.Background(), fixture.agentID)
	require.NoError(t, err)
	assert.Equal(t, "robot", updated.Avatar)
}

func TestAppV23MergeRejectsBeforeLocalMemoryMutation(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.agentID, Name: "Companion", Role: "member",
		Status: "active", Clearance: 1,
	}))
	_, sourceKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	sourceID := agentIDForKey(sourceKey)
	insertTestMemoryWithAgent(t, sqlStore, "orphan-memory", "companion-home", sourceID)
	publishAppV23DashboardRecord(
		t, sqlStore, fixture.badger, "orphan-memory",
		uint8(store.ClearanceInternal), true,
	)

	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":110,"log":"access denied"},"hash":"DENIED","height":"3"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	h.store = sqlStore
	req := appV23AccessRequest(
		t, http.MethodPost, "/agents/merge", "", "", map[string]any{
			"source_agent_id": sourceID,
			"target_agent_id": fixture.agentID,
		},
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleMergeAgent(sqlStore).ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "memory_reassign_retired")
	require.Nil(t, captured, "retired mutation must not reach consensus RPC")

	memoryAfter, err := sqlStore.GetMemory(context.Background(), "orphan-memory")
	require.NoError(t, err)
	assert.Equal(t, sourceID, memoryAfter.SubmittingAgent)
	canonicalAuthor, err := fixture.badger.GetMemoryAuthor("orphan-memory")
	require.NoError(t, err)
	assert.Equal(t, sourceID, canonicalAuthor)
	_, err = fixture.badger.ValidateMemoryProjection(memoryAfter)
	require.NoError(t, err, "rejected reassignment must leave CEREBRUM disclosure valid")
}

func TestAppV23RootRotationUsesTx39AndStoresRecoveryBundle(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	require.NoError(t, os.WriteFile(filepath.Join(home, "agent.key"), fixture.rootKey.Seed(), 0600))

	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		require.NotNil(t, captured.RootCredentialRotate)
		require.NoError(t, fixture.badger.RotateAppV23RootCredential(
			captured.RootCredentialRotate.ExpectedGeneration,
			captured.RootCredentialRotate.NewCredentialID,
			3,
		))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"ROTATE","height":"3"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	baseResolver := h.ResolveAgentKeyFn
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if key, ok := baseResolver(id); ok {
			return key, true
		}
		raw, readErr := os.ReadFile(filepath.Join(sageHome(), "bundles", id, "agent.key"))
		if readErr != nil || len(raw) != ed25519.SeedSize {
			return nil, false
		}
		return ed25519.NewKeyFromSeed(raw), true
	}
	req := appV23AccessRequest(
		t, http.MethodPost, "/v1/dashboard/network/access/root/handover",
		"", "", map[string]any{
			"confirm_irreversible": true,
			"confirmation_phrase":  appV23RootHandoverPhrase,
			"expected_generation":  1,
		},
	)
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleAppV23RootCredentialHandover().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	assert.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
	require.NotNil(t, captured)
	require.NotNil(t, captured.RootCredentialRotate)
	assert.Equal(t, tx.TxTypeRootCredentialRotate, captured.Type)
	assert.Nil(t, captured.LocalElevation)
	rotation := captured.RootCredentialRotate
	newPublic, err := hex.DecodeString(rotation.NewCredentialID)
	require.NoError(t, err)
	assert.True(t, ed25519.Verify(
		ed25519.PublicKey(newPublic),
		tx.RootCredentialRotationSignBytes(fixture.rootID, rotation),
		rotation.NewCredentialSignature,
	))
	root, err := fixture.badger.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, root)
	assert.Equal(t, uint64(2), root.Generation)
	assert.Equal(t, rotation.NewCredentialID, root.CredentialID)
	seed, err := os.ReadFile(filepath.Join(sageHome(), "bundles", root.CredentialID, "agent.key"))
	require.NoError(t, err)
	assert.Len(t, seed, ed25519.SeedSize)
	oldSeed, err := os.ReadFile(filepath.Join(home, "agent.key"))
	require.NoError(t, err)
	assert.Equal(t, fixture.rootKey.Seed(), oldSeed)
	var response struct {
		RecoveryBundle string `json:"recovery_bundle"`
		BundleURL      string `json:"bundle_url"`
		Message        string `json:"message"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	assert.NotEmpty(t, response.RecoveryBundle)
	assert.Empty(t, response.BundleURL)
	assert.Contains(t, response.Message, "were not moved or copied")
	assert.Contains(t, response.Message, "historical authorship is unchanged")
	archive, err := base64.StdEncoding.DecodeString(response.RecoveryBundle)
	require.NoError(t, err)
	assert.NotEmpty(t, archive)
}

func TestAppV23EveryRootCredentialGenerationRemainsHidden(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, generationTwoKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	generationTwoID := agentIDForKey(generationTwoKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, generationTwoID, 2))
	_, generationThreeKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	generationThreeID := agentIDForKey(generationThreeKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(2, generationThreeID, 3))

	h := appV23AccessTestHandler(fixture, "http://unused.invalid", map[string]ed25519.PrivateKey{
		generationTwoID:   generationTwoKey,
		generationThreeID: generationThreeKey,
	})
	for _, id := range []string{fixture.rootID, generationTwoID, generationThreeID} {
		assert.True(t, h.appV23IsRootIdentity(id), "Root generation %s must stay out of agent surfaces", id)
	}
	assert.False(t, h.appV23IsRootIdentity(fixture.agentID))
	_, err = canonicalAppV23GroupMembers(
		fixture.badger, mustAppV23Root(t, fixture.badger), []string{generationTwoID},
	)
	require.ErrorContains(t, err, "CEREBRUM Root cannot be placed")

	retiredPolicyReq := appV23AccessRequest(
		t, http.MethodPut, "/v1/dashboard/network/access/agents/"+generationTwoID+"/policy",
		"id", generationTwoID, map[string]any{
			"role": "member", "profile": "standard", "clearance": 1,
			"capabilities": 0, "expected_enrollment_revision": 0,
			"expected_role_revision": 0,
		},
	)
	retiredPolicyReq = appV23AccessAs(retiredPolicyReq, generationThreeID)
	retiredPolicyRec := httptest.NewRecorder()
	h.handleAppV23AgentPolicy().ServeHTTP(retiredPolicyRec, retiredPolicyReq)
	require.Equal(t, http.StatusForbidden, retiredPolicyRec.Code, retiredPolicyRec.Body.String())
	assert.Contains(t, retiredPolicyRec.Body.String(), "root_policy_immutable")
}

func TestAppV23GraphLabelsRootAuthorshipWithoutCreatingAnAgent(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	insertTestMemoryWithAgent(
		t, sqlStore, "root-authored-memory", "root-history", fixture.rootID,
	)
	publishAppV23DashboardRecord(
		t, sqlStore, fixture.badger, "root-authored-memory",
		uint8(store.ClearanceInternal), true,
	)
	h := appV23AccessTestHandler(fixture, "", nil)
	h.store = sqlStore
	body, err := h.computeGraphJSON(
		context.Background(), "proposed", "", 10, true, nil,
	)
	require.NoError(t, err)
	var graph struct {
		Nodes []struct {
			Agent       string `json:"agent"`
			AgentLabel  string `json:"agent_label"`
			AgentIsRoot bool   `json:"agent_is_root"`
		} `json:"nodes"`
	}
	require.NoError(t, json.Unmarshal(body, &graph))
	require.Len(t, graph.Nodes, 1)
	assert.Equal(t, fixture.rootID, graph.Nodes[0].Agent, "ledger signer attribution remains immutable")
	assert.Equal(t, "CEREBRUM Root", graph.Nodes[0].AgentLabel)
	assert.True(t, graph.Nodes[0].AgentIsRoot)
}

func mustAppV23Root(t *testing.T, badgerStore *store.BadgerStore) *store.AppV23RootState {
	t.Helper()
	root, err := badgerStore.GetAppV23Root()
	require.NoError(t, err)
	require.NotNil(t, root)
	return root
}

func TestAppV23RootHandoverFailsBeforeBroadcastWhenCeremonyOrResolverIsInvalid(t *testing.T) {
	tests := []struct {
		name       string
		body       map[string]any
		wantStatus int
		wantCode   string
	}{
		{
			name: "missing second-stage phrase",
			body: map[string]any{
				"confirm_irreversible": true,
				"confirmation_phrase":  "ROTATE ROOT",
				"expected_generation":  1,
			},
			wantStatus: http.StatusBadRequest,
			wantCode:   "root_handover_confirmation_required",
		},
		{
			name: "stale ceremony generation",
			body: map[string]any{
				"confirm_irreversible": true,
				"confirmation_phrase":  appV23RootHandoverPhrase,
				"expected_generation":  99,
			},
			wantStatus: http.StatusConflict,
			wantCode:   "stale_root_generation",
		},
		{
			name: "new key not visible to production resolver",
			body: map[string]any{
				"confirm_irreversible": true,
				"confirmation_phrase":  appV23RootHandoverPhrase,
				"expected_generation":  1,
			},
			wantStatus: http.StatusServiceUnavailable,
			wantCode:   "root_rotation_key_unresolvable",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fixture := newAppV23AccessFixture(t)
			t.Setenv("SAGE_HOME", t.TempDir())
			broadcasts := 0
			rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				broadcasts++
				t.Fatal("handover must not broadcast before every preflight succeeds")
			}))
			defer rpc.Close()
			h := appV23AccessTestHandler(fixture, rpc.URL, nil)
			// This resolver can drive the current Root but deliberately cannot
			// see a newly generated recovery key.
			h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
				if id == fixture.rootID {
					return fixture.rootKey, true
				}
				return nil, false
			}
			req := appV23AccessRequest(
				t, http.MethodPost, "/v1/dashboard/network/access/root/handover",
				"", "", tt.body,
			)
			req = appV23AccessAs(req, fixture.rootID)
			rec := httptest.NewRecorder()
			h.handleAppV23RootCredentialHandover().ServeHTTP(rec, req)
			require.Equal(t, tt.wantStatus, rec.Code, rec.Body.String())
			assert.Contains(t, rec.Body.String(), tt.wantCode)
			assert.Zero(t, broadcasts)
			root, err := fixture.badger.GetAppV23Root()
			require.NoError(t, err)
			require.NotNil(t, root)
			assert.Equal(t, fixture.rootID, root.CredentialID)
			assert.Equal(t, uint64(1), root.Generation)
		})
	}
}

func TestAppV23RootIsHiddenFromGenericBundlePairingAndLegacyClaim(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	home := t.TempDir()
	t.Setenv("SAGE_HOME", home)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	token, err := generateClaimToken()
	require.NoError(t, err)
	expiry := time.Now().Add(time.Hour)
	rootBundleDir := filepath.Join(home, "bundles", fixture.rootID)
	require.NoError(t, os.MkdirAll(rootBundleDir, 0700))
	require.NoError(t, os.WriteFile(
		filepath.Join(rootBundleDir, "agent.key"), fixture.rootKey.Seed(), 0600,
	))
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: fixture.rootID, Name: "legacy CEREBRUM row", Role: "admin",
		Status: "active", Clearance: 4, BundlePath: filepath.Join(rootBundleDir, "root.zip"),
		ClaimToken: token, ClaimExpiresAt: &expiry,
	}))
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)

	listRec := httptest.NewRecorder()
	h.handleListAgents(sqlStore).ServeHTTP(
		listRec, httptest.NewRequest(http.MethodGet, "/v1/dashboard/network/agents", nil),
	)
	require.Equal(t, http.StatusOK, listRec.Code, listRec.Body.String())
	assert.NotContains(t, listRec.Body.String(), fixture.rootID)

	bundleReq := appV23AccessRequest(
		t, http.MethodGet, "/v1/dashboard/network/agents/"+fixture.rootID+"/bundle",
		"id", fixture.rootID, nil,
	)
	bundleRec := httptest.NewRecorder()
	h.handleDownloadBundle(sqlStore).ServeHTTP(bundleRec, bundleReq)
	assert.Equal(t, http.StatusNotFound, bundleRec.Code, bundleRec.Body.String())

	pairing := NewPairingStore()
	pairCreateReq := appV23AccessRequest(
		t, http.MethodPost, "/v1/dashboard/network/agents/"+fixture.rootID+"/pair",
		"id", fixture.rootID, nil,
	)
	pairCreateRec := httptest.NewRecorder()
	handleCreatePairingCode(sqlStore, pairing, h.appV23IsRootIdentity).
		ServeHTTP(pairCreateRec, pairCreateReq)
	assert.Equal(t, http.StatusNotFound, pairCreateRec.Code, pairCreateRec.Body.String())

	legacyPair, err := pairing.Generate(fixture.rootID)
	require.NoError(t, err)
	redeemReq := appV23AccessRequest(
		t, http.MethodGet, "/v1/dashboard/network/pair/"+legacyPair.Code,
		"code", legacyPair.Code, nil,
	)
	redeemReq.RemoteAddr = "127.0.0.1:10101"
	redeemRec := httptest.NewRecorder()
	handleRedeemPairingCode(
		sqlStore, pairing, &redeemRateLimiter{}, h.appV23IsRootIdentity,
	).ServeHTTP(redeemRec, redeemReq)
	assert.Equal(t, http.StatusNotFound, redeemRec.Code, redeemRec.Body.String())
	_, stillLive := pairing.Consume(legacyPair.Code)
	assert.False(t, stillLive, "pre-v23 Root pairing code must be burned")

	claimReq := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/network/claim",
		strings.NewReader(fmt.Sprintf(`{"token":%q}`, token)),
	)
	claimReq.RemoteAddr = "127.0.0.1:20202"
	claimRec := httptest.NewRecorder()
	handleClaimAgent(
		sqlStore, sqlStore, &redeemRateLimiter{}, h.appV23IsRootIdentity,
	).ServeHTTP(claimRec, claimReq)
	assert.Equal(t, http.StatusNotFound, claimRec.Code, claimRec.Body.String())
	assert.NotContains(t, claimRec.Body.String(), base64.StdEncoding.EncodeToString(fixture.rootKey.Seed()))
	_, err = sqlStore.RedeemAgentClaim(context.Background(), token, time.Now())
	assert.ErrorIs(t, err, store.ErrAgentClaimInvalid, "pre-v23 Root claim must be burned")
}

type appV23RedeploySpy struct {
	quickCalls  int
	deployCalls int
}

func (*appV23RedeploySpy) IsRedeploying() bool { return false }
func (s *appV23RedeploySpy) DeployOp(context.Context, string, string) error {
	s.deployCalls++
	return nil
}
func (*appV23RedeploySpy) GetRedeployStatus(context.Context) (bool, string, string, error) {
	return false, "", "", nil
}
func (*appV23RedeploySpy) GetLiveStatus(context.Context) (string, string, string, string, string, error) {
	return "idle", "", "", "", "", nil
}
func (*appV23RedeploySpy) ClearStale(context.Context) (int, error) { return 0, nil }
func (s *appV23RedeploySpy) QuickAgentOp(context.Context, string, string) error {
	s.quickCalls++
	return nil
}

func TestAppV23RootCannotBeTargetedByGenericRedeployment(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	redeployer := &appV23RedeploySpy{}
	h.Redeployer = redeployer
	req := httptest.NewRequest(
		http.MethodPost,
		"/v1/dashboard/network/redeploy",
		strings.NewReader(fmt.Sprintf(
			`{"operation":"remove_agent","agent_id":%q}`, fixture.rootID,
		)),
	)
	rec := httptest.NewRecorder()
	h.handleTriggerRedeploy(rec, req)
	require.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())
	assert.Contains(t, rec.Body.String(), "root_agent_surface_forbidden")
	assert.Zero(t, redeployer.quickCalls)
	assert.Zero(t, redeployer.deployCalls)
}

func TestAppV23RootSessionAuthorityRejectsCrossOriginAndLANBrowsers(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.Encrypted.Store(true)
	h.sessions.Store("root-session", time.Now().Add(time.Hour))

	request := func(remote, host, origin, secFetch string) *http.Request {
		req := httptest.NewRequest(http.MethodPost, "/v1/dashboard/network/access/root/handover", nil)
		req.RemoteAddr = remote
		req.Host = host
		req.Header.Set("Origin", origin)
		req.Header.Set("Sec-Fetch-Site", secFetch)
		req.AddCookie(&http.Cookie{Name: sessionCookieName, Value: "root-session"})
		return req
	}
	assert.True(t, h.appV23SessionHasRootAuthority(request(
		"127.0.0.1:1234", "localhost:8080", "http://localhost:8080", "same-origin",
	)))
	assert.False(t, h.appV23SessionHasRootAuthority(request(
		"127.0.0.1:1234", "localhost:8080", "https://attacker.example", "cross-site",
	)))
	assert.False(t, h.appV23SessionHasRootAuthority(request(
		"127.0.0.1:1234", "localhost:8080", "https://attacker.example", "same-origin",
	)))
	assert.False(t, h.appV23SessionHasRootAuthority(request(
		"192.168.1.20:1234", "192.168.1.10:8080", "http://192.168.1.10:8080", "same-origin",
	)))
}

func TestAppV23DashboardTaskUsesRotatedRootCredentialOnExistingRootDomain(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	require.NoError(t, fixture.badger.RegisterDomain("root-home", fixture.rootID, "", 1))
	_, rotatedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rotatedID := agentIDForKey(rotatedKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, rotatedID, 2))

	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		require.Equal(t, tx.TxTypeMemorySubmit, captured.Type)
		require.NoError(t, fixture.badger.SetMemoryAuthor(captured.MemorySubmit.MemoryID, rotatedID))
		require.NoError(t, fixture.badger.SetMemoryAuthorPrincipal(captured.MemorySubmit.MemoryID, fixture.rootID))
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"TASK","height":"3"}}`)
	}))
	defer rpc.Close()

	h := appV23AccessTestHandler(
		fixture, rpc.URL, map[string]ed25519.PrivateKey{rotatedID: rotatedKey},
	)
	h.store = sqlStore
	h.SigningKey = fixture.rootKey // deliberately stale: must never author this task
	h.AdminSigningKey = fixture.rootKey
	req := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/tasks",
		strings.NewReader(`{"content":"radio reminder","domain":"root-home"}`),
	)
	req.Header.Set("Content-Type", "application/json")
	req = appV23AccessAs(req, rotatedID)
	rec := httptest.NewRecorder()
	h.handleCreateTaskDashboard(rec, req)
	require.Equal(t, http.StatusCreated, rec.Code, rec.Body.String())
	require.NotNil(t, captured)
	assert.Equal(t, rotatedID, hex.EncodeToString(captured.AgentPubKey))
	assert.Equal(t, "root-home", captured.MemorySubmit.DomainTag)
	assert.Nil(t, captured.LocalElevation, "current Root signs directly")
	author, err := fixture.badger.GetMemoryAuthor(captured.MemorySubmit.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, rotatedID, author)
	principal, err := fixture.badger.GetMemoryAuthorPrincipal(captured.MemorySubmit.MemoryID)
	require.NoError(t, err)
	assert.Equal(t, fixture.rootID, principal)
	allowed, definitive := h.agentDomainReadDecision(context.Background(), rotatedID, "root-home")
	assert.True(t, definitive)
	assert.True(t, allowed, "rotated Root must retain access to the existing Root domain")

	staleReq := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/tasks",
		strings.NewReader(`{"content":"stale root attempt","domain":"root-home"}`),
	)
	staleReq.Header.Set("Content-Type", "application/json")
	staleReq = appV23AccessAs(staleReq, fixture.rootID)
	staleRec := httptest.NewRecorder()
	h.handleCreateTaskDashboard(staleRec, staleReq)
	assert.Equal(t, http.StatusForbidden, staleRec.Code, staleRec.Body.String())
	assert.Contains(t, staleRec.Body.String(), "stale_root_credential")
}

func TestAppV23RootCannotBeTargetedByTaskPipelineOrDomainReassignment(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	insertTestTask(t, sqlStore, "root-target-task", "companion-home", fixture.agentID)
	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.store = sqlStore
	h.SigningKey = fixture.agentKey
	h.ValidatorCountFn = func() int { return 1 }

	assignReq := appV23AccessRequest(
		t, http.MethodPut, "/v1/dashboard/tasks/root-target-task/assign",
		"id", "root-target-task", map[string]any{"assignee": fixture.rootID},
	)
	assignReq = appV23AccessAs(assignReq, fixture.rootID)
	assignRec := httptest.NewRecorder()
	h.handleAssignTask(assignRec, assignReq)
	require.Equal(t, http.StatusForbidden, assignRec.Code, assignRec.Body.String())
	assert.Contains(t, assignRec.Body.String(), "root_agent_surface_forbidden")

	pipelineReq := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/pipeline/send",
		strings.NewReader(fmt.Sprintf(
			`{"to_agent":%q,"intent":"note","payload":"do work"}`, fixture.rootID,
		)),
	)
	pipelineRec := httptest.NewRecorder()
	h.handlePipelineSend(pipelineRec, pipelineReq)
	require.Equal(t, http.StatusForbidden, pipelineRec.Code, pipelineRec.Body.String())
	assert.Contains(t, pipelineRec.Body.String(), "root_agent_surface_forbidden")

	reassignReq := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/network/reassign-domain-ownership",
		strings.NewReader(fmt.Sprintf(
			`{"source_agent_id":%q,"target_agent_id":%q,"domain":"companion-home"}`,
			fixture.agentID, fixture.rootID,
		)),
	)
	reassignReq = appV23AccessAs(reassignReq, fixture.rootID)
	reassignRec := httptest.NewRecorder()
	h.handleReassignDomainOwnership(sqlStore).ServeHTTP(reassignRec, reassignReq)
	require.Equal(t, http.StatusForbidden, reassignRec.Code, reassignRec.Body.String())
	assert.Contains(t, reassignRec.Body.String(), "root_agent_surface_forbidden")
}

func TestAppV26RootCanTransferCurrentlyRootOwnedDomainToActiveLocalAgent(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetID := fixture.agentID
	require.NoError(t, fixture.badger.RegisterDomain("root-recovery-domain", fixture.rootID, "", 2))

	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: targetID, Name: "Recovery target", Role: "member", Status: "active",
	}))

	var captured []*tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		parsed, decodeErr := tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		captured = append(captured, parsed)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0,"log":"purged 0 grants"},"hash":"ROOTREASSIGN","height":"42"}}`)
	}))
	t.Cleanup(rpc.Close)

	h := appV23AccessTestHandler(fixture, rpc.URL, nil)
	// The target's private key is intentionally unavailable to CEREBRUM.
	// app-v26 ownership itself must make the transfer immediately usable.
	h.ResolveAgentKeyFn = func(id string) (ed25519.PrivateKey, bool) {
		if id == fixture.rootID {
			return fixture.rootKey, true
		}
		return nil, false
	}
	h.store = sqlStore
	h.SigningKey = validatorKey
	h.AppV20ActiveFn = func() bool { return true }
	h.AppV26ActiveFn = func() bool { return true }
	h.GovernanceDomainFn = func() string { return dashboardTestGovernanceDomain }
	h.ValidatorCountFn = func() int { return 1 }
	req := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/network/reassign-domain-ownership",
		strings.NewReader(fmt.Sprintf(
			`{"source_agent_id":%q,"target_agent_id":%q,"domain":"root-recovery-domain"}`,
			fixture.rootID, targetID,
		)),
	)
	req.Header.Set("Content-Type", "application/json")
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleReassignDomainOwnership(sqlStore).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var response map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &response))
	require.Equal(t, "ok", response["status"])
	require.Equal(t, false, response["grant_deferred"])
	require.Equal(t, "ownership", response["owner_access"])
	require.Len(t, captured, 2, "app-v26 must not emit a redundant owner self-grant")
	require.Equal(t, tx.TxTypeDomainReassign, captured[1].Type)
	require.Equal(t, "root-recovery-domain", captured[1].DomainReassign.Domain)
	require.Equal(t, targetID, captured[1].DomainReassign.NewOwnerID)
	require.Equal(t, fixture.rootID, captured[1].DomainReassign.ExpectedOwnerID)
	for _, parsed := range captured {
		require.NotEqual(t, tx.TxTypeAccessGrant, parsed.Type)
	}
}

func TestAppV26DomainTransferRejectsStaleOrGuessedSourceOwner(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetID := agentIDForKey(targetKey)

	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	require.NoError(t, sqlStore.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID: targetID, Name: "Target", Role: "member", Status: "active",
	}))

	h := appV23AccessTestHandler(fixture, "http://unused.invalid", nil)
	h.store = sqlStore
	h.SigningKey = fixture.agentKey
	h.ValidatorCountFn = func() int { return 1 }
	req := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/network/reassign-domain-ownership",
		strings.NewReader(fmt.Sprintf(
			`{"source_agent_id":%q,"target_agent_id":%q,"domain":"companion-home"}`,
			strings.Repeat("ab", 32), targetID,
		)),
	)
	req.Header.Set("Content-Type", "application/json")
	req = appV23AccessAs(req, fixture.rootID)
	rec := httptest.NewRecorder()
	h.handleReassignDomainOwnership(sqlStore).ServeHTTP(rec, req)
	require.Equal(t, http.StatusConflict, rec.Code, rec.Body.String())
	require.Contains(t, rec.Body.String(), "owner_changed")
}

func TestAppV23DomainReassignmentUsesCurrentRotatedRootCredential(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, rotatedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rotatedID := agentIDForKey(rotatedKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, rotatedID, 2))
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	_, targetKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	targetID := agentIDForKey(targetKey)
	require.NoError(t, fixture.badger.RegisterDomain(
		"source-work", fixture.agentID, "", 2,
	))

	sqlStore, err := store.NewSQLiteStore(context.Background(), filepath.Join(t.TempDir(), "sage.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, sqlStore.Close()) })
	for _, agent := range []*store.AgentEntry{
		{AgentID: fixture.agentID, Name: "Source", Role: "member", Status: "active"},
		{AgentID: targetID, Name: "Target", Role: "member", Status: "active"},
	} {
		require.NoError(t, sqlStore.CreateAgent(context.Background(), agent))
	}

	var captured []*tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		parsed, decodeErr := tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		captured = append(captured, parsed)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0,"log":"purged 0 grants"},"hash":"REASSIGN","height":"42"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(
		fixture, rpc.URL, map[string]ed25519.PrivateKey{
			rotatedID: rotatedKey,
			targetID:  targetKey,
		},
	)
	h.store = sqlStore
	h.AdminSigningKey = fixture.rootKey // deliberately retired
	h.SigningKey = validatorKey
	h.AppV20ActiveFn = func() bool { return true }
	h.GovernanceDomainFn = func() string { return dashboardTestGovernanceDomain }
	h.ValidatorCountFn = func() int { return 1 }
	req := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/network/reassign-domain-ownership",
		strings.NewReader(fmt.Sprintf(
			`{"source_agent_id":%q,"target_agent_id":%q,"domain":"source-work"}`,
			fixture.agentID, targetID,
		)),
	)
	req.Header.Set("Content-Type", "application/json")
	req = appV23AccessAs(req, rotatedID)
	rec := httptest.NewRecorder()
	h.handleReassignDomainOwnership(sqlStore).ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.Len(t, captured, 3)

	propose := captured[0]
	require.Equal(t, tx.TxTypeGovPropose, propose.Type)
	assert.Equal(t, agentIDForKey(validatorKey), hex.EncodeToString(propose.PublicKey))
	assert.Equal(t, rotatedID, hex.EncodeToString(propose.AgentPubKey))
	assert.Nil(t, propose.LocalElevation)
	reassign := captured[1]
	require.Equal(t, tx.TxTypeDomainReassign, reassign.Type)
	assert.Equal(t, rotatedID, hex.EncodeToString(reassign.PublicKey))
	assert.Nil(t, reassign.LocalElevation)
	grant := captured[2]
	require.Equal(t, tx.TxTypeAccessGrant, grant.Type)
	assert.Equal(t, targetID, hex.EncodeToString(grant.PublicKey))
}

func TestAppV23DashboardGovernanceProofUsesRotatedRootCredential(t *testing.T) {
	fixture := newAppV23AccessFixture(t)
	_, rotatedKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	rotatedID := agentIDForKey(rotatedKey)
	require.NoError(t, fixture.badger.RotateAppV23RootCredential(1, rotatedID, 2))
	_, validatorKey, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)

	var captured *tx.ParsedTx
	rpc := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, decodeErr := hex.DecodeString(strings.TrimPrefix(r.URL.Query().Get("tx"), "0x"))
		require.NoError(t, decodeErr)
		captured, decodeErr = tx.DecodeTx(raw)
		require.NoError(t, decodeErr)
		_, _ = fmt.Fprint(w, `{"result":{"check_tx":{"code":0},"tx_result":{"code":0},"hash":"VOTE","height":"3"}}`)
	}))
	defer rpc.Close()
	h := appV23AccessTestHandler(
		fixture, rpc.URL, map[string]ed25519.PrivateKey{rotatedID: rotatedKey},
	)
	h.SigningKey = validatorKey
	h.AdminSigningKey = fixture.rootKey // stale after rotation; must not authorize
	h.AppV20ActiveFn = func() bool { return true }
	h.GovernanceDomainFn = func() string { return strings.Repeat("ab", 32) }
	proposal, err := json.Marshal(governance.ProposalState{
		ProposalID: "proposal-1",
		Operation:  governance.OpAddValidator,
		TargetID:   rotatedID,
		ProposerID: rotatedID,
		Status:     governance.StatusVoting,
	})
	require.NoError(t, err)
	require.NoError(t, fixture.badger.SetGovProposal("proposal-1", proposal))
	req := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/governance/vote",
		strings.NewReader(`{"proposal_id":"proposal-1","decision":"accept"}`),
	)
	req = appV23AccessAs(req, rotatedID)
	rec := httptest.NewRecorder()
	h.handleDashboardGovVote(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	require.NotNil(t, captured)
	assert.Equal(t, agentIDForKey(validatorKey), hex.EncodeToString(captured.PublicKey))
	assert.Equal(t, rotatedID, hex.EncodeToString(captured.AgentPubKey))
	assert.Nil(t, captured.LocalElevation, "current Root needs no delegated elevation")

	staleReq := httptest.NewRequest(
		http.MethodPost, "/v1/dashboard/governance/vote",
		strings.NewReader(`{"proposal_id":"proposal-1","decision":"accept"}`),
	)
	staleReq = appV23AccessAs(staleReq, fixture.rootID)
	staleRec := httptest.NewRecorder()
	h.handleDashboardGovVote(staleRec, staleReq)
	assert.Equal(t, http.StatusForbidden, staleRec.Code, staleRec.Body.String())
	assert.Contains(t, staleRec.Body.String(), "stale_root_credential")
}
