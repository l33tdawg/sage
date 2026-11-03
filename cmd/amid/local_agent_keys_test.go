package main

import (
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"

	"github.com/l33tdawg/sage/internal/auth"
	"github.com/l33tdawg/sage/internal/store"
	"github.com/l33tdawg/sage/internal/tx"
)

func TestAMIDLocalAgentKeyResolver(t *testing.T) {
	key := amidOperatorTestKey("local-consent")
	id := auth.PublicKeyToAgentID(key.Public().(ed25519.PublicKey))
	dir := t.TempDir()
	path := filepath.Join(dir, id+".key")
	resolve := amidLocalAgentKeyResolver(nil, dir)
	for _, data := range [][]byte{key.Seed(), key} {
		require.NoError(t, os.WriteFile(path, data, 0600))
		got, ok := resolve(id)
		require.True(t, ok)
		require.Equal(t, key, got)
	}
	for _, bad := range []string{"", "../" + id, strings.ToUpper(id), id + ".key"} {
		_, ok := resolve(bad)
		require.False(t, ok, bad)
	}
	_, ok := amidLocalAgentKeyResolver(nil, "")(id)
	require.False(t, ok, "an unconfigured resolver must not discover target keys")
	for _, bad := range [][]byte{nil, []byte("not a key"), make([]byte, 65), amidOperatorTestKey("other").Seed()} {
		require.NoError(t, os.WriteFile(path, bad, 0600))
		_, ok = resolve(id)
		require.False(t, ok)
	}
	corrupt := append([]byte(nil), key...)
	corrupt[63] ^= 1
	require.NoError(t, os.WriteFile(path, corrupt, 0600))
	_, ok = resolve(id)
	require.False(t, ok, "mismatched private/public halves must fail")
	_, err := loadCEREBRUMRootKey(path)
	require.Error(t, err, "Root key parsing must reject the same malformed encoding")
	require.NoError(t, os.WriteFile(path, key.Seed(), 0600))
	_, ok = resolve(id)
	require.True(t, ok, "provisioning is visible without restart")
	require.NoError(t, os.Remove(path))
	_, ok = resolve(id)
	require.False(t, ok, "removed consent keys must not remain cached")
	require.NoError(t, os.Mkdir(path, 0700))
	_, ok = resolve(id)
	require.False(t, ok, "a directory is not a key")
}

func TestAMIDLocalAgentKeyResolverRejectsSymlinks(t *testing.T) {
	key := amidOperatorTestKey("symlink-consent")
	id := auth.PublicKeyToAgentID(key.Public().(ed25519.PublicKey))
	dir := t.TempDir()
	target := filepath.Join(t.TempDir(), "target.key")
	require.NoError(t, os.WriteFile(target, key.Seed(), 0600))
	if err := os.Symlink(target, filepath.Join(dir, id+".key")); err != nil {
		t.Skipf("symlinks unavailable: %v", err)
	}
	_, ok := amidLocalAgentKeyResolver(nil, dir)(id)
	require.False(t, ok)
	linkDir := filepath.Join(t.TempDir(), "keys")
	require.NoError(t, os.Symlink(dir, linkDir))
	_, ok = amidLocalAgentKeyResolver(nil, linkDir)(id)
	require.False(t, ok)
}

func TestAMIDOperatorApprovalWithProvisionedTargetConsent(t *testing.T) {
	for _, scenario := range []string{"approve", "missing key", "wrong key", "unregistered", "unsigned", "ordinary agent", "remote", "missing root"} {
		t.Run(scenario, func(t *testing.T) {
			fixture := newAmidOperatorFixture(t)
			target := amidOperatorTestKey("pending-consent")
			id := auth.PublicKeyToAgentID(target.Public().(ed25519.PublicKey))
			if scenario != "unregistered" {
				require.NoError(t, fixture.badger.RegisterAgentWithCapabilities(id, "Pending", store.AppV23RoleMember, "", "", "", 2, 30))
			}
			dir := t.TempDir()
			if scenario != "missing key" {
				seed := target.Seed()
				if scenario == "wrong key" {
					seed = fixture.agentKey.Seed()
				}
				require.NoError(t, os.WriteFile(filepath.Join(dir, id+".key"), seed, 0600))
			}
			resolveRoot := func(want string) (ed25519.PrivateKey, bool) {
				return fixture.rootKey, scenario != "missing root" && want == fixture.rootID
			}
			var captured *tx.ParsedTx
			rpc := amidOperatorRPCStub(t, &captured)
			defer rpc.Close()
			router := chi.NewRouter()
			wireAmidOperatorRoutes(router, fixture.agents, fixture.badger, rpc.URL, "test", func() bool { return true }, amidLocalAgentKeyResolver(resolveRoot, dir))
			body := amidOperatorPolicyBody(1)
			body["home_domain"] = "pending-consent-home"
			req, encoded := amidOperatorRequest(t, http.MethodPut, "/v1/dashboard/network/access/agents/"+id+"/policy", body)
			if scenario != "unsigned" {
				key := fixture.rootKey
				if scenario == "ordinary agent" {
					key = fixture.agentKey
				}
				signAmidOperatorRequest(t, req, key, encoded)
			}
			if scenario == "remote" {
				req.RemoteAddr = "192.0.2.1:54321"
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if scenario != "approve" {
				require.GreaterOrEqual(t, rec.Code, 400, rec.Body.String())
				require.Nil(t, captured, "refused approval must never broadcast")
				if scenario == "missing key" || scenario == "wrong key" {
					require.Contains(t, rec.Body.String(), "target_key_unavailable")
				}
				return
			}
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.NotNil(t, captured)
			require.Equal(t, tx.TxTypeLocalAgentApprove, captured.Type)
			require.Equal(t, []byte(fixture.rootKey.Public().(ed25519.PublicKey)), []byte(captured.AgentPubKey))
			approval := captured.LocalAgentApprove
			require.NotNil(t, approval)
			require.Equal(t, id, approval.AgentID)
			require.True(t, ed25519.Verify(target.Public().(ed25519.PublicKey), tx.LocalAgentApprovalSignBytes(fixture.rootID, approval), approval.TargetSignature))
			approval.Clearance++
			require.False(t, ed25519.Verify(target.Public().(ed25519.PublicKey), tx.LocalAgentApprovalSignBytes(fixture.rootID, approval), approval.TargetSignature), "consent binds the exact policy")
		})
	}
}
