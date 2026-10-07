package main

import (
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestLoadOrGenerateKeyConcurrentFirstLaunchUsesOneIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "agent.key")
	const callers = 24
	ids := make(chan string, callers)
	errs := make(chan error, callers)
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			key, err := loadOrGenerateKey(path)
			if err == nil {
				ids <- hex.EncodeToString(key.Public().(ed25519.PublicKey))
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(ids)
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	unique := map[string]bool{}
	for id := range ids {
		unique[id] = true
	}
	require.Len(t, unique, 1)
}

func TestCanonicalWorkspaceRootCollapsesLinkedWorktree(t *testing.T) {
	root := t.TempDir()
	run := func(args ...string) {
		cmd := exec.Command("git", append([]string{"-C", root}, args...)...)
		output, err := cmd.CombinedOutput()
		require.NoError(t, err, "%s", output)
	}
	run("init")
	require.NoError(t, os.WriteFile(filepath.Join(root, "README.md"), []byte("workspace\n"), 0o600))
	run("add", "README.md")
	run("-c", "user.name=SAGE Test", "-c", "user.email=sage@example.invalid", "commit", "-m", "init")
	worktree := filepath.Join(t.TempDir(), "scratchpad-agent")
	run("worktree", "add", "-b", "scratch-test", worktree)

	got, err := canonicalWorkspaceRoot(worktree)
	require.NoError(t, err)
	require.True(t, sameFilesystemPath(root, got), "linked worktrees must reuse the primary repository boundary")

	key := filepath.Join(t.TempDir(), "primary-agent.key")
	config := map[string]any{"mcpServers": map[string]any{"sage": map[string]any{"env": map[string]any{
		"SAGE_PROVIDER": "claude-code", "SAGE_PROJECT": "workspace", "SAGE_IDENTITY_PATH": key,
	}}}}
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, ".mcp.json"), raw, 0o600))
	rootKey, _, _, err := resolveImplicitWorkspaceIdentity(t.TempDir(), root, "claude-code", "")
	require.NoError(t, err)
	worktreeKey, _, _, err := resolveImplicitWorkspaceIdentity(t.TempDir(), worktree, "claude-code", "")
	require.NoError(t, err)
	require.Equal(t, rootKey, worktreeKey)
	require.Equal(t, key, worktreeKey, "managed worktrees must use the primary repository's established signer")
}

func TestCanonicalWorkspaceRootFailsClosedWhenGitProbeTimesOut(t *testing.T) {
	started := time.Now()
	_, err := canonicalWorkspaceRootWithProbe(t.TempDir(), func(ctx context.Context, _ string) ([]byte, error) {
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.Less(t, time.Until(deadline), 3*time.Second,
			"Git discovery must keep a bounded identity-resolution budget")
		<-ctx.Done()
		return nil, ctx.Err()
	})
	require.ErrorIs(t, err, context.DeadlineExceeded,
		"a timed-out Git probe must not fall back to a transient worktree identity")
	require.Less(t, time.Since(started), 4*time.Second,
		"identity resolution must fail closed promptly when Git stalls")
}

func TestCanonicalWorkspaceRootRejectsFilesystemRootWithoutGit(t *testing.T) {
	root := string(filepath.Separator)
	probed := false

	resolved, err := canonicalWorkspaceRootWithProbe(root, func(context.Context, string) ([]byte, error) {
		probed = true
		return nil, exec.ErrNotFound
	})

	require.ErrorContains(t, err, "refusing broad workspace identity root")
	require.Empty(t, resolved)
	require.False(t, probed, "a filesystem-root workspace must fail before Git probing or identity generation")
}

func TestResolveImplicitWorkspaceIdentityRejectsFilesystemRoot(t *testing.T) {
	keyPath, provider, project, err := resolveImplicitWorkspaceIdentity(
		t.TempDir(), string(filepath.Separator), "codex", "",
	)

	require.ErrorContains(t, err, "refusing broad workspace identity root")
	require.Empty(t, keyPath)
	require.Equal(t, "codex", provider)
	require.Empty(t, project)
}

func TestPrimaryWorkspaceMCPEnvPreservesPinnedProviderIdentity(t *testing.T) {
	root := t.TempDir()
	key := filepath.Join(t.TempDir(), "agent.key")
	config := map[string]any{"mcpServers": map[string]any{"sage": map[string]any{"env": map[string]any{
		"SAGE_PROVIDER": "claude-code", "SAGE_PROJECT": "sage", "SAGE_IDENTITY_PATH": key,
	}}}}
	raw, err := json.Marshal(config)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, ".mcp.json"), raw, 0o600))

	env, found, err := primaryWorkspaceMCPEnv(root)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "claude-code", env["SAGE_PROVIDER"])
	require.Equal(t, "sage", env["SAGE_PROJECT"])
	require.Equal(t, key, env["SAGE_IDENTITY_PATH"])

	home := t.TempDir()
	inherited, provider, project, err := resolveImplicitWorkspaceIdentity(home, root, "", "")
	require.NoError(t, err)
	require.Equal(t, key, inherited)
	require.Equal(t, "claude-code", provider)
	require.Equal(t, "sage", project)

	codexKey, codexProvider, _, err := resolveImplicitWorkspaceIdentity(home, root, "codex", "")
	require.NoError(t, err)
	require.Equal(t, "codex", codexProvider)
	require.NotEqual(t, key, codexKey, "Claude Code and Codex must remain separate signers")
}

func TestProviderProjectAgentDirSeparatesProvidersInOneCheckout(t *testing.T) {
	home := t.TempDir()
	project := filepath.Join(t.TempDir(), "tii-sage")
	claudePath := providerProjectAgentDir(home, project, "claude-code")
	codexPath := providerProjectAgentDir(home, project, "codex")

	require.NotEqual(t, claudePath, codexPath, "Claude Code and Codex must never share a project identity path")
	require.Contains(t, claudePath, "tii-sage-claude-code-")
	require.Contains(t, codexPath, "tii-sage-codex-")
	require.Equal(t, claudePath, providerProjectAgentDir(home, project, "CLAUDE-CODE"), "provider spelling must not fork an identity")
}

func writeCodexProjectConfig(t *testing.T, root, identityPath string) {
	t.Helper()
	configPath := filepath.Join(root, ".codex", "config.toml")
	require.NoError(t, os.MkdirAll(filepath.Dir(configPath), 0o755))
	require.NoError(t, os.WriteFile(configPath, []byte(codexSageConfigBlockWithIdentity(
		configPath, "/Applications/SAGE.app/Contents/MacOS/sage-gui", t.TempDir(), "codex", identityPath,
	)), 0o600))
}

// A Codex session that resolves a checkout must inherit the pin written in that
// checkout's .codex/config.toml. Reading only .mcp.json (Claude Code) made every
// such session mint a second, hashed agent id for the same repository, so one
// checkout showed up as two agents with the same name.
func TestResolveImplicitWorkspaceIdentityInheritsCodexProjectPin(t *testing.T) {
	root := t.TempDir()
	key := filepath.Join(t.TempDir(), "agent.key")
	writeCodexProjectConfig(t, root, key)
	home := t.TempDir()

	inherited, provider, project, err := resolveImplicitWorkspaceIdentity(home, root, "codex", "")
	require.NoError(t, err)
	require.Equal(t, key, inherited)
	require.Equal(t, "codex", provider)
	require.Equal(t, filepath.Base(root), project)

	// Lifecycle hooks carry no provider of their own and must land on the same
	// signer as the MCP session in this checkout.
	hookKey, hookProvider, _, err := resolveImplicitWorkspaceIdentity(home, root, "", "")
	require.NoError(t, err)
	require.Equal(t, key, hookKey)
	require.Equal(t, "codex", hookProvider)
}

// One checkout can pin both a Claude Code signer (.mcp.json) and a Codex signer
// (.codex/config.toml). Each caller must get the pin that owns its provider, and
// a provider-less caller keeps the historical Claude-first preference.
func TestResolveImplicitWorkspaceIdentitySelectsPinByProvider(t *testing.T) {
	root := t.TempDir()
	claudeKey := filepath.Join(t.TempDir(), "claude.key")
	codexKey := filepath.Join(t.TempDir(), "codex.key")
	raw, err := json.Marshal(map[string]any{"mcpServers": map[string]any{"sage": map[string]any{"env": map[string]any{
		"SAGE_PROVIDER": "claude-code", "SAGE_IDENTITY_PATH": claudeKey,
	}}}})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(root, ".mcp.json"), raw, 0o600))
	writeCodexProjectConfig(t, root, codexKey)
	home := t.TempDir()

	codexResolved, _, _, err := resolveImplicitWorkspaceIdentity(home, root, "codex", "")
	require.NoError(t, err)
	require.Equal(t, codexKey, codexResolved, "a Codex session must inherit the Codex project pin")

	claudeResolved, _, _, err := resolveImplicitWorkspaceIdentity(home, root, "claude-code", "")
	require.NoError(t, err)
	require.Equal(t, claudeKey, claudeResolved)

	neutral, neutralProvider, _, err := resolveImplicitWorkspaceIdentity(home, root, "", "")
	require.NoError(t, err)
	require.Equal(t, claudeKey, neutral, "provider-less callers keep the historical Claude-first preference")
	require.Equal(t, "claude-code", neutralProvider)
}

func TestResolveImplicitWorkspaceIdentityFailsClosedOnInvalidCodexConfig(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".codex"), 0o755))
	require.NoError(t, os.WriteFile(
		filepath.Join(root, ".codex", "config.toml"), []byte("[mcp_servers.sage\nunterminated = "), 0o600,
	))

	_, _, _, err := resolveImplicitWorkspaceIdentity(t.TempDir(), root, "codex", "")
	require.ErrorContains(t, err, "invalid TOML")
}
