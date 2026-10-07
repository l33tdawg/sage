package main

import (
	"os"
	"path/filepath"
	"testing"
)

// selfHealProject auto-heals a project's hooks on bridge startup. It must SKIP that
// heal only when the working dir is $HOME — there the project-scoped
// ${CLAUDE_PROJECT_DIR} hooks would land in the user-global config and break in
// every other project. CLAUDE_CONFIG_DIR overrides the USER config dir, not
// project config (projectDir/.claude/settings.json is still discovered from the
// working directory), so a normal project must still heal when it is set.
func TestSelfHealProject_SkipsHomeNotConfigDir(t *testing.T) {
	// A project whose .claude/hooks already exists, so the heal WOULD fire.
	seedProject := func(t *testing.T) string {
		p := t.TempDir()
		if err := os.MkdirAll(filepath.Join(p, ".claude", "hooks"), 0o755); err != nil {
			t.Fatal(err)
		}
		return p
	}
	healed := func(dir string) bool {
		_, err := os.Stat(filepath.Join(dir, ".claude", "hooks", "sage-stop.sh"))
		return err == nil
	}

	t.Run("heals a normal project", func(t *testing.T) {
		t.Setenv("CLAUDE_CONFIG_DIR", "")
		p := seedProject(t)
		selfHealProject(p, t.TempDir())
		if !healed(p) {
			t.Error("expected sage hooks healed into a normal project")
		}
	})

	// Regression: CLAUDE_CONFIG_DIR must NOT disable project hook repair — it
	// overrides the user config dir, not project config.
	t.Run("heals a normal project even when CLAUDE_CONFIG_DIR is set", func(t *testing.T) {
		t.Setenv("CLAUDE_CONFIG_DIR", t.TempDir())
		p := seedProject(t)
		selfHealProject(p, t.TempDir())
		if !healed(p) {
			t.Error("expected sage hooks healed into a normal project when CLAUDE_CONFIG_DIR is set (it overrides user config, not project config)")
		}
	})

	t.Run("skips when the project dir is the user home", func(t *testing.T) {
		t.Setenv("CLAUDE_CONFIG_DIR", "")
		home := t.TempDir()
		t.Setenv("HOME", home)
		if err := os.MkdirAll(filepath.Join(home, ".claude", "hooks"), 0o755); err != nil {
			t.Fatal(err)
		}
		selfHealProject(home, t.TempDir())
		if healed(home) {
			t.Error("must NOT heal into ~/.claude when projectDir == $HOME")
		}
	})
}
