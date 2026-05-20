package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/rs/zerolog"

	"github.com/l33tdawg/sage/internal/store"
)

const testAdminAgentID = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func newInitialAdminTestStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	s, err := store.NewSQLiteStore(context.Background(), dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

// TestEnsureInitialAdmin_EnvUnset_NoOp: with SAGE_INITIAL_ADMIN_AGENT_ID
// absent, the helper must not touch the SQL store. This is the default
// state for operators who don't want to declare an admin agent up front.
func TestEnsureInitialAdmin_EnvUnset_NoOp(t *testing.T) {
	os.Unsetenv("SAGE_INITIAL_ADMIN_AGENT_ID")
	s := newInitialAdminTestStore(t)

	ensureInitialAdmin(context.Background(), s, zerolog.Nop())

	agents, err := s.ListAgents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(agents) != 0 {
		t.Errorf("env unset must be no-op, got %d agents", len(agents))
	}
}

// TestEnsureInitialAdmin_FreshInstall_CreatesAdmin: with the env set and
// no prior agent, helper creates the admin row.
func TestEnsureInitialAdmin_FreshInstall_CreatesAdmin(t *testing.T) {
	os.Setenv("SAGE_INITIAL_ADMIN_AGENT_ID", testAdminAgentID)
	defer os.Unsetenv("SAGE_INITIAL_ADMIN_AGENT_ID")
	os.Setenv("SAGE_INITIAL_ADMIN_NAME", "levelup-admin")
	defer os.Unsetenv("SAGE_INITIAL_ADMIN_NAME")

	s := newInitialAdminTestStore(t)
	ensureInitialAdmin(context.Background(), s, zerolog.Nop())

	a, err := s.GetAgent(context.Background(), testAdminAgentID)
	if err != nil || a == nil {
		t.Fatalf("expected admin agent, got err=%v a=%v", err, a)
	}
	if a.Role != "admin" {
		t.Errorf("role = %q, want admin", a.Role)
	}
	if a.Name != "levelup-admin" {
		t.Errorf("name = %q, want levelup-admin", a.Name)
	}
	if a.Clearance != 4 {
		t.Errorf("clearance = %d, want 4 (TopSecret)", a.Clearance)
	}
}

// TestEnsureInitialAdmin_PromotesExistingNonAdmin: an agent that exists
// in SQL with role=member must get promoted to admin.
func TestEnsureInitialAdmin_PromotesExistingNonAdmin(t *testing.T) {
	os.Setenv("SAGE_INITIAL_ADMIN_AGENT_ID", testAdminAgentID)
	defer os.Unsetenv("SAGE_INITIAL_ADMIN_AGENT_ID")

	s := newInitialAdminTestStore(t)
	if err := s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID:   testAdminAgentID,
		Name:      "pre-existing",
		Role:      "member",
		Status:    "active",
		Clearance: 1,
	}); err != nil {
		t.Fatal(err)
	}

	ensureInitialAdmin(context.Background(), s, zerolog.Nop())

	a, _ := s.GetAgent(context.Background(), testAdminAgentID)
	if a.Role != "admin" {
		t.Errorf("role = %q, want admin (must be promoted)", a.Role)
	}
	if a.Clearance < 4 {
		t.Errorf("clearance = %d, want >= 4", a.Clearance)
	}
}

// TestEnsureInitialAdmin_AlreadyAdmin_Idempotent: re-running the bootstrap
// against an already-admin agent must be a no-op.
func TestEnsureInitialAdmin_AlreadyAdmin_Idempotent(t *testing.T) {
	os.Setenv("SAGE_INITIAL_ADMIN_AGENT_ID", testAdminAgentID)
	defer os.Unsetenv("SAGE_INITIAL_ADMIN_AGENT_ID")

	s := newInitialAdminTestStore(t)
	if err := s.CreateAgent(context.Background(), &store.AgentEntry{
		AgentID:   testAdminAgentID,
		Name:      "already-admin",
		Role:      "admin",
		Status:    "active",
		Clearance: 4,
	}); err != nil {
		t.Fatal(err)
	}

	ensureInitialAdmin(context.Background(), s, zerolog.Nop())

	a, _ := s.GetAgent(context.Background(), testAdminAgentID)
	if a.Name != "already-admin" {
		t.Errorf("idempotent path must not mutate name, got %q", a.Name)
	}
}

// TestEnsureInitialAdmin_BadAgentID_SkipsWithWarning: a malformed agent
// ID env value must produce a warning log but no SQL changes — safer than
// silently inserting junk.
func TestEnsureInitialAdmin_BadAgentID_SkipsWithWarning(t *testing.T) {
	for _, bad := range []string{"not-hex", "short", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"} {
		bad := bad
		t.Run(bad, func(t *testing.T) {
			os.Setenv("SAGE_INITIAL_ADMIN_AGENT_ID", bad)
			defer os.Unsetenv("SAGE_INITIAL_ADMIN_AGENT_ID")

			s := newInitialAdminTestStore(t)
			ensureInitialAdmin(context.Background(), s, zerolog.Nop())

			agents, _ := s.ListAgents(context.Background())
			if len(agents) != 0 {
				t.Errorf("malformed agent id %q must be skipped, got %d agents", bad, len(agents))
			}
		})
	}
}

func TestIsValidAgentID(t *testing.T) {
	if !isValidAgentID(testAdminAgentID) {
		t.Error("canonical 64-char hex must validate")
	}
	for _, bad := range []string{"", "short", "0123", "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdeg"} {
		if isValidAgentID(bad) {
			t.Errorf("invalid id %q must not validate", bad)
		}
	}
}
