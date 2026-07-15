package metrics

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func readiness(t *testing.T, h *HealthChecker, query string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/ready"+query, nil)
	rec := httptest.NewRecorder()
	h.ReadinessHandler(rec, req)
	var body map[string]any
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	return rec.Code, body
}

func TestReadiness_CoreDown_NotReady(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(false)
	code, body := readiness(t, h, "")
	if code != http.StatusServiceUnavailable {
		t.Fatalf("core down must be 503, got %d", code)
	}
	if body["status"] != "not_ready" {
		t.Errorf("want not_ready, got %v", body["status"])
	}
}

func TestReadiness_SemanticEmbedderDown_Degraded(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(true)
	h.SetEmbedderHealth(EmbedderStatus{OK: false, Semantic: true, Provider: "ollama", Detail: "connection refused"})

	// Default: 200 degraded (node still serves keyword recall).
	code, body := readiness(t, h, "")
	if code != http.StatusOK {
		t.Fatalf("semantic embedder down must be 200 degraded by default, got %d", code)
	}
	if body["status"] != "degraded" {
		t.Errorf("want degraded, got %v", body["status"])
	}

	// strict=1: hard 503 for gates that require semantic recall.
	code2, body2 := readiness(t, h, "?strict=1")
	if code2 != http.StatusServiceUnavailable {
		t.Fatalf("strict degraded must be 503, got %d", code2)
	}
	if body2["status"] != "degraded" {
		t.Errorf("want degraded, got %v", body2["status"])
	}
}

func TestReadiness_HashEmbedderDown_StillReady(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(true)
	// Hash provider is non-semantic: "down" is a capability note, not a fault.
	h.SetEmbedderHealth(EmbedderStatus{OK: false, Semantic: false})
	code, body := readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("hash provider must stay ready, got %d %v", code, body["status"])
	}
}

func TestReadiness_CoreDownTakesPrecedenceOverEmbedder(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(false) // core down
	h.SetEmbedderHealth(EmbedderStatus{OK: true, Semantic: true})
	code, body := readiness(t, h, "")
	if code != http.StatusServiceUnavailable || body["status"] != "not_ready" {
		t.Fatalf("core-down must win over a healthy embedder, got %d %v", code, body["status"])
	}
}

func TestReadiness_SemanticEmbedderUp_Ready(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(true)
	h.SetEmbedderHealth(EmbedderStatus{OK: true, Semantic: true, Provider: "ollama", Model: "nomic-embed-text"})
	code, body := readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("healthy semantic embedder must be ready, got %d %v", code, body["status"])
	}
}

func TestReadiness_VoterBlockInformationalOnly(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(true)

	// Never reported (voter-less node / loop not up yet): block present,
	// checked=false, readiness untouched.
	code, body := readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("unreported voter must not gate readiness, got %d %v", code, body["status"])
	}
	v, _ := body["voter"].(map[string]any)
	if v == nil || v["checked"] != false {
		t.Fatalf("voter block should report checked=false before the first tick, got %v", body["voter"])
	}

	// Live voter with a backlog snapshot: fields surface verbatim.
	h.SetVoterStatus(VoterStatus{
		Running:                  true,
		ValidatorID:              "ab12cd34",
		LastVoteUnix:             1700000000,
		OldestProposedAgeSeconds: 12.5,
		PendingProposed:          3,
	})
	code, body = readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("healthy voter must be ready, got %d %v", code, body["status"])
	}
	v, _ = body["voter"].(map[string]any)
	if v == nil {
		t.Fatal("missing voter block in /ready body")
	}
	if v["checked"] != true || v["running"] != true || v["validator_id"] != "ab12cd34" {
		t.Errorf("voter identity fields wrong: %v", v)
	}
	if v["last_vote_unix"] != float64(1700000000) || v["oldest_proposed_age_seconds"] != 12.5 || v["pending_proposed"] != float64(3) {
		t.Errorf("voter backlog fields wrong: %v", v)
	}

	// Dead voter with a huge stuck backlog: still informational — the /ready
	// status must NOT flip (the alarm surface is sage_proposed_oldest_age_seconds).
	h.SetVoterStatus(VoterStatus{Running: false, OldestProposedAgeSeconds: 99999, PendingProposed: 42})
	code, body = readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("dead voter must stay informational (ready/200), got %d %v", code, body["status"])
	}
	v, _ = body["voter"].(map[string]any)
	if v == nil || v["running"] != false || v["pending_proposed"] != float64(42) {
		t.Errorf("dead-voter block wrong: %v", v)
	}
}

func TestReadiness_EmbedderUnchecked_Ready(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(true)
	// Never probed (watchdog hasn't run): must not block readiness.
	code, body := readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("unchecked embedder must be ready, got %d %v", code, body["status"])
	}
	emb, _ := body["embedder"].(map[string]any)
	if emb == nil || emb["checked"] != false {
		t.Errorf("embedder block should report checked=false, got %v", body["embedder"])
	}
}

func TestReadiness_ScopedProjectionRequiredFailsClosed(t *testing.T) {
	h := NewHealthChecker()
	h.SetPostgresHealth(true)
	h.SetCometBFTHealth(true)

	// Empty/pre-v20 nodes have no scoped serving dependency.
	h.SetScopedProjectionStatus(ScopedProjectionStatus{Required: false, OK: true})
	code, body := readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("empty scoped projection must stay ready, got %d %v", code, body["status"])
	}

	// Once canonical envelopes exist, a locked or failed SQL projection is a
	// hard readiness failure: serving an empty selected domain is not healthy.
	h.SetScopedProjectionStatus(ScopedProjectionStatus{
		Required: true, OK: false, Records: 4, Detail: "vault locked",
	})
	code, body = readiness(t, h, "")
	if code != http.StatusServiceUnavailable || body["status"] != "not_ready" {
		t.Fatalf("missing scoped projection must be 503, got %d %v", code, body["status"])
	}
	scoped, _ := body["scoped_projection"].(map[string]any)
	if scoped == nil || scoped["checked"] != true || scoped["required"] != true || scoped["records"] != float64(4) {
		t.Fatalf("scoped projection status missing or wrong: %v", body["scoped_projection"])
	}

	h.SetScopedProjectionStatus(ScopedProjectionStatus{Required: true, OK: true, Records: 4, Rebuilt: 4})
	code, body = readiness(t, h, "")
	if code != http.StatusOK || body["status"] != "ready" {
		t.Fatalf("verified scoped projection must recover readiness, got %d %v", code, body["status"])
	}
}
