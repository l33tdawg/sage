package metrics

import (
	"encoding/json"
	"net/http"
	"sync/atomic"
)

// EmbedderStatus is the health checker's view of the embedding provider, refreshed by
// the node's background watchdog. It lets /ready report whether SEMANTIC recall is
// actually available — a down embedder silently degrades hybrid recall to keyword-only.
type EmbedderStatus struct {
	Checked  bool   `json:"checked"`            // has the watchdog probed yet?
	OK       bool   `json:"ok"`                 // reachable this probe
	Semantic bool   `json:"semantic"`           // true=meaning-based (Ollama/…); false=hash fallback
	Provider string `json:"provider,omitempty"` // e.g. "ollama"
	Model    string `json:"model,omitempty"`    // e.g. "nomic-embed-text"
	Detail   string `json:"detail,omitempty"`   // error summary when down
}

// VoterStatus is the health checker's view of the memory auto-voter, refreshed
// every poll tick by the voter loop. It lets /ready show whether memories can
// actually leave status='proposed' on this node — with no voter anywhere,
// submissions strand at proposed forever with no other signal. Informational
// only: it NEVER gates readiness, because a voter-less node is a legitimate
// deployment (another validator may vote memories through).
type VoterStatus struct {
	Checked                  bool    `json:"checked"`                     // has the voter reported yet?
	Running                  bool    `json:"running"`                     // voter goroutine live right now
	ValidatorID              string  `json:"validator_id,omitempty"`      // hex consensus pubkey the voter signs with
	LastVoteUnix             int64   `json:"last_vote_unix,omitempty"`    // when this node last broadcast a memory vote (0 = never this session)
	OldestProposedAgeSeconds float64 `json:"oldest_proposed_age_seconds"` // node-local stuck-memory watermark
	PendingProposed          int     `json:"pending_proposed"`            // node-local count of status='proposed'
}

// ScopedProjectionStatus reports whether AppHash-covered v11.9 scoped content
// has been verified into this node's local serving projection. Consensus can
// remain healthy while this is false, but the node must not advertise serving
// readiness until canonical scoped records are queryable locally.
type ScopedProjectionStatus struct {
	Checked  bool   `json:"checked"`
	Required bool   `json:"required"`
	OK       bool   `json:"ok"`
	Records  int    `json:"records"`
	Rebuilt  int    `json:"rebuilt"`
	Detail   string `json:"detail,omitempty"`
}

// HealthChecker tracks the health status of dependencies.
type HealthChecker struct {
	postgresOK atomic.Bool
	cometbftOK atomic.Bool
	embedder   atomic.Value // EmbedderStatus, set by SetEmbedderHealth
	voter      atomic.Value // VoterStatus, set by SetVoterStatus
	scoped     atomic.Value // ScopedProjectionStatus, set by recovery wiring
	Version    string
}

// NewHealthChecker creates a new health checker.
func NewHealthChecker() *HealthChecker {
	return &HealthChecker{}
}

// SetEmbedderHealth records the latest embedding-provider probe (called by the node's
// watchdog). Until the first call, the embedder reads as not-yet-checked.
func (h *HealthChecker) SetEmbedderHealth(s EmbedderStatus) {
	s.Checked = true
	h.embedder.Store(s)
}

func (h *HealthChecker) embedderStatus() EmbedderStatus {
	if v, ok := h.embedder.Load().(EmbedderStatus); ok {
		return v
	}
	return EmbedderStatus{}
}

// SetVoterStatus records the memory auto-voter's latest self-report (called by
// the voter loop each poll tick). Until the first call, the voter reads as
// not-yet-checked. Informational only — it never changes the
// ready/degraded/not_ready decision; the alerting surface for a stuck backlog
// is the sage_proposed_oldest_age_seconds gauge.
func (h *HealthChecker) SetVoterStatus(s VoterStatus) {
	s.Checked = true
	h.voter.Store(s)
}

func (h *HealthChecker) voterStatus() VoterStatus {
	if v, ok := h.voter.Load().(VoterStatus); ok {
		return v
	}
	return VoterStatus{}
}

// SetScopedProjectionStatus records the latest canonical projection rebuild.
// Callers set Required only when canonical scoped envelopes actually exist, so
// pre-v20 nodes and empty app-v20 scopes do not acquire a synthetic dependency.
func (h *HealthChecker) SetScopedProjectionStatus(s ScopedProjectionStatus) {
	s.Checked = true
	h.scoped.Store(s)
}

func (h *HealthChecker) scopedProjectionStatus() ScopedProjectionStatus {
	if v, ok := h.scoped.Load().(ScopedProjectionStatus); ok {
		return v
	}
	return ScopedProjectionStatus{}
}

// SetPostgresHealth updates the PostgreSQL health status.
func (h *HealthChecker) SetPostgresHealth(ok bool) {
	h.postgresOK.Store(ok)
}

// SetCometBFTHealth updates the CometBFT health status.
func (h *HealthChecker) SetCometBFTHealth(ok bool) {
	h.cometbftOK.Store(ok)
}

// IsHealthy returns true if all dependencies are healthy.
func (h *HealthChecker) IsHealthy() bool {
	return h.postgresOK.Load() && h.cometbftOK.Load()
}

// HealthHandler handles GET /health requests.
func (h *HealthChecker) HealthHandler(w http.ResponseWriter, r *http.Request) {
	status := "healthy"
	httpStatus := http.StatusOK

	if !h.IsHealthy() {
		status = "unhealthy"
		httpStatus = http.StatusServiceUnavailable
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	// /health is reachable through the wizard's tunnel allowlist; we keep
	// it minimal so internet visitors can't easily fingerprint a SAGE node
	// to a specific version.
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status": status,
	})
}

// ReadinessHandler handles GET /ready requests.
func (h *HealthChecker) ReadinessHandler(w http.ResponseWriter, r *http.Request) {
	pgOK := h.postgresOK.Load()
	cmtOK := h.cometbftOK.Load()
	emb := h.embedderStatus()
	scoped := h.scopedProjectionStatus()

	status := "ready"
	httpStatus := http.StatusOK

	switch {
	case !pgOK || !cmtOK:
		// Core infrastructure down — genuinely not ready.
		status = "not_ready"
		httpStatus = http.StatusServiceUnavailable
	case scoped.Required && (!scoped.Checked || !scoped.OK):
		// Canonical scoped content exists but the local serving projection is
		// absent, locked, or failed verification. Reporting ready here would let
		// a state-synced replica silently serve an incomplete selected domain.
		status = "not_ready"
		httpStatus = http.StatusServiceUnavailable
	case emb.Checked && emb.Semantic && !emb.OK:
		// A semantic embedder that has been probed and is down: the node still SERVES
		// (keyword recall works) but semantic/hybrid recall is unavailable. Report
		// "degraded" with HTTP 200 by default so orchestrators pick their own
		// strictness; ?strict=1 makes it a hard 503 for readiness gates that require
		// semantic recall. A hash provider (Semantic=false) is a capability, not a
		// fault, so it stays "ready".
		status = "degraded"
		if r.URL.Query().Get("strict") == "1" {
			httpStatus = http.StatusServiceUnavailable
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"status":            status,
		"postgres":          pgOK,
		"cometbft":          cmtOK,
		"embedder":          emb,
		"scoped_projection": scoped,
		// Informational voter/backlog block — never gates the status above
		// (a voter-less node is legitimate; peers may vote memories through).
		"voter": h.voterStatus(),
	})
}
