package memory

// CleanupConfig holds settings for memory auto-cleanup.
type CleanupConfig struct {
	Enabled                bool    `json:"enabled"`
	ObservationTTLDays     int     `json:"observation_ttl_days"`     // TTL for observation-type memories
	SessionTTLDays         int     `json:"session_ttl_days"`         // TTL for session-context observations
	StaleThreshold         float64 `json:"stale_threshold"`          // Confidence below this = stale
	AutoChallengeConflicts bool    `json:"auto_challenge_conflicts"` // Auto-challenge contradicting facts
	CleanupIntervalHours   int     `json:"cleanup_interval_hours"`   // How often to run cleanup
}

// DefaultCleanupConfig returns sensible defaults (disabled by default).
func DefaultCleanupConfig() CleanupConfig {
	return CleanupConfig{
		Enabled:                false,
		ObservationTTLDays:     7,
		SessionTTLDays:         2,
		StaleThreshold:         0.10,
		AutoChallengeConflicts: false,
		CleanupIntervalHours:   24,
	}
}
