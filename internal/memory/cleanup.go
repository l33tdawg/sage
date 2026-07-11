package memory

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

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

// CleanupConfigFromPrefs builds a CleanupConfig from a preferences map.
func CleanupConfigFromPrefs(prefs map[string]string) CleanupConfig {
	cfg := DefaultCleanupConfig()
	if v, ok := prefs["cleanup_enabled"]; ok {
		cfg.Enabled = v == "true"
	}
	if v, ok := prefs["cleanup_observation_ttl_days"]; ok {
		_, _ = fmt.Sscanf(v, "%d", &cfg.ObservationTTLDays)
	}
	if v, ok := prefs["cleanup_session_ttl_days"]; ok {
		_, _ = fmt.Sscanf(v, "%d", &cfg.SessionTTLDays)
	}
	if v, ok := prefs["cleanup_stale_threshold"]; ok {
		_, _ = fmt.Sscanf(v, "%f", &cfg.StaleThreshold)
	}
	if v, ok := prefs["cleanup_auto_challenge"]; ok {
		cfg.AutoChallengeConflicts = v == "true"
	}
	if v, ok := prefs["cleanup_interval_hours"]; ok {
		_, _ = fmt.Sscanf(v, "%d", &cfg.CleanupIntervalHours)
	}
	return cfg
}

// CleanupConfigToPrefs converts CleanupConfig to a preferences map.
func CleanupConfigToPrefs(cfg CleanupConfig) map[string]string {
	return map[string]string{
		"cleanup_enabled":              fmt.Sprintf("%t", cfg.Enabled),
		"cleanup_observation_ttl_days": fmt.Sprintf("%d", cfg.ObservationTTLDays),
		"cleanup_session_ttl_days":     fmt.Sprintf("%d", cfg.SessionTTLDays),
		"cleanup_stale_threshold":      fmt.Sprintf("%.2f", cfg.StaleThreshold),
		"cleanup_auto_challenge":       fmt.Sprintf("%t", cfg.AutoChallengeConflicts),
		"cleanup_interval_hours":       fmt.Sprintf("%d", cfg.CleanupIntervalHours),
	}
}

// CleanupStore defines the store methods needed by the cleanup engine.
type CleanupStore interface {
	GetAllPreferences(ctx context.Context) (map[string]string, error)
	SetPreference(ctx context.Context, key, value string) error
	GetCleanupCandidates(ctx context.Context, observationTTLDays int, sessionTTLDays int, staleThreshold float64) ([]*MemoryRecord, error)
	DeprecateMemories(ctx context.Context, memoryIDs []string) (int, error)
}

// CleanupResult holds the result of a cleanup run.
type CleanupResult struct {
	Checked       int      `json:"checked"`
	Deprecated    int      `json:"deprecated"`
	DeprecatedIDs []string `json:"deprecated_ids,omitempty"`
	DryRun        bool     `json:"dry_run"`
	Reason        string   `json:"reason,omitempty"`
}

// RunCleanup executes the memory cleanup logic.
// If dryRun is true, it reports what would be cleaned up without making changes.
func RunCleanup(ctx context.Context, store CleanupStore, cfg CleanupConfig, dryRun bool) (*CleanupResult, error) {
	result := &CleanupResult{DryRun: dryRun}

	if !cfg.Enabled && !dryRun {
		result.Reason = "cleanup disabled"
		return result, nil
	}

	// Get candidates: expired observations + stale memories
	candidates, err := store.GetCleanupCandidates(ctx, cfg.ObservationTTLDays, cfg.SessionTTLDays, cfg.StaleThreshold)
	if err != nil {
		return nil, fmt.Errorf("get cleanup candidates: %w", err)
	}

	result.Checked = len(candidates)

	// Filter by computed confidence for stale threshold check
	now := time.Now()
	var toDeprecate []string
	for _, rec := range candidates {
		// Never auto-deprecate open tasks
		if rec.IsOpenTask() {
			continue
		}

		currentConf := ComputeConfidence(rec.ConfidenceScore, rec.CreatedAt, now, 0, rec.DomainTag)

		// Apply type-based decay multiplier for observations
		if rec.MemoryType == TypeObservation {
			// Observations get 3x decay rate applied
			ageDays := now.Sub(rec.CreatedAt).Hours() / 24.0

			// Session-context observations: check against session TTL
			if rec.DomainTag == "session-context" && ageDays > float64(cfg.SessionTTLDays) {
				toDeprecate = append(toDeprecate, rec.MemoryID)
				continue
			}

			// Regular observations: check against observation TTL
			if ageDays > float64(cfg.ObservationTTLDays) {
				toDeprecate = append(toDeprecate, rec.MemoryID)
				continue
			}
		}

		// Any memory type: if confidence has decayed below threshold
		if currentConf < cfg.StaleThreshold {
			toDeprecate = append(toDeprecate, rec.MemoryID)
		}
	}

	result.DeprecatedIDs = toDeprecate

	if dryRun {
		result.Deprecated = len(toDeprecate)
		return result, nil
	}

	// Execute deprecation
	if len(toDeprecate) > 0 {
		n, err := store.DeprecateMemories(ctx, toDeprecate)
		if err != nil {
			return nil, fmt.Errorf("deprecate memories: %w", err)
		}
		result.Deprecated = n
	}

	// Record last cleanup time
	_ = store.SetPreference(ctx, "cleanup_last_run", time.Now().UTC().Format(time.RFC3339))
	_ = store.SetPreference(ctx, "cleanup_last_result", mustJSON(result))

	return result, nil
}

// StartCleanupLoop starts a background goroutine that runs cleanup periodically.
func StartCleanupLoop(ctx context.Context, store CleanupStore) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Initial delay — let the system boot up
		select {
		case <-time.After(30 * time.Second):
		case <-ctx.Done():
			return
		}

		for {
			prefs, err := store.GetAllPreferences(ctx)
			if err != nil {
				log.Printf("[cleanup] error loading preferences: %v", err)
				select {
				case <-time.After(1 * time.Hour):
				case <-ctx.Done():
					return
				}
				continue
			}

			cfg := CleanupConfigFromPrefs(prefs)
			if !cfg.Enabled {
				// Check again in an hour
				select {
				case <-time.After(1 * time.Hour):
				case <-ctx.Done():
					return
				}
				continue
			}

			result, err := RunCleanup(ctx, store, cfg, false)
			if err != nil {
				log.Printf("[cleanup] error running cleanup: %v", err)
			} else if result.Deprecated > 0 {
				log.Printf("[cleanup] deprecated %d stale memories", result.Deprecated)
			}

			interval := time.Duration(cfg.CleanupIntervalHours) * time.Hour
			if interval < 1*time.Hour {
				interval = 1 * time.Hour
			}
			select {
			case <-time.After(interval):
			case <-ctx.Done():
				return
			}
		}
	}()
	return done
}

func mustJSON(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
