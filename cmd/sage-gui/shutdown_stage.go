package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/rs/zerolog"
)

const (
	mcpHTTPShutdownBudget       = 30 * time.Second
	federationP2PShutdownBudget = 15 * time.Second
)

// runShutdownStage gives a potentially blocking cleanup routine its own
// wall-clock budget. A timeout is returned to the caller and logged with the
// stage name so a later supervisor restart cannot hide which resource stalled.
func runShutdownStage(
	ctx context.Context,
	logger zerolog.Logger,
	name string,
	budget time.Duration,
	fn func(context.Context) error,
) error {
	if fn == nil {
		return fmt.Errorf("%s: shutdown callback is unavailable", name)
	}
	if budget <= 0 {
		return fmt.Errorf("%s: shutdown budget must be positive", name)
	}

	stageCtx, cancel := context.WithTimeout(ctx, budget)
	defer cancel()

	started := time.Now()
	logger.Info().Str("shutdown_stage", name).Dur("budget", budget).Msg("shutdown stage starting")
	done := make(chan error, 1)
	go func() {
		done <- fn(stageCtx)
	}()

	select {
	case err := <-done:
		if err != nil {
			return fmt.Errorf("%s: %w", name, err)
		}
		logger.Info().
			Str("shutdown_stage", name).
			Dur("elapsed", time.Since(started)).
			Msg("shutdown stage complete")
		return nil
	case <-stageCtx.Done():
		err := stageCtx.Err()
		if errors.Is(err, context.Canceled) {
			logger.Warn().
				Str("shutdown_stage", name).
				Dur("elapsed", time.Since(started)).
				Msg("shutdown stage canceled")
		} else {
			logger.Warn().
				Str("shutdown_stage", name).
				Dur("budget", budget).
				Dur("elapsed", time.Since(started)).
				Msg("shutdown stage exceeded its budget")
		}
		return fmt.Errorf("%s: %w", name, err)
	}
}
