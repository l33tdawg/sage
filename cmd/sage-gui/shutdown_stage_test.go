package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestRunShutdownStageReturnsWorkerError(t *testing.T) {
	want := errors.New("cleanup failed")
	err := runShutdownStage(
		context.Background(),
		zerolog.Nop(),
		"test stage",
		time.Second,
		func(context.Context) error { return want },
	)
	require.ErrorIs(t, err, want)
	require.Contains(t, err.Error(), "test stage")
}

func TestRunShutdownStageBoundsBlockedWorker(t *testing.T) {
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })

	started := time.Now()
	err := runShutdownStage(
		context.Background(),
		zerolog.Nop(),
		"blocked stage",
		20*time.Millisecond,
		func(context.Context) error {
			<-release
			return nil
		},
	)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), time.Second)
}

func TestRunShutdownStageRejectsInvalidConfiguration(t *testing.T) {
	require.ErrorContains(t, runShutdownStage(
		context.Background(), zerolog.Nop(), "missing callback", time.Second, nil,
	), "callback is unavailable")
	require.ErrorContains(t, runShutdownStage(
		context.Background(), zerolog.Nop(), "zero budget", 0,
		func(context.Context) error { return nil },
	), "budget must be positive")
}
