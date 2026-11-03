package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

func TestRerankFallbackLoggerRateLimits(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)
	now := time.Unix(1_700_000_000, 0)
	observe := newRerankFallbackLogger(logger, func() time.Time { return now }, time.Minute)

	observe(100, context.DeadlineExceeded)
	require.Equal(t, 1, strings.Count(buf.String(), "\n"), "the first fallback is logged at once")
	require.Contains(t, buf.String(), `"candidates":100`)
	require.Contains(t, buf.String(), context.DeadlineExceeded.Error())

	now = now.Add(10 * time.Second)
	observe(100, context.DeadlineExceeded)
	observe(100, context.DeadlineExceeded)
	require.Equal(t, 1, strings.Count(buf.String(), "\n"), "fallbacks inside the interval are folded, not logged")

	now = now.Add(time.Minute)
	observe(10, context.DeadlineExceeded)
	lines := strings.Split(strings.TrimSpace(buf.String()), "\n")
	require.Len(t, lines, 2)
	require.Contains(t, lines[1], `"fallbacks_since_last_log":2`)
	require.Contains(t, lines[1], `"candidates":10`)
}
